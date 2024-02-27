package dump

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/charmbracelet/log"
	"github.com/urfave/cli/v3"
	"github.com/videahealth/pg-snap/internal/db"
	"github.com/videahealth/pg-snap/internal/pgcommand"
	"github.com/videahealth/pg-snap/internal/relations"
	"github.com/videahealth/pg-snap/internal/utils"
)

func ParseSkipTables(st string) map[string]struct{} {

	if st == "" {
		return make(map[string]struct{})
	}

	parts := strings.Split(st, ",")
	tableSet := make(map[string]struct{})
	for _, part := range parts {
		tableSet[part] = struct{}{}
	}

	return tableSet
}

func ExtractAndRemoveFKConstraints(input string) (string, string, error) {
	lines := strings.Split(input, "\n")

	var remainingContent string
	var extractedContent string
	capture := false

	for _, line := range lines {
		if strings.HasPrefix(line, "-- Name:") {
			if strings.Contains(line, "Type: FK CONSTRAINT") {
				capture = true
			} else {
				capture = false
				remainingContent += line + "\n"
			}
		}

		if capture {
			extractedContent += line + "\n"
		} else if !strings.HasPrefix(line, "-- Name:") {
			remainingContent += line + "\n"
		}
	}

	if extractedContent == "" && remainingContent == "" {
		return "", "", errors.New("no content processed")
	}

	return extractedContent, remainingContent, nil
}

func CompressDir(dir string, outFile string) error {
	var buf bytes.Buffer
	err := utils.Compress(dir, &buf)
	if err != nil {
		return err
	}

	fileToWrite, err := os.OpenFile(outFile, os.O_CREATE|os.O_RDWR, os.FileMode(0600))
	if err != nil {
		return err
	}
	if _, err := io.Copy(fileToWrite, &buf); err != nil {
		return err
	}

	return nil
}

func Run(ctx context.Context, cmd *cli.Command) error {
	dbParams := *utils.ParseDbParamsFromCli(cmd)
	programParams := *utils.ParseProgramParamsFromCli(cmd)

	if err := RunCmd(dbParams, programParams); err != nil {
		return err
	}

	return nil
}

func RunCmd(dbParams utils.DbParams, programParams utils.ProgramParams) error {

	pg, err := db.NewDb(context.Background(), dbParams)

	if err != nil {
		return err
	}

	skipTablesStr := programParams.SkipTables
	skipTables := ParseSkipTables(skipTablesStr)
	allTables, err := pg.GetAllTables(skipTables)
	if err != nil {
		return err
	}

	tblRelations, err := pg.GetForeignKeys()
	if err != nil {
		return nil
	}
	tables, err := relations.GetRelations(pg, allTables, "public.nut_data", 10, tblRelations)

	if err != nil {
		return err
	}

	// if true {
	// 	return nil
	// }

	// var tableStructure [][]db.Table
	// for _, table := range tables {
	// 	tableStructure = append(tableStructure, []db.Table{table})
	// }

	pgDbVersion := pg.GetVersion()
	pgDumpVersion, err := pgcommand.GetPgCmdVersion("pg_dump")

	if err != nil {
		return err
	}

	log.Info(utils.SprintfNoNewlines("Remote postgres version: %s, local version: %s", pgDbVersion, pgDumpVersion))

	if err := utils.ValidateDbVersion(pgDbVersion, pgDumpVersion); err != nil {
		return errors.New("major postgres version does not match pg_dump")
	}

	root := "./data-dump"
	defer os.RemoveAll(root)

	if err != nil {
		return err
	}

	concurrencyLimit := make(chan struct{}, programParams.Concurrency)

	var wg sync.WaitGroup
	var ops atomic.Uint64
	total := len(tables)

	for currDepth, tableLevel := range tables {
		for _, table := range tableLevel {
			wg.Add(1)

			concurrencyLimit <- struct{}{}

			go func(tbl db.Table, depth int) {
				defer wg.Done()

				if depth == 0 {
					L := 5
					numRows, err := tbl.GetNumRows()
					if err != nil {
						log.Fatalf("error getting num rows %s", err)
					}
					rowsToQuery := int64(math.Round(float64(numRows) * float64(L) * 0.01))
					tbl.SampleQuery = fmt.Sprintf("SELECT * FROM %s LIMIT %d", tbl.Details.Identifier, rowsToQuery)
				} else {
					predecessors := relations.GetTablePredecessors(tbl.Details.Schema, tbl.Details.Name, tblRelations)
					tbl.SampleQuery = relations.BuildSelectQuery(tbl.Details.Identifier, predecessors)
				}

				log.Debug(utils.SprintfNoNewlines("COPYING data from table %s", tbl.Details.Display))

				dirPath := filepath.Join(root, tbl.Details.Display)

				if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
					log.Error("Error copying for table %s: %s", tbl.Details.Display, err)
				}

				path := filepath.Join(dirPath, "data.csv")
				dataPath := filepath.Join(dirPath, "table.bin")

				rows, err := tbl.CopyOut(path, tbl.SampleQuery)
				if err != nil {
					log.Error("Error copying data for table %s: %w", tbl.Details.Display, err)
				}

				err = tbl.SerializeTable(dataPath)

				if err != nil {
					log.Error("Error serializing data for table %s: %w", tbl.Details.Display, err)
				}

				if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
					log.Error("Error serializing data for table %s: %s", tbl.Details.Display, err)
				}

				ops.Add(1)
				progress := ops.Load()

				log.Info(utils.SprintfNoNewlines("COPIED %s rows from %s",
					utils.Colored(utils.Green, fmt.Sprint(rows)),
					utils.Colored(utils.Yellow, tbl.Details.Display)),
					"progress",
					utils.SprintfNoNewlines("%d / %d", progress, total))

				<-concurrencyLimit
			}(table, currDepth)
		}
	}

	wg.Wait()

	log.Info("Extracting database DDL")

	ddlPath, err := filepath.Abs(filepath.Join(root, "ddl.sql"))
	if err != nil {
		return err
	}
	fkPath, err := filepath.Abs(filepath.Join(root, "fk_constraints.sql"))
	if err != nil {
		return err
	}
	ddlFile, err := os.OpenFile(ddlPath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	fkFile, err := os.OpenFile(fkPath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}

	dumpOutput, err := pgcommand.DumpDb(&dbParams)

	if err != nil {
		return err
	}

	fks, ddl, err := ExtractAndRemoveFKConstraints(dumpOutput)

	if err != nil {
		return err
	}

	_, err = ddlFile.WriteString(ddl)
	if err != nil {
		return err
	}
	_, err = fkFile.WriteString(fks)
	if err != nil {
		return err
	}

	if err = CompressDir(root, fmt.Sprintf("./%s.tar.gz", dbParams.Db)); err != nil {
		return err
	}

	return nil
}
