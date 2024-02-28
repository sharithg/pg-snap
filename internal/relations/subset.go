package relations

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/charmbracelet/log"
	"github.com/videahealth/pg-snap/internal/db"
	"github.com/videahealth/pg-snap/internal/utils"
)

type Row struct {
	Table   *db.Table
	ColName string
	Data    string
}

type Subset struct {
	Relations        []db.ForeignKeyInfo
	Tables           []db.Table
	StartTableName   string
	StartTableSchema string
	DAG              DAG
}

func NewSubset(pg *db.Db, tables []db.Table) (*Subset, error) {
	relations, err := pg.GetForeignKeys()
	if err != nil {
		return nil, err
	}

	return &Subset{
		Relations:        relations,
		Tables:           tables,
		StartTableName:   "nutr_def",
		StartTableSchema: "public",
		DAG:              BuildRelations(relations),
	}, nil
}

func (s *Subset) Visit() {
	copiedData := make(map[*Node]bool)
	gVisited := make(map[*Node]bool)

	startNode := s.DAG.FindNodeByData(db.NormalizeName(s.StartTableSchema, s.StartTableName))
	printCallback := func(node *Node, visited map[*Node]bool) bool {
		table, err := GetTable(s.Tables, node.Data)
		gVisited[node] = true
		if err != nil {
			log.Fatalf("error getting table: %s", err)
			return false
		}
		fmt.Println(table.Id, copiedData[node])
		if !copiedData[node] {
			if node.Data == db.NormalizeName(s.StartTableSchema, s.StartTableName) {
				PerformCopy(*table, "SELECT * FROM public.nutr_def LIMIT 1")
				copiedData[node] = true
			} else {
				var queryB strings.Builder
				hasQ := false
				for _, p := range node.Children {
					vis := gVisited[p]
					fmt.Println("\t"+p.Data, vis)
					if vis && copiedData[p] {
						q := BuildQuery(p.Data, node.Data, s.Relations, s.Tables)
						queryB.WriteString(q)
						hasQ = true
					}
				}

				if hasQ {
					selectSt := fmt.Sprintf("SELECT * FROM %s WHERE %s", node.Data, queryB.String())
					fmt.Println("\t" + "COPYING")
					PerformCopy(*table, selectSt)
					copiedData[node] = true
				}
			}

		}
		return true
	}
	printCallback2 := func(node *Node, visited map[*Node]bool) bool {
		table, err := GetTable(s.Tables, node.Data)
		gVisited[node] = true

		if err != nil {
			log.Fatalf("error getting table: %s", err)
			return false
		}
		fmt.Println(table.Id, copiedData[node])
		if !copiedData[node] {
			if node.Data == db.NormalizeName(s.StartTableSchema, s.StartTableName) {
				PerformCopy(*table, "SELECT * FROM public.nutr_def LIMIT 1")
				copiedData[node] = true
			} else {
				var conditions []string
				hasQ := false
				for _, p := range s.DAG.FindPredecessors(node) {
					vis := gVisited[p]
					fmt.Println("\t"+p.Data, vis)
					if vis && copiedData[p] {
						q := BuildQuery2(p.Data, node.Data, s.Relations, s.Tables)
						if q != "" {
							conditions = append(conditions, q)
						}
						hasQ = true
					}
				}

				if hasQ {
					queryCondition := strings.Join(conditions, " OR ")
					selectSt := fmt.Sprintf("SELECT * FROM %s WHERE %s", node.Data, queryCondition)
					fmt.Println("\t" + selectSt)
					PerformCopy(*table, selectSt)
					copiedData[node] = true
				}
			}

		}
		return true
	}
	s.DAG.TraverseGraphFromStart(startNode, printCallback)

	startNode2 := s.DAG.FindNodeByData(db.NormalizeName("public", "food_des"))
	fmt.Println("-----------")
	s.DAG.TraverseGraphFromStart(startNode2, printCallback2)
	startNode3 := s.DAG.FindNodeByData(db.NormalizeName("public", "weight"))
	fmt.Println("-----------")
	s.DAG.TraverseGraphFromStart(startNode3, printCallback)
}

func BuildQuery2(foreignTableId string, toCopyId string, relations []db.ForeignKeyInfo, tables []db.Table) string {

	toCopy, err := GetTable(tables, toCopyId)
	if err != nil {
		log.Fatalf("error getting table: %s", err)
	}
	foreignTable, err := GetTable(tables, foreignTableId)
	if err != nil {
		log.Fatalf("error getting table: %s", err)
	}
	foreignTableCsvPath := filepath.Join("./data-dump", foreignTable.Details.Display, "data.csv")

	cols := GetTableFk(toCopy.Details.Schema, toCopy.Details.Name, foreignTable.Details.Schema, foreignTable.Details.Name, relations)
	var conditions []string

	for _, fk := range cols {
		data, err := ReadCSVColumnByName(foreignTableCsvPath, fk.ColumnName)
		if err != nil {
			log.Fatalf("error getting csv data for table: %s: %s", foreignTableCsvPath, err)
		}
		if len(data) == 0 {
			continue
		}
		condition := fmt.Sprintf("%s IN (%s)", fk.ColumnName, FormatCols(data, fk.ColType))
		conditions = append(conditions, condition)
	}

	if len(conditions) != 0 {
		queryCondition := strings.Join(conditions, " OR ")

		return queryCondition
	}
	return ""

}

func BuildQuery(foreignTableId string, toCopyId string, relations []db.ForeignKeyInfo, tables []db.Table) string {
	toCopy, err := GetTable(tables, toCopyId)
	if err != nil {
		log.Fatalf("error getting table: %s", err)
	}
	foreignTable, err := GetTable(tables, foreignTableId)
	if err != nil {
		log.Fatalf("error getting table: %s", err)
	}
	foreignTableCsvPath := filepath.Join("./data-dump", foreignTable.Details.Display, "data.csv")
	cols := GetTableFk(foreignTable.Details.Schema, foreignTable.Details.Name, toCopy.Details.Schema, toCopy.Details.Name, relations)
	var conditions []string

	for _, fk := range cols {
		data, err := ReadCSVColumnByName(foreignTableCsvPath, fk.ForeignColumnName)
		if err != nil {
			log.Fatalf("error getting csv data for table: %s: %s", foreignTableCsvPath, err)
		}
		condition := fmt.Sprintf("%s IN (%s)", fk.ColumnName, FormatCols(data, fk.ColType))
		conditions = append(conditions, condition)
	}

	if len(conditions) == 0 {
		log.Fatalf("No matching foreign key constraint found: %s", err)
	}

	// Combine all conditions with AND or OR depending on the logic you need
	// This example uses OR for simplicity
	queryCondition := strings.Join(conditions, " OR ")

	return queryCondition
}

func FormatCols(data []string, colType string) string {
	var dataVals []string

	for _, d := range data {
		switch t := colType; t {
		case "character":
			dataVals = append(dataVals, fmt.Sprintf(`'%s'`, d))
		case "text":
			dataVals = append(dataVals, fmt.Sprintf(`'%s'`, d))
		case "integer":
			dataVals = append(dataVals, d)
		}
	}

	return strings.Join(dataVals, ",")
}

func GetTableFk(ftSchema, ftName, schema, name string, relations []db.ForeignKeyInfo) []db.ForeignKeyInfo {
	var preds []db.ForeignKeyInfo

	for _, rel := range relations {
		if ftSchema == rel.ForeignSchema && ftName == rel.ForeignName && rel.Name == name && rel.Schema == schema {
			preds = append(preds, rel)
		}
	}
	return preds
}

func BuildRelations(relations []db.ForeignKeyInfo) DAG {
	dag := &DAG{}

	for _, rel := range relations {
		node1 := dag.CreateOrGetNode(db.NormalizeName(rel.Schema, rel.Name))
		node2 := dag.CreateOrGetNode(db.NormalizeName(rel.ForeignSchema, rel.ForeignName))

		dag.AddNode(node1)
		dag.AddNode(node2)

		dag.AddEdge(node1, node2)
	}

	fmt.Println(strings.ReplaceAll(dag.ToGraphviz(), "public_", ""))

	return *dag
}

func PerformCopy(tbl db.Table, query string) {
	root := "./data-dump"

	log.Debug(utils.SprintfNoNewlines("COPYING data from table %s", tbl.Details.Display))

	dirPath := filepath.Join(root, tbl.Details.Display)

	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		log.Error("Error copying for table %s: %s", tbl.Details.Display, err)
	}

	path := filepath.Join(dirPath, "data.csv")
	dataPath := filepath.Join(dirPath, "table.bin")

	_, err := tbl.CopyOut(path, query)
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

	// ops.Add(1)
	// progress := ops.Load()

	// log.Info(utils.SprintfNoNewlines("COPIED %s rows from %s",
	// 	utils.Colored(utils.Green, fmt.Sprint(rows)),
	// 	utils.Colored(utils.Yellow, tbl.Details.Display)),
	// 	"progress",
	// 	utils.SprintfNoNewlines("%d / %d", progress, total))

}
