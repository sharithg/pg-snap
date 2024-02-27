package relations

import (
	"fmt"
	"strings"

	"github.com/videahealth/pg-snap/internal/db"
)

func reverse[S ~[]E, E any](s S) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func GetTable(tables []db.Table, id string) (*db.Table, error) {
	for _, table := range tables {
		if table.Details.Identifier == id {
			return &table, nil
		}
	}
	return nil, fmt.Errorf("error table not found for name %s", id)
}

func GetTablePredecessors(schema string, name string, relations []db.ForeignKeyInfo) []db.ForeignKeyInfo {
	var preds []db.ForeignKeyInfo

	for _, rel := range relations {
		if schema == rel.Schema && name == rel.Name {
			preds = append(preds, rel)
		}
	}
	return preds
}

func GetRelations(pg *db.Db, tables []db.Table, sampleTable string, numSample int, relations []db.ForeignKeyInfo) ([][]db.Table, error) {
	var tablesComb [][]db.Table

	dag := &DAG{}

	for _, rel := range relations {
		node1 := dag.CreateOrGetNode(db.NormalizeName(rel.Schema, rel.Name))
		node2 := dag.CreateOrGetNode(db.NormalizeName(rel.ForeignSchema, rel.ForeignName))

		dag.AddNode(node1)
		dag.AddNode(node2)

		dag.AddEdge(node1, node2)
	}

	dag.PrettyPrint()

	sorted := dag.TopologicalSort()
	reverse(sorted)

	for depth, group := range sorted {
		fmt.Printf("Depth %d: %v\n", depth, group)
	}

	for _, tableList := range sorted {
		var tablesCombLevel []db.Table
		for _, tableId := range tableList {
			table, err := GetTable(tables, tableId)
			if err != nil {
				return nil, err
			}
			tablesCombLevel = append(tablesCombLevel, *table)
		}
		tablesComb = append(tablesComb, tablesCombLevel)
	}

	return tablesComb, nil

	// first := sorted[0]
	// rest := sorted[1:]
	// L := 5

	// for _, tableId := range first {
	// 	table, err := GetTable(tables, tableId)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	numRows, err := table.GetNumRows()
	// 	if err != nil {
	// 		return err
	// 	}
	// 	rowsToQuery := int64(math.Round(float64(numRows) * float64(L) * 0.01))
	// 	table.SampleQuery = fmt.Sprintf("SELECT * FROM %s LIMIT %d", table.Details.Identifier, rowsToQuery)
	// 	tablesComb = append(tablesComb, *table)
	// }

	// for _, tableList := range rest {
	// 	for _, tableId := range tableList {
	// 		table, err := GetTable(tables, tableId)
	// 		if err != nil {
	// 			return err
	// 		}
	// 		predecessors := GetTablePredecessors(table.Details.Schema, table.Details.Name, relations)
	// 		table.SampleQuery = BuildSelectQuery(table.Details.Identifier, predecessors)
	// 		tablesComb = append(tablesComb, *table)
	// 	}
	// }

	// for _, comb := range tablesComb {
	// 	fmt.Println(comb.SampleQuery)
	// }

	// return nil
}

func BuildSelectQuery(mainTable string, predecessors []db.ForeignKeyInfo) string {
	var query strings.Builder
	query.WriteString(fmt.Sprintf("SELECT %s.* FROM %s", mainTable, mainTable))

	// Track the aliases to ensure uniqueness
	aliases := make(map[string]int)

	for _, fk := range predecessors {
		// Generate a unique alias for the foreign table based on its name and how many times it has been used
		aliasCount, exists := aliases[fk.ForeignName]
		if exists {
			aliases[fk.ForeignName] = aliasCount + 1
		} else {
			aliases[fk.ForeignName] = 1
		}
		alias := fmt.Sprintf("%s_%d", fk.ForeignName, aliases[fk.ForeignName])

		joinClause := fmt.Sprintf(" LEFT JOIN %s.%s AS %s ON %s.%s = %s.%s",
			fk.ForeignSchema, fk.ForeignName, alias, // JOIN table with alias
			mainTable, fk.ColumnName, // main table column
			alias, fk.ForeignColumnName) // foreign table column with alias

		query.WriteString(joinClause)
	}

	return query.String()
}
