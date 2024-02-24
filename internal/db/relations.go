package db

import (
	"fmt"
	"strings"

	"github.com/dominikbraun/graph"
)

func GetRelationsForTable(fkInfo []ForeignKeyInfo, table *Table, schema string, name string) (*ForeignKeyInfo, error) {
	for _, fk := range fkInfo {
		if fk.ForeignTableName == table.Details.Name &&
			fk.ForeignTableSchema == table.Details.Schema &&
			fk.TableName == name &&
			fk.TableSchema == schema {
			return &fk, nil
		}
	}

	return nil, fmt.Errorf("error finding relation for table %s.%s", schema, name)
}

func findPredecessorsUsingMap(predecessorMap map[int]map[int]graph.Edge[int], targetVertexID int) []int {
	predecessors := []int{}

	if predecessorsMap, exists := predecessorMap[targetVertexID]; exists {
		for pred := range predecessorsMap {
			predecessors = append(predecessors, pred)
		}
	}

	return predecessors
}

func findPredecessors(g graph.Graph[int, int], targetVertexID int) ([]int, error) {
	// Use the PredecessorMap method
	predecessorMap, err := g.PredecessorMap()
	if err != nil {
		fmt.Println("Error computing the predecessor map:", err)
		return nil, err
	}

	// Given a target vertex ID, find its predecessors
	predecessors := findPredecessorsUsingMap(predecessorMap, targetVertexID)

	return predecessors, nil
}

func BuildRelationGraph(db *Db, relations []ForeignKeyInfo) (graph.Graph[int, int], graph.Graph[int, int], map[string]int, error) {
	g := graph.New(graph.IntHash, graph.Directed(), graph.Acyclic())
	reverseG := graph.New(graph.IntHash, graph.Directed(), graph.Acyclic()) // Reverse graph

	tableToVertexID := make(map[string]int)
	vertexID := 1

	// Add vertices
	for _, relation := range relations {
		sourceIdentifier := relation.TableSchema + "." + relation.TableName
		targetIdentifier := relation.ForeignTableSchema + "." + relation.ForeignTableName

		if _, exists := tableToVertexID[sourceIdentifier]; !exists {
			_ = g.AddVertex(vertexID)
			_ = reverseG.AddVertex(vertexID) // Also add to reverse graph
			tableToVertexID[sourceIdentifier] = vertexID
			vertexID++
		}

		if _, exists := tableToVertexID[targetIdentifier]; !exists {
			_ = g.AddVertex(vertexID)
			_ = reverseG.AddVertex(vertexID) // Also add to reverse graph
			tableToVertexID[targetIdentifier] = vertexID
			vertexID++
		}
	}

	// Add edges to both graphs
	for _, relation := range relations {
		sourceIdentifier := relation.TableSchema + "." + relation.TableName
		targetIdentifier := relation.ForeignTableSchema + "." + relation.ForeignTableName

		sourceVertexID := tableToVertexID[sourceIdentifier]
		targetVertexID := tableToVertexID[targetIdentifier]

		_ = g.AddEdge(sourceVertexID, targetVertexID)        // Original direction
		_ = reverseG.AddEdge(targetVertexID, sourceVertexID) // Reverse direction
	}

	return g, reverseG, tableToVertexID, nil
}

func GetTables(db *Db, tables []*Table, sampleTable string, numToSample int64) {

	relations, err := db.GetForeignKeys()

	if err != nil {
		fmt.Println(err)
	}

	g, reverseG, tableToVertexID, err := BuildRelationGraph(db, relations)

	vertexIDToTable := make(map[int]string)

	for table, vertexID := range tableToVertexID {
		vertexIDToTable[vertexID] = table
	}

	if err != nil {
		fmt.Println(err)
	}

	var tableToSample *Table

	for _, table := range tables {
		res := strings.Split(sampleTable, ".")

		if table.Details.Schema == res[0] && table.Details.Name == res[1] {
			tableToSample = table
		}
	}

	tableToSample.SampleQuery = fmt.Sprintf("SELECT * FROM %s LIMIT %d ORDER BY num_data_pts DESC", tableToSample.Details.Identifier, numToSample)

	_ = graph.DFS(g, 7, func(value int) bool {
		fmt.Println(value)
		return false
	})
	_ = graph.DFS(reverseG, 7, func(value int) bool {
		fmt.Println(value)
		return false
	})

	g.AdjacencyMap()

	if err != nil {
		fmt.Println(err)
	}
}

// var queryBuilder strings.Builder
// queryBuilder.WriteString(fmt.Sprintf("SELECT %s.* FROM %s", table.Details.Identifier, table.Details.Identifier))

// vId := tableToVertexID[fmt.Sprintf("%s.%s", table.Details.Schema, table.Details.Name)]
// predecessors, err := findPredecessors(g, vId)

// if err != nil {
// 	fmt.Println(err)
// }

// fmt.Println(table.Details.Identifier)
// for _, pred := range predecessors {
// 	tableId := vertexIDToTable[pred]
// 	res1 := strings.Split(tableId, ".")
// 	fk, err := GetRelationsForTable(relations, table, res1[0], res1[1])
// 	if err != nil {
// 		fmt.Println(err)
// 	}
// 	joinClause := fmt.Sprintf(` INNER JOIN "%s"."%s" ON %s."%s" = "%s"."%s"."%s"`,
// 		fk.TableSchema, fk.TableName, // Join table
// 		table.Details.Identifier, fk.ColumnName, // Current table column
// 		fk.TableSchema, fk.TableName, fk.ForeignColumnName) // Foreign table column

// 	queryBuilder.WriteString(joinClause)
// }

// query := queryBuilder.String()

// if sampleCount, ok := sampleMap[table.Details.Schema+table.Details.Name]; ok {
// 	table.SampleQuery = fmt.Sprintf("SELECT * FROM %s LIMIT %d ORDER BY num_data_pts DESC", table.Details.Identifier, sampleCount)
// 	table.SelectQuery = query
// } else {
// 	table.SelectQuery = query
// }

// fmt.Println("\t", query)
