package relations

// https://www.tonic.ai/blog/condenser-a-database-subsetting-tool

import (
	"fmt"
	"strings"
)

type Node struct {
	Data     string
	Children []*Node
}

type DAG struct {
	Nodes map[string]*Node
}

func (dag *DAG) CreateOrGetNode(data string) *Node {
	if dag.Nodes == nil {
		dag.Nodes = make(map[string]*Node)
	}
	if node, exists := dag.Nodes[data]; exists {
		return node
	}
	newNode := &Node{
		Data: data,
	}
	dag.Nodes[data] = newNode
	return newNode
}

func (dag *DAG) AddNode(node *Node) {
	if dag.Nodes == nil {
		dag.Nodes = make(map[string]*Node)
	}
	if _, exists := dag.Nodes[node.Data]; !exists {
		dag.Nodes[node.Data] = node
	}
}

func (dag *DAG) AddEdge(parent, child *Node) {
	// Ensure the child is not already in the parent's children slice
	exists := false
	for _, existingChild := range parent.Children {
		if existingChild == child {
			exists = true
			break
		}
	}
	if !exists {
		parent.Children = append(parent.Children, child)
	}
}

func (dag *DAG) PrettyPrintNode(node *Node, depth int) {
	indent := strings.Repeat(" ", depth*4) // Adjust the indentation multiplier as needed
	fmt.Printf("%s- %s\n", indent, node.Data)
	for _, child := range node.Children {
		dag.PrettyPrintNode(child, depth+1)
	}
}

func (dag *DAG) PrettyPrint() {
	roots := dag.FindRoots()
	for _, root := range roots {
		dag.PrettyPrintNode(root, 0)
	}
}

// FindRoots finds all nodes in the DAG that have no incoming edges.
// These nodes are considered "root" nodes and are starting points for printing.
func (dag *DAG) FindRoots() []*Node {
	childSet := make(map[string]bool)
	// Mark all children in the DAG
	for _, node := range dag.Nodes {
		for _, child := range node.Children {
			childSet[child.Data] = true
		}
	}

	// Find nodes not in childSet; those are roots
	var roots []*Node
	for _, node := range dag.Nodes {
		if !childSet[node.Data] {
			roots = append(roots, node)
		}
	}
	return roots
}

// Helper function to find roots of the DAG
func (dag *DAG) findRoots() []*Node {
	inDegree := make(map[string]int)
	for _, node := range dag.Nodes {
		for _, child := range node.Children {
			inDegree[child.Data]++
		}
	}

	var roots []*Node
	for _, node := range dag.Nodes {
		if inDegree[node.Data] == 0 {
			roots = append(roots, node)
		}
	}
	return roots
}

// Corrected Topological Sort that calculates depth correctly
func (dag *DAG) TopologicalSort() [][]string {
	visited := make(map[string]bool)
	depth := make(map[string]int)
	var dfs func(*Node, int)

	dfs = func(node *Node, currentDepth int) {
		if visited[node.Data] {
			if currentDepth <= depth[node.Data] {
				return // Already visited at an equal or deeper depth
			}
		}
		visited[node.Data] = true
		depth[node.Data] = currentDepth

		for _, child := range node.Children {
			dfs(child, currentDepth+1)
		}
	}

	// Initialize DFS from each root
	for _, root := range dag.findRoots() {
		dfs(root, 0)
	}

	// Determine the max depth to size the result slice
	maxDepth := 0
	for _, d := range depth {
		if d > maxDepth {
			maxDepth = d
		}
	}

	// Group nodes by their depth
	result := make([][]string, maxDepth+1)
	for nodeData, d := range depth {
		result[d] = append(result[d], nodeData)
	}

	return result
}
