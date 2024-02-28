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

func (dag *DAG) FindPredecessors(startNode *Node) []*Node {
	var predecessors []*Node
	for _, node := range dag.Nodes {
		for _, child := range node.Children {
			if child == startNode {
				predecessors = append(predecessors, node)
			}
		}
	}
	return predecessors
}

// Modified DFS to traverse through predecessors
func (dag *DAG) DFSPredecessors(startNode *Node, callback func(node *Node) bool) {
	visited := make(map[*Node]bool) // Keep track of visited nodes

	var dfsRecursive func(node *Node)
	dfsRecursive = func(node *Node) {
		visited[node] = true

		if !callback(node) {
			return // Stop traversal if callback returns false
		}

		// Find and recursively call DFS on each predecessor
		predecessors := dag.FindPredecessors(node)
		for _, pred := range predecessors {
			if !visited[pred] {
				dfsRecursive(pred)
			}
		}
	}

	dfsRecursive(startNode)
}

func (dag *DAG) TraverseGraphFromStart(startNode *Node) []*Node {
	visited := make(map[*Node]bool) // Keep track of visited nodes
	var result []*Node              // Slice to store the nodes encountered during DFS

	var dfsRecursive func(node *Node)
	dfsRecursive = func(node *Node) {
		if visited[node] {
			return // If already visited, skip
		}
		visited[node] = true

		// Add the current node to the result slice
		result = append(result, node)

		// Visit all predecessors
		for _, pred := range dag.FindPredecessors(node) {
			dfsRecursive(pred)
		}
	}

	// Initial DFS from the start node
	dfsRecursive(startNode)

	// Optionally, continue with DFS for unvisited nodes, ensuring the entire graph is covered.
	// Comment out or remove the below loop if you only want to traverse the connected component starting from startNode.
	for _, node := range dag.Nodes {
		if !visited[node] {
			dfsRecursive(node)
		}
	}

	return result
}

func (dag *DAG) DFS(startNode *Node, callback func(node *Node) bool) {
	visited := make(map[*Node]bool) // Keep track of visited nodes

	var dfsRecursive func(node *Node)
	dfsRecursive = func(node *Node) {
		visited[node] = true

		if !callback(node) {
			return
		}

		for _, child := range node.Children {
			if !visited[child] {
				dfsRecursive(child)
			}
		}
	}

	dfsRecursive(startNode)
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

func (dag *DAG) FindNodeByData(data string) *Node {
	var resultNode *Node = nil // This will hold the found node

	// Define a callback function for the DFS search
	callback := func(node *Node) bool {
		if node.Data == data {
			resultNode = node // Set the result node if found
			return false      // Return false to stop the DFS search
		}
		return true // Return true to continue the search
	}

	// Iterate over all nodes since the graph might not be fully connected
	for _, node := range dag.Nodes {
		if resultNode != nil {
			break // Stop if we've already found the node
		}
		dag.DFS(node, callback)
	}

	return resultNode
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

// ToGraphviz converts the DAG to a Graphviz representation.
func (dag *DAG) ToGraphviz() string {
	var builder strings.Builder
	builder.WriteString("digraph G {\n")

	// Function to recursively print nodes and edges
	var printNode func(*Node, map[string]bool)
	printed := make(map[string]bool) // Track printed nodes to avoid duplicates

	printNode = func(node *Node, visited map[string]bool) {
		if _, done := printed[node.Data]; done {
			return // Skip if already printed
		}
		printed[node.Data] = true
		for _, child := range node.Children {
			// Print edge
			builder.WriteString(fmt.Sprintf("    %s -> %s\n", strings.ReplaceAll(
				strings.ReplaceAll(node.Data, `"`, ""), ".", "_"),
				strings.ReplaceAll(strings.ReplaceAll(child.Data, `"`, ""), ".", "_")),
			)
			if _, seen := visited[child.Data]; !seen {
				visited[child.Data] = true
				printNode(child, visited)
			}
		}
	}

	// Find roots and print from there
	roots := dag.FindRoots()
	for _, root := range roots {
		printNode(root, make(map[string]bool))
	}

	builder.WriteString("}")
	return builder.String()
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
