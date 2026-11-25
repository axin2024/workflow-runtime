package console

import (
	"fmt"

	"github.com/axin2024/workflow-runtime/internal/store"
	"github.com/axin2024/workflow-runtime/internal/workflow"
)

type diagram struct {
	Width, Height int
	Nodes         []diagramNode
	Edges         []diagramEdge
}

type diagramNode struct {
	Key, Task, Queue, State string
	X, Y                    int
}

type diagramEdge struct {
	Path, Condition, Status string
}

func makeDiagram(graph workflow.Compiled, results []store.NodeRun) diagram {
	const column, row, margin, cardWidth, cardHeight = 240, 105, 22, 166, 62
	byKey := make(map[string]workflow.Node, len(graph.Definition.Nodes))
	for _, node := range graph.Definition.Nodes {
		byKey[node.Key] = node
	}
	states := make(map[string]string, len(results))
	decisions := make(map[[2]string]string)
	for _, result := range results {
		states[result.Key] = result.State
		for _, dependency := range result.Dependencies {
			decisions[[2]string{dependency.Node, result.Key}] = dependency.Status
		}
	}
	layers := make(map[string]int, len(byKey))
	counts := make(map[int]int)
	places := make(map[string]diagramNode, len(byKey))
	var result diagram
	for _, key := range graph.Order {
		node := byKey[key]
		layer := 0
		for _, edge := range node.DependsOn {
			layer = max(layer, layers[edge.Node]+1)
		}
		layers[key] = layer
		placed := diagramNode{Key: key, Task: node.Task, Queue: node.Queue, State: states[key],
			X: margin + layer*column, Y: margin + counts[layer]*row}
		counts[layer]++
		result.Width = max(result.Width, placed.X+cardWidth+margin)
		result.Height = max(result.Height, placed.Y+cardHeight+margin)
		places[key] = placed
		result.Nodes = append(result.Nodes, placed)
	}
	drawingHeight := result.Height
	longEdges := 0
	for _, placed := range result.Nodes {
		for _, edge := range byKey[placed.Key].DependsOn {
			origin := places[edge.Node]
			x1, y1 := origin.X+cardWidth, origin.Y+cardHeight/2
			x2, y2 := placed.X, placed.Y+cardHeight/2
			var path string
			if layers[placed.Key]-layers[edge.Node] == 1 {
				midpoint := (x1 + x2) / 2
				path = fmt.Sprintf("M%d %d C%d %d %d %d %d %d", x1, y1, midpoint, y1, midpoint, y2, x2, y2)
			} else {
				bus := drawingHeight + 8*(longEdges%8)
				longEdges++
				result.Height = max(result.Height, bus+margin)
				path = fmt.Sprintf("M%d %d H%d V%d H%d V%d H%d", x1, y1, x1+18, bus, x2-18, y2, x2)
			}
			condition := string(edge.When)
			if edge.Branch != "" {
				condition += " / " + edge.Branch
			}
			result.Edges = append(result.Edges, diagramEdge{Path: path, Condition: edge.Node + " → " + placed.Key + " · " + condition,
				Status: decisions[[2]string{edge.Node, placed.Key}]})
		}
	}
	return result
}

func shorten(value string) string {
	if len(value) <= 20 {
		return value
	}
	return value[:19] + "…"
}
