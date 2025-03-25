package intersection

import (
	"sort"

	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
)

type IJoinOrder interface {
	ForeignJoinPredicate(x []rds.Predicate) [][]rds.Predicate
	SelfJoinPredicate(x []rds.Predicate) [][]rds.Predicate
	TransferSelfJoinRule(x []rds.Predicate) []rds.Predicate
}

type Graph struct {
	root           *GraphNode
	graphNodeMap   map[int]*GraphNode // tid -> *GraphNode
	tableIdMap     map[string]bool    // graph tableId map
	predicates     []rds.Predicate    // only x predicates, different tid predicates
	selfPredicates []rds.Predicate    // only x predicates, the same table predicates
}

type GraphNode struct {
	tid        int
	predicates []*rds.Predicate // Associate predicates in the graph, stored in the neighbor graph nodes. Preserved to bfs.
	neighbors  []*GraphNode
}

type JoinOrder struct {
	graphs []*Graph // len(graphs) == 1 is one graph; == 2 is two graphs
}

func NewJoinOrder(y rds.Predicate) IJoinOrder {
	jo := &JoinOrder{}
	root := &GraphNode{
		tid:        y.LeftColumn.ColumnIndex,
		predicates: []*rds.Predicate{},
		neighbors:  []*GraphNode{},
	}
	left := &Graph{
		root:         root,
		graphNodeMap: map[int]*GraphNode{root.tid: root},
		tableIdMap:   map[string]bool{y.LeftColumn.TableId: true},
	}
	root = &GraphNode{
		tid:        y.RightColumn.ColumnIndex,
		predicates: []*rds.Predicate{},
		neighbors:  []*GraphNode{},
	}
	right := &Graph{
		root:         root,
		graphNodeMap: map[int]*GraphNode{root.tid: root},
		tableIdMap:   map[string]bool{y.RightColumn.TableId: true},
	}
	jo.graphs = []*Graph{left, right}
	return jo
}

func (jo *JoinOrder) ForeignJoinPredicate(x []rds.Predicate) [][]rds.Predicate {
	tmp := make([]*rds.Predicate, len(x))
	for i := 0; i < len(x); i++ {
		tmp[i] = &x[i]
	}
	jo.buildGraph(nil, jo.graphs[0], jo.graphs[0].root, tmp)
	jo.buildGraph(jo.graphs[0], jo.graphs[1], jo.graphs[1].root, tmp)

	// process const predicates
	for _, p := range tmp {
		if p.PredicateType != 0 {
			continue
		}
		if jo.graphs[0].tableIdMap[p.LeftColumn.TableId] {
			jo.graphs[0].predicates = append(jo.graphs[0].predicates, *p)
		} else {
			jo.graphs[1].predicates = append(jo.graphs[1].predicates, *p)
		}
	}
	return [][]rds.Predicate{jo.graphs[0].predicates, jo.graphs[1].predicates}
}

func (jo *JoinOrder) buildGraph(graph0, graph *Graph, root *GraphNode, x []*rds.Predicate) {
	var nextX []*rds.Predicate
	for i := 0; i < len(x); i++ {
		// const predicates will be processed later
		if x[i].PredicateType == 0 {
			continue
		}
		if x[i].LeftColumn.TableId == x[i].RightColumn.TableId {
			continue
		}
		leftTid := x[i].LeftColumn.ColumnIndex
		rightTid := x[i].RightColumn.ColumnIndex
		// exists in graph0
		if graph0 != nil && (graph0.graphNodeMap[leftTid] != nil || graph0.graphNodeMap[rightTid] != nil) {
			continue
		}
		if root.tid == leftTid {
			node := graph.graphNodeMap[rightTid]
			if node == nil {
				neighbor := &GraphNode{
					tid:        rightTid,
					predicates: []*rds.Predicate{},
					neighbors:  []*GraphNode{},
				}
				root.neighbors = append(root.neighbors, neighbor)
				graph.graphNodeMap[rightTid] = neighbor
				graph.tableIdMap[x[i].RightColumn.TableId] = true
				node = neighbor
			}
			node.predicates = append(node.predicates, x[i])
			graph.predicates = append(graph.predicates, *x[i])
		} else if root.tid == rightTid {
			node := graph.graphNodeMap[leftTid]
			if node == nil {
				neighbor := &GraphNode{
					tid:        leftTid,
					predicates: []*rds.Predicate{},
					neighbors:  []*GraphNode{},
				}
				root.neighbors = append(root.neighbors, neighbor)
				graph.graphNodeMap[leftTid] = neighbor
				graph.tableIdMap[x[i].LeftColumn.TableId] = true
				node = neighbor
			}
			node.predicates = append(node.predicates, x[i])
			graph.predicates = append(graph.predicates, *x[i])
		} else {
			nextX = append(nextX, x[i])
		}
	}
	for _, node := range root.neighbors {
		jo.buildGraph(graph0, graph, node, nextX)
	}
}

func (jo *JoinOrder) SelfJoinPredicate(x []rds.Predicate) [][]rds.Predicate {
	for i := 0; i < len(x); i++ {
		if x[i].LeftColumn.TableId != x[i].RightColumn.TableId {
			continue
		}
		tableId := x[i].LeftColumn.TableId
		leftTid := x[i].LeftColumn.ColumnIndex
		rightTid := x[i].RightColumn.ColumnIndex
		if jo.graphs[0].graphNodeMap[leftTid] != nil {
			if jo.graphs[1].graphNodeMap[rightTid] != nil {
				// discard, because of both lhs and rhs in graph0 and graph1 respectively
			} else {
				// predicate lhs in graph0, but rhs not in graph1
				jo.graphs[0].selfPredicates = append(jo.graphs[0].selfPredicates, x[i])
			}
		} else {
			if jo.graphs[1].graphNodeMap[rightTid] != nil {
				// predicate lhs not in graph0, but rhs in graph1
				jo.graphs[1].selfPredicates = append(jo.graphs[1].selfPredicates, x[i])
			} else {
				// predicate lhs not in graph0, and rhs not in graph1
				if jo.graphs[0].tableIdMap[tableId] {
					jo.graphs[0].selfPredicates = append(jo.graphs[0].selfPredicates, x[i])
				} else {
					jo.graphs[1].selfPredicates = append(jo.graphs[1].selfPredicates, x[i])
				}
			}
		}
	}
	return [][]rds.Predicate{jo.graphs[0].selfPredicates, jo.graphs[1].selfPredicates}
}

func (jo *JoinOrder) TransferSelfJoinRule(x []rds.Predicate) []rds.Predicate {
	var ret []rds.Predicate
	for i := 0; i < len(x); i++ {
		if x[i].PredicateType == 0 {
			continue
		}
		if x[i].LeftColumn.TableId != x[i].RightColumn.TableId {
			continue
		}
		if (jo.graphs[0].graphNodeMap[x[i].LeftColumn.ColumnIndex] != nil && jo.graphs[1].graphNodeMap[x[i].RightColumn.ColumnIndex] != nil) ||
			(jo.graphs[0].graphNodeMap[x[i].RightColumn.ColumnIndex] != nil && jo.graphs[1].graphNodeMap[x[i].LeftColumn.ColumnIndex] != nil) {
			ret = append(ret, x[i])
		}
	}
	return ret
}

// deprecated
func sortPredicates(data []rds.Predicate) {
	sort.SliceStable(data, func(i, j int) bool {
		tableId1 := data[i].LeftColumn.TableId
		if data[i].LeftColumn.TableId > data[i].RightColumn.TableId {
			tableId1 = data[i].RightColumn.TableId
		}
		tableId2 := data[j].LeftColumn.TableId
		if data[j].LeftColumn.TableId > data[j].RightColumn.TableId {
			tableId2 = data[j].RightColumn.TableId
		}
		if tableId1 == tableId2 {
			columnId1 := data[i].LeftColumn.ColumnId
			if data[i].LeftColumn.ColumnId > data[i].RightColumn.ColumnId {
				columnId1 = data[i].RightColumn.ColumnId
			}
			columnId2 := data[j].LeftColumn.ColumnId
			if data[j].LeftColumn.ColumnId > data[j].RightColumn.ColumnId {
				columnId2 = data[j].RightColumn.ColumnId
			}
			return columnId1 < columnId2
		}
		return tableId1 < tableId2
	})
}
