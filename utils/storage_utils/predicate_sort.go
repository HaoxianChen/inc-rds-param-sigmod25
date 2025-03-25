package storage_utils

import (
	"errors"
	"fmt"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/storage/storage2/utils"
	"golang.org/x/exp/maps"
	"sort"
)

var UnListable = errors.New("UnListable")

// PredicatesSort 谓词排序，尽量保证跨表时，从一端表到另一端表。如果做不到，影响性能
// 返回时，跨表谓词在前，结构谓词和常数谓词在后面
func PredicatesSort(predicates []rds.Predicate) ([]rds.Predicate, error) {
	var foreignKeyPredicates []rds.Predicate
	var otherPredicates []rds.Predicate

	for _, predicate := range predicates {
		if ForeignKeyPredicate(&predicate) {
			foreignKeyPredicates = append(foreignKeyPredicates, predicate)
		} else {
			otherPredicates = append(otherPredicates, predicate)
		}
	}

	// 没有跨表谓词，或者只跨一张表，不排序
	if len(foreignKeyPredicates) == 0 {
		return predicates, nil
	}

	// 只有一个跨表谓词，让左侧表为小表，因为左侧表是索引表
	if len(foreignKeyPredicates) == 1 {
		leftTab := foreignKeyPredicates[0].LeftColumn.TableId
		rightTab := foreignKeyPredicates[0].RightColumn.TableId
		leftSz, err := TableLengthNotRegradingDeleted(leftTab)
		if err != nil {
			return nil, err
		}
		rightSz, err := TableLengthNotRegradingDeleted(rightTab)
		if err != nil {
			return nil, err
		}
		if leftSz > rightSz {
			foreignKeyPredicates[0].LeftColumn, foreignKeyPredicates[0].RightColumn = foreignKeyPredicates[0].RightColumn, foreignKeyPredicates[0].LeftColumn
		}
		return append(foreignKeyPredicates, otherPredicates...), nil
	}

	// 多个跨表谓词，需要图算法
	graph := graph[tableName]{}
	for _, fp := range foreignKeyPredicates {
		leftTable := fp.LeftColumn.TableId
		rightTable := fp.RightColumn.TableId
		graph.insert(leftTable, rightTable)
	}

	// 如果图是链表，则找到两个端点
	tables, toListOk := graph.toList()
	if toListOk {
		first := tables[0]
		last := tables[len(tables)-1]

		// 查看端点是否有结构谓词
		firstStruct := hasStructurePredicate(first, otherPredicates)
		lastStruct := hasStructurePredicate(last, otherPredicates)

		// 端点没有结构谓词
		if !firstStruct && !lastStruct {
			logger.Warnf("端点没有结构谓词 %+v", predicatesToStrings(predicates))
			goto badCondition
		}

		// 前后都有结构谓词，按照表大小选定起点
		if firstStruct && lastStruct {
			firstSz, err := TableLengthNotRegradingDeleted(first)
			if err != nil {
				return nil, err
			}
			lastSz, err := TableLengthNotRegradingDeleted(last)
			if err != nil {
				return nil, err
			}

			if firstSz > lastSz {
				// 反转
				for i, j := 0, len(tables)-1; i < j; i, j = i+1, j-1 {
					tables[i], tables[j] = tables[j], tables[i]
				}
			}
		} else if lastStruct { // 只有 last 才有结构谓词
			// 反转
			for i, j := 0, len(tables)-1; i < j; i, j = i+1, j-1 {
				tables[i], tables[j] = tables[j], tables[i]
			}
		}

		// 对 foreignKeyPredicates 排序
		sortedForeignKeyPredicates := foreignKeyPredicatesSort(tables, foreignKeyPredicates)
		return append(sortedForeignKeyPredicates, otherPredicates...), nil
	}

badCondition:
	logger.Warnf("不是链表，或者链表端点没有结构谓词 %+v", predicatesToStrings(predicates))
	// 找到一个有结构谓词的表
	for _, table := range graph.points() {
		if hasStructurePredicate(table, otherPredicates) {
			tableLinks, ok := graph.sortFromEdges(table)
			if ok {
				// 对 foreignKeyPredicates 排序
				sortedForeignKeyPredicates := foreignKeyPredicatesSortByEdge(tableLinks, foreignKeyPredicates)

				return append(sortedForeignKeyPredicates, otherPredicates...), nil
			}
		}
	}

	logger.Warnf("拓扑排序失败 %+v", predicatesToStrings(predicates))
	if toListOk {
		sortedForeignKeyPredicates := foreignKeyPredicatesSort(tables, foreignKeyPredicates)
		return append(sortedForeignKeyPredicates, otherPredicates...), nil
	} else {
		// 我就原序了
		return predicates, nil
	}
}

func foreignKeyPredicatesSort(tables []tableName, foreignKeyPredicates []rds.Predicate) []rds.Predicate {
	var sortedForeignKeyPredicates []rds.Predicate
	for i := 0; i < len(tables)-1; i++ {
		left := tables[i]
		right := tables[i+1]
		ps := findForeignKeyPredicate(foreignKeyPredicates, left, right)
		sortedForeignKeyPredicates = append(sortedForeignKeyPredicates, ps...)
	}
	return sortedForeignKeyPredicates
}

func foreignKeyPredicatesSortByEdge(edges []edge[tableName], foreignKeyPredicates []rds.Predicate) []rds.Predicate {
	var sortedForeignKeyPredicates []rds.Predicate
	for _, e := range edges {
		left := e.a
		right := e.b
		ps := findForeignKeyPredicate(foreignKeyPredicates, left, right)
		sortedForeignKeyPredicates = append(sortedForeignKeyPredicates, ps...)
	}
	return sortedForeignKeyPredicates
}

func hasStructurePredicate(tab tableName, predicates []rds.Predicate) bool {
	for _, p := range predicates {
		if p.PredicateType == structPredicateType {
			if p.LeftColumn.TableId == tab {
				return true
			}
		}
	}

	return false
}

func findForeignKeyPredicate(foreignKeyPredicates []rds.Predicate, leftTable tableName, rightTable tableName) []rds.Predicate {
	var ps []rds.Predicate
	for _, fp := range foreignKeyPredicates {
		theLeftTable := fp.LeftColumn.TableId
		theRightTable := fp.RightColumn.TableId
		if leftTable == theLeftTable && rightTable == theRightTable {
			ps = append(ps, fp)
		} else if leftTable == theRightTable && rightTable == theLeftTable {
			fp.LeftColumn, fp.RightColumn = fp.RightColumn, fp.LeftColumn
			ps = append(ps, fp)
		}
	}
	return ps
}

type set[P comparable] map[P]struct{}

// graph 无向图
type graph[P comparable] map[P]set[P]

func (g graph[P]) insert(p1, p2 P) {
	g.insert0(p1, p2)
	g.insert0(p2, p1)
}

// toList 将图转为链表，如果可行的话
func (g graph[P]) toList() (ps []P, ok bool) {
	return g.deppCopy().toList0(nil)
}

func (g graph[P]) sortFrom(p P) (ps []P, ok bool) {
	cg := g.deppCopy()
	ps = append(ps, p) // from p
	queue := []P{p}
	for len(cg) != 0 && len(queue) != 0 {
		start := queue[0]
		queue = queue[1:]
		ends := cg.removeEdgeFrom(start)
		queue = append(queue, ends...)
		ps = append(ps, ends...)
	}
	return ps, len(cg) == 0
}

func (g graph[P]) sortFromEdges(p P) (edges []edge[P], ok bool) {
	cg := g.deppCopy()
	queue := []P{p}
	for len(cg) != 0 && len(queue) != 0 {
		start := queue[0]
		queue = queue[1:]
		ends := cg.removeEdgeFrom(start)
		for _, end := range ends {
			edges = append(edges, edge[P]{start, end})
			queue = append(queue, end)
		}
	}
	return edges, len(cg) == 0
}

func (g graph[P]) removeEdgeFrom(p P) []P {
	s, ok := g[p]
	if !ok {
		return nil
	}
	pAllTo := s.toList()
	for _, p2 := range pAllTo {
		g.remove(p, p2)
	}
	return pAllTo
}

func (g graph[P]) toList0(ps []P) ([]P, bool) {
	if len(g) == 0 {
		return ps, true
	}
	if len(ps) > 0 {
		p := ps[len(ps)-1]
		s := g[p]
		if len(s) == 1 {
			p2 := s.head()
			ps = append(ps, p2)
			g.remove(p, p2)
			return g.toList0(ps)
		} else {
			return nil, false
		}
	} else {
		for _, p := range g.points() {
			s := g[p]
			if len(s) == 1 {
				p2 := s.head()
				ps = append(ps, p)
				ps = append(ps, p2)
				g.remove(p, p2)
				return g.toList0(ps)
			}
		}
		return nil, false
	}
}

// remove 移除一条边
func (g graph[P]) remove(p1, p2 P) {
	if _, ok := g[p1][p2]; !ok {
		panic("bad code " + fmt.Sprintf("%+v", g))
	}
	if _, ok := g[p2][p1]; !ok {
		panic("bad code " + fmt.Sprintf("%+v", g))
	}

	delete(g[p1], p2)
	if len(g[p1]) == 0 {
		delete(g, p1)
	}
	delete(g[p2], p1)
	if len(g[p2]) == 0 {
		delete(g, p2)
	}
}

func (g graph[P]) oneEdge() (p1, p2 P) {
	p1 = g.points()[0]
	p2 = g[p1].head()
	return
}

func (g graph[P]) deppCopy() graph[P] {
	cp := make(graph[P], len(g))
	for p, s := range g {
		cp[p] = maps.Clone(s)
	}
	return cp
}

func (g graph[P]) insert0(p1, p2 P) {
	s, ok := g[p1]
	if !ok {
		s = set[P]{}
	}
	s[p2] = struct{}{}
	g[p1] = s
}

// points 结果稳定
func (g graph[P]) points() []P {
	var ps []P
	for p := range g {
		ps = append(ps, p)
	}
	sort.Slice(ps, func(i, j int) bool {
		return utils.ToString(ps[i]) < utils.ToString(ps[j])
	})
	return ps
}

// head 结果稳定
func (s set[P]) head() (p P) {
	if len(s) == 0 {
		panic("bad code")
	}
	return s.toList()[0]
}

// toList 结果稳定
func (s set[P]) toList() (ps []P) {
	if len(s) == 0 {
		panic("bad code ")
	}
	for p := range s {
		ps = append(ps, p)
	}
	sort.Slice(ps, func(i, j int) bool {
		return utils.ToString(ps[i]) < utils.ToString(ps[j])
	})
	return ps
}

type edge[P comparable] struct {
	a P
	b P
}

func (e edge[P]) String() string {
	return fmt.Sprintf("%v-%v", e.a, e.b)
}
