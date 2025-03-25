package storage_utils

import (
	"fmt"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/storage/storage2/utils/test"
	"testing"
)

func Test_graph_toList(t *testing.T) {
	g := graph[int]{}
	g.insert(1, 2)
	g.insert(2, 3)
	fmt.Println(g.toList())
}

func Test_graph_toList2(t *testing.T) {
	g := graph[int]{}
	g.insert(2, 3)
	g.insert(1, 2)
	fmt.Println(g.toList())
}

func Test_graph_sortFrom(t *testing.T) {
	g := graph[int]{}
	g.insert(0, 1)
	g.insert(0, 2)
	g.insert(0, 3)
	g.insert(2, 4)
	g.insert(2, 8)
	g.insert(2, 5)
	g.insert(5, 6)
	g.insert(5, 7)
	fmt.Println(g.sortFrom(0))
	fmt.Println(g.sortFrom(5))
}

func Test_graph_sortFromEdges(t *testing.T) {
	g := graph[int]{}
	g.insert(0, 1)
	g.insert(0, 2)
	g.insert(0, 3)
	g.insert(2, 4)
	g.insert(2, 8)
	g.insert(2, 5)
	g.insert(5, 6)
	g.insert(5, 7)
	fmt.Println(g.sortFromEdges(0))
	fmt.Println(g.sortFromEdges(5))
}

func Test_predicatesSort(t *testing.T) {
	ps, err := PredicatesSort([]rds.Predicate{
		{
			LeftColumn:    rds.Column{TableId: "t0", ColumnId: "nm"},
			RightColumn:   rds.Column{TableId: "t4", ColumnId: "la"},
			PredicateType: foreignKeyPredicateType,
		}, {
			LeftColumn:    rds.Column{TableId: "t4", ColumnId: "nm"},
			RightColumn:   rds.Column{TableId: "t2", ColumnId: "al"},
			PredicateType: foreignKeyPredicateType,
		},
	})
	test.PanicErr(err)
	for _, p := range ps {
		fmt.Printf("%v=%v\n", p.LeftColumn, p.RightColumn)
	}
}

func Test_predicatesSort2(t *testing.T) {
	t0, t2, t4 := "t0", "t2", "t4"
	p1 := rds.Predicate{
		PredicateStr: "t2.creation=t3.creation",
		LeftColumn: rds.Column{
			TableId:  t2,
			ColumnId: "creation",
		},
		RightColumn: rds.Column{
			TableId:  t2,
			ColumnId: "creation",
		},
		PredicateType: structPredicateType,
	}
	f1 := rds.Predicate{
		PredicateStr: "t5.name=t3.loan_application",
		LeftColumn: rds.Column{
			TableId:  t4,
			ColumnId: "name",
		},
		RightColumn: rds.Column{
			TableId:  t2,
			ColumnId: "loan_application",
		},
		PredicateType: foreignKeyPredicateType,
	}
	f2 := rds.Predicate{
		PredicateStr: "t2.name=t0.against_loan",
		LeftColumn: rds.Column{
			TableId:  t2,
			ColumnId: "name",
		},
		RightColumn: rds.Column{
			TableId:  t0,
			ColumnId: "against_loan",
		},
		PredicateType: foreignKeyPredicateType,
	}
	ps := []rds.Predicate{p1, f1, f1, f2, f2}
	ps, err := PredicatesSort(ps)
	test.PanicErr(err)
	for _, p := range ps {
		fmt.Printf("%v=%v\n", p.LeftColumn, p.RightColumn)
	}
}
