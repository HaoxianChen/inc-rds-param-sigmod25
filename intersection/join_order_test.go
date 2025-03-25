package intersection

import (
	"fmt"
	"testing"

	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
)

func TestJoinOrder(t *testing.T) {
	// x := []rds.Predicate{
	// 	{LeftColumn: rds.Column{TableId: "1", ColumnIndex: 1}, RightColumn: rds.Column{TableId: "2", ColumnIndex: 2}},
	// 	{LeftColumn: rds.Column{TableId: "1", ColumnIndex: 1}, RightColumn: rds.Column{TableId: "1", ColumnIndex: 3}},
	// 	{LeftColumn: rds.Column{TableId: "1", ColumnIndex: 2}, RightColumn: rds.Column{TableId: "4", ColumnIndex: 5}},
	// }
	// y := rds.Predicate{LeftColumn: rds.Column{TableId: "1", ColumnIndex: 1}, RightColumn: rds.Column{TableId: "1", ColumnIndex: 2}}
	f1 := rds.Predicate{
		PredicateStr: "t3.sales_order_name = t1.sales_order_name",
		LeftColumn: rds.Column{
			TableId:     "tab_sales_invoice_5w",
			ColumnId:    "sales_order_name",
			ColumnIndex: 3,
		},
		RightColumn: rds.Column{
			TableId:     "tab_sales_order_5w",
			ColumnId:    "sales_order_name",
			ColumnIndex: 1,
		},
		ConstantValue: nil,
		PredicateType: 2,
	}
	f2 := rds.Predicate{
		PredicateStr: "t2.sales_order_name = t0.sales_order_name",
		LeftColumn: rds.Column{
			TableId:     "tab_sales_invoice_5w",
			ColumnId:    "sales_order_name",
			ColumnIndex: 2,
		},
		RightColumn: rds.Column{
			TableId:     "tab_sales_order_5w",
			ColumnId:    "sales_order_name",
			ColumnIndex: 0,
		},
		ConstantValue: nil,
		PredicateType: 2,
	}
	p1 := rds.Predicate{
		PredicateStr: "t0.selling_price_list = t1.selling_price_list",
		LeftColumn: rds.Column{
			TableId:     "tab_sales_order_5w",
			ColumnId:    "selling_price_list",
			ColumnIndex: 0,
		},
		RightColumn: rds.Column{
			TableId:     "tab_sales_order_5w",
			ColumnId:    "selling_price_list",
			ColumnIndex: 1,
		},
		ConstantValue: nil,
		PredicateType: 1,
	}
	p2 := rds.Predicate{
		PredicateStr: "t2.mailing_address = t3.mailing_address",
		LeftColumn: rds.Column{
			TableId:     "tab_sales_invoice_5w",
			ColumnId:    "mailing_address",
			ColumnIndex: 2,
		},
		RightColumn: rds.Column{
			TableId:     "tab_sales_invoice_5w",
			ColumnId:    "mailing_address",
			ColumnIndex: 3,
		},
		ConstantValue: nil,
		PredicateType: 1,
	}

	x := []rds.Predicate{f1, f2, p1, p2}
	y := rds.Predicate{
		PredicateStr: "t0.shipping_address_name = t1.shipping_address_name",
		LeftColumn: rds.Column{
			TableId:     "tab_sales_order_5w",
			ColumnId:    "shipping_address_name",
			ColumnIndex: 0,
		},
		RightColumn: rds.Column{
			TableId:     "tab_sales_order_5w",
			ColumnId:    "shipping_address_name",
			ColumnIndex: 1,
		},
		ConstantValue: nil,
		PredicateType: 1,
	}

	jo := NewJoinOrder(y)
	fmt.Println(jo.(*JoinOrder).graphs[0])
	fmt.Println(jo.(*JoinOrder).graphs[1])
	fmt.Println()

	graphPredicates := jo.ForeignJoinPredicate(x)
	for i, graphPredicate := range graphPredicates {
		fmt.Printf("inter table graph %v:\n", i)
		for _, predicate := range graphPredicate {
			fmt.Println(predicate.LeftColumn.TableId, predicate.LeftColumn.ColumnIndex, predicate.RightColumn.TableId, predicate.RightColumn.ColumnIndex)
		}
		fmt.Println()
	}

	graphPredicates = jo.SelfJoinPredicate(x)
	for i, graphPredicate := range graphPredicates {
		fmt.Printf("inner table graph %v:\n", i)
		for _, predicate := range graphPredicate {
			fmt.Println(predicate.LeftColumn.TableId, predicate.LeftColumn.ColumnIndex, predicate.RightColumn.TableId, predicate.RightColumn.ColumnIndex)
		}
		fmt.Println()
	}

	xOut := jo.TransferSelfJoinRule(x)
	fmt.Println("out x:")
	for _, predicate := range xOut {
		fmt.Println(predicate.LeftColumn.TableId, predicate.LeftColumn.ColumnIndex, predicate.RightColumn.TableId, predicate.RightColumn.ColumnIndex)
	}
}
