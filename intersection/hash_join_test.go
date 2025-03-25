package intersection

import (
	"fmt"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables/table_data"
	utils2 "gitlab.grandhoo.com/rock/rock_v3/utils"
	"gitlab.grandhoo.com/rock/rock_v3/utils/storage_utils"
	"gitlab.grandhoo.com/rock/storage/storage2/database/database_facade/rich_dababase"
	"gitlab.grandhoo.com/rock/storage/storage2/database/database_facade/rich_dababase/irich_dababase"
	"gitlab.grandhoo.com/rock/storage/storage2/database/etl/extern_db/postgres"
	"gitlab.grandhoo.com/rock/storage/storage2/database/etl/import_from_DB/import_from_DB_config"
	"gitlab.grandhoo.com/rock/storage/storage2/rock_db"
	"gitlab.grandhoo.com/rock/storage/storage2/storage3/database/database_impl/database_impl_s3_base"
	"gitlab.grandhoo.com/rock/storage/storage2/storage3/database/database_impl/database_impl_s3_tx/database_impl_s3_tx_impl"
	"gitlab.grandhoo.com/rock/storage/storage2/storage3/file_system/memory_file_system"
	"gitlab.grandhoo.com/rock/storage/storage2/storage3/page_memory/page_memory"
	"gitlab.grandhoo.com/rock/storage/storage2/utils"
	"gitlab.grandhoo.com/rock/storage/storage2/utils/test"
	"testing"
	"time"
)

// SELECT count(*) -- 9815289
// from small1.tab_delivery_note dn,
//
//	small1.tab_delivery_stop ds,
//	small1.tab_delivery_note dn2,
//	small1.tab_delivery_stop ds2
//
// where
//
//	dn.delivery_note_name = ds.delivery_note_name and
//	dn2.delivery_note_name = ds2.delivery_note_name and
//	ds.delivery_address = ds2.delivery_address and
//	dn.shipping_address = dn2.shipping_address;
func Test_Hash_Join(t *testing.T) {
	useDBS3(func(richDB irich_dababase.IRichDatabase) {
		rock_db.DB = richDB
		tab_sales_order_5w := "tab_sales_order_5w"
		tab_sales_invoice_5w := "tab_sales_invoice_5w"
		dataSourceName := postgres.DataSourceName("127.0.0.1", "5432", "xxxx", "xxxx", "xxxx", "sslmode", "disable")
		pg, err := postgres.Open(dataSourceName)
		test.PanicErr(err)
		var importLength int64 = 0
		test.PanicErr(rock_db.DB.ImportFromDB(&import_from_DB_config.Config{
			ExternDataBase: pg,
			DBTableName:    "sales_1b.tab_sales_invoice_5w",
			NewTableName:   tab_sales_invoice_5w,
			ImportLength:   importLength,
			ColumnConfigMap: map[string]*import_from_DB_config.ColumnConfig{
				"selling_price_list": {DBColumnName: "selling_price_list"},
				"sales_order_name":   {DBColumnName: "sales_order_name"},
				"mailing_address":    {DBColumnName: "mailing_address"},
				"tax":                {DBColumnName: "tax"},
			},
		}))
		test.PanicErr(rock_db.DB.ImportFromDB(&import_from_DB_config.Config{
			ExternDataBase: pg,
			DBTableName:    "sales_1b.tab_sales_order_5w",
			NewTableName:   tab_sales_order_5w,
			ImportLength:   importLength,
			ColumnConfigMap: map[string]*import_from_DB_config.ColumnConfig{
				"selling_price_list":    {DBColumnName: "selling_price_list"},
				"sales_order_name":      {DBColumnName: "sales_order_name"},
				"mailing_address":       {DBColumnName: "mailing_address"},
				"shipping_address_name": {DBColumnName: "shipping_address_name"},
				"customer_name":         {DBColumnName: "customer_name"},
				"consumption_tax_rate":  {DBColumnName: "consumption_tax_rate"},
			},
		}))
		fmt.Println(rock_db.DB.Show(tab_sales_invoice_5w))
		fmt.Println(rock_db.DB.Show(tab_sales_order_5w))
		lhs, rhs := getPredicates2("tab_sales_order_5w", "tab_sales_invoice_5w")

		// 数据准备
		const taskId = 123456
		chanSize := 32
		table_data.LoadSchema(taskId, []string{tab_sales_invoice_5w, tab_sales_order_5w})
		table_data.LoadDataCreatePli(taskId, []string{tab_sales_invoice_5w, tab_sales_order_5w})
		storage_utils.DropCacheDataBase()
		table_data.CreateIndex(taskId)

		start := time.Now()
		//rs, xs, xys := NewHashJoin().CalcRule(taskId, []rds.Predicate{f1, f2, p1, p2}, rhs, 32)
		var calcIntersection = true
		if calcIntersection {
			intersection := NewHashJoin().CalcIntersection(taskId, lhs, rhs, chanSize)
			fmt.Printf("%s %d\n", utils.DurationString(start), len(intersection))
		} else {
			rowSize, xs, xys := NewHashJoin().CalcRule(taskId, lhs, rhs, chanSize)
			//var fkPredicates []rds.Predicate
			//var rowSize int
			//for _, lh := range lhs {
			//	if storage_utils.ForeignKeyPredicate(&lh) {
			//		fkPredicates = append(fkPredicates, lh)
			//	}
			//}
			//if len(fkPredicates) == len(lhs) {
			//	rowSize = xs
			//} else {
			//	uniqueFp, uniqueOk := storage_utils.ForeignKeyPredicatesUnique(fkPredicates)
			//	if uniqueOk {
			//		fkPredicates = uniqueFp
			//	}
			//	var fkUsedTableIndex []int
			//	for _, p := range fkPredicates {
			//		lTid, rTid := utils2.GetPredicateColumnIndexNew(p)
			//		fkUsedTableIndex = append(fkUsedTableIndex, lTid)
			//		fkUsedTableIndex = append(fkUsedTableIndex, rTid)
			//	}
			//	fkUsedTableIndex = utils.Distinct(fkUsedTableIndex)
			//	var fkIntersection = CalIntersection(nil, fkPredicates[0], taskId, chanSize)
			//
			//	for _, each := range fkIntersection {
			//		tmpFkIntersection := [][][]int32{each}
			//		for _, p := range fkPredicates[1:] {
			//			tmpFkIntersection = CalIntersection(tmpFkIntersection, p, taskId, chanSize)
			//			if len(tmpFkIntersection) < 1 {
			//				break
			//			}
			//		}
			//		rowSize += CalIntersectionSupport(tmpFkIntersection, fkUsedTableIndex, chanSize)
			//	}
			//
			//	if uniqueOk {
			//		rowSize *= rowSize
			//	}
			//}
			//logger.Infof("finish cal multi table rule rowSize:%v", utils2.GetLhsStr(fkPredicates))
			fmt.Printf("%s %d %d %d\n", utils.DurationString(start), rowSize, xs, xys)
		}
	})
}

func getPredicatesFromRuleJson(ruleJson string) ([]rds.Predicate, rds.Predicate) {
	rule := utils2.GetFormatRule(ruleJson)
	return rule.LhsPredicates, rule.Rhs
}

func useDBS3(use func(richDB irich_dababase.IRichDatabase)) {
	mfs := memory_file_system.New()
	pageMemory := test.PanicErr1(page_memory.New(mfs, 16*1024, 5*1024*1024*1024))
	db3base := database_impl_s3_base.New(pageMemory, 10)
	db3tx := database_impl_s3_tx_impl.New(db3base)
	rdb := rich_dababase.New(db3tx)
	test.PanicErr(database_impl_s3_tx_impl.InitLogTables(db3tx))
	use(rdb)
}

func getPredicates(table1 string, table2 string) ([]rds.Predicate, rds.Predicate) {
	f1 := rds.Predicate{
		PredicateStr: "t3.sales_order_name = t1.sales_order_name",
		LeftColumn: rds.Column{
			TableId:     table2,
			ColumnId:    "sales_order_name",
			ColumnIndex: 3,
		},
		RightColumn: rds.Column{
			TableId:     table1,
			ColumnId:    "sales_order_name",
			ColumnIndex: 1,
		},
		ConstantValue: nil,
		PredicateType: 2,
	}
	f2 := rds.Predicate{
		PredicateStr: "t2.sales_order_name = t0.sales_order_name",
		LeftColumn: rds.Column{
			TableId:     table2,
			ColumnId:    "sales_order_name",
			ColumnIndex: 2,
		},
		RightColumn: rds.Column{
			TableId:     table1,
			ColumnId:    "sales_order_name",
			ColumnIndex: 0,
		},
		ConstantValue: nil,
		PredicateType: 2,
	}
	p1 := rds.Predicate{
		PredicateStr: "t0.selling_price_list = t1.selling_price_list",
		LeftColumn: rds.Column{
			TableId:     table1,
			ColumnId:    "selling_price_list",
			ColumnIndex: 0,
		},
		RightColumn: rds.Column{
			TableId:     table1,
			ColumnId:    "selling_price_list",
			ColumnIndex: 1,
		},
		ConstantValue: nil,
		PredicateType: 1,
	}
	p2 := rds.Predicate{
		PredicateStr: "t2.mailing_address = t3.mailing_address",
		LeftColumn: rds.Column{
			TableId:     table2,
			ColumnId:    "mailing_address",
			ColumnIndex: 2,
		},
		RightColumn: rds.Column{
			TableId:     table2,
			ColumnId:    "mailing_address",
			ColumnIndex: 3,
		},
		ConstantValue: nil,
		PredicateType: 1,
	}

	rhs := rds.Predicate{
		PredicateStr: "t0.shipping_address_name = t1.shipping_address_name",
		LeftColumn: rds.Column{
			TableId:     table1,
			ColumnId:    "shipping_address_name",
			ColumnIndex: 0,
		},
		RightColumn: rds.Column{
			TableId:     table1,
			ColumnId:    "shipping_address_name",
			ColumnIndex: 1,
		},
		ConstantValue: nil,
		PredicateType: 1,
	}

	return []rds.Predicate{f1, f2, p1, p2}, rhs
}

func getPredicates1(table1 string, table2 string) ([]rds.Predicate, rds.Predicate) {
	f1 := rds.Predicate{
		PredicateStr: "t3.sales_order_name = t1.sales_order_name",
		LeftColumn: rds.Column{
			TableId:     table2,
			ColumnId:    "sales_order_name",
			ColumnIndex: 3,
		},
		RightColumn: rds.Column{
			TableId:     table1,
			ColumnId:    "sales_order_name",
			ColumnIndex: 1,
		},
		ConstantValue: nil,
		PredicateType: 2,
	}
	f2 := rds.Predicate{
		PredicateStr: "t2.sales_order_name = t0.sales_order_name",
		LeftColumn: rds.Column{
			TableId:     table2,
			ColumnId:    "sales_order_name",
			ColumnIndex: 2,
		},
		RightColumn: rds.Column{
			TableId:     table1,
			ColumnId:    "sales_order_name",
			ColumnIndex: 0,
		},
		ConstantValue: nil,
		PredicateType: 2,
	}
	p1 := rds.Predicate{
		PredicateStr: "t0.selling_price_list = t1.selling_price_list",
		LeftColumn: rds.Column{
			TableId:     table1,
			ColumnId:    "selling_price_list",
			ColumnIndex: 0,
		},
		RightColumn: rds.Column{
			TableId:     table1,
			ColumnId:    "selling_price_list",
			ColumnIndex: 1,
		},
		ConstantValue: nil,
		PredicateType: 1,
	}
	p2 := rds.Predicate{
		PredicateStr: " t2.tax=t3.tax",
		LeftColumn: rds.Column{
			TableId:     table2,
			ColumnId:    "tax",
			ColumnIndex: 2,
		},
		RightColumn: rds.Column{
			TableId:     table2,
			ColumnId:    "tax",
			ColumnIndex: 3,
		},
		ConstantValue: nil,
		PredicateType: 1,
	}

	rhs := rds.Predicate{
		PredicateStr: "t0.customer_name=t1.customer_name",
		LeftColumn: rds.Column{
			TableId:     table1,
			ColumnId:    "customer_name",
			ColumnIndex: 0,
		},
		RightColumn: rds.Column{
			TableId:     table1,
			ColumnId:    "customer_name",
			ColumnIndex: 1,
		},
		ConstantValue: nil,
		PredicateType: 1,
	}

	return []rds.Predicate{f1, f2, p1, p2}, rhs
}

func getPredicates2(table1 string, table2 string) ([]rds.Predicate, rds.Predicate) {
	f1 := rds.Predicate{
		PredicateStr: "t3.sales_order_name = t1.sales_order_name",
		LeftColumn: rds.Column{
			TableId:     table2,
			ColumnId:    "sales_order_name",
			ColumnIndex: 3,
		},
		RightColumn: rds.Column{
			TableId:     table1,
			ColumnId:    "sales_order_name",
			ColumnIndex: 1,
		},
		ConstantValue: nil,
		PredicateType: 2,
	}
	f2 := rds.Predicate{
		PredicateStr: "t2.sales_order_name = t0.sales_order_name",
		LeftColumn: rds.Column{
			TableId:     table2,
			ColumnId:    "sales_order_name",
			ColumnIndex: 2,
		},
		RightColumn: rds.Column{
			TableId:     table1,
			ColumnId:    "sales_order_name",
			ColumnIndex: 0,
		},
		ConstantValue: nil,
		PredicateType: 2,
	}
	p1 := rds.Predicate{
		PredicateStr: "t0.consumption_tax_rate=t1.consumption_tax_rate",
		LeftColumn: rds.Column{
			TableId:     table1,
			ColumnId:    "consumption_tax_rate",
			ColumnIndex: 0,
		},
		RightColumn: rds.Column{
			TableId:     table1,
			ColumnId:    "consumption_tax_rate",
			ColumnIndex: 1,
		},
		ConstantValue: nil,
		PredicateType: 1,
	}
	p2 := rds.Predicate{
		PredicateStr: " t2.tax=t3.tax",
		LeftColumn: rds.Column{
			TableId:     table2,
			ColumnId:    "tax",
			ColumnIndex: 2,
		},
		RightColumn: rds.Column{
			TableId:     table2,
			ColumnId:    "tax",
			ColumnIndex: 3,
		},
		ConstantValue: nil,
		PredicateType: 1,
	}

	rhs := rds.Predicate{
		PredicateStr: "t0.customer_name=t1.customer_name",
		LeftColumn: rds.Column{
			TableId:     table1,
			ColumnId:    "customer_name",
			ColumnIndex: 0,
		},
		RightColumn: rds.Column{
			TableId:     table1,
			ColumnId:    "customer_name",
			ColumnIndex: 1,
		},
		ConstantValue: nil,
		PredicateType: 1,
	}

	return []rds.Predicate{f1, f2, p1, p2}, rhs
}

func getPredicates4(table1 string, table2 string) ([]rds.Predicate, rds.Predicate) {
	f1 := rds.Predicate{
		PredicateStr: "t3.sales_order_name = t1.sales_order_name",
		LeftColumn: rds.Column{
			TableId:     table2,
			ColumnId:    "sales_order_name",
			ColumnIndex: 3,
		},
		RightColumn: rds.Column{
			TableId:     table1,
			ColumnId:    "sales_order_name",
			ColumnIndex: 1,
		},
		ConstantValue: nil,
		PredicateType: 2,
	}
	f2 := rds.Predicate{
		PredicateStr: "t2.sales_order_name = t0.sales_order_name",
		LeftColumn: rds.Column{
			TableId:     table2,
			ColumnId:    "sales_order_name",
			ColumnIndex: 2,
		},
		RightColumn: rds.Column{
			TableId:     table1,
			ColumnId:    "sales_order_name",
			ColumnIndex: 0,
		},
		ConstantValue: nil,
		PredicateType: 2,
	}
	//p1 := rds.Predicate{
	//	PredicateStr: "t0.mailing_address=t1.mailing_address",
	//	LeftColumn: rds.Column{
	//		TableId:     table1,
	//		ColumnId:    "mailing_address",
	//		ColumnIndex: 0,
	//	},
	//	RightColumn: rds.Column{
	//		TableId:     table1,
	//		ColumnId:    "mailing_address",
	//		ColumnIndex: 1,
	//	},
	//	ConstantValue: nil,
	//	PredicateType: 1,
	//}
	//p2 := rds.Predicate{
	//	PredicateStr: " t2.mailing_address=t3.mailing_address",
	//	LeftColumn: rds.Column{
	//		TableId:     table2,
	//		ColumnId:    "mailing_address",
	//		ColumnIndex: 2,
	//	},
	//	RightColumn: rds.Column{
	//		TableId:     table2,
	//		ColumnId:    "mailing_address",
	//		ColumnIndex: 3,
	//	},
	//	ConstantValue: nil,
	//	PredicateType: 1,
	//}
	//p3 := rds.Predicate{
	//	PredicateStr: " t2.selling_price_list=一般客户折扣",
	//	LeftColumn: rds.Column{
	//		TableId:     table2,
	//		ColumnId:    "selling_price_list",
	//		ColumnIndex: 2,
	//	},
	//	RightColumn: rds.Column{
	//		TableId:     "",
	//		ColumnId:    "",
	//		ColumnIndex: 0,
	//	},
	//	ConstantValue: "一般客户折扣",
	//	PredicateType: 0,
	//}
	//
	//p4 := rds.Predicate{
	//	PredicateStr: " t3.selling_price_list=一般客户折扣",
	//	LeftColumn: rds.Column{
	//		TableId:     table2,
	//		ColumnId:    "selling_price_list",
	//		ColumnIndex: 2,
	//	},
	//	RightColumn: rds.Column{
	//		TableId:     "",
	//		ColumnId:    "",
	//		ColumnIndex: 0,
	//	},
	//	ConstantValue: "一般客户折扣",
	//	PredicateType: 0,
	//}

	//rhs := rds.Predicate{
	//	PredicateStr: "t0.customer_name=t1.customer_name",
	//	LeftColumn: rds.Column{
	//		TableId:     table1,
	//		ColumnId:    "customer_name",
	//		ColumnIndex: 0,
	//	},
	//	RightColumn: rds.Column{
	//		TableId:     table1,
	//		ColumnId:    "customer_name",
	//		ColumnIndex: 1,
	//	},
	//	ConstantValue: nil,
	//	PredicateType: 1,
	//}

	rhs := rds.Predicate{
		PredicateStr: "t0.shipping_address_name = t1.shipping_address_name ",
		LeftColumn: rds.Column{
			TableId:     table1,
			ColumnId:    "shipping_address_name ",
			ColumnIndex: 0,
		},
		RightColumn: rds.Column{
			TableId:     table1,
			ColumnId:    "shipping_address_name ",
			ColumnIndex: 1,
		},
		ConstantValue: nil,
		PredicateType: 1,
	}

	return []rds.Predicate{f1, f2}, rhs
}

func Test_generate_rule(t *testing.T) {
	table1 := "1706152512138"
	table2 := "1706152504047"
	lhs, rhs := getPredicates1(table1, table2)
	var ree = utils2.GetLhsStr(lhs) + "->" + rhs.PredicateStr
	rule := rds.Rule{
		Ree:           ree,
		LhsPredicates: lhs,
		Rhs:           rhs,
	}
	ruleJson := utils2.GetRuleJson(rule)
	fmt.Println(ruleJson)

}

func Test_graphPathUnique(t *testing.T) {
	//table1 := "1706152512138"
	//table2 := "1706152504047"
	//lhs, _ := getPredicates1(table1, table2)
	//unique := graphPathUnique(lhs)
	//for _, predicate := range unique {
	//	fmt.Println(predicate.PredicateStr)
	//}

	var aaa = []int{0}
	fmt.Println(aaa[0])
	for _, a := range aaa[1:] {
		fmt.Println(a)
	}
}

// 多集合实现笛卡尔
func Test_slices_cartesian(t *testing.T) {
	var tmp1 = [][]int32{0: {0, 1}, 1: {4, 5, 6}, 2: {7, 8}}
	var tmp2 = [][]int32{0: {0, 1}, 1: {4, 5, 6}, 2: {7, 8}}
	var joinTable1 = [][][]int32{tmp1, tmp2}
	var joinTable2 = [][][]int32{tmp1}
	result := MultiSliceCartesian([][][][]int32{joinTable1, joinTable2})
	for idx, row := range result {
		fmt.Printf("第%d张大宽表\n", idx)
		for index, val := range row {
			fmt.Println(index, val)
		}
	}

	//var joinTable = [][][]int32{tmp1, tmp2}
	//cartesian := make([][]int32, 0)
	//for _, idPairs := range joinTable {
	//	tmp := test1(idPairs)
	//	cartesian = append(cartesian, tmp...)
	//}
	//// 打印
	//for index, val := range cartesian {
	//	fmt.Println(index, val)
	//}
}

func test1(datalist [][]int32) [][]int32 {
	list1 := datalist[0]
	twoDim := make([][]int32, len(list1))
	for idx, item := range list1 {
		twoDim[idx] = []int32{item}
	}
	for _, items := range datalist[1:] {
		result := test2(twoDim, items)
		twoDim = result
	}
	return twoDim
}

func test2(list1 [][]int32, list2 []int32) [][]int32 {
	var res [][]int32

	for _, item1 := range list1 {
		for _, item2 := range list2 {
			tmpItem := make([]int32, 0, len(item1))
			tmpItem = append(tmpItem, item1...)
			tmpItem = append(tmpItem, item2)
			res = append(res, tmpItem)
		}
	}
	return res
}

func Test_fiter_row(t *testing.T) {
	rowValues := make([]int, 100000000)
	for i := 0; i < 100000000; i++ {
		rowValues[i] = i
	}
	values := map[string]map[string][]int{
		"table1": {"column1": rowValues},
	}
	rows := values["table1"]["column1"]
	newRows := make([]int, 0)
	//for id, row := range rows {
	//	if row%1000 == 0 {
	//		rows = append(rows[:id], rows[id+1:]...)
	//	}
	//}
	//values["table1"]["column1"] = rows
	for _, row := range rows {
		if row%1000 == 0 {
			continue
		}
		newRows = append(newRows, row)
	}
	values["table1"]["column1"] = newRows
	fmt.Println(len(values["table1"]["column1"]))

}

func Test_SortPredicatesRelatedNew(t *testing.T) {
	//ruleJson := "{\"TableId\":\"1706586407869\",\"Ree\":\"tab_sales_order(t0) ^ tab_sales_order(t1) ^ tab_sales_invoice(t2) ^ t0.sales_order_name = t2.sales_order_name ^ t0.sales_invoice_name = t2.sales_invoice_name ^ t0.selling_price_list = 一般客户折扣 -> t0.customer_name = t1.customer_name \",\"LhsPredicates\":[{\"PredicateStr\":\"t0.sales_order_name = t2.sales_order_name\",\"LeftColumn\":{\"TableId\":\"1706586407869\",\"TableAlias\":\"t0\",\"ColumnId\":\"sales_order_name\",\"ColumnType\":\"string\",\"ColumnIndex\":0},\"RightColumn\":{\"TableId\":\"1706586521309\",\"TableAlias\":\"t2\",\"ColumnId\":\"sales_order_name\",\"ColumnType\":\"string\",\"ColumnIndex\":2},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":2,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0},{\"PredicateStr\":\"t0.sales_invoice_name = t2.sales_invoice_name\",\"LeftColumn\":{\"TableId\":\"1706586407869\",\"TableAlias\":\"t0\",\"ColumnId\":\"sales_invoice_name\",\"ColumnType\":\"string\",\"ColumnIndex\":0},\"RightColumn\":{\"TableId\":\"1706586521309\",\"TableAlias\":\"t2\",\"ColumnId\":\"sales_invoice_name\",\"ColumnType\":\"string\",\"ColumnIndex\":2},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":2,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0},{\"PredicateStr\":\"t0.selling_price_list = 一般客户折扣\",\"LeftColumn\":{\"TableId\":\"1706586407869\",\"TableAlias\":\"t0\",\"ColumnId\":\"selling_price_list\",\"ColumnType\":\"string\",\"ColumnIndex\":0},\"RightColumn\":{\"TableId\":\"\",\"TableAlias\":\"\",\"ColumnId\":\"\",\"ColumnType\":\"\",\"ColumnIndex\":0},\"GroupByColumn\":null,\"ConstantValue\":\"一般客户折扣\",\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":0,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0}],\"LhsColumns\":[{\"TableId\":\"1706586407869\",\"TableAlias\":\"t0\",\"ColumnId\":\"sales_order_name\",\"ColumnType\":\"string\",\"ColumnIndex\":0},{\"TableId\":\"1706586521309\",\"TableAlias\":\"t2\",\"ColumnId\":\"sales_order_name\",\"ColumnType\":\"string\",\"ColumnIndex\":2},{\"TableId\":\"1706586407869\",\"TableAlias\":\"t0\",\"ColumnId\":\"sales_invoice_name\",\"ColumnType\":\"string\",\"ColumnIndex\":0},{\"TableId\":\"1706586521309\",\"TableAlias\":\"t2\",\"ColumnId\":\"sales_invoice_name\",\"ColumnType\":\"string\",\"ColumnIndex\":2},{\"TableId\":\"1706586407869\",\"TableAlias\":\"t0\",\"ColumnId\":\"selling_price_list\",\"ColumnType\":\"string\",\"ColumnIndex\":0}],\"Rhs\":{\"PredicateStr\":\"t0.customer_name = t1.customer_name\",\"LeftColumn\":{\"TableId\":\"1706586407869\",\"TableAlias\":\"t0\",\"ColumnId\":\"customer_name\",\"ColumnType\":\"string\",\"ColumnIndex\":0},\"RightColumn\":{\"TableId\":\"1706586407869\",\"TableAlias\":\"t1\",\"ColumnId\":\"customer_name\",\"ColumnType\":\"string\",\"ColumnIndex\":1},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":3,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0},\"RhsColumn\":{\"TableId\":\"1706586407869\",\"TableAlias\":\"t0\",\"ColumnId\":\"customer_name\",\"ColumnType\":\"string\",\"ColumnIndex\":0},\"CR\":0,\"FTR\":0,\"RuleType\":0,\"XSupp\":0,\"XySupp\":0,\"XSatisfyCount\":0,\"CfdStatus\":0}"
	ruleJson := "{\"TableId\":\"1701844220997\",\"Ree\":\"tabLoan(t0)^tabLoan(t1)^t0.loan_application=t1.loan_application->t0.company=t1.company\",\"LhsPredicates\":[{\"PredicateStr\":\"t0.loan_application=t1.loan_application\",\"LeftColumn\":{\"TableId\":\"1701844220997\",\"TableAlias\":\"t0\",\"ColumnId\":\"loan_application\",\"ColumnType\":\"string\"},\"RightColumn\":{\"TableId\":\"1701844220997\",\"TableAlias\":\"t1\",\"ColumnId\":\"loan_application\",\"ColumnType\":\"string\"},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":2,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0}],\"LhsColumns\":[{\"TableId\":\"1701844220997\",\"TableAlias\":\"t0\",\"ColumnId\":\"loan_application\",\"ColumnType\":\"string\"},{\"TableId\":\"1701844220997\",\"TableAlias\":\"t1\",\"ColumnId\":\"loan_application\",\"ColumnType\":\"string\"}],\"Rhs\":{\"PredicateStr\":\"t0.company=t1.company\",\"LeftColumn\":{\"TableId\":\"1701844220997\",\"TableAlias\":\"t0\",\"ColumnId\":\"company\",\"ColumnType\":\"string\"},\"RightColumn\":{\"TableId\":\"1701844220997\",\"TableAlias\":\"t1\",\"ColumnId\":\"company\",\"ColumnType\":\"string\"},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":3,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0},\"RhsColumn\":{\"TableId\":\"1701844220997\",\"TableAlias\":\"t0\",\"ColumnId\":\"company\",\"ColumnType\":\"string\"},\"CR\":0,\"FTR\":0,\"RuleType\":0,\"XSupp\":0,\"XySupp\":0,\"XSatisfyCount\":0,\"CfdStatus\":0}"
	rule := utils2.GetFormatRule(ruleJson)
	sortedPredicates := utils2.SortPredicatesRelatedNew(rule.LhsPredicates)
	for _, p := range sortedPredicates {
		fmt.Println(p.PredicateStr)
	}
}
