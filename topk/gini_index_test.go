package topk

import (
	"fmt"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock_v3/rds_config"
	"gitlab.grandhoo.com/rock/rock_v3/utils/train_data_util"
	"testing"
)

func getTable1() (string, map[string]map[string][]interface{}, map[string]string) {
	tableName := "Table1"
	data := map[string]map[string][]interface{}{
		"t0": {
			"a": {"a1", "a2", "a1", "a3"},
			"b": {int64(1), int64(2), int64(3), int64(4)},
			"c": {10.1, 20.2, 30.3, 40.4},
			"d": {true, false, false, true},
			"e": {"e1", "e2", "e1", "e3"},
			"f": {int64(100), int64(200), int64(300), int64(400)},
			"g": {1000.1, 2000.2, 3000.3, 4000.4},
			"h": {false, true, true, false},
		},
	}
	dataType := map[string]string{
		"a": rds_config.EnumType,
		"b": rds_config.IntType,
		"c": rds_config.FloatType,
		"d": rds_config.BoolType,
		"e": rds_config.EnumType,
		"f": rds_config.IntType,
		"g": rds_config.FloatType,
		"h": rds_config.BoolType,
	}
	return tableName, data, dataType
}

func getTable2() (string, map[string]map[string][]interface{}, map[string]string) {
	tableName := "Table2"
	data := map[string]map[string][]interface{}{
		"t0": {
			"A": {"A1", "A2", "A1", "A3"},
			"B": {int64(1), int64(2), int64(3), int64(4)},
			"C": {10.1, 20.2, 30.3, 40.4},
			"D": {true, false, false, true},
			"E": {"e1", "e2", "e1", "e3"},
			"F": {int64(100), int64(200), int64(300), int64(400)},
			"G": {1000.1, 2000.2, 3000.3, 4000.4},
			"H": {false, true, true, false},
		},
	}
	dataType := map[string]string{
		"A": rds_config.EnumType,
		"B": rds_config.IntType,
		"C": rds_config.FloatType,
		"D": rds_config.BoolType,
		"E": rds_config.EnumType,
		"F": rds_config.IntType,
		"G": rds_config.FloatType,
		"H": rds_config.BoolType,
	}
	return tableName, data, dataType
}

func generateLhs1() []rds.Predicate {
	var result []rds.Predicate
	p1 := rds.Predicate{
		PredicateStr: "t0.a=a1",
		LeftColumn: rds.Column{
			TableId:    "Table1",
			ColumnId:   "a",
			ColumnType: rds_config.EnumType,
		},
		ConstantValue: "a1",
		PredicateType: 0,
		SymbolType:    rds_config.Equal,
	}
	result = append(result, p1)
	p2 := rds.Predicate{
		PredicateStr: "t0.b=1",
		LeftColumn: rds.Column{
			TableId:    "Table1",
			ColumnId:   "b",
			ColumnType: rds_config.IntType,
		},
		ConstantValue: int64(1),
		PredicateType: 0,
		SymbolType:    rds_config.Equal,
	}
	result = append(result, p2)
	p3 := rds.Predicate{
		PredicateStr: "t0.c=10.1",
		LeftColumn: rds.Column{
			TableId:    "Table1",
			ColumnId:   "c",
			ColumnType: rds_config.FloatType,
		},
		ConstantValue: 10.1,
		PredicateType: 0,
		SymbolType:    rds_config.Equal,
	}
	result = append(result, p3)
	p4 := rds.Predicate{
		PredicateStr: "t0.d=true",
		LeftColumn: rds.Column{
			TableId:    "Table1",
			ColumnId:   "d",
			ColumnType: rds_config.BoolType,
		},
		ConstantValue: true,
		PredicateType: 0,
		SymbolType:    rds_config.Equal,
	}
	result = append(result, p4)
	return result
}

func generateLhs2() []rds.Predicate {
	var result []rds.Predicate
	p1 := rds.Predicate{
		PredicateStr: "t0.a=t1.a",
		LeftColumn: rds.Column{
			TableId:    "Table1",
			ColumnId:   "a",
			ColumnType: rds_config.EnumType,
		},
		RightColumn: rds.Column{
			TableId:    "Table1",
			ColumnId:   "a",
			ColumnType: rds_config.EnumType,
		},
		ConstantValue: nil,
		PredicateType: 1,
		SymbolType:    rds_config.Equal,
	}
	result = append(result, p1)
	p2 := rds.Predicate{
		PredicateStr: "t0.b=t1.b",
		LeftColumn: rds.Column{
			TableId:    "Table1",
			ColumnId:   "b",
			ColumnType: rds_config.IntType,
		},
		RightColumn: rds.Column{
			TableId:    "Table1",
			ColumnId:   "b",
			ColumnType: rds_config.IntType,
		},
		ConstantValue: nil,
		PredicateType: 1,
		SymbolType:    rds_config.Equal,
	}
	result = append(result, p2)
	p3 := rds.Predicate{
		PredicateStr: "t0.c=t1.c",
		LeftColumn: rds.Column{
			TableId:    "Table1",
			ColumnId:   "c",
			ColumnType: rds_config.FloatType,
		},
		RightColumn: rds.Column{
			TableId:    "Table1",
			ColumnId:   "c",
			ColumnType: rds_config.FloatType,
		},
		ConstantValue: nil,
		PredicateType: 1,
		SymbolType:    rds_config.Equal,
	}
	result = append(result, p3)
	p4 := rds.Predicate{
		PredicateStr: "t0.d=t1.d",
		LeftColumn: rds.Column{
			TableId:    "Table1",
			ColumnId:   "d",
			ColumnType: rds_config.BoolType,
		},
		RightColumn: rds.Column{
			TableId:    "Table1",
			ColumnId:   "d",
			ColumnType: rds_config.BoolType,
		},
		ConstantValue: nil,
		PredicateType: 1,
		SymbolType:    rds_config.Equal,
	}
	result = append(result, p4)
	return result
}

// 普通单行规则
func TestNormalSingleRule(t *testing.T) {
	tableName, data, dataType := getTable1()
	addPredicates := generateLhs1()
	rhs1 := rds.Predicate{
		PredicateStr: "t0.e=e1",
		LeftColumn: rds.Column{
			TableId:    tableName,
			ColumnId:   "e",
			ColumnType: rds_config.EnumType,
		},
		ConstantValue: "e1",
		PredicateType: 0,
		SymbolType:    rds_config.Equal,
	}
	columns, trainData, _, _ := train_data_util.GenerateTrainData([]map[string]map[string][]interface{}{data}, []string{tableName}, nil, map[string]map[string]string{tableName: dataType}, true, false, rhs1, map[string]float64{"t0": 1.0})
	fmt.Println(columns)
	for _, d := range trainData {
		fmt.Println(d)
	}
	gini := CalGiniIndexes(addPredicates, rhs1, trainData, columns)
	for _, info := range gini {
		fmt.Println(info)
	}

	rhs2 := rds.Predicate{
		PredicateStr: "t0.f=100",
		LeftColumn: rds.Column{
			TableId:    tableName,
			ColumnId:   "f",
			ColumnType: rds_config.IntType,
		},
		ConstantValue: int64(100),
		PredicateType: 0,
	}
	columns, trainData, _, _ = train_data_util.GenerateTrainData([]map[string]map[string][]interface{}{data}, []string{tableName}, nil, map[string]map[string]string{tableName: dataType}, true, false, rhs2, map[string]float64{"t0": 1.0})
	fmt.Println(columns)
	for _, d := range trainData {
		fmt.Println(d)
	}
	gini = CalGiniIndexes(addPredicates, rhs2, trainData, columns)
	for _, info := range gini {
		fmt.Println(info)
	}

	rhs3 := rds.Predicate{
		PredicateStr: "t0.g=1000.1",
		LeftColumn: rds.Column{
			TableId:    tableName,
			ColumnId:   "g",
			ColumnType: rds_config.FloatType,
		},
		ConstantValue: 1000.1,
		PredicateType: 0,
	}
	columns, trainData, _, _ = train_data_util.GenerateTrainData([]map[string]map[string][]interface{}{data}, []string{tableName}, nil, map[string]map[string]string{tableName: dataType}, true, false, rhs3, map[string]float64{"t0": 1.0})
	fmt.Println(columns)
	for _, d := range trainData {
		fmt.Println(d)
	}
	gini = CalGiniIndexes(addPredicates, rhs3, trainData, columns)
	for _, info := range gini {
		fmt.Println(info)
	}

	rhs4 := rds.Predicate{
		PredicateStr: "t0.h=false",
		LeftColumn: rds.Column{
			TableId:    tableName,
			ColumnId:   "h",
			ColumnType: rds_config.BoolType,
		},
		ConstantValue: false,
		PredicateType: 0,
	}
	columns, trainData, _, _ = train_data_util.GenerateTrainData([]map[string]map[string][]interface{}{data}, []string{tableName}, nil, map[string]map[string]string{tableName: dataType}, true, false, rhs4, map[string]float64{"t0": 1.0})
	fmt.Println(columns)
	for _, d := range trainData {
		fmt.Println(d)
	}
	gini = CalGiniIndexes(addPredicates, rhs4, trainData, columns)
	for _, info := range gini {
		fmt.Println(info)
	}
}

// 单表多行规则
func TestSingleTableMultiRowRule(t *testing.T) {
	tableName, data, dataType := getTable1()
	addPredicates := generateLhs2()
	data["t1"] = data["t0"]
	rhs1 := rds.Predicate{
		PredicateStr: "t0.e=t1.e",
		LeftColumn: rds.Column{
			TableId:    tableName,
			ColumnId:   "e",
			ColumnType: "enum",
		},
		RightColumn: rds.Column{
			TableId:    tableName,
			ColumnId:   "e",
			ColumnType: "enum",
		},
		ConstantValue: "",
		PredicateType: 1,
		SymbolType:    rds_config.Equal,
	}
	index2table := map[string]string{
		"t0": tableName,
		"t1": tableName,
	}
	columns, trainData, _, _ := train_data_util.GenerateTrainData([]map[string]map[string][]interface{}{data}, []string{tableName}, index2table, map[string]map[string]string{tableName: dataType}, true, false, rhs1, map[string]float64{"t0": 1, "t1": 1})
	fmt.Println(columns)
	for _, d := range trainData {
		fmt.Println(d)
	}
	gini := CalGiniIndexes(addPredicates, rhs1, trainData, columns)
	for _, info := range gini {
		fmt.Println(info)
	}

	tableName, data, dataType = getTable1()
	data["t1"] = data["t0"]
	rhs2 := rds.Predicate{
		PredicateStr: "t0.f=t1.f",
		LeftColumn: rds.Column{
			TableId:    tableName,
			ColumnId:   "f",
			ColumnType: rds_config.IntType,
		},
		RightColumn: rds.Column{
			TableId:    tableName,
			ColumnId:   "f",
			ColumnType: rds_config.IntType,
		},
		ConstantValue: "",
		PredicateType: 1,
		SymbolType:    rds_config.Equal,
	}
	columns, trainData, _, _ = train_data_util.GenerateTrainData([]map[string]map[string][]interface{}{data}, []string{tableName}, index2table, map[string]map[string]string{tableName: dataType}, true, false, rhs2, map[string]float64{"t0": 1, "t1": 1})
	fmt.Println(columns)
	for _, d := range trainData {
		fmt.Println(d)
	}
	gini = CalGiniIndexes(addPredicates, rhs2, trainData, columns)
	for _, info := range gini {
		fmt.Println(info)
	}
}

func TestMultiTableMultiRowRule(t *testing.T) {
	addPredicates := generateLhs2()
	tableName1, data1, dataType1 := getTable1()
	tableName2, data2, dataType2 := getTable2()
	rhs1 := rds.Predicate{
		PredicateStr: "t0.a=t2.A",
		LeftColumn: rds.Column{
			TableId:    tableName1,
			ColumnId:   "a",
			ColumnType: "enum",
		},
		RightColumn: rds.Column{
			TableId:    tableName2,
			ColumnId:   "A",
			ColumnType: "enum",
		},
		ConstantValue: "",
		PredicateType: 3,
		SymbolType:    rds_config.Equal,
	}
	data := make(map[string]map[string][]interface{})
	data["t0"] = data1["t0"]
	data["t1"] = data1["t0"]
	data["t2"] = data2["t0"]
	data["t3"] = data2["t0"]
	tableNames := []string{tableName1, tableName2}
	index2table := map[string]string{
		"t0": tableName1,
		"t1": tableName1,
		"t2": tableName2,
		"t3": tableName2,
	}
	dataType := map[string]map[string]string{tableName1: dataType1, tableName2: dataType2}
	filterRatio := map[string]float64{"t0": 1, "t1": 1, "t2": 1, "t3": 1}
	columns, trainData, _, _ := train_data_util.GenerateTrainData([]map[string]map[string][]interface{}{data}, tableNames, index2table, dataType, true, false, rhs1, filterRatio)
	fmt.Println(columns)
	for _, d := range trainData {
		fmt.Println(d)
	}
	gini := CalGiniIndexes(addPredicates, rhs1, trainData, columns)
	for _, info := range gini {
		fmt.Println(info)
	}

	rhs2 := rds.Predicate{
		PredicateStr: "t0.e=t2.E",
		LeftColumn: rds.Column{
			TableId:    tableName1,
			ColumnId:   "e",
			ColumnType: "enum",
		},
		RightColumn: rds.Column{
			TableId:    tableName2,
			ColumnId:   "E",
			ColumnType: "enum",
		},
		ConstantValue: "",
		PredicateType: 3,
		SymbolType:    rds_config.Equal,
	}
	columns, trainData, _, _ = train_data_util.GenerateTrainData([]map[string]map[string][]interface{}{data}, tableNames, index2table, dataType, true, false, rhs2, filterRatio)
	fmt.Println(columns)
	for _, d := range trainData {
		fmt.Println(d)
	}
	gini = CalGiniIndexes(addPredicates, rhs1, trainData, columns)
	for _, info := range gini {
		fmt.Println(info)
	}
}
