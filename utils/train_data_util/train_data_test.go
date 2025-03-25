package train_data_util

import (
	"fmt"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock_v3/rds_config"
	"testing"
)

func getTable1() (string, map[string]map[string][]interface{}, map[string]string) {
	tableName := "Table1"
	data := map[string]map[string][]interface{}{
		"t0": {
			"a": {"a1", "a2", "a1", "a3"},
			"b": {int64(1), int64(2), int64(3), int64(4)},
			"c": {nil, 20.2, 30.3, 40.4},
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

// 普通单行规则
func TestNormalSingleRule(t *testing.T) {
	tableName, data, dataType := getTable1()
	rhs1 := rds.Predicate{
		PredicateStr: "t0.e=e1",
		LeftColumn: rds.Column{
			TableId:  tableName,
			ColumnId: "e",
		},
		ConstantValue: "e1",
		PredicateType: 0,
	}
	columns, trainData, _, _ := GenerateTrainData([]map[string]map[string][]interface{}{data}, []string{tableName}, nil, map[string]map[string]string{tableName: dataType}, true, false, rhs1, map[string]float64{"t0": 1.0})
	fmt.Println(columns)
	for _, d := range trainData {
		fmt.Println(d)
	}

	rhs2 := rds.Predicate{
		PredicateStr: "t0.f=100",
		LeftColumn: rds.Column{
			TableId:  tableName,
			ColumnId: "f",
		},
		ConstantValue: int64(100),
		PredicateType: 0,
	}
	columns, trainData, _, _ = GenerateTrainData([]map[string]map[string][]interface{}{data}, []string{tableName}, nil, map[string]map[string]string{tableName: dataType}, true, false, rhs2, map[string]float64{"t0": 1.0})
	fmt.Println(columns)
	for _, d := range trainData {
		fmt.Println(d)
	}

	rhs3 := rds.Predicate{
		PredicateStr: "t0.g=1000.1",
		LeftColumn: rds.Column{
			TableId:  tableName,
			ColumnId: "g",
		},
		ConstantValue: 1000.1,
		PredicateType: 0,
	}
	columns, trainData, _, _ = GenerateTrainData([]map[string]map[string][]interface{}{data}, []string{tableName}, nil, map[string]map[string]string{tableName: dataType}, true, false, rhs3, map[string]float64{"t0": 1.0})
	fmt.Println(columns)
	for _, d := range trainData {
		fmt.Println(d)
	}

	rhs4 := rds.Predicate{
		PredicateStr: "t0.h=false",
		LeftColumn: rds.Column{
			TableId:  tableName,
			ColumnId: "h",
		},
		ConstantValue: false,
		PredicateType: 0,
	}
	columns, trainData, _, _ = GenerateTrainData([]map[string]map[string][]interface{}{data}, []string{tableName}, nil, map[string]map[string]string{tableName: dataType}, true, false, rhs4, map[string]float64{"t0": 1.0})
	fmt.Println(columns)
	for _, d := range trainData {
		fmt.Println(d)
	}

}

// 多项式单行规则
func TestPolyRule(t *testing.T) {
	tableName, data, dataType := getTable1()
	rhs1 := rds.Predicate{
		PredicateStr: "t0.b",
		LeftColumn: rds.Column{
			TableId:  tableName,
			ColumnId: "",
		},
		ConstantValue: "",
		PredicateType: 0,
		SymbolType:    rds_config.Poly,
	}
	columns, trainData, _, _ := GenerateTrainData([]map[string]map[string][]interface{}{data}, []string{tableName}, nil, map[string]map[string]string{tableName: dataType}, false, false, rhs1, map[string]float64{"t0": 0.5})
	fmt.Println(columns)
	for _, d := range trainData {
		fmt.Println(d)
	}

	tableName, data, dataType = getTable1()
	rhs2 := rds.Predicate{
		PredicateStr: "t0.b+t0.c",
		LeftColumn: rds.Column{
			TableId:  tableName,
			ColumnId: "",
		},
		ConstantValue: "",
		PredicateType: 0,
		SymbolType:    rds_config.Poly,
	}
	columns, trainData, _, _ = GenerateTrainData([]map[string]map[string][]interface{}{data}, []string{tableName}, nil, map[string]map[string]string{tableName: dataType}, false, false, rhs2, map[string]float64{"t0": 1.0})
	fmt.Println(columns)
	for _, d := range trainData {
		fmt.Println(d)
	}

	tableName, data, dataType = getTable1()
	rhs3 := rds.Predicate{
		PredicateStr: "-t0.f-t0.b+t0.c",
		LeftColumn: rds.Column{
			TableId:  tableName,
			ColumnId: "",
		},
		ConstantValue: "",
		PredicateType: 0,
		SymbolType:    rds_config.Poly,
	}
	columns, trainData, _, _ = GenerateTrainData([]map[string]map[string][]interface{}{data}, []string{tableName}, nil, map[string]map[string]string{tableName: dataType}, false, false, rhs3, map[string]float64{"t0": 1.0})
	fmt.Println(columns)
	for _, d := range trainData {
		fmt.Println(d)
	}
}

// 单表多行规则
func TestSingleTableMultiRowRule(t *testing.T) {
	tableName, data, dataType := getTable1()
	data["t1"] = data["t0"]
	rhs1 := rds.Predicate{
		PredicateStr: "t0.a=t1.a",
		LeftColumn: rds.Column{
			TableId:    tableName,
			ColumnId:   "a",
			ColumnType: "enum",
		},
		RightColumn: rds.Column{
			TableId:    tableName,
			ColumnId:   "a",
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
	columns, trainData, _, _ := GenerateTrainData([]map[string]map[string][]interface{}{data}, []string{tableName}, index2table, map[string]map[string]string{tableName: dataType}, true, false, rhs1, map[string]float64{"t0": 0.5, "t1": 0.5})
	fmt.Println(columns)
	for _, d := range trainData {
		fmt.Println(d)
	}
}

// 多表多行规则
func TestMultiTableMultiRowRule(t *testing.T) {
	tableName1, data1, dataType1 := getTable1()
	tableName2, data2, dataType2 := getTable2()
	rhs1 := rds.Predicate{
		PredicateStr: "t0.a=t3.A",
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
	columns, trainData, _, _ := GenerateTrainData([]map[string]map[string][]interface{}{data}, tableNames, index2table, dataType, true, false, rhs1, filterRatio)
	fmt.Println(columns)
	for _, d := range trainData {
		fmt.Println(d)
	}

	rhs2 := rds.Predicate{
		PredicateStr: "t0.e=t3.E",
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
	columns, trainData, _, _ = GenerateTrainData([]map[string]map[string][]interface{}{data}, tableNames, index2table, dataType, true, false, rhs2, filterRatio)
	fmt.Println(columns)
	for _, d := range trainData {
		fmt.Println(d)
	}
}
