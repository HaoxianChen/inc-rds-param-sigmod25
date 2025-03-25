package train_data_util

import (
	"fmt"
	"github.com/Knetic/govaluate"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock_v3/rds_config"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type indexInfo struct {
	index    string
	beginRow int
	endRow   int
}

func getIndexInfo(tableNames []string) map[string]string {
	index2tableName := make(map[string]string)
	for i, tableName := range tableNames {
		start := i * 2
		index2tableName[fmt.Sprintf("t%d", start)] = tableName
		index2tableName[fmt.Sprintf("t%d", start+1)] = tableName
	}
	return index2tableName
}

func getUniqueData(data []interface{}) []interface{} {
	m := make(map[interface{}]bool)
	hasNil := false
	for _, value := range data {
		if value == nil {
			hasNil = true
			continue
		}
		m[value] = true
	}
	arr := make([]interface{}, len(m))
	i := 0
	for value := range m {
		arr[i] = value
		i++
	}
	if hasNil {
		arr = append(arr, nil)
	}
	return arr
}

func generateSingleRowResultColumns(satisfyData map[string][]interface{}, dataType map[string]string, enableEnum, enableText bool, rhsColumn string, columnUniqueValue map[string]map[string][]interface{}) ([]string, map[string]string) {
	var resultColumns []string
	resultType := make(map[string]string)
	for column, columnType := range dataType {
		// 时间类型暂时跳过
		if columnType == rds_config.TimeType {
			continue
		}
		columnStr := fmt.Sprintf("t0.%s", column)
		// y放在最后一列
		if column == rhsColumn {
			continue
		}
		// 数值类型
		if columnType == rds_config.FloatType || columnType == rds_config.IntType {
			resultColumns = append(resultColumns, columnStr)
			resultType[columnStr] = columnType
			continue
		}
		// 枚举类型, 列名转换为key=value的形式
		if (columnType == rds_config.EnumType || columnType == rds_config.BoolType) && enableEnum {
			uniqueData := columnUniqueValue["t0"][column]
			for _, columnValue := range uniqueData {
				// null值不生成
				if columnValue == nil {
					continue
				}
				if columnType == rds_config.BoolType {
					columnValue = strconv.FormatBool(columnValue.(bool))
				}
				columnStrT := fmt.Sprintf("%s%s%s", columnStr, rds_config.Equal, columnValue)
				resultColumns = append(resultColumns, columnStrT)
				resultType[columnStrT] = columnType
			}
			continue
		}
		// 字符串类型
		// TODO 文本类型肯定不是直接用原列名,具体方案后续再添加
		if columnType == rds_config.TextType && enableText {
			resultColumns = append(resultColumns, column)
			resultType[column] = columnType
		}
	}
	return resultColumns, resultType
}

func generatePolyData(satisfyData map[string][]interface{}, expressionStr string, rhsColumns []string) map[string][]interface{} {
	expression, err := govaluate.NewEvaluableExpression(expressionStr)
	if err != nil {
		logger.Errorf("EvaluableExpression err:%v", err)
		return nil
	}
	rowSize := len(satisfyData[rhsColumns[0]])
	satisfyData[expressionStr] = make([]interface{}, rowSize)
	for i := 0; i < rowSize; i++ {
		variables := make(map[string]interface{}, len(rhsColumns))
		for _, columnName := range rhsColumns {
			variables[columnName] = satisfyData[columnName][i]
		}
		result, err := expression.Evaluate(variables)
		if err != nil {
			logger.Errorf("Evaluable err:%v", err)
			return nil
		}
		satisfyData[expressionStr][i] = result
	}
	for _, columnName := range rhsColumns {
		delete(satisfyData, columnName)
	}
	return satisfyData
}

func generatePolyExpression(predicateStr string) (string, []string) {
	str := ""
	var columns []string
	// 定义正则表达式模式
	pattern := `([+-])?(t[0-9]+)\.([a-zA-Z]+)`
	// 编译正则表达式
	regExp := regexp.MustCompile(pattern)
	// 使用正则表达式匹配多项式
	matches := regExp.FindAllStringSubmatch(predicateStr, -1)
	// 提取匹配结果
	for _, match := range matches {
		// 提取符号、表索引和变量
		sign := match[1]
		//table := match[2]
		variable := match[3]
		//exponent := match[5]
		str = fmt.Sprintf("%s%s%s", str, sign, variable)
		columns = append(columns, variable)
	}
	return str, columns
}

func getPredicateColumnInfo(predicateStr string, symbolType string) (string, string, string, string) {
	split := strings.Split(predicateStr, symbolType)
	leftStr := split[0]
	rightStr := split[1]
	return strings.Split(leftStr, ".")[0], strings.Split(leftStr, ".")[1], strings.Split(rightStr, ".")[0], strings.Split(rightStr, ".")[1]
}

func getPredicateColumnInfoNew(p rds.Predicate) (string, string, string, string) {
	return "t" + strconv.Itoa(p.LeftColumn.ColumnIndex), p.LeftColumn.ColumnId, "t" + strconv.Itoa(p.RightColumn.ColumnIndex), p.RightColumn.ColumnId
}

// 生成多行规则训练集的表头
func generateHeader(satisfyData map[string]map[string][]interface{}, index2table map[string]string, allDataType map[string]map[string]string, enableEnum, enableText bool, leftIndex, leftColumn, rightIndex, rightColumn string, rhs rds.Predicate, columnUniqueValue map[string]map[string][]interface{}) ([]string, map[string]string) {
	var rhsColumnType string
	var resultColumns []string
	i := 0
	resultType := make(map[string]string)
	for tableIndex, indexValues := range satisfyData {
		tableName := index2table[tableIndex]
		columnsType := allDataType[tableName]
		isLeftIndex := tableIndex == leftIndex
		isRightIndex := tableIndex == rightIndex
		for columnName := range indexValues {
			columnType := columnsType[columnName]
			// 时间类型暂时跳过
			if columnType == rds_config.TimeType {
				continue
			}
			// rhs相关的列不需要生成训练数据
			if isLeftIndex && columnName == leftColumn {
				rhsColumnType = columnType
				continue
			}
			if isRightIndex && columnName == rightColumn {
				continue
			}
			// 数值类型
			if columnType == rds_config.FloatType || columnType == rds_config.IntType {
				columnStr := fmt.Sprintf("%s.%s", tableIndex, columnName)
				resultColumns = append(resultColumns, columnStr)
				i++
				resultType[columnStr] = columnType
				continue
			}
			// 枚举类型的列,抽样前获取枚举值
			if enableEnum {
				uniqueData := columnUniqueValue[tableIndex][columnName]
				for _, columnValue := range uniqueData {
					// null值不生成
					if columnValue == nil {
						continue
					}
					if columnType == rds_config.BoolType {
						columnValue = strconv.FormatBool(columnValue.(bool))
					}
					columnStr := fmt.Sprintf("%s.%s%s%s", tableIndex, columnName, rds_config.Equal, columnValue)
					resultColumns = append(resultColumns, columnStr)
					i++
					resultType[columnStr] = columnType
				}
				continue
			}
			// 文本类型的列
			// todo 暂时未确定方案
			if enableText {

			}
		}
	}
	rhsStr := rhs.PredicateStr
	sort.Strings(resultColumns)
	//if rhsColumnType == rds_config.IntType || rhsColumnType == rds_config.FloatType {
	//	rhsStr = "t" + strconv.Itoa(rhs.LeftColumn.ColumnIndex) + "." + rhs.LeftColumn.ColumnId
	//}
	resultColumns = append(resultColumns, rhsStr)
	resultType[rhsStr] = rhsColumnType
	return resultColumns, resultType
}

// 把索引相关信息生成一个数组
func generateIndexInfoArr(satisfyData map[string]map[string][]interface{}, filterRatios map[string]float64) ([]indexInfo, map[string]int, int) {
	indexInfoArr := make([]indexInfo, len(satisfyData))
	index2pos := make(map[string]int, len(satisfyData))
	i := 0
	resultRowSize := 1
	for tableIndex, indexValues := range satisfyData {
		index2pos[tableIndex] = i
		indexRowSize := 0
		for _, columnValue := range indexValues {
			indexRowSize = len(columnValue)
			break
		}
		rowSize := int(float64(indexRowSize) * filterRatios[tableIndex])
		indexInfoArr[i] = indexInfo{
			index:    tableIndex,
			beginRow: 0,
			endRow:   rowSize,
		}
		resultRowSize *= rowSize
		i++
	}
	return indexInfoArr, index2pos, resultRowSize
}

func getPredicateInfo(str string) (string, string, string) {
	columnSplit := strings.Split(str, ".")
	columnIndex := columnSplit[0]
	tmpSplit := strings.Split(columnSplit[1], rds_config.Equal)
	if len(tmpSplit) < 2 {
		return columnIndex, tmpSplit[0], ""
	} else {
		return columnIndex, tmpSplit[0], tmpSplit[1]
	}
}

func getColumnUnique(data []map[string]map[string][]interface{}, dataType map[string]map[string]string, index2table map[string]string) map[string]map[string][]interface{} {
	result := make(map[string]map[string][]interface{}, len(index2table))
	for i, datum := range data {
		for tableIndex, indexData := range datum {
			if i == 0 {
				result[tableIndex] = make(map[string][]interface{})
			}
			for columnName, columnData := range indexData {
				if dataType[index2table[tableIndex]][columnName] == rds_config.EnumType || dataType[index2table[tableIndex]][columnName] == rds_config.BoolType {
					if i == 0 {
						result[tableIndex][columnName] = getUniqueData(columnData)
					} else {
						result[tableIndex][columnName] = getUniqueData(append(result[tableIndex][columnName], columnData...))
					}
				}
			}
		}
	}
	return result
}
