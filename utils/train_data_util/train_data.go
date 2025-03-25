package train_data_util

import (
	"fmt"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock_v3/rds_config"
	"gitlab.grandhoo.com/rock/rock_v3/utils/storage_utils"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

type CacheValue[E any] struct {
	Val E
	_   [64]byte
}

func GenerateTrainDataParallel(satisfyData []map[string]map[string][]interface{}, tableNames []string, index2table map[string]string, dataType map[string]map[string]string, enableEnum, enableText bool, rhs rds.Predicate, filterRatio map[string]float64, chanSize int, decisionTreeMaxRowSize int) ([]string, [][]float64, map[interface{}]float64, map[string]string) {
	var header []string
	var data [][]float64
	columnType := make(map[string]string)
	rhsValue2index := make(map[interface{}]float64)
	columnUniqueValue := make(map[string]map[string][]interface{})
	t := time.Now().UnixMilli()
	if enableEnum {
		columnUniqueValue = getColumnUnique(satisfyData, dataType, index2table)
	}
	logger.Infof("spent time:%vms, get column unique", time.Now().UnixMilli()-t)

	//leftIndex, leftColumn, rightIndex, rightColumn := getPredicateColumnInfo(rhs.PredicateStr, rhs.SymbolType)
	leftIndex, leftColumn, rightIndex, rightColumn := getPredicateColumnInfoNew(rhs)
	resultColumns, resultType := generateHeader(satisfyData[0], index2table, dataType, enableEnum, enableText, leftIndex, leftColumn, rightIndex, rightColumn, rhs, columnUniqueValue)
	header = resultColumns
	columnType = resultType

	var parallelData = make([]CacheValue[[][]float64], chanSize)
	eachBatchSize := decisionTreeMaxRowSize / len(satisfyData)
	_ = storage_utils.ParallelBatch(0, len(satisfyData), chanSize, func(start, end, workerId int) error {
		for _, datum := range satisfyData[start:end] {
			var dataT [][]float64
			_, dataT, _, _ = generateMultiRowTrainData(datum, tableNames, index2table, dataType, enableEnum, enableText, rhs, filterRatio, columnUniqueValue, eachBatchSize)
			for _, d := range dataT {
				parallelData[workerId].Val = append(parallelData[workerId].Val, d)
			}
		}
		return nil
	})
	for _, tempIntersection := range parallelData {
		data = append(data, tempIntersection.Val...)
		if len(data) > decisionTreeMaxRowSize {
			break
		}
	}
	return header, data, rhsValue2index, columnType
}

func GenerateTrainData(satisfyData []map[string]map[string][]interface{}, tableNames []string, index2table map[string]string, dataType map[string]map[string]string, enableEnum, enableText bool, rhs rds.Predicate, filterRatio map[string]float64) ([]string, [][]float64, map[interface{}]float64, map[string]string) {
	var header []string
	var data [][]float64
	columnType := make(map[string]string)
	rhsValue2index := make(map[interface{}]float64)
	columnUniqueValue := make(map[string]map[string][]interface{})
	t := time.Now().UnixMilli()
	if enableEnum {
		columnUniqueValue = getColumnUnique(satisfyData, dataType, index2table)
	}
	logger.Infof("spent time:%vms, get column unique", time.Now().UnixMilli()-t)
	//eachBatchSize := rds_config.DecisionTreeMaxRowSize / len(satisfyData)

	dataSize := rds_config.DecisionTreeMaxRowSize - len(data)

	for i, datum := range satisfyData {
		var headerT []string
		var dataT [][]float64
		columnTypeT := make(map[string]string)
		rhsValue2indexT := make(map[interface{}]float64)
		// 单行规则
		if len(datum) == 1 {
			if rhs.SymbolType == rds_config.Poly { // 多项式规则
				polyStr, rhsRelatedColumns := generatePolyExpression(rhs.PredicateStr)
				headerT, dataT, rhsValue2indexT, columnTypeT = generatePolySingleRowTrainData(datum["t0"], dataType[tableNames[0]], polyStr, filterRatio["t0"], rhsRelatedColumns, columnUniqueValue)
			} else { // 普通单行规则
				headerT, dataT, rhsValue2indexT, columnTypeT = generateNormalSingleRowTrainData(datum["t0"], dataType[tableNames[0]], enableEnum, enableText, rhs.LeftColumn.ColumnId, rhs.ConstantValue, filterRatio["t0"], columnUniqueValue)
			}
		} else { //多行规则
			headerT, dataT, rhsValue2indexT, columnTypeT = generateMultiRowTrainData(datum, tableNames, index2table, dataType, enableEnum, enableText, rhs, filterRatio, columnUniqueValue, dataSize)
		}
		//addSize := eachBatchSize
		//if len(dataT) < addSize {
		//	addSize = len(dataT)
		//}
		if i == 0 {
			header = headerT
			columnType = columnTypeT
			rhsValue2index = rhsValue2indexT
		}
		//data = append(data, dataT[:addSize]...)
		data = append(data, dataT...)
		dataSize = rds_config.DecisionTreeMaxRowSize - len(data)
	}
	return header, data, rhsValue2index, columnType
}

func generatePolySingleRowTrainData(satisfyData map[string][]interface{}, dataType map[string]string, rhsStr string, filterRatio float64, rhsColumns []string, columnUniqueValue map[string]map[string][]interface{}) ([]string, [][]float64, map[interface{}]float64, map[string]string) {
	if len(rhsColumns) > 1 {
		satisfyData = generatePolyData(satisfyData, rhsStr, rhsColumns)
		for _, columnName := range rhsColumns {
			delete(dataType, columnName)
		}
		dataType[rhsStr] = rds_config.FloatType
	}
	return generateNormalSingleRowTrainData(satisfyData, dataType, false, false, rhsStr, nil, filterRatio, columnUniqueValue)
}

func generateNormalSingleRowTrainData(satisfyData map[string][]interface{}, dataType map[string]string, enableEnum, enableText bool, rhsColumn string, constantValue interface{}, filterRatio float64, columnUniqueValue map[string]map[string][]interface{}) ([]string, [][]float64, map[interface{}]float64, map[string]string) {
	resultColumns, resultType := generateSingleRowResultColumns(satisfyData, dataType, enableEnum, enableText, rhsColumn, columnUniqueValue)
	rhsStr := fmt.Sprintf("t0.%s", rhsColumn)
	rhsValue2index := make(map[interface{}]float64)
	//if dataType[rhsColumn] == rds_config.EnumType {
	//	rhsStr = fmt.Sprintf("t0.%s%s%s", rhsColumn, rds_config.Equal, constantValue)
	//} else if dataType[rhsColumn] == rds_config.BoolType {
	//	rhsStr = fmt.Sprintf("t0.%s%s%s", rhsColumn, rds_config.Equal, strconv.FormatBool(constantValue.(bool)))
	//}
	resultColumns = append(resultColumns, rhsStr)
	resultType[rhsStr] = dataType[rhsColumn]
	rowSize := int(float64(len(satisfyData[rhsColumn])) * filterRatio)
	resultData := make([][]float64, rowSize)
	for i := 0; i < rowSize; i++ {
		resultData[i] = make([]float64, len(resultColumns))
	}
	for j := 0; j < len(resultColumns); {
		column := resultColumns[j]
		columnType := resultType[column]
		columnData := satisfyData[strings.Split(column, "t0.")[1]]
		if columnType == rds_config.FloatType {
			for i := 0; i < rowSize; i++ {
				value := columnData[i]
				if value == nil {
					if column == rhsStr {
						resultData[i][j] = rds_config.DecisionTreeYNilReplace
					} else {
						resultData[i][j] = math.NaN()
					}
				} else {
					resultData[i][j] = value.(float64)
				}
			}
			j++
		} else if columnType == rds_config.IntType {
			for i := 0; i < rowSize; i++ {
				value := columnData[i]
				if value == nil {
					if column == rhsStr {
						resultData[i][j] = rds_config.DecisionTreeYNilReplace
					} else {
						resultData[i][j] = math.NaN()
					}
				} else {
					resultData[i][j] = float64(value.(int64))
				}
			}
			j++
		} else if (columnType == rds_config.EnumType || columnType == rds_config.BoolType) && enableEnum {
			// 作为rhs的时候只用考虑列名
			if column == rhsStr {
				uniqueData := columnUniqueValue["t0"][strings.Split(column, "t0.")[1]]
				uniqueData = sortEnumColumnValues(columnType, uniqueData)
				for i, datum := range uniqueData {
					rhsValue2index[datum] = float64(i)
				}
				for i := 0; i < rowSize; i++ {
					resultData[i][j] = rhsValue2index[columnData[i]]
				}
				j++
				continue
			}
			split := strings.Split(column, rds_config.Equal)
			baseColumn := split[0]
			columnData = satisfyData[strings.Split(baseColumn, "t0.")[1]]
			column = split[0]
			columnValue := split[1]
			k := 0
			for i := 0; i < rowSize; i++ {
				value := columnData[i]
				k = j
				split = strings.Split(resultColumns[k], rds_config.Equal)
				column = split[0]
				if column != baseColumn {
					break
				}
				columnValue = split[1]
				var valueStr string
				switch columnType {
				case rds_config.EnumType:
					if value == nil {
						value = ""
					}
					valueStr = value.(string)
				case rds_config.BoolType:
					if value == nil {
						value = false
					}
					valueStr = strconv.FormatBool(value.(bool))
				}
				for {
					if columnValue == valueStr {
						resultData[i][k] = float64(1)
					} else {
						resultData[i][k] = float64(0)
					}
					k++
					if k >= len(resultColumns) {
						break
					}
					split = strings.Split(resultColumns[k], rds_config.Equal)
					column = split[0]
					if column != baseColumn {
						break
					}
					columnValue = split[1]
				}
			}
			j = k
		} else if enableText {

		}
	}
	return resultColumns, resultData, rhsValue2index, resultType
}

func sortEnumColumnValues(columnType string, sortedData []interface{}) []interface{} {
	switch columnType {
	case rds_config.EnumType:
		sort.Slice(sortedData, func(i, j int) bool {
			value1 := sortedData[i]
			value2 := sortedData[j]

			// 将 nil 值排在最后
			if value1 == nil {
				return false
			} else if value2 == nil {
				return true
			}

			return value1.(string) < value2.(string)
		})
	case rds_config.BoolType:
		sort.Slice(sortedData, func(i, j int) bool {
			value1 := sortedData[i]
			value2 := sortedData[j]

			// 将 nil 值排在最后
			if value1 == nil {
				return false
			} else if value2 == nil {
				return true
			}

			boolValue1 := value1.(bool)
			boolValue2 := value2.(bool)

			// 进行布尔值的比较
			if boolValue1 && !boolValue2 {
				return true
			} else if !boolValue1 && boolValue2 {
				return false
			}

			// 如果两个布尔值相同，则根据索引进行升序排序
			return i < j
		})
	default:
		logger.Errorf("sortEnumColumnValues, error enum columnType %s", columnType)
	}
	return sortedData
}

func generateMultiRowTrainData(satisfyData map[string]map[string][]interface{}, tableNames []string, index2table map[string]string, allDataType map[string]map[string]string, enableEnum, enableText bool, rhs rds.Predicate, filterRatios map[string]float64, columnUniqueValue map[string]map[string][]interface{}, batchSize int) ([]string, [][]float64, map[interface{}]float64, map[string]string) {
	// 获取rhs对应的列和表索引
	//leftIndex, leftColumn, rightIndex, rightColumn := getPredicateColumnInfo(rhs.PredicateStr, rhs.SymbolType)
	leftIndex, leftColumn, rightIndex, rightColumn := getPredicateColumnInfoNew(rhs)

	var indexArr []string
	for index := range index2table {
		indexArr = append(indexArr, index)
	}
	sort.Strings(indexArr)

	// 生成训练集的表头
	resultColumns, resultType := generateHeader(satisfyData, index2table, allDataType, enableEnum, enableText, leftIndex, leftColumn, rightIndex, rightColumn, rhs, columnUniqueValue)

	// 把索引相关信息生成一个数组
	indexInfoArr, index2pos, resultRowSize := generateIndexInfoArr(satisfyData, filterRatios)
	if batchSize < resultRowSize {
		resultRowSize = batchSize
	}

	resultData := make([][]float64, resultRowSize)
	leftColumnData := satisfyData[leftIndex][leftColumn]
	rightColumnData := satisfyData[rightIndex][rightColumn]
	//rhsType := allDataType[rhs.LeftColumn.TableId][rhs.LeftColumn.ColumnId]
	for i := 0; i < resultRowSize; i++ {
		resultData[i] = make([]float64, len(resultColumns))
		for j := 0; j < len(resultColumns)-1; j++ {
			columnType := resultType[resultColumns[j]]
			columnIndex, columnName, constantValue := getPredicateInfo(resultColumns[j])
			columnData := satisfyData[columnIndex][columnName][indexInfoArr[index2pos[columnIndex]].beginRow]
			switch columnType {
			case rds_config.IntType:
				if columnData == nil {
					resultData[i][j] = math.NaN()
				} else {
					resultData[i][j] = float64(columnData.(int64))
				}
			case rds_config.FloatType:
				if columnData == nil {
					resultData[i][j] = math.NaN()
				} else {
					resultData[i][j] = columnData.(float64)
				}

			case rds_config.EnumType:
				if enableEnum {
					if columnData == nil {
						columnData = ""
					}
					if columnData.(string) == constantValue {
						resultData[i][j] = float64(1)
					} else {
						resultData[i][j] = float64(0)
					}
				}
			case rds_config.BoolType:
				if enableEnum {
					if columnData == nil {
						columnData = false
					}
					if strconv.FormatBool(columnData.(bool)) == constantValue {
						resultData[i][j] = float64(1)
					} else {
						resultData[i][j] = float64(0)
					}
				}
			case rds_config.TextType:
				if enableText {

				}
			}
		}

		// rhs的值
		leftValue := leftColumnData[indexInfoArr[index2pos[leftIndex]].beginRow]
		rightValue := rightColumnData[indexInfoArr[index2pos[rightIndex]].beginRow]
		//if rhsType == rds_config.FloatType {
		//	if leftValue == nil {
		//		resultData[i][len(resultColumns)-1] = 0
		//	} else {
		//		resultData[i][len(resultColumns)-1] = leftValue.(float64)
		//	}
		//} else if rhsType == rds_config.IntType {
		//	if leftValue == nil {
		//		resultData[i][len(resultColumns)-1] = 0
		//	} else {
		//		resultData[i][len(resultColumns)-1] = float64(leftValue.(int64))
		//	}
		//} else if leftValue == rightValue {
		//	resultData[i][len(resultColumns)-1] = float64(1)
		//} else {
		//	resultData[i][len(resultColumns)-1] = float64(0)
		//}
		if leftValue == rightValue {
			resultData[i][len(resultColumns)-1] = float64(1)
		} else {
			resultData[i][len(resultColumns)-1] = float64(0)
		}

		//indexInfoArr[0].beginRow++
		//for j := 0; j < len(indexInfoArr)-1; j++ {
		//	indexInfoArr[j+1].beginRow += indexInfoArr[j].beginRow / indexInfoArr[j].endRow
		//	indexInfoArr[j].beginRow = indexInfoArr[j].beginRow % indexInfoArr[j].endRow
		//}
		tid := index2pos[indexArr[0]]
		indexInfoArr[tid].beginRow++
		for j := 0; j < len(indexArr)-1; j++ {
			indexInfoArr[index2pos[indexArr[j+1]]].beginRow += indexInfoArr[index2pos[indexArr[j]]].beginRow / indexInfoArr[index2pos[indexArr[j]]].endRow
			indexInfoArr[index2pos[indexArr[j]]].beginRow = indexInfoArr[index2pos[indexArr[j]]].beginRow % indexInfoArr[index2pos[indexArr[j]]].endRow
		}
	}

	return resultColumns, resultData, nil, resultType
}

func generateMultiRowTrainDataSingleTable(satisfyData map[string]map[string][]interface{}, allDataType map[string]map[string]string, enableEnum, enableText bool, rhs rds.Predicate, filterRatios map[string]float64, leftIndex, leftColumn, rightIndex, rightColumn string, resultColumns []string, resultType map[string]string) [][]float64 {

	// 把索引相关信息生成一个数组
	_, _, resultRowSize := generateIndexInfoArr(satisfyData, filterRatios)

	resultData := make([][]float64, resultRowSize)
	leftColumnData := satisfyData[leftIndex][leftColumn]
	rightColumnData := satisfyData[rightIndex][rightColumn]
	rhsType := allDataType[rhs.LeftColumn.TableId][rhs.LeftColumn.ColumnId]
	i := 0
	l0 := 0
	l1 := 0
	for _, tmp := range satisfyData["t0"] {
		l0 = len(tmp)
		break
	}
	for _, tmp := range satisfyData["t1"] {
		l1 = len(tmp)
		break
	}
	for p := 0; p < l0; p++ {
		for k := 0; k < l1; k++ {
			for j := 0; j < len(resultColumns)-1; j++ {
				columnType := resultType[resultColumns[j]]
				columnIndex, columnName, constantValue := getPredicateInfo(resultColumns[j])
				var columnData interface{}
				if columnIndex == "t0" {
					columnData = satisfyData[columnIndex][columnName][p]
				} else if columnIndex == "t1" {
					columnData = satisfyData[columnIndex][columnName][k]
				}
				switch columnType {
				case rds_config.IntType:
					if columnData == nil {
						resultData[i][j] = math.NaN()
					} else {
						resultData[i][j] = float64(columnData.(int64))
					}
				case rds_config.FloatType:
					if columnData == nil {
						resultData[i][j] = math.NaN()
					} else {
						resultData[i][j] = columnData.(float64)
					}

				case rds_config.EnumType:
					if enableEnum {
						if columnData == nil {
							columnData = ""
						}
						if columnData.(string) == constantValue {
							resultData[i][j] = float64(1)
						} else {
							resultData[i][j] = float64(0)
						}
					}
				case rds_config.BoolType:
					if enableEnum {
						if columnData == nil {
							columnData = false
						}
						if strconv.FormatBool(columnData.(bool)) == constantValue {
							resultData[i][j] = float64(1)
						} else {
							resultData[i][j] = float64(0)
						}
					}
				case rds_config.TextType:
					if enableText {

					}
				}
			}
			// rhs的值
			leftValue := leftColumnData[p]
			rightValue := rightColumnData[k]
			if rhsType == rds_config.FloatType {
				if leftValue == nil {
					resultData[i][len(resultColumns)-1] = 0
				} else {
					resultData[i][len(resultColumns)-1] = leftValue.(float64)
				}
			} else if rhsType == rds_config.IntType {
				if leftValue == nil {
					resultData[i][len(resultColumns)-1] = 0
				} else {
					resultData[i][len(resultColumns)-1] = float64(leftValue.(int64))
				}
			} else if leftValue == rightValue {
				resultData[i][len(resultColumns)-1] = float64(1)
			} else {
				resultData[i][len(resultColumns)-1] = float64(0)
			}
		}
		i++
	}

	return resultData
}
