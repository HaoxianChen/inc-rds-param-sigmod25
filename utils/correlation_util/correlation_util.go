package correlation_util

import (
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock_v3/rds_config"
	"gitlab.grandhoo.com/rock/rock_v3/utils"
	"gitlab.grandhoo.com/rock/rock_v3/utils/storage_utils"
	"gitlab.grandhoo.com/rock/storage/storage2/database/types/special_type/timetype"
	"math"
	"sort"
)

func getSampleData(lhs []rds.Predicate, rhs rds.Predicate, sampleCount int) (df correlationDF, yLabel []float64, xTables, xColumns []string) {
	//是单表还是多表？单表直接取数据，多表取最新联表谓词涉及到的新的表的数据
	isSingle := isSingleTable(lhs, rhs)
	if isSingle {
		df, yLabel, xTables, xColumns = generateSingleDF(rhs.LeftColumn.TableId, sampleCount, rhs.LeftColumn.ColumnId)
	} else { //todo 目前是取联表谓词引入的新表的数据。是不是应该取联表后的数据？
		df, yLabel, xTables, xColumns = generateMultiDF(lhs, rhs, sampleCount)
	}
	return df, yLabel, xTables, xColumns
}

func isSingleTable(lhs []rds.Predicate, rhs rds.Predicate) bool {
	tables := make(map[string]struct{})
	leftTable := rhs.LeftColumn.TableId
	tables[leftTable] = struct{}{}
	if rhs.RightColumn.TableId != "" && rhs.RightColumn.TableId != leftTable {
		tables[rhs.RightColumn.TableId] = struct{}{}
	}
	if len(tables) > 1 {
		return false
	}
	for _, predicate := range lhs {
		if _, exist := tables[predicate.LeftColumn.TableId]; !exist {
			tables[predicate.LeftColumn.TableId] = struct{}{}
		}
		if predicate.RightColumn.TableId != "" {
			if _, exist := tables[predicate.RightColumn.TableId]; !exist {
				tables[predicate.RightColumn.TableId] = struct{}{}
			}
		}
		if len(tables) > 1 {
			return false
		}
	}
	return len(tables) == 1
}

func CorrelationSort(lhs []rds.Predicate, rhs rds.Predicate, sampleCount int, workNum int) (table2Column2Rank map[string]map[string]int) {
	table2Column2Rank = make(map[string]map[string]int)
	result := calculateCorrelation(lhs, rhs, sampleCount, workNum)
	sortColumns, sortTables := sortResult(result)
	for i, column := range sortColumns {
		table := sortTables[i]
		if _, exist := table2Column2Rank[table]; !exist {
			table2Column2Rank[table] = make(map[string]int)
		}
		table2Column2Rank[table][column] = i
	}
	return table2Column2Rank
}

func sortResult(correlationResult []CorrelationResult) (sortColumns []string, sortTables []string) {
	sortColumns = make([]string, len(correlationResult))
	sortTables = make([]string, len(correlationResult))
	sort.SliceStable(correlationResult, func(i, j int) bool {
		if correlationResult[i].Spearman != correlationResult[j].Spearman {
			return correlationResult[i].Spearman > correlationResult[j].Spearman
		} else if correlationResult[i].Pearson != correlationResult[j].Pearson {
			return correlationResult[i].Pearson > correlationResult[j].Pearson
		} else {
			return correlationResult[i].Kendall > correlationResult[j].Kendall
		}
	})

	for i, result := range correlationResult {
		sortColumns[i] = result.Index
		sortTables[i] = result.Table
	}
	return sortColumns, sortTables
}

func calculateCorrelation(lhs []rds.Predicate, rhs rds.Predicate, sampleCount int, workNum int) (results []CorrelationResult) {
	df, yLabel, xTables, xColumns := getSampleData(lhs, rhs, sampleCount)
	if len(df.data) == 0 {
		return
	}
	cluster := NewCalculatorManager(yLabel, "Normal", workNum)
	for i, column := range xColumns {
		cluster.AppendColIndex(column)
		cluster.AppendColTable(xTables[i])
	}
	cluster.Run(&df)

	return cluster.results
}

func getJoinPredicates(lhs []rds.Predicate) []rds.Predicate {
	resultMap := make(map[string]rds.Predicate)
	result := make([]rds.Predicate, 0)
	for _, predicate := range lhs {
		if predicate.LeftColumn.TableId == predicate.RightColumn.TableId || predicate.RightColumn.TableId == "" {
			continue
		}
		predicateStr := predicate.LeftColumn.TableId + "_" + predicate.LeftColumn.ColumnId + "=" + predicate.RightColumn.TableId + "_" + predicate.RightColumn.ColumnId
		if _, exist := resultMap[predicateStr]; !exist {
			resultMap[predicateStr] = predicate
		}
	}
	for _, predicate := range resultMap {
		result = append(result, predicate)
	}
	return result
}

func getCurrentTableAndY(lhs []rds.Predicate, rhs rds.Predicate) (string, string) {
	if len(lhs) == 0 {
		return rhs.LeftColumn.TableId, rhs.LeftColumn.ColumnId
	}

	newLhs := lhs[len(lhs)-1]
	leftTable := newLhs.LeftColumn.TableId
	rightTable := newLhs.RightColumn.TableId
	if rightTable == "" { //常数谓词
		if leftTable == rhs.LeftColumn.TableId { //跟Y谓词同表
			return leftTable, rhs.LeftColumn.ColumnId
		} else { //跟Y谓词不同表，找其他lhs谓词中跨表谓词的当前表的列，为目标列
			var yColumn string
			for _, predicate := range lhs {
				if predicate.PredicateType != 2 {
					continue
				}
				if predicate.LeftColumn.TableId == leftTable {
					yColumn = predicate.LeftColumn.ColumnId
					break
				} else if predicate.RightColumn.TableId == leftTable {
					yColumn = predicate.RightColumn.ColumnId
					break
				}
			}
			if yColumn == "" {
				logger.Errorf("Error path, lhs: %s, rhs: %s", lhs, rhs)
			}
			return leftTable, yColumn
		}
	}
	//非常数谓词
	if leftTable != rightTable {
		if leftTable == rhs.LeftColumn.TableId || leftTable == rhs.RightColumn.TableId { //右表为新表
			return rightTable, newLhs.RightColumn.ColumnId
		} else if rightTable == rhs.LeftColumn.TableId || rightTable == rhs.RightColumn.TableId { //左表为新表
			return leftTable, newLhs.LeftColumn.ColumnId
		} else { //左右表都跟Y表不同，说明中间有其他联表谓词，判断左右表是否在前面的lhs中出现过，没有出现过的为新表
			for _, predicate := range lhs[0 : len(lhs)-1] {
				if predicate.PredicateType != 2 {
					continue
				}
				left := predicate.LeftColumn.TableId
				right := predicate.RightColumn.TableId
				if left == leftTable || right == leftTable { //左表出现过
					return rightTable, newLhs.RightColumn.ColumnId
				} else if left == rightTable || right == rightTable { //右表出现过
					return leftTable, newLhs.LeftColumn.ColumnId
				}
			}
			logger.Errorf("Error path, lhs: %s, rhs: %s", lhs, rhs)
			return "", ""
		}
	} else { //非跨表谓词
		if leftTable == rhs.LeftColumn.TableId { //跟Y同表
			return leftTable, rhs.LeftColumn.ColumnId
		} else if leftTable == rhs.RightColumn.TableId {
			return leftTable, rhs.RightColumn.ColumnId
		} else { //跟Y不同表，说明中间有关于该表的联表谓词，需找到目标列
			for _, predicate := range lhs[0 : len(lhs)-1] {
				if predicate.PredicateType != 2 {
					continue
				}
				left := predicate.LeftColumn.TableId
				right := predicate.RightColumn.TableId
				if left == leftTable {
					return leftTable, predicate.LeftColumn.ColumnId
				} else if right == leftTable {
					return leftTable, predicate.RightColumn.ColumnId
				}
			}
			logger.Errorf("Error path, lhs: %s, rhs: %s", lhs, rhs.PredicateStr)
			return "", ""
		}
	}
}

func generateMultiDF(lhs []rds.Predicate, rhs rds.Predicate, sampleCount int) (df correlationDF, yLabel []float64, xTables, xColumns []string) {
	//todo y跨表应该做两次？
	targetTable, targetColumn := getCurrentTableAndY(lhs, rhs)
	df = newCorrelationDF()

	xColumns = make([]string, 0)
	xTables = make([]string, 0)
	_, rowSize, columnType, _ := storage_utils.GetSchemaInfo(targetTable)
	if rowSize < sampleCount {
		sampleCount = rowSize
	}
	for column, colType := range columnType {
		colValues, _ := storage_utils.GetColumnValues(targetTable, column)
		colValues = colValues[:sampleCount]
		var numberedValues []float64
		if utils.IsNumberType(colType) {
			numberedValues = digitalDataFormat(colValues, colType, colType)
		} else {
			numberedValues, _ = dataNumber(colValues, column, colType)
		}
		if column == targetColumn {
			yLabel = numberedValues
			if yLabel == nil {
				logger.Errorf("error getting yLabel, column: %s", column)
				return df, nil, nil, nil
			}
			continue
		}
		if numberedValues != nil {
			df.data[column] = numberedValues
			xColumns = append(xColumns, column)
			xTables = append(xTables, targetTable)
		}
	}
	return df, yLabel, xTables, xColumns
}

func generateSingleDF(tableId string, sampleCount int, yColumn string) (df correlationDF, yLabel []float64, xTables, xColumns []string) {
	df = newCorrelationDF()
	xColumns = make([]string, 0)
	xTables = make([]string, 0)
	_, rowSize, columnType, _ := storage_utils.GetSchemaInfo(tableId)
	if rowSize < sampleCount {
		sampleCount = rowSize
	}
	for column, colType := range columnType {
		colValues, _ := storage_utils.GetColumnValues(tableId, column)
		colValues = colValues[:sampleCount]
		var numberedValues []float64
		if utils.IsNumberType(colType) {
			numberedValues = digitalDataFormat(colValues, colType, colType)
		} else {
			numberedValues, _ = dataNumber(colValues, column, colType)
		}
		if column == yColumn {
			yLabel = numberedValues
			if yLabel == nil {
				logger.Errorf("error getting yLabel, column: %s", column)
				return df, nil, nil, nil
			}
			continue
		}
		if numberedValues != nil {
			df.data[column] = numberedValues
			xColumns = append(xColumns, column)
			xTables = append(xTables, tableId)
		}
	}
	return df, yLabel, xTables, xColumns
}

func digitalDataFormat(values []interface{}, column, columnType string) (digitalValues []float64) {
	digitalValues = make([]float64, len(values))
	switch columnType {
	case rds_config.FloatType:
		for i, value := range values {
			if value != nil {
				digitalValues[i] = value.(float64)
			} else {
				digitalValues[i] = math.NaN()
			}
		}
		return digitalValues
	case rds_config.IntType:
		for i, value := range values {
			if value != nil {
				digitalValues[i] = float64(value.(int64))
			} else {
				digitalValues[i] = math.NaN()
			}
		}
		return digitalValues
	default:
		logger.Errorf("error columnType %s, column: %s", columnType, column)
		return nil
	}
}

// 数据转编号， bool类型、time类型、string类型需要
func dataNumber(values []interface{}, column, columnType string) (numberedValues []float64, value2Number map[interface{}]float64) {
	numberedValues = make([]float64, len(values))
	value2Number = make(map[interface{}]float64)

	sortedData := make([]interface{}, len(values))
	copy(sortedData, values)
	switch columnType {
	case rds_config.StringType:
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
	case rds_config.TimeType:
		sort.Slice(sortedData, func(i, j int) bool {
			value1 := sortedData[i]
			value2 := sortedData[j]

			// 将 nil 值排在最后
			if value1 == nil {
				return false
			} else if value2 == nil {
				return true
			}

			time1 := value1.(timetype.Time)
			time2 := value2.(timetype.Time)
			return time1 < time2
		})
	default:
		logger.Errorf("error columnType %s, column: %s", columnType, column)
		return nil, nil
	}

	num := float64(1)
	for _, cell := range sortedData {
		if _, ok := value2Number[cell]; !ok {
			value2Number[cell] = num
			num++
		}
	}
	for i, value := range values {
		numberedValues[i] = value2Number[value]
	}
	return numberedValues, value2Number
}
