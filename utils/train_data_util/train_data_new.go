package train_data_util

import (
	"fmt"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock_v3/rds_config"
	"math"
	"sort"
	"strconv"
	"time"
)

func GenerateYTrainData(data map[string]map[string][]interface{}, yColumn string, dataType map[string]map[string]string, tablesIndex map[string]int, isSingle, isCross bool, connectedColumns map[string]string) ([]string, map[string]string, [][]float64, map[interface{}]float64) {
	dataUniqueValue := getColumnDataUnique(data, dataType)
	var yTableId string
	var headers []string
	var resultData [][]float64
	var rhsValue2Index map[interface{}]float64
	headersType := make(map[string]string)
	if isCross { // 跨表
		newTableId := strconv.FormatInt(time.Now().UnixMilli(), 10)
		// 只有rds的情况下，列名可能重复，这里需要防止重复，存在后端的时候不会有这种情况
		dataAdd, yColumnAdd, dataTypeAdd, connectedColumnsAdd := generateConnectedTablesInfo(data, yColumn, dataType, connectedColumns)

		newTableData := map[string]map[string][]interface{}{newTableId: generateConnectedData(dataAdd, connectedColumnsAdd, rds_config.DecisionTreeMaxRowSize)}
		newDataType := map[string]map[string]string{newTableId: {}}
		newTableIndex := map[string]int{newTableId: len(tablesIndex)}
		deleteColumns := make(map[string]bool)
		for tableId, column := range connectedColumnsAdd {
			deleteColumns[tableId+rds_config.DecisionSplitSymbol+column] = true
		}
		for _, columnsType := range dataTypeAdd {
			for column, columnType := range columnsType {
				if deleteColumns[column] {
					continue
				}
				newDataType[newTableId][column] = columnType
			}
		}
		headers, headersType, resultData, _ = GenerateYTrainData(newTableData, yColumnAdd, newDataType, newTableIndex, false, false, nil)
	} else {
		// x相关的列
		for tableId, tableColumnType := range dataType {
			tableIndex := tablesIndex[tableId]
			tableData := data[tableId]
			// 列排序，保证xy训练集的稳定
			keys := make([]string, 0, len(tableColumnType))
			for k := range tableColumnType {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, column := range keys {
				if column == yColumn {
					yTableId = tableId
					continue
				}
				columnType := tableColumnType[column]
				headersTmp, headersTypeTmp, dataTmp := getSingleColumnTrainData(tableData[column], column, columnType, tableIndex, dataUniqueValue[tableId], isSingle)
				headers = append(headers, headersTmp...)
				for key, value := range headersTypeTmp {
					headersType[key] = value
				}
				resultData = append(resultData, dataTmp...)
				if !isSingle {
					headerTmp_, headerTypeTmp, dataTmp_ := getMultiColumnTrainData(tableData[column], column, columnType, tableIndex)
					if headerTmp_ != "" {
						headers = append(headers, headerTmp_)
						headersType[headerTmp_] = headerTypeTmp
						resultData = append(resultData, dataTmp_)
					}
				}
			}
		}
		// y列
		if isSingle { // 单行
			headerTmp, headersTypeTmp, dataTmp, rhsValue2IndexTmp := getSingleColumnYTrainData(data[yTableId][yColumn], yColumn, dataType[yTableId][yColumn], tablesIndex[yTableId], dataUniqueValue[yTableId][yColumn])
			rhsValue2Index = rhsValue2IndexTmp
			headers = append(headers, headerTmp)
			for key, value := range headersTypeTmp {
				headersType[key] = value
			}
			resultData = append(resultData, dataTmp)
		} else { // 多行
			headerTmp, headerTypeTmp, dataTmp := getMultiColumnTrainData(data[yTableId][yColumn], yColumn, dataType[yTableId][yColumn], tablesIndex[yTableId])
			headers = append(headers, headerTmp)
			headersType[headerTmp] = headerTypeTmp
			resultData = append(resultData, dataTmp)
		}
	}
	return headers, headersType, transpose(resultData), rhsValue2Index
}

// getColumnTrainDataHeader 生成单列谓词的表头和数据
func getSingleColumnTrainData(columnData []interface{}, column, columnType string, tableIndex int, dataUniqueValue map[string][]interface{}, isSingle bool) ([]string, map[string]string, [][]float64) {
	var headers []string
	var data [][]float64
	headersType := make(map[string]string)
	if columnType == rds_config.EnumType || columnType == rds_config.BoolType {
		columnUniqueValues := dataUniqueValue[column]
		for _, value := range columnUniqueValues {
			if value == nil { //去空值
				continue
			}
			header := fmt.Sprintf("t%v.%v=%v", tableIndex*2, column, value)
			headers = append(headers, header)
			headersType[header] = columnType
			if isSingle {
				data = append(data, getSingleColumnSingleRowTrainDataValues(columnData, value, columnType, rds_config.DecisionTreeMaxRowSize))
			} else {
				header = fmt.Sprintf("t%v.%v=%v", tableIndex*2+1, column, value)
				headers = append(headers, header)
				headersType[header] = columnType
				data = append(data, getSingleColumnMultiRowTrainDataValues(columnData, value, columnType, rds_config.DecisionTreeMaxRowSize)...)
			}
		}
	} else if columnType == rds_config.IntType || columnType == rds_config.FloatType {
		header := fmt.Sprintf("t%v.%v", tableIndex*2, column)
		headers = append(headers, header)
		headersType[header] = columnType
		if isSingle {
			data = append(data, getSingleColumnSingleRowTrainDataValues(columnData, nil, columnType, rds_config.DecisionTreeMaxRowSize))
		} else {
			header = fmt.Sprintf("t%v.%v", tableIndex*2+1, column)
			headers = append(headers, header)
			headersType[header] = columnType
			data = append(data, getSingleColumnMultiRowTrainDataValues(columnData, nil, columnType, rds_config.DecisionTreeMaxRowSize)...)
		}
	} else {
		logger.Infof("列%v是%v类型不需要生成单列训练集", column, columnType)
	}
	return headers, headersType, data
}

func getSingleColumnYTrainData(columnData []interface{}, column, columnType string, tableIndex int, columnUniqueValue []interface{}) (string, map[string]string, []float64, map[interface{}]float64) {
	header := fmt.Sprintf("t%v.%v", tableIndex*2, column)
	headersType := map[string]string{header: columnType}
	value2index := make(map[interface{}]float64)
	for i, value := range columnUniqueValue {
		if value == nil {
			value2index[value] = rds_config.DecisionTreeYNilReplace
			continue
		}
		value2index[value] = float64(i)
	}

	size := rds_config.DecisionTreeMaxRowSize
	if size > len(columnData) {
		size = len(columnData)
	}
	result := make([]float64, size)
	for i := 0; i < size; i++ {
		result[i] = value2index[columnData[i]]
	}

	return header, headersType, result, value2index
}

// getMultiColumnTrainData 生成多列谓词的表头和数据
func getMultiColumnTrainData(columnData []interface{}, column, columnType string, tableIndex int) (string, string, []float64) {
	var header string
	var data []float64
	if columnType == rds_config.EnumType || columnType == rds_config.BoolType || columnType == rds_config.IntType || columnType == rds_config.FloatType {
		header = fmt.Sprintf("t%v.%v=t%v.%v", tableIndex*2, column, tableIndex*2+1, column)
		data = getMultiColumnMultiRowTrainDataValues(columnData, rds_config.DecisionTreeMaxRowSize)
	} else {
		logger.Infof("列%v是%v类型不需要生成多列训练集", column, columnType)
	}
	return header, columnType, data
}

// getSingleColumnSingleRowTrainDataValues 纯单行谓词中单列数据生成
func getSingleColumnSingleRowTrainDataValues(columnData []interface{}, value interface{}, columnType string, size int) []float64 {
	if size > len(columnData) {
		size = len(columnData)
	}
	result := make([]float64, size)
	if columnType == rds_config.EnumType || columnType == rds_config.BoolType {
		for i, tmpValue := range columnData {
			if i >= size {
				break
			}
			if tmpValue == value {
				result[i] = 1
			} else {
				result[i] = 0
			}
		}
	} else if columnType == rds_config.IntType {
		for i, tmpValue := range columnData {
			if i >= size {
				break
			}
			result[i] = float64(tmpValue.(int64))
		}
	} else if columnType == rds_config.FloatType {
		for i, tmpValue := range columnData {
			if i >= size {
				break
			}
			result[i] = tmpValue.(float64)
		}
	}
	return result
}

// getSingleColumnMultiRowTrainDataValues 多行谓词中单列数据生成
func getSingleColumnMultiRowTrainDataValues(columnData []interface{}, value interface{}, columnType string, size int) [][]float64 {
	singleSize := len(columnData) / 2
	if size > singleSize*singleSize {
		size = singleSize * singleSize
	}
	leftData := columnData[:singleSize]
	rightData := columnData[singleSize:]
	leftResult, rightResult := make([]float64, size), make([]float64, size)
	i := 0
	if columnType == rds_config.EnumType || columnType == rds_config.BoolType {
		for _, leftValue := range leftData {
			leftTmp := float64(0)
			if leftValue == value {
				leftTmp = float64(1)
			}
			for _, rightValue := range rightData {
				leftResult[i] = leftTmp
				rightTmp := float64(0)
				if rightValue == value {
					rightTmp = float64(1)
				}
				rightResult[i] = rightTmp
				i++
				if i >= size {
					return [][]float64{leftResult, rightResult}
				}
			}
		}
	} else if columnType == rds_config.IntType {
		for _, leftValue := range leftData {
			leftTmp := float64(leftValue.(int64))
			for _, rightValue := range rightData {
				leftResult[i] = leftTmp
				rightResult[i] = float64(rightValue.(int64))
				i++
				if i >= size {
					return [][]float64{leftResult, rightResult}
				}
			}
		}
	} else if columnType == rds_config.FloatType {
		for _, leftValue := range leftData {
			leftTmp := leftValue.(float64)
			for _, rightValue := range rightData {
				leftResult[i] = leftTmp
				rightResult[i] = rightValue.(float64)
				i++
				if i >= size {
					return [][]float64{leftResult, rightResult}
				}
			}
		}
	}
	return [][]float64{leftResult, rightResult}
}

// getMultiColumnMultiRowTrainDataValues 多行谓词中多列数据生成
func getMultiColumnMultiRowTrainDataValues(columnData []interface{}, size int) []float64 {
	singleSize := len(columnData) / 2
	if size > singleSize*singleSize {
		size = singleSize * singleSize
	}
	leftData := columnData[:singleSize]
	rightData := columnData[singleSize:]
	result := make([]float64, size)
	i := 0
	for _, leftValue := range leftData {
		for _, rightValue := range rightData {
			if leftValue == rightValue {
				result[i] = 1
			} else {
				result[i] = 0
			}
			i++
			if i >= size {
				return result
			}
		}
	}
	return result
}

// getColumnDataUnique 生成枚举类型和bool类型列的枚举值，并且保证有序
func getColumnDataUnique(data map[string]map[string][]interface{}, dataType map[string]map[string]string) map[string]map[string][]interface{} {
	result := make(map[string]map[string][]interface{})
	for tableId, tableColumnType := range dataType {
		result[tableId] = map[string][]interface{}{}
		for column, columnType := range tableColumnType {
			if columnType == rds_config.EnumType || columnType == rds_config.BoolType {
				columnData := data[tableId][column]
				tmp := make(map[interface{}]bool)
				for _, value := range columnData {
					tmp[value] = true
				}
				tmpArr := make([]interface{}, len(tmp))
				i := 0
				for key := range tmp {
					tmpArr[i] = key
					i++
				}
				sort.Slice(tmpArr, func(i, j int) bool {
					strI := fmt.Sprint(tmpArr[i]) // 将元素转换为字符串
					strJ := fmt.Sprint(tmpArr[j])
					return strI < strJ
				})
				result[tableId][column] = tmpArr
			}
		}
	}
	return result
}

// generateConnectedData 根据主外键连接表，目前只考虑了两表相连
func generateConnectedData(data map[string]map[string][]interface{}, connectedColumns map[string]string, size int) map[string][]interface{} {
	singleSize := int(math.Sqrt(float64(size)))

	// 根据主外键筛选出特定的行
	var leftColumnData, rightColumnData []interface{}
	var leftTableId, rightTableId, leftColumnId, rightColumnId string
	k := 0
	for tableId, columnId := range connectedColumns {
		if k == 0 {
			leftColumnData = data[tableId][columnId]
			leftTableId = tableId
			leftColumnId = columnId
		} else {
			rightColumnData = data[tableId][columnId]
			rightTableId = tableId
			rightColumnId = columnId
		}
		k++
	}
	var leftRowIds, rightRowIds []int
	cnt := 1
	for leftRowId, leftValue := range leftColumnData {
		if cnt > singleSize {
			break
		}
		add := false
		for rightRowId, rightValue := range rightColumnData {
			if cnt > singleSize {
				break
			}
			if leftValue == rightValue {
				add = true
				rightRowIds = append(rightRowIds, rightRowId)
				cnt++
			}
		}
		if add {
			leftRowIds = append(leftRowIds, leftRowId)
		}
	}

	result := make(map[string][]interface{})
	resultSize := len(leftRowIds) * len(rightRowIds)
	leftTableData := data[leftTableId]
	for column := range leftTableData {
		if column == leftColumnId {
			continue
		}
		result[column] = make([]interface{}, resultSize)
	}
	rightTableData := data[rightTableId]
	for column := range rightTableData {
		if column == rightColumnId {
			continue
		}
		result[column] = make([]interface{}, resultSize)
	}

	cnt = 0
	for _, leftRowId := range leftRowIds {
		for _, rightRowId := range rightRowIds {
			for column := range leftTableData {
				if column == leftColumnId {
					continue
				}
				result[column][cnt] = leftTableData[column][leftRowId]
			}
			for column := range rightTableData {
				if column == rightColumnId {
					continue
				}
				result[column][cnt] = rightTableData[column][rightRowId]
			}
			cnt++
		}
	}
	return result
}

func transpose(matrix [][]float64) [][]float64 {
	// 获取原始矩阵的行数和列数
	rows := len(matrix)
	if rows == 0 {
		return nil
	}
	cols := len(matrix[0])

	// 创建一个新的二维切片来存储转置后的矩阵
	transposed := make([][]float64, cols)
	for i := range transposed {
		transposed[i] = make([]float64, rows)
	}

	// 遍历原始矩阵，将元素放置到转置后的矩阵中
	for i := 0; i < rows; i++ {
		for j := 0; j < cols; j++ {
			// 将 matrix[i][j] 放置到 transposed[j][i]
			transposed[j][i] = matrix[i][j]
		}
	}

	// 返回转置后的矩阵
	return transposed
}

func generateConnectedTablesInfo(data map[string]map[string][]interface{}, yColumn string, dataType map[string]map[string]string, connectedColumns map[string]string) (map[string]map[string][]interface{}, string, map[string]map[string]string, map[string]string) {
	dataAdd := make(map[string]map[string][]interface{})
	dataTypeAdd := make(map[string]map[string]string)
	var yTableId string
	for tableId, tableData := range data {
		tableDataAdd := make(map[string][]interface{})
		tableTypeAdd := make(map[string]string)
		for column, values := range tableData {
			if column == yColumn {
				yTableId = tableId
			}
			columnAdd := tableId + rds_config.DecisionSplitSymbol + column
			tableDataAdd[columnAdd] = values
			tableTypeAdd[columnAdd] = dataType[tableId][column]
		}
		dataAdd[tableId] = tableDataAdd
		dataTypeAdd[tableId] = tableTypeAdd
	}
	yColumnAdd := yTableId + rds_config.DecisionSplitSymbol + yColumn
	connectedColumnsAdd := make(map[string]string)
	for tableId, column := range connectedColumns {
		connectedColumnsAdd[tableId] = tableId + rds_config.DecisionSplitSymbol + column
	}
	return dataAdd, yColumnAdd, dataTypeAdd, connectedColumnsAdd
}
