package dataManager

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"runtime/debug"
	"strconv"
	"time"

	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/format"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables"
	"gitlab.grandhoo.com/rock/rock_v3/rds_config"
	"gitlab.grandhoo.com/rock/rock_v3/utils"
	"gitlab.grandhoo.com/rock/rock_v3/utils/storage_utils"
	"gitlab.grandhoo.com/rock/storage/etl/table"
	v2 "gitlab.grandhoo.com/rock/storage/etl/table/v2"
)

func int64ConditionFilter(table table.ITable, colValue interface{}, colId int, symbol string, rowFlags []bool) error {
	colValueStr := fmt.Sprint(colValue)
	values, nullFlags, err := table.GetInt64ColumnData(colId, 0, table.Length())
	if err != nil {
		logger.Errorf("get int64 column %s data error", colId)
		return &MyError{Msg: fmt.Sprintf("get int64 column %v data error", colId)}
	}

	//条件值为null
	if colValue == nil {
		if "=" != symbol && "!=" != symbol {
			logger.Errorf("unsupported symbol '%s' with value null", symbol)
			return &MyError{Msg: fmt.Sprintf("unsupported symbol '%s' with value null", symbol)}
		}
		for i := 0; i < len(nullFlags); i++ {
			if !("=" == symbol && nullFlags[i] == true ||
				"!=" == symbol && nullFlags[i] == false) {
				rowFlags[i] = false
			}
		}
		return nil
	}

	//条件值不是null
	targetValue, parseErr := strconv.ParseInt(colValueStr, 10, 64)
	if parseErr != nil {
		if parseErr != nil {
			logger.Errorf("parse %s to int64 error", colValueStr)
			return &MyError{Msg: fmt.Sprintf("parse %s to int64 error", colValueStr)}
		}
	}
	for i, value := range values {
		if (nullFlags[i] == true && "!=" != symbol) ||
			(nullFlags[i] == false &&
				!(">" == symbol && value > targetValue ||
					">=" == symbol && value >= targetValue ||
					"<" == symbol && value < targetValue ||
					"<=" == symbol && value <= targetValue ||
					"=" == symbol && value == targetValue ||
					"!=" == symbol && value != targetValue)) {
			rowFlags[i] = false
		}
	}
	return nil
}

func float64ConditionFilter(table table.ITable, colValue interface{}, colId int, symbol string, rowFlags []bool) error {
	colValueStr := fmt.Sprint(colValue)
	values, nullFlags, err := table.GetFloat64ColumnData(colId, 0, table.Length())
	if err != nil {
		logger.Errorf("get float64 column %s data error", colId)
		return &MyError{Msg: fmt.Sprintf("get float64 column %v data error", colId)}
	}

	//条件值为null
	if colValue == nil {
		if "=" != symbol && "!=" != symbol {
			logger.Errorf("unsupported symbol '%s' with value null", symbol)
			return &MyError{Msg: fmt.Sprintf("unsupported symbol '%s' with value null", symbol)}
		}
		for i := 0; i < len(nullFlags); i++ {
			if !("=" == symbol && nullFlags[i] == true ||
				"!=" == symbol && nullFlags[i] == false) {
				rowFlags[i] = false
			}
		}
		return nil
	}

	//条件值不是null
	targetValue, parseErr := strconv.ParseFloat(colValueStr, 10)
	if parseErr != nil {
		logger.Errorf("parse %s to float64 error", colValueStr)
		return &MyError{Msg: fmt.Sprintf("parse %s to float64 error", colValueStr)}
	}
	for i, value := range values {
		if (nullFlags[i] == true && "!=" != symbol) ||
			(nullFlags[i] == false &&
				!(">" == symbol && value > targetValue ||
					">=" == symbol && value >= targetValue ||
					"<" == symbol && value < targetValue ||
					"<=" == symbol && value <= targetValue ||
					"=" == symbol && value == targetValue ||
					"!=" == symbol && value != targetValue)) {
			rowFlags[i] = false
		}
	}
	return nil
}

func timeConditionFilter(table table.ITable, colValue interface{}, colId int, symbol string, rowFlags []bool) error {
	colValueStr := fmt.Sprint(colValue)
	values, nullFlags, err := table.GetTimeColumnData(colId, 0, table.Length())
	if err != nil {
		logger.Errorf("get time column %s data error", colId)
		return &MyError{Msg: fmt.Sprintf("get time column %v data error", colId)}
	}

	//条件值为null
	if colValue == nil {
		if "=" != symbol && "!=" != symbol {
			logger.Errorf("unsupported symbol '%s' with value null", symbol)
			return &MyError{Msg: fmt.Sprintf("unsupported symbol '%s' with value null", symbol)}
		}
		for i := 0; i < len(nullFlags); i++ {
			if !("=" == symbol && nullFlags[i] == true ||
				"!=" == symbol && nullFlags[i] == false) {
				rowFlags[i] = false
			}
		}
		return nil
	}

	//条件值不是null
	targetValueInt, parseErr := strconv.ParseInt(colValueStr, 10, 64)
	targetValue := time.UnixMilli(targetValueInt)
	if parseErr != nil {
		logger.Errorf("parse %s to int64 error", colValueStr)
		return &MyError{Msg: fmt.Sprintf("parse %s to int64 error", colValueStr)}
	}
	for i, value := range values {
		if (nullFlags[i] == true && "!=" != symbol) ||
			(nullFlags[i] == false &&
				!(">" == symbol && value.After(targetValue) ||
					">=" == symbol && (value.After(targetValue) || value.Equal(targetValue)) ||
					"<" == symbol && value.Before(targetValue) ||
					"<=" == symbol && (value.Before(targetValue) || value.Equal(targetValue)) ||
					"=" == symbol && value.Equal(targetValue) ||
					"!=" == symbol && !value.Equal(targetValue))) {
			rowFlags[i] = false
		}
	}
	return nil
}

func stringConditionFilter(table table.ITable, colValue interface{}, colId int, symbol string, rowFlags []bool) error {
	colValueStr := fmt.Sprint(colValue)
	values, nullFlags, err := table.GetStringColumnData(colId, 0, table.Length())
	if err != nil {
		logger.Errorf("get string column %s data error", colId)
		return &MyError{Msg: fmt.Sprintf("get string column %v data error", colId)}
	}

	//条件值为null
	if colValue == nil {
		if "=" != symbol && "!=" != symbol {
			logger.Errorf("unsupported symbol '%s' with value null", symbol)
			return &MyError{Msg: fmt.Sprintf("unsupported symbol '%s' with value null", symbol)}
		}
		for i := 0; i < len(nullFlags); i++ {
			if !("=" == symbol && nullFlags[i] == true ||
				"!=" == symbol && nullFlags[i] == false) {
				rowFlags[i] = false
			}
		}
		return nil
	}

	//条件值不是null
	targetValue := colValueStr

	for i, value := range values {
		if (nullFlags[i] == true && "!=" != symbol) ||
			(nullFlags[i] == false &&
				!(">" == symbol && value > targetValue ||
					">=" == symbol && value >= targetValue ||
					"<" == symbol && value < targetValue ||
					"<=" == symbol && value <= targetValue ||
					"=" == symbol && value == targetValue ||
					"!=" == symbol && value != targetValue)) {
			rowFlags[i] = false
		}
	}
	return nil
}

func preConditionsFilter(table table.ITable, conditions [][]interface{}, colName2Type map[string]string, colName2ColId map[string]int) ([]bool, int) {
	rowFlags := make([]bool, table.Length())
	var num = 0
	for i := range rowFlags {
		rowFlags[i] = true
	}

	for _, condition := range conditions {
		colName := fmt.Sprint(condition[0])
		colId, _ := colName2ColId[colName]

		symbol := fmt.Sprint(condition[1])
		colType := colName2Type[colName]
		var err error
		if "int64" == colType {
			err = int64ConditionFilter(table, condition[2], colId, symbol, rowFlags)
		} else if "float64" == colType {
			err = float64ConditionFilter(table, condition[2], colId, symbol, rowFlags)
		} else if "time" == colType {
			err = timeConditionFilter(table, condition[2], colId, symbol, rowFlags)
		} else if "string" == colType {
			err = stringConditionFilter(table, condition[2], colId, symbol, rowFlags)
		}
		if err != nil {
			logger.Errorf("pre condition %s %s %s filter error", colName, symbol, utils.GetInterfaceToString(condition[2]))
			return nil, 0
		}
	}

	for _, flag := range rowFlags {
		if flag {
			num++
		}
	}
	return rowFlags, num
}

func TableToDataFrameWithConditions(tableId string, conditions [][]interface{}, yColumn string) (*format.DataFrame, string, int64) {
	//输入：表id，条件，y，输出:数字列数据，表名，表长度
	tableName, length, colName2Type, colName2ColId := storage_utils.GetSchemaInfo(tableId)

	//条件列存在
	for _, condition := range conditions {
		colName := fmt.Sprint(condition[0])
		if _, exist := colName2ColId[colName]; !exist {
			logger.Errorf("condition column %s is not exist in table: %s, tableId: %s", colName, tableName, tableId)
			return nil, "", 0
		}
	}

	////yColumn需是数字类型
	//colType, exist := colName2Type[yColumn]
	//if !exist {
	//	log.Error().Msgf("yColumn %s is not exist", yColumn)
	//	return nil, "", 0
	//}
	//if "int64" != colType && "float64" != colType {
	//	log.Error().Msgf("yColumn %s is not digital type", yColumn)
	//	return nil, "", 0
	//}

	var digitalColumnNames []string
	for columnName, columnType := range colName2Type {
		if "int64" == columnType || "float64" == columnType {
			digitalColumnNames = append(digitalColumnNames, columnName)
		}
	}

	//将yColumn放到最后一列
	for i := 0; i < len(digitalColumnNames); i++ {
		if digitalColumnNames[i] == yColumn {
			digitalColumnNames[i] = digitalColumnNames[len(digitalColumnNames)-1]
			digitalColumnNames[len(digitalColumnNames)-1] = yColumn
			break
		}
	}

	table, _ := v2.Open(tableId)
	tableLength := table.Length()

	//符合前置条件的行数 是否符合前置条件的标志
	rowFlags, num := preConditionsFilter(table, conditions, colName2Type, colName2ColId)
	if rowFlags == nil {
		return nil, "", 0
	}

	//初始化数组
	var data = make([][]float64, num)
	for i := 0; i < num; i++ {
		data[i] = make([]float64, len(digitalColumnNames))
	}

	for index, colName := range digitalColumnNames {
		colId, exist := colName2ColId[colName]
		if !exist {
			logger.Errorf("columnName %s get colId failed.", colName)
			return nil, "", 0
		}
		//todo 分批取
		colData, err := table.GetColumnData(colId, 0, int64(length))
		if err != nil {
			logger.Errorf("get column data of column %s failed.", colName)
			return nil, "", 0
		}
		var rowIndex int = 0
		for i := 0; i < length; i++ {
			if rowFlags[i] == false {
				continue
			}
			valueStr := fmt.Sprint(colData[i])
			var value float64
			if colData[i] == nil {
				value = 0
			} else {
				var err error
				value, err = strconv.ParseFloat(valueStr, 10)
				if err != nil {
					logger.Errorf("fail to convert %s to float64 type", valueStr)
					return nil, "", 0
				}
			}
			data[rowIndex][index] = value
			rowIndex++
		}
	}
	xList := digitalColumnNames[:len(digitalColumnNames)-1]
	dataframe := format.NewDataFrame(data, xList, yColumn)
	return dataframe, tableName, tableLength
}

func TableToDataFrameWithRowIndexes(tableId string, conditionCols []string, rows []int32, yColumn string, gv *global_variables.GlobalV) (*format.DataFrame, string, int64, map[string]string, bool, error) {
	//处理panic
	defer func() {
		if err := recover(); err != nil {
			s := string(debug.Stack())
			logger.Error("recover.err:%v, stack:%v", err, s)
		}
	}()

	tableName := gv.TableName
	length := gv.RowSize
	colName2Type := gv.ColumnsType
	//tableValues := gv.TableValues
	if length > rds_config.RdsSize {
		length = rds_config.RdsSize
	}

	//用于过滤对数字类型条件列的处理
	tmpSet := make(map[string]struct{})
	for _, conditionColumn := range conditionCols {
		tmpSet[conditionColumn] = struct{}{}
	}

	var columnNamesToExecute []string
	for columnName, columnType := range colName2Type {
		if _, ok := tmpSet[columnName]; ok {
			continue
		}
		if ("int64" == columnType || "float64" == columnType) && columnName != yColumn {
			columnNamesToExecute = append(columnNamesToExecute, columnName)
		}
	}

	if len(columnNamesToExecute) == 0 {
		//log.Error().Msgf("No digital column to be x column")
		//return nil, "", 0, nil, &MyError{Msg: fmt.Sprintf("No digital column to be x column， tableId:%s, tableName:%s, yColumn:%s", tableId, tableName, yColumn)}
		return nil, "", 0, nil, false, nil
	}

	//将yColumn放到最后一列
	columnNamesToExecute = append(columnNamesToExecute, yColumn)

	//根据rowIndexes生成rowFlags -- 表示是否取该行数据，false为不取
	rowFlags := make([]bool, length)
	for i := range rowFlags {
		rowFlags[i] = false
	}

	//没有前置条件的情况下，rows为空表示取全部数据
	if len(conditionCols) == 0 && len(rows) == 0 {
		rows = make([]int32, length)
		for i := 0; i < length; i++ {
			rows[i] = int32(i)
		}
	}

	rowsRealLength := len(rows)
	for i := 0; i < len(rows); i++ {
		if rows[i] >= rds_config.RdsSize { //单个谓词条件的rows可能超过配置的数量，后面要丢掉
			rowsRealLength = i
			break
		}
		rowFlags[rows[i]] = true
	}

	//取y列，对y列进行枚举编号
	yValueSet := make(map[interface{}]struct{})
	yColValue := make([]interface{}, 0)               //y列值字符串形式
	yValueNumToValue := make(map[float64]interface{}) //y值编号到y值
	yValueToNum := make(map[interface{}]float64)      //y值到y值编号
	yColumnIndex := len(columnNamesToExecute) - 1
	colName := columnNamesToExecute[yColumnIndex]

	colData, exist := gv.TableValues[colName]
	if !exist {
		//log.Error().Msgf("get column data of column %s failed.", colName)
		return nil, "", 0, nil, false, &MyError{Msg: fmt.Sprintf("get column data of column %s failed.", colName)}
	}
	// colData, err := storage_utils.GetColumnValues(tableId, colName)
	// if err != nil {
	// 	return nil, "", 0, nil, true, &MyError{Msg: fmt.Sprintf("get column data of column %s failed.", colName)}
	// }
	var rowIndex = 0
	for i := 0; i < length; i++ {
		if rowFlags[i] == false {
			continue
		}
		//valueStr := utils.GetInterfaceToString(colData[i])
		if colData[i] == nil {
			//yColValue[rowIndex] = nil
			//rowIndex++
			//yValueSet[nil] = struct{}{}
			rowFlags[i] = false //x也不取这行了
			rowsRealLength--
			continue
		}
		yColValue = append(yColValue, colData[i])
		rowIndex++
		yValueSet[colData[i]] = struct{}{}
	}
	num := 0
	for yValue := range yValueSet {
		yValueNumToValue[float64(num)] = yValue
		yValueToNum[yValue] = float64(num)
		num++
	}

	//初始化数组
	var data = make([][]float64, rowsRealLength)
	for i := 0; i < rowsRealLength; i++ {
		data[i] = make([]float64, len(columnNamesToExecute))
	}

	//取x列数据
	for index := 0; index < len(columnNamesToExecute)-1; index++ {
		colName := columnNamesToExecute[index]
		colData, exist := gv.TableValues[colName]
		if !exist {
			//log.Error().Msgf("get column data of column %s failed.", colName)
			return nil, "", 0, nil, false, &MyError{Msg: fmt.Sprintf("get column data of column %s failed.", colName)}
		}
		// colData, err := storage_utils.GetColumnValues(tableId, colName)
		// if err != nil {
		// 	return nil, "", 0, nil, true, &MyError{Msg: fmt.Sprintf("get column data of column %s failed.", colName)}
		// }
		var rowIndex = 0
		for i := 0; i < length; i++ {
			if rowFlags[i] == false {
				continue
			}
			valueStr := utils.GetInterfaceToString(colData[i])
			var value float64
			if colData[i] == nil {
				value = math.NaN()
			} else {
				var err error
				value, err = strconv.ParseFloat(valueStr, 10)
				if err != nil {
					//log.Error().Msgf("fail to convert %s to float64 type", valueStr)
					return nil, "", 0, nil, true, &MyError{Msg: fmt.Sprintf("fail to convert %s to float64 type", valueStr)}
				}
			}
			data[rowIndex][index] = value
			rowIndex++
		}
	}

	//将y编号存入data
	for i := 0; i < len(yColValue); i++ {
		yNum := yValueToNum[yColValue[i]]
		data[i][yColumnIndex] = yNum
	}

	xList := columnNamesToExecute[:len(columnNamesToExecute)-1]
	dataframe := format.NewDataFrame(data, xList, yColumn)
	dataframe.SetYNumToValueMap(yValueNumToValue)
	return dataframe, tableName, int64(length), colName2Type, true, nil
}

// 定义一个 MyError 的结构体
type MyError struct {
	Msg string
}

// 实现 error 接口的 Error 方法
func (m *MyError) Error() string {
	return m.Msg
}

func readDataFromCsv(path string) ([][]float64, []string, string) {
	xList := make([]string, 0)
	data := make([][]float64, 0)
	fs, err := os.Open(path)
	if err != nil {
		logger.Errorf("can not open the file, err is %s", err.Error())
		panic(err)
	}
	defer fs.Close()
	r := csv.NewReader(fs)
	lineNum := 0
	for {
		row, err := r.Read()
		if err != nil && err != io.EOF {
			logger.Errorf("can not read, err is %s", err.Error())
			panic(err)
		}
		if err == io.EOF {
			break
		}
		//读取Attribute
		if lineNum == 0 {
			for i := 0; i < cap(row); i++ {
				xList = append(xList, row[i])
			}
		} else { //读取tuple
			rowData := make([]string, 0)
			for i := 0; i < cap(row); i++ {
				rowData = append(rowData, row[i])
			}
			floatData := make([]float64, len(rowData))
			for i := 0; i < len(rowData); i++ {
				floatData[i], err = strconv.ParseFloat(rowData[i], 64)
				if err != nil {
					logger.Errorf("string %s convert to float64 error : %s", rowData[i], err.Error())
					panic(err)
				}
			}
			data = append(data, floatData)
		}
		lineNum++
	}
	y := xList[len(xList)-1]
	xList = xList[:len(xList)-1]
	return data, xList, y
}
