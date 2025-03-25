package storage_utils

import (
	"fmt"
	"gitlab.grandhoo.com/rock/storage/storage2/database/etl/import_from_csv"
	"gitlab.grandhoo.com/rock/storage/storage2/database/etl/import_from_csv/csv_reader"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/bits-and-blooms/bitset"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables/metadata"

	config2 "gitlab.grandhoo.com/rock/storage/config"
	"gitlab.grandhoo.com/rock/storage/storage2/database/table/itable"
	"gitlab.grandhoo.com/rock/storage/storage2/database/table/line"
	"gitlab.grandhoo.com/rock/storage/storage2/utils"
	"gitlab.grandhoo.com/rock/storage/storage2/utils/hasher"

	"gitlab.grandhoo.com/rock/rock-share/global/db"
	"gitlab.grandhoo.com/rock/rock_v3/rds_config"
	"gitlab.grandhoo.com/rock/storage/config/cpu_busy"
	"gitlab.grandhoo.com/rock/storage/storage2/database/database_facade/database_facede_impl_v4_tlog/transaction_handler/transaction_handler_utils"
	"gitlab.grandhoo.com/rock/storage/storage2/database/etl/extern_db/postgres"
	"gitlab.grandhoo.com/rock/storage/storage2/database/etl/import_from_DB/import_from_DB_config"
	"gitlab.grandhoo.com/rock/storage/storage2/database/sql"
	"gitlab.grandhoo.com/rock/storage/storage2/database/sql/condition"
	"gitlab.grandhoo.com/rock/storage/storage2/database/table/table_impl/delete_flag"
	"gitlab.grandhoo.com/rock/storage/storage2/database/table/table_impl/row_id"
	"gitlab.grandhoo.com/rock/storage/storage2/database/types/special_type/decimal"
	"gitlab.grandhoo.com/rock/storage/storage2/rock_db"

	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/global/model/po"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables"
	"gitlab.grandhoo.com/rock/rock_v3/request"
	v2 "gitlab.grandhoo.com/rock/storage/etl/table/v2"
	"gitlab.grandhoo.com/rock/storage/specific_business"
)

const HashBucketSize = 2000 * 100 * 100

// ImportTable 在存储中创建一张表,返回tableId
func ImportTable(request *request.DataSource) string {
	cpu_busy.CPUBusy(1)
	defer cpu_busy.CPUBusy(-1)
	defer rock_db.DB.Sync()
	if strings.HasSuffix(request.TableName, ".csv") {
		tableId := strconv.FormatInt(time.Now().UnixMilli(), 10)
		if request.DataLimit == 0 {
			request.DataLimit = -1
		}
		file, err := os.OpenFile(request.TableName, os.O_RDONLY, 0666)
		if err != nil {
			logger.Error(err)
			return ""
		}
		var data = make([]byte, 10*1024*1024)
		n, err := file.Read(data)
		if err != nil {
			logger.Error(err)
			return ""
		}
		_ = file.Close()
		sampleResult, err := csv_reader.Sample(data[:n], csv_reader.CsvConfig{
			Limit: 100,
		})
		if err != nil {
			logger.Error(err)
			return ""
		}
		file, err = os.OpenFile(request.TableName, os.O_RDONLY, 0666)
		if err != nil {
			logger.Error(err)
			return ""
		}
		_, _, err = import_from_csv.Import(rock_db.DB, tableId, file, &csv_reader.CsvConfig{
			Charset:         sampleResult.Charset,
			ColumnDelimiter: sampleResult.ColumnDelimiter,
			TextDelimiter:   &sampleResult.TextDelimiter,
			ColumnTypes:     sampleResult.ColumnTypes,
			HeaderOffset:    0,
			Limit:           request.DataLimit,
			ColumnNameList:  utils.ToStringSlice(sampleResult.Content[0]),
		})
		_ = file.Close()
		if err != nil {
			logger.Error(err)
			return ""
		}
		return tableId
	}

	dataSourceName := postgres.DataSourceName(request.Host, strconv.Itoa(request.Port), request.User, request.Password, request.DatabaseName, "sslmode", "disable")
	dataBase, err := postgres.Open(dataSourceName)
	if err != nil {
		logger.Error(err)
	}
	defer dataBase.Close()
	tableId := strconv.FormatInt(time.Now().UnixMilli(), 10)
	if request.DataLimit == 0 {
		request.DataLimit = -1
	}
	err = rock_db.DB.ImportFromDB(&import_from_DB_config.Config{
		ExternDataBase:   dataBase,
		DBTableName:      request.TableName,
		NewTableName:     tableId,
		DeleteExistTable: false,
		ImportLength:     int64(request.DataLimit),
	})
	if err != nil {
		logger.Error(err)
	}
	return tableId
}

func GetTableValuesByCols(tableId string, columnsType map[string]string) (map[string][]interface{}, int) {
	tableValues := make(map[string][]interface{})
	for column := range columnsType {
		values, _ := GetColumnValues(tableId, column)
		tableValues[column] = values
	}
	return tableValues, 0
}

// GetColumnValuesWithLimit 获取一张表某一列的值
func GetColumnValuesWithLimit(tableId string, columnName string, offset, limit int64) ([]interface{}, error) {
	lines, err := rock_db.DB.Query(sql.SingleTable.SELECT(columnName).FROM(tableId).WHERE(
		condition.GreaterEq(row_id.ColumnName, offset), condition.Less(row_id.ColumnName, offset+limit)))
	if err != nil {
		return nil, err
	}
	data := make([]interface{}, len(lines))
	for i, line := range lines {
		value := line[0]
		if value != nil {
			if d, ok := value.(decimal.BigDecimal); ok {
				value = d.FloatVal()
			}
		}
		data[i] = value
	}
	return data, nil
}

func GetTableValuesByColsWithLimit(tableId string, columnsType map[string]string, offset, limit int64) map[string][]interface{} {
	tableValues := make(map[string][]interface{})
	for column := range columnsType {
		values, _ := GetColumnValuesWithLimit(tableId, column, offset, limit)
		tableValues[column] = values
	}
	return tableValues
}

func GetTableAllValues(tableId string) (map[string][]interface{}, int) {
	if rds_config.UseStorage {
		deletedRowSize, err := rock_db.DB.Query(sql.SingleTable.SELECT(sql.CountAsterisk).FROM(tableId).WHERE(condition.Equal(delete_flag.ColumnName, delete_flag.Deleted)))
		if err != nil {
			logger.Error(err)
			return nil, 0
		}

		return map[string][]interface{}{}, int(deletedRowSize[0][0].(int64))
	}

	m := map[string][]interface{}{}

	columns, err := rock_db.DB.TableColumnInfos(tableId)
	if err != nil {
		logger.Error(err)
	}
	var columnNames []string
	for _, column := range columns {
		columnNames = append(columnNames, column.ColumnName)
	}
	lengthLine, err := rock_db.DB.Query(sql.SingleTable.SELECT(sql.CountAsterisk).FROM(tableId))
	if err != nil {
		logger.Error(err)
	}
	length := lengthLine[0][0].(int64)
	for _, columnName := range columnNames {
		m[columnName] = make([]interface{}, length)
	}

	lines, err := rock_db.DB.Query(sql.SingleTable.SELECT(columnNames...).FROM(tableId))
	if err != nil {
		logger.Error(err)
	}

	for rowId, line := range lines {
		for colId, value := range line {
			if value != nil {
				if d, ok := value.(decimal.BigDecimal); ok {
					value = d.FloatVal()
				}
			}
			m[columnNames[colId]][rowId] = value
		}
	}

	deletedRowIds, err := rock_db.DB.Query(sql.SingleTable.SELECT(row_id.ColumnName).FROM(tableId).WHERE(condition.Equal(delete_flag.ColumnName, delete_flag.Deleted)))
	if err != nil {
		logger.Error(err)
	}
	for _, deletedRowId := range deletedRowIds {
		rowId := deletedRowId[0].(int64)
		for _, values := range m {
			values[rowId] = nil
		}
	}

	return m, len(deletedRowIds)
}

func GetTableAllValueIndexes(tableValues map[string][]interface{}, columnTypeMap map[string]string) (map[string][]int32, map[string]map[int32]interface{}, map[string]map[interface{}]int32) {
	if rds_config.UseStorage {
		return map[string][]int32{}, map[string]map[int32]interface{}{}, map[string]map[interface{}]int32{}
	}

	tableIndexes := map[string][]int32{}
	index2Value := map[string]map[int32]interface{}{}
	value2Index := map[string]map[interface{}]int32{}
	for colName, values := range tableValues {
		colIndex2Value := map[int32]interface{}{}
		colIndex2Value[rds_config.NilIndex] = nil
		colValue2Index := map[interface{}]int32{}
		colValue2Index[nil] = rds_config.NilIndex
		// 因为认为空字符串之间不相等,所以直接把空字符串当nil来索引
		colValue2Index[""] = rds_config.NilIndex
		//set := map[interface{}]int32{}
		//set[nil] = rds_config.NilIndex
		indexes := make([]int32, 0, len(values))
		if columnTypeMap[colName] == rds_config.FloatType || columnTypeMap[colName] == rds_config.IntType {
			// need sort
			valuesDistinctNotNil := UniqueArrayFilterNil(values)
			if columnTypeMap[colName] == rds_config.FloatType {
				sort.Slice(valuesDistinctNotNil, func(i, j int) bool {
					return floatVal(valuesDistinctNotNil[i]) < floatVal(valuesDistinctNotNil[j])
				})
			}
			if columnTypeMap[colName] == rds_config.IntType {
				sort.Slice(valuesDistinctNotNil, func(i, j int) bool {
					return valuesDistinctNotNil[i].(int64) < valuesDistinctNotNil[j].(int64)
				})
			}
			for orderIndex, value := range valuesDistinctNotNil {
				colValue2Index[value] = int32(orderIndex)
				colIndex2Value[int32(orderIndex)] = value
			}
		}
		for _, value := range values {
			if id, ok := colValue2Index[value]; ok {
				indexes = append(indexes, id)
			} else {
				id = int32(len(colValue2Index))
				colValue2Index[value] = id
				colIndex2Value[id] = value
				indexes = append(indexes, id)
			}
		}
		tableIndexes[colName] = indexes
		index2Value[colName] = colIndex2Value
		value2Index[colName] = colValue2Index
	}
	return tableIndexes, index2Value, value2Index
}

func floatVal(i interface{}) float64 {
	if f, ok := i.(float64); ok {
		return f
	}
	if f, ok := i.(decimal.BigDecimal); ok {
		return f.FloatVal()
	}
	logger.Warn("no float", i)
	return 0.0
}

// GetValue 获取一张表某个单元格的值
func GetValue(tableId string, columnName string, rowIndex int32, tableValues map[string][]interface{}) (interface{}, error) {
	if !rds_config.UseStorage {
		return tableValues[columnName][rowIndex], nil
	}
	//value, err := rock_db.DB.Query(sql.SingleTable.SELECT(columnName).FROM(tableId).WHERE(condition.Equal(row_id.ColumnName, rowIndex)))
	//if err != nil {
	//	return nil, err
	//}
	//return value[0][0], nil

	return rock_db.DB.CellValue(tableId, columnName, int64(rowIndex))
}

// GetColumnValues 获取一张表某一列的值
func GetColumnValues(tableId string, columnName string) ([]interface{}, error) {
	//lines, err := rock_db.DB.Query(sql.SingleTable.SELECT(columnName).FROM(tableId))
	//if err != nil {
	//	return nil, err
	//}
	//data := make([]interface{}, len(lines))
	//for i, line := range lines {
	//	value := line[0]
	//	if value != nil {
	//		if d, ok := value.(decimal.BigDecimal); ok {
	//			value = d.FloatVal()
	//		}
	//	}
	//	data[i] = value
	//}
	//return data, nil
	column, err := ColumnReader.GetColumnValue(tableId, columnName)
	if err != nil {
		return nil, err
	}
	var values = make([]interface{}, column.Length())
	for i := ColumnValueRowIdType(0); i < column.Length(); i++ {
		values[i] = column.Get(i)
	}
	return values, nil
}

func GetColumnDistinctValues(value []interface{}) []interface{} {
	return UniqueArray(value)
}

func GetRowGroupsBucket(tableId string, columnName string, hashIndex, hashBucketNumber int64) ([][]int32, error) {
	cellRowIds, err := GetValueRowGroupsBucket(tableId, columnName, hashIndex, hashBucketNumber)
	var cellRowIds32 [][]int32
	if err != nil {
		return cellRowIds32, err
	}
	for _, rowIds := range cellRowIds {
		cellRowIds32 = append(cellRowIds32, rowIds)
	}
	return cellRowIds32, err
}

var valueRowGroupsBucketCache = sync.Map{}

func GetValueRowGroupsBucket(tableId string, columnName string, hashIndex, hashBucketNumber int64) (map[interface{}][]int32, error) {
	key := fmt.Sprintf("%s:%s:%d:%d", tableId, columnName, hashIndex, hashBucketNumber)
	if val, ok := valueRowGroupsBucketCache.Load(key); ok {
		return val.(map[interface{}][]int32), nil
	}
	cellRowIds, err := rock_db.DB.CellRowIds(tableId, columnName, hashIndex, hashBucketNumber)
	var cellRowIds32 = map[interface{}][]int32{}
	if err != nil {
		return cellRowIds32, err
	}
	for val, rowIds := range cellRowIds {
		var rowIds32 []int32
		for _, rowId := range rowIds {
			rowIds32 = append(rowIds32, int32(rowId))
		}
		cellRowIds32[val] = rowIds32
	}
	//valueRowGroupsBucketCache.Store(key, cellRowIds32)
	return cellRowIds32, err
}

func GetColumnCardinality(tableId string, columnName string) (int64, error) {
	return rock_db.DB.CardinalityEstimate(tableId, columnName, 100000)
}

func GetTableLength(tableId string) (int64, error) {
	// return 10, nil // mock test schedule task tree to single node
	// return 500000000, nil // mock test schedule task tree to two nodes
	//result, err := rock_db.DB.Query(sql.SingleTable.SELECT(sql.CountAsterisk).FROM(tableId))
	//if err != nil {
	//	logger.Errorf("query table length failed, err=%v, tableId=%v", err, tableId)
	//	return 0, err
	//}
	//return result[0][0].(int64), nil
	return ColumnReader.GetTableLength(tableId)
}

// GetValueIndex 获取一张表某一列中某个值对应哪些行
func GetValueIndex(tableId string, columnName string, value interface{}, pli map[string]map[interface{}][]int32, limits ...int64) []int32 {
	if !rds_config.UseStorage {
		return pli[columnName][value]
	}
	SQL := sql.SingleTable.SELECT(row_id.ColumnName).FROM(tableId).WHERE(condition.Equal(columnName, value))
	if len(limits) == 2 {
		SQL = SQL.LIMIT(limits[0], limits[1])
	}
	lines, err := rock_db.DB.Query(SQL)
	if err != nil {
		fmt.Println(err)
	}
	rowIds := make([]int32, len(lines))
	for i, line := range lines {
		rowIds[i] = int32(line[0].(int64))
	}
	return rowIds
}

func GetGreaterValueIndex(tableId string, columnName string, value interface{}, hasEqual bool, limits ...int64) []int32 {
	SQL := sql.SingleTable.SELECT(row_id.ColumnName).FROM(tableId)
	if hasEqual {
		SQL = SQL.WHERE(condition.GreaterEq(columnName, value))
	} else {
		SQL = SQL.WHERE(condition.Greater(columnName, value))
	}
	if len(limits) == 2 {
		SQL = SQL.LIMIT(limits[0], limits[1])
	}
	lines, err := rock_db.DB.Query(SQL)
	if err != nil {
		fmt.Println(err)
	}
	rowIds := make([]int32, len(lines))
	for i, line := range lines {
		rowIds[i] = int32(line[0].(int64))
	}
	return rowIds
}

func GetLessValueIndex(tableId string, columnName string, value interface{}, hasEqual bool, limits ...int64) []int32 {
	SQL := sql.SingleTable.SELECT(row_id.ColumnName).FROM(tableId)
	if hasEqual {
		SQL = SQL.WHERE(condition.LessEq(columnName, value))
	} else {
		SQL = SQL.WHERE(condition.Less(columnName, value))
	}
	if len(limits) == 2 {
		SQL = SQL.LIMIT(limits[0], limits[1])
	}
	lines, err := rock_db.DB.Query(SQL)
	if err != nil {
		fmt.Println(err)
	}
	rowIds := make([]int32, len(lines))
	for i, line := range lines {
		rowIds[i] = int32(line[0].(int64))
	}
	return rowIds
}

// GetSchemaInfo 获取表的schema信息,包括表名,行数,列名和列类型
func GetSchemaInfo(tableId string) (string, int, map[string]string, map[string]int) {
	table, err := v2.Open(tableId)
	if err != nil {
		logger.Error(err)
		return tableId, 0, map[string]string{}, map[string]int{}
	}
	table.SetColumnMaxMappingNum(1)
	name := table.TableName()
	length := table.Length()
	m := map[string]string{}
	colName2Id := map[string]int{}
	colNames := table.ColumnNames()[2:]
	typeStrings := table.ColumnTypeStrings()[2:]

	for i := 0; i < table.ColumnNumber()-2; i++ {
		colId, _ := table.ColumnId(colNames[i])
		colName2Id[colNames[i]] = colId
		typeStr := typeStrings[i]
		if typeStr == "bigdecimal" {
			typeStr = rds_config.FloatType
		}
		m[colNames[i]] = typeStr
	}
	return name, int(length), m, colName2Id
}

func GetSchemaInfoNew(tableId string) (string, int, map[string]string, map[string]int, error) {
	table, err := v2.Open(tableId)
	if err != nil {
		logger.Error(err)
		return "", 0, nil, nil, err
	}
	table.SetColumnMaxMappingNum(1)
	name := table.TableName()
	length := table.Length()
	m := map[string]string{}
	colName2Id := map[string]int{}
	colNames := table.ColumnNames()[2:]
	typeStrings := table.ColumnTypeStrings()[2:]

	for i := 0; i < table.ColumnNumber()-2; i++ {
		colId, _ := table.ColumnId(colNames[i])
		colName2Id[colNames[i]] = colId
		typeStr := typeStrings[i]
		if typeStr == "bigdecimal" {
			typeStr = rds_config.FloatType
		}
		m[colNames[i]] = typeStr
	}
	return name, int(length), m, colName2Id, nil
}

func SaveRule(rule rds.Rule, gv *global_variables.GlobalV) bool {
	gv.WriteRulesChan <- rule
	//r := fmt.Sprintf("rule: %v, xySupp: %v, xSupp: %v, cr: %v, ftr:%v", rule.Ree, rule.XySupp, rule.XSupp, rule.CR, rule.FTR)
	//gv.RulesLock.Lock()
	//gv.Rules = append(gv.Rules, r)
	//gv.RulesLock.Unlock()
	return true
}

func UniqueArray(arr []any) []any {
	type ValCnt struct {
		pos int
		cnt int
	}
	uniq := make(map[any]int)
	valCnts := make([]ValCnt, 0, len(arr))
	for i := 0; i < len(arr); i++ {
		if pos, ok := uniq[arr[i]]; ok {
			valCnts[pos].cnt++
			continue
		}
		uniq[arr[i]] = len(valCnts)
		valCnts = append(valCnts, ValCnt{i, 1})
	}
	sort.SliceStable(valCnts, func(i, j int) bool {
		return valCnts[i].cnt > valCnts[j].cnt
	})
	result := make([]any, 0, len(valCnts))
	for i := 0; i < len(valCnts); i++ {
		result = append(result, arr[valCnts[i].pos])
	}
	return result
}

func UniqueArrayFilterNil(arr []interface{}) []interface{} {
	var result []interface{}
	tmp := make(map[interface{}]bool)
	for _, value := range arr {
		tmp[value] = true
	}
	for key := range tmp {
		if key == nil {
			continue
		}
		result = append(result, key)
	}
	return result
}

func SaveConflict(taskId int64, ruleId string, left int32, right []int32, gv *global_variables.GlobalV) error {
	return nil
	if len(right) < 1 {
		return nil
	}
	ruleIdi, _ := strconv.ParseInt(ruleId, 10, 64)
	gv.RowConflictSizeLock.Lock()
	size := int32(len(right))
	gv.RowConflictSize[left] = gv.RowConflictSize[left] + size
	gv.RowConflictSizeLock.Unlock()
	//specific_business.RuleErrorStorage.PutPair(taskId, int64(99999), int64(left), []int64{gv.RowConflictSize[left]})
	//specific_business.RuleErrorStorage.AppendPair(taskId, int64(99999), int64(left), *((*[]int64)(unsafe.Pointer(&right))))
	//return specific_business.RuleErrorStorage.PutPair(taskId, ruleIdi, int64(left), *((*[]int64)(unsafe.Pointer(&right))))
	specific_business.RuleErrorStorage.PutPair32(taskId, int64(99999), left, []int32{gv.RowConflictSize[left]})
	specific_business.RuleErrorStorage.PutPair32(taskId, ruleIdi, left, right)
	return nil
}

func SaveConflicts(taskId int64, ruleId string, data map[int32][]int32, gv *global_variables.GlobalV) error {
	ruleIdi, _ := strconv.ParseInt(ruleId, 10, 64)
	allRuleConflict := make(map[int32][]int32)
	gv.RowConflictSizeLock.Lock()
	for left, right := range data {
		size := gv.RowConflictSize[left] + int32(len(right))
		gv.RowConflictSize[left] = size
		allRuleConflict[left] = []int32{size}
	}
	gv.RowConflictSizeLock.Unlock()
	specific_business.RuleErrorStorage.PutMapI32(taskId, int64(99999), allRuleConflict)
	return specific_business.RuleErrorStorage.PutMapI32(taskId, ruleIdi, data)
}

func SavePositiveExample(taskId int64, ruleId string, left int32, right []int32) error {
	return nil
	if len(right) < 1 {
		return nil
	}
	ruleIdi, _ := strconv.ParseInt(ruleId, 10, 64)
	specific_business.PositiveExampleStorage.PutPair32(taskId, ruleIdi, left, right)
	return nil
}

func SaveXSatisfyRows(taskId int64, ruleId int, rowSize int, rowIds []int32) {
	return
	specific_business.RecordRuleRelativeRowIds(taskId, int64(ruleId), int32(rowSize), rowIds)
	return
}

func SaveHasConflictRows(taskId int64, ruleId int, rowSize int, rowIds []int32) {
	return
	specific_business.RecordRuleRelativeRowIds(taskId, int64(ruleId), int32(rowSize+1), rowIds)
	return
}

func DeleteTaskConflict(taskId int64) error {
	return nil
	err := specific_business.RuleErrorStorage.DeleteTaskId(taskId)
	if err != nil {
		return err
	}
	return nil
}

// DeleteRuleConflict */
func DeleteRuleConflict(taskId int64, ruleId int64, opLogId int64) error {
	return nil
	startTime := time.Now().UnixMilli()
	ruleConflict, err := specific_business.RuleErrorStorage.GetRuleErrorCnt(taskId, ruleId)
	if err != nil {
		logger.Errorf("[DeleteRuleConflict] Storage 获取规则的冲突数失败, taskId=%s|ruleId=%d|error=%v", taskId, ruleId, err)
		return err
	}

	allConflictMap, err := specific_business.RuleErrorStorage.GetMap(taskId, int64(99999))
	if err != nil {
		logger.Errorf("[DeleteRuleConflict] Storage 获取99999规则的冲突数失败, taskId=%s|error=%v", taskId, err)
		return err
	}

	// 1、更新99999
	if len(allConflictMap) > 0 {
		for leftRowId, rightLen := range ruleConflict {
			update99999InMemory(allConflictMap, taskId, ruleId, int64(leftRowId), int64(rightLen))
		}
	}
	putMapStartTime := time.Now().UnixMilli()
	//err = specific_business.RuleErrorStorage.PutMapTransaction(opLogId, taskId, int64(99999), allConflictMap)
	err = specific_business.RuleErrorStorage.PutMap(taskId, int64(99999), allConflictMap)
	if err != nil {
		logger.Errorf("[DeleteRuleConflict] Storage 更新99999冲突数失败, taskId=%s|error=%v", taskId, err)
		return err
	}
	putMapUseTime := time.Now().UnixMilli() - putMapStartTime

	useTime := time.Now().UnixMilli() - startTime
	logger.Debugf("[DeleteRuleConflict] 忽略规则:taskId=%d|ruleId=%d, PutMapTransaction耗时:%d(ms), 总耗时:%d(ms)", taskId, ruleId, putMapUseTime, useTime)
	//// 2、更新指定规则
	////err = specific_business.RuleErrorStorage.DeleteMap(taskId, ruleId)
	//err = specific_business.RuleErrorStorage.DeleteMapTransaction(opLogId, taskId, ruleId)
	//if err != nil {
	//	return err
	//}

	return nil
}

// DeleteRuleRowConflict1 删除规则下指定行的冲突包括99999全量规则 */
func DeleteRuleRowConflict1(taskId int64, ruleId int64, rowId int64) error {
	return nil
	rightRowIds, err := specific_business.RuleErrorStorage.GetPair(taskId, ruleId, rowId)
	if err != nil {
		return err
	}
	if rightRowIds == nil || len(rightRowIds) == 0 {
		return nil
	}
	// 1.处理左边
	// 更新规则
	err = specific_business.RuleErrorStorage.PutPair(taskId, ruleId, rowId, []int64{})
	if err != nil {
		return err
	}
	// 更新99999
	rowSize, err := specific_business.RuleErrorStorage.GetPair(taskId, int64(99999), rowId)
	if err != nil {
		return err
	}
	if rowSize != nil && len(rowSize) > 0 {
		diffValue := rowSize[0] - int64(len(rightRowIds))
		if diffValue == 0 {
			specific_business.RuleErrorStorage.PutPair(taskId, int64(99999), rowId, []int64{})
		} else {
			specific_business.RuleErrorStorage.PutPair(taskId, int64(99999), rowId, []int64{diffValue})
		}
	}
	// 2.处理右边
	for _, rightRowId := range rightRowIds {
		// 更新规则
		conflict, err := specific_business.RuleErrorStorage.GetPair(taskId, ruleId, rightRowId)
		if err != nil {
			return err
		}
		if conflict == nil || len(conflict) == 0 {
			continue
		}
		delRowIds := DeleteDuplicatesRowId(conflict, rowId)
		err = specific_business.RuleErrorStorage.PutPair(taskId, ruleId, rightRowId, delRowIds)
		if err != nil {
			return err
		}
		// 更新99999
		rowSize, err := specific_business.RuleErrorStorage.GetPair(taskId, int64(99999), rightRowId)
		if err != nil {
			return err
		}
		if rowSize != nil && len(rowSize) > 0 {
			diffValue := rowSize[0] - 1 // 一般情况int64(len(rightRowIds)-len(delRowIds))的值为1
			if diffValue == 0 {
				specific_business.RuleErrorStorage.PutPair(taskId, int64(99999), rightRowId, []int64{})
			} else {
				specific_business.RuleErrorStorage.PutPair(taskId, int64(99999), rightRowId, []int64{diffValue})
			}
		}
	}

	return updateRuleConflictCount(taskId, ruleId)
}

// DeleteRuleRowConflict 删除规则下指定行的冲突包括99999全量规则 */
func DeleteRuleRowConflict(taskId int64, ruleId int64, rowId int64, opLogId int64) (delConflictSize int, err error) {
	return 0, nil
	delConflictSize = 0
	rightRowIds, err := specific_business.RuleErrorStorage.GetPair(taskId, ruleId, rowId)
	if err != nil {
		logger.Error(err)
		return delConflictSize, err
	}
	if rightRowIds == nil || len(rightRowIds) == 0 {
		return delConflictSize, nil
	}
	// 1.处理左边
	// 更新规则
	err = PutPairOppositeOfUpdateRowOrCell(taskId, ruleId, rowId, []int64{}, opLogId)
	if err != nil {
		logger.Error(err)
		return delConflictSize, err
	}
	// 记录规则删除了多少冲突
	delConflictSize += len(rightRowIds)

	// 更新99999
	rowSize, err := specific_business.RuleErrorStorage.GetPair(taskId, int64(99999), rowId)
	if err != nil {
		logger.Error(err)
		return delConflictSize, err
	}
	if rowSize != nil && len(rowSize) > 0 {
		diffValue := rowSize[0] - int64(len(rightRowIds))
		if diffValue == 0 {
			err = PutPairOppositeOfUpdateRowOrCell(taskId, int64(99999), rowId, []int64{}, opLogId)
		} else {
			err = PutPairOppositeOfUpdateRowOrCell(taskId, int64(99999), rowId, []int64{diffValue}, opLogId)
		}
		if err != nil {
			logger.Error(err)
			return delConflictSize, err
		}
	}

	// 2.处理右边
	for _, rightRowId := range rightRowIds {
		// 更新规则
		conflict, err := specific_business.RuleErrorStorage.GetPair(taskId, ruleId, rightRowId)
		if err != nil {
			logger.Errorf("[DeleteRuleRowConflict] Storage GetPair失败, taskId=%s|ruleId=%d|rowId=%d", taskId, ruleId, rightRowId)
			continue
		}
		if conflict == nil || len(conflict) == 0 {
			continue
		}
		delRowIds := DeleteDuplicatesRowId(conflict, rowId)
		err = PutPairOppositeOfUpdateRowOrCell(taskId, ruleId, rightRowId, delRowIds, opLogId)
		if err != nil {
			logger.Errorf("[DeleteRuleRowConflict] Storage PutPair失败, taskId=%s|ruleId=%d|rightRowId=%d", taskId, ruleId, rightRowId)
			continue
		}
		// 记录规则删除了多少冲突
		delConflictSize++

		// 更新99999
		rowSize, err = specific_business.RuleErrorStorage.GetPair(taskId, int64(99999), rightRowId)
		if err != nil {
			continue
		}
		if rowSize != nil && len(rowSize) > 0 {
			diffValue := rowSize[0] - 1 // 一般情况int64(len(rightRowIds)-len(delRowIds))的值为1
			if diffValue == 0 {
				err = PutPairOppositeOfUpdateRowOrCell(taskId, int64(99999), rightRowId, []int64{}, opLogId)
			} else {
				err = PutPairOppositeOfUpdateRowOrCell(taskId, int64(99999), rightRowId, []int64{diffValue}, opLogId)
			}
			if err != nil {
				logger.Errorf("[DeleteRuleRowConflict] Storage 99999规则PutPair失败, taskId=%s|rowId=%d", taskId, rightRowId)
			}
		}
	}

	return delConflictSize, nil
}

func update99999InMemory(allConflict map[int64][]int64, taskId int64, ruleId int64, rowId int64, modifyVal int64) {
	if rowSize, ok := allConflict[rowId]; ok {
		diffVal := rowSize[0] - modifyVal
		if diffVal < 0 {
			logger.Warnf("[updateRuleRowDiffWith99999] 差值小于零, taskId=%s|ruleId=%d|rowId=%d|val1=%d|val2=%d", taskId, ruleId, rowId, rowSize[0], modifyVal)
			return
		}

		if diffVal == 0 {
			allConflict[rowId] = []int64{}
		} else {
			allConflict[rowId] = []int64{diffVal}
		}
	}
}

func updateRuleConflictCount(taskId int64, ruleId int64) error {
	return nil
	ruleErrorMap, err := specific_business.RuleErrorStorage.GetMap(taskId, ruleId)
	if err != nil {
		logger.Errorf("[updateRuleConflictCount] 获取存储中规则的冲突信息失败, taskId=%s|ruleId=%d|error=%v", taskId, ruleId, err)
		return err
	}

	conflictCount := 0
	for _, rightRowIds := range ruleErrorMap {
		rightRowIdsLen := len(rightRowIds)
		if rightRowIdsLen > 0 {
			conflictCount += rightRowIdsLen
		}
	}

	logger.Debugf("[updateRuleConflictCount] 更新Rule表中规则的冲突数, taskId=%s|ruleId=%d|count=%d", taskId, ruleId, conflictCount)
	po.UpdateConflictCount(ruleId, conflictCount, db.DB)
	return nil
}

// DeleteRuleRowConflictWithout99999 删除规则下指定行的冲突不包括99999全量规则 + 更新PG的Rule表规则的冲突数 */
func DeleteRuleRowConflictWithout99999(taskId int64, ruleId int64, rowId int64) error {
	return nil
	conflictData, err := specific_business.RuleErrorStorage.GetMap(taskId, ruleId)
	if err != nil {
		return err
	}

	for leftRowId, rightRowIds := range conflictData {
		if leftRowId == rowId {
			err1 := specific_business.RuleErrorStorage.PutPair(taskId, ruleId, leftRowId, []int64{})
			if err1 != nil {
				return err1
			}
		} else {
			delRowIds := DeleteDuplicatesRowId(rightRowIds, rowId)
			err2 := specific_business.RuleErrorStorage.PutPair(taskId, ruleId, leftRowId, delRowIds)
			if err2 != nil {
				return err2
			}
		}
	}

	return updateRuleConflictCount(taskId, ruleId)
}

// DeleteRowId 会修改原来的切片,如果rowId有重复只会删除一个 /*
func DeleteRowId(rowIds []int64, delRowId int64) []int64 {
	// 只需检查key看是否已删除过，value使用空结构体，不占内存
	processed := make(map[int64]struct{})

	j := 0
	for _, rowId := range rowIds {
		_, ok := processed[delRowId]
		if rowId == delRowId && !ok {
			processed[delRowId] = struct{}{}
			continue
		}

		rowIds[j] = rowId
		j++
	}

	return rowIds[:j]
}

// DeleteDuplicatesRowId 会修改原来的切片,删除切片中所有的rowId /*
func DeleteDuplicatesRowId(rowIds []int64, delRowId int64) []int64 {
	j := 0
	for _, rowId := range rowIds {
		if rowId == delRowId {
			continue
		}

		rowIds[j] = rowId
		j++
	}

	return rowIds[:j]
}

func SaveIncrementConflict(taskId int64, ruleId int64, left int, rights []int, isConstant bool, opLogId int64) (err error) {
	if len(rights) < 1 {
		return nil
	}
	err = IncrementUpdate99999Conflict(taskId, left, rights, isConstant, opLogId)
	if err != nil {
		return err
	}
	return IncrementUpdateRuleConflict(taskId, ruleId, left, rights, isConstant, opLogId)
}

func IncrementUpdate99999Conflict(taskId int64, left int, rights []int, isConstant bool, opLogId int64) (err error) {
	err = IncrementUpdate99999Size(taskId, left, len(rights), opLogId)
	if err != nil {
		return err
	}

	if isConstant {
		return nil
	}
	for _, right := range rights {
		err = IncrementUpdate99999Size(taskId, right, 1, opLogId)
		if err != nil {
			return err
		}
	}
	return nil
}

func IncrementUpdateRuleConflict(taskId int64, ruleId int64, left int, rights []int, isConstant bool, opLogId int64) (err error) {
	err = IncrementUpdateRuleRowConflict(taskId, ruleId, left, rights, opLogId)
	if err != nil {
		return err
	}

	if isConstant {
		return nil
	}
	for _, right := range rights {
		err = IncrementUpdateRuleRowConflict(taskId, ruleId, right, []int{left}, opLogId)
		if err != nil {
			return err
		}
	}

	return nil
}

func IncrementUpdateRuleRowConflict(taskId int64, ruleId int64, left int, rights []int, opLogId int64) (err error) {
	return nil
	pair, err := specific_business.RuleErrorStorage.GetPair(taskId, ruleId, int64(left))
	if err != nil {
		logger.Errorf("[IncrementUpdateRuleRowConflict] Storage Failed to GetPair, rowId=%d", left)
		return err
	}

	if len(pair) == 0 {
		PutPairOppositeOfUpdateRowOrCell(taskId, ruleId, int64(left), *((*[]int64)(unsafe.Pointer(&rights))), opLogId)
	} else {
		// pair与rights合并
		pair = append(pair, *((*[]int64)(unsafe.Pointer(&rights)))...)
		PutPairOppositeOfUpdateRowOrCell(taskId, ruleId, int64(left), pair, opLogId)
	}

	return nil
}

func IncrementUpdate99999Size(taskId int64, left int, incSize int, opLogId int64) (err error) {
	return nil
	pair, err := specific_business.RuleErrorStorage.GetPair(taskId, int64(99999), int64(left))
	if err != nil {
		logger.Errorf("[IncrementUpdate99999Size] Storage Failed to GetPair, rowId=%d", left)
		return err
	}
	var sum int64 = 0
	if pair == nil || len(pair) == 0 {
		sum = int64(incSize)
	} else {
		sum = pair[0] + int64(incSize)
	}

	return PutPairOppositeOfUpdateRowOrCell(taskId, int64(99999), int64(left), []int64{sum}, opLogId)
}

func StartErrorCheck(taskId int64) {
	specific_business.RuleErrorStorage.StartErrorCheck(taskId)
	specific_business.PositiveExampleStorage.StartErrorCheck(taskId)
}

func FinishErrorCheck(taskId int64, tableId string) {
	go specific_business.RuleErrorStorage.FinishErrorCheck(taskId, tableId)
	go specific_business.PositiveExampleStorage.FinishErrorCheck(taskId, tableId)
}

func SaveIncrementPositiveExample(taskId int64, ruleId int64, left int, rights []int, isConstant bool, opLogId int64) (err error) {
	if len(rights) < 1 {
		return nil
	}
	return IncrementUpdateRulePositiveExample(taskId, ruleId, left, rights, isConstant, opLogId)
}

func IncrementUpdateRulePositiveExample(taskId int64, ruleId int64, left int, rights []int, isConstant bool, opLogId int64) (err error) {
	err = IncrementUpdateRuleRowPositiveExample(taskId, ruleId, left, rights, opLogId)
	if err != nil {
		return err
	}

	if isConstant {
		return nil
	}
	for _, right := range rights {
		err = IncrementUpdateRuleRowPositiveExample(taskId, ruleId, right, []int{left}, opLogId)
		if err != nil {
			return err
		}
	}

	return nil
}

func IncrementUpdateRuleRowPositiveExample(taskId int64, ruleId int64, left int, rights []int, opLogId int64) (err error) {
	return nil
	pair, err := specific_business.PositiveExampleStorage.GetPair(taskId, ruleId, int64(left))
	if err != nil {
		logger.Errorf("[IncrementUpdateRuleRowPositiveExample] Storage Failed to GetPair, rowId=%d", left)
		return err
	}

	if pair == nil || len(pair) == 0 {
		PutPairPositiveOfUpdateRowOrCell(taskId, ruleId, int64(left), *((*[]int64)(unsafe.Pointer(&rights))), opLogId)
	} else {
		// pair与rights合并
		pair = append(pair, *((*[]int64)(unsafe.Pointer(&rights)))...)
		PutPairPositiveOfUpdateRowOrCell(taskId, ruleId, int64(left), pair, opLogId)
	}

	return nil
}

func DeleteRuleRowPositiveExample(taskId int64, ruleId int64, rowId int64, opLogId int64) error {
	return nil
	rightRowIds, err := specific_business.PositiveExampleStorage.GetPair(taskId, ruleId, rowId)
	if err != nil {
		return err
	}

	if rightRowIds == nil || len(rightRowIds) == 0 {
		return nil
	}
	err = PutPairPositiveOfUpdateRowOrCell(taskId, ruleId, rowId, []int64{}, opLogId)
	if err != nil {
		return err
	}
	for _, rightRowId := range rightRowIds {
		rowIds, _ := specific_business.PositiveExampleStorage.GetPair(taskId, ruleId, rightRowId)
		if rowIds != nil && len(rowIds) > 0 {
			delRowIds := DeleteDuplicatesRowId(rowIds, rowId)
			PutPairPositiveOfUpdateRowOrCell(taskId, ruleId, rightRowId, delRowIds, opLogId)
		}
	}
	return nil
}

// GetColumnValuesOfRows 获取某列指定行的列值 */
func GetColumnValuesOfRows(tableId string, columnName string, rows ...interface{}) ([]interface{}, error) {
	SQL := sql.SingleTable.SELECT(columnName).FROM(tableId).WHERE(condition.In(row_id.ColumnName, rows))

	lines, err := rock_db.DB.Query(SQL)
	if err != nil {
		return nil, err
	}
	data := make([]interface{}, len(lines))
	for idx, line := range lines {
		data[idx] = line[0]
	}

	for i, value := range data {
		if value != nil {
			if d, ok := value.(decimal.BigDecimal); ok {
				data[i] = d.FloatVal()
			}
		}
	}

	return data, nil
}

func StoreHistoryTable(tableId string, txId int64) error {
	historyTableName := transaction_handler_utils.HistoryTableName(tableId, txId)
	err := rock_db.DB.CopyTable(tableId, historyTableName)
	return err
}

func PutPairOppositeOfUpdateRowOrCell(taskId int64, ruleId int64, left int64, rights []int64, opLogId int64) error {
	return nil
	err := specific_business.RuleErrorStorage.PutPairTransaction(opLogId, taskId, ruleId, left, rights)
	if err != nil {
		logger.Errorf("[PutPairOppositeOfUpdateRowOrCell] Storage PutPair反例失败, error=%v", err)
		return err
	}

	err = specific_business.RuleErrorStorage.PutPairTransactionFinish(opLogId)
	if err != nil {
		logger.Error(err)
		return err
	}
	return nil
}

func PutPairPositiveOfUpdateRowOrCell(taskId int64, ruleId int64, left int64, rights []int64, opLogId int64) error {
	return nil
	err := specific_business.PositiveExampleStorage.PutPairTransaction(opLogId, taskId, ruleId, left, rights)
	if err != nil {
		logger.Errorf("[PutPairPositiveOfUpdateRowOrCell] Storage PutPair正例失败, error=%v", err)
		return err
	}
	err = specific_business.PositiveExampleStorage.PutPairTransactionFinish(opLogId)
	if err != nil {
		logger.Error(err)
		return err
	}
	return nil
}

func StartCheckRule(taskId, ruleId int64, leftArrayCapacity int) {
	return
	specific_business.RuleErrorStorage.StartRuleErrorCheck(taskId, ruleId, leftArrayCapacity)
	specific_business.PositiveExampleStorage.StartRuleErrorCheck(taskId, ruleId, leftArrayCapacity)
}

func StartCheck9Rule(taskId int64, leftArrayCapacity int) {
	return
	specific_business.RuleErrorStorage.StartRuleErrorCheck(taskId, 99999, leftArrayCapacity)
	specific_business.PositiveExampleStorage.StartRuleErrorCheck(taskId, 99999, leftArrayCapacity)
}

// UpdateCellValue 改数据只在主节点，这样最保险。
// 或者按照 config.ShardSize 分片大小，同一个分片的数据保证在一个节点上改。不同分片的可以同时修改。比较麻烦
func UpdateCellValue(tableId string, columnId string, rowId int32, value any) error {
	logger.Debug("修改 ", tableId, " ", columnId, " ", rowId, " ", value)
	_, err := rock_db.DB.Exec(sql.SingleTable.UPDATE(tableId).SET(columnId, value).WHERE(condition.Equal(row_id.ColumnName, rowId)), true)
	return err
}

// SyncDataBase 全部修改做完后，主节点调用
func SyncDataBase() {
	rock_db.DB.Sync()
}

// DropCacheDataBase 先主节点调用 SyncDataBase，全部节点广播调用 DropCacheDataBase
func DropCacheDataBase() {
	rock_db.DB.ReleaseCache()
	ColumnReader.ClearCache()
}

func CPUBusy(n int64) {
	cpu_busy.CPUBusy(n)
}

type Comparable interface {
	int32 | string | int | int64
}

func SortSlice[T Comparable](s []T) {
	sort.Slice(s, func(i, j int) bool {
		return s[i] < s[j]
	})
}

func BinaryFound[T Comparable](s []T, e T) (int, bool) {
	return sort.Find(len(s), func(i int) int {
		if e > s[i] {
			return 1
		} else if e < s[i] {
			return -1
		} else {
			return 0
		}
	})
}

func Intersection[T Comparable](s1, s2 []T) ([]T, bool) {
	SortSlice(s1)
	SortSlice(s2)
	var r []T
	for _, e := range s1 {
		if _, ok := BinaryFound(s2, e); ok {
			r = append(r, e)
		}
	}
	return r, len(r) > 0
}

func Union[T Comparable](s1, s2 []T) []T {
	s := append([]T{}, s1...)
	s = append(s, s2...)
	r := utils.Distinct(s)
	return r
}

func ForeachParallel[E any](parallelDegree int, es []E, f func(E)) {
	pool := make(chan struct{}, parallelDegree)
	wg := sync.WaitGroup{}
	for _, e := range es {
		wg.Add(1)
		pool <- struct{}{}
		go func(e E) {
			defer func() { <-pool }()
			defer wg.Done()
			f(e)
		}(e)
	}
	wg.Wait()
}

type HashBucketMeta struct {
	Size     int   // hash 桶的大小（元素数目）
	MinRowId int32 // hash 桶内元素最小行号
	MaxRowId int32 // hash 桶内元素最大行号
}

// HashBucketInfo 对指定的表的列进行 hash 分桶，返回每个桶的统计信息
func HashBucketInfo(tableId string, columnName string, hashBucketNumber int) ([]HashBucketMeta, error) {
	metas := make([]HashBucketMeta, hashBucketNumber)
	for i := range metas {
		// metas[i].MaxRowId = 0
		metas[i].MinRowId = math.MaxInt32
	}
	length, err := TableLengthNotRegradingDeleted(tableId)
	if err != nil {
		return nil, err
	}
	batchSize := int64(100 * 100 * 100)
	start := int64(0)
	for start < length {
		SQL := sql.SingleTable.SELECT(row_id.ColumnName, columnName).FROM(tableId).WHERE(
			condition.GreaterEq(row_id.ColumnName, start), condition.Less(row_id.ColumnName, start+batchSize),
			condition.Equal(delete_flag.ColumnName, delete_flag.NotDelete))
		rcSlice, err := rock_db.DB.Query(SQL)
		if err != nil {
			logger.Error(err)
			return metas, err
		}
		for _, rc := range rcSlice {
			rowId := int32(rc[0].(int64))
			value := rc[1]
			bucketId := hasher.Hash(value) % int64(hashBucketNumber)
			metas[bucketId].Size++
			metas[bucketId].MaxRowId = Max(rowId, metas[bucketId].MaxRowId)
			metas[bucketId].MinRowId = Min(rowId, metas[bucketId].MinRowId)
		}
		start += batchSize
	}
	return metas, err
}

// CrossColumnHashBucketSizes 对两个列进行 hash 分桶，返回每对桶中更大的桶的元素数目
func CrossColumnHashBucketSizes(table1, column1, table2, column2 string, hashBucketNumber int) ([]int, error) {
	bucketSizes := make([]int, hashBucketNumber)
	leftMetaArr, err := HashBucketInfo(table1, column1, hashBucketNumber)
	if err != nil {
		return bucketSizes, err
	}
	rightMetaArr, err := HashBucketInfo(table2, column2, hashBucketNumber)
	if err != nil {
		return bucketSizes, err
	}
	for i := 0; i < hashBucketNumber; i++ {
		bucketSizes[i] = Max(leftMetaArr[i].Size, rightMetaArr[i].Size)
	}
	return bucketSizes, nil
}

// CrossTableData 跨表表数据抽取，表按照传入的谓词连接
// [连接组合]map[表名]map[列名][数据]interface{}
func CrossTableData(crossTablePredicates []rds.Predicate) ([]map[string]map[string][]interface{}, error) {
	return nil, nil
}

// TableLengthNotRegradingDeleted 获取表长度
func TableLengthNotRegradingDeleted(tableId string) (tableLength int64, err error) {
	//lenW, err := rock_db.DB.Query(sql.SingleTable.SELECT(sql.CountAsterisk).FROM(tableId))
	//if err != nil {
	//	logger.Error(err)
	//	return 0, err
	//}
	//return lenW[0][0].(int64), nil
	return ColumnReader.GetTableLength(tableId)
}

// TableDataQuery 表数据抽取
// map[string表名]map[int32行号]struct{}
// map[string表名]map[string列名]map[int32行号]interface{}值
func TableDataQuery(tableRowIds map[string]*bitset.BitSet, table2Columns map[tableName][]columnName,
	tableRanges map[string]Pair[int64, int64]) (tableData map[string]map[string]map[int32]interface{}, err error) {
	var x = &XTableBatchQueryCallBack{}
	tableData = make(map[tableName]map[columnName]map[rowId]interface{}, len(tableRowIds))
	for tabName, rowIds := range tableRowIds {
		columnData := map[columnName]map[rowId]interface{}{}
		tableData[tabName] = columnData

		if rowIds.Len() == 0 {
			continue
		}

		columns := table2Columns[tabName]
		if len(columns) == 0 {
			continue
		}
		columnMap := make(map[string]bool, len(columns))
		for _, column := range columns {
			columnMap[column] = true
		}

		infos, err := rock_db.DB.TableColumnInfos(tabName)
		if err != nil {
			return tableData, err
		}
		columnNames := Mapping(infos, func(e *itable.ColumnInfo) columnName {
			return e.ColumnName
		})
		for _, colName := range columnNames {
			columnData[colName] = map[rowId]interface{}{}
		}
		err = TableBatchQueryCallBack(tabName, tableRanges[tabName].Left, tableRanges[tabName].Right, config2.ShardSize, x, func(lines []line.Line) error {
			for _, row := range lines {
				rid, row := PopNumber[int64, int32](row)
				if rowIds.Test(uint(rid)) {
					for cid, val := range row {
						colName := columnNames[cid]
						if columnMap[colName] {
							columnData[colName][rid] = val
						}
					}
				}
			}
			return nil
		})
		if err != nil {
			return tableData, err
		}
	}
	return tableData, nil
}

func TableDataQueryNew(tableRowIds map[string]*bitset.BitSet, meta *metadata.Metadata,
	tableRanges map[string]Pair[int64, int64]) (tableData [][][]any, tableRowIdList [][]int32, err error) {
	var x = &XTableBatchQueryCallBack{}
	tableNumber := len(meta.Table2Index)
	tableData = make([][][]any, tableNumber)
	tableRowIdList = make([][]int32, tableNumber)
	for tabName, rowIds := range tableRowIds {
		column2Index := meta.Column2Index[tabName]
		var columnData = make([][]any, len(column2Index))
		var rowIdList []int32

		allColumnSet := make(map[string]bool, len(column2Index))
		//for colName := range column2Index {
		//	allColumnSet[colName] = true
		//}
		for _, colName := range meta.ColumnsOfSingleBatchRule[tabName] {
			allColumnSet[colName] = true
		}

		var allColumns []string
		{
			infos, err := rock_db.DB.TableColumnInfos(tabName)
			if err != nil {
				return nil, nil, err
			}
			allColumns = Mapping(infos, func(e *itable.ColumnInfo) columnName {
				return e.ColumnName
			})
		}

		err = TableBatchQueryCallBack(tabName, tableRanges[tabName].Left, tableRanges[tabName].Right, config2.ShardSize, x, func(lines []line.Line) error {
			for _, row := range lines {
				rid, row := PopNumber[int64, int32](row)
				if rowIds.Test(uint(rid)) {
					rowIdList = append(rowIdList, rid)
					for cid, val := range row {
						colName := allColumns[cid]
						if allColumnSet[colName] {
							columnData[column2Index[colName]] = append(columnData[column2Index[colName]], val)
						}
					}
				}
			}
			return nil
		})
		if err != nil {
			return nil, nil, err
		}

		tableIndex := meta.Table2Index[tabName]
		tableData[tableIndex] = columnData
		tableRowIdList[tableIndex] = rowIdList
	}
	return tableData, tableRowIdList, nil
}

func TableColumnNameIndexMap(tableName string) (map[string]int, error) {
	infos, err := rock_db.DB.TableColumnInfos(tableName)
	if err != nil {
		return nil, err
	}
	m := make(map[string]int, len(infos))
	for i, info := range infos {
		m[info.ColumnName] = i
	}
	return m, nil
}

// preloadTableData 预先加载数据，map[string]Pair[int64, int64] 表名 -> [start, limit]
func preloadTableData(tableRanges map[string]Pair[int64, int64]) error {
	s := time.Now()
	defer func() {
		logger.Info("加载数据 ", fmt.Sprintf("%v", tableRanges), "耗时 ", utils.DurationString(s))
	}()
	var x XTableBatchQueryCallBack
	for tabName, rangeInfo := range tableRanges {
		{
			length, err := TableLengthNotRegradingDeleted(tabName)
			if err != nil {
				logger.Error(err)
			}
			logger.Info("获取表 ", tabName, " 长度信息 ", length)
		}

		//var columns = []string{row_id.ColumnName}
		//infos, err := rock_db.DB.TableColumnInfos(tabName)
		//if err != nil {
		//	logger.Error(err)
		//	return err
		//}
		//for _, info := range infos {
		//	columns = append(columns, info.ColumnName)
		//}

		offset := rangeInfo.Left
		limit := rangeInfo.Right
		if limit == 0 {
			limit = config2.ShardSize
		}

		err := TableBatchQueryCallBack(tabName, offset, limit, config2.ShardSize, &x, func(lines []line.Line) error {
			if len(lines) > 0 {
				logger.Info("加载表 ", tabName, " offset ", lines[0][0], " limit ", limit)
			}
			return nil
		})
		if err != nil {
			logger.Error(err)
		}

		//bound := offset + limit
		//for batch := int64(0); true; batch += config2.ShardSize {
		//	start := offset + batch
		//	if start >= bound {
		//		break
		//	}
		//	end := start + config2.ShardSize
		//	if end > bound {
		//		end = bound
		//	}
		//	logger.Info("加载表 ", tabName, " 数据范围 offset=", start, " end=", end)
		//	SQL := sql.SingleTable.SELECT(columns...).FROM(tabName)
		//	SQL.WHERE(condition.GreaterEq(row_id.ColumnName, start))
		//	SQL.WHERE(condition.Less(row_id.ColumnName, end))
		//	_, err := rock_db.DB.Query(SQL)
		//	if err != nil {
		//		logger.Error(err)
		//		return err
		//	}
		//	if end == bound {
		//		break
		//	}
		//}
	}
	return nil
}

type XTableBatchQueryCallBack struct {
	LoadTime     int64 // ns
	CallBackTime int64 // ns
	IndexTime    int64 // ns
}

func (x *XTableBatchQueryCallBack) indexTimeRecord(f func()) {
	ts := time.Now()
	f()
	x.IndexTime += time.Now().UnixNano() - ts.UnixNano()
}

func TableBatchQueryCallBack(tabName tableName, offset, limit, batchSize int64, x *XTableBatchQueryCallBack,
	callback func(lines []line.Line) error) error {

	var columns = []columnName{row_id.ColumnName}
	{
		infos, err := rock_db.DB.TableColumnInfos(tabName)
		if err != nil {
			return err
		}
		columnNames := Mapping(infos, func(e *itable.ColumnInfo) columnName {
			return e.ColumnName
		})
		columns = append(columns, columnNames...)
	}

	start := offset
	var end int64
	if limit > 0 {
		end = offset + limit
	} else {
		ts := time.Now()
		length, err := TableLengthNotRegradingDeleted(tabName)
		if err != nil {
			return err
		}
		x.LoadTime += time.Now().UnixNano() - ts.UnixNano()
		end = length
	}

	for start < end {
		var batchEnd = start + batchSize
		if batchEnd > end {
			batchEnd = end
		}

		ts := time.Now()
		SQL := sql.SingleTable.SELECT(columns...).FROM(tabName).WHERE(condition.GreaterEq(row_id.ColumnName, start),
			condition.Less(row_id.ColumnName, batchEnd))
		lines, err := rock_db.DB.Query(SQL)
		if err != nil {
			logger.Error(err)
			return err
		}
		for _, row := range lines {
			for i, value := range row {
				if d, ok := value.(decimal.BigDecimal); ok {
					row[i] = d.FloatVal()
				}
			}
		}
		x.LoadTime += time.Now().UnixNano() - ts.UnixNano()
		ts = time.Now()
		err = callback(lines)
		if err != nil {
			return err
		}
		x.CallBackTime += time.Now().UnixNano() - ts.UnixNano()
		start += batchSize
		if batchEnd == end {
			break
		}
	}
	return nil
}

func tableRangeQueryCallBack(SQL *sql.SingleTableSQL, offset, limit int64, callback func(lines []line.Line)) error {
	s := SQL.DeepCopy()
	lines, err := rock_db.DB.Query(s.WHERE(condition.GreaterEq(row_id.ColumnName, offset),
		condition.Less(row_id.ColumnName, offset+limit), condition.Equal(delete_flag.ColumnName, delete_flag.NotDelete)))
	if err != nil {
		logger.Error(err)
		return err
	}
	callback(lines)
	return nil
}

func sliceMap[E1, E2 any](es []E1, converter func(E1) E2) []E2 {
	r := make([]E2, len(es))
	for i := range es {
		r[i] = converter(es[i])
	}
	return r
}

func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}

	return fmt.Sprintf("%v", v)
}
