package metadata

import (
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"time"
)

var taskMetadata = map[int64]*Metadata{} // taskId -> metadata

type Metadata struct {
	RuleMap                  map[int]*rds.Rule         // ruleId -> rule
	Table2Index              map[string]int            // tableId -> index
	Column2Index             map[string]map[string]int // tableId -> column -> index
	Table2RowSize            map[string]int64          // tableId -> rowSize
	HistogramThreshold       int
	ColumnsOfSingleBatchRule map[string][]string // tableId -> columns  单批规则涉及到的列
	Tables                   []string            // 数组的下标和Table2Index的index一致
	Columns                  map[string][]string // tableId -> columns 数组的下标和Column2Index的index一致
	//Rule2CubeKey map[string]string         // ruleId -> cubeKey
	//Bucket2Index map[string]int            // bucketId -> index  bucketId=ruleId-cubeKey
}

func InitTaskMetadata(taskId int64, ruleMap map[int]*rds.Rule, histogramThreshold int, table2RowSize map[string]int64) {
	table2Columns := getRulesColumns(ruleMap)
	tablesIndex := getTablesIndex(table2Columns)
	columnsIndex := getColumnsIndex(table2Columns)
	tables := getTables(tablesIndex)
	columns := getColumns(columnsIndex)

	taskMetadata[taskId] = &Metadata{
		RuleMap:            ruleMap,
		Table2Index:        tablesIndex,
		Column2Index:       columnsIndex,
		Table2RowSize:      table2RowSize,
		HistogramThreshold: histogramThreshold,
		Tables:             tables,
		Columns:            columns,
	}
}

func SetColumnsOfSingleBatchRule(taskId int64, ruleMap map[int]*rds.Rule) {
	taskMetadata[taskId].ColumnsOfSingleBatchRule = getRulesColumns(ruleMap)
}

func GetTable2RowSize(taskId int64) map[string]int64 {
	return taskMetadata[taskId].Table2RowSize
}

func GetMetadata() map[int64]*Metadata {
	return taskMetadata
}

func GetTaskMetadata(taskId int64) *Metadata {
	if taskMetadata == nil || len(taskMetadata) == 0 {
		logger.Warnf("[GetTaskMetadata] taskMetadata为空")
		return nil
	}
	return taskMetadata[taskId]
}

func SetMetadata(metadata map[int64]*Metadata) {
	taskMetadata = metadata
}

func GetTableCount(taskId int64) int {
	if taskMetadata == nil || len(taskMetadata) == 0 {
		logger.Warnf("[GetTableCount] taskMetadata为空")
		return 0
	}
	return len(taskMetadata[taskId].Table2Index)
}

func GetTableIndex(taskId int64, tableId string) int {
	if taskMetadata == nil || len(taskMetadata) == 0 {
		logger.Warnf("[GetTableIndex] taskMetadata为空")
		return -1
	}
	return taskMetadata[taskId].Table2Index[tableId]
}

func GetColumnIndex(taskId int64, tableId string, column string) int {
	if taskMetadata == nil || len(taskMetadata) == 0 {
		logger.Warnf("[GetColumnIndex] taskMetadata为空")
		return -1
	}
	return taskMetadata[taskId].Column2Index[tableId][column]
}

func ClearTaskMetadata(taskId int64) {
	if taskMetadata == nil || len(taskMetadata) == 0 {
		logger.Warnf("[ClearTaskMetadata] taskMetadata为空")
		return
	}
	delete(taskMetadata, taskId)
}

func ClearMetadata() {
	taskMetadata = map[int64]*Metadata{}
}

// GetTablesIndex 获取表的索引
func getTablesIndex(table2Columns map[string][]string) map[string]int {
	tablesIndex := make(map[string]int, len(table2Columns))
	index := 0
	for tableId := range table2Columns {
		tablesIndex[tableId] = index
		index++
	}
	return tablesIndex
}

// GetColumnsIndex 获取列的索引
func getColumnsIndex(table2Columns map[string][]string) map[string]map[string]int {
	columnsIndex := make(map[string]map[string]int, len(table2Columns))
	for tableId, columns := range table2Columns {
		columnIndex := make(map[string]int)
		for index, column := range columns {
			columnIndex[column] = index
		}
		columnsIndex[tableId] = columnIndex
	}
	return columnsIndex
}

// GetRulesColumns 获取规则涉及到的列名
// output param: map[string][]string table -> columns
func getRulesColumns(ruleMap map[int]*rds.Rule) map[string][]string {
	startTime := time.Now().UnixMilli()
	table2Columns := make(map[string][]string)
	table2ColumnMap := make(map[string]map[string]struct{})
	for _, rule := range ruleMap {
		lhs := rule.LhsPredicates
		predicates := make([]rds.Predicate, 0, len(lhs)+1)
		predicates = append(predicates, lhs...)
		predicates = append(predicates, rule.Rhs)
		for _, predicate := range predicates {
			if predicate.PredicateType == 0 {
				tableId := predicate.LeftColumn.TableId
				colName := predicate.LeftColumn.ColumnId
				appendColName(table2ColumnMap, tableId, colName)
			} else {
				lTabId := predicate.LeftColumn.TableId
				lColName := predicate.LeftColumn.ColumnId
				rTabId := predicate.RightColumn.TableId
				rColName := predicate.RightColumn.ColumnId
				if lTabId == rTabId && lColName == rColName {
					appendColName(table2ColumnMap, lTabId, lColName)
				} else {
					appendColName(table2ColumnMap, lTabId, lColName)
					appendColName(table2ColumnMap, rTabId, rColName)
				}
			}
		}
	}

	for tableId, colMap := range table2ColumnMap {
		columns := make([]string, 0, len(colMap))
		for column := range colMap {
			columns = append(columns, column)
		}
		table2Columns[tableId] = columns
	}
	useTime := time.Now().UnixMilli() - startTime
	logger.Infof("[GetRulesColumns] 获取 %d张表|%d条规则 的所有列,耗时:%d(ms)", len(table2Columns), len(ruleMap), useTime)
	return table2Columns
}

func appendColName(table2ColumnMap map[string]map[string]struct{}, tableId string, colName string) {
	if cols, ok := table2ColumnMap[tableId]; ok {
		cols[colName] = struct{}{}
	} else {
		table2ColumnMap[tableId] = map[string]struct{}{
			colName: {},
		}
	}
}

func getTables(table2Index map[string]int) []string {
	tables := make([]string, len(table2Index))
	for tableId, index := range table2Index {
		tables[index] = tableId
	}
	return tables
}

func getColumns(column2Index map[string]map[string]int) map[string][]string {
	columns := make(map[string][]string, len(column2Index))
	for tableId, col2Idx := range column2Index {
		cols := make([]string, len(col2Idx))
		for col, idx := range col2Idx {
			cols[idx] = col
		}
		columns[tableId] = cols
	}
	return columns
}
