package table_data

import (
	"fmt"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock_v3/rds_config"
)

const JoinTableIdPrefix = "t"

type columnInfo struct {
	tableId    string
	columnType string
}

type JoinTableData struct {
	TableValues      map[string]map[string][]interface{}           // tableId -> column -> []
	TableIndexValues map[string]map[string][]int32                 // tableId -> column -> []
	TableColumnTypes map[string]map[string]columnInfo              // tableId -> column -> type
	PLI              map[string]map[string]map[interface{}][]int32 // tableId -> column -> value -> 有序rowIds
	IndexPLI         map[string]map[string]map[int32][]int32
}

func NewJoinTableData(lhs []rds.Predicate, rhs rds.Predicate, joinTables [][][]int32, taskId int64) *JoinTableData {
	data := &JoinTableData{
		TableValues:      map[string]map[string][]interface{}{},
		TableIndexValues: map[string]map[string][]int32{},
		TableColumnTypes: map[string]map[string]columnInfo{},
		PLI:              map[string]map[string]map[interface{}][]int32{},
		IndexPLI:         map[string]map[string]map[int32][]int32{},
	}
	var joinTableIds []string
	for idx := range joinTables {
		joinTableIds = append(joinTableIds, fmt.Sprint(JoinTableIdPrefix, idx))
	}
	data.LoadSchema(lhs, rhs)
	data.LoadDataCreatePLI(joinTables, taskId)
	data.CreateIndex()
	return data
}

func (data *JoinTableData) LoadSchema(lhs []rds.Predicate, rhs rds.Predicate) {
	var predicates []rds.Predicate
	predicates = append(lhs, rhs)
	for _, predicate := range predicates {
		lJoinTableId := predicate.LeftColumn.JoinTableId
		lTableId := predicate.LeftColumn.TableId
		lColName := predicate.LeftColumn.ColumnId
		lColType := predicate.LeftColumn.ColumnType
		if columnTypes, ok := data.TableColumnTypes[lJoinTableId]; !ok {
			data.TableColumnTypes[lJoinTableId] = map[string]columnInfo{lColName: {lTableId, lColType}}
		} else {
			columnTypes[lColName] = columnInfo{lTableId, lColType}
			data.TableColumnTypes[lJoinTableId] = columnTypes
		}
		rJoinTableId := predicate.RightColumn.JoinTableId
		rTableId := predicate.RightColumn.TableId
		rColName := predicate.RightColumn.ColumnId
		rColType := predicate.RightColumn.ColumnType
		if columnTypes, ok := data.TableColumnTypes[rJoinTableId]; !ok {
			data.TableColumnTypes[rJoinTableId] = map[string]columnInfo{rColName: {rTableId, rColType}}
		} else {
			columnTypes[rColName] = columnInfo{rTableId, rColType}
			data.TableColumnTypes[rJoinTableId] = columnTypes
		}
	}
}

// LoadDataCreatePLI 加载需要的列的数据。
func (data *JoinTableData) LoadDataCreatePLI(joinTables [][][]int32, taskId int64) {
	task := TaskData[taskId]
	for idx, joinTable := range joinTables {
		joinTableId := fmt.Sprint(JoinTableIdPrefix, idx)
		//joinTableRowSize := len(joinTable[0])
		joinTableRowSize := len(joinTable)
		data.TableValues[joinTableId] = map[string][]interface{}{}
		data.PLI[joinTableId] = map[string]map[interface{}][]int32{}
		for column, info := range data.TableColumnTypes[joinTableId] {
			tableId := info.tableId
			columnValues := task.TableValues[tableId][column]
			var values = make([]interface{}, 0, joinTableRowSize)
			// 获取列的值
			tableIdIndex := task.TableIndex[tableId]
			//for _, rowId := range joinTable[tableIdIndex] {
			//	values = append(values, columnValues[rowId])
			//}
			for _, row := range joinTable {
				rowId := row[tableIdIndex]
				values = append(values, columnValues[rowId])
			}

			data.TableValues[joinTableId][column] = values

			pli := map[interface{}][]int32{}
			for rowId, value := range values {
				pli[value] = append(pli[value], int32(rowId))
			}
			data.PLI[joinTableId][column] = pli
		}
	}
}

// CreateIndex 创建索引
func (data *JoinTableData) CreateIndex() {
	idProvider := map[interface{}]int32{}
	idProvider[nil] = rds_config.NilIndex
	idProvider[""] = rds_config.NilIndex
	providerCnt := int32(0)

	for tableName, columnValues := range data.TableValues {
		data.TableIndexValues[tableName] = map[string][]int32{}
		for columnName, values := range columnValues {
			var columnIndexes = make([]int32, len(values))
			for i, value := range values {
				index, ok := idProvider[value]
				if !ok {
					idProvider[value] = providerCnt
					index = providerCnt
					providerCnt++
				}
				columnIndexes[i] = index
			}
			data.TableIndexValues[tableName][columnName] = columnIndexes
		}
	}

	for tableName, columnPLI := range data.PLI {
		data.IndexPLI[tableName] = map[string]map[int32][]int32{}
		for columnMame, pli := range columnPLI {
			indexPli := map[int32][]int32{}
			for value, rowIds := range pli {
				indexPli[idProvider[value]] = rowIds
			}
			data.IndexPLI[tableName][columnMame] = indexPli
		}
	}
}

func NewJoinTableDataNew(lhs []rds.Predicate, rhs rds.Predicate, joinTables [][][]int32, taskId int64) *JoinTableData {
	data := &JoinTableData{
		TableValues: map[string]map[string][]interface{}{},
		//TableIndexValues: map[string]map[string][]int32{},
		TableColumnTypes: map[string]map[string]columnInfo{},
		//PLI:              map[string]map[string]map[interface{}][]int32{},
		//IndexPLI:         map[string]map[string]map[int32][]int32{},
	}
	var joinTableIds []string
	for idx := range joinTables {
		joinTableIds = append(joinTableIds, fmt.Sprint(JoinTableIdPrefix, idx))
	}
	data.LoadSchema(lhs, rhs)
	data.LoadJoinTableData(joinTables, taskId)
	return data
}

func (data *JoinTableData) LoadJoinTableData(joinTables [][][]int32, taskId int64) {
	task := TaskData[taskId]
	for idx, joinTable := range joinTables {
		joinTableId := fmt.Sprint(JoinTableIdPrefix, idx)
		joinTableRowSize := len(joinTable)
		data.TableValues[joinTableId] = map[string][]interface{}{}
		for column, info := range data.TableColumnTypes[joinTableId] {
			tableId := info.tableId
			columnValues := task.TableValues[tableId][column]
			var values = make([]interface{}, 0, joinTableRowSize)
			// 获取列的值
			tableIdIndex := task.TableIndex[tableId]
			for _, row := range joinTable {
				rowId := row[tableIdIndex]
				values = append(values, columnValues[rowId])
			}

			data.TableValues[joinTableId][column] = values
		}
	}
}

func (data *JoinTableData) CreateTempPLI(predicate rds.Predicate) map[string]map[string]map[interface{}][]int32 {
	var tablePLI = map[string]map[string]map[interface{}][]int32{}
	tableValues := data.TableValues

	lJoinTableId := predicate.LeftColumn.JoinTableId
	lColumnId := predicate.LeftColumn.ColumnId
	rJoinTableId := predicate.RightColumn.JoinTableId
	rColumnId := predicate.RightColumn.ColumnId
	isSameColumn := lJoinTableId == rJoinTableId && lColumnId == rColumnId
	if isSameColumn {
		tablePLI[lJoinTableId] = map[string]map[interface{}][]int32{}
		columnValues := tableValues[lJoinTableId][lColumnId]
		columnPLI := map[interface{}][]int32{}
		for rowId, value := range columnValues {
			columnPLI[value] = append(columnPLI[value], int32(rowId))
		}
		tablePLI[lJoinTableId][lColumnId] = columnPLI
	} else {
		tablePLI[lJoinTableId] = map[string]map[interface{}][]int32{}

		lColValues := tableValues[lJoinTableId][lColumnId]
		lColPLI := map[interface{}][]int32{}
		for rowId, value := range lColValues {
			lColPLI[value] = append(lColPLI[value], int32(rowId))
		}
		tablePLI[lJoinTableId][lColumnId] = lColPLI

		rColValues := tableValues[lJoinTableId][rColumnId]
		rColPLI := map[interface{}][]int32{}
		for rowId, value := range rColValues {
			rColPLI[value] = append(rColPLI[value], int32(rowId))
		}
		tablePLI[rJoinTableId][rColumnId] = rColPLI
	}
	return tablePLI
}
