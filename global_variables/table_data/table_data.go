package table_data

import (
	"gitlab.grandhoo.com/rock/rock_v3/rds_config"
	"gitlab.grandhoo.com/rock/rock_v3/request/udf"
	"gitlab.grandhoo.com/rock/rock_v3/utils/blocking/blocking_conf"
	"gitlab.grandhoo.com/rock/rock_v3/utils/blocking/blocking_reader"
	"gitlab.grandhoo.com/rock/rock_v3/utils/blocking/udf_column"
	"gitlab.grandhoo.com/rock/rock_v3/utils/storage_utils"
	"sync"
)

var TaskData = map[int64]*Task{} // taskId -> Task
var mu sync.Mutex

type Task struct {
	TableValues            map[string]map[string][]interface{} // tableId -> column -> []
	TableIndexValues       map[string]map[string][]int32       // tableId -> column -> []
	SampleTableIndexValues map[string]map[string][]int32
	TableRowSize           map[string]int                                // tableId -> rowSize
	TableColumnTypes       map[string]map[string]string                  // tableId -> column -> type
	PLI                    map[string]map[string]map[interface{}][]int32 // tableId -> column -> value -> 有序rowIds
	SamplePLI              map[string]map[string]map[interface{}][]int32
	IndexPLI               map[string]map[string]map[int32][]int32
	SampleIndexPLI         map[string]map[string]map[int32][]int32
	PLIRowSize             map[string]map[string]map[interface{}]int // tableId -> column -> value -> rowSize
	IndexPLIRowSize        map[string]map[string]map[int32]int

	//UDFTableColumn map[string]map[string]bool // tableId -> column
	UDFInfo    []udf.UDFTabCol
	TableIndex map[string]int // tableId -> index

	MIRuleTableColumn map[string]map[string]bool // MI 规则涉及的列 表名 -> 列名 -> bool
}

var skipColumn = map[string]bool{"id": true, "row_id": true, "update_time": true, "${df}": true, "${mk}": true}

// LoadSchema 加载 schema，即列名信息，长度信息。会初始化数据结构，需要优先调用
func LoadSchema(taskId int64, tableIds []string) {
	mu.Lock()
	defer mu.Unlock()
	TaskData[taskId] = &Task{
		TableValues:       map[string]map[string][]interface{}{},
		TableRowSize:      map[string]int{},
		TableColumnTypes:  map[string]map[string]string{},
		PLI:               map[string]map[string]map[interface{}][]int32{},
		SamplePLI:         map[string]map[string]map[interface{}][]int32{},
		TableIndex:        map[string]int{},
		MIRuleTableColumn: map[string]map[string]bool{},
	}
	for idx, tableId := range tableIds {
		_, rowSize, columnTypes, _ := storage_utils.GetSchemaInfo(tableId)
		for col := range skipColumn {
			delete(columnTypes, col)
		}
		TaskData[taskId].TableRowSize[tableId] = rowSize
		TaskData[taskId].TableColumnTypes[tableId] = columnTypes
		TaskData[taskId].TableIndex[tableId] = idx
		TaskData[taskId].MIRuleTableColumn[tableId] = map[string]bool{}
	}
}

func LoadSchemaNew(taskId int64, tableIds []string) error {
	mu.Lock()
	defer mu.Unlock()
	TaskData[taskId] = &Task{
		TableValues:       map[string]map[string][]interface{}{},
		TableRowSize:      map[string]int{},
		TableColumnTypes:  map[string]map[string]string{},
		PLI:               map[string]map[string]map[interface{}][]int32{},
		TableIndex:        map[string]int{},
		PLIRowSize:        map[string]map[string]map[interface{}]int{},
		IndexPLIRowSize:   map[string]map[string]map[int32]int{},
		MIRuleTableColumn: map[string]map[string]bool{},
	}
	for idx, tableId := range tableIds {
		_, rowSize, columnTypes, _, err := storage_utils.GetSchemaInfoNew(tableId)
		if err != nil {
			return err
		}
		for col := range skipColumn {
			delete(columnTypes, col)
		}
		TaskData[taskId].TableRowSize[tableId] = rowSize
		TaskData[taskId].TableColumnTypes[tableId] = columnTypes
		TaskData[taskId].TableIndex[tableId] = idx
		TaskData[taskId].MIRuleTableColumn[tableId] = map[string]bool{}
	}
	return nil
}

// SetBlockingInfo 在 LoadSchema 后调用
func SetBlockingInfo(taskId int64, UTCs []udf.UDFTabCol) {
	mu.Lock()
	defer mu.Unlock()
	task := TaskData[taskId]
	//UDF := map[string]map[string]bool{}
	for i := range UTCs {
		utc := &UTCs[i]
		//if UDF[utc.LeftTableId] == nil {
		//	UDF[utc.LeftTableId] = map[string]bool{}
		//}
		newLeftColumnName := udf_column.MakeUdfColumn(utc.LeftTableId, utc.LeftColumnNames, utc.Name, utc.Threshold)
		newRightColumnName := udf_column.MakeUdfColumn(utc.RightTableId, utc.RightColumnNames, utc.Name, utc.Threshold)

		task.TableColumnTypes[utc.LeftTableId][newLeftColumnName] = rds_config.IntType   // add new col
		task.TableColumnTypes[utc.RightTableId][newRightColumnName] = rds_config.IntType // add new col

		// fileSuffix string, threshold float64

		//UDF[utc.LeftTableId][newLeftColumnName] = true
		//UDF[utc.LeftTableId][newRightColumnName] = true
	}
	//TaskData[taskId].UDFTableColumn = UDF
	TaskData[taskId].UDFInfo = UTCs
}

// LoadDataCreatePli 加载需要的列的数据。需要先调用 LoadSchema 和 SetBlockingInfo
func LoadDataCreatePli(taskId int64, tableIds []string) {
	mu.Lock()
	defer mu.Unlock()
	task := TaskData[taskId]
	//type tableColumnNilNumber struct {
	//	tableName  string
	//	columnName string
	//	nilNumber  int
	//}
	//var totalColumnNumber = 0
	//var tableColumnNilNumbers []tableColumnNilNumber
	for _, tableId := range tableIds {
		task.TableValues[tableId] = map[string][]interface{}{}
		task.PLI[tableId] = map[string]map[interface{}][]int32{}
		for column := range task.TableColumnTypes[tableId] {
			var values []interface{}
			if udf_column.IsUdfColumn(column) { // blocking column
				tableName, columnNames, modelName, threshold := udf_column.ParseUdfColumn(column)
				blkRd := blocking_reader.New(tableName, columnNames, blocking_conf.ModelName2Suffix(modelName), threshold)
				values, _ = blkRd.ReadMostFreqTokenIds(0, task.TableRowSize[tableId])
			} else {
				values, _ = storage_utils.GetColumnValues(tableId, column)
			}

			task.TableValues[tableId][column] = values

			pli := map[interface{}][]int32{}
			for rowId, value := range values {
				pli[value] = append(pli[value], int32(rowId))
			}
			task.PLI[tableId][column] = pli

			//// MI 暂时实现
			//// MI规则判断：
			////1、Y字段无空值不输出MI规则
			////2、空值最多的25%字段为Y的规则作为MI规则，若含空值字段不足25%，则全部含空值字段做为Y的规则为MI规则
			////3、若没有字段含空值则不输出MI规则
			//totalColumnNumber++
			//nilNumber := len(task.PLI[tableId][column][nil]) + len(task.PLI[tableId][column][""])
			//if nilNumber > 0 {
			//	tableColumnNilNumbers = append(tableColumnNilNumbers, tableColumnNilNumber{
			//		tableName:  tableId,
			//		columnName: column,
			//		nilNumber:  nilNumber,
			//	})
			//}
		}
	}

	//maxMIColumnNumber := totalColumnNumber / 4
	//if len(tableColumnNilNumbers) > maxMIColumnNumber {
	//	sort.Slice(tableColumnNilNumbers, func(i, j int) bool {
	//		if tableColumnNilNumbers[i].nilNumber == tableColumnNilNumbers[j].nilNumber {
	//			return tableColumnNilNumbers[i].columnName < tableColumnNilNumbers[j].columnName
	//		}
	//		return tableColumnNilNumbers[i].nilNumber > tableColumnNilNumbers[j].nilNumber
	//	})
	//	tableColumnNilNumbers = tableColumnNilNumbers[:maxMIColumnNumber]
	//}
	//for _, tcn := range tableColumnNilNumbers {
	//	if tcn.nilNumber > 0 {
	//		task.MIRuleTableColumn[tcn.tableName][tcn.columnName] = true
	//	}
	//}
}

func CreateSamplePLI(taskId int64, limit int) {
	mu.Lock()
	defer mu.Unlock()
	task := TaskData[taskId]

	for tableName, tableValues := range task.TableValues {
		task.SamplePLI[tableName] = map[string]map[interface{}][]int32{}
		for columnName, columnValues := range tableValues {
			samplePli := map[interface{}][]int32{}
			for rowId, value := range columnValues[:limit] {
				samplePli[value] = append(samplePli[value], int32(rowId))
			}
			task.SamplePLI[tableName][columnName] = samplePli
		}
	}
}

// CreatePliRowSize 只创建pli且不带行号，不读数据到内存
func CreatePliRowSize(taskId int64, tableIds []string) {
	mu.Lock()
	defer mu.Unlock()
	task := TaskData[taskId]
	//type tableColumnNilNumber struct {
	//	tableName  string
	//	columnName string
	//	nilNumber  int
	//}
	//var totalColumnNumber = 0
	//var tableColumnNilNumbers []tableColumnNilNumber
	for _, tableId := range tableIds {
		task.TableValues[tableId] = map[string][]interface{}{}
		task.PLIRowSize[tableId] = map[string]map[interface{}]int{}
		for column := range task.TableColumnTypes[tableId] {
			var values []interface{}
			if udf_column.IsUdfColumn(column) { // blocking column
				tableName, columnNames, modelName, threshold := udf_column.ParseUdfColumn(column)
				blkRd := blocking_reader.New(tableName, columnNames, blocking_conf.ModelName2Suffix(modelName), threshold)
				values, _ = blkRd.ReadMostFreqTokenIds(0, task.TableRowSize[tableId])
			} else {
				values, _ = storage_utils.GetColumnValues(tableId, column)
			}

			pli := map[interface{}]int{}
			for _, value := range values {
				pli[value]++
			}
			task.PLIRowSize[tableId][column] = pli

			//// MI 暂时实现
			//// MI规则判断：
			////1、Y字段无空值不输出MI规则
			////2、空值最多的25%字段为Y的规则作为MI规则，若含空值字段不足25%，则全部含空值字段做为Y的规则为MI规则
			////3、若没有字段含空值则不输出MI规则
			//totalColumnNumber++
			//nilNumber := pli[nil] + pli[""]
			//if nilNumber > 0 {
			//	tableColumnNilNumbers = append(tableColumnNilNumbers, tableColumnNilNumber{
			//		tableName:  tableId,
			//		columnName: column,
			//		nilNumber:  nilNumber,
			//	})
			//}
		}
	}

	//maxMIColumnNumber := totalColumnNumber / 4
	//if len(tableColumnNilNumbers) > maxMIColumnNumber {
	//	sort.Slice(tableColumnNilNumbers, func(i, j int) bool {
	//		if tableColumnNilNumbers[i].nilNumber == tableColumnNilNumbers[j].nilNumber {
	//			return tableColumnNilNumbers[i].columnName < tableColumnNilNumbers[j].columnName
	//		}
	//		return tableColumnNilNumbers[i].nilNumber > tableColumnNilNumbers[j].nilNumber
	//	})
	//	tableColumnNilNumbers = tableColumnNilNumbers[:maxMIColumnNumber]
	//}
	//for _, tcn := range tableColumnNilNumbers {
	//	if tcn.nilNumber > 0 {
	//		task.MIRuleTableColumn[tcn.tableName][tcn.columnName] = true
	//	}
	//}
}

// CreateIndex 先 LoadSchema 然后 SetBlockingInfo 然后 LoadDataCreatePli
func CreateIndex(taskId int64) {
	mu.Lock()
	defer mu.Unlock()
	idProvider := map[interface{}]int32{}
	idProvider[nil] = rds_config.NilIndex
	idProvider[""] = rds_config.NilIndex
	providerCnt := int32(0)
	task := GetTask(taskId)

	task.TableIndexValues = map[string]map[string][]int32{}
	for tableName, columnValues := range task.TableValues {
		task.TableIndexValues[tableName] = map[string][]int32{}
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
			task.TableIndexValues[tableName][columnName] = columnIndexes
		}
	}

	task.IndexPLI = map[string]map[string]map[int32][]int32{}
	for tableName, columnPLI := range task.PLI {
		task.IndexPLI[tableName] = map[string]map[int32][]int32{}
		for columnMame, pli := range columnPLI {
			indexPli := map[int32][]int32{}
			for value, rowIds := range pli {
				indexPli[idProvider[value]] = rowIds
			}
			task.IndexPLI[tableName][columnMame] = indexPli
		}
	}

	//task.TableValues = nil
	//task.PLI = nil
}

func CreateSampleIndex(taskId int64, limit int) {
	mu.Lock()
	defer mu.Unlock()
	idProvider := map[interface{}]int32{}
	idProvider[nil] = rds_config.NilIndex
	idProvider[""] = rds_config.NilIndex
	providerCnt := int32(0)
	task := GetTask(taskId)

	task.SampleTableIndexValues = map[string]map[string][]int32{}
	for tableName, columnValues := range task.TableValues {
		task.SampleTableIndexValues[tableName] = map[string][]int32{}
		for columnName, values := range columnValues {
			limitValues := values[:limit]
			var columnIndexes = make([]int32, len(limitValues))
			for i, value := range limitValues {
				index, ok := idProvider[value]
				if !ok {
					idProvider[value] = providerCnt
					index = providerCnt
					providerCnt++
				}
				columnIndexes[i] = index
			}
			task.SampleTableIndexValues[tableName][columnName] = columnIndexes
		}
	}

	task.SampleIndexPLI = map[string]map[string]map[int32][]int32{}
	for tableName, columnPLI := range task.SamplePLI {
		task.SampleIndexPLI[tableName] = map[string]map[int32][]int32{}
		for columnMame, pli := range columnPLI {
			indexPli := map[int32][]int32{}
			for value, rowIds := range pli {
				indexPli[idProvider[value]] = rowIds
			}
			task.SampleIndexPLI[tableName][columnMame] = indexPli
		}
	}

	//task.TableValues = nil
	//task.PLI = nil
}

func CreateIndexPLIRowSize(taskId int64) {
	mu.Lock()
	defer mu.Unlock()
	idProvider := map[interface{}]int32{}
	idProvider[nil] = rds_config.NilIndex
	idProvider[""] = rds_config.NilIndex
	providerCnt := int32(0)
	task := GetTask(taskId)

	task.TableIndexValues = map[string]map[string][]int32{}
	for tableName, columnValues := range task.PLIRowSize {
		task.TableIndexValues[tableName] = map[string][]int32{}
		for _, values := range columnValues {
			for value := range values {
				_, ok := idProvider[value]
				if !ok {
					idProvider[value] = providerCnt
					providerCnt++
				}
			}
		}
	}

	task.IndexPLIRowSize = map[string]map[string]map[int32]int{}
	for tableName, columnPLI := range task.PLIRowSize {
		task.IndexPLIRowSize[tableName] = map[string]map[int32]int{}
		for columnMame, pli := range columnPLI {
			indexPliRowSize := map[int32]int{}
			for value, rowIds := range pli {
				indexPliRowSize[idProvider[value]] = rowIds
			}
			task.IndexPLIRowSize[tableName][columnMame] = indexPliRowSize
		}
	}
}

func GetColumnId2TableId(taskId int64) map[string]string {
	c2t := map[string]string{}
	for tableName, columns := range GetTask(taskId).TableColumnTypes {
		for columnName := range columns {
			c2t[columnName] = tableName
		}
	}
	return c2t
}

func GetTask(taskId int64) *Task {
	return TaskData[taskId]
}

func DropTaskId(taskId int64) {
	mu.Lock()
	defer mu.Unlock()
	delete(TaskData, taskId)
}
