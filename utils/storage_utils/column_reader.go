package storage_utils

import (
	"errors"
	"fmt"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/global/stand_data/field_role/rule_cmp/eid"
	"gitlab.grandhoo.com/rock/rock-share/global/utils/lru"
	"gitlab.grandhoo.com/rock/rock_memery"
	utils2 "gitlab.grandhoo.com/rock/rock_memery/utils"
	"gitlab.grandhoo.com/rock/rock_v3/rds_config"
	"gitlab.grandhoo.com/rock/storage/storage2/database/sql"
	"gitlab.grandhoo.com/rock/storage/storage2/database/table/itable"
	"gitlab.grandhoo.com/rock/storage/storage2/database/types/logic_type"
	"gitlab.grandhoo.com/rock/storage/storage2/database/types/special_type/decimal"
	"gitlab.grandhoo.com/rock/storage/storage2/database/types/special_type/money"
	"gitlab.grandhoo.com/rock/storage/storage2/database/types/special_type/timetype"
	"gitlab.grandhoo.com/rock/storage/storage2/rock_db"
	"gitlab.grandhoo.com/rock/storage/storage2/utils"
	"sync"
	"time"
)

type columnReaderType struct {
	columnCache         *lru.Cache[TableColumnNameType, *ColumnValue]
	tableLength         *lru.Cache[tableName, int64]
	mu                  sync.Mutex
	indexOutOfRangeMute bool
}

var ColumnReader = columnReaderType{
	columnCache: lru.New[TableColumnNameType, *ColumnValue](200*100*100, func(key TableColumnNameType, value *ColumnValue) {
		if value.values != nil {
			logger.Debugf("LRU expire column %s.%s data", key.tableName, key.columnName)
			//err := value.values.Free()
			err := value.values.Free()
			if err != nil {
				logger.Error(err)
			}
		}
	}, func(_ TableColumnNameType, value *ColumnValue) int {
		if value.values == nil {
			return 64
		}
		return value.values.Len() + 64
	}),
	tableLength: lru.New[tableName, int64](100, nil, nil),
}

type ColumnValue struct {
	values *valuesType // nullable
	//	StringType = "string"
	//	IntType    = "int64"
	//	FloatType  = "float64"
	//	BoolType   = "bool"
	//	TimeType   = "time"
	typeName ColumnTypeName
}

func (cr *columnReaderType) GetTableLength(tableName string) (int64, error) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	value, ok := cr.tableLength.Get(tableName)
	if !ok {
		lenW, err := rock_db.DB.Query(sql.SingleTable.SELECT(sql.CountAsterisk).FROM(tableName))
		if err != nil {
			logger.Error(err)
			return 0, err
		}
		value = lenW[0][0].(int64)
		cr.tableLength.Put(tableName, value)
		//rock_db.DB.ReleaseCache()
	}
	return value, nil
}

func (cr *columnReaderType) GetColumnValue(tableName string, columnName string) (column *ColumnValue, err error) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	key := makeTableColumnName(tableName, columnName)
	columnValue, ok := cr.columnCache.Get(key)
	if !ok {
		start := time.Now()
		defer func() {
			logger.Info("从磁盘读取表列", tableName, " ", columnName, "耗时", utils.DurationString(start))
		}()
		var columnInfoMap map[string]*itable.ColumnInfo
		columnInfoMap, err = rock_db.DB.TableColumnInfoMap(tableName)
		if err != nil {
			logger.Error(err)
			return nil, err
		}
		columnInfo := columnInfoMap[columnName]
		if columnInfo == nil {
			errStr := "no column " + columnName + " in " + tableName
			logger.Error(errStr)
			return nil, errors.New(errStr)
		}
		var columnNames = []string{columnName}
		var columnTypes = []logic_type.LogicType{columnInfo.LogicType}
		//for cName, cInfo := range columnInfoMap {
		//	if cName != cInfo.ColumnName {
		//		logger.Error("cName != cInfo.ColumnName", cName, *cInfo)
		//	}
		//	if columnName == cName {
		//		columnNames = append(columnNames, cName)
		//		columnTypes = append(columnTypes, cInfo.LogicType)
		//		break
		//	}
		//}
		var tableLines, err = rock_db.DB.Query(sql.SingleTable.SELECT(columnNames...).FROM(tableName))
		if err != nil {
			logger.Error(err)
			return nil, err
		}
		cr.tableLength.Put(tableName, int64(len(tableLines)))
		var linesList = make([][]interface{}, len(columnNames))
		for columnIndex := range columnNames {
			linesList[columnIndex] = make([]interface{}, len(tableLines))
		}
		for rowIndex, line := range tableLines {
			for columnIndex, cellValue := range line {
				linesList[columnIndex][rowIndex] = cellValue
			}
		}
		for columnIndex, lines := range linesList {
			logger.Info("read table ", tableName, " column ", columnNames[columnIndex], " line number ", len(lines))
			var columnType string
			var values *valuesType
			columnLogicType := columnTypes[columnIndex]
			//var values = make([]any, len(lines))
			//const (
			//	StringType = "string"
			//	IntType    = "int64"
			//	FloatType  = "float64"
			//	BoolType   = "bool"
			//	TimeType   = "time"
			//	TextType   = "text"
			//)
			if len(lines) > 0 {
				switch columnLogicType {
				case logic_type.STRING:
					//values, _, err = rock_memery.NewRockSliceWithCapacity(rock_memery.STRING, len(lines), rock_memery.StorageCalc)
					values, _, err = newValuesType(rock_memery.STRING, len(lines))
					if err != nil {
						logger.Error(err)
						return nil, err
					}
					columnType = rds_config.StringType
					for _, line := range lines {
						if line == nil {
							_, err = valuesAppend(values, nil)
							if err != nil {
								logger.Error(err)
								//err2 := values.Free()
								err2 := values.Free()
								if err2 != nil {
									logger.Error(err2)
								}
								return nil, err
							}
							continue
						}
						s, ok := line.(string)
						//_, err = values.Append(s)
						if ok {
							_, err = valuesAppend(values, s)
						} else {
							logger.Error(fmt.Sprintf("bad value %+v %T", line, line))
							_, err = valuesAppend(values, nil)
						}
						if err != nil {
							logger.Error(err)
							//err2 := values.Free()
							err2 := values.Free()
							if err2 != nil {
								logger.Error(err2)
							}
							return nil, err
						}
					}
				case logic_type.INDEX:
					fallthrough
				case logic_type.INT64:
					//values, _, err = rock_memery.NewRockSliceWithCapacity(rock_memery.INT, len(lines), rock_memery.StorageCalc)
					values, _, err = newValuesType(rock_memery.INT, len(lines))
					if err != nil {
						logger.Error(err)
						return nil, err
					}
					columnType = rds_config.IntType
					for _, line := range lines {
						if line == nil {
							_, err = valuesAppend(values, nil)
							if err != nil {
								logger.Error(err)
								//err2 := values.Free()
								err2 := values.Free()
								if err2 != nil {
									logger.Error(err2)
								}
								return nil, err
							}
							continue
						}
						i64, ok := line.(int64)
						//_, err = values.Append(int(i64))
						if ok {
							_, err = valuesAppend(values, int(i64))
						} else {
							logger.Error(fmt.Sprintf("bad value %+v %T", line, line))
							_, err = valuesAppend(values, nil)
						}
						if err != nil {
							logger.Error(err)
							//err2 := values.Free()
							err2 := values.Free()
							if err2 != nil {
								logger.Error(err2)
							}
							return nil, err
						}
					}
				case logic_type.EID:
					//values, _, err = rock_memery.NewRockSliceWithCapacity(rock_memery.INT, len(lines), rock_memery.StorageCalc)
					values, _, err = newValuesType(rock_memery.INT, len(lines))
					if err != nil {
						logger.Error(err)
						return nil, err
					}
					columnType = rds_config.IntType
					for _, line := range lines {
						if line == nil {
							_, err = valuesAppend(values, nil)
							if err != nil {
								logger.Error(err)
								//err2 := values.Free()
								err2 := values.Free()
								if err2 != nil {
									logger.Error(err2)
								}
								return nil, err
							}
							continue
						}
						i64, ok := line.(eid.EID)
						//_, err = values.Append(int(i64))
						if ok {
							_, err = valuesAppend(values, int(i64))
						} else {
							logger.Error(fmt.Sprintf("bad value %+v %T", line, line))
							_, err = valuesAppend(values, nil)
						}
						if err != nil {
							logger.Error(err)
							//err2 := values.Free()
							err2 := values.Free()
							if err2 != nil {
								logger.Error(err2)
							}
							return nil, err
						}
					}
				case logic_type.MONEY:
					//values, _, err = rock_memery.NewRockSliceWithCapacity(rock_memery.FLOAT64, len(lines), rock_memery.StorageCalc)
					values, _, err = newValuesType(rock_memery.FLOAT64, len(lines))
					if err != nil {
						logger.Error(err)
						return nil, err
					}
					columnType = rds_config.FloatType
					for _, line := range lines {
						if line == nil {
							_, err = valuesAppend(values, nil)
							if err != nil {
								logger.Error(err)
								//err2 := values.Free()
								err2 := values.Free()
								if err2 != nil {
									logger.Error(err2)
								}
								return nil, err
							}
							continue
						}
						m, ok := line.(money.Money)
						//_, err = values.Append(m.FloatVal())
						if !ok {
							logger.Error(fmt.Sprintf("bad value %+v %T", line, line))
							_, err = valuesAppend(values, nil)
						} else {
							_, err = valuesAppend(values, m.FloatVal())
						}
						if err != nil {
							logger.Error(err)
							//err2 := values.Free()
							err2 := values.Free()
							if err2 != nil {
								logger.Error(err2)
							}
							return nil, err
						}
					}
				case logic_type.FLOAT64:
					//values, _, err = rock_memery.NewRockSliceWithCapacity(rock_memery.FLOAT64, len(lines), rock_memery.StorageCalc)
					values, _, err = newValuesType(rock_memery.FLOAT64, len(lines))
					if err != nil {
						logger.Error(err)
						return nil, err
					}
					columnType = rds_config.FloatType
					for _, line := range lines {
						if line == nil {
							_, err = valuesAppend(values, nil)
							if err != nil {
								logger.Error(err)
								//err2 := values.Free()
								err2 := values.Free()
								if err2 != nil {
									logger.Error(err2)
								}
								return nil, err
							}
							continue
						}
						f, ok := line.(float64)
						//_, err = values.Append(f)
						if ok {
							_, err = valuesAppend(values, f)
						} else {
							logger.Error(fmt.Sprintf("bad value %+v %T", line, line))
							_, err = valuesAppend(values, nil)
						}
						if err != nil {
							logger.Error(err)
							//err2 := values.Free()
							err2 := values.Free()
							if err2 != nil {
								logger.Error(err2)
							}
							return nil, err
						}
					}
				case logic_type.TIME:
					//values, _, err = rock_memery.NewRockSliceWithCapacity(rock_memery.TIME, len(lines), rock_memery.StorageCalc)
					values, _, err = newValuesType(rock_memery.TIME, len(lines))
					if err != nil {
						logger.Error(err)
						return nil, err
					}
					columnType = rds_config.TimeType
					for _, line := range lines {
						if line == nil {
							_, err = valuesAppend(values, nil)
							if err != nil {
								logger.Error(err)
								//err2 := values.Free()
								err2 := values.Free()
								if err2 != nil {
									logger.Error(err2)
								}
								return nil, err
							}
							continue
						}
						t, ok := line.(timetype.Time)
						//_, err = values.Append(t.TimeVal())
						if ok {
							_, err = valuesAppend(values, t.TimeVal())
						} else {
							logger.Error(fmt.Sprintf("bad value %+v %T", line, line))
							_, err = valuesAppend(values, nil)
						}
						if err != nil {
							logger.Error(err)
							//err2 := values.Free()
							err2 := values.Free()
							if err2 != nil {
								logger.Error(err2)
							}
							return nil, err
						}
					}
				case logic_type.BOOLEAN:
					//values, _, err = rock_memery.NewRockSliceWithCapacity(rock_memery.BOOL, len(lines), rock_memery.StorageCalc)
					values, _, err = newValuesType(rock_memery.BOOL, len(lines))
					if err != nil {
						logger.Error(err)
						return nil, err
					}
					columnType = rds_config.BoolType
					for _, line := range lines {
						if line == nil {
							_, err = valuesAppend(values, nil)
							if err != nil {
								logger.Error(err)
								//err2 := values.Free()
								err2 := values.Free()
								if err2 != nil {
									logger.Error(err2)
								}
								return nil, err
							}
							continue
						}
						b, ok := line.(bool)
						//_, err = values.Append(b)
						if ok {
							_, err = valuesAppend(values, b)
						} else {
							logger.Error(fmt.Sprintf("bad value %+v %T", line, line))
							_, err = valuesAppend(values, nil)
						}
						if err != nil {
							logger.Error(err)
							//err2 := values.Free()
							err2 := values.Free()
							if err2 != nil {
								logger.Error(err2)
							}
							return nil, err
						}
					}
				case logic_type.BIGDECIMAL:
					//values, _, err = rock_memery.NewRockSliceWithCapacity(rock_memery.FLOAT64, len(lines), rock_memery.StorageCalc)
					values, _, err = newValuesType(rock_memery.FLOAT64, len(lines))
					if err != nil {
						logger.Error(err)
						return nil, err
					}
					columnType = rds_config.FloatType
					for _, line := range lines {
						if line == nil {
							_, err = valuesAppend(values, nil)
							if err != nil {
								logger.Error(err)
								//err2 := values.Free()
								err2 := values.Free()
								if err2 != nil {
									logger.Error(err2)
								}
								return nil, err
							}
							continue
						}
						d, ok := line.(decimal.BigDecimal)
						//_, err = values.Append(d.FloatVal())
						if ok {
							_, err = valuesAppend(values, d.FloatVal())
						} else {
							logger.Error(fmt.Sprintf("bad value %+v %T", line, line))
							_, err = valuesAppend(values, nil)
						}
						if err != nil {
							logger.Error(err)
							//err2 := values.Free()
							err2 := values.Free()
							if err2 != nil {
								logger.Error(err2)
							}
							return nil, err
						}
					}
				default:
					panic(columnInfo.LogicType)
				}
			}
			var examples []any
			for i := 0; i < utils.Min(len(lines), 3); i++ {
				val, err := values.Get(i)
				if err != nil {
					logger.Warn(err)
				}
				examples = append(examples, val)
			}
			logger.Info("read table ", tableName, " column ", columnNames[columnIndex], " examples ", examples)
			var value = &ColumnValue{
				values:   values,
				typeName: columnType,
			}
			var theKey = makeTableColumnName(tableName, columnNames[columnIndex])
			cr.columnCache.Put(theKey, value)
			if theKey == key {
				columnValue = value
			}
		}
	} else {
		logger.Debug("从内存读取列", tableName, columnName)
	}
	return columnValue, nil
}

func (cr *columnReaderType) ClearCache() {
	cr.columnCache.RemoveAllNoExpire()
}

func (c *ColumnValue) Length() ColumnValueRowIdType {
	if c.values == nil {
		return 0
	}
	return ColumnValueRowIdType(c.values.Len())
}

func (c *ColumnValue) ColumnType() ColumnTypeName {
	return c.typeName
}

func (c *ColumnValue) Get(rowId ColumnValueRowIdType) any {
	if rowId < 0 {
		logger.Info("rowId < 0. rowId = ", rowId)
	}
	if !ColumnReader.indexOutOfRangeMute && rowId >= c.Length() {
		logger.Info("rowId >= length. rowId = ", rowId, " length = ", c.Length())
		ColumnReader.indexOutOfRangeMute = true
	}
	if c.values == nil {
		return nil
	}
	value, _ := c.values.Get(int(rowId))
	return value
}

func (c *ColumnValue) GetString(rowId ColumnValueRowIdType) (value string, ok bool) {
	value, ok = c.Get(rowId).(string)
	return
}

func (c *ColumnValue) GetInt64(rowId ColumnValueRowIdType) (value int64, ok bool) {
	value, ok = c.Get(rowId).(int64)
	return
}

func (c *ColumnValue) GetFloat64(rowId ColumnValueRowIdType) (value float64, ok bool) {
	value, ok = c.Get(rowId).(float64)
	return
}

func (c *ColumnValue) GetTime(rowId ColumnValueRowIdType) (value time.Time, ok bool) {
	value, ok = c.Get(rowId).(time.Time)
	return
}

func (c *ColumnValue) GetBool(rowId ColumnValueRowIdType) (value bool, ok bool) {
	value, ok = c.Get(rowId).(bool)
	return
}

type ColumnTypeName = string
type TableColumnNameType struct {
	tableName  string
	columnName string
}
type ColumnValueRowIdType = int32

type valuesType struct {
	values []any
	//values *rock_memery.RockSlice
}

func newValuesType(elementType int, capacity int) (*valuesType, utils2.MemPoolWarn, error) {
	return &valuesType{values: make([]any, 0, capacity)}, nil, nil
}

func valuesAppend(v *valuesType, value any) (utils2.MemPoolWarn, error) {
	v.values = append(v.values, value)
	return nil, nil
}

func (v *valuesType) Len() int {
	return len(v.values)
}

func (v *valuesType) Get(i int) (any, error) {
	if v == nil {
		return nil, errors.New("valuesType is nil")
	}
	if i < 0 || i >= v.Len() {
		return nil, errors.New(fmt.Sprintf("out of range %d, %d", i, v.Len()))
	}
	return v.values[i], nil
}

func (v *valuesType) Free() error {
	return nil
	//return common.StorageSliceAgent.FreeSlice(v.values)
}

func makeTableColumnName(tableName string, columnName string) TableColumnNameType {
	return TableColumnNameType{
		tableName:  tableName,
		columnName: columnName,
	}
}
