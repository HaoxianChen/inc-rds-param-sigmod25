package calculate

import (
	"gitlab.grandhoo.com/rock/rock_v3/global_variables/table_data"
	"gitlab.grandhoo.com/rock/rock_v3/utils/blocking/blocking_conf"
	"gitlab.grandhoo.com/rock/rock_v3/utils/blocking/udf_column"
	"golang.org/x/exp/slices"
	"runtime/debug"
	"sync"

	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/global/enum"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables"
	"gitlab.grandhoo.com/rock/rock_v3/rds_config"
	"gitlab.grandhoo.com/rock/rock_v3/utils"
	"gitlab.grandhoo.com/rock/rock_v3/utils/storage_utils"
)

func CreateTempPli(gv *global_variables.GlobalV) map[string]map[interface{}][]int32 {
	pli := make(map[string]map[interface{}][]int32)
	if rds_config.UseStorage {
		return pli
	}
	gv.TableValues, _ = storage_utils.GetTableAllValues(gv.TableId)
	gv.TableValueIndexes, gv.Index2Value, gv.Value2Index = storage_utils.GetTableAllValueIndexes(gv.TableValues, gv.ColumnsType)
	for colName, colValues := range gv.TableValues {
		m := make(map[interface{}][]int32)
		for rid, value := range colValues {
			m[value] = append(m[value], int32(rid))
		}
		pli[colName] = m
	}
	return pli
}

func GeneratePliUseStorage(tableId string, gv *global_variables.GlobalV) {
	task := table_data.GetTask(gv.TaskId)
	gv.TableId = tableId
	gv.Table2ColumnType[tableId] = make(map[string]string)
	skipColumn := map[string]int{"id": 1, "row_id": 1, "update_time": 1, "${df}": 1, "${mk}": 1}
	for column := range gv.SkipColumns {
		skipColumn[column] = 1
	}
	//tableName, rowSize, columnsType, _ := storage_utils.GetSchemaInfo(gv.TableId)
	rowSize := task.TableRowSize[tableId]
	columnsType := task.TableColumnTypes[tableId]
	tableName := tableId
	for s, s2 := range columnsType {
		if _, okk := skipColumn[s]; okk {
			continue
		}
		gv.ColumnsType[s] = s2
		gv.Table2ColumnType[tableId][s] = s2
	}
	gv.TableId2Name[tableId] = tableName
	//gv.ColumnsType = columnsType
	logger.Infof("table name:%s,rowSize:%d,columnType:%s\n", tableName, rowSize, columnsType)
	gv.TableName = tableName
	gv.RowSize = rowSize

	// 根据数据量决定confidence
	//if rowSize < 10000 {
	//	gv.FTR = 0.85
	//}
	//if rowSize < 1000 {
	//	gv.FTR = 0.8
	//}
	//if rowSize < 100 {
	//	gv.FTR = 0.75
	//}

	//ch := utils.GenTokenChan(1)
	var wg sync.WaitGroup

	//构建谓词
	predicateSupport := make(map[string]float64)
	predicateSupportLock := &sync.RWMutex{}
	ch := make(chan struct{}, gv.RdsChanSize)

	for columnId, dataType := range columnsType {
		if _, okk := skipColumn[columnId]; okk {
			continue
		}
		ch <- struct{}{}
		wg.Add(1)
		go func(columnId, dataType string) {
			defer func() {
				wg.Done()
				<-ch
				global_variables.Ch <- struct{}{}
				if err := recover(); err != nil {
					gv.HasError = true
					s := string(debug.Stack())
					logger.Error("recover.err:%v, stack:%v", err, s)
				}
			}()
			logger.Infof("taskId:%v,计算%v的倒排索引", gv.TaskId, columnId)
			column := rds.Column{TableId: tableId, ColumnId: columnId, ColumnType: dataType}

			//cardinality, _ := storage_utils.GetColumnCardinality(tableId, columnId)
			cardinality := len(task.IndexPLI[tableId][columnId])
			logger.Infof("columnId=%s, cardinality=%d", columnId, cardinality)
			//bucket := cardinality/storage_utils.HashBucketSize + 1
			//valNumber := cardinality
			//var valRowIds map[interface{}][]int32
			nonConstPredSupport := 0
			//unique := 0
			//for i := int64(0); i < bucket; i++ {
			//	valRowIds, _ = storage_utils.GetValueRowGroupsBucket(tableId, columnId, i, bucket)
			//	unique += len(valRowIds)
			//logger.Info("bucket=", bucket, " i=", i, " len(valRowIds)=", len(valRowIds))
			for value, rows := range task.IndexPLI[tableId][columnId] {
				if value == rds_config.NilIndex { //过滤掉=nil和=""的常数谓词
					continue
				}
				if len(rows) == 0 {
					continue
				}
				lines := len(rows)
				if lines > 1 {
					nonConstPredSupport += lines * (lines - 1)
				}
			}
			//if dataType != rds_config.TimeType {
			//	// 只有枚举类型会生成常数谓词
			//	// 新方案,数值类型的列也要生成常数谓词,但是说只跑决策树
			//	// 非时间类型的列生成常数谓词
			//	for value, rows := range valRowIds {
			//		if value == nil || value == "" { //过滤掉=nil和=""的常数谓词
			//			continue
			//		}
			//		if len(rows) == 0 {
			//			continue
			//		}
			//		lines := len(rows)
			//		if lines > 1 {
			//			nonConstPredSupport += lines * (lines - 1)
			//		}
			//
			//		// 数值类型只需要生成一个常数谓词,本质上是用列
			//		//if lines > int(float64(rowSize)*rds_config.RatioOfColumn) && (dataType == rds_config.IntType || dataType == rds_config.FloatType) {
			//		//	predicateStr := "t0." + columnId + "=" + utils.GetInterfaceToString(value)
			//		//	predicateSupportLock.Lock()
			//		//	predicate := rds.Predicate{
			//		//		PredicateStr:       predicateStr,
			//		//		LeftColumn:         column,
			//		//		RightColumn:        rds.Column{},
			//		//		ConstantValue:      value,
			//		//		ConstantIndexValue: 0,
			//		//		SymbolType:         enum.Equal,
			//		//		PredicateType:      0,
			//		//		Support:            1, // 设置为0的话会影响计算support
			//		//		Intersection:       nil,
			//		//	}
			//		//	gv.Predicates = append(gv.Predicates, predicate)
			//		//	predicateSupportLock.Unlock()
			//		//	goto nonConstPred
			//		//}
			//		// bool类型一定是枚举类型,string类型会根据具体某个值的占比,判断是否是枚举类型,这个占比可配置
			//		// 所有数据都满足这个常数谓词,则剔除这个常数谓词
			//		//if lines < rowSize && (dataType == rds_config.BoolType || (dataType == rds_config.StringType && lines > int(float64(rowSize)*rds_config.RatioOfColumn))) {
			//		//	predicateStr := "t0." + columnId + "=" + utils.GetInterfaceToString(value)
			//		//	predicateSupportLock.Lock()
			//		//	predicateSupport[predicateStr] = (float64(lines)) / float64(rowSize)
			//		//	predicate := rds.Predicate{
			//		//		PredicateStr:       predicateStr,
			//		//		LeftColumn:         column,
			//		//		RightColumn:        rds.Column{},
			//		//		ConstantValue:      value,
			//		//		ConstantIndexValue: -1,
			//		//		SymbolType:         enum.Equal,
			//		//		PredicateType:      0,
			//		//		Support:            (float64(lines)) / float64(rowSize),
			//		//		Intersection:       nil,
			//		//	}
			//		//	gv.Predicates = append(gv.Predicates, predicate)
			//		//	predicateSupportLock.Unlock()
			//		//}
			//	}
			//}
			//}

			//生成正则谓词
			//if !rds_config.UseStorage {
			//	columnValues = gv.TableValues[columnId]
			//}
			//regularPredicates, rowNums, oppositeRowNums := ru.GetRegularPredict(ctx, columnId, dataType, columnValues, gv.FTR)
			//for _, regularPredicate := range regularPredicates {
			//	rows := rowNums[fmt.Sprint(regularPredicate.ConstantValue)]
			//	lines := len(rows)
			//	support := (float64(lines)) / float64(rowSize)
			//	predicateSupportLock.Lock()
			//	predicateSupport[predicateStr] = support
			//	regularPredicate.Support = support
			//	intersection := make([][2][]int32, 1)
			//	intersection[0][0] = rows
			//	regularPredicate.Intersection = intersection
			//	gv.RegularPredicates = append(gv.RegularPredicates, *regularPredicate)
			//	predicateSupportLock.Unlock()
			//
			//	// 存反例行号
			//	oppositeRows := oppositeRowNums[fmt.Sprint(regularPredicate.ConstantValue)]
			//	intersection[0][1] = oppositeRows
			//}
			//nonConstPred:
			// 设置筛选条件不生成非常数谓词

			// 一列的值都不相同,或者基本都相同,该列去掉
			if nonConstPredSupport <= 0 || (float64(nonConstPredSupport))/float64(rowSize*(rowSize-1)) >= gv.PredicateSupportLimit {
				logger.Infof("nonConstPredSupport:%v", nonConstPredSupport)
				logger.Infof("PredicateSupportLimit:%v", gv.PredicateSupportLimit)
				predicateSupportLock.Lock()
				delete(gv.ColumnsType, columnId)
				delete(gv.Table2ColumnType[tableId], columnId)
				predicateSupportLock.Unlock()
				logger.Infof("taskid:%v, tableid:%v,column:%v,support:%v,不参与规则发现", gv.TaskId, tableId, columnId, (float64(nonConstPredSupport))/float64(rowSize*(rowSize-1)))
				return
			}

			//生成决策树单行规则的Y列
			if dataType == rds_config.StringType {
				newType := ""
				if cardinality < gv.EnumSize {
					newType = rds_config.EnumType
				} else {
					newType = rds_config.TextType
				}
				column.ColumnType = newType
				predicateSupportLock.Lock()
				gv.ColumnsType[columnId] = newType
				gv.Table2ColumnType[tableId][columnId] = newType
				predicateSupportLock.Unlock()
			}
			// 纯单行规则数值类型不作为Y出现 dataType == rds_config.IntType || dataType == rds_config.FloatType ||
			if (dataType == rds_config.BoolType ||
				(dataType == rds_config.StringType && cardinality < gv.EnumSize)) && cardinality > 1 && !udf_column.IsUdfColumn(columnId) {
				predicateSupportLock.Lock()
				if _, exist := gv.Table2DecisionY[tableId]; !exist {
					gv.Table2DecisionY[tableId] = make([]string, 0)
				}
				gv.Table2DecisionY[tableId] = append(gv.Table2DecisionY[tableId], column.ColumnId)
				predicateSupportLock.Unlock()
			}

			//if rowSize > rds_config.MultiFilterRowSize && valNumber < rds_config.FilterCondition {
			//	logger.Infof("taskId:%v, 数据量:%v, 列:%v去重后的值有%v个,不生成非常数谓词", gv.TaskId, rowSize, column.ColumnId, valNumber)
			//	return
			//}
			// 尝试下数值类型不生成结构性谓词
			//if dataType == rds_config.IntType || dataType == rds_config.FloatType {
			//	return
			//}
			// 尝试下日期类型不生成结构性谓词
			if dataType == rds_config.TimeType {
				return
			}
			var predicateStr string
			if _, okk := skipColumn[columnId]; !okk {
				predicateStr = "t0." + columnId + "=t1." + columnId
				// 谓词的support
				//support := 0
				//for value, valueIndex := range valRowIds {
				//	if value == nil || value == "" { //nil和空字符串不与任何值相等,不参与计算
				//		continue
				//	}
				//	size := len(valueIndex)
				//	if size < 2 {
				//		continue
				//	}
				//	support += size * (size - 1)
				//	if support > 0 {
				//		break
				//	}
				//}
				support := (float64(nonConstPredSupport)) / float64(rowSize*rowSize)
				if (nonConstPredSupport > 0 && support < gv.PredicateSupportLimit) || udf_column.IsUdfColumn(columnId) {
					predicateSupportLock.Lock()
					predicateSupport[predicateStr] = support
					symbolType := enum.Equal
					leftColumn, rightColumn := column, column
					leftColumn.ColumnIndex = 0
					rightColumn.ColumnIndex = 1
					predicate := rds.Predicate{
						PredicateStr:  predicateStr,
						LeftColumn:    leftColumn,
						RightColumn:   rightColumn,
						ConstantValue: nil,
						SymbolType:    symbolType,
						PredicateType: 1,
						Support:       support,
						Intersection:  nil,
					}
					//GeneratePredicateIntersection(gv, &[]*rds.Predicate{&predicate})
					var addFlag = true
					if udf_column.IsUdfColumn(columnId) {
						addFlag = false
						_, columnNames, modelName, threshold := udf_column.ParseUdfColumn(columnId)
						for i := range task.UDFInfo {
							udf := task.UDFInfo[i]
							// 防止跨表谓词生成了不跨表的谓词
							if udf.RightTableName == udf.LeftTableName && slices.Equal(udf.RightColumnNames, udf.LeftColumnNames) {
								if udf.RightTableName == tableName && slices.Equal(udf.RightColumnNames, columnNames) {
									addFlag = true
									break
								}
							}
						}
						symbolType = blocking_conf.ModelName2Type(modelName)
						predicate.SymbolType = symbolType
						predicate.UDFName = modelName
						predicate.Threshold = threshold
					}
					if addFlag {
						utils.GeneratePredicateStrNew(&predicate)
						gv.Predicates = append(gv.Predicates, predicate)
					}
					predicateSupportLock.Unlock()
				}
				// 只有string列会生成相似度谓词
				// 当前只有地址和公司类型的列会生成相似度谓词
				//if dataType == rds_config.StringType && (utils.IsSpecificColumn(columnValues, rds_config.AddressRegex) || utils.IsSpecificColumn(columnValues, rds_config.CompanyRegex)) {
				//	predicate := GenerateSimilarPredicate(column, column, rds_config.JaroWinkler, enum.SimilarThreshold, columnValues, columnValues)
				//	predicateSupportLock.Lock()
				//	gv.Predicates = append(gv.Predicates, predicate)
				//	predicateSupportLock.Unlock()
				//}
			}
		}(columnId, dataType)
	}
	wg.Wait()
	if gv.HasError {
		return
	}

	if len(gv.Predicates) == 0 {
		return
	}

	logger.Infof("all predicates size:%v", len(gv.Predicates))
	logger.Debugf("all predicates:%v", utils.GetLhsStr(gv.Predicates))
	//logger.Debugf("all regular predicates:%v", utils.GetLhsStr(gv.RegularPredicates))
	utils.SortPredicates(gv.Predicates, true)
	// 计算CR,构成规则的谓词中support最小的谓词的5%
	logger.Debugf("minimal support predicate:%s, %v\n", gv.Predicates[0].PredicateStr, gv.Predicates[0].Support)
	//gv.CR = gv.Predicates[0].Support / float64(20)
	logger.Infof("CR: %v, FTR: %v", gv.CR, gv.FTR)
}
