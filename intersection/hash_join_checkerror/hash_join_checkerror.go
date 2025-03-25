package hash_join_checkerror

import (
	"errors"
	"fmt"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/global/enum"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables/table_data"
	"gitlab.grandhoo.com/rock/rock_v3/intersection"
	"gitlab.grandhoo.com/rock/rock_v3/rule_execute/calculate_intersection/bucket_data"
	"gitlab.grandhoo.com/rock/rock_v3/utils"
	"gitlab.grandhoo.com/rock/rock_v3/utils/storage_utils"
	"gitlab.grandhoo.com/rock/storage/storage2/database/types/special_type/decimal"
	"gitlab.grandhoo.com/rock/storage/storage2/database/types/special_type/timetype"
	utils2 "gitlab.grandhoo.com/rock/storage/storage2/utils"
	"math"
	"strconv"
	"time"
)

type ICheckErrorHashJoin interface {
	CalcCheckErrorIntersection(bucketData *bucket_data.BktData, lhs []rds.Predicate, rhs rds.Predicate, chanSize int) [][][]int32
}

type HashJoin struct {
	crossGraphs            [][]rds.Predicate
	semiGraphs             [][]rds.Predicate
	selfGraph              []rds.Predicate
	joinTables             [][][]int32 // joinTableIndex -> joinRowId -> 数组下标:tableIndex 数组元素:rowId
	joinTableIds           []string    // 大宽表表名
	joinTid2JoinTableIndex map[int]int
	usedTupleIds           []int         // 去重后的tid集合
	joinTid2Tids           map[int][]int // joinTid -> tids
	tid2TableIndex         []int         // 索引:tid 元素:tableIndex
	isDuplicate            bool          // 跨表谓词是否去重
	bucketData             *bucket_data.BktData
	constPredicates        []rds.Predicate // 常数谓词
}

func NewCheckErrorHashJoin() ICheckErrorHashJoin {
	return &HashJoin{}
}

func (hj *HashJoin) CalcCheckErrorIntersection(bucketData *bucket_data.BktData, lhs []rds.Predicate, rhs rds.Predicate, chanSize int) [][][]int32 {
	hj.bucketData = bucketData
	// 1.先计算大宽表交集结果
	hj.getGraphs(lhs, rhs)
	hj.filterConstPredicates()
	if len(hj.constPredicates) > 0 {
		hj.executeConstPredicates()
	}
	joinTables := hj.getJoinTables(chanSize)
	hj.joinTables = joinTables
	hj.initJoinTablesInfo()
	var xPredicates []rds.Predicate
	var yPredicate rds.Predicate
	xPredicates, yPredicate = transferSelfPredicate(hj.selfGraph, rhs, len(joinTables))
	joinTableData := NewJoinTableData(xPredicates, yPredicate, joinTables, bucketData)

	var usedJoinTableIndex []int
	for _, predicate := range xPredicates {
		lJoinTableIndex, rJoinTableIndex := utils.GetPredicateColumnIndexNew(predicate)
		usedJoinTableIndex = append(usedJoinTableIndex, lJoinTableIndex)
		usedJoinTableIndex = append(usedJoinTableIndex, rJoinTableIndex)
	}
	usedJoinTableIndex = utils2.Distinct(usedJoinTableIndex)

	var joinIntersection [][][]int32
	for _, predicate := range xPredicates {
		joinIntersection = hj.calcJoinTableIntersectionNew(joinIntersection, joinTableData, predicate, chanSize)
	}
	//logger.Infof("完成计算大宽表的交集")
	if len(joinIntersection) < 1 {
		return joinIntersection
	}
	// 2.对大宽表交集结果转换
	// 2-1 tid到tableIndex映射
	hj.getTid2TableIndex(lhs)
	// 2-2 joinTid到tids的映射
	hj.getJoinTidToTid()
	// 2-3 大宽表的交集 转换 多表交集的中间结果
	tmpIntersections := hj.getMultiTableTmpIntersection(joinIntersection)
	//logger.Infof("完成 大宽表交集 -> 临时交集 ")

	// 2-4 组成最终的多表交集结果
	return hj.transferMultiTableTmpIntersection(tmpIntersections)
}

// getGraphs 获取谓词的执行顺序
func (hj *HashJoin) getGraphs(lhs []rds.Predicate, rhs rds.Predicate) {
	joinOrder := intersection.NewJoinOrder(rhs)
	hj.crossGraphs = joinOrder.ForeignJoinPredicate(lhs)
	hj.semiGraphs = joinOrder.SelfJoinPredicate(lhs)
	hj.selfGraph = joinOrder.TransferSelfJoinRule(lhs)
}

// 获取大宽表 joinTableIndex(第几张大宽表) -> joinTableRow -> 数组下标为tableIndex,元素为(table1的rowId, table2的rowId, table3的rowId, ...)
func (hj *HashJoin) getJoinTables(chanSize int) [][][]int32 {
	// 2.根据cross谓词生成大宽表
	unique, err := hj.graphEdgeUnique(hj.crossGraphs)
	if err != nil {
		return [][][]int32{}
	}
	// 判断是否跨表谓词是否去过重

	joinTablesTmp := hj.generateJoinTables(unique, chanSize)

	// 3.根据semi谓词修改大宽表
	for i, predicates := range hj.semiGraphs {
		if predicates == nil {
			continue
		}
		joinTable := hj.selfHashJoinTable(predicates, joinTablesTmp[i], chanSize)
		joinTablesTmp[i] = joinTable
	}

	// 把行号展开
	return MultiSliceCartesian(joinTablesTmp)
}

func (hj *HashJoin) initJoinTablesInfo() {
	joinTableCount := len(hj.joinTables)
	// join表的表名
	var joinTableIds []string
	for i := 0; i < joinTableCount; i++ {
		joinTableId := fmt.Sprint(table_data.JoinTableIdPrefix, i)
		joinTableIds = append(joinTableIds, joinTableId)
	}
	// joinTid到joinTableIndex的映射
	// todo:这里先写死，后续再优化
	hj.joinTid2JoinTableIndex = make(map[int]int)
	if len(joinTableIds) == 1 {
		hj.joinTid2JoinTableIndex[0] = 0
		hj.joinTid2JoinTableIndex[1] = 0
	} else {
		// 最多两张大宽表
		hj.joinTid2JoinTableIndex[0] = 0
		hj.joinTid2JoinTableIndex[1] = 1
	}
}

// 结果：tid到tableIndex的映射 例如：t0/t1 -> shop(表索引:0) t2/t3 -> product(表索引:1) 输出结果:[0, 0, 1, 1]
func (hj *HashJoin) getTid2TableIndex(predicates []rds.Predicate) {
	maxTid := 0
	tableIndexMap := map[int]int{} // tid -> tableIndex
	tableIndexes := hj.bucketData.TableIndex
	for _, predicate := range predicates {
		lTableIndex := tableIndexes[predicate.LeftColumn.TableId]
		rTableIndex := tableIndexes[predicate.RightColumn.TableId]
		lTid := predicate.LeftColumn.ColumnIndex
		rTid := predicate.RightColumn.ColumnIndex
		tableIndexMap[lTid] = lTableIndex
		tableIndexMap[rTid] = rTableIndex
		maxTid = utils2.Max(maxTid, lTid)
		maxTid = utils2.Max(maxTid, rTid)
	}
	hj.tid2TableIndex = make([]int, maxTid+1)
	for tid, tableIndex := range tableIndexMap {
		hj.tid2TableIndex[tid] = tableIndex
	}
}

func (hj *HashJoin) getJoinTidToTid() {
	var joinTid2Tid = map[int][]int{
		0: make([]int, 0),
		1: make([]int, 0),
	}
	for joinTid, predicates := range hj.crossGraphs {
		for _, predicate := range predicates {
			joinTid2Tid[joinTid] = append(joinTid2Tid[0], predicate.LeftColumn.ColumnIndex, predicate.RightColumn.ColumnIndex)
		}
	}
	for joinTid, predicates := range hj.semiGraphs {
		for _, predicate := range predicates {
			joinTid2Tid[joinTid] = append(joinTid2Tid[0], predicate.LeftColumn.ColumnIndex, predicate.RightColumn.ColumnIndex)
		}
	}
	for _, predicate := range hj.selfGraph {
		joinTid2Tid[0] = append(joinTid2Tid[0], predicate.LeftColumn.ColumnIndex)
		joinTid2Tid[1] = append(joinTid2Tid[1], predicate.RightColumn.ColumnIndex)
	}
	hj.joinTid2Tids = joinTid2Tid
}

func getMaxTid(tids []int) int {
	max := 0
	for _, tid := range tids {
		if tid > max {
			max = tid
		}
	}
	return max
}

func (hj *HashJoin) transferMultiTableTmpIntersection(tmpIntersections [][][][]int32) [][][]int32 {
	allTids := make([]int, 0)
	for joinTid := range tmpIntersections {
		allTids = append(allTids, hj.joinTid2Tids[joinTid]...)
	}

	maxTid := getMaxTid(allTids)
	groupCount := len(tmpIntersections[0])
	resultIntersection := make([][][]int32, groupCount)
	for i := 0; i < groupCount; i++ {
		resultIntersection[i] = make([][]int32, maxTid+1)
	}

	for joinTid, tmpIntersection := range tmpIntersections { // 最多两层
		tids := hj.joinTid2Tids[joinTid]
		for gid, idPairs := range tmpIntersection { // 这个层数比较多
			for _, tid := range tids { // 这个层数比较少
				tableIndex := hj.tid2TableIndex[tid]
				resultIntersection[gid][tid] = idPairs[tableIndex]
			}
		}
	}
	return resultIntersection
}

func (hj *HashJoin) getMultiTableTmpIntersection(joinIntersection [][][]int32) [][][][]int32 { // joinTid -> group -> tableIndex -> rowIds
	joinTidCount := len(joinIntersection[0])
	joinTids := make([][][]int32, joinTidCount)      // joinTid -> group -> joinRowIds
	groups := make([][]int32, len(joinIntersection)) // group -> joinRowIds
	for i := 0; i < joinTidCount; i++ {
		joinTids[i] = groups
	}

	for group, idPairs := range joinIntersection {
		for joinTid, ids := range idPairs {
			joinTids[joinTid][group] = append(joinTids[joinTid][group], ids...)
		}
	}

	result := make([][][][]int32, joinTidCount) // joinTid -> group -> tableIndex -> rowIds
	for joinTid, groups := range joinTids {
		groupsResult := make([][][]int32, len(groups))
		for gid, joinRowIds := range groups {
			tablesRowIds := hj.joinRowsMappedToRows(joinTid, joinRowIds)
			groupsResult[gid] = tablesRowIds
		}
		result[joinTid] = groupsResult
	}
	return result
}

// joinRowsMappedToRows 大宽表的rowIds映射到各个表的rowIds
func (hj *HashJoin) joinRowsMappedToRows(joinTid int, joinRowIds []int32) [][]int32 { // 输出: tableIndex -> rowIds
	if len(joinRowIds) < 1 {
		logger.Warnf("[joinRowsMappedToRows] joinRowIds length is 0")
		return [][]int32{}
	}
	joinTableIndex := hj.joinTid2JoinTableIndex[joinTid]
	joinTableRows := hj.joinTables[joinTableIndex]
	//tableLen := len(joinTableRows)
	tableCount := len(joinTableRows[0])
	tablesRowIds := make([][]int32, tableCount)
	for tableIndex := 0; tableIndex < tableCount; tableIndex++ {
		tablesRowIds[tableIndex] = make([]int32, 0, len(joinRowIds))
	}

	// 遍历大宽表的每一行
	for _, joinRowId := range joinRowIds {
		for tableIndex, tableRowId := range joinTableRows[joinRowId] {
			tablesRowIds[tableIndex] = append(tablesRowIds[tableIndex], tableRowId)
		}
	}
	return tablesRowIds
}

// MultiSliceCartesian 把多张表hashJoin后的rowId按笛卡尔展开为大宽表
// 输入: [][][][]int32 大宽表序号(可能多张) -> 分组序号(没啥用) -> tableIndex -> rowIds
// 输出: [][][]int32   大宽表序号(可能多张) -> 大宽表rowId -> 数组下标为tableIndex,元素为(table1的rowId, table2的rowId, table3的rowId, ...)
func MultiSliceCartesian(hashJoinTables [][][][]int32) [][][]int32 {
	var joinTables [][][]int32
	for _, table := range hashJoinTables {
		joinTable := make([][]int32, 0)
		for _, idPairs := range table {
			list1 := idPairs[0]
			twoDim := make([][]int32, len(list1))
			for idx, item := range list1 {
				twoDim[idx] = []int32{item}
			}
			for _, items := range idPairs[1:] {
				result := towSliceCartesian(twoDim, items)
				twoDim = result
			}
			joinTable = append(joinTable, twoDim...)
		}
		joinTables = append(joinTables, joinTable)
	}
	return joinTables
}

func towSliceCartesian(list1 [][]int32, list2 []int32) [][]int32 {
	var res [][]int32
	for _, item1 := range list1 {
		for _, item2 := range list2 {
			tmpItem := make([]int32, 0, len(item1))
			tmpItem = append(tmpItem, item1...)
			tmpItem = append(tmpItem, item2)
			res = append(res, tmpItem)
		}
	}
	return res
}

func transferSelfPredicate(xPredicates []rds.Predicate, yPredicate rds.Predicate, tableCount int) (x []rds.Predicate, y rds.Predicate) {
	x = make([]rds.Predicate, 0, len(xPredicates))
	if tableCount == 1 {
		for _, predicate := range xPredicates {
			predicate.LeftColumn.JoinTableId = "t0"
			predicate.LeftColumn.ColumnIndex = 0
			predicate.RightColumn.JoinTableId = "t0"
			predicate.RightColumn.ColumnIndex = 1
			x = append(x, predicate)
		}
		yPredicate.LeftColumn.JoinTableId = "t0"
		yPredicate.LeftColumn.ColumnIndex = 0
		yPredicate.RightColumn.JoinTableId = "t0"
		yPredicate.RightColumn.ColumnIndex = 1
		y = yPredicate
	} else {
		for _, predicate := range xPredicates {
			predicate.LeftColumn.JoinTableId = "t0"
			predicate.LeftColumn.ColumnIndex = 0
			predicate.RightColumn.JoinTableId = "t1"
			predicate.RightColumn.ColumnIndex = 1
			x = append(x, predicate)
		}
		yPredicate.LeftColumn.JoinTableId = "t0"
		yPredicate.LeftColumn.ColumnIndex = 0
		yPredicate.RightColumn.JoinTableId = "t1"
		yPredicate.RightColumn.ColumnIndex = 1
		y = yPredicate
	}
	return x, y
}

//func cartesianProduct(idPairs [][]int32) [][]int32 {
//	var result [][]int32
//	if len(idPairs) == 0 {
//		return result
//	}
//	for _, id := range idPairs[0] {
//		for _, id1 := range cartesianProduct(idPairs[1:]) {
//			var row = []int32{id, id1}
//			row = append()
//			result = append(result)
//		}
//	}
//	return result
//}

// GenerateJoinTables 按JoinOrder生成大宽表(最多两张)
func (hj *HashJoin) generateJoinTables(graph [][]rds.Predicate, chanSize int) [][][][]int32 {
	joinTables := make([][][][]int32, 0, len(graph))
	for _, predicates := range graph {
		var joinTable [][][]int32
		joinTable = hj.hashJoinTable(predicates, chanSize, joinTable)
		joinTables = append(joinTables, joinTable)
	}
	return joinTables
}

// hashJoinTable
func (hj *HashJoin) hashJoinTable(predicates []rds.Predicate, chanSize int, joinTable [][][]int32) [][][]int32 {
	for _, predicate := range predicates {
		joinTable = hj.hashJoinByPredicate(joinTable, predicate, chanSize)
	}
	return joinTable
}

func (hj *HashJoin) hashJoinByPredicate(hashJoin [][][]int32, predicate rds.Predicate, chanSize int) [][][]int32 {
	var hashJoinResult [][][]int32
	bucketData := hj.bucketData

	leftTableId := predicate.LeftColumn.TableId
	rightTableId := predicate.RightColumn.TableId
	leftColumn := predicate.LeftColumn.ColumnId
	rightColumn := predicate.RightColumn.ColumnId
	leftTableIndex := bucketData.TableIndex[leftTableId]
	rightTableIndex := bucketData.TableIndex[rightTableId]

	if len(hashJoin) < 1 {
		maxTableIndex := rightTableIndex
		if leftTableIndex > maxTableIndex {
			maxTableIndex = leftTableIndex
		}
		pli := bucket_data.CreateTempPLI(bucketData, predicate)
		var leftPli, rightPli = pli[leftTableId][leftColumn], pli[rightTableId][rightColumn]

		for value, leftIds := range leftPli {
			idPairs := make([][]int32, maxTableIndex+1)
			rightIds := rightPli[value]
			if len(rightIds) > 0 {
				idPairs[leftTableIndex] = leftIds
				idPairs[rightTableIndex] = rightIds
			}
			hashJoinResult = append(hashJoinResult, idPairs)
		}
		return hashJoinResult
	}

	leftValues := bucketData.BucketValues[leftTableId][leftColumn]
	rightValues := bucketData.BucketValues[rightTableId][rightColumn]

	var newIdPairSize = len(hashJoin[0])
	newIdPairSize = utils2.Max(newIdPairSize, leftTableIndex+1)
	newIdPairSize = utils2.Max(newIdPairSize, rightTableIndex+1)

	var leftPad = leftTableIndex >= len(hashJoin[0]) || len(hashJoin[0][leftTableIndex]) < 1
	var leftPli map[interface{}][]int32
	if leftPad {
		leftPli = bucket_data.CreateColumnPLI(bucketData, leftTableId, leftColumn)
	}
	var rightPad = rightTableIndex >= len(hashJoin[0]) || len(hashJoin[0][rightTableIndex]) < 1
	var rightPli map[interface{}][]int32
	if rightPad {
		rightPli = bucket_data.CreateColumnPLI(bucketData, rightTableId, rightColumn)
	}

	var parallelIntersection = make([]intersection.CacheValue[[][][]int32], chanSize)
	_ = storage_utils.ParallelBatch(0, len(hashJoin), chanSize, func(start, end, workerId int) error {
		for _, idPairs := range hashJoin[start:end] {
			var tempIdPairs [][][]int32
			value2Index := make(map[interface{}]int)
			for tableIndex, ids := range idPairs {
				if tableIndex == leftTableIndex || tableIndex == rightTableIndex {
					for _, rowId := range ids {
						var value interface{}
						if tableIndex == leftTableIndex {
							value = leftValues[rowId]
						} else {
							value = rightValues[rowId]
						}
						if value == nil || value == "" {
							continue
						}
						index, ok := value2Index[value]
						if !ok {
							var pad []int32
							if rightPad && tableIndex == leftTableIndex {
								pad = rightPli[value]
								if len(pad) == 0 {
									continue
								}
							}
							if leftPad && tableIndex == rightTableIndex {
								pad = leftPli[value]
								if len(pad) == 0 {
									continue
								}
							}
							value2Index[value] = len(tempIdPairs)
							index = len(tempIdPairs)
							tempIdPairs = append(tempIdPairs, make([][]int32, newIdPairSize))
							if leftPad {
								tempIdPairs[index][leftTableIndex] = pad
							}
							if rightPad {
								tempIdPairs[index][rightTableIndex] = pad
							}
						}
						tempIdPairs[index][tableIndex] = append(tempIdPairs[index][tableIndex], rowId)
					}
				}
			}

			for k := range tempIdPairs {
				flag := false
				for tableIndex, rowIds := range tempIdPairs[k] {
					if tableIndex == leftTableIndex || tableIndex == rightTableIndex {
						if len(rowIds) < 1 {
							flag = true
							continue
						}
					}
				}
				if flag {
					continue
				}
				parallelIntersection[workerId].Val = append(parallelIntersection[workerId].Val, tempIdPairs[k])
			}
		}
		return nil
	})

	for _, tempIntersection := range parallelIntersection {
		hashJoinResult = append(hashJoinResult, tempIntersection.Val...)
	}
	return hashJoinResult
}

func (hj *HashJoin) selfHashJoinTable(predicates []rds.Predicate, joinTable [][][]int32, chanSize int) [][][]int32 {
	return hj.hashJoinTable(predicates, chanSize, joinTable)
}

// calcJoinTableIntersection 计算大宽表交集
func (hj *HashJoin) calcJoinTableIntersectionNew(intersection [][][]int32, joinTableData *JoinTableData, predicate rds.Predicate, chanSize int) [][][]int32 {
	//logger.Infof("cal intersection:%v", predicate.PredicateStr)
	if len(intersection) < 1 {
		return generateJoinTablePredicateIntersectionNew(predicate, joinTableData)
	}
	leftTableId := predicate.LeftColumn.JoinTableId
	rightTableId := predicate.RightColumn.JoinTableId
	leftColumnName := predicate.LeftColumn.ColumnId
	rightColumnName := predicate.RightColumn.ColumnId
	leftTableIndex, rightTableIndex := utils.GetPredicateColumnIndexNew(predicate)

	leftValues := joinTableData.TableValues[leftTableId][leftColumnName]
	rightValues := joinTableData.TableValues[rightTableId][rightColumnName]

	var newIdPairSize = len(intersection[0])
	newIdPairSize = utils2.Max(newIdPairSize, leftTableIndex+1)
	newIdPairSize = utils2.Max(newIdPairSize, rightTableIndex+1)

	var parallelIntersection = make([]CacheValue[[][][]int32], chanSize)
	_ = storage_utils.ParallelBatch(0, len(intersection), chanSize, func(start, end, workerId int) error {
		for _, idPairs := range intersection[start:end] {
			var tempIdPairs [][][]int32
			value2Index := make(map[interface{}]int)
			for tableIndex, ids := range idPairs {
				if tableIndex == leftTableIndex || tableIndex == rightTableIndex {
					for _, rowId := range ids {
						var value interface{}
						if tableIndex == leftTableIndex {
							value = leftValues[rowId]
						} else {
							value = rightValues[rowId]
						}
						if value == nil || value == "" {
							continue
						}
						index, ok := value2Index[value]
						if !ok {
							value2Index[value] = len(tempIdPairs)
							index = len(tempIdPairs)
							tempIdPairs = append(tempIdPairs, make([][]int32, newIdPairSize))
						}
						tempIdPairs[index][tableIndex] = append(tempIdPairs[index][tableIndex], rowId)
					}
				}
			}

			for k := range tempIdPairs {
				flag := false
				for tableIndex, rowIds := range tempIdPairs[k] {
					if tableIndex == leftTableIndex || tableIndex == rightTableIndex {
						if len(rowIds) < 1 {
							flag = true
							continue
						}
					}
				}
				if flag {
					continue
				}
				parallelIntersection[workerId].Val = append(parallelIntersection[workerId].Val, tempIdPairs[k])
			}
		}
		return nil
	})

	var resultIntersection [][][]int32
	for _, tempIntersection := range parallelIntersection {
		resultIntersection = append(resultIntersection, tempIntersection.Val...)
	}
	return resultIntersection
}

func generateJoinTablePredicateIntersectionNew(predicate rds.Predicate, joinTableData *JoinTableData) [][][]int32 {
	var intersection [][][]int32
	leftTableId := predicate.LeftColumn.JoinTableId
	rightTableId := predicate.RightColumn.JoinTableId
	leftColumnName := predicate.LeftColumn.ColumnId
	rightColumnName := predicate.RightColumn.ColumnId
	leftTableIndex, rightTableIndex := utils.GetPredicateColumnIndexNew(predicate)
	maxTableIndex := rightTableIndex
	if leftTableIndex > rightTableIndex {
		maxTableIndex = leftTableIndex
	}

	var pli map[string]map[string]map[interface{}][]int32
	if len(joinTableData.PLI) < 1 {
		pli = joinTableData.CreateTempPLI(predicate)
	}
	var leftPli, rightPli = pli[leftTableId][leftColumnName], pli[rightTableId][rightColumnName]

	isSameColumn := leftTableId == rightTableId && leftColumnName == rightColumnName
	for value, leftIds := range leftPli {
		idPairs := make([][]int32, maxTableIndex+1)
		if isSameColumn {
			idPairs[leftTableIndex] = leftIds
			idPairs[rightTableIndex] = leftIds
		} else {
			rightIds := rightPli[value]
			if len(rightIds) > 0 {
				idPairs[leftTableIndex] = leftIds
				idPairs[rightTableIndex] = rightIds
			} else {
				continue
			}
		}
		intersection = append(intersection, idPairs)
	}
	return intersection
}

// graphEdgeUnique 跨表谓词去重
func (hj *HashJoin) graphEdgeUnique(graph [][]rds.Predicate) ([][]rds.Predicate, error) {
	if len(graph) == 0 {
		logger.Warnf("[graphEdgeUnique] graph is null")
		return nil, errors.New("graph is null")
	}
	graphUnique := make([][]rds.Predicate, 0, 2)
	if len(graph) < 2 {
		unique := hj.graphPathUnique(graph[0])
		graphUnique = append(graphUnique, unique)
		return graphUnique, nil
	}

	path0 := graph[0]
	if path0 != nil {
		path0 = hj.graphPathUnique(path0)
	}
	path1 := graph[1]
	if path1 != nil {
		path1 = hj.graphPathUnique(path1)
	}
	for _, p1 := range path0 {
		for i, p2 := range path1 {
			if sameLeftRight(p1, p2) {
				hj.isDuplicate = true
				path1 = append(path1[:i], path1[i+1:]...)
			}
		}
	}
	if len(path0) > 0 {
		graphUnique = append(graphUnique, path0)
	}
	if len(path1) > 0 {
		graphUnique = append(graphUnique, path1)
	}
	return graphUnique, nil
}

// 对图的一条路径去重
func (hj *HashJoin) graphPathUnique(path []rds.Predicate) []rds.Predicate {
	for idx, p := range path {
		nextIdx := idx + 1
		if nextIdx >= len(path) {
			break
		}
		if sameLeftRight(p, path[nextIdx]) {
			hj.isDuplicate = true
			path = append(path[:nextIdx], path[nextIdx+1:]...)
		}
	}
	return path
}

func sameLeftRight(p1 rds.Predicate, p2 rds.Predicate) bool {
	return p1.LeftColumn.TableId == p2.LeftColumn.TableId && p1.LeftColumn.ColumnId == p2.LeftColumn.ColumnId &&
		p1.RightColumn.TableId == p2.RightColumn.TableId && p1.RightColumn.ColumnId == p2.RightColumn.ColumnId
}

type CacheValue[E any] struct {
	Val E
	_   [64]byte
}

func (hj *HashJoin) filterConstPredicates() {
	constPredicates := make([]rds.Predicate, 0)
	resultGraphs := make([][]rds.Predicate, len(hj.crossGraphs))
	for gid, graph := range hj.crossGraphs {
		filterGraph := make([]rds.Predicate, 0)
		for _, predicate := range graph {
			if predicate.PredicateType == 0 {
				constPredicates = append(constPredicates, predicate)
			} else {
				filterGraph = append(filterGraph, predicate)
			}
		}
		resultGraphs[gid] = filterGraph
	}
	hj.crossGraphs = resultGraphs
	hj.constPredicates = constPredicates
}

// executeConstPredicates 先执行常数谓词对表的数据做过滤
func (hj *HashJoin) executeConstPredicates() {
	// 按表和列来区分常数谓词，一列可能存在t0.a > 1 且 t0.a < 10
	startTime := time.Now()
	table2Predicates := make(map[string]map[string][]rds.Predicate) // tableId -> column -> predicates
	for _, predicate := range hj.constPredicates {
		tableId := predicate.LeftColumn.TableId
		columnId := predicate.LeftColumn.ColumnId
		if _, ok := table2Predicates[tableId]; !ok {
			predicates := []rds.Predicate{predicate}
			table2Predicates[tableId] = map[string][]rds.Predicate{columnId: predicates}
			continue
		}
		if _, ok := table2Predicates[tableId][columnId]; !ok {
			table2Predicates[tableId][columnId] = []rds.Predicate{predicate}
			continue
		}
		table2Predicates[tableId][columnId] = append(table2Predicates[tableId][columnId], predicate)
	}

	for tableId, col2Predicates := range table2Predicates {
		for columnId, predicates := range col2Predicates {
			rows := hj.bucketData.BucketValues[tableId][columnId]
			newRows := make([]interface{}, 0)
			for _, value := range rows {
				if filterRow(value, predicates) {
					//rows = append(rows[:id], rows[id+1:])  // 数据量大时很难，但内存共享
					continue
				}
				newRows = append(newRows, value)
			}
			hj.bucketData.BucketValues[tableId][columnId] = newRows
		}
	}
	logger.Infof("[executeConstPredicates] 执行常数谓词耗时:%s", utils2.DurationString(startTime))
}

// filterRow true:不满足谓词的过滤掉, false:满足谓词保留
func filterRow(currentValue any, predicate []rds.Predicate) bool {
	for _, p := range predicate {
		constantValue := p.ConstantValue
		switch p.SymbolType {
		case enum.Equal:
			if currentValue != constantValue {
				return true
			}
		case enum.NotEqual:
			if currentValue == constantValue {
				return true
			}
		case enum.GreaterE:
			if float64Of(currentValue) < float64Of(constantValue) {
				return true
			}
		case enum.Greater:
			if float64Of(currentValue) <= float64Of(constantValue) {
				return true
			}
		case enum.Less:
			if float64Of(currentValue) >= float64Of(constantValue) {
				return true
			}
		case enum.LessE:
			if float64Of(currentValue) > float64Of(constantValue) {
				return true
			}
		}
	}
	return false
}

func float64Of(i any) float64 {
	if i == nil {
		return math.NaN()
	}
	switch v := i.(type) {
	case int64:
		return float64(v)
	case float64:
		return v
	case decimal.BigDecimal:
		return v.FloatVal()
	case timetype.Time:
		return float64(v.UnixMilli())
	case string:
		return parseFloat64(v)
	default:
		s := utils2.ToString(i)
		return parseFloat64(s)
	}
}

func parseFloat64(s string) float64 {
	if len(s) == 0 {
		return math.NaN()
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return math.NaN()
	}
	return f
}
