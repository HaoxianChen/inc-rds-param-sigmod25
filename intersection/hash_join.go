package intersection

import (
	"errors"
	"fmt"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables/table_data"
	"gitlab.grandhoo.com/rock/rock_v3/rds_config"
	"gitlab.grandhoo.com/rock/rock_v3/request"
	"gitlab.grandhoo.com/rock/rock_v3/utils"
	"gitlab.grandhoo.com/rock/rock_v3/utils/storage_utils"
	utils2 "gitlab.grandhoo.com/rock/storage/storage2/utils"
	"time"
)

type IHashJoin interface {
	HashJoinHttp(req *request.HashJoinReq) *request.HashJoinResp
	CalcRule(taskId int64, lhs []rds.Predicate, rhs rds.Predicate, chanSize int) (int, int, int)
	GenerateJoinTables(taskId int64, graph [][]rds.Predicate, chanSize int) [][][][]int32
	CalcIntersection(taskId int64, lhs []rds.Predicate, rhs rds.Predicate, chanSize int) [][][]int32
	//CalcIntersection(intersection [][][]int32, joinTableData *table_data.JoinTableData, predicate rds.Predicate, chanSize int) [][][]int32
	CalcRuleNew(taskId int64, lhs []rds.Predicate, rhs rds.Predicate, chanSize int) (int, int, int, [][][]int32)
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
}

func NewHashJoin() IHashJoin {
	return &HashJoin{}
}

func (hj *HashJoin) HashJoinHttp(req *request.HashJoinReq) *request.HashJoinResp {
	taskId := req.TaskId
	tableIds := req.TableIds
	chanSize := req.ChanSize
	if chanSize == 0 {
		chanSize = 128
	}
	rule := utils.GetFormatRule(req.Rule)
	// 预热数据
	loadStartTime := time.Now()
	logger.Infof("load data...")
	table_data.LoadSchema(taskId, tableIds)
	table_data.LoadDataCreatePli(taskId, tableIds)
	storage_utils.DropCacheDataBase()
	table_data.CreateIndex(taskId)
	logger.Infof("finish load data, time:%s", utils2.DurationString(loadStartTime))

	startTime := time.Now()
	logger.Infof("multi table rule, cal rule...")
	rowSize, xSupp, xySupp := NewHashJoin().CalcRule(taskId, rule.LhsPredicates, rule.Rhs, chanSize)
	useTime := utils2.DurationString(startTime)
	logger.Infof("finish cal multi table rule, time:%s|rowSize:%d|xSupp:%d|xySupp:%d", useTime, rowSize, xSupp, xySupp)

	//logger.Infof("multi table rule, cal rowSize...")
	//lhs := rule.LhsPredicates
	//var fkPredicates []rds.Predicate
	//var rowSize int
	//for _, lh := range lhs {
	//	if storage_utils.ForeignKeyPredicate(&lh) {
	//		fkPredicates = append(fkPredicates, lh)
	//	}
	//}
	//if len(fkPredicates) == len(lhs) {
	//	rowSize = xSupp
	//} else {
	//	uniqueFp, uniqueOk := storage_utils.ForeignKeyPredicatesUnique(fkPredicates)
	//	if uniqueOk {
	//		fkPredicates = uniqueFp
	//	}
	//	var fkUsedTableIndex []int
	//	for _, p := range fkPredicates {
	//		lTid, rTid := utils.GetPredicateColumnIndexNew(p)
	//		fkUsedTableIndex = append(fkUsedTableIndex, lTid)
	//		fkUsedTableIndex = append(fkUsedTableIndex, rTid)
	//	}
	//	fkUsedTableIndex = utils2.Distinct(fkUsedTableIndex)
	//	var fkIntersection = CalIntersection(nil, fkPredicates[0], taskId, chanSize)
	//
	//	for _, each := range fkIntersection {
	//		tmpFkIntersection := [][][]int32{each}
	//		for _, p := range fkPredicates[1:] {
	//			tmpFkIntersection = CalIntersection(tmpFkIntersection, p, taskId, chanSize)
	//			if len(tmpFkIntersection) < 1 {
	//				break
	//			}
	//		}
	//		rowSize += CalIntersectionSupport(tmpFkIntersection, fkUsedTableIndex, chanSize)
	//	}
	//
	//	if uniqueOk {
	//		rowSize *= rowSize
	//	}
	//}
	//logger.Infof("finish cal multi table rule rowSize:%v", utils.GetLhsStr(fkPredicates))
	//useTime := utils2.DurationString(startTime)
	//logger.Infof("finish cal rule time:%s|rowSize:%d|xSupp:%d|xySupp:%d|", useTime, rowSize, xSupp, xySupp)
	return &request.HashJoinResp{
		RowSize:      rowSize,
		XSupp:        xSupp,
		XySupp:       xySupp,
		HashJoinTime: useTime,
	}
}

func (hj *HashJoin) CalcRule(taskId int64, lhs []rds.Predicate, rhs rds.Predicate, chanSize int) (rowSize int, xSupp int, xySupp int) {
	//// 1.根据Y获取谓词执行顺序
	//joinOrder := NewJoinOrder(rhs)

	//// 2.根据跨表谓词生成大宽表
	//graph := joinOrder.ForeignJoinPredicate(lhs)
	//unique, err := graphEdgeUnique(graph)
	//if err != nil {
	//	return 0, 0
	//}
	//joinTablesTmp := hj.GenerateJoinTables(taskId, unique, chanSize)
	//
	//// 3.补充扩展
	//selfGraph := joinOrder.SelfJoinPredicate(lhs)
	//for i, predicates := range selfGraph {
	//	if predicates == nil {
	//		continue
	//	}
	//	joinTable := selfHashJoinTable(predicates, joinTablesTmp[i], taskId, chanSize)
	//	joinTablesTmp[i] = joinTable
	//}
	//
	//
	//// 5.把行号展开
	//joinTables := MultiSliceCartesian(joinTablesTmp)
	rowSize = -1
	hj.getGraphs(lhs, rhs)
	// 获取大宽表
	joinTables := hj.getJoinTables(taskId, chanSize)

	// 4.获取表内谓词
	var xPredicates []rds.Predicate
	var yPredicate rds.Predicate
	xPredicates, yPredicate = transferSelfPredicate(hj.selfGraph, rhs, len(joinTables))
	// 加载JoinTable的数据
	//joinTableData := table_data.NewJoinTableData(xPredicates, yPredicate, joinTables, taskId)
	joinTableData := table_data.NewJoinTableDataNew(xPredicates, yPredicate, joinTables, taskId)

	// 计算大宽表交集
	var usedJoinTableIndex []int
	for _, predicate := range xPredicates {
		lJoinTableIndex, rJoinTableIndex := utils.GetPredicateColumnIndexNew(predicate)
		usedJoinTableIndex = append(usedJoinTableIndex, lJoinTableIndex)
		usedJoinTableIndex = append(usedJoinTableIndex, rJoinTableIndex)
	}
	usedJoinTableIndex = utils2.Distinct(usedJoinTableIndex)

	// todo 获取rowSize的方式需要优化
	var intersection [][][]int32
	if len(xPredicates) == 0 {
		// todo 目前不会走到这个分支，这个地方再想想
		xSupp = len(joinTables[0])
		if rowSize == -1 {
			rowSize = xSupp
			rowSize *= rowSize
		}
	} else {
		if len(joinTables) == 1 {
			rowSize = len(joinTables[0])
			if hj.isDuplicate {
				rowSize *= rowSize
			}
		}
		intersection = hj.calcJoinTableIntersectionNew(intersection, joinTableData, xPredicates[0], chanSize)
		if len(xPredicates) == 1 {
			xSupp = CalIntersectionSupport(intersection, usedJoinTableIndex, chanSize)
			if rowSize == -1 {
				rowSize = xSupp
				if hj.isDuplicate {
					rowSize *= rowSize
				}
			}
		} else {
			if rowSize == -1 {
				rowSize = CalIntersectionSupport(intersection, usedJoinTableIndex, chanSize)
				if hj.isDuplicate {
					rowSize *= rowSize
				}
			}
			for _, predicate := range xPredicates[1:] {
				//intersection = hj.calcJoinTableIntersection(intersection, joinTableData, predicate, chanSize)
				intersection = hj.calcJoinTableIntersectionNew(intersection, joinTableData, predicate, chanSize)
			}
			xSupp = CalIntersectionSupport(intersection, usedJoinTableIndex, chanSize)
		}
	}

	//intersection = hj.calcJoinTableIntersection(intersection, joinTableData, yPredicate, chanSize)
	intersection = hj.calcJoinTableIntersectionNew(intersection, joinTableData, yPredicate, chanSize)
	xySupp = CalIntersectionSupport(intersection, usedJoinTableIndex, chanSize)

	if rowSize == -1 {
		rowSize = 0
	}
	return rowSize, xSupp, xySupp
}

func (hj *HashJoin) CalcIntersection(taskId int64, lhs []rds.Predicate, rhs rds.Predicate, chanSize int) [][][]int32 {
	// 1.先计算大宽表交集结果
	hj.getGraphs(lhs, rhs)
	joinTables := hj.getJoinTables(taskId, chanSize)
	hj.joinTables = joinTables
	hj.initJoinTablesInfo()
	var xPredicates []rds.Predicate
	var yPredicate rds.Predicate
	xPredicates, yPredicate = transferSelfPredicate(hj.selfGraph, rhs, len(joinTables))
	//joinTableData := table_data.NewJoinTableData(xPredicates, yPredicate, joinTables, taskId)
	joinTableData := table_data.NewJoinTableDataNew(xPredicates, yPredicate, joinTables, taskId)

	var usedJoinTableIndex []int
	for _, predicate := range xPredicates {
		lJoinTableIndex, rJoinTableIndex := utils.GetPredicateColumnIndexNew(predicate)
		usedJoinTableIndex = append(usedJoinTableIndex, lJoinTableIndex)
		usedJoinTableIndex = append(usedJoinTableIndex, rJoinTableIndex)
	}
	usedJoinTableIndex = utils2.Distinct(usedJoinTableIndex)

	var joinIntersection [][][]int32
	for _, predicate := range xPredicates {
		//joinIntersection = hj.calcJoinTableIntersection(joinIntersection, joinTableData, predicate, chanSize)
		joinIntersection = hj.calcJoinTableIntersectionNew(joinIntersection, joinTableData, predicate, chanSize)
	}
	logger.Infof("完成计算大宽表的交集")
	// 2.对大宽表交集结果转换
	// 2-1 tid到tableIndex映射
	hj.getTid2TableIndex(taskId, lhs)
	// 2-2 joinTid到tids的映射
	hj.getJoinTidToTid()
	// 2-3 大宽表的交集 转换 多表交集的中间结果
	tmpIntersections := hj.getMultiTableTmpIntersection(joinIntersection)
	logger.Infof("完成 大宽表交集 -> 临时交集 ")

	// 2-4 组成最终的多表交集结果
	return hj.transferMultiTableTmpIntersection(tmpIntersections)
}

// getGraphs 获取谓词的执行顺序
func (hj *HashJoin) getGraphs(lhs []rds.Predicate, rhs rds.Predicate) {
	joinOrder := NewJoinOrder(rhs)
	hj.crossGraphs = joinOrder.ForeignJoinPredicate(lhs)
	hj.semiGraphs = joinOrder.SelfJoinPredicate(lhs)
	hj.selfGraph = joinOrder.TransferSelfJoinRule(lhs)
}

// 获取大宽表 joinTableIndex(第几张大宽表) -> joinTableRow -> 数组下标为tableIndex,元素为(table1的rowId, table2的rowId, table3的rowId, ...)
func (hj *HashJoin) getJoinTables(taskId int64, chanSize int) [][][]int32 {
	// 2.根据cross谓词生成大宽表
	unique, err := hj.graphEdgeUnique(hj.crossGraphs)
	if err != nil {
		return [][][]int32{}
	}
	// 判断是否跨表谓词是否去过重

	joinTablesTmp := hj.GenerateJoinTables(taskId, unique, chanSize)

	// 3.根据semi谓词修改大宽表
	for i, predicates := range hj.semiGraphs {
		if predicates == nil {
			continue
		}
		joinTable := selfHashJoinTable(predicates, joinTablesTmp[i], taskId, chanSize)
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
func (hj *HashJoin) getTid2TableIndex(taskId int64, predicates []rds.Predicate) {
	maxTid := 0
	tableIndexMap := map[int]int{} // tid -> tableIndex
	tableIndexes := table_data.GetTask(taskId).TableIndex
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

// transferMultiTableIntersection 转换交集结果
// input param:  group -> joinTid -> joinRowId
// output param: group -> tid -> rowId
//func (hj *HashJoin) transferMultiTableIntersection(joinIntersection [][][]int32) [][][]int32 {
//	allTids := make([]int, 0)
//	allTids = append(allTids, hj.joinTid2Tids[0]...)
//	allTids = append(allTids, hj.joinTid2Tids[1]...)
//	maxTid := getMaxTid(allTids)
//	groupCount := len(joinIntersection)
//}

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
func (hj *HashJoin) GenerateJoinTables(taskId int64, graph [][]rds.Predicate, chanSize int) [][][][]int32 {
	joinTables := make([][][][]int32, 0, len(graph))
	for _, predicates := range graph {
		var joinTable [][][]int32
		joinTable = hashJoinTable(taskId, predicates, chanSize, joinTable)
		joinTables = append(joinTables, joinTable)
	}
	return joinTables
}

// hashJoinTable
func hashJoinTable(taskId int64, predicates []rds.Predicate, chanSize int, joinTable [][][]int32) [][][]int32 {
	for _, predicate := range predicates {
		joinTable = hashJoinByPredicate(joinTable, predicate, taskId, chanSize)
	}
	return joinTable
}

func hashJoinByPredicate(hashJoin [][][]int32, predicate rds.Predicate, taskId int64, chanSize int) [][][]int32 {
	var hashJoinResult [][][]int32
	task := table_data.GetTask(taskId)
	leftTableId := predicate.LeftColumn.TableId
	rightTableId := predicate.RightColumn.TableId
	leftColumn := predicate.LeftColumn.ColumnId
	rightColumn := predicate.RightColumn.ColumnId
	leftTableIndex := task.TableIndex[leftTableId]
	rightTableIndex := task.TableIndex[rightTableId]

	if len(hashJoin) < 1 {
		maxTableIndex := rightTableIndex
		if leftTableIndex > maxTableIndex {
			maxTableIndex = leftTableIndex
		}
		var leftPli, rightPli = task.IndexPLI[leftTableId][leftColumn], task.IndexPLI[rightTableId][rightColumn]
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

	leftValues := task.TableIndexValues[leftTableId][leftColumn]
	rightValues := task.TableIndexValues[rightTableId][rightColumn]
	var leftPli, rightPli = task.IndexPLI[leftTableId][leftColumn], task.IndexPLI[rightTableId][rightColumn]

	var newIdPairSize = len(hashJoin[0])
	newIdPairSize = utils2.Max(newIdPairSize, leftTableIndex+1)
	newIdPairSize = utils2.Max(newIdPairSize, rightTableIndex+1)

	var leftPad = leftTableIndex >= len(hashJoin[0]) || len(hashJoin[0][leftTableIndex]) < 1
	var rightPad = rightTableIndex >= len(hashJoin[0]) || len(hashJoin[0][rightTableIndex]) < 1

	var parallelIntersection = make([]CacheValue[[][][]int32], chanSize)
	_ = storage_utils.ParallelBatch(0, len(hashJoin), chanSize, func(start, end, workerId int) error {
		for _, idPairs := range hashJoin[start:end] {
			var tempIdPairs [][][]int32
			value2Index := make(map[int32]int)
			for tableIndex, ids := range idPairs {
				if tableIndex == leftTableIndex || tableIndex == rightTableIndex {
					for _, rowId := range ids {
						var value int32
						if tableIndex == leftTableIndex {
							value = leftValues[rowId]
						} else {
							value = rightValues[rowId]
						}
						if value == rds_config.NilIndex {
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

func selfHashJoinTable(predicates []rds.Predicate, joinTable [][][]int32, taskId int64, chanSize int) [][][]int32 {
	return hashJoinTable(taskId, predicates, chanSize, joinTable)
}

// calcJoinTableIntersection 计算大宽表交集
func (hj *HashJoin) calcJoinTableIntersection(intersection [][][]int32, joinTableData *table_data.JoinTableData, predicate rds.Predicate, chanSize int) [][][]int32 {
	//logger.Infof("cal intersection:%v", predicate.PredicateStr)
	if len(intersection) < 1 {
		return generateJoinTablePredicateIntersection(predicate, joinTableData)
	}
	leftTableId := predicate.LeftColumn.JoinTableId
	rightTableId := predicate.RightColumn.JoinTableId
	leftColumnName := predicate.LeftColumn.ColumnId
	rightColumnName := predicate.RightColumn.ColumnId
	leftTableIndex, rightTableIndex := utils.GetPredicateColumnIndexNew(predicate)

	leftValues := joinTableData.TableIndexValues[leftTableId][leftColumnName]
	rightValues := joinTableData.TableIndexValues[rightTableId][rightColumnName]
	var leftPli, rightPli = joinTableData.IndexPLI[leftTableId][leftColumnName], joinTableData.IndexPLI[rightTableId][rightColumnName]

	var newIdPairSize = len(intersection[0])
	newIdPairSize = utils2.Max(newIdPairSize, leftTableIndex+1)
	newIdPairSize = utils2.Max(newIdPairSize, rightTableIndex+1)

	var leftPad = leftTableIndex >= len(intersection[0]) || len(intersection[0][leftTableIndex]) < 1
	var rightPad = rightTableIndex >= len(intersection[0]) || len(intersection[0][rightTableIndex]) < 1

	var noLimitIndexes []int
	for tableIndex := range intersection[0] {
		if tableIndex != leftTableIndex && tableIndex != rightTableIndex {
			noLimitIndexes = append(noLimitIndexes, tableIndex)
		}
	}

	var parallelIntersection = make([]CacheValue[[][][]int32], chanSize)
	_ = storage_utils.ParallelBatch(0, len(intersection), chanSize, func(start, end, workerId int) error {
		for _, idPairs := range intersection[start:end] {
			var tempIdPairs [][][]int32
			value2Index := make(map[int32]int)
			for tableIndex, ids := range idPairs {
				if tableIndex == leftTableIndex || tableIndex == rightTableIndex {
					for _, rowId := range ids {
						var value int32
						if tableIndex == leftTableIndex {
							value = leftValues[rowId]
						} else {
							value = rightValues[rowId]
						}
						if value == rds_config.NilIndex {
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
				for _, tableIndex := range noLimitIndexes {
					tempIdPairs[k][tableIndex] = idPairs[tableIndex]
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

func generateJoinTablePredicateIntersection(predicate rds.Predicate, joinTableData *table_data.JoinTableData) [][][]int32 {
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
	var leftPli, rightPli = joinTableData.IndexPLI[leftTableId][leftColumnName], joinTableData.IndexPLI[rightTableId][rightColumnName]
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

// calcJoinTableIntersection 计算大宽表交集
func (hj *HashJoin) calcJoinTableIntersectionNew(intersection [][][]int32, joinTableData *table_data.JoinTableData, predicate rds.Predicate, chanSize int) [][][]int32 {
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

func generateJoinTablePredicateIntersectionNew(predicate rds.Predicate, joinTableData *table_data.JoinTableData) [][][]int32 {
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

func getTableRowIdsByJoinTable(joinTableRows [][]int32) [][]int32 { // 输出: tableIndex -> rowIds
	if len(joinTableRows) < 1 {
		logger.Warnf("[getTableRowIdsByJoinTable] joinTable length is 0")
		return [][]int32{}
	}

	tableLen := len(joinTableRows)
	tableCount := len(joinTableRows[0])
	tablesRowIds := make([][]int32, tableCount)
	for tableIndex := 0; tableIndex < tableCount; tableIndex++ {
		tablesRowIds[tableIndex] = make([]int32, 0, tableLen)
	}

	// 遍历大宽表的每一行
	for _, joinRow := range joinTableRows {
		for tableIndex, tableRowId := range joinRow {
			tablesRowIds[tableIndex] = append(tablesRowIds[tableIndex], tableRowId)
		}
	}
	return tablesRowIds
}

func (hj *HashJoin) CalcRuleNew(taskId int64, lhs []rds.Predicate, rhs rds.Predicate, chanSize int) (rowSize int, xSupp int, xySupp int, xIntersection [][][]int32) {
	rowSize = -1
	hj.getGraphs(lhs, rhs)
	// 获取大宽表
	joinTables := hj.getJoinTables(taskId, chanSize)

	// 4.获取表内谓词
	var xPredicates []rds.Predicate
	var yPredicate rds.Predicate
	xPredicates, yPredicate = transferSelfPredicate(hj.selfGraph, rhs, len(joinTables))
	// 加载JoinTable的数据
	//joinTableData := table_data.NewJoinTableData(xPredicates, yPredicate, joinTables, taskId)
	joinTableData := table_data.NewJoinTableDataNew(xPredicates, yPredicate, joinTables, taskId)

	// 计算大宽表交集
	var usedJoinTableIndex []int
	for _, predicate := range xPredicates {
		lJoinTableIndex, rJoinTableIndex := utils.GetPredicateColumnIndexNew(predicate)
		usedJoinTableIndex = append(usedJoinTableIndex, lJoinTableIndex)
		usedJoinTableIndex = append(usedJoinTableIndex, rJoinTableIndex)
	}
	usedJoinTableIndex = utils2.Distinct(usedJoinTableIndex)

	// todo 获取rowSize的方式需要优化
	var intersection [][][]int32
	if len(xPredicates) == 0 {
		// todo 目前不会走到这个分支，这个地方再想想
		xSupp = len(joinTables[0])
		if rowSize == -1 {
			rowSize = xSupp
			rowSize *= rowSize
		}
	} else {
		if len(joinTables) == 1 {
			rowSize = len(joinTables[0])
			if hj.isDuplicate {
				rowSize *= rowSize
			}
		}
		intersection = hj.calcJoinTableIntersectionNew(intersection, joinTableData, xPredicates[0], chanSize)
		if len(xPredicates) == 1 {
			xSupp = CalIntersectionSupport(intersection, usedJoinTableIndex, chanSize)
			if rowSize == -1 {
				rowSize = xSupp
				if hj.isDuplicate {
					rowSize *= rowSize
				}
			}
		} else {
			if rowSize == -1 {
				rowSize = CalIntersectionSupport(intersection, usedJoinTableIndex, chanSize)
				if hj.isDuplicate {
					rowSize *= rowSize
				}
			}
			for _, predicate := range xPredicates[1:] {
				//intersection = hj.calcJoinTableIntersection(intersection, joinTableData, predicate, chanSize)
				intersection = hj.calcJoinTableIntersectionNew(intersection, joinTableData, predicate, chanSize)
			}
			xSupp = CalIntersectionSupport(intersection, usedJoinTableIndex, chanSize)
			xIntersection = intersection
		}
	}

	//intersection = hj.calcJoinTableIntersection(intersection, joinTableData, yPredicate, chanSize)
	intersection = hj.calcJoinTableIntersectionNew(intersection, joinTableData, yPredicate, chanSize)
	xySupp = CalIntersectionSupport(intersection, usedJoinTableIndex, chanSize)

	if rowSize == -1 {
		rowSize = 0
	}
	return rowSize, xSupp, xySupp, intersection
}
