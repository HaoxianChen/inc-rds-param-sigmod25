package rpc_universal_call

import (
	"encoding/binary"
	"encoding/json"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables/table_data"
	"gitlab.grandhoo.com/rock/rock_v3/intersection"
	"gitlab.grandhoo.com/rock/rock_v3/rds_config"
	"gitlab.grandhoo.com/rock/rock_v3/request"
	"gitlab.grandhoo.com/rock/rock_v3/request/udf"
	"gitlab.grandhoo.com/rock/rock_v3/topk"
	"gitlab.grandhoo.com/rock/rock_v3/utils"
	"gitlab.grandhoo.com/rock/rock_v3/utils/blocking/udf_column"
	"gitlab.grandhoo.com/rock/rock_v3/utils/db_util"
	"gitlab.grandhoo.com/rock/rock_v3/utils/storage_utils"
	"gitlab.grandhoo.com/rock/rock_v3/utils/train_data_util"
	utils2 "gitlab.grandhoo.com/rock/storage/storage2/utils"
	"runtime"
	"runtime/debug"
	"strconv"
	"time"
)

/**
万能 RPC 等代码稳定后再拆分
*/

type MethodCode = int32

const (
	GetDataLength MethodCode = iota + 1 // 获取请求体数据大小，测试用
	LocalRuleFind
	LoadData
	DropTaskData
	CalSupp
	LocalRuleFindNew
	DecisionTree
)

var Router map[MethodCode]func([]byte) ([]byte, error)

func Register(kind MethodCode, logic func([]byte) ([]byte, error)) {
	Router[kind] = logic
}

func init() {
	Router = map[MethodCode]func([]byte) ([]byte, error){}

	Register(GetDataLength, func(bytes []byte) ([]byte, error) {
		return binary.BigEndian.AppendUint64(nil, uint64(len(bytes))), nil
	})
	Register(LocalRuleFind, func(bytes []byte) ([]byte, error) {
		type request struct {
			Lhs            []rds.Predicate
			Rhs            rds.Predicate
			TableId2index  map[string]int
			AbandonedRules []string
			CR             float64
			FTR            float64
			TaskId         int64
			TableId        string
			ChanSize       int
		}
		type response struct {
			HasRule, Prune, IsDelete bool
			RowSize, XSupp, XySupp   int
		}
		var req request

		_ = json.Unmarshal(bytes, &req)
		logger.Infof("cal lhs:%v, rhs:%v", utils.GetLhsStr(req.Lhs), req.Rhs.PredicateStr)
		startTime := time.Now().UnixMilli()
		var rowSize, xSupp, xySupp int
		//if len(req.TableId2index) == 1 {
		//	tableName, rowSize, columnsType, _ := storage_utils.GetSchemaInfo(req.TableId)
		//	gv := global_variables.InitGlobalV(req.TaskId)
		//	gv.ColumnsType = columnsType
		//	gv.TableName = tableName
		//	gv.TaskId = req.TaskId
		//	gv.TableId = req.TableId
		//	gv.RowSize = rowSize
		//	gv.CR = req.CR
		//	gv.FTR = req.FTR
		//	hasRule, prune, isDelete, _ := trees.ShouldPrune1(&global_variables.TaskTree{
		//		Rhs: req.Rhs,
		//		Lhs: req.Lhs,
		//	}, rowSize, gv, false, nil)
		//	res := &response{
		//		HasRule:  hasRule,
		//		Prune:    prune,
		//		IsDelete: isDelete,
		//	}
		//	ret, _ := json.Marshal(res)
		//	return ret, nil
		//} else {
		//	rowSize, xSupp, xySupp = storage_utils.CalculateRule(req.Lhs, req.Rhs)
		//}
		rowSize, xSupp, xySupp = intersection.CalRuleNew2(req.Lhs, req.Rhs, req.TaskId, req.ChanSize)
		logger.Infof("finish cal lhs:%v, rhs:%v rowSize:%v, xSupp:%v, xySupp:%v, spent time:%vms", utils.GetLhsStr(req.Lhs), req.Rhs.PredicateStr, rowSize, xSupp, xySupp, time.Now().UnixMilli()-startTime)

		// rowSize, xSupp, xySupp := storage_utils.CalculateRule(node.Lhs, node.Rhs)
		var support, confidence float64 = 0, 0
		if rowSize > 0 {
			support = float64(xySupp) / float64(rowSize)
		}
		if xSupp > 0 {
			confidence = float64(xySupp) / float64(xSupp)
		}

		var hasRule, prune, isDelete, isAbandoned bool

		hasRule = req.CR <= support && req.FTR <= confidence
		prune = hasRule || req.CR > support
		isDelete = prune

		//if hasRule {
		//	px := utils.GetLhsStr(req.Lhs)
		//	ree := px + "->" + req.Rhs.PredicateStr
		//	ree = utils.GenerateMultiTableIdStr(req.TableId2index) + " ^ " + ree
		//
		//	if utils.IsAbandoned(ree, req.AbandonedRules) {
		//		logger.Infof("规则:%v 曾经被废弃过,不再进行后续操作", ree)
		//		isAbandoned = true
		//	} else {
		//		ruleType := 1
		//
		//		rule := rds.Rule{
		//			TableId:       req.TableId,
		//			Ree:           ree,
		//			LhsPredicates: req.Lhs,
		//			LhsColumns:    utils.GetPredicatesColumn(req.Lhs),
		//			Rhs:           req.Rhs,
		//			RhsColumn:     req.Rhs.LeftColumn,
		//			CR:            support,
		//			FTR:           confidence,
		//			RuleType:      ruleType,
		//			XSupp:         xSupp,
		//			XySupp:        xySupp,
		//			XSatisfyCount: 0,
		//			XSatisfyRows:  nil,
		//			XIntersection: nil,
		//		}
		//
		//		logger.Debugf("%v find rule: %v, rowSize: %v, xySupp: %v, xSupp: %v", req.TaskId, ree, rowSize, xySupp, xSupp)
		//		//storage_utils.SaveRule(rule, gv)
		//		db_util.WriteRule2DB(rule, req.TaskId, 0, 0)
		//	}
		//}

		if isAbandoned {
			hasRule = false
			prune = true
			isDelete = true
		}

		res := &response{
			HasRule:  hasRule,
			Prune:    prune,
			IsDelete: isDelete,
			RowSize:  rowSize,
			XSupp:    xSupp,
			XySupp:   xySupp,
		}
		ret, _ := json.Marshal(res)
		return ret, nil
	})

	Register(LoadData, func(bytes []byte) ([]byte, error) {
		type request struct {
			TaskId     int64
			TableIds   []string
			UDFTabCols []udf.UDFTabCol
			Limit      int
		}
		type response struct {
			Msg string
		}
		var req request
		_ = json.Unmarshal(bytes, &req)
		logger.Infof("预热数据 %v", req.TableIds)
		start := time.Now()
		table_data.LoadSchema(req.TaskId, req.TableIds)
		table_data.SetBlockingInfo(req.TaskId, req.UDFTabCols)
		table_data.LoadDataCreatePli(req.TaskId, req.TableIds)
		if req.Limit > 0 {
			table_data.CreateSamplePLI(req.TaskId, req.Limit)
		}
		storage_utils.DropCacheDataBase()
		table_data.CreateIndex(req.TaskId)
		if req.Limit > 0 {
			table_data.CreateSampleIndex(req.TaskId, req.Limit)
		}
		//for _, tableId := range req.TableIds {
		//	length, err := storage_utils.TableLengthNotRegradingDeleted(tableId)
		//	if err != nil {
		//		logger.Error(err)
		//		continue
		//	}
		//	columnInfos, err := rock_db.DB.TableColumnInfos(tableId)
		//	if err != nil {
		//		logger.Error(err)
		//		continue
		//	}
		//	var columns = []string{row_id.ColumnName}
		//	for _, info := range columnInfos {
		//		columns = append(columns, info.ColumnName)
		//	}
		//	var batchSz int64 = config.ShardSize
		//	for offset := int64(0); true; offset += batchSz {
		//		if offset >= length {
		//			break
		//		}
		//		lines, err := rock_db.DB.Query(sql.SingleTable.SELECT(columns...).FROM(tableId).WHERE(
		//			condition.GreaterEq(row_id.ColumnName, offset), condition.Less(row_id.ColumnName, offset+batchSz)))
		//		if err != nil {
		//			logger.Error(err)
		//		} else {
		//			logger.Infof("预热表 %s offset %d 数据长度 %d", tableId, offset, len(lines))
		//		}
		//	}
		//
		//}
		logger.Infof("预热完成,耗时 %s", utils2.DurationString(start))
		var res = &response{Msg: "ok"}
		ret, _ := json.Marshal(res)
		return ret, nil
	})

	Register(DropTaskData, func(bytes []byte) ([]byte, error) {
		type request struct {
			TaskId int64
		}
		type response struct {
			Msg string
		}
		var req request
		_ = json.Unmarshal(bytes, &req)
		logger.Infof("清除任务内存 %v", req.TaskId)
		table_data.DropTaskId(req.TaskId)
		runtime.GC()
		debug.FreeOSMemory()
		var res = &response{Msg: "ok"}
		ret, _ := json.Marshal(res)
		return ret, nil
	})

	Register(CalSupp, func(bytes []byte) ([]byte, error) {
		type request struct {
			Lhs        []rds.Predicate
			Rhs        rds.Predicate
			TaskId     int64
			ChanSize   int
			ValueIndex interface{}
			RuleId     int
			IndexId    int
		}
		type response struct {
			XSupp, XySupp int
		}
		var req request

		_ = json.Unmarshal(bytes, &req)
		//logger.Infof("cal lhs:%v, rhs:%v, indexId:%v", utils.GetLhsStr(req.Lhs), req.Rhs.PredicateStr, req.IndexId)
		//startTime := time.Now().UnixMilli()
		xSupp, xySupp := intersection.CalSupp(req.Lhs, req.Rhs, req.TaskId, req.ChanSize, req.ValueIndex)
		//logger.Infof("finish cal lhs:%v, rhs:%v, indexId:%v, xSupp:%v, xySupp:%v, spent time:%vms", utils.GetLhsStr(req.Lhs), req.Rhs.PredicateStr, req.IndexId, xSupp, xySupp, time.Now().UnixMilli()-startTime)

		res := &response{
			XSupp:  xSupp,
			XySupp: xySupp,
		}
		ret, _ := json.Marshal(res)
		return ret, nil
	})

	Register(LocalRuleFindNew, func(bytes []byte) ([]byte, error) {
		type request struct {
			Lhs               []rds.Predicate
			Rhs               rds.Predicate
			TableId2index     map[string]int
			AbandonedRules    []string
			CR                float64
			FTR               float64
			TaskId            int64
			TableId           string
			ChanSize          int
			calGiniFlag       bool
			LhsCandidate      []rds.Predicate
			LhsCrossCandidate []rds.Predicate
		}
		type response struct {
			RuleSize                   int
			Prune                      bool
			IsDelete                   bool
			LhsCandidateGiniIndex      []float32
			LhsCrossCandidateGiniIndex []float32
		}
		var req request

		_ = json.Unmarshal(bytes, &req)
		logger.Infof("cal lhs:%v, rhs:%v", utils.GetLhsStr(req.Lhs), req.Rhs.PredicateStr)
		startTime := time.Now().UnixMilli()
		var rowSize, xSupp, xySupp int
		var xIntersection [][][]int32
		//if len(req.TableId2index) == 1 {
		//	tableName, rowSize, columnsType, _ := storage_utils.GetSchemaInfo(req.TableId)
		//	gv := global_variables.InitGlobalV(req.TaskId)
		//	gv.ColumnsType = columnsType
		//	gv.TableName = tableName
		//	gv.TaskId = req.TaskId
		//	gv.TableId = req.TableId
		//	gv.RowSize = rowSize
		//	gv.CR = req.CR
		//	gv.FTR = req.FTR
		//	hasRule, prune, isDelete, _ := trees.ShouldPrune1(&global_variables.TaskTree{
		//		Rhs: req.Rhs,
		//		Lhs: req.Lhs,
		//	}, rowSize, gv, false, nil)
		//	res := &response{
		//		HasRule:  hasRule,
		//		Prune:    prune,
		//		IsDelete: isDelete,
		//	}
		//	ret, _ := json.Marshal(res)
		//	return ret, nil
		//} else {
		//	rowSize, xSupp, xySupp = storage_utils.CalculateRule(req.Lhs, req.Rhs)
		//}
		rowSize, xSupp, xySupp, xIntersection = intersection.CalRuleNew3(req.Lhs, req.Rhs, req.TaskId, req.ChanSize)
		logger.Infof("finish cal lhs:%v, rhs:%v rowSize:%v, xSupp:%v, xySupp:%v, spent time:%vms", utils.GetLhsStr(req.Lhs), req.Rhs.PredicateStr, rowSize, xSupp, xySupp, time.Now().UnixMilli()-startTime)

		// rowSize, xSupp, xySupp := storage_utils.CalculateRule(node.Lhs, node.Rhs)
		var support, confidence float64 = 0, 0
		if rowSize > 0 {
			support = float64(xySupp) / float64(rowSize)
		}
		if xSupp > 0 {
			confidence = float64(xySupp) / float64(xSupp)
		}

		var hasRule, prune, isDelete, isAbandoned bool

		hasRule = req.CR <= support && req.FTR <= confidence
		prune = hasRule || req.CR > support
		isDelete = hasRule

		if hasRule {
			px := utils.GetLhsStr(req.Lhs)
			ree := px + "->" + req.Rhs.PredicateStr
			ree = utils.GenerateMultiTableIdStr(req.TableId2index) + " ^ " + ree

			if utils.IsAbandoned(ree, req.AbandonedRules) {
				logger.Infof("规则:%v 曾经被废弃过,不再进行后续操作", ree)
				isAbandoned = true
			} else {
				ruleType := 1

				rule := rds.Rule{
					TableId:       req.TableId,
					Ree:           ree,
					LhsPredicates: req.Lhs,
					LhsColumns:    utils.GetPredicatesColumn(req.Lhs),
					Rhs:           req.Rhs,
					RhsColumn:     req.Rhs.LeftColumn,
					CR:            support,
					FTR:           confidence,
					RuleType:      ruleType,
					XSupp:         xSupp,
					XySupp:        xySupp,
					XSatisfyCount: 0,
					XSatisfyRows:  nil,
					XIntersection: nil,
				}

				logger.Debugf("%v find rule: %v, rowSize: %v, xySupp: %v, xSupp: %v", req.TaskId, ree, rowSize, xySupp, xSupp)
				//storage_utils.SaveRule(rule, gv)
				db_util.WriteRule2DB(rule, req.TaskId, 0, 0)
			}
		}

		if isAbandoned {
			hasRule = false
			prune = true
			isDelete = true
		}

		var lhsCandidateGiniIndex, lhsCrossCandidateGiniIndex = make([]float32, len(req.LhsCandidate)), make([]float32, len(req.LhsCrossCandidate))
		ruleSize := 0

		res := &response{
			RuleSize:                   ruleSize,
			Prune:                      prune,
			IsDelete:                   isDelete,
			LhsCandidateGiniIndex:      lhsCandidateGiniIndex,
			LhsCrossCandidateGiniIndex: lhsCrossCandidateGiniIndex,
		}

		if hasRule {
			ruleSize = 1
			res.RuleSize = ruleSize
			ret, _ := json.Marshal(res)
			return ret, nil
		} else if !prune {
			predicates := utils.SortPredicatesRelated(req.Lhs)
			tableIdMap := map[int]string{}
			maxTid := 0
			var fkPredicate []rds.Predicate
			var innerPredicate []rds.Predicate
			for _, p := range predicates {
				if storage_utils.ForeignKeyPredicate(&p) {
					fkPredicate = append(fkPredicate, p)
				}
				if p.PredicateType == 1 {
					innerPredicate = append(innerPredicate, p)
				}
				leftTableId := p.LeftColumn.TableId
				rightTableId := p.RightColumn.TableId
				lTid, rTid := utils.GetPredicateColumnIndexNew(p)
				tableIdMap[lTid] = leftTableId
				tableIdMap[rTid] = rightTableId
				maxTid = utils2.Max(maxTid, lTid)
				maxTid = utils2.Max(maxTid, rTid)
			}
			tableIds := make([]string, maxTid+1)
			for tid, table := range tableIdMap {
				tableIds[tid] = table
			}

			tableId2index := generateTableId2tableIndex(req.Lhs)
			isSingleTable := len(tableId2index) == 1

			index2table, tableArr := generateIndex2Table(append(req.Lhs, req.Rhs))

			satisfyData := intersection.BuildIdTableFromIntersection(xIntersection, tableIds, req.TaskId, isSingleTable)

			if len(satisfyData) < 1 {
				ret, _ := json.Marshal(res)
				return ret, nil
			}

			filterRatio := make(map[string]float64)
			for index := range index2table {
				filterRatio[index] = 1
			}

			task := table_data.GetTask(req.TaskId)

			tableColumnType := removeUdfColumn(task.TableColumnTypes)

			columns, trainData, rhsValue2index, columnType := train_data_util.GenerateTrainDataParallel(satisfyData, tableArr, index2table, tableColumnType, true, false, req.Rhs, filterRatio, req.ChanSize, rds_config.DecisionTreeMaxRowSize)

			rules, _, _, _ := decision_tree.DecisionTreeWithDataInput(req.Lhs, req.Rhs, columns, trainData, columnType, index2table, rhsValue2index, req.TaskId, req.CR, req.FTR, 3, false)

			for _, rule := range rules {
				// 因为现在规则发现涉及到多表，所以tableId字段先随便填一个吧
				db_util.WriteRule2DB(rule, req.TaskId, 0, 0)
			}

			if req.calGiniFlag {
				utils.CheckPredicatesIndex(req.LhsCrossCandidate, tableId2index)
				utils.CheckPredicatesIndex(req.LhsCandidate, tableId2index)
				res.LhsCandidateGiniIndex = topk.CalGiniIndexes(req.LhsCandidate, req.Rhs, trainData, columns, req.TableId2index)
				res.LhsCrossCandidateGiniIndex = topk.CalGiniIndexes(req.LhsCrossCandidate, req.Rhs, trainData, columns, req.TableId2index)
			}

			res.RuleSize = len(rules)
		}

		ret, _ := json.Marshal(res)
		return ret, nil
	})

	Register(DecisionTree, func(bytes []byte) ([]byte, error) {
		var req request.DecisionTreeTaskReq
		_ = json.Unmarshal(bytes, &req)
		type response struct {
			RuleSize int
		}
		//_, ruleSize := rule_dig.ExecuteDTTask(req)
		ruleSize := 0
		res := &response{
			RuleSize: ruleSize,
		}
		ret, _ := json.Marshal(res)
		return ret, nil
	})
}

func generateTableId2tableIndex(predicates []rds.Predicate) map[string]int {
	result := make(map[string]int)
	for _, predicate := range predicates {
		leftIndex, rightIndex := utils.GetPredicateColumnIndexNew(predicate)
		result[predicate.LeftColumn.TableId] = leftIndex / 2
		result[predicate.RightColumn.TableId] = rightIndex / 2
	}
	return result
}

func generateIndex2Table(predicates []rds.Predicate) (map[string]string, []string) {
	index2Table := make(map[string]string)
	tableMap := make(map[string]bool)
	for _, predicate := range predicates {
		//arr := strings.Split(predicate.PredicateStr, predicate.SymbolType)
		//leftIndex := strings.Split(arr[0], ".")[0]
		//rightIndex := strings.Split(arr[1], ".")[0]
		leftIndex := "t" + strconv.Itoa(predicate.LeftColumn.ColumnIndex)
		rightIndex := "t" + strconv.Itoa(predicate.RightColumn.ColumnIndex)
		leftTableId := predicate.LeftColumn.TableId
		rightTableId := predicate.RightColumn.TableId
		index2Table[leftIndex] = leftTableId
		index2Table[rightIndex] = rightTableId
		tableMap[leftTableId] = true
		tableMap[rightTableId] = true
	}
	tableArr := make([]string, len(tableMap))
	i := 0
	for tableId := range tableMap {
		tableArr[i] = tableId
		i++
	}
	return index2Table, tableArr
}

func removeUdfColumn(tableColumnType map[string]map[string]string) map[string]map[string]string {
	for _, columnType := range tableColumnType {
		var deleting []string
		for columnName := range columnType {
			if udf_column.IsUdfColumn(columnName) {
				deleting = append(deleting, columnName)
			}
		}
		for _, columnName := range deleting {
			delete(columnType, columnName)
		}
	}
	return tableColumnType
}
