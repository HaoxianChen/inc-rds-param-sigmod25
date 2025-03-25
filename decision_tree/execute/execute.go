package decision_tree

import (
	"encoding/json"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables/table_data"
	"math"
	"sort"

	"gitlab.grandhoo.com/rock/rock_v3/rds_config"

	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/distribute"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock-share/rpc/grpc/pb"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree"
	"gitlab.grandhoo.com/rock/rock_v3/request"
	"gitlab.grandhoo.com/rock/rock_v3/utils/db_util"
	"gitlab.grandhoo.com/rock/rock_v3/utils/storage_utils"
	"gitlab.grandhoo.com/rock/rock_v3/utils/train_data_util"
)

func ExecuteSingleRowDecisionTree(singleRowDecisionTreeReq *request.SingleRowDecisionTreeReq) *request.SingleRowDecisionTreeResp {
	logger.Infof("ExecuteSingleRowDecisionTree, taskId: %v, tableId: %s , column :%s", singleRowDecisionTreeReq.TaskId, singleRowDecisionTreeReq.TableName, singleRowDecisionTreeReq.YColumn)

	//gv := global_variables.InitGlobalV(singleRowDecisionTreeReq.TaskId)
	//gv.TaskId = singleRowDecisionTreeReq.TaskId
	//gv.CR = singleRowDecisionTreeReq.CR
	//gv.FTR = singleRowDecisionTreeReq.FTR
	//gv.StopTask = singleRowDecisionTreeReq.StopTask
	//go func() {
	//	defer func() {
	//		if err := recover(); err != nil {
	//			gv.HasError = true
	//			s := string(debug.Stack())
	//			logger.Error("recover.err:%v, stack:%v", err, s)
	//		}
	//	}()
	//	rule_dig.WritePg(gv, true)
	//}()

	//取抽样数据，生成训练数据
	tableName := singleRowDecisionTreeReq.TableName
	columnType := singleRowDecisionTreeReq.ColumnType
	sampleRatio := float64(1)
	sampleThreshold2Ratio := singleRowDecisionTreeReq.SampleThreshold2Ratio

	//单行决策树抽样比例有传参时使用接口参数的map，没有时使用配置DecisionTreeMaxRowSize
	//若DecisionTreeMaxRowSize也为0时使用DecisionTreeSampleThreshold2Ratio
	//DecisionTreeSampleThreshold2Ratio为空时ratio为1
	limit := 0
	if len(sampleThreshold2Ratio) == 0 {
		limit = singleRowDecisionTreeReq.DecisionTreeMaxRowSize
		sampleThreshold2Ratio = rds_config.DecisionTreeSampleThreshold2Ratio
	}

	sortThreshold := make([]int, 0)
	for threshold := range sampleThreshold2Ratio {
		sortThreshold = append(sortThreshold, threshold)
	}
	sort.Slice(sortThreshold, func(i, j int) bool {
		return sortThreshold[i] > sortThreshold[j]
	})
	for _, threshold := range sortThreshold {
		if singleRowDecisionTreeReq.TableLength > threshold {
			sampleRatio = sampleThreshold2Ratio[threshold]
			break
		}
	}
	logger.Infof("ExecuteSingleRowDecisionTree, taskId: %v, tableId: %s, tableLength: %v, sampleRatio: %v", singleRowDecisionTreeReq.TaskId, singleRowDecisionTreeReq.TableName, singleRowDecisionTreeReq.TableLength, sampleRatio)
	calcNodeIds, err := distribute.GEtcd.GetCalcNodeEndpoint(distribute.TRANSPORT_TYPE_GRPC)
	sampleData := make(map[string][]interface{})
	client, err := distribute.NewClient(func(ch chan any) {
		for sampleDataBytesFromNode := range ch {
			if sampleDataBytesFromNode == nil {
				break
			}
			sampleDataFromNode := sampleDataBytesFromNode.(*request.GetTableSampleDataResp)
			if len(sampleData) == 0 {
				sampleData = sampleDataFromNode.SampleData
			} else {
				for column, data := range sampleData {
					newData := sampleDataFromNode.SampleData[column]
					data = append(data, newData...)
					sampleData[column] = data
				}
			}
		}
	}, distribute.WithEndpoints(calcNodeIds))
	if err != nil {
		logger.Error("new stream err=", err)
		return &request.SingleRowDecisionTreeResp{Success: false}
	}

	reqBytes, _ := json.Marshal(&request.GetTableSampleDataReq{
		TaskId:     singleRowDecisionTreeReq.TaskId,
		TableName:  tableName,
		ColumnType: columnType,
		Limit:      limit,
		Ratio:      sampleRatio,
	})
	req := &pb.GetTableSampleDataReq{
		Req: reqBytes,
	}
	resp := &pb.ServerStreamResp{}
	client.RoundRobin(pb.Rock_GetTableSampleData_FullMethodName, req, &resp, func() {
		response := &request.GetTableSampleDataResp{}
		err := json.Unmarshal(resp.Resp, response)
		if err != nil {
			logger.Error("json.Unmarshal failed, err=", err, string(resp.Resp))
			return
		}
		client.PushPostProcChannel(response)
		return
	})
	//现在得用单播,因为所有节点有全部数据,广播会取重复
	//stream.Broadcast(pb.Rock_GetTableSampleData_FullMethodName, req, resp, func(eachResp any) {
	//	response := &request.GetTableSampleDataResp{}
	//	response2 := eachResp.(*pb.GetTableSampleDataResp)
	//
	//	err := json.Unmarshal(response2.SampleData, response)
	//	if err != nil {
	//		logger.Error("json.Unmarshal failed, err=", err, string(resp.SampleData))
	//		return
	//	}
	//	stream.PushPostProcChannel(response)
	//	return
	//})
	client.Close()

	//直接当前节点取数据
	//limitFloat := math.Ceil(sampleRatio * float64(singleRowDecisionTreeReq.TableLength))
	//limit := int64(limitFloat)
	//sampleData := storage_utils.GetTableValuesByColsWithLimit(tableName, columnType, 0, limit)

	trainDataInput := make(map[string]map[string][]interface{})
	trainDataInput["t0"] = sampleData

	index2Table := make(map[string]string)
	index2Table["t0"] = tableName

	dataType := make(map[string]map[string]string)
	dataType[singleRowDecisionTreeReq.TableName] = columnType

	ratioMap := make(map[string]float64)
	ratioMap["t0"] = float64(1)
	columns, decisionTreeInput, yValue2Num, trainDataColumnType := train_data_util.GenerateTrainData([]map[string]map[string][]interface{}{trainDataInput}, []string{tableName}, index2Table, dataType, true, false, rds.Predicate{
		LeftColumn: rds.Column{
			TableId:    tableName,
			TableAlias: "t0",
			ColumnId:   singleRowDecisionTreeReq.YColumn,
			ColumnType: columnType[singleRowDecisionTreeReq.YColumn],
		},
		PredicateType: 0,
	}, ratioMap)
	trainDataLength := 0
	if len(columns) > 0 {
		trainDataLength = len(decisionTreeInput)
	}
	logger.Infof("get train data, taskId: %v, y: %s, trainTableLength: %v", singleRowDecisionTreeReq.TaskId, singleRowDecisionTreeReq.YColumn, trainDataLength)
	//3.决策树
	rules, xSupports, xySupports, err := decision_tree.DecisionTreeWithDataInput(nil, rds.Predicate{}, columns, decisionTreeInput,
		trainDataColumnType, index2Table, yValue2Num, singleRowDecisionTreeReq.TaskId, singleRowDecisionTreeReq.CR, singleRowDecisionTreeReq.FTR, singleRowDecisionTreeReq.MaxTreeDepth, singleRowDecisionTreeReq.StopTask)
	//规则入库
	for i, rule := range rules {
		logger.Debugf("taskId: %v, y:%s, find rule: %v, rowSize: %v, xSupp: %v, xySupp: %v", singleRowDecisionTreeReq.TaskId, singleRowDecisionTreeReq.YColumn, rule.Ree, len(decisionTreeInput), xSupports[i], xySupports[i])
		db_util.WriteRule2DB(rule, singleRowDecisionTreeReq.TaskId, 0, 0)
		//storage_utils.SaveRule(rules[i], gv)
	}
	if err != nil {
		logger.Errorf("execute decision tree error", err)
	}

	return &request.SingleRowDecisionTreeResp{RuleSize: len(rules), Success: true}
}

func GetTableSampleData(req *request.GetTableSampleDataReq) *request.GetTableSampleDataResp {
	logger.Infof("Decision Tree, get table sample data, tableId: %s, limit: %v, sampleRatio: %v", req.TableName, req.Limit, req.Ratio)
	_, length, _, _ := storage_utils.GetSchemaInfo(req.TableName) //当前节点该表数据行数, 目前是全表
	limit := req.Limit
	if limit != 0 {
		if limit == -1 || limit > length {
			limit = length
		}
	} else {
		ratio := req.Ratio
		limitFloat := ratio * float64(length)
		limit = int(math.Ceil(limitFloat))
	}
	//offset := 0
	//sampleData := storage_utils.GetTableValuesByColsWithLimit(req.TableName, req.ColumnType, int64(offset), int64(limit))
	task := table_data.GetTask(req.TaskId)
	sampleData := map[string][]interface{}{}
	for columnName := range req.ColumnType {
		sampleData[columnName] = task.TableValues[req.TableName][columnName][:limit]
	}
	return &request.GetTableSampleDataResp{SampleData: sampleData}
}
