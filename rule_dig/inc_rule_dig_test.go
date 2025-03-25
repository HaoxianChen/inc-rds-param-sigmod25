package rule_dig

import (
	"fmt"
	"gitlab.grandhoo.com/rock/rock-share/base/config"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/global/db"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables/table_data"
	"gitlab.grandhoo.com/rock/rock_v3/inc_rule_dig"
	"gitlab.grandhoo.com/rock/rock_v3/inc_rule_dig/dao"
	"gitlab.grandhoo.com/rock/rock_v3/intersection"
	"gitlab.grandhoo.com/rock/rock_v3/request"
	"gitlab.grandhoo.com/rock/rock_v3/request/udf"
	"gitlab.grandhoo.com/rock/rock_v3/utils"
	"gitlab.grandhoo.com/rock/rock_v3/utils/storage_utils"
	"gitlab.grandhoo.com/rock/storage/config/storage_initiator"
	utils2 "gitlab.grandhoo.com/rock/storage/storage2/utils"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func Test_IncRuleGroupByRhs(t *testing.T) {
	config.InitConfig1()
	all := config.All
	l := all.Logger
	s := all.Server
	logger.InitLogger(l.Level, "rock", l.Path, l.MaxAge, l.RotationTime, l.RotationSize, s.SentryDsn)
	db.InitGorm()

	prePrunedIncRules, err := inc_rule_dig.GetPrunedIncRulesByTaskId(1000017)
	if err != nil {
		logger.Errorf("[IncMinerSupport] GetPruneTaskTreesByTaskId error:%v, taskId:%v", err, 1000017)
		return
	}
	logger.Infof("pruneRuleSize:%v", len(prePrunedIncRules))

	startTime := time.Now().UnixMilli()
	rhsKey2IncRules, _ := incRuleGroupByRhs(prePrunedIncRules)
	useTime := time.Now().UnixMilli() - startTime
	logger.Infof("按RHS分组时间:%v", useTime)

	for _, incRules := range rhsKey2IncRules {
		layer2IncRules := incRuleGroupByLayer(incRules)
		for layer, rules := range layer2IncRules {
			for _, rule := range rules {
				ree := inc_rule_dig.GetIncRuleRee(rule)
				logger.Infof("layer:%v, ree:%v", layer, ree)
			}
		}
	}
}

func Test_getCurrentLayer(t *testing.T) {
	config.InitConfig1()
	all := config.All
	l := all.Logger
	s := all.Server
	logger.InitLogger(l.Level, "rock", l.Path, l.MaxAge, l.RotationTime, l.RotationSize, s.SentryDsn)
	db.InitGorm()

	prePrunedIncRules, err := inc_rule_dig.GetPrunedIncRulesByTaskId(1000017)
	if err != nil {
		logger.Errorf("[IncMinerSupport] GetPruneTaskTreesByTaskId error:%v, taskId:%v", err, 1000017)
		return
	}
	logger.Infof("pruneRuleSize:%v", len(prePrunedIncRules))

	minimalIncRules := make([]*inc_rule_dig.IncRule, 0)
	gv := global_variables.InitGlobalV(int64(10000))
	gv.CR = 0.001
	gv.FTR = 0.75

	rhsKey2IncRules, _ := incRuleGroupByRhs(prePrunedIncRules)

	startTime := time.Now().UnixMilli()
	for _, incRules := range rhsKey2IncRules {
		layer2IncRules := incRuleGroupByLayer(incRules)
		getCurrentLayer(layer2IncRules, gv, minimalIncRules)
	}
	useTime := time.Now().UnixMilli() - startTime
	logger.Infof("耗时:%v", useTime)
}

func getCurrentLayer(layer2IncRules map[int][]*inc_rule_dig.IncRule, gv *global_variables.GlobalV, minimalRules []*inc_rule_dig.IncRule) (layer2NeedExpandRules map[int][]*inc_rule_dig.IncRule) {
	layer2NeedExpandRules = make(map[int][]*inc_rule_dig.IncRule)
	for layer, incRules := range layer2IncRules {
		needExpandRules := make([]*inc_rule_dig.IncRule, 0, len(incRules))
		for _, incRule := range incRules {
			if incRule.CR >= gv.CR && incRule.FTR >= gv.FTR {
				minimalRules = append(minimalRules, incRule)
			} else {
				needExpandRules = append(needExpandRules, incRule)
			}
		}
		layer2NeedExpandRules[layer] = needExpandRules
	}
	return
}

func Test_getPrunedRee(t *testing.T) {
	config.InitConfig1()
	all := config.All
	l := all.Logger
	s := all.Server
	logger.InitLogger(l.Level, "rock", l.Path, l.MaxAge, l.RotationTime, l.RotationSize, s.SentryDsn)
	db.InitGorm()

	prePrunedIncRules, err := inc_rule_dig.GetPrunedIncRulesByTaskId(1000021)
	if err != nil {
		logger.Errorf("[IncMinerSupport] GetPruneTaskTreesByTaskId error:%v, taskId:%v", err, 1000021)
		return
	}

	for _, incRule := range prePrunedIncRules {
		ree := inc_rule_dig.GetIncRuleRee(incRule)
		logger.Infof("ree: %v", ree)
	}
}

func Test_incMinerConfidence(t *testing.T) {
	config.InitConfig1()
	all := config.All
	l := all.Logger
	s := all.Server
	logger.InitLogger(l.Level, "rock", l.Path, l.MaxAge, l.RotationTime, l.RotationSize, s.SentryDsn)
	db.InitGorm()

	minimalRules := make([]*inc_rule_dig.IncRule, 0)
	// 获取上一轮的采样节点(从pg)
	recallBound := 1.0
	preTaskId := int64(1000042)
	updatedConfidence := 0.9
	preSampleNodes, err := inc_rule_dig.GetSampleNodesByTaskId(preTaskId)
	if err != nil {
		logger.Errorf("[IncMinerConfidence] GetSampleNodesByTaskId error:%v, taskId:%v", err, preTaskId)
		return
	}
	logger.Infof("preSampleNodes size:%v", len(preSampleNodes))
	startTime := time.Now().UnixMilli()
	// TODO 补充sampleNodes排序功能(不排序也可以,后续再实现)
	var relaxSampleNodes []*inc_rule_dig.SampleNode
	//if recallBound >= 1 {
	//	relaxSampleNodes = preSampleNodes
	//} else {
	relaxSampleNodes = getRelaxSampleNodes(preSampleNodes, updatedConfidence, recallBound)
	//}
	logger.Infof("relaxSampleNodes size:%v", len(relaxSampleNodes))
	for _, sampleNode := range relaxSampleNodes {
		if sampleNode.MaxConf >= updatedConfidence {
			if sampleNode.CurrentNode.FTR >= updatedConfidence {
				minimalRules = append(minimalRules, sampleNode.CurrentNode)
			}
			for _, neighborNode := range sampleNode.NeighborNodes {
				if neighborNode.FTR >= updatedConfidence {
					minimalRules = append(minimalRules, neighborNode)
				}
			}
		}
	}
	totalTime := time.Now().UnixMilli() - startTime
	logger.Infof("minimalRules size:%v, 耗时:%v(ms)", len(minimalRules), totalTime)
}

func Test_getCurrentLayerNew(t *testing.T) {
	config.InitConfig1()
	all := config.All
	l := all.Logger
	s := all.Server
	logger.InitLogger(l.Level, "rock", l.Path, l.MaxAge, l.RotationTime, l.RotationSize, s.SentryDsn)
	db.InitGorm()

	prePrunedIncRules, err := inc_rule_dig.GetPrunedIncRulesByTaskId(1000088)
	if err != nil {
		logger.Errorf("[IncMinerSupport] GetPruneTaskTreesByTaskId error:%v, taskId:%v", err, 1000021)
		return
	}

	logger.Infof("prePrunedIncRules size:%v", len(prePrunedIncRules))
	gv := global_variables.InitGlobalV(1000)
	gv.CR = 0.000001
	gv.FTR = 0.75
	// 剪枝的taskTrees按rhs分组
	rhsKey2IncRules, _ := incRuleGroupByRhs(prePrunedIncRules)
	//for _, incRules := range rhsKey2IncRules {
	//	layer2IncRules := incRuleGroupByLayer(incRules)
	//	getCurrentLayerNew(layer2IncRules, gv)
	//}

	var wg sync.WaitGroup
	//mutex := sync.Mutex{}
	ch := make(chan struct{}, 10)
	for rhsKey, incRules := range rhsKey2IncRules {
		ch <- struct{}{}
		wg.Add(1)
		go func(rhsKey string, incRules []*inc_rule_dig.IncRule) {
			defer func() {
				wg.Done()
				<-ch
				if err := recover(); err != nil {
					s := string(debug.Stack())
					logger.Errorf("recover.err:%v, stack:\n%v", err, s)
				}
			}()
			// taskTrees按layer分层
			layer2IncRules := incRuleGroupByLayer(incRules)
			getCurrentLayerNew(layer2IncRules, gv)
		}(rhsKey, incRules)
	}
	wg.Wait()

}

func getCurrentLayerNew(layer2IncRules map[int][]*inc_rule_dig.IncRule, gv *global_variables.GlobalV) (layer2NeedExpandRules map[int][]*inc_rule_dig.IncRule, minimalRules []*inc_rule_dig.IncRule, prunedRules []*inc_rule_dig.IncRule) {
	layer2NeedExpandRules = make(map[int][]*inc_rule_dig.IncRule)
	needExpandSize := 0
	rhsStr := ""
	for layer, incRules := range layer2IncRules {
		rhsStr = incRules[0].Node.Rhs.PredicateStr
		needExpandRules := make([]*inc_rule_dig.IncRule, 0, len(incRules))
		for _, incRule := range incRules {
			if incRule.CR >= gv.CR && incRule.FTR >= gv.FTR {
				minimalRules = append(minimalRules, incRule)
			} else if incRule.CR < gv.CR {
				prunedRules = append(prunedRules, incRule)
			} else {
				needExpandRules = append(needExpandRules, incRule)
			}
		}
		needExpandSize += len(needExpandRules)
		layer2NeedExpandRules[layer] = needExpandRules
	}
	logger.Infof("[getCurrentLayer] rhs:%v, needExpandRules size:%v, minimalRules size:%v, prunedRules size:%v", rhsStr, needExpandSize, len(minimalRules), len(prunedRules))
	return
}

func Test_getNeedExpandRules(t *testing.T) {
	config.InitConfig1()
	all := config.All
	l := all.Logger
	s := all.Server
	logger.InitLogger(l.Level, "rock", l.Path, l.MaxAge, l.RotationTime, l.RotationSize, s.SentryDsn)
	db.InitGorm()

	needExpandDBRules, err := dao.GetNeedExpandIncRules(1000015, 0.000001, 0.75)
	if err != nil {
		logger.Error(err)
		return
	}
	logger.Infof("needExpandRules size:%v", len(needExpandDBRules))
	needExpandRules := make([]*inc_rule_dig.IncRule, len(needExpandDBRules))
	for i, rule := range needExpandDBRules {
		node := inc_rule_dig.GetNode(rule)
		if node == nil {
			continue
		}
		expandRule := &inc_rule_dig.IncRule{
			Node:     node,
			RuleType: rule.RuleType,
			CR:       rule.Support,
			FTR:      rule.Confidence,
		}
		needExpandRules[i] = expandRule
	}

	rhsKey2IncRules, _ := incRuleGroupByRhs(needExpandRules)
	for _, incRules := range rhsKey2IncRules {
		layer2IncRules := incRuleGroupByLayer(incRules)
		for layer, rules := range layer2IncRules {
			logger.Infof("layer:%v, rule size:%v", layer, len(rules))
		}
	}
}

func Test_getSampleNeighbors(t *testing.T) {
	config.InitConfig1()
	all := config.All
	l := all.Logger
	s := all.Server
	logger.InitLogger(l.Level, "rock", l.Path, l.MaxAge, l.RotationTime, l.RotationSize, s.SentryDsn)
	db.InitGorm()
	storage_initiator.InitStorage()
	source := request.DataSource{
		Database:     "postgres",
		Host:         "127.0.0.1",
		Port:         5432,
		DatabaseName: "xxxx",
		TableName:    "inc_rds.inspection",
		User:         "xxxx",
		Password:     "xxxx",
	}
	taskId := int64(1005260)
	//taskId1 := "100345"
	tableId := storage_utils.ImportTable(&source)
	tableIds := []string{tableId}

	localLoadData(taskId, tableIds)

	updatedSupport := 0.000001
	updatedConfidence := 0.95
	//recallBound := 1.0
	//preTaskId := int64(1000042)
	//preSampleNodes, err := inc_rule_dig.GetSampleNodesByTaskId(preTaskId)
	//if err != nil {
	//	logger.Errorf("[IncMinerConfidence] GetSampleNodesByTaskId error:%v, taskId:%v", err, preTaskId)
	//	return
	//}
	//logger.Infof("preSampleNodes size:%v", len(preSampleNodes))
	id := int64(10026258)
	sampleNode, err := inc_rule_dig.GetSampleNodesById(id)
	if err != nil {
		logger.Error(err)
		return
	}

	needExpandCount := 0
	//relaxSampleNodes := getRelaxSampleNodes(preSampleNodes, updatedConfidence, recallBound)
	//for _, sampleNode := range relaxSampleNodes {
	neighbors := getSampleNodeNeighbors(sampleNode)
	for _, neighbor := range neighbors {
		convertIncRules([]*inc_rule_dig.IncRule{neighbor}, tableId)
		support, confidence := localCalcRule(taskId, neighbor)
		logger.Infof("support:%v, confidence:%v", support, confidence)
		if support > updatedSupport && confidence < updatedConfidence {
			needExpandCount++
		}
	}
	logger.Infof("neighbors:%v, needExpand:%v, pg neighbors:%v", len(neighbors), needExpandCount, len(sampleNode.NeighborNodes))
	//}

}

func localLoadData(taskId int64, tableIds []string) {
	table_data.LoadSchema(taskId, tableIds)
	table_data.SetBlockingInfo(taskId, []udf.UDFTabCol{})
	table_data.LoadDataCreatePli(taskId, tableIds)
	storage_utils.DropCacheDataBase()
	table_data.CreateIndex(taskId)
}

func localCalcRule(taskId int64, rule *inc_rule_dig.IncRule) (support, confidence float64) {
	rowSize, xSupp, xySupp := intersection.CalRuleNew2(rule.Node.Lhs, rule.Node.Rhs, taskId, 16)
	//logger.Infof("finish cal lhs:%v, rhs:%v rowSize:%v, xSupp:%v, xySupp:%v, spent time:%vms", utils.GetLhsStr(req.Lhs), req.Rhs.PredicateStr, rowSize, xSupp, xySupp, time.Now().UnixMilli()-startTime)

	// rowSize, xSupp, xySupp := storage_utils.CalculateRule(node.Lhs, node.Rhs)
	if rowSize > 0 {
		support = float64(xySupp) / float64(rowSize)
	}
	if xSupp > 0 {
		confidence = float64(xySupp) / float64(xSupp)
	}
	return
}

func Test_recallRate(t *testing.T) {
	config.InitConfig1()
	all := config.All
	l := all.Logger
	s := all.Server
	logger.InitLogger(l.Level, "rock", l.Path, l.MaxAge, l.RotationTime, l.RotationSize, s.SentryDsn)
	db.InitGorm()

	incTaskId := int64(1001185)
	batchTaskId := int64(1001200)

	incMinimalRules, err := inc_rule_dig.GetMinimalIncRulesByTaskId(incTaskId)
	if err != nil {
		logger.Errorf("[getRecallRate] GetMinimalIncRulesByTaskId failed, error:%v, batchMinerTaskId:%v", err, incTaskId)
	}
	//getRecallRate(incMinimalRules, batchTaskId)
	getRecallRateNew(incMinimalRules, batchTaskId)
}

func Test_sampleNodes(t *testing.T) {
	config.InitConfig1()
	all := config.All
	l := all.Logger
	s := all.Server
	logger.InitLogger(l.Level, "rock", l.Path, l.MaxAge, l.RotationTime, l.RotationSize, s.SentryDsn)
	db.InitGorm()

	oldBatchTaskId := int64(1000231)
	preSampleNodes, err := inc_rule_dig.GetSampleNodesByTaskId(oldBatchTaskId)
	if err != nil {
		logger.Errorf("GetSampleNodesByTaskId failed, error:%v", err)
		return
	}

	for i, sample := range preSampleNodes {
		logger.Infof("sampleId:%v, parent:%v", i, inc_rule_dig.GetIncRulesRee(sample.PredecessorNodes))
	}
}

func Test_getMinimizeProcessRules(t *testing.T) {
	config.InitConfig1()
	all := config.All
	l := all.Logger
	s := all.Server
	logger.InitLogger(l.Level, "rock", l.Path, l.MaxAge, l.RotationTime, l.RotationSize, s.SentryDsn)
	db.InitGorm()

	taskId := int64(1002738)
	minimalRules, err := inc_rule_dig.GetMinimalIncRulesByTaskId(taskId)
	if err != nil {
		logger.Error(err)
		return
	}
	//incSaveRules := getMinimizeProcessRules(minimalRules)
	incSaveRules := getMinimizeProcessRulesNew(minimalRules)
	for _, rule := range incSaveRules {
		logger.Infof("inc save ree:%v", inc_rule_dig.GetIncRuleRee(rule))
	}

	//batchTaskId := int64(1000230)
	//batchRules, err := inc_rule_dig.GetMinimalIncRulesByTaskId(batchTaskId)
	//if err != nil {
	//	logger.Error(err)
	//	return
	//}
	//batchSaveRules := getMinimizeProcessRules(batchRules)
	//for _, rule := range batchSaveRules {
	//	logger.Infof("batch save ree:%v", inc_rule_dig.GetIncRuleRee(rule))
	//}
	//
	//getRecallRateNew1(incSaveRules, batchSaveRules)
}

func getRecallRateNew1(incMinimalRules []*inc_rule_dig.IncRule, batchMinimalRules []*inc_rule_dig.IncRule) float64 {
	logger.Infof("[getRecallRateNew] incREEs size:%v, batchREEs size:%v", len(incMinimalRules), len(batchMinimalRules))

	incKey2Rules := incRuleGroupByRhsNew(incMinimalRules)
	batchKey2Rules := incRuleGroupByRhsNew(batchMinimalRules)

	intersectCount := 0
	for batchKey, batchRules := range batchKey2Rules {
		incRules := incKey2Rules[batchKey]
		//incLhsStrs := getRulesSortedLhsStr(incRules)
		for _, batchRule := range batchRules {
			//batchLhsStr := getSortedLhsStr(batchRule)
			isCover := false
			//for _, incLhsStr := range incLhsStrs {
			//	if strings.Contains(batchLhsStr, incLhsStr) {
			//		intersectCount++
			//		isCover = true
			//		break
			//	}
			//}
			for _, incRule := range incRules {
				if inc_rule_dig.ContainAllPredicates(batchRule.Node.Lhs, incRule.Node.Lhs) {
					intersectCount++
					isCover = true
					break
				}
			}
			if !isCover {
				logger.Infof("[调试日志] rhs:%v, ree:%v", batchKey, inc_rule_dig.GetIncRuleRee(batchRule))
			}
		}
	}

	//set := make(map[string]struct{})
	//for _, incRule := range incMinimalRules {
	//	ree := inc_rule_dig.GetIncRuleRee(incRule)
	//	set[ree] = struct{}{}
	//}
	//
	//intersectCount := 0
	//for _, batchRule := range batchMinimalRules {
	//	ree := inc_rule_dig.GetIncRuleRee(batchRule)
	//	if _, ok := set[ree]; ok {
	//		intersectCount++
	//	} else {
	//		logger.Infof("[调试日志] ree:%v, support:%v, confidence:%v", ree, batchRule.CR, batchRule.FTR)
	//	}
	//}

	batchCount := len(batchMinimalRules)
	var recallRate float64
	if batchCount > 0 {
		recallRate = float64(intersectCount) / float64(batchCount)
	}

	logger.Infof("[getRecallRateNew] finish get recall rate, intersectCount:%v, batchCount:%v, recallRate:%v", intersectCount, batchCount, recallRate)
	return recallRate
}

func Test_db_logger_print(t *testing.T) {
	config.InitConfig1()
	all := config.All
	l := all.Logger
	s := all.Server
	logger.InitLogger(l.Level, "rock", l.Path, l.MaxAge, l.RotationTime, l.RotationSize, s.SentryDsn)
	pgConfig := all.Pg

	logger.Infof("GORM connect init:")

	connectionString := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable&TimeZone=Asia/Shanghai",
		pgConfig.User, pgConfig.Password, pgConfig.Host, pgConfig.Port, pgConfig.DB)
	logger.Info("database connectionString: ", connectionString)

	db, err := gorm.Open(postgres.Open(connectionString), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			//禁用表的复数形式
			SingularTable: true,
		},
		//Logger: logger2.Default.LogMode(logger2.Silent), // 默认日志输出级别warn
	})

	if err != nil {
		logger.Errorf("GORM connect Open error: %v\n", err)
	}

	sampleNode := &dao.IncSampleNode{
		TaskId:               10001,
		CoverRadius:          5,
		CurrentNodeJson:      "{\"Node\":{\"Rhs\":{\"PredicateStr\":\"t0.income=t1.income\",\"LeftColumn\":{\"ColumnIndex\":0,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"income\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"RightColumn\":{\"ColumnIndex\":1,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"income\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":1,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0.6343286594351695,\"UdfIndex\":0,\"LeftColumnVectorFilePath\":\"\",\"RightColumnVectorFilePath\":\"\"},\"GiniIndex\":0,\"Lhs\":[{\"PredicateStr\":\"t0.education=t1.education\",\"LeftColumn\":{\"ColumnIndex\":0,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"education\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"RightColumn\":{\"ColumnIndex\":1,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"education\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":1,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0.19038945266841784,\"UdfIndex\":0,\"LeftColumnVectorFilePath\":\"\",\"RightColumnVectorFilePath\":\"\"},{\"PredicateStr\":\"t0.education-num=t1.education-num\",\"LeftColumn\":{\"ColumnIndex\":0,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"education-num\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"RightColumn\":{\"ColumnIndex\":1,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"education-num\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":1,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0.19038945266841784,\"UdfIndex\":0,\"LeftColumnVectorFilePath\":\"\",\"RightColumnVectorFilePath\":\"\"}],\"LhsCandidate\":[{\"PredicateStr\":\"t0.occupation=t1.occupation\",\"LeftColumn\":{\"ColumnIndex\":0,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"occupation\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"RightColumn\":{\"ColumnIndex\":1,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"occupation\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":1,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0.09711042067139654,\"UdfIndex\":0,\"LeftColumnVectorFilePath\":\"\",\"RightColumnVectorFilePath\":\"\"},{\"PredicateStr\":\"t0.age=t1.age\",\"LeftColumn\":{\"ColumnIndex\":0,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"age\",\"ColumnType\":\"text\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"RightColumn\":{\"ColumnIndex\":1,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"age\",\"ColumnType\":\"text\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":1,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0.021321017590293995,\"UdfIndex\":0,\"LeftColumnVectorFilePath\":\"\",\"RightColumnVectorFilePath\":\"\"},{\"PredicateStr\":\"t0.fnlwgt=t1.fnlwgt\",\"LeftColumn\":{\"ColumnIndex\":0,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"fnlwgt\",\"ColumnType\":\"text\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"RightColumn\":{\"ColumnIndex\":1,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"fnlwgt\",\"ColumnType\":\"text\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":1,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0.000038020456724230964,\"UdfIndex\":0,\"LeftColumnVectorFilePath\":\"\",\"RightColumnVectorFilePath\":\"\"}],\"LhsCrossCandidate\":[],\"LhsCandidateGiniIndex\":[],\"LhsCrossCandidateGiniIndex\":[],\"TableId2index\":{\"1735094580823\":0}},\"RuleType\":1,\"CR\":0.13095734894120964,\"FTR\":0.6877283687520619}",
		PredecessorNodesJson: "[{\"Node\":{\"Rhs\":{\"PredicateStr\":\"t0.income=t1.income\",\"LeftColumn\":{\"ColumnIndex\":0,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"income\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"RightColumn\":{\"ColumnIndex\":1,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"income\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":1,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0.6343286594351695,\"UdfIndex\":0,\"LeftColumnVectorFilePath\":\"\",\"RightColumnVectorFilePath\":\"\"},\"GiniIndex\":0,\"Lhs\":[{\"PredicateStr\":\"t0.education=t1.education\",\"LeftColumn\":{\"ColumnIndex\":0,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"education\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"RightColumn\":{\"ColumnIndex\":1,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"education\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":1,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0.19038945266841784,\"UdfIndex\":0,\"LeftColumnVectorFilePath\":\"\",\"RightColumnVectorFilePath\":\"\"}],\"LhsCandidate\":[{\"PredicateStr\":\"t0.education-num=t1.education-num\",\"LeftColumn\":{\"ColumnIndex\":0,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"education-num\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"RightColumn\":{\"ColumnIndex\":1,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"education-num\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":1,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0.19038945266841784,\"UdfIndex\":0,\"LeftColumnVectorFilePath\":\"\",\"RightColumnVectorFilePath\":\"\"},{\"PredicateStr\":\"t0.occupation=t1.occupation\",\"LeftColumn\":{\"ColumnIndex\":0,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"occupation\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"RightColumn\":{\"ColumnIndex\":1,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"occupation\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":1,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0.09711042067139654,\"UdfIndex\":0,\"LeftColumnVectorFilePath\":\"\",\"RightColumnVectorFilePath\":\"\"},{\"PredicateStr\":\"t0.age=t1.age\",\"LeftColumn\":{\"ColumnIndex\":0,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"age\",\"ColumnType\":\"text\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"RightColumn\":{\"ColumnIndex\":1,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"age\",\"ColumnType\":\"text\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":1,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0.021321017590293995,\"UdfIndex\":0,\"LeftColumnVectorFilePath\":\"\",\"RightColumnVectorFilePath\":\"\"},{\"PredicateStr\":\"t0.fnlwgt=t1.fnlwgt\",\"LeftColumn\":{\"ColumnIndex\":0,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"fnlwgt\",\"ColumnType\":\"text\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"RightColumn\":{\"ColumnIndex\":1,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"fnlwgt\",\"ColumnType\":\"text\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":1,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0.000038020456724230964,\"UdfIndex\":0,\"LeftColumnVectorFilePath\":\"\",\"RightColumnVectorFilePath\":\"\"}],\"LhsCrossCandidate\":[],\"LhsCandidateGiniIndex\":[],\"LhsCrossCandidateGiniIndex\":[],\"TableId2index\":{\"1735094580823\":0}},\"RuleType\":1,\"CR\":0.13095734894120964,\"FTR\":0.6877283687520619},{\"Node\":{\"Rhs\":{\"PredicateStr\":\"t0.income=t1.income\",\"LeftColumn\":{\"ColumnIndex\":0,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"income\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"RightColumn\":{\"ColumnIndex\":1,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"income\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":1,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0.6343286594351695,\"UdfIndex\":0,\"LeftColumnVectorFilePath\":\"\",\"RightColumnVectorFilePath\":\"\"},\"GiniIndex\":0,\"Lhs\":[{\"PredicateStr\":\"t0.sex=t1.sex\",\"LeftColumn\":{\"ColumnIndex\":0,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"sex\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"RightColumn\":{\"ColumnIndex\":1,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"sex\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":1,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0.5572302849385358,\"UdfIndex\":0,\"LeftColumnVectorFilePath\":\"\",\"RightColumnVectorFilePath\":\"\"}],\"LhsCandidate\":[{\"PredicateStr\":\"t0.workclass=t1.workclass\",\"LeftColumn\":{\"ColumnIndex\":0,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"workclass\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"RightColumn\":{\"ColumnIndex\":1,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"workclass\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":1,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0.5028549066716583,\"UdfIndex\":0,\"LeftColumnVectorFilePath\":\"\",\"RightColumnVectorFilePath\":\"\"},{\"PredicateStr\":\"t0.marital-status=t1.marital-status\",\"LeftColumn\":{\"ColumnIndex\":0,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"marital-status\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"RightColumn\":{\"ColumnIndex\":1,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"marital-status\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":1,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0.3398607031388196,\"UdfIndex\":0,\"LeftColumnVectorFilePath\":\"\",\"RightColumnVectorFilePath\":\"\"},{\"PredicateStr\":\"t0.relationship=t1.relationship\",\"LeftColumn\":{\"ColumnIndex\":0,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"relationship\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"RightColumn\":{\"ColumnIndex\":1,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"relationship\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":1,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0.26784684365142425,\"UdfIndex\":0,\"LeftColumnVectorFilePath\":\"\",\"RightColumnVectorFilePath\":\"\"},{\"PredicateStr\":\"t0.hours-per-week=t1.hours-per-week\",\"LeftColumn\":{\"ColumnIndex\":0,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"hours-per-week\",\"ColumnType\":\"text\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"RightColumn\":{\"ColumnIndex\":1,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"hours-per-week\",\"ColumnType\":\"text\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":1,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0.2375203502938334,\"UdfIndex\":0,\"LeftColumnVectorFilePath\":\"\",\"RightColumnVectorFilePath\":\"\"},{\"PredicateStr\":\"t0.education=t1.education\",\"LeftColumn\":{\"ColumnIndex\":0,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"education\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"RightColumn\":{\"ColumnIndex\":1,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"education\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":1,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0.19038945266841784,\"UdfIndex\":0,\"LeftColumnVectorFilePath\":\"\",\"RightColumnVectorFilePath\":\"\"},{\"PredicateStr\":\"t0.education-num=t1.education-num\",\"LeftColumn\":{\"ColumnIndex\":0,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"education-num\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"RightColumn\":{\"ColumnIndex\":1,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"education-num\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":1,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0.19038945266841784,\"UdfIndex\":0,\"LeftColumnVectorFilePath\":\"\",\"RightColumnVectorFilePath\":\"\"},{\"PredicateStr\":\"t0.occupation=t1.occupation\",\"LeftColumn\":{\"ColumnIndex\":0,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"occupation\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"RightColumn\":{\"ColumnIndex\":1,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"occupation\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":1,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0.09711042067139654,\"UdfIndex\":0,\"LeftColumnVectorFilePath\":\"\",\"RightColumnVectorFilePath\":\"\"},{\"PredicateStr\":\"t0.age=t1.age\",\"LeftColumn\":{\"ColumnIndex\":0,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"age\",\"ColumnType\":\"text\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"RightColumn\":{\"ColumnIndex\":1,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"age\",\"ColumnType\":\"text\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":1,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0.021321017590293995,\"UdfIndex\":0,\"LeftColumnVectorFilePath\":\"\",\"RightColumnVectorFilePath\":\"\"},{\"PredicateStr\":\"t0.fnlwgt=t1.fnlwgt\",\"LeftColumn\":{\"ColumnIndex\":0,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"fnlwgt\",\"ColumnType\":\"text\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"RightColumn\":{\"ColumnIndex\":1,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"fnlwgt\",\"ColumnType\":\"text\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":1,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0.000038020456724230964,\"UdfIndex\":0,\"LeftColumnVectorFilePath\":\"\",\"RightColumnVectorFilePath\":\"\"}],\"LhsCrossCandidate\":[],\"LhsCandidateGiniIndex\":[],\"LhsCrossCandidateGiniIndex\":[],\"TableId2index\":{\"1735094580823\":0}},\"RuleType\":1,\"CR\":0.3458108461376622,\"FTR\":0.6205545485750082},{\"Node\":{\"Rhs\":{\"PredicateStr\":\"t0.income=t1.income\",\"LeftColumn\":{\"ColumnIndex\":0,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"income\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"RightColumn\":{\"ColumnIndex\":1,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"income\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":1,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0.6343286594351695,\"UdfIndex\":0,\"LeftColumnVectorFilePath\":\"\",\"RightColumnVectorFilePath\":\"\"},\"GiniIndex\":0,\"Lhs\":[{\"PredicateStr\":\"t0.hours-per-week=t1.hours-per-week\",\"LeftColumn\":{\"ColumnIndex\":0,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"hours-per-week\",\"ColumnType\":\"text\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"RightColumn\":{\"ColumnIndex\":1,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"hours-per-week\",\"ColumnType\":\"text\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":1,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0.2375203502938334,\"UdfIndex\":0,\"LeftColumnVectorFilePath\":\"\",\"RightColumnVectorFilePath\":\"\"}],\"LhsCandidate\":[{\"PredicateStr\":\"t0.education=t1.education\",\"LeftColumn\":{\"ColumnIndex\":0,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"education\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"RightColumn\":{\"ColumnIndex\":1,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"education\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":1,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0.19038945266841784,\"UdfIndex\":0,\"LeftColumnVectorFilePath\":\"\",\"RightColumnVectorFilePath\":\"\"},{\"PredicateStr\":\"t0.education-num=t1.education-num\",\"LeftColumn\":{\"ColumnIndex\":0,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"education-num\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"RightColumn\":{\"ColumnIndex\":1,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"education-num\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":1,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0.19038945266841784,\"UdfIndex\":0,\"LeftColumnVectorFilePath\":\"\",\"RightColumnVectorFilePath\":\"\"},{\"PredicateStr\":\"t0.occupation=t1.occupation\",\"LeftColumn\":{\"ColumnIndex\":0,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"occupation\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"RightColumn\":{\"ColumnIndex\":1,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"occupation\",\"ColumnType\":\"enum\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":1,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0.09711042067139654,\"UdfIndex\":0,\"LeftColumnVectorFilePath\":\"\",\"RightColumnVectorFilePath\":\"\"},{\"PredicateStr\":\"t0.age=t1.age\",\"LeftColumn\":{\"ColumnIndex\":0,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"age\",\"ColumnType\":\"text\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"RightColumn\":{\"ColumnIndex\":1,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"age\",\"ColumnType\":\"text\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":1,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0.021321017590293995,\"UdfIndex\":0,\"LeftColumnVectorFilePath\":\"\",\"RightColumnVectorFilePath\":\"\"},{\"PredicateStr\":\"t0.fnlwgt=t1.fnlwgt\",\"LeftColumn\":{\"ColumnIndex\":0,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"fnlwgt\",\"ColumnType\":\"text\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"RightColumn\":{\"ColumnIndex\":1,\"TableId\":\"1735094580823\",\"TableAlias\":\"\",\"ColumnId\":\"fnlwgt\",\"ColumnType\":\"text\",\"IsML\":false,\"JoinTableId\":\"\",\"Role\":0},\"GroupByColumn\":null,\"ConstantValue\":null,\"ConstantIndexValue\":0,\"SymbolType\":\"=\",\"PredicateType\":1,\"UDFName\":\"\",\"Threshold\":0,\"Support\":0.000038020456724230964,\"UdfIndex\":0,\"LeftColumnVectorFilePath\":\"\",\"RightColumnVectorFilePath\":\"\"}],\"LhsCrossCandidate\":[],\"LhsCandidateGiniIndex\":[],\"LhsCrossCandidateGiniIndex\":[],\"TableId2index\":{\"1735094580823\":0}},\"RuleType\":1,\"CR\":0.1566329962966198,\"FTR\":0.6593655909434596}]",
		NeighborNodesJson:    "[]",
		MinConfidence:        0.23252,
		MaxConfidence:        0.6859,
		CreateTime:           1735094596314,
		UpdateTime:           1735094596314,
		NeighborConfsJson:    "[0.6431165878841175,0.6350588028965002,0.6338888907178487,0.6708972301117689,0.6648600102247129,0.6505764951931273,0.6714618374170787,0.6989999027761714,0.6989999027761714,0.6689843192306825,0.6958123232644178,0.6584178031964277,0.6568151744635069,0.6944047516458582,0.6871007193881333,0.6732372836615508,0.6931198100366148,0.7188928174385933,0.7188928174385933,0.6909523068483412,0.7167011372039624,0.6162728552574116,0.6498906268665772,0.6533573575827721,0.6400590717238608,0.6541346916180634,0.6855127510668979,0.6855127510668979,0.6504605946059349,0.6784541085042857,0.6431165878841175,0.6350588028965002,0.6338888907178487,0.6708972301117689,0.6648600102247129,0.6505764951931273,0.6714618374170787,0.6989999027761714,0.6989999027761714,0.6689843192306825,0.6958123232644178,0.6584178031964277,0.6568151744635069,0.6944047516458582,0.6871007193881333,0.6732372836615508,0.6931198100366148,0.7188928174385933,0.7188928174385933,0.6909523068483412,0.7167011372039624,0.6162728552574116,0.6498906268665772,0.6533573575827721,0.6400590717238608,0.6541346916180634,0.6855127510668979,0.6855127510668979,0.6504605946059349,0.6784541085042857,0.614948239933772,0.5916028107639404,0.6413407835321857,0.6763793386684608,0.6763793386684608,0.6662690316931272,0.666465740662948,0.6835895119584334,0.7091163880036049,0.7091163880036049,0.6962875328206816,0.6431165878841175,0.6350588028965002,0.6338888907178487,0.6708972301117689,0.6648600102247129,0.6505764951931273,0.6714618374170787,0.6989999027761714,0.6989999027761714,0.6689843192306825,0.6958123232644178,0.6584178031964277,0.6568151744635069,0.6944047516458582,0.6871007193881333,0.6732372836615508,0.6931198100366148,0.7188928174385933,0.7188928174385933,0.6909523068483412,0.7167011372039624,0.6162728552574116,0.6498906268665772,0.6533573575827721,0.6400590717238608,0.6541346916180634,0.6855127510668979,0.6855127510668979,0.6504605946059349,0.6784541085042857,0.6431165878841175,0.6350588028965002,0.6338888907178487,0.6708972301117689,0.6648600102247129,0.6505764951931273,0.6714618374170787,0.6989999027761714,0.6989999027761714,0.6689843192306825,0.6958123232644178,0.6584178031964277,0.6568151744635069,0.6944047516458582,0.6871007193881333,0.6732372836615508,0.6931198100366148,0.7188928174385933,0.7188928174385933,0.6909523068483412,0.7167011372039624,0.6162728552574116,0.6498906268665772,0.6533573575827721,0.6400590717238608,0.6541346916180634,0.6855127510668979,0.6855127510668979,0.6504605946059349,0.6784541085042857,0.6431165878841175,0.6350588028965002,0.6338888907178487,0.6708972301117689,0.6648600102247129,0.6505764951931273,0.6714618374170787,0.6989999027761714,0.6989999027761714,0.6689843192306825,0.6958123232644178,0.6584178031964277,0.6568151744635069,0.6944047516458582,0.6871007193881333,0.6732372836615508,0.6931198100366148,0.7188928174385933,0.7188928174385933,0.6909523068483412,0.7167011372039624,0.6162728552574116,0.6498906268665772,0.6533573575827721,0.6400590717238608,0.6541346916180634,0.6855127510668979,0.6855127510668979,0.6504605946059349,0.6784541085042857,0.6431165878841175,0.6350588028965002,0.6338888907178487,0.6708972301117689,0.6648600102247129,0.6505764951931273,0.6714618374170787,0.6989999027761714,0.6989999027761714,0.6689843192306825,0.6958123232644178,0.6584178031964277,0.6568151744635069,0.6944047516458582,0.6871007193881333,0.6732372836615508,0.6931198100366148,0.7188928174385933,0.7188928174385933,0.6909523068483412,0.7167011372039624,0.6162728552574116,0.6498906268665772,0.6533573575827721,0.6400590717238608,0.6541346916180634,0.6855127510668979,0.6855127510668979,0.6504605946059349,0.6784541085042857,0.6431165878841175,0.6350588028965002,0.6338888907178487,0.6708972301117689,0.6648600102247129,0.6505764951931273,0.6714618374170787,0.6989999027761714,0.6989999027761714,0.6689843192306825,0.6958123232644178,0.6584178031964277,0.6568151744635069,0.6944047516458582,0.6871007193881333,0.6732372836615508,0.6931198100366148,0.7188928174385933,0.7188928174385933,0.6909523068483412,0.7167011372039624,0.6162728552574116,0.6498906268665772,0.6533573575827721,0.6400590717238608,0.6541346916180634,0.6855127510668979,0.6855127510668979,0.6504605946059349,0.6784541085042857,0.6431165878841175,0.6350588028965002,0.6338888907178487,0.6708972301117689,0.6648600102247129,0.6505764951931273,0.6714618374170787,0.6989999027761714,0.6989999027761714,0.6689843192306825,0.6958123232644178,0.6584178031964277,0.6568151744635069,0.6944047516458582,0.6871007193881333,0.6732372836615508,0.6931198100366148,0.7188928174385933,0.7188928174385933,0.6909523068483412,0.7167011372039624,0.6162728552574116,0.6498906268665772,0.6533573575827721,0.6400590717238608,0.6541346916180634,0.6855127510668979,0.6855127510668979,0.6504605946059349,0.6784541085042857,0.614948239933772,0.5916028107639404,0.6413407835321857,0.6763793386684608,0.6763793386684608,0.6662690316931272,0.666465740662948,0.6835895119584334,0.7091163880036049,0.7091163880036049,0.6962875328206816,0.6431165878841175,0.6350588028965002,0.6338888907178487,0.6708972301117689,0.6648600102247129,0.6505764951931273,0.6714618374170787,0.6989999027761714,0.6989999027761714,0.6689843192306825,0.6958123232644178,0.6584178031964277,0.6568151744635069,0.6944047516458582,0.6871007193881333,0.6732372836615508,0.6931198100366148,0.7188928174385933,0.7188928174385933,0.6909523068483412,0.7167011372039624,0.6162728552574116,0.6498906268665772,0.6533573575827721,0.6400590717238608,0.6541346916180634,0.6855127510668979,0.6855127510668979,0.6504605946059349,0.6784541085042857,0.6431165878841175,0.6350588028965002,0.6338888907178487,0.6708972301117689,0.6648600102247129,0.6505764951931273,0.6714618374170787,0.6989999027761714,0.6989999027761714,0.6689843192306825,0.6958123232644178,0.6584178031964277,0.6568151744635069,0.6944047516458582,0.6871007193881333,0.6732372836615508,0.6931198100366148,0.7188928174385933,0.7188928174385933,0.6909523068483412,0.7167011372039624,0.6162728552574116,0.6498906268665772,0.6533573575827721,0.6400590717238608,0.6541346916180634,0.6855127510668979,0.6855127510668979,0.6504605946059349,0.6784541085042857,0.6431165878841175,0.6350588028965002,0.6338888907178487,0.6708972301117689,0.6648600102247129,0.6505764951931273,0.6714618374170787,0.6989999027761714,0.6989999027761714,0.6689843192306825,0.6958123232644178,0.6584178031964277,0.6568151744635069,0.6944047516458582,0.6871007193881333,0.6732372836615508,0.6931198100366148,0.7188928174385933,0.7188928174385933,0.6909523068483412,0.7167011372039624,0.6162728552574116,0.6498906268665772,0.6533573575827721,0.6400590717238608,0.6541346916180634,0.6855127510668979,0.6855127510668979,0.6504605946059349,0.6784541085042857,0.7010985061913407,0.6775608277951808,0.6814375927223362]",
	}

	err = db.Create(sampleNode).Error
	if err != nil {
		logger.Errorf("create failed, err:%v", err)
	}
}

func Test_getOutputSize(t *testing.T) {
	config.InitConfig1()
	all := config.All
	l := all.Logger
	s := all.Server
	logger.InitLogger(l.Level, "rock", l.Path, l.MaxAge, l.RotationTime, l.RotationSize, s.SentryDsn)
	db.InitGorm()

	taskId := int64(1000612)
	minimalRules, err := inc_rule_dig.GetMinimalIncRulesByTaskId(taskId)
	if err != nil {
		logger.Error(err)
		return
	}
	minimalRules = getMinimizeProcessRulesNew(minimalRules)

	prunedRules, err := inc_rule_dig.GetPrunedIncRulesByTaskId(taskId)
	if err != nil {
		logger.Error(err)
		return
	}

	sampleNodes, err := inc_rule_dig.GetSampleNodesByTaskId(taskId)
	if err != nil {
		logger.Error(err)
		return
	}

	_, minimalSize, auxSize, _, _ := getOutputMemorySizeNew(minimalRules, prunedRules, sampleNodes, taskId, true, false)
	logger.Infof("minimal size:%v, aux size:%v", minimalSize, auxSize)
}

func Test_recallRate_new(t *testing.T) {
	config.InitConfig1()
	all := config.All
	l := all.Logger
	s := all.Server
	logger.InitLogger(l.Level, "rock", l.Path, l.MaxAge, l.RotationTime, l.RotationSize, s.SentryDsn)
	db.InitGorm()

	incTaskId := int64(1003018)
	batchTaskId := int64(1003011)

	incRules, err := inc_rule_dig.GetMinimalIncRulesByTaskId(incTaskId)
	if err != nil {
		logger.Errorf("[getRecallRate] GetMinimalIncRulesByTaskId failed, error:%v, incTaskId:%v", err, incTaskId)
	}

	incMinimalRules := getMinimizeProcessRulesNew(incRules)

	batchRules, err := inc_rule_dig.GetMinimalIncRulesByTaskId(batchTaskId)
	if err != nil {
		logger.Errorf("[getRecallRate] GetMinimalIncRulesByTaskId failed, error:%v, batchTaskId:%v", err, batchTaskId)
	}
	batchMinimalRules := getMinimizeProcessRulesNew(batchRules)
	getRecallRateNew2(incMinimalRules, batchMinimalRules)
}

func getRecallRateNew2(incMinimalRules []*inc_rule_dig.IncRule, batchMinimalRules []*inc_rule_dig.IncRule) float64 {

	incKey2Rules := incRuleGroupByRhsNew(incMinimalRules)
	batchKey2Rules := incRuleGroupByRhsNew(batchMinimalRules)

	intersectCount := 0
	for batchKey, batchRules := range batchKey2Rules {
		incRules := incKey2Rules[batchKey]
		//incLhsStrs := getRulesSortedLhsStr(incRules)
		for _, batchRule := range batchRules {
			//batchLhsStr := getSortedLhsStr(batchRule)
			isCover := false
			//for _, incLhsStr := range incLhsStrs {
			//	if strings.Contains(batchLhsStr, incLhsStr) {
			//		intersectCount++
			//		isCover = true
			//		break
			//	}
			//}
			for _, incRule := range incRules {
				if inc_rule_dig.ContainAllPredicates(batchRule.Node.Lhs, incRule.Node.Lhs) {
					intersectCount++
					isCover = true
					break
				}
			}
			if !isCover {
				logger.Infof("[调试日志] rhs:%v, ree:%v", batchKey, inc_rule_dig.GetIncRuleRee(batchRule))
			}
		}
	}

	//set := make(map[string]struct{})
	//for _, incRule := range incMinimalRules {
	//	ree := inc_rule_dig.GetIncRuleRee(incRule)
	//	set[ree] = struct{}{}
	//}
	//
	//intersectCount := 0
	//for _, batchRule := range batchMinimalRules {
	//	ree := inc_rule_dig.GetIncRuleRee(batchRule)
	//	if _, ok := set[ree]; ok {
	//		intersectCount++
	//	} else {
	//		logger.Infof("[调试日志] ree:%v, support:%v, confidence:%v", ree, batchRule.CR, batchRule.FTR)
	//	}
	//}

	batchCount := len(batchMinimalRules)
	var recallRate float64
	if batchCount > 0 {
		recallRate = float64(intersectCount) / float64(batchCount)
	}

	logger.Infof("[getRecallRateNew] finish get recall rate, intersectCount:%v, batchCount:%v, recallRate:%v", intersectCount, batchCount, recallRate)
	return recallRate
}

func Test_golden_recall(t *testing.T) {
	config.InitConfig1()
	all := config.All
	l := all.Logger
	s := all.Server
	logger.InitLogger(l.Level, "rock", l.Path, l.MaxAge, l.RotationTime, l.RotationSize, s.SentryDsn)
	db.InitGorm()

	goldenRules := []string{
		"t0.education-num=t1.education-num -> t0.education=t1.education",
		"t0.capital-loss=t1.capital-loss ^ t0.income=t1.income ^ t0.education=t1.education ^ t0.education-num=t1.education-num -> t0.capital-gain=t1.capital-gain",
		"t0.education=t1.education ^ t0.age=t1.age ^ t0.income=t1.income -> t0.capital-gain=t1.capital-gain",
		"t0.capital-gain=t1.capital-gain -> t0.capital-loss=t1.capital-loss",
		"t0.income=t1.income -> t0.capital-gain=t1.capital-gain",
		"t0.marital-status=t1.marital-status ^ t0.relationship=t1.relationship -> t0.sex=t1.sex",
		"t0.income=t1.income ^ t0.marital-status=t1.marital-status ^ t0.age=t1.age ^ t0.education=t1.education -> t0.native-country=t1.native-country",
		"t0.race=t1.race ^ t0.income=t1.income ^ t0.hours-per-week=t1.hours-per-week -> t0.capital-loss=t1.capital-loss",
		"t0.fnlwgt=t1.fnlwgt ^ t0.education-num=t1.education-num -> t0.income=t1.income",
		"t0.capital-gain=t1.capital-gain ^ t0.native-country=t1.native-country ^ t0.sex=t1.sex ^ t0.marital-status=t1.marital-status -> t0.race=t1.race",
		"t0.fnlwgt=t1.fnlwgt ^ t0.education=t1.education -> t0.workclass=t1.workclass",
		"t0.education=Bachelors -> t0.education-num=13",
		"t0.workclass=noValueSetHere123156456 -> t0.occupation=noValueSetHere123156456",
		"t0.capital-gain>=8614 ^ t0.marital-status!=Married-civ-spouse -> t0.income=>50K",
		"t0.relationship=Own-child -> t0.marital-status=Never-married",
	}
	//taskIds := []int64{
	//	1003018,
	//	1003019,
	//	1003020,
	//	1003021,
	//}
	//taskIds := []int64{
	//	1003013,
	//	1003014,
	//	1003015,
	//	1003016,
	//}
	//taskIds := []int64{
	//	1003076,
	//	1003077,
	//	1003078,
	//	1003079,
	//	1003080,
	//	1003081,
	//	1003082,
	//	1003083,
	//}
	taskIds := []int64{
		1003085,
		1003086,
		1003087,
		1003088,
		1003089,
		1003090,
		1003091,
		1003092,
	}
	coverIds := make([]int, 0)
	for _, taskId := range taskIds {
		incTaskId := taskId
		incRules, err := inc_rule_dig.GetMinimalIncRulesByTaskId(incTaskId)
		if err != nil {
			logger.Errorf("[getRecallRate] GetMinimalIncRulesByTaskId failed, error:%v, incTaskId:%v", err, incTaskId)
		}

		incMinimalRules := getMinimizeProcessRulesNew(incRules)

		dtRules, err := inc_rule_dig.GetDecisionTreeRules(incTaskId)
		if err != nil {
			logger.Errorf("[getRecallRate] GetDecisionTreeRules failed, error:%v, incTaskId:%v", err, incTaskId)
		}

		ids := getGuidelinesCoverCount(incMinimalRules, dtRules, goldenRules)
		coverIds = append(coverIds, ids...)
		coverIds = utils2.Distinct(coverIds)
		sort.Slice(coverIds, func(i, j int) bool {
			return coverIds[i] < coverIds[j]
		})

		recallRate := float64(len(coverIds)) / float64(len(goldenRules))
		logger.Infof("taskId:%v, cover size:%v, coverIds:%v, recall:%v", taskId, len(coverIds), coverIds, recallRate)
	}

}

func getGuidelinesCoverCount(incMinimalRules []*inc_rule_dig.IncRule, decisionTreeRules []*rds.Rule, goldenRules []string) []int {
	logger.Infof("[getGuidelinesCoverCount] incremental REEs size:%v, decision tree REEs size:%v, golden REEs size:%v", len(incMinimalRules), len(decisionTreeRules), len(goldenRules))

	incKey2Rules := incRuleGroupByRhsNew(incMinimalRules)
	dtKey2Rules := dtRuleGroupByRhs(decisionTreeRules)
	goldenKey2Rules := goldenRuleGroupByRhs(goldenRules)

	coverIds := make([]int, 0)
	for goldenKey, rules := range goldenKey2Rules {
		incRules := incKey2Rules[goldenKey]
		for _, goldenRule := range rules {
			goldenLhsStrs := getGoldenRuleLhsStrs(goldenRule.Ree)
			isCover := false
			for _, incRule := range incRules {
				lhsStrs := getIncRuleLhsStrs(incRule)
				if inc_rule_dig.Contains(goldenLhsStrs, lhsStrs) {
					coverIds = append(coverIds, goldenRule.Id)
					isCover = true
					break
				}
			}
			if !isCover {
				logger.Infof("[getRecallRateOfGuidelines] incremental not cover golden rule, rhs:%v, ree:%v", goldenKey, goldenRule)
			}
		}
	}

	for goldenKey, rules := range goldenKey2Rules {
		dtRules := dtKey2Rules[goldenKey]
		for _, goldenRule := range rules {
			goldenLhsStrs := getGoldenRuleLhsStrs(goldenRule.Ree)
			isCover := false
			for _, dtRule := range dtRules {
				lhsStrs := getDTRuleLhsStrs(dtRule)
				if inc_rule_dig.Contains(goldenLhsStrs, lhsStrs) {
					coverIds = append(coverIds, goldenRule.Id)
					isCover = true
					break
				}
			}
			if !isCover {
				logger.Infof("[getGuidelinesCoverCount] decision tree not cover golden rule, rhs:%v, ree:%v", goldenKey, goldenRule)
			}
		}
	}

	logger.Infof("[getGuidelinesCoverCount] finish get Guidelines cover IDs:%v", coverIds)
	return coverIds
}

func Test_decision_tree_minimize(t *testing.T) {
	config.InitConfig1()
	all := config.All
	l := all.Logger
	s := all.Server
	logger.InitLogger(l.Level, "rock", l.Path, l.MaxAge, l.RotationTime, l.RotationSize, s.SentryDsn)
	db.InitGorm()

	taskId := int64(1002987)
	decisionTreeRules, err := inc_rule_dig.GetDecisionTreeRules(taskId)
	if err != nil {
		logger.Error(err)
		return
	}
	//incSaveRules := getDecisionTreeRuleMinimize(decisionTreeRules)
	//for _, rule := range incSaveRules {
	//	logger.Infof("inc save ree:%v", inc_rule_dig.GetRuleRee(*rule))
	//}

	taskId1 := int64(1002997)
	decisionTreeRules1, err := inc_rule_dig.GetDecisionTreeRules(taskId1)
	if err != nil {
		logger.Error(err)
		return
	}
	//incSaveRules1 := getDecisionTreeRuleMinimize(decisionTreeRules1)
	//for _, rule := range incSaveRules {
	//	logger.Infof("inc save ree:%v", inc_rule_dig.GetRuleRee(*rule))
	//}
	allDecisionTreeRules := make([]*rds.Rule, 0)
	allDecisionTreeRules = append(allDecisionTreeRules, decisionTreeRules...)
	allDecisionTreeRules = append(allDecisionTreeRules, decisionTreeRules1...)

	getDecisionTreeRuleMinimize(allDecisionTreeRules)

	batchTaskId := int64(1003002)
	batchRules, err := inc_rule_dig.GetDecisionTreeRules(batchTaskId)
	if err != nil {
		logger.Error(err)
		return
	}
	getDecisionTreeRuleMinimize(batchRules)
	//for _, rule := range batchSaveRules {
	//	logger.Infof("batch save ree:%v", inc_rule_dig.GetRuleRee(*rule))
	//}

	//tmpDistinctRules(incSaveRules, batchSaveRules)
}

func getDecisionTreeRuleMinimize(allRules []*rds.Rule) []*rds.Rule {
	resultRules := make([]*rds.Rule, 0)
	key2Rules := rdsRuleGroupByRhs(allRules)
	var wg sync.WaitGroup
	var mutex sync.Mutex
	ch := utils.GenTokenChan(1)
	for _, rules := range key2Rules {
		wg.Add(1)
		<-ch
		go func(rules []*rds.Rule) {
			defer func() {
				ch <- struct{}{}
				wg.Done()
			}()
			uniqueRules := removeDuplicatesRdsRules(rules)
			logger.Infof("rules size:%v, unique size:%v", len(rules), len(uniqueRules))
			saveIndex := make([]int, 0)
			size := len(uniqueRules)
			for i := 0; i < size; i++ {
				isContain := false
				for j := 0; j < size; j++ {
					if i == j {
						continue
					}
					if inc_rule_dig.ContainAllPredicates(uniqueRules[i].LhsPredicates, uniqueRules[j].LhsPredicates) {
						isContain = true
						//logger.Infof("[调试日志-getMinimizeProcessRules] fiter index:%v-ree:%v, index:%v-save ree:%v", i, inc_rule_dig.GetIncRuleRee(rules[i]), j, inc_rule_dig.GetIncRuleRee(rules[j]))
						break
					}
				}
				if !isContain {
					saveIndex = append(saveIndex, i)
				}
			}
			saveIndex = utils2.Distinct(saveIndex)
			saveRules := make([]*rds.Rule, 0, len(saveIndex))
			for _, index := range saveIndex {
				saveRules = append(saveRules, uniqueRules[index])
			}
			mutex.Lock()
			resultRules = append(resultRules, saveRules...)
			mutex.Unlock()
		}(rules)
	}
	wg.Wait()
	logger.Infof("[getDecisionTreeRuleMinimize] src rules size:%v, minimize rules size:%v", len(allRules), len(resultRules))
	return resultRules
}

func rdsRuleGroupByRhs(rules []*rds.Rule) map[string][]*rds.Rule {
	groupByResult := make(map[string][]*rds.Rule)
	for _, rule := range rules {
		if rule == nil {
			continue
		}
		//key := rule.Rhs.Key()
		key := rule.Rhs.PredicateStr
		groupByResult[key] = append(groupByResult[key], rule)
	}
	return groupByResult
}

func removeDuplicatesRdsRules(rules []*rds.Rule) []*rds.Rule {
	unique := make([]*rds.Rule, 0)
	seen := make(map[string]struct{})
	for _, rule := range rules {
		pStrs := make([]string, len(rule.LhsPredicates))
		for i, p := range rule.LhsPredicates {
			pStrs[i] = p.PredicateStr
		}
		sort.Slice(pStrs, func(i, j int) bool {
			return pStrs[i] < pStrs[j]
		})
		key := strings.Join(pStrs, " ^ ")
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			unique = append(unique, rule)
		}
	}
	return unique
}

func tmpDistinctRules(incRules []*rds.Rule, batchRules []*rds.Rule) {
	batchOfRhs := rdsRuleGroupByRhs(batchRules)
	incOfRhs := rdsRuleGroupByRhs(incRules)

	for key, rules := range batchOfRhs {
		rulesLhsStrs := getSortedRulesLhs(rules)
		set := make(map[string]struct{})
		for _, lhsStr := range rulesLhsStrs {
			set[lhsStr] = struct{}{}
		}

		incRulesRhs := incOfRhs[key]
		for _, rule := range incRulesRhs {
			incRuleLhsStr := getSortedRulesLhs([]*rds.Rule{rule})
			if _, ok := set[incRuleLhsStr[0]]; !ok {
				logger.Infof("incRule not in batch:%v", inc_rule_dig.GetRuleRee(*rule))
			}
		}
	}
}

func getSortedRulesLhs(rules []*rds.Rule) []string {
	result := make([]string, len(rules))
	for idx, rule := range rules {
		pStrs := make([]string, len(rule.LhsPredicates))
		for i, p := range rule.LhsPredicates {
			pStrs[i] = p.PredicateStr
		}
		sort.Slice(pStrs, func(i, j int) bool {
			return pStrs[i] < pStrs[j]
		})
		result[idx] = strings.Join(pStrs, " ^ ")
	}
	return result
}

func Test_getDecisionTreeMinimizeAndOutputSize(t *testing.T) {
	config.InitConfig1()
	all := config.All
	l := all.Logger
	s := all.Server
	logger.InitLogger(l.Level, "rock", l.Path, l.MaxAge, l.RotationTime, l.RotationSize, s.SentryDsn)
	db.InitGorm()

	taskIds := []int64{
		//1003168,
		//1003169,
		//1003170,
		//1003171,
		//1003172,
		//1003173,
		//1003174,
		//1003175,
		//1003176,
		//1003177,
		//1003178,
		//1003179,
		//1003180,
		//1003181,
		//1003182,
		//1003183,
		//1003184,
		//1003185,
		//1003186,
		//1003187,
		1003251,
		1003252,
		1003253,
		1003254,
		1003255,
		1003256,
		1003257,
		1003258,
		1003259,
		1003260,
		1003261,
		1003262,
		1003263,
		1003264,
		1003265,
		1003266,
		1003267,
		1003268,
		1003269,
		1003270,
	}

	for _, taskId := range taskIds {
		decisionTreeRules, err := inc_rule_dig.GetDecisionTreeRules(taskId)
		if err != nil {
			logger.Error(err)
			return
		}
		minimalRules := getDecisionTreeRuleMinimize(decisionTreeRules)

		decisionTreeRulesSize := inc_rule_dig.GetRulesSize(minimalRules)
		decisionTreeRulesMB := bytes2MB(decisionTreeRulesSize)
		logger.Infof("**** taskId:%v, minimal ree num:%v, minimal ree size:%v", taskId, len(minimalRules), decisionTreeRulesMB)
	}

}

func Test_relaxSampleNodes(t *testing.T) {
	config.InitConfig1()
	all := config.All
	l := all.Logger
	s := all.Server
	logger.InitLogger(l.Level, "rock", l.Path, l.MaxAge, l.RotationTime, l.RotationSize, s.SentryDsn)
	db.InitGorm()

	preTaskId := int64(1003163)
	preSampleNodes, err := inc_rule_dig.GetSampleNodesByTaskId(preTaskId)
	if err != nil {
		logger.Errorf("GetSampleNodesByTaskId error:%v, taskId:%v", err, preTaskId)
		return
	}

	//newSupport := 0.000001
	newConfidence := 0.6

	recallBound := 0.7
	var relaxSampleNodes []*inc_rule_dig.SampleNode
	if recallBound == 1.0 {
		relaxSampleNodes = preSampleNodes
	} else {
		relaxSampleNodes = getRelaxSampleNodes(preSampleNodes, newConfidence, recallBound)
	}
	logger.Infof("relaxSampleNodes size:%v", len(relaxSampleNodes))
	coverCount := 0
	for _, sampleNode := range relaxSampleNodes {
		if sampleNode.MaxConf >= newConfidence {
			//printSample(i, sampleNode, "****命中")
			coverCount++
		}
	}
	logger.Infof("coverCount:%v", coverCount)
}
