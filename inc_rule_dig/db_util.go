package inc_rule_dig

import (
	"encoding/json"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/global/model/po"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables/task_tree"
	"gitlab.grandhoo.com/rock/rock_v3/inc_rule_dig/dao"
	"gitlab.grandhoo.com/rock/rock_v3/utils"
	"time"
)

func GetTaskIdByIndicator(support float64, confidence float64) (int64, error) {
	//task, err := dao.GetIncRdsTaskByIndicator(support, confidence)
	task, err := dao.GetLatestIncRdsTaskByIndicator(support, confidence)
	if err != nil {
		logger.Errorf("[GetTaskIdByIndicator] pgError: GetIncRdsTaskByIndicator failed, error:%v, support:%v, confidence:%v", err, support, confidence)
		return 0, err
	}
	return task.Id, nil
}

func GetMinimalIncRulesByTaskId(taskId int64) ([]*IncRule, error) {
	rules, err := dao.GetIncMinimalRulesByTaskId(taskId)
	if err != nil {
		logger.Errorf("[GetMinimalIncRulesByTaskId] pgError: GetIncMinimalRulesByTaskId failed, error:%v, taskId:%v", err, taskId)
		return nil, err
	}
	incRules := make([]*IncRule, 0, len(rules))
	for _, rule := range rules {
		node, err := getTaskTreeFromJson(rule.NodeJson)
		if err != nil {
			logger.Error(err)
			continue
		}
		incRule := &IncRule{
			Node:     node,
			RuleType: rule.ReeType,
			CR:       rule.Support,
			FTR:      rule.Confidence,
		}
		incRules = append(incRules, incRule)
	}
	return incRules, nil
}

func GetPrunedIncRulesByTaskId(taskId int64) ([]*IncRule, error) {
	rules, err := dao.GetPrunedIncRulesByTaskId(taskId)
	if err != nil {
		logger.Errorf("[GetPrunedIncRulesByTaskId] pgError: GetPrunedIncRulesByTaskId failed, error:%v, taskId:%v", err, taskId)
		return nil, err
	}
	prunedIncRules := make([]*IncRule, 0, len(rules))
	for _, rule := range rules {
		node, err := getTaskTreeFromJson(rule.NodeJson)
		if err != nil {
			logger.Error(err)
			continue
		}
		prunedIncRule := &IncRule{
			Node:     node,
			RuleType: rule.RuleType,
			CR:       rule.Support,
			FTR:      rule.Confidence,
		}
		prunedIncRules = append(prunedIncRules, prunedIncRule)
	}
	return prunedIncRules, nil
}

func GetSampleNodesByTaskId(taskId int64) ([]*SampleNode, error) {
	nodes, err := dao.GetSampleNodesByTaskId(taskId)
	if err != nil {
		logger.Errorf("[GetPrunedIncRulesByTaskId] pgError: GetPrunedIncRulesByTaskId failed, error:%v, taskId:%v", err, taskId)
		return nil, err
	}
	sampleNodes := make([]*SampleNode, 0, len(nodes))
	for _, node := range nodes {
		currentNode, err := getIncRuleFromJson(node.CurrentNodeJson)
		if err != nil {
			logger.Error(err)
			return nil, err
		}
		predecessorNodes, err := getIncRulesFromJson(node.PredecessorNodesJson)
		if err != nil {
			logger.Error(err)
			return nil, err
		}
		neighborNodes, err := getIncRulesFromJson(node.NeighborNodesJson)
		if err != nil {
			logger.Error(err)
			return nil, err
		}
		//neighborConfs, err := getNeighborConfsFromJson(node.NeighborConfsJson)
		//if err != nil {
		//	logger.Error(err)
		//	return nil, err
		//}
		neighborCDF, err := getNeighborCDFFromJson(node.NeighborCDFJson)
		if err != nil {
			logger.Error(err)
			return nil, err
		}
		sampleNode := &SampleNode{
			CoverRadius:      node.CoverRadius,
			CurrentNode:      currentNode,
			PredecessorNodes: predecessorNodes,
			NeighborNodes:    neighborNodes,
			MinConf:          node.MinConfidence,
			MaxConf:          node.MaxConfidence,
			//NeighborConfs:    neighborConfs,
			NeighborCDF: neighborCDF,
		}
		sampleNodes = append(sampleNodes, sampleNode)
	}
	return sampleNodes, nil
}

//func WriteDecisionTreeRuleToDB(taskId int64, rule rds.Rule) error {
//	currentTime := time.Now().UnixMilli()
//	ree := GetRuleRee(rule)
//	incRule := &dao.IncMinimalRule{
//		TaskId:     taskId,
//		Ree:        ree,
//		NodeJson:   "",
//		ReeType:    rule.RuleType,
//		Support:    rule.CR,
//		Confidence: rule.FTR,
//		CreateTime: currentTime,
//		UpdateTime: currentTime,
//	}
//	err = dao.CreateIncMinimalRule(rule)
//	if err != nil {
//		logger.Errorf("[WriteMinimalIncRulesToDB] pgError: insert rule failed, error:%v", err)
//		return err
//	}
//}
//return nil
//}

func WriteMinimalIncRulesToDB(taskId int64, minimalIncRules []*IncRule) error {
	for _, minimalIncRule := range minimalIncRules {
		currentTime := time.Now().UnixMilli()
		ree := GetIncRuleRee(minimalIncRule)
		nodeJson, err := getJsonFromTaskTree(minimalIncRule.Node)
		if err != nil {
			logger.Error(err)
			return err
		}
		rule := &dao.IncMinimalRule{
			TaskId:     taskId,
			Ree:        ree,
			NodeJson:   nodeJson,
			ReeType:    minimalIncRule.RuleType,
			Support:    minimalIncRule.CR,
			Confidence: minimalIncRule.FTR,
			CreateTime: currentTime,
			UpdateTime: currentTime,
		}
		err = dao.CreateIncMinimalRule(rule)
		if err != nil {
			logger.Errorf("[WriteMinimalIncRulesToDB] pgError: insert rule failed, error:%v", err)
			return err
		}
	}
	return nil
}

func WritePrunedIncRulesToDB(taskId int64, prunedIncRules []*IncRule) error {
	for _, prunedIncRule := range prunedIncRules {
		currentTime := time.Now().UnixMilli()
		nodeJson, err := getJsonFromTaskTree(prunedIncRule.Node)
		if err != nil {
			logger.Error(err)
			return err
		}
		rule := &dao.IncPruneRule{
			TaskId:     taskId,
			NodeJson:   nodeJson,
			RuleType:   prunedIncRule.RuleType,
			Support:    prunedIncRule.CR,
			Confidence: prunedIncRule.FTR,
			CreateTime: currentTime,
			UpdateTime: currentTime,
		}
		err = dao.CreateIncPruneRule(rule)
		if err != nil {
			logger.Errorf("[WritePrunedIncRulesToDB] pgError: insert rule failed, error:%v", err)
			return err
		}
	}
	return nil
}

func WriteSampleNodesToDB(taskId int64, sampleNodes []*SampleNode) error {
	for _, sampleNode := range sampleNodes {
		currentTime := time.Now().UnixMilli()
		currentNodeJson, err := getJsonFromIncRule(sampleNode.CurrentNode)
		if err != nil {
			logger.Error(err)
			return err
		}
		predecessorNodesJson, err := getJsonFromIncRules(sampleNode.PredecessorNodes)
		if err != nil {
			logger.Error(err)
			return err
		}
		neighborNodesJson, err := getJsonFromIncRules(sampleNode.NeighborNodes)
		if err != nil {
			logger.Error(err)
			return err
		}
		//neighborConfsJson, err := getJsonFromNeighborConfs(sampleNode.NeighborConfs)
		//if err != nil {
		//	logger.Error(err)
		//	return err
		//}
		neighborCDFJson, err := getJsonFromNeighborCDF(sampleNode.NeighborCDF)
		node := &dao.IncSampleNode{
			TaskId:               taskId,
			CoverRadius:          sampleNode.CoverRadius,
			CurrentNodeJson:      currentNodeJson,
			PredecessorNodesJson: predecessorNodesJson,
			NeighborNodesJson:    neighborNodesJson,
			MinConfidence:        sampleNode.MinConf,
			MaxConfidence:        sampleNode.MaxConf,
			//NeighborConfsJson:    neighborConfsJson,
			NeighborCDFJson: neighborCDFJson,
			CreateTime:      currentTime,
			UpdateTime:      currentTime,
		}
		err = dao.CreateIncSampleNode(node)
		if err != nil {
			logger.Errorf("[WriteSampleNodesToDB] pgError: insert sample node failed, error:%v", err)
			return err
		}
	}
	return nil
}

func InsertIncRdsTaskToDB(support float64, confidence float64) (int64, error) {
	currentTime := time.Now().UnixMilli()
	task := &dao.IncRdsTask{
		Support:    support,
		Confidence: confidence,
		IsDeleted:  0,
		CreateTime: currentTime,
		UpdateTime: currentTime,
	}
	taskId, err := dao.CreateIncRdsTask(task)
	if err != nil {
		logger.Errorf("[InsertIncRdsTaskToDB] pgError: CreateIncRdsTask failed, error:%v, support:%v, confidence:%v", err, support, confidence)
		return 0, err
	}
	return taskId, nil
}

func DeleteIncRdsTask(taskId int64) error {
	err := dao.DeleteIncRdsTask(taskId)
	if err != nil {
		logger.Errorf("[DeleteIncRdsTask] pgError: DeleteIncRdsTask failed, error:%v, taskId:%v", err, taskId)
		return err
	}
	return nil
}

func DeleteIncRdsTaskByIndicator(support float64, confidence float64) error {
	err := dao.DeleteIncRdsTaskByIndicator(support, confidence)
	if err != nil {
		logger.Errorf("[DeleteIncRdsTaskByIndicator] pgError: DeleteIncRdsTaskByIndicator failed, error:%v, support:%v, confidence:%v", err, support, confidence)
		return err
	}
	return nil
}

func GetLatestIncRdsTask() (*dao.IncRdsTask, error) {
	task, err := dao.GetLatestIncRdsTask()
	if err != nil {
		logger.Errorf("[GetLatestIncRdsTask] pgError: GetLatestIncRdsTask failed, error:%v", err)
		return nil, err
	}
	return &task, nil
}

func GetIncRdsTaskById(taskId int64) (*dao.IncRdsTask, error) {
	task, err := dao.GetIncRdsTaskById(taskId)
	if err != nil {
		logger.Errorf("[GetIncRdsTaskById] pgError: GetIncRdsTaskById failed, error:%v, taskId:%v", err, taskId)
		return nil, err
	}
	return &task, nil
}

func getTaskTreeFromJson(nodeJson string) (*task_tree.TaskTree, error) {
	taskTree := &task_tree.TaskTree{}
	err := json.Unmarshal([]byte(nodeJson), taskTree)
	if err != nil {
		logger.Errorf("[getTaskTreeFromJson] json.Unmarshal error:%v, json:%s", err, nodeJson)
		return nil, err
	}
	return taskTree, nil
}

func getJsonFromTaskTree(taskTree *task_tree.TaskTree) (string, error) {
	treeJson, err := json.Marshal(taskTree)
	if err != nil {
		logger.Errorf("[getJsonFromTaskTree] json.Marshal failed, error:%v, taskTree:%v", err, *taskTree)
		return "", err
	}
	return string(treeJson), nil
}

func getIncRuleFromJson(incRuleJson string) (*IncRule, error) {
	incRule := &IncRule{}
	err := json.Unmarshal([]byte(incRuleJson), incRule)
	if err != nil {
		logger.Errorf("[getIncRuleFromJson] json.Unmarshal failed, error:%v, json:%v", err, incRuleJson)
		return nil, err
	}
	return incRule, nil
}

func getIncRulesFromJson(incRulesJson string) ([]*IncRule, error) {
	var incRules []*IncRule
	err := json.Unmarshal([]byte(incRulesJson), &incRules)
	if err != nil {
		logger.Errorf("[getIncRulesFromJson] json.Unmarshal failed, error:%v, json:%v", err, incRulesJson)
		return nil, err
	}
	return incRules, nil
}

func getJsonFromIncRule(incRule *IncRule) (string, error) {
	incRuleJson, err := json.Marshal(incRule)
	if err != nil {
		logger.Errorf("[getJsonFromIncRule] json.Marshal failed, error:%v, incRule:%v", err, *incRule)
		return "", err
	}
	return string(incRuleJson), err
}

func getJsonFromIncRules(incRules []*IncRule) (string, error) {
	incRulesJson, err := json.Marshal(incRules)
	if err != nil {
		logger.Errorf("[getJsonFromIncRules] json.Marshal failed, error:%v, incRules:%v", err, incRules)
		for _, incRule := range incRules {
			logger.Infof("[getJsonFromIncRules] json.Marshal failed, incRule:%v", *incRule)
		}
		return "", err
	}
	return string(incRulesJson), nil
}

func getJsonFromNeighborConfs(confs []float64) (string, error) {
	confsBytes, err := json.Marshal(confs)
	if err != nil {
		logger.Errorf("[getJsonFromNeighborConfs] json.Marshal failed, error:%v, confs:%v", err, confs)
		return "", err
	}
	return string(confsBytes), nil
}

func getJsonFromNeighborCDF(cdf []int) (string, error) {
	confsBytes, err := json.Marshal(cdf)
	if err != nil {
		logger.Errorf("[getJsonFromNeighborCDF] json.Marshal failed, error:%v, confs:%v", err, cdf)
		return "", err
	}
	return string(confsBytes), nil
}

func getNeighborConfsFromJson(confsJson string) ([]float64, error) {
	if confsJson == "" {
		return []float64{}, nil
	}
	var confs []float64
	err := json.Unmarshal([]byte(confsJson), &confs)
	if err != nil {
		logger.Errorf("[getNeighborConfsFromJson] json.Unmarshal failed, error:%v, json:%v", err, confsJson)
		return nil, err
	}
	return confs, nil
}

func getNeighborCDFFromJson(cdfJson string) ([]int, error) {
	if cdfJson == "" {
		return []int{}, nil
	}
	var cdf []int
	err := json.Unmarshal([]byte(cdfJson), &cdf)
	if err != nil {
		logger.Errorf("[getNeighborCDFFromJson] json.Unmarshal failed, error:%v, json:%v", err, cdfJson)
		return nil, err
	}
	return cdf, nil
}

func GetIncRoots(taskId int64) ([]string, error) {
	roots, err := dao.GetIncRoot(taskId)
	if err != nil {
		return nil, err
	}
	rootJsons := make([]string, len(roots))
	for i, root := range roots {
		rootJsons[i] = root.NodeJson
	}
	return rootJsons, nil
}

func CreateIncRoot(taskId int64, node *task_tree.TaskTree) (err error) {
	bytes, err := json.Marshal(node)
	if err != nil {
		logger.Errorf("[CreateIncRoot] json.Marshal failed, error:%v", err)
		return err
	}
	root := &dao.IncRoot{
		TaskId:   taskId,
		NodeJson: string(bytes),
	}
	return dao.CreateIncRoot(root)
}

func GetSampleNodesById(id int64) (*SampleNode, error) {
	node, err := dao.GetSampleNodesById(id)
	if err != nil {
		logger.Errorf("[GetSampleNodesById] pgError: GetSampleNodesById failed, error:%v, id:%v", err, id)
		return nil, err
	}

	currentNode, err := getIncRuleFromJson(node.CurrentNodeJson)
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	predecessorNodes, err := getIncRulesFromJson(node.PredecessorNodesJson)
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	neighborNodes, err := getIncRulesFromJson(node.NeighborNodesJson)
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	//neighborConfs, err := getNeighborConfsFromJson(node.NeighborConfsJson)
	//if err != nil {
	//	logger.Error(err)
	//	return nil, err
	//}
	neighborCDF, err := getNeighborCDFFromJson(node.NeighborCDFJson)
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	sampleNode := &SampleNode{
		CoverRadius:      node.CoverRadius,
		CurrentNode:      currentNode,
		PredecessorNodes: predecessorNodes,
		NeighborNodes:    neighborNodes,
		MinConf:          node.MinConfidence,
		MaxConf:          node.MaxConfidence,
		//NeighborConfs:    neighborConfs,
		NeighborCDF: neighborCDF,
	}

	return sampleNode, nil
}

func GetLatestTaskExtendField(taskId int64) (*IncRdsTaskExtendField, error) {
	taskExtend, err := dao.GetLatestExtendByTaskId(taskId)
	if err != nil {
		logger.Errorf("[GetLatestTaskExtendField] dao.GetLatestExtendByTaskId failed, error:%v, taskId:%v", err, taskId)
		return nil, err
	}
	field, err := getTaskExtendFieldFromJson(taskExtend.ExtendJson)
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	return field, err
}

func WriteTaskExtendToDB(taskId int64, field *IncRdsTaskExtendField) error {
	fieldJson, err := getJsonFromTaskExtendField(field)
	if err != nil {
		logger.Error(err)
		return err
	}
	currentTime := time.Now().UnixMilli()
	taskExtend := &dao.IncRdsTaskExtend{
		TaskId:     taskId,
		ExtendJson: fieldJson,
		CreateTime: currentTime,
		UpdateTime: currentTime,
	}
	err = dao.CreateIncRdsTaskExtend(taskExtend)
	if err != nil {
		logger.Errorf("[WriteTaskExtendToDB] dao.CreateIncRdsTaskExtend failed, error:%v, taskId:%v, field:%v", err, taskId, *field)
	}
	return err
}

func getJsonFromTaskExtendField(field *IncRdsTaskExtendField) (string, error) {
	dataBytes, err := json.Marshal(field)
	if err != nil {
		logger.Errorf("[getJsonFromTaskExtendField] json.Marshal failed, error:%v, field:%v", err, *field)
		return "", nil
	}
	return string(dataBytes), nil
}

func getTaskExtendFieldFromJson(fieldJson string) (*IncRdsTaskExtendField, error) {
	field := &IncRdsTaskExtendField{}
	err := json.Unmarshal([]byte(fieldJson), field)
	if err != nil {
		logger.Errorf("[getTaskExtendFieldFromJson] json.Unmarshal failed, error:%v, json:%v", err, fieldJson)
		return nil, err
	}
	return field, nil
}

func GetDecisionTreeRules(taskId int64) ([]*rds.Rule, error) {
	rules := make([]*rds.Rule, 0)
	infos, err := po.SelectRulesByTaskId(taskId)
	if err != nil {
		logger.Errorf("[GetDecisionTreeRules] pg error: po.SelectRulesByTaskId failed, taskId:%v, error:%v", taskId, err)
		return nil, err
	}
	for _, info := range infos {
		rule := utils.GetFormatRule(info.ReeJson)
		rules = append(rules, &rule)
	}
	return rules, nil
}
