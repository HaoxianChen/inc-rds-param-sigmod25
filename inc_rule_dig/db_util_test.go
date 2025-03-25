package inc_rule_dig

import (
	"encoding/json"
	"gitlab.grandhoo.com/rock/rock-share/base/config"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/global/db"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables/table_data"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables/task_tree"
	"gitlab.grandhoo.com/rock/rock_v3/inc_rule_dig/dao"
	"gitlab.grandhoo.com/rock/rock_v3/request"
	"gitlab.grandhoo.com/rock/rock_v3/utils/storage_utils"
	"gitlab.grandhoo.com/rock/storage/config/storage_initiator"
	"testing"
)

func Test_SampleNodesJson(t *testing.T) {
	node1 := &task_tree.TaskTree{GiniIndex: 0.25}
	node2 := &task_tree.TaskTree{GiniIndex: 0.35}
	incRule1 := &IncRule{Node: node1, RuleType: 0, CR: 0.8, FTR: 0.8}
	incRule2 := &IncRule{Node: node2, RuleType: 1, CR: 0.9, FTR: 0.9}
	sampleNode := &SampleNode{
		CoverRadius:      1,
		CurrentNode:      incRule1,
		PredecessorNodes: []*IncRule{incRule1, incRule2},
		NeighborNodes:    []*IncRule{incRule1, incRule2},
		MinConf:          0.6,
		MaxConf:          0.9,
	}

	currentNodeJson, err := json.Marshal(sampleNode.CurrentNode)
	if err != nil {
		t.Logf("currentNodeJson: json.Marshal failed")
		return
	}
	t.Logf("currentNodeJson: %v", string(currentNodeJson))

	predecessorNodesJson, err := json.Marshal(sampleNode.PredecessorNodes)
	if err != nil {
		t.Logf("predecessorNodesJson: json.Marshal failed")
		return
	}
	t.Logf("predecessorNodesJson: %v", string(predecessorNodesJson))

	currentNode := &IncRule{}
	err = json.Unmarshal(currentNodeJson, currentNode)
	if err != nil {
		t.Logf("currentNode: json.Unmarshal failed")
	}
	t.Logf("currentNode: CR:%v, FTR:%v, currentNode:%v", currentNode.CR, currentNode.FTR, *currentNode)

	var predecessorNodes []*IncRule
	err = json.Unmarshal(predecessorNodesJson, &predecessorNodes)
	if err != nil {
		t.Logf("predecessorNodes: json.Unmarshal failed")
	}
	t.Logf("predecessorNodes: nodeLen:%v, CR:%v, FTR:%v", len(predecessorNodes), predecessorNodes[0].CR, predecessorNodes[0].FTR)

}

func Test_GetLatestIncRdsTaskByIndicator(t *testing.T) {
	config.InitConfig1()
	all := config.All
	l := all.Logger
	s := all.Server
	logger.InitLogger(l.Level, "rock", l.Path, l.MaxAge, l.RotationTime, l.RotationSize, s.SentryDsn)
	db.InitGorm()

	support := 0.2
	confidence := 0.8
	task, err := dao.GetLatestIncRdsTaskByIndicator(support, confidence)
	if err != nil {
		t.Error(err)
		return
	}
	t.Logf("taskId:%v", task.Id)
}

func Test_GetLatestIncRdsTask(t *testing.T) {
	config.InitConfig1()
	all := config.All
	l := all.Logger
	s := all.Server
	logger.InitLogger(l.Level, "rock", l.Path, l.MaxAge, l.RotationTime, l.RotationSize, s.SentryDsn)
	db.InitGorm()

	task, err := dao.GetLatestIncRdsTask()
	if err != nil {
		t.Error(err)
		return
	}
	t.Logf("taskId:%v", task.Id)
}

func Test_GetIncRdsTaskById(t *testing.T) {
	config.InitConfig1()
	all := config.All
	l := all.Logger
	s := all.Server
	logger.InitLogger(l.Level, "rock", l.Path, l.MaxAge, l.RotationTime, l.RotationSize, s.SentryDsn)
	db.InitGorm()

	taskId := int64(1000051)

	task, err := dao.GetIncRdsTaskById(taskId)
	if err != nil {
		t.Error(err)
		return
	}
	t.Logf("support:%v, confidence:%v", task.Support, task.Confidence)
}

func Test_containAll(t *testing.T) {
	slice1 := []string{"cherry", "apple", "banana"}
	slice2 := []string{"apple", "banana"}

	t.Logf("%v", ContainsAll(slice1, slice2))
}

func Test_slice1ContainsSlice2(t *testing.T) {
	slice1 := []string{"cherry", "apple", "banana"}
	slice2 := []string{"apple", "banana"}

	t.Logf("%v", Contains(slice1, slice2))
	t.Logf("%v", Contains(slice2, slice1))
}

func Test_SamplesSize(t *testing.T) {
	config.InitConfig1()
	all := config.All
	l := all.Logger
	s := all.Server
	logger.InitLogger(l.Level, "rock", l.Path, l.MaxAge, l.RotationTime, l.RotationSize, s.SentryDsn)
	db.InitGorm()

	taskId := int64(1000267)

	samples, err := GetSampleNodesByTaskId(taskId)
	if err != nil {
		logger.Error(err)
		return
	}

	size := GetSampleNodesSize(samples)

	sizeMB := float64(size) / (1024 * 1024)
	logger.Infof("task:%v samples size:%v, MB:%v", taskId, size, sizeMB)
}

func Test_load_data(t *testing.T) {
	config.InitConfig1()
	all := config.All
	l := all.Logger
	s := all.Server
	logger.InitLogger(l.Level, "rock", l.Path, l.MaxAge, l.RotationTime, l.RotationSize, s.SentryDsn)
	db.InitGorm()
	storage_initiator.InitStorage()

	// 可能需要导入表
	source := request.DataSource{
		Database:     "postgres",
		Host:         "127.0.0.1",
		Port:         5432,
		DatabaseName: "xxxx",
		TableName:    "inc_rds.adult",
		User:         "xxxx",
		Password:     "xxxx",
	}
	limit := 20
	taskId := int64(1005261)
	tableId := storage_utils.ImportTable(&source)
	tableIds := []string{tableId}

	table_data.LoadSchema(taskId, tableIds)
	//table_data.SetBlockingInfo(taskId, UDFTabCols)
	table_data.LoadDataCreatePli(taskId, tableIds)
	table_data.CreateSamplePLI(taskId, limit)
	//storage_utils.DropCacheDataBase()
	table_data.CreateIndex(taskId)
	table_data.CreateSampleIndex(taskId, limit)

	task := table_data.GetTask(taskId)
	logger.Infof("samplePLI:%v", task.SamplePLI)
	logger.Infof("sampleTableIndex:%v", task.TableIndexValues)
	logger.Infof("sampleIndexPLI:%v", task.SampleIndexPLI)
}
