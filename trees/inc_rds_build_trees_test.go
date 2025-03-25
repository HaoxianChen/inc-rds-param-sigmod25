package trees

import (
	"gitlab.grandhoo.com/rock/rock-share/base/config"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/global/db"
	"gitlab.grandhoo.com/rock/rock_v3/inc_rule_dig"
	"testing"
)

func Test_sampleNodes(t *testing.T) {
	sampleNode1 := &inc_rule_dig.SampleNode{MinConf: 0.2, MaxConf: 0.8}
	sampleNode2 := &inc_rule_dig.SampleNode{MinConf: 0.3, MaxConf: 0.85}
	sampleNode3 := &inc_rule_dig.SampleNode{MinConf: 0.4, MaxConf: 0.9}
	sampleNodes := []*inc_rule_dig.SampleNode{sampleNode1, sampleNode2, sampleNode3}
	for i, sampleNode := range sampleNodes {
		if i == 0 {
			sampleNode.MaxConf = 0.75
			break
		}
	}
	t.Log("aaa")
}

func Test_mergeSampleNodes(t *testing.T) {
	config.InitConfig1()
	all := config.All
	l := all.Logger
	s := all.Server
	logger.InitLogger(l.Level, "rock", l.Path, l.MaxAge, l.RotationTime, l.RotationSize, s.SentryDsn)
	db.InitGorm()

	preTaskId := int64(1000032)
	preSampleNodes, err := inc_rule_dig.GetSampleNodesByTaskId(preTaskId)
	if err != nil {
		logger.Errorf("[Test_mergeSampleNodes] GetSampleNodesByTaskId error:%v, taskId:%v", err, preTaskId)
		return
	}
	logger.Infof("preSampleNodes size:%v", len(preSampleNodes))

	mergedCount := 0
	rhsKey2SampleNodes, rhsKey2Rhs := inc_rule_dig.SampleNodesGroupByRhs(preSampleNodes)
	for key, nodes := range rhsKey2SampleNodes {
		//if key != "t0.Risk=t1.Risk" {
		//	continue
		//}
		rhs := rhsKey2Rhs[key]
		layer2SampleNodes := make(map[int][]*inc_rule_dig.SampleNode)

		for _, sampleNode := range nodes {
			mergeSampleNodes(layer2SampleNodes, sampleNode)
		}

		sampleNodes := make([]*inc_rule_dig.SampleNode, 0)
		for _, sampleNode := range layer2SampleNodes {
			sampleNodes = append(sampleNodes, sampleNode...)
		}
		//logger.Infof("sampleNodes size:%v", len(sampleNodes))
		mergedCount += len(sampleNodes)

		inc_rule_dig.PrintRhsSampleNodes(rhs, sampleNodes)
	}
	logger.Infof("mergedSampleNodes size:%v", mergedCount)
}

func Test_getOldSampleNodes(t *testing.T) {
	config.InitConfig1()
	all := config.All
	l := all.Logger
	s := all.Server
	logger.InitLogger(l.Level, "rock", l.Path, l.MaxAge, l.RotationTime, l.RotationSize, s.SentryDsn)
	db.InitGorm()

	preTaskId := int64(1000032)
	preSampleNodes, err := inc_rule_dig.GetSampleNodesByTaskId(preTaskId)
	if err != nil {
		logger.Errorf("[Test_mergeSampleNodes] GetSampleNodesByTaskId error:%v, taskId:%v", err, preTaskId)
		return
	}
	logger.Infof("preSampleNodes size:%v", len(preSampleNodes))

	mergedCount := 0
	rhsKey2SampleNodes, rhsKey2Rhs := inc_rule_dig.SampleNodesGroupByRhs(preSampleNodes)
	for key, nodes := range rhsKey2SampleNodes {
		rhs := rhsKey2Rhs[key]
		mergedCount += len(nodes)

		inc_rule_dig.PrintRhsSampleNodes(rhs, nodes)
	}
	logger.Infof("sampleNodes size:%v", mergedCount)
}
