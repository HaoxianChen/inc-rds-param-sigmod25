package rule_dig

import (
	"encoding/json"
	"errors"
	"fmt"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/distribute"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock-share/rpc/grpc/pb"
	"gitlab.grandhoo.com/rock/rock_v3/calculate"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables/table_data"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables/task_tree"
	"gitlab.grandhoo.com/rock/rock_v3/inc_rule_dig"
	"gitlab.grandhoo.com/rock/rock_v3/rds_config"
	"gitlab.grandhoo.com/rock/rock_v3/request"
	"gitlab.grandhoo.com/rock/rock_v3/request/udf"
	"gitlab.grandhoo.com/rock/rock_v3/trees"
	"gitlab.grandhoo.com/rock/rock_v3/utils"
	"gitlab.grandhoo.com/rock/rock_v3/utils/blocking/udf_column"
	"gitlab.grandhoo.com/rock/rock_v3/utils/db_util"
	"gitlab.grandhoo.com/rock/rock_v3/utils/storage_utils"
	utils2 "gitlab.grandhoo.com/rock/storage/storage2/utils"
	"math"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"
)

func IncDigRulesDistribute(req *inc_rule_dig.IncRdsRequest) (*inc_rule_dig.IncRdsResponse, error) {
	if req.PreSupport == -1 && req.PreConfidence == -1 {
		taskId, err := inc_rule_dig.InsertIncRdsTaskToDB(req.UpdatedSupport, req.UpdatedConfidence)
		if err != nil {
			logger.Errorf("[IncDigRulesDistribute] InsertIncRdsTaskToDB error:%v, support:%v, confidence:%v", err, req.UpdatedSupport, req.UpdatedConfidence)
			return nil, err
		}
		req.TaskID = taskId
		// 执行batchMiner
		output, err := batchMiner(req)
		if err != nil {
			logger.Error(err)
			return nil, err
		}
		//// 调试方法
		//inc_rule_dig.PrintSampleNodes(output.SampleNodes)

		//output.MinimalIncRules = getMinimizeProcessRulesNew(output.MinimalIncRules)

		err = inc_rule_dig.WriteMinimalIncRulesToDB(req.TaskID, output.MinimalIncRules)
		if err != nil {
			logger.Error(err)
			return nil, err
		}
		err = inc_rule_dig.WritePrunedIncRulesToDB(req.TaskID, output.PrunedIncRules)
		if err != nil {
			logger.Error(err)
			return nil, err
		}
		err = inc_rule_dig.WriteSampleNodesToDB(req.TaskID, output.SampleNodes)
		if err != nil {
			logger.Error(err)
			return nil, err
		}

		output.MinimalIncRules = getMinimizeProcessRulesNew(output.MinimalIncRules)
		outputSize, minimalSize, auxiliarySize, decisionTreeSize, decisionTreeRules := getOutputMemorySizeNew(output.MinimalIncRules, output.PrunedIncRules, output.SampleNodes, req.TaskID, true, false)

		var goldenRecallRate float64
		if len(req.GoldenRules) > 0 {
			goldenRecallRate = getRecallRateOfGuidelines(output.MinimalIncRules, decisionTreeRules, req.GoldenRules)
		}
		dropTaskData(taskId)

		return &inc_rule_dig.IncRdsResponse{
			TotalTime:            output.TotalTime,
			PreTaskId:            0,
			UpdatedTaskId:        taskId,
			MinimalREEsSize:      len(output.MinimalIncRules),
			PruneREEsSize:        len(output.PrunedIncRules),
			SampleSize:           len(output.SampleNodes),
			PreSupport:           req.PreSupport,
			PreConfidence:        req.PreConfidence,
			UpdatedSupport:       req.UpdatedSupport,
			UpdatedConfidence:    req.UpdatedConfidence,
			OutputSize:           outputSize,
			AuxiliarySize:        auxiliarySize,
			MinimalSize:          minimalSize,
			DecisionTreeREEsSize: len(decisionTreeRules),
			DecisionTreeSize:     decisionTreeSize,
			GoldenRecallRate:     goldenRecallRate,
		}, nil
	}

	preTaskId := req.PreTaskID
	if preTaskId > 0 {
		preTask, err := inc_rule_dig.GetIncRdsTaskById(preTaskId)
		if err != nil {
			logger.Error(err)
			return nil, err
		}
		req.PreSupport = preTask.Support
		req.PreConfidence = preTask.Confidence
	} else if req.PreSupport == 0 && req.PreConfidence == 0 {
		// 没有输入旧的参数，就找最新的task作为旧的参数
		preTask, err := inc_rule_dig.GetLatestIncRdsTask()
		if err != nil {
			logger.Error(err)
			return nil, err
		}
		preTaskId = preTask.Id
		req.PreSupport = preTask.Support
		req.PreConfidence = preTask.Confidence
	} else {
		var err error
		preTaskId, err = inc_rule_dig.GetTaskIdByIndicator(req.PreSupport, req.PreConfidence)
		if err != nil {
			logger.Errorf("[IncDigRulesDistribute] GetTaskIdByIndicator error:%v, support:%v, confidence:%v", err, req.PreSupport, req.PreConfidence)
			return nil, err
		}
	}
	logger.Infof("[IncDigRulesDistribute] preTaskId:%v, preSupport:%v, preConfidence:%v", preTaskId, req.PreSupport, req.PreConfidence)

	supportChange := req.UpdatedSupport - req.PreSupport
	confidenceChange := req.UpdatedConfidence - req.PreConfidence
	if supportChange == 0 && confidenceChange == 0 {
		errMsg := fmt.Sprint("The parameters have not been updated, so there is no need to execute rule discovery.")
		logger.Warn(errMsg)
		return nil, errors.New(errMsg)
	}

	taskId, err := inc_rule_dig.InsertIncRdsTaskToDB(req.UpdatedSupport, req.UpdatedConfidence)
	if err != nil {
		logger.Errorf("[IncDigRulesDistribute] InsertIncRdsTaskToDB error:%v, support:%v, confidence:%v", err, req.UpdatedSupport, req.UpdatedConfidence)
		return nil, err
	}

	logger.Infof("[IncDigRulesDistribute] updatedTaskId:%v, updatedSupport:%v, updatedConfidence:%v", taskId, req.UpdatedSupport, req.UpdatedConfidence)
	req.TaskID = taskId

	if req.WithoutSampling {
		if (supportChange >= 0 && confidenceChange < 0) || (supportChange < 0 && confidenceChange < 0) {
			logger.Infof("[IncDigRulesDistribute] without sampling, execute batchMiner, supportChange:%v, confidenceChange:%v", supportChange, confidenceChange)
			// 执行batchMiner
			output, err := batchMiner(req)
			if err != nil {
				logger.Error(err)
				return nil, err
			}
			//// 调试方法
			//inc_rule_dig.PrintSampleNodes(output.SampleNodes)
			//output.MinimalIncRules = getMinimizeProcessRulesNew(output.MinimalIncRules)

			err = inc_rule_dig.WriteMinimalIncRulesToDB(req.TaskID, output.MinimalIncRules)
			if err != nil {
				logger.Error(err)
				return nil, err
			}
			err = inc_rule_dig.WritePrunedIncRulesToDB(req.TaskID, output.PrunedIncRules)
			if err != nil {
				logger.Error(err)
				return nil, err
			}
			err = inc_rule_dig.WriteSampleNodesToDB(req.TaskID, output.SampleNodes)
			if err != nil {
				logger.Error(err)
				return nil, err
			}

			output.MinimalIncRules = getMinimizeProcessRulesNew(output.MinimalIncRules)
			outputSize, minimalSize, auxiliarySize, decisionTreeSize, decisionTreeRules := getOutputMemorySizeNew(output.MinimalIncRules, output.PrunedIncRules, output.SampleNodes, req.TaskID, false, req.WithoutSampling)

			var goldenRecallRate float64
			if len(req.GoldenRules) > 0 {
				goldenRecallRate = getRecallRateOfGuidelines(output.MinimalIncRules, decisionTreeRules, req.GoldenRules)
			}
			dropTaskData(taskId)

			return &inc_rule_dig.IncRdsResponse{
				TotalTime:            output.TotalTime,
				PreTaskId:            preTaskId,
				UpdatedTaskId:        taskId,
				MinimalREEsSize:      len(output.MinimalIncRules),
				PruneREEsSize:        len(output.PrunedIncRules),
				SampleSize:           len(output.SampleNodes),
				PreSupport:           req.PreSupport,
				PreConfidence:        req.PreConfidence,
				UpdatedSupport:       req.UpdatedSupport,
				UpdatedConfidence:    req.UpdatedConfidence,
				OutputSize:           outputSize,
				AuxiliarySize:        auxiliarySize,
				MinimalSize:          minimalSize,
				DecisionTreeREEsSize: len(decisionTreeRules),
				DecisionTreeSize:     decisionTreeSize,
				GoldenRecallRate:     goldenRecallRate,
			}, nil
		}
	}

	gv := global_variables.InitGlobalV(taskId)
	tableName2Id, err := setConfOfInc(gv, req)
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	loadData(gv.TableIds, []udf.UDFTabCol{}, taskId, req.DecisionTreeMaxRowSize)
	gv.ChEndpoint, err = getChanEndpoint()
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	if gv.UseDecisionTree {
		for _, tableId := range gv.TableIds {
			//generateDecisionTreeY(tableId, gv)
			generateDecisionTreeYNew(tableId, gv)
		}
		//单行规则用决策树挖掘
		t := time.Now().UnixMilli()
		singleRowRuleDig(gv)
		singleRowRuleTime := time.Now().UnixMilli() - t
		logger.Infof("taskid:%v,单行规则执行时间:%vms, 发现的规则数:%v", gv.TaskId, singleRowRuleTime, gv.SingleRuleSize)
	}

	if supportChange != 0 && confidenceChange == 0 {
		inputParam := &inc_rule_dig.IncMinerSupportInput{
			PreTaskId:       preTaskId,
			UpdateIndicator: inc_rule_dig.Indicator{Support: req.UpdatedSupport, Confidence: req.UpdatedConfidence},
			UpdateTaskId:    taskId,
			SupportChange:   supportChange,
			Gv:              gv,
			TableName2Id:    tableName2Id,
		}
		outputParam, err := incMinerSupport(inputParam)
		if err != nil {
			logger.Error(err)
			// 任务运行有报错，把之前插入的task从DB删除
			inc_rule_dig.DeleteIncRdsTask(taskId)
			return nil, err
		}

		//outputParam.MinimalIncRules = getMinimizeProcessRulesNew(outputParam.MinimalIncRules)

		err = inc_rule_dig.WriteMinimalIncRulesToDB(taskId, outputParam.MinimalIncRules)
		if err != nil {
			logger.Error(err)
			inc_rule_dig.DeleteIncRdsTask(taskId)
			return nil, err
		}
		err = inc_rule_dig.WritePrunedIncRulesToDB(taskId, outputParam.PrunedIncRules)
		if err != nil {
			logger.Error(err)
			inc_rule_dig.DeleteIncRdsTask(taskId)
			return nil, err
		}

		err = inc_rule_dig.WriteSampleNodesToDB(taskId, outputParam.SampleNodes)
		if err != nil {
			logger.Error(err)
			inc_rule_dig.DeleteIncRdsTask(taskId)
			return nil, err
		}

		outputParam.MinimalIncRules = getMinimizeProcessRulesNew(outputParam.MinimalIncRules)
		outputSize, minimalSize, auxiliarySize, decisionTreeSize, decisionTreeRules := getOutputMemorySizeNew(outputParam.MinimalIncRules, outputParam.PrunedIncRules, outputParam.SampleNodes, taskId, false, req.WithoutSampling)

		var goldenRecallRate float64
		if len(req.GoldenRules) > 0 {
			goldenRecallRate = getRecallRateOfGuidelines(outputParam.MinimalIncRules, decisionTreeRules, req.GoldenRules)
		}
		dropTaskData(taskId)

		return &inc_rule_dig.IncRdsResponse{
			TotalTime:            outputParam.TotalTime,
			PreTaskId:            preTaskId,
			UpdatedTaskId:        taskId,
			MinimalREEsSize:      len(outputParam.MinimalIncRules),
			PruneREEsSize:        len(outputParam.PrunedIncRules),
			SampleSize:           len(outputParam.SampleNodes),
			PreSupport:           req.PreSupport,
			PreConfidence:        req.PreConfidence,
			UpdatedSupport:       req.UpdatedSupport,
			UpdatedConfidence:    req.UpdatedConfidence,
			OutputSize:           outputSize,
			AuxiliarySize:        auxiliarySize,
			MinimalSize:          minimalSize,
			DecisionTreeREEsSize: len(decisionTreeRules),
			DecisionTreeSize:     decisionTreeSize,
			GoldenRecallRate:     goldenRecallRate,
		}, nil
	} else if supportChange == 0 && confidenceChange != 0 {
		inputParam := &inc_rule_dig.IncMinerConfidenceInput{
			PreTaskId:        preTaskId,
			UpdateIndicator:  inc_rule_dig.Indicator{Support: req.UpdatedSupport, Confidence: req.UpdatedConfidence},
			UpdateTaskId:     taskId,
			ConfidenceChange: confidenceChange,
			Gv:               gv,
			CoverRadius:      req.CoverRadius,
			RecallBound:      req.RecallBound,
			UseCDF:           req.UseCDF,
			WithoutSampling:  req.WithoutSampling,
			TableName2Id:     tableName2Id,
		}
		outputParam, err := incMinerConfidence(inputParam)
		if err != nil {
			logger.Error(err)
			inc_rule_dig.DeleteIncRdsTask(taskId)
			return nil, err
		}

		//outputParam.MinimalIncRules = getMinimizeProcessRulesNew(outputParam.MinimalIncRules)

		err = inc_rule_dig.WriteMinimalIncRulesToDB(taskId, outputParam.MinimalIncRules)
		if err != nil {
			logger.Error(err)
			inc_rule_dig.DeleteIncRdsTask(taskId)
			return nil, err
		}
		err = inc_rule_dig.WriteSampleNodesToDB(taskId, outputParam.UpdatedSampleNodes)
		if err != nil {
			logger.Error(err)
			inc_rule_dig.DeleteIncRdsTask(taskId)
			return nil, err
		}

		err = inc_rule_dig.WritePrunedIncRulesToDB(taskId, outputParam.PrunedIncRules)
		if err != nil {
			logger.Error(err)
			inc_rule_dig.DeleteIncRdsTask(taskId)
			return nil, err
		}

		// 获取召回率
		//recallRate := getRecallRate(outputParam.MinimalIncRules, req.RecallRateBatchMinerTaskId)
		recallRate := getRecallRateNew(outputParam.MinimalIncRules, req.RecallRateBatchMinerTaskId)

		outputParam.MinimalIncRules = getMinimizeProcessRulesNew(outputParam.MinimalIncRules)
		outputSize, minimalSize, auxiliarySize, decisionTreeSize, decisionTreeRules := getOutputMemorySizeNew(outputParam.MinimalIncRules, outputParam.PrunedIncRules, outputParam.UpdatedSampleNodes, taskId, false, req.WithoutSampling)

		var goldenRecallRate float64
		if len(req.GoldenRules) > 0 {
			goldenRecallRate = getRecallRateOfGuidelines(outputParam.MinimalIncRules, decisionTreeRules, req.GoldenRules)
		}
		dropTaskData(taskId)

		return &inc_rule_dig.IncRdsResponse{
			TotalTime:            outputParam.TotalTime,
			PreTaskId:            preTaskId,
			UpdatedTaskId:        taskId,
			MinimalREEsSize:      len(outputParam.MinimalIncRules),
			PruneREEsSize:        len(outputParam.PrunedIncRules),
			SampleSize:           len(outputParam.UpdatedSampleNodes),
			RecallRate:           recallRate,
			PreSupport:           req.PreSupport,
			PreConfidence:        req.PreConfidence,
			UpdatedSupport:       req.UpdatedSupport,
			UpdatedConfidence:    req.UpdatedConfidence,
			OutputSize:           outputSize,
			AuxiliarySize:        auxiliarySize,
			MinimalSize:          minimalSize,
			DecisionTreeREEsSize: len(decisionTreeRules),
			DecisionTreeSize:     decisionTreeSize,
			GoldenRecallRate:     goldenRecallRate,
		}, nil
	} else {
		inputParam := &inc_rule_dig.IncMinerInput{
			PreTaskId:        preTaskId,
			UpdateIndicator:  inc_rule_dig.Indicator{Support: req.UpdatedSupport, Confidence: req.UpdatedConfidence},
			UpdateTaskId:     taskId,
			SupportChange:    supportChange,
			ConfidenceChange: confidenceChange,
			Gv:               gv,
			CoverRadius:      req.CoverRadius,
			RecallBound:      req.RecallBound,
			UseCDF:           req.UseCDF,
			WithoutSampling:  req.WithoutSampling,
			TableName2Id:     tableName2Id,
		}
		outputParam, err := incMinerIndicator(inputParam)
		if err != nil {
			logger.Error(err)
			inc_rule_dig.DeleteIncRdsTask(taskId)
			return nil, err
		}

		//outputParam.MinimalIncRules = getMinimizeProcessRulesNew(outputParam.MinimalIncRules)

		err = inc_rule_dig.WriteMinimalIncRulesToDB(taskId, outputParam.MinimalIncRules)
		if err != nil {
			logger.Error(err)
			inc_rule_dig.DeleteIncRdsTask(taskId)
			return nil, err
		}
		err = inc_rule_dig.WritePrunedIncRulesToDB(taskId, outputParam.PrunedIncRules)
		if err != nil {
			logger.Error(err)
			inc_rule_dig.DeleteIncRdsTask(taskId)
			return nil, err
		}
		err = inc_rule_dig.WriteSampleNodesToDB(taskId, outputParam.UpdatedSampleNodes)
		if err != nil {
			logger.Error(err)
			inc_rule_dig.DeleteIncRdsTask(taskId)
			return nil, err
		}

		// 获取召回率
		//recallRate := getRecallRate(outputParam.MinimalIncRules, req.RecallRateBatchMinerTaskId)
		recallRate := getRecallRateNew(outputParam.MinimalIncRules, req.RecallRateBatchMinerTaskId)

		outputParam.MinimalIncRules = getMinimizeProcessRulesNew(outputParam.MinimalIncRules)
		outputSize, minimalSize, auxiliarySize, decisionTreeSize, decisionTreeRules := getOutputMemorySizeNew(outputParam.MinimalIncRules, outputParam.PrunedIncRules, outputParam.UpdatedSampleNodes, taskId, false, req.WithoutSampling)

		var goldenRecallRate float64
		if len(req.GoldenRules) > 0 {
			goldenRecallRate = getRecallRateOfGuidelines(outputParam.MinimalIncRules, decisionTreeRules, req.GoldenRules)
		}
		dropTaskData(taskId)

		return &inc_rule_dig.IncRdsResponse{
			TotalTime:            outputParam.TotalTime,
			PreTaskId:            preTaskId,
			UpdatedTaskId:        taskId,
			MinimalREEsSize:      len(outputParam.MinimalIncRules),
			PruneREEsSize:        len(outputParam.PrunedIncRules),
			SampleSize:           len(outputParam.UpdatedSampleNodes),
			RecallRate:           recallRate,
			PreSupport:           req.PreSupport,
			PreConfidence:        req.PreConfidence,
			UpdatedSupport:       req.UpdatedSupport,
			UpdatedConfidence:    req.UpdatedConfidence,
			OutputSize:           outputSize,
			AuxiliarySize:        auxiliarySize,
			MinimalSize:          minimalSize,
			DecisionTreeREEsSize: len(decisionTreeRules),
			DecisionTreeSize:     decisionTreeSize,
			GoldenRecallRate:     goldenRecallRate,
		}, nil
	}
}

func incMinerSupport(inputParam *inc_rule_dig.IncMinerSupportInput) (*inc_rule_dig.IncMinerSupportOutput, error) {
	logger.Infof("[incMinerSupport] start incMinerSupport, preTaskId:%v, taskId:%v, supportChange:%v", inputParam.PreTaskId, inputParam.UpdateTaskId, inputParam.SupportChange)
	oldTaskExtend, err := inc_rule_dig.GetLatestTaskExtendField(inputParam.PreTaskId)
	if err != nil {
		logger.Errorf("[IncMinerIndicator] GetLatestTaskExtendField error:%v, taskId:%v", err, inputParam.PreTaskId)
		return nil, err
	}

	// 获取preIndicator下的REE集合
	preMinimalIncRules, err := inc_rule_dig.GetMinimalIncRulesByTaskId(inputParam.PreTaskId)
	if err != nil {
		logger.Errorf("[IncMinerSupport] GetRulesByTaskId error:%v, taskId:%v", err, inputParam.PreTaskId)
		return nil, err
	}
	//convertIncRules(preMinimalIncRules, inputParam.Gv.TableIds[0])
	convertIncRulesNew(preMinimalIncRules, oldTaskExtend.TableId2Name, inputParam.TableName2Id)

	// 获取preIndicator被剪枝的nodes
	prePrunedIncRules, err := inc_rule_dig.GetPrunedIncRulesByTaskId(inputParam.PreTaskId)
	if err != nil {
		logger.Errorf("[IncMinerSupport] GetPruneTaskTreesByTaskId error:%v, taskId:%v", err, inputParam.PreTaskId)
		return nil, err
	}
	//convertIncRules(prePrunedIncRules, inputParam.Gv.TableIds[0])
	convertIncRulesNew(prePrunedIncRules, oldTaskExtend.TableId2Name, inputParam.TableName2Id)

	// 获取preIndicator的samples
	preSamples, err := inc_rule_dig.GetSampleNodesByTaskId(inputParam.PreTaskId)
	if err != nil {
		logger.Errorf("[IncMinerSupport] GetSampleNodesByTaskId error:%v, taskId:%v", err, inputParam.PreTaskId)
		return nil, err
	}
	convertSampleNodesNew(preSamples, oldTaskExtend.TableId2Name, inputParam.TableName2Id)

	startTime := time.Now().UnixMilli()

	minimalIncRules := make([]*inc_rule_dig.IncRule, 0)

	if inputParam.SupportChange > 0 {
		for _, preMinimalIncRule := range preMinimalIncRules {
			if inputParam.UpdateIndicator.Support <= preMinimalIncRule.CR && inputParam.UpdateIndicator.Confidence <= preMinimalIncRule.FTR {
				minimalIncRules = append(minimalIncRules, preMinimalIncRule)
			}
		}
		totalTime := time.Now().UnixMilli() - startTime
		logger.Infof("[incMinerSupport] finish incMinerSupport, taskId:%v, supportChange:%v, totalTime:%v", inputParam.UpdateTaskId, inputParam.SupportChange, totalTime)
		return &inc_rule_dig.IncMinerSupportOutput{
			MinimalIncRules: minimalIncRules,
			PrunedIncRules:  prePrunedIncRules,
			SampleNodes:     preSamples,
			TotalTime:       totalTime,
		}, nil
	} else {
		minimalIncRules = append(minimalIncRules, preMinimalIncRules...)
		prunedIncRules := make([]*inc_rule_dig.IncRule, 0)

		// 剪枝的taskTrees按rhs分组
		rhsKey2IncRules, rhsKey2Rhs := incRuleGroupByRhs(prePrunedIncRules)

		var wg sync.WaitGroup
		mutex := sync.Mutex{}
		ch := make(chan struct{}, inputParam.Gv.TreeChanSize)
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
				expandInput := &inc_rule_dig.IncExpandSupportInput{
					Rhs:                rhsKey2Rhs[rhsKey],
					NeedExpandIncRules: layer2IncRules,
					Gv:                 inputParam.Gv,
				}
				expandOutput := trees.IncExpandSupport(expandInput)
				mutex.Lock()
				minimalIncRules = append(minimalIncRules, expandOutput.MinimalIncRules...)
				prunedIncRules = append(prunedIncRules, expandOutput.PrunedIncRules...)
				mutex.Unlock()
			}(rhsKey, incRules)
		}
		wg.Wait()
		totalTime := time.Now().UnixMilli() - startTime
		logger.Infof("[incMinerSupport] finish incMinerSupport, taskId:%v, supportChange:%v, totalTime:%v", inputParam.UpdateTaskId, inputParam.SupportChange, totalTime)
		return &inc_rule_dig.IncMinerSupportOutput{
			MinimalIncRules: minimalIncRules,
			PrunedIncRules:  prunedIncRules,
			SampleNodes:     preSamples,
			TotalTime:       totalTime,
		}, nil
	}
}

func incMinerConfidence(inputParam *inc_rule_dig.IncMinerConfidenceInput) (*inc_rule_dig.IncMinerConfidenceOutput, error) {
	logger.Infof("[incMinerConfidence] start incMinerConfidence, preTaskId:%v, taskId:%v, confidenceChange:%v", inputParam.PreTaskId, inputParam.UpdateTaskId, inputParam.ConfidenceChange)
	minimalRules := make([]*inc_rule_dig.IncRule, 0)
	updatedSampleNodes := make([]*inc_rule_dig.SampleNode, 0)

	oldTaskExtend, err := inc_rule_dig.GetLatestTaskExtendField(inputParam.PreTaskId)
	if err != nil {
		logger.Errorf("[IncMinerIndicator] GetLatestTaskExtendField error:%v, taskId:%v", err, inputParam.PreTaskId)
		return nil, err
	}

	// 获取上一轮的采样节点(从pg)
	preSampleNodes, err := inc_rule_dig.GetSampleNodesByTaskId(inputParam.PreTaskId)
	if err != nil {
		logger.Errorf("[IncMinerConfidence] GetSampleNodesByTaskId error:%v, taskId:%v", err, inputParam.PreTaskId)
		return nil, err
	}
	//convertSampleNodes(preSampleNodes, inputParam.Gv.TableIds[0])
	convertSampleNodesNew(preSampleNodes, oldTaskExtend.TableId2Name, inputParam.TableName2Id)

	// 获取preIndicator下的REE集合
	preMinimalRules, err := inc_rule_dig.GetMinimalIncRulesByTaskId(inputParam.PreTaskId)
	if err != nil {
		logger.Errorf("[IncMinerConfidence] GetIncRulesByTaskId error:%v, taskId:%v", err, inputParam.PreTaskId)
		return nil, err
	}
	//convertIncRules(preMinimalRules, inputParam.Gv.TableIds[0])
	convertIncRulesNew(preMinimalRules, oldTaskExtend.TableId2Name, inputParam.TableName2Id)
	logger.Infof("[IncMinerConfidence] preSampleNodes size:%v, preMinimalRules size:%v", len(preSampleNodes), len(preMinimalRules))

	prePrunedRules, err := inc_rule_dig.GetPrunedIncRulesByTaskId(inputParam.PreTaskId)
	if err != nil {
		logger.Errorf("[IncMinerConfidence] GetPrunedIncRulesByTaskId error:%v, taskId:%v", err, inputParam.PreTaskId)
		return nil, err
	}
	convertIncRulesNew(prePrunedRules, oldTaskExtend.TableId2Name, inputParam.TableName2Id)

	startTime := time.Now().UnixMilli()

	if inputParam.ConfidenceChange > 0 {
		needExpandRules := make([]*inc_rule_dig.IncRule, 0)
		for _, preRule := range preMinimalRules {
			if preRule.FTR < inputParam.UpdateIndicator.Confidence {
				needExpandRules = append(needExpandRules, preRule)
			} else {
				minimalRules = append(minimalRules, preRule)
			}
		}
		rhsKey2IncRules, rhsKey2Rhs := incRuleGroupByRhs(needExpandRules)

		rhsKey2SampleNodes, notUseSampleNodes := getRhsKey2SampleNodes(rhsKey2IncRules, preSampleNodes)
		updatedSampleNodes = append(updatedSampleNodes, notUseSampleNodes...)

		var wg sync.WaitGroup
		mutex := sync.Mutex{}
		ch := make(chan struct{}, inputParam.Gv.TreeChanSize)
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
				layer2IncRules := incRuleGroupByLayer(incRules)
				//sampleNodesOfRhs := getSampleNodesByRhsKey(preSampleNodes, rhsKey)
				sampleNodesOfRhs := rhsKey2SampleNodes[rhsKey]
				expandInput := &inc_rule_dig.IncExpandConfidenceInput{
					Rhs:                rhsKey2Rhs[rhsKey],
					NeedExpandIncRules: layer2IncRules,
					SampleNodes:        sampleNodesOfRhs,
					Gv:                 inputParam.Gv,
					CoverRadius:        inputParam.CoverRadius,
					WithoutSampling:    inputParam.WithoutSampling,
				}
				//if expandInput.Rhs.PredicateStr == "t0.ZIP_Code=t1.ZIP_Code" {
				expandOutput := trees.IncExpandConfidence(expandInput)
				mutex.Lock()
				minimalRules = append(minimalRules, expandOutput.MinimalIncRules...)
				updatedSampleNodes = append(updatedSampleNodes, expandOutput.UpdateSampleNodes...)
				mutex.Unlock()
				//}
			}(rhsKey, incRules)
		}
		wg.Wait()
		totalTime := time.Now().UnixMilli() - startTime
		logger.Infof("[incMinerConfidence] finish incMinerConfidence, taskId:%v, confidenceChange:%v, totalTime:%v", inputParam.UpdateTaskId, inputParam.ConfidenceChange, totalTime)
		return &inc_rule_dig.IncMinerConfidenceOutput{
			MinimalIncRules:    minimalRules,
			UpdatedSampleNodes: updatedSampleNodes,
			PrunedIncRules:     prePrunedRules,
			TotalTime:          totalTime,
		}, nil
	} else {
		// support不变，confidence减小，上一轮minimal REEs一定符合新阈值
		minimalRules = append(minimalRules, preMinimalRules...)

		// TODO 补充sampleNodes排序功能(不排序也可以,后续再实现)
		var relaxSampleNodes []*inc_rule_dig.SampleNode
		if !inputParam.UseCDF || inputParam.RecallBound == 1.0 {
			relaxSampleNodes = preSampleNodes
		} else {
			relaxSampleNodes = getRelaxSampleNodes(preSampleNodes, inputParam.UpdateIndicator.Confidence, inputParam.RecallBound)
		}
		logger.Infof("[incMinerConfidence] relaxSampleNodes size:%v", len(relaxSampleNodes))

		////neighborNodes := make([]*inc_rule_dig.IncRule, 0)
		//for _, sampleNode := range relaxSampleNodes {
		//	if sampleNode.MaxConf >= inputParam.UpdateIndicator.Confidence {
		//		if sampleNode.CurrentNode.FTR >= inputParam.UpdateIndicator.Confidence {
		//			minimalRules = append(minimalRules, sampleNode.CurrentNode)
		//		}
		//		for _, neighborNode := range sampleNode.NeighborNodes {
		//			//neighborNodes = append(neighborNodes, neighborNode)
		//			if neighborNode.FTR >= inputParam.UpdateIndicator.Confidence {
		//				minimalRules = append(minimalRules, neighborNode)
		//			}
		//		}
		//	}
		//}
		////trees.CalcIncRulesForSample(neighborNodes, inputParam.Gv)
		////for _, neighborNode := range neighborNodes {
		////	if neighborNode.FTR >= inputParam.UpdateIndicator.Confidence {
		////		minimalRules = append(minimalRules, neighborNode)
		////	}
		////}
		tmpMinimalRules, err := calcSampleNeighbors(relaxSampleNodes, inputParam.UpdateIndicator, inputParam.Gv)
		if err != nil {
			logger.Error(err)
			return nil, err
		}
		minimalRules = append(minimalRules, tmpMinimalRules...)

		totalTime := time.Now().UnixMilli() - startTime
		logger.Infof("[incMinerConfidence] finish incMinerConfidence, taskId:%v, confidenceChange:%v, totalTime:%v", inputParam.UpdateTaskId, inputParam.ConfidenceChange, totalTime)
		return &inc_rule_dig.IncMinerConfidenceOutput{
			MinimalIncRules:    minimalRules,
			UpdatedSampleNodes: preSampleNodes,
			PrunedIncRules:     prePrunedRules,
			TotalTime:          totalTime,
		}, nil
	}
}

func incMinerIndicator(inputParam *inc_rule_dig.IncMinerInput) (*inc_rule_dig.IncMinerOutput, error) {
	logger.Infof("[incMinerIndicator] start incMinerIndicator, preTaskId:%v, taskId:%v, supportChange:%v, confidenceChange:%v", inputParam.PreTaskId, inputParam.UpdateTaskId, inputParam.SupportChange, inputParam.ConfidenceChange)
	oldTaskExtend, err := inc_rule_dig.GetLatestTaskExtendField(inputParam.PreTaskId)
	if err != nil {
		logger.Errorf("[IncMinerIndicator] GetLatestTaskExtendField error:%v, taskId:%v", err, inputParam.PreTaskId)
		return nil, err
	}

	preMinimalRules, err := inc_rule_dig.GetMinimalIncRulesByTaskId(inputParam.PreTaskId)
	if err != nil {
		logger.Errorf("[IncMinerIndicator] GetIncRulesByTaskId error:%v, taskId:%v", err, inputParam.PreTaskId)
		return nil, err
	}
	//convertIncRules(preMinimalRules, inputParam.Gv.TableIds[0])
	convertIncRulesNew(preMinimalRules, oldTaskExtend.TableId2Name, inputParam.TableName2Id)

	prePrunedRules, err := inc_rule_dig.GetPrunedIncRulesByTaskId(inputParam.PreTaskId)
	if err != nil {
		logger.Errorf("[IncMinerIndicator] GetPruneTaskTreesByTaskId error:%v, taskId:%v", err, inputParam.PreTaskId)
		return nil, err
	}
	//convertIncRules(prePrunedRules, inputParam.Gv.TableIds[0])
	convertIncRulesNew(prePrunedRules, oldTaskExtend.TableId2Name, inputParam.TableName2Id)

	preSampleNodes, err := inc_rule_dig.GetSampleNodesByTaskId(inputParam.PreTaskId)
	if err != nil {
		logger.Errorf("[IncMinerIndicator] GetSampleNodesByTaskId error:%v, taskId:%v", err, inputParam.PreTaskId)
		return nil, err
	}
	//convertSampleNodes(preSampleNodes, inputParam.Gv.TableIds[0])
	convertSampleNodesNew(preSampleNodes, oldTaskExtend.TableId2Name, inputParam.TableName2Id)

	startTime := time.Now().UnixMilli()
	updatedMinimalRules := make([]*inc_rule_dig.IncRule, 0)
	updatedPrunedRules := make([]*inc_rule_dig.IncRule, 0)
	updatedSampleNodes := make([]*inc_rule_dig.SampleNode, 0)

	if inputParam.SupportChange > 0 && inputParam.ConfidenceChange > 0 {
		// preMinimalRules中过滤：(1)符合新参数的规则，添加到minimalRules，(2)不符合conf(φ) < δ + ∆δ的规则，添加的扩展队列
		filteredMinimalRules := make([]*inc_rule_dig.IncRule, 0)
		needExpandRules := make([]*inc_rule_dig.IncRule, 0)
		for _, preMinimalRule := range preMinimalRules {
			if preMinimalRule.CR >= inputParam.UpdateIndicator.Support && preMinimalRule.FTR >= inputParam.UpdateIndicator.Confidence {
				filteredMinimalRules = append(filteredMinimalRules, preMinimalRule)
			} else if (preMinimalRule.CR < inputParam.UpdateIndicator.Support) || (preMinimalRule.FTR < inputParam.UpdateIndicator.Confidence) {
				// preMinimalRule.CR < inputParam.UpdateIndicator.Support 保证规则放到剪枝集合里
				// preMinimalRule.FTR < inputParam.UpdateIndicator.Confidence 参与搜索扩展
				// 根据support变大和反单调性(X谓词越多，support越小)，上一轮剪枝的集合没有必要再扩展了，因此不加入新的扩展队列
				needExpandRules = append(needExpandRules, preMinimalRule)
			}
		}

		updatedMinimalRules = append(updatedMinimalRules, filteredMinimalRules...)
		updatedPrunedRules = append(updatedPrunedRules, prePrunedRules...)

		rhsKey2Rules, rhsKey2Rhs := incRuleGroupByRhs(needExpandRules)

		rhsKey2SampleNodes, notUseSampleNodes := getRhsKey2SampleNodes(rhsKey2Rules, preSampleNodes)
		updatedSampleNodes = append(updatedSampleNodes, notUseSampleNodes...)

		var wg sync.WaitGroup
		mutex := sync.Mutex{}
		ch := make(chan struct{}, inputParam.Gv.TreeChanSize)
		for rhsKey, incRules := range rhsKey2Rules {
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
				layer2IncRules := incRuleGroupByLayer(incRules)
				//sampleNodesOfRhs := getSampleNodesByRhsKey(preSampleNodes, rhsKey)
				sampleNodesOfRhs := rhsKey2SampleNodes[rhsKey]

				incExpandInputParam := &inc_rule_dig.IncExpandInput{
					Rhs:                rhsKey2Rhs[rhsKey],
					NeedExpandIncRules: layer2IncRules,
					SampleNodes:        sampleNodesOfRhs,
					Gv:                 inputParam.Gv,
					CoverRadius:        inputParam.CoverRadius,
					WithoutSampling:    inputParam.WithoutSampling,
				}
				incExpandOutputParam := trees.IncExpandIndicator(incExpandInputParam)
				mutex.Lock()
				updatedMinimalRules = append(updatedMinimalRules, incExpandOutputParam.MinimalIncRules...)
				updatedPrunedRules = append(updatedPrunedRules, incExpandOutputParam.PrunedIncRules...)
				updatedSampleNodes = append(updatedSampleNodes, incExpandOutputParam.UpdatedSampleNodes...)
				mutex.Unlock()
			}(rhsKey, incRules)
		}
		wg.Wait()
	} else if inputParam.SupportChange > 0 && inputParam.ConfidenceChange < 0 {
		for _, preMinimalRule := range preMinimalRules {
			if preMinimalRule.CR < inputParam.UpdateIndicator.Support {
				// 和 陈老师 确认，这里需要放到剪枝集合为下次挖掘做准备
				//continue
				updatedPrunedRules = append(updatedPrunedRules, preMinimalRule)
			}
			updatedMinimalRules = append(updatedMinimalRules, preMinimalRule)
		}
		//// TODO 补充sampleNodes排序功能(不排序也可以,后续再实现)
		var relaxSampleNodes []*inc_rule_dig.SampleNode
		if !inputParam.UseCDF || inputParam.RecallBound == 1.0 {
			relaxSampleNodes = preSampleNodes
		} else {
			relaxSampleNodes = getRelaxSampleNodes(preSampleNodes, inputParam.UpdateIndicator.Confidence, inputParam.RecallBound)
		}
		logger.Infof("[incMinerConfidence] relaxSampleNodes size:%v", len(relaxSampleNodes))

		//for _, sampleNode := range preSampleNodes {
		//	if sampleNode.MaxConf >= inputParam.UpdateIndicator.Confidence {
		//		if sampleNode.CurrentNode.FTR >= inputParam.UpdateIndicator.Confidence {
		//			updatedMinimalRules = append(updatedMinimalRules, sampleNode.CurrentNode)
		//		}
		//		for _, neighborNode := range sampleNode.NeighborNodes {
		//			if neighborNode.FTR >= inputParam.UpdateIndicator.Confidence {
		//				updatedMinimalRules = append(updatedMinimalRules, neighborNode)
		//			}
		//		}
		//	}
		//}

		tmpMinimalRules, err := calcSampleNeighbors(relaxSampleNodes, inputParam.UpdateIndicator, inputParam.Gv)
		if err != nil {
			logger.Error(err)
			return nil, err
		}
		updatedMinimalRules = append(updatedMinimalRules, tmpMinimalRules...)
		updatedPrunedRules = prePrunedRules
		updatedSampleNodes = preSampleNodes
	} else if inputParam.SupportChange < 0 && inputParam.ConfidenceChange > 0 {
		needExpandRules := make([]*inc_rule_dig.IncRule, 0)
		for _, preMinimalRule := range preMinimalRules {
			if preMinimalRule.FTR < inputParam.UpdateIndicator.Confidence {
				needExpandRules = append(needExpandRules, preMinimalRule)
			} else {
				updatedMinimalRules = append(updatedMinimalRules, preMinimalRule)
			}
		}
		// support变小，上一轮被剪枝的规则，需要继续扩展
		needExpandRules = append(needExpandRules, prePrunedRules...)

		rhsKey2Rules, rhsKey2Rhs := incRuleGroupByRhs(needExpandRules)

		rhsKey2SampleNodes, notUseSampleNodes := getRhsKey2SampleNodes(rhsKey2Rules, preSampleNodes)
		updatedSampleNodes = append(updatedSampleNodes, notUseSampleNodes...)

		var wg sync.WaitGroup
		mutex := sync.Mutex{}
		ch := make(chan struct{}, inputParam.Gv.TreeChanSize)
		for rhsKey, incRules := range rhsKey2Rules {
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
				layer2IncRules := incRuleGroupByLayer(incRules)
				//sampleNodesOfRhs := getSampleNodesByRhsKey(preSampleNodes, rhsKey)
				sampleNodesOfRhs := rhsKey2SampleNodes[rhsKey]

				incExpandInputParam := &inc_rule_dig.IncExpandInput{
					Rhs:                rhsKey2Rhs[rhsKey],
					NeedExpandIncRules: layer2IncRules,
					SampleNodes:        sampleNodesOfRhs,
					Gv:                 inputParam.Gv,
					CoverRadius:        inputParam.CoverRadius,
					WithoutSampling:    inputParam.WithoutSampling,
				}
				//if incExpandInputParam.Rhs.PredicateStr == "t0.income=t1.income" {
				incExpandOutputParam := trees.IncExpandIndicator(incExpandInputParam)
				mutex.Lock()
				updatedMinimalRules = append(updatedMinimalRules, incExpandOutputParam.MinimalIncRules...)
				updatedPrunedRules = append(updatedPrunedRules, incExpandOutputParam.PrunedIncRules...)
				updatedSampleNodes = append(updatedSampleNodes, incExpandOutputParam.UpdatedSampleNodes...)
				mutex.Unlock()
				//}
			}(rhsKey, incRules)
		}
		wg.Wait()
	} else {
		updatedMinimalRules = append(updatedMinimalRules, preMinimalRules...)
		needExpandRules := make([]*inc_rule_dig.IncRule, 0)
		for _, prePrunedRule := range prePrunedRules {
			if prePrunedRule.CR >= inputParam.UpdateIndicator.Support {
				needExpandRules = append(needExpandRules, prePrunedRule)
			}
		}

		rhsKey2Rules, rhsKey2Rhs := incRuleGroupByRhs(needExpandRules)

		//for key := range rhsKey2Rules {
		//	logger.Infof("[调试日志] rhsKey:%v", key)
		//}
		rhsKey2SampleNodes, notUseSampleNodes := getRhsKey2SampleNodes(rhsKey2Rules, preSampleNodes)
		updatedSampleNodes = append(updatedSampleNodes, notUseSampleNodes...)

		var wg sync.WaitGroup
		mutex := sync.Mutex{}
		ch := make(chan struct{}, inputParam.Gv.TreeChanSize)
		for rhsKey, incRules := range rhsKey2Rules {
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
				layer2IncRules := incRuleGroupByLayer(incRules)
				//sampleNodesOfRhs := getSampleNodesByRhsKey(preSampleNodes, rhsKey)
				sampleNodesOfRhs := rhsKey2SampleNodes[rhsKey]

				incExpandInputParam := &inc_rule_dig.IncExpandInput{
					Rhs:                rhsKey2Rhs[rhsKey],
					NeedExpandIncRules: layer2IncRules,
					SampleNodes:        sampleNodesOfRhs,
					Gv:                 inputParam.Gv,
					CoverRadius:        inputParam.CoverRadius,
					WithoutSampling:    inputParam.WithoutSampling,
				}
				//if incExpandInputParam.Rhs.PredicateStr == "t0.State=t1.State" {
				incExpandOutputParam := trees.IncExpandIndicator(incExpandInputParam)
				mutex.Lock()
				updatedMinimalRules = append(updatedMinimalRules, incExpandOutputParam.MinimalIncRules...)
				updatedPrunedRules = append(updatedPrunedRules, incExpandOutputParam.PrunedIncRules...)
				updatedSampleNodes = append(updatedSampleNodes, incExpandOutputParam.UpdatedSampleNodes...)
				mutex.Unlock()
				//}
			}(rhsKey, incRules)
		}
		wg.Wait()
		// 和 陈老师 确认，这里遍历上一轮的采样节点集合，不需要更新的采样节点集合，更新的用到下一轮
		var relaxSampleNodes []*inc_rule_dig.SampleNode
		if !inputParam.UseCDF || inputParam.RecallBound == 1.0 {
			relaxSampleNodes = preSampleNodes
		} else {
			relaxSampleNodes = getRelaxSampleNodes(preSampleNodes, inputParam.UpdateIndicator.Confidence, inputParam.RecallBound)
		}
		logger.Infof("[incMinerConfidence] relaxSampleNodes size:%v", len(relaxSampleNodes))

		//for _, sampleNode := range preSampleNodes {
		//	if sampleNode.MaxConf >= inputParam.UpdateIndicator.Confidence {
		//		if sampleNode.CurrentNode.FTR >= inputParam.UpdateIndicator.Confidence {
		//			updatedMinimalRules = append(updatedMinimalRules, sampleNode.CurrentNode)
		//		}
		//		for _, neighborNode := range sampleNode.NeighborNodes {
		//			if neighborNode.FTR >= inputParam.UpdateIndicator.Confidence {
		//				updatedMinimalRules = append(updatedMinimalRules, neighborNode)
		//			}
		//		}
		//	}
		//}
		tmpMinimalRules, err := calcSampleNeighbors(relaxSampleNodes, inputParam.UpdateIndicator, inputParam.Gv)
		if err != nil {
			logger.Error(err)
			return nil, err
		}
		updatedMinimalRules = append(updatedMinimalRules, tmpMinimalRules...)
	}
	totalTime := time.Now().UnixMilli() - startTime
	logger.Infof("[incMinerIndicator] finish incMinerIndicator, taskId:%v, supportChange:%v, confidenceChange:%v, totalTime:%v", inputParam.UpdateTaskId, inputParam.SupportChange, inputParam.ConfidenceChange, totalTime)
	return &inc_rule_dig.IncMinerOutput{
		MinimalIncRules:    updatedMinimalRules,
		PrunedIncRules:     updatedPrunedRules,
		UpdatedSampleNodes: updatedSampleNodes,
		TotalTime:          totalTime,
	}, nil
}

func getRhsKey2SampleNodes(rhsKey2Rules map[string][]*inc_rule_dig.IncRule, sampleNodes []*inc_rule_dig.SampleNode) (map[string][]*inc_rule_dig.SampleNode, []*inc_rule_dig.SampleNode) {
	notUseSampleNodes := make([]*inc_rule_dig.SampleNode, 0)
	rhsKey2SampleNodes := make(map[string][]*inc_rule_dig.SampleNode)
	for _, sampleNode := range sampleNodes {
		//logger.Infof("[调试日志] sample node rhs key:%v", sampleNode.CurrentNode.Node.Rhs.Key())
		if _, ok := rhsKey2Rules[sampleNode.CurrentNode.Node.Rhs.Key()]; !ok {
			notUseSampleNodes = append(notUseSampleNodes, sampleNode)
		} else {
			rhsKey2SampleNodes[sampleNode.CurrentNode.Node.Rhs.Key()] = append(rhsKey2SampleNodes[sampleNode.CurrentNode.Node.Rhs.Key()], sampleNode)
		}
	}
	return rhsKey2SampleNodes, notUseSampleNodes
}

func incRuleGroupByRhs(incRules []*inc_rule_dig.IncRule) (map[string][]*inc_rule_dig.IncRule, map[string]rds.Predicate) {
	groupByResult := make(map[string][]*inc_rule_dig.IncRule)
	key2Rhs := make(map[string]rds.Predicate)
	for _, incRule := range incRules {
		if incRule == nil {
			continue
		}
		key := incRule.Node.Rhs.Key()
		groupByResult[key] = append(groupByResult[key], incRule)
		key2Rhs[key] = incRule.Node.Rhs
	}
	return groupByResult, key2Rhs
}

func incRuleGroupByLayer(incRules []*inc_rule_dig.IncRule) map[int][]*inc_rule_dig.IncRule /* layerNum -> incRules */ {
	groupByResult := make(map[int][]*inc_rule_dig.IncRule)

	for _, incRule := range incRules {
		if incRule == nil {
			continue
		}
		layerNum := len(incRule.Node.Lhs)
		groupByResult[layerNum] = append(groupByResult[layerNum], incRule)
	}

	return groupByResult
}

func getSampleNodesByRhsKey(sampleNodes []*inc_rule_dig.SampleNode, rhsKey string) []*inc_rule_dig.SampleNode {
	sampleNodesOfRhsKey := make([]*inc_rule_dig.SampleNode, 0)
	for _, sampleNode := range sampleNodes {
		if rhsKey == sampleNode.CurrentNode.Node.Rhs.Key() {
			sampleNodesOfRhsKey = append(sampleNodesOfRhsKey, sampleNode)
		}
	}
	return sampleNodesOfRhsKey
}

func getRelaxSampleNodes(sampleNodes []*inc_rule_dig.SampleNode, updateConfidence float64, recallBound float64) []*inc_rule_dig.SampleNode {
	startTime := time.Now().UnixMilli()
	relaxSampleNodes := make([]*inc_rule_dig.SampleNode, 0)
	cdfs := getSampleNodesCDF(sampleNodes, updateConfidence)
	N := getSampleNodesValidReeCount(sampleNodes, cdfs)
	FN := 0
	FNMax := int(math.Ceil(float64(N) * (1 - recallBound)))
	for i, sampleNode := range sampleNodes {
		n := getSampleNodeValidReeCount(sampleNode, cdfs[i])
		if (FN + n) <= FNMax {
			FN += n
		} else {
			relaxSampleNodes = append(relaxSampleNodes, sampleNode)
		}
	}
	useTime := time.Now().UnixMilli() - startTime
	logger.Infof("[getRelaxSampleNodes] finish getRelaxSampleNodes, N:%v, FNMax:%v, CDFs:%v, time:%v", N, FNMax, cdfs, useTime)
	return relaxSampleNodes
}

func getSampleNodesValidReeCount(sampleNodes []*inc_rule_dig.SampleNode, cdfs []float64) int {
	allValidCount := 0
	for i, sampleNode := range sampleNodes {
		allValidCount += getSampleNodeValidReeCount(sampleNode, cdfs[i])
	}
	return allValidCount
}

func getSampleNodeValidReeCount(sampleNode *inc_rule_dig.SampleNode, cdf float64) int {
	sampleReeCount := len(sampleNode.NeighborNodes) + 1
	validCount := math.Ceil((1 - cdf) * float64(sampleReeCount))
	return int(validCount)
}

func getSampleNodesCDF(sampleNodes []*inc_rule_dig.SampleNode, updatedConfidence float64) []float64 {
	cdfs := make([]float64, len(sampleNodes))
	for i, sampleNode := range sampleNodes {
		cdf := float64(0)
		invalidCount := 0
		validCount := 0
		if sampleNode.CurrentNode.FTR < updatedConfidence {
			invalidCount++
		} else {
			validCount++
		}
		for _, incRule := range sampleNode.NeighborNodes {
			if incRule.FTR < updatedConfidence {
				invalidCount++
			} else {
				validCount++
			}
		}
		//for _, conf := range sampleNode.NeighborConfs {
		//	if conf < updatedConfidence {
		//		invalidCount++
		//	} else {
		//		validCount++
		//	}
		//}
		if len(sampleNode.NeighborCDF) > 0 {
			valid, invalid := inc_rule_dig.GetValidAndInvalidCount(updatedConfidence, sampleNode.NeighborCDF)
			validCount += valid
			invalidCount += invalid
		}

		if validCount == 0 {
			//logger.Warnf("[getSampleNodesCDF] Num. of valid REEs is zero, Set the value of CDF to 1")
			cdf = 1
		} else {
			cdf = float64(invalidCount) / float64(invalidCount+validCount)
		}
		cdfs[i] = cdf
	}
	return cdfs
}

func batchMiner(request *inc_rule_dig.IncRdsRequest) (*inc_rule_dig.BatchMinerOutput, error) {
	taskId := request.TaskID
	logger.Infof("task id:%v, start batchMiner", taskId)
	storage_utils.CPUBusy(1)
	defer storage_utils.CPUBusy(-1)
	gv := global_variables.InitGlobalV(taskId)
	setConfOfInc(gv, request)
	var err error
	gv.ChEndpoint, err = getChanEndpoint()
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	loadDataTime := loadData(gv.TableIds, []udf.UDFTabCol{}, gv.TaskId, gv.DecisionTreeMaxRowSize)
	logger.Infof("task id:%v, finish create pli, loadDataTime:%v", gv.TaskId, loadDataTime)

	startTime := time.Now().UnixMilli()
	// 生成跨表谓词
	//createCrossPredicates(gv, request.JoinKeys, request.OtherMappingKeys, request.UDFTabCols)
	logger.Infof("task id:%v, finish create cross predicates", gv.TaskId)
	//生成单表谓词
	for _, tableId := range gv.TableIds {
		calculate.GeneratePliUseStorage(tableId, gv)
	}
	logger.Infof("task id:%v, finish create inner predicates", gv.TaskId)
	if len(gv.Predicates) == 0 && len(gv.CrossTablePredicates[1]) == 0 {
		logger.Infof("taskid:%v,生成的谓词数量为0,不需要进行规则发现", taskId)
		return nil, nil
	}
	gv.AbandonedRules = db_util.GetAbandonedRules()

	// 生成y谓词和候选x谓词
	root := make([]rds.Predicate, 0)
	root = append(root, gv.Predicates...)
	root = append(root, gv.CrossTablePredicates[1]...)
	if len(request.Rhs) > 0 {
		root = utils.GenerateRhs(request.Rhs, root, gv.TableIds)
	}

	//// 可以作为Y出现的谓词
	//root = allRhsPredicates(gv, gv.SkipYColumns)

	root = utils.FilterRhs(root, gv.CrossTablePredicates[0])
	logger.Infof("taskid:%v,多行规则Y的个数:%v", gv.TaskId, len(root))
	lhs := make([]rds.Predicate, 0)
	lhs = append(lhs, gv.Predicates...)
	lhs = append(lhs, gv.CrossTablePredicates[0]...)

	if gv.UseDecisionTree {
		//单行规则用决策树挖掘
		t := time.Now().UnixMilli()
		singleRowRuleDig(gv)
		singleRowRuleTime := time.Now().UnixMilli() - t
		logger.Infof("taskid:%v,单行规则执行时间:%vms, 发现的规则数:%v", gv.TaskId, singleRowRuleTime, gv.SingleRuleSize)
	}

	// 多行规则
	output := trees.BuildTreeOfInc(root, lhs, gv, request.CoverRadius)
	rdsTime := time.Now().UnixMilli() - startTime
	logger.Infof("%v多行规则发现完成，发现规则数:%v, 耗时:%v", gv.TaskId, len(output.MinimalIncRules), rdsTime)
	//output.TotalTime = loadDataTime + rdsTime
	output.TotalTime = rdsTime
	return output, nil
}

func setConfOfInc(gv *global_variables.GlobalV, request *inc_rule_dig.IncRdsRequest) (map[string]string, error) {
	if request.ChanSize < 1 {
		request.ChanSize = 1
	}
	if request.TreeChanSize < 1 {
		request.TreeChanSize = 1
	}
	gv.TreeChanSize = request.TreeChanSize
	gv.RdsChanSize = request.ChanSize
	gv.CubeSize = request.CubeSize
	gv.UseNeighborConfs = request.UseNeighborConfs
	gv.UsePruneVersion = request.UsePruneVersion
	gv.UseDecisionTree = request.UseDecisionTree

	if request.PredicateSupportLimit > 0 {
		gv.PredicateSupportLimit = request.PredicateSupportLimit
	}

	// 可能需要导入表
	logger.Infof("task id:%v, start import table:%v", gv.TaskId, gv.TableIds)
	tableId2Name := make(map[string]string)
	for i, table := range request.DataSources {
		if table.TableType != 0 {
			continue
		}
		tableId := table.TableId
		if tableId == "" {
			tableId = storage_utils.ImportTable(&table)
		}
		gv.TableIds = append(gv.TableIds, tableId)
		gv.TableIndex[tableId] = i
		request.DataSources[i].TableId = tableId
		tableId2Name[tableId] = table.TableName
	}
	logger.Infof("task id:%v, finish import table:%v", gv.TaskId, gv.TableIds)

	field := &inc_rule_dig.IncRdsTaskExtendField{
		TableId2Name: tableId2Name,
	}
	err := inc_rule_dig.WriteTaskExtendToDB(gv.TaskId, field)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	gv.CR = request.UpdatedSupport
	gv.FTR = request.UpdatedConfidence
	if request.TreeLevel > 0 {
		gv.TreeMaxLevel = request.TreeLevel
	}
	if request.TopKLayer > 0 {
		gv.TopKLayer = request.TopKLayer
	}
	if request.TopKSize > 0 {
		gv.TopKSize = request.TopKSize
	}
	if request.DecisionTreeMaxDepth > 0 {
		gv.DecisionTreeMaxDepth = request.DecisionTreeMaxDepth
	}
	if request.DecisionTreeMaxRowSize != 0 {
		gv.DecisionTreeMaxRowSize = request.DecisionTreeMaxRowSize
	}
	if request.EnumSize > 0 {
		gv.EnumSize = request.EnumSize
	} else {
		gv.EnumSize = rds_config.EnumSize
	}
	gv.MultiTable = true

	tableName2Id := make(map[string]string)
	for tableId, tableName := range tableId2Name {
		tableName2Id[tableName] = tableId
	}
	return tableName2Id, nil
}

// 替换规则中的tableId
func convertIncRules(incRules []*inc_rule_dig.IncRule, updatedTableId string) {
	startTime := time.Now().UnixMilli()
	for _, incRule := range incRules {
		// 转换Rhs
		incRule.Node.Rhs.LeftColumn.TableId = updatedTableId
		if incRule.Node.Rhs.RightColumn.TableId != "" {
			incRule.Node.Rhs.RightColumn.TableId = updatedTableId
		}
		// 转换Lhs
		for pi := range incRule.Node.Lhs {
			p := &incRule.Node.Lhs[pi]
			p.LeftColumn.TableId = updatedTableId
			if p.RightColumn.TableId != "" {
				p.RightColumn.TableId = updatedTableId
			}
		}
		// 转换LhsCandidate
		for pi := range incRule.Node.LhsCandidate {
			p := &incRule.Node.LhsCandidate[pi]
			p.LeftColumn.TableId = updatedTableId
			if p.RightColumn.TableId != "" {
				p.RightColumn.TableId = updatedTableId
			}
		}
		// 转换tableId2Index
		newTableId2Index := make(map[string]int, len(incRule.Node.TableId2index))
		for _, value := range incRule.Node.TableId2index {
			newTableId2Index[updatedTableId] = value
		}
		incRule.Node.TableId2index = newTableId2Index
	}
	useTime := time.Now().UnixMilli() - startTime
	logger.Debugf("[convertIncRules] finish convertIncRules, ruleSize:%v, time:%v", len(incRules), useTime)
}

// 替换规则中的tableId
func convertIncRulesNew(incRules []*inc_rule_dig.IncRule, oldTableId2Name map[string]string, newTableName2Id map[string]string) {
	startTime := time.Now().UnixMilli()
	logger.Debugf("[convertIncRules] start convertIncRules, oldTableId2Name:%v, newTableName2Id:%v", oldTableId2Name, newTableName2Id)
	for _, incRule := range incRules {
		// 转换Rhs
		incRule.Node.Rhs.LeftColumn.TableId = getNewTableId(incRule.Node.Rhs.LeftColumn.TableId, oldTableId2Name, newTableName2Id)
		if incRule.Node.Rhs.RightColumn.TableId != "" {
			incRule.Node.Rhs.RightColumn.TableId = getNewTableId(incRule.Node.Rhs.RightColumn.TableId, oldTableId2Name, newTableName2Id)
		}
		// 转换Lhs
		for pi := range incRule.Node.Lhs {
			p := &incRule.Node.Lhs[pi]
			p.LeftColumn.TableId = getNewTableId(p.LeftColumn.TableId, oldTableId2Name, newTableName2Id)
			if p.RightColumn.TableId != "" {
				p.RightColumn.TableId = getNewTableId(p.RightColumn.TableId, oldTableId2Name, newTableName2Id)
			}
		}
		// 转换LhsCandidate
		for pi := range incRule.Node.LhsCandidate {
			p := &incRule.Node.LhsCandidate[pi]
			p.LeftColumn.TableId = getNewTableId(p.LeftColumn.TableId, oldTableId2Name, newTableName2Id)
			if p.RightColumn.TableId != "" {
				p.RightColumn.TableId = getNewTableId(p.RightColumn.TableId, oldTableId2Name, newTableName2Id)
			}
		}
		// 转换tableId2Index
		newTableId2Index := make(map[string]int, len(incRule.Node.TableId2index))
		for tableId, value := range incRule.Node.TableId2index {
			newTableId := getNewTableId(tableId, oldTableId2Name, newTableName2Id)
			newTableId2Index[newTableId] = value
		}
		incRule.Node.TableId2index = newTableId2Index
	}
	useTime := time.Now().UnixMilli() - startTime
	logger.Debugf("[convertIncRules] finish convertIncRules, ruleSize:%v, time:%v", len(incRules), useTime)
}

func getNewTableId(oldTableId string, oldTableId2Name map[string]string, newTableName2Id map[string]string) string {
	if tableName, ok := oldTableId2Name[oldTableId]; !ok {
		logger.Warnf("[getNewTableId] can't get tableName by oldTableId:%v", oldTableId)
		return oldTableId
	} else if newTableId, ok1 := newTableName2Id[tableName]; !ok1 {
		logger.Warnf("[getNewTableId] can't get newTableId by tableName:%v, newTableName2Id:%v", tableName, newTableName2Id)
		return oldTableId
	} else {
		return newTableId
	}
}

func convertSampleNodes(sampleNodes []*inc_rule_dig.SampleNode, updatedTableId string) {
	startTime := time.Now().UnixMilli()
	for _, sampleNode := range sampleNodes {
		convertIncRules([]*inc_rule_dig.IncRule{sampleNode.CurrentNode}, updatedTableId)
		convertIncRules(sampleNode.PredecessorNodes, updatedTableId)
		convertIncRules(sampleNode.NeighborNodes, updatedTableId)
	}
	useTime := time.Now().UnixMilli() - startTime
	logger.Debugf("[convertSampleNodes] finish convertSampleNodes, sampleNodeSize:%v, time:%v", len(sampleNodes), useTime)
}

func convertSampleNodesNew(sampleNodes []*inc_rule_dig.SampleNode, oldTableId2Name map[string]string, newTableName2Id map[string]string) {
	startTime := time.Now().UnixMilli()
	for _, sampleNode := range sampleNodes {
		convertIncRulesNew([]*inc_rule_dig.IncRule{sampleNode.CurrentNode}, oldTableId2Name, newTableName2Id)
		convertIncRulesNew(sampleNode.PredecessorNodes, oldTableId2Name, newTableName2Id)
		convertIncRulesNew(sampleNode.NeighborNodes, oldTableId2Name, newTableName2Id)
	}
	useTime := time.Now().UnixMilli() - startTime
	logger.Debugf("[convertSampleNodes] finish convertSampleNodes, sampleNodeSize:%v, time:%v", len(sampleNodes), useTime)
}

func getChanEndpoint() (chan string, error) {
	calcEndpoints, err := distribute.GEtcd.GetCalcNodeEndpoint(distribute.TRANSPORT_TYPE_GRPC)
	if err != nil {
		logger.Errorf("[IncDigRulesDistribute] GetCalcNodeEndpoint failed, error:%v", err)
		return nil, err
	}
	chEndpoint := make(chan string, len(calcEndpoints))
	for _, endpoint := range calcEndpoints {
		chEndpoint <- endpoint
	}
	return chEndpoint, nil
}

func calcSampleNeighbors(sampleNodes []*inc_rule_dig.SampleNode, updatedIndicate inc_rule_dig.Indicator, gv *global_variables.GlobalV) (minimalRules []*inc_rule_dig.IncRule, err error) {
	calcDecisionTreeNeighbors := make([]*inc_rule_dig.IncRule, 0)

	client, err := distribute.NewClient(nil)
	if err != nil {
		logger.Errorf("[calcSampleNeighbors] new rpc client failed, error:%v", err)
		return nil, err
	}
	chCalcRuleResult := make(chan *inc_rule_dig.CalcRuleResult)
	// 从channel读取计算结果
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer func() {
			wg.Done()
		}()
		recvCount := 0
		for {
			calcRuleResult := <-chCalcRuleResult
			if calcRuleResult == nil {
				break
			}
			recvCount++
			if calcRuleResult.Rule.CR >= updatedIndicate.Support && calcRuleResult.Rule.FTR >= updatedIndicate.Confidence {
				minimalRules = append(minimalRules, calcRuleResult.Rule)
			} else if calcRuleResult.Rule.CR > updatedIndicate.Support {
				calcDecisionTreeNeighbors = append(calcDecisionTreeNeighbors, calcRuleResult.Rule)
			}
		}
		logger.Infof("[calcSampleNeighbors] taskId:%v, 接收到 %v 条规则的结果", gv.TaskId, recvCount)
	}()
	sendCount := 0
	// 遍历samples,计算规则并把结果写入channel
	for _, sampleNode := range sampleNodes {
		if sampleNode.MaxConf >= updatedIndicate.Confidence {
			//printSample(i, sampleNode, "****命中")
			if sampleNode.CurrentNode.CR >= updatedIndicate.Support && sampleNode.CurrentNode.FTR >= updatedIndicate.Confidence {
				minimalRules = append(minimalRules, sampleNode.CurrentNode)
			} else if sampleNode.CurrentNode.CR > updatedIndicate.Support {
				calcDecisionTreeNeighbors = append(calcDecisionTreeNeighbors, sampleNode.CurrentNode)
			}
			if len(sampleNode.NeighborNodes) > 0 {
				for _, neighborNode := range sampleNode.NeighborNodes {
					if neighborNode.CR >= updatedIndicate.Support && neighborNode.FTR >= updatedIndicate.Confidence {
						minimalRules = append(minimalRules, neighborNode)
					} else if neighborNode.CR > updatedIndicate.Support {
						calcDecisionTreeNeighbors = append(calcDecisionTreeNeighbors, neighborNode)
					}
				}
			}
			neighborNodes := getSampleNodeNeighbors(sampleNode)
			for _, neighborNode := range neighborNodes {
				//if neighborNode.FTR >= updatedConfidence {
				//	minimalRules = append(minimalRules, neighborNode)
				//}
				sendCount++
				trees.CalIncRule(client, neighborNode, gv, chCalcRuleResult)
				//if gv.UseDecisionTree {
				//	trees.CalcNotSatisfyNode(client, neighborNode, false, gv, sendCount)
				//}
			}
		}
		//else {
		//	// 调试日志
		//	printSample(i, sampleNode, "****未命中")
		//}
	}
	logger.Infof("[calcSampleNeighbors] taskId:%v, 发送 %v 条规则", gv.TaskId, sendCount)
	client.Close()
	chCalcRuleResult <- nil
	wg.Wait()

	if gv.UseDecisionTree {
		logger.Infof("[calcSampleNeighbors] taskId:%v, need calculate decision node count:%v", gv.TaskId, len(calcDecisionTreeNeighbors))
		dtClient, err := distribute.NewClient(nil)
		if err != nil {
			logger.Errorf("[calcSampleNeighbors] new decision tree rpc client failed, error:%v", err)
			return nil, err
		}

		dtSendCount := 0
		for _, neighbor := range calcDecisionTreeNeighbors {
			dtSendCount++
			trees.CalcNotSatisfyNode(dtClient, neighbor, false, gv, dtSendCount)
		}
		dtClient.Close()
	}
	return
}

func getSampleNodeNeighbors(sampleNode *inc_rule_dig.SampleNode) []*inc_rule_dig.IncRule {
	//return sampleNode.NeighborNodes
	// 按论文要求neighbor不能预先存起来，需要重新获取并计算
	startTime := time.Now().UnixMilli()
	neighbors := make([]*inc_rule_dig.IncRule, 0)
	currentNodeLhsStr := utils.GetLhsStr(sampleNode.CurrentNode.Node.Lhs)
	sum := 0
	for _, predecessorNode := range sampleNode.PredecessorNodes {
		children := trees.GetChildren(predecessorNode, sampleNode.CurrentNode.Node.Rhs)
		sum += len(children)
		for _, child := range children {
			childLhsStr := utils.GetLhsStr(child.Node.Lhs)
			if currentNodeLhsStr == childLhsStr {
				continue
			}
			neighbors = append(neighbors, child)
		}
	}
	useTime := time.Now().UnixMilli() - startTime
	logger.Debugf("[getSampleNodeNeighbors] time:%v(ms), sum:%v, neighbors:%v", useTime, sum, len(neighbors))
	return neighbors
}

func getRecallRate(incMinimalRules []*inc_rule_dig.IncRule, batchMinerTaskId int64) float64 {
	batchMinimalRules, err := inc_rule_dig.GetMinimalIncRulesByTaskId(batchMinerTaskId)
	if err != nil {
		logger.Errorf("[getRecallRate] GetMinimalIncRulesByTaskId failed, error:%v, batchMinerTaskId:%v", err, batchMinerTaskId)
		return -1
	}
	logger.Infof("[getRecallRate] incREEs size:%v, batchREEs size:%v", len(incMinimalRules), len(batchMinimalRules))
	set := make(map[string]struct{})
	for _, incRule := range incMinimalRules {
		ree := inc_rule_dig.GetIncRuleRee(incRule)
		set[ree] = struct{}{}
	}

	intersectCount := 0
	for _, batchRule := range batchMinimalRules {
		ree := inc_rule_dig.GetIncRuleRee(batchRule)
		if _, ok := set[ree]; ok {
			intersectCount++
		} else {
			logger.Infof("[调试日志] ree:%v, support:%v, confidence:%v", ree, batchRule.CR, batchRule.FTR)
		}
	}

	batchCount := len(batchMinimalRules)
	var recallRate float64
	if batchCount > 0 {
		recallRate = float64(intersectCount) / float64(batchCount)
	}

	logger.Infof("[getRecallRate] finish get recall rate, intersectCount:%v, batchCount:%v, recallRate:%v", intersectCount, batchCount, recallRate)
	return recallRate
}

func getRecallRateNew(incMinimalRules []*inc_rule_dig.IncRule, batchMinerTaskId int64) float64 {
	batchMinimalRules, err := inc_rule_dig.GetMinimalIncRulesByTaskId(batchMinerTaskId)
	if err != nil {
		logger.Errorf("[getRecallRateNew] GetMinimalIncRulesByTaskId failed, error:%v, batchMinerTaskId:%v", err, batchMinerTaskId)
		return -1
	}
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

func getRecallRateOfGuidelines(incMinimalRules []*inc_rule_dig.IncRule, decisionTreeRules []*rds.Rule, goldenRules []string) float64 {
	logger.Infof("[getRecallRateOfGuidelines] incremental REEs size:%v, decision tree REEs size:%v, golden REEs size:%v", len(incMinimalRules), len(decisionTreeRules), len(goldenRules))

	//incKey2Rules := incRuleGroupByRhsNew(incMinimalRules)
	//dtKey2Rules := dtRuleGroupByRhs(decisionTreeRules)
	//goldenKey2Rules := goldenRuleGroupByRhs(goldenRules)

	intersectCount := 0
	//for goldenKey, rules := range goldenKey2Rules {
	//	incRules := incKey2Rules[goldenKey]
	//	for _, goldenRule := range rules {
	//		goldenLhsStrs := getGoldenRuleLhsStrs(goldenRule)
	//		isCover := false
	//		for _, incRule := range incRules {
	//			lhsStrs := getIncRuleLhsStrs(incRule)
	//			if inc_rule_dig.Contains(goldenLhsStrs, lhsStrs) {
	//				intersectCount++
	//				isCover = true
	//				break
	//			}
	//		}
	//		if !isCover {
	//			logger.Infof("[getRecallRateOfGuidelines] incremental not cover golden rule, rhs:%v, ree:%v", goldenKey, goldenRule)
	//		}
	//	}
	//}

	//for goldenKey, rules := range goldenKey2Rules {
	//	dtRules := dtKey2Rules[goldenKey]
	//	for _, goldenRule := range rules {
	//		goldenLhsStrs := getGoldenRuleLhsStrs(goldenRule)
	//		isCover := false
	//		for _, dtRule := range dtRules {
	//			lhsStrs := getDTRuleLhsStrs(dtRule)
	//			if inc_rule_dig.Contains(goldenLhsStrs, lhsStrs) {
	//				intersectCount++
	//				isCover = true
	//				break
	//			}
	//		}
	//		if !isCover {
	//			logger.Infof("[getRecallRateOfGuidelines] decision tree not cover golden rule, rhs:%v, ree:%v", goldenKey, goldenRule)
	//		}
	//	}
	//}

	goldenCount := len(goldenRules)
	var recallRate float64
	if goldenCount > 0 {
		recallRate = float64(intersectCount) / float64(goldenCount)
	}

	logger.Infof("[getRecallRateOfGuidelines] finish get recall rate, intersectCount:%v, goldenCount:%v, recallRate:%v", intersectCount, goldenCount, recallRate)
	return recallRate
}

func incRuleGroupByRhsNew(rules []*inc_rule_dig.IncRule) map[string][]*inc_rule_dig.IncRule {
	rhsKey2Rules := make(map[string][]*inc_rule_dig.IncRule)
	for _, rule := range rules {
		rhsKey2Rules[rule.Node.Rhs.PredicateStr] = append(rhsKey2Rules[rule.Node.Rhs.PredicateStr], rule)
	}
	return rhsKey2Rules
}

func dtRuleGroupByRhs(rules []*rds.Rule) map[string][]*rds.Rule {
	rhsKey2Rules := make(map[string][]*rds.Rule)
	for _, rule := range rules {
		rhsKey2Rules[rule.Rhs.PredicateStr] = append(rhsKey2Rules[rule.Rhs.PredicateStr], rule)
	}
	return rhsKey2Rules
}

type GoldenRule struct {
	Id  int
	Ree string
}

func goldenRuleGroupByRhs(rules []string) map[string][]*GoldenRule {
	rhsKey2Rules := make(map[string][]*GoldenRule)
	for i, rule := range rules {
		ruleSplit := strings.Split(rule, "->")
		rhsKey := strings.TrimSpace(ruleSplit[1])
		rhsKey2Rules[rhsKey] = append(rhsKey2Rules[rhsKey], &GoldenRule{Id: i, Ree: rule})
	}
	return rhsKey2Rules
}

func getIncRuleLhsStrs(rule *inc_rule_dig.IncRule) []string {
	lhsStrs := make([]string, len(rule.Node.Lhs))
	for i, p := range rule.Node.Lhs {
		lhsStrs[i] = p.PredicateStr
	}
	return lhsStrs
}

func getDTRuleLhsStrs(rule *rds.Rule) []string {
	lhsStrs := make([]string, len(rule.LhsPredicates))
	for i, p := range rule.LhsPredicates {
		lhsStrs[i] = p.PredicateStr
	}
	return lhsStrs
}

func getGoldenRuleLhsStrs(rule string) []string {
	ruleSplit := strings.Split(rule, "->")
	lhs := strings.TrimSpace(ruleSplit[0])
	lhsSplit := strings.Split(lhs, "^")
	lhsStrs := make([]string, len(lhsSplit))
	for i := range lhsSplit {
		lhsStrs[i] = strings.TrimSpace(lhsSplit[i])
	}
	return lhsStrs
}

func getRulesSortedLhsStr(rules []*inc_rule_dig.IncRule) []string {
	ruleLhsStrs := make([]string, len(rules))
	for i, rule := range rules {
		ruleLhsStr := getSortedLhsStr(rule)
		ruleLhsStrs[i] = ruleLhsStr
	}
	return ruleLhsStrs
}

func getSortedLhsStr(rule *inc_rule_dig.IncRule) string {
	pStrs := make([]string, len(rule.Node.Lhs))
	for i, predicate := range rule.Node.Lhs {
		pStrs[i] = predicate.PredicateStr
	}
	sort.Slice(pStrs, func(i, j int) bool {
		return pStrs[i] < pStrs[j]
	})

	return strings.Join(pStrs, " ^ ")
}

// 获取预估的内存
// totalSize包括: minimal REEs、pruned REEs、samples、PLI的预估内存
// auxiliarySize包括: pruned REEs、samples、PLI的预估内存
func getOutputMemorySize(minimalRules []*inc_rule_dig.IncRule, prunedRules []*inc_rule_dig.IncRule, samples []*inc_rule_dig.SampleNode, taskId int64) (totalSize float64, auxiliarySize float64) {
	minRulesSize := inc_rule_dig.GetIncRulesSize(minimalRules)
	minRulesMB := bytes2MB(minRulesSize)
	prunedRulesSize := inc_rule_dig.GetIncRulesSize(prunedRules)
	prunedRulesMB := bytes2MB(prunedRulesSize)
	samplesSize := inc_rule_dig.GetSampleNodesSize(samples)
	samplesMB := bytes2MB(samplesSize)

	task := table_data.GetTask(taskId)
	PLISize := inc_rule_dig.GetPLISize(task.IndexPLI)
	pliMB := bytes2MB(PLISize)
	logger.Infof("[getOutputMemorySize] minimal REEs size:%v(MB), pruned REEs size:%v(MB), samples size:%v(MB), PLI size:%v(MB)", minRulesMB, prunedRulesMB, samplesMB, pliMB)

	totalSize = minRulesMB + prunedRulesMB + samplesMB + pliMB
	auxiliarySize = prunedRulesMB + samplesMB + pliMB
	return
}

func bytes2MB(size uint64) float64 {
	mb := float64(size) / (1024 * 1024)
	return mb
}

func getOutputMemorySizeNew(
	minimalRules []*inc_rule_dig.IncRule,
	prunedRules []*inc_rule_dig.IncRule,
	samples []*inc_rule_dig.SampleNode,
	taskId int64,
	isBatch bool,
	useWithoutSamping bool) (outputSize float64, minimalSize float64, auxiliarySize float64, decisionTreeSize float64, decisionTreeRules []*rds.Rule) {

	var err error
	decisionTreeRules, err = inc_rule_dig.GetDecisionTreeRules(taskId)
	if err != nil {
		logger.Warnf("[getOutputMemorySizeNew] GetDecisionTreeRules is empty, error:%v", err)
		decisionTreeRules = []*rds.Rule{}
	}

	minRulesSize := inc_rule_dig.GetIncRulesSize(minimalRules)
	minRulesMB := bytes2MB(minRulesSize)
	prunedRulesSize := inc_rule_dig.GetIncRulesSize(prunedRules)
	prunedRulesMB := bytes2MB(prunedRulesSize)
	samplesSize := inc_rule_dig.GetSampleNodesSize(samples)
	samplesMB := bytes2MB(samplesSize)
	decisionTreeRulesSize := inc_rule_dig.GetRulesSize(decisionTreeRules)
	decisionTreeRulesMB := bytes2MB(decisionTreeRulesSize)

	task := table_data.GetTask(taskId)
	PLISize := inc_rule_dig.GetPLISize(task.IndexPLI)
	pliMB := bytes2MB(PLISize)
	logger.Infof("[getOutputMemorySize] minimal REEs size:%v(MB), pruned REEs size:%v(MB), samples size:%v(MB), PLI size:%v(MB), decision size:%v(MB)", minRulesMB, prunedRulesMB, samplesMB, pliMB, decisionTreeRulesMB)

	minimalSize = minRulesMB
	decisionTreeSize = decisionTreeRulesMB
	if isBatch {
		auxiliarySize = pliMB
	} else if useWithoutSamping {
		auxiliarySize = prunedRulesMB + pliMB
	} else {
		auxiliarySize = prunedRulesMB + samplesMB + pliMB
	}
	outputSize = minimalSize + auxiliarySize + decisionTreeSize
	return
}

func taskTreeGroupByRhs(taskTrees []*task_tree.TaskTree) (map[string][]*task_tree.TaskTree, map[string]rds.Predicate) {
	groupByResult := make(map[string][]*task_tree.TaskTree)
	key2Rhs := make(map[string]rds.Predicate)
	for _, taskTree := range taskTrees {
		if taskTree == nil {
			continue
		}
		key := taskTree.Rhs.Key()
		groupByResult[key] = append(groupByResult[key], taskTree)
		key2Rhs[key] = taskTree.Rhs
	}
	return groupByResult, key2Rhs
}

func taskTreeGroupByLayer(taskTrees []*task_tree.TaskTree) map[int][]*task_tree.TaskTree /* layerNum -> nodes */ {
	groupByResult := make(map[int][]*task_tree.TaskTree)

	for _, taskTree := range taskTrees {
		if taskTree == nil {
			continue
		}
		layerNum := len(taskTree.Lhs)
		groupByResult[layerNum] = append(groupByResult[layerNum], taskTree)
	}

	return groupByResult
}

func printSample(sampleId int, sample *inc_rule_dig.SampleNode, text string) {
	logger.Infof("text:%v, sampleId:%v, current:%v, maxConf:%v, minConf:%v, candidate:%v", text, sampleId, inc_rule_dig.GetIncRuleRee(sample.CurrentNode), sample.MaxConf, sample.MinConf, getLhsStr(sample.CurrentNode.Node.LhsCandidate))
	for _, predecessor := range sample.PredecessorNodes {
		logger.Infof("text:%v, sampleId:%v, predecessorLayer:%v, predecessor:%v, candidate:%v", text, sampleId, len(predecessor.Node.Lhs), inc_rule_dig.GetIncRuleRee(predecessor), getLhsStr(predecessor.Node.LhsCandidate))
	}
	for _, neighbor := range sample.NeighborNodes {
		logger.Infof("text:%v, sampleId:%v, neighbor:%v, candidate:%v", text, sampleId, inc_rule_dig.GetIncRuleRee(neighbor), getLhsStr(neighbor.Node.LhsCandidate))
	}
}

func getLhsStr(lhs []rds.Predicate) []string {
	pStrs := make([]string, len(lhs))
	for i, p := range lhs {
		pStrs[i] = p.PredicateStr
	}
	return pStrs
}

func printParent2CountTmp(sampleNodes []*inc_rule_dig.SampleNode) {
	parent2Count := make(map[string]int)
	for _, sampleNode := range sampleNodes {
		for _, predecessorNode := range sampleNode.PredecessorNodes {
			ree := inc_rule_dig.GetIncRuleRee(predecessorNode)
			if count, ok := parent2Count[ree]; !ok {
				parent2Count[ree] = 1
			} else {
				parent2Count[ree] = count + 1
			}
		}
	}

	for ree, count := range parent2Count {
		if count > 1 {
			logger.Infof("[调试日志] parent:%v, count:%v", ree, count)
		}
	}
}

// getMinimizeProcessRules 对规则做最小化处理
func getMinimizeProcessRules(incRules []*inc_rule_dig.IncRule) []*inc_rule_dig.IncRule {
	resultRules := make([]*inc_rule_dig.IncRule, 0)
	key2Rules, _ := incRuleGroupByRhs(incRules)
	//key2Rules := incRuleGroupByRhsNew(incRules)
	var wg sync.WaitGroup
	var mutex sync.Mutex
	ch := utils.GenTokenChan(1)
	for _, rules := range key2Rules {
		//if key != "t0.StateAvg=t1.StateAvg" {
		//	continue
		//}
		wg.Add(1)
		<-ch
		go func(rules []*inc_rule_dig.IncRule) {
			defer func() {
				ch <- struct{}{}
				wg.Done()
			}()
			uniqueRules := removeDuplicates(rules)
			logger.Infof("rules size:%v, unique size:%v", len(rules), len(uniqueRules))
			saveIndex := make([]int, 0)
			size := len(uniqueRules)
			for i := 0; i < size; i++ {
				isContain := false
				for j := i + 1; j < size; j++ {
					//if i == j {
					//	continue
					//}
					//if inc_rule_dig.ContainAllPredicates(rules[i].Node.Lhs, rules[j].Node.Lhs) {
					//	isContain = true
					//	logger.Infof("[调试日志-getMinimizeProcessRules] fiter index:%v-ree:%v, index:%v-save ree:%v", i, inc_rule_dig.GetIncRuleRee(rules[i]), j, inc_rule_dig.GetIncRuleRee(rules[j]))
					//	break
					//}
					if len(uniqueRules[i].Node.Lhs) >= len(uniqueRules[j].Node.Lhs) {
						if inc_rule_dig.ContainAllPredicates(uniqueRules[i].Node.Lhs, uniqueRules[j].Node.Lhs) {
							isContain = true
							logger.Debugf("[getMinimizeProcessRules] fiter index:%v-ree:%v, index:%v-save ree:%v", i, inc_rule_dig.GetIncRuleRee(uniqueRules[i]), j, inc_rule_dig.GetIncRuleRee(uniqueRules[j]))
							saveIndex = append(saveIndex, j)
							break
						}
					} else {
						if inc_rule_dig.ContainAllPredicates(uniqueRules[j].Node.Lhs, uniqueRules[i].Node.Lhs) {
							isContain = true
							logger.Debugf("[getMinimizeProcessRules] fiter index:%v-ree:%v, index:%v-save ree:%v", j, inc_rule_dig.GetIncRuleRee(uniqueRules[j]), i, inc_rule_dig.GetIncRuleRee(uniqueRules[i]))
							saveIndex = append(saveIndex, i)
							break
						}
					}
				}
				if !isContain {
					saveIndex = append(saveIndex, i)
				}
			}
			saveIndex = utils2.Distinct(saveIndex)
			saveRules := make([]*inc_rule_dig.IncRule, 0, len(saveIndex))
			for _, index := range saveIndex {
				saveRules = append(saveRules, uniqueRules[index])
			}
			mutex.Lock()
			resultRules = append(resultRules, saveRules...)
			mutex.Unlock()
		}(rules)
	}
	wg.Wait()
	logger.Infof("[getMinimizeProcessRules] src rules size:%v, minimize rules size:%v", len(incRules), len(resultRules))
	return resultRules
}

func removeDuplicates(incRules []*inc_rule_dig.IncRule) []*inc_rule_dig.IncRule {
	unique := make([]*inc_rule_dig.IncRule, 0)
	seen := make(map[string]struct{})
	for _, rule := range incRules {
		pStrs := make([]string, len(rule.Node.Lhs))
		for i, p := range rule.Node.Lhs {
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

// getMinimizeProcessRulesNew 对规则做最小化处理
func getMinimizeProcessRulesNew(incRules []*inc_rule_dig.IncRule) []*inc_rule_dig.IncRule {
	resultRules := make([]*inc_rule_dig.IncRule, 0)
	key2Rules, _ := incRuleGroupByRhs(incRules)
	//key2Rules := incRuleGroupByRhsNew(incRules)
	var wg sync.WaitGroup
	var mutex sync.Mutex
	ch := utils.GenTokenChan(1)
	for _, rules := range key2Rules {
		//if key != "t0.StateAvg=t1.StateAvg" {
		//	continue
		//}
		wg.Add(1)
		<-ch
		go func(rules []*inc_rule_dig.IncRule) {
			defer func() {
				ch <- struct{}{}
				wg.Done()
			}()
			uniqueRules := removeDuplicates(rules)
			logger.Infof("rules size:%v, unique size:%v", len(rules), len(uniqueRules))
			saveIndex := make([]int, 0)
			size := len(uniqueRules)
			for i := 0; i < size; i++ {
				isContain := false
				for j := 0; j < size; j++ {
					if i == j {
						continue
					}
					if inc_rule_dig.ContainAllPredicates(uniqueRules[i].Node.Lhs, uniqueRules[j].Node.Lhs) {
						isContain = true
						//logger.Infof("[调试日志-getMinimizeProcessRules] fiter index:%v-ree:%v, index:%v-save ree:%v", i, inc_rule_dig.GetIncRuleRee(rules[i]), j, inc_rule_dig.GetIncRuleRee(rules[j]))
						break
					}
					//if len(uniqueRules[i].Node.Lhs) >= len(uniqueRules[j].Node.Lhs) {
					//	if inc_rule_dig.ContainAllPredicates(uniqueRules[i].Node.Lhs, uniqueRules[j].Node.Lhs) {
					//		isContain = true
					//		logger.Debugf("[getMinimizeProcessRules] fiter index:%v-ree:%v, index:%v-save ree:%v", i, inc_rule_dig.GetIncRuleRee(uniqueRules[i]), j, inc_rule_dig.GetIncRuleRee(uniqueRules[j]))
					//		saveIndex = append(saveIndex, j)
					//		break
					//	}
					//} else {
					//	if inc_rule_dig.ContainAllPredicates(uniqueRules[j].Node.Lhs, uniqueRules[i].Node.Lhs) {
					//		isContain = true
					//		logger.Debugf("[getMinimizeProcessRules] fiter index:%v-ree:%v, index:%v-save ree:%v", j, inc_rule_dig.GetIncRuleRee(uniqueRules[j]), i, inc_rule_dig.GetIncRuleRee(uniqueRules[i]))
					//		saveIndex = append(saveIndex, i)
					//		break
					//	}
					//}
				}
				if !isContain {
					saveIndex = append(saveIndex, i)
				}
			}
			saveIndex = utils2.Distinct(saveIndex)
			saveRules := make([]*inc_rule_dig.IncRule, 0, len(saveIndex))
			for _, index := range saveIndex {
				saveRules = append(saveRules, uniqueRules[index])
			}
			mutex.Lock()
			resultRules = append(resultRules, saveRules...)
			mutex.Unlock()
		}(rules)
	}
	wg.Wait()
	logger.Infof("[getMinimizeProcessRules] src rules size:%v, minimize rules size:%v", len(incRules), len(resultRules))
	return resultRules
}

func generateDecisionTreeY(tableId string, gv *global_variables.GlobalV) {
	task := table_data.GetTask(gv.TaskId)
	rowSize := task.TableRowSize[tableId]
	skipColumn := map[string]int{"id": 1, "row_id": 1, "update_time": 1, "${df}": 1, "${mk}": 1}
	columnsType := task.TableColumnTypes[tableId]

	var wg sync.WaitGroup
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

			cardinality := len(task.IndexPLI[tableId][columnId])
			logger.Infof("columnId=%s, cardinality=%d", columnId, cardinality)
			nonConstPredSupport := 0
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

			// 一列的值都不相同,或者基本都相同,该列去掉
			if nonConstPredSupport <= 0 || (float64(nonConstPredSupport))/float64(rowSize*(rowSize-1)) >= gv.PredicateSupportLimit {
				return
			}

			// 纯单行规则数值类型不作为Y出现 dataType == rds_config.IntType || dataType == rds_config.FloatType ||
			if (dataType == rds_config.BoolType ||
				(dataType == rds_config.StringType && cardinality < gv.EnumSize)) && cardinality > 1 && !udf_column.IsUdfColumn(columnId) {
				predicateSupportLock.Lock()
				if _, exist := gv.Table2DecisionY[tableId]; !exist {
					gv.Table2DecisionY[tableId] = make([]string, 0)
				}
				gv.Table2DecisionY[tableId] = append(gv.Table2DecisionY[tableId], columnId)
				predicateSupportLock.Unlock()
			}
		}(columnId, dataType)
	}
	wg.Wait()
}

func generateDecisionTreeYNew(tableId string, gv *global_variables.GlobalV) {
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

	var wg sync.WaitGroup

	//构建谓词
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

			cardinality := len(task.IndexPLI[tableId][columnId])
			logger.Infof("columnId=%s, cardinality=%d", columnId, cardinality)
			nonConstPredSupport := 0
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

			// 尝试下日期类型不生成结构性谓词
			if dataType == rds_config.TimeType {
				return
			}
		}(columnId, dataType)
	}
	wg.Wait()
}

func dropTaskData(taskId int64) {
	type request struct {
		TaskId int64
	}
	var res = request{TaskId: taskId}
	resByte, _ := json.Marshal(res)
	req := &pb.UniversalReq{
		Kind: 4,
		Data: resByte,
	}
	resq := &pb.UniversalResp{}

	client, _ := distribute.NewClient(nil)
	client.Broadcast(pb.Rock_UniversalCall_FullMethodName, req, resq, nil)
	endpoint, err := distribute.GEtcd.GetMasterEndpoint(distribute.TRANSPORT_TYPE_GRPC)
	if err != nil {
		client.RoundRobin(pb.Rock_UniversalCall_FullMethodName, req, resq, nil, distribute.WithEndpoint(endpoint))
	}
	table_data.DropTaskId(taskId)
	client.Close()
}

func loadData(tableIds []string, UDFTabCols []udf.UDFTabCol, taskId int64, limit int) int64 {
	start := time.Now()
	startTime := time.Now().UnixMilli()
	type request2 struct {
		TaskId     int64
		TableIds   []string
		UDFTabCols []udf.UDFTabCol
		Limit      int
	}
	var res = request2{TaskId: taskId, TableIds: tableIds, UDFTabCols: UDFTabCols, Limit: limit}
	resByte, _ := json.Marshal(res)
	req := &pb.UniversalReq{
		Kind: 3,
		Data: resByte,
	}
	resq := &pb.UniversalResp{}

	client, _ := distribute.NewClient(nil)
	client.Broadcast(pb.Rock_UniversalCall_FullMethodName, req, resq, nil)
	endpoint, err := distribute.GEtcd.GetMasterEndpoint(distribute.TRANSPORT_TYPE_GRPC)
	if err != nil {
		client.RoundRobin(pb.Rock_UniversalCall_FullMethodName, req, resq, nil, distribute.WithEndpoint(endpoint))
	}
	table_data.LoadSchema(taskId, tableIds)
	table_data.SetBlockingInfo(taskId, UDFTabCols)
	table_data.LoadDataCreatePli(taskId, tableIds)
	if limit > 0 {
		table_data.CreateSamplePLI(taskId, limit)
	}
	storage_utils.DropCacheDataBase()
	table_data.CreateIndex(taskId)
	if limit > 0 {
		table_data.CreateSampleIndex(taskId, limit)
	}
	client.Close()
	logger.Infof("预热数据完成,耗时 %s", utils2.DurationString(start))
	return time.Now().UnixMilli() - startTime
}

func singleRowRuleDig(gv *global_variables.GlobalV) {
	//0.生成备选Y，已生成
	//1.分发任务
	client, err := distribute.NewClient(nil)
	if err != nil {
		logger.Error("new client err=", err)
		return
	}
	defer client.Close()
	for tableId, yColumns := range gv.Table2DecisionY {
		logger.Infof("taskid:%v, tableid:%v,单行规则Y的个数:%v", gv.TaskId, tableId, len(yColumns))
		logger.Infof("Y:%v", yColumns)
		_, rowSize, _, _ := storage_utils.GetSchemaInfo(tableId)
		oriColumnsType := gv.Table2ColumnType[tableId]
		columnsType := map[string]string{}
		for colName, colType := range oriColumnsType {
			if !udf_column.IsUdfColumn(colName) {
				columnsType[colName] = colType
			}
		}
		for _, yColumn := range yColumns {
			r := &request.SingleRowDecisionTreeReq{
				TableName:    tableId,
				TaskId:       gv.TaskId,
				CR:           gv.CR,
				FTR:          gv.FTR,
				StopTask:     gv.StopTask,
				MaxTreeDepth: gv.DecisionTreeMaxDepth,

				YColumn:                yColumn,
				ColumnType:             columnsType,
				TableLength:            rowSize,
				SampleThreshold2Ratio:  gv.DecisionTreeSampleThreshold2Ratio,
				DecisionTreeMaxRowSize: gv.DecisionTreeMaxRowSize,
			}
			reqBytes, _ := json.Marshal(r)
			req := &pb.SingleRowDecisionTreeReq{
				Req: reqBytes,
			}
			resp := &pb.SingleRowDecisionTreeResp{}
			client.RoundRobin(pb.Rock_ExecuteSingleRowDecisionTree_FullMethodName, req, resp, func() {
				ruleSize := resp.RuleSize
				gv.SingleRuleSize += ruleSize
				logger.Debugf("taskId: %v, tableId: %s, yColumn: %s, finish decision tre, ruleSize: %v", gv.TaskId, r.TableName, r.YColumn, ruleSize)
			})
		}
	}

}
