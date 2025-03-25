package trees

import (
	"encoding/json"
	"github.com/zeromicro/go-zero/core/hash"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/distribute"
	"gitlab.grandhoo.com/rock/rock-share/global/enum"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock-share/rpc/grpc/pb"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables/task_tree"
	"gitlab.grandhoo.com/rock/rock_v3/inc_rule_dig"
	"gitlab.grandhoo.com/rock/rock_v3/intersection"
	"gitlab.grandhoo.com/rock/rock_v3/request"
	"gitlab.grandhoo.com/rock/rock_v3/topk"
	"gitlab.grandhoo.com/rock/rock_v3/utils"
	"gitlab.grandhoo.com/rock/rock_v3/utils/blocking/udf_column"
	"gitlab.grandhoo.com/rock/rock_v3/utils/db_util"
	"gitlab.grandhoo.com/rock/rock_v3/utils/storage_utils"
	"gitlab.grandhoo.com/rock/rock_v3/utils/train_data_util"
	"math"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func IncExpandSupport(inputParam *inc_rule_dig.IncExpandSupportInput) *inc_rule_dig.IncExpandSupportOutput {
	minimalIncRules := make([]*inc_rule_dig.IncRule, 0)
	prunedIncRules := make([]*inc_rule_dig.IncRule, 0)

	// 待扩展的规则需要先重新计算一次,防止遗漏当前层
	needExpandIncRules, currLayerMinimalRules, currLayerPrunedRules := getCurrentLayer(inputParam.NeedExpandIncRules, inputParam.Gv)
	minimalIncRules = append(minimalIncRules, currLayerMinimalRules...)
	prunedIncRules = append(prunedIncRules, currLayerPrunedRules...)

	if len(needExpandIncRules) < 1 {
		return &inc_rule_dig.IncExpandSupportOutput{
			MinimalIncRules: minimalIncRules,
			PrunedIncRules:  prunedIncRules,
		}
	}

	layer := getMinLayer(needExpandIncRules)
	//var finish bool
	currLayer := needExpandIncRules[layer]
	//var mu sync.Mutex
	var nextLayer []*inc_rule_dig.IncRule

	// 新流程，将一整层的节点汇总起来然后给查错那边处理计算出supp和confidence
	// 对于需要继续拓展的节点，先执行决策树和计算对应子节点的giniIndex
	// 选出giniIndex最高的作为下一层的top1
	// 下层的所有节点也都是需要计算的
	// 对下层仍需要继续拓展的节点，迭代和top1节点求overlap，选出最小的topK个节点，然后进行拓展
	for len(currLayer) > 0 {
		layer++
		if layer > inputParam.Gv.TreeMaxLevel {
			break
		}
		children := make([]*inc_rule_dig.IncRule, 0)
		for _, incRule := range currLayer {
			utils.SortPredicates(incRule.Node.Lhs, false)
			utils.SortPredicates(incRule.Node.LhsCandidate, false)
			utils.SortPredicates(incRule.Node.LhsCrossCandidate, false)

			//检查，记录当前lhs有关的
			related, inRelated, crossRelated, crossInRelated := checkCandidatesRelatedNew(incRule.Node)
			// 可以在lhs中新添加的谓词集合
			// todo 这里跨表谓词的候选集需要加入一些限制，根据测试造的规则进行限制，主外键的t0和t1成对出现，t0跨表倒t2之后，不再出现t0相关的谓词
			tmp := make([]rds.Predicate, 0)
			tmp = append(tmp, related...)
			tmp = append(tmp, crossRelated...)

			hasCrossPredicate := len(incRule.Node.TableId2index) > 1

			for i, lhsP := range tmp {
				// 当规则已经是跨表的时候,表内谓词的support必须小于某个值
				if hasCrossPredicate && lhsP.Support > inputParam.Gv.CrossTablePredicateSupp {
					continue
				}
				// 需要保持谓词的编号是统一，当碰到跨表谓词的时候需要对谓词进行一定的变形
				// 比如当前谓词中有t0.a=t1.a，表示A表，新添加谓词t0.b=t2.a，t0.b来自B表，这时候需要将谓词转换成t0.a=t2.b，并且添加谓词t1.a=t3.b
				tableId2index := make(map[string]int, len(incRule.Node.TableId2index))
				for tableId, indexId := range incRule.Node.TableId2index {
					tableId2index[tableId] = indexId
				}
				childLhs := make([]rds.Predicate, len(incRule.Node.Lhs))
				copy(childLhs, incRule.Node.Lhs)
				if i >= len(related) { //表示该谓词是跨表谓词
					if len(tableId2index) > 2 { // 最多三张表相关联
						continue
					}
					childLhs = append(childLhs, utils.GenerateConnectPredicate(lhsP, tableId2index)...)
				} else {
					utils.CheckPredicateIndex(&lhsP, tableId2index)
					childLhs = append(childLhs, lhsP)
				}

				child := &task_tree.TaskTree{
					Rhs:           inputParam.Rhs,
					Lhs:           childLhs,
					TableId2index: tableId2index,
					//GiniIndex:                  tmpGini[i],
					LhsCandidate:               []rds.Predicate{},
					LhsCrossCandidate:          []rds.Predicate{},
					LhsCandidateGiniIndex:      []float64{},
					LhsCrossCandidateGiniIndex: []float64{},
				}
				if i < len(related) { //还没到联表的谓词
					child.LhsCandidate = make([]rds.Predicate, 0)
					child.LhsCandidate = append(child.LhsCandidate, related[i+1:]...)
					child.LhsCandidate = append(child.LhsCandidate, inRelated...)
					child.LhsCrossCandidate = make([]rds.Predicate, len(incRule.Node.LhsCrossCandidate))
					copy(child.LhsCrossCandidate, incRule.Node.LhsCrossCandidate)
				} else { //当前为联表谓词
					child.LhsCandidate = make([]rds.Predicate, 0)
					child.LhsCandidate = append(child.LhsCandidate, inRelated...)
					child.LhsCrossCandidate = make([]rds.Predicate, 0)
					child.LhsCrossCandidate = append(child.LhsCrossCandidate, tmp[i+1:]...)
					child.LhsCrossCandidate = append(child.LhsCrossCandidate, crossInRelated...)
				}
				if inputParam.Gv.MultiTable {
					childIncRule := &inc_rule_dig.IncRule{
						Node: child,
					}
					children = append(children, childIncRule)
					continue
				}
			}
		}

		//printLayerChildren(inputParam.Rhs, layer, children)

		if len(children) < 1 {
			logger.Infof("需要计算的节点数为0")
			return &inc_rule_dig.IncExpandSupportOutput{
				MinimalIncRules: minimalIncRules,
				PrunedIncRules:  prunedIncRules,
			}
		}
		// 汇总一层需要计算的节点，计算出这一组节点各自的supp和confidence
		//hasRules, prunes, isDeletes := calIncRules(children, inputParam.Gv)
		hasRules, prunes, isDeletes := calIncRulesNew(children, inputParam.Gv)
		// 根据返回结果，处理那些节点可以生成规则，哪些节点需要走决策树和计算gini系数
		for i := range hasRules { //和上面循环的i顺序应该一致
			child := children[i]
			hasRule := hasRules[i]
			prune := prunes[i]
			if inputParam.Gv.UsePruneVersion {
				isDelete := isDeletes[i]
				if isDelete {
					for index, nextNode := range nextLayer {
						isDeleted := false
						nextNode.Node.LhsCandidate, isDeleted = deleteLhsCandidatesPredicate(nextNode, child)
						if isDeleted {
							addChildLhsCandidatesNew(child, nextNode, index, nextLayer)
							//logger.Infof("[调试日志] ****addChildAfter child:%v, candidate:%v", inc_rule_dig.GetIncRuleRee(child), getLhsStr(child.Node.LhsCandidate))
						}
					}
					if !hasRule {
						for index, nextNode := range prunedIncRules {
							isDeleted := false
							nextNode.Node.LhsCandidate, isDeleted = deleteLhsCandidatesPredicate(nextNode, child)
							if isDeleted {
								addChildLhsCandidatesNew(child, nextNode, index, prunedIncRules)
								//logger.Infof("[调试日志] ****addChildAfter child:%v, candidate:%v", inc_rule_dig.GetIncRuleRee(child), getLhsStr(child.Node.LhsCandidate))
							}
						}
					}
				}
			}
			if prune {
				// 终止搜索的条件: (1) φ是最小的λ限制的, 或者(2) φ的支持度低于σ
				if hasRule {
					minimalIncRules = append(minimalIncRules, child)
				} else {
					// 被剪枝的集合
					prunedIncRules = append(prunedIncRules, child)
				}
				continue
			}

			nextLayer = append(nextLayer, child)
		}

		if len(needExpandIncRules[layer]) > 0 {
			nextLayer = append(nextLayer, needExpandIncRules[layer]...)
		}

		// 执行决策树
		if inputParam.Gv.UseDecisionTree {
			distributeNotSatisfyNodeOfInc(nextLayer, false, inputParam.Gv)
		}
		currLayer = nextLayer
		nextLayer = make([]*inc_rule_dig.IncRule, 0)

		if layer >= inputParam.Gv.TreeMaxLevel {
			break
		}
	}

	logger.Infof("predicate type[%v], predicate str[%v]", inputParam.Rhs.PredicateType, inputParam.Rhs.PredicateStr)
	return &inc_rule_dig.IncExpandSupportOutput{
		MinimalIncRules: minimalIncRules,
		PrunedIncRules:  prunedIncRules,
	}
}

func IncExpandConfidence(inputParam *inc_rule_dig.IncExpandConfidenceInput) *inc_rule_dig.IncExpandConfidenceOutput {
	minimalIncRules := make([]*inc_rule_dig.IncRule, 0)
	sampleNodesResult := make([]*inc_rule_dig.SampleNode, 0)

	needExpandIncRules, currLayerMinimalRules, _ := getCurrentLayer(inputParam.NeedExpandIncRules, inputParam.Gv)
	minimalIncRules = append(minimalIncRules, currLayerMinimalRules...)

	if len(needExpandIncRules) < 1 {
		return &inc_rule_dig.IncExpandConfidenceOutput{
			MinimalIncRules:   minimalIncRules,
			UpdateSampleNodes: inputParam.SampleNodes,
		}
	}

	layer2SampleNodes := sampleNodesGroupByLayer(inputParam.SampleNodes)

	layer := getMinLayer(needExpandIncRules)
	currLayer := needExpandIncRules[layer]
	var nextLayer []*inc_rule_dig.IncRule

	// 新流程，将一整层的节点汇总起来然后给查错那边处理计算出supp和confidence
	// 对于需要继续拓展的节点，先执行决策树和计算对应子节点的giniIndex
	// 选出giniIndex最高的作为下一层的top1
	// 下层的所有节点也都是需要计算的
	// 对下层仍需要继续拓展的节点，迭代和top1节点求overlap，选出最小的topK个节点，然后进行拓展
	for len(currLayer) > 0 {
		layer++
		if layer > inputParam.Gv.TreeMaxLevel {
			break
		}
		children := make([]*inc_rule_dig.IncRule, 0)
		childrenPreLayerIndex := make([]int, 0)
		for index, incRule := range currLayer {
			utils.SortPredicates(incRule.Node.Lhs, false)
			utils.SortPredicates(incRule.Node.LhsCandidate, false)
			utils.SortPredicates(incRule.Node.LhsCrossCandidate, false)

			//检查，记录当前lhs有关的
			related, inRelated, crossRelated, crossInRelated := checkCandidatesRelatedNew(incRule.Node)
			// 可以在lhs中新添加的谓词集合
			// todo 这里跨表谓词的候选集需要加入一些限制，根据测试造的规则进行限制，主外键的t0和t1成对出现，t0跨表倒t2之后，不再出现t0相关的谓词
			tmp := make([]rds.Predicate, 0)
			tmp = append(tmp, related...)
			tmp = append(tmp, crossRelated...)

			hasCrossPredicate := len(incRule.Node.TableId2index) > 1

			for i, lhsP := range tmp {
				// 当规则已经是跨表的时候,表内谓词的support必须小于某个值
				if hasCrossPredicate && lhsP.Support > inputParam.Gv.CrossTablePredicateSupp {
					continue
				}
				// 需要保持谓词的编号是统一，当碰到跨表谓词的时候需要对谓词进行一定的变形
				// 比如当前谓词中有t0.a=t1.a，表示A表，新添加谓词t0.b=t2.a，t0.b来自B表，这时候需要将谓词转换成t0.a=t2.b，并且添加谓词t1.a=t3.b
				tableId2index := make(map[string]int, len(incRule.Node.TableId2index))
				for tableId, indexId := range incRule.Node.TableId2index {
					tableId2index[tableId] = indexId
				}
				childLhs := make([]rds.Predicate, len(incRule.Node.Lhs))
				copy(childLhs, incRule.Node.Lhs)
				if i >= len(related) { //表示该谓词是跨表谓词
					if len(tableId2index) > 2 { // 最多三张表相关联
						continue
					}
					childLhs = append(childLhs, utils.GenerateConnectPredicate(lhsP, tableId2index)...)
				} else {
					utils.CheckPredicateIndex(&lhsP, tableId2index)
					childLhs = append(childLhs, lhsP)
				}

				child := &task_tree.TaskTree{
					Rhs:           inputParam.Rhs,
					Lhs:           childLhs,
					TableId2index: tableId2index,
					//GiniIndex:                  tmpGini[i],
					LhsCandidate:               []rds.Predicate{},
					LhsCrossCandidate:          []rds.Predicate{},
					LhsCandidateGiniIndex:      []float64{},
					LhsCrossCandidateGiniIndex: []float64{},
				}
				if i < len(related) { //还没到联表的谓词
					child.LhsCandidate = make([]rds.Predicate, 0)
					child.LhsCandidate = append(child.LhsCandidate, related[i+1:]...)
					child.LhsCandidate = append(child.LhsCandidate, inRelated...)
					child.LhsCrossCandidate = make([]rds.Predicate, len(incRule.Node.LhsCrossCandidate))
					copy(child.LhsCrossCandidate, incRule.Node.LhsCrossCandidate)
				} else { //当前为联表谓词
					child.LhsCandidate = make([]rds.Predicate, 0)
					child.LhsCandidate = append(child.LhsCandidate, inRelated...)
					child.LhsCrossCandidate = make([]rds.Predicate, 0)
					child.LhsCrossCandidate = append(child.LhsCrossCandidate, tmp[i+1:]...)
					child.LhsCrossCandidate = append(child.LhsCrossCandidate, crossInRelated...)
				}
				if inputParam.Gv.MultiTable {
					childIncRule := &inc_rule_dig.IncRule{
						Node: child,
					}
					children = append(children, childIncRule)
					childrenPreLayerIndex = append(childrenPreLayerIndex, index)
					continue
				}
			}
		}

		if len(children) < 1 {
			logger.Infof("需要计算的节点数为0")
			for _, nodes := range layer2SampleNodes {
				sampleNodesResult = append(sampleNodesResult, nodes...)
			}
			return &inc_rule_dig.IncExpandConfidenceOutput{
				MinimalIncRules:   minimalIncRules,
				UpdateSampleNodes: sampleNodesResult,
			}
		}
		//printLayerChildren(inputParam.Rhs, layer, children)

		// 汇总一层需要计算的节点，计算出这一组节点各自的supp和confidence
		//hasRules, prunes, isDeletes := calIncRules(children, inputParam.Gv)
		hasRules, prunes, isDeletes := calIncRulesNew(children, inputParam.Gv)
		// 根据返回结果，处理那些节点可以生成规则，哪些节点需要走决策树和计算gini系数
		for i := range hasRules { //和上面循环的i顺序应该一致
			child := children[i]
			hasRule := hasRules[i]
			prune := prunes[i]
			if inputParam.Gv.UsePruneVersion {
				isDelete := isDeletes[i]
				if isDelete {
					for index, nextNode := range nextLayer {
						isDeleted := false
						nextNode.Node.LhsCandidate, isDeleted = deleteLhsCandidatesPredicate(nextNode, child)
						if isDeleted {
							addChildLhsCandidatesNew(child, nextNode, index, nextLayer)
							//logger.Infof("[调试日志] ****addChildAfter child:%v, candidate:%v", inc_rule_dig.GetIncRuleRee(child), getLhsStr(child.Node.LhsCandidate))
						}
					}
				}
			}
			if prune {
				// 终止搜索的条件: (1) φ是最小的λ限制的, 或者(2) φ的支持度低于σ
				if hasRule {
					minimalIncRules = append(minimalIncRules, child)
				}
				continue
			}

			if !inputParam.WithoutSampling {
				layer2SampleNodes = updatedSampleNodesNew(layer, layer2SampleNodes, child, currLayer, inputParam.CoverRadius, childrenPreLayerIndex[i])
			}

			nextLayer = append(nextLayer, child)
		}

		if len(needExpandIncRules[layer]) > 0 {
			nextLayer = append(nextLayer, needExpandIncRules[layer]...)
		}

		// 执行决策树
		if inputParam.Gv.UseDecisionTree {
			distributeNotSatisfyNodeOfInc(nextLayer, false, inputParam.Gv)
		}
		currLayer = nextLayer
		nextLayer = make([]*inc_rule_dig.IncRule, 0)

		if layer >= inputParam.Gv.TreeMaxLevel {
			break
		}
	}

	for _, nodes := range layer2SampleNodes {
		sampleNodesResult = append(sampleNodesResult, nodes...)
	}
	logger.Infof("predicate type[%v], predicate str[%v]", inputParam.Rhs.PredicateType, inputParam.Rhs.PredicateStr)
	return &inc_rule_dig.IncExpandConfidenceOutput{
		MinimalIncRules:   minimalIncRules,
		UpdateSampleNodes: sampleNodesResult,
	}
}

func IncExpandIndicator(inputParam *inc_rule_dig.IncExpandInput) *inc_rule_dig.IncExpandOutput {
	minimalIncRules := make([]*inc_rule_dig.IncRule, 0)
	prunedIncRules := make([]*inc_rule_dig.IncRule, 0)
	sampleNodesResult := make([]*inc_rule_dig.SampleNode, 0)

	needExpandIncRules, currLayerMinimalRules, currLayerPrunedRules := getCurrentLayer(inputParam.NeedExpandIncRules, inputParam.Gv)
	minimalIncRules = append(minimalIncRules, currLayerMinimalRules...)
	prunedIncRules = append(prunedIncRules, currLayerPrunedRules...)

	if len(needExpandIncRules) < 1 {
		return &inc_rule_dig.IncExpandOutput{
			MinimalIncRules:    minimalIncRules,
			PrunedIncRules:     prunedIncRules,
			UpdatedSampleNodes: inputParam.SampleNodes,
		}
	}

	layer2SampleNodes := sampleNodesGroupByLayer(inputParam.SampleNodes)

	layer := getMinLayer(needExpandIncRules)
	currLayer := needExpandIncRules[layer]
	var nextLayer []*inc_rule_dig.IncRule

	// 新流程，将一整层的节点汇总起来然后给查错那边处理计算出supp和confidence
	// 对于需要继续拓展的节点，先执行决策树和计算对应子节点的giniIndex
	// 选出giniIndex最高的作为下一层的top1
	// 下层的所有节点也都是需要计算的
	// 对下层仍需要继续拓展的节点，迭代和top1节点求overlap，选出最小的topK个节点，然后进行拓展
	for len(currLayer) > 0 {
		layer++
		if layer > inputParam.Gv.TreeMaxLevel {
			break
		}
		children := make([]*inc_rule_dig.IncRule, 0)
		childrenPreLayerIndex := make([]int, 0)
		for index, incRule := range currLayer {
			utils.SortPredicates(incRule.Node.Lhs, false)
			utils.SortPredicates(incRule.Node.LhsCandidate, false)
			utils.SortPredicates(incRule.Node.LhsCrossCandidate, false)

			//检查，记录当前lhs有关的
			related, inRelated, crossRelated, crossInRelated := checkCandidatesRelatedNew(incRule.Node)
			// 可以在lhs中新添加的谓词集合
			// todo 这里跨表谓词的候选集需要加入一些限制，根据测试造的规则进行限制，主外键的t0和t1成对出现，t0跨表倒t2之后，不再出现t0相关的谓词
			tmp := make([]rds.Predicate, 0)
			tmp = append(tmp, related...)
			tmp = append(tmp, crossRelated...)

			hasCrossPredicate := len(incRule.Node.TableId2index) > 1

			for i, lhsP := range tmp {
				// 当规则已经是跨表的时候,表内谓词的support必须小于某个值
				if hasCrossPredicate && lhsP.Support > inputParam.Gv.CrossTablePredicateSupp {
					continue
				}
				// 需要保持谓词的编号是统一，当碰到跨表谓词的时候需要对谓词进行一定的变形
				// 比如当前谓词中有t0.a=t1.a，表示A表，新添加谓词t0.b=t2.a，t0.b来自B表，这时候需要将谓词转换成t0.a=t2.b，并且添加谓词t1.a=t3.b
				tableId2index := make(map[string]int, len(incRule.Node.TableId2index))
				for tableId, indexId := range incRule.Node.TableId2index {
					tableId2index[tableId] = indexId
				}
				childLhs := make([]rds.Predicate, len(incRule.Node.Lhs))
				copy(childLhs, incRule.Node.Lhs)
				if i >= len(related) { //表示该谓词是跨表谓词
					if len(tableId2index) > 2 { // 最多三张表相关联
						continue
					}
					childLhs = append(childLhs, utils.GenerateConnectPredicate(lhsP, tableId2index)...)
				} else {
					utils.CheckPredicateIndex(&lhsP, tableId2index)
					childLhs = append(childLhs, lhsP)
				}

				child := &task_tree.TaskTree{
					Rhs:           inputParam.Rhs,
					Lhs:           childLhs,
					TableId2index: tableId2index,
					//GiniIndex:                  tmpGini[i],
					LhsCandidate:               []rds.Predicate{},
					LhsCrossCandidate:          []rds.Predicate{},
					LhsCandidateGiniIndex:      []float64{},
					LhsCrossCandidateGiniIndex: []float64{},
				}
				if i < len(related) { //还没到联表的谓词
					child.LhsCandidate = make([]rds.Predicate, 0)
					child.LhsCandidate = append(child.LhsCandidate, related[i+1:]...)
					child.LhsCandidate = append(child.LhsCandidate, inRelated...)
					child.LhsCrossCandidate = make([]rds.Predicate, len(incRule.Node.LhsCrossCandidate))
					copy(child.LhsCrossCandidate, incRule.Node.LhsCrossCandidate)
				} else { //当前为联表谓词
					child.LhsCandidate = make([]rds.Predicate, 0)
					child.LhsCandidate = append(child.LhsCandidate, inRelated...)
					child.LhsCrossCandidate = make([]rds.Predicate, 0)
					child.LhsCrossCandidate = append(child.LhsCrossCandidate, tmp[i+1:]...)
					child.LhsCrossCandidate = append(child.LhsCrossCandidate, crossInRelated...)
				}
				if inputParam.Gv.MultiTable {
					childIncRule := &inc_rule_dig.IncRule{
						Node: child,
					}
					children = append(children, childIncRule)
					childrenPreLayerIndex = append(childrenPreLayerIndex, index)
					continue
				}
			}
		}

		if len(children) < 1 {
			logger.Infof("需要计算的节点数为0")
			for _, nodes := range layer2SampleNodes {
				sampleNodesResult = append(sampleNodesResult, nodes...)
			}
			return &inc_rule_dig.IncExpandOutput{
				MinimalIncRules:    minimalIncRules,
				PrunedIncRules:     prunedIncRules,
				UpdatedSampleNodes: sampleNodesResult,
			}
		}
		//printLayerChildren(inputParam.Rhs, layer, children)

		// 汇总一层需要计算的节点，计算出这一组节点各自的supp和confidence
		//hasRules, prunes, isDeletes := calIncRules(children, inputParam.Gv)
		hasRules, prunes, isDeletes := calIncRulesNew(children, inputParam.Gv)
		// 根据返回结果，处理那些节点可以生成规则，哪些节点需要走决策树和计算gini系数
		for i := range hasRules { //和上面循环的i顺序应该一致
			child := children[i]
			hasRule := hasRules[i]
			prune := prunes[i]
			if inputParam.Gv.UsePruneVersion {
				isDelete := isDeletes[i]
				if isDelete {
					for index, nextNode := range nextLayer {
						isDeleted := false
						nextNode.Node.LhsCandidate, isDeleted = deleteLhsCandidatesPredicate(nextNode, child)
						if isDeleted {
							addChildLhsCandidatesNew(child, nextNode, index, nextLayer)
							//logger.Infof("[调试日志] ****addChildAfter child:%v, candidate:%v", inc_rule_dig.GetIncRuleRee(child), getLhsStr(child.Node.LhsCandidate))
						}
					}
					if !hasRule {
						for index, nextNode := range prunedIncRules {
							isDeleted := false
							nextNode.Node.LhsCandidate, isDeleted = deleteLhsCandidatesPredicate(nextNode, child)
							if isDeleted {
								addChildLhsCandidatesNew(child, nextNode, index, prunedIncRules)
								//logger.Infof("[调试日志] ****addChildAfter child:%v, candidate:%v", inc_rule_dig.GetIncRuleRee(child), getLhsStr(child.Node.LhsCandidate))
							}
						}
					}
				}
			}
			if prune {
				// 终止搜索的条件: (1) φ是最小的λ限制的, 或者(2) φ的支持度低于σ
				if hasRule {
					minimalIncRules = append(minimalIncRules, child)
				} else {
					// 被剪枝的集合
					prunedIncRules = append(prunedIncRules, child)
				}
				continue
			}

			if !inputParam.WithoutSampling {
				layer2SampleNodes = updatedSampleNodesNew(layer, layer2SampleNodes, child, currLayer, inputParam.CoverRadius, childrenPreLayerIndex[i])
			}

			nextLayer = append(nextLayer, child)
		}

		if len(needExpandIncRules[layer]) > 0 {
			nextLayer = append(nextLayer, needExpandIncRules[layer]...)
		}

		// 执行决策树
		if inputParam.Gv.UseDecisionTree {
			distributeNotSatisfyNodeOfInc(nextLayer, false, inputParam.Gv)
		}
		currLayer = nextLayer
		nextLayer = make([]*inc_rule_dig.IncRule, 0)

		if layer >= inputParam.Gv.TreeMaxLevel {
			break
		}
	}

	logger.Infof("predicate type[%v], predicate str[%v]", inputParam.Rhs.PredicateType, inputParam.Rhs.PredicateStr)
	for _, nodes := range layer2SampleNodes {
		sampleNodesResult = append(sampleNodesResult, nodes...)
	}
	return &inc_rule_dig.IncExpandOutput{
		MinimalIncRules:    minimalIncRules,
		PrunedIncRules:     prunedIncRules,
		UpdatedSampleNodes: sampleNodesResult,
	}
}

func getCurrentLayer(layer2IncRules map[int][]*inc_rule_dig.IncRule, gv *global_variables.GlobalV) (layer2NeedExpandRules map[int][]*inc_rule_dig.IncRule, minimalRules []*inc_rule_dig.IncRule, prunedRules []*inc_rule_dig.IncRule) {
	layer2NeedExpandRules = make(map[int][]*inc_rule_dig.IncRule)
	needExpandSize := 0
	for layer, incRules := range layer2IncRules {
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
		if len(needExpandRules) > 0 {
			needExpandSize += len(needExpandRules)
			layer2NeedExpandRules[layer] = needExpandRules
		}
	}
	logger.Infof("[getCurrentLayer] current layer, needExpandRules size:%v, minimalRules size:%v, prunedRules size:%v", needExpandSize, len(minimalRules), len(prunedRules))
	return
}

func getMinLayer(layer2IncRules map[int][]*inc_rule_dig.IncRule) int {
	keys := make([]int, 0, len(layer2IncRules))
	for layer := range layer2IncRules {
		keys = append(keys, layer)
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	return keys[0]
}

func updatedSampleNodes(layer2SampleNodes map[int][]*inc_rule_dig.SampleNode, currentIncRule *inc_rule_dig.IncRule, preLayerNodes []*inc_rule_dig.IncRule, coverRadius int, index int) map[int][]*inc_rule_dig.SampleNode {
	coverSampleNode := getCoverSampleNode(layer2SampleNodes, currentIncRule.Node)
	if coverSampleNode == nil {
		// 没有覆盖
		newSampleNode := createSampleNode(currentIncRule, preLayerNodes, coverRadius, index)
		//sampleNodes = append(sampleNodes, newSampleNode)
		mergeSampleNodes(layer2SampleNodes, newSampleNode)
	} else {
		maxConf := math.Max(coverSampleNode.MaxConf, currentIncRule.FTR)
		coverSampleNode.MaxConf = maxConf
		//coverSampleNode.NeighborNodes = append(coverSampleNode.NeighborNodes, currentIncRule)
	}
	return layer2SampleNodes
}

func updatedSampleNodesNew(layer int, layer2SampleNodes map[int][]*inc_rule_dig.SampleNode, currentIncRule *inc_rule_dig.IncRule, preLayerNodes []*inc_rule_dig.IncRule, coverRadius int, index int) map[int][]*inc_rule_dig.SampleNode {
	if layer == 1 {
		if _, ok := layer2SampleNodes[layer]; !ok {
			newSampleNode := &inc_rule_dig.SampleNode{
				CoverRadius:      coverRadius,
				CurrentNode:      currentIncRule,
				PredecessorNodes: nil, // root
				NeighborNodes:    []*inc_rule_dig.IncRule{},
				MinConf:          currentIncRule.FTR,
				MaxConf:          currentIncRule.FTR,
			}
			layer2SampleNodes[layer] = append(layer2SampleNodes[layer], newSampleNode)
		} else {
			coverSampleNode := layer2SampleNodes[layer][0] // 第一层只有一个采样节点(前驱节点即为root)
			maxConf := math.Max(coverSampleNode.MaxConf, currentIncRule.FTR)
			coverSampleNode.MaxConf = maxConf
			coverSampleNode.NeighborNodes = append(coverSampleNode.NeighborNodes, currentIncRule)
		}
	} else {
		coverSampleNode := getCoverSampleNode(layer2SampleNodes, currentIncRule.Node)
		if coverSampleNode == nil {
			// 没有覆盖
			newSampleNode := createSampleNode(currentIncRule, preLayerNodes, coverRadius, index)
			//logger.Infof("创建新sample, current:%v, FTR:%v, maxConf:%v", inc_rule_dig.GetIncRuleRee(currentIncRule), currentIncRule.FTR, newSampleNode.MaxConf)
			mergeSampleNodes(layer2SampleNodes, newSampleNode)
		} else {
			//logger.Infof("已有sample覆盖, 覆盖前, cover:%v, current:%v, FTR:%v, maxConf:%v", inc_rule_dig.GetIncRuleRee(coverSampleNode.CurrentNode), inc_rule_dig.GetIncRuleRee(currentIncRule), currentIncRule.FTR, coverSampleNode.MaxConf)
			maxConf := math.Max(coverSampleNode.MaxConf, currentIncRule.FTR)
			coverSampleNode.MaxConf = maxConf
			//coverSampleNode.NeighborConfs = append(coverSampleNode.NeighborConfs, currentIncRule.FTR)
			inc_rule_dig.AddConfidence2CDF(currentIncRule.FTR, coverSampleNode.NeighborCDF)
			//logger.Infof("已有sample覆盖, 覆盖后, cover:%v, current:%v, FTR:%v, maxConf:%v", inc_rule_dig.GetIncRuleRee(coverSampleNode.CurrentNode), inc_rule_dig.GetIncRuleRee(currentIncRule), currentIncRule.FTR, coverSampleNode.MaxConf)
		}
	}
	return layer2SampleNodes
}

func createSampleNode(currentNode *inc_rule_dig.IncRule, preLayerNodes []*inc_rule_dig.IncRule, coverRadius int, index int) *inc_rule_dig.SampleNode {
	predecessorNodes := getPredecessorNodes(preLayerNodes, currentNode, coverRadius, index)
	sampleNode := &inc_rule_dig.SampleNode{
		CoverRadius:      coverRadius,
		CurrentNode:      currentNode,
		PredecessorNodes: predecessorNodes,
		NeighborNodes:    []*inc_rule_dig.IncRule{},
		MinConf:          currentNode.FTR,
		MaxConf:          currentNode.FTR,
		NeighborCDF:      make([]int, len(inc_rule_dig.CdfValues)),
	}
	return sampleNode
}

func getPredecessorNodes(preLayerNodes []*inc_rule_dig.IncRule, currentNode *inc_rule_dig.IncRule, coverRadius int, index int) []*inc_rule_dig.IncRule {
	if coverRadius == 1 {
		return []*inc_rule_dig.IncRule{preLayerNodes[index]}
	}
	predecessorNodes := make([]*inc_rule_dig.IncRule, 0, coverRadius)
	predecessorNodes = append(predecessorNodes, preLayerNodes[index])

	tmpPredecessorNodes := make([]*inc_rule_dig.IncRule, 0)
	predicateStr := currentNode.Node.Lhs[len(currentNode.Node.Lhs)-1].PredicateStr
	for i, preLayerNode := range preLayerNodes {
		if index == i {
			continue
		}
		// 当前节点新增的谓词是否在preLayerNode中的候选谓词里
		for _, predicate := range preLayerNode.Node.LhsCandidate {
			if predicate.PredicateStr == predicateStr {
				tmpPredecessorNodes = append(tmpPredecessorNodes, preLayerNode)
				break
			}
		}
	}

	if len(tmpPredecessorNodes) <= (coverRadius - 1) {
		predecessorNodes = append(predecessorNodes, tmpPredecessorNodes...)
		return predecessorNodes
	}
	// 排序后，取前coverRadius个
	sortPredecessorNodes(tmpPredecessorNodes)
	predecessorNodes = append(predecessorNodes, tmpPredecessorNodes[:coverRadius-1]...)

	return predecessorNodes
}

func sortPredecessorNodes(predecessorNodes []*inc_rule_dig.IncRule) {
	sort.SliceStable(predecessorNodes, func(i, j int) bool {
		pxI := utils.GetLhsStr(predecessorNodes[i].Node.Lhs)
		pxJ := utils.GetLhsStr(predecessorNodes[j].Node.Lhs)
		return hash.Hash([]byte(pxI)) < hash.Hash([]byte(pxJ))
	})
}

func getCoverSampleNode(layer2SampleNodes map[int][]*inc_rule_dig.SampleNode, currentNode *task_tree.TaskTree) *inc_rule_dig.SampleNode {
	for _, sampleNode := range layer2SampleNodes[len(currentNode.Lhs)] {
		if isCoverSampleNode(sampleNode, currentNode) {
			return sampleNode
		}
	}
	return nil
}

func isCoverSampleNode(sampleNode *inc_rule_dig.SampleNode, checkNode *task_tree.TaskTree) bool {
	//if len(sampleNode.CurrentNode.Node.Lhs) != len(checkNode.Lhs) {
	//	// 与采样节点不在同一层
	//	return false
	//}

	checkLhsLen := len(checkNode.Lhs)
	checkPredecessorPredStrs := make([]string, 0, checkLhsLen-1)
	for i, predicate := range checkNode.Lhs {
		if i == checkLhsLen-1 {
			break
		}
		checkPredecessorPredStrs = append(checkPredecessorPredStrs, predicate.PredicateStr)
	}
	checkPredecessorStr := strings.Join(checkPredecessorPredStrs, "")

	checkNodeLastPred := checkNode.Lhs[checkLhsLen-1]

	// 遍历前驱节点
	for _, predecessorNode := range sampleNode.PredecessorNodes {
		nodePredecessorPredStrs := make([]string, len(predecessorNode.Node.Lhs))
		for i, predicate := range predecessorNode.Node.Lhs {
			nodePredecessorPredStrs[i] = predicate.PredicateStr
		}
		nodePredecessorStr := strings.Join(nodePredecessorPredStrs, "")
		if checkPredecessorStr != nodePredecessorStr {
			continue
		}

		for _, predicate := range predecessorNode.Node.LhsCandidate {
			if predicate.PredicateStr == checkNodeLastPred.PredicateStr {
				return true
			}
		}
	}
	return false
}

func isCoverSampleNodeOld(sampleNode *inc_rule_dig.SampleNode, node *task_tree.TaskTree) bool {
	if len(sampleNode.CurrentNode.Node.Lhs) != len(node.Lhs) {
		// 与采样节点不在同一层
		return false
	}

	predicateStrs := make([]string, len(node.Lhs))
	for i, predicate := range node.Lhs {
		predicateStrs[i] = predicate.PredicateStr
	}
	nodeStr := getSortedPredicatesStr(predicateStrs)

	// 遍历前驱节点
	for _, predecessorNode := range sampleNode.PredecessorNodes {
		lhsStrs := make([]string, len(predecessorNode.Node.Lhs))
		for i, predicate := range predecessorNode.Node.Lhs {
			lhsStrs[i] = predicate.PredicateStr
		}
		reeLen := len(lhsStrs) + 1
		// 遍历前驱节点下的所有子节点
		for _, predicate := range predecessorNode.Node.LhsCandidate {
			reeStrs := make([]string, 0, reeLen)
			reeStrs = append(reeStrs, predicate.PredicateStr)
			reeNodeStr := getSortedPredicatesStr(reeStrs)
			if reeNodeStr == nodeStr {
				return true
			}
		}
	}
	return false
}

func getSortedPredicatesStr(predicateStrs []string) string {
	sort.Slice(predicateStrs, func(i, j int) bool {
		return predicateStrs[i] < predicateStrs[j]
	})

	return strings.Join(predicateStrs, ":")
}

func calIncRules(incRules []*inc_rule_dig.IncRule, gv *global_variables.GlobalV) (hasRules, prunes, isDeletes []bool) {
	startTime := time.Now().UnixMilli()
	hasRules, prunes, isDeletes = make([]bool, len(incRules)), make([]bool, len(incRules)), make([]bool, len(incRules))
	rowSizes, xSupps, xySupps := make([]int, len(incRules)), make([]int, len(incRules)), make([]int, len(incRules))
	client, _ := distribute.NewClient(nil)
	for i, incRule := range incRules {
		i := i
		logger.Infof("cal lhs:%v, rhs:%v", utils.GetLhsStr(incRule.Node.Lhs), incRule.Node.Rhs.PredicateStr)
		// 检查lhs和lhs+rhs是否都是连通图,两者有一个不满足则不计算该节点
		if !checkNode(incRule.Node) {
			hasRules[i] = false
			prunes[i] = false
			isDeletes[i] = false
			continue
		}

		// 跨表谓词中表内谓词的supp太大,过滤掉
		if !utils.CheckSatisfyPredicateSupp(incRule.Node.Lhs, incRule.Node.TableId2index, gv.CrossTablePredicateSupp) {
			hasRules[i] = false
			prunes[i] = false
			isDeletes[i] = false
			continue
		}

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

		incRule.Node.Lhs = utils.SortPredicatesRelated(incRule.Node.Lhs)

		res := request{
			Lhs:            incRule.Node.Lhs,
			Rhs:            incRule.Node.Rhs,
			TableId2index:  incRule.Node.TableId2index,
			AbandonedRules: gv.AbandonedRules,
			CR:             gv.CR,
			FTR:            gv.FTR,
			TaskId:         gv.TaskId,
			TableId:        gv.TableId,
			ChanSize:       gv.RdsChanSize,
		}
		resBytes, _ := json.Marshal(res)

		req := &pb.UniversalReq{
			Kind: 2,
			Data: resBytes,
		}
		resp := &pb.UniversalResp{}

		client.RoundRobin(pb.Rock_UniversalCall_FullMethodName, req, resp, func() {
			if resp.Err != "" {
				logger.Error(resp.Err)
			}
			var res response
			_ = json.Unmarshal(resp.Data, &res)
			logger.Infof("finish cal lhs:%v, rhs:%v res：%v", utils.GetLhsStr(incRules[i].Node.Lhs), incRules[i].Node.Rhs.PredicateStr, res)

			hasRules[i] = res.HasRule
			prunes[i] = res.Prune
			isDeletes[i] = res.IsDelete
			rowSizes[i] = res.RowSize
			xSupps[i] = res.XSupp
			xySupps[i] = res.XySupp
		}, distribute.WithSync(true))
	}
	client.Close()
	for ruleId := range hasRules {
		//if hasRule {
		rowSize, xSupp, xySupp := rowSizes[ruleId], xSupps[ruleId], xySupps[ruleId]
		var support, confidence float64 = 0, 0
		if rowSize > 0 {
			support = float64(xySupp) / float64(rowSize)
		}
		if xSupp > 0 {
			confidence = float64(xySupp) / float64(xSupp)
		}
		incRules[ruleId].CR = support
		incRules[ruleId].FTR = confidence
		incRules[ruleId].RuleType = 1
		//}
	}
	logger.Infof("cal %v tree's level %v spent time:%vms", incRules[0].Node.Rhs.PredicateStr, len(incRules[0].Node.Lhs), time.Now().UnixMilli()-startTime)
	return hasRules, prunes, isDeletes
}

func BuildTreeOfInc(rootPredicates []rds.Predicate, allPredicates []rds.Predicate, gv *global_variables.GlobalV, coverRadius int) *inc_rule_dig.BatchMinerOutput {
	minimalIncRules, prunedIncRules := make([]*inc_rule_dig.IncRule, 0), make([]*inc_rule_dig.IncRule, 0)
	sampleNodes := make([]*inc_rule_dig.SampleNode, 0)

	var wg1 sync.WaitGroup
	mutex := sync.Mutex{}
	ch := make(chan struct{}, gv.TreeChanSize)
	for _, rhs := range rootPredicates {
		if gv.StopTask {
			logger.Infof("taskId:%v, 收到停止信号，不再跑新的规则发现任务树", gv.TaskId)
			//utils.StopTask(gv.TaskId)
			return nil
		}
		// 相似度谓词不能作为y出现
		if rhs.SymbolType == enum.Similar {
			continue
		}
		ch <- struct{}{}
		wg1.Add(1)
		go func(rhs1 rds.Predicate, allPredicatesFilter1 []rds.Predicate) {
			defer func() {
				wg1.Done()
				<-ch
				if err := recover(); err != nil {
					gv.HasError = true
					s := string(debug.Stack())
					logger.Errorf("recover.err:%v, stack:\n%v", err, s)
				}
			}()
			logger.Infof("taskId:%v,计算根节点为%v的树", gv.TaskId, rhs1.PredicateStr)
			//if rhs1.PredicateStr == "t0.State=t1.State" {
			output := generateTreeOfInc(rhs1, allPredicatesFilter1, gv, coverRadius) //几层就几个谓词
			mutex.Lock()
			minimalIncRules = append(minimalIncRules, output.MinimalIncRules...)
			prunedIncRules = append(prunedIncRules, output.PrunedIncRules...)
			sampleNodes = append(sampleNodes, output.SampleNodes...)
			mutex.Unlock()
			//}
		}(rhs, allPredicates)
	}
	wg1.Wait()

	return &inc_rule_dig.BatchMinerOutput{
		MinimalIncRules: minimalIncRules,
		PrunedIncRules:  prunedIncRules,
		SampleNodes:     sampleNodes,
	}
}

func generateTreeOfInc(rhs rds.Predicate, lhs []rds.Predicate, gv *global_variables.GlobalV, coverRadius int) *inc_rule_dig.BatchMinerOutput {
	startTime := time.Now().UnixMilli()
	var lhsClone []rds.Predicate
	//rhs是常数,则lhs也全要常数
	var lhsOther []rds.Predicate
	var lhsCrossTable []rds.Predicate
	if gv.MultiTable {
		//多表，保留同表及有关联关系的谓词,
		lhsClone = utils.MultiTableFilterLhs(rhs, lhs, gv.Table2JoinTables)
	} else {
		//rhs中包含的列,lhs中不应该再出现
		lhsClone = utils.FilterLhs(rhs, lhs)
	}

	for _, k := range lhsClone {
		if k.PredicateType == 1 {
			lhsOther = append(lhsOther, k)
		} else if k.PredicateType == 2 {
			lhsCrossTable = append(lhsCrossTable, k)
		}
	}
	utils.SortPredicates(lhsOther, false)
	utils.SortPredicates(lhsCrossTable, false)

	root := &task_tree.TaskTree{Rhs: rhs, Lhs: []rds.Predicate{}}
	root.LhsCandidate = lhsOther
	root.LhsCrossCandidate = lhsCrossTable
	// 生成该节点对于已有谓词中涉及的表的对应索引， 之后的流程中碰到了添加跨表谓词到lhs中的时候会用到
	root.TableId2index = generateTableId2tableIndex(append(root.Lhs, root.Rhs))
	if rhs.PredicateType == 3 { //y是跨表谓词的时候，x中一定要出现其对应的跨表谓词
		rhsLeftTableId := rhs.LeftColumn.TableId
		rhsRightTableId := rhs.RightColumn.TableId
		joinedMap := make(map[string]map[string]bool)
		for i, predicate := range lhsCrossTable {
			leftTableId := predicate.LeftColumn.TableId
			rightTableId := predicate.RightColumn.TableId
			if _, ok := joinedMap[leftTableId]; !ok {
				joinedMap[leftTableId] = make(map[string]bool)
			}
			if _, ok := joinedMap[rightTableId]; !ok {
				joinedMap[rightTableId] = make(map[string]bool)
			}
			joinedMap[leftTableId][rightTableId] = true
			joinedMap[rightTableId][leftTableId] = true
			if (rhsLeftTableId == leftTableId && rhsRightTableId == rightTableId) || (rhsLeftTableId == rightTableId && rhsRightTableId == leftTableId) {
				root.Lhs = append(root.Lhs, utils.GenerateConnectPredicateSingle(predicate, root.TableId2index))
				root.LhsCrossCandidate = append(lhsCrossTable[:i], lhsCrossTable[i+1:]...)
				break
			}
		}
		// 当没有找到主外键可以直接连接满足rhs的时候需要继续匹配
		// 论文版本最多只有三张表,先按照三张表去实现吧
		// todo 大于三张表的情况
		if len(root.Lhs) < 1 {
			midTableId := ""
			for midTableId = range joinedMap[rhsLeftTableId] {
				b := false
				for endTableId := range joinedMap[midTableId] {
					if endTableId == rhsRightTableId {
						b = true
						break
					}
				}
				if b {
					break
				}
			}
			for i, predicate := range lhsCrossTable {
				leftTableId := predicate.LeftColumn.TableId
				rightTableId := predicate.RightColumn.TableId
				if (rhsLeftTableId == leftTableId && midTableId == rightTableId) || (rhsLeftTableId == rightTableId && midTableId == leftTableId) {
					utils.CheckPredicateIndex(&predicate, root.TableId2index)
					root.Lhs = append(root.Lhs, utils.GenerateConnectPredicateSingle(predicate, root.TableId2index))
					root.LhsCrossCandidate = append(lhsCrossTable[:i], lhsCrossTable[i+1:]...)
					break
				}
			}
			lhsCrossTable = root.LhsCrossCandidate
			for i, predicate := range lhsCrossTable {
				leftTableId := predicate.LeftColumn.TableId
				rightTableId := predicate.RightColumn.TableId
				if (midTableId == leftTableId && rhsRightTableId == rightTableId) || (midTableId == rightTableId && rhsRightTableId == leftTableId) {
					root.Lhs = append(root.Lhs, utils.GenerateConnectPredicateSingle(predicate, root.TableId2index))
					root.LhsCrossCandidate = append(lhsCrossTable[:i], lhsCrossTable[i+1:]...)
					break
				}
			}
		}
	}

	logger.Infof("cal tableId:%v, predicate:%v, spent time:%vms", rhs.LeftColumn.TableId, rhs.PredicateStr, time.Now().UnixMilli()-startTime)
	return distributeTraversalGrpcOf(rhs, root, gv, coverRadius)
}

func distributeTraversalGrpcOf(rhs rds.Predicate, root *task_tree.TaskTree, gv *global_variables.GlobalV, coverRadius int) *inc_rule_dig.BatchMinerOutput {
	//inc_rule_dig.CreateIncRoot(gv.TaskId, root)
	minimalIncRules := make([]*inc_rule_dig.IncRule, 0)
	prunedIncRules := make([]*inc_rule_dig.IncRule, 0)
	sampleNodes := make([]*inc_rule_dig.SampleNode, 0)

	layer2SampleNodes := make(map[int][]*inc_rule_dig.SampleNode)

	rootIncRule := &inc_rule_dig.IncRule{
		Node: root,
	}
	layer := len(root.Lhs)
	var finish bool
	currLayer := []*inc_rule_dig.IncRule{rootIncRule}
	//var mu sync.Mutex
	var nextLayer []*inc_rule_dig.IncRule

	// 新流程，将一整层的节点汇总起来然后给查错那边处理计算出supp和confidence
	// 对于需要继续拓展的节点，先执行决策树和计算对应子节点的giniIndex
	// 选出giniIndex最高的作为下一层的top1
	// 下层的所有节点也都是需要计算的
	// 对下层仍需要继续拓展的节点，迭代和top1节点求overlap，选出最小的topK个节点，然后进行拓展
	for len(currLayer) > 0 {
		layer++
		children := make([]*inc_rule_dig.IncRule, 0)
		childrenPreLayerIndex := make([]int, 0)
		for index, incRule := range currLayer {
			utils.SortPredicates(incRule.Node.Lhs, false)
			utils.SortPredicates(incRule.Node.LhsCandidate, false)
			utils.SortPredicates(incRule.Node.LhsCrossCandidate, false)

			//检查，记录当前lhs有关的
			related, inRelated, crossRelated, crossInRelated := checkCandidatesRelatedNew(incRule.Node)
			// 可以在lhs中新添加的谓词集合
			// todo 这里跨表谓词的候选集需要加入一些限制，根据测试造的规则进行限制，主外键的t0和t1成对出现，t0跨表倒t2之后，不再出现t0相关的谓词
			tmp := make([]rds.Predicate, 0)
			tmp = append(tmp, related...)
			tmp = append(tmp, crossRelated...)

			hasCrossPredicate := len(incRule.Node.TableId2index) > 1

			for i, lhsP := range tmp {
				// 当规则已经是跨表的时候,表内谓词的support必须小于某个值
				if hasCrossPredicate && lhsP.Support > gv.CrossTablePredicateSupp {
					continue
				}
				// 需要保持谓词的编号是统一，当碰到跨表谓词的时候需要对谓词进行一定的变形
				// 比如当前谓词中有t0.a=t1.a，表示A表，新添加谓词t0.b=t2.a，t0.b来自B表，这时候需要将谓词转换成t0.a=t2.b，并且添加谓词t1.a=t3.b
				tableId2index := make(map[string]int, len(incRule.Node.TableId2index))
				for tableId, indexId := range incRule.Node.TableId2index {
					tableId2index[tableId] = indexId
				}
				childLhs := make([]rds.Predicate, len(incRule.Node.Lhs))
				copy(childLhs, incRule.Node.Lhs)
				if i >= len(related) { //表示该谓词是跨表谓词
					if len(tableId2index) > 2 { // 最多三张表相关联
						continue
					}
					childLhs = append(childLhs, utils.GenerateConnectPredicate(lhsP, tableId2index)...)
				} else {
					utils.CheckPredicateIndex(&lhsP, tableId2index)
					childLhs = append(childLhs, lhsP)
				}

				child := &task_tree.TaskTree{
					Rhs:           rhs,
					Lhs:           childLhs,
					TableId2index: tableId2index,
					//GiniIndex:                  tmpGini[i],
					LhsCandidate:               []rds.Predicate{},
					LhsCrossCandidate:          []rds.Predicate{},
					LhsCandidateGiniIndex:      []float64{},
					LhsCrossCandidateGiniIndex: []float64{},
				}
				if i < len(related) { //还没到联表的谓词
					child.LhsCandidate = make([]rds.Predicate, 0)
					child.LhsCandidate = append(child.LhsCandidate, related[i+1:]...)
					child.LhsCandidate = append(child.LhsCandidate, inRelated...)
					child.LhsCrossCandidate = make([]rds.Predicate, len(incRule.Node.LhsCrossCandidate))
					copy(child.LhsCrossCandidate, incRule.Node.LhsCrossCandidate)
				} else { //当前为联表谓词
					child.LhsCandidate = make([]rds.Predicate, 0)
					child.LhsCandidate = append(child.LhsCandidate, inRelated...)
					child.LhsCrossCandidate = make([]rds.Predicate, 0)
					child.LhsCrossCandidate = append(child.LhsCrossCandidate, tmp[i+1:]...)
					child.LhsCrossCandidate = append(child.LhsCrossCandidate, crossInRelated...)
				}
				if gv.MultiTable {
					childIncRule := &inc_rule_dig.IncRule{
						Node: child,
					}
					children = append(children, childIncRule)
					childrenPreLayerIndex = append(childrenPreLayerIndex, index)
					continue
				}
			}
		}

		if gv.MultiTable {
			if len(children) < 1 {
				logger.Infof("需要计算的节点数为0")
				for _, nodes := range layer2SampleNodes {
					sampleNodes = append(sampleNodes, nodes...)
				}
				return &inc_rule_dig.BatchMinerOutput{
					MinimalIncRules: minimalIncRules,
					PrunedIncRules:  prunedIncRules,
					SampleNodes:     sampleNodes,
				}
			}
			//printLayerChildren(rhs, layer, children)

			// 汇总一层需要计算的节点，计算出这一组节点各自的supp和confidence
			//hasRules, prunes, isDeletes := calIncRules(children, gv)
			hasRules, prunes, isDeletes := calIncRulesNew(children, gv)
			// 根据返回结果，处理那些节点可以生成规则，哪些节点需要走决策树和计算gini系数
			for i := range hasRules { //和上面循环的i顺序应该一致
				child := children[i]
				hasRule := hasRules[i]
				prune := prunes[i]
				if gv.UsePruneVersion {
					isDelete := isDeletes[i]
					if isDelete {
						for index, nextNode := range nextLayer {
							isDeleted := false
							nextNode.Node.LhsCandidate, isDeleted = deleteLhsCandidatesPredicate(nextNode, child)
							if isDeleted {
								addChildLhsCandidatesNew(child, nextNode, index, nextLayer)
								//logger.Infof("[调试日志] ****addChildAfter child:%v, candidate:%v", inc_rule_dig.GetIncRuleRee(child), getLhsStr(child.Node.LhsCandidate))
							}
						}
						if !hasRule {
							for index, nextNode := range prunedIncRules {
								isDeleted := false
								nextNode.Node.LhsCandidate, isDeleted = deleteLhsCandidatesPredicate(nextNode, child)
								if isDeleted {
									addChildLhsCandidatesNew(child, nextNode, index, prunedIncRules)
									//logger.Infof("[调试日志] ****addChildAfter child:%v, candidate:%v", inc_rule_dig.GetIncRuleRee(child), getLhsStr(child.Node.LhsCandidate))
								}
							}
						}
					}
				}

				if prune {
					if hasRule {
						if rhs.PredicateType == 0 {
							gv.SingleRuleSize++
						} else {
							gv.MultiRuleSize++
						}
						//printMinimalRule(rhs, layer, child)
						minimalIncRules = append(minimalIncRules, child)
					} else {
						// 被剪枝的规则
						//printPrunedRule(rhs, layer, child)
						prunedIncRules = append(prunedIncRules, child)
					}
					continue
				}

				layer2SampleNodes = updatedSampleNodesNew(layer, layer2SampleNodes, child, currLayer, coverRadius, childrenPreLayerIndex[i])

				nextLayer = append(nextLayer, child)

				if gv.StopTask {
					logger.Infof("taskId:%v, 收到停止信号，终止规则发现任务树:%v", gv.TaskId, rhs.PredicateStr)
					finish = true
				}
				if finish {
					break
				}
			}
		}

		// 执行决策树
		if gv.UseDecisionTree {
			distributeNotSatisfyNodeOfInc(nextLayer, false, gv)
		}
		currLayer = nextLayer
		nextLayer = make([]*inc_rule_dig.IncRule, 0)

		if finish || layer >= gv.TreeMaxLevel {
			break
		}
	}

	//printLayerSampleNodes(layer2SampleNodes)

	for _, nodes := range layer2SampleNodes {
		sampleNodes = append(sampleNodes, nodes...)
	}

	logger.Infof("predicate type[%v], predicate str[%v]", rhs.PredicateType, rhs.PredicateStr)
	//printRhsSampleNodes(rhs, sampleNodes)
	return &inc_rule_dig.BatchMinerOutput{
		MinimalIncRules: minimalIncRules,
		PrunedIncRules:  prunedIncRules,
		SampleNodes:     sampleNodes,
	}
}

func nextLayerGroupByParent(nextLayer []*inc_rule_dig.IncRule) map[string][]*inc_rule_dig.IncRule /* parentPredStr -> nodes */ {
	parentPredStr2Nodes := make(map[string][]*inc_rule_dig.IncRule)
	for _, next := range nextLayer {
		if len(next.Node.Lhs) < 2 {
			parentPredStr2Nodes[next.Node.Rhs.PredicateStr] = append(parentPredStr2Nodes[next.Node.Rhs.PredicateStr], next)
			continue
		}
		parentPredStr := next.Node.Lhs[len(next.Node.Lhs)-2].PredicateStr
		parentPredStr2Nodes[parentPredStr] = append(parentPredStr2Nodes[parentPredStr], next)
	}
	return parentPredStr2Nodes
}

func addChildLhsCandidates(child *inc_rule_dig.IncRule, currNext *inc_rule_dig.IncRule, currNextIndex int, nextLayer []*inc_rule_dig.IncRule) {
	if len(child.Node.Lhs) == 1 && len(currNext.Node.Lhs) == 1 {
		child.Node.LhsCandidate = append(child.Node.LhsCandidate, currNext.Node.Lhs[len(currNext.Node.Lhs)-1])
		logger.Infof("[调试日志] 第一层, currNext:%v, candidate:%v", inc_rule_dig.GetIncRuleRee(currNext), getLhsStr(currNext.Node.LhsCandidate))
		logger.Infof("[调试日志] 第一层, child:%v, candidate:%v", inc_rule_dig.GetIncRuleRee(child), getLhsStr(child.Node.LhsCandidate))
		return
	}

	childParentStr := child.Node.Lhs[len(child.Node.Lhs)-2].PredicateStr
	currNextParentStr := currNext.Node.Lhs[len(currNext.Node.Lhs)-2].PredicateStr
	if childParentStr == currNextParentStr {
		child.Node.LhsCandidate = append(child.Node.LhsCandidate, currNext.Node.Lhs[len(currNext.Node.Lhs)-1])
		logger.Infof("[调试日志] 同一个父节点, currNext:%v, candidate:%v", inc_rule_dig.GetIncRuleRee(currNext), getLhsStr(currNext.Node.LhsCandidate))
		logger.Infof("[调试日志] 同一个父节点, child:%v, candidate:%v", inc_rule_dig.GetIncRuleRee(child), getLhsStr(child.Node.LhsCandidate))
		return
	}
	isFlag := false
	childStr := child.Node.Lhs[len(child.Node.Lhs)-1].PredicateStr
	for i, next := range nextLayer {
		if currNextIndex == i {
			logger.Infof("[调试日志] *******currNext:%v", inc_rule_dig.GetIncRuleRee(currNext))
			logger.Infof("[调试日志] *******childStr:%v", childStr)
			continue
		}
		logger.Infof("[调试日志] next:%v", inc_rule_dig.GetIncRuleRee(next))
		nextParentStr := next.Node.Lhs[len(next.Node.Lhs)-2].PredicateStr
		if currNextParentStr == nextParentStr {
			// 同一个父节点
			logger.Infof("[调试日志] 与currNext父节点:%v相同的next:%v", currNextParentStr, inc_rule_dig.GetIncRuleRee(next))
			nextStr := next.Node.Lhs[len(next.Node.Lhs)-1].PredicateStr
			if nextStr == childStr {
				next.Node.LhsCandidate = append(next.Node.LhsCandidate, currNext.Node.Lhs[len(currNext.Node.Lhs)-1])
				logger.Infof("[调试日志] 与childStr:%v相同的next:%v, candidate:%v, 添加的谓词:%v", childStr, inc_rule_dig.GetIncRuleRee(next), getLhsStr(next.Node.LhsCandidate), currNext.Node.Lhs[len(currNext.Node.Lhs)-1].PredicateStr)
				isFlag = true
			}
		}
	}
	if !isFlag {
		logger.Warnf("[调试日志] 找不到与child相同的谓词")
	}
}

func addChildLhsCandidatesNew(child *inc_rule_dig.IncRule, currNext *inc_rule_dig.IncRule, currNextIndex int, nextLayer []*inc_rule_dig.IncRule) {
	if len(child.Node.Lhs) == 1 && len(currNext.Node.Lhs) == 1 {
		child.Node.LhsCandidate = append(child.Node.LhsCandidate, currNext.Node.Lhs[len(currNext.Node.Lhs)-1])
		//logger.Infof("[调试日志] 第一层, currNext:%v, candidate:%v", inc_rule_dig.GetIncRuleRee(currNext), getLhsStr(currNext.Node.LhsCandidate))
		//logger.Infof("[调试日志] 第一层, child:%v, candidate:%v", inc_rule_dig.GetIncRuleRee(child), getLhsStr(child.Node.LhsCandidate))
		return
	}

	childParentStr := child.Node.Lhs[len(child.Node.Lhs)-2].PredicateStr
	currNextParentStr := currNext.Node.Lhs[len(currNext.Node.Lhs)-2].PredicateStr
	if childParentStr == currNextParentStr {
		child.Node.LhsCandidate = append(child.Node.LhsCandidate, currNext.Node.Lhs[len(currNext.Node.Lhs)-1])
		//logger.Infof("[调试日志] 同一个父节点, currNext:%v, candidate:%v", inc_rule_dig.GetIncRuleRee(currNext), getLhsStr(currNext.Node.LhsCandidate))
		//logger.Infof("[调试日志] 同一个父节点, child:%v, candidate:%v", inc_rule_dig.GetIncRuleRee(child), getLhsStr(child.Node.LhsCandidate))
		return
	}

	predicates := make([]rds.Predicate, 0, len(currNext.Node.Lhs)+1)
	predicates = append(predicates, currNext.Node.Lhs...)
	predicates = append(predicates, child.Node.Lhs[len(child.Node.Lhs)-1])
	distinctP := inc_rule_dig.GetDistinctPredicate(predicates, child.Node.Lhs)
	child.Node.LhsCandidate = append(child.Node.LhsCandidate, distinctP)
	//isFlag := false
	//childStr := child.Node.Lhs[len(child.Node.Lhs)-1].PredicateStr
	//for i, next := range nextLayer {
	//	if currNextIndex == i {
	//		logger.Infof("[调试日志] *******currNext:%v", inc_rule_dig.GetIncRuleRee(currNext))
	//		logger.Infof("[调试日志] *******childStr:%v", childStr)
	//		continue
	//	}
	//	logger.Infof("[调试日志] next:%v", inc_rule_dig.GetIncRuleRee(next))
	//	nextParentStr := next.Node.Lhs[len(next.Node.Lhs)-2].PredicateStr
	//	if currNextParentStr == nextParentStr {
	//		// 同一个父节点
	//		logger.Infof("[调试日志] 与currNext父节点:%v相同的next:%v", currNextParentStr, inc_rule_dig.GetIncRuleRee(next))
	//		nextStr := next.Node.Lhs[len(next.Node.Lhs)-1].PredicateStr
	//		if nextStr == childStr {
	//			next.Node.LhsCandidate = append(next.Node.LhsCandidate, currNext.Node.Lhs[len(currNext.Node.Lhs)-1])
	//			logger.Infof("[调试日志] 与childStr:%v相同的next:%v, candidate:%v, 添加的谓词:%v", childStr, inc_rule_dig.GetIncRuleRee(next), getLhsStr(next.Node.LhsCandidate), currNext.Node.Lhs[len(currNext.Node.Lhs)-1].PredicateStr)
	//			isFlag = true
	//		}
	//	}
	//}
	//if !isFlag {
	//	logger.Warnf("[调试日志] 找不到与child相同的谓词")
	//}
}

func getLhsStr(lhs []rds.Predicate) []string {
	pStrs := make([]string, len(lhs))
	for i, p := range lhs {
		pStrs[i] = p.PredicateStr
	}
	return pStrs
}

func CalcIncRulesForSample(incRules []*inc_rule_dig.IncRule, gv *global_variables.GlobalV) {
	calIncRules(incRules, gv)
}

func calIncRulesNew(incRules []*inc_rule_dig.IncRule, gv *global_variables.GlobalV) (hasRules, prunes, isDeletes []bool) {
	startTime := time.Now().UnixMilli()
	hasRules, prunes, isDeletes = make([]bool, len(incRules)), make([]bool, len(incRules)), make([]bool, len(incRules))
	rowSizes, xSupps, xySupps := make([]int, len(incRules)), make([]int, len(incRules)), make([]int, len(incRules))
	client, _ := distribute.NewClient(nil)
	for i, incRule := range incRules {
		endpoint := <-gv.ChEndpoint
		i := i
		logger.Infof("cal rhs:%v, endpoint:%v, lhs:%v", incRule.Node.Rhs.PredicateStr, endpoint, utils.GetLhsStr(incRule.Node.Lhs))
		// 检查lhs和lhs+rhs是否都是连通图,两者有一个不满足则不计算该节点
		if !checkNode(incRule.Node) {
			hasRules[i] = false
			prunes[i] = false
			isDeletes[i] = false
			continue
		}

		// 跨表谓词中表内谓词的supp太大,过滤掉
		if !utils.CheckSatisfyPredicateSupp(incRule.Node.Lhs, incRule.Node.TableId2index, gv.CrossTablePredicateSupp) {
			hasRules[i] = false
			prunes[i] = false
			isDeletes[i] = false
			continue
		}

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

		incRule.Node.Lhs = utils.SortPredicatesRelated(incRule.Node.Lhs)

		res := request{
			Lhs:            incRule.Node.Lhs,
			Rhs:            incRule.Node.Rhs,
			TableId2index:  incRule.Node.TableId2index,
			AbandonedRules: gv.AbandonedRules,
			CR:             gv.CR,
			FTR:            gv.FTR,
			TaskId:         gv.TaskId,
			TableId:        gv.TableId,
			ChanSize:       gv.RdsChanSize,
		}
		resBytes, _ := json.Marshal(res)

		req := &pb.UniversalReq{
			Kind: 2,
			Data: resBytes,
		}
		resp := &pb.UniversalResp{}

		client.RoundRobin(pb.Rock_UniversalCall_FullMethodName, req, resp, func() {
			gv.ChEndpoint <- endpoint
			if resp.Err != "" {
				logger.Error(resp.Err)
			}
			var res response
			_ = json.Unmarshal(resp.Data, &res)
			logger.Infof("finish cal rhs:%v, lhs:%v, res:%v", incRules[i].Node.Rhs.PredicateStr, utils.GetLhsStr(incRules[i].Node.Lhs), res)

			hasRules[i] = res.HasRule
			prunes[i] = res.Prune
			isDeletes[i] = res.IsDelete
			rowSizes[i] = res.RowSize
			xSupps[i] = res.XSupp
			xySupps[i] = res.XySupp
		}, distribute.WithEndpoint(endpoint))
	}
	client.Close()
	for ruleId := range hasRules {
		//if hasRule {
		rowSize, xSupp, xySupp := rowSizes[ruleId], xSupps[ruleId], xySupps[ruleId]
		var support, confidence float64 = 0, 0
		if rowSize > 0 {
			support = float64(xySupp) / float64(rowSize)
		}
		if xSupp > 0 {
			confidence = float64(xySupp) / float64(xSupp)
		}
		incRules[ruleId].CR = support
		incRules[ruleId].FTR = confidence
		incRules[ruleId].RuleType = 1
		//}
	}
	logger.Infof("cal %v tree's level %v spent time:%vms", incRules[0].Node.Rhs.PredicateStr, len(incRules[0].Node.Lhs), time.Now().UnixMilli()-startTime)
	return hasRules, prunes, isDeletes
}

func CalIncRule(client *distribute.Client, incRule *inc_rule_dig.IncRule, gv *global_variables.GlobalV, chCalcRuleResult chan *inc_rule_dig.CalcRuleResult) {
	endpoint := <-gv.ChEndpoint
	logger.Infof("cal rhs:%v, endpoint:%v, lhs:%v", incRule.Node.Rhs.PredicateStr, endpoint, utils.GetLhsStr(incRule.Node.Lhs))
	// 检查lhs和lhs+rhs是否都是连通图,两者有一个不满足则不计算该节点
	if !checkNode(incRule.Node) {
		result := &inc_rule_dig.CalcRuleResult{
			Rule:      incRule,
			HasRule:   false,
			Prune:     false,
			IsDeleted: false,
		}
		chCalcRuleResult <- result
		return
	}

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

	incRule.Node.Lhs = utils.SortPredicatesRelated(incRule.Node.Lhs)

	res := request{
		Lhs:            incRule.Node.Lhs,
		Rhs:            incRule.Node.Rhs,
		TableId2index:  incRule.Node.TableId2index,
		AbandonedRules: gv.AbandonedRules,
		CR:             gv.CR,
		FTR:            gv.FTR,
		TaskId:         gv.TaskId,
		TableId:        gv.TableId,
		ChanSize:       gv.RdsChanSize,
	}
	resBytes, _ := json.Marshal(res)

	req := &pb.UniversalReq{
		Kind: 2,
		Data: resBytes,
	}
	resp := &pb.UniversalResp{}

	client.RoundRobin(pb.Rock_UniversalCall_FullMethodName, req, resp, func() {
		gv.ChEndpoint <- endpoint
		if resp.Err != "" {
			logger.Error(resp.Err)
		}
		var res response
		_ = json.Unmarshal(resp.Data, &res)
		logger.Infof("finish cal rhs:%v, lhs:%v, res:%v", incRule.Node.Rhs.PredicateStr, utils.GetLhsStr(incRule.Node.Lhs), res)

		var support, confidence float64 = 0, 0
		if res.RowSize > 0 {
			support = float64(res.XySupp) / float64(res.RowSize)
		}
		if res.XSupp > 0 {
			confidence = float64(res.XySupp) / float64(res.XSupp)
		}
		incRule.CR = support
		incRule.FTR = confidence
		incRule.RuleType = 1

		result := &inc_rule_dig.CalcRuleResult{
			Rule:      incRule,
			HasRule:   res.HasRule,
			Prune:     res.Prune,
			IsDeleted: res.IsDelete,
		}
		chCalcRuleResult <- result
	}, distribute.WithEndpoint(endpoint))
}

// GetChildren 获取前驱节点下的children节点
func GetChildren(predecessorNode *inc_rule_dig.IncRule, rhs rds.Predicate) []*inc_rule_dig.IncRule {
	children := make([]*inc_rule_dig.IncRule, 0)

	utils.SortPredicates(predecessorNode.Node.Lhs, false)
	utils.SortPredicates(predecessorNode.Node.LhsCandidate, false)
	utils.SortPredicates(predecessorNode.Node.LhsCrossCandidate, false)

	//检查，记录当前lhs有关的
	related, inRelated, crossRelated, crossInRelated := checkCandidatesRelatedNew(predecessorNode.Node)
	// 可以在lhs中新添加的谓词集合
	// todo 这里跨表谓词的候选集需要加入一些限制，根据测试造的规则进行限制，主外键的t0和t1成对出现，t0跨表倒t2之后，不再出现t0相关的谓词
	tmp := make([]rds.Predicate, 0)
	tmp = append(tmp, related...)
	tmp = append(tmp, crossRelated...)

	for i, lhsP := range tmp {
		// 需要保持谓词的编号是统一，当碰到跨表谓词的时候需要对谓词进行一定的变形
		// 比如当前谓词中有t0.a=t1.a，表示A表，新添加谓词t0.b=t2.a，t0.b来自B表，这时候需要将谓词转换成t0.a=t2.b，并且添加谓词t1.a=t3.b
		tableId2index := make(map[string]int, len(predecessorNode.Node.TableId2index))
		for tableId, indexId := range predecessorNode.Node.TableId2index {
			tableId2index[tableId] = indexId
		}
		childLhs := make([]rds.Predicate, len(predecessorNode.Node.Lhs))
		copy(childLhs, predecessorNode.Node.Lhs)
		if i >= len(related) { //表示该谓词是跨表谓词
			if len(tableId2index) > 2 { // 最多三张表相关联
				continue
			}
			childLhs = append(childLhs, utils.GenerateConnectPredicate(lhsP, tableId2index)...)
		} else {
			utils.CheckPredicateIndex(&lhsP, tableId2index)
			childLhs = append(childLhs, lhsP)
		}

		child := &task_tree.TaskTree{
			Rhs:           rhs,
			Lhs:           childLhs,
			TableId2index: tableId2index,
			//GiniIndex:                  tmpGini[i],
			LhsCandidate:               []rds.Predicate{},
			LhsCrossCandidate:          []rds.Predicate{},
			LhsCandidateGiniIndex:      []float64{},
			LhsCrossCandidateGiniIndex: []float64{},
		}
		if i < len(related) { //还没到联表的谓词
			child.LhsCandidate = make([]rds.Predicate, 0)
			child.LhsCandidate = append(child.LhsCandidate, related[i+1:]...)
			child.LhsCandidate = append(child.LhsCandidate, inRelated...)
			child.LhsCrossCandidate = make([]rds.Predicate, len(predecessorNode.Node.LhsCrossCandidate))
			copy(child.LhsCrossCandidate, predecessorNode.Node.LhsCrossCandidate)
		} else { //当前为联表谓词
			child.LhsCandidate = make([]rds.Predicate, 0)
			child.LhsCandidate = append(child.LhsCandidate, inRelated...)
			child.LhsCrossCandidate = make([]rds.Predicate, 0)
			child.LhsCrossCandidate = append(child.LhsCrossCandidate, tmp[i+1:]...)
			child.LhsCrossCandidate = append(child.LhsCrossCandidate, crossInRelated...)
		}

		childIncRule := &inc_rule_dig.IncRule{
			Node: child,
		}
		children = append(children, childIncRule)
	}
	return children
}

func checkCandidatesRelatedNew(node *task_tree.TaskTree) (related []rds.Predicate, inRelated []rds.Predicate,
	crossRelated []rds.Predicate, crossInRelated []rds.Predicate) {
	currentTables := getTreeTables(node)
	related = make([]rds.Predicate, 0)
	inRelated = make([]rds.Predicate, 0)
	crossRelated = make([]rds.Predicate, 0)
	crossInRelated = make([]rds.Predicate, 0)

	for i := range node.LhsCandidate {
		candidate := node.LhsCandidate[i]
		if _, exist := currentTables[candidate.LeftColumn.TableId]; exist {
			related = append(related, candidate)
			continue
		} else if _, exist := currentTables[candidate.RightColumn.TableId]; exist {
			related = append(related, candidate)
		} else {
			inRelated = append(inRelated, candidate)
		}
	}
	for i := range node.LhsCrossCandidate {
		candidate := node.LhsCrossCandidate[i]
		if _, exist := currentTables[candidate.LeftColumn.TableId]; exist {
			crossRelated = append(crossRelated, candidate)
			continue
		} else if _, exist := currentTables[candidate.RightColumn.TableId]; exist {
			crossRelated = append(crossRelated, candidate)
		} else {
			crossInRelated = append(crossInRelated, candidate)
		}
	}
	return related, inRelated, crossRelated, crossInRelated
}

func getTreeTables(node *task_tree.TaskTree) map[string]struct{} {
	currentTables := make(map[string]struct{})
	currentTables[node.Rhs.LeftColumn.TableId] = struct{}{}
	if node.Rhs.RightColumn.TableId != "" {
		currentTables[node.Rhs.RightColumn.TableId] = struct{}{}
	}
	for _, lhs := range node.Lhs {
		currentTables[lhs.LeftColumn.TableId] = struct{}{}
		if lhs.RightColumn.TableId != "" {
			currentTables[lhs.RightColumn.TableId] = struct{}{}
		}
	}
	return currentTables
}

func deletePredicateAndGini(predicates []rds.Predicate, predicate rds.Predicate) ([]rds.Predicate, bool) {
	var result []rds.Predicate
	isDeleted := false
	for _, p := range predicates {
		if p.PredicateStr != predicate.PredicateStr {
			result = append(result, p)
		}
	}
	if len(predicates) != len(result) {
		isDeleted = true
	}
	return result, isDeleted
}

func deleteLhsCandidatesPredicate(next *inc_rule_dig.IncRule, child *inc_rule_dig.IncRule) ([]rds.Predicate, bool) {
	deleteP := child.Node.Lhs[len(child.Node.Lhs)-1]

	var result []rds.Predicate
	isDeleted := false
	for _, candidateP := range next.Node.LhsCandidate {
		if candidateP.PredicateStr != deleteP.PredicateStr {
			result = append(result, candidateP)
		} else {
			predicates := make([]rds.Predicate, len(next.Node.Lhs))
			//predicates = append(predicates, next.Node.Lhs...)
			copy(predicates, next.Node.Lhs)
			predicates = append(predicates, candidateP)
			if inc_rule_dig.ContainAllPredicates(predicates, child.Node.Lhs) {
				isDeleted = true
				//logger.Infof("[调试日志-deleteLhsCandidatesPredicate] child:%v, 删除 next:%v, candidate:%v中的谓词:%v", inc_rule_dig.GetIncRuleRee(child), inc_rule_dig.GetIncRuleRee(next), getLhsStr(next.Node.LhsCandidate), candidateP.PredicateStr)
			} else {
				result = append(result, candidateP)
				//logger.Infof("[调试日志-deleteLhsCandidatesPredicate] child:%v, ***不删除 next:%v, candidate:%v中的谓词:%v", inc_rule_dig.GetIncRuleRee(child), inc_rule_dig.GetIncRuleRee(next), getLhsStr(next.Node.LhsCandidate), candidateP.PredicateStr)
			}
		}
	}
	return result, isDeleted
}

func sampleNodesGroupByLayer(sampleNodes []*inc_rule_dig.SampleNode) map[int][]*inc_rule_dig.SampleNode {
	layer2SampleNodes := make(map[int][]*inc_rule_dig.SampleNode)
	for _, sampleNode := range sampleNodes {
		layer := len(sampleNode.CurrentNode.Node.Lhs)
		layer2SampleNodes[layer] = append(layer2SampleNodes[layer], sampleNode)
	}
	return layer2SampleNodes
}

func mergeSampleNodes(layer2SampleNodes map[int][]*inc_rule_dig.SampleNode, toBeMergedSample *inc_rule_dig.SampleNode) {
	layer := len(toBeMergedSample.CurrentNode.Node.Lhs)
	if len(layer2SampleNodes) < 1 {
		layer2SampleNodes[layer] = append(layer2SampleNodes[layer], toBeMergedSample)
	}
	isContainAll := false
	//for i, sampleNode := range layer2SampleNodes[layer] {
	//	maxSample, minSample, status := getContainInfo(sampleNode, toBeMergedSample)
	//	switch status {
	//	case inc_rule_dig.ContainAll:
	//		layer2SampleNodes[layer][i] = maxSample
	//		isContainAll = true
	//		break
	//	case inc_rule_dig.ContainNot:
	//		continue
	//	case inc_rule_dig.ContainPart:
	//		layer2SampleNodes[layer][i] = maxSample
	//		toBeMergedSample = minSample
	//	}
	//}
	for i, sampleNode := range layer2SampleNodes[layer] {
		status := getContainStatus(sampleNode, toBeMergedSample)
		switch status {
		case inc_rule_dig.ContainAllLeft:
			//logger.Infof("ContainAllLeft, sample:%v, sampleParent:%v, toBeSample:%v, toBeSampleParent:%v", inc_rule_dig.GetIncRuleRee(sampleNode.CurrentNode), getSampleParent(sampleNode), inc_rule_dig.GetIncRuleRee(toBeMergedSample.CurrentNode), getSampleParent(toBeMergedSample))
			isContainAll = true
			break
		case inc_rule_dig.ContainAllRight:
			//logger.Infof("ContainAllRight, sample:%v, sampleParent:%v, toBeSample:%v, toBeSampleParent:%v", inc_rule_dig.GetIncRuleRee(sampleNode.CurrentNode), getSampleParent(sampleNode), inc_rule_dig.GetIncRuleRee(toBeMergedSample.CurrentNode), getSampleParent(toBeMergedSample))
			layer2SampleNodes[layer][i] = toBeMergedSample
			isContainAll = true
			break
		case inc_rule_dig.ContainNot:
			continue
		case inc_rule_dig.ContainPart:
			layer2SampleNodes[layer][i] = sampleNode
		}
	}
	if !isContainAll {
		layer2SampleNodes[layer] = append(layer2SampleNodes[layer], toBeMergedSample)
	}
	//else {
	//logger.Infof("执行了merge, current:%v, FTR:%v, merge后的maxConf:%v", inc_rule_dig.GetIncRuleRee(toBeMergedSample.CurrentNode), toBeMergedSample.CurrentNode.FTR, toBeMergedSample.MaxConf)
	//}
}

func getContainStatus(sample *inc_rule_dig.SampleNode, toBeMergedSample *inc_rule_dig.SampleNode) int {
	var maxSample, minSample *inc_rule_dig.SampleNode
	var maxRees, minRees []string

	sampleRees := inc_rule_dig.GetIncRulesRee(sample.PredecessorNodes)
	toBeMergedSampleRees := inc_rule_dig.GetIncRulesRee(toBeMergedSample.PredecessorNodes)

	containAllLeft := false
	if len(sampleRees) >= len(toBeMergedSampleRees) {
		maxSample = sample
		minSample = toBeMergedSample
		maxRees = sampleRees
		minRees = toBeMergedSampleRees
		containAllLeft = true
	} else {
		maxSample = toBeMergedSample
		minSample = sample
		maxRees = toBeMergedSampleRees
		minRees = sampleRees
		containAllLeft = false
	}
	set := make(map[string]struct{})
	for _, v := range maxRees {
		set[v] = struct{}{}
	}

	notContainIndex := make([]int, 0)

	for i, v := range minRees {
		if _, ok := set[v]; !ok {
			notContainIndex = append(notContainIndex, i)
		}
	}

	containStatus := 0
	if len(notContainIndex) == 0 {
		// 全包含
		if containAllLeft {
			containStatus = inc_rule_dig.ContainAllLeft
		} else {
			containStatus = inc_rule_dig.ContainAllRight
		}
		// 更新maxConfidence
		maxConf := math.Max(maxSample.MaxConf, minSample.MaxConf)
		maxSample.MaxConf = maxConf

		//maxSample.NeighborConfs = append(maxSample.NeighborConfs, minSample.NeighborConfs...)
		maxSample.NeighborCDF = inc_rule_dig.MergeCDF(maxSample.NeighborCDF, minSample.NeighborCDF, true)

	} else if len(notContainIndex) == len(minRees) {
		// 全不包含
		containStatus = inc_rule_dig.ContainNot
	} else {
		// 部分包含,保留minSample中没包含的前驱节点
		containStatus = inc_rule_dig.ContainPart
		notContainPredecessors := make([]*inc_rule_dig.IncRule, len(notContainIndex))
		for i, index := range notContainIndex {
			notContainPredecessors[i] = minSample.PredecessorNodes[index]
		}
		minSample.PredecessorNodes = notContainPredecessors

		maxConf := math.Max(maxSample.MaxConf, minSample.MaxConf)
		maxSample.MaxConf = maxConf
		minSample.MaxConf = maxConf

		//maxSample.NeighborConfs = append(maxSample.NeighborConfs, minSample.NeighborConfs...)
		//minSample.NeighborConfs = append(minSample.NeighborConfs, maxSample.NeighborConfs...)
		maxSample.NeighborCDF = inc_rule_dig.MergeCDF(maxSample.NeighborCDF, minSample.NeighborCDF, true)

	}

	return containStatus
}

func distributeNotSatisfyNodeOfInc(incRules []*inc_rule_dig.IncRule, calGiniFlag bool, gv *global_variables.GlobalV) {
	client, err := distribute.NewClient(nil)
	if err != nil {
		logger.Error("new client err=", err)
		return
	}
	for i, incRule := range incRules {
		endpoint := <-gv.ChEndpoint
		startTime := time.Now().UnixMilli()
		incRule := incRule
		//if i > gv.TopKSize {
		//	node.LhsCandidateGiniIndex = make([]float64, len(node.LhsCandidate))
		//	node.LhsCrossCandidateGiniIndex = make([]float64, len(node.LhsCrossCandidate))
		//	continue
		//}

		if !checkNode(incRule.Node) {
			//node.LhsCandidateGiniIndex = make([]float64, len(node.LhsCandidate))
			//node.LhsCrossCandidateGiniIndex = make([]float64, len(node.LhsCrossCandidate))
			continue
		}
		//// 跨表谓词中表内谓词的supp太大,过滤掉
		//if !utils.CheckSatisfyPredicateSupp(node.Lhs, node.TableId2index, gv.CrossTablePredicateSupp) {
		//	node.LhsCandidateGiniIndex = make([]float64, len(node.LhsCandidate))
		//	node.LhsCrossCandidateGiniIndex = make([]float64, len(node.LhsCrossCandidate))
		//	continue
		//}
		//// 如果node中包含ml谓词跳过
		//if utils.CheckHasML(node.Lhs) {
		//	node.LhsCandidateGiniIndex = make([]float64, len(node.LhsCandidate))
		//	node.LhsCrossCandidateGiniIndex = make([]float64, len(node.LhsCrossCandidate))
		//	continue
		//}

		reqBytes, err := json.Marshal(&request.CalNotSatisfyNodeReq{
			NodeId:                 i,
			TaskId:                 gv.TaskId,
			CR:                     gv.CR,
			FTR:                    gv.FTR,
			Node:                   incRule.Node,
			CalGiniFlag:            calGiniFlag,
			TableColumnType:        gv.Table2ColumnType,
			FilterArr:              gv.FilterArr,
			DecisionTreeMaxDepth:   gv.DecisionTreeMaxDepth,
			CubeSize:               gv.CubeSize,
			ChanSize:               gv.RdsChanSize,
			DecisionTreeMaxRowSize: gv.DecisionTreeMaxRowSize,
		})
		if err != nil {
			logger.Error("json Marshal err=", err)
			logger.Errorf("node:%v,", incRule.Node.Tostring())
			continue
		}
		req := &pb.CalNotSatisfyNodeReq{
			Req: reqBytes,
		}
		resp := &pb.CalNotSatisfyNodeResp{}
		client.RoundRobin(pb.Rock_CalNotSatisfyNode_FullMethodName, req, resp, func() {
			gv.ChEndpoint <- endpoint
			gv.MultiRuleSize += resp.RuleSize
			logger.Infof("finish cal not satisfy node,lhs:%v, rhs:%v, rule size:%v, spent time:%vms",
				utils.GetLhsStr(incRule.Node.Lhs), incRule.Node.Rhs.PredicateStr, resp.RuleSize, time.Now().UnixMilli()-startTime)
			if calGiniFlag {
				lhsCandidateGiniIndex := resp.LhsCandidateGiniIndex
				lhsCrossCandidateGiniIndex := resp.LhsCrossCandidateGiniIndex
				incRule.Node.LhsCandidateGiniIndex = utils.ChangeFloatType(lhsCandidateGiniIndex)
				incRule.Node.LhsCrossCandidateGiniIndex = utils.ChangeFloatType(lhsCrossCandidateGiniIndex)
			}
		}, distribute.WithEndpoint(endpoint))
	}
	client.Close()
}

func CalcNotSatisfyNode(client *distribute.Client, incRule *inc_rule_dig.IncRule, calGiniFlag bool, gv *global_variables.GlobalV, nodeId int) {
	endpoint := <-gv.ChEndpoint
	startTime := time.Now().UnixMilli()

	if !checkNode(incRule.Node) {
		return
	}

	reqBytes, err := json.Marshal(&request.CalNotSatisfyNodeReq{
		NodeId:                 nodeId,
		TaskId:                 gv.TaskId,
		CR:                     gv.CR,
		FTR:                    gv.FTR,
		Node:                   incRule.Node,
		CalGiniFlag:            calGiniFlag,
		TableColumnType:        gv.Table2ColumnType,
		FilterArr:              gv.FilterArr,
		DecisionTreeMaxDepth:   gv.DecisionTreeMaxDepth,
		CubeSize:               gv.CubeSize,
		ChanSize:               gv.RdsChanSize,
		DecisionTreeMaxRowSize: gv.DecisionTreeMaxRowSize,
	})
	if err != nil {
		logger.Error("json Marshal err=", err)
		logger.Errorf("node:%v,", incRule.Node.Tostring())
		return
	}
	req := &pb.CalNotSatisfyNodeReq{
		Req: reqBytes,
	}
	resp := &pb.CalNotSatisfyNodeResp{}
	client.RoundRobin(pb.Rock_CalNotSatisfyNode_FullMethodName, req, resp, func() {
		gv.ChEndpoint <- endpoint
		gv.MultiRuleSize += resp.RuleSize
		logger.Infof("finish cal not satisfy node,lhs:%v, rhs:%v, rule size:%v, spent time:%vms",
			utils.GetLhsStr(incRule.Node.Lhs), incRule.Node.Rhs.PredicateStr, resp.RuleSize, time.Now().UnixMilli()-startTime)
		if calGiniFlag {
			lhsCandidateGiniIndex := resp.LhsCandidateGiniIndex
			lhsCrossCandidateGiniIndex := resp.LhsCrossCandidateGiniIndex
			incRule.Node.LhsCandidateGiniIndex = utils.ChangeFloatType(lhsCandidateGiniIndex)
			incRule.Node.LhsCrossCandidateGiniIndex = utils.ChangeFloatType(lhsCrossCandidateGiniIndex)
		}
	}, distribute.WithEndpoint(endpoint))
}

func CalNoeSatisfyNode(req *request.CalNotSatisfyNodeReq) *request.CalNotSatisfyNodeResp {
	startTime := time.Now().UnixMilli()
	taskId := req.TaskId
	cr := req.CR
	ftr := req.FTR
	node := req.Node
	lhs := node.Lhs
	rhs := node.Rhs
	tableColumnType := removeUdfColumn(req.TableColumnType)
	decisionTreeMaxDepth := req.DecisionTreeMaxDepth
	cubeSize := req.CubeSize
	logger.Infof("cal not satisfy node lhs:%v, rhs:%v", utils.GetLhsStr(lhs), rhs.PredicateStr)
	index2table, tableArr := generateIndex2Table(append(lhs, rhs))
	filterRatio := make(map[string]float64)
	tableId2index := generateTableId2tableIndex(lhs)
	isSingleTable := len(tableId2index) == 1

	logger.Infof("get %v sample data ", utils.GetLhsStr(lhs))
	var cube [][][]int32
	var tableIds []string
	if req.DecisionTreeMaxRowSize == -1 {
		cube, tableIds = intersection.SampleIntersection(lhs, taskId, cubeSize, req.ChanSize, rhs)
		// 不抽样
		_, length, _, _ := storage_utils.GetSchemaInfo(rhs.LeftColumn.TableId) //当前节点该表数据行数, 目前是全表
		req.DecisionTreeMaxRowSize = length
	} else {
		if cubeSize == -1 {
			// 抽源数据
			cube, tableIds = intersection.SampleIntersectionNew(lhs, taskId, cubeSize, req.ChanSize, rhs)
		} else {
			// 抽桶
			cube, tableIds = intersection.SampleIntersection(lhs, taskId, cubeSize, req.ChanSize, rhs)
		}
	}

	t1 := time.Now().UnixMilli() - startTime
	logger.Infof("spent time:%vms, finish get %v sample data, cube size:%v", t1, utils.GetLhsStr(lhs), len(cube))
	// 这里返回来的cube可能长度为0
	if len(cube) < 1 {
		return &request.CalNotSatisfyNodeResp{
			RuleSize:                   0,
			LhsCandidateGiniIndex:      make([]float32, len(node.LhsCandidate)),
			LhsCrossCandidateGiniIndex: make([]float32, len(node.LhsCrossCandidate)),
		}
	}

	t := time.Now().UnixMilli()

	if cubeSize > 0 {
		// 抽桶
		if len(cube) > cubeSize {
			cube = cube[:cubeSize]
		}

		cubeDataSize := req.DecisionTreeMaxRowSize / len(cube)
		for j, rowSets := range cube {
			s := 1
			for _, rowSet := range rowSets {
				s *= len(rowSet)
			}
			if s > cubeDataSize {
				ratio := float64(cubeDataSize) / float64(s)
				if len(rowSets) > 1 {
					for i := 0; i < len(rowSets)/2; i++ {
						ratio = math.Sqrt(ratio)
					}
				}
				for i := range rowSets {
					i2 := int(float64(len(rowSets[i])) * ratio)
					if i2 < 1 {
						i2 = 1
					}
					cube[j][i] = rowSets[i][:i2]
				}
			}
		}
	}

	satisfyData := intersection.BuildIdTableFromIntersection(cube, tableIds, taskId, isSingleTable)

	if len(satisfyData) < 1 {
		logger.Infof("satisfyData size:%v", len(satisfyData))
		return &request.CalNotSatisfyNodeResp{
			RuleSize:                   0,
			LhsCandidateGiniIndex:      make([]float32, len(node.LhsCandidate)),
			LhsCrossCandidateGiniIndex: make([]float32, len(node.LhsCrossCandidate)),
		}
	}

	for index := range index2table {
		filterRatio[index] = 1
	}

	t2 := time.Now().UnixMilli() - t
	logger.Infof("spent time:%vms, filter data", t2)
	t = time.Now().UnixMilli()
	logger.Infof("generate train data lhs:%v, rhs:%v", utils.GetLhsStr(lhs), rhs.PredicateStr)
	columns, trainData, rhsValue2index, columnType := train_data_util.GenerateTrainDataParallel(satisfyData, tableArr, index2table, tableColumnType, true, false, rhs, filterRatio, req.ChanSize, req.DecisionTreeMaxRowSize)
	t3 := time.Now().UnixMilli() - t
	logger.Infof("spent time:%vms, finish generate train data lhs:%v, rhs:%v", t3, utils.GetLhsStr(lhs), rhs.PredicateStr)

	// 生成的训练数据有时候长度为0了,可能是因为抽样的原因?
	if len(trainData) < 1 {
		return &request.CalNotSatisfyNodeResp{
			RuleSize:                   0,
			LhsCandidateGiniIndex:      make([]float32, len(node.LhsCandidate)),
			LhsCrossCandidateGiniIndex: make([]float32, len(node.LhsCrossCandidate)),
		}
	}

	// 调用决策树
	t = time.Now().UnixMilli()
	logger.Infof("execute decision tree lhs:%v, rhs:%v", utils.GetLhsStr(lhs), rhs.PredicateStr)
	rules, _, _, err := decision_tree.DecisionTreeWithDataInput(lhs, rhs, columns, trainData, columnType, index2table, rhsValue2index, taskId, cr, ftr, decisionTreeMaxDepth, false)
	if err != nil {
		logger.Errorf("execute decision tree err:%v", err)
		return nil
	}
	t4 := time.Now().UnixMilli() - t
	logger.Infof("spent time:%vms, finish execute decision tree lhs:%v, rhs:%v", t4, utils.GetLhsStr(lhs), rhs.PredicateStr)

	t = time.Now().UnixMilli()
	for _, rule := range rules {
		// 因为现在规则发现涉及到多表，所以tableId字段先随便填一个吧
		db_util.WriteRule2DB(rule, taskId, 0, 0)
	}
	t5 := time.Now().UnixMilli() - t
	logger.Infof("spent time:%vms, write rules", t5)

	// 计算下层节点的giniIndex
	t = time.Now().UnixMilli()
	var candidateGiniIndexInfos, crossCandidateGiniIndexInfos = make([]float32, len(node.LhsCandidate)), make([]float32, len(node.LhsCrossCandidate))
	if req.CalGiniFlag {
		utils.CheckPredicatesIndex(node.LhsCrossCandidate, tableId2index)
		utils.CheckPredicatesIndex(node.LhsCandidate, tableId2index)
		candidateGiniIndexInfos = topk.CalGiniIndexes(node.LhsCandidate, rhs, trainData, columns, node.TableId2index)
		crossCandidateGiniIndexInfos = topk.CalGiniIndexes(node.LhsCrossCandidate, rhs, trainData, columns, node.TableId2index)
	}
	t6 := time.Now().UnixMilli() - t
	logger.Infof("spent time:%vms, cal gini", t6)

	logger.Infof("total spent time:%vms, get sample sate time:%vms, filter data time:%vms, generate train data time:%vms, "+
		"decision tree time:%vms, write rules time:%vms, cal gini time:%vms, "+
		"finish calculate not satisfy node lhs:%v, rhs:%v",
		time.Now().UnixMilli()-startTime, t1, t2, t3, t4, t5, t6, utils.GetLhsStr(lhs), rhs.PredicateStr)
	return &request.CalNotSatisfyNodeResp{
		RuleSize:                   len(rules),
		LhsCandidateGiniIndex:      candidateGiniIndexInfos,
		LhsCrossCandidateGiniIndex: crossCandidateGiniIndexInfos,
	}
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

func generateTableId2tableIndex(predicates []rds.Predicate) map[string]int {
	result := make(map[string]int)
	for _, predicate := range predicates {
		leftIndex, rightIndex := utils.GetPredicateColumnIndexNew(predicate)
		result[predicate.LeftColumn.TableId] = leftIndex / 2
		result[predicate.RightColumn.TableId] = rightIndex / 2
	}
	return result
}

func checkNode(node *task_tree.TaskTree) bool {
	// 检查lhs和lhs+rhs是否都是连通图,两者有一个不满足则不计算该节点
	lhs := node.Lhs
	tmpP := make([]rds.Predicate, len(lhs)+1)
	copy(tmpP, lhs)
	tmpP[len(lhs)] = node.Rhs
	//if utils.CheckPredicatesIsConnectedGraph(lhs) && utils.CheckPredicatesIsConnectedGraph(tmpP) {
	//	return true
	//}
	//logger.Infof("lhs:%v, rhs:%v, 不满足计算要求", utils.GetLhsStr(lhs), node.Rhs.PredicateStr)
	m := make(map[int]bool)
	for _, pre := range node.Lhs {
		m[pre.LeftColumn.ColumnIndex] = true
		m[pre.RightColumn.ColumnIndex] = true
	}
	if utils.CheckPredicatesIsConnectedGraph(lhs) && utils.CheckPredicatesIsConnectedGraph(tmpP) && m[node.Rhs.LeftColumn.ColumnIndex] && m[node.Rhs.RightColumn.ColumnIndex] {
		return true
	}
	logger.Infof("lhs:%v, rhs:%v, 不满足计算要求", utils.GetLhsStr(lhs), node.Rhs.PredicateStr)
	return false
}
