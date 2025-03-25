package inc_rule_dig

import (
	"encoding/json"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables/task_tree"
	"gitlab.grandhoo.com/rock/rock_v3/inc_rule_dig/dao"
	"gitlab.grandhoo.com/rock/rock_v3/utils"
	"sort"
	"strings"
)

func GetRuleRee(rule rds.Rule) string {
	px := utils.GetLhsStr(rule.LhsPredicates)
	ree := px + " -> " + rule.Rhs.PredicateStr
	return ree
}

func GetIncRuleRee(incRule *IncRule) string {
	node := incRule.Node
	px := utils.GetLhsStr(node.Lhs)
	ree := px + " -> " + node.Rhs.PredicateStr
	//ree = utils.GenerateMultiTableIdStrNew2(node.TableId2index, node.Lhs) + " ^ " + ree
	return ree
}

func GetIncRulesRee(incRules []*IncRule) []string {
	rees := make([]string, len(incRules))
	for i, incRule := range incRules {
		rees[i] = GetIncRuleRee(incRule)
	}
	return rees
}

// ContainAllPredicates slice1 contain all slice2 return true else false
func ContainAllPredicates(predicates []rds.Predicate, subP []rds.Predicate) bool {
	//pStr := GetSortedLhsStr(p)
	//subPStr := GetSortedLhsStr(subP)
	//logger.Infof("[调试日志] pStr:%v, subPStr:%v", pStr, subPStr)
	//if strings.Contains(pStr, subPStr) {
	//	return true
	//}
	//return false

	set := make(map[string]struct{})
	for _, p := range predicates {
		set[p.PredicateStr] = struct{}{}
	}

	for _, p := range subP {
		if _, ok := set[p.PredicateStr]; !ok {
			return false
		}
	}
	return true
}

func GetDistinctPredicate(predicates []rds.Predicate, subP []rds.Predicate) rds.Predicate {
	set := make(map[string]struct{})
	for _, p := range subP {
		set[p.PredicateStr] = struct{}{}
	}

	for _, p := range predicates {
		if _, ok := set[p.PredicateStr]; !ok {
			return p
		}
	}
	logger.Warnf("[GetDistinctPredicate] 没有不同的谓词")
	return predicates[0]
}

func GetSortedLhsStr(lhs []rds.Predicate) string {
	pStrs := make([]string, len(lhs))
	for i, p := range lhs {
		pStrs[i] = p.PredicateStr
	}

	sort.Slice(pStrs, func(i, j int) bool {
		return pStrs[i] < pStrs[j]
	})

	return strings.Join(pStrs, " ^ ")
}

func ContainsAll(slice1, slice2 []string) bool {
	set := make(map[string]struct{})
	var minSlice []string
	if len(slice1) > len(slice2) {
		for _, v := range slice1 {
			set[v] = struct{}{}
		}
		minSlice = slice2
	} else {
		for _, v := range slice2 {
			set[v] = struct{}{}
		}
		minSlice = slice1
	}

	for _, v := range minSlice {
		if _, ok := set[v]; !ok {
			return false
		}
	}
	return true
}

func Contains(strs, subStrs []string) bool {
	set := make(map[string]struct{})
	for _, v := range strs {
		set[v] = struct{}{}
	}

	for _, v := range subStrs {
		if _, ok := set[v]; !ok {
			return false
		}
	}
	return true
}

func GetLessConfidenceIncRules(incRules []*IncRule, confidence float64) []*IncRule {
	satisfyIncRules := make([]*IncRule, 0, len(incRules))
	for _, incRule := range incRules {
		if incRule.FTR < confidence {
			satisfyIncRules = append(satisfyIncRules, incRule)
		}
	}
	return satisfyIncRules
}

func ConvertIncRule2Rule(incRule IncRule) rds.Rule {
	node := incRule.Node
	px := utils.GetLhsStr(node.Lhs)
	ree := px + " -> " + node.Rhs.PredicateStr
	ree = utils.GenerateMultiTableIdStrNew2(node.TableId2index, node.Lhs) + " ^ " + ree
	rule := rds.Rule{
		TableId:       "",
		Ree:           ree,
		LhsPredicates: node.Lhs,
		LhsColumns:    utils.GetPredicatesColumn(node.Lhs),
		Rhs:           node.Rhs,
		RhsColumn:     node.Rhs.LeftColumn,
		CR:            incRule.CR,
		FTR:           incRule.FTR,
		RuleType:      incRule.RuleType,
		XSupp:         0,
		XySupp:        0,
		XSatisfyCount: 0,
		XSatisfyRows:  nil,
		XIntersection: nil,
	}
	return rule
}

func GetNode(rule dao.IncPruneRule) *task_tree.TaskTree {
	incRule := &task_tree.TaskTree{}
	err := json.Unmarshal([]byte(rule.NodeJson), incRule)
	if err != nil {
		logger.Error(err)
		return nil
	}
	return incRule
}

func PrintSampleNodes(sampleNodes []*SampleNode) {
	key2SampleNodes, key2Predicate := SampleNodesGroupByRhs(sampleNodes)
	for key, nodes := range key2SampleNodes {
		rhs := key2Predicate[key]
		logger.Infof("*************** sample rhs:%v start *****************", rhs.PredicateStr)
		for i, sampleNode := range nodes {
			logger.Infof("sample Num.%v, layer:%v", i, len(sampleNode.CurrentNode.Node.Lhs))
			currentNodeRee := GetIncRuleRee(sampleNode.CurrentNode)
			logger.Infof("current node ree:%v, support:%v, confidence:%v", currentNodeRee, sampleNode.CurrentNode.CR, sampleNode.CurrentNode.FTR)
			for _, predecessorNode := range sampleNode.PredecessorNodes {
				predecessorNodeRee := GetIncRuleRee(predecessorNode)
				logger.Infof("predecessor node ree:%v, support:%v, confidence:%v", predecessorNodeRee, predecessorNode.CR, predecessorNode.FTR)
			}
			for _, neighbor := range sampleNode.NeighborNodes {
				neighborRee := GetIncRuleRee(neighbor)
				logger.Infof("neighbor node ree:%v, support:%v, confidence:%v", neighborRee, neighbor.CR, neighbor.FTR)
			}
		}

		logger.Infof("*************** sample rhs:%v end *****************", rhs.PredicateStr)
	}
}

func SampleNodesGroupByRhs(sampleNodes []*SampleNode) (map[string][]*SampleNode, map[string]rds.Predicate) {
	key2SampleNodes := make(map[string][]*SampleNode)
	key2Predicate := make(map[string]rds.Predicate)

	for _, sample := range sampleNodes {
		key := sample.CurrentNode.Node.Rhs.PredicateStr
		key2SampleNodes[key] = append(key2SampleNodes[key], sample)
		key2Predicate[key] = sample.CurrentNode.Node.Rhs
	}
	return key2SampleNodes, key2Predicate
}

func PrintRhsSampleNodes(rhs rds.Predicate, sampleNodes []*SampleNode) {
	logger.Infof("*************** sample rhs:%v start *****************", rhs.PredicateStr)
	for i, sampleNode := range sampleNodes {
		logger.Infof("sample Num.%v, layer:%v", i, len(sampleNode.CurrentNode.Node.Lhs))
		currentNodeRee := GetIncRuleRee(sampleNode.CurrentNode)
		logger.Infof("current node ree:%v, support:%v, confidence:%v", currentNodeRee, sampleNode.CurrentNode.CR, sampleNode.CurrentNode.FTR)
		for _, predecessorNode := range sampleNode.PredecessorNodes {
			predecessorNodeRee := GetIncRuleRee(predecessorNode)
			logger.Infof("predecessor node ree:%v, support:%v, confidence:%v", predecessorNodeRee, predecessorNode.CR, predecessorNode.FTR)
		}
		//for _, neighbor := range sampleNode.NeighborNodes {
		//	neighborRee := GetIncRuleRee(neighbor)
		//	logger.Infof("neighbor node ree:%v, support:%v, confidence:%v", neighborRee, neighbor.CR, neighbor.FTR)
		//}
	}

	logger.Infof("*************** sample rhs:%v end *****************", rhs.PredicateStr)
}
