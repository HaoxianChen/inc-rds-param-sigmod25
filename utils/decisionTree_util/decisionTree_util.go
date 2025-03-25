package decisionTree_util

import (
	"fmt"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/global/enum"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables"
	"gitlab.grandhoo.com/rock/rock_v3/rds_config"
	"gitlab.grandhoo.com/rock/rock_v3/utils"
	"gitlab.grandhoo.com/rock/rock_v3/utils/storage_utils"
)

func ExecuteDecisionTree(tableId string, lhs []rds.Predicate, rhs rds.Predicate, xRows [][2][]int32, cr float64, ftr float64, gv *global_variables.GlobalV) int {
	//lhs := GetSortedLhs(lhsMap)
	rhsColumn := rhs.LeftColumn.ColumnId
	lhsStr := utils.GetLhsStr(lhs)
	gv.DecisionTreeLock.Lock()
	_, ok1 := gv.DecisionTreeIndex[lhsStr+"__"+rhsColumn]
	gv.DecisionTreeLock.Unlock()
	cnt := 0
	if !ok1 {
		gv.DecisionTreeLock.Lock()
		gv.DecisionTreeIndex[lhsStr+"__"+rhsColumn] = true
		gv.DecisionTreeLock.Unlock()
		var rows []int32
		for _, value := range xRows {
			rows = value[0]
		}
		rules, xSupports, xySupports, err := decision_tree.DecisionTree(tableId, lhs, lhsStr, rows, rhsColumn, cr, ftr, gv)
		if err != nil {
			fmt.Println(err)
			return 0
		}
		if rules != nil {
			for i := 0; i < len(rules); i++ {
				if utils.IsAbandoned(rules[i].Ree, gv.AbandonedRules) {
					logger.Infof("规则:%v 曾经被废弃过,不再进行后续操作", rules[i].Ree)
				} else {
					// 置信度为1的规则跳过
					if xySupports[i] > xSupports[i] {
						continue
					}
					logger.Debugf("decision tree %v find rule: %v, rowSize: %v, xySupp: %v, xSupp: %v", gv.TaskId, rules[i].Ree, gv.RowSize, xySupports[i], xSupports[i])
					rules[i].Rhs.ConstantIndexValue = gv.Value2Index[rules[i].Rhs.LeftColumn.ColumnId][rules[i].Rhs.ConstantValue]
					// 设置涉及到的行
					rules[i].XSatisfyCount = xSupports[i]
					for j, predicate := range rules[i].LhsPredicates {
						if predicate.SymbolType != enum.Equal {
							rules[i].LhsPredicates[j].ConstantIndexValue = gv.Value2Index[predicate.LeftColumn.ColumnId][predicate.ConstantValue]
						}
					}
					gv.SingleRuleSize++
					cnt++

					storage_utils.SaveRule(rules[i], gv)

					if cnt >= rds_config.PredicateRuleLimit {
						return cnt
					}
				}
			}
		}
	}
	return cnt
}
