package dcfinder

import (
	"bufio"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock_v3/inc_rule_dig"
	"gitlab.grandhoo.com/rock/rock_v3/rds_config"
	"gitlab.grandhoo.com/rock/rock_v3/utils"
	utils2 "gitlab.grandhoo.com/rock/storage/storage2/utils"
	"os"
	"sort"
	"strings"
	"sync"
)

func getDCFinderCoverIncMiner(taskId int64, dcRulePath string) int {
	logger.Infof("[getDCFinderCoverIncMiner] taskId:%v, dcRulePath:%v", taskId, dcRulePath)
	incRuleMap, err := getIncMinerRules(taskId)
	if err != nil {
		logger.Error(err)
		return 0
	}
	reeCount := 0
	for _, incRules := range incRuleMap {
		reeCount += len(incRules)
	}

	dcRuleMap, err := getDCFinderRules(dcRulePath)
	if err != nil {
		logger.Error(err)
		return 0
	}

	coverCount := 0
	for rhsKey, incRules := range dcRuleMap {
		dcRules := incRuleMap[rhsKey]
		for _, dcRule := range dcRules {
			isCover := false
			for _, incRule := range incRules {
				if inc_rule_dig.Contains(dcRule, incRule) {
					coverCount++
					isCover = true
					break
				}
			}
			if !isCover {
				logger.Infof("[getDCFinderCoverIncMiner] not cover dc rule, rhs:%v, ree:%v", rhsKey, strings.Join(dcRule, " ^ "))
			}
		}
	}

	ratio := float64(coverCount) / float64(reeCount)
	logger.Infof("[getDCFinderCoverIncMiner] finish... cover count:%v, ratio:%v", coverCount, ratio)
	return coverCount
}

func getIncMinerCoverDCFinder(taskId int64, dcRulePath string) int {
	logger.Infof("[getIncMinerCoverDCFinder] taskId:%v, dcRulePath:%v", taskId, dcRulePath)
	incRuleMap, err := getIncMinerRules(taskId)
	if err != nil {
		logger.Error(err)
		return 0
	}

	dcRuleMap, err := getDCFinderRules(dcRulePath)
	if err != nil {
		logger.Error(err)
		return 0
	}
	dcCount := 0
	for _, dcRules := range dcRuleMap {
		dcCount += len(dcRules)
	}

	coverCount := 0
	for rhsKey, incRules := range incRuleMap {
		dcRules := dcRuleMap[rhsKey]
		for _, dcRule := range dcRules {
			isCover := false
			for _, incRule := range incRules {
				if inc_rule_dig.Contains(dcRule, incRule) {
					coverCount++
					isCover = true
					break
				}
			}
			if !isCover {
				logger.Infof("[getIncMinerCoverDCFinder] not cover dc rule, rhs:%v, ree:%v", rhsKey, strings.Join(dcRule, " ^ "))
			}
		}
	}

	ratio := float64(coverCount) / float64(dcCount)
	logger.Infof("[getIncMinerCoverDCFinder] finish ... cover count:%v, ratio:%v", coverCount, ratio)
	return coverCount
}

func getIncMinerRules(taskId int64) (map[string][][]string /* rhs -> []lhs */, error) {
	rulesMap := make(map[string][][]string)

	incRules, err := inc_rule_dig.GetMinimalIncRulesByTaskId(taskId)
	if err != nil {
		logger.Errorf("[getIncMinerRules] GetMinimalIncRulesByTaskId error:%v, taskId:%v", err, taskId)
		return nil, err
	}
	miniRules := getMinimizeProcessRulesNew(incRules)
	for _, rule := range miniRules {
		rhs := rule.Node.Rhs.PredicateStr
		lhsStrs := make([]string, len(rule.Node.Lhs))
		for i, p := range rule.Node.Lhs {
			lhsStrs[i] = p.PredicateStr
		}
		rulesMap[rhs] = append(rulesMap[rhs], lhsStrs)
	}

	decisionRules, err := getDecisionTreeRules(taskId)
	if err != nil {
		logger.Errorf("[getIncMinerRules] GetDecisionTreeRules error:%v, taskId:%v", err, taskId)
		return nil, err
	}
	for _, rule := range decisionRules {
		rhs := rule.Rhs.PredicateStr
		lhsStrs := make([]string, len(rule.LhsPredicates))
		for i, p := range rule.LhsPredicates {
			lhsStrs[i] = p.PredicateStr
		}
		rulesMap[rhs] = append(rulesMap[rhs], lhsStrs)
	}

	srcRulesTotal := len(incRules) + len(decisionRules)
	groupRulesTotal := 0
	for _, rules := range rulesMap {
		groupRulesTotal += len(rules)
	}
	logger.Infof("[getIncMinerRules] inc:%v|decision tree:%v|total:%v -> after group:%v", len(incRules), len(decisionRules), srcRulesTotal, groupRulesTotal)
	return rulesMap, nil
}

func getDCFinderRules(txtFile string) (map[string][][]string /* rhs -> lhs */, error) {
	file, err := os.Open(txtFile)
	if err != nil {
		logger.Errorf("[getDCFinderRules] os.Open error:%v, txtFile:%v", err, txtFile)
		return nil, err
	}
	defer file.Close()

	lines := make([]string, 0)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		lines = append(lines, line)
	}

	// 检查是否发生错误
	if err = scanner.Err(); err != nil {
		logger.Errorf("[getDCFinderRules] reading file error:%v, txtFile:%v", err, txtFile)
		return nil, err
	}

	ruleMap := make(map[string][][]string)
	// 解析每一行，获取ree规则
	for _, line := range lines {
		lineSplit := strings.Split(line, "->")
		left := lineSplit[0]
		rhs := strings.TrimSpace(lineSplit[1])
		rhs = strings.Replace(rhs, "'", "", -1)
		leftSplit := strings.Split(left, "^")
		lhs := make([]string, len(leftSplit))
		for i, pStr := range leftSplit {
			lhs[i] = strings.TrimSpace(pStr)
			lhs[i] = strings.Replace(lhs[i], "'", "", -1)
		}
		ruleMap[rhs] = append(ruleMap[rhs], lhs)
	}

	total := 0
	for _, rules := range ruleMap {
		total += len(rules)
	}
	logger.Infof("[getDCFinderRules] dc ree total:%v", total)
	return ruleMap, nil
}

func getDecisionTreeRules(taskId int64) ([]*rds.Rule, error) {
	rules, err := inc_rule_dig.GetDecisionTreeRules(taskId)
	if err != nil {
		logger.Errorf("GetDecisionTreeRules error:%v, taskId:%v", err, taskId)
		return nil, err
	}
	needRules := make([]*rds.Rule, 0)
	for _, rule := range rules {
		isNeed := true
		for _, p := range rule.LhsPredicates {
			if p.SymbolType != rds_config.Equal {
				isNeed = false
				break
			}
		}
		if rule.Rhs.SymbolType != rds_config.Equal {
			isNeed = false
		}
		if isNeed {
			needRules = append(needRules, rule)
		}
	}
	miniRules := getDecisionTreeRuleMinimize(needRules)
	logger.Infof("[getDecisionTreeRules] totalRules:%v, needRules:%v, minimalRules:%v", len(rules), len(needRules), len(miniRules))
	return miniRules, nil
}

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

func getTasksIncMinerRules(taskIds []int64) (map[string][][]string /* rhs -> []lhs */, error) {
	rulesMap := make(map[string][][]string)

	srcRulesTotal := 0
	for _, taskId := range taskIds {
		incRules, err := inc_rule_dig.GetMinimalIncRulesByTaskId(taskId)
		if err != nil {
			logger.Errorf("[getIncMinerRules] GetMinimalIncRulesByTaskId error:%v, taskId:%v", err, taskId)
			return nil, err
		}
		for _, rule := range incRules {
			rhs := rule.Node.Rhs.PredicateStr
			rhs = strings.Replace(rhs, "=", " = ", -1)
			lhsStrs := make([]string, len(rule.Node.Lhs))
			for i, p := range rule.Node.Lhs {
				val := strings.Replace(p.PredicateStr, "=", " = ", -1)
				lhsStrs[i] = val
			}
			rulesMap[rhs] = append(rulesMap[rhs], lhsStrs)
		}

		decisionRules, err := inc_rule_dig.GetDecisionTreeRules(taskId)
		if err != nil {
			logger.Errorf("[getIncMinerRules] GetDecisionTreeRules error:%v, taskId:%v", err, taskId)
			return nil, err
		}
		for _, rule := range decisionRules {
			rhs := rule.Rhs.PredicateStr
			rhs = strings.Replace(rhs, "=", " = ", -1)
			lhsStrs := make([]string, len(rule.LhsPredicates))
			for i, p := range rule.LhsPredicates {
				val := strings.Replace(p.PredicateStr, "=", " = ", -1)
				lhsStrs[i] = val
			}
			rulesMap[rhs] = append(rulesMap[rhs], lhsStrs)
		}
		oneRulesSize := len(incRules) + len(decisionRules)
		srcRulesTotal += oneRulesSize
	}

	groupRulesTotal := 0
	for _, rules := range rulesMap {
		groupRulesTotal += len(rules)
	}
	logger.Infof("[getIncMinerRules] total:%v -> after group:%v", srcRulesTotal, groupRulesTotal)
	return rulesMap, nil
}

func getTasksIncMinerCoverDCFinder(taskIds []int64, dcRulePath string) int {
	logger.Infof("[getIncMinerCoverDCFinder] taskIds:%v, dcRulePath:%v", taskIds, dcRulePath)
	incRuleMap, err := getTasksIncMinerRules(taskIds)
	if err != nil {
		logger.Error(err)
		return 0
	}

	dcRuleMap, err := getDCFinderRules(dcRulePath)
	if err != nil {
		logger.Error(err)
		return 0
	}

	coverCount := 0
	for rhsKey, incRules := range incRuleMap {
		dcRules := dcRuleMap[rhsKey]
		for _, dcRule := range dcRules {
			isCover := false
			for _, incRule := range incRules {
				if inc_rule_dig.Contains(dcRule, incRule) {
					coverCount++
					isCover = true
					break
				}
			}
			if !isCover {
				logger.Infof("[getIncMinerCoverDCFinder] not cover dc rule, rhs:%v, ree:%v", rhsKey, strings.Join(dcRule, " ^ "))
			}
		}
	}

	logger.Infof("[getIncMinerCoverDCFinder] finish cover count:%v", coverCount)
	return coverCount
}

func preProcessTxtFile(inputFile string, outputFile string) {
	readFile, err := os.Open(inputFile)
	if err != nil {
		logger.Errorf("[preProcessTxtFile] os.Open error:%v, txtFile:%v", err, inputFile)
		return
	}

	lines := make([]string, 0)
	scanner := bufio.NewScanner(readFile)
	for scanner.Scan() {
		line := scanner.Text()
		lineSplit := strings.Split(line, ",")
		lines = append(lines, lineSplit[0])
	}

	writeFile, err := os.Create(outputFile)
	if err != nil {
		logger.Errorf("[preProcessTxtFile] os.Create error:%v, outputFile:%v", err, outputFile)
		return
	}
	defer func() {
		readFile.Close()
		writeFile.Close()
	}()

	writer := bufio.NewWriter(writeFile)
	for i, line := range lines {
		_, err1 := writer.WriteString(line + "\n")
		if err1 != nil {
			logger.Errorf("[preProcessTxtFile] writing to file error:%v, index:%v", err1, i)
			return
		}
	}

	// 刷新缓冲区，将数据写入文件
	err = writer.Flush()
	if err != nil {
		logger.Errorf("[preProcessTxtFile] flushing data error:%v", err)
	}
}
