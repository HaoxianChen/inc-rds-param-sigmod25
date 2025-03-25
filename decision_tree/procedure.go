package decision_tree

//package main

import (
	"encoding/json"
	"fmt"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/global/enum"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/call"
	mine_conf "gitlab.grandhoo.com/rock/rock_v3/decision_tree/conf/mine"
	tree_conf "gitlab.grandhoo.com/rock/rock_v3/decision_tree/conf/tree"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/corr"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/etcd"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/format"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/ml/tree"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/param/conf_cluster"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/param/conf_decisiontree"
	decision_utils "gitlab.grandhoo.com/rock/rock_v3/decision_tree/util"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/util/dataManager"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/util/predicate"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables"
	"gitlab.grandhoo.com/rock/rock_v3/rds_config"
	"gitlab.grandhoo.com/rock/rock_v3/utils"
	"math"
	"math/rand"
	"net"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"
)

var _selfRand = rand.New(rand.NewSource(1)) //select-num的随机种子
var seq = 1

func Mining(worker *call.ServerOnWorker, seq int, tableId string, lhsPredicates []rds.Predicate,
	lhs string, conditionCols []string, rows []int32, y string, support float64,
	confidence float64, gv *global_variables.GlobalV) (rules []rds.Rule, xSupports []int, xySupports []int, err error) {
	rules = make([]rds.Rule, 0)
	xSupports = make([]int, 0)
	xySupports = make([]int, 0)

	if conf_decisiontree.CMD_DECISIONTREE {
		//生成dataframe
		data, _, tableLength, colName2Type, haveDigitalXColumn, err := dataManager.TableToDataFrameWithRowIndexes(tableId, conditionCols, rows, y, gv)
		//data := format.NewDataFrame(tableData, xList, y)
		if !haveDigitalXColumn {
			return nil, nil, nil, nil
		}
		if err != nil {
			return nil, nil, nil, err
		}
		yList := []string{y}

		if data == nil {
			return nil, nil, nil, &dataManager.MyError{Msg: "data is nil"}
		}
		logger.Debugf("Mining Start! tableId: %s, lhs:%s, yColumn:%s, support:%v, confidence:%v, worker:%p", tableId, lhs, y, support, confidence, worker)
		logger.Debugf("dataFrame:----%d, worker:%p", data.Len(), worker)
		if gv.StopTask {
			return nil, nil, nil, nil
		}
		for _, info := range yList {
			data.SetYIndex(info)
			//相关性分析
			CorrAnalysis(worker, data, mine_conf.GenerateDataFrameCoreNum)

			err := call.SetData(worker, data, nil)
			if err != nil {
				return nil, nil, nil, err
			}
			masterPort, _ := strconv.Atoi(conf_cluster.Cluster.HspawnMasterServicePort)
			// 获取master，master只在一台机器上存在
			master, _, manager, err := call.StartMasterServer(masterPort, seq)
			if err != nil {
				return nil, nil, nil, err
			}
			key := fmt.Sprint(unsafe.Pointer(worker))
			call.Worker2Master.Set(key, master)
			//if server == nil {
			//	// 不是master，等待master完成，因为还要同步这里的垂直扩展
			//	call.WaitMaster()
			//} else {
			//step 7:通过决策树（随机森林）生成zlRule
			totalPaths := GenRules(worker, data, manager, gv.DecisionTreeMaxDepth, gv.StopTask)
			//tree.PrintDecisionPath(totalPaths)
			if gv.StopTask {
				return nil, nil, nil, nil
			}
			ruleList, xSupportList, xySupportList := PathToRules(tableId, data, colName2Type, lhsPredicates, totalPaths, tableLength, lhs, support, confidence, gv)
			rules = append(rules, ruleList...)
			xSupports = append(xySupports, xSupportList...)
			xySupports = append(xySupports, xySupportList...)
			//这里要去清空之前塞入etcd的信息
			//ClearTempEtcdInfo()
			//err = server.Stop()
			//if err != nil {
			//	log.Error().Msgf("failed to stop master server --> %v", err)
			//}
			call.WorkerAndMasterDone(worker, master)
		}
		//data.Clear()
		//手动gc
		runtime.GC()
	}
	logger.Debugf("Mining Over! tableId: %s, lhs:%s, yColumn:%s, support:%v, confidence:%v, worker:%p", tableId, lhs, y, support, confidence, worker)
	return rules, xSupports, xySupports, nil
}

func GenRules(worker *call.ServerOnWorker, dataFrame *format.DataFrame, manager *call.ManagerOnMaster, maxTreeDepth int, stopTask bool) [][]*tree.DecisionInfo {
	//step 1:统计y的singlePos和neg
	totalPaths := [][]*tree.DecisionInfo(nil)
	//step 2:获取实例在dataFrame中的下标
	allInsNum := dataFrame.Len()
	instance := make([]int, allInsNum)
	for m := 0; m < allInsNum; m++ {
		instance[m] = m
	}
	//logger.Debugf("instance num is : %d, worker:%p", allInsNum, worker)
	//step 3:构建决策树（随机森林）
	for i := 0; i < tree_conf.TreeNum; i++ {
		support := int(float64(len(instance)) * tree_conf.MinSupportRate)
		//如果低于最低support限制，改为最低support
		if support < tree_conf.MinSupportLimit {
			support = tree_conf.MinSupportLimit
		}
		//startTime := time.Now()
		classifierOptions := []call.ClassifierOption{call.CriterionByName(tree_conf.ImpurityCriterion)}
		if tree_conf.MaxLeafNum > 0 {
			classifierOptions = append(classifierOptions, call.BuilderDirect(&call.NewBestFirstTreeBuilder(tree_conf.MaxLeafNum).BuilderBasic))
		}
		if tree_conf.WeightDownByPivot {
			runtime.GC() // 回收一下pivot的列表
		}

		t := call.NewClassifier(classifierOptions...).Fit(
			worker,
			manager,
			stopTask,
			call.MaxDepth(uint32(maxTreeDepth)),
			call.MaxFeatureNum(tree_conf.MaxFeatureNum),
			call.MinImpurityDecrease(tree_conf.MinImpurityDecrease),
			call.MinSupportRateInSplit(tree_conf.MinSupportRate), // todo:现在还没有全部实例的信息
		)
		//t.ToSimpleGraph("./tree.dot")
		result := t.DecisionPaths()
		//log.Info().Msgf("decision tree cost: %v the segmentResult is: %d", time.Since(startTime), len(result))
		totalPaths = append(totalPaths, result...)
	}
	return totalPaths
}

func CorrAnalysis(worker *call.ServerOnWorker, dataFrame *format.DataFrame, coreNum int) {
	//startTime := time.Now()
	xIndexes := dataFrame.GetXIndexList()
	newXIndexes := make([]string, 0)
	if tree_conf.CorrFlag {
		yData := make([]float64, dataFrame.Len())
		dataFrame.GetAllValuesOf(dataFrame.GetYIndex(), yData)
		c := corr.NewCalculatorManager(yData, "normal", coreNum)
		for _, index := range xIndexes {
			c.AppendColIndex(index)
		}
		c.Run(dataFrame)
		newXIndexes = selectTopCol(c.GetResult())
	} else {
		newXIndexes = xIndexes
	}
	//这里需要排个序
	sort.Strings(newXIndexes)
	//step 3:在这里插入xIndex进行更新，新增属性的方法都需要放在这之前
	//随机生成selectNum个不重复的数
	//如果selectNum过大，就按照全部塞入
	keyNums := len(newXIndexes)
	if keyNums <= tree_conf.SelectNum {
		dataFrame.SetValidXList(newXIndexes...)
	} else {
		xList := make([]string, keyNums, keyNums)
		copy(xList, newXIndexes)
		for k := 0; k < tree_conf.SelectNum; k++ {
			get := _selfRand.Intn(len(xList) - k)
			xList[get], xList[len(xList)-1-k] = xList[len(xList)-1-k], xList[get]
		}
		filterXList := make([]string, tree_conf.SelectNum, tree_conf.SelectNum)
		copy(filterXList, xList[len(xList)-tree_conf.SelectNum:])
		dataFrame.SetValidXList(filterXList...)
		logger.Warnf("after sample,the select key num is : %d", len(filterXList))
	}
	//log.Debug().Msgf("corr analysis cost: %v, worker:%p", time.Since(startTime), worker)
	//log.Warn().Msgf("after corr analysis,the xKey num is: %d, worker:%p", len(newXIndexes), worker)
}

func selectTopCol(result []corr.CorrelationResult) []string {
	////todo 在这里塞入etcd，将result序列化
	marshal, err := json.Marshal(result)
	if err != nil {
		logger.Error(err.Error())
	}

	//if _, err = etcd.GlobalEtcd.Put(tree_conf.CorrMask+string(rune(conf_cluster.Cluster.MachineNumber)), string(marshal)); err != nil {
	//	log.Error().Msg(err.Error())
	//}
	var wg sync.WaitGroup
	finalResult := make([]string, 0)
	wg.Add(1)
	for {
		//kVs := etcd.GlobalEtcd.GetKVWithPrefix(tree_conf.CorrMask)
		//if len(kVs) == conf_cluster.Cluster.MachineNumber {
		totalMap := make(map[string]corr.CorrelationResult)
		totalAttr := make([]string, 0)
		topMap := make(map[string]struct{})
		//for _, v := range kVs {
		get := make([]corr.CorrelationResult, 0)
		err = json.Unmarshal(marshal, &get)
		if err != nil {
			logger.Error(err.Error())
			return nil
		}
		for _, corrResult := range get {
			if avg, ok := totalMap[corrResult.Index]; !ok {
				totalMap[corrResult.Index] = corrResult
				totalAttr = append(totalAttr, corrResult.Index)
			} else {
				avg.Pearson += corrResult.Pearson
				avg.Spearman += corrResult.Spearman
				avg.Kendall += corrResult.Kendall
				totalMap[corrResult.Index] = avg
			}
		}
		//}
		//输出Spearman的topCol
		sort.Slice(totalAttr, func(i, j int) bool {
			if totalMap[totalAttr[i]].Spearman == totalMap[totalAttr[j]].Spearman {
				return totalAttr[i] < totalAttr[j]
			}
			return totalMap[totalAttr[i]].Spearman > totalMap[totalAttr[j]].Spearman
		})
		max := 0
		if len(totalAttr) < tree_conf.TopCol {
			max = len(totalAttr)
		} else {
			max = tree_conf.TopCol
		}
		for i := 0; i < max; i++ {
			topMap[totalAttr[i]] = struct{}{}
		}
		//输出Kendall的topCol
		sort.Slice(totalAttr, func(i, j int) bool {
			if totalMap[totalAttr[i]].Kendall == totalMap[totalAttr[j]].Kendall {
				return totalAttr[i] < totalAttr[j]
			}
			return totalMap[totalAttr[i]].Kendall > totalMap[totalAttr[j]].Kendall
		})
		for i := 0; i < max; i++ {
			topMap[totalAttr[i]] = struct{}{}
		}
		//输出Pearson的topCol
		sort.Slice(totalAttr, func(i, j int) bool {
			if totalMap[totalAttr[i]].Pearson == totalMap[totalAttr[j]].Pearson {
				return totalAttr[i] < totalAttr[j]
			}
			return totalMap[totalAttr[i]].Pearson > totalMap[totalAttr[j]].Pearson
		})
		for i := 0; i < max; i++ {
			topMap[totalAttr[i]] = struct{}{}
		}
		//转换结果输出
		for s := range topMap {
			finalResult = append(finalResult, s)
		}
		wg.Done()
		break
		//}
	}
	wg.Wait()
	return finalResult
}

func getYSupport() (pos int, neg int) {
	//这里是master节点，需要去等待其他机器做完
	var wg sync.WaitGroup
	wg.Add(1)
	for {
		posKVs := etcd.GlobalEtcd.GetKVWithPrefix(tree_conf.YCountPosMask)
		negKVs := etcd.GlobalEtcd.GetKVWithPrefix(tree_conf.YCountNegMask)
		//所有机器都统计完了
		if len(posKVs) == conf_cluster.Cluster.MachineNumber && len(negKVs) == conf_cluster.Cluster.MachineNumber {
			for _, v := range posKVs {
				count, err := strconv.Atoi(string(v.Value))
				if err != nil {
					logger.Error(err.Error())
					return 0, 0
				} else {
					pos += count
				}
			}
			for _, v := range negKVs {
				count, err := strconv.Atoi(string(v.Value))
				if err != nil {
					logger.Error(err.Error())
					return 0, 0
				} else {
					neg += count
				}
			}
			wg.Done()
			break
		}
	}
	wg.Wait()
	return
}

// ClearTempEtcdInfo 清除一轮决策树后etcd里相关的暂存内容，以免影响下一次计算
func ClearTempEtcdInfo() {
	_, err := etcd.GlobalEtcd.DeleteWithPrefix(tree_conf.CorrMask)
	if err != nil {
		logger.Errorf("CorrMask: %v", err)
		return
	}
	_, err = etcd.GlobalEtcd.DeleteWithPrefix(tree_conf.YCountPosMask)
	if err != nil {
		logger.Errorf("YCountPosMask: %v", err)
		return
	}
	_, err = etcd.GlobalEtcd.DeleteWithPrefix(tree_conf.YCountNegMask)
	if err != nil {
		logger.Errorf("YCountNegMask: %v", err)
		return
	}
}

// legalRule判断当前路径的规则是否满足support、confidence条件
func legalRule(data *format.DataFrame, lhsList [][]interface{}, yValue float64, tableLength int64, support float64,
	confidence float64, stopTask bool) (bool, float64, float64, int, int, error) {
	legalXIndex := make([]int, data.Len(), data.Len())
	for i := range legalXIndex {
		legalXIndex[i] = i
	}

	for _, lhs := range lhsList {
		if stopTask {
			return false, 0, 0, 0, 0, nil
		}
		colName := utils.GetInterfaceToString(lhs[0])
		symbol := utils.GetInterfaceToString(lhs[1])
		constant, err := strconv.ParseFloat(utils.GetInterfaceToString(lhs[2]), 10)
		if err != nil {
			logger.Infof("Parse value %s to Float error", fmt.Sprint(lhs[2]))
			return false, 0, 0, 0, 0, err
		}
		rowIndexes := make([]int, len(legalXIndex))
		count := 0
		for i := 0; i < len(legalXIndex); i++ {
			value := data.GetFloat64Element(legalXIndex[i], colName)
			if (rds_config.Less == symbol && value < constant) ||
				(rds_config.GreaterE == symbol && value >= constant) ||
				(rds_config.Greater == symbol && value > constant) {
				rowIndexes[count] = legalXIndex[i]
				count++
			}
		}
		legalXIndex = rowIndexes[:count]
	}
	xSupport := len(legalXIndex)
	yColName := data.GetYIndex()
	legalYIndex := make([]int, len(legalXIndex))
	count := 0
	for i := 0; i < len(legalXIndex); i++ {
		if data.GetFloat64Element(legalXIndex[i], yColName) == yValue {
			legalYIndex[count] = legalXIndex[i]
			count++
		}
	}
	legalYIndex = legalYIndex[:count]
	xySupport := len(legalYIndex)
	curSupport := float64(xySupport) / float64(tableLength)
	curConfidence := float64(xySupport) / float64(xSupport)
	if math.IsNaN(curSupport) {
		curSupport = 0
	}
	if math.IsNaN(curConfidence) {
		curConfidence = 0
	}
	if curSupport > support && curConfidence > confidence {
		return true, curSupport, curConfidence, xSupport, xySupport, nil
	} else {
		return false, curSupport, curConfidence, xSupport, xySupport, nil
	}

}

func copyPath(path []*tree.DecisionInfo) []tree.DecisionInfo {
	pathCopied := make([]tree.DecisionInfo, 0)
	for _, node := range path {
		pathCopied = append(pathCopied, *node)
	}
	return pathCopied
}

func PathToRules(tableId string, data *format.DataFrame, colName2Type map[string]string, lhsPredicates []rds.Predicate,
	totalPath [][]*tree.DecisionInfo, tableLength int64, lhs string,
	support float64, confidence float64, gv *global_variables.GlobalV) (rules []rds.Rule, xSupports []int, xySupports []int) {
	rules = make([]rds.Rule, 0)
	xSupports = make([]int, 0)
	xySupports = make([]int, 0)
	if totalPath == nil {
		return rules, xSupports, xySupports
	}

	yNum2RealValue := data.GetYNumToValueMap()

	for _, path := range totalPath {
		if gv.StopTask {
			return nil, nil, nil
		}
		if len(path) == 1 {
			continue
		}
		sortedLhs := make([]string, 0)
		lhsList := make([][]interface{}, 0)
		lhsNewPredicates := make([]rds.Predicate, 0)
		pathCopied := copyPath(path)
		path = lhsFilter(pathCopied)
		for x, node := range path {
			if x == len(path)-1 {
				break
			}
			feature := node.Feature
			colName := data.GetFeatureByValidIndex(int(feature))
			var symbol string
			if path[x+1].IsLeft { //决策树将等于号划分到右边
				symbol = "<"
			} else {
				symbol = ">="
			}
			value := node.SplitValue

			var constantValue interface{}
			//不同的值的数量超过100时会变成区间去计算，计算的分裂值可能是小数，所以分裂值可能不在该列的数据中，但查错是根据索引进行计算的而不是根据值，所以这里需要根据分裂值找到最接近的向上的真实值
			constantValue, exist := data.QueryExistRealValue(colName, value)
			var colDataSorted []float64
			if !exist {
				colDataSorted = data.GetColumnValuesSorted(colName)
				index := sort.Search(len(colDataSorted), func(i int) bool {
					return colDataSorted[i] >= value
				})

				if index == len(colDataSorted) { //应该不会出现的，因为决策树不会将全部划分到一个区间
					constantValue = colDataSorted[index-1]
					data.SetSplitValue2RealValue(colName, value, colDataSorted[index-1])
				} else {
					constantValue = colDataSorted[index]
					data.SetSplitValue2RealValue(colName, value, colDataSorted[index])
				}
			}

			//dataframe读入x列时统一转为float64，返回给规则时需以真实类型返回
			colType := colName2Type[colName]
			if colType == rds_config.IntType {
				relValue, e := strconv.ParseInt(utils.GetInterfaceToString(constantValue), 10, 64)
				if e != nil {
					logger.Error("column " + colName + ", parse constantValue " + fmt.Sprint(constantValue) + " to " + colType + " type error")
				}
				constantValue = relValue
			}

			lhs := make([]interface{}, 0)
			lhs = append(lhs, colName, symbol, constantValue)
			lhsList = append(lhsList, lhs)

			lhsPredicate := rds.Predicate{
				PredicateStr:  fmt.Sprintf("t0.%s%s%s", lhs[0], lhs[1], utils.GetInterfaceToString(lhs[2])),
				LeftColumn:    rds.Column{ColumnId: colName, ColumnType: colName2Type[colName], TableId: tableId},
				RightColumn:   rds.Column{},
				ConstantValue: constantValue,
				SymbolType:    symbol,
				PredicateType: 0,
			}
			lhsNewPredicates = append(lhsNewPredicates, lhsPredicate)
		}
		classSingleCount := path[len(path)-1].ClassSingleCount
		legalYValues := make([]float64, 0)
		y2supportConfidence := make(map[float64][]float64)
		y2RhsPredicate := make(map[float64]rds.Predicate)
		for yValue := range classSingleCount {
			legalFlag, realSupport, realConfidence, xSupport, xySupport, err := legalRule(data, lhsList, yValue, tableLength, support, confidence, gv.StopTask)
			if err != nil {
				logger.Errorf("lhs:%s, yValue:%s, judge legal rule error", lhsList, yValue, err)
			}
			if legalFlag {
				legalYValues = append(legalYValues, yValue)
				y2supportConfidence[yValue] = []float64{realSupport, realConfidence, float64(xSupport), float64(xySupport)}
				rhsPredicateNew := rds.Predicate{
					PredicateStr:  fmt.Sprintf("t0.%s%s%s", data.GetYIndex(), enum.Equal, utils.GetInterfaceToString(yNum2RealValue[yValue])),
					LeftColumn:    rds.Column{ColumnId: data.GetYIndex(), ColumnType: colName2Type[data.GetYIndex()], TableId: tableId},
					RightColumn:   rds.Column{},
					ConstantValue: yNum2RealValue[yValue],
					SymbolType:    enum.Equal,
					PredicateType: 0,
				}
				y2RhsPredicate[yValue] = rhsPredicateNew
			}
		}

		if len(legalYValues) == 0 {
			continue
		}

		for _, lhs := range lhsList {
			lhsString := fmt.Sprintf("t0.%s%s%v", lhs[0], lhs[1], lhs[2])
			sortedLhs = append(sortedLhs, lhsString)
		}

		//sortedLhs = append(sortedLhs, conditionLhsList...)
		sort.Strings(sortedLhs)
		//tableNameLhs := fmt.Sprintf("%s(t0)", tableName)
		if lhs != "" {
			sortedLhs = append([]string{lhs}, sortedLhs...)
			lhsNewPredicates = append(lhsNewPredicates, lhsPredicates...)
		}
		//sortedLhs = append([]string{tableNameLhs}, sortedLhs...)
		px := strings.Join(sortedLhs, " ^ ")
		for _, label := range legalYValues {
			yRealValue, exist := yNum2RealValue[label]
			if !exist {
				logger.Errorf("Can not find realValue of yNum %v", label)
				continue
			}
			yRealValue = fmt.Sprintf("%v", yRealValue)
			rhs := fmt.Sprintf("t0.%s%s%s", data.GetYIndex(), "=", yRealValue)
			ree := fmt.Sprintf("%s %s %s", px, "->", rhs)
			ree = utils.GenerateTableIdStr(tableId, true) + ree
			fmt.Printf("%s,%v,%v,%v,%v\n", ree, y2supportConfidence[label][0], y2supportConfidence[label][1],
				y2supportConfidence[label][2], y2supportConfidence[label][3])
			lhsColumns := make([]rds.Column, 0)
			for _, predicate := range lhsNewPredicates {
				lhsColumns = append(lhsColumns, predicate.LeftColumn)
			}
			rule := rds.Rule{
				TableId:       tableId,
				Ree:           ree,
				LhsPredicates: lhsNewPredicates,
				LhsColumns:    lhsColumns,
				Rhs:           y2RhsPredicate[label],
				RhsColumn:     y2RhsPredicate[label].LeftColumn,
				CR:            y2supportConfidence[label][0],
				FTR:           y2supportConfidence[label][1],
				RuleType:      0,
				XSupp:         int(y2supportConfidence[label][2]),
				XySupp:        int(y2supportConfidence[label][3]),
			}

			rules = append(rules, rule)
			xSupports = append(xSupports, int(y2supportConfidence[label][2]))
			xySupports = append(xySupports, int(y2supportConfidence[label][3]))
		}
	}
	return rules, xSupports, xySupports
}

// lhs谓词过滤同列存在包含关系的谓词
func lhsFilter(path []tree.DecisionInfo) []*tree.DecisionInfo {
	newPath := make([]*tree.DecisionInfo, 0)
	discard := make([]bool, len(path))
	for x := 0; x < len(path)-1; x++ {
		if discard[x] {
			continue
		}
		isLeft := path[x+1].IsLeft
		for y := x + 1; y < len(path)-1; y++ {
			if path[x].Feature == path[y].Feature && isLeft == path[y+1].IsLeft {
				if (isLeft && path[y].SplitValue < path[x].SplitValue) || (!isLeft && path[y].SplitValue > path[x].SplitValue) { //都为<，保留分裂值小的; 都为>=，保留分裂值大的
					discard[x] = true
					path[x+1].IsLeft = path[x].IsLeft
					break
				} else {
					discard[y] = true
					path[y+1].IsLeft = path[y].IsLeft
				}
			}
		}
		if !discard[x] {
			newPath = append(newPath, &path[x])
		}
	}
	newPath = append(newPath, &path[len(path)-1])
	return newPath
}

func DecisionTree(tableId string, lhsPredicates []rds.Predicate, lhs string, rows []int32, rhsColumn string,
	support float64, confidence float64, gv *global_variables.GlobalV) ([]rds.Rule, []int, []int, error) {
	if rds_config.UseStorage {
		return nil, nil, nil, nil
	}

	worker, _, err := call.StartWorkerServer(
		"xxx",
		net.JoinHostPort(conf_cluster.Cluster.LocalIp, conf_cluster.Cluster.HspawnWorkerServicePort),
		[]string{conf_cluster.Cluster.EtcdAddr},
		tree_conf.ImpurityCriterion,
	)

	if err != nil {
		return nil, nil, nil, err
	}

	//取到条件列名
	conditionCols := make([]string, 0)

	for _, lhsPredicate := range lhsPredicates {
		colName := lhsPredicate.LeftColumn.ColumnId
		conditionCols = append(conditionCols, colName)
	}

	if gv.StopTask {
		return nil, nil, nil, nil
	}

	rules, xSupports, xySupports, err := Mining(worker, seq, tableId, lhsPredicates, lhs, conditionCols, rows, rhsColumn, support, confidence, gv)
	if err != nil {
		return nil, nil, nil, err
	}
	return rules, xSupports, xySupports, nil
}

// 如果Y只有一种值，不进行挖掘
func checkYColumnData(data *format.DataFrame) bool {
	colValues := make([]float64, data.Len())
	data.GetAllValuesOf(data.GetYIndex(), colValues)
	valueMap := make(map[float64]struct{})
	for _, value := range colValues {
		valueMap[value] = struct{}{}
		if len(valueMap) > 1 {
			return true
		}
	}
	return false
}

func DecisionTreeWithDataInput(lhs []rds.Predicate, rhs rds.Predicate, header []string, inputData [][]float64, colName2Type map[string]string, index2Table map[string]string,
	yRealValue2Index map[interface{}]float64, taskId int64, CR float64, FTR float64, maxTreeDepth int, stopTask bool) (rules []rds.Rule, xSupports []int, xySupports []int, err error) {
	//return nil, nil, nil, err
	startTime := time.Now().UnixMilli()
	logger.Infof("cal decision tree, row size:%v, column size:%v", len(inputData), len(header))
	logger.Infof("header:%v", header)
	rules = make([]rds.Rule, 0)
	xSupports = make([]int, 0)
	xySupports = make([]int, 0)
	if len(inputData) == 0 || len(header) == 0 {
		logger.Infof("decision tree input data or columns is nil, lhs %s, input data length: %v, columns: %s", utils.GetLhsStr(lhs), len(inputData), header)
		return rules, xSupports, xySupports, nil
	}

	//if len(yRealValue2Index) == 1 { //单行在生成decisionY时控制了，多行没有这个。所以这里没用
	//	logger.Infof("taskId: %v, lhs: %s, yColumn: %s, only one value of yColumn, skip.", taskId, utils.GetLhsStr(lhs), header[len(header)-1])
	//	return rules, xSupports, xySupports, nil
	//}

	worker, _, err := call.StartWorkerServer(
		"xxx",
		net.JoinHostPort(conf_cluster.Cluster.LocalIp, conf_cluster.Cluster.HspawnWorkerServicePort),
		[]string{conf_cluster.Cluster.EtcdAddr},
		tree_conf.ImpurityCriterion,
	)
	if err != nil {
		return nil, nil, nil, err
	}

	index2YRealValue := make(map[float64]interface{})
	for yRealValue, index := range yRealValue2Index {
		index2YRealValue[index] = yRealValue
	}

	xList := header[:len(header)-1]
	y := header[len(header)-1]
	data := format.NewDataFrame(inputData, xList, y)
	data.SetYNumToValueMap(index2YRealValue)
	//if rhs.PredicateType != 0 && !checkYColumnData(data) { //多行
	//	logger.Infof("taskId: %v, lhs: %s, yColumn: %s, only one value of yColumn, skip.", taskId, utils.GetLhsStr(lhs), header[len(header)-1])
	//	return rules, xSupports, xySupports, nil
	//}
	if conf_decisiontree.CMD_DECISIONTREE {
		yList := []string{y}

		if data == nil {
			return nil, nil, nil, &dataManager.MyError{Msg: "data is nil"}
		}
		//logger.Debugf("Mining Start! taskId: %v, lhs:%s, yColumn:%s, support:%v, confidence:%v, worker:%p", taskId, utils.GetLhsStr(lhs), y, CR, FTR, worker)
		//logger.Debugf("dataFrame:----%d, worker:%p", data.Len(), worker)
		if stopTask {
			return nil, nil, nil, nil
		}
		for _, info := range yList {
			data.SetYIndex(info)
			//相关性分析
			CorrAnalysis(worker, data, mine_conf.GenerateDataFrameCoreNum)

			err := call.SetData(worker, data, nil)
			if err != nil {
				return nil, nil, nil, err
			}
			masterPort, _ := strconv.Atoi(conf_cluster.Cluster.HspawnMasterServicePort)
			// 获取master，master只在一台机器上存在
			master, _, manager, err := call.StartMasterServer(masterPort, seq)
			if err != nil {
				return nil, nil, nil, err
			}
			key := fmt.Sprint(unsafe.Pointer(worker))
			call.Worker2Master.Set(key, master)
			totalPaths := GenRules(worker, data, manager, maxTreeDepth, stopTask)
			if stopTask {
				return nil, nil, nil, nil
			}
			logger.Debugf("taskId: %v, lhs:%s, yColumn:%s, finish generate path.start to translate path to rule", taskId, utils.GetLhsStr(lhs), y)
			ruleList, xSupportList, xySupportList := PathTranslation(data, colName2Type, lhs, totalPaths, index2Table, CR, FTR, stopTask)
			rules = append(rules, ruleList...)
			xSupports = append(xySupports, xSupportList...)
			xySupports = append(xySupports, xySupportList...)
			call.WorkerAndMasterDone(worker, master)
		}
		runtime.GC()
	}
	var realRules []rds.Rule
	ruleMap := make(map[string]bool)
	for i := 0; i < len(rules); i++ {
		rule := rules[i]
		if !ruleMap[rule.Ree] {
			ruleMap[rule.Ree] = true
			lhsMap := make(map[string]rds.Predicate)
			for _, pre := range rule.LhsPredicates {
				lhsMap[pre.PredicateStr] = pre
			}
			if len(lhsMap) < len(rule.LhsPredicates) {
				ruleLhs := make([]rds.Predicate, 0, len(lhsMap))
				ruleLhsStr := make([]string, 0, len(lhsMap))
				for _, pre := range lhsMap {
					ruleLhs = append(ruleLhs, pre)
					ruleLhsStr = append(ruleLhsStr, pre.PredicateStr)
				}
				rule.LhsPredicates = ruleLhs
				tableStr := strings.Split(rule.Ree, " ^ ")[0]
				rule.Ree = fmt.Sprintf("%v ^ %v->%v", tableStr, strings.Join(ruleLhsStr, " ^ "), rule.Rhs.PredicateStr)
			}
			realRules = append(realRules, rule)
		}

	}
	logger.Infof("spent time:%vms,Mining Over! taskId: %v, lhs:%s, yColumn:%s, ruleSize: %v", time.Now().UnixMilli()-startTime, taskId, utils.GetLhsStr(lhs), y, len(realRules))
	return realRules, xSupports, xySupports, nil
}

// PathTranslation 决策树路径翻译成规则
// 节点名字可能是column(单行)、t0.column、t0.column=a、t0.column=t2.column等
// 会跑决策树的类型：int,float,bool,enum(string),text(?)
func PathTranslation(data *format.DataFrame, colName2Type map[string]string, lhsPredicates []rds.Predicate, totalPaths [][]*tree.DecisionInfo,
	index2Table map[string]string, CR, FTR float64, stopTask bool) (rules []rds.Rule, xSupports []int, xySupports []int) {
	rules = make([]rds.Rule, 0)
	xSupports = make([]int, 0)
	xySupports = make([]int, 0)
	if totalPaths == nil {
		return rules, xSupports, xySupports
	}

	yNum2RealValue := data.GetYNumToValueMap()
	for _, path := range totalPaths {
		if stopTask {
			return nil, nil, nil
		}
		if len(path) == 1 {
			continue
		}
		sortedLhs := make([]string, 0)
		lhsList := make([][]interface{}, 0)
		lhsNewPredicates := make([]rds.Predicate, 0)
		pathCopied := copyPath(path)
		path = lhsFilter(pathCopied)
		continueFlag := false
		for x, node := range path {
			transferColNameFlag := false
			if x == len(path)-1 {
				break
			}
			feature := node.Feature
			colName := data.GetFeatureByValidIndex(int(feature))
			deriveColumnType := decision_utils.GetDeriveColumnType(colName)
			decisionTreePredicateUtil := predicate.GetDecisionTreePredicatesUtil(deriveColumnType)
			var symbol string
			if path[x+1].IsLeft { //决策树将等于号划分到右边
				symbol = rds_config.Less
			} else {
				symbol = rds_config.GreaterE
			}
			value := node.SplitValue
			//枚举类型、布尔类型: <0(不存在，丢弃),<1(转成!=),>=0(冲突，丢弃),>=1(实际上是等于1,保留),
			//数值类型: 无需另外处理
			if deriveColumnType == predicate.SingleDeriveColumn || deriveColumnType == predicate.MultiDeriveColumn { //枚举或布尔
				if deriveColumnType == predicate.MultiDeriveColumn { //x没有这种
					fmt.Println(1)
				} else {
					if value == float64(0) { //<0, >=0
						continueFlag = true
						break
					} else if rds_config.Less == symbol { //<1
						transferColNameFlag = true
					}
				}
			}
			var constantValue interface{}
			//不同的值的数量超过100时会变成区间去计算，计算的分裂值可能是小数，所以分裂值可能不在该列的数据中，但查错是根据索引进行计算的而不是根据值，所以这里需要根据分裂值找到最接近的向上的真实值
			constantValue, exist := data.QueryExistRealValue(colName, value) //0，1，真实数字值
			var colDataSorted []float64
			if !exist {
				colDataSorted = data.GetColumnValuesSorted(colName)
				index := sort.Search(len(colDataSorted), func(i int) bool {
					return colDataSorted[i] >= value
				})

				if index == len(colDataSorted) { //应该不会出现的，因为决策树不会将全部划分到一个区间
					constantValue = colDataSorted[index-1]
					data.SetSplitValue2RealValue(colName, value, colDataSorted[index-1])
				} else {
					constantValue = colDataSorted[index]
					data.SetSplitValue2RealValue(colName, value, colDataSorted[index])
				}
			}

			//dataframe读入x列时统一转为float64，返回给规则时需以真实类型返回
			colType, exist := colName2Type[colName]
			if !exist {
				logger.Errorf("未找到%s的字段类型", colName)
				continueFlag = true
				break
			}
			if deriveColumnType == predicate.RealColumn && colType == rds_config.IntType {
				relValue, e := strconv.ParseInt(utils.GetInterfaceToString(constantValue), 10, 64)
				if e != nil {
					logger.Error("column " + colName + ", parse constantValue " + fmt.Sprint(constantValue) + " to " + colType + " type error")
				}
				constantValue = relValue
			}

			lhs := make([]interface{}, 0)
			lhs = append(lhs, colName, symbol, constantValue)
			lhsList = append(lhsList, lhs)
			lhsPredicate, err := decisionTreePredicateUtil.GeneratePredicates(index2Table, colName, colType, constantValue, lhs, false, transferColNameFlag)
			if err != nil || lhsPredicate.SymbolType == "" {
				continueFlag = true
				break
			}
			sortedLhs = append(sortedLhs, lhsPredicate.PredicateStr)
			lhsNewPredicates = append(lhsNewPredicates, lhsPredicate)

		}
		if continueFlag {
			continue
		}

		lhsNewPredicates, lhsList, sortedLhs = lhsPredicateFilter(lhsNewPredicates, lhsList, sortedLhs)
		//legalX := checkLhsPredicates(lhsNewPredicates)
		//if !legalX {
		//	continue
		//}

		//y的处理
		yColumnName := data.GetYIndex()
		yDeriveColumnType := decision_utils.GetDeriveColumnType(yColumnName)
		yColumnType, exist := colName2Type[yColumnName]
		if !exist {
			logger.Errorf("未找到%s的字段类型", yColumnName)
			continue
		}
		classSingleCount := path[len(path)-1].ClassSingleCount
		legalYValues := make([]float64, 0)
		y2supportConfidence := make(map[float64][]float64)
		y2RhsPredicate := make(map[float64]rds.Predicate)
		for yValue := range classSingleCount {
			if yDeriveColumnType != predicate.RealColumn && yValue == 0 || yValue == math.MaxFloat64 { //不满足
				continue
			}
			legalFlag, realSupport, realConfidence, xSupport, xySupport, err := legalRule(data, lhsList, yValue, int64(data.Len()), CR, FTR, stopTask)
			if err != nil {
				logger.Errorf("lhs:%s, yValue:%s, judge legal rule error", lhsList, yValue, err)
			}
			if legalFlag {
				y2supportConfidence[yValue] = []float64{realSupport, realConfidence, float64(xSupport), float64(xySupport)}
				var yRealValue interface{} = yValue
				if yNum2RealValue != nil && len(yNum2RealValue) > 0 {
					if _, exist := yNum2RealValue[yValue]; exist {
						yRealValue = yNum2RealValue[yValue]
					}
				}
				if yRealValue == nil {
					continue
				}
				rhsPredicateNew, err := predicate.GetDecisionTreePredicatesUtil(yDeriveColumnType).GeneratePredicates(index2Table, yColumnName, yColumnType, yRealValue, []interface{}{yColumnName, enum.Equal, yRealValue}, true, false)
				if err != nil || rhsPredicateNew.SymbolType == "" {
					continue
				}
				legalYValues = append(legalYValues, yValue)
				y2RhsPredicate[yValue] = rhsPredicateNew
			}
		}

		if len(legalYValues) == 0 {
			continue
		}

		sort.Strings(sortedLhs)

		if len(lhsPredicates) > 0 {
			lhsPredicatesCopy := make([]rds.Predicate, len(lhsPredicates))
			copy(lhsPredicatesCopy, lhsPredicates)
			utils.SortPredicates(lhsPredicatesCopy, true)
			lhsStrs := make([]string, 0)
			for _, l := range lhsPredicatesCopy {
				lhsStrs = append(lhsStrs, l.PredicateStr)
			}
			sortedLhs = append(lhsStrs, sortedLhs...)
			lhsNewPredicates = append(lhsPredicatesCopy, lhsNewPredicates...)
		}
		tableStr := utils.GenerateTableIdStrByIndex2Table(index2Table)
		sortedLhs = append([]string{tableStr}, sortedLhs...)

		px := strings.Join(sortedLhs, " ^ ")
		for _, label := range legalYValues {
			yPredicate := y2RhsPredicate[label]
			ree := fmt.Sprintf("%s %s %s", px, "->", yPredicate.PredicateStr)
			//fmt.Printf("%s,%v,%v,%v,%v\n", ree, y2supportConfidence[label][0], y2supportConfidence[label][1],
			//	y2supportConfidence[label][2], y2supportConfidence[label][3])

			ruleType := 0
			for _, lhs := range lhsNewPredicates {
				if lhs.PredicateType != 0 {
					ruleType = 1
				}
			}
			if yPredicate.PredicateType != 0 {
				ruleType = 1
			}

			rule := rds.Rule{
				TableId:       yPredicate.LeftColumn.TableId,
				Ree:           ree,
				LhsPredicates: lhsNewPredicates,
				LhsColumns:    utils.GetMultiTablePredicatesColumn(lhsNewPredicates),
				Rhs:           yPredicate,
				RhsColumn:     y2RhsPredicate[label].LeftColumn,
				CR:            y2supportConfidence[label][0],
				FTR:           y2supportConfidence[label][1],
				RuleType:      ruleType,
				XSupp:         int(y2supportConfidence[label][2]),
				XySupp:        int(y2supportConfidence[label][3]),
			}

			rules = append(rules, rule)
			xSupports = append(xSupports, int(y2supportConfidence[label][2]))
			xySupports = append(xySupports, int(y2supportConfidence[label][3]))
		}
	}
	return rules, xSupports, xySupports
}

// x谓词检查：1.同表同列冲突谓词，如t0.a=1 ^ t0.a=2，但现在好像不会有这种情况了
func checkLhsPredicates(lhsPredicates []rds.Predicate) bool {
	if len(lhsPredicates) < 2 {
		return true
	}
	keyMap := make(map[string]struct{})
	for _, lhsPredicate := range lhsPredicates {
		if lhsPredicate.PredicateType != 0 {
			continue
		}
		if lhsPredicate.SymbolType != rds_config.Equal {
			continue
		}
		key := fmt.Sprintf("%s_%s", lhsPredicate.LeftColumn.TableId, lhsPredicate.LeftColumn.ColumnId)
		if _, exist := keyMap[key]; exist {
			return false
		}
		keyMap[key] = struct{}{}
	}
	return true
}

// x谓词过滤：1.同表同列冲多余谓词，如t0.a!=1 ^ t0.a=2
func lhsPredicateFilter(predicates []rds.Predicate, lhsList [][]interface{}, sortedLhs []string) ([]rds.Predicate, [][]interface{}, []string) {
	removeFlag := make([]bool, len(predicates))
	for i := 0; i < len(predicates); i++ {
		if predicates[i].PredicateType != 0 || predicates[i].SymbolType != rds_config.Equal {
			continue
		}
		columnId := predicates[i].LeftColumn.ColumnId
		for j := 0; j < len(predicates); j++ {
			if i == j {
				continue
			}
			if predicates[j].PredicateType == 0 && predicates[j].LeftColumn.ColumnId == columnId && predicates[j].SymbolType != rds_config.Equal {
				removeFlag[j] = true
			}
		}
	}

	filterPredicates := make([]rds.Predicate, 0)
	filterLhsList := make([][]interface{}, 0)
	filterSortedLhs := make([]string, 0)

	for i, remove := range removeFlag {
		if !remove {
			filterPredicates = append(filterPredicates, predicates[i])
			filterLhsList = append(filterLhsList, lhsList[i])
			filterSortedLhs = append(filterSortedLhs, sortedLhs[i])
		}
	}
	return filterPredicates, filterLhsList, filterSortedLhs
}
