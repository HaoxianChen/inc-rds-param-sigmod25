package intersection

import (
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables/table_data"
	"gitlab.grandhoo.com/rock/rock_v3/rds_config"
	"gitlab.grandhoo.com/rock/rock_v3/utils"
	"gitlab.grandhoo.com/rock/rock_v3/utils/blocking/udf_column"
	"gitlab.grandhoo.com/rock/rock_v3/utils/list_cluster"
	"gitlab.grandhoo.com/rock/rock_v3/utils/mapi32"
	"gitlab.grandhoo.com/rock/rock_v3/utils/storage_utils"
	utils2 "gitlab.grandhoo.com/rock/storage/storage2/utils"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
)

var CalRuleChan = make(chan int, 2)

func GeneratePredicateIntersection(predicate rds.Predicate, taskId int64) [][][]int32 {
	var intersection [][][]int32
	leftTableId := predicate.LeftColumn.TableId
	rightTableId := predicate.RightColumn.TableId
	leftColumnName := predicate.LeftColumn.ColumnId
	rightColumnName := predicate.RightColumn.ColumnId
	leftTableIndex, rightTableIndex := utils.GetPredicateColumnIndexNew(predicate)
	maxTableIndex := rightTableIndex
	if leftTableIndex > rightTableIndex {
		maxTableIndex = leftTableIndex
	}
	task := table_data.GetTask(taskId)
	var leftPli, rightPli = task.IndexPLI[leftTableId][leftColumnName], task.IndexPLI[rightTableId][rightColumnName]
	isSameColumn := leftTableId == rightTableId && leftColumnName == rightColumnName
	for value, leftIds := range leftPli {
		idPairs := make([][]int32, maxTableIndex+1)
		if isSameColumn {
			idPairs[leftTableIndex] = leftIds
			idPairs[rightTableIndex] = leftIds
		} else {
			rightIds := rightPli[value]
			if len(rightIds) > 0 {
				idPairs[leftTableIndex] = leftIds
				idPairs[rightTableIndex] = rightIds
			} else {
				continue
			}
		}
		intersection = append(intersection, idPairs)
	}
	return intersection
}

func CalRule(lhs []rds.Predicate, rhs rds.Predicate, taskId int64, chanSize int) (int, int, int) {
	if chanSize < 1 {
		chanSize = 1
	}
	logger.Infof("chanSize:%v", chanSize)
	CalRuleChan <- 1
	defer func() { <-CalRuleChan }()

	task := table_data.GetTask(taskId)

	lhs = utils.SortPredicatesRelated(lhs)
	var fkPredicates []rds.Predicate
	for _, lh := range lhs {
		if storage_utils.ForeignKeyPredicate(&lh) {
			fkPredicates = append(fkPredicates, lh)
		}
	}

	var intersection [][][]int32
	var usedTableIndex []int
	for _, lh := range lhs {
		lTid, rTid := utils.GetPredicateColumnIndexNew(lh)
		usedTableIndex = append(usedTableIndex, lTid)
		usedTableIndex = append(usedTableIndex, rTid)
		intersection = CalIntersection(intersection, lh, taskId, chanSize)
		if len(intersection) < 1 {
			break
		}
	}
	logger.Infof("finish cal lhs:%v", utils.GetLhsStr(lhs))
	usedTableIndex = utils2.Distinct(usedTableIndex)
	xSupp := CalIntersectionSupport(intersection, usedTableIndex, chanSize)
	if xSupp == 0 {
		return 0, 0, 0
	}

	intersection = CalIntersection(intersection, rhs, taskId, chanSize)
	logger.Infof("finish cal rule:%v->%v", utils.GetLhsStr(lhs), rhs.PredicateStr)
	xySupp := CalIntersectionSupport(intersection, usedTableIndex, chanSize)

	var rowSize int
	if len(fkPredicates) < 1 {
		tableIndex2tableId := GetTableIndex2tableId(lhs)
		rowSize = 1
		for _, tableId := range tableIndex2tableId {
			rowSize *= task.TableRowSize[tableId]
		}
	} else {
		logger.Infof("multi table rule, cal rowSize")
		if len(fkPredicates) == len(lhs) {
			rowSize = xSupp
		} else {
			uniqueFp, uniqueOk := storage_utils.ForeignKeyPredicatesUnique(fkPredicates)
			if uniqueOk {
				fkPredicates = uniqueFp
			}
			var usedTableIndex []int
			var fkIntersection [][][]int32
			for _, p := range fkPredicates {
				fkIntersection = CalIntersection(fkIntersection, p, taskId, chanSize)
				if len(fkIntersection) < 1 {
					break
				}
				lTid, rTid := utils.GetPredicateColumnIndexNew(p)
				usedTableIndex = append(usedTableIndex, lTid)
				usedTableIndex = append(usedTableIndex, rTid)
			}
			usedTableIndex = utils2.Distinct(usedTableIndex)
			rowSize = CalIntersectionSupport(fkIntersection, usedTableIndex, chanSize)
			if uniqueOk {
				rowSize *= rowSize
			}
		}
		logger.Infof("finish cal multi table rule rowSize:%v", utils.GetLhsStr(fkPredicates))
	}

	return rowSize, xSupp, xySupp
}

func CalRuleNew(lhs []rds.Predicate, rhs rds.Predicate, taskId int64, chanSize int) (int, int, int) {
	if chanSize < 1 {
		chanSize = 1
	}
	logger.Infof("chanSize:%v", chanSize)
	CalRuleChan <- 1
	defer func() { <-CalRuleChan }()

	task := table_data.GetTask(taskId)

	lhs = utils.SortPredicatesRelated(lhs)
	var fkPredicates []rds.Predicate

	var xSupp, xySupp, rowSize int
	var usedTableIndex []int
	for _, p := range lhs {
		if storage_utils.ForeignKeyPredicate(&p) {
			fkPredicates = append(fkPredicates, p)
		}
		lTid, rTid := utils.GetPredicateColumnIndexNew(p)
		usedTableIndex = append(usedTableIndex, lTid)
		usedTableIndex = append(usedTableIndex, rTid)
	}
	usedTableIndex = utils2.Distinct(usedTableIndex)

	//var intersection = CalIntersection(nil, lhs[0], taskId, chanSize)
	//logger.Infof("predicate:%v的交集结果共有%v个pair", lhs[0].PredicateStr, len(intersection))
	//for i, eachIntersection := range intersection {
	//	if i%1000000 == 0 {
	//		logger.Infof("predicate:%v已计算%v个pair", lhs[0].PredicateStr, i)
	//	}
	//	tmpIntersection := [][][]int32{eachIntersection}
	//	if len(lhs) < 2 {
	//		xSupp += CalIntersectionSupport(tmpIntersection, usedTableIndex, chanSize)
	//	} else {
	//		for _, lh := range lhs[1:] {
	//			tmpIntersection = CalIntersection(tmpIntersection, lh, taskId, chanSize)
	//			if len(tmpIntersection) < 1 {
	//				break
	//			}
	//		}
	//		xSupp += CalIntersectionSupport(tmpIntersection, usedTableIndex, chanSize)
	//	}
	//	if len(tmpIntersection) < 1 {
	//		continue
	//	}
	//	tmpIntersection = CalIntersection(tmpIntersection, rhs, taskId, chanSize)
	//	xySupp += CalIntersectionSupport(tmpIntersection, usedTableIndex, chanSize)
	//}

	type pair struct {
		intersection [][][]int32
		pid          int
	}
	predicates := make([]rds.Predicate, len(lhs)+1)
	copy(predicates, lhs)
	predicates[len(lhs)] = rhs
	var stack []pair
	stack = append(stack, pair{nil, 0})

	for len(stack) > 0 {
		one := stack[len(stack)-1]
		stack = stack[:len(stack)-1] // pop
		var tmp = one.intersection
		//if len(one.idPairs) > 0 {
		//	tmp = append(tmp, one.idPairs)
		//}
		if one.pid == len(predicates) {
			xySupp += CalIntersectionSupport(tmp, usedTableIndex, chanSize)
		} else {
			if one.pid == len(predicates)-1 {
				xSupp += CalIntersectionSupport(tmp, usedTableIndex, chanSize)
			}
			many := CalIntersection(tmp, predicates[one.pid], taskId, chanSize)
			batchSize := 2 * chanSize
			for i, j := 0, batchSize; i < len(many); j += batchSize {
				if j > len(many) {
					j = len(many)
				}
				stack = append(stack, pair{many[i:j], one.pid + 1})
				i = j
			}
			//for _, each := range many {
			//	stack = append(stack, pair{each, one.pid + 1})
			//}
		}
	}

	//type idPairs struct {
	//	idPairs [][]int32
	//	pid     int
	//}
	//var (
	//	idPairStack   = []idPairs{{idPairs: nil, pid: 0}} // 栈
	//	idPairStackMu sync.Mutex                          // 互斥锁，保证对stack的安全访问
	//
	//	waitingNumber   int // 等待人数
	//	waitingNumberMu sync.Mutex
	//	finish          = false // 是否全部算完,也就是都在等待
	//	finishMu        sync.RWMutex
	//	cond            = sync.NewCond(&finishMu) // 等待 or 唤醒
	//	wg              sync.WaitGroup
	//
	//	xSuppSlice  = make([]CacheValue[int], chanSize)
	//	xySuppSlice = make([]CacheValue[int], chanSize)
	//)
	//
	//for i := 0; i < chanSize; i++ {
	//	wg.Add(1)
	//	go func(i int) {
	//		defer wg.Done()
	//		for {
	//			// finish
	//			finishMu.RLock()
	//			if finish {
	//				cond.Broadcast()
	//				logger.Info("[并发RD]看到finish退出", i, utils.GetLhsStr(lhs))
	//				return
	//			}
	//			finishMu.RUnlock()
	//			// 取
	//			var p idPairs
	//			var shouldWait = false
	//			idPairStackMu.Lock()
	//			if len(idPairStack) == 0 {
	//				shouldWait = true
	//			} else {
	//				p = idPairStack[len(idPairStack)-1]
	//				idPairStack = idPairStack[:len(idPairStack)-1]
	//			}
	//			idPairStackMu.Unlock()
	//
	//			if shouldWait {
	//				waitingNumberMu.Lock()
	//				waitingNumber++
	//				var curWaitingNumber = waitingNumber
	//				waitingNumberMu.Unlock()
	//				if curWaitingNumber == chanSize {
	//					finishMu.Lock()
	//					finish = true
	//					finishMu.Unlock()
	//					cond.Broadcast()
	//					logger.Info("[并发RD]%d所有人都在等待,退出", i, utils.GetLhsStr(lhs))
	//					return
	//				} else {
	//					logger.Info("[并发RD]%d没有任务,等待", i, utils.GetLhsStr(lhs))
	//					cond.L.Lock()
	//					if finish {
	//						cond.L.Unlock()
	//						return
	//					}
	//					cond.Wait()
	//					cond.L.Unlock()
	//					logger.Info("[并发RD]%d被唤醒", i, utils.GetLhsStr(lhs))
	//					waitingNumberMu.Lock()
	//					waitingNumber--
	//					waitingNumberMu.Unlock()
	//				}
	//			} else {
	//				var tmp [][][]int32
	//				if len(p.idPairs) > 0 {
	//					tmp = append(tmp, p.idPairs)
	//				}
	//
	//				if p.pid == len(predicates) {
	//					xySuppT := CalIntersectionSupport(tmp, usedTableIndex, chanSize)
	//					xySuppSlice[i].Val += xySuppT
	//				} else {
	//					if p.pid == len(predicates)-1 {
	//						xSuppT := CalIntersectionSupport(tmp, usedTableIndex, chanSize)
	//						xSuppSlice[i].Val += xSuppT
	//					}
	//					many := CalIntersection(tmp, predicates[p.pid], taskId, chanSize)
	//					if len(many) > 0 {
	//						idPairStackMu.Lock()
	//						for _, t := range many {
	//							idPairStack = append(idPairStack, idPairs{t, p.pid + 1})
	//						}
	//						idPairStackMu.Unlock()
	//						cond.Broadcast()
	//					}
	//				}
	//			}
	//		}
	//	}(i)
	//}
	//wg.Wait()
	//for _, s := range xSuppSlice {
	//	xSupp += s.Val
	//}
	//for _, s := range xySuppSlice {
	//	xySupp += s.Val
	//}

	logger.Infof("finish cal rule:%v->%v", utils.GetLhsStr(lhs), rhs.PredicateStr)
	if len(fkPredicates) < 1 {
		tableIndex2tableId := GetTableIndex2tableId(lhs)
		rowSize = 1
		for _, tableId := range tableIndex2tableId {
			rowSize *= task.TableRowSize[tableId]
		}
	} else {
		logger.Infof("multi table rule, cal rowSize")
		if len(fkPredicates) == len(lhs) {
			rowSize = xSupp
		} else {
			uniqueFp, uniqueOk := storage_utils.ForeignKeyPredicatesUnique(fkPredicates)
			if uniqueOk {
				fkPredicates = uniqueFp
			}
			var fkUsedTableIndex []int
			for _, p := range fkPredicates {
				lTid, rTid := utils.GetPredicateColumnIndexNew(p)
				fkUsedTableIndex = append(fkUsedTableIndex, lTid)
				fkUsedTableIndex = append(fkUsedTableIndex, rTid)
			}
			fkUsedTableIndex = utils2.Distinct(fkUsedTableIndex)
			var fkIntersection = CalIntersection(nil, fkPredicates[0], taskId, chanSize)

			for _, each := range fkIntersection {
				tmpFkIntersection := [][][]int32{each}
				for _, p := range fkPredicates[1:] {
					tmpFkIntersection = CalIntersection(tmpFkIntersection, p, taskId, chanSize)
					if len(tmpFkIntersection) < 1 {
						break
					}
				}
				rowSize += CalIntersectionSupport(tmpFkIntersection, fkUsedTableIndex, chanSize)
			}

			if uniqueOk {
				rowSize *= rowSize
			}
		}
		logger.Infof("finish cal multi table rule rowSize:%v", utils.GetLhsStr(fkPredicates))
	}

	return rowSize, xSupp, xySupp
}

func CalRuleNew2(lhs []rds.Predicate, rhs rds.Predicate, taskId int64, chanSize int) (int, int, int) {
	if chanSize < 1 {
		chanSize = 1
	}
	logger.Infof("chanSize:%v", chanSize)
	CalRuleChan <- 1
	defer func() { <-CalRuleChan }()

	task := table_data.GetTask(taskId)

	var innerPredicates []rds.Predicate
	var fkPredicates []rds.Predicate

	var xSupp, xySupp, rowSize int
	var usedTableIndex []int
	for _, p := range lhs {
		if storage_utils.ForeignKeyPredicate(&p) {
			fkPredicates = append(fkPredicates, p)
		}
		if p.PredicateType == 1 {
			innerPredicates = append(innerPredicates, p)
		}
		lTid, rTid := utils.GetPredicateColumnIndexNew(p)
		usedTableIndex = append(usedTableIndex, lTid)
		usedTableIndex = append(usedTableIndex, rTid)
	}
	usedTableIndex = utils2.Distinct(usedTableIndex)

	if len(fkPredicates) > 0 && len(innerPredicates) > 0 {
		// 多表
		logger.Infof("***** start cal multi table rule:%v->%v", utils.GetLhsStr(lhs), rhs.PredicateStr)
		rowSize, xSupp, xySupp = NewHashJoin().CalcRule(taskId, lhs, rhs, chanSize)
	} else {
		// 单表 或 多表单行
		sort.Slice(lhs, func(i, j int) bool {
			return lhs[i].PredicateStr < lhs[j].PredicateStr
		})
		lhs = utils.SortPredicatesRelated(lhs)
		predicates := make([]rds.Predicate, len(lhs)+1)
		copy(predicates, lhs)
		predicates[len(lhs)] = rhs

		var (
			waitCond       = sync.NewCond(&sync.Mutex{}) // 条件变量
			mainCond       = sync.NewCond(&sync.Mutex{}) // 主协程等待一个协程结束
			mainShouldWait = true                        // 主协程是否应该等待
			runningNumber  atomic.Int32                  // 正在往下搜索的协程数目。如果数目为0且栈为空，则说明搜索结束
			stack          = newStack(chanSize)

			xSuppSlice  = make([]CacheValue[int], chanSize)
			xySuppSlice = make([]CacheValue[int], chanSize)
		)
		stack.push(IdPairsTask{
			idPairs: nil,
			pid:     0,
		})
		for i := 0; i < chanSize; i++ {
			go func(i int) {
				//var mapData []mapi32.Entry[int32]
				//lists := list_cluster.NewCapacity[int32](1024)
			startRunning:
				runningNumber.Add(1)
			takeLoop:
				one, ok := stack.pop(i) // 出栈一个
				if ok {
					//many := CalIntersectionNew(one.idPairs, predicates[one.pid], taskId, &mapData, lists)
					var tmp [][][]int32
					if len(one.idPairs) > 0 {
						tmp = append(tmp, one.idPairs)
					}
					many := CalIntersection(tmp, predicates[one.pid], taskId, chanSize)
					if one.pid == len(predicates)-1 {
						xSuppSlice[i].Val += CalIntersectionSupport(tmp, usedTableIndex, chanSize)
						xySuppSlice[i].Val += CalIntersectionSupport(many, usedTableIndex, chanSize)
					} else {
						var idPairsTasks = make([]IdPairsTask, len(many))
						for k := range many {
							idPairsTasks[k].idPairs = many[k]
							idPairsTasks[k].pid = one.pid + 1
						}
						stack.push(idPairsTasks...)
					}

					waitCond.Broadcast() // 唤醒所有节点
					goto takeLoop
				} else {
					waitCond.L.Lock()
					if runningNumber.Add(-1) == 0 && stack.empty() {
						waitCond.L.Unlock()
						waitCond.Signal() // 唤醒一个子协程

						mainCond.L.Lock() // 唤醒主协程
						if mainShouldWait {
							mainShouldWait = false
						}
						mainCond.Signal()
						mainCond.L.Unlock()

						return
					}
					waitCond.Wait()
					waitCond.L.Unlock()
					goto startRunning
				}
			}(i)
		}

		mainCond.L.Lock()
		if mainShouldWait {
			mainCond.Wait()
		}
		mainCond.L.Unlock()
		for _, s := range xSuppSlice {
			xSupp += s.Val
		}
		for _, s := range xySuppSlice {
			xySupp += s.Val
		}

		logger.Infof("finish cal rule:%v->%v", utils.GetLhsStr(lhs), rhs.PredicateStr)
		if len(fkPredicates) < 1 {
			tableIndex2tableId := GetTableIndex2tableId(lhs)
			rowSize = 1
			for _, tableId := range tableIndex2tableId {
				rowSize *= task.TableRowSize[tableId]
			}
		} else {
			logger.Infof("multi table rule, cal rowSize")
			if len(fkPredicates) == len(lhs) {
				rowSize = xSupp
			} else {
				uniqueFp, uniqueOk := storage_utils.ForeignKeyPredicatesUnique(fkPredicates)
				if uniqueOk {
					fkPredicates = uniqueFp
				}
				var fkUsedTableIndex []int
				for _, p := range fkPredicates {
					lTid, rTid := utils.GetPredicateColumnIndexNew(p)
					fkUsedTableIndex = append(fkUsedTableIndex, lTid)
					fkUsedTableIndex = append(fkUsedTableIndex, rTid)
				}
				fkUsedTableIndex = utils2.Distinct(fkUsedTableIndex)
				var fkIntersection = CalIntersection(nil, fkPredicates[0], taskId, chanSize)

				for _, each := range fkIntersection {
					tmpFkIntersection := [][][]int32{each}
					for _, p := range fkPredicates[1:] {
						tmpFkIntersection = CalIntersection(tmpFkIntersection, p, taskId, chanSize)
						if len(tmpFkIntersection) < 1 {
							break
						}
					}
					rowSize += CalIntersectionSupport(tmpFkIntersection, fkUsedTableIndex, chanSize)
				}

				if uniqueOk {
					rowSize *= rowSize
				}
			}
			logger.Infof("finish cal multi table rule rowSize:%v", utils.GetLhsStr(fkPredicates))
		}
	}

	return rowSize, xSupp, xySupp
}

func CalSupp(lhs []rds.Predicate, rhs rds.Predicate, taskId int64, chanSize int, valueIndex interface{}) (int, int) {
	if chanSize < 1 {
		chanSize = 1
	}
	//logger.Infof("chanSize:%v", chanSize)
	CalRuleChan <- 1
	defer func() { <-CalRuleChan }()

	task := table_data.GetTask(taskId)

	var usedTableIndex []int
	for _, p := range lhs {
		lTid, rTid := utils.GetPredicateColumnIndexNew(p)
		usedTableIndex = append(usedTableIndex, lTid)
		usedTableIndex = append(usedTableIndex, rTid)
	}
	usedTableIndex = utils2.Distinct(usedTableIndex)

	var intersection [][][]int32

	leftTableIndex, rightTableIndex := utils.GetPredicateColumnIndexNew(lhs[0])
	idPairs := make([][]int32, rightTableIndex+1)
	rows := task.PLI[lhs[0].LeftColumn.TableId][lhs[0].LeftColumn.ColumnId][valueIndex]
	idPairs[leftTableIndex] = rows
	idPairs[rightTableIndex] = rows
	intersection = append(intersection, idPairs)

	for i := 1; i < len(lhs); i++ {
		intersection = CalIntersection(intersection, lhs[i], taskId, chanSize)
	}

	xSupp := CalIntersectionSupport(intersection, usedTableIndex, chanSize)
	if xSupp == 0 {
		return 0, 0
	}

	intersection = CalIntersection(intersection, rhs, taskId, chanSize)
	//logger.Infof("finish cal rule:%v->%v", utils.GetLhsStr(lhs), rhs.PredicateStr)
	xySupp := CalIntersectionSupport(intersection, usedTableIndex, chanSize)

	return xSupp, xySupp
}

// SampleIntersection XY 训练抽样
// 第二个结果是表ID到表名，例如 t0/t1->shop t2/t3->product，[shop, shop, product, product]
func SampleIntersection(predicates []rds.Predicate, taskId int64, intersectionSize int, chanSize int, rhs rds.Predicate) (intersection [][][]int32, tableIds []string) {
	sort.Slice(predicates, func(i, j int) bool {
		return predicates[i].PredicateStr < predicates[j].PredicateStr
	})
	predicates = utils.SortPredicatesRelated(predicates)
	tableIdMap := map[int]string{}
	maxTid := 0
	var fkPredicate []rds.Predicate
	var innerPredicate []rds.Predicate
	for _, p := range predicates {
		if storage_utils.ForeignKeyPredicate(&p) {
			fkPredicate = append(fkPredicate, p)
		}
		if p.PredicateType == 1 {
			innerPredicate = append(innerPredicate, p)
		}
		leftTableId := p.LeftColumn.TableId
		rightTableId := p.RightColumn.TableId
		lTid, rTid := utils.GetPredicateColumnIndexNew(p)
		tableIdMap[lTid] = leftTableId
		tableIdMap[rTid] = rightTableId
		maxTid = utils2.Max(maxTid, lTid)
		maxTid = utils2.Max(maxTid, rTid)
	}
	tableIds = make([]string, maxTid+1)
	for tid, table := range tableIdMap {
		tableIds[tid] = table
	}
	if len(fkPredicate) > 0 && len(innerPredicate) > 0 {
		logger.Infof("Multi table SampleIntersection...")
		intersection = NewHashJoin().CalcIntersection(taskId, predicates, rhs, chanSize)
	} else {
		// 单表 或 多表单行
		type pair struct {
			idPairs [][]int32
			pid     int
		}
		var stack []pair
		stack = append(stack, pair{nil, 0})

		for len(stack) > 0 {
			one := stack[len(stack)-1]
			stack = stack[:len(stack)-1] // pop
			if one.pid == len(predicates) {
				intersection = append(intersection, one.idPairs)
				//if len(intersection) >= intersectionSize {
				//	return intersection, tableIds
				//}
			} else {
				var tmp [][][]int32
				if len(one.idPairs) > 0 {
					tmp = append(tmp, one.idPairs)
				}
				many := CalIntersection(tmp, predicates[one.pid], taskId, chanSize)
				for _, each := range many {
					stack = append(stack, pair{each, one.pid + 1})
				}
			}
		}
	}

	return intersection, tableIds
}

func SampleIntersectionNew(predicates []rds.Predicate, taskId int64, intersectionSize int, chanSize int, rhs rds.Predicate) (intersection [][][]int32, tableIds []string) {
	sort.Slice(predicates, func(i, j int) bool {
		return predicates[i].PredicateStr < predicates[j].PredicateStr
	})
	predicates = utils.SortPredicatesRelated(predicates)
	tableIdMap := map[int]string{}
	maxTid := 0
	var fkPredicate []rds.Predicate
	var innerPredicate []rds.Predicate
	for _, p := range predicates {
		if storage_utils.ForeignKeyPredicate(&p) {
			fkPredicate = append(fkPredicate, p)
		}
		if p.PredicateType == 1 {
			innerPredicate = append(innerPredicate, p)
		}
		leftTableId := p.LeftColumn.TableId
		rightTableId := p.RightColumn.TableId
		lTid, rTid := utils.GetPredicateColumnIndexNew(p)
		tableIdMap[lTid] = leftTableId
		tableIdMap[rTid] = rightTableId
		maxTid = utils2.Max(maxTid, lTid)
		maxTid = utils2.Max(maxTid, rTid)
	}
	tableIds = make([]string, maxTid+1)
	for tid, table := range tableIdMap {
		tableIds[tid] = table
	}
	if len(fkPredicate) > 0 && len(innerPredicate) > 0 {
		logger.Infof("Multi table SampleIntersection...")
		intersection = NewHashJoin().CalcIntersection(taskId, predicates, rhs, chanSize)
	} else {
		// 单表 或 多表单行
		type pair struct {
			idPairs [][]int32
			pid     int
		}
		var stack []pair
		stack = append(stack, pair{nil, 0})

		for len(stack) > 0 {
			one := stack[len(stack)-1]
			stack = stack[:len(stack)-1] // pop
			if one.pid == len(predicates) {
				intersection = append(intersection, one.idPairs)
				//if len(intersection) >= intersectionSize {
				//	return intersection, tableIds
				//}
			} else {
				var tmp [][][]int32
				if len(one.idPairs) > 0 {
					tmp = append(tmp, one.idPairs)
				}
				many := CalIntersectionSample(tmp, predicates[one.pid], taskId, chanSize)
				for _, each := range many {
					stack = append(stack, pair{each, one.pid + 1})
				}
			}
		}
	}

	return intersection, tableIds
}

// BuildIdTableFromIntersection tableIds 就是 SampleIntersection 第二个返回值
func BuildIdTableFromIntersection(intersection [][][]int32, tableIds []string, taskId int64, isSingle bool) (tables []map[string]map[string][]interface{}) {
	task := table_data.GetTask(taskId)
	for _, idPairs := range intersection {
		var onePiece = map[string]map[string][]interface{}{}
		for tid, rowIds := range idPairs {
			if len(rowIds) == 0 {
				break
			}
			if len(rowIds) < 2 && isSingle {
				break
			}
			tableName := tableIds[tid]
			allColumnValues := task.TableValues[tableName]
			var columnValues = map[string][]interface{}{}
			for _, rowId := range rowIds {
				for columnName, values := range allColumnValues {
					if udf_column.IsUdfColumn(columnName) {
						continue
					}
					columnValues[columnName] = append(columnValues[columnName], values[rowId])
				}
			}
			onePiece["t"+strconv.Itoa(tid)] = columnValues // 这是一个组的一张TID表
		}
		if len(onePiece) < 1 {
			continue
		}
		tables = append(tables, onePiece)
	}
	return
}

func GetTableIndex2tableId(lhs []rds.Predicate) map[int]string {
	r := map[int]string{}
	for _, p := range lhs {
		lid, rid := utils.GetPredicateColumnIndexNew(p)
		r[lid] = p.LeftColumn.TableId
		r[rid] = p.RightColumn.TableId
	}
	return r
}

func CalIntersection(intersection [][][]int32, predicate rds.Predicate, taskId int64, chanSize int) [][][]int32 {
	//logger.Infof("cal intersection:%v", predicate.PredicateStr)
	if len(intersection) < 1 {
		return GeneratePredicateIntersection(predicate, taskId)
	}
	leftTableId := predicate.LeftColumn.TableId
	rightTableId := predicate.RightColumn.TableId
	leftColumnName := predicate.LeftColumn.ColumnId
	rightColumnName := predicate.RightColumn.ColumnId
	leftTableIndex, rightTableIndex := utils.GetPredicateColumnIndexNew(predicate)

	task := table_data.GetTask(taskId)
	leftValues := task.TableIndexValues[leftTableId][leftColumnName]
	rightValues := task.TableIndexValues[rightTableId][rightColumnName]
	var leftPli, rightPli = task.IndexPLI[leftTableId][leftColumnName], task.IndexPLI[rightTableId][rightColumnName]

	var newIdPairSize = len(intersection[0])
	newIdPairSize = utils2.Max(newIdPairSize, leftTableIndex+1)
	newIdPairSize = utils2.Max(newIdPairSize, rightTableIndex+1)

	var leftPad = leftTableIndex >= len(intersection[0]) || len(intersection[0][leftTableIndex]) < 1
	var rightPad = rightTableIndex >= len(intersection[0]) || len(intersection[0][rightTableIndex]) < 1

	var noLimitIndexes []int
	for tableIndex := range intersection[0] {
		if tableIndex != leftTableIndex && tableIndex != rightTableIndex {
			noLimitIndexes = append(noLimitIndexes, tableIndex)
		}
	}

	var parallelIntersection = make([]CacheValue[[][][]int32], chanSize)
	_ = storage_utils.ParallelBatch(0, len(intersection), chanSize, func(start, end, workerId int) error {
		for _, idPairs := range intersection[start:end] {
			var tempIdPairs [][][]int32
			value2Index := make(map[int32]int)
			for tableIndex, ids := range idPairs {
				if tableIndex == leftTableIndex || tableIndex == rightTableIndex {
					for _, rowId := range ids {
						var value int32
						if tableIndex == leftTableIndex {
							value = leftValues[rowId]
						} else {
							value = rightValues[rowId]
						}
						if value == rds_config.NilIndex {
							continue
						}
						index, ok := value2Index[value]
						if !ok {
							var pad []int32
							if rightPad && tableIndex == leftTableIndex {
								pad = rightPli[value]
								if len(pad) == 0 {
									continue
								}
							}
							if leftPad && tableIndex == rightTableIndex {
								pad = leftPli[value]
								if len(pad) == 0 {
									continue
								}
							}
							value2Index[value] = len(tempIdPairs)
							index = len(tempIdPairs)
							tempIdPairs = append(tempIdPairs, make([][]int32, newIdPairSize))
							if leftPad {
								tempIdPairs[index][leftTableIndex] = pad
							}
							if rightPad {
								tempIdPairs[index][rightTableIndex] = pad
							}
						}
						tempIdPairs[index][tableIndex] = append(tempIdPairs[index][tableIndex], rowId)
					}
				}
			}

			for k := range tempIdPairs {
				flag := false
				for tableIndex, rowIds := range tempIdPairs[k] {
					if tableIndex == leftTableIndex || tableIndex == rightTableIndex {
						if len(rowIds) < 1 {
							flag = true
							continue
						}
					}
				}
				if flag {
					continue
				}
				for _, tableIndex := range noLimitIndexes {
					tempIdPairs[k][tableIndex] = idPairs[tableIndex]
				}
				parallelIntersection[workerId].Val = append(parallelIntersection[workerId].Val, tempIdPairs[k])
			}
		}
		return nil
	})

	var resultIntersection [][][]int32
	for _, tempIntersection := range parallelIntersection {
		resultIntersection = append(resultIntersection, tempIntersection.Val...)
	}
	return resultIntersection
}

func CalIntersectionSample(intersection [][][]int32, predicate rds.Predicate, taskId int64, chanSize int) [][][]int32 {
	//logger.Infof("cal intersection:%v", predicate.PredicateStr)
	if len(intersection) < 1 {
		return GeneratePredicateIntersectionSample(predicate, taskId)
	}
	leftTableId := predicate.LeftColumn.TableId
	rightTableId := predicate.RightColumn.TableId
	leftColumnName := predicate.LeftColumn.ColumnId
	rightColumnName := predicate.RightColumn.ColumnId
	leftTableIndex, rightTableIndex := utils.GetPredicateColumnIndexNew(predicate)

	task := table_data.GetTask(taskId)
	leftValues := task.SampleTableIndexValues[leftTableId][leftColumnName]
	rightValues := task.SampleTableIndexValues[rightTableId][rightColumnName]
	var leftPli, rightPli = task.SampleIndexPLI[leftTableId][leftColumnName], task.SampleIndexPLI[rightTableId][rightColumnName]

	var newIdPairSize = len(intersection[0])
	newIdPairSize = utils2.Max(newIdPairSize, leftTableIndex+1)
	newIdPairSize = utils2.Max(newIdPairSize, rightTableIndex+1)

	var leftPad = leftTableIndex >= len(intersection[0]) || len(intersection[0][leftTableIndex]) < 1
	var rightPad = rightTableIndex >= len(intersection[0]) || len(intersection[0][rightTableIndex]) < 1

	var noLimitIndexes []int
	for tableIndex := range intersection[0] {
		if tableIndex != leftTableIndex && tableIndex != rightTableIndex {
			noLimitIndexes = append(noLimitIndexes, tableIndex)
		}
	}

	var parallelIntersection = make([]CacheValue[[][][]int32], chanSize)
	_ = storage_utils.ParallelBatch(0, len(intersection), chanSize, func(start, end, workerId int) error {
		for _, idPairs := range intersection[start:end] {
			var tempIdPairs [][][]int32
			value2Index := make(map[int32]int)
			for tableIndex, ids := range idPairs {
				if tableIndex == leftTableIndex || tableIndex == rightTableIndex {
					for _, rowId := range ids {
						var value int32
						if tableIndex == leftTableIndex {
							value = leftValues[rowId]
						} else {
							value = rightValues[rowId]
						}
						if value == rds_config.NilIndex {
							continue
						}
						index, ok := value2Index[value]
						if !ok {
							var pad []int32
							if rightPad && tableIndex == leftTableIndex {
								pad = rightPli[value]
								if len(pad) == 0 {
									continue
								}
							}
							if leftPad && tableIndex == rightTableIndex {
								pad = leftPli[value]
								if len(pad) == 0 {
									continue
								}
							}
							value2Index[value] = len(tempIdPairs)
							index = len(tempIdPairs)
							tempIdPairs = append(tempIdPairs, make([][]int32, newIdPairSize))
							if leftPad {
								tempIdPairs[index][leftTableIndex] = pad
							}
							if rightPad {
								tempIdPairs[index][rightTableIndex] = pad
							}
						}
						tempIdPairs[index][tableIndex] = append(tempIdPairs[index][tableIndex], rowId)
					}
				}
			}

			for k := range tempIdPairs {
				flag := false
				for tableIndex, rowIds := range tempIdPairs[k] {
					if tableIndex == leftTableIndex || tableIndex == rightTableIndex {
						if len(rowIds) < 1 {
							flag = true
							continue
						}
					}
				}
				if flag {
					continue
				}
				for _, tableIndex := range noLimitIndexes {
					tempIdPairs[k][tableIndex] = idPairs[tableIndex]
				}
				parallelIntersection[workerId].Val = append(parallelIntersection[workerId].Val, tempIdPairs[k])
			}
		}
		return nil
	})

	var resultIntersection [][][]int32
	for _, tempIntersection := range parallelIntersection {
		resultIntersection = append(resultIntersection, tempIntersection.Val...)
	}
	return resultIntersection
}

func GeneratePredicateIntersectionSample(predicate rds.Predicate, taskId int64) [][][]int32 {
	var intersection [][][]int32
	leftTableId := predicate.LeftColumn.TableId
	rightTableId := predicate.RightColumn.TableId
	leftColumnName := predicate.LeftColumn.ColumnId
	rightColumnName := predicate.RightColumn.ColumnId
	leftTableIndex, rightTableIndex := utils.GetPredicateColumnIndexNew(predicate)
	maxTableIndex := rightTableIndex
	if leftTableIndex > rightTableIndex {
		maxTableIndex = leftTableIndex
	}
	task := table_data.GetTask(taskId)
	var leftPli, rightPli = task.SampleIndexPLI[leftTableId][leftColumnName], task.SampleIndexPLI[rightTableId][rightColumnName]
	isSameColumn := leftTableId == rightTableId && leftColumnName == rightColumnName
	for value, leftIds := range leftPli {
		idPairs := make([][]int32, maxTableIndex+1)
		if isSameColumn {
			idPairs[leftTableIndex] = leftIds
			idPairs[rightTableIndex] = leftIds
		} else {
			rightIds := rightPli[value]
			if len(rightIds) > 0 {
				idPairs[leftTableIndex] = leftIds
				idPairs[rightTableIndex] = rightIds
			} else {
				continue
			}
		}
		intersection = append(intersection, idPairs)
	}
	return intersection
}

func CalIntersectionNew(idPairs [][]int32, predicate rds.Predicate, taskId int64, mapData *[]mapi32.Entry[int32], lists *list_cluster.LiCluster[int32]) [][][]int32 {
	//logger.Infof("cal intersection:%v", predicate.PredicateStr)
	if len(idPairs) < 1 {
		return GeneratePredicateIntersection(predicate, taskId)
	}
	leftTableId := predicate.LeftColumn.TableId
	rightTableId := predicate.RightColumn.TableId
	leftColumnName := predicate.LeftColumn.ColumnId
	rightColumnName := predicate.RightColumn.ColumnId
	leftTableIndex, rightTableIndex := utils.GetPredicateColumnIndexNew(predicate)

	task := table_data.GetTask(taskId)
	leftValues := task.TableIndexValues[leftTableId][leftColumnName]
	rightValues := task.TableIndexValues[rightTableId][rightColumnName]
	var leftPli, rightPli = task.IndexPLI[leftTableId][leftColumnName], task.IndexPLI[rightTableId][rightColumnName]

	var newIdPairSize = len(idPairs)
	newIdPairSize = utils2.Max(newIdPairSize, leftTableIndex+1)
	newIdPairSize = utils2.Max(newIdPairSize, rightTableIndex+1)

	var leftPad = leftTableIndex >= len(idPairs) || len(idPairs[leftTableIndex]) < 1
	var rightPad = rightTableIndex >= len(idPairs) || len(idPairs[rightTableIndex]) < 1
	var padTableIndex = -1
	if leftPad {
		padTableIndex = leftTableIndex
	} else {
		padTableIndex = rightTableIndex
	}

	var noLimitIndexes []int
	for tableIndex := range idPairs {
		if tableIndex != leftTableIndex && tableIndex != rightTableIndex {
			noLimitIndexes = append(noLimitIndexes, tableIndex)
		}
	}

	var rowIdSize = len(idPairs[leftTableIndex]) + len(idPairs[rightTableIndex])
	var value2Index = mapi32.NewUseArray[int32](int32(rowIdSize), mapData)
	var tempIdPairs = make([][]list_cluster.Head, 0, rowIdSize)
	var paddings = make([][]int32, rowIdSize)
	for tableIndex, ids := range idPairs {
		if tableIndex == leftTableIndex || tableIndex == rightTableIndex {
			for _, rowId := range ids {
				var value int32
				if tableIndex == leftTableIndex {
					value = leftValues[rowId]
				} else {
					value = rightValues[rowId]
				}
				if value == rds_config.NilIndex {
					continue
				}
				index, ok := value2Index.Get2(value)
				if !ok {
					var pad []int32
					if rightPad && tableIndex == leftTableIndex {
						pad = rightPli[value]
						if len(pad) == 0 {
							continue
						}
					}
					if leftPad && tableIndex == rightTableIndex {
						pad = leftPli[value]
						if len(pad) == 0 {
							continue
						}
					}
					value2Index.DirectPut(value, int32(len(tempIdPairs)))
					index = int32(len(tempIdPairs))
					idPair := make([]list_cluster.Head, newIdPairSize)
					for i := 0; i < newIdPairSize; i++ {
						idPair[0] = list_cluster.Null
					}
					tempIdPairs = append(tempIdPairs, idPair)
					if leftPad {
						paddings = append(paddings, pad)
					}
					if rightPad {
						paddings = append(paddings, pad)
					}
				}
				if tempIdPairs[index][tableIndex] == list_cluster.Null {
					tempIdPairs[index][tableIndex] = lists.NewList(rowId)
				} else {
					lists.AppendList(tempIdPairs[index][tableIndex], rowId)
				}

			}
		}
	}

	var retIdPairs = make([][][]int32, 0, len(idPairs))
	for k := range tempIdPairs {
		flag := false
		var idPair = make([][]int32, newIdPairSize)
		for tableIndex, rowIds := range tempIdPairs[k] {
			if tableIndex == leftTableIndex || tableIndex == rightTableIndex {
				if rowIds == list_cluster.Null {
					flag = true
					break
				} else {
					idPair[tableIndex] = lists.ToSlice(rowIds)
				}
			}
			if tableIndex == padTableIndex {
				idPair[tableIndex] = paddings[k]
			}
		}
		if flag {
			continue
		}

		for _, tableIndex := range noLimitIndexes {
			idPair[tableIndex] = idPairs[tableIndex]
		}
		retIdPairs = append(retIdPairs, idPair)
	}

	lists.Clean()
	value2Index.Clean()

	return retIdPairs
}

func CalIntersectionSupport(intersection [][][]int32, useTableIndexes []int, chanSize int) int {
	if len(intersection) < 1 {
		return 0
	}
	var parallelSupp = make([]CacheValue[int], chanSize)
	_ = storage_utils.ParallelBatch(0, len(intersection), chanSize, func(batchStart, batchEnd, workerId int) error {
		for _, idPairs := range intersection[batchStart:batchEnd] {
			var temp = 1
			for _, index := range useTableIndexes {
				temp *= len(idPairs[index])
			}
			parallelSupp[workerId].Val += temp
		}
		return nil
	})

	var supp = 0
	for _, tempSupp := range parallelSupp {
		supp += tempSupp.Val
	}
	return supp
}

type CacheValue[E any] struct {
	Val E
	_   [64]byte
}

func CalRuleNew3(lhs []rds.Predicate, rhs rds.Predicate, taskId int64, chanSize int) (int, int, int, [][][]int32) {
	if chanSize < 1 {
		chanSize = 1
	}
	logger.Infof("chanSize:%v", chanSize)
	CalRuleChan <- 1
	defer func() { <-CalRuleChan }()

	task := table_data.GetTask(taskId)

	var innerPredicates []rds.Predicate
	var fkPredicates []rds.Predicate

	var xSupp, xySupp, rowSize int
	var usedTableIndex []int
	for _, p := range lhs {
		if storage_utils.ForeignKeyPredicate(&p) {
			fkPredicates = append(fkPredicates, p)
		}
		if p.PredicateType == 1 {
			innerPredicates = append(innerPredicates, p)
		}
		lTid, rTid := utils.GetPredicateColumnIndexNew(p)
		usedTableIndex = append(usedTableIndex, lTid)
		usedTableIndex = append(usedTableIndex, rTid)
	}
	usedTableIndex = utils2.Distinct(usedTableIndex)

	var xIntersection [][][]int32

	if len(fkPredicates) > 0 && len(innerPredicates) > 0 {
		// 多表
		logger.Infof("***** start cal multi table rule:%v->%v", utils.GetLhsStr(lhs), rhs.PredicateStr)
		rowSize, xSupp, xySupp, xIntersection = NewHashJoin().CalcRuleNew(taskId, lhs, rhs, chanSize)
	} else {
		// 单表 或 多表单行
		lhs = utils.SortPredicatesRelated(lhs)
		predicates := make([]rds.Predicate, len(lhs)+1)
		copy(predicates, lhs)
		predicates[len(lhs)] = rhs

		var (
			waitCond       = sync.NewCond(&sync.Mutex{}) // 条件变量
			mainCond       = sync.NewCond(&sync.Mutex{}) // 主协程等待一个协程结束
			mainShouldWait = true                        // 主协程是否应该等待
			runningNumber  atomic.Int32                  // 正在往下搜索的协程数目。如果数目为0且栈为空，则说明搜索结束
			stack          = newStack(chanSize)

			xSuppSlice      = make([]CacheValue[int], chanSize)
			xySuppSlice     = make([]CacheValue[int], chanSize)
			tmpIntersection = make([]CacheValue[[][][]int32], chanSize)
		)
		stack.push(IdPairsTask{
			idPairs: nil,
			pid:     0,
		})
		for i := 0; i < chanSize; i++ {
			go func(i int) {
				//var mapData []mapi32.Entry[int32]
				//lists := list_cluster.NewCapacity[int32](1024)
			startRunning:
				runningNumber.Add(1)
			takeLoop:
				one, ok := stack.pop(i) // 出栈一个
				if ok {
					//many := CalIntersectionNew(one.idPairs, predicates[one.pid], taskId, &mapData, lists)
					var tmp [][][]int32
					if len(one.idPairs) > 0 {
						tmp = append(tmp, one.idPairs)
					}
					many := CalIntersection(tmp, predicates[one.pid], taskId, chanSize)
					if one.pid == len(predicates)-1 {
						xSuppSlice[i].Val += CalIntersectionSupport(tmp, usedTableIndex, chanSize)
						xySuppSlice[i].Val += CalIntersectionSupport(many, usedTableIndex, chanSize)
						tmpIntersection[i].Val = append(tmpIntersection[i].Val, tmp...)
					} else {
						var idPairsTasks = make([]IdPairsTask, len(many))
						for k := range many {
							idPairsTasks[k].idPairs = many[k]
							idPairsTasks[k].pid = one.pid + 1
						}
						stack.push(idPairsTasks...)
					}

					waitCond.Broadcast() // 唤醒所有节点
					goto takeLoop
				} else {
					waitCond.L.Lock()
					if runningNumber.Add(-1) == 0 && stack.empty() {
						waitCond.L.Unlock()
						waitCond.Signal() // 唤醒一个子协程

						mainCond.L.Lock() // 唤醒主协程
						if mainShouldWait {
							mainShouldWait = false
						}
						mainCond.Signal()
						mainCond.L.Unlock()

						return
					}
					waitCond.Wait()
					waitCond.L.Unlock()
					goto startRunning
				}
			}(i)
		}

		mainCond.L.Lock()
		if mainShouldWait {
			mainCond.Wait()
		}
		mainCond.L.Unlock()
		for _, s := range xSuppSlice {
			xSupp += s.Val
		}
		for _, s := range xySuppSlice {
			xySupp += s.Val
		}
		for _, s := range tmpIntersection {
			xIntersection = append(xIntersection, s.Val...)
		}

		logger.Infof("finish cal rule:%v->%v", utils.GetLhsStr(lhs), rhs.PredicateStr)
		if len(fkPredicates) < 1 {
			tableIndex2tableId := GetTableIndex2tableId(lhs)
			rowSize = 1
			for _, tableId := range tableIndex2tableId {
				rowSize *= task.TableRowSize[tableId]
			}
		} else {
			logger.Infof("multi table rule, cal rowSize")
			if len(fkPredicates) == len(lhs) {
				rowSize = xSupp
			} else {
				uniqueFp, uniqueOk := storage_utils.ForeignKeyPredicatesUnique(fkPredicates)
				if uniqueOk {
					fkPredicates = uniqueFp
				}
				var fkUsedTableIndex []int
				for _, p := range fkPredicates {
					lTid, rTid := utils.GetPredicateColumnIndexNew(p)
					fkUsedTableIndex = append(fkUsedTableIndex, lTid)
					fkUsedTableIndex = append(fkUsedTableIndex, rTid)
				}
				fkUsedTableIndex = utils2.Distinct(fkUsedTableIndex)
				var fkIntersection = CalIntersection(nil, fkPredicates[0], taskId, chanSize)

				for _, each := range fkIntersection {
					tmpFkIntersection := [][][]int32{each}
					for _, p := range fkPredicates[1:] {
						tmpFkIntersection = CalIntersection(tmpFkIntersection, p, taskId, chanSize)
						if len(tmpFkIntersection) < 1 {
							break
						}
					}
					rowSize += CalIntersectionSupport(tmpFkIntersection, fkUsedTableIndex, chanSize)
				}

				if uniqueOk {
					rowSize *= rowSize
				}
			}
			logger.Infof("finish cal multi table rule rowSize:%v", utils.GetLhsStr(fkPredicates))
		}
	}

	return rowSize, xSupp, xySupp, xIntersection
}
