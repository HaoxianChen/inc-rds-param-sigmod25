package call

import (
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/call/gen-go/rpc"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/ml/tree"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/util/add"
)

func mergePartitionInsBasic(main, sub *rpc.PartitionInsBasic) *rpc.PartitionInsBasic {
	if main == nil {
		return sub
	}
	if sub == nil {
		return main
	}
	main.Weights += sub.Weights
	main.SingleCount += sub.SingleCount
	main.MultiCount += sub.MultiCount
	for k, v := range sub.ClassSingleCount {
		main.ClassSingleCount[k] += v
	}
	for k, v := range sub.ClassMultiCount {
		main.ClassMultiCount[k] += v
	}
	return main
}

func mergeAttrBasic(main, sub map[int32]*rpc.AttrBasic) map[int32]*rpc.AttrBasic {
	if main == nil {
		return sub
	}
	if sub == nil {
		return main
	}
	for feature, subInfo := range sub {
		if mainInfo, has := main[feature]; !has {
			main[feature] = subInfo
		} else {
			// 照理说各个属性是各个分区上都要存在的
			if mainInfo.Empty && subInfo.Empty {
				// 都只有NaN
				mainInfo.NaNWeights += subInfo.NaNWeights
			} else if mainInfo.Empty {
				subInfo.NaNWeights += mainInfo.NaNWeights
				*mainInfo = *subInfo
			} else if subInfo.Empty {
				mainInfo.NaNWeights += subInfo.NaNWeights
			} else {
				// 都有效
				mainInfo.NaNWeights += subInfo.NaNWeights
				if subInfo.Min < mainInfo.Min {
					mainInfo.Min = subInfo.Min
				}
				if subInfo.Max > mainInfo.Max {
					mainInfo.Max = subInfo.Max
				}
				for l, c := range subInfo.ValidClassWeightCount {
					mainInfo.ValidClassWeightCount[l] += c
				}
				// values的合并这里要注意一下，如果有一项为nil，表示在某机器上，数值类型取值过多，不做记录，合并之后总的也改成nil
				if len(mainInfo.Values) == 0 || len(subInfo.Values) == 0 {
					mainInfo.Values = nil
				} else {
					// 合并一下，去重、排序，用归并吧
					mainLen := len(mainInfo.Values)
					subLen := len(subInfo.Values)
					mainId, subId := 0, 0
					newValues := make([]float64, 0, mainLen+subLen)
					for mainId < mainLen && subId < subLen {
						mainV, subV := mainInfo.Values[mainId], subInfo.Values[subId]
						if mainV < subV {
							newValues = append(newValues, mainV)
							mainId++
						} else if subV < mainV {
							newValues = append(newValues, subV)
							subId++
						} else {
							newValues = append(newValues, mainV)
							mainId++
							subId++
						}
					}
					for mainId < mainLen {
						newValues = append(newValues, mainInfo.Values[mainId])
						mainId++
					}
					for subId < subLen {
						newValues = append(newValues, subInfo.Values[subId])
						subId++
					}
					mainInfo.Values = newValues
				}
			}
		}

	}

	return main
}

func mergeIntervalAVC(main, sub []*tree.AVC) []*tree.AVC {
	if main == nil {
		return sub
	}
	if sub == nil {
		return main
	}
	intervalNum := len(main) // 这里不再做校验，认为是没问题的
	mainAVC, subAVC := (*tree.AVC)(nil), (*tree.AVC)(nil)
	for i := 0; i < intervalNum; i++ {
		mainAVC, subAVC = main[i], sub[i]
		if mainAVC != nil && subAVC != nil {
			mainAVC.Merge(subAVC)
		} else if mainAVC == nil {
			main[i] = subAVC
		}
	}
	return main
}

func divideIntervals(min, max float64, intervalNum int) []float64 {
	gap := (max - min) / float64(intervalNum)
	if gap <= tree.EPSILON {
		return nil
	}
	borders := make([]float64, intervalNum+1)
	adder := add.NewFloatAdder()
	adder.Add(min)
	for i := 0; i < intervalNum; i++ {
		adder.Add(gap)
		borders[i+1] = adder.Result()
	}
	// 首尾换成正负无穷
	borders[0] = tree.NEG_INFINITY
	borders[intervalNum] = tree.INFINITY
	return borders
}

func fillStatisticInfo(statistic *tree.StatisticInfo, partitionInfo *rpc.PartitionInsBasic) {
	statistic.NodeSingleCount = int(partitionInfo.SingleCount)
	statistic.NodeMultiCount = int(partitionInfo.MultiCount)
	statistic.ClassSingleCount = make(map[float64]int, len(partitionInfo.ClassSingleCount))
	for k, v := range partitionInfo.ClassSingleCount {
		statistic.ClassSingleCount[k] = int(v)
	}
	statistic.ClassMultiCount = make(map[float64]int, len(partitionInfo.ClassMultiCount))
	for k, v := range partitionInfo.ClassMultiCount {
		statistic.ClassMultiCount[k] = int(v)
	}
	// todo:还有实例
}
