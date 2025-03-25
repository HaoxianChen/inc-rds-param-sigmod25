package inc_rule_dig

import (
	"gitlab.grandhoo.com/rock/rock-share/global/model/rds"
	"gitlab.grandhoo.com/rock/rock_v3/global_variables/task_tree"
	"unsafe"
)

func getMapDataSize(m map[string]int) uintptr {
	if m == nil {
		return 0
	}

	//// map的元数据大小
	//mapSize := uint64(unsafe.Sizeof(m))
	var mapSize uintptr

	// 估算map的元素大小
	for k, v := range m {
		// 键的内存：16字节（指针 + 长度） + 字符串数据的大小
		mapSize += unsafe.Sizeof(k) + uintptr(len(k))

		// 值的内存大小（int的大小）
		mapSize += unsafe.Sizeof(v)
	}

	// 注意：无法精确计算桶的大小
	// Go内存管理细节，桶和哈希表的实现是Go内部的优化，无法通过标准方法获得。

	return mapSize
}

func getColumnSize(column rds.Column) uintptr {
	size := unsafe.Sizeof(column)
	return size
}

func getPredicateSize(p rds.Predicate) uintptr {
	// 获取结构体本身内存大小
	pSize := unsafe.Sizeof(p)
	pSize += uintptr(len(p.PredicateStr))
	for _, column := range p.GroupByColumn {
		pSize += getColumnSize(column)
	}
	pSize += uintptr(len(p.SymbolType))
	pSize += uintptr(len(p.UDFName))
	pSize += uintptr(len(p.LeftColumnVectorFilePath))
	pSize += uintptr(len(p.RightColumnVectorFilePath))
	return pSize
}

func getPredicateDataSize(p rds.Predicate) uintptr {
	// 获取结构体本身内存大小
	pSize := uintptr(len(p.PredicateStr))
	for _, column := range p.GroupByColumn {
		pSize += getColumnSize(column)
	}
	pSize += uintptr(len(p.SymbolType))
	pSize += uintptr(len(p.UDFName))
	pSize += uintptr(len(p.LeftColumnVectorFilePath))
	pSize += uintptr(len(p.RightColumnVectorFilePath))
	return pSize
}

func getTaskTreeSize(node task_tree.TaskTree) uintptr {
	nodeSize := unsafe.Sizeof(node)
	nodeSize += getPredicateDataSize(node.Rhs)
	for _, p := range node.Lhs {
		nodeSize += getPredicateSize(p)
	}
	for _, p := range node.LhsCandidate {
		nodeSize += getPredicateSize(p)
	}
	nodeSize += getMapDataSize(node.TableId2index)
	return nodeSize
}

func getIncRuleSize(rule IncRule) uintptr {
	rSize := unsafe.Sizeof(rule)
	rSize += getTaskTreeSize(*rule.Node)
	return rSize
}

func getSampleSize(sample SampleNode) uintptr {
	size := unsafe.Sizeof(sample)
	size += getIncRuleSize(*sample.CurrentNode)
	for _, predecessor := range sample.PredecessorNodes {
		size += getIncRuleSize(*predecessor)
	}
	for _, conf := range sample.NeighborConfs {
		size += unsafe.Sizeof(conf)
	}
	for _, count := range sample.NeighborCDF {
		size += unsafe.Sizeof(count)
	}
	return size
}

func GetIncRulesSize(rules []*IncRule) uint64 {
	size := unsafe.Sizeof(rules)
	for _, rule := range rules {
		size += getIncRuleSize(*rule)
	}
	return uint64(size)
}

func GetSampleNodesSize(samples []*SampleNode) uint64 {
	size := unsafe.Sizeof(samples)
	for _, sample := range samples {
		size += getSampleSize(*sample)
	}
	return uint64(size)
}

func GetPLISize(pli map[string]map[string]map[int32][]int32) uint64 {
	if pli == nil {
		return 0
	}

	// map 的元数据大小
	mapSize := uint64(unsafe.Sizeof(pli))

	// 计算每个元素
	for outerKey, outerValue := range pli {
		// 计算 outerKey 的内存大小
		mapSize += uint64(unsafe.Sizeof(outerKey)) + uint64(len(outerKey))

		// 计算嵌套的 map[string]map[int32][]int32
		for innerKey, innerValue := range outerValue {
			// 计算 innerKey 的内存大小
			mapSize += uint64(unsafe.Sizeof(innerKey)) + uint64(len(innerKey))

			// 计算嵌套的 map[int32][]int32
			for intKey, intValue := range innerValue {
				// 计算 intKey 的内存大小
				mapSize += uint64(unsafe.Sizeof(intKey))

				// 计算 intValue 的内存大小，即一个 slice
				mapSize += uint64(unsafe.Sizeof(intValue)) // slice 本身的大小
				// 计算底层数组的大小
				if intValue != nil {
					mapSize += uint64(len(intValue)) * uint64(unsafe.Sizeof(intValue[0]))
				}
			}
		}
	}

	return mapSize
}

func getRuleSize(rule rds.Rule) uintptr {
	size := unsafe.Sizeof(rule)
	size += uintptr(len(rule.TableId))
	size += uintptr(len(rule.Ree))
	for _, p := range rule.LhsPredicates {
		size += getPredicateSize(p)
	}
	for _, c := range rule.LhsColumns {
		size += getColumnSize(c)
	}
	size += getPredicateDataSize(rule.Rhs)
	return size
}

func GetRulesSize(rules []*rds.Rule) uint64 {
	size := unsafe.Sizeof(rules)
	for _, rule := range rules {
		size += getRuleSize(*rule)
	}
	return uint64(size)
}
