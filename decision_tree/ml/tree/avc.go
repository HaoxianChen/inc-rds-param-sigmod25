/*
	avc(Attribute-Value, ClassLabel)直方图，对连续取值的属性做区间划分，在各个区间上统计avc。
*/

package tree

import (
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/util/add"
	"math"
	"sort"
)

// AVC 一个属性的AVC图
type AVC struct {
	attrT    AttributeType // attrT 属性类型，这个类型其实意义可能不是很大，因为就算是 NumericUnlimited 之后也可能要再统计某个区间内具体的各个值的直方图，到时候是不是区间还是用下面的长度是否相等来判断吧
	attr     FeatureId     // attr 属性id
	nanCount float64       // nanCount 属性取值为NaN的不再参与划分，只是计个数，这里计数都是权重累和
	// attrVs 该属性的各个取值，如果是 NumericUnlimited 类型的，那就需要相邻的两项来确定一个区间，分k个区间，有k+1个边界值，这里左闭右开，但对最左边和最右边的区间会特殊处理，可能会用正负无穷
	// 这个切片是有序排列的。且对于区间情况下，是有特殊设置的。
	// 如果是统计区间的直方图，那么第一项为负无穷(替代原先的最小值)，最后一项为正无穷(替代原先的最大值)；
	// 如果是区间内部的统计，第一项和最后一项仍然是正负无穷，只是此时没有任何含义，只是labelCount中会多两项，代表小于这个区间的，和大于这个区间的count累计。
	// 这里是不含NaN的，虽然要放也可以，但要额外的判断逻辑，所有的NaN不再参与划分了，折算成相应比例
	// fixme:其实这个切片只在内部划分时有用，对于其他情况各个结点上的取值是一致的，其实没必要重复传的，到时候要优化的话可以看看，那一些判断逻辑就要改了
	attrVs []float64
	// labelCount 对属性的各个取值，统计各label的count。这个切片的下标与 attrVs 的下标对应，如果是区间的话，这里会少一项。每个取值(区间)的统计是独立的，如果要拿一个范围，要自己累加。
	// 这里的count值对应权重和，所以用float。
	// fixme:如果label可以转成从0开始的int的话，可以直接用切片代替map，之前是这么做的，就是要额外转一步
	labelCount []map[float64]float64
}

func ReConstructAVC(
	attrT AttributeType,
	attr FeatureId,
	nanCount float64,
	attrVs []float64,
	labelCount []map[float64]float64,
) *AVC {
	return &AVC{
		attrT:      attrT,
		attr:       attr,
		nanCount:   nanCount,
		attrVs:     attrVs,
		labelCount: labelCount,
	}
}

func (avc *AVC) AttrType() AttributeType {
	return (*avc).attrT
}

func (avc *AVC) Attr() FeatureId {
	return (*avc).attr
}

func (avc *AVC) NaNCount() float64 {
	return (*avc).nanCount
}

func (avc *AVC) AttrVs() []float64 {
	return (*avc).attrVs
}

func (avc *AVC) LabelCount() []map[float64]float64 {
	return (*avc).labelCount
}

// GenSmallAVC 生成small-AVC，不需要进行区间划分。globalValues是收集了各个机器上的属性之后得到的结果，全局统一的，这个数量不会很多，多的话就走区间划分了。
func GenSmallAVC(
	feat FeatureId, numeric bool,
	globalValues []float64, featureValues []float64, labels []float64, sampleWeights []float64) *AVC {
	avc := &AVC{attr: feat, attrVs: globalValues}
	if !numeric {
		// 非数值类型的
		(*avc).attrT = NonNumeric
	} else {
		(*avc).attrT = NumericLimited
	}
	valueIndexMap := make(map[float64]int, len(globalValues))
	for i, v := range globalValues {
		valueIndexMap[v] = i
	}

	labelCountMap := make([]map[float64]*add.FloatAdder, len(globalValues))
	for i := 0; i < len(globalValues); i++ {
		labelCountMap[i] = make(map[float64]*add.FloatAdder)
	}

	insNum := len(featureValues)
	wi := 1.0
	countMap := map[float64]*add.FloatAdder(nil)
	for i := 0; i < insNum; i++ {
		vi := featureValues[i]
		li := labels[i]
		if len(sampleWeights) != 0 {
			// 如果有NaN的话，在划分时会给权重，不会在这中途加权重
			wi = sampleWeights[i]
		}
		if math.IsNaN(vi) {
			(*avc).nanCount += wi // nan计数
		} else {
			// 给各个value取值，统计直方图
			countMap = labelCountMap[valueIndexMap[vi]]
			if adder, has := countMap[li]; has {
				adder.Add(wi)
			} else {
				adder = add.NewFloatAdder()
				adder.Add(wi)
				countMap[li] = adder
			}
		}
	}

	(*avc).labelCount = make([]map[float64]float64, len(globalValues))
	for i := 0; i < len(globalValues); i++ {
		(*avc).labelCount[i] = adder2Float(labelCountMap[i])
	}

	return avc
}

// GenConciseAVC 生成concise-AVC，也就是对于连续的数值类型，对各区间统计直方图// 计算每个区间中各个label的weight
func GenConciseAVC(feat FeatureId, globalIntervals []float64, featureValues []float64, labels []float64, sampleWeights []float64) *AVC {
	avc := &AVC{attr: feat, attrT: NumericUnlimited, attrVs: globalIntervals}

	// 认为globalIntervals里已经排好序，且边界值不会有重复。区间是左闭右开的，所以首尾做了相应的替换(换成无穷)
	labelCount := make([]map[float64]*add.FloatAdder, len(globalIntervals)-1)
	for i := range labelCount {
		labelCount[i] = make(map[float64]*add.FloatAdder)
	}
	insNum := len(featureValues)
	wi := 1.0
	for i := 0; i < insNum; i++ {
		vi := featureValues[i]
		li := labels[i]
		if len(sampleWeights) != 0 {
			// 如果有NaN的话，在划分时会给权重，不会在这中途加权重
			wi = sampleWeights[i]
		}
		if math.IsNaN(vi) {
			(*avc).nanCount += wi // nan计数
		} else {
			// 查找这个值属于哪个区间
			// 因为首尾替换成了无穷，所以不会返回0和len-1
			belongTo := sort.SearchFloat64s(globalIntervals, vi)
			thisInterval := map[float64]*add.FloatAdder(nil)
			// 区间是左闭右开的，所以对于在区间内和在边界上的处理是不同的
			if globalIntervals[belongTo] == vi {
				// 是边界值
				thisInterval = labelCount[belongTo]
			} else {
				// 落在区间内
				thisInterval = labelCount[belongTo-1] // 这里不再做保护，因为有-inf
			}
			if adder, has := thisInterval[li]; has {
				adder.Add(wi)
			} else {
				adder = add.NewFloatAdder()
				adder.Add(wi)
				thisInterval[li] = adder
			}
		}
	}
	(*avc).labelCount = make([]map[float64]float64, len(labelCount))
	for i := range labelCount {
		(*avc).labelCount[i] = adder2Float(labelCount[i])
	}

	return avc
}

func adder2Float(in map[float64]*add.FloatAdder) map[float64]float64 {
	out := make(map[float64]float64, len(in))
	for k, v := range in {
		out[k] = v.Result()
	}
	return out
}

// GenPartialAVC 为区间内部各个点生成AVC直方图，区间按左闭右开来看
func GenPartialAVC(feat FeatureId, interval [2]float64, featureValues []float64, labels []float64, sampleWeights []float64) *AVC {
	avc := &AVC{attr: feat, attrT: NumericUnlimited} // 既然划分了区间，肯定是这个类型的，且不必再统计一次NaN，对一个属性来说是固定的，之前已经统计过了
	insNum := len(featureValues)
	beforeInterval := make(map[float64]*add.FloatAdder)
	afterInterval := make(map[float64]*add.FloatAdder)
	inInterval := make(map[float64]map[float64]*add.FloatAdder) // 对区间内的每个value取值有一个map
	wi := 1.0
	for i := 0; i < insNum; i++ {
		vi := featureValues[i]
		li := labels[i]
		if len(sampleWeights) != 0 {
			// 如果有NaN的话，在划分时会给权重，不会在这中途加权重
			wi = sampleWeights[i]
		}
		if math.IsNaN(vi) {
			// NaN的跳过，也不需要统计数量，第一次遍历的时候统计过了
			continue
		} else if vi < interval[0] {
			if adder, has := beforeInterval[li]; has {
				adder.Add(wi)
			} else {
				adder = add.NewFloatAdder()
				adder.Add(wi)
				beforeInterval[li] = adder
			}
		} else if vi >= interval[1] {
			if adder, has := afterInterval[li]; has {
				adder.Add(wi)
			} else {
				adder = add.NewFloatAdder()
				adder.Add(wi)
				afterInterval[li] = adder
			}
		} else {
			// 是区间内的值
			nonEmptyClassMap := inInterval[vi]
			if nonEmptyClassMap == nil {
				nonEmptyClassMap = make(map[float64]*add.FloatAdder)
				inInterval[vi] = nonEmptyClassMap
			}
			// fixme:其实对于连续值来说，可能很难有重复的值，每个用个map可能有点浪费空间，到时候再看看要不要直接把value和value的label带着好了，那还要个权重
			if adder, has := nonEmptyClassMap[li]; has {
				adder.Add(wi)
			} else {
				adder = add.NewFloatAdder()
				adder.Add(wi)
				nonEmptyClassMap[li] = adder
			}
		}
	}

	attrVs := make([]float64, 0, len(inInterval)+2) // 第一项和最后一项为负无穷、正无穷，代表该区间之前的和该区间之后的
	labelCount := make([]map[float64]float64, 0, len(inInterval)+2)
	attrVs = append(attrVs, NEG_INFINITY) // 负无穷，代表区间左边
	labelCount = append(labelCount, adder2Float(beforeInterval))
	for k, v := range inInterval {
		attrVs = append(attrVs, k)
		labelCount = append(labelCount, adder2Float(v))
	}
	attrVs = append(attrVs, INFINITY) // 正无穷，代表区间右边
	labelCount = append(labelCount, adder2Float(afterInterval))
	tmp := &sort2{
		// 其实不截取也没事，反正第一项和最后一项是无穷
		attrVs:     attrVs[1 : len(inInterval)+1],
		labelCount: labelCount[1 : len(inInterval)+1],
	}
	sort.Sort(tmp)
	(*avc).attrVs = attrVs
	(*avc).labelCount = labelCount

	return avc
}

type sort2 struct {
	attrVs     []float64
	labelCount []map[float64]float64
}

func (s *sort2) Len() int {
	return len((*s).attrVs)
}

func (s *sort2) Swap(i, j int) {
	(*s).attrVs[i], (*s).attrVs[j] = (*s).attrVs[j], (*s).attrVs[i]
	(*s).labelCount[i], (*s).labelCount[j] = (*s).labelCount[j], (*s).labelCount[i]
}

func (s *sort2) Less(i, j int) bool {
	return (*s).attrVs[i] < (*s).attrVs[j]
}

// IntervalCount 是否是对各个区间统计的直方图
func (avc *AVC) IntervalCount() bool {
	// 区间的边界点数量会比区间数多一
	return len((*avc).attrVs) != len((*avc).labelCount)
}

// CountInInterval 是否是对区间内部的统计(这是对区间处理的第二步)
func (avc *AVC) CountInInterval() bool {
	// 首先要是会进行区间划分的属性，而且是在第二阶段对区间内部做划分
	return (*avc).attrT == NumericUnlimited && len((*avc).attrVs) == len((*avc).labelCount)
}

// AllLabelCountMap 把所有取值(区间)的直方图合并
func (avc *AVC) AllLabelCountMap() map[float64]float64 {
	allCountMap := make(map[float64]*add.FloatAdder)
	for _, count := range (*avc).labelCount {
		for k, v := range count {
			if adder, has := allCountMap[k]; has {
				adder.Add(v)
			} else {
				adder = add.NewFloatAdder()
				adder.Add(v)
				allCountMap[k] = adder
			}
		}
	}

	res := make(map[float64]float64, len(allCountMap))
	for k, v := range allCountMap {
		res[k] = v.Result()
	}
	return res
}

// Merge 合并另一个AVC
func (avc *AVC) Merge(other *AVC) {
	if (*avc).attr != other.attr && avc.CountInInterval() == other.CountInInterval() {
		// 相同的属性才能合并，而且要确保是在同一阶段
		return
	}

	if avc.CountInInterval() {
		// 其实到这一步的话，不统计nan也没事了，因为第一阶段的时候统计过一次了
		//(*avc).nanCount += other.nanCount

		// 如果是对区间内部的统计，那不能保证两个avc的属性值列表是一样的
		newAttrVs := []float64(nil) // 这里不预先分配空间了
		newLabelCount := []map[float64]float64(nil)
		leftP, rightP := 0, 0
		leftNum, rightNum := len((*avc).attrVs), len(other.attrVs)
		leftElem, rightElem := map[float64]float64(nil), map[float64]float64(nil)
		// 归并合并，attrVs是有序排列的
		for leftP < leftNum && rightP < rightNum {
			if (*avc).attrVs[leftP] == other.attrVs[rightP] {
				// 相等，要合并
				leftElem = (*avc).labelCount[leftP]
				rightElem = other.labelCount[rightP]
				for k, v := range rightElem {
					leftElem[k] += v
				}
				newAttrVs = append(newAttrVs, (*avc).attrVs[leftP])
				newLabelCount = append(newLabelCount, leftElem)
				leftP++
				rightP++
			} else if (*avc).attrVs[leftP] < other.attrVs[rightP] {
				// 左边的小，选左边
				newAttrVs = append(newAttrVs, (*avc).attrVs[leftP])
				newLabelCount = append(newLabelCount, (*avc).labelCount[leftP])
				leftP++
			} else {
				// 右边的小，选右边
				newAttrVs = append(newAttrVs, other.attrVs[rightP])
				newLabelCount = append(newLabelCount, other.labelCount[rightP])
				rightP++
			}
		}
		for leftP < leftNum {
			newAttrVs = append(newAttrVs, (*avc).attrVs[leftP])
			newLabelCount = append(newLabelCount, (*avc).labelCount[leftP])
			leftP++
		}
		for rightP < rightNum {
			newAttrVs = append(newAttrVs, other.attrVs[rightP])
			newLabelCount = append(newLabelCount, other.labelCount[rightP])
			rightP++
		}
		(*avc).attrVs = newAttrVs
		(*avc).labelCount = newLabelCount
		return
	}

	// 除了区间内部的划分，其他的都要求各个avc之间对齐
	if len((*avc).attrVs) != len(other.attrVs) || len((*avc).labelCount) != len(other.labelCount) {
		// 要求对属性值的划分是一致的，这里只做长度判断
		return
	}
	//vNum := len((*avc).attrVs)
	//for i := 0; i < vNum; i++ {
	//	if (*avc).attrVs[i] != other.attrVs[i] {
	//		// 再做个值判断，其实如果能保证的话，这里不要这个检查也是可以的，省点时间
	//		return
	//	}
	//}
	(*avc).nanCount += other.nanCount
	countNum := len((*avc).labelCount)
	ownCount := map[float64]float64(nil)
	otherCount := map[float64]float64(nil)
	for i := 0; i < countNum; i++ {
		ownCount = (*avc).labelCount[i]
		otherCount = other.labelCount[i]
		if ownCount == nil {
			// 这里是很有可能有nil的
			(*avc).labelCount[i] = otherCount
			continue
		}
		for k, v := range otherCount {
			ownCount[k] += v
		}
	}
}

// BestSplit 选择最优二分划分点。
// 对于非区间的取值，直接对各值按序划分就好，返回值不用考虑candiIntervals。要注意一种特殊情况，就是对可能有更好结果的区间内部划分，对于这种情况到时候会特殊处理；
// 对于划分了区间的取值，在各个区间边界处进行划分，计算一个最优值，然后计算各个区间边界处的split值，得到一个内部划分的improvement上界，和边界处划分的最优值比较，返回那些可能更有的区间。
func (avc *AVC) BestSplit(criterion Criterion) InnerSplitInfo {
	if (*avc).attrT == NonNumeric {
		// 非数值类型的划分
		return *avc._splitNonNumeric(criterion)
	} else {
		// 数值类型的划分
		return *avc._splitNumeric(criterion)
	}
}

// _splitNumeric 对数值类型的进行划分
func (avc *AVC) _splitNumeric(criterion Criterion) *InnerSplitInfo {
	bestSplit := new(InnerSplitInfo)
	bestSplit.Init()
	curSplit := InnerSplitInfo{} // 这个不用初始化

	curPos := 0 // 遍历avc中的直方图
	bestPos := -1
	end := len((*avc).labelCount)
	allCount := avc.AllLabelCountMap()
	leftAddMap := make(map[float64]*add.FloatAdder, len(allCount)) // fixme:到时候看看有没有必要用这个来求和
	leftCountMap := make(map[float64]float64, len(allCount))
	rightCountMap := make(map[float64]float64, len(allCount))

	for k, v := range allCount {
		leftAddMap[k] = add.NewFloatAdder()
		rightCountMap[k] = v // 右边先设为全部的，作为初始状态
	}

	for curPos < end-1 {
		// cur所在的那一块，要划分到左边
		for class, count := range (*avc).labelCount[curPos] {
			adder := leftAddMap[class]
			adder.Add(count)
			// cur不涉及的那些k，就不动
			leftCountMap[class] = adder.Result()
			rightCountMap[class] = allCount[class] - adder.Result()
		}
		curSplit.LeftImpurity, curSplit.LeftWeight = criterion.Impurity(leftCountMap)
		curSplit.RightImpurity, curSplit.RightWeight = criterion.Impurity(rightCountMap)
		curSplit.ImprovementProxy = -curSplit.LeftImpurity*curSplit.LeftWeight - curSplit.RightImpurity*curSplit.RightWeight
		if curSplit.ImprovementProxy > bestSplit.ImprovementProxy {
			bestPos = curPos
			*bestSplit = curSplit
		}
		curPos++
	}
	if bestPos == -1 {
		// 只有一块，无法进行划分，如果是区间的话那就后续再对区间内部去分吧
		if avc.IntervalCount() {
			// 只有这一个区间，直接赋值就好
			(*bestSplit).CandiIntervals = [][2]float64{[2]float64{(*avc).attrVs[0], (*avc).attrVs[1]}}
		}
		return bestSplit
	}

	if avc.IntervalCount() {
		// 区间统计的话，还要对区间内各个边界点做判断，记录可能更好的区间
		(*bestSplit).SplitValue = (*avc).attrVs[bestPos+1] // attrVs[bestPos]~attrVs[bestPos+1]对应这个区间，这个区间被分到左边，这个区间的开闭影响splitValue应该用小于还是小于等于去划分
		(*bestSplit).CandiIntervals = avc.CandiIntervalsBetterThan((*bestSplit).ImprovementProxy, criterion)
	} else if avc.CountInInterval() && (bestPos == 0 || bestPos == end-2) {
		// 如果是区间内的count，也就上面得到的intervals内部再做统计，那么attrVs的首尾两项分别是负无穷和正无穷
		// 如果split时在第一项或最后一项划分，那么相当于还是区间外的划分，那就没必要返回
		bestSplit.Init()
	} else {
		// 这里不再取中间了，而是用边界值
		// 注意best是把bestPos处的划到左边了，这里splitValue取bestPos+1处，和上面区间内统计保持一致，那就是要把所有小于该splitValue的，划分到左边
		// 这样可能也不太好，>= [bestPos] 和 < [bestPos+1]，不一定哪个好，应该取中间的
		// 所以就下面这样处理了，外面判断一律把小于splitValue的，划分到左边
		if avc.attrT == NumericLimited {
			// 对于属性值有限的，取边界值
			(*bestSplit).SplitValue = (*avc).attrVs[bestPos+1]
		} else {
			// 其他的，取平均
			(*bestSplit).SplitValue = (*avc).attrVs[bestPos]/2 + (*avc).attrVs[bestPos+1]/2
		}
	}

	return bestSplit
}

// _splitNonNumeric 对非数值类型的划分
func (avc *AVC) _splitNonNumeric(criterion Criterion) *InnerSplitInfo {
	// 非数值类型的划分会有些区别，主要是属性值的大小没有什么意义了
	// 这里做二分类的话，就任意取一个值分到一边，剩下的值分到另一边，之后splitValue设为这个取的值
	bestSplit := new(InnerSplitInfo)
	bestSplit.Init()
	curSplit := InnerSplitInfo{} // 这个不用初始化

	curPos := 0 // 遍历avc中的直方图
	bestPos := -1
	end := len((*avc).labelCount)
	allCount := avc.AllLabelCountMap()
	remainedCount := make(map[float64]float64)

	if end <= 1 {
		// 只有一块，无法进行划分，如果取值是常量的话，会属于这种情况
		return bestSplit
	}
	for curPos < end {
		// 选中cur作为单独的一块(左边)，其他的合在一起作为另一块
		curMap := (*avc).labelCount[curPos]
		for class, count := range allCount {
			// 更新remained
			remainedCount[class] = count - curMap[class]
		}
		curSplit.LeftImpurity, curSplit.LeftWeight = criterion.Impurity(curMap)
		curSplit.RightImpurity, curSplit.RightWeight = criterion.Impurity(remainedCount)
		curSplit.ImprovementProxy = -curSplit.LeftImpurity*curSplit.LeftWeight - curSplit.RightImpurity*curSplit.RightWeight
		if curSplit.ImprovementProxy > bestSplit.ImprovementProxy {
			bestPos = curPos
			*bestSplit = curSplit
		}
		curPos++
	}
	(*bestSplit).SplitValue = (*avc).attrVs[bestPos]
	return bestSplit
}

// CandiIntervalsBetterThan 记录所有区间内部最大可能的提升度大于threshold的区间
func (avc *AVC) CandiIntervalsBetterThan(threshold float64, criterion Criterion) [][2]float64 {
	allCount := avc.AllLabelCountMap()
	beforeCur := make(map[float64]*add.FloatAdder, len(allCount))
	for k := range allCount {
		beforeCur[k] = add.NewFloatAdder()
	}
	leftCountMap := make(map[float64]float64, len(allCount))
	rightCountMap := make(map[float64]float64, len(allCount))
	leftImpurity, leftWeight, rightImpurity, rightWeight := 0.0, 0.0, 0.0, 0.0
	improvementProxy := 0.0
	res := [][2]float64(nil)
	for pInterval, countMap := range (*avc).labelCount {
		leftCountMap = make(map[float64]float64, len(allCount))
		rightCountMap = make(map[float64]float64, len(allCount)) //原本下一次循环中没有清空这个map，导致不稳定的原因之一
		// 要计算2^c个值，c是类别数，对应这里的countMap长度
		// 这里用循环来拿到
		// 先把map转切片，保序
		classSlice := make([]float64, 0, len(countMap))
		countSlice := make([]float64, 0, len(countMap))
		for k, v := range countMap {
			classSlice = append(classSlice, k)
			countSlice = append(countSlice, v)
		}
		// 可以用类似进位的方法来拿到，不用递归
		// 给个函数吧
		if len(classSlice) > 8 {
			// 类别数太多，不再考虑区间内部的情况，也就是只拿个近似解了
			//log.Warn().Msgf("class-num is too large! --> %d escape the inner-interval check", len(classSlice))
			// 更新一下beforeCur
			for k, v := range countMap {
				beforeCur[k].Add(v)
			}
			continue
		}
		enum := newEnumerator(int8(len(classSlice)), nil)
		curClass := 0.0
		for enum.hasNext() {
			// 已经跳过全0和全1的了
			pickToLeft := enum.next()
			for i, toLeft := range pickToLeft {
				curClass = classSlice[i]
				if toLeft {
					// 分到左边去
					leftCountMap[curClass] = beforeCur[curClass].Result() + countSlice[i]
				} else {
					leftCountMap[curClass] = beforeCur[curClass].Result()
				}
			}
			for k, v := range allCount {
				rightCountMap[k] = v - leftCountMap[k]
			}
			leftImpurity, leftWeight = criterion.Impurity(leftCountMap)
			rightImpurity, rightWeight = criterion.Impurity(rightCountMap)
			improvementProxy = -leftImpurity*leftWeight - rightImpurity*rightWeight
			if improvementProxy > threshold {
				// 这个区间内部是可能更优的
				res = append(res, [2]float64{(*avc).attrVs[pInterval], (*avc).attrVs[pInterval+1]})
				break
			}
		}

		// 更新一下beforeCur
		for k, v := range countMap {
			beforeCur[k].Add(v)
		}
	}
	return res
}

type enumerator struct {
	bitLen    int8 // 位数，枚举[0, 2^bitLen)，所以要求bitLen不要太长
	enumerate uint64
	buffer    []bool // 长度为bitLen
}

func newEnumerator(bitLen int8, buffer []bool) *enumerator {
	if bitLen > 64 {
		// 太大，枚举不了
		return nil
	}
	if buffer == nil {
		buffer = make([]bool, bitLen)
	}
	// 从1开始，跳过全0
	return &enumerator{bitLen: bitLen, enumerate: 1, buffer: buffer}
}

func (enum *enumerator) hasNext() bool {
	// 不计全1
	return (*enum).enumerate < (uint64(1)<<(*enum).bitLen)-1
}

func (enum *enumerator) next() []bool {
	// 那这是会包含全零和全1的
	for i := int8(0); i < (*enum).bitLen; i++ {
		(*enum).buffer[i] = false // 先清0
	}
	// 要判断哪几位是1
	curBit := 0
	tmp := (*enum).enumerate

	for tmp != 0 {
		if tmp&1 == 1 {
			(*enum).buffer[curBit] = true
		}
		curBit += 1
		tmp >>= 1
	}
	(*enum).enumerate += 1

	return (*enum).buffer
}

type InnerSplitInfo struct {
	SplitValue       float64      // SplitValue 用于划分的值
	LeftWeight       float64      // LeftWeight 划分之后左半部分实例权重和
	RightWeight      float64      // RightWeight 划分之后右半部分实例权重和
	LeftImpurity     float64      // LeftImpurity 划分之后左半部分impurity
	RightImpurity    float64      // RightImpurity 划分之后右半部分impurity
	ImprovementProxy float64      // ImprovementProxy 提升度，这个提升度是用于该属性内部比较的，属性间的比较需要另外再算
	CandiIntervals   [][2]float64 // CandiIntervals 对于划分成区间的连续属性，会计算区间内部的improvement上界，返回所有可能更好的区间
}

func (split *InnerSplitInfo) Init() {
	(*split).SplitValue = NEG_INFINITY // 如果splitValue是这个，认为是无效的
	(*split).ImprovementProxy = NEG_INFINITY
}

func (split InnerSplitInfo) Valid() bool {
	return split.SplitValue != NEG_INFINITY || len(split.CandiIntervals) != 0
}

type Criterion interface {
	// Impurity 根据label的分布计算一个属性的impurity，另有一个sum是各label的计数之和
	Impurity(countMap map[float64]float64) (impurity float64, sum float64)
	// String thrift那没法传接口，所以返回个名字，方便重新构建
	String() string
}

// GetCriterionByName 一般还是自己直接创建Criterion的好，这里为了序列化、反序列化
func GetCriterionByName(name string) Criterion {
	switch name {
	case Entropy{}.String():
		return Entropy{}
	case Gini{}.String():
		return Gini{}
	default:
		return nil
	}
}

type Entropy struct{}

func (en Entropy) Impurity(countMap map[float64]float64) (impurity float64, sum float64) {
	// 这个调用次数应该不会很多，这是把所有统计放在一起看
	sumAdder := add.NewFloatAdder()
	for _, count := range countMap {
		sumAdder.Add(count)
	}
	sum = sumAdder.Result()

	impurityAdder := add.NewFloatAdder()
	for _, labelCount := range countMap {
		if labelCount > 0 {
			labelCount /= sum
			impurityAdder.Add(-labelCount * math.Log(labelCount))
		}
	}
	impurity = impurityAdder.Result()
	return
}

func (en Entropy) String() string {
	return "entropy"
}

type Gini struct{}

func (g Gini) Impurity(countMap map[float64]float64) (impurity float64, sum float64) {
	// 这个调用次数应该不会很多，这是把所有统计放在一起看
	sumAdder := add.NewFloatAdder()
	for _, count := range countMap {
		sumAdder.Add(count)
	}
	sum = sumAdder.Result()

	impurity = 0.0
	tmpSq := add.NewFloatAdder()
	for _, labelCount := range countMap {
		if labelCount > 0 {
			tmpSq.Add(labelCount * labelCount)
		}
	}
	impurity = 1 - tmpSq.Result()/(sum*sum)

	return
}

func (g Gini) String() string {
	return "gini"
}
