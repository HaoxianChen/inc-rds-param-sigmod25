package tree

import (
	"sort"
	"testing"
	"time"
)

func TestAVCGen(t *testing.T) {
	// 就看一列
	data := []float64{16645.38684, 769.7886154, 1827.325317, 9230.18978, 7021.026403, 2045.869139, 844.972936, 7530.419378, 9758.895324, 3388.155641, 9543.686023, 175.9991867, 1152.857346, 1308.439581, 796.6879397, 6509.121181, 185.3846981, 5257.200472, 6163.448101, 3402.700653, 3762.055837, 16204.81884, 269.693692, 3366.759056, 20663.73853, 3818.566373, 20447.92397, 3628.283783, 6637.979658, 5255.123809, 1510.952806, 18572.94073, 7466.346469, 615.328472, 19062.78526, 13164.06528, 7166.539554, 1642.747511, 12693.60197, 620.2108093, 162.4400221, 9633.846929, 959.2139618, 38.40373827, 109.2514021, 865.277558, 5364.348895, 17126.76507, 12550.13094, 868.0785315, 13030.82195, 91.5537725, 5055.716656, 1543.408811, 1921.888328, 278.757937, 80.93212263, 2190.886945, 14383.98188, 10692.32788, 11353.14651, 230.5792159, 3584.143383, 6743.489287, 5841.540406, 3067.081358, 5052.527042, 2000.691952, 19261.87691, 6137.511805, 15291.37646, 3144.480498, 2524.056865, 2676.43194, 13751.73709, 2348.878717, 17397.00387, 3068.821984, 1240.368456, 6330.077323, 96.72113057, 4757.701954, 2843.684379, 7106.239511, 14961.77962, 15509.19614, 4305.753748, 3545.295482, 4145.983677, -53.65273829, 2652.576793, 3749.100157, 2506.703985, 3345.165698, 1029.271849, 594.9792526, 259.8126291, 3940.715287, 1128.969305, 3471.440027, 8495.905989, 139.490041, 6834.705369, 768.7116045, 5500.97091, 9842.838879, 5846.516354, 4605.600302, 140.7754819, 6591.901069, 7498.378711, 5249.278042, 14745.9931, 6159.171151, 9150.718257, 224.9956622, 20375.87014, 12346.01705, 6384.032185, 12804.82332, 7926.06203, 3498.902898, 3805.505314, 493.6114822, 1057.099649, 7879.689758, 1881.905779, 2179.364612}
	labels := []float64{2, 4, 4, 3, 1, 3, 4, 3, 2, 3, 3, 2, 1, 1, 4, 1, 3, 1, 1, 2, 1, 1, 2, 4, 1, 3, 1, 3, 2, 3, 4, 2, 2, 3, 2, 2, 3, 1, 3, 1, 3, 3, 2, 1, 3, 4, 4, 2, 4, 3, 4, 3, 1, 3, 2, 3, 1, 3, 4, 2, 4, 2, 3, 3, 2, 3, 4, 2, 2, 1, 2, 2, 2, 3, 4, 2, 2, 1, 2, 4, 4, 1, 4, 3, 1, 2, 2, 1, 2, 2, 3, 4, 1, 4, 3, 2, 4, 2, 3, 2, 3, 1, 1, 2, 3, 3, 3, 3, 1, 3, 3, 1, 2, 4, 3, 1, 2, 1, 3, 4, 1, 2, 1, 2, 1, 3, 4, 1}
	weights := []float64(nil)
	criterion := Entropy{}

	minV, maxV := data[0], data[0]
	dataNum := len(data)
	for i := 0; i < dataNum; i++ {
		if data[i] < minV {
			minV = data[i]
		} else if data[i] > maxV {
			maxV = data[i]
		}
	}
	intervalNum := 10
	gap := (maxV - minV) / float64(intervalNum)
	intervals := make([]float64, intervalNum+1)
	for i := 0; i <= intervalNum; i++ {
		intervals[i] = minV + float64(i)*gap
	}
	intervals[0] = NEG_INFINITY
	intervals[intervalNum] = INFINITY
	startTime := time.Now()
	avc := GenConciseAVC(0, intervals, data, labels, weights)

	info := avc.BestSplit(criterion)
	t.Log("first==>", info, time.Since(startTime))
	// small
	best := info
	startTime = time.Now()
	for _, interval := range info.CandiIntervals {
		avc = GenPartialAVC(0, interval, data, labels, weights)
		tmp := avc.BestSplit(criterion)
		if tmp.Valid() && tmp.ImprovementProxy > best.ImprovementProxy {
			best = tmp
		}
	}
	t.Log("best==>", best, time.Since(startTime))
	globalValueMap := make(map[float64]struct{})
	for _, v := range data {
		globalValueMap[v] = struct{}{}
	}
	globalValues := make([]float64, 0, len(globalValueMap))
	for v := range globalValueMap {
		globalValues = append(globalValues, v)
	}
	sort.Float64s(globalValues)
	startTime = time.Now()
	avc = GenSmallAVC(0, true, globalValues, data, labels, weights)
	smallBest := avc.BestSplit(criterion)

	t.Log("small best==>", smallBest, time.Since(startTime))

	sortedData := make([]float64, dataNum)
	sortedLabels := make([]float64, dataNum)
	copy(sortedData, data)
	copy(sortedLabels, labels)
	// fixme:如果有带weight的话，这里要连weight一起排
	sort.Sort(&sort2Floats{main: sortedData, sub: sortedLabels})

	allLabelCount := make(map[float64]float64)
	leftLabelCount := make(map[float64]float64)
	rightLabelCount := make(map[float64]float64)

	for _, label := range sortedLabels {
		allLabelCount[label] += 1
	}
	for k, v := range allLabelCount {
		rightLabelCount[k] = v
	}
	best = InnerSplitInfo{}
	best.Init()
	startTime = time.Now()
	for i := 0; i < dataNum-1; i++ {
		leftLabelCount[sortedLabels[i]] += 1
		rightLabelCount[sortedLabels[i]] -= 1

		leftImpurity, leftSum := criterion.Impurity(leftLabelCount)
		rightImpurity, rightSum := criterion.Impurity(rightLabelCount)
		improvementProxy := -leftImpurity*leftSum - rightImpurity*rightSum
		if improvementProxy > best.ImprovementProxy {
			best.ImprovementProxy = improvementProxy
			best.LeftImpurity, best.LeftWeight = leftImpurity, leftSum
			best.RightImpurity, best.RightWeight = rightImpurity, rightSum
			best.SplitValue = sortedData[i]/2 + sortedData[i+1]/2
		}
	}
	t.Log("base==>", best, time.Since(startTime))
}

func TestAVCMerge(t *testing.T) {
	data := []float64{16645.38684, 769.7886154, 1827.325317, 9230.18978, 7021.026403, 2045.869139, 844.972936, 7530.419378, 9758.895324, 3388.155641, 9543.686023, 175.9991867, 1152.857346, 1308.439581, 796.6879397, 6509.121181, 185.3846981, 5257.200472, 6163.448101, 3402.700653, 3762.055837, 16204.81884, 269.693692, 3366.759056, 20663.73853, 3818.566373, 20447.92397, 3628.283783, 6637.979658, 5255.123809, 1510.952806, 18572.94073, 7466.346469, 615.328472, 19062.78526, 13164.06528, 7166.539554, 1642.747511, 12693.60197, 620.2108093, 162.4400221, 9633.846929, 959.2139618, 38.40373827, 109.2514021, 865.277558, 5364.348895, 17126.76507, 12550.13094, 868.0785315, 13030.82195, 91.5537725, 5055.716656, 1543.408811, 1921.888328, 278.757937, 80.93212263, 2190.886945, 14383.98188, 10692.32788, 11353.14651, 230.5792159, 3584.143383, 6743.489287, 5841.540406, 3067.081358, 5052.527042, 2000.691952, 19261.87691, 6137.511805, 15291.37646, 3144.480498, 2524.056865, 2676.43194, 13751.73709, 2348.878717, 17397.00387, 3068.821984, 1240.368456, 6330.077323, 96.72113057, 4757.701954, 2843.684379, 7106.239511, 14961.77962, 15509.19614, 4305.753748, 3545.295482, 4145.983677, -53.65273829, 2652.576793, 3749.100157, 2506.703985, 3345.165698, 1029.271849, 594.9792526, 259.8126291, 3940.715287, 1128.969305, 3471.440027, 8495.905989, 139.490041, 6834.705369, 768.7116045, 5500.97091, 9842.838879, 5846.516354, 4605.600302, 140.7754819, 6591.901069, 7498.378711, 5249.278042, 14745.9931, 6159.171151, 9150.718257, 224.9956622, 20375.87014, 12346.01705, 6384.032185, 12804.82332, 7926.06203, 3498.902898, 3805.505314, 493.6114822, 1057.099649, 7879.689758, 1881.905779, 2179.364612}
	labels := []float64{2, 4, 4, 3, 1, 3, 4, 3, 2, 3, 3, 2, 1, 1, 4, 1, 3, 1, 1, 2, 1, 1, 2, 4, 1, 3, 1, 3, 2, 3, 4, 2, 2, 3, 2, 2, 3, 1, 3, 1, 3, 3, 2, 1, 3, 4, 4, 2, 4, 3, 4, 3, 1, 3, 2, 3, 1, 3, 4, 2, 4, 2, 3, 3, 2, 3, 4, 2, 2, 1, 2, 2, 2, 3, 4, 2, 2, 1, 2, 4, 4, 1, 4, 3, 1, 2, 2, 1, 2, 2, 3, 4, 1, 4, 3, 2, 4, 2, 3, 2, 3, 1, 1, 2, 3, 3, 3, 3, 1, 3, 3, 1, 2, 4, 3, 1, 2, 1, 3, 4, 1, 2, 1, 2, 1, 3, 4, 1}
	//weights := []float64(nil)
	criterion := Entropy{}

	minV, maxV := data[0], data[0]
	dataNum := len(data)
	for i := 0; i < dataNum; i++ {
		if data[i] < minV {
			minV = data[i]
		} else if data[i] > maxV {
			maxV = data[i]
		}
	}
	intervalNum := 10
	gap := (maxV - minV) / float64(intervalNum)
	intervals := make([]float64, intervalNum+1)
	for i := 0; i <= intervalNum; i++ {
		intervals[i] = minV + float64(i)*gap
	}
	intervals[0] = NEG_INFINITY
	intervals[intervalNum] = INFINITY

	// 把data划分一下，测试merge
	partNum := 8
	partSize := dataNum / partNum
	leftNum := dataNum - partNum*partSize
	cur := 0
	dataParts := make([][2]int, 0, partNum)

	for i := 0; i < leftNum; i++ {
		dataParts = append(dataParts, [2]int{cur, cur + partSize + 1})
		cur += partSize + 1
	}
	for cur < dataNum {
		dataParts = append(dataParts, [2]int{cur, cur + partSize})
		cur += partSize
	}

	startTime := time.Now()
	avcCh := make(chan *AVC, partNum)
	for _, part := range dataParts {
		part := part
		go func() {
			avcCh <- GenConciseAVC(0, intervals, data[part[0]:part[1]], labels[part[0]:part[1]], nil)
		}()
	}
	globalAVC := (*AVC)(nil)
	for i := 0; i < partNum; i++ {
		tmp := <-avcCh
		if globalAVC == nil {
			globalAVC = tmp
		} else {
			globalAVC.Merge(tmp)
		}
	}

	info := globalAVC.BestSplit(criterion)
	best := info
	for _, interval := range info.CandiIntervals {
		//globalAVC = GenPartialAVC(0, interval, data, labels, nil)
		for _, part := range dataParts {
			part := part
			go func() {
				avcCh <- GenPartialAVC(0, interval, data[part[0]:part[1]], labels[part[0]:part[1]], nil)
			}()
		}
		globalAVC = nil
		for i := 0; i < partNum; i++ {
			tmp := <-avcCh
			if globalAVC == nil {
				globalAVC = tmp
			} else {
				globalAVC.Merge(tmp)
			}
		}
		tmp := globalAVC.BestSplit(criterion)
		if tmp.Valid() && tmp.ImprovementProxy > best.ImprovementProxy {
			best = tmp
		}
	}
	t.Log("merge==>", best, time.Since(startTime))

	// small
	globalValueMap := make(map[float64]struct{})
	for _, v := range data {
		globalValueMap[v] = struct{}{}
	}
	globalValues := make([]float64, 0, len(globalValueMap))
	for v := range globalValueMap {
		globalValues = append(globalValues, v)
	}
	sort.Float64s(globalValues)
	startTime = time.Now()
	for _, part := range dataParts {
		part := part
		go func() {
			avcCh <- GenSmallAVC(0, true, globalValues, data[part[0]:part[1]], labels[part[0]:part[1]], nil)
		}()
	}
	globalAVC = nil
	for i := 0; i < partNum; i++ {
		tmp := <-avcCh
		if globalAVC == nil {
			globalAVC = tmp
		} else {
			globalAVC.Merge(tmp)
		}
	}
	smallBest := globalAVC.BestSplit(criterion)
	t.Log("merge small==>", smallBest, time.Since(startTime))
}

func TestAVCMerge2(t *testing.T) {
	data := []float64{16645.38684, 769.7886154, 1827.325317, 9230.18978, 7021.026403, 2045.869139, 844.972936, 7530.419378, 9758.895324, 3388.155641, 9543.686023, 175.9991867, 1152.857346, 1308.439581, 796.6879397, 6509.121181, 185.3846981, 5257.200472, 6163.448101, 3402.700653, 3762.055837, 16204.81884, 269.693692, 3366.759056, 20663.73853, 3818.566373, 20447.92397, 3628.283783, 6637.979658, 5255.123809, 1510.952806, 18572.94073, 7466.346469, 615.328472, 19062.78526, 13164.06528, 7166.539554, 1642.747511, 12693.60197, 620.2108093, 162.4400221, 9633.846929, 959.2139618, 38.40373827, 109.2514021, 865.277558, 5364.348895, 17126.76507, 12550.13094, 868.0785315, 13030.82195, 91.5537725, 5055.716656, 1543.408811, 1921.888328, 278.757937, 80.93212263, 2190.886945, 14383.98188, 10692.32788, 11353.14651, 230.5792159, 3584.143383, 6743.489287, 5841.540406, 3067.081358, 5052.527042, 2000.691952, 19261.87691, 6137.511805, 15291.37646, 3144.480498, 2524.056865, 2676.43194, 13751.73709, 2348.878717, 17397.00387, 3068.821984, 1240.368456, 6330.077323, 96.72113057, 4757.701954, 2843.684379, 7106.239511, 14961.77962, 15509.19614, 4305.753748, 3545.295482, 4145.983677, -53.65273829, 2652.576793, 3749.100157, 2506.703985, 3345.165698, 1029.271849, 594.9792526, 259.8126291, 3940.715287, 1128.969305, 3471.440027, 8495.905989, 139.490041, 6834.705369, 768.7116045, 5500.97091, 9842.838879, 5846.516354, 4605.600302, 140.7754819, 6591.901069, 7498.378711, 5249.278042, 14745.9931, 6159.171151, 9150.718257, 224.9956622, 20375.87014, 12346.01705, 6384.032185, 12804.82332, 7926.06203, 3498.902898, 3805.505314, 493.6114822, 1057.099649, 7879.689758, 1881.905779, 2179.364612}
	labels := []float64{2, 4, 4, 3, 1, 3, 4, 3, 2, 3, 3, 2, 1, 1, 4, 1, 3, 1, 1, 2, 1, 1, 2, 4, 1, 3, 1, 3, 2, 3, 4, 2, 2, 3, 2, 2, 3, 1, 3, 1, 3, 3, 2, 1, 3, 4, 4, 2, 4, 3, 4, 3, 1, 3, 2, 3, 1, 3, 4, 2, 4, 2, 3, 3, 2, 3, 4, 2, 2, 1, 2, 2, 2, 3, 4, 2, 2, 1, 2, 4, 4, 1, 4, 3, 1, 2, 2, 1, 2, 2, 3, 4, 1, 4, 3, 2, 4, 2, 3, 2, 3, 1, 1, 2, 3, 3, 3, 3, 1, 3, 3, 1, 2, 4, 3, 1, 2, 1, 3, 4, 1, 2, 1, 2, 1, 3, 4, 1}
	//weights := []float64(nil)
	criterion := Entropy{}

	minV, maxV := data[0], data[0]
	dataNum := len(data)
	for i := 0; i < dataNum; i++ {
		if data[i] < minV {
			minV = data[i]
		} else if data[i] > maxV {
			maxV = data[i]
		}
	}
	intervalNum := 10
	gap := (maxV - minV) / float64(intervalNum)
	intervals := make([]float64, intervalNum+1)
	for i := 0; i <= intervalNum; i++ {
		intervals[i] = minV + float64(i)*gap
	}
	intervals[0] = NEG_INFINITY
	intervals[intervalNum] = INFINITY

	// 把data划分一下，测试merge
	partNum := 8
	partSize := dataNum / partNum
	leftNum := dataNum - partNum*partSize
	cur := 0
	dataParts := make([][2]int, 0, partNum)

	for i := 0; i < leftNum; i++ {
		dataParts = append(dataParts, [2]int{cur, cur + partSize + 1})
		cur += partSize + 1
	}
	for cur < dataNum {
		dataParts = append(dataParts, [2]int{cur, cur + partSize})
		cur += partSize
	}

	startTime := time.Now()
	avcCh := make(chan *AVC, partNum)
	for _, part := range dataParts {
		part := part
		go func() {
			avcCh <- GenConciseAVC(0, intervals, data[part[0]:part[1]], labels[part[0]:part[1]], nil)
		}()
	}
	globalAVC := (*AVC)(nil)
	for i := 0; i < partNum; i++ {
		tmp := <-avcCh
		if globalAVC == nil {
			globalAVC = tmp
		} else {
			globalAVC.Merge(tmp)
		}
	}

	info := globalAVC.BestSplit(criterion)
	best := info
	for _, interval := range info.CandiIntervals {
		//globalAVC = GenPartialAVC(0, interval, data, labels, nil)
		for _, part := range dataParts {
			part := part
			go func() {
				avcCh <- GenPartialAVC(0, interval, data[part[0]:part[1]], labels[part[0]:part[1]], nil)
			}()
		}
		globalAVC = nil
		for i := 0; i < partNum; i++ {
			tmp := <-avcCh
			if globalAVC == nil {
				globalAVC = tmp
			} else {
				globalAVC.Merge(tmp)
			}
		}
		tmp := globalAVC.BestSplit(criterion)
		if tmp.Valid() && tmp.ImprovementProxy > best.ImprovementProxy {
			best = tmp
		}
	}
	t.Log("merge==>", best, time.Since(startTime))

	// small
	globalValueMap := make(map[float64]struct{})
	for _, v := range data {
		globalValueMap[v] = struct{}{}
	}
	globalValues := make([]float64, 0, len(globalValueMap))
	for v := range globalValueMap {
		globalValues = append(globalValues, v)
	}
	sort.Float64s(globalValues)
	startTime = time.Now()
	for _, part := range dataParts {
		part := part
		go func() {
			avcCh <- GenSmallAVC(0, true, globalValues, data[part[0]:part[1]], labels[part[0]:part[1]], nil)
		}()
	}
	globalAVC = nil
	for i := 0; i < partNum; i++ {
		tmp := <-avcCh
		if globalAVC == nil {
			globalAVC = tmp
		} else {
			globalAVC.Merge(tmp)
		}
	}
	smallBest := globalAVC.BestSplit(criterion)
	t.Log("merge small==>", smallBest, time.Since(startTime))
}

type sort2Floats struct {
	main []float64
	sub  []float64
}

func (s *sort2Floats) Len() int {
	return len((*s).main)
}

func (s *sort2Floats) Less(i, j int) bool {
	return (*s).main[i] < (*s).main[j]
}

func (s *sort2Floats) Swap(i, j int) {
	(*s).main[i], (*s).main[j] = (*s).main[j], (*s).main[i]
	(*s).sub[i], (*s).sub[j] = (*s).sub[j], (*s).sub[i]
}
