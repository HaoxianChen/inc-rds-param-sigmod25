package correlation_util

import (
	"math"
	"sort"
)

// 等级排序结构
type rank struct {
	X     float64
	Y     float64
	Xrank float64
	Yrank float64
}

// Spearman Spearman相关系数计算，用到pearson
func Spearman(data1, data2 []float64, ranks []rank, spearman_a, spearman_b []float64) float64 {

	for index := 0; index < len(data1); index++ {
		x := data1[index]
		y := data2[index]
		ranks[index] = rank{x, y, 0, 0}
	}

	sort.Slice(ranks, func(i int, j int) bool {
		return ranks[i].X < ranks[j].X
	})

	for position := 0; position < len(ranks); position++ {
		ranks[position].Xrank = float64(position) + 1

		duplicateValues := []int{position}
		for nested, p := range ranks {
			if (math.IsNaN(p.X) && math.IsNaN(ranks[position].X)) || ranks[position].X == p.X {
				if position != nested {
					duplicateValues = append(duplicateValues, nested)
				}
			}
		}
		sum := 0
		for _, val := range duplicateValues {
			sum += val
		}

		avg := float64((sum + len(duplicateValues))) / float64(len(duplicateValues))
		ranks[position].Xrank = avg

		for index := 1; index < len(duplicateValues); index++ {
			ranks[duplicateValues[index]].Xrank = avg
		}

		position += len(duplicateValues) - 1
	}

	sort.Slice(ranks, func(i int, j int) bool {

		return ranks[i].Y < ranks[j].Y
	})

	for position := 0; position < len(ranks); position++ {
		ranks[position].Yrank = float64(position) + 1
		duplicateValues := []int{position}
		for nested, p := range ranks {
			if (math.IsNaN(p.Y) && math.IsNaN(ranks[position].Y)) || ranks[position].Y == p.Y {
				if position != nested {
					duplicateValues = append(duplicateValues, nested)
				}
			}
		}
		sum := 0
		for _, val := range duplicateValues {
			sum += val
		}
		avg := float64((sum + len(duplicateValues))) / float64(len(duplicateValues))
		ranks[position].Yrank = avg

		for index := 1; index < len(duplicateValues); index++ {
			ranks[duplicateValues[index]].Yrank = avg
		}
		position += len(duplicateValues) - 1
	}

	for i := 0; i < len(ranks); i++ {
		spearman_a[i] = ranks[i].Xrank
		spearman_b[i] = ranks[i].Yrank
	}

	return pearsonV3(spearman_a, spearman_b)
}

// SpearmanV2 解决里面重复开辟duplicateValues切片问题
func SpearmanV2(data1, data2 []float64, ranks []rank, spearman_a, spearman_b, duplicateValuesRE []float64) float64 {

	for index := 0; index < len(data1); index++ {
		x := data1[index]
		y := data2[index]
		ranks[index] = rank{x, y, 0, 0}
	}

	//根据x值排序
	sort.Slice(ranks, func(i int, j int) bool {
		if math.IsNaN(ranks[i].X) {
			return true
		}
		if math.IsNaN(ranks[j].X) {
			return false
		}
		return ranks[i].X < ranks[j].X
	})

	rankIndex := 1

	for position := 0; position < len(ranks); position++ {

		if math.IsNaN(ranks[position].X) {
			continue
		}

		ranks[position].Xrank = float64(rankIndex) + 1

		//duplicateValues := []int{position}
		duplicateValuesRE[0] = float64(position)
		index := 1

		for nested, p := range ranks {
			if ranks[position].X == p.X {
				if position != nested {
					duplicateValuesRE[index] = float64(nested)
					index += 1
					//duplicateValues = append(duplicateValues, nested)
				}
			}
		}

		sum := 0.
		for i := 0; i < index; i++ {
			sum += duplicateValuesRE[i]
		}

		//for _, val := range duplicateValues {
		//	sum += val
		//}

		avg := (sum + float64(index)) / float64(index)
		ranks[position].Xrank = avg

		for i := 1; i < index; i++ {
			ranks[int(duplicateValuesRE[i])].Xrank = avg
		}
		position += int(index) - 1
	}

	sort.Slice(ranks, func(i int, j int) bool {
		if math.IsNaN(ranks[i].Y) {
			return true
		}
		if math.IsNaN(ranks[j].Y) {
			return false
		}
		return ranks[i].Y < ranks[j].Y
	})

	for position := 0; position < len(ranks); position++ {
		ranks[position].Yrank = float64(position) + 1

		//duplicateValues := []int{position}
		duplicateValuesRE[0] = float64(position)
		index := 1

		for nested, p := range ranks {
			if (math.IsNaN(p.Y) && math.IsNaN(ranks[position].Y)) || ranks[position].Y == p.Y {
				if position != nested {
					duplicateValuesRE[index] = float64(nested)
					index += 1
					//duplicateValues = append(duplicateValues, nested)
				}
			}
		}
		sum := 0.
		for i := 0; i < index; i++ {
			sum += duplicateValuesRE[i]
		}

		avg := (sum + float64(index)) / float64(index)
		ranks[position].Yrank = avg

		for i := 1; i < index; i++ {
			ranks[int(duplicateValuesRE[i])].Yrank = avg
		}

		position += int(index) - 1
	}

	for i := 0; i < len(ranks); i++ {
		spearman_a[i] = ranks[i].Xrank
		spearman_b[i] = ranks[i].Yrank
	}

	return pearsonV3(spearman_a, spearman_b)
}

func SpearmanV3(data1, data2 []float64, ranks []rank, spearman_a, spearman_b []float64) float64 {
	for index := 0; index < len(data1); index++ {
		x := data1[index]
		y := data2[index]
		ranks[index] = rank{x, y, 0, 0}
	}
	//根据x值排序
	sort.Slice(ranks, func(i int, j int) bool {
		if math.IsNaN(ranks[i].X) {
			return true
		}
		if math.IsNaN(ranks[j].X) {
			return false
		}
		return ranks[i].X < ranks[j].X
	})

	rankIndex := 1
	noneIndex := 0
	for i := 0; i < len(ranks); i++ {
		if !math.IsNaN(ranks[i].X) {
			noneIndex = i
			break
		}
	}

	for pos := noneIndex; pos < len(ranks); pos++ {
		nowValue := ranks[pos].X
		avg := float64(rankIndex)
		equalValueNum := 1

		for pos2 := pos + 1; pos2 < len(ranks); pos2++ {
			if ranks[pos2].X != nowValue || pos2 == len(ranks)-1 {
				equalValueNum = pos2 - pos //相同元素的数量
				avg = (avg + float64(rankIndex)) / 2.
				rankIndex += 1
				break
			}
			rankIndex += 1
		}

		for i := 0; i < equalValueNum; i++ {
			ranks[i+pos].Xrank = avg
		}
		pos += equalValueNum - 1
	}

	//根据Y值排序
	sort.Slice(ranks, func(i int, j int) bool {
		if math.IsNaN(ranks[i].Y) {
			return true
		}
		if math.IsNaN(ranks[j].Y) {
			return false
		}
		return ranks[i].Y < ranks[j].Y
	})

	rankIndex = 1
	noneIndex = 0
	for i := 0; i < len(ranks); i++ {
		if !math.IsNaN(ranks[i].Y) {
			noneIndex = i
			break
		}
	}
	for pos := noneIndex; pos < len(ranks); pos++ {
		nowValue := ranks[pos].Y
		avg := float64(rankIndex)
		equalValueNum := 1
		for pos2 := pos + 1; pos2 < len(ranks); pos2++ {
			if ranks[pos2].Y != nowValue || pos2 == len(ranks)-1 {
				equalValueNum = pos2 - pos //相同元素的数量
				avg = (avg + float64(rankIndex)) / (2.)
				rankIndex += 1
				break
			}
			rankIndex += 1
		}
		for i := 0; i < equalValueNum; i++ {
			ranks[i+pos].Yrank = avg
		}
		pos += equalValueNum - 1
	}

	for i := 0; i < len(ranks); i++ {
		spearman_a[i] = ranks[i].Xrank
		spearman_b[i] = ranks[i].Yrank
	}
	return pearsonV3(spearman_a, spearman_b)
}
