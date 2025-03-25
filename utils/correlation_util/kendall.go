package correlation_util

import (
	"math"
	"sort"
)

// kendall Kendall精准计算方法
func kendallExact(a, b []float64) float64 {
	length := len(a)
	ties_onlyin_x := 0.0
	ties_onlyin_y := 0.0
	con_pair := 0.0
	dis_pair := 0.0
	for i := 0; i < length-1; i++ {
		for j := i + 1; j < length; j++ {
			test_tying_x := sign(a[i] - a[j])
			test_tying_y := sign(b[i] - b[j])
			panduan := test_tying_x * test_tying_y
			if panduan == 1.0 {
				con_pair += 1
			} else {
				if panduan == -1.0 {
					dis_pair += 1
				}
			}
			if test_tying_y == 0 && test_tying_x != 0 {
				ties_onlyin_y += 1
			} else {
				if test_tying_x == 0 && test_tying_y != 0 {
					ties_onlyin_x += 1
				}
			}
		}
	}
	return (con_pair - dis_pair) / math.Sqrt((con_pair+dis_pair+ties_onlyin_x)*(dis_pair+con_pair+ties_onlyin_y))
}

// kendallAsymptotic kendall近似计算方法
func kendallAsymptotic(label, x []float64, reuseY, reuseX, reuseZ []float64, reuseOBS []bool) float64 {
	size := float64(len(x))
	//因为这里涉及对y的排序，所以需要复制y,否则会影响原本顺序
	//TODO 这块看看有没有好办法，但是其实损耗已经不大了
	y := make([]float64, len(x))
	for i := 0; i < len(x); i++ {
		y[i] = label[i]
	}
	sort.Sort(sortSlices{y, x})
	dislocationEqualitySUM(y, reuseY)
	sort.Sort(sortSlices{x, reuseY})
	dislocationEqualitySUM(x, reuseX)
	dis := kendallDis(reuseX, reuseY)
	dislocationEqualityARRAY(reuseX, reuseY, reuseOBS)

	cnt, length := countGapOfTrue(reuseOBS, reuseZ)
	ntie := jointTies(cnt, length)
	xtie, _, _ := countRankTie(reuseX, reuseZ)
	ytie, _, _ := countRankTie(reuseY, reuseZ)
	//xtie, x0, x1 := countRankTie(reuseX)
	//ytie, y0, y1 := countRankTie(reuseY)
	tot := (size * (size - 1)) / 2
	if xtie == tot || ytie == tot {
		return math.NaN()
	}
	con_minus_dis := tot - xtie - ytie + ntie - 2*dis
	tau := float64(con_minus_dis) / math.Sqrt(float64(tot)-float64(xtie)) / math.Sqrt(float64(tot)-float64(ytie))
	tau = math.Min(1.0, math.Max(-1.0, tau))
	//Pvalue 计算方法
	//temp_var := (float64(size)*(float64(size)-1)*(2.*float64(size)+5)-float64(x1)-float64(y1))/18. + (2.*float64(size)*float64(size))/(float64(size)*(float64(size)-1)) + float64(x0)*float64(y0)/(9.*float64(size)*(float64(size)-1.0)*(float64(size)-2.0))
	//result := math.Erfc(math.Abs(float64(con_minus_dis)) / math.Sqrt(temp_var) / math.Sqrt(2))
	return tau
}

// dislocationEqualitySUM 错位相等,累加相等值
func dislocationEqualitySUM(a []float64, reuse []float64) {
	count := 1.0
	reuse[0] = count
	length := len(a)
	for i := 1; i < length; i++ {
		if a[i] != a[i-1] {
			count += 1
		}
		reuse[i] = count
	}
}

// dislocationEqualityARRAY 错位相等,返回数组
func dislocationEqualityARRAY(a, b []float64, reuseOBS []bool) {
	reuseOBS[0] = true
	reuseOBS[len(a)] = true
	length := len(a)
	for i := 1; i < length; i++ {
		reuseOBS[i] = a[i] != a[i-1] || b[i] != b[i-1]
	}
}

// jointTies
func jointTies(a []float64, length int) float64 {
	sum := float64(0)
	for i := 0; i < length; i++ {
		sum += (a[i]*a[i] - 1) / 2
	}
	return sum
}

// countRankTie
func countRankTie(a, reuseZ []float64) (float64, float64, float64) {
	//arr := make([]float64, 0)
	length := 0.0
	index := 0
	for i := 0; i < len(a); i++ {
		if a[i] >= length {
			for j := 0; j < int(a[i]+1-length); j++ {
				//arr = append(arr, 0)
				reuseZ[index] = 0
				index += 1
			}
			length += a[i] + 1 - length
		}
		reuseZ[int(a[i])] += 1
		//arr[int(a[i])] += 1
	}
	n1 := 0.0
	n2 := 0.0
	n3 := 0.0
	for i := 0; i < index; i++ {
		if reuseZ[i] > 1 {
			cnt := reuseZ[i]
			/*
				return ((cnt * (cnt - 1) // 2).sum(),
				(cnt * (cnt - 1.) * (cnt - 2)).sum(),
				(cnt * (cnt - 1.) * (2*cnt + 5)).sum())
			*/
			n1 += cnt * (cnt - 1) / 2
			n2 += cnt * (cnt - 1.) * (cnt - 2)
			n3 += cnt * (cnt - 1.) * (2*cnt + 5)
		}
	}
	return n1, n2, n3
}

func countRankTieV1(a []float64) (float64, float64, float64) {
	arr := make([]float64, 0)
	length := 0.0
	for i := 0; i < len(a); i++ {
		if a[i] >= length {
			for j := 0; j < int(a[i]+1-length); j++ {
				arr = append(arr, 0)
			}
			length += a[i] + 1 - length
		}
		arr[int(a[i])] += 1
	}
	n1 := 0.0
	n2 := 0.0
	n3 := 0.0
	for i := 0; i < len(arr); i++ {
		if arr[i] > 1 {
			cnt := arr[i]
			/*
				return ((cnt * (cnt - 1) // 2).sum(),
				(cnt * (cnt - 1.) * (cnt - 2)).sum(),
				(cnt * (cnt - 1.) * (2*cnt + 5)).sum())
			*/
			n1 += cnt * (cnt - 1) / 2
			n2 += cnt * (cnt - 1.) * (cnt - 2)
			n3 += cnt * (cnt - 1.) * (2*cnt + 5)
		}
	}
	return n1, n2, n3
}

// kendallDis
func kendallDis(x, y []float64) float64 {
	/*
		这里也看不懂
		intp_t sup = 1 + np.max(y)
		# Use of `>> 14` improves cache performance of the Fenwick tree (see gh-10108)
		intp_t[::1] arr = np.zeros(sup + ((sup - 1) >> 14), dtype=np.intp)
	*/
	sup := int64(1 + max(y))
	arr := make([]int64, sup+((sup-1)>>14))
	i := int64(0)
	k := int64(0)
	size := int64(len(x))
	idx := int64(0)
	dis := int64(0)
	for {
		for {
			dis += i
			idx = int64(y[k])
			for {
				dis -= arr[idx+(idx>>14)]
				idx = idx & (idx - 1)
				if idx == 0 {
					break
				}
			}
			k += 1
			if k >= size || x[i] != x[k] {
				break
			}

		}
		for {
			idx = int64(y[i])
			for {
				arr[idx+(idx>>14)] += 1
				idx += idx & -idx
				if idx >= sup {
					break
				}

			}
			i += 1
			if i >= k {
				break
			}
		}
		if i >= size {
			break
		}
	}
	return float64(dis)
}

// countGapOfTrue
// TODO ARR 可以复用
func countGapOfTrue(obs []bool, reuseZ []float64) ([]float64, int) {
	count := 1.0
	index := 0
	//arr := make([]float64, 0)
	for i := 1; i < len(obs); i++ {
		if obs[i] == true {
			//arr = append(arr, count)
			reuseZ[index] = count
			index += 1
			count = 0
		}
		count += 1
	}
	return reuseZ, index
}

// countGapOfTrue
func countGapOfTrueV1(obs []bool) []float64 {
	count := 1.0
	arr := make([]float64, 0)
	for i := 1; i < len(obs); i++ {
		if obs[i] == true {
			arr = append(arr, count)
			count = 0
		}
		count += 1
	}
	return arr
}

// 排序用结构
type sortSlices struct {
	sortBy []float64
	other  []float64
}

func (s sortSlices) Len() int {
	return len(s.sortBy)
}
func (s sortSlices) Less(i int, j int) bool {
	return (s.sortBy)[i] < (s.sortBy)[j]
}
func (s sortSlices) Swap(i int, j int) {
	(s.sortBy)[i], (s.sortBy)[j] = (s.sortBy)[j], (s.sortBy)[i]
	(s.other)[i], (s.other)[j] = (s.other)[j], (s.other)[i]
}
