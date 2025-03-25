package correlation_util

import "math"

/*
   基础计算
*/

// mean 均值
func mean(v []float64) float64 {
	var res float64 = 0
	var n = len(v)
	for i := 0; i < n; i++ {
		res += v[i]
	}
	return res / float64(n)
}

// variance 方差
func variance(v []float64, m float64) float64 {
	var res float64 = 0
	var n = len(v)
	for i := 0; i < n; i++ {
		res += (v[i] - m) * (v[i] - m)
	}
	return res / float64(n-1)
}

// std 标准差
func std(v []float64, m float64) float64 {
	return math.Sqrt(variance(v, m))
}

// sign 取数字符号
func sign(a float64) float64 {
	if a > 0 {
		return 1.0
	} else {
		if a < 0 {
			return -1.0
		} else {
			return 0.0
		}
	}
}

// max 最大值
func max(a []float64) float64 {
	m := a[0]
	for i := 1; i < len(a); i++ {
		if m < a[i] {
			m = a[i]
		}
	}
	return m
}

func nullRatio(a []float64) float64 {
	length := float64(len(a))
	count := 0.
	for _, v := range a {
		if math.IsNaN(v) {
			count += 1.
		}
	}
	return count / length
}
