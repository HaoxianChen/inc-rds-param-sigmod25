package corr

import "math"

// pearsonV1 第一种实现 较快，内存消耗大
func pearsonV1(a, b []float64) float64 {
	a_mean := mean(a)
	b_mean := mean(b)
	a_std := std(a, a_mean)
	b_std := std(b, b_mean)
	r := 0.0
	length := float64(len(a))
	for i := 0; i < len(a); i++ {
		a_z := (a[i] - a_mean) / a_std
		b_z := (b[i] - b_mean) / b_std
		r += (a_z * b_z)
	}
	return r / length
}

// pearsonV2 第二种实现 较慢，内存消耗小
func pearsonV2(data1, data2 []float64) float64 {
	var sum [5]float64

	var n = float64(len(data1))
	for i := 0; i < len(data1); i++ {
		x := data1[i]
		y := data2[i]

		sum[0] += x * y
		sum[1] += x
		sum[2] += y
		sum[3] += math.Pow(x, 2)
		sum[4] += math.Pow(y, 2)
	}

	sqrtX := math.Sqrt(sum[3] - (math.Pow(sum[1], 2) / n))
	sqrtY := math.Sqrt(sum[4] - (math.Pow(sum[2], 2) / n))

	dividend := sum[0] - ((sum[1] * sum[2]) / n)
	divisor := sqrtX * sqrtY

	return dividend / divisor
}

// pearsonV3 第三种实现，可处理空值
func pearsonV3(a, b []float64) float64 {
	nobs := 0.0
	ssqdmx := 0.0
	ssqdmy := 0.0
	covxy := 0.0
	meanx := 0.0
	meany := 0.0
	for i := 0; i < len(a); i++ {
		vx := a[i]
		vy := b[i]
		if math.IsNaN(vx) || math.IsNaN(vy) {
			continue
		}

		nobs += 1
		dx := vx - meanx
		dy := vy - meany
		meanx += 1. / nobs * dx
		meany += 1. / nobs * dy
		ssqdmx += (vx - meanx) * dx
		ssqdmy += (vy - meany) * dy
		covxy += (vx - meanx) * dy
	}
	if nobs < 5 {
		return math.NaN()
	}
	divisor := math.Sqrt(ssqdmx * ssqdmy)
	if divisor != 0 {
		return covxy / divisor
	}
	return math.NaN()
}
