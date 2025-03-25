package add

import (
	"testing"
	"time"
)

func TestAdd(t *testing.T) {
	start := time.Now()
	adder1 := NewFloatAdder()
	for i := 2_0000_0000; i >= 0; i-- {
		adder1.Add(float64(i))
	}
	t.Log(adder1.Result(), time.Since(start))

	start = time.Now()
	sum1 := 0.0
	for i := 2_0000_0000; i >= 0; i-- {
		sum1 += float64(i)
	}
	t.Log(sum1, time.Since(start))
	// 10X

	start = time.Now()
	adder2 := NewFloatAdder()
	for i := 20_0000_0000; i >= 0; i-- {
		adder2.Add(float64(i))
	}
	t.Log(adder2.Result(), time.Since(start))

	start = time.Now()
	sum2 := 0.0
	for i := 20_0000_0000; i >= 0; i-- {
		sum2 += float64(i)
	}
	t.Log(sum2, time.Since(start))

	t.Log(sum1 + sum2)
	t.Log(adder1.Merge(*adder2).Result())
}
