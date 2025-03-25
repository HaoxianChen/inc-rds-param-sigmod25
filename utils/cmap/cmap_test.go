package cmap

import (
	"golang.org/x/exp/maps"
	"strconv"
	"testing"
)

func BenchmarkConcurrentMap_MSetUnThreadSafe(b *testing.B) {
	var m = make(map[string]string, b.N)
	for i := 0; i < b.N; i++ {
		ii := strconv.Itoa(i)
		m[ii] = ii
	}
	var cm = New[string]()
	b.ResetTimer()
	cm.MSetUnThreadSafe(m)
	b.StopTimer()
	if !maps.Equal(m, cm.Items()) {
		panic("!!!")
	}
}

func BenchmarkConcurrentMap_MSetUnThreadSafeParallel(b *testing.B) {
	var m = make(map[string]string, b.N)
	for i := 0; i < b.N; i++ {
		ii := strconv.Itoa(i)
		m[ii] = ii
	}
	var cm = New[string]()
	b.ResetTimer()
	cm.MSetUnThreadSafeParallel(m)
	b.StopTimer()
	if !maps.Equal(m, cm.Items()) {
		panic("!!!")
	}
}
