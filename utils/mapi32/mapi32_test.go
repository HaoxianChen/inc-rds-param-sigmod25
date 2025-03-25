package mapi32

import (
	"github.com/kelindar/intmap"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"math/rand"
	"testing"
)

func TestNew(t *testing.T) {
	m := New[string](4)
	m.DirectPut(0, "a")
	m.DirectPut(1, "a")
	m.DirectPut(2, "a")
	m.DirectPut(3, "a")
	m.DirectPut(4, "a")
	m.DirectPut(5, "a")
	m.DirectPut(8, "a")

	m.Put(3, "q")
	m.Put(5, "b")
	m.Put(4, "c")
	m.Put(8, "d")
	m.Put(12, "k")

	for i := 0; i < 16; i++ {
		println(i, m.Get1(int32(i)))
	}

	m.Print()
}

func TestNewCap(t *testing.T) {
	var arr []Entry[string]
	m := NewUseArray[string](16, &arr)
	m.DirectPut(0, "a")
	m.DirectPut(1, "a")
	m.DirectPut(2, "a")
	m.DirectPut(3, "a")
	m.DirectPut(4, "a")
	m.DirectPut(5, "a")
	m.DirectPut(8, "a")

	m.Put(3, "q")
	m.Put(5, "b")
	m.Put(4, "c")
	m.Put(8, "d")
	m.Put(12, "k")

	for i := 0; i < 16; i++ {
		println(i, m.Get1(int32(i)))
	}

	m.Print()
}

const sz = 100_0000

func BenchmarkMap(b *testing.B) {
	rand.Seed(1)
	m := make(map[int32]string, sz)
	for i := int32(0); i < sz; i++ {
		m[rand.Int31()] = ""
	}
	keys := make([]int32, b.N)
	for i := int32(0); i < int32(b.N); i++ {
		keys[i] = rand.Int31()
	}
	b.ResetTimer()
	for _, k := range keys {
		_ = m[k]
	}
}

func BenchmarkMapI32String(b *testing.B) {
	rand.Seed(1)
	m := New[string](sz)
	for i := int32(0); i < sz; i++ {
		m.Put(rand.Int31(), "")
	}
	keys := make([]int32, b.N)
	for i := int32(0); i < int32(b.N); i++ {
		keys[i] = rand.Int31()
	}
	b.ResetTimer()
	for _, k := range keys {
		_ = m.Get1(k)
	}
}

func BenchmarkMapI32Uint32(b *testing.B) {
	rand.Seed(1)
	m := New[uint32](sz)
	keys := make([]int32, b.N)
	for i := int32(0); i < int32(b.N); i++ {
		keys[i] = rand.Int31()
	}
	for i := int32(0); i < sz; i++ {
		m.Put(rand.Int31(), 0)
	}
	b.ResetTimer()
	for _, k := range keys {
		_ = m.Get1(k)
	}
}

func BenchmarkUMap(b *testing.B) {
	rand.Seed(1)
	m := intmap.New(sz, 0.5)
	keys := make([]uint32, b.N)
	for i := int32(0); i < int32(b.N); i++ {
		keys[i] = uint32(rand.Int31())
	}
	for i := int32(0); i < sz; i++ {
		m.Store(uint32(rand.Int31()), 0)
	}
	b.ResetTimer()
	for _, k := range keys {
		_, _ = m.Load(k)
	}
}

func TestMap_Clean(t *testing.T) {
	m := New[string](4)
	m.DirectPut(0, "a")
	m.DirectPut(1, "a")
	m.DirectPut(2, "a")
	m.DirectPut(3, "a")
	m.DirectPut(4, "a")
	m.DirectPut(5, "a")
	m.DirectPut(8, "a")

	m.Put(3, "q")
	m.Put(5, "b")
	m.Put(4, "c")
	m.Put(8, "d")
	m.Put(12, "k")

	m.Print()
	m.Foreach(func(key int32, val string) {
		println(key, val)
	})

	m.Clean()
	m.Put(3, "q")
	m.Put(5, "b")
	m.Put(4, "c")
	m.Put(8, "d")
	m.Put(12, "k")

	m.Print()
	m.Foreach(func(key int32, val string) {
		println(key, val)
	})

}

func BenchmarkClean(b *testing.B) {
	m := New[[]int](8192)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Clean()
	}
}

func Test_growLen2Cap(t *testing.T) {
	s := make([]int, 0, 10)
	logger.Info(len(s), " ", cap(s), " ", s)
	growLen2Cap(&s)
	logger.Info(len(s), " ", cap(s), " ", s)
}
