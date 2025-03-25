package list_cluster

import (
	"fmt"
	"gitlab.grandhoo.com/rock/rock_v3/utils/mapi32"
	"testing"
)

func TestLiCluster_NewList(t *testing.T) {
	liC := New[int]()

	head := liC.NewList(1)

	liC.AppendList(head, 2)
	head2 := liC.NewList(11)
	liC.AppendList(head2, 33)
	liC.AppendList(head2, 22)
	liC.AppendList(head, 3)
	liC.AppendList(head, 4)
	liC.AppendList(head2, 44)

	liC.ForeachValue(head, func(value int) {
		fmt.Println(value)
	})
	liC.ForeachValue(head2, func(value int) {
		fmt.Println(value)
	})

	liC.Clean()

	head = liC.NewList(12)
	liC.AppendList(head, 3)
	liC.AppendList(head, 4)
	liC.ForeachValue(head, func(value int) {
		fmt.Println(value)
	})
	liC.ForeachValue(head, func(value int) {
		fmt.Println(value)
	})
}

const size = 10

func BenchmarkMapAppend(b *testing.B) {
	for i := 0; i < b.N; i++ {
		m := make(map[int32][]string, size)
		for k := int32(0); k < size; k++ {
			m[k] = append(m[k], "a")
		}
		for k := int32(0); k < size; k++ {
			m[k] = append(m[k], "a")
		}
	}
}

func BenchmarkMapAppendList(b *testing.B) {
	li := NewCapacity[string](128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m := make(map[int32]Head, size)
		for k := int32(0); k < size; k++ {
			head, ok := m[k]
			if !ok {
				head = li.NewList("a")
			} else {
				li.AppendList(head, "a")
			}
			m[k] = head
		}
		for k := int32(0); k < size; k++ {
			head, ok := m[k]
			if !ok {
				head = li.NewList("a")
			} else {
				li.AppendList(head, "a")
			}
			m[k] = head
		}
		li.Clean()
	}
}

func BenchmarkMapI32Append(b *testing.B) {
	table := make([]mapi32.Entry[[]string], size*3)
	tablePtr := &table
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m := mapi32.NewUseArray[[]string](size, tablePtr)
		for k := int32(0); k < size; k++ {
			s := m.Get1(k)
			s = append(s, "a")
			m.Put(k, s)
		}
		for k := int32(0); k < size; k++ {
			s := m.Get1(k)
			s = append(s, "a")
			m.Put(k, s)
		}
	}
}

func BenchmarkMapI32AppendList(b *testing.B) {
	table := make([]mapi32.Entry[Head], size*3)
	tablePtr := &table
	li := NewCapacity[string](128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m := mapi32.NewUseArray[Head](size, tablePtr)
		for k := int32(0); k < size; k++ {
			s, ok := m.Get2(k)
			if !ok {
				s = li.NewList("a")
			} else {
				li.AppendList(s, "a")
			}
			m.Put(k, s)
		}
		for k := int32(0); k < size; k++ {
			s, ok := m.Get2(k)
			if !ok {
				s = li.NewList("a")
			} else {
				li.AppendList(s, "a")
			}
			m.Put(k, s)
		}
		li.Clean()
	}
}

func TestMapAppend(t *testing.T) {
	m := make(map[int32][]string, 10)
	for k := int32(0); k < 10; k++ {
		m[k] = append(m[k], "a")
	}
	for k := int32(0); k < 10; k++ {
		m[k] = append(m[k], "a")
	}
	fmt.Printf("%v", m)
}

func TestMapI32AppendList(t *testing.T) {
	table := make([]mapi32.Entry[Head], 10)
	tablePtr := &table
	li := NewCapacity[string](128)

	m := mapi32.NewUseArray[Head](10, tablePtr)
	for k := int32(0); k < 10; k++ {
		s, ok := m.Get2(k)
		if !ok {
			s = li.NewList("a")
		} else {
			li.AppendList(s, "a")
		}
		m.Put(k, s)
	}
	for k := int32(0); k < 10; k++ {
		s, ok := m.Get2(k)
		if !ok {
			s = li.NewList("a")
		} else {
			li.AppendList(s, "a")
		}
		m.Put(k, s)
	}
	m.Foreach(func(key int32, val Head) {
		fmt.Print(key, "->")
		li.ForeachValue(val, func(value string) {
			fmt.Print(value, ",")
		})
		fmt.Println()
	})
	li.Clean()
}

func TestLiCluster_ToSlice(t *testing.T) {
	liC := New[int]()

	head := liC.NewList(1)

	liC.AppendList(head, 2)
	head2 := liC.NewList(11)
	liC.AppendList(head2, 33)
	liC.AppendList(head2, 22)
	liC.AppendList(head, 3)
	liC.AppendList(head, 4)

	liC.ForeachValue(head, func(value int) {
		fmt.Println(value)
	})
	liC.ForeachValue(head2, func(value int) {
		fmt.Println(value)
	})

	fmt.Printf("Length %v\n", liC.Length(head))
	fmt.Printf("Length %v\n", liC.Length(head2))

	fmt.Printf("Length %v\n", liC.ToSlice(head))
	fmt.Printf("Length %v\n", liC.ToSlice(head2))
}
