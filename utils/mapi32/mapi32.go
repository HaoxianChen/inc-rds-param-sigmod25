package mapi32

import (
	"fmt"
	"reflect"
	"unsafe"
)

type Map[T any] struct {
	free int32
	mask int32
	data []Entry[T] // 前 mask 个为 table，后面为链表
}

type Entry[T any] struct {
	key  int32
	next int32
	val  T
}

func New[T any](tableSize int32) Map[T] {
	var size int32 = 1
	for size < tableSize {
		size <<= 2
	}
	size <<= 2

	return Map[T]{
		data: make([]Entry[T], size+tableSize),
		free: size,
		mask: size - 1,
	}
}

func NewUseArray[T any](tableSize int32, array *[]Entry[T]) Map[T] {
	var size int32 = 1
	for size < tableSize {
		size <<= 2
	}
	size <<= 2

	growLen2Cap(array)

	arrSize := size + tableSize
	if arrSize > int32(len(*array)) {
		*array = make([]Entry[T], arrSize)
	} else {
		*array = (*array)[:arrSize]
	}

	m := Map[T]{
		data: *array,
		free: size,
		mask: size - 1,
	}

	if arrSize <= int32(len(*array)) {
		m.Clean()
	}

	return m
}

func growLen2Cap[E any](slice *[]E) {
	r := (*reflect.SliceHeader)(unsafe.Pointer(slice))
	r.Len = r.Cap
}

func NewP[T any](tableSize int32) *Map[T] {
	var size int32 = 1
	for size < tableSize {
		size <<= 2
	}
	size <<= 2

	return &Map[T]{
		data: make([]Entry[T], size+tableSize),
		free: size,
		mask: size - 1,
	}
}

func (m *Map[T]) Put(k int32, v T) {
	loc := k & m.mask
	next := m.data[loc].next
	if next == 0 { // empty
		m.data[loc].key = k
		m.data[loc].next = -1
		m.data[loc].val = v
	} else if next == -1 { // only one, no link
		if m.data[loc].key == k {
			m.data[loc].val = v
		} else { // add link
			m.data[loc].next = m.free
			m.data[m.free].key = k
			m.data[m.free].next = -1 // end
			m.data[m.free].val = v
			m.free++
		}
	} else { // has link
		if m.data[loc].key == k {
			m.data[loc].val = v
		} else {
			for {
				if m.data[next].key == k {
					m.data[next].val = v
					return
				}
				if m.data[next].next == -1 { // end
					m.data[next].next = m.free
					m.data[m.free].key = k
					m.data[m.free].next = -1 // end
					m.data[m.free].val = v
					m.free++
					return
				}
				next = m.data[next].next
			}
		}
	}
}

func (m *Map[T]) DirectPut(k int32, v T) {
	loc := k & m.mask

	next := m.data[loc].next
	if next == 0 { // empty
		m.data[loc].key = k
		m.data[loc].next = -1
		m.data[loc].val = v
	} else if next == -1 { // only one, no link
		//if m.data[loc].key == k {
		//	m.data[loc].val = v
		//} else { // add link
		m.data[loc].next = m.free
		m.data[m.free].key = k
		m.data[m.free].next = -1 // end
		m.data[m.free].val = v
		m.free++
		//}
	} else { // has link
		//if m.data[loc].key == k {
		//	m.data[loc].val = v
		//} else {
		for {
			//if m.data[next].key == k {
			//	m.data[next].val = v
			//	return
			//}
			if m.data[next].next == -1 { // end
				m.data[next].next = m.free
				m.data[m.free].key = k
				m.data[m.free].next = -1 // end
				m.data[m.free].val = v
				m.free++
				return
			}
			next = m.data[next].next
		}
		//}
	}
}

func (m *Map[T]) Get2(k int32) (val T, ok bool) {
	loc := k & m.mask
	next := m.data[loc].next
	if next == 0 {
		return val, false
	} else if next == -1 {
		if m.data[loc].key == k {
			return m.data[loc].val, true
		} else {
			return val, false
		}
	} else {
		if m.data[loc].key == k {
			return m.data[loc].val, true
		} else {
			for {
				if m.data[next].key == k {
					return m.data[next].val, true
				}
				if m.data[next].next == -1 {
					return val, false
				}
				next = m.data[next].next
			}
		}
	}
}

func (m *Map[T]) Get1(k int32) (val T) {
	loc := k & m.mask
	next := m.data[loc].next
	if next <= 0 {
		if m.data[loc].key == k {
			if next == 0 {
				return val
			} else {
				return m.data[loc].val
			}
		} else {
			return val
		}
	} else {
		if m.data[loc].key == k {
			return m.data[loc].val
		} else {
			for {
				if m.data[next].key == k {
					return m.data[next].val
				}
				if m.data[next].next == -1 {
					return val
				}
				next = m.data[next].next
			}
		}
	}
}

func (m *Map[T]) Clean() {
	for i := int32(0); i <= m.mask; i++ {
		//m.data[i] = Entry[T]{}
		*((*int64)(unsafe.Pointer(&m.data[i]))) = 0
	}
	m.free = m.mask + 1
}

func (m *Map[T]) NotInit() bool {
	return len(m.data) == 0
}

func (m *Map[T]) TableSize() int32 {
	if len(m.data) == 0 {
		return 0
	}
	return m.mask + 1
}

func (m *Map[T]) Foreach(consumer func(key int32, val T)) {
	for i := int32(0); i <= m.mask; i++ {
		entry := m.data[i]
		nx := entry.next
		if nx != 0 {
			consumer(entry.key, entry.val)
			for nx != -1 {
				consumer(m.data[nx].key, m.data[nx].val)
				nx = m.data[nx].next
			}
		}
	}
}

func (m *Map[T]) Print() {
	for i := int32(0); i <= m.mask; i++ {
		entry := m.data[i]
		nx := entry.next
		if nx == 0 {
			fmt.Printf("%d[]\n", i)
		} else if nx == -1 {
			fmt.Printf("%d[%d:%v]\n", i, entry.key, entry.val)
		} else {
			fmt.Printf("%d[%d:%v]", i, entry.key, entry.val)
			for nx != -1 {
				fmt.Printf("->%d:%v", m.data[nx].key, m.data[nx].val)
				nx = m.data[nx].next
			}
			fmt.Println()
		}
	}
}
