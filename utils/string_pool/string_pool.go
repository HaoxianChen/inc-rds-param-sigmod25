package string_pool

import (
	"reflect"
	"unsafe"
)

/**
字符串池
复用字符串，减少内存碎片
调用方法即 Intern
*/

const blkSize = 32 * 1024 * 1024 // 32MB

type Pool struct {
	ss map[string]string
	ms memory
}

type memory struct {
	ms   [][]byte
	free int
}

func NewCap(cap int) *Pool {
	return &Pool{
		ss: make(map[string]string, cap),
		ms: memory{
			ms:   [][]byte{make([]byte, blkSize)},
			free: 0,
		},
	}
}

// Intern 即 Java 代码 String::intern
func (p *Pool) Intern(s string) string {
	if pooled, ok := p.ss[s]; ok {
		return pooled
	} else {
		sv := p.ms.RAII(s)
		p.ss[sv] = sv
		return sv
	}
	//sb := strings.Builder{}
	//sb.WriteString(s)
	//return sb.String()
}

func (m *memory) RAII(s string) string {
	blk := m.ms[len(m.ms)-1]
	if m.free+len(s) > blkSize {
		blk = make([]byte, blkSize)
		m.ms = append(m.ms, blk)
		m.free = 0
	}

	mem := blk[m.free:]
	copy(mem, s)
	sv := stringView(mem[:len(s)])
	m.free += len(s)
	return sv
}

func stringView(data []byte) string {
	head := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	strH := reflect.StringHeader{
		Data: head.Data,
		Len:  head.Len,
	}
	return *(*string)(unsafe.Pointer(&strH))
}
