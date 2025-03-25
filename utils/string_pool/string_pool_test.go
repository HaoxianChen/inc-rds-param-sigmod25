package string_pool

import (
	"fmt"
	"strconv"
	"testing"
)

func TestNewCap(t *testing.T) {
	pool := NewCap(10)
	fmt.Println(pool.Intern("aaa"))
	fmt.Println(pool.Intern("hello"))

	for i := 0; i < 102400; i++ {
		pool.Intern(strconv.Itoa(i))
	}
	fmt.Println(len(pool.ss))
	fmt.Println(len(pool.ms.ms))
}
