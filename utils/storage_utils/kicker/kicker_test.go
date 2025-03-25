package kicker

import (
	"fmt"
	"testing"
	"time"
)

func TestInit(t *testing.T) {
	k := Init(1 * time.Second)
	for i := 0; i < 10; i++ {
		k.WaitKicked()
		fmt.Println(time.Now())
	}
}
