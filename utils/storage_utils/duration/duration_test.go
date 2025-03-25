package duration

import (
	"fmt"
	"testing"
	"time"
)

func TestInit(t *testing.T) {
	var d Duration
	d.Enter()
	time.Sleep(1 * time.Second)
	d.Exit()
	fmt.Println(d.DurationString())
	fmt.Println(d.AccumulationString())

	time.Sleep(500 * time.Millisecond)

	d.Enter()
	time.Sleep(120 * time.Millisecond)
	fmt.Println(d.DurationString())
	fmt.Println(d.AccumulationString())
}
