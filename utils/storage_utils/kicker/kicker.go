package kicker

import (
	"runtime"
	"sync"
	"time"
)

type Kicker struct {
	mu   sync.Mutex
	tick time.Duration
	cnt  int
}

func Init(tick time.Duration) *Kicker {
	kicker := &Kicker{
		mu:   sync.Mutex{},
		tick: tick,
		cnt:  0,
	}
	go func() {
		for true {
			kicker.Kick()
			time.Sleep(tick)
		}
	}()

	return kicker
}

func (k *Kicker) Kick() {
	k.mu.Lock()
	k.cnt++
	k.mu.Unlock()
}

func (k *Kicker) WaitKicked() {
retry:
	k.mu.Lock()
	if k.cnt <= 0 {
		k.mu.Unlock()
		runtime.Gosched()
		goto retry
	} else {
		k.cnt--
		k.mu.Unlock()
	}

}
