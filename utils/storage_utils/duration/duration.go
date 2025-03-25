package duration

import (
	"gitlab.grandhoo.com/rock/storage/storage2/utils"
	"sync"
	"time"
)

type Duration struct {
	initTime   int64 // ns
	tick       int64 // ns
	accumulate int64 // ns
	reentrant  int
	mu         sync.RWMutex
}

func (d *Duration) Enter() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.reentrant == 0 {
		d.tick = time.Now().UnixNano()
		if d.initTime == 0 {
			d.initTime = d.tick
		}
	}
	d.reentrant++
}

func (d *Duration) Exit() {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.reentrant--
	if d.reentrant == 0 {
		d.accumulate += time.Now().UnixNano() - d.tick
	}
}

func (d *Duration) DurationString() string {
	return utils.PeriodString(time.Now().UnixNano() - d.initTime)
}

func (d *Duration) AccumulationString() string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.reentrant == 0 {
		return utils.PeriodString(d.accumulate)
	} else {
		return utils.PeriodString(d.accumulate + time.Now().UnixNano() - d.tick)
	}
}
