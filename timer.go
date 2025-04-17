package nsqite

import (
	"sync"
	"time"
)

var timerPool = sync.Pool{
	New: func() any {
		return time.NewTimer(0)
	},
}

func getTimer(d time.Duration) *time.Timer {
	t := timerPool.Get().(*time.Timer)
	t.Reset(d)
	return t
}

// putTimer 将计时器放回池中
func putTimer(t *time.Timer) {
	timerPool.Put(t)
}
