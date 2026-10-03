package coordinate

import (
	"sync"
	"time"
)

// ManualClock 是测试用的手动时钟：时间只在 Advance 时前进。
// 领导者盖章时间（Op.At）取自该时钟，因此同一份"推进脚本 + 操作脚本"
// 在任意次独立运行中都会产生完全一致的历史（含时间戳）。
type ManualClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*manualTimer
}

type manualTimer struct {
	at    time.Time
	ch    chan time.Time
	fired bool
}

// NewManualClock 创建从 start 开始的手动时钟。
func NewManualClock(start time.Time) *ManualClock {
	return &ManualClock{now: start}
}

// Now 返回当前（手动）时间。
func (c *ManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// After 注册一个在手动时间前进 d 后触发的定时器。
func (c *ManualClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &manualTimer{at: c.now.Add(d), ch: make(chan time.Time, 1)}
	c.timers = append(c.timers, t)
	return t.ch
}

// Sleep 阻塞直到手动时间前进 d（需要其他 goroutine 调用 Advance）。
func (c *ManualClock) Sleep(d time.Duration) {
	<-c.After(d)
}

// Advance 把手动时间前进 d，并触发所有到期的定时器。
func (c *ManualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	keep := c.timers[:0]
	for _, t := range c.timers {
		if !t.fired && !t.at.After(c.now) {
			t.fired = true
			t.ch <- c.now // 缓冲为 1，绝不阻塞
		}
		if !t.fired {
			keep = append(keep, t)
		}
	}
	c.timers = keep
}
