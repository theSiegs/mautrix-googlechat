package gchattest

import (
	"context"
	"sync"
	"time"
)

// clockStart is the instant every fake clock starts at.
var clockStart = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

// DefaultAutoAdvance is the longest Sleep a Clock skips over by itself.
const DefaultAutoAdvance = 3 * time.Hour

// Clock is a fake clock. A Sleep up to AutoAdvance returns at once and moves Now forward by the
// slept duration; a longer one (the daily refresh) waits until Advance moves Now past its deadline.
type Clock struct {
	// AutoAdvance may be changed before the clock is used.
	AutoAdvance time.Duration

	mu     sync.Mutex
	now    time.Time
	sleeps []time.Duration
	wake   chan struct{}
}

// NewClock starts a fake clock at a fixed instant.
func NewClock() *Clock {
	return &Clock{AutoAdvance: DefaultAutoAdvance, now: clockStart, wake: make(chan struct{})}
}

// Now returns the fake time.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Sleep records d and returns once the fake time has passed now+d, or with the error of a done ctx.
func (c *Clock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	c.sleeps = append(c.sleeps, d)
	if d <= c.AutoAdvance {
		c.now = c.now.Add(d)
		c.mu.Unlock()
		return nil
	}
	deadline := c.now.Add(d)
	for c.now.Before(deadline) {
		wake := c.wake
		c.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return ctx.Err()
		}
		c.mu.Lock()
	}
	c.mu.Unlock()
	return nil
}

// Advance moves the clock forward by d and wakes sleepers whose deadline has passed.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	close(c.wake)
	c.wake = make(chan struct{})
	c.mu.Unlock()
}

// Sleeps returns every duration passed to Sleep so far.
func (c *Clock) Sleeps() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.sleeps...)
}

// Elapsed is how far the clock has moved since NewClock.
func (c *Clock) Elapsed() time.Duration {
	return c.Now().Sub(clockStart)
}
