package gchattest

import (
	"context"
	"sync"
	"time"
)

// Clock is a fake clock: Sleep returns at once and moves Now forward by the slept duration.
type Clock struct {
	mu     sync.Mutex
	now    time.Time
	sleeps []time.Duration
}

// NewClock starts a fake clock at a fixed instant.
func NewClock() *Clock {
	return &Clock{now: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}
}

// Now returns the fake time.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Sleep advances the clock by d without waiting; it fails only when ctx is done.
func (c *Clock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.sleeps = append(c.sleeps, d)
	c.mu.Unlock()
	return nil
}

// Advance moves the clock forward by d.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
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
	return c.Now().Sub(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
}
