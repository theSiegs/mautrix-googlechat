package gchatmeow

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"
)

func within(d, nominal time.Duration) bool {
	lo := time.Duration(float64(nominal) * (1 - jitterFraction))
	hi := time.Duration(float64(nominal) * (1 + jitterFraction))
	return d >= lo && d <= hi
}

func TestJitterStaysWithinTwentyPercent(t *testing.T) {
	seeded := rand.New(rand.NewSource(1))
	c := &Client{randFn: seeded.Float64}
	const nominal = 90 * time.Minute
	seen := map[time.Duration]bool{}
	for i := 0; i < 1000; i++ {
		d := c.jitter(nominal)
		if !within(d, nominal) {
			t.Fatalf("jitter(%v) = %v, outside ±20%%", nominal, d)
		}
		seen[d] = true
	}
	if len(seen) < 900 {
		t.Errorf("only %d distinct values in 1000 draws; the interval is not being spread", len(seen))
	}
	c.randFn = func() float64 { return 0 }
	if got := c.jitter(nominal); got != 72*time.Minute {
		t.Errorf("jitter at the low end = %v, want 72m", got)
	}
	c.randFn = nil
	if got := c.jitter(nominal); got != nominal {
		t.Errorf("jitter with no rand source = %v, want the nominal value", got)
	}
}

// TestBackoffIsJitteredAndResetsAfterConnected: the ladder restarts once a channel reached Connected.
func TestBackoffIsJitteredAndResetsAfterConnected(t *testing.T) {
	netErr := &NetworkError{Err: errors.New("connection reset")}
	connectedThenDrop := func(f *scriptedChannel, ctx context.Context) error {
		f.onConnect(ctx)
		return netErr
	}
	fake := newScriptedChannel(ret(netErr), ret(netErr), ret(netErr), connectedThenDrop, ret(netErr), ret(netErr))
	rec := &stateRecorder{}
	c := ladderClient(t, fake, rec)
	c.reconnectBackoffMin = 4 * time.Second
	c.reconnectBackoffMax = 60 * time.Second
	c.randFn = func() float64 { return 0.25 }
	sleeps := &sleepLog{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.sleepFn = func(ctx context.Context, d time.Duration) error {
		if len(sleeps.snapshot()) == 5 {
			cancel()
		}
		return sleeps.fn(ctx, d)
	}

	if err := c.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	// rand 0.25 scales by 0.9; the fourth channel connected, so its backoff starts over at the minimum.
	want := []time.Duration{3600 * time.Millisecond, 7200 * time.Millisecond, 14400 * time.Millisecond, 3600 * time.Millisecond, 7200 * time.Millisecond, 14400 * time.Millisecond}
	got := sleeps.snapshot()
	if len(got) != len(want) {
		t.Fatalf("sleeps = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("sleep %d = %v, want %v (all: %v)", i, got[i], want[i], got)
		}
	}
}

func TestChannelRecycleAndRetryBackoffAreJittered(t *testing.T) {
	c, err := NewClient(ClientOpts{Rand: func() float64 { return 1 }})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ch := c.buildChannel()
	if ch.maxAge != 108*time.Minute {
		t.Errorf("channel maxAge = %v, want 90m + 20%%", ch.maxAge)
	}
	if got := ch.jitter(2 * time.Second); got != 2400*time.Millisecond {
		t.Errorf("channel retry backoff jitter(2s) = %v, want 2.4s", got)
	}
	plain, err := NewClient(ClientOpts{})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	for i := 0; i < 50; i++ {
		if age := plain.buildChannel().maxAge; !within(age, defaultChannelMaxAge) {
			t.Fatalf("default channel maxAge = %v, outside ±20%% of 90m", age)
		}
	}
}

func TestXSRFRefreshIntervalIsJittered(t *testing.T) {
	seeded := rand.New(rand.NewSource(7))
	c, err := NewClient(ClientOpts{Rand: seeded.Float64})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var waits []time.Duration
	c.xsrfSleepFn = func(_ context.Context, d time.Duration) error {
		waits = append(waits, d)
		cancel()
		return context.Canceled
	}
	c.xsrfRefreshLoop(ctx)
	if len(waits) != 1 || !within(waits[0], defaultXSRFRefreshInterval) || waits[0] == defaultXSRFRefreshInterval {
		t.Errorf("refresh waits = %v, want one jittered wait within ±20%% of 24h", waits)
	}
}
