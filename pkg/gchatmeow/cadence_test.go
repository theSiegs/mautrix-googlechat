package gchatmeow_test

import (
	"context"
	"math/rand"
	"net/http"
	"testing"
	"time"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/gchattest"
)

// TestBackoffLadderOnAFakeClockIsJitteredWithinBounds: seeded rand, fake clock, failing network.
func TestBackoffLadderOnAFakeClockIsJitteredWithinBounds(t *testing.T) {
	srv := gchattest.New(t)
	clock := gchattest.NewClock()
	seeded := rand.New(rand.NewSource(42))
	c, _ := fakeClockClient(t, srv, clock, gchatmeow.ClientOpts{Rand: seeded.Float64})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// A failing /mole/world keeps the supervisor on its transient ladder without ever opening a channel.
	srv.SetMoleWorld(gchattest.MoleWorldOK, http.StatusBadGateway)
	done := connectUntilDone(ctx, c)
	// The parked daily refresh is also a recorded sleep; only the supervisor backoffs are short.
	backoffs := func() []time.Duration {
		var out []time.Duration
		for _, d := range clock.Sleeps() {
			if d < time.Hour {
				out = append(out, d)
			}
		}
		return out
	}
	waitUntil(t, "eight backoff sleeps", func() bool { return len(backoffs()) >= 8 })
	cancel()
	<-done

	nominal := []time.Duration{4, 8, 16, 32, 60, 60, 60, 60}
	exact := 0
	for i, got := range backoffs()[:8] {
		want := nominal[i] * time.Second
		lo, hi := want*8/10, want*12/10
		if got < lo || got > hi {
			t.Errorf("backoff %d = %v, want within ±20%% of %v", i, got, want)
		}
		if got == want {
			exact++
		}
	}
	if exact == 8 {
		t.Error("every backoff equals its nominal value; jitter is not applied")
	}
}

// TestDailyRefreshFiresWhenTheFakeClockPassesIt: the refresh parks on the clock and runs once time moves on.
func TestDailyRefreshFiresWhenTheFakeClockPassesIt(t *testing.T) {
	srv := gchattest.New(t)
	clock := gchattest.NewClock()
	c, log := fakeClockClient(t, srv, clock, gchatmeow.ClientOpts{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := connectUntilDone(ctx, c)
	waitUntil(t, "Connected", func() bool { return log.count(gchatmeow.ConnStateConnected) == 1 })
	if n := srv.CountPath("/mole/world"); n != 1 {
		t.Fatalf("/mole/world fetched %d times at connect, want 1", n)
	}
	time.Sleep(30 * time.Millisecond)
	if n := srv.CountPath("/mole/world"); n != 1 {
		t.Fatalf("/mole/world fetched %d times before a day passed, want 1", n)
	}
	clock.Advance(30 * time.Hour)
	waitUntil(t, "the daily refresh", func() bool { return srv.CountPath("/mole/world") == 2 })
	cancel()
	<-done
}
