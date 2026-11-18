package gchatmeow_test

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/gchattest"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// stateLog records connection states across goroutines.
type stateLog struct {
	mu     sync.Mutex
	states []gchatmeow.ConnState
}

func (l *stateLog) record(s gchatmeow.ConnState, _ error) {
	l.mu.Lock()
	l.states = append(l.states, s)
	l.mu.Unlock()
}

func (l *stateLog) count(want gchatmeow.ConnState) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, s := range l.states {
		if s == want {
			n++
		}
	}
	return n
}

// fakeClockClient builds a client against srv whose sleeps only advance clock.
func fakeClockClient(t *testing.T, srv *gchattest.Server, clock *gchattest.Clock, opts gchatmeow.ClientOpts) (*gchatmeow.Client, *stateLog) {
	t.Helper()
	opts.Cookies = map[string]string{"SID": "sid-value"}
	opts.BaseURLs = gchatmeow.BaseURLs{Chat: srv.ChatURL(), Upload: srv.UploadURL()}
	opts.Now, opts.Sleep = clock.Now, clock.Sleep
	c, err := gchatmeow.NewClient(opts)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	log := &stateLog{}
	c.OnConnectionState = log.record
	return c, log
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// connectUntilDone runs Connect in the background and returns its result channel.
func connectUntilDone(ctx context.Context, c *gchatmeow.Client) chan error {
	done := make(chan error, 1)
	go func() { done <- c.Connect(ctx) }()
	return done
}

func wantBadCredentials(t *testing.T, done chan error, log *stateLog) {
	t.Helper()
	select {
	case err := <-done:
		if !gchatmeow.IsAuthError(err) {
			t.Fatalf("Connect returned %v, want an auth error", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Connect did not stop on the dead session")
	}
	if log.count(gchatmeow.ConnStateBadCredentials) != 1 {
		t.Errorf("BAD_CREDENTIALS reported %d times, want once", log.count(gchatmeow.ConnStateBadCredentials))
	}
}

// TestForbiddenChannelWithSignInPageIsBadCredentialsWithinMinutes: a 403 forces the /mole/world check at once.
func TestForbiddenChannelWithSignInPageIsBadCredentialsWithinMinutes(t *testing.T) {
	srv := gchattest.New(t)
	clock := gchattest.NewClock()
	c, log := fakeClockClient(t, srv, clock, gchatmeow.ClientOpts{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.FetchXSRFToken(ctx); err != nil {
		t.Fatalf("FetchXSRFToken: %v", err)
	}
	srv.SetEventsStatus(http.StatusForbidden)
	srv.SetMoleWorld(gchattest.MoleWorldSignedOut, 0)

	wantBadCredentials(t, connectUntilDone(ctx, c), log)
	if clock.Elapsed() > 5*time.Minute {
		t.Errorf("took %v of fake time, want minutes", clock.Elapsed())
	}
	if n := srv.CountPath("/webchannel/register"); n != 1 {
		t.Errorf("register calls = %d, want 1: a 403 must not wait for more failures", n)
	}
}

// TestRepeatedChannelFailuresForceASessionCheck: three non-network failures in a row are enough.
func TestRepeatedChannelFailuresForceASessionCheck(t *testing.T) {
	srv := gchattest.New(t)
	clock := gchattest.NewClock()
	c, log := fakeClockClient(t, srv, clock, gchatmeow.ClientOpts{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.FetchXSRFToken(ctx); err != nil {
		t.Fatalf("FetchXSRFToken: %v", err)
	}
	srv.SetRegister(false, http.StatusInternalServerError)
	srv.SetMoleWorld(gchattest.MoleWorldSignedOut, 0)

	wantBadCredentials(t, connectUntilDone(ctx, c), log)
	if n := srv.CountPath("/webchannel/register"); n != 3 {
		t.Errorf("register calls = %d, want 3 before the forced check", n)
	}
	if clock.Elapsed() > 5*time.Minute {
		t.Errorf("took %v of fake time, want minutes, not the 24 h refresh", clock.Elapsed())
	}
}

// TestRegisterWithoutCompassCountsAsAFailure: a 200 with no session cookie is not a connection.
func TestRegisterWithoutCompassCountsAsAFailure(t *testing.T) {
	t.Run("dead session", func(t *testing.T) {
		srv := gchattest.New(t)
		c, log := fakeClockClient(t, srv, gchattest.NewClock(), gchatmeow.ClientOpts{})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := c.FetchXSRFToken(ctx); err != nil {
			t.Fatalf("FetchXSRFToken: %v", err)
		}
		srv.SetRegister(true, 0)
		srv.SetMoleWorld(gchattest.MoleWorldSignedOut, 0)
		wantBadCredentials(t, connectUntilDone(ctx, c), log)
		if n := srv.CountPath("/webchannel/events"); n != 0 {
			t.Errorf("events requests = %d, want 0 without a webchannel session", n)
		}
	})
	t.Run("live session keeps retrying", func(t *testing.T) {
		srv := gchattest.New(t)
		c, log := fakeClockClient(t, srv, gchattest.NewClock(), gchatmeow.ClientOpts{})
		srv.SetRegister(true, 0)
		ctx, cancel := context.WithCancel(context.Background())
		done := connectUntilDone(ctx, c)
		waitUntil(t, "several register attempts", func() bool { return srv.CountPath("/webchannel/register") >= 7 })
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Connect returned %v, want nil after cancel", err)
		}
		if log.count(gchatmeow.ConnStateBadCredentials) != 0 || log.count(gchatmeow.ConnStateConnected) != 0 {
			t.Error("a register without a session was reported as connected or as bad credentials")
		}
		if log.count(gchatmeow.ConnStateTransient) == 0 {
			t.Error("no transient state reported for the failing register")
		}
	})
}

// TestRejectedRPCTriggersARateLimitedSessionCheck: a 403 from an RPC asks /mole/world, at most every few minutes.
func TestRejectedRPCTriggersARateLimitedSessionCheck(t *testing.T) {
	srv := gchattest.New(t)
	srv.HandleStatus("get_group", http.StatusForbidden)
	clock := gchattest.NewClock()
	c, log := fakeClockClient(t, srv, clock, gchatmeow.ClientOpts{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := connectUntilDone(ctx, c)
	waitUntil(t, "Connected", func() bool { return log.count(gchatmeow.ConnStateConnected) == 1 })
	checks := func() int { return srv.CountPath("/mole/world") }
	base := checks()

	rejected := func() {
		t.Helper()
		var ue *gchatmeow.UnexpectedStatusError
		if _, err := c.GetGroup(ctx, &pb.GetGroupRequest{}); !errors.As(err, &ue) || ue.Status != http.StatusForbidden {
			t.Fatalf("GetGroup error = %v, want the 403", err)
		}
	}
	rejected()
	waitUntil(t, "the first session check", func() bool { return checks() == base+1 })
	rejected()
	time.Sleep(50 * time.Millisecond)
	if checks() != base+1 {
		t.Errorf("session checks = %d, want 1: a second 403 inside the interval must not ask again", checks()-base)
	}
	if log.count(gchatmeow.ConnStateBadCredentials) != 0 {
		t.Fatal("a 403 on a live session was reported as bad credentials")
	}

	clock.Advance(10 * time.Minute)
	srv.SetMoleWorld(gchattest.MoleWorldSignedOut, 0)
	rejected()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Connect did not stop after the session check found the sign-in page")
	}
	if log.count(gchatmeow.ConnStateBadCredentials) != 1 {
		t.Errorf("BAD_CREDENTIALS reported %d times, want once", log.count(gchatmeow.ConnStateBadCredentials))
	}
	if got := checks() - base; got != 3 {
		t.Errorf("session checks = %d, want 3 (one live, two to confirm the logout)", got)
	}
}
