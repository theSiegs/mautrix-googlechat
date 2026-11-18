package gchatmeow_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/gchattest"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// countingObserver counts every signal by a readable key.
type countingObserver struct {
	mu     sync.Mutex
	counts map[string]int
	streak int
}

func (o *countingObserver) add(key string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.counts == nil {
		o.counts = map[string]int{}
	}
	o.counts[key]++
}

func (o *countingObserver) get(key string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.counts[key]
}

func (o *countingObserver) Request(endpoint string, status int) {
	o.add(fmt.Sprintf("request/%s/%d", endpoint, status))
}

func (o *countingObserver) MoleWorld(result gchatmeow.MoleWorldResult, streak int) {
	o.add(fmt.Sprintf("mole_world/%d", result))
	o.mu.Lock()
	o.streak = streak
	o.mu.Unlock()
}

func (o *countingObserver) DecodeSkip(kind string) { o.add("skip/" + kind) }
func (o *countingObserver) ChannelConnected()      { o.add("connected") }
func (o *countingObserver) StreamEvent()           { o.add("event") }

func TestObserverSeesRequestsEventsAndDecodeSkips(t *testing.T) {
	srv := gchattest.New(t)
	srv.HandleProto("get_group", &pb.GetGroupResponse{})
	srv.HandleStatus("get_members", http.StatusBadRequest)
	obs := &countingObserver{}
	c, log := fakeClockClient(t, srv, gchattest.NewClock(), gchatmeow.ClientOpts{Observer: obs})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := connectUntilDone(ctx, c)
	waitUntil(t, "Connected", func() bool { return log.count(gchatmeow.ConnStateConnected) == 1 })

	c.GetGroup(ctx, &pb.GetGroupRequest{})
	c.GetGroup(ctx, &pb.GetGroupRequest{})
	c.GetMembers(ctx, &pb.GetMembersRequest{})
	if err := srv.PushEvent(&pb.Event{Type: pb.Event_MESSAGE_POSTED.Enum(), Body: &pb.Event_EventBody{EventType: pb.Event_MESSAGE_POSTED.Enum()}}); err != nil {
		t.Fatalf("PushEvent: %v", err)
	}
	srv.PushRaw(`{"not":"an array"}`)
	srv.PushRaw(`[{"not":"pblite"}]`)
	waitUntil(t, "the event and both skips", func() bool {
		return obs.get("event") == 1 && obs.get("skip/frame") == 1 && obs.get("skip/stream_event") == 1
	})
	cancel()
	<-done

	for key, want := range map[string]int{
		"request/get_group/200":   2,
		"request/get_members/400": 1,
		"mole_world/0":            1,
		"connected":               1,
	} {
		if got := obs.get(key); got != want {
			t.Errorf("observer count %s = %d, want %d", key, got, want)
		}
	}
}

func TestObserverCountsConsecutiveUnparseableMoleWorld(t *testing.T) {
	srv := gchattest.New(t)
	obs := &countingObserver{}
	c, _ := fakeClockClient(t, srv, gchattest.NewClock(), gchatmeow.ClientOpts{})
	c.SetObserver(obs)
	ctx := context.Background()

	srv.SetMoleWorld(gchattest.MoleWorldUnparseable, 0)
	for i := 1; i <= 3; i++ {
		if err := c.FetchXSRFToken(ctx); !errors.Is(err, gchatmeow.ErrMoleWorldUnparseable) || gchatmeow.IsAuthError(err) {
			t.Fatalf("fetch %d returned %v, want the unparseable error and no logout signal", i, err)
		}
		if obs.streak != i {
			t.Errorf("after %d unparseable pages the streak is %d", i, obs.streak)
		}
	}
	// A failed request neither extends nor ends the streak.
	srv.SetMoleWorld(gchattest.MoleWorldOK, http.StatusBadGateway)
	c.FetchXSRFToken(ctx)
	if obs.streak != 3 || obs.get(fmt.Sprintf("mole_world/%d", gchatmeow.MoleWorldFailed)) != 1 {
		t.Errorf("after a 502 the streak is %d, want it unchanged at 3 and one failed result", obs.streak)
	}
	srv.SetMoleWorld(gchattest.MoleWorldOK, 0)
	if err := c.FetchXSRFToken(ctx); err != nil {
		t.Fatalf("FetchXSRFToken: %v", err)
	}
	if obs.streak != 0 {
		t.Errorf("after a good page the streak is %d, want 0", obs.streak)
	}
}
