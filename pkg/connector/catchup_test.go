package connector

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/gchattest"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// eventLog collects queued remote events and resyncs across goroutines.
type eventLog struct {
	mu      sync.Mutex
	events  []bridgev2.RemoteEvent
	resyncs []*simplevent.ChatResync
}

func (l *eventLog) queue(evt bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, evt)
	return bridgev2.EventHandlingResultQueued
}

func (l *eventLog) resync(evt *simplevent.ChatResync) bridgev2.EventHandlingResult {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.resyncs = append(l.resyncs, evt)
	return bridgev2.EventHandlingResultQueued
}

func (l *eventLog) messageIDs() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var ids []string
	for _, evt := range l.events {
		if msg, ok := evt.(*simplevent.Message[*pb.Message]); ok {
			ids = append(ids, string(msg.ID))
		}
	}
	return ids
}

func (l *eventLog) resyncKeys() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var keys []string
	for _, evt := range l.resyncs {
		keys = append(keys, string(evt.PortalKey.ID))
	}
	return keys
}

func withUserRevision(evt *pb.Event, rev int64) *pb.Event {
	evt.RevisionType = &pb.Event_UserRevision{UserRevision: &pb.WriteRevision{Timestamp: proto.Int64(rev)}}
	return evt
}

func withGroupRevision(evt *pb.Event, rev int64) *pb.Event {
	evt.RevisionType = &pb.Event_GroupRevision{GroupRevision: &pb.WriteRevision{Timestamp: proto.Int64(rev)}}
	return evt
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestRestartCatchesUpOnFirstConnectAgainstFakeServer: a stored revision and two pending events are replayed once.
func TestRestartCatchesUpOnFirstConnectAgainstFakeServer(t *testing.T) {
	srv := gchattest.New(t)
	srv.HandleProto("paginated_world", &pb.PaginatedWorldResponse{})
	srv.HandleProto("get_members", &pb.GetMembersResponse{})
	var fromRevisions []int64
	var mu sync.Mutex
	srv.Handle("catch_up_user", func(body []byte) (proto.Message, int) {
		req := &pb.CatchUpUserRequest{}
		_ = proto.Unmarshal(body, req)
		mu.Lock()
		fromRevisions = append(fromRevisions, req.GetRange().GetFromRevisionTimestamp())
		mu.Unlock()
		return &pb.CatchUpResponse{
			Status: pb.CatchUpResponse_COMPLETED.Enum(),
			Events: []*pb.Event{
				withUserRevision(messagePostedEvent(dmGroupID("dm-1"), "missed-1", "2002", "one", 10), 110),
				withUserRevision(messagePostedEvent(dmGroupID("dm-1"), "missed-2", "2002", "two", 20), 120),
			},
		}, 0
	})

	meta := &UserLoginMetadata{Cookies: fakeCookies(), Revision: 100}
	events := &eventLog{}
	gc := &GChatClient{
		UserLogin:            newTestUserLogin(meta),
		Main:                 &GChatConnector{Config: *newTestConfig(t)},
		saveFn:               func(context.Context) error { return nil },
		queueRemoteEventFn:   events.queue,
		queueChatResyncFn:    events.resync,
		savePortalRevisionFn: func(context.Context, networkid.PortalKey, int64) {},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gc.wireAndStart(ctx, newFakeServerConn(t, srv, gchatmeow.ClientOpts{}))

	waitFor(t, "both missed events", func() bool { return len(events.messageIDs()) >= 2 })
	gc.Disconnect()
	time.Sleep(50 * time.Millisecond)

	if got := events.messageIDs(); len(got) != 2 || got[0] != "missed-1" || got[1] != "missed-2" {
		t.Errorf("queued messages = %v, want missed-1 then missed-2 exactly once", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(fromRevisions) != 1 || fromRevisions[0] != 100 {
		t.Errorf("catch_up_user from-revisions = %v, want one call from the stored revision 100", fromRevisions)
	}
	if got := gc.getRevision(); got != 120 {
		t.Errorf("stored revision = %d, want 120 after the replay", got)
	}
}

func TestFreshLoginDoesNotCatchUpOnFirstConnect(t *testing.T) {
	synced := make(chan struct{})
	catchUps := 0
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		Main:      &GChatConnector{Config: *newTestConfig(t)},
		saveFn:    func(context.Context) error { return nil },
		paginatedWorldFn: func(context.Context, *pb.PaginatedWorldRequest) (*pb.PaginatedWorldResponse, error) {
			close(synced)
			return &pb.PaginatedWorldResponse{}, nil
		},
		catchUpUserFn: func(context.Context, *pb.CatchUpUserRequest) (*pb.CatchUpResponse, error) {
			catchUps++
			return &pb.CatchUpResponse{Status: pb.CatchUpResponse_COMPLETED.Enum()}, nil
		},
	}
	gc.handleConnState(context.Background(), gchatmeow.ConnStateConnected, nil)
	<-synced
	waitUntilSyncNotInProgress(t, gc, 2*time.Second)
	time.Sleep(20 * time.Millisecond)
	if catchUps != 0 {
		t.Errorf("catch_up_user called %d times on a fresh login, want 0", catchUps)
	}
}

// TestFirstConnectCatchUpUsesTheRevisionStoredBeforeLiveEvents: a live event during the sync must not hide the gap.
func TestFirstConnectCatchUpUsesTheRevisionStoredBeforeLiveEvents(t *testing.T) {
	from := make(chan int64, 1)
	var gc *GChatClient
	gc = &GChatClient{
		UserLogin:            newTestUserLogin(&UserLoginMetadata{Revision: 100}),
		Main:                 &GChatConnector{Config: *newTestConfig(t)},
		saveFn:               func(context.Context) error { return nil },
		savePortalRevisionFn: func(context.Context, networkid.PortalKey, int64) {},
		queueRemoteEventFn: func(bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
			return bridgev2.EventHandlingResultQueued
		},
		paginatedWorldFn: func(ctx context.Context, _ *pb.PaginatedWorldRequest) (*pb.PaginatedWorldResponse, error) {
			gc.handleGChatEvent(ctx, withUserRevision(messagePostedEvent(dmGroupID("dm-1"), "live", "2002", "hi", 5), 500))
			return &pb.PaginatedWorldResponse{}, nil
		},
		catchUpUserFn: func(_ context.Context, req *pb.CatchUpUserRequest) (*pb.CatchUpResponse, error) {
			from <- req.GetRange().GetFromRevisionTimestamp()
			return &pb.CatchUpResponse{Status: pb.CatchUpResponse_COMPLETED.Enum()}, nil
		},
	}
	gc.handleConnState(context.Background(), gchatmeow.ConnStateConnected, nil)
	select {
	case got := <-from:
		if got != 100 {
			t.Errorf("catch-up started from %d, want the pre-connect revision 100", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("catch_up_user was never called after the first sync")
	}
}

func catchUpTestPortal(group gcid.GroupID, revision int64) *bridgev2.Portal {
	return &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: gcid.MakePortalKey(group, "112233"),
		MXID:      "!room:example.com",
		Metadata:  &PortalMetadata{Revision: revision},
	}}
}

// TestUserCatchUpGivingUpFallsBackPerPortal: catch_up_group per chat, and a resync where that cannot help.
func TestUserCatchUpGivingUpFallsBackPerPortal(t *testing.T) {
	for _, tc := range []struct {
		name string
		user func(calls int) *pb.CatchUpResponse
	}{
		{"aborted", func(int) *pb.CatchUpResponse {
			return &pb.CatchUpResponse{Status: pb.CatchUpResponse_ABORTED_FROM_REVISION_TOO_OLD.Enum()}
		}},
		{"page cap", func(calls int) *pb.CatchUpResponse {
			return &pb.CatchUpResponse{
				Status: pb.CatchUpResponse_PAGINATED.Enum(),
				Events: []*pb.Event{withUserRevision(&pb.Event{}, int64(100+calls))},
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			okGroup := gcid.GroupID{ID: "dm-ok", IsDM: true}
			abortedGroup := gcid.GroupID{ID: "dm-aborted", IsDM: true}
			newGroup := gcid.GroupID{ID: "space-new"}
			events := &eventLog{}
			userCalls := 0
			var groupCalls []string
			gc := &GChatClient{
				UserLogin:            newTestUserLogin(&UserLoginMetadata{Revision: 100}),
				saveFn:               func(context.Context) error { return nil },
				queueRemoteEventFn:   events.queue,
				queueChatResyncFn:    events.resync,
				savePortalRevisionFn: func(context.Context, networkid.PortalKey, int64) {},
				gapFillSpacing:       time.Millisecond,
				listPortalsFn: func(context.Context) ([]*bridgev2.Portal, error) {
					return []*bridgev2.Portal{
						catchUpTestPortal(okGroup, 50), catchUpTestPortal(abortedGroup, 60), catchUpTestPortal(newGroup, 0),
					}, nil
				},
				catchUpUserFn: func(context.Context, *pb.CatchUpUserRequest) (*pb.CatchUpResponse, error) {
					userCalls++
					return tc.user(userCalls), nil
				},
				catchUpGroupFn: func(_ context.Context, req *pb.CatchUpGroupRequest) (*pb.CatchUpResponse, error) {
					id, _, _ := gchatmeow.GroupIDToParts(req.GetGroupId())
					groupCalls = append(groupCalls, id)
					if id != okGroup.ID {
						return &pb.CatchUpResponse{Status: pb.CatchUpResponse_ABORTED_CUTOFF_EXCEEDED.Enum()}, nil
					}
					if got := req.GetRange().GetFromRevisionTimestamp(); got != 50 {
						t.Errorf("catch_up_group from-revision = %d, want the stored 50", got)
					}
					return &pb.CatchUpResponse{
						Status: pb.CatchUpResponse_COMPLETED.Enum(),
						Events: []*pb.Event{withGroupRevision(messagePostedEvent(req.GetGroupId(), "group-missed", "2002", "hi", 7), 70)},
					}, nil
				},
			}

			gc.catchUp(context.Background())

			if len(groupCalls) != 2 || groupCalls[0] != okGroup.ID || groupCalls[1] != abortedGroup.ID {
				t.Errorf("catch_up_group calls = %v, want one for each portal with a revision", groupCalls)
			}
			if got := events.messageIDs(); len(got) != 1 || got[0] != "group-missed" {
				t.Errorf("queued messages = %v, want the one from catch_up_group", got)
			}
			if got := events.resyncKeys(); len(got) != 2 || got[0] != "dm:dm-aborted" || got[1] != "space:space-new" {
				t.Errorf("resynced portals = %v, want the aborted and the revision-less one", got)
			}
		})
	}
}

// TestInterruptedCatchUpResumesFromItsStart: live events move the watermark, the pending revision keeps the gap.
func TestInterruptedCatchUpResumesFromItsStart(t *testing.T) {
	meta := &UserLoginMetadata{Revision: 100}
	var from []int64
	fail := true
	gc := &GChatClient{
		UserLogin:            newTestUserLogin(meta),
		saveFn:               func(context.Context) error { return nil },
		savePortalRevisionFn: func(context.Context, networkid.PortalKey, int64) {},
		queueRemoteEventFn: func(bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
			return bridgev2.EventHandlingResultQueued
		},
		catchUpUserFn: func(_ context.Context, req *pb.CatchUpUserRequest) (*pb.CatchUpResponse, error) {
			from = append(from, req.GetRange().GetFromRevisionTimestamp())
			if fail {
				return nil, context.DeadlineExceeded
			}
			return &pb.CatchUpResponse{Status: pb.CatchUpResponse_COMPLETED.Enum()}, nil
		},
	}
	ctx := context.Background()

	gc.catchUpFrom(ctx, gc.beginCatchUp(ctx))
	gc.handleGChatEvent(ctx, withUserRevision(messagePostedEvent(dmGroupID("dm-1"), "live", "2002", "hi", 5), 500))
	if meta.Revision != 500 || meta.CatchUpFrom != 100 {
		t.Fatalf("after a failed drain and a live event: revision %d, pending %d; want 500 and 100", meta.Revision, meta.CatchUpFrom)
	}

	fail = false
	gc.catchUpFrom(ctx, gc.beginCatchUp(ctx))
	if len(from) != 2 || from[1] != 100 {
		t.Errorf("catch-up from-revisions = %v, want the retry to start at 100 again", from)
	}
	if meta.CatchUpFrom != 0 {
		t.Errorf("pending revision = %d after a completed drain, want it cleared", meta.CatchUpFrom)
	}
	if got := gc.beginCatchUp(ctx); got != 500 {
		t.Errorf("next catch-up starts at %d, want the watermark 500", got)
	}
}
