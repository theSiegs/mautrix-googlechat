package connector

import (
	"context"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/bridgev2/status"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/gchattest"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// driftTestClient is connected, with a scripted chat list and portal count.
func driftTestClient(t *testing.T, world func() *pb.PaginatedWorldResponse, portals int) *GChatClient {
	t.Helper()
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		Main:      &GChatConnector{Config: *newTestConfig(t)},
		saveFn:    func(context.Context) error { return nil },
		paginatedWorldFn: func(context.Context, *pb.PaginatedWorldRequest) (*pb.PaginatedWorldResponse, error) {
			return world(), nil
		},
		getMembersFn: func(context.Context, *pb.GetMembersRequest) (*pb.GetMembersResponse, error) {
			return &pb.GetMembersResponse{}, nil
		},
		queueChatResyncFn: func(*simplevent.ChatResync) bridgev2.EventHandlingResult {
			return bridgev2.EventHandlingResultQueued
		},
		listPortalsFn: func(context.Context) ([]*bridgev2.Portal, error) {
			out := make([]*bridgev2.Portal, portals)
			for i := range out {
				out[i] = catchUpTestPortal(gcid.GroupID{ID: "dm-1", IsDM: true}, 0)
			}
			return out, nil
		},
	}
	gc.reportState(gchatmeow.ConnStateConnected, nil)
	return gc
}

func oneChatWorld() *pb.PaginatedWorldResponse {
	return &pb.PaginatedWorldResponse{WorldItems: []*pb.WorldItemLite{{GroupId: dmGroupID("dm-1")}}}
}

func TestEmptyChatListWithPortalsRaisesAWarning(t *testing.T) {
	world := &pb.PaginatedWorldResponse{}
	gc := driftTestClient(t, func() *pb.PaginatedWorldResponse { return world }, 2)

	gc.syncChats(context.Background())
	if gc.getLastState() != status.StateConnected || gc.getLastErrorCode() != GChatEmptyChatList {
		t.Fatalf("state = %s / %s, want CONNECTED with the empty-chat-list warning", gc.getLastState(), gc.getLastErrorCode())
	}

	// The warning survives a silent channel recycle, which reports Connected again.
	gc.reportState(gchatmeow.ConnStateConnected, nil)
	if gc.getLastErrorCode() != GChatEmptyChatList {
		t.Error("the warning was dropped by the next Connected although the chat list is still empty")
	}
	gc.reportState(gchatmeow.ConnStateTransient, nil)
	if gc.getLastErrorCode() != GChatTransientDisconnect {
		t.Errorf("transient state carries %s, want the plain transient code", gc.getLastErrorCode())
	}

	world = oneChatWorld()
	gc.reportState(gchatmeow.ConnStateConnected, nil)
	gc.syncChats(context.Background())
	if gc.getLastState() != status.StateConnected || gc.getLastErrorCode() != "" {
		t.Errorf("state = %s / %q after a good chat list, want plain CONNECTED", gc.getLastState(), gc.getLastErrorCode())
	}
}

func TestEmptyChatListWithoutPortalsIsNotAWarning(t *testing.T) {
	gc := driftTestClient(t, func() *pb.PaginatedWorldResponse { return &pb.PaginatedWorldResponse{} }, 0)
	gc.syncChats(context.Background())
	if gc.getLastErrorCode() != "" {
		t.Errorf("a new account with no chats got the warning %q", gc.getLastErrorCode())
	}
}

func TestUnparseableMoleWorldStreakRaisesAWarning(t *testing.T) {
	gc := driftTestClient(t, oneChatWorld, 1)
	obs := loginObserver{gc}

	for streak := 1; streak < moleWorldUnparseableAlarm; streak++ {
		obs.MoleWorld(gchatmeow.MoleWorldUnparseable, streak)
	}
	if gc.getLastErrorCode() != "" {
		t.Fatalf("warning %q raised before %d unreadable answers in a row", gc.getLastErrorCode(), moleWorldUnparseableAlarm)
	}
	obs.MoleWorld(gchatmeow.MoleWorldFailed, moleWorldUnparseableAlarm-1)
	obs.MoleWorld(gchatmeow.MoleWorldUnparseable, moleWorldUnparseableAlarm)
	if gc.getLastState() != status.StateConnected || gc.getLastErrorCode() != GChatSessionUnverifiable {
		t.Fatalf("state = %s / %s, want CONNECTED with the session warning", gc.getLastState(), gc.getLastErrorCode())
	}

	gc.reportState(gchatmeow.ConnStateTransient, nil)
	if gc.getLastState() != status.StateTransientDisconnect || gc.getLastErrorCode() != GChatSessionUnverifiable {
		t.Errorf("state = %s / %s, want the reconnect loop to keep the relogin hint", gc.getLastState(), gc.getLastErrorCode())
	}
	gc.reportState(gchatmeow.ConnStateBadCredentials, nil)
	if gc.getLastErrorCode() != GChatBadCredentials {
		t.Errorf("bad credentials carries %s, want its own code", gc.getLastErrorCode())
	}

	gc.reportState(gchatmeow.ConnStateConnected, nil)
	obs.MoleWorld(gchatmeow.MoleWorldOK, 0)
	if gc.getLastErrorCode() != "" {
		t.Errorf("warning %q still set after a readable /mole/world", gc.getLastErrorCode())
	}
}

// TestUnparseableMoleWorldWarningEndToEnd: a challenge page on every session check surfaces after N fetches.
func TestUnparseableMoleWorldWarningEndToEnd(t *testing.T) {
	srv := gchattest.New(t)
	srv.SetMoleWorld(gchattest.MoleWorldUnparseable, 0)
	clock := gchattest.NewClock()
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{Cookies: fakeCookies()}),
		saveFn:    func(context.Context) error { return nil },
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gc.wireAndStart(ctx, newFakeServerConn(t, srv, gchatmeow.ClientOpts{Now: clock.Now, Sleep: clock.Sleep}))

	waitFor(t, "the session warning", func() bool { return gc.getLastErrorCode() == GChatSessionUnverifiable })
	gc.Disconnect()
	if gc.getLastState() != status.StateTransientDisconnect {
		t.Errorf("state = %s, want the reconnect loop to stay transient, not logged out", gc.getLastState())
	}
	if got := srv.CountPath("/mole/world"); got < moleWorldUnparseableAlarm {
		t.Errorf("/mole/world fetched %d times, want at least %d before the warning", got, moleWorldUnparseableAlarm)
	}
	if clock.Elapsed().Hours() > 1 {
		t.Errorf("took %v of fake time, want minutes", clock.Elapsed())
	}
}
