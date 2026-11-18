package connector

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/gchattest"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// readOnlyLogin builds a login in read-only mode whose real client talks to a fake server.
func readOnlyLogin(t *testing.T, configReadOnly bool) (*GChatClient, *gchattest.Server) {
	t.Helper()
	srv := gchattest.New(t)
	cfg := newTestConfig(t)
	cfg.ReadOnly = configReadOnly
	gc := &GChatClient{
		Main:      &GChatConnector{Config: *cfg},
		UserLogin: newTestUserLogin(&UserLoginMetadata{Cookies: fakeCookies()}),
		saveFn:    func(context.Context) error { return nil },
	}
	// The client is always read-only, so the second half of the test exercises the gchatmeow backstop alone.
	gc.conn = newFakeServerConn(t, srv, gchatmeow.ClientOpts{ReadOnly: true})
	return gc, srv
}

// assertNothingMutated fails if any request reached a mutating RPC, the upload host or the forward channel.
func assertNothingMutated(t *testing.T, srv *gchattest.Server) {
	t.Helper()
	mutating := map[string]bool{}
	for _, ep := range gchatmeow.Endpoints() {
		mutating[ep.Name] = ep.Mutates
	}
	for _, req := range srv.Requests() {
		switch {
		case req.Endpoint != "" && mutating[req.Endpoint]:
			t.Errorf("read-only mode sent the mutating RPC %s", req.Endpoint)
		case strings.Contains(req.Path, "/uploads"):
			t.Error("read-only mode sent an upload")
		case strings.HasSuffix(req.Path, "/webchannel/events") && req.Method == "POST":
			t.Error("read-only mode sent a stream event")
		}
	}
}

// TestReadOnlyModeNeverReachesAMutatingPath runs every Matrix-to-Google entry point in read-only mode.
func TestReadOnlyModeNeverReachesAMutatingPath(t *testing.T) {
	gc, srv := readOnlyLogin(t, true)
	ctx := context.Background()
	portal := flatBackfillPortal(gcid.GroupID{ID: "space-1"})

	refused := map[string]error{}
	_, refused["message"] = gc.HandleMatrixMessage(ctx, &bridgev2.MatrixMessage{})
	refused["edit"] = gc.HandleMatrixEdit(ctx, &bridgev2.MatrixEdit{})
	refused["redaction"] = gc.HandleMatrixMessageRemove(ctx, &bridgev2.MatrixMessageRemove{})
	_, refused["reaction"] = gc.HandleMatrixReaction(ctx, &bridgev2.MatrixReaction{})
	refused["reaction removal"] = gc.HandleMatrixReactionRemove(ctx, &bridgev2.MatrixReactionRemove{})
	_, refused["membership"] = gc.HandleMatrixMembership(ctx, &bridgev2.MatrixMembershipChange{})
	_, refused["room name"] = gc.HandleMatrixRoomName(ctx, &bridgev2.MatrixRoomName{})
	_, refused["room topic"] = gc.HandleMatrixRoomTopic(ctx, &bridgev2.MatrixRoomTopic{})
	refused["mute"] = gc.HandleMute(ctx, &bridgev2.MatrixMute{})
	refused["delete chat"] = gc.HandleMatrixDeleteChat(ctx, &bridgev2.MatrixDeleteChat{})
	_, refused["create dm"] = gc.ResolveIdentifier(ctx, "alex@example.com", true)
	_, refused["create chat with ghost"] = gc.CreateChatWithGhost(ctx, &bridgev2.Ghost{})
	_, refused["create space"] = gc.CreateGroup(ctx, &bridgev2.GroupCreateParams{})
	for name, err := range refused {
		if !errors.Is(err, gchatmeow.ErrReadOnly) {
			t.Errorf("%s in read-only mode returned %v, want ErrReadOnly", name, err)
		}
	}

	if err := gc.HandleMatrixReadReceipt(ctx, matrixReadReceipt(portal, nil, time.Now())); err != nil {
		t.Errorf("read receipt in read-only mode returned %v, want it dropped silently", err)
	}
	if err := gc.HandleMatrixTyping(ctx, &bridgev2.MatrixTyping{Portal: portal, IsTyping: true}); err != nil {
		t.Errorf("typing in read-only mode returned %v, want it dropped silently", err)
	}
	if reply := gc.Main.runLeaveCommand(ctx, portal, nil, nil); !strings.Contains(reply, "read-only") {
		t.Errorf("leave command replied %q, want a read-only refusal", reply)
	}

	if n := len(srv.Requests()); n != 0 {
		t.Errorf("read-only handlers made %d requests, want 0", n)
	}
	assertNothingMutated(t, srv)
}

// TestReadOnlyClientIsTheBackstop: with the connector guard off, the client still refuses before any HTTP.
func TestReadOnlyClientIsTheBackstop(t *testing.T) {
	gc, srv := readOnlyLogin(t, false)
	ctx := context.Background()
	portal := flatBackfillPortal(gcid.GroupID{ID: "space-1"})

	if err := gc.HandleMatrixReadReceipt(ctx, matrixReadReceipt(portal, nil, time.Now())); !errors.Is(err, gchatmeow.ErrReadOnly) {
		t.Errorf("read receipt returned %v, want ErrReadOnly from the client", err)
	}
	if err := gc.HandleMatrixTyping(ctx, &bridgev2.MatrixTyping{Portal: portal, IsTyping: true}); !errors.Is(err, gchatmeow.ErrReadOnly) {
		t.Errorf("typing returned %v, want ErrReadOnly from the client", err)
	}
	if err := gc.hideGroup(ctx, gcid.GroupID{ID: "dm-1", IsDM: true}); !errors.Is(err, gchatmeow.ErrReadOnly) {
		t.Errorf("hideGroup returned %v, want ErrReadOnly from the client", err)
	}
	if n := len(srv.Requests()); n != 0 {
		t.Errorf("the read-only client made %d requests, want 0", n)
	}
}

func TestReadOnlyCapabilities(t *testing.T) {
	gc, _ := readOnlyLogin(t, true)
	caps := gc.GetCapabilities(context.Background(), flatBackfillPortal(gcid.GroupID{ID: "space-1"}))
	if caps.Reply != 0 || caps.Edit != 0 || caps.Delete != 0 || caps.Reaction != 0 || caps.Thread != 0 {
		t.Errorf("read-only room features still allow reply/edit/delete/reaction/thread: %+v", caps)
	}
	if caps.ReadReceipts || caps.TypingNotifications || len(caps.File) != 0 || len(caps.MemberActions) != 0 || len(caps.State) != 0 {
		t.Errorf("read-only room features still allow receipts, typing, files, membership or state: %+v", caps)
	}
	if caps.Formatting[event.FmtBold] != 0 {
		t.Error("read-only room features still advertise formatting")
	}

	general := gc.Main.GetCapabilities()
	if general.Provisioning.ResolveIdentifier.CreateDM || len(general.Provisioning.GroupCreation) != 0 {
		t.Errorf("read-only general capabilities still offer chat creation: %+v", general.Provisioning)
	}
	_, roCaps := gc.Main.GetBridgeInfoVersion()
	_, rwCaps := (&GChatConnector{}).GetBridgeInfoVersion()
	if roCaps == rwCaps {
		t.Error("capabilities version is the same with and without read_only; rooms would keep stale features")
	}
}

// TestConnectAppliesReadOnlyFromConfig: the config reaches the gchatmeow client built by Connect.
func TestConnectAppliesReadOnlyFromConfig(t *testing.T) {
	srv := gchattest.New(t)
	cfg := newTestConfig(t)
	cfg.ReadOnly = true
	gc := &GChatClient{
		Main: &GChatConnector{
			Config:   *cfg,
			baseURLs: gchatmeow.BaseURLs{Chat: srv.ChatURL(), Upload: srv.UploadURL()},
		},
		UserLogin: newTestUserLogin(&UserLoginMetadata{Cookies: fakeCookies()}),
		saveFn:    func(context.Context) error { return nil },
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	gc.Connect(ctx)
	conn := gc.getConn()
	if conn == nil || !conn.ReadOnly() {
		t.Fatal("Connect built a client that is not read-only although network.read_only is set")
	}
}
