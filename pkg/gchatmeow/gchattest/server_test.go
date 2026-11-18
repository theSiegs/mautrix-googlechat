package gchattest_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/gchattest"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

func newClient(t *testing.T, srv *gchattest.Server) *gchatmeow.Client {
	t.Helper()
	c, err := gchatmeow.NewClient(gchatmeow.ClientOpts{
		Cookies:  map[string]string{"SID": "sid-value"},
		BaseURLs: gchatmeow.BaseURLs{Chat: srv.ChatURL(), Upload: srv.UploadURL()},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}

func TestClientTalksToFakeServer(t *testing.T) {
	srv := gchattest.New(t)
	srv.HandleProto("get_self_user_status", &pb.GetSelfUserStatusResponse{
		UserStatus: &pb.UserStatus{UserId: &pb.UserId{Id: proto.String("1001")}},
	})
	c := newClient(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.FetchXSRFToken(ctx); err != nil {
		t.Fatalf("FetchXSRFToken: %v", err)
	}
	if c.XSRFToken() != gchattest.XSRFToken {
		t.Errorf("XSRFToken = %q, want the fake server's token", c.XSRFToken())
	}
	resp, err := c.GetSelfUserStatus(ctx, &pb.GetSelfUserStatusRequest{})
	if err != nil {
		t.Fatalf("GetSelfUserStatus: %v", err)
	}
	if got := resp.GetUserStatus().GetUserId().GetId(); got != "1001" {
		t.Errorf("gaia id = %q, want 1001", got)
	}
	reqs := srv.Requests()
	last := reqs[len(reqs)-1]
	if last.Endpoint != "get_self_user_status" || last.Header.Get("Cookie") != "SID=sid-value" {
		t.Errorf("last request = %s with Cookie %q, want the RPC with the session cookie", last.Endpoint, last.Header.Get("Cookie"))
	}
}

func TestClientReceivesPushedEvent(t *testing.T) {
	srv := gchattest.New(t)
	c := newClient(t, srv)

	connected := make(chan struct{}, 1)
	events := make(chan *pb.Event, 1)
	c.OnConnectionState = func(state gchatmeow.ConnState, _ error) {
		if state == gchatmeow.ConnStateConnected {
			select {
			case connected <- struct{}{}:
			default:
			}
		}
	}
	c.OnStreamEvent = func(_ context.Context, ev *pb.Event) { events <- ev }

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { c.Connect(ctx); close(done) }()

	select {
	case <-connected:
	case <-ctx.Done():
		t.Fatal("client never reported Connected")
	}
	err := srv.PushEvent(&pb.Event{
		Type: pb.Event_MESSAGE_POSTED.Enum(),
		Body: &pb.Event_EventBody{EventType: pb.Event_MESSAGE_POSTED.Enum()},
	})
	if err != nil {
		t.Fatalf("PushEvent: %v", err)
	}
	select {
	case ev := <-events:
		if ev.GetType() != pb.Event_MESSAGE_POSTED {
			t.Errorf("event type = %v, want MESSAGE_POSTED", ev.GetType())
		}
	case <-ctx.Done():
		t.Fatal("pushed event never arrived")
	}
	c.Disconnect()
	<-done
	if srv.CountPath("/webchannel/register") != 1 {
		t.Errorf("register calls = %d, want 1", srv.CountPath("/webchannel/register"))
	}
}
