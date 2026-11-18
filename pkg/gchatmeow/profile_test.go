package gchatmeow_test

import (
	"context"
	"net/url"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/gchattest"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/pblite"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// connectOnce connects a client built from opts, waits for Connected, makes one RPC and disconnects.
func connectOnce(t *testing.T, opts gchatmeow.ClientOpts) *gchattest.Server {
	t.Helper()
	srv := gchattest.New(t)
	srv.HandleProto("get_group", &pb.GetGroupResponse{})
	c, log := fakeClockClient(t, srv, gchattest.NewClock(), opts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := connectUntilDone(ctx, c)
	waitUntil(t, "Connected", func() bool { return log.count(gchatmeow.ConnStateConnected) == 1 })
	if _, err := c.GetGroup(ctx, &pb.GetGroupRequest{}); err != nil {
		t.Fatalf("GetGroup: %v", err)
	}
	cancel()
	<-done
	return srv
}

// sentPing decodes the PingEvent of the first forward-channel POST.
func sentPing(t *testing.T, srv *gchattest.Server) *pb.PingEvent {
	t.Helper()
	for _, req := range srv.Requests() {
		if req.Method != "POST" || !strings.HasSuffix(req.Path, "/webchannel/events") {
			continue
		}
		form, err := url.ParseQuery(string(req.Body))
		if err != nil {
			t.Fatalf("parse ping form: %v", err)
		}
		ev := &pb.StreamEventsRequest{}
		if err := pblite.Unmarshal([]byte(form.Get("req0_data")), ev); err != nil {
			t.Fatalf("decode ping: %v", err)
		}
		return ev.GetPingEvent()
	}
	t.Fatal("no ping was sent")
	return nil
}

func TestPastedUserAgentIsSentByteForByte(t *testing.T) {
	const pasted = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.6778.86 Safari/537.36"
	srv := connectOnce(t, gchatmeow.ClientOpts{UserAgent: pasted})
	paths := map[string]bool{}
	for _, req := range srv.Requests() {
		paths[req.Path] = true
		if got := req.Header.Get("User-Agent"); got != pasted {
			t.Errorf("%s %s sent User-Agent %q, want the pasted value", req.Method, req.Path, got)
		}
	}
	for _, want := range []string{"/u/0/mole/world", "/u/0/webchannel/register", "/u/0/webchannel/events", "/u/0/api/get_group"} {
		if !paths[want] {
			t.Errorf("no request to %s was seen", want)
		}
	}
}

func TestProtocolOverridesReachTheURLAndRequestHeader(t *testing.T) {
	check := func(t *testing.T, opts gchatmeow.ClientOpts, want gchatmeow.Protocol) {
		t.Helper()
		srv := connectOnce(t, opts)
		for _, req := range srv.Requests() {
			switch {
			case strings.HasSuffix(req.Path, "/mole/world"):
				if got := req.Query.Get("hs"); got != want.MoleWorldHS {
					t.Errorf("/mole/world hs = %q, want %q", got, want.MoleWorldHS)
				}
			case req.Endpoint == "get_group":
				if got := req.Query.Get("key"); got != want.APIKey {
					t.Errorf("api key = %q, want %q", got, want.APIKey)
				}
				sent := &pb.GetGroupRequest{}
				if err := proto.Unmarshal(req.Body, sent); err != nil {
					t.Fatalf("decode request: %v", err)
				}
				if got := sent.GetRequestHeader().GetClientVersion(); got != want.ClientVersion {
					t.Errorf("client_version = %d, want %d", got, want.ClientVersion)
				}
			}
		}
	}
	t.Run("defaults", func(t *testing.T) {
		check(t, gchatmeow.ClientOpts{}, gchatmeow.DefaultProtocol())
	})
	t.Run("overrides", func(t *testing.T) {
		custom := gchatmeow.Protocol{APIKey: "test-api-key", ClientVersion: 42, MoleWorldHS: `["h_hs","test-build"]`}
		check(t, gchatmeow.ClientOpts{Protocol: custom}, custom)
	})
	t.Run("partial override keeps the other defaults", func(t *testing.T) {
		want := gchatmeow.DefaultProtocol()
		want.ClientVersion = 7
		check(t, gchatmeow.ClientOpts{Protocol: gchatmeow.Protocol{ClientVersion: 7}}, want)
	})
}

func TestPresenceSelectsThePing(t *testing.T) {
	active := sentPing(t, connectOnce(t, gchatmeow.ClientOpts{}))
	if active.GetState() != pb.PingEvent_ACTIVE || active.GetApplicationFocusState() != pb.PingEvent_FOCUS_STATE_FOREGROUND ||
		active.GetClientInteractiveState() != pb.PingEvent_INTERACTIVE || !active.GetClientNotificationsEnabled() {
		t.Errorf("default ping = %v, want active, foreground, interactive with notifications", active)
	}
	inactive := sentPing(t, connectOnce(t, gchatmeow.ClientOpts{Presence: gchatmeow.PresenceInactive}))
	if inactive.GetState() != pb.PingEvent_INACTIVE || inactive.GetApplicationFocusState() != pb.PingEvent_FOCUS_STATE_BACKGROUND ||
		inactive.GetClientInteractiveState() != pb.PingEvent_HIDDEN || inactive.GetClientNotificationsEnabled() {
		t.Errorf("inactive ping = %v, want inactive, background, hidden without notifications", inactive)
	}
}

func TestParsePresence(t *testing.T) {
	for value, want := range map[string]gchatmeow.Presence{"": gchatmeow.PresenceActive, "active": gchatmeow.PresenceActive, "inactive": gchatmeow.PresenceInactive} {
		if got, err := gchatmeow.ParsePresence(value); err != nil || got != want {
			t.Errorf("ParsePresence(%q) = %q, %v; want %q", value, got, err, want)
		}
	}
	if _, err := gchatmeow.ParsePresence("away"); err == nil {
		t.Error("ParsePresence accepted an unknown value")
	}
}
