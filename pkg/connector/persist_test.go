package connector

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/gchattest"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// newFakeServerConn builds a real gchatmeow.Client that talks to srv.
func newFakeServerConn(t *testing.T, srv *gchattest.Server, opts gchatmeow.ClientOpts) *gchatmeow.Client {
	t.Helper()
	if opts.Cookies == nil {
		opts.Cookies = fakeCookies()
	}
	opts.BaseURLs = gchatmeow.BaseURLs{Chat: srv.ChatURL(), Upload: srv.UploadURL()}
	conn, err := gchatmeow.NewClient(opts)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return conn
}

// saveRecorder is a saveFn that snapshots the stored cookies on every save.
type saveRecorder struct {
	mu    sync.Mutex
	meta  *UserLoginMetadata
	saves []map[string]string
}

func (r *saveRecorder) save(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	snap := map[string]string{}
	for k, v := range r.meta.Cookies {
		snap[k] = v
	}
	r.saves = append(r.saves, snap)
	return nil
}

func (r *saveRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.saves)
}

func (r *saveRecorder) last() map[string]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.saves) == 0 {
		return nil
	}
	return r.saves[len(r.saves)-1]
}

// rotatingLogin wires a GChatClient to a fake server that rotates one cookie on every response.
func rotatingLogin(t *testing.T, debounce time.Duration) (*GChatClient, *gchatmeow.Client, *saveRecorder, *gchattest.Server) {
	t.Helper()
	srv := gchattest.New(t)
	srv.HandleProto("get_group", &pb.GetGroupResponse{})
	srv.SetCookieOnResponses(&http.Cookie{Name: "__Secure-1PSIDTS", Value: "rotated", Path: "/"})
	meta := &UserLoginMetadata{Cookies: fakeCookies()}
	rec := &saveRecorder{meta: meta}
	gc := &GChatClient{UserLogin: newTestUserLogin(meta), saveFn: rec.save, cookieSaveDebounce: debounce}
	conn := newFakeServerConn(t, srv, gchatmeow.ClientOpts{})
	gc.installConn(context.Background(), conn)
	return gc, conn, rec, srv
}

func TestRotatedCookieIsSavedWithinTheDebounceWithoutAReconnect(t *testing.T) {
	_, conn, rec, srv := rotatingLogin(t, 20*time.Millisecond)
	for i := 0; i < 3; i++ {
		if _, err := conn.GetGroup(context.Background(), &pb.GetGroupRequest{}); err != nil {
			t.Fatalf("GetGroup: %v", err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for rec.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := rec.last()["__Secure-1PSIDTS"]; got != "rotated" {
		t.Fatalf("saved __Secure-1PSIDTS = %q, want the rotated value", got)
	}
	if rec.count() != 1 {
		t.Errorf("saves = %d, want one debounced save for one rotation", rec.count())
	}
	if n := srv.CountPath("/webchannel/register"); n != 0 {
		t.Errorf("register calls = %d, want none: the save must not need a reconnect", n)
	}
}

func TestDisconnectSavesUnsavedCookies(t *testing.T) {
	gc, conn, rec, _ := rotatingLogin(t, time.Hour)
	if _, err := conn.GetGroup(context.Background(), &pb.GetGroupRequest{}); err != nil {
		t.Fatalf("GetGroup: %v", err)
	}
	gc.Disconnect()
	if got := rec.last()["__Secure-1PSIDTS"]; got != "rotated" {
		t.Errorf("after Disconnect saved __Secure-1PSIDTS = %q, want the rotated value", got)
	}
	gc.Disconnect()
	if rec.count() != 1 {
		t.Errorf("saves = %d, want 1: a second Disconnect has nothing new to save", rec.count())
	}
}

func TestNoCookieSaveAfterLogout(t *testing.T) {
	gc, conn, rec, _ := rotatingLogin(t, 150*time.Millisecond)
	if _, err := conn.GetGroup(context.Background(), &pb.GetGroupRequest{}); err != nil {
		t.Fatalf("GetGroup: %v", err)
	}
	gc.LogoutRemote(context.Background())
	// A late response on the old conn must not re-arm a save either.
	conn.GetGroup(context.Background(), &pb.GetGroupRequest{})
	time.Sleep(300 * time.Millisecond)
	if rec.count() != 1 || len(rec.last()) != 0 {
		t.Errorf("saves = %d with %d cookies in the last one, want only the logout save with none", rec.count(), len(rec.last()))
	}
}
