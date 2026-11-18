package gchatmeow

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// cookieEchoServer records each request's Cookie header by path and runs respond first.
func cookieEchoServer(t *testing.T, respond func(w http.ResponseWriter, r *http.Request)) (*httptest.Server, func(path string) string) {
	t.Helper()
	seen := map[string]string{}
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen[r.URL.Path] = r.Header.Get("Cookie")
		mu.Unlock()
		if respond != nil {
			respond(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, func(path string) string {
		mu.Lock()
		defer mu.Unlock()
		return seen[path]
	}
}

func newCookieTestSession(t *testing.T, srv *httptest.Server, cookies map[string]string) *Session {
	t.Helper()
	sess, err := NewSession(cookies, "")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	sess.allowedHostSuffixes = []string{testServerHost(t, srv.URL)}
	return sess
}

func TestCookieNameCaseIsPreservedExceptLegacyFive(t *testing.T) {
	srv, cookieAt := cookieEchoServer(t, nil)
	sess := newCookieTestSession(t, srv, map[string]string{"sid": "a", "__Secure-1PSIDTS": "b"})
	if _, err := sess.Fetch(context.Background(), http.MethodGet, srv.URL+"/x", nil, nil); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if got, want := cookieAt("/x"), "SID=a; __Secure-1PSIDTS=b"; got != want {
		t.Errorf("Cookie header = %q, want %q", got, want)
	}
	if _, mangled := sess.Cookies()["__SECURE-1PSIDTS"]; mangled {
		t.Error("Cookies() holds an upper-cased __Secure- name")
	}
}

func TestCookieDeletedOnMaxAgeZeroOrEpochExpiry(t *testing.T) {
	srv, cookieAt := cookieEchoServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/delete" {
			w.Header().Add("Set-Cookie", "GONE1=; Max-Age=0; Path=/")
			w.Header().Add("Set-Cookie", "GONE2=x; Expires=Thu, 01 Jan 1970 00:00:00 GMT; Path=/")
		}
	})
	sess := newCookieTestSession(t, srv, map[string]string{"GONE1": "a", "GONE2": "b", "KEEP": "c"})
	changes := 0
	sess.SetOnCookiesChanged(func() { changes++ })
	for _, p := range []string{"/delete", "/after"} {
		if _, err := sess.Fetch(context.Background(), http.MethodGet, srv.URL+p, nil, nil); err != nil {
			t.Fatalf("Fetch %s: %v", p, err)
		}
	}
	if got := cookieAt("/after"); got != "KEEP=c" {
		t.Errorf("Cookie header after deletion = %q, want only KEEP", got)
	}
	if got := sess.Cookies(); len(got) != 1 || got["KEEP"] != "c" {
		t.Errorf("Cookies() = %d entries, want only KEEP", len(got))
	}
	if changes != 1 {
		t.Errorf("change callback ran %d times, want 1", changes)
	}
}

func TestWebchannelCompassStaysOutOfThePersistedCookies(t *testing.T) {
	srv, cookieAt := cookieEchoServer(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/register") {
			http.SetCookie(w, &http.Cookie{Name: "COMPASS", Value: "dynamite-ui=channel", Path: "/u/0/webchannel/"})
		}
	})
	sess := newCookieTestSession(t, srv, map[string]string{"COMPASS": "login", "SID": "s"})
	changes := 0
	sess.SetOnCookiesChanged(func() { changes++ })
	for _, p := range []string{"/u/0/webchannel/register", "/u/0/webchannel/events", "/u/0/api/get_group"} {
		if _, err := sess.Fetch(context.Background(), http.MethodGet, srv.URL+p, nil, nil); err != nil {
			t.Fatalf("Fetch %s: %v", p, err)
		}
	}
	if got := cookieAt("/u/0/webchannel/events"); got != "COMPASS=dynamite-ui=channel; SID=s" {
		t.Errorf("webchannel Cookie = %q, want the channel COMPASS", got)
	}
	if got := cookieAt("/u/0/api/get_group"); got != "COMPASS=login; SID=s" {
		t.Errorf("api Cookie = %q, want the login COMPASS", got)
	}
	if got := sess.Cookies()["COMPASS"]; got != "login" {
		t.Errorf("persisted COMPASS = %q, want the login value", got)
	}
	if changes != 0 {
		t.Errorf("change callback ran %d times for a channel-only cookie, want 0", changes)
	}
}

func TestCookieChangeCallbackFiresOnlyOnRealChanges(t *testing.T) {
	value := "v1"
	srv, _ := cookieEchoServer(t, func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "__Secure-1PSIDTS", Value: value, Path: "/"})
	})
	sess := newCookieTestSession(t, srv, map[string]string{"SID": "s"})
	changes := 0
	sess.SetOnCookiesChanged(func() { changes++ })
	fetch := func() {
		t.Helper()
		if _, err := sess.Fetch(context.Background(), http.MethodGet, srv.URL, nil, nil); err != nil {
			t.Fatalf("Fetch: %v", err)
		}
	}
	fetch()
	fetch()
	if changes != 1 {
		t.Fatalf("change callback ran %d times after one new cookie, want 1", changes)
	}
	value = "v2"
	fetch()
	if changes != 2 || sess.Cookies()["__Secure-1PSIDTS"] != "v2" {
		t.Errorf("after rotation: %d callbacks, want 2 and the rotated value stored", changes)
	}
}

func TestStaleUpperCasedSecureNamesAreDropped(t *testing.T) {
	sess, err := NewSession(map[string]string{"__SECURE-1PSIDTS": "stale", "__Secure-1PSIDTS": "live", "SID": "s"}, "")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	got := sess.Cookies()
	if len(got) != 2 || got["__Secure-1PSIDTS"] != "live" {
		t.Errorf("Cookies() has %d entries, want SID and the properly cased __Secure-1PSIDTS only", len(got))
	}
}
