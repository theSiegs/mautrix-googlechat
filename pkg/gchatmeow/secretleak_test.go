package gchatmeow

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// TestCookieValuesNeverReachLogsOrErrors drives the failure paths with sentinel cookie values.
func TestCookieValuesNeverReachLogsOrErrors(t *testing.T) {
	const sentinel = "SENTINEL-COOKIE-VALUE"
	var logs bytes.Buffer
	prev := log.Logger
	log.Logger = zerolog.New(&logs).Level(zerolog.TraceLevel)
	t.Cleanup(func() { log.Logger = prev })

	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "__Secure-1PSIDTS", Value: sentinel + "-rotated", Path: "/"})
		switch {
		case strings.HasSuffix(r.URL.Path, "/mole/world"):
			w.Write([]byte(signInWizHTML))
		default:
			w.WriteHeader(status)
			w.Write([]byte("request rejected"))
		}
	}))
	cookies := map[string]string{}
	for _, name := range append(append([]string{}, RequiredCookies...), OptionalCookies...) {
		cookies[name] = sentinel + "-" + name
	}
	c, err := NewClient(ClientOpts{Cookies: cookies, BaseURLs: BaseURLs{Chat: srv.URL + "/u/0", Upload: srv.URL + "/uploads"}})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c.xsrfRefreshInterval = 0
	ctx := context.Background()

	var errs []error
	errs = append(errs, c.FetchXSRFToken(ctx))
	for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusBadRequest, http.StatusInternalServerError} {
		status = code
		_, err := c.GetGroup(ctx, &pb.GetGroupRequest{})
		errs = append(errs, err)
		_, err = c.UploadFile(ctx, "g", []byte("x"), "a.txt", "text/plain")
		errs = append(errs, err)
		errs = append(errs, c.buildChannel().Listen(ctx, 0, 0))
	}
	errs = append(errs, c.Connect(ctx))
	srv.Close()
	_, err = c.GetGroup(ctx, &pb.GetGroupRequest{})
	errs = append(errs, err, c.buildChannel().Listen(ctx, 0, 0))

	for i, err := range errs {
		if err == nil {
			t.Errorf("failure path %d returned no error", i)
		} else if strings.Contains(err.Error(), sentinel) {
			t.Errorf("failure path %d leaks a cookie value in its error", i)
		}
	}
	if strings.Contains(logs.String(), sentinel) {
		t.Error("a cookie value was logged")
	}
	if c.Cookies()["__Secure-1PSIDTS"] != sentinel+"-rotated" {
		t.Error("the sentinel cookies were not in play (rotation was not absorbed)")
	}
}
