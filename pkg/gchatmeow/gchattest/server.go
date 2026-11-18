// Package gchattest is an in-process fake of the Google Chat web endpoints for offline tests.
package gchattest

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"unicode/utf16"

	"google.golang.org/protobuf/proto"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/pblite"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// MoleWorldMode selects what GET /mole/world answers.
type MoleWorldMode int

const (
	// MoleWorldOK serves a logged-in page carrying XSRFToken.
	MoleWorldOK MoleWorldMode = iota
	// MoleWorldSignedOut serves the AccountsSignInUi page.
	MoleWorldSignedOut
	// MoleWorldUnparseable serves a page with no WIZ_global_data, like a challenge page.
	MoleWorldUnparseable
)

// XSRFToken is the token the logged-in /mole/world page carries.
const XSRFToken = "fake-xsrf-token"

// Request is one request the server saw.
type Request struct {
	Method string
	Path   string
	// Endpoint is the RPC name for /api/* requests, else "".
	Endpoint string
	Query    url.Values
	Header   http.Header
	Body     []byte
}

// APIHandler answers one RPC from the binary request body; a non-zero status is sent as an error.
type APIHandler func(body []byte) (resp proto.Message, status int)

// Server is the fake. The zero value is not usable; call New.
type Server struct {
	srv *httptest.Server

	mu            sync.Mutex
	reqs          []Request
	api           map[string]APIHandler
	moleMode      MoleWorldMode
	moleStatus    int
	registerNoCSI bool
	registerCode  int
	eventsCode    int
	setCookies    []*http.Cookie
	sidSeq        int
	polls         map[string]*poll
	aid           int
}

// poll is the state of one webchannel SID.
type poll struct {
	acked  bool
	frames chan string
}

// New starts a fake server that is closed when the test ends.
func New(t testing.TB) *Server {
	t.Helper()
	s := &Server{api: map[string]APIHandler{}, polls: map[string]*poll{}}
	s.srv = httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(s.srv.Close)
	return s
}

// ChatURL is the base that stands in for https://chat.google.com/u/0.
func (s *Server) ChatURL() string { return s.srv.URL + "/u/0" }

// UploadURL stands in for https://chat.google.com/uploads.
func (s *Server) UploadURL() string { return s.srv.URL + "/uploads" }

// Handle registers the handler for one RPC endpoint name.
func (s *Server) Handle(endpoint string, h APIHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.api[endpoint] = h
}

// HandleProto answers endpoint with a fixed message.
func (s *Server) HandleProto(endpoint string, resp proto.Message) {
	s.Handle(endpoint, func([]byte) (proto.Message, int) { return resp, 0 })
}

// HandleStatus answers endpoint with a bare HTTP error status.
func (s *Server) HandleStatus(endpoint string, status int) {
	s.Handle(endpoint, func([]byte) (proto.Message, int) { return nil, status })
}

// SetMoleWorld selects the /mole/world page; status 0 means 200.
func (s *Server) SetMoleWorld(mode MoleWorldMode, status int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.moleMode, s.moleStatus = mode, status
}

// SetRegister makes register omit its COMPASS cookie and/or answer with status (0 means 200).
func (s *Server) SetRegister(omitCompass bool, status int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.registerNoCSI, s.registerCode = omitCompass, status
}

// SetEventsStatus makes every webchannel events request answer with status; 0 restores normal service.
func (s *Server) SetEventsStatus(status int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.eventsCode = status
}

// SetCookieOnResponses adds a Set-Cookie to every later response; nil clears them.
func (s *Server) SetCookieOnResponses(c *http.Cookie) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c == nil {
		s.setCookies = nil
		return
	}
	s.setCookies = append(s.setCookies, c)
}

// PushEvent streams one event to every open long poll.
func (s *Server) PushEvent(ev *pb.Event) error {
	raw, err := pblite.Marshal(&pb.StreamEventsResponse{Event: ev})
	if err != nil {
		return err
	}
	s.PushRaw("[" + string(raw) + "]")
	return nil
}

// PushRaw streams one data_array (JSON) to every open long poll.
func (s *Server) PushRaw(dataArray string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.aid++
	frame := fmt.Sprintf("[[%d,%s]]", s.aid, dataArray)
	for _, p := range s.polls {
		select {
		case p.frames <- frame:
		default:
		}
	}
}

// Requests returns a copy of the request log.
func (s *Server) Requests() []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Request(nil), s.reqs...)
}

// Count returns how many requests hit the RPC endpoint name.
func (s *Server) Count(endpoint string) int {
	n := 0
	for _, r := range s.Requests() {
		if r.Endpoint == endpoint {
			n++
		}
	}
	return n
}

// CountPath returns how many requests had a path ending in suffix.
func (s *Server) CountPath(suffix string) int {
	n := 0
	for _, r := range s.Requests() {
		if strings.HasSuffix(r.Path, suffix) {
			n++
		}
	}
	return n
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	rec := Request{Method: r.Method, Path: r.URL.Path, Query: r.URL.Query(), Header: r.Header.Clone(), Body: body}
	if i := strings.Index(r.URL.Path, "/api/"); i >= 0 {
		rec.Endpoint = r.URL.Path[i+len("/api/"):]
	}
	s.mu.Lock()
	s.reqs = append(s.reqs, rec)
	for _, c := range s.setCookies {
		http.SetCookie(w, c)
	}
	s.mu.Unlock()

	switch {
	case strings.HasSuffix(r.URL.Path, "/mole/world"):
		s.serveMoleWorld(w)
	case strings.HasSuffix(r.URL.Path, "/webchannel/register"):
		s.serveRegister(w)
	case strings.HasSuffix(r.URL.Path, "/webchannel/events"):
		s.serveEvents(w, r)
	case rec.Endpoint != "":
		s.serveAPI(w, rec)
	default:
		http.Error(w, "gchattest: unexpected request "+r.URL.Path, http.StatusNotFound)
	}
}

func (s *Server) serveMoleWorld(w http.ResponseWriter) {
	s.mu.Lock()
	mode, status := s.moleMode, s.moleStatus
	s.mu.Unlock()
	if status != 0 {
		w.WriteHeader(status)
	}
	switch mode {
	case MoleWorldSignedOut:
		io.WriteString(w, `<html><script>window.WIZ_global_data = {"qwAQke":"AccountsSignInUi"};</script></html>`)
	case MoleWorldUnparseable:
		io.WriteString(w, `<html><body>verify it is you</body></html>`)
	default:
		io.WriteString(w, `<html><script>window.WIZ_global_data = {"qwAQke":"ChatUi","SMqcke":"`+XSRFToken+`"};</script></html>`)
	}
}

func (s *Server) serveRegister(w http.ResponseWriter) {
	s.mu.Lock()
	omit, status := s.registerNoCSI, s.registerCode
	s.mu.Unlock()
	if !omit {
		http.SetCookie(w, &http.Cookie{Name: "COMPASS", Value: "dynamite-ui=fake-csessionid", Path: "/u/0/webchannel/"})
	}
	if status != 0 {
		w.WriteHeader(status)
	}
}

func (s *Server) serveEvents(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	status := s.eventsCode
	s.mu.Unlock()
	if status != 0 {
		w.WriteHeader(status)
		return
	}
	if r.Method == http.MethodPost {
		return
	}
	sid := r.URL.Query().Get("SID")
	if sid == "null" {
		s.mu.Lock()
		s.sidSeq++
		sid = fmt.Sprintf("fake-sid-%d", s.sidSeq)
		p := &poll{frames: make(chan string, 64)}
		s.polls[sid] = p
		s.mu.Unlock()
		w.Header().Set("X-HTTP-Initial-Response", fmt.Sprintf(`[[0,["c",%q,"",8,12]]]`, sid))
		s.stream(w, r, p)
		return
	}
	s.mu.Lock()
	p := s.polls[sid]
	firstGet := p != nil && !p.acked
	if firstGet {
		p.acked = true
	}
	s.mu.Unlock()
	if p == nil {
		http.Error(w, "Unknown SID", http.StatusBadRequest)
		return
	}
	if firstGet {
		return
	}
	s.stream(w, r, p)
}

// stream writes frames until the client goes away; a "noop" frame opens the stream so the client reports Connected.
func (s *Server) stream(w http.ResponseWriter, r *http.Request, p *poll) {
	w.WriteHeader(http.StatusOK)
	writeFrame(w, `[[0,["noop"]]]`)
	for {
		select {
		case <-r.Context().Done():
			return
		case frame := <-p.frames:
			writeFrame(w, frame)
		}
	}
}

func writeFrame(w http.ResponseWriter, payload string) {
	fmt.Fprintf(w, "%d\n%s", len(utf16.Encode([]rune(payload))), payload)
	if fl, ok := w.(http.Flusher); ok {
		fl.Flush()
	}
}

func (s *Server) serveAPI(w http.ResponseWriter, rec Request) {
	s.mu.Lock()
	h := s.api[rec.Endpoint]
	s.mu.Unlock()
	if h == nil {
		http.Error(w, "gchattest: no handler for "+rec.Endpoint, http.StatusNotImplemented)
		return
	}
	resp, status := h(rec.Body)
	if status != 0 {
		w.WriteHeader(status)
		return
	}
	out, err := proto.Marshal(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Write(out)
}
