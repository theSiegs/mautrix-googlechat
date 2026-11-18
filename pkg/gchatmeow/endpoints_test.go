package gchatmeow

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// apiCallNames returns the endpoint literal of every c.call(...) in api.go.
func apiCallNames(t *testing.T) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "api.go", nil, 0)
	if err != nil {
		t.Fatalf("parse api.go: %v", err)
	}
	var names []string
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || len(call.Args) < 2 {
			return true
		}
		switch sel.Sel.Name {
		case "call":
			lit, ok := call.Args[1].(*ast.BasicLit)
			if !ok {
				t.Errorf("a call(...) in api.go does not name its endpoint with a string literal")
				return true
			}
			name, _ := strconv.Unquote(lit.Value)
			names = append(names, name)
		case "doRequestWithPolicy", "doRequestOnce":
			if _, isLit := call.Args[1].(*ast.BasicLit); isLit {
				t.Errorf("api.go calls %s with a literal endpoint, bypassing the endpoint table", sel.Sel.Name)
			}
		}
		return true
	})
	return names
}

func TestEveryAPIEndpointIsInTheTable(t *testing.T) {
	names := apiCallNames(t)
	if len(names) == 0 {
		t.Fatal("found no call(...) in api.go; the scan is broken")
	}
	used := map[string]bool{}
	for _, name := range names {
		used[name] = true
		if _, ok := endpointByName[name]; !ok {
			t.Errorf("api.go uses endpoint %q, which is missing from the endpoint table", name)
		}
	}
	for _, ep := range Endpoints() {
		if !used[ep.Name] {
			t.Errorf("endpoint table lists %q, which api.go never calls", ep.Name)
		}
	}
	if len(endpointByName) != len(endpointTable) {
		t.Error("the endpoint table has a duplicate name")
	}
}

func TestEndpointTableClassifiesReadsAndWrites(t *testing.T) {
	for _, ep := range Endpoints() {
		reads := strings.HasPrefix(ep.Name, "get_") || strings.HasPrefix(ep.Name, "list_") ||
			strings.HasPrefix(ep.Name, "catch_up_") || ep.Name == "paginated_world"
		if reads == ep.Mutates {
			t.Errorf("endpoint %q: Mutates=%v does not match its name", ep.Name, ep.Mutates)
		}
	}
	for _, name := range []string{"create_topic", "create_message", "create_dm", "create_group"} {
		if endpointByName[name].RetryOn5xx {
			t.Errorf("endpoint %q must not be retried on a 5xx: it creates something", name)
		}
	}
}

// rpcMethods lists the exported Client methods that take (ctx, *Request) and return (*Response, error).
func rpcMethods(c *Client) map[string]reflect.Value {
	out := map[string]reflect.Value{}
	v := reflect.ValueOf(c)
	for i := 0; i < v.NumMethod(); i++ {
		m := v.Type().Method(i)
		ft := m.Type
		if ft.NumIn() != 3 || ft.NumOut() != 2 || ft.In(2).Kind() != reflect.Ptr {
			continue
		}
		if !strings.HasSuffix(ft.In(2).Elem().Name(), "Request") {
			continue
		}
		out[m.Name] = v.Method(i)
	}
	return out
}

// TestReadOnlyRefusesEveryMutatingCallBeforeAnyHTTP calls every RPC method on a read-only client.
func TestReadOnlyRefusesEveryMutatingCallBeforeAnyHTTP(t *testing.T) {
	var mutatingHits atomic.Int64
	var readHits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		if ep, ok := endpointByName[name]; ok && !ep.Mutates {
			readHits.Add(1)
			return
		}
		mutatingHits.Add(1)
	}))
	defer srv.Close()
	c, err := NewClient(ClientOpts{ReadOnly: true, BaseURLs: BaseURLs{Chat: srv.URL + "/u/0", Upload: srv.URL + "/uploads"}})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ctx := context.Background()

	refused, allowed := 0, 0
	for name, method := range rpcMethods(c) {
		req := reflect.New(method.Type().In(1).Elem())
		out := method.Call([]reflect.Value{reflect.ValueOf(ctx), req})
		callErr, _ := out[1].Interface().(error)
		if errors.Is(callErr, ErrReadOnly) {
			refused++
			continue
		}
		if callErr != nil {
			t.Errorf("%s on a read-only client failed with %v, want success or ErrReadOnly", name, callErr)
		}
		allowed++
	}
	wantRefused := 0
	for _, ep := range Endpoints() {
		if ep.Mutates {
			wantRefused++
		}
	}
	if refused != wantRefused || allowed != len(endpointTable)-wantRefused {
		t.Errorf("refused %d and allowed %d calls, want %d and %d", refused, allowed, wantRefused, len(endpointTable)-wantRefused)
	}

	if _, err := c.UploadFile(ctx, "g", []byte("x"), "a.txt", "text/plain"); !errors.Is(err, ErrReadOnly) {
		t.Errorf("UploadFile error = %v, want ErrReadOnly", err)
	}
	if err := c.SendStreamEvent(ctx, &pb.StreamEventsRequest{}); !errors.Is(err, ErrReadOnly) {
		t.Errorf("SendStreamEvent error = %v, want ErrReadOnly", err)
	}
	if mutatingHits.Load() != 0 {
		t.Errorf("%d requests reached a mutating path in read-only mode, want 0", mutatingHits.Load())
	}
	if int(readHits.Load()) != allowed {
		t.Errorf("%d read requests reached the server, want %d", readHits.Load(), allowed)
	}
}

func TestUnregisteredEndpointIsRefused(t *testing.T) {
	c, err := NewClient(ClientOpts{})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	err = c.call(context.Background(), "not_a_real_endpoint", &pb.GetGroupRequest{}, &pb.GetGroupResponse{})
	if err == nil || !strings.Contains(err.Error(), "endpoint table") {
		t.Errorf("call with an unknown endpoint returned %v, want the endpoint-table error", err)
	}
}
