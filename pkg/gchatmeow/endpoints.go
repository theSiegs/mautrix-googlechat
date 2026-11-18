package gchatmeow

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"google.golang.org/protobuf/proto"
)

// ErrReadOnly is returned, before any HTTP, for a call that would change the account while ClientOpts.ReadOnly is set.
var ErrReadOnly = errors.New("gchatmeow: refused in read-only mode")

// Endpoint describes one /api/* RPC.
type Endpoint struct {
	Name string
	// Mutates marks calls that change anything on the account, visible to others or not.
	Mutates bool
	// RetryOn5xx is false where a retry after a 5xx could create a duplicate.
	RetryOn5xx bool
}

// endpointTable is the single list of RPCs the client may call; api.go names its calls from here.
var endpointTable = []Endpoint{
	{Name: "catch_up_group", RetryOn5xx: true},
	{Name: "catch_up_user", RetryOn5xx: true},
	{Name: "get_group", RetryOn5xx: true},
	{Name: "get_members", RetryOn5xx: true},
	{Name: "get_self_user_status", RetryOn5xx: true},
	{Name: "get_user_presence", RetryOn5xx: true},
	{Name: "list_messages", RetryOn5xx: true},
	{Name: "list_topics", RetryOn5xx: true},
	{Name: "paginated_world", RetryOn5xx: true},

	{Name: "create_dm", Mutates: true},
	{Name: "create_group", Mutates: true},
	{Name: "create_membership", Mutates: true, RetryOn5xx: true},
	{Name: "create_message", Mutates: true},
	{Name: "create_topic", Mutates: true},
	{Name: "delete_message", Mutates: true, RetryOn5xx: true},
	{Name: "edit_message", Mutates: true, RetryOn5xx: true},
	{Name: "hide_group", Mutates: true, RetryOn5xx: true},
	{Name: "mark_group_readstate", Mutates: true, RetryOn5xx: true},
	{Name: "remove_memberships", Mutates: true, RetryOn5xx: true},
	{Name: "set_typing_state", Mutates: true, RetryOn5xx: true},
	{Name: "update_group", Mutates: true, RetryOn5xx: true},
	{Name: "update_group_notification_settings", Mutates: true, RetryOn5xx: true},
	{Name: "update_reaction", Mutates: true, RetryOn5xx: true},
}

var endpointByName = func() map[string]Endpoint {
	m := make(map[string]Endpoint, len(endpointTable))
	for _, ep := range endpointTable {
		m[ep.Name] = ep
	}
	return m
}()

// Endpoints returns the RPC table sorted by name, for tests and metrics labels.
func Endpoints() []Endpoint {
	out := append([]Endpoint(nil), endpointTable...)
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// call runs one registered RPC, refusing mutating ones in read-only mode before any HTTP.
func (c *Client) call(ctx context.Context, name string, requestPB, responsePB proto.Message) error {
	ep, ok := endpointByName[name]
	if !ok {
		return fmt.Errorf("gchatmeow: endpoint %q is not in the endpoint table", name)
	}
	if ep.Mutates && c.readOnly {
		return fmt.Errorf("%s: %w", name, ErrReadOnly)
	}
	return c.doRequestWithPolicy(ctx, name, requestPB, responsePB, ep.RetryOn5xx)
}

// ReadOnly reports whether the client refuses mutating calls.
func (c *Client) ReadOnly() bool {
	return c.readOnly
}
