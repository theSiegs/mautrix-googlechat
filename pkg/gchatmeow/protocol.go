package gchatmeow

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// Protocol holds the web-client constants Google may rotate; zero fields mean the built-in defaults.
type Protocol struct {
	// APIKey is the `key` query param of every /api/* call.
	APIKey string
	// ClientVersion is RequestHeader.client_version.
	ClientVersion int64
	// MoleWorldHS is the shell-state blob sent as the `hs` query param of /mole/world.
	MoleWorldHS string
}

// DefaultProtocol returns the built-in constants.
func DefaultProtocol() Protocol {
	return Protocol{APIKey: apiKey, ClientVersion: clientVersion, MoleWorldHS: moleWorldHS}
}

// withDefaults fills zero fields from DefaultProtocol.
func (p Protocol) withDefaults() Protocol {
	def := DefaultProtocol()
	if p.APIKey == "" {
		p.APIKey = def.APIKey
	}
	if p.ClientVersion == 0 {
		p.ClientVersion = def.ClientVersion
	}
	if p.MoleWorldHS == "" {
		p.MoleWorldHS = def.MoleWorldHS
	}
	return p
}

// Presence is what the channel ping tells Google about this client.
type Presence string

const (
	// PresenceActive declares an active, focused, interactive client with notifications on (the web default).
	PresenceActive Presence = "active"
	// PresenceInactive declares an idle background client that shows no notifications itself.
	PresenceInactive Presence = "inactive"
)

// ParsePresence maps a config value to a Presence; empty means active.
func ParsePresence(value string) (Presence, error) {
	switch Presence(value) {
	case "", PresenceActive:
		return PresenceActive, nil
	case PresenceInactive:
		return PresenceInactive, nil
	default:
		return "", fmt.Errorf("gchatmeow: unknown presence %q (want active or inactive)", value)
	}
}

// pingEvent builds the PingEvent that starts the stream for presence p.
func (p Presence) pingEvent() *pb.PingEvent {
	if p == PresenceInactive {
		return &pb.PingEvent{
			State:                      pb.PingEvent_INACTIVE.Enum(),
			ApplicationFocusState:      pb.PingEvent_FOCUS_STATE_BACKGROUND.Enum(),
			ClientInteractiveState:     pb.PingEvent_HIDDEN.Enum(),
			ClientNotificationsEnabled: proto.Bool(false),
		}
	}
	return &pb.PingEvent{
		State:                      pb.PingEvent_ACTIVE.Enum(),
		ApplicationFocusState:      pb.PingEvent_FOCUS_STATE_FOREGROUND.Enum(),
		ClientInteractiveState:     pb.PingEvent_INTERACTIVE.Enum(),
		ClientNotificationsEnabled: proto.Bool(true),
	}
}

// requestHeader builds the RequestHeader of an RPC with the configured client version.
func (c *Client) requestHeader() *pb.RequestHeader {
	hdr := newRequestHeader()
	if c.protocol.ClientVersion != 0 {
		hdr.ClientVersion = proto.Int64(c.protocol.ClientVersion)
	}
	return hdr
}
