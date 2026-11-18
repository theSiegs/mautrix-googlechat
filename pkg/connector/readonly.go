package connector

import (
	"fmt"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
)

// errReadOnly answers every Matrix action that would change the Google account in read-only mode.
var errReadOnly = fmt.Errorf("googlechat: the bridge is read-only (network.read_only), so nothing is sent to Google Chat: %w", gchatmeow.ErrReadOnly)

// gchatCapsReadOnly advertises no outgoing feature; receipts and typing are dropped silently.
var gchatCapsReadOnly = &event.RoomFeatures{
	MaxTextLength: MaxTextLength,
}

// gchatGeneralCapsReadOnly offers no chat creation.
var gchatGeneralCapsReadOnly = &bridgev2.NetworkGeneralCapabilities{}

// readOnly reports whether network.read_only is set.
func (gc *GChatConnector) readOnly() bool {
	return gc != nil && gc.Config.ReadOnly
}

// readOnly reports whether this login must not change anything on Google Chat.
func (c *GChatClient) readOnly() bool {
	return c.Main.readOnly()
}
