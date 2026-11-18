package connector

import (
	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
)

// clientOpts builds the gchatmeow options for a login from its cookies, captured User-Agent and the config.
func (gc *GChatConnector) clientOpts(cookies map[string]string, userAgent string) gchatmeow.ClientOpts {
	opts := gchatmeow.ClientOpts{Cookies: cookies, UserAgent: userAgent}
	if gc == nil {
		return opts
	}
	if gc.Config.UserAgent != "" {
		opts.UserAgent = gc.Config.UserAgent
	}
	// PostProcess already rejected unknown values, so an error here can only mean "active".
	opts.Presence, _ = gchatmeow.ParsePresence(gc.Config.Presence)
	opts.Protocol = gchatmeow.Protocol{
		APIKey:        gc.Config.Protocol.APIKey,
		ClientVersion: gc.Config.Protocol.ClientVersion,
		MoleWorldHS:   gc.Config.Protocol.MoleWorldHS,
	}
	opts.ReadOnly = gc.Config.ReadOnly
	opts.BaseURLs = gc.baseURLs
	return opts
}
