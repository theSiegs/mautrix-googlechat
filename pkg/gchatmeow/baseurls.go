package gchatmeow

import (
	"net/url"
	"strings"
)

// BaseURLs points the client at other hosts, for tests against a fake server; zero fields mean Google.
type BaseURLs struct {
	// Chat replaces https://chat.google.com/u/0 for /mole/world, /api/* and the webchannel.
	Chat string
	// Upload replaces https://chat.google.com/uploads.
	Upload string
}

// apply installs the overrides on c and lets the session send cookies to the overridden hosts.
func (b BaseURLs) apply(c *Client) {
	if b.Chat != "" {
		chat := strings.TrimSuffix(b.Chat, "/")
		c.baseURL = chat
		c.channelBaseURL = chat + "/webchannel/"
		c.session.moleWorldBaseURL = chat
		c.session.allowHostOf(chat)
	}
	if b.Upload != "" {
		c.uploadBaseURL = b.Upload
		c.session.allowHostOf(b.Upload)
	}
}

// allowHostOf adds rawURL's host to the cookie allowlist.
func (s *Session) allowHostOf(rawURL string) {
	u, err := url.Parse(rawURL)
	if err != nil || u.Hostname() == "" {
		return
	}
	s.mu.Lock()
	s.allowedHostSuffixes = append(s.allowedHostSuffixes, u.Hostname())
	s.mu.Unlock()
}
