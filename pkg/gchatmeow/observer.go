package gchatmeow

import (
	"errors"
)

// ErrMoleWorldUnparseable wraps every /mole/world answer that is a 200 but carries no usable WIZ_global_data.
var ErrMoleWorldUnparseable = errors.New("gchatmeow: /mole/world response is not parseable")

// MoleWorldResult classifies one /mole/world fetch.
type MoleWorldResult int

const (
	// MoleWorldOK: the page carried an XSRF token.
	MoleWorldOK MoleWorldResult = iota
	// MoleWorldSignedOut: the page was the sign-in UI.
	MoleWorldSignedOut
	// MoleWorldUnparseable: a 200 the client could not read, such as a challenge page or a changed layout.
	MoleWorldUnparseable
	// MoleWorldFailed: no usable HTTP answer (network error or an error status).
	MoleWorldFailed
)

// Observer receives protocol-health signals for metrics and drift alarms.
// Methods are called from request and channel goroutines, so they must be cheap and safe for concurrent use.
type Observer interface {
	// Request reports one finished RPC; status is the HTTP status, or 0 when there was no response.
	Request(endpoint string, status int)
	// MoleWorld reports one /mole/world fetch; consecutiveUnparseable counts unparseable answers in a row.
	MoleWorld(result MoleWorldResult, consecutiveUnparseable int)
	// DecodeSkip reports a stream frame the decoder had to skip; kind is "frame" or "stream_event".
	DecodeSkip(kind string)
	// ChannelConnected reports that a webchannel started delivering.
	ChannelConnected()
	// StreamEvent reports one event handed to OnStreamEvent.
	StreamEvent()
}

// noopObserver is used when no Observer is set.
type noopObserver struct{}

func (noopObserver) Request(string, int)            {}
func (noopObserver) MoleWorld(MoleWorldResult, int) {}
func (noopObserver) DecodeSkip(string)              {}
func (noopObserver) ChannelConnected()              {}
func (noopObserver) StreamEvent()                   {}

// SetObserver installs o (nil removes it); call it before Connect.
func (c *Client) SetObserver(o Observer) {
	c.mu.Lock()
	c.observer = o
	c.mu.Unlock()
}

// obs returns the installed Observer or a no-op.
func (c *Client) obs() Observer {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.observer == nil {
		return noopObserver{}
	}
	return c.observer
}

// statusOf extracts the HTTP status an error stands for: 200 for nil, 0 when there was no response.
func statusOf(err error) int {
	if err == nil {
		return 200
	}
	var ue *UnexpectedStatusError
	if errors.As(err, &ue) {
		return ue.Status
	}
	return 0
}

// noteMoleWorld classifies a /mole/world result, keeps the unparseable streak and tells the Observer.
func (c *Client) noteMoleWorld(err error) {
	result := MoleWorldOK
	switch {
	case err == nil:
	case errors.Is(err, ErrNotLoggedIn):
		result = MoleWorldSignedOut
	case errors.Is(err, ErrMoleWorldUnparseable):
		result = MoleWorldUnparseable
	default:
		result = MoleWorldFailed
	}
	c.mu.Lock()
	switch result {
	case MoleWorldUnparseable:
		c.moleWorldUnparseable++
	case MoleWorldOK, MoleWorldSignedOut:
		c.moleWorldUnparseable = 0
	}
	streak := c.moleWorldUnparseable
	c.mu.Unlock()
	c.obs().MoleWorld(result, streak)
}
