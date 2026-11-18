package connector

import (
	"sync/atomic"
	"time"

	"maunium.net/go/mautrix/bridgev2/status"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
)

const (
	// GChatEmptyChatList: the chat list came back empty although this login has bridged chats.
	GChatEmptyChatList status.BridgeStateErrorCode = "gchat-empty-chat-list"
	// GChatSessionUnverifiable: /mole/world keeps answering with a page the bridge cannot read.
	GChatSessionUnverifiable status.BridgeStateErrorCode = "gchat-session-unverifiable"
)

// moleWorldUnparseableAlarm consecutive unreadable /mole/world answers raise GChatSessionUnverifiable.
const moleWorldUnparseableAlarm = 5

// driftWarnings are conditions that are not disconnects but mean the protocol or session drifted.
type driftWarnings struct {
	emptyChatList       bool
	sessionUnverifiable bool
}

// decorate adds the active warning to a connected or reconnecting state; other states speak for themselves.
func (w driftWarnings) decorate(bs status.BridgeState) status.BridgeState {
	switch {
	case bs.StateEvent != status.StateConnected && bs.StateEvent != status.StateTransientDisconnect:
	case w.sessionUnverifiable:
		bs.Error = GChatSessionUnverifiable
		bs.UserAction = status.UserActionRelogin
	case w.emptyChatList && bs.StateEvent == status.StateConnected:
		bs.Error = GChatEmptyChatList
	}
	return bs
}

// setWarning updates the warnings and, when something changed, re-sends the current state so it shows at once.
func (c *GChatClient) setWarning(update func(*driftWarnings)) {
	c.mu.Lock()
	before := c.warnings
	update(&c.warnings)
	changed := before != c.warnings
	state, reported := c.lastConnState, c.connStateReported
	c.mu.Unlock()
	if changed && reported {
		c.reportState(state, nil)
	}
}

// loginObserver turns gchatmeow health signals into drift warnings and last-activity times.
type loginObserver struct {
	c *GChatClient
}

var _ gchatmeow.Observer = loginObserver{}

func (o loginObserver) Request(string, int) {}
func (o loginObserver) DecodeSkip(string)   {}

func (o loginObserver) MoleWorld(result gchatmeow.MoleWorldResult, consecutiveUnparseable int) {
	switch {
	case result == gchatmeow.MoleWorldUnparseable && consecutiveUnparseable >= moleWorldUnparseableAlarm:
		o.c.setWarning(func(w *driftWarnings) { w.sessionUnverifiable = true })
	case result == gchatmeow.MoleWorldOK || result == gchatmeow.MoleWorldSignedOut:
		o.c.setWarning(func(w *driftWarnings) { w.sessionUnverifiable = false })
	}
}

func (o loginObserver) ChannelConnected() { o.c.lastChannelConnect.Store(time.Now().UnixMilli()) }
func (o loginObserver) StreamEvent()      { o.c.lastStreamEvent.Store(time.Now().UnixMilli()) }

// activityTimes holds the last channel connect and stream event as Unix milliseconds; 0 means never.
type activityTimes struct {
	lastChannelConnect atomic.Int64
	lastStreamEvent    atomic.Int64
}

// LastStreamEvent returns when the last Google Chat event arrived, or the zero time.
func (c *GChatClient) LastStreamEvent() time.Time {
	if ms := c.lastStreamEvent.Load(); ms != 0 {
		return time.UnixMilli(ms)
	}
	return time.Time{}
}
