package gchatmeow

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	// deadSessionFailureThreshold consecutive non-network channel failures force a /mole/world check.
	deadSessionFailureThreshold = 3
	// sessionCheckConfirmDelay separates the two sign-in pages needed to call a session dead.
	sessionCheckConfirmDelay = 30 * time.Second
	// sessionCheckMinInterval limits checks triggered by RPC 401/403s, which may be per-request refusals.
	sessionCheckMinInterval = 5 * time.Minute
)

// ErrRegisterNoSession is returned when register answers 200 without the webchannel COMPASS cookie.
var ErrRegisterNoSession = errors.New("gchatmeow: register returned no webchannel session")

func isForbiddenStatus(err error) bool {
	var ue *UnexpectedStatusError
	return errors.As(err, &ue) && ue.Status == http.StatusForbidden
}

// verifySession asks /mole/world whether the cookies are dead; true needs two sign-in pages in a row.
func (c *Client) verifySession(ctx context.Context) bool {
	for attempt := 1; ; attempt++ {
		err := c.forceTokenRefresh(ctx)
		if !IsAuthError(err) {
			if err != nil {
				log.Warn().Err(err).Msg("gchatmeow: session check was inconclusive")
			}
			return false
		}
		if attempt >= xsrfRefreshAuthFailuresBeforeGivingUp {
			return true
		}
		if c.sleep(ctx, sessionCheckConfirmDelay) != nil {
			return false
		}
	}
}

// forceTokenRefresh always fetches /mole/world, unlike refreshXSRFToken which shares a recent fetch.
func (c *Client) forceTokenRefresh(ctx context.Context) error {
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()
	token, err := c.session.FetchXSRFToken(ctx)
	if err != nil {
		return err
	}
	c.SetXSRFToken(token)
	c.mu.Lock()
	c.lastTokenRefresh = c.now()
	c.mu.Unlock()
	return nil
}

// suspectSession starts one rate-limited background session check after an RPC came back 401 or 403.
// A confirmed dead session is reported as bad credentials and stops Connect.
func (c *Client) suspectSession() {
	c.mu.Lock()
	ctx := c.connCtx
	now := c.now()
	skip := ctx == nil || c.sessionCheckRunning ||
		(!c.lastSessionCheck.IsZero() && now.Sub(c.lastSessionCheck) < sessionCheckMinInterval)
	if !skip {
		c.sessionCheckRunning = true
		c.lastSessionCheck = now
	}
	c.mu.Unlock()
	if skip {
		return
	}
	go func() {
		dead := c.verifySession(ctx)
		c.mu.Lock()
		c.sessionCheckRunning = false
		c.mu.Unlock()
		if dead && ctx.Err() == nil {
			log.Warn().Msg("gchatmeow: session check after a rejected request found the session logged out")
			c.emitState(ConnStateBadCredentials, ErrNotLoggedIn)
			c.Disconnect()
		}
	}()
}

// now is the client clock; tests replace it through ClientOpts.Now.
func (c *Client) now() time.Time {
	if c.nowFn != nil {
		return c.nowFn()
	}
	return time.Now()
}
