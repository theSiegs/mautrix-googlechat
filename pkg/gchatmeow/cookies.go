package gchatmeow

import (
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// RequiredCookies are the cookies the login UI must collect (uppercased).
var RequiredCookies = []string{"COMPASS", "SSID", "SID", "OSID", "HSID"}

// OptionalCookies are the rest of Google's session family; newer accounts need the rotating *PSIDTS ones.
var OptionalCookies = []string{
	"APISID", "SAPISID", "SIDCC",
	"__Secure-1PSID", "__Secure-3PSID",
	"__Secure-1PAPISID", "__Secure-3PAPISID",
	"__Secure-1PSIDTS", "__Secure-3PSIDTS",
	"__Secure-1PSIDCC", "__Secure-3PSIDCC",
}

// CookieIsDomainSpecific reports whether the login UI must extract the chat.google.com copy of
// a cookie; SSID/SID/HSID live only on .google.com and must NOT be pinned.
func CookieIsDomainSpecific(cookieName string) bool {
	return cookieName == "COMPASS" || cookieName == "OSID"
}

// canonicalCookieName upper-cases only the five legacy names; every other name is case-sensitive.
func canonicalCookieName(name string) string {
	upper := strings.ToUpper(name)
	for _, legacy := range RequiredCookies {
		if upper == legacy {
			return upper
		}
	}
	return name
}

// webchannelPathMarker identifies requests and cookie paths that belong to the webchannel.
const webchannelPathMarker = "/webchannel"

// cookieJar is a name-keyed store with no domain scoping (host gating is in Session).
// It never quotes values, which Google rejects, so it is not wired into http.Client.Jar.
type cookieJar struct {
	mu      sync.RWMutex
	cookies map[string]string // name (as stored) -> value
	// channel holds webchannel-path cookies (the register COMPASS); never persisted.
	channel map[string]string
	// onChange runs, outside mu, after a persisted cookie was added, changed or removed.
	onChange func()
}

func newCookieJar() *cookieJar {
	return &cookieJar{cookies: make(map[string]string), channel: make(map[string]string)}
}

func (j *cookieJar) set(name, value string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.cookies[name] = value
}

// isDeletion reports whether c asks for removal: Max-Age<=0 or an epoch Expires.
func isDeletion(c *http.Cookie) bool {
	if c.MaxAge < 0 {
		return true
	}
	// Only the 1970 idiom counts, so a wrong local clock can never drop live cookies.
	return !c.Expires.IsZero() && c.Expires.Before(time.Unix(86400, 0))
}

// absorb ingests every Set-Cookie, capturing rotation and deletion; path is the request path.
func (j *cookieJar) absorb(resp *http.Response, path string) {
	changed := false
	j.mu.Lock()
	for _, c := range resp.Cookies() {
		cookiePath := c.Path
		if cookiePath == "" {
			cookiePath = path
		}
		store, persisted := j.cookies, true
		// A COMPASS from a webchannel request is the channel's own, whatever Path it claims.
		fromChannel := c.Name == "COMPASS" && strings.Contains(path, webchannelPathMarker)
		if fromChannel || strings.Contains(cookiePath, webchannelPathMarker) {
			store, persisted = j.channel, false
		}
		old, had := store[c.Name]
		if isDeletion(c) {
			delete(store, c.Name)
			changed = changed || (persisted && had)
			continue
		}
		store[c.Name] = c.Value
		changed = changed || (persisted && (!had || old != c.Value))
	}
	onChange := j.onChange
	j.mu.Unlock()
	if changed && onChange != nil {
		onChange()
	}
}

// header builds the unquoted, name-sorted Cookie header; webchannel requests prefer the channel cookies.
func (j *cookieJar) header(path string) string {
	j.mu.RLock()
	defer j.mu.RUnlock()
	merged := j.cookies
	if strings.Contains(path, webchannelPathMarker) && len(j.channel) > 0 {
		merged = make(map[string]string, len(j.cookies)+len(j.channel))
		for name, v := range j.cookies {
			merged[name] = v
		}
		for name, v := range j.channel {
			merged[name] = v
		}
	}
	if len(merged) == 0 {
		return ""
	}
	names := make([]string, 0, len(merged))
	for name := range merged {
		names = append(names, name)
	}
	sort.Strings(names)
	pairs := make([]string, len(names))
	for i, name := range names {
		pairs[i] = name + "=" + merged[name]
	}
	return strings.Join(pairs, "; ")
}

// snapshotAll returns a copy of the persisted jar, including rotation extras but not channel cookies.
func (j *cookieJar) snapshotAll() map[string]string {
	j.mu.RLock()
	defer j.mu.RUnlock()
	out := make(map[string]string, len(j.cookies))
	for name, v := range j.cookies {
		out[name] = v
	}
	return out
}

// setOnChange installs the change callback.
func (j *cookieJar) setOnChange(fn func()) {
	j.mu.Lock()
	j.onChange = fn
	j.mu.Unlock()
}
