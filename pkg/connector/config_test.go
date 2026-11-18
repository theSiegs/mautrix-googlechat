package connector

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
)

// yamlKeys lists the yaml tag of every exported field of t, recursing into nested structs.
func yamlKeys(t reflect.Type) []string {
	var keys []string
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := f.Tag.Get("yaml")
		if tag == "" || tag == "-" {
			continue
		}
		keys = append(keys, tag)
		if f.Type.Kind() == reflect.Struct {
			keys = append(keys, yamlKeys(f.Type)...)
		}
	}
	return keys
}

func TestEveryConfigKeyIsInTheExampleConfig(t *testing.T) {
	for _, key := range yamlKeys(reflect.TypeOf(Config{})) {
		if !strings.Contains(ExampleConfig, "\n"+key+":") && !strings.Contains(ExampleConfig, "    "+key+":") && !strings.HasPrefix(ExampleConfig, key+":") {
			t.Errorf("config key %q is missing from example-config.yaml", key)
		}
	}
}

func TestConfigRejectsAnUnknownPresence(t *testing.T) {
	cfg := newTestConfig(t)
	for _, ok := range []string{"", "active", "inactive"} {
		cfg.Presence = ok
		if err := cfg.PostProcess(); err != nil {
			t.Errorf("presence %q rejected: %v", ok, err)
		}
	}
	cfg.Presence = "away"
	if err := cfg.PostProcess(); err == nil || !strings.Contains(err.Error(), "network.presence") {
		t.Errorf("presence \"away\" returned %v, want an error naming network.presence", err)
	}
}

func TestClientOptsFollowTheConfig(t *testing.T) {
	var none *GChatConnector
	plain := none.clientOpts(fakeCookies(), "captured-ua")
	if plain.UserAgent != "captured-ua" || plain.ReadOnly || plain.Presence != "" || plain.Protocol != (gchatmeow.Protocol{}) {
		t.Errorf("clientOpts without a connector = %+v, want only the cookies and captured User-Agent", plain)
	}

	cfg := newTestConfig(t)
	defaults := (&GChatConnector{Config: *cfg}).clientOpts(fakeCookies(), "captured-ua")
	if defaults.UserAgent != "captured-ua" || defaults.Presence != gchatmeow.PresenceActive || defaults.ReadOnly || defaults.Protocol != (gchatmeow.Protocol{}) {
		t.Errorf("clientOpts with a default config = %+v, want the captured User-Agent, active presence and built-in protocol", defaults)
	}

	cfg.ReadOnly = true
	cfg.Presence = "inactive"
	cfg.UserAgent = "override-ua"
	cfg.Protocol = ProtocolConfig{APIKey: "k", ClientVersion: 9, MoleWorldHS: "hs"}
	opts := (&GChatConnector{Config: *cfg}).clientOpts(fakeCookies(), "captured-ua")
	want := gchatmeow.Protocol{APIKey: "k", ClientVersion: 9, MoleWorldHS: "hs"}
	if !opts.ReadOnly || opts.Presence != gchatmeow.PresenceInactive || opts.UserAgent != "override-ua" || opts.Protocol != want {
		t.Errorf("clientOpts = %+v, want every network option applied", opts)
	}
}

// TestUserAgentOverrideDoesNotReplaceTheCapturedOne: turning the override off must bring the captured UA back.
func TestUserAgentOverrideDoesNotReplaceTheCapturedOne(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.UserAgent = "override-ua"
	meta := &UserLoginMetadata{Cookies: fakeCookies(), UserAgent: "captured-ua"}
	gc := &GChatClient{
		Main:      &GChatConnector{Config: *cfg},
		UserLogin: newTestUserLogin(meta),
		saveFn:    func(context.Context) error { return nil },
	}
	conn, err := gchatmeow.NewClient(gc.Main.clientOpts(meta.Cookies, meta.UserAgent))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	gc.conn = conn
	if conn.UserAgent() != "override-ua" {
		t.Fatalf("client User-Agent = %q, want the override", conn.UserAgent())
	}
	gc.persistCookies(context.Background())
	if meta.UserAgent != "captured-ua" {
		t.Errorf("stored User-Agent = %q, want the captured one kept", meta.UserAgent)
	}
}
