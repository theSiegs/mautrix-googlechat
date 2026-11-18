package connector

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
	"text/template"

	"github.com/rs/zerolog"
	"go.mau.fi/util/configupgrade"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
)

//go:embed example-config.yaml
var ExampleConfig string

type Config struct {
	DisplaynameTemplate string `yaml:"displayname_template"`
	InitialChatSync     int    `yaml:"initial_chat_sync"`

	// ReadOnly makes the bridge receive only: nothing that changes the Google account is ever sent.
	ReadOnly bool `yaml:"read_only"`

	// Presence is what the bridge tells Google about itself: "active" (default) or "inactive".
	Presence string `yaml:"presence"`

	// UserAgent overrides the User-Agent captured at login; empty keeps the captured one.
	UserAgent string `yaml:"user_agent"`

	// Protocol overrides web-client constants Google may rotate; empty or zero fields use the built-in values.
	Protocol ProtocolConfig `yaml:"protocol"`

	// DisableOutboundMedia rejects Matrix media sends; Google's /uploads has been returning HTTP 500.
	DisableOutboundMedia bool `yaml:"disable_outbound_media"`

	// DisableInlineURLMedia stops fetching url_metadata image_url media, the bridge's only third-party egress.
	DisableInlineURLMedia bool `yaml:"disable_inline_url_media"`

	// SkipLeaveConfirmation skips the `leave confirm` step; negative so the zero value keeps the confirmation.
	SkipLeaveConfirmation bool `yaml:"skip_leave_confirmation"`

	displaynameTemplate *template.Template `yaml:"-"`
}

// ProtocolConfig mirrors gchatmeow.Protocol.
type ProtocolConfig struct {
	APIKey        string `yaml:"api_key"`
	ClientVersion int64  `yaml:"client_version"`
	MoleWorldHS   string `yaml:"mole_world_hs"`
}

type DisplaynameParams struct {
	Name      string
	FirstName string
	Email     string
}

func (c *Config) PostProcess() error {
	if _, err := gchatmeow.ParsePresence(c.Presence); err != nil {
		return fmt.Errorf("network.presence: %w", err)
	}
	var err error
	c.displaynameTemplate, err = template.New("displayname").Parse(c.DisplaynameTemplate)
	return err
}

// FormatDisplayname renders displayname_template, logging (not failing on) template execution errors.
func (c *Config) FormatDisplayname(ctx context.Context, params DisplaynameParams) string {
	var buf strings.Builder
	if err := c.displaynameTemplate.Execute(&buf, params); err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).
			Msg("googlechat: displayname_template execution failed, name may be incomplete")
	}
	return buf.String()
}

func upgradeConfig(helper configupgrade.Helper) {
	helper.Copy(configupgrade.Str, "displayname_template")
	helper.Copy(configupgrade.Int, "initial_chat_sync")
	helper.Copy(configupgrade.Bool, "read_only")
	helper.Copy(configupgrade.Str, "presence")
	helper.Copy(configupgrade.Str, "user_agent")
	helper.Copy(configupgrade.Str, "protocol", "api_key")
	helper.Copy(configupgrade.Int, "protocol", "client_version")
	helper.Copy(configupgrade.Str, "protocol", "mole_world_hs")
	helper.Copy(configupgrade.Bool, "disable_outbound_media")
	helper.Copy(configupgrade.Bool, "disable_inline_url_media")
	helper.Copy(configupgrade.Bool, "skip_leave_confirmation")
}
