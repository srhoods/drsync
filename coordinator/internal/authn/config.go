// Package authn provides WebUI/API login: local host-account authentication
// (via /etc/shadow) or Active Directory (via LDAP bind), gated by a per-user
// or per-group allowlist. Settings are loaded from a YAML config
// (conventionally /etc/drsync/auth.yaml). When that file is absent,
// authentication is disabled and the API falls back to bearer-token-only (or
// no auth at all in dev), matching prior behaviour.
package authn

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"

	"gopkg.in/yaml.v3"
)

// Config is the auth configuration, loaded from /etc/drsync/auth.yaml.
type Config struct {
	// Mode selects the authenticator: "local" (host /etc/shadow accounts) or
	// "ad" (Active Directory via LDAP simple bind). Required.
	Mode string `yaml:"mode"`

	// Allow gates access after a successful authentication: the authenticated
	// identity must match a listed username or belong to a listed group.
	// Empty Users and Groups means nobody is allowed through (fail closed) —
	// an operator must explicitly name who gets in.
	Allow AllowList `yaml:"allow"`

	// LDAP holds Active Directory / LDAP bind settings. Required when Mode is
	// "ad"; ignored otherwise.
	LDAP LDAPConfig `yaml:"ldap,omitempty"`

	// SessionTTLMinutes bounds how long an issued session cookie is valid
	// before the user must log in again. Defaults to 480 (8 hours).
	SessionTTLMinutes int `yaml:"session_ttl_minutes,omitempty"`
}

// AllowList gates access by username or group membership.
type AllowList struct {
	Users  []string `yaml:"users,omitempty"`
	Groups []string `yaml:"groups,omitempty"`
}

// LDAPConfig holds the settings needed to bind to Active Directory and
// resolve a user's group membership.
type LDAPConfig struct {
	// URL is the LDAP server URL, e.g. "ldaps://dc1.example.com:636" or
	// "ldap://dc1.example.com:389". Required.
	URL string `yaml:"url"`
	// StartTLS upgrades a plain "ldap://" connection with STARTTLS before any
	// bind. Ignored for "ldaps://" URLs, which are already encrypted.
	StartTLS bool `yaml:"starttls,omitempty"`
	// InsecureSkipVerify disables TLS certificate verification. Dev/test only.
	InsecureSkipVerify bool `yaml:"insecure_skip_verify,omitempty"`

	// BindDN and BindPassword are a service account used to search for the
	// user's DN and group memberships before the user's own credential bind.
	// Required.
	BindDN       string `yaml:"bind_dn"`
	BindPassword string `yaml:"bind_password"`

	// BaseDN is the search base for both user and group lookups, e.g.
	// "DC=example,DC=com". Required.
	BaseDN string `yaml:"base_dn"`

	// UserFilter is the LDAP filter used to find the user entry by login
	// name; "%s" is replaced with the submitted username. Defaults to
	// "(sAMAccountName=%s)" (Active Directory's logon-name attribute).
	UserFilter string `yaml:"user_filter,omitempty"`

	// GroupAttribute is the user-entry attribute read for group membership.
	// Defaults to "memberOf", which AD populates with each group's full DN.
	GroupAttribute string `yaml:"group_attribute,omitempty"`
}

// LoadConfig reads, defaults and validates the auth config at path. When
// missingOK is true an absent file yields (nil, nil) so a deployment that
// does not want interactive login need not create one; an explicitly
// configured path that is missing is an error.
func LoadConfig(path string, missingOK bool) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if missingOK && errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var c Config
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse auth config %s: %w", path, err)
	}
	c.applyDefaults()
	if err := c.validate(); err != nil {
		return nil, fmt.Errorf("auth config %s: %w", path, err)
	}
	return &c, nil
}

func (c *Config) applyDefaults() {
	if c.SessionTTLMinutes == 0 {
		c.SessionTTLMinutes = 480
	}
	if c.Mode == "ad" {
		if c.LDAP.UserFilter == "" {
			c.LDAP.UserFilter = "(sAMAccountName=%s)"
		}
		if c.LDAP.GroupAttribute == "" {
			c.LDAP.GroupAttribute = "memberOf"
		}
	}
}

func (c *Config) validate() error {
	switch c.Mode {
	case "local":
	case "ad":
		if c.LDAP.URL == "" {
			return errors.New("ldap.url is required when mode is \"ad\"")
		}
		if c.LDAP.BindDN == "" {
			return errors.New("ldap.bind_dn is required when mode is \"ad\"")
		}
		if c.LDAP.BaseDN == "" {
			return errors.New("ldap.base_dn is required when mode is \"ad\"")
		}
	default:
		return fmt.Errorf("mode must be \"local\" or \"ad\", got %q", c.Mode)
	}
	if len(c.Allow.Users) == 0 && len(c.Allow.Groups) == 0 {
		return errors.New("allow.users or allow.groups must name at least one entry (fail-closed default)")
	}
	return nil
}
