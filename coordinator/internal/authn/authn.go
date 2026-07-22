package authn

import "fmt"

// User is an authenticated identity: the login name plus the group names
// used to evaluate the allowlist.
type User struct {
	Username string
	Groups   []string
}

// Authenticator verifies a username/password against some identity source
// (local host accounts or Active Directory) and reports group membership.
type Authenticator interface {
	// Authenticate verifies the credential and returns the resulting User.
	// It returns an error for any failure (bad credential, directory
	// unreachable, unknown user) — callers must not distinguish these in
	// what they show the client, to avoid leaking account existence.
	Authenticate(username, password string) (*User, error)
}

// New builds the Authenticator for cfg.Mode.
func New(cfg *Config) (Authenticator, error) {
	switch cfg.Mode {
	case "local":
		return &LocalAuthenticator{}, nil
	case "ad":
		return &ADAuthenticator{cfg: cfg.LDAP}, nil
	default:
		return nil, fmt.Errorf("unknown auth mode %q", cfg.Mode)
	}
}

// Allowed reports whether u passes cfg's allowlist: an exact username match,
// or membership in any listed group.
func (c *Config) Allowed(u *User) bool {
	for _, name := range c.Allow.Users {
		if name == u.Username {
			return true
		}
	}
	if len(c.Allow.Groups) == 0 {
		return false
	}
	allowed := make(map[string]bool, len(c.Allow.Groups))
	for _, g := range c.Allow.Groups {
		allowed[g] = true
	}
	for _, g := range u.Groups {
		if allowed[g] {
			return true
		}
	}
	return false
}
