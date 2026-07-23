package authn

import (
	"crypto/tls"
	"fmt"
	"strings"

	"github.com/go-ldap/ldap/v3"
)

// ADAuthenticator authenticates against Active Directory: bind as the
// configured service account, search for the user's DN, then re-bind as the
// user with the submitted password to verify it (a "search + bind" flow —
// the standard way to authenticate against AD without knowing DN layouts up
// front). Group membership comes from the user entry's memberOf attribute.
type ADAuthenticator struct {
	cfg LDAPConfig
}

func (a *ADAuthenticator) Authenticate(username, password string) (*User, error) {
	if username == "" || password == "" {
		return nil, errInvalidCredential
	}
	conn, err := a.dial()
	if err != nil {
		return nil, fmt.Errorf("connect to AD: %w", err)
	}
	defer conn.Close()

	if err := conn.Bind(a.cfg.BindDN, a.cfg.BindPassword); err != nil {
		return nil, fmt.Errorf("service account bind: %w", err)
	}

	filter := fmt.Sprintf(a.cfg.UserFilter, ldap.EscapeFilter(username))
	req := ldap.NewSearchRequest(a.cfg.BaseDN, ldap.ScopeWholeSubtree,
		ldap.NeverDerefAliases, 2, 0, false, filter,
		[]string{"dn", a.cfg.GroupAttribute}, nil)
	res, err := conn.Search(req)
	if err != nil {
		return nil, fmt.Errorf("user search: %w", err)
	}
	if len(res.Entries) != 1 {
		return nil, errInvalidCredential // not found, or ambiguous
	}
	entry := res.Entries[0]

	// Re-bind as the user to verify the password. A fresh connection is used
	// so a failed user bind can't leave the service-account connection in an
	// unauthenticated state for any code path that might reuse it.
	userConn, err := a.dial()
	if err != nil {
		return nil, fmt.Errorf("connect to AD: %w", err)
	}
	defer userConn.Close()
	if err := userConn.Bind(entry.DN, password); err != nil {
		return nil, errInvalidCredential
	}

	groups := make([]string, 0, len(entry.Attributes))
	for _, dn := range entry.GetAttributeValues(a.cfg.GroupAttribute) {
		groups = append(groups, groupCN(dn))
	}
	return &User{Username: username, Groups: groups}, nil
}

func (a *ADAuthenticator) dial() (*ldap.Conn, error) {
	var conn *ldap.Conn
	var err error
	if strings.HasPrefix(a.cfg.URL, "ldaps://") {
		conn, err = ldap.DialURL(a.cfg.URL, ldap.DialWithTLSConfig(&tls.Config{
			InsecureSkipVerify: a.cfg.InsecureSkipVerify,
		}))
	} else {
		conn, err = ldap.DialURL(a.cfg.URL)
	}
	if err != nil {
		return nil, err
	}
	if a.cfg.StartTLS && !strings.HasPrefix(a.cfg.URL, "ldaps://") {
		if err := conn.StartTLS(&tls.Config{InsecureSkipVerify: a.cfg.InsecureSkipVerify}); err != nil {
			conn.Close()
			return nil, fmt.Errorf("starttls: %w", err)
		}
	}
	return conn, nil
}

// groupCN extracts the CN (common name) from a group's distinguished name,
// e.g. "CN=drsync-admins,OU=Groups,DC=example,DC=com" -> "drsync-admins". AD
// populates memberOf with full DNs; the allowlist in auth.yaml is written in
// terms of the short group name.
func groupCN(dn string) string {
	parsed, err := ldap.ParseDN(dn)
	if err != nil || len(parsed.RDNs) == 0 {
		return dn
	}
	for _, attr := range parsed.RDNs[0].Attributes {
		if strings.EqualFold(attr.Type, "CN") {
			return attr.Value
		}
	}
	return dn
}
