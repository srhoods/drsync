package authn

import "testing"

func TestAllowedByUsername(t *testing.T) {
	cfg := &Config{Allow: AllowList{Users: []string{"alice", "bob"}}}
	if !cfg.Allowed(&User{Username: "alice"}) {
		t.Error("alice should be allowed by username")
	}
	if cfg.Allowed(&User{Username: "carol"}) {
		t.Error("carol should not be allowed")
	}
}

func TestAllowedByGroup(t *testing.T) {
	cfg := &Config{Allow: AllowList{Groups: []string{"drsync-admins"}}}
	if !cfg.Allowed(&User{Username: "carol", Groups: []string{"wheel", "drsync-admins"}}) {
		t.Error("carol should be allowed via group membership")
	}
	if cfg.Allowed(&User{Username: "dave", Groups: []string{"wheel"}}) {
		t.Error("dave should not be allowed, wrong group")
	}
}

func TestAllowedNoGroupsConfigured(t *testing.T) {
	cfg := &Config{Allow: AllowList{Users: []string{"alice"}}}
	if cfg.Allowed(&User{Username: "carol", Groups: []string{"anything"}}) {
		t.Error("carol should not be allowed when no groups are configured")
	}
}

func TestConfigValidateFailClosed(t *testing.T) {
	c := &Config{Mode: "local"} // no users, no groups
	if err := c.validate(); err == nil {
		t.Error("expected validation error when allow list is empty")
	}
}

func TestConfigValidateUnknownMode(t *testing.T) {
	c := &Config{Mode: "kerberos", Allow: AllowList{Users: []string{"alice"}}}
	if err := c.validate(); err == nil {
		t.Error("expected validation error for unknown mode")
	}
}

func TestConfigValidateADRequiresLDAP(t *testing.T) {
	c := &Config{Mode: "ad", Allow: AllowList{Users: []string{"alice"}}}
	if err := c.validate(); err == nil {
		t.Error("expected validation error when ad mode is missing ldap settings")
	}
}

func TestConfigApplyDefaults(t *testing.T) {
	c := &Config{Mode: "ad"}
	c.applyDefaults()
	if c.SessionTTLMinutes != 480 {
		t.Errorf("SessionTTLMinutes = %d, want 480", c.SessionTTLMinutes)
	}
	if c.LDAP.UserFilter != "(sAMAccountName=%s)" {
		t.Errorf("UserFilter = %q", c.LDAP.UserFilter)
	}
	if c.LDAP.GroupAttribute != "memberOf" {
		t.Errorf("GroupAttribute = %q", c.LDAP.GroupAttribute)
	}
}

func TestGroupCN(t *testing.T) {
	cases := map[string]string{
		"CN=drsync-admins,OU=Groups,DC=example,DC=com": "drsync-admins",
		"cn=lowercase,dc=example,dc=com":               "lowercase",
		"not a dn":                                     "not a dn",
	}
	for dn, want := range cases {
		if got := groupCN(dn); got != want {
			t.Errorf("groupCN(%q) = %q, want %q", dn, got, want)
		}
	}
}

func TestLoadConfigMissingOK(t *testing.T) {
	c, err := LoadConfig("/nonexistent/auth.yaml", true)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if c != nil {
		t.Errorf("expected nil config for missing file, got %+v", c)
	}
}

func TestLoadConfigMissingRequired(t *testing.T) {
	if _, err := LoadConfig("/nonexistent/auth.yaml", false); err == nil {
		t.Error("expected error for explicitly-required missing config")
	}
}
