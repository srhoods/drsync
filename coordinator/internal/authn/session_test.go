package authn

import (
	"path/filepath"
	"testing"
	"time"
)

func newTestSessionManager(t *testing.T, ttl time.Duration) *SessionManager {
	t.Helper()
	sm, err := NewSessionManager(filepath.Join(t.TempDir(), "session.key"), ttl)
	if err != nil {
		t.Fatalf("NewSessionManager: %v", err)
	}
	return sm
}

func TestSessionIssueVerify(t *testing.T) {
	sm := newTestSessionManager(t, time.Hour)
	tok := sm.Issue("alice")
	got, err := sm.Verify(tok)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if got != "alice" {
		t.Errorf("got %q, want alice", got)
	}
}

func TestSessionUsernameWithDot(t *testing.T) {
	sm := newTestSessionManager(t, time.Hour)
	tok := sm.Issue("first.last")
	got, err := sm.Verify(tok)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if got != "first.last" {
		t.Errorf("got %q, want first.last", got)
	}
}

func TestSessionExpired(t *testing.T) {
	sm := newTestSessionManager(t, -time.Second) // already expired
	tok := sm.Issue("alice")
	if _, err := sm.Verify(tok); err == nil {
		t.Fatal("expected expired token to fail verification")
	}
}

func TestSessionTamperedSignature(t *testing.T) {
	sm := newTestSessionManager(t, time.Hour)
	tok := sm.Issue("alice")
	tampered := tok[:len(tok)-1] + "x"
	if _, err := sm.Verify(tampered); err == nil {
		t.Fatal("expected tampered token to fail verification")
	}
}

func TestSessionCrossSecretRejected(t *testing.T) {
	sm1 := newTestSessionManager(t, time.Hour)
	sm2 := newTestSessionManager(t, time.Hour)
	tok := sm1.Issue("alice")
	if _, err := sm2.Verify(tok); err == nil {
		t.Fatal("expected token signed by a different secret to be rejected")
	}
}

func TestSessionMalformedToken(t *testing.T) {
	sm := newTestSessionManager(t, time.Hour)
	for _, tok := range []string{"", "a", "a.b", "a.b.c.d"} {
		if _, err := sm.Verify(tok); err == nil {
			t.Errorf("token %q: expected error", tok)
		}
	}
}

func TestSessionSecretPersistsAcrossRestarts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.key")
	sm1, err := NewSessionManager(path, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	tok := sm1.Issue("alice")

	sm2, err := NewSessionManager(path, time.Hour) // simulates a coordinator restart
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sm2.Verify(tok); err != nil {
		t.Fatalf("token from before restart should still verify: %v", err)
	}
}
