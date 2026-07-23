package authn

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// CookieName is the session cookie the WebUI login flow sets and the API
// middleware reads.
const CookieName = "drsync_session"

// errInvalidSession covers every session validation failure (bad signature,
// malformed token, expired) — callers should treat them identically and not
// leak which case occurred.
var errInvalidSession = errors.New("invalid or expired session")

// SessionManager issues and verifies signed, stateless session tokens: the
// token is "username.expiry.signature" (base64url fields), HMAC-SHA256 signed
// with a server-only secret. No server-side session store is needed — the
// coordinator stays simple to run (single SQLite file, no separate session
// table) at the cost of not being able to revoke a single session early
// (logout just deletes the client's cookie; a stolen token remains valid
// until it expires, bounded by SessionTTL).
type SessionManager struct {
	secret []byte
	ttl    time.Duration
}

// NewSessionManager loads (or creates) a persistent HMAC secret at
// secretPath — conventionally alongside the coordinator's state DB, so
// sessions survive a restart but a fresh data-dir gets a fresh secret and
// invalidates any old tokens.
func NewSessionManager(secretPath string, ttl time.Duration) (*SessionManager, error) {
	secret, err := loadOrCreateSecret(secretPath)
	if err != nil {
		return nil, err
	}
	return &SessionManager{secret: secret, ttl: ttl}, nil
}

func loadOrCreateSecret(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err == nil && len(data) == 32 {
		return data, nil
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return nil, fmt.Errorf("generate session secret: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	if err := os.WriteFile(path, secret, 0o600); err != nil {
		return nil, fmt.Errorf("write session secret: %w", err)
	}
	return secret, nil
}

// Issue mints a signed token for username, valid for the configured TTL.
// username is base64url-encoded in the token so it can safely contain "."
// or any other character.
func (m *SessionManager) Issue(username string) string {
	exp := time.Now().Add(m.ttl).Unix()
	encUser := base64.RawURLEncoding.EncodeToString([]byte(username))
	payload := encUser + "." + formatInt64(exp)
	sig := m.sign(payload)
	return payload + "." + sig
}

// Verify checks token's signature and expiry and returns the username it
// was issued for.
func (m *SessionManager) Verify(token string) (string, error) {
	parts := strings.SplitN(token, ".", 3)
	if len(parts) != 3 {
		return "", errInvalidSession
	}
	encUser, expStr, sig := parts[0], parts[1], parts[2]
	payload := encUser + "." + expStr
	want := m.sign(payload)
	if subtle.ConstantTimeCompare([]byte(sig), []byte(want)) != 1 {
		return "", errInvalidSession
	}
	exp, err := parseInt64(expStr)
	if err != nil {
		return "", errInvalidSession
	}
	if time.Now().Unix() > exp {
		return "", errInvalidSession
	}
	userBytes, err := base64.RawURLEncoding.DecodeString(encUser)
	if err != nil {
		return "", errInvalidSession
	}
	return string(userBytes), nil
}

func (m *SessionManager) sign(payload string) string {
	h := hmac.New(sha256.New, m.secret)
	h.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString(h.Sum(nil))
}

func formatInt64(v int64) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v))
	return base64.RawURLEncoding.EncodeToString(b[:])
}

func parseInt64(s string) (int64, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || len(b) != 8 {
		return 0, errInvalidSession
	}
	return int64(binary.BigEndian.Uint64(b)), nil
}
