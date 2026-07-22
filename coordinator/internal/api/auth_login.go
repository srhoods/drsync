package api

import (
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"drsync/coordinator/internal/authn"
)

// loginLimiter throttles repeated failed logins per source IP to blunt
// online password guessing. It is intentionally simple (in-memory, reset on
// restart) — the coordinator is a single operational instance, not a
// public-facing service, so this is defense in depth rather than the primary
// control (the primary control is the allowlist plus AD/shadow itself rate
// limiting or locking accounts).
type loginLimiter struct {
	mu       sync.Mutex
	failures map[string]*failState
}

type failState struct {
	count      int
	lockedTill time.Time
}

const (
	maxLoginFailures = 5
	loginLockout     = 30 * time.Second
)

func newLoginLimiter() *loginLimiter {
	return &loginLimiter{failures: make(map[string]*failState)}
}

func (l *loginLimiter) allowed(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	fs := l.failures[key]
	if fs == nil {
		return true
	}
	return time.Now().After(fs.lockedTill)
}

func (l *loginLimiter) recordFailure(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fs := l.failures[key]
	if fs == nil {
		fs = &failState{}
		l.failures[key] = fs
	}
	fs.count++
	if fs.count >= maxLoginFailures {
		fs.lockedTill = time.Now().Add(loginLockout)
		fs.count = 0
	}
}

func (l *loginLimiter) recordSuccess(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.failures, key)
}

// loginRequest is the POST /api/v1/login body.
type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// handleLogin authenticates against the configured authn.Authenticator and,
// on success, sets a signed session cookie. Returns 404 when interactive
// auth isn't configured (auth.yaml absent) so the WebUI can detect that and
// fall back to token-only mode; 401 for any authentication failure
// (unknown user, bad password, or allowlist rejection — deliberately not
// distinguished, to avoid confirming account existence).
func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	if s.authenticator == nil {
		httpErr(w, http.StatusNotFound, "interactive login is not configured on this coordinator")
		return
	}
	var req loginRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		httpErr(w, http.StatusBadRequest, "invalid request body")
		return
	}

	limitKey := clientIP(r)
	if !s.loginLimiter.allowed(limitKey) {
		httpErr(w, http.StatusTooManyRequests, "too many failed login attempts, try again shortly")
		return
	}

	user, err := s.authenticator.Authenticate(req.Username, req.Password)
	if err != nil || !s.authConfig.Allowed(user) {
		s.loginLimiter.recordFailure(limitKey)
		if err != nil {
			slog.Warn("login failed", "username", req.Username, "remote", limitKey)
		} else {
			slog.Warn("login rejected by allowlist", "username", req.Username, "remote", limitKey)
		}
		httpErr(w, http.StatusUnauthorized, "invalid username or password")
		return
	}
	s.loginLimiter.recordSuccess(limitKey)

	tok := s.sessions.Issue(user.Username)
	http.SetCookie(w, &http.Cookie{
		Name:     authn.CookieName,
		Value:    tok,
		Path:     "/",
		HttpOnly: true,
		Secure:   s.httpsEnabled,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   s.authConfig.SessionTTLMinutes * 60,
	})
	slog.Info("login succeeded", "username", user.Username, "remote", limitKey)
	writeJSON(w, http.StatusOK, map[string]string{"username": user.Username})
}

// handleLogout clears the session cookie. Sessions are stateless (signed,
// not server-tracked — see authn.SessionManager), so logout cannot revoke
// the token itself, only tell the browser to stop sending it.
func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     authn.CookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		Secure:   s.httpsEnabled,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
	})
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleWhoAmI reports the caller's authenticated identity, letting the
// WebUI show a username and decide whether to render the login page.
func (s *Server) handleWhoAmI(w http.ResponseWriter, r *http.Request) {
	username := ""
	if c, err := r.Cookie(authn.CookieName); err == nil && s.sessions != nil {
		if u, err := s.sessions.Verify(c.Value); err == nil {
			username = u
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"username":         username,
		"login_configured": s.authenticator != nil,
	})
}

// clientIP returns the caller's IP with any port stripped, so repeated
// connections from the same client (each a fresh ephemeral port) land in the
// same rate-limit bucket.
func clientIP(r *http.Request) string {
	addr := r.RemoteAddr
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		// XFF may be a comma-separated chain; the client is the first hop.
		addr = strings.TrimSpace(strings.SplitN(fwd, ",", 2)[0])
	}
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return addr
}
