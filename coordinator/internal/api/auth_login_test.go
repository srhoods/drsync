package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"drsync/coordinator/internal/authn"
	"drsync/coordinator/internal/metrics"
	"drsync/coordinator/internal/store"
)

// stubAuthenticator authenticates exactly one hardcoded credential pair, so
// tests don't touch the real /etc/shadow or a live AD server.
type stubAuthenticator struct {
	username, password string
	groups             []string
}

var errStubInvalidCredential = errors.New("invalid username or password")

func (s *stubAuthenticator) Authenticate(username, password string) (*authn.User, error) {
	if username != s.username || password != s.password {
		return nil, errStubInvalidCredential
	}
	return &authn.User{Username: username, Groups: s.groups}, nil
}

// setupAuthServer builds a bare Server (no store-backed job data needed for
// these tests) wired with interactive login against a stub authenticator.
func setupAuthServer(t *testing.T, allow authn.AllowList) (*Server, *stubAuthenticator) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	srv := New(st, nil, metrics.New(), nil, dir, "")
	sm, err := authn.NewSessionManager(filepath.Join(dir, "session.key"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	stub := &stubAuthenticator{username: "alice", password: "correct-horse", groups: []string{"eng"}}
	cfg := &authn.Config{Mode: "local", Allow: allow, SessionTTLMinutes: 60}
	srv.SetAuth(cfg, stub, sm)
	return srv, stub
}

func doLogin(t *testing.T, srv *Server, username, password string) *httptest.ResponseRecorder {
	t.Helper()
	body, _ := json.Marshal(loginRequest{Username: username, Password: password})
	r := httptest.NewRequest(http.MethodPost, "/api/v1/login", bytes.NewReader(body))
	r.RemoteAddr = "203.0.113.1:5555"
	w := httptest.NewRecorder()
	srv.handleLogin(w, r)
	return w
}

func TestLoginSuccessSetsCookie(t *testing.T) {
	srv, _ := setupAuthServer(t, authn.AllowList{Users: []string{"alice"}})
	w := doLogin(t, srv, "alice", "correct-horse")
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	cookies := w.Result().Cookies()
	if len(cookies) != 1 || cookies[0].Name != authn.CookieName {
		t.Fatalf("expected a %s cookie, got %+v", authn.CookieName, cookies)
	}
	if !cookies[0].HttpOnly {
		t.Error("session cookie must be HttpOnly")
	}
	if cookies[0].SameSite != http.SameSiteLaxMode {
		t.Error("session cookie must be SameSite=Lax")
	}
}

func TestLoginWrongPassword(t *testing.T) {
	srv, _ := setupAuthServer(t, authn.AllowList{Users: []string{"alice"}})
	w := doLogin(t, srv, "alice", "wrong")
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status %d, want 401", w.Code)
	}
}

func TestLoginRejectedByAllowlist(t *testing.T) {
	srv, _ := setupAuthServer(t, authn.AllowList{Users: []string{"someone-else"}})
	w := doLogin(t, srv, "alice", "correct-horse")
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status %d, want 401", w.Code)
	}
}

func TestLoginAllowedByGroup(t *testing.T) {
	srv, _ := setupAuthServer(t, authn.AllowList{Groups: []string{"eng"}})
	w := doLogin(t, srv, "alice", "correct-horse")
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
}

func TestLoginRateLimited(t *testing.T) {
	srv, _ := setupAuthServer(t, authn.AllowList{Users: []string{"alice"}})
	for i := 0; i < maxLoginFailures; i++ {
		w := doLogin(t, srv, "alice", "wrong")
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d: status %d, want 401", i, w.Code)
		}
	}
	// One more (even with correct credentials) should now be locked out.
	w := doLogin(t, srv, "alice", "correct-horse")
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("status %d, want 429", w.Code)
	}
}

func TestLoginNotConfigured(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	srv := New(st, nil, metrics.New(), nil, dir, "")
	w := doLogin(t, srv, "alice", "correct-horse")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status %d, want 404", w.Code)
	}
}

func TestLogoutClearsCookie(t *testing.T) {
	srv, _ := setupAuthServer(t, authn.AllowList{Users: []string{"alice"}})
	r := httptest.NewRequest(http.MethodPost, "/api/v1/logout", nil)
	w := httptest.NewRecorder()
	srv.handleLogout(w, r)
	cookies := w.Result().Cookies()
	if len(cookies) != 1 || cookies[0].MaxAge >= 0 {
		t.Fatalf("expected an expiring cookie, got %+v", cookies)
	}
}

func TestWhoAmIWithValidSession(t *testing.T) {
	srv, _ := setupAuthServer(t, authn.AllowList{Users: []string{"alice"}})
	loginResp := doLogin(t, srv, "alice", "correct-horse")
	cookie := loginResp.Result().Cookies()[0]

	r := httptest.NewRequest(http.MethodGet, "/api/v1/whoami", nil)
	r.AddCookie(cookie)
	w := httptest.NewRecorder()
	srv.handleWhoAmI(w, r)

	var got map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["username"] != "alice" {
		t.Errorf("username = %v, want alice", got["username"])
	}
	if got["login_configured"] != true {
		t.Errorf("login_configured = %v, want true", got["login_configured"])
	}
}

func TestWhoAmIWithoutSession(t *testing.T) {
	srv, _ := setupAuthServer(t, authn.AllowList{Users: []string{"alice"}})
	r := httptest.NewRequest(http.MethodGet, "/api/v1/whoami", nil)
	w := httptest.NewRecorder()
	srv.handleWhoAmI(w, r)

	var got map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["username"] != "" {
		t.Errorf("username = %v, want empty", got["username"])
	}
}

// TestWhoAmIReportsTokenRequired is the WebUI-stuck-on-"connecting…"
// regression: a coordinator can have -api-token-file set (its default even
// names a conventional path an operator may not realise is populated, e.g.
// left over from an earlier deployment) with no auth.yaml at all — SetAuth
// is never called, so s.authenticator stays nil and login_configured is
// correctly false, but every route s.auth() protects still 401s a browser
// that supplies no bearer token. Before token_required existed, the WebUI
// could not tell this state apart from genuine "no auth at all" and looped
// silently (see webui/test/console.test.mjs's matching JS-side regression
// test). token_required must be true here specifically because a token IS
// configured and no authenticator is — the two other combinations
// (TestWhoAmIWithSession/WithoutSession above) must both report false.
func TestWhoAmIReportsTokenRequired(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	srv := New(st, nil, metrics.New(), nil, dir, "s3cr3t-token") // SetAuth deliberately never called

	r := httptest.NewRequest(http.MethodGet, "/api/v1/whoami", nil)
	w := httptest.NewRecorder()
	srv.handleWhoAmI(w, r)

	var got map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["login_configured"] != false {
		t.Errorf("login_configured = %v, want false (no auth.yaml, SetAuth never called)", got["login_configured"])
	}
	if got["token_required"] != true {
		t.Errorf("token_required = %v, want true (a token is set with no authenticator to obtain a session through)", got["token_required"])
	}

	// And a protected route genuinely does 401 an unauthenticated request in
	// this state — token_required=true is not just a label, it's true.
	r2 := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	w2 := httptest.NewRecorder()
	srv.auth(srv.listJobs)(w2, r2)
	if w2.Code != http.StatusUnauthorized {
		t.Fatalf("protected route status = %d, want 401 (token configured, none supplied)", w2.Code)
	}
}

// TestAuthMiddlewareAcceptsSessionCookie exercises the middleware end to
// end: a session cookie from login must be sufficient to pass s.auth() on a
// protected route, without any bearer token.
func TestAuthMiddlewareAcceptsSessionCookie(t *testing.T) {
	srv, _ := setupAuthServer(t, authn.AllowList{Users: []string{"alice"}})
	loginResp := doLogin(t, srv, "alice", "correct-horse")
	cookie := loginResp.Result().Cookies()[0]

	protected := srv.auth(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	r := httptest.NewRequest(http.MethodGet, "/api/v1/info", nil)
	r.AddCookie(cookie)
	w := httptest.NewRecorder()
	protected(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d, want 200", w.Code)
	}
}

func TestAuthMiddlewareRejectsNoCredentials(t *testing.T) {
	srv, _ := setupAuthServer(t, authn.AllowList{Users: []string{"alice"}})
	protected := srv.auth(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	r := httptest.NewRequest(http.MethodGet, "/api/v1/info", nil)
	w := httptest.NewRecorder()
	protected(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status %d, want 401", w.Code)
	}
}

func TestAuthMiddlewareRejectsForgedCookie(t *testing.T) {
	srv, _ := setupAuthServer(t, authn.AllowList{Users: []string{"alice"}})
	protected := srv.auth(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	r := httptest.NewRequest(http.MethodGet, "/api/v1/info", nil)
	r.AddCookie(&http.Cookie{Name: authn.CookieName, Value: "forged.garbage.token"})
	w := httptest.NewRecorder()
	protected(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status %d, want 401", w.Code)
	}
}
