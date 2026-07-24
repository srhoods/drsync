// Package api serves the REST surface the CLI and WebUI consume
// (docs/DESIGN-coordinator.md §6).
package api

import (
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"drsync/coordinator/internal/agentsrv"
	"drsync/coordinator/internal/authn"
	"drsync/coordinator/internal/events"
	"drsync/coordinator/internal/metrics"
	"drsync/coordinator/internal/model"
	"drsync/coordinator/internal/passctrl"
	"drsync/coordinator/internal/store"
	drsyncpb "drsync/proto/gen/drsyncpb"
	"drsync/webui"
)

type Server struct {
	st          *store.Store
	pc          *passctrl.Controller
	met         *metrics.Metrics
	bus         *events.Bus
	journalRoot string
	token       string // empty = no auth (dev only)

	// Interactive login (WebUI). authenticator and authConfig are nil when
	// /etc/drsync/auth.yaml is absent — the API then falls back to
	// token-only auth, matching prior behaviour. sessions is always set
	// (session secret is generated even if login is never used) so
	// handleWhoAmI can cheaply check a cookie either way.
	authenticator authn.Authenticator
	authConfig    *authn.Config
	sessions      *authn.SessionManager
	loginLimiter  *loginLimiter
	// httpsEnabled marks the session cookie Secure when the HTTP listener is
	// serving TLS, so it never gets sent in the clear over a fallback http://
	// deployment that later regains a TLS listener on a different host.
	httpsEnabled bool
	// ConnectedAgents is injected by agentsrv for the fleet view.
	ConnectedAgents func() []string
	// AgentInflight returns an agent's last-reported in-flight work, when it was
	// reported, whether the agent is new enough to report it at all, and whether
	// it is connected. Injected by agentsrv.
	AgentInflight func(id string) (items []*drsyncpb.InflightItem, at time.Time, reports, connected bool)
	// SetAgentDrain tells a live agent to start/stop draining (hand back queued
	// shards, take no new work). Returns false if the agent is not connected.
	// Injected by agentsrv.
	SetAgentDrain func(id string, drain bool) bool
	// NotifyJobDone tells connected agents a job reached a terminal state so they
	// release its cached options and root fds. Injected by agentsrv.
	NotifyJobDone func(jobID int64)
	// DropJournal removes a purged job's on-disk journal segments; injected in
	// main so the API can reclaim disk on `job purge`.
	DropJournal func(jobID int64) error
	// Info is static coordinator metadata surfaced by GET /api/v1/info (the
	// console header). Injected in main.
	Info CoordinatorInfo
}

// CoordinatorInfo is the static coordinator identity/config the console header
// shows. FleetEpoch is a hex string because a uint64 would lose precision as a
// JSON number in the browser.
type CoordinatorInfo struct {
	FleetEpoch string `json:"fleet_epoch"`
	LeaseTTLS  int    `json:"lease_ttl_s"`
	MTLS       bool   `json:"mtls"`
	Version    string `json:"version"`
}

func New(st *store.Store, pc *passctrl.Controller, met *metrics.Metrics,
	bus *events.Bus, journalRoot, token string) *Server {
	return &Server{st: st, pc: pc, met: met, bus: bus, journalRoot: journalRoot,
		token: token, loginLimiter: newLoginLimiter(),
		ConnectedAgents: func() []string { return nil }}
}

// SetAuth wires interactive login (WebUI username/password → session
// cookie). cfg and auther are nil when /etc/drsync/auth.yaml is absent, in
// which case the API stays token-only. sessions is required whenever cfg is
// non-nil (built from a secret persisted in the coordinator's data-dir).
func (s *Server) SetAuth(cfg *authn.Config, auther authn.Authenticator, sessions *authn.SessionManager) {
	s.authConfig = cfg
	s.authenticator = auther
	s.sessions = sessions
}

// SetHTTPSEnabled marks whether the HTTP listener is serving TLS, so the
// session cookie's Secure flag can be set correctly.
func (s *Server) SetHTTPSEnabled(enabled bool) {
	s.httpsEnabled = enabled
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok\n"))
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.met.Registry, promhttp.HandlerOpts{}))

	// Operations console (served unauthenticated so the page can load and
	// render its own login screen or token prompt; the data and action
	// endpoints below still enforce auth).
	mux.HandleFunc("GET /{$}", s.serveUI)
	mux.HandleFunc("GET /ui", s.serveUI)

	// Login/logout/whoami are unauthenticated by construction (you can't
	// require a session to obtain a session); handleLogin does its own
	// credential check and rate limiting.
	mux.HandleFunc("POST /api/v1/login", s.handleLogin)
	mux.HandleFunc("POST /api/v1/logout", s.handleLogout)
	mux.HandleFunc("GET /api/v1/whoami", s.handleWhoAmI)

	mux.HandleFunc("POST /api/v1/jobs", s.auth(s.submitJob))
	mux.HandleFunc("GET /api/v1/jobs", s.auth(s.listJobs))
	mux.HandleFunc("POST /api/v1/jobs/purge", s.auth(s.purgeJobs))
	mux.HandleFunc("GET /api/v1/jobs/{name}", s.auth(s.getJob))
	mux.HandleFunc("GET /api/v1/jobs/{name}/spec", s.auth(s.getJobSpec))
	mux.HandleFunc("DELETE /api/v1/jobs/{name}", s.auth(s.purgeJob))
	mux.HandleFunc("POST /api/v1/jobs/{name}/start", s.auth(s.jobAction("start")))
	mux.HandleFunc("POST /api/v1/jobs/{name}/pause", s.auth(s.jobAction("pause")))
	mux.HandleFunc("POST /api/v1/jobs/{name}/resume", s.auth(s.jobAction("resume")))
	mux.HandleFunc("POST /api/v1/jobs/{name}/cancel", s.auth(s.jobAction("cancel")))
	mux.HandleFunc("POST /api/v1/jobs/{name}/passes", s.auth(s.triggerPass))
	mux.HandleFunc("GET /api/v1/jobs/{name}/passes/{n}", s.auth(s.getPass))
	mux.HandleFunc("GET /api/v1/jobs/{name}/errors", s.auth(s.getErrors))
	mux.HandleFunc("GET /api/v1/jobs/{name}/journal", s.auth(s.getJournal))
	mux.HandleFunc("GET /api/v1/jobs/{name}/report", s.auth(s.getReport))
	mux.HandleFunc("GET /api/v1/info", s.auth(s.getInfo))
	mux.HandleFunc("GET /api/v1/agents", s.auth(s.listAgents))
	mux.HandleFunc("GET /api/v1/agents/{id}/inflight", s.auth(s.getAgentInflight))
	mux.HandleFunc("POST /api/v1/agents/{id}/enable", s.auth(s.setAgentEnabled(true)))
	mux.HandleFunc("POST /api/v1/agents/{id}/disable", s.auth(s.setAgentEnabled(false)))
	mux.HandleFunc("GET /api/v1/queue", s.auth(s.getQueue))
	mux.HandleFunc("POST /api/v1/parked/{id}/retry", s.auth(s.parkedShardAction("retry")))
	mux.HandleFunc("POST /api/v1/parked/{id}/drop", s.auth(s.parkedShardAction("drop")))
	mux.HandleFunc("POST /api/v1/jobs/{name}/parked/retry", s.auth(s.parkedJobAction("retry")))
	mux.HandleFunc("POST /api/v1/jobs/{name}/parked/drop", s.auth(s.parkedJobAction("drop")))
	mux.HandleFunc("GET /api/v1/events", s.auth(s.eventsWS))
	return cors(mux)
}

// getInfo returns static coordinator metadata for the console header.
func (s *Server) getInfo(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.Info)
}

// serveUI serves the embedded monitoring console.
func (s *Server) serveUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Write(webui.Console)
}

// cors adds permissive CORS headers so the console works both same-origin (the
// coordinator serves it) and standalone (opened from a file against a remote
// coordinator). Access-Control-Allow-Credentials is deliberately never set, so
// a wildcard origin cannot read any cross-origin response made with cookies
// attached; the session cookie itself is SameSite=Lax, so browsers won't
// attach it to a cross-site request in the first place (CSRF protection for
// the state-changing POST/DELETE routes). Preflights are answered here before
// the method-specific routes would 405 on OPTIONS.
func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// auth accepts either credential the API supports: the static bearer token
// (CLI/scripts, and the WebUI's token-entry fallback) or a signed session
// cookie (WebUI login). If neither a token nor auth.yaml is configured, the
// request passes through unauthenticated (dev mode, unchanged from before).
func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.token == "" && s.authenticator == nil {
			next(w, r)
			return
		}
		if s.token != "" && s.validBearerToken(r) {
			next(w, r)
			return
		}
		if s.sessions != nil && s.validSessionCookie(r) {
			next(w, r)
			return
		}
		httpErr(w, http.StatusUnauthorized, "invalid or missing credentials")
	}
}

func (s *Server) validBearerToken(r *http.Request) bool {
	got := r.Header.Get("Authorization")
	if got == "" && r.URL.Query().Get("token") != "" {
		// Browser WebSocket clients cannot set headers; accept the token as
		// a query parameter on an equal footing.
		got = "Bearer " + r.URL.Query().Get("token")
	}
	want := "Bearer " + s.token
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func (s *Server) validSessionCookie(r *http.Request) bool {
	c, err := r.Cookie(authn.CookieName)
	if err != nil {
		return false
	}
	_, err = s.sessions.Verify(c.Value)
	return err == nil
}

func httpErr(w http.ResponseWriter, code int, format string, args ...any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf(format, args...)})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// ---------------------------------------------------------------------------

type jobView struct {
	Name   string         `json:"name"`
	State  model.JobState `json:"state"`
	DryRun bool           `json:"dry_run"`
	Passes []passView     `json:"passes,omitempty"`
}

// jobListView is a jobs-list row: the job plus the pass rollup the console
// needs to draw it. It exists so listing N jobs costs one request rather than
// one per row (see store.JobSummaries).
type jobListView struct {
	Name          string          `json:"name"`
	State         model.JobState  `json:"state"`
	DryRun        bool            `json:"dry_run"`
	PassCount     int             `json:"pass_count"`
	PassNo        int             `json:"pass_no,omitempty"`
	PassState     model.PassState `json:"pass_state,omitempty"`
	EntriesWalked int64           `json:"entries_walked"`
	FilesCopied   int64           `json:"files_copied"`
	BytesCopied   int64           `json:"bytes_copied"`
	Errors        int64           `json:"errors"`
	CreatedAtMs   int64           `json:"created_at_ms"`
	UpdatedAtMs   int64           `json:"updated_at_ms"`
}

type passView struct {
	PassNo        int             `json:"pass_no"`
	State         model.PassState `json:"state"`
	EntriesWalked int64           `json:"entries_walked"`
	FilesCopied   int64           `json:"files_copied"`
	BytesCopied   int64           `json:"bytes_copied"`
	MetaFixed     int64           `json:"meta_fixed"`
	Orphans       int64           `json:"orphans"`
	Errors        int64           `json:"errors"`
	FidelityExc   int64           `json:"fidelity_exceptions"`
	VerifyOK      int64           `json:"verify_ok"`
	VerifyFail    int64           `json:"verify_fail"`
	StartedAtMs   int64           `json:"started_at_ms,omitempty"`
	FinishedAtMs  int64           `json:"finished_at_ms,omitempty"`
	// DurationMs is finished-started for a completed pass, or elapsed-so-far
	// (now-started) for one still running. Zero if the pass never started.
	DurationMs int64 `json:"duration_ms"`
}

func passViewOf(p *store.Pass) passView {
	v := passView{
		PassNo: p.PassNo, State: p.State, EntriesWalked: p.EntriesWalked,
		FilesCopied: p.FilesCopied, BytesCopied: p.BytesCopied,
		MetaFixed: p.MetaFixed, Orphans: p.Orphans, Errors: p.Errors,
		FidelityExc: p.FidelityExceptions,
		VerifyOK:    p.VerifyOK, VerifyFail: p.VerifyFail,
	}
	if p.Started.Valid {
		v.StartedAtMs = p.Started.Int64
		end := time.Now().UnixMilli() // running pass: elapsed so far
		if p.Finished.Valid {
			v.FinishedAtMs = p.Finished.Int64
			end = p.Finished.Int64
		}
		if d := end - p.Started.Int64; d > 0 {
			v.DurationMs = d
		}
	}
	return v
}

func (s *Server) submitJob(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		httpErr(w, http.StatusBadRequest, "read body: %v", err)
		return
	}
	spec, err := model.ParseSpec(body)
	if err != nil {
		httpErr(w, http.StatusUnprocessableEntity, "%v", err)
		return
	}
	dryRun := r.URL.Query().Get("dry_run") == "true"
	job, err := s.st.CreateJob(spec.Metadata.Name, body, dryRun)
	var dc *store.DestinationConflictError
	if errors.As(err, &dc) {
		httpErr(w, http.StatusConflict, "%s", destConflictMsg(dc))
		return
	}
	if err != nil {
		httpErr(w, http.StatusConflict, "create job: %v", err)
		return
	}
	slog.Info("job submitted", "job", job.Name, "dry_run", dryRun)
	writeJSON(w, http.StatusCreated, jobView{Name: job.Name, State: job.State, DryRun: job.DryRun})
}

// destConflictMsg explains an overlapping destination in operator terms: what
// breaks, and the two ways out.
func destConflictMsg(dc *store.DestinationConflictError) string {
	return fmt.Sprintf(
		"destination %q overlaps the destination of job %q (%s), which is still live. "+
			"Two jobs writing into one tree corrupt each other's in-progress files: each "+
			"reclaims the other's .drsync.tmp temps as stray residue, so a large file being "+
			"assembled by one job can be truncated or lost by the other. Finish or cancel %q, "+
			"or use a destination outside that tree.",
		dc.Dst, dc.Other, dc.OtherDst, dc.Other)
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.st.JobSummaries()
	if err != nil {
		httpErr(w, http.StatusInternalServerError, "%v", err)
		return
	}
	out := make([]jobListView, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, jobListView{
			Name: j.Name, State: j.State, DryRun: j.DryRun,
			PassCount: j.Passes, PassNo: j.LatestPassNo,
			PassState: j.LatestPassState, EntriesWalked: j.LatestEntriesWalked,
			FilesCopied: j.FilesCopied, BytesCopied: j.BytesCopied,
			Errors: j.Errors, CreatedAtMs: j.CreatedAt, UpdatedAtMs: j.UpdatedAt,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	job, err := s.st.GetJob(r.PathValue("name"))
	if errors.Is(err, sql.ErrNoRows) {
		httpErr(w, http.StatusNotFound, "no such job")
		return
	}
	if err != nil {
		httpErr(w, http.StatusInternalServerError, "%v", err)
		return
	}
	v := jobView{Name: job.Name, State: job.State, DryRun: job.DryRun}
	passes, err := s.st.ListPasses(job.ID)
	if err != nil {
		httpErr(w, http.StatusInternalServerError, "%v", err)
		return
	}
	for _, p := range passes {
		if p.State != model.PassComplete {
			kinds, err := s.st.ShardKindsPresent(p.ID)
			if err != nil {
				httpErr(w, http.StatusInternalServerError, "%v", err)
				return
			}
			p.State = p.State.EffectiveState(kinds)
		}
		v.Passes = append(v.Passes, passViewOf(p))
	}
	writeJSON(w, http.StatusOK, v)
}

// GET /api/v1/jobs/{name}/spec — the raw YAML the job was submitted with, so
// an operator can review it or resubmit a variant. Returned verbatim (not
// re-serialized from the parsed struct) so it reproduces exactly what was
// submitted, comments and all.
func (s *Server) getJobSpec(w http.ResponseWriter, r *http.Request) {
	job, err := s.st.GetJob(r.PathValue("name"))
	if errors.Is(err, sql.ErrNoRows) {
		httpErr(w, http.StatusNotFound, "no such job")
		return
	}
	if err != nil {
		httpErr(w, http.StatusInternalServerError, "%v", err)
		return
	}
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write(job.SpecYAML)
}

// dropJournal removes a purged job's on-disk journal (best effort; logged).
func (s *Server) dropJournal(name string, id int64) {
	if s.DropJournal == nil {
		return
	}
	if err := s.DropJournal(id); err != nil {
		slog.Warn("purge: journal cleanup failed", "job", name, "err", err)
	}
}

// DELETE /api/v1/jobs/{name} — purge one terminal job (rows + journal).
func (s *Server) purgeJob(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	id, err := s.st.DeleteJob(name)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		httpErr(w, http.StatusNotFound, "no such job")
		return
	case errors.Is(err, store.ErrJobActive):
		httpErr(w, http.StatusConflict,
			"job %q is not finished; cancel it before purging", name)
		return
	case err != nil:
		httpErr(w, http.StatusInternalServerError, "%v", err)
		return
	}
	s.dropJournal(name, id)
	slog.Info("job purged", "job", name)
	writeJSON(w, http.StatusOK, map[string]any{"purged": name})
}

// POST /api/v1/jobs/purge?state=completed|cancelled|failed|terminal&older_than_ms=N
// Bulk-purge terminal jobs. state defaults to "completed"; "terminal" matches
// any of completed/cancelled/failed. older_than_ms (optional) restricts to jobs
// last updated before now-older_than_ms.
func (s *Server) purgeJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	sel := strings.ToUpper(q.Get("state"))
	if sel == "" {
		sel = "COMPLETED"
	}
	if sel != "TERMINAL" && !store.TerminalJobState(sel) {
		httpErr(w, http.StatusBadRequest,
			"state must be completed, cancelled, failed or terminal")
		return
	}
	var cutoff int64
	if v := q.Get("older_than_ms"); v != "" {
		ms, err := strconv.ParseInt(v, 10, 64)
		if err != nil || ms < 0 {
			httpErr(w, http.StatusBadRequest, "older_than_ms must be a non-negative integer")
			return
		}
		if ms > 0 {
			cutoff = time.Now().UnixMilli() - ms
		}
	}
	dryRun := q.Get("dry_run") == "true"
	jobs, err := s.st.ListJobs()
	if err != nil {
		httpErr(w, http.StatusInternalServerError, "%v", err)
		return
	}
	purged := []string{}
	for _, j := range jobs {
		state := string(j.State)
		if !store.TerminalJobState(state) {
			continue
		}
		if sel != "TERMINAL" && sel != state {
			continue
		}
		if cutoff > 0 && j.UpdatedAt >= cutoff {
			continue
		}
		if dryRun { // preview: match but delete nothing
			purged = append(purged, j.Name)
			continue
		}
		id, err := s.st.DeleteJob(j.Name)
		if err != nil {
			continue // raced into non-terminal, or vanished; skip
		}
		s.dropJournal(j.Name, id)
		purged = append(purged, j.Name)
	}
	slog.Info("bulk job purge", "state", sel, "count", len(purged), "dry_run", dryRun)
	writeJSON(w, http.StatusOK,
		map[string]any{"purged": purged, "count": len(purged), "dry_run": dryRun})
}

func (s *Server) jobAction(action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		var err error
		switch action {
		case "start":
			err = s.pc.StartJob(name)
		case "pause":
			err = s.setState(name, model.JobRunning, model.JobPaused)
		case "resume":
			err = s.pc.ResumeJob(name) // same destination gate as start
		case "cancel":
			var job *store.Job
			if job, err = s.st.GetJob(name); err == nil {
				if err = s.st.SetJobState(job.ID, model.JobCancelled); err == nil && s.NotifyJobDone != nil {
					// Tell agents to release the job's cached options + root fds.
					s.NotifyJobDone(job.ID)
				}
			}
		}
		if errors.Is(err, sql.ErrNoRows) {
			httpErr(w, http.StatusNotFound, "no such job")
			return
		}
		var dc *store.DestinationConflictError
		if errors.As(err, &dc) {
			httpErr(w, http.StatusConflict, "cannot %s: %s", action, destConflictMsg(dc))
			return
		}
		if err != nil {
			httpErr(w, http.StatusConflict, "%v", err)
			return
		}
		slog.Info("job action", "job", name, "action", action)
		writeJSON(w, http.StatusOK, map[string]string{"job": name, "action": action})
	}
}

func (s *Server) setState(name string, from, to model.JobState) error {
	job, err := s.st.GetJob(name)
	if err != nil {
		return err
	}
	if job.State != from {
		return fmt.Errorf("job %q is %s; expected %s", name, job.State, from)
	}
	return s.st.SetJobState(job.ID, to)
}

func (s *Server) triggerPass(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var req struct {
		Delete  bool   `json:"delete"`
		Confirm string `json:"confirm"`
	}
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			httpErr(w, http.StatusBadRequest, "decode body: %v", err)
			return
		}
	}
	if req.Delete && req.Confirm != name {
		// Second gate for destructive passes (D5): confirm must echo job name.
		httpErr(w, http.StatusPreconditionFailed, "delete pass requires confirm == job name")
		return
	}
	if err := s.pc.TriggerPass(name, req.Delete); err != nil {
		httpErr(w, http.StatusConflict, "%v", err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"job": name, "delete": req.Delete})
}

func (s *Server) listAgents(w http.ResponseWriter, r *http.Request) {
	agents, err := s.st.ListAgents()
	if err != nil {
		httpErr(w, http.StatusInternalServerError, "%v", err)
		return
	}
	live := map[string]bool{}
	for _, id := range s.ConnectedAgents() {
		live[id] = true
	}
	type agentView struct {
		ID            string `json:"id"`
		Hostname      string `json:"hostname"`
		Version       string `json:"version"`
		ProtoMinor    uint32 `json:"proto_minor"`
		Stale         bool   `json:"stale"` // behind the coordinator's protocol minor
		State         string `json:"state"`
		Connected     bool   `json:"connected"`
		Enabled       bool   `json:"enabled"`
		LastHeartbeat int64  `json:"last_heartbeat_ms"`
	}
	out := make([]agentView, 0, len(agents))
	for _, a := range agents {
		out = append(out, agentView{ID: a.ID, Hostname: a.Hostname, Version: a.Version,
			ProtoMinor: a.ProtoMinor, Stale: a.ProtoMinor < agentsrv.ProtoMinor,
			State: a.State, Connected: live[a.ID], Enabled: a.Enabled, LastHeartbeat: a.LastHeartbeat})
	}
	writeJSON(w, http.StatusOK, out)
}

// getAgentInflight answers "what is this agent doing right now" from the last
// heartbeat's snapshot.
//
// `supported: false` means the agent is too old to report in-flight detail —
// deliberately distinct from an empty list, which means the agent really is
// holding nothing. Conflating the two would make a stale agent look idle.
func (s *Server) getAgentInflight(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if s.AgentInflight == nil {
		httpErr(w, http.StatusServiceUnavailable, "agent server not attached")
		return
	}
	items, at, reports, connected := s.AgentInflight(id)
	if !connected {
		httpErr(w, http.StatusNotFound, "agent not connected")
		return
	}
	type itemView struct {
		LeaseID     uint64 `json:"lease_id"`
		ShardID     uint64 `json:"shard_id"`
		JobID       uint64 `json:"job_id"`
		Kind        string `json:"kind"`
		RelPath     string `json:"rel_path"`
		HeldMS      uint32 `json:"held_ms"`
		RunningMS   uint32 `json:"running_ms"`
		Running     bool   `json:"running"`
		EntriesDone uint64 `json:"entries_done"`
	}
	out := make([]itemView, 0, len(items))
	for _, it := range items {
		out = append(out, itemView{
			LeaseID: it.LeaseId, ShardID: it.ShardId, JobID: it.JobId,
			Kind: it.Kind, RelPath: it.RelPath, HeldMS: it.HeldMs,
			RunningMS: it.RunningMs, Running: it.Running, EntriesDone: it.EntriesDone,
		})
	}
	// Longest-running first: the head of this list is what to investigate.
	sort.Slice(out, func(i, j int) bool { return out[i].RunningMS > out[j].RunningMS })
	var reportedAt int64
	if !at.IsZero() {
		reportedAt = at.UnixMilli()
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"agent":          id,
		"supported":      reports,
		"reported_at_ms": reportedAt,
		"inflight":       out,
	})
}

// setAgentEnabled toggles an agent's scheduling flag. Disabled agents stay
// connected and finish their running leases but receive no new grants, and are
// told to drain: they hand back any shards still queued (unstarted) on them so
// active agents can pick that work up immediately rather than after it drains.
func (s *Server) setAgentEnabled(enabled bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		err := s.st.SetAgentEnabled(id, enabled)
		if errors.Is(err, sql.ErrNoRows) {
			httpErr(w, http.StatusNotFound, "no such agent")
			return
		}
		if err != nil {
			httpErr(w, http.StatusInternalServerError, "%v", err)
			return
		}
		// Disabling drains the node; enabling clears the drain so it takes work
		// again. Best-effort: an offline agent has nothing queued to hand back.
		if s.SetAgentDrain != nil {
			s.SetAgentDrain(id, !enabled)
		}
		action := "disable"
		if enabled {
			action = "enable"
		}
		slog.Info("agent action", "agent", id, "action", action)
		writeJSON(w, http.StatusOK, map[string]any{"agent": id, "enabled": enabled})
	}
}

// parkedShardAction retries or drops a single PARKED shard by id.
//
//	POST /api/v1/parked/{id}/retry — requeue for a fresh attempt (any agent)
//	POST /api/v1/parked/{id}/drop  — discard, accepting the gap
func (s *Server) parkedShardAction(action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil {
			httpErr(w, http.StatusBadRequest, "invalid shard id")
			return
		}
		if action == "retry" {
			err = s.st.RetryParkedShard(id)
		} else {
			err = s.st.DropParkedShard(id)
		}
		if errors.Is(err, store.ErrNotParked) {
			httpErr(w, http.StatusNotFound, "no parked shard with id %d", id)
			return
		}
		if err != nil {
			httpErr(w, http.StatusInternalServerError, "%v", err)
			return
		}
		slog.Info("parked shard action", "shard", id, "action", action)
		writeJSON(w, http.StatusOK, map[string]any{"shard_id": id, "action": action})
	}
}

// parkedJobAction retries or drops every PARKED shard of one job.
//
//	POST /api/v1/jobs/{name}/parked/retry
//	POST /api/v1/jobs/{name}/parked/drop
func (s *Server) parkedJobAction(action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		var n int64
		var err error
		if action == "retry" {
			n, err = s.st.RetryParkedByJob(name)
		} else {
			n, err = s.st.DropParkedByJob(name)
		}
		if err != nil {
			httpErr(w, http.StatusInternalServerError, "%v", err)
			return
		}
		slog.Info("parked job action", "job", name, "action", action, "count", n)
		writeJSON(w, http.StatusOK, map[string]any{"job": name, "action": action, "count": n})
	}
}
