// Package api serves the REST surface the CLI and (later) WebUI consume
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
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"drsync/coordinator/internal/events"
	"drsync/coordinator/internal/metrics"
	"drsync/coordinator/internal/model"
	"drsync/coordinator/internal/passctrl"
	"drsync/coordinator/internal/store"
)

type Server struct {
	st          *store.Store
	pc          *passctrl.Controller
	met         *metrics.Metrics
	bus         *events.Bus
	journalRoot string
	token       string // empty = no auth (dev only)
	// ConnectedAgents is injected by agentsrv for the fleet view.
	ConnectedAgents func() []string
	// DropJournal removes a purged job's on-disk journal segments; injected in
	// main so the API can reclaim disk on `job purge`.
	DropJournal func(jobID int64) error
}

func New(st *store.Store, pc *passctrl.Controller, met *metrics.Metrics,
	bus *events.Bus, journalRoot, token string) *Server {
	return &Server{st: st, pc: pc, met: met, bus: bus, journalRoot: journalRoot,
		token: token, ConnectedAgents: func() []string { return nil }}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok\n"))
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.met.Registry, promhttp.HandlerOpts{}))

	mux.HandleFunc("POST /api/v1/jobs", s.auth(s.submitJob))
	mux.HandleFunc("GET /api/v1/jobs", s.auth(s.listJobs))
	mux.HandleFunc("POST /api/v1/jobs/purge", s.auth(s.purgeJobs))
	mux.HandleFunc("GET /api/v1/jobs/{name}", s.auth(s.getJob))
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
	mux.HandleFunc("GET /api/v1/agents", s.auth(s.listAgents))
	mux.HandleFunc("POST /api/v1/agents/{id}/enable", s.auth(s.setAgentEnabled(true)))
	mux.HandleFunc("POST /api/v1/agents/{id}/disable", s.auth(s.setAgentEnabled(false)))
	mux.HandleFunc("GET /api/v1/queue", s.auth(s.getQueue))
	mux.HandleFunc("GET /api/v1/events", s.auth(s.eventsWS))
	return mux
}

func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.token != "" {
			got := r.Header.Get("Authorization")
			if got == "" && r.URL.Query().Get("token") != "" {
				// Browser WebSocket clients cannot set headers; accept the
				// token as a query parameter on an equal footing.
				got = "Bearer " + r.URL.Query().Get("token")
			}
			want := "Bearer " + s.token
			if subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
				httpErr(w, http.StatusUnauthorized, "invalid or missing bearer token")
				return
			}
		}
		next(w, r)
	}
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
}

func passViewOf(p *store.Pass) passView {
	return passView{
		PassNo: p.PassNo, State: p.State, EntriesWalked: p.EntriesWalked,
		FilesCopied: p.FilesCopied, BytesCopied: p.BytesCopied,
		MetaFixed: p.MetaFixed, Orphans: p.Orphans, Errors: p.Errors,
		FidelityExc: p.FidelityExceptions,
		VerifyOK:    p.VerifyOK, VerifyFail: p.VerifyFail,
	}
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
	if err != nil {
		httpErr(w, http.StatusConflict, "create job: %v", err)
		return
	}
	slog.Info("job submitted", "job", job.Name, "dry_run", dryRun)
	writeJSON(w, http.StatusCreated, jobView{Name: job.Name, State: job.State, DryRun: job.DryRun})
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.st.ListJobs()
	if err != nil {
		httpErr(w, http.StatusInternalServerError, "%v", err)
		return
	}
	out := make([]jobView, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, jobView{Name: j.Name, State: j.State, DryRun: j.DryRun})
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
		v.Passes = append(v.Passes, passViewOf(p))
	}
	writeJSON(w, http.StatusOK, v)
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
			err = s.setState(name, model.JobPaused, model.JobRunning)
		case "cancel":
			var job *store.Job
			if job, err = s.st.GetJob(name); err == nil {
				err = s.st.SetJobState(job.ID, model.JobCancelled)
			}
		}
		if errors.Is(err, sql.ErrNoRows) {
			httpErr(w, http.StatusNotFound, "no such job")
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
		State         string `json:"state"`
		Connected     bool   `json:"connected"`
		Enabled       bool   `json:"enabled"`
		LastHeartbeat int64  `json:"last_heartbeat_ms"`
	}
	out := make([]agentView, 0, len(agents))
	for _, a := range agents {
		out = append(out, agentView{ID: a.ID, Hostname: a.Hostname, Version: a.Version,
			State: a.State, Connected: live[a.ID], Enabled: a.Enabled, LastHeartbeat: a.LastHeartbeat})
	}
	writeJSON(w, http.StatusOK, out)
}

// setAgentEnabled toggles an agent's scheduling flag. Disabled agents stay
// connected and finish in-flight leases but receive no new shard grants.
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
		action := "disable"
		if enabled {
			action = "enable"
		}
		slog.Info("agent action", "agent", id, "action", action)
		writeJSON(w, http.StatusOK, map[string]any{"agent": id, "enabled": enabled})
	}
}
