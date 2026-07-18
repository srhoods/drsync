package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"drsync/coordinator/internal/metrics"
	"drsync/coordinator/internal/model"
	"drsync/coordinator/internal/store"
)

func specFor(name, src, dst string) string {
	return fmt.Sprintf(`apiVersion: drsync/v1
kind: Job
metadata: { name: %s }
spec:
  source: { path: %s }
  destination: { path: %s }
`, name, src, dst)
}

// submitSrv builds a server over a real store, plus a helper that submits a
// spec and returns the status code and body.
func submitSrv(t *testing.T) (*Server, func(spec string) (int, string)) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	srv := New(st, nil, metrics.New(), nil, filepath.Join(dir, "journals"), "")
	return srv, func(spec string) (int, string) {
		t.Helper()
		r := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", strings.NewReader(spec))
		w := httptest.NewRecorder()
		srv.submitJob(w, r)
		return w.Code, w.Body.String()
	}
}

// TestSubmitRejectsOverlappingDestination: two live jobs must never share a
// destination tree. An agent's orphan sweep can only recognise its own job+pass
// as live work, so job A's chunk temp — present for the whole multi-host
// assembly of a big file — looks like stray residue to job B's walk of the same
// directory and is unlinked underneath it, failing A's finalize or letting A
// rename a partially written file into place. The tagged-temp fix cannot see
// across jobs, so submit is where this is stopped.
func TestSubmitRejectsOverlappingDestination(t *testing.T) {
	_, submit := submitSrv(t)

	if code, body := submit(specFor("first", "/src/a", "/dst/home")); code != http.StatusCreated {
		t.Fatalf("first submit: status %d: %s", code, body)
	}

	for _, tc := range []struct {
		name, dst string
	}{
		{"same", "/dst/home"},
		{"trailing-slash", "/dst/home/"},
		{"nested-under", "/dst/home/users"},
		{"contains", "/dst"},
		// "/" is not listed: it contains the source too, so single-spec
		// validation rejects it as non-disjoint (422) before this check runs.
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, body := submit(specFor("second", "/src/b", tc.dst))
			if code != http.StatusConflict {
				t.Fatalf("destination %q: status %d, want 409 conflict: %s", tc.dst, code, body)
			}
			if !strings.Contains(body, "first") {
				t.Errorf("conflict for %q does not name the job it clashes with: %s", tc.dst, body)
			}
		})
	}

	// A sibling directory is not an overlap — the check is on whole components,
	// so an over-eager string prefix test would wrongly block this.
	if code, body := submit(specFor("sibling", "/src/c", "/dst/home2")); code != http.StatusCreated {
		t.Fatalf("sibling destination rejected: status %d: %s", code, body)
	}
}

// A finished job holds no destination: its tree can be re-synced by a new job.
// Each state gets a fresh store — reusing one would leave the job created by
// the previous iteration live on that destination, so the next submit would
// conflict with it rather than with the terminal job under test.
func TestSubmitAllowsOverlapWithFinishedJob(t *testing.T) {
	for _, state := range []model.JobState{model.JobCompleted, model.JobCancelled, model.JobFailed} {
		t.Run(string(state), func(t *testing.T) {
			srv, submit := submitSrv(t)
			if code, body := submit(specFor("done", "/src/a", "/dst/home")); code != http.StatusCreated {
				t.Fatalf("first submit: status %d: %s", code, body)
			}
			job, err := srv.st.GetJob("done")
			if err != nil {
				t.Fatal(err)
			}
			if err := srv.st.SetJobState(job.ID, state); err != nil {
				t.Fatal(err)
			}
			code, body := submit(specFor("next", "/src/b", "/dst/home"))
			if code != http.StatusCreated {
				t.Fatalf("overlap with %s job rejected: status %d: %s", state, code, body)
			}
		})
	}
}

// Re-submitting a name that already exists must report the name clash, not a
// self-overlap on its own destination.
func TestSubmitDuplicateNameReportsNameConflict(t *testing.T) {
	_, submit := submitSrv(t)
	if code, body := submit(specFor("dup", "/src/a", "/dst/home")); code != http.StatusCreated {
		t.Fatalf("first submit: status %d: %s", code, body)
	}
	code, body := submit(specFor("dup", "/src/a", "/dst/home"))
	if code != http.StatusConflict {
		t.Fatalf("duplicate name: status %d, want 409: %s", code, body)
	}
	if strings.Contains(body, "overlaps the destination") {
		t.Errorf("duplicate name reported as a destination overlap with itself: %s", body)
	}
}
