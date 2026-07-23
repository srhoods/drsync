package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestGetJobSpecReturnsSubmittedYAMLVerbatim: the console's view-settings and
// resubmit-with-previous-settings features both depend on getting back
// exactly the bytes a job was submitted with, not a re-serialization of the
// parsed spec (which would drop comments and normalize formatting).
func TestGetJobSpecReturnsSubmittedYAMLVerbatim(t *testing.T) {
	srv, submit := submitSrv(t)
	spec := specFor("alpha", "/src/a", "/dst/a")
	if code, body := submit(spec); code != http.StatusCreated {
		t.Fatalf("submit: status %d: %s", code, body)
	}

	r := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/alpha/spec", nil)
	r.SetPathValue("name", "alpha")
	w := httptest.NewRecorder()
	srv.getJobSpec(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	if got := w.Body.String(); got != spec {
		t.Errorf("spec round-trip mismatch:\n got: %q\nwant: %q", got, spec)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/yaml; charset=utf-8" {
		t.Errorf("Content-Type = %q", ct)
	}
}

func TestGetJobSpecUnknownJobIs404(t *testing.T) {
	srv, _ := submitSrv(t)

	r := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/nosuch/spec", nil)
	r.SetPathValue("name", "nosuch")
	w := httptest.NewRecorder()
	srv.getJobSpec(w, r)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status %d, want 404: %s", w.Code, w.Body.String())
	}
}
