package api

import (
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"

	"drsync/coordinator/internal/journal"
	"drsync/coordinator/internal/metrics"
	"drsync/coordinator/internal/model"
	"drsync/coordinator/internal/store"
	drsyncpb "drsync/proto/gen/drsyncpb"
)

// writeJournal lays down one pass's records in the same on-disk framing agents
// produce (zstd of length-delimited JournalRecords inside a JournalBatch).
func writeJournal(t *testing.T, jw *journal.Writer, jobID int64, passNo int,
	recs ...*drsyncpb.JournalRecord) {
	t.Helper()
	var raw []byte
	for _, r := range recs {
		b, err := proto.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		raw = binary.AppendUvarint(raw, uint64(len(b)))
		raw = append(raw, b...)
	}
	enc, _ := zstd.NewWriter(nil)
	z := enc.EncodeAll(raw, nil)
	enc.Close()
	if err := jw.Append(&drsyncpb.JournalBatch{
		Seq: 1, JobId: uint64(jobID), PassNo: uint32(passNo),
		RecordCount: uint32(len(recs)), RecordsZstd: z,
	}); err != nil {
		t.Fatal(err)
	}
	if err := jw.Flush(); err != nil {
		t.Fatal(err)
	}
}

func rec(typ drsyncpb.JournalRecord_Type, rel, detail string, errno int32) *drsyncpb.JournalRecord {
	return &drsyncpb.JournalRecord{Type: typ, RelPath: []byte(rel), Detail: detail, Errno: errno}
}

// setup builds a server over a real store + on-disk journal with two passes:
// pass 1 holds the failures, pass 2 is the clean converged pass.
func setup(t *testing.T) (*Server, int64) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	job, err := st.CreateJob("j", []byte("spec"), false)
	if err != nil {
		t.Fatal(err)
	}
	p1, _ := st.CreatePass(job.ID, 1, model.PassScanning)
	st.SetPassState(p1.ID, model.PassComplete)
	p2, _ := st.CreatePass(job.ID, 2, model.PassScanning)
	st.SetPassState(p2.ID, model.PassComplete)

	jroot := filepath.Join(dir, "journals")
	jw, err := journal.NewWriter(jroot)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { jw.Close() })
	// Pass 1: a verify failure and a walk error — the records an operator hunts.
	writeJournal(t, jw, job.ID, 1,
		rec(drsyncpb.JournalRecord_JR_VERIFY_FAIL, "mysock", "mtime mismatch", 0),
		rec(drsyncpb.JournalRecord_JR_ERROR, "locked/f", "open src", 13 /*EACCES*/),
		rec(drsyncpb.JournalRecord_JR_COPIED, "a.txt", "", 0),
	)
	// Pass 2 (latest): only clean dir metadata.
	writeJournal(t, jw, job.ID, 2,
		rec(drsyncpb.JournalRecord_JR_DIR_META, "bigdir", "", 0))

	srv := New(st, nil, metrics.New(), nil, jroot, "")
	return srv, job.ID
}

func doJSON(t *testing.T, h http.HandlerFunc, target string) map[string]any {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, target, nil)
	r.SetPathValue("name", "j")
	w := httptest.NewRecorder()
	h(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	var out map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// The default view (no pass=) must reach the pass-1 failures, and a verify
// failure must be reported as an error. This is the regression for "these
// errors do not appear in the operator surfaces" (default was latest-pass, and
// the errors browser ignored JR_VERIFY_FAIL entirely).
func TestErrorsDefaultAllPassesIncludesVerifyFail(t *testing.T) {
	srv, _ := setup(t)
	out := doJSON(t, srv.getErrors, "/api/v1/jobs/j/errors")

	errs, _ := out["errors"].([]any)
	if len(errs) != 2 {
		t.Fatalf("want 2 error records (verify_fail + error), got %d: %v", len(errs), out)
	}
	types := map[string]bool{}
	for _, e := range errs {
		m := e.(map[string]any)
		types[m["type"].(string)] = true
	}
	if !types["VERIFY_FAIL"] {
		t.Error("verify failure not surfaced by the errors browser")
	}
	if !types["ERROR"] {
		t.Error("walk error not surfaced")
	}
	byClass, _ := out["by_class"].(map[string]any)
	if _, ok := byClass["VERIFY_FAIL"]; !ok {
		t.Errorf("verify failure not grouped under VERIFY_FAIL: %v", byClass)
	}
	if _, ok := byClass["EACCES"]; !ok {
		t.Errorf("errno error not grouped under EACCES: %v", byClass)
	}
}

// Explicit pass=latest still scopes to the final pass (no failures there).
func TestErrorsPassLatestScopes(t *testing.T) {
	srv, _ := setup(t)
	out := doJSON(t, srv.getErrors, "/api/v1/jobs/j/errors?pass=latest")
	if errs, _ := out["errors"].([]any); len(errs) != 0 {
		t.Fatalf("pass=latest should find no errors, got %v", errs)
	}
}

// journal cat's default must likewise span all passes so earlier-pass records
// (the verify failure) are returned, not just the latest pass.
func TestJournalDefaultSpansAllPasses(t *testing.T) {
	srv, _ := setup(t)
	out := doJSON(t, srv.getJournal, "/api/v1/jobs/j/journal")
	recs, _ := out["records"].([]any)
	seen := map[string]bool{}
	for _, r := range recs {
		seen[r.(map[string]any)["type"].(string)] = true
	}
	if !seen["VERIFY_FAIL"] || !seen["DIR_META"] {
		t.Fatalf("default journal view should span passes 1 and 2; saw %v", seen)
	}
}
