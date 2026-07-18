package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"drsync/coordinator/internal/metrics"
	"drsync/coordinator/internal/model"
	"drsync/coordinator/internal/store"
	drsyncpb "drsync/proto/gen/drsyncpb"
)

// These tests pin the read-side contract the WebUI console binds to. The
// console renders these fields directly, so a field that silently stops being
// emitted shows up there as a blank or a dash rather than as a failure —
// exactly the class of regression that is invisible until an operator hits it
// mid-migration.

func consoleSrv(t *testing.T) *Server {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return New(st, nil, metrics.New(), nil, filepath.Join(dir, "journals"), "")
}

func getJSON(t *testing.T, h http.HandlerFunc, path string, into any) {
	t.Helper()
	w := httptest.NewRecorder()
	h(w, httptest.NewRequest(http.MethodGet, path, nil))
	if w.Code != http.StatusOK {
		t.Fatalf("%s: status %d: %s", path, w.Code, w.Body.String())
	}
	if err := json.Unmarshal(w.Body.Bytes(), into); err != nil {
		t.Fatalf("%s: decode: %v (body %s)", path, err, w.Body.String())
	}
}

// The jobs list carries each job's pass rollup so the console can draw a row
// without a follow-up detail fetch per job. Losing these fields would silently
// reintroduce the N+1 the console used to issue on every 2.5s poll.
func TestListJobsCarriesPassRollup(t *testing.T) {
	srv := consoleSrv(t)
	job, err := srv.st.CreateJob("rollup", []byte(specFor("rollup", "/src/a", "/dst/a")), false)
	if err != nil {
		t.Fatal(err)
	}
	p1, err := srv.st.CreatePass(job.ID, 1, model.PassComplete)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := srv.st.CreatePass(job.ID, 2, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	// Two passes with counters: the list must report lifetime sums but the
	// *latest* pass's identity and walk count.
	mustAccumulate(t, srv, p1.ID, 100, 7, 4096, 1)
	mustAccumulate(t, srv, p2.ID, 250, 3, 512, 0)

	var got []struct {
		Name          string `json:"name"`
		PassCount     int    `json:"pass_count"`
		PassNo        int    `json:"pass_no"`
		PassState     string `json:"pass_state"`
		EntriesWalked int64  `json:"entries_walked"`
		FilesCopied   int64  `json:"files_copied"`
		BytesCopied   int64  `json:"bytes_copied"`
		Errors        int64  `json:"errors"`
	}
	getJSON(t, srv.listJobs, "/api/v1/jobs", &got)
	if len(got) != 1 {
		t.Fatalf("want 1 job, got %d", len(got))
	}
	j := got[0]
	if j.PassCount != 2 || j.PassNo != 2 || j.PassState != string(model.PassScanning) {
		t.Errorf("latest pass wrong: count=%d no=%d state=%q", j.PassCount, j.PassNo, j.PassState)
	}
	if j.EntriesWalked != 250 {
		t.Errorf("entries_walked = %d, want the latest pass's 250", j.EntriesWalked)
	}
	if j.FilesCopied != 10 || j.BytesCopied != 4608 || j.Errors != 1 {
		t.Errorf("totals wrong: files=%d bytes=%d errors=%d, want 10/4608/1",
			j.FilesCopied, j.BytesCopied, j.Errors)
	}
}

// A job with no passes yet must still list cleanly — the LEFT JOINs have to
// yield zeroes rather than dropping the row or erroring on NULLs.
func TestListJobsWithNoPasses(t *testing.T) {
	srv := consoleSrv(t)
	if _, err := srv.st.CreateJob("fresh", []byte(specFor("fresh", "/src/b", "/dst/b")), true); err != nil {
		t.Fatal(err)
	}
	var got []struct {
		Name      string `json:"name"`
		DryRun    bool   `json:"dry_run"`
		PassCount int    `json:"pass_count"`
		PassNo    int    `json:"pass_no"`
	}
	getJSON(t, srv.listJobs, "/api/v1/jobs", &got)
	if len(got) != 1 || got[0].Name != "fresh" {
		t.Fatalf("want the passless job listed, got %+v", got)
	}
	if got[0].PassCount != 0 || got[0].PassNo != 0 {
		t.Errorf("passless job reports pass_count=%d pass_no=%d, want 0/0",
			got[0].PassCount, got[0].PassNo)
	}
	if !got[0].DryRun {
		t.Error("dry_run lost in the rollup query")
	}
}

// The console's parked-shard table shows how long a shard has been parked, so
// an operator can tell a failure that just happened from stale residue. That
// needs the park timestamp on the wire.
func TestQueueParkedCarriesParkTime(t *testing.T) {
	srv := consoleSrv(t)
	shardID, before := parkOneShard(t, srv)

	var got struct {
		Parked []struct {
			ShardID    int64  `json:"shard_id"`
			Job        string `json:"job"`
			RelPath    string `json:"rel_path"`
			Error      string `json:"error"`
			Attempt    int    `json:"attempt"`
			ParkedAtMs int64  `json:"parked_at_ms"`
		} `json:"parked"`
	}
	getJSON(t, srv.getQueue, "/api/v1/queue", &got)
	if len(got.Parked) != 1 {
		t.Fatalf("want 1 parked shard, got %d", len(got.Parked))
	}
	p := got.Parked[0]
	if p.ShardID != shardID || p.Job != "parked-job" || p.RelPath != "deep/file.bin" {
		t.Errorf("parked identity wrong: %+v", p)
	}
	if p.Error != "EIO on read" {
		t.Errorf("error text = %q, want the park reason", p.Error)
	}
	if p.ParkedAtMs < before {
		t.Errorf("parked_at_ms = %d, want >= %d (the park time)", p.ParkedAtMs, before)
	}
}

// The report's parked list feeds the same table for a single job.
func TestReportParkedCarriesParkTime(t *testing.T) {
	srv := consoleSrv(t)
	_, before := parkOneShard(t, srv)

	var got struct {
		ParkedShards []struct {
			ParkedAtMs int64 `json:"parked_at_ms"`
		} `json:"parked_shards"`
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/parked-job/report", nil)
	r.SetPathValue("name", "parked-job")
	srv.getReport(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("report: status %d: %s", w.Code, w.Body.String())
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.ParkedShards) != 1 {
		t.Fatalf("want 1 parked shard in report, got %d", len(got.ParkedShards))
	}
	if got.ParkedShards[0].ParkedAtMs < before {
		t.Errorf("parked_at_ms = %d, want >= %d", got.ParkedShards[0].ParkedAtMs, before)
	}
}

// parkOneShard drives a shard through lease → park the way the scheduler does,
// and returns its id plus a timestamp taken before the park.
func parkOneShard(t *testing.T, srv *Server) (int64, int64) {
	t.Helper()
	job, err := srv.st.CreateJob("parked-job", []byte(specFor("parked-job", "/src/c", "/dst/c")), false)
	if err != nil {
		t.Fatal(err)
	}
	// Shards are only leasable out of a RUNNING job, which is the state a shard
	// would actually be parked from.
	if err := srv.st.SetJobState(job.ID, model.JobRunning); err != nil {
		t.Fatal(err)
	}
	pass, err := srv.st.CreatePass(job.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := srv.st.InsertShards(pass.ID, 0, []store.NewShard{
		{Kind: model.KindDir, RelPath: "deep/file.bin"},
	})
	if err != nil {
		t.Fatal(err)
	}
	leased, err := srv.st.LeaseShards("agent-a", 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(leased) != 1 {
		t.Fatalf("want 1 leased shard, got %d", len(leased))
	}
	before := time.Now().UnixMilli()
	if err := srv.st.ParkShard(ids[0], leased[0].LeaseID, "EIO on read"); err != nil {
		t.Fatal(err)
	}
	return ids[0], before
}

func mustAccumulate(t *testing.T, srv *Server, passID int64, walked, files, bytes, errs uint64) {
	t.Helper()
	c := &drsyncpb.ShardCounters{
		EntriesWalked: walked, FilesCopied: files,
		BytesCopied: bytes, Errors: errs,
	}
	if err := srv.st.AccumulatePassCounters(passID, c); err != nil {
		t.Fatal(err)
	}
}
