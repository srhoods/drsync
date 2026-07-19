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

// The Fleet table's expandable row binds to these exact field names. A rename
// would not fail any server-side test — the panel would just render blank — so
// the shape is pinned here.
func TestAgentInflightShape(t *testing.T) {
	srv := consoleSrv(t)
	srv.AgentInflight = func(id string) ([]*drsyncpb.InflightItem, time.Time, bool, bool) {
		return []*drsyncpb.InflightItem{
			{LeaseId: 9, ShardId: 501, JobId: 1, Kind: "chunk", RelPath: "big/file.bin",
				HeldMs: 812000, RunningMs: 795000, Running: true, EntriesDone: 41200},
			{LeaseId: 10, ShardId: 502, JobId: 1, Kind: "entrylist", RelPath: "some/dir",
				HeldMs: 4200, RunningMs: 0, Running: false, EntriesDone: 0},
		}, time.Now(), true, true
	}

	var got struct {
		Agent        string `json:"agent"`
		Supported    bool   `json:"supported"`
		ReportedAtMs int64  `json:"reported_at_ms"`
		Inflight     []struct {
			Kind        string `json:"kind"`
			RelPath     string `json:"rel_path"`
			HeldMS      uint32 `json:"held_ms"`
			RunningMS   uint32 `json:"running_ms"`
			Running     bool   `json:"running"`
			EntriesDone uint64 `json:"entries_done"`
		} `json:"inflight"`
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/v1/agents/a1/inflight", nil)
	r.SetPathValue("id", "a1")
	srv.getAgentInflight(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if !got.Supported || got.ReportedAtMs == 0 {
		t.Errorf("supported=%v reported_at_ms=%d, want true and non-zero",
			got.Supported, got.ReportedAtMs)
	}
	if len(got.Inflight) != 2 {
		t.Fatalf("want 2 in-flight items, got %d", len(got.Inflight))
	}
	// Longest-running first: the console marks the head as the one to look at.
	if !got.Inflight[0].Running || got.Inflight[0].RunningMS != 795000 {
		t.Errorf("head item = %+v, want the running 795s one first", got.Inflight[0])
	}
	if got.Inflight[0].Kind != "chunk" || got.Inflight[0].RelPath != "big/file.bin" {
		t.Errorf("identity fields wrong: %+v", got.Inflight[0])
	}
	if got.Inflight[0].EntriesDone != 41200 {
		t.Errorf("entries_done = %d, want 41200 (stuck-vs-slow signal)", got.Inflight[0].EntriesDone)
	}
	// A queued item reports running=false with running_ms 0; the console falls
	// back to held_ms for it, so both must survive.
	q := got.Inflight[1]
	if q.Running || q.RunningMS != 0 || q.HeldMS != 4200 {
		t.Errorf("queued item = %+v, want running=false running_ms=0 held_ms=4200", q)
	}
}

// An agent too old to report in-flight detail must be distinguishable from one
// that is genuinely idle, or the console would show "idle" for a stale build.
func TestAgentInflightUnsupportedIsNotIdle(t *testing.T) {
	srv := consoleSrv(t)
	srv.AgentInflight = func(id string) ([]*drsyncpb.InflightItem, time.Time, bool, bool) {
		return nil, time.Time{}, false, true // connected, but does not report
	}
	var got struct {
		Supported bool       `json:"supported"`
		Inflight  []struct{} `json:"inflight"`
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/v1/agents/old/inflight", nil)
	r.SetPathValue("id", "old")
	srv.getAgentInflight(w, r)
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Supported {
		t.Error("supported=true for an agent that does not report in-flight detail")
	}
	if len(got.Inflight) != 0 {
		t.Errorf("want an empty list alongside supported=false, got %d items", len(got.Inflight))
	}
}

// A disconnected agent 404s; the console turns that into "no longer connected"
// rather than an empty panel.
func TestAgentInflightDisconnected404s(t *testing.T) {
	srv := consoleSrv(t)
	srv.AgentInflight = func(id string) ([]*drsyncpb.InflightItem, time.Time, bool, bool) {
		return nil, time.Time{}, false, false
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/v1/agents/gone/inflight", nil)
	r.SetPathValue("id", "gone")
	srv.getAgentInflight(w, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status %d, want 404 for a disconnected agent", w.Code)
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
	leased, err := srv.st.LeaseShards("agent-a", 1, time.Minute, 0)
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
