package agentsrv

import (
	"path/filepath"
	"testing"
	"time"

	"drsync/coordinator/internal/metrics"
	"drsync/coordinator/internal/model"
	"drsync/coordinator/internal/scheduler"
	"drsync/coordinator/internal/store"
	drsyncpb "drsync/proto/gen/drsyncpb"
)

const preserveSpecYAML = `
apiVersion: drsync/v1
kind: Job
metadata:
  name: preserve-e2e
spec:
  source: { path: /src }
  destination: { path: /dst }
  metadata:
    hardlinks: preserve
`

func newTestServerWithSpec(t *testing.T, name string, spec []byte) (*Server, *store.Store, *store.Job) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	met := metrics.New()
	sched := scheduler.New(st, met, 30*time.Second)
	job, err := st.CreateJob(name, spec, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SetJobState(job.ID, model.JobRunning); err != nil {
		t.Fatal(err)
	}
	srv := New(Config{HeartbeatInterval: 5 * time.Second, LeaseTTL: 30 * time.Second},
		st, sched, nil, met)
	return srv, st, job
}

// TestPlanLinkSightingsReportModeIsGated: a job left on the D3 default
// ("report") must not record any sightings — planLinkSightings returns nil so
// RecordSplit never touches link_groups for a job that never asked for
// preservation.
func TestPlanLinkSightingsReportModeIsGated(t *testing.T) {
	srv, st, job := newTestServerWithSpec(t, "report-e2e", []byte(specYAML))
	pass, err := st.CreatePass(job.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	root, err := st.InsertShards(pass.ID, 0, []store.NewShard{{Kind: model.KindDir}})
	if err != nil {
		t.Fatal(err)
	}

	wire := []*drsyncpb.ShardSplit_LinkSighting{
		{Dev: 1, Ino: 100, RelPath: []byte("a/one"), Nlink: 2, Size: 10, MtimeNs: 1},
	}
	sightings, maxScan, err := srv.planLinkSightings(root[0], wire)
	if err != nil {
		t.Fatal(err)
	}
	if sightings != nil || maxScan != 0 {
		t.Fatalf("planLinkSightings on a report-mode job = (%v, %d), want (nil, 0)", sightings, maxScan)
	}
}

// TestPlanLinkSightingsPreserveModeConverts: an opted-in job gets its wire
// sightings converted 1:1 into store.NewLinkSighting, plus the job's
// configured max-group-scan cap.
func TestPlanLinkSightingsPreserveModeConverts(t *testing.T) {
	srv, st, job := newTestServerWithSpec(t, "preserve-e2e", []byte(preserveSpecYAML))
	pass, err := st.CreatePass(job.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	root, err := st.InsertShards(pass.ID, 0, []store.NewShard{{Kind: model.KindDir}})
	if err != nil {
		t.Fatal(err)
	}

	wire := []*drsyncpb.ShardSplit_LinkSighting{
		{Dev: 7, Ino: 200, RelPath: []byte("a/one"), Nlink: 3, Size: 4096, MtimeNs: 123},
	}
	sightings, _, err := srv.planLinkSightings(root[0], wire)
	if err != nil {
		t.Fatal(err)
	}
	if len(sightings) != 1 {
		t.Fatalf("planLinkSightings returned %d sightings, want 1", len(sightings))
	}
	sg := sightings[0]
	if sg.Dev != 7 || sg.Ino != 200 || sg.RelPath != "a/one" || sg.Nlink != 3 ||
		sg.Size != 4096 || sg.MtimeNs != 123 {
		t.Errorf("converted sighting = %+v, want dev=7 ino=200 rel=a/one nlink=3 size=4096 mtime=123", sg)
	}
}

// TestPlanLinkSightingsSkipsSpecLookupWhenEmpty: an ordinary ShardSplit with
// no nlink>1 files (the overwhelming common case) must not pay a job-spec
// resolve at all — this is what keeps hardlink support a no-cost feature for
// jobs/shards that never trip nlink>1.
func TestPlanLinkSightingsSkipsSpecLookupWhenEmpty(t *testing.T) {
	srv, _, _ := newTestServerWithSpec(t, "empty-e2e", []byte(specYAML))
	// parentShardID 99999 does not exist: if planLinkSightings tried to
	// resolve it (ShardJobPass), this would fail loudly instead of returning
	// cleanly, proving the empty-slice fast path never touches the store.
	sightings, maxScan, err := srv.planLinkSightings(99999, nil)
	if err != nil {
		t.Fatalf("planLinkSightings with no sightings should never look anything up, got err: %v", err)
	}
	if sightings != nil || maxScan != 0 {
		t.Fatalf("planLinkSightings(no sightings) = (%v, %d), want (nil, 0)", sightings, maxScan)
	}
}
