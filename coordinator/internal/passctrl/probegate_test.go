package passctrl

import (
	"testing"
	"time"

	"drsync/coordinator/internal/model"
)

// TestSeedPassProbesConnectedAgents: with a fleet present, pass 1 opens in
// PROBING with one probe pinned to each agent — the root walk shard is withheld.
func TestSeedPassProbesConnectedAgents(t *testing.T) {
	c := newController(t)
	if _, err := c.st.CreateJob("t1", []byte(baseSpec), false); err != nil {
		t.Fatal(err)
	}
	if err := c.st.UpsertAgent("a1", "h", "v1", 1); err != nil {
		t.Fatal(err)
	}
	if err := c.st.UpsertAgent("a2", "h", "v1", 1); err != nil {
		t.Fatal(err)
	}
	if err := c.StartJob("t1"); err != nil {
		t.Fatal(err)
	}

	job, _ := c.st.GetJob("t1")
	pass, err := c.st.ActivePass(job.ID)
	if err != nil || pass == nil {
		t.Fatalf("active pass: %v %v", pass, err)
	}
	if pass.State != model.PassProbing {
		t.Fatalf("pass state = %s, want PROBING", pass.State)
	}
	counts, err := c.st.ShardStateCounts(pass.ID)
	if err != nil {
		t.Fatal(err)
	}
	if counts[model.ShardQueued] != 2 {
		t.Fatalf("queued probes = %d, want 2 (one per agent)", counts[model.ShardQueued])
	}
	// No root walk shard until the probes pass.
	rows, err := c.st.LeaseShards("a1", 8, time.Minute, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if r.Kind != model.KindProbe {
			t.Fatalf("a1 leased a %s before probes passed", r.Kind)
		}
	}
}

// TestProbeGateTransitionsToScanning: once every probe reports OK the phase
// machine inserts the root shard and advances PROBING → SCANNING.
func TestProbeGateTransitionsToScanning(t *testing.T) {
	c := newController(t)
	if _, err := c.st.CreateJob("t1", []byte(baseSpec), false); err != nil {
		t.Fatal(err)
	}
	if err := c.st.UpsertAgent("a1", "h", "v1", 1); err != nil {
		t.Fatal(err)
	}
	if err := c.StartJob("t1"); err != nil {
		t.Fatal(err)
	}
	job, _ := c.st.GetJob("t1")

	// Agent leases and completes its probe OK.
	rows, err := c.st.LeaseShards("a1", 8, time.Minute, 0)
	if err != nil || len(rows) != 1 {
		t.Fatalf("probe lease = %+v err=%v", rows, err)
	}
	if err := c.st.CompleteShard(rows[0].ID, rows[0].LeaseID, nil); err != nil {
		t.Fatal(err)
	}

	// One advance tick: probes drained OK → root shard seeded, phase SCANNING.
	if err := c.advance(job); err != nil {
		t.Fatal(err)
	}
	pass, _ := c.st.ActivePass(job.ID)
	if pass.State != model.PassScanning {
		t.Fatalf("pass state = %s, want SCANNING", pass.State)
	}
	// The withheld root walk shard is now queued and grantable.
	rows, err = c.st.LeaseShards("a1", 8, time.Minute, 0)
	if err != nil || len(rows) != 1 || rows[0].Kind != model.KindDir || rows[0].RelPath != "" {
		t.Fatalf("post-gate lease = %+v err=%v, want the root dir shard", rows, err)
	}
}

// TestSeedPassNoAgentsFallsBackToScanning: with an empty fleet there is nobody
// to probe, so the pass opens directly in SCANNING with the root shard.
func TestSeedPassNoAgentsFallsBackToScanning(t *testing.T) {
	c := newController(t)
	if _, err := c.st.CreateJob("t1", []byte(baseSpec), false); err != nil {
		t.Fatal(err)
	}
	if err := c.StartJob("t1"); err != nil {
		t.Fatal(err)
	}
	job, _ := c.st.GetJob("t1")
	pass, err := c.st.ActivePass(job.ID)
	if err != nil || pass == nil {
		t.Fatalf("active pass: %v %v", pass, err)
	}
	if pass.State != model.PassScanning {
		t.Fatalf("pass state = %s, want SCANNING (no agents to probe)", pass.State)
	}
}
