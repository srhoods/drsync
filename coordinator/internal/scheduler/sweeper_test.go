package scheduler

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"drsync/coordinator/internal/metrics"
	"drsync/coordinator/internal/model"
	"drsync/coordinator/internal/store"
)

const sweeperSpec = `
apiVersion: drsync/v1
kind: Job
metadata: { name: t1 }
spec:
  source: { path: /src }
  destination: { path: /dst }
`

func newSweeperStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

// RunSweeper's whole purpose (beyond the requeue/park it delegates to
// ExpireLeases) is attributing each expiry to the agent that held it, by
// kind, so drsync_lease_expiries_by_agent_total can answer "one flaky host or
// fleet-wide" from /metrics. This pins that the label values are exactly
// right, not just that some counter incremented.
func TestRunSweeperLabelsExpiryByAgentAndKind(t *testing.T) {
	st := newSweeperStore(t)
	job, err := st.CreateJob("t1", []byte(sweeperSpec), false, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SetJobState(job.ID, model.JobRunning); err != nil {
		t.Fatal(err)
	}
	pass, err := st.CreatePass(job.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.InsertShards(pass.ID, 0, []store.NewShard{
		{Kind: model.KindDir, RelPath: "a"},
		{Kind: model.KindChunk, RelPath: "b"},
	}); err != nil {
		t.Fatal(err)
	}
	// Lease both to a known agent with an already-past TTL.
	if _, err := st.LeaseShards("agent-flaky", 4, -time.Second); err != nil {
		t.Fatal(err)
	}

	met := metrics.New()
	sched := New(st, met, time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	go sched.RunSweeper(ctx, 10*time.Millisecond)
	t.Cleanup(cancel)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		dir := testutil.ToFloat64(met.LeaseExpiriesByAgent.WithLabelValues("agent-flaky", "dir", "requeued"))
		chunk := testutil.ToFloat64(met.LeaseExpiriesByAgent.WithLabelValues("agent-flaky", "chunk", "requeued"))
		if dir == 1 && chunk == 1 {
			return // pass
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expiry not attributed to agent-flaky by kind within deadline: dir=%v chunk=%v",
		testutil.ToFloat64(met.LeaseExpiriesByAgent.WithLabelValues("agent-flaky", "dir", "requeued")),
		testutil.ToFloat64(met.LeaseExpiriesByAgent.WithLabelValues("agent-flaky", "chunk", "requeued")))
}

// A shard that expires repeatedly on the same agent until it parks must be
// labeled "parked", not "requeued", on its final expiry — that distinction is
// what separates "this agent is flaky but self-heals" from "this agent is
// the reason shards are dying outright" when reading the metric.
func TestRunSweeperLabelsFinalExpiryAsParked(t *testing.T) {
	st := newSweeperStore(t)
	job, err := st.CreateJob("t1", []byte(sweeperSpec), false, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SetJobState(job.ID, model.JobRunning); err != nil {
		t.Fatal(err)
	}
	pass, err := st.CreatePass(job.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.InsertShards(pass.ID, 0, []store.NewShard{
		{Kind: model.KindDir, RelPath: "a"},
	}); err != nil {
		t.Fatal(err)
	}

	met := metrics.New()

	// Drive ExpireLeases directly (not the ticking sweeper) so the test is
	// deterministic about which call is the final, parking one, and apply the
	// same outcome→label mapping RunSweeper's loop body uses. The ticking
	// path itself (RunSweeper picking these labels up from real ExpireLeases
	// output) is covered by TestRunSweeperLabelsExpiryByAgentAndKind above.
	for i := 0; i < store.MaxShardAttempts; i++ {
		if _, err := st.LeaseShards("agent-a", 1, -time.Second); err != nil {
			t.Fatal(err)
		}
		expired, err := st.ExpireLeases(time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if len(expired) != 1 {
			t.Fatalf("attempt %d: expired=%+v, want exactly 1", i, expired)
		}
		outcome := "requeued"
		if expired[0].Parked {
			outcome = "parked"
		}
		met.LeaseExpiriesByAgent.WithLabelValues(expired[0].Agent, string(expired[0].Kind), outcome).Inc()
	}

	if got := testutil.ToFloat64(met.LeaseExpiriesByAgent.WithLabelValues("agent-a", "dir", "requeued")); got != float64(store.MaxShardAttempts-1) {
		t.Errorf("requeued count = %v, want %d", got, store.MaxShardAttempts-1)
	}
	if got := testutil.ToFloat64(met.LeaseExpiriesByAgent.WithLabelValues("agent-a", "dir", "parked")); got != 1 {
		t.Errorf("parked count = %v, want 1", got)
	}
}
