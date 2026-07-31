package scheduler

import (
	"path/filepath"
	"testing"
	"time"

	"drsync/coordinator/internal/metrics"
	"drsync/coordinator/internal/model"
	"drsync/coordinator/internal/store"
	drsyncpb "drsync/proto/gen/drsyncpb"
)

// TestGrantNeverExceedsAgentReceiveBuffer is the regression test for the
// silent-lease-loss bug: the agent's WorkGrant decoder has a fixed-size
// receive buffer (GRANT_MAX_ITEMS = 64, agent/src/msgs.h) and silently drops
// — frees, never calls lease_add, never queues, returns success regardless —
// any item past the 64th in one frame. Before grantMaxItems existed, Grant
// leased up to fairShare(credits, ...) shards with no reference to that
// ceiling: a single agent (fairShare's own cap only applies with >= 2
// agents) requesting more than 64 credits — which maybe_request_work sizes
// to (workers+copy_threads)*2, so any -w/-C combination above 32 combined —
// got every shard past the 64th leased (committed LEASED in the store) and
// then silently dropped on arrival. Nothing ever renews a lease the agent
// never knew it held, so those rows sat until the sweeper expired them —
// with the expired lease id present in no agent's own held-lease log
// anywhere, since lease_add was never called for it on any host. This is
// exactly the -w 14 -C 42 fleet report (workers+copy_threads=56, credits up
// to 112) and the observed "~32 combined thread count" threshold
// (grantMaxItems / the *2 multiplier = 32). See docs/DESIGN-agent.md §3.7.
func TestGrantNeverExceedsAgentReceiveBuffer(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

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
	// Queue far more than one agent could ever want in a single grant —
	// realistic for steady-state convergence, where the walk queue is deep.
	const nQueued = 500
	batch := make([]store.NewShard, nQueued)
	for i := range batch {
		batch[i] = store.NewShard{Kind: model.KindDir, RelPath: "d"}
	}
	if _, err := st.InsertShards(pass.ID, 0, batch); err != nil {
		t.Fatal(err)
	}

	sched := New(st, metrics.New(), time.Minute)
	// A single connected agent: fairShare's own cross-agent cap does not
	// apply (agents < 2 always returns credits unmodified), so without
	// grantMaxItems this would lease and grant all 112 requested items in
	// one WorkGrant — 48 more than the agent's fixed 64-item buffer holds.
	// -w 14 -C 42, work-stealing on: (14+42)*2 = 112, matching the reported
	// fleet exactly.
	const credits = 112
	grant, err := sched.Grant("agent-a", &drsyncpb.WorkRequest{ShardCredits: uint32(credits)})
	if err != nil {
		t.Fatal(err)
	}
	if len(grant.Items) > grantMaxItems {
		t.Fatalf("grant carries %d items, want <= %d (agent's GRANT_MAX_ITEMS) — "+
			"the excess would be silently dropped on the wire, leaving their "+
			"leases committed LEASED but never reported held by any agent",
			len(grant.Items), grantMaxItems)
	}
	if len(grant.Items) != grantMaxItems {
		t.Fatalf("grant carries %d items, want exactly %d (queue is far deeper "+
			"than the cap, so the cap — not queue depth — should be binding)",
			len(grant.Items), grantMaxItems)
	}

	// The shards NOT included in this grant must still be QUEUED, not LEASED:
	// leasing more than fits in the grant is exactly the bug (a lease with no
	// corresponding WorkItem the agent will ever see).
	counts, err := st.ShardStateCounts(pass.ID)
	if err != nil {
		t.Fatal(err)
	}
	if counts[model.ShardLeased] != int64(grantMaxItems) {
		t.Fatalf("LEASED count = %d, want exactly %d (one per granted item, no orphans)",
			counts[model.ShardLeased], grantMaxItems)
	}
	if counts[model.ShardQueued] != nQueued-int64(grantMaxItems) {
		t.Fatalf("QUEUED count = %d, want %d (everything not granted stays queued for the next request)",
			counts[model.ShardQueued], nQueued-int64(grantMaxItems))
	}
}

// A request for fewer credits than the cap must be unaffected — the cap only
// binds when it's the tightest constraint, never rationing a request that
// was already going to be small.
func TestGrantBelowCapIsUnaffected(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

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
	batch := make([]store.NewShard, 10)
	for i := range batch {
		batch[i] = store.NewShard{Kind: model.KindDir, RelPath: "d"}
	}
	if _, err := st.InsertShards(pass.ID, 0, batch); err != nil {
		t.Fatal(err)
	}

	sched := New(st, metrics.New(), time.Minute)
	grant, err := sched.Grant("agent-a", &drsyncpb.WorkRequest{ShardCredits: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(grant.Items) != 10 {
		t.Fatalf("grant carries %d items, want 10 (below the cap, unaffected)", len(grant.Items))
	}
}
