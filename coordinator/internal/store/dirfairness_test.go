package store

import (
	"testing"
	"time"

	"drsync/coordinator/internal/model"
)

// An entry-list shard statx-hammers one pathological directory, so an agent must
// never hold more than maxEntrylistPerAgent (4) of them at once — the rest wait
// QUEUED for a free slot. Other shard kinds are never capped. These tests pin
// that per-agent cap and the backfill that keeps it from starving an agent of
// other work.

// seedEntrylist queues nEL entry-list shards (one pathological directory's
// slices) plus nOther dir shards from elsewhere, returning their ids. The
// entry-list shards are inserted first so they sort earliest by id — the case
// that would starve everything else without the cap.
func seedEntrylist(t *testing.T, s *Store, nEL, nOther int) (elIDs, otherIDs []int64) {
	t.Helper()
	job, err := s.CreateJob("t1", []byte(specYAML), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetJobState(job.ID, model.JobRunning); err != nil {
		t.Fatal(err)
	}
	pass, err := s.CreatePass(job.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	// The splitting shard, parent of every entry-list slice below it. Marked done
	// so it does not compete for a grant slot.
	parents, err := s.InsertShards(pass.ID, 0, []NewShard{{Kind: model.KindDir, RelPath: "bigdir"}})
	if err != nil {
		t.Fatal(err)
	}
	bigParent := parents[0]
	if _, err := s.db.Exec(`UPDATE shards SET state = ? WHERE id = ?`,
		string(model.ShardDone), bigParent); err != nil {
		t.Fatal(err)
	}

	mk := func(n int, kind model.ShardKind, rel string) []NewShard {
		v := make([]NewShard, n)
		for i := range v {
			v[i] = NewShard{Kind: kind, RelPath: rel}
		}
		return v
	}
	elIDs, err = s.InsertShards(pass.ID, bigParent, mk(nEL, model.KindEntryList, "bigdir"))
	if err != nil {
		t.Fatal(err)
	}
	otherIDs, err = s.InsertShards(pass.ID, 0, mk(nOther, model.KindDir, "elsewhere"))
	if err != nil {
		t.Fatal(err)
	}
	return elIDs, otherIDs
}

func idSet(ids []int64) map[int64]bool {
	m := make(map[int64]bool, len(ids))
	for _, id := range ids {
		m[id] = true
	}
	return m
}

func countKind(rows []*ShardRow, kind model.ShardKind) int {
	n := 0
	for _, r := range rows {
		if r.Kind == kind {
			n++
		}
	}
	return n
}

// A single grant must never hand one agent more than maxEntrylistPerAgent
// entry-list shards, and the grant must backfill with other work rather than
// coming back short when entry-list shards sit at the head of the queue.
func TestLeaseShardsCapsEntrylistPerAgentSingleGrant(t *testing.T) {
	s := openTest(t)
	seedEntrylist(t, s, 50, 50) // plenty of both kinds

	rows, err := s.LeaseShards("a1", 20, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got := countKind(rows, model.KindEntryList); got != maxEntrylistPerAgent {
		t.Errorf("single grant leased %d entry-list shards, want the cap of %d",
			got, maxEntrylistPerAgent)
	}
	if len(rows) != 20 {
		t.Errorf("grant returned %d shards, want a full window of 20 (the cap must "+
			"backfill with other work, not withhold the slots)", len(rows))
	}
}

// The cap is fleet-wide-per-agent: it counts entry-list shards the agent already
// holds LEASED, so repeated grants never push one agent past the cap even as it
// asks again.
func TestLeaseShardsCapsEntrylistAcrossGrants(t *testing.T) {
	s := openTest(t)
	// Only entry-list work: nothing to backfill with, so the cap is what bounds it.
	seedEntrylist(t, s, 50, 0)

	total := 0
	for range 5 {
		rows, err := s.LeaseShards("a1", 8, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		total += len(rows)
	}
	if total != maxEntrylistPerAgent {
		t.Errorf("agent accumulated %d entry-list leases across grants, want the "+
			"cap of %d (excess must stay QUEUED)", total, maxEntrylistPerAgent)
	}
}

// The cap is per-agent, so the fleet as a whole runs more than one agent's worth
// of entry-list shards in parallel — each agent gets its own budget.
func TestLeaseShardsEntrylistCapIsPerAgent(t *testing.T) {
	s := openTest(t)
	seedEntrylist(t, s, 50, 0)

	a, err := s.LeaseShards("a1", 8, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.LeaseShards("a2", 8, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got := countKind(a, model.KindEntryList); got != maxEntrylistPerAgent {
		t.Errorf("a1 got %d entry-list shards, want %d", got, maxEntrylistPerAgent)
	}
	if got := countKind(b, model.KindEntryList); got != maxEntrylistPerAgent {
		t.Errorf("a2 got %d entry-list shards, want %d", got, maxEntrylistPerAgent)
	}
}

// Entry-list backlog at the queue head must not starve an agent of other work:
// once the cap is hit the grant reaches past the entry-list run for dir shards.
func TestLeaseShardsEntrylistCapDoesNotStarveOtherWork(t *testing.T) {
	s := openTest(t)
	elIDs, otherIDs := seedEntrylist(t, s, 50, 50)

	rows, err := s.LeaseShards("a1", 20, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	el := 0
	for _, r := range rows {
		if idSet(elIDs)[r.ID] {
			el++
		}
	}
	other := 0
	for _, r := range rows {
		if idSet(otherIDs)[r.ID] {
			other++
		}
	}
	if el != maxEntrylistPerAgent {
		t.Errorf("leased %d entry-list shards, want the cap of %d", el, maxEntrylistPerAgent)
	}
	if other == 0 {
		t.Error("no dir shards granted: the entry-list backlog starved the rest of the tree")
	}
}

// Non-entry-list kinds are never capped: a grant may return far more than
// maxEntrylistPerAgent dir shards.
func TestLeaseShardsDoesNotCapNonEntrylist(t *testing.T) {
	s := openTest(t)
	seedEntrylist(t, s, 0, 100)

	rows, err := s.LeaseShards("a1", 40, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 40 {
		t.Errorf("dir-only grant returned %d shards, want the full 40 — non-entry-list "+
			"work must not be capped", len(rows))
	}
}

// A grant must never return the same shard twice — the backfill pass re-scans
// rows the capped tiers already took.
func TestLeaseShardsNoDuplicatesAcrossTiers(t *testing.T) {
	s := openTest(t)
	seedEntrylist(t, s, 30, 30)

	rows, err := s.LeaseShards("a1", 16, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[int64]bool{}
	for _, r := range rows {
		if seen[r.ID] {
			t.Fatalf("shard %d granted twice in one lease", r.ID)
		}
		seen[r.ID] = true
	}
}
