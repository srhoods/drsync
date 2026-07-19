package store

import (
	"testing"
	"time"

	"drsync/coordinator/internal/model"
)

// One pathological directory fans out into a contiguous run of sibling shards.
// Because grants walk shards in id order, that run is handed out as a block and
// the fleet's whole prefetch window fills with a single directory — the rest of
// the tree, including the rest of the same job, then makes no progress until it
// drains. These tests pin the cap that prevents that, and the fallback that
// stops the cap from idling the fleet.

// seedTwoDirs builds a pass holding a big directory's entry-list slices plus a
// handful of shards from elsewhere in the tree, and returns their ids. The big
// directory's shards are inserted first so they sort earliest by id — the case
// that starves everything else.
func seedTwoDirs(t *testing.T, s *Store, nBig, nOther int) (bigParent int64, big, other []int64) {
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
	// The splitting shard, parent of every entry-list slice below it.
	parents, err := s.InsertShards(pass.ID, 0, []NewShard{{Kind: model.KindDir, RelPath: "bigdir"}})
	if err != nil {
		t.Fatal(err)
	}
	bigParent = parents[0]
	// The splitting shard has finished by the time its slices are queued —
	// otherwise it competes for a grant slot and skews the counts below.
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
	big, err = s.InsertShards(pass.ID, bigParent, mk(nBig, model.KindEntryList, "bigdir"))
	if err != nil {
		t.Fatal(err)
	}
	other, err = s.InsertShards(pass.ID, 0, mk(nOther, model.KindDir, "elsewhere"))
	if err != nil {
		t.Fatal(err)
	}
	return bigParent, big, other
}

func idSet(ids []int64) map[int64]bool {
	m := make(map[int64]bool, len(ids))
	for _, id := range ids {
		m[id] = true
	}
	return m
}

func countFrom(rows []*ShardRow, want map[int64]bool) int {
	n := 0
	for _, r := range rows {
		if want[r.ID] {
			n++
		}
	}
	return n
}

// With the cap in force, repeated grants must stop taking the big directory
// once it holds cap leases, and start handing out the rest of the tree —
// even though those shards sort later by id.
func TestLeaseShardsCapsOneDirectory(t *testing.T) {
	s := openTest(t)
	// The rest of the tree must hold more shards than the fleet can take, or
	// it runs dry and the never-strand fallback legitimately grants the big
	// directory past its cap — which would be measuring the wrong thing.
	_, big, other := seedTwoDirs(t, s, 200, 200)
	bigSet, otherSet := idSet(big), idSet(other)

	const cap = 10
	var gotBig, gotOther int
	// Several agents each taking a full window, as a fleet does at steady state.
	for i, agent := range []string{"a1", "a2", "a3", "a4"} {
		rows, err := s.LeaseShards(agent, 8, time.Minute, cap)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 8 {
			t.Fatalf("grant %d returned %d shards, want a full window of 8 "+
				"(the cap must redirect work, never withhold it)", i, len(rows))
		}
		gotBig += countFrom(rows, bigSet)
		gotOther += countFrom(rows, otherSet)
	}

	if gotBig > cap {
		t.Errorf("big directory holds %d leases, cap is %d", gotBig, cap)
	}
	if gotOther == 0 {
		t.Error("no shards from the rest of the tree were granted: one directory " +
			"still starves everything else")
	}
}

// The cap is a preference, not a quota. When the saturated directory is the
// only work left, grants must still be filled rather than idling the fleet.
func TestLeaseShardsNeverStrandsOnCap(t *testing.T) {
	s := openTest(t)
	_, big, _ := seedTwoDirs(t, s, 50, 0)

	const cap = 4
	total := 0
	for range 5 {
		rows, err := s.LeaseShards("solo", 8, time.Minute, cap)
		if err != nil {
			t.Fatal(err)
		}
		total += len(rows)
	}
	if total <= cap {
		t.Fatalf("granted only %d shards of %d queued: the cap stranded work that "+
			"nothing else could have taken", total, len(big))
	}
}

// A grant must never return the same shard twice — the uncapped fallback pass
// re-selects rows the capped tiers already took.
func TestLeaseShardsNoDuplicatesAcrossTiers(t *testing.T) {
	s := openTest(t)
	seedTwoDirs(t, s, 30, 0)

	rows, err := s.LeaseShards("a1", 16, time.Minute, 2)
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

// maxPerParent = 0 disables the cap: existing callers keep prior behaviour.
func TestLeaseShardsCapDisabled(t *testing.T) {
	s := openTest(t)
	_, big, _ := seedTwoDirs(t, s, 100, 0)

	rows, err := s.LeaseShards("a1", 20, time.Minute, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n := countFrom(rows, idSet(big)); n != 20 {
		t.Errorf("cap 0 granted %d of the big directory's shards, want the full 20", n)
	}
}
