package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"drsync/coordinator/internal/model"
)

// TestConcurrentReadsDuringWrites exercises the read pool: monitoring reads run
// concurrently with the writer churning leases, with no errors or deadlock.
func TestConcurrentReadsDuringWrites(t *testing.T) {
	s := openTest(t)
	jobID, passID, _ := seed(t, s)
	batch := make([]NewShard, 200)
	for i := range batch {
		batch[i] = NewShard{Kind: model.KindDir, RelPath: fmt.Sprintf("d%03d", i)}
	}
	if _, err := s.InsertShards(passID, 0, batch); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	errCh := make(chan error, 64)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() { // writer: churn leases + expiry on the write connection
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if _, err := s.LeaseShards("w", 16, -time.Second); err != nil {
					errCh <- err
					return
				}
				if _, err := s.ExpireLeases(time.Now()); err != nil {
					errCh <- err
					return
				}
			}
		}
	}()

	readers := []func() error{
		func() error { _, e := s.ShardStateCounts(passID); return e },
		func() error { _, e := s.QueueSummary(); return e },
		func() error { _, e := s.ListJobs(); return e },
		func() error { _, e := s.ParkedShards(); return e },
		func() error { _, e := s.ListPasses(jobID); return e },
	}
	for _, r := range readers {
		wg.Add(1)
		go func(fn func() error) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					if err := fn(); err != nil {
						errCh <- err
						return
					}
				}
			}
		}(r)
	}

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent op failed: %v", err)
	}
}

// assertCountsConsistent verifies the trigger-maintained shard_counts rollup
// exactly matches an authoritative GROUP BY over the live shards table.
func assertCountsConsistent(t *testing.T, s *Store, stage string) {
	t.Helper()
	read := func(q string) map[string]int64 {
		rows, err := s.db.Query(q)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		m := map[string]int64{}
		for rows.Next() {
			var pass, n int64
			var kind, state string
			if err := rows.Scan(&pass, &kind, &state, &n); err != nil {
				t.Fatal(err)
			}
			m[fmt.Sprintf("%d/%s/%s", pass, kind, state)] = n
		}
		return m
	}
	want := read(`SELECT pass_id, kind, state, COUNT(*) FROM shards GROUP BY pass_id, kind, state`)
	got := read(`SELECT pass_id, kind, state, n FROM shard_counts WHERE n <> 0`)
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("%s: shard_counts drift\n  authoritative: %v\n  rollup:        %v", stage, want, got)
	}
}

// TestShardCountsRollupConsistent drives every shard transition type and checks
// the rollup never drifts from the truth.
func TestShardCountsRollupConsistent(t *testing.T) {
	s := openTest(t)
	jobID, passID, shardID := seed(t, s)
	assertCountsConsistent(t, s, "after seed (1 QUEUED)")

	// Split the root shard into children (parent -> SPLIT, +2 QUEUED children).
	if _, err := s.LeaseShards("agent-a", 1, time.Minute); err != nil {
		t.Fatal(err)
	}
	assertCountsConsistent(t, s, "after lease (1 LEASED)")
	if _, err := s.RecordSplit(shardID, 1, []NewShard{
		{Kind: model.KindDir, RelPath: "a"}, {Kind: model.KindEntryList, RelPath: "b"},
	}, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	assertCountsConsistent(t, s, "after split")

	// Lease + complete a child.
	rows, err := s.LeaseShards("agent-b", 1, time.Minute)
	if err != nil || len(rows) != 1 {
		t.Fatalf("lease child: %v %v", rows, err)
	}
	if err := s.CompleteShard(rows[0].ID, rows[0].LeaseID, nil); err != nil {
		t.Fatal(err)
	}
	assertCountsConsistent(t, s, "after complete child (1 DONE)")

	// Drive the other child to PARKED via repeated lease+expiry.
	for i := 0; i < MaxShardAttempts+1; i++ {
		s.LeaseShards("agent-c", 1, -time.Second)
		s.ExpireLeases(time.Now())
	}
	assertCountsConsistent(t, s, "after park")

	// Retry then drop the parked shard.
	parked, _ := s.ParkedShards()
	if len(parked) == 1 {
		if err := s.RetryParkedShard(parked[0].ID); err != nil {
			t.Fatal(err)
		}
		assertCountsConsistent(t, s, "after retry parked")
	}
	// Delete the whole job (bulk DELETE — per-row AFTER DELETE triggers).
	if err := s.SetJobState(jobID, model.JobCompleted); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DeleteJob("t1"); err != nil {
		t.Fatal(err)
	}
	assertCountsConsistent(t, s, "after job delete")
	_ = passID
}

// JournalTypeCounts sums per-type across every pass of a job, from the rollup
// SetJournalTypeCounts writes — the whole point being that /report (and the
// completion email) read this instead of scanning journal files on every
// request. This pins: cross-pass summing, and that a second SetJournalTypeCounts
// call for the same pass replaces rather than adds (the source is always a full
// recount of that pass's journal, never a delta).
func TestJournalTypeCountsSumsAcrossPassesAndReplaces(t *testing.T) {
	s := openTest(t)
	jobID, pass1ID, _ := seed(t, s)
	pass2, err := s.CreatePass(jobID, 2, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}

	if err := s.SetJournalTypeCounts(pass1ID, map[string]int64{"COPIED": 5, "ORPHAN": 2}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetJournalTypeCounts(pass2.ID, map[string]int64{"COPIED": 3, "ERROR": 1}); err != nil {
		t.Fatal(err)
	}
	got, total, err := s.JournalTypeCounts(jobID)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int64{"COPIED": 8, "ORPHAN": 2, "ERROR": 1}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("[%q] = %d, want %d (full: %+v)", k, got[k], v, got)
		}
	}
	if total != 11 { // COPIED 8 + ORPHAN 2 + ERROR 1
		t.Errorf("total = %d, want 11", total)
	}

	// Re-set pass 1 (as if its journal were rescanned): the new counts must
	// replace, not add to, the old ones.
	if err := s.SetJournalTypeCounts(pass1ID, map[string]int64{"COPIED": 5, "ORPHAN": 2, "ERROR": 4}); err != nil {
		t.Fatal(err)
	}
	got, total, err = s.JournalTypeCounts(jobID)
	if err != nil {
		t.Fatal(err)
	}
	if got["ERROR"] != 5 { // 4 (re-set pass 1) + 1 (pass 2, untouched)
		t.Errorf("ERROR after re-set = %d, want 5 (full: %+v)", got["ERROR"], got)
	}
	if total != 15 { // pass 1 COPIED 5 + ORPHAN 2 + ERROR 4, plus pass 2 COPIED 3 + ERROR 1
		t.Errorf("total after re-set = %d, want 15 (full: %+v)", total, got)
	}
}

// TestDeleteJobRemovesJournalTypeCounts pins the fix for the leak where
// journal_type_counts (written once per pass by SetJournalTypeCounts, at
// PassComplete) was never in DeleteJob's cleanup list: purging a job left its
// rows behind permanently, keyed by a pass_id no live pass would ever reuse.
func TestDeleteJobRemovesJournalTypeCounts(t *testing.T) {
	s := openTest(t)
	jobID, passID, _ := seed(t, s)
	if err := s.SetJournalTypeCounts(passID, map[string]int64{"COPIED": 5, "ORPHAN": 2}); err != nil {
		t.Fatal(err)
	}

	var before int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM journal_type_counts WHERE pass_id = ?`, passID).
		Scan(&before); err != nil {
		t.Fatal(err)
	}
	if before == 0 {
		t.Fatal("expected journal_type_counts rows before purge")
	}

	if err := s.SetJobState(jobID, model.JobCompleted); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DeleteJob("t1"); err != nil {
		t.Fatal(err)
	}

	var after int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM journal_type_counts WHERE pass_id = ?`, passID).
		Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != 0 {
		t.Errorf("journal_type_counts rows survived job purge: %d", after)
	}
}

// A zero count for a type must not be written as a row (so it doesn't show up
// as a spurious zero-count line in a caller that lists map keys) — mirrors the
// omission behavior the WebUI/email rendering already relies on.
func TestJournalTypeCountsOmitsZeroCounts(t *testing.T) {
	s := openTest(t)
	jobID, passID, _ := seed(t, s)
	if err := s.SetJournalTypeCounts(passID, map[string]int64{"COPIED": 5, "DIR_META": 0}); err != nil {
		t.Fatal(err)
	}
	got, _, err := s.JournalTypeCounts(jobID)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got["DIR_META"]; ok {
		t.Errorf("expected DIR_META omitted (zero count), got %+v", got)
	}
}

// TestReapDoneShards checks the core Shard Reaper contract: only DONE shards
// of the requested kinds are deleted, everything else (other kinds, other
// states, other passes) is untouched, and the shard_counts rollup stays exact
// afterward (the AFTER DELETE trigger, not special-cased reap logic, is what
// keeps it consistent).
func TestReapDoneShards(t *testing.T) {
	s := openTest(t)
	jobID, passID, _ := seed(t, s) // seed's root shard is QUEUED, kind=dir

	// A second pass's shards must never be touched by a reap scoped to passID.
	otherPass, err := s.CreatePass(jobID, 2, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	otherPassID := otherPass.ID

	mk := func(pid int64, kind model.ShardKind, n int) []int64 {
		batch := make([]NewShard, n)
		for i := range batch {
			batch[i] = NewShard{Kind: kind, RelPath: fmt.Sprintf("%s-%03d", kind, i)}
		}
		ids, err := s.InsertShards(pid, 0, batch)
		if err != nil {
			t.Fatal(err)
		}
		return ids
	}
	markDone := func(ids ...int64) {
		for _, id := range ids {
			if _, err := s.db.Exec(`UPDATE shards SET state = ? WHERE id = ?`,
				string(model.ShardDone), id); err != nil {
				t.Fatal(err)
			}
		}
	}

	dirDone := mk(passID, model.KindDir, 5)
	markDone(dirDone...)
	verifyDone := mk(passID, model.KindVerify, 3)
	markDone(verifyDone...)
	dirQueued := mk(passID, model.KindDir, 2) // left QUEUED: must survive
	otherDone := mk(otherPassID, model.KindDir, 4)
	markDone(otherDone...)
	assertCountsConsistent(t, s, "after seeding reap fixture")

	n, err := s.ReapDoneShards(passID, []model.ShardKind{model.KindDir})
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(dirDone)) {
		t.Fatalf("reaped %d, want %d", n, len(dirDone))
	}
	assertCountsConsistent(t, s, "after reaping dir")

	for _, id := range dirDone {
		var count int
		s.db.QueryRow(`SELECT COUNT(*) FROM shards WHERE id = ?`, id).Scan(&count)
		if count != 0 {
			t.Errorf("done dir shard %d survived reap", id)
		}
	}
	for _, id := range append(append(dirQueued, verifyDone...), otherDone...) {
		var count int
		s.db.QueryRow(`SELECT COUNT(*) FROM shards WHERE id = ?`, id).Scan(&count)
		if count != 1 {
			t.Errorf("shard %d should have survived reap (wrong kind/state/pass), got count=%d", id, count)
		}
	}

	// A second call against an already-reaped kind is a no-op, not an error.
	n, err = s.ReapDoneShards(passID, []model.ShardKind{model.KindDir})
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected nothing left to reap, got %d", n)
	}

	// Reaping with no kinds is a no-op guard, not "reap everything".
	n, err = s.ReapDoneShards(passID, nil)
	if err != nil || n != 0 {
		t.Fatalf("ReapDoneShards(nil kinds) = %d, %v; want 0, nil", n, err)
	}
}

// TestReapDoneShardsDeletesSplits pins the fix for the leak where a DONE
// dir/entry-list shard that was split gets its shards row reaped but its
// splits row (parent_shard_id keyed, no FK) survives forever — nothing else
// ever revisits it, since DeleteJob's own splits cleanup is scoped to shards
// still present at purge time. The reap must delete both in the same
// transaction.
func TestReapDoneShardsDeletesSplits(t *testing.T) {
	s := openTest(t)
	_, passID, shardID := seed(t, s) // seed's root shard is QUEUED, kind=dir

	if _, err := s.LeaseShards("agent-a", 1, time.Minute); err != nil {
		t.Fatal(err)
	}
	childIDs, err := s.RecordSplit(shardID, 1, []NewShard{
		{Kind: model.KindEntryList, RelPath: "a"}, {Kind: model.KindEntryList, RelPath: "b"},
	}, nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(childIDs) != 2 {
		t.Fatalf("split produced %d children, want 2", len(childIDs))
	}

	var splitsBefore int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM splits WHERE parent_shard_id = ?`, shardID).
		Scan(&splitsBefore); err != nil {
		t.Fatal(err)
	}
	if splitsBefore != 1 {
		t.Fatalf("splits rows before reap = %d, want 1", splitsBefore)
	}

	if _, err := s.db.Exec(`UPDATE shards SET state = ? WHERE id = ?`,
		string(model.ShardDone), shardID); err != nil {
		t.Fatal(err)
	}

	// A split row belonging to a shard that's still alive (not reaped) must
	// survive — otherwise this test can't tell "deleted because reaped" apart
	// from "deleted regardless of state".
	otherParent, err := s.InsertShards(passID, 0, []NewShard{{Kind: model.KindDir, RelPath: "other"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecordSplit(otherParent[0], 1, []NewShard{
		{Kind: model.KindEntryList, RelPath: "c"},
	}, nil, nil, 0); err != nil {
		t.Fatal(err)
	}

	n, err := s.ReapDoneShards(passID, []model.ShardKind{model.KindDir})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("reaped %d, want 1 (the DONE root shard)", n)
	}

	var splitsAfter int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM splits WHERE parent_shard_id = ?`, shardID).
		Scan(&splitsAfter); err != nil {
		t.Fatal(err)
	}
	if splitsAfter != 0 {
		t.Errorf("splits row for reaped parent %d survived, want deleted", shardID)
	}

	var otherSplits int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM splits WHERE parent_shard_id = ?`, otherParent[0]).
		Scan(&otherSplits); err != nil {
		t.Fatal(err)
	}
	if otherSplits != 1 {
		t.Errorf("splits row for non-reaped (still QUEUED) parent %d = %d, want 1 (untouched)", otherParent[0], otherSplits)
	}
}

// TestReapDoneShardsPreservesDoneCount guards the regression the four failing
// e2e tests (fanout_e2e, deep_e2e, scale_e2e, e2e) caught in CI: reaping must
// not make a completed pass's reported DONE total disappear along with the
// rows. ShardStateCounts is the API's pass-detail endpoint's only source for
// "how many shards of this pass finished", so it has to keep counting a
// shard that ran and succeeded, whether or not the reaper has since deleted
// its row.
func TestReapDoneShardsPreservesDoneCount(t *testing.T) {
	s := openTest(t)
	_, passID, _ := seed(t, s)

	batch := make([]NewShard, 30)
	for i := range batch {
		batch[i] = NewShard{Kind: model.KindDir, RelPath: fmt.Sprintf("d-%03d", i)}
	}
	ids, err := s.InsertShards(passID, 0, batch)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if _, err := s.db.Exec(`UPDATE shards SET state = ? WHERE id = ?`,
			string(model.ShardDone), id); err != nil {
			t.Fatal(err)
		}
	}
	// The pre-existing root shard from seed() is also QUEUED->left alone here;
	// only count the 30 we just marked DONE below, before reaping anything.
	before, err := s.ShardStateCounts(passID)
	if err != nil {
		t.Fatal(err)
	}
	if before[model.ShardDone] != 30 {
		t.Fatalf("DONE before reap = %d, want 30", before[model.ShardDone])
	}

	if n, err := s.ReapDoneShards(passID, []model.ShardKind{model.KindDir}); err != nil || n != 30 {
		t.Fatalf("reap = %d, %v; want 30, nil", n, err)
	}

	after, err := s.ShardStateCounts(passID)
	if err != nil {
		t.Fatal(err)
	}
	if after[model.ShardDone] != 30 {
		t.Fatalf("DONE after reap = %d, want 30 (unchanged despite rows being deleted)", after[model.ShardDone])
	}

	// A second, independent reap call (a later phase transition on the same
	// pass reaping a different kind) must ADD to shards_reaped, not overwrite
	// it — the running total across the pass's whole life must be additive.
	batch2 := make([]NewShard, 5)
	for i := range batch2 {
		batch2[i] = NewShard{Kind: model.KindVerify, RelPath: fmt.Sprintf("v-%03d", i)}
	}
	ids2, err := s.InsertShards(passID, 0, batch2)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids2 {
		if _, err := s.db.Exec(`UPDATE shards SET state = ? WHERE id = ?`,
			string(model.ShardDone), id); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := s.ReapDoneShards(passID, []model.ShardKind{model.KindVerify}); err != nil || n != 5 {
		t.Fatalf("second reap = %d, %v; want 5, nil", n, err)
	}
	final, err := s.ShardStateCounts(passID)
	if err != nil {
		t.Fatal(err)
	}
	if final[model.ShardDone] != 35 {
		t.Fatalf("DONE after second reap = %d, want 35 (30 + 5, additive across reaps)", final[model.ShardDone])
	}
}

// TestReapDoneShardsBatched checks that a backlog bigger than one batch is
// only partially drained per call (the batching that keeps a huge reap from
// holding the writer lock for one long delete) and a second call picks up
// where the first left off.
func TestReapDoneShardsBatched(t *testing.T) {
	s := openTest(t)
	_, passID, _ := seed(t, s)

	total := ReapBatchSize + 100
	batch := make([]NewShard, total)
	for i := range batch {
		batch[i] = NewShard{Kind: model.KindVerify, RelPath: fmt.Sprintf("v-%06d", i)}
	}
	if _, err := s.InsertShards(passID, 0, batch); err != nil {
		t.Fatal(err)
	}
	// One bulk UPDATE rather than per-row: this test only cares about count
	// behavior across the batch boundary, not which specific ids survive.
	if _, err := s.db.Exec(`UPDATE shards SET state = ? WHERE pass_id = ? AND kind = ?`,
		string(model.ShardDone), passID, string(model.KindVerify)); err != nil {
		t.Fatal(err)
	}

	n1, err := s.ReapDoneShards(passID, []model.ShardKind{model.KindVerify})
	if err != nil {
		t.Fatal(err)
	}
	if n1 != ReapBatchSize {
		t.Fatalf("first batch = %d, want exactly ReapBatchSize (%d)", n1, ReapBatchSize)
	}
	n2, err := s.ReapDoneShards(passID, []model.ShardKind{model.KindVerify})
	if err != nil {
		t.Fatal(err)
	}
	if n2 != int64(total)-n1 {
		t.Fatalf("second batch = %d, want %d (the remainder)", n2, int64(total)-n1)
	}
	assertCountsConsistent(t, s, "after batched reap")
}

// TestOpenSetsIncrementalVacuum checks the auto_vacuum migration Open performs:
// a pre-existing database (the SQLite default, auto_vacuum=NONE) is converted
// to INCREMENTAL on open, and reopening an already-converted database is a
// cheap no-op (no needless VACUUM on every restart).
func TestOpenSetsIncrementalVacuum(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var mode int
	if err := s.db.QueryRow(`PRAGMA auto_vacuum`).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if mode != 2 {
		t.Fatalf("auto_vacuum = %d, want 2 (INCREMENTAL)", mode)
	}
	s.Close()

	// Reopening must not fail and must keep the mode (VACUUM is one-time).
	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if err := s2.db.QueryRow(`PRAGMA auto_vacuum`).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if mode != 2 {
		t.Fatalf("auto_vacuum after reopen = %d, want 2 (INCREMENTAL)", mode)
	}
}

// TestIncrementalVacuumReclaimsSpace exercises IncrementalVacuum end to end:
// after reaping a large batch of DONE shards, running it should not error and
// the database's freelist should shrink (pages actually handed back).
func TestIncrementalVacuumReclaimsSpace(t *testing.T) {
	s := openTest(t)
	_, passID, _ := seed(t, s)

	batch := make([]NewShard, 5000)
	for i := range batch {
		batch[i] = NewShard{Kind: model.KindVerify, Payload: make([]byte, 512),
			RelPath: fmt.Sprintf("v-%06d", i)}
	}
	if _, err := s.InsertShards(passID, 0, batch); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE shards SET state = ? WHERE pass_id = ? AND kind = ?`,
		string(model.ShardDone), passID, string(model.KindVerify)); err != nil {
		t.Fatal(err)
	}
	for {
		n, err := s.ReapDoneShards(passID, []model.ShardKind{model.KindVerify})
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
	}

	var freelistBefore int
	if err := s.db.QueryRow(`PRAGMA freelist_count`).Scan(&freelistBefore); err != nil {
		t.Fatal(err)
	}
	if freelistBefore == 0 {
		t.Fatal("expected freed pages on the freelist after reaping; test fixture didn't produce any")
	}
	if err := s.IncrementalVacuum(freelistBefore); err != nil {
		t.Fatal(err)
	}
	var freelistAfter int
	if err := s.db.QueryRow(`PRAGMA freelist_count`).Scan(&freelistAfter); err != nil {
		t.Fatal(err)
	}
	if freelistAfter >= freelistBefore {
		t.Fatalf("freelist_count did not shrink: before=%d after=%d", freelistBefore, freelistAfter)
	}
}

// TestOpenSkipsRebuildWhenRollupPopulated guards the startup-scan fix: Open
// must not re-derive shard_counts from a full GROUP BY over shards when the
// rollup is already populated (the normal case on every restart after the
// first). Only a rollup that is empty while shards is not (a pre-upgrade or
// pre-existing DB) should trigger the rebuild.
func TestOpenSkipsRebuildWhenRollupPopulated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	_, passID, _ := seed(t, s)
	if _, err := s.InsertShards(passID, 0, []NewShard{{Kind: model.KindVerify}}); err != nil {
		t.Fatal(err)
	}
	assertCountsConsistent(t, s, "before corrupting rollup")

	// Sabotage the rollup directly so a real rebuild would visibly fix it; if
	// Open's skip-when-populated guard regresses to "always rebuild", this
	// corrupted row would disappear on reopen and the test would pass for the
	// wrong reason — so leave it in place and assert it survives instead.
	if _, err := s.db.Exec(`UPDATE shard_counts SET n = n + 1000 WHERE pass_id = ? LIMIT 1`, passID); err != nil {
		// LIMIT on UPDATE isn't supported by this SQLite build; fall back to
		// bumping every row for this pass, which is just as good a sentinel.
		if _, err := s.db.Exec(`UPDATE shard_counts SET n = n + 1000 WHERE pass_id = ?`, passID); err != nil {
			t.Fatal(err)
		}
	}
	s.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	var n int64
	if err := s2.db.QueryRow(`SELECT SUM(n) FROM shard_counts WHERE pass_id = ?`, passID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n < 1000 {
		t.Fatalf("rollup was rebuilt on reopen despite already being populated (sentinel corruption lost): sum=%d", n)
	}
}

const specYAML = `
apiVersion: drsync/v1
kind: Job
metadata:
  name: t1
spec:
  source: { path: /src }
  destination: { path: /dst }
`

func openTest(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func seed(t *testing.T, s *Store) (jobID, passID, shardID int64) {
	t.Helper()
	job, err := s.CreateJob("t1", []byte(specYAML), false, "")
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
	ids, err := s.InsertShards(pass.ID, 0, []NewShard{{Kind: model.KindDir, RelPath: ""}})
	if err != nil {
		t.Fatal(err)
	}
	return job.ID, pass.ID, ids[0]
}

// TestLeaseShardsIndexOrdered guards the grant hot path: both tiers of the
// LeaseShards query must walk shards_sched in order and stop at LIMIT, never
// sort the whole queued set. A leading computed ORDER BY term (e.g. soft
// affinity as `(attempt>0 AND lease_agent=?) ASC`) reintroduces a temp B-tree
// that pegs a core under the store lock at scale — this fails if that returns.
func TestLeaseShardsIndexOrdered(t *testing.T) {
	s := openTest(t)
	// Mirror the two queries inside LeaseShards (kept in sync by this guard).
	queries := []string{
		`SELECT s.id FROM shards s JOIN passes p ON p.id = s.pass_id JOIN jobs j ON j.id = p.job_id
		 WHERE s.state = ? AND j.state = ? AND NOT (s.attempt > 0 AND s.lease_agent = ?)
		 ORDER BY s.priority DESC, s.id LIMIT ?`,
		`SELECT s.id FROM shards s JOIN passes p ON p.id = s.pass_id JOIN jobs j ON j.id = p.job_id
		 WHERE s.state = ? AND j.state = ? AND s.attempt > 0 AND s.lease_agent = ?
		 ORDER BY s.priority DESC, s.id LIMIT ?`,
	}
	for i, q := range queries {
		rows, err := s.db.Query("EXPLAIN QUERY PLAN "+q, "QUEUED", "RUNNING", "a", 64)
		if err != nil {
			t.Fatal(err)
		}
		usedIndex := false
		for rows.Next() {
			var id, parent, notused int
			var detail string
			if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(detail, "TEMP B-TREE") {
				rows.Close()
				t.Fatalf("tier %d sorts the whole queued set: %q", i+1, detail)
			}
			if strings.Contains(detail, "shards_sched") {
				usedIndex = true
			}
		}
		rows.Close()
		if !usedIndex {
			t.Fatalf("tier %d does not use shards_sched", i+1)
		}
	}
}

// TestRenewLeasesByIDMatchedCount pins the diagnostic RenewLeasesByID exists
// for (docs/DESIGN-agent.md §3.6): matched must equal exactly how many of the
// requested ids matched a still-LEASED row this agent owns, not just "err ==
// nil" — a completed/reassigned/foreign-agent lease id must count as
// unmatched (0) individually, distinguishable from the all-matched case, so
// agentsrv's "heartbeat renewal did not match every held lease" warning has
// an honest signal to fire on.
func TestRenewLeasesByIDMatchedCount(t *testing.T) {
	s := openTest(t)
	_, passID, shardID := seed(t, s)

	rows, err := s.LeaseShards("agent-a", 1, time.Minute)
	if err != nil || len(rows) != 1 {
		t.Fatalf("lease: rows=%v err=%v", rows, err)
	}
	leaseID := rows[0].LeaseID

	// A lease id that was never granted (agent misreports, or a stale id from
	// a prior session) matches nothing.
	matched, err := s.RenewLeasesByID("agent-a", []int64{999999}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if matched != 0 {
		t.Fatalf("unknown lease id matched %d, want 0", matched)
	}

	// A real, currently-LEASED lease reported by the *wrong* agent matches
	// nothing — lease_agent must be checked, not just lease_id.
	matched, err = s.RenewLeasesByID("agent-b", []int64{leaseID}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if matched != 0 {
		t.Fatalf("lease renewed by the wrong agent matched %d, want 0", matched)
	}

	// The genuine owner renewing it matches exactly 1.
	matched, err = s.RenewLeasesByID("agent-a", []int64{leaseID}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if matched != 1 {
		t.Fatalf("genuine renewal matched %d, want 1", matched)
	}

	// Once the shard completes (state != LEASED), the same lease id no longer
	// matches — this is the "not a bug, the shard just finished" case the
	// agentsrv warning comment documents, not a data-loss signal on its own.
	if err := s.CompleteShard(shardID, leaseID, nil); err != nil {
		t.Fatal(err)
	}
	matched, err = s.RenewLeasesByID("agent-a", []int64{leaseID}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if matched != 0 {
		t.Fatalf("renewal after completion matched %d, want 0", matched)
	}
	_ = passID
}

// TestRenewLeasesByIDOnlyHeld is the end-of-scan stall regression: a heartbeat
// renews only the leases the agent still holds; a lease the agent no longer
// holds (lost grant / dropped result) is left to expire and requeue.
func TestRenewLeasesByIDOnlyHeld(t *testing.T) {
	s := openTest(t)
	_, passID, _ := seed(t, s)
	if _, err := s.InsertShards(passID, 0, []NewShard{{Kind: model.KindDir, RelPath: "b"}}); err != nil {
		t.Fatal(err)
	}
	// Lease both shards to agent-a with an already-past TTL (due to expire).
	rows, err := s.LeaseShards("agent-a", 4, -time.Second)
	if err != nil || len(rows) != 2 {
		t.Fatalf("want 2 leased, got %v err=%v", rows, err)
	}
	// The agent's heartbeat reports holding only the first lease.
	matched, err := s.RenewLeasesByID("agent-a", []int64{rows[0].LeaseID}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if matched != 1 {
		t.Fatalf("RenewLeasesByID matched %d, want 1", matched)
	}
	// Sweep: exactly the un-renewed lease requeues; the renewed one survives.
	expired, err := s.ExpireLeases(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(expired) != 1 || expired[0].Parked {
		t.Fatalf("want 1 requeued (the unheld lease), got %+v", expired)
	}
	// The detail must attribute the expiry to the agent that actually held it —
	// this is the whole point of returning it: lease_agent is gone once the
	// shard is re-granted, so this is the only place that identity survives.
	if expired[0].Agent != "agent-a" {
		t.Fatalf("expired lease attributed to %q, want agent-a", expired[0].Agent)
	}
	if expired[0].ShardID != rows[1].ID {
		t.Fatalf("expired shard = %d, want the unheld one (%d)", expired[0].ShardID, rows[1].ID)
	}
	// LeaseID must be captured before the requeue UPDATE clears it — the
	// diagnostic (docs/DESIGN-agent.md §3.6) depends on this surviving so an
	// expired lease can be grepped back through an agent's heartbeat logs.
	if expired[0].LeaseID != rows[1].LeaseID {
		t.Fatalf("expired lease_id = %d, want %d", expired[0].LeaseID, rows[1].LeaseID)
	}
	counts, _ := s.ShardStateCounts(passID)
	if counts[model.ShardLeased] != 1 || counts[model.ShardQueued] != 1 {
		t.Fatalf("counts=%+v, want 1 LEASED (renewed) + 1 QUEUED (expired)", counts)
	}
	// Empty held list must renew nothing (never fall back to renew-all).
	matched, err = s.RenewLeasesByID("agent-a", nil, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if matched != 0 {
		t.Fatalf("RenewLeasesByID with an empty held list matched %d, want 0", matched)
	}
}

func TestLeaseLifecycle(t *testing.T) {
	s := openTest(t)
	_, passID, shardID := seed(t, s)

	rows, err := s.LeaseShards("agent-a", 4, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ID != shardID {
		t.Fatalf("lease grant = %+v", rows)
	}
	lease := rows[0].LeaseID

	// Second grant: nothing queued.
	rows2, _ := s.LeaseShards("agent-b", 4, time.Minute)
	if len(rows2) != 0 {
		t.Fatalf("double-granted shard: %+v", rows2)
	}

	// Wrong lease id must be rejected.
	if err := s.CompleteShard(shardID, lease+1, nil); err != ErrLeaseMismatch {
		t.Fatalf("stale lease accepted: %v", err)
	}
	if err := s.CompleteShard(shardID, lease, nil); err != nil {
		t.Fatal(err)
	}
	counts, _ := s.ShardStateCounts(passID)
	if counts[model.ShardDone] != 1 {
		t.Fatalf("counts = %+v", counts)
	}
}

func TestLeaseExpiryRequeuesThenParks(t *testing.T) {
	s := openTest(t)
	seed(t, s)

	for i := 0; i < MaxShardAttempts; i++ {
		rows, err := s.LeaseShards("agent-a", 1, -time.Second) // already expired
		if err != nil {
			t.Fatal(err)
		}
		if i < MaxShardAttempts && len(rows) != 1 {
			// anti-affinity skips agent-a on retries; lease from another agent
			rows, err = s.LeaseShards("agent-b", 1, -time.Second)
			if err != nil || len(rows) != 1 {
				t.Fatalf("attempt %d: rows=%v err=%v", i, rows, err)
			}
		}
		expired, err := s.ExpireLeases(time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if i < MaxShardAttempts-1 && (len(expired) != 1 || expired[0].Parked) {
			t.Fatalf("attempt %d: expired=%+v, want 1 requeued", i, expired)
		}
		if i == MaxShardAttempts-1 && (len(expired) != 1 || !expired[0].Parked) {
			t.Fatalf("final attempt: expired=%+v, want 1 parked", expired)
		}
	}
}

// TestExpireLeasesUsesLeaseExpiryIndexAtScale pins the shards_lease_expiry
// index added alongside link_members_pending (same class of gap, found by
// auditing every high-write table's query patterns after the LINKFIX
// incident): ExpireLeases' select and both its follow-up UPDATEs filter on
// (state = 'leased' AND lease_expiry < ?), run fleet-wide on every sweep tick
// (leaseTTL/3, 10s by default). Without an index on lease_expiry,
// shards_sched's (state, priority DESC, id) can narrow to the leased set but
// then has to check every leased row's expiry individually — fine when few
// shards are leased at once, but a busy fleet can easily have tens of
// thousands concurrently leased (this is exactly the shape LINKFIX at scale
// produced before batching). Measured directly: ~295ms per sweep at 50K
// leased shards without this index, ~90µs with it.
//
// Asserts via EXPLAIN QUERY PLAN rather than a wall-clock bound — an earlier
// scale test in this package (TestMarkLinkMembersQueuedStaysFastAtScale)
// flaked on slower CI hardware with an absolute cutoff; asserting the query
// plan directly is immune to hardware speed entirely, and is exactly what
// distinguishes "seeks the index" from "scans and filters" regardless of how
// fast either happens to run on a given machine.
func TestExpireLeasesUsesLeaseExpiryIndexAtScale(t *testing.T) {
	if testing.Short() {
		t.Skip("scale test; skipped with -short")
	}
	s := openTest(t)
	_, passID, _ := seed(t, s)

	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.Prepare(`INSERT INTO shards
		(pass_id, kind, state, lease_agent, lease_expiry, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(time.Hour).UnixMilli()
	const leasedCount = 50_000
	for i := 0; i < leasedCount; i++ {
		if _, err := stmt.Exec(passID, "linkfix", string(model.ShardLeased), "agent-a", future, 0); err != nil {
			t.Fatal(err)
		}
	}
	// A handful of genuinely expired leases mixed in, so ExpireLeases has real
	// work to do and this test also pins correctness, not just the plan.
	past := time.Now().Add(-time.Minute).UnixMilli()
	const expiredCount = 3
	for i := 0; i < expiredCount; i++ {
		if _, err := stmt.Exec(passID, "linkfix", string(model.ShardLeased), "agent-b", past, 0); err != nil {
			t.Fatal(err)
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	var planUsesIndex bool
	prows, err := s.rdb.Query(`EXPLAIN QUERY PLAN SELECT s.id FROM shards s
		JOIN passes p ON p.id = s.pass_id JOIN jobs j ON j.id = p.job_id
		WHERE s.state = ? AND s.lease_expiry < ? ORDER BY s.id`,
		string(model.ShardLeased), time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	for prows.Next() {
		var a, b, c int
		var detail string
		if err := prows.Scan(&a, &b, &c, &detail); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(detail, "shards_lease_expiry") {
			planUsesIndex = true
		}
	}
	prows.Close()
	if !planUsesIndex {
		t.Fatal("ExpireLeases' select did not use shards_lease_expiry — " +
			"looks like a scan-based plan regressed back in")
	}

	expired, err := s.ExpireLeases(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(expired) != expiredCount {
		t.Fatalf("ExpireLeases returned %d, want %d", len(expired), expiredCount)
	}
}

// TestSchedulerCountsUsesJobsStateIndexAtScale pins the jobs_state index
// added alongside shards_lease_expiry's fix (same class of gap, found chasing
// a live "leases start expiring and shards park under sustained load, a
// drsyncd restart clears it" report at ~400M files walked in one job):
// SchedulerCounts (and QueueSummary) join shard_counts -> passes -> jobs
// filtered on job state, but shard_counts has no bound on its row count — it
// accumulates one row per (pass_id, kind, state) ever seen, across every job
// the coordinator has ever run, and is only cleared by an explicit DeleteJob
// purge. Without an index to seek jobs by state, the planner has nothing to
// drive the join from but a full scan of shard_counts, the largest table in
// it — and SchedulerCounts runs on every Grant call (refreshed every
// countsTTL, scheduler.go), fleet-wide, for the coordinator's entire uptime,
// so that cost is paid continuously and grows with cumulative historical
// shard volume, not the size of whatever job is currently running.
//
// Asserts via EXPLAIN QUERY PLAN, same reasoning as
// TestExpireLeasesUsesLeaseExpiryIndexAtScale: immune to hardware speed,
// where a wall-clock cutoff has flaked on slower CI before.
func TestSchedulerCountsUsesJobsStateIndexAtScale(t *testing.T) {
	if testing.Short() {
		t.Skip("scale test; skipped with -short")
	}
	s := openTest(t)

	// Simulate a long coordinator uptime: many terminal jobs' shard_counts
	// rows left behind (no auto-purge), each with rows across every
	// kind/state a pass can reach, plus one currently RUNNING job — the case
	// SchedulerCounts actually needs to answer fast.
	const terminalJobs = 2000
	kinds := []string{"dir", "entrylist", "chunk", "dirfix", "linkfix", "verify"}
	states := []string{"queued", "leased", "done", "parked"}

	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	jobStmt, err := tx.Prepare(`INSERT INTO jobs (name, spec_yaml, state, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		t.Fatal(err)
	}
	passStmt, err := tx.Prepare(`INSERT INTO passes (job_id, pass_no, state) VALUES (?, ?, ?)`)
	if err != nil {
		t.Fatal(err)
	}
	countStmt, err := tx.Prepare(`INSERT INTO shard_counts (pass_id, kind, state, n) VALUES (?, ?, ?, ?)`)
	if err != nil {
		t.Fatal(err)
	}
	now := nowMS()
	for i := 0; i < terminalJobs; i++ {
		res, err := jobStmt.Exec(fmt.Sprintf("done-job-%d", i), []byte(specYAML),
			string(model.JobCompleted), now, now)
		if err != nil {
			t.Fatal(err)
		}
		jobID, err := res.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		pres, err := passStmt.Exec(jobID, 1, string(model.PassComplete))
		if err != nil {
			t.Fatal(err)
		}
		passID, err := pres.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range kinds {
			for _, st := range states {
				// Reflects reality: reaping/counts trend to 0 but the rollup
				// row itself is never deleted short of a job purge.
				if _, err := countStmt.Exec(passID, k, st, 0); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	jobStmt.Close()
	passStmt.Close()
	countStmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// The one RUNNING job SchedulerCounts actually needs to total up. seed()
	// already inserts a root dir shard, which the shard_counts trigger counts
	// as (dir, queued, 1); top it up to a distinctive number so the assert
	// below can't pass by accident.
	_, passID, _ := seed(t, s)
	if _, err := s.db.Exec(`UPDATE shard_counts SET n = 7
		WHERE pass_id = ? AND kind = ? AND state = ?`,
		passID, string(model.KindDir), string(model.ShardQueued)); err != nil {
		t.Fatal(err)
	}

	var planUsesIndex, planScansShardCounts bool
	prows, err := s.rdb.Query(`EXPLAIN QUERY PLAN SELECT
		COALESCE(SUM(CASE WHEN sc.kind IN (?,?) AND sc.state IN (?,?) THEN sc.n ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN sc.state = ? THEN sc.n ELSE 0 END), 0)
		FROM shard_counts sc
		JOIN passes p ON p.id = sc.pass_id
		JOIN jobs   j ON j.id = p.job_id
		WHERE j.state = ?`,
		string(model.KindDir), string(model.KindEntryList),
		string(model.ShardQueued), string(model.ShardLeased),
		string(model.ShardQueued), string(model.JobRunning))
	if err != nil {
		t.Fatal(err)
	}
	for prows.Next() {
		var a, b, c int
		var detail string
		if err := prows.Scan(&a, &b, &c, &detail); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(detail, "jobs_state") {
			planUsesIndex = true
		}
		if strings.Contains(detail, "SCAN sc") {
			planScansShardCounts = true
		}
	}
	prows.Close()
	if !planUsesIndex {
		t.Fatal("SchedulerCounts did not use jobs_state — looks like a scan-based plan regressed back in")
	}
	if planScansShardCounts {
		t.Fatal("SchedulerCounts scans shard_counts despite jobs_state existing")
	}

	counts, err := s.SchedulerCounts()
	if err != nil {
		t.Fatal(err)
	}
	if counts.WalkPending != 7 || counts.Queued != 7 {
		t.Fatalf("SchedulerCounts = %+v, want WalkPending=7 Queued=7 (only the running job's row)", counts)
	}
}

// TestQueueSummaryContents pins QueueSummary's semantics across its rewrite
// from a single OR'd query to a UNION of two index-seekable legs (see the
// scale test below for why): every non-terminal pass's non-zero buckets, plus
// parked buckets even on a pass that has otherwise completed, each row
// reported exactly once — including a shard that is both (parked, on a
// non-terminal pass), which lands in both UNION legs and must be deduped, not
// doubled. Also pins the third leg: a non-terminal pass's shards_reaped total
// (accumulated by the eager SCANNING reap and phase-transition reaps, see
// passctrl.advance) surfaces as a synthetic (model.KindReaped, DONE) bucket —
// without it, a job's DONE total collapses toward zero as soon as its
// completed shards are reaped, well before the pass itself finishes.
func TestQueueSummaryContents(t *testing.T) {
	s := openTest(t)
	spec := func(name, dst string) []byte {
		return []byte("apiVersion: drsync/v1\nkind: Job\nmetadata: { name: " + name +
			" }\nspec:\n  source: { path: /src }\n  destination: { path: " + dst + " }\n")
	}

	// Pass A: SCANNING (non-terminal), one queued dir bucket and one parked
	// bucket — the parked one exercises the dedup case, present in both legs.
	jobA, err := s.CreateJob("job-a", spec("job-a", "/dst-a"), false, "")
	if err != nil {
		t.Fatal(err)
	}
	passA, err := s.CreatePass(jobA.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.InsertShards(passA.ID, 0, []NewShard{{Kind: model.KindDir, RelPath: "a"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO shard_counts (pass_id, kind, state, n) VALUES (?, ?, ?, ?)`,
		passA.ID, string(model.KindVerify), string(model.ShardParked), 3); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE passes SET shards_reaped = ? WHERE id = ?`, 42, passA.ID); err != nil {
		t.Fatal(err)
	}

	// Pass B: COMPLETE, with a leftover parked bucket (the case the original
	// OR's second arm exists for) and a non-parked bucket that must NOT show.
	jobB, err := s.CreateJob("job-b", spec("job-b", "/dst-b"), false, "")
	if err != nil {
		t.Fatal(err)
	}
	passB, err := s.CreatePass(jobB.ID, 1, model.PassComplete)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO shard_counts (pass_id, kind, state, n) VALUES (?, ?, ?, ?)`,
		passB.ID, string(model.KindVerify), string(model.ShardParked), 2); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO shard_counts (pass_id, kind, state, n) VALUES (?, ?, ?, ?)`,
		passB.ID, string(model.KindVerify), string(model.ShardDone), 100); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE passes SET shards_reaped = ? WHERE id = ?`, 999, passB.ID); err != nil {
		t.Fatal(err)
	}

	rows, err := s.QueueSummary()
	if err != nil {
		t.Fatal(err)
	}

	byBucket := map[[4]string]int64{}
	for _, r := range rows {
		key := [4]string{r.Job, fmt.Sprint(r.PassNo), string(r.Kind), string(r.State)}
		if _, dup := byBucket[key]; dup {
			t.Fatalf("bucket %v reported more than once: %+v", key, rows)
		}
		byBucket[key] = r.Count
	}

	want := map[[4]string]int64{
		{"job-a", "1", string(model.KindDir), string(model.ShardQueued)}:    1,
		{"job-a", "1", string(model.KindVerify), string(model.ShardParked)}: 3,
		{"job-a", "1", string(model.KindReaped), string(model.ShardDone)}:   42,
		{"job-b", "1", string(model.KindVerify), string(model.ShardParked)}: 2,
	}
	if len(byBucket) != len(want) {
		t.Fatalf("QueueSummary returned %d buckets, want %d: got %+v", len(byBucket), len(want), rows)
	}
	for key, n := range want {
		got, ok := byBucket[key]
		if !ok {
			t.Errorf("missing bucket %v", key)
			continue
		}
		if got != n {
			t.Errorf("bucket %v = %d, want %d", key, got, n)
		}
	}
	if _, present := byBucket[[4]string{"job-b", "1", string(model.KindVerify), string(model.ShardDone)}]; present {
		t.Fatalf("job-b's DONE bucket on a COMPLETE pass should not appear: %+v", rows)
	}
	if _, present := byBucket[[4]string{"job-b", "1", string(model.KindReaped), string(model.ShardDone)}]; present {
		t.Fatalf("job-b's shards_reaped total should not appear once its pass is COMPLETE: %+v", rows)
	}
}

// TestQueueSummaryUsesIndexesAtScale pins the passes_state/shard_counts_state
// indexes: QueueSummary's original single query OR'd `p.state != COMPLETE`
// with `sc.state = 'parked'`, but SQLite cannot serve a `!=` with an index
// seek no matter what exists on passes.state, so that form always fell back
// to a full scan of shard_counts regardless of the jobs_state index added for
// SchedulerCounts (same underlying growth problem — shard_counts has no
// bound on its own row count, see that test — different join leg). Recast as
// a UNION of two positive membership tests, each leg is independently
// seekable. Found alongside SchedulerCounts' fix, same live "leases start
// expiring and shards park, a drsyncd restart clears it" report.
func TestQueueSummaryUsesIndexesAtScale(t *testing.T) {
	if testing.Short() {
		t.Skip("scale test; skipped with -short")
	}
	s := openTest(t)

	const terminalJobs = 2000
	kinds := []string{"dir", "entrylist", "chunk", "dirfix", "linkfix", "verify"}
	states := []string{"queued", "leased", "done", "parked"}

	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	jobStmt, err := tx.Prepare(`INSERT INTO jobs (name, spec_yaml, state, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		t.Fatal(err)
	}
	passStmt, err := tx.Prepare(`INSERT INTO passes (job_id, pass_no, state) VALUES (?, ?, ?)`)
	if err != nil {
		t.Fatal(err)
	}
	countStmt, err := tx.Prepare(`INSERT INTO shard_counts (pass_id, kind, state, n) VALUES (?, ?, ?, ?)`)
	if err != nil {
		t.Fatal(err)
	}
	now := nowMS()
	for i := 0; i < terminalJobs; i++ {
		res, err := jobStmt.Exec(fmt.Sprintf("done-job-%d", i), []byte(specYAML),
			string(model.JobCompleted), now, now)
		if err != nil {
			t.Fatal(err)
		}
		jobID, err := res.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		pres, err := passStmt.Exec(jobID, 1, string(model.PassComplete))
		if err != nil {
			t.Fatal(err)
		}
		passID, err := pres.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range kinds {
			for _, st := range states {
				if _, err := countStmt.Exec(passID, k, st, 0); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	jobStmt.Close()
	passStmt.Close()
	countStmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// One running job's queued bucket (non-terminal leg) and one parked
	// bucket on an otherwise-complete pass (parked leg).
	_, passID, _ := seed(t, s)
	if _, err := s.db.Exec(`UPDATE shard_counts SET n = 5
		WHERE pass_id = ? AND kind = ? AND state = ?`,
		passID, string(model.KindDir), string(model.ShardQueued)); err != nil {
		t.Fatal(err)
	}

	nonTerminal := model.NonTerminalPassStates()
	ph := strings.TrimSuffix(strings.Repeat("?,", len(nonTerminal)), ",")
	nonTerminalArgs := make([]any, 0, len(nonTerminal))
	for _, st := range nonTerminal {
		nonTerminalArgs = append(nonTerminalArgs, string(st))
	}
	args := make([]any, 0, 2*len(nonTerminal)+3)
	args = append(args, nonTerminalArgs...)
	args = append(args, string(model.ShardParked))
	args = append(args, string(model.KindReaped), string(model.ShardDone))
	args = append(args, nonTerminalArgs...)

	prows, err := s.rdb.Query(`EXPLAIN QUERY PLAN SELECT j.name, p.pass_no, sc.kind, sc.state, sc.n
		FROM shard_counts sc
		JOIN passes p ON p.id = sc.pass_id
		JOIN jobs   j ON j.id = p.job_id
		WHERE sc.n > 0 AND p.state IN (`+ph+`)
		UNION
		SELECT j.name, p.pass_no, sc.kind, sc.state, sc.n
		FROM shard_counts sc
		JOIN passes p ON p.id = sc.pass_id
		JOIN jobs   j ON j.id = p.job_id
		WHERE sc.n > 0 AND sc.state = ?
		UNION
		SELECT j.name, p.pass_no, ?, ?, p.shards_reaped
		FROM passes p
		JOIN jobs j ON j.id = p.job_id
		WHERE p.shards_reaped > 0 AND p.state IN (`+ph+`)
		ORDER BY 1, 2`, args...)
	if err != nil {
		t.Fatal(err)
	}
	var planScans bool
	var sawPassesState, sawShardCountsState bool
	for prows.Next() {
		var a, b, c int
		var detail string
		if err := prows.Scan(&a, &b, &c, &detail); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(detail, "SCAN sc") || strings.Contains(detail, "SCAN p") {
			planScans = true
		}
		if strings.Contains(detail, "passes_state") {
			sawPassesState = true
		}
		if strings.Contains(detail, "shard_counts_state") {
			sawShardCountsState = true
		}
	}
	prows.Close()
	if planScans {
		t.Fatal("QueueSummary scans shard_counts or passes despite passes_state/shard_counts_state existing")
	}
	if !sawPassesState || !sawShardCountsState {
		t.Fatalf("QueueSummary did not seek both indexes: passes_state=%v shard_counts_state=%v",
			sawPassesState, sawShardCountsState)
	}

	rows, err := s.QueueSummary()
	if err != nil {
		t.Fatal(err)
	}
	var sawQueued bool
	for _, r := range rows {
		if r.Kind == model.KindDir && r.State == model.ShardQueued && r.Count == 5 {
			sawQueued = true
		}
	}
	if !sawQueued {
		t.Fatalf("QueueSummary missing the running job's queued dir bucket: %+v", rows)
	}
}

func TestSplitIdempotency(t *testing.T) {
	s := openTest(t)
	_, passID, shardID := seed(t, s)
	if _, err := s.LeaseShards("agent-a", 1, time.Minute); err != nil {
		t.Fatal(err)
	}

	subs := []NewShard{
		{Kind: model.KindDir, RelPath: "a"},
		{Kind: model.KindDir, RelPath: "b"},
	}
	ids1, err := s.RecordSplit(shardID, 7, subs, nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	ids2, err := s.RecordSplit(shardID, 7, subs, nil, nil, 0) // retransmit
	if err != nil {
		t.Fatal(err)
	}
	if len(ids1) != 2 || len(ids2) != 2 || ids1[0] != ids2[0] || ids1[1] != ids2[1] {
		t.Fatalf("split not idempotent: %v vs %v", ids1, ids2)
	}
	counts, _ := s.ShardStateCounts(passID)
	if counts[model.ShardQueued] != 2 {
		t.Fatalf("counts = %+v (want 2 queued children)", counts)
	}
}

// TestRecordSplitMissingParentIsIdempotent covers the case a live incident
// surfaced: an agent's outbox can replay a SHARD_SPLIT for a parent that has
// since been reaped (or whose job was dropped) across a reconnect. Before
// this fix RecordSplit's "SELECT pass_id FROM shards" returned sql.ErrNoRows
// and the error propagated all the way to dispatch(), which tore the session
// down — and since the outbox replays the same frame on every subsequent
// reconnect, the agent never made it past this split, spamming "dispatch
// failed" and "coordinator sent protocol error" forever. The fix: treat a
// missing parent as "already processed" (nil, nil) rather than fatal.
func TestRecordSplitMissingParentIsIdempotent(t *testing.T) {
	s := openTest(t)
	_, _, shardID := seed(t, s)
	if _, err := s.LeaseShards("agent-a", 1, time.Minute); err != nil {
		t.Fatal(err)
	}

	// Simulate the parent shard having been reaped: delete its row directly
	// (mirrors what the DONE-shard reaper does) without a matching splits row.
	if _, err := s.db.Exec(`DELETE FROM shards WHERE id = ?`, shardID); err != nil {
		t.Fatal(err)
	}

	subs := []NewShard{{Kind: model.KindDir, RelPath: "a"}}
	ids, err := s.RecordSplit(shardID, 9, subs, nil, nil, 0)
	if err != nil {
		t.Fatalf("RecordSplit on reaped parent returned an error, want nil: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("RecordSplit on reaped parent returned %d ids, want none inserted", len(ids))
	}
}

// TestRunningSinceStampedOnEntryToRunning locks down SetJobState's
// running_since_ms behavior: stamped on the initial READY->RUNNING
// transition, left untouched across a pause (so a paused job still reports
// when it last started running), and restamped with a later time on resume.
func TestRunningSinceStampedOnEntryToRunning(t *testing.T) {
	s := openTest(t)
	job, err := s.CreateJob("run-since", []byte(specYAML), false, "")
	if err != nil {
		t.Fatal(err)
	}
	if job.RunningSinceMs.Valid {
		t.Fatalf("new job already has running_since_ms = %v, want unset", job.RunningSinceMs)
	}

	if err := s.SetJobState(job.ID, model.JobRunning); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetJobByID(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !got.RunningSinceMs.Valid {
		t.Fatal("running_since_ms not set after READY->RUNNING")
	}
	firstStart := got.RunningSinceMs.Int64

	if err := s.SetJobState(job.ID, model.JobPaused); err != nil {
		t.Fatal(err)
	}
	got, err = s.GetJobByID(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.RunningSinceMs.Int64 != firstStart {
		t.Fatalf("running_since_ms changed on pause: %d, want unchanged %d", got.RunningSinceMs.Int64, firstStart)
	}

	time.Sleep(2 * time.Millisecond)
	if err := s.SetJobState(job.ID, model.JobRunning); err != nil {
		t.Fatal(err)
	}
	got, err = s.GetJobByID(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.RunningSinceMs.Int64 <= firstStart {
		t.Fatalf("running_since_ms not advanced on resume: %d, want > %d", got.RunningSinceMs.Int64, firstStart)
	}

	if err := s.SetJobState(job.ID, model.JobCompleted); err != nil {
		t.Fatal(err)
	}
	got, err = s.GetJobByID(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !got.RunningSinceMs.Valid {
		t.Fatal("running_since_ms cleared on completion, want it preserved for duration display")
	}
}

func TestPausedJobNotGranted(t *testing.T) {
	s := openTest(t)
	jobID, _, _ := seed(t, s)
	if err := s.SetJobState(jobID, model.JobPaused); err != nil {
		t.Fatal(err)
	}
	rows, err := s.LeaseShards("agent-a", 4, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("paused job granted work: %+v", rows)
	}
}

// A draining agent hands back a shard it had queued but never started. It must
// return to the queue with no error and no attempt penalty, and become grantable
// to another agent — the same reassignment path a normal grant uses.
func TestReleaseShardRequeuesWithoutPenalty(t *testing.T) {
	s := openTest(t)
	_, _, shardID := seed(t, s)
	rows, err := s.LeaseShards("drainer", 4, time.Minute)
	if err != nil || len(rows) != 1 {
		t.Fatalf("lease = %+v, err=%v", rows, err)
	}
	leaseID := rows[0].LeaseID
	if rows[0].Attempt != 1 {
		t.Fatalf("attempt after first grant = %d, want 1", rows[0].Attempt)
	}
	if err := s.ReleaseShard(shardID, leaseID); err != nil {
		t.Fatalf("ReleaseShard: %v", err)
	}
	// A stale release (wrong lease) is rejected, like any stale transition.
	if err := s.ReleaseShard(shardID, leaseID); !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("second release err = %v, want ErrLeaseMismatch", err)
	}
	// Released shard carries no error and is grantable again; attempt only ever
	// advances on grant, so the next grant makes it 2 (the release did not bump).
	rows, err = s.LeaseShards("other", 4, time.Minute)
	if err != nil || len(rows) != 1 || rows[0].ID != shardID {
		t.Fatalf("re-grant after release = %+v, err=%v", rows, err)
	}
	if rows[0].Attempt != 2 {
		t.Errorf("attempt after release+regrant = %d, want 2 (release must not penalise)", rows[0].Attempt)
	}
}

func TestDisabledAgentNotGranted(t *testing.T) {
	s := openTest(t)
	_, _, shardID := seed(t, s)
	if err := s.UpsertAgent("agent-a", "host", "v1", 1); err != nil {
		t.Fatal(err)
	}
	if err := s.SetAgentEnabled("agent-a", false); err != nil {
		t.Fatal(err)
	}
	// Disabled agent: no grant, even with queued work.
	rows, err := s.LeaseShards("agent-a", 4, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("disabled agent granted work: %+v", rows)
	}
	// Another, enabled agent still gets the shard.
	rows, err = s.LeaseShards("agent-b", 4, time.Minute)
	if err != nil || len(rows) != 1 || rows[0].ID != shardID {
		t.Fatalf("enabled agent grant = %+v, err=%v", rows, err)
	}
	// Re-enable: eligible again (seed a fresh queued shard first).
	if err := s.SetAgentEnabled("agent-a", true); err != nil {
		t.Fatal(err)
	}
	if l := s.enabledFlag(t, "agent-a"); !l {
		t.Fatal("agent-a still disabled after enable")
	}
	// Unknown agent id is a not-found error.
	if err := s.SetAgentEnabled("ghost", false); err != sql.ErrNoRows {
		t.Fatalf("SetAgentEnabled(unknown) = %v, want sql.ErrNoRows", err)
	}
}

// TestSoleAgentRequeuedShardNotStranded is the end-of-job stall regression.
// With only one agent, a shard requeued after that agent's lease expired must
// still be re-granted to it (soft affinity, not a hard bar) and ultimately
// park — never sit QUEUED forever with no eligible taker.
func TestSoleAgentRequeuedShardNotStranded(t *testing.T) {
	s := openTest(t)
	_, passID, _ := seed(t, s)

	granted := 0
	for i := 0; i < MaxShardAttempts+2; i++ {
		rows, err := s.LeaseShards("agent-a", 1, -time.Second) // sole agent, expired ttl
		if err != nil {
			t.Fatal(err)
		}
		granted += len(rows)
		if _, err := s.ExpireLeases(time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if granted < 2 {
		t.Fatalf("sole agent re-granted its own requeued shard only %d time(s); it was stranded", granted)
	}
	counts, _ := s.ShardStateCounts(passID)
	if counts[model.ShardQueued] != 0 {
		t.Fatalf("shard still QUEUED (counts=%+v): the pass would stall forever", counts)
	}
	if counts[model.ShardParked] != 1 {
		t.Fatalf("shard did not reach PARKED (counts=%+v)", counts)
	}
}

// TestSoftAffinityPrefersFreshWork proves the avoidance is still a preference:
// given fresh work, an agent takes that before a shard it last failed on.
func TestSoftAffinityPrefersFreshWork(t *testing.T) {
	s := openTest(t)
	_, _, shard1 := seed(t, s)
	ids, err := s.InsertShards(mustPass(t, s, shard1), 0, []NewShard{{Kind: model.KindDir, RelPath: "b"}})
	if err != nil {
		t.Fatal(err)
	}
	shard2 := ids[0]

	// Poison shard1 for agent-a: lease then expire it.
	if _, err := s.LeaseShards("agent-a", 1, -time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ExpireLeases(time.Now()); err != nil {
		t.Fatal(err)
	}

	// agent-a, one credit: must get the fresh shard2, not its poisoned shard1.
	rows, err := s.LeaseShards("agent-a", 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ID != shard2 {
		t.Fatalf("soft affinity: agent-a got %+v, want fresh shard %d", rows, shard2)
	}
}

func TestRetryAndDropParkedShard(t *testing.T) {
	s := openTest(t)
	_, passID, shardID := seed(t, s)
	parkShard(t, s) // drive the shard to PARKED

	// Unknown / non-parked id is a typed error.
	if err := s.RetryParkedShard(shardID + 999); err != ErrNotParked {
		t.Fatalf("RetryParkedShard(unknown) = %v, want ErrNotParked", err)
	}

	// Retry returns it to the queue, reset for a fresh attempt on any agent.
	if err := s.RetryParkedShard(shardID); err != nil {
		t.Fatal(err)
	}
	counts, _ := s.ShardStateCounts(passID)
	if counts[model.ShardParked] != 0 || counts[model.ShardQueued] != 1 {
		t.Fatalf("after retry counts=%+v, want 1 queued 0 parked", counts)
	}
	rows, err := s.LeaseShards("agent-a", 1, time.Minute)
	if err != nil || len(rows) != 1 || rows[0].Attempt != 1 {
		t.Fatalf("retried shard not grantable fresh: rows=%+v err=%v", rows, err)
	}

	// Park it again, then drop it: the row is gone, pass unblocked.
	if err := s.ExpireLeasesForce(t); err != nil { // helper drives to park quickly
		t.Fatal(err)
	}
	parked, _ := s.ParkedShards()
	if len(parked) != 1 {
		t.Fatalf("want 1 parked before drop, got %d", len(parked))
	}
	if err := s.DropParkedShard(parked[0].ID); err != nil {
		t.Fatal(err)
	}
	if err := s.DropParkedShard(parked[0].ID); err != ErrNotParked {
		t.Fatalf("second drop = %v, want ErrNotParked", err)
	}
	counts, _ = s.ShardStateCounts(passID)
	if counts[model.ShardParked] != 0 || counts[model.ShardQueued] != 0 {
		t.Fatalf("after drop counts=%+v, want empty", counts)
	}
}

func TestRetryAndDropParkedByJob(t *testing.T) {
	s := openTest(t)
	_, _, _ = seed(t, s)
	parkShard(t, s)

	n, err := s.RetryParkedByJob("t1")
	if err != nil || n != 1 {
		t.Fatalf("RetryParkedByJob = %d, %v; want 1", n, err)
	}
	if n, _ := s.RetryParkedByJob("nope"); n != 0 {
		t.Fatalf("RetryParkedByJob(unknown job) = %d, want 0", n)
	}

	parkShard(t, s) // park it again
	n, err = s.DropParkedByJob("t1")
	if err != nil || n != 1 {
		t.Fatalf("DropParkedByJob = %d, %v; want 1", n, err)
	}
}

// mustPass resolves the pass id owning a shard (test helper).
func mustPass(t *testing.T, s *Store, shardID int64) int64 {
	t.Helper()
	p, err := s.PassOfShard(shardID)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// parkShard drives the seeded job's single shard to PARKED via repeated
// lease+expiry up to the attempt ceiling.
func parkShard(t *testing.T, s *Store) {
	t.Helper()
	for i := 0; i < MaxShardAttempts+1; i++ {
		if _, err := s.LeaseShards("agent-a", 1, -time.Second); err != nil {
			t.Fatal(err)
		}
		if _, err := s.ExpireLeases(time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	parked, _ := s.ParkedShards()
	if len(parked) != 1 {
		t.Fatalf("parkShard: want 1 parked, got %d", len(parked))
	}
}

// ExpireLeasesForce leases then expires the seeded shard until it parks (test
// helper used to re-park after a retry).
func (s *Store) ExpireLeasesForce(t *testing.T) error {
	t.Helper()
	for i := 0; i < MaxShardAttempts+2; i++ {
		if _, err := s.LeaseShards("agent-a", 1, -time.Second); err != nil {
			return err
		}
		// A far-future "now" expires any outstanding lease, including one still
		// held under a normal ttl from a prior grant.
		if _, err := s.ExpireLeases(time.Now().Add(time.Hour)); err != nil {
			return err
		}
	}
	return nil
}

// enabledFlag reads an agent's enabled bit via ListAgents (test helper).
func (s *Store) enabledFlag(t *testing.T, id string) bool {
	t.Helper()
	agents, err := s.ListAgents()
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range agents {
		if a.ID == id {
			return a.Enabled
		}
	}
	t.Fatalf("agent %q not found", id)
	return false
}

// TestSchedulerCounts covers the inputs to the fan-out and fair-share
// decisions: walk shards must count queued AND leased (an agent busy on a
// shard is not starved), while Queued spans every kind, since the fair-share
// divisor must not go blind during a walk-free phase such as verify.
func TestSchedulerCounts(t *testing.T) {
	s := openTest(t)
	_, passID, rootID := seed(t, s)

	c, err := s.SchedulerCounts()
	if err != nil {
		t.Fatal(err)
	}
	if c.WalkPending != 1 || c.Queued != 1 {
		t.Fatalf("seeded root: got %+v, want WalkPending=1 Queued=1", c)
	}

	// Leasing the root must not make the fleet look starved.
	if _, err := s.LeaseShards("agent-a", 1, time.Minute); err != nil {
		t.Fatal(err)
	}
	if c, err = s.SchedulerCounts(); err != nil {
		t.Fatal(err)
	}
	if c.WalkPending != 1 {
		t.Errorf("leased root: WalkPending = %d, want 1 (leased still counts)", c.WalkPending)
	}
	if c.Queued != 0 {
		t.Errorf("leased root: Queued = %d, want 0 (nothing grantable)", c.Queued)
	}

	// Children pushed back by a split.
	if _, err := s.InsertShards(passID, rootID, []NewShard{
		{Kind: model.KindDir, RelPath: "a"},
		{Kind: model.KindEntryList, RelPath: "b"},
	}); err != nil {
		t.Fatal(err)
	}
	// Verify tasks are not walk work, but are grantable.
	if _, err := s.InsertShards(passID, 0, []NewShard{
		{Kind: model.KindVerify, RelPath: "v1"},
		{Kind: model.KindVerify, RelPath: "v2"},
	}); err != nil {
		t.Fatal(err)
	}
	if c, err = s.SchedulerCounts(); err != nil {
		t.Fatal(err)
	}
	if c.WalkPending != 3 {
		t.Errorf("WalkPending = %d, want 3 (leased root + dir + entrylist)", c.WalkPending)
	}
	if c.Queued != 4 {
		t.Errorf("Queued = %d, want 4 (dir + entrylist + 2 verify)", c.Queued)
	}
}

// Only a RUNNING job's shards are schedulable, so a paused job's backlog must
// not suppress fan-out for the job that is actually running.
func TestSchedulerCountsIgnoresNonRunningJobs(t *testing.T) {
	s := openTest(t)
	jobID, _, _ := seed(t, s)
	if err := s.SetJobState(jobID, model.JobPaused); err != nil {
		t.Fatal(err)
	}
	c, err := s.SchedulerCounts()
	if err != nil {
		t.Fatal(err)
	}
	if c.WalkPending != 0 || c.Queued != 0 {
		t.Fatalf("paused job: got %+v, want zeroes", c)
	}
}

func TestCountSchedulableAgents(t *testing.T) {
	s := openTest(t)

	// An empty fleet reports 1, never 0: the count is a divisor.
	n, err := s.CountSchedulableAgents()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("empty fleet: got %d, want 1", n)
	}

	for _, id := range []string{"a", "b", "c"} {
		if err := s.UpsertAgent(id, id+".host", "v1", 1); err != nil {
			t.Fatal(err)
		}
	}
	if n, err = s.CountSchedulableAgents(); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("got %d, want 3", n)
	}

	// A drained agent finishes its leases but takes no new work, so it must
	// not inflate the divisor.
	if err := s.SetAgentEnabled("b", false); err != nil {
		t.Fatal(err)
	}
	// Nor may a disconnected one.
	if err := s.SetAgentState("c", "disconnected"); err != nil {
		t.Fatal(err)
	}
	if n, err = s.CountSchedulableAgents(); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("after disable+disconnect: got %d, want 1", n)
	}
}

// TestTargetedShardOnlyLeasedByTarget covers probe targeting: a shard pinned to
// one agent is leasable only by that agent, while untargeted shards stay open to
// anyone.
func TestTargetedShardOnlyLeasedByTarget(t *testing.T) {
	s := openTest(t)
	job, err := s.CreateJob("t1", []byte(specYAML), false, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetJobState(job.ID, model.JobRunning); err != nil {
		t.Fatal(err)
	}
	pass, err := s.CreatePass(job.ID, 1, model.PassProbing)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.InsertShards(pass.ID, 0, []NewShard{
		{Kind: model.KindProbe, TargetAgent: "agent-a"},
		{Kind: model.KindProbe, TargetAgent: "agent-b"},
	}); err != nil {
		t.Fatal(err)
	}

	// agent-b may not take agent-a's probe: it gets only its own.
	rows, err := s.LeaseShards("agent-b", 8, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Kind != model.KindProbe {
		t.Fatalf("agent-b lease = %+v, want its single probe", rows)
	}
	// agent-a takes the remaining probe (its own).
	rows, err = s.LeaseShards("agent-a", 8, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("agent-a lease = %+v, want its single probe", rows)
	}
}

// TestPruneStaleProbes drops probes pinned to departed agents so the probing
// phase is not stalled by a shard no live agent can lease.
func TestPruneStaleProbes(t *testing.T) {
	s := openTest(t)
	job, err := s.CreateJob("t1", []byte(specYAML), false, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetJobState(job.ID, model.JobRunning); err != nil {
		t.Fatal(err)
	}
	pass, err := s.CreatePass(job.ID, 1, model.PassProbing)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.InsertShards(pass.ID, 0, []NewShard{
		{Kind: model.KindProbe, TargetAgent: "live"},
		{Kind: model.KindProbe, TargetAgent: "gone"},
	}); err != nil {
		t.Fatal(err)
	}
	// Only "live" is connected; "gone" was never registered (or disconnected).
	if err := s.UpsertAgent("live", "h", "v1", 1); err != nil {
		t.Fatal(err)
	}

	n, err := s.PruneStaleProbes(pass.ID)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("pruned %d, want 1 (the departed agent's probe)", n)
	}
	// The rollup the phase machine reads must reflect the delete.
	counts, err := s.ShardStateCounts(pass.ID)
	if err != nil {
		t.Fatal(err)
	}
	if counts[model.ShardQueued] != 1 {
		t.Fatalf("queued after prune = %d, want 1", counts[model.ShardQueued])
	}
	// The live agent's probe survived and is still leasable by it.
	rows, err := s.LeaseShards("live", 8, time.Minute)
	if err != nil || len(rows) != 1 {
		t.Fatalf("live probe lease = %+v, err=%v", rows, err)
	}
}

// TestSchedulableAgents lists connected+enabled agents (probe targets).
func TestSchedulableAgents(t *testing.T) {
	s := openTest(t)
	for _, id := range []string{"a", "b", "c"} {
		if err := s.UpsertAgent(id, "h", "v1", 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.SetAgentEnabled("b", false); err != nil { // disabled: excluded
		t.Fatal(err)
	}
	if err := s.SetAgentState("c", "disconnected"); err != nil { // gone: excluded
		t.Fatal(err)
	}
	got, err := s.SchedulableAgents()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []string{"a"}) {
		t.Fatalf("schedulable = %v, want [a]", got)
	}
}

// TestCreateJobDestinationConflictIsAtomic: the overlap check runs under the
// same lock as the insert, so concurrent submits of overlapping destinations
// cannot both land. Checking in the caller (as the API originally did) left a
// window where each request read the table before either row was visible, and
// both were created — exactly the two-jobs-one-tree state the check exists to
// prevent.
func TestCreateJobDestinationConflictIsAtomic(t *testing.T) {
	s := openTest(t)
	spec := func(name, dst string) []byte {
		return []byte("apiVersion: drsync/v1\nkind: Job\nmetadata: { name: " + name +
			" }\nspec:\n  source: { path: /src }\n  destination: { path: " + dst + " }\n")
	}

	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start // release them together to maximise overlap
			name := fmt.Sprintf("j%d", i)
			_, errs[i] = s.CreateJob(name, spec(name, "/dst/home"), false, "")
		}(i)
	}
	close(start)
	wg.Wait()

	created := 0
	for i, err := range errs {
		var dc *DestinationConflictError
		switch {
		case err == nil:
			created++
		case errors.As(err, &dc):
		default:
			t.Fatalf("job %d: unexpected error %v", i, err)
		}
	}
	if created != 1 {
		t.Fatalf("%d concurrent submits of the same destination created %d jobs, want 1", n, created)
	}
}

// TestShardKindsPresent locks down the query ShardKindsPresent.EffectiveState
// relies on: a kind with only DONE shards still counts as present (the pass
// isn't "back out" of a phase once its shards finish), and a kind with zero
// rows in shard_counts is absent.
func TestShardKindsPresent(t *testing.T) {
	s := openTest(t)
	_, passID, shardID := seed(t, s) // one KindDir shard, QUEUED

	kinds, err := s.ShardKindsPresent(passID)
	if err != nil {
		t.Fatal(err)
	}
	if !kinds[model.KindDir] || kinds[model.KindDirfix] {
		t.Fatalf("kinds = %+v, want {dir: true}", kinds)
	}

	rows, err := s.LeaseShards("agent-a", 4, time.Minute)
	if err != nil || len(rows) != 1 {
		t.Fatalf("lease: rows=%v err=%v", rows, err)
	}
	if err := s.CompleteShard(shardID, rows[0].LeaseID, nil); err != nil {
		t.Fatal(err)
	}
	kinds, err = s.ShardKindsPresent(passID)
	if err != nil {
		t.Fatal(err)
	}
	if !kinds[model.KindDir] {
		t.Fatalf("kinds = %+v, want dir still present once DONE", kinds)
	}
}

// TestJobSummariesReportsLiveShardKindOverStaleState reproduces the reported
// bug directly: passctrl.advance seeds a phase's shards before flipping
// passes.state to that phase (deliberately, so a tick can't see the new
// phase with an empty queue and skip it), so there is a real window where
// DIRFIX shards are already queued/leased while the stored pass state still
// reads SCANNING. JobSummaries — the WebUI job list / API source — must
// report the phase the live queue proves, not the stale stored one.
func TestJobSummariesReportsLiveShardKindOverStaleState(t *testing.T) {
	s := openTest(t)
	jobID, passID, _ := seed(t, s) // pass created in PassScanning

	if _, err := s.InsertShards(passID, 0,
		[]NewShard{{Kind: model.KindDirfix, Payload: []byte("x")}}); err != nil {
		t.Fatal(err)
	}
	// passes.state deliberately NOT flipped yet — this is the window advance()
	// leaves open between seeding and SetPassState.

	sums, err := s.JobSummaries()
	if err != nil {
		t.Fatal(err)
	}
	var got *JobSummary
	for _, v := range sums {
		if v.ID == jobID {
			got = v
		}
	}
	if got == nil {
		t.Fatal("job missing from JobSummaries")
	}
	if got.LatestPassState != model.PassDirfix {
		t.Fatalf("LatestPassState = %s, want DIRFIX (stored state is still SCANNING, "+
			"but dirfix shards are already queued)", got.LatestPassState)
	}
}

// TestWALAutocheckpointDisabled pins Open's wal_autocheckpoint(0): SQLite's
// own auto-checkpoint trigger fires inline on whichever write crosses its WAL
// page threshold, under s.mu, blocking every agent's grant/renew/complete
// call for however long copying WAL frames back to the main db file takes.
// RunWALCheckpoint (driven from main.go on a fixed interval) is meant to be
// the only thing that ever checkpoints this database; if this pragma silently
// stopped applying (a DSN typo, a modernc.org/sqlite version that parses
// _pragma= differently), the auto-checkpoint would start firing again with no
// other signal until it showed up as the exact "degrades under sustained
// load, a restart clears it" symptom this exists to prevent.
func TestWALAutocheckpointDisabled(t *testing.T) {
	s := openTest(t)
	var n int
	if err := s.db.QueryRow(`PRAGMA wal_autocheckpoint`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("wal_autocheckpoint = %d, want 0 (disabled — RunWALCheckpoint should be the only checkpointer)", n)
	}
}

// TestWALCheckpointRuns exercises WALCheckpoint end-to-end against a real
// write connection: writes enough rows to guarantee the WAL is non-empty,
// then asserts the checkpoint pragma itself runs without error. It cannot
// assert wal_pages/checkpointed_pages are non-zero without depending on
// modernc.org/sqlite's exact WAL page-count behavior at a given row count —
// what matters here is that WALCheckpoint's query/scan shape (PRAGMA
// wal_checkpoint(PASSIVE) returning busy, log, checkpointed) is correct
// against the real driver, not the exact numbers it reports.
func TestWALCheckpointRuns(t *testing.T) {
	s := openTest(t)
	_, passID, _ := seed(t, s)
	batch := make([]NewShard, 500)
	for i := range batch {
		batch[i] = NewShard{Kind: model.KindDir, RelPath: fmt.Sprintf("d%03d", i)}
	}
	if _, err := s.InsertShards(passID, 0, batch); err != nil {
		t.Fatal(err)
	}
	if err := s.WALCheckpoint(); err != nil {
		t.Fatalf("WALCheckpoint: %v", err)
	}
}

// TestWALCheckpointShrinksWALFile pins the fix for a live "the WAL file is
// now the same size as the main db file" report. PRAGMA wal_checkpoint
// (PASSIVE) — what WALCheckpoint used to run — fully copies WAL content back
// to the main db file (correctness is never at risk either way) but leaves
// the WAL file itself on disk at whatever size it grew to; only TRUNCATE (now
// what WALCheckpoint runs) actually shrinks the file. This writes enough rows
// to force real WAL growth, confirms the file is non-trivially sized before
// checkpointing, then asserts it shrinks back down after.
func TestWALCheckpointShrinksWALFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })

	_, passID, _ := seed(t, s)
	blob := make([]byte, 512)
	batch := make([]NewShard, 3000)
	for i := range batch {
		batch[i] = NewShard{Kind: model.KindDir, RelPath: fmt.Sprintf("d%05d", i), Payload: blob}
	}
	if _, err := s.InsertShards(passID, 0, batch); err != nil {
		t.Fatal(err)
	}

	walPath := path + "-wal"
	before, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat WAL before checkpoint: %v", err)
	}
	const minWAL = 64 * 1024 // small margin above SQLite's default page/frame overhead
	if before.Size() < minWAL {
		t.Fatalf("WAL only %d bytes before checkpoint, too small to exercise the shrink — "+
			"increase the write volume above", before.Size())
	}

	if err := s.WALCheckpoint(); err != nil {
		t.Fatalf("WALCheckpoint: %v", err)
	}

	after, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat WAL after checkpoint: %v", err)
	}
	if after.Size() >= before.Size() {
		t.Fatalf("WAL file did not shrink: %d bytes before, %d after — "+
			"checkpoint must use TRUNCATE mode, not PASSIVE, to reclaim WAL file space",
			before.Size(), after.Size())
	}
}

// TestRunWALCheckpointStopsOnContextCancel: the background loop exits
// promptly when its context is cancelled, same select{ctx.Done(): return}
// shape as RunIncrementalVacuum/scheduler.RunSweeper — a goroutine that
// outlives its caller would leak across tests (and, in production, across a
// graceful shutdown that starts a fresh Store on restart).
func TestRunWALCheckpointStopsOnContextCancel(t *testing.T) {
	s := openTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.RunWALCheckpoint(ctx, time.Millisecond)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunWALCheckpoint did not stop after context cancellation")
	}
}
