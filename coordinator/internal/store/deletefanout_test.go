package store

import (
	"testing"
	"time"

	"drsync/coordinator/internal/model"
)

// deleteRemainderShard is a NewShard shaped like onShardSplit builds for a
// ShardSplit.DeleteRemainder — Kind KindDelete, RelPath set to the directory
// being emptied. This is the signal CompleteDeleteRemainder's caller
// (agentsrv.onShardResult) uses to tell a split-produced delete-remainder
// shard apart from an ordinary top-level one (RelPath empty).
func deleteRemainderShard(dirRel string) NewShard {
	return NewShard{Kind: model.KindDelete, RelPath: dirRel}
}

func cleanupShard(dirRel string) NewShard {
	return NewShard{Kind: model.KindDelete}
}

// TestDeleteGroupNestedChildBlocksParentClose is the nested fan-out
// regression: a batch shard under "parent" ships a name ("parent/child")
// that turns out to itself be pathological (agent/src/delete.c
// remove_object checks EVERY directory a removal touches, not just the one
// named in a shard's paths[]) and gets streamed off as its own delete_groups
// row instead of being removed inline. The batch shard that shipped it
// still reports itself done as soon as the hand-off completes — that is
// correct, the batch has no more work — but "parent/child" is NOT yet gone,
// so "parent" must not close (its cleanup rmdir would race an ENOTEMPTY
// directory, or worse, close successfully while the real removal is still
// in flight) until "parent/child"'s own group closes too.
func TestDeleteGroupNestedChildBlocksParentClose(t *testing.T) {
	s := openTest(t)
	_, passID, shardID := seed(t, s)
	if _, err := s.LeaseShards("agent-a", 1, time.Minute); err != nil {
		t.Fatal(err)
	}

	const parent = "parent"
	const child = "parent/child"

	// parent's only (and therefore final) batch: one remainder shard whose
	// job is to remove the single name "child" — which, in the real agent,
	// turns out to be itself pathological, so it never actually gets
	// removed by this shard; it gets streamed off as its own group instead
	// (modeled below by directly recording a split for "child" whose parent
	// shard is this same remainder shard).
	parentIDs, err := s.RecordSplit(shardID, 1, []NewShard{deleteRemainderShard(parent)}, nil, nil, 0,
		[]DeleteGroupTotal{{DirRel: parent, LastBatch: true, BuildCleanup: cleanupShard}})
	if err != nil {
		t.Fatal(err)
	}
	if len(parentIDs) != 1 {
		t.Fatalf("RecordSplit produced %d ids, want 1", len(parentIDs))
	}
	parentBatchID := parentIDs[0]
	parentLeased, err := s.LeaseShards("agent-a", 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(parentLeased) != 1 || parentLeased[0].ID != parentBatchID {
		t.Fatalf("lease mismatch: leased=%v parentBatchID=%d", parentLeased, parentBatchID)
	}
	parentBatchLeaseID := parentLeased[0].LeaseID

	// The parent batch shard, while running, discovers "child" is itself
	// pathological and ships it as a nested split — its own parent_shard_id
	// is parentBatchID (the remainder shard that found it), not shardID (the
	// original walk/delete shard) — same as agent/src/delete.c's
	// remove_object calling stream_delete_split for a nested directory.
	childIDs, err := s.RecordSplit(parentBatchID, 1, []NewShard{deleteRemainderShard(child)}, nil, nil, 0,
		[]DeleteGroupTotal{{DirRel: child, LastBatch: true, BuildCleanup: cleanupShard}})
	if err != nil {
		t.Fatal(err)
	}
	if len(childIDs) != 1 {
		t.Fatalf("RecordSplit produced %d ids, want 1", len(childIDs))
	}

	// The parent batch shard itself now completes (it was leased BEFORE the
	// nested split above, so it's not re-leased here) — it did its job
	// (handed "child" off), so it reports done. This must NOT close parent's
	// group: parent/child's own group is still open (pending_children=1).
	if err := s.CompleteDeleteRemainder(parentBatchID, parentBatchLeaseID, passID, parent, nil,
		true, cleanupShard(parent), cleanupShard, nil, nil); err != nil {
		t.Fatal(err)
	}
	counts, err := s.ShardStateCounts(passID)
	if err != nil {
		t.Fatal(err)
	}
	// Only "child"'s own remainder shard should be queued — parent's cleanup
	// must NOT have been seeded yet, since parent/child's group is still open.
	if counts[model.ShardQueued] != 1 {
		t.Fatalf("queued after parent's batch completes (child still pending) = %d, want 1 (only child's own remainder shard, no parent cleanup)",
			counts[model.ShardQueued])
	}

	// Now child's own (only) remainder shard completes — its group closes,
	// which must decrement parent's pending_children and, since parent's own
	// counters were already satisfied, close parent's group too in the same
	// transaction (closeDeleteGroupTx's upward walk).
	childLeased, err := s.LeaseShards("agent-a", 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(childLeased) != 1 || childLeased[0].ID != childIDs[0] {
		t.Fatalf("lease mismatch: leased=%v childIDs=%v", childLeased, childIDs)
	}
	if err := s.CompleteDeleteRemainder(childLeased[0].ID, childLeased[0].LeaseID, passID, child, nil,
		true, cleanupShard(child), cleanupShard, nil, nil); err != nil {
		t.Fatal(err)
	}
	counts, err = s.ShardStateCounts(passID)
	if err != nil {
		t.Fatal(err)
	}
	// Both cleanup shards must now be queued: child's own, and parent's
	// (unblocked by child's closure propagating up).
	if counts[model.ShardQueued] != 2 {
		t.Fatalf("queued after child's group closes = %d, want 2 (child's cleanup AND parent's, unblocked by the chain)",
			counts[model.ShardQueued])
	}
}

// TestDeleteGroupSeedsCleanupOnceAllChildrenDone is the ordinary case: a
// directory splits into 3 delete-remainder shards in one ShardSplit (the
// final one carrying LastBatch), all 3 complete, and the cleanup shard for
// the directory itself is seeded exactly once, only after the last one.
func TestDeleteGroupSeedsCleanupOnceAllChildrenDone(t *testing.T) {
	s := openTest(t)
	_, passID, shardID := seed(t, s)
	if _, err := s.LeaseShards("agent-a", 1, time.Minute); err != nil {
		t.Fatal(err)
	}

	const dir = "big-orphan-dir"
	remainders := []NewShard{
		deleteRemainderShard(dir), deleteRemainderShard(dir), deleteRemainderShard(dir),
	}
	// One DeleteGroupTotal per batch shard (RecordSplit bumps n_total by one
	// per entry, same units as n_done) — only the last carries LastBatch.
	ids, err := s.RecordSplit(shardID, 1, remainders, nil, nil, 0, []DeleteGroupTotal{
		{DirRel: dir, BuildCleanup: cleanupShard},
		{DirRel: dir, BuildCleanup: cleanupShard},
		{DirRel: dir, LastBatch: true, BuildCleanup: cleanupShard},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 3 {
		t.Fatalf("RecordSplit produced %d shard ids, want 3", len(ids))
	}

	countsAfterSplit, err := s.ShardStateCounts(passID)
	if err != nil {
		t.Fatal(err)
	}
	// Only the 3 remainder shards queued yet — no cleanup shard seeded before
	// any child has completed.
	if countsAfterSplit[model.ShardQueued] != 3 {
		t.Fatalf("queued after split = %d, want 3 (no cleanup shard yet)", countsAfterSplit[model.ShardQueued])
	}

	leased, err := s.LeaseShards("agent-a", 3, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(leased) != 3 {
		t.Fatalf("leased %d of 3 remainder shards", len(leased))
	}

	for i, r := range leased {
		if err := s.CompleteDeleteRemainder(r.ID, r.LeaseID, passID, dir, nil,
			true, cleanupShard(dir), cleanupShard, nil, nil); err != nil {
			t.Fatalf("complete remainder %d: %v", i, err)
		}
		counts, err := s.ShardStateCounts(passID)
		if err != nil {
			t.Fatal(err)
		}
		if i < 2 {
			if counts[model.ShardQueued] != 0 {
				t.Fatalf("after %d/3 children done: queued = %d, want 0 (cleanup not seeded yet)",
					i+1, counts[model.ShardQueued])
			}
		} else {
			// The 3rd (last) completion must have seeded exactly one new
			// queued shard: the cleanup for dir itself.
			if counts[model.ShardQueued] != 1 {
				t.Fatalf("after all 3 children done: queued = %d, want 1 (the cleanup shard)",
					counts[model.ShardQueued])
			}
		}
	}
}

// TestDeleteGroupHandlesChildCompletionRacingFinalBatch: a child shard's
// completion is an independent frame from the streaming parent's own
// ShardSplit batches — only the parent's own ShardResult is ordered after
// every split it shipped (protocol §4.2), not the split's own processing
// relative to its children. This drives that race directly: two batches are
// recorded (n_total reaches 2, only the second carrying LastBatch), the
// FIRST batch's shard completes before the SECOND (final) batch is ever
// recorded — CompleteDeleteRemainder's own check finds streaming not yet
// done and doesn't close the group — then the final batch lands and
// RecordSplit notices n_done already reached n_total and seeds the cleanup
// shard itself.
func TestDeleteGroupHandlesChildCompletionRacingFinalBatch(t *testing.T) {
	s := openTest(t)
	_, passID, shardID := seed(t, s)
	if _, err := s.LeaseShards("agent-a", 1, time.Minute); err != nil {
		t.Fatal(err)
	}

	const dir = "racing-dir"
	// First ShardSplit: one remainder batch, NOT the final one — every batch
	// (final or not) ships a DeleteRemainder and therefore a DeleteGroupTotal
	// entry (server.go onShardSplit), so n_total is bumped to 1 here; only
	// LastBatch (from total_children>0 on the wire) is still unset.
	ids, err := s.RecordSplit(shardID, 1, []NewShard{deleteRemainderShard(dir)}, nil, nil, 0,
		[]DeleteGroupTotal{{DirRel: dir, BuildCleanup: cleanupShard}})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 {
		t.Fatalf("RecordSplit produced %d ids, want 1", len(ids))
	}

	leased, err := s.LeaseShards("agent-a", 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(leased) != 1 {
		t.Fatalf("leased %d shards, want 1", len(leased))
	}
	// This first child completes while n_total is still 1 (streaming not
	// finished) — reaches n_done >= n_total by coincidence, but done_streaming
	// is still 0, so the group must NOT close yet.
	if err := s.CompleteDeleteRemainder(leased[0].ID, leased[0].LeaseID, passID, dir, nil,
		true, cleanupShard(dir), cleanupShard, nil, nil); err != nil {
		t.Fatal(err)
	}
	counts, err := s.ShardStateCounts(passID)
	if err != nil {
		t.Fatal(err)
	}
	if counts[model.ShardQueued] != 0 {
		t.Fatalf("queued after the first child completes with streaming unfinished = %d, want 0",
			counts[model.ShardQueued])
	}

	// Now the streaming parent's final batch lands (a second ShardSplit,
	// different seq — the parent is still mid-stream, this is its EOF batch,
	// carrying its own remainder shard plus LastBatch): n_total becomes 2,
	// but only 1 child has completed so far, so RecordSplit itself must not
	// close the group on this call either.
	ids2, err := s.RecordSplit(shardID, 2, []NewShard{deleteRemainderShard(dir)}, nil, nil, 0,
		[]DeleteGroupTotal{{DirRel: dir, LastBatch: true, BuildCleanup: cleanupShard}})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids2) != 1 {
		t.Fatalf("RecordSplit produced %d ids, want 1", len(ids2))
	}
	counts, err = s.ShardStateCounts(passID)
	if err != nil {
		t.Fatal(err)
	}
	if counts[model.ShardQueued] != 1 {
		t.Fatalf("queued after the final batch lands with 1/2 done = %d, want 1 (only the new remainder, no cleanup yet)",
			counts[model.ShardQueued])
	}

	// The second (final) batch's own shard now completes — n_done reaches
	// n_total (2) AND done_streaming is set, so THIS completion must seed the
	// cleanup shard.
	leased2, err := s.LeaseShards("agent-a", 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(leased2) != 1 || leased2[0].ID != ids2[0] {
		t.Fatalf("lease mismatch: leased=%v ids2=%v", leased2, ids2)
	}
	if err := s.CompleteDeleteRemainder(leased2[0].ID, leased2[0].LeaseID, passID, dir, nil,
		true, cleanupShard(dir), cleanupShard, nil, nil); err != nil {
		t.Fatal(err)
	}
	counts, err = s.ShardStateCounts(passID)
	if err != nil {
		t.Fatal(err)
	}
	if counts[model.ShardQueued] != 1 {
		t.Fatalf("queued after both children done and streaming finished = %d, want 1 (the cleanup shard)",
			counts[model.ShardQueued])
	}
}

// TestDeleteGroupNeverSeedsCleanupTwice: the closed flag must survive both
// write paths (CompleteDeleteRemainder and RecordSplit) each independently
// re-checking a group they didn't close — otherwise a retransmit or a
// re-delivered result could seed the cleanup shard more than once, which
// would attempt to remove an already-gone directory twice (harmless per se,
// since delete is ENOENT-tolerant, but wasteful and would double-count).
func TestDeleteGroupNeverSeedsCleanupTwice(t *testing.T) {
	s := openTest(t)
	_, passID, shardID := seed(t, s)
	if _, err := s.LeaseShards("agent-a", 1, time.Minute); err != nil {
		t.Fatal(err)
	}

	const dir = "dup-dir"
	ids, err := s.RecordSplit(shardID, 1, []NewShard{deleteRemainderShard(dir)}, nil, nil, 0,
		[]DeleteGroupTotal{{DirRel: dir, LastBatch: true, BuildCleanup: cleanupShard}})
	if err != nil {
		t.Fatal(err)
	}
	leased, err := s.LeaseShards("agent-a", 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(leased) != 1 || leased[0].ID != ids[0] {
		t.Fatalf("lease mismatch: leased=%v ids=%v", leased, ids)
	}
	if err := s.CompleteDeleteRemainder(leased[0].ID, leased[0].LeaseID, passID, dir, nil,
		true, cleanupShard(dir), cleanupShard, nil, nil); err != nil {
		t.Fatal(err)
	}
	counts, err := s.ShardStateCounts(passID)
	if err != nil {
		t.Fatal(err)
	}
	if counts[model.ShardQueued] != 1 {
		t.Fatalf("queued after the only child completes = %d, want 1 (cleanup seeded)", counts[model.ShardQueued])
	}

	// A retransmitted final batch for the same directory (agent outbox
	// replay racing a reconnect, same shape RecordSplit already handles for
	// every other split kind) must not seed a second cleanup shard.
	if _, err := s.RecordSplit(shardID, 2, nil, nil, nil, 0,
		[]DeleteGroupTotal{{DirRel: dir, LastBatch: true, BuildCleanup: cleanupShard}}); err != nil {
		t.Fatal(err)
	}
	counts, err = s.ShardStateCounts(passID)
	if err != nil {
		t.Fatal(err)
	}
	if counts[model.ShardQueued] != 1 {
		t.Fatalf("queued after a retransmitted final batch = %d, want still 1 (no duplicate cleanup shard)",
			counts[model.ShardQueued])
	}
}
