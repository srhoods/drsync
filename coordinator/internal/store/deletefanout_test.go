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
		{DirRel: dir, CleanupShard: cleanupShard(dir)},
		{DirRel: dir, CleanupShard: cleanupShard(dir)},
		{DirRel: dir, LastBatch: true, CleanupShard: cleanupShard(dir)},
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
			cleanupShard(dir), nil); err != nil {
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
		[]DeleteGroupTotal{{DirRel: dir, CleanupShard: cleanupShard(dir)}})
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
		cleanupShard(dir), nil); err != nil {
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
		[]DeleteGroupTotal{{DirRel: dir, LastBatch: true, CleanupShard: cleanupShard(dir)}})
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
		cleanupShard(dir), nil); err != nil {
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
		[]DeleteGroupTotal{{DirRel: dir, LastBatch: true, CleanupShard: cleanupShard(dir)}})
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
		cleanupShard(dir), nil); err != nil {
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
		[]DeleteGroupTotal{{DirRel: dir, LastBatch: true, CleanupShard: cleanupShard(dir)}}); err != nil {
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
