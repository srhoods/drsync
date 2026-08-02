package passctrl

import (
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"

	"drsync/coordinator/internal/model"
	"drsync/coordinator/internal/store"
	drsyncpb "drsync/proto/gen/drsyncpb"
)

// TestSeedLinkfixBuildsOneBatchTaskForPendingMembers: seedLinkfix packs every
// PendingLinkMembers row into LinkTaskBatch shards (linkfixBatchSize members
// each, one shard per batch — not one shard per member), and marks every
// batched member 'queued' so a defensive re-run would see nothing left to do.
func TestSeedLinkfixBuildsOneBatchTaskForPendingMembers(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, []byte(baseSpec))
	pass, err := c.st.CreatePass(job.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	root, err := c.st.InsertShards(pass.ID, 0, []store.NewShard{{Kind: model.KindDir}})
	if err != nil {
		t.Fatal(err)
	}
	shardID := root[0]

	// Two link groups: one with a pending member (needs a LinkEntry), one
	// that's anchor-only (no member needs one).
	if _, err := c.st.RecordSplit(shardID, 1, nil, nil, []store.NewLinkSighting{
		{Dev: 1, Ino: 100, RelPath: "a/one", Nlink: 2, Size: 10, MtimeNs: 1},
	}, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.st.RecordSplit(shardID, 2, nil, nil, []store.NewLinkSighting{
		{Dev: 1, Ino: 100, RelPath: "b/two", Nlink: 2, Size: 10, MtimeNs: 1},
	}, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.st.RecordSplit(shardID, 3, nil, nil, []store.NewLinkSighting{
		{Dev: 1, Ino: 200, RelPath: "c/anchor-only", Nlink: 1, Size: 5, MtimeNs: 2},
	}, 0); err != nil {
		t.Fatal(err)
	}

	n, err := c.seedLinkfix(pass)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("seedLinkfix returned %d, want 1 (only b/two needs a LinkEntry)", n)
	}

	rows, err := c.st.LeaseShards("agent-a", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	var linkfixShards int
	var batch drsyncpb.LinkTaskBatch
	for _, r := range rows {
		if r.Kind != model.KindLinkfix {
			continue
		}
		linkfixShards++
		if err := proto.Unmarshal(r.Payload, &batch); err != nil {
			t.Fatalf("unmarshal LinkTaskBatch: %v", err)
		}
	}
	if linkfixShards != 1 {
		t.Fatalf("granted %d linkfix shards, want 1 (one batch, below linkfixBatchSize)", linkfixShards)
	}
	if len(batch.Links) != 1 || batch.Links[0].MemberRel != "b/two" || batch.Links[0].AnchorRel != "a/one" {
		t.Fatalf("batch.Links = %v, want one LinkEntry b/two <- a/one", batch.Links)
	}

	members, err := c.st.PendingLinkMembers(pass.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 0 {
		t.Fatalf("PendingLinkMembers after seedLinkfix = %v, want none (queued, not pending)", members)
	}
}

// TestSeedLinkfixFlushesAtBatchBoundary: more pending members than
// linkfixBatchSize must split across multiple LinkTaskBatch shards, each
// capped at linkfixBatchSize entries — otherwise a single InsertShards call
// is back to holding the store lock for an unbounded number of members, the
// exact problem batching exists to bound.
func TestSeedLinkfixFlushesAtBatchBoundary(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, []byte(baseSpec))
	pass, err := c.st.CreatePass(job.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	root, err := c.st.InsertShards(pass.ID, 0, []store.NewShard{{Kind: model.KindDir}})
	if err != nil {
		t.Fatal(err)
	}
	shardID := root[0]

	// linkfixBatchSize + 1 distinct groups, each with exactly one pending
	// member, so the pending-member count is exactly one over one batch.
	const want = linkfixBatchSize + 1
	var seq uint64
	for i := 0; i < want; i++ {
		seq++
		anchor := store.NewLinkSighting{
			Dev: 1, Ino: uint64(1000 + i), RelPath: fmt.Sprintf("a/anchor%d", i),
			Nlink: 2, Size: 10, MtimeNs: 1,
		}
		if _, err := c.st.RecordSplit(shardID, seq, nil, nil,
			[]store.NewLinkSighting{anchor}, 0); err != nil {
			t.Fatal(err)
		}
		seq++
		member := store.NewLinkSighting{
			Dev: 1, Ino: uint64(1000 + i), RelPath: fmt.Sprintf("b/member%d", i),
			Nlink: 2, Size: 10, MtimeNs: 1,
		}
		if _, err := c.st.RecordSplit(shardID, seq, nil, nil,
			[]store.NewLinkSighting{member}, 0); err != nil {
			t.Fatal(err)
		}
	}

	n, err := c.seedLinkfix(pass)
	if err != nil {
		t.Fatal(err)
	}
	if n != want {
		t.Fatalf("seedLinkfix returned %d, want %d", n, want)
	}

	rows, err := c.st.LeaseShards("agent-a", want, 0)
	if err != nil {
		t.Fatal(err)
	}
	var linkfixShards, totalEntries int
	for _, r := range rows {
		if r.Kind != model.KindLinkfix {
			continue
		}
		linkfixShards++
		var batch drsyncpb.LinkTaskBatch
		if err := proto.Unmarshal(r.Payload, &batch); err != nil {
			t.Fatalf("unmarshal LinkTaskBatch: %v", err)
		}
		if len(batch.Links) > linkfixBatchSize {
			t.Fatalf("batch has %d entries, want <= linkfixBatchSize (%d)",
				len(batch.Links), linkfixBatchSize)
		}
		totalEntries += len(batch.Links)
	}
	if linkfixShards != 2 {
		t.Fatalf("granted %d linkfix shards, want 2 (one full batch + one overflow batch of 1)",
			linkfixShards)
	}
	if totalEntries != want {
		t.Fatalf("total LinkEntry rows across batches = %d, want %d", totalEntries, want)
	}
}

// TestAdvanceGoesThroughLinkfixBetweenDirfixAndVerify: the full phase
// sequence — DIRFIX drains, seedLinkfix runs (here: no link groups, so it's a
// no-op), LINKFIX drains immediately since nothing was seeded, and the next
// tick reaches VERIFY.
func TestAdvanceGoesThroughLinkfixBetweenDirfixAndVerify(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, []byte(baseSpec))
	pass, err := c.st.CreatePass(job.ID, 1, model.PassDirfix)
	if err != nil {
		t.Fatal(err)
	}

	if err := c.advance(job); err != nil {
		t.Fatal(err)
	}
	pass, err = c.st.PassByNo(job.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if pass.State != model.PassLinkfix {
		t.Fatalf("pass state after DIRFIX advance = %s, want LINKFIX", pass.State)
	}

	// LINKFIX has no shards (no link groups existed), so the next tick's
	// drain check passes immediately and advances to VERIFY.
	if err := c.advance(job); err != nil {
		t.Fatal(err)
	}
	pass, err = c.st.PassByNo(job.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if pass.State != model.PassVerify {
		t.Fatalf("pass state after LINKFIX advance = %s, want VERIFY", pass.State)
	}
}
