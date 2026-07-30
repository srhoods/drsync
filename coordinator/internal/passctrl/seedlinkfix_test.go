package passctrl

import (
	"testing"

	"drsync/coordinator/internal/model"
	"drsync/coordinator/internal/store"
)

// TestSeedLinkfixBuildsOneTaskPerPendingMember: seedLinkfix turns every
// PendingLinkMembers row into a LinkTask shard, and marks the member 'queued'
// so a defensive re-run would see nothing left to do.
func TestSeedLinkfixBuildsOneTaskPerPendingMember(t *testing.T) {
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

	// Two link groups: one with a pending member (needs a LinkTask), one
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
		t.Fatalf("seedLinkfix returned %d, want 1 (only b/two needs a LinkTask)", n)
	}

	rows, err := c.st.LeaseShards("agent-a", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	var linkfixShards int
	for _, r := range rows {
		if r.Kind == model.KindLinkfix {
			linkfixShards++
		}
	}
	if linkfixShards != 1 {
		t.Fatalf("granted %d linkfix shards, want 1", linkfixShards)
	}

	members, err := c.st.PendingLinkMembers(pass.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 0 {
		t.Fatalf("PendingLinkMembers after seedLinkfix = %v, want none (queued, not pending)", members)
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
