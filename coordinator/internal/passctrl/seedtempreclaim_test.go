package passctrl

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"drsync/coordinator/internal/model"
	"drsync/coordinator/internal/store"
	drsyncpb "drsync/proto/gen/drsyncpb"
)

// TestSeedTempReclaimOnlyUnfinalized: a chunk group that never finalized leaves
// its temp in the destination, and nothing else will collect it — the agent's
// orphan sweep spares temps tagged with the pass it is running (that rule is
// what stops a re-walk from deleting a group's temp while its chunks are still
// writing), so a job ending on this pass would keep the residue forever. The
// SCANNING drain seeds a reclaim task per such group, and none for groups that
// finalized normally.
func TestSeedTempReclaimOnlyUnfinalized(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, []byte(baseSpec))
	pass, err := c.st.CreatePass(job.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	parent, err := c.st.InsertShards(pass.ID, 0, []store.NewShard{{Kind: model.KindDir}})
	if err != nil {
		t.Fatal(err)
	}

	// Two big files fanned out: one finalizes, one is left mid-assembly.
	chunkShard := func(rel, temp string) store.NewShard {
		payload, err := proto.Marshal(&drsyncpb.ChunkTask{RelPath: rel, TempName: temp})
		if err != nil {
			t.Fatal(err)
		}
		return store.NewShard{Kind: model.KindChunk, RelPath: rel, Payload: payload}
	}
	const doneTemp, deadTemp = ".drsync.tmp.1-1.aa.0", ".drsync.tmp.1-1.aa.1"
	if _, err := c.st.RecordSplit(parent[0], 1,
		[]store.NewShard{
			chunkShard("a/done.bin", doneTemp),
			chunkShard("b/dead.bin", deadTemp),
		},
		[]store.NewChunkGroup{
			{RelPath: "a/done.bin", TempName: doneTemp, Size: 8, MtimeNs: 1, NChunks: 1},
			{RelPath: "b/dead.bin", TempName: deadTemp, Size: 8, MtimeNs: 2, NChunks: 1},
		}); err != nil {
		t.Fatal(err)
	}

	// Drive a/done.bin's group to 'done'; b/dead.bin's stays unfinalized.
	leased, err := c.st.LeaseShards("a1", 100, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	var finalized bool
	for _, sh := range leased {
		if sh.Kind != model.KindChunk || sh.RelPath != "a/done.bin" {
			continue
		}
		if err := c.st.CompleteFinalizeChunk(sh.ID, sh.LeaseID, pass.ID, sh.RelPath); err != nil {
			t.Fatal(err)
		}
		finalized = true
		break
	}
	if !finalized {
		t.Fatal("no chunk shard for a/done.bin to finalize")
	}

	n, err := c.seedTempReclaim(pass)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("seeded %d reclaim tasks, want 1 (only the unfinalized group)", n)
	}

	// The seeded task names the dead group's temp, and nothing else.
	queued, err := c.st.LeaseShards("a2", 100, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	var got []*drsyncpb.ChunkTask
	for _, sh := range queued {
		if sh.Kind != model.KindChunk {
			continue
		}
		ct := &drsyncpb.ChunkTask{}
		if err := proto.Unmarshal(sh.Payload, ct); err != nil {
			t.Fatal(err)
		}
		if ct.Reclaim {
			got = append(got, ct)
		}
	}
	if len(got) != 1 {
		t.Fatalf("found %d reclaim tasks queued, want 1", len(got))
	}
	if got[0].TempName != deadTemp || got[0].RelPath != "b/dead.bin" {
		t.Fatalf("reclaim task = %+v, want temp %q for b/dead.bin", got[0], deadTemp)
	}
	// A reclaim carries no byte range and no gen: it must never be mistaken for
	// assembly work by the agent or the completion path.
	if got[0].Length != 0 || got[0].Finalize || got[0].CreateTemp || got[0].Gen != nil {
		t.Fatalf("reclaim task carries assembly fields: %+v", got[0])
	}
}
