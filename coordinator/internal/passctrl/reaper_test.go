package passctrl

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"drsync/coordinator/internal/metrics"
	"drsync/coordinator/internal/model"
	"drsync/coordinator/internal/store"
)

// seedDoneShards queues n shards of kind under passID and drives each to DONE
// via the real lease/complete path, returning their ids. Loops the
// lease→complete round trip rather than granting once: entry-list shards are
// capped at 4 leased per agent (maxEntrylistPerAgent), so completing frees a
// slot for the next round rather than every shard landing in one grant.
func seedDoneShards(t *testing.T, c *Controller, passID int64, kind model.ShardKind, n int) []int64 {
	t.Helper()
	batch := make([]store.NewShard, n)
	for i := range batch {
		batch[i] = store.NewShard{Kind: kind}
	}
	ids, err := c.st.InsertShards(passID, 0, batch)
	if err != nil {
		t.Fatal(err)
	}
	remaining := n
	for remaining > 0 {
		rows, err := c.st.LeaseShards("seed-agent", remaining, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == 0 {
			t.Fatalf("seedDoneShards: stuck with %d of %d %s shards still ungranted", remaining, n, kind)
		}
		for _, r := range rows {
			if err := c.st.CompleteShard(r.ID, r.LeaseID, nil); err != nil {
				t.Fatal(err)
			}
			remaining--
		}
	}
	return ids
}

// shardGone reports whether a shard's row has been deleted (ShardMeta reads
// straight from the shards table, so sql.ErrNoRows is the reap signal).
func shardGone(t *testing.T, c *Controller, id int64) bool {
	t.Helper()
	_, _, _, err := c.st.ShardMeta(id)
	if errors.Is(err, sql.ErrNoRows) {
		return true
	}
	if err != nil {
		t.Fatal(err)
	}
	return false
}

func assertAllGone(t *testing.T, c *Controller, label string, ids []int64) {
	t.Helper()
	for _, id := range ids {
		if !shardGone(t, c, id) {
			t.Errorf("%s: shard %d should have been reaped but still exists", label, id)
		}
	}
}

func assertNoneGone(t *testing.T, c *Controller, label string, ids []int64) {
	t.Helper()
	for _, id := range ids {
		if shardGone(t, c, id) {
			t.Errorf("%s: shard %d should have survived but was reaped", label, id)
		}
	}
}

// TestAdvanceReapsScanningOnDirfixTransition: once SCANNING drains and advance
// flips the pass to DIRFIX, every DONE probe/dir/entrylist/chunk shard of that
// pass must be gone — this is the phase that dwarfs everything else on a large
// tree, so it's the transition the Shard Reaper matters most at.
func TestAdvanceReapsScanningOnDirfixTransition(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, []byte(baseSpec))
	pass, err := c.st.CreatePass(job.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}

	doneDir := seedDoneShards(t, c, pass.ID, model.KindDir, 50)
	doneEL := seedDoneShards(t, c, pass.ID, model.KindEntryList, 50)
	doneChunk := seedDoneShards(t, c, pass.ID, model.KindChunk, 20)

	// A shard of a DIFFERENT job's pass must never be touched by this reap.
	// (A second pass on the SAME job can't be used for this: ActivePass picks
	// the newest non-COMPLETE pass, so advance(job) would operate on that pass
	// instead of the one under test.)
	// makeJob always names the job "t1" (ignores metadata.name), so a second
	// job in the same store needs CreateJob directly.
	otherJob, err := c.st.CreateJob("t2", []byte(`
apiVersion: drsync/v1
kind: Job
metadata:
  name: t2
spec:
  source: { path: /src2 }
  destination: { path: /dst2 }
`), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.st.SetJobState(otherJob.ID, model.JobRunning); err != nil {
		t.Fatal(err)
	}
	otherPass, err := c.st.CreatePass(otherJob.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	otherDone := seedDoneShards(t, c, otherPass.ID, model.KindDir, 5)

	if err := c.advance(job); err != nil {
		t.Fatal(err)
	}
	pass, err = c.st.PassByNo(job.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if pass.State != model.PassDirfix {
		t.Fatalf("pass state = %s, want DIRFIX", pass.State)
	}

	assertAllGone(t, c, "scan-phase DONE shards", append(append(doneDir, doneEL...), doneChunk...))
	assertNoneGone(t, c, "other pass's DONE shards", otherDone)
}

// TestAdvanceReapsDirfixOnLinkfixTransition: DIRFIX's own DONE shards are
// reaped once the pass reaches LINKFIX (the phase now inserted between DIRFIX
// and VERIFY, docs/DESIGN-hardlinks.md).
func TestAdvanceReapsDirfixOnLinkfixTransition(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, []byte(baseSpec))
	pass, err := c.st.CreatePass(job.ID, 1, model.PassDirfix)
	if err != nil {
		t.Fatal(err)
	}
	doneDirfix := seedDoneShards(t, c, pass.ID, model.KindDirfix, 30)

	if err := c.advance(job); err != nil {
		t.Fatal(err)
	}
	pass, err = c.st.PassByNo(job.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if pass.State != model.PassLinkfix {
		t.Fatalf("pass state = %s, want LINKFIX", pass.State)
	}
	assertAllGone(t, c, "dirfix-phase DONE shards", doneDirfix)
}

// TestAdvanceReapsLinkfixOnVerifyTransition: LINKFIX's own DONE shards (empty
// link_groups here — no LinkTasks were seeded, so this exercises the
// no-op-but-still-advances path) are reaped once the pass reaches VERIFY.
func TestAdvanceReapsLinkfixOnVerifyTransition(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, []byte(baseSpec))
	pass, err := c.st.CreatePass(job.ID, 1, model.PassLinkfix)
	if err != nil {
		t.Fatal(err)
	}
	doneLinkfix := seedDoneShards(t, c, pass.ID, model.KindLinkfix, 30)

	if err := c.advance(job); err != nil {
		t.Fatal(err)
	}
	pass, err = c.st.PassByNo(job.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if pass.State != model.PassVerify {
		t.Fatalf("pass state = %s, want VERIFY", pass.State)
	}
	assertAllGone(t, c, "linkfix-phase DONE shards", doneLinkfix)
}

// TestAdvanceReapsVerifyOnPassComplete: VERIFY's own DONE shards are reaped
// once the pass completes (whether or not the job goes on to seed another
// pass — both branches of advance()'s PassVerify case call reapPhase before
// deciding what's next).
func TestAdvanceReapsVerifyOnPassComplete(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, []byte(baseSpec))
	pass, err := c.st.CreatePass(job.ID, 1, model.PassVerify)
	if err != nil {
		t.Fatal(err)
	}
	doneVerify := seedDoneShards(t, c, pass.ID, model.KindVerify, 30)

	if err := c.advance(job); err != nil {
		t.Fatal(err)
	}
	pass, err = c.st.PassByNo(job.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if pass.State != model.PassComplete {
		t.Fatalf("pass state = %s, want COMPLETE", pass.State)
	}
	assertAllGone(t, c, "verify-phase DONE shards", doneVerify)
}

// TestAdvanceReapsDeleteOnJobComplete: a DELETE pass's own DONE shards are
// reaped once the job reaches COMPLETED.
func TestAdvanceReapsDeleteOnJobComplete(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, []byte(baseSpec))
	pass, err := c.st.CreatePass(job.ID, 2, model.PassDelete)
	if err != nil {
		t.Fatal(err)
	}
	doneDelete := seedDoneShards(t, c, pass.ID, model.KindDelete, 30)

	if err := c.advance(job); err != nil {
		t.Fatal(err)
	}
	if st := jobState(t, c, job.ID); st != model.JobCompleted {
		t.Fatalf("job state = %s, want COMPLETED", st)
	}
	assertAllGone(t, c, "delete-phase DONE shards", doneDelete)
}

// TestAdvanceDoesNotReapWhilePhaseStillDraining: advance() must not reap
// anything when the phase hasn't actually drained (queued/leased work
// remains) — reaping is gated on the same drain proof that gates the phase
// transition itself, never on a timer or a DONE count alone.
func TestAdvanceDoesNotReapWhilePhaseStillDraining(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, []byte(baseSpec))
	pass, err := c.st.CreatePass(job.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	doneDir := seedDoneShards(t, c, pass.ID, model.KindDir, 10)
	// One shard still queued: the phase has not drained.
	if _, err := c.st.InsertShards(pass.ID, 0,
		[]store.NewShard{{Kind: model.KindDir}}); err != nil {
		t.Fatal(err)
	}

	if err := c.advance(job); err != nil {
		t.Fatal(err)
	}
	pass, err = c.st.PassByNo(job.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if pass.State != model.PassScanning {
		t.Fatalf("pass state = %s, want still SCANNING (phase not drained)", pass.State)
	}
	assertNoneGone(t, c, "DONE shards while phase still draining", doneDir)
}

// TestReapPhaseIncrementsMetric: reapPhase reports how much it reaped to the
// ShardsReaped counter when metrics are wired (SetMetrics), and does not panic
// when they aren't (the nil default every test above this one relies on).
func TestReapPhaseIncrementsMetric(t *testing.T) {
	c := newController(t)
	met := metrics.New()
	c.SetMetrics(met)

	job := makeJob(t, c, []byte(baseSpec))
	pass, err := c.st.CreatePass(job.ID, 1, model.PassDirfix)
	if err != nil {
		t.Fatal(err)
	}
	seedDoneShards(t, c, pass.ID, model.KindDirfix, 12)

	if err := c.advance(job); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(met.ShardsReaped); got != 12 {
		t.Fatalf("ShardsReaped = %v, want 12", got)
	}
}
