package passctrl

import (
	"testing"
	"time"

	"drsync/coordinator/internal/model"
)

// TestAdvanceAutoRetriesParkedShardsOnce: a pass that would otherwise block
// indefinitely on parked work now gets one automatic retry round first —
// many park causes (a mount blip, a brief NFS hiccup) are transient at the
// moment a shard exhausts MaxShardAttempts, and by the time the rest of a
// large pass has drained, conditions may well have changed.
// RetryParkedByJob resets the attempt counter, so this call must requeue the
// shard (not silently no-op), and advance() must not treat that requeue as a
// completed phase this same tick — the shard needs a later tick to actually
// be granted and drain.
func TestAdvanceAutoRetriesParkedShardsOnce(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, []byte(baseSpec))
	pass, err := c.st.CreatePass(job.ID, 1, model.PassDirfix)
	if err != nil {
		t.Fatal(err)
	}
	parkOneShard(t, c, pass, "a/parked-dir")

	if err := c.advance(job); err != nil {
		t.Fatal(err)
	}

	counts, err := c.st.ShardStateCounts(pass.ID)
	if err != nil {
		t.Fatal(err)
	}
	if counts[model.ShardParked] != 0 {
		t.Fatalf("parked count after first advance() = %d, want 0 (auto-retry should have requeued it)",
			counts[model.ShardParked])
	}
	if counts[model.ShardQueued] != 1 {
		t.Fatalf("queued count after first advance() = %d, want 1 (the retried shard)",
			counts[model.ShardQueued])
	}
	// Pass must still be DIRFIX: the requeued shard has not been granted or
	// drained yet, so the phase cannot have transitioned.
	pass, err = c.st.PassByNo(job.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if pass.State != model.PassDirfix {
		t.Fatalf("pass state = %s, want still DIRFIX (retried shard not yet drained)", pass.State)
	}
}

// TestAdvanceBlocksOnSecondParkAfterAutoRetry: if the auto-retried shard
// parks again, the pass must fall back to the original block-and-alert
// behavior rather than retrying forever — an unbounded auto-retry loop on a
// genuinely stuck shard (e.g. a real permissions problem) would just be an
// infinite loop wearing a different name, and would silently mask a job that
// needs operator attention.
func TestAdvanceBlocksOnSecondParkAfterAutoRetry(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, []byte(baseSpec))
	pass, err := c.st.CreatePass(job.ID, 1, model.PassDirfix)
	if err != nil {
		t.Fatal(err)
	}
	parkOneShard(t, c, pass, "a/parked-dir")

	if err := c.advance(job); err != nil {
		t.Fatal(err)
	}
	// Simulate the retried shard failing again: lease it and park it once more.
	leased, err := c.st.LeaseShards("agent-1", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(leased) != 1 {
		t.Fatalf("leased %d shards, want 1 (the retried shard)", len(leased))
	}
	if err := c.st.ParkShard(leased[0].ID, leased[0].LeaseID, "EIO again"); err != nil {
		t.Fatal(err)
	}

	if err := c.advance(job); err != nil {
		t.Fatal(err)
	}
	counts, err := c.st.ShardStateCounts(pass.ID)
	if err != nil {
		t.Fatal(err)
	}
	if counts[model.ShardParked] != 1 {
		t.Fatalf("parked count after second advance() = %d, want 1 (must stay parked, "+
			"not retried a second time)", counts[model.ShardParked])
	}
	if counts[model.ShardQueued] != 0 {
		t.Fatalf("queued count after second advance() = %d, want 0 (no further auto-retry)",
			counts[model.ShardQueued])
	}
	pass, err = c.st.PassByNo(job.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if pass.State != model.PassDirfix {
		t.Fatalf("pass state = %s, want still DIRFIX (blocked on the re-parked shard)", pass.State)
	}
}

// TestAdvanceProceedsAfterAutoRetrySucceeds: the common case the feature
// exists for — the retried shard succeeds this time, and the phase advances
// normally once it drains, with no operator intervention needed at all.
func TestAdvanceProceedsAfterAutoRetrySucceeds(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, []byte(baseSpec))
	pass, err := c.st.CreatePass(job.ID, 1, model.PassDirfix)
	if err != nil {
		t.Fatal(err)
	}
	parkOneShard(t, c, pass, "a/parked-dir")

	if err := c.advance(job); err != nil {
		t.Fatal(err)
	}
	leased, err := c.st.LeaseShards("agent-1", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(leased) != 1 {
		t.Fatalf("leased %d shards, want 1 (the retried shard)", len(leased))
	}
	if err := c.st.CompleteShard(leased[0].ID, leased[0].LeaseID, 0, nil, nil); err != nil {
		t.Fatal(err)
	}

	if err := c.advance(job); err != nil {
		t.Fatal(err)
	}
	drainReaps(t, c)
	pass, err = c.st.PassByNo(job.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if pass.State != model.PassLinkfix {
		t.Fatalf("pass state = %s, want LINKFIX (retried shard completed, phase should advance)", pass.State)
	}
}
