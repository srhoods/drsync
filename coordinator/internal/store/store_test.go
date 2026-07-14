package store

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"drsync/coordinator/internal/model"
)

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
	ids, err := s.InsertShards(pass.ID, 0, []NewShard{{Kind: model.KindDir, RelPath: ""}})
	if err != nil {
		t.Fatal(err)
	}
	return job.ID, pass.ID, ids[0]
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
		requeued, parked, err := s.ExpireLeases(time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if i < MaxShardAttempts-1 && requeued != 1 {
			t.Fatalf("attempt %d: requeued=%d parked=%d", i, requeued, parked)
		}
		if i == MaxShardAttempts-1 && parked != 1 {
			t.Fatalf("final attempt: requeued=%d parked=%d (want park)", requeued, parked)
		}
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
	ids1, err := s.RecordSplit(shardID, 7, subs)
	if err != nil {
		t.Fatal(err)
	}
	ids2, err := s.RecordSplit(shardID, 7, subs) // retransmit
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

func TestDisabledAgentNotGranted(t *testing.T) {
	s := openTest(t)
	_, _, shardID := seed(t, s)
	if err := s.UpsertAgent("agent-a", "host", "v1"); err != nil {
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
		if _, _, err := s.ExpireLeases(time.Now()); err != nil {
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
	if _, _, err := s.ExpireLeases(time.Now()); err != nil {
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
		if _, _, err := s.ExpireLeases(time.Now()); err != nil {
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
		if _, _, err := s.ExpireLeases(time.Now().Add(time.Hour)); err != nil {
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
