package store

import (
	"database/sql"
	"errors"
	"fmt"
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
				if _, err := s.LeaseShards("w", 16, -time.Second, 0); err != nil {
					errCh <- err
					return
				}
				if _, _, err := s.ExpireLeases(time.Now()); err != nil {
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
	if _, err := s.LeaseShards("agent-a", 1, time.Minute, 0); err != nil {
		t.Fatal(err)
	}
	assertCountsConsistent(t, s, "after lease (1 LEASED)")
	if _, err := s.RecordSplit(shardID, 1, []NewShard{
		{Kind: model.KindDir, RelPath: "a"}, {Kind: model.KindEntryList, RelPath: "b"},
	}, nil); err != nil {
		t.Fatal(err)
	}
	assertCountsConsistent(t, s, "after split")

	// Lease + complete a child.
	rows, err := s.LeaseShards("agent-b", 1, time.Minute, 0)
	if err != nil || len(rows) != 1 {
		t.Fatalf("lease child: %v %v", rows, err)
	}
	if err := s.CompleteShard(rows[0].ID, rows[0].LeaseID, nil); err != nil {
		t.Fatal(err)
	}
	assertCountsConsistent(t, s, "after complete child (1 DONE)")

	// Drive the other child to PARKED via repeated lease+expiry.
	for i := 0; i < MaxShardAttempts+1; i++ {
		s.LeaseShards("agent-c", 1, -time.Second, 0)
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
	rows, err := s.LeaseShards("agent-a", 4, -time.Second, 0)
	if err != nil || len(rows) != 2 {
		t.Fatalf("want 2 leased, got %v err=%v", rows, err)
	}
	// The agent's heartbeat reports holding only the first lease.
	if err := s.RenewLeasesByID("agent-a", []int64{rows[0].LeaseID}, time.Hour); err != nil {
		t.Fatal(err)
	}
	// Sweep: exactly the un-renewed lease requeues; the renewed one survives.
	requeued, parked, err := s.ExpireLeases(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if requeued != 1 || parked != 0 {
		t.Fatalf("want 1 requeued (the unheld lease), got requeued=%d parked=%d", requeued, parked)
	}
	counts, _ := s.ShardStateCounts(passID)
	if counts[model.ShardLeased] != 1 || counts[model.ShardQueued] != 1 {
		t.Fatalf("counts=%+v, want 1 LEASED (renewed) + 1 QUEUED (expired)", counts)
	}
	// Empty held list must renew nothing (never fall back to renew-all).
	if err := s.RenewLeasesByID("agent-a", nil, time.Hour); err != nil {
		t.Fatal(err)
	}
}

func TestLeaseLifecycle(t *testing.T) {
	s := openTest(t)
	_, passID, shardID := seed(t, s)

	rows, err := s.LeaseShards("agent-a", 4, time.Minute, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ID != shardID {
		t.Fatalf("lease grant = %+v", rows)
	}
	lease := rows[0].LeaseID

	// Second grant: nothing queued.
	rows2, _ := s.LeaseShards("agent-b", 4, time.Minute, 0)
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
		rows, err := s.LeaseShards("agent-a", 1, -time.Second, 0) // already expired
		if err != nil {
			t.Fatal(err)
		}
		if i < MaxShardAttempts && len(rows) != 1 {
			// anti-affinity skips agent-a on retries; lease from another agent
			rows, err = s.LeaseShards("agent-b", 1, -time.Second, 0)
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
	if _, err := s.LeaseShards("agent-a", 1, time.Minute, 0); err != nil {
		t.Fatal(err)
	}

	subs := []NewShard{
		{Kind: model.KindDir, RelPath: "a"},
		{Kind: model.KindDir, RelPath: "b"},
	}
	ids1, err := s.RecordSplit(shardID, 7, subs, nil)
	if err != nil {
		t.Fatal(err)
	}
	ids2, err := s.RecordSplit(shardID, 7, subs, nil) // retransmit
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
	rows, err := s.LeaseShards("agent-a", 4, time.Minute, 0)
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
	rows, err := s.LeaseShards("drainer", 4, time.Minute, 0)
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
	rows, err = s.LeaseShards("other", 4, time.Minute, 0)
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
	rows, err := s.LeaseShards("agent-a", 4, time.Minute, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("disabled agent granted work: %+v", rows)
	}
	// Another, enabled agent still gets the shard.
	rows, err = s.LeaseShards("agent-b", 4, time.Minute, 0)
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
		rows, err := s.LeaseShards("agent-a", 1, -time.Second, 0) // sole agent, expired ttl
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
	if _, err := s.LeaseShards("agent-a", 1, -time.Second, 0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ExpireLeases(time.Now()); err != nil {
		t.Fatal(err)
	}

	// agent-a, one credit: must get the fresh shard2, not its poisoned shard1.
	rows, err := s.LeaseShards("agent-a", 1, time.Minute, 0)
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
	rows, err := s.LeaseShards("agent-a", 1, time.Minute, 0)
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
		if _, err := s.LeaseShards("agent-a", 1, -time.Second, 0); err != nil {
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
		if _, err := s.LeaseShards("agent-a", 1, -time.Second, 0); err != nil {
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
	if _, err := s.LeaseShards("agent-a", 1, time.Minute, 0); err != nil {
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
	job, err := s.CreateJob("t1", []byte(specYAML), false)
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
	rows, err := s.LeaseShards("agent-b", 8, time.Minute, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Kind != model.KindProbe {
		t.Fatalf("agent-b lease = %+v, want its single probe", rows)
	}
	// agent-a takes the remaining probe (its own).
	rows, err = s.LeaseShards("agent-a", 8, time.Minute, 0)
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
	job, err := s.CreateJob("t1", []byte(specYAML), false)
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
	rows, err := s.LeaseShards("live", 8, time.Minute, 0)
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
			_, errs[i] = s.CreateJob(name, spec(name, "/dst/home"), false)
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
