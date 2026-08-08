// Package store is the coordinator's system of record (SQLite, WAL mode).
// State is sized by shards, not files: per-file outcomes live in journals.
// See docs/DESIGN-coordinator.md §3.
package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"drsync/coordinator/internal/model"
	drsyncpb "drsync/proto/gen/drsyncpb"
)

// MaxShardAttempts parks a shard after this many lease grants (poison guard).
const MaxShardAttempts = 5

// maxEntrylistPerAgent caps how many entry-list shards a single agent may hold
// leased at once. Each entry-list shard statx-hammers one pathological
// directory, so running many on one node destroys that node's storage
// throughput; beyond this the fleet-wide entry-list backlog waits in the queue
// until the agent finishes some (or another agent has a free slot). Other shard
// kinds (dir, chunk, verify, delete, dirfix, probe) are never capped.
const maxEntrylistPerAgent = 4

var ErrLeaseMismatch = errors.New("lease mismatch or shard not leased")

// DestinationConflictError reports a job whose destination tree overlaps the
// one being submitted or started. Callers surface Other so the operator knows
// which job to finish or cancel.
type DestinationConflictError struct {
	Other    string // the job already holding the tree
	OtherDst string
	Dst      string
}

func (e *DestinationConflictError) Error() string {
	return fmt.Sprintf("destination %q overlaps job %q (%s)", e.Dst, e.Other, e.OtherDst)
}

// JobStatesHoldingDestination are the states in which a job may still write to
// its destination. CREATED/READY have not started but are about to, and a
// PAUSED job's in-flight leases can still be draining; the terminal states
// write nothing, so their tree is free to be re-synced by another job.
var JobStatesHoldingDestination = []model.JobState{
	model.JobCreated, model.JobReady, model.JobRunning, model.JobPaused,
}

// JobStatesRunning are the states in which a job is actually executing work.
var JobStatesRunning = []model.JobState{model.JobRunning, model.JobPaused}

type Store struct {
	db  *sql.DB    // single writer connection; all mutations serialize on mu
	rdb *sql.DB    // read-only connection pool; pure reads run here, lock-free
	mu  sync.Mutex // single-writer discipline for db
}

// lockWaitWarnThreshold: diagnostic for the lease-requeue investigation
// (docs/DESIGN-agent.md §3.4) — mu is process-wide, held not just by an
// agent's own dispatch (agentsrv onWorkRequest/onShardResult/onJournalBatch)
// but by every coordinator-internal background job that writes state
// (passctrl's pass-phase advance, the lease sweeper, the Shard Reaper, the
// journal flusher). A single active agent reproduced the requeue symptom at
// high -w/-C, which rules out cross-agent contention as the cause but not
// contention with the coordinator's *own* background work sharing this same
// lock. Logged, not acted on; safe to remove once the investigation
// concludes either way.
const lockWaitWarnThreshold = 200 * time.Millisecond

// lockHoldWarnThreshold: the wait-side counterpart above only ever showed who
// was left waiting, never who they were waiting FOR — a caller that acquires
// mu instantly (so lockTimed never logs anything for it) can still sit on it
// doing real work for a long time, and every other call site pays for that as
// an unexplained wait with no caller attached. Found live: a batch of
// unrelated callers all reporting >50s waited_ms simultaneously, with no
// contention-side log line accounting for what they were actually waiting on
// — i.e. exactly the hold-side blind spot this closes.
// var, not const, so a test can shorten it rather than wait out the real value.
var lockHoldWarnThreshold = 1 * time.Second

// lockTimed acquires mu, logging a warning if the wait was long enough to be
// a candidate for stalling an agent's dispatch (and therefore its next read,
// heartbeat included) behind unrelated coordinator-internal work, and returns
// a release func that separately logs if THIS call then held the lock long
// enough to be the cause of somebody else's wait. label identifies the
// calling method so either log is actionable, not just "some caller waited"
// or "somebody held the lock a while". Every s.mu.Lock() call site in this
// package goes through this instead of a bare defer s.mu.Unlock(), so both
// sides of contention are visible fleet-wide from two log lines rather than
// needing per-call-site instrumentation at all 30+ lock sites.
func (s *Store) lockTimed(label string) func() {
	start := time.Now()
	s.mu.Lock()
	acquired := time.Now()
	if waited := acquired.Sub(start); waited > lockWaitWarnThreshold {
		slog.Warn("store: long wait for write lock",
			"caller", label, "waited_ms", waited.Milliseconds())
	}
	return func() {
		s.mu.Unlock()
		if held := time.Since(acquired); held > lockHoldWarnThreshold {
			slog.Warn("store: long write lock hold",
				"caller", label, "held_ms", held.Milliseconds())
		}
	}
}

const schema = `
CREATE TABLE IF NOT EXISTS jobs (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  name             TEXT NOT NULL UNIQUE,
  spec_yaml        BLOB NOT NULL,
  state            TEXT NOT NULL,
  dry_run          INTEGER NOT NULL DEFAULT 0,
  created_at       INTEGER NOT NULL,
  updated_at       INTEGER NOT NULL,
  -- Epoch ms of the most recent transition into RUNNING (initial start or
  -- resume from PAUSED); NULL if the job has never run. We don't track
  -- cumulative wall time or stop the clock across a pause, only the instant
  -- the job most recently started running, so "duration running" for a job
  -- still RUNNING is just now - running_since_ms.
  running_since_ms INTEGER
);
-- SchedulerCounts and QueueSummary both join shard_counts -> passes -> jobs
-- filtered on job state (running, or not-yet-complete). jobs.id and passes'
-- own (job_id, pass_no) unique constraint already index the join's other two
-- legs, but without this index the planner has nothing to seek jobs by state
-- with, and falls back to a full scan of shard_counts instead — the biggest
-- table in the join, and one with no bound on its row count: it accumulates
-- one row per (pass_id, kind, state) ever seen and is only cleared by an
-- explicit DeleteJob purge (docs/PROJECT-SUMMARY.md §10, "no automatic
-- job-history purge policy was added deliberately"). SchedulerCounts runs on
-- every Grant call (refreshed every countsTTL, scheduler.go), fleet-wide, for
-- the coordinator's entire uptime, so that scan's cost is paid continuously
-- and grows with cumulative historical shard volume, not just the current
-- job's size — the same class of gap shards_lease_expiry closed for
-- ExpireLeases, found chasing a live "leases start expiring and shards park
-- under sustained load, a drsyncd restart clears it" report at ~400M files
-- walked in one job.
CREATE INDEX IF NOT EXISTS jobs_state ON jobs (state);
CREATE TABLE IF NOT EXISTS passes (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id      INTEGER NOT NULL REFERENCES jobs(id),
  pass_no     INTEGER NOT NULL,
  state       TEXT NOT NULL,
  started_at  INTEGER,
  finished_at INTEGER,
  entries_walked  INTEGER NOT NULL DEFAULT 0,
  files_copied    INTEGER NOT NULL DEFAULT 0,
  bytes_copied    INTEGER NOT NULL DEFAULT 0,
  meta_fixed      INTEGER NOT NULL DEFAULT 0,
  orphans         INTEGER NOT NULL DEFAULT 0,
  errors          INTEGER NOT NULL DEFAULT 0,
  nlink_dup_files INTEGER NOT NULL DEFAULT 0,
  nlink_dup_bytes INTEGER NOT NULL DEFAULT 0,
  fidelity_exceptions INTEGER NOT NULL DEFAULT 0,
  verify_ok INTEGER NOT NULL DEFAULT 0,
  verify_fail INTEGER NOT NULL DEFAULT 0,
  -- Hardlink preservation (docs/DESIGN-hardlinks.md), all pass-scoped.
  links_created INTEGER NOT NULL DEFAULT 0,
  link_anchor_races INTEGER NOT NULL DEFAULT 0,
  link_fallback INTEGER NOT NULL DEFAULT 0,
  -- Historical DONE-shard total the Shard Reaper has deleted for this pass.
  -- ShardStateCounts adds this to the live DONE count from shard_counts, so a
  -- reaped pass still reports the DONE total it actually had — reaping frees
  -- the row, not the fact that the shard ran and succeeded.
  shards_reaped INTEGER NOT NULL DEFAULT 0,
  UNIQUE (job_id, pass_no)
);
-- QueueSummary's non-terminal-pass leg (see below) seeks this by state.
CREATE INDEX IF NOT EXISTS passes_state ON passes (state);
CREATE TABLE IF NOT EXISTS shards (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  pass_id         INTEGER NOT NULL REFERENCES passes(id),
  parent_shard_id INTEGER,
  kind            TEXT NOT NULL,
  rel_path        TEXT NOT NULL DEFAULT '',
  payload         BLOB,
  priority        INTEGER NOT NULL DEFAULT 0,
  state           TEXT NOT NULL,
  attempt         INTEGER NOT NULL DEFAULT 0,
  lease_id        INTEGER,
  lease_agent     TEXT,
  lease_expiry    INTEGER,
  result          BLOB,
  error           TEXT,
  target_agent    TEXT,               -- non-NULL = leasable only by this agent (probes)
  updated_at      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS shards_sched ON shards (state, priority DESC, id);
CREATE INDEX IF NOT EXISTS shards_pass  ON shards (pass_id, state);
-- ExpireLeases' select and both its follow-up UPDATEs filter on exactly
-- (state = 'leased' AND lease_expiry < ?), run fleet-wide on every sweep tick
-- (leaseTTL/3, 10s by default). shards_sched's (state, priority DESC, id)
-- narrows to the leased set but can't prune by lease_expiry, so a large
-- concurrently-leased set (tens of thousands of shards across a busy fleet —
-- plausible even outside a LINKFIX-scale event) makes every sweep scan all of
-- it looking for the handful actually expired. Measured directly: ~295ms per
-- sweep at 50K leased shards without this index, ~90µs with it — the same
-- shape of bug link_members_pending fixed for MarkLinkMembersQueued/
-- PendingLinkMembers, caught by the same kind of audit after that incident.
CREATE INDEX IF NOT EXISTS shards_lease_expiry ON shards (state, lease_expiry);

-- shard_counts is an incrementally-maintained rollup of shards by
-- (pass_id, kind, state), so the queue/pass-progress views are O(states)
-- instead of a GROUP BY over millions of shard rows. Triggers below keep it
-- exact and transactional; Open() rebuilds it from shards at startup.
CREATE TABLE IF NOT EXISTS shard_counts (
  pass_id INTEGER NOT NULL,
  kind    TEXT NOT NULL,
  state   TEXT NOT NULL,
  n       INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (pass_id, kind, state)
) WITHOUT ROWID;
-- The PK is (pass_id, kind, state), so it can't serve a lookup by state
-- alone. QueueSummary's parked-shard leg (see below) needs exactly that —
-- without this index that leg falls back to scanning shard_counts in full,
-- same failure mode jobs_state above fixes for SchedulerCounts, just on the
-- other join leg.
CREATE INDEX IF NOT EXISTS shard_counts_state ON shard_counts (state);
CREATE TRIGGER IF NOT EXISTS shard_counts_ai AFTER INSERT ON shards BEGIN
  INSERT INTO shard_counts (pass_id, kind, state, n) VALUES (NEW.pass_id, NEW.kind, NEW.state, 1)
    ON CONFLICT (pass_id, kind, state) DO UPDATE SET n = n + 1;
END;
CREATE TRIGGER IF NOT EXISTS shard_counts_ad AFTER DELETE ON shards BEGIN
  UPDATE shard_counts SET n = n - 1
    WHERE pass_id = OLD.pass_id AND kind = OLD.kind AND state = OLD.state;
END;
CREATE TRIGGER IF NOT EXISTS shard_counts_au AFTER UPDATE ON shards
  WHEN OLD.state <> NEW.state OR OLD.kind <> NEW.kind OR OLD.pass_id <> NEW.pass_id
BEGIN
  UPDATE shard_counts SET n = n - 1
    WHERE pass_id = OLD.pass_id AND kind = OLD.kind AND state = OLD.state;
  INSERT INTO shard_counts (pass_id, kind, state, n) VALUES (NEW.pass_id, NEW.kind, NEW.state, 1)
    ON CONFLICT (pass_id, kind, state) DO UPDATE SET n = n + 1;
END;
CREATE TABLE IF NOT EXISTS agents (
  id             TEXT PRIMARY KEY,
  hostname       TEXT,
  version        TEXT,
  state          TEXT NOT NULL,
  caps           BLOB,
  last_heartbeat INTEGER,
  registered_at  INTEGER,
  enabled        INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS splits (
  parent_shard_id INTEGER NOT NULL,
  seq             INTEGER NOT NULL,
  assigned_ids    TEXT NOT NULL,
  PRIMARY KEY (parent_shard_id, seq)
);
CREATE TABLE IF NOT EXISTS journal_cursors (
  pass_id   INTEGER NOT NULL,
  agent_id  TEXT NOT NULL,
  acked_seq INTEGER NOT NULL,
  PRIMARY KEY (pass_id, agent_id)
);
-- One row per large file being copied across the fleet as chunk shards. The
-- coordinator seeds n_chunks data-chunk shards, counts their completions in
-- n_done, and seeds the finalize shard (fsync+meta+rename) when they all land.
-- Keyed by (pass_id, rel_path): a pass has at most one copy of any file.
CREATE TABLE IF NOT EXISTS chunk_groups (
  pass_id   INTEGER NOT NULL,
  rel_path  TEXT NOT NULL,
  temp_name TEXT NOT NULL,
  size      INTEGER NOT NULL,
  mtime_ns  INTEGER NOT NULL,
  n_chunks  INTEGER NOT NULL,
  n_done    INTEGER NOT NULL DEFAULT 0,
  state     TEXT NOT NULL DEFAULT 'copying',  -- copying | done | aborted
  PRIMARY KEY (pass_id, rel_path)
) WITHOUT ROWID;

-- One row per pathological orphan directory being fanned out across DELETE
-- shards (docs/DESIGN-coordinator.md §2.2 DELETE fan-out) — the delete-pass
-- analogue of chunk_groups above. Unlike a chunk group, n_total is not known
-- when the row is first created: the agent streams the directory via
-- readdir and only learns the true child-shard count once it hits EOF (the
-- last ShardSplit.DeleteRemainder batch for this dir_rel carries
-- total_children; every earlier batch is 0, meaning "not yet known"). A
-- child shard's completion can race ahead of that final batch — the two are
-- independent shards/frames, only the *streaming parent's own* ShardResult
-- is ordered after every split it shipped (protocol §4.2) — so n_done is
-- incremented unconditionally on every child completion, and the "seed a
-- cleanup shard for dir_rel" decision is checked at BOTH the write that sets
-- n_total and the write that increments n_done, whichever lands last.
-- n_total = 0 doubles as "still streaming" and "impossible legitimate total"
-- (a split with zero children is never shipped), so a single comparison
-- (n_total > 0 AND n_done >= n_total) covers "closeable" without a separate
-- state column.
CREATE TABLE IF NOT EXISTS delete_groups (
  pass_id  INTEGER NOT NULL,
  rel_path TEXT NOT NULL,  -- the directory being emptied (dir_rel)
  n_total  INTEGER NOT NULL DEFAULT 0,
  n_done   INTEGER NOT NULL DEFAULT 0,
  closed   INTEGER NOT NULL DEFAULT 0,  -- 1 once the cleanup shard has been seeded (idempotency)
  PRIMARY KEY (pass_id, rel_path)
) WITHOUT ROWID;

-- link_groups/link_members correlate hardlink-group members across shards
-- (docs/DESIGN-hardlinks.md). A group is identified by (dev,ino), which is
-- unique only within one pass's walk — this is scratch state, reaped with the
-- rest of the pass's shard rows, not a durable cross-pass inode map (D3's
-- rationale survives: no *durable* global inode map is introduced).
--
-- anchor_state: pending (anchor sighted, its shard result not back yet) |
-- copied (anchor confirmed landed; members can now be linked) | fallback
-- (anchor failed, or the group exceeded hardlinks_max_group_scan — members
-- fall back to independent copies, already what happened by default).
CREATE TABLE IF NOT EXISTS link_groups (
  pass_id          INTEGER NOT NULL,
  dev              INTEGER NOT NULL,
  ino              INTEGER NOT NULL,
  nlink_expected   INTEGER NOT NULL,
  members_seen     INTEGER NOT NULL DEFAULT 0,
  anchor_rel_path  TEXT NOT NULL DEFAULT '',
  -- anchor_size/anchor_mtime_ns are the gen the anchor was copied at, carried
  -- on every LinkTask (FileGen) so a linkat re-checks the anchor has not
  -- drifted since — the same drift guard ChunkTask.gen gives chunk fan-out.
  anchor_size      INTEGER NOT NULL DEFAULT 0,
  anchor_mtime_ns  INTEGER NOT NULL DEFAULT 0,
  anchor_state     TEXT NOT NULL DEFAULT 'pending',
  updated_at       INTEGER NOT NULL,
  PRIMARY KEY (pass_id, dev, ino)
) WITHOUT ROWID;

-- One row per sighted member of a link group (including the anchor member
-- itself, state 'anchor'). state: pending (awaiting a LinkTask) | queued (a
-- LinkTask now exists for it) | linked (linkat succeeded) | anchor (this
-- member IS the group's anchor copy).
CREATE TABLE IF NOT EXISTS link_members (
  pass_id  INTEGER NOT NULL,
  dev      INTEGER NOT NULL,
  ino      INTEGER NOT NULL,
  rel_path TEXT NOT NULL,
  state    TEXT NOT NULL DEFAULT 'pending',
  PRIMARY KEY (pass_id, dev, ino, rel_path)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS link_members_group ON link_members (pass_id, dev, ino);
-- Both PendingLinkMembers (WHERE pass_id = ? AND state = 'pending') and
-- MarkLinkMembersQueued (same predicate, plus an IN(rel_path) list) filter on
-- state with no supporting index beyond the (pass_id, dev, ino, rel_path)
-- primary key — which only lets SQLite narrow to this pass_id, then scan
-- every one of its rows checking state row by row. Harmless at the
-- original one-member-at-a-time LinkTask seeding (a handful of rows to
-- scan), but LinkTaskBatch's ~2000-row batches turned this into a real
-- bottleneck: ~1.3s per MarkLinkMembersQueued call against 2.5M link_members
-- rows, run once per batch (~1250 times for a 2.5M-member pass) — over
-- 20 minutes of cumulative time holding the coordinator's single write lock,
-- stalling every agent's heartbeat renewal fleet-wide. This index lets both
-- queries seek directly to the pending rows for a pass instead of scanning
-- every row the pass has ever recorded.
CREATE INDEX IF NOT EXISTS link_members_pending ON link_members (pass_id, state);

-- journal_type_counts is the per-(pass, journal record type) histogram —
-- what "drsync journal cat --summary" reports — computed once by
-- SetJournalTypeCounts when a pass reaches COMPLETE (passctrl.advancePhase)
-- and read thereafter by /report and the completion email. Written once per
-- pass rather than incrementally: unlike shard_counts, nothing needs this
-- number *during* a pass, only after, so there is no hot path to keep exact
-- with triggers — a single INSERT after the one full journal scan a
-- completed pass ever needs is simpler and just as correct.
CREATE TABLE IF NOT EXISTS journal_type_counts (
  pass_id INTEGER NOT NULL,
  type    TEXT NOT NULL,
  n       INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (pass_id, type)
) WITHOUT ROWID;
`

func Open(path string) (*Store, error) {
	// wal_autocheckpoint=0 disables SQLite's own trigger (default: fire a
	// PASSIVE checkpoint the moment the WAL passes 1000 pages). That trigger
	// runs inline, on whichever write commits the page that crosses the
	// threshold — i.e. on this connection, under s.mu, blocking every agent's
	// grant/renew/complete call for however long copying WAL frames back to
	// the main db file takes. At million-shard scale with a large db file
	// that can be tens of ms, which compounds with this store's other s.mu
	// holders (ReapDoneShards, ExpireLeases, ordinary shard-transition
	// writes) to delay a heartbeat renewal past the lease TTL — the same
	// "degrades over time, a restart fixes it" shape as the shard_counts scan
	// (jobs_state index) and the shards-table growth (Shard Reaper) fixes:
	// accumulation, not random contention, correlated with write volume
	// rather than concurrency. A restart "fixing" it is consistent with this
	// exact mechanism — SQLite does a full checkpoint closing every
	// connection cleanly, so the WAL starts the next run empty and the
	// runaway auto-checkpoint has nothing to fire on for a while.
	// RunWALCheckpoint (below) replaces the disabled trigger with an explicit
	// PASSIVE checkpoint on a fixed interval, so the same work still happens
	// — predictably scheduled and independently observable, instead of
	// firing at whatever moment a write happens to cross 1000 WAL pages.
	base := "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)&_pragma=wal_autocheckpoint(0)"
	db, err := sql.Open("sqlite", base)
	if err != nil {
		return nil, err
	}
	// One connection: SQLite single-writer, and we serialize with s.mu anyway.
	db.SetMaxOpenConns(1)

	// auto_vacuum must be set before the schema below creates any table, and is
	// a permanent property of a database file once one exists: switching an
	// existing NONE (the SQLite default) database to INCREMENTAL requires a
	// one-time VACUUM to rewrite the file, which is why this happens ahead of
	// CREATE TABLE and is itself guarded on the current mode. Without this, the
	// Shard Reaper's deletes free pages inside the file but the file on disk
	// never shrinks — exactly the "purge doesn't help without a reset" failure
	// mode this exists to close.
	if err := ensureIncrementalVacuum(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("auto_vacuum: %w", err)
	}

	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	// Additive migrations for DBs created before a column existed. ALTER TABLE
	// ADD COLUMN is a no-op error ("duplicate column name") once applied, so we
	// swallow that and only fail on anything unexpected.
	for _, m := range migrations {
		if _, err := db.Exec(m); err != nil && !strings.Contains(err.Error(), "duplicate column") {
			db.Close()
			return nil, fmt.Errorf("migrate: %w", err)
		}
	}
	// Rebuild the shard_counts rollup from the authoritative shards table. Only
	// needed for a DB that predates the table (shard_counts empty, shards not):
	// once populated, the AFTER INSERT/UPDATE/DELETE triggers keep it exact and
	// transactional with every shard row change, clean shutdown or not, so
	// re-deriving it from scratch on every restart would just be an unindexed
	// GROUP BY over the whole shards table for no benefit — at millions of rows
	// (the scale the Shard Reaper below exists for) that cost is paid at every
	// restart for nothing.
	var countsRows, shardRows int64
	if err := db.QueryRow(`SELECT
		(SELECT COUNT(*) FROM shard_counts), (SELECT COUNT(*) FROM shards)`).
		Scan(&countsRows, &shardRows); err != nil {
		db.Close()
		return nil, fmt.Errorf("check shard_counts: %w", err)
	}
	if countsRows == 0 && shardRows > 0 {
		if _, err := db.Exec(`INSERT INTO shard_counts (pass_id, kind, state, n)
			SELECT pass_id, kind, state, COUNT(*) FROM shards GROUP BY pass_id, kind, state;`); err != nil {
			db.Close()
			return nil, fmt.Errorf("rebuild shard_counts: %w", err)
		}
	}

	// Separate read-only connection pool. WAL lets these reads run concurrently
	// with the single writer, so monitoring/poller queries never block the agent
	// grant/renew/complete hot path (which holds mu on db).
	rdb, err := sql.Open("sqlite", base+"&_pragma=query_only(true)")
	if err != nil {
		db.Close()
		return nil, err
	}
	rdb.SetMaxOpenConns(max(4, runtime.NumCPU()))
	return &Store{db: db, rdb: rdb}, nil
}

// ensureIncrementalVacuum switches the database to auto_vacuum=INCREMENTAL if
// it is not already in that mode. VACUUM rewrites the whole file, so this only
// pays that one-time cost the first time a coordinator with this code opens a
// pre-existing (auto_vacuum=NONE, the SQLite default) database; a database
// created fresh, or already migrated by an earlier run, is a no-op check.
func ensureIncrementalVacuum(db *sql.DB) error {
	var mode int
	if err := db.QueryRow(`PRAGMA auto_vacuum`).Scan(&mode); err != nil {
		return err
	}
	const incremental = 2
	if mode == incremental {
		return nil
	}
	if _, err := db.Exec(`PRAGMA auto_vacuum = INCREMENTAL`); err != nil {
		return err
	}
	// The pragma above only stages the change; VACUUM performs it and is what
	// actually reads/rewrites the whole file, so this is the expensive step
	// (once) for any database not already using auto_vacuum.
	_, err := db.Exec(`VACUUM`)
	return err
}

// migrations are additive DDL applied after the base schema; each must be
// idempotent (guarded by the duplicate-column check in Open).
var migrations = []string{
	`ALTER TABLE agents ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1`,
	`ALTER TABLE shards ADD COLUMN target_agent TEXT`,
	// 0 for rows written before the column existed, which is also the minor an
	// agent that predates minor negotiation reports — the two are equivalent.
	`ALTER TABLE agents ADD COLUMN proto_minor INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE passes ADD COLUMN shards_reaped INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE jobs ADD COLUMN running_since_ms INTEGER`,
	`ALTER TABLE passes ADD COLUMN links_created INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE passes ADD COLUMN link_anchor_races INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE passes ADD COLUMN link_fallback INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE jobs ADD COLUMN username TEXT NOT NULL DEFAULT ''`,
}

func (s *Store) Close() error {
	if s.rdb != nil {
		s.rdb.Close()
	}
	return s.db.Close()
}

func nowMS() int64 { return time.Now().UnixMilli() }

func newLeaseID() int64 {
	var b [8]byte
	rand.Read(b[:])
	return int64(binary.LittleEndian.Uint64(b[:]) >> 1) // positive
}

// ---------------------------------------------------------------------------
// Jobs
// ---------------------------------------------------------------------------

type Job struct {
	ID             int64
	Name           string
	SpecYAML       []byte
	State          model.JobState
	DryRun         bool
	Username       string
	CreatedAt      int64
	UpdatedAt      int64
	RunningSinceMs sql.NullInt64
}

// DestinationConflict returns a *DestinationConflictError if any job in one of
// states (other than excludeName) has a destination tree overlapping dst.
//
// Two jobs writing into one tree damage each other: an agent's orphan sweep
// reclaims .drsync.tmp entries it finds in the destination and recognises only
// its own job+pass as live work, so one job's chunk temp — present for the whole
// multi-host assembly of a big file — reads as stray residue to the other's walk
// of that directory and is unlinked underneath it.
func (s *Store) DestinationConflict(excludeName, dst string, states ...model.JobState) error {
	defer s.lockTimed("DestinationConflict")()
	return s.destinationConflictLocked(excludeName, dst, states)
}

func (s *Store) destinationConflictLocked(excludeName, dst string, states []model.JobState) error {
	want := make(map[model.JobState]bool, len(states))
	for _, st := range states {
		want[st] = true
	}
	rows, err := s.rdb.Query(`SELECT name, state, spec_yaml FROM jobs`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name, state string
		var specYAML []byte
		if err := rows.Scan(&name, &state, &specYAML); err != nil {
			return err
		}
		if name == excludeName || !want[model.JobState(state)] {
			continue
		}
		other, err := model.ParseSpec(specYAML)
		if err != nil {
			// A stored spec that no longer parses (downgrade, hand-edited row)
			// must not wedge every future submit; it also cannot be compared.
			slog.Warn("skipping unparsable stored spec in destination check",
				"job", name, "err", err)
			continue
		}
		if model.PathsOverlap(dst, other.Spec.Destination.Path) {
			return &DestinationConflictError{
				Other: name, OtherDst: other.Spec.Destination.Path, Dst: dst}
		}
	}
	return rows.Err()
}

// CreateJob inserts a READY job. The destination-overlap check runs under the
// same lock as the insert: checking in the caller would let two concurrent
// submits of overlapping destinations both pass before either row lands.
func (s *Store) CreateJob(name string, specYAML []byte, dryRun bool, username string) (*Job, error) {
	defer s.lockTimed("CreateJob")()
	spec, err := model.ParseSpec(specYAML)
	if err != nil {
		return nil, err
	}
	if err := s.destinationConflictLocked(name, spec.Spec.Destination.Path,
		JobStatesHoldingDestination); err != nil {
		return nil, err
	}
	now := nowMS()
	res, err := s.db.Exec(
		`INSERT INTO jobs (name, spec_yaml, state, dry_run, username, created_at, updated_at) VALUES (?,?,?,?,?,?,?)`,
		name, specYAML, string(model.JobReady), boolInt(dryRun), username, now, now)
	if err != nil {
		return nil, err
	}
	id, _ := res.LastInsertId()
	return &Job{ID: id, Name: name, SpecYAML: specYAML, State: model.JobReady,
		DryRun: dryRun, Username: username, CreatedAt: now, UpdatedAt: now}, nil
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func scanJob(row interface{ Scan(...any) error }) (*Job, error) {
	var j Job
	var dry int
	if err := row.Scan(&j.ID, &j.Name, &j.SpecYAML, &j.State, &dry, &j.Username, &j.CreatedAt, &j.UpdatedAt, &j.RunningSinceMs); err != nil {
		return nil, err
	}
	j.DryRun = dry != 0
	return &j, nil
}

const jobCols = `id, name, spec_yaml, state, dry_run, username, created_at, updated_at, running_since_ms`

func (s *Store) GetJob(name string) (*Job, error) {
	return scanJob(s.rdb.QueryRow(`SELECT `+jobCols+` FROM jobs WHERE name = ?`, name))
}

func (s *Store) GetJobByID(id int64) (*Job, error) {
	return scanJob(s.rdb.QueryRow(`SELECT `+jobCols+` FROM jobs WHERE id = ?`, id))
}

func (s *Store) ListJobs() ([]*Job, error) {
	rows, err := s.rdb.Query(`SELECT ` + jobCols + ` FROM jobs ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// JobSummary is one row of the jobs-list view: the job plus just enough pass
// rollup for a console row (latest pass and lifetime totals) without a
// per-job detail fetch.
//
// The embedded Job's SpecYAML is deliberately left nil — the list view never
// renders the spec, and reading every job's spec blob on a 2.5s poll would
// undo the point of the rollup.
type JobSummary struct {
	Job
	Passes              int
	LatestPassNo        int
	LatestPassState     model.PassState
	LatestEntriesWalked int64
	FilesCopied         int64
	BytesCopied         int64
	Errors              int64
}

// JobSummaries returns every job with its pass rollup in one query.
//
// The console renders this directly, which is why the aggregation lives here
// rather than in the caller: a list of N jobs used to cost N+1 round trips
// (list, then a detail fetch per row) every poll, and at fleet scale that
// dominated the coordinator's request load.
//
// The totals subquery and the latest-pass join are kept separate on purpose:
// mixing SUM() with a bare-column MAX() would leave the non-aggregated columns
// undefined in SQLite, so the latest pass is selected by an explicit pass_no
// match instead.
func (s *Store) JobSummaries() ([]*JobSummary, error) {
	rows, err := s.rdb.Query(`SELECT j.id, j.name, j.state,
		  j.dry_run, j.username, j.created_at, j.updated_at, j.running_since_ms,
		  COALESCE(t.npasses, 0), COALESCE(t.files, 0),
		  COALESCE(t.bytes, 0), COALESCE(t.errors, 0),
		  COALESCE(lp.pass_no, 0), COALESCE(lp.state, ''),
		  COALESCE(lp.entries_walked, 0)
		FROM jobs j
		LEFT JOIN (SELECT job_id, COUNT(*) npasses, SUM(files_copied) files,
		                  SUM(bytes_copied) bytes, SUM(errors) errors
		           FROM passes GROUP BY job_id) t ON t.job_id = j.id
		LEFT JOIN passes lp ON lp.job_id = j.id AND lp.pass_no =
		           (SELECT MAX(pass_no) FROM passes WHERE job_id = j.id)
		ORDER BY j.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*JobSummary
	for rows.Next() {
		var v JobSummary
		var dry int
		if err := rows.Scan(&v.ID, &v.Name, &v.State, &dry, &v.Username,
			&v.CreatedAt, &v.UpdatedAt, &v.RunningSinceMs, &v.Passes, &v.FilesCopied, &v.BytesCopied,
			&v.Errors, &v.LatestPassNo, &v.LatestPassState,
			&v.LatestEntriesWalked); err != nil {
			return nil, err
		}
		v.DryRun = dry != 0
		out = append(out, &v)
	}
	for _, v := range out {
		if v.LatestPassNo == 0 || v.LatestPassState == model.PassComplete {
			continue
		}
		var passID int64
		if err := s.rdb.QueryRow(`SELECT id FROM passes WHERE job_id = ? AND pass_no = ?`,
			v.ID, v.LatestPassNo).Scan(&passID); err != nil {
			continue // best-effort: fall back to the stored state
		}
		kinds, err := s.ShardKindsPresent(passID)
		if err != nil {
			continue
		}
		v.LatestPassState = v.LatestPassState.EffectiveState(kinds)
	}
	return out, rows.Err()
}

// SetJobState transitions a job's state. Entering RUNNING (initial start or
// resume from PAUSED) stamps running_since_ms with the current time; the
// console uses it to show elapsed time running, computed as now minus this
// timestamp for as long as the job stays RUNNING, so a pause doesn't need its
// own accounting — only the moment RUNNING was most recently (re)entered.
func (s *Store) SetJobState(id int64, st model.JobState) error {
	defer s.lockTimed("SetJobState")()
	var runningSince any
	if st == model.JobRunning {
		runningSince = nowMS()
	}
	_, err := s.db.Exec(`UPDATE jobs SET state = ?, updated_at = ?,
		running_since_ms = COALESCE(?, running_since_ms) WHERE id = ?`,
		string(st), nowMS(), runningSince, id)
	return err
}

// ErrJobActive is returned when a purge targets a job that has not reached a
// terminal state (COMPLETED/CANCELLED/FAILED) — deleting live work would strand
// leases and agents.
var ErrJobActive = errors.New("job is not in a terminal state")

// TerminalJobState reports whether a job in this state can be purged.
func TerminalJobState(st string) bool {
	return st == string(model.JobCompleted) || st == string(model.JobCancelled) ||
		st == string(model.JobFailed)
}

// DeleteJob removes a terminal job and all its rows (passes, shards, splits,
// journal cursors, journal type counts) in one transaction. Returns the
// deleted job's id so the caller can drop its on-disk journal segments.
// ErrJobActive if not terminal, sql.ErrNoRows if the name is unknown.
func (s *Store) DeleteJob(name string) (int64, error) {
	defer s.lockTimed("DeleteJob")()

	var id int64
	var state string
	if err := s.db.QueryRow(`SELECT id, state FROM jobs WHERE name = ?`, name).
		Scan(&id, &state); err != nil {
		return 0, err
	}
	if !TerminalJobState(state) {
		return 0, ErrJobActive
	}

	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	passSel := `SELECT id FROM passes WHERE job_id = ?`
	stmts := []string{
		`DELETE FROM journal_cursors WHERE pass_id IN (` + passSel + `)`,
		`DELETE FROM journal_type_counts WHERE pass_id IN (` + passSel + `)`,
		`DELETE FROM splits WHERE parent_shard_id IN
		   (SELECT id FROM shards WHERE pass_id IN (` + passSel + `))`,
		`DELETE FROM shards WHERE pass_id IN (` + passSel + `)`,
		`DELETE FROM shard_counts WHERE pass_id IN (` + passSel + `)`,
		`DELETE FROM chunk_groups WHERE pass_id IN (` + passSel + `)`,
		`DELETE FROM link_members WHERE pass_id IN (` + passSel + `)`,
		`DELETE FROM link_groups WHERE pass_id IN (` + passSel + `)`,
		`DELETE FROM passes WHERE job_id = ?`,
		`DELETE FROM jobs WHERE id = ?`,
	}
	args := []any{id, id, id, id, id, id, id, id, id, id}
	for i, q := range stmts {
		if _, err := tx.Exec(q, args[i]); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return id, nil
}

// ---------------------------------------------------------------------------
// Passes
// ---------------------------------------------------------------------------

type Pass struct {
	ID       int64
	JobID    int64
	PassNo   int
	State    model.PassState
	Started  sql.NullInt64
	Finished sql.NullInt64
	// Denormalized counters, accumulated from ShardResults.
	EntriesWalked, FilesCopied, BytesCopied, MetaFixed int64
	Orphans, Errors, NlinkDupFiles, NlinkDupBytes      int64
	FidelityExceptions, VerifyOK, VerifyFail           int64
	// Hardlink preservation (docs/DESIGN-hardlinks.md), all pass-scoped.
	LinksCreated, LinkAnchorRaces, LinkFallback int64
}

const passCols = `id, job_id, pass_no, state, started_at, finished_at, entries_walked,
 files_copied, bytes_copied, meta_fixed, orphans, errors, nlink_dup_files,
 nlink_dup_bytes, fidelity_exceptions, verify_ok, verify_fail,
 links_created, link_anchor_races, link_fallback`

func scanPass(row interface{ Scan(...any) error }) (*Pass, error) {
	var p Pass
	if err := row.Scan(&p.ID, &p.JobID, &p.PassNo, &p.State, &p.Started, &p.Finished,
		&p.EntriesWalked, &p.FilesCopied, &p.BytesCopied, &p.MetaFixed,
		&p.Orphans, &p.Errors, &p.NlinkDupFiles, &p.NlinkDupBytes,
		&p.FidelityExceptions, &p.VerifyOK, &p.VerifyFail,
		&p.LinksCreated, &p.LinkAnchorRaces, &p.LinkFallback); err != nil {
		return nil, err
	}
	return &p, nil
}

func (s *Store) CreatePass(jobID int64, passNo int, st model.PassState) (*Pass, error) {
	defer s.lockTimed("CreatePass")()
	res, err := s.db.Exec(
		`INSERT INTO passes (job_id, pass_no, state, started_at) VALUES (?,?,?,?)`,
		jobID, passNo, string(st), nowMS())
	if err != nil {
		return nil, err
	}
	id, _ := res.LastInsertId()
	return &Pass{ID: id, JobID: jobID, PassNo: passNo, State: st}, nil
}

// ActivePass returns the job's newest non-COMPLETE pass, or nil.
func (s *Store) ActivePass(jobID int64) (*Pass, error) {
	p, err := scanPass(s.rdb.QueryRow(`SELECT `+passCols+` FROM passes
		WHERE job_id = ? AND state != ? ORDER BY pass_no DESC LIMIT 1`,
		jobID, string(model.PassComplete)))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return p, err
}

func (s *Store) LatestPass(jobID int64) (*Pass, error) {
	p, err := scanPass(s.rdb.QueryRow(`SELECT `+passCols+` FROM passes
		WHERE job_id = ? ORDER BY pass_no DESC LIMIT 1`, jobID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return p, err
}

// PassByNo returns one specific pass of a job.
func (s *Store) PassByNo(jobID int64, passNo int) (*Pass, error) {
	return scanPass(s.rdb.QueryRow(`SELECT `+passCols+` FROM passes
		WHERE job_id = ? AND pass_no = ?`, jobID, passNo))
}

func (s *Store) ListPasses(jobID int64) ([]*Pass, error) {
	rows, err := s.rdb.Query(`SELECT `+passCols+` FROM passes WHERE job_id = ? ORDER BY pass_no`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Pass
	for rows.Next() {
		p, err := scanPass(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *Store) SetPassState(passID int64, st model.PassState) error {
	defer s.lockTimed("SetPassState")()
	var fin any
	if st == model.PassComplete {
		fin = nowMS()
	}
	_, err := s.db.Exec(`UPDATE passes SET state = ?, finished_at = COALESCE(?, finished_at) WHERE id = ?`,
		string(st), fin, passID)
	return err
}

// AccumulatePassCounters takes s.mu itself for a standalone call. Call sites
// that already hold an open tx under s.mu for a related write (shardTransition,
// CompleteDataChunk, CompleteFinalizeChunk) should use accumulatePassCountersTx
// instead, folding the counters update into that same transaction rather than
// paying for a second lock acquisition — onShardResult is the highest-volume
// call site in the coordinator (one per successful ShardResult frame from
// every agent), so this split is what let CompleteShard merge the two.
func (s *Store) AccumulatePassCounters(passID int64, c *drsyncpb.ShardCounters) error {
	if c == nil {
		return nil
	}
	defer s.lockTimed("AccumulatePassCounters")()
	return accumulatePassCountersTx(s.db, passID, c)
}

// dbOrTx is the subset of *sql.DB and *sql.Tx that accumulatePassCountersTx
// needs, so it can run either standalone (AccumulatePassCounters) or folded
// into a caller's existing transaction.
type dbOrTx interface {
	Exec(query string, args ...any) (sql.Result, error)
}

func accumulatePassCountersTx(x dbOrTx, passID int64, c *drsyncpb.ShardCounters) error {
	if c == nil {
		return nil
	}
	_, err := x.Exec(`UPDATE passes SET
		entries_walked  = entries_walked  + ?,
		files_copied    = files_copied    + ?,
		bytes_copied    = bytes_copied    + ?,
		meta_fixed      = meta_fixed      + ?,
		orphans         = orphans         + ?,
		errors          = errors          + ?,
		nlink_dup_files = nlink_dup_files + ?,
		nlink_dup_bytes = nlink_dup_bytes + ?,
		fidelity_exceptions = fidelity_exceptions + ?,
		verify_ok = verify_ok + ?,
		verify_fail = verify_fail + ?,
		links_created = links_created + ?,
		link_anchor_races = link_anchor_races + ?,
		link_fallback = link_fallback + ?
		WHERE id = ?`,
		c.EntriesWalked, c.FilesCopied, c.BytesCopied, c.MetaFixed,
		c.Orphans, c.Errors, c.NlinkDupFiles, c.NlinkDupBytes,
		c.FidelityExceptions, c.VerifyOk, c.VerifyFail,
		c.LinksCreated, c.LinkAnchorRaces, c.LinkFallback, passID)
	return err
}

// SetJournalTypeCounts persists the per-type journal record histogram for one
// pass, computed once (by the caller, from a single full scan of that pass's
// journal — see journal.Summary) at the point the pass reaches COMPLETE. A
// second call for the same pass_id (there should not be one — a pass
// completes exactly once) replaces rather than adds, since the source is a
// full recount, not a delta.
func (s *Store) SetJournalTypeCounts(passID int64, counts map[string]int64) error {
	defer s.lockTimed("SetJournalTypeCounts")()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM journal_type_counts WHERE pass_id = ?`, passID); err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT INTO journal_type_counts (pass_id, type, n) VALUES (?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for typ, n := range counts {
		if n == 0 {
			continue
		}
		if _, err := stmt.Exec(passID, typ, n); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// JournalTypeCounts sums the per-type journal histogram across every pass of
// a job — the same aggregation `drsync journal cat <name> --summary` reports
// — from the SQLite rollup rather than reading journal files, so callers on
// the request path (the /report endpoint) never pay for a journal scan.
func (s *Store) JournalTypeCounts(jobID int64) (byType map[string]int64, total int64, err error) {
	rows, err := s.rdb.Query(`SELECT jtc.type, SUM(jtc.n)
		FROM journal_type_counts jtc JOIN passes p ON p.id = jtc.pass_id
		WHERE p.job_id = ? GROUP BY jtc.type`, jobID)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	byType = map[string]int64{}
	for rows.Next() {
		var typ string
		var n int64
		if err := rows.Scan(&typ, &n); err != nil {
			return nil, 0, err
		}
		byType[typ] = n
		total += n
	}
	return byType, total, rows.Err()
}

// ---------------------------------------------------------------------------
// Shards & leases
// ---------------------------------------------------------------------------

type NewShard struct {
	Kind    model.ShardKind
	RelPath string
	Payload []byte // marshaled inner work message for non-dir kinds
	// TargetAgent pins the shard to one agent: only that agent can lease it.
	// Empty = grantable to anyone (the default). Used for probe shards, where
	// each agent must verify its own mounts.
	TargetAgent string
}

type ShardRow struct {
	ID int64
	// ParentShardID groups the sibling shards one directory fanned out into
	// (0 = none); the scheduler's per-directory fairness cap keys on it.
	ParentShardID int64
	PassID        int64
	JobID         int64
	PassNo        int
	Kind          model.ShardKind
	RelPath       string
	Payload       []byte
	Attempt       int
	LeaseID       int64
	State         model.ShardState
}

// InsertShards queues new shards for a pass (initial seed, phase task batches).
func (s *Store) InsertShards(passID, parentID int64, shards []NewShard) ([]int64, error) {
	defer s.lockTimed("InsertShards")()
	return s.insertShardsLocked(passID, parentID, shards)
}

func (s *Store) insertShardsLocked(passID, parentID int64, shards []NewShard) ([]int64, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	ids, err := insertShardsTx(tx, passID, parentID, shards)
	if err != nil {
		return nil, err
	}
	return ids, tx.Commit()
}

func insertShardsTx(tx *sql.Tx, passID, parentID int64, shards []NewShard) ([]int64, error) {
	var parent any
	if parentID > 0 {
		parent = parentID
	}
	ids := make([]int64, 0, len(shards))
	for _, ns := range shards {
		var target any
		if ns.TargetAgent != "" {
			target = ns.TargetAgent
		}
		res, err := tx.Exec(`INSERT INTO shards
			(pass_id, parent_shard_id, kind, rel_path, payload, priority, state, target_agent, updated_at)
			VALUES (?,?,?,?,?,?,?,?,?)`,
			passID, parent, string(ns.Kind), ns.RelPath, ns.Payload,
			ns.Kind.Priority(), string(model.ShardQueued), target, nowMS())
		if err != nil {
			return nil, err
		}
		id, _ := res.LastInsertId()
		ids = append(ids, id)
	}
	return ids, nil
}

// NewChunkGroup seeds a chunk_groups row alongside a big file's data-chunk
// shards. Created in the same transaction as those shards so the split stays
// atomic and idempotent (protocol doc §4.3).
type NewChunkGroup struct {
	RelPath  string
	TempName string
	Size     uint64
	MtimeNs  int64
	NChunks  int
}

// NewLinkSighting is one nlink>1 file the walker reported on a ShardSplit
// (docs/DESIGN-hardlinks.md §3.1) — a raw sighting, not yet correlated against
// any other member of its (dev,ino) group.
type NewLinkSighting struct {
	Dev, Ino uint64
	RelPath  string
	Nlink    uint32
	Size     uint64
	MtimeNs  int64
}

// recordLinkSightingsTx folds sightings into link_groups/link_members. The
// first sighting of a (pass,dev,ino) group becomes its anchor (state
// 'copied' — set here, not 'pending', because the walker copies the anchor
// speculatively inline during the same walk that reports the sighting, per
// the speculative-anchor design in docs/DESIGN-hardlinks.md §3.4; there is no
// separate "anchor confirmed" round trip to wait for). Every later sighting of
// the same group is inserted as a 'pending' member.
//
// Idempotent throughout via INSERT OR IGNORE on the (pass,dev,ino,rel_path)
// primary key of link_members: a re-sent ShardSplit (retransmit) or a member
// re-seen after its shard is re-queued records the same row twice for free,
// with no separate dedup check needed. members_seen is a straight COUNT(*)
// after each insert, not an incremented counter — a counter invites exactly
// the double-counting bug a retransmit would otherwise trigger.
//
// maxGroupScan (0 = unlimited) caps a group's member count: once the count
// exceeds it, the group flips to anchor_state 'fallback' so seedLinkfix never
// seeds a LinkTask for any of its members — they keep the independent copies
// the walker already made. The member row this sighting represents is still
// recorded either way, so the report's group-size number stays accurate.
func recordLinkSightingsTx(tx *sql.Tx, passID int64, sightings []NewLinkSighting, maxGroupScan uint64) error {
	for _, sg := range sightings {
		// SQLite INTEGER storage is 64-bit two's-complement, same as int64 —
		// but database/sql's default converter rejects a uint64 with the high
		// bit set outright ("values with high bit set are not supported"),
		// which VAST (and other filesystems with 64-bit inode allocators)
		// routinely produces. int64(x) here is a bit-preserving reinterpret,
		// not a truncation: the exact value round-trips back out via
		// uint64(int64Value) on read (see PendingLinkMembers).
		dev, ino := int64(sg.Dev), int64(sg.Ino)
		size := int64(sg.Size)

		var exists bool
		if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM link_groups
			WHERE pass_id = ? AND dev = ? AND ino = ?)`, passID, dev, ino).Scan(&exists); err != nil {
			return err
		}
		memberState := "pending"
		if !exists {
			memberState = "anchor"
			if _, err := tx.Exec(`INSERT INTO link_groups
				(pass_id, dev, ino, nlink_expected, anchor_rel_path,
				 anchor_size, anchor_mtime_ns, anchor_state, updated_at)
				VALUES (?,?,?,?,?,?,?, 'copied', ?)`,
				passID, dev, ino, sg.Nlink, sg.RelPath, size, sg.MtimeNs, nowMS()); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(`INSERT OR IGNORE INTO link_members
			(pass_id, dev, ino, rel_path, state) VALUES (?,?,?,?,?)`,
			passID, dev, ino, sg.RelPath, memberState); err != nil {
			return err
		}
		var seen uint64
		var priorState string
		if err := tx.QueryRow(`SELECT COUNT(*) FROM link_members
			WHERE pass_id = ? AND dev = ? AND ino = ?`, passID, dev, ino).Scan(&seen); err != nil {
			return err
		}
		if err := tx.QueryRow(`SELECT anchor_state FROM link_groups
			WHERE pass_id = ? AND dev = ? AND ino = ?`, passID, dev, ino).Scan(&priorState); err != nil {
			return err
		}
		fallback := maxGroupScan > 0 && seen > maxGroupScan
		if _, err := tx.Exec(`UPDATE link_groups SET members_seen = ?, updated_at = ?,
			anchor_state = CASE WHEN ? THEN 'fallback' ELSE anchor_state END
			WHERE pass_id = ? AND dev = ? AND ino = ?`,
			seen, nowMS(), fallback, passID, dev, ino); err != nil {
			return err
		}
		// Count the transition into fallback exactly once per group — not
		// every subsequent sighting of an already-fallen-back group, which
		// would inflate the report's cost signal past the actual group count.
		if fallback && priorState != "fallback" {
			if _, err := tx.Exec(`UPDATE passes SET link_fallback = link_fallback + 1
				WHERE id = ?`, passID); err != nil {
				return err
			}
		}
	}
	return nil
}

// DeleteGroupTotal is one delete-remainder batch's "this was the last batch
// for dirRel" signal (ShardSplit.DeleteRemainder.total_children — see its
// doc comment for why the total is only known on the final streamed batch,
// not upfront like a chunk group's byte-size-derived n_chunks). cleanupShard
// is pre-built by the caller (store stays payload-agnostic, same division of
// labor as CompleteDataChunk's finalizeShard) and only actually inserted if
// every sibling delete-remainder shard has already reported done by the time
// this total lands — the same race CompleteDeleteRemainder resolves from the
// other direction.
type DeleteGroupTotal struct {
	DirRel        string
	TotalChildren int
	CleanupShard  NewShard
}

// RecordSplit persists a ShardSplit idempotently: retransmits of the same
// (parent, seq) return the originally assigned ids (protocol doc §4.3). groups
// (for big files whose data-chunk shards are among shards), sightings
// (nlink>1 files reported this split), and deleteTotals (final delete-remainder
// batches — see DeleteGroupTotal) are recorded in the same transaction; pass
// nil for any that don't apply. maxGroupScan is the job's
// hardlinks_max_group_scan (0 = unlimited).
func (s *Store) RecordSplit(parentShardID int64, seq uint64, shards []NewShard,
	groups []NewChunkGroup, sightings []NewLinkSighting, maxGroupScan uint64,
	deleteTotals []DeleteGroupTotal) ([]int64, error) {
	// int64(seq): bit-preserving reinterpret, not a truncation — see
	// recordLinkSightingsTx's comment. seq is agent-assigned per-parent and in
	// practice never near 2^63, but binding a uint64 with the high bit set
	// fails outright regardless of value, so this is defensive rather than a
	// known-triggering case like dev/ino.
	seq64 := int64(seq)

	// Both pre-checks below run on s.rdb (the lock-free reader pool), before
	// s.mu is ever taken: a busy split (many sightings — recordLinkSightingsTx
	// is 5 statements per sighting) used to hold the single write lock for
	// this whole lookup-then-decide sequence, serializing every agent's
	// heartbeat/dispatch behind it (docs/DESIGN-agent.md §3.4 investigation:
	// "store: long wait for write lock" fired thousands of times with
	// RecordSplit as a top offender). Neither query needs mu — one is a plain
	// idempotency lookup, the other resolves passID — so both can run
	// concurrently with everything else and only the actual INSERTs need the
	// lock. A retransmit racing itself past this pre-check is not a new risk:
	// splits' PRIMARY KEY (parent_shard_id, seq) still catches a genuine
	// double-insert at the INSERT below, and in practice can't happen anyway
	// — one agent's frames dispatch on a single goroutine, so a second copy of
	// the same frame is only ever read after the first's RecordSplit returns.
	var idsJSON string
	err := s.rdb.QueryRow(`SELECT assigned_ids FROM splits WHERE parent_shard_id = ? AND seq = ?`,
		parentShardID, seq64).Scan(&idsJSON)
	if err == nil {
		var ids []int64
		return ids, json.Unmarshal([]byte(idsJSON), &ids)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	var passID int64
	if err := s.rdb.QueryRow(`SELECT pass_id FROM shards WHERE id = ?`, parentShardID).Scan(&passID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// The parent shard is gone: either eagerly reaped after it completed
			// (docs/DESIGN-coordinator.md §3, PR #57/#58) or dropped with its job.
			// The splits idempotency check above already ruled out "this exact
			// split was already recorded" — if we're here, this is instead an
			// outbox replay of a split the agent queued before a disconnect and
			// is resending on reconnect, for a parent shard that finished (or was
			// dropped) during the gap. There is no work to insert: whatever this
			// split would have produced either already ran (as part of the
			// parent shard's own completion) or belongs to a job that no longer
			// exists. Returning nil, nil rather than an error lets onShardSplit
			// ack the split normally instead of tearing the session down (which
			// would just cause the agent to reconnect and replay the same poison
			// frame forever — see the dispatch loop in agentsrv/server.go).
			slog.Warn("split for missing parent shard; treating as already-processed",
				"parent_shard_id", parentShardID, "seq", seq)
			return nil, nil
		}
		return nil, fmt.Errorf("split parent %d: %w", parentShardID, err)
	}

	// Only the write transaction itself needs mu: passID is a plain value (no
	// FK back to the parent shard row — parent_shard_id is never re-read once
	// assigned, see ReapDoneShards), and the pass row it names outlives any
	// one shard for the whole job, so a lock-free read of it above stays valid
	// here even if the parent shard itself is reaped in between.
	defer s.lockTimed("RecordSplit")()
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	ids, err := insertShardsTx(tx, passID, parentShardID, shards)
	if err != nil {
		return nil, err
	}
	for _, g := range groups {
		// INSERT OR IGNORE: a big file already fanned out in an earlier split
		// (e.g. the parent re-ran) keeps its original group and chunk shards.
		// int64(g.Size): see recordLinkSightingsTx's comment on why a uint64
		// with the high bit set must be reinterpreted, not passed as-is —
		// database/sql rejects it outright regardless of column type.
		if _, err := tx.Exec(`INSERT OR IGNORE INTO chunk_groups
			(pass_id, rel_path, temp_name, size, mtime_ns, n_chunks)
			VALUES (?,?,?,?,?,?)`,
			passID, g.RelPath, g.TempName, int64(g.Size), g.MtimeNs, g.NChunks); err != nil {
			return nil, err
		}
	}
	if err := recordLinkSightingsTx(tx, passID, sightings, maxGroupScan); err != nil {
		return nil, err
	}
	for _, dt := range deleteTotals {
		// INSERT OR IGNORE: a child's own completion (CompleteDeleteRemainder)
		// may have already created this row via its own INSERT OR IGNORE if
		// it raced ahead of this, the streaming parent's final batch — the
		// two are independent shards/frames, only the parent's own
		// ShardResult is ordered after every split it shipped (protocol
		// §4.2), not the split's own processing relative to its children's.
		if _, err := tx.Exec(`INSERT OR IGNORE INTO delete_groups (pass_id, rel_path) VALUES (?, ?)`,
			passID, dt.DirRel); err != nil {
			return nil, err
		}
		var nTotal, nDone, closed int
		if err := tx.QueryRow(`UPDATE delete_groups SET n_total = ?
			WHERE pass_id = ? AND rel_path = ? RETURNING n_total, n_done, closed`,
			dt.TotalChildren, passID, dt.DirRel).Scan(&nTotal, &nDone, &closed); err != nil {
			return nil, err
		}
		if closed != 0 || nDone < nTotal {
			continue // not every sibling has reported done yet, or already closed
		}
		if _, err := insertShardsTx(tx, passID, 0, []NewShard{dt.CleanupShard}); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(`UPDATE delete_groups SET closed = 1 WHERE pass_id = ? AND rel_path = ?`,
			passID, dt.DirRel); err != nil {
			return nil, err
		}
	}
	blob, _ := json.Marshal(ids)
	if _, err := tx.Exec(`INSERT INTO splits (parent_shard_id, seq, assigned_ids) VALUES (?,?,?)`,
		parentShardID, seq64, string(blob)); err != nil {
		return nil, err
	}
	return ids, tx.Commit()
}

// ShardJobPass resolves the job and pass number that own a shard. Both are
// needed to name a chunk temp: the (job, pass) pair is what an agent's orphan
// sweep compares against to tell a live temp from crash residue.
func (s *Store) ShardJobPass(shardID int64) (jobID, passNo int64, err error) {
	err = s.rdb.QueryRow(`SELECT p.job_id, p.pass_no FROM shards s
		JOIN passes p ON p.id = s.pass_id WHERE s.id = ?`, shardID).Scan(&jobID, &passNo)
	return jobID, passNo, err
}

// ChunkTemp is one chunk group's destination temp: the file's path and the
// coordinator-assigned temp name that lives in the file's parent directory.
type ChunkTemp struct {
	RelPath  string
	TempName string
}

// UnfinalizedChunkTemps lists the temps of a pass's chunk groups that never
// reached 'done' — aborted mid-assembly on source drift, in practice. Their
// temps are pure residue, but the agent orphan sweep will not reclaim a temp
// tagged with the pass it is running (that rule is what stops a re-walk from
// deleting a group's temp while its chunks are still writing), so a job whose
// last pass this is would leave them behind forever. Call only once a pass's
// scan phase has drained: with no chunk shard queued or leased, no name here
// can still be in use.
func (s *Store) UnfinalizedChunkTemps(passID int64) ([]ChunkTemp, error) {
	rows, err := s.rdb.Query(`SELECT rel_path, temp_name FROM chunk_groups
		WHERE pass_id = ? AND state != 'done'`, passID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ChunkTemp
	for rows.Next() {
		var t ChunkTemp
		if err := rows.Scan(&t.RelPath, &t.TempName); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// LinkMember is one hardlink-group member still awaiting a LinkTask, joined
// with the group's anchor path and the (size, mtime) gen it was copied at —
// everything seedLinkfix needs to build the LinkTask directly, no second query
// per row. Dev/Ino are carried through so MarkLinkMembersQueued can flip this
// exact row's state via the link_members primary key (pass_id, dev, ino,
// rel_path) — a real index seek — instead of an unindexed rel_path lookup.
type LinkMember struct {
	Dev, Ino      uint64
	RelPath       string
	AnchorRelPath string
	AnchorSize    uint64
	AnchorMtimeNs int64
}

// PendingLinkMembers lists a pass's hardlink-group members whose group anchor
// has confirmed COPIED but which have not yet been linked — the set
// seedLinkfix turns into LinkTasks at the DIRFIX→LINKFIX transition. A group
// whose anchor never reached 'copied' (fallback, or still pending — the
// latter should not happen once the scan phase has drained, since every
// sighting arrives via ShardSplit before its shard's result) contributes no
// rows: its members keep the independent copies they already made
// speculatively.
func (s *Store) PendingLinkMembers(passID int64) ([]LinkMember, error) {
	rows, err := s.rdb.Query(`
		SELECT m.dev, m.ino, m.rel_path, g.anchor_rel_path, g.anchor_size, g.anchor_mtime_ns
		FROM link_members m
		JOIN link_groups g ON g.pass_id = m.pass_id AND g.dev = m.dev AND g.ino = m.ino
		WHERE m.pass_id = ? AND m.state = 'pending' AND g.anchor_state = 'copied'`,
		passID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LinkMember
	for rows.Next() {
		var m LinkMember
		var dev, ino, anchorSize int64 // see recordLinkSightingsTx: bit-preserving reinterpret
		if err := rows.Scan(&dev, &ino, &m.RelPath, &m.AnchorRelPath, &anchorSize, &m.AnchorMtimeNs); err != nil {
			return nil, err
		}
		m.Dev = uint64(dev)
		m.Ino = uint64(ino)
		m.AnchorSize = uint64(anchorSize)
		out = append(out, m)
	}
	return out, rows.Err()
}

// LinkMemberKey identifies one link_members row by its full primary key
// (minus pass_id, supplied separately) — enough for MarkLinkMembersQueued to
// seek directly to the row instead of scanning for it by rel_path alone.
type LinkMemberKey struct {
	Dev, Ino uint64
	RelPath  string
}

// MarkLinkMembersQueued flips 'pending' members to 'queued' once seedLinkfix
// has inserted their LinkTask, so a defensive re-run of seedLinkfix (there is
// none on the current call path — advance() only reaches PendingLinkMembers
// once per pass, at the DIRFIX→LINKFIX transition — but the same table would
// otherwise silently double-seed if that ever changed) sees nothing left to do.
//
// One prepared UPDATE executed once per member, addressed by the full
// link_members primary key (pass_id, dev, ino, rel_path), inside a single
// transaction — not one UPDATE with a rel_path IN (...) list. The IN-list
// shape (kept until this was measured against a production-scale
// link_members table) cannot use any index to prune by rel_path alone
// (rel_path is only the trailing PK column, and a WITHOUT ROWID table has no
// separate rowid to index it by), so SQLite fell back to scanning every
// pending row for the pass checking each against the IN-list — ~1.3s per
// 2000-member call against 2.5M rows, run once per LinkTaskBatch flush
// (~1250 times for a 2.5M-member pass), all of it under s.mu. Per-row PK
// updates measured at ~10ms for the same 2000 rows — a ~120x improvement —
// since dev/ino narrow straight to the exact row via the primary key's own
// index, the same one link_members_group already provides.
func (s *Store) MarkLinkMembersQueued(passID int64, members []LinkMemberKey) error {
	if len(members) == 0 {
		return nil
	}
	defer s.lockTimed("MarkLinkMembersQueued")()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare(`UPDATE link_members SET state = 'queued'
		WHERE pass_id = ? AND dev = ? AND ino = ? AND rel_path = ? AND state = 'pending'`)
	if err != nil {
		return fmt.Errorf("MarkLinkMembersQueued prepare: %w", err)
	}
	defer stmt.Close()
	for _, m := range members {
		if _, err := stmt.Exec(passID, int64(m.Dev), int64(m.Ino), m.RelPath); err != nil {
			return fmt.Errorf("MarkLinkMembersQueued (members=%d): %w", len(members), err)
		}
	}
	return tx.Commit()
}

// ShardMeta returns a leased shard's pass, kind and inner payload — enough for
// the result handler to route a completion without a second round trip. Read
// before the shard transitions, so a chunk's ChunkTask payload is still there.
func (s *Store) ShardMeta(shardID int64) (passID int64, kind model.ShardKind, payload []byte, relPath string, err error) {
	var k string
	err = s.rdb.QueryRow(`SELECT pass_id, kind, payload, rel_path FROM shards WHERE id = ?`,
		shardID).Scan(&passID, &k, &payload, &relPath)
	return passID, model.ShardKind(k), payload, relPath, err
}

// CompleteDataChunk marks a data-chunk shard DONE and bumps its group's n_done.
// When that was the last chunk it inserts finalizeShard (fsync+meta+rename) in
// the SAME transaction — so a reader never observes every chunk done with no
// finalize queued, which would let passctrl advance the phase past a file that
// is not yet renamed into place. Returns whether the finalize shard was seeded.
// An aborted group (source drifted under another chunk) seeds nothing.
// counters folds the pass-counter accumulation into this same transaction —
// see CompleteShard's doc comment for why (onShardResult's chunk path is the
// other half of the same highest-volume call site).
func (s *Store) CompleteDataChunk(shardID, leaseID, passID int64, relPath string, finalizeShard NewShard, counters *drsyncpb.ShardCounters) (seeded bool, err error) {
	defer s.lockTimed("CompleteDataChunk")()
	tx, err := s.db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	n, err := execCountTx(tx, `UPDATE shards SET state = ?, lease_id = NULL, updated_at = ?
		WHERE id = ? AND state = ? AND lease_id = ?`,
		string(model.ShardDone), nowMS(), shardID, string(model.ShardLeased), leaseID)
	if err != nil {
		return false, err
	}
	if n == 0 {
		return false, ErrLeaseMismatch // stale/duplicate result; drop it
	}
	if err := accumulatePassCountersTx(tx, passID, counters); err != nil {
		return false, err
	}

	var nChunks, nDone int
	var state string
	if err := tx.QueryRow(`UPDATE chunk_groups SET n_done = n_done + 1
		WHERE pass_id = ? AND rel_path = ? RETURNING n_chunks, n_done, state`,
		passID, relPath).Scan(&nChunks, &nDone, &state); err != nil {
		return false, err
	}
	if state != "copying" || nDone < nChunks {
		return false, tx.Commit() // more chunks outstanding, or already aborted
	}
	if _, err := insertShardsTx(tx, passID, 0, []NewShard{finalizeShard}); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

// CompleteDeleteRemainder marks a split-produced delete-remainder shard DONE
// and bumps its delete_groups row's n_done — the delete-pass analogue of
// CompleteDataChunk. relPath is the directory being emptied (dir_rel), not
// this shard's own identity; see agentsrv.onShardResult for how a
// split-produced KindDelete shard is told apart from an ordinary top-level
// one. When n_done reaches n_total (known only once the streaming parent's
// final DeleteRemainder batch has landed and set it — see RecordSplit) this
// inserts cleanupShard (built by the caller: a single-entry DeleteBatch for
// relPath itself, run through the ordinary unmodified delete path — store
// stays payload-agnostic, same division of labor as CompleteDataChunk's
// finalizeShard) and marks the group closed so a re-delivered final result
// can never seed it twice. counters folds the pass-counter accumulation into
// this same transaction — see CompleteShard's doc comment for why.
func (s *Store) CompleteDeleteRemainder(shardID, leaseID, passID int64, relPath string, result []byte, cleanupShard NewShard, counters *drsyncpb.ShardCounters) error {
	defer s.lockTimed("CompleteDeleteRemainder")()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	n, err := execCountTx(tx, `UPDATE shards SET state = ?, result = COALESCE(?, result), updated_at = ?
		WHERE id = ? AND state = ? AND lease_id = ?`,
		string(model.ShardDone), result, nowMS(), shardID, string(model.ShardLeased), leaseID)
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrLeaseMismatch // stale/duplicate result; drop it
	}
	if err := accumulatePassCountersTx(tx, passID, counters); err != nil {
		return err
	}

	// The row may not exist yet if this child's completion raced ahead of
	// RecordSplit's own insert for the batch that produced it — vanishingly
	// unlikely (the grant that produced this ShardResult already required
	// the row's shard to exist) but INSERT OR IGNORE makes the ordering
	// irrelevant either way, same defensive shape as chunk_groups.
	if _, err := tx.Exec(`INSERT OR IGNORE INTO delete_groups (pass_id, rel_path) VALUES (?, ?)`,
		passID, relPath); err != nil {
		return err
	}
	var nTotal, nDone, closed int
	if err := tx.QueryRow(`UPDATE delete_groups SET n_done = n_done + 1
		WHERE pass_id = ? AND rel_path = ? RETURNING n_total, n_done, closed`,
		passID, relPath).Scan(&nTotal, &nDone, &closed); err != nil {
		return err
	}
	if closed != 0 || nTotal == 0 || nDone < nTotal {
		return tx.Commit() // already closed, total not known yet, or siblings outstanding
	}
	if _, err := insertShardsTx(tx, passID, 0, []NewShard{cleanupShard}); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE delete_groups SET closed = 1 WHERE pass_id = ? AND rel_path = ?`,
		passID, relPath); err != nil {
		return err
	}
	return tx.Commit()
}

// CompleteFinalizeChunk marks the finalize shard DONE and closes its group.
// counters folds the pass-counter accumulation into this same transaction —
// see CompleteShard's doc comment for why.
func (s *Store) CompleteFinalizeChunk(shardID, leaseID, passID int64, relPath string, counters *drsyncpb.ShardCounters) error {
	defer s.lockTimed("CompleteFinalizeChunk")()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	n, err := execCountTx(tx, `UPDATE shards SET state = ?, lease_id = NULL, updated_at = ?
		WHERE id = ? AND state = ? AND lease_id = ?`,
		string(model.ShardDone), nowMS(), shardID, string(model.ShardLeased), leaseID)
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrLeaseMismatch
	}
	if err := accumulatePassCountersTx(tx, passID, counters); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE chunk_groups SET state = 'done'
		WHERE pass_id = ? AND rel_path = ?`, passID, relPath); err != nil {
		return err
	}
	return tx.Commit()
}

// AbortChunkGroup marks a group aborted after a chunk reported the source drift
// (RESULT_SRC_CHANGED). No finalize is seeded; the half-written temp is left
// for the next walk to reclaim as .drsync.tmp residue, and the file is
// re-diffed next pass. The triggering chunk shard is marked DONE (not retried:
// re-copying this pass would race the same moving source).
func (s *Store) AbortChunkGroup(shardID, leaseID, passID int64, relPath string) error {
	defer s.lockTimed("AbortChunkGroup")()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	n, err := execCountTx(tx, `UPDATE shards SET state = ?, lease_id = NULL, updated_at = ?
		WHERE id = ? AND state = ? AND lease_id = ?`,
		string(model.ShardDone), nowMS(), shardID, string(model.ShardLeased), leaseID)
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrLeaseMismatch
	}
	if _, err := tx.Exec(`UPDATE chunk_groups SET state = 'aborted'
		WHERE pass_id = ? AND rel_path = ?`, passID, relPath); err != nil {
		return err
	}
	return tx.Commit()
}

// LeaseShards grants up to max queued shards to an agent, highest priority
// first. A retried shard softly avoids the agent whose lease last expired on
// it: it takes fresh work first, but such shards remain grantable if they are
// all that is queued. This is a preference, never a hard bar — a shard whose
// last holder is the only eligible agent (common at end-of-job as the fleet
// shrinks) is still granted rather than stranded QUEUED forever; it then either
// succeeds or climbs to MaxShardAttempts and parks. (A hard exclusion would
// freeze attempt below any cap, since attempt only advances on grant.)
//
// The preference is implemented as two index-ordered passes rather than a
// computed ORDER BY: a leading `(attempt>0 AND lease_agent=?)` sort key forces
// SQLite to sort the WHOLE queued set on every grant (a temp B-tree over
// hundreds of thousands of verify shards pegs a core under the store lock).
// Both queries below walk shards_sched (state, priority DESC, id) in order and
// stop at LIMIT.
//
// Entry-list shards carry one extra rule: a hard per-agent cap
// (maxEntrylistPerAgent). An agent already holding that many entry-list leases
// is granted no more — the rest wait QUEUED for it to finish some or for an
// agent with a free slot. This is a real quota, not a preference: an entry-list
// backlog with every agent at the cap deliberately sits in the queue rather
// than pile more storage-punishing work onto any one node.
func (s *Store) LeaseShards(agentID string, max int, ttl time.Duration) ([]*ShardRow, error) {
	if max <= 0 {
		return nil, nil
	}
	defer s.lockTimed("LeaseShards")()

	// Administratively-disabled agents stay connected and finish in-flight
	// leases (renewed by heartbeat), but are granted no new shards.
	var enabled int
	switch err := s.db.QueryRow(`SELECT enabled FROM agents WHERE id = ?`, agentID).Scan(&enabled); {
	case errors.Is(err, sql.ErrNoRows): // not yet registered — allow
	case err != nil:
		return nil, err
	case enabled == 0:
		return nil, nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// How many entry-list shards this agent already holds → how many more it may
	// take this grant. At the cap, exclude entry-list from the query entirely so
	// the grant fills with other work instead of coming back short.
	var heldEL int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM shards
		WHERE state = ? AND lease_agent = ? AND kind = ?`,
		string(model.ShardLeased), agentID, string(model.KindEntryList)).Scan(&heldEL); err != nil {
		return nil, err
	}
	elBudget := maxEntrylistPerAgent - heldEL
	if elBudget < 0 {
		elBudget = 0
	}
	notEntrylist := `AND s.kind <> '` + string(model.KindEntryList) + `' `
	elExcl := ""
	if elBudget == 0 {
		elExcl = notEntrylist
	}

	// A targeted shard (target_agent set) is leasable only by that agent; an
	// untargeted one (NULL) by anyone. Probe shards use this so each agent
	// verifies its own mounts.
	const selCols = `SELECT s.id, COALESCE(s.parent_shard_id, 0), s.pass_id, s.kind,
		s.rel_path, s.payload, s.attempt, p.job_id, p.pass_no
		FROM shards s
		JOIN passes p ON p.id = s.pass_id
		JOIN jobs   j ON j.id = p.job_id
		WHERE s.state = ? AND j.state = ? AND (s.target_agent IS NULL OR s.target_agent = ?) `

	var out []*ShardRow
	// Rows stay QUEUED until the UPDATE at the end of this function, so a later
	// pass would re-select what an earlier one already took. Dedup on the way
	// in rather than after: a shard granted twice in one lease would be run
	// twice and its lease id overwritten.
	seen := map[int64]bool{}
	scan := func(query string, args ...any) error {
		rows, err := tx.Query(query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			r := &ShardRow{State: model.ShardLeased}
			if err := rows.Scan(&r.ID, &r.ParentShardID, &r.PassID, &r.Kind, &r.RelPath,
				&r.Payload, &r.Attempt, &r.JobID, &r.PassNo); err != nil {
				return err
			}
			if seen[r.ID] {
				continue
			}
			seen[r.ID] = true
			out = append(out, r)
		}
		return rows.Err()
	}

	// Tier 1: fresh work — shards this agent did NOT last hold.
	if err = scan(selCols+elExcl+`AND NOT (s.attempt > 0 AND s.lease_agent = ?)
		ORDER BY s.priority DESC, s.id LIMIT ?`,
		string(model.ShardQueued), string(model.JobRunning), agentID, agentID, max); err != nil {
		return nil, err
	}
	// Tier 2: only if fresh work didn't fill the grant, fall back to shards this
	// agent last held (disjoint from tier 1, so no dedup needed).
	if len(out) < max {
		if err = scan(selCols+elExcl+`AND s.attempt > 0 AND s.lease_agent = ?
			ORDER BY s.priority DESC, s.id LIMIT ?`,
			string(model.ShardQueued), string(model.JobRunning), agentID, agentID,
			max-len(out)); err != nil {
			return nil, err
		}
	}

	// Enforce the entry-list budget: keep at most elBudget entry-list shards from
	// the selection, dropping the rest so they stay QUEUED. Other kinds pass
	// through untouched. (When elBudget == 0 the query already excluded
	// entry-list, so this is a no-op safety net.)
	budget := elBudget
	kept := out[:0]
	trimmed := false
	for _, r := range out {
		if r.Kind == model.KindEntryList {
			if budget <= 0 {
				trimmed = true
				continue
			}
			budget--
		}
		kept = append(kept, r)
	}
	out = kept

	// If the entry-list cap left the grant short, backfill with non-entry-list
	// work deeper in the queue that the capped selection didn't reach, so an
	// entry-list backlog at the queue head doesn't starve an agent of other work.
	if trimmed && len(out) < max {
		if err = scan(selCols+notEntrylist+`ORDER BY s.priority DESC, s.id LIMIT ?`,
			string(model.ShardQueued), string(model.JobRunning), agentID, max-len(out)); err != nil {
			return nil, err
		}
	}

	expiry := time.Now().Add(ttl).UnixMilli()
	for _, r := range out {
		r.LeaseID = newLeaseID()
		if _, err := tx.Exec(`UPDATE shards SET state = ?, attempt = attempt + 1,
			lease_id = ?, lease_agent = ?, lease_expiry = ?, updated_at = ? WHERE id = ?`,
			string(model.ShardLeased), r.LeaseID, agentID, expiry, nowMS(), r.ID); err != nil {
			return nil, err
		}
		r.Attempt++
	}
	return out, tx.Commit()
}

// RenewLeasesByID extends only the leases the agent reports still holding in
// its heartbeat (held_lease_ids), scoped to leases actually assigned to it.
//
// Renewing every lease by agent id (the old behaviour) kept alive shards the
// agent does NOT actually hold — a WorkGrant frame lost in flight, or a shard
// the agent finished whose ShardResult was dropped (it has already done
// lease_remove). Those leases would be renewed forever, so inFlight never
// reaches 0 and the pass stalls until the agent is stopped. Honouring the held
// list lets such a lease expire, so the sweeper requeues it and it is re-granted
// (re-execution is idempotent) — the stall self-heals within one TTL.
// RenewLeasesByID extends lease_expiry for leaseIDs, restricted to rows still
// LEASED and still owned by agentID. Returns how many of the requested IDs
// actually matched a row (RowsAffected) — diagnostic for the lease-requeue
// investigation (docs/DESIGN-agent.md §3.6): a heartbeat can list a lease id
// that legitimately no longer matches (state != LEASED because the shard
// already completed via a separate, faster ShardResult frame — not a bug,
// just a stale-by-a-beat snapshot), so the caller logs matched vs requested
// rather than treating any gap as automatically wrong; a *sustained* gap for
// one specific id across consecutive heartbeats is the real signal.
// One bind parameter per lease ID plus 3 fixed args. Bounded by
// AGENT_MAX_LEASES (agent/src/agent.h) + 3 in practice, well under sqlite's
// compiled-in SQLITE_MAX_VARIABLE_NUMBER (32766, modernc.org/sqlite v1.53.0).
func (s *Store) RenewLeasesByID(agentID string, leaseIDs []int64, ttl time.Duration) (matched int64, err error) {
	if len(leaseIDs) == 0 {
		return 0, nil
	}
	defer s.lockTimed("RenewLeasesByID")()
	args := make([]any, 0, len(leaseIDs)+3)
	args = append(args, time.Now().Add(ttl).UnixMilli(), string(model.ShardLeased), agentID)
	ph := make([]byte, 0, len(leaseIDs)*2)
	for i, id := range leaseIDs {
		if i > 0 {
			ph = append(ph, ',')
		}
		ph = append(ph, '?')
		args = append(args, id)
	}
	res, err := s.db.Exec(`UPDATE shards SET lease_expiry = ?
		WHERE state = ? AND lease_agent = ? AND lease_id IN (`+string(ph)+`)`, args...)
	if err != nil {
		return 0, fmt.Errorf("RenewLeasesByID agent=%s (leaseIDs=%d): %w", agentID, len(leaseIDs), err)
	}
	matched, err = res.RowsAffected()
	return matched, err
}

// ReapBatchSize bounds how many DONE shards ReapDoneShards deletes per call.
// A pass whose phase just drained can have millions of DONE rows behind it
// (state is sized by shards, and that includes finished ones until something
// reaps them); deleting them all in one transaction would hold s.mu — and
// therefore every agent's grant/renew/complete hot-path call — for however
// long that delete takes. Batching bounds each call's cost; callers loop
// (see passctrl.reapPhase) until a batch comes back short of this size, which
// is how they detect the backlog is exhausted rather than merely paused.
const ReapBatchSize = 20000

// ReapDoneShards deletes up to ReapBatchSize DONE shards of the given kinds
// for one pass, and returns how many were deleted.
//
// Callers must only pass kinds whose phase has fully drained — advance()'s
// queued+leased check (ShardStateCounts) is what proves that, at the moment
// it transitions a pass past that phase, so it is the only correct call site
// (see passctrl.advance). parent_shard_id is a plain int with no FK back to
// the parent row's own content (children never re-read it), so deleting a
// DONE parent out from under still-referencing children is safe, and
// pass-level file/byte counters live denormalized on passes
// (AccumulatePassCounters) rather than derived by re-scanning shards.
//
// A reaped shard's row in splits (if it was ever split — KindDir/KindEntryList
// only) has no other cleanup path: DeleteJob's splits delete is scoped to
// shards still present at purge time, so once the parent shard itself is gone
// its splits row would otherwise be orphaned forever. Delete it here, in the
// same transaction, while parent_shard_id still identifies which row to drop.
//
// The DONE *count* itself is not otherwise denormalized anywhere, though, and
// operator-facing views (the pass-detail API, CLI) read it live from the
// shard_counts rollup — so deleting the rows the rollup counts would zero out
// a completed pass's reported DONE total almost immediately. This bumps
// passes.shards_reaped by the same amount in the same transaction as the
// delete, so ShardStateCounts can report shards_reaped + the live DONE count
// and the total an operator sees is unaffected by whether the rows are still
// there.
func (s *Store) ReapDoneShards(passID int64, kinds []model.ShardKind) (int64, error) {
	if len(kinds) == 0 {
		return 0, nil
	}
	defer s.lockTimed("ReapDoneShards")()
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	ph := strings.TrimSuffix(strings.Repeat("?,", len(kinds)), ",")
	args := make([]any, 0, len(kinds)+3)
	args = append(args, passID, string(model.ShardDone))
	for _, k := range kinds {
		args = append(args, string(k))
	}
	args = append(args, ReapBatchSize)

	rows, err := tx.Query(`SELECT id FROM shards WHERE pass_id = ? AND state = ? AND kind IN (`+ph+`)
		LIMIT ?`, args...)
	if err != nil {
		return 0, fmt.Errorf("ReapDoneShards select (kinds=%d args=%d): %w", len(kinds), len(args), err)
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	rows.Close()
	if len(ids) == 0 {
		return 0, tx.Commit()
	}

	idPh := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	idArgs := make([]any, len(ids))
	for i, id := range ids {
		idArgs[i] = id
	}
	if _, err := tx.Exec(`DELETE FROM splits WHERE parent_shard_id IN (`+idPh+`)`, idArgs...); err != nil {
		return 0, fmt.Errorf("ReapDoneShards delete splits (ids=%d): %w", len(idArgs), err)
	}
	n, err := execCountTx(tx, `DELETE FROM shards WHERE id IN (`+idPh+`)`, idArgs...)
	if err != nil {
		return 0, fmt.Errorf("ReapDoneShards delete shards (ids=%d): %w", len(idArgs), err)
	}
	if _, err := tx.Exec(`UPDATE passes SET shards_reaped = shards_reaped + ? WHERE id = ?`,
		n, passID); err != nil {
		return 0, err
	}
	return n, tx.Commit()
}

// LinkRegistryReapBatchSize bounds how many link_groups (and each group's
// link_members rows) ReapLinkRegistry deletes per call — the same reasoning
// as ReapBatchSize: a hardlink-dense pass can leave millions of rows behind,
// and deleting them all in one transaction would hold s.mu (and therefore
// every agent's grant/renew/complete call) for however long that delete
// takes.
const LinkRegistryReapBatchSize = 20000

// ReapLinkRegistry deletes up to LinkRegistryReapBatchSize link_groups (and
// every link_members row belonging to them) for one pass, and returns how
// many groups were deleted.
//
// link_groups/link_members are disposable per-pass scratch (docs/DESIGN-
// hardlinks.md §3.5): a group is identified by (dev, ino), unique only within
// one pass's walk, not a durable cross-pass inode map. The only reader of
// either table anywhere in the coordinator is passctrl.seedLinkfix, called
// exactly once per pass at the DIRFIX->LINKFIX transition (PendingLinkMembers
// to read, MarkLinkMembersQueued to mark rows consumed) — nothing reads them
// again afterward for that pass. The only writer is recordLinkSightingsTx,
// reached from RecordSplit (the ShardSplit handler), which can only add rows
// for a pass while its SCANNING phase is still accepting split traffic — the
// same protocol invariant that makes the eager SCANNING shard reap safe
// (every ShardSplit is acked, and its sightings recorded, before that
// shard's own ShardResult is sent) means no sighting for this pass can ever
// arrive after SCANNING is proven drained, which happens well before DIRFIX
// even starts. So by the time seedLinkfix has run for a pass, both tables'
// rows for it are permanently settled: safe to delete right after the
// DIRFIX->LINKFIX SetPassState commits (see passctrl.advance), same
// placement as reapPhase(KindDirfix) there.
//
// Callers must only call this once seedLinkfix has run for the pass (i.e.
// from the DIRFIX->LINKFIX transition, after SetPassState succeeds) — same
// contract as ReapDoneShards' "only call at a phase's own transition", just
// keyed to when the registry itself was last read rather than a queued+leased
// drain check (there is no live shard state to check: the registry's
// consumer is a one-shot seed, not an ongoing queue).
func (s *Store) ReapLinkRegistry(passID int64) (int64, error) {
	defer s.lockTimed("ReapLinkRegistry")()
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`DELETE FROM link_members WHERE pass_id = ? AND (dev, ino) IN (
		SELECT dev, ino FROM link_groups WHERE pass_id = ? LIMIT ?)`,
		passID, passID, LinkRegistryReapBatchSize); err != nil {
		return 0, fmt.Errorf("ReapLinkRegistry delete link_members: %w", err)
	}
	n, err := execCountTx(tx, `DELETE FROM link_groups WHERE pass_id = ? AND (dev, ino) IN (
		SELECT dev, ino FROM link_groups WHERE pass_id = ? LIMIT ?)`,
		passID, passID, LinkRegistryReapBatchSize)
	if err != nil {
		return 0, fmt.Errorf("ReapLinkRegistry delete link_groups: %w", err)
	}
	return n, tx.Commit()
}

// incrementalVacuumPages bounds how many freed pages IncrementalVacuum
// reclaims per call — small enough that the write-lock hold is negligible
// next to a normal shard-completion write, run often enough (RunIncrementalVacuum)
// to keep pace with the Shard Reaper's steady trickle of freed pages.
const incrementalVacuumPages = 500

// IncrementalVacuum reclaims up to n freed pages back to the OS. Cheap and
// safe to call often: with auto_vacuum=INCREMENTAL (set at Open), deleting
// rows only marks their pages free in the file's internal freelist — the file
// itself does not shrink until this pragma runs. Paired with the Shard
// Reaper's deletes, this is what turns "reaped rows" into actual disk space
// given back, rather than freelist bloat the file quietly carries forever.
func (s *Store) IncrementalVacuum(n int) error {
	defer s.lockTimed("IncrementalVacuum")()
	_, err := s.db.Exec(fmt.Sprintf(`PRAGMA incremental_vacuum(%d)`, n))
	return err
}

// RunIncrementalVacuum periodically reclaims freed pages until ctx is done.
// A small, frequent pull (incrementalVacuumPages per tick) keeps the file size
// tracking what the Shard Reaper actually frees, instead of letting a large
// backlog of freed-but-unreclaimed pages build up between rare big VACUUMs.
func (s *Store) RunIncrementalVacuum(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := s.IncrementalVacuum(incrementalVacuumPages); err != nil {
				slog.Error("incremental vacuum failed", "err", err)
			}
		}
	}
}

// WALCheckpoint runs a TRUNCATE wal_checkpoint: like PASSIVE, it copies WAL
// frames back into the main db file without blocking any other connection —
// readers on s.rdb keep running, and it still checkpoints less than the full
// WAL if a reader is holding an old snapshot open, exactly like PASSIVE would
// (`busy` comes back nonzero in that case; nothing here treats that as an
// error). The difference, and the reason this isn't PASSIVE: once every frame
// is copied back, PASSIVE leaves the WAL file on disk at whatever size it
// grew to — it reclaims WAL *content*, not the *file*. Only TRUNCATE (or
// RESTART, which reclaims the file down to empty-but-still-allocated rather
// than deleting it) shrinks bytes on disk. Without this, the WAL file grows
// to its peak size under load and stays there indefinitely even though every
// checkpoint since has fully "succeeded" (checkpointed == wal_pages,
// busy == false) — this is what a live report of the WAL file matching the
// main db file's own size turned out to be: checkpointing was working
// exactly as designed, PASSIVE mode just never promised to shrink the file.
// Still takes s.mu, and NOT briefly: TRUNCATE needs an exclusive lock over
// the whole WAL to shrink the file, so this connection's own copy-back work
// runs under s.mu for as long as that takes — measured live at a 10-agent
// fleet with the original 5-minute interval at a median hold of 33s and a
// max of 144s (docs/DESIGN-agent.md's lockTimed hold-side logging is what
// caught this: every other write in the coordinator blocked for the whole
// stretch, not a rare tail). Moving this off s.mu would not remove the cost,
// only relocate it — TRUNCATE's exclusive WAL lock blocks any other
// connection's write attempt regardless of what Go-level mutex wraps this
// call, trading a blocked goroutine for a SQLITE_BUSY retry loop of similar
// wall-clock cost.
//
// Shrinking RunWALCheckpoint's interval (5min -> 20s) helped a lot (median
// hold 33s -> 4.6s) but scaled sublinearly — a 15x shorter interval only
// bought a 7x shorter hold, which means TRUNCATE has real fixed overhead
// (the file-truncate step plus its fsync) that does not shrink just because
// less WAL content has accumulated. checkpointRunPassive (called on its own,
// independent cadence, see RunWALCheckpoint) is the fix for that floor:
// PASSIVE copies WAL content back to the main db file the same as TRUNCATE
// does, without needing TRUNCATE's exclusive file-shrinking lock, so it
// holds s.mu only as long as the copy itself takes — cheap and frequent.
// TRUNCATE (this method, still what WALCheckpoint calls directly) then runs
// far less often, and by the time it does, the intervening PASSIVE calls
// have already drained most of the WAL content, so there is less left for
// TRUNCATE to redo and its own hold should be shorter still — not just
// called less often, but each call doing less work. See RunWALCheckpoint's
// doc comment and its call site in main.go for the current cadences and the
// reasoning behind them (including why the two run on independent tickers
// rather than one coupled ratio). Called on a fixed schedule now
// that wal_autocheckpoint is disabled at Open (see its comment there) — this
// is what actually keeps the WAL from growing unbounded, just on a schedule
// this process controls instead of one SQLite's internal page-count trigger
// picked. Logs at info when a checkpoint does real work (checkpointed > 0)
// so the interval and WAL growth rate can be sanity-checked from operator
// logs without extra tooling; silent when there is nothing to do, which is
// the common case between busy stretches.
func (s *Store) WALCheckpoint() error {
	return s.walCheckpoint("TRUNCATE", "WALCheckpoint")
}

// checkpointRunPassive runs the cheap, non-exclusive checkpoint mode — see
// WALCheckpoint's doc comment for why RunWALCheckpoint calls this on most
// ticks and only escalates to TRUNCATE periodically.
func (s *Store) checkpointRunPassive() error {
	return s.walCheckpoint("PASSIVE", "WALCheckpointPassive")
}

func (s *Store) walCheckpoint(mode, label string) error {
	defer s.lockTimed(label)()
	var busy, log, checkpointed int
	if err := s.db.QueryRow(`PRAGMA wal_checkpoint(`+mode+`)`).
		Scan(&busy, &log, &checkpointed); err != nil {
		return err
	}
	if checkpointed > 0 {
		slog.Info("wal checkpoint", "mode", mode, "busy", busy != 0, "wal_pages", log,
			"checkpointed_pages", checkpointed)
	}
	return nil
}

// RunWALCheckpoint periodically checkpoints the WAL until ctx is done: a
// cheap PASSIVE checkpoint every passiveEvery to keep WAL content
// continuously drained, escalating to the more expensive TRUNCATE every
// truncateEvery to reclaim the file's size on disk. See WALCheckpoint's doc
// comment for the full reasoning (this split is what brought the s.mu hold
// time down further than shortening one shared interval alone could) and the
// wal_autocheckpoint(0) comment in Open for why this exists: with SQLite's
// own auto-checkpoint trigger disabled, nothing else keeps the WAL bounded.
//
// The two cadences are independent, not one ticker with TRUNCATE riding
// every Nth PASSIVE tick (an earlier version did that): coupling them meant
// widening either cadence to fix one moved the other too. That mattered live
// — a 6-hour, 10-agent test at passiveEvery=20s/truncateEvery=200s (the
// coupled 1-tick/10-tick version) found PASSIVE's own median hold (3.4s)
// running *higher* than TRUNCATE's (2.95s), the opposite of what the split
// was for. At a 20s interval PASSIVE has very little WAL content to copy
// each call, so its hold time is dominated by fixed per-call overhead (lock
// dispatch, the PRAGMA itself) rather than actual copy work — running it 9x
// more often than truncateEvery was paying that fixed cost 9x without
// letting it amortize over more real work per call, the same floor
// TRUNCATE's own interval hit before this split existed. Decoupling lets
// passiveEvery widen (accumulate more real backlog per call, so the fixed
// cost matters less) independently of whatever truncateEvery is already
// tuned to.
func (s *Store) RunWALCheckpoint(ctx context.Context, passiveEvery, truncateEvery time.Duration) {
	passiveT := time.NewTicker(passiveEvery)
	defer passiveT.Stop()
	truncateT := time.NewTicker(truncateEvery)
	defer truncateT.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-passiveT.C:
			if err := s.checkpointRunPassive(); err != nil {
				slog.Error("wal checkpoint failed", "err", err)
			}
		case <-truncateT.C:
			if err := s.WALCheckpoint(); err != nil {
				slog.Error("wal checkpoint failed", "err", err)
			}
		}
	}
}

// ExpiredLease is one shard whose lease aged out, captured at the moment
// ExpireLeases acts on it — the detail an operator needs to tell "one flaky
// agent" from "fleet-wide" apart, which the UPDATE alone can't preserve:
// lease_agent is overwritten by the very next grant, so by the time a
// re-queued shard completes there is no durable record of which agent held
// the expired lease. See docs/DESIGN-coordinator.md §7 (Metrics).
type ExpiredLease struct {
	ShardID int64
	LeaseID int64 // the lease_id this shard held at expiry — id_lease is cleared by
	// the requeue/park UPDATE below, so like Agent this is the only place it survives.
	// Diagnostic use: grep this against an agent's "heartbeat queued ids=[...]" log
	// lines to see whether the expiring lease was ever actually reported held
	// (docs/DESIGN-agent.md §3.6).
	Job     string
	PassNo  int
	Kind    model.ShardKind
	RelPath string
	Agent   string // lease_agent at expiry time; "" if the shard was never leased (shouldn't happen)
	Attempt int    // attempt count at expiry, before any further re-grant
	Parked  bool   // true if this expiry hit the attempt ceiling (parked, not requeued)
}

// ExpireLeases re-queues shards whose lease TTL passed; shards at the attempt
// ceiling park instead. Returns the full detail of every shard acted on —
// see ExpiredLease — captured before the requeue/park UPDATE, since lease_agent
// does not survive a re-grant.
func (s *Store) ExpireLeases(now time.Time) ([]ExpiredLease, error) {
	defer s.lockTimed("ExpireLeases")()
	ms := now.UnixMilli()

	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := tx.Query(`SELECT s.id, COALESCE(s.lease_id, 0), j.name, p.pass_no, s.kind, s.rel_path,
		COALESCE(s.lease_agent, ''), s.attempt, (s.attempt >= ?)
		FROM shards s
		JOIN passes p ON p.id = s.pass_id
		JOIN jobs   j ON j.id = p.job_id
		WHERE s.state = ? AND s.lease_expiry < ?
		ORDER BY s.id`, MaxShardAttempts, string(model.ShardLeased), ms)
	if err != nil {
		return nil, err
	}
	var expired []ExpiredLease
	for rows.Next() {
		var e ExpiredLease
		if err := rows.Scan(&e.ShardID, &e.LeaseID, &e.Job, &e.PassNo, &e.Kind, &e.RelPath,
			&e.Agent, &e.Attempt, &e.Parked); err != nil {
			rows.Close()
			return nil, err
		}
		expired = append(expired, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	if len(expired) == 0 {
		return nil, tx.Commit()
	}

	if _, err := execCountTx(tx, `UPDATE shards SET state = ?, lease_id = NULL, updated_at = ?
		WHERE state = ? AND lease_expiry < ? AND attempt < ?`,
		string(model.ShardQueued), nowMS(), string(model.ShardLeased), ms, MaxShardAttempts); err != nil {
		return nil, err
	}
	if _, err := execCountTx(tx, `UPDATE shards SET state = ?, lease_id = NULL,
		error = COALESCE(error, 'attempt ceiling after lease expiry'), updated_at = ?
		WHERE state = ? AND lease_expiry < ?`,
		string(model.ShardParked), nowMS(), string(model.ShardLeased), ms); err != nil {
		return nil, err
	}
	return expired, tx.Commit()
}

func (s *Store) execCount(q string, args ...any) (int64, error) {
	res, err := s.db.Exec(q, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func execCountTx(tx *sql.Tx, q string, args ...any) (int64, error) {
	res, err := tx.Exec(q, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// shardTransition applies the shard-state UPDATE and, when counters is
// non-nil, accumulates it onto passID's running totals in the same
// transaction — one s.mu acquisition instead of two. Only CompleteShard ever
// passes counters (RequeueShard/ReleaseShard/ParkShard never accumulate); see
// its doc comment for why this merge exists.
func (s *Store) shardTransition(shardID, leaseID int64, to model.ShardState, result []byte, errMsg string, requeue bool, passID int64, counters *drsyncpb.ShardCounters) error {
	defer s.lockTimed("shardTransition:" + string(to))()
	if counters == nil {
		q := `UPDATE shards SET state = ?, result = COALESCE(?, result),
			error = COALESCE(NULLIF(?, ''), error), lease_id = NULL, updated_at = ?
			WHERE id = ? AND state = ? AND lease_id = ?`
		n, err := s.execCount(q, string(to), result, errMsg, nowMS(),
			shardID, string(model.ShardLeased), leaseID)
		if err != nil {
			return err
		}
		if n == 0 {
			return ErrLeaseMismatch
		}
		_ = requeue
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	n, err := execCountTx(tx, `UPDATE shards SET state = ?, result = COALESCE(?, result),
		error = COALESCE(NULLIF(?, ''), error), lease_id = NULL, updated_at = ?
		WHERE id = ? AND state = ? AND lease_id = ?`,
		string(to), result, errMsg, nowMS(), shardID, string(model.ShardLeased), leaseID)
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrLeaseMismatch
	}
	if err := accumulatePassCountersTx(tx, passID, counters); err != nil {
		return err
	}
	return tx.Commit()
}

// CompleteShard marks a leased shard DONE and, when counters is non-nil,
// accumulates it onto passID in the same transaction (one s.mu acquisition
// instead of the two separate CompleteShard+AccumulatePassCounters calls this
// replaced — onShardResult is the single highest-volume call site in the
// coordinator, one per successful ShardResult frame from every agent, so
// halving its lock round-trips was worth a wider signature here). Lease must
// match: stale results from an expired lease are rejected and the caller
// drops them.
func (s *Store) CompleteShard(shardID, leaseID, passID int64, result []byte, counters *drsyncpb.ShardCounters) error {
	return s.shardTransition(shardID, leaseID, model.ShardDone, result, "", false, passID, counters)
}

// RequeueShard returns a leased shard to the queue (transient failure).
func (s *Store) RequeueShard(shardID, leaseID int64, errMsg string) error {
	return s.shardTransition(shardID, leaseID, model.ShardQueued, nil, errMsg, true, 0, nil)
}

// ReleaseShard returns an unstarted shard a draining agent handed back to the
// queue. Unlike RequeueShard it records no error — the shard never ran and did
// not fail; it is simply reassigned to an active agent. Attempt is untouched
// (it only advances on grant), so a released shard is not penalised.
func (s *Store) ReleaseShard(shardID, leaseID int64) error {
	return s.shardTransition(shardID, leaseID, model.ShardQueued, nil, "", true, 0, nil)
}

// ParkShard sidelines a shard for operator attention (permanent failure).
func (s *Store) ParkShard(shardID, leaseID int64, errMsg string) error {
	return s.shardTransition(shardID, leaseID, model.ShardParked, nil, errMsg, false, 0, nil)
}

// PassOfShard resolves a shard's pass id.
func (s *Store) PassOfShard(shardID int64) (int64, error) {
	var passID int64
	err := s.rdb.QueryRow(`SELECT pass_id FROM shards WHERE id = ?`, shardID).Scan(&passID)
	return passID, err
}

// ShardStateCounts returns shard counts by state for a pass. Served from the
// maintained shard_counts rollup on the read pool — O(states), no full scan,
// no write-lock contention.
// ShardKindsPresent reports which shard kinds have any (queued/leased/done/
// parked) shards for a pass. Reporting uses this to catch a pass whose live
// queue has already moved on to the next phase's shard kind while
// passes.state has not yet been flipped to match — see PassState.EffectiveState.
func (s *Store) ShardKindsPresent(passID int64) (map[model.ShardKind]bool, error) {
	rows, err := s.rdb.Query(`SELECT kind, SUM(n) FROM shard_counts
		WHERE pass_id = ? GROUP BY kind HAVING SUM(n) > 0`, passID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[model.ShardKind]bool{}
	for rows.Next() {
		var k string
		var n int64
		if err := rows.Scan(&k, &n); err != nil {
			return nil, err
		}
		out[model.ShardKind(k)] = true
	}
	return out, rows.Err()
}

// ShardStateCounts returns shard counts by state for a pass, live rows plus
// whatever the Shard Reaper has already deleted: DONE is shard_counts' live
// DONE (queued/leased/parked shards are never reaped, so those need no such
// adjustment) plus passes.shards_reaped, so a fully-reaped completed pass
// still reports the DONE total it actually had rather than the zero its
// remaining row count would otherwise suggest.
func (s *Store) ShardStateCounts(passID int64) (map[model.ShardState]int64, error) {
	rows, err := s.rdb.Query(`SELECT state, SUM(n) FROM shard_counts
		WHERE pass_id = ? GROUP BY state HAVING SUM(n) > 0`, passID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[model.ShardState]int64{}
	for rows.Next() {
		var st string
		var n int64
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		out[model.ShardState(st)] = n
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var reaped int64
	if err := s.rdb.QueryRow(`SELECT shards_reaped FROM passes WHERE id = ?`, passID).
		Scan(&reaped); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if reaped > 0 {
		out[model.ShardDone] += reaped
	}
	return out, nil
}

// QueueRow is one (job, pass, shard-state) bucket of the global queue view.
type QueueRow struct {
	Job    string
	PassNo int
	Kind   model.ShardKind
	State  model.ShardState
	Count  int64
}

// QueueSummary reports shard counts by state across every non-terminal pass,
// plus parked shards on a pass that has otherwise completed — the
// /api/v1/queue depth view. Served from the shard_counts rollup on the read
// pool, so it is O(rollup rows) rather than a GROUP BY over every live shard.
//
// The two conditions are UNIONed rather than OR'd in one query: `p.state !=
// COMPLETE` is a negation, which SQLite cannot serve with an index seek no
// matter what exists on passes.state, so the single-query OR form always
// falls back to scanning shard_counts in full regardless of jobs_state/
// shard_counts_state below (confirmed via EXPLAIN QUERY PLAN — same failure
// mode as SchedulerCounts, just immune to that fix since it's a different
// join leg). Recast as a positive membership test — non-terminal states,
// model.NonTerminalPassStates so this can't drift from the enum — each leg
// becomes independently index-seekable: the first drives off passes_state,
// the second off shard_counts_state.
//
// A third leg adds passes.shards_reaped as a synthetic DONE row (kind
// model.KindReaped) for every non-terminal pass with a nonzero total. Since
// the eager SCANNING reap and the reap-at-phase-transition calls
// (passctrl.advance) now delete a DONE shard's row long before its phase — or
// often its whole pass — finishes, the live shard_counts DONE total alone
// understates real progress: a job walking a large tree can show a shrinking
// or flat DONE count while continuously completing work. shards_reaped is
// exactly the running total ShardStateCounts already adds back for the same
// reason (see its comment); this does the same for the queue view so job
// progress bars, the DONE tile and the DONE segment of the queue's state bar
// all read the true count instead of whatever fraction hasn't been reaped
// yet. Per-kind DONE breakdown for already-reaped shards is not
// reconstructable (shards_reaped is one running total per pass, not
// per-kind) — callers that need progress or a DONE total should sum across
// kind, which is what every current consumer does; callers that need a
// per-kind live breakdown (e.g. "what's queued right now") are unaffected,
// since only QUEUED/LEASED/PARKED rows are ever broken out that way.
func (s *Store) QueueSummary() ([]QueueRow, error) {
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

	rows, err := s.rdb.Query(`SELECT j.name, p.pass_no, sc.kind, sc.state, sc.n
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
		return nil, err
	}
	defer rows.Close()
	var out []QueueRow
	for rows.Next() {
		var r QueueRow
		if err := rows.Scan(&r.Job, &r.PassNo, &r.Kind, &r.State, &r.Count); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ParkedShard is a shard sidelined at the attempt ceiling — operator attention
// required (retry after fixing the cause, or accept the gap).
type ParkedShard struct {
	ID        int64
	Job       string
	PassNo    int
	Kind      model.ShardKind
	RelPath   string
	Attempt   int
	Error     string
	LastAgent string
	UpdatedAt int64
}

func (s *Store) ParkedShards() ([]ParkedShard, error) {
	rows, err := s.rdb.Query(`SELECT s.id, j.name, p.pass_no, s.kind, s.rel_path,
		s.attempt, COALESCE(s.error, ''), COALESCE(s.lease_agent, ''), s.updated_at
		FROM shards s
		JOIN passes p ON p.id = s.pass_id
		JOIN jobs   j ON j.id = p.job_id
		WHERE s.state = ?
		ORDER BY s.updated_at`, string(model.ShardParked))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ParkedShard
	for rows.Next() {
		var r ParkedShard
		if err := rows.Scan(&r.ID, &r.Job, &r.PassNo, &r.Kind, &r.RelPath,
			&r.Attempt, &r.Error, &r.LastAgent, &r.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ErrNotParked is returned when a retry/drop targets a shard that is not
// PARKED (unknown id, or still in flight).
var ErrNotParked = errors.New("shard not found or not parked")

// RetryParkedShard returns a single PARKED shard to the queue for a fresh
// attempt: attempt and agent affinity are reset so any agent may take it, and
// the recorded error is cleared. The shard's pass/job must still exist; a
// completed job's pass will not schedule it until the job runs again.
func (s *Store) RetryParkedShard(shardID int64) error {
	defer s.lockTimed("RetryParkedShard")()
	n, err := s.execCount(`UPDATE shards
		SET state = ?, attempt = 0, lease_id = NULL, lease_agent = NULL,
		    lease_expiry = NULL, error = NULL, updated_at = ?
		WHERE id = ? AND state = ?`,
		string(model.ShardQueued), nowMS(), shardID, string(model.ShardParked))
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotParked
	}
	return nil
}

// RetryParkedByJob requeues every PARKED shard belonging to a job. Returns the
// number requeued.
func (s *Store) RetryParkedByJob(jobName string) (int64, error) {
	defer s.lockTimed("RetryParkedByJob")()
	return s.execCount(`UPDATE shards
		SET state = ?, attempt = 0, lease_id = NULL, lease_agent = NULL,
		    lease_expiry = NULL, error = NULL, updated_at = ?
		WHERE state = ? AND pass_id IN (
			SELECT p.id FROM passes p JOIN jobs j ON j.id = p.job_id WHERE j.name = ?)`,
		string(model.ShardQueued), nowMS(), string(model.ShardParked), jobName)
}

// DropParkedShard permanently discards a single PARKED shard, accepting the
// gap it represents. This unblocks a pass that advance() is holding open on
// parked work.
func (s *Store) DropParkedShard(shardID int64) error {
	defer s.lockTimed("DropParkedShard")()
	n, err := s.execCount(`DELETE FROM shards WHERE id = ? AND state = ?`,
		shardID, string(model.ShardParked))
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotParked
	}
	return nil
}

// DropParkedByJob permanently discards every PARKED shard of a job. Returns the
// number dropped.
func (s *Store) DropParkedByJob(jobName string) (int64, error) {
	defer s.lockTimed("DropParkedByJob")()
	return s.execCount(`DELETE FROM shards
		WHERE state = ? AND pass_id IN (
			SELECT p.id FROM passes p JOIN jobs j ON j.id = p.job_id WHERE j.name = ?)`,
		string(model.ShardParked), jobName)
}

// ---------------------------------------------------------------------------
// Agents
// ---------------------------------------------------------------------------

type Agent struct {
	ID            string
	Hostname      string
	Version       string
	ProtoMinor    uint32
	State         string
	LastHeartbeat int64
	Enabled       bool
}

func (s *Store) UpsertAgent(id, hostname, version string, protoMinor uint32) error {
	defer s.lockTimed("UpsertAgent")()
	now := nowMS()
	_, err := s.db.Exec(`INSERT INTO agents (id, hostname, version, proto_minor, state, last_heartbeat, registered_at)
		VALUES (?,?,?,?,?,?,?)
		ON CONFLICT(id) DO UPDATE SET hostname=excluded.hostname,
		  version=excluded.version, proto_minor=excluded.proto_minor,
		  state='connected', last_heartbeat=excluded.last_heartbeat`,
		id, hostname, version, protoMinor, "connected", now, now)
	return err
}

func (s *Store) SetAgentState(id, state string) error {
	defer s.lockTimed("SetAgentState")()
	_, err := s.db.Exec(`UPDATE agents SET state = ? WHERE id = ?`, state, id)
	return err
}

// SetAgentEnabled flips an agent's administrative scheduling flag. A disabled
// agent stays connected but receives no new shard grants. Returns sql.ErrNoRows
// if the agent id is unknown.
func (s *Store) SetAgentEnabled(id string, enabled bool) error {
	defer s.lockTimed("SetAgentEnabled")()
	v := 0
	if enabled {
		v = 1
	}
	res, err := s.db.Exec(`UPDATE agents SET enabled = ? WHERE id = ?`, v, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// AgentHeartbeatStamp is one agent's last-heartbeat timestamp to persist, as
// batched by agentsrv's periodic flusher (see its doc comment).
type AgentHeartbeatStamp struct {
	AgentID string
	AtMs    int64
}

// TouchAgents persists a batch of agents' last_heartbeat in one transaction —
// display-only (the operator-facing agents API; nothing in scheduling or
// lease logic reads it), so batching it is purely about lock-hold time, not
// correctness: this used to be a per-agent call (TouchAgent) taking store.mu
// on every single heartbeat frame from every connected agent, which at fleet
// scale meant the write lock's next contender was almost always another
// TouchAgent, not real work. Here the whole batch runs as one transaction (a
// prepared statement executed once per stamp), so the lock is held once per
// flush interval instead of once per heartbeat.
func (s *Store) TouchAgents(stamps []AgentHeartbeatStamp) error {
	if len(stamps) == 0 {
		return nil
	}
	defer s.lockTimed("TouchAgents")()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare(`UPDATE agents SET last_heartbeat = ? WHERE id = ?`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, st := range stamps {
		if _, err := stmt.Exec(st.AtMs, st.AgentID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) ListAgents() ([]*Agent, error) {
	rows, err := s.rdb.Query(`SELECT id, hostname, version, proto_minor, state,
		COALESCE(last_heartbeat, 0), enabled FROM agents ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Agent
	for rows.Next() {
		a := &Agent{}
		var enabled int
		if err := rows.Scan(&a.ID, &a.Hostname, &a.Version, &a.ProtoMinor, &a.State,
			&a.LastHeartbeat, &enabled); err != nil {
			return nil, err
		}
		a.Enabled = enabled != 0
		out = append(out, a)
	}
	return out, rows.Err()
}

// CountSchedulableAgents counts the agents that could be granted work right
// now: connected and administratively enabled. It is the divisor for the
// scheduler's fan-out and fair-share decisions, so on success it never reports
// less than 1 — an empty fleet has nobody to spread across, and a zero divisor
// must not reach the caller.
func (s *Store) CountSchedulableAgents() (int64, error) {
	var n int64
	if err := s.rdb.QueryRow(`SELECT COUNT(*) FROM agents
		WHERE enabled = 1 AND state = ?`, "connected").Scan(&n); err != nil {
		return 0, err
	}
	return max(n, 1), nil
}

// SchedulableAgents lists the agent ids that could be granted work right now:
// connected and administratively enabled. Used to target one probe shard per
// agent at pass start. Unlike CountSchedulableAgents this reports the true set,
// so an empty fleet returns an empty slice (no probes to seed).
func (s *Store) SchedulableAgents() ([]string, error) {
	rows, err := s.rdb.Query(`SELECT id FROM agents
		WHERE enabled = 1 AND state = ? ORDER BY id`, "connected")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// PruneStaleProbes deletes still-pending probe shards whose target agent is no
// longer schedulable (disconnected or disabled). Without this a probe pinned to
// an agent that dropped after seeding would sit queued forever and stall the
// probing phase; dropping it lets the pass proceed on the agents that did
// probe. Returns the number pruned.
func (s *Store) PruneStaleProbes(passID int64) (int64, error) {
	defer s.lockTimed("PruneStaleProbes")()
	res, err := s.db.Exec(`DELETE FROM shards
		WHERE pass_id = ? AND kind = ? AND state IN (?, ?)
		  AND target_agent IS NOT NULL
		  AND target_agent NOT IN (SELECT id FROM agents WHERE enabled = 1 AND state = 'connected')`,
		passID, string(model.KindProbe), string(model.ShardQueued), string(model.ShardLeased))
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// SchedulerCounts is a snapshot of the global queue driving the scheduler's
// fan-out and fair-share decisions (docs/DESIGN-coordinator.md §4.1).
type SchedulerCounts struct {
	// WalkPending is queued+leased dir/entrylist shards across running jobs:
	// how much parallel tree-walking the fleet already has to chew on. Leased
	// shards count — an agent busy on one is not starved.
	WalkPending int64
	// Queued is grantable shards of every kind. The fair-share cap divides
	// this, so it must not be walk-only: during the verify phase the walk
	// queue is empty while hundreds of thousands of verify shards wait, and a
	// walk-only divisor would throttle every agent to one shard per poll.
	Queued int64
}

// SchedulerCounts reads both counters in one query off the trigger-maintained
// shard_counts rollup, so the cost is O(kinds × states × running passes)
// rather than a scan of the shards table.
func (s *Store) SchedulerCounts() (SchedulerCounts, error) {
	var c SchedulerCounts
	err := s.rdb.QueryRow(`SELECT
		COALESCE(SUM(CASE WHEN sc.kind IN (?,?) AND sc.state IN (?,?) THEN sc.n ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN sc.state = ? THEN sc.n ELSE 0 END), 0)
		FROM shard_counts sc
		JOIN passes p ON p.id = sc.pass_id
		JOIN jobs   j ON j.id = p.job_id
		WHERE j.state = ?`,
		string(model.KindDir), string(model.KindEntryList),
		string(model.ShardQueued), string(model.ShardLeased),
		string(model.ShardQueued), string(model.JobRunning),
	).Scan(&c.WalkPending, &c.Queued)
	return c, err
}

// ---------------------------------------------------------------------------
// Journal cursors (flow control / replay dedup)
// ---------------------------------------------------------------------------

func (s *Store) JournalCursor(passID int64, agentID string) (uint64, error) {
	defer s.lockTimed("JournalCursor")()
	// Scan into int64, not uint64: database/sql's Scan rejects a stored value
	// with the high bit set the same way Exec rejects a uint64 argument (see
	// recordLinkSightingsTx). uint64(seq) below is the matching bit-preserving
	// reinterpret back.
	var seq int64
	err := s.db.QueryRow(`SELECT acked_seq FROM journal_cursors WHERE pass_id = ? AND agent_id = ?`,
		passID, agentID).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return uint64(seq), err
}

func (s *Store) SetJournalCursor(passID int64, agentID string, seq uint64) error {
	defer s.lockTimed("SetJournalCursor")()
	_, err := s.db.Exec(`INSERT INTO journal_cursors (pass_id, agent_id, acked_seq) VALUES (?,?,?)
		ON CONFLICT(pass_id, agent_id) DO UPDATE SET acked_seq = MAX(acked_seq, excluded.acked_seq)`,
		passID, agentID, int64(seq))
	return err
}
