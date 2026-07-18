// Package store is the coordinator's system of record (SQLite, WAL mode).
// State is sized by shards, not files: per-file outcomes live in journals.
// See docs/DESIGN-coordinator.md §3.
package store

import (
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

const schema = `
CREATE TABLE IF NOT EXISTS jobs (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  name       TEXT NOT NULL UNIQUE,
  spec_yaml  BLOB NOT NULL,
  state      TEXT NOT NULL,
  dry_run    INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
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
  UNIQUE (job_id, pass_no)
);
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
`

func Open(path string) (*Store, error) {
	base := "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)"
	db, err := sql.Open("sqlite", base)
	if err != nil {
		return nil, err
	}
	// One connection: SQLite single-writer, and we serialize with s.mu anyway.
	db.SetMaxOpenConns(1)
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
	// Rebuild the shard_counts rollup from the authoritative shards table. Done
	// once at startup so an upgraded DB (or one that predates the table) is
	// consistent; the triggers keep it exact from here on.
	if _, err := db.Exec(`DELETE FROM shard_counts;
		INSERT INTO shard_counts (pass_id, kind, state, n)
		SELECT pass_id, kind, state, COUNT(*) FROM shards GROUP BY pass_id, kind, state;`); err != nil {
		db.Close()
		return nil, fmt.Errorf("rebuild shard_counts: %w", err)
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

// migrations are additive DDL applied after the base schema; each must be
// idempotent (guarded by the duplicate-column check in Open).
var migrations = []string{
	`ALTER TABLE agents ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1`,
	`ALTER TABLE shards ADD COLUMN target_agent TEXT`,
	// 0 for rows written before the column existed, which is also the minor an
	// agent that predates minor negotiation reports — the two are equivalent.
	`ALTER TABLE agents ADD COLUMN proto_minor INTEGER NOT NULL DEFAULT 0`,
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
	ID        int64
	Name      string
	SpecYAML  []byte
	State     model.JobState
	DryRun    bool
	CreatedAt int64
	UpdatedAt int64
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
	s.mu.Lock()
	defer s.mu.Unlock()
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
func (s *Store) CreateJob(name string, specYAML []byte, dryRun bool) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
		`INSERT INTO jobs (name, spec_yaml, state, dry_run, created_at, updated_at) VALUES (?,?,?,?,?,?)`,
		name, specYAML, string(model.JobReady), boolInt(dryRun), now, now)
	if err != nil {
		return nil, err
	}
	id, _ := res.LastInsertId()
	return &Job{ID: id, Name: name, SpecYAML: specYAML, State: model.JobReady,
		DryRun: dryRun, CreatedAt: now, UpdatedAt: now}, nil
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
	if err := row.Scan(&j.ID, &j.Name, &j.SpecYAML, &j.State, &dry, &j.CreatedAt, &j.UpdatedAt); err != nil {
		return nil, err
	}
	j.DryRun = dry != 0
	return &j, nil
}

const jobCols = `id, name, spec_yaml, state, dry_run, created_at, updated_at`

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

func (s *Store) SetJobState(id int64, st model.JobState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`UPDATE jobs SET state = ?, updated_at = ? WHERE id = ?`,
		string(st), nowMS(), id)
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
// journal cursors) in one transaction. Returns the deleted job's id so the
// caller can drop its on-disk journal segments. ErrJobActive if not terminal,
// sql.ErrNoRows if the name is unknown.
func (s *Store) DeleteJob(name string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

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
		`DELETE FROM splits WHERE parent_shard_id IN
		   (SELECT id FROM shards WHERE pass_id IN (` + passSel + `))`,
		`DELETE FROM shards WHERE pass_id IN (` + passSel + `)`,
		`DELETE FROM shard_counts WHERE pass_id IN (` + passSel + `)`,
		`DELETE FROM chunk_groups WHERE pass_id IN (` + passSel + `)`,
		`DELETE FROM passes WHERE job_id = ?`,
		`DELETE FROM jobs WHERE id = ?`,
	}
	args := []any{id, id, id, id, id, id, id}
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
}

const passCols = `id, job_id, pass_no, state, started_at, finished_at, entries_walked,
 files_copied, bytes_copied, meta_fixed, orphans, errors, nlink_dup_files,
 nlink_dup_bytes, fidelity_exceptions, verify_ok, verify_fail`

func scanPass(row interface{ Scan(...any) error }) (*Pass, error) {
	var p Pass
	if err := row.Scan(&p.ID, &p.JobID, &p.PassNo, &p.State, &p.Started, &p.Finished,
		&p.EntriesWalked, &p.FilesCopied, &p.BytesCopied, &p.MetaFixed,
		&p.Orphans, &p.Errors, &p.NlinkDupFiles, &p.NlinkDupBytes,
		&p.FidelityExceptions, &p.VerifyOK, &p.VerifyFail); err != nil {
		return nil, err
	}
	return &p, nil
}

func (s *Store) CreatePass(jobID int64, passNo int, st model.PassState) (*Pass, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	s.mu.Lock()
	defer s.mu.Unlock()
	var fin any
	if st == model.PassComplete {
		fin = nowMS()
	}
	_, err := s.db.Exec(`UPDATE passes SET state = ?, finished_at = COALESCE(?, finished_at) WHERE id = ?`,
		string(st), fin, passID)
	return err
}

func (s *Store) AccumulatePassCounters(passID int64, c *drsyncpb.ShardCounters) error {
	if c == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`UPDATE passes SET
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
		verify_fail = verify_fail + ?
		WHERE id = ?`,
		c.EntriesWalked, c.FilesCopied, c.BytesCopied, c.MetaFixed,
		c.Orphans, c.Errors, c.NlinkDupFiles, c.NlinkDupBytes,
		c.FidelityExceptions, c.VerifyOk, c.VerifyFail, passID)
	return err
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
	ID      int64
	PassID  int64
	JobID   int64
	PassNo  int
	Kind    model.ShardKind
	RelPath string
	Payload []byte
	Attempt int
	LeaseID int64
	State   model.ShardState
}

// InsertShards queues new shards for a pass (initial seed, phase task batches).
func (s *Store) InsertShards(passID, parentID int64, shards []NewShard) ([]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
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

// RecordSplit persists a ShardSplit idempotently: retransmits of the same
// (parent, seq) return the originally assigned ids (protocol doc §4.3). groups
// (for big files whose data-chunk shards are among shards) are created in the
// same transaction; pass nil when there are none.
func (s *Store) RecordSplit(parentShardID int64, seq uint64, shards []NewShard, groups []NewChunkGroup) ([]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var idsJSON string
	err := s.db.QueryRow(`SELECT assigned_ids FROM splits WHERE parent_shard_id = ? AND seq = ?`,
		parentShardID, seq).Scan(&idsJSON)
	if err == nil {
		var ids []int64
		return ids, json.Unmarshal([]byte(idsJSON), &ids)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	var passID int64
	if err := s.db.QueryRow(`SELECT pass_id FROM shards WHERE id = ?`, parentShardID).Scan(&passID); err != nil {
		return nil, fmt.Errorf("split parent %d: %w", parentShardID, err)
	}
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
		if _, err := tx.Exec(`INSERT OR IGNORE INTO chunk_groups
			(pass_id, rel_path, temp_name, size, mtime_ns, n_chunks)
			VALUES (?,?,?,?,?,?)`,
			passID, g.RelPath, g.TempName, g.Size, g.MtimeNs, g.NChunks); err != nil {
			return nil, err
		}
	}
	blob, _ := json.Marshal(ids)
	if _, err := tx.Exec(`INSERT INTO splits (parent_shard_id, seq, assigned_ids) VALUES (?,?,?)`,
		parentShardID, seq, string(blob)); err != nil {
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

// ShardMeta returns a leased shard's pass, kind and inner payload — enough for
// the result handler to route a completion without a second round trip. Read
// before the shard transitions, so a chunk's ChunkTask payload is still there.
func (s *Store) ShardMeta(shardID int64) (passID int64, kind model.ShardKind, payload []byte, err error) {
	var k string
	err = s.rdb.QueryRow(`SELECT pass_id, kind, payload FROM shards WHERE id = ?`,
		shardID).Scan(&passID, &k, &payload)
	return passID, model.ShardKind(k), payload, err
}

// CompleteDataChunk marks a data-chunk shard DONE and bumps its group's n_done.
// When that was the last chunk it inserts finalizeShard (fsync+meta+rename) in
// the SAME transaction — so a reader never observes every chunk done with no
// finalize queued, which would let passctrl advance the phase past a file that
// is not yet renamed into place. Returns whether the finalize shard was seeded.
// An aborted group (source drifted under another chunk) seeds nothing.
func (s *Store) CompleteDataChunk(shardID, leaseID, passID int64, relPath string, finalizeShard NewShard) (seeded bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
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

// CompleteFinalizeChunk marks the finalize shard DONE and closes its group.
func (s *Store) CompleteFinalizeChunk(shardID, leaseID, passID int64, relPath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	s.mu.Lock()
	defer s.mu.Unlock()
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
func (s *Store) LeaseShards(agentID string, max int, ttl time.Duration) ([]*ShardRow, error) {
	if max <= 0 {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

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

	// A targeted shard (target_agent set) is leasable only by that agent; an
	// untargeted one (NULL) by anyone. Probe shards use this so each agent
	// verifies its own mounts.
	const selCols = `SELECT s.id, s.pass_id, s.kind, s.rel_path, s.payload, s.attempt, p.job_id, p.pass_no
		FROM shards s
		JOIN passes p ON p.id = s.pass_id
		JOIN jobs   j ON j.id = p.job_id
		WHERE s.state = ? AND j.state = ? AND (s.target_agent IS NULL OR s.target_agent = ?) `

	var out []*ShardRow
	scan := func(query string, args ...any) error {
		rows, err := tx.Query(query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			r := &ShardRow{State: model.ShardLeased}
			if err := rows.Scan(&r.ID, &r.PassID, &r.Kind, &r.RelPath, &r.Payload, &r.Attempt, &r.JobID, &r.PassNo); err != nil {
				return err
			}
			out = append(out, r)
		}
		return rows.Err()
	}

	// Tier 1: fresh work — shards this agent did NOT last hold.
	if err := scan(selCols+`AND NOT (s.attempt > 0 AND s.lease_agent = ?)
		ORDER BY s.priority DESC, s.id LIMIT ?`,
		string(model.ShardQueued), string(model.JobRunning), agentID, agentID, max); err != nil {
		return nil, err
	}
	// Tier 2: only if fresh work didn't fill the grant, fall back to shards this
	// agent last held (disjoint from tier 1, so no dedup needed) — never strand.
	if len(out) < max {
		if err := scan(selCols+`AND s.attempt > 0 AND s.lease_agent = ?
			ORDER BY s.priority DESC, s.id LIMIT ?`,
			string(model.ShardQueued), string(model.JobRunning), agentID, agentID, max-len(out)); err != nil {
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
func (s *Store) RenewLeasesByID(agentID string, leaseIDs []int64, ttl time.Duration) error {
	if len(leaseIDs) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
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
	_, err := s.db.Exec(`UPDATE shards SET lease_expiry = ?
		WHERE state = ? AND lease_agent = ? AND lease_id IN (`+string(ph)+`)`, args...)
	return err
}

// ExpireLeases re-queues shards whose lease TTL passed; shards at the attempt
// ceiling park instead. Returns (requeued, parked).
func (s *Store) ExpireLeases(now time.Time) (int64, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ms := now.UnixMilli()
	requeued, err := s.execCount(`UPDATE shards SET state = ?, lease_id = NULL, updated_at = ?
		WHERE state = ? AND lease_expiry < ? AND attempt < ?`,
		string(model.ShardQueued), nowMS(), string(model.ShardLeased), ms, MaxShardAttempts)
	if err != nil {
		return 0, 0, err
	}
	parked, err := s.execCount(`UPDATE shards SET state = ?, lease_id = NULL,
		error = COALESCE(error, 'attempt ceiling after lease expiry'), updated_at = ?
		WHERE state = ? AND lease_expiry < ?`,
		string(model.ShardParked), nowMS(), string(model.ShardLeased), ms)
	if err != nil {
		return 0, 0, err
	}
	return requeued, parked, nil
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

func (s *Store) shardTransition(shardID, leaseID int64, to model.ShardState, result []byte, errMsg string, requeue bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
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

// CompleteShard marks a leased shard DONE (lease must match: stale results
// from an expired lease are rejected and the caller drops them).
func (s *Store) CompleteShard(shardID, leaseID int64, result []byte) error {
	return s.shardTransition(shardID, leaseID, model.ShardDone, result, "", false)
}

// RequeueShard returns a leased shard to the queue (transient failure).
func (s *Store) RequeueShard(shardID, leaseID int64, errMsg string) error {
	return s.shardTransition(shardID, leaseID, model.ShardQueued, nil, errMsg, true)
}

// ParkShard sidelines a shard for operator attention (permanent failure).
func (s *Store) ParkShard(shardID, leaseID int64, errMsg string) error {
	return s.shardTransition(shardID, leaseID, model.ShardParked, nil, errMsg, false)
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
	return out, rows.Err()
}

// QueueRow is one (job, pass, shard-state) bucket of the global queue view.
type QueueRow struct {
	Job    string
	PassNo int
	Kind   model.ShardKind
	State  model.ShardState
	Count  int64
}

// QueueSummary reports shard counts by state across every non-terminal pass —
// the /api/v1/queue depth view. Served from the shard_counts rollup on the read
// pool, so it is O(rollup rows) rather than a GROUP BY over every live shard.
func (s *Store) QueueSummary() ([]QueueRow, error) {
	rows, err := s.rdb.Query(`SELECT j.name, p.pass_no, sc.kind, sc.state, sc.n
		FROM shard_counts sc
		JOIN passes p ON p.id = sc.pass_id
		JOIN jobs   j ON j.id = p.job_id
		WHERE sc.n > 0 AND (p.state != ? OR sc.state = ?)
		ORDER BY j.name, p.pass_no`,
		string(model.PassComplete), string(model.ShardParked))
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
	s.mu.Lock()
	defer s.mu.Unlock()
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
	s.mu.Lock()
	defer s.mu.Unlock()
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
	s.mu.Lock()
	defer s.mu.Unlock()
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
	s.mu.Lock()
	defer s.mu.Unlock()
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
	s.mu.Lock()
	defer s.mu.Unlock()
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
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`UPDATE agents SET state = ? WHERE id = ?`, state, id)
	return err
}

// SetAgentEnabled flips an agent's administrative scheduling flag. A disabled
// agent stays connected but receives no new shard grants. Returns sql.ErrNoRows
// if the agent id is unknown.
func (s *Store) SetAgentEnabled(id string, enabled bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
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

func (s *Store) TouchAgent(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`UPDATE agents SET last_heartbeat = ? WHERE id = ?`, nowMS(), id)
	return err
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
	s.mu.Lock()
	defer s.mu.Unlock()
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
	s.mu.Lock()
	defer s.mu.Unlock()
	var seq uint64
	err := s.db.QueryRow(`SELECT acked_seq FROM journal_cursors WHERE pass_id = ? AND agent_id = ?`,
		passID, agentID).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return seq, err
}

func (s *Store) SetJournalCursor(passID int64, agentID string, seq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`INSERT INTO journal_cursors (pass_id, agent_id, acked_seq) VALUES (?,?,?)
		ON CONFLICT(pass_id, agent_id) DO UPDATE SET acked_seq = MAX(acked_seq, excluded.acked_seq)`,
		passID, agentID, seq)
	return err
}
