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

type Store struct {
	db *sql.DB
	mu sync.Mutex // single-writer discipline; reads share the same conn
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
  updated_at      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS shards_sched ON shards (state, priority DESC, id);
CREATE INDEX IF NOT EXISTS shards_pass  ON shards (pass_id, state);
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
`

func Open(path string) (*Store, error) {
	dsn := "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)"
	db, err := sql.Open("sqlite", dsn)
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
	return &Store{db: db}, nil
}

// migrations are additive DDL applied after the base schema; each must be
// idempotent (guarded by the duplicate-column check in Open).
var migrations = []string{
	`ALTER TABLE agents ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1`,
}

func (s *Store) Close() error { return s.db.Close() }

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

func (s *Store) CreateJob(name string, specYAML []byte, dryRun bool) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	s.mu.Lock()
	defer s.mu.Unlock()
	return scanJob(s.db.QueryRow(`SELECT `+jobCols+` FROM jobs WHERE name = ?`, name))
}

func (s *Store) GetJobByID(id int64) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return scanJob(s.db.QueryRow(`SELECT `+jobCols+` FROM jobs WHERE id = ?`, id))
}

func (s *Store) ListJobs() ([]*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`SELECT ` + jobCols + ` FROM jobs ORDER BY id`)
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
		`DELETE FROM passes WHERE job_id = ?`,
		`DELETE FROM jobs WHERE id = ?`,
	}
	args := []any{id, id, id, id, id}
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
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := scanPass(s.db.QueryRow(`SELECT `+passCols+` FROM passes
		WHERE job_id = ? AND state != ? ORDER BY pass_no DESC LIMIT 1`,
		jobID, string(model.PassComplete)))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return p, err
}

func (s *Store) LatestPass(jobID int64) (*Pass, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := scanPass(s.db.QueryRow(`SELECT `+passCols+` FROM passes
		WHERE job_id = ? ORDER BY pass_no DESC LIMIT 1`, jobID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return p, err
}

// PassByNo returns one specific pass of a job.
func (s *Store) PassByNo(jobID int64, passNo int) (*Pass, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return scanPass(s.db.QueryRow(`SELECT `+passCols+` FROM passes
		WHERE job_id = ? AND pass_no = ?`, jobID, passNo))
}

func (s *Store) ListPasses(jobID int64) ([]*Pass, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`SELECT `+passCols+` FROM passes WHERE job_id = ? ORDER BY pass_no`, jobID)
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
		res, err := tx.Exec(`INSERT INTO shards
			(pass_id, parent_shard_id, kind, rel_path, payload, priority, state, updated_at)
			VALUES (?,?,?,?,?,?,?,?)`,
			passID, parent, string(ns.Kind), ns.RelPath, ns.Payload,
			ns.Kind.Priority(), string(model.ShardQueued), nowMS())
		if err != nil {
			return nil, err
		}
		id, _ := res.LastInsertId()
		ids = append(ids, id)
	}
	return ids, nil
}

// RecordSplit persists a ShardSplit idempotently: retransmits of the same
// (parent, seq) return the originally assigned ids (protocol doc §4.3).
func (s *Store) RecordSplit(parentShardID int64, seq uint64, shards []NewShard) ([]int64, error) {
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
	blob, _ := json.Marshal(ids)
	if _, err := tx.Exec(`INSERT INTO splits (parent_shard_id, seq, assigned_ids) VALUES (?,?,?)`,
		parentShardID, seq, string(blob)); err != nil {
		return nil, err
	}
	return ids, tx.Commit()
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

	const selCols = `SELECT s.id, s.pass_id, s.kind, s.rel_path, s.payload, s.attempt, p.job_id, p.pass_no
		FROM shards s
		JOIN passes p ON p.id = s.pass_id
		JOIN jobs   j ON j.id = p.job_id
		WHERE s.state = ? AND j.state = ? `

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
		string(model.ShardQueued), string(model.JobRunning), agentID, max); err != nil {
		return nil, err
	}
	// Tier 2: only if fresh work didn't fill the grant, fall back to shards this
	// agent last held (disjoint from tier 1, so no dedup needed) — never strand.
	if len(out) < max {
		if err := scan(selCols+`AND s.attempt > 0 AND s.lease_agent = ?
			ORDER BY s.priority DESC, s.id LIMIT ?`,
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

// RenewLeases extends every lease held by the agent (heartbeat side effect).
func (s *Store) RenewLeases(agentID string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`UPDATE shards SET lease_expiry = ? WHERE state = ? AND lease_agent = ?`,
		time.Now().Add(ttl).UnixMilli(), string(model.ShardLeased), agentID)
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
	s.mu.Lock()
	defer s.mu.Unlock()
	var passID int64
	err := s.db.QueryRow(`SELECT pass_id FROM shards WHERE id = ?`, shardID).Scan(&passID)
	return passID, err
}

// ShardStateCounts returns shard counts by state for a pass.
func (s *Store) ShardStateCounts(passID int64) (map[model.ShardState]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`SELECT state, COUNT(*) FROM shards WHERE pass_id = ? GROUP BY state`, passID)
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
// the /api/v1/queue depth view.
func (s *Store) QueueSummary() ([]QueueRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`SELECT j.name, p.pass_no, s.kind, s.state, COUNT(*)
		FROM shards s
		JOIN passes p ON p.id = s.pass_id
		JOIN jobs   j ON j.id = p.job_id
		WHERE p.state != ? OR s.state = ?
		GROUP BY j.name, p.pass_no, s.kind, s.state
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
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`SELECT s.id, j.name, p.pass_no, s.kind, s.rel_path,
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
	State         string
	LastHeartbeat int64
	Enabled       bool
}

func (s *Store) UpsertAgent(id, hostname, version string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := nowMS()
	_, err := s.db.Exec(`INSERT INTO agents (id, hostname, version, state, last_heartbeat, registered_at)
		VALUES (?,?,?,?,?,?)
		ON CONFLICT(id) DO UPDATE SET hostname=excluded.hostname,
		  version=excluded.version, state='connected', last_heartbeat=excluded.last_heartbeat`,
		id, hostname, version, "connected", now, now)
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
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`SELECT id, hostname, version, state, COALESCE(last_heartbeat, 0), enabled FROM agents ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Agent
	for rows.Next() {
		a := &Agent{}
		var enabled int
		if err := rows.Scan(&a.ID, &a.Hostname, &a.Version, &a.State, &a.LastHeartbeat, &enabled); err != nil {
			return nil, err
		}
		a.Enabled = enabled != 0
		out = append(out, a)
	}
	return out, rows.Err()
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
