# drsync Detailed Design — Coordinator (`drsyncd`)

**Status:** Detailed design v1 — 2026-07-10
**Language:** Go (decision D1). Runs on a dedicated host (D7); needs fast local disk for
the state store and journals (NVMe recommended; journals for a 1B-file pass ≈ 100–200 GB
before compression, ~30–60 GB with zstd).

> **Implementation status (2026-07-11):** the §6 operator surface is complete.
> All REST endpoints are live — job CRUD/actions, pass detail with shard
> breakdown and duration, the error browser (errno-class + path-prefix
> filters, per-class counts), the paged journal query (type/path filters,
> `pass=N|all`), the migration report (per-pass delta trajectory, verify and
> fidelity totals, orphans outstanding, parked shards) and the global queue /
> parked-shard view — plus the `GET /api/v1/events` WebSocket: job/pass state
> changes, agent connect/disconnect, parked-shard alerts, and 1 Hz stats
> frames for running jobs. Events are produced by a 1 s store-snapshot differ
> (`internal/events`) rather than per-transition hooks: one producer, always
> consistent with what the REST views report. WebSocket auth accepts the
> bearer token as a `?token=` query parameter (browser clients cannot set
> headers), or the session cookie for a same-origin browser client.
> **Auth (2026-07-22):** interactive login (local host accounts or Active
> Directory, session-cookie based, `/etc/drsync/auth.yaml`) and HTTPS for this
> listener (`/etc/drsync/certs.yaml`) have landed — see §6 and
> `docs/ADMIN.md` §8. Not yet: role-based access (every authenticated caller
> is equivalent) or OIDC/SSO, coordinator HA (§8), the event-driven pass
> controller (state machine still ticks at 2 s).
> **Hardlinks (2026-07-31, D11):** preservation is live and on by default — see §2.2 for the
> LINKFIX phase and §3 for the `link_groups`/`link_members` schema; full design in
> `docs/DESIGN-hardlinks.md`. `link_groups`/`link_members` currently follow
> `chunk_groups`'s lifecycle (cleaned up at job purge, not per-pass) — a per-pass
> reap is a tracked follow-up for hardlink-dense, long-running jobs, not yet built.

---

## 1. Process Structure

```
drsyncd
├── grpc-less TCP listener (:7440)   agent protocol (see DESIGN-protocol.md)
├── HTTP listener (:7441)            REST API + WebSocket events + /metrics
├── scheduler                        shard queue, grants, credit accounting
├── lease manager                    TTL wheel, expiry → re-queue
├── pass controller                  per-job pass lifecycle state machine
├── journal writer                   per-pass segment files, zstd, fsync policy
├── stats aggregator                 fleet counters, rate windows, ETA model
└── state store (SQLite, WAL mode)   single-writer goroutine, batched txns
```

**Why SQLite over RocksDB:** state is sized by *shards* (10⁵–10⁶ rows), not files;
write rate is O(shard transitions) ≈ low thousands/s peak. SQLite-WAL with batched
transactions handles that with a single file to back up, snapshot, and reason about.
RocksDB remains the fallback if shard counts ever explode (interface is a thin
`Store` abstraction), but it is not justified at D7 scale (4 agents).

## 2. State Machines

### 2.1 Job

```
CREATED ──validate──▶ READY ──start──▶ RUNNING ⇄ PAUSED
                                        │  │
                       converged/max ───┘  └──▶ CANCELLED
                              ▼
                          COMPLETED            (any state) ──▶ FAILED
```

- `RUNNING` iterates passes via the pass controller.
- Convergence: after each pass, compare pass delta (files+bytes copied) against
  `spec.passes.converge_when`; met ⇒ `COMPLETED` (or hold in `RUNNING/awaiting-cutover`
  if `schedule: manual`).

### 2.2 Pass

```
PENDING ──▶ PROBING ──all probes ok──▶ SCANNING ──all shards done──▶ DIRFIX ──▶ LINKFIX ──▶ VERIFY ──▶ [DELETE] ──▶ COMPLETE
            (per-agent               (walk+diff+copy               (dir     (hardlink   (sampled     (only if
             mount probe)             interleaved per               metadata  group      checksums,   explicitly
                                      shard)                        sweep)    members     metadata)    triggered)
                                                                               linked to
                                                                               anchor by
                                                                               default —
                                                                               D11)
```

- `PROBING` gates the pass: one `ProbeTask` shard is pinned (`target_agent`) to each
  schedulable agent, and the root walk shard is withheld until every probe reports OK.
  Each agent verifies **its own** source and destination roots are present directories,
  and (when `probe.require_mount` is set, the default) that each sits on a real mounted
  filesystem — the agent checks `/proc/self/mountinfo` for a non-`/` mount covering the
  root, so an unmounted volume's leftover stub directory is caught rather than silently
  synced into the underlying rootfs. A missing/misordered mount or a stub on any host is
  thus caught before bulk work runs — not just on whichever agent grabbed the root shard.
  A failed probe parks (like any shard),
  and the parked-shard guard holds the pass until the operator fixes the mount and
  retries. Probes pinned to an agent that departs after seeding are pruned so the phase
  is not stalled. An empty fleet skips probing (nobody to probe or grant work to).
- `SCANNING` is the long phase: walk, diff, and copy are interleaved *per shard*, so
  data starts moving seconds after pass start; there is no global "scan first" barrier.
- `DIRFIX` applies directory metadata deepest-first from the journal's dir records
  (see agent doc §6.3). Cheap: directories are typically 1–5% of entries.
- `LINKFIX` (D11, `metadata.hardlinks`, default `preserve`; `docs/DESIGN-hardlinks.md`)
  seeds one `LinkTask` per hardlink-group member whose anchor copy has landed —
  `seedLinkfix` reads `link_groups`/`link_members`, not the journal. A job that opts
  out (`hardlinks: report`, D3's original behavior) never populates that table, so
  this phase seeds nothing and drains immediately. Sits after DIRFIX (so a member's
  destination directory already exists) and before VERIFY (so linked files are
  verified like any other entry).
- `VERIFY` grants verify batches built from the pass journal (all-metadata + sampled
  checksum per D4).
- `DELETE` exists only when triggered with the explicit double-gate (D5); tasks are
  built from the orphan journal — **no additional scan**. The orphan count a scan pass
  reports (job summary, convergence table, `drsync journal cat --summary`) and the
  removal count the delete pass later reports are different units, not the same number
  reappearing twice: the scan journals one `ORPHAN` record per orphan *path* and never
  descends into an orphaned directory (no reason to — the whole subtree is getting
  deleted), so a directory with thousands of descendants underneath still counts once.
  At delete time, `agent/src/delete.c`'s `rm_tree` recursively removes and counts every
  filesystem object under each orphan path — so it is expected and correct for the
  delete pass's live removal count to run well past the prior pass's reported orphan
  count while still in progress, not a sign of double-counting or runaway deletion.

### 2.3 Shard

```
QUEUED ──grant──▶ LEASED ──ShardResult ok──▶ DONE
                    │  │
        lease expiry┘  └─ShardResult(err)──▶ PARKED ──(operator retry / auto after
                    ▼                                   transient-window)──▶ QUEUED
                 QUEUED (attempt++)
```

- `attempt` counter with ceiling (default 5): repeated lease-expiry of the same shard
  (e.g. a directory that OOMs/kills agents or hangs an NFS mount) parks it with
  diagnosis breadcrumbs instead of poisoning the fleet forever.
- Shards created by `ShardSplit` enter `QUEUED` in the same transaction that records
  the split against the parent (ordering invariant, protocol doc §4.2).

## 3. Schema (SQLite)

```sql
jobs    (id, name UNIQUE, spec_yaml, spec_hash, state, created_at, updated_at)
passes  (id, job_id, pass_no, state, started_at, finished_at,
         files_scanned, files_copied, bytes_copied, files_meta_fixed,
         orphans, errors, nlink_dup_files, nlink_dup_bytes,
         links_created, link_anchor_races, link_fallback)     -- denormalized counters
shards  (id, pass_id, parent_shard_id, kind,        -- kind: dir | entrylist | chunk |
         rel_path, payload BLOB,                    --  dirfix | linkfix | verify | delete
         state, attempt, lease_id, lease_agent, lease_expiry,
         result BLOB, updated_at)
         INDEX (pass_id, state)                     -- the scheduler's working set
agents  (id, hostname, state, version, caps BLOB, last_heartbeat,
         cert_cn, registered_at)
chunk_groups (pass_id, rel_path, temp_name, size, mtime_ns,
              n_chunks, n_done, state)               -- large-file cross-fleet assembly;
              -- finalize task seeded (same tx as the last data chunk) at n_done==n_chunks
link_groups  (pass_id, dev, ino, nlink_expected, members_seen,
              anchor_rel_path, anchor_size, anchor_mtime_ns,
              anchor_state, updated_at)              -- D11 hardlink correlation, pass-scoped;
link_members (pass_id, dev, ino, rel_path, state)    -- reaped at DIRFIX->LINKFIX, see §3
journal_cursors (pass_id, agent_id, acked_seq)      -- JournalBatch flow control/dedup
```

- All writes funnel through one writer goroutine committing batched transactions every
  20 ms or 1000 ops — keeps SQLite happy and makes crash recovery trivial (WAL replay).
- Recovery on restart: load `shards WHERE state='LEASED'` → leases resume their TTL
  countdown from `lease_expiry` (persisted absolute time); everything else is stateless.
- **Shard Reaper:** `shards` is sized by every unit of work ever created, not just
  live work — a DONE row otherwise sits in the table for the rest of the job's life,
  which is most of it on a large tree (a single pass over a big filesystem can put
  millions of rows through SCANNING alone). `passctrl.advance()` deletes a phase's
  DONE rows the moment it proves that phase fully drained (the same queued+leased
  check that gates the phase transition itself), batched (`store.ReapBatchSize`) so
  one reap never holds the writer lock as long as a multi-million-row backlog would
  take in one delete. Pass-level file/byte totals live denormalized on `passes`
  (`AccumulatePassCounters`), not derived by re-scanning `shards`, so those are
  unaffected — but the shard-count-by-state operator views (pass-detail API, CLI)
  read live from the `shard_counts` rollup, which the reaper's deletes correctly
  drain to zero. `passes.shards_reaped` preserves the historical DONE total across
  that (bumped in the same transaction as each reap), so `ShardStateCounts` still
  reports what a completed pass actually did — per-row detail (which shard, which
  agent) does not survive, only the count. The database is opened with
  `auto_vacuum=INCREMENTAL` and a periodic `PRAGMA incremental_vacuum` pump reclaims
  the pages the reaper frees, so deleting rows actually shrinks the file instead of
  leaving freed-but-unreturned pages in it forever.
- **Eager SCANNING reap:** the transition-time reap above still leaves SCANNING's own
  DONE probe/dir/entrylist/chunk rows sitting in `shards` (and their `splits` rows)
  for the entire scan, since that phase alone can run for most of a large job's wall
  time — `shards`/`splits` grow to that phase's full peak before the very first reap
  ever fires. `passctrl.advance()` now also reaps these kinds on every tick a
  SCANNING pass has any DONE row of them, not just at the SCANNING→DIRFIX
  transition — the same `reapPhase`/`ReapDoneShards` call, just no longer gated on
  the whole phase having drained first. Safe because of the agent protocol's own
  ordering invariant (agent/src/walker.c `ship_split`/`await_split`, protocol doc
  §4.2): every `ShardSplit` for a shard is acked — and durably recorded in `splits`
  via `RecordSplit`'s `(parent_shard_id, seq)` idempotency key — *before* that
  shard's own `ShardResult` is ever sent, so a DONE shard can never again be
  referenced as a split parent by a retransmit; and `ExpireLeases` only ever touches
  `LEASED` rows, so a DONE shard can never be requeued either. Nothing else reads a
  DONE `dir`/`entrylist`/`chunk` row after the fact: DIRFIX/VERIFY seeding
  (`seedDirfix`/`seedVerify`) read the pass's on-disk journal, not `shards`, and the
  operator-facing counts read the `shard_counts` rollup, which the reap's deletes
  already drain correctly regardless of timing. The phase itself still only
  transitions once every queued/leased shard drains (unchanged) — this only stops
  *already-finished* shards from sitting around for the rest of the phase.
- **A reaped parent can still be split-referenced, across a reconnect.** The
  ordering invariant above (split-before-result) holds *within* one agent
  session, but not across one: the agent's outbox (agent/src/state.c) is a
  single durable FIFO for the whole process, spanning reconnects — a
  `ShardSplit` queued but not yet acked when the control connection drops sits
  there and gets replayed verbatim once the agent reconnects, frame type and
  all, with no awareness of what happened to its parent shard in the meantime.
  If that parent finished and was eagerly reaped during the outage, the
  replayed split now names a `shards` row that no longer exists.
  `RecordSplit`'s own idempotency check (`splits` keyed on
  `(parent_shard_id, seq)`) doesn't save this case either, since the parent's
  reap is what proves the split was already fully processed — there was never
  a reason to also keep the `splits` row once the parent shard itself, and
  everything downstream of it, was gone. Before the fix this surfaced live as
  a permanent disconnect loop across the fleet: `RecordSplit`'s
  `SELECT pass_id FROM shards` returned `sql.ErrNoRows`, `dispatch()` treated
  that as fatal and closed the session, and since the outbox replays the same
  unacked frame on every subsequent reconnect the agent could never get past
  it — logged as repeated "dispatch failed: sql: no rows in result set" on the
  coordinator and "coordinator sent protocol error" on the agent, with
  queue-to-wire latency climbing as the outbox backed up behind the poison
  frame. Fixed by treating a missing parent as *already processed* rather than
  an error: `RecordSplit` returns `(nil, nil)` instead of propagating
  `sql.ErrNoRows`, and `onShardSplit`'s `planBigFiles`/`planLinkSightings`
  calls (which resolve the parent's (job, pass) before `RecordSplit` even
  runs its own check) ACK the split the same way on the identical error rather
  than failing the frame. There is genuinely nothing left to do for a replay
  like this: whatever the split would have produced either already ran as
  part of the parent shard's own completion, or belonged to a job that no
  longer exists.
- **QueueSummary reports reaped DONE too:** `ShardStateCounts` (previous bullet) folds
  `passes.shards_reaped` into its DONE total, but `QueueSummary` — the `/api/v1/queue`
  view the console's job progress bars, DONE tiles and queue-state bar all read — did
  not, since it predates the eager reap and originally had no reason to: DONE rows
  used to sit in `shard_counts` until the whole phase drained, so a live scan was
  always the true total in the moment that mattered. Once shards started being reaped
  mid-phase, that stopped holding — a job walking a large tree could show a shrinking
  or flat DONE count, or a progress bar stuck near 0%, while steadily completing work,
  because the count was reading only whatever hadn't been reaped out from under it
  yet. Fixed the same way as `ShardStateCounts`: `QueueSummary`'s query gained a third
  UNIONed leg selecting `passes.shards_reaped` as a synthetic `(kind=model.KindReaped,
  state=DONE)` row per non-terminal pass with a nonzero total, seekable on
  `passes_state` like the first leg. Every current consumer of the DONE total (job
  progress, the DONE tile, the queue's state bar) already sums across `kind`, so the
  synthetic row folds in transparently; only a strict per-kind breakdown of
  *already-reaped* DONE work is unavailable, since `shards_reaped` is one running total
  per pass, not per kind — no current view needs that.
- **link_groups/link_members reap:** these two tables (D11 hardlink correlation,
  `docs/DESIGN-hardlinks.md` §3) shipped with a job-purge-only lifecycle, the same as
  `chunk_groups` — no per-pass reap. Unlike `chunk_groups` (small in practice; chunk
  fan-out only happens for genuinely huge files), a hardlink-dense tree does not stay
  small: closed after a live 400M-file job accumulated 10M `link_groups` / 18.5M
  `link_members` rows under that lifecycle. `store.ReapLinkRegistry`, called from
  `passctrl.advance()` right after the DIRFIX→LINKFIX `SetPassState` commits (same
  tick as `reapPhase(KindDirfix)` there), deletes a pass's `link_groups` rows and every
  `link_members` row belonging to them (via the `link_members_group` index), batched
  like `ReapDoneShards`. Safe because `passctrl.seedLinkfix` — called immediately
  before, in the same transition — is the *only* reader of either table anywhere in
  the coordinator, and the same split-before-result protocol invariant that makes the
  eager SCANNING reap above safe (a `ShardSplit`'s sightings, via `RecordSplit`, are
  always recorded before that shard's own `ShardResult`) means no sighting for this
  pass can arrive after SCANNING is proven drained — well before DIRFIX even starts,
  let alone LINKFIX. By the time `seedLinkfix` has run, a pass's registry rows are
  permanently settled.
- **WAL checkpointing is explicit, not automatic:** the write connection opens with
  `wal_autocheckpoint(0)`, disabling SQLite's own trigger (by default, a `PASSIVE`
  checkpoint fires the instant the WAL crosses 1000 pages). That trigger runs inline
  on whichever write commits the page that crosses the threshold — i.e. under
  `store.Store.mu`, blocking every agent's grant/renew/complete call for however long
  copying WAL frames back to the main db file takes, which at a large database and
  million-write history can be tens of ms — long enough on its own, and worse when it
  compounds with this store's other `s.mu` holders (`ReapDoneShards`,
  `ExpireLeases`), to delay a heartbeat renewal past the lease TTL. The same
  "degrades under sustained load, a restart clears it" shape as the `shards`-table
  growth (Shard Reaper, above) and the missing `jobs(state)` index
  (`SchedulerCounts`'s full scan of `shard_counts`) — accumulation correlated with
  write volume, not random contention, and a restart resets it because SQLite
  performs a full checkpoint on clean shutdown. `store.RunWALCheckpoint`, started
  from `main.go` on a 20-second interval, replaces the disabled trigger with an
  explicit `PRAGMA wal_checkpoint(TRUNCATE)` this process schedules itself, so the WAL
  still gets bounded — just predictably, instead of at whatever moment a write
  happens to cross SQLite's own page-count threshold. TRUNCATE, not PASSIVE: both
  fully copy WAL content back to the main db file without blocking *readers* (a
  reader holding an old snapshot just makes either mode checkpoint less than the
  full WAL that cycle — `busy` comes back nonzero, nothing more), but PASSIVE never
  shrinks the WAL *file* on disk, only its content — once the file grows to its peak
  size under load it stays there indefinitely even though every checkpoint since has
  fully succeeded. TRUNCATE additionally reclaims the file itself down to empty. Found
  live: the WAL file matching the main db file's own size despite `RunWALCheckpoint`
  running every 5 minutes without error — checkpointing was working exactly as
  designed, PASSIVE just never promised to shrink the file.
  Unlike PASSIVE, though, TRUNCATE genuinely does block *writers*: shrinking the file
  needs an exclusive lock over the whole WAL, so `WALCheckpoint`'s `s.mu` hold spans
  that entire copy-back-and-truncate step, not just the query dispatch around it. At
  a live 10-agent fleet, still on the original 5-minute interval, `lockTimed`'s
  hold-side logging (§ below) measured a *median* `WALCheckpoint` hold of 33s and a
  max of 144s — every other write in the coordinator blocked for the whole stretch,
  the actual mechanism behind a batch of unrelated callers all reporting `waited_ms`
  in the tens of seconds simultaneously with no single one of them individually
  slow. Moving the checkpoint off `s.mu` would not remove this cost, only relocate
  it: TRUNCATE's exclusive WAL lock is SQLite's own constraint and would block any
  other connection's write attempt regardless of what Go-level mutex wraps the call,
  trading a blocked goroutine for a `SQLITE_BUSY` retry loop of similar wall-clock
  cost. The interval is the actual lever — TRUNCATE's cost scales with how much WAL
  content has accumulated since the last checkpoint, so 20s (down from 5min) trades
  more frequent, individually cheaper checkpoints for eliminating the large backlog
  a long interval let build up. Re-measured after this change, not just assumed
  fixed by the same reasoning that picked 5 minutes originally and turned out wrong.
- **Indexing discipline for high-write tables:** every predicate a hot-path query
  filters or joins on needs an index that actually serves it — not just "an index
  exists on the table." A single-writer store makes this load-bearing in a way a
  connection-pooled RDBMS would only partially mask: a query that falls back to
  scanning-and-filtering holds `store.Store.mu` for the scan's full duration, and
  that lock is shared by every agent's grant/renew/complete call, not just the slow
  query's own caller. Found twice, both in the LINKFIX hardlink-batching incident's
  aftermath (docs/DESIGN-hardlinks.md §3.3 has the full account): `link_members`
  had no index reachable by `rel_path` alone (only the tail of its primary key),
  costing ~1.3s per `MarkLinkMembersQueued` call at 2.5M rows; `shards` had no
  index on `lease_expiry`, costing ~295ms per `ExpireLeases` sweep (every
  `leaseTTL/3`, so continuously, not just once per incident) at 50K concurrently
  leased shards. Both fixed by adding the missing index and, for the first case,
  switching from an `IN (...)`-list statement to a primary-key seek per row (the
  index alone did not change the query planner's handling of a large `IN` list).
  A third instance surfaced later, missed by that audit because it targeted
  directly-hot-written tables (`shards`, `link_members`) rather than a
  trigger-maintained rollup: `SchedulerCounts` (and `QueueSummary`) join
  `shard_counts` -> `passes` -> `jobs` filtered on job state, but `shard_counts`
  has no bound on its own row count -- it accumulates one row per
  `(pass_id, kind, state)` ever seen and is only cleared by an explicit
  `DeleteJob` purge (no auto-purge policy exists), so a coordinator with a long
  uptime and job history carries it indefinitely. With no index to seek `jobs`
  by state, the planner had nothing to drive the join from but a full scan of
  `shard_counts` -- and `SchedulerCounts` runs on every `Grant` call (refreshed
  every `countsTTL`, section 4), fleet-wide, continuously. Found chasing a live
  "leases start expiring and shards park under sustained load, a `drsyncd`
  restart clears it" report at ~400M files walked in one job: the current
  job's shard volume was unremarkable, but the coordinator's cumulative
  history of prior jobs had grown `shard_counts` large enough that the
  per-`Grant` scan itself became slow enough, held `Scheduler.mu` long enough,
  to push lease renewals past their TTL. Fixed with a `jobs(state)` index --
  `passes` already had (`job_id`, `pass_no`) uniquely indexed, so that alone
  let the planner drive `jobs` -> `passes` -> `shard_counts` instead of
  scanning the rollup. `QueueSummary` shares the join but not the fix: its
  predicate was `p.state != COMPLETE OR sc.state = 'parked'`, and SQLite
  cannot serve a `!=` with an index seek regardless of what exists on
  `passes.state`, so it kept scanning `shard_counts` even after `jobs_state`
  landed. Recast as a `UNION` of two positive membership tests -- non-terminal
  pass states (`model.NonTerminalPassStates()`, kept in sync with the
  `PassState` enum rather than a hardcoded SQL list) for the first leg, parked
  shards for the second -- each independently seekable, backed by a new
  `passes(state)` index and a `shard_counts(state)` index (`shard_counts`'
  own primary key is `(pass_id, kind, state)`, which can't serve a `state`-only
  lookup). `UNION` (not `UNION ALL`) dedups the case where a shard is both
  parked and on a non-terminal pass, landing in both legs.
  The rest of the schema was audited table-by-table
  against every query site after finding these -- see
  `TestMarkLinkMembersQueuedStaysFastAtScale`,
  `TestExpireLeasesUsesLeaseExpiryIndexAtScale`,
  `TestSchedulerCountsUsesJobsStateIndexAtScale`, and
  `TestQueueSummaryUsesIndexesAtScale` (`coordinator/internal/store`)
  for the regression pins, and re-run that audit -- including rollup tables fed
  by triggers, not just directly-written ones, and checking whether a `!=`/`OR`
  predicate defeats an otherwise-correct index -- whenever a new hot-path query
  is added to a table this store expects to grow large.
- **Lock-hold time, not caller count, is the recurring failure mode.** Three
  more instances found live (thousands of "store: long wait for write lock"
  entries fleet-wide, `dispatch took unusually long` warnings in the hundreds
  of ms, heartbeat renewal not matching every held lease, and every agent
  exceeding lease TTL in unison — the same class of symptom as the index
  findings above, but caused by doing avoidable work *under* `s.mu` rather than
  by a missing index):
  - `RecordSplit`'s two pre-checks — the `splits` idempotency lookup and the
    `SELECT pass_id FROM shards` parent resolve — used to run on `s.db` (the
    single write connection) while already holding `s.mu`, so a busy split
    (`recordLinkSightingsTx` is 5 statements per sighting) held the lock for
    the whole lookup-then-decide sequence, not just the final insert. Both now
    run on `s.rdb` (the lock-free reader pool) before `s.mu` is ever taken;
    only the actual `INSERT`s run under the lock. A retransmit racing itself
    past the now-unlocked pre-check is not a new risk: `splits`' own
    `PRIMARY KEY (parent_shard_id, seq)` still catches a genuine double-insert
    at the `INSERT`, and in practice can't happen anyway — one agent's frames
    dispatch on a single goroutine, so a second copy of the same frame is only
    ever read after the first `RecordSplit` call returns.
  - `onHeartbeat` used to call `store.TouchAgent` (`s.mu`) on every single
    heartbeat frame from every connected agent to persist `last_heartbeat` —
    at fleet scale, the write lock's next contender was almost always another
    heartbeat's `TouchAgent`, not real work, even though `last_heartbeat` is
    purely display-only (the operator-facing agents API; nothing in
    scheduling or lease logic reads it). Replaced with an in-memory
    `agentConn.lastHeartbeatMs` (same "sampled state, not history" pattern as
    the existing in-flight snapshot) set lock-free on every heartbeat, and
    `agentsrv.RunHeartbeatFlusher` — a periodic goroutine (`heartbeatFlushInterval`,
    5s) that batches every connected agent's current timestamp into one
    `store.TouchAgents` transaction. Trades a few seconds of display staleness
    for taking the write lock a handful of times a minute instead of once per
    heartbeat.
  - `passctrl`'s reap calls (`doReapPhase`/`doReapLinkRegistry`, formerly
    `reapPhase`/`reapLinkRegistry`) used to run inline inside `advance()`,
    which itself runs on `tick()`'s single goroutine, serially across every
    running job. A pass with a large reap backlog loops `ReapDoneShards`/
    `ReapLinkRegistry` batches back-to-back with no yield between them for up
    to `reapPhaseBudget` (30s) — inline, that meant one job's reap could block
    every *other* running job's `advance()` call for up to 30 seconds solid,
    while simultaneously re-acquiring `s.mu` in a tight loop the entire time,
    directly competing with every connected agent's heartbeat/dispatch call on
    every single acquisition. Reaping is best-effort by design (each call site
    already documented that "a reap failure must not stop the phase
    transition that already committed above it" — nothing downstream of the
    six `advance()` call sites depends on the reap having finished), so it was
    moved off the tick goroutine entirely: `advance()` now calls
    `enqueueReap`/`enqueueReapLinkRegistry`, which hands a request to a
    channel (`reapCh`, buffered) drained by a dedicated `runReapWorker`
    goroutine (started alongside `tick()` from `Controller.Run`) and returns
    immediately. `reapInflight` dedups the channel per `(pass, registry?)` key
    so the SCANNING eager-reap call site — which re-enters the gate every 2s
    tick while `counts[ShardDone] > 0` — cannot pile up redundant requests
    behind a worker that has not caught up yet. The lock is still taken the
    same way inside the actual reap (one goroutine at a time instead of
    contending with itself across jobs' `tick()` iterations), but never blocks
    unrelated coordinator work upstream of it.
  - A fourth, found from live lock-wait counts *after* the first three
    shipped (`RecordSplit` still the top caller by volume, `shardTransition:DONE`
    and `AccumulatePassCounters` firing in an almost 1:1, back-to-back
    pattern): `onShardResult` — the handler for every successful `ShardResult`
    frame, the single highest-volume event in the coordinator — called
    `CompleteShard`/`CompleteDataChunk`/`CompleteFinalizeChunk` (their own
    `s.mu` acquisition to update shard state) and then, separately,
    `AccumulatePassCounters` (a second `s.mu` acquisition to update the
    pass's running totals) for every successful shard. Two lock round-trips
    where the two updates could just as well be one transaction. `CompleteShard`
    and the two chunk-completion methods now take an optional
    `*drsyncpb.ShardCounters` and fold the counters `UPDATE` into the same
    transaction as the shard-state `UPDATE`, under one `s.mu` acquisition;
    `AccumulatePassCounters` itself is unchanged for callers that only ever
    need the counters update on its own. The merge is genuinely atomic, not
    just "called from the same function": a rejected transition (stale
    lease, `ErrLeaseMismatch`) rolls the whole transaction back, so counters
    from a stale/duplicate result are never applied either — same guarantee
    the two-call version gave by only calling `AccumulatePassCounters` after
    `CompleteShard` returned success, just without paying for a second lock
    acquisition on the (overwhelmingly common) success path.

  None of these four needed a schema change or a new index — the fix in each
  case was moving avoidable work to a place that does not hold `s.mu` while
  doing it, or collapsing two lock acquisitions that were always going to
  succeed or fail together into one. See
  `TestRecordSplitPreChecksDoNotBlockOnWriteConnection`,
  `TestHeartbeatDefersStoreWriteToFlusher`,
  `TestAdvanceDoesNotBlockOnReap`/`TestAdvanceDedupsReapRequests`, and
  `TestCompleteShardMergesCounterUpdateAtomically` for the regression pins.

## 4. Scheduler

- **Credit-based pull** (protocol doc §3): agents advertise capacity; the scheduler
  grants up to `parallel_shards_per_agent` outstanding shards each.
- **Queue ordering:** FIFO within a pass, with a few twists, by shard-kind priority
  (higher granted first: probe 20 > delete 15 > chunk 10 > everything else 0):
  1. **Probe tasks outrank all** — they gate pass start (`PROBING`).
  2. **Delete (orphan-removal) tasks outrank chunk and walk work** — a mirror-mode
     delete pass reclaims destination space promptly once seeded.
  3. **Chunk tasks outrank dir shards** — a huge file's chunks should saturate the
     fleet rather than trickle while walkers churn.
  4. **Anti-affinity for retries** — a re-queued shard is preferentially granted to a
     *different* agent than the one whose lease expired (dodges host-local mount issues).
- **Fairness across jobs:** weighted round-robin by job priority (spec field, default
  equal). Multiple concurrent jobs are first-class.
- **Throttles:** bandwidth/IOPS ceilings are enforced agent-side (token bucket), but the
  scheduler enforces `src_load_ceiling` by shrinking grant credits when agents report
  p99 latency above the ceiling — global backpressure with no agent coordination.

### 4.1 Fan-out: who decides how far a shard descends

An agent walking a shard cannot know whether the fleet needs more shards — it
sees one subtree, not the queue. Left to itself it descends until
`tuning.shard_budget` (250k entries) runs out, so **a volume smaller than the
budget never splits at all**: one shard, one agent, one walker thread, whatever
the fleet size. That is a correctness-preserving but capacity-wasting outcome,
and it is the common case when consolidating many modest volumes.

The decision therefore belongs to the coordinator, which is the only party that
knows both numbers:

```
target  = spread_target_per_agent × (connected AND enabled agents)
pending = queued + leased dir/entrylist shards, across RUNNING jobs
spread  = pending < target                       # tuning.spread_mode: auto
```

While `spread` holds, every granted walk shard carries `WalkOverrides`
(protocol §4.3) with `walk_budget = 0`: descend nothing, push every
subdirectory back as a new shard. The queue therefore grows exponentially from
the root until it can cover the fleet, at which point the overrides stop, shards
revert to `shard_budget`, and agents descend deeply in-process with no further
round trips. Steady-state behaviour at PB scale is unchanged (D7) — the cost is
bounded at roughly `target` extra round trips in a job's first moments.

Two properties this must preserve:

- **Overrides only ever fan out harder.** The spread `split_threshold` is
  `min(spread default, the job's dir_split_threshold)`: an operator who lowered
  the threshold to break up a pathological directory keeps it.
- **Leased shards count as pending.** An agent busy on a shard is not starved;
  counting only queued shards would spread forever.

`spread_mode: off` pins the pre-fan-out behaviour, `always` spreads on every
grant. Both are diagnostic.

### 4.2 Fair-share grants

Fan-out is not enough on its own. An agent requests
`(workers + copy_threads) × 2` credits — 48 on a default host — and
`LeaseShards` grants whatever it can, so the first agent to poll drains a
shallow queue and the fleet still idles.

While the queue is too shallow to fill every agent's request
(`queued < agents × credits`), a grant is capped at `ceil(queued / agents)`.
Once the queue is deep the full request is granted, so nothing extra is paid at
scale and phases with a large task backlog (verify) are untouched. The cap is
never below 1 and never applies to a single-agent fleet: work must not sit
QUEUED because nobody is permitted to take it — the same "never strand"
property the anti-affinity tier-2 fallback protects above.

There is a second, narrower cap keyed on a shard's **parent**: a single
pathological directory fans out into a contiguous run of hundreds of entry-list
sibling shards, and granted in id order that run would fill the fleet's whole
prefetch window and starve the rest of the tree. `LeaseShards` therefore prefers
other work once one parent already holds `spread_target_per_agent × agents`
leases. This cap applies **only to entry-list shards** — the shards that hammer
one directory. Regular dir-walk shards that fan out into sub-directory shards
read a different directory each, so they are never counted or capped; throttling
them would only slow the walk. Like the fair-share cap it is a preference, not a
quota: if a saturated parent is all that is left, its shards are granted anyway.

### 4.3 Cross-fleet chunk fan-out

A file too large for one host is not copied by the agent that walks it. The
agent proposes it (`ShardSplit.big_files`: rel_path + size + mtime); the
coordinator lays it out from `copy.chunk_size` into N data-chunk shards plus a
`chunk_groups` row, all in the split's transaction so the fan-out is idempotent
on retransmit. Every chunk carries the file's (size, mtime) gen and the shared,
coordinator-named temp; chunk 0 alone creates and preallocates it.

Assembly is counted, not coordinated between agents: each data chunk's OK bumps
`n_done`, and the completion that reaches `n_done == n_chunks` seeds the finalize
shard **in the same transaction**. That atomicity is load-bearing — a reader
must never see every chunk done with no finalize queued, or `advance` (§4, which
gates on `queued+leased == 0`) would step the pass past a file not yet renamed
into place. The finalize task re-checks the gen, fsyncs, applies metadata, and
renames the temp to the final name — the commit point. A chunk that finds the
source drifted returns `RESULT_SRC_CHANGED`; the group is marked aborted, no
finalize is seeded, and the file is re-diffed next pass. The half-written temp
is removed by a **reclaim** chunk task (`ChunkTask.reclaim`: unlink `temp_name`,
nothing else), seeded for every group that never reached `done` at the moment
the pass leaves SCANNING. That instant is the whole point: `advance` has just
established `queued+leased == 0`, so no chunk of this pass can still be writing
to the name, which makes the unlink safe rather than a guess. The agent's own
orphan sweep cannot do it, because it spares temps tagged with the pass it is
running (§ below) — so without this task an abandoned temp would survive its
pass by design, and a job ending on that pass would leave it in the destination
permanently.

The coordinator names the temp `.drsync.tmp.<job>-<pass>.<shard>.<index>` (hex).
The `<job>-<pass>` tag is load-bearing, not decorative: the temp has no source
counterpart, so an agent walking its directory sees it as an orphan and the
sweep reclaims prefix-matching orphans as crash residue. Agents skip temps
carrying their own `(job, pass)`, which is what stops a re-walk of the directory
— routine, since a requeued parent walk shard keeps its already-fanned-out chunk
group (`RecordSplit`'s `INSERT OR IGNORE`) — from deleting a temp its chunks are
still writing into. Only the finalize accounts
the file (files_copied +1, bytes +size), so a pass that copied solely via chunks
still shows a nonzero delta and does not falsely converge.

## 5. Journals

Append-only, per (job, pass), the system of record for per-file outcomes:

```
/var/lib/drsync/journals/<job>/<pass>/segment-<n>.drj    (zstd frames)
```

- Record = length-delimited protobuf `JournalRecord`; batches arrive pre-compressed from
  agents and are appended as received (coordinator does not decompress on the hot path).
- Record types: `COPIED`, `META_FIXED`, `SKIPPED_CLEAN` (sampled, not exhaustive —
  counters cover the rest), `ORPHAN`, `DIR_META` (input to DIRFIX), `ERROR`,
  `FIDELITY_EXCEPTION` (e.g. untranslatable ACL), `NLINK_DUP`, `VERIFY_OK`,
  `VERIFY_FAIL`, `WOULD_COPY`/`WOULD_DELETE` (dry-run), `DELETED`.
- Every record: rel_path, record type, src/dst stat essentials, timestamps, agent id,
  and type-specific payload (e.g. checksum, errno, ACL blob that failed translation).
- Consumers: `DIRFIX`/`VERIFY`/`DELETE` task generation, `drsync journal cat`,
  the WebUI error browser, and — once, per pass, at the moment it reaches
  COMPLETE — `passctrl.recordJournalTypeCounts`, which persists the per-type
  histogram into `journal_type_counts` (store.go). `drsync report`, the
  WebUI's job detail panel, and the completion email all read that SQLite
  rollup (`store.JournalTypeCounts`), never the journal files directly: those
  are on a request/click path, and a live per-type scan there would cost
  proportional to the whole job's journal (potentially gigabytes of
  zstd-compressed segments) on every fetch.
- Retention: journals are the audit trail — kept until job deletion; segments are
  immutable and rsync-able for archival.
- **Durability / ack gating:** an incoming `JournalBatch` is written, but the
  `JournalAck` is withheld until a periodic flusher fsyncs the open segments
  (`RunJournalFlusher`, 250 ms). Only then is each agent acked up to its durable
  high-water sequence. This matters because the agent releases its send buffer
  and unblocks the shard's `ShardResult` on the ack (`agent/src/jrn.c`
  `jrn_wait_acked`): acking before fsync would let a shard complete — and its
  records be discarded by the agent — while the journal write is still only in
  the page cache, so a coordinator crash would lose them. If an fsync fails,
  every ack for that cycle is withheld (counted by
  `drsync_journal_fsync_errors_total`) and retried on the next successful flush.

## 6. REST API & WebSocket (day-1 surface, also the WebUI contract)

```
POST   /api/v1/jobs                    submit (YAML or JSON body)
GET    /api/v1/jobs                    list (+state filter)
GET    /api/v1/jobs/{name}             spec + live status + per-pass summary
POST   /api/v1/jobs/{name}/pause|resume|cancel
POST   /api/v1/jobs/{name}/passes      trigger manual pass  {delete: bool, confirm: str}
GET    /api/v1/jobs/{name}/passes/{n}  pass detail: counters, timings, delta trajectory
GET    /api/v1/jobs/{name}/errors      paged error browser (class/path filters)
GET    /api/v1/jobs/{name}/journal     paged journal query (type/path filters)
GET    /api/v1/jobs/{name}/report      migration report (JSON; CLI/WebUI render it)
GET    /api/v1/agents                  fleet: state, version, live rates, mounts probed
GET    /api/v1/queue                   shard queue depth, parked shards
GET    /metrics                        Prometheus
GET    /api/v1/events                  WebSocket: job/pass/shard state changes,
                                       1 Hz aggregated stats frames, error events
POST   /api/v1/login                   WebUI login (username/password) → session cookie
POST   /api/v1/logout                  clear the session cookie
GET    /api/v1/whoami                  current session identity + whether login is configured
```

- Auth: a bearer token (`-api-token-file`, a mode-0600 file — never a raw
  command-line value), and/or interactive login backed by
  local host accounts or Active Directory (`/etc/drsync/auth.yaml`, gated by a
  username/group allowlist) that issues a signed, `HttpOnly`/`SameSite=Lax`
  session cookie — either credential is accepted on every protected endpoint.
  The WebUI itself only ever uses the session cookie (no coordinator-URL
  override, no token entry in the page); the bearer token remains a
  CLI/script credential. See `docs/ADMIN.md` §8. There is no role distinction yet (every
  authenticated caller can do everything an authenticated caller can do);
  delete-pass's protection is the in-body confirmation string, not a
  privilege tier. `login`/`logout`/`whoami` are themselves unauthenticated
  (you can't require a session to obtain one); `whoami` never 401s.
- The listener is plain HTTP unless `/etc/drsync/certs.yaml` configures a
  cert/key pair, in which case it serves HTTPS and the session cookie is
  marked `Secure`.
- The WebSocket event stream is designed for the phase-3 WebUI but is useful
  immediately (`drsync job status --watch` consumes it).

## 7. Metrics (Prometheus)

Per-job and fleet-aggregated, the load-bearing ones:

```
drsync_scan_entries_total{job,agent}          drsync_copy_bytes_total{job,agent}
drsync_copy_files_total{job,agent}            drsync_verify_fail_total{job}
drsync_shard_queue_depth{job,state}           drsync_lease_expiries_total
drsync_errors_total{job,class}                drsync_orphans_total{job}
drsync_pass_delta_files{job,pass}             drsync_pass_delta_bytes{job,pass}
drsync_mount_latency_seconds{agent,mount,op}  (histogram: stat/read/write/readdir)
drsync_agent_up{agent}                        drsync_eta_seconds{job}
```

`drsync_lease_expiries_total` is the fleet-wide aggregate (what the WebUI's
"retry pressure" tile divides by `drsync_work_grants_total`). As of
2026-07-30, `drsync_lease_expiries_by_agent_total{agent,kind,outcome}` breaks
the same events out by which agent held the expired lease, the shard kind,
and outcome (`requeued`|`parked`) — added because `lease_agent` (see "Leases"
above) does not survive a shard's next grant, so without this the coordinator
had no durable record of which agent a given expiry belonged to once the
re-granted shard completed. Use it (or the matching per-shard `slog.Warn
("lease expired", ...)` line the sweeper now emits, which additionally
carries job/pass/shard-id/path) to tell "one flaky agent" from "fleet-wide"
apart — see `passctrl` → `scheduler.RunSweeper` → `store.ExpireLeases`
(`store.ExpiredLease`).

`drsync_pass_delta_*` per pass is the **convergence curve** — the single most important
migration-management signal (flattening curve = ready for cutover window planning).

ETA model: exponentially-weighted copy rate × remaining known bytes, marked "lower
bound" while the walk is still discovering (queue depth > 0 and discovery rate > 0).

**Gap — no per-agent walker/copy pool utilization metric.** The agent heartbeat
(see DESIGN-protocol.md `Heartbeat`) already carries `shard_queue_depth` and
`copy_queue_depth`, but `onHeartbeat` in `agentsrv/server.go` never reads
them, and an agent's configured pool sizes (`-w`/`-C` in `agent/src/main.c`)
aren't sent over the wire at all — so there's no way to compute a true
busy/total utilization figure for either pool, only queue depth once wired
through, and no total to divide by even then. The WebUI's fleet view (added
in PR #25) approximates this instead, from in-flight shard *kind* per agent
(`chunk` shards counted as copy-pool activity, everything else as
walker-pool activity) — a labelled proxy, not a real reading, since the
agent's two pools can steal work from each other and a shard's kind doesn't
map 1:1 onto which pool actually ran it. Closing this gap needs: (1) a new
heartbeat field for configured pool size (or a one-time value sent at
connect), and (2) wiring `shard_queue_depth`/`copy_queue_depth` through to a
metric here, e.g. `drsync_agent_pool_queue_depth{agent,pool}`.

## 8. HA Posture (phase 2, designed-for now)

- Active/passive: standby `drsyncd` with the SQLite file + journal directory on shared
  or replicated storage (DRBD / NFS / litestream continuous replication). Failover =
  start standby, agents reconnect (they retry `coordinator_addrs` list in order).
- A coordinator outage **pauses** grant flow; agents finish leased shards, buffer
  journal batches (bounded, then stall), and reconnect. Nothing is lost; the migration
  resumes where it stopped. This makes single-node-with-replication acceptable for
  phase 1 at D7 scale.
