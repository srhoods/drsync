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
> headers). Not yet: OIDC/roles, coordinator HA (§8), the event-driven pass
> controller (state machine still ticks at 2 s).

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
PENDING ──▶ PROBING ──all probes ok──▶ SCANNING ──all shards done──▶ DIRFIX ──▶ VERIFY ──▶ [DELETE] ──▶ COMPLETE
            (per-agent               (walk+diff+copy               (dir     (sampled     (only if
             mount probe)             interleaved per               metadata  checksums,   explicitly
                                      shard)                        sweep)    metadata)    triggered)
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
- `VERIFY` grants verify batches built from the pass journal (all-metadata + sampled
  checksum per D4).
- `DELETE` exists only when triggered with the explicit double-gate (D5); tasks are
  built from the orphan journal — **no additional scan**.

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
         orphans, errors, nlink_dup_files, nlink_dup_bytes)   -- denormalized counters
shards  (id, pass_id, parent_shard_id, kind,        -- kind: dir | entrylist | chunk |
         rel_path, payload BLOB,                    --        dirfix | verify | delete
         state, attempt, lease_id, lease_agent, lease_expiry,
         result BLOB, updated_at)
         INDEX (pass_id, state)                     -- the scheduler's working set
agents  (id, hostname, state, version, caps BLOB, last_heartbeat,
         cert_cn, registered_at)
chunk_groups (pass_id, rel_path, temp_name, size, mtime_ns,
              n_chunks, n_done, state)               -- large-file cross-fleet assembly;
              -- finalize task seeded (same tx as the last data chunk) at n_done==n_chunks
journal_cursors (pass_id, agent_id, acked_seq)      -- JournalBatch flow control/dedup
```

- All writes funnel through one writer goroutine committing batched transactions every
  20 ms or 1000 ops — keeps SQLite happy and makes crash recovery trivial (WAL replay).
- Recovery on restart: load `shards WHERE state='LEASED'` → leases resume their TTL
  countdown from `lease_expiry` (persisted absolute time); everything else is stateless.

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
  `drsync report`, the WebUI error browser, and the final migration audit report.
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
```

- Auth: bearer tokens (static file to start; OIDC when the WebUI lands). Mutating
  endpoints require a token with `operator` role; delete-pass additionally requires the
  in-body confirmation string.
- The WebSocket event stream is designed for the phase-3 WebUI but is useful
  immediately (`drsync job status --watch` consumes it).

## 7. Metrics (Prometheus)

Per-job and fleet-aggregated, the load-bearing ones:

```
drsync_scan_entries_total{job,agent}          drsync_copy_bytes_total{job,agent}
drsync_copy_files_total{job,agent}            drsync_verify_fail_total{job}
drsync_shard_queue_depth{job,state}           drsync_lease_expiries_total{agent}
drsync_errors_total{job,class}                drsync_orphans_total{job}
drsync_pass_delta_files{job,pass}             drsync_pass_delta_bytes{job,pass}
drsync_mount_latency_seconds{agent,mount,op}  (histogram: stat/read/write/readdir)
drsync_agent_up{agent}                        drsync_eta_seconds{job}
```

`drsync_pass_delta_*` per pass is the **convergence curve** — the single most important
migration-management signal (flattening curve = ready for cutover window planning).

ETA model: exponentially-weighted copy rate × remaining known bytes, marked "lower
bound" while the walk is still discovering (queue depth > 0 and discovery rate > 0).

## 8. HA Posture (phase 2, designed-for now)

- Active/passive: standby `drsyncd` with the SQLite file + journal directory on shared
  or replicated storage (DRBD / NFS / litestream continuous replication). Failover =
  start standby, agents reconnect (they retry `coordinator_addrs` list in order).
- A coordinator outage **pauses** grant flow; agents finish leased shards, buffer
  journal batches (bounded, then stall), and reconnect. Nothing is lost; the migration
  resumes where it stopped. This makes single-node-with-replication acceptable for
  phase 1 at D7 scale.
