# drsync Detailed Design — Agent (`drsync-agent`, C)

**Status:** Detailed design v1 — 2026-07-10
**Language:** C11, Linux-only. Dependencies: liburing, protobuf-c, OpenSSL (TLS + nothing
else), zstd, xxHash. No glib/libevent; no YAML (all job config arrives as protobuf,
decision D9).
**Sizing context (D7):** 4 agent hosts, 100 GbE, dual mounts (D2). Defaults below target
≥100k stat/s and ~10 GB/s copy per host, bounded by an explicit memory budget.

> **Implementation status (2026-07-10):** slices 1–2 shipped in `agent/`.
> Slice 1 — session layer (hello/heartbeat/credit-based pull), dual-tree merge
> walker with budget-based splitting (openat2 `RESOLVE_BENEATH|
> RESOLVE_NO_SYMLINKS`), temp+rename copies with owner/mode/times fidelity,
> dir-metadata-after-children, orphan reporting + temp reclaim, dry-run.
> Protobuf is a hand-rolled codec (`src/pb.c`/`src/msgs.c`, field numbers
> pinned to `proto/drsync.proto`) pending a protobuf-c toolchain.
> Slice 2 — batched stat prefetch through a raw io_uring wrapper (`src/uring.c`,
> no liburing dependency; runtime probe falls back to serial fstatat when the
> kernel forbids io_uring), and the dedicated copy pool (`src/copy.c`): bounded
> queue with walker backpressure, per-directory pending counters so directory
> metadata still lands after every rename into it, `copy_file_range`
> server-side-copy/reflink first with read/write fallback.
> **Deployment note:** RHEL 9 ships `kernel.io_uring_disabled=2`; agent hosts
> need `0` (or `1` + privileged agents) for the statx batching to engage — the
> agent logs which path it took, and `-U` forces the fallback for A/B testing.
> **Measured (single host, xfs, 50k×256B files):** copy pool fully overlaps
> data movement with scanning (copy pass ≈ dry-run pass wall time); per-file
> fsync costs ~2.7 ms/file here — bulk passes on crash-tolerant migrations
> should consider `copy.fsync: batched` (5× end-to-end at this shape).
> Slice 3 — metadata fidelity (`src/xattr.c`): full xattr copy on files
> (fd-based, during copy, before chown/chmod/times per §5), directories and
> symlinks (via `/proc/self/fd` paths — no `*at()` xattr syscalls exist);
> POSIX ACLs as raw `system.posix_acl_*` blobs, NFSv4 ACLs as raw
> `system.nfs4_acl`, gated by the job ACL options with the untranslatable
> policy (warn→fidelity exception, fail→error, skip); stale dst-only xattrs
> removed; `security.selinux` deliberately excluded (destination policy owns
> labels; copying it unprivileged would re-flag every file every pass).
> Diff predicate step 6 implemented: otherwise-clean files get a no-open
> xattr-set comparison (two llistxattr via /proc paths), drift is fixed
> metadata-only — verified: an xattr-only change syncs with files_copied=0,
> meta_fixed=1. Sparse files: SEEK_DATA/SEEK_HOLE extent copy + ftruncate
> (blocks<size heuristic; dense fallback when SEEK_DATA is unsupported) —
> verified: 16 MiB/4 KiB sparse file arrives content-identical using <1 MiB
> on disk. New `fidelity_exceptions` counter flows agent→proto→coordinator→
> API, and each exception is now also **journalled** as a
> `JR_FIDELITY_EXCEPTION` record (rel_path, the attribute name, errno), so an
> unpreservable attribute is visible in `drsync journal cat --type
> fidelity_exception` and the report — not just counted (`walk_fidelity`,
> unit-tested by `agent/test/fidelity_test.c`). POSIX↔NFSv4 ACL *translation*
> remains a tracked follow-up: cross-flavor pairs still hit the untranslatable
> policy, and it cannot be exercised without an NFSv4 mount.
> Slice 4 — journals + the delete pass. Agents journal per-file outcomes
> (`src/jrn.c`): COPIED, META_FIXED, ORPHAN, DIR_META, ERROR,
> FIDELITY_EXCEPTION, NLINK_DUP, SRC_CHANGED, WOULD_COPY/WOULD_DELETE,
> DELETED — varint-delimited records, zstd level-1 batches (1 MiB flush
> threshold), agent-global sequence numbers, and the ordering invariant
> extended to journals: a shard result is sent only after its highest batch
> seq is acked (at-least-once; readers dedup). The coordinator gained a
> journal reader (`coordinator/internal/journal/reader.go`, klauspost zstd)
> and `drsync-journal` (dump/summarize tool, precursor of `drsync journal
> cat`). The delete pass is implemented end-to-end (D5): triggered only via
> the API double gate (explicit `delete:true` + `confirm:<job name>`), built
> from the previous pass's deduped ORPHAN records deepest-first — no extra
> scan — executed by the agent's WI_DELETE handler with recursive fd-anchored
> removal (orphan dirs were never descended during scan), every removal
> journaled JR_DELETED; a delete pass returns the job to COMPLETED and never
> auto-seeds another pass. Dry-run jobs journal WOULD_DELETE and remove
> nothing.
> Slice 5 — the verify pass (D4) with XXH3-128 checksums (vendored xxhash
> v0.8.3, `agent/vendor/`). The coordinator seeds VerifyBatch shards from the
> pass's own journal at the DIRFIX→VERIFY transition: every COPIED entry
> (files, symlinks, specials — all journal COPIED now) gets a metadata
> re-check (type/size/mtime/owner/mode/xattrs), and a deterministic sample
> (stable hash of rel_path; job-level `sample_rate`, floor of one when
> anything copied) is re-read on BOTH sides with hashes compared. The agent's
> `verify.c` executor journals VERIFY_OK (with the dst hash) / VERIFY_FAIL
> (with the reason) and, under `on_mismatch: recopy`, re-copies inline.
> Copy paths that move bytes through agent buffers (read/write, not
> copy_file_range/sparse-extent) also journal an inline source checksum on
> COPIED records for free. New `verify_ok`/`verify_fail` counters flow
> through proto→store→API. Verified: a single flipped byte with identical
> size and mtime is caught by checksum, journaled `JR_VERIFY_FAIL
> "checksum mismatch"`, recopied, and the job still converges; and on a
> cross-fs copy the inline copy-time source hash equals the verify-time
> destination hash. Note: this pass audits what drsync wrote this pass;
> detecting later bit rot needs the full-checksum compare mode (roadmap).
> Done since: mTLS client (`tls.c`, TLS 1.3, verifies server host/IP) +
> auto-reconnect-resume (`test/tls_e2e.sh`); entry-list shards for pathological
> directories (walker `split_entrylist`/`process_entrylist`, WI_ENTRYLIST) and
> parallel chunked copy for huge files (`copy_ranges_parallel`, honours
> `server_side_copy`), both verified by `test/scale_e2e.sh`; coordinator-
> orchestrated cross-fleet ChunkTask fan-out for big files (`chunk.c`,
> WI_CHUNK, `chunk_groups` + finalize), verified by `test/chunk_e2e.sh`.
> DIRFIX is now wired end-to-end: the coordinator seeds DirFixBatch shards from
> the pass's `DIR_META` journal records at the SCANNING→DIRFIX transition
> (`seedDirfix`, streamed in bounded batches, deepest-first per batch), and the
> agent's `dirfix.c` executor (WI_DIRFIX) re-applies each directory's
> owner/mode/mtime after the pass has drained — a diff-then-apply that leaves an
> already-correct directory untouched. This lands split/fanned-out directory
> mtimes within the same pass rather than relying on convergence over passes
> (`test/dirfix_e2e.sh`). A per-agent mount probe now gates pass start
> (WI_PROBE / `probe.c`; `test/probe_e2e.sh`).
> The byte-copy fallback (used when copy_file_range is unavailable — cross-
> device or a mount pair without server-side copy/reflink) now runs on an
> io_uring registered-buffer engine (`ucopy.c`): a depth-2 ping-pong over two
> fixed 1 MiB buffers overlaps the read of the next block with the write of the
> current one (the read and write are on different mounts in a migration), and
> the inline xxh3 hash is preserved via a stream-order sink callback. It
> self-tests READ_FIXED on a memfd and falls back to the serial read/write loop
> when io_uring is unavailable. Verified by `agent/test/ucopy_test.c` (byte-exact
> + in-order sink across edge sizes) and `test/ucopy_e2e.sh` (server_side_copy
> off → engine engaged, byte-exact, verify clean).
> Not yet: POSIX↔NFSv4 ACL translation — cross-flavor pairs still hit the
> untranslatable policy (now journalled as JR_FIDELITY_EXCEPTION, not just
> counted); the translation needs an NFSv4 mount to develop and verify.
> Verified end-to-end by `test/e2e.sh` (sync + fidelity + verify + delete).

---

## 1. Process Model

One process per host, run as root (systemd unit, `Restart=always`). Local config is
minimal and static — everything job-related comes from the coordinator:

```
/etc/drsync/agent.conf        # key = value
  agent_id       = auto        # default: stable machine-id derivation
  coordinator    = coord1:7440,coord2:7440
  cert / key / ca_cert
  mem_limit      = 16GiB       # the one knob everything derives from
  walker_threads = 8
  copy_threads   = 16
  meta_threads   = 8
```

```
main
├── control thread     TLS conn, framing, dispatch; owns all protocol state
├── walker pool  (8)   shard walks: getdents64 + statx-via-io_uring + merge-diff
├── copy pool   (16)   one io_uring ring per thread; data movement + checksums
├── meta pool    (8)   xattr/ACL/chown/chmod/utimensat application
├── stats thread       1 Hz aggregation of per-thread counters (no locks on hot path:
│                      per-thread counter cachelines, collected by reader)
└── watchdog           detects stuck syscalls on sick NFS mounts (op deadline ~120 s),
                       marks mount unhealthy → agent stops requesting work, parks
                       in-flight tasks with MOUNT_SICK, keeps heartbeating
```

**Queues:** bounded MPMC rings between pools (`walker → copy`, `copy → meta`,
`* → control(results/journal)`). Every queue full ⇒ producer blocks ⇒ natural
backpressure all the way to `WorkRequest` credits. Memory budget partitioning:

| Pool | Budget (of 16 GiB default) |
|---|---|
| copy buffers | 16 threads × QD 32 × 1 MiB registered buffers = 512 MiB |
| walker dir tables | 8 × 256 MiB cap (a 5M-entry directory fits; larger ⇒ entry-list split) |
| task/journal queues | 1 GiB |
| statx batches, misc | remainder headroom; RSS watchdog at 90% of mem_limit |

## 2. Shard Walk — the core algorithm

A shard = one directory subtree slice. Pseudocode of the dual-tree walk:

```
walk_shard(shard):
  work = stack of rel_paths, seeded with shard.rel_path
  # The coordinator's per-shard override wins when present: it knows the fleet
  # size and queue depth, and sends budget 0 to fan a job out across the fleet
  # (coordinator §4.1). Absent, the job's own tuning applies.
  budget = shard.overrides.walk_budget ?? opts.shard_budget   # default 250k entries
  while (rel = pop(work)):
    src_fd = openat_chain(src_root_fd, rel, O_NOFOLLOW|O_DIRECTORY)
    dst_fd = openat_chain(dst_root_fd, rel, ...) # may be ENOENT → all-create mode
    S = read_entries(src_fd)                     # getdents64 64 KiB batches
    D = dst_fd ? read_entries(dst_fd) : []
    sort_by_name(S); sort_by_name(D)
    statx_prefetch(S, D)                         # io_uring, 256 in flight (§3)
    for (s, d) in merge(S, D):                   # classic sorted merge
      case s && !d:            emit_create(s)
      case s && d:             diff_and_emit(s, d)      # §2.1
      case !s && d:            journal(ORPHAN, d)
      if s.is_dir:
        journal(DIR_META, s)                     # input for the DIRFIX phase
        # Descent is recursive in the implementation, but bounded: past
        # MAX_WALK_DEPTH (256) a subdir is shard_split rather than descended,
        # so a pathologically deep chain is sharded across the fleet and
        # re-walked at depth 0 instead of overflowing the walker stack.
        if budget > 0 and depth < MAX_WALK_DEPTH:
                       recurse(s.rel); budget -= subtree_estimate
        else:          shard_split(s.rel)        # push back to coordinator
    if entries(src_fd) > opts.dir_split_threshold mid-readdir:
        entry_list_split(...)                    # §2.3
  await all emitted tasks complete               # copy/meta pools drain
  flush journal batches; await acks; await split acks
  send ShardResult
```

- **Depth-first with a budget:** small subtrees complete inline (no coordinator round
  trip); anything beyond the budget fans out. Self-tuning to tree shape — but *only*
  to tree shape: the agent cannot see the fleet, so the coordinator overrides the
  budget while there is too little work queued to keep every agent busy.
- **fd-relative everything:** `openat` chains anchored at `src_root_fd`/`dst_root_fd`
  opened once at job start with `O_PATH|O_DIRECTORY`; `O_NOFOLLOW` on every component.
  No absolute-path resolution after startup ⇒ immune to symlink swaps and rename races
  escaping the roots.
- **ESTALE recovery (NFS):** re-open the `openat` chain from the root once; second
  ESTALE ⇒ task parked as transient.

### 2.1 Diff predicate (per merged entry, cheap → expensive)

```
1. d_type differs (file vs dir vs symlink vs special)      → emit replace (unlink+create)
2. regular file: size differs                              → emit copy
3. mtime differs beyond mtime_slop_ns (default 1 ms)       → emit copy
4. symlink: target string differs                          → emit relink
5. uid|gid|mode differs                                    → emit meta-fix (no data)
6. xattr/ACL digest differs (lazy: only fetched when 1–5 clean AND job preserves them;
   digest = xxh3 of sorted (name,value) pairs)             → emit meta-fix
7. in checksum sample (xxh3(rel_path) mod 10⁴ < rate·10⁴)  → emit verify(compare) task
8. all clean                                               → count as clean, journal
                                                             SKIPPED_CLEAN (sampled)
```

Step 6 keeps the common path at one `statx` per side per entry; xattr round trips are
paid only by entries that are otherwise clean and only when xattr/ACL preservation is on.

### 2.2 statx batching — the NFS scan multiplier

Serial `stat` over NFS at 0.5 ms RTT = 2k entries/s/thread — hopeless. The walker
instead submits `IORING_OP_STATX` in batches (default 256 in flight per walker thread,
`STATX_BASIC_STATS|STATX_BTIME`, `AT_SYMLINK_NOFOLLOW|AT_STATX_DONT_SYNC`), overlapping
round trips: 8 walkers × 256 in-flight ≈ enough concurrency to hit the NFS client's
slot-table limit rather than the RTT. Target ≥ 100k stat/s/host; the actual ceiling is
tunable via `statx_batch` and the host's `nfs4 max_session_slots`.

`statx_batch` sets the per-thread io_uring **ring depth** (the number of statx
SQEs a walker keeps in flight); io_uring rounds it up to a power of two, and it
is clamped to [1, 4096]. Size it at or below the backend's outstanding-RPC
budget (`nfs4 max_session_slots`); pushing it higher only queues client-side.
Rings are built lazily per walker thread, so a change applies to threads that
create their ring after the new value is seen.

### 2.3 Huge single directories (entry-list sharding)

A directory whose entry count passes `dir_split_threshold` (50k) mid-readdir switches
mode: the walker completes **enumeration only** (names, no stats — getdents64 streams
millions of names/s), packs names into `EntryListShard` batches of ~50k, and ships them
to the coordinator, which fans them out fleet-wide. Each entry-list shard then runs the
same statx/diff/copy pipeline, minus the readdir. NFS readdir cookies never cross hosts
(not portable); names are the split currency.

## 3. Copy Engine

Per copy task (one file, or one chunk of a large file):

```
1. open src O_RDONLY|O_NOFOLLOW; statx snapshot → gen = (size, mtime)
2. open dst temp: parent_fd + ".drsync.tmp.<job>-<pass>.<shard>.<seq>"
                                                      O_CREAT|O_EXCL|O_WRONLY
   (chunks: shared temp file created by first chunk via coordinator-sequenced
    creator task; others open existing temp, pwrite their range)
3. fallocate(dst, 0, 0, size)                     # contiguity + early ENOSPC
4. if server_side_copy=auto and same-mount pair supports it:
       loop copy_file_range() until done          # NFSv4.2 SSC / clone; free bytes
   else:
       io_uring loop: read(src, off) → [xxh3 fold] → write(dst, off)
       QD 32, 1 MiB registered buffers, chained SQEs where profitable
   sparse mode: lseek(SEEK_DATA/SEEK_HOLE) drives the offset list; holes skipped
       (fallocate already zeroed); fallback: all-zero 1 MiB blocks skipped
5. re-statx src: gen changed? → abort, journal SRC_CHANGED, re-diff next pass
6. fdatasync(dst)
7. apply metadata (§6) on the temp file via fd
8. renameat(temp → final)                          # atomic replace of any old version
9. journal COPIED {rel, size, xxh3, timings}; ack task
```

- **Crash residue:** orphaned `.drsync.tmp.*` files are recognized by name pattern and
  reclaimed/deleted by the next walk of that directory (they never match source names,
  so they appear as orphans with special handling: always deleted, even in report mode)
  — *except* temps tagged with the sweeping shard's own `<job>-<pass>`, which are live
  work elsewhere in the fleet, not residue. The tag exists because a chunked file's
  temp sits in the destination for the whole multi-host copy, and its directory can be
  re-walked meanwhile (parent walk shard requeued after a lease lapse or journal-ack
  timeout, with the chunk group deliberately kept rather than re-fanned). Reclaiming it
  then failed the finalize with `open temp for finalize`, or — mid-group — let the
  remaining chunks recreate the temp and finalize rename a hole-ridden file into place.
  Untagged temps from a pre-tag build remain reclaimable; a tagged temp orphaned by a
  crash is reclaimed by the next pass, whose pass number no longer matches. A chunk
  group abandoned mid-assembly (source drift) does not wait that long: the coordinator
  seeds a **reclaim** chunk task (`ChunkTask.reclaim` — unlink `temp_name`, no source
  read, no metadata) once the pass's scan phase has drained and nothing can still be
  writing to the name. The tag is parsed strictly — lowercase hex digits only, no sign,
  whitespace or `0x` — since a false "live" reading would protect a file from reclaim
  forever.
- **Atomicity contract:** readers of the destination never observe a half-copied file
  under its final name — rename is the commit point. (Chunked files: finalize task does
  steps 6–9 once all chunks report done; `chunk_sets` tracking in the coordinator.)
- **Inline source checksum is free** (folded into the read loop) and journaled with
  every copy — the verify phase and any future audit compare against it without
  re-reading the source.
- Throughput math (D7): 16 threads × QD32 × 1 MiB against a 100 GbE full-duplex NIC
  saturates ~12 GB/s combined read+write for large files; small-file regimes are
  IOPS/latency-bound and scale with copy-thread count and NFS slot tables instead.

### 3.1 Cross-pool work-stealing

The fixed `-w`/`-C` split is a starting allocation, not a ceiling: an idle walker
drains the copy backlog, and — when a job's mostly-metadata convergence pass leaves
the copy queue empty — an idle copy thread steals a queued walk shard and crawls it
(`g_steal_enabled`, default on; `-S` pins the pools to their fixed sizes instead).
Mechanically: non-blocking `wq_trypop`/`cq_trypop` plus short-timeout waits so an
idle thread rechecks the other pool's queue, a shared `process_item()` dispatch
runnable from either pool, and a shard-prefetch credit sized to the *whole* pool
(`(workers + copy_threads) * 2`, not just `workers`) so the extra crawl capacity has
shards to pull.

**Copy-pool reserve.** A stolen walk shard that fans out into files re-enters the
copy engine (§3) from whichever thread stole it — a copy-pool thread producing into
its own pool's queue via `cp_submit`, which blocks that thread on a full queue
(`agent/src/poolsize.c` `cp_reserve_for`). Some number of copy threads must therefore
stay pure drainers (never steal), or the pool can starve itself of the throughput
needed to drain what its own stealing thread just queued. The original sizing
(`cp_init`'s introducing commit) reserved a flat 1 thread — enough to guarantee
liveness (the drain side always makes *some* progress, so the pool cannot deadlock)
but not enough to guarantee *throughput*: with the default 8 copy threads, 7 could
simultaneously become stealing producers against a single drainer. The reserve now
scales at 25% of `copy_threads` (floored at 1, so the deadlock-freedom guarantee is
never lost) — matching the codebase's existing 25/75 walker/copy design ratio
(`ansible/roles/drsync_agent`) — so the drain side keeps real throughput at any pool
size instead of degrading to one thread as the pool grows.

> **This was not, on its own, the cause of the lease-requeue bug described in §3.2.**
> It was the first hypothesis, built from reading the copy-pool contention path in
> isolation, and shipped as a real (if insufficient) improvement to a genuine
> deadlock-vs-throughput tradeoff. A fleet running `-w 14 -C 42` — a 4x larger reserve
> than the flat-1 baseline under this fix — saw no change in the requeue rate,
> disproving copy-pool starvation as *the* mechanism. See §3.2 for the actual cause
> and how the two were told apart.

### 3.2 Writer thread — outbound frames off the heartbeat path

A step toward the lease-requeue bug's cause (§6's `RunSweeper`/`ExpiredLease` in
DESIGN-coordinator.md exists to diagnose this class of symptom: a shard expires its
lease, requeues, and completes almost instantly on retry, with no obvious cause).
Confirmed live on the reporting fleet: `ss -tn` on an agent's control connection
showed **Send-Q at 1657 bytes at the exact moment a burst of leases expired** —
direct proof the socket's outbound buffer was backed up when the requeues happened.

> **This fix alone was also insufficient** — see §3.3. Moving the outbox drain off
> the control thread stops a full send buffer from blocking the *thread that owns
> the heartbeat timer*, but a heartbeat queued behind a large batch of already-queued
> traffic still has to wait for the writer thread's plain FIFO to reach it — which,
> at `-w 14 -C 42`, can be long enough on its own to reproduce the same symptom. The
> fleet that reported this fix didn't resolve the issue confirmed exactly that: after
> deploying it, Send-Q still spiked at requeue time.

Before this fix, one thread — the control thread — did three things on a single
blocking socket/TLS connection: read incoming frames (`FR_HEARTBEAT_ACK`,
`FR_WORK_GRANT`, ...), send its own periodic frames (`FR_HEARTBEAT`, 1 per
`hb_interval_s`; `FR_STATS_REPORT`, 1 Hz), and drain the **outbox** — a queue every
worker and copy thread pushes into (`out_push`) with its shard results and journal
batches. All outbound writes used `wire_write` → `write_full`, a plain blocking
`write()`/`SSL_write()` loop with no `O_NONBLOCK` and no socket buffer tuning
(`SO_SNDBUF`); the outbox queue itself was (and remains) unbounded.

At high worker/copy thread counts, the aggregate rate of shard results and journal
batches produced can exceed what the coordinator (or the network) drains from that
one TCP connection. When the kernel send buffer fills, `write()` blocks until the
peer makes room — and since the control thread did the outbox drain and the
heartbeat send on the same loop iteration, a blocked write there stalled **the whole
control thread**, heartbeat included, for as long as the buffer stayed full. Every
lease the agent held could miss renewal at once (heartbeats renew only the leases
listed in the agent's `held_lease_ids`, coordinator-side: `RenewLeasesByID`,
DESIGN-coordinator.md §6), so a burst of leases would expire together, requeue, and
— since the underlying work was often already finished or small — complete almost
instantly once the send buffer drained and a normal heartbeat got through again.

This is why the §3.1 copy-pool reserve fix made no difference: work-stealing was
never the mechanism. It scales with the same knob (`-w`/`-C`, more threads means
more result/journal traffic) but is otherwise unrelated to which pool did the
crawling — `-S` (stealing disabled) does not change how much traffic the outbox
carries or how the control thread drains it.

**Fix:** a dedicated writer thread per session. `out_push` still queues frames
(now including `FR_HEARTBEAT`/`FR_STATS_REPORT`/`FR_WORK_REQUEST`, via a new
`queue_pb` that every producer besides the initial `FR_HELLO` handshake uses instead
of writing inline); the writer thread alone calls `wire_write`, blocking as long as
it needs to on a full send buffer without affecting anything else. The control
thread's poll loop drops the outbox eventfd entirely — it only waits on the control
socket (incoming frames) and the 1 Hz timer (queue, don't send, stats/heartbeat) —
so a full send buffer can no longer delay when the timer is checked or a heartbeat
is queued, only how promptly it's *written*, which is exactly the backpressure a
lease renewal is supposed to tolerate (`RenewLeasesByID`'s at-least-once design
already assumes some latency; it just cannot survive the sender going silent
entirely). A second eventfd lets the writer report a send failure back to the
control thread (which then drives the existing reconnect path) without either
thread touching the connection while the other is using or tearing it down —
matching OpenSSL's supported one-reader/one-writer-thread threading model for a
single `SSL*`. The writer is started after a successful `dial()` and stopped
(signaled, joined) before `session_teardown()` closes the connection, on every exit
path from the per-session loop.

### 3.3 Heartbeat priority mailbox

§3.2's writer thread removed the *control-thread-blocks* mechanism but not
*head-of-line delay*: `out_push`/`out_drain` (the bulk outbox) is a strict FIFO
with no priority field. At `-w 14 -C 42`, the volume of shard results and journal
batches queued between one heartbeat interval and the next can be large enough
that a heartbeat appended to the tail still has to wait for the writer thread to
work through everything queued ahead of it — a `wire_write` per message, each
capable of blocking on the same send-buffer pressure §3.2 was written to route
around. Confirmed on the reporting fleet: Send-Q still spiked at requeue time with
the §3.2 fix deployed.

**Fix:** a second, single-slot mailbox (`out_push_priority`/`out_take_priority`,
state.c) used only for `FR_HEARTBEAT`. A new heartbeat *replaces* whatever
not-yet-sent one is already in the slot rather than queuing alongside it — correct,
not just an optimization, since only the newest lease-renewal snapshot is ever
useful to send. The writer thread checks this slot ahead of the bulk FIFO on every
wakeup, **and again between every individual message inside a bulk-drain batch** —
not just once per wakeup, since a single `out_drain()` call can return hundreds of
queued messages and checking only at the top would let a heartbeat queued mid-batch
wait behind the rest of that same batch, reproducing the same delay one level down.

**Result on the reporting fleet (`-w 14 -C 42`): a large improvement, not full
resolution.** Requeue rate 8.5% → 1.68%; peak Send-Q 1657 bytes → low 300s. A
second-order pattern remains: Send-Q pulses into the hundreds, and a requeue fires
roughly one lease TTL (~30s) later. §3.4 hypothesizes why and adds the
instrumentation to confirm it.

### 3.4 Open: residual requeues after the priority mailbox (under investigation)

The ~30s gap between a Send-Q pulse and the resulting requeue is too close to the
lease TTL to be coincidental — a *single* delayed heartbeat wouldn't cause an
expiry (the coordinator resets `lease_expiry` on every renewal, so one late
heartbeat just renews a bit late and the clock resets again); an actual expiry
needs a sustained window with **no** renewal reaching the coordinator for that
lease, roughly the length of the TTL itself. Two candidate mechanisms, not yet
distinguished:

- **Agent-side, within one `wire_write`.** §3.3's priority check runs *between*
  messages, not within one — if the writer thread is mid-way through a single large
  in-flight write when a heartbeat is queued, the heartbeat waits for that one write
  to finish regardless of priority. Journal batches flush at ~1 MiB uncompressed
  (`jrn.c` `JRN_FLUSH_RAW`) and `WIRE_MAX_FRAME` allows up to 16 MiB — a large
  enough single frame, on a sufficiently contended link, could plausibly take
  long enough to matter, though it would need to be unusually large or the link
  unusually slow to approach a meaningful fraction of 30s.
- **Coordinator-side.** The per-agent read loop is single-threaded
  (`for { ReadFrame → dispatch → loop }`, agentsrv/server.go); `onWorkRequest` →
  `Scheduler.Grant` → `store.LeaseShards`, and several other dispatch paths, take
  the coordinator's single store-write mutex (`store.Store.mu`,
  DESIGN-coordinator.md §3, "the store is the hot path"). If that mutex is held
  by something else for long enough, one agent's `dispatch` call — and therefore
  its next `ReadFrame`, heartbeat included — can stall.

**Update: reproduced on a single active agent (the other three disabled), same
`-w`/`-C` thread counts that reproduced it fleet-wide.** This rules out
cross-agent contention specifically — with only one agent connected, no other
agent's `WorkRequest`/`ShardResult`/`JournalBatch` traffic is competing for
`store.Store.mu`. It does **not** rule out the mutex itself: the lock is
process-wide, so a single agent's own dispatch still contends with the
coordinator's *internal* background jobs that also write state and share the
same lock — the lease sweeper (`ExpireLeases`, `scheduler.RunSweeper`), the
Shard Reaper (`ReapDoneShards`), the passctrl pass-phase advance, and the
journal flusher. Any of these holding the lock briefly, at the moment this one
agent's dispatch needs it, reproduces the same stall with zero other agents
involved. A separate thread-count experiment on the same fleet also narrowed
things usefully: 14 workers / 14 copy threads (28 total) stayed clean; 24/24
(48 total) started showing requeues; 14/42 (56 total, the original report)
showed 1.68%. The common factor is **this one agent's own total thread count**,
not the walker:copy split (both 14/14 and 24/24 are 1:1) and not fleet size
(reproduced with only one agent active) — consistent with total output rate
from one connection being the actual independent variable, which is equally
consistent with either the agent-side or coordinator-side hypothesis above (more
producer threads means more frames per second either straining a single
`wire_write`'s queue position or arriving at the coordinator fast enough to
matter if something else is holding its lock).

Also fixed alongside this (a genuine bug found on re-reading, independent of
either hypothesis above): the control loop's 1 Hz `timerfd` read returns an
`expiries` count — how many 1-second intervals actually elapsed since last
read, which can be `> 1` if the loop was busy elsewhere — but the code did a
flat `tick++` regardless of that count, silently under-advancing the heartbeat
schedule relative to real wall-clock time if the loop was ever stalled for
more than a second. Now advances `tick` by the real `expiries` and logs a
warning whenever `expiries > 1`, which is itself direct evidence of a
control-loop stall if it fires.

**Diagnostics added** (log at a rate that's fine to run in production
short-term, but noisy over a long window — safe to remove once this concludes):

- Agent, `send_heartbeat`: `"heartbeat queued: seq=N held=M"` at the moment a
  heartbeat is handed to the priority mailbox.
- Agent, `writer_send_one`: `"heartbeat sent: queued Xms, write took Yms"` — the
  actual queue-to-wire latency, logged for every heartbeat (not just outliers), plus
  a `WARN` if it exceeds 2s. Any *other* frame type is logged only if its own
  queue-to-wire latency exceeds 5s (`"frame type=T len=N ... exceeds ..."`) — a
  candidate for "this is what blocked the priority slot from being rechecked".
- Agent, control loop: `"control loop poll stall: N timer intervals elapsed"`
  whenever the fixed `tick++` bug above would have mattered (`expiries > 1`).
- Coordinator, `onHeartbeat`: `"heartbeat received" agent=... seq=... held=...` —
  correlates by `seq` against the agent's "heartbeat queued" line for the same
  agent, and by interval against the agent's own "heartbeat sent" cadence.
- Coordinator, the per-agent read loop: `"dispatch took unusually long"` whenever
  one `dispatch` call exceeds 500ms — direct evidence for or against store-lock
  contention as the cause, independent of Send-Q (which only reflects the
  agent's *send* side, not what the coordinator was doing).
- Coordinator, `store.Store.lockTimed` (every one of the 30 `s.mu.Lock()` call
  sites in store.go, mechanically replaced with this labeled wrapper):
  `"store: long wait for write lock" caller=... waited_ms=...` whenever
  acquiring the write mutex takes over 200ms — the direct test of the
  refined coordinator-internal-contention hypothesis above. If this fires with
  `caller` values like `ExpireLeases`/`ReapDoneShards` around the same time as
  a `dispatch took unusually long` warning for the active agent, that is
  confirmation: a background job, not another agent, is starving this agent's
  dispatch by holding the process-wide lock.

Reading the logs together for one agent around a requeue event should show which
mechanism is real: a gap between "heartbeat queued" and "heartbeat sent" points
at the agent-side in-flight-write hypothesis; "heartbeat sent" firing roughly on
schedule but the matching "heartbeat received" arriving late, alongside a
"dispatch took unusually long" and/or "long wait for write lock" warning in the
same window, points at the coordinator side instead — and the lock-wait
`caller` label says which coordinator-internal job was in the way.

**Both instrumented mechanisms came back negative.** Neither `"dispatch took
unusually long"` nor `"store: long wait for write lock"` fired, which rules out
both the coordinator-side hypothesis (§3.4) and, by the network path being
implicated by neither, most of the agent-side in-flight-write hypothesis too.

### 3.5 Root cause: O(lifetime lease count) scans under `lease_mu` — fixed

New fleet data reframed the search entirely: with `-S` (stealing off, so the
copy pool plays no role at all — confirmed separately: cranking `-C` had zero
effect, because these jobs have nothing to copy), the requeue symptom
reproduced by raising `-w` alone, starting around 34 workers. That rules out
data-path traffic as the mechanism (there is none) and points at something
that scales with **walker thread count on its own**.

`lease_mu` — the mutex guarding the agent's held-lease table — is taken by
every `lease_add` (on grant), `lease_start`/`lease_end` (once per shard, by
whichever walker thread picks it up), `lease_remove` (on completion), *and* by
`send_heartbeat`'s own `lease_snapshot`/`lease_inflight` calls every heartbeat
interval. The previous implementation scanned `[0, n_used)` for every one of
these — `n_used` a **high-water mark that only ever grew**, never shrinking as
leases completed ("freed in place... not compacted"). For a job with nothing
to copy, walk shards turn over very fast, so 34+ walker threads churn
`lease_add`/`lease_start`/`lease_remove` at high frequency; as the job
progresses `n_used` climbs toward `MAX_LEASES` (8192), and every one of those
calls' scan cost climbs with it — all serialized behind one lock that
`send_heartbeat` also needs, twice, every heartbeat interval. Severe enough
contention here starves the heartbeat with **zero footprint on the network or
the coordinator**: nothing to see in Send-Q, nothing slow in coordinator
dispatch, nothing waiting on the coordinator's store lock — it's a pure
in-process bottleneck upstream of anything that instrumentation could observe.
This fits every property of the residual symptom: walker-count-driven,
copy-independent, invisible to both prior diagnostics, and worsening over a
job's lifetime (consistent with the noisy, non-monotonic 34-vs-35 threshold
observed live — not a hard capacity wall, a race against however far `n_used`
had already climbed when a given heartbeat needed the lock).

**Fix:** rewrote the lease table (`state.c`) so every operation is O(leases
*currently* held) or better, never O(leases ever held):
- an intrusive free list for slot allocation (`lease_add` no longer scans for
  a free slot),
- an intrusive doubly-linked active list, walked by `lease_snapshot`/
  `lease_inflight`/`lease_job_held` — cost bounded by concurrently-held
  leases (roughly `(workers+copy_threads)*2` in practice), not by the
  process's lifetime lease count,
- an open-chained hash table keyed by `lease_id` (16384 buckets, load factor
  ≤ 0.5 at the table's 8192-slot capacity), giving `lease_start`/`lease_remove`
  O(1) lookup instead of a linear scan.

Slot addresses never move (array-backed, never compacted), preserving the
existing invariant that a worker's cached `tl_lease` pointer stays valid for
the life of its lease. The external API is unchanged — no caller anywhere in
the codebase needed to change.

Unit tested in isolation (`agent/test/state_test.c`, `make -C agent test`):
lifecycle correctness, `lease_job_held` reflecting only active leases, 500
concurrently-held leases surviving an unrelated mid-list removal intact, and
the direct regression test for the bug — 20,000 add+remove cycles (far more
than the table's 8192-slot capacity, the scenario that made the old
implementation's scan cost climb) followed by a snapshot that correctly shows
zero held leases, not a leaked or degraded table.

**Result: also insufficient.** Deployed and re-tested — same issue. The
`lease_mu` scan cost was a real bug, worth fixing regardless, but it was not
(or not solely) the mechanism behind the residual requeue rate.

### 3.6 Ruled out: spread mode; confirmed: every timing signal is clean

**Spread mode ruled out.** `tuning.spread_target_per_agent` defaults to 32 —
matching the reported "~32 combined thread count" threshold closely enough to
be worth checking directly. Tested `spread_mode: off` and independently
reduced shard counts: **no material difference**. Whatever the mechanism is,
it is not spread-driven shard fan-out or `FR_SHARD_SPLIT` frequency — the
default's numeric coincidence with the observed threshold was exactly that, a
coincidence.

**Every timing signal now checks out end-to-end**, confirmed live against a
build with the full §3.1–§3.5 diagnostics deployed:
- The control loop's own poll cadence is on schedule — `"control loop poll
  stall"` (the direct test for the control thread being delayed for *any*
  reason, including plain OS scheduling contention) never fires.
- `"heartbeat queued"`/`"heartbeat sent"` keep firing every ~5s straight
  through a requeue event, with consistently low queue and write latency —
  the agent is not failing to build, queue, or transmit heartbeats on time.
- `held` counts are sane (bounded near the expected `(workers+copy_threads)*2`
  ceiling, not stuck or degenerate) and **match exactly** between the agent's
  "heartbeat queued" line and the coordinator's "heartbeat received" line for
  the same `seq` — the frame is not being corrupted, truncated, or
  misparsed in transit.
- Neither `"dispatch took unusually long"` nor `"store: long wait for write
  lock"` fires — the coordinator is not slow to process what it receives, and
  not lock-contended when it tries to renew.

With delivery, timing, and aggregate content all provably correct, the only
remaining unverified dimension is **identity**: does the specific lease that
later expires actually appear in `held_lease_ids` for the heartbeat(s)
immediately before it does? A correct *count* does not guarantee a correct
*membership list* — the count could be right while the wrong ids are in it (a
different, currently-untested lease-table bug from the one just fixed) or
right by coincidence while one specific id is silently missing every time.

**Diagnostics added to test this directly:**
- Agent, `send_heartbeat`: the `"heartbeat queued"` line now includes the
  full id list (`ids=[...]`), not just the count — truncated safely (never
  mid-number) with a `"...(N more)"` marker if it would overflow the log
  buffer, so a huge held-set never silently drops ids without saying so.
- Coordinator, `onHeartbeat`: `"heartbeat received"` likewise now logs
  `held_ids` (the full slice), plus a new `"heartbeat renewal did not match
  every held lease"` `WARN` whenever `store.RenewLeasesByID`'s `matched`
  return (rows actually updated) is less than the number of ids requested.
  This is **not automatically a bug on its own** — a listed lease legitimately
  stops matching the instant its shard completes via a separate, faster
  `ShardResult` frame that can race a heartbeat built moments earlier; an
  isolated one-beat gap right before that shard's own result arrives is
  expected. A *sustained* gap for the same id across consecutive heartbeats is
  the real signal.
- `store.ExpiredLease` (returned by `ExpireLeases`, logged by
  `scheduler.RunSweeper`'s `"lease expired"` line) now carries the shard's
  `lease_id` directly, captured before the requeue/park `UPDATE` clears it —
  previously only `Agent` survived that far. This is the id to grep backward
  through both sides' heartbeat logs: was it ever reported held, and if so,
  did the coordinator's renewal for it ever come back `matched`?

Reading these together for one specific expired lease id should finally
distinguish "the agent silently stopped reporting a lease it still held" (a
membership bug in the rewritten lease table, or upstream of it) from "the
coordinator received a correct report but the renewal still didn't stick" (a
different, not-yet-identified coordinator-side issue) from "something not
captured by any of this instrumentation" (at which point a packet capture of
one specific heartbeat's payload, decoded and diffed against what
`lease_snapshot` should have produced, is the next escalation).

**Result: the trace answered it.** Grepping an expired lease's id
(from `"lease expired" lease_id=...`) against every agent's own
`"heartbeat queued"`/`"heartbeat sent"` logs found **no record of it on any
host, ever**. Not a delayed report, not a membership bug in the lease
table — the lease was never added to any agent's held-lease table at all,
on any machine, despite the coordinator's own database recording it as
`LEASED` and granted.

### 3.7 Root cause: `WorkGrant` exceeding the agent's fixed receive buffer — fixed

`dec_work_grant` (`agent/src/msgs.c`) decodes each `WorkItem` in an incoming
grant into a fixed-size array, `struct work_grant { struct shard_item
items[GRANT_MAX_ITEMS]; ... }`, `GRANT_MAX_ITEMS = 64`
(`agent/src/msgs.h`). Once that array is full, `dec_work_item` silently
`shard_item_free`s any further item — **never calling `lease_add`, never
queuing it** — and `dec_work_grant` still returns `true`: no error frame, no
rejection, no log line that actually fires for this path (the
`n_unsupported`/"skipped %zu unsupported work items" counter exists for a
different, currently-dead code path — every work item kind the coordinator
emits decodes successfully; this is a pure *count* overflow, not a
kind-support one).

The coordinator's `Scheduler.Grant` had no knowledge of this ceiling at
all: `s.st.LeaseShards(agentID, fairShare(credits, counts.Queued, agents),
...)` leased up to the agent's requested credit count with no upper bound
beyond `fairShare`'s own cross-agent fairness cap — which does not apply to
a single connected agent (`fairShare` returns `credits` unmodified whenever
`agents < 2`). `maybe_request_work` (agent-side) sizes its credit request to
`(workers + copy_threads) * 2` — 112 at the reported `-w 14 -C 42`. So a
single busy agent, in steady state with a deep walk queue (exactly what a
real convergence pass looks like), regularly requested more than 64 credits,
the coordinator leased and granted all of them, and everything past the
64th was **committed `LEASED` in the database and then silently discarded
on arrival** — a lease with no corresponding `WorkItem` the agent would
ever see, and therefore never added to `lease_add`'s table, never appearing
in any heartbeat, on any host, exactly matching the trace. Nothing renews a
lease the agent never knew it held; it simply sat until the sweeper's TTL
expired it.

This also resolves the "~32 combined thread count" threshold precisely: the
credit request is `(workers+copy_threads) * 2`, so it first exceeds
`GRANT_MAX_ITEMS` (64) exactly when `workers+copy_threads` exceeds 32 — not
`spread_target_per_agent`'s coincidentally-identical default (§3.6, already
ruled out directly), but `GRANT_MAX_ITEMS` divided by the agent's own
credit-request multiplier. The noisy, non-monotonic `-w 34` "8/8 failed"
vs. `-w 35` "5/10 clean" split fits too: whether any *particular* grant
actually exceeds 64 items depends on how deep the queue is and how many
credits are outstanding at that exact moment, not on `-w` alone — a
borderline thread count is right at the edge of "does this request's
`fairShare`-uncapped size land above or below 64 this time," which is
sensitive to timing, not a hard wall.

**Fix:** `Scheduler.Grant` now caps the lease request at `grantMaxItems`
(=64, `coordinator/internal/scheduler/scheduler.go`, documented as
mirroring the agent's fixed buffer) in addition to `fairShare`'s existing
cap — `min(fairShare(credits, counts.Queued, agents), grantMaxItems)` — so
the coordinator can never lease more than one `WorkGrant` can carry, on any
fleet size. Shards beyond the cap simply stay `QUEUED` for the agent's next
`WorkRequest`, which credit-based pull already handles correctly (this is
exactly the same "ask again when idle" pattern that already governs normal
credit exhaustion). No protocol change, no agent rebuild required — the fix
is entirely coordinator-side.

A structurally identical, smaller-blast-radius gap exists for
`WorkGrant.options` (`GRANT_MAX_OPTIONS = 8`, keyed per distinct job in one
grant batch, not per item) — left unaddressed here since it would need 9+
concurrent uncached jobs granted to one agent in a single batch to trigger,
far outside anything reported, but worth remembering if a future symptom
looks similar and involves many concurrently-running jobs on one fleet.

Unit tested (`coordinator/internal/scheduler/grant_test.go`):
`TestGrantNeverExceedsAgentReceiveBuffer` reproduces the exact fleet
scenario (112 requested credits, a queue far deeper than the cap, a single
agent so `fairShare` does not itself bind) and asserts the grant never
exceeds 64 items, that leased-but-ungranted shards is impossible (LEASED
count matches granted count exactly, nothing orphaned), and confirms the
test fails without the fix (verified by hand: reverting the `min()` call
reproduces a 112-item grant and a failing test). `TestGrantBelowCapIsUnaffected`
pins that a request already under the cap is untouched.

**Confirmed live: 4 jobs, ~15,000 grants, 0% requeue rate.** Investigation
closed.

### 3.8 Post-mortem: gating the diagnostic logging

Every fix from §3.1 onward is being kept (see the branch summary for the full
per-change rationale) — none of it was dead weight, even the steps that turned
out not to be the cause. But three log lines added along the way fire on every
heartbeat interval, forever, per agent: real `journalctl` volume for
information only useful while actively tracing a lease-identity issue.

- Agent `"heartbeat queued"` (`send_heartbeat`) and `"heartbeat sent"`
  (`writer_send_one`, the routine case) are now gated behind a new `-v` flag
  (`g_verbose_lease_trace`) — off by default. The `WARN` variants (queue-to-wire
  latency exceeding threshold) are unconditional, since they only fire when
  something is already wrong and are therefore cheap at steady state.
- Coordinator `"heartbeat received"` (`onHeartbeat`) changed from `slog.Info`
  to `slog.Debug` — silent at the default `-log-level info`, visible with
  `-log-level debug` (an existing, already-wired flag; no new coordinator flag
  needed).
- `"control loop poll stall"`, `"dispatch took unusually long"`, `"store: long
  wait for write lock"`, and `"heartbeat renewal did not match every held
  lease"` all stay unconditional — every one only logs on an actual anomaly
  (a real stall, a slow dispatch, real lock contention, a genuine renewal
  mismatch), so their steady-state cost is already zero.

Verified live: a fresh coordinator+agent pair produces zero heartbeat-related
log lines with no flags set, and both lines reappear correctly with `-v`
(agent) / `-log-level debug` (coordinator).

**Follow-up:** the `-v` `"heartbeat queued"` line printed bare lease ids
(`ids=[1234,5678,...]`), with no job/shard attribution. A held lease can
legitimately outlive a job long enough to look alarming — a slow verify or
DIRFIX shard, or a probe pinned at pass start — and a bare id gives an
operator nothing to check it against without grepping the coordinator's own
logs by id first. `fmt_lease_ids` (`main.c`) now looks each id up in the same
`inflight` view `send_heartbeat` already builds for the wire payload
(`lease_inflight`, capped at `HB_INFLIGHT_MAX`) and prints
`lease_id/job=job_id` (`job=?` if the id fell outside that cap). A job that's
actually still running now reads as unremarkable at a glance; a bare `job=?`
or one job id repeating well past when it should have finished is the actual
signal to chase, without needing a second log source to tell the two apart.

### 3.9 GPFS: io_uring copy engine disabled per-job

GPFS's `WRITE_FIXED` silently ignores the submitted length and flushes the
whole 1 MiB registered buffer regardless of the actual write size
(`ucopy_disable`'s comment in `ucopy.c`; `tools/fsprobe` independently
reproduces the metadata-path cost this filesystem imposes). The io_uring
self-test at
startup (`uring_probe`) only exercises `READ_FIXED` against a memfd, so it
never observes this — the only place the bug was visible was per-file, via
the destination-size mismatch check in `copy_file_task` (`copy.c`): every
small file on a GPFS destination wrote the wrong length, got caught, logged a
`WARN`, and was redone with the serial byte copy before the engine finally
disabled itself for that thread. Correct, but noisy (one bad write + one
`WARN` per thread's first file) and wasteful (a full 1 MiB write and readback
thrown away every time).

`opts_store` (`state.c`) now `fstatfs`s the job's destination root fd (and
source, for `dry_run` jobs that never open a destination) at the same point
it already opens both roots — before any shard for the job is walked — and
sets `no_uring_copy` on the job's `opts_entry` (`agent.h`) when either side's
`f_type` is `GPFS_SUPER_MAGIC` (`0x47504653`, the same constant
`tools/fsprobe/fsprobe.c` reports as `"gpfs"`). `copy_file_task`'s engine
selection checks this flag before `ucopy_available()`, so a GPFS job never
attempts the io_uring path at all — it goes straight to the serial copy,
which is always correct. This is per-job, not global: `g_uring_enabled`
(statx batching, a separate ring with a different bug surface) is untouched,
and a non-GPFS job sharing the same agent process is unaffected. The
destination-size mismatch check in `copy_file_task` stays as a safety net for
any other filesystem sharing GPFS's `WRITE_FIXED` behavior that `fstatfs`
doesn't identify.

Unit tested (`agent/test/opts_test.c`): `no_uring_copy` defaults false on an
ordinary filesystem (no GPFS mount in CI to exercise the true-positive path)
and survives a tunables-only options update untouched, proving the flag lives
outside the embedded `job_options` struct that update path overwrites.

## 4. Special Entry Types

| Type | Handling |
|---|---|
| symlink | `readlinkat` → `symlinkat` (replace via rename trick not possible: unlink+create; brief window documented) → `lchown` + `utimensat(AT_SYMLINK_NOFOLLOW)`; never followed, never chmod'd |
| dir | created eagerly (mode 0700 initially) during walk; true metadata in DIRFIX phase |
| device/FIFO/socket | `mknodat` + metadata; requires CAP_MKNOD (root) |
| hardlinked file (nlink>1) | copied as independent file (D3); journal `NLINK_DUP {dev, ino, nlink, size}` — the report aggregates by (dev,ino) to compute duplication cost |

## 5. Metadata Engine

Application order on every copied/fixed entry (fd-based, on temp file pre-rename):

```
1. xattrs:  flistxattr/fgetxattr from src → fsetxattr to dst
            namespaces: user.*, system.posix_acl_*, trusted.* (root), security.*
2. ACLs:    §5.1
3. fchown(uid, gid)          # before chmod: chown clears setuid/setgid bits
4. fchmod(mode)              # restores full mode incl. suid/sgid/sticky
5. futimens(atime, mtime)    # ns precision; last, nothing may touch the file after
```

### 5.1 ACL module (D8: NFSv4 from the outset)

```
detect per (mount, direction) at job start (capability probe on real test files):
  posix_acl : system.posix_acl_access/default xattrs readable/writable?
  nfs4_acl  : system.nfs4_acl xattr exposed by this NFS client?

per file:
  src posix ∧ dst posix   → raw xattr copy (byte format is kernel-stable)
  src nfs4  ∧ dst nfs4    → raw system.nfs4_acl copy (XDR blob, server-interpreted);
                            read-back verify on first N files per mount pair to prove
                            the servers agree, then trust
  src nfs4  ∧ dst posix   → translate: v4 ACE list → POSIX ACL when representable
  src posix ∧ dst nfs4    → translate: POSIX → v4 ACEs (always representable)
  not representable       → per opts.acls.untranslatable: warn (journal
                            FIDELITY_EXCEPTION + apply mode bits only) | fail | skip
```

**Status:** the raw same-flavor copies and the untranslatable policy (with
`JR_FIDELITY_EXCEPTION` now journalled, not just counted) are implemented. The
two **translate** branches are a tracked follow-up — a cross-flavor pair
currently falls straight to the untranslatable policy. The translation is
deferred because it cannot be verified without an NFSv4 mount (local
filesystems expose `system.posix_acl_*`, never `system.nfs4_acl`), so landing
it untested would be a correctness risk on exactly the metadata operators care
about most.

Translation tables follow the IETF POSIX↔NFSv4 ACL mapping draft semantics; the
read-back verification probe at job start is what turns "should work" into "measured on
this exact mount pair" — surfaced in the migration report per mount pair.

## 6. Verification & DIRFIX phases (agent side)

- **VERIFY batch task:** for each listed entry: statx both sides + xattr/ACL digest
  compare (= diff predicate steps 1–6 must be clean); entries in the checksum sample
  additionally re-read **both** sides (io_uring, same engine) and compare xxh3-128.
  Mismatch ⇒ `VERIFY_FAIL` journal + (per `on_mismatch: recopy`) an immediate copy task.
- **DIRFIX batch task:** list of (rel_path, uid, gid, mode, atime, mtime) from the pass's
  `DIR_META` journal records, applied deepest-first (coordinator pre-sorts each batch by
  depth descending). A **diff-then-apply**: owner/mode/mtime are compared first and a
  directory already at its source values is left untouched (atime, which drifts on every
  read, is refreshed when applying but is never the reason to apply). Fixes are counted
  for observability but deliberately **not** as `meta_fixed` — the walker re-bumps a
  fanned-out directory every pass, so counting would keep a job from ever converging;
  correctness comes from DIRFIX running after every pass drains, including the converging
  one. Restrictive modes (0500 dirs) land after population by construction.

## 7. Error Taxonomy

| Class | Examples | Policy |
|---|---|---|
| transient | EAGAIN, EINTR, ETIMEDOUT, ESTALE(×1), EBUSY, ENOBUFS | retry in-agent: 3 attempts, exp backoff 100 ms→2 s; then park task → coordinator re-queues (anti-affinity) |
| capacity | ENOSPC, EDQUOT | park shard + raise job-level alarm (pause job if >N in window) — these never resolve by retry |
| permission | EACCES, EPERM | journal ERROR, count, continue (per-file failures must not stall a billion-file job); surfaced in error browser |
| integrity | SRC_CHANGED, VERIFY_FAIL | journal; re-handled next pass / recopy per spec |
| mount | watchdog deadline, ENOTCONN, EIO burst | mark mount sick, stop taking work, park in-flight, heartbeat MOUNT_SICK; auto-probe recovery every 30 s |
| fatal | assertion, OOM watchdog | crash fast (leases expire, fleet unaffected); systemd restarts; core + journal breadcrumb |

## 8. Observability (agent side)

- 1 Hz `StatsReport`: per-mount op latency histograms (stat/readdir/read/write, HDR
  buckets), files/bytes per state, queue depths, RSS, ring saturation.
- Structured logs (JSON lines) to local journald; log level runtime-adjustable via
  `Control` message.
- `drsync-agent --selftest <src> <dst>`: on-host capability + fidelity probe (the same
  probes the job-start validation uses), prints the support matrix for the mount pair —
  the first thing to run on any new host.

## 9. Testing Strategy (agent-critical)

1. **Fidelity matrix suite:** generator creates every entry type × metadata combination
   (sparse layouts, 100+ xattrs, both ACL flavors, ns timestamps, suid/sticky, deep
   names, 255-byte names, symlink targets with newlines…); sync; assert byte- and
   metadata-identical. Runs against loopback NFSv3, NFSv4.2, and local fs in CI.
2. **Fault injection:** kill -9 agents mid-shard/mid-copy/mid-rename under load;
   assert convergence and zero corruption after re-run (the at-least-once invariant).
3. **Scale rig:** synthetic tree generator (configurable shape: deep, wide, huge-dir,
   many-small, few-huge) to 1B entries on a scratch filesystem; tracks scan rate,
   copy rate, memory ceiling adherence, coordinator queue behavior.
4. **NFS misery suite:** iptables-injected latency/drops, server restarts mid-job
   (ESTALE storms), mount hangs → watchdog behavior.
