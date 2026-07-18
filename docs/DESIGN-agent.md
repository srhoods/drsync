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
  crash is reclaimed by the next pass, whose pass number no longer matches.
- **Atomicity contract:** readers of the destination never observe a half-copied file
  under its final name — rename is the commit point. (Chunked files: finalize task does
  steps 6–9 once all chunks report done; `chunk_sets` tracking in the coordinator.)
- **Inline source checksum is free** (folded into the read loop) and journaled with
  every copy — the verify phase and any future audit compare against it without
  re-reading the source.
- Throughput math (D7): 16 threads × QD32 × 1 MiB against a 100 GbE full-duplex NIC
  saturates ~12 GB/s combined read+write for large files; small-file regimes are
  IOPS/latency-bound and scale with copy-thread count and NFS slot tables instead.

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
