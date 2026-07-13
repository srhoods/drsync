# drsync — Administrator & CLI Guide

How to drive real migrations with the `drsync` CLI: concepts, the full job-spec
and command reference, worked use cases, monitoring, tuning, and
troubleshooting. For standing up the fleet first, see [INSTALL.md](INSTALL.md).

---

## 1. Concepts an operator needs

**Job.** A source→destination sync defined by a YAML spec. Named, and the name
is how you address it in every command.

**Pass.** A job runs in *passes*. Each pass is a full scan+reconcile that walks
both trees, copies/fixes whatever differs, then verifies. A pass moves through
phases:

```
SCANNING → DIRFIX → VERIFY → [DELETE] → COMPLETE
```

- **SCANNING** — walk source and destination in parallel, diff, copy new/changed
  files, fix metadata. Journals every action.
- **DIRFIX** — settle directory metadata after children landed.
- **VERIFY** — re-check metadata on everything touched, and re-read + checksum a
  deterministic sample (see `verify.checksum.sample_rate`); recopy on mismatch.
- **DELETE** — *only* when you explicitly trigger it (see §5). Removes
  destination orphans.

**Convergence.** Because the source can change while you copy, one pass rarely
suffices. drsync repeats passes until a pass changes *nothing* (a true
fixpoint), or until `passes.converge_when` thresholds are met, or the
`passes.max` ceiling is hit. A converged job reaches `COMPLETED`.

**Shards & leases.** The coordinator splits the walk into *shards* granted to
agents under a lease (TTL `-lease-ttl`). If an agent dies, its lease expires and
the shard is requeued elsewhere. A shard that fails repeatedly is **parked** for
operator attention rather than retried forever.

**Journals.** Every copy, meta-fix, orphan, error and verify result is journaled
durably on the coordinator. This is your audit trail and the input to the verify
and delete phases. Browse it with `drsync journal` / `drsync errors`.

**Policies you should know (the ratified design decisions):**

- **Orphans (destination files with no source) are report-only by default** —
  drsync never deletes anything until you run a doubly-gated delete pass (§5).
- **Hardlinks are copied as independent files**, with `nlink>1` reported so the
  storage cost is visible rather than silently de-duplicated.
- **Full metadata fidelity**: owner, mode, times, xattrs, POSIX + NFSv4 ACLs,
  sparse extents, device/FIFO specials. An attribute that can't be translated is
  counted as a *fidelity exception* (or fails the entry under policy), never
  silently dropped. `security.selinux` is deliberately excluded.
- **`.drsync.tmp.*` temp files** are drsync's own; crash residue is reclaimed
  automatically on the next walk.

---

## 2. The job spec

A spec is YAML. Only `apiVersion`, `kind`, `metadata.name`, `source.path` and
`destination.path` are required; everything else has a default. Source and
destination must be **absolute** and **disjoint**.

```yaml
apiVersion: drsync/v1
kind: Job
metadata:
  name: proj-migration           # required; addressable name
  description: optional free text
spec:
  source:      { path: /mnt/src/projects }    # required, absolute
  destination: { path: /mnt/dst/projects }    # required, absolute, disjoint

  # Optional filters, evaluated in order (rsync-like globs, ** supported):
  filters:
    - exclude: "**/*.tmp"
    - include: "**"

  passes:
    max: 10                       # ceiling on passes (default 10)
    schedule: continuous          # continuous | manual  (manual = you trigger each pass)
    converge_when:                # stop early once a pass delta is "small enough"
      delta_files_below: 1        # a pass that changes 0 files always converges regardless
      delta_bytes_below: 0

  copy:
    chunk_threshold: 1GiB         # files ≥ this are copied in parallel ranges (huge files)
    chunk_size: 8GiB              # chunk granularity hint
    buffer_size: 1MiB
    preserve_sparse: true         # SEEK_HOLE/SEEK_DATA extent copy
    server_side_copy: auto        # auto | off | require  (copy_file_range / NFSv4.2 SSC / reflink)
    temp_naming: ".drsync.tmp."
    fsync: per_file               # per_file | batched  (batched is ~5× faster, weaker crash durability)

  metadata:
    owner: true
    mode: true
    times: true
    xattrs: true
    acls:  { posix: true, nfs4: true, untranslatable: warn }  # warn | fail | skip
    specials: true                # device nodes, FIFOs, sockets

  verify:
    checksum:
      sample_rate: 0.01           # fraction of copied entries re-read + checksummed (0..1)
      on_mismatch: recopy         # recopy | fail

  deletes:
    mode: report                  # report | mirror  (see §5; delete still needs the CLI gate)

  limits:
    bandwidth_per_agent: 0        # bytes/s, 0 = unlimited
    iops_per_agent: 0

  tuning:
    shard_budget: 250000          # entries a shard handles before it self-splits
    dir_split_threshold: 50000    # a directory bigger than this is fanned out as entry-list shards
    statx_batch: 256              # statx in flight per walker (io_uring)
    mtime_slop_ns: 1000000        # mtimes within this are "equal" (1 ms)
```

You can keep the spec minimal and override individual fields at submit time with
`--set` (§3), which is handy for reusing one template across jobs.

---

## 3. CLI reference

**Connection.** Every command that talks to the coordinator honours:

```bash
export DRSYNC_SERVER=http://coord.example.com:7441   # or --server URL
export DRSYNC_TOKEN=<api-token>                       # or --token T
```

(`drsync ca` is the exception — it is local-only crypto and needs no server.)

### Jobs

| Command | What it does |
|---------|--------------|
| `drsync job submit <spec.yaml> [--dry-run] [--start] [--set path=value]...` | Register a job. `--dry-run` walks/diffs/journals but executes nothing. `--start` runs it immediately. `--set` overrides a spec field (repeatable), e.g. `--set spec.tuning.shard_budget=4`. |
| `drsync job list` | All jobs and their states. |
| `drsync job status [<name>] [--watch] [--all]` | Job state + per-pass table (walked/copied/bytes/meta/orphans/verify/errors). **With no name**, shows every *active* job (`--all` includes finished ones). `--watch` streams one job's live progress over the WebSocket until it reaches a terminal state. |
| `drsync job start\|pause\|resume\|cancel <name>` | Lifecycle control. `pause` stops granting new work (in-flight finishes); `resume` continues; `cancel` ends the job. |
| `drsync job purge <name>` | Delete one **finished** job — its rows and on-disk journal — to reclaim coordinator disk. Refused for jobs that aren't terminal (cancel first). |
| `drsync job purge --completed [--older-than 168h] [--dry-run]` | Bulk-purge finished jobs. `--completed` targets `COMPLETED`; `--state completed\|cancelled\|failed\|terminal` selects which finished states; `--older-than` keeps jobs that finished more recently than the given duration; **`--dry-run` lists what would be purged without deleting anything**. |

### Passes

| Command | What it does |
|---------|--------------|
| `drsync pass trigger <name>` | Manually start the next pass (useful with `passes.schedule: manual`, or to re-scan a `COMPLETED` job before cutover). |
| `drsync pass trigger <name> --delete-pass --i-know-this-deletes` | Run a **delete pass** — removes destination orphans. Double-gated (§5). |

### Inspection & audit

| Command | What it does |
|---------|--------------|
| `drsync agent list` | Connected agents, liveness, and scheduling status (`SCHED` = `enabled`/`DISABLED`). |
| `drsync agent disable <id>` | Stop granting new shards to an agent. It stays connected and finishes its in-flight leases; nothing new is scheduled onto it. Survives agent reconnects. |
| `drsync agent enable <id>` | Re-admit a disabled agent to scheduling. |
| `drsync report <name> [--json]` | Migration/cutover summary: per-pass delta, the convergence curve, totals, fidelity exceptions. Your go/no-go artifact. |
| `drsync queue` | Shard queue depth by state, including **parked** shards. |
| `drsync errors <name> [--pass N\|all] [--class EACCES] [--path prefix] [--limit N] [--offset N]` | Browse errors, filterable by errno class and path prefix. |
| `drsync journal cat <name> [--pass N\|all] [--type orphan] [--path prefix] [--jsonl]` | Page the journal. `--type` filters record kind (`orphan`, `error`, `copied`, `meta_fixed`, `verify_fail`, …); `--jsonl` emits raw records for scripting. |
| `drsync events [--job name]` | Tail the live event stream (state changes, agent connect/disconnect, parked-shard alerts, 1 Hz stats). |

### Certificates

| Command | What it does |
|---------|--------------|
| `drsync ca init [--dir D] [--cn NAME] [--days N]` | Create the fleet CA (`ca.crt`/`ca.key`). |
| `drsync ca issue --type server\|agent --cn NAME [--dir D] [--dns H]... [--ip A]... [--out BASE] [--days N]` | Issue a leaf cert signed by the CA (serverAuth for the coordinator, clientAuth for an agent). |

---

## 4. Use cases & worked examples

### 4.1 A first migration (the safe path)

Dry-run to preview, then run for real, then confirm convergence and integrity.

```bash
# 1. Preview: what WOULD change? Nothing is written.
drsync job submit projects.yaml --dry-run --start
drsync job status projects --watch
drsync report projects                 # inspect the would-copy/would-delete counts

# 2. Run for real (submit a fresh job, or re-submit without --dry-run)
drsync job submit projects.yaml --start
drsync job status projects --watch     # to COMPLETED

# 3. Confirm
drsync report projects                 # convergence curve, 0 errors, verify clean
```

Raise `verify.checksum.sample_rate` for a high-stakes first copy (e.g. `0.1`, or
`1.0` for full re-read verification of everything copied) via `--set`:

```bash
drsync job submit projects.yaml --start --set spec.verify.checksum.sample_rate=0.1
```

### 4.2 Consolidating many sources into one destination

Run one job per source into distinct destination subtrees (sources may live on
different mounts; keep destinations disjoint). A shared template plus `--set`:

```yaml
# template.yaml
apiVersion: drsync/v1
kind: Job
metadata: { name: PLACEHOLDER }
spec:
  source:      { path: /PLACEHOLDER }
  destination: { path: /PLACEHOLDER }
  passes: { converge_when: { delta_files_below: 1 } }
```

```bash
for site in alpha beta gamma; do
  drsync job submit template.yaml --start \
    --set metadata.name=consolidate-$site \
    --set spec.source.path=/mnt/$site/data \
    --set spec.destination.path=/mnt/dst/$site
done
drsync job list                        # watch them all converge
```

### 4.3 Incremental re-sync and cutover

drsync copies only the diff, so re-running against a high-change-rate source is
cheap. For a maintenance-window cutover:

```bash
# during normal operation: keep converging in the background
drsync job status live-data           # watch the per-pass delta shrink toward 0

# at the cutover window: freeze the source (app quiesced), then one final pass
drsync pass trigger live-data
drsync job status live-data --watch    # final pass copies the last delta → 0
drsync report live-data                # sign-off artifact
```

A job that has already `COMPLETED` can be reopened for another pass with
`drsync pass trigger` — the converge/cutover flow.

### 4.4 Reclaiming space: deleting destination orphans (§5)

### 4.5 Migrating a tree with pathological directories or huge files

drsync handles both shapes automatically; tune the thresholds if your data is
extreme.

- **Directories with millions of entries.** A directory whose *source* entry
  count exceeds `tuning.dir_split_threshold` (default 50 000) is enumerated once
  and fanned out to the fleet as **entry-list shards** — the fleet stats and
  copies its entries in parallel instead of one agent grinding through it.
  Lower the threshold if single directories dominate your runtime:

  ```bash
  drsync job submit huge-dirs.yaml --start --set spec.tuning.dir_split_threshold=20000
  ```

- **Very large files.** A file at/above `copy.chunk_threshold` (default 1 GiB)
  is copied in **parallel byte ranges** into one temp, then finalized —
  so a single 500 GB file is not bottlenecked on one thread. On same-mount pairs
  `server_side_copy: auto` offloads to the filesystem (NFSv4.2 SSC / reflink,
  which moves no bytes through the agent); set it `off` to force the parallel
  byte-copy path, or `require` to fail if server-side copy is unavailable.

  ```bash
  # cross-mount migration of large media, force parallel chunked copy
  drsync job submit media.yaml --start \
    --set spec.copy.chunk_threshold=512MiB --set spec.copy.server_side_copy=off
  ```

### 4.6 Throttling agents

Cap per-agent throughput so a migration doesn't starve production I/O:

```bash
drsync job submit bulk.yaml --start \
  --set spec.limits.bandwidth_per_agent=500MiB \
  --set spec.limits.iops_per_agent=20000
```

### 4.7 Draining an agent for maintenance

To take a node out of a running migration without disrupting jobs — e.g. before
a reboot, kernel patch, or to shift its NIC/mount load elsewhere — disable it
rather than killing it:

```bash
drsync agent disable agent-07     # no new shards granted to agent-07
drsync agent list                 # SCHED shows DISABLED; CONNECTED still true
# ... agent-07 finishes the leases it already holds, then sits idle ...
# do the maintenance, then:
drsync agent enable agent-07      # re-admit it to scheduling
```

A disabled agent keeps its connection and renews its in-flight leases by
heartbeat, so work already leased to it completes normally — nothing is
force-requeued. Only *new* grants stop. The disabled flag is stored on the
coordinator and **persists across agent restarts/reconnects**, so a bounce
during the maintenance window won't silently re-admit the node. Contrast with
killing the agent: that strands its leases until the TTL expires and they
requeue elsewhere (a `pause` on the *job* stops grants to the whole fleet, not
one node).

---

## 5. Deleting destination orphans (the double gate)

By default drsync **never deletes**. Orphans (destination paths absent from the
source) are only *reported* — journaled as `ORPHAN` records you can review:

```bash
drsync journal cat myjob --type orphan          # exactly what would be removed
```

When you have reviewed them and want the destination to mirror the source, run
an explicit **delete pass**. It is doubly gated — both flags are mandatory:

```bash
drsync pass trigger myjob --delete-pass --i-know-this-deletes
drsync job status myjob --watch                 # DELETE phase removes the orphans
```

The delete pass only removes paths that were journaled as orphans by a preceding
scan, so review-then-delete is always a two-step, auditable operation.
`spec.deletes.mode: mirror` expresses intent in the spec, but the CLI gate is
still required to actually delete — there is no "just delete" switch.

---

## 6. Monitoring

- **Live, per job:** `drsync job status <name> --watch` — the per-pass table
  updates over the WebSocket until the job finishes.
- **Live, fleet-wide:** `drsync events` — state transitions, agent
  connect/disconnect, **parked-shard alerts**, and 1 Hz throughput stats.
- **Point-in-time:** `drsync report <name>` (convergence + totals),
  `drsync queue` (backlog + parked), `drsync agent list` (fleet liveness).
- **Metrics:** the coordinator exposes Prometheus metrics at
  `http://<coord>:7441/metrics` (grants, journal batches, parked shards,
  per-agent scan/copy rates and RSS). Point Grafana at it for dashboards.

---

## 6a. Managing coordinator disk

The coordinator's `-data-dir` holds the SQLite state store **and** the per-job
journal segments. Journals for billion-file jobs are large and are retained
after a job finishes (they're your audit trail). Over many migrations this
grows without bound, so reclaim it by **purging finished jobs** once you no
longer need their history:

```bash
drsync job purge proj-migration                 # one finished job (rows + journal)
drsync job purge --completed --dry-run          # preview: list what would go
drsync job purge --completed                    # every COMPLETED job
drsync job purge --completed --older-than 336h  # keep the last ~2 weeks
drsync job purge --state terminal               # completed + cancelled + failed
```

Purge only touches **terminal** jobs (COMPLETED / CANCELLED / FAILED); an active
job is refused so live work is never stranded. Purging is irreversible — it
removes the job row, its passes/shards, and its journal from disk — so export
anything you need first (`drsync report <name> --json`, `drsync journal cat
<name> --jsonl`). A good habit is a scheduled `drsync job purge --completed
--older-than <retention>` on the operator host.

---

## 7. Troubleshooting

| Symptom | Likely cause & fix |
|---------|--------------------|
| Agent shows `CONNECTED false` / never appears in `drsync agent list` | mTLS failure. Check the agent log: a cert not signed by the fleet CA is rejected by the coordinator; a server-cert SAN that doesn't match the dialed host/IP is rejected by the agent. Re-issue with the right `--dns`/`--ip`. A plaintext agent against a TLS coordinator is refused. |
| Job stuck, `drsync queue` shows **parked** shards | A shard failed its max attempts (permissions, a sick mount, a path that keeps erroring). `drsync errors <name>` to see why; fix the underlying issue (e.g. remount, chmod) and the coordinator retries after the lease/park logic, or cancel and resubmit. |
| Errors with `class MOUNT_SICK` / `ESTALE` | An agent's mount is unhealthy; that shard is requeued to another agent. Check `RequiresMountsFor` and NFS health on the offending host. |
| Job runs to `passes.max` without `COMPLETED` | The source is changing faster than it converges, or `converge_when` is too strict. A pass that changes nothing always converges; if the delta never reaches zero, quiesce the source for a final pass (§4.3) or relax `converge_when`. |
| A single huge directory or file dominates runtime | Lower `tuning.dir_split_threshold` (fan the directory out) and/or `copy.chunk_threshold` (parallelize the file). See §4.5. |
| Copy fails with "server-side copy required but unavailable" | `server_side_copy: require` on a mount pair that can't do `copy_file_range`. Use `auto` (falls back to byte copy) or ensure both sides are the same NFSv4.2/reflink-capable filesystem. |
| Fidelity exceptions in the report | An attribute couldn't be translated (e.g. an ACL with no destination equivalent). Under `acls.untranslatable: warn` these are counted and the entry still copies with mode bits; set `fail` to make them hard errors or `skip` to ignore. |

---

## 8. Quick reference card

```bash
# connection
export DRSYNC_SERVER=http://coord:7441 DRSYNC_TOKEN=…

# run a job, watch it converge
drsync job submit spec.yaml --start && drsync job status <name> --watch

# preview only
drsync job submit spec.yaml --dry-run --start

# override spec fields at submit
drsync job submit spec.yaml --start --set spec.copy.server_side_copy=off

# review then delete orphans (two steps, gated)
drsync journal cat <name> --type orphan
drsync pass trigger <name> --delete-pass --i-know-this-deletes

# health & audit
drsync agent list ; drsync queue ; drsync report <name>
drsync errors <name> --class EACCES ; drsync events

# certificates
drsync ca init --cn drsync-ca
drsync ca issue --type server --cn coord --dns coord --ip 10.0.0.10
drsync ca issue --type agent  --cn agent-01
```
