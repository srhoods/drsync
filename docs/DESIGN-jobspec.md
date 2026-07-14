# drsync Detailed Design — Job Specification (YAML) & CLI

**Status:** Detailed design v1 — 2026-07-10
**Decision D9:** YAML job specs are the primary interface; every field has a CLI flag
equivalent for ad-hoc jobs and overrides. YAML parsing exists **only** in the Go control
plane — agents receive fully-resolved options as protobuf `JobOptions`.

> **Implementation status (2026-07-11):** the `drsync` CLI shipped
> (`cli/drsync`, a thin REST/WebSocket client — no coordinator internals).
> Commands: `job submit` (with repeatable `--set` YAML-path overrides,
> `--dry-run`, `--start`), `job list`, `job status` (`--watch` follows the
> event feed and exits when the job settles), `job start|pause|resume|cancel`,
> `pass trigger` (delete passes require `--delete-pass --i-know-this-deletes`
> in addition to the API-side confirm string — both halves of the D5 gate),
> `agent list`, `errors`, `journal cat` (`--type/--path/--jsonl`), `report`
> (`--json` for machines), `queue`, and `events` (raw JSONL event tail).
> Connection via `--server`/`--token` or `DRSYNC_SERVER`/`DRSYNC_TOKEN`.
> Not yet: ad-hoc flag-built specs (`--src/--dst` without a file),
> `drsync job update`, and `drsync ca` (arrives with mTLS).

---

## 1. Job Spec Schema

Annotated, complete example (defaults shown are the shipped defaults, tuned per D7 for
4×100 GbE agents):

```yaml
apiVersion: drsync/v1
kind: Job
metadata:
  name: home-consolidation          # unique job name (also the journal directory name)
  description: "migrate /gpfs/home to weka"

spec:
  source:
    path: /mnt/src/home              # must be identical on every agent host (D2)
  destination:
    path: /mnt/dst/home

  filters:                           # evaluated in order, first match wins; rsync-like
    - exclude: "**/.snapshot/**"     # always exclude snapshot dirs of either FS
    - exclude: "**/*.tmp"
    - include: "**"                  # implicit default

  passes:
    max: 10                          # hard stop; convergence usually stops earlier
    converge_when:
      delta_files_below: 100000      # OR-combined convergence criteria
      delta_bytes_below: 50GiB
    schedule: continuous             # continuous | manual (operator triggers each pass)

  copy:
    chunk_threshold: 1GiB            # files >= this are split into chunk tasks
    chunk_size: 8GiB                 # size of each chunk task
    buffer_size: 1MiB                # io_uring buffer unit
    preserve_sparse: true            # SEEK_HOLE/DATA; auto-fallback to zero-detect
    server_side_copy: auto           # try copy_file_range (NFSv4.2), fallback read/write
    temp_naming: ".drsync.tmp.{id}"  # in-progress destination names
    fsync: per_file                  # per_file | batched(n)

  metadata:
    owner: true                      # uid/gid (needs root)
    mode: true
    times: true                      # atime+mtime, ns precision
    xattrs: true                     # all readable namespaces
    acls:
      posix: true
      nfs4: true                     # D8: on by default
      untranslatable: warn           # warn | fail | skip  (journaled either way)
    hardlinks: report                # fixed: report-only (D3); field reserved
    specials: true                   # device nodes, FIFOs, sockets (needs root)

  verify:
    mode: on                         # on (default) | off — off skips the verify phase
    metadata: all                    # every entry re-checked after copy pass
    checksum:
      algorithm: xxh3-128
      sample_rate: 0.01              # D4: deterministic 1% by hash(rel_path)
      on_mismatch: recopy            # recopy | fail

  deletes:
    mode: report                     # D5: report | mirror (mirror requires --i-know flag
                                     # at pass trigger time as a second gate)

  limits:
    bandwidth_per_agent: 0           # 0 = unlimited; else bytes/s throttle
    iops_per_agent: 0
    parallel_shards_per_agent: 32    # outstanding shard leases per agent
    src_load_ceiling: null           # optional: pause if src latency p99 exceeds N ms

  tuning:                            # rarely touched; sane defaults from D7 sizing
    shard_budget: 250000             # entries processed before pushing subdirs back
    dir_split_threshold: 50000       # single-dir size that triggers entry-list sharding
    statx_batch: 256                 # in-flight statx per walker = io_uring ring depth (pow2, 1–4096)
    mtime_slop_ns: 1000000           # 1ms slop for cross-FS timestamp granularity
```

Quantity suffixes: `KiB/MiB/GiB/TiB` (binary), plain integers are bytes/counts.

### 1.1 Validation

`drsync job submit spec.yaml` validates before anything runs:

- schema + unknown-field rejection (typo safety),
- src/dst paths exist and are directories **on every registered agent** (coordinator
  issues a probe task to each agent; catches missing/misordered mounts before pass 1),
- src ≠ dst and neither is a prefix of the other,
- destination mount has plausible free space (statfs vs. src estimate once pass 1 has a
  running total; hard check is per-write ENOSPC handling),
- filters compile.

## 2. CLI

Thin client of the REST API (Go, same binary ships everywhere):

```
drsync job submit spec.yaml [--set spec.verify.checksum.sample_rate=0.05] [--dry-run]
drsync job list | status <name> [--watch]
drsync job pause|resume|cancel <name>
drsync pass trigger <name> [--delete-pass --i-know-this-deletes]
drsync agent list                       # fleet view: state, throughput, versions
drsync errors <name> [--class EACCES] [--tail]
drsync journal cat <name> --pass 3 [--type orphan|error|fidelity] [--jsonl]
drsync report <name>                    # migration report: fidelity summary, per-pass
                                        # deltas, orphans, nlink>1 duplication cost
drsync ca init | issue --agent <host>
```

- `--set` overrides use the YAML path syntax; ad-hoc jobs can be defined entirely with
  flags (`drsync job submit --src ... --dst ... --name ...`) which build the same spec
  object internally.
- `--dry-run` runs a full pass pipeline with copy/metadata/delete execution stubbed:
  everything is walked, diffed and journaled (`would_copy`, `would_delete`), giving an
  exact preview and a free scan benchmark.

## 3. Resolution Pipeline

```
YAML file ──parse+validate──▶ JobSpec (Go struct)
        ──apply --set overrides──▶ resolved spec (immutable, hashed)
        ──persist to state store──▶ job row (spec blob + hash)
        ──translate──▶ protobuf JobOptions (what agents see, cached by hash)
```

The resolved spec is immutable once the job starts; changing tuning mid-job means
`drsync job update` which bumps the options hash and rolls out at shard-grant
granularity (agents apply new options to newly leased shards only — no mid-shard
behavior changes).
