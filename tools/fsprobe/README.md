# fsprobe — filesystem metadata-path profiler

drsync's speed on a many-small-files migration is set by the destination
filesystem's **per-file metadata cost** and by how much of that cost
**serializes on a single directory** — not by bandwidth. Local XFS creates a
file in microseconds; GPFS and Weka answer each create, chmod, chown, and
rename with a round trip to a metadata server, and creates into one directory
may serialize on that directory's lock or token. When they do, adding agents
and copy threads buys nothing.

`fsprobe` reproduces drsync's exact per-file write sequence in isolation so you
can measure this **on the real source and destination filesystems**, with no
drsync deployment involved. It links nothing from drsync and needs only a C
compiler and pthreads.

```
cc -O2 -pthread -o fsprobe fsprobe.c      # or: make        (make static for scp)
```

## The per-file sequence it measures

It mirrors `agent/src/copy.c`. Each bracketed step is a drsync option and a
`fsprobe` flag, so the **defaults measure what drsync actually does**:

```
openat(O_CREAT|O_EXCL) → [ftruncate] → write → [setxattr] → [fdatasync]
    → [fchown] → [fchmod] → [futimens] → [rename] → close
```

`fdatasync` is **off** by default, matching what matters on NFS/GPFS/Weka:
`close()` already commits the data (close-to-open consistency), so per-file
`fdatasync` is largely redundant there. Add `--fsync` to measure `per_file`
mode. On local XFS `fdatasync` is the dominant cost and `--fsync` shows it.

## Modes

Run each **on the destination filer** (and `read` on the source):

| Command | Answers |
|---|---|
| `fsprobe info <dir>` | What filesystem is this, and how much space? |
| `fsprobe write <dir>` | Per-operation latency breakdown + files/s. **Start here.** |
| `fsprobe scale <dir>` | Does throughput scale with threads, or is one directory the wall? |
| `fsprobe spread <dir>` | Same files into 1 dir vs many — is the bottleneck per-directory? |
| `fsprobe ablate <dir>` | Which step costs what — drops one drsync step per row. |
| `fsprobe read <dir>` | Source characterization: readdir / statx / read rates. |

Options: `-n` files (default 20000), `-s` bytes/file (1100), `-t` threads (8),
`-d` spread across N subdirs (1), `--fsync`, `--no-{trunc,xattr,chown,chmod,times,rename}`, `--keep`.

`write`, `scale`, `spread`, and `ablate` create their files under
`<dir>/dNNN/` and delete them at the end (use `--keep` to inspect, then point
`read` at `<dir>/d000`).

## Reading the results

**1. `write` — where the time goes.** The per-op table gives mean and p50/p90/p99/max
for each syscall. On a clustered filesystem the metadata ops (open, chmod,
chown, ftruncate, rename) each cost a full server round trip while `write`
costs nothing — the opposite of local disk. Example from a slow NFS mount:

```
op             count      mean       p50       max (ms)
open             300    28.39     21.13    399.97
ftruncate        300    13.53     11.12     55.62
write            300     0.01      0.01      0.03      ← data is free
fchmod           300    68.44     55.40    461.30      ← the dominant cost here
fchown           300    65.56     61.66    199.82
futimens         300    14.05     11.12    144.44
rename           300    13.93     11.12    122.25
```

**2. `scale` — is the directory the wall?** Throughput at 1 → 64 threads into
one directory. If it barely rises (e.g. 7 → 11 files/s from 1 → 16 threads),
the filesystem serializes creates/renames on the directory, and **no amount of
fleet capacity will help** — this is the single most important thing to know.

**3. `spread` — confirm it.** One directory vs many. A large multiplier (e.g.
3×) going wide confirms the bottleneck is per-directory rather than the
filesystem overall.

**4. `ablate` — what to cut.** Each row drops one drsync step and shows the
speedup. If `no fchmod` or `no rename (direct)` jumps significantly, that step
is a candidate for a drsync optimization (fold chmod into the open mode; skip
the temp+rename for new files).

## Sizing `-n` to the filesystem

Pick `-n` so each phase runs a few seconds — long enough to average out
scheduling noise, short enough not to wait forever. `scale` and `ablate` run
**seven and eight phases**, so their per-phase count wants to be smaller.

- **Fast (local XFS, ~40k files/s):** phases finish in well under a second, so
  small counts are dominated by noise — `write`/`ablate` deltas jitter (you may
  even see a "faster with more work" artifact). Use `-n 100000+` if you want
  stable numbers here, though the interesting measurements are on the slow
  destination, not local disk.
- **Slow (GPFS/Weka/NFS, tens of files/s):** a single file is ~50–150 ms, so
  keep counts modest or a run takes an hour. Good starting points:

```
fsprobe info   <dst>
fsprobe write  <dst> -n 5000              # per-op breakdown (~a few min)
fsprobe scale  <dst> -n 1000              # 7 phases; the shape is what matters
fsprobe spread <dst> -n 2000 -t 16 -d 16
fsprobe ablate <dst> -n 1000              # 8 phases
```

Send those back plus `mount | grep <dst>`, and for the source
`fsprobe read <src-dir>`. The **rates and the shape of the scaling curve** are
what matter, not the absolute file count — a flat `scale` curve at 1000 files
says as much as at 100000.
