# genfixture — synthetic fidelity-test tree generator

drsync's job is to make a destination byte- and metadata-identical to a
source across every entry type and attribute it knows how to read, copy, or
verify. Testing that with hand-picked files (see `test/e2e.sh`'s handful of
xattrs and one ACL) covers the happy path; it does not cover the combinatorics
— sparse layouts, 100+ xattrs on one entry, both ACL flavors, nanosecond
timestamps, suid/sticky bits, dangling symlinks, symlink targets with
newlines, 255-byte names — that docs/DESIGN-agent.md §9 calls for in its
"fidelity matrix suite."

`genfixture` builds that tree. Point a real `drsync` job's source at its
output, sync, and diff — that's the fidelity test.

```
cc -O2 -o genfixture genfixture.c      # or: make        (make static for scp)
```

It links nothing from drsync (like `tools/fsprobe`) and needs only a C
compiler; the POSIX ACL blobs are built directly from the kernel UAPI headers
(`linux/posix_acl*.h`), so it needs no libacl either.

## Usage

```
genfixture <directory> <total-size> [depth] [options]
```

- `<directory>` — root to build the tree under. Created if missing.
- `<total-size>` — approximate total bytes of regular-file content, e.g.
  `500M`, `2G`, or a bare byte count.
- `[depth]` — directory nesting depth (default 5).

Options: `--seed N` (reproduce a prior run's exact tree), `--no-acl`,
`--no-xattr`, `--no-special` (skip device/socket creation attempts).

```
genfixture /mnt/src 2G 6
genfixture /mnt/src 2G 6 --seed 12345      # rebuild the same tree elsewhere
```

## What it creates

Per directory (recursed to `depth`, two subdirectories per level):

| Entry | Notes |
|---|---|
| Regular files | random sizes (mostly small, some mid, occasional multi-MiB); random content per file; ~1-in-6 sparse (head/hole/tail via `ftruncate`+`lseek`, matching `preserve_sparse`'s SEEK_DATA/SEEK_HOLE path); occasional setuid/setgid/sticky bit |
| Hardlink | one link to the level's last regular file (drsync copies each link independently — `nlink_dup` accounting in `agent/src/walker.c`, not link preservation) |
| Symlinks | one ordinary, one dangling, one with a literal newline in the target |
| FIFO | always (`mkfifo`, no privilege needed) |
| Device node | char device cloned from `/dev/null`'s major/minor; needs `CAP_MKNOD` — skipped (counted, not fatal) without it |
| Socket | `AF_UNIX` bind; a real socket special file |
| 255-byte name | one per shallow level — the exact `NAME_MAX` boundary |
| Wide-xattr directory | the tree root carries 120+ small `user.*` xattrs |

Metadata applied to (almost) everything, matching `struct job_options`'s
toggles (`agent/src/msgs.h`) and `struct estat`'s fields (`agent/src/agent.h`):

- **owner/group** — chowned to a random non-self uid/gid (best-effort; needs
  `CAP_CHOWN` to actually take effect, same as an unprivileged drsync agent)
- **mode**, including setuid/setgid/sticky on some regular files
- **nanosecond atime/mtime**, deliberately non-"now" and non-round, so a
  second-granularity or atime-dropping copy fails verify immediately
- **xattrs** in `user.*`, `trusted.*`, and `security.*` (the namespaces
  `agent/src/xattr.c`'s `classify()` copies) — `trusted.*`/`security.*` need
  privilege to set and are silently skipped without it
- **POSIX ACLs** (`system.posix_acl_access`, plus `system.posix_acl_default`
  on directories) — hand-built as the kernel's on-disk
  `posix_acl_xattr_{header,entry}` blob, so `getfacl` reads them back exactly
- **a synthetic `system.nfs4_acl`** — opaque bytes under the real xattr name,
  to exercise the raw-copy and untranslatable-policy paths even off an NFSv4
  mount (real NFSv4 ACL XDR semantics are never interpreted there anyway)

`security.selinux` is deliberately never touched — `xattr.c` classifies it
`XC_IGNORE` (destination-host policy), so setting it here would just test a
path drsync intentionally never copies.

## Never overwrites

Every path is created with `O_EXCL`, `mkdir`, `symlink`, `mkfifo`, `mknod`, or
`bind` — all inherently exclusive. An existing path is left untouched, counted
as skipped, and the run continues. Re-running `genfixture` against a
directory that already has data (or a prior `genfixture` run, or a
`--seed`-matched replay) extends it rather than clobbering anything. This
means you can layer runs — e.g. one broad run plus a second targeted one with
different flags — without losing the first.

## Progress and output

A status line to stderr roughly once a second (percent of `total-size`
written, entry count, throughput, elapsed time), then a summary to stdout:

```
[genfixture]   54.2%    41823 files   270.8 MiB /  500.0 MiB   210.3 MiB/s     87s elapsed

genfixture summary:
  regular files   : 41823 (6971 sparse), 524288911 bytes
  directories     : 1364
  symlinks        : 4092
  hardlinks       : 1364
  fifos           : 1364
  device nodes    : 1364
  sockets         : 1364
  xattrs set      : 292761
  acl xattrs set  : 2848
  skipped existing: 0
  errors          : 0
  seed            : 4172837592038109284 (pass --seed 4172837592038109284 to reproduce)
```

`errors` is anything unexpected (ENOSPC, a permission failure other than the
expected privilege gaps above). `skipped existing` is expected and harmless on
a re-run. A non-zero `errors` count is the tool's own exit code.

## Using it as a fidelity test

```
genfixture /mnt/src 2G 6
drsync job submit job.yaml --set spec.source.path=/mnt/src \
                            --set spec.destination.path=/mnt/dst --start
# ... wait for COMPLETED ...
diff -r /mnt/src /mnt/dst          # ignore "is a fifo"/"is a socket" lines;
                                    # diff -r does not follow dangling symlinks,
                                    # so compare those with readlink instead
getfacl -pcR /mnt/src | diff - <(getfacl -pcR /mnt/dst)
```

Run as root to also exercise device-node copy (needs `CAP_MKNOD` on both
sides) and real (non-self) chown/chgrp.
