# drsync

A **distributed rsync replacement** for migrating and consolidating extremely
large POSIX filesets — billions of files, PB scale, high change rate — between
filesystems presented over NFS, Weka and GPFS mounts, preserving all file data
and metadata at the destination.

A **Go coordinator** (`drsyncd`) owns all state and grants work; a fleet of
high-performance **C agents** (`drsync-agent`) scan source and destination in
parallel, copy only the diff, preserve full metadata (owner/mode/times/xattrs/
POSIX + NFSv4 ACLs/sparse/specials), verify with checksums, and journal every
action. File data never crosses the control network. A single **CLI**
(`drsync`) drives and monitors everything.

## Documentation

| Doc | For |
|-----|-----|
| **[docs/INSTALL.md](docs/INSTALL.md)** | Build, dependencies, topology (dual mounts), mTLS setup, running the coordinator and agents, and **verifying a new setup**. |
| **[docs/ADMIN.md](docs/ADMIN.md)** | Operator guide: concepts, the job-spec reference, the full CLI, worked use cases (migration, consolidation, cutover, orphan deletion, pathological dirs / huge files), monitoring, and troubleshooting. |
| **[ARCHITECTURE.md](ARCHITECTURE.md)** | System design and the ratified decisions (D1–D9). |
| **[webui/](webui/)** | Read-only monitoring console (jobs, convergence, throughput, agent performance, queue/parked shards), served live by the coordinator at `http://<coordinator>:7441/`. |
| `docs/DESIGN-*.md` | Deep-dives: protocol, coordinator, agent, jobspec/CLI. |

## Quick start

```bash
# build
go build -o bin/drsyncd ./coordinator/cmd/drsyncd
go build -o bin/drsync  ./cli/drsync
make -C agent

# run (dev, no TLS): coordinator, then an agent that mounts both trees
bin/drsyncd -data-dir /var/lib/drsync -listen-agent :7440 -listen-http :7441
agent/bin/drsync-agent -c 127.0.0.1:7440 -i "$(hostname -s)" -w 8 -C 16

# drive it
export DRSYNC_SERVER=http://127.0.0.1:7441
drsync job submit myjob.yaml --start
drsync job status myjob --watch
```

See **[docs/INSTALL.md](docs/INSTALL.md)** for the production path (mTLS,
systemd, verification).

## Tests

```bash
go test ./...            # coordinator unit tests
make -C agent test       # agent unit tests (glob matcher, fidelity, ucopy, temp naming)
make webui-test          # console behaviour in jsdom (needs Node >= 20)
make test-all            # go + console
```

Each `test/*_e2e.sh` drives a real coordinator and C agent against a real
tree, and builds the binaries it needs itself:

| Script | Covers |
|--------|--------|
| `e2e.sh` | full lifecycle: sync, converge, verify, gated delete, CLI, events |
| `chunk_e2e.sh` | cross-host chunk fan-out for a large file |
| `chunk_abort_reclaim_e2e.sh` | a chunk group abandoned mid-assembly is reclaimed |
| `chunk_resilience_e2e.sh` | agent dies mid-copy; leases expire and re-grant |
| `deep_e2e.sh` | directory chain deeper than the walker's in-agent limit |
| `dirfix_e2e.sh` | DIRFIX over a directory that fans out to entry-lists |
| `direct_write_e2e.sh` | `copy.direct_write`: new files skip the temp+rename, updates stay atomic |
| `fanout_e2e.sh` | a small volume must still use the whole fleet |
| `filter_e2e.sh` | include/exclude filters |
| `probe_e2e.sh` | per-agent mount probe gates pass start |
| `scale_e2e.sh` | pathological shapes: huge directory, huge file |
| `temp_reclaim_e2e.sh` | sweep reclaims crash residue, spares live temps |
| `tls_e2e.sh` | mTLS, auth enforcement, reconnect-resume |
| `ucopy_e2e.sh` | io_uring copy path with server-side copy disabled |

CI (`.github/workflows/ci.yml`) runs gofmt, vet, build and the Go tests; the
console tests; the agent build and its unit tests; and every e2e script above
as a separate matrix leg.
