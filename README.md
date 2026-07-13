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
bash test/e2e.sh         # full lifecycle: sync, converge, verify, gated delete, CLI, events
bash test/tls_e2e.sh     # mTLS + auth enforcement + reconnect-resume
bash test/scale_e2e.sh   # entry-list (pathological dir) + chunked copy (huge file)
```
