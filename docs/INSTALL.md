# drsync — Installation & First-Run Verification

drsync is a distributed rsync replacement for migrating and consolidating very
large POSIX filesets (billions of files, PB scale, high change rate) between
filesystems reachable over NFS / Weka / GPFS mounts. It has three programs:

| Program         | Language | Role |
|-----------------|----------|------|
| `drsyncd`       | Go       | **Coordinator** (control plane). Owns all job/shard/lease state in SQLite, grants work, serves the REST + WebSocket API and Prometheus metrics. Never touches file data. |
| `drsync-agent`  | C        | **Data-plane agent.** Scans source and destination, diffs, copies, preserves metadata, verifies, journals. One or more per fleet; each mounts both filesystems. |
| `drsync`        | Go       | **Operator CLI.** A thin client of the coordinator REST API; also mints the mTLS certificate material (`drsync ca`). |

File data moves **only** through the agents, host-locally between the two
mounts. It never crosses the control network between agent and coordinator.

---

## 1. Topology

```
                 ┌───────────────┐   REST/WS :7441   ┌──────────┐
                 │  coordinator  │◀──────────────────│  drsync  │  (operator CLI)
                 │   (drsyncd)   │                   └──────────┘
                 └───────┬───────┘
             agent proto │ :7440  (protobuf frames, mTLS)   ← control only
        ┌────────────────┼────────────────┐
   ┌────▼────┐      ┌────▼────┐       ┌────▼────┐
   │ agent 1 │      │ agent 2 │  ...  │ agent N │   each host mounts BOTH:
   │  ▲   ▲  │      │  ▲   ▲  │       │  ▲   ▲  │     /mnt/src   (source)
   └──┼───┼──┘      └──┼───┼──┘       └──┼───┼──┘     /mnt/dst   (destination)
      │   │            │   │             │   │
   source dst       source dst        source dst    ← file data, host-local
```

**Requirements**

- One coordinator host (modest: a few cores, a fast local disk for the state
  store + journals). Not in the data path.
- One or more agent hosts. Each agent host **must mount both the source and the
  destination filesystem**, at the *same absolute paths the job spec names*
  (e.g. every agent mounts the source at `/mnt/src` and the destination at
  `/mnt/dst`). The coordinator hands agents the spec's `source.path` /
  `destination.path` verbatim.
- Reachability: agents dial the coordinator's agent port (default `7440`). The
  operator/CLI reaches the REST port (default `7441`).

---

## 2. Build dependencies

### Coordinator + CLI (Go)

- **Go ≥ 1.26.** That is all — the SQLite driver is `modernc.org/sqlite`
  (pure Go), so no cgo, no libsqlite, and static-friendly builds
  (`CGO_ENABLED=0`). All other modules are vendored through the module cache.

### Agent (C)

- A **C11 compiler** (gcc or clang).
- **libzstd** + headers — journal batch compression. (`libzstd-devel` on
  RHEL / Rocky / Alma, `libzstd-dev` on Debian / Ubuntu.)
- **OpenSSL ≥ 1.1.1** + headers — mTLS to the coordinator (TLS 1.3).
  (`openssl-devel` / `libssl-dev`.)
- **pthreads**, **glibc**, a **Linux kernel** new enough for `statx(2)` and
  `openat2(2)` (≈ 5.6+). Both have runtime fallbacks (`fstatat`, component-wise
  `O_NOFOLLOW` open), so older kernels still work, just slower/less strict.
- Vendored, no package needed: `xxhash` (checksums) and the `openat2` uapi
  header live under `agent/vendor/`.

`io_uring` is **optional**: the batched-`statx` scan path uses it when
available and silently falls back to serial `fstatat` otherwise. See §6.

Example dependency install:

```bash
# RHEL / Rocky / Alma 9
sudo dnf install -y gcc make libzstd-devel openssl-devel   # + Go 1.26 from go.dev
# Debian / Ubuntu
sudo apt install -y build-essential libzstd-dev libssl-dev # + Go 1.26 from go.dev
```

---

## 3. Build

From the repository root:

```bash
# Coordinator + CLI  →  bin/drsyncd, bin/drsync
go build -o bin/drsyncd ./coordinator/cmd/drsyncd
go build -o bin/drsync  ./cli/drsync

# Agent  →  agent/bin/drsync-agent
make -C agent
```

Install the binaries where each host expects them:

```bash
# coordinator host
sudo install -m0755 bin/drsyncd /usr/local/bin/
sudo install -m0755 bin/drsync  /usr/local/bin/     # CLI, wherever operators run it
# each agent host
sudo install -m0755 agent/bin/drsync-agent /usr/local/bin/
```

---

## 4. Set up mTLS (recommended for any real deployment)

The coordinator authenticates every agent with a client certificate and
presents its own server certificate; agents verify the coordinator the same way.
`drsync ca` mints all of it. Run this once on a trusted machine:

```bash
mkdir -p pki && cd pki

# 1. the fleet CA (ECDSA P-256; ca.crt + ca.key)
drsync ca init --cn drsync-ca

# 2. the coordinator's server cert — name every address agents will dial
drsync ca issue --type server --cn coord.example.com \
    --dns coord.example.com --ip 10.0.0.10

# 3. one client cert per agent (repeat per host; CN is just a label)
drsync ca issue --type agent --cn agent-01
drsync ca issue --type agent --cn agent-02
```

Distribute:

- **Coordinator** gets `ca.crt`, `coord.example.com.crt`, `coord.example.com.key`.
- **Each agent** gets `ca.crt` and its own `agent-NN.crt` / `agent-NN.key`.
- Keep `ca.key` offline; it is only needed to issue more agent certs.

> Certificate keys are written `0600`. Server certs need a SAN that matches what
> agents dial — pass `--ip`/`--dns` accordingly (a bare `--cn host` also becomes
> a SAN as a fallback). Default validity: CA 10 years, leaves 825 days.

Running **without** TLS is supported for a throwaway lab only: omit all TLS
flags and both the coordinator and agents log a loud plaintext warning.

---

## 5. Run

### Coordinator

```bash
drsyncd \
  -data-dir /var/lib/drsync \
  -listen-agent 0.0.0.0:7440 \
  -listen-http  0.0.0.0:7441 \
  -api-token "$(cat /etc/drsync/api-token)" \
  -tls-cert /etc/drsync/coord.example.com.crt \
  -tls-key  /etc/drsync/coord.example.com.key \
  -tls-ca   /etc/drsync/ca.crt \
  -smtp-config /etc/drsync/smtp.yaml \
  -lease-ttl 30s -heartbeat-interval 5s -log-level info
```

| Flag | Default | Meaning |
|------|---------|---------|
| `-data-dir` | `/var/lib/drsync` | SQLite state store **and** journal segments. Put it on a persistent, reasonably fast local disk. |
| `-listen-agent` | `:7440` | Agent protocol listener. |
| `-listen-http` | `:7441` | REST API, `/metrics`, `/healthz`, WebSocket. |
| `-api-token` | *(empty)* | Bearer token for the REST API. Empty = no auth (dev only). |
| `-tls-cert`/`-tls-key`/`-tls-ca` | *(empty)* | Server cert/key and the CA bundle used to verify agent client certs. All three or none. |
| `-smtp-config` | `/etc/drsync/smtp.yaml` | SMTP server settings for email notifications. The **default path is optional**: if it is absent, notifications are silently disabled. A path given explicitly must exist and validate. |
| `-lease-ttl` | `30s` | Shard lease TTL; a dead agent's work is requeued after this. |
| `-heartbeat-interval` | `5s` | Expected agent heartbeat cadence. |
| `-log-level` | `info` | `debug`\|`info`\|`warn`\|`error`. |

### 5.1 Email notifications (optional)

To enable the per-pass / end-of-job emails a job spec can request
(`spec.notifications`, see DESIGN-jobspec.md §1.2), give the coordinator an SMTP
config. It is read **once at startup**; changes need a coordinator restart.

`/etc/drsync/smtp.yaml`:

```yaml
host: smtp.example.com          # required
port: 587                       # optional; default per security: starttls→587, tls→465, none→25
security: starttls              # starttls (default) | tls (implicit, ~465) | none (plaintext, dev only)
username: drsync@example.com    # optional; omit for an unauthenticated relay
password: "s3cr3t"              # PLAIN auth (send over starttls/tls only)
from: "drsync <drsync@example.com>"   # required; header + envelope sender
subject_prefix: "[drsync]"      # optional; prepended to every subject
helo: coord.example.com         # optional; defaults to the coordinator hostname
timeout_seconds: 30             # optional; bounds the whole SMTP exchange
```

- The file holds a password, so restrict it: `chown drsyncd:drsyncd /etc/drsync/smtp.yaml && chmod 600 /etc/drsync/smtp.yaml`.
- Recipients are **per job** (`spec.notifications.recipients`), not here — this
  file only describes *how* to send.
- Unknown keys are rejected (typo safety), matching the job-spec decoder.
- Delivery is best-effort and asynchronous: a send failure is logged
  (`notify: email send failed …`) and never affects the migration. On startup
  the coordinator logs `email notifications enabled` (or `… disabled …`) so you
  can confirm the config was picked up.

### Agent (on every agent host)

```bash
drsync-agent \
  -c coord.example.com:7440 \
  -i "$(hostname -s)" \
  -w 8 -C 16 \
  -A /etc/drsync/ca.crt \
  -E /etc/drsync/agent-01.crt \
  -K /etc/drsync/agent-01.key
```

| Flag | Default | Meaning |
|------|---------|---------|
| `-c host:port` | `127.0.0.1:7440` | Coordinator agent endpoint. |
| `-i agent-id` | machine-id derived | Stable agent identity (survives restarts; used for lease resume). |
| `-w N` | `4` | Walker threads (scan/diff concurrency). |
| `-C N` | `8` | Copy-pool threads (data movement concurrency). |
| `-U` | off | Disable io_uring (force serial `fstatat`); A/B testing / escape hatch. |
| `-S` | off | Disable adaptive work-stealing; pin the walker/copy pools to their fixed `-w`/`-C` sizes. |

**Adaptive work-stealing (default on).** The `-w`/`-C` split is a *starting*
allocation, not a fixed ceiling: when one phase idles a pool, its threads flex
to the other kind of work. Idle walkers help drain the copy backlog, and idle
copy threads pull shards to help crawl — so a job that shifts from data-heavy
(pass 1) to metadata-heavy (later passes) rebalances automatically instead of
leaving hosts waiting. One copy thread stays a reserved drainer, which
guarantees the pool can never deadlock. Total threads are unchanged (no
overcommit); they just change role. The agent logs `work-stealing: copy threads
crawled N shards, walkers drained M copies` at shutdown so you can see it act;
`-S` reverts to fixed pools for A/B comparison.
| `-A`/`-E`/`-K` | *(none)* | CA bundle / client cert / client key for mTLS. All three or plaintext. |

The agent **auto-reconnects**: if the control connection drops it retries with
backoff (0.5 s → 15 s), keeping its worker pools and in-flight leases alive so
the coordinator resumes it without re-queuing work. A coordinator restart is
detected (fleet-epoch change) and logged; the agent resumes with fresh grants.

### systemd (recommended)

`/etc/systemd/system/drsyncd.service` (coordinator):

```ini
[Unit]
Description=drsync coordinator
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/drsyncd -data-dir /var/lib/drsync \
  -listen-agent 0.0.0.0:7440 -listen-http 0.0.0.0:7441 \
  -api-token %S/drsync/api-token \
  -tls-cert /etc/drsync/coord.crt -tls-key /etc/drsync/coord.key -tls-ca /etc/drsync/ca.crt
Restart=always
RestartSec=2
StateDirectory=drsync

[Install]
WantedBy=multi-user.target
```

`/etc/systemd/system/drsync-agent.service` (each agent host):

```ini
[Unit]
Description=drsync agent
# ensure both mounts are present before starting
RequiresMountsFor=/mnt/src /mnt/dst
After=network-online.target remote-fs.target
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/drsync-agent -c coord.example.com:7440 -i %H -w 8 -C 16 \
  -A /etc/drsync/ca.crt -E /etc/drsync/%H.crt -K /etc/drsync/%H.key
Restart=always
RestartSec=2
# Open-file limit. The agent's fd use scales with -w/-C (io_uring rings, held
# directory fds, concurrent copies), so high-core hosts can exhaust a low
# default (systemd's DefaultLimitNOFILE is often 1024).
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
```

Because the agent reconnects on its own, `Restart=always` is a backstop, not the
primary recovery path.

> **Open files (`EMFILE` / "too many open files").** `/etc/security/limits.conf`
> and `/etc/security/limits.d/*` are applied by **pam_limits — login sessions
> only**. **systemd services do not go through PAM**, so raising the limit there
> has *no effect* on the agent; set **`LimitNOFILE=`** in the unit (above) and
> `systemctl daemon-reload && systemctl restart drsync-agent`. As a safety net
> the agent also raises its own soft limit to the hard ceiling at startup (it
> logs `open-file limit soft=… hard=…`), so a high `LimitNOFILE` *or* a high
> `DefaultLimitNOFILE`/hard limit is enough; check the startup log to confirm
> the effective value.

---

## 6. io_uring note

The batched-`statx` scan path (the "NFS scan multiplier") uses raw `io_uring`.
Some hardened kernels disable it via `sysctl kernel.io_uring_disabled` (RHEL 9
ships it set to `2` = disabled for everyone). Check and enable on agent hosts if
you want the ring path:

```bash
sysctl kernel.io_uring_disabled                 # 0 = enabled
echo 'kernel.io_uring_disabled = 0' | sudo tee /etc/sysctl.d/99-drsync.conf
sudo sysctl --system
```

If it stays disabled, drsync still works — the agent falls back to serial
`fstatat` (lower scan throughput) and logs the mode at startup.

---

## 7. Verify a new setup

Do these in order the first time you stand up a fleet.

### 7.1 Coordinator is up

```bash
curl -fsS http://coord.example.com:7441/healthz        # → ok
curl -fsS http://coord.example.com:7441/metrics | head # Prometheus exposition
```

### 7.2 Agents registered (mTLS handshake succeeded)

Point the CLI at the coordinator (over the REST port), then list agents:

```bash
export DRSYNC_SERVER=http://coord.example.com:7441
export DRSYNC_TOKEN=$(cat /etc/drsync/api-token)

drsync agent list
# AGENT       CONNECTED   HOST          ...
# agent-01    true        host01        ...
# agent-02    true        host02        ...
```

Every agent host you started should show `CONNECTED true`. If one is missing,
check that host's agent log for a TLS verify error (wrong CA, cert not signed by
the fleet CA, or a server-cert SAN that doesn't match the dialed address).

### 7.3 A smoke sync converges

Create a tiny disjoint source/destination *under the real mounts* and run one
job end to end:

```bash
# on an agent host, seed a few files (paths must exist on ALL agents' mounts)
mkdir -p /mnt/src/_smoke/a /mnt/dst/_smoke
echo hello > /mnt/src/_smoke/a/f1.txt
head -c 1048576 /dev/urandom > /mnt/src/_smoke/a/blob.bin

cat > smoke.yaml <<'YAML'
apiVersion: drsync/v1
kind: Job
metadata: { name: smoke }
spec:
  source:      { path: /mnt/src/_smoke }
  destination: { path: /mnt/dst/_smoke }
  passes: { max: 4, converge_when: { delta_files_below: 1 } }
  verify: { checksum: { sample_rate: 1.0 } }   # checksum everything for the smoke
YAML

drsync job submit smoke.yaml --start
drsync job status smoke --watch          # follows the live event stream to COMPLETED
```

Confirm:

- `drsync job status smoke` ends at `COMPLETED`.
- The per-pass table shows pass 1 copied the files and the final pass copied
  **0** (converged).
- `verify_ok > 0` and `verify_fail == 0`.
- The destination content matches: `diff -r /mnt/src/_smoke /mnt/dst/_smoke`.
- `drsync report smoke` shows the convergence curve and zero errors.

Then clean up the smoke tree. You are ready to run real jobs — see
[ADMIN.md](ADMIN.md).

### 7.4 In-repo test suites (for validating a build)

If you have the source tree, these exercise the whole stack on loopback (no real
mounts needed):

```bash
bash test/e2e.sh         # full lifecycle: sync, converge, verify, gated delete, CLI, events
bash test/tls_e2e.sh     # mTLS handshake + auth enforcement + reconnect-resume
bash test/scale_e2e.sh   # pathological directory (entry-list) + huge file (chunked copy)
go test ./...            # coordinator unit tests
```

All should print a `PASS:` line.
