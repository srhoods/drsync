# drsync Ansible deployment

Deploys a working drsync fleet — one coordinator (`drsyncd`) and one or more
agents (`drsync-agent`) — from a checked-out copy of this repo, and leaves it
ready to accept `drsync job submit`. This automates the manual steps in
[`docs/INSTALL.md`](../docs/INSTALL.md); read that first if you want the
non-automated walkthrough this mirrors.

## Layout

```
ansible/
  ansible.cfg
  site.yml                        # the whole play: build -> PKI -> coordinator -> agents -> verify
  inventories/example/             # copy this per environment
    hosts.ini
    group_vars/all.yml             # every customisable setting, with defaults
  roles/
    drsync_build/                  # builds drsyncd, drsync CLI, drsync-agent on the controller
    drsync_pki/                    # mints the fleet's mTLS trust material, once
    drsync_common/                 # shared /etc/drsync + install-dir setup
    drsync_coordinator/            # deploys + starts drsyncd
    drsync_agent/                  # deploys + starts drsync-agent
    drsync_verify/                 # confirms the fleet is actually healthy and job-ready
```

## Quick start

```bash
cp -r inventories/example inventories/prod
$EDITOR inventories/prod/hosts.ini              # your real hostnames/IPs
$EDITOR inventories/prod/group_vars/all.yml     # your real paths/ports/split %, etc.

ansible-playbook -i inventories/prod/hosts.ini site.yml
```

A successful run ends with the `drsync_verify` role reporting every agent
`connected: true`. At that point you can `drsync job submit` per
[`docs/ADMIN.md`](../docs/ADMIN.md).

Re-running `site.yml` is safe — every task guards on the state it would
create (see "Idempotency" below), so a re-run against an already-deployed
fleet converges any changed variables and otherwise reports nothing changed.

### Useful tag subsets

```bash
ansible-playbook site.yml --tags coordinator   # just the coordinator host
ansible-playbook site.yml --tags agents        # just the agent hosts
ansible-playbook site.yml --tags verify         # re-run only the health/connectivity checks
```

## Design decisions (and why)

**Binaries are built once on the controller, not on every target host.**
There's no prebuilt binary distribution for drsync — deploying means
building from source (Go for the coordinator/CLI, C for the agent). Building
on each target host would mean installing a full Go + C toolchain on every
production box just to produce a binary once. Instead, `drsync_build` builds
`bin/drsyncd`, `bin/drsync`, and `agent/bin/drsync-agent` once on the
controller (`hosts: localhost` in `site.yml`), and every other role ships
the resulting binary out via `copy:`. This assumes the controller and
targets are the same OS/libc/architecture family (a homogeneous Linux
x86_64 fleet) — there's no cross-compilation handling here.

**mTLS PKI is minted once, idempotently, also on the controller.**
`drsync ca init`/`issue` mint a fleet CA and per-host leaf certificates.
Re-running cert issuance on every play would either constantly regenerate
trust material (breaking already-deployed hosts) or need a `--force` gate
that's easy to trip by accident. Instead, `drsync_pki` guards every
`ca init`/`ca issue` call with `creates:` against the file it would
produce, so a fresh fleet gets a full working PKI from nothing, and a
second run touches none of it. **To rotate a cert on purpose** (e.g. an
agent host was rebuilt and needs a new identity), delete that specific
`.crt`/`.key` pair from `drsync_pki_local_dir` on the controller and
re-run — everything else stays untouched.

**The walker/copy thread split is derived from detected CPU cores, not a
fixed number.** `drsync-agent` has no percentage-based flag — only absolute
`-w`/`-C` integer thread counts (each independently valid 1..256). To honor
"customisable by %", `drsync_agent` computes
`walkers = round(vcpus * drsync_agent_walker_pct / 100)` (and the
complementary `copy_threads`) per host from Ansible's own
`ansible_processor_vcpus` fact, floored at 1 each. This scales automatically
across a heterogeneous fleet — an 8-core box and a 64-core box both get a
sensible split with no per-host tuning. See "Walker/copy thread split"
below for the exact numbers this produces.

Note this split is only the agent's *starting* allocation — adaptive
work-stealing (on by default; `drsync_agent_work_stealing_enabled`) lets
idle threads in either pool pick up the other kind of work at runtime, so
the -w/-C split is a starting point, not a hard ceiling. See
`docs/INSTALL.md` §5 for the full explanation.

**The agent runs as root.** `metadata.owner` (chown to arbitrary uid/gid) and
`metadata.specials` (mknod for device nodes/FIFOs/sockets) both require it —
see `docs/DESIGN-agent.md` ("one process per host, run as root"). The
coordinator does not need root unless `drsync_auth_mode: local` is enabled,
which needs `/etc/shadow` read access (root, or add the coordinator's
service account to the `shadow` group — see `docs/ADMIN.md` §8).

## Variable reference

All variables live in `inventories/<yours>/group_vars/all.yml` with sane
defaults already set; override only what you need to change.

| Variable | Default | Meaning |
|---|---|---|
| `drsync_repo_dir` | `{{ playbook_dir }}/..` | Source tree the controller builds from |
| `drsync_install_dir` | `/usr/local/bin` | Where binaries land on every host |
| `drsync_config_dir` | `/etc/drsync` | Config files + mTLS material |
| `drsync_data_dir` | `/var/lib/drsync` | Coordinator's SQLite state + journals |
| `drsync_pki_local_dir` | `{{ drsync_build_dir }}/pki` | Where the controller stages minted certs before distributing them |
| `coordinator_host` | *(required)* | Coordinator's hostname — used as its cert CN/SAN and the address agents dial |
| `coordinator_ip` | from inventory | Optional extra SAN / dial address |
| `drsync_agent_port` | `7440` | Agent protocol listener |
| `drsync_http_port` | `7441` | REST/WebUI/metrics listener |
| `drsync_tls_enabled` | `true` | Agent<->coordinator mTLS. Disabling runs the fleet in plaintext dev mode. |
| `drsync_agent_walker_pct` / `drsync_agent_copy_pct` | `25` / `75` | Walker/copy thread split, as a % of detected vCPUs. Must sum to 100. |
| `drsync_agent_uring_enabled` | `true` | `false` adds `-U` (force serial `fstatat`) |
| `drsync_agent_work_stealing_enabled` | `true` | `false` adds `-S` (pin fixed `-w`/`-C` sizes) |
| `drsync_agent_nofile_limit` | `1048576` | systemd `LimitNOFILE=` for the agent unit |
| `drsync_agent_fix_io_uring_sysctl` | `true` | Set `kernel.io_uring_disabled=0` if a hardened kernel ships it disabled |
| `drsync_auth_enabled` | `false` | WebUI/API interactive login (`auth.yaml`) |
| `drsync_https_enabled` | `false` | REST/WebUI listener HTTPS (`certs.yaml`); self-signed cert generated automatically if no `drsync_https_cert_src`/`_key_src` given |
| `drsync_smtp_enabled` | `false` | Email notifications (`smtp.yaml`) |

See `roles/*/defaults/main.yml` for the complete set, including every
`drsync_auth_*`/`drsync_smtp_*` sub-setting.

## Walker/copy thread split

| vCPUs | 25/75 (default) | 50/50 | 10/90 |
|---|---|---|---|
| 4 | 1 / 3 | 2 / 2 | 1 / 4 (rounds up to keep >=1 each side) |
| 8 | 2 / 6 | 4 / 4 | 1 / 7 |
| 32 | 8 / 24 | 16 / 16 | 3 / 29 |
| 64 | 16 / 48 | 32 / 32 | 6 / 58 |

The `drsync_agent` role prints the computed split for every host during a
run (`debug` task), so you can confirm it before the service starts.

## Idempotency

Every task either:
- checks the exact state it would create before acting (`creates:` on the
  PKI/build commands, `force: false` on the API token distribution), or
- converges declarative state that's a no-op when unchanged (`file`,
  `template`, `copy`, `systemd` — Ansible's own change-detection), or
- is a read-only check (`stat`, `assert`, the `drsync_verify` health polls).

Config file changes (`auth.yaml`/`certs.yaml`/`smtp.yaml`/the systemd units)
notify a handler that restarts the affected service — `drsyncd`/`smtp.yaml`
etc. are read once at startup (`docs/INSTALL.md`), so a restart is the
correct way to pick up an edit, and it only fires when something actually
changed.

## What's deliberately out of scope (for now)

- Cross-architecture builds (controller and targets must match).
- Automated cert *rotation* (only initial issuance) — see "mTLS PKI" above
  for the manual rotation step.
- Multiple coordinators / HA — the coordinator is a single control-plane
  process per fleet (see `docs/DESIGN-coordinator.md`).
