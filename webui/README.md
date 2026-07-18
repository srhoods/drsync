# drsync WebUI

An operations console for monitoring and driving a drsync cluster — jobs, pass
convergence, aggregate throughput, agent performance, the shard queue /
parked-shard triage view, and the journal-backed error browser.

It is wired to a live coordinator: no build step, no framework — a single
self-contained HTML file.

## Running it

The coordinator embeds and serves the console:

```
drsyncd -listen-http :7441 …      # then browse to
http://<coordinator>:7441/        # (also /ui)
```

Served same-origin, `fetch`/WebSocket calls need no configuration. If the
coordinator sets `-api-token`, open **⚙ connection** (top right) and paste the
token — it is stored in `localStorage` and sent as a bearer token; the events
WebSocket receives it as a `?token=` query parameter.

You can also open `webui/console.html` directly from disk against a remote
coordinator: use **⚙ connection** to set the coordinator URL (and token). The
coordinator sends permissive CORS headers so this cross-origin case works too.
The connection indicator (top bar) is green when polling and the event socket
are healthy, amber while connecting, red when the coordinator is unreachable.

Data refreshes every 2.5 s. Live state changes (job/pass transitions, agent
up/down, parked-shard alerts) arrive over the events WebSocket and trigger an
immediate refresh, and the 1 Hz `stats` frames are folded into the selected
job's detail so its counters advance between polls. Per-agent rates are derived
by differencing successive `/metrics` samples, so throughput/scan/copy figures
populate after the second poll.

## Views

- **Overview** — cluster KPI strip; a jobs list with lifecycle state
  (SCANNING → DIRFIX → VERIFY → DELETE → COMPLETE); a selected-job detail with
  the **convergence curve** (Δfiles per pass → zero-delta fixpoint), the
  per-pass ledger incl. the TOTAL row, and the job's lifecycle controls; a live
  aggregate-throughput timeline; the **Fleet** table (per-agent scan/s, files/s,
  throughput, RSS, heartbeat, drain state and drain/resume control), where any
  agent row expands to show **what it is holding right now** — each in-flight
  shard's kind, path, running (or queued) time and entries done, longest-running
  first; and an Attention rail (queue composition, parked shards, event feed).
- **Queue & shards** — shard-queue depth by job · pass · kind · state (filterable
  by job and state), work-by-kind and state-mix breakdowns, retry-pressure
  counters, and the parked-shard table (attempts N/5, errno, last agent, age,
  retry/drop). Queue and parked rows click through to the owning job's detail.
- **Errors** — the journal-backed error browser: copy errors, fidelity
  exceptions and verify failures for one job, filterable by pass, errno class
  (chips built from the response's `by_class` histogram) and rel-path prefix,
  paged.

## Operator actions

Every control maps to an existing coordinator endpoint, and the console only
offers the transitions the job's current state permits.

| Control | Endpoint |
|---------|----------|
| start / pause / resume / cancel | `POST /api/v1/jobs/{name}/{action}` |
| trigger pass, trigger delete pass | `POST /api/v1/jobs/{name}/passes` |
| drain / resume an agent | `POST /api/v1/agents/{id}/{disable,enable}` |
| retry / drop a parked shard | `POST /api/v1/parked/{id}/{retry,drop}` |
| retry / drop all parked of a job | `POST /api/v1/jobs/{name}/parked/{retry,drop}` |
| download report | client-side, from `GET /api/v1/jobs/{name}/report` |

Destructive actions open a confirm dialog. The two that lose data
irrecoverably — the **delete pass** and **drop parked shards** — additionally
require typing the job name, mirroring the API's own `confirm` gate. Failures
are surfaced verbatim from the API's `{"error": …}` body, because those
messages are written to be read by an operator.

## Data mapping

| Panel | Source |
|-------|--------|
| Jobs list, state, per-row pass rollup | `GET /api/v1/jobs` (one request for all rows) |
| Job detail, convergence, pass ledger | `GET /api/v1/jobs/{name}/report` (selected job only) |
| Live per-pass counters between polls | `GET /api/v1/events` (WebSocket, 1 Hz `stats` frames) |
| Throughput / files·s⁻¹ / scan rate, agent RSS | `GET /metrics` — `rate()` over `drsync_scan_entries_total`, `drsync_copy_files_total`, `drsync_copy_bytes_total`, plus `drsync_agent_rss_bytes`, `drsync_agent_up` |
| Lease requeues, requeue rate | `GET /metrics` — `drsync_lease_expiries_total` / `drsync_work_grants_total` |
| Queue depth & parked shards (incl. park time) | `GET /api/v1/queue` |
| Fleet roster & enable/disable state | `GET /api/v1/agents` |
| Per-agent in-flight work (expanded row only) | `GET /api/v1/agents/{id}/inflight` |
| Errors view | `GET /api/v1/jobs/{name}/errors` |

Throughput comes from `/metrics`, not from the WebSocket: the `stats` frames
carry per-pass cumulative counters, not fleet-wide rates.

In-flight detail is fetched only for the one expanded agent, and only while it
stays expanded. The endpoint is per-agent, so polling the whole roster would
cost one request per agent per tick — the same fan-out the jobs list was
restructured to avoid.

The in-flight panel keeps three situations distinct, because collapsing them
would mislead: an agent whose build predates in-flight reporting says so
(`supported: false`), an agent genuinely holding nothing reads as *idle*, and
one whose session has dropped reads as *no longer connected*. The snapshot
rides the heartbeat, so the panel states its age rather than implying it is
live.

## Notes

All values rendered from coordinator data are HTML-escaped. This is not
optional hardening: `rel_path` comes from the tree being migrated, so a file
named `<img onerror=…>` would otherwise execute script in an operator's browser
just by appearing in a parked-shard row.

Auth is a bearer token today; it moves to OIDC per DESIGN-coordinator §6.

## Design notes

Deep-slate ground with a single teal-cyan accent reserved for throughput/flow,
and a reserved semantic trio — green nominal / amber informational / red critical
— that matches the `drsync journal cat --summary` colour language so console and
CLI read as one system. Monospace for every figure, ID, and label (tabular
numerals). Theme-aware (light/dark, honouring OS preference and an explicit
toggle that persists) and reduced-motion-safe.
