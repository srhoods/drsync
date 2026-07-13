# drsync WebUI

A read-only operations console for monitoring a drsync cluster — jobs, pass
convergence, aggregate throughput, agent performance, and the shard queue /
parked-shard triage view.

This is the **phase-1 design prototype** (see `docs/DESIGN-coordinator.md` §6 —
the REST + WebSocket surface is explicitly the WebUI contract). It is a
self-contained mockup: open `console.html` in any browser, no build step and no
server required. The figures are **simulated** at 1 Hz to convey the live feel;
the console is not yet wired to a coordinator.

## Views

- **Overview** — cluster KPI strip; a jobs list with lifecycle state
  (SCANNING → DIRFIX → VERIFY → DELETE → COMPLETE); a selected-job detail with
  the **convergence curve** (Δfiles per pass → zero-delta fixpoint) and the
  per-pass ledger incl. the TOTAL row; a live aggregate-throughput timeline; the
  **Fleet** table (per-agent scan/s, files/s, throughput, RSS, heartbeat, drain
  state); and an Attention rail (queue composition, parked shards, event feed).
- **Queue & shards** — shard-queue depth by job · pass · kind · state (filterable
  by job and state), work-by-kind and state-mix breakdowns, and the parked-shard
  table (attempts N/5, errno, last agent, age). Queue and parked rows click
  through to the owning job's detail.

## Data mapping (when wired to a live coordinator)

| Panel | Source |
|-------|--------|
| KPI strip, throughput, live counters | `GET /api/v1/events` (WebSocket, 1 Hz stats frames) |
| Agent performance (scan/copy rates, RSS, up) | `GET /metrics` — `drsync_scan_entries_total`, `drsync_copy_files_total`, `drsync_copy_bytes_total`, `drsync_agent_rss_bytes`, `drsync_agent_up` (rate() over the cumulative gauges) |
| Jobs list & state | `GET /api/v1/jobs` |
| Job detail, convergence, pass ledger | `GET /api/v1/jobs/{name}` and `GET /api/v1/jobs/{name}/report` |
| Queue depth & parked shards | `GET /api/v1/queue` |
| Fleet roster & enable/disable state | `GET /api/v1/agents` |

## Roadmap

Phase 1 is read-only. Phase 2 adds operator actions already stubbed in the UI as
disabled controls — job pause/resume/cancel (`POST /api/v1/jobs/{name}/…`),
delete-pass trigger, agent enable/disable (`POST /api/v1/agents/{id}/…`), and
parked-shard retry/drop. Auth moves from bearer token to OIDC when the WebUI
lands (design doc §6).

## Design notes

Deep-slate ground with a single teal-cyan accent reserved for throughput/flow,
and a reserved semantic trio — green nominal / amber informational / red critical
— that matches the `drsync journal cat --summary` colour language so console and
CLI read as one system. Monospace for every figure, ID, and label (tabular
numerals). Theme-aware (light/dark, honouring OS preference and an explicit
toggle) and reduced-motion-safe.
