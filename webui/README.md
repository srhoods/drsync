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

Served same-origin, `fetch`/WebSocket calls need no configuration — the
console always talks to the host it was loaded from. There is no
coordinator-URL override and no API-token entry anywhere in the UI: an
operator cannot point their browser at a different coordinator, and the only
credential the WebUI itself deals with is a session cookie from signing in.
(The bearer token remains available for the CLI and scripts — see
`docs/ADMIN.md` §"Authentication & TLS" — but the WebUI never surfaces it.)

If the coordinator has interactive login configured (`/etc/drsync/auth.yaml` —
local host accounts or Active Directory), the console shows a **login
screen** before anything else and a **logout** button + username chip in the
top bar once signed in. Login sets an HttpOnly, `SameSite=Lax` session
cookie (`POST /api/v1/login`) that rides along on every subsequent request
automatically. If the coordinator has no `auth.yaml` at all (open dev mode),
the console skips the login screen and connects directly, unchanged.

The connection indicator (top bar) is green when polling and the event socket
are healthy, amber while connecting, red when the coordinator is unreachable.

Data refreshes every 2.5 s. Live state changes (job/pass transitions, agent
up/down, parked-shard alerts) arrive over the events WebSocket and trigger an
immediate refresh, and the 1 Hz `stats` frames are folded into the selected
job's detail so its counters advance between polls. Per-agent rates are derived
by differencing successive `/metrics` samples, so throughput/scan/copy figures
populate after the second poll.

## Views

- **Overview** — cluster KPI strip; a jobs list, filterable by **All / Ready /
  Running / Finished** (Running includes Paused; Finished covers both
  Completed and Cancelled), with lifecycle state (SCANNING → DIRFIX → VERIFY
  → DELETE → COMPLETE); a **+ new job** button that opens a dialog pre-filled
  with the shipped job template, editable, with Submit / Submit and start job
  / Cancel; a selected-job detail with the **convergence curve** (Δfiles per
  pass → zero-delta fixpoint), the per-pass ledger incl. the TOTAL row, and
  the job's lifecycle controls (Ready can now cancel, not just start — see
  Operator actions) — plus **settings** (view the YAML a job was submitted
  with, read-only, any state), **resubmit** (Completed / Cancelled / Failed
  jobs only — reopens the same dialog pre-filled with that job's settings and
  an incremented job name), **purge** (Completed / Cancelled / Failed only —
  permanently removes the job and its journal) and, for a Completed job whose
  last scan still has orphans on disk, a **clean up N orphans** action (the
  existing delete-pass endpoint, confirm-gated the same way); a live
  aggregate-throughput timeline; the **Fleet** table (per-agent scan/s,
  files/s, throughput, RSS, heartbeat, drain state and drain/resume control) —
  a disabled agent reads **Draining** while it still holds leased work and
  **Drained** once that in-flight list is empty (only resolvable for the
  expanded row; see Data mapping) — where any agent row expands to show
  **what it is holding right now**: each in-flight shard's kind, path, running
  (or queued) time and entries done, longest-running first, plus an
  approximate walker/copy activity split (see Data mapping); and an Attention
  rail (queue composition, parked shards, event feed).
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
| submit a new job | `POST /api/v1/jobs` (raw YAML body) |
| submit and start | `POST /api/v1/jobs` then `POST /api/v1/jobs/{name}/start` |
| resubmit (prefilled from a finished job) | `GET /api/v1/jobs/{name}/spec` to prefill, then submit as above |
| view job settings | `GET /api/v1/jobs/{name}/spec` (read-only) |
| start / pause / resume / cancel | `POST /api/v1/jobs/{name}/{action}` |
| trigger pass, trigger delete pass, clean up orphans | `POST /api/v1/jobs/{name}/passes` |
| purge (Completed / Cancelled / Failed only) | `DELETE /api/v1/jobs/{name}` |
| drain / resume an agent | `POST /api/v1/agents/{id}/{disable,enable}` |
| retry / drop a parked shard | `POST /api/v1/parked/{id}/{retry,drop}` |
| retry / drop all parked of a job | `POST /api/v1/jobs/{name}/parked/{retry,drop}` |
| download report | client-side, from `GET /api/v1/jobs/{name}/report` |

"Clean up orphans" is not a distinct endpoint: it is the existing trigger-delete-pass
call (`{"delete": true, "confirm": "<job name>"}`), newly surfaced on a
Completed job whenever its report's `orphans_remaining` is non-zero — the
underlying action was already there for running jobs, this only exposes it
for the terminal case operators actually hit it in.

**Cancel is offered on a Ready job, not just Running/Paused.** The cancel
endpoint has no state gate on the coordinator side — it always just sets the
job to CANCELLED — so cancelling a job that never started is already a valid
call, this only makes the button reachable for that state too. It's also the
only way to make a Ready job purgeable: **purge** targets `DELETE
/api/v1/jobs/{name}`, which the coordinator only permits once a job is
terminal (Completed/Cancelled/Failed — `store.TerminalJobState`), so a Ready
job must be cancelled first.

Destructive actions open a confirm dialog. The ones that lose data
irrecoverably — **delete pass**, **drop parked shards**, and **purge** —
additionally require typing the job name, mirroring the API's own `confirm`
gate (purge has no server-side confirm field, but the console asks anyway
since deleting a job's rows and journal is unrecoverable). Failures are
surfaced verbatim from the API's `{"error": …}` body, because those messages
are written to be read by an operator.

## Data mapping

| Panel | Source |
|-------|--------|
| Jobs list, state, per-row pass rollup | `GET /api/v1/jobs` (one request for all rows) |
| Job detail, convergence, pass ledger, `orphans_remaining` | `GET /api/v1/jobs/{name}/report` (selected job only) |
| Job settings dialog (view / resubmit prefill) | `GET /api/v1/jobs/{name}/spec` — the stored raw YAML, verbatim |
| Live per-pass counters between polls | `GET /api/v1/events` (WebSocket, 1 Hz `stats` frames) |
| Throughput / files·s⁻¹ / scan rate, agent RSS | `GET /metrics` — `rate()` over `drsync_scan_entries_total`, `drsync_copy_files_total`, `drsync_copy_bytes_total`, plus `drsync_agent_rss_bytes`, `drsync_agent_up` |
| Lease requeues, requeue rate | `GET /metrics` — `drsync_lease_expiries_total` / `drsync_work_grants_total` |
| Queue depth & parked shards (incl. park time) | `GET /api/v1/queue` |
| Fleet roster & enable/disable state | `GET /api/v1/agents` |
| Per-agent in-flight work (expanded row only) | `GET /api/v1/agents/{id}/inflight` |
| Draining vs Drained (expanded row only) | derived client-side from the same `/inflight` snapshot — see below |
| Walker/copy slot activity (expanded row only, approximate) | derived client-side from `/inflight` shard kinds — see below |
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

**Draining vs Drained.** The coordinator only persists whether an agent is
enabled or disabled — there is no separate "still finishing its last leases"
state anywhere in the API. The console derives it: a disabled agent whose
expanded in-flight list is non-empty reads **Draining**; once that list is
empty it reads **Drained**. This is only shown for the currently expanded
row, not the whole roster, for the same fan-out reason in-flight detail
itself is expand-only — computing it for every disabled agent on every poll
would mean one extra request per disabled agent per tick. A collapsed
disabled row keeps the plain "drained" label it always had.

**Walker/copy slot activity** in the expanded panel is an approximation, not
a real utilization reading, and is labelled as such in the UI. It counts
running in-flight items by shard kind (`chunk` as copy work, everything else
as walker work) as a proxy for which of an agent's two thread pools is busy.
This is necessarily approximate: the agent's copy and walker pools can steal
work from each other, so a shard's kind doesn't map 1:1 onto which pool
executed it, and there is no true denominator to compute a percentage
against — the agent's configured pool sizes (`-w`/`-C`) and its own
queue-depth counters (`shard_queue_depth`/`copy_queue_depth`, carried on the
heartbeat wire) are not currently surfaced by the coordinator. A follow-up
to wire those through (plus reporting configured pool size) would let this
become a real busy/total reading instead of a proxy.

## Tests

`make webui-test` runs the console in jsdom against mocked coordinator
responses and asserts on the rendered DOM — see `test/README.md`. `make test`
stays Go-only; `make test-all` runs both.

## Notes

All values rendered from coordinator data are HTML-escaped. This is not
optional hardening: `rel_path` comes from the tree being migrated, so a file
named `<img onerror=…>` would otherwise execute script in an operator's browser
just by appearing in a parked-shard row.

The WebUI's only auth surface is session-cookie login (local host accounts or
Active Directory) — see `docs/ADMIN.md` §"Authentication & TLS". The bearer
token is a separate, CLI/script-facing credential the WebUI never asks for or
stores. OIDC remains a possible future addition per DESIGN-coordinator §6.

The coordinator's HTTP(S) listener serves plain `http://` unless
`/etc/drsync/certs.yaml` names a cert/key pair, in which case it serves
`https://` and the session cookie is marked `Secure`.

## Design notes

Deep-slate ground with a single teal-cyan accent reserved for throughput/flow,
and a reserved semantic trio — green nominal / amber informational / red critical
— that matches the `drsync journal cat --summary` colour language so console and
CLI read as one system. Monospace for every figure, ID, and label (tabular
numerals). Theme-aware (light/dark, honouring OS preference and an explicit
toggle that persists) and reduced-motion-safe.

The favicon is the wordmark's own "dr" mark (ink `d`, accent `r`, a faint
accent slash) as an inline SVG data URI — no separate asset file, keeping the
single-self-contained-page property. It carries its own `prefers-color-scheme`
media query so the browser tab matches the page's theme independent of the
in-page toggle (a favicon can't read the page's `data-theme` attribute or its
CSS custom properties; only the OS-level media query is available to it).
