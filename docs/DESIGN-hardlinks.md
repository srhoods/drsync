# drsync Detailed Design — Hardlink Preservation

**Status:** Implemented (decision D11, `ARCHITECTURE.md` §10), ratified 2026-07-31.
Supersedes D3's blanket "not preserved." `metadata.hardlinks: preserve` is now the
**default** (as of 2026-07-31); set `hardlinks: report` to opt out to D3's original
behavior.
**Scope:** dual-tree walker (agent), coordinator state + scheduler, wire protocol, job
spec — all four repo areas, landed as five incremental PRs.

> **Implementation status (2026-07-31):** shipped end-to-end. `struct estat` carries
> `dev` alongside `ino` (closing a pre-existing reporting gap where `NLINK_DUP`
> aggregation only had `ino`); the wire protocol gained `LinkSighting`
> (`ShardSplit`), `LinkTask` (`WorkItem`), and three `ShardCounters` fields
> (`links_created`/`link_anchor_races`/`link_fallback`), proto minor 2. The
> coordinator has a pass-scoped `link_groups`/`link_members` SQLite schema
> (`store.go`, cleaned up at job purge like `chunk_groups` — not reaped per-pass as
> originally sketched below in §3.5, since the actual `chunk_groups` precedent
> doesn't reap per-pass either) and a new LINKFIX phase between DIRFIX and VERIFY
> (`model.PassLinkfix`/`KindLinkfix`, `passctrl.seedLinkfix`). `onShardSplit` gates
> on the job's spec (`scheduler.HardlinksPolicy`, cached alongside `SpreadPolicy`) so
> a `report`-mode job never resolves a spec or touches `link_groups` at all. The
> agent's walker emits `LinkSighting`s unconditionally alongside the existing
> `NLINK_DUP` journal record (`walker.c` `queue_linksighting`/`flush_linksightings`);
> `agent/src/link.c`'s `do_linkfix` executes `linkat` under the same fd-anchored
> containment as the rest of the agent, with anchor-gen drift detection (mirroring
> `ChunkTask.gen`) and EEXIST handling for both idempotent re-runs and stale-entry
> replacement. Verified end-to-end against a real multi-agent fleet
> (`test/hardlink_e2e.sh`, `test/hardlink_resilience_e2e.sh` — an agent killed
> mid-LINKFIX, and an oversized group exceeding `hardlinks_max_group_scan` falling
> back to independent copies) — those two scripts caught two real bugs unit tests
> had missed: a missing `KindLinkfix` case in the scheduler's shard-to-`WorkItem`
> builder (every `LinkTask` parked as "unknown shard kind" until fixed), and a
> `links_created`/`link_fallback` counting bug where a fully-converged second pass
> re-counted already-linked members. Both fixed; `TestRecordLinkSightingsMaxGroupScanFallback`
> now pins the counter behavior at the unit level too.

---

## 0. Should we do this at all?

Before reading further: confirm the business case first. Every hardlinked file is
already counted today — `nlink_dup_files` / `nlink_dup_bytes` per pass (`walker.c:711`,
D3's mitigation) and the per-file `NLINK_DUP` journal record. If the duplicated-bytes
cost the report already shows is small relative to the migration, this doc is not worth
building. If it's large (e.g. dedup-heavy backup trees, compiler-output farms, VM image
directories with snapshot chains), read on.

---

## 1. Why D3 made this hard

The whole architecture optimizes for **shard independence**:

- Directories are dynamically, recursively split and pulled by whichever agent has
  spare capacity (`ARCHITECTURE.md` §3.2) — there is no stable mapping of paths to
  agents, and no way to predict it in advance.
- Two hardlinked files living in different directories can be walked by different
  agents, in different shards, minutes apart, possibly in different passes.
- Agents are "stateless-ish" by design (`ARCHITECTURE.md` §2, §5): crash recovery
  relies on shards being independently retryable.
- Coordinator state is deliberately sized by *shards* (10⁵–10⁶ rows), not files (10⁹)
  — called out in `DESIGN-coordinator.md` as the reason SQLite-WAL suffices over
  RocksDB. Any global per-inode table works directly against that invariant.

Preserving hardlinks requires correlating files across shard boundaries by
**(dev, ino)** — which is precisely the global state D3 avoided. This doc proposes the
smallest structure that does it without abandoning the shard-independence model
everywhere else.

## 2. Closest existing precedent: cross-fleet chunk fan-out

`DESIGN-coordinator.md` §4.3 already solves a structurally similar problem: a large
file discovered by one agent needs work fanned out to others, tracked to a single
completion point, with idempotent retry. Its shape:

```
agent discovers big file → ShardSplit.big_files (rel_path, size, mtime)
  → coordinator creates chunk_groups row + N ChunkTask shards (one transaction)
  → each chunk's OK bumps n_done
  → n_done == n_chunks seeds the finalize task (same transaction, atomic)
  → finalize fsyncs, applies metadata, renames temp into place
```

Hardlink correlation is the same *shape* — discover once, fan out from coordinator
state, converge to a completion point — but two things are structurally harder than
chunking:

| | Chunk fan-out | Hardlink fan-in |
|---|---|---|
| Who seeds the group | One agent (the one that walked the file) | **Every** agent that walks **any** member of the link set — unbounded, unordered discovery |
| Fan-in cardinality | Known upfront (`size / chunk_size`) | Unknown until the whole tree has been walked (`nlink` on the first-seen file bounds it, but later links may be walked before earlier ones) |
| Completion signal | Count reaches a known N | "No more shards remain that could discover a new member of this group" — i.e. **end of SCANNING**, not a per-group counter |
| Ordering dependency | None between chunks | The `linkat` target must exist before any link task runs — first-seen copy must complete before dependents are grantable |

That last row is the one piece with no clean precedent elsewhere in the codebase: it's
an explicit "wait for X, then do Y" dependency, where today every task type is designed
to be independently gradable the moment it's discovered.

## 3. Design

### 3.1 Detection during the walk (agent)

`struct estat` (`agent/src/agent.h:171`) currently tracks `ino` but not `st_dev` — a gap
already present in today's reporting-only accounting (inode numbers are only unique
within one filesystem, and src/dst roots can each span several). Add `dev` alongside
`ino`, populated from the same `statx`/`fstatat` call already made per entry (no extra
syscall).

When the walker sees `nlink > 1` on a regular file (`walker.c:711`), instead of only
journaling `NLINK_DUP` and copying independently, it reports the sighting to the
coordinator:

```
ShardSplit.link_sightings: repeated LinkSighting {
  uint64 dev, ino;
  bytes  rel_path;
  uint32 nlink;       // from stat — the group's expected total member count
  uint64 size;
  int64  mtime_ns;
}
```

Piggybacked on the existing `ShardSplit` message (agent already sends this per shard;
no new round trip). The walker still copies the file *speculatively* on first local
sighting within its own shard — see §3.4 for why waiting is worse — and always still
emits `NLINK_DUP` for the report regardless of what the registry does with it.

### 3.2 Coordinator: the link registry

New table, keyed by the group identity, not by path:

```sql
link_groups (
  pass_id, dev, ino,
  nlink_expected,        -- from the first stat seen; sanity-checked, not trusted absolutely
  members_seen,          -- count of distinct rel_paths reported
  anchor_rel_path,       -- first member copied; NULL until one lands
  anchor_state,          -- PENDING | COPIED | FAILED
  updated_at,
  PRIMARY KEY (pass_id, dev, ino)
)
link_members (pass_id, dev, ino, rel_path, state)  -- state: PENDING | LINKED | ANCHOR
  INDEX (pass_id, dev, ino)
```

Flow per `LinkSighting`:

1. `INSERT OR IGNORE` the group row on first sighting of a `(dev,ino)` pair; insert the
   member row (`PENDING`).
2. If no anchor exists yet for the group: this sighting's agent has *already* copied the
   file speculatively (§3.4) — mark it `ANCHOR`, set `anchor_rel_path`/`anchor_state =
   COPIED` once that shard's result confirms the copy landed.
3. If an anchor exists: this member does **not** need a data copy. Seed a `LinkTask`
   (new task kind) once the anchor is confirmed `COPIED` — `linkat(anchor_rel_path,
   this_rel_path)` on the destination, then apply this member's own metadata (owner/mode/
   times can legitimately differ per hardlink path component only in dir metadata, not
   file content, so this is cheap: chown/chmod/xattr are shared inode-wide in POSIX,
   already correct from the anchor copy — only the *directory entry* needs creating).
4. If the anchor is still pending (its shard hasn't reported result yet), the member row
   stays `PENDING`; a lightweight requeue check picks it up on the next scheduler tick
   once the anchor flips to `COPIED`. This is the same pattern the scheduler already
   uses for fair-share/lease-driven requeueing — no new polling loop, just an additional
   condition on shard-grant eligibility.

### 3.3 New task type + phase: LINKFIX

Rather than trying to grant `LinkTask`s interleaved with SCANNING (where the anchor may
not exist yet for a late-discovered member), hold them until a new phase, **LINKFIX**,
inserted between DIRFIX and VERIFY:

```
PROBING → SCANNING → DIRFIX → LINKFIX → VERIFY → [DELETE] → COMPLETE
```

This reuses the exact safety argument `passctrl.advance()` already relies on for chunk
temp-reclaim (`passctrl.go:384-395`): the SCANNING→DIRFIX transition only happens once
`queued+leased == 0` for all SCANNING-kind shards, which proves *no shard can discover a
new link_groups member after this point*. That is exactly the fan-in completion signal
missing from §2's comparison table — it doesn't need a per-group counter at all, it
piggybacks on a proof the scheduler already establishes for a different reason.

At the SCANNING→DIRFIX transition (extend `seedTempReclaim`/`seedDirfix` sibling-style):

- `seedLinkfix(pass)`: for every `link_groups` row with `anchor_state = COPIED` and at
  least one `PENDING` member, seed `LinkTask`s for those members, transitioning them to
  a grantable state.
- Any group whose anchor never confirmed `COPIED` (source drifted, copy errored) falls
  back to D3 behavior for its members: they were already speculatively copied
  independently by their own shards (§3.4), so nothing is lost — just no dedup for that
  group this pass. Journaled as `LINK_FALLBACK {dev, ino, reason}`.

`LinkTask` (as originally landed; superseded by `LinkTaskBatch` below — kept in the
proto, unencoded, only so an old journal/capture still decodes):

```protobuf
message LinkTask {
  uint64 task_id      = 1;
  uint64 job_id       = 2;
  uint32 pass_no      = 3;
  string anchor_rel   = 4;   // destination path already populated
  string member_rel   = 5;   // destination path to linkat into existence
  FileGen anchor_gen   = 6;  // size/mtime the anchor had when copied; re-check before linking
}
```

`LinkTaskBatch` (as landed by the §3.3 follow-up below — this is what `seedLinkfix`
actually seeds today):

```protobuf
message LinkEntry {
  string anchor_rel  = 1;
  string member_rel  = 2;
  FileGen anchor_gen = 3;
}

message LinkTaskBatch {
  uint64 task_id           = 1;
  uint64 job_id            = 2;
  uint32 pass_no           = 3;
  repeated LinkEntry links = 4;
}
```

Executor (`agent/src/link.c`, new): `linkat(dst_fd, anchor_rel, dst_fd, member_rel,
0)` (both fd-relative under the existing `openat2 RESOLVE_BENEATH` containment, no new
traversal surface). `EEXIST` on the destination (stale file from a prior pass, or
independent-copy leftover from a fallback) is resolved by `unlinkat` + retry — same
pattern as `remove_dst` used for type mismatches (`walker.c:298`). Journals
`LINK_CREATED {dev, ino, anchor_rel, member_rel}`.

**Follow-up: batched at scale, `LinkTaskBatch` (proto minor 3).** As landed above,
`seedLinkfix` inserted one `LinkTask` shard per pending member — fine at the group
sizes/densities this doc's §3.6 scale analysis modeled, but a real 1B-file/500TB job
with a hardlink-heavy tree seeded ~2.5M individual shards in one LINKFIX phase, from a
single `InsertShards` call. That call holds the coordinator's one process-wide write
lock (`store.Store.mu`) for the whole insert, stalling every agent's dispatch —
including heartbeat lease renewal — for however long the insert took; and the agent side
independently capped how many held-lease ids it can report per heartbeat
(`agent/src/main.c`, `AGENT_MAX_LEASES` — sized to the lease table's own 8192-slot
capacity, previously a much smaller ad hoc constant that a link-heavy agent could
exceed). Leases that fall off either edge are never renewed, expire under still-running
work, get requeued and re-granted elsewhere, and the original agent's eventual result
arrives to find its lease no longer matches — logged as `"dropping stale shard result"`
— so the phase's queued/leased counts can look like LINKFIX is repeatedly failing to
drain even though real `linkat` work keeps completing underneath it.

Fixed the same way DIRFIX/VERIFY already bound their own phases: `seedLinkfix` now packs
up to `linkfixBatchSize` (2000) pending members into one `LinkTaskBatch` shard —
`LinkEntry{anchor_rel, member_rel, anchor_gen}` repeated, task-level `job_id`/`pass_no`
carried once per batch instead of once per member — flushing a batch (one `InsertShards`
call) at a time rather than building the whole pass's shards in memory and inserting
them in one call. `agent/src/link.c`'s `process_linkfix` now loops `do_linkfix` over a
batch's entries; one entry's failure (a drifted anchor, a real error) does not abort the
rest of the batch — the same soft-fail-and-continue discipline DIRFIX's own per-item
loop already uses — so a single bad member does not force the other ~1999 already-
succeeded links in its batch to be redone. `do_linkfix` itself (the per-entry `linkat` +
gen-check + `EEXIST`-retry core) is unchanged.

No mixed-fleet fallback: unlike DIRFIX/VERIFY batching, `LinkTask` (unbatched) is not
kept as a shape the coordinator still seeds for older agents — `seedLinkfix` always
seeds `LinkTaskBatch` now. An operator must raise `-min-agent-minor` to 3
(`Config.MinAgentMinor`, `coordinator/internal/agentsrv`) before resuming a job whose
LINKFIX phase has pending work, so no agent below minor 3 (unable to decode
`LinkTaskBatch`) is ever granted one. `LinkTask` itself stays defined in the proto,
byte-for-byte, as a historical/reference shape only — decoding an old capture or journal
still works; nothing encodes it anymore.

### 3.4 Why the anchor is copied *speculatively*, not gated

An alternative design holds **every** member of a link group back until the group is
"complete" (all `nlink_expected` members sighted), then designates an anchor and copies
just one. Rejected:

- `nlink_expected` is a lower bound only for the *pass's view* of the tree — filters
  (`spec.filters`) can exclude some hardlinked paths, so "all members sighted" is not
  reliably detectable during SCANNING; waiting for it would require holding the phase
  open past the point every other shard kind has already proven safe to drain.
- It would turn every hardlinked file into blocked work until the slowest-to-be-
  discovered sibling shard reports in — directly reintroducing the kind of
  scan-then-copy global barrier the dual-tree walk was built to avoid
  (`ARCHITECTURE.md` §3.3).
- Speculative-copy-first means pass 1 throughput for non-hardlinked trees (the common
  case) is completely unaffected — the registry only does extra work when `nlink > 1`
  sightings actually occur.

Cost of the speculative approach: the first-discovered member of a group is copied in
full even though it will become the anchor either way, so there is no wasted *data*
copy — but if two members of the same group are walked concurrently by two different
agents before either result reaches the coordinator, both copy speculatively and the
loser's copy is redundant (bytes moved, not wrong data). This is bounded: it costs at
most one redundant copy per group per pass, not per member, since the second sighting of
an already-anchored group blocks new speculative copies at ingestion (step 2 in §3.2 —
once *any* anchor exists, no further member starts a data copy). The report should
surface this as a distinct counter (`link_anchor_races`) so it stays visible, in keeping
with the "every non-preservable/approximated attribute is counted, never silently
absorbed" principle already applied to fidelity exceptions.

### 3.5 Cross-pass durability

`link_groups`/`link_members` are pass-scoped by construction (`PRIMARY KEY (pass_id,
dev, ino, ...)`), so **no persistent cross-pass state is needed** — this is the main
reason to prefer the LINKFIX-phase design over anything that tries to maintain a
durable global inode map across the job's lifetime, which would cost much more state
for no benefit.

That does *not* mean pass 2+ skips the registry, though: the walker still sees
`nlink > 1` on every already-linked file (that is what the source's hardlinks look
like — nlink doesn't drop just because the destination converged) and still emits a
fresh sighting every pass, so pass 2+ builds its own `link_groups` rows from scratch
and re-discovers the same anchor/member structure. The work is *not* wasted, though
it does look like it should be at first glance: `do_linkfix`'s `linkat` hits `EEXIST`
against the correct, already-linked destination entry, detects same-inode via `fstat`,
and reports success without re-linking or re-journaling it as a new
`links_created` — verified by `test/hardlink_e2e.sh`'s convergence-pass assertion,
which is exactly the check that caught an early version of this counting the
already-linked case as a fresh link every pass. So: no *durable* state (each pass's
registry is disposable scratch, cleaned up at job purge alongside `chunk_groups` — not
per-pass reaped, matching how `chunk_groups` itself is actually cleaned up, contrary to
this doc's original draft below), but real, repeated per-pass work proportional to
hardlink density, not proportional to job length.

Reaped in practice: `link_groups`/`link_members` rows accumulate for the job's whole
life (one set per pass) and are deleted only at job purge (`PurgeJob`), the same
lifecycle `chunk_groups` already has — not swept per-pass by anything like the Shard
Reaper. A long-running job with many passes and a hardlink-heavy tree will accumulate
one link-registry snapshot per pass until purged; this was a simplification made
during implementation once the real `chunk_groups` precedent turned out not to
per-pass-reap either, rather than building new reaping machinery this doc originally
assumed existed for the precedent it was modeled on.

The one edge case: a hardlink group where members span a **shard-split boundary created
mid-pass by a directory reorganized on the source between pass N's SCANNING and pass
N+1** — e.g. a member added between passes. This is not a new problem: it is exactly
the existing incremental-pass model already handles (new file → new CREATE task,
folded into the next pass's own link_groups scratch state). No special case needed.

### 3.6 Scale

If hardlinked files are ~1% of a 1B-file tree (10M files), and link groups average
small (2–5 members, the common case for reflink/backup dedup, as opposed to pathological
million-member groups), that's on the order of 3–5M `link_groups`/`link_members` rows
**per pass**, accumulating across the job's passes until purge (§3.5 — this is the
`chunk_groups` lifecycle, not a per-pass reap). This is meaningfully larger than the
shard table's normal 10⁵–10⁶ working set (`ARCHITECTURE.md` §6), so:

- `link_groups`/`link_members` are a **separate SQLite table** (`store.go`, `WITHOUT
  ROWID`, keyed `(pass_id, dev, ino[, rel_path])`), not shoehorned into the `shards`
  table — implemented as designed.
- A job spec cap, `hardlinks_max_group_scan` (default 0 = unlimited, documented in
  `template.yaml`), lets an operator bound worst-case registry size for
  known-pathological trees (e.g. a single 10M-member group, which does happen with
  certain backup tools) by falling back to D3 behavior (independent copies) for any
  group exceeding the cap — reported via the `link_fallback` counter (§3.3), counted
  once per pass per group that exceeds the cap (a long job re-encountering the same
  oversized group every pass counts it every pass — that repetition is real work, not
  a display bug: see §3.5).
- Not yet built: nothing prunes old passes' `link_groups`/`link_members` rows short of
  a full job purge. A hardlink-dense job left running for hundreds of passes would grow
  this table without bound in a way `chunk_groups` already tolerates (chunk fan-out
  only happens for genuinely huge files, so its row count stays small in practice) but
  a hardlink-dense tree would not. If this becomes a real operational problem, the fix
  is a `link_groups`/`link_members` reap keyed on pass completion, mirroring the Shard
  Reaper's design (`DESIGN-coordinator.md` §3) rather than `chunk_groups`'s job-purge-only
  lifecycle — tracked as a follow-up, not blocking the opt-in feature today.
- This is the concrete number to revisit before committing to the feature: measure
  actual hardlink density and group-size distribution on the real source tree first
  (§0) — the design is sound at 1%, but a source tree that is *mostly* hardlinks (some
  VM-snapshot or CI-artifact estates are) would want a different strategy entirely
  (possibly the GPFS/Weka snapshot-diff listers doing this natively, since both
  underlying filesystems already track link counts in their metadata engines).

## 4. Job spec & report surface

```yaml
metadata_fidelity:
  hardlinks: preserve              # preserve (default, as of 2026-07-31) | report
  hardlinks_max_group_scan: 0       # 0 = unlimited; caps worst-case registry size (§3.6)
```

`hardlinks: preserve` is the default — this design's link registry runs for every job
unless explicitly turned off. `hardlinks: report` opts back out to exactly D3's
original behavior (independent copies, counted, no `link_groups` state built at all).

Report additions (alongside existing `nlink_dup_files`/`nlink_dup_bytes`):

| Field | Meaning |
|---|---|
| `link_groups_total` | distinct (dev,ino) groups seen this pass |
| `links_created` | member links created via `linkat` (space actually saved) |
| `link_anchor_races` | redundant speculative copies (§3.4) |
| `link_fallback` | groups that fell back to independent-copy (anchor failure or `max_group_scan` cap) |

## 5. What this does *not* solve

- **Cross-job / cross-filesystem hardlinks are still impossible by definition** — a
  hardlink cannot span filesystems on POSIX; this design only ever links within one
  destination mount, matching source reality.
- **Directories are never hardlinked on POSIX** — out of scope, not a gap.
- **GPFS/Weka accelerator listers** (`gpfs-policy`, `weka-snap`, D6) would need their own
  sightings path into §3.1 when built; this doc only wires the `posix` lister.
- Does not attempt hardlink preservation *across* a delete pass boundary in a way that
  differs from normal orphan handling — an orphaned link-group member is just an orphan,
  handled by the existing D5 delete-pass path unchanged.

## 6. Rollout (as landed)

Landed as five incremental PRs, each independently buildable/testable, feature inert
(`hardlinks: report`, D3's original default at the time) until the last one. The
default itself later flipped to `preserve` (2026-07-31, after the series above
merged) — see §4.

1. `dev` added to `struct estat`, wired through the `NLINK_DUP` journal record
   (`StatInfo.dev`, field 9 — already reserved in the proto, never previously
   populated). No behavior change; closes the pre-existing `(dev,ino)` reporting gap.
2. Wire protocol: `LinkSighting` (`ShardSplit`), `LinkTask` (`WorkItem`), the three
   `ShardCounters` fields, proto minor 2. Nothing emits or consumes these yet.
3. Coordinator: `link_groups`/`link_members` schema, `seedLinkfix`, the LINKFIX phase
   inserted into `passctrl.advance()`'s state machine, job spec fields
   (`metadata.hardlinks`, `metadata.hardlinks_max_group_scan`) added to `spec.go`,
   `template.yaml`, and the WebUI's embedded job template together (a defaults change
   touches all three, per this repo's own convention).
4. Agent: walker emits `LinkSighting`s unconditionally alongside `NLINK_DUP`;
   `agent/src/link.c`'s `do_linkfix` executes the `linkat` + anchor-gen-check +
   EEXIST-retry logic; coordinator-side `onShardSplit` gates on the job's spec
   (`scheduler.HardlinksPolicy`) before ever touching `link_groups`.
5. Fault injection + e2e: `test/hardlink_e2e.sh` (multi-agent fleet, real dedup
   verified via shared destination inode) and `test/hardlink_resilience_e2e.sh`
   (agent killed mid-LINKFIX; an oversized group exceeding
   `hardlinks_max_group_scan` falls back). These caught two real bugs no unit test
   had exercised — see the implementation-status note at the top of this doc — both
   fixed before landing, with unit tests added afterward to pin the fixes.
6. `LinkTaskBatch` (proto minor 3, §3.3 follow-up): batched LINKFIX seeding/execution
   after a real 1B-file production job exposed the unbatched design's two scale
   ceilings (coordinator lock hold time, agent heartbeat-reportable lease count) —
   see §3.3 for the mechanism. `agent/src/agent.h`'s self-reported `PROTO_MINOR` was
   also corrected to 3 here; it had never been bumped past 1 despite minor 2 (item 2
   above) already landing, an unrelated pre-existing gap this follow-up's own
   `MinAgentMinor` gating depends on being accurate.

**D11 — Hardlinks preserved via a pass-scoped link registry, opt-in, superseding D3's
blanket "not preserved" for jobs that request it.** D3's rationale (no *durable* global
inode map) stays true in spirit — the registry is disposable scratch state — though
§3.5/§3.6 above correct this doc's original assumption that it would be reaped
per-pass: in practice it follows `chunk_groups`'s actual lifecycle (cleaned up at job
purge), which is a real, tracked follow-up rather than a blocking gap.
