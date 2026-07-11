# drsync Detailed Design — Wire Protocol

**Status:** Detailed design v1 — 2026-07-10
**Scope:** the agent ⇄ coordinator protocol. The REST/WebSocket API is in
[DESIGN-coordinator.md](DESIGN-coordinator.md) §6.

---

## 1. Transport

- **TCP + TLS 1.3, mutual authentication.** Each agent has a cert issued by the internal
  drsync CA (`drsync ca init` / `drsync ca issue --agent <host>`); the coordinator
  verifies the client CN against its agent registry, agents pin the coordinator cert.
- **One long-lived connection per agent**, initiated by the agent (agents live behind
  whatever network policy; only `agent → coordinator:7440` connectivity is required).
  All message flows are multiplexed over this single connection.
- Reconnect with jittered exponential backoff (250 ms → 30 s cap). All protocol state
  survives reconnect: leases are keyed by agent ID, not by connection.
- **No file data ever crosses this connection** (decision D2 — agents copy mount-to-mount
  locally). Traffic is control + per-file journal records; at full fleet scan rate
  (~400k files/s) journal traffic is O(100 MB/s) into the coordinator, well within a
  dedicated host's budget.

## 2. Framing

Length-prefixed protobuf, minimal by design:

```
┌────────────┬────────────┬──────────────────────────┐
│ len u32 LE │ type u16 LE│ protobuf payload (len B) │
└────────────┴────────────┴──────────────────────────┘
```

- `len` = payload bytes only; hard cap 16 MiB (larger logical payloads must batch).
- `type` = message type enum below; unknown types are an error (protocol version is
  negotiated in HELLO, so both sides know the full type set in use).
- On the C side this is trivially parseable with no allocation surprises; payloads are
  encoded/decoded with **protobuf-c** generated from the same `proto/drsync.proto` the Go
  side uses. The `.proto` files are the single cross-language contract.

## 3. Message Types

Direction key: `A→C` agent to coordinator, `C→A` coordinator to agent.

| # | Message | Dir | Purpose |
|---|---|---|---|
| 1 | `Hello` | A→C | agent id, hostname, protocol version, agent version, capabilities (io_uring statx? copy_file_range? nfs4_acl xattr visible per mount?), resource info (cores, mem-limit, NIC) |
| 2 | `HelloAck` | C→A | accepted protocol version, agent registered, current fleet epoch, resolved runtime options |
| 3 | `Heartbeat` | A→C | liveness + implicit renewal of **all** leases held by this agent; includes coarse load (queue depths, in-flight bytes) |
| 4 | `HeartbeatAck` | C→A | piggybacks control state: pause/resume/drain flags, config-changed epoch |
| 5 | `WorkRequest` | A→C | credit-based pull: "I have capacity for N shards / M copy tasks"; sent whenever local queues drop below low-water marks |
| 6 | `WorkGrant` | C→A | 0..N work items, each a `Shard`, `EntryListShard`, `ChunkTask`, `DirFixBatch`, `VerifyBatch`, or `DeleteBatch`; each carries a lease (id, TTL) |
| 7 | `ShardSplit` | A→C | new shards discovered mid-walk (subdirectories pushed back, or entry-list batches from a huge directory); coordinator persists + queues them, acks with assigned shard ids |
| 8 | `ShardSplitAck` | C→A | ids assigned; until received, the agent must not report the parent shard complete (no lost subtrees) |
| 9 | `ShardResult` | A→C | terminal state of a leased shard: counters (entries walked, tasks emitted/completed, bytes copied), orphan count, error summary, nlink>1 stats, wall/IO timings |
| 10 | `TaskResult` | A→C | terminal state for coordinator-tracked tasks (chunk copies, dirfix batches, verify batches); batched |
| 11 | `JournalBatch` | A→C | stream of per-file journal records (see coordinator doc §5); zstd-compressed batches, ≤ 4 MiB; coordinator acks with a high-water sequence number for flow control |
| 12 | `JournalAck` | C→A | consumed sequence; agent may release its send buffer |
| 13 | `StatsReport` | A→C | 1 Hz counters: files/bytes scanned/copied/verified, IOPS, latency histograms per mount, error counts by class |
| 14 | `Control` | C→A | pause / resume / drain (finish leases, take no new work) / cancel-job / shutdown / log-level |
| 15 | `Error` | both | protocol-level fault before connection teardown |

### 3.1 Work item payloads (inside `WorkGrant`)

```protobuf
message Shard {
  uint64 shard_id   = 1;
  uint64 job_id     = 2;
  uint32 pass_no    = 3;
  string rel_path   = 4;   // relative to job src/dst roots
  uint64 lease_id   = 5;
  uint32 lease_ttl_s= 6;
  JobOptions opts   = 7;   // fully-resolved job options (sent once per job, then by ref)
}

message EntryListShard {       // slice of a huge directory (see agent doc §4.3)
  uint64 shard_id   = 1;
  string dir_rel    = 2;
  repeated string names = 3;   // the entries this shard is responsible for
  // walk/diff/copy proceeds exactly as a Shard, minus the readdir
}

message ChunkTask {            // one range of a large file
  uint64 task_id    = 1;
  string rel_path   = 2;
  uint64 offset     = 3;
  uint64 length     = 4;
  uint64 file_gen   = 5;       // src (mtime,size) snapshot; chunk aborts if src changed
}
```

`JobOptions` (filters, thresholds, ACL policy, verify rate, etc.) is versioned by a hash;
`WorkGrant` sends the full options blob only when the agent's cached hash differs.

## 4. Interaction Flows

### 4.1 Steady state (pull-based)

```
Agent                                   Coordinator
  │  Hello ────────────────────────────────▶ │  register, version-negotiate
  │ ◀──────────────────────────── HelloAck   │
  │  WorkRequest(credits) ─────────────────▶ │
  │ ◀──────────── WorkGrant(shards, leases)  │
  │  ...walks; large subdirs found...        │
  │  ShardSplit(subdirs) ──────────────────▶ │  persist, queue
  │ ◀─────────────────────── ShardSplitAck   │
  │  JournalBatch ─────────────────────────▶ │  append to pass journal
  │  StatsReport (1 Hz) ───────────────────▶ │  aggregate, expose
  │  Heartbeat (5 s) ──────────────────────▶ │  renew all leases
  │  ShardResult ──────────────────────────▶ │  mark done, update pass counters
  │  WorkRequest(credits) ─────────────────▶ │  ...
```

### 4.2 Ordering invariant (no lost work)

A shard may be acknowledged complete (`ShardResult`) **only after** every `ShardSplit`
and `JournalBatch` it produced has been acked. This gives the safety property:

> Every entry of the source tree is either fully processed under some completed shard,
> or reachable from a shard persisted in the coordinator's queue.

Agent crash at any point ⇒ lease expiry ⇒ shard re-queued ⇒ re-walked. Re-walking is
idempotent (diff-driven: already-copied files compare clean and are skipped), so
at-least-once delivery is sufficient everywhere; nothing needs exactly-once.

### 4.3 Failure cases

| Event | Handling |
|---|---|
| Agent connection drop | Coordinator keeps leases until TTL; if the agent reconnects in time (same agent id), leases continue. Otherwise expire → re-queue. |
| Coordinator restart | Agents reconnect+re-Hello; in-flight `ShardSplit`/`JournalBatch` without acks are retransmitted (dedup by (shard_id, seq)). |
| Agent version mismatch | `HelloAck` may refuse (protocol major) or accept with capability mask (minor). Rolling agent upgrades are drain → restart → rejoin. |
| Clock skew | Irrelevant: all lease timing is coordinator-local; agents only echo lease ids. |

## 5. Versioning Rules

- `proto/drsync.proto` follows standard protobuf hygiene: field numbers never reused,
  additions optional, no semantic change to existing fields.
- Protocol **major** bump only for framing/flow changes; coordinator supports current
  and previous major during migration windows.
- Capabilities (Hello) gate optional behaviors (e.g. `copy_file_range` availability per
  mount) so mixed fleets degrade per-agent, not fleet-wide.
