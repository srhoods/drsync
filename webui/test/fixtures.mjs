// Mocked coordinator responses for the console harness.
//
// The shapes here are the REST contract in webui/README.md's data-mapping
// table. Where a field's *absence* used to be a bug — the parked-shard park
// time, the jobs-list pass rollup — it is present and non-trivial here so the
// assertions can prove it reaches the DOM.

// Rendered wherever coordinator data reaches innerHTML. rel_path is the one
// that matters: it comes from the tree being migrated, so an attacker only
// needs to create a file with this name. If the console ever stops escaping,
// this sets globalThis.__PWNED and the XSS tests fail.
export const XSS = `evil<img src=x onerror="globalThis.__PWNED=true">`;

export const JOBS = [
  { name: "alpha", state: "RUNNING", dry_run: false, pass_count: 2, pass_no: 2,
    pass_state: "COPYING", entries_walked: 1234567, files_copied: 89012,
    bytes_copied: 987654321, errors: 3, created_at_ms: Date.now() - 6e5,
    updated_at_ms: Date.now() },
  { name: XSS, state: "PAUSED", dry_run: true, pass_count: 1, pass_no: 1,
    pass_state: "SCANNING", entries_walked: 42, files_copied: 0,
    bytes_copied: 0, errors: 0, created_at_ms: Date.now(),
    updated_at_ms: Date.now() },
];

export const QUEUE = {
  depth: [
    { job: "alpha", pass_no: 2, kind: "entrylist", state: "QUEUED", count: 252904 },
    { job: "alpha", pass_no: 2, kind: "chunk", state: "LEASED", count: 2046 },
    { job: "alpha", pass_no: 2, kind: "entrylist", state: "DONE", count: 852186 },
    { job: "alpha", pass_no: 2, kind: "chunk", state: "PARKED", count: 2 },
  ],
  parked: [
    // Ages chosen to render as a round "1h" and "2m".
    { shard_id: 77, job: "alpha", pass_no: 2, kind: "chunk",
      rel_path: "deep/tree/" + XSS, attempt: 5, error: "EIO",
      last_agent: "agent-3", parked_at_ms: Date.now() - 3.6e6 },
    { shard_id: 78, job: "alpha", pass_no: 2, kind: "chunk",
      rel_path: "other/file.bin", attempt: 5, error: "ESTALE",
      last_agent: "agent-1", parked_at_ms: Date.now() - 120000 },
  ],
};

export const AGENTS = [
  { id: "agent-1", hostname: "node01", version: "0.1", state: "READY",
    connected: true, enabled: true, last_heartbeat_ms: Date.now() - 1100 },
  // Drained (connected but not scheduling), and doubles as the agent whose
  // build is too old to report in-flight detail.
  { id: XSS, hostname: "node02", version: "0.1", state: "READY",
    connected: true, enabled: false, last_heartbeat_ms: Date.now() - 900 },
];

export const INFO = {
  fleet_epoch: "55e20bae98f6dcfb", lease_ttl_s: 30, mtls: true, version: "0.1.0",
};

export const REPORT = {
  job: "alpha", state: "RUNNING", dry_run: false, converged: false,
  orphans_remaining: 17, delete_pass_ran: false, parked_shard_count: 2,
  passes: [
    { pass_no: 1, state: "COMPLETE", duration_ms: 3723000, delta_files: 900000,
      delta_bytes: 8e11, orphans: 40, verify_ok: 899000, verify_fail: 0,
      errors: 2, entries_walked: 1000000 },
    { pass_no: 2, state: "COPYING", duration_ms: 605000, delta_files: 12000,
      delta_bytes: 4e9, orphans: 17, verify_ok: 11000, verify_fail: 4,
      errors: 1, entries_walked: 1234567 },
  ],
  totals: { files_copied: 912000, bytes_copied: 8.04e11, meta_fixed: 30,
            errors: 3, fidelity_exceptions: 1, verify_ok: 910000, verify_fail: 4 },
};

export const ERRORS = {
  job: "alpha", count: 2, truncated: false,
  by_class: { EACCES: 12, VERIFY_FAIL: 4, ESTALE: 1 },
  errors: [
    { pass: 2, type: "ERROR", class: "EACCES", rel_path: "a/" + XSS,
      detail: "open denied", ts_ns: Date.now() * 1e6 },
    { pass: 2, type: "VERIFY_FAIL", class: "VERIFY_FAIL", rel_path: "b/x.bin",
      detail: "mtime mismatch", ts_ns: Date.now() * 1e6 },
  ],
};

// One reporting agent, one whose build predates in-flight reporting. Anything
// else 404s as "not connected" — the three states the panel must keep apart.
export const INFLIGHT = {
  "agent-1": {
    agent: "agent-1", supported: true, reported_at_ms: Date.now() - 4000,
    inflight: [
      { lease_id: 9, shard_id: 501, job_id: 1, kind: "chunk",
        rel_path: "big/" + XSS, held_ms: 812000, running_ms: 795000,
        running: true, entries_done: 41200 },
      // running_ms 0 while it sits in the agent's own work queue: the panel
      // must fall back to held_ms and label it queued, not show "0s running".
      { lease_id: 10, shard_id: 502, job_id: 1, kind: "entrylist",
        rel_path: "some/dir", held_ms: 4200, running_ms: 0,
        running: false, entries_done: 0 },
    ],
  },
  [XSS]: { agent: XSS, supported: false, reported_at_ms: 0, inflight: [] },
};

// Two /metrics samples so the console's rate differencing yields a non-zero
// throughput on the second poll, as it does against a live coordinator.
let sample = 0;
export function metricsText() {
  sample++;
  const scan = 1e6 * sample, files = 5e4 * sample, bytes = 2e9 * sample;
  return [
    `drsync_scan_entries_total{agent="agent-1"} ${scan}`,
    `drsync_copy_files_total{agent="agent-1"} ${files}`,
    `drsync_copy_bytes_total{agent="agent-1"} ${bytes}`,
    `drsync_agent_rss_bytes{agent="agent-1"} 734003200`,
    `drsync_agent_up{agent="agent-1"} 1`,
    `drsync_lease_expiries_total 41`,
    `drsync_work_grants_total 3980`,
    `drsync_shards_parked_total 2`,
    // A labelled histogram, to prove the console's global-counter regex is an
    // allowlist and does not read these as counter values.
    `drsync_shard_duration_seconds_bucket{kind="chunk",le="0.5"} 17`,
    `drsync_shard_duration_seconds_bucket{kind="chunk",le="+Inf"} 23`,
    `drsync_shard_duration_seconds_sum{kind="chunk"} 91.4`,
    `drsync_shard_duration_seconds_count{kind="chunk"} 23`,
  ].join("\n");
}
