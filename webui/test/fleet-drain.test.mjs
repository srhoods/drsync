// Behavioural tests for the Drained-vs-Draining fleet label and the
// walker/copy slot-utilization approximation shown in the expanded agent row.
//
// Run: make webui-test   (or: npm --prefix webui/test test)

import { test, before, after } from "node:test";
import assert from "node:assert/strict";

import { boot, POLL_MS } from "./harness.mjs";
import * as F from "./fixtures.mjs";

const AGENTS = [
  { id: "agent-1", hostname: "node01", version: "0.2", state: "READY",
    connected: true, enabled: true, last_heartbeat_ms: Date.now() - 1100 },
  // Disabled but still finishing leased work — should read "draining".
  { id: "agent-draining", hostname: "node02", version: "0.2", state: "READY",
    connected: true, enabled: false, last_heartbeat_ms: Date.now() - 900 },
  // Disabled with nothing left — should read "drained".
  { id: "agent-drained", hostname: "node03", version: "0.2", state: "READY",
    connected: true, enabled: false, last_heartbeat_ms: Date.now() - 900 },
];

const INFLIGHT = {
  "agent-draining": {
    agent: "agent-draining", supported: true, reported_at_ms: Date.now() - 2000,
    inflight: [
      { lease_id: 1, shard_id: 1, job_id: 1, kind: "chunk", rel_path: "a/b.bin",
        held_ms: 5000, running_ms: 4000, running: true, entries_done: 0 },
      { lease_id: 2, shard_id: 2, job_id: 1, kind: "entrylist", rel_path: "c/d",
        held_ms: 3000, running_ms: 2000, running: true, entries_done: 10 },
    ],
  },
  "agent-drained": {
    agent: "agent-drained", supported: true, reported_at_ms: Date.now() - 2000,
    inflight: [],
  },
};

function route(path) {
  const inflight = path.match(/^\/api\/v1\/agents\/(.+)\/inflight$/);
  if (inflight) {
    const id = decodeURIComponent(inflight[1]);
    return INFLIGHT[id] ? { json: INFLIGHT[id] } : { status: 404, json: { error: "agent not connected" } };
  }
  if (path.startsWith("/metrics")) return { text: F.metricsText() };
  if (path === "/api/v1/jobs" || path.startsWith("/api/v1/jobs?")) return { json: F.JOBS };
  if (path.startsWith("/api/v1/queue")) return { json: F.QUEUE };
  if (path.startsWith("/api/v1/agents")) return { json: AGENTS };
  if (path.startsWith("/api/v1/info")) return { json: F.INFO };
  if (path.includes("/report")) return { json: F.REPORT };
  return { json: {} };
}

let c;
before(async () => {
  c = await boot({ routeOverrides: route });
  await c.tick(400);
  await c.tick(POLL_MS + 600);
});
after(() => c.dom.window.close());

const agentRow = id => c.$$("#fleet tr.agrow").find(r => r.dataset.exp === id);

test("a collapsed disabled agent shows the plain 'drained' label", () => {
  const dot = agentRow("agent-draining").querySelector(".st-dot");
  assert.equal(dot.textContent.trim(), "drained", "should not claim draining without expanding");
});

test("expanding a disabled agent still holding leases shows 'draining'", async () => {
  agentRow("agent-draining").querySelector(".ag").click();
  await c.tick(250);
  const dot = agentRow("agent-draining").querySelector(".st-dot");
  assert.equal(dot.textContent.trim(), "draining");
});

test("expanding a disabled agent with no in-flight work shows 'drained'", async () => {
  agentRow("agent-drained").querySelector(".ag").click();
  await c.tick(250);
  const dot = agentRow("agent-drained").querySelector(".st-dot");
  assert.equal(dot.textContent.trim(), "drained");
});

test("expanded in-flight panel shows an approximate walker/copy utilization split", async () => {
  agentRow("agent-draining").querySelector(".ag").click();
  await c.tick(250);
  const panel = c.$("#fleet .infl");
  assert.ok(panel, "in-flight panel did not render");
  assert.match(panel.textContent, /walker/);
  assert.match(panel.textContent, /copy/);
  assert.match(panel.textContent, /approx/i, "utilization should be labelled as approximate");
});
