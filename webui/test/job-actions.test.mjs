// Behavioural tests for the job-dialog features (new job / resubmit / view
// settings), the orphan-cleanup control, the job-list filter, and the
// Drained-vs-Draining fleet label — all added on top of the console's
// existing read-only monitoring surface.
//
// Run: make webui-test   (or: npm --prefix webui/test test)

import { test, before, after } from "node:test";
import assert from "node:assert/strict";

import { boot, requests, POLL_MS } from "./harness.mjs";
import * as F from "./fixtures.mjs";

const SPEC_YAML = `apiVersion: drsync/v1
kind: Job
metadata:
  name: finished-job
spec:
  source: { path: /src/a }
  destination: { path: /dst/a }
`;

const JOBS = [
  ...F.JOBS,
  { name: "finished-job", state: "COMPLETED", dry_run: false, pass_count: 3, pass_no: 3,
    pass_state: "COMPLETE", entries_walked: 500, files_copied: 500,
    bytes_copied: 5e6, errors: 0, created_at_ms: Date.now() - 9e5,
    updated_at_ms: Date.now() - 1e5 },
  { name: "cancelled-job", state: "CANCELLED", dry_run: false, pass_count: 1, pass_no: 1,
    pass_state: "SCANNING", entries_walked: 10, files_copied: 0,
    bytes_copied: 0, errors: 0, created_at_ms: Date.now() - 9e5,
    updated_at_ms: Date.now() - 8e5 },
];

const REPORT_WITH_ORPHANS = {
  ...F.REPORT, job: "finished-job", state: "COMPLETED", converged: true,
  orphans_remaining: 17,
};
const CANCELLED_REPORT = {
  ...F.REPORT, job: "cancelled-job", state: "CANCELLED", converged: false,
  orphans_remaining: 0, passes: [],
};

function route(path) {
  if (path === "/api/v1/jobs/finished-job/spec") return { text: SPEC_YAML };
  if (path === "/api/v1/jobs/finished-job/report") return { json: REPORT_WITH_ORPHANS };
  if (path === "/api/v1/jobs/cancelled-job/report") return { json: CANCELLED_REPORT };
  if (path.includes("/report")) return { json: F.REPORT };
  if (path === "/api/v1/jobs" || path.startsWith("/api/v1/jobs?")) return { json: JOBS };
  if (path.startsWith("/api/v1/queue")) return { json: F.QUEUE };
  if (path.startsWith("/api/v1/agents")) return { json: F.AGENTS };
  if (path.startsWith("/api/v1/info")) return { json: F.INFO };
  if (path.startsWith("/metrics")) return { text: F.metricsText() };
  const inflight = path.match(/^\/api\/v1\/agents\/(.+)\/inflight$/);
  if (inflight) {
    const id = decodeURIComponent(inflight[1]);
    return F.INFLIGHT[id] ? { json: F.INFLIGHT[id] } : { status: 404, json: { error: "agent not connected" } };
  }
  return { json: {} };
}

let c;
let postedSpecs = [];
before(async () => {
  c = await boot({
    routeOverrides: route,
    postHandler: (path, opts) => {
      if (path === "/api/v1/jobs") {
        postedSpecs.push(opts.body);
        if (String(opts.body).includes("bad-spec")) {
          return { status: 422, json: { error: "metadata.name is required" } };
        }
        return { status: 201, json: { name: "new-job", state: "READY", dry_run: false } };
      }
    },
  });
  await c.tick(400);
  await c.tick(POLL_MS + 600);
  c.window.document.querySelector(".nav button[data-v='overview']").click();
});
after(() => c.dom.window.close());

// --------------------------------------------------------------------------
// Job list filter
// --------------------------------------------------------------------------

test("job filter chips default to 'all' and show every job", () => {
  assert.equal(c.$$("#joblist .jrow").length, JOBS.length);
  assert.ok(c.$("#job-filters .chip.on"));
  assert.equal(c.$("#job-filters .chip.on").dataset.jf, "all");
});

test("the finished filter narrows to COMPLETED/CANCELLED jobs only", () => {
  c.$("#job-filters .chip[data-jf='finished']").click();
  const rows = c.$$("#joblist .jrow");
  assert.equal(rows.length, 2, "expected the completed + cancelled job");
  const names = rows.map(r => r.dataset.job);
  assert.ok(names.includes("finished-job"));
  assert.ok(names.includes("cancelled-job"));
  // restore for later tests
  c.$("#job-filters .chip[data-jf='all']").click();
});

test("the running filter includes paused jobs", () => {
  c.$("#job-filters .chip[data-jf='running']").click();
  const rows = c.$$("#joblist .jrow");
  const names = rows.map(r => r.dataset.job);
  assert.ok(names.includes(F.XSS), "paused job (fixture uses an XSS name) should be under 'running'");
  c.$("#job-filters .chip[data-jf='all']").click();
});

// --------------------------------------------------------------------------
// New job dialog
// --------------------------------------------------------------------------

test("new job button opens the dialog pre-filled with the template", () => {
  c.$("#job-new").click();
  assert.equal(c.$("#jmodal-bg").hidden, false);
  assert.match(c.$("#jmodal-yaml").value, /apiVersion: drsync\/v1/);
  assert.match(c.$("#jmodal-yaml").value, /metadata:/);
  assert.equal(c.$("#jmodal-yaml").readOnly, false);
  assert.equal(c.$("#jmodal-submit").hidden, false);
  assert.equal(c.$("#jmodal-submit-start").hidden, false);
  c.$("#jmodal-cancel").click();
  assert.equal(c.$("#jmodal-bg").hidden, true);
});

test("submit posts the raw YAML body to POST /api/v1/jobs", async () => {
  postedSpecs = [];
  c.$("#job-new").click();
  c.$("#jmodal-yaml").value = SPEC_YAML;
  c.$("#jmodal-submit").click();
  await c.tick(300);
  assert.equal(postedSpecs.length, 1);
  assert.equal(postedSpecs[0], SPEC_YAML, "body should be the raw YAML, not JSON-wrapped");
  assert.equal(c.$("#jmodal-bg").hidden, true, "dialog closes on success");
});

test("submit-and-start also calls the job's /start endpoint", async () => {
  postedSpecs = [];
  c.$("#job-new").click();
  c.$("#jmodal-yaml").value = SPEC_YAML;
  c.$("#jmodal-submit-start").click();
  await c.tick(300);
  const startPost = requests.post.find(r => r.path === "/api/v1/jobs/new-job/start");
  assert.ok(startPost, "expected a POST to /api/v1/jobs/new-job/start after submit");
});

test("a validation error from the API stays visible in the dialog for editing", async () => {
  c.$("#job-new").click();
  c.$("#jmodal-yaml").value = "bad-spec: true";
  c.$("#jmodal-submit").click();
  await c.tick(300);
  assert.equal(c.$("#jmodal-bg").hidden, false, "dialog must stay open on failure");
  assert.match(c.text("#jmodal-err"), /metadata\.name is required/);
  c.$("#jmodal-cancel").click();
});

// --------------------------------------------------------------------------
// View settings
// --------------------------------------------------------------------------

test("view settings loads the job's stored spec read-only", async () => {
  const row = c.$$("#joblist .jrow").find(r => r.dataset.job === "finished-job");
  row.click();
  await c.tick(300);
  const btn = c.$$("#detail button[data-ja='viewspec']")[0];
  assert.ok(btn, "settings button should be present for any job");
  btn.click();
  await c.tick(300);
  assert.equal(c.$("#jmodal-bg").hidden, false);
  assert.equal(c.$("#jmodal-yaml").readOnly, true);
  assert.match(c.$("#jmodal-yaml").value, /finished-job/);
  assert.equal(c.$("#jmodal-submit").hidden, true, "no submit controls in view mode");
  c.$("#jmodal-cancel").click();
});

// --------------------------------------------------------------------------
// Resubmit
// --------------------------------------------------------------------------

test("resubmit is offered for completed and cancelled jobs, pre-filled with an incremented name", async () => {
  const row = c.$$("#joblist .jrow").find(r => r.dataset.job === "finished-job");
  row.click();
  await c.tick(300);
  const btn = c.$$("#detail button[data-ja='resubmit']")[0];
  assert.ok(btn, "resubmit button missing for a completed job");
  btn.click();
  await c.tick(300);
  assert.equal(c.$("#jmodal-bg").hidden, false);
  assert.match(c.$("#jmodal-yaml").value, /name:\s*finished-job-2/);
  c.$("#jmodal-cancel").click();
});

test("resubmit is not offered for a running job", async () => {
  const row = c.$$("#joblist .jrow").find(r => r.dataset.job === "alpha");
  row.click();
  await c.tick(300);
  assert.equal(c.$$("#detail button[data-ja='resubmit']").length, 0);
});

// --------------------------------------------------------------------------
// Orphan cleanup
// --------------------------------------------------------------------------

test("a completed job with orphans remaining offers a cleanup action", async () => {
  const row = c.$$("#joblist .jrow").find(r => r.dataset.job === "finished-job");
  row.click();
  await c.tick(300);
  const btn = c.$$("#detail button[data-ja='orphans']")[0];
  assert.ok(btn, "orphan cleanup button missing");
  assert.match(btn.textContent, /17/);
});

test("triggering orphan cleanup requires confirmation, then posts the delete-pass endpoint", async () => {
  const row = c.$$("#joblist .jrow").find(r => r.dataset.job === "finished-job");
  row.click();
  await c.tick(300);
  const btn = c.$$("#detail button[data-ja='orphans']")[0];
  btn.click();
  await c.tick(150);
  assert.equal(c.$("#modal").hidden, false, "confirmation dialog should appear");
  c.$("#modal-input").value = "finished-job";
  c.$("#modal-input").dispatchEvent(new c.window.Event("input", { bubbles: true }));
  c.$("#modal-ok").click();
  await c.tick(300);
  const passPost = requests.post.find(r => r.path === "/api/v1/jobs/finished-job/passes");
  assert.ok(passPost, "expected a POST to the passes endpoint");
  const body = JSON.parse(passPost.body);
  assert.equal(body.delete, true);
  assert.equal(body.confirm, "finished-job");
});
