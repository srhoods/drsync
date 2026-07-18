// Behavioural tests for the operations console (webui/console.html).
//
// These exist because the console's failure mode is silence. A field that
// stops being populated, a control that stops firing, or an unescaped path
// renders as a blank cell or a dash — nothing throws, no Go test fails, and
// the first person to notice is an operator mid-migration. The Go-side
// console_contract_test.go pins the JSON shapes; this pins what the page
// actually does with them.
//
// Run: make webui-test   (or: npm --prefix webui/test test)

import { test, before, after } from "node:test";
import assert from "node:assert/strict";

import { boot, requests, POLL_MS } from "./harness.mjs";
import { XSS } from "./fixtures.mjs";

let c;

before(async () => {
  c = await boot();
  // First poll paints; the second gives the /metrics rate differencing two
  // samples, without which throughput is legitimately zero.
  await c.tick(400);
  await c.tick(POLL_MS + 600);
});

after(() => c.dom.window.close());

// --------------------------------------------------------------------------
// Overview
// --------------------------------------------------------------------------

test("coordinator identity comes from /api/v1/info", () => {
  assert.equal(c.text("#hdr-epoch"), "55e20bae");
  assert.equal(c.text("#hdr-mtls"), "on");
  assert.equal(c.text("#hdr-ver"), "0.1.0");
});

test("KPI strip shows live counts, not placeholders", () => {
  assert.match(c.text("#k-jobs"), /^1/);
  assert.match(c.text("#k-agents"), /^2/);
  assert.equal(c.text("#k-park"), "2");
  for (const id of ["#k-bw", "#k-files", "#k-scan"]) {
    assert.notEqual(c.text(id), "–", `${id} never populated`);
    assert.match(c.text(id), /\d/, `${id} has no figure`);
  }
});

test("job rows render the pass rollup from the list endpoint", () => {
  assert.equal(c.$$("#joblist .jrow").length, 2);
  assert.match(c.$("#joblist").textContent, /walked/);
  assert.match(c.$("#joblist").textContent, /copied/);
});

test("listing N jobs costs one request, not one per job", () => {
  // The regression this guards: the console used to fetch /jobs/{name} for
  // every row on every 2.5s poll.
  const perJob = requests.get.filter(p => /^\/api\/v1\/jobs\/[^/]+$/.test(p));
  assert.deepEqual(perJob, [], `per-job detail fetches: ${perJob.join(", ")}`);
});

test("fleet table renders agents with rates and drain controls", () => {
  assert.equal(c.$$("#fleet tr.agrow").length, 2);
  assert.match(c.$("#fleet").textContent, /MiB/);
  assert.ok(c.$("#fleet button[data-ag='disable']"), "no drain control");
  assert.ok(c.$("#fleet button[data-ag='enable']"), "no resume control for drained agent");
});

test("job detail renders the ledger and convergence curve", () => {
  assert.match(c.$("#detail").textContent, /alpha/);
  assert.equal(c.$$("#detail table.pt tbody tr").length, 3, "2 passes + total row");
  assert.equal(c.$$("#detail .cbar").length, 2);
});

test("job controls are live, not disabled stubs", () => {
  for (const a of ["pause", "cancel", "pass", "delpass"]) {
    const btn = c.$(`#detail button[data-ja='${a}']`);
    assert.ok(btn, `missing ${a} control`);
    assert.equal(btn.disabled, false, `${a} control is disabled`);
  }
  assert.doesNotMatch(c.document.body.innerHTML, /Phase 2/,
    "phase-2 placeholder text still shipped");
});

// --------------------------------------------------------------------------
// Queue & shards
// --------------------------------------------------------------------------

test("queue KPIs report real depths", async () => {
  c.nav(1).click();
  await c.tick();
  assert.equal(c.text("#qk-q"), "252,904");
  assert.equal(c.text("#qk-l"), "2,046");
  assert.equal(c.text("#qk-d"), "852,186");
  assert.equal(c.text("#qk-p"), "2");
});

test("retry-pressure tiles come from scheduler counters", () => {
  // Previously hardcoded as "41" and "1.03" with no id and no code path.
  assert.equal(c.text("#qk-req"), "41");
  assert.match(c.text("#qk-rr"), /^1\.03/);
  assert.match(c.text("#qk-rr-sub"), /41 of 3,980/);
});

test("a labelled histogram is not misread as a global counter", () => {
  // drsync_shard_duration_seconds_* is in the fixture; if the allowlist regex
  // ever loosened, these tiles would take a bucket value.
  assert.equal(c.text("#qk-req"), "41");
});

test("refresh interval is stated truthfully", () => {
  assert.match(c.text("#q-updated"), /2\.5s/);
});

test("parked shards show age, not a hardcoded dash", () => {
  const rows = c.$$("#parkedtb tr");
  assert.equal(rows.length, 2);
  const ages = rows.map(r => r.children[7].textContent.trim());
  assert.deepEqual(ages, ["1h", "2m"], "age column not rendered from parked_at_ms");
});

test("parked shards offer retry and drop", () => {
  const retry = c.$("#parkedtb button[data-pk='retry']");
  assert.ok(retry);
  assert.equal(retry.disabled, false);
  assert.ok(c.$("#parkedtb button[data-pk='drop']"));
});

// --------------------------------------------------------------------------
// Errors browser
// --------------------------------------------------------------------------

test("errors view lists journal records", async () => {
  c.nav(2).click();
  await c.tick(300);
  assert.equal(c.$$("#etb tr").length, 2);
  assert.match(c.$("#etb").textContent, /open denied/);
  assert.match(c.text("#e-count"), /17 records/);
});

test("class chips are built from the by_class histogram", () => {
  // Regression: chips were rendered before the histogram loaded and never
  // rebuilt, so the filter row read "none" against a job full of errors.
  const chips = c.$("#e-classes").textContent;
  assert.match(chips, /EACCES 12/);
  assert.match(chips, /VERIFY_FAIL 4/);
});

// --------------------------------------------------------------------------
// Per-agent in-flight work
// --------------------------------------------------------------------------

const inflightFetches = () =>
  requests.get.filter(p => /\/api\/v1\/agents\/.+\/inflight$/.test(p));
const agentRow = id => c.$$("#fleet tr.agrow").find(r => r.dataset.exp === id);
const panel = () => (c.$("#fleet .infl") ? c.$("#fleet .infl").textContent : "<no panel>");

test("in-flight detail is not fetched until a row is expanded", async () => {
  c.nav(0).click();
  await c.tick();
  assert.deepEqual(inflightFetches(), []);
});

test("expanding an agent fetches only that agent", async () => {
  agentRow("agent-1").querySelector(".ag").click();
  await c.tick(250);
  const f = inflightFetches();
  assert.equal(f.length, 1);
  assert.match(f[0], /agent-1/);
});

test("in-flight panel shows what the agent is holding", () => {
  assert.ok(c.$("#fleet .infl"), "panel did not render");
  assert.match(panel(), /2 items/);
  assert.match(panel(), /as of .* ago/, "snapshot age not stated");
  assert.match(panel(), /chunk/);
  assert.match(panel(), /entry-list/);
});

test("running and queued items are distinguished", () => {
  // running_ms is 0 for a queued item; showing "0s running" would imply it is
  // wedged rather than simply not started.
  assert.match(panel(), /running/);
  assert.match(panel(), /13m/, "running item duration missing");
  assert.match(panel(), /queued/);
  assert.match(panel(), /4s/, "queued item should fall back to held_ms");
});

test("progress is shown, to separate a stuck shard from a slow one", () => {
  assert.match(panel(), /41\.2k done/);
  assert.ok(c.$("#fleet .ifi.lead"), "longest-running item not marked");
});

test("the expanded row refreshes, and no other agent is fetched", async () => {
  const before = inflightFetches().length;
  await c.tick(POLL_MS + 600);
  assert.ok(inflightFetches().length > before, "open row stopped refreshing");
  assert.ok(inflightFetches().every(p => /agent-1/.test(p)),
    "fetched an agent that was not expanded");
});

test("collapsing stops the fetching", async () => {
  agentRow("agent-1").querySelector(".ag").click();
  await c.tick(150);
  assert.equal(c.$("#fleet .infl"), null);
  const after = inflightFetches().length;
  await c.tick(POLL_MS + 600);
  assert.equal(inflightFetches().length, after, "still polling a collapsed row");
});

test("an agent that cannot report is not shown as idle", async () => {
  agentRow(XSS).querySelector(".ag").click();
  await c.tick(300);
  assert.doesNotMatch(panel(), /idle/,
    "supported:false rendered as idle — a stale agent would look healthy");
  assert.match(panel(), /does not report in-flight detail/);
  agentRow(XSS).querySelector(".ag").click();
  await c.tick(150);
});

// --------------------------------------------------------------------------
// Destructive-action gates
// --------------------------------------------------------------------------

test("cancel confirms before it POSTs", async () => {
  const before = requests.post.length;
  c.$("#detail button[data-ja='cancel']").click();
  await c.tick(120);
  assert.equal(c.$("#modal").hidden, false, "no confirm dialog");
  assert.equal(requests.post.length, before, "POSTed before confirming");
  c.$("#modal-cancel").click();
  await c.tick(120);
  assert.equal(c.$("#modal").hidden, true);
  assert.equal(requests.post.length, before, "dismissing the dialog still acted");
});

test("delete pass requires echoing the job name", async () => {
  c.$("#detail button[data-ja='delpass']").click();
  await c.tick(120);
  const ok = c.$("#modal-ok");
  assert.equal(ok.disabled, true, "confirm enabled before the name was typed");

  const type = v => {
    c.$("#modal-input").value = v;
    c.$("#modal-input").dispatchEvent(new c.window.Event("input"));
  };
  type("wrong");
  assert.equal(ok.disabled, true, "wrong name enabled confirm");
  type("alpha");
  assert.equal(ok.disabled, false, "correct name did not enable confirm");

  ok.click();
  await c.tick(250);
  const sent = requests.post.find(p =>
    /\/jobs\/alpha\/passes$/.test(p.path) && /"delete":true/.test(p.body || ""));
  assert.ok(sent, "delete pass was not sent");
  // The API gates this a second time on confirm === job name.
  assert.match(sent.body, /"confirm":"alpha"/);
});

// --------------------------------------------------------------------------
// Action routing
// --------------------------------------------------------------------------

test("drain POSTs the agent disable endpoint", async () => {
  c.$("#fleet button[data-ag='disable']").click();
  await c.tick(200);
  assert.ok(requests.post.some(p => /^\/api\/v1\/agents\/.+\/disable$/.test(p.path)),
    `no disable POST in ${requests.post.map(p => p.path).join(", ")}`);
});

test("parked retry POSTs the shard retry endpoint", async () => {
  c.nav(1).click();
  await c.tick(200);
  c.$("#parkedtb button[data-pk='retry']").click();
  await c.tick(200);
  assert.ok(requests.post.some(p => p.path === "/api/v1/parked/77/retry"),
    `no retry POST in ${requests.post.map(p => p.path).join(", ")}`);
});

// --------------------------------------------------------------------------
// Escaping
// --------------------------------------------------------------------------
// rel_path is filesystem-derived: an attacker only needs to create a file with
// a crafted name in a tree being migrated. Job names are accepted by the API
// verbatim. Both reach the DOM through innerHTML.

test("a hostile payload never executes", () => {
  assert.notEqual(c.window.__PWNED, true, "script from coordinator data ran");
  assert.equal(c.$$("img").length, 0, "injected element was parsed into the DOM");
});

test("hostile values render as text", async () => {
  assert.ok(c.$("#joblist").textContent.includes("evil<img"), "job name not escaped");
  assert.ok(c.$("#parkedtb").textContent.includes("evil<img"), "rel_path not escaped");
  c.nav(2).click();
  await c.tick(300);
  assert.ok(c.$("#etb").textContent.includes("evil<img"), "error rel_path not escaped");
});

// --------------------------------------------------------------------------

test("the page ran without uncaught script errors", () => {
  assert.deepEqual(c.scriptErrors, []);
});
