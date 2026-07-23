// Behavioural tests for the interactive login screen (webui/console.html).
//
// The console must never show live data before an authenticated session
// exists when the coordinator has login configured, and must never trap an
// open-dev-mode deployment (no auth.yaml at all) behind a login screen it
// can't satisfy. There is no coordinator-URL override or API-token entry in
// the WebUI — session-cookie login is the only credential path it offers.
// These pin both directions.
//
// Run: make webui-test   (or: npm --prefix webui/test test)

import { test, after } from "node:test";
import assert from "node:assert/strict";

import { boot, requests, POLL_MS } from "./harness.mjs";
import * as F from "./fixtures.mjs";

function routeWithWhoAmI(who) {
  return path => {
    if (path === "/api/v1/whoami") return { json: who };
    if (path === "/api/v1/jobs" || path.startsWith("/api/v1/jobs?")) return { json: F.JOBS };
    if (path.startsWith("/api/v1/queue")) return { json: F.QUEUE };
    if (path.startsWith("/api/v1/agents")) return { json: F.AGENTS };
    if (path.startsWith("/api/v1/info")) return { json: F.INFO };
    if (path.startsWith("/metrics")) return { text: F.metricsText() };
    return { json: {} };
  };
}

let dom1, dom2, dom3, dom4, dom5;
after(() => { for (const d of [dom1, dom2, dom3, dom4, dom5]) d?.dom.window.close(); });

test("login screen appears when login is configured and no session exists", async () => {
  const c = await boot({ routeOverrides: routeWithWhoAmI({ username: "", login_configured: true }) });
  dom1 = c;
  await c.tick(300);
  assert.equal(c.$("#login-screen").hidden, false, "login screen should be visible");
});

test("login screen is skipped when a session is already established", async () => {
  const c = await boot({ routeOverrides: routeWithWhoAmI({ username: "alice", login_configured: true }) });
  dom2 = c;
  await c.tick(300);
  await c.tick(POLL_MS + 300);
  assert.equal(c.$("#login-screen").hidden, true, "login screen should stay hidden for an existing session");
  assert.equal(c.text("#user-name"), "alice");
  assert.equal(c.$("#user-chip").hidden, false);
  assert.equal(c.$("#logout").hidden, false);
});

test("login screen is skipped entirely when login is not configured (open dev mode)", async () => {
  const c = await boot({ routeOverrides: routeWithWhoAmI({ username: "", login_configured: false }) });
  dom3 = c;
  await c.tick(300);
  await c.tick(POLL_MS + 300);
  assert.equal(c.$("#login-screen").hidden, true, "a deployment without auth.yaml must never show a login screen");
  assert.equal(c.$("#user-chip").hidden, true);
});

test("submitting the login form posts credentials and reveals the console on success", async () => {
  let loginAttempted = false;
  const c = await boot({
    routeOverrides: routeWithWhoAmI({ username: "", login_configured: true }),
    postHandler: (path, opts) => {
      if (path === "/api/v1/login") {
        loginAttempted = true;
        const body = JSON.parse(opts.body);
        if (body.username === "alice" && body.password === "hunter2") {
          return { status: 200, json: { username: "alice" } };
        }
        return { status: 401, json: { error: "invalid username or password" } };
      }
    },
  });
  dom4 = c;
  await c.tick(300);
  assert.equal(c.$("#login-screen").hidden, false);

  c.$("#login-user").value = "alice";
  c.$("#login-pass").value = "hunter2";
  c.$("#login-form").dispatchEvent(new c.window.Event("submit", { cancelable: true, bubbles: true }));
  await c.tick(300);

  assert.ok(loginAttempted, "the login POST should have been sent");
  assert.equal(c.$("#login-screen").hidden, true, "a successful login should hide the login screen");
  assert.equal(c.text("#user-name"), "alice");
  const loginPost = requests.post.find(r => r.path === "/api/v1/login");
  assert.ok(loginPost, "expected a POST to /api/v1/login");
});

test("there is no coordinator-URL/API-token override anywhere in the page", async () => {
  const c = await boot({ routeOverrides: routeWithWhoAmI({ username: "alice", login_configured: true }) });
  dom5 = c;
  await c.tick(300);
  for (const sel of ["#settings", "#cfg-pop", "#cfg-base", "#cfg-token", "#login-use-token"]) {
    assert.equal(c.$(sel), null, `${sel} should not exist — no connection override is offered`);
  }
});

let dom6;
after(() => dom6?.dom.window.close());

test("the username field has no autofocus attribute (it fights the password field for focus)", async () => {
  const c = await boot({ routeOverrides: routeWithWhoAmI({ username: "", login_configured: true }) });
  dom6 = c;
  await c.tick(300);
  // autofocus re-runs on some visibility/layout changes rather than strictly
  // once at parse time, so leaving it on the username input while JS also
  // calls .focus() on it (in showLogin) gave the field two independent ways
  // to steal focus back after the operator had already moved to the password
  // field. The fix keeps exactly one: the explicit JS call in showLogin.
  assert.equal(c.$("#login-user").hasAttribute("autofocus"), false,
    "login-user must not carry the autofocus attribute");
});
