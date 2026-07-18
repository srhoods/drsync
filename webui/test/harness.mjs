// Boots the real webui/console.html inside jsdom against mocked coordinator
// responses, and records the requests it makes.
//
// The console is a single self-contained page with no build step and no module
// boundary, so there is no unit to import — the only way to test it is to run
// it. This loads the shipped file verbatim: no copy, no transform, so the tests
// exercise exactly what the coordinator embeds.

import { JSDOM, VirtualConsole } from "jsdom";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

import * as F from "./fixtures.mjs";

const HERE = dirname(fileURLToPath(import.meta.url));
export const CONSOLE_HTML = join(HERE, "..", "console.html");

// Requests the console issued, so tests can assert on fetch behaviour — that
// the jobs list is not re-fetched per job, and that in-flight detail is pulled
// only for the expanded agent.
export const requests = { get: [], post: [] };

function route(path) {
  const inflight = path.match(/^\/api\/v1\/agents\/(.+)\/inflight$/);
  if (inflight) {
    const id = decodeURIComponent(inflight[1]);
    return F.INFLIGHT[id]
      ? { json: F.INFLIGHT[id] }
      : { status: 404, json: { error: "agent not connected" } };
  }
  if (path.startsWith("/metrics")) return { text: F.metricsText() };
  if (path === "/api/v1/jobs" || path.startsWith("/api/v1/jobs?")) return { json: F.JOBS };
  if (path.startsWith("/api/v1/queue")) return { json: F.QUEUE };
  if (path.startsWith("/api/v1/agents")) return { json: F.AGENTS };
  if (path.startsWith("/api/v1/info")) return { json: F.INFO };
  if (path.includes("/errors")) return { json: F.ERRORS };
  if (path.includes("/report")) return { json: F.REPORT };
  return { json: {} };
}

export async function boot() {
  const html = readFileSync(CONSOLE_HTML, "utf8");
  const virtualConsole = new VirtualConsole();
  const scriptErrors = [];
  virtualConsole.on("jsdomError", e => scriptErrors.push(e.message));
  virtualConsole.on("error", (...a) => scriptErrors.push(a.join(" ")));

  const dom = new JSDOM(html, {
    runScripts: "dangerously",
    pretendToBeVisual: true,
    virtualConsole,
    url: "http://coordinator.test/",
    beforeParse(w) {
      w.fetch = async (url, opts) => {
        const path = String(url).replace(/^https?:\/\/[^/]+/, "");
        if (opts && opts.method === "POST") {
          requests.post.push({ path, body: opts.body });
          return { ok: true, status: 200, json: async () => ({ ok: true }) };
        }
        requests.get.push(path);
        const r = route(path);
        const status = r.status ?? 200;
        return {
          ok: status < 400,
          status,
          json: async () => r.json ?? {},
          text: async () => r.text ?? "",
        };
      };
      // The console opens an event socket; the tests drive state through the
      // REST polling path, so a stub that never delivers frames is enough.
      w.WebSocket = class {
        constructor() { this.readyState = 1; setTimeout(() => this.onopen && this.onopen(), 0); }
        send() {} close() {}
      };
      // jsdom implements none of these. They are environment, not console
      // logic, so stubbing them tests the page rather than the polyfill.
      w.matchMedia = q => ({
        matches: false, media: q,
        addEventListener() {}, removeEventListener() {},
        addListener() {}, removeListener() {},
      });
      w.scrollTo = () => {};
      w.Element.prototype.scrollIntoView = () => {};
      w.HTMLCanvasElement.prototype.getContext = () => new Proxy({}, {
        get: (_t, k) => k === "createLinearGradient"
          ? () => ({ addColorStop() {} })
          : () => {},
      });
    },
  });

  const window = dom.window;
  const document = window.document;
  return {
    dom, window, document, scriptErrors,
    $: sel => document.querySelector(sel),
    $$: sel => [...document.querySelectorAll(sel)],
    text: sel => {
      const el = document.querySelector(sel);
      return el ? el.textContent.trim() : "<missing>";
    },
    // The page is timer-driven (2.5s poll, async renders); tests advance by
    // waiting rather than by reaching into its internals.
    tick: (ms = 200) => new Promise(r => setTimeout(r, ms)),
    nav: i => document.querySelectorAll(".nav button")[i],
  };
}

export const POLL_MS = 2500;
