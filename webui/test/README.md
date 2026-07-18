# Console tests

Behavioural tests for `webui/console.html`, run in [jsdom].

```
make webui-test          # from the repo root; installs deps on first run
npm --prefix webui/test test
```

Needs Node ≥ 20. `make test` stays Go-only, so a Go toolchain alone is still
enough to build and check the coordinator; `make test-all` runs both.

## Why these exist

The console's failure mode is silence. A field that stops being populated, a
control that stops firing, or a value that stops being escaped renders as a
blank cell or a dash — nothing throws, no Go test fails, and the first person
to find out is an operator mid-migration. Several of the assertions here are
regressions that actually shipped:

- KPI tiles that were hardcoded in markup and never written by any code path.
- A parked-shard age column that was a literal `—` because the timestamp was
  in the store but never serialised.
- Error-class filter chips built before the histogram they render had loaded,
  and never rebuilt.
- Job names and `rel_path` reaching `innerHTML` unescaped. `rel_path` comes
  from the tree being migrated, so an attacker only had to create a file with
  a crafted name.

`coordinator/internal/api/console_contract_test.go` is the other half: it pins
the JSON field names the page binds to. These tests pin what the page does
with them. Neither catches the other's regressions.

## Layout

| File | What it is |
|------|------------|
| `console.test.mjs` | The tests. |
| `harness.mjs` | Boots the real `console.html` in jsdom, routes `fetch` to the fixtures, records requests. |
| `fixtures.mjs` | Mocked coordinator responses, shaped to the REST contract in `../README.md`. |

The page is loaded verbatim — no copy, no transform — so the tests exercise
exactly the file the coordinator embeds. It has no build step and no module
boundary, so there is no unit to import: the only way to test it is to run it.

## Writing tests

Tests share one booted page and run in file order, because much of what is
worth asserting is sequential (expand a row, poll again, collapse it). Advance
time with `c.tick(ms)` rather than reaching into the page's internals; the poll
interval is exported as `POLL_MS`.

`requests.get` / `requests.post` record what the page asked for, which is how
the fetch-shape assertions work — that listing N jobs stays one request, and
that in-flight detail is pulled only for the expanded agent.

## Check that a new test can fail

A test that cannot fail is worse than no test. Break the thing it covers and
confirm it goes red before committing:

```
# escaping
-  const esc = v => String(v == null ? "" : v).replace(/[&<>"']/g, c => ESC[c]);
+  const esc = v => String(v == null ? "" : v);
```

That mutation should fail the XSS tests and nothing else.

[jsdom]: https://github.com/jsdom/jsdom
