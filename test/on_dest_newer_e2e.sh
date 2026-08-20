#!/usr/bin/env bash
# drsync copy.on_dest_newer e2e: a merge-dataset scenario where the
# destination has been edited out-of-band and is now newer than the source
# for one file. Default (skip) must leave that file's content and mtime
# completely untouched and record JR_SKIPPED_NEWER — not silently overwrite
# it, which was the only behavior before this option existed (source always
# won regardless of mtime direction, agent/src/walker.c's times_equal is an
# unsigned |diff| check). Also covers the ordinary "source is newer" case
# still copying normally, and the explicit on_dest_newer: overwrite opt-out
# restoring the old always-overwrite behavior.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
. "$ROOT/test/lib.sh"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/drsync-destnewer.XXXXXX")
read -r _CP _HP < <(pick_ports)
CP=${CP:-$_CP}; HP=${HP:-$_HP}
API="http://127.0.0.1:${HP}"; AUTH="Authorization: Bearer destnewertok"
PASS=0
cleanup() {
    for p in "${APID:-}" "${CPID:-}"; do [[ -n "$p" ]] && kill "$p" 2>/dev/null || true; done
    wait 2>/dev/null || true
    if [[ $PASS -eq 1 ]]; then rm -rf "$WORK"; else echo "work dir kept: $WORK"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }
export DRSYNC_SERVER="$API" DRSYNC_TOKEN=destnewertok

API_TOKEN_FILE="$WORK/api-token"
echo -n destnewertok >"$API_TOKEN_FILE"
chmod 600 "$API_TOKEN_FILE"
DRSYNC="$ROOT/bin/drsync"

wait_completed() {
    local job=$1 i
    for i in $(seq 1 60); do
        curl -sf -H "$AUTH" "$API/api/v1/jobs/$job" | grep -q '"state":"COMPLETED"' && return 0
        sleep 0.25
    done
    tail -n 8 "$WORK"/agent.log "$WORK"/coord.log
    fail "$job did not converge"
}

# --- build -------------------------------------------------------------------
make -C "$ROOT/agent" -s
( cd "$ROOT" && go build -o bin/drsyncd ./coordinator/cmd/drsyncd \
             && go build -o bin/drsync ./cli/drsync )

SRC="$WORK/src"; DST="$WORK/dst"
mkdir -p "$SRC"
echo "source v1" > "$SRC/a.txt"
echo "source v1" > "$SRC/b.txt"
echo "source v1" > "$SRC/c.txt"

# --- services ----------------------------------------------------------------
"$ROOT/bin/drsyncd" -data-dir "$WORK/coord" -listen-agent 127.0.0.1:$CP \
    -listen-http 127.0.0.1:$HP -api-token-file "$API_TOKEN_FILE" -log-level warn \
    >"$WORK/coord.log" 2>&1 &
CPID=$!
wait_coordinator "$API" "$AUTH" || exit 1
"$ROOT/agent/bin/drsync-agent" -c 127.0.0.1:$CP -i destnewer-agent -w 4 -C 4 \
    >"$WORK/agent.log" 2>&1 &
APID=$!
sleep 1

cat > "$WORK/job.yaml" <<EOF
apiVersion: drsync/v1
kind: Job
metadata: { name: destnewer }
spec:
  source: { path: $SRC }
  destination: { path: $DST }
  probe: { require_mount: false }
  passes: { max: 1, converge_when: { delta_files_below: 1 } }
EOF
"$DRSYNC" job submit "$WORK/job.yaml" --start | grep -q "job destnewer started" \
    || fail "submit failed"
wait_completed destnewer

for f in a.txt b.txt c.txt; do
    [[ "$(cat "$DST/$f")" == "source v1" ]] || fail "pass 1: $DST/$f not synced"
done

# --- out-of-band destination edits, one per scenario ---------------------------
# a.txt: destination edited AFTER the source (dest strictly newer) — the
# merge scenario this option exists for.
sleep 1.2
echo "dest edited (should survive)" > "$DST/a.txt"
touch -d "+1 hour" "$DST/a.txt"

# b.txt: source edited normally (source newer, ordinary case) — must still
# copy exactly as before this option existed.
echo "source v2 (should win)" > "$SRC/b.txt"

# c.txt: left alone on both sides — must stay "clean", not spuriously
# skipped or recopied.

# --- pass 2: default on_dest_newer (skip) --------------------------------------
has() { local pat=$1; shift; "$@" | grep -q -- "$pat"; }
has "pass triggered" "$DRSYNC" pass trigger destnewer || fail "pass 2 trigger failed"
wait_completed destnewer

[[ "$(cat "$DST/a.txt")" == "dest edited (should survive)" ]] \
    || fail "pass 2: a.txt was overwritten despite being newer at the destination (default should be skip)"
[[ "$(cat "$DST/b.txt")" == "source v2 (should win)" ]] \
    || fail "pass 2: b.txt (source newer, ordinary case) was not copied"
[[ "$(cat "$DST/c.txt")" == "source v1" ]] || fail "pass 2: c.txt (unchanged) diverged unexpectedly"

"$DRSYNC" report destnewer --json > "$WORK/report2.json"
python3 - "$WORK/report2.json" <<'EOF' || fail "pass 2 report: unexpected totals"
import json, sys
r = json.load(open(sys.argv[1]))
t = r["totals"]
assert t["errors"] == 0, t["errors"]
assert t["skipped_newer"] == 1, t["skipped_newer"]
EOF

SKIPPED=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/destnewer/journal?type=SKIPPED_NEWER&pass=2" | python3 -c '
import sys, json
recs = json.load(sys.stdin).get("records", [])
print("\n".join(r.get("rel_path", "") for r in recs))
')
grep -qF "a.txt" <<<"$SKIPPED" \
    || fail "a.txt not recorded as JR_SKIPPED_NEWER in pass 2's journal (got: $SKIPPED)"

echo "default on_dest_newer (skip): dest-newer file preserved, source-newer file still copied, clean file untouched"

# --- pass 3: on_dest_newer: overwrite must restore the old always-wins behavior
cat > "$WORK/job2.yaml" <<EOF
apiVersion: drsync/v1
kind: Job
metadata: { name: destnewer-overwrite }
spec:
  source: { path: $SRC }
  destination: { path: $DST }
  probe: { require_mount: false }
  passes: { max: 1, converge_when: { delta_files_below: 1 } }
  copy: { on_dest_newer: overwrite }
EOF
"$DRSYNC" job submit "$WORK/job2.yaml" --start | grep -q "job destnewer-overwrite started" \
    || fail "second job submit failed"
wait_completed destnewer-overwrite

[[ "$(cat "$DST/a.txt")" == "source v1" ]] \
    || fail "on_dest_newer: overwrite did not restore source-always-wins behavior for a.txt"

echo "PASS: copy.on_dest_newer default (skip) and explicit overwrite both behave correctly"
PASS=1
