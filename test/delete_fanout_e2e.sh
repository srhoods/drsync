#!/usr/bin/env bash
# drsync delete-fanout e2e: a single orphan directory whose own entry count
# exceeds tuning.delete_split_threshold is streamed out as DeleteRemainder
# splits (docs/DESIGN-coordinator.md §2.2 DELETE fan-out) instead of being
# unlinked depth-first by one agent — asserted via the coordinator's recorded
# KindDelete shard count during the pass (must be > 1: the original orphan
# path plus at least one split-produced remainder shard) and via full,
# correct removal including the directory itself.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
. "$ROOT/test/lib.sh"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/drsync-delfanout.XXXXXX")
read -r _CP _HP < <(pick_ports)
CP=${CP:-$_CP}; HP=${HP:-$_HP}
API="http://127.0.0.1:${HP}"; AUTH="Authorization: Bearer delfanouttok"
PASS=0
cleanup() {
    for p in "${APID:-}" "${CPID:-}"; do [[ -n "$p" ]] && kill "$p" 2>/dev/null || true; done
    wait 2>/dev/null || true
    if [[ $PASS -eq 1 ]]; then rm -rf "$WORK"; else echo "work dir kept: $WORK"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }
export DRSYNC_SERVER="$API" DRSYNC_TOKEN=delfanouttok

API_TOKEN_FILE="$WORK/api-token"
echo -n delfanouttok >"$API_TOKEN_FILE"
chmod 600 "$API_TOKEN_FILE"
DRSYNC="$ROOT/bin/drsync"

# --- build -------------------------------------------------------------------
make -C "$ROOT/agent" -s
( cd "$ROOT" && go build -o bin/drsyncd ./coordinator/cmd/drsyncd \
             && go build -o bin/drsync ./cli/drsync )

# --- trees: a small source, a destination carrying one huge orphan dir -------
# The orphan directory is built directly on the destination (not synced then
# deleted from source) — it only needs to exist as a destination-only
# subtree for the scan to journal it as an ORPHAN; how it got there is not
# part of what this test exercises.
SRC="$WORK/src"; DST="$WORK/dst"
mkdir -p "$SRC/keep" "$DST/keep" "$DST/orphandir"
echo keepme > "$SRC/keep/file.txt"
echo keepme > "$DST/keep/file.txt"
for i in $(seq 1 300); do echo "junk $i" > "$DST/orphandir/f$(printf %04d "$i").txt"; done

# --- services ----------------------------------------------------------------
"$ROOT/bin/drsyncd" -data-dir "$WORK/coord" -listen-agent 127.0.0.1:$CP \
    -listen-http 127.0.0.1:$HP -api-token-file "$API_TOKEN_FILE" -log-level warn \
    >"$WORK/coord.log" 2>&1 &
CPID=$!
wait_coordinator "$API" "$AUTH" || exit 1
"$ROOT/agent/bin/drsync-agent" -c 127.0.0.1:$CP -i delfanout-agent -w 4 -C 4 \
    >"$WORK/agent.log" 2>&1 &
APID=$!
sleep 1

# Small thresholds so the 300-entry orphan directory (well below what a real
# deployment would call pathological) still exercises the fan-out path
# without the test needing to build a huge tree.
cat > "$WORK/job.yaml" <<EOF
apiVersion: drsync/v1
kind: Job
metadata: { name: delfanout }
spec:
  source: { path: $SRC }
  destination: { path: $DST }
  probe: { require_mount: false }
  passes: { max: 2, converge_when: { delta_files_below: 1 } }
  tuning: { delete_split_threshold: 50, delete_split_batch: 40 }
EOF
"$DRSYNC" job submit "$WORK/job.yaml" --start | grep -q "job delfanout started" \
    || fail "submit failed"
for _ in $(seq 1 60); do
    curl -sf -H "$AUTH" "$API/api/v1/jobs/delfanout" | grep -q '"state":"COMPLETED"' && break
    sleep 0.25
done
curl -sf -H "$AUTH" "$API/api/v1/jobs/delfanout" | grep -q '"state":"COMPLETED"' \
    || { tail -8 "$WORK"/agent.log "$WORK"/coord.log; fail "initial pass did not converge"; }

# orphandir must be journaled as a single ORPHAN record (never descended
# during scan, D5) even though it will fan out at delete time.
ORPHANED=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/delfanout/journal?type=ORPHAN" | python3 -c '
import sys, json
recs = json.load(sys.stdin).get("records", [])
print("\n".join(r.get("rel_path", "") for r in recs))
')
grep -qx "orphandir" <<<"$ORPHANED" \
    || fail "orphandir was not journaled as a single top-level orphan (got: $(tr "\n" " " <<<"$ORPHANED"))"

# --- delete pass: fan-out must actually fire ----------------------------------
has "DELETE pass triggered" \
    "$DRSYNC" pass trigger delfanout --delete-pass --i-know-this-deletes \
    || fail "delete pass trigger refused"

# Track the high-water mark of KindDelete shards recorded during the pass —
# the Shard Reaper deletes DONE delete shards once the phase drains, so a
# query run only after COMPLETED would read back whatever survived reaping,
# not proof the fan-out happened (same fix scale_e2e.sh needed for
# entrylist shards, and fanout_e2e.sh/chunk_e2e.sh needed for their own kinds).
DB=$(ls "$WORK/coord"/*.db 2>/dev/null | head -1)
delete_shard_count() {
    python3 - "$DB" <<'PY'
import sqlite3, sys
c = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
print(c.execute("select count(*) from shards where kind='delete'").fetchone()[0])
PY
}
NDEL=0
STATE=""
for _ in $(seq 1 120); do
    n=$(delete_shard_count)
    [[ "${n:-0}" -gt "$NDEL" ]] && NDEL=$n
    STATE=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/delfanout" | grep -o '"state":"[A-Z]*"' | tail -1)
    [[ "$STATE" == '"state":"COMPLETED"' ]] && break
    sleep 0.25
done
[[ "$STATE" == '"state":"COMPLETED"' ]] \
    || { tail -8 "$WORK"/agent.log "$WORK"/coord.log; fail "delete pass did not complete (state=$STATE)"; }

# 1. fan-out actually happened: more than just the one top-level orphan shard
#    (the original orphandir path) — split-produced remainder shards, plus the
#    coordinator-seeded cleanup shard for orphandir itself, must have run too.
[[ "$NDEL" -ge 3 ]] || fail "only $NDEL delete shards recorded; fan-out did not fire " \
    "(want >=3: at least one remainder batch, the original, and the cleanup shard)"

# 2. orphandir and everything under it is gone — including the directory
#    itself, which only the coordinator-seeded cleanup shard removes (nothing
#    in the split-produced children ever unlinks their own parent).
[[ ! -e "$DST/orphandir" ]] || fail "orphandir (or its cleanup) was not fully removed"

# 3. synced content untouched
DIFF=$(diff -r "$SRC" "$DST" 2>&1 || true)
[[ -z "$DIFF" ]] || fail "delete pass damaged synced content:"$'\n'"$DIFF"

# 4. no errors, nothing parked (report totals, same fields e2e.sh checks)
"$DRSYNC" report delfanout --json > "$WORK/report.json"
python3 - "$WORK/report.json" <<'EOF' || fail "report shows errors or parked shards"
import json, sys
r = json.load(open(sys.argv[1]))
assert r["totals"]["errors"] == 0, r["totals"]["errors"]
assert r["parked_shard_count"] == 0, r["parked_shard_count"]
EOF

echo "delete shards recorded: $NDEL; orphandir fully removed; content intact"
PASS=1
echo "PASS: pathological orphan directory fanned out across DELETE shards OK"
