#!/usr/bin/env bash
# drsync delete-fanout budget e2e: an orphan tree that is pathological by
# aggregate depth/branching, NOT by any single directory's own entry count —
# every directory anywhere in the tree stays comfortably under tuning.
# delete_split_threshold, so mechanism 1 (stream_delete_split, delete_
# fanout_e2e.sh / delete_fanout_nested_e2e.sh) never fires anywhere. Only
# tuning.delete_shard_budget (agent/src/delete.c, queue_delete_subdir) can
# catch this shape: once a shard's work budget runs out mid-descent, every
# not-yet-opened subdirectory is handed off as its own new top-level DELETE
# shard, the delete-pass analogue of the scan walker's queue_split.
#
# This is the exact shape a real production incident hit: a 77-way branching
# orphan tree, several levels deep, no single directory large enough to trip
# any reasonable threshold — the delete pass finished all its other work and
# then spent 6 hours on its last 2 (of what should have been many) shards.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
. "$ROOT/test/lib.sh"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/drsync-delfanoutbudget.XXXXXX")
read -r _CP _HP < <(pick_ports)
CP=${CP:-$_CP}; HP=${HP:-$_HP}
API="http://127.0.0.1:${HP}"; AUTH="Authorization: Bearer delfanoutbudgettok"
PASS=0
cleanup() {
    for p in "${APID:-}" "${CPID:-}"; do [[ -n "$p" ]] && kill "$p" 2>/dev/null || true; done
    wait 2>/dev/null || true
    if [[ $PASS -eq 1 ]]; then rm -rf "$WORK"; else echo "work dir kept: $WORK"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }
has() {
    local pat=$1 out
    shift
    out=$("$@") || return 1
    grep -q -- "$pat" <<<"$out"
}
export DRSYNC_SERVER="$API" DRSYNC_TOKEN=delfanoutbudgettok

API_TOKEN_FILE="$WORK/api-token"
echo -n delfanoutbudgettok >"$API_TOKEN_FILE"
chmod 600 "$API_TOKEN_FILE"
DRSYNC="$ROOT/bin/drsync"

# --- build -------------------------------------------------------------------
make -C "$ROOT/agent" -s
( cd "$ROOT" && go build -o bin/drsyncd ./coordinator/cmd/drsyncd \
             && go build -o bin/drsync ./cli/drsync )

# --- tree: branching but never individually wide ------------------------------
# orphandir/b00..b09 (10-way branch), each with c00..c09 (10-way branch again),
# each holding 5 files — 10*10*5 = 500 files total. Every directory's own
# immediate entry count is at most 10, comfortably under any threshold used
# below, so mechanism 1 (entry-count streaming) never triggers anywhere in
# this tree — only a work-budget-based handoff can fan this out.
SRC="$WORK/src"; DST="$WORK/dst"
mkdir -p "$SRC/keep" "$DST/keep"
echo keepme > "$SRC/keep/file.txt"
echo keepme > "$DST/keep/file.txt"
for b in $(seq 0 9); do
    for c in $(seq 0 9); do
        leaf="$DST/orphandir/b$(printf %02d "$b")/c$(printf %02d "$c")"
        mkdir -p "$leaf"
        for f in $(seq 1 5); do
            echo "junk $b $c $f" > "$leaf/f$(printf %02d "$f").txt"
        done
    done
done

# --- services ----------------------------------------------------------------
"$ROOT/bin/drsyncd" -data-dir "$WORK/coord" -listen-agent 127.0.0.1:$CP \
    -listen-http 127.0.0.1:$HP -api-token-file "$API_TOKEN_FILE" -log-level warn \
    >"$WORK/coord.log" 2>&1 &
CPID=$!
wait_coordinator "$API" "$AUTH" || exit 1
"$ROOT/agent/bin/drsync-agent" -c 127.0.0.1:$CP -i delfanoutbudget-agent -w 4 -C 4 \
    >"$WORK/agent.log" 2>&1 &
APID=$!
sleep 1

# delete_split_threshold set high enough that NOTHING in this tree (max 10
# entries in any one directory) ever trips it — mechanism 1 must stay
# completely inert here. delete_shard_budget=15 is small enough that a shard
# starting at orphandir (10 b-dirs) exhausts its budget a handful of objects
# into the first b-dir's own c-dirs, forcing at least several handoffs.
cat > "$WORK/job.yaml" <<EOF
apiVersion: drsync/v1
kind: Job
metadata: { name: delfanoutbudget }
spec:
  source: { path: $SRC }
  destination: { path: $DST }
  probe: { require_mount: false }
  passes: { max: 2, converge_when: { delta_files_below: 1 } }
  tuning: { delete_split_threshold: 1000, delete_shard_budget: 15 }
EOF
"$DRSYNC" job submit "$WORK/job.yaml" --start | grep -q "job delfanoutbudget started" \
    || fail "submit failed"
for _ in $(seq 1 60); do
    curl -sf -H "$AUTH" "$API/api/v1/jobs/delfanoutbudget" | grep -q '"state":"COMPLETED"' && break
    sleep 0.25
done
curl -sf -H "$AUTH" "$API/api/v1/jobs/delfanoutbudget" | grep -q '"state":"COMPLETED"' \
    || { tail -n 8 "$WORK"/agent.log "$WORK"/coord.log; fail "initial pass did not converge"; }

# orphandir must be journaled as a single ORPHAN record (never descended
# during scan, D5) even though the delete pass will fan it out via the
# budget mechanism.
ORPHANED=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/delfanoutbudget/journal?type=ORPHAN" | python3 -c '
import sys, json
recs = json.load(sys.stdin).get("records", [])
print("\n".join(r.get("rel_path", "") for r in recs))
')
grep -qx "orphandir" <<<"$ORPHANED" \
    || fail "orphandir was not journaled as a single top-level orphan (got: $(tr "\n" " " <<<"$ORPHANED"))"

# --- delete pass: budget-based fan-out must fire -------------------------------
has "DELETE pass triggered" \
    "$DRSYNC" pass trigger delfanoutbudget --delete-pass --i-know-this-deletes \
    || fail "delete pass trigger refused"

# High-water mark of KindDelete shards during the pass — the Shard Reaper
# deletes DONE ones once the phase drains, so a post-COMPLETED query alone
# would undercount (same fix delete_fanout_e2e.sh needed).
DB=$(ls "$WORK/coord"/*.db 2>/dev/null | head -1)
delete_shard_count() {
    python3 - "$DB" <<'PY'
import sqlite3, sys
c = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
print(c.execute("select count(*) from shards where kind='delete'").fetchone()[0])
PY
}
NDEL=0
DONE=0
for _ in $(seq 1 120); do
    n=$(delete_shard_count)
    [[ "${n:-0}" -gt "$NDEL" ]] && NDEL=$n
    curl -sf -H "$AUTH" "$API/api/v1/jobs/delfanoutbudget" | grep -q '"state":"COMPLETED"' \
        && { DONE=1; break; }
    sleep 0.25
done
[[ "$DONE" -eq 1 ]] \
    || { tail -n 8 "$WORK"/agent.log "$WORK"/coord.log; fail "delete pass did not complete"; }

# 1. fan-out fired via the BUDGET mechanism specifically: with no directory
#    anywhere in the tree ever over delete_split_threshold, mechanism 1 never
#    fires — the only way this tree splits into more than 1 shard at all is
#    queue_delete_subdir handing off unopened b/c-directories once a shard's
#    budget runs out. Before this fix, this whole tree (10 b-dirs * 10 c-dirs
#    * 5 files = 500 objects, comfortably reproducing the "no single dir is
#    large" production shape) would run to completion inside the ONE
#    top-level shard, start to finish, with zero fan-out.
[[ "$NDEL" -ge 3 ]] || fail "only $NDEL delete shards recorded; budget-based fan-out did not fire " \
    "(want >=3: the top-level shard plus at least one budget-exhausted handoff)"

# 2. orphandir and everything under it (all 10 b-dirs, all 100 c-dirs, all
#    500 files) is gone.
[[ ! -e "$DST/orphandir" ]] || fail "orphandir was not fully removed"

# 3. synced content untouched
DIFF=$(diff -r "$SRC" "$DST" 2>&1 || true)
[[ -z "$DIFF" ]] || fail "delete pass damaged synced content:"$'\n'"$DIFF"

# 4. no errors, nothing parked
"$DRSYNC" report delfanoutbudget --json > "$WORK/report.json"
python3 - "$WORK/report.json" <<'EOF' || fail "report shows errors or parked shards"
import json, sys
r = json.load(open(sys.argv[1]))
assert r["totals"]["errors"] == 0, r["totals"]["errors"]
assert r["parked_shard_count"] == 0, r["parked_shard_count"]
EOF

echo "delete shards recorded: $NDEL; branching-but-not-wide orphan tree fully removed; content intact"
PASS=1
echo "PASS: budget-based DELETE fan-out (queue_delete_subdir) fired on a tree with no wide directory OK"
