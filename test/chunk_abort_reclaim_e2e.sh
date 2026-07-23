#!/usr/bin/env bash
# drsync chunk-abort reclaim: a chunk group abandoned mid-assembly must not
# leave its temp behind when the job ends on that pass.
#
# A group whose source drifts under it is aborted: no finalize is seeded and a
# partially written temp is left in the destination. The agent's orphan sweep
# will NOT collect it — it spares temps tagged with the pass it is running,
# which is what keeps a re-walk from deleting a group's temp while its chunks
# are still writing — so with passes.max: 1 nothing else ever would. The
# coordinator therefore seeds a reclaim task per unfinalized group once the scan
# phase has drained (no chunk queued or leased, so the name is provably dead).
#
# passes.max: 1 is the point of the test: with a second pass the temp would be
# reclaimed by the next walk as a foreign-pass tag, and the sweep would not be
# what is under test.
#
# Known-flaky watch (2026-07-19): see the same note in chunk_resilience_e2e.sh.
# Both scripts left kept work dirs in /tmp repeatedly during the preceding week
# and neither reproduced in 37 runs; the diagnostic logs are gone, so the cause
# is unknown rather than fixed.
#
# This script's standing suspect is the window it has to hit: it polls every
# 20ms for n_done >= 1 and then appends to the source, so the drift has to land
# between the first chunk completing and the last. If the copy outruns the
# poller the group finalizes and the `state = aborted` assertion fails. CI runs
# this as its own leg and keeps logs on failure — that is the detector.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
. "$ROOT/test/lib.sh"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/drsync-abortreclaim.XXXXXX")
# Ports come from the kernel (test/lib.sh), not a hardcoded pair: fixed ports
# collide with anything already listening — including another checkout's
# coordinator — and several of these scripts used to share the same pair, so
# they could not run side by side. Override with CP/HP to pin them.
read -r _CP _HP < <(pick_ports)
CP=${CP:-$_CP}; HP=${HP:-$_HP}
API="http://127.0.0.1:${HP}"; AUTH="Authorization: Bearer abrtok"
PASS=0
cleanup() {
    for p in ${APIDS:-}; do kill "$p" 2>/dev/null || true; done
    [[ -n "${CPID:-}" ]] && kill "$CPID" 2>/dev/null || true
    wait 2>/dev/null || true
    if [[ $PASS -eq 1 ]]; then rm -rf "$WORK"; else echo "work dir kept: $WORK"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }
export DRSYNC_SERVER="$API" DRSYNC_TOKEN=abrtok

# The coordinator reads its bearer token from a file (never a raw CLI
# value); the file must be 0600 or drsyncd refuses to start.
API_TOKEN_FILE="$WORK/api-token"
echo -n abrtok >"$API_TOKEN_FILE"
chmod 600 "$API_TOKEN_FILE"
DRSYNC="$ROOT/bin/drsync"

make -C "$ROOT/agent" -s
( cd "$ROOT" && go build -o bin/drsyncd ./coordinator/cmd/drsyncd \
             && go build -o bin/drsync ./cli/drsync )

SRC="$WORK/src"; DST="$WORK/dst"
mkdir -p "$SRC/d" "$DST"
# Big enough that the copy is still running when the source is mutated below.
head -c 268435456 /dev/urandom > "$SRC/d/big.bin"
echo small > "$SRC/d/note.txt"

"$ROOT/bin/drsyncd" -data-dir "$WORK/coord" -listen-agent 127.0.0.1:$CP \
    -listen-http 127.0.0.1:$HP -api-token-file "$API_TOKEN_FILE" -log-level warn \
    >"$WORK/coord.log" 2>&1 &
CPID=$!
wait_coordinator "$API" "$AUTH" || exit 1

APIDS=""
for a in ab-a ab-b; do
    "$ROOT/agent/bin/drsync-agent" -c 127.0.0.1:$CP -i "$a" -w 2 -C 2 \
        >"$WORK/$a.log" 2>&1 &
    APIDS="$APIDS $!"
done
for _ in $(seq 1 40); do
    n=$(curl -sf -H "$AUTH" "$API/api/v1/agents" | { grep -o '"connected":true' || true; } | wc -l)
    [[ "$n" -eq 2 ]] && break; sleep 0.25
done
[[ "${n:-0}" -eq 2 ]] || fail "expected 2 agents, got ${n:-0}"

cat > "$WORK/job.yaml" <<EOF
apiVersion: drsync/v1
kind: Job
metadata: { name: abrt }
spec:
  source: { path: $SRC }
  destination: { path: $DST }
  probe: { require_mount: false }   # test roots are plain dirs, not mounts
  passes: { max: 1 }
  copy: { server_side_copy: off, chunk_threshold: 1MiB, chunk_size: 2MiB }
  verify: { mode: off }
EOF
"$DRSYNC" job submit "$WORK/job.yaml" --start >/dev/null || fail "submit failed"

# Drift the source once at least one chunk has landed. The wait is what makes
# the test non-vacuous: a chunk checks the source gen BEFORE it creates or
# writes the temp, so drifting from t=0 aborts every chunk before the temp ever
# exists (n_done stays 0) and there is nothing to reclaim — the test then passes
# whether or not the sweep runs. Waiting for n_done >= 1 means the temp is on
# disk with data in it, and the bump aborts the chunks that remain.
DONE0=0
for _ in $(seq 1 600); do
    DONE0=$(python3 - "$WORK/coord/state.db" <<'PY'
import sqlite3, sys
try:
    con = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
    row = con.execute("SELECT n_done FROM chunk_groups WHERE rel_path='d/big.bin'").fetchone()
    print(row[0] if row else 0)
except Exception:
    print(0)
PY
)
    [[ "${DONE0:-0}" -ge 1 ]] && break
    sleep 0.02
done
[[ "${DONE0:-0}" -ge 1 ]] || fail "no chunk completed before the copy ended — cannot abort mid-assembly"
printf 'drift' >> "$SRC/d/big.bin"

STATE=""
for _ in $(seq 1 400); do
    STATE=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/abrt" | grep -o '"state":"[A-Z]*"' | head -1)
    [[ "$STATE" == '"state":"COMPLETED"' ]] && break
    sleep 0.5
done
[[ "$STATE" == '"state":"COMPLETED"' ]] || {
    tail -8 "$WORK"/coord.log; fail "job did not complete (state=$STATE)"; }

# Two conditions make this test non-vacuous, and both have been wrong once:
#   state = aborted — the drift landed inside the copy at all;
#   n_done >= 1     — it landed AFTER a chunk had written the temp. A chunk
#                     checks the source gen before it creates or writes the
#                     temp, so a group aborted at n_done == 0 never produced a
#                     temp, and the assertions below would hold with the
#                     reclaim sweep removed entirely.
read -r GSTATE GDONE < <(python3 - "$WORK/coord/state.db" <<'PY'
import sqlite3, sys
con = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
row = con.execute("SELECT state, n_done FROM chunk_groups WHERE rel_path='d/big.bin'").fetchone()
print(row[0], row[1]) if row else print("none", 0)
PY
)
[[ "$GSTATE" == "aborted" ]] || fail "chunk group state is '$GSTATE', want 'aborted' \
— the drift never landed inside the copy, so no temp was abandoned and nothing was exercised"
[[ "${GDONE:-0}" -ge 1 ]] || fail "group aborted at n_done=$GDONE, before any chunk wrote \
— no temp was ever created, so the reclaim path was not exercised"

# The abandoned temp is gone even though no later pass will ever run.
LEFT=$(find "$DST" -name '.drsync.tmp.*' -print)
[[ -z "$LEFT" ]] || fail "aborted group left temp residue after the final pass: $LEFT"

# The unaffected small file still copied.
[[ -f "$DST/d/note.txt" ]] || fail "note.txt was not copied"

echo "PASS: aborted chunk group's temp reclaimed before the job ended (no later pass)"
PASS=1
