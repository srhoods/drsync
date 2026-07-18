#!/usr/bin/env bash
# drsync deep-tree e2e: a directory chain deeper than the walker's in-agent
# recursion cap (MAX_WALK_DEPTH=256) must still sync and converge — the walker
# pushes the too-deep subtree back to the coordinator as its own shard and
# re-walks it at depth 0, instead of recursing arbitrarily deep and overflowing
# its stack.
#
# With verify OFF and spread OFF, dir shards are the ONLY shards, so the pass's
# DONE count is exactly the number of directory shards: 1 if the whole chain was
# recursed in one shard (the old behaviour), >= 2 once the depth cap forces a
# split. That is the distinguishing signal.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
. "$ROOT/test/lib.sh"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/drsync-deep.XXXXXX")
# Ports come from the kernel (test/lib.sh), not a hardcoded pair: fixed ports
# collide with anything already listening — including another checkout's
# coordinator — and several of these scripts used to share the same pair, so
# they could not run side by side. Override to pin them.
read -r _CP _HP < <(pick_ports)
COORD_PORT=${COORD_PORT:-$_CP}
HTTP_PORT=${HTTP_PORT:-$_HP}
API="http://127.0.0.1:${HTTP_PORT}"
AUTH="Authorization: Bearer e2etoken"
PASS=0

cleanup() {
    [[ -n "${AGENT_PID:-}" ]] && kill "$AGENT_PID" 2>/dev/null || true
    [[ -n "${COORD_PID:-}" ]] && kill "$COORD_PID" 2>/dev/null || true
    wait 2>/dev/null || true
    if [[ $PASS -eq 1 ]]; then rm -rf "$WORK"; else echo "work dir kept: $WORK"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }

make -C "$ROOT/agent" -s
( cd "$ROOT" && go build -o bin/drsyncd ./coordinator/cmd/drsyncd \
             && go build -o bin/drsync ./cli/drsync )
DRSYNC="$ROOT/bin/drsync"
export DRSYNC_SERVER="$API" DRSYNC_TOKEN=e2etoken

# --- deep source chain (300 > MAX_WALK_DEPTH) with a leaf file at the bottom --
SRC="$WORK/src" DST="$WORK/dst"
DEPTH=300
CHAIN=$(printf 'd/%.0s' $(seq 1 $DEPTH))   # d/d/.../d  (DEPTH segments)
mkdir -p "$SRC/$CHAIN"
echo "bottom of a very deep tree" > "$SRC/${CHAIN}leaf.txt"
echo "near the top" > "$SRC/top.txt"

# --- services ----------------------------------------------------------------
"$ROOT/bin/drsyncd" -data-dir "$WORK/coord" \
    -listen-agent "127.0.0.1:${COORD_PORT}" -listen-http "127.0.0.1:${HTTP_PORT}" \
    -api-token e2etoken -log-level warn >"$WORK/coord.log" 2>&1 &
COORD_PID=$!
wait_coordinator "$API" "$AUTH" || exit 1

"$ROOT/agent/bin/drsync-agent" -c "127.0.0.1:${COORD_PORT}" -i agent-deep -w 4 \
    >"$WORK/agent.log" 2>&1 &
AGENT_PID=$!
sleep 1
curl -sf -H "$AUTH" "$API/api/v1/agents" | grep -q '"connected":true' \
    || fail "agent did not register"

# --- submit + run ------------------------------------------------------------
cat > "$WORK/job.yaml" <<EOF
apiVersion: drsync/v1
kind: Job
metadata:
  name: deep
spec:
  source: { path: $SRC }
  destination: { path: $DST }
  passes:
    max: 4
    converge_when:
      delta_files_below: 1
  verify:
    mode: off               # no verify shards → DONE counts dir shards only
  tuning:
    spread_mode: off        # no spread-forced splits → depth cap is the only splitter
EOF
"$DRSYNC" job submit "$WORK/job.yaml" --start | grep -q "job deep started" \
    || fail "job submit --start failed"

for _ in $(seq 1 180); do
    STATE=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/deep" | grep -o '"state":"[A-Z]*"' | head -1)
    [[ "$STATE" == '"state":"COMPLETED"' ]] && break
    sleep 0.5
done
[[ "${STATE:-}" == '"state":"COMPLETED"' ]] || {
    tail -8 "$WORK/agent.log" "$WORK/coord.log"
    fail "deep-tree job did not converge (state=${STATE:-none}) — walker may have crashed"
}
# agent must still be alive (a stack overflow would have killed it).
kill -0 "$AGENT_PID" 2>/dev/null || fail "agent process died during the deep walk"
JOB=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/deep")

# --- assertions --------------------------------------------------------------
# 1. the whole chain replicated, leaf content byte-exact.
[[ -d "$DST/$CHAIN" ]] || fail "deep directory chain not fully created in dst"
cmp -s "$SRC/${CHAIN}leaf.txt" "$DST/${CHAIN}leaf.txt" \
    || fail "deep leaf file missing or mismatched"
cmp -s "$SRC/top.txt" "$DST/top.txt" || fail "top-level file mismatch"

# 2. the depth cap forced a split: pass 1 has >= 2 dir shards (was 1 when the
#    chain recursed in a single shard).
DONE=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/deep/passes/1" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["shards"].get("DONE",0))')
[[ "${DONE:-0}" -ge 2 ]] \
    || fail "pass 1 had $DONE dir shard(s); depth cap did not split the deep chain"

# 3. converged: final pass copied nothing, no errors.
PL=$(echo "$JOB" | grep -o '"files_copied":[0-9]*' | tail -1 | cut -d: -f2)
[[ "${PL:-1}" -eq 0 ]] || fail "final pass still copied $PL files (not converged)"
echo "$JOB" | grep -q '"errors":[1-9]' && fail "job reported errors: $JOB"

echo "PASS: ${DEPTH}-deep tree synced via $DONE shards (depth cap sharded it), converged, no crash"
PASS=1
