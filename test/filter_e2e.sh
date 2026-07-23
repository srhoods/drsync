#!/usr/bin/env bash
# drsync filter e2e: a job with include/exclude filters must copy only the
# non-excluded entries, prune excluded subtrees, and still converge. Before the
# agent honoured filters (JobOptions field 5 was skipped) every one of these
# excluded files landed in the destination.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
. "$ROOT/test/lib.sh"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/drsync-filter.XXXXXX")
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

# --- build -------------------------------------------------------------------
make -C "$ROOT/agent" -s
( cd "$ROOT" && go build -o bin/drsyncd ./coordinator/cmd/drsyncd \
             && go build -o bin/drsync ./cli/drsync )
DRSYNC="$ROOT/bin/drsync"
export DRSYNC_SERVER="$API" DRSYNC_TOKEN=e2etoken

# The coordinator reads its bearer token from a file (never a raw CLI
# value); the file must be 0600 or drsyncd refuses to start.
API_TOKEN_FILE="$WORK/api-token"
echo -n e2etoken >"$API_TOKEN_FILE"
chmod 600 "$API_TOKEN_FILE"

# --- source tree: mix of kept and excluded entries ---------------------------
SRC="$WORK/src" DST="$WORK/dst"
mkdir -p "$SRC"/keep "$SRC"/data/.snapshot "$SRC"/logs "$SRC"/stuff/cache/inner
# kept
echo "alpha"  > "$SRC/keep/a.txt"
echo "beta"   > "$SRC/keep/b.log"
echo "real"   > "$SRC/data/real.dat"
echo "applog" > "$SRC/logs/app.log"
echo "root"   > "$SRC/rootfile"
# excluded by "**/*.tmp" (at depth, and at the root to exercise the '**/'
# matches-zero-segments rule)
echo "scratch" > "$SRC/keep/scratch.tmp"
echo "toplevel" > "$SRC/top.tmp"
# excluded by "**/.snapshot/**": the snapshot *contents* are dropped
echo "snap" > "$SRC/data/.snapshot/snap1"
echo "snap2" > "$SRC/data/.snapshot/snap2"
# excluded by "**/cache": the whole directory subtree is pruned
echo "cached" > "$SRC/stuff/cache/inner/blob"
echo "keepme" > "$SRC/stuff/keep.txt"

# --- start services -----------------------------------------------------------
"$ROOT/bin/drsyncd" -data-dir "$WORK/coord" \
    -listen-agent "127.0.0.1:${COORD_PORT}" -listen-http "127.0.0.1:${HTTP_PORT}" \
    -api-token-file "$API_TOKEN_FILE" -log-level warn >"$WORK/coord.log" 2>&1 &
COORD_PID=$!
wait_coordinator "$API" "$AUTH" || exit 1

"$ROOT/agent/bin/drsync-agent" -c "127.0.0.1:${COORD_PORT}" -i agent-filter -w 4 \
    >"$WORK/agent.log" 2>&1 &
AGENT_PID=$!
sleep 1
curl -sf -H "$AUTH" "$API/api/v1/agents" | grep -q '"connected":true' \
    || fail "agent did not register"

# --- submit + run job ----------------------------------------------------------
cat > "$WORK/job.yaml" <<EOF
apiVersion: drsync/v1
kind: Job
metadata:
  name: filt
spec:
  source: { path: $SRC }
  destination: { path: $DST }
  probe: { require_mount: false }   # test roots are plain dirs, not mounts
  filters:
    - exclude: "**/.snapshot/**"
    - exclude: "**/*.tmp"
    - exclude: "**/cache"
    - include: "**"
  passes:
    max: 4
    converge_when:
      delta_files_below: 1
  verify:
    checksum:
      sample_rate: 1.0
EOF
"$DRSYNC" job submit "$WORK/job.yaml" --set spec.tuning.shard_budget=4 --start \
    | grep -q "job filt started" || fail "job submit --start failed"

for _ in $(seq 1 120); do
    STATE=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/filt" | grep -o '"state":"[A-Z]*"' | head -1)
    [[ "$STATE" == '"state":"COMPLETED"' ]] && break
    sleep 0.5
done
[[ "${STATE:-}" == '"state":"COMPLETED"' ]] || {
    tail -5 "$WORK/agent.log" "$WORK/coord.log"
    fail "job did not converge (state=${STATE:-none})"
}
JOB=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/filt")

# --- assertions ---------------------------------------------------------------
# 1. kept entries copied, byte-identical
for rel in keep/a.txt keep/b.log data/real.dat logs/app.log rootfile stuff/keep.txt; do
    [[ -f "$DST/$rel" ]] || fail "kept file missing in dst: $rel"
    cmp -s "$SRC/$rel" "$DST/$rel" || fail "kept file content mismatch: $rel"
done

# 2. excluded entries absent
for rel in keep/scratch.tmp top.tmp data/.snapshot/snap1 data/.snapshot/snap2; do
    [[ ! -e "$DST/$rel" ]] || fail "excluded entry present in dst: $rel"
done

# 3. "**/cache" prunes the whole subtree — the dir must never be descended
[[ ! -e "$DST/stuff/cache" ]] || fail "excluded directory subtree not pruned: stuff/cache"

# 4. no *.tmp anywhere in the destination
if find "$DST" -name '*.tmp' | grep -q .; then
    fail "a *.tmp file slipped into the destination: $(find "$DST" -name '*.tmp')"
fi

# 5. convergence: final pass copied nothing, no verify failures, no errors
PL=$(echo "$JOB" | grep -o '"files_copied":[0-9]*' | tail -1 | cut -d: -f2)
[[ "${PL:-1}" -eq 0 ]] || fail "final pass still copied $PL files (not converged)"
echo "$JOB" | grep -q '"verify_fail":[1-9]' && fail "verify failures reported"
echo "$JOB" | grep -q '"errors":[1-9]' && fail "job reported errors: $JOB"

# 6. the excluded files must not appear in the copy journal either
if "$DRSYNC" journal cat filt --pass 1 --type copied --jsonl 2>/dev/null \
    | grep -Eq '\.tmp"|/\.snapshot/|/cache/'; then
    fail "an excluded path was journalled as copied"
fi

echo "PASS: filters honoured — kept files copied, excluded files/subtrees pruned, converged"
PASS=1
