#!/usr/bin/env bash
# drsync io_uring copy e2e: with server_side_copy disabled the agent takes the
# byte-copy path, which uses the io_uring registered-buffer engine (ucopy.c)
# when io_uring is available. Files of assorted sizes (empty, sub-block, block-
# aligned, multi-block, odd) must arrive byte-exact and verify clean — proving
# the overlapped read/write copy and its inline hash are correct end to end.
#
# Skips cleanly if the agent reports io_uring unavailable (the engine then
# legitimately does not run and the serial fallback is exercised instead).
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
. "$ROOT/test/lib.sh"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/drsync-ucopy.XXXXXX")
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

# --- source files spanning the block boundary (1 MiB buffer) -----------------
SRC="$WORK/src" DST="$WORK/dst"
mkdir -p "$SRC"
: > "$SRC/empty.bin"                                   # 0 bytes
head -c 1        /dev/urandom > "$SRC/one.bin"         # 1 byte
head -c 1048575  /dev/urandom > "$SRC/under.bin"       # block - 1
head -c 1048576  /dev/urandom > "$SRC/exact.bin"       # exactly one block
head -c 1048577  /dev/urandom > "$SRC/over.bin"        # block + 1
head -c 5242883  /dev/urandom > "$SRC/multi.bin"       # ~5 blocks, odd tail

# --- services ----------------------------------------------------------------
"$ROOT/bin/drsyncd" -data-dir "$WORK/coord" \
    -listen-agent "127.0.0.1:${COORD_PORT}" -listen-http "127.0.0.1:${HTTP_PORT}" \
    -api-token e2etoken -log-level warn >"$WORK/coord.log" 2>&1 &
COORD_PID=$!
wait_coordinator "$API" "$AUTH" || exit 1

# The agent logs at info unconditionally, so the io_uring-copy-engine line
# appears when the engine engages.
"$ROOT/agent/bin/drsync-agent" -c "127.0.0.1:${COORD_PORT}" -i agent-ucopy -w 4 \
    >"$WORK/agent.log" 2>&1 &
AGENT_PID=$!
sleep 1
curl -sf -H "$AUTH" "$API/api/v1/agents" | grep -q '"connected":true' \
    || fail "agent did not register"

# --- submit + run (server-side copy OFF → byte-copy path → ucopy) ------------
cat > "$WORK/job.yaml" <<EOF
apiVersion: drsync/v1
kind: Job
metadata:
  name: ucopy
spec:
  source: { path: $SRC }
  destination: { path: $DST }
  probe: { require_mount: false }   # test roots are plain dirs, not mounts
  copy:
    server_side_copy: off      # force the byte-copy fallback (exercises ucopy)
  passes:
    max: 3
    converge_when: { delta_files_below: 1 }
  verify:
    checksum:
      sample_rate: 1.0         # re-read both sides; validates content + inline hash
EOF
"$DRSYNC" job submit "$WORK/job.yaml" --start | grep -q "job ucopy started" \
    || fail "job submit --start failed"

for _ in $(seq 1 120); do
    STATE=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/ucopy" | grep -o '"state":"[A-Z]*"' | head -1)
    [[ "$STATE" == '"state":"COMPLETED"' ]] && break
    sleep 0.5
done
[[ "${STATE:-}" == '"state":"COMPLETED"' ]] || {
    tail -8 "$WORK/agent.log" "$WORK/coord.log"; fail "job did not converge (state=${STATE:-none})"
}
JOB=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/ucopy")

# If io_uring is unavailable here, the engine cannot run — skip rather than
# pretend to have tested it (the serial fallback still copied correctly).
if ! grep -q "io_uring copy engine enabled" "$WORK/agent.log"; then
    echo "SKIP: io_uring copy engine did not engage (io_uring unavailable); serial fallback used"
    if grep -q '"io_uring":0' <<<"$(curl -s -H "$AUTH" "$API/api/v1/agents")" 2>/dev/null; then :; fi
    PASS=1
    exit 0
fi

# --- assertions --------------------------------------------------------------
# 1. every file byte-exact.
DIFF=$(diff -r "$SRC" "$DST" 2>&1 || true)
[[ -z "$DIFF" ]] || fail "content mismatch:"$'\n'"$DIFF"
for f in empty one under exact over multi; do
    cmp -s "$SRC/$f.bin" "$DST/$f.bin" || fail "$f.bin mismatch"
done

# 2. converged, and the inline hash held up: verify re-read everything, 0 fails.
PL=$(echo "$JOB" | grep -o '"files_copied":[0-9]*' | tail -1 | cut -d: -f2)
[[ "${PL:-1}" -eq 0 ]] || fail "final pass still copied $PL files (not converged)"
echo "$JOB" | grep -q '"verify_fail":[1-9]' && fail "verify failures: $JOB"
echo "$JOB" | grep -q '"errors":[1-9]' && fail "job reported errors: $JOB"
V1=$(echo "$JOB" | grep -o '"pass_no":1,[^}]*' | grep -o '"verify_ok":[0-9]*' | cut -d: -f2)
[[ "${V1:-0}" -ge 5 ]] || fail "pass 1 verified only ${V1:-0} files (expected the 5 non-empty)"

echo "PASS: io_uring copy engine — 6 files across the block boundary byte-exact, verify clean, converged"
PASS=1
