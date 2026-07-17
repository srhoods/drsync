#!/usr/bin/env bash
# drsync chunk resilience: an agent dies mid-copy; its chunk leases expire and
# are re-granted elsewhere, and the big file still finalizes byte-exact.
#
# Exercises the idempotent re-execution the design relies on: chunk copies
# recreate their range in the shared temp, and a re-run finalize whose temp was
# already renamed by a lost-result predecessor still reports the file done
# rather than parking (which would stall the pass).
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d "${TMPDIR:-/tmp}/drsync-chunkres.XXXXXX")
CP=${CP:-17620}; HP=${HP:-17621}
API="http://127.0.0.1:${HP}"; AUTH="Authorization: Bearer restok"
PASS=0
cleanup() {
    for p in ${APIDS:-}; do kill "$p" 2>/dev/null || true; done
    [[ -n "${CPID:-}" ]] && kill "$CPID" 2>/dev/null || true
    wait 2>/dev/null || true
    if [[ $PASS -eq 1 ]]; then rm -rf "$WORK"; else echo "work dir kept: $WORK"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }
export DRSYNC_SERVER="$API" DRSYNC_TOKEN=restok
DRSYNC="$ROOT/bin/drsync"

make -C "$ROOT/agent" -s
( cd "$ROOT" && go build -o bin/drsyncd ./coordinator/cmd/drsyncd \
             && go build -o bin/drsync ./cli/drsync )

SRC="$WORK/src"; DST="$WORK/dst"
mkdir -p "$SRC" "$DST"
# 80 MiB / 2 MiB chunks = 40 data chunks: enough in flight that killing an
# agent lands on live chunk leases.
head -c 83886080 /dev/urandom > "$SRC/big.bin"
HUGE_SUM=$(sha256sum "$SRC/big.bin" | cut -d' ' -f1)

# Short lease TTL so a dead agent's chunks re-queue within the test's patience.
"$ROOT/bin/drsyncd" -data-dir "$WORK/coord" -listen-agent 127.0.0.1:$CP \
    -listen-http 127.0.0.1:$HP -api-token restok -lease-ttl 3s \
    -heartbeat-interval 1s -log-level warn >"$WORK/coord.log" 2>&1 &
CPID=$!
for _ in $(seq 1 40); do curl -sf "$API/healthz" >/dev/null 2>&1 && break; sleep 0.25; done
curl -sf "$API/healthz" >/dev/null || fail "coordinator did not come up"

APIDS=""
declare -A APID
for a in res-a res-b res-c; do
    "$ROOT/agent/bin/drsync-agent" -c 127.0.0.1:$CP -i "$a" -w 2 -C 4 \
        >"$WORK/$a.log" 2>&1 &
    APID[$a]=$!
    APIDS="$APIDS $!"
done
for _ in $(seq 1 40); do
    n=$(curl -sf -H "$AUTH" "$API/api/v1/agents" | grep -o '"connected":true' | wc -l)
    [[ "$n" -eq 3 ]] && break; sleep 0.25
done
[[ "${n:-0}" -eq 3 ]] || fail "expected 3 agents, got ${n:-0}"

cat > "$WORK/job.yaml" <<EOF
apiVersion: drsync/v1
kind: Job
metadata: { name: res }
spec:
  source: { path: $SRC }
  destination: { path: $DST }
  passes: { max: 3, converge_when: { delta_files_below: 1 } }
  copy: { server_side_copy: off, chunk_threshold: 1MiB, chunk_size: 2MiB }
  verify: { checksum: { sample_rate: 1.0 } }
EOF
"$DRSYNC" job submit "$WORK/job.yaml" --start >/dev/null || fail "submit failed"

# Kill one agent while chunks are in flight (the temp is being assembled).
sleep 1
kill -9 "${APID[res-b]}" 2>/dev/null || true
echo "killed res-b mid-copy"

STATE=""
for _ in $(seq 1 400); do
    STATE=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/res" | grep -o '"state":"[A-Z]*"' | head -1)
    [[ "$STATE" == '"state":"COMPLETED"' ]] && break
    sleep 0.5
done
[[ "$STATE" == '"state":"COMPLETED"' ]] || {
    tail -8 "$WORK"/coord.log; fail "did not converge after agent death (state=$STATE)"; }

# The file is intact despite the mid-flight death.
[[ "$(sha256sum "$DST/big.bin" | cut -d' ' -f1)" == "$HUGE_SUM" ]] \
    || fail "big.bin content mismatch after agent death"
[[ -z "$(find "$DST" -name '.drsync.tmp.*')" ]] || fail "temp residue left after recovery"

# No shard was permanently parked (a parked chunk would have stalled the pass).
PARKED=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/res/passes/1" \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['shards'].get('PARKED',0))")
[[ "$PARKED" -eq 0 ]] || fail "$PARKED shard(s) parked during recovery"

echo "PASS: 80 MiB file converged byte-exact after an agent was killed mid-copy; no parks"
PASS=1
