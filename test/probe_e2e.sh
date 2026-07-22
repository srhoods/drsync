#!/usr/bin/env bash
# drsync probe-gate e2e: a per-agent mount probe must gate pass start. A job
# whose source mount is missing on the agent is blocked at the probe — no data
# is copied and the pass never leaves PROBING — while a healthy job passes the
# probe and converges. Before the gate existed the pass started immediately and
# only failed later, after other work had run.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
. "$ROOT/test/lib.sh"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/drsync-probe.XXXXXX")
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

jobstate() { curl -sf -H "$AUTH" "$API/api/v1/jobs/$1" | grep -o '"state":"[A-Z]*"' | head -1; }

# --- build + start ------------------------------------------------------------
make -C "$ROOT/agent" -s
( cd "$ROOT" && go build -o bin/drsyncd ./coordinator/cmd/drsyncd \
             && go build -o bin/drsync ./cli/drsync )
DRSYNC="$ROOT/bin/drsync"
export DRSYNC_SERVER="$API" DRSYNC_TOKEN=e2etoken

"$ROOT/bin/drsyncd" -data-dir "$WORK/coord" \
    -listen-agent "127.0.0.1:${COORD_PORT}" -listen-http "127.0.0.1:${HTTP_PORT}" \
    -api-token e2etoken -log-level info >"$WORK/coord.log" 2>&1 &
COORD_PID=$!
wait_coordinator "$API" "$AUTH" || exit 1

"$ROOT/agent/bin/drsync-agent" -c "127.0.0.1:${COORD_PORT}" -i agent-probe -w 4 \
    >"$WORK/agent.log" 2>&1 &
AGENT_PID=$!
sleep 1
curl -sf -H "$AUTH" "$API/api/v1/agents" | grep -q '"connected":true' \
    || fail "agent did not register"

# --- negative: source mount missing → probe blocks the pass -------------------
BADDST="$WORK/dst-bad"
cat > "$WORK/bad.yaml" <<EOF
apiVersion: drsync/v1
kind: Job
metadata:
  name: probe-neg
spec:
  source: { path: $WORK/does-not-exist }
  destination: { path: $BADDST }
  passes: { max: 2 }
EOF
"$DRSYNC" job submit "$WORK/bad.yaml" --start | grep -q "job probe-neg started" \
    || fail "bad-mount job submit --start failed (should be accepted, then probe-blocked)"

# Give the gate time to run the probe and park it.
sleep 4
ST=$(jobstate probe-neg)
[[ "$ST" == '"state":"COMPLETED"' ]] && fail "bad-mount job reached COMPLETED — probe did not gate"
[[ "$ST" == '"state":"RUNNING"' ]] || fail "bad-mount job unexpected state: $ST"

# Pass must still be in PROBING (root walk shard never seeded).
PSTATE=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/probe-neg/passes/1" | grep -o '"state":"[A-Z]*"' | head -1)
[[ "$PSTATE" == '"state":"PROBING"' ]] || fail "pass not held in PROBING (got $PSTATE)"

# A shard is parked with the mount error, and nothing was copied.
"$DRSYNC" report probe-neg --json > "$WORK/neg.json"
python3 - "$WORK/neg.json" <<'EOF' || fail "expected a parked probe shard"
import json, sys
r = json.load(open(sys.argv[1]))
assert r["parked_shard_count"] >= 1, r["parked_shard_count"]
EOF
if [[ -d "$BADDST" ]] && find "$BADDST" -type f | grep -q .; then
    fail "bad-mount job copied files despite the missing source: $(find "$BADDST" -type f)"
fi
# The parked shard's error names the mount problem.
# Capture first, then grep — piping straight into `grep -q` lets grep close the
# pipe on its first match, so `drsync queue` takes SIGPIPE and `set -o pipefail`
# reports the pipeline as failed even though the pattern matched.
QUEUE_OUT=$("$DRSYNC" queue 2>/dev/null || true)
grep -qi "root\|mount\|source" <<<"$QUEUE_OUT" \
    || fail "parked probe error does not mention the mount problem: $QUEUE_OUT"
echo "negative case OK: bad mount blocked at probe (pass held in PROBING, nothing copied)"

# --- stub mount: a real directory that is not on a mounted filesystem ----------
# probe.require_mount (default true) must park a root whose only covering mount
# is "/" — the tell-tale of an unmounted volume's leftover stub. This only holds
# when the work dir really lives on the root filesystem; if the test host put
# $WORK on its own sub-mount there is nothing to detect, so skip the assertion.
if [[ "$(stat -c '%m' "$WORK")" == "/" ]]; then
    SRCSTUB="$WORK/src-stub"; mkdir -p "$SRCSTUB"; echo x > "$SRCSTUB/f.txt"
    cat > "$WORK/stub.yaml" <<EOF
apiVersion: drsync/v1
kind: Job
metadata:
  name: probe-stub
spec:
  source: { path: $SRCSTUB }
  destination: { path: $WORK/dst-stub }
  passes: { max: 2 }
EOF
    "$DRSYNC" job submit "$WORK/stub.yaml" --start | grep -q "job probe-stub started" \
        || fail "stub-mount job submit --start failed"
    sleep 4
    ST=$(jobstate probe-stub)
    [[ "$ST" == '"state":"COMPLETED"' ]] && fail "stub-mount job reached COMPLETED — require_mount did not gate"
    PSTATE=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/probe-stub/passes/1" | grep -o '"state":"[A-Z]*"' | head -1)
    [[ "$PSTATE" == '"state":"PROBING"' ]] || fail "stub-mount pass not held in PROBING (got $PSTATE)"
    QUEUE_OUT=$("$DRSYNC" queue 2>/dev/null || true)
    grep -qi "mounted filesystem\|stub\|mount" <<<"$QUEUE_OUT" \
        || fail "stub-mount parked error does not name the mount problem: $QUEUE_OUT"
    echo "stub case OK: unmounted-stub root (covered only by /) blocked at probe"
else
    echo "stub case SKIPPED: \$WORK is on a sub-mount, no rootfs stub to detect"
fi

# --- positive: healthy job passes the probe and converges ---------------------
SRC="$WORK/src" DST="$WORK/dst"
mkdir -p "$SRC/sub"
echo hello > "$SRC/a.txt"; echo world > "$SRC/sub/b.txt"
cat > "$WORK/ok.yaml" <<EOF
apiVersion: drsync/v1
kind: Job
metadata:
  name: probe-ok
spec:
  source: { path: $SRC }
  destination: { path: $DST }
  probe: { require_mount: false }   # test roots are plain dirs, not real mounts
  passes: { max: 3, converge_when: { delta_files_below: 1 } }
  verify: { checksum: { sample_rate: 1.0 } }
EOF
"$DRSYNC" job submit "$WORK/ok.yaml" --start | grep -q "job probe-ok started" \
    || fail "healthy job submit failed"
for _ in $(seq 1 120); do
    [[ "$(jobstate probe-ok)" == '"state":"COMPLETED"' ]] && break; sleep 0.5
done
[[ "$(jobstate probe-ok)" == '"state":"COMPLETED"' ]] || {
    tail -8 "$WORK/agent.log" "$WORK/coord.log"; fail "healthy job did not converge"
}
cmp -s "$SRC/a.txt" "$DST/a.txt" && cmp -s "$SRC/sub/b.txt" "$DST/sub/b.txt" \
    || fail "healthy job content mismatch"
# The gate actually ran: the coordinator logged the phase transition.
grep -q "PROBING → SCANNING" "$WORK/coord.log" \
    || fail "healthy job did not pass through the PROBING gate"
echo "positive case OK: healthy job passed the probe gate and converged"

echo "PASS: probe gate blocks missing mounts and admits healthy ones"
PASS=1
