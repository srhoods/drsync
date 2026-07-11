#!/usr/bin/env bash
# drsync mTLS + reconnect-resume e2e:
#  - `drsync ca` mints a CA + server/agent certs
#  - coordinator and agent complete a full sync over mutually-authenticated TLS
#  - a plaintext agent and an agent with a foreign client cert are both rejected
#  - the agent survives a coordinator bounce: it auto-reconnects (new fleet
#    epoch) and a follow-up job still converges — no manual restart.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d "${TMPDIR:-/tmp}/drsync-tls.XXXXXX")
COORD_PORT=${COORD_PORT:-17560}
HTTP_PORT=${HTTP_PORT:-17561}
API="http://127.0.0.1:${HTTP_PORT}"
AUTH="Authorization: Bearer tlstoken"
PASS=0

cleanup() {
    for p in "${ROGUE_PID:-}" "${PLAIN_PID:-}" "${AGENT_PID:-}" "${COORD_PID:-}"; do
        [[ -n "$p" ]] && kill "$p" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    if [[ $PASS -eq 1 ]]; then rm -rf "$WORK"; else echo "work dir kept: $WORK"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }

DRSYNC="$ROOT/bin/drsync"
export DRSYNC_SERVER="$API" DRSYNC_TOKEN=tlstoken

start_coord() {
    "$ROOT/bin/drsyncd" -data-dir "$WORK/coord" \
        -listen-agent "127.0.0.1:${COORD_PORT}" -listen-http "127.0.0.1:${HTTP_PORT}" \
        -api-token tlstoken -log-level warn \
        -tls-cert "$PKI/coordinator.crt" -tls-key "$PKI/coordinator.key" \
        -tls-ca "$PKI/ca.crt" >>"$WORK/coord.log" 2>&1 &
    COORD_PID=$!
    for _ in $(seq 1 40); do
        curl -sf "$API/healthz" >/dev/null 2>&1 && return 0; sleep 0.25
    done
    fail "coordinator did not come up"
}

agent_connected() {
    curl -sf -H "$AUTH" "$API/api/v1/agents" 2>/dev/null | grep -q '"connected":true'
}

# --- build -------------------------------------------------------------------
make -C "$ROOT/agent" -s
( cd "$ROOT" && go build -o bin/drsyncd ./coordinator/cmd/drsyncd \
             && go build -o bin/drsync ./cli/drsync )

# --- PKI via `drsync ca` -----------------------------------------------------
PKI="$WORK/pki"; PKI2="$WORK/pki-foreign"
mkdir -p "$PKI" "$PKI2"
"$DRSYNC" ca init --dir "$PKI" --cn drsync-test-ca >/dev/null
"$DRSYNC" ca issue --dir "$PKI" --type server --cn coordinator --ip 127.0.0.1 --dns localhost >/dev/null
"$DRSYNC" ca issue --dir "$PKI" --type agent --cn agent-tls >/dev/null
# a second, untrusted CA that mints a rogue agent cert
"$DRSYNC" ca init --dir "$PKI2" --cn rogue-ca >/dev/null
"$DRSYNC" ca issue --dir "$PKI2" --type agent --cn rogue >/dev/null
[[ -s "$PKI/ca.crt" && -s "$PKI/coordinator.crt" && -s "$PKI/agent-tls.key" ]] \
    || fail "ca did not produce the expected material"

# --- source tree -------------------------------------------------------------
SRC="$WORK/src"; DST="$WORK/dst"
mkdir -p "$SRC"/a/b/c "$SRC"/d
for i in $(seq 1 30); do head -c 2048 /dev/urandom > "$SRC/a/f$i.bin"; done
echo hi > "$SRC/a/b/c/leaf.txt"; echo world > "$SRC/d/w.txt"
ln -s ../d/w.txt "$SRC/a/link"

# --- start coordinator (mTLS) ------------------------------------------------
start_coord

# --- negative: plaintext agent is refused ------------------------------------
"$ROOT/agent/bin/drsync-agent" -c "127.0.0.1:${COORD_PORT}" -i plain-agent -w 2 \
    >"$WORK/plain.log" 2>&1 &
PLAIN_PID=$!
sleep 2
agent_connected && fail "plaintext agent was accepted by the TLS coordinator"
kill "$PLAIN_PID" 2>/dev/null || true; PLAIN_PID=

# --- negative: agent with a foreign client cert is refused -------------------
"$ROOT/agent/bin/drsync-agent" -c "127.0.0.1:${COORD_PORT}" -i rogue-agent -w 2 \
    -A "$PKI/ca.crt" -E "$PKI2/rogue.crt" -K "$PKI2/rogue.key" \
    >"$WORK/rogue.log" 2>&1 &
ROGUE_PID=$!
sleep 2
agent_connected && fail "agent with untrusted client cert was accepted"
kill "$ROGUE_PID" 2>/dev/null || true; ROGUE_PID=

# --- the real mTLS agent registers -------------------------------------------
"$ROOT/agent/bin/drsync-agent" -c "127.0.0.1:${COORD_PORT}" -i agent-tls -w 4 \
    -A "$PKI/ca.crt" -E "$PKI/agent-tls.crt" -K "$PKI/agent-tls.key" \
    >"$WORK/agent.log" 2>&1 &
AGENT_PID=$!
for _ in $(seq 1 20); do agent_connected && break; sleep 0.25; done
agent_connected || { tail -5 "$WORK/agent.log" "$WORK/coord.log"; fail "mTLS agent did not register"; }
echo "ok: mTLS agent connected; plaintext + foreign-cert agents rejected"

# --- submit + run a job over mTLS --------------------------------------------
submit_and_wait() {
    local name=$1
    cat > "$WORK/$name.yaml" <<EOF
apiVersion: drsync/v1
kind: Job
metadata:
  name: $name
spec:
  source: { path: $SRC }
  destination: { path: $DST }
  passes: { max: 4, converge_when: { delta_files_below: 1 } }
EOF
    "$DRSYNC" job submit "$WORK/$name.yaml" --set spec.tuning.shard_budget=4 --start \
        | grep -q "job $name started" || fail "submit $name failed"
    local state=""
    for _ in $(seq 1 120); do
        state=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/$name" | grep -o '"state":"[A-Z]*"' | head -1)
        [[ "$state" == '"state":"COMPLETED"' ]] && return 0
        sleep 0.5
    done
    tail -8 "$WORK/agent.log" "$WORK/coord.log"
    fail "$name did not converge (state=$state)"
}

submit_and_wait job1
DIFF=$(diff -r "$SRC" "$DST" 2>&1 | grep -v "^Only in $DST" || true)
[[ -z "$DIFF" ]] || fail "content mismatch after mTLS sync:"$'\n'"$DIFF"
echo "ok: job1 converged over mTLS, content matches"

# --- reconnect-resume: bounce the coordinator --------------------------------
kill "$COORD_PID" 2>/dev/null || true; wait "$COORD_PID" 2>/dev/null || true; COORD_PID=
[[ -n "$AGENT_PID" ]] && kill -0 "$AGENT_PID" 2>/dev/null \
    || fail "agent exited when the coordinator dropped (should reconnect)"
sleep 1
start_coord   # fresh process → new random fleet epoch

# the same agent process must re-register on its own
for _ in $(seq 1 40); do agent_connected && break; sleep 0.25; done
agent_connected || { tail -10 "$WORK/agent.log"; fail "agent did not reconnect after coordinator bounce"; }
grep -q "reconnecting to" "$WORK/agent.log" || fail "agent log shows no reconnect attempt"
grep -q "coordinator restarted (fleet epoch" "$WORK/agent.log" \
    || fail "agent did not observe the fleet-epoch bump"
echo "ok: agent auto-reconnected after coordinator restart (same pid $AGENT_PID)"

# a follow-up job proves the resumed session actually does work
rm -rf "$DST"; mkdir -p "$DST"
submit_and_wait job2
DIFF=$(diff -r "$SRC" "$DST" 2>&1 | grep -v "^Only in $DST" || true)
[[ -z "$DIFF" ]] || fail "content mismatch after reconnect:"$'\n'"$DIFF"

PASS=1
echo "PASS: mTLS handshake + auth enforcement + reconnect-resume all OK"
