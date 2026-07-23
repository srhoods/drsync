#!/usr/bin/env bash
# drsync fan-out e2e: a volume with few files must still use the whole fleet.
#
# The regression this guards: a shard only pushes subdirectories back to the
# coordinator once it has walked tuning.shard_budget (250k) entries, so a volume
# smaller than that never split at all — the root shard walked the entire tree on
# one thread of one agent while the rest of the cluster idled.
#
# Both runs below use the SAME small tree and the default shard_budget. They
# differ only in tuning.spread_mode, which isolates the fix:
#   spread_mode=off  → the old behaviour: exactly ONE agent walks (the control)
#   spread_mode=auto → the fix: all THREE agents walk
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
. "$ROOT/test/lib.sh"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/drsync-fanout.XXXXXX")
# Ports come from the kernel (test/lib.sh), not a hardcoded pair: fixed ports
# collide with anything already listening — including another checkout's
# coordinator — and several of these scripts used to share the same pair, so
# they could not run side by side. Override to pin them.
read -r _CP _HP < <(pick_ports)
COORD_PORT=${COORD_PORT:-$_CP}
HTTP_PORT=${HTTP_PORT:-$_HP}
API="http://127.0.0.1:${HTTP_PORT}"
AUTH="Authorization: Bearer fanouttoken"
AGENTS=(fan-a fan-b fan-c)
PASS=0

cleanup() {
    for p in ${AGENT_PIDS:-}; do kill "$p" 2>/dev/null || true; done
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
export DRSYNC_SERVER="$API" DRSYNC_TOKEN=fanouttoken

# The coordinator reads its bearer token from a file (never a raw CLI
# value); the file must be 0600 or drsyncd refuses to start.
API_TOKEN_FILE="$WORK/api-token"
echo -n fanouttoken >"$API_TOKEN_FILE"
chmod 600 "$API_TOKEN_FILE"

# --- source tree -------------------------------------------------------------
# ~4800 files over 60 leaf directories: far below shard_budget (250k) and every
# directory far below dir_split_threshold (50k), so NEITHER of the pre-existing
# split triggers can fire. This is exactly the shape that used to pin a whole
# volume to one agent.
SRC="$WORK/src"
for top in $(seq 1 12); do
    for sub in $(seq 1 5); do
        d="$SRC/top$top/sub$sub"
        mkdir -p "$d"
        for f in $(seq 1 80); do echo "top$top/sub$sub/file$f" > "$d/f$f.txt"; done
    done
done
SRC_FILES=$(find "$SRC" -type f | wc -l)
[[ "$SRC_FILES" -gt 4000 ]] || fail "source tree not built (only $SRC_FILES files)"

# --- start coordinator + a 3-agent fleet -------------------------------------
"$ROOT/bin/drsyncd" -data-dir "$WORK/coord" \
    -listen-agent "127.0.0.1:${COORD_PORT}" -listen-http "127.0.0.1:${HTTP_PORT}" \
    -api-token-file "$API_TOKEN_FILE" -log-level warn >"$WORK/coord.log" 2>&1 &
COORD_PID=$!
wait_coordinator "$API" "$AUTH" || exit 1

AGENT_PIDS=""
for a in "${AGENTS[@]}"; do
    "$ROOT/agent/bin/drsync-agent" -c "127.0.0.1:${COORD_PORT}" -i "$a" -w 2 -C 2 \
        >"$WORK/$a.log" 2>&1 &
    AGENT_PIDS="$AGENT_PIDS $!"
done
# All three must be registered BEFORE the job starts: fan-out is sized from the
# fleet the coordinator can see, and a late arrival would make the run flaky.
for _ in $(seq 1 40); do
    # grep -o|wc counts OCCURRENCES (the payload is a single JSON line, so
    # grep -c would cap at 1). The `|| true` keeps a no-match grep — normal
    # while agents are still connecting — from aborting the script via
    # pipefail+set -e instead of retrying; same trap e2e.sh documents on has().
    n=$(curl -sf -H "$AUTH" "$API/api/v1/agents" | { grep -o '"connected":true' || true; } | wc -l)
    [[ "$n" -eq 3 ]] && break; sleep 0.25
done
[[ "${n:-0}" -eq 3 ]] || fail "expected 3 connected agents, got ${n:-0}"

# walk_agents JOB → number of distinct agents that ran a dir/entrylist shard in
# PASS 1. Read from the coordinator's own record: shards.lease_agent survives
# completion, so it is the authoritative account of who did the walking.
#
# Scoped to one pass because each pass seeds its own root shard, and to walk
# kinds because verify/delete tasks fan out regardless — either would mask the
# thing under test.
walk_agents() {
    python3 - "$WORK/coord/state.db" "$1" <<'EOF'
import sqlite3, sys
db, job = sys.argv[1], sys.argv[2]
con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
rows = con.execute("""
    SELECT s.lease_agent, COUNT(*) FROM shards s
    JOIN passes p ON p.id = s.pass_id
    JOIN jobs   j ON j.id = p.job_id
    WHERE j.name = ? AND p.pass_no = 1
      AND s.kind IN ('dir','entrylist') AND s.lease_agent IS NOT NULL
    GROUP BY s.lease_agent ORDER BY 2 DESC""", (job,)).fetchall()
print("   pass-1 walk shards per agent: "
      + (", ".join(f"{a}={n}" for a, n in rows) or "<none>"), file=sys.stderr)
print(len(rows))
EOF
}

run_job() {
    local name=$1 dst=$2 spread=$3
    cat > "$WORK/$name.yaml" <<EOF
apiVersion: drsync/v1
kind: Job
metadata:
  name: $name
spec:
  source: { path: $SRC }
  destination: { path: $dst }
  probe: { require_mount: false }   # test roots are plain dirs, not mounts
  passes:
    max: 2
    converge_when:
      delta_files_below: 1
  verify:
    mode: off
  tuning:
    spread_mode: $spread
EOF
    # shard_budget deliberately left at its 250k default: the whole point is
    # that a small volume must fan out without hand-tuning it.
    "$DRSYNC" job submit "$WORK/$name.yaml" --start >/dev/null \
        || fail "$name: submit failed"
    for _ in $(seq 1 240); do
        STATE=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/$name" | grep -o '"state":"[A-Z]*"' | head -1)
        [[ "$STATE" == '"state":"COMPLETED"' ]] && return 0
        sleep 0.5
    done
    tail -5 "$WORK/coord.log"
    fail "$name: did not converge (state=${STATE:-none})"
}

# --- control: the old behaviour ----------------------------------------------
echo "== control: spread_mode=off (pre-fix behaviour) =="
run_job fanout-off "$WORK/dst-off" off
N_OFF=$(walk_agents fanout-off)
[[ "$N_OFF" -eq 1 ]] || fail "spread off: expected the walk pinned to 1 agent, got $N_OFF"

# --- the fix ------------------------------------------------------------------
echo "== fix: spread_mode=auto =="
run_job fanout-auto "$WORK/dst-auto" auto
N_AUTO=$(walk_agents fanout-auto)
[[ "$N_AUTO" -eq 3 ]] || fail "spread auto: expected all 3 agents walking, got $N_AUTO"

# --- fan-out must not cost correctness ----------------------------------------
# Same tree, both destinations: a fanned-out walk must produce byte-identical
# output to the single-agent one.
diff -r "$SRC" "$WORK/dst-auto" >/dev/null 2>&1 || fail "fanned-out copy differs from source"
diff -r "$WORK/dst-off" "$WORK/dst-auto" >/dev/null 2>&1 \
    || fail "fanned-out destination differs from the single-agent destination"

# pass_field JOB PASS_NO FIELD
pass_field() {
    curl -sf -H "$AUTH" "$API/api/v1/jobs/$1/passes/$2" \
        | python3 -c "import json,sys; print(json.load(sys.stdin)['pass']['$3'])"
}

# Pass 1 must have done the actual work, spread or not: a fan-out that walked
# the tree but copied nothing would satisfy every assertion above.
P1_COPIED=$(pass_field fanout-auto 1 files_copied)
[[ "$P1_COPIED" -eq "$SRC_FILES" ]] \
    || fail "pass 1 copied $P1_COPIED files, expected $SRC_FILES"

# Convergence: pass 2 re-walks and must copy nothing. If fan-out dropped work or
# re-copied files, the delta never reaches zero.
P2_COPIED=$(pass_field fanout-auto 2 files_copied)
[[ "$P2_COPIED" -eq 0 ]] || fail "pass 2 copied $P2_COPIED files; expected 0"
P2_ERRORS=$(pass_field fanout-auto 2 errors)
[[ "$P2_ERRORS" -eq 0 ]] || fail "pass 2 reported $P2_ERRORS errors"

echo "PASS: ${SRC_FILES} files — pinned to $N_OFF agent with spread off, spread across $N_AUTO with spread auto"
PASS=1
