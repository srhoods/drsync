#!/usr/bin/env bash
# drsync DIRFIX e2e: a directory that fans out into entry-list shards has files
# renamed into it AFTER the walker set its mtime, so its mtime is left at
# copy-time. The DIRFIX phase must re-apply the source mtime once the pass has
# drained. The job is forced to converge in ONE pass (delta_files_below huge),
# so there is no second pass to fix the directory the old way — only DIRFIX can.
# Before DIRFIX was generated, the split directory kept a wrong mtime here.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
. "$ROOT/test/lib.sh"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/drsync-dirfix.XXXXXX")
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

# The coordinator reads its bearer token from a file (never a raw CLI
# value); the file must be 0600 or drsyncd refuses to start.
API_TOKEN_FILE="$WORK/api-token"
echo -n e2etoken >"$API_TOKEN_FILE"
chmod 600 "$API_TOKEN_FILE"

# --- source tree --------------------------------------------------------------
SRC="$WORK/src" DST="$WORK/dst"
# "big": 30 entries → entry-list split at dir_split_threshold=10. Its files are
# renamed in by other shards after the walker set the dir mtime.
mkdir -p "$SRC/big" "$SRC/small"
for i in $(seq 1 30); do echo "f$i" > "$SRC/big/f$i.txt"; done
echo one > "$SRC/small/a.txt"; echo two > "$SRC/small/b.txt"
# Distinctive OLD directory mtimes, set AFTER populating so they are the dirs'
# own mtimes (copy-time would be ~now, decades off).
BIG_MT='2019-06-15 12:00:00'
SMALL_MT='2018-03-04 08:00:00'
touch -d "$BIG_MT"   "$SRC/big"
touch -d "$SMALL_MT" "$SRC/small"

# --- services -----------------------------------------------------------------
"$ROOT/bin/drsyncd" -data-dir "$WORK/coord" \
    -listen-agent "127.0.0.1:${COORD_PORT}" -listen-http "127.0.0.1:${HTTP_PORT}" \
    -api-token-file "$API_TOKEN_FILE" -log-level info >"$WORK/coord.log" 2>&1 &
COORD_PID=$!
wait_coordinator "$API" "$AUTH" || exit 1

"$ROOT/agent/bin/drsync-agent" -c "127.0.0.1:${COORD_PORT}" -i agent-dirfix -w 4 \
    >"$WORK/agent.log" 2>&1 &
AGENT_PID=$!
sleep 1
curl -sf -H "$AUTH" "$API/api/v1/agents" | grep -q '"connected":true' \
    || fail "agent did not register"

# --- submit + run (single pass) ----------------------------------------------
cat > "$WORK/job.yaml" <<EOF
apiVersion: drsync/v1
kind: Job
metadata:
  name: dirfix
spec:
  source: { path: $SRC }
  destination: { path: $DST }
  probe: { require_mount: false }   # test roots are plain dirs, not mounts
  passes:
    max: 3
    converge_when:
      delta_files_below: 1000000   # pass 1's copies are under this → converge in ONE pass
  verify:
    checksum:
      sample_rate: 1.0
EOF
# dir_split_threshold=10 forces "big" (30 entries) onto the entry-list path.
"$DRSYNC" job submit "$WORK/job.yaml" --start \
    --set spec.tuning.dir_split_threshold=10 | grep -q "job dirfix started" \
    || fail "job submit --start failed"

for _ in $(seq 1 120); do
    STATE=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/dirfix" | grep -o '"state":"[A-Z]*"' | head -1)
    [[ "$STATE" == '"state":"COMPLETED"' ]] && break
    sleep 0.5
done
[[ "${STATE:-}" == '"state":"COMPLETED"' ]] || {
    tail -8 "$WORK/agent.log" "$WORK/coord.log"; fail "job did not converge (state=${STATE:-none})"
}
JOB=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/dirfix")

# --- assertions ---------------------------------------------------------------
# 1. converged in a single pass — so no "next pass" could have fixed the dir.
# counts occurrences, not lines; `|| true` stops a no-match grep from
# aborting the script under pipefail + set -e.
NP=$(echo "$JOB" | { grep -o '"pass_no":' || true; } | wc -l)
[[ "$NP" -eq 1 ]] || fail "expected 1 pass, got $NP (the single-pass premise is broken)"

# 2. the split directory's mtime is the SOURCE mtime, not copy-time. This is the
#    regression: only DIRFIX re-applies it after the entry-list renames.
want=$(date -d "$BIG_MT" +%s)
got=$(stat -c '%Y' "$DST/big")
[[ "$got" -eq "$want" ]] \
    || fail "split dir mtime not restored: dst=$got want=$want (copy-time drift = DIRFIX missing)"

# 3. the small (non-split) directory is also correct.
want=$(date -d "$SMALL_MT" +%s)
got=$(stat -c '%Y' "$DST/small")
[[ "$got" -eq "$want" ]] || fail "small dir mtime wrong: dst=$got want=$want"

# 4. content intact and no errors / verify failures.
DIFF=$(diff -r "$SRC" "$DST" 2>&1 || true)
[[ -z "$DIFF" ]] || fail "content mismatch:"$'\n'"$DIFF"
echo "$JOB" | grep -q '"errors":[1-9]' && fail "job reported errors: $JOB"
echo "$JOB" | grep -q '"verify_fail":[1-9]' && fail "verify failures: $JOB"

# 5. the DIRFIX phase actually ran this pass.
grep -q "SCANNING → DIRFIX" "$WORK/coord.log" || fail "DIRFIX phase did not run"

echo "PASS: DIRFIX restored the split directory's mtime in a single converging pass"
PASS=1
