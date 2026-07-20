#!/usr/bin/env bash
# drsync direct-write e2e: copy.direct_write copies a NEW file straight to its
# final name (no temp + rename), which halves the per-file metadata cost on a
# filesystem that serializes directory operations. The atomic temp+rename must
# still be used for a file that already EXISTS at the destination — overwriting
# a live file in place would corrupt it on a crash. This asserts:
#   - a new file lands, byte-exact, with no .drsync.tmp residue;
#   - a changed pre-existing file is still updated correctly (temp+rename path);
#   - the whole tree converges and the verify pass (full checksum) is clean.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
. "$ROOT/test/lib.sh"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/drsync-directwrite.XXXXXX")
read -r _CP _HP < <(pick_ports)
COORD_PORT=${COORD_PORT:-$_CP}
HTTP_PORT=${HTTP_PORT:-$_HP}
API="http://127.0.0.1:${HTTP_PORT}"
AUTH="Authorization: Bearer dwtoken"
PASS=0

cleanup() {
    [[ -n "${AGENT_PID:-}" ]] && kill "$AGENT_PID" 2>/dev/null || true
    [[ -n "${COORD_PID:-}" ]] && kill "$COORD_PID" 2>/dev/null || true
    wait 2>/dev/null || true
    if [[ $PASS -eq 1 ]]; then rm -rf "$WORK"; else echo "work dir kept: $WORK"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }
export DRSYNC_SERVER="$API" DRSYNC_TOKEN=dwtoken
DRSYNC="$ROOT/bin/drsync"

# --- build -------------------------------------------------------------------
make -C "$ROOT/agent" -s
( cd "$ROOT" && go build -o bin/drsyncd ./coordinator/cmd/drsyncd \
             && go build -o bin/drsync ./cli/drsync )

# --- source tree + a pre-existing destination file ---------------------------
SRC="$WORK/src"; DST="$WORK/dst"
mkdir -p "$SRC/sub" "$DST"
# many new files, to exercise the direct path in bulk
for i in $(seq 1 200); do echo "new file $i" > "$SRC/f$(printf %04d "$i").txt"; done
echo nested > "$SRC/sub/deep.txt"
# a file that already exists at the destination and has genuinely changed:
# longer content and an older dst mtime, so the diff copies it — and because it
# EXISTS, direct-write must NOT apply; the atomic temp+rename must.
printf 'a genuinely longer NEW version of this file\n' > "$SRC/updated.txt"
printf 'old\n' > "$DST/updated.txt"; touch -d '2020-01-01' "$DST/updated.txt"
# distinctive xattr + mode on one new file, to check metadata on the direct path
setfattr -n user.dwtag -v direct "$SRC/f0001.txt"
chmod 0640 "$SRC/f0001.txt"

# --- services ----------------------------------------------------------------
"$ROOT/bin/drsyncd" -data-dir "$WORK/coord" -listen-agent 127.0.0.1:$COORD_PORT \
    -listen-http 127.0.0.1:$HTTP_PORT -api-token dwtoken -log-level warn \
    >"$WORK/coord.log" 2>&1 &
COORD_PID=$!
wait_coordinator "$API" "$AUTH" || exit 1
"$ROOT/bin/drsync-agent" -c 127.0.0.1:$COORD_PORT -i dw-agent -w 4 -C 8 \
    >"$WORK/agent.log" 2>&1 &
AGENT_PID=$!
sleep 1

cat > "$WORK/job.yaml" <<EOF
apiVersion: drsync/v1
kind: Job
metadata: { name: dw }
spec:
  source: { path: $SRC }
  destination: { path: $DST }
  passes: { max: 2, converge_when: { delta_files_below: 1 } }
  copy: { fsync: off, direct_write: true }
  verify: { checksum: { sample_rate: 1.0 } }
EOF
"$DRSYNC" job submit "$WORK/job.yaml" --start | grep -q "job dw started" \
    || fail "submit failed"

STATE=""
for _ in $(seq 1 400); do
    STATE=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/dw" | grep -o '"state":"[A-Z]*"' | head -1)
    [[ "$STATE" == '"state":"COMPLETED"' || "$STATE" == '"state":"FAILED"' ]] && break
    sleep 0.5
done
[[ "$STATE" == '"state":"COMPLETED"' ]] || {
    echo "state=$STATE"; tail -n 12 "$WORK/agent.log" "$WORK/coord.log"
    fail "did not converge (state=$STATE)"; }

# 1. verify pass (full checksum) found no content mismatch — proves both the
#    direct-written new files and the temp+renamed update are byte-exact.
curl -sf -H "$AUTH" "$API/api/v1/jobs/dw" | grep -q '"verify_fail":[1-9]' \
    && fail "verify failures reported"

# 2. whole tree converged, byte for byte.
DIFF=$(diff -r "$SRC" "$DST" 2>&1 || true)
[[ -z "$DIFF" ]] || fail "content mismatch:"$'\n'"$DIFF"

# 3. the pre-existing file was updated to the source content (temp+rename path).
[[ "$(cat "$DST/updated.txt")" == "$(cat "$SRC/updated.txt")" ]] \
    || fail "pre-existing file not updated"

# 4. no temp residue anywhere — the direct path leaves none, and the temp path
#    renamed its temp away.
LEFT=$(find "$DST" -name '.drsync.tmp.*')
[[ -z "$LEFT" ]] || fail "temp residue left: $LEFT"

# 5. metadata on a direct-written file converged: mode + xattr.
GM=$(stat -c '%a' "$DST/f0001.txt")
[[ "$GM" == "640" ]] || fail "direct-written file mode wrong: got $GM"
GX=$(getfattr -n user.dwtag --only-values "$DST/f0001.txt" 2>/dev/null || true)
[[ "$GX" == "direct" ]] || fail "direct-written file xattr not applied (got '$GX')"

echo "PASS: direct_write copied new files with no temp, updated a pre-existing file atomically, verify clean"
PASS=1
