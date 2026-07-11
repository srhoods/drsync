#!/usr/bin/env bash
# drsync scale e2e: the two "pathological shape" task types.
#  - entry-list: a directory whose source entry count exceeds
#    dir_split_threshold is fanned out as EntryListShard slices instead of being
#    walked in one shard. Asserted via the coordinator's recorded shard kinds.
#  - chunked copy: a file at/above chunk_threshold on the byte-copy path
#    (server_side_copy off) is copied in parallel ranges into one temp, then
#    finalized. Asserted via the agent's "chunked copy" log + a byte-exact hash.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d "${TMPDIR:-/tmp}/drsync-scale.XXXXXX")
CP=${CP:-17580}; HP=${HP:-17581}
API="http://127.0.0.1:${HP}"; AUTH="Authorization: Bearer scaletok"
PASS=0
cleanup() {
    for p in "${APID:-}" "${CPID:-}"; do [[ -n "$p" ]] && kill "$p" 2>/dev/null || true; done
    wait 2>/dev/null || true
    if [[ $PASS -eq 1 ]]; then rm -rf "$WORK"; else echo "work dir kept: $WORK"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }
export DRSYNC_SERVER="$API" DRSYNC_TOKEN=scaletok
DRSYNC="$ROOT/bin/drsync"

# --- build -------------------------------------------------------------------
make -C "$ROOT/agent" -s
( cd "$ROOT" && go build -o bin/drsyncd ./coordinator/cmd/drsyncd \
             && go build -o bin/drsync ./cli/drsync )

# --- source tree: one pathological dir + one huge file -----------------------
SRC="$WORK/src"; DST="$WORK/dst"
mkdir -p "$SRC/bigdir/sub" "$DST/bigdir"
for i in $(seq 1 200); do echo "entry $i" > "$SRC/bigdir/f$(printf %04d "$i").txt"; done
echo nested > "$SRC/bigdir/sub/deep.txt"     # subdir inside the split dir
echo stale  > "$DST/bigdir/f0001.txt"        # must be replaced
echo orphan > "$DST/bigdir/zzz-orphan.txt"   # dst-only → orphan (report-only)
head -c 629145600 /dev/urandom > "$SRC/huge.bin"   # 600 MiB → 3 ranges
HUGE_SUM=$(sha256sum "$SRC/huge.bin" | cut -d' ' -f1)

# --- services ----------------------------------------------------------------
"$ROOT/bin/drsyncd" -data-dir "$WORK/coord" -listen-agent 127.0.0.1:$CP \
    -listen-http 127.0.0.1:$HP -api-token scaletok -log-level warn \
    >"$WORK/coord.log" 2>&1 &
CPID=$!
for _ in $(seq 1 40); do curl -sf "$API/healthz" >/dev/null 2>&1 && break; sleep 0.25; done
curl -sf "$API/healthz" >/dev/null || fail "coordinator did not come up"
"$ROOT/agent/bin/drsync-agent" -c 127.0.0.1:$CP -i scale-agent -w 4 -C 8 \
    >"$WORK/agent.log" 2>&1 &
APID=$!
sleep 1

cat > "$WORK/job.yaml" <<EOF
apiVersion: drsync/v1
kind: Job
metadata: { name: scale }
spec:
  source: { path: $SRC }
  destination: { path: $DST }
  passes: { max: 4, converge_when: { delta_files_below: 1 } }
  copy: { server_side_copy: off, chunk_threshold: 1MiB }
  verify: { checksum: { sample_rate: 1.0 } }
  tuning: { dir_split_threshold: 25, shard_budget: 100000 }
EOF
"$DRSYNC" job submit "$WORK/job.yaml" --start | grep -q "job scale started" \
    || fail "submit failed"

STATE=""
for _ in $(seq 1 180); do
    STATE=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/scale" | grep -o '"state":"[A-Z]*"' | head -1)
    [[ "$STATE" == '"state":"COMPLETED"' ]] && break
    sleep 0.5
done
[[ "$STATE" == '"state":"COMPLETED"' ]] || { tail -8 "$WORK"/agent.log "$WORK"/coord.log; fail "did not converge (state=$STATE)"; }

# 1. entry-list path fired: the coordinator recorded entrylist shards
DB=$(ls "$WORK/coord"/*.db 2>/dev/null | head -1)
NEL=$(python3 - "$DB" <<'PY'
import sqlite3, sys
c = sqlite3.connect(sys.argv[1])
print(c.execute("select count(*) from shards where kind='entrylist'").fetchone()[0])
PY
)
[[ "${NEL:-0}" -ge 1 ]] || fail "no entrylist shards recorded (entry-list path not taken)"

# 2. chunked-copy path fired for the huge file
grep -q "chunked copy: huge.bin" "$WORK/agent.log" \
    || { grep -i chunk "$WORK/agent.log" | head; fail "huge.bin was not chunk-copied"; }

# 3. content fidelity: byte-exact huge file + full tree match (orphan aside)
[[ "$(sha256sum "$DST/huge.bin" | cut -d' ' -f1)" == "$HUGE_SUM" ]] \
    || fail "huge.bin content mismatch after chunked copy"
DIFF=$(diff -r "$SRC" "$DST" 2>&1 | grep -v "zzz-orphan" || true)
[[ -z "$DIFF" ]] || fail "content mismatch:"$'\n'"$DIFF"
[[ -f "$DST/bigdir/zzz-orphan.txt" ]] || fail "orphan deleted (violates D5)"
[[ -f "$DST/bigdir/sub/deep.txt" ]] || fail "subdir inside split dir not synced"

# 4. verify pass re-read the chunked file (hash journaled as 0) with no failures
curl -sf -H "$AUTH" "$API/api/v1/jobs/scale" | grep -q '"verify_fail":[1-9]' \
    && fail "verify failures reported"

echo "entrylist shards: $NEL; huge.bin chunk-copied; content byte-exact; verify clean"
PASS=1
echo "PASS: entry-list + chunked-copy task types OK"
