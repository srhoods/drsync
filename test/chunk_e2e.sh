#!/usr/bin/env bash
# drsync cross-host chunk fan-out e2e: a large file is copied by ranges spread
# across the fleet, not by the one agent that walked it.
#
# A file at/above chunk_threshold and larger than one chunk is proposed to the
# coordinator (ShardSplit.big_files), which lays it out as ChunkTask shards. The
# data chunks land ranges into one shared temp on different hosts; the finalize
# task fsyncs, applies metadata, and renames it into place. Asserted via the
# coordinator's record of which agent ran each chunk shard, a byte-exact hash,
# metadata fidelity, and a clean second pass.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
. "$ROOT/test/lib.sh"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/drsync-chunk.XXXXXX")
# Ports come from the kernel (test/lib.sh), not a hardcoded pair: fixed ports
# collide with anything already listening — including another checkout's
# coordinator — and several of these scripts used to share the same pair, so
# they could not run side by side. Override with CP/HP to pin them.
read -r _CP _HP < <(pick_ports)
CP=${CP:-$_CP}; HP=${HP:-$_HP}
API="http://127.0.0.1:${HP}"; AUTH="Authorization: Bearer chunktok"
AGENTS=(chunk-a chunk-b chunk-c)
PASS=0
cleanup() {
    for p in ${APIDS:-}; do kill "$p" 2>/dev/null || true; done
    [[ -n "${CPID:-}" ]] && kill "$CPID" 2>/dev/null || true
    wait 2>/dev/null || true
    if [[ $PASS -eq 1 ]]; then rm -rf "$WORK"; else echo "work dir kept: $WORK"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }
export DRSYNC_SERVER="$API" DRSYNC_TOKEN=chunktok
DRSYNC="$ROOT/bin/drsync"

# --- build -------------------------------------------------------------------
make -C "$ROOT/agent" -s
( cd "$ROOT" && go build -o bin/drsyncd ./coordinator/cmd/drsyncd \
             && go build -o bin/drsync ./cli/drsync )

# --- source: one big file (+ a small one so not every shard is a chunk) ------
SRC="$WORK/src"; DST="$WORK/dst"
mkdir -p "$SRC" "$DST"
# 40 MiB with chunk_size 4 MiB → 10 data chunks + 1 finalize, plenty to spread.
head -c 41943040 /dev/urandom > "$SRC/huge.bin"
echo "small" > "$SRC/note.txt"
# Distinctive metadata that must survive finalize (set last).
chmod 0640 "$SRC/huge.bin"
touch -d '2021-03-04 05:06:07' "$SRC/huge.bin"
setfattr -n user.tag -v chunked "$SRC/huge.bin"
HUGE_SUM=$(sha256sum "$SRC/huge.bin" | cut -d' ' -f1)
H_MODE=$(stat -c '%a' "$SRC/huge.bin"); H_MTIME=$(stat -c '%Y' "$SRC/huge.bin")

# --- coordinator + 3-agent fleet ---------------------------------------------
"$ROOT/bin/drsyncd" -data-dir "$WORK/coord" -listen-agent 127.0.0.1:$CP \
    -listen-http 127.0.0.1:$HP -api-token chunktok -log-level warn \
    >"$WORK/coord.log" 2>&1 &
CPID=$!
wait_coordinator "$API" "$AUTH" || exit 1
APIDS=""
for a in "${AGENTS[@]}"; do
    "$ROOT/agent/bin/drsync-agent" -c 127.0.0.1:$CP -i "$a" -w 2 -C 4 \
        >"$WORK/$a.log" 2>&1 &
    APIDS="$APIDS $!"
done
for _ in $(seq 1 40); do
    # grep -o|wc counts OCCURRENCES (the payload is a single JSON line, so
    # grep -c would cap at 1). The `|| true` keeps a no-match grep — normal
    # while agents are still connecting — from aborting the script via
    # pipefail+set -e instead of retrying; same trap e2e.sh documents on has().
    n=$(curl -sf -H "$AUTH" "$API/api/v1/agents" | { grep -o '"connected":true' || true; } | wc -l)
    [[ "$n" -eq 3 ]] && break; sleep 0.25
done
[[ "${n:-0}" -eq 3 ]] || fail "expected 3 connected agents, got ${n:-0}"

# --- job: force chunk fan-out with a small chunk_size ------------------------
cat > "$WORK/job.yaml" <<EOF
apiVersion: drsync/v1
kind: Job
metadata: { name: chunk }
spec:
  source: { path: $SRC }
  destination: { path: $DST }
  passes: { max: 2, converge_when: { delta_files_below: 1 } }
  copy: { server_side_copy: off, chunk_threshold: 1MiB, chunk_size: 4MiB }
  verify: { checksum: { sample_rate: 1.0 } }
EOF
"$DRSYNC" job submit "$WORK/job.yaml" --start >/dev/null || fail "submit failed"
STATE=""
for _ in $(seq 1 240); do
    STATE=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/chunk" | grep -o '"state":"[A-Z]*"' | head -1)
    [[ "$STATE" == '"state":"COMPLETED"' ]] && break
    sleep 0.5
done
[[ "$STATE" == '"state":"COMPLETED"' ]] || {
    tail -8 "$WORK"/coord.log "$WORK"/chunk-*.log; fail "did not converge (state=$STATE)"; }

# --- 1. chunks ran, spread across more than one agent ------------------------
read -r NCHUNK NAGENTS < <(python3 - "$WORK/coord/state.db" <<'PY'
import sqlite3, sys
con = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
rows = con.execute("""
    SELECT s.lease_agent, COUNT(*) FROM shards s
    JOIN passes p ON p.id = s.pass_id JOIN jobs j ON j.id = p.job_id
    WHERE j.name='chunk' AND p.pass_no=1 AND s.kind='chunk' AND s.lease_agent IS NOT NULL
    GROUP BY s.lease_agent""").fetchall()
total = sum(n for _, n in rows)
print(total, len(rows), file=sys.stderr)
print(total, len(rows))
PY
)
[[ "${NCHUNK:-0}" -ge 11 ]] || fail "expected >=11 chunk shards (10 data + finalize), got ${NCHUNK:-0}"
[[ "${NAGENTS:-0}" -ge 2 ]] || fail "chunks did not spread: ran on ${NAGENTS:-0} agent(s)"

# --- 2. the file is byte-exact and metadata survived finalize ----------------
[[ "$(sha256sum "$DST/huge.bin" | cut -d' ' -f1)" == "$HUGE_SUM" ]] \
    || fail "huge.bin content mismatch after chunked copy"
d_mode=$(stat -c '%a' "$DST/huge.bin"); d_mtime=$(stat -c '%Y' "$DST/huge.bin")
[[ "$d_mode" == "$H_MODE" ]] || fail "mode not preserved: src=$H_MODE dst=$d_mode"
[[ "$d_mtime" == "$H_MTIME" ]] || fail "mtime not preserved: src=$H_MTIME dst=$d_mtime"
[[ "$(getfattr -n user.tag --only-values "$DST/huge.bin" 2>/dev/null)" == "chunked" ]] \
    || fail "xattr not preserved through finalize"
# no temp residue left behind
[[ -z "$(find "$DST" -name '.drsync.tmp.*')" ]] || fail "temp residue not cleaned"

# --- 3. convergence: pass 2 re-diffs and copies nothing ----------------------
p2() { curl -sf -H "$AUTH" "$API/api/v1/jobs/chunk/passes/$1" \
       | python3 -c "import json,sys; print(json.load(sys.stdin)['pass']['$2'])"; }
[[ "$(p2 1 files_copied)" -eq 2 ]] || fail "pass 1 copied $(p2 1 files_copied), expected 2"
[[ "$(p2 2 files_copied)" -eq 0 ]] || fail "pass 2 copied $(p2 2 files_copied), expected 0"
[[ "$(p2 2 errors)" -eq 0 ]] || fail "pass 2 reported errors"
[[ "$(p2 1 verify_fail)" -eq 0 ]] || fail "checksum verify failed on the chunked file"

echo "PASS: 40 MiB file → $NCHUNK chunk shards across $NAGENTS agents; byte-exact; meta preserved; converged"
PASS=1
