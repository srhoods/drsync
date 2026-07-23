#!/usr/bin/env bash
# drsync temp reclaim: the orphan sweep must reclaim crash residue but must NOT
# touch a temp tagged with the pass it is currently running.
#
# This is the "open temp for finalize" regression. A chunk temp sits in the
# destination directory, with no source counterpart, for the whole multi-host
# copy — indistinguishable from residue to the sweep, which unlinks
# prefix-matching destination orphans. The directory can legitimately be
# re-walked while the chunk group runs (the parent walk shard is requeued after
# a lease lapse or a journal-ack timeout, and the coordinator deliberately keeps
# the group it already fanned out rather than re-fanning it). Reclaiming the
# temp then failed the finalize chunk on ENOENT — or, when the unlink landed
# mid-group, let the remaining chunks recreate the temp with O_CREAT so finalize
# renamed a hole-ridden file into place under a correct-looking size and mtime.
#
# Racing a real re-walk against a real chunk group is timing-dependent, so this
# drives the decision directly: seed the destination with temps that are (a)
# tagged for the running job+pass and (b) not, and assert only the latter are
# reclaimed. Pass 1 of job id 1 gives a predictable "1-1" tag.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
. "$ROOT/test/lib.sh"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/drsync-tmpreclaim.XXXXXX")
# Ports come from the kernel (test/lib.sh), not a hardcoded pair: fixed ports
# collide with anything already listening — including another checkout's
# coordinator — and several of these scripts used to share the same pair, so
# they could not run side by side. Override with CP/HP to pin them.
read -r _CP _HP < <(pick_ports)
CP=${CP:-$_CP}; HP=${HP:-$_HP}
API="http://127.0.0.1:${HP}"; AUTH="Authorization: Bearer tmptok"
PASS=0
cleanup() {
    [[ -n "${APID:-}" ]] && kill "$APID" 2>/dev/null || true
    [[ -n "${CPID:-}" ]] && kill "$CPID" 2>/dev/null || true
    wait 2>/dev/null || true
    if [[ $PASS -eq 1 ]]; then rm -rf "$WORK"; else echo "work dir kept: $WORK"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }
export DRSYNC_SERVER="$API" DRSYNC_TOKEN=tmptok

# The coordinator reads its bearer token from a file (never a raw CLI
# value); the file must be 0600 or drsyncd refuses to start.
API_TOKEN_FILE="$WORK/api-token"
echo -n tmptok >"$API_TOKEN_FILE"
chmod 600 "$API_TOKEN_FILE"
DRSYNC="$ROOT/bin/drsync"

make -C "$ROOT/agent" -s
( cd "$ROOT" && go build -o bin/drsyncd ./coordinator/cmd/drsyncd \
             && go build -o bin/drsync ./cli/drsync )

SRC="$WORK/src"; DST="$WORK/dst"
mkdir -p "$SRC/d" "$DST/d"
echo hello > "$SRC/d/real.txt"
head -c 1048576 /dev/urandom > "$SRC/d/mid.bin"

# Seed the destination directory the walk will sweep.
#   LIVE  — tagged job 1 / pass 1: what a chunk group assembling right now looks
#           like. Must survive the pass that owns it.
#   STALE — same job, earlier pass: real residue, must be reclaimed.
#   OTHER — another job's temp: not this pass's work, reclaimed as before.
#   LEGACY— pre-tag name from an older build: still reclaimable.
LIVE="$DST/d/.drsync.tmp.1-1.deadbeef.0"
STALE="$DST/d/.drsync.tmp.1-0.deadbeef.0"
OTHER="$DST/d/.drsync.tmp.7-1.deadbeef.0"
LEGACY="$DST/d/.drsync.tmp.deadbeef.0"
for f in "$LIVE" "$STALE" "$OTHER" "$LEGACY"; do echo residue > "$f"; done

"$ROOT/bin/drsyncd" -data-dir "$WORK/coord" -listen-agent 127.0.0.1:$CP \
    -listen-http 127.0.0.1:$HP -api-token-file "$API_TOKEN_FILE" -log-level warn \
    >"$WORK/coord.log" 2>&1 &
CPID=$!
wait_coordinator "$API" "$AUTH" || exit 1

"$ROOT/agent/bin/drsync-agent" -c 127.0.0.1:$CP -i tr-a -w 2 -C 4 \
    >"$WORK/tr-a.log" 2>&1 &
APID=$!
for _ in $(seq 1 40); do
    n=$(curl -sf -H "$AUTH" "$API/api/v1/agents" | { grep -o '"connected":true' || true; } | wc -l)
    [[ "$n" -eq 1 ]] && break; sleep 0.25
done
[[ "${n:-0}" -eq 1 ]] || fail "agent did not connect"

# max: 1 pins the assertion to the pass that owns the LIVE tag — a second pass
# would (correctly) reclaim it as residue of a finished pass.
cat > "$WORK/job.yaml" <<EOF
apiVersion: drsync/v1
kind: Job
metadata: { name: tmpreclaim }
spec:
  source: { path: $SRC }
  destination: { path: $DST }
  probe: { require_mount: false }   # test roots are plain dirs, not mounts
  passes: { max: 1 }
  copy: { server_side_copy: off }
  verify: { mode: off }
EOF
"$DRSYNC" job submit "$WORK/job.yaml" --start >/dev/null || fail "submit failed"

STATE=""
for _ in $(seq 1 240); do
    STATE=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/tmpreclaim" \
        | grep -o '"state":"[A-Z]*"' | head -1)
    [[ "$STATE" == '"state":"COMPLETED"' ]] && break
    sleep 0.5
done
[[ "$STATE" == '"state":"COMPLETED"' ]] || {
    tail -8 "$WORK"/coord.log; fail "pass did not complete (state=$STATE)"; }

# The pass copied the real files.
[[ -f "$DST/d/real.txt" && -f "$DST/d/mid.bin" ]] || fail "pass did not copy the source files"

# The live temp survived its own pass. Without the (job, pass) tag the sweep
# cannot tell it from residue and deletes it — which is exactly what breaks a
# chunk group mid-assembly.
[[ -f "$LIVE" ]] || fail "live temp $(basename "$LIVE") was reclaimed by the pass that owns it \
— a chunk group assembling here would fail its finalize with 'open temp for finalize'"

# Everything not tagged for this pass is still reclaimed as before.
for f in "$STALE" "$OTHER" "$LEGACY"; do
    [[ -e "$f" ]] && fail "residue $(basename "$f") was not reclaimed"
done

echo "PASS: pass-tagged temp survived its own pass; stale/foreign/legacy residue reclaimed"
PASS=1
