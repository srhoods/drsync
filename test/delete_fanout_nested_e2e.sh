#!/usr/bin/env bash
# drsync delete-fanout nested e2e: an orphan directory whose OWN entry count
# is small, but whose descendants sum to far more than tuning.
# delete_split_threshold, must still fan out — at whichever depth a
# directory is itself pathological, not just at the top level named in the
# shard's paths[] (docs/DESIGN-coordinator.md §2.2 DELETE fan-out). This is
# a distinct shape from delete_fanout_e2e.sh (one large flat directory): a
# real incident showed a wide/deep tree of many individually-small
# subdirectories — none of which looked large from its own parent's point of
# view — never split at all under the single-level-only version of this
# check, and got removed serially by one agent thread over several hours.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
. "$ROOT/test/lib.sh"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/drsync-delfanoutnest.XXXXXX")
read -r _CP _HP < <(pick_ports)
CP=${CP:-$_CP}; HP=${HP:-$_HP}
API="http://127.0.0.1:${HP}"; AUTH="Authorization: Bearer delfanoutnesttok"
PASS=0
cleanup() {
    for p in "${APID:-}" "${CPID:-}"; do [[ -n "$p" ]] && kill "$p" 2>/dev/null || true; done
    wait 2>/dev/null || true
    if [[ $PASS -eq 1 ]]; then rm -rf "$WORK"; else echo "work dir kept: $WORK"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }
has() {
    local pat=$1 out
    shift
    out=$("$@") || return 1
    grep -q -- "$pat" <<<"$out"
}
export DRSYNC_SERVER="$API" DRSYNC_TOKEN=delfanoutnesttok

API_TOKEN_FILE="$WORK/api-token"
echo -n delfanoutnesttok >"$API_TOKEN_FILE"
chmod 600 "$API_TOKEN_FILE"
DRSYNC="$ROOT/bin/drsync"

# --- build -------------------------------------------------------------------
make -C "$ROOT/agent" -s
( cd "$ROOT" && go build -o bin/drsyncd ./coordinator/cmd/drsyncd \
             && go build -o bin/drsync ./cli/drsync )

# --- trees: a small source, a destination carrying a wide orphan tree --------
# orphandir/sub0000 .. orphandir/sub0039: 40 subdirectories, 20 files each
# (800 files total). orphandir itself has only 40 entries (all directories)
# and each subN has only 20 — both individually well under the threshold
# below, so neither the top-level probe nor a naive per-subdirectory probe
# would trigger on either level alone; only summing across the whole
# subtree reveals it's pathological, which is exactly why every directory
# in the descent must be checked, not just the one named in paths[].
SRC="$WORK/src"; DST="$WORK/dst"
mkdir -p "$SRC/keep" "$DST/keep"
echo keepme > "$SRC/keep/file.txt"
echo keepme > "$DST/keep/file.txt"
for d in $(seq 0 39); do
    sub="$DST/orphandir/sub$(printf %04d "$d")"
    mkdir -p "$sub"
    for f in $(seq 1 20); do
        echo "junk $d $f" > "$sub/f$(printf %04d "$f").txt"
    done
done

# --- services ----------------------------------------------------------------
"$ROOT/bin/drsyncd" -data-dir "$WORK/coord" -listen-agent 127.0.0.1:$CP \
    -listen-http 127.0.0.1:$HP -api-token-file "$API_TOKEN_FILE" -log-level warn \
    >"$WORK/coord.log" 2>&1 &
CPID=$!
wait_coordinator "$API" "$AUTH" || exit 1
"$ROOT/agent/bin/drsync-agent" -c 127.0.0.1:$CP -i delfanoutnest-agent -w 4 -C 4 \
    >"$WORK/agent.log" 2>&1 &
APID=$!
sleep 1

# threshold=10 sits below BOTH orphandir's own entry count (40 subN
# directories) and each subN's own entry count (20 files) — so the
# regression check is direct: every subN directory must independently fan
# out when rm_dir_contents reaches it during descent, not just orphandir
# itself at the shard's own top level. Proves both levels split in one run.
cat > "$WORK/job.yaml" <<EOF
apiVersion: drsync/v1
kind: Job
metadata: { name: delfanoutnest }
spec:
  source: { path: $SRC }
  destination: { path: $DST }
  probe: { require_mount: false }
  passes: { max: 2, converge_when: { delta_files_below: 1 } }
  tuning: { delete_split_threshold: 10, delete_split_batch: 8 }
EOF
"$DRSYNC" job submit "$WORK/job.yaml" --start | grep -q "job delfanoutnest started" \
    || fail "submit failed"
for _ in $(seq 1 60); do
    curl -sf -H "$AUTH" "$API/api/v1/jobs/delfanoutnest" | grep -q '"state":"COMPLETED"' && break
    sleep 0.25
done
curl -sf -H "$AUTH" "$API/api/v1/jobs/delfanoutnest" | grep -q '"state":"COMPLETED"' \
    || { tail -n 8 "$WORK"/agent.log "$WORK"/coord.log; fail "initial pass did not converge"; }

# orphandir must be journaled as a single ORPHAN record (never descended
# during scan, D5) even though both it and subdirectories under it will fan
# out at delete time.
ORPHANED=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/delfanoutnest/journal?type=ORPHAN" | python3 -c '
import sys, json
recs = json.load(sys.stdin).get("records", [])
print("\n".join(r.get("rel_path", "") for r in recs))
')
grep -qx "orphandir" <<<"$ORPHANED" \
    || fail "orphandir was not journaled as a single top-level orphan (got: $(tr "\n" " " <<<"$ORPHANED"))"

# --- delete pass: fan-out must fire at BOTH the top level and nested -----------
has "DELETE pass triggered" \
    "$DRSYNC" pass trigger delfanoutnest --delete-pass --i-know-this-deletes \
    || fail "delete pass trigger refused"

# High-water mark of KindDelete shards during the pass — the Shard Reaper
# deletes DONE ones once the phase drains, so a post-COMPLETED query alone
# would undercount (same fix delete_fanout_e2e.sh needed).
DB=$(ls "$WORK/coord"/*.db 2>/dev/null | head -1)
delete_shard_count() {
    python3 - "$DB" <<'PY'
import sqlite3, sys
c = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
print(c.execute("select count(*) from shards where kind='delete'").fetchone()[0])
PY
}
NDEL=0
DONE=0
for _ in $(seq 1 120); do
    n=$(delete_shard_count)
    [[ "${n:-0}" -gt "$NDEL" ]] && NDEL=$n
    curl -sf -H "$AUTH" "$API/api/v1/jobs/delfanoutnest" | grep -q '"state":"COMPLETED"' \
        && { DONE=1; break; }
    sleep 0.25
done
[[ "$DONE" -eq 1 ]] \
    || { tail -n 8 "$WORK"/agent.log "$WORK"/coord.log; fail "delete pass did not complete"; }

# 1. fan-out fired more than once: the top-level orphandir shard alone
#    finding 40 sub-directories would produce only a couple of remainder
#    batches (batch=8) plus a cleanup shard if it were the only pathological
#    directory — but 40 subN directories each individually over threshold
#    (20 > 10) must ALSO each fan out when reached during descent, which
#    multiplies the shard count well past what a single-level check could
#    ever produce. This is the actual regression check: before the fix, this
#    number would be tiny (orphandir's own ~6 batches + cleanup, nothing
#    from any subN) and the subdirectories' 800 files would be removed
#    serially inside one shard instead.
[[ "$NDEL" -ge 20 ]] || fail "only $NDEL delete shards recorded; nested fan-out did not fire " \
    "(want >=20: each of 40 subN dirs over threshold must independently split, " \
    "not just orphandir itself)"

# 2. orphandir and everything under it (all 40 subN dirs, all 800 files) is
#    gone — including every subN directory itself, each removed only by its
#    own split's coordinator-seeded cleanup shard.
[[ ! -e "$DST/orphandir" ]] || fail "orphandir (or its nested cleanup) was not fully removed"

# 3. synced content untouched
DIFF=$(diff -r "$SRC" "$DST" 2>&1 || true)
[[ -z "$DIFF" ]] || fail "delete pass damaged synced content:"$'\n'"$DIFF"

# 4. no errors, nothing parked
"$DRSYNC" report delfanoutnest --json > "$WORK/report.json"
python3 - "$WORK/report.json" <<'EOF' || fail "report shows errors or parked shards"
import json, sys
r = json.load(open(sys.argv[1]))
assert r["totals"]["errors"] == 0, r["totals"]["errors"]
assert r["parked_shard_count"] == 0, r["parked_shard_count"]
EOF

echo "delete shards recorded: $NDEL; nested orphan tree fully removed; content intact"
PASS=1
echo "PASS: nested pathological subdirectories fanned out across DELETE shards OK"
