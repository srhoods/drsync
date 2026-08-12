#!/usr/bin/env bash
# drsync type-change e2e: a source path changes TYPE between passes (a real
# directory with content replaced by a symlink) — reported live as a bug:
# the destination still has the old directory (with content, so non-empty),
# creating the symlink failed with ENOTEMPTY (from replace-unlink's
# unlinkat/rmdir) then EEXIST (from the subsequent symlinkat), leaving the
# destination stuck in its stale state forever every pass thereafter.
#
# remove_dst (agent/src/walker.c) now renames a non-empty directory aside
# under a dedicated STALE_PREFIX instead of just failing, and journals the
# new name as an ordinary JR_ORPHAN — so the symlink (or file, or special)
# creation that follows succeeds immediately, and the stale content is
# cleaned up by the next explicit delete pass through the SAME machinery
# (delete.c) that already handles arbitrary-depth/pathological orphan
# removal, no new coordinator-side code needed.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
. "$ROOT/test/lib.sh"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/drsync-typechange.XXXXXX")
read -r _CP _HP < <(pick_ports)
CP=${CP:-$_CP}; HP=${HP:-$_HP}
API="http://127.0.0.1:${HP}"; AUTH="Authorization: Bearer typechangetok"
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
export DRSYNC_SERVER="$API" DRSYNC_TOKEN=typechangetok

API_TOKEN_FILE="$WORK/api-token"
echo -n typechangetok >"$API_TOKEN_FILE"
chmod 600 "$API_TOKEN_FILE"
DRSYNC="$ROOT/bin/drsync"

# --- build -------------------------------------------------------------------
make -C "$ROOT/agent" -s
( cd "$ROOT" && go build -o bin/drsyncd ./coordinator/cmd/drsyncd \
             && go build -o bin/drsync ./cli/drsync )

# --- pass 1: a real, non-empty directory synced normally ----------------------
SRC="$WORK/src"; DST="$WORK/dst"
mkdir -p "$SRC/keep" "$SRC/wasdir/nested"
echo keepme > "$SRC/keep/file.txt"
echo one > "$SRC/wasdir/a.txt"
echo two > "$SRC/wasdir/nested/b.txt"
LINKTARGET=/somewhere/else

# --- services ----------------------------------------------------------------
"$ROOT/bin/drsyncd" -data-dir "$WORK/coord" -listen-agent 127.0.0.1:$CP \
    -listen-http 127.0.0.1:$HP -api-token-file "$API_TOKEN_FILE" -log-level warn \
    >"$WORK/coord.log" 2>&1 &
CPID=$!
wait_coordinator "$API" "$AUTH" || exit 1
"$ROOT/agent/bin/drsync-agent" -c 127.0.0.1:$CP -i typechange-agent -w 4 -C 4 \
    >"$WORK/agent.log" 2>&1 &
APID=$!
sleep 1

cat > "$WORK/job.yaml" <<EOF
apiVersion: drsync/v1
kind: Job
metadata: { name: typechange }
spec:
  source: { path: $SRC }
  destination: { path: $DST }
  probe: { require_mount: false }
  passes: { max: 1, converge_when: { delta_files_below: 1 } }
EOF
"$DRSYNC" job submit "$WORK/job.yaml" --start | grep -q "job typechange started" \
    || fail "submit failed"
for _ in $(seq 1 60); do
    curl -sf -H "$AUTH" "$API/api/v1/jobs/typechange" | grep -q '"state":"COMPLETED"' && break
    sleep 0.25
done
curl -sf -H "$AUTH" "$API/api/v1/jobs/typechange" | grep -q '"state":"COMPLETED"' \
    || { tail -n 8 "$WORK"/agent.log "$WORK"/coord.log; fail "pass 1 did not converge"; }

[[ -d "$DST/wasdir" ]] || fail "pass 1: wasdir was not synced as a directory"
[[ -f "$DST/wasdir/nested/b.txt" ]] || fail "pass 1: wasdir contents were not synced"

# --- source: replace the (still non-empty) directory with a symlink -----------
rm -rf "$SRC/wasdir"
ln -s "$LINKTARGET" "$SRC/wasdir"

# --- pass 2: the type change must be applied, not left stuck -------------------
has "pass triggered" "$DRSYNC" pass trigger typechange || fail "pass 2 trigger failed"
for _ in $(seq 1 60); do
    curl -sf -H "$AUTH" "$API/api/v1/jobs/typechange" | grep -q '"state":"COMPLETED"' && break
    sleep 0.25
done
curl -sf -H "$AUTH" "$API/api/v1/jobs/typechange" | grep -q '"state":"COMPLETED"' \
    || { tail -n 8 "$WORK"/agent.log "$WORK"/coord.log; fail "pass 2 did not converge"; }

# 1. the symlink now exists at the destination, pointing at the right target —
#    the actual bug: this used to fail with ENOTEMPTY then EEXIST and never
#    happen at all, leaving wasdir as a stale real directory forever.
[[ -L "$DST/wasdir" ]] || fail "wasdir is not a symlink at the destination after pass 2"
GOT=$(readlink "$DST/wasdir")
[[ "$GOT" == "$LINKTARGET" ]] || fail "wasdir symlink target = $GOT, want $LINKTARGET"

# 2. no errors reported for the type-change pass — a stuck ENOTEMPTY/EEXIST
#    pair would show up here.
"$DRSYNC" report typechange --json > "$WORK/report2.json"
python3 - "$WORK/report2.json" <<'EOF' || fail "pass 2 report shows errors"
import json, sys
r = json.load(open(sys.argv[1]))
assert r["totals"]["errors"] == 0, r["totals"]["errors"]
EOF

# 3. the old directory's content must have been renamed aside under the
#    destination directory (not left at "wasdir" under its old name, which is
#    now the symlink) and journaled as an ORPHAN — the delete pass's own
#    entry point, so no coordinator-side awareness of "type change" is needed.
STALE=$(find "$DST" -maxdepth 1 -name '.drsync.stale.*')
[[ -n "$STALE" ]] || fail "no .drsync.stale.* entry found at the destination root after pass 2"
[[ -d "$STALE" ]] || fail "stale entry $STALE is not a directory"
[[ -f "$STALE/nested/b.txt" ]] || fail "stale entry $STALE lost the old directory's content"

ORPHANED=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/typechange/journal?type=ORPHAN&pass=2" | python3 -c '
import sys, json
recs = json.load(sys.stdin).get("records", [])
print("\n".join(r.get("rel_path", "") for r in recs))
')
grep -qF "$(basename "$STALE")" <<<"$ORPHANED" \
    || fail "renamed-aside stale directory was not journaled as an ORPHAN (got: $(tr "\n" " " <<<"$ORPHANED"))"

# --- delete pass: the stale content must be fully cleaned up -------------------
has "DELETE pass triggered" \
    "$DRSYNC" pass trigger typechange --delete-pass --i-know-this-deletes \
    || fail "delete pass trigger refused"
for _ in $(seq 1 60); do
    curl -sf -H "$AUTH" "$API/api/v1/jobs/typechange" | grep -q '"state":"COMPLETED"' && break
    sleep 0.25
done
curl -sf -H "$AUTH" "$API/api/v1/jobs/typechange" | grep -q '"state":"COMPLETED"' \
    || { tail -n 8 "$WORK"/agent.log "$WORK"/coord.log; fail "delete pass did not complete"; }

[[ ! -e "$STALE" ]] || fail "stale renamed-aside directory was not removed by the delete pass"
[[ -L "$DST/wasdir" ]] || fail "the delete pass must not have touched the symlink itself"

# 4. everything else about the tree is still correct. --no-dereference: the
#    symlink's own target ($LINKTARGET) is a deliberately dangling path (its
#    content was never part of what this test syncs), so a following diff -r
#    would try to read through it on both sides and fail with ENOENT — the
#    symlink itself (already checked above: -L + readlink match) is what
#    must match, not whatever it happens to point at.
DIFF=$(diff -r --no-dereference "$SRC" "$DST" 2>&1 || true)
[[ -z "$DIFF" ]] || fail "final tree diverged from source:"$'\n'"$DIFF"

echo "wasdir directory->symlink transition applied cleanly; stale content orphaned and reaped"
PASS=1
echo "PASS: destination type change (non-empty directory -> symlink) handled OK"
