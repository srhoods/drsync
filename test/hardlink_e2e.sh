#!/usr/bin/env bash
# drsync hardlink preservation e2e (docs/DESIGN-hardlinks.md): a job opted
# into `metadata.hardlinks: preserve` links hardlink-group members to a
# shared anchor copy instead of duplicating their data, across a multi-agent
# fleet so members can land on different agents/shards from their anchor.
#
# Asserts: destination files sharing a source inode still share one
# destination inode (nlink matches); the report's links_created counter is
# nonzero and link_fallback/link_anchor_races are accounted for; the journal
# records LINK_CREATED entries; and the job still converges cleanly (byte-
# exact single-member file untouched, zero errors, verify passes).
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
. "$ROOT/test/lib.sh"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/drsync-hardlink.XXXXXX")
read -r _CP _HP < <(pick_ports)
CP=${CP:-$_CP}; HP=${HP:-$_HP}
API="http://127.0.0.1:${HP}"; AUTH="Authorization: Bearer hltok"
AGENTS=(hl-a hl-b hl-c)
PASS=0
cleanup() {
    for p in ${APIDS:-}; do kill "$p" 2>/dev/null || true; done
    [[ -n "${CPID:-}" ]] && kill "$CPID" 2>/dev/null || true
    wait 2>/dev/null || true
    if [[ $PASS -eq 1 ]]; then rm -rf "$WORK"; else echo "work dir kept: $WORK"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }

# has PATTERN CMD... — CMD must succeed AND its stdout must match PATTERN.
# Not `CMD | grep -q PAT`: grep -q exits at its first match, CMD then dies of
# SIGPIPE, and under `set -o pipefail` the pipeline reports 141 even though the
# pattern matched (see e2e.sh for the full rationale).
has() {
    local pat=$1 out
    shift
    out=$("$@") || return 1
    grep -q -- "$pat" <<<"$out"
}

export DRSYNC_SERVER="$API" DRSYNC_TOKEN=hltok

API_TOKEN_FILE="$WORK/api-token"
echo -n hltok >"$API_TOKEN_FILE"
chmod 600 "$API_TOKEN_FILE"
DRSYNC="$ROOT/bin/drsync"

# --- build -------------------------------------------------------------------
make -C "$ROOT/agent" -s
( cd "$ROOT" && go build -o bin/drsyncd ./coordinator/cmd/drsyncd \
             && go build -o bin/drsync ./cli/drsync )

# --- source: hardlink groups spread across directories, so a small
# shard_budget forces different agents to walk different members. ------------
SRC="$WORK/src"; DST="$WORK/dst"
mkdir -p "$SRC"/a "$SRC"/b "$SRC"/c
head -c 65536 /dev/urandom > "$SRC/a/original.bin"
ln "$SRC/a/original.bin" "$SRC/b/hardlink1.bin"
ln "$SRC/a/original.bin" "$SRC/c/hardlink2.bin"
GROUP_SUM=$(sha256sum "$SRC/a/original.bin" | cut -d' ' -f1)
[[ "$(stat -c '%h' "$SRC/a/original.bin")" == "3" ]] || fail "test setup: expected nlink=3 on source"

# a second, smaller group (2 members) to check more than one group correlates
echo "small shared content" > "$SRC/a/small.txt"
ln "$SRC/a/small.txt" "$SRC/c/small-link.txt"

# an ordinary, non-hardlinked file: must be copied normally, untouched by any
# of this — the control for "hardlink support doesn't break the common case".
echo "plain file" > "$SRC/b/plain.txt"

# --- coordinator + 3-agent fleet ---------------------------------------------
"$ROOT/bin/drsyncd" -data-dir "$WORK/coord" -listen-agent 127.0.0.1:$CP \
    -listen-http 127.0.0.1:$HP -api-token-file "$API_TOKEN_FILE" -log-level warn \
    >"$WORK/coord.log" 2>&1 &
CPID=$!
wait_coordinator "$API" "$AUTH" || exit 1
APIDS=""
for a in "${AGENTS[@]}"; do
    "$ROOT/agent/bin/drsync-agent" -c 127.0.0.1:$CP -i "$a" -w 2 -C 2 \
        >"$WORK/$a.log" 2>&1 &
    APIDS="$APIDS $!"
done
for _ in $(seq 1 40); do
    n=$(curl -sf -H "$AUTH" "$API/api/v1/agents" | { grep -o '"connected":true' || true; } | wc -l)
    [[ "$n" -eq 3 ]] && break; sleep 0.25
done
[[ "${n:-0}" -eq 3 ]] || fail "expected 3 connected agents, got ${n:-0}"

# --- job: hardlinks: preserve, tiny shard_budget forces a/b/c to different
# shards (and likely different agents) so the anchor and its members are
# discovered independently, exercising real cross-shard correlation. ---------
cat > "$WORK/job.yaml" <<EOF
apiVersion: drsync/v1
kind: Job
metadata: { name: hardlink }
spec:
  source: { path: $SRC }
  destination: { path: $DST }
  probe: { require_mount: false }   # test roots are plain dirs, not mounts
  passes: { max: 2, converge_when: { delta_files_below: 1 } }
  metadata: { hardlinks: preserve }
  tuning: { shard_budget: 1, spread_mode: always }
  verify: { checksum: { sample_rate: 1.0 } }
EOF
"$DRSYNC" job submit "$WORK/job.yaml" --start >/dev/null || fail "submit failed"

STATE=""
for _ in $(seq 1 120); do
    STATE=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/hardlink" | grep -o '"state":"[A-Z]*"' | head -1)
    [[ "$STATE" == '"state":"COMPLETED"' ]] && break
    sleep 0.25
done
[[ "$STATE" == '"state":"COMPLETED"' ]] || {
    tail -8 "$WORK"/coord.log "$WORK"/hl-*.log; fail "did not converge (state=$STATE)"; }

# --- 1. content is correct everywhere -----------------------------------------
for f in a/original.bin b/hardlink1.bin c/hardlink2.bin; do
    [[ "$(sha256sum "$DST/$f" | cut -d' ' -f1)" == "$GROUP_SUM" ]] \
        || fail "$f content mismatch"
done
[[ "$(cat "$DST/a/small.txt")" == "small shared content" ]] || fail "small.txt content mismatch"
[[ "$(cat "$DST/c/small-link.txt")" == "small shared content" ]] || fail "small-link.txt content mismatch"
[[ "$(cat "$DST/b/plain.txt")" == "plain file" ]] || fail "plain.txt (non-hardlinked control) mismatch"

# --- 2. destination files actually SHARE one inode (the point of the feature) -
DINO_A=$(stat -c '%i' "$DST/a/original.bin")
DINO_B=$(stat -c '%i' "$DST/b/hardlink1.bin")
DINO_C=$(stat -c '%i' "$DST/c/hardlink2.bin")
[[ "$DINO_A" == "$DINO_B" && "$DINO_A" == "$DINO_C" ]] \
    || fail "hardlink group not deduped: inodes a=$DINO_A b=$DINO_B c=$DINO_C"
NLINK=$(stat -c '%h' "$DST/a/original.bin")
[[ "$NLINK" == "3" ]] || fail "destination nlink=$NLINK, want 3 (dedup did not happen)"

SINO_A=$(stat -c '%i' "$DST/a/small.txt")
SINO_C=$(stat -c '%i' "$DST/c/small-link.txt")
[[ "$SINO_A" == "$SINO_C" ]] || fail "small group not deduped: inodes a=$SINO_A c=$SINO_C"

# the plain file must NOT be linked to anything (it has no source siblings)
[[ "$(stat -c '%h' "$DST/b/plain.txt")" == "1" ]] || fail "plain.txt got an unexpected extra link"

# --- 3. report: links_created counts the 3 member links actually created
# (original.bin's anchor + 2 linked members = 2 links; small.txt's anchor +
# 1 linked member = 1 link; total 3), no fallback, no anchor races expected
# at this small scale (still tolerated if the fleet raced, just logged). -----
"$DRSYNC" report hardlink --json > "$WORK/report.json"
python3 - "$WORK/report.json" <<'EOF' || fail "report hardlink totals wrong"
import json, sys
r = json.load(open(sys.argv[1]))
t = r["totals"]
assert t["links_created"] == 3, f"links_created = {t['links_created']}, want 3"
assert t["link_fallback"] == 0, f"link_fallback = {t['link_fallback']}, want 0"
print(f"links_created={t['links_created']} link_anchor_races={t.get('link_anchor_races')} "
      f"link_fallback={t['link_fallback']}")
EOF

# --- 4. journal recorded the links, and NLINK_DUP sightings for every member -
has "LINK_CREATED" "$DRSYNC" journal cat hardlink --pass 1 --type link_created \
    || fail "journal missing LINK_CREATED records"
LC=$("$DRSYNC" journal cat hardlink --pass 1 --type link_created --jsonl | wc -l)
[[ "$LC" -eq 3 ]] || fail "journal has $LC LINK_CREATED records, want 3"

# --- 5. zero errors, verify passed, converged in one further no-op pass ------
JOB=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/hardlink")
echo "$JOB" | grep -q '"errors":0' || fail "job reported errors: $JOB"
echo "$JOB" | grep -q '"verify_fail":[1-9]' && fail "verify failures reported: $JOB"
PL=$(echo "$JOB" | grep -o '"files_copied":[0-9]*' | tail -1 | cut -d: -f2)
[[ "${PL:-1}" -eq 0 ]] || fail "final pass still copied $PL files (not converged)"

echo "PASS: hardlink groups deduped across a 3-agent fleet (nlink=3, one shared" \
     "inode each); links_created=3; converged; verify clean"
PASS=1
