#!/usr/bin/env bash
# drsync e2e: coordinator + C agent sync a real tree, converge, verify fidelity.
# Uses a tiny shard_budget to force the split path (self-partitioning walk).
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d "${TMPDIR:-/tmp}/drsync-e2e.XXXXXX")
COORD_PORT=${COORD_PORT:-17540}
HTTP_PORT=${HTTP_PORT:-17541}
API="http://127.0.0.1:${HTTP_PORT}"
AUTH="Authorization: Bearer e2etoken"
PASS=0

cleanup() {
    [[ -n "${WATCH_PID:-}" ]] && kill "$WATCH_PID" 2>/dev/null || true
    [[ -n "${AGENT_PID:-}" ]] && kill "$AGENT_PID" 2>/dev/null || true
    [[ -n "${COORD_PID:-}" ]] && kill "$COORD_PID" 2>/dev/null || true
    wait 2>/dev/null || true
    if [[ $PASS -eq 1 ]]; then rm -rf "$WORK"; else echo "work dir kept: $WORK"; fi
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

# has PATTERN CMD... — CMD must succeed AND its stdout must match PATTERN.
#
# Not `CMD | grep -q PAT`: grep -q exits at its first match, CMD then dies of
# SIGPIPE, and under `set -o pipefail` the pipeline reports 141 even though the
# pattern matched. Whether that fires depends on how much CMD had left to write
# when grep quit — i.e. on where the match happens to sit in the output — so it
# shows up as an intermittent failure that moves whenever record ordering does.
#
# `out` is declared before the assignment on purpose: `local out=$(CMD)` would
# mask CMD's exit status behind local's own, silently dropping the status check
# that pipefail used to give us.
has() {
    local pat=$1 out
    shift
    out=$("$@") || return 1
    grep -q -- "$pat" <<<"$out"
}

# --- build -------------------------------------------------------------------
make -C "$ROOT/agent" -s
( cd "$ROOT" && go build -o bin/drsyncd ./coordinator/cmd/drsyncd \
             && go build -o bin/drsync ./cli/drsync )
DRSYNC="$ROOT/bin/drsync"
export DRSYNC_SERVER="$API" DRSYNC_TOKEN=e2etoken

# --- source tree -------------------------------------------------------------
SRC="$WORK/src" DST="$WORK/dst"
mkdir -p "$SRC"/projects/{alpha,beta/nested/deep} "$SRC"/home/{u1,u2} "$SRC"/empty
echo "hello drsync" > "$SRC/projects/alpha/readme.txt"
head -c 5242880 /dev/urandom > "$SRC/projects/alpha/blob.bin"      # 5 MiB
head -c 1000 /dev/urandom > "$SRC/projects/beta/nested/deep/leaf"
for i in $(seq 1 40); do echo "file $i" > "$SRC/home/u1/f$i.txt"; done
echo "exec me" > "$SRC/home/u2/tool.sh"; chmod 0750 "$SRC/home/u2/tool.sh"
ln -s ../alpha/readme.txt "$SRC/projects/beta/link"
# a special file (FIFO): exercises the mknod path + metadata fidelity on
# specials. mode/mtime set last so they are what we assert on.
mkfifo "$SRC/home/u2/pipe"; chmod 0640 "$SRC/home/u2/pipe"
touch -d '2020-09-13 13:26:40' "$SRC/home/u2/pipe"
touch -d '2020-05-04 03:02:01' "$SRC/projects/alpha/readme.txt"
chmod 0640 "$SRC/projects/alpha/readme.txt"

# xattrs on a file and a directory
python3 - "$SRC" <<'EOF'
import os, sys
src = sys.argv[1]
os.setxattr(f"{src}/projects/alpha/readme.txt", "user.drsync.test", b"forty-two")
os.setxattr(f"{src}/projects/alpha/readme.txt", "user.mime", b"text/plain")
os.setxattr(f"{src}/home/u1", "user.dirattr", b"on-a-directory")
EOF
# POSIX ACL (travels as system.posix_acl_access xattr)
setfacl -m u:root:r "$SRC/home/u2/tool.sh"
# sparse file: 16 MiB logical, one 4 KiB data extent at 1 MiB
python3 - "$SRC" <<'EOF'
import os, sys
p = f"{sys.argv[1]}/projects/beta/sparse.img"
with open(p, "wb") as f:
    f.truncate(16 * 1024 * 1024)
    f.seek(1024 * 1024)
    f.write(os.urandom(4096))
st = os.stat(p)
assert st.st_blocks * 512 < st.st_size, "test fs did not create a sparse file"
EOF

# --- pre-existing destination state ------------------------------------------
mkdir -p "$DST/projects/alpha" "$DST/keepme/nested" "$DST/home"
echo "STALE CONTENT" > "$DST/projects/alpha/readme.txt"            # must be replaced
touch -d '2019-01-01 00:00:00' "$DST/projects/alpha/readme.txt"
echo "orphan" > "$DST/keepme/orphan.txt"                           # survives sync (D5)
echo "deep" > "$DST/keepme/nested/deep.txt"                        # inside orphan dir
echo "nested orphan" > "$DST/home/u1-orphan.txt"                   # orphan in synced dir? (u1-orphan sits at home/)
echo "residue" > "$DST/projects/.drsync.tmp.deadbeef.1"            # must be reclaimed

# --- start services -----------------------------------------------------------
"$ROOT/bin/drsyncd" -data-dir "$WORK/coord" \
    -listen-agent "127.0.0.1:${COORD_PORT}" -listen-http "127.0.0.1:${HTTP_PORT}" \
    -api-token e2etoken -log-level warn >"$WORK/coord.log" 2>&1 &
COORD_PID=$!
for _ in $(seq 1 40); do
    curl -sf "$API/healthz" >/dev/null 2>&1 && break; sleep 0.25
done
curl -sf "$API/healthz" >/dev/null || fail "coordinator did not come up"

"$ROOT/agent/bin/drsync-agent" -c "127.0.0.1:${COORD_PORT}" -i agent-e2e -w 4 \
    >"$WORK/agent.log" 2>&1 &
AGENT_PID=$!
sleep 1
curl -sf -H "$AUTH" "$API/api/v1/agents" | grep -q '"connected":true' \
    || fail "agent did not register"
has "agent-e2e.*true" "$DRSYNC" agent list \
    || fail "CLI agent list does not show the connected agent"

# --- submit + run job ----------------------------------------------------------
cat > "$WORK/job.yaml" <<EOF
apiVersion: drsync/v1
kind: Job
metadata:
  name: e2e
spec:
  source: { path: $SRC }
  destination: { path: $DST }
  passes:
    max: 4
    converge_when:
      delta_files_below: 1
  verify:
    checksum:
      sample_rate: 1.0       # checksum-verify everything that was copied
EOF
# submit + start via the CLI; --set exercises the YAML-path override pipeline
# (shard_budget deliberately absent from the spec file above)
has "job e2e started" "$DRSYNC" job submit "$WORK/job.yaml" \
    --set spec.tuning.shard_budget=4 --start || fail "CLI job submit --start failed"
curl -sf -H "$AUTH" "$API/api/v1/jobs/e2e" >/dev/null || fail "submitted job not visible"

# follow live progress over the WebSocket event feed while the job runs
"$DRSYNC" job status e2e --watch >"$WORK/watch.log" 2>&1 &
WATCH_PID=$!

for _ in $(seq 1 120); do
    STATE=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/e2e" | grep -o '"state":"[A-Z]*"' | head -1)
    [[ "$STATE" == '"state":"COMPLETED"' ]] && break
    sleep 0.5
done
[[ "${STATE:-}" == '"state":"COMPLETED"' ]] || {
    curl -s -H "$AUTH" "$API/api/v1/jobs/e2e"; echo
    tail -5 "$WORK/agent.log" "$WORK/coord.log"
    fail "job did not converge (state=$STATE)"
}

# --- verify -------------------------------------------------------------------
JOB=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/e2e")

# 1. content: every src file identical in dst; only the orphan may be extra
# (diff notes matching FIFOs/sockets with an "is a fifo" line, not a real diff)
DIFF=$(diff -r "$SRC" "$DST" 2>&1 | grep -v "^Only in $DST" | grep -v " is a fifo " || true)
[[ -z "$DIFF" ]] || fail "content mismatch:"$'\n'"$DIFF"

# 2. orphan preserved (report-only deletes), temp residue reclaimed
[[ -f "$DST/keepme/orphan.txt" ]] || fail "orphan was deleted (violates D5)"
[[ ! -e "$DST/projects/.drsync.tmp.deadbeef.1" ]] || fail "temp residue not reclaimed"

# 3. metadata: mode + mtime preserved
for f in projects/alpha/readme.txt home/u2/tool.sh; do
    s=$(stat -c '%a %Y' "$SRC/$f"); d=$(stat -c '%a %Y' "$DST/$f")
    [[ "$s" == "$d" ]] || fail "metadata mismatch on $f: src=($s) dst=($d)"
done
# directory mtime
s=$(stat -c '%Y' "$SRC/home/u1"); d=$(stat -c '%Y' "$DST/home/u1")
[[ "$s" == "$d" ]] || fail "dir mtime mismatch on home/u1: src=$s dst=$d"

# 4. symlink preserved as symlink with same target
[[ "$(readlink "$DST/projects/beta/link")" == "../alpha/readme.txt" ]] \
    || fail "symlink not preserved"

# 4b. xattrs preserved on file and directory
python3 - "$SRC" "$DST" <<'EOF' || exit 1
import os, sys
src, dst = sys.argv[1], sys.argv[2]
for rel in ["projects/alpha/readme.txt", "home/u1"]:
    s = {n: os.getxattr(f"{src}/{rel}", n) for n in os.listxattr(f"{src}/{rel}")}
    d = {n: os.getxattr(f"{dst}/{rel}", n) for n in os.listxattr(f"{dst}/{rel}")}
    assert s == d and s, f"xattr mismatch on {rel}: src={s} dst={d}"
print("xattrs preserved")
EOF

# 4c. POSIX ACL preserved
SA=$(getfacl -pc "$SRC/home/u2/tool.sh"); DA=$(getfacl -pc "$DST/home/u2/tool.sh")
[[ "$SA" == "$DA" ]] || fail "ACL mismatch: src=[$SA] dst=[$DA]"
echo "$DA" | grep -q "user:root:r--" || fail "named-user ACE missing on dst: [$DA]"

# 4d. sparse file: content identical AND sparseness preserved
cmp -s "$SRC/projects/beta/sparse.img" "$DST/projects/beta/sparse.img" \
    || fail "sparse file content mismatch"
DBLK=$(stat -c '%b' "$DST/projects/beta/sparse.img")
[[ $((DBLK * 512)) -lt 1048576 ]] \
    || fail "sparseness lost: dst uses $((DBLK * 512)) bytes for 16MiB logical"

# 4e. special file (FIFO) recreated with matching type + metadata. The mtime is
#     the regression that matters: specials were created without utimens, so
#     verify failed on "mtime mismatch" and never converged (they are not
#     recopied). Section 7 below asserts verify_fail==0, which covers that.
[[ -p "$DST/home/u2/pipe" ]] || fail "FIFO not recreated as a fifo"
s=$(stat -c '%a %Y' "$SRC/home/u2/pipe"); d=$(stat -c '%a %Y' "$DST/home/u2/pipe")
[[ "$s" == "$d" ]] || fail "special-file metadata mismatch: src=($s) dst=($d)"

# 5. convergence: pass 1 copied files, final pass copied none
P1=$(echo "$JOB" | grep -o '"pass_no":1,[^}]*' | grep -o '"files_copied":[0-9]*' | cut -d: -f2)
PL=$(echo "$JOB" | grep -o '"files_copied":[0-9]*' | tail -1 | cut -d: -f2)
[[ "${P1:-0}" -gt 40 ]] || fail "pass 1 copied only ${P1:-0} files"
[[ "${PL:-1}" -eq 0 ]] || fail "final pass still copied $PL files (not converged)"

# 6. zero errors reported
echo "$JOB" | grep -q '"errors":0' || fail "job reported errors: $JOB"

# 7. verify phase: pass 1 checksummed every copied file, none failed
V1=$(echo "$JOB" | grep -o '"pass_no":1,[^}]*' | grep -o '"verify_ok":[0-9]*' | cut -d: -f2)
VF=$(echo "$JOB" | grep -o '"verify_fail":[0-9]*' | tr -dc '0-9\n' | sort -u | tr '\n' ' ')
[[ "${V1:-0}" -ge "$P1" ]] || fail "pass1 verified only ${V1:-0} of $P1 copied entries"
echo "$JOB" | grep -q '"verify_fail":[1-9]' && fail "verify failures reported: $JOB"

# counts occurrences, not lines; `|| true` stops a no-match grep from
# aborting the script under pipefail + set -e.
N_PASSES=$(echo "$JOB" | { grep -o '"pass_no":' || true; } | wc -l)
echo "sync converged in $N_PASSES passes (pass1 copied $P1 files), fidelity checks OK"

# --- operator surface: CLI + events + query endpoints --------------------------
# 8. the --watch client (WebSocket feed) saw live progress and exited on its own
for _ in $(seq 1 20); do kill -0 "$WATCH_PID" 2>/dev/null || break; sleep 0.5; done
kill -0 "$WATCH_PID" 2>/dev/null && fail "watch client did not exit after job completion"
WATCH_PID=
grep -q "walked=" "$WORK/watch.log" || fail "watch saw no stats frames: $(cat "$WORK/watch.log")"
grep -q "job e2e -> COMPLETED" "$WORK/watch.log" \
    || fail "watch missed the terminal job_state event: $(cat "$WORK/watch.log")"

# 9. CLI job list / status render the converged job. Capture output first:
# piping straight into `grep -q` lets grep close the pipe on its first match,
# and the CLI then takes SIGPIPE mid-table, which pipefail turns into a failure.
JOBLIST=$("$DRSYNC" job list)
grep -Eq 'e2e +COMPLETED' <<<"$JOBLIST" || fail "CLI job list wrong"
JOBSTAT=$("$DRSYNC" job status e2e)
grep -q "job e2e: COMPLETED" <<<"$JOBSTAT" || fail "CLI job status wrong"

# 10. pass detail endpoint: shard breakdown + duration
PD=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/e2e/passes/1")
echo "$PD" | grep -q '"duration_ms"' || fail "pass detail missing duration: $PD"
echo "$PD" | grep -q '"DONE"' || fail "pass detail missing shard counts: $PD"

# 11. journal query: pass-1 orphans visible through the API, with type filter
has "keepme" "$DRSYNC" journal cat e2e --pass 1 --type orphan \
    || fail "journal cat did not list the keepme orphan"
has '"rel_path":"projects/alpha/readme.txt"' \
    "$DRSYNC" journal cat e2e --pass 1 --type copied --path projects/ --jsonl \
    || fail "journal cat path filter failed"

# 12. error browser: clean job reports none
has "no errors" "$DRSYNC" errors e2e --pass all || fail "CLI errors not clean"

# 13. report: converged, verify totals, orphans still outstanding (pre-delete)
"$DRSYNC" report e2e --json > "$WORK/report1.json"
grep -q '"converged": true' "$WORK/report1.json" || fail "report not converged"
grep -q '"delete_pass_ran": false' "$WORK/report1.json" || fail "report claims delete ran"
python3 - "$WORK/report1.json" <<'EOF' || fail "report totals wrong"
import json, sys
r = json.load(open(sys.argv[1]))
assert r["orphans_remaining"] >= 2, r["orphans_remaining"]   # keepme + u1-orphan
assert r["totals"]["verify_ok"] >= r["passes"][0]["files_copied"] > 40
assert r["totals"]["verify_fail"] == 0 and r["totals"]["errors"] == 0
assert r["parked_shard_count"] == 0
assert r["passes"][0]["duration_ms"] > 0 and r["passes"][0]["delta_files"] > 40
EOF
has "converged: true" "$DRSYNC" report e2e || fail "human report wrong"

# --- delete pass (journals + D5 double gate) ----------------------------------
# journals were written on the coordinator
ls "$WORK"/coord/journals/*/pass-*/segment-*.drj >/dev/null 2>&1 \
    || fail "no journal segments on the coordinator"

# gate 1a: API refuses delete without the confirm string
CODE=$(curl -s -o /dev/null -w '%{http_code}' -H "$AUTH" -X POST \
    -d '{"delete": true}' "$API/api/v1/jobs/e2e/passes")
[[ "$CODE" == "412" ]] || fail "unconfirmed delete pass not refused (got $CODE)"
# gate 1b: CLI refuses --delete-pass without --i-know-this-deletes
"$DRSYNC" pass trigger e2e --delete-pass 2>/dev/null \
    && fail "CLI allowed --delete-pass without acknowledgement"
[[ -f "$DST/keepme/orphan.txt" ]] || fail "refused delete pass still deleted something"

# gate 2: fully acknowledged delete pass runs
has "DELETE pass triggered" \
    "$DRSYNC" pass trigger e2e --delete-pass --i-know-this-deletes \
    || fail "confirmed delete pass rejected"
for _ in $(seq 1 60); do
    JOB=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/e2e")
    echo "$JOB" | grep -q '"state":"COMPLETED"' && break
    sleep 0.5
done
echo "$JOB" | grep -q '"state":"COMPLETED"' || fail "delete pass did not complete: $JOB"

# orphans (incl. recursive orphan dir) removed; synced content untouched
[[ ! -e "$DST/keepme" ]] || fail "orphan dir keepme/ not removed recursively"
[[ ! -e "$DST/home/u1-orphan.txt" ]] || fail "nested orphan file not removed"
POSTDIFF=$(diff -r "$SRC" "$DST" 2>&1 | grep -v " is a fifo " || true)
[[ -z "$POSTDIFF" ]] || fail "delete pass damaged synced content:"$'\n'"$POSTDIFF"
DP=$(echo "$JOB" | grep -o '"state":"COMPLETE"[^}]*"orphans":[0-9]*' | tail -1 | grep -o '"orphans":[0-9]*' | cut -d: -f2)
DELP=$(echo "$JOB" | grep -o '"pass_no":[0-9]*' | tail -1 | cut -d: -f2)
[[ "${DP:-0}" -ge 4 ]] || fail "delete pass removed only ${DP:-0} objects (want >=4)"

# 14. post-delete report reflects the reclaim; queue is drained
"$DRSYNC" report e2e --json > "$WORK/report2.json"
grep -q '"delete_pass_ran": true' "$WORK/report2.json" || fail "report missed delete pass"
grep -q '"orphans_remaining": 0' "$WORK/report2.json" || fail "orphans not zeroed in report"
has "keepme" "$DRSYNC" journal cat e2e --pass "$DELP" --type deleted \
    || fail "delete pass journal missing DELETED records"
has "queue empty" "$DRSYNC" queue || fail "queue not drained: $("$DRSYNC" queue)"

echo "PASS: e2e sync converged in $N_PASSES passes (pass1 copied $P1 files);" \
     "delete pass $DELP removed $DP orphaned objects; operator surface + fidelity checks OK"
PASS=1
