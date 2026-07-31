#!/usr/bin/env bash
# drsync hardlink resilience (docs/DESIGN-hardlinks.md): two fault paths the
# design's LINKFIX phase must survive.
#
# 1. An agent dies while LinkTask shards are in flight — the same lease-expiry
#    + re-grant recovery chunk_resilience_e2e.sh proves for cross-host chunk
#    fan-out, here for linkat's own EEXIST-retry idempotency: a re-granted
#    LinkTask must land on "already correctly linked" (RES_OK, no double
#    count — the bug hardlink_e2e.sh's convergence pass caught) rather than
#    erroring or parking.
#
# 2. A hardlink group larger than the job's hardlinks_max_group_scan cap: the
#    coordinator marks it 'fallback' and never seeds a LinkTask for it at all
#    (§3.6) — its members must still arrive as correct, complete, independent
#    copies (not missing, not corrupted), same as D3's default behavior,
#    with link_fallback counting the group.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
. "$ROOT/test/lib.sh"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/drsync-hlres.XXXXXX")
read -r _CP _HP < <(pick_ports)
CP=${CP:-$_CP}; HP=${HP:-$_HP}
API="http://127.0.0.1:${HP}"; AUTH="Authorization: Bearer hlrestok"
PASS=0
cleanup() {
    for p in ${APIDS:-}; do kill "$p" 2>/dev/null || true; done
    [[ -n "${CPID:-}" ]] && kill "$CPID" 2>/dev/null || true
    wait 2>/dev/null || true
    if [[ $PASS -eq 1 ]]; then rm -rf "$WORK"; else echo "work dir kept: $WORK"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }
export DRSYNC_SERVER="$API" DRSYNC_TOKEN=hlrestok

API_TOKEN_FILE="$WORK/api-token"
echo -n hlrestok >"$API_TOKEN_FILE"
chmod 600 "$API_TOKEN_FILE"
DRSYNC="$ROOT/bin/drsync"

make -C "$ROOT/agent" -s
( cd "$ROOT" && go build -o bin/drsyncd ./coordinator/cmd/drsyncd \
             && go build -o bin/drsync ./cli/drsync )

# --- source: many hardlink groups, spread across directories, so several
# LinkTask shards are in flight when an agent is killed. Also one huge group
# (11 members) to exceed a small hardlinks_max_group_scan cap. ---------------
SRC="$WORK/src"; DST="$WORK/dst"
mkdir -p "$SRC"/d{0..9}
for i in $(seq 0 9); do
    head -c 8192 /dev/urandom > "$SRC/d$i/anchor.bin"
    ln "$SRC/d$i/anchor.bin" "$SRC/d$i/link1.bin"
    ln "$SRC/d$i/anchor.bin" "$SRC/d$i/link2.bin"
done
# The oversized group: 11 members, cap will be 5.
mkdir -p "$WORK/bigsrc"
head -c 4096 /dev/urandom > "$SRC/d0/huge_anchor.bin"
BIG_SUM=$(sha256sum "$SRC/d0/huge_anchor.bin" | cut -d' ' -f1)
for i in $(seq 1 10); do
    ln "$SRC/d0/huge_anchor.bin" "$SRC/d0/huge_member$i.bin"
done

# Short lease TTL so a dead agent's linkfix leases re-queue within patience.
"$ROOT/bin/drsyncd" -data-dir "$WORK/coord" -listen-agent 127.0.0.1:$CP \
    -listen-http 127.0.0.1:$HP -api-token-file "$API_TOKEN_FILE" -lease-ttl 3s \
    -heartbeat-interval 1s -log-level warn >"$WORK/coord.log" 2>&1 &
CPID=$!
wait_coordinator "$API" "$AUTH" || exit 1

APIDS=""
declare -A APID
for a in hlres-a hlres-b hlres-c; do
    "$ROOT/agent/bin/drsync-agent" -c 127.0.0.1:$CP -i "$a" -w 2 -C 2 \
        >"$WORK/$a.log" 2>&1 &
    APID[$a]=$!
    APIDS="$APIDS $!"
done
for _ in $(seq 1 40); do
    n=$(curl -sf -H "$AUTH" "$API/api/v1/agents" | { grep -o '"connected":true' || true; } | wc -l)
    [[ "$n" -eq 3 ]] && break; sleep 0.25
done
[[ "${n:-0}" -eq 3 ]] || fail "expected 3 agents, got ${n:-0}"

cat > "$WORK/job.yaml" <<EOF
apiVersion: drsync/v1
kind: Job
metadata: { name: hlres }
spec:
  source: { path: $SRC }
  destination: { path: $DST }
  probe: { require_mount: false }   # test roots are plain dirs, not mounts
  passes: { max: 2, converge_when: { delta_files_below: 1 } }
  metadata: { hardlinks: preserve, hardlinks_max_group_scan: 5 }
  tuning: { shard_budget: 1, spread_mode: always }
  verify: { checksum: { sample_rate: 1.0 } }
EOF
"$DRSYNC" job submit "$WORK/job.yaml" --start >/dev/null || fail "submit failed"

# Kill one agent partway through — by the time LINKFIX seeds (after DIRFIX,
# itself after SCANNING drains), several LinkTask shards should be queued or
# in flight. A fixed wait matched to chunk_resilience_e2e.sh's own approach:
# too early recovers nothing (vacuous pass), too late and it's already done.
sleep 2
kill -9 "${APID[hlres-b]}" 2>/dev/null || true
echo "killed hlres-b (best-effort, may land during any phase)"

STATE=""
for _ in $(seq 1 240); do
    STATE=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/hlres" | grep -o '"state":"[A-Z]*"' | head -1)
    [[ "$STATE" == '"state":"COMPLETED"' ]] && break
    sleep 0.5
done
[[ "$STATE" == '"state":"COMPLETED"' ]] || {
    tail -8 "$WORK"/coord.log "$WORK"/hlres-*.log; fail "did not converge after agent death (state=$STATE)"; }

# --- 1. no shard was permanently parked (a parked linkfix would stall the pass)
PARKED=$(curl -sf -H "$AUTH" "$API/api/v1/jobs/hlres/passes/1" \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['shards'].get('PARKED',0))")
[[ "$PARKED" -eq 0 ]] || fail "$PARKED shard(s) parked during recovery"

# --- 2. the small groups (nlink=3, under the cap) still deduped correctly ----
for i in $(seq 0 9); do
    A=$(stat -c '%i' "$DST/d$i/anchor.bin")
    L1=$(stat -c '%i' "$DST/d$i/link1.bin")
    L2=$(stat -c '%i' "$DST/d$i/link2.bin")
    [[ "$A" == "$L1" && "$A" == "$L2" ]] \
        || fail "group d$i not deduped after agent death: $A $L1 $L2"
    NLINK=$(stat -c '%h' "$DST/d$i/anchor.bin")
    [[ "$NLINK" == "3" ]] || fail "group d$i nlink=$NLINK after recovery, want 3"
done

# --- 3. the oversized group (11 members > cap of 5) fell back: every member
# is a correct, complete, INDEPENDENT copy — not deduped, not missing. -------
for f in huge_anchor.bin huge_member1.bin huge_member5.bin huge_member10.bin; do
    [[ "$(sha256sum "$DST/d0/$f" | cut -d' ' -f1)" == "$BIG_SUM" ]] \
        || fail "$f content wrong after fallback"
done
FALLBACK_NLINK=$(stat -c '%h' "$DST/d0/huge_anchor.bin")
[[ "$FALLBACK_NLINK" == "1" ]] \
    || fail "oversized group nlink=$FALLBACK_NLINK, want 1 (fallback should NOT dedup)"

# --- 4. report reflects both outcomes: dedup for small groups, fallback for
# the oversized one — the fallback counter is what makes the space cost of a
# capped group visible, matching D3's reporting principle. -------------------
"$DRSYNC" report hlres --json > "$WORK/report.json"
python3 - "$WORK/report.json" <<'EOF' || fail "report totals wrong"
import json, sys
r = json.load(open(sys.argv[1]))
t = r["totals"]
# 10 small groups x 2 links each = 20; the oversized group contributes 0
# (fallback, no LinkTask ever seeded for it).
assert t["links_created"] == 20, f"links_created = {t['links_created']}, want 20"
assert t["link_fallback"] >= 1, f"link_fallback = {t['link_fallback']}, want >=1"
print(f"links_created={t['links_created']} link_fallback={t['link_fallback']}")
EOF

echo "PASS: agent killed mid-LINKFIX recovered with no parks; small groups deduped" \
     "(nlink=3); oversized group correctly fell back to independent copies"
PASS=1
