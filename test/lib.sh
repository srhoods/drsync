# Shared helpers for the e2e scripts. Source after ROOT is set:
#   . "$ROOT/test/lib.sh"

# pick_ports prints two free localhost TCP ports (agent listener, HTTP listener).
#
# The scripts used to hardcode a pair each. That fails in two ways: several
# scripts picked the SAME pair, so they could not run concurrently, and any
# unrelated process already on the port took the run down in a way that pointed
# nowhere near the real cause — a long-lived drsyncd from another checkout
# answered our /healthz probe, then rejected our API token, and the script died
# on the resulting 401 several steps later.
#
# The kernel picks both (bind to port 0), and both sockets stay open until each
# number has been read so they cannot come back equal. There is a small window
# between close and the coordinator's bind; wait_coordinator below is what
# turns a lost race into a clear message instead of a confusing one.
pick_ports() {
    python3 - <<'PY'
import socket

socks = []
for _ in range(2):
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    socks.append(s)
print(*(s.getsockname()[1] for s in socks))
for s in socks:
    s.close()
PY
}

# wait_coordinator <api-url> <auth-header> — blocks until OUR coordinator is
# serving, and fails loudly if something else is.
#
# /healthz alone is not proof: any drsyncd answers it, including one that is not
# ours and does not accept our token. Authenticating as well distinguishes "not
# up yet" from "someone else is on this port", which is the failure that cost an
# afternoon.
wait_coordinator() {
    local api=$1 auth=$2 i
    for i in $(seq 1 40); do
        curl -sf "$api/healthz" >/dev/null 2>&1 && break
        sleep 0.25
    done
    if ! curl -sf "$api/healthz" >/dev/null 2>&1; then
        echo "FAIL: coordinator did not come up at $api" >&2
        return 1
    fi
    if ! curl -sf -H "$auth" "$api/api/v1/jobs" >/dev/null 2>&1; then
        echo "FAIL: $api answers /healthz but rejects our API token —" \
             "another coordinator is on this port" >&2
        return 1
    fi
}
