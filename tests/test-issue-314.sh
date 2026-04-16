#!/usr/bin/env bash
# tests/test-issue-314.sh — Verify Raft snapshot compaction actually fires.
#
# Uses NAUKA_SNAPSHOT_THRESHOLD=3 so the init+join sequence (which produces
# ~5 log entries) reliably crosses the threshold and triggers a snapshot.
# Then asserts the `raft: built snapshot` tracing event appears in logs.
set -euo pipefail

NAUKA_BIN="${NAUKA_BIN:-target/x86_64-unknown-linux-musl/release/nauka}"
SSH_KEY_NAME="${SSH_KEY_NAME:-nauka-agent-local}"
SERVER_TYPE="${SERVER_TYPE:-cpx22}"
LOCATION="${LOCATION:-fsn1}"
IMAGE="${IMAGE:-ubuntu-24.04}"

RUN_DIR="/tmp/nauka-issue-314/$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$RUN_DIR"

[[ -x $NAUKA_BIN ]] || { echo "✗ NAUKA_BIN not executable" >&2; exit 1; }

NAMES=(nauka-dev-1 nauka-dev-2)
IPS=("" "")

log()  { printf "\033[36m[%s] %s\033[0m\n" "$(date -u +%H:%M:%S)" "$*"; }
ok()   { printf "\033[32m✓ %s\033[0m\n" "$*"; }
fail() { printf "\033[31m✗ %s\033[0m\n" "$*" >&2; }
die()  { fail "$*"; exit 1; }

cleanup() {
    local rc=$?
    if [[ ${KEEP_SERVERS:-0} == 1 && $rc -eq 0 ]]; then
        log "KEEP_SERVERS=1 — leaving servers"
        return
    fi
    log "tearing down..."
    for n in "${NAMES[@]}"; do hcloud server delete "$n" >/dev/null 2>&1 || true; done
    [[ $rc -ne 0 ]] && fail "FAILED — logs in $RUN_DIR"
}
trap cleanup EXIT

ssh_node() {
    local ip=$1; shift
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        -o LogLevel=ERROR -o ConnectTimeout=10 "root@$ip" "$@"
}
scp_to() {
    local ip=$1 src=$2 dst=$3
    scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        -o LogLevel=ERROR "$src" "root@$ip:$dst"
}
wait_ssh() {
    local ip=$1
    for _ in $(seq 1 60); do ssh_node "$ip" true 2>/dev/null && return 0; sleep 4; done
    die "SSH never came up on $ip"
}
wait_port() {
    local ip=$1 port=$2
    for _ in $(seq 1 30); do
        ssh_node "$ip" "ss -tln | grep -q ':$port '" 2>/dev/null && return 0
        sleep 2
    done
    return 1
}

# ─── Wipe + Provision ────────────────────────────────────────────────
for n in "${NAMES[@]}"; do hcloud server delete "$n" >/dev/null 2>&1 || true; done
log "▶ provisioning 2 servers"
for i in "${!NAMES[@]}"; do
    out=$(hcloud server create --name "${NAMES[$i]}" --type "$SERVER_TYPE" --image "$IMAGE" \
        --location "$LOCATION" --ssh-key "$SSH_KEY_NAME" --output json 2>/dev/null)
    IPS[$i]=$(echo "$out" | jq -r '.server.public_net.ipv4.ip // empty')
    [[ -n ${IPS[$i]} ]] || die "${NAMES[$i]}: no IPv4"
    log "    ${NAMES[$i]} → ${IPS[$i]}"
done
for ip in "${IPS[@]}"; do wait_ssh "$ip"; done
ok "provisioned"

log "▶ deploying binary"
for ip in "${IPS[@]}"; do
    scp_to "$ip" "$NAUKA_BIN" /usr/local/bin/nauka >/dev/null
    ssh_node "$ip" 'chmod +x /usr/local/bin/nauka'
done
ok "deployed"

NODE1=${IPS[0]}
NODE2=${IPS[1]}

# ═══════════════════════════════════════════════════════════════════
# Run with a low snapshot threshold so the init+join sequence alone
# crosses it and forces the snapshot + log purge path to exercise.
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ init + join with NAUKA_SNAPSHOT_THRESHOLD=3 ═══"
ssh_node "$NODE1" 'NAUKA_SNAPSHOT_THRESHOLD=3 setsid nauka mesh up </dev/null >/tmp/nauka.log 2>&1 &'
wait_port "$NODE1" 51821 || die "peering never started"
PIN=$(ssh_node "$NODE1" "grep -oP 'join pin:\s+\K\S+' /tmp/nauka.log" 2>/dev/null || true)
[[ -n $PIN ]] || die "no PIN"
ok "node-1 up (pin: $PIN)"

ssh_node "$NODE2" "NAUKA_SNAPSHOT_THRESHOLD=3 setsid nauka mesh join $NODE1 --pin '$PIN' </dev/null >/tmp/nauka.log 2>&1 &"
wait_port "$NODE2" 4001 || die "node-2 raft never started"
ok "node-2 joined"

# Give Raft time to replicate, apply, build the snapshot and purge logs.
sleep 10

# ═══════════════════════════════════════════════════════════════════
# Pull the snapshot tracing event from the leader.
# ═══════════════════════════════════════════════════════════════════
log "▶ checking for 'raft: built snapshot' tracing events"
SNAP_LINE=$(ssh_node "$NODE1" 'grep -h "raft: built snapshot" /tmp/nauka.log 2>/dev/null | head -1' || true)
if [[ -z $SNAP_LINE ]]; then
    ssh_node "$NODE1" 'cat /tmp/nauka.log' > "$RUN_DIR/node-1.log" 2>/dev/null || true
    die "no 'raft: built snapshot' in node-1 log — compaction didn't fire"
fi
ok "  snapshot fired: $SNAP_LINE"

log "▶ checking for 'raft: purged log entries' tracing events"
PURGE_LINE=$(ssh_node "$NODE1" 'grep -h "raft: purged log entries" /tmp/nauka.log 2>/dev/null | head -1' || true)
if [[ -z $PURGE_LINE ]]; then
    ssh_node "$NODE1" 'cat /tmp/nauka.log' > "$RUN_DIR/node-1.log" 2>/dev/null || true
    die "no 'raft: purged log entries' in node-1 log — purge didn't fire"
fi
ok "  purge fired: $PURGE_LINE"

# ─── Collect logs ────────────────────────────────────────────────────
for i in 0 1; do
    ip=${IPS[$i]}
    mkdir -p "$RUN_DIR/node-$((i+1))"
    ssh_node "$ip" 'cat /tmp/nauka.log' > "$RUN_DIR/node-$((i+1))/daemon.log" 2>/dev/null || true
done

echo ""
ok "═══ ALL CHECKS PASSED ═══"
ok "  NAUKA_SNAPSHOT_THRESHOLD=3 triggers snapshot + purge on Hetzner"
ok "  logs: $RUN_DIR"
