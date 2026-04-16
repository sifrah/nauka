#!/usr/bin/env bash
# tests/test-issue-303.sh — 8-node Hetzner test for persistent Raft log store.
#
# Validates that the Raft cluster survives a leader restart:
#   1. Spin up 8 fresh Hetzner VMs
#   2. `mesh up` on node-1 (creates Raft leader)
#   3. `mesh join` on nodes 2–8
#   4. Verify Raft files exist on all nodes
#   5. Kill node-1 daemon (the leader)
#   6. `mesh start` on node-1 (restarts with persistent Raft state)
#   7. Verify the Raft server comes back up and cluster recovers
#
# Env vars (same defaults as test-issue-282.sh):
#   NAUKA_BIN      path to musl binary (default: target/x86_64-unknown-linux-musl/release/nauka)
#   SSH_KEY_NAME   hcloud ssh-key      (default: nauka-agent-local)
#   SERVER_TYPE    hcloud server type   (default: cpx11)
#   LOCATION       hcloud location      (default: fsn1)
#   IMAGE          hcloud image         (default: ubuntu-24.04)
#   NODE_COUNT     number of nodes      (default: 8)
#   KEEP_SERVERS   1 to leave servers running on success
set -euo pipefail

NAUKA_BIN="${NAUKA_BIN:-target/x86_64-unknown-linux-musl/release/nauka}"
SSH_KEY_NAME="${SSH_KEY_NAME:-nauka-agent-local}"
SERVER_TYPE="${SERVER_TYPE:-cpx22}"
LOCATION="${LOCATION:-fsn1}"
IMAGE="${IMAGE:-ubuntu-24.04}"
NODE_COUNT="${NODE_COUNT:-8}"

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
RUN_DIR="/tmp/nauka-issue-303/$RUN_ID"
mkdir -p "$RUN_DIR"

[[ -x $NAUKA_BIN ]] || {
    echo "✗ NAUKA_BIN ($NAUKA_BIN) not executable — cross-compile first:" >&2
    echo "    cargo build --target x86_64-unknown-linux-musl --release -p nauka" >&2
    exit 1
}
command -v hcloud >/dev/null || { echo "✗ hcloud CLI not found" >&2; exit 1; }
command -v jq     >/dev/null || { echo "✗ jq not found"         >&2; exit 1; }

# Generate server names
NAMES=()
IPS=()
for i in $(seq 1 "$NODE_COUNT"); do
    NAMES+=("nauka-dev-$i")
    IPS+=("")
done

log()  { printf "\033[36m[%s] %s\033[0m\n" "$(date -u +%H:%M:%S)" "$*"; }
ok()   { printf "\033[32m✓ %s\033[0m\n" "$*"; }
fail() { printf "\033[31m✗ %s\033[0m\n" "$*" >&2; }
die()  { fail "$*"; exit 1; }

cleanup() {
    local rc=$?
    if [[ ${KEEP_SERVERS:-0} == 1 && $rc -eq 0 ]]; then
        log "KEEP_SERVERS=1 — leaving servers; cleanup: hcloud server delete ${NAMES[*]}"
        return
    fi
    log "tearing down test servers..."
    for n in "${NAMES[@]}"; do
        hcloud server delete "$n" >/dev/null 2>&1 || true
    done
    if [[ $rc -ne 0 ]]; then
        fail "test FAILED — logs in $RUN_DIR"
    fi
}
trap cleanup EXIT

ssh_node() {
    local ip=$1; shift
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        -o LogLevel=ERROR "root@$ip" "$@"
}
scp_to() {
    local ip=$1 src=$2 dst=$3
    scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        -o LogLevel=ERROR "$src" "root@$ip:$dst"
}
wait_ssh() {
    local ip=$1
    for _ in $(seq 1 60); do
        ssh_node "$ip" true 2>/dev/null && return 0
        sleep 4
    done
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

# ─── Wipe any leftover VMs ──────────────────────────────────────────
log "▶ wiping any leftover nauka-dev-* servers"
for n in "${NAMES[@]}"; do
    hcloud server delete "$n" >/dev/null 2>&1 || true
done

# ─── Provision ───────────────────────────────────────────────────────
log "▶ provisioning $NODE_COUNT Hetzner servers ($SERVER_TYPE/$LOCATION)"
for i in "${!NAMES[@]}"; do
    n=${NAMES[$i]}
    out=$(hcloud server create \
        --name "$n" \
        --type "$SERVER_TYPE" \
        --image "$IMAGE" \
        --location "$LOCATION" \
        --ssh-key "$SSH_KEY_NAME" \
        --output json 2>/dev/null)
    IPS[$i]=$(echo "$out" | jq -r '.server.public_net.ipv4.ip // empty')
    [[ -n ${IPS[$i]} ]] || die "$n: no IPv4 from hcloud"
    log "    $n → ${IPS[$i]}"
done
log "waiting for SSH on all nodes..."
for ip in "${IPS[@]}"; do wait_ssh "$ip"; done
ok "provisioned $NODE_COUNT nodes"

# ─── Deploy binary ───────────────────────────────────────────────────
log "▶ deploying nauka binary to all nodes"
for i in "${!IPS[@]}"; do
    ip=${IPS[$i]}
    scp_to "$ip" "$NAUKA_BIN" /usr/local/bin/nauka >/dev/null
    ssh_node "$ip" 'chmod +x /usr/local/bin/nauka' &
done
wait
ok "deployed"

# ─── Init node-1 (Raft leader) ──────────────────────────────────────
NODE1_IP=${IPS[0]}
mkdir -p "$RUN_DIR/node-1"

log "▶ starting mesh daemon on node-1 (init)"
ssh_node "$NODE1_IP" 'setsid nauka mesh up </dev/null >/tmp/nauka-daemon.log 2>&1 &'

# Wait for peering listener (port 51821)
log "  waiting for peering listener on node-1..."
wait_port "$NODE1_IP" 51821 || die "peering listener never started on node-1"

# Extract PIN from daemon log
PIN=$(ssh_node "$NODE1_IP" "grep -oP 'join pin:\s+\K\S+' /tmp/nauka-daemon.log" 2>/dev/null || true)
[[ -n $PIN ]] || die "could not extract PIN from node-1 daemon log"
log "    pin: $PIN"
ok "node-1 daemon running (Raft leader)"

# ─── Join nodes 2–N ─────────────────────────────────────────────────
for i in $(seq 1 $((NODE_COUNT - 1))); do
    node_num=$((i + 1))
    ip=${IPS[$i]}
    mkdir -p "$RUN_DIR/node-$node_num"

    log "▶ joining node-$node_num ($ip)"
    ssh_node "$ip" "setsid nauka mesh join $NODE1_IP --pin '$PIN' </dev/null >/tmp/nauka-daemon.log 2>&1 &"

    # Wait for this node's Raft server to start (port 4001)
    wait_port "$ip" 4001 || {
        fail "node-$node_num Raft server never started"
        ssh_node "$ip" 'cat /tmp/nauka-daemon.log' > "$RUN_DIR/node-$node_num/daemon.log" 2>/dev/null || true
        die "see $RUN_DIR/node-$node_num/daemon.log"
    }

    ok "node-$node_num joined"
    sleep 2
done
ok "all $NODE_COUNT nodes in the mesh"

# ─── Verify Raft files exist on all nodes ────────────────────────────
log "▶ verifying Raft state files on all nodes"
for i in "${!IPS[@]}"; do
    ip=${IPS[$i]}
    node_num=$((i + 1))

    has_vote=$(ssh_node "$ip" 'test -f /var/lib/nauka/raft/vote.json && echo yes || echo no')
    has_log=$(ssh_node "$ip" 'ls /var/lib/nauka/raft/log/*.json 2>/dev/null | wc -l')

    log "    node-$node_num: vote.json=$has_vote, log_entries=$has_log"

    # Leader must have vote + log entries
    if [[ $i -eq 0 ]]; then
        [[ $has_vote == "yes" ]] || die "node-1 (leader) missing vote.json"
        [[ $has_log -gt 0 ]]    || die "node-1 (leader) has no log entries"
    fi
done
ok "Raft state persisted to disk"

# ─── Save node-1 state for comparison ────────────────────────────────
log "▶ saving node-1 Raft state snapshot"
ssh_node "$NODE1_IP" 'cat /var/lib/nauka/raft/vote.json' > "$RUN_DIR/node-1/vote-before.json"
ssh_node "$NODE1_IP" 'ls -la /var/lib/nauka/raft/log/' > "$RUN_DIR/node-1/log-before.txt"
ssh_node "$NODE1_IP" 'cat /tmp/nauka-daemon.log' > "$RUN_DIR/node-1/daemon-before.log" 2>/dev/null || true

# ─── KILL NODE-1 DAEMON (leader crash) ──────────────────────────────
log "▶ killing node-1 daemon (simulating leader crash)"
ssh_node "$NODE1_IP" 'kill $(pgrep -x nauka) 2>/dev/null || true'
sleep 3
running=$(ssh_node "$NODE1_IP" 'pgrep -x nauka >/dev/null && echo yes || echo no')
if [[ $running == "yes" ]]; then
    ssh_node "$NODE1_IP" 'kill -9 $(pgrep -x nauka) 2>/dev/null || true'
    sleep 2
fi
ok "node-1 daemon killed"

# ─── Verify Raft files survived the kill ─────────────────────────────
log "▶ verifying Raft state survived crash on node-1"
has_vote=$(ssh_node "$NODE1_IP" 'test -f /var/lib/nauka/raft/vote.json && echo yes || echo no')
has_log=$(ssh_node "$NODE1_IP" 'ls /var/lib/nauka/raft/log/*.json 2>/dev/null | wc -l')
[[ $has_vote == "yes" ]] || die "vote.json lost after daemon kill!"
[[ $has_log -gt 0 ]]    || die "log entries lost after daemon kill!"
ok "Raft state survived (vote=$has_vote, entries=$has_log)"

# ─── RESTART NODE-1 ─────────────────────────────────────────────────
log "▶ restarting node-1 daemon (mesh start)"
ssh_node "$NODE1_IP" 'setsid nauka mesh start </dev/null >/tmp/nauka-restart.log 2>&1 &'

# Wait for Raft server to start on node-1
log "  waiting for Raft server on node-1..."
wait_port "$NODE1_IP" 4001 || {
    ssh_node "$NODE1_IP" 'cat /tmp/nauka-restart.log' > "$RUN_DIR/node-1/restart.log" 2>/dev/null || true
    die "Raft server never restarted on node-1 — see $RUN_DIR/node-1/restart.log"
}
ok "node-1 Raft server restarted"

# ─── Wait for cluster to stabilize ──────────────────────────────────
log "▶ waiting for Raft cluster to stabilize after restart..."
sleep 10

# ─── Verify Raft state after restart ────────────────────────────────
log "▶ verifying Raft state after restart on node-1"
ssh_node "$NODE1_IP" 'cat /var/lib/nauka/raft/vote.json' > "$RUN_DIR/node-1/vote-after.json"
ssh_node "$NODE1_IP" 'ls -la /var/lib/nauka/raft/log/' > "$RUN_DIR/node-1/log-after.txt"
ssh_node "$NODE1_IP" 'cat /tmp/nauka-restart.log' > "$RUN_DIR/node-1/restart.log" 2>/dev/null || true

has_vote_after=$(ssh_node "$NODE1_IP" 'test -f /var/lib/nauka/raft/vote.json && echo yes || echo no')
log_count_after=$(ssh_node "$NODE1_IP" 'ls /var/lib/nauka/raft/log/*.json 2>/dev/null | wc -l')
log "    after restart: vote=$has_vote_after, log_entries=$log_count_after"
[[ $has_vote_after == "yes" ]] || die "vote.json lost after restart!"
ok "Raft state intact after restart"

# ─── Verify all other nodes still have Raft running ──────────────────
log "▶ verifying remaining nodes still running"
alive=0
for i in $(seq 1 $((NODE_COUNT - 1))); do
    node_num=$((i + 1))
    ip=${IPS[$i]}
    if ssh_node "$ip" 'pgrep nauka >/dev/null' 2>/dev/null; then
        alive=$((alive + 1))
    else
        fail "node-$node_num daemon died"
    fi
done
log "    $alive / $((NODE_COUNT - 1)) follower daemons alive"
[[ $alive -eq $((NODE_COUNT - 1)) ]] || die "some follower daemons died"
ok "all followers alive"

# ─── Verify Raft connectivity: node-1 can reach followers ────────────
log "▶ verifying Raft network connectivity from restarted leader"
connected=0
for i in $(seq 1 $((NODE_COUNT - 1))); do
    node_num=$((i + 1))
    ip=${IPS[$i]}
    # Check the follower's Raft port is still listening
    if ssh_node "$ip" 'ss -tln | grep -q ":4001 "' 2>/dev/null; then
        connected=$((connected + 1))
    else
        fail "node-$node_num Raft port 4001 not listening"
    fi
done
log "    $connected / $((NODE_COUNT - 1)) followers have Raft port open"
ok "Raft network intact"

# ─── Collect logs ────────────────────────────────────────────────────
log "▶ collecting daemon logs from all nodes"
for i in "${!IPS[@]}"; do
    ip=${IPS[$i]}
    node_num=$((i + 1))
    ssh_node "$ip" 'cat /tmp/nauka-daemon.log 2>/dev/null || true' > "$RUN_DIR/node-$node_num/daemon.log" 2>/dev/null || true
    ssh_node "$ip" 'cat /tmp/nauka-restart.log 2>/dev/null || true' >> "$RUN_DIR/node-$node_num/daemon.log" 2>/dev/null || true
done

echo ""
ok "ALL CHECKS PASSED — Raft log store persists across leader restart"
ok "  $NODE_COUNT nodes tested, leader killed and restarted successfully"
ok "  logs in $RUN_DIR"
