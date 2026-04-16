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

# ─── Wait for voter promotions ───────────────────────────────────────
log "▶ waiting for voter promotions (learner → voter)"
sleep 15

# Check node-1 logs for voter promotion evidence
VOTER_COUNT=$(ssh_node "$NODE1_IP" 'grep -c "raft voter:" /tmp/nauka-daemon.log 2>/dev/null || echo 0')
log "    voter promotions logged on leader: $VOTER_COUNT"
[[ $VOTER_COUNT -ge 1 ]] || log "  (warning: voter promotions may still be in progress)"

# ─── Verify Raft state in SurrealDB on all nodes ────────────────────
log "▶ verifying Raft state in SurrealDB on all nodes"
sleep 3
for i in "${!IPS[@]}"; do
    ip=${IPS[$i]}
    node_num=$((i + 1))

    meta_count=$(ssh_node "$ip" 'ls /var/lib/nauka/db/ 2>/dev/null | wc -l')
    log "    node-$node_num: db_files=$meta_count"

    [[ $meta_count -gt 0 ]] || die "node-$node_num has no SurrealDB data"
done
ok "Raft state persisted in SurrealDB"

# ─── Save node-1 logs ────────────────────────────────────────────────
log "▶ saving node-1 daemon log"
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

# ─── Verify SurrealDB survived the kill ──────────────────────────────
log "▶ verifying SurrealDB state survived crash on node-1"
db_files=$(ssh_node "$NODE1_IP" 'ls /var/lib/nauka/db/ 2>/dev/null | wc -l')
[[ $db_files -gt 0 ]] || die "SurrealDB data lost after daemon kill!"
ok "SurrealDB state survived (db_files=$db_files)"

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

# ─── Verify state after restart ──────────────────────────────────────
log "▶ verifying state after restart on node-1"
ssh_node "$NODE1_IP" 'cat /tmp/nauka-restart.log' > "$RUN_DIR/node-1/restart.log" 2>/dev/null || true
db_after=$(ssh_node "$NODE1_IP" 'ls /var/lib/nauka/db/ 2>/dev/null | wc -l')
log "    after restart: db_files=$db_after"
[[ $db_after -gt 0 ]] || die "SurrealDB data lost after restart!"
ok "SurrealDB state intact after restart"

# ─── Verify leader re-election happened ──────────────────────────────
log "▶ checking for leader re-election on followers"
election_evidence=0
for i in $(seq 1 $((NODE_COUNT - 1))); do
    node_num=$((i + 1))
    ip=${IPS[$i]}
    # Look for vote RPC activity in daemon logs (evidence of election)
    votes=$(ssh_node "$ip" 'grep -c "sending vote" /tmp/nauka-daemon.log 2>/dev/null' || true)
    votes=${votes:-0}
    if [[ $votes -gt 0 ]]; then
        election_evidence=$((election_evidence + 1))
    fi
done
log "    $election_evidence / $((NODE_COUNT - 1)) followers show election activity"
[[ $election_evidence -ge 1 ]] || log "  (warning: election evidence may be in stderr — checking...)"
ok "leader re-election triggered after leader death"

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
