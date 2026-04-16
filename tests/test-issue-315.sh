#!/usr/bin/env bash
# tests/test-issue-315.sh — Verify restart actively refreshes a stale endpoint.
#
# Hetzner's primary IP can't be swapped under a live VM without significant
# ceremony, so instead we simulate the "my IP changed while I was down"
# scenario by corrupting the cluster's stored view of a node's endpoint,
# then restarting the node and asserting the refresh task puts it back.
#
# 1. 2 nodes — init + join, cluster healthy
# 2. From node-1, write a bogus endpoint for node-2 via debug raft-write
# 3. Confirm node-1's reconciler picked up the bogus endpoint on WG
# 4. Restart node-2 — its refresh_own_endpoint task should whoami node-1,
#    see the DB has the wrong endpoint, and UPDATE back to the real one
# 5. Confirm node-1's WG peer for node-2 is back to the real endpoint
set -euo pipefail

NAUKA_BIN="${NAUKA_BIN:-target/x86_64-unknown-linux-musl/release/nauka}"
SSH_KEY_NAME="${SSH_KEY_NAME:-nauka-agent-local}"
SERVER_TYPE="${SERVER_TYPE:-cpx22}"
LOCATION="${LOCATION:-fsn1}"
IMAGE="${IMAGE:-ubuntu-24.04}"

RUN_DIR="/tmp/nauka-issue-315/$(date -u +%Y%m%dT%H%M%SZ)-$$"
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
# Init + join
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ bring up 2-node mesh ═══"
ssh_node "$NODE1" 'setsid nauka mesh up </dev/null >/tmp/nauka.log 2>&1 &'
wait_port "$NODE1" 51821 || die "peering never started"
PIN=$(ssh_node "$NODE1" "grep -oP 'join pin:\s+\K\S+' /tmp/nauka.log" 2>/dev/null || true)
[[ -n $PIN ]] || die "no PIN"
ok "node-1 up (pin: $PIN)"

ssh_node "$NODE2" "setsid nauka mesh join $NODE1 --pin '$PIN' </dev/null >/tmp/nauka.log 2>&1 &"
wait_port "$NODE2" 4001 || die "node-2 raft never started"
ok "node-2 joined"

# Wait for voter promotion so raft.write from node-1 will commit.
sleep 15
ssh_node "$NODE1" 'grep -q "raft voter:" /tmp/nauka.log' || die "node-2 never promoted to voter"
ok "node-2 promoted to voter"

# Grab node-2's public key for the poison query.
NODE2_PK=$(ssh_node "$NODE2" "grep -oP 'public key:\s+\K\S+' /tmp/nauka.log | head -1")
[[ -n $NODE2_PK ]] || die "could not read node-2 pubkey"
log "  node-2 pubkey: $NODE2_PK"
log "  real node-2 endpoint: $NODE2:51820"

# ═══════════════════════════════════════════════════════════════════
# Poison node-2's endpoint, THEN kill node-2 immediately so its
# keepalives stop flowing — otherwise WireGuard's endpoint roaming
# would re-learn the real IP from the inbound packets and overwrite
# whatever the reconciler just configured.
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ poison node-2's endpoint to 9.9.9.9:51820 ═══"
BOGUS="9.9.9.9:51820"
ssh_node "$NODE1" "nauka mesh debug raft-write \"UPDATE hypervisor SET endpoint = '$BOGUS' WHERE public_key = '$NODE2_PK'\"" \
    || die "debug raft-write failed"
ok "bogus endpoint written via Raft"

# Kill node-2 right away; WG on node-1 will stop receiving packets from
# the real IP and keep whatever endpoint the reconciler installs.
ssh_node "$NODE2" 'kill $(pgrep -x nauka) 2>/dev/null || true'
ok "node-2 stopped (no more keepalives to node-1)"

# Let reconciler on node-1 pick up the bogus endpoint (5s cycle).
sleep 12
log "▶ confirming node-1's WG view of node-2 is now bogus"
PEER_EP=$(ssh_node "$NODE1" 'nauka mesh status 2>/dev/null | tr -d "\n" | grep -oP "endpoint: Some\(\s*\K[^,)]+" | head -1' || true)
log "    node-1 sees node-2 at: $PEER_EP"
if [[ $PEER_EP != "$BOGUS" ]]; then
    log "DIAG: last 20 lines of node-1 daemon log:"
    ssh_node "$NODE1" 'tail -20 /tmp/nauka.log' || true
    die "reconciler didn't pick up bogus endpoint (got: $PEER_EP)"
fi
ok "node-1's WG now targets $BOGUS for node-2 (cluster state is poisoned)"

# ═══════════════════════════════════════════════════════════════════
# Restart node-2 — refresh_own_endpoint should correct the cluster view
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ restart node-2 ═══"
ssh_node "$NODE2" 'setsid nauka mesh start </dev/null >/tmp/nauka-restart.log 2>&1 &'
wait_port "$NODE2" 4001 || die "node-2 raft port not back after restart"
ok "node-2 restarted"

# The refresh task sleeps 3s then runs once; give Raft apply + reconciler
# cycle time on top of that.
log "  waiting 20s for refresh + Raft replication + reconciler..."
sleep 20

# ═══════════════════════════════════════════════════════════════════
# Verify the correct endpoint was restored
# ═══════════════════════════════════════════════════════════════════
log "▶ checking node-1's WG view of node-2 is back to the real IP"
PEER_EP2=$(ssh_node "$NODE1" 'nauka mesh status 2>/dev/null | tr -d "\n" | grep -oP "endpoint: Some\(\s*\K[^,)]+" | head -1' || true)
log "    node-1 sees node-2 at: $PEER_EP2"
EXPECTED="$NODE2:51820"
if [[ $PEER_EP2 != "$EXPECTED" ]]; then
    log "DIAG: raw 'nauka mesh status' on node-1:"
    ssh_node "$NODE1" 'nauka mesh status 2>&1' || true
    log "DIAG: node-2 refresh log:"
    ssh_node "$NODE2" 'grep -E "endpoint refresh|sm: apply|reconciler" /tmp/nauka-restart.log 2>/dev/null | head -30' || true
    log "DIAG: node-1 recent log:"
    ssh_node "$NODE1" 'grep -E "endpoint refresh|sm: apply|reconciler" /tmp/nauka.log 2>/dev/null | tail -20' || true
    die "endpoint not restored (got: $PEER_EP2, want: $EXPECTED)"
fi
ok "endpoint restored — restart detected the mismatch and UPDATEd via Raft"

# Also confirm node-2's refresh task actively propagated the correction.
ssh_node "$NODE2" 'grep -q "endpoint refresh: propagated via Raft" /tmp/nauka-restart.log' \
    || die "node-2's refresh task didn't propagate via Raft"
ok "node-2's refresh task logged propagation via Raft"

# ─── Collect logs ────────────────────────────────────────────────────
for i in 0 1; do
    ip=${IPS[$i]}
    mkdir -p "$RUN_DIR/node-$((i+1))"
    ssh_node "$ip" 'cat /tmp/nauka.log /tmp/nauka-restart.log 2>/dev/null' \
        > "$RUN_DIR/node-$((i+1))/daemon.log" 2>/dev/null || true
done

echo ""
ok "═══ ALL CHECKS PASSED ═══"
ok "  restart-time endpoint refresh corrects a stale cluster view"
ok "  logs: $RUN_DIR"
