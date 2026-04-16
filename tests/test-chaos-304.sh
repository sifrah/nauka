#!/usr/bin/env bash
# tests/test-chaos-304.sh — Chaos test for Raft voter promotion.
#
# 1. 8 nodes — init + 7 joins + voter promotions
# 2. Kill 3 random non-leader nodes — cluster must survive (5/8 quorum)
# 3. Kill the leader — new leader must be elected (4/8, still majority of 5 remaining)
# 4. Reboot killed nodes — they must rejoin the cluster
set -euo pipefail

NAUKA_BIN="${NAUKA_BIN:-target/x86_64-unknown-linux-musl/release/nauka}"
SSH_KEY_NAME="${SSH_KEY_NAME:-nauka-agent-local}"
SERVER_TYPE="${SERVER_TYPE:-cpx22}"
LOCATION="${LOCATION:-fsn1}"
IMAGE="${IMAGE:-ubuntu-24.04}"
NODE_COUNT=8

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
RUN_DIR="/tmp/nauka-chaos-304/$RUN_ID"
mkdir -p "$RUN_DIR"

[[ -x $NAUKA_BIN ]] || { echo "✗ NAUKA_BIN not executable" >&2; exit 1; }
command -v hcloud >/dev/null || { echo "✗ hcloud not found" >&2; exit 1; }
command -v jq     >/dev/null || { echo "✗ jq not found"     >&2; exit 1; }

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
node_alive() {
    local ip=$1
    ssh_node "$ip" 'pgrep -x nauka >/dev/null' 2>/dev/null
}

# ─── Wipe ────────────────────────────────────────────────────────────
log "▶ wiping leftover nauka-dev-* servers"
for n in "${NAMES[@]}"; do hcloud server delete "$n" >/dev/null 2>&1 || true; done

# ─── Provision ───────────────────────────────────────────────────────
log "▶ provisioning $NODE_COUNT servers"
for i in "${!NAMES[@]}"; do
    n=${NAMES[$i]}
    out=$(hcloud server create --name "$n" --type "$SERVER_TYPE" --image "$IMAGE" \
        --location "$LOCATION" --ssh-key "$SSH_KEY_NAME" --output json 2>/dev/null)
    IPS[$i]=$(echo "$out" | jq -r '.server.public_net.ipv4.ip // empty')
    [[ -n ${IPS[$i]} ]] || die "$n: no IPv4"
    log "    $n → ${IPS[$i]}"
done
for ip in "${IPS[@]}"; do wait_ssh "$ip"; done
ok "provisioned"

# ─── Deploy ──────────────────────────────────────────────────────────
log "▶ deploying binary"
for ip in "${IPS[@]}"; do
    scp_to "$ip" "$NAUKA_BIN" /usr/local/bin/nauka >/dev/null &
done
wait
for ip in "${IPS[@]}"; do ssh_node "$ip" 'chmod +x /usr/local/bin/nauka'; done
ok "deployed"

# ─── Init + Join ─────────────────────────────────────────────────────
NODE1_IP=${IPS[0]}
log "▶ init node-1"
ssh_node "$NODE1_IP" 'setsid nauka mesh up </dev/null >/tmp/nauka.log 2>&1 &'
wait_port "$NODE1_IP" 51821 || die "peering never started"
PIN=$(ssh_node "$NODE1_IP" "grep -oP 'join pin:\s+\K\S+' /tmp/nauka.log" 2>/dev/null || true)
[[ -n $PIN ]] || die "no PIN"
ok "node-1 up (pin: $PIN)"

for i in $(seq 1 $((NODE_COUNT - 1))); do
    ip=${IPS[$i]}
    log "▶ join node-$((i+1))"
    ssh_node "$ip" "setsid nauka mesh join $NODE1_IP --pin '$PIN' </dev/null >/tmp/nauka.log 2>&1 &"
    wait_port "$ip" 4001 || die "node-$((i+1)) raft never started"
    ok "node-$((i+1)) joined"
    sleep 2
done
ok "all $NODE_COUNT nodes in mesh"

# ─── Wait for voter promotions ───────────────────────────────────────
log "▶ waiting 20s for voter promotions"
sleep 20
VOTERS=$(ssh_node "$NODE1_IP" 'grep -c "raft voter:" /tmp/nauka.log 2>/dev/null || echo 0')
log "    voter promotions: $VOTERS / $((NODE_COUNT - 1))"
[[ $VOTERS -ge $((NODE_COUNT - 1)) ]] || die "not all nodes promoted to voters ($VOTERS / $((NODE_COUNT - 1)))"
ok "all nodes are voters"

# ═══════════════════════════════════════════════════════════════════
# CHAOS PHASE 1: Kill 3 random followers
# With 8 voters, killing 3 leaves 5 — still a majority (5 > 8/2 = 4)
# ═══════════════════════════════════════════════════════════════════
KILL_INDICES=(2 5 7)  # 0-indexed: nodes 3, 6, 8
log ""
log "═══ CHAOS PHASE 1: killing 3 followers (nodes 3, 6, 8) ═══"
for ki in "${KILL_INDICES[@]}"; do
    ip=${IPS[$ki]}
    node_num=$((ki + 1))
    log "  killing node-$node_num ($ip)"
    ssh_node "$ip" 'kill $(pgrep -x nauka) 2>/dev/null || true'
done
sleep 5

alive_count=0
for i in "${!IPS[@]}"; do
    ip=${IPS[$i]}
    skip=false
    for ki in "${KILL_INDICES[@]}"; do [[ $i -eq $ki ]] && skip=true; done
    [[ $skip == true ]] && continue
    if node_alive "$ip"; then
        alive_count=$((alive_count + 1))
    else
        fail "node-$((i+1)) died unexpectedly"
    fi
done
log "    alive: $alive_count / 5 (expected)"
[[ $alive_count -eq 5 ]] || die "not enough nodes alive"
ok "cluster survived killing 3 nodes ($alive_count/5 alive)"

# Verify leader (node-1) raft port still open
wait_port "$NODE1_IP" 4001 || die "leader raft port down after killing followers"
ok "leader still serving Raft"

# ═══════════════════════════════════════════════════════════════════
# CHAOS PHASE 2: Kill the leader
# 5 nodes alive, kill leader → 4 remain. Quorum of 8 needs 5.
# 4 < 5 → cluster CANNOT elect a new leader with 8-voter config.
# But the remaining 4 nodes should stay alive and retry.
# When we restart killed nodes, quorum is restored.
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ CHAOS PHASE 2: killing leader (node-1) ═══"
ssh_node "$NODE1_IP" 'kill $(pgrep -x nauka) 2>/dev/null || true'
sleep 5

surviving=0
for i in "${!IPS[@]}"; do
    ip=${IPS[$i]}
    skip=false
    for ki in "${KILL_INDICES[@]}"; do [[ $i -eq $ki ]] && skip=true; done
    [[ $i -eq 0 ]] && skip=true  # leader
    [[ $skip == true ]] && continue
    if node_alive "$ip"; then
        surviving=$((surviving + 1))
    fi
done
log "    surviving followers: $surviving / 4"
[[ $surviving -eq 4 ]] || die "followers crashed after leader death"
ok "4 followers survived leader death"

# ═══════════════════════════════════════════════════════════════════
# CHAOS PHASE 3: Restart killed nodes
# Restart node-1 (leader) + nodes 3, 6, 8
# Once 5+ nodes are up, quorum is restored and a new leader elected
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ CHAOS PHASE 3: restarting killed nodes ═══"
sleep 5

# Restart the 3 killed followers first
for ki in "${KILL_INDICES[@]}"; do
    ip=${IPS[$ki]}
    node_num=$((ki + 1))
    log "  restarting node-$node_num ($ip)"
    ssh_node "$ip" 'setsid nauka mesh start </dev/null >/tmp/nauka-restart.log 2>&1 &'
done

# Restart leader
log "  restarting node-1 (was leader)"
ssh_node "$NODE1_IP" 'setsid nauka mesh start </dev/null >/tmp/nauka-restart.log 2>&1 &'

# Wait for raft ports
log "  waiting for Raft ports..."
sleep 10
raft_up=0
for i in "${!IPS[@]}"; do
    ip=${IPS[$i]}
    if wait_port "$ip" 4001; then
        raft_up=$((raft_up + 1))
    else
        fail "node-$((i+1)) raft port not open after restart"
    fi
done
log "    $raft_up / $NODE_COUNT nodes have Raft port open"
[[ $raft_up -eq $NODE_COUNT ]] || die "not all nodes recovered"
ok "all $NODE_COUNT nodes back online with Raft"

# Wait for election stabilization
log "  waiting 15s for cluster to stabilize..."
sleep 15

# Final check: all nodes alive
final_alive=0
for ip in "${IPS[@]}"; do
    if node_alive "$ip"; then
        final_alive=$((final_alive + 1))
    fi
done
log "    final alive: $final_alive / $NODE_COUNT"
[[ $final_alive -eq $NODE_COUNT ]] || die "some nodes died during recovery"
ok "cluster fully recovered — $final_alive/$NODE_COUNT nodes alive"

# ═══════════════════════════════════════════════════════════════════
# CHAOS PHASE 4: functional consensus check
# Prove the cluster can still do Raft writes post-recovery:
# 0. a new leader was elected post-restart (state-machine applies ran)
# 1. remove a peer via CLI (issues DELETE hypervisor through Raft)
# 2. verify every non-victim node's reconciler picks up the change
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ CHAOS PHASE 4: functional consensus check ═══"

# A new leader emits a blank entry on election; every node then applies it
# through its state machine. If no leader was elected, nothing applies and
# grep returns 0. Time-bound: by this point in the test we've already slept
# 15s for stabilization, so any missing applies here are a real failure.
log "  ▶ verifying a leader was elected after restart"
leader_elected=false
for ip in "${IPS[@]}"; do
    if ssh_node "$ip" "grep -q 'sm: apply' /tmp/nauka-restart.log 2>/dev/null"; then
        leader_elected=true
        break
    fi
done
$leader_elected \
    || die "no 'sm: apply' found on any node post-restart — no leader was elected within the stabilization window"
ok "  leader election confirmed (state-machine applied post-restart)"

VICTIM_IDX=$((NODE_COUNT - 1))
VICTIM_IP=${IPS[$VICTIM_IDX]}
VICTIM_PK=$(ssh_node "$VICTIM_IP" \
    "grep -hoP 'public key:\s+\K\S+' /tmp/nauka.log /tmp/nauka-restart.log 2>/dev/null | head -1")
[[ -n $VICTIM_PK ]] || die "could not read victim pubkey"
log "  victim: node-$((VICTIM_IDX + 1)) ($VICTIM_IP) pk=$VICTIM_PK"

# The peer-remove CLI talks to the local daemon, which forwards the write
# to the leader via the Raft RPC channel (#315). Any node should accept.
# We still probe each one so the test prints which node is the leader.
log "  ▶ nauka mesh peer remove (probing every node for the leader)"
remove_ok=false
for i in "${!IPS[@]}"; do
    [[ $i -eq $VICTIM_IDX ]] && continue
    ip=${IPS[$i]}
    wait_port "$ip" 51821 2>/dev/null || continue
    if ssh_node "$ip" "nauka mesh peer remove --public-key '$VICTIM_PK'" 2>&1 \
        | tee -a "$RUN_DIR/remove-attempts.log" \
        | grep -q '^peer removal requested'; then
        ok "  peer remove accepted by node-$((i + 1)) (leader)"
        remove_ok=true
        break
    fi
done
$remove_ok || die "no node accepted the peer-remove — Raft write rejected everywhere"

# Each node's reconciler polls every 5s — give 2 cycles for apply+reconcile
log "  waiting 12s for Raft apply + reconciler cycle..."
sleep 12

# Every non-victim node must have dropped the victim from its WG peer list.
# The victim still sees its old peers — with its own record deleted, its
# reconciler has nothing to diff against. Skipping the victim is intentional.
log "  ▶ checking WG peer counts on non-victim nodes"
expected=$((NODE_COUNT - 2))   # self already excluded; victim now excluded too
for i in "${!IPS[@]}"; do
    [[ $i -eq $VICTIM_IDX ]] && continue
    ip=${IPS[$i]}
    count=$(ssh_node "$ip" "nauka mesh status 2>/dev/null | grep -c ': Peer {'" \
        2>/dev/null || echo -1)
    log "    node-$((i + 1)): $count peers (want $expected)"
    [[ $count -eq $expected ]] || \
        die "node-$((i + 1)) still has $count peers after peer-remove (expected $expected)"
done
ok "post-recovery Raft write replicated to all $((NODE_COUNT - 1)) non-victim nodes"

# ─── Collect logs ────────────────────────────────────────────────────
log "▶ collecting logs"
for i in "${!IPS[@]}"; do
    ip=${IPS[$i]}
    mkdir -p "$RUN_DIR/node-$((i+1))"
    ssh_node "$ip" 'cat /tmp/nauka.log 2>/dev/null; cat /tmp/nauka-restart.log 2>/dev/null' \
        > "$RUN_DIR/node-$((i+1))/daemon.log" 2>/dev/null || true
done

echo ""
ok "═══ ALL CHAOS TESTS PASSED ═══"
ok "  8 nodes, killed 3 followers + leader, all recovered"
ok "  logs: $RUN_DIR"
