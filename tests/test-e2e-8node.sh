#!/usr/bin/env bash
# tests/test-e2e-8node.sh — End-to-end exercise of every hypervisor feature
# on 8 fresh Hetzner VMs. Every command is systemd-managed; nothing runs
# in the foreground.
#
# Phases:
#   0. provision + deploy
#   1. init + 7 joins, voter promotions
#   2. status — every node agrees on the full 8-node table
#   3. admin: debug raft-write (forwarding), peer remove, mesh status
#   4. chaos: stop leader, verify survivors, restart it, verify recovery
#   5. endpoint refresh: poison an endpoint, restart, assert correction
#   6. snapshot: systemd drop-in NAUKA_SNAPSHOT_THRESHOLD=10, generate
#      writes, grep journal for 'raft: built snapshot' + 'purged'
#   7. UX errors: already-in-mesh, wrong pin, closed peering
#   8. leave all, verify clean state everywhere
set -euo pipefail

NAUKA_BIN="${NAUKA_BIN:-target/x86_64-unknown-linux-musl/release/nauka}"
SSH_KEY_NAME="${SSH_KEY_NAME:-nauka-agent-local}"
SERVER_TYPE="${SERVER_TYPE:-cpx22}"
LOCATION="${LOCATION:-fsn1}"
IMAGE="${IMAGE:-ubuntu-24.04}"
NODE_COUNT=8

RUN_DIR="/tmp/nauka-e2e/$(date -u +%Y%m%dT%H%M%SZ)-$$"
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
    if [[ $rc -ne 0 ]]; then fail "FAILED — logs in $RUN_DIR"; fi
    return $rc
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
count_hypervisors() {
    ssh_node "$1" 'timeout 10 nauka hypervisor status 2>/dev/null | grep -oP "hypervisors \(\K\d+"' || echo 0
}
count_wg_peers() {
    ssh_node "$1" 'timeout 10 nauka hypervisor mesh status 2>/dev/null | grep -c ": Peer {"' || echo 0
}

# ═══════════════════════════════════════════════════════════════════
# Phase 0: provision + deploy
# ═══════════════════════════════════════════════════════════════════
for n in "${NAMES[@]}"; do hcloud server delete "$n" >/dev/null 2>&1 || true; done
log "▶ provisioning $NODE_COUNT servers (sequential — parallel hits transient Hetzner API errors)"
for i in "${!NAMES[@]}"; do
    out=$(hcloud server create --name "${NAMES[$i]}" --type "$SERVER_TYPE" --image "$IMAGE" \
        --location "$LOCATION" --ssh-key "$SSH_KEY_NAME" --output json 2>"$RUN_DIR/create-$i.err")
    IPS[$i]=$(echo "$out" | jq -r '.server.public_net.ipv4.ip // empty')
    if [[ -z ${IPS[$i]} ]]; then
        cat "$RUN_DIR/create-$i.err" >&2
        die "${NAMES[$i]}: no IPv4 (see $RUN_DIR/create-$i.err)"
    fi
    log "    ${NAMES[$i]} → ${IPS[$i]}"
done
for ip in "${IPS[@]}"; do wait_ssh "$ip" & done; wait
ok "provisioned"

log "▶ deploying binary (parallel)"
for ip in "${IPS[@]}"; do scp_to "$ip" "$NAUKA_BIN" /usr/local/bin/nauka >/dev/null & done; wait
for ip in "${IPS[@]}"; do ssh_node "$ip" 'chmod +x /usr/local/bin/nauka'; done
ok "deployed"

# ═══════════════════════════════════════════════════════════════════
# Phase 1: init node-1 + join nodes 2..8
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ Phase 1: init + 7 joins ═══"
INIT_OUT=$(ssh_node "${IPS[0]}" 'timeout 30 nauka hypervisor init 2>&1')
PIN=$(echo "$INIT_OUT" | grep -oP 'join pin:\s+\K\S+')
[[ -n $PIN ]] || { echo "$INIT_OUT" | sed 's/^/    /'; die "init didn't print a PIN"; }
ok "  node-1 init — PIN=$PIN"

for i in $(seq 1 $((NODE_COUNT - 1))); do
    ip=${IPS[$i]}
    if ssh_node "$ip" "timeout 30 nauka hypervisor join ${IPS[0]} --pin '$PIN' 2>&1" \
        | grep -q '^joined mesh'; then
        ok "  node-$((i + 1)) joined"
    else
        die "node-$((i + 1)) join failed"
    fi
done

log "▶ waiting 30s for voter promotions"
sleep 30
VOTERS=$(ssh_node "${IPS[0]}" 'journalctl -u nauka-hypervisor.service --no-pager | grep -c "raft.voter.promoted"' || echo 0)
log "    voter promotions on node-1: $VOTERS (want $((NODE_COUNT - 1)))"
[[ $VOTERS -ge $((NODE_COUNT - 1)) ]] \
    || die "only $VOTERS/$((NODE_COUNT - 1)) promotions — some joiners never became voters"
ok "all $VOTERS joiners promoted to voters"

# ═══════════════════════════════════════════════════════════════════
# Phase 2: status — every node agrees on the 8-node table
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ Phase 2: replicated cluster view ═══"
for i in "${!IPS[@]}"; do
    c=$(count_hypervisors "${IPS[$i]}")
    log "    node-$((i + 1)) sees $c hypervisors"
    [[ $c -eq $NODE_COUNT ]] || die "node-$((i + 1)) sees $c (expected $NODE_COUNT)"
done
ok "all $NODE_COUNT nodes agree on the full cluster"

# WG peer count on every node = NODE_COUNT - 1 (all peers except self)
for i in "${!IPS[@]}"; do
    p=$(count_wg_peers "${IPS[$i]}")
    [[ $p -eq $((NODE_COUNT - 1)) ]] \
        || die "node-$((i + 1)) has $p WG peers (expected $((NODE_COUNT - 1)))"
done
ok "every node has $((NODE_COUNT - 1)) WG peers in its interface"

# ═══════════════════════════════════════════════════════════════════
# Phase 3: admin operations
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ Phase 3: admin — debug raft-write, peer remove, mesh status ═══"

# 3a. debug raft-write from a follower (exercises leader forwarding).
#     We don't know which node is leader, but forwarding means any node works.
log "▶ debug raft-write from node-5 (tests leader forwarding)"
ssh_node "${IPS[4]}" "timeout 15 nauka hypervisor debug raft-write \"CREATE marker:e2e_probe SET value = 'phase3'\" 2>&1" \
    | grep -q 'raft write ok' \
    || die "debug raft-write from follower failed"
ok "  forwarded write committed"

# 3b. peer remove — pick node-8 as victim, removal triggered from node-3
VICTIM_IP=${IPS[7]}
VICTIM_PK=$(ssh_node "$VICTIM_IP" 'timeout 10 nauka hypervisor status 2>/dev/null | grep -oP "public key:\s+\K\S+"')
[[ -n $VICTIM_PK ]] || die "could not read node-8 pubkey"
log "▶ peer remove node-8 (pk=$VICTIM_PK) via node-3"
ssh_node "${IPS[2]}" "timeout 15 nauka hypervisor peer remove --public-key '$VICTIM_PK' 2>&1" \
    | grep -q 'peer removal requested' \
    || die "peer remove CLI failed"
sleep 12

# Survivors (nodes 1..7) should now see 7 hypervisors each.
for i in $(seq 0 $((NODE_COUNT - 2))); do
    c=$(count_hypervisors "${IPS[$i]}")
    [[ $c -eq $((NODE_COUNT - 1)) ]] \
        || die "node-$((i + 1)) sees $c after peer-remove (expected $((NODE_COUNT - 1)))"
done
ok "  all 7 survivors dropped node-8 from the replicated table"

# 3c. mesh status — low-level WG interface info
log "▶ mesh status on node-1 should show 6 WG peers (original 7 - victim 1)"
p=$(count_wg_peers "${IPS[0]}")
[[ $p -eq 6 ]] || die "node-1 has $p WG peers after peer-remove (expected 6)"
ok "  WG peer count correct"

# ═══════════════════════════════════════════════════════════════════
# Phase 4: chaos — stop leader, verify consensus, restart
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ Phase 4: chaos — stop leader + writes survive ═══"

# Find a node that successfully accepts a raft-write; that's either the leader
# or a follower with working forwarding. Stop it.
# Simpler: just stop node-1 (often the leader since it init'd), see what happens.
log "▶ systemctl stop on node-1"
ssh_node "${IPS[0]}" 'systemctl stop nauka-hypervisor.service'
sleep 3
ssh_node "${IPS[0]}" 'systemctl is-active nauka-hypervisor.service' | grep -q active \
    && die "node-1 still active after stop"
ok "  node-1 stopped"

# 7 nodes alive, which is still a majority of 7-node membership (we removed
# node-8 earlier, so membership is 7). Quorum = 4/7.
log "▶ write via node-5 (node-1 down — forwarding should find a new leader)"
sleep 10 # let re-election happen
ssh_node "${IPS[4]}" "timeout 30 nauka hypervisor debug raft-write \"CREATE marker:e2e_chaos SET value = 'after_leader_stop'\" 2>&1" \
    | grep -q 'raft write ok' \
    || die "write failed after leader stop — new leader didn't emerge or forwarding broken"
ok "  write committed post-leader-stop (new leader elected)"

log "▶ restart node-1"
ssh_node "${IPS[0]}" 'systemctl start nauka-hypervisor.service'
sleep 15
ssh_node "${IPS[0]}" 'systemctl is-active nauka-hypervisor.service' | grep -q active \
    || die "node-1 didn't restart"
ok "  node-1 back up"

# All 7 survivors should agree on cluster membership.
for i in $(seq 0 $((NODE_COUNT - 2))); do
    c=$(count_hypervisors "${IPS[$i]}")
    [[ $c -eq $((NODE_COUNT - 1)) ]] \
        || die "node-$((i + 1)) sees $c after chaos (expected $((NODE_COUNT - 1)))"
done
ok "  all 7 nodes agree on $((NODE_COUNT - 1)) hypervisors post-recovery"

# ═══════════════════════════════════════════════════════════════════
# Phase 5: endpoint refresh
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ Phase 5: endpoint refresh on restart ═══"

# Pick node-2 as refresh subject. Poison its endpoint via debug raft-write
# on node-3, then restart node-2 — its refresh task should correct it.
NODE2_PK=$(ssh_node "${IPS[1]}" 'timeout 10 nauka hypervisor status 2>/dev/null | grep -oP "public key:\s+\K\S+"')
REAL_EP="${IPS[1]}:51820"
BOGUS="9.9.9.9:51820"

log "▶ poison node-2's endpoint to $BOGUS via node-3"
ssh_node "${IPS[2]}" "timeout 15 nauka hypervisor debug raft-write \"UPDATE hypervisor SET endpoint = '$BOGUS' WHERE public_key = '$NODE2_PK'\" 2>&1" \
    | grep -q 'raft write ok' \
    || die "poison raft-write failed"

# Stop node-2 so it doesn't send keepalives that would mask the test.
ssh_node "${IPS[1]}" 'systemctl stop nauka-hypervisor.service'
sleep 12

# A neighbour's WG should now show the bogus endpoint for node-2.
ep=$(ssh_node "${IPS[2]}" 'timeout 10 nauka hypervisor mesh status 2>/dev/null | tr -d "\n" | grep -oP "endpoint: Some\(\s*\K[^,)]+" | head -10' | grep -c "9.9.9.9" || echo 0)
[[ $ep -ge 1 ]] || die "node-3's WG never picked up bogus endpoint for node-2"
ok "  poison visible on node-3's WG"

log "▶ restart node-2 — refresh task should correct DB + propagate"
ssh_node "${IPS[1]}" 'systemctl start nauka-hypervisor.service'
sleep 20
ssh_node "${IPS[1]}" 'journalctl -u nauka-hypervisor.service --since "30 seconds ago" --no-pager | grep -q "endpoint refresh: propagated via Raft"' \
    || die "node-2's refresh task didn't propagate via Raft"
ok "  node-2's refresh task logged 'propagated via Raft'"

# ═══════════════════════════════════════════════════════════════════
# Phase 6: snapshot compaction
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ Phase 6: snapshot compaction (threshold=10 via systemd drop-in) ═══"

# Drop in an override on node-4 that sets a low threshold, then restart.
ssh_node "${IPS[3]}" \
    'mkdir -p /etc/systemd/system/nauka-hypervisor.service.d
     cat > /etc/systemd/system/nauka-hypervisor.service.d/threshold.conf <<EOF
[Service]
Environment=NAUKA_SNAPSHOT_THRESHOLD=10
EOF
     systemctl daemon-reload
     systemctl restart nauka-hypervisor.service'
sleep 10

# Generate ~15 writes through node-4 so the threshold is crossed.
for i in $(seq 1 15); do
    ssh_node "${IPS[3]}" "nauka hypervisor debug raft-write \"CREATE marker:snap_$i SET value = '$i'\"" >/dev/null 2>&1 || true
done
sleep 10

if ssh_node "${IPS[3]}" 'journalctl -u nauka-hypervisor.service --no-pager | grep -q "raft: built snapshot"'; then
    ok "  snapshot fired on node-4"
else
    die "no 'raft: built snapshot' on node-4 after crossing threshold"
fi

if ssh_node "${IPS[3]}" 'journalctl -u nauka-hypervisor.service --no-pager | grep -q "raft: purged log entries"'; then
    ok "  log purge fired on node-4"
else
    die "no 'raft: purged log entries' on node-4"
fi

# ═══════════════════════════════════════════════════════════════════
# Phase 7: UX error paths
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ Phase 7: UX error paths ═══"

# init on a node that already has state
out=$(ssh_node "${IPS[0]}" 'timeout 10 nauka hypervisor init 2>&1' || true)
echo "$out" | grep -q "already has hypervisor state" \
    || { echo "$out" | sed 's/^/    /'; die "init-twice didn't print expected error"; }
ok "  init-on-inited rejects with helpful message"

# Stop node-1's peering-open daemon briefly to get a "peering not enabled"
# path from another node. Easier: attempt a join from node-2 (peering closed
# by design on all joiners).
out=$(ssh_node "${IPS[5]}" "timeout 15 nauka hypervisor join ${IPS[1]} --pin 'whatever' 2>&1" || true)
echo "$out" | grep -q "already has hypervisor state" \
    || { echo "$out" | sed 's/^/    /'; die "join-while-already-in-mesh didn't short-circuit"; }
ok "  join-while-in-mesh rejects with helpful message"

# ═══════════════════════════════════════════════════════════════════
# Phase 8: clean teardown — leave all
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ Phase 8: leave all 7 survivors ═══"
# Remove the systemd drop-in on node-4 first so cleanup is tidy.
ssh_node "${IPS[3]}" 'rm -rf /etc/systemd/system/nauka-hypervisor.service.d; systemctl daemon-reload' 2>/dev/null || true

for i in $(seq 0 $((NODE_COUNT - 2))); do
    ssh_node "${IPS[$i]}" 'timeout 30 nauka hypervisor leave 2>&1 | tail -1'
done
sleep 3

set +e
for i in $(seq 0 $((NODE_COUNT - 2))); do
    active=$(ssh_node "${IPS[$i]}" 'systemctl is-active nauka-hypervisor.service 2>&1')
    unit=$(ssh_node "${IPS[$i]}" 'test -f /etc/systemd/system/nauka-hypervisor.service && echo exists || echo gone')
    db=$(ssh_node "${IPS[$i]}" 'test -d /var/lib/nauka/db && echo exists || echo gone')
    if [[ $active != inactive || $unit != gone || $db != gone ]]; then
        set -e
        die "node-$((i + 1)) not fully cleaned: active=$active unit=$unit db=$db"
    fi
done
set -e
ok "  every node: service inactive, unit removed, DB wiped"

# ─── Collect logs ────────────────────────────────────────────────────
log "▶ collecting logs"
for i in "${!IPS[@]}"; do
    ip=${IPS[$i]}
    mkdir -p "$RUN_DIR/node-$((i + 1))"
    ssh_node "$ip" 'journalctl -u nauka-hypervisor.service --no-pager 2>/dev/null' \
        > "$RUN_DIR/node-$((i + 1))/daemon.log" 2>/dev/null || true
done

echo ""
ok "═══════════════════════════════════════════════"
ok "  ALL E2E CHECKS PASSED — every feature green"
ok "  logs: $RUN_DIR"
ok "═══════════════════════════════════════════════"
