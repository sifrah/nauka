#!/usr/bin/env bash
# tests/test-hypervisor-ux.sh — End-to-end validation of the systemd-based
# UX: `nauka hypervisor init`, `join`, `status`, `leave`.
#
# Every command is synchronous and returns to the shell once its job is
# done — no foreground daemon, no ctrl+c. The long-running work happens
# under `nauka-hypervisor.service`, inspected via systemctl / journalctl.
set -euo pipefail

NAUKA_BIN="${NAUKA_BIN:-target/x86_64-unknown-linux-musl/release/nauka}"
SSH_KEY_NAME="${SSH_KEY_NAME:-nauka-agent-local}"
SERVER_TYPE="${SERVER_TYPE:-cpx22}"
LOCATION="${LOCATION:-fsn1}"
IMAGE="${IMAGE:-ubuntu-24.04}"
NODE_COUNT=3

RUN_DIR="/tmp/nauka-hypervisor-ux/$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$RUN_DIR"

[[ -x $NAUKA_BIN ]] || { echo "✗ NAUKA_BIN not executable" >&2; exit 1; }

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
log "▶ provisioning $NODE_COUNT servers (parallel)"
for i in "${!NAMES[@]}"; do
    (
        out=$(hcloud server create --name "${NAMES[$i]}" --type "$SERVER_TYPE" --image "$IMAGE" \
            --location "$LOCATION" --ssh-key "$SSH_KEY_NAME" --output json 2>/dev/null)
        echo "$out" | jq -r '.server.public_net.ipv4.ip // empty' > "$RUN_DIR/ip-$i"
    ) &
done
wait
for i in "${!NAMES[@]}"; do
    IPS[$i]=$(cat "$RUN_DIR/ip-$i" 2>/dev/null || true)
    [[ -n ${IPS[$i]} ]] || die "${NAMES[$i]}: no IPv4"
    log "    ${NAMES[$i]} → ${IPS[$i]}"
done
for ip in "${IPS[@]}"; do wait_ssh "$ip" & done; wait
ok "provisioned"

log "▶ deploying binary"
for ip in "${IPS[@]}"; do
    scp_to "$ip" "$NAUKA_BIN" /usr/local/bin/nauka >/dev/null &
done
wait
for ip in "${IPS[@]}"; do ssh_node "$ip" 'chmod +x /usr/local/bin/nauka'; done
ok "deployed"

NODE1=${IPS[0]}
NODE2=${IPS[1]}
NODE3=${IPS[2]}

# ═══════════════════════════════════════════════════════════════════
# INIT — creates mesh, installs systemd unit, starts service, exits
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ nauka hypervisor init on node-1 ═══"
set +e
INIT_OUT=$(ssh_node "$NODE1" 'nauka hypervisor init 2>&1')
INIT_RC=$?
set -e
echo "$INIT_OUT" | sed 's/^/    /'
[[ $INIT_RC -eq 0 ]] || die "init exited with code $INIT_RC"
PIN=$(echo "$INIT_OUT" | grep -oP 'join pin:\s+\K\S+')
[[ -n $PIN ]] || die "init didn't print a PIN"
ok "init returned a PIN: $PIN"

# The CLI should have exited — verify it's no longer running, but the
# systemd-managed daemon is.
ssh_node "$NODE1" 'pgrep -x nauka >/dev/null' \
    || die "daemon process not running after init"
ssh_node "$NODE1" 'systemctl is-active --quiet nauka-hypervisor.service' \
    || die "nauka-hypervisor.service not active on node-1"
ok "systemd unit active, daemon running"

# Raft + peering ports up.
wait_port "$NODE1" 4001 || die "raft port never opened on node-1"
wait_port "$NODE1" 51821 || die "peering port never opened on node-1"
ok "node-1 serving Raft (4001) + peering (51821)"

# ═══════════════════════════════════════════════════════════════════
# JOIN — node-2 and node-3 pick up the PIN, install + start service
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ nauka hypervisor join on nodes 2, 3 ═══"
for i in 1 2; do
    ip=${IPS[$i]}
    OUT=$(ssh_node "$ip" "nauka hypervisor join $NODE1 --pin '$PIN' 2>&1")
    echo "$OUT" | sed 's/^/    /'
    echo "$OUT" | grep -q '^joined mesh' || die "node-$((i+1)) join didn't print 'joined mesh'"
    ssh_node "$ip" 'systemctl is-active --quiet nauka-hypervisor.service' \
        || die "nauka-hypervisor.service not active on node-$((i+1))"
    wait_port "$ip" 4001 || die "node-$((i+1)) raft port never opened"
    ok "node-$((i+1)) joined + service active"
done

# ═══════════════════════════════════════════════════════════════════
# Wait for voter promotion + Raft propagation of hypervisor records
# ═══════════════════════════════════════════════════════════════════
log ""
log "▶ waiting 20s for voter promotions + Raft replication"
sleep 20
VOTERS=$(ssh_node "$NODE1" 'journalctl -u nauka-hypervisor.service --no-pager 2>/dev/null | grep -c "raft voter:" || echo 0')
log "    voter promotions on node-1: $VOTERS (want $((NODE_COUNT - 1)))"
[[ $VOTERS -ge $((NODE_COUNT - 1)) ]] || die "not all nodes promoted"
ok "$VOTERS / $((NODE_COUNT - 1)) voter promotions"

# ═══════════════════════════════════════════════════════════════════
# STATUS — each node sees 3 hypervisors in the replicated table
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ nauka hypervisor status — replicated cluster view ═══"
for i in "${!IPS[@]}"; do
    ip=${IPS[$i]}
    set +e
    STATUS=$(ssh_node "$ip" 'nauka hypervisor status 2>&1')
    rc=$?
    set -e
    if [[ $rc -ne 0 ]]; then
        echo "$STATUS" | sed 's/^/      /'
        die "node-$((i+1)) status exited with code $rc"
    fi
    COUNT=$(echo "$STATUS" | grep -oP 'hypervisors \(\K\d+' || echo 0)
    log "    node-$((i+1)) sees $COUNT hypervisors"
    if [[ $COUNT -ne $NODE_COUNT ]]; then
        echo "$STATUS" | sed 's/^/      /'
        die "node-$((i+1)) sees $COUNT, expected $NODE_COUNT"
    fi
done
ok "all $NODE_COUNT nodes agree on cluster membership"

# ═══════════════════════════════════════════════════════════════════
# LEAVE — node-3 gracefully exits; node-1 and node-2 should drop it
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ nauka hypervisor leave on node-3 ═══"
ssh_node "$NODE3" 'nauka hypervisor leave 2>&1' | sed 's/^/    /'
ssh_node "$NODE3" 'systemctl is-active --quiet nauka-hypervisor.service' \
    && die "nauka-hypervisor.service still active on node-3 after leave"
ssh_node "$NODE3" 'test -f /etc/systemd/system/nauka-hypervisor.service' \
    && die "systemd unit file still present on node-3 after leave"
ok "node-3: service stopped + unit file removed"

# Give the leave's DELETE time to replicate + reconciler cycle.
sleep 12

for i in 0 1; do
    ip=${IPS[$i]}
    STATUS=$(ssh_node "$ip" 'nauka hypervisor status 2>&1')
    COUNT=$(echo "$STATUS" | grep -oP 'hypervisors \(\K\d+')
    log "    node-$((i+1)) now sees $COUNT hypervisors"
    [[ $COUNT -eq 2 ]] \
        || { echo "$STATUS" | sed 's/^/      /'; die "node-$((i+1)) sees $COUNT after leave, expected 2"; }
done
ok "survivors dropped node-3 from the replicated table"

# ─── Collect logs ────────────────────────────────────────────────────
for i in "${!IPS[@]}"; do
    ip=${IPS[$i]}
    mkdir -p "$RUN_DIR/node-$((i+1))"
    ssh_node "$ip" 'journalctl -u nauka-hypervisor.service --no-pager 2>/dev/null' \
        > "$RUN_DIR/node-$((i+1))/daemon.log" 2>/dev/null || true
done

echo ""
ok "═══ ALL CHECKS PASSED ═══"
ok "  init → join → status → leave, all systemd-managed"
ok "  logs: $RUN_DIR"
