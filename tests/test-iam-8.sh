#!/usr/bin/env bash
# tests/test-iam-8.sh — Hetzner 3-node validation for IAM-8 (#352).
#
# Signup on the leader and login on a follower should each produce
# one `active_session` row, and both rows must be visible to the
# owner on any node — i.e. session records replicate via Raft like
# every other cluster resource.
set -euo pipefail

NAUKA_BIN="${NAUKA_BIN:-target/x86_64-unknown-linux-musl/release/nauka}"
SSH_KEY_NAME="${SSH_KEY_NAME:-nauka-agent-local}"
SERVER_TYPE="${SERVER_TYPE:-cpx22}"
LOCATION="${LOCATION:-fsn1}"
IMAGE="${IMAGE:-ubuntu-24.04}"
NODE_COUNT=3

ALICE_EMAIL="alice@example.com"
ALICE_PW="alice-iam8-test"

RUN_DIR="/tmp/nauka-iam-8/$(date -u +%Y%m%dT%H%M%SZ)-$$"
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
    if [[ ${KEEP_SERVERS:-0} == 1 ]]; then
        log "KEEP_SERVERS=1 — leaving servers (rc=$rc)"
        if [[ $rc -ne 0 ]]; then fail "FAILED — logs in $RUN_DIR"; fi
        return $rc
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
count_hypervisors() {
    ssh_node "$1" 'timeout 10 nauka hypervisor status 2>/dev/null | grep -oP "hypervisors \(\K\d+"' || echo 0
}

# Phase 0: provision + deploy
for n in "${NAMES[@]}"; do hcloud server delete "$n" >/dev/null 2>&1 || true; done
log "▶ provisioning $NODE_COUNT servers"
for i in "${!NAMES[@]}"; do
    out=$(hcloud server create --name "${NAMES[$i]}" --type "$SERVER_TYPE" --image "$IMAGE" \
        --location "$LOCATION" --ssh-key "$SSH_KEY_NAME" --output json 2>"$RUN_DIR/create-$i.err")
    IPS[$i]=$(echo "$out" | jq -r '.server.public_net.ipv4.ip // empty')
    [[ -n ${IPS[$i]} ]] || { cat "$RUN_DIR/create-$i.err" >&2; die "${NAMES[$i]}: no IPv4"; }
    log "    ${NAMES[$i]} → ${IPS[$i]}"
done
for ip in "${IPS[@]}"; do wait_ssh "$ip" & done; wait
ok "provisioned"

log "▶ deploying binary"
for ip in "${IPS[@]}"; do scp_to "$ip" "$NAUKA_BIN" /usr/local/bin/nauka >/dev/null & done; wait
for ip in "${IPS[@]}"; do ssh_node "$ip" 'chmod +x /usr/local/bin/nauka'; done
ok "deployed"

# Phase 1: init + 2 joins
log ""
log "═══ Phase 1: init + $((NODE_COUNT - 1)) joins ═══"
INIT_OUT=$(ssh_node "${IPS[0]}" 'timeout 60 nauka hypervisor init 2>&1')
PIN=$(echo "$INIT_OUT" | grep -oP 'join pin:\s+\K\S+')
[[ -n $PIN ]] || die "no PIN"
ok "  node-1 init — PIN=$PIN"
for i in $(seq 1 $((NODE_COUNT - 1))); do
    ssh_node "${IPS[$i]}" "timeout 30 nauka hypervisor join ${IPS[0]} --pin '$PIN' 2>&1" \
        | grep -q '^joined mesh' \
        && ok "  node-$((i + 1)) joined" \
        || die "node-$((i + 1)) join failed"
done
sleep 30
for i in "${!IPS[@]}"; do
    c=$(count_hypervisors "${IPS[$i]}")
    [[ $c -eq $NODE_COUNT ]] || die "node-$((i + 1)) sees $c (expected $NODE_COUNT)"
done
ok "all $NODE_COUNT nodes agree on cluster"

# Phase 2: signup on leader → 1 session
log ""
log "═══ Phase 2: signup alice on node-1 ═══"
ssh_node "${IPS[0]}" "printf '%s\n%s\n' '$ALICE_PW' '$ALICE_PW' \
    | timeout 60 nauka iam user create --email '$ALICE_EMAIL' --display-name 'Alice' 2>&1" \
    | grep -q "user created: $ALICE_EMAIL" || die "signup failed"
ok "  signup succeeded"

# Let the session write replicate.
sleep 2
session_out1=$(ssh_node "${IPS[0]}" 'timeout 30 nauka iam session list 2>&1')
echo "$session_out1" | grep -q "active sessions (1):" \
    || { echo "$session_out1" | sed 's/^/    /'; die "expected 1 session after signup"; }
ok "  node-1: 1 active session after signup"

# Phase 3: login on node-3 (follower) → 2 sessions
log ""
log "═══ Phase 3: login alice on node-3 → 2 sessions cluster-wide ═══"
ssh_node "${IPS[2]}" "printf '%s\n' '$ALICE_PW' \
    | timeout 60 nauka iam login --email '$ALICE_EMAIL' 2>&1" \
    | grep -q "logged in as $ALICE_EMAIL" || die "login on node-3 failed"
ok "  login on node-3"

sleep 2
# Both nodes should now see 2 sessions (one for signup on node-1, one
# for login on node-3). Raft replicates both rows.
session_out_n1=$(ssh_node "${IPS[0]}" 'timeout 30 nauka iam session list 2>&1')
echo "$session_out_n1" | grep -q "active sessions (2):" \
    || { echo "$session_out_n1" | sed 's/^/    /'; die "node-1 did not see 2 sessions"; }
ok "  node-1 sees 2 sessions (signup + login replicated)"

session_out_n3=$(ssh_node "${IPS[2]}" 'timeout 30 nauka iam session list 2>&1')
echo "$session_out_n3" | grep -q "active sessions (2):" \
    || { echo "$session_out_n3" | sed 's/^/    /'; die "node-3 did not see 2 sessions"; }
ok "  node-3 sees 2 sessions (cluster-wide view)"

# Phase 4: bob sees zero sessions
log ""
log "═══ Phase 4: cross-user isolation — bob sees zero ═══"
ssh_node "${IPS[1]}" "printf '%s\n%s\n' 'bob-pw-iam8-test' 'bob-pw-iam8-test' \
    | timeout 60 nauka iam user create --email 'bob@example.com' --display-name 'Bob' 2>&1" \
    | grep -q "user created: bob@example.com" || die "bob create failed"
# bob's signup created his own session. List as bob → 1 session (his own).
bob_sessions=$(ssh_node "${IPS[1]}" 'timeout 30 nauka iam session list 2>&1')
echo "$bob_sessions" | grep -q "active sessions (1):" \
    || { echo "$bob_sessions" | sed 's/^/    /'; die "bob should see 1 session (his own)"; }
# and bob must NOT see alice's uid in his list.
if echo "$bob_sessions" | grep -q "$ALICE_EMAIL"; then
    die "bob sees alice's sessions — PERMISSIONS leaked"
fi
ok "  bob sees only his own session (PERMISSIONS cross-user isolation)"

# Phase 5: teardown
log ""
log "═══ Phase 5: leave all $NODE_COUNT nodes ═══"
for i in "${!IPS[@]}"; do
    ssh_node "${IPS[$i]}" 'timeout 30 nauka hypervisor leave 2>&1 | tail -1'
done
sleep 3

log "▶ collecting logs"
for i in "${!IPS[@]}"; do
    ip=${IPS[$i]}
    mkdir -p "$RUN_DIR/node-$((i + 1))"
    ssh_node "$ip" 'journalctl -u nauka-hypervisor.service --no-pager 2>/dev/null' \
        > "$RUN_DIR/node-$((i + 1))/daemon.log" 2>/dev/null || true
done

echo ""
ok "═══════════════════════════════════════════════"
ok "  IAM-8 validated on a 3-node Hetzner cluster"
ok "  logs: $RUN_DIR"
ok "═══════════════════════════════════════════════"
