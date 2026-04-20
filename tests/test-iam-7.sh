#!/usr/bin/env bash
# tests/test-iam-7.sh — Hetzner 3-node validation for IAM-7 (#351).
#
# Confirms the password-reset flow survives Raft replication and
# token state stays consistent across nodes:
#   - Create alice on node-1.
#   - Request a reset token on node-1; `iam.password.reset_request.minted`
#     log line carries the plaintext token id.
#   - Redeem from node-3 (follower). Old password stops working
#     everywhere; new password allows signin from node-2.
#   - Replay the same token-id → rejected (consumed flag replicated).
#   - Weak new-password rejected at redeem.
set -euo pipefail

NAUKA_BIN="${NAUKA_BIN:-target/x86_64-unknown-linux-musl/release/nauka}"
SSH_KEY_NAME="${SSH_KEY_NAME:-nauka-agent-local}"
SERVER_TYPE="${SERVER_TYPE:-cpx22}"
LOCATION="${LOCATION:-fsn1}"
IMAGE="${IMAGE:-ubuntu-24.04}"
NODE_COUNT=3

ALICE_EMAIL="alice@example.com"
ALICE_PW_OLD="old-password-alice"
ALICE_PW_NEW="brand-new-alice-pw"

RUN_DIR="/tmp/nauka-iam-7/$(date -u +%Y%m%dT%H%M%SZ)-$$"
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

# Phase 2: create alice with old password
log ""
log "═══ Phase 2: create alice on node-1 ═══"
ssh_node "${IPS[0]}" "printf '%s\n%s\n' '$ALICE_PW_OLD' '$ALICE_PW_OLD' \
    | timeout 60 nauka iam user create --email '$ALICE_EMAIL' --display-name 'Alice' 2>&1" \
    | grep -q "user created: $ALICE_EMAIL" || die "alice create failed"
ok "  alice created with old password"

# Phase 3: request reset token on node-1, read plaintext from journal
log ""
log "═══ Phase 3: request reset token on node-1 ═══"
reset_out=$(ssh_node "${IPS[0]}" "timeout 30 nauka iam password reset-request --email '$ALICE_EMAIL' 2>&1" || true)
echo "$reset_out" | grep -q "if that email is registered" \
    || { echo "$reset_out" | sed 's/^/    /'; die "reset-request CLI output unexpected"; }
ok "  CLI returned the no-enumeration response"

sleep 2
token_id=$(ssh_node "${IPS[0]}" 'journalctl -u nauka-hypervisor.service --since "2 minutes ago" --no-pager \
    | grep -oP "iam\.password\.reset_request\.minted.*token_id=\K\S+" | tail -1' || true)
[[ -n $token_id ]] || die "no token_id logged on node-1"
ok "  token id pulled from journal: $token_id"

# Phase 4: redeem on node-3 (follower), with wrong-then-right flow
log ""
log "═══ Phase 4: redeem token from node-3 ═══"
# Complexity reject should happen on node-3 without consuming the token.
weak=$(ssh_node "${IPS[2]}" "printf 'short\nshort\n' \
    | timeout 30 nauka iam password reset --token-id '$token_id' --email '$ALICE_EMAIL' 2>&1 || true")
echo "$weak" | grep -qiE 'password|complexity' \
    || { echo "$weak" | sed 's/^/    /'; die "weak password not rejected"; }
ok "  weak password rejected (token not consumed)"

# Now a real reset with a proper password.
ok_reset=$(ssh_node "${IPS[2]}" "printf '%s\n%s\n' '$ALICE_PW_NEW' '$ALICE_PW_NEW' \
    | timeout 60 nauka iam password reset --token-id '$token_id' --email '$ALICE_EMAIL' 2>&1" || true)
echo "$ok_reset" | grep -q "password updated" \
    || { echo "$ok_reset" | sed 's/^/    /'; die "reset failed on node-3"; }
ok "  reset succeeded from node-3 (Raft-forwarded)"

# Phase 5: old password no longer works, new one does
log ""
log "═══ Phase 5: rotation verified cluster-wide ═══"
bad_login=$(ssh_node "${IPS[1]}" "printf '%s\n' '$ALICE_PW_OLD' \
    | timeout 60 nauka iam login --email '$ALICE_EMAIL' 2>&1 || true")
echo "$bad_login" | grep -qiE 'error|invalid' \
    || { echo "$bad_login" | sed 's/^/    /'; die "old password still accepted"; }
ok "  old password rejected on node-2"

good_login=$(ssh_node "${IPS[1]}" "printf '%s\n' '$ALICE_PW_NEW' \
    | timeout 60 nauka iam login --email '$ALICE_EMAIL' 2>&1" || true)
echo "$good_login" | grep -q "logged in as $ALICE_EMAIL" \
    || { echo "$good_login" | sed 's/^/    /'; die "new password not accepted"; }
ok "  new password works on node-2"

# Phase 6: replay the same token — replicated consumed flag rejects
log ""
log "═══ Phase 6: token replay rejected (consumed flag replicated) ═══"
replay=$(ssh_node "${IPS[0]}" "printf '%s\n%s\n' 'yet-another-pw-1' 'yet-another-pw-1' \
    | timeout 60 nauka iam password reset --token-id '$token_id' --email '$ALICE_EMAIL' 2>&1 || true")
echo "$replay" | grep -qiE 'error|invalid' \
    || { echo "$replay" | sed 's/^/    /'; die "consumed token accepted on replay"; }
ok "  replay rejected — consumed flag replicated cluster-wide"

# Phase 7: teardown
log ""
log "═══ Phase 7: leave all $NODE_COUNT nodes ═══"
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
ok "  IAM-7 validated on a 3-node Hetzner cluster"
ok "  logs: $RUN_DIR"
ok "═══════════════════════════════════════════════"
