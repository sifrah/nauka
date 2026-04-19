#!/usr/bin/env bash
# tests/test-iam-9.sh — Hetzner 3-node validation for IAM-9 (#353).
#
# Closes the epic's day-one governance bar:
#   - `role bind` without `--reason` is rejected.
#   - Binding with a reason replicates the reason cluster-wide.
#   - `user deactivate --email --reason` blocks future signin on
#     every node (SIGNIN clause filters by `active = true`).
#   - `user activate` restores signin.
set -euo pipefail

NAUKA_BIN="${NAUKA_BIN:-target/x86_64-unknown-linux-musl/release/nauka}"
SSH_KEY_NAME="${SSH_KEY_NAME:-nauka-agent-local}"
SERVER_TYPE="${SERVER_TYPE:-cpx22}"
LOCATION="${LOCATION:-fsn1}"
IMAGE="${IMAGE:-ubuntu-24.04}"
NODE_COUNT=3

ALICE_EMAIL="alice@example.com"
ALICE_PW="alice-iam9-test"
BOB_EMAIL="bob@example.com"
BOB_PW="bob-iam9-test"
ORG_SLUG="acme"

RUN_DIR="/tmp/nauka-iam-9/$(date -u +%Y%m%dT%H%M%SZ)-$$"
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
        [[ $rc -ne 0 ]] && fail "FAILED — logs in $RUN_DIR"
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

# Phase 2: create bob then alice, then alice's org
log ""
log "═══ Phase 2: seed users + org (alice owns acme) ═══"
ssh_node "${IPS[0]}" "printf '%s\n%s\n' '$BOB_PW' '$BOB_PW' \
    | timeout 60 nauka user create --email '$BOB_EMAIL' --display-name 'Bob' 2>&1" \
    | grep -q "user created: $BOB_EMAIL" || die "bob create failed"
ssh_node "${IPS[0]}" "printf '%s\n%s\n' '$ALICE_PW' '$ALICE_PW' \
    | timeout 60 nauka user create --email '$ALICE_EMAIL' --display-name 'Alice' 2>&1" \
    | grep -q "user created: $ALICE_EMAIL" || die "alice create failed"
ssh_node "${IPS[0]}" "timeout 30 nauka org create --slug '$ORG_SLUG' \
    --display-name 'Acme' 2>&1" | grep -q "org created: $ORG_SLUG" \
    || die "org create failed"
ok "  bob + alice + $ORG_SLUG org seeded"

# Phase 3: bind without reason fails at CLI parse (required arg)
log ""
log "═══ Phase 3: role bind requires --reason (IAM-9 gate) ═══"
miss_reason=$(ssh_node "${IPS[0]}" "timeout 30 nauka role bind \
    --principal '$BOB_EMAIL' --role viewer --org '$ORG_SLUG' 2>&1 || true")
echo "$miss_reason" | grep -qiE "required|reason" \
    || { echo "$miss_reason" | sed 's/^/    /'; die "missing reason not flagged"; }
ok "  CLI rejects bind without --reason"

# Phase 4: bind with reason, confirm replication
log ""
log "═══ Phase 4: bind with reason, verify reason replicates ═══"
bind_out=$(ssh_node "${IPS[0]}" "timeout 30 nauka role bind \
    --principal '$BOB_EMAIL' --role viewer --org '$ORG_SLUG' \
    --reason 'onboarding bob to the ops team' 2>&1" || true)
echo "$bind_out" | grep -q "bound $BOB_EMAIL" \
    || { echo "$bind_out" | sed 's/^/    /'; die "bind failed"; }
ok "  bind succeeded on node-1"

sleep 3
audit_list=$(ssh_node "${IPS[2]}" "printf '%s\n' '$ALICE_PW' \
    | timeout 60 nauka login --email '$ALICE_EMAIL' 2>&1 >/dev/null; \
    timeout 30 nauka audit list --limit 20 2>&1")
echo "$audit_list" | grep -q "success:onboarding bob to the ops team" \
    || { echo "$audit_list" | sed 's/^/    /'; die "reason not in audit outcome"; }
ok "  audit outcome carries the reason on node-3 (Raft replication)"

# Phase 5: deactivate bob, signin fails on every node
log ""
log "═══ Phase 5: deactivate bob, signin blocked cluster-wide ═══"
# login as alice on node-1 again so the token file is hers (admin).
ssh_node "${IPS[0]}" "printf '%s\n' '$ALICE_PW' \
    | timeout 60 nauka login --email '$ALICE_EMAIL' 2>&1 >/dev/null" \
    || die "alice re-login"
ssh_node "${IPS[0]}" "timeout 30 nauka user deactivate --email '$BOB_EMAIL' \
    --reason 'leaving the company' 2>&1" | grep -q "user deactivated: $BOB_EMAIL" \
    || die "deactivate failed"
ok "  bob deactivated from node-1"

sleep 3
bad_bob=$(ssh_node "${IPS[1]}" "printf '%s\n' '$BOB_PW' \
    | timeout 60 nauka login --email '$BOB_EMAIL' 2>&1 || true")
echo "$bad_bob" | grep -qiE "error|invalid" \
    || { echo "$bad_bob" | sed 's/^/    /'; die "deactivated bob signed in on node-2"; }
ok "  bob's signin rejected on node-2 (deactivation replicated)"

bad_bob_n3=$(ssh_node "${IPS[2]}" "printf '%s\n' '$BOB_PW' \
    | timeout 60 nauka login --email '$BOB_EMAIL' 2>&1 || true")
echo "$bad_bob_n3" | grep -qiE "error|invalid" \
    || { echo "$bad_bob_n3" | sed 's/^/    /'; die "deactivated bob signed in on node-3"; }
ok "  bob's signin rejected on node-3"

# Phase 6: user list shows the caller's own row (owner-scoped PERMISSIONS)
log ""
log "═══ Phase 6: user list is owner-scoped ═══"
users_out=$(ssh_node "${IPS[0]}" 'timeout 30 nauka user list 2>&1' || true)
echo "$users_out" | grep -q "$ALICE_EMAIL" \
    || { echo "$users_out" | sed 's/^/    /'; die "alice does not see her own row"; }
# PERMISSIONS filter blocks alice from seeing bob (same filter IAM-6 set
# up for User.password_hash scoping). Admin-view across users will
# land when RoleBinding enforcement gates `nauka user list` — tracked
# as a follow-up alongside IAM-9b EmergencyAccess.
echo "$users_out" | grep -q "$BOB_EMAIL" \
    && { echo "$users_out" | sed 's/^/    /'; die "alice can see bob's row — PERMISSIONS leaked"; }
ok "  alice sees only her own row; bob hidden until admin enforcement lands"

# Phase 7: reactivate, signin works again
log ""
log "═══ Phase 7: reactivate bob → signin restored ═══"
ssh_node "${IPS[0]}" "timeout 30 nauka user activate --email '$BOB_EMAIL' \
    --reason 'rehiring' 2>&1" | grep -q "user activated: $BOB_EMAIL" \
    || die "reactivate failed"
sleep 3
good_bob=$(ssh_node "${IPS[1]}" "printf '%s\n' '$BOB_PW' \
    | timeout 60 nauka login --email '$BOB_EMAIL' 2>&1" || true)
echo "$good_bob" | grep -q "logged in as $BOB_EMAIL" \
    || { echo "$good_bob" | sed 's/^/    /'; die "reactivated signin failed"; }
ok "  bob signed in on node-2 after reactivation"

# Phase 8: teardown
log ""
log "═══ Phase 8: leave all $NODE_COUNT nodes ═══"
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
ok "  IAM-9 validated on a 3-node Hetzner cluster"
ok "  logs: $RUN_DIR"
ok "═══════════════════════════════════════════════"
