#!/usr/bin/env bash
# tests/test-iam-1.sh — Hetzner 3-node validation for IAM-1 (#345).
#
# Validates the full production IAM-1 flow on real VMs:
#
#   1. Provision 3 fresh Hetzner VMs, deploy musl binary.
#   2. init on node-1 (no --peering), join on node-2 + node-3.
#   3. Wait for voter promotions — cluster is 3 voters.
#   4. `nauka iam user create` on node-1 → hashes password in Rust,
#      Writer::create routes through Raft → replicates to all nodes.
#   5. `nauka iam login` succeeds on node-2 (a follower) for the user
#      created on the leader — proves the user record replicated.
#   6. Token file on node-2 exists at mode 0600 and contains a JWT.
#   7. `nauka iam whoami` on node-2 prints the correct email.
#   8. `nauka iam logout` on node-2 removes the token file.
#   9. `nauka iam login` with the wrong password errors out.
#  10. Teardown — leave all nodes, destroy VMs.
#
# Policy reminders (see CLAUDE.md memory):
#   - Always start from a clean slate: every `nauka-dev-*` VM is
#     destroyed before provisioning.
#   - `nauka hypervisor init` is invoked WITHOUT `--peering`.
set -euo pipefail

NAUKA_BIN="${NAUKA_BIN:-target/x86_64-unknown-linux-musl/release/nauka}"
SSH_KEY_NAME="${SSH_KEY_NAME:-nauka-agent-local}"
SERVER_TYPE="${SERVER_TYPE:-cpx22}"
LOCATION="${LOCATION:-fsn1}"
IMAGE="${IMAGE:-ubuntu-24.04}"
NODE_COUNT=3

EMAIL="alice@example.com"
DISPLAY_NAME="Alice"
# Test-only literal password. The script is idempotent — the whole
# cluster is wiped at teardown — so there is no hash to leak here.
PASSWORD="hunter2-iam1-test"

RUN_DIR="/tmp/nauka-iam-1/$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$RUN_DIR"

[[ -x $NAUKA_BIN ]] || { echo "✗ NAUKA_BIN not executable: $NAUKA_BIN" >&2; exit 1; }
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

# ═══════════════════════════════════════════════════════════════════
# Phase 0: provision + deploy
# ═══════════════════════════════════════════════════════════════════
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

# ═══════════════════════════════════════════════════════════════════
# Phase 1: init + 2 joins — standard bootstrap, no --peering
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ Phase 1: init + $((NODE_COUNT - 1)) joins ═══"
INIT_OUT=$(ssh_node "${IPS[0]}" 'timeout 30 nauka hypervisor init 2>&1')
PIN=$(echo "$INIT_OUT" | grep -oP 'join pin:\s+\K\S+')
[[ -n $PIN ]] || { echo "$INIT_OUT" | sed 's/^/    /'; die "init didn't print a PIN"; }
ok "  node-1 init — PIN=$PIN"

for i in $(seq 1 $((NODE_COUNT - 1))); do
    ip=${IPS[$i]}
    ssh_node "$ip" "timeout 30 nauka hypervisor join ${IPS[0]} --pin '$PIN' 2>&1" \
        | grep -q '^joined mesh' \
        && ok "  node-$((i + 1)) joined" \
        || die "node-$((i + 1)) join failed"
done

log "▶ waiting 30s for voter promotions"
sleep 30
for i in "${!IPS[@]}"; do
    c=$(count_hypervisors "${IPS[$i]}")
    [[ $c -eq $NODE_COUNT ]] || die "node-$((i + 1)) sees $c hypervisors (expected $NODE_COUNT)"
done
ok "all $NODE_COUNT nodes agree on the full cluster"

# ═══════════════════════════════════════════════════════════════════
# Phase 2: user create on leader — tests the Rust-hash + Raft path
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ Phase 2: nauka iam user create on node-1 ═══"

# `nauka iam user create` prompts for the password twice (main + confirm).
# Piping two lines via stdin exercises rpassword's non-TTY fallback —
# what the CLI does for scripted / batch operator use.
# `|| true` + explicit echo keeps the CLI's own error message visible;
# without it, `set -e` kills the script before the diagnostic prints.
create_out=$(ssh_node "${IPS[0]}" "printf '%s\\n%s\\n' '$PASSWORD' '$PASSWORD' \
    | timeout 15 nauka iam user create --email '$EMAIL' --display-name '$DISPLAY_NAME' 2>&1" || true)
echo "$create_out" | grep -q "user created: $EMAIL" \
    || { echo "$create_out" | sed 's/^/    [create] /'; die "user create on node-1 failed"; }
ok "  user $EMAIL created on node-1"

# The CLI auto-logs-in the newly-created user and persists the JWT.
ssh_node "${IPS[0]}" "test -f /root/.config/nauka/token" \
    || die "node-1: token file missing after user create"
mode=$(ssh_node "${IPS[0]}" "stat -c %a /root/.config/nauka/token")
[[ $mode == 600 ]] || die "node-1: token file mode is $mode, expected 600"
ok "  node-1: token file at 0600"

# ═══════════════════════════════════════════════════════════════════
# Phase 3: login on a follower — proves the user row replicated
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ Phase 3: login on node-2 (a follower) ═══"
login_out=$(ssh_node "${IPS[1]}" "printf '%s\\n' '$PASSWORD' \
    | timeout 15 nauka iam login --email '$EMAIL' 2>&1" || true)
echo "$login_out" | grep -q "logged in as $EMAIL" \
    || { echo "$login_out" | sed 's/^/    [login2] /'; die "login on node-2 failed — user didn't replicate"; }
ok "  login on node-2 succeeded"

ssh_node "${IPS[1]}" "test -f /root/.config/nauka/token" \
    || die "node-2: token file missing after login"
mode=$(ssh_node "${IPS[1]}" "stat -c %a /root/.config/nauka/token")
[[ $mode == 600 ]] || die "node-2: token file mode is $mode, expected 600"

token=$(ssh_node "${IPS[1]}" 'cat /root/.config/nauka/token')
# JWT = 3 base64url segments joined by dots.
dots=$(echo -n "$token" | tr -cd '.' | wc -c)
[[ $dots -eq 2 ]] || die "node-2: token doesn't look like a JWT (dots=$dots)"
ok "  node-2: token file at 0600 and carries a JWT"

# ═══════════════════════════════════════════════════════════════════
# Phase 4: whoami decodes the claims
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ Phase 4: whoami on node-2 ═══"
whoami_out=$(ssh_node "${IPS[1]}" 'timeout 10 nauka iam whoami 2>&1')
echo "$whoami_out" | grep -q "email:.*$EMAIL" \
    || { echo "$whoami_out" | sed 's/^/    /'; die "whoami didn't show email on node-2"; }
echo "$whoami_out" | grep -q "access:.*user" \
    || { echo "$whoami_out" | sed 's/^/    /'; die "whoami didn't show access=user"; }
ok "  whoami shows email=$EMAIL and access=user"

# ═══════════════════════════════════════════════════════════════════
# Phase 5: login on the remaining follower + wrong-password check
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ Phase 5: login on node-3 + wrong password rejected ═══"
login3=$(ssh_node "${IPS[2]}" "printf '%s\\n' '$PASSWORD' \
    | timeout 15 nauka iam login --email '$EMAIL' 2>&1" || true)
echo "$login3" | grep -q "logged in as $EMAIL" \
    || { echo "$login3" | sed 's/^/    [login3] /'; die "login on node-3 failed"; }
ok "  login on node-3 succeeded"

# Wrong password → daemon returns the signin error, CLI exits non-zero.
bad=$(ssh_node "${IPS[2]}" "printf 'not-the-password\\n' \
    | timeout 15 nauka iam login --email '$EMAIL' 2>&1 || true")
echo "$bad" | grep -qiE 'error:|invalid' \
    || { echo "$bad" | sed 's/^/    /'; die "wrong-password login should have errored"; }
ok "  wrong password rejected"

# ═══════════════════════════════════════════════════════════════════
# Phase 6: logout removes the token file
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ Phase 6: logout on node-2 removes the token ═══"
ssh_node "${IPS[1]}" 'timeout 10 nauka iam logout 2>&1' | grep -q 'logged out' \
    || die "logout didn't print success"
ssh_node "${IPS[1]}" "test -f /root/.config/nauka/token" \
    && die "node-2: token file still present after logout"
ok "  node-2: token file removed"

# ═══════════════════════════════════════════════════════════════════
# Phase 7: clean teardown
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ Phase 7: leave all $NODE_COUNT nodes ═══"
for i in "${!IPS[@]}"; do
    ssh_node "${IPS[$i]}" 'timeout 30 nauka hypervisor leave 2>&1 | tail -1'
done
sleep 3
for i in "${!IPS[@]}"; do
    active=$(ssh_node "${IPS[$i]}" 'systemctl is-active nauka-hypervisor.service 2>&1' || true)
    [[ $active == inactive || $active == "Unit nauka-hypervisor.service could not be found."* ]] \
        || die "node-$((i + 1)) still active: $active"
done
ok "  every node: service inactive, unit removed"

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
ok "  IAM-1 validated on a 3-node Hetzner cluster"
ok "  logs: $RUN_DIR"
ok "═══════════════════════════════════════════════"
