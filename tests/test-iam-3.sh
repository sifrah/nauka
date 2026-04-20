#!/usr/bin/env bash
# tests/test-iam-3.sh — Hetzner 3-node validation for IAM-3 (#347).
#
# Confirms end-to-end that:
#   - The permission catalog + primitive roles (`editor`, `viewer`)
#     get seeded at `nauka hypervisor init` time and replicate via
#     Raft to every joiner.
#   - `fn::iam::can` consults `RoleBinding` records: alice binds bob
#     as `viewer` at her org → bob can see alice's org / project /
#     env. Remove the binding → bob goes back to seeing nothing.
#   - Role binding rows themselves replicate across the cluster
#     (bind on one node, list on another).
#
# Same guardrails as IAM-1/IAM-2: wipe `nauka-dev-*`, never
# `init --peering`.
set -euo pipefail

NAUKA_BIN="${NAUKA_BIN:-target/x86_64-unknown-linux-musl/release/nauka}"
SSH_KEY_NAME="${SSH_KEY_NAME:-nauka-agent-local}"
SERVER_TYPE="${SERVER_TYPE:-cpx22}"
LOCATION="${LOCATION:-fsn1}"
IMAGE="${IMAGE:-ubuntu-24.04}"
NODE_COUNT=3

ALICE_EMAIL="alice@example.com"
ALICE_PW="alice-iam3-test"
BOB_EMAIL="bob@example.com"
BOB_PW="bob-iam3-test"
ORG_SLUG="acme"
PROJECT_SLUG="web"
ENV_SLUG="production"

RUN_DIR="/tmp/nauka-iam-3/$(date -u +%Y%m%dT%H%M%SZ)-$$"
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
# Phase 1: init + 2 joins — seeding runs at init
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

log "▶ waiting 30s for voter promotions + Raft replay of seeded data"
sleep 30
for i in "${!IPS[@]}"; do
    c=$(count_hypervisors "${IPS[$i]}")
    [[ $c -eq $NODE_COUNT ]] || die "node-$((i + 1)) sees $c hypervisors (expected $NODE_COUNT)"
done
ok "all $NODE_COUNT nodes agree on the full cluster"

# ═══════════════════════════════════════════════════════════════════
# Phase 2: users + org/project/env owned by alice
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ Phase 2: create bob + alice + alice's org tree ═══"
ssh_node "${IPS[0]}" "printf '%s\n%s\n' '$BOB_PW' '$BOB_PW' \
    | timeout 30 nauka iam user create --email '$BOB_EMAIL' --display-name 'Bob' 2>&1" \
    | grep -q "user created: $BOB_EMAIL" || die "bob create failed"
ok "  bob created on node-1"

ssh_node "${IPS[0]}" "printf '%s\n%s\n' '$ALICE_PW' '$ALICE_PW' \
    | timeout 30 nauka iam user create --email '$ALICE_EMAIL' --display-name 'Alice' 2>&1" \
    | grep -q "user created: $ALICE_EMAIL" || die "alice create failed"
ok "  alice created on node-1 (token belongs to alice)"

ssh_node "${IPS[0]}" "timeout 30 nauka iam org create --slug '$ORG_SLUG' \
    --display-name 'Acme Corp' 2>&1" | grep -q "org created: $ORG_SLUG" \
    || die "org create failed"
ssh_node "${IPS[0]}" "timeout 30 nauka iam project create --org '$ORG_SLUG' \
    --slug '$PROJECT_SLUG' --display-name 'Web Platform' 2>&1" \
    | grep -q "project created:" || die "project create failed"
ssh_node "${IPS[0]}" "timeout 30 nauka iam env create --project '${ORG_SLUG}-${PROJECT_SLUG}' \
    --slug '$ENV_SLUG' --display-name 'Production' 2>&1" \
    | grep -q "env created:" || die "env create failed"
ok "  alice's org tree created"

# ═══════════════════════════════════════════════════════════════════
# Phase 3: primitive roles are visible to alice (seeded + replicated)
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ Phase 3: seeded primitive roles visible cluster-wide ═══"
role_list=$(ssh_node "${IPS[0]}" 'timeout 30 nauka iam role list 2>&1')
echo "$role_list" | grep -q "viewer" \
    || { echo "$role_list" | sed 's/^/    /'; die "viewer role not visible"; }
echo "$role_list" | grep -q "editor" \
    || { echo "$role_list" | sed 's/^/    /'; die "editor role not visible"; }
ok "  node-1 sees viewer + editor primitive roles"

# Same on a follower: confirms the Raft replay carried the seed writes.
role_list3=$(ssh_node "${IPS[2]}" "printf '%s\n' '$ALICE_PW' \
    | timeout 30 nauka iam login --email '$ALICE_EMAIL' 2>&1 >/dev/null; \
    timeout 30 nauka iam role list 2>&1")
echo "$role_list3" | grep -q "viewer" \
    || { echo "$role_list3" | sed 's/^/    /'; die "viewer not replicated to node-3"; }
ok "  node-3 sees the same roles (Raft replayed the seed writes)"

# ═══════════════════════════════════════════════════════════════════
# Phase 4: bob without a binding sees nothing
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ Phase 4: bob without a binding sees nothing ═══"
ssh_node "${IPS[1]}" 'nauka iam logout >/dev/null 2>&1' || true
ssh_node "${IPS[1]}" "printf '%s\n' '$BOB_PW' \
    | timeout 30 nauka iam login --email '$BOB_EMAIL' 2>&1" \
    | grep -q "logged in as $BOB_EMAIL" || die "bob login on node-2 failed"
bob_orgs=$(ssh_node "${IPS[1]}" 'timeout 30 nauka iam org list 2>&1' || true)
echo "$bob_orgs" | grep -q "orgs (0):" \
    || { echo "$bob_orgs" | sed 's/^/    /'; die "bob should see 0 orgs without a binding"; }
ok "  bob on node-2: 0 orgs visible"

# ═══════════════════════════════════════════════════════════════════
# Phase 5: alice binds bob as viewer on node-1
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ Phase 5: alice binds bob as viewer on node-1 ═══"
bind_out=$(ssh_node "${IPS[0]}" "timeout 30 nauka iam role bind \
    --principal '$BOB_EMAIL' --role viewer --org '$ORG_SLUG' 2>&1" || true)
echo "$bind_out" | grep -q "bound $BOB_EMAIL to role viewer" \
    || { echo "$bind_out" | sed 's/^/    /'; die "bind failed"; }
ok "  alice bound bob to viewer@$ORG_SLUG"

# ═══════════════════════════════════════════════════════════════════
# Phase 6: bob on node-3 now sees the full scope tree
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ Phase 6: binding replicates — bob sees the tree on node-3 ═══"
# The binding was created on node-1 but is read by bob on node-2
# (he's still logged in there from Phase 4) — this exercises the
# binding-record Raft replication.
sleep 3
bob_orgs=$(ssh_node "${IPS[1]}" 'timeout 30 nauka iam org list 2>&1' || true)
echo "$bob_orgs" | grep -q "orgs (1):" \
    || { echo "$bob_orgs" | sed 's/^/    /'; die "bob should see 1 org after binding"; }
echo "$bob_orgs" | grep -q "$ORG_SLUG" \
    || { echo "$bob_orgs" | sed 's/^/    /'; die "bob should see acme"; }
ok "  bob on node-2 sees acme (binding replicated)"

bob_projects=$(ssh_node "${IPS[1]}" 'timeout 30 nauka iam project list 2>&1' || true)
echo "$bob_projects" | grep -q "projects (1):" \
    || { echo "$bob_projects" | sed 's/^/    /'; die "bob should see 1 project via scope chain"; }
ok "  bob sees the project (scope chain walked correctly)"

bob_envs=$(ssh_node "${IPS[1]}" 'timeout 30 nauka iam env list 2>&1' || true)
echo "$bob_envs" | grep -q "envs (1):" \
    || { echo "$bob_envs" | sed 's/^/    /'; die "bob should see 1 env via scope chain"; }
ok "  bob sees the env (two-hop scope chain)"

# ═══════════════════════════════════════════════════════════════════
# Phase 7: alice unbinds bob; bob goes back to seeing nothing
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ Phase 7: alice unbinds bob ═══"
unbind_out=$(ssh_node "${IPS[0]}" "timeout 30 nauka iam role unbind \
    --principal '$BOB_EMAIL' --role viewer --org '$ORG_SLUG' 2>&1" || true)
echo "$unbind_out" | grep -q "unbound $BOB_EMAIL" \
    || { echo "$unbind_out" | sed 's/^/    /'; die "unbind failed"; }
ok "  alice unbound bob"

sleep 3
bob_orgs=$(ssh_node "${IPS[1]}" 'timeout 30 nauka iam org list 2>&1' || true)
echo "$bob_orgs" | grep -q "orgs (0):" \
    || { echo "$bob_orgs" | sed 's/^/    /'; die "bob should see 0 orgs after unbind"; }
ok "  bob on node-2: 0 orgs (unbind replicated)"

# ═══════════════════════════════════════════════════════════════════
# Phase 8: teardown
# ═══════════════════════════════════════════════════════════════════
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
ok "  IAM-3 validated on a 3-node Hetzner cluster"
ok "  logs: $RUN_DIR"
ok "═══════════════════════════════════════════════"
