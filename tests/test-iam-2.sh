#!/usr/bin/env bash
# tests/test-iam-2.sh — Hetzner 3-node validation for IAM-2 (#346).
#
# Confirms end-to-end that:
#   - Org / Project / Env records replicate via Raft from leader to
#     followers (Writer::create path).
#   - `fn::iam::can` + `scope_by` / `permissions` clauses in the
#     DDL actually filter results when a user queries from any node.
#   - A second user who isn't the org owner sees an empty list on
#     every node — `PERMISSIONS` works against real mTLS-separated
#     SurrealDB sessions, not just the single-process integration
#     test.
#
# Same guardrails as IAM-1: wipe `nauka-dev-*` before provisioning,
# never pass `--peering` to init.
set -euo pipefail

NAUKA_BIN="${NAUKA_BIN:-target/x86_64-unknown-linux-musl/release/nauka}"
SSH_KEY_NAME="${SSH_KEY_NAME:-nauka-agent-local}"
SERVER_TYPE="${SERVER_TYPE:-cpx22}"
LOCATION="${LOCATION:-fsn1}"
IMAGE="${IMAGE:-ubuntu-24.04}"
NODE_COUNT=3

ALICE_EMAIL="alice@example.com"
ALICE_PW="alice-iam2-test"
BOB_EMAIL="bob@example.com"
BOB_PW="bob-iam2-test"
ORG_SLUG="acme"
PROJECT_SLUG="web"
ENV_SLUG="production"

RUN_DIR="/tmp/nauka-iam-2/$(date -u +%Y%m%dT%H%M%SZ)-$$"
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
# Phase 1: init + 2 joins
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
# Phase 2: create two users (alice on node-1, bob on node-2)
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ Phase 2: create bob + alice (bob first so node-1's token ends up as alice's) ═══"
# `nauka user create` auto-logs-in on the calling node, so the last
# `user create` dictates whose JWT sits in `~/.config/nauka/token`.
# Phase 3 expects alice's token on node-1, so create bob first.
# Creating both on the leader avoids the Raft-forward race a
# follower write would hit during a transient re-election.
create_bob=$(ssh_node "${IPS[0]}" "printf '%s\n%s\n' '$BOB_PW' '$BOB_PW' \
    | timeout 30 nauka user create --email '$BOB_EMAIL' --display-name 'Bob' 2>&1" || true)
echo "$create_bob" | grep -q "user created: $BOB_EMAIL" \
    || { echo "$create_bob" | sed 's/^/    /'; die "bob user create failed"; }
ok "  bob created on node-1"

create_alice=$(ssh_node "${IPS[0]}" "printf '%s\n%s\n' '$ALICE_PW' '$ALICE_PW' \
    | timeout 30 nauka user create --email '$ALICE_EMAIL' --display-name 'Alice' 2>&1" || true)
echo "$create_alice" | grep -q "user created: $ALICE_EMAIL" \
    || { echo "$create_alice" | sed 's/^/    /'; die "alice user create failed"; }
ok "  alice created on node-1 — node-1 now holds alice's token"

# ═══════════════════════════════════════════════════════════════════
# Phase 3: alice creates an Org + Project + Env on node-1
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ Phase 3: alice creates org / project / env ═══"
# node-1 already holds alice's token from the auto-login at user create.
org_out=$(ssh_node "${IPS[0]}" "timeout 30 nauka org create --slug '$ORG_SLUG' \
    --display-name 'Acme Corp' 2>&1" || true)
echo "$org_out" | grep -q "org created: $ORG_SLUG" \
    || { echo "$org_out" | sed 's/^/    /'; die "org create failed on node-1"; }
ok "  org $ORG_SLUG created (owner=alice)"

proj_out=$(ssh_node "${IPS[0]}" "timeout 30 nauka project create --org '$ORG_SLUG' \
    --slug '$PROJECT_SLUG' --display-name 'Web Platform' 2>&1" || true)
echo "$proj_out" | grep -q "project created:" \
    || { echo "$proj_out" | sed 's/^/    /'; die "project create failed on node-1"; }
ok "  project $ORG_SLUG-$PROJECT_SLUG created"

env_out=$(ssh_node "${IPS[0]}" "timeout 30 nauka env create --project '${ORG_SLUG}-${PROJECT_SLUG}' \
    --slug '$ENV_SLUG' --display-name 'Production' 2>&1" || true)
echo "$env_out" | grep -q "env created:" \
    || { echo "$env_out" | sed 's/^/    /'; die "env create failed on node-1"; }
ok "  env ${ORG_SLUG}-${PROJECT_SLUG}-${ENV_SLUG} created"

# ═══════════════════════════════════════════════════════════════════
# Phase 4: alice logs in on node-3 and sees everything
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ Phase 4: alice on node-3 sees her org tree (Raft replicated) ═══"
login_out=$(ssh_node "${IPS[2]}" "printf '%s\n' '$ALICE_PW' \
    | timeout 30 nauka login --email '$ALICE_EMAIL' 2>&1" || true)
echo "$login_out" | grep -q "logged in as $ALICE_EMAIL" \
    || { echo "$login_out" | sed 's/^/    /'; die "alice login on node-3 failed"; }
ok "  alice logged in on node-3"

org_list=$(ssh_node "${IPS[2]}" 'timeout 30 nauka org list 2>&1' || true)
echo "$org_list" | grep -q "orgs (1):" \
    || { echo "$org_list" | sed 's/^/    /'; die "alice should see 1 org on node-3"; }
echo "$org_list" | grep -q "$ORG_SLUG" \
    || { echo "$org_list" | sed 's/^/    /'; die "acme not in alice's org list on node-3"; }
ok "  alice on node-3 sees org $ORG_SLUG"

proj_list=$(ssh_node "${IPS[2]}" 'timeout 30 nauka project list 2>&1' || true)
echo "$proj_list" | grep -q "projects (1):" \
    || { echo "$proj_list" | sed 's/^/    /'; die "alice should see 1 project on node-3"; }
ok "  alice on node-3 sees project via scope_by = \"org\""

env_list=$(ssh_node "${IPS[2]}" 'timeout 30 nauka env list 2>&1' || true)
echo "$env_list" | grep -q "envs (1):" \
    || { echo "$env_list" | sed 's/^/    /'; die "alice should see 1 env on node-3"; }
ok "  alice on node-3 sees env via scope_by chain (env → project → org)"

# ═══════════════════════════════════════════════════════════════════
# Phase 5: bob logs in and sees NOTHING — PERMISSIONS filter works
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ Phase 5: bob sees empty lists (PERMISSIONS filter) ═══"
# Log out alice on node-3 so the next login stores bob's token.
ssh_node "${IPS[2]}" 'nauka logout 2>&1 >/dev/null' || true
bob_login=$(ssh_node "${IPS[2]}" "printf '%s\n' '$BOB_PW' \
    | timeout 30 nauka login --email '$BOB_EMAIL' 2>&1" || true)
echo "$bob_login" | grep -q "logged in as $BOB_EMAIL" \
    || { echo "$bob_login" | sed 's/^/    /'; die "bob login on node-3 failed"; }
ok "  bob logged in on node-3"

bob_orgs=$(ssh_node "${IPS[2]}" 'timeout 30 nauka org list 2>&1' || true)
echo "$bob_orgs" | grep -q "orgs (0):" \
    || { echo "$bob_orgs" | sed 's/^/    /'; die "bob should see 0 orgs"; }
ok "  bob sees 0 orgs"

bob_projects=$(ssh_node "${IPS[2]}" 'timeout 30 nauka project list 2>&1' || true)
echo "$bob_projects" | grep -q "projects (0):" \
    || { echo "$bob_projects" | sed 's/^/    /'; die "bob should see 0 projects"; }
ok "  bob sees 0 projects"

bob_envs=$(ssh_node "${IPS[2]}" 'timeout 30 nauka env list 2>&1' || true)
echo "$bob_envs" | grep -q "envs (0):" \
    || { echo "$bob_envs" | sed 's/^/    /'; die "bob should see 0 envs"; }
ok "  bob sees 0 envs"

# ═══════════════════════════════════════════════════════════════════
# Phase 6: bob tries to create an org claiming alice's slug — rejected
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ Phase 6: bob cannot steal alice's org slug ═══"
# Raft is the single source of truth: the id `org:acme` is taken,
# so the CREATE fails with an already-exists error even though bob
# is authenticated.
bob_steal=$(ssh_node "${IPS[2]}" "timeout 30 nauka org create --slug '$ORG_SLUG' \
    --display-name 'Bob Corp' 2>&1 || true")
echo "$bob_steal" | grep -qiE 'already exists|Database record' \
    || { echo "$bob_steal" | sed 's/^/    /'; die "bob's duplicate org create should have failed"; }
ok "  duplicate org rejected (already-exists)"

# ═══════════════════════════════════════════════════════════════════
# Phase 7: bob creates HIS OWN org — confirms PERMISSIONS allow it
# ═══════════════════════════════════════════════════════════════════
log ""
log "═══ Phase 7: bob creates his own org ═══"
bob_org=$(ssh_node "${IPS[2]}" "timeout 30 nauka org create --slug 'bobs-co' \
    --display-name 'Bobs Co' 2>&1" || true)
echo "$bob_org" | grep -q "org created: bobs-co" \
    || { echo "$bob_org" | sed 's/^/    /'; die "bob's own org create failed"; }
ok "  bob created bobs-co"

# bob now sees 1 org (his), alice still sees her 1 org (hers).
bob_orgs=$(ssh_node "${IPS[2]}" 'timeout 30 nauka org list 2>&1' || true)
echo "$bob_orgs" | grep -q "orgs (1):" \
    || { echo "$bob_orgs" | sed 's/^/    /'; die "bob should see exactly 1 org"; }
echo "$bob_orgs" | grep -q "bobs-co" \
    || { echo "$bob_orgs" | sed 's/^/    /'; die "bob should see bobs-co"; }
echo "$bob_orgs" | grep -q "$ORG_SLUG" \
    && { echo "$bob_orgs" | sed 's/^/    /'; die "bob should NOT see alice's acme"; }
ok "  bob sees only his own org — cross-user isolation confirmed"

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
ok "  IAM-2 validated on a 3-node Hetzner cluster"
ok "  logs: $RUN_DIR"
ok "═══════════════════════════════════════════════"
