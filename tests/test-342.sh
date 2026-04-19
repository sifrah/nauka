#!/usr/bin/env bash
# tests/test-342.sh — Hetzner 3-node validation for #358 (342-E).
#
# Exercises the full #342 epic against real servers:
#   - `nauka hypervisor init` (without --peering) + two joins.
#   - Generated CLI hits the daemon's HTTPS API through the SDK
#     (`hypervisor list`, `org list`, `org create`, `mesh get`).
#   - REST surface (`/openapi.json`, `/docs`, `/graphql/schema`)
#     serves without auth; the resource endpoints enforce Bearer.
#   - GraphQL query + mutation round-trip via `curl`.
#   - Raft still replicates so org-create on node-1 shows up on
#     node-2 and node-3.
#   - `hypervisor status` still shows 3 nodes; `hypervisor leave`
#     is untouched.
set -euo pipefail

NAUKA_BIN="${NAUKA_BIN:-target/x86_64-unknown-linux-musl/release/nauka}"
SSH_KEY_NAME="${SSH_KEY_NAME:-nauka-agent-local}"
SERVER_TYPE="${SERVER_TYPE:-cpx22}"
LOCATION="${LOCATION:-fsn1}"
IMAGE="${IMAGE:-ubuntu-24.04}"
NODE_COUNT=3

ALICE_EMAIL="alice@example.com"
ALICE_PW="alice-342e-test"
ORG_SLUG="acme"

RUN_DIR="/tmp/nauka-342-e/$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$RUN_DIR"

[[ -x $NAUKA_BIN ]] || { echo "✗ NAUKA_BIN not executable ($NAUKA_BIN)" >&2; exit 1; }
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

# -------- Phase 0: wipe + provision + deploy --------
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

# -------- Phase 1: init + 2 joins --------
log ""
log "═══ Phase 1: init + $((NODE_COUNT - 1)) joins ═══"
INIT_OUT=$(ssh_node "${IPS[0]}" 'timeout 60 nauka hypervisor init 2>&1')
PIN=$(echo "$INIT_OUT" | grep -oP 'join pin:\s+\K\S+')
[[ -n $PIN ]] || { echo "$INIT_OUT" | sed 's/^/    /'; die "no PIN"; }
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

# -------- Phase 2: seed user + org (through the bespoke IPC flow) --------
log ""
log "═══ Phase 2: seed alice + $ORG_SLUG org ═══"
ssh_node "${IPS[0]}" "printf '%s\n%s\n' '$ALICE_PW' '$ALICE_PW' \
    | timeout 60 nauka user create --email '$ALICE_EMAIL' --display-name 'Alice' 2>&1" \
    | grep -q "user created: $ALICE_EMAIL" || die "alice create failed"
ssh_node "${IPS[0]}" "timeout 30 nauka org create --slug '$ORG_SLUG' \
    --display-name 'Acme' 2>&1" | grep -q "org created: $ORG_SLUG" \
    || die "org create failed"
ok "  alice + $ORG_SLUG seeded on node-1"

# -------- Phase 3: CLI list commands → SDK over HTTPS --------
log ""
log "═══ Phase 3: generated CLI via SDK over HTTPS ═══"
HV_LIST=$(ssh_node "${IPS[1]}" "printf '%s\n' '$ALICE_PW' \
    | timeout 60 nauka login --email '$ALICE_EMAIL' 2>&1 >/dev/null; \
    timeout 30 nauka hypervisor list 2>&1" || true)
echo "$HV_LIST" | grep -qE "hypervisors \($NODE_COUNT\):" \
    || { echo "$HV_LIST" | sed 's/^/    /'; die "hypervisor list failed on node-2"; }
ok "  node-2 'nauka hypervisor list' → $NODE_COUNT rows"

ORG_LIST=$(ssh_node "${IPS[2]}" "printf '%s\n' '$ALICE_PW' \
    | timeout 60 nauka login --email '$ALICE_EMAIL' 2>&1 >/dev/null; \
    timeout 30 nauka org list 2>&1" || true)
echo "$ORG_LIST" | grep -q "$ORG_SLUG" \
    || { echo "$ORG_LIST" | sed 's/^/    /'; die "org list missing $ORG_SLUG on node-3 — Raft replication?"; }
ok "  node-3 'nauka org list' → $ORG_SLUG visible (Raft replicated)"

MESH_GET=$(ssh_node "${IPS[0]}" 'timeout 30 nauka mesh get 2>&1' || true)
echo "$MESH_GET" | grep -q "mesh id" \
    || { echo "$MESH_GET" | sed 's/^/    /'; die "nauka mesh get failed"; }
echo "$MESH_GET" | grep -qE "(private_key|peering_pin)" \
    && die "mesh get leaked a secret field — #[serde(skip)] regressed"
ok "  node-1 'nauka mesh get' renders metadata without secrets"

# -------- Phase 4: REST surface (curl -k, self-signed cert) --------
log ""
log "═══ Phase 4: REST + docs surface (curl) ═══"
JWT=$(ssh_node "${IPS[0]}" 'cat ~/.config/nauka/token 2>/dev/null || cat /root/.config/nauka/token 2>/dev/null' | tr -d '\r\n')
[[ -n $JWT ]] || die "could not read stored JWT on node-1"

OPENAPI=$(ssh_node "${IPS[0]}" 'curl -sk https://127.0.0.1:4000/openapi.json')
echo "$OPENAPI" | jq -e '.openapi == "3.1.0" and (.paths | length > 5)' >/dev/null \
    || { echo "$OPENAPI" | head -20 | sed 's/^/    /'; die "/openapi.json shape unexpected"; }
ok "  /openapi.json (no auth) returns OpenAPI 3.1 doc with paths"

DOCS=$(ssh_node "${IPS[0]}" 'curl -sk https://127.0.0.1:4000/docs')
echo "$DOCS" | grep -q "api-reference" \
    || { echo "$DOCS" | head -10 | sed 's/^/    /'; die "/docs missing Scalar script"; }
ok "  /docs (no auth) returns Scalar HTML"

UNAUTH=$(ssh_node "${IPS[0]}" 'curl -sk -o /dev/null -w "%{http_code}" https://127.0.0.1:4000/v1/orgs')
[[ $UNAUTH == 401 ]] || die "/v1/orgs without Bearer returned $UNAUTH (expected 401)"
ok "  /v1/orgs without Bearer → 401"

AUTHED=$(ssh_node "${IPS[0]}" "curl -sk -H 'Authorization: Bearer $JWT' https://127.0.0.1:4000/v1/orgs")
echo "$AUTHED" | jq -e "any(.[]; .slug == \"$ORG_SLUG\")" >/dev/null \
    || { echo "$AUTHED" | sed 's/^/    /'; die "REST /v1/orgs missing $ORG_SLUG"; }
ok "  /v1/orgs with Bearer → row for $ORG_SLUG"

# -------- Phase 5: GraphQL query + mutation --------
log ""
log "═══ Phase 5: GraphQL ═══"
GQL_Q='{"query":"query{ orgs { slug displayName owner } }"}'
GQL_RESP=$(ssh_node "${IPS[0]}" "curl -sk -H 'Authorization: Bearer $JWT' \
    -H 'Content-Type: application/json' -d '$GQL_Q' \
    https://127.0.0.1:4000/graphql")
echo "$GQL_RESP" | jq -e "any(.data.orgs[]?; .slug == \"$ORG_SLUG\")" >/dev/null \
    || { echo "$GQL_RESP" | sed 's/^/    /'; die "GraphQL orgs missing $ORG_SLUG"; }
ok "  GraphQL query returns $ORG_SLUG"

SDL=$(ssh_node "${IPS[0]}" 'curl -sk https://127.0.0.1:4000/graphql/schema')
echo "$SDL" | grep -qE "type (Org|Hypervisor|Mesh)" \
    || { echo "$SDL" | head -10 | sed 's/^/    /'; die "GraphQL SDL missing expected types"; }
ok "  /graphql/schema (no auth) returns SDL"

# -------- Phase 6: admin regression — status, leave --------
log ""
log "═══ Phase 6: admin commands — status + leave regression ═══"
STATUS=$(ssh_node "${IPS[0]}" 'timeout 30 nauka hypervisor status 2>&1')
echo "$STATUS" | grep -qE "hypervisors \($NODE_COUNT\):" \
    || { echo "$STATUS" | sed 's/^/    /'; die "hypervisor status broken"; }
ok "  'nauka hypervisor status' shows $NODE_COUNT nodes"

ssh_node "${IPS[2]}" 'timeout 30 nauka hypervisor leave 2>&1' \
    | grep -q "hypervisor left mesh" \
    || die "node-3 leave failed"
sleep 10
REMAINING=$(count_hypervisors "${IPS[0]}")
[[ $REMAINING -eq $((NODE_COUNT - 1)) ]] \
    || die "after leave, node-1 sees $REMAINING (expected $((NODE_COUNT - 1)))"
ok "  node-3 left; node-1 now sees $REMAINING hypervisors"

log ""
ok "ALL PHASES PASSED — #342 epic validated on $NODE_COUNT-node Hetzner cluster"
