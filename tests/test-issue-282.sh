#!/usr/bin/env bash
# tests/test-issue-282.sh — reproduction + regression guard for
# sifrah/nauka#282 ("nauka hypervisor join exits non-zero on
# 'Announcing to peers' despite successful join").
#
# Spin up 2 fresh Hetzner servers, init node-1, join node-2 and
# ASSERT that the join exits 0. Before the fix, join exited 1
# on the last step with a SurrealKV LOCK error because the
# announce systemd service had already grabbed bootstrap.skv's
# flock while the CLI was still trying to re-open the DB.
#
# Inputs (env vars):
#   NAUKA_BIN          path to musl-built nauka binary
#                      (default: target/x86_64-unknown-linux-musl/release/nauka)
#   SSH_KEY_NAME       hcloud ssh-key name (default: ifrah.sacha@gmail.com)
#   SERVER_TYPE        hcloud server type  (default: cpx22)
#   LOCATION           hcloud location     (default: fsn1)
#   IMAGE              hcloud image        (default: ubuntu-24.04)
#   S3_ENDPOINT        (default: https://fsn1.your-objectstorage.com)
#   S3_BUCKET          (default: syfrah-storage-eu-central-fsn1)
#   AWS_PROFILE        (default: hetzner-s3) — read from ~/.aws/credentials
#   KEEP_SERVERS       1 to leave the servers running on success
#
# Exit 0 ⇒ #282 is fixed. Exit 1 ⇒ regression.
set -euo pipefail

NAUKA_BIN="${NAUKA_BIN:-target/x86_64-unknown-linux-musl/release/nauka}"
SSH_KEY_NAME="${SSH_KEY_NAME:-nauka-agent-local}"
SERVER_TYPE="${SERVER_TYPE:-cpx22}"
LOCATION="${LOCATION:-fsn1}"
IMAGE="${IMAGE:-ubuntu-24.04}"
S3_ENDPOINT="${S3_ENDPOINT:-https://fsn1.your-objectstorage.com}"
S3_BUCKET="${S3_BUCKET:-syfrah-storage-eu-central-fsn1}"
AWS_PROFILE="${AWS_PROFILE:-hetzner-s3}"

# Load S3 creds from ~/.aws/credentials under [$AWS_PROFILE].
creds_file="${HOME}/.aws/credentials"
if [[ ! -r $creds_file ]]; then
    echo "✗ $creds_file not readable" >&2
    exit 1
fi
S3_ACCESS_KEY=$(awk -v p="[$AWS_PROFILE]" '
    $0==p {in_p=1; next}
    in_p && /^\[/ {exit}
    in_p && /aws_access_key_id/ {print $NF; exit}' "$creds_file")
S3_SECRET_KEY=$(awk -v p="[$AWS_PROFILE]" '
    $0==p {in_p=1; next}
    in_p && /^\[/ {exit}
    in_p && /aws_secret_access_key/ {print $NF; exit}' "$creds_file")
[[ -n $S3_ACCESS_KEY && -n $S3_SECRET_KEY ]] \
    || { echo "✗ could not read [$AWS_PROFILE] from $creds_file" >&2; exit 1; }

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
RUN_DIR="/tmp/nauka-issue-282/$RUN_ID"
mkdir -p "$RUN_DIR"

[[ -x $NAUKA_BIN ]] || {
    echo "✗ NAUKA_BIN ($NAUKA_BIN) not executable — cross-compile first:" >&2
    echo "    cargo build --target x86_64-unknown-linux-musl --release -p nauka" >&2
    exit 1
}
command -v hcloud >/dev/null || { echo "✗ hcloud CLI not found" >&2; exit 1; }
command -v jq     >/dev/null || { echo "✗ jq not found"     >&2; exit 1; }

NAMES=(nauka-282-1 nauka-282-2)
IPS=("" "")

log()  { printf "\033[36m[%s] %s\033[0m\n" "$(date -u +%H:%M:%S)" "$*"; }
ok()   { printf "\033[32m✓ %s\033[0m\n" "$*"; }
fail() { printf "\033[31m✗ %s\033[0m\n" "$*" >&2; }
die()  { fail "$*"; exit 1; }

cleanup() {
    local rc=$?
    if [[ ${KEEP_SERVERS:-0} == 1 && $rc -eq 0 ]]; then
        log "KEEP_SERVERS=1 — leaving servers; cleanup: hcloud server delete ${NAMES[*]}"
        return
    fi
    log "tearing down test servers..."
    for n in "${NAMES[@]}"; do
        hcloud server delete "$n" >/dev/null 2>&1 || true
    done
    if [[ $rc -ne 0 ]]; then
        fail "test FAILED — logs preserved in $RUN_DIR"
    fi
}
trap cleanup EXIT

ssh_node() {
    local ip=$1; shift
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        -o LogLevel=ERROR "root@$ip" "$@"
}
scp_to() {
    local ip=$1 src=$2 dst=$3
    scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        -o LogLevel=ERROR "$src" "root@$ip:$dst"
}
wait_ssh() {
    local ip=$1
    # Fresh Hetzner cpx22 boots in ~90s, allow 4 min budget.
    for _ in $(seq 1 60); do
        ssh_node "$ip" true 2>/dev/null && return 0
        sleep 4
    done
    die "SSH never came up on $ip"
}
wait_port() {
    local host=$1 port=$2
    for _ in $(seq 1 20); do
        nc -z -w 2 "$host" "$port" 2>/dev/null && return 0
        sleep 1
    done
    return 1
}

# ─── Provision ───────────────────────────────────────────────────────
log "▶ provisioning 2 Hetzner servers ($SERVER_TYPE/$LOCATION)"
for i in "${!NAMES[@]}"; do
    n=${NAMES[$i]}
    # Best-effort delete in case a previous run left something behind.
    hcloud server delete "$n" >/dev/null 2>&1 || true
    out=$(hcloud server create \
        --name "$n" \
        --type "$SERVER_TYPE" \
        --image "$IMAGE" \
        --location "$LOCATION" \
        --ssh-key "$SSH_KEY_NAME" \
        --output json 2>/dev/null)
    IPS[$i]=$(echo "$out" | jq -r '.server.public_net.ipv4.ip // empty')
    [[ -n ${IPS[$i]} ]] || die "$n: no IPv4 from hcloud"
    log "    $n → ${IPS[$i]}"
done
for ip in "${IPS[@]}"; do wait_ssh "$ip"; done
ok "provisioned"

# ─── Deploy binary ───────────────────────────────────────────────────
log "▶ deploying nauka binary"
for ip in "${IPS[@]}"; do
    scp_to "$ip" "$NAUKA_BIN" /usr/local/bin/nauka >/dev/null
    ssh_node "$ip" 'chmod +x /usr/local/bin/nauka && nauka --version' \
        > "$RUN_DIR/version-$ip.txt"
done
ok "deployed"

# ─── Init node-1 ─────────────────────────────────────────────────────
NODE1_IP=${IPS[0]}
NODE2_IP=${IPS[1]}
mkdir -p "$RUN_DIR/node-1" "$RUN_DIR/node-2"

log "▶ init node-1"
ssh_node "$NODE1_IP" "nauka hypervisor init test-282 \
    --s3-endpoint '$S3_ENDPOINT' \
    --s3-bucket '$S3_BUCKET' \
    --s3-access-key '$S3_ACCESS_KEY' \
    --s3-secret-key '$S3_SECRET_KEY'" \
    > "$RUN_DIR/node-1/init.log" 2>&1 \
    || die "node-1 init failed — see $RUN_DIR/node-1/init.log"

PIN=$(grep -E '^\s*pin\b' "$RUN_DIR/node-1/init.log" | awk '{print $2}' | tail -1)
[[ -n $PIN ]] || die "could not extract pin from init log"
log "    pin: $PIN"

# ─── Start peering listener ──────────────────────────────────────────
log "▶ starting peering listener on node-1"
ssh_node "$NODE1_IP" 'setsid nauka hypervisor peering </dev/null >/tmp/peering.log 2>&1 &'
wait_port "$NODE1_IP" 51821 || die "peering listener never bound to :51821"
ok "listener bound"

# ─── THE ASSERTION: join must exit 0 ─────────────────────────────────
log "▶ joining node-2 (asserting exit 0 — #282 regression guard)"
set +e
ssh_node "$NODE2_IP" "nauka hypervisor join \
    --target $NODE1_IP \
    --pin '$PIN'" \
    > "$RUN_DIR/node-2/join.log" 2>&1
JOIN_RC=$?
set -e

if [[ $JOIN_RC -ne 0 ]]; then
    fail "#282 REGRESSION: join exited $JOIN_RC"
    echo "─── last 20 lines of join.log ─────────────────────────────" >&2
    tail -20 "$RUN_DIR/node-2/join.log" >&2
    echo "───────────────────────────────────────────────────────────" >&2
    exit 1
fi

ok "join exited 0 — #282 fixed ✅"

# ─── Sanity: join output reports at least 1 peer ─────────────────────
# The join command prints a trailing summary block that includes a
# `peers N` line. Anything ≥1 means node-2 actually joined the mesh.
log "▶ sanity: join summary reports ≥1 peer"
PEER_COUNT=$(awk '/^ *peers +/ {print $2; exit}' "$RUN_DIR/node-2/join.log")
[[ ${PEER_COUNT:-0} -ge 1 ]] \
    || die "node-2 join summary reports '${PEER_COUNT}' peers, expected ≥1"
ok "node-2 joined with $PEER_COUNT peer(s)"

ok "ALL CHECKS PASSED — logs in $RUN_DIR"
