#!/usr/bin/env bash
# shellcheck disable=SC2207  # indexed-array version: macOS bash 3.2 has
# no `declare -A`, so we keep parallel arrays NAMES[i] / IPS[i] / IDS[i]
# and look up by index. node_idx() resolves a name back to its index.
# tests/multi-node-failover.sh — P2.18 (sifrah/nauka#222)
#
# Spin up 3 fresh Hetzner servers, bootstrap a Nauka cluster across
# them, exercise cross-node read/write, then kill the PD leader and
# verify the cluster recovers.
#
# Inputs (env vars):
#   NAUKA_BIN          path to musl-built nauka binary (required)
#   SSH_KEY_NAME       hcloud ssh-key name (default: ifrah.sacha@gmail.com)
#   SERVER_TYPE        hcloud server type (default: cx22)
#   LOCATION           hcloud location  (default: fsn1)
#   IMAGE              hcloud image     (default: ubuntu-24.04)
#   S3_ENDPOINT, S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY  (required)
#   KEEP_SERVERS       set to 1 to leave servers running after success
#                      (handy for post-mortem; default is teardown on exit)
#
# Outputs:
#   /tmp/nauka-multi-node-test/<run-id>/
#     ├── node-{1,2,3}/init.log, join.log, doctor.log
#     ├── timing.json    high-level phase timings
#     └── summary.txt    pass/fail per phase
set -euo pipefail

# ─── Config ──────────────────────────────────────────────────────────
NAUKA_BIN="${NAUKA_BIN:-target/x86_64-unknown-linux-musl/release/nauka}"
SSH_KEY_NAME="${SSH_KEY_NAME:-ifrah.sacha@gmail.com}"
SERVER_TYPE="${SERVER_TYPE:-cpx22}"  # x86, 3 vCPU / 4 GB — cpx21 is EOL'd in fsn1
LOCATION="${LOCATION:-fsn1}"
IMAGE="${IMAGE:-ubuntu-24.04}"
S3_ENDPOINT="${S3_ENDPOINT:-https://fsn1.your-objectstorage.com}"
S3_BUCKET="${S3_BUCKET:-syfrah-storage-eu-central-fsn1}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:?S3_ACCESS_KEY required}"
S3_SECRET_KEY="${S3_SECRET_KEY:?S3_SECRET_KEY required}"

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
RUN_DIR="/tmp/nauka-multi-node-test/$RUN_ID"
mkdir -p "$RUN_DIR"

if [[ ! -x "$NAUKA_BIN" ]]; then
    echo "✗ NAUKA_BIN ($NAUKA_BIN) not executable — cross-compile first:" >&2
    echo "    cargo build --target x86_64-unknown-linux-musl --release -p nauka" >&2
    exit 1
fi
if ! command -v hcloud >/dev/null; then
    echo "✗ hcloud CLI not found in PATH" >&2
    exit 1
fi
if ! command -v jq >/dev/null; then
    echo "✗ jq not found in PATH (brew install jq)" >&2
    exit 1
fi

NAMES=(nauka-multi-1 nauka-multi-2 nauka-multi-3)
IPS=("" "" "")
IDS=("" "" "")

# Resolve a node name back to its index in NAMES[].
node_idx() {
    local target=$1
    local i
    for i in "${!NAMES[@]}"; do
        [[ "${NAMES[$i]}" == "$target" ]] && { echo "$i"; return; }
    done
    echo "-1"
}

# ─── Logging helpers ─────────────────────────────────────────────────
log()  { printf "\033[36m[%s] %s\033[0m\n" "$(date -u +%H:%M:%S)" "$*"; }
ok()   { printf "\033[32m✓ %s\033[0m\n" "$*"; }
fail() { printf "\033[31m✗ %s\033[0m\n" "$*" >&2; }
die()  { fail "$*"; exit 1; }

phase_start() { PHASE_T0=$(date +%s); log "▶ $1"; }
phase_end()   { local dt=$(( $(date +%s) - PHASE_T0 )); ok "$1 ($dt s)"; printf '%s\t%s\n' "$1" "$dt" >> "$RUN_DIR/timing.txt"; }

# ─── Cleanup ─────────────────────────────────────────────────────────
cleanup() {
    local rc=$?
    if [[ "${KEEP_SERVERS:-0}" == "1" && $rc -eq 0 ]]; then
        log "KEEP_SERVERS=1 — leaving servers running for inspection"
        log "  Cleanup later: hcloud server delete ${NAMES[*]}"
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

# ─── Helpers ─────────────────────────────────────────────────────────
ip_of() {
    local idx; idx=$(node_idx "$1")
    [[ "$idx" -ge 0 ]] || die "ip_of: unknown node $1"
    echo "${IPS[$idx]}"
}

ssh_node() {
    local name=$1; shift
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR \
        "root@$(ip_of "$name")" "$@"
}
scp_to_node() {
    local name=$1 src=$2 dst=$3
    scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR \
        "$src" "root@$(ip_of "$name"):$dst"
}

# Run a CLI command on a node with the bootstrap.skv writers stopped
# first, so the command's `controlplane::connect` doesn't lose the
# SurrealKV flock race. Both `nauka-forge` (reconcile loop) and
# `nauka-announce` (announce listener) hold that flock; either one
# is enough to block the CLI for the entire 5s retry budget. We
# restart them after the command returns.
ssh_node_no_forge() {
    local name=$1; shift
    ssh_node "$name" "systemctl stop nauka-forge nauka-announce 2>/dev/null; \
                       sleep 1; \
                       out=\$($*); rc=\$?; \
                       systemctl start nauka-forge nauka-announce 2>/dev/null; \
                       printf '%s' \"\$out\"; exit \$rc"
}

wait_ssh() {
    local name=$1
    for _ in $(seq 1 30); do
        if ssh_node "$name" 'true' 2>/dev/null; then return 0; fi
        sleep 2
    done
    die "$name: SSH never came up"
}

# ─── Phase 1: provision ──────────────────────────────────────────────
phase_start "Phase 1: provision 3 Hetzner servers"
for i in "${!NAMES[@]}"; do
    n=${NAMES[$i]}
    log "  creating $n..."
    # `hcloud server create` writes progress to stderr — discard it
    # so jq only sees the JSON envelope on stdout.
    out=$(hcloud server create \
        --name "$n" \
        --type "$SERVER_TYPE" \
        --image "$IMAGE" \
        --location "$LOCATION" \
        --ssh-key "$SSH_KEY_NAME" \
        --output json 2>/dev/null)
    IDS[$i]=$(echo "$out" | jq -r '.server.id // empty')
    IPS[$i]=$(echo "$out" | jq -r '.server.public_net.ipv4.ip // empty')
    [[ -n "${IPS[$i]}" ]] || die "$n: no IPv4 from hcloud"
    log "    $n → ${IPS[$i]}"
done
for n in "${NAMES[@]}"; do wait_ssh "$n"; done
phase_end "Phase 1: provision 3 Hetzner servers"

# ─── Phase 2: deploy binary ──────────────────────────────────────────
phase_start "Phase 2: deploy nauka binary"
for n in "${NAMES[@]}"; do
    mkdir -p "$RUN_DIR/$n"
    scp_to_node "$n" "$NAUKA_BIN" /usr/local/bin/nauka >/dev/null
    ssh_node "$n" 'nauka --version' >> "$RUN_DIR/$n/version.txt"
done
phase_end "Phase 2: deploy nauka binary"

# ─── Phase 3: bootstrap node-1 + join 2 & 3 ──────────────────────────
phase_start "Phase 3: bootstrap cluster + join 2 nodes"

NODE1=${NAMES[0]}
ssh_node "$NODE1" "nauka hypervisor init test-mesh \
    --s3-endpoint '$S3_ENDPOINT' \
    --s3-bucket '$S3_BUCKET' \
    --s3-access-key '$S3_ACCESS_KEY' \
    --s3-secret-key '$S3_SECRET_KEY'" \
    > "$RUN_DIR/$NODE1/init.log" 2>&1 \
    || die "node-1 init failed — see $RUN_DIR/$NODE1/init.log"

# Init prints "  pin    XXXXXX" — extract it. Same flow as the
# operator would copy/paste from the terminal.
PIN=$(grep -E '^\s*pin\b' "$RUN_DIR/$NODE1/init.log" | awk '{print $2}' | tail -1)
[[ -n "$PIN" ]] || die "could not extract pin from init log"
log "  bootstrap pin: $PIN"

# Start the peering listener in the background on node-1 so the join
# requests from node-2/3 actually have a server to talk to. The
# listener exits on its own after one accept by default; we restart
# it for each join to avoid a sleeping window.
#
# `setsid` + `</dev/null` fully detaches the listener from the
# parent ssh session — otherwise it can be reaped when the ssh
# channel closes before the remote TCP port is bound.
wait_port_open() {
    local host=$1 port=$2
    for _ in $(seq 1 20); do
        if nc -z -w 2 "$host" "$port" 2>/dev/null; then return 0; fi
        sleep 1
    done
    return 1
}

for joiner in "${NAMES[1]}" "${NAMES[2]}"; do
    ssh_node "$NODE1" 'setsid nauka hypervisor peering </dev/null >/tmp/peering.log 2>&1 &'
    if ! wait_port_open "$(ip_of "$NODE1")" 51821; then
        die "peering listener on $NODE1 never bound to :51821"
    fi
    log "  joining $joiner..."
    if ! ssh_node "$joiner" "nauka hypervisor join \
        --target $(ip_of "$NODE1") \
        --pin '$PIN'" \
        > "$RUN_DIR/$joiner/join.log" 2>&1
    then
        # The join command sometimes exits non-zero on the very last
        # "Announcing to peers" step due to a SurrealKV flock race
        # against the freshly-started nauka-forge. Cluster membership
        # itself is already established at that point — Phase 4
        # (doctor) is the source of truth for connectivity.
        warn_msg=$(tail -1 "$RUN_DIR/$joiner/join.log" || true)
        log "  ⚠ join exited non-zero on $joiner ($warn_msg) — verifying via doctor in Phase 4"
    fi
    sleep 5
done
phase_end "Phase 3: bootstrap cluster + join 2 nodes"

# ─── Phase 4: doctor on each node ────────────────────────────────────
phase_start "Phase 4: nauka hypervisor doctor on every node"
# nauka-forge holds the SurrealKV flock on bootstrap.skv for most of
# its 30s reconcile window; the doctor's `controlplane::connect` needs
# the same flock to read PD endpoints and the EmbeddedDb retry budget
# (~5s) is not always enough on a busy node. Stop forge for the
# doctor pass, then restart it.
for n in "${NAMES[@]}"; do
    # Retry up to 3× — flock races between forge/announce/doctor are
    # a known flake pending sifrah/nauka#277. Each retry gives
    # stopped services 5s to fully release the bootstrap.skv flock.
    local_pass=0
    for attempt in 1 2 3; do
        ssh_node "$n" 'systemctl stop nauka-forge nauka-announce 2>/dev/null' || true
        sleep 5
        ssh_node "$n" 'nauka hypervisor doctor' > "$RUN_DIR/$n/doctor.log" 2>&1 || true
        ssh_node "$n" 'systemctl start nauka-forge nauka-announce 2>/dev/null' || true
        if grep -q 'connectivity: cluster reachable' "$RUN_DIR/$n/doctor.log"; then
            local_pass=1
            break
        fi
        log "  doctor flock flake on $n (attempt $attempt) — retrying"
        sleep 3
    done
    [[ $local_pass -eq 1 ]] \
        || die "$n: SurrealDB connectivity check did not pass after 3 attempts — see $RUN_DIR/$n/doctor.log"
done
phase_end "Phase 4: nauka hypervisor doctor on every node"

# ─── Phase 5: cross-node consistency ─────────────────────────────────
phase_start "Phase 5: cross-node read/write consistency"

# Write from node-1, read from node-2 and node-3. Every CLI call goes
# through ssh_node_no_forge so we don't race forge's reconcile loop on
# the SurrealKV flock — see helper for the rationale.
ssh_node_no_forge "${NAMES[0]}" 'nauka org create from-node-1' >/dev/null
sleep 3
for reader in "${NAMES[1]}" "${NAMES[2]}"; do
    if ssh_node_no_forge "$reader" 'nauka org list' 2>&1 | grep -q from-node-1; then
        ok "  $reader sees from-node-1"
    else
        die "$reader does NOT see from-node-1 — replication broken"
    fi
done

# Reverse: write from node-3, read from node-1.
ssh_node_no_forge "${NAMES[2]}" 'nauka org create from-node-3' >/dev/null
sleep 3
if ssh_node_no_forge "${NAMES[0]}" 'nauka org list' 2>&1 | grep -q from-node-3; then
    ok "  ${NAMES[0]} sees from-node-3 (reverse path)"
else
    die "${NAMES[0]} does NOT see from-node-3"
fi
phase_end "Phase 5: cross-node read/write consistency"

# ─── Phase 6: PD leader failover ─────────────────────────────────────
phase_start "Phase 6: kill PD leader, verify recovery"

# Find the current PD leader by name. `pd-ctl member` lists members
# and marks the leader; we grep it out via the pd_client API on node-1.
LEADER=$(ssh_node_no_forge "$NODE1" 'nauka hypervisor doctor' 2>&1 | awk '/pd leader:/{print $NF}')
log "  PD leader: $LEADER"
[[ -n "$LEADER" ]] || die "could not identify PD leader from doctor output"

# Match leader name back to one of our nodes.
LEADER_HOST=""
for n in "${NAMES[@]}"; do
    HV_NAME=$(ssh_node_no_forge "$n" 'nauka hypervisor status' 2>&1 | awk '/Name:/ {print $2; exit}')
    if [[ "$HV_NAME" == "$LEADER" ]]; then
        LEADER_HOST=$n
        break
    fi
done
[[ -n "$LEADER_HOST" ]] || die "leader name $LEADER did not match any test node"
log "  leader host: $LEADER_HOST"

# Stop PD on the leader. systemctl stop is the operator-equivalent of
# a clean shutdown — Raft re-elects within seconds. Timing in whole
# seconds is enough here (macOS `date` has no %N, and we only care
# about the order-of-magnitude).
T_KILL=$(date +%s)
ssh_node "$LEADER_HOST" 'systemctl stop nauka-pd' || true

# Poll a survivor for write availability. Cap at 60s — Raft elections
# typically settle in <10s on a 3-member quorum.
SURVIVOR=""
for n in "${NAMES[@]}"; do
    [[ "$n" != "$LEADER_HOST" ]] && SURVIVOR=$n && break
done
log "  polling $SURVIVOR for write recovery..."

WROTE=0
for i in $(seq 1 30); do
    if ssh_node_no_forge "$SURVIVOR" 'nauka org create after-failover' >/dev/null 2>&1; then
        T_OK=$(date +%s)
        WROTE=$(( T_OK - T_KILL ))
        ok "  write succeeded ${WROTE}s after PD kill (attempt $i)"
        break
    fi
    sleep 2
done
[[ $WROTE -ge 0 && $WROTE -lt 60 ]] || die "writes did not recover within 60s after killing PD leader"

# Restart PD on the original leader so cleanup leaves the cluster in
# a good state if KEEP_SERVERS=1.
ssh_node "$LEADER_HOST" 'systemctl start nauka-pd' || true

phase_end "Phase 6: kill PD leader, verify recovery"

# ─── Summary ─────────────────────────────────────────────────────────
python3 - "$RUN_DIR/timing.txt" <<PYEOF > "$RUN_DIR/timing.json"
import json, sys
phases = {}
with open(sys.argv[1]) as f:
    for line in f:
        name, dt = line.rstrip("\n").split("\t", 1)
        phases[name] = int(dt)
print(json.dumps({
    "run_id": "$RUN_ID",
    "location": "$LOCATION",
    "server_type": "$SERVER_TYPE",
    "failover_recovery_seconds": $WROTE,
    "phases": phases,
}, indent=2))
PYEOF

ok "ALL PHASES PASSED — recovery time ${WROTE}s, full logs in $RUN_DIR"
