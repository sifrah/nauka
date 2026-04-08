#!/usr/bin/env bash
# E2E test: run nauka CLI commands like a real user.
# Uses --mode mock to avoid WireGuard/TiKV dependencies.
# Exit 1 on any failure.
set -euo pipefail

NAUKA="${NAUKA_BIN:-nauka}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

PASS=0
FAIL=0

check() {
    local name="$1"
    shift
    if "$@" > /dev/null 2>&1; then
        echo -e "  ${GREEN}✓${NC} $name"
        PASS=$((PASS + 1))
    else
        echo -e "  ${RED}✗${NC} $name"
        FAIL=$((FAIL + 1))
    fi
}

check_output() {
    local name="$1"
    local pattern="$2"
    shift 2
    local output
    output=$("$@" 2>&1) || true
    if echo "$output" | grep -q "$pattern"; then
        echo -e "  ${GREEN}✓${NC} $name"
        PASS=$((PASS + 1))
    else
        echo -e "  ${RED}✗${NC} $name (expected '$pattern' in output)"
        echo "    Got: $(echo "$output" | head -3)"
        FAIL=$((FAIL + 1))
    fi
}

cleanup() {
    # Kill any background nauka processes
    pkill -x nauka 2>/dev/null || true
    # Clean state
    echo y | $NAUKA hypervisor leave 2>/dev/null || true
    rm -rf ~/.nauka
}

echo "=== Nauka CLI E2E Tests (mock mode) ==="
echo ""

# Cleanup before start
cleanup 2>/dev/null

# ── Init ──
echo "Init:"
check_output "init succeeds" "initialized" \
    $NAUKA hypervisor init --region eu --zone test --mode mock \
    --s3-endpoint https://s3.example.com --s3-bucket test-bucket \
    --s3-access-key TESTKEY --s3-secret-key TESTSECRET

check_output "init detects already initialized" "already initialized" \
    $NAUKA hypervisor init --region eu --zone test --mode mock \
    --s3-endpoint https://s3.example.com --s3-bucket test-bucket \
    --s3-access-key TESTKEY --s3-secret-key TESTSECRET

# ── Status ──
echo "Status:"
check_output "status shows name" "$(hostname | tr '[:upper:]' '[:lower:]')" \
    $NAUKA hypervisor status

check_output "status shows region" "eu" \
    $NAUKA hypervisor status

# ── List ──
echo "List:"
check_output "list shows this node" "$(hostname | tr '[:upper:]' '[:lower:]')" \
    $NAUKA hypervisor list

# ── Doctor ──
echo "Doctor:"
check_output "doctor runs" "passed" \
    $NAUKA hypervisor doctor

# ── API Server ──
echo "API Server:"
$NAUKA serve --bind 127.0.0.1:18443 &
SERVE_PID=$!
sleep 2

check_output "health endpoint" '"status":"ok"' \
    curl -sf http://127.0.0.1:18443/health

check_output "list endpoint" '"data"' \
    curl -sf http://127.0.0.1:18443/admin/v1/hypervisors

check_output "openapi endpoint" '"openapi"' \
    curl -sf http://127.0.0.1:18443/openapi.json

kill $SERVE_PID 2>/dev/null
wait $SERVE_PID 2>/dev/null || true

# ── Stop / Start ──
echo "Lifecycle:"
check_output "stop succeeds" "stopped" \
    $NAUKA hypervisor stop

check_output "start succeeds" "started" \
    $NAUKA hypervisor start

# ── Leave ──
echo "Leave:"
check_output "leave succeeds" "Left the cluster" \
    bash -c "echo y | $NAUKA hypervisor leave"

check_output "status after leave fails" "not initialized" \
    $NAUKA hypervisor status

# ── Help ──
echo "Help:"
check_output "help shows version" "2.0.0" \
    $NAUKA --version

check_output "hypervisor help" "init" \
    $NAUKA hypervisor --help

# ── Summary ──
echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="

cleanup 2>/dev/null

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
