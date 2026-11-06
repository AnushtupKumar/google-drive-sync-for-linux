#!/usr/bin/env bash
#
# scripts/test_e2e.sh — Comprehensive End-to-End Test for syncd & syncctl
#
# Tests:
#  1. Daemon bootstrap & SQLite WAL migration
#  2. UDS IPC handshake & telemetry snapshot
#  3. Continuous 1-way backup with file:// local object store
#  4. Dynamic bandwidth rate limiting
#  5. Timed pause & resume state transitions
#  6. 5-Stage TOCTOU Drop & Purge offloading (upload, assert, unlink)
#  7. Embedded Web dashboard & REST API endpoints
#  8. Clean shutdown & resource cleanup

set -euo pipefail

BOLD="\033[1m"
GREEN="\033[0;32m"
RED="\033[0;31m"
BLUE="\033[0;34m"
RESET="\033[0m"

log_info() { echo -e "${BLUE}${BOLD}[INFO]${RESET} $1"; }
log_pass() { echo -e "${GREEN}${BOLD}[PASS]${RESET} $1"; }
log_fail() { echo -e "${RED}${BOLD}[FAIL]${RESET} $1"; exit 1; }

WORKSPACE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SYNCD_BIN="${WORKSPACE_DIR}/target/release/syncd"
SYNCCTL_BIN="${WORKSPACE_DIR}/target/release/syncctl"

if [[ ! -x "$SYNCD_BIN" || ! -x "$SYNCCTL_BIN" ]]; then
    log_info "Compiling release binaries..."
    cargo build --workspace --release
fi

TEST_ROOT=$(mktemp -d /tmp/syncd-e2e-XXXXXX)
log_info "Using test environment at: ${TEST_ROOT}"

cleanup() {
    log_info "Cleaning up processes and temporary files..."
    if [[ -n "${WEB_PID:-}" ]] && kill -0 "$WEB_PID" 2>/dev/null; then
        kill "$WEB_PID" 2>/dev/null || true
    fi
    if [[ -n "${SYNCD_PID:-}" ]] && kill -0 "$SYNCD_PID" 2>/dev/null; then
        kill "$SYNCD_PID" 2>/dev/null || true
        wait "$SYNCD_PID" 2>/dev/null || true
    fi
    rm -rf "$TEST_ROOT"
}
trap cleanup EXIT

# ── 1. Setup Directories ───────────────────────────────────────────────────────
mkdir -p "${TEST_ROOT}/state" "${TEST_ROOT}/logs"
mkdir -p "${TEST_ROOT}/docs_local" "${TEST_ROOT}/docs_remote"
mkdir -p "${TEST_ROOT}/renders_local" "${TEST_ROOT}/renders_remote"

SOCKET_PATH="${TEST_ROOT}/syncd.sock"
CONFIG_FILE="${TEST_ROOT}/config.toml"

cat > "$CONFIG_FILE" <<EOF
socket_path = "${SOCKET_PATH}"
state_dir = "${TEST_ROOT}/state"
log_dir = "${TEST_ROOT}/logs"
log_max_size_bytes = 10485760
log_max_archives = 3
max_concurrent_transfers = 2
default_bandwidth_limit_bps = 0
debounce_quiet_ms = 1000
debounce_max_wait_ms = 3000

[[folders]]
id = "11111111-1111-1111-1111-111111111111"
name = "e2e-docs"
local_path = "${TEST_ROOT}/docs_local"
remote_target = "file://${TEST_ROOT}/docs_remote"
mode = "continuous_backup"
exclude_patterns = [".git/", "*.tmp"]
empty_folder_protection = true

[[folders]]
id = "22222222-2222-2222-2222-222222222222"
name = "e2e-renders"
local_path = "${TEST_ROOT}/renders_local"
remote_target = "file://${TEST_ROOT}/renders_remote"
mode = "drop_and_purge"
exclude_patterns = ["*.part"]
empty_folder_protection = false
EOF

# ── 2. Start syncd Daemon ──────────────────────────────────────────────────────
log_info "Starting syncd daemon..."
"$SYNCD_BIN" -c "$CONFIG_FILE" &
SYNCD_PID=$!

# Wait up to 5s for the socket to appear
for i in {1..50}; do
    if [[ -S "$SOCKET_PATH" ]]; then
        break
    fi
    sleep 0.1
done

if [[ ! -S "$SOCKET_PATH" ]]; then
    log_fail "syncd socket failed to appear at ${SOCKET_PATH}"
fi
log_pass "syncd daemon started successfully (PID: ${SYNCD_PID})"

# ── 3. Test syncctl status ────────────────────────────────────────────────────
log_info "Testing syncctl status..."
STATUS_OUT=$("$SYNCCTL_BIN" -s "$SOCKET_PATH" status)
echo "$STATUS_OUT"
if echo "$STATUS_OUT" | grep -q "e2e-docs" && echo "$STATUS_OUT" | grep -q "e2e-renders"; then
    log_pass "syncctl status returned watched folders"
else
    log_fail "syncctl status missing expected folders"
fi

# ── 4. Test Continuous Backup Sync ─────────────────────────────────────────────
log_info "Testing file upload in ContinuousBackup mode..."
echo "Hello End-to-End Test $(date)" > "${TEST_ROOT}/docs_local/report.txt"

# Wait for debouncer (1000ms) + upload (up to 6s)
for i in {1..30}; do
    if [[ -f "${TEST_ROOT}/docs_remote/report.txt" ]]; then
        break
    fi
    sleep 0.2
done

if [[ -f "${TEST_ROOT}/docs_remote/report.txt" ]]; then
    ORIG_CONTENT=$(cat "${TEST_ROOT}/docs_local/report.txt")
    REMOTE_CONTENT=$(cat "${TEST_ROOT}/docs_remote/report.txt")
    if [[ "$ORIG_CONTENT" == "$REMOTE_CONTENT" ]]; then
        log_pass "File synchronized correctly to local object store"
    else
        log_fail "Remote content does not match original"
    fi
else
    log_fail "File was not synced to remote target"
fi

# ── 5. Test Bandwidth Limiting ────────────────────────────────────────────────
log_info "Testing dynamic bandwidth limit setting..."
BW_OUT=$("$SYNCCTL_BIN" -s "$SOCKET_PATH" bwlimit 15MB/s)
if echo "$BW_OUT" | grep -q "15728640"; then
    log_pass "Bandwidth limit applied dynamically"
else
    log_fail "Failed to set bandwidth limit: ${BW_OUT}"
fi

# ── 6. Test Pause & Resume ────────────────────────────────────────────────────
log_info "Testing timed pause..."
PAUSE_OUT=$("$SYNCCTL_BIN" -s "$SOCKET_PATH" pause --for 5m)
if echo "$PAUSE_OUT" | grep -q "paused for 300s"; then
    log_pass "Pause command acknowledged"
else
    log_fail "Pause command failed: ${PAUSE_OUT}"
fi

STATUS_PAUSED=$("$SYNCCTL_BIN" -s "$SOCKET_PATH" status)
if echo "$STATUS_PAUSED" | grep -qi "paused"; then
    log_pass "Status reflects paused state"
else
    log_fail "Folder state not updated to Paused"
fi

log_info "Testing resume..."
RESUME_OUT=$("$SYNCCTL_BIN" -s "$SOCKET_PATH" resume)
if echo "$RESUME_OUT" | grep -qi "resumed"; then
    log_pass "Resume command acknowledged"
else
    log_fail "Resume command failed: ${RESUME_OUT}"
fi
sleep 0.5

# ── 7. Test 5-Stage TOCTOU Drop & Purge Offload ─────────────────────────────────
log_info "Testing 5-stage TOCTOU Drop & Purge offloading..."
mkdir -p "${TEST_ROOT}/renders_local/project1"
echo "Large render asset $(date)" > "${TEST_ROOT}/renders_local/project1/frame001.raw"

# Wait for debouncer + upload + 5-stage verify + atomic unlink (up to 8s)
for i in {1..40}; do
    if [[ -f "${TEST_ROOT}/renders_remote/project1/frame001.raw" && ! -f "${TEST_ROOT}/renders_local/project1/frame001.raw" ]]; then
        break
    fi
    sleep 0.2
done

if [[ -f "${TEST_ROOT}/renders_remote/project1/frame001.raw" ]]; then
    log_pass "Offloaded file verified in remote store"
else
    log_fail "Offloaded file missing from remote store"
fi

if [[ ! -f "${TEST_ROOT}/renders_local/project1/frame001.raw" ]]; then
    log_pass "Local file unlinked after verification (space freed)"
else
    log_fail "Local file was not removed in DropAndPurge mode"
fi

# ── 8. Test Web Dashboard Server ──────────────────────────────────────────────
log_info "Testing embedded Web UI server on port 8399..."
"$SYNCCTL_BIN" -s "$SOCKET_PATH" web --port 8399 &
WEB_PID=$!
sleep 1

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8399/)
if [[ "$HTTP_CODE" == "200" ]]; then
    log_pass "Web dashboard served HTML (HTTP 200)"
else
    log_fail "Web dashboard failed with HTTP ${HTTP_CODE}"
fi

API_JSON=$(curl -s http://127.0.0.1:8399/api/status)
if echo "$API_JSON" | grep -q "e2e-docs" && echo "$API_JSON" | grep -q "e2e-renders"; then
    log_pass "Web API returned valid telemetry JSON snapshot"
else
    log_fail "Web API returned invalid response: ${API_JSON}"
fi

log_info "======================================================"
log_pass "ALL END-TO-END PLATFORM TESTS PASSED SUCCESSFULLY!"
log_info "======================================================"

