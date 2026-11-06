#!/usr/bin/env bash
#
# install.sh — Production Installer for syncd & syncctl
# Linux Cloud Sync & File Offload Platform
#

set -euo pipefail

# ── Styling ───────────────────────────────────────────────────────────────────
BOLD="\033[1m"
GREEN="\033[0;32m"
CYAN="\033[0;36m"
YELLOW="\033[0;33m"
RED="\033[0;31m"
RESET="\033[0m"

log_info()    { echo -e "${CYAN}${BOLD}[INFO]${RESET} $1"; }
log_success() { echo -e "${GREEN}${BOLD}[OK]${RESET}   $1"; }
log_warn()    { echo -e "${YELLOW}${BOLD}[WARN]${RESET} $1"; }
log_error()   { echo -e "${RED}${BOLD}[ERR]${RESET}  $1"; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_INSTALL_DIR="${HOME}/.local/bin"
CONFIG_DIR="${HOME}/.config/syncd"
CONFIG_FILE="${CONFIG_DIR}/config.toml"
SYSTEMD_USER_DIR="${HOME}/.config/systemd/user"
SERVICE_FILE="${SYSTEMD_USER_DIR}/syncd.service"

echo -e "${BOLD}${CYAN}"
echo "============================================================"
echo "    syncd & syncctl — Linux Cloud Sync & Offload Platform   "
echo "============================================================"
echo -e "${RESET}"

# ── 1. Check Prerequisites ────────────────────────────────────────────────────
log_info "Checking prerequisites..."

# Check Rust / Cargo
if ! command -v cargo &>/dev/null; then
    if [[ -x "${HOME}/.cargo/bin/cargo" ]]; then
        export PATH="${HOME}/.cargo/bin:${PATH}"
    else
        log_error "Rust toolchain ('cargo') not found in PATH."
        echo "Please install Rust using rustup:"
        echo "  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
        exit 1
    fi
fi
log_success "Rust toolchain found: $(cargo --version)"

# Check optional rclone support
if command -v rclone &>/dev/null; then
    log_success "rclone found: $(rclone --version | head -n 1)"
else
    log_info "Optional: 'rclone' not found. Install rclone if you plan to sync with Google Drive, OneDrive, Dropbox, etc."
fi

# Check systemd user session
if ! command -v systemctl &>/dev/null; then
    log_warn "'systemctl' command not found. Daemon unit installation may fail."
fi

# ── 2. Build Release Binaries ──────────────────────────────────────────────────
log_info "Compiling release binaries with Cargo (this may take a few moments)..."
cd "$SCRIPT_DIR"
cargo build --workspace --release

SYNCD_BIN="${SCRIPT_DIR}/target/release/syncd"
SYNCCTL_BIN="${SCRIPT_DIR}/target/release/syncctl"

if [[ ! -x "$SYNCD_BIN" || ! -x "$SYNCCTL_BIN" ]]; then
    log_error "Build failed: binary output not found in target/release."
    exit 1
fi
log_success "Release binaries built successfully."

# ── 3. Install Binaries ────────────────────────────────────────────────────────
log_info "Installing binaries to ${BIN_INSTALL_DIR}..."
mkdir -p "$BIN_INSTALL_DIR"

# Atomically install to avoid 'Text file busy' errors if daemon is running
install -m 755 "$SYNCD_BIN" "${BIN_INSTALL_DIR}/syncd"
install -m 755 "$SYNCCTL_BIN" "${BIN_INSTALL_DIR}/syncctl"
log_success "Installed 'syncd' and 'syncctl' into ${BIN_INSTALL_DIR}/"

# Also install to ~/.cargo/bin if present in user environment
if [[ -d "${HOME}/.cargo/bin" ]]; then
    install -m 755 "$SYNCD_BIN" "${HOME}/.cargo/bin/syncd"
    install -m 755 "$SYNCCTL_BIN" "${HOME}/.cargo/bin/syncctl"
    log_success "Also updated binaries in ${HOME}/.cargo/bin/"
fi

# ── 4. Set Up Configuration ───────────────────────────────────────────────────
log_info "Configuring syncd daemon..."
mkdir -p "$CONFIG_DIR"

if [[ -f "$CONFIG_FILE" ]]; then
    log_info "Existing configuration found at ${CONFIG_FILE} (preserved)."
else
    if [[ -f "${SCRIPT_DIR}/config.example.toml" ]]; then
        cp "${SCRIPT_DIR}/config.example.toml" "$CONFIG_FILE"
        chmod 600 "$CONFIG_FILE"
        log_success "Created template configuration at ${CONFIG_FILE}"
        log_warn "Remember to edit ${CONFIG_FILE} to set up your desired sync folders and storage targets!"
    fi
fi

# ── 5. Install & Start systemd User Service ───────────────────────────────────
log_info "Setting up systemd user service..."
mkdir -p "$SYSTEMD_USER_DIR"

if [[ -f "${SCRIPT_DIR}/systemd/syncd.service" ]]; then
    install -m 644 "${SCRIPT_DIR}/systemd/syncd.service" "$SERVICE_FILE"
    log_success "Installed unit file to ${SERVICE_FILE}"

    if command -v systemctl &>/dev/null; then
        systemctl --user daemon-reload
        log_info "Restarting syncd service..."
        systemctl --user enable --now syncd
        sleep 1
        if systemctl --user is-active --quiet syncd; then
            log_success "syncd.service is active and running!"
        else
            log_warn "syncd.service is not currently active. Check logs via: journalctl --user -u syncd -e"
        fi
    fi
fi

# ── 6. Check PATH Environment ─────────────────────────────────────────────────
if [[ ":$PATH:" != *":$BIN_INSTALL_DIR:"* ]]; then
    echo ""
    log_warn "${BIN_INSTALL_DIR} is not currently in your PATH environment variable."
    echo "To run 'syncctl' directly from any terminal, add the following to your ~/.bashrc or ~/.zshrc:"
    echo -e "${BOLD}  export PATH=\"\$HOME/.local/bin:\$PATH\"${RESET}"
    echo ""
fi

# ── 7. Verification Summary ───────────────────────────────────────────────────
echo ""
echo -e "${GREEN}${BOLD}Installation Complete!${RESET}"
echo ""
echo "Quick Commands:"
echo "  • Open Interactive TUI Dashboard:    syncctl"
echo "  • Manage Folders (Interactive CLI):  syncctl folder"
echo "  • Real-Time Upload Monitor:          syncctl watch"
echo "  • Check Status via CLI:              syncctl status"
echo "  • Reload Daemon After Config Edit:   syncctl reload"
echo "  • Launch Web Dashboard (Browser):    syncctl web"
echo "  • View Live Daemon Logs:             journalctl --user -u syncd -f"
echo "  • Edit Sync Folders Config:          syncctl folder edit <NAME>  (or nano ~/.config/syncd/config.toml)"
echo "  • Restart Daemon Service:            systemctl --user restart syncd"
echo ""

