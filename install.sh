#!/usr/bin/env bash
#
# install.sh — Production Installer for syncd & syncctl
# Linux Cloud Sync & File Offload Platform
#
# Features:
#   • Automatically uses pre-compiled binaries if present (no Rust/Cargo needed)
#   • Compiles from source with Cargo if pre-compiled binaries are not present (or if --build is passed)
#   • Sets up systemd user service for background daemon management
#   • Configures ~/.config/syncd/config.toml
#   • Supports uninstall, custom install prefix, and non-interactive automation
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

# ── Command-Line Argument Parsing ─────────────────────────────────────────────
NON_INTERACTIVE=false
DO_BUILD=false
DO_UNINSTALL=false
CUSTOM_PREFIX=""
SKIP_SERVICE=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        -y|--yes|--non-interactive)
            NON_INTERACTIVE=true
            shift
            ;;
        -b|--build|--rebuild)
            DO_BUILD=true
            shift
            ;;
        -u|--uninstall)
            DO_UNINSTALL=true
            shift
            ;;
        --prefix)
            CUSTOM_PREFIX="$2"
            shift 2
            ;;
        --skip-service)
            SKIP_SERVICE=true
            shift
            ;;
        -h|--help)
            echo "syncd & syncctl Installer"
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -b, --build            Force recompiling binaries from source with Cargo"
            echo "  -y, --yes              Non-interactive mode (use defaults without prompting)"
            echo "  --prefix <DIR>         Custom install directory (default: ~/.local/bin or /usr/local/bin)"
            echo "  --skip-service         Do not configure or start the systemd user service"
            echo "  -u, --uninstall        Uninstall syncd, syncctl and systemd service"
            echo "  -h, --help             Show this help message"
            echo ""
            echo "Tip: For a single, standalone zero-dependency installer file, run:"
            echo "  ./syncd-installer.sh"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Run '$0 --help' for usage."
            exit 1
            ;;
    esac
done

# Determine installation directory
if [[ -n "$CUSTOM_PREFIX" ]]; then
    BIN_INSTALL_DIR="${CUSTOM_PREFIX}"
elif [[ "$EUID" -eq 0 ]]; then
    BIN_INSTALL_DIR="/usr/local/bin"
else
    BIN_INSTALL_DIR="${HOME}/.local/bin"
fi

CONFIG_DIR="${HOME}/.config/syncd"
CONFIG_FILE="${CONFIG_DIR}/config.toml"
SYSTEMD_USER_DIR="${HOME}/.config/systemd/user"
SERVICE_FILE="${SYSTEMD_USER_DIR}/syncd.service"

# Handle uninstall
if [[ "$DO_UNINSTALL" = true ]]; then
    echo -e "${BOLD}${YELLOW}Uninstalling syncd & syncctl...${RESET}"
    if command -v systemctl &>/dev/null; then
        systemctl --user stop syncd &>/dev/null || true
        systemctl --user disable syncd &>/dev/null || true
    fi
    rm -f "${BIN_INSTALL_DIR}/syncd" "${BIN_INSTALL_DIR}/syncctl"
    rm -f "${SERVICE_FILE}"
    if command -v systemctl &>/dev/null; then
        systemctl --user daemon-reload &>/dev/null || true
    fi
    log_success "Binaries and systemd service removed from ${BIN_INSTALL_DIR}."
    echo "Note: Configuration at ${CONFIG_DIR} was preserved."
    exit 0
fi

echo -e "${BOLD}${CYAN}"
echo "============================================================"
echo "    syncd & syncctl — Linux Cloud Sync & Offload Platform   "
echo "============================================================"
echo -e "${RESET}"

# ── 1. Locate or Compile Binaries ──────────────────────────────────────────────
SYNCD_BIN=""
SYNCCTL_BIN=""

# Check if pre-compiled release binaries already exist in repo
if [[ "$DO_BUILD" = false ]]; then
    if [[ -x "${SCRIPT_DIR}/bin/syncd" && -x "${SCRIPT_DIR}/bin/syncctl" ]]; then
        SYNCD_BIN="${SCRIPT_DIR}/bin/syncd"
        SYNCCTL_BIN="${SCRIPT_DIR}/bin/syncctl"
        log_success "Using pre-compiled binaries found in bin/ (no Cargo build needed)"
    elif [[ -x "${SCRIPT_DIR}/target/release/syncd" && -x "${SCRIPT_DIR}/target/release/syncctl" ]]; then
        SYNCD_BIN="${SCRIPT_DIR}/target/release/syncd"
        SYNCCTL_BIN="${SCRIPT_DIR}/target/release/syncctl"
        log_success "Using pre-compiled binaries found in target/release/ (no Cargo build needed)"
    fi
fi

# If binaries need to be compiled
if [[ -z "$SYNCD_BIN" || -z "$SYNCCTL_BIN" ]]; then
    log_info "Pre-compiled binaries not found or --build requested. Checking Rust toolchain..."

    if ! command -v cargo &>/dev/null; then
        if [[ -x "${HOME}/.cargo/bin/cargo" ]]; then
            export PATH="${HOME}/.cargo/bin:${PATH}"
        else
            log_error "Rust toolchain ('cargo') not found in PATH."
            echo ""
            echo "Options to resolve:"
            echo "  1. Install Rust via rustup:"
            echo "       curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
            echo "  2. Or run the standalone zero-dependency installer (no Rust needed):"
            echo "       ./syncd-installer.sh"
            echo ""
            exit 1
        fi
    fi
    log_success "Rust toolchain found: $(cargo --version)"

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

    # Keep bin/ synced
    mkdir -p "${SCRIPT_DIR}/bin"
    cp "$SYNCD_BIN" "${SCRIPT_DIR}/bin/syncd"
    cp "$SYNCCTL_BIN" "${SCRIPT_DIR}/bin/syncctl"
fi

# ── 2. Install Binaries ────────────────────────────────────────────────────────
log_info "Installing binaries to ${BIN_INSTALL_DIR}..."
mkdir -p "$BIN_INSTALL_DIR"

# Atomically install to avoid 'Text file busy' errors if daemon is running
install -m 755 "$SYNCD_BIN" "${BIN_INSTALL_DIR}/syncd"
install -m 755 "$SYNCCTL_BIN" "${BIN_INSTALL_DIR}/syncctl"
log_success "Installed 'syncd' and 'syncctl' into ${BIN_INSTALL_DIR}/"

# Also install to ~/.cargo/bin if present in user environment
if [[ -z "$CUSTOM_PREFIX" && -d "${HOME}/.cargo/bin" && "$BIN_INSTALL_DIR" != "${HOME}/.cargo/bin" && -w "${HOME}/.cargo/bin" ]]; then
    install -m 755 "$SYNCD_BIN" "${HOME}/.cargo/bin/syncd" 2>/dev/null || true
    install -m 755 "$SYNCCTL_BIN" "${HOME}/.cargo/bin/syncctl" 2>/dev/null || true
    log_success "Also updated binaries in ${HOME}/.cargo/bin/"
fi

# ── 3. Set Up Configuration ───────────────────────────────────────────────────
log_info "Configuring syncd daemon..."
mkdir -p "$CONFIG_DIR" 2>/dev/null || true

if [[ -f "$CONFIG_FILE" ]]; then
    log_info "Existing configuration found at ${CONFIG_FILE} (preserved)."
else
    if [[ -f "${SCRIPT_DIR}/config.example.toml" && -w "$CONFIG_DIR" ]]; then
        cp "${SCRIPT_DIR}/config.example.toml" "$CONFIG_FILE"
        chmod 600 "$CONFIG_FILE"
        log_success "Created template configuration at ${CONFIG_FILE}"
        log_warn "Remember to edit ${CONFIG_FILE} or use 'syncctl setup' to configure sync folders!"
    fi
fi

# ── 4. Install & Start systemd User Service ───────────────────────────────────
if [[ "$SKIP_SERVICE" = false && "$EUID" -ne 0 ]]; then
    log_info "Setting up systemd user service..."
    if mkdir -p "$SYSTEMD_USER_DIR" 2>/dev/null; then
        if [[ -f "${SCRIPT_DIR}/systemd/syncd.service" ]]; then
            if install -m 644 "${SCRIPT_DIR}/systemd/syncd.service" "$SERVICE_FILE" 2>/dev/null; then
                log_success "Installed unit file to ${SERVICE_FILE}"

                if command -v systemctl &>/dev/null; then
                    systemctl --user daemon-reload 2>/dev/null || true
                    log_info "Restarting syncd service..."
                    systemctl --user enable --now syncd 2>/dev/null || true
                    sleep 1
                    if systemctl --user is-active --quiet syncd 2>/dev/null; then
                        log_success "syncd.service is active and running!"
                    else
                        log_warn "syncd.service is not currently active. Check logs via: journalctl --user -u syncd -e"
                    fi
                fi
            else
                log_warn "Could not write to ${SERVICE_FILE}. Skipping systemd service setup."
            fi
        fi
    fi
elif [[ "$EUID" -eq 0 ]]; then
    log_info "Running as root: systemd user service skipped (run as regular user for desktop integration)."
fi

# ── 5. Check PATH Environment ─────────────────────────────────────────────────
if [[ ":$PATH:" != *":$BIN_INSTALL_DIR:"* ]]; then
    echo ""
    log_warn "${BIN_INSTALL_DIR} is not currently in your PATH environment variable."
    echo "To run 'syncctl' directly from any terminal, add the following to your ~/.bashrc or ~/.zshrc:"
    echo -e "${BOLD}  export PATH=\"${BIN_INSTALL_DIR}:\$PATH\"${RESET}"
    echo ""
fi

# ── 6. Verification & Quick Commands ──────────────────────────────────────────
echo ""
echo -e "${GREEN}${BOLD}Installation Complete!${RESET}"
echo ""
echo "Quick Commands:"
echo "  • First-Time Onboarding Wizard:      syncctl setup"
echo "  • Cloud Auth & Google Drive Login:   syncctl auth login  (or: syncctl auth status)"
echo "  • Open Interactive TUI Dashboard:    syncctl"
echo "  • Manage Folders (Interactive CLI):  syncctl folder"
echo "  • Real-Time Upload Monitor:          syncctl watch"
echo "  • Check Status via CLI:              syncctl status"
echo "  • Reload Daemon After Config Edit:   syncctl reload"
echo "  • Launch Web Dashboard (Browser):    syncctl web"
echo "  • View Live Daemon Logs:             journalctl --user -u syncd -f"
echo "  • Edit Sync Folders Config:          syncctl folder edit <NAME>"
echo "  • Restart Daemon Service:            systemctl --user restart syncd"
echo ""

# ── 7. Interactive Onboarding Prompt ──────────────────────────────────────────
if [[ "$NON_INTERACTIVE" = false && -t 0 ]]; then
    read -r -p "Would you like to run the first-time setup wizard ('syncctl setup') now? [Y/n]: " RUN_SETUP
    RUN_SETUP=${RUN_SETUP:-Y}
    if [[ "$RUN_SETUP" =~ ^[Yy]$ ]]; then
        export PATH="${BIN_INSTALL_DIR}:${PATH}"
        exec "${BIN_INSTALL_DIR}/syncctl" setup
    fi
fi
