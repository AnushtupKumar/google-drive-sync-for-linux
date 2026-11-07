#!/usr/bin/env bash
#
# package_standalone.sh — Package syncd & syncctl into a Standalone Installer
#
# This script bundles the compiled release binaries, systemd unit, and sample
# configuration into a single, self-extracting .sh installer that can be copied
# and run on any x86_64 Linux PC without needing Rust, Cargo, or git installed.
#

set -euo pipefail

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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$SCRIPT_DIR"

OUTPUT_INSTALLER="${SCRIPT_DIR}/syncd-installer.sh"
ALIAS_INSTALLER="${SCRIPT_DIR}/cloud-sync-installer.sh"
SYNCD_BIN="${SCRIPT_DIR}/target/release/syncd"
SYNCCTL_BIN="${SCRIPT_DIR}/target/release/syncctl"
SERVICE_FILE="${SCRIPT_DIR}/systemd/syncd.service"
CONFIG_EXAMPLE="${SCRIPT_DIR}/config.example.toml"

echo -e "${BOLD}${CYAN}"
echo "============================================================"
echo "    Packaging syncd & syncctl into Standalone Installer     "
echo "============================================================"
echo -e "${RESET}"

# 1. Verify / Build release binaries
if [[ ! -x "$SYNCD_BIN" || ! -x "$SYNCCTL_BIN" ]]; then
    if [[ -x "${SCRIPT_DIR}/bin/syncd" && -x "${SCRIPT_DIR}/bin/syncctl" ]]; then
        SYNCD_BIN="${SCRIPT_DIR}/bin/syncd"
        SYNCCTL_BIN="${SCRIPT_DIR}/bin/syncctl"
    else
        log_info "Release binaries not found. Building with Cargo..."
        cargo build --workspace --release
    fi
fi

if [[ ! -x "$SYNCD_BIN" || ! -x "$SYNCCTL_BIN" ]]; then
    log_error "Failed to locate release binaries."
    exit 1
fi

log_success "Found release binaries:"
ls -lh "$SYNCD_BIN" "$SYNCCTL_BIN"

# Ensure repo bin/ directory is synced with prebuilt binaries
mkdir -p "${SCRIPT_DIR}/bin"
cp "$SYNCD_BIN" "${SCRIPT_DIR}/bin/syncd"
cp "$SYNCCTL_BIN" "${SCRIPT_DIR}/bin/syncctl"
chmod 755 "${SCRIPT_DIR}/bin/syncd" "${SCRIPT_DIR}/bin/syncctl"
log_success "Updated project bin/ directory with release binaries."

# 2. Create staging directory and archive
TMP_STAGE="$(mktemp -d -t syncd-pkg-XXXXXX)"
trap 'rm -rf "$TMP_STAGE"' EXIT

mkdir -p "$TMP_STAGE/payload"
cp "$SYNCD_BIN" "$TMP_STAGE/payload/syncd"
cp "$SYNCCTL_BIN" "$TMP_STAGE/payload/syncctl"
cp "$SERVICE_FILE" "$TMP_STAGE/payload/syncd.service"
cp "$CONFIG_EXAMPLE" "$TMP_STAGE/payload/config.example.toml"
chmod 755 "$TMP_STAGE/payload/syncd" "$TMP_STAGE/payload/syncctl"

log_info "Compressing payload (tar.gz)..."
tar -czf "$TMP_STAGE/payload.tar.gz" -C "$TMP_STAGE/payload" .
PAYLOAD_SIZE="$(du -h "$TMP_STAGE/payload.tar.gz" | cut -f1)"
log_success "Compressed payload created (${PAYLOAD_SIZE})"

# 3. Generate Standalone Self-Extracting Installer Script
log_info "Assembling standalone installer script..."

cat << 'EOF' > "$OUTPUT_INSTALLER"
#!/usr/bin/env bash
#
# syncd & syncctl — Standalone Linux Installer
# Self-extracting, zero-dependency installer for Linux (x86_64)
#
# Usage:
#   bash syncd-installer.sh            # Standard user installation
#   bash syncd-installer.sh --yes      # Non-interactive installation
#   bash syncd-installer.sh --uninstall# Remove installation
#

set -euo pipefail

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

NON_INTERACTIVE=false
DO_UNINSTALL=false
CUSTOM_PREFIX=""
SKIP_SERVICE=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        -y|--yes|--non-interactive)
            NON_INTERACTIVE=true
            shift
            ;;
        --uninstall|-u)
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
            echo "syncd Standalone Zero-Dependency Installer"
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -y, --yes              Non-interactive mode (use defaults without prompting)"
            echo "  --prefix <DIR>         Custom install prefix (default: ~/.local/bin or /usr/local/bin)"
            echo "  --skip-service         Do not configure or start the systemd user service"
            echo "  -u, --uninstall        Uninstall syncd, syncctl and systemd service"
            echo "  -h, --help             Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Run '$0 --help' for usage."
            exit 1
            ;;
    esac
done

# Determine target directories
if [[ -n "$CUSTOM_PREFIX" ]]; then
    BIN_DIR="${CUSTOM_PREFIX}"
elif [[ "$EUID" -eq 0 ]]; then
    BIN_DIR="/usr/local/bin"
else
    BIN_DIR="${HOME}/.local/bin"
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
    rm -f "${BIN_DIR}/syncd" "${BIN_DIR}/syncctl"
    rm -f "${SERVICE_FILE}"
    if command -v systemctl &>/dev/null; then
        systemctl --user daemon-reload &>/dev/null || true
    fi
    log_success "Binaries and systemd service removed from ${BIN_DIR}."
    echo "Note: Configuration at ${CONFIG_DIR} was preserved."
    exit 0
fi

echo -e "${BOLD}${CYAN}"
echo "============================================================"
echo "    syncd & syncctl — Linux Cloud Sync & Offload Platform   "
echo "    Standalone Zero-Dependency Installer (x86_64)           "
echo "============================================================"
echo -e "${RESET}"

# Verify OS & Architecture
OS_TYPE="$(uname -s)"
ARCH_TYPE="$(uname -m)"

if [[ "$OS_TYPE" != "Linux" ]]; then
    log_error "This installer is designed for Linux. Detected OS: ${OS_TYPE}"
    exit 1
fi

if [[ "$ARCH_TYPE" != "x86_64" ]]; then
    log_error "This standalone binary bundle is built for x86_64. Detected arch: ${ARCH_TYPE}"
    echo "For other architectures (e.g. aarch64 / ARM), please compile using 'cargo build --release'."
    exit 1
fi

# Verify required core tools (standard on virtually all Linux systems)
for req in base64 tar gzip; do
    if ! command -v "$req" &>/dev/null; then
        log_error "Missing required core tool: '${req}'. Please install it and re-run."
        exit 1
    fi
done

# 1. Unpack Payload
TMP_EXTRACT="$(mktemp -d -t syncd-standalone-XXXXXX)"
cleanup() {
    rm -rf "$TMP_EXTRACT"
}
trap cleanup EXIT

log_info "Unpacking standalone binaries..."
PAYLOAD_LINE=$(awk '/^__PAYLOAD_BEGINS__$/ { print NR + 1; exit 0; }' "$0")
if [[ -z "$PAYLOAD_LINE" ]]; then
    log_error "Corrupted installer: unable to find payload marker."
    exit 1
fi

tail -n +"$PAYLOAD_LINE" "$0" | base64 -d | tar -xz -C "$TMP_EXTRACT"
log_success "Payload successfully unpacked."

# 2. Install Binaries
log_info "Installing binaries into ${BIN_DIR}..."
mkdir -p "$BIN_DIR"
install -m 755 "$TMP_EXTRACT/syncd" "${BIN_DIR}/syncd"
install -m 755 "$TMP_EXTRACT/syncctl" "${BIN_DIR}/syncctl"
log_success "Installed 'syncd' and 'syncctl' into ${BIN_DIR}/"

# If ~/.cargo/bin exists and is writable, update it as well
if [[ -z "$CUSTOM_PREFIX" && -d "${HOME}/.cargo/bin" && "$BIN_DIR" != "${HOME}/.cargo/bin" && -w "${HOME}/.cargo/bin" ]]; then
    install -m 755 "$TMP_EXTRACT/syncd" "${HOME}/.cargo/bin/syncd" 2>/dev/null || true
    install -m 755 "$TMP_EXTRACT/syncctl" "${HOME}/.cargo/bin/syncctl" 2>/dev/null || true
fi

# 3. Setup Configuration
mkdir -p "$CONFIG_DIR" 2>/dev/null || true
if [[ -f "$CONFIG_FILE" ]]; then
    log_info "Existing configuration found at ${CONFIG_FILE} (preserved)."
else
    if [[ -f "$TMP_EXTRACT/config.example.toml" && -w "$CONFIG_DIR" ]]; then
        cp "$TMP_EXTRACT/config.example.toml" "$CONFIG_FILE"
        chmod 600 "$CONFIG_FILE"
        log_success "Created template configuration at ${CONFIG_FILE}"
    fi
fi

# 4. Install & Start systemd User Service
if [[ "$SKIP_SERVICE" = false && "$EUID" -ne 0 ]]; then
    log_info "Setting up systemd user service..."
    if mkdir -p "$SYSTEMD_USER_DIR" 2>/dev/null; then
        if [[ -f "$TMP_EXTRACT/syncd.service" ]]; then
            if install -m 644 "$TMP_EXTRACT/syncd.service" "$SERVICE_FILE" 2>/dev/null; then
                log_success "Installed unit file to ${SERVICE_FILE}"

                if command -v systemctl &>/dev/null; then
                    systemctl --user daemon-reload 2>/dev/null || true
                    log_info "Enabling and starting syncd user service..."
                    systemctl --user enable --now syncd 2>/dev/null || true
                    sleep 1
                    if systemctl --user is-active --quiet syncd 2>/dev/null; then
                        log_success "syncd.service is active and running!"
                    else
                        log_warn "syncd.service is not currently active. Check logs: journalctl --user -u syncd -e"
                    fi
                fi
            else
                log_warn "Could not write to ${SERVICE_FILE}. Skipping service setup."
            fi
        fi
    else
        log_warn "Could not create ${SYSTEMD_USER_DIR}. Skipping service setup."
    fi
elif [[ "$EUID" -eq 0 ]]; then
    log_info "Running as root: systemd user service skipped (run as regular user for desktop integration)."
fi

# 5. Check PATH
if [[ ":$PATH:" != *":$BIN_DIR:"* ]]; then
    echo ""
    log_warn "${BIN_DIR} is not in your current PATH."
    echo "To run 'syncctl' directly from any terminal, add this to ~/.bashrc or ~/.zshrc:"
    echo -e "${BOLD}  export PATH=\"${BIN_DIR}:\$PATH\"${RESET}"
    echo ""
fi

# 6. First-Time Setup Prompt
echo ""
echo -e "${GREEN}${BOLD}Installation Complete!${RESET}"
echo ""
echo "Quick Commands:"
echo "  • First-Time Onboarding Wizard:      syncctl setup"
echo "  • Cloud Auth & Google Drive Login:   syncctl auth login"
echo "  • Open Interactive TUI Dashboard:    syncctl"
echo "  • Manage Folders (Interactive CLI):  syncctl folder"
echo "  • Real-Time Upload Monitor:          syncctl watch"
echo "  • Check Status via CLI:              syncctl status"
echo "  • Launch Web Dashboard (Browser):    syncctl web"
echo ""

if [[ "$NON_INTERACTIVE" = false && -t 0 ]]; then
    read -r -p "Would you like to run the setup wizard ('syncctl setup') now? [Y/n]: " RUN_SETUP
    RUN_SETUP=${RUN_SETUP:-Y}
    if [[ "$RUN_SETUP" =~ ^[Yy]$ ]]; then
        export PATH="${BIN_DIR}:${PATH}"
        exec "${BIN_DIR}/syncctl" setup
    fi
fi

exit 0

__PAYLOAD_BEGINS__
EOF

# 4. Append Base64 Payload
base64 "$TMP_STAGE/payload.tar.gz" >> "$OUTPUT_INSTALLER"
chmod 755 "$OUTPUT_INSTALLER"

# Create duplicate alias cloud-sync-installer.sh
cp "$OUTPUT_INSTALLER" "$ALIAS_INSTALLER"
chmod 755 "$ALIAS_INSTALLER"

INSTALLER_SIZE="$(du -h "$OUTPUT_INSTALLER" | cut -f1)"
log_success "Standalone installer successfully generated:"
echo -e "  -> ${BOLD}${OUTPUT_INSTALLER}${RESET} (${INSTALLER_SIZE})"
echo -e "  -> ${BOLD}${ALIAS_INSTALLER}${RESET} (${INSTALLER_SIZE})"
echo ""
echo "You can now copy '${OUTPUT_INSTALLER}' to ANY x86_64 Linux machine and run:"
echo "  bash syncd-installer.sh"
echo "It installs syncd & syncctl with ZERO dependencies (no Rust, Cargo, or git required)!"
