# Linux Cloud Sync & File Offload Platform (`syncd` & `syncctl`)

[![License: MIT OR Apache-2.0](https://img.shields.io/badge/License-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.80%2B-orange.svg?logo=rust)](https://www.rust-lang.org)
[![Platform](https://img.shields.io/badge/Platform-Linux%20x86__64-brightgreen.svg?logo=linux)](https://kernel.org)
[![systemd](https://img.shields.io/badge/systemd-user%20service-informational.svg)](https://systemd.io)

A production-grade, crash-safe, real-time cloud synchronization and file-offloading platform for Linux written in Rust.

`syncd` runs as a resilient background user daemon under `systemd --user` managing recursive `inotify` watches, SQLite WAL queue persistence, and strict Time-of-Check to Time-of-Use (TOCTOU) file offloading. `syncctl` is the accompanying command-line client communicating over a Unix Domain Socket with framed JSON streaming telemetry, featuring an interactive keyboard-driven Terminal UI, an embedded local Web dashboard, and an automated onboarding wizard.

---

## Key Features

- **Native Google Drive Integration**: 100% pure Rust Drive v3 REST API implementation. No `rclone` daemon, FUSE mounts, or external binaries required.
- **Multi-Cloud Storage**: First-class support for **AWS S3**, **Cloudflare R2**, **MinIO**, **Google Cloud Storage (GCS)**, **Azure Blob Storage**, **Local/NAS/USB drives** (`file://`), and optional **rclone remotes** (`rclone://`).
- **Zero-Dependency Standalone Installer**: Single 13 MB self-extracting file (`syncd-installer.sh`) that installs on any x86_64 Linux PC in seconds without Rust, Cargo, or compiler toolchains.
- **5-Stage TOCTOU Safe Offloading**: Safely drop & purge local files (renders, videos, raw footage) only *after* verified chunked upload and SHA-256 integrity confirmation.
- **Hardware-Level Mount Safety**: Three-layer canary system (`.syncd-anchor`, filesystem `st_dev` ID, and kernel `/proc/self/mountinfo` epoll) prevents accidental cloud deletions when USB drives or network shares disconnect.
- **Empty Folder Safety Lock**: Circuit-breaker halts sync operations if a previously populated folder suddenly reports zero files.
- **Interactive First-Time Setup Wizard**: `syncctl setup` guides users through cloud authentication, folder configuration, collision prevention, and service startup.
- **Multiple Control Interfaces**:
  - **TUI Dashboard** (`syncctl`): Interactive keyboard-driven terminal dashboard with live progress meters, folder inspector, and modal controls.
  - **Embedded Web UI** (`syncctl web`): Responsive dark-mode browser dashboard served locally on port 8384.
  - **Live Terminal Monitor** (`syncctl watch`): Real-time streaming progress bars, transfer rates, and ETAs.
  - **Rich CLI** (`syncctl folder`, `syncctl status`, `syncctl bwlimit`, `syncctl pause`): Full programmatic automation support.
- **Dynamic Bandwidth Throttling**: Live rate-limiting adjustable mid-stream without restarting transfers.

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph Host ["Linux Host System"]
        FS["Filesystem / Watched Folders"] -->|inotify events| Watcher["watcher.rs\n(Recursive Inotify + Gitignore)"]
        Mounts["/proc/self/mountinfo"] -->|poll(2) EPOLLPRI/ERR| MountWatch["safety.rs\n(Mount Table Monitor)"]
        
        Watcher -->|Filtered Events| Debounce["debouncer.rs\n(Sliding-Window + Max-Wait Coalescer)"]
        Debounce -->|Settled Events| DB[("db.rs\nSQLite WAL Queue")]
        
        MountWatch -->|Hotplug/Unmount Event| Supervisor["supervisor.rs\n(Actor Orchestrator)"]
        Supervisor -->|Pause/Resume/Reconcile| Debounce
        Supervisor -->|Claim Tasks| Workers["Worker Pool\n(tokio Semaphore)"]
        
        Workers --> Pipeline{"Offload Pipeline"}
        Pipeline -->|ContinuousBackup| BackupFlow["Upload + SHA-256 + Cloud Trash Archival"]
        Pipeline -->|DropAndPurge| OffloadFlow["5-Stage TOCTOU Drop & Purge"]
        
        Workers -->|Throttled Stream| Limiter["storage/mod.rs\n(Token-Bucket Bandwidth Limiter)"]
        Limiter --> Cloud[("Cloud Object Storage\n(Google Drive / S3 / R2 / GCS / Azure)")]
        
        Supervisor -->|Telemetry Stream| IPCServer["ipc_server.rs\n(tokio UnixListener mode 0600)"]
    end

    subgraph Client ["CLI & TUI Clients"]
        Syncctl["syncctl\n(CLI / TUI / Web / Wizard)"] <-->|Length-Delimited JSON IPC| IPCServer
    end
```

---

## Subsystems & Safety Guarantees

### 1. 5-Stage TOCTOU-Safe Drop & Purge Pipeline (`offload.rs`)
To prevent data loss when freeing local disk space, files in `DropAndPurge` mode pass through a strict 5-stage verification state machine:

```text
Stage 1: Pre-Flight Stat & SHA-256
    • Open file with O_RDONLY (rejects unhandled symlinks)
    • Record pre_stat: (inode, size_bytes, mtime_sec, mtime_nsec)
    • Stream through sha2::Sha256 in-flight

Stage 2: Throttled Cloud Upload
    • Stream chunked bytes to cloud target via TokenBucketReader
    • Attach SHA-256 digest in object metadata

Stage 3: Strict Remote Verification
    • Execute HEAD request / metadata check against the cloud backend
    • Assert remote_object.size == pre_stat.size_bytes

Stage 4: TOCTOU Re-Stat Guard
    • Run symlink_metadata(local_path) immediately before deletion
    • Assert (post_ino == pre_ino && post_size == pre_size && post_mtime == pre_mtime)
    • Abort unlink and re-enqueue if modified or replaced during upload

Stage 5: Atomic Unlink & Bottom-Up Directory Pruning
    • Call unlink(2) on the validated file
    • Walk upward calling POSIX rmdir(2) on empty ancestor directories
    • rmdir(2) atomically fails with ENOTEMPTY on non-empty folders; stop pruning
```

### 2. Drive Disconnect & Canary Anchor (`safety.rs`)
External USB/SATA drives unmounting mid-sync can falsely present an empty directory on the root partition. `syncd` guards against this with three layers of defense:
* **Anchor Canary (`.syncd-anchor`)**: Writes a canary file with the folder UUID. If missing or mismatched, all operations halt immediately.
* **POSIX Device ID (`st_dev`)**: Records filesystem device ID on initial mount. If `st_dev` differs, the drive was unmounted or replaced.
* **Kernel `/proc/self/mountinfo` Watcher**: Spawns a background task calling `poll(2)` with `POLLPRI | POLLERR` on `/proc/self/mountinfo`. Upon any mount table change, active folders are inspected immediately:
  * Cancels active transfer tokens.
  * Purges in-memory and database `DELETE` tasks.
  * Transitions folder state to `FolderState::DriveDisconnected`.
  * Sends a desktop alert over D-Bus via `notify-rust`.
  * Runs a full background reconciliation walk once both `st_dev` and `.syncd-anchor` return.

### 3. Empty Folder Safety Lock
In `ContinuousBackup` mode, before executing any remote deletion:
* If visible file count drops to `0` while SQLite records a positive `baseline_file_count`, `syncd` trips the safety lock circuit breaker.
* Halts operations on that folder, sets `FolderState::Error { reason: "Empty Folder Safety Lock triggered" }`, and fires a critical desktop notification.

### 4. Sliding-Window Debouncer (`debouncer.rs`)
Prevents syncing partial files while preventing starvation on continuously written files:
* Coalesces incoming `inotify` events in an in-memory sliding window.
* **Delete Priority**: Within a single window, a `Delete` event overrides an `Upload`.
* **Flush Conditions**: Flushes to SQLite `transfer_queue` when:
  1. Activity has settled: `now - last_updated >= debounce_quiet_ms` (default 3s).
  2. Starvation cap: `now - first_seen >= debounce_max_wait_ms` (default 60s).

### 5. Dynamic Bandwidth Throttling & Decorrelated Backoff
* **Live Bandwidth Throttling**: Rate limiting is held in an `Arc<RwLock<Option<Arc<BandwidthLimiter>>>>`. Adjusting the rate via `syncctl bwlimit` swaps the limiter mid-stream without restarting transfers.
* **Exponential Backoff with Full Jitter**:
  $$\text{delay} \sim U\left(0,\, \min(300\text{s},\, 1.0 \times 2^{\text{attempt}})\right)$$
  Persisted in SQLite (`attempt_count`, `next_retry_ms`) so backoff survives service restarts.

---

## Installation

### Option 1: Standalone Single-File Installer (No Dependencies — Any Linux PC)

You can copy the single `syncd-installer.sh` file to **any Linux PC (x86_64)**. **Zero dependencies needed — no Rust, Cargo, or compiler required!**

```bash
# Run the self-extracting installer
bash syncd-installer.sh

# Or non-interactive / automated:
bash syncd-installer.sh --yes
```

The standalone installer:
1. Verifies Linux architecture (`x86_64`).
2. Extracts and installs `syncd` and `syncctl` into `~/.local/bin/`.
3. Creates default `~/.config/syncd/config.toml` (if not already present).
4. Configures, enables, and starts the background user service (`systemctl --user enable --now syncd`).
5. Prompts to immediately launch the first-time setup wizard (`syncctl setup`).

---

### Option 2: Project Repository Installer (`install.sh`)

Clone the repository and run `install.sh`:

```bash
git clone https://github.com/<your-username>/cloud-sync-platform.git
cd cloud-sync-platform
./install.sh
```

- **Instant install**: If pre-compiled binaries are present in `bin/` or `target/release/`, it installs them immediately without compiling.
- **Source compilation**: If binaries are not present, or if `--build` is passed, it compiles with `cargo build --release`.
- **Options**:
  - `./install.sh --yes` (non-interactive)
  - `./install.sh --build` (force rebuild from source)
  - `./install.sh --prefix /custom/bin`
  - `./install.sh --uninstall`

---

### Option 3: Compile from Source with Cargo

```bash
# 1. Clone repository
git clone https://github.com/<your-username>/cloud-sync-platform.git
cd cloud-sync-platform

# 2. Build release profile
cargo build --workspace --release

# 3. Install binaries
mkdir -p ~/.local/bin
install -m 755 target/release/syncd ~/.local/bin/syncd
install -m 755 target/release/syncctl ~/.local/bin/syncctl

# 4. Set up systemd user service
mkdir -p ~/.config/systemd/user
install -m 644 systemd/syncd.service ~/.config/systemd/user/syncd.service
systemctl --user daemon-reload
systemctl --user enable --now syncd
```

> [!TIP]
> Ensure `~/.local/bin` is in your `PATH`. If not, add `export PATH="$HOME/.local/bin:$PATH"` to your `~/.bashrc` or `~/.zshrc`.

---

## 3-Minute Quickstart Guide

After installation, configure your first sync folder with the interactive wizard:

```bash
syncctl setup
```

The wizard will guide you through:
1. **Cloud Authentication**: Connect your Google Drive, AWS S3 / Cloudflare R2, GCS, or Local folder.
2. **Watched Folder**: Choose the folder you want to sync (e.g. `~/Documents` or `~/Renders`).
3. **Sync Mode**:
   - `Backup [Safe]`: Keeps files on disk, mirrors edits, and archives deletions to cloud `.Trash-syncd/`.
   - `Offload`: Uploads files, verifies SHA-256, and safely purges local copies to free disk space.
4. **Daemon Activation**: Automatically reloads `syncd` to start watching and syncing immediately!

---

## Storage Provider Setup Guide

`syncd` supports cloud object stores natively, standard POSIX filesystems, and external services:

### 1. Native Google Drive (`gdrive://...`) — Pure Rust

Direct native synchronization with personal or workspace Google Drive via Google's Drive v3 REST API. Fully in-process and written in 100% pure Rust: **no `rclone` daemon or FUSE mounts required**.

#### Authentication:
Run the authentication command:
```bash
syncctl auth login
```
- Opens your default web browser for OAuth2 consent.
- Headless / SSH server? Use: `syncctl auth login --manual`
- Have an existing rclone configuration? Import instantly with: `syncctl auth import-rclone`

#### Configuration (`~/.config/syncd/config.toml`):
```toml
[[folders]]
id = "a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d"
name = "documents"
local_path = "~/Documents"
remote_target = "gdrive://Documents"
mode = "continuous_backup"
empty_folder_protection = true

[[folders]]
id = "f6e5d4c3-b2a1-0f9e-8d7c-6b5a4f3e2d1c"
name = "renders-dropzone"
local_path = "~/Renders"
remote_target = "gdrive://renders"
mode = "drop_and_purge"
empty_folder_protection = false
```

---

### 2. Amazon S3, Cloudflare R2, MinIO, or Wasabi (`s3://...`)

Set your credentials via standard environment variables or AWS credentials file:
```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
```

Configure `~/.config/syncd/config.toml`:
```toml
[[folders]]
id = "b2c3d4e5-f6a7-8b9c-0d1e-2f3a4b5c6d7e"
name = "s3-backups"
local_path = "~/Projects"
remote_target = "s3://my-backup-bucket/workstation/projects"
mode = "continuous_backup"
empty_folder_protection = true

# Optional: MinIO / Cloudflare R2 endpoint configuration
# endpoint = "https://<account-id>.r2.cloudflarestorage.com"
# region = "auto"
# allow_http = false
```

---

### 3. Google Cloud Storage (`gs://...`)

Set your Service Account credentials:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="~/.config/gcloud/service-account.json"
```

Configure `~/.config/syncd/config.toml`:
```toml
[[folders]]
id = "c3d4e5f6-a7b8-9c0d-1e2f-3a4b5c6d7e8f"
name = "gcs-archive"
local_path = "~/Renders"
remote_target = "gs://my-company-archive/renders"
mode = "drop_and_purge"
empty_folder_protection = false
```

---

### 4. Local Filesystem, External USB Drive, or NAS (`file://...`)

Sync directly to a local directory, mounted external drive, or NFS/SMB share:

```toml
[[folders]]
id = "d4e5f6a7-b8c9-0d1e-2f3a-4b5c6d7e8f9a"
name = "local-nas-backup"
local_path = "~/Documents"
remote_target = "file:///mnt/backup_drive/syncd_backups"
mode = "continuous_backup"
empty_folder_protection = true
```

---

### 5. Optional Rclone Remotes (`rclone://...`)

For cloud providers not yet natively integrated (e.g. OneDrive, Dropbox, Box, pCloud, WebDAV), `syncd` can sync directly via `rclone://<remote>/<folder>` without running `rclone mount`:

```toml
[[folders]]
name = "onedrive-backup"
local_path = "~/Documents"
remote_target = "rclone://onedrive/Documents"
mode = "continuous_backup"
```

---

## Sync Modes vs. Safety Lock Explained

| Feature | `Mode: Backup [Safe]` (`continuous_backup`) | `Mode: Offload` (`drop_and_purge`) |
| :--- | :--- | :--- |
| **Primary Goal** | Continuous 1-to-1 mirror copy. | Free local physical drive space (renders, raw archives). |
| **Local File Lifecycle** | Local file is **kept** on disk permanently. | Local file is **unlinked** once verified in cloud. |
| **Local Deletions** | Archives remote copy to `.Trash-syncd/`. | Preserves cloud archive (does not delete cloud file). |
| **Safety Integration** | **Armed**: Empty Folder Safety Lock prevents mass deletion if drive unmounts. | **Purge Mode**: Disabled (emptying local folder is expected). |
| **Tracking Metric** | Active files on disk and in database catalog. | SQLite catalog of all files preserved in the cloud. |

---

## CLI & Control Tool Reference (`syncctl`)

### 1. Interactive First-Time Setup Wizard
```bash
syncctl setup
```
Runs the onboarding interview to configure storage credentials, select folders, choose modes, validate paths, and activate the daemon.

---

### 2. Interactive Terminal UI Dashboard
```bash
syncctl
# or: syncctl ui
```
Full-screen terminal interface with live transfer cards, folder selector, bandwidth meters, and keyboard controls:
* `↑` / `↓` or `k` / `j`: Select folder.
* `Enter` or `i`: Inspect folder properties (UUID, mode, paths, anchor status).
* `p`: Open timed pause modal (e.g. `15m`, `1h`, or indefinite).
* `r`: Resume synchronization.
* `b`: Open bandwidth rate-limit modal.
* `s`: Force reconciliation scan.
* `d` / `x`: Delete folder configuration with confirmation.
* `?`: Show keyboard help overlay.
* `q`: Clean exit.

---

### 3. Embedded Web Dashboard
```bash
# Launch on default port (http://127.0.0.1:8384):
syncctl web

# Custom host and port:
syncctl web --bind 0.0.0.0 --port 9090
```
Open `http://127.0.0.1:8384` in any web browser to view real-time sync metrics, folder cards, active file progress, and one-click controls.

---

### 4. Live Telemetry Streaming
```bash
syncctl watch
```
Displays streaming progress bars, per-file transfer speeds, EWMA throughput, and ETAs updated every 500ms.

---

### 5. Folder Management Commands
```bash
# Interactive menu:
syncctl folder

# List all configured folders:
syncctl folder list
syncctl folder list --verbose

# Add a new folder:
syncctl folder add --name docs --path ~/Documents --target gdrive://Documents --mode backup
syncctl folder add --name renders --path ~/Renders --target s3://my-bucket/renders --mode offload

# Inspect folder:
syncctl folder show docs

# Edit folder settings:
syncctl folder edit docs --mode offload

# Remove folder:
syncctl folder remove docs

# Reload running daemon with updated config:
syncctl reload
```

---

### 6. Cloud Authentication Management
```bash
# Google Drive browser OAuth2 login:
syncctl auth login

# Headless / SSH server manual code:
syncctl auth login --manual

# Custom Google Cloud OAuth client credentials:
syncctl auth login --client-id "<CLIENT_ID>" --client-secret "<CLIENT_SECRET>"

# Inspect credentials status:
syncctl auth status

# Import existing rclone credentials:
syncctl auth import-rclone

# Log out / remove saved credentials:
syncctl auth logout
```

---

### 7. Bandwidth Throttling & Queue Management
```bash
# Set bandwidth rate limits dynamically:
syncctl bwlimit 5MB/s
syncctl bwlimit 500KB/s
syncctl bwlimit 0            # Unlimited

# Pause and resume:
syncctl pause --for 30m      # Pause for 30 minutes
syncctl pause --for 2h       # Pause for 2 hours
syncctl pause                # Pause indefinitely
syncctl resume               # Resume syncing

# Trigger immediate reconciliation scan:
syncctl scan
```

---

## Daemon Operation & Maintenance

### systemd Service Management
`syncd` runs as a standard Linux systemd user service:

```bash
# Check service status:
systemctl --user status syncd

# View live daemon logs:
journalctl --user -u syncd -f

# Restart daemon:
systemctl --user restart syncd

# Stop daemon:
systemctl --user stop syncd
```

### Configuration & Database File Locations
- **Configuration File**: `~/.config/syncd/config.toml`
- **Google Credentials**: `~/.config/syncd/gdrive_credentials.json` (chmod 600)
- **SQLite WAL Database**: `~/.local/share/syncd/syncd.db`
- **IPC Domain Socket**: `/run/user/<UID>/syncd.sock` (mode 0600)
- **Log Files**: `~/.local/share/syncd/logs/` (rotated and gzip-compressed)

---

## Packaging the Standalone Installer

To generate a new self-contained `syncd-installer.sh` bundle after modifying code:

```bash
# Build release binaries and bundle into standalone installer:
./scripts/package_standalone.sh
```

This creates `syncd-installer.sh` (and alias `cloud-sync-installer.sh`), which can be shared and run on any Linux PC with zero dependencies.

---

## Automated Test Suite

Run the full automated test suite covering SQLite WAL transactions, TOCTOU stat guards, anchor canaries, mount monitors, debouncer sliding-windows, and IPC framing:

```bash
cargo test --workspace
```

---

## Contributing

Contributions are welcome! Please feel free to submit issues, feature requests, or pull requests on GitHub.

1. Fork the repository.
2. Create your feature branch (`git checkout -b feature/my-new-feature`).
3. Commit your changes (`git commit -am 'Add some feature'`).
4. Ensure all tests pass (`cargo test --workspace`).
5. Push to the branch (`git push origin feature/my-new-feature`).
6. Create a new Pull Request.

---

## License

This project is dual-licensed under either:
* **MIT License** ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)
* **Apache License, Version 2.0** ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)

at your option.
