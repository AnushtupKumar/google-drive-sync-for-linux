# Linux Cloud Sync & File Offload Platform (`syncd` & `syncctl`)

A production-grade, crash-safe, real-time cloud synchronization and file-offloading platform for Linux written in Rust.

`syncd` runs as a resilient background user daemon under `systemd --user` managing recursive `inotify` watches, SQLite WAL queue persistence, and strict Time-of-Check to Time-of-Use (TOCTOU) file offloading. `syncctl` is the accompanying command-line client communicating over a Unix Domain Socket with framed JSON streaming telemetry.

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
        Limiter --> Cloud[("Cloud Object Storage\n(S3 / GCS / Azure / HTTP)")]
        
        Supervisor -->|Telemetry Stream| IPCServer["ipc_server.rs\n(tokio UnixListener mode 0600)"]
    end

    subgraph Client ["CLI Client"]
        Syncctl["syncctl\n(Clap v4 + Comfy-Table + Indicatif)"] <-->|Length-Delimited JSON IPC| IPCServer
    end
```

---

## Key Subsystems & Safety Guarantees

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
    • Execute HEAD request against the cloud storage backend
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
* **Anchor Canary (`.syncd-anchor`)**: Writes a canary file with the folder UUID. If missing or mismatched, all operations halt.
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
* Coalesces incoming inotify events in an in-memory sliding window.
* **Delete Priority**: Within a single window, a `Delete` event overrides an `Upload`.
* **Flush Conditions**: Flushes to SQLite `transfer_queue` when:
  1. Activity has settled: `now - last_updated >= debounce_quiet_ms` (default 3s).
  2. Starvation cap: `now - first_seen >= debounce_max_wait_ms` (default 60s).

### 5. Dynamic Bandwidth Throttling & Decorrelated Backoff
* **Live Bandwidth Throttling**: Rate limiting is held in an `Arc<RwLock<Option<Arc<BandwidthLimiter>>>>`. Adjusting the rate via `syncctl bwlimit` swaps the limiter mid-stream without restarting transfers.
* **Exponential Backoff with Full Jitter**:
  $$\text{delay} \sim U\left(0,\, \min(300\text{s},\, 1.0 \times 2^{\text{attempt}})\right)$$
  Persisted in SQLite (`attempt_count`, `next_retry_ms`) so backoff persists across restarts.

---

## Workspace Structure

```text
cloud-sync-platform/
├── Cargo.toml
├── config.example.toml
├── systemd/
│   └── syncd.service
└── crates/
    ├── sync-common/
    │   ├── Cargo.toml
    │   └── src/
    │       ├── lib.rs
    │       ├── config.rs        # TOML configuration & collision validation
    │       ├── ipc.rs           # LengthDelimitedCodec + JSON IPC framing
    │       └── state.rs         # FolderState, TransferProgress, Telemetry
    ├── syncd/
    │   ├── Cargo.toml
    │   ├── migrations/
    │   │   └── 0001_init.sql    # SQLite WAL schema
    │   └── src/
    │       ├── main.rs          # Daemon entrypoint, sd_notify, signals
    │       ├── supervisor.rs    # Actor orchestrator, ticker, worker pool
    │       ├── watcher.rs       # Recursive inotify + gitignore engine
    │       ├── debouncer.rs     # Sliding-window coalescing debouncer
    │       ├── safety.rs        # st_dev, .syncd-anchor, mountinfo epoll
    │       ├── offload.rs       # 5-stage TOCTOU Drop & Purge pipeline
    │       ├── storage/
    │       │   ├── mod.rs       # CloudStorage trait, BandwidthLimiter
    │       │   └── rclone.rs    # ObjectStoreBackend (S3/GCS/Azure/HTTP)
    │       ├── db.rs            # SQLite WAL connection pool & transactions
    │       ├── ipc_server.rs    # Unix domain socket listener (mode 0600)
    │       └── logging.rs       # FileRotate gzip compression + D-Bus alerts
    └── syncctl/
        ├── Cargo.toml
        └── src/
            ├── main.rs          # Clap v4 CLI client + command dispatch
            ├── tui.rs           # Interactive keyboard-driven Terminal UI
            ├── web.rs           # Embedded local HTTP dashboard server
            └── dashboard.html   # Zero-dependency responsive browser UI
```

---

## Installation & Setup

### Option A: Standalone Single-File Installer (Zero Dependencies — Any Linux PC)

You can copy the single `syncd-installer.sh` file to **any Linux PC (x86_64)** and install immediately. **No Rust, Cargo, or compiler required!**

```bash
# Run the self-extracting standalone installer
bash syncd-installer.sh

# Or non-interactive / automated:
bash syncd-installer.sh --yes
```

The standalone installer:
1. Verifies Linux architecture (x86_64).
2. Unpacks pre-compiled binaries into `~/.local/bin/` (`syncd` and `syncctl`).
3. Sets up `~/.config/syncd/config.toml` template.
4. Installs and starts the `systemd --user` background daemon (`syncd.service`).
5. Prompts to immediately launch the first-time setup wizard (`syncctl setup`).

---

### Option B: Automated Project Installer (`install.sh`)

If you have cloned or unzipped the repository:

```bash
./install.sh
```

- **Instant install**: If pre-compiled binaries exist in `bin/` or `target/release/`, it installs them immediately without compiling.
- **Source compilation**: If binaries are not present, or if `--build` is specified, it compiles with `cargo build --release`.
- **Options**:
  - `./install.sh --yes` (non-interactive)
  - `./install.sh --build` (force rebuild from source)
  - `./install.sh --prefix /custom/bin`
  - `./install.sh --uninstall`

---

### Option C: Manual Installation from Source

#### 1. Compile Release Binaries
```bash
cargo build --workspace --release
```

#### 2. Install Binaries to PATH
```bash
mkdir -p ~/.local/bin
install -m 755 target/release/syncd ~/.local/bin/syncd
install -m 755 target/release/syncctl ~/.local/bin/syncctl
```
> [!TIP]
> Ensure `~/.local/bin` is in your PATH. If not, add `export PATH="$HOME/.local/bin:$PATH"` to your `~/.bashrc` or `~/.zshrc`.

#### 3. Set Up Configuration
```bash
mkdir -p ~/.config/syncd
cp config.example.toml ~/.config/syncd/config.toml
nano ~/.config/syncd/config.toml
```

#### 4. Enable and Start systemd User Service
```bash
mkdir -p ~/.config/systemd/user
cp systemd/syncd.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now syncd
```

Verify service status:
```bash
systemctl --user status syncd
journalctl --user -u syncd -f
```

---

## Storage Backend Configuration Guide

`syncd` supports cloud object stores via the `object_store` multi-backend as well as standard POSIX / FUSE filesystems (`file://`).

### 1. Google Drive (via rclone)

Because Google Drive uses document IDs rather than a standard blob hierarchy, the recommended Linux pattern is mounting Google Drive using `rclone`:

1. **Configure rclone**:
   ```bash
   rclone config
   # Name: gdrive
   # Storage Type: drive (Google Drive)
   # Follow web browser prompt to authenticate.
   ```
2. **Mount Google Drive locally**:
   ```bash
   mkdir -p ~/GoogleDrive
   rclone mount gdrive: ~/GoogleDrive --vfs-cache-mode writes --daemon
   ```
3. **Configure folder in `~/.config/syncd/config.toml`**:
   ```toml
   [[folders]]
   id = "a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d"
   name = "documents"
   local_path = "/home/anushtup/Documents"
   remote_target = "file:///home/anushtup/GoogleDrive/Documents"
   mode = "continuous_backup"
   empty_folder_protection = true
   ```
   > [!NOTE]
   > When deleted locally in `ContinuousBackup` mode, `syncd` safely streams the file into `.Trash-syncd/` on your Google Drive before removing the active file.

### 2. Amazon S3, MinIO, Cloudflare R2, or Wasabi

Set your AWS credentials in your environment or credentials file:
```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
```

Configure `~/.config/syncd/config.toml`:
```toml
[[folders]]
id = "b2c3d4e5-f6a7-8b9c-0d1e-2f3a4b5c6d7e"
name = "s3-backups"
local_path = "/home/anushtup/Projects"
remote_target = "s3://my-backup-bucket/workstation/projects"
mode = "continuous_backup"
empty_folder_protection = true

# Optional: MinIO / Cloudflare R2 custom endpoint
# endpoint = "https://<account-id>.r2.cloudflarestorage.com"
# region = "auto"
# allow_http = false
```

### 3. Google Cloud Storage (GCS)

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/home/anushtup/.config/gcloud/service-account.json"
```
```toml
[[folders]]
id = "c3d4e5f6-a7b8-9c0d-1e2f-3a4b5c6d7e8f"
name = "gcs-archive"
local_path = "/home/anushtup/Renders"
remote_target = "gs://my-company-archive/renders"
mode = "drop_and_purge"
empty_folder_protection = false
```

### 4. Native Google Drive (Pure Rust, Zero rclone dependency)

Direct native synchronization with personal or workspace Google Drive via Google's Drive v3 REST API. Fully in-process and written in 100% pure Rust (`reqwest` + `tokio`): **no `rclone` binary or FUSE mounts required**:

* **Automatic OAuth2 Token Refresh**: Refreshes Google tokens automatically in-process.
* **Resumable Upload Streaming**: Streams chunks directly from disk with bandwidth limiting and live byte counters.
* **Credential Auto-Discovery**: Automatically reads credentials from `~/.config/syncd/gdrive_credentials.json`, environment variables, or existing `~/.config/rclone/rclone.conf`.

```toml
[[folders]]
id = "a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d"
name = "documents"
local_path = "/home/anushtup/Documents"
remote_target = "gdrive://Documents"
mode = "continuous_backup"
empty_folder_protection = true

[[folders]]
id = "f6e5d4c3-b2a1-0f9e-8d7c-6b5a4f3e2d1c"
name = "renders-dropzone"
local_path = "/home/anushtup/Renders"
remote_target = "gdrive://renders"
mode = "drop_and_purge"
empty_folder_protection = false
```

### 5. Optional Rclone Remotes

For providers other than Google Drive (e.g. OneDrive, Dropbox, Box, pCloud, WebDAV), `syncd` can also sync directly via `rclone://<remote>/<folder>` without running `rclone mount`:

```toml
[[folders]]
name = "onedrive-backup"
local_path = "/home/anushtup/Documents"
remote_target = "rclone://onedrive/Documents"
mode = "continuous_backup"
```

---

## Sync Modes vs. Safety Lock Explained

| Feature | `Mode: Backup [Safe]` (`continuous_backup`) | `Mode: Offload` (`drop_and_purge`) |
| :--- | :--- | :--- |
| **Purpose** | Continuous 1-to-1 mirror copy. | Free local disk space (renders, raw archives). |
| **Local File Lifecycle** | Local file is **kept** on disk permanently. | Local file is **unlinked** once verified in cloud. |
| **Local Deletions** | Archives remote copy to `.Trash-syncd/`. | Preserves cloud archive (does not delete cloud file). |
| **Safety Integration** | **Armed**: Empty Folder Safety Lock prevents mass deletion if drive unmounts. | **Purge Mode**: Disabled (emptying local folder is normal offloading). |

---

## Live Configuration Reloading

To apply changes made to `~/.config/syncd/config.toml` (adding/removing folders or changing bandwidth limits):

```bash
# Restart the user daemon:
systemctl --user restart syncd

# Or send a SIGHUP reload signal directly:
kill -HUP $(pgrep syncd)
```

The supervisor diffs your active folder configuration live, adds newly defined watchers, gracefully tears down removed watchers, and updates the SQLite catalog without losing in-flight transfers.

---

## CLI & UI Control Reference (`syncctl`)

### 1. Interactive First-Time Setup Wizard (`syncctl setup` or `syncctl init`)
Guides first-time users step-by-step through setting up `syncd` from a blank slate:
```bash
# Run first-time setup wizard:
syncctl setup
```
**Wizard Steps:**
1. **Cloud Provider Selection & Auth**: Select Google Drive, AWS S3 / Cloudflare R2, Google Cloud Storage, or Local/NAS disk. For Google Drive, initiates instant browser OAuth2 login or manual code authorization.
2. **First Watched Folder**: Configures folder name, local path (with automatic `~/` expansion, directory auto-creation, and strict duplicate/nested folder collision checks), and remote cloud destination URI.
3. **Sync Mode Selection**: Choose between `ContinuousBackup` (`Backup [Safe]`, keeps files locally, mirrors edits, arms Empty Folder Safety Lock, and archives deletions to `.Trash-syncd/`) or `DropAndPurge` (`Offload`, 5-stage TOCTOU offload for renders/archives).
4. **Safety & Validation**: Runs strict configuration validation ensuring no duplicate paths, name clashes, or remote target overlaps.
5. **Daemon Activation**: Connects to the running daemon to reload configuration live, or prompts to enable and start the `systemd --user` background service.

---

### 2. Cloud Storage Authentication (`syncctl auth` or `syncctl login`)
Inspect and manage authentication credentials for all supported cloud providers:

#### Authenticate with Google Drive (OAuth2)
```bash
# Default: Spawns automated local browser loopback server on port 53682
syncctl auth login

# Headless / SSH server: manual authorization code paste
syncctl auth login --manual

# Custom Google Cloud OAuth client credentials
syncctl auth login --client-id "<YOUR_CLIENT_ID>" --client-secret "<YOUR_CLIENT_SECRET>"
```

#### Check Provider Authentication Status
```bash
syncctl auth status
# or: syncctl auth
```
Displays a real-time status table validating Google Drive OAuth2 token refresh health, AWS/R2 environment credentials, and GCS Service Account configurations.

#### Import from rclone
If you previously configured Google Drive in `rclone`, import it directly without re-authenticating:
```bash
syncctl auth import-rclone          # Imports [gdrive] from ~/.config/rclone/rclone.conf
syncctl auth import-rclone myremote # Imports custom remote
```

#### Log Out / Revoke Credentials
```bash
syncctl auth logout
```

---

### 3. Interactive Terminal UI (`syncctl ui` or bare `syncctl`)
Launches the full interactive, keyboard-driven terminal dashboard with live status cards, active folder selector, in-flight progress bars, and modal controls:
```bash
# Launch interactive TUI directly:
syncctl
```
**Interactive Keybindings:**
* `↑` / `↓` or `k` / `j`: Navigate watched sync folders.
* `Enter` or `i`: Inspect folder properties (UUID, mode, baseline count, safety lock status, local/remote paths).
* `p`: Open timed pause modal for the selected folder (e.g. `15m`, `1h`, or indefinite).
* `r`: Resume synchronization for the selected folder.
* `a`: Quick instructions to add a new folder to configuration.
* `d` / `x`: Delete and remove the selected folder from configuration with confirmation dialog.
* `b`: Open global bandwidth rate-limit modal dialog.
* `s`: Trigger an immediate recursive reconciliation scan on the selected folder.
* `0` / `1` / `5`: Quick-keys to set bandwidth to Unlimited, 1 MB/s, or 5 MB/s.
* `?`: Show keyboard reference help overlay.
* `q` / `Ctrl+C`: Exit cleanly and restore terminal screen buffer.

### 4. Embedded Web Dashboard (`syncctl web`)
Serves a clean, responsive, dark-mode browser dashboard on `http://127.0.0.1:8384` with zero external dependencies:
```bash
# Launch on default port (http://127.0.0.1:8384):
syncctl web

# Custom host and port:
syncctl web --bind 0.0.0.0 --port 9090
```
Open `http://127.0.0.1:8384` in any browser to inspect metrics, active transfer progress bars, and trigger actions (Pause, Resume, Bandwidth Limit, Re-Scan) with one click.

### 5. Folder Management CLI & Console (`syncctl folder`)

Manage watched folders directly from the terminal without editing configuration files manually. Includes interactive wizards, configuration validation, collision protection, and automatic daemon reloads.

#### Interactive Console Menu
Launch the interactive terminal console to manage all folders with single-key menu choices:
```bash
syncctl folder
# or: syncctl folder menu / ui
```

#### List Configured Folders
View all watched folders, sync modes, safety lock status, and daemon states:
```bash
syncctl folder list
syncctl folder list --verbose    # Extended details (UUIDs, excludes, endpoints)
```

#### Add a New Folder
Launch the step-by-step interactive wizard:
```bash
syncctl folder add
```
Or specify parameters directly via CLI flags:
```bash
# Add a continuous backup folder:
syncctl folder add \
  --name documents \
  --path ~/Documents \
  --target s3://my-backup-bucket/documents \
  --mode backup

# Add a safe drop & purge offload folder:
syncctl folder add \
  --name renders \
  --path ~/Renders \
  --target gs://archive-bucket/renders \
  --mode offload \
  --exclude "*.tmp" --exclude "*.crdownload"

# Add folder with custom S3 / MinIO / Cloudflare R2 endpoint:
syncctl folder add \
  --name r2-sync \
  --path ~/Projects \
  --target s3://projects-bucket/work \
  --endpoint https://<account_id>.r2.cloudflarestorage.com \
  --region auto
```

#### Inspect Folder Details
Display comprehensive configuration and live runtime telemetry for a folder:
```bash
syncctl folder show documents
syncctl folder show <FOLDER_UUID>
```

#### Edit an Existing Folder
Update folder settings interactively or via flags without touching the TOML file:
```bash
syncctl folder edit documents
syncctl folder edit documents --target s3://new-bucket/documents --mode offload
```

#### Remove a Folder
Safely remove a folder from configuration and stop syncing it (local & remote files remain untouched):
```bash
syncctl folder remove documents
syncctl folder remove documents --yes   # Skip confirmation prompt
```

#### Reload Daemon Configuration
Apply configuration changes to the running daemon without restarting the process:
```bash
syncctl reload
```

---

### 6. Non-Interactive CLI Commands

#### Status Overview
Inspect folder states, device status, pending queues, and active transfers:
```bash
syncctl status
```

### Live Telemetry Streaming
Live progress bars, EWMA throughput, and ETAs updated every 500ms:
```bash
syncctl watch
```

### Bandwidth Throttling
Dynamically adjust upload bandwidth without restarting the daemon:
```bash
syncctl bwlimit 5MB/s
syncctl bwlimit 500KB/s
syncctl bwlimit 0 # Unlimited
```

### Pause & Resume
Pause indefinitely or for a timed duration (e.g., during gaming or meetings):
```bash
syncctl pause --for 30m
syncctl pause --for 2h
syncctl pause <FOLDER_ID>
syncctl resume
```

### Trigger Reconciliation Scan
Force an immediate recursive scan comparing filesystem baseline with remote state:
```bash
syncctl scan
syncctl scan <FOLDER_ID>
```

---

## Testing

Run the full automated test suite covering in-memory SQLite transactions, TOCTOU stat asserts, anchor canaries, debouncer sliding-window priority, and IPC codecs:

```bash
cargo test --workspace
```

