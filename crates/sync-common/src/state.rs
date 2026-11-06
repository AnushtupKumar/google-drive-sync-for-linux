//! Folder state machine and telemetry types shared between `syncd` and `syncctl`.
//!
//! `syncd` writes these; `syncctl` deserialises and renders them.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ─────────────────────────────────────────────
// Folder state machine
// ─────────────────────────────────────────────

/// The lifecycle state of a single watched/offloaded folder.
///
/// All transitions are driven by the per-folder actor in `supervisor.rs` and
/// written to SQLite so they survive daemon restarts.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "state", content = "detail", rename_all = "snake_case")]
pub enum FolderState {
    /// The folder is being watched; inotify is active; no transfers in flight.
    Watching,

    /// Events have arrived but are still inside the debounce window.
    Debouncing,

    /// At least one transfer task is actively running.
    Syncing,

    /// All operations are suspended.
    Paused {
        /// `None` = indefinite pause; `Some(ts)` = resume at this UTC timestamp.
        until: Option<DateTime<Utc>>,
    },

    /// The folder's physical drive has disappeared (st_dev mismatch or missing
    /// mount point in `/proc/self/mountinfo`).
    DriveDisconnected,

    /// A non-recoverable condition.  Human intervention is required.
    Error {
        /// Human-readable description surfaced in `syncctl status`.
        reason: String,
    },

    /// The folder is undergoing a full reconciliation walk after reconnection.
    Reconciling,
}

impl FolderState {
    /// Returns `true` when the daemon should enqueue new transfer tasks.
    pub fn is_operational(&self) -> bool {
        matches!(self, FolderState::Watching | FolderState::Debouncing | FolderState::Syncing)
    }

    /// Returns `true` when the folder is paused (but not in error).
    pub fn is_paused(&self) -> bool {
        matches!(self, FolderState::Paused { .. })
    }

    /// Serialises the state name only (no detail), used for SQLite persistence.
    pub fn as_db_str(&self) -> &'static str {
        match self {
            FolderState::Watching => "Watching",
            FolderState::Debouncing => "Debouncing",
            FolderState::Syncing => "Syncing",
            FolderState::Paused { .. } => "Paused",
            FolderState::DriveDisconnected => "DriveDisconnected",
            FolderState::Error { .. } => "Error",
            FolderState::Reconciling => "Reconciling",
        }
    }
}

impl std::fmt::Display for FolderState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FolderState::Watching => write!(f, "Watching"),
            FolderState::Debouncing => write!(f, "Debouncing (events settling)"),
            FolderState::Syncing => write!(f, "Syncing"),
            FolderState::Paused { until: None } => write!(f, "Paused (indefinite)"),
            FolderState::Paused { until: Some(ts) } => {
                write!(f, "Paused until {}", ts.format("%Y-%m-%d %H:%M:%S UTC"))
            }
            FolderState::DriveDisconnected => write!(f, "Drive Disconnected"),
            FolderState::Error { reason } => write!(f, "Error: {reason}"),
            FolderState::Reconciling => write!(f, "Reconciling"),
        }
    }
}

// ─────────────────────────────────────────────
// Transfer progress
// ─────────────────────────────────────────────

/// Live progress report for a single in-flight transfer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferProgress {
    /// Unique task identifier from `transfer_queue.task_id`.
    pub task_id: i64,

    /// The folder this transfer belongs to.
    pub folder_id: Uuid,

    /// Relative path within the folder (e.g. `"photos/2024/img001.jpg"`).
    pub rel_path: String,

    /// `UPLOAD`, `DELETE`, or `OFFLOAD`.
    pub op_type: OpType,

    /// Total file size in bytes (for ETA calculation).
    pub total_bytes: u64,

    /// Bytes transferred so far.
    pub bytes_transferred: u64,

    /// EWMA-smoothed upload rate in bytes/second.
    pub upload_bps_ewma: f64,

    /// Estimated seconds until this transfer completes; `None` if unknown.
    pub eta_secs: Option<f64>,

    /// Number of retry attempts so far.
    pub attempt_count: u32,

    /// UTC timestamp when the transfer was enqueued.
    pub enqueued_at: DateTime<Utc>,
}

impl TransferProgress {
    /// Returns a value in `[0.0, 1.0]` representing upload completion.
    pub fn fraction(&self) -> f64 {
        if self.total_bytes == 0 {
            return 0.0;
        }
        (self.bytes_transferred as f64 / self.total_bytes as f64).clamp(0.0, 1.0)
    }

    /// Returns a human-friendly progress string like `"12.3 MiB / 100.0 MiB (12%)"`
    pub fn progress_display(&self) -> String {
        let xferred = format_bytes(self.bytes_transferred);
        let total = format_bytes(self.total_bytes);
        let pct = (self.fraction() * 100.0) as u64;
        format!("{xferred} / {total} ({pct}%)")
    }
}

/// The kind of cloud operation being tracked.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum OpType {
    Upload,
    Delete,
    Offload,
}

impl std::fmt::Display for OpType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpType::Upload => write!(f, "UPLOAD"),
            OpType::Delete => write!(f, "DELETE"),
            OpType::Offload => write!(f, "OFFLOAD"),
        }
    }
}

impl OpType {
    pub fn as_db_str(self) -> &'static str {
        match self {
            OpType::Upload => "UPLOAD",
            OpType::Delete => "DELETE",
            OpType::Offload => "OFFLOAD",
        }
    }

    pub fn from_db_str(s: &str) -> Option<Self> {
        match s {
            "UPLOAD" => Some(OpType::Upload),
            "DELETE" => Some(OpType::Delete),
            "OFFLOAD" => Some(OpType::Offload),
            _ => None,
        }
    }
}

// ─────────────────────────────────────────────
// Telemetry snapshot (IPC response payload)
// ─────────────────────────────────────────────

/// Per-folder status rolled up for the `GetStatus` / `SubscribeTelemetry` response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FolderStatusReport {
    pub folder_id: Uuid,
    pub name: String,
    pub local_path: String,
    pub remote_target: String,
    pub state: FolderState,

    /// Synchronization mode ("continuous_backup" or "drop_and_purge").
    #[serde(default)]
    pub sync_mode: String,

    /// Total files in the synced_files baseline.
    pub baseline_file_count: u64,

    /// Pending items in transfer_queue for this folder.
    pub pending_tasks: u64,

    /// Cumulative bytes uploaded since daemon start (resets on restart).
    pub session_bytes_uploaded: u64,

    /// Cumulative bytes freed by DropAndPurge since daemon start.
    pub session_bytes_freed: u64,

    /// Whether the empty-folder safety lock is currently armed.
    pub safety_lock_armed: bool,
}

/// Aggregate telemetry snapshot sent to `syncctl` every 500 ms during `watch`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetrySnapshot {
    /// Current moment (UTC) so the client can compute staleness.
    pub sampled_at: DateTime<Utc>,

    /// Aggregate upload rate across all folders (bytes/sec, EWMA).
    pub global_upload_bps: u64,

    /// The configured bandwidth cap (`0` = unlimited).
    pub configured_bw_limit_bps: u64,

    /// One entry per configured folder.
    pub folders: Vec<FolderStatusReport>,

    /// In-flight transfers across all folders.
    pub active_transfers: Vec<TransferProgress>,
}

// ─────────────────────────────────────────────
// Utility
// ─────────────────────────────────────────────

/// Format a byte count as a human-readable string (KiB / MiB / GiB).
pub fn format_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    const TIB: f64 = GIB * 1024.0;

    let b = bytes as f64;
    if b >= TIB {
        format!("{:.1} TiB", b / TIB)
    } else if b >= GIB {
        format!("{:.1} GiB", b / GIB)
    } else if b >= MIB {
        format!("{:.1} MiB", b / MIB)
    } else if b >= KIB {
        format!("{:.1} KiB", b / KIB)
    } else {
        format!("{bytes} B")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn folder_state_display() {
        use chrono::TimeZone;
        let ts = Utc.with_ymd_and_hms(2025, 1, 1, 12, 0, 0).unwrap();
        let s = FolderState::Paused { until: Some(ts) };
        assert!(s.to_string().contains("2025-01-01"));
    }

    #[test]
    fn format_bytes_ranges() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024), "1.0 KiB");
        assert_eq!(format_bytes(1_048_576), "1.0 MiB");
        assert_eq!(format_bytes(1_073_741_824), "1.0 GiB");
    }

    #[test]
    fn op_type_roundtrip() {
        for op in [OpType::Upload, OpType::Delete, OpType::Offload] {
            assert_eq!(OpType::from_db_str(op.as_db_str()), Some(op));
        }
    }
}
