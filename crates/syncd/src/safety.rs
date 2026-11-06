//! Drive hotplug detection, `.syncd-anchor` canary guard, and
//! empty-folder safety lock.
//!
//! ## Responsibilities
//!
//! | Concern | Implementation |
//! |---|---|
//! | **Anchor canary** | Write/read/verify `.syncd-anchor` containing folder UUID |
//! | **st_dev verification** | Assert `st_dev` hasn't changed (drive swap detection) |
//! | **Mount table monitoring** | `poll(2)` with `POLLPRI\|POLLERR` on `/proc/self/mountinfo` |
//! | **Empty-folder safety lock** | Count visible files; halt if 0 while baseline > 0 |
//! | **TOCTOU re-stat** | Capture pre-upload fingerprint; assert unchanged before unlink |
//!
//! ## Mount monitoring
//!
//! `/proc/self/mountinfo` generates `POLLPRI` and `POLLERR` events when the
//! kernel's mount namespace changes (drive plugged in or out, bind mounts, etc.).
//! We call `poll(2)` with a 2 s timeout inside `tokio::task::spawn_blocking`
//! so the tokio executor threads are never blocked.  When triggered, a
//! `tokio::sync::watch` counter is incremented; the supervisor watches that
//! counter and re-checks `st_dev` for all active folders.

use std::{io, path::{Path, PathBuf}, time::Duration};
use std::os::unix::fs::MetadataExt;
use std::os::unix::io::AsRawFd;
use thiserror::Error;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;
use walkdir::WalkDir;

// ─────────────────────────────────────────────
// Error type
// ─────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum SafetyError {
    #[error("anchor file missing at {path}")]
    AnchorMissing { path: PathBuf },

    #[error("anchor UUID mismatch at {path}: expected {expected}, got '{actual}'")]
    AnchorMismatch {
        path: PathBuf,
        expected: Uuid,
        actual: String,
    },

    #[error("anchor path is not a regular file: {path}")]
    AnchorNotFile { path: PathBuf },

    #[error("st_dev mismatch at {path}: expected {expected}, got {actual} \
             (drive may have been swapped)")]
    StDevMismatch {
        path: PathBuf,
        expected: u64,
        actual: u64,
    },

    #[error("empty-folder safety lock: {path} has 0 visible files \
             but baseline is {baseline}")]
    EmptyFolderLock { path: PathBuf, baseline: u64 },

    #[error("file changed during upload (TOCTOU re-stat): {path}")]
    ToctouMismatch { path: PathBuf },

    #[error("I/O error at {path}: {source}")]
    Io { path: PathBuf, source: io::Error },
}

impl SafetyError {
    fn io(path: impl Into<PathBuf>, source: io::Error) -> Self {
        Self::Io { path: path.into(), source }
    }
}

// ─────────────────────────────────────────────
// Anchor canary
// ─────────────────────────────────────────────

/// Returns the canonical path of the anchor file for `local_path`.
pub fn anchor_path(local_path: &Path) -> PathBuf {
    local_path.join(".syncd-anchor")
}

/// Create the anchor file on first run **or** validate it on subsequent runs.
///
/// First run (`anchor does not exist`):
/// - Writes `folder_id\n` to `.syncd-anchor` atomically via a temp file +
///   rename, which is TOCTOU-safe.
///
/// Subsequent runs:
/// - Reads the file and verifies the UUID matches.
///
/// Returns `Ok(())` if the anchor is valid (created or verified).
pub async fn ensure_anchor(local_path: &Path, folder_id: Uuid) -> Result<(), SafetyError> {
    let path = anchor_path(local_path);

    match tokio::fs::read_to_string(&path).await {
        Ok(contents) => validate_anchor_contents(&path, folder_id, &contents),

        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            // First run — write atomically via tmp file + rename
            write_anchor_atomically(local_path, folder_id).await
        }

        Err(e) => Err(SafetyError::io(&path, e)),
    }
}

/// Verify the anchor file without creating it.  Used before every DELETE task.
pub async fn verify_anchor(local_path: &Path, folder_id: Uuid) -> Result<(), SafetyError> {
    let path = anchor_path(local_path);

    // Reject symlinks (O_NOFOLLOW equivalent via symlink_metadata)
    let meta = tokio::fs::symlink_metadata(&path).await.map_err(|e| {
        if e.kind() == io::ErrorKind::NotFound {
            SafetyError::AnchorMissing { path: path.clone() }
        } else {
            SafetyError::io(&path, e)
        }
    })?;

    if !meta.is_file() {
        return Err(SafetyError::AnchorNotFile { path });
    }

    let contents = tokio::fs::read_to_string(&path)
        .await
        .map_err(|e| SafetyError::io(&path, e))?;

    validate_anchor_contents(&path, folder_id, &contents)
}

fn validate_anchor_contents(
    path: &Path,
    expected: Uuid,
    contents: &str,
) -> Result<(), SafetyError> {
    let stored = contents.trim();
    match Uuid::parse_str(stored) {
        Ok(u) if u == expected => {
            debug!(folder_id = %expected, "anchor file validated OK");
            Ok(())
        }
        Ok(u) => Err(SafetyError::AnchorMismatch {
            path: path.to_owned(),
            expected,
            actual: u.to_string(),
        }),
        Err(_) => Err(SafetyError::AnchorMismatch {
            path: path.to_owned(),
            expected,
            actual: stored.to_owned(),
        }),
    }
}

/// Write the anchor atomically: write to `.syncd-tmp-anchor`, then rename.
async fn write_anchor_atomically(
    local_path: &Path,
    folder_id: Uuid,
) -> Result<(), SafetyError> {
    let anchor = anchor_path(local_path);
    let tmp = local_path.join(format!(".syncd-tmp-anchor-{}", uuid::Uuid::new_v4()));

    let content = format!("{}\n", folder_id);
    tokio::fs::write(&tmp, &content)
        .await
        .map_err(|e| SafetyError::io(&tmp, e))?;

    tokio::fs::rename(&tmp, &anchor)
        .await
        .map_err(|e| SafetyError::io(&anchor, e))?;

    info!(folder_id = %folder_id, path = ?anchor, "anchor file created");
    Ok(())
}

// ─────────────────────────────────────────────
// st_dev verification
// ─────────────────────────────────────────────

/// Read the `st_dev` for `path` using `symlink_metadata` (no symlink follow).
pub async fn read_st_dev(path: &Path) -> Result<u64, SafetyError> {
    let meta = tokio::fs::symlink_metadata(path)
        .await
        .map_err(|e| SafetyError::io(path, e))?;
    Ok(meta.dev())
}

/// Assert that `path`'s current `st_dev` equals `expected_dev`.
///
/// Returns `Err(SafetyError::StDevMismatch)` when they differ, which
/// indicates the underlying physical device has changed (drive unmounted
/// and a different filesystem is now visible).
pub async fn assert_st_dev(path: &Path, expected_dev: u64) -> Result<(), SafetyError> {
    let actual = read_st_dev(path).await?;
    if actual != expected_dev {
        return Err(SafetyError::StDevMismatch {
            path: path.to_owned(),
            expected: expected_dev,
            actual,
        });
    }
    Ok(())
}

// ─────────────────────────────────────────────
// Empty-folder safety lock
// ─────────────────────────────────────────────

/// Count user-visible files under `local_path` (recursive).
///
/// Files matching `.syncd-anchor` or `.syncd-tmp-*` are excluded from the
/// count so they don't inflate the baseline.
pub async fn count_visible_files(local_path: &Path) -> Result<u64, SafetyError> {
    let base = local_path.to_owned();

    tokio::task::spawn_blocking(move || {
        let mut count = 0u64;
        for entry in WalkDir::new(&base).follow_links(false).into_iter().flatten() {
            if !entry.file_type().is_file() {
                continue;
            }
            let name = entry.file_name().to_string_lossy();
            if name == ".syncd-anchor" || name.starts_with(".syncd-tmp-") {
                continue;
            }
            count += 1;
        }
        Ok::<u64, SafetyError>(count)
    })
    .await
    .map_err(|e| SafetyError::io(local_path, io::Error::other(e.to_string())))?
}

/// Run the empty-folder safety lock check.
///
/// Returns `Err(SafetyError::EmptyFolderLock)` if the folder appears empty
/// while the recorded baseline is non-zero, indicating a potential data-
/// loss scenario (accidental wipe, wrong mount point, etc.).
///
/// `ContinuousBackup` folders only — `DropAndPurge` intentionally empties
/// the directory, so the caller is responsible for skipping this check.
pub async fn check_empty_folder_lock(
    local_path: &Path,
    folder_id: Uuid,
    baseline_file_count: u64,
) -> Result<(), SafetyError> {
    if baseline_file_count == 0 {
        // Brand-new folder: no baseline to compare against.
        return Ok(());
    }

    // Verify anchor first — if it's missing the drive may not be mounted.
    verify_anchor(local_path, folder_id).await?;

    let visible = count_visible_files(local_path).await?;
    if visible == 0 {
        error!(
            folder_id = %folder_id,
            path = ?local_path,
            baseline = baseline_file_count,
            "EMPTY FOLDER SAFETY LOCK TRIGGERED — halting all operations"
        );
        // Fire critical desktop alert (fire-and-forget, non-blocking)
        let path_str = local_path.display().to_string();
        tokio::task::spawn_blocking(move || {
            let _ = notify_rust::Notification::new()
                .summary("syncd: CRITICAL — Empty Folder Safety Lock")
                .body(&format!(
                    "Folder '{}' appears empty (0 files) but baseline \
                     recorded {} files.\nAll operations halted.\n\
                     Please verify your storage device is mounted.",
                    path_str, baseline_file_count
                ))
                .urgency(notify_rust::Urgency::Critical)
                .timeout(notify_rust::Timeout::Never)
                .show();
        });

        return Err(SafetyError::EmptyFolderLock {
            path: local_path.to_owned(),
            baseline: baseline_file_count,
        });
    }

    debug!(
        folder_id = %folder_id,
        visible,
        baseline = baseline_file_count,
        "empty-folder lock check passed"
    );
    Ok(())
}

// ─────────────────────────────────────────────
// /proc/self/mountinfo watcher
// ─────────────────────────────────────────────

/// Spawn a background task that monitors `/proc/self/mountinfo` for changes
/// using `poll(2)` with `POLLPRI | POLLERR`.
///
/// Returns a `watch::Receiver<u64>` whose value increments each time the
/// mount table changes.  The supervisor awaits `receiver.changed()` and
/// re-checks `st_dev` for all active folders.
///
/// The watcher exits cleanly when `cancel` fires (the 2 s `poll` timeout
/// provides the exit window).
pub fn spawn_mountinfo_watcher(cancel: CancellationToken) -> watch::Receiver<u64> {
    let (tx, rx) = watch::channel(0u64);

    tokio::spawn(async move {
        let mut counter: u64 = 0;

        loop {
            if cancel.is_cancelled() {
                break;
            }

            let changed = tokio::task::spawn_blocking(|| poll_mountinfo_blocking(2_000))
                .await;

            match changed {
                Ok(Ok(true)) => {
                    counter = counter.wrapping_add(1);
                    debug!(counter, "mount table changed");
                    // Ignore send error — receiver may have been dropped
                    let _ = tx.send(counter);
                }
                Ok(Ok(false)) => {
                    // poll(2) timed out — loop to re-check cancel token
                }
                Ok(Err(e)) => {
                    warn!("poll on /proc/self/mountinfo failed: {}", e);
                    // Back off to avoid a busy spin on persistent errors
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                Err(join_err) => {
                    error!("spawn_blocking panicked in mountinfo watcher: {}", join_err);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }

        info!("mountinfo watcher task exited");
    });

    rx
}

/// Open `/proc/self/mountinfo` and block on `poll(2)` with `POLLPRI | POLLERR`
/// for up to `timeout_ms` milliseconds.
///
/// Returns:
/// - `Ok(true)` — mount table changed.
/// - `Ok(false)` — timeout expired with no event.
/// - `Err(_)` — syscall error.
///
/// # Blocking
///
/// This function is **synchronous** and must be called from
/// `tokio::task::spawn_blocking`.
fn poll_mountinfo_blocking(timeout_ms: i32) -> io::Result<bool> {
    let file = std::fs::File::open("/proc/self/mountinfo")?;
    let fd = file.as_raw_fd();

    // SAFETY:
    // - `fd` is valid for the lifetime of `file` which is alive for the
    //   duration of this call.
    // - `poll(2)` is always safe to call with a valid fd and a correctly
    //   sized `pollfd` slice.
    // - We only read `revents` after the call returns.
    let result = unsafe {
        let mut pfd = libc::pollfd {
            fd,
            events: libc::POLLPRI | libc::POLLERR,
            revents: 0,
        };
        libc::poll(&mut pfd as *mut libc::pollfd, 1, timeout_ms)
    };

    match result {
        0 => Ok(false),                     // timeout
        1 => Ok(true),                      // event fired
        _ => Err(io::Error::last_os_error()), // error
    }
}

// ─────────────────────────────────────────────
// TOCTOU pre-upload fingerprint
// ─────────────────────────────────────────────

/// File identity snapshot captured in Stage 1 of the offload pipeline.
///
/// Used in Stage 4 (`assert_unchanged`) to detect whether a file was
/// modified or replaced during the cloud upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreUploadStat {
    /// inode number
    pub ino: u64,
    /// File size in bytes
    pub size: u64,
    /// Modification time seconds (UNIX epoch)
    pub mtime_sec: i64,
    /// Modification time sub-second nanoseconds
    pub mtime_nsec: i64,
}

impl PreUploadStat {
    /// Capture a fingerprint for `path` using `symlink_metadata`
    /// (does **not** follow symlinks — equivalent to `O_NOFOLLOW`).
    pub async fn capture(path: &Path) -> Result<Self, SafetyError> {
        let meta = tokio::fs::symlink_metadata(path)
            .await
            .map_err(|e| SafetyError::io(path, e))?;

        Ok(Self {
            ino: meta.ino(),
            size: meta.len(),
            mtime_sec: meta.mtime(),
            mtime_nsec: meta.mtime_nsec(),
        })
    }

    /// Stage 4 — TOCTOU re-stat.
    ///
    /// Re-reads the file metadata and compares it against the pre-upload
    /// snapshot.  If any field differs the file was modified during upload;
    /// the caller must **not** proceed with `unlink(2)`.
    ///
    /// Returns `Err(SafetyError::ToctouMismatch)` on mismatch.
    pub async fn assert_unchanged(&self, path: &Path) -> Result<(), SafetyError> {
        let post = Self::capture(path).await?;

        if post.ino != self.ino
            || post.size != self.size
            || post.mtime_sec != self.mtime_sec
            || post.mtime_nsec != self.mtime_nsec
        {
            warn!(
                path = ?path,
                pre_ino = self.ino,
                pre_size = self.size,
                pre_mtime_sec = self.mtime_sec,
                pre_mtime_nsec = self.mtime_nsec,
                post_ino = post.ino,
                post_size = post.size,
                post_mtime_sec = post.mtime_sec,
                post_mtime_nsec = post.mtime_nsec,
                "TOCTOU: file was modified during upload — aborting unlink"
            );
            return Err(SafetyError::ToctouMismatch { path: path.to_owned() });
        }

        debug!(path = ?path, "TOCTOU re-stat OK — file unchanged");
        Ok(())
    }
}

// ─────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn anchor_create_and_verify() {
        let dir = tempdir().unwrap();
        let id = Uuid::new_v4();

        // First call: creates the anchor
        ensure_anchor(dir.path(), id).await.unwrap();
        // Second call: verifies it
        ensure_anchor(dir.path(), id).await.unwrap();
        // Direct verify: also passes
        verify_anchor(dir.path(), id).await.unwrap();
    }

    #[tokio::test]
    async fn anchor_uuid_mismatch_detected() {
        let dir = tempdir().unwrap();
        let id_a = Uuid::new_v4();
        let id_b = Uuid::new_v4();

        // Write anchor for id_a
        ensure_anchor(dir.path(), id_a).await.unwrap();
        // Verifying with id_b should fail
        let err = verify_anchor(dir.path(), id_b).await.unwrap_err();
        assert!(matches!(err, SafetyError::AnchorMismatch { .. }));
    }

    #[tokio::test]
    async fn anchor_missing_returns_correct_error() {
        let dir = tempdir().unwrap();
        let id = Uuid::new_v4();
        let err = verify_anchor(dir.path(), id).await.unwrap_err();
        assert!(matches!(err, SafetyError::AnchorMissing { .. }));
    }

    #[tokio::test]
    async fn count_visible_files_excludes_anchor() {
        let dir = tempdir().unwrap();
        let id = Uuid::new_v4();

        // Create anchor + 3 real files
        ensure_anchor(dir.path(), id).await.unwrap();
        for i in 0..3 {
            std::fs::write(dir.path().join(format!("file{i}.txt")), b"data").unwrap();
        }

        let count = count_visible_files(dir.path()).await.unwrap();
        assert_eq!(count, 3, "anchor must not be counted as a user file");
    }

    #[tokio::test]
    async fn empty_folder_lock_passes_for_zero_baseline() {
        let dir = tempdir().unwrap();
        let id = Uuid::new_v4();
        // baseline = 0 → lock must not trip even with 0 visible files
        check_empty_folder_lock(dir.path(), id, 0).await.unwrap();
    }

    #[tokio::test]
    async fn toctou_stat_detects_modification() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data.bin");
        std::fs::write(&path, b"original").unwrap();

        let pre = PreUploadStat::capture(&path).await.unwrap();
        // Simulate modification during upload
        std::fs::write(&path, b"modified content that is longer").unwrap();

        let err = pre.assert_unchanged(&path).await.unwrap_err();
        assert!(matches!(err, SafetyError::ToctouMismatch { .. }));
    }

    #[tokio::test]
    async fn toctou_stat_passes_for_unchanged_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data.bin");
        std::fs::write(&path, b"stable content").unwrap();

        let pre = PreUploadStat::capture(&path).await.unwrap();
        pre.assert_unchanged(&path).await.unwrap();
    }
}
