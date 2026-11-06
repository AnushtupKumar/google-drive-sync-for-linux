//! 5-stage TOCTOU-safe Drop & Purge offload pipeline and 3-stage continuous
//! backup pipeline.
//!
//! ## DropAndPurge — 5 stages
//!
//! ```text
//! Stage 1 │ Pre-upload stat     symlink_metadata → (ino, size, mtime)
//! Stage 2 │ Upload              stream to cloud with SHA-256 in-flight
//! Stage 3 │ Remote verify       HEAD remote object; assert size matches
//! Stage 4 │ TOCTOU re-stat      re-stat local; assert ino/size/mtime unchanged
//! Stage 5 │ Unlink + prune      unlink(2), then bottom-up rmdir(2) empty dirs
//! ```
//!
//! Stages 1–4 are all non-destructive.  The file is only removed in Stage 5
//! after the remote object has been independently verified (Stage 3) and the
//! local file confirmed unmodified (Stage 4).
//!
//! ## ContinuousBackup — 3 stages
//!
//! ```text
//! Stage 1 │ Upload + SHA-256    stream to cloud, no rate-limit token overhead
//! Stage 2 │ Remote verify       HEAD remote object; assert size matches
//! Stage 3 │ Update database     upsert synced_files, delete transfer_queue row
//! ```
//!
//! For `Delete` operations in `ContinuousBackup` mode, the remote object is
//! **archived** to a trash prefix (`.Trash-syncd/`) rather than hard-deleted,
//! providing a recovery window.
//!
//! ## Empty-directory pruning
//!
//! After a successful `unlink(2)`, `prune_empty_parents` walks **up** the
//! directory tree using `rmdir(2)`, which atomically fails with `ENOTEMPTY`
//! on non-empty directories.  Walking stops at the watched folder root or when
//! a non-empty directory is encountered.

use crate::{
    db::{self, DbError},
    safety::{self, PreUploadStat, SafetyError},
    storage::{BandwidthLimiter, CloudStorage, StorageError},
};
use sqlx::SqlitePool;
use std::{
    io,
    path::{Path, PathBuf},
    sync::{atomic::AtomicU64, Arc},
};
use sync_common::{
    config::{FolderConfig, SyncMode},
    state::OpType,
};
use thiserror::Error;
use tracing::{debug, info, warn};
use uuid::Uuid;

// ─────────────────────────────────────────────
// Error type
// ─────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum OffloadError {
    #[error("stage 1 stat failed: {0}")]
    PreStat(#[source] SafetyError),

    #[error("stage 2 upload failed: {0}")]
    Upload(#[source] StorageError),

    #[error("stage 3 remote verification failed: {0}")]
    RemoteVerify(#[source] StorageError),

    #[error("stage 3 size mismatch: uploaded {uploaded} bytes but remote reports {remote}")]
    RemoteSizeMismatch { uploaded: u64, remote: u64 },

    #[error("stage 4 TOCTOU re-stat detected modification: {0}")]
    ToctouModified(#[source] SafetyError),

    #[error("stage 5 unlink failed: {0}")]
    Unlink(#[source] io::Error),

    #[error("database error: {0}")]
    Db(#[from] DbError),

    #[error("anchor verification failed before delete: {0}")]
    AnchorVerify(#[source] SafetyError),
}

// ─────────────────────────────────────────────
// Task context (shared inputs)
// ─────────────────────────────────────────────

/// All context needed to execute one transfer task.
pub struct TaskContext {
    pub task_id: i64,
    pub folder_id: Uuid,
    pub folder_cfg: Arc<FolderConfig>,
    pub rel_path: String,
    pub op: OpType,
    pub pool: SqlitePool,
    pub storage: Arc<dyn CloudStorage>,
    pub limiter: Option<Arc<BandwidthLimiter>>,
    pub progress: Option<Arc<AtomicU64>>,
}

impl TaskContext {
    /// Absolute path of the local file.
    pub fn local_abs(&self) -> PathBuf {
        self.folder_cfg.local_path.join(&self.rel_path)
    }
}

// ─────────────────────────────────────────────
// ContinuousBackup upload pipeline
// ─────────────────────────────────────────────

/// Run a 3-stage continuous-backup upload for one task.
///
/// On success, updates `synced_files` and removes the task from
/// `transfer_queue` in a single transaction.
pub async fn run_backup_upload(ctx: &TaskContext) -> Result<(), OffloadError> {
    let abs_path = ctx.local_abs();
    let remote_key = ctx.storage.remote_key_for(&ctx.rel_path);

    // Check if the local file exists. If it was deleted/moved before upload:
    if !abs_path.exists() {
        let is_synced = match db::get_synced_file(&ctx.pool, ctx.folder_id, &ctx.rel_path).await {
            Ok(Some(_)) => true,
            _ => false,
        };

        if is_synced {
            info!(
                folder_id = %ctx.folder_id,
                rel_path = %ctx.rel_path,
                "Local file was deleted after sync; delegating to backup delete"
            );
            return run_backup_delete(ctx).await;
        } else {
            info!(
                folder_id = %ctx.folder_id,
                rel_path = %ctx.rel_path,
                "Local file was deleted/moved before initial upload; dropping transient task"
            );
            let _ = db::delete_task(&ctx.pool, ctx.task_id).await;
            return Ok(());
        }
    }

    info!(
        folder_id = %ctx.folder_id,
        rel_path = %ctx.rel_path,
        remote_key,
        "backup upload: stage 2 — uploading"
    );

    // ── Stage 2: upload ──────────────────────────────────────────────────────
    let upload_info = ctx
        .storage
        .put_file(&abs_path, &remote_key, ctx.limiter.as_deref(), ctx.progress.clone())
        .await
        .map_err(OffloadError::Upload)?;

    // ── Stage 3: remote verify ───────────────────────────────────────────────
    verify_remote_size(&ctx.storage, &remote_key, upload_info.size_bytes).await?;

    // ── Stage DB: update synced_files + complete task ────────────────────────
    let meta = tokio::fs::symlink_metadata(&abs_path)
        .await
        .map_err(|e| OffloadError::Db(DbError::Sqlx(sqlx::Error::Io(e))))?;

    use std::os::unix::fs::MetadataExt;
    db::complete_task(
        &ctx.pool,
        ctx.task_id,
        ctx.folder_id,
        &ctx.rel_path,
        meta.ino(),
        meta.len(),
        meta.mtime_nsec(),
        &upload_info.sha256_hex,
    )
    .await?;

    info!(
        folder_id = %ctx.folder_id,
        rel_path = %ctx.rel_path,
        size = upload_info.size_bytes,
        sha256 = %upload_info.sha256_hex,
        "backup upload: complete"
    );
    Ok(())
}

/// Run a `Delete` operation in `ContinuousBackup` mode.
///
/// Archives the remote object to `.Trash-syncd/<rel_path>` (server-side copy
/// then delete), then removes from `synced_files`.
pub async fn run_backup_delete(ctx: &TaskContext) -> Result<(), OffloadError> {
    let abs_path = ctx.local_abs();

    // Safety check: if the local file exists on disk, it was re-created or atomically replaced!
    // Do NOT delete the remote file. Instead, re-queue an upload and remove this delete task.
    if abs_path.exists() {
        info!(
            folder_id = %ctx.folder_id,
            rel_path = %ctx.rel_path,
            "Local file exists on disk (re-created during queue interval); aborting delete and re-enqueuing upload"
        );
        let _ = db::delete_task(&ctx.pool, ctx.task_id).await;
        let _ = db::enqueue_task(&ctx.pool, ctx.folder_id, &ctx.rel_path, OpType::Upload).await;
        return Ok(());
    }

    let remote_key = ctx.storage.remote_key_for(&ctx.rel_path);
    let trash_key = ctx.storage.trash_key_for(&ctx.rel_path);

    info!(
        folder_id = %ctx.folder_id,
        rel_path = %ctx.rel_path,
        remote_key,
        trash_key,
        "backup delete: archiving to trash"
    );

    // Copy to trash prefix (server-side copy or stream copy fallback).
    // MUST succeed before deleting original remote object to prevent permanent data loss!
    ctx.storage
        .copy_object(&remote_key, &trash_key)
        .await
        .map_err(OffloadError::RemoteVerify)?;

    match ctx.storage.delete_object(&remote_key).await {
        Ok(()) | Err(StorageError::ObjectStore(object_store::Error::NotFound { .. })) => {}
        Err(e) => return Err(OffloadError::RemoteVerify(e)),
    }

    db::remove_synced_file(&ctx.pool, ctx.folder_id, &ctx.rel_path).await?;
    db::delete_task(&ctx.pool, ctx.task_id).await?;

    info!(
        folder_id = %ctx.folder_id,
        rel_path = %ctx.rel_path,
        "backup delete: complete"
    );
    Ok(())
}

// ─────────────────────────────────────────────
// DropAndPurge offload pipeline
// ─────────────────────────────────────────────

/// Run the complete 5-stage TOCTOU-safe offload for one `DropAndPurge` task.
///
/// Returns `Ok(())` on success.  The caller is responsible for calling
/// [`db::fail_task`] if this returns `Err`.
pub async fn run_drop_and_purge(ctx: &TaskContext) -> Result<(), OffloadError> {
    let abs_path = ctx.local_abs();
    let remote_key = ctx.storage.remote_key_for(&ctx.rel_path);

    // If local file does not exist, it was already purged or removed locally before upload
    if !abs_path.exists() {
        info!(
            folder_id = %ctx.folder_id,
            rel_path = %ctx.rel_path,
            "Drop & Purge: local file does not exist on disk; clearing obsolete task"
        );
        let _ = db::delete_task(&ctx.pool, ctx.task_id).await;
        return Ok(());
    }

    // ── Stage 1: pre-upload stat ─────────────────────────────────────────────
    debug!(
        folder_id = %ctx.folder_id,
        rel_path = %ctx.rel_path,
        "offload stage 1: pre-stat"
    );
    let pre_stat = PreUploadStat::capture(&abs_path)
        .await
        .map_err(OffloadError::PreStat)?;

    // ── Stage 2: upload ──────────────────────────────────────────────────────
    info!(
        folder_id = %ctx.folder_id,
        rel_path = %ctx.rel_path,
        remote_key,
        "offload stage 2: uploading"
    );
    let upload_info = ctx
        .storage
        .put_file(&abs_path, &remote_key, ctx.limiter.as_deref(), ctx.progress.clone())
        .await
        .map_err(OffloadError::Upload)?;

    // ── Stage 3: remote verify ───────────────────────────────────────────────
    debug!(
        folder_id = %ctx.folder_id,
        rel_path = %ctx.rel_path,
        "offload stage 3: remote size verify"
    );
    verify_remote_size(&ctx.storage, &remote_key, upload_info.size_bytes).await?;

    // ── Stage 4: TOCTOU re-stat ──────────────────────────────────────────────
    debug!(
        folder_id = %ctx.folder_id,
        rel_path = %ctx.rel_path,
        "offload stage 4: TOCTOU re-stat"
    );
    pre_stat
        .assert_unchanged(&abs_path)
        .await
        .map_err(OffloadError::ToctouModified)?;

    // ── Stage 5a: anchor + safety checks before unlink ───────────────────────
    // Verify the anchor is still present (guards against accidental unmount)
    safety::verify_anchor(&ctx.folder_cfg.local_path, ctx.folder_id)
        .await
        .map_err(OffloadError::AnchorVerify)?;

    // ── Stage 5b: unlink ─────────────────────────────────────────────────────
    info!(
        folder_id = %ctx.folder_id,
        rel_path = %ctx.rel_path,
        "offload stage 5: unlink"
    );
    tokio::fs::remove_file(&abs_path)
        .await
        .map_err(OffloadError::Unlink)?;

    // ── Stage 5c: prune empty parent directories ─────────────────────────────
    prune_empty_parents(&abs_path, &ctx.folder_cfg.local_path).await;

    // ── Stage DB: record completion ──────────────────────────────────────────
    // Record completion using the pre-stat values (file is already deleted)
    db::complete_task(
        &ctx.pool,
        ctx.task_id,
        ctx.folder_id,
        &ctx.rel_path,
        pre_stat.ino,
        pre_stat.size,
        pre_stat.mtime_nsec,
        &upload_info.sha256_hex,
    )
    .await?;

    info!(
        folder_id = %ctx.folder_id,
        rel_path = %ctx.rel_path,
        size = upload_info.size_bytes,
        sha256 = %upload_info.sha256_hex,
        "offload: drop-and-purge complete"
    );
    Ok(())
}

// ─────────────────────────────────────────────
// Dispatcher (routes to the correct pipeline)
// ─────────────────────────────────────────────

/// Execute one transfer task, routing to the correct pipeline based on
/// `FolderConfig::mode` and `TaskContext::op`.
pub async fn execute_task(ctx: &TaskContext) -> Result<(), OffloadError> {
    match (ctx.folder_cfg.mode.clone(), ctx.op) {
        (SyncMode::DropAndPurge, OpType::Upload) => run_drop_and_purge(ctx).await,

        (SyncMode::ContinuousBackup, OpType::Upload) => run_backup_upload(ctx).await,

        (SyncMode::ContinuousBackup, OpType::Delete) => run_backup_delete(ctx).await,

        (SyncMode::DropAndPurge, OpType::Delete) => {
            // Drop & Purge: Local file unlinking is the intended mechanism to reclaim disk space.
            // Under NO circumstance should a local delete event delete the cloud offloaded archive!
            info!(
                folder_id = %ctx.folder_id,
                rel_path = %ctx.rel_path,
                "Drop & Purge: local file unlinked to reclaim space — preserving remote offloaded object"
            );
            db::delete_task(&ctx.pool, ctx.task_id).await?;
            Ok(())
        }

        (_, OpType::Offload) => {
            // OpType::Offload is a synonym for DropAndPurge upload in the DB
            run_drop_and_purge(ctx).await
        }
    }
}

// ─────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────

/// Stage 3: HEAD the remote object and assert its reported size matches
/// the number of bytes we uploaded.
async fn verify_remote_size(
    storage: &Arc<dyn CloudStorage>,
    remote_key: &str,
    uploaded_bytes: u64,
) -> Result<(), OffloadError> {
    let meta = storage
        .head_object(remote_key)
        .await
        .map_err(OffloadError::RemoteVerify)?;

    if meta.size_bytes != uploaded_bytes {
        return Err(OffloadError::RemoteSizeMismatch {
            uploaded: uploaded_bytes,
            remote: meta.size_bytes,
        });
    }

    debug!(
        remote_key,
        size = uploaded_bytes,
        etag = ?meta.etag,
        "remote size verified OK"
    );
    Ok(())
}

/// Walk **up** from `deleted_file`'s directory toward `root`, calling
/// `rmdir(2)` on each parent.  Stops when:
///
/// - `rmdir` fails with `ENOTEMPTY` (non-empty directory — correct stop).
/// - We reach `root` itself (never remove the watched folder root).
/// - Any other I/O error (logged as a warning, not fatal).
///
/// This is the "bottom-up POSIX rmdir" called in Stage 5 of Drop & Purge.
pub async fn prune_empty_parents(deleted_file: &Path, root: &Path) {
    let mut dir = match deleted_file.parent() {
        Some(p) => p.to_owned(),
        None => return,
    };

    loop {
        // Never remove the watched root itself
        if dir == root || !dir.starts_with(root) {
            break;
        }

        match tokio::fs::remove_dir(&dir).await {
            Ok(()) => {
                debug!(path = ?dir, "pruned empty parent directory");
                // Walk up one level
                dir = match dir.parent() {
                    Some(p) => p.to_owned(),
                    None => break,
                };
            }
            Err(e) if e.raw_os_error() == Some(libc::ENOTEMPTY) => {
                // Directory is non-empty — stop here (this is the normal case)
                debug!(path = ?dir, "parent not empty, stopping prune");
                break;
            }
            Err(e) if e.raw_os_error() == Some(libc::ENOENT) => {
                // Already gone — keep walking up
                dir = match dir.parent() {
                    Some(p) => p.to_owned(),
                    None => break,
                };
            }
            Err(e) => {
                // Permission denied, read-only fs, etc. — log and stop
                warn!(path = ?dir, "prune_empty_parents failed: {}", e);
                break;
            }
        }
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
    async fn prune_removes_nested_empty_dirs() {
        let root = tempdir().unwrap();
        // Create root/a/b/c/file.txt then delete the file
        let c_dir = root.path().join("a").join("b").join("c");
        tokio::fs::create_dir_all(&c_dir).await.unwrap();
        let file = c_dir.join("file.txt");
        tokio::fs::write(&file, b"data").await.unwrap();
        tokio::fs::remove_file(&file).await.unwrap();

        // prune should remove c, b, a (all empty)
        prune_empty_parents(&file, root.path()).await;

        assert!(
            !root.path().join("a").exists(),
            "empty dirs should have been pruned"
        );
    }

    #[tokio::test]
    async fn prune_stops_at_non_empty_dir() {
        let root = tempdir().unwrap();
        // a/b/c/file.txt  + a/sibling.txt  → prune should stop at a/
        let c_dir = root.path().join("a").join("b").join("c");
        tokio::fs::create_dir_all(&c_dir).await.unwrap();
        let file = c_dir.join("file.txt");
        tokio::fs::write(&file, b"data").await.unwrap();
        tokio::fs::write(root.path().join("a").join("sibling.txt"), b"x")
            .await
            .unwrap();
        tokio::fs::remove_file(&file).await.unwrap();

        prune_empty_parents(&file, root.path()).await;

        // b and c should be pruned, but a must remain (has sibling.txt)
        assert!(
            !root.path().join("a").join("b").exists(),
            "b and c should be pruned"
        );
        assert!(
            root.path().join("a").exists(),
            "a must remain because it contains sibling.txt"
        );
    }

    #[tokio::test]
    async fn prune_never_removes_root() {
        let root = tempdir().unwrap();
        let file = root.path().join("only_file.txt");
        tokio::fs::write(&file, b"x").await.unwrap();
        tokio::fs::remove_file(&file).await.unwrap();

        prune_empty_parents(&file, root.path()).await;

        // The root tempdir should still exist
        assert!(root.path().exists(), "root must never be removed");
    }

    #[tokio::test]
    async fn test_drop_and_purge_delete_preserves_remote_file() {
        let root = tempdir().unwrap();
        let remote_root = tempdir().unwrap();
        let remote_url = format!("file://{}", remote_root.path().display());
        let storage = Arc::new(crate::storage::rclone::ObjectStoreBackend::from_remote_target(&remote_url).unwrap());

        let db_dir = tempdir().unwrap();
        let pool = crate::db::open_and_migrate(&db_dir.path().join("syncd.db")).await.unwrap();

        let folder_id = Uuid::new_v4();
        let folder_cfg = Arc::new(FolderConfig {
            id: folder_id,
            name: "offload-test".into(),
            local_path: root.path().to_path_buf(),
            remote_target: remote_url,
            mode: SyncMode::DropAndPurge,
            exclude_patterns: vec![],
            empty_folder_protection: false,
            ..Default::default()
        });

        // Simulate file already in remote store
        let remote_file = remote_root.path().join("render.mov");
        tokio::fs::write(&remote_file, b"video data").await.unwrap();

        // Seed folder in DB so foreign key passes
        crate::db::upsert_folder(&pool, &folder_cfg).await.unwrap();

        // Enqueue a Delete task as would happen when inotify sees the unlinked file
        let task_id = crate::db::enqueue_task(&pool, folder_id, "render.mov", OpType::Delete).await.unwrap();

        let ctx = TaskContext {
            task_id,
            folder_id,
            folder_cfg,
            rel_path: "render.mov".into(),
            op: OpType::Delete,
            pool: pool.clone(),
            storage,
            limiter: None,
            progress: None,
        };

        // Dispatch task
        execute_task(&ctx).await.unwrap();

        // Remote file MUST STILL EXIST!
        assert!(remote_file.exists(), "Remote file must NOT be deleted in DropAndPurge mode");

        // The delete task in DB must be cleared
        assert_eq!(crate::db::pending_task_count(&pool, folder_id).await.unwrap(), 0);
    }
}
