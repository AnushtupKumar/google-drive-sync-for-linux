//! SQLite WAL repository for `syncd`.
//!
//! This module owns the single `SqlitePool` (WAL mode, NORMAL synchronous)
//! shared across the entire daemon process.  Every public function is `async`
//! and runs on the tokio runtime via `sqlx`'s runtime-tokio driver.
//!
//! ## Design principles
//!
//! * **No business logic here.** `db.rs` is purely a data-access layer:
//!   it translates between Rust domain types and SQL rows.
//! * **Typed errors.** All public functions return `Result<_, DbError>` so
//!   callers can pattern-match on the error kind without downcasting.
//! * **Crash-safe writes.** Every multi-statement write uses an explicit
//!   transaction.  `IN_PROGRESS` tasks are reset to `PENDING` on startup so
//!   a daemon crash mid-transfer is automatically recovered.
//! * **Clock source.** All timestamps are UNIX milliseconds (`i64`).  The
//!   helper `now_ms()` is the single source of truth.

use chrono::{DateTime, TimeZone, Utc};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous},
    SqlitePool,
};
use std::{path::Path, str::FromStr, time::Duration};
use sync_common::{
    config::{FolderConfig, SyncMode},
    state::{FolderState, OpType},
};
use thiserror::Error;
use uuid::Uuid;

// ─────────────────────────────────────────────
// Error type
// ─────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum DbError {
    #[error("SQLite error: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),

    #[error("invalid UUID '{raw}': {source}")]
    InvalidUuid {
        raw: String,
        source: uuid::Error,
    },

    #[error("unknown op_type '{0}' in transfer_queue")]
    UnknownOpType(String),

    #[error("folder {0} not found in database")]
    FolderNotFound(Uuid),

    #[error("integer overflow computing timestamp")]
    TimestampOverflow,
}

// ─────────────────────────────────────────────
// DB-layer row types (private, used for query_as! mapping)
// ─────────────────────────────────────────────

/// Raw row returned by SELECT * FROM folders.
#[derive(Debug, sqlx::FromRow)]
struct FolderRow {
    folder_id: String,
    name: String,
    local_path: String,
    remote_target: String,
    mode: String,
    expected_st_dev: Option<i64>,
    baseline_file_count: i64,
    paused_until_unix_ms: Option<i64>,
    state: String,
    last_error: Option<String>,
    updated_at_ms: i64,
}

/// Public domain object rebuilt from `FolderRow`.
#[derive(Debug, Clone)]
pub struct FolderRecord {
    pub folder_id: Uuid,
    pub name: String,
    pub local_path: String,
    pub remote_target: String,
    pub mode: SyncMode,
    /// Last-known st_dev value; `None` if never successfully verified.
    pub expected_st_dev: Option<u64>,
    pub baseline_file_count: u64,
    /// `None` = not paused, `Some(-1)` = indefinite, `Some(ms)` = timed.
    pub paused_until_unix_ms: Option<i64>,
    pub state: FolderState,
    pub last_error: Option<String>,
    pub updated_at: DateTime<Utc>,
}

/// A single entry in the transfer queue, as returned by the DB.
#[derive(Debug, Clone)]
pub struct QueueTask {
    pub task_id: i64,
    pub folder_id: Uuid,
    pub rel_path: String,
    pub op_type: OpType,
    pub status: TaskStatus,
    pub attempt_count: u32,
    /// UNIX milliseconds; `0` = eligible immediately.
    pub next_retry_ms: i64,
    pub last_error: Option<String>,
    pub enqueued_at: DateTime<Utc>,
}

/// Lifecycle status of a `QueueTask`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Pending,
    InProgress,
    FailedRetrying,
}

impl TaskStatus {
    pub fn as_db_str(self) -> &'static str {
        match self {
            TaskStatus::Pending => "PENDING",
            TaskStatus::InProgress => "IN_PROGRESS",
            TaskStatus::FailedRetrying => "FAILED_RETRYING",
        }
    }

    fn from_db_str(s: &str) -> Option<Self> {
        match s {
            "PENDING" => Some(TaskStatus::Pending),
            "IN_PROGRESS" => Some(TaskStatus::InProgress),
            "FAILED_RETRYING" => Some(TaskStatus::FailedRetrying),
            _ => None,
        }
    }
}

/// One row from `synced_files`.
#[derive(Debug, Clone)]
pub struct SyncedFile {
    pub folder_id: Uuid,
    pub rel_path: String,
    pub inode: u64,
    pub size_bytes: u64,
    pub mtime_nsec: i64,
    pub sha256_hex: String,
    pub last_synced: DateTime<Utc>,
}

// ─────────────────────────────────────────────
// Pool initialisation
// ─────────────────────────────────────────────

/// Open (or create) the SQLite database at `db_path`, apply all pending
/// migrations, reset stuck `IN_PROGRESS` tasks to `PENDING`, and return
/// the connection pool.
///
/// Should be called exactly once from `main.rs` before spawning any workers.
pub async fn open_and_migrate(db_path: &Path) -> Result<SqlitePool, DbError> {
    // Ensure parent directory exists.
    if let Some(parent) = db_path.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(|e| {
            // sqlx 0.8: Io variant wraps std::io::Error directly
            DbError::Sqlx(sqlx::Error::Io(e))
        })?;
    }

    let opts = SqliteConnectOptions::from_str(&format!("sqlite://{}", db_path.display()))
        .map_err(DbError::Sqlx)?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .foreign_keys(true)
        .pragma("busy_timeout", "5000")
        .pragma("cache_size", "-32000") // 32 MiB page cache
        .pragma("temp_store", "memory")
        .pragma("mmap_size", "67108864"); // 64 MiB mmap

    let pool = SqlitePool::connect_with(opts).await.map_err(DbError::Sqlx)?;

    // Run embedded migrations (crates/syncd/migrations/*.sql).
    sqlx::migrate!("./migrations").run(&pool).await.map_err(DbError::Migration)?;

    // Crash recovery: any task that was `IN_PROGRESS` at shutdown is
    // re-queued as `PENDING` so the upload pipeline picks it up again.
    reset_in_progress_tasks(&pool).await?;

    Ok(pool)
}

/// Resets all `IN_PROGRESS` tasks to `PENDING` (called after crash recovery).
async fn reset_in_progress_tasks(pool: &SqlitePool) -> Result<(), DbError> {
    sqlx::query!(
        "UPDATE transfer_queue SET status = 'PENDING' WHERE status = 'IN_PROGRESS'"
    )
    .execute(pool)
    .await
    .map_err(DbError::Sqlx)?;
    Ok(())
}

// ─────────────────────────────────────────────
// Folder CRUD
// ─────────────────────────────────────────────

/// Upsert a folder row from a `FolderConfig`.
///
/// On first insert: `state = 'Watching'`, `baseline_file_count = 0`.
/// On update: preserves `expected_st_dev`, `baseline_file_count`, `state`,
/// `last_error`, and `paused_until_unix_ms` — only metadata fields are refreshed.
pub async fn upsert_folder(pool: &SqlitePool, cfg: &FolderConfig) -> Result<(), DbError> {
    let folder_id = cfg.id.to_string();
    let local_path = cfg.local_path.to_string_lossy().to_string();
    let mode = cfg.mode.to_string();
    let now = now_ms();

    sqlx::query!(
        r#"
        INSERT INTO folders (folder_id, name, local_path, remote_target, mode, updated_at_ms)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6)
        ON CONFLICT(folder_id) DO UPDATE SET
            name          = excluded.name,
            local_path    = excluded.local_path,
            remote_target = excluded.remote_target,
            mode          = excluded.mode,
            updated_at_ms = excluded.updated_at_ms
        "#,
        folder_id,
        cfg.name,
        local_path,
        cfg.remote_target,
        mode,
        now,
    )
    .execute(pool)
    .await
    .map_err(DbError::Sqlx)?;

    Ok(())
}

/// Remove a folder and all its associated rows (cascaded by FK).
pub async fn delete_folder(pool: &SqlitePool, folder_id: Uuid) -> Result<(), DbError> {
    let id = folder_id.to_string();
    sqlx::query!("DELETE FROM folders WHERE folder_id = ?1", id)
        .execute(pool)
        .await
        .map_err(DbError::Sqlx)?;
    Ok(())
}

/// Load all folder records from the database.
pub async fn load_all_folders(pool: &SqlitePool) -> Result<Vec<FolderRecord>, DbError> {
    let rows: Vec<FolderRow> = sqlx::query_as!(
        FolderRow,
        "SELECT folder_id, name, local_path, remote_target, mode,
                expected_st_dev, baseline_file_count, paused_until_unix_ms,
                state, last_error, updated_at_ms
         FROM folders
         ORDER BY name"
    )
    .fetch_all(pool)
    .await
    .map_err(DbError::Sqlx)?;

    rows.into_iter().map(row_to_folder_record).collect()
}

/// Load a single folder record by UUID.
pub async fn load_folder(pool: &SqlitePool, folder_id: Uuid) -> Result<FolderRecord, DbError> {
    let id = folder_id.to_string();
    let row: Option<FolderRow> = sqlx::query_as!(
        FolderRow,
        "SELECT folder_id, name, local_path, remote_target, mode,
                expected_st_dev, baseline_file_count, paused_until_unix_ms,
                state, last_error, updated_at_ms
         FROM folders WHERE folder_id = ?1",
        id
    )
    .fetch_optional(pool)
    .await
    .map_err(DbError::Sqlx)?;

    row.map(row_to_folder_record)
        .ok_or(DbError::FolderNotFound(folder_id))?
}

// ─────────────────────────────────────────────
// Folder state management
// ─────────────────────────────────────────────

/// Persist the `FolderState` to the `folders` table.
///
/// For `Error` states the `reason` string is stored in `last_error`.
/// For `Paused` states the `until` timestamp is stored in `paused_until_unix_ms`.
pub async fn set_folder_state(
    pool: &SqlitePool,
    folder_id: Uuid,
    state: &FolderState,
) -> Result<(), DbError> {
    let id = folder_id.to_string();
    let state_str = state.as_db_str();
    let now = now_ms();

    let (last_error, paused_until) = match state {
        FolderState::Error { reason } => (Some(reason.as_str()), None),
        FolderState::Paused { until } => {
            let ms: Option<i64> = match until {
                None => Some(-1),
                Some(ts) => Some(ts.timestamp_millis()),
            };
            (None, ms)
        }
        _ => (None, None),
    };

    sqlx::query!(
        r#"
        UPDATE folders
        SET state                = ?2,
            last_error           = ?3,
            paused_until_unix_ms = ?4,
            updated_at_ms        = ?5
        WHERE folder_id = ?1
        "#,
        id,
        state_str,
        last_error,
        paused_until,
        now,
    )
    .execute(pool)
    .await
    .map_err(DbError::Sqlx)?;

    Ok(())
}

/// Record the `expected_st_dev` after first successful mount verification.
pub async fn set_expected_st_dev(
    pool: &SqlitePool,
    folder_id: Uuid,
    st_dev: u64,
) -> Result<(), DbError> {
    let id = folder_id.to_string();
    let dev = st_dev as i64;
    let now = now_ms();
    sqlx::query!(
        "UPDATE folders SET expected_st_dev = ?2, updated_at_ms = ?3 WHERE folder_id = ?1",
        id,
        dev,
        now,
    )
    .execute(pool)
    .await
    .map_err(DbError::Sqlx)?;
    Ok(())
}

/// Update `baseline_file_count` after a full reconciliation walk.
pub async fn set_baseline_file_count(
    pool: &SqlitePool,
    folder_id: Uuid,
    count: u64,
) -> Result<(), DbError> {
    let id = folder_id.to_string();
    let c = count as i64;
    let now = now_ms();
    sqlx::query!(
        "UPDATE folders SET baseline_file_count = ?2, updated_at_ms = ?3 WHERE folder_id = ?1",
        id,
        c,
        now,
    )
    .execute(pool)
    .await
    .map_err(DbError::Sqlx)?;
    Ok(())
}

// ─────────────────────────────────────────────
// Pause / Resume helpers
// ─────────────────────────────────────────────

/// Set a timed or indefinite pause.  Writes `paused_until_unix_ms` and flips
/// the state column to `'Paused'`.
///
/// `duration` = `None` → indefinite (`paused_until_unix_ms = -1`).
/// `duration` = `Some(d)` → timed; `paused_until_unix_ms = now + d`.
pub async fn pause_folder(
    pool: &SqlitePool,
    folder_id: Uuid,
    duration: Option<Duration>,
) -> Result<(), DbError> {
    let paused_until: i64 = match duration {
        None => -1,
        Some(d) => {
            let extra_ms = i64::try_from(d.as_millis())
                .map_err(|_| DbError::TimestampOverflow)?;
            now_ms()
                .checked_add(extra_ms)
                .ok_or(DbError::TimestampOverflow)?
        }
    };

    let id = folder_id.to_string();
    let now = now_ms();
    sqlx::query!(
        r#"
        UPDATE folders
        SET state = 'Paused', paused_until_unix_ms = ?2, updated_at_ms = ?3
        WHERE folder_id = ?1
        "#,
        id,
        paused_until,
        now,
    )
    .execute(pool)
    .await
    .map_err(DbError::Sqlx)?;

    Ok(())
}

/// Atomically resume a folder:
/// 1. Move all rows in `dirty_paths_while_paused` into `transfer_queue` (PENDING).
/// 2. Clear `paused_until_unix_ms` and set `state = 'Watching'`.
pub async fn resume_folder(pool: &SqlitePool, folder_id: Uuid) -> Result<(), DbError> {
    let id = folder_id.to_string();
    let now = now_ms();

    let mut tx = pool.begin().await.map_err(DbError::Sqlx)?;

    // Move dirty paths into the transfer queue.
    // UPSERT: if a task for (folder_id, rel_path) already exists (from a
    // pre-pause upload attempt), reset its status to PENDING so it is retried.
    sqlx::query!(
        r#"
        INSERT INTO transfer_queue
            (folder_id, rel_path, op_type, status, enqueued_at_ms)
        SELECT
            folder_id,
            rel_path,
            'UPLOAD',
            'PENDING',
            ?2
        FROM dirty_paths_while_paused
        WHERE folder_id = ?1
        ON CONFLICT(folder_id, rel_path) DO UPDATE SET
            status        = 'PENDING',
            attempt_count = 0,
            next_retry_ms = 0,
            last_error    = NULL,
            enqueued_at_ms = excluded.enqueued_at_ms
        "#,
        id,
        now,
    )
    .execute(&mut *tx)
    .await
    .map_err(DbError::Sqlx)?;

    // Remove the dirty-paths staging rows.
    sqlx::query!(
        "DELETE FROM dirty_paths_while_paused WHERE folder_id = ?1",
        id
    )
    .execute(&mut *tx)
    .await
    .map_err(DbError::Sqlx)?;

    // Clear pause and flip state.
    sqlx::query!(
        r#"
        UPDATE folders
        SET state = 'Watching', paused_until_unix_ms = NULL, updated_at_ms = ?2
        WHERE folder_id = ?1
        "#,
        id,
        now,
    )
    .execute(&mut *tx)
    .await
    .map_err(DbError::Sqlx)?;

    tx.commit().await.map_err(DbError::Sqlx)?;
    Ok(())
}

/// Return all folders whose `paused_until_unix_ms` has expired (i.e. is
/// non-negative and ≤ `now_ms()`).  Used by the 1-second pause ticker.
pub async fn expired_timed_pauses(pool: &SqlitePool) -> Result<Vec<Uuid>, DbError> {
    let now = now_ms();
    let rows = sqlx::query!(
        r#"
        SELECT folder_id
        FROM   folders
        WHERE  paused_until_unix_ms IS NOT NULL
               AND paused_until_unix_ms >= 0
               AND paused_until_unix_ms <= ?1
        "#,
        now,
    )
    .fetch_all(pool)
    .await
    .map_err(DbError::Sqlx)?;

    rows.into_iter()
        .map(|r| parse_uuid(&r.folder_id))
        .collect()
}

// ─────────────────────────────────────────────
// Transfer queue
// ─────────────────────────────────────────────

/// Enqueue a new task or reset an existing one for a `(folder_id, rel_path)`.
///
/// If a row already exists for the same pair (regardless of its current
/// status), it is replaced with a fresh PENDING entry.  This is the correct
/// behaviour when a file changes again during an in-flight upload.
pub async fn enqueue_task(
    pool: &SqlitePool,
    folder_id: Uuid,
    rel_path: &str,
    op_type: OpType,
) -> Result<i64, DbError> {
    let id = folder_id.to_string();
    let op = op_type.as_db_str();
    let now = now_ms();

    let result = sqlx::query!(
        r#"
        INSERT INTO transfer_queue
            (folder_id, rel_path, op_type, status, attempt_count, next_retry_ms, enqueued_at_ms)
        VALUES (?1, ?2, ?3, 'PENDING', 0, 0, ?4)
        ON CONFLICT(folder_id, rel_path) DO UPDATE SET
            op_type       = excluded.op_type,
            status        = 'PENDING',
            attempt_count = 0,
            next_retry_ms = 0,
            last_error    = NULL,
            enqueued_at_ms = excluded.enqueued_at_ms
        RETURNING task_id
        "#,
        id,
        rel_path,
        op,
        now,
    )
    .fetch_one(pool)
    .await
    .map_err(DbError::Sqlx)?;

    Ok(result.task_id)
}

/// Insert a dirty path captured while a folder is paused.  Ignores conflicts
/// (the path is already recorded).
pub async fn insert_dirty_while_paused(
    pool: &SqlitePool,
    folder_id: Uuid,
    rel_path: &str,
) -> Result<(), DbError> {
    let id = folder_id.to_string();
    let now = now_ms();
    sqlx::query!(
        r#"
        INSERT OR IGNORE INTO dirty_paths_while_paused (folder_id, rel_path, detected_at_ms)
        VALUES (?1, ?2, ?3)
        "#,
        id,
        rel_path,
        now,
    )
    .execute(pool)
    .await
    .map_err(DbError::Sqlx)?;
    Ok(())
}

/// Fetch up to `limit` tasks that are eligible to run right now.
///
/// Eligible = (`status = 'PENDING'` OR `status = 'FAILED_RETRYING'`)
///             AND `next_retry_ms <= now_ms()`.
///
/// Results are ordered by `next_retry_ms ASC` (oldest retry-eligible first)
/// then `enqueued_at_ms ASC` (FIFO for new tasks).
pub async fn fetch_eligible_tasks(
    pool: &SqlitePool,
    limit: u32,
) -> Result<Vec<QueueTask>, DbError> {
    let now = now_ms();
    let limit = limit as i64;

    let rows = sqlx::query!(
        r#"
        SELECT task_id, folder_id, rel_path, op_type, status,
               attempt_count, next_retry_ms, last_error, enqueued_at_ms
        FROM   transfer_queue
        WHERE  (status = 'PENDING' OR status = 'FAILED_RETRYING')
               AND next_retry_ms <= ?1
        ORDER  BY next_retry_ms ASC, enqueued_at_ms ASC
        LIMIT  ?2
        "#,
        now,
        limit,
    )
    .fetch_all(pool)
    .await
    .map_err(DbError::Sqlx)?;

    rows.into_iter().map(|r| {
        let folder_id = parse_uuid(&r.folder_id)?;
        let op_type = OpType::from_db_str(&r.op_type)
            .ok_or_else(|| DbError::UnknownOpType(r.op_type.clone()))?;
        let status = TaskStatus::from_db_str(&r.status)
            .unwrap_or(TaskStatus::Pending);

        Ok(QueueTask {
            task_id: r.task_id,
            folder_id,
            rel_path: r.rel_path,
            op_type,
            status,
            attempt_count: r.attempt_count as u32,
            next_retry_ms: r.next_retry_ms,
            last_error: r.last_error,
            enqueued_at: ms_to_dt(r.enqueued_at_ms),
        })
    })
    .collect()
}

/// Mark a task as `IN_PROGRESS` atomically (optimistic update — only succeeds
/// if the task is currently `PENDING` or `FAILED_RETRYING`).
///
/// Returns `true` if the row was updated (i.e. we "won" the claim).
pub async fn claim_task(pool: &SqlitePool, task_id: i64) -> Result<bool, DbError> {
    let now = now_ms();
    let result = sqlx::query!(
        r#"
        UPDATE transfer_queue
        SET    status = 'IN_PROGRESS'
        WHERE  task_id = ?1
               AND (status = 'PENDING' OR status = 'FAILED_RETRYING')
               AND next_retry_ms <= ?2
        "#,
        task_id,
        now,
    )
    .execute(pool)
    .await
    .map_err(DbError::Sqlx)?;

    Ok(result.rows_affected() == 1)
}

/// Remove a task that completed successfully and record the file in
/// `synced_files`, all within a single transaction.
#[allow(clippy::too_many_arguments)]
pub async fn complete_task(
    pool: &SqlitePool,
    task_id: i64,
    folder_id: Uuid,
    rel_path: &str,
    inode: u64,
    size_bytes: u64,
    mtime_nsec: i64,
    sha256_hex: &str,
) -> Result<(), DbError> {
    let fid = folder_id.to_string();
    let ino = inode as i64;
    let sz = size_bytes as i64;
    let now = now_ms();

    let mut tx = pool.begin().await.map_err(DbError::Sqlx)?;

    // Upsert the synced_files baseline entry.
    sqlx::query!(
        r#"
        INSERT INTO synced_files
            (folder_id, rel_path, inode, size_bytes, mtime_nsec, sha256_hex, last_synced_ms)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        ON CONFLICT(folder_id, rel_path) DO UPDATE SET
            inode          = excluded.inode,
            size_bytes     = excluded.size_bytes,
            mtime_nsec     = excluded.mtime_nsec,
            sha256_hex     = excluded.sha256_hex,
            last_synced_ms = excluded.last_synced_ms
        "#,
        fid, rel_path, ino, sz, mtime_nsec, sha256_hex, now,
    )
    .execute(&mut *tx)
    .await
    .map_err(DbError::Sqlx)?;

    // Remove the completed task.
    sqlx::query!("DELETE FROM transfer_queue WHERE task_id = ?1", task_id)
        .execute(&mut *tx)
        .await
        .map_err(DbError::Sqlx)?;

    tx.commit().await.map_err(DbError::Sqlx)?;
    Ok(())
}

/// Mark a task as `FAILED_RETRYING` with updated backoff metadata.
///
/// `next_retry_ms` should be computed by the caller using the exponential
/// backoff formula.
pub async fn fail_task(
    pool: &SqlitePool,
    task_id: i64,
    error: &str,
    next_retry_ms: i64,
) -> Result<(), DbError> {
    sqlx::query!(
        r#"
        UPDATE transfer_queue
        SET    status        = 'FAILED_RETRYING',
               last_error   = ?2,
               next_retry_ms = ?3,
               attempt_count = attempt_count + 1
        WHERE  task_id = ?1
        "#,
        task_id,
        error,
        next_retry_ms,
    )
    .execute(pool)
    .await
    .map_err(DbError::Sqlx)?;
    Ok(())
}

/// Delete a task unconditionally (used by `offload.rs` after the POSIX unlink).
pub async fn delete_task(pool: &SqlitePool, task_id: i64) -> Result<(), DbError> {
    sqlx::query!("DELETE FROM transfer_queue WHERE task_id = ?1", task_id)
        .execute(pool)
        .await
        .map_err(DbError::Sqlx)?;
    Ok(())
}

/// Purge all pending DELETE tasks for a folder from the in-memory queue
/// and the database.  Called when `safety.rs` detects a drive disconnect.
pub async fn purge_delete_tasks_for_folder(
    pool: &SqlitePool,
    folder_id: Uuid,
) -> Result<u64, DbError> {
    let id = folder_id.to_string();
    let result = sqlx::query!(
        r#"
        DELETE FROM transfer_queue
        WHERE folder_id = ?1 AND op_type = 'DELETE'
        "#,
        id,
    )
    .execute(pool)
    .await
    .map_err(DbError::Sqlx)?;

    Ok(result.rows_affected())
}

/// Purge upload tasks whose local file no longer exists on disk.
/// This cleans up transient files that were created and deleted before sync.
pub async fn purge_missing_upload_tasks(
    pool: &SqlitePool,
    folder_id: Uuid,
    local_root: &std::path::Path,
) -> Result<u64, DbError> {
    use sqlx::Row;
    let id = folder_id.to_string();
    let rows = sqlx::query("SELECT task_id, rel_path FROM transfer_queue WHERE folder_id = ?1 AND op_type = 'UPLOAD'")
        .bind(&id)
        .fetch_all(pool)
        .await
        .map_err(DbError::Sqlx)?;

    let mut purged = 0u64;
    for r in rows {
        let task_id: i64 = r.try_get("task_id").map_err(DbError::Sqlx)?;
        let rel_path: String = r.try_get("rel_path").map_err(DbError::Sqlx)?;
        let abs = local_root.join(&rel_path);
        if !abs.exists() {
            sqlx::query("DELETE FROM transfer_queue WHERE task_id = ?1")
                .bind(task_id)
                .execute(pool)
                .await
                .map_err(DbError::Sqlx)?;
            purged += 1;
        }
    }

    Ok(purged)
}

/// Return the count of pending/retrying tasks for a folder.
pub async fn pending_task_count(pool: &SqlitePool, folder_id: Uuid) -> Result<u64, DbError> {
    let id = folder_id.to_string();
    let row = sqlx::query!(
        r#"
        SELECT COUNT(*) AS cnt
        FROM   transfer_queue
        WHERE  folder_id = ?1
               AND (status = 'PENDING' OR status = 'FAILED_RETRYING' OR status = 'IN_PROGRESS')
        "#,
        id,
    )
    .fetch_one(pool)
    .await
    .map_err(DbError::Sqlx)?;

    Ok(row.cnt as u64)
}

// ─────────────────────────────────────────────
// synced_files helpers
// ─────────────────────────────────────────────

/// Look up the baseline record for a single file.  Returns `None` if the
/// file has never been synced.
pub async fn get_synced_file(
    pool: &SqlitePool,
    folder_id: Uuid,
    rel_path: &str,
) -> Result<Option<SyncedFile>, DbError> {
    let id = folder_id.to_string();
    let row = sqlx::query!(
        r#"
        SELECT folder_id, rel_path, inode, size_bytes, mtime_nsec, sha256_hex, last_synced_ms
        FROM   synced_files
        WHERE  folder_id = ?1 AND rel_path = ?2
        "#,
        id,
        rel_path,
    )
    .fetch_optional(pool)
    .await
    .map_err(DbError::Sqlx)?;

    match row {
        None => Ok(None),
        Some(r) => Ok(Some(SyncedFile {
            folder_id: parse_uuid(&r.folder_id)?,
            rel_path: r.rel_path,
            inode: r.inode as u64,
            size_bytes: r.size_bytes as u64,
            mtime_nsec: r.mtime_nsec,
            sha256_hex: r.sha256_hex,
            last_synced: ms_to_dt(r.last_synced_ms),
        })),
    }
}

/// Delete the `synced_files` record for a file that has been remotely deleted
/// or locally offloaded.
pub async fn remove_synced_file(
    pool: &SqlitePool,
    folder_id: Uuid,
    rel_path: &str,
) -> Result<(), DbError> {
    let id = folder_id.to_string();
    sqlx::query!(
        "DELETE FROM synced_files WHERE folder_id = ?1 AND rel_path = ?2",
        id,
        rel_path,
    )
    .execute(pool)
    .await
    .map_err(DbError::Sqlx)?;
    Ok(())
}

/// Count all synced files for a folder.  Used to update `baseline_file_count`.
pub async fn count_synced_files(pool: &SqlitePool, folder_id: Uuid) -> Result<u64, DbError> {
    let id = folder_id.to_string();
    let row = sqlx::query!(
        "SELECT COUNT(*) AS cnt FROM synced_files WHERE folder_id = ?1",
        id,
    )
    .fetch_one(pool)
    .await
    .map_err(DbError::Sqlx)?;

    Ok(row.cnt as u64)
}

/// Return all relative paths currently recorded in `synced_files` for a folder.
pub async fn list_synced_files(
    pool: &SqlitePool,
    folder_id: Uuid,
) -> Result<Vec<String>, DbError> {
    use sqlx::Row;
    let id = folder_id.to_string();
    let rows = sqlx::query("SELECT rel_path FROM synced_files WHERE folder_id = ?1")
        .bind(&id)
        .fetch_all(pool)
        .await
        .map_err(DbError::Sqlx)?;

    let mut paths = Vec::with_capacity(rows.len());
    for r in rows {
        let p: String = r.try_get("rel_path").map_err(DbError::Sqlx)?;
        paths.push(p);
    }
    Ok(paths)
}

// ─────────────────────────────────────────────
// Internal helpers
// ─────────────────────────────────────────────

/// Returns the current UNIX time in milliseconds.
pub fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

/// Converts a UNIX millisecond timestamp to a `DateTime<Utc>`.
fn ms_to_dt(ms: i64) -> DateTime<Utc> {
    let secs = ms / 1000;
    let nsecs = ((ms % 1000) * 1_000_000) as u32;
    Utc.timestamp_opt(secs, nsecs)
        .single()
        .unwrap_or_else(Utc::now)
}

/// Parse a UUID from a database string, returning a typed error on failure.
fn parse_uuid(s: &str) -> Result<Uuid, DbError> {
    Uuid::parse_str(s).map_err(|e| DbError::InvalidUuid {
        raw: s.to_owned(),
        source: e,
    })
}

/// Convert a `FolderRow` into the public `FolderRecord` domain type.
fn row_to_folder_record(r: FolderRow) -> Result<FolderRecord, DbError> {
    let folder_id = parse_uuid(&r.folder_id)?;
    let mode = match r.mode.as_str() {
        "continuous_backup" => SyncMode::ContinuousBackup,
        "drop_and_purge" => SyncMode::DropAndPurge,
        other => {
            // Treat unknown modes as ContinuousBackup rather than crashing.
            tracing::warn!(
                folder_id = %folder_id,
                "unknown sync mode '{}' in db, defaulting to ContinuousBackup",
                other
            );
            SyncMode::ContinuousBackup
        }
    };

    let state = reconstruct_folder_state(&r.state, r.paused_until_unix_ms, r.last_error.as_deref());

    Ok(FolderRecord {
        folder_id,
        name: r.name,
        local_path: r.local_path,
        remote_target: r.remote_target,
        mode,
        expected_st_dev: r.expected_st_dev.map(|d| d as u64),
        baseline_file_count: r.baseline_file_count as u64,
        paused_until_unix_ms: r.paused_until_unix_ms,
        state,
        last_error: r.last_error,
        updated_at: ms_to_dt(r.updated_at_ms),
    })
}

/// Reconstruct a coarse `FolderState` from the DB columns.
///
/// The database only stores the variant name; rich detail (full paused-until
/// timestamp) is reconstructed here.
fn reconstruct_folder_state(
    state_str: &str,
    paused_until_ms: Option<i64>,
    last_error: Option<&str>,
) -> FolderState {
    match state_str {
        "Watching" => FolderState::Watching,
        "Debouncing" => FolderState::Debouncing,
        "Syncing" => FolderState::Syncing,
        "Paused" => {
            let until = match paused_until_ms {
                None | Some(-1) => None,
                Some(ms) => Some(ms_to_dt(ms)),
            };
            FolderState::Paused { until }
        }
        "DriveDisconnected" => FolderState::DriveDisconnected,
        "Error" => FolderState::Error {
            reason: last_error.unwrap_or("unknown error").to_owned(),
        },
        "Reconciling" => FolderState::Reconciling,
        other => {
            tracing::warn!("unknown folder state '{}' in db, defaulting to Error", other);
            FolderState::Error {
                reason: format!("unrecognised persisted state: {other}"),
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
    use std::path::PathBuf;
    use sync_common::config::FolderConfig;
    use tempfile::tempdir;

    async fn test_db() -> (tempfile::TempDir, SqlitePool) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let pool = open_and_migrate(&path).await.expect("open_and_migrate");
        (dir, pool) // keep dir alive so the temp directory isn't deleted
    }

    fn make_folder_cfg(id: Uuid) -> FolderConfig {
        FolderConfig {
            id,
            name: "test".into(),
            local_path: PathBuf::from("/tmp/test"),
            remote_target: format!("s3://bucket/{id}"),
            mode: SyncMode::ContinuousBackup,
            exclude_patterns: vec![],
            empty_folder_protection: true,
            endpoint: None,
            region: None,
            allow_http: false,
        }
    }

    #[tokio::test]
    async fn upsert_and_load_folder() {
        let (_dir, pool) = test_db().await;
        let id = Uuid::new_v4();
        let cfg = make_folder_cfg(id);

        upsert_folder(&pool, &cfg).await.unwrap();
        let all = load_all_folders(&pool).await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].folder_id, id);
        assert_eq!(all[0].name, "test");
    }

    #[tokio::test]
    async fn enqueue_and_claim_task() {
        let (_dir, pool) = test_db().await;
        let id = Uuid::new_v4();
        let cfg = make_folder_cfg(id);
        upsert_folder(&pool, &cfg).await.unwrap();

        let task_id = enqueue_task(&pool, id, "photos/img.jpg", OpType::Upload)
            .await
            .unwrap();

        let tasks = fetch_eligible_tasks(&pool, 10).await.unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].task_id, task_id);

        let claimed = claim_task(&pool, task_id).await.unwrap();
        assert!(claimed);

        // Should not appear in eligible list while IN_PROGRESS.
        let tasks2 = fetch_eligible_tasks(&pool, 10).await.unwrap();
        assert!(tasks2.is_empty());
    }

    #[tokio::test]
    async fn complete_task_upserts_synced_files() {
        let (_dir, pool) = test_db().await;
        let id = Uuid::new_v4();
        let cfg = make_folder_cfg(id);
        upsert_folder(&pool, &cfg).await.unwrap();

        let task_id = enqueue_task(&pool, id, "a/b.txt", OpType::Upload)
            .await
            .unwrap();
        claim_task(&pool, task_id).await.unwrap();

        complete_task(
            &pool,
            task_id,
            id,
            "a/b.txt",
            12345,
            1024,
            1_700_000_000_000_000_000i64,
            "deadbeef",
        )
        .await
        .unwrap();

        let sf = get_synced_file(&pool, id, "a/b.txt").await.unwrap();
        assert!(sf.is_some());
        assert_eq!(sf.unwrap().sha256_hex, "deadbeef");
    }

    #[tokio::test]
    async fn pause_resume_moves_dirty_paths() {
        let (_dir, pool) = test_db().await;
        let id = Uuid::new_v4();
        let cfg = make_folder_cfg(id);
        upsert_folder(&pool, &cfg).await.unwrap();

        // Pause indefinitely.
        pause_folder(&pool, id, None).await.unwrap();

        // Record dirty paths while paused.
        insert_dirty_while_paused(&pool, id, "file1.txt").await.unwrap();
        insert_dirty_while_paused(&pool, id, "file2.txt").await.unwrap();

        // Resume: dirty paths should become transfer_queue rows.
        resume_folder(&pool, id).await.unwrap();

        let tasks = fetch_eligible_tasks(&pool, 10).await.unwrap();
        let paths: Vec<_> = tasks.iter().map(|t| t.rel_path.as_str()).collect();
        assert!(paths.contains(&"file1.txt"));
        assert!(paths.contains(&"file2.txt"));
    }

    #[tokio::test]
    async fn fail_task_increments_attempt_count() {
        let (_dir, pool) = test_db().await;
        let id = Uuid::new_v4();
        let cfg = make_folder_cfg(id);
        upsert_folder(&pool, &cfg).await.unwrap();

        let task_id = enqueue_task(&pool, id, "x.bin", OpType::Upload).await.unwrap();
        claim_task(&pool, task_id).await.unwrap();

        let retry_ms = now_ms() + 5_000;
        fail_task(&pool, task_id, "connection refused", retry_ms).await.unwrap();

        // Task should be FAILED_RETRYING and not immediately eligible.
        let tasks = fetch_eligible_tasks(&pool, 10).await.unwrap();
        assert!(tasks.is_empty(), "should not be eligible before retry delay");
    }
}
