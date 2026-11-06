-- syncd SQLite WAL schema – migration 0001
-- Applied automatically by sqlx::migrate! at daemon startup.
--
-- NOTE: journal_mode and synchronous cannot be changed inside a transaction,
-- so they are set programmatically via SqliteConnectOptions in db.rs.
-- Only foreign_keys and busy_timeout are safe to set here.

PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;

-- ─────────────────────────────────────────────────────────────────────────────
-- Folder metadata, mount device IDs, and pause timers
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS folders (
    -- Stable UUID from FolderConfig::id; never changes across config reloads.
    folder_id   TEXT    PRIMARY KEY NOT NULL,

    -- Human-readable label (may change on config reload without ill effect).
    name        TEXT    NOT NULL,

    -- Absolute local path being watched / offloaded.
    local_path  TEXT    NOT NULL UNIQUE,

    -- Cloud destination URI (e.g. "s3://bucket/path").
    remote_target TEXT  NOT NULL UNIQUE,

    -- Serialised SyncMode: "continuous_backup" | "drop_and_purge"
    mode        TEXT    NOT NULL,

    -- POSIX st_dev value recorded when the folder was first successfully opened.
    -- NULL until first successful mount verification.
    expected_st_dev INTEGER,

    -- Number of files in the synced_files table at the last full reconciliation.
    -- Used by the Empty Folder Safety Lock.
    baseline_file_count INTEGER NOT NULL DEFAULT 0,

    -- Pause expiry:
    --   NULL  → not paused
    --   -1    → indefinitely paused
    --   >0    → paused until this UNIX epoch millisecond timestamp
    paused_until_unix_ms INTEGER,

    -- Current FolderState variant name (e.g. "Watching", "Paused", "Error").
    -- The full FolderState (with detail JSON for Error/Paused) lives in memory;
    -- this column lets us reconstruct a coarse state on restart.
    state       TEXT    NOT NULL DEFAULT 'Watching',

    -- Last error message, if state = 'Error'.
    last_error  TEXT,

    -- UNIX epoch milliseconds of the last state transition write.
    updated_at_ms INTEGER NOT NULL
);

-- ─────────────────────────────────────────────────────────────────────────────
-- Baseline index: one row per synced file
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS synced_files (
    folder_id   TEXT    NOT NULL,

    -- Path relative to folder.local_path, using '/' as separator, no leading '/'.
    rel_path    TEXT    NOT NULL,

    -- Inode number at the time of last successful sync.
    inode       INTEGER NOT NULL,

    -- File size in bytes.
    size_bytes  INTEGER NOT NULL,

    -- Modification time in nanoseconds since UNIX epoch.
    mtime_nsec  INTEGER NOT NULL,

    -- Hex-encoded SHA-256 digest of the file content at last sync.
    sha256_hex  TEXT    NOT NULL,

    -- UNIX epoch milliseconds when this file was last uploaded successfully.
    last_synced_ms INTEGER NOT NULL,

    PRIMARY KEY (folder_id, rel_path),
    FOREIGN KEY (folder_id) REFERENCES folders(folder_id) ON DELETE CASCADE
);

-- Speed up "list all files for folder X" scans.
CREATE INDEX IF NOT EXISTS idx_synced_files_folder
    ON synced_files(folder_id);

-- ─────────────────────────────────────────────────────────────────────────────
-- Crash-safe persistent transfer & operation queue
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS transfer_queue (
    -- Auto-incrementing PK; used as stable task identifier in TelemetrySnapshot.
    task_id     INTEGER PRIMARY KEY AUTOINCREMENT,

    folder_id   TEXT    NOT NULL,

    -- Path relative to folder root.
    rel_path    TEXT    NOT NULL,

    -- Operation type: 'UPLOAD' | 'DELETE' | 'OFFLOAD'
    op_type     TEXT    NOT NULL,

    -- Task lifecycle:
    --   'PENDING'          → waiting to be picked up by the worker pool
    --   'IN_PROGRESS'      → currently being executed (reset to PENDING on restart)
    --   'FAILED_RETRYING'  → previous attempt failed; next_retry_ms is set
    status      TEXT    NOT NULL DEFAULT 'PENDING',

    -- Total number of upload attempts made so far (for exponential backoff).
    attempt_count INTEGER NOT NULL DEFAULT 0,

    -- UNIX epoch milliseconds before which this task must not be retried.
    -- 0 means "eligible immediately".
    next_retry_ms INTEGER NOT NULL DEFAULT 0,

    -- The error string from the most recent failed attempt.
    last_error  TEXT,

    -- UNIX epoch milliseconds when this task entered the queue.
    enqueued_at_ms INTEGER NOT NULL,

    -- Only one pending/retrying task per (folder, path) at a time.
    -- If a file is modified again while a task is IN_PROGRESS, the debouncer
    -- will upsert this row (resetting status to PENDING) after the current
    -- attempt completes.
    UNIQUE (folder_id, rel_path),
    FOREIGN KEY (folder_id) REFERENCES folders(folder_id) ON DELETE CASCADE
);

-- Speed up "fetch next eligible tasks" queries.
CREATE INDEX IF NOT EXISTS idx_tq_status_retry
    ON transfer_queue(status, next_retry_ms);

CREATE INDEX IF NOT EXISTS idx_tq_folder
    ON transfer_queue(folder_id, status);

-- ─────────────────────────────────────────────────────────────────────────────
-- Dirty paths captured by inotify while a folder is Paused
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dirty_paths_while_paused (
    folder_id      TEXT    NOT NULL,
    rel_path       TEXT    NOT NULL,
    detected_at_ms INTEGER NOT NULL,

    PRIMARY KEY (folder_id, rel_path),
    FOREIGN KEY (folder_id) REFERENCES folders(folder_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_dirty_folder
    ON dirty_paths_while_paused(folder_id);
