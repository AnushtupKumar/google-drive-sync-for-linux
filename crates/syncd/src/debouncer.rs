//! Sliding-window + max-wait debouncer that coalesces inotify events
//! into `transfer_queue` entries.
//!
//! ## Algorithm
//!
//! Every incoming [`WatchEvent`] is inserted into an in-memory
//! `HashMap<(Uuid, RelPath), DebounceEntry>`.  A 250 ms tokio interval
//! ("tick") examines every entry and flushes it to SQLite when **either**
//! condition is true:
//!
//! 1. **Quiet window**: `now − last_updated ≥ debounce_quiet_ms`
//!    (activity has settled).
//! 2. **Starvation cap**: `now − first_seen ≥ debounce_max_wait_ms`
//!    (file is continuously written but we can no longer wait).
//!
//! While any entry exists in the map, the folder's reported state is
//! `FolderState::Debouncing` (reported by the supervisor, not here).
//!
//! ## Paused folders
//!
//! When a folder is paused, incoming events are written to
//! `dirty_paths_while_paused` in SQLite instead of the debounce map.
//! The supervisor calls [`Debouncer::set_paused`] / [`Debouncer::set_active`].
//!
//! ## Delete priority
//!
//! Within one debounce window, `Delete` takes priority over `Upload`:
//! once we see that a file is gone, we never queue an upload for it.

use crate::{db, watcher::WatchEvent};
use sqlx::SqlitePool;
use std::{collections::{HashMap, HashSet}, path::PathBuf, time::Duration};
use sync_common::state::OpType;
use thiserror::Error;
use tokio::time::{interval, Instant, MissedTickBehavior};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

// ─────────────────────────────────────────────
// Error type
// ─────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum DebouncerError {
    #[error("database error: {0}")]
    Db(#[from] db::DbError),
}

// ─────────────────────────────────────────────
// Internal state
// ─────────────────────────────────────────────

/// Per-path coalescing state.
#[derive(Debug)]
struct DebounceEntry {
    /// Timestamp of the very first event for this path in the current window.
    first_seen: Instant,
    /// Timestamp of the most recent event for this path.
    last_updated: Instant,
    /// Current winning operation (Delete > Upload).
    latest_op: OpType,
}

impl DebounceEntry {
    fn new(op: OpType) -> Self {
        let now = Instant::now();
        Self { first_seen: now, last_updated: now, latest_op: op }
    }

    /// Update with a new event, applying Delete-wins priority.
    fn update(&mut self, op: OpType) {
        self.last_updated = Instant::now();
        // Delete is terminal: once we know the file is gone, don't upload it
        if op == OpType::Delete {
            self.latest_op = OpType::Delete;
        }
    }

    /// `true` if this entry is ready to flush based on the timing config.
    fn should_flush(&self, now: Instant, quiet: Duration, max_wait: Duration) -> bool {
        now.duration_since(self.last_updated) >= quiet
            || now.duration_since(self.first_seen) >= max_wait
    }
}

// ─────────────────────────────────────────────
// Control Commands & Handle
// ─────────────────────────────────────────────

#[derive(Debug)]
pub enum DebouncerCmd {
    SetPaused(Uuid),
    SetActive(Uuid),
    DrainFolder(Uuid),
    DrainDeletes(Uuid),
    HasPending(Uuid, tokio::sync::oneshot::Sender<bool>),
}

/// A cloneable handle to the running debouncer actor.
#[derive(Clone)]
pub struct DebouncerHandle {
    cmd_tx: mpsc::UnboundedSender<DebouncerCmd>,
    event_tx: mpsc::UnboundedSender<WatchEvent>,
}

impl DebouncerHandle {
    /// Send an event into the debouncer.
    pub fn send_event(&self, event: WatchEvent) -> Result<(), mpsc::error::SendError<WatchEvent>> {
        self.event_tx.send(event)
    }

    /// Return a clone of the watch event sender.
    pub fn event_sender(&self) -> mpsc::UnboundedSender<WatchEvent> {
        self.event_tx.clone()
    }

    /// Mark folder as paused.
    pub fn set_paused(&self, folder_id: Uuid) {
        let _ = self.cmd_tx.send(DebouncerCmd::SetPaused(folder_id));
    }

    /// Mark folder as active.
    pub fn set_active(&self, folder_id: Uuid) {
        let _ = self.cmd_tx.send(DebouncerCmd::SetActive(folder_id));
    }

    /// Drain all entries for a folder.
    pub fn drain_folder(&self, folder_id: Uuid) {
        let _ = self.cmd_tx.send(DebouncerCmd::DrainFolder(folder_id));
    }

    /// Drain all Delete entries for a folder.
    pub fn drain_deletes_for_folder(&self, folder_id: Uuid) {
        let _ = self.cmd_tx.send(DebouncerCmd::DrainDeletes(folder_id));
    }

    /// Query if debouncer has pending entries for a folder.
    pub async fn has_pending(&self, folder_id: Uuid) -> bool {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self.cmd_tx.send(DebouncerCmd::HasPending(folder_id, tx)).is_err() {
            return false;
        }
        rx.await.unwrap_or(false)
    }
}

// ─────────────────────────────────────────────
// Debouncer
// ─────────────────────────────────────────────

/// The debouncer actor.
pub struct Debouncer {
    /// Receives raw events from all active folder watchers.
    event_rx: mpsc::UnboundedReceiver<WatchEvent>,
    /// Receives control commands from supervisor.
    cmd_rx: mpsc::UnboundedReceiver<DebouncerCmd>,
    pool: SqlitePool,
    /// `now − last_updated` must be ≥ this before flushing.
    quiet: Duration,
    /// Hard upper bound on how long a path stays in the map.
    max_wait: Duration,
    /// Folders whose events go to `dirty_paths_while_paused` instead.
    paused: HashSet<Uuid>,
    /// The coalescing map.
    map: HashMap<(Uuid, PathBuf), DebounceEntry>,
}

impl Debouncer {
    /// Construct a Debouncer along with its control handle.
    pub fn create(
        pool: SqlitePool,
        quiet_ms: u64,
        max_wait_ms: u64,
    ) -> (Self, DebouncerHandle) {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let handle = DebouncerHandle { cmd_tx, event_tx };
        let debouncer = Self {
            event_rx,
            cmd_rx,
            pool,
            quiet: Duration::from_millis(quiet_ms),
            max_wait: Duration::from_millis(max_wait_ms),
            paused: HashSet::new(),
            map: HashMap::new(),
        };
        (debouncer, handle)
    }

    pub fn new(
        event_rx: mpsc::UnboundedReceiver<WatchEvent>,
        pool: SqlitePool,
        quiet_ms: u64,
        max_wait_ms: u64,
    ) -> Self {
        let (_cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        Self {
            event_rx,
            cmd_rx,
            pool,
            quiet: Duration::from_millis(quiet_ms),
            max_wait: Duration::from_millis(max_wait_ms),
            paused: HashSet::new(),
            map: HashMap::new(),
        }
    }

    // ── Supervisor control API (called from the supervisor actor) ────────────

    /// Mark `folder_id` as paused so new events go to `dirty_paths_while_paused`.
    pub fn set_paused(&mut self, folder_id: Uuid) {
        self.paused.insert(folder_id);
    }

    /// Mark `folder_id` as active (resumed).
    pub fn set_active(&mut self, folder_id: Uuid) {
        self.paused.remove(&folder_id);
    }

    /// Remove all in-memory entries for `folder_id`.
    ///
    /// Call when a folder is removed from config or transitions to
    /// `DriveDisconnected`.
    pub fn drain_folder(&mut self, folder_id: Uuid) {
        self.map.retain(|(fid, _), _| *fid != folder_id);
    }

    /// Remove all `Delete` entries for `folder_id`.
    ///
    /// Called by `safety.rs` on drive disconnect to prevent accidental
    /// remote deletions while the drive is unreachable.
    pub fn drain_deletes_for_folder(&mut self, folder_id: Uuid) {
        self.map.retain(|(fid, _), e| {
            !(*fid == folder_id && e.latest_op == OpType::Delete)
        });
    }

    /// Returns `true` if any entries are pending for `folder_id`.
    pub fn has_pending(&self, folder_id: Uuid) -> bool {
        self.map.keys().any(|(fid, _)| *fid == folder_id)
    }

    // ── Main event loop ──────────────────────────────────────────────────────

    /// Run the debouncer until `cancel` fires.
    ///
    /// On shutdown, all remaining entries are flushed immediately so no
    /// events are lost across daemon restarts (they survive in `transfer_queue`).
    pub async fn run(mut self, cancel: CancellationToken) {
        let mut tick = interval(Duration::from_millis(250));
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;

                _ = cancel.cancelled() => {
                    info!("debouncer shutting down; flushing {} pending entries", self.map.len());
                    break;
                }

                Some(cmd) = self.cmd_rx.recv() => {
                    match cmd {
                        DebouncerCmd::SetPaused(fid) => self.set_paused(fid),
                        DebouncerCmd::SetActive(fid) => self.set_active(fid),
                        DebouncerCmd::DrainFolder(fid) => self.drain_folder(fid),
                        DebouncerCmd::DrainDeletes(fid) => self.drain_deletes_for_folder(fid),
                        DebouncerCmd::HasPending(fid, reply) => {
                            let _ = reply.send(self.has_pending(fid));
                        }
                    }
                }

                Some(event) = self.event_rx.recv() => {
                    self.ingest(event).await;
                }

                _ = tick.tick() => {
                    self.flush_ready().await;
                }
            }
        }

        // Drain: flush everything immediately so we don't lose events on restart
        self.flush_all_now().await;
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    /// Ingest a single watch event.
    async fn ingest(&mut self, event: WatchEvent) {
        let WatchEvent { folder_id, rel_path, op } = event;

        if self.paused.contains(&folder_id) {
            // Folder is paused → write to dirty staging table
            let rel_str = rel_path.to_string_lossy();
            if let Err(e) =
                db::insert_dirty_while_paused(&self.pool, folder_id, &rel_str).await
            {
                error!(
                    folder_id = %folder_id,
                    path = %rel_str,
                    "insert_dirty_while_paused failed: {}", e
                );
            }
            return;
        }

        let key = (folder_id, rel_path);
        match self.map.get_mut(&key) {
            Some(entry) => {
                entry.update(op);
                debug!(folder_id = %folder_id, path = ?key.1, op = ?entry.latest_op,
                    "debounce entry refreshed");
            }
            None => {
                debug!(folder_id = %folder_id, path = ?key.1, op = ?op,
                    "debounce entry created");
                self.map.insert(key, DebounceEntry::new(op));
            }
        }
    }

    /// Flush entries whose quiet window or max-wait deadline has expired.
    async fn flush_ready(&mut self) {
        let now = Instant::now();

        // Collect keys to flush (avoid borrow conflict with self.map.remove)
        let to_flush: Vec<(Uuid, PathBuf, OpType)> = self
            .map
            .iter()
            .filter(|(_, e)| e.should_flush(now, self.quiet, self.max_wait))
            .map(|((fid, p), e)| (*fid, p.clone(), e.latest_op))
            .collect();

        for (folder_id, rel_path, op) in to_flush {
            let rel_str = rel_path.to_string_lossy();
            match db::enqueue_task(&self.pool, folder_id, &rel_str, op).await {
                Ok(task_id) => {
                    debug!(
                        folder_id = %folder_id,
                        path = %rel_str,
                        op = ?op,
                        task_id,
                        "flushed to transfer_queue"
                    );
                    self.map.remove(&(folder_id, rel_path));
                }
                Err(e) => {
                    // Leave the entry; retry on the next tick
                    warn!(
                        folder_id = %folder_id,
                        path = %rel_str,
                        "enqueue_task failed (will retry): {}", e
                    );
                }
            }
        }
    }

    /// Flush everything immediately without waiting for the quiet window.
    ///
    /// Used on shutdown so events survive in `transfer_queue` across restarts.
    async fn flush_all_now(&mut self) {
        let all: Vec<_> = self
            .map
            .drain()
            .map(|((fid, p), e)| (fid, p, e.latest_op))
            .collect();

        for (folder_id, rel_path, op) in all {
            let rel_str = rel_path.to_string_lossy();
            if let Err(e) = db::enqueue_task(&self.pool, folder_id, &rel_str, op).await {
                error!(
                    folder_id = %folder_id,
                    path = %rel_str,
                    "flush_all_now failed to enqueue: {}", e
                );
            }
        }
    }
}

// ─────────────────────────────────────────────
// Unit tests (in-memory, no real files)
// ─────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delete_wins_over_upload() {
        let mut entry = DebounceEntry::new(OpType::Upload);
        entry.update(OpType::Delete);
        assert_eq!(entry.latest_op, OpType::Delete);
    }

    #[test]
    fn upload_does_not_override_delete() {
        let mut entry = DebounceEntry::new(OpType::Delete);
        entry.update(OpType::Upload);
        assert_eq!(entry.latest_op, OpType::Delete);
    }

    #[test]
    fn quiet_window_triggers_flush() {
        let entry = DebounceEntry::new(OpType::Upload);
        // Simulate a very short quiet window that's already expired
        let now = Instant::now() + Duration::from_secs(100);
        let quiet = Duration::from_millis(3_000);
        let max_wait = Duration::from_millis(60_000);
        assert!(entry.should_flush(now, quiet, max_wait));
    }

    #[test]
    fn max_wait_triggers_flush_before_quiet() {
        let entry = DebounceEntry::new(OpType::Upload);
        // Far future: both windows expired
        let now = Instant::now() + Duration::from_secs(120);
        let quiet = Duration::from_millis(3_000);
        let max_wait = Duration::from_millis(60_000);
        assert!(entry.should_flush(now, quiet, max_wait));
    }

    #[test]
    fn fresh_entry_does_not_flush() {
        let entry = DebounceEntry::new(OpType::Upload);
        let now = Instant::now();
        let quiet = Duration::from_millis(3_000);
        let max_wait = Duration::from_millis(60_000);
        // Brand-new entry should not flush immediately
        assert!(!entry.should_flush(now, quiet, max_wait));
    }
}
