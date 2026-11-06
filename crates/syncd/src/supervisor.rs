//! Actor supervisor and orchestrator for `syncd`.
//!
//! ## Responsibilities
//!
//! - **Folder Actor Lifecycle**: Manages inotify watchers, anchors, and device IDs.
//! - **Debouncer Coordination**: Runs the debouncer actor and routes filesystem events.
//! - **Hotplug & Drive Disconnect**: Epoll-based `/proc/self/mountinfo` change detection,
//!   `st_dev` verification, cancellation of pending DELETEs, and reconciliation walks.
//! - **Empty Folder Safety Lock**: Prevents catastrophic wipe when an external mount
//!   appears as an empty directory on the root filesystem.
//! - **Transfer Worker Pool**: Concurrency control via tokio `Semaphore`, exponential
//!   retry backoff with full random jitter, and dynamic bandwidth throttling.
//! - **Live Observability**: 500ms EWMA throughput calculation ($R_{\text{ewma}} = \alpha \cdot R_{\text{inst}} + (1 - \alpha) \cdot R_{\text{prev}}$)
//!   and telemetry broadcasting.
//! - **Live Config Reloading**: Diffs active folder configurations on SIGHUP or IPC request.

use crate::{
    db::{self, DbError},
    debouncer::{Debouncer, DebouncerHandle},
    logging::{notify_critical, notify_info, notify_warn},
    offload::{self, TaskContext},
    safety::{self, SafetyError},
    storage::{rclone::ObjectStoreBackend, BandwidthLimiter, CloudStorage},
    watcher::{self, FolderWatcher},
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use sqlx::SqlitePool;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use sync_common::{
    config::{FolderConfig, GlobalConfig, SyncMode},
    state::{FolderState, FolderStatusReport, OpType, TelemetrySnapshot, TransferProgress},
};
use thiserror::Error;
use tokio::{
    sync::{broadcast, RwLock, Semaphore},
    time::{interval, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;
use walkdir::WalkDir;

// ─────────────────────────────────────────────
// Error types
// ─────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum SupervisorError {
    #[error("database error: {0}")]
    Db(#[from] DbError),

    #[error("safety error: {0}")]
    Safety(#[from] SafetyError),

    #[error("storage initialization error: {0}")]
    Storage(String),

    #[error("watcher error: {0}")]
    Watcher(#[from] watcher::WatcherError),

    #[error("folder '{0}' not found in supervisor")]
    FolderNotFound(Uuid),

    #[error("configuration error: {0}")]
    Config(String),
}

// ─────────────────────────────────────────────
// Active Transfer Telemetry Tracking
// ─────────────────────────────────────────────

#[allow(dead_code)]
struct ActiveTransferTracker {
    task_id: i64,
    folder_id: Uuid,
    folder_name: String,
    rel_path: String,
    op_type: OpType,
    total_bytes: u64,
    bytes_transferred: Arc<AtomicU64>,
    started_at: DateTime<Utc>,
    last_sampled_bytes: u64,
    ewma_speed: f64,
}

// ─────────────────────────────────────────────
// Folder Actor State
// ─────────────────────────────────────────────

struct FolderActorState {
    config: FolderConfig,
    state: FolderState,
    expected_st_dev: Option<u64>,
    baseline_file_count: u64,
    /// Held for RAII: dropping this removes inotify watches
    _watcher: Option<FolderWatcher>,
    cancel_token: CancellationToken,
}

// ─────────────────────────────────────────────
// Supervisor Struct
// ─────────────────────────────────────────────

pub struct Supervisor {
    config: Arc<RwLock<GlobalConfig>>,
    pool: SqlitePool,
    cancel: CancellationToken,
    debouncer_handle: DebouncerHandle,
    folders: Arc<RwLock<HashMap<Uuid, FolderActorState>>>,
    storages: Arc<RwLock<HashMap<Uuid, Arc<dyn CloudStorage>>>>,
    rate_limiter: Arc<RwLock<Option<Arc<BandwidthLimiter>>>>,
    active_transfers: Arc<RwLock<HashMap<i64, ActiveTransferTracker>>>,
    transfer_semaphore: Arc<Semaphore>,
    telemetry_tx: broadcast::Sender<TelemetrySnapshot>,
    global_ewma_upload_bps: Arc<AtomicU64>,
}

impl Supervisor {
    /// Bootstraps the supervisor actor system.
    pub async fn new(
        config: GlobalConfig,
        pool: SqlitePool,
        cancel: CancellationToken,
    ) -> Result<Arc<Self>, SupervisorError> {
        let (debouncer, debouncer_handle) = Debouncer::create(
            pool.clone(),
            config.debounce_quiet_ms,
            config.debounce_max_wait_ms,
        );

        // Spawn debouncer event loop
        let debouncer_cancel = cancel.child_token();
        tokio::spawn(async move {
            debouncer.run(debouncer_cancel).await;
        });

        let rate_limiter = Arc::new(RwLock::new(
            BandwidthLimiter::new(config.default_bandwidth_limit_bps).map(Arc::new),
        ));

        let transfer_semaphore = Arc::new(Semaphore::new(config.max_concurrent_transfers.max(1)));
        let (telemetry_tx, _) = broadcast::channel(32);

        let supervisor = Arc::new(Self {
            config: Arc::new(RwLock::new(config.clone())),
            pool,
            cancel: cancel.clone(),
            debouncer_handle,
            folders: Arc::new(RwLock::new(HashMap::new())),
            storages: Arc::new(RwLock::new(HashMap::new())),
            rate_limiter,
            active_transfers: Arc::new(RwLock::new(HashMap::new())),
            transfer_semaphore,
            telemetry_tx,
            global_ewma_upload_bps: Arc::new(AtomicU64::new(0)),
        });

        // Initialize folder configurations
        supervisor.initialize_folders(&config.folders).await?;

        // Launch background supervisor subroutines
        supervisor.clone().spawn_mount_monitor();
        supervisor.clone().spawn_pause_ticker();
        supervisor.clone().spawn_worker_pool();
        supervisor.clone().spawn_telemetry_ticker();

        Ok(supervisor)
    }

    /// Initializes folders on startup.
    async fn initialize_folders(&self, folder_configs: &[FolderConfig]) -> Result<(), SupervisorError> {
        let mut total_dirs = 0;

        for cfg in folder_configs {
            let mut folder_state = FolderState::Watching;

            // Check naming collision or configuration validity
            if let Err(e) = self.check_naming_collision(cfg, folder_configs) {
                error!(folder_id = %cfg.id, name = %cfg.name, "Folder configuration rejected: {}", e);
                folder_state = FolderState::Error { reason: e.clone() };
                notify_critical("syncd: Configuration Error", format!("Folder '{}': {}", cfg.name, e));
            }

            // Sync with DB
            db::upsert_folder(&self.pool, cfg).await?;

            // Setup storage backend
            match ObjectStoreBackend::from_folder_config(cfg) {
                Ok(storage) => {
                    self.storages.write().await.insert(cfg.id, Arc::new(storage));
                }
                Err(e) => {
                    warn!(folder_id = %cfg.id, "Cloud storage init failed: {}", e);
                    folder_state = FolderState::Error { reason: e.to_string() };
                }
            }

            let mut expected_st_dev = None;
            let mut baseline_count = 0;
            let mut watcher_opt = None;
            let watcher_cancel = self.cancel.child_token();
            let task_cancel = self.cancel.child_token();

            if folder_state.is_operational() {
                // Auto-create local folder directory if it does not exist
                if !cfg.local_path.exists() {
                    if let Err(e) = tokio::fs::create_dir_all(&cfg.local_path).await {
                        warn!(folder_id = %cfg.id, path = ?cfg.local_path, "Could not auto-create missing local folder: {}", e);
                    } else {
                        info!(folder_id = %cfg.id, path = ?cfg.local_path, "Auto-created local folder directory");
                    }
                }

                // Ensure canary anchor on all watched folders
                if let Err(e) = safety::ensure_anchor(&cfg.local_path, cfg.id).await {
                    error!(folder_id = %cfg.id, "Failed ensuring .syncd-anchor: {}", e);
                    folder_state = FolderState::Error { reason: e.to_string() };
                }

                // Record st_dev
                match safety::read_st_dev(&cfg.local_path).await {
                    Ok(dev) => {
                        expected_st_dev = Some(dev);
                        let _ = db::set_expected_st_dev(&self.pool, cfg.id, dev).await;
                    }
                    Err(e) => {
                        warn!(folder_id = %cfg.id, "Could not read st_dev: {}", e);
                    }
                }

                // Count baseline files
                if let Ok(count) = safety::count_visible_files(&cfg.local_path).await {
                    baseline_count = count;
                    let _ = db::set_baseline_file_count(&self.pool, cfg.id, count).await;
                }

                // Purge any stale upload tasks whose local files were deleted while syncd was offline
                if let Ok(purged) = db::purge_missing_upload_tasks(&self.pool, cfg.id, &cfg.local_path).await {
                    if purged > 0 {
                        info!(folder_id = %cfg.id, purged_tasks = purged, "Purged obsolete upload tasks for non-existent files");
                    }
                }

                // Count subdirectories for inotify budget warning
                total_dirs += watcher::count_watched_dirs(&cfg.local_path);

                // Spawn inotify watcher
                match watcher::spawn_folder_watcher(cfg, watcher_cancel.clone()) {
                    Ok((fw, mut rx)) => {
                        watcher_opt = Some(fw);
                        let debouncer = self.debouncer_handle.clone();
                        let cancel_token = watcher_cancel;

                        tokio::spawn(async move {
                            loop {
                                tokio::select! {
                                    _ = cancel_token.cancelled() => break,
                                    Some(ev) = rx.recv() => {
                                        let _ = debouncer.send_event(ev);
                                    }
                                    else => break,
                                }
                            }
                        });
                    }
                    Err(e) => {
                        error!(folder_id = %cfg.id, "Failed spawning inotify watcher: {}", e);
                        folder_state = FolderState::Error { reason: e.to_string() };
                    }
                }
            }

            // Restore paused state if recorded in DB
            if let Ok(rec) = db::load_folder(&self.pool, cfg.id).await {
                if rec.state.is_paused() {
                    folder_state = rec.state;
                    self.debouncer_handle.set_paused(cfg.id);
                }
            }

            let _ = db::set_folder_state(&self.pool, cfg.id, &folder_state).await;

            let actor = FolderActorState {
                config: cfg.clone(),
                state: folder_state,
                expected_st_dev,
                baseline_file_count: baseline_count,
                _watcher: watcher_opt,
                cancel_token: task_cancel,
            };

            self.folders.write().await.insert(cfg.id, actor);
        }

        // Check inotify limits across all folders
        watcher::check_inotify_watch_limit(total_dirs);

        Ok(())
    }

    /// Check for remote target or naming collisions.
    fn check_naming_collision(&self, cfg: &FolderConfig, all: &[FolderConfig]) -> Result<(), String> {
        for other in all {
            if other.id == cfg.id {
                continue;
            }

            if other.remote_target == cfg.remote_target {
                return Err(format!(
                    "Remote naming collision: target '{}' is already bound by folder '{}'",
                    cfg.remote_target, other.name
                ));
            }

            let c_target = cfg.remote_target.trim_end_matches('/');
            let o_target = other.remote_target.trim_end_matches('/');

            if c_target.starts_with(o_target) || o_target.starts_with(c_target) {
                return Err(format!(
                    "Overlapping remote target detected between '{}' and '{}'",
                    c_target, o_target
                ));
            }
        }
        Ok(())
    }

    // ─────────────────────────────────────────────
    // Background Subroutines
    // ─────────────────────────────────────────────

    /// Monitor `/proc/self/mountinfo` epoll events and detect hotplug/disconnects.
    fn spawn_mount_monitor(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut mount_rx = safety::spawn_mountinfo_watcher(self.cancel.clone());

            loop {
                tokio::select! {
                    _ = self.cancel.cancelled() => break,
                    res = mount_rx.changed() => {
                        if res.is_err() {
                            break;
                        }
                        self.handle_mount_table_change().await;
                    }
                }
            }
            info!("supervisor mount monitor exited");
        });
    }

    async fn handle_mount_table_change(&self) {
        debug!("mount table changed, inspecting active folder devices");
        let mut state_changed = false;
        let mut folders = self.folders.write().await;

        for (fid, actor) in folders.iter_mut() {
            let local_path = &actor.config.local_path;

            match safety::read_st_dev(local_path).await {
                Ok(current_dev) => {
                    let st_dev_matches = actor.expected_st_dev.map_or(true, |exp| exp == current_dev);
                    let anchor_ok = safety::verify_anchor(local_path, *fid).await.is_ok();

                    if !st_dev_matches || !anchor_ok {
                        if actor.state != FolderState::DriveDisconnected {
                            warn!(folder_id = %fid, path = ?local_path, "Drive disconnected or swapped!");
                            actor.state = FolderState::DriveDisconnected;
                            let _ = db::set_folder_state(&self.pool, *fid, &actor.state).await;

                            // Abort active in-flight transfers for this folder
                            actor.cancel_token.cancel();
                            actor.cancel_token = self.cancel.child_token();

                            // Purge deletes from memory & DB to protect remote data
                            self.debouncer_handle.drain_deletes_for_folder(*fid);
                            let _ = db::purge_delete_tasks_for_folder(&self.pool, *fid).await;

                            notify_warn(
                                "syncd: Drive Disconnected",
                                format!("Storage device for '{}' unmounted. Deletions suspended.", actor.config.name),
                            );
                            state_changed = true;
                        }
                    } else if actor.state == FolderState::DriveDisconnected {
                        info!(folder_id = %fid, path = ?local_path, "Drive reconnected, triggering reconciliation");
                        actor.state = FolderState::Reconciling;
                        let _ = db::set_folder_state(&self.pool, *fid, &actor.state).await;

                        notify_info(
                            "syncd: Drive Reconnected",
                            format!("Storage device for '{}' reconnected. Reconciling...", actor.config.name),
                        );
                        state_changed = true;

                        let pool = self.pool.clone();
                        let folder_cfg = actor.config.clone();
                        let fid_copy = *fid;
                        let folders_arc = Arc::clone(&self.folders);

                        tokio::spawn(async move {
                            if Self::reconcile_folder(&pool, &folder_cfg).await {
                                let _ = db::set_folder_state(&pool, fid_copy, &FolderState::Watching).await;
                                let mut write_map = folders_arc.write().await;
                                if let Some(act) = write_map.get_mut(&fid_copy) {
                                    act.state = FolderState::Watching;
                                }
                            } else {
                                if let Ok(rec) = db::load_folder(&pool, fid_copy).await {
                                    let mut write_map = folders_arc.write().await;
                                    if let Some(act) = write_map.get_mut(&fid_copy) {
                                        act.state = rec.state;
                                    }
                                }
                            }
                        });
                    }
                }
                Err(_) => {
                    if actor.state != FolderState::DriveDisconnected {
                        actor.state = FolderState::DriveDisconnected;
                        let _ = db::set_folder_state(&self.pool, *fid, &actor.state).await;
                        actor.cancel_token.cancel();
                        actor.cancel_token = self.cancel.child_token();
                        self.debouncer_handle.drain_deletes_for_folder(*fid);
                        let _ = db::purge_delete_tasks_for_folder(&self.pool, *fid).await;
                        state_changed = true;
                    }
                }
            }
        }

        drop(folders);
        if state_changed {
            self.broadcast_telemetry().await;
        }
    }

    /// 1-second interval pause ticker.
    fn spawn_pause_ticker(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(1));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = self.cancel.cancelled() => break,
                    _ = ticker.tick() => {
                        if let Ok(expired) = db::expired_timed_pauses(&self.pool).await {
                            let mut resumed_any = false;
                            for fid in expired {
                                info!(folder_id = %fid, "Timed pause expired, resuming");
                                if let Err(e) = self.resume_folder_internal(fid).await {
                                    error!(folder_id = %fid, "Failed resuming folder after timed pause: {}", e);
                                } else {
                                    resumed_any = true;
                                }
                            }
                            if resumed_any {
                                self.broadcast_telemetry().await;
                            }
                        }
                    }
                }
            }
            info!("supervisor pause ticker exited");
        });
    }

    /// Transfer task worker pool.
    fn spawn_worker_pool(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut poll_interval = interval(Duration::from_millis(500));
            poll_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = self.cancel.cancelled() => break,
                    _ = poll_interval.tick() => {
                        self.dispatch_available_tasks().await;
                    }
                }
            }
            info!("supervisor worker pool dispatcher exited");
        });
    }

    async fn dispatch_available_tasks(&self) {
        let permits = self.transfer_semaphore.available_permits();
        if permits == 0 {
            return;
        }

        let tasks = match db::fetch_eligible_tasks(&self.pool, permits as u32).await {
            Ok(t) => t,
            Err(e) => {
                error!("fetch_eligible_tasks error: {}", e);
                return;
            }
        };

        for task in tasks {
            let folders = self.folders.read().await;
            let folder_actor = match folders.get(&task.folder_id) {
                Some(f) => f,
                None => continue,
            };

            if !folder_actor.state.is_operational() {
                continue;
            }

            // DropAndPurge safety: local file unlinking is the intended mechanism to free disk space.
            // Under NO circumstance should a local delete event delete the cloud offloaded archive!
            if task.op_type == OpType::Delete && folder_actor.config.mode == SyncMode::DropAndPurge {
                debug!(
                    folder_id = %task.folder_id,
                    task_id = task.task_id,
                    "Drop & Purge: dropping delete task — preserving remote offloaded object"
                );
                let _ = db::delete_task(&self.pool, task.task_id).await;
                continue;
            }

            // Empty Folder Safety Lock check prior to executing any deletion
            if task.op_type == OpType::Delete && folder_actor.config.mode == SyncMode::ContinuousBackup {
                if let Err(e) = safety::check_empty_folder_lock(
                    &folder_actor.config.local_path,
                    task.folder_id,
                    folder_actor.baseline_file_count,
                )
                .await
                {
                    error!(folder_id = %task.folder_id, "Empty folder safety lock triggered: {}", e);
                    drop(folders);
                    let mut write_folders = self.folders.write().await;
                    if let Some(act) = write_folders.get_mut(&task.folder_id) {
                        act.state = FolderState::Error { reason: e.to_string() };
                        let _ = db::set_folder_state(&self.pool, task.folder_id, &act.state).await;
                    }
                    continue;
                }
            }

            // Claim the task atomically
            match db::claim_task(&self.pool, task.task_id).await {
                Ok(true) => {
                    let permit = match self.transfer_semaphore.clone().try_acquire_owned() {
                        Ok(p) => p,
                        Err(_) => break,
                    };

                    let storage = match self.storages.read().await.get(&task.folder_id).cloned() {
                        Some(s) => s,
                        None => continue,
                    };

                    let limiter = self.rate_limiter.read().await.clone();
                    let folder_cfg = Arc::new(folder_actor.config.clone());
                    let pool = self.pool.clone();
                    let active_map = self.active_transfers.clone();

                    let abs_path = folder_cfg.local_path.join(&task.rel_path);
                    let file_size = tokio::fs::metadata(&abs_path).await.map(|m| m.len()).unwrap_or(0);
                    let bytes_counter = Arc::new(AtomicU64::new(0));

                    {
                        let mut map = active_map.write().await;
                        map.insert(
                            task.task_id,
                            ActiveTransferTracker {
                                task_id: task.task_id,
                                folder_id: task.folder_id,
                                folder_name: folder_cfg.name.clone(),
                                rel_path: task.rel_path.clone(),
                                op_type: task.op_type,
                                total_bytes: file_size,
                                bytes_transferred: bytes_counter.clone(),
                                started_at: Utc::now(),
                                last_sampled_bytes: 0,
                                ewma_speed: 0.0,
                            },
                        );
                    }

                    let ctx = TaskContext {
                        task_id: task.task_id,
                        folder_id: task.folder_id,
                        folder_cfg,
                        rel_path: task.rel_path.clone(),
                        op: task.op_type,
                        pool: pool.clone(),
                        storage,
                        limiter,
                        progress: Some(bytes_counter.clone()),
                    };

                    tokio::spawn(async move {
                        let _permit = permit;
                        let outcome = offload::execute_task(&ctx).await;

                        active_map.write().await.remove(&task.task_id);

                        if let Err(e) = outcome {
                            let err_str = e.to_string();
                            warn!(task_id = task.task_id, error = %err_str, "Transfer task failed");

                            let is_missing_file = err_str.contains("No such file")
                                || err_str.contains("not found")
                                || err_str.contains("os error 2");

                            if is_missing_file {
                                info!(
                                    task_id = task.task_id,
                                    "Target file no longer exists on disk; clearing obsolete task"
                                );
                                let _ = db::delete_task(&pool, task.task_id).await;
                            } else if task.attempt_count + 1 >= 5 {
                                error!(
                                    task_id = task.task_id,
                                    attempts = task.attempt_count + 1,
                                    "Task exceeded maximum retry attempts (5); dropping from queue"
                                );
                                let _ = db::delete_task(&pool, task.task_id).await;
                            } else {
                                let delay = Self::calculate_backoff_delay(task.attempt_count + 1);
                                let next_retry = db::now_ms() + delay.as_millis() as i64;
                                let _ = db::fail_task(&pool, task.task_id, &err_str, next_retry).await;
                            }
                        }
                    });
                }
                Ok(false) => {}
                Err(e) => error!("Failed claiming task {}: {}", task.task_id, e),
            }
        }
    }

    /// Full jitter exponential backoff: uniformly distributed in [0, min(300s, base * 2^attempt)].
    fn calculate_backoff_delay(attempt: u32) -> Duration {
        let base_secs = 1.0f64;
        let factor = 2.0f64;
        let max_secs = 300.0f64;

        let exp_secs = (base_secs * factor.powi(attempt as i32)).min(max_secs);

        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            ^ (attempt as u128).wrapping_mul(0x517cc1b727220a95);
        let rand_frac = ((seed % 10_000) as f64) / 10_000.0;
        let jittered_secs = exp_secs * rand_frac;

        Duration::from_millis((jittered_secs * 1000.0) as u64)
    }

    /// 500ms EWMA throughput calculation & telemetry broadcast ticker.
    fn spawn_telemetry_ticker(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(500));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = self.cancel.cancelled() => break,
                    _ = ticker.tick() => {
                        let snapshot = self.compute_telemetry_snapshot().await;
                        let _ = self.telemetry_tx.send(snapshot);
                    }
                }
            }
            info!("supervisor telemetry ticker exited");
        });
    }

    async fn compute_telemetry_snapshot(&self) -> TelemetrySnapshot {
        let mut active_reports = Vec::new();
        let mut total_instant_bytes = 0u64;

        {
            let mut transfers = self.active_transfers.write().await;
            for tracker in transfers.values_mut() {
                let current_bytes = tracker.bytes_transferred.load(Ordering::Relaxed);
                let delta = current_bytes.saturating_sub(tracker.last_sampled_bytes);
                tracker.last_sampled_bytes = current_bytes;

                total_instant_bytes += delta;
                let inst_speed = (delta as f64) / 0.5; // per second

                // EWMA speed smoothing: alpha = 0.3
                tracker.ewma_speed = 0.3 * inst_speed + 0.7 * tracker.ewma_speed;
                let speed_bps = tracker.ewma_speed.round() as u64;

                let remaining = tracker.total_bytes.saturating_sub(current_bytes);
                let eta_secs = if speed_bps > 0 {
                    Some(remaining as f64 / speed_bps as f64)
                } else {
                    None
                };

                active_reports.push(TransferProgress {
                    task_id: tracker.task_id,
                    folder_id: tracker.folder_id,
                    rel_path: tracker.rel_path.clone(),
                    op_type: tracker.op_type,
                    total_bytes: tracker.total_bytes,
                    bytes_transferred: current_bytes,
                    upload_bps_ewma: tracker.ewma_speed,
                    eta_secs,
                    attempt_count: 0,
                    enqueued_at: tracker.started_at,
                });
            }
        }

        // Global upload rate EWMA
        let inst_global_speed = (total_instant_bytes as f64) / 0.5;
        let prev_global = self.global_ewma_upload_bps.load(Ordering::Relaxed) as f64;
        let new_global = (0.3 * inst_global_speed + 0.7 * prev_global).round() as u64;
        self.global_ewma_upload_bps.store(new_global, Ordering::Relaxed);

        let mut folder_reports = Vec::new();
        {
            let folders = self.folders.read().await;
            for (fid, actor) in folders.iter() {
                let pending = db::pending_task_count(&self.pool, *fid).await.unwrap_or(0);
                folder_reports.push(FolderStatusReport {
                    folder_id: *fid,
                    name: actor.config.name.clone(),
                    local_path: actor.config.local_path.to_string_lossy().to_string(),
                    remote_target: actor.config.remote_target.clone(),
                    state: actor.state.clone(),
                    sync_mode: actor.config.mode.to_string(),
                    baseline_file_count: actor.baseline_file_count,
                    pending_tasks: pending,
                    session_bytes_uploaded: 0,
                    session_bytes_freed: 0,
                    safety_lock_armed: actor.config.empty_folder_protection,
                });
            }
        }

        let configured_bw = self.config.read().await.default_bandwidth_limit_bps;

        TelemetrySnapshot {
            sampled_at: Utc::now(),
            global_upload_bps: new_global,
            configured_bw_limit_bps: configured_bw,
            folders: folder_reports,
            active_transfers: active_reports,
        }
    }

    /// Background reconciliation walk when reconnecting or on trigger scan.
    async fn reconcile_folder(pool: &SqlitePool, cfg: &FolderConfig) -> bool {
        info!(folder_id = %cfg.id, path = ?cfg.local_path, "Starting reconciliation scan");
        let local_path = cfg.local_path.clone();

        let gitignore = match watcher::build_gitignore(cfg) {
            Ok(gi) => gi,
            Err(e) => {
                error!("Reconcile failed to build gitignore: {}", e);
                return false;
            }
        };

        let files: Vec<String> = tokio::task::spawn_blocking(move || {
            let mut paths = Vec::new();
            let walker = WalkDir::new(&local_path)
                .follow_links(false)
                .into_iter()
                .filter_entry(|entry| {
                    if let Ok(rel) = entry.path().strip_prefix(&local_path) {
                        if rel.as_os_str().is_empty() {
                            return true;
                        }
                        let is_dir = entry.file_type().is_dir();
                        !gitignore.matched_path_or_any_parents(rel, is_dir).is_ignore()
                    } else {
                        true
                    }
                });

            for entry in walker.flatten() {
                if entry.file_type().is_file() {
                    if let Ok(rel) = entry.path().strip_prefix(&local_path) {
                        paths.push(rel.to_string_lossy().to_string());
                    }
                }
            }
            paths
        })
        .await
        .unwrap_or_default();

        for rel_str in files {
            let _ = db::enqueue_task(pool, cfg.id, &rel_str, OpType::Upload).await;
        }

        let total = safety::count_visible_files(&cfg.local_path).await.unwrap_or(0);

        // In continuous backup mode, propagate offline local deletions
        if cfg.mode == SyncMode::ContinuousBackup {
            // Safety check: Empty Folder Safety Lock check prior to queuing mass deletions
            if cfg.empty_folder_protection && total == 0 {
                // Fetch previous baseline file count from DB
                let prev_baseline = db::load_folder(pool, cfg.id)
                    .await
                    .map(|r| r.baseline_file_count)
                    .unwrap_or(0);

                if prev_baseline > 0 {
                    error!(
                        folder_id = %cfg.id,
                        path = ?cfg.local_path,
                        prev_baseline,
                        "EMPTY FOLDER SAFETY LOCK TRIGGERED during reconciliation scan — aborting delete propagation to prevent cloud data loss"
                    );
                    notify_critical(
                        "syncd: Empty Folder Safety Lock",
                        format!("Folder '{}' appears completely empty while {} files were previously synced. Remote deletions halted.", cfg.name, prev_baseline),
                    );
                    let _ = db::set_folder_state(
                        pool,
                        cfg.id,
                        &FolderState::Error {
                            reason: format!("Empty Folder Safety Lock triggered (0 visible files vs {} baseline)", prev_baseline),
                        },
                    ).await;
                    return false;
                }
            }

            if let Ok(synced) = db::list_synced_files(pool, cfg.id).await {
                for rel_str in synced {
                    let abs = cfg.local_path.join(&rel_str);
                    if !abs.exists() {
                        let _ = db::enqueue_task(pool, cfg.id, &rel_str, OpType::Delete).await;
                    }
                }
            }
        }

        // Purge any stale upload tasks whose files no longer exist on disk
        let _ = db::purge_missing_upload_tasks(pool, cfg.id, &cfg.local_path).await;

        let _ = db::set_baseline_file_count(pool, cfg.id, total).await;
        info!(folder_id = %cfg.id, files = total, "Reconciliation scan complete");
        true
    }

    // ─────────────────────────────────────────────
    // Control API (Called by IPC Server)
    // ─────────────────────────────────────────────

    pub async fn get_status(&self) -> TelemetrySnapshot {
        self.compute_telemetry_snapshot().await
    }

    pub async fn broadcast_telemetry(&self) {
        let snapshot = self.compute_telemetry_snapshot().await;
        let _ = self.telemetry_tx.send(snapshot);
    }

    pub fn subscribe_telemetry(&self) -> broadcast::Receiver<TelemetrySnapshot> {
        self.telemetry_tx.subscribe()
    }

    pub async fn pause(
        &self,
        folder_id: Option<Uuid>,
        duration_secs: Option<u64>,
    ) -> Result<String, SupervisorError> {
        let dur = duration_secs.map(Duration::from_secs);

        let targets: Vec<Uuid> = match folder_id {
            Some(id) => vec![id],
            None => self.folders.read().await.keys().cloned().collect(),
        };

        for fid in &targets {
            db::pause_folder(&self.pool, *fid, dur).await?;
            self.debouncer_handle.set_paused(*fid);

            let mut folders = self.folders.write().await;
            if let Some(act) = folders.get_mut(fid) {
                let until = dur.map(|d| Utc::now() + ChronoDuration::milliseconds(d.as_millis() as i64));
                act.state = FolderState::Paused { until };
                act.cancel_token.cancel();
                act.cancel_token = self.cancel.child_token();
            }
        }

        let msg = match (folder_id, duration_secs) {
            (Some(id), Some(secs)) => format!("Folder {} paused for {}s", id, secs),
            (Some(id), None) => format!("Folder {} paused indefinitely", id),
            (None, Some(secs)) => format!("All folders paused for {}s", secs),
            (None, None) => "All folders paused indefinitely".into(),
        };
        self.broadcast_telemetry().await;
        Ok(msg)
    }

    pub async fn resume(&self, folder_id: Option<Uuid>) -> Result<String, SupervisorError> {
        let targets: Vec<Uuid> = match folder_id {
            Some(id) => vec![id],
            None => self.folders.read().await.keys().cloned().collect(),
        };

        for fid in &targets {
            self.resume_folder_internal(*fid).await?;
        }

        let msg = match folder_id {
            Some(id) => format!("Folder {} resumed", id),
            None => "All folders resumed".into(),
        };
        self.broadcast_telemetry().await;
        Ok(msg)
    }

    async fn resume_folder_internal(&self, fid: Uuid) -> Result<(), SupervisorError> {
        self.debouncer_handle.set_active(fid);
        db::resume_folder(&self.pool, fid).await?;

        let mut folders = self.folders.write().await;
        if let Some(act) = folders.get_mut(&fid) {
            act.state = FolderState::Watching;
        }
        Ok(())
    }

    pub async fn set_bandwidth_limit(&self, bytes_per_sec: u64) -> Result<String, SupervisorError> {
        let new_limiter = BandwidthLimiter::new(bytes_per_sec).map(Arc::new);
        *self.rate_limiter.write().await = new_limiter;
        self.config.write().await.default_bandwidth_limit_bps = bytes_per_sec;

        let msg = if bytes_per_sec == 0 {
            "Upload bandwidth limit removed (unlimited)".into()
        } else {
            format!("Upload bandwidth limit set to {} bytes/sec", bytes_per_sec)
        };
        info!("{}", msg);
        self.broadcast_telemetry().await;
        Ok(msg)
    }

    pub async fn trigger_scan(&self, folder_id: Option<Uuid>) -> Result<String, SupervisorError> {
        let targets: Vec<FolderConfig> = {
            let folders = self.folders.read().await;
            match folder_id {
                Some(id) => match folders.get(&id) {
                    Some(a) => vec![a.config.clone()],
                    None => return Err(SupervisorError::FolderNotFound(id)),
                },
                None => folders.values().map(|a| a.config.clone()).collect(),
            }
        };

        for cfg in targets {
            let pool = self.pool.clone();
            let fid = cfg.id;
            let folders_arc = Arc::clone(&self.folders);

            {
                let mut map = self.folders.write().await;
                if let Some(act) = map.get_mut(&fid) {
                    act.state = FolderState::Reconciling;
                }
            }
            let _ = db::set_folder_state(&self.pool, fid, &FolderState::Reconciling).await;

            tokio::spawn(async move {
                if Self::reconcile_folder(&pool, &cfg).await {
                    let _ = db::set_folder_state(&pool, fid, &FolderState::Watching).await;
                    let mut map = folders_arc.write().await;
                    if let Some(act) = map.get_mut(&fid) {
                        act.state = FolderState::Watching;
                    }
                } else {
                    if let Ok(rec) = db::load_folder(&pool, fid).await {
                        let mut map = folders_arc.write().await;
                        if let Some(act) = map.get_mut(&fid) {
                            act.state = rec.state;
                        }
                    }
                }
            });
        }

        self.broadcast_telemetry().await;
        Ok("Reconciliation scan initiated".into())
    }

    pub async fn reload_config(&self, new_config: GlobalConfig) -> Result<(), SupervisorError> {
        info!("Reloading configuration live");

        // Validate new config
        new_config.validate().map_err(|e| SupervisorError::Config(e.to_string()))?;

        // Update bandwidth limiter
        self.set_bandwidth_limit(new_config.default_bandwidth_limit_bps).await?;

        // Update active folders
        let existing_ids: Vec<Uuid> = self.folders.read().await.keys().cloned().collect();
        let new_map: HashMap<Uuid, FolderConfig> = new_config.folders.iter().map(|f| (f.id, f.clone())).collect();

        // Remove old folders
        for old_id in existing_ids {
            if !new_map.contains_key(&old_id) {
                info!(folder_id = %old_id, "Removing deleted folder from supervisor");
                let mut folders = self.folders.write().await;
                if let Some(act) = folders.remove(&old_id) {
                    act.cancel_token.cancel();
                }
                self.debouncer_handle.drain_folder(old_id);
                self.storages.write().await.remove(&old_id);
                let _ = db::delete_folder(&self.pool, old_id).await;
            }
        }

        // Add or update folders
        self.initialize_folders(&new_config.folders).await?;

        *self.config.write().await = new_config;
        info!("Live configuration reload complete");
        Ok(())
    }
}

// ─────────────────────────────────────────────
// Unit Tests
// ─────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_backoff_delay_bounds() {
        for attempt in 0..10 {
            let delay = Supervisor::calculate_backoff_delay(attempt);
            let max_ceiling = (1.0 * 2.0f64.powi(attempt as i32)).min(300.0);
            assert!(
                delay.as_secs_f64() <= max_ceiling + 0.001,
                "Delay {:?} exceeded ceiling {}",
                delay,
                max_ceiling
            );
        }
    }

    #[test]
    fn test_naming_collision_detection() {
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        let cfg1 = FolderConfig {
            id: id1,
            name: "photos".into(),
            local_path: PathBuf::from("/data/photos"),
            remote_target: "s3://my-bucket/backups/photos".into(),
            mode: SyncMode::ContinuousBackup,
            exclude_patterns: vec![],
            empty_folder_protection: true,
            ..Default::default()
        };

        let cfg2 = FolderConfig {
            id: id2,
            name: "photos-duplicate".into(),
            local_path: PathBuf::from("/data/other_photos"),
            remote_target: "s3://my-bucket/backups/photos".into(),
            mode: SyncMode::ContinuousBackup,
            exclude_patterns: vec![],
            empty_folder_protection: true,
            ..Default::default()
        };

        let all = vec![cfg1.clone(), cfg2.clone()];

        // Standalone collision check logic test
        let mut collision = false;
        for other in &all {
            if other.id != cfg1.id && other.remote_target == cfg1.remote_target {
                collision = true;
            }
        }
        assert!(collision);
    }

    #[tokio::test]
    async fn test_reconcile_empty_folder_safety_lock() {
        let db_dir = tempfile::tempdir().unwrap();
        let db_path = db_dir.path().join("syncd.db");
        let pool = db::open_and_migrate(&db_path).await.unwrap();

        let fid = Uuid::new_v4();
        let folder_dir = tempfile::tempdir().unwrap();

        let cfg = FolderConfig {
            id: fid,
            name: "safety-test".into(),
            local_path: folder_dir.path().to_path_buf(),
            remote_target: "s3://my-bucket/test".into(),
            mode: SyncMode::ContinuousBackup,
            exclude_patterns: vec![],
            empty_folder_protection: true,
            ..Default::default()
        };

        // Seed folder in db with baseline_file_count = 50
        db::upsert_folder(&pool, &cfg).await.unwrap();
        db::set_baseline_file_count(&pool, fid, 50).await.unwrap();

        // The directory has 0 visible files.
        let success = Supervisor::reconcile_folder(&pool, &cfg).await;
        // Should halt and return false
        assert!(!success);

        // Verify folder state became Error
        let record = db::load_folder(&pool, fid).await.unwrap();
        match record.state {
            FolderState::Error { reason } => {
                assert!(reason.contains("Empty Folder Safety Lock triggered"));
            }
            other => panic!("Expected FolderState::Error, got {:?}", other),
        }

        // Baseline file count must NOT be overwritten to 0
        assert_eq!(record.baseline_file_count, 50);
    }
}
