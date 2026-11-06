//! Recursive inotify watcher with a gitignore-style exclude engine.
//!
//! ## Event selection rationale
//!
//! We deliberately **skip** raw `Modify(Data)` events because they fire for
//! every `write(2)` syscall and would trigger uploads of half-written files.
//! Instead we act on:
//!
//! | inotify event       | `notify` kind                                        | Action  |
//! |---------------------|------------------------------------------------------|---------|
//! | `IN_CLOSE_WRITE`    | `Access(Close(Write))`                               | Upload  |
//! | `IN_MOVED_FROM/TO`  | `Modify(Name(_))`                                    | Upload  |
//! | `IN_DELETE*`        | `Remove(_)`                                          | Delete  |
//! | `IN_CREATE` (dir)   | `Create(Folder)`                                     | Synthesise events |
//!
//! ## New-directory race condition
//!
//! When a directory is created, any files inside it that existed before
//! `inotify_add_watch` returned are invisible to the kernel.  We close this
//! gap by walking the new directory immediately with `walkdir` and synthesising
//! `Upload` events for every file found.

use ignore::gitignore::{Gitignore, GitignoreBuilder};
use notify::{
    event::{AccessKind, AccessMode, CreateKind, ModifyKind},
    Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher,
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use sync_common::{config::FolderConfig, state::OpType};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};
use uuid::Uuid;
use walkdir::WalkDir;

// ─────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────

const INOTIFY_LIMIT_PATH: &str = "/proc/sys/fs/inotify/max_user_watches";
/// Fraction of the kernel watch limit at which we emit a warning.
const WATCH_LIMIT_WARN_FRACTION: f64 = 0.80;

// ─────────────────────────────────────────────
// Error type
// ─────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum WatcherError {
    #[error("gitignore build failed for folder {folder_id}: {source}")]
    GitignoreBuild {
        folder_id: Uuid,
        #[source]
        source: ignore::Error,
    },

    #[error("notify error for folder {folder_id}: {source}")]
    Notify {
        folder_id: Uuid,
        #[source]
        source: notify::Error,
    },

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

// ─────────────────────────────────────────────
// Watch event (output of this module)
// ─────────────────────────────────────────────

/// A pre-filtered, relative-path event ready for the debouncer.
#[derive(Debug)]
pub struct WatchEvent {
    /// Which folder generated this event.
    pub folder_id: Uuid,
    /// Path relative to `FolderConfig::local_path` using `/` as separator.
    pub rel_path: PathBuf,
    /// The operation to perform.
    pub op: OpType,
}

// ─────────────────────────────────────────────
// FolderWatcher (RAII handle)
// ─────────────────────────────────────────────

/// Holds the active `notify` watcher for one folder.
///
/// Dropping this struct removes all inotify watches for the folder and
/// signals the bridge thread to exit (by dropping the sync sender).
pub struct FolderWatcher {
    pub folder_id: Uuid,
    pub local_path: PathBuf,
    /// Shared with the bridge thread so it can filter new events after
    /// a live config reload.
    pub gitignore: Arc<Gitignore>,
    /// Kept alive so inotify watches remain registered.
    _watcher: RecommendedWatcher,
    /// Dropping this stops the bridge thread.
    _bridge_stop: std::sync::mpsc::SyncSender<()>,
}

impl std::fmt::Debug for FolderWatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FolderWatcher")
            .field("folder_id", &self.folder_id)
            .field("local_path", &self.local_path)
            .finish_non_exhaustive()
    }
}

// ─────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────

/// Read `/proc/sys/fs/inotify/max_user_watches` and warn (+ desktop alert)
/// if `estimated_dirs` exceeds 80% of the limit.
///
/// Call this once on startup after walking all watched folder trees.
pub fn check_inotify_watch_limit(estimated_dirs: usize) {
    let content = match std::fs::read_to_string(INOTIFY_LIMIT_PATH) {
        Ok(c) => c,
        Err(e) => {
            warn!("cannot read {}: {}", INOTIFY_LIMIT_PATH, e);
            return;
        }
    };

    let limit: usize = match content.trim().parse() {
        Ok(n) => n,
        Err(e) => {
            warn!("cannot parse {}: {}", INOTIFY_LIMIT_PATH, e);
            return;
        }
    };

    let fraction = estimated_dirs as f64 / limit as f64;
    if fraction >= WATCH_LIMIT_WARN_FRACTION {
        warn!(
            estimated_dirs,
            limit,
            usage_pct = format!("{:.1}%", fraction * 100.0),
            "inotify watch limit approaching — \
             consider: sudo sysctl fs.inotify.max_user_watches=524288"
        );
        tokio::task::spawn_blocking(move || {
            let _ = notify_rust::Notification::new()
                .summary("syncd: inotify limit warning")
                .body(&format!(
                    "Using {:.0}% of {} inotify watches ({} dirs).\n\
                     Raise: sudo sysctl fs.inotify.max_user_watches=524288",
                    fraction * 100.0,
                    limit,
                    estimated_dirs
                ))
                .timeout(notify_rust::Timeout::Milliseconds(8_000))
                .show();
        });
    } else {
        debug!(
            estimated_dirs,
            limit,
            usage_pct = format!("{:.1}%", fraction * 100.0),
            "inotify watch budget OK"
        );
    }
}

/// Count all subdirectories under `base_path` (used with
/// `check_inotify_watch_limit`).
pub fn count_watched_dirs(base_path: &Path) -> usize {
    WalkDir::new(base_path)
        .follow_links(false)
        .into_iter()
        .flatten()
        .filter(|e| e.file_type().is_dir())
        .count()
}

/// Build a [`Gitignore`] matcher for the given folder configuration.
///
/// Automatically appends `.syncd-anchor` and `.syncd-tmp-*` to the
/// ignore list regardless of user configuration.
pub fn build_gitignore(cfg: &FolderConfig) -> Result<Gitignore, WatcherError> {
    let mut builder = GitignoreBuilder::new(&cfg.local_path);

    for pattern in &cfg.exclude_patterns {
        builder
            .add_line(None, pattern)
            .map_err(|e| WatcherError::GitignoreBuild { folder_id: cfg.id, source: e })?;
    }
    builder
        .add_line(None, ".syncd-anchor")
        .map_err(|e| WatcherError::GitignoreBuild { folder_id: cfg.id, source: e })?;
    builder
        .add_line(None, ".syncd-tmp-*")
        .map_err(|e| WatcherError::GitignoreBuild { folder_id: cfg.id, source: e })?;

    builder.build().map_err(|e| WatcherError::GitignoreBuild { folder_id: cfg.id, source: e })
}

/// Spawn a recursive inotify watcher for `cfg`.
///
/// Returns:
/// - A [`FolderWatcher`] RAII guard (must be kept alive for as long as
///   watching is desired).
/// - A `tokio::sync::mpsc::UnboundedReceiver<WatchEvent>` that yields
///   pre-filtered events ready for the debouncer.
///
/// The `_cancel` token is reserved for future integration with the supervisor
/// shutdown path; the bridge thread exits naturally when the watcher is dropped.
pub fn spawn_folder_watcher(
    cfg: &FolderConfig,
    _cancel: CancellationToken,
) -> Result<(FolderWatcher, mpsc::UnboundedReceiver<WatchEvent>), WatcherError> {
    let folder_id = cfg.id;
    let local_path = cfg.local_path.clone();
    let gitignore = Arc::new(build_gitignore(cfg)?);

    // Tokio channel: bridge thread → debouncer
    let (event_tx, event_rx) = mpsc::unbounded_channel::<WatchEvent>();

    // Sync channel: notify callback → bridge thread
    let (raw_tx, raw_rx) = std::sync::mpsc::channel::<notify::Result<Event>>();

    // Zero-capacity sync channel used to signal the bridge thread to stop.
    let (stop_tx, stop_rx) = std::sync::mpsc::sync_channel::<()>(0);

    // Create the notify watcher (callback runs in notify's own thread pool)
    let mut watcher = notify::recommended_watcher(move |res| {
        // If the channel is closed (FolderWatcher dropped), ignore the error
        let _ = raw_tx.send(res);
    })
    .map_err(|e| WatcherError::Notify { folder_id, source: e })?;

    watcher
        .watch(&local_path, RecursiveMode::Recursive)
        .map_err(|e| WatcherError::Notify { folder_id, source: e })?;

    // Bridge thread: drains the sync channel and forwards to the tokio sender
    let tx = event_tx.clone();
    let gi = Arc::clone(&gitignore);
    let base = local_path.clone();

    std::thread::Builder::new()
        .name(format!("syncd-watch-{:.8}", folder_id.to_string()))
        .spawn(move || {
            // We select between incoming notify events and the stop signal.
            // Since std::sync::mpsc doesn't have select!, we use try_recv on
            // stop_rx and rely on raw_rx.recv() blocking briefly.
            //
            // In practice: raw_rx.recv() blocks until a notify event arrives
            // or the watcher is dropped (closing raw_tx).  The stop_rx is only
            // checked at the top of each iteration, which is fine because the
            // watcher drop also closes raw_tx, unblocking the thread.
            loop {
                // Check stop signal (non-blocking)
                if stop_rx.try_recv().is_ok() {
                    break;
                }

                match raw_rx.recv() {
                    Err(_) => {
                        // raw_tx dropped → watcher removed → exit cleanly
                        break;
                    }
                    Ok(Err(e)) => {
                        error!(folder_id = %folder_id, "notify error: {}", e);
                    }
                    Ok(Ok(event)) => {
                        process_notify_event(event, folder_id, &base, &gi, &tx);
                    }
                }
            }
            debug!(folder_id = %folder_id, "watcher bridge thread exited");
        })
        .map_err(WatcherError::Io)?;

    Ok((
        FolderWatcher {
            folder_id,
            local_path,
            gitignore,
            _watcher: watcher,
            _bridge_stop: stop_tx,
        },
        event_rx,
    ))
}

// ─────────────────────────────────────────────
// Internal helpers
// ─────────────────────────────────────────────

/// Returns `Some(OpType)` for events we handle, `None` for events to ignore.
///
/// We intentionally skip `Modify(Data)` (raw writes) to avoid syncing
/// half-written files.
fn classify_event(kind: &EventKind) -> Option<OpType> {
    match kind {
        // IN_CLOSE_WRITE — file data safely flushed, safe to upload
        EventKind::Access(AccessKind::Close(AccessMode::Write)) => Some(OpType::Upload),
        // IN_MOVED_FROM / IN_MOVED_TO — rename within or into the tree
        EventKind::Modify(ModifyKind::Name(_)) => Some(OpType::Upload),
        // IN_DELETE / IN_DELETE_SELF
        EventKind::Remove(_) => Some(OpType::Delete),
        // New directory: handled specially below
        EventKind::Create(CreateKind::Folder) => None,
        // Everything else (raw data writes, chmod, access, etc.) → skip
        _ => None,
    }
}

/// Strip `base` from `abs_path`, returning a relative `PathBuf`.
///
/// Returns `None` if `abs_path` is not under `base`.
fn make_relative(base: &Path, abs_path: &Path) -> Option<PathBuf> {
    abs_path.strip_prefix(base).ok().map(PathBuf::from)
}

/// Walk `new_dir` and synthesise `Upload` events for every file inside.
///
/// Closes the classic inotify "mkdir -p" race: files created inside a new
/// subdirectory before `inotify_add_watch` returned are invisible to the
/// kernel.  We walk immediately and emit synthetic events for them.
fn synthesise_close_write_for_new_dir(
    new_dir: &Path,
    base: &Path,
    gitignore: &Gitignore,
    tx: &mpsc::UnboundedSender<WatchEvent>,
    folder_id: Uuid,
) {
    for entry in WalkDir::new(new_dir)
        .follow_links(false)
        .min_depth(1)
        .into_iter()
        .flatten()
    {
        let path = entry.path();
        let is_dir = entry.file_type().is_dir();

        // Apply exclude filter
        if gitignore
            .matched_path_or_any_parents(path, is_dir)
            .is_ignore()
        {
            continue;
        }
        if is_dir {
            // Subdirectory events are handled by notify attaching watches
            continue;
        }
        if let Some(rel) = make_relative(base, path) {
            debug!(
                folder_id = %folder_id,
                path = ?rel,
                "synthesised IN_CLOSE_WRITE for pre-existing file in new dir"
            );
            let _ = tx.send(WatchEvent { folder_id, rel_path: rel, op: OpType::Upload });
        }
    }
}

/// Core event dispatcher: filter → classify → forward.
fn process_notify_event(
    event: Event,
    folder_id: Uuid,
    base: &Path,
    gitignore: &Gitignore,
    tx: &mpsc::UnboundedSender<WatchEvent>,
) {
    let is_folder_create = matches!(event.kind, EventKind::Create(CreateKind::Folder));
    let op_maybe = classify_event(&event.kind);

    // Skip events we don't handle (not a directory create either)
    if op_maybe.is_none() && !is_folder_create {
        debug!("skipping event kind {:?}", event.kind);
        return;
    }

    for abs_path in &event.paths {
        let is_dir = abs_path.is_dir();

        // ── Gitignore gate ──────────────────────────────────────────
        if gitignore
            .matched_path_or_any_parents(abs_path, is_dir)
            .is_ignore()
        {
            debug!(?abs_path, "excluded by gitignore");
            continue;
        }

        // ── New directory: synthesise missed file events ─────────────
        if is_folder_create && is_dir {
            debug!(?abs_path, "new directory created; synthesising events");
            synthesise_close_write_for_new_dir(abs_path, base, gitignore, tx, folder_id);
            // notify will automatically attach a recursive watch on the new dir;
            // we don't need to call watch() again explicitly.
            continue;
        }

        // ── Regular file events ─────────────────────────────────────
        if is_dir {
            // Remove / rename events on directories don't need individual
            // file-level handling; the debouncer will process child files.
            continue;
        }

        if let Some(op) = op_maybe {
            if let Some(rel) = make_relative(base, abs_path) {
                debug!(folder_id = %folder_id, path = ?rel, op = ?op, "emitting watch event");
                let _ = tx.send(WatchEvent { folder_id, rel_path: rel, op });
            }
        }
    }
}
