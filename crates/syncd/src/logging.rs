//! Logging infrastructure and desktop notification engine for `syncd`.
//!
//! ## Log Rotation & Gzip Compression
//!
//! Log events are written simultaneously to `stderr` (for interactive execution
//! and `systemd` journal capture) and to a rotating, gzip-compressed log file at
//! `~/.local/state/syncd/logs/syncd.log`.
//!
//! Rotation is managed by [`file_rotate::FileRotate`] with:
//! - [`ContentLimit::Bytes`] set to `GlobalConfig::log_max_size_bytes` (default 25 MiB).
//! - [`Compression::OnRotate(0)`] for immediate gzip compression of rotated archives.
//! - [`AppendCount::new`] keeping up to `GlobalConfig::log_max_archives` archives.
//!
//! ## Desktop Notifications
//!
//! Desktop alerts are surfaced via D-Bus using [`notify_rust`].  All notifications
//! are dispatched onto `tokio::task::spawn_blocking` so they never block the
//! async reactor or inotify event loop.

use file_rotate::{
    compression::Compression,
    suffix::AppendCount,
    ContentLimit, FileRotate,
};
use notify_rust::{Notification, Timeout, Urgency};
use std::{
    io::{self, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
};
use thiserror::Error;
use tracing_subscriber::{
    filter::EnvFilter,
    fmt,
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

// ─────────────────────────────────────────────
// Error types
// ─────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum LoggingError {
    #[error("failed to create log directory '{0}': {1}")]
    CreateDir(PathBuf, io::Error),

    #[error("failed to initialize tracing subscriber: {0}")]
    InitSubscriber(String),
}

// ─────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────

/// Configuration for file logging and rotation.
#[derive(Debug, Clone)]
pub struct LogConfig {
    pub log_file: PathBuf,
    pub max_size_bytes: u64,
    pub max_archives: usize,
}

// ─────────────────────────────────────────────
// Custom MakeWriter for FileRotate
// ─────────────────────────────────────────────

#[derive(Clone)]
struct MakeFileWriter {
    inner: Arc<Mutex<FileRotate<AppendCount>>>,
}

impl<'a> fmt::writer::MakeWriter<'a> for MakeFileWriter {
    type Writer = FileRotateGuard<'a>;

    fn make_writer(&'a self) -> Self::Writer {
        FileRotateGuard {
            guard: self.inner.lock().expect("file rotator mutex poisoned"),
        }
    }
}

struct FileRotateGuard<'a> {
    guard: std::sync::MutexGuard<'a, FileRotate<AppendCount>>,
}

impl<'a> Write for FileRotateGuard<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.guard.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.guard.flush()
    }
}

// ─────────────────────────────────────────────
// Initialization
// ─────────────────────────────────────────────

/// Initialize global tracing with console (stderr) and rotating file layers.
///
/// If `config` is provided, file rotation is configured and enabled.
/// If `config` is `None`, only stderr logging is activated.
pub fn init_logging(config: Option<&LogConfig>) -> Result<(), LoggingError> {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,syncd=debug,sync_common=debug"));

    let stderr_layer = fmt::layer()
        .with_target(true)
        .with_thread_ids(false)
        .with_level(true)
        .with_ansi(atty_stderr())
        .with_writer(io::stderr);

    if let Some(cfg) = config {
        if let Some(parent) = cfg.log_file.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| LoggingError::CreateDir(parent.to_path_buf(), e))?;
        }

        let rotator = FileRotate::new(
            &cfg.log_file,
            AppendCount::new(cfg.max_archives),
            ContentLimit::Bytes(cfg.max_size_bytes as usize),
            Compression::OnRotate(0),
            None,
        );

        let make_writer = MakeFileWriter {
            inner: Arc::new(Mutex::new(rotator)),
        };

        let file_layer = fmt::layer()
            .with_target(true)
            .with_thread_ids(true)
            .with_level(true)
            .with_ansi(false)
            .with_writer(make_writer);

        tracing_subscriber::registry()
            .with(env_filter)
            .with(stderr_layer)
            .with(file_layer)
            .try_init()
            .map_err(|e| LoggingError::InitSubscriber(e.to_string()))?;
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(stderr_layer)
            .try_init()
            .map_err(|e| LoggingError::InitSubscriber(e.to_string()))?;
    }

    Ok(())
}

fn atty_stderr() -> bool {
    unsafe { libc::isatty(libc::STDERR_FILENO) == 1 }
}

// ─────────────────────────────────────────────
// Desktop Notifications
// ─────────────────────────────────────────────

/// Send an asynchronous desktop notification via D-Bus.
///
/// This function never blocks the calling async task.  Errors communicating with
/// the D-Bus daemon (e.g. headless environment) are logged as debug messages
/// and safely discarded.
pub fn notify_alert(
    summary: impl Into<String>,
    body: impl Into<String>,
    urgency: Urgency,
    timeout_ms: Option<u32>,
) {
    let summary = summary.into();
    let body = body.into();

    tokio::task::spawn_blocking(move || {
        let mut n = Notification::new();
        n.appname("syncd")
            .summary(&summary)
            .body(&body)
            .urgency(urgency);

        if let Some(ms) = timeout_ms {
            n.timeout(Timeout::Milliseconds(ms));
        } else {
            n.timeout(Timeout::Default);
        }

        if let Err(e) = n.show() {
            tracing::debug!(summary = %summary, error = %e, "desktop notification suppressed");
        }
    });
}

/// Helper for standard informational desktop notifications (settles in 5s).
pub fn notify_info(summary: impl Into<String>, body: impl Into<String>) {
    notify_alert(summary, body, Urgency::Normal, Some(5_000));
}

/// Helper for warning notifications (persists 10s).
pub fn notify_warn(summary: impl Into<String>, body: impl Into<String>) {
    notify_alert(summary, body, Urgency::Critical, Some(10_000));
}

/// Helper for critical safety locks / errors (persists until dismissed).
pub fn notify_critical(summary: impl Into<String>, body: impl Into<String>) {
    notify_alert(summary, body, Urgency::Critical, None);
}

