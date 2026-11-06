//! `syncd` — Real-Time Cloud Synchronization and Safe File Offloading Daemon.
//!
//! Entrypoint orchestrating configuration parsing, structured log rotation,
//! SQLite database migration, supervisor bootstrap, Unix socket IPC,
//! systemd notification, and POSIX signal handling.

pub mod db;
pub mod debouncer;
pub mod ipc_server;
pub mod logging;
pub mod offload;
pub mod safety;
pub mod storage;
pub mod supervisor;
pub mod watcher;

use anyhow::{Context, Result};
use clap::Parser;
use logging::LogConfig;
use std::{
    os::unix::net::UnixDatagram,
    path::PathBuf,
};
use supervisor::Supervisor;
use sync_common::config::GlobalConfig;
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

// ─────────────────────────────────────────────
// Command-line arguments
// ─────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "syncd",
    about = "Real-time cloud synchronization and crash-safe file offloading daemon for Linux",
    version
)]
struct Args {
    /// Path to configuration TOML file
    #[arg(short, long, value_name = "FILE")]
    config: Option<PathBuf>,
}

// ─────────────────────────────────────────────
// Default config file locator
// ─────────────────────────────────────────────

fn default_config_path() -> PathBuf {
    if let Ok(cfg_dir) = std::env::var("XDG_CONFIG_HOME") {
        PathBuf::from(cfg_dir).join("syncd/config.toml")
    } else {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
        PathBuf::from(home).join(".config/syncd/config.toml")
    }
}

// ─────────────────────────────────────────────
// Systemd sd_notify helper
// ─────────────────────────────────────────────

fn sd_notify(state: &str) {
    if let Ok(socket_path) = std::env::var("NOTIFY_SOCKET") {
        if let Ok(sock) = UnixDatagram::unbound() {
            let _ = sock.send_to(state.as_bytes(), socket_path);
        }
    }
}

// ─────────────────────────────────────────────
// Main entrypoint
// ─────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config_path = args.config.unwrap_or_else(default_config_path);

    // Bootstrap initial basic console logger
    let _ = logging::init_logging(None);

    info!(version = env!("CARGO_PKG_VERSION"), "syncd starting");

    // Load configuration
    if !config_path.exists() {
        error!(
            path = ?config_path,
            "Configuration file not found. Please create ~/.config/syncd/config.toml"
        );
        anyhow::bail!("Configuration file missing: {}", config_path.display());
    }

    let config = GlobalConfig::from_file(&config_path)
        .with_context(|| format!("Failed to parse config at '{}'", config_path.display()))?;

    // Validate config rules (naming collisions, remote targets)
    config
        .validate()
        .with_context(|| "Config validation failed")?;

    // Re-initialize logging with configured file rotation & limits
    let log_cfg = LogConfig {
        log_file: config.log_file_path(),
        max_size_bytes: config.log_max_size_bytes,
        max_archives: config.log_max_archives,
    };
    let _ = logging::init_logging(Some(&log_cfg));

    info!(
        log_file = ?config.log_file_path(),
        max_size = config.log_max_size_bytes,
        archives = config.log_max_archives,
        "Configured rotating file logger"
    );

    // Initialize SQLite WAL pool and run embedded schema migrations
    let db_path = config.db_path();
    info!(db_path = ?db_path, "Connecting to SQLite database");
    let pool = db::open_and_migrate(&db_path)
        .await
        .with_context(|| format!("Database migration failed at '{}'", db_path.display()))?;

    let cancel = CancellationToken::new();

    // Boot supervisor orchestrator
    info!("Bootstrapping actor supervisor");
    let supervisor = Supervisor::new(config.clone(), pool.clone(), cancel.clone())
        .await
        .with_context(|| "Failed to bootstrap supervisor")?;

    // Spawn Unix Domain Socket IPC listener
    let socket_path = config.resolved_socket_path();
    let ipc_cancel = cancel.child_token();
    let sup_clone = supervisor.clone();
    let sock_path_clone = socket_path.clone();

    tokio::spawn(async move {
        if let Err(e) = ipc_server::run_ipc_server(sock_path_clone, sup_clone, ipc_cancel).await {
            error!("IPC server error: {}", e);
        }
    });

    // Notify systemd that service startup is ready
    sd_notify("READY=1");
    info!("Daemon initialized and ready for synchronization");

    // Signal handlers for SIGINT, SIGTERM, and SIGHUP
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sighup = signal(SignalKind::hangup())?;

    loop {
        tokio::select! {
            _ = sigint.recv() => {
                info!("Received SIGINT (Ctrl+C), initiating graceful shutdown");
                break;
            }
            _ = sigterm.recv() => {
                info!("Received SIGTERM, initiating graceful shutdown");
                break;
            }
            _ = sighup.recv() => {
                info!("Received SIGHUP, reloading configuration live");
                sd_notify("RELOADING=1");
                match GlobalConfig::from_file(&config_path) {
                    Ok(new_cfg) => {
                        if let Err(e) = supervisor.reload_config(new_cfg).await {
                            error!("Failed to apply reloaded config: {}", e);
                            logging::notify_warn("syncd: Reload Failed", format!("Config error: {}", e));
                        } else {
                            logging::notify_info("syncd: Config Reloaded", "Configuration updated successfully");
                        }
                    }
                    Err(e) => {
                        error!("Failed parsing config on SIGHUP: {}", e);
                        logging::notify_warn("syncd: Config Syntax Error", format!("{}", e));
                    }
                }
                sd_notify("READY=1");
            }
        }
    }

    // Graceful shutdown sequence
    sd_notify("STOPPING=1");
    info!("Cancelling workers and flushing debounce state...");
    cancel.cancel();

    // Small delay to let in-flight cancellations and socket cleanup finish
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    info!("syncd terminated cleanly");
    Ok(())
}
