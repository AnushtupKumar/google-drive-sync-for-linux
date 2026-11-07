//! `syncctl` — CLI control and live telemetry client for `syncd`.
//!
//! Communicates with the background daemon over a Unix Domain Socket using
//! length-delimited JSON-RPC framing.

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use comfy_table::{
    modifiers::UTF8_ROUND_CORNERS,
    presets::UTF8_FULL,
    Attribute, Cell, Color, ContentArrangement, Table,
};
use futures::{SinkExt, StreamExt};
use std::path::PathBuf;
use sync_common::{
    ipc::{client_framed_split, IpcRequest, IpcResponse},
    state::{format_bytes, FolderState, TelemetrySnapshot},
};
use tokio::net::UnixStream;
use uuid::Uuid;

pub mod auth;
pub mod folder;
pub mod setup;
pub mod tui;
pub mod web;

// ─────────────────────────────────────────────
// Command-line interface
// ─────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "syncctl",
    about = "Control and live telemetry client for syncd",
    version
)]
struct Cli {
    /// Custom path to syncd Unix domain socket
    #[arg(short, long, value_name = "SOCKET")]
    socket: Option<PathBuf>,

    /// Custom path to syncd configuration file
    #[arg(short, long, value_name = "CONFIG")]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Interactive Terminal User Interface (keyboard-driven real-time dashboard)
    #[command(alias = "tui")]
    Ui,

    /// First-time setup wizard for authentication and folder initialization
    #[command(alias = "init")]
    Setup,

    /// Cloud storage authentication and credentials management
    #[command(alias = "login")]
    Auth(auth::AuthArgs),

    /// Launch embedded Web dashboard server (accessible via browser)
    Web {
        /// Bind host address
        #[arg(long, default_value = "127.0.0.1")]
        bind: String,

        /// HTTP server port
        #[arg(short, long, default_value_t = 8384)]
        port: u16,
    },

    /// Manage watched folders (wizard, list, add, remove, inspect, edit)
    #[command(alias = "folders")]
    Folder(folder::FolderArgs),

    /// Reload syncd daemon configuration from disk
    Reload,

    /// Show current daemon status, watched folders, and in-flight transfers
    Status,

    /// Stream live transfer progress and throughput in real-time
    Watch,

    /// Pause synchronization (indefinitely or for a specified duration)
    Pause {
        /// Optional folder UUID or name
        folder: Option<String>,

        /// Pause duration (e.g. '30m', '1h', '45s')
        #[arg(long, value_name = "DURATION")]
        r#for: Option<String>,
    },

    /// Resume synchronization for one or all folders
    Resume {
        /// Optional folder UUID or name
        folder: Option<String>,
    },

    /// Set global upload bandwidth limit (0 = unlimited)
    Bwlimit {
        /// Rate limit (e.g. '5MB/s', '500KB', '10M', '0' for unlimited)
        rate: String,
    },

    /// Trigger an immediate reconciliation scan for one or all folders
    Scan {
        /// Optional folder UUID or name
        folder: Option<String>,
    },
}

// ─────────────────────────────────────────────
// Default socket locator
// ─────────────────────────────────────────────

fn default_socket_path() -> PathBuf {
    if let Ok(runtime) = std::env::var("XDG_RUNTIME_DIR") {
        PathBuf::from(runtime).join("syncd.sock")
    } else {
        let uid = unsafe { libc::getuid() };
        PathBuf::from(format!("/run/user/{}/syncd.sock", uid))
    }
}

// ─────────────────────────────────────────────
// Client connection helper
// ─────────────────────────────────────────────

async fn connect_to_daemon(
    socket_override: Option<PathBuf>,
) -> Result<(
    tokio_util::codec::FramedRead<tokio::net::unix::OwnedReadHalf, sync_common::ipc::IpcResponseCodec>,
    tokio_util::codec::FramedWrite<tokio::net::unix::OwnedWriteHalf, sync_common::ipc::IpcRequestCodec>,
)> {
    let socket_path = socket_override.unwrap_or_else(default_socket_path);

    let stream = UnixStream::connect(&socket_path)
        .await
        .with_context(|| {
            format!(
                "Failed to connect to syncd socket at '{}'. Is syncd running?",
                socket_path.display()
            )
        })?;

    Ok(client_framed_split(stream))
}

// ─────────────────────────────────────────────
// Main entrypoint
// ─────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let socket = cli.socket.clone().unwrap_or_else(default_socket_path);

    match cli.command.unwrap_or(Commands::Ui) {
        Commands::Ui => tui::run_tui(socket).await?,
        Commands::Setup => setup::run_setup_wizard(cli.config, cli.socket).await?,
        Commands::Auth(args) => auth::handle_auth_command(args).await?,
        Commands::Web { bind, port } => {
            let addr: std::net::SocketAddr = format!("{}:{}", bind, port)
                .parse()
                .with_context(|| format!("Invalid bind address '{}:{}'", bind, port))?;
            web::run_web_server(socket, addr).await?;
        }
        Commands::Folder(args) => {
            folder::handle_folder_command(cli.config, cli.socket, args.command).await?;
        }
        Commands::Reload => cmd_reload(cli.socket).await?,
        Commands::Status => cmd_status(cli.socket).await?,
        Commands::Watch => cmd_watch(cli.socket).await?,
        Commands::Pause { folder, r#for } => cmd_pause(cli.socket, folder, r#for).await?,
        Commands::Resume { folder } => cmd_resume(cli.socket, folder).await?,
        Commands::Bwlimit { rate } => cmd_bwlimit(cli.socket, rate).await?,
        Commands::Scan { folder } => cmd_scan(cli.socket, folder).await?,
    }

    Ok(())
}

// ─────────────────────────────────────────────
// Subcommand Implementations
// ─────────────────────────────────────────────

async fn cmd_status(socket: Option<PathBuf>) -> Result<()> {
    let (mut reader, mut writer) = connect_to_daemon(socket).await?;

    writer.send(IpcRequest::GetStatus).await?;

    match reader.next().await {
        Some(Ok(IpcResponse::StatusSnapshot(snapshot))) => {
            render_status(&snapshot);
        }
        Some(Ok(IpcResponse::Error { code, message })) => {
            eprintln!("Error ({:?}): {}", code, message);
        }
        Some(Ok(other)) => {
            println!("Unexpected response: {:?}", other);
        }
        Some(Err(e)) => {
            eprintln!("Protocol error: {}", e);
        }
        None => {
            eprintln!("Connection closed by syncd unexpectedly.");
        }
    }

    Ok(())
}

async fn cmd_watch(socket: Option<PathBuf>) -> Result<()> {
    let (mut reader, mut writer) = connect_to_daemon(socket).await?;

    writer.send(IpcRequest::SubscribeTelemetry).await?;

    // Clear screen on start
    print!("\x1B[2J\x1B[1;1H");

    while let Some(msg) = reader.next().await {
        match msg {
            Ok(IpcResponse::TelemetryUpdate(snapshot)) => {
                // Move cursor to top-left without clearing to prevent flicker
                print!("\x1B[1;1H");
                render_status(&snapshot);
            }
            Ok(IpcResponse::ServerShuttingDown) => {
                println!("\nDaemon is shutting down.");
                break;
            }
            Ok(IpcResponse::Error { code, message }) => {
                eprintln!("\nError ({:?}): {}", code, message);
                break;
            }
            Err(e) => {
                eprintln!("\nConnection closed: {}", e);
                break;
            }
            _ => {}
        }
    }

    Ok(())
}

async fn cmd_pause(
    socket: Option<PathBuf>,
    folder: Option<String>,
    duration_str: Option<String>,
) -> Result<()> {
    let (mut reader, mut writer) = connect_to_daemon(socket).await?;

    let folder_id = resolve_folder_id(&mut reader, &mut writer, folder).await?;
    let duration_secs = match duration_str {
        Some(s) => Some(parse_duration(&s)?),
        None => None,
    };

    writer
        .send(IpcRequest::Pause {
            folder_id,
            duration_secs,
        })
        .await?;

    handle_ok_response(reader.next().await).await
}

async fn cmd_resume(socket: Option<PathBuf>, folder: Option<String>) -> Result<()> {
    let (mut reader, mut writer) = connect_to_daemon(socket).await?;
    let folder_id = resolve_folder_id(&mut reader, &mut writer, folder).await?;

    writer.send(IpcRequest::Resume { folder_id }).await?;
    handle_ok_response(reader.next().await).await
}

async fn cmd_bwlimit(socket: Option<PathBuf>, rate_str: String) -> Result<()> {
    let (mut reader, mut writer) = connect_to_daemon(socket).await?;
    let bytes_per_sec = parse_bandwidth(&rate_str)?;

    writer
        .send(IpcRequest::SetBandwidthLimit { bytes_per_sec })
        .await?;

    handle_ok_response(reader.next().await).await
}

async fn cmd_scan(socket: Option<PathBuf>, folder: Option<String>) -> Result<()> {
    let (mut reader, mut writer) = connect_to_daemon(socket).await?;
    let folder_id = resolve_folder_id(&mut reader, &mut writer, folder).await?;

    writer.send(IpcRequest::TriggerScan { folder_id }).await?;
    handle_ok_response(reader.next().await).await
}

async fn cmd_reload(socket: Option<PathBuf>) -> Result<()> {
    let (mut reader, mut writer) = connect_to_daemon(socket).await?;
    writer.send(IpcRequest::ReloadConfig).await?;
    handle_ok_response(reader.next().await).await
}

// ─────────────────────────────────────────────
// Rendering Helpers
// ─────────────────────────────────────────────

fn render_status(snapshot: &TelemetrySnapshot) {
    let bw_limit_str = if snapshot.configured_bw_limit_bps == 0 {
        "Unlimited".to_string()
    } else {
        format!("{}/s", format_bytes(snapshot.configured_bw_limit_bps))
    };

    println!(
        "syncd Telemetry  •  Upload Speed: {}/s  •  Bandwidth Cap: {}",
        format_bytes(snapshot.global_upload_bps),
        bw_limit_str
    );
    println!();

    // Folders Table
    let mut f_table = Table::new();
    f_table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new("Name").add_attribute(Attribute::Bold),
            Cell::new("Folder ID").add_attribute(Attribute::Bold),
            Cell::new("State").add_attribute(Attribute::Bold),
            Cell::new("Mode").add_attribute(Attribute::Bold),
            Cell::new("Pending").add_attribute(Attribute::Bold),
            Cell::new("Tracked").add_attribute(Attribute::Bold),
            Cell::new("Target").add_attribute(Attribute::Bold),
            Cell::new("Local Path").add_attribute(Attribute::Bold),
        ]);

    for f in &snapshot.folders {
        let state_cell = match &f.state {
            FolderState::Watching => Cell::new("Watching").fg(Color::Green),
            FolderState::Syncing => Cell::new("Syncing").fg(Color::Cyan),
            FolderState::Debouncing => Cell::new("Debouncing").fg(Color::Blue),
            FolderState::Paused { until: None } => Cell::new("Paused").fg(Color::Yellow),
            FolderState::Paused { until: Some(ts) } => {
                Cell::new(format!("Paused (until {})", ts.format("%H:%M:%S"))).fg(Color::Yellow)
            }
            FolderState::DriveDisconnected => Cell::new("Disconnected").fg(Color::DarkRed),
            FolderState::Error { reason } => Cell::new(format!("Error: {}", reason)).fg(Color::Red),
            FolderState::Reconciling => Cell::new("Reconciling").fg(Color::Magenta),
        };

        let mode_label = match (f.sync_mode.as_str(), f.safety_lock_armed) {
            ("continuous_backup", true) => "Backup [Safe]",
            ("continuous_backup", false) => "Backup",
            ("drop_and_purge", _) => "Offload",
            (other, _) => other,
        };

        f_table.add_row(vec![
            Cell::new(&f.name).add_attribute(Attribute::Bold),
            Cell::new(f.folder_id.to_string()),
            state_cell,
            Cell::new(mode_label),
            Cell::new(f.pending_tasks.to_string()),
            Cell::new(f.baseline_file_count.to_string()),
            Cell::new(&f.remote_target),
            Cell::new(&f.local_path),
        ]);
    }

    println!("{}", f_table);

    // Active Transfers Table
    if !snapshot.active_transfers.is_empty() {
        println!("\nActive Transfers ({}):", snapshot.active_transfers.len());

        let folder_names: std::collections::HashMap<Uuid, &str> = snapshot
            .folders
            .iter()
            .map(|f| (f.folder_id, f.name.as_str()))
            .collect();

        let mut t_table = Table::new();
        t_table
            .load_preset(UTF8_FULL)
            .apply_modifier(UTF8_ROUND_CORNERS)
            .set_content_arrangement(ContentArrangement::Dynamic)
            .set_header(vec![
                Cell::new("Task").add_attribute(Attribute::Bold),
                Cell::new("Folder").add_attribute(Attribute::Bold),
                Cell::new("File").add_attribute(Attribute::Bold),
                Cell::new("Op").add_attribute(Attribute::Bold),
                Cell::new("Progress").add_attribute(Attribute::Bold),
                Cell::new("Speed").add_attribute(Attribute::Bold),
                Cell::new("ETA").add_attribute(Attribute::Bold),
            ]);

        for t in &snapshot.active_transfers {
            let pct = if t.total_bytes > 0 {
                (t.bytes_transferred as f64 / t.total_bytes as f64) * 100.0
            } else {
                100.0
            };

            let eta_str = match t.eta_secs {
                Some(secs) => format!("{:.0}s", secs),
                None => "—".to_string(),
            };

            let folder_label = folder_names.get(&t.folder_id).copied().unwrap_or("—");
            let speed_bps = t.upload_bps_ewma.round() as u64;

            t_table.add_row(vec![
                Cell::new(t.task_id.to_string()),
                Cell::new(folder_label),
                Cell::new(&t.rel_path),
                Cell::new(t.op_type.as_db_str()),
                Cell::new(format!(
                    "{} / {} ({:.1}%)",
                    format_bytes(t.bytes_transferred),
                    format_bytes(t.total_bytes),
                    pct
                )),
                Cell::new(format!("{}/s", format_bytes(speed_bps))),
                Cell::new(eta_str),
            ]);
        }

        println!("{}", t_table);
    }
}

// ─────────────────────────────────────────────
// Parsers & Helpers
// ─────────────────────────────────────────────

async fn handle_ok_response(
    resp_opt: Option<Result<IpcResponse, sync_common::ipc::CodecError>>,
) -> Result<()> {
    match resp_opt {
        Some(Ok(IpcResponse::Ok { message })) => {
            println!("✓ {}", message);
            Ok(())
        }
        Some(Ok(IpcResponse::Error { code, message })) => {
            eprintln!("✗ Error ({:?}): {}", code, message);
            anyhow::bail!("Command rejected: {}", message);
        }
        Some(Ok(other)) => {
            println!("Response: {:?}", other);
            Ok(())
        }
        Some(Err(e)) => anyhow::bail!("Protocol decode error: {}", e),
        None => anyhow::bail!("Connection closed by syncd"),
    }
}

async fn resolve_folder_id(
    reader: &mut tokio_util::codec::FramedRead<tokio::net::unix::OwnedReadHalf, sync_common::ipc::IpcResponseCodec>,
    writer: &mut tokio_util::codec::FramedWrite<tokio::net::unix::OwnedWriteHalf, sync_common::ipc::IpcRequestCodec>,
    s_opt: Option<String>,
) -> Result<Option<Uuid>> {
    let Some(s) = s_opt else { return Ok(None); };
    if s.eq_ignore_ascii_case("all") {
        return Ok(None);
    }
    if let Ok(id) = Uuid::parse_str(&s) {
        return Ok(Some(id));
    }
    // Query status from syncd to find the folder by configured name
    writer.send(IpcRequest::GetStatus).await?;
    match reader.next().await {
        Some(Ok(IpcResponse::StatusSnapshot(snap))) => {
            if let Some(f) = snap.folders.iter().find(|f| f.name.eq_ignore_ascii_case(&s)) {
                Ok(Some(f.folder_id))
            } else {
                let available: Vec<_> = snap.folders.iter().map(|f| f.name.as_str()).collect();
                anyhow::bail!("Folder '{}' not found. Configured folders: [{}]", s, available.join(", "))
            }
        }
        Some(Ok(other)) => anyhow::bail!("Unexpected response while resolving folder: {:?}", other),
        Some(Err(e)) => anyhow::bail!("Protocol error while resolving folder: {}", e),
        None => anyhow::bail!("Connection closed by syncd while resolving folder"),
    }
}

fn parse_duration(s: &str) -> Result<u64> {
    let s = s.trim().to_lowercase();
    if let Ok(secs) = s.parse::<u64>() {
        return Ok(secs);
    }

    let mut total_secs = 0u64;
    let mut current_num = String::new();

    for c in s.chars() {
        if c.is_ascii_digit() {
            current_num.push(c);
        } else {
            let num: u64 = current_num
                .parse()
                .with_context(|| format!("Invalid number in duration: {}", s))?;
            current_num.clear();
            match c {
                's' => total_secs += num,
                'm' => total_secs += num * 60,
                'h' => total_secs += num * 3600,
                'd' => total_secs += num * 86400,
                other => anyhow::bail!("Unknown duration unit '{}' in '{}'", other, s),
            }
        }
    }

    if !current_num.is_empty() {
        let num: u64 = current_num.parse()?;
        total_secs += num;
    }

    Ok(total_secs)
}

fn parse_bandwidth(s: &str) -> Result<u64> {
    let s = s.trim().to_lowercase();
    if s == "0" || s == "unlimited" || s == "none" {
        return Ok(0);
    }

    if let Ok(raw) = s.parse::<u64>() {
        return Ok(raw);
    }

    let (num_part, unit_part) = if let Some(idx) = s.find(|c: char| !c.is_ascii_digit() && c != '.') {
        (&s[..idx], &s[idx..])
    } else {
        (s.as_str(), "")
    };

    let val: f64 = num_part
        .parse()
        .with_context(|| format!("Invalid bandwidth number: {}", s))?;
    let unit = unit_part.trim_end_matches("/s").trim_end_matches("ps").trim();

    let bytes = match unit {
        "k" | "kb" | "kib" => val * 1024.0,
        "m" | "mb" | "mib" => val * 1024.0 * 1024.0,
        "g" | "gb" | "gib" => val * 1024.0 * 1024.0 * 1024.0,
        "b" | "" => val,
        other => anyhow::bail!("Unknown bandwidth unit '{}' in '{}'", other, s),
    };

    Ok(bytes.round() as u64)
}

// ─────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("30s").unwrap(), 30);
        assert_eq!(parse_duration("5m").unwrap(), 300);
        assert_eq!(parse_duration("1h").unwrap(), 3600);
        assert_eq!(parse_duration("1d").unwrap(), 86400);
        assert_eq!(parse_duration("1h30m").unwrap(), 5400);
        assert_eq!(parse_duration("45").unwrap(), 45);
    }

    #[test]
    fn test_parse_bandwidth() {
        assert_eq!(parse_bandwidth("0").unwrap(), 0);
        assert_eq!(parse_bandwidth("unlimited").unwrap(), 0);
        assert_eq!(parse_bandwidth("1024").unwrap(), 1024);
        assert_eq!(parse_bandwidth("10KB").unwrap(), 10 * 1024);
        assert_eq!(parse_bandwidth("5MB/s").unwrap(), 5 * 1024 * 1024);
        assert_eq!(parse_bandwidth("1G").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1024), "1.0 KiB");
        assert_eq!(format_bytes(1048576), "1.0 MiB");
        assert_eq!(format_bytes(1073741824), "1.0 GiB");
    }
}
