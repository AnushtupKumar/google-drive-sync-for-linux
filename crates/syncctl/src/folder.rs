//! Folder management CLI and interactive terminal console for `syncctl`.
//!
//! Provides commands and interactive wizards to add, list, show, edit,
//! and remove sync folders from `~/.config/syncd/config.toml` without manual editing,
//! with automatic live daemon reload.

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use comfy_table::{
    modifiers::UTF8_ROUND_CORNERS,
    presets::UTF8_FULL,
    Attribute, Cell, Color, ContentArrangement, Table,
};
use futures::{SinkExt, StreamExt};
use std::{
    io::{stdin, stdout, IsTerminal, Write},
    path::{Path, PathBuf},
};
use sync_common::{
    config::{FolderConfig, GlobalConfig, SyncMode, ValidationResult},
    ipc::{client_framed_split, IpcRequest, IpcResponse},
    state::FolderState,
};
use tokio::net::UnixStream;
use uuid::Uuid;

// ─────────────────────────────────────────────────────────────────────────────
// Command-line interface definitions
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Args, Debug, Clone)]
pub struct FolderArgs {
    #[command(subcommand)]
    pub command: Option<FolderCommands>,
}

#[derive(Subcommand, Debug, Clone)]
pub enum FolderCommands {
    /// Launch the interactive terminal console for managing folders
    #[command(alias = "ui", alias = "menu", alias = "console")]
    Interactive,

    /// List all configured folders and their sync status
    #[command(alias = "ls")]
    List {
        /// Show extended folder configuration details (IDs, endpoints, regions)
        #[arg(short, long)]
        verbose: bool,
    },

    /// Add a new folder to synchronization (runs interactive wizard if flags omitted)
    #[command(alias = "new", alias = "create")]
    Add {
        /// Human-readable label for the folder (e.g. 'documents', 'renders')
        #[arg(short, long)]
        name: Option<String>,

        /// Local directory path to watch or offload
        #[arg(short, long)]
        path: Option<PathBuf>,

        /// Cloud destination URI (e.g. 'gdrive://Documents', 's3://bucket/path', 'file:///path', 'gs://bucket')
        #[arg(short, long)]
        target: Option<String>,

        /// Sync mode: 'backup' (continuous_backup) or 'offload' (drop_and_purge)
        #[arg(short, long, value_parser = ["backup", "continuous_backup", "offload", "drop_and_purge"])]
        mode: Option<String>,

        /// Exclude patterns (can specify multiple: -e '*.tmp' -e '.git/')
        #[arg(short = 'e', long = "exclude")]
        excludes: Vec<String>,

        /// Enable empty folder safety lock (default: true for backup, false for offload)
        #[arg(long)]
        safety_lock: Option<bool>,

        /// Optional custom S3 / S3-compatible endpoint URL (e.g. MinIO, Cloudflare R2)
        #[arg(long)]
        endpoint: Option<String>,

        /// Optional S3 region override (e.g. 'us-east-1', 'auto')
        #[arg(long)]
        region: Option<String>,

        /// Allow unencrypted HTTP for local S3 / MinIO testing
        #[arg(long)]
        allow_http: bool,

        /// Force interactive wizard step-by-step prompts
        #[arg(short, long)]
        interactive: bool,

        /// Automatically reload syncd daemon after adding
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        reload: bool,
    },

    /// Remove a folder from configuration and stop syncing it
    #[command(alias = "rm", alias = "delete")]
    Remove {
        /// Folder name or UUID to remove
        folder: Option<String>,

        /// Skip confirmation prompt
        #[arg(short = 'y', long = "yes", alias = "force")]
        yes: bool,

        /// Automatically reload syncd daemon after removal
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        reload: bool,
    },

    /// Show detailed configuration and runtime status of a folder
    #[command(alias = "get", alias = "info", alias = "inspect")]
    Show {
        /// Folder name or UUID
        folder: Option<String>,
    },

    /// Edit properties of an existing folder
    #[command(alias = "update")]
    Edit {
        /// Folder name or UUID to edit
        folder: Option<String>,

        /// New folder name
        #[arg(short, long)]
        name: Option<String>,

        /// New local directory path
        #[arg(short, long)]
        path: Option<PathBuf>,

        /// New remote cloud target URI
        #[arg(short, long)]
        target: Option<String>,

        /// New sync mode ('backup' or 'offload')
        #[arg(short, long, value_parser = ["backup", "continuous_backup", "offload", "drop_and_purge"])]
        mode: Option<String>,

        /// Update empty folder safety lock
        #[arg(long)]
        safety_lock: Option<bool>,

        /// Force interactive edit mode
        #[arg(short, long)]
        interactive: bool,

        /// Automatically reload syncd daemon after editing
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        reload: bool,
    },
}

// ─────────────────────────────────────────────────────────────────────────────
// Dispatch entrypoint
// ─────────────────────────────────────────────────────────────────────────────

pub async fn handle_folder_command(
    config_override: Option<PathBuf>,
    socket_override: Option<PathBuf>,
    cmd_opt: Option<FolderCommands>,
) -> Result<()> {
    let config_path = config_override.unwrap_or_else(GlobalConfig::default_config_path);

    match cmd_opt.unwrap_or(FolderCommands::Interactive) {
        FolderCommands::Interactive => {
            cmd_interactive(&config_path, socket_override).await?;
        }
        FolderCommands::List { verbose } => {
            cmd_list(&config_path, socket_override, verbose).await?;
        }
        FolderCommands::Add {
            name,
            path,
            target,
            mode,
            excludes,
            safety_lock,
            endpoint,
            region,
            allow_http,
            interactive,
            reload,
        } => {
            cmd_add(
                &config_path,
                socket_override,
                name,
                path,
                target,
                mode,
                excludes,
                safety_lock,
                endpoint,
                region,
                allow_http,
                interactive,
                reload,
            )
            .await?;
        }
        FolderCommands::Remove {
            folder,
            yes,
            reload,
        } => {
            cmd_remove(&config_path, socket_override, folder, yes, reload).await?;
        }
        FolderCommands::Show { folder } => {
            cmd_show(&config_path, socket_override, folder).await?;
        }
        FolderCommands::Edit {
            folder,
            name,
            path,
            target,
            mode,
            safety_lock,
            interactive,
            reload,
        } => {
            cmd_edit(
                &config_path,
                socket_override,
                folder,
                name,
                path,
                target,
                mode,
                safety_lock,
                interactive,
                reload,
            )
            .await?;
        }
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Subcommand implementations
// ─────────────────────────────────────────────────────────────────────────────

/// List configured folders with status from live daemon if running.
pub async fn cmd_list(
    config_path: &Path,
    socket: Option<PathBuf>,
    verbose: bool,
) -> Result<()> {
    let config = load_config(config_path)?;
    let daemon_snapshot = fetch_status_quiet(socket).await;

    println!("\x1B[1mWatched Folders\x1B[0m ({} configured)", config.folders.len());
    println!("Config file: \x1B[36m{}\x1B[0m\n", config_path.display());

    if config.folders.is_empty() {
        println!("  No folders configured yet.");
        println!("  Run '\x1B[32msyncctl folder add\x1B[0m' or '\x1B[32msyncctl folder\x1B[0m' to add your first folder.\n");
        return Ok(());
    }

    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_content_arrangement(ContentArrangement::Dynamic);

    if verbose {
        table.set_header(vec![
            Cell::new("#").add_attribute(Attribute::Bold),
            Cell::new("Name").add_attribute(Attribute::Bold),
            Cell::new("Folder ID").add_attribute(Attribute::Bold),
            Cell::new("Mode").add_attribute(Attribute::Bold),
            Cell::new("State").add_attribute(Attribute::Bold),
            Cell::new("Local Path").add_attribute(Attribute::Bold),
            Cell::new("Remote Target").add_attribute(Attribute::Bold),
            Cell::new("Excludes").add_attribute(Attribute::Bold),
        ]);
    } else {
        table.set_header(vec![
            Cell::new("#").add_attribute(Attribute::Bold),
            Cell::new("Name").add_attribute(Attribute::Bold),
            Cell::new("Mode").add_attribute(Attribute::Bold),
            Cell::new("State").add_attribute(Attribute::Bold),
            Cell::new("Local Path").add_attribute(Attribute::Bold),
            Cell::new("Remote Target").add_attribute(Attribute::Bold),
        ]);
    }

    for (idx, f) in config.folders.iter().enumerate() {
        let mode_cell = match (f.mode, f.empty_folder_protection) {
            (SyncMode::ContinuousBackup, true) => Cell::new("Backup [Safe]").fg(Color::Cyan),
            (SyncMode::ContinuousBackup, false) => Cell::new("Backup").fg(Color::DarkGrey),
            (SyncMode::DropAndPurge, _) => Cell::new("Offload").fg(Color::Yellow),
        };

        let state_cell = if let Some(snap) = &daemon_snapshot {
            if let Some(fs) = snap.folders.iter().find(|item| item.folder_id == f.id) {
                match &fs.state {
                    FolderState::Watching => Cell::new("Watching").fg(Color::Green),
                    FolderState::Syncing => Cell::new("Syncing").fg(Color::Cyan),
                    FolderState::Debouncing => Cell::new("Debouncing").fg(Color::Blue),
                    FolderState::Paused { .. } => Cell::new("Paused").fg(Color::Yellow),
                    FolderState::DriveDisconnected => Cell::new("Disconnected").fg(Color::Red),
                    FolderState::Error { .. } => Cell::new("Error").fg(Color::Red),
                    FolderState::Reconciling => Cell::new("Reconciling").fg(Color::Magenta),
                }
            } else {
                Cell::new("Not in daemon").fg(Color::Yellow)
            }
        } else {
            Cell::new("Stopped").fg(Color::DarkGrey)
        };

        let local_path_display = if f.local_path.exists() {
            f.local_path.display().to_string()
        } else {
            format!("{} (missing)", f.local_path.display())
        };

        if verbose {
            let excludes_str = if f.exclude_patterns.is_empty() {
                "—".to_string()
            } else {
                f.exclude_patterns.join(", ")
            };

            table.add_row(vec![
                Cell::new((idx + 1).to_string()),
                Cell::new(&f.name).add_attribute(Attribute::Bold),
                Cell::new(f.id.to_string()),
                mode_cell,
                state_cell,
                Cell::new(local_path_display),
                Cell::new(&f.remote_target),
                Cell::new(excludes_str),
            ]);
        } else {
            table.add_row(vec![
                Cell::new((idx + 1).to_string()),
                Cell::new(&f.name).add_attribute(Attribute::Bold),
                mode_cell,
                state_cell,
                Cell::new(local_path_display),
                Cell::new(&f.remote_target),
            ]);
        }
    }

    println!("{}\n", table);

    if daemon_snapshot.is_none() {
        println!("\x1B[90m(syncd background daemon is currently not running)\x1B[0m\n");
    }

    Ok(())
}

/// Add a new folder, either via command-line flags or step-by-step interactive wizard.
#[allow(clippy::too_many_arguments)]
pub async fn cmd_add(
    config_path: &Path,
    socket: Option<PathBuf>,
    name_opt: Option<String>,
    path_opt: Option<PathBuf>,
    target_opt: Option<String>,
    mode_opt: Option<String>,
    excludes_arg: Vec<String>,
    safety_lock_opt: Option<bool>,
    endpoint_opt: Option<String>,
    region_opt: Option<String>,
    allow_http_flag: bool,
    interactive_flag: bool,
    reload: bool,
) -> Result<()> {
    let mut config = load_config(config_path)?;

    let is_interactive = interactive_flag
        || (stdin().is_terminal()
            && (name_opt.is_none() || path_opt.is_none() || target_opt.is_none()));

    let folder_config = if is_interactive {
        if !stdin().is_terminal() {
            anyhow::bail!(
                "Non-interactive terminal detected. Please provide required flags: --name, --path, and --target"
            );
        }
        run_add_wizard(
            &config,
            name_opt,
            path_opt,
            target_opt,
            mode_opt,
            excludes_arg,
            safety_lock_opt,
            endpoint_opt,
            region_opt,
            allow_http_flag,
        )?
    } else {
        let name = name_opt.context("Missing required flag --name")?;
        let raw_path = path_opt.context("Missing required flag --path")?;
        let target = target_opt.context("Missing required flag --target")?;

        let local_path = resolve_local_path(&raw_path)?;
        if !local_path.exists() {
            anyhow::bail!("Local directory '{}' does not exist", local_path.display());
        }

        let mode = match mode_opt.as_deref() {
            Some("offload") | Some("drop_and_purge") => SyncMode::DropAndPurge,
            _ => SyncMode::ContinuousBackup,
        };

        let safety_lock = safety_lock_opt.unwrap_or(mode == SyncMode::ContinuousBackup);

        let default_excludes = vec![
            ".git/".to_string(),
            "target/".to_string(),
            "node_modules/".to_string(),
            "*.tmp".to_string(),
            "*.swp".to_string(),
            "*~".to_string(),
        ];
        let exclude_patterns = if excludes_arg.is_empty() {
            default_excludes
        } else {
            excludes_arg
        };

        FolderConfig {
            id: Uuid::new_v4(),
            name,
            local_path,
            remote_target: target,
            mode,
            exclude_patterns,
            empty_folder_protection: safety_lock,
            endpoint: endpoint_opt,
            region: region_opt,
            allow_http: allow_http_flag,
        }
    };

    let folder_name = folder_config.name.clone();
    let folder_id = folder_config.id;

    // Validate adding this folder
    config.folders.push(folder_config);
    validate_and_save(&config, config_path)?;

    println!(
        "\x1B[32m✓\x1B[0m Successfully added folder '\x1B[1m{}\x1B[0m' (ID: \x1B[36m{}\x1B[0m)",
        folder_name, folder_id
    );
    println!("  Configuration saved to: {}", config_path.display());

    if reload {
        reload_daemon_and_report(socket).await;
    }

    Ok(())
}

/// Remove a folder from configuration and stop syncing it.
pub async fn cmd_remove(
    config_path: &Path,
    socket: Option<PathBuf>,
    target_str: Option<String>,
    yes: bool,
    reload: bool,
) -> Result<()> {
    let mut config = load_config(config_path)?;

    if config.folders.is_empty() {
        println!("No folders currently configured in {}", config_path.display());
        return Ok(());
    }

    let folder_idx = match target_str {
        Some(s) => match find_folder_index(&config, &s) {
            Some(idx) => idx,
            None => {
                let names: Vec<_> = config.folders.iter().map(|f| f.name.as_str()).collect();
                anyhow::bail!(
                    "Folder '{}' not found. Configured folders: [{}]",
                    s,
                    names.join(", ")
                );
            }
        },
        None => {
            if !stdin().is_terminal() {
                anyhow::bail!("Folder name or ID required when running non-interactively");
            }
            println!("\nSelect a folder to remove:");
            for (i, f) in config.folders.iter().enumerate() {
                println!("  [{}] {} ({})", i + 1, f.name, f.local_path.display());
            }
            let choice = prompt_line("\nEnter folder number or name to remove", None)?;
            if choice.trim().is_empty() || choice.trim().eq_ignore_ascii_case("q") {
                println!("Cancelled.");
                return Ok(());
            }
            if let Ok(num) = choice.trim().parse::<usize>() {
                if num >= 1 && num <= config.folders.len() {
                    num - 1
                } else {
                    anyhow::bail!("Invalid selection: {}", num);
                }
            } else {
                find_folder_index(&config, &choice)
                    .context(format!("Folder '{}' not found", choice))?
            }
        }
    };

    let removed = config.folders.remove(folder_idx);

    if !yes {
        println!(
            "\n\x1B[33m⚠ You are about to remove folder '\x1B[1m{}\x1B[0m' (\x1B[36m{}\x1B[0m)\x1B[0m",
            removed.name, removed.id
        );
        println!("  Local path:   {}", removed.local_path.display());
        println!("  Remote target: {}", removed.remote_target);
        println!("  (This stops cloud sync; local and remote files are untouched)");

        let confirmed = prompt_bool("Are you sure you want to remove this folder?", false)?;
        if !confirmed {
            println!("Removal aborted.");
            return Ok(());
        }
    }

    validate_and_save(&config, config_path)?;

    println!(
        "\x1B[32m✓\x1B[0m Removed folder '\x1B[1m{}\x1B[0m' from configuration.",
        removed.name
    );

    if reload {
        reload_daemon_and_report(socket).await;
    }

    Ok(())
}

/// Show full details of a folder.
pub async fn cmd_show(
    config_path: &Path,
    socket: Option<PathBuf>,
    target_str: Option<String>,
) -> Result<()> {
    let config = load_config(config_path)?;

    if config.folders.is_empty() {
        println!("No folders currently configured in {}", config_path.display());
        return Ok(());
    }

    let folder_idx = match target_str {
        Some(s) => match find_folder_index(&config, &s) {
            Some(idx) => idx,
            None => {
                let names: Vec<_> = config.folders.iter().map(|f| f.name.as_str()).collect();
                anyhow::bail!(
                    "Folder '{}' not found. Configured folders: [{}]",
                    s,
                    names.join(", ")
                );
            }
        },
        None => {
            if !stdin().is_terminal() {
                anyhow::bail!("Folder name or ID required when running non-interactively");
            }
            println!("\nSelect a folder to view details:");
            for (i, f) in config.folders.iter().enumerate() {
                println!("  [{}] {} ({})", i + 1, f.name, f.local_path.display());
            }
            let choice = prompt_line("\nEnter folder number or name", None)?;
            if choice.trim().is_empty() {
                return Ok(());
            }
            if let Ok(num) = choice.trim().parse::<usize>() {
                if num >= 1 && num <= config.folders.len() {
                    num - 1
                } else {
                    anyhow::bail!("Invalid selection: {}", num);
                }
            } else {
                find_folder_index(&config, &choice)
                    .context(format!("Folder '{}' not found", choice))?
            }
        }
    };

    let f = &config.folders[folder_idx];
    let daemon_snapshot = fetch_status_quiet(socket).await;

    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_content_arrangement(ContentArrangement::Dynamic);

    table.set_header(vec![
        Cell::new("Property").add_attribute(Attribute::Bold),
        Cell::new("Value").add_attribute(Attribute::Bold),
    ]);

    table.add_row(vec![Cell::new("Folder Name"), Cell::new(&f.name).add_attribute(Attribute::Bold)]);
    table.add_row(vec![Cell::new("Folder ID (UUID)"), Cell::new(f.id.to_string())]);

    let path_status = if f.local_path.exists() {
        format!("{} \x1B[32m(exists)\x1B[0m", f.local_path.display())
    } else {
        format!("{} \x1B[31m(MISSING)\x1B[0m", f.local_path.display())
    };
    table.add_row(vec![Cell::new("Local Path"), Cell::new(path_status)]);
    table.add_row(vec![Cell::new("Remote Target"), Cell::new(&f.remote_target)]);

    let mode_desc = match (f.mode, f.empty_folder_protection) {
        (SyncMode::ContinuousBackup, true) => "Continuous 1-Way Backup [Safety Locked] (safe cloud trash archiving & mass-deletion lock)",
        (SyncMode::ContinuousBackup, false) => "Continuous 1-Way Backup [Safety Off] (direct sync without empty folder lock)",
        (SyncMode::DropAndPurge, _) => "Safe Drop & Purge (TOCTOU verified offloading to free disk space)",
    };
    table.add_row(vec![Cell::new("Sync Mode"), Cell::new(mode_desc)]);

    let excludes_desc = if f.exclude_patterns.is_empty() {
        "None (defaults applied internally)".to_string()
    } else {
        f.exclude_patterns.join("\n")
    };
    table.add_row(vec![Cell::new("Exclude Patterns"), Cell::new(excludes_desc)]);

    if let Some(ep) = &f.endpoint {
        table.add_row(vec![Cell::new("Custom S3 Endpoint"), Cell::new(ep)]);
    }
    if let Some(reg) = &f.region {
        table.add_row(vec![Cell::new("S3 Region"), Cell::new(reg)]);
    }
    if f.allow_http {
        table.add_row(vec![Cell::new("Allow HTTP"), Cell::new("Yes (unencrypted)")]);
    }

    // Live daemon status
    if let Some(snap) = daemon_snapshot {
        if let Some(fs) = snap.folders.iter().find(|item| item.folder_id == f.id) {
            let (state_str, _) = crate::tui::folder_state_display(&fs.state);
            table.add_row(vec![Cell::new("Daemon State"), Cell::new(state_str)]);
            table.add_row(vec![
                Cell::new("Tracked Files"),
                Cell::new(fs.baseline_file_count.to_string()),
            ]);
            table.add_row(vec![
                Cell::new("Pending Queue"),
                Cell::new(fs.pending_tasks.to_string()),
            ]);
            table.add_row(vec![
                Cell::new("Safety Lock Armed"),
                Cell::new(if fs.safety_lock_armed { "ARMED" } else { "Idle" }),
            ]);
        }
    } else {
        table.add_row(vec![Cell::new("Daemon State"), Cell::new("Daemon offline")]);
    }

    println!("\n{}", table);
    Ok(())
}

/// Edit / update properties of an existing folder.
#[allow(clippy::too_many_arguments)]
pub async fn cmd_edit(
    config_path: &Path,
    socket: Option<PathBuf>,
    target_str: Option<String>,
    new_name: Option<String>,
    new_path: Option<PathBuf>,
    new_target: Option<String>,
    new_mode_str: Option<String>,
    new_safety: Option<bool>,
    interactive_flag: bool,
    reload: bool,
) -> Result<()> {
    let mut config = load_config(config_path)?;

    if config.folders.is_empty() {
        println!("No folders currently configured in {}", config_path.display());
        return Ok(());
    }

    let folder_idx = match target_str {
        Some(s) => find_folder_index(&config, &s)
            .context(format!("Folder '{}' not found", s))?,
        None => {
            if !stdin().is_terminal() {
                anyhow::bail!("Folder name or ID required when running non-interactively");
            }
            println!("\nSelect a folder to edit:");
            for (i, f) in config.folders.iter().enumerate() {
                println!("  [{}] {} ({})", i + 1, f.name, f.local_path.display());
            }
            let choice = prompt_line("\nEnter folder number or name to edit", None)?;
            if choice.trim().is_empty() {
                return Ok(());
            }
            if let Ok(num) = choice.trim().parse::<usize>() {
                if num >= 1 && num <= config.folders.len() {
                    num - 1
                } else {
                    anyhow::bail!("Invalid selection: {}", num);
                }
            } else {
                find_folder_index(&config, &choice)
                    .context(format!("Folder '{}' not found", choice))?
            }
        }
    };

    let is_interactive = interactive_flag
        || (stdin().is_terminal()
            && new_name.is_none()
            && new_path.is_none()
            && new_target.is_none()
            && new_mode_str.is_none()
            && new_safety.is_none());

    if is_interactive {
        let f = &mut config.folders[folder_idx];
        println!("\n\x1B[1mEditing folder '{}'\x1B[0m (Press Enter to keep current value):", f.name);

        let name_input = prompt_line(&format!("Folder name [{}]", f.name), Some(&f.name))?;
        if !name_input.trim().is_empty() {
            f.name = name_input.trim().to_string();
        }

        let current_path = f.local_path.display().to_string();
        let path_input = prompt_line(&format!("Local path [{}]", current_path), Some(&current_path))?;
        if !path_input.trim().is_empty() {
            f.local_path = resolve_local_path(Path::new(path_input.trim()))?;
        }

        let target_input = prompt_line(&format!("Remote target [{}]", f.remote_target), Some(&f.remote_target))?;
        if !target_input.trim().is_empty() {
            f.remote_target = target_input.trim().to_string();
        }

        let current_mode = match f.mode {
            SyncMode::ContinuousBackup => "backup",
            SyncMode::DropAndPurge => "offload",
        };
        let mode_input = prompt_line(
            &format!("Sync mode (backup [safe] / offload) [{}]", current_mode),
            Some(current_mode),
        )?;
        if mode_input.trim().eq_ignore_ascii_case("offload")
            || mode_input.trim().eq_ignore_ascii_case("drop_and_purge")
        {
            f.mode = SyncMode::DropAndPurge;
            f.empty_folder_protection = false;
        } else if mode_input.trim().eq_ignore_ascii_case("backup")
            || mode_input.trim().eq_ignore_ascii_case("continuous_backup")
        {
            f.mode = SyncMode::ContinuousBackup;
            f.empty_folder_protection = true;
        }
    } else {
        let f = &mut config.folders[folder_idx];
        if let Some(n) = new_name {
            f.name = n;
        }
        if let Some(p) = new_path {
            f.local_path = resolve_local_path(&p)?;
        }
        if let Some(t) = new_target {
            f.remote_target = t;
        }
        if let Some(m) = new_mode_str {
            match m.as_str() {
                "offload" | "drop_and_purge" => {
                    f.mode = SyncMode::DropAndPurge;
                    f.empty_folder_protection = false;
                }
                _ => {
                    f.mode = SyncMode::ContinuousBackup;
                    f.empty_folder_protection = true;
                }
            }
        }
        if let Some(s) = new_safety {
            f.empty_folder_protection = s;
        }
    }

    validate_and_save(&config, config_path)?;
    println!(
        "\x1B[32m✓\x1B[0m Updated folder '\x1B[1m{}\x1B[0m'.",
        config.folders[folder_idx].name
    );

    if reload {
        reload_daemon_and_report(socket).await;
    }

    Ok(())
}

/// Interactive Full Console Menu for managing folders.
pub async fn cmd_interactive(config_path: &Path, socket: Option<PathBuf>) -> Result<()> {
    if !stdin().is_terminal() {
        anyhow::bail!("Interactive folder console requires an interactive terminal.");
    }

    loop {
        let config = load_config(config_path)?;
        let daemon_running = is_daemon_running(socket.clone()).await;

        print!("\x1B[2J\x1B[1;1H"); // Clear screen
        println!("\x1B[1;36m╔═══════════════════════════════════════════════════════════════════════╗\x1B[0m");
        println!("\x1B[1;36m║                  syncd Folder Management Console                     ║\x1B[0m");
        println!("\x1B[1;36m╚═══════════════════════════════════════════════════════════════════════╝\x1B[0m");
        println!(
            "Config: \x1B[33m{}\x1B[0m ({} folder{} configured)",
            config_path.display(),
            config.folders.len(),
            if config.folders.len() == 1 { "" } else { "s" }
        );
        let daemon_status = if daemon_running {
            "\x1B[32m● Running (syncd.sock connected)\x1B[0m"
        } else {
            "\x1B[90m○ Stopped\x1B[0m"
        };
        println!("Daemon: {}\n", daemon_status);

        println!("  \x1B[1m1)\x1B[0m List all folders");
        println!("  \x1B[1m2)\x1B[0m Add a new folder \x1B[32m(Interactive Wizard)\x1B[0m");
        println!("  \x1B[1m3)\x1B[0m Show folder details");
        println!("  \x1B[1m4)\x1B[0m Edit a folder");
        println!("  \x1B[1m5)\x1B[0m Remove a folder");
        println!("  \x1B[1m6)\x1B[0m Reload syncd daemon");
        println!("  \x1B[1m7)\x1B[0m Exit\n");

        let choice = prompt_line("Select an option [1-7]", Some("1"))?;
        match choice.trim() {
            "1" => {
                println!();
                cmd_list(config_path, socket.clone(), false).await?;
                pause_prompt()?;
            }
            "2" => {
                println!();
                if let Err(e) = cmd_add(
                    config_path,
                    socket.clone(),
                    None,
                    None,
                    None,
                    None,
                    vec![],
                    None,
                    None,
                    None,
                    false,
                    true,
                    true,
                )
                .await
                {
                    println!("\x1B[31mError adding folder: {}\x1B[0m", e);
                }
                pause_prompt()?;
            }
            "3" => {
                println!();
                if let Err(e) = cmd_show(config_path, socket.clone(), None).await {
                    println!("\x1B[31mError: {}\x1B[0m", e);
                }
                pause_prompt()?;
            }
            "4" => {
                println!();
                if let Err(e) = cmd_edit(
                    config_path,
                    socket.clone(),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    true,
                    true,
                )
                .await
                {
                    println!("\x1B[31mError: {}\x1B[0m", e);
                }
                pause_prompt()?;
            }
            "5" => {
                println!();
                if let Err(e) = cmd_remove(config_path, socket.clone(), None, false, true).await {
                    println!("\x1B[31mError: {}\x1B[0m", e);
                }
                pause_prompt()?;
            }
            "6" => {
                println!();
                reload_daemon_and_report(socket.clone()).await;
                pause_prompt()?;
            }
            "7" | "q" | "exit" => {
                println!("\nGoodbye!");
                break;
            }
            _ => {
                println!("\x1B[31mInvalid choice. Please choose 1-7.\x1B[0m");
                std::thread::sleep(std::time::Duration::from_millis(600));
            }
        }
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Interactive Step-by-Step Add Wizard
// ─────────────────────────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn run_add_wizard(
    existing_config: &GlobalConfig,
    pre_name: Option<String>,
    pre_path: Option<PathBuf>,
    pre_target: Option<String>,
    pre_mode: Option<String>,
    pre_excludes: Vec<String>,
    pre_safety: Option<bool>,
    pre_endpoint: Option<String>,
    pre_region: Option<String>,
    pre_allow_http: bool,
) -> Result<FolderConfig> {
    println!("\x1B[1;34m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\x1B[0m");
    println!("\x1B[1m          New Folder Synchronization Wizard\x1B[0m");
    println!("\x1B[1;34m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\x1B[0m");
    println!("This wizard will guide you through configuring a watched folder.\n");

    // 1. Folder Name
    let name = loop {
        let default_name = pre_name.as_deref();
        let prompt_text = if let Some(d) = default_name {
            format!("1. Folder name [{}]", d)
        } else {
            "1. Folder name (e.g. 'documents', 'renders', 'work')".to_string()
        };
        let input = prompt_line(&prompt_text, default_name)?;
        let trimmed = input.trim();
        if trimmed.is_empty() {
            println!("  \x1B[31mFolder name cannot be empty.\x1B[0m");
            continue;
        }
        if existing_config
            .folders
            .iter()
            .any(|f| f.name.eq_ignore_ascii_case(trimmed))
        {
            println!(
                "  \x1B[31mA folder named '{}' already exists. Please choose a different name.\x1B[0m",
                trimmed
            );
            continue;
        }
        break trimmed.to_string();
    };

    // 2. Local Path
    let local_path = loop {
        let default_path_str = pre_path.as_ref().map(|p| p.display().to_string());
        let prompt_text = if let Some(d) = &default_path_str {
            format!("2. Local directory path [{}]", d)
        } else {
            "2. Local directory path (e.g. '~/Documents', '/home/user/Renders')".to_string()
        };
        let input = prompt_line(&prompt_text, default_path_str.as_deref())?;
        let trimmed = input.trim();
        if trimmed.is_empty() {
            println!("  \x1B[31mPath cannot be empty.\x1B[0m");
            continue;
        }

        let resolved = resolve_local_path(Path::new(trimmed))?;

        if !resolved.exists() {
            let create = prompt_bool(
                &format!("  Directory '{}' does not exist. Create it now?", resolved.display()),
                true,
            )?;
            if create {
                if let Err(e) = std::fs::create_dir_all(&resolved) {
                    println!("  \x1B[31mFailed to create directory: {}\x1B[0m", e);
                    continue;
                }
                println!("  \x1B[32mCreated directory.\x1B[0m");
            } else {
                continue;
            }
        }

        if !resolved.is_dir() {
            println!("  \x1B[31m'{}' is a file, not a directory.\x1B[0m", resolved.display());
            continue;
        }

        // Collision check against existing folders (exact and nested)
        let norm_new = sync_common::config::normalize_local_path(&resolved);
        let mut collision = None;
        for f in &existing_config.folders {
            let norm_ex = sync_common::config::normalize_local_path(&f.local_path);
            if norm_new == norm_ex {
                collision = Some(format!(
                    "Local directory '{}' is already watched by folder '{}'. You cannot sync the same folder twice.",
                    resolved.display(), f.name
                ));
                break;
            } else if norm_new.starts_with(&norm_ex) {
                collision = Some(format!(
                    "Local directory '{}' is inside existing watched folder '{}' ('{}'). Nested sync folders are not allowed.",
                    resolved.display(), f.name, f.local_path.display()
                ));
                break;
            } else if norm_ex.starts_with(&norm_new) {
                collision = Some(format!(
                    "Local directory '{}' contains existing watched folder '{}' ('{}'). You cannot watch a parent folder of an active sync folder.",
                    resolved.display(), f.name, f.local_path.display()
                ));
                break;
            }
        }

        if let Some(msg) = collision {
            println!("  \x1B[31m{}\x1B[0m", msg);
            continue;
        }

        break resolved;
    };

    // 3. Remote Target
    let remote_target = loop {
        let default_target = pre_target.as_deref();
        let prompt_text = if let Some(d) = default_target {
            format!("3. Remote cloud destination URI [{}]", d)
        } else {
            "3. Remote cloud destination URI\n   (e.g. 'gdrive://Documents', 's3://my-bucket/documents', 'gs://bucket/renders', 'file:///mnt/drive/backup')".to_string()
        };
        let input = prompt_line(&prompt_text, default_target)?;
        let trimmed = input.trim();
        if trimmed.is_empty() {
            println!("  \x1B[31mRemote target cannot be empty.\x1B[0m");
            continue;
        }
        if !trimmed.contains("://") {
            println!("  \x1B[31mRemote target must include a scheme (e.g. gdrive://, s3://, gs://, file://, az://).\x1B[0m");
            continue;
        }

        // Check for collisions with existing folders
        let norm_new = trimmed.trim_end_matches('/');
        let mut collision = false;
        for existing in &existing_config.folders {
            let norm_ex = existing.remote_target.trim_end_matches('/');
            if norm_new == norm_ex {
                println!(
                    "  \x1B[31mRemote target collides with existing folder '{}' ({})\x1B[0m",
                    existing.name, existing.remote_target
                );
                collision = true;
                break;
            }
        }
        if collision {
            continue;
        }

        break trimmed.to_string();
    };

    // 4. Mode
    println!("\n4. Select Synchronization Mode:");
    println!("   \x1B[1m[1] Continuous 1-Way Backup [Safety Locked] (Recommended)\x1B[0m");
    println!("       Permanent copy on disk: edits sync to cloud, Empty Folder Safety Lock prevents");
    println!("       accidental mass deletion, remote deletions safely archive to .Trash-syncd/.");
    println!("   \x1B[1m[2] Safe Offload (Drop & Purge)\x1B[0m");
    println!("       Frees local space: uploads to cloud, verifies SHA-256 + size, then removes local file.");

    let default_choice = match pre_mode.as_deref() {
        Some("offload") | Some("drop_and_purge") => "2",
        _ => "1",
    };
    let mode_choice = prompt_line("Choice [1/2]", Some(default_choice))?;
    let mode = if mode_choice.trim() == "2" {
        SyncMode::DropAndPurge
    } else {
        SyncMode::ContinuousBackup
    };

    // 5. Exclude Patterns
    let default_excludes_str = if pre_excludes.is_empty() {
        ".git/, target/, node_modules/, *.tmp, *.swp, *~".to_string()
    } else {
        pre_excludes.join(", ")
    };
    let prompt_text = format!(
        "\n5. Exclude patterns (comma-separated, Enter for defaults)\n   [{}]",
        default_excludes_str
    );
    let excludes_input = prompt_line(&prompt_text, Some(&default_excludes_str))?;
    let exclude_patterns = if excludes_input.trim().eq_ignore_ascii_case("none") {
        vec![]
    } else if excludes_input.trim() == default_excludes_str {
        if pre_excludes.is_empty() {
            vec![
                ".git/".to_string(),
                "target/".to_string(),
                "node_modules/".to_string(),
                "*.tmp".to_string(),
                "*.swp".to_string(),
                "*~".to_string(),
            ]
        } else {
            pre_excludes
        }
    } else {
        excludes_input
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    };

    // Safety Lock is automatically tied to mode
    let safety_lock = pre_safety.unwrap_or(mode == SyncMode::ContinuousBackup);

    // 6. Advanced options
    let configure_advanced = prompt_bool(
        "\n6. Configure advanced S3 options (endpoint, region, allow-http)?",
        false,
    )?;

    let (endpoint, region, allow_http) = if configure_advanced {
        let ep = prompt_line("   Custom S3 Endpoint (optional, Enter to skip)", None)?;
        let reg = prompt_line("   S3 Region (optional, Enter to skip)", None)?;
        let http = prompt_bool("   Allow unencrypted HTTP?", false)?;
        (
            if ep.trim().is_empty() { None } else { Some(ep.trim().to_string()) },
            if reg.trim().is_empty() { None } else { Some(reg.trim().to_string()) },
            http,
        )
    } else {
        (pre_endpoint, pre_region, pre_allow_http)
    };

    // Review Summary
    println!("\n\x1B[1mConfiguration Review:\x1B[0m");
    println!("  Name:         \x1B[32m{}\x1B[0m", name);
    println!("  Local Path:   {}", local_path.display());
    println!("  Target:       {}", remote_target);
    println!(
        "  Mode:         {}",
        match (mode, safety_lock) {
            (SyncMode::ContinuousBackup, true) => "Continuous 1-Way Backup [Safety Locked]",
            (SyncMode::ContinuousBackup, false) => "Continuous 1-Way Backup [Safety Off]",
            (SyncMode::DropAndPurge, _) => "Safe Drop & Purge (Offload)",
        }
    );
    println!("  Excludes:     {}", exclude_patterns.join(", "));
    if let Some(ep) = &endpoint {
        println!("  Endpoint:     {}", ep);
    }

    let confirm = prompt_bool("\nSave and activate this folder?", true)?;
    if !confirm {
        anyhow::bail!("Folder creation cancelled by user.");
    }

    Ok(FolderConfig {
        id: Uuid::new_v4(),
        name,
        local_path,
        remote_target,
        mode,
        exclude_patterns,
        empty_folder_protection: safety_lock,
        endpoint,
        region,
        allow_http,
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Path & Configuration Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn load_config(config_path: &Path) -> Result<GlobalConfig> {
    if config_path.exists() {
        GlobalConfig::from_file(config_path)
            .with_context(|| format!("Failed to parse config at '{}'", config_path.display()))
    } else {
        Ok(GlobalConfig::default())
    }
}

fn validate_and_save(config: &GlobalConfig, config_path: &Path) -> Result<()> {
    let results = sync_common::config::validate_config(config);
    for res in results {
        if let ValidationResult::Error(err) = res {
            anyhow::bail!("Configuration validation error: {}", err);
        }
    }

    config
        .save_to_file(config_path)
        .with_context(|| format!("Failed writing configuration to '{}'", config_path.display()))?;

    Ok(())
}

fn find_folder_index(config: &GlobalConfig, target: &str) -> Option<usize> {
    let trimmed = target.trim();
    if let Ok(id) = Uuid::parse_str(trimmed) {
        if let Some(idx) = config.folders.iter().position(|f| f.id == id) {
            return Some(idx);
        }
    }
    // Match exact name (case-insensitive)
    if let Some(idx) = config
        .folders
        .iter()
        .position(|f| f.name.eq_ignore_ascii_case(trimmed))
    {
        return Some(idx);
    }
    // Match prefix / substring if unique
    let matches: Vec<usize> = config
        .folders
        .iter()
        .enumerate()
        .filter(|(_, f)| f.name.to_lowercase().contains(&trimmed.to_lowercase()))
        .map(|(i, _)| i)
        .collect();

    if matches.len() == 1 {
        Some(matches[0])
    } else {
        None
    }
}

/// Expands `~` to $HOME and resolves relative paths to absolute paths.
pub fn resolve_local_path(path: &Path) -> Result<PathBuf> {
    let path_str = path.to_string_lossy();
    let expanded = if path_str.starts_with("~/") || path_str == "~" {
        let home = std::env::var("HOME").context("HOME environment variable not set")?;
        if path_str == "~" {
            PathBuf::from(home)
        } else {
            PathBuf::from(home).join(path_str.trim_start_matches("~/"))
        }
    } else {
        path.to_path_buf()
    };

    if expanded.is_absolute() {
        Ok(expanded)
    } else {
        let current_dir = std::env::current_dir().context("Failed to get current directory")?;
        Ok(current_dir.join(expanded))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Daemon IPC interaction helpers
// ─────────────────────────────────────────────────────────────────────────────

fn default_socket_path() -> PathBuf {
    if let Ok(runtime) = std::env::var("XDG_RUNTIME_DIR") {
        PathBuf::from(runtime).join("syncd.sock")
    } else {
        let uid = unsafe { libc::getuid() };
        PathBuf::from(format!("/run/user/{}/syncd.sock", uid))
    }
}

pub async fn is_daemon_running(socket_override: Option<PathBuf>) -> bool {
    let socket_path = socket_override.unwrap_or_else(default_socket_path);
    UnixStream::connect(&socket_path).await.is_ok()
}

pub async fn fetch_status_quiet(
    socket_override: Option<PathBuf>,
) -> Option<sync_common::state::TelemetrySnapshot> {
    let socket_path = socket_override.unwrap_or_else(default_socket_path);
    let stream = UnixStream::connect(&socket_path).await.ok()?;
    let (mut reader, mut writer) = client_framed_split(stream);

    writer.send(IpcRequest::GetStatus).await.ok()?;
    match reader.next().await {
        Some(Ok(IpcResponse::StatusSnapshot(snap))) => Some(snap),
        _ => None,
    }
}

pub async fn reload_daemon_and_report(socket_override: Option<PathBuf>) -> bool {
    let socket_path = socket_override.unwrap_or_else(default_socket_path);
    match UnixStream::connect(&socket_path).await {
        Ok(stream) => {
            let (mut reader, mut writer) = client_framed_split(stream);
            if let Err(e) = writer.send(IpcRequest::ReloadConfig).await {
                eprintln!("\x1B[33m⚠ Failed sending reload command to syncd: {}\x1B[0m", e);
                return false;
            }
            match reader.next().await {
                Some(Ok(IpcResponse::Ok { message })) => {
                    println!("\x1B[32m✓ syncd daemon: {}\x1B[0m", message);
                    true
                }
                Some(Ok(IpcResponse::Error { message, .. })) => {
                    eprintln!("\x1B[31m✗ syncd daemon reload error: {}\x1B[0m", message);
                    false
                }
                _ => {
                    println!("\x1B[32m✓ syncd daemon notified.\x1B[0m");
                    true
                }
            }
        }
        Err(_) => {
            println!(
                "\x1B[90mℹ syncd daemon is not currently running. Changes will take effect when it starts.\x1B[0m"
            );
            false
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Terminal prompt helpers
// ─────────────────────────────────────────────────────────────────────────────

fn prompt_line(prompt: &str, default: Option<&str>) -> Result<String> {
    print!("{} ", prompt);
    stdout().flush()?;
    let mut buf = String::new();
    stdin().read_line(&mut buf)?;
    let trimmed = buf.trim().to_string();
    if trimmed.is_empty() {
        if let Some(d) = default {
            return Ok(d.to_string());
        }
    }
    Ok(trimmed)
}

fn prompt_bool(prompt: &str, default: bool) -> Result<bool> {
    let hint = if default { "[Y/n]" } else { "[y/N]" };
    print!("{} {} ", prompt, hint);
    stdout().flush()?;
    let mut buf = String::new();
    stdin().read_line(&mut buf)?;
    let trimmed = buf.trim();
    if trimmed.is_empty() {
        return Ok(default);
    }
    Ok(trimmed.eq_ignore_ascii_case("y")
        || trimmed.eq_ignore_ascii_case("yes")
        || trimmed.eq_ignore_ascii_case("true"))
}

fn pause_prompt() -> Result<()> {
    print!("\nPress Enter to continue...");
    stdout().flush()?;
    let mut buf = String::new();
    stdin().read_line(&mut buf)?;
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_local_path_absolute() {
        let p = Path::new("/var/log/syncd");
        let res = resolve_local_path(p).unwrap();
        assert_eq!(res, PathBuf::from("/var/log/syncd"));
    }

    #[test]
    fn test_resolve_local_path_tilde() {
        let home = std::env::var("HOME").unwrap();
        let p = Path::new("~/my_folder");
        let res = resolve_local_path(p).unwrap();
        assert_eq!(res, PathBuf::from(home).join("my_folder"));
    }

    #[test]
    fn test_find_folder_index() {
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let config = GlobalConfig {
            folders: vec![
                FolderConfig {
                    id: id1,
                    name: "documents".to_string(),
                    local_path: PathBuf::from("/tmp/docs"),
                    remote_target: "s3://b/docs".to_string(),
                    ..Default::default()
                },
                FolderConfig {
                    id: id2,
                    name: "renders".to_string(),
                    local_path: PathBuf::from("/tmp/renders"),
                    remote_target: "s3://b/renders".to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        // Match by UUID
        assert_eq!(find_folder_index(&config, &id1.to_string()), Some(0));
        assert_eq!(find_folder_index(&config, &id2.to_string()), Some(1));

        // Match by name
        assert_eq!(find_folder_index(&config, "documents"), Some(0));
        assert_eq!(find_folder_index(&config, "DOCUMENTS"), Some(0));
        assert_eq!(find_folder_index(&config, "renders"), Some(1));

        // Substring
        assert_eq!(find_folder_index(&config, "rend"), Some(1));

        // Not found
        assert_eq!(find_folder_index(&config, "photos"), None);
    }

    #[tokio::test]
    async fn test_folder_add_and_remove_workflow() {
        let temp_dir = std::env::temp_dir().join(format!("syncd_ctl_test_{}", Uuid::new_v4()));
        let config_file = temp_dir.join("config.toml");
        let local_watch = temp_dir.join("my_watched_folder");
        std::fs::create_dir_all(&local_watch).unwrap();

        // 1. Add folder via cmd_add
        cmd_add(
            &config_file,
            None,
            Some("test_docs".to_string()),
            Some(local_watch.clone()),
            Some("s3://bucket/test_docs".to_string()),
            Some("backup".to_string()),
            vec!["*.log".to_string()],
            Some(true),
            None,
            None,
            false,
            false,
            false, // no reload
        )
        .await
        .expect("cmd_add should succeed");

        // Verify config on disk
        let loaded = GlobalConfig::from_file(&config_file).expect("config file should exist");
        assert_eq!(loaded.folders.len(), 1);
        assert_eq!(loaded.folders[0].name, "test_docs");
        assert_eq!(loaded.folders[0].local_path, local_watch);
        assert_eq!(loaded.folders[0].remote_target, "s3://bucket/test_docs");
        assert_eq!(loaded.folders[0].mode, SyncMode::ContinuousBackup);
        assert!(loaded.folders[0].empty_folder_protection);

        // 2. Remove folder via cmd_remove
        cmd_remove(
            &config_file,
            None,
            Some("test_docs".to_string()),
            true,  // yes (skip confirmation)
            false, // no reload
        )
        .await
        .expect("cmd_remove should succeed");

        let reloaded = GlobalConfig::from_file(&config_file).expect("config should still exist");
        assert_eq!(reloaded.folders.len(), 0);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_folder_edit_workflow() {
        let temp_dir = std::env::temp_dir().join(format!("syncd_ctl_test_{}", Uuid::new_v4()));
        let config_file = temp_dir.join("config.toml");
        let local_watch = temp_dir.join("my_renders");
        std::fs::create_dir_all(&local_watch).unwrap();

        // Add
        cmd_add(
            &config_file,
            None,
            Some("renders".to_string()),
            Some(local_watch.clone()),
            Some("s3://bucket/renders".to_string()),
            Some("backup".to_string()),
            vec![],
            None,
            None,
            None,
            false,
            false,
            false,
        )
        .await
        .unwrap();

        // Edit mode to offload and target to gs://
        cmd_edit(
            &config_file,
            None,
            Some("renders".to_string()),
            Some("renders_offload".to_string()),
            None,
            Some("gs://archive/renders".to_string()),
            Some("offload".to_string()),
            Some(false),
            false,
            false,
        )
        .await
        .unwrap();

        let updated = GlobalConfig::from_file(&config_file).unwrap();
        assert_eq!(updated.folders.len(), 1);
        assert_eq!(updated.folders[0].name, "renders_offload");
        assert_eq!(updated.folders[0].remote_target, "gs://archive/renders");
        assert_eq!(updated.folders[0].mode, SyncMode::DropAndPurge);
        assert!(!updated.folders[0].empty_folder_protection);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_cli_folder_command_parsing() {
        use clap::Parser;

        // Test `syncctl folder add ...`
        let cli = crate::Cli::try_parse_from([
            "syncctl",
            "folder",
            "add",
            "--name",
            "photos",
            "--path",
            "/tmp",
            "--target",
            "s3://bucket/photos",
            "--mode",
            "backup",
        ])
        .expect("parse should succeed");

        assert!(matches!(
            cli.command,
            Some(crate::Commands::Folder(FolderArgs {
                command: Some(FolderCommands::Add { .. })
            }))
        ));

        // Test `syncctl folder list`
        let cli_ls = crate::Cli::try_parse_from(["syncctl", "folder", "list", "--verbose"])
            .expect("parse should succeed");
        assert!(matches!(
            cli_ls.command,
            Some(crate::Commands::Folder(FolderArgs {
                command: Some(FolderCommands::List { verbose: true })
            }))
        ));

        // Test `syncctl folder remove docs -y`
        let cli_rm = crate::Cli::try_parse_from(["syncctl", "folder", "remove", "docs", "-y"])
            .expect("parse should succeed");
        assert!(matches!(
            cli_rm.command,
            Some(crate::Commands::Folder(FolderArgs {
                command: Some(FolderCommands::Remove { yes: true, .. })
            }))
        ));

        // Test `syncctl reload`
        let cli_reload = crate::Cli::try_parse_from(["syncctl", "reload"])
            .expect("parse should succeed");
        assert!(matches!(cli_reload.command, Some(crate::Commands::Reload)));
    }

    #[tokio::test]
    async fn test_reload_daemon_and_report_with_ipc_server() {
        use sync_common::ipc::framed_split;
        use tokio::net::UnixListener;

        let temp_dir = std::env::temp_dir().join(format!("syncd_ipc_test_{}", Uuid::new_v4()));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let sock_path = temp_dir.join("test.sock");

        let listener = UnixListener::bind(&sock_path).unwrap();

        // Spawn mock server
        let server_task = tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let (mut reader, mut writer) = framed_split(stream);
                if let Some(Ok(IpcRequest::ReloadConfig)) = reader.next().await {
                    let _ = writer
                        .send(IpcResponse::Ok {
                            message: "Configuration reloaded successfully (1 active folder)".into(),
                        })
                        .await;
                }
            }
        });

        // Client reload
        reload_daemon_and_report(Some(sock_path.clone())).await;

        let _ = server_task.await;
        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
