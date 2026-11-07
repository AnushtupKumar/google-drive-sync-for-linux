//! `syncctl setup` — Interactive First-Time Setup & Onboarding Wizard.
//!
//! Guides new users through cloud storage provider authentication, initial folder
//! configuration, safety settings, and starting the background syncd daemon.

use anyhow::{Context, Result};
use std::{
    path::PathBuf,
    process::Command,
};
use sync_common::config::{FolderConfig, GlobalConfig, SyncMode};
use uuid::Uuid;

use crate::auth::{self, prompt_input, StoredGoogleCredentials};

pub async fn run_setup_wizard(
    config_override: Option<PathBuf>,
    socket_override: Option<PathBuf>,
) -> Result<()> {
    println!("\n\x1B[1;36m╔════════════════════════════════════════════════════════════════╗\x1B[0m");
    println!("\x1B[1;36m║               syncd — First-Time Setup Wizard                  ║\x1B[0m");
    println!("\x1B[1;36m║         Linux Cloud Synchronization & File Offloading          ║\x1B[0m");
    println!("\x1B[1;36m╚════════════════════════════════════════════════════════════════╝\x1B[0m\n");

    println!("Welcome to syncd! This interactive wizard will help you:");
    println!("  1. Authenticate with your cloud storage provider");
    println!("  2. Configure your first watched folder and sync mode");
    println!("  3. Set up the automated background daemon service\n");

    let cfg_path = config_override.unwrap_or_else(GlobalConfig::default_config_path);
    let mut config = if cfg_path.exists() {
        println!("Note: Existing configuration found at '{}'.", cfg_path.display());
        let ans = prompt_input("Would you like to add a new folder to this configuration? [Y/n]", Some("Y"))?;
        if !ans.eq_ignore_ascii_case("y") && !ans.eq_ignore_ascii_case("yes") {
            println!("Setup cancelled.");
            return Ok(());
        }
        GlobalConfig::from_file(&cfg_path).unwrap_or_default()
    } else {
        GlobalConfig::default()
    };

    // ─────────────────────────────────────────────
    // Step 1: Cloud Storage Provider & Auth
    // ─────────────────────────────────────────────
    println!("\n\x1B[1;33m=== Step 1: Select Your Cloud Storage Provider ===\x1B[0m");
    println!("  1. Google Drive  (gdrive://...) — Pure Rust native Drive v3 integration");
    println!("  2. AWS S3 / Cloudflare R2 / MinIO (s3://...) — S3-compatible object storage");
    println!("  3. Google Cloud Storage (gs://...) — GCP buckets with Service Account / ADC");
    println!("  4. Local Disk / External Drive / NAS (file://...) — Local paths");

    let provider_choice = prompt_input("Select provider [1-4]", Some("1"))?;
    let default_remote_prefix = match provider_choice.trim() {
        "1" => {
            println!("\n\x1B[1m--- Google Drive Authentication ---\x1B[0m");
            if let Some(creds) = StoredGoogleCredentials::load() {
                println!("\x1B[32m✓ Google Drive is already authenticated!\x1B[0m (Client ID: {}...)", &creds.client_id[..creds.client_id.len().min(12)]);
                let reauth = prompt_input("Would you like to re-authenticate with a different account? [y/N]", Some("N"))?;
                if reauth.eq_ignore_ascii_case("y") || reauth.eq_ignore_ascii_case("yes") {
                    auth::login_gdrive(None, None, false).await?;
                }
            } else {
                auth::login_gdrive(None, None, false).await?;
            }
            "gdrive://Documents".to_string()
        }
        "2" => {
            println!("\n\x1B[1m--- AWS S3 / Cloudflare R2 / MinIO ---\x1B[0m");
            let bucket = prompt_input("Enter your S3 bucket name", Some("my-backups"))?;
            let custom_ep = prompt_input("Custom endpoint URL (press Enter for standard AWS S3)", None)?;

            if !custom_ep.is_empty() {
                println!("  To use S3/R2 with syncd, make sure these environment variables are set:");
                println!("    export AWS_ACCESS_KEY_ID=\"...\"");
                println!("    export AWS_SECRET_ACCESS_KEY=\"...\"");
                println!("    export AWS_ENDPOINT_URL=\"{}\"", custom_ep);
            }
            format!("s3://{}/documents", bucket)
        }
        "3" => {
            println!("\n\x1B[1m--- Google Cloud Storage (GCS) ---\x1B[0m");
            let bucket = prompt_input("Enter your GCS bucket name", Some("my-gcs-bucket"))?;
            println!("  Make sure GOOGLE_APPLICATION_CREDENTIALS points to your Service Account JSON:");
            println!("    export GOOGLE_APPLICATION_CREDENTIALS=\"~/.config/gcloud/key.json\"");
            format!("gs://{}/documents", bucket)
        }
        "4" => {
            println!("\n\x1B[1m--- Local / NAS Storage ---\x1B[0m");
            let target_path = prompt_input("Destination directory path", Some("/mnt/backup_drive/syncd_backups"))?;
            format!("file://{}", target_path)
        }
        _ => {
            println!("Defaulting to Google Drive.");
            "gdrive://Documents".to_string()
        }
    };

    // ─────────────────────────────────────────────
    // Step 2: First Sync Folder
    // ─────────────────────────────────────────────
    println!("\n\x1B[1;33m=== Step 2: Configure Your Sync Folder ===\x1B[0m");

    // 1. Folder Name (unique check)
    let folder_name = loop {
        let input = prompt_input("Folder identifier name", Some("documents"))?;
        let trimmed = input.trim();
        if trimmed.is_empty() {
            println!("  \x1B[31mName cannot be empty.\x1B[0m");
            continue;
        }
        if config.folders.iter().any(|f| f.name.eq_ignore_ascii_case(trimmed)) {
            println!(
                "  \x1B[31mA folder named '{}' already exists. Please choose a different name.\x1B[0m",
                trimmed
            );
            continue;
        }
        break trimmed.to_string();
    };

    // 2. Local Path (unique & nesting check)
    let local_path = loop {
        let local_raw = prompt_input("Local folder path to synchronize", Some("~/Documents"))?;
        let trimmed = local_raw.trim();
        if trimmed.is_empty() {
            println!("  \x1B[31mPath cannot be empty.\x1B[0m");
            continue;
        }

        let resolved = resolve_path(trimmed);

        if !resolved.exists() {
            println!("  Local directory '{}' does not exist yet.", resolved.display());
            let create_ans = prompt_input("  Create it now? [Y/n]", Some("Y"))?;
            if create_ans.eq_ignore_ascii_case("y") || create_ans.eq_ignore_ascii_case("yes") {
                if let Err(e) = std::fs::create_dir_all(&resolved) {
                    println!("  \x1B[31mFailed to create directory: {}\x1B[0m", e);
                    continue;
                }
                println!("  \x1B[32m✓ Created local folder '{}'\x1B[0m", resolved.display());
            } else {
                continue;
            }
        }

        if !resolved.is_dir() {
            println!("  \x1B[31m'{}' is a file, not a directory.\x1B[0m", resolved.display());
            continue;
        }

        // Collision check against existing folders
        let norm_new = sync_common::config::normalize_local_path(&resolved);
        let mut collision = None;
        for f in &config.folders {
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

    // 3. Remote Target (collision check)
    let remote_target = loop {
        let input = prompt_input("Remote destination cloud URI", Some(&default_remote_prefix))?;
        let trimmed = input.trim();
        if trimmed.is_empty() {
            println!("  \x1B[31mRemote target cannot be empty.\x1B[0m");
            continue;
        }
        if !trimmed.contains("://") {
            println!("  \x1B[31mRemote target must include a scheme (e.g. gdrive://, s3://, gs://, file://).\x1B[0m");
            continue;
        }

        let norm_new = trimmed.trim_end_matches('/');
        let mut collision = None;
        for f in &config.folders {
            let norm_ex = f.remote_target.trim_end_matches('/');
            if norm_new == norm_ex {
                collision = Some(format!(
                    "Remote target '{}' collides with existing folder '{}' ({}).",
                    trimmed, f.name, f.remote_target
                ));
                break;
            } else if norm_new.starts_with(norm_ex) && norm_new.as_bytes().get(norm_ex.len()) == Some(&b'/') {
                collision = Some(format!(
                    "Remote target '{}' overlaps with existing folder '{}' ({}).",
                    trimmed, f.name, f.remote_target
                ));
                break;
            } else if norm_ex.starts_with(norm_new) && norm_ex.as_bytes().get(norm_new.len()) == Some(&b'/') {
                collision = Some(format!(
                    "Remote target '{}' contains existing folder '{}' ({}).",
                    trimmed, f.name, f.remote_target
                ));
                break;
            }
        }

        if let Some(msg) = collision {
            println!("  \x1B[31m{}\x1B[0m", msg);
            continue;
        }

        break trimmed.to_string();
    };

    println!("\n\x1B[1mChoose Synchronization Mode:\x1B[0m");
    println!("  1. Continuous 1-Way Backup [Safety Locked] (Recommended)");
    println!("     Keeps files permanently on disk, mirrors edits to cloud, automatically arms");
    println!("     Empty Folder Safety Lock, and safely archives deletions to .Trash-syncd/.");
    println!("  2. Safe Drop & Purge Offload");
    println!("     Intended for renders, raw archives, and downloads; safely frees local disk");
    println!("     space ONLY after 5-stage TOCTOU cloud upload verification.");

    let mode_choice = prompt_input("Select mode [1/2]", Some("1"))?;
    let (mode, empty_protect) = match mode_choice.trim() {
        "2" => (SyncMode::DropAndPurge, false),
        _ => (SyncMode::ContinuousBackup, true),
    };

    let folder_id = Uuid::new_v4();
    let folder = FolderConfig {
        id: folder_id,
        name: folder_name.clone(),
        local_path: local_path.clone(),
        remote_target: remote_target.clone(),
        mode,
        exclude_patterns: vec![
            ".git/".into(),
            "target/".into(),
            "node_modules/".into(),
            "*.tmp".into(),
            "*.swp".into(),
            "*~".into(),
        ],
        empty_folder_protection: empty_protect,
        endpoint: None,
        region: None,
        allow_http: false,
    };

    config.folders.push(folder);

    // Validate entire configuration before saving
    config.validate().with_context(|| "Configuration validation failed")?;

    // ─────────────────────────────────────────────
    // Step 3: Save Configuration
    // ─────────────────────────────────────────────
    println!("\n\x1B[1;33m=== Step 3: Saving Configuration ===\x1B[0m");
    config.save_to_file(&cfg_path)
        .with_context(|| format!("Failed to write config file to '{}'", cfg_path.display()))?;
    println!("\x1B[32m✓ Saved configuration to '{}'\x1B[0m", cfg_path.display());

    // ─────────────────────────────────────────────
    // Step 4: Daemon & Background Service
    // ─────────────────────────────────────────────
    println!("\n\x1B[1;33m=== Step 4: Start Background Daemon ===\x1B[0m");

    // Try live reload via IPC first if daemon is already running
    let reloaded = crate::folder::reload_daemon_and_report(socket_override).await;
    if reloaded {
        println!("\x1B[32m✓ syncd daemon was running and loaded new configuration live!\x1B[0m");
    } else {
        // Offer to enable and start systemd service
        println!("syncd runs continuously in the background via a systemd user service.");
        let start_ans = prompt_input("Enable and start the syncd user service now? [Y/n]", Some("Y"))?;
        if start_ans.eq_ignore_ascii_case("y") || start_ans.eq_ignore_ascii_case("yes") {
            let res = Command::new("systemctl")
                .args(["--user", "enable", "--now", "syncd"])
                .status();
            match res {
                Ok(status) if status.success() => {
                    println!("\x1B[32m✓ syncd.service enabled and started!\x1B[0m");
                }
                _ => {
                    println!("\x1B[33mNote: Could not start systemd service automatically. You can start it manually with:\x1B[0m");
                    println!("  systemctl --user enable --now syncd");
                }
            }
        }
    }

    // ─────────────────────────────────────────────
    // Step 5: Summary & Next Steps
    // ─────────────────────────────────────────────
    println!("\n\x1B[1;32m╔════════════════════════════════════════════════════════════════╗\x1B[0m");
    println!("\x1B[1;32m║                  Setup Successfully Completed!                ║\x1B[0m");
    println!("\x1B[1;32m╚════════════════════════════════════════════════════════════════╝\x1B[0m\n");

    println!("Your folder '\x1B[1m{}\x1B[0m' is now configured:", folder_name);
    println!("  Local:   {}", local_path.display());
    println!("  Remote:  {}", remote_target);
    let mode_str = match mode {
        SyncMode::ContinuousBackup => "Continuous 1-Way Backup [Safety Locked]",
        SyncMode::DropAndPurge => "Safe Drop & Purge Offload",
    };
    println!("  Mode:    {}", mode_str);

    println!("\nHelpful Commands:");
    println!("  • Open Terminal Dashboard:   \x1B[1;36msyncctl\x1B[0m");
    println!("  • Live Transfer Progress:    \x1B[1;36msyncctl watch\x1B[0m");
    println!("  • Check Daemon Status:       \x1B[1;36msyncctl status\x1B[0m");
    println!("  • Manage Watched Folders:    \x1B[1;36msyncctl folder\x1B[0m");
    println!("  • Cloud Auth & Credentials:  \x1B[1;36msyncctl auth status\x1B[0m");
    println!();

    Ok(())
}

fn resolve_path(raw: &str) -> PathBuf {
    let trimmed = raw.trim();
    if let Some(stripped) = trimmed.strip_prefix("~/") {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
        PathBuf::from(home).join(stripped)
    } else if trimmed == "~" {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
        PathBuf::from(home)
    } else {
        PathBuf::from(trimmed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_path() {
        let abs = resolve_path("/var/log");
        assert_eq!(abs, PathBuf::from("/var/log"));

        let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
        let tilde_path = resolve_path("~/Documents/test");
        assert_eq!(tilde_path, PathBuf::from(&home).join("Documents/test"));

        let just_tilde = resolve_path("~");
        assert_eq!(just_tilde, PathBuf::from(&home));
    }
}
