//! Configuration structs and startup / reload-time validation logic.
//!
//! Deserialized from `~/.config/syncd/config.toml` by both `syncd` (which
//! owns the validated, live config) and `syncctl` (read-only presentation).

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use thiserror::Error;
use uuid::Uuid;

// ─────────────────────────────────────────────
// Enums
// ─────────────────────────────────────────────

/// Determines what `syncd` does after a successful upload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SyncMode {
    /// Continuous 1-way backup.  Remote deletions go to `.syncd_trash/` first.
    ContinuousBackup,
    /// Safe "Drop & Purge" offload: upload → verify → re-stat → unlink → prune.
    DropAndPurge,
}

impl std::fmt::Display for SyncMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncMode::ContinuousBackup => write!(f, "continuous_backup"),
            SyncMode::DropAndPurge => write!(f, "drop_and_purge"),
        }
    }
}

// ─────────────────────────────────────────────
// Configuration structs
// ─────────────────────────────────────────────

/// Top-level daemon configuration, read from `~/.config/syncd/config.toml`.
///
/// All `Option<T>` fields fall back to documented defaults when absent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalConfig {
    /// Unix socket path for IPC.  Default: `$XDG_RUNTIME_DIR/syncd.sock`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub socket_path: Option<PathBuf>,

    /// Directory used for SQLite database and runtime state.
    /// Default: `~/.local/share/syncd`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_dir: Option<PathBuf>,

    /// Directory for structured log files.
    /// Default: `~/.local/state/syncd/logs`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_dir: Option<PathBuf>,

    /// Maximum size in bytes before a log file is rotated.
    /// Default: `26_214_400` (25 MiB).
    #[serde(default = "default_log_max_size")]
    pub log_max_size_bytes: u64,

    /// Number of rotated (gzip-compressed) log archives to keep.
    /// Default: `10`.
    #[serde(default = "default_log_max_archives")]
    pub log_max_archives: usize,

    /// Maximum number of concurrent cloud transfer tasks across all folders.
    /// Default: `4`.
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent_transfers: usize,

    /// Global upload bandwidth cap in bytes-per-second.  `0` = unlimited.
    #[serde(default)]
    pub default_bandwidth_limit_bps: u64,

    /// Time (ms) a path must stay quiet before it is flushed to the DB queue.
    /// Default: `3 000` (3 s).
    #[serde(default = "default_debounce_quiet_ms")]
    pub debounce_quiet_ms: u64,

    /// Hard maximum wait (ms) before a continuously-written file is flushed.
    /// Default: `60 000` (60 s).
    #[serde(default = "default_debounce_max_wait_ms")]
    pub debounce_max_wait_ms: u64,

    /// One entry per watched folder / offload target.
    #[serde(default)]
    pub folders: Vec<FolderConfig>,
}

impl GlobalConfig {
    /// Returns the resolved socket path, preferring `socket_path` from config
    /// then falling back to `$XDG_RUNTIME_DIR/syncd.sock`.
    pub fn resolved_socket_path(&self) -> PathBuf {
        if let Some(p) = &self.socket_path {
            return p.clone();
        }
        let runtime = std::env::var("XDG_RUNTIME_DIR")
            // SAFETY: getuid() never fails; see libc_getuid below.
            .unwrap_or_else(|_| format!("/run/user/{}", unsafe { libc_getuid() }));
        PathBuf::from(runtime).join("syncd.sock")
    }

    /// Returns the resolved state directory.
    pub fn resolved_state_dir(&self) -> PathBuf {
        self.state_dir.clone().unwrap_or_else(|| {
            dirs_next_data_dir().join("syncd")
        })
    }

    /// Returns the resolved log directory.
    pub fn resolved_log_dir(&self) -> PathBuf {
        self.log_dir.clone().unwrap_or_else(|| {
            dirs_next_state_dir().join("syncd").join("logs")
        })
    }

    /// Returns the resolved SQLite database path.
    pub fn db_path(&self) -> PathBuf {
        self.resolved_state_dir().join("syncd.db")
    }

    /// Returns the default log file path.
    pub fn log_file_path(&self) -> PathBuf {
        self.resolved_log_dir().join("syncd.log")
    }

    /// Returns the default config file path ($XDG_CONFIG_HOME/syncd/config.toml or ~/.config/syncd/config.toml).
    pub fn default_config_path() -> PathBuf {
        if let Ok(cfg_dir) = std::env::var("XDG_CONFIG_HOME") {
            PathBuf::from(cfg_dir).join("syncd/config.toml")
        } else {
            let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
            PathBuf::from(home).join(".config/syncd/config.toml")
        }
    }

    /// Serialize GlobalConfig to a pretty-formatted TOML string.
    pub fn to_string_pretty(&self) -> Result<String, toml::ser::Error> {
        toml::to_string_pretty(self)
    }

    /// Save GlobalConfig to a TOML file atomically (writes to .tmp then renames).
    pub fn save_to_file(&self, path: &std::path::Path) -> Result<(), std::io::Error> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let serialized = self.to_string_pretty()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let header = "# ~/.config/syncd/config.toml\n# Managed automatically by syncctl or manual editing\n\n";
        let content = format!("{}{}", header, serialized);

        let tmp_path = path.with_extension("tmp");
        std::fs::write(&tmp_path, content)?;
        std::fs::rename(&tmp_path, path)?;
        Ok(())
    }

    /// Parse a GlobalConfig from a TOML file.
    pub fn from_file(path: &std::path::Path) -> Result<Self, FileConfigError> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        Ok(config)
    }

    /// Validates the configuration, returning Ok(()) if no fatal errors were found.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let results = validate_config(self);
        for res in results {
            if let ValidationResult::Error(err) = res {
                return Err(err);
            }
        }
        Ok(())
    }
}

impl Default for GlobalConfig {
    fn default() -> Self {
        Self {
            socket_path: None,
            state_dir: None,
            log_dir: None,
            log_max_size_bytes: default_log_max_size(),
            log_max_archives: default_log_max_archives(),
            max_concurrent_transfers: default_max_concurrent(),
            default_bandwidth_limit_bps: 0,
            debounce_quiet_ms: default_debounce_quiet_ms(),
            debounce_max_wait_ms: default_debounce_max_wait_ms(),
            folders: Vec::new(),
        }
    }
}

/// Per-folder synchronisation target.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FolderConfig {
    /// Stable identity UUID — must not change between reloads.
    pub id: Uuid,

    /// Human-readable label used in logs and `syncctl status` output.
    pub name: String,

    /// Absolute path to the directory being watched/offloaded.
    pub local_path: PathBuf,

    /// Cloud destination URI, e.g. `"s3://my-bucket/backups/photos"`.
    pub remote_target: String,

    /// How to handle successful uploads.
    pub mode: SyncMode,

    /// Gitignore-style patterns; `.syncd-anchor` and `.syncd-tmp-*` are
    /// always appended internally and do not need to appear here.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclude_patterns: Vec<String>,

    /// When `true` (default), the Empty Folder Safety Lock is active.
    /// Automatically bypassed for `DropAndPurge` (files are intentionally removed).
    #[serde(default = "bool_true")]
    pub empty_folder_protection: bool,

    /// Optional custom S3 / S3-compatible endpoint URL (e.g. MinIO, Cloudflare R2, Wasabi).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,

    /// Optional S3 region override (e.g. "us-east-1", "auto").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,

    /// Allow unencrypted HTTP for local S3 / MinIO testing.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub allow_http: bool,
}

impl Default for FolderConfig {
    fn default() -> Self {
        Self {
            id: Uuid::nil(),
            name: String::new(),
            local_path: PathBuf::new(),
            remote_target: String::new(),
            mode: SyncMode::ContinuousBackup,
            exclude_patterns: Vec::new(),
            empty_folder_protection: true,
            endpoint: None,
            region: None,
            allow_http: false,
        }
    }
}

// ─────────────────────────────────────────────
// Validation
// ─────────────────────────────────────────────

/// Errors produced when reading/parsing a config file.
#[derive(Debug, Error)]
pub enum FileConfigError {
    #[error("I/O error reading config file: {0}")]
    Io(#[from] std::io::Error),
    #[error("TOML parse error: {0}")]
    Toml(#[from] toml::de::Error),
}

/// Errors produced during `validate_config`.
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("folder '{name}' (id={id}): remote_target collision with folder '{other_name}'")]
    RemoteTargetCollision {
        id: Uuid,
        name: String,
        other_name: String,
    },

    #[error(
        "folder '{name}' (id={id}): remote_target '{target}' is a prefix of folder \
         '{other_name}' remote_target '{other_target}'"
    )]
    RemoteTargetPrefixOverlap {
        id: Uuid,
        name: String,
        target: String,
        other_name: String,
        other_target: String,
    },

    #[error("folder '{name}' (id={id}): duplicate folder id in config")]
    DuplicateId { id: Uuid, name: String },

    #[error("duplicate folder name '{name}' (folders must have unique names)")]
    DuplicateName { name: String },

    #[error("folder '{name}': local_path '{}' collides with folder '{other_name}'", path.display())]
    DuplicateLocalPath {
        path: PathBuf,
        name: String,
        other_name: String,
    },

    #[error("no folders configured")]
    NoFolders,

    #[error("folder '{name}' (id={id}): local_path is not absolute")]
    RelativeLocalPath { id: Uuid, name: String },
}

/// A single validation result: either a hard error or a non-fatal warning.
#[derive(Debug)]
pub enum ValidationResult {
    Error(ConfigError),
    Warning(String),
}

/// Validate `GlobalConfig`, returning a list of `ValidationResult`s.
///
/// `syncd` treats any `ValidationResult::Error` as fatal for the affected folder
/// and transitions it to `FolderState::Error`; warnings are logged only.
pub fn validate_config(cfg: &GlobalConfig) -> Vec<ValidationResult> {
    let mut results = Vec::new();

    if cfg.folders.is_empty() {
        results.push(ValidationResult::Warning(
            "No folders configured. syncd will remain idle until a folder is added.".to_string(),
        ));
        return results;
    }

    // Detect duplicate UUIDs
    let mut seen_ids: std::collections::HashMap<Uuid, &str> = Default::default();
    for f in &cfg.folders {
        if let Some(prev_name) = seen_ids.insert(f.id, &f.name) {
            results.push(ValidationResult::Error(ConfigError::DuplicateId {
                id: f.id,
                name: f.name.clone(),
            }));
            let _ = prev_name;
        }
    }

    // Detect duplicate folder names (case-insensitive)
    let mut seen_names: std::collections::HashMap<String, &str> = Default::default();
    for f in &cfg.folders {
        let key = f.name.trim().to_lowercase();
        if let Some(_prev) = seen_names.insert(key, &f.name) {
            results.push(ValidationResult::Error(ConfigError::DuplicateName {
                name: f.name.clone(),
            }));
        }
    }

    // Detect duplicate local paths
    let mut seen_paths: std::collections::HashMap<PathBuf, &str> = Default::default();
    for f in &cfg.folders {
        if let Some(prev_name) = seen_paths.insert(f.local_path.clone(), &f.name) {
            results.push(ValidationResult::Error(ConfigError::DuplicateLocalPath {
                path: f.local_path.clone(),
                name: f.name.clone(),
                other_name: prev_name.to_string(),
            }));
        }
    }

    // Validate local_path is absolute
    for f in &cfg.folders {
        if !f.local_path.is_absolute() {
            results.push(ValidationResult::Error(ConfigError::RelativeLocalPath {
                id: f.id,
                name: f.name.clone(),
            }));
        }
    }

    // Detect remote_target exact matches and prefix overlaps.
    // We normalise by stripping trailing '/' for comparison.
    let normalise = |s: &str| s.trim_end_matches('/').to_owned();

    let folders: Vec<_> = cfg.folders.iter().collect();
    for i in 0..folders.len() {
        let a = folders[i];
        let a_target = normalise(&a.remote_target);

        for b in &folders[(i + 1)..] {
            let b_target = normalise(&b.remote_target);

            if a_target == b_target {
                results.push(ValidationResult::Error(ConfigError::RemoteTargetCollision {
                    id: a.id,
                    name: a.name.clone(),
                    other_name: b.name.clone(),
                }));
            } else if a_target.starts_with(&b_target)
                && a_target.as_bytes().get(b_target.len()) == Some(&b'/')
            {
                results.push(ValidationResult::Error(ConfigError::RemoteTargetPrefixOverlap {
                    id: a.id,
                    name: a.name.clone(),
                    target: a.remote_target.clone(),
                    other_name: b.name.clone(),
                    other_target: b.remote_target.clone(),
                }));
            } else if b_target.starts_with(&a_target)
                && b_target.as_bytes().get(a_target.len()) == Some(&b'/')
            {
                results.push(ValidationResult::Error(ConfigError::RemoteTargetPrefixOverlap {
                    id: b.id,
                    name: b.name.clone(),
                    target: b.remote_target.clone(),
                    other_name: a.name.clone(),
                    other_target: a.remote_target.clone(),
                }));
            }
        }
    }

    results
}

// ─────────────────────────────────────────────
// Default value helpers (required by serde)
// ─────────────────────────────────────────────

fn default_log_max_size() -> u64 {
    26_214_400 // 25 MiB
}

fn default_log_max_archives() -> usize {
    10
}

fn default_max_concurrent() -> usize {
    4
}

fn default_debounce_quiet_ms() -> u64 {
    3_000
}

fn default_debounce_max_wait_ms() -> u64 {
    60_000
}

fn bool_true() -> bool {
    true
}

// ─────────────────────────────────────────────
// Platform helpers (avoid pulling in extra deps)
// ─────────────────────────────────────────────

fn dirs_next_data_dir() -> PathBuf {
    std::env::var("XDG_DATA_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
            PathBuf::from(home).join(".local").join("share")
        })
}

fn dirs_next_state_dir() -> PathBuf {
    std::env::var("XDG_STATE_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
            PathBuf::from(home).join(".local").join("state")
        })
}

/// Thin shim so we can get the real UID without pulling in the full `nix` crate
/// into `sync-common`.
///
/// # Safety
/// `getuid(2)` never fails and has no preconditions; this function is always
/// safe to call, but is declared `unsafe` because it crosses the FFI boundary.
unsafe fn libc_getuid() -> u32 {
    extern "C" {
        fn getuid() -> u32;
    }
    getuid()
}

// ─────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_global_config_roundtrip() {
        let mut config = GlobalConfig::default();
        config.folders.push(FolderConfig {
            id: Uuid::new_v4(),
            name: "docs".to_string(),
            local_path: PathBuf::from("/home/user/Documents"),
            remote_target: "s3://my-bucket/docs".to_string(),
            mode: SyncMode::ContinuousBackup,
            exclude_patterns: vec![".git/".to_string(), "*.tmp".to_string()],
            empty_folder_protection: true,
            endpoint: Some("https://s3.example.com".to_string()),
            region: Some("us-west-2".to_string()),
            allow_http: false,
        });

        let serialized = config.to_string_pretty().expect("failed to serialize");
        let deserialized: GlobalConfig = toml::from_str(&serialized).expect("failed to deserialize");

        assert_eq!(deserialized.folders.len(), 1);
        assert_eq!(deserialized.folders[0].name, "docs");
        assert_eq!(deserialized.folders[0].local_path, PathBuf::from("/home/user/Documents"));
        assert_eq!(deserialized.folders[0].remote_target, "s3://my-bucket/docs");
        assert_eq!(deserialized.folders[0].mode, SyncMode::ContinuousBackup);
        assert_eq!(deserialized.folders[0].endpoint.as_deref(), Some("https://s3.example.com"));
    }

    #[test]
    fn test_save_and_load_file() {
        let dir = std::env::temp_dir().join(format!("syncd_test_{}", Uuid::new_v4()));
        let config_file = dir.join("config.toml");

        let mut config = GlobalConfig::default();
        config.folders.push(FolderConfig {
            id: Uuid::new_v4(),
            name: "test".to_string(),
            local_path: PathBuf::from("/tmp/test"),
            remote_target: "s3://bucket/test".to_string(),
            mode: SyncMode::DropAndPurge,
            exclude_patterns: vec![],
            empty_folder_protection: false,
            endpoint: None,
            region: None,
            allow_http: true,
        });

        config.save_to_file(&config_file).expect("save_to_file failed");
        assert!(config_file.exists());

        let loaded = GlobalConfig::from_file(&config_file).expect("from_file failed");
        assert_eq!(loaded.folders.len(), 1);
        assert_eq!(loaded.folders[0].name, "test");
        assert_eq!(loaded.folders[0].mode, SyncMode::DropAndPurge);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_validate_detects_duplicate_names() {
        let mut config = GlobalConfig::default();
        config.folders.push(FolderConfig {
            id: Uuid::new_v4(),
            name: "Docs".to_string(),
            local_path: PathBuf::from("/home/user/Docs1"),
            remote_target: "s3://b/1".to_string(),
            ..Default::default()
        });
        config.folders.push(FolderConfig {
            id: Uuid::new_v4(),
            name: "docs".to_string(), // case-insensitive duplicate!
            local_path: PathBuf::from("/home/user/Docs2"),
            remote_target: "s3://b/2".to_string(),
            ..Default::default()
        });

        let results = validate_config(&config);
        let has_dup = results.iter().any(|r| matches!(r, ValidationResult::Error(ConfigError::DuplicateName { .. })));
        assert!(has_dup, "expected DuplicateName error");
    }

    #[test]
    fn test_validate_detects_duplicate_paths() {
        let mut config = GlobalConfig::default();
        config.folders.push(FolderConfig {
            id: Uuid::new_v4(),
            name: "Folder1".to_string(),
            local_path: PathBuf::from("/home/user/Docs"),
            remote_target: "s3://b/1".to_string(),
            ..Default::default()
        });
        config.folders.push(FolderConfig {
            id: Uuid::new_v4(),
            name: "Folder2".to_string(),
            local_path: PathBuf::from("/home/user/Docs"),
            remote_target: "s3://b/2".to_string(),
            ..Default::default()
        });

        let results = validate_config(&config);
        let has_dup = results.iter().any(|r| matches!(r, ValidationResult::Error(ConfigError::DuplicateLocalPath { .. })));
        assert!(has_dup, "expected DuplicateLocalPath error");
    }

    #[test]
    fn test_validate_empty_folders_is_warning() {
        let config = GlobalConfig::default();
        let results = validate_config(&config);
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], ValidationResult::Warning(_)));
        assert!(config.validate().is_ok());
    }
}
