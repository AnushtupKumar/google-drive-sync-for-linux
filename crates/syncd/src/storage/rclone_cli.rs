//! Direct Rclone CLI backend implementing [`CloudStorage`].
//!
//! Enables direct synchronization with any cloud remote supported by rclone
//! (e.g. Google Drive, Dropbox, OneDrive, WebDAV, Proton Drive, etc.) without
//! requiring a local FUSE filesystem mount.
//!
//! ## URL syntax
//!
//! `remote_target` must start with `rclone://` followed by the configured remote name
//! and optional folder prefix:
//!
//! | `remote_target` URI               | Remote   | Prefix           | Target path in rclone        |
//! |-----------------------------------|----------|------------------|------------------------------|
//! | `rclone://gdrive/Documents`       | `gdrive` | `"Documents"`    | `gdrive:Documents/<rel_path>`|
//! | `rclone://gdrive:Documents`       | `gdrive` | `"Documents"`    | `gdrive:Documents/<rel_path>`|
//! | `rclone://dropbox/backups/photos` | `dropbox`| `"backups/photos"`| `dropbox:backups/photos/<rel_path>`|
//! | `rclone://onedrive`               | `onedrive`| `""`            | `onedrive:<rel_path>`        |
//!
//! ## Progress tracking
//!
//! While uploading via `rclone copyto`, `RcloneBackend` monitors rclone's JSON
//! accounting stream (`--use-json-log --stats 200ms`) and continuously updates
//! the caller's [`AtomicU64`] progress counter, providing smooth, accurate
//! network transfer speeds in the CLI and TUI.

use std::{
    path::Path,
    process::Stdio,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing::{debug, info};

use super::{
    BandwidthLimiter, CloudStorage, RemoteObjectMeta, StorageError, UploadInfo,
};

/// A [`CloudStorage`] backend that delegates directly to the `rclone` CLI.
pub struct RcloneBackend {
    /// Remote name configured in rclone (e.g. "gdrive").
    remote: String,
    /// Directory/prefix on the remote (e.g. "Documents" or "renders").
    prefix: String,
}

impl RcloneBackend {
    /// Parse an `rclone://` remote target URI into `(remote_name, prefix)`.
    pub fn parse_target(target: &str) -> Result<(String, String), StorageError> {
        let trimmed = target.trim();
        let without_scheme = if let Some(stripped) = trimmed.strip_prefix("rclone://") {
            stripped
        } else {
            return Err(StorageError::InvalidUrl {
                url: target.to_owned(),
                reason: "URL must begin with 'rclone://'".to_owned(),
            });
        };

        let without_scheme = without_scheme.trim();
        if without_scheme.is_empty() {
            return Err(StorageError::InvalidUrl {
                url: target.to_owned(),
                reason: "Missing remote name after 'rclone://'".to_owned(),
            });
        }

        let (remote, prefix) = if let Some((rem, pref)) = without_scheme.split_once(':') {
            (rem, pref)
        } else if let Some((rem, pref)) = without_scheme.split_once('/') {
            (rem, pref)
        } else {
            (without_scheme, "")
        };

        let remote = remote.trim();
        let prefix = prefix.trim_matches('/');

        if remote.is_empty() {
            return Err(StorageError::InvalidUrl {
                url: target.to_owned(),
                reason: "Remote name cannot be empty".to_owned(),
            });
        }

        Ok((remote.to_string(), prefix.to_string()))
    }

    /// Construct an `RcloneBackend` from a `remote_target` URI string.
    pub fn from_remote_target(remote_target: &str) -> Result<Self, StorageError> {
        let (remote, prefix) = Self::parse_target(remote_target)?;
        info!(remote_target, remote = %remote, prefix = %prefix, "RcloneBackend constructed");
        Ok(Self {
            remote,
            prefix,
        })
    }

    /// Construct an `RcloneBackend` from a [`sync_common::config::FolderConfig`].
    pub fn from_folder_config(cfg: &sync_common::config::FolderConfig) -> Result<Self, StorageError> {
        let (remote, prefix) = Self::parse_target(&cfg.remote_target)?;
        info!(
            folder = %cfg.name,
            remote = %remote,
            prefix = %prefix,
            "RcloneBackend constructed from folder config"
        );
        Ok(Self {
            remote,
            prefix,
        })
    }

    /// Return the full rclone remote path specifier (e.g. `"gdrive:Documents/sub/file.pdf"`).
    pub fn full_remote_path(&self, remote_key: &str) -> String {
        let key = remote_key.trim_matches('/');
        if key.is_empty() {
            format!("{}:", self.remote)
        } else {
            format!("{}:{}", self.remote, key)
        }
    }
}

#[async_trait]
impl CloudStorage for RcloneBackend {
    fn remote_key_for(&self, rel_path: &str) -> String {
        let clean_rel = rel_path.trim_matches('/');
        if self.prefix.is_empty() {
            clean_rel.to_string()
        } else if clean_rel.is_empty() {
            self.prefix.clone()
        } else {
            format!("{}/{}", self.prefix, clean_rel)
        }
    }

    fn trash_key_for(&self, rel_path: &str) -> String {
        let clean_rel = rel_path.trim_matches('/');
        if self.prefix.is_empty() {
            format!(".Trash-syncd/{}", clean_rel)
        } else {
            format!("{}/.Trash-syncd/{}", self.prefix, clean_rel)
        }
    }

    async fn put_file(
        &self,
        local_path: &Path,
        remote_key: &str,
        limiter: Option<&BandwidthLimiter>,
        progress: Option<Arc<AtomicU64>>,
    ) -> Result<UploadInfo, StorageError> {
        if !local_path.exists() {
            return Err(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Local file '{}' not found", local_path.display()),
            )));
        }

        let meta = tokio::fs::metadata(local_path).await?;
        let size_bytes = meta.len();

        // Calculate SHA-256 of the source file
        let path_clone = local_path.to_owned();
        let sha256_hex = tokio::task::spawn_blocking(move || -> std::io::Result<String> {
            let mut file = std::fs::File::open(path_clone)?;
            let mut hasher = Sha256::new();
            std::io::copy(&mut file, &mut hasher)?;
            Ok(hex::encode(hasher.finalize()))
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e)))??;

        let dst = self.full_remote_path(remote_key);
        debug!(
            path = ?local_path,
            dst = %dst,
            size_bytes,
            sha256 = %sha256_hex,
            "starting rclone direct upload"
        );

        let mut cmd = tokio::process::Command::new("rclone");
        cmd.arg("copyto")
            .arg(local_path)
            .arg(&dst)
            .arg("--use-json-log")
            .arg("--stats")
            .arg("200ms")
            .arg("-v")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        // Apply bandwidth rate limit if active
        if let Some(lim) = limiter {
            if lim.burst_bytes() > 0 {
                cmd.arg("--bwlimit").arg(format!("{}B", lim.burst_bytes()));
            }
        }

        let mut child = cmd.spawn().map_err(StorageError::Io)?;
        let stderr = child.stderr.take().ok_or_else(|| {
            StorageError::Io(std::io::Error::other("Failed to capture rclone stderr pipe"))
        })?;

        let reader = BufReader::new(stderr);
        let mut lines = reader.lines();
        let mut last_error_msg = String::new();

        while let Ok(Some(line)) = lines.next_line().await {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(&line) {
                if let Some(stats) = val.get("stats") {
                    if let Some(bytes) = stats.get("bytes").and_then(|b| b.as_u64()) {
                        if let Some(p) = &progress {
                            p.store(bytes, Ordering::Relaxed);
                        }
                    }
                }
                if let Some(lvl) = val.get("level").and_then(|l| l.as_str()) {
                    if lvl == "error" {
                        if let Some(msg) = val.get("msg").and_then(|m| m.as_str()) {
                            last_error_msg = msg.to_string();
                        }
                    }
                }
            }
        }

        let status = child.wait().await.map_err(StorageError::Io)?;
        if !status.success() {
            return Err(StorageError::Rclone(if last_error_msg.is_empty() {
                format!("rclone copyto failed with exit status {}", status)
            } else {
                format!("rclone copyto failed: {}", last_error_msg)
            }));
        }

        // Ensure progress reflects 100% on successful completion
        if let Some(p) = &progress {
            p.store(size_bytes, Ordering::Relaxed);
        }

        info!(
            path = ?local_path,
            dst = %dst,
            size_bytes,
            sha256 = %sha256_hex,
            "rclone direct upload complete"
        );

        Ok(UploadInfo {
            remote_key: remote_key.to_string(),
            size_bytes,
            sha256_hex,
            etag: None,
        })
    }

    async fn head_object(&self, remote_key: &str) -> Result<RemoteObjectMeta, StorageError> {
        let dst = self.full_remote_path(remote_key);
        let output = tokio::process::Command::new("rclone")
            .arg("lsjson")
            .arg(&dst)
            .arg("--stat")
            .kill_on_drop(true)
            .output()
            .await
            .map_err(StorageError::Io)?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(StorageError::ObjectStore(object_store::Error::NotFound {
                path: remote_key.to_string(),
                source: stderr.trim().to_string().into(),
            }));
        }

        let val: serde_json::Value = serde_json::from_slice(&output.stdout).map_err(|e| {
            StorageError::Rclone(format!("Failed to parse rclone lsjson JSON: {}", e))
        })?;

        let is_dir = val.get("IsDir").and_then(|b| b.as_bool()).unwrap_or(false);
        let size = val.get("Size").and_then(|s| s.as_i64()).unwrap_or(-1);

        if is_dir || size < 0 {
            return Err(StorageError::ObjectStore(object_store::Error::NotFound {
                path: remote_key.to_string(),
                source: "Object is a directory or does not exist".into(),
            }));
        }

        let etag = val.get("ID").and_then(|i| i.as_str()).map(|s| s.to_string());

        Ok(RemoteObjectMeta {
            remote_key: remote_key.to_string(),
            size_bytes: size as u64,
            etag,
        })
    }

    async fn delete_object(&self, remote_key: &str) -> Result<(), StorageError> {
        let dst = self.full_remote_path(remote_key);
        let output = tokio::process::Command::new("rclone")
            .arg("deletefile")
            .arg(&dst)
            .kill_on_drop(true)
            .output()
            .await
            .map_err(StorageError::Io)?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("not found")
                || stderr.contains("doesn't exist")
                || stderr.contains("directory not found")
            {
                return Ok(());
            }
            return Err(StorageError::Rclone(format!(
                "rclone deletefile failed: {}",
                stderr.trim()
            )));
        }

        debug!(remote_key, "rclone object deleted");
        Ok(())
    }

    async fn copy_object(&self, src_key: &str, dst_key: &str) -> Result<(), StorageError> {
        let src = self.full_remote_path(src_key);
        let dst = self.full_remote_path(dst_key);
        let output = tokio::process::Command::new("rclone")
            .arg("copyto")
            .arg(&src)
            .arg(&dst)
            .kill_on_drop(true)
            .output()
            .await
            .map_err(StorageError::Io)?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(StorageError::Rclone(format!(
                "rclone copyto failed: {}",
                stderr.trim()
            )));
        }

        debug!(src = %src, dst = %dst, "rclone object copied server-side");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_target() {
        let (rem, pref) = RcloneBackend::parse_target("rclone://gdrive/Documents").unwrap();
        assert_eq!(rem, "gdrive");
        assert_eq!(pref, "Documents");

        let (rem, pref) = RcloneBackend::parse_target("rclone://gdrive:Documents").unwrap();
        assert_eq!(rem, "gdrive");
        assert_eq!(pref, "Documents");

        let (rem, pref) = RcloneBackend::parse_target("rclone://dropbox/backups/photos/").unwrap();
        assert_eq!(rem, "dropbox");
        assert_eq!(pref, "backups/photos");

        let (rem, pref) = RcloneBackend::parse_target("rclone://onedrive").unwrap();
        assert_eq!(rem, "onedrive");
        assert_eq!(pref, "");

        let (rem, pref) = RcloneBackend::parse_target("rclone://onedrive/").unwrap();
        assert_eq!(rem, "onedrive");
        assert_eq!(pref, "");

        assert!(RcloneBackend::parse_target("s3://bucket/docs").is_err());
        assert!(RcloneBackend::parse_target("rclone://").is_err());
    }

    #[test]
    fn test_key_paths() {
        let backend = RcloneBackend::from_remote_target("rclone://gdrive/Documents").unwrap();
        assert_eq!(backend.remote_key_for("notes.txt"), "Documents/notes.txt");
        assert_eq!(backend.remote_key_for("2026/taxes.pdf"), "Documents/2026/taxes.pdf");
        assert_eq!(
            backend.trash_key_for("2026/taxes.pdf"),
            "Documents/.Trash-syncd/2026/taxes.pdf"
        );
        assert_eq!(
            backend.full_remote_path("Documents/notes.txt"),
            "gdrive:Documents/notes.txt"
        );

        let root_backend = RcloneBackend::from_remote_target("rclone://gdrive").unwrap();
        assert_eq!(root_backend.remote_key_for("notes.txt"), "notes.txt");
        assert_eq!(root_backend.trash_key_for("notes.txt"), ".Trash-syncd/notes.txt");
        assert_eq!(root_backend.full_remote_path("notes.txt"), "gdrive:notes.txt");
    }
}
