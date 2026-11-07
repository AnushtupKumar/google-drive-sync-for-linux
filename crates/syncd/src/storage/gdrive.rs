//! Native Google Drive v3 REST API backend implementing [`CloudStorage`].
//!
//! Provides direct, 100% pure Rust synchronization with personal and workspace
//! Google Drive accounts without requiring `rclone`, FUSE mounts, or any external binary.
//!
//! ## URL syntax
//!
//! Supported `remote_target` URI schemes:
//! - `gdrive://<folder>` (e.g. `gdrive://Documents` or `gdrive://renders`)
//! - `gdrive://<remote>/<folder>` (e.g. `gdrive://gdrive/Documents`)
//! - `googledrive://<folder>`
//! - `rclone://gdrive/<folder>` (seamlessly routed to native Google Drive backend)
//!
//! ## Authentication
//!
//! Credentials are automatically discovered in this order:
//! 1. `~/.config/syncd/gdrive_credentials.json` (dedicated syncd credentials)
//! 2. `~/.config/rclone/rclone.conf` (auto-discovered and migrated to syncd config)
//! 3. Environment variables: `GDRIVE_CLIENT_ID`, `GDRIVE_CLIENT_SECRET`, `GDRIVE_REFRESH_TOKEN`
//!
//! ## Streaming & Resumable Uploads
//!
//! Uploads use the official Google Drive Resumable Upload protocol. Data is streamed
//! in 256 KiB chunks directly from disk to Google Drive, with in-flight rate limiting,
//! real-time atomic progress tracking for CLI/TUI, and in-flight SHA-256 calculation.

use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use futures::SinkExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{io::AsyncReadExt, sync::RwLock};
use tracing::{debug, info, warn};

use super::{
    BandwidthLimiter, CloudStorage, RemoteObjectMeta, StorageError, UploadInfo,
};

// ─────────────────────────────────────────────
// Credentials & OAuth2 Management
// ─────────────────────────────────────────────

/// Google Drive OAuth2 application credentials.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoogleDriveCredentials {
    pub client_id: String,
    pub client_secret: String,
    pub refresh_token: String,
}

impl GoogleDriveCredentials {
    /// Return the standard path for syncd's Google Drive credentials file.
    pub fn default_path() -> PathBuf {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
        PathBuf::from(home).join(".config/syncd/gdrive_credentials.json")
    }

    /// Load credentials from `~/.config/syncd/gdrive_credentials.json`.
    pub fn load_from_syncd() -> Option<Self> {
        let path = Self::default_path();
        if path.exists() {
            if let Ok(data) = fs::read_to_string(&path) {
                if let Ok(creds) = serde_json::from_str::<Self>(&data) {
                    if !creds.client_id.is_empty() && !creds.refresh_token.is_empty() {
                        return Some(creds);
                    }
                }
            }
        }
        None
    }

    /// Save credentials to `~/.config/syncd/gdrive_credentials.json` with secure permissions.
    pub fn save_to_syncd(&self) -> std::io::Result<()> {
        let path = Self::default_path();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        fs::write(&path, json)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(&path, fs::Permissions::from_mode(0o600));
        }
        Ok(())
    }

    /// Load credentials by parsing `~/.config/rclone/rclone.conf` for the given remote name.
    ///
    /// If successful, automatically saves a copy to `~/.config/syncd/gdrive_credentials.json`
    /// so syncd operates independently going forward.
    pub fn load_from_rclone(remote_name: &str) -> Option<Self> {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
        let rclone_conf = PathBuf::from(home).join(".config/rclone/rclone.conf");
        if !rclone_conf.exists() {
            return None;
        }

        let content = fs::read_to_string(&rclone_conf).ok()?;
        let target_header = format!("[{}]", remote_name);
        let mut inside_target = false;

        let mut client_id = String::new();
        let mut client_secret = String::new();
        let mut refresh_token = String::new();

        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with('[') && trimmed.ends_with(']') {
                if inside_target {
                    break;
                }
                if trimmed.eq_ignore_ascii_case(&target_header) {
                    inside_target = true;
                }
                continue;
            }

            if inside_target {
                if let Some((k, v)) = trimmed.split_once('=') {
                    let key = k.trim();
                    let val = v.trim();
                    if key.eq_ignore_ascii_case("client_id") {
                        client_id = val.to_string();
                    } else if key.eq_ignore_ascii_case("client_secret") {
                        client_secret = val.to_string();
                    } else if key.eq_ignore_ascii_case("token") {
                        if let Ok(json) = serde_json::from_str::<serde_json::Value>(val) {
                            if let Some(rt) = json.get("refresh_token").and_then(|t| t.as_str()) {
                                refresh_token = rt.to_string();
                            }
                        }
                    }
                }
            }
        }

        if !client_id.is_empty() && !refresh_token.is_empty() {
            let creds = Self {
                client_id,
                client_secret,
                refresh_token,
            };
            // Persist to syncd config directory for standalone independence
            if let Err(e) = creds.save_to_syncd() {
                warn!("Could not cache Google Drive credentials to syncd config: {}", e);
            } else {
                info!("Auto-migrated Google Drive credentials from rclone.conf to syncd config");
            }
            return Some(creds);
        }

        None
    }

    /// Load credentials from environment variables.
    pub fn load_from_env() -> Option<Self> {
        let client_id = std::env::var("GDRIVE_CLIENT_ID").ok()?;
        let client_secret = std::env::var("GDRIVE_CLIENT_SECRET").unwrap_or_default();
        let refresh_token = std::env::var("GDRIVE_REFRESH_TOKEN").ok()?;

        if !client_id.is_empty() && !refresh_token.is_empty() {
            Some(Self {
                client_id,
                client_secret,
                refresh_token,
            })
        } else {
            None
        }
    }

    /// Discover credentials using the discovery hierarchy.
    pub fn discover(remote_name: Option<&str>) -> Result<Self, StorageError> {
        if let Some(creds) = Self::load_from_syncd() {
            return Ok(creds);
        }
        if let Some(creds) = Self::load_from_env() {
            return Ok(creds);
        }
        let rem = remote_name.unwrap_or("gdrive");
        if let Some(creds) = Self::load_from_rclone(rem) {
            return Ok(creds);
        }

        Err(StorageError::InvalidUrl {
            url: format!("gdrive://{}", rem),
            reason: format!(
                "Google Drive credentials not found. Please provide credentials in '{}' \
                 or run 'rclone config' for remote '{}'.",
                Self::default_path().display(),
                rem
            ),
        })
    }
}

// ─────────────────────────────────────────────
// OAuth2 Token Manager
// ─────────────────────────────────────────────

struct CachedToken {
    access_token: String,
    expires_at: Instant,
}

pub struct TokenManager {
    credentials: GoogleDriveCredentials,
    client: reqwest::Client,
    cache: RwLock<Option<CachedToken>>,
}

impl TokenManager {
    pub fn new(credentials: GoogleDriveCredentials) -> Self {
        Self {
            credentials,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_default(),
            cache: RwLock::new(None),
        }
    }

    /// Retrieve a valid OAuth2 Bearer token, automatically refreshing if expired.
    pub async fn get_token(&self) -> Result<String, StorageError> {
        // 1. Fast read path: token cached and valid for at least 5 more minutes
        {
            let lock = self.cache.read().await;
            if let Some(ref cached) = *lock {
                if Instant::now() + Duration::from_secs(300) < cached.expires_at {
                    return Ok(cached.access_token.clone());
                }
            }
        }

        // 2. Slow write path: acquire write lock and refresh from Google
        let mut lock = self.cache.write().await;
        // Double-check condition after acquiring write lock
        if let Some(ref cached) = *lock {
            if Instant::now() + Duration::from_secs(300) < cached.expires_at {
                return Ok(cached.access_token.clone());
            }
        }

        debug!("Refreshing Google Drive OAuth2 access token");
        let params = [
            ("client_id", self.credentials.client_id.as_str()),
            ("client_secret", self.credentials.client_secret.as_str()),
            ("refresh_token", self.credentials.refresh_token.as_str()),
            ("grant_type", "refresh_token"),
        ];

        let resp = self
            .client
            .post("https://oauth2.googleapis.com/token")
            .form(&params)
            .send()
            .await
            .map_err(|e| StorageError::Io(std::io::Error::other(format!("Token refresh request failed: {}", e))))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let err_body = resp.text().await.unwrap_or_default();
            return Err(StorageError::InvalidUrl {
                url: "https://oauth2.googleapis.com/token".into(),
                reason: format!("OAuth2 token refresh rejected (HTTP {}): {}", status, err_body),
            });
        }

        let json: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| StorageError::Io(std::io::Error::other(format!("Failed to parse token response: {}", e))))?;

        let access_token = json
            .get("access_token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| StorageError::InvalidUrl {
                url: "https://oauth2.googleapis.com/token".into(),
                reason: "OAuth2 response missing 'access_token'".into(),
            })?
            .to_string();

        let expires_in_secs = json
            .get("expires_in")
            .and_then(|v| v.as_u64())
            .unwrap_or(3600);

        let expires_at = Instant::now() + Duration::from_secs(expires_in_secs);
        *lock = Some(CachedToken {
            access_token: access_token.clone(),
            expires_at,
        });

        info!(expires_in = expires_in_secs, "Google Drive access token refreshed successfully");
        Ok(access_token)
    }
}

// ─────────────────────────────────────────────
// Folder Hierarchy Resolver
// ─────────────────────────────────────────────

pub struct FolderResolver {
    client: reqwest::Client,
    token_mgr: Arc<TokenManager>,
    /// Cache mapping `"parent_id/folder_name"` -> `"folder_id"`
    cache: RwLock<HashMap<String, String>>,
}

impl FolderResolver {
    pub fn new(client: reqwest::Client, token_mgr: Arc<TokenManager>) -> Self {
        Self {
            client,
            token_mgr,
            cache: RwLock::new(HashMap::new()),
        }
    }

    /// Escape single quotes and backslashes for Google Drive API queries.
    fn escape_query(s: &str) -> String {
        s.replace('\\', "\\\\").replace('\'', "\\'")
    }

    /// Retrieve or auto-create a folder child under `parent_id`.
    pub async fn get_or_create_folder(
        &self,
        parent_id: &str,
        folder_name: &str,
    ) -> Result<String, StorageError> {
        let cache_key = format!("{}/{}", parent_id, folder_name);
        {
            let lock = self.cache.read().await;
            if let Some(id) = lock.get(&cache_key) {
                return Ok(id.clone());
            }
        }

        let token = self.token_mgr.get_token().await?;
        let escaped = Self::escape_query(folder_name);
        let q = format!(
            "name = '{}' and '{}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false",
            escaped, parent_id
        );

        let resp = self
            .client
            .get("https://www.googleapis.com/drive/v3/files")
            .bearer_auth(&token)
            .query(&[
                ("q", q.as_str()),
                ("fields", "files(id, name)"),
                ("supportsAllDrives", "true"),
                ("includeItemsFromAllDrives", "true"),
            ])
            .send()
            .await
            .map_err(|e| StorageError::Io(std::io::Error::other(format!("Folder query failed: {}", e))))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(StorageError::InvalidUrl {
                url: format!("https://www.googleapis.com/drive/v3/files?q={}", q),
                reason: format!("Google Drive folder search failed (HTTP {}): {}", status, body),
            });
        }

        let json: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| StorageError::Io(std::io::Error::other(format!("Invalid JSON in folder list: {}", e))))?;

        if let Some(files) = json.get("files").and_then(|v| v.as_array()) {
            if let Some(first) = files.first() {
                if let Some(id) = first.get("id").and_then(|v| v.as_str()) {
                    let folder_id = id.to_string();
                    self.cache.write().await.insert(cache_key, folder_id.clone());
                    return Ok(folder_id);
                }
            }
        }

        // Folder does not exist: create it dynamically
        debug!(parent_id, folder_name, "Creating remote folder on Google Drive");
        let create_body = serde_json::json!({
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id]
        });

        let create_resp = self
            .client
            .post("https://www.googleapis.com/drive/v3/files?supportsAllDrives=true")
            .bearer_auth(&token)
            .json(&create_body)
            .send()
            .await
            .map_err(|e| StorageError::Io(std::io::Error::other(format!("Folder creation request failed: {}", e))))?;

        if !create_resp.status().is_success() {
            let status = create_resp.status();
            let body = create_resp.text().await.unwrap_or_default();
            return Err(StorageError::InvalidUrl {
                url: "https://www.googleapis.com/drive/v3/files".into(),
                reason: format!("Google Drive folder creation failed (HTTP {}): {}", status, body),
            });
        }

        let create_json: serde_json::Value = create_resp
            .json()
            .await
            .map_err(|e| StorageError::Io(std::io::Error::other(format!("Invalid JSON in folder create: {}", e))))?;

        let created_id = create_json
            .get("id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| StorageError::InvalidUrl {
                url: "https://www.googleapis.com/drive/v3/files".into(),
                reason: "Google Drive folder create response missing 'id'".into(),
            })?
            .to_string();

        info!(parent_id, folder_name, folder_id = %created_id, "Created remote Google Drive folder");
        self.cache.write().await.insert(cache_key, created_id.clone());
        Ok(created_id)
    }

    /// Resolve an arbitrary directory path (e.g. `"Documents/2026/taxes"`) starting from `'root'`,
    /// creating missing intermediate directories along the way.
    pub async fn resolve_path(&self, dir_path: &str) -> Result<String, StorageError> {
        let trimmed = dir_path.trim_matches('/');
        if trimmed.is_empty() {
            return Ok("root".to_string());
        }

        let segments: Vec<&str> = trimmed.split('/').filter(|s| !s.is_empty()).collect();
        let mut current_id = "root".to_string();

        for segment in segments {
            current_id = self.get_or_create_folder(&current_id, segment).await?;
        }

        Ok(current_id)
    }
}

// ─────────────────────────────────────────────
// Google Drive Backend
// ─────────────────────────────────────────────

/// Native Google Drive [`CloudStorage`] implementation.
pub struct GoogleDriveBackend {
    token_mgr: Arc<TokenManager>,
    resolver: Arc<FolderResolver>,
    client: reqwest::Client,
    /// Root directory prefix on Google Drive (e.g. `"Documents"` or `"renders"`).
    prefix: String,
}

impl GoogleDriveBackend {
    /// Parse a target URL (`gdrive://...`, `googledrive://...`, or `rclone://gdrive/...`)
    /// into `(remote_name, prefix)`.
    pub fn parse_target(target: &str) -> Result<(String, String), StorageError> {
        let trimmed = target.trim();

        let stripped = if let Some(s) = trimmed.strip_prefix("gdrive://") {
            s
        } else if let Some(s) = trimmed.strip_prefix("googledrive://") {
            s
        } else if let Some(s) = trimmed.strip_prefix("rclone://gdrive") {
            // Strip optional remote prefix separator
            s.strip_prefix('/').or_else(|| s.strip_prefix(':')).unwrap_or(s)
        } else {
            return Err(StorageError::InvalidUrl {
                url: target.to_owned(),
                reason: "Expected URL scheme 'gdrive://', 'googledrive://', or 'rclone://gdrive/'".to_owned(),
            });
        };

        let stripped = stripped.trim();
        let (remote_name, prefix) = if stripped.is_empty() {
            ("gdrive", "")
        } else if let Some((first, rest)) = stripped.split_once('/') {
            // If URL is `gdrive://gdrive/Documents` -> remote is "gdrive", prefix is "Documents"
            if first.eq_ignore_ascii_case("gdrive") {
                ("gdrive", rest.trim_matches('/'))
            } else {
                ("gdrive", stripped.trim_matches('/'))
            }
        } else if stripped.eq_ignore_ascii_case("gdrive") {
            ("gdrive", "")
        } else {
            ("gdrive", stripped.trim_matches('/'))
        };

        Ok((remote_name.to_string(), prefix.to_string()))
    }

    /// Construct a `GoogleDriveBackend` from a `remote_target` URI string.
    pub fn from_remote_target(remote_target: &str) -> Result<Self, StorageError> {
        let (remote_name, prefix) = Self::parse_target(remote_target)?;
        let credentials = GoogleDriveCredentials::discover(Some(&remote_name))?;
        let token_mgr = Arc::new(TokenManager::new(credentials));
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(300))
            .build()
            .unwrap_or_default();
        let resolver = Arc::new(FolderResolver::new(client.clone(), token_mgr.clone()));

        info!(
            remote_target,
            prefix = %prefix,
            "Native GoogleDriveBackend initialized in pure Rust"
        );

        Ok(Self {
            token_mgr,
            resolver,
            client,
            prefix,
        })
    }

    /// Construct a `GoogleDriveBackend` from a [`sync_common::config::FolderConfig`].
    pub fn from_folder_config(cfg: &sync_common::config::FolderConfig) -> Result<Self, StorageError> {
        Self::from_remote_target(&cfg.remote_target)
    }

    /// Detect simple MIME types based on file extension.
    fn guess_mime(path: &Path) -> &'static str {
        match path
            .extension()
            .and_then(|s| s.to_str())
            .map(|s| s.to_ascii_lowercase())
            .as_deref()
        {
            Some("txt") => "text/plain",
            Some("html") | Some("htm") => "text/html",
            Some("json") => "application/json",
            Some("pdf") => "application/pdf",
            Some("zip") => "application/zip",
            Some("tar") => "application/x-tar",
            Some("gz") => "application/gzip",
            Some("png") => "image/png",
            Some("jpg") | Some("jpeg") => "image/jpeg",
            Some("gif") => "image/gif",
            Some("webp") => "image/webp",
            Some("svg") => "image/svg+xml",
            Some("mp4") => "video/mp4",
            Some("mov") => "video/quicktime",
            Some("mkv") => "video/x-matroska",
            Some("mp3") => "audio/mpeg",
            Some("wav") => "audio/wav",
            Some("csv") => "text/csv",
            Some("rs") => "text/x-rust",
            Some("toml") => "application/toml",
            _ => "application/octet-stream",
        }
    }

    /// Find an existing file ID by name inside a parent folder.
    async fn find_file_in_parent(
        &self,
        parent_id: &str,
        file_name: &str,
    ) -> Result<Option<RemoteObjectMeta>, StorageError> {
        let token = self.token_mgr.get_token().await?;
        let escaped = FolderResolver::escape_query(file_name);
        let q = format!(
            "name = '{}' and '{}' in parents and mimeType != 'application/vnd.google-apps.folder' and trashed = false",
            escaped, parent_id
        );

        let resp = self
            .client
            .get("https://www.googleapis.com/drive/v3/files")
            .bearer_auth(&token)
            .query(&[
                ("q", q.as_str()),
                ("fields", "files(id, name, size, md5Checksum)"),
                ("supportsAllDrives", "true"),
                ("includeItemsFromAllDrives", "true"),
            ])
            .send()
            .await
            .map_err(|e| StorageError::Io(std::io::Error::other(format!("File lookup failed: {}", e))))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(StorageError::InvalidUrl {
                url: "https://www.googleapis.com/drive/v3/files".into(),
                reason: format!("File lookup returned HTTP {}: {}", status, body),
            });
        }

        let json: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| StorageError::Io(std::io::Error::other(format!("Invalid JSON in file lookup: {}", e))))?;

        if let Some(files) = json.get("files").and_then(|v| v.as_array()) {
            if let Some(first) = files.first() {
                let id = first.get("id").and_then(|v| v.as_str()).unwrap_or_default().to_string();
                let size = first
                    .get("size")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);

                return Ok(Some(RemoteObjectMeta {
                    remote_key: file_name.to_string(),
                    size_bytes: size,
                    etag: Some(id),
                }));
            }
        }

        Ok(None)
    }
}

#[async_trait]
impl CloudStorage for GoogleDriveBackend {
    fn remote_key_for(&self, rel_path: &str) -> String {
        let clean = rel_path.trim_matches('/');
        if self.prefix.is_empty() {
            clean.to_string()
        } else if clean.is_empty() {
            self.prefix.clone()
        } else {
            format!("{}/{}", self.prefix, clean)
        }
    }

    fn trash_key_for(&self, rel_path: &str) -> String {
        let clean = rel_path.trim_matches('/');
        if self.prefix.is_empty() {
            format!(".Trash-syncd/{}", clean)
        } else {
            format!("{}/.Trash-syncd/{}", self.prefix, clean)
        }
    }

    async fn put_file(
        &self,
        local_path: &Path,
        remote_key: &str,
        limiter: Option<&BandwidthLimiter>,
        progress: Option<Arc<AtomicU64>>,
    ) -> Result<UploadInfo, StorageError> {
        let meta = tokio::fs::metadata(local_path).await?;
        let file_size = meta.len();

        // 1. Separate directory path from filename
        let (dir_part, file_name) = match remote_key.rfind('/') {
            Some(idx) => (&remote_key[..idx], &remote_key[idx + 1..]),
            None => ("", remote_key),
        };

        // 2. Resolve or create parent folder hierarchy
        let parent_id = self.resolver.resolve_path(dir_part).await?;

        // 3. Check if file already exists in parent folder (to update in-place vs create)
        let existing = self.find_file_in_parent(&parent_id, file_name).await?;
        let token = self.token_mgr.get_token().await?;
        let mime_type = Self::guess_mime(local_path);

        // 4. Initiate Google Drive Resumable Upload Session
        let (init_url, is_patch) = match &existing {
            Some(meta) => {
                let file_id = meta.etag.as_deref().unwrap_or(&meta.remote_key);
                (
                    format!(
                        "https://www.googleapis.com/upload/drive/v3/files/{}?uploadType=resumable&supportsAllDrives=true",
                        file_id
                    ),
                    true,
                )
            }
            None => (
                "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&supportsAllDrives=true"
                    .to_string(),
                false,
            ),
        };

        let init_req = if is_patch {
            self.client.patch(&init_url)
        } else {
            let body = serde_json::json!({
                "name": file_name,
                "parents": [parent_id]
            });
            self.client.post(&init_url).json(&body)
        };

        let init_resp = init_req
            .bearer_auth(&token)
            .header("X-Upload-Content-Type", mime_type)
            .header("X-Upload-Content-Length", file_size.to_string())
            .send()
            .await
            .map_err(|e| StorageError::Io(std::io::Error::other(format!("Resumable upload init failed: {}", e))))?;

        if !init_resp.status().is_success() {
            let status = init_resp.status();
            let body = init_resp.text().await.unwrap_or_default();
            return Err(StorageError::InvalidUrl {
                url: init_url,
                reason: format!("Google Drive resumable upload init failed (HTTP {}): {}", status, body),
            });
        }

        let upload_url = init_resp
            .headers()
            .get("Location")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| StorageError::InvalidUrl {
                url: init_url,
                reason: "Google Drive did not return a 'Location' header for resumable upload".into(),
            })?
            .to_string();

        debug!(remote_key, file_size, "Streaming file data to Google Drive resumable session");

        // 5. Stream file content with bandwidth limiting, atomic progress tracking, and in-flight SHA-256
        let (mut tx, rx) = futures::channel::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(2);
        let path_clone = local_path.to_owned();
        let limiter_clone = limiter.cloned();
        let progress_clone = progress.clone();
        let (sha_tx, sha_rx) = tokio::sync::oneshot::channel::<String>();

        tokio::spawn(async move {
            let mut file = match tokio::fs::File::open(&path_clone).await {
                Ok(f) => f,
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            };
            let mut hasher = Sha256::new();
            let chunk_size: usize = if let Some(lim) = &limiter_clone {
                (lim.burst_bytes() as usize).clamp(4 * 1024, 256 * 1024)
            } else {
                256 * 1024
            };

            let mut buf = vec![0u8; chunk_size];
            loop {
                match file.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        if let Some(lim) = &limiter_clone {
                            lim.acquire(n).await;
                        }
                        hasher.update(&buf[..n]);
                        if let Some(p) = &progress_clone {
                            p.fetch_add(n as u64, Ordering::Relaxed);
                        }
                        if tx
                            .send(Ok(bytes::Bytes::copy_from_slice(&buf[..n])))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        break;
                    }
                }
            }
            let _ = sha_tx.send(hex::encode(hasher.finalize()));
        });

        let body = reqwest::Body::wrap_stream(rx);
        let content_range = if file_size == 0 {
            "bytes */0".to_string()
        } else {
            format!("bytes 0-{}/{}", file_size - 1, file_size)
        };

        let upload_resp = self
            .client
            .put(&upload_url)
            .bearer_auth(&token)
            .header("Content-Length", file_size.to_string())
            .header("Content-Range", content_range)
            .body(body)
            .send()
            .await
            .map_err(|e| StorageError::Io(std::io::Error::other(format!("Resumable upload PUT failed: {}", e))))?;

        if !upload_resp.status().is_success() {
            let status = upload_resp.status();
            let body = upload_resp.text().await.unwrap_or_default();
            return Err(StorageError::InvalidUrl {
                url: upload_url,
                reason: format!("Google Drive upload failed (HTTP {}): {}", status, body),
            });
        }

        let upload_json: serde_json::Value = upload_resp
            .json()
            .await
            .map_err(|e| StorageError::Io(std::io::Error::other(format!("Invalid JSON in upload response: {}", e))))?;

        let file_id = upload_json
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        let sha256_hex = sha_rx.await.unwrap_or_default();

        if let Some(p) = &progress {
            p.store(file_size, Ordering::Relaxed);
        }

        info!(
            remote_key,
            size = file_size,
            file_id = %file_id,
            sha256 = %sha256_hex,
            "Native Google Drive upload complete"
        );

        Ok(UploadInfo {
            remote_key: remote_key.to_string(),
            size_bytes: file_size,
            sha256_hex,
            etag: Some(file_id),
        })
    }

    async fn head_object(&self, remote_key: &str) -> Result<RemoteObjectMeta, StorageError> {
        let (dir_part, file_name) = match remote_key.rfind('/') {
            Some(idx) => (&remote_key[..idx], &remote_key[idx + 1..]),
            None => ("", remote_key),
        };

        let parent_id = self.resolver.resolve_path(dir_part).await?;
        if let Some(mut meta) = self.find_file_in_parent(&parent_id, file_name).await? {
            meta.remote_key = remote_key.to_string();
            return Ok(meta);
        }

        Err(StorageError::ObjectStore(object_store::Error::NotFound {
            path: remote_key.to_string(),
            source: format!("File '{}' not found in Google Drive folder '{}'", file_name, dir_part).into(),
        }))
    }

    async fn delete_object(&self, remote_key: &str) -> Result<(), StorageError> {
        let (dir_part, file_name) = match remote_key.rfind('/') {
            Some(idx) => (&remote_key[..idx], &remote_key[idx + 1..]),
            None => ("", remote_key),
        };

        let parent_id = self.resolver.resolve_path(dir_part).await?;
        if let Some(meta) = self.find_file_in_parent(&parent_id, file_name).await? {
            let file_id = meta.etag.as_deref().unwrap_or(&meta.remote_key);
            let token = self.token_mgr.get_token().await?;
            let delete_url = format!("https://www.googleapis.com/drive/v3/files/{}?supportsAllDrives=true", file_id);

            let resp = self
                .client
                .delete(&delete_url)
                .bearer_auth(&token)
                .send()
                .await
                .map_err(|e| StorageError::Io(std::io::Error::other(format!("Delete request failed: {}", e))))?;

            if !resp.status().is_success() && resp.status() != reqwest::StatusCode::NOT_FOUND {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(StorageError::InvalidUrl {
                    url: delete_url,
                    reason: format!("Delete failed (HTTP {}): {}", status, body),
                });
            }
            debug!(remote_key, file_id, "Deleted remote file from Google Drive");
        }

        Ok(())
    }

    async fn copy_object(&self, src_key: &str, dst_key: &str) -> Result<(), StorageError> {
        let (src_dir, src_file) = match src_key.rfind('/') {
            Some(idx) => (&src_key[..idx], &src_key[idx + 1..]),
            None => ("", src_key),
        };
        let (dst_dir, dst_file) = match dst_key.rfind('/') {
            Some(idx) => (&dst_key[..idx], &dst_key[idx + 1..]),
            None => ("", dst_key),
        };

        let src_parent_id = self.resolver.resolve_path(src_dir).await?;
        let src_meta = self
            .find_file_in_parent(&src_parent_id, src_file)
            .await?
            .ok_or_else(|| StorageError::ObjectStore(object_store::Error::NotFound {
                path: src_key.to_string(),
                source: "Source file to copy not found in Google Drive".into(),
            }))?;

        let src_file_id = src_meta.etag.as_deref().unwrap_or(&src_meta.remote_key);
        let dst_parent_id = self.resolver.resolve_path(dst_dir).await?;
        let token = self.token_mgr.get_token().await?;

        let copy_url = format!(
            "https://www.googleapis.com/drive/v3/files/{}/copy?supportsAllDrives=true",
            src_file_id
        );
        let body = serde_json::json!({
            "name": dst_file_name_or(dst_file, src_file),
            "parents": [dst_parent_id]
        });

        let resp = self
            .client
            .post(&copy_url)
            .bearer_auth(&token)
            .json(&body)
            .send()
            .await
            .map_err(|e| StorageError::Io(std::io::Error::other(format!("Copy request failed: {}", e))))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(StorageError::InvalidUrl {
                url: copy_url,
                reason: format!("Server-side copy failed (HTTP {}): {}", status, body),
            });
        }

        debug!(src_key, dst_key, "Google Drive server-side copy completed");
        Ok(())
    }
}

fn dst_file_name_or<'a>(dst: &'a str, fallback: &'a str) -> &'a str {
    if dst.is_empty() {
        fallback
    } else {
        dst
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_gdrive_urls() {
        let (rem, pref) = GoogleDriveBackend::parse_target("gdrive://Documents").unwrap();
        assert_eq!(rem, "gdrive");
        assert_eq!(pref, "Documents");

        let (rem, pref) = GoogleDriveBackend::parse_target("gdrive://gdrive/Documents").unwrap();
        assert_eq!(rem, "gdrive");
        assert_eq!(pref, "Documents");

        let (rem, pref) = GoogleDriveBackend::parse_target("googledrive://renders/raw").unwrap();
        assert_eq!(rem, "gdrive");
        assert_eq!(pref, "renders/raw");

        let (rem, pref) = GoogleDriveBackend::parse_target("rclone://gdrive/Documents").unwrap();
        assert_eq!(rem, "gdrive");
        assert_eq!(pref, "Documents");

        let (rem, pref) = GoogleDriveBackend::parse_target("gdrive://").unwrap();
        assert_eq!(rem, "gdrive");
        assert_eq!(pref, "");
    }
}
