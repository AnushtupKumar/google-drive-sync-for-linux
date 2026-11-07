//! Cloud storage abstraction layer.
//!
//! ## Architecture
//!
//! ```text
//!   offload.rs / supervisor.rs
//!         │
//!         ▼
//!   CloudStorage trait  (this file)
//!         │
//!         ├─► ObjectStoreBackend  (rclone.rs)  — S3 / GCS / Azure via object_store
//!         └─► (future) RcloneRcdBackend        — rclone RCD HTTP JSON-RPC
//! ```
//!
//! ## Bandwidth limiting
//!
//! A [`BandwidthLimiter`] wraps a `governor::RateLimiter` configured to allow
//! `bps` tokens (bytes) per second.  The upload helper
//! [`read_file_with_limit`] reads the source file in fixed-size chunks,
//! acquiring tokens before each read so the byte rate is bounded.

pub mod gdrive;
pub mod rclone;
pub mod rclone_cli;

use async_trait::async_trait;
use bytes::Bytes;
use governor::{
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
    Quota, RateLimiter,
};
use sha2::{Digest, Sha256};
use std::{
    io,
    num::NonZeroU32,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use thiserror::Error;
use tokio::io::AsyncReadExt;
use tracing::debug;

// ─────────────────────────────────────────────
// Rate-limiter types
// ─────────────────────────────────────────────

/// A per-connection byte-budget rate limiter backed by the token-bucket
/// algorithm (governor crate).
///
/// One token ≡ one byte.  Call [`BandwidthLimiter::acquire`] before reading
/// each chunk; it sleeps until the budget is available.
#[derive(Clone)]
pub struct BandwidthLimiter {
    inner: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
    /// Maximum chunk size for `acquire` calls (must fit in `u32`).
    burst_bytes: u32,
}

impl BandwidthLimiter {
    /// Create a limiter allowing `bps` bytes per second.
    ///
    /// Returns `None` when `bps == 0` (unlimited).
    pub fn new(bps: u64) -> Option<Self> {
        if bps == 0 {
            return None;
        }
        // Clamp to u32 range for governor's NonZeroU32 quota
        let bps_u32 = u32::try_from(bps).unwrap_or(u32::MAX);
        let tokens = NonZeroU32::new(bps_u32).unwrap(); // safe: clamped above
        let quota = Quota::per_second(tokens);
        Some(Self {
            inner: Arc::new(RateLimiter::direct(quota)),
            burst_bytes: bps_u32,
        })
    }

    /// Block asynchronously until `n_bytes` tokens are available.
    ///
    /// Automatically caps the request to the burst capacity so the call
    /// never returns `InsufficientCapacity`.
    pub async fn acquire(&self, n_bytes: usize) {
        let capped = NonZeroU32::new((n_bytes as u32).min(self.burst_bytes))
            .unwrap_or(NonZeroU32::new(1).unwrap());
        // until_n_ready only errors on InsufficientCapacity which we've
        // prevented above by capping to burst_bytes.
        let _ = self.inner.until_n_ready(capped).await;
    }

    /// Return the maximum tokens / bytes permitted per burst.
    pub fn burst_bytes(&self) -> u32 {
        self.burst_bytes
    }
}

// ─────────────────────────────────────────────
// Error type
// ─────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("invalid remote target URL '{url}': {reason}")]
    InvalidUrl { url: String, reason: String },

    #[error("unsupported URL scheme '{scheme}' (supported: s3, gs, az, https, rclone, file)")]
    UnsupportedScheme { scheme: String },

    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("I/O error reading local file: {0}")]
    Io(#[from] io::Error),

    #[error("size mismatch after upload: uploaded {uploaded}, remote reports {remote}")]
    SizeMismatch { uploaded: u64, remote: u64 },

    #[error("SHA-256 mismatch after upload: local {local}, remote {remote}")]
    HashMismatch { local: String, remote: String },

    #[error("rclone execution error: {0}")]
    Rclone(String),
}

// ─────────────────────────────────────────────
// Shared data types
// ─────────────────────────────────────────────

/// Outcome of a successful upload operation.
#[derive(Debug, Clone)]
pub struct UploadInfo {
    /// The remote key that was written (relative to the bucket/container root).
    pub remote_key: String,
    /// Size of the object in bytes as confirmed by the upload.
    pub size_bytes: u64,
    /// Hex-encoded SHA-256 of the bytes transferred.
    pub sha256_hex: String,
    /// ETag returned by the storage backend (may be MD5, content hash, etc.).
    pub etag: Option<String>,
}

/// Metadata of an existing remote object.
#[derive(Debug, Clone)]
pub struct RemoteObjectMeta {
    pub remote_key: String,
    pub size_bytes: u64,
    pub etag: Option<String>,
}

// ─────────────────────────────────────────────
// CloudStorage trait
// ─────────────────────────────────────────────

/// Abstraction over cloud object storage backends.
///
/// Implemented by [`crate::storage::rclone::ObjectStoreBackend`].
/// All methods are cancel-safe; i.e. they can be dropped at `.await` points
/// without leaving the remote store in an inconsistent state (the object_store
/// crate provides this guarantee for standard PUT/DELETE operations).
#[async_trait]
pub trait CloudStorage: Send + Sync + 'static {
    /// Upload the file at `local_path` to `remote_key`.
    ///
    /// Streams the file through the bandwidth limiter (if provided), computing
    /// the SHA-256 digest in-flight, and reporting chunk progress via `progress`.
    /// Returns an [`UploadInfo`] on success.
    async fn put_file(
        &self,
        local_path: &Path,
        remote_key: &str,
        limiter: Option<&BandwidthLimiter>,
        progress: Option<Arc<AtomicU64>>,
    ) -> Result<UploadInfo, StorageError>;

    /// Retrieve metadata for `remote_key` without downloading content.
    async fn head_object(&self, remote_key: &str) -> Result<RemoteObjectMeta, StorageError>;

    /// Delete `remote_key` from the remote store.
    ///
    /// For `ContinuousBackup` mode this should be a **move** to a trash
    /// prefix (e.g. `.Trash-<uuid>/`) rather than a hard delete; that
    /// behaviour is the caller's responsibility via `trash_remote_key`.
    async fn delete_object(&self, remote_key: &str) -> Result<(), StorageError>;

    /// Server-side copy from `src_key` to `dst_key`.
    ///
    /// Used by `ContinuousBackup` to archive deleted files to a trash prefix
    /// without re-uploading them.
    async fn copy_object(&self, src_key: &str, dst_key: &str) -> Result<(), StorageError>;

    /// Build the remote key for a relative local path.
    ///
    /// Joins the configured prefix with `rel_path` using `/` as separator.
    fn remote_key_for(&self, rel_path: &str) -> String;

    /// Build the trash-archive key for a relative local path.
    ///
    /// Default: `.Trash-syncd/<rel_path>`.  Backends may override.
    fn trash_key_for(&self, rel_path: &str) -> String {
        format!(".Trash-syncd/{}", rel_path)
    }
}

// ─────────────────────────────────────────────
// File reading helper (rate-limited + SHA-256)
// ─────────────────────────────────────────────

/// Read the entire file at `local_path` in 256 KiB chunks, applying bandwidth
/// limiting and computing SHA-256 in-flight.
///
/// Returns `(content_bytes, sha256_hex)` on success.
///
/// This function should be called from inside a `tokio::task::spawn_blocking`
/// context for very large files, but since it is `async` and does sequential
/// I/O it works correctly in both contexts.
pub async fn read_file_with_limit(
    local_path: &Path,
    limiter: Option<&BandwidthLimiter>,
    progress: Option<&Arc<AtomicU64>>,
) -> Result<(Bytes, String), StorageError> {
    let mut file = tokio::fs::File::open(local_path).await?;

    // Choose chunk size: if rate-limited, cap to burst capacity so we never
    // request more tokens than the limiter can ever grant.
    let chunk_size: usize = if let Some(lim) = limiter {
        (lim.burst_bytes as usize).clamp(4 * 1024, 256 * 1024)
    } else {
        256 * 1024 // 256 KiB default
    };

    let file_size = file.metadata().await.map(|m| m.len()).unwrap_or(0);
    // Cap initial pre-allocation to 64 MiB to prevent OOM panics on huge files
    let initial_cap = (file_size as usize).min(64 * 1024 * 1024);
    let mut buf: Vec<u8> = Vec::with_capacity(initial_cap);
    let mut chunk = vec![0u8; chunk_size];
    let mut hasher = Sha256::new();

    loop {
        let n = file.read(&mut chunk).await?;
        if n == 0 {
            break;
        }

        // Acquire bandwidth tokens before counting this chunk
        if let Some(lim) = limiter {
            lim.acquire(n).await;
        }

        hasher.update(&chunk[..n]);
        buf.extend_from_slice(&chunk[..n]);

        if let Some(p) = progress {
            p.fetch_add(n as u64, Ordering::Relaxed);
        }

        debug!(bytes_read = buf.len(), "streaming upload chunk");
    }

    let sha256_hex = hex::encode(hasher.finalize());
    Ok((Bytes::from(buf), sha256_hex))
}

// ─────────────────────────────────────────────
// Storage backend factory
// ─────────────────────────────────────────────

/// Construct an appropriate [`CloudStorage`] backend for the given [`sync_common::config::FolderConfig`].
///
/// Automatically dispatches:
/// - `gdrive://<folder>` or `googledrive://<folder>` → [`gdrive::GoogleDriveBackend`] (pure Rust Google Drive v3)
/// - `rclone://gdrive/<folder>` → [`gdrive::GoogleDriveBackend`] (natively handled without rclone)
/// - `rclone://<other_remote>/<prefix>` → [`rclone_cli::RcloneBackend`]
/// - `s3://`, `gs://`, `az://`, `file://`, `https://` → [`rclone::ObjectStoreBackend`]
pub fn create_storage_backend(
    cfg: &sync_common::config::FolderConfig,
) -> Result<Arc<dyn CloudStorage>, StorageError> {
    let target = cfg.remote_target.trim();
    if target.starts_with("gdrive://")
        || target.starts_with("googledrive://")
        || target.starts_with("rclone://gdrive")
    {
        let backend = gdrive::GoogleDriveBackend::from_folder_config(cfg)?;
        Ok(Arc::new(backend))
    } else if target.starts_with("rclone://") {
        let backend = rclone_cli::RcloneBackend::from_folder_config(cfg)?;
        Ok(Arc::new(backend))
    } else {
        let backend = rclone::ObjectStoreBackend::from_folder_config(cfg)?;
        Ok(Arc::new(backend))
    }
}

/// Construct an appropriate [`CloudStorage`] backend for a raw `remote_target` URI string.
pub fn create_storage_backend_from_target(
    remote_target: &str,
) -> Result<Arc<dyn CloudStorage>, StorageError> {
    let target = remote_target.trim();
    if target.starts_with("gdrive://")
        || target.starts_with("googledrive://")
        || target.starts_with("rclone://gdrive")
    {
        let backend = gdrive::GoogleDriveBackend::from_remote_target(remote_target)?;
        Ok(Arc::new(backend))
    } else if target.starts_with("rclone://") {
        let backend = rclone_cli::RcloneBackend::from_remote_target(remote_target)?;
        Ok(Arc::new(backend))
    } else {
        let backend = rclone::ObjectStoreBackend::from_remote_target(remote_target)?;
        Ok(Arc::new(backend))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn test_read_file_with_limit_progress_counter() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("progress_test.bin");
        let payload = vec![0x42u8; 1024 * 1024]; // 1 MiB

        let mut file = tokio::fs::File::create(&file_path).await.unwrap();
        file.write_all(&payload).await.unwrap();
        file.flush().await.unwrap();
        drop(file);

        let progress = Arc::new(AtomicU64::new(0));
        let (data, hash) = read_file_with_limit(&file_path, None, Some(&progress))
            .await
            .unwrap();

        assert_eq!(data.len(), 1024 * 1024);
        assert_eq!(progress.load(Ordering::Relaxed), 1024 * 1024);
        assert!(!hash.is_empty());
    }

    #[tokio::test]
    async fn test_create_storage_backend_dispatch() {
        let rclone_backend = create_storage_backend_from_target("rclone://gdrive/Documents").unwrap();
        assert_eq!(rclone_backend.remote_key_for("test.txt"), "Documents/test.txt");

        let file_dir = tempfile::tempdir().unwrap();
        let file_url = format!("file://{}", file_dir.path().display());
        let file_backend = create_storage_backend_from_target(&file_url).unwrap();
        assert_eq!(file_backend.remote_key_for("test.txt"), "test.txt");
    }
}
