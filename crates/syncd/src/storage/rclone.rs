//! Production [`CloudStorage`] implementation backed by the `object_store` crate.
//!
//! ## Supported URL schemes
//!
//! | `remote_target` prefix | Backend                        | Auth source           |
//! |------------------------|--------------------------------|-----------------------|
//! | `s3://bucket/prefix`   | Amazon S3 (or S3-compatible)   | Env / IAM role / IRSA |
//! | `gs://bucket/prefix`   | Google Cloud Storage           | GOOGLE_APPLICATION_CREDENTIALS / ADC |
//! | `az://container/prefix`| Azure Blob Storage             | AZURE_STORAGE_* env   |
//! | `https://...`          | HTTP/HTTPS (public or signed)  | None / bearer token   |
//!
//! Credentials are read from environment variables by the `object_store` crate
//! builders using `from_env()`.  For production deployments, set the relevant
//! environment variables or mount a credentials file before starting `syncd`.
//!
//! ## Retry
//!
//! `put_file`, `head_object`, and `delete_object` are automatically retried
//! with exponential back-off using the `backon` crate.  The retry predicate
//! excludes 4xx errors (which are considered permanent).

use std::{
    path::Path,
    sync::{atomic::AtomicU64, Arc},
};

use async_trait::async_trait;
use backon::{ExponentialBuilder, Retryable};
use object_store::{
    path::Path as OsPath, ObjectStore, PutPayload,
};
use tracing::{debug, info, warn};
use url::Url;

use super::{
    BandwidthLimiter, CloudStorage, RemoteObjectMeta, StorageError, UploadInfo,
    read_file_with_limit,
};

// ─────────────────────────────────────────────
// Retry policy
// ─────────────────────────────────────────────

/// Maximum upload/download retries before bubbling the error up.
const MAX_RETRIES: usize = 5;

fn is_retryable(e: &StorageError) -> bool {
    match e {
        // object_store classifies 5xx and network errors as transient
        StorageError::ObjectStore(oe) => {
            !matches!(
                oe,
                object_store::Error::NotFound { .. }
                    | object_store::Error::NotSupported { .. }
                    | object_store::Error::AlreadyExists { .. }
                    | object_store::Error::Precondition { .. }
                    | object_store::Error::NotModified { .. }
            )
        }
        StorageError::Io(_) => true,
        StorageError::Rclone(_) => true,
        // Size/hash mismatches are logic errors, not transient
        _ => false,
    }
}

fn retry_builder() -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_max_times(MAX_RETRIES)
        .with_min_delay(std::time::Duration::from_millis(200))
        .with_max_delay(std::time::Duration::from_secs(30))
}

// ─────────────────────────────────────────────
// Backend
// ─────────────────────────────────────────────

/// A [`CloudStorage`] backend built on the `object_store` crate.
///
/// Supports S3, GCS, Azure Blob, and HTTP/HTTPS stores.
/// Construct with [`ObjectStoreBackend::from_remote_target`].
pub struct ObjectStoreBackend {
    store: Arc<dyn ObjectStore>,
    /// Remote key prefix derived from the URL path component.
    /// e.g. for `s3://bucket/backups/photos` → `"backups/photos"`.
    prefix: String,
}

impl ObjectStoreBackend {
    /// Parse `remote_target` and construct an `ObjectStoreBackend`.
    ///
    /// `remote_target` must be a URL of the form `<scheme>://<bucket>/<prefix>`,
    /// e.g. `s3://my-bucket/backups/photos`.  Credentials are read from
    /// environment variables by the underlying `object_store` builder.
    pub fn from_folder_config(cfg: &sync_common::config::FolderConfig) -> Result<Self, StorageError> {
        Self::from_remote_target_with_options(
            &cfg.remote_target,
            cfg.endpoint.as_deref(),
            cfg.region.as_deref(),
            cfg.allow_http,
        )
    }

    pub fn from_remote_target(remote_target: &str) -> Result<Self, StorageError> {
        Self::from_remote_target_with_options(remote_target, None, None, false)
    }

    pub fn from_remote_target_with_options(
        remote_target: &str,
        endpoint: Option<&str>,
        region: Option<&str>,
        allow_http: bool,
    ) -> Result<Self, StorageError> {
        let url = Url::parse(remote_target).map_err(|e| StorageError::InvalidUrl {
            url: remote_target.to_owned(),
            reason: e.to_string(),
        })?;

        let mut prefix = url
            .path()
            .trim_start_matches('/')
            .trim_end_matches('/')
            .to_owned();

        let bucket_or_container = url
            .host_str()
            .unwrap_or("")
            .to_owned();

        let store: Arc<dyn ObjectStore> = match url.scheme() {
            "file" => {
                let path = url.to_file_path().map_err(|_| StorageError::InvalidUrl {
                    url: remote_target.to_owned(),
                    reason: "Invalid file URL path".to_owned(),
                })?;
                std::fs::create_dir_all(&path)?;

                // Detect if the directory is on an rclone FUSE mount
                if let Ok(mounts) = proc_mounts::MountIter::new() {
                    for mount in mounts.flatten() {
                        if path.starts_with(&mount.dest) && mount.fstype == "fuse.rclone" {
                            warn!(
                                path = %path.display(),
                                mount = %mount.dest.display(),
                                source = %mount.source.display(),
                                "Destination folder is on an rclone FUSE mount (fuse.rclone). \
                                 File writes are buffered locally by rclone VFS and uploaded asynchronously \
                                 in the background, causing instant upload blinks in syncd. \
                                 Recommendation: use remote_target = \"rclone://{}/<folder>\" for direct sync \
                                 with real-time progress and without needing to keep rclone mounted.",
                                mount.source.display()
                            );
                            break;
                        }
                    }
                }

                let local = object_store::local::LocalFileSystem::new_with_prefix(&path)
                    .map_err(StorageError::ObjectStore)?;
                prefix = String::new();
                Arc::new(local)
            }
            "s3" => {
                if std::env::var("AWS_EC2_METADATA_DISABLED").is_err() {
                    std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
                }
                let mut builder = object_store::aws::AmazonS3Builder::from_env()
                    .with_bucket_name(&bucket_or_container);
                if let Some(ep) = endpoint {
                    builder = builder.with_endpoint(ep);
                }
                if let Some(reg) = region {
                    builder = builder.with_region(reg);
                }
                if allow_http {
                    builder = builder.with_allow_http(true);
                }
                let s3 = builder.build().map_err(StorageError::ObjectStore)?;
                Arc::new(s3)
            }
            "gs" => {
                let gcs = object_store::gcp::GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(&bucket_or_container)
                    .build()
                    .map_err(StorageError::ObjectStore)?;
                Arc::new(gcs)
            }
            "az" | "azure" | "abfs" | "abfss" => {
                let azure = object_store::azure::MicrosoftAzureBuilder::from_env()
                    .with_container_name(&bucket_or_container)
                    .build()
                    .map_err(StorageError::ObjectStore)?;
                Arc::new(azure)
            }
            "http" | "https" => {
                let http = object_store::http::HttpBuilder::new()
                    .with_url(remote_target)
                    .build()
                    .map_err(StorageError::ObjectStore)?;
                Arc::new(http)
            }
            other => {
                return Err(StorageError::UnsupportedScheme {
                    scheme: other.to_owned(),
                })
            }
        };

        info!(remote_target, prefix, "ObjectStoreBackend constructed");
        Ok(Self { store, prefix })
    }

    /// Build an `object_store::path::Path` for `rel_path` under our prefix.
    fn os_path(&self, rel_path: &str) -> OsPath {
        let key = if self.prefix.is_empty() {
            rel_path.to_owned()
        } else {
            format!("{}/{}", self.prefix, rel_path)
        };
        OsPath::from(key)
    }
}

// ─────────────────────────────────────────────
// CloudStorage impl
// ─────────────────────────────────────────────

#[async_trait]
impl CloudStorage for ObjectStoreBackend {
    fn remote_key_for(&self, rel_path: &str) -> String {
        if self.prefix.is_empty() {
            rel_path.to_owned()
        } else {
            format!("{}/{}", self.prefix, rel_path)
        }
    }

    async fn put_file(
        &self,
        local_path: &Path,
        remote_key: &str,
        limiter: Option<&BandwidthLimiter>,
        progress: Option<Arc<AtomicU64>>,
    ) -> Result<UploadInfo, StorageError> {
        let store = Arc::clone(&self.store);
        let os_path = self.os_path(remote_key);
        let local_path = local_path.to_owned();
        let remote_key = remote_key.to_owned();

        // Read the file with optional rate-limiting, computing SHA-256 in-flight.
        let (data, sha256_hex) = read_file_with_limit(&local_path, limiter, progress.as_ref()).await?;
        let size_bytes = data.len() as u64;

        debug!(
            path = ?local_path,
            remote_key,
            size_bytes,
            sha256 = %sha256_hex,
            "uploading to object store"
        );

        let payload = PutPayload::from(data);

        let result = {
            let store = Arc::clone(&store);
            let os_path = os_path.clone();
            let payload = payload.clone();

            (move || {
                let store = Arc::clone(&store);
                let os_path = os_path.clone();
                let payload = payload.clone();
                async move {
                    store.put(&os_path, payload).await.map_err(StorageError::ObjectStore)
                }
            })
            .retry(retry_builder())
            .when(is_retryable)
            .await?
        };

        let etag = result.e_tag;

        info!(
            path = ?local_path,
            remote_key,
            size_bytes,
            sha256 = %sha256_hex,
            etag = ?etag,
            "upload complete"
        );

        Ok(UploadInfo {
            remote_key,
            size_bytes,
            sha256_hex,
            etag,
        })
    }

    async fn head_object(&self, remote_key: &str) -> Result<RemoteObjectMeta, StorageError> {
        let store = Arc::clone(&self.store);
        let os_path = self.os_path(remote_key);
        let remote_key = remote_key.to_owned();

        let meta = {
            let store = Arc::clone(&store);
            let os_path = os_path.clone();

            (move || {
                let store = Arc::clone(&store);
                let os_path = os_path.clone();
                async move {
                    store.head(&os_path).await.map_err(StorageError::ObjectStore)
                }
            })
            .retry(retry_builder())
            .when(is_retryable)
            .await?
        };

        Ok(RemoteObjectMeta {
            remote_key,
            size_bytes: meta.size as u64,
            etag: meta.e_tag,
        })
    }

    async fn delete_object(&self, remote_key: &str) -> Result<(), StorageError> {
        let store = Arc::clone(&self.store);
        let os_path = self.os_path(remote_key);

        {
            let store = Arc::clone(&store);
            let os_path = os_path.clone();

            (move || {
                let store = Arc::clone(&store);
                let os_path = os_path.clone();
                async move {
                    store.delete(&os_path).await.map_err(StorageError::ObjectStore)
                }
            })
            .retry(retry_builder())
            .when(is_retryable)
            .await?
        };

        debug!(remote_key, "object deleted from store");
        Ok(())
    }

    async fn copy_object(&self, src_key: &str, dst_key: &str) -> Result<(), StorageError> {
        let store = Arc::clone(&self.store);
        let src_path = self.os_path(src_key);
        let dst_path = self.os_path(dst_key);

        let copy_result = {
            let store = Arc::clone(&store);
            let src = src_path.clone();
            let dst = dst_path.clone();

            (move || {
                let store = Arc::clone(&store);
                let src = src.clone();
                let dst = dst.clone();
                async move {
                    store.copy(&src, &dst).await.map_err(StorageError::ObjectStore)
                }
            })
            .retry(retry_builder())
            .when(is_retryable)
            .await
        };

        if let Err(e) = copy_result {
            debug!(src_key, dst_key, "server-side copy failed ({}); falling back to stream copy", e);
            let get_res = store.get(&src_path).await.map_err(StorageError::ObjectStore)?;
            let bytes = get_res.bytes().await.map_err(StorageError::ObjectStore)?;
            store
                .put(&dst_path, PutPayload::from(bytes))
                .await
                .map_err(StorageError::ObjectStore)?;
        }

        debug!(src_key, dst_key, "object copied to destination");
        Ok(())
    }
}

// ─────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn test_file_scheme_roundtrip() {
        let tmp_root = std::env::temp_dir().join(format!("syncd-test-store-{}", uuid::Uuid::new_v4()));
        let remote_url = format!("file://{}", tmp_root.display());

        let backend = ObjectStoreBackend::from_remote_target(&remote_url)
            .expect("Local file backend should construct");

        // Create a local dummy file to upload
        let src_dir = std::env::temp_dir().join(format!("syncd-test-src-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&src_dir).await.unwrap();
        let src_file = src_dir.join("hello.txt");
        {
            let mut f = tokio::fs::File::create(&src_file).await.unwrap();
            f.write_all(b"Hello Cloud Sync!").await.unwrap();
        }

        // 1. Put file
        let info = backend.put_file(&src_file, "hello.txt", None, None).await.unwrap();
        assert_eq!(info.size_bytes, 17);

        // 2. Head object
        let meta = backend.head_object("hello.txt").await.unwrap();
        assert_eq!(meta.size_bytes, 17);

        // 3. Copy object
        backend.copy_object("hello.txt", "trash/hello.txt").await.unwrap();
        let trash_meta = backend.head_object("trash/hello.txt").await.unwrap();
        assert_eq!(trash_meta.size_bytes, 17);

        // 4. Delete object
        backend.delete_object("hello.txt").await.unwrap();
        assert!(backend.head_object("hello.txt").await.is_err());

        // Cleanup
        let _ = tokio::fs::remove_dir_all(&tmp_root).await;
        let _ = tokio::fs::remove_dir_all(&src_dir).await;
    }
}
