//! AWS S3 implementation of the FileStorageService trait.
//!
//! This module provides S3-compatible storage for audio files, supporting:
//! - AWS S3
//! - DigitalOcean Spaces (via custom endpoint)
//! - MinIO (via custom endpoint)
//! - Backblaze B2 (via S3-compatible API)
//! - Other S3-compatible services
//!
//! Uses the `rust-s3` crate (0.37+), which talks HTTP through reqwest with the
//! bundled webpki root certificates. That is what makes the same code work on
//! Android: earlier rust-s3 versions looked for an OS certificate directory,
//! which Android does not have, so every TLS handshake failed there.

use std::path::Path;
use std::time::Duration;

use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;

use crate::file_storage::{DownloadUrl, FileStorageError, FileStorageService, UploadResult};

/// Per-request timeout. Audio files can be large and connections slow (mobile
/// data), so this is deliberately generous; connection failures still fail
/// fast because they error before any transfer starts.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15 * 60);

/// Suffix for the temporary file used while a download is in progress.
const PARTIAL_SUFFIX: &str = ".part";

/// Configuration for S3 storage service.
#[derive(Debug, Clone)]
pub struct S3Config {
    /// S3 bucket name
    pub bucket: String,
    /// AWS region (e.g., "us-east-1")
    pub region: String,
    /// AWS access key ID
    pub access_key_id: String,
    /// AWS secret access key
    pub secret_access_key: String,
    /// Optional path prefix for all objects (e.g., "audio/")
    pub prefix: Option<String>,
    /// Optional custom endpoint for S3-compatible services
    /// (e.g., "https://nyc3.digitaloceanspaces.com")
    pub endpoint: Option<String>,
}

/// S3-based file storage service.
pub struct S3StorageService {
    bucket: Box<Bucket>,
    prefix: Option<String>,
}

impl S3StorageService {
    /// Create a new S3 storage service from configuration.
    pub fn new(config: S3Config) -> Result<Self, FileStorageError> {
        if config.bucket.is_empty() {
            return Err(FileStorageError::Config("bucket name is required".to_string()));
        }
        if config.access_key_id.is_empty() {
            return Err(FileStorageError::Config("access_key_id is required".to_string()));
        }
        if config.secret_access_key.is_empty() {
            return Err(FileStorageError::Config("secret_access_key is required".to_string()));
        }

        let credentials = Credentials::new(
            Some(&config.access_key_id),
            Some(&config.secret_access_key),
            None, // security token
            None, // session token
            None, // profile
        )
        .map_err(|e| FileStorageError::Config(format!("Invalid credentials: {}", e)))?;

        let region = if let Some(endpoint) = &config.endpoint {
            Region::Custom {
                region: config.region.clone(),
                endpoint: endpoint.clone(),
            }
        } else {
            config.region.parse().map_err(|e| {
                FileStorageError::Config(format!("Invalid region '{}': {}", config.region, e))
            })?
        };

        let mut bucket = Bucket::new(&config.bucket, region, credentials)
            .map_err(|e| FileStorageError::Config(format!("Failed to create bucket: {}", e)))?;

        // Use path style for S3-compatible services (MinIO, DigitalOcean Spaces, etc.)
        if config.endpoint.is_some() {
            bucket = bucket.with_path_style();
        }

        bucket = bucket
            .with_request_timeout(REQUEST_TIMEOUT)
            .map_err(|e| FileStorageError::Config(format!("Failed to configure HTTP client: {}", e)))?;

        tracing::debug!(
            bucket = %config.bucket,
            region = %config.region,
            prefix = ?config.prefix,
            has_endpoint = config.endpoint.is_some(),
            "Created S3 storage service"
        );

        Ok(Self {
            bucket,
            prefix: config.prefix,
        })
    }

    /// Get the full storage key with prefix applied.
    fn full_key(&self, key: &str) -> String {
        match &self.prefix {
            Some(prefix) => {
                let prefix = prefix.trim_end_matches('/');
                if prefix.is_empty() {
                    key.to_string()
                } else {
                    format!("{}/{}", prefix, key)
                }
            }
            None => key.to_string(),
        }
    }

    /// Map an S3 error to the closest FileStorageError variant.
    fn map_error(err: s3::error::S3Error, what: &str, key: &str) -> FileStorageError {
        use s3::error::S3Error;
        match err {
            S3Error::HttpFailWithBody(404, _) => FileStorageError::NotFound(key.to_string()),
            S3Error::HttpFailWithBody(401, _) | S3Error::HttpFailWithBody(403, _) => {
                FileStorageError::Auth(format!("{} rejected for {}: {}", what, key, err))
            }
            S3Error::HttpFailWithBody(code, body) => {
                FileStorageError::Network(format!("{} of {} failed with HTTP {}: {}", what, key, code, body.trim()))
            }
            S3Error::Reqwest(e) => FileStorageError::Network(format!("{} of {}: {}", what, key, e)),
            S3Error::Io(e) => FileStorageError::LocalFile(format!("{} of {}: {}", what, key, e)),
            other => FileStorageError::Network(format!("{} of {}: {}", what, key, other)),
        }
    }

    /// Path of the temporary file used while downloading `local_path`.
    fn partial_path(local_path: &Path) -> std::path::PathBuf {
        let mut name = local_path
            .file_name()
            .map(|n| n.to_os_string())
            .unwrap_or_default();
        name.push(PARTIAL_SUFFIX);
        local_path.with_file_name(name)
    }
}

impl FileStorageService for S3StorageService {
    async fn upload(
        &self,
        local_path: &Path,
        remote_key: &str,
    ) -> Result<UploadResult, FileStorageError> {
        let metadata = tokio::fs::metadata(local_path)
            .await
            .map_err(|e| FileStorageError::LocalFile(format!("Failed to read {}: {}", local_path.display(), e)))?;
        let size_bytes = metadata.len();
        let full_key = self.full_key(remote_key);

        tracing::debug!(
            key = %full_key,
            bucket = %self.bucket.name(),
            size_bytes = size_bytes,
            "Uploading to S3"
        );

        // Stream from disk instead of reading the whole file into memory; the
        // library switches to multipart upload for large files.
        let mut file = tokio::fs::File::open(local_path)
            .await
            .map_err(|e| FileStorageError::LocalFile(format!("Failed to open {}: {}", local_path.display(), e)))?;

        let response = self
            .bucket
            .put_object_stream(&mut file, &full_key)
            .await
            .map_err(|e| {
                let err = Self::map_error(e, "Upload", &full_key);
                tracing::error!(error = %err, key = %full_key, "S3 upload failed");
                match err {
                    // Anything the service refused counts as an upload failure
                    FileStorageError::Network(msg) => FileStorageError::Upload(msg),
                    other => other,
                }
            })?;

        let status = response.status_code();
        if !(200..300).contains(&status) {
            return Err(FileStorageError::Upload(format!(
                "S3 upload of {} failed with status {}",
                full_key, status
            )));
        }

        tracing::info!(
            key = %full_key,
            bucket = %self.bucket.name(),
            size_bytes = size_bytes,
            "Uploaded file to S3"
        );

        Ok(UploadResult {
            storage_key: full_key,
            provider: "s3".to_string(),
            size_bytes,
        })
    }

    async fn download(&self, storage_key: &str, local_path: &Path) -> Result<u64, FileStorageError> {
        // 1. Ask for the object metadata first: a clear NotFound/Auth error,
        //    and the expected size for verification afterwards.
        let (head, status) = self
            .bucket
            .head_object(storage_key)
            .await
            .map_err(|e| Self::map_error(e, "Download", storage_key))?;
        if status == 404 {
            return Err(FileStorageError::NotFound(storage_key.to_string()));
        }
        if !(200..300).contains(&status) {
            return Err(FileStorageError::Download(format!(
                "HEAD {} returned status {}",
                storage_key, status
            )));
        }
        let expected_len = head.content_length.and_then(|n| u64::try_from(n).ok());

        // 2. Stream into a temporary file next to the destination.
        if let Some(parent) = local_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                FileStorageError::LocalFile(format!("Failed to create {}: {}", parent.display(), e))
            })?;
        }
        let partial = Self::partial_path(local_path);

        let result = async {
            let mut file = tokio::fs::File::create(&partial).await.map_err(|e| {
                FileStorageError::LocalFile(format!("Failed to create {}: {}", partial.display(), e))
            })?;

            let status = self
                .bucket
                .get_object_to_writer(storage_key, &mut file)
                .await
                .map_err(|e| Self::map_error(e, "Download", storage_key))?;
            if !(200..300).contains(&status) {
                return Err(FileStorageError::Download(format!(
                    "GET {} returned status {}",
                    storage_key, status
                )));
            }

            use tokio::io::AsyncWriteExt;
            file.flush().await?;
            file.sync_all().await?;
            let written = file.metadata().await?.len();
            drop(file);

            // 3. Verify before making the file visible.
            if let Some(expected) = expected_len {
                if written != expected {
                    return Err(FileStorageError::Download(format!(
                        "Incomplete download of {}: got {} bytes, expected {}",
                        storage_key, written, expected
                    )));
                }
            }

            // 4. Atomically move into place.
            tokio::fs::rename(&partial, local_path).await.map_err(|e| {
                FileStorageError::LocalFile(format!(
                    "Failed to move {} to {}: {}",
                    partial.display(),
                    local_path.display(),
                    e
                ))
            })?;

            Ok(written)
        }
        .await;

        if result.is_err() {
            let _ = tokio::fs::remove_file(&partial).await;
        }
        result
    }

    async fn get_download_url(&self, storage_key: &str) -> Result<DownloadUrl, FileStorageError> {
        // Pre-signed URLs are valid for 1 hour (3600 seconds)
        let expiry_secs = 3600u32;

        let url = self
            .bucket
            .presign_get(storage_key, expiry_secs, None)
            .await
            .map_err(|e| FileStorageError::DownloadUrl(format!("Failed to generate URL: {}", e)))?;

        let expires_at = chrono::Utc::now().timestamp() + i64::from(expiry_secs);

        Ok(DownloadUrl { url, expires_at })
    }

    async fn delete(&self, storage_key: &str) -> Result<(), FileStorageError> {
        let response = self
            .bucket
            .delete_object(storage_key)
            .await
            .map_err(|e| Self::map_error(e, "Delete", storage_key))?;

        // S3 returns 204 for successful delete
        let status = response.status_code();
        if !(200..300).contains(&status) && status != 404 {
            return Err(FileStorageError::Network(format!(
                "Delete of {} failed with status {}",
                storage_key, status
            )));
        }

        tracing::info!(key = %storage_key, bucket = %self.bucket.name(), "Deleted file from S3");
        Ok(())
    }

    async fn exists(&self, storage_key: &str) -> Result<bool, FileStorageError> {
        match self.bucket.head_object(storage_key).await {
            Ok((_, code)) => Ok(code == 200),
            Err(e) => match Self::map_error(e, "Exists check", storage_key) {
                FileStorageError::NotFound(_) => Ok(false),
                other => Err(other),
            },
        }
    }

    fn provider_name(&self) -> &'static str {
        "s3"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn service(prefix: Option<&str>) -> S3StorageService {
        S3StorageService::new(S3Config {
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            access_key_id: "test-key".to_string(),
            secret_access_key: "test-secret".to_string(),
            prefix: prefix.map(String::from),
            endpoint: None,
        })
        .unwrap()
    }

    #[test]
    fn test_new_rejects_missing_fields() {
        let bad = S3Config {
            bucket: "".to_string(),
            region: "us-east-1".to_string(),
            access_key_id: "k".to_string(),
            secret_access_key: "s".to_string(),
            prefix: None,
            endpoint: None,
        };
        assert!(matches!(S3StorageService::new(bad), Err(FileStorageError::Config(_))));
    }

    #[test]
    fn test_s3_config_with_endpoint_uses_custom_region() {
        let svc = S3StorageService::new(S3Config {
            bucket: "my-bucket".to_string(),
            region: "nyc3".to_string(),
            access_key_id: "key".to_string(),
            secret_access_key: "secret".to_string(),
            prefix: None,
            endpoint: Some("https://nyc3.digitaloceanspaces.com".to_string()),
        });
        assert!(svc.is_ok());
    }

    #[test]
    fn test_full_key_generation() {
        assert_eq!(service(Some("audio/")).full_key("test.mp3"), "audio/test.mp3");
        assert_eq!(service(Some("audio")).full_key("test.mp3"), "audio/test.mp3");
        assert_eq!(service(Some("")).full_key("test.mp3"), "test.mp3");
        assert_eq!(service(None).full_key("test.mp3"), "test.mp3");
    }

    #[test]
    fn test_partial_path_keeps_full_name() {
        let p = S3StorageService::partial_path(Path::new("/audio/abc.mp3"));
        assert_eq!(p, std::path::PathBuf::from("/audio/abc.mp3.part"));
    }

    #[test]
    fn test_map_error_classification() {
        use s3::error::S3Error;
        assert!(matches!(
            S3StorageService::map_error(S3Error::HttpFailWithBody(404, String::new()), "Download", "k"),
            FileStorageError::NotFound(_)
        ));
        assert!(matches!(
            S3StorageService::map_error(S3Error::HttpFailWithBody(403, "denied".into()), "Download", "k"),
            FileStorageError::Auth(_)
        ));
        assert!(matches!(
            S3StorageService::map_error(S3Error::HttpFailWithBody(500, "boom".into()), "Download", "k"),
            FileStorageError::Network(_)
        ));
    }
}
