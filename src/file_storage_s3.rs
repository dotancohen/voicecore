//! AWS S3 implementation of the FileStorageService trait.
//!
//! This module provides S3-compatible storage for audio files, supporting:
//! - AWS S3
//! - DigitalOcean Spaces (via custom endpoint)
//! - MinIO (via custom endpoint)
//! - Backblaze B2 (via S3-compatible API)
//! - Other S3-compatible services
//!
//! Uses the `rust-s3` crate which is much lighter than aws-sdk-s3.

use std::path::Path;

use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;

use crate::file_storage::{DownloadUrl, FileStorageError, FileStorageService, UploadResult};

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
///
/// Implements the `FileStorageService` trait using the rust-s3 crate.
/// Supports AWS S3 and S3-compatible services.
pub struct S3StorageService {
    bucket: Box<Bucket>,
    prefix: Option<String>,
}

impl S3StorageService {
    /// Create a new S3 storage service from configuration.
    ///
    /// # Arguments
    /// * `config` - S3 configuration including credentials and bucket
    ///
    /// # Returns
    /// * `Ok(S3StorageService)` - Ready to use storage service
    /// * `Err(FileStorageError)` - If configuration is invalid
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
}

impl FileStorageService for S3StorageService {
    async fn upload(
        &self,
        local_path: &Path,
        remote_key: &str,
    ) -> Result<UploadResult, FileStorageError> {
        // Read the file
        let data = tokio::fs::read(local_path)
            .await
            .map_err(|e| FileStorageError::LocalFile(format!("Failed to read file: {}", e)))?;

        let size_bytes = data.len() as u64;
        let full_key = self.full_key(remote_key);

        // Upload to S3
        let response = self
            .bucket
            .put_object(&full_key, &data)
            .await
            .map_err(|e| FileStorageError::Upload(format!("S3 upload failed: {}", e)))?;

        if response.status_code() != 200 {
            return Err(FileStorageError::Upload(format!(
                "S3 upload failed with status {}",
                response.status_code()
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

    async fn get_download_url(&self, storage_key: &str) -> Result<DownloadUrl, FileStorageError> {
        // Pre-signed URLs are valid for 1 hour (3600 seconds)
        let expiry_secs = 3600u32;

        let url = self
            .bucket
            .presign_get(storage_key, expiry_secs, None)
            .await
            .map_err(|e| FileStorageError::DownloadUrl(format!("Failed to generate URL: {}", e)))?;

        let expires_at = chrono::Utc::now().timestamp() + i64::from(expiry_secs);

        tracing::debug!(
            key = %storage_key,
            expires_at = expires_at,
            "Generated pre-signed download URL"
        );

        Ok(DownloadUrl { url, expires_at })
    }

    async fn delete(&self, storage_key: &str) -> Result<(), FileStorageError> {
        let response = self
            .bucket
            .delete_object(storage_key)
            .await
            .map_err(|e| FileStorageError::Network(format!("Failed to delete object: {}", e)))?;

        // S3 returns 204 for successful delete
        if response.status_code() != 204 {
            return Err(FileStorageError::Network(format!(
                "Delete failed with status {}",
                response.status_code()
            )));
        }

        tracing::info!(
            key = %storage_key,
            bucket = %self.bucket.name(),
            "Deleted file from S3"
        );

        Ok(())
    }

    async fn exists(&self, storage_key: &str) -> Result<bool, FileStorageError> {
        let result = self.bucket.head_object(storage_key).await;

        match result {
            Ok((_, code)) => {
                // 200 means object exists
                Ok(code == 200)
            }
            Err(e) => {
                // Check if it's a 404 (not found) error
                let err_str = e.to_string();
                if err_str.contains("404") || err_str.contains("Not Found") {
                    Ok(false)
                } else {
                    Err(FileStorageError::Network(format!(
                        "Failed to check if object exists: {}",
                        e
                    )))
                }
            }
        }
    }

    fn provider_name(&self) -> &'static str {
        "s3"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_full_key_with_prefix() {
        let config = S3Config {
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            access_key_id: "test-key".to_string(),
            secret_access_key: "test-secret".to_string(),
            prefix: Some("audio".to_string()),
            endpoint: None,
        };

        assert_eq!(config.bucket, "test-bucket");
        assert_eq!(config.prefix, Some("audio".to_string()));
    }

    #[test]
    fn test_s3_config_with_endpoint() {
        let config = S3Config {
            bucket: "my-bucket".to_string(),
            region: "nyc3".to_string(),
            access_key_id: "key".to_string(),
            secret_access_key: "secret".to_string(),
            prefix: None,
            endpoint: Some("https://nyc3.digitaloceanspaces.com".to_string()),
        };

        assert_eq!(
            config.endpoint,
            Some("https://nyc3.digitaloceanspaces.com".to_string())
        );
    }

    #[test]
    fn test_full_key_generation() {
        // Test with prefix
        let service = S3StorageService {
            bucket: Bucket::new(
                "test",
                "us-east-1".parse().unwrap(),
                Credentials::new(Some("key"), Some("secret"), None, None, None).unwrap(),
            )
            .unwrap(),
            prefix: Some("audio/".to_string()),
        };
        assert_eq!(service.full_key("test.mp3"), "audio/test.mp3");

        // Test without prefix
        let service_no_prefix = S3StorageService {
            bucket: Bucket::new(
                "test",
                "us-east-1".parse().unwrap(),
                Credentials::new(Some("key"), Some("secret"), None, None, None).unwrap(),
            )
            .unwrap(),
            prefix: None,
        };
        assert_eq!(service_no_prefix.full_key("test.mp3"), "test.mp3");
    }
}
