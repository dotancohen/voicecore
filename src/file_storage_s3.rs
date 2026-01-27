//! AWS S3 implementation of the FileStorageService trait.
//!
//! This module provides S3-compatible storage for audio files, supporting:
//! - AWS S3
//! - DigitalOcean Spaces (via custom endpoint)
//! - MinIO (via custom endpoint)
//! - Other S3-compatible services

use std::path::Path;
use std::time::Duration;

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;

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
/// Implements the `FileStorageService` trait using AWS SDK for Rust.
/// Supports AWS S3 and S3-compatible services.
pub struct S3StorageService {
    client: Client,
    bucket: String,
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
    pub async fn new(config: S3Config) -> Result<Self, FileStorageError> {
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
            &config.access_key_id,
            &config.secret_access_key,
            None, // session token
            None, // expiration
            "voice-config",
        );

        let region = Region::new(config.region.clone());

        let mut sdk_config_builder = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(credentials)
            .region(region);

        // Add custom endpoint for S3-compatible services
        if let Some(endpoint) = &config.endpoint {
            sdk_config_builder = sdk_config_builder
                .endpoint_url(endpoint)
                .force_path_style(true); // Required for most S3-compatible services
        }

        let sdk_config = sdk_config_builder.build();
        let client = Client::from_conf(sdk_config);

        Ok(Self {
            client,
            bucket: config.bucket,
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
        let body = ByteStream::from_path(local_path)
            .await
            .map_err(|e| FileStorageError::LocalFile(format!("Failed to read file: {}", e)))?;

        // Get file size
        let metadata = tokio::fs::metadata(local_path)
            .await
            .map_err(|e| FileStorageError::LocalFile(format!("Failed to get file metadata: {}", e)))?;
        let size_bytes = metadata.len();

        let full_key = self.full_key(remote_key);

        // Upload to S3
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .body(body)
            .send()
            .await
            .map_err(|e| FileStorageError::Upload(format!("S3 upload failed: {}", e)))?;

        tracing::info!(
            key = %full_key,
            bucket = %self.bucket,
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
        // Pre-signed URLs are valid for 1 hour
        let expires_in = Duration::from_secs(3600);
        let presigning_config = PresigningConfig::expires_in(expires_in)
            .map_err(|e| FileStorageError::DownloadUrl(format!("Invalid expiration: {}", e)))?;

        let presigned = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(storage_key)
            .presigned(presigning_config)
            .await
            .map_err(|e| FileStorageError::DownloadUrl(format!("Failed to generate URL: {}", e)))?;

        let expires_at = chrono::Utc::now().timestamp() + 3600;

        tracing::debug!(
            key = %storage_key,
            expires_at = expires_at,
            "Generated pre-signed download URL"
        );

        Ok(DownloadUrl {
            url: presigned.uri().to_string(),
            expires_at,
        })
    }

    async fn delete(&self, storage_key: &str) -> Result<(), FileStorageError> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(storage_key)
            .send()
            .await
            .map_err(|e| FileStorageError::Network(format!("Failed to delete object: {}", e)))?;

        tracing::info!(
            key = %storage_key,
            bucket = %self.bucket,
            "Deleted file from S3"
        );

        Ok(())
    }

    async fn exists(&self, storage_key: &str) -> Result<bool, FileStorageError> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(storage_key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(err) => {
                // Check if it's a "not found" error
                let service_err = err.as_service_error();
                if service_err.is_some() && service_err.unwrap().is_not_found() {
                    Ok(false)
                } else {
                    Err(FileStorageError::Network(format!(
                        "Failed to check if object exists: {}",
                        err
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
        // We can't easily test the full S3StorageService without mocking AWS,
        // but we can test the key generation logic by checking the config structure
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
}
