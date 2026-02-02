//! AWS SDK S3 implementation for Android.
//!
//! Uses the official AWS SDK which has better TLS handling on Android.

use std::path::Path;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::presigning::PresigningConfig;
use std::time::Duration;
use android_logger::{Config, FilterBuilder}; // Logging experiment

use crate::file_storage::{DownloadUrl, FileStorageError, FileStorageService, UploadResult};

pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub prefix: Option<String>,
    pub endpoint: Option<String>,
}

pub struct S3StorageService {
    client: Client,
    bucket: String,
    prefix: Option<String>,
}

impl S3StorageService {
    pub fn new(config: S3Config) -> Result<Self, FileStorageError> {
        let creds = Credentials::new(
            &config.access_key_id,
            &config.secret_access_key,
            None, None, "voicecore"
        );

        let mut builder = aws_sdk_s3::Config::builder()
            .credentials_provider(creds)
            .region(Region::new(config.region.clone()))
            .behavior_version_latest();

        if let Some(endpoint) = &config.endpoint {
            builder = builder.endpoint_url(endpoint).force_path_style(true);
        }

        let sdk_config = builder.build();
        let client = Client::from_conf(sdk_config);

        tracing::info!(bucket = %config.bucket, region = %config.region, "Created AWS SDK S3 client");

        Ok(Self {
            client,
            bucket: config.bucket,
            prefix: config.prefix,
        })
    }

    fn full_key(&self, key: &str) -> String {
        match &self.prefix {
            Some(p) => {
                let p = p.trim_end_matches('/');
                if p.is_empty() { key.to_string() } else { format!("{}/{}", p, key) }
            }
            None => key.to_string(),
        }
    }
}

impl FileStorageService for S3StorageService {
    async fn upload(&self, local_path: &Path, remote_key: &str) -> Result<UploadResult, FileStorageError> {

        tracing::info!("===== TEST MARKER - UPLOAD FUNCTION REACHED ====="); // Logging experiment
        tracing::info!(
            "Local path: {:?} | Remote key: {} | Bucket: {}",
            local_path,
            remote_key,
            self.bucket
        ); // Logging experiment

        tracing::info!("ANDROID RUST TEST: upload() started at {}", chrono::Utc::now()); // Logging experiment


        RUNTIME.block_on(async {
            let data = tokio::fs::read(local_path).await
                .map_err(|e| FileStorageError::LocalFile(e.to_string()))?;
            let size_bytes = data.len() as u64;
            let full_key = self.full_key(remote_key);

            tracing::info!(key = %full_key, size = size_bytes, "Uploading to S3 via AWS SDK");

            self.client.put_object()
                .bucket(&self.bucket)
                .key(&full_key)
                .body(data.into())
                .send()
                .await
                .map_err(|e| {
                    //tracing::error!(error = %e, "AWS SDK S3 upload failed");
                    tracing::error!(error = %e, error_debug = ?e, "AWS SDK S3 upload failed - full error chain");
                    FileStorageError::Upload(e.to_string())
                })?;

            Ok(UploadResult {
                storage_key: full_key,
                provider: "s3".to_string(),
                size_bytes,
            })
        })
    }

    async fn get_download_url(&self, storage_key: &str) -> Result<DownloadUrl, FileStorageError> {
        RUNTIME.block_on(async {
            let presign_config = PresigningConfig::expires_in(Duration::from_secs(3600))
                .map_err(|e| FileStorageError::DownloadUrl(e.to_string()))?;

            let presigned = self.client.get_object()
                .bucket(&self.bucket)
                .key(storage_key)
                .presigned(presign_config)
                .await
                .map_err(|e| FileStorageError::DownloadUrl(e.to_string()))?;

            Ok(DownloadUrl {
                url: presigned.uri().to_string(),
                expires_at: chrono::Utc::now().timestamp() + 3600,
            })
        })
    }

    async fn delete(&self, storage_key: &str) -> Result<(), FileStorageError> {
        RUNTIME.block_on(async {
            self.client.delete_object()
                .bucket(&self.bucket)
                .key(storage_key)
                .send()
                .await
                .map_err(|e| FileStorageError::Network(e.to_string()))?;
            Ok(())
        })
    }

    async fn exists(&self, storage_key: &str) -> Result<bool, FileStorageError> {
        RUNTIME.block_on(async {
            match self.client.head_object().bucket(&self.bucket).key(storage_key).send().await {
                Ok(_) => Ok(true),
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("404") || err_str.contains("NotFound") {
                        Ok(false)
                    } else {
                        Err(FileStorageError::Network(err_str))
                    }
                }
            }
        })
    }

    fn provider_name(&self) -> &'static str { "s3" }
}
