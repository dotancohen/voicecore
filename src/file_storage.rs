//! Abstract file storage service for cloud storage.
//!
//! This module provides a trait-based abstraction for storing audio files
//! in cloud storage providers (AWS S3, Backblaze B2, DigitalOcean Spaces).
//!
//! The design allows:
//! - Database metadata syncs to the server as before
//! - Binary audio files are uploaded to cloud storage
//! - Devices download files on-demand for playback via pre-signed URLs

use std::fmt;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// Result of a successful file upload to cloud storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadResult {
    /// Object key/path in storage (e.g., "audio/019abc123.mp3")
    pub storage_key: String,
    /// Storage provider identifier (e.g., "s3", "backblaze", "digitalocean")
    pub provider: String,
    /// File size in bytes
    pub size_bytes: u64,
}

/// A pre-signed download URL with expiration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadUrl {
    /// The pre-signed URL for downloading the file
    pub url: String,
    /// Unix timestamp when the URL expires
    pub expires_at: i64,
}

/// Errors that can occur during file storage operations.
#[derive(Debug)]
pub enum FileStorageError {
    /// Configuration is missing or invalid
    Config(String),
    /// Failed to upload file to storage
    Upload(String),
    /// Failed to generate download URL
    DownloadUrl(String),
    /// File not found in storage
    NotFound(String),
    /// Network or connectivity error
    Network(String),
    /// Local file system error
    LocalFile(String),
    /// Authentication or authorization error
    Auth(String),
}

impl std::error::Error for FileStorageError {}

impl fmt::Display for FileStorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileStorageError::Config(msg) => write!(f, "Configuration error: {}", msg),
            FileStorageError::Upload(msg) => write!(f, "Upload failed: {}", msg),
            FileStorageError::DownloadUrl(msg) => write!(f, "Download URL generation failed: {}", msg),
            FileStorageError::NotFound(msg) => write!(f, "File not found: {}", msg),
            FileStorageError::Network(msg) => write!(f, "Network error: {}", msg),
            FileStorageError::LocalFile(msg) => write!(f, "Local file error: {}", msg),
            FileStorageError::Auth(msg) => write!(f, "Authentication error: {}", msg),
        }
    }
}

impl From<std::io::Error> for FileStorageError {
    fn from(err: std::io::Error) -> Self {
        FileStorageError::LocalFile(err.to_string())
    }
}

/// Trait for file storage service implementations.
///
/// Implementations of this trait provide cloud storage functionality for
/// uploading, downloading, and managing audio files.
///
/// # Example
///
/// ```ignore
/// use voicecore::file_storage::{FileStorageService, UploadResult};
///
/// async fn upload_audio(storage: &dyn FileStorageService) -> Result<UploadResult, FileStorageError> {
///     storage.upload("/path/to/audio.mp3", "audio/019abc123.mp3").await
/// }
/// ```
pub trait FileStorageService: Send + Sync {
    /// Upload a file to cloud storage.
    ///
    /// # Arguments
    /// * `local_path` - Path to the local file to upload
    /// * `remote_key` - Object key/path in cloud storage (e.g., "audio/019abc123.mp3")
    ///
    /// # Returns
    /// * `Ok(UploadResult)` - Contains the storage key, provider name, and file size
    /// * `Err(FileStorageError)` - If the upload fails
    fn upload(
        &self,
        local_path: &Path,
        remote_key: &str,
    ) -> impl std::future::Future<Output = Result<UploadResult, FileStorageError>> + Send;

    /// Get a pre-signed download URL for a file.
    ///
    /// The URL is typically valid for about 1 hour.
    ///
    /// # Arguments
    /// * `storage_key` - Object key/path in cloud storage
    ///
    /// # Returns
    /// * `Ok(DownloadUrl)` - Contains the URL and expiration timestamp
    /// * `Err(FileStorageError)` - If URL generation fails
    fn get_download_url(
        &self,
        storage_key: &str,
    ) -> impl std::future::Future<Output = Result<DownloadUrl, FileStorageError>> + Send;

    /// Delete a file from cloud storage.
    ///
    /// # Arguments
    /// * `storage_key` - Object key/path in cloud storage
    ///
    /// # Returns
    /// * `Ok(())` - If deletion succeeds (or file didn't exist)
    /// * `Err(FileStorageError)` - If deletion fails
    fn delete(
        &self,
        storage_key: &str,
    ) -> impl std::future::Future<Output = Result<(), FileStorageError>> + Send;

    /// Check if a file exists in cloud storage.
    ///
    /// # Arguments
    /// * `storage_key` - Object key/path in cloud storage
    ///
    /// # Returns
    /// * `Ok(true)` - If the file exists
    /// * `Ok(false)` - If the file doesn't exist
    /// * `Err(FileStorageError)` - If the check fails
    fn exists(
        &self,
        storage_key: &str,
    ) -> impl std::future::Future<Output = Result<bool, FileStorageError>> + Send;

    /// Get the provider name for this storage service.
    ///
    /// This is used to populate the `storage_provider` field in the database.
    fn provider_name(&self) -> &'static str;
}

/// Generate the storage key for an audio file.
///
/// Creates a consistent key format: `{prefix}/{audio_file_id}.{extension}`
///
/// # Arguments
/// * `prefix` - Optional path prefix (e.g., "audio/")
/// * `audio_file_id` - UUID of the audio file (hex string)
/// * `filename` - Original filename to extract extension from
///
/// # Returns
/// The storage key, e.g., "audio/019abc123.mp3"
pub fn generate_storage_key(prefix: Option<&str>, audio_file_id: &str, filename: &str) -> String {
    let extension = filename
        .rsplit('.')
        .next()
        .unwrap_or("bin");

    match prefix {
        Some(p) => {
            let p = p.trim_end_matches('/');
            if p.is_empty() {
                format!("{}.{}", audio_file_id, extension)
            } else {
                format!("{}/{}.{}", p, audio_file_id, extension)
            }
        }
        None => format!("{}.{}", audio_file_id, extension),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_storage_key_with_prefix() {
        let key = generate_storage_key(Some("audio"), "019abc123def", "recording.mp3");
        assert_eq!(key, "audio/019abc123def.mp3");
    }

    #[test]
    fn test_generate_storage_key_with_trailing_slash() {
        let key = generate_storage_key(Some("audio/"), "019abc123def", "recording.mp3");
        assert_eq!(key, "audio/019abc123def.mp3");
    }

    #[test]
    fn test_generate_storage_key_no_prefix() {
        let key = generate_storage_key(None, "019abc123def", "recording.wav");
        assert_eq!(key, "019abc123def.wav");
    }

    #[test]
    fn test_generate_storage_key_empty_prefix() {
        let key = generate_storage_key(Some(""), "019abc123def", "test.flac");
        assert_eq!(key, "019abc123def.flac");
    }

    #[test]
    fn test_generate_storage_key_no_extension() {
        let key = generate_storage_key(Some("files"), "019abc123def", "noextension");
        assert_eq!(key, "files/019abc123def.noextension");
    }

    #[test]
    fn test_generate_storage_key_multiple_dots() {
        let key = generate_storage_key(Some("audio"), "019abc123def", "my.recording.mp3");
        assert_eq!(key, "audio/019abc123def.mp3");
    }

    #[test]
    fn test_file_storage_error_display() {
        let err = FileStorageError::Config("missing bucket".to_string());
        assert_eq!(format!("{}", err), "Configuration error: missing bucket");

        let err = FileStorageError::NotFound("file123".to_string());
        assert_eq!(format!("{}", err), "File not found: file123");
    }

    #[test]
    fn test_upload_result_serialization() {
        let result = UploadResult {
            storage_key: "audio/test.mp3".to_string(),
            provider: "s3".to_string(),
            size_bytes: 1024,
        };

        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("audio/test.mp3"));
        assert!(json.contains("s3"));
        assert!(json.contains("1024"));

        let parsed: UploadResult = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.storage_key, result.storage_key);
        assert_eq!(parsed.provider, result.provider);
        assert_eq!(parsed.size_bytes, result.size_bytes);
    }

    #[test]
    fn test_download_url_serialization() {
        let url = DownloadUrl {
            url: "https://s3.amazonaws.com/bucket/file?signature=abc".to_string(),
            expires_at: 1704067200,
        };

        let json = serde_json::to_string(&url).unwrap();
        let parsed: DownloadUrl = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.url, url.url);
        assert_eq!(parsed.expires_at, url.expires_at);
    }
}
