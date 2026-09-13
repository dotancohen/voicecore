//! Abstract file storage service for cloud storage.
//!
//! This module provides a trait-based abstraction for storing audio files
//! in cloud storage providers (AWS S3 and S3-compatible services today,
//! other providers later).
//!
//! The design:
//! - Audio file *metadata* syncs through the sync server like every other entity.
//! - Audio file *binaries* are uploaded to cloud storage by the device that
//!   holds them, and recorded in `audio_files.storage_provider/storage_key`.
//! - Other devices download a binary only when the user asks for it
//!   ([`download_audio_file`] / [`download_audio_files_for_note`]), unless the
//!   installation opted in to mirroring everything ([`download_missing_audio_files`]).
//!
//! Robustness rules (these systems are often offline for hours):
//! - "Cloud storage not configured" is never an error for the automatic paths;
//!   they simply do nothing until the configuration arrives via sync.
//! - A file that is not present locally is not this device's job to upload.
//! - Downloads are written to a temporary file, verified against the object
//!   size, and renamed into place, so a crash or lost connection never leaves
//!   a truncated file that later looks "present".
//! - After the first network failure in a batch the batch stops; the remaining
//!   files are simply retried on the next sync instead of timing out one by one.

use std::fmt;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::database::{AudioFileRow, Database};
use crate::models::{audio_file_extension, audio_local_path};

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
    /// Failed to download file from storage
    Download(String),
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

impl FileStorageError {
    /// True when the error is about this machine (config or local disk) rather
    /// than the remote service. Batch operations keep going after local errors
    /// but stop after the first remote one.
    pub fn is_local(&self) -> bool {
        matches!(self, FileStorageError::Config(_) | FileStorageError::LocalFile(_))
    }
}

impl std::error::Error for FileStorageError {}

impl fmt::Display for FileStorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileStorageError::Config(msg) => write!(f, "Configuration error: {}", msg),
            FileStorageError::Upload(msg) => write!(f, "Upload failed: {}", msg),
            FileStorageError::Download(msg) => write!(f, "Download failed: {}", msg),
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
pub trait FileStorageService: Send + Sync {
    /// Upload a file to cloud storage.
    ///
    /// * `local_path` - Path to the local file to upload
    /// * `remote_key` - Object key/path in cloud storage, without provider prefix
    ///
    /// Returns the *full* storage key (prefix applied) that must be stored in
    /// the database and passed back to the other methods.
    fn upload(
        &self,
        local_path: &Path,
        remote_key: &str,
    ) -> impl std::future::Future<Output = Result<UploadResult, FileStorageError>> + Send;

    /// Download a file from cloud storage to `local_path`.
    ///
    /// Implementations MUST write atomically: stream into a temporary file next
    /// to `local_path`, verify the size against the object metadata, then
    /// rename. `local_path` must not exist afterwards unless the download is
    /// complete and verified. Returns the number of bytes written.
    fn download(
        &self,
        storage_key: &str,
        local_path: &Path,
    ) -> impl std::future::Future<Output = Result<u64, FileStorageError>> + Send;

    /// Get a pre-signed download URL for a file (valid for about an hour).
    fn get_download_url(
        &self,
        storage_key: &str,
    ) -> impl std::future::Future<Output = Result<DownloadUrl, FileStorageError>> + Send;

    /// Delete a file from cloud storage. Succeeds if the file did not exist.
    fn delete(
        &self,
        storage_key: &str,
    ) -> impl std::future::Future<Output = Result<(), FileStorageError>> + Send;

    /// Check if a file exists in cloud storage.
    fn exists(
        &self,
        storage_key: &str,
    ) -> impl std::future::Future<Output = Result<bool, FileStorageError>> + Send;

    /// Provider name stored in `audio_files.storage_provider`.
    fn provider_name(&self) -> &'static str;

    /// The full storage key (prefix applied) an upload of `remote_key` would use.
    fn full_storage_key(&self, remote_key: &str) -> String;

    /// Tag an object purged (Stage 14): the bucket's lifecycle rule deletes
    /// it a day later. The key cannot delete.
    fn tag_purged(
        &self,
        storage_key: &str,
    ) -> impl std::future::Future<Output = Result<(), FileStorageError>> + Send;
}

/// Generate the storage key for an audio file.
///
/// Creates a consistent key format: `{prefix}/{audio_file_id}.{extension}`
/// where the extension is normalised with [`audio_file_extension`].
pub fn generate_storage_key(prefix: Option<&str>, audio_file_id: &str, filename: &str) -> String {
    let extension = audio_file_extension(filename);

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

/// Build the storage service described by the `file_storage_config` table.
///
/// Returns `Ok(None)` when no provider is configured (provider "none" or no
/// row at all). Returns `Err` when a provider is configured but unusable.
#[cfg(feature = "file-storage")]
pub fn create_storage_service(
    db: &Database,
) -> Result<Option<crate::file_storage_s3::S3StorageService>, FileStorageError> {
    use crate::file_storage_s3::{S3Config, S3StorageService};

    let storage_config = db
        .get_file_storage_config_struct()
        .map_err(|e| FileStorageError::Config(format!("Failed to get storage config: {}", e)))?;

    if !storage_config.is_enabled() {
        return Ok(None);
    }

    if storage_config.provider != "s3" {
        return Err(FileStorageError::Config(format!(
            "Unsupported storage provider: {}. Only 's3' is currently supported.",
            storage_config.provider
        )));
    }

    let bucket = storage_config
        .s3_bucket()
        .ok_or_else(|| FileStorageError::Config("S3 bucket not configured".to_string()))?;
    let region = storage_config
        .s3_region()
        .ok_or_else(|| FileStorageError::Config("S3 region not configured".to_string()))?;
    let access_key_id = storage_config
        .s3_access_key_id()
        .ok_or_else(|| FileStorageError::Config("S3 access_key_id not configured".to_string()))?;
    let secret_access_key = storage_config.s3_secret_access_key().ok_or_else(|| {
        FileStorageError::Config("S3 secret_access_key not configured".to_string())
    })?;

    let s3_config = S3Config {
        bucket: bucket.to_string(),
        region: region.to_string(),
        access_key_id: access_key_id.to_string(),
        secret_access_key: secret_access_key.to_string(),
        prefix: storage_config.s3_prefix().map(String::from),
        endpoint: storage_config.s3_endpoint().map(String::from),
    };

    Ok(Some(S3StorageService::new(s3_config)?))
}

/// Result of uploading pending audio files to cloud storage.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UploadPendingResult {
    /// Number of files successfully uploaded
    pub uploaded: usize,
    /// Number of pending records skipped because the file is not on this device
    pub skipped: usize,
    /// Number of files that failed to upload
    pub failed: usize,
    /// Number of files not attempted because an earlier remote failure stopped the batch
    pub deferred: usize,
    /// Error messages for failed uploads
    pub errors: Vec<String>,
}

/// Upload all pending audio files to cloud storage.
///
/// "Pending" means `storage_provider IS NULL` and not deleted. Records whose
/// file is not present on this device are skipped silently: they were created
/// on another device, which is responsible for uploading them.
///
/// Returns `Err(Config)` when cloud storage is not configured; callers that
/// run automatically should check [`create_storage_service`] first.
#[cfg(feature = "file-storage")]
pub async fn upload_pending_audio_files(
    db: &Database,
    audiofile_directory: &Path,
) -> Result<UploadPendingResult, FileStorageError> {
    let storage = create_storage_service(db)?.ok_or_else(|| {
        FileStorageError::Config(
            "Cloud storage is not enabled. Use 'storage configure-s3' first.".to_string(),
        )
    })?;

    let pending_files = db
        .get_audio_files_pending_upload()
        .map_err(|e| FileStorageError::Config(format!("Failed to get pending files: {}", e)))?;

    tracing::debug!(count = pending_files.len(), "Audio file records pending upload");

    let mut result = UploadPendingResult::default();
    let total = pending_files.len();

    // The objects of purged recordings are tagged first (Stage 14): a purge
    // reaches the bucket at the next upload run, and the tag costs one request
    match db.purged_objects() {
        Ok(keys) => {
            for key in keys {
                match storage.tag_purged(&key).await {
                    Ok(()) | Err(FileStorageError::NotFound(_)) => {
                        if let Err(e) = db.forget_purged_object(&key) {
                            tracing::warn!("Could not forget the purged object {}: {}", key, e);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("The purged object {} was not tagged: {}", key, e);
                        result.errors.push(format!("Purged recording {} not yet tagged in the bucket: {}", key, e));
                    }
                }
            }
        }
        Err(e) => tracing::warn!("Could not list purged objects: {}", e),
    }

    for (index, audio_file) in pending_files.into_iter().enumerate() {
        let local_path = audio_local_path(audiofile_directory, &audio_file.local_name);

        if !local_path.is_file() {
            tracing::debug!(
                audio_id = %audio_file.id,
                path = %local_path.display(),
                "Pending audio file is not on this device; another device will upload it"
            );
            result.skipped += 1;
            continue;
        }

        // Generate storage key WITHOUT prefix - the storage service adds the prefix.
        let storage_key = generate_storage_key(None, &audio_file.id, &audio_file.filename);

        // Already there? One request instead of a whole file (Stage 8): a
        // row can say "not uploaded" after a snapshot restore while the
        // object is in the bucket
        let full_key = storage.full_storage_key(&storage_key);
        if let Ok(true) = storage.exists(&full_key).await {
            match db.update_audio_file_storage(&audio_file.id, storage.provider_name(), &full_key) {
                Ok(_) => {
                    tracing::info!(audio_id = %audio_file.id, storage_key = %full_key, "The object was in the bucket already; the row now says so");
                    result.uploaded += 1;
                }
                Err(e) => {
                    result.errors.push(format!("{} is in the bucket but the row could not be updated: {}", audio_file.id, e));
                    result.failed += 1;
                }
            }
            continue;
        }

        tracing::info!(
            audio_id = %audio_file.id,
            filename = %audio_file.filename,
            storage_key = %storage_key,
            "Uploading audio file to cloud storage"
        );

        match storage.upload(&local_path, &storage_key).await {
            Ok(upload) => {
                match db.update_audio_file_storage(&audio_file.id, &upload.provider, &upload.storage_key) {
                    Ok(_) => {
                        tracing::info!(
                            audio_id = %audio_file.id,
                            storage_key = %upload.storage_key,
                            size_bytes = upload.size_bytes,
                            "Uploaded audio file"
                        );
                        result.uploaded += 1;
                    }
                    Err(e) => {
                        let msg = format!(
                            "Uploaded {} but failed to update database: {}",
                            audio_file.id, e
                        );
                        tracing::error!("{}", msg);
                        result.errors.push(msg);
                        result.failed += 1;
                    }
                }
            }
            Err(e) => {
                let msg = format!("Failed to upload {}: {}", audio_file.id, e);
                tracing::error!("{}", msg);
                result.errors.push(msg);
                result.failed += 1;

                if !e.is_local() {
                    // Probably offline or the service is down: do not burn a
                    // timeout per remaining file. They stay pending and are
                    // retried on the next sync.
                    result.deferred = total - index - 1;
                    if result.deferred > 0 {
                        result.errors.push(format!(
                            "Stopped after a cloud storage failure; {} file(s) will be retried on the next sync",
                            result.deferred
                        ));
                    }
                    break;
                }
            }
        }
    }

    Ok(result)
}

/// Outcome of a single on-demand download.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DownloadOutcome {
    /// The file was already present locally; nothing was done.
    AlreadyLocal,
    /// The file was downloaded and verified; contains the size in bytes.
    Downloaded(u64),
    /// The record has no cloud storage location yet (the owning device has
    /// not uploaded it), so there is nothing to download.
    NotInCloud,
}

/// Download one audio file from cloud storage if it is not already local.
///
/// This is the on-demand path used by "media missing, download" actions.
/// Unlike the batch helpers it reports a missing configuration as an error,
/// because the user explicitly asked for the file.
#[cfg(feature = "file-storage")]
pub async fn download_audio_file(
    db: &Database,
    audiofile_directory: &Path,
    audio_file_id: &str,
) -> Result<DownloadOutcome, FileStorageError> {
    let audio_file = db
        .get_audio_file(audio_file_id)
        .map_err(|e| FileStorageError::Config(format!("Failed to read audio file record: {}", e)))?
        .ok_or_else(|| FileStorageError::NotFound(format!("Audio file record {} not found", audio_file_id)))?;

    let local_path = audio_local_path(audiofile_directory, &audio_file.local_name);
    if local_path.is_file() {
        return Ok(DownloadOutcome::AlreadyLocal);
    }

    let storage_key = match &audio_file.storage_key {
        Some(key) if audio_file.storage_provider.is_some() => key.clone(),
        _ => return Ok(DownloadOutcome::NotInCloud),
    };

    let storage = create_storage_service(db)?.ok_or_else(|| {
        FileStorageError::Config(
            "Cloud storage is not configured on this device yet. Sync with the server to receive the configuration.".to_string(),
        )
    })?;

    tracing::info!(
        audio_id = %audio_file.id,
        storage_key = %storage_key,
        "Downloading audio file from cloud storage"
    );

    let bytes = storage.download(&storage_key, &local_path).await?;

    tracing::info!(audio_id = %audio_file.id, size_bytes = bytes, "Downloaded audio file");
    Ok(DownloadOutcome::Downloaded(bytes))
}

/// Result of downloading a set of audio files from cloud storage.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DownloadMissingResult {
    /// Number of files downloaded and verified
    pub downloaded: usize,
    /// Number of records that were already present locally
    pub already_local: usize,
    /// Number of records that have no cloud location yet
    pub not_in_cloud: usize,
    /// Number of downloads that failed
    pub failed: usize,
    /// Number of files not attempted because an earlier remote failure stopped the batch
    pub deferred: usize,
    /// Error messages for failed downloads
    pub errors: Vec<String>,
}

#[cfg(feature = "file-storage")]
async fn download_audio_file_set<S: FileStorageService>(
    storage: &S,
    audiofile_directory: &Path,
    audio_files: Vec<AudioFileRow>,
) -> DownloadMissingResult {
    let mut result = DownloadMissingResult::default();
    let total = audio_files.len();

    for (index, audio_file) in audio_files.into_iter().enumerate() {
        let local_path = audio_local_path(audiofile_directory, &audio_file.local_name);
        if local_path.is_file() {
            result.already_local += 1;
            continue;
        }

        let storage_key = match (&audio_file.storage_provider, &audio_file.storage_key) {
            (Some(_), Some(key)) => key.clone(),
            _ => {
                result.not_in_cloud += 1;
                continue;
            }
        };

        tracing::info!(
            audio_id = %audio_file.id,
            storage_key = %storage_key,
            "Downloading audio file from cloud storage"
        );

        match storage.download(&storage_key, &local_path).await {
            Ok(bytes) => {
                tracing::info!(audio_id = %audio_file.id, size_bytes = bytes, "Downloaded audio file");
                result.downloaded += 1;
            }
            Err(e) => {
                let msg = format!("Failed to download {}: {}", audio_file.id, e);
                tracing::error!("{}", msg);
                result.errors.push(msg);
                result.failed += 1;

                if !e.is_local() && !matches!(e, FileStorageError::NotFound(_)) {
                    result.deferred = total - index - 1;
                    if result.deferred > 0 {
                        result.errors.push(format!(
                            "Stopped after a cloud storage failure; {} file(s) not attempted",
                            result.deferred
                        ));
                    }
                    break;
                }
            }
        }
    }

    result
}

/// Download every audio file attached to a note that is in cloud storage but
/// not on this device. Returns `Err(Config)` when storage is not configured.
#[cfg(feature = "file-storage")]
pub async fn download_audio_files_for_note(
    db: &Database,
    audiofile_directory: &Path,
    note_id: &str,
) -> Result<DownloadMissingResult, FileStorageError> {
    let audio_files = db
        .get_audio_files_for_note(note_id)
        .map_err(|e| FileStorageError::Config(format!("Failed to read audio files for note: {}", e)))?;

    let needs_cloud = audio_files.iter().any(|af| {
        !audio_local_path(audiofile_directory, &af.local_name).is_file()
            && af.storage_key.is_some()
    });

    if !needs_cloud {
        // Nothing to fetch: report counts without touching the network.
        let mut result = DownloadMissingResult::default();
        for af in &audio_files {
            if audio_local_path(audiofile_directory, &af.local_name).is_file() {
                result.already_local += 1;
            } else {
                result.not_in_cloud += 1;
            }
        }
        return Ok(result);
    }

    let storage = create_storage_service(db)?.ok_or_else(|| {
        FileStorageError::Config(
            "Cloud storage is not configured on this device yet. Sync with the server to receive the configuration.".to_string(),
        )
    })?;

    Ok(download_audio_file_set(&storage, audiofile_directory, audio_files).await)
}

/// Download every non-deleted audio file that is in cloud storage but not on
/// this device. Used by installations that mirror the whole bucket.
///
/// Returns `Ok(default)` when storage is not configured: mirroring is an
/// automatic background task and must never fail a sync for lack of config.
#[cfg(feature = "file-storage")]
pub async fn download_missing_audio_files(
    db: &Database,
    audiofile_directory: &Path,
) -> Result<DownloadMissingResult, FileStorageError> {
    let storage = match create_storage_service(db)? {
        Some(s) => s,
        None => {
            tracing::debug!("Cloud storage not configured; nothing to mirror");
            return Ok(DownloadMissingResult::default());
        }
    };

    let audio_files: Vec<AudioFileRow> = db
        .get_all_audio_files()
        .map_err(|e| FileStorageError::Config(format!("Failed to get audio files: {}", e)))?
        .into_iter()
        .filter(|af| af.deleted_at.is_none())
        .collect();

    Ok(download_audio_file_set(&storage, audiofile_directory, audio_files).await)
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
        assert_eq!(key, "files/019abc123def.bin");
    }

    #[test]
    fn test_generate_storage_key_uppercase_extension_is_lowercased() {
        let key = generate_storage_key(Some("audio"), "019abc123def", "REC.MP3");
        assert_eq!(key, "audio/019abc123def.mp3");
    }

    #[test]
    fn test_generate_storage_key_multiple_dots() {
        let key = generate_storage_key(Some("audio"), "019abc123def", "my.recording.mp3");
        assert_eq!(key, "audio/019abc123def.mp3");
    }

    #[test]
    fn test_generate_storage_key_hebrew_filename() {
        let key = generate_storage_key(Some("audio"), "019abc123def", "הקלטה של פגישה.OGG");
        assert_eq!(key, "audio/019abc123def.ogg");
    }

    #[test]
    fn test_file_storage_error_display() {
        let err = FileStorageError::Config("missing bucket".to_string());
        assert_eq!(format!("{}", err), "Configuration error: missing bucket");

        let err = FileStorageError::NotFound("file123".to_string());
        assert_eq!(format!("{}", err), "File not found: file123");
    }

    #[test]
    fn test_file_storage_error_is_local() {
        assert!(FileStorageError::Config("x".into()).is_local());
        assert!(FileStorageError::LocalFile("x".into()).is_local());
        assert!(!FileStorageError::Network("x".into()).is_local());
        assert!(!FileStorageError::Upload("x".into()).is_local());
        assert!(!FileStorageError::Auth("x".into()).is_local());
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

    // ------------------------------------------------------------------
    // Batch behaviour tests with a fake storage service (no network).
    // ------------------------------------------------------------------

    #[cfg(feature = "file-storage")]
    mod batch {
        use super::super::*;
        use std::sync::Mutex;
        use tempfile::TempDir;

        /// Fake storage: `objects` maps storage_key -> content. `fail_after`
        /// makes every download after N successful ones fail with a Network error.
        struct FakeStorage {
            objects: std::collections::HashMap<String, Vec<u8>>,
            downloads: Mutex<usize>,
            fail_after: Option<usize>,
        }

        impl FileStorageService for FakeStorage {
            async fn upload(&self, _local_path: &Path, remote_key: &str) -> Result<UploadResult, FileStorageError> {
                Ok(UploadResult { storage_key: remote_key.to_string(), provider: "fake".into(), size_bytes: 0 })
            }
            async fn download(&self, storage_key: &str, local_path: &Path) -> Result<u64, FileStorageError> {
                let mut n = self.downloads.lock().unwrap();
                if let Some(limit) = self.fail_after {
                    if *n >= limit {
                        return Err(FileStorageError::Network("offline".into()));
                    }
                }
                let data = self.objects.get(storage_key)
                    .ok_or_else(|| FileStorageError::NotFound(storage_key.to_string()))?;
                std::fs::write(local_path, data)?;
                *n += 1;
                Ok(data.len() as u64)
            }
            async fn get_download_url(&self, _k: &str) -> Result<DownloadUrl, FileStorageError> {
                Err(FileStorageError::DownloadUrl("unsupported".into()))
            }
            async fn delete(&self, _k: &str) -> Result<(), FileStorageError> { Ok(()) }
            async fn exists(&self, k: &str) -> Result<bool, FileStorageError> { Ok(self.objects.contains_key(k)) }
            fn provider_name(&self) -> &'static str { "fake" }
            fn full_storage_key(&self, remote_key: &str) -> String { remote_key.to_string() }
            async fn tag_purged(&self, _k: &str) -> Result<(), FileStorageError> { Ok(()) }
        }

        fn setup() -> (Database, TempDir) {
            let temp = TempDir::new().unwrap();
            let db = Database::new(&temp.path().join("test.db")).unwrap();
            (db, temp)
        }

        fn row(db: &Database, filename: &str, in_cloud: bool) -> AudioFileRow {
            let id = db.create_audio_file(filename, None).unwrap();
            if in_cloud {
                let key = generate_storage_key(Some("audio"), &id, filename);
                db.update_audio_file_storage(&id, "fake", &key).unwrap();
            }
            db.get_audio_file(&id).unwrap().unwrap()
        }

        #[tokio::test]
        async fn download_set_skips_local_and_not_in_cloud_and_downloads_the_rest() {
            let (db, temp) = setup();
            let dir = temp.path().join("audio");
            std::fs::create_dir_all(&dir).unwrap();

            let local = row(&db, "מקומי.MP3", true);
            std::fs::write(audio_local_path(&dir, &local.local_name), b"x").unwrap();
            let not_uploaded = row(&db, "not-yet.ogg", false);
            let remote = row(&db, "בענן.WAV", true);

            let mut objects = std::collections::HashMap::new();
            objects.insert(remote.storage_key.clone().unwrap(), b"wav-bytes".to_vec());
            let storage = FakeStorage { objects, downloads: Mutex::new(0), fail_after: None };

            let result = download_audio_file_set(&storage, &dir, vec![local.clone(), not_uploaded.clone(), remote.clone()]).await;
            assert_eq!(result.already_local, 1);
            assert_eq!(result.not_in_cloud, 1);
            assert_eq!(result.downloaded, 1);
            assert_eq!(result.failed, 0);
            assert!(result.errors.is_empty());
            // Downloaded to the path the row names (Stage 13)
            assert!(remote.local_name.ends_with(".wav") && remote.local_name.contains('-'), "{}", remote.local_name);
            assert!(dir.join(&remote.local_name).is_file());
        }

        #[tokio::test]
        async fn download_set_stops_after_first_network_failure() {
            let (db, temp) = setup();
            let dir = temp.path().join("audio");
            std::fs::create_dir_all(&dir).unwrap();

            let rows: Vec<AudioFileRow> = (0..4).map(|i| row(&db, &format!("f{}.mp3", i), true)).collect();
            let mut objects = std::collections::HashMap::new();
            for r in &rows {
                objects.insert(r.storage_key.clone().unwrap(), vec![1u8; 10]);
            }
            let storage = FakeStorage { objects, downloads: Mutex::new(0), fail_after: Some(1) };

            let result = download_audio_file_set(&storage, &dir, rows).await;
            assert_eq!(result.downloaded, 1);
            assert_eq!(result.failed, 1);
            assert_eq!(result.deferred, 2, "remaining files must be deferred, not attempted");
            assert_eq!(result.errors.len(), 2);
        }

        #[tokio::test]
        async fn download_set_not_found_does_not_stop_the_batch() {
            let (db, temp) = setup();
            let dir = temp.path().join("audio");
            std::fs::create_dir_all(&dir).unwrap();

            let missing = row(&db, "gone.mp3", true);
            let present = row(&db, "here.mp3", true);
            let mut objects = std::collections::HashMap::new();
            objects.insert(present.storage_key.clone().unwrap(), vec![2u8; 5]);
            let storage = FakeStorage { objects, downloads: Mutex::new(0), fail_after: None };

            let result = download_audio_file_set(&storage, &dir, vec![missing, present]).await;
            assert_eq!(result.failed, 1);
            assert_eq!(result.downloaded, 1);
            assert_eq!(result.deferred, 0);
        }

        #[tokio::test]
        async fn download_missing_is_noop_without_storage_config() {
            let (db, temp) = setup();
            let dir = temp.path().join("audio");
            row(&db, "a.mp3", false);
            let result = download_missing_audio_files(&db, &dir).await.unwrap();
            assert_eq!(result.downloaded, 0);
            assert!(result.errors.is_empty());
        }

        #[tokio::test]
        async fn download_audio_file_reports_not_in_cloud_and_already_local() {
            let (db, temp) = setup();
            let dir = temp.path().join("audio");
            std::fs::create_dir_all(&dir).unwrap();

            let pending = row(&db, "pending.mp3", false);
            assert_eq!(
                download_audio_file(&db, &dir, &pending.id).await.unwrap(),
                DownloadOutcome::NotInCloud
            );

            let local = row(&db, "local.mp3", true);
            std::fs::write(audio_local_path(&dir, &local.local_name), b"x").unwrap();
            assert_eq!(
                download_audio_file(&db, &dir, &local.id).await.unwrap(),
                DownloadOutcome::AlreadyLocal
            );

            // In cloud, not local, no config on this device: a clear Config error
            let remote = row(&db, "remote.mp3", true);
            match download_audio_file(&db, &dir, &remote.id).await {
                Err(FileStorageError::Config(_)) => {}
                other => panic!("expected Config error, got {:?}", other.map(|_| ())),
            }

            // Unknown id
            match download_audio_file(&db, &dir, "00000000000070008000000000000099").await {
                Err(FileStorageError::NotFound(_)) => {}
                other => panic!("expected NotFound, got {:?}", other.map(|_| ())),
            }
        }

        #[tokio::test]
        async fn download_for_note_without_cloud_files_does_not_need_config() {
            let (db, temp) = setup();
            let dir = temp.path().join("audio");
            std::fs::create_dir_all(&dir).unwrap();

            let note_id = db.create_note("פתק עם הקלטה").unwrap();
            let pending = row(&db, "pending.mp3", false);
            db.attach_to_note(&note_id, &pending.id, "audio_file").unwrap();

            let result = download_audio_files_for_note(&db, &dir, &note_id).await.unwrap();
            assert_eq!(result.not_in_cloud, 1);
            assert_eq!(result.downloaded, 0);
        }

        #[tokio::test]
        async fn upload_pending_skips_files_missing_locally() {
            let (db, temp) = setup();
            let dir = temp.path().join("audio");
            std::fs::create_dir_all(&dir).unwrap();
            // A record synced from another device: pending, but no local file.
            row(&db, "elsewhere.mp3", false);

            // Storage not configured -> Config error for the explicit call
            match upload_pending_audio_files(&db, &dir).await {
                Err(FileStorageError::Config(_)) => {}
                other => panic!("expected Config error, got {:?}", other.map(|_| ())),
            }

            // Configure a (fake) S3 provider; the record has no local file so it
            // must be skipped before any network access is attempted.
            let cfg = serde_json::json!({
                "bucket": "b", "region": "us-east-1",
                "access_key_id": "k", "secret_access_key": "s"
            });
            db.set_file_storage_config("s3", Some(&cfg)).unwrap();
            let result = upload_pending_audio_files(&db, &dir).await.unwrap();
            assert_eq!(result.skipped, 1);
            assert_eq!(result.uploaded, 0);
            assert_eq!(result.failed, 0);
            assert!(result.errors.is_empty());
        }
    }
}
