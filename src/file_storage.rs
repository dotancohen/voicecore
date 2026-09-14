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
//! - Each file is tried three times, the third a minute after the second; after
//!   three files failed every try the batch stops, and the files not attempted
//!   wait for the next upload or download (FILE-14).

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
    /// it a day later, so a key without `s3:DeleteObject` purges as well.
    fn tag_purged(
        &self,
        storage_key: &str,
    ) -> impl std::future::Future<Output = Result<(), FileStorageError>> + Send;

    /// Whether an object carries the purge tag (Stage 14): it is waiting for
    /// the lifecycle rule to delete it, and holds no recording.
    fn purge_tagged(
        &self,
        storage_key: &str,
    ) -> impl std::future::Future<Output = Result<bool, FileStorageError>> + Send;

    /// Upload a large file in parts (Stage 13, FILE-19): the parts the
    /// journal lists as uploaded are not sent again, each part sent is
    /// written to the journal before the next, and the object exists only
    /// once every part is there. A cancel or a failure leaves the journal
    /// for the next run to continue from.
    fn upload_in_parts(
        &self,
        source: &mut dyn crate::crypto::ByteSource,
        remote_key: &str,
        journal: &dyn PartJournal,
    ) -> impl std::future::Future<Output = Result<UploadResult, FileStorageError>>;
}

/// Bytes per part of an upload in parts (Stage 13). Amazon requires at
/// least 5 MiB for every part but the last; a file smaller than one part
/// is uploaded whole.
pub const PART_SIZE: u64 = 8 * 1024 * 1024;

/// The three requests of an upload in parts, as a bucket answers them:
/// the seam between the loop, which is tested with a store in memory, and
/// the signed requests of the S3 service.
pub trait PartStore {
    /// Begin an upload of `key`; returns the bucket's upload id.
    fn begin(&self, key: &str) -> impl std::future::Future<Output = Result<String, FileStorageError>>;
    /// Send one part; returns the tag the bucket gave it.
    fn put_part(&self, key: &str, upload_id: &str, part_number: u32, bytes: Vec<u8>) -> impl std::future::Future<Output = Result<String, FileStorageError>>;
    /// Make the object from the parts, in order.
    fn complete(&self, key: &str, upload_id: &str, parts: &[(u32, String)]) -> impl std::future::Future<Output = Result<(), FileStorageError>>;
}

/// Where an upload in parts remembers how far it is (Stage 13): the
/// database, through `upload_begun` and its siblings; and what it asks
/// between parts.
pub trait PartJournal {
    /// The upload begun earlier, if one is under way.
    fn begun(&self) -> Result<Option<crate::database::UploadBegun>, FileStorageError>;
    /// A new upload has begun.
    fn begin(&self, storage_key: &str, upload_id: &str, part_size: u64) -> Result<(), FileStorageError>;
    /// One more part is in the bucket.
    fn part_done(&self, part_number: u32, etag: &str) -> Result<(), FileStorageError>;
    /// The object is complete; the parts are forgotten.
    fn finished(&self) -> Result<(), FileStorageError>;
    /// Whether the person asked to stop; asked before every part.
    fn cancelled(&self) -> bool {
        false
    }
    /// How many bytes of the file are in the bucket, after each part.
    fn moved(&self, _done: u64, _total: u64) {}
}

/// The loop of an upload in parts (Stage 13): continue the upload the
/// journal remembers when its key and part size still fit, otherwise begin
/// one; skip the parts already there; send the rest in order; complete.
pub async fn upload_in_parts<S: PartStore>(store: &S, source: &mut dyn crate::crypto::ByteSource, key: &str, journal: &dyn PartJournal, part_size: u64) -> Result<u64, FileStorageError> {
    let total = source.len();
    let part_count = u32::try_from(total.div_ceil(part_size).max(1)).map_err(|_| FileStorageError::Upload("too many parts".to_string()))?;

    let (upload_id, mut parts) = match journal.begun()? {
        Some(begun) if begun.storage_key == key && begun.part_size == part_size => {
            tracing::info!(key = %key, parts_done = begun.parts.len(), "Continuing an upload in parts");
            (begun.upload_id, begun.parts)
        }
        _ => {
            let upload_id = store.begin(key).await?;
            journal.begin(key, &upload_id, part_size)?;
            (upload_id, Vec::new())
        }
    };

    for part_number in 1..=part_count {
        if parts.iter().any(|(n, _)| *n == part_number) {
            continue;
        }
        if journal.cancelled() {
            return Err(FileStorageError::Upload(crate::sync_client::CANCELLED.to_string()));
        }
        let offset = u64::from(part_number - 1) * part_size;
        let length = (total - offset).min(part_size) as usize;
        let mut bytes = vec![0u8; length];
        source.read_at(offset, &mut bytes)
            .map_err(|e| FileStorageError::LocalFile(format!("Failed to read part {} of {}: {}", part_number, key, e)))?;
        let etag = store.put_part(key, &upload_id, part_number, bytes).await?;
        journal.part_done(part_number, &etag)?;
        parts.push((part_number, etag));
        journal.moved((offset + length as u64).min(total), total);
    }
    parts.sort_by_key(|(n, _)| *n);
    store.complete(key, &upload_id, &parts).await?;
    journal.finished()?;
    Ok(total)
}

/// The database as a journal for one recording's upload (Stage 13).
pub struct DatabaseJournal<'a> {
    pub db: &'a Database,
    pub audio_id: &'a str,
    pub cancel: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    pub sink: Option<std::sync::Arc<dyn crate::sync_client::ProgressSink>>,
}

impl PartJournal for DatabaseJournal<'_> {
    fn begun(&self) -> Result<Option<crate::database::UploadBegun>, FileStorageError> {
        self.db.upload_begun(self.audio_id).map_err(|e| FileStorageError::Config(e.to_string()))
    }
    fn begin(&self, storage_key: &str, upload_id: &str, part_size: u64) -> Result<(), FileStorageError> {
        self.db.upload_begin(self.audio_id, storage_key, upload_id, part_size).map_err(|e| FileStorageError::Config(e.to_string()))
    }
    fn part_done(&self, part_number: u32, etag: &str) -> Result<(), FileStorageError> {
        self.db.upload_part_done(self.audio_id, part_number, etag).map_err(|e| FileStorageError::Config(e.to_string()))
    }
    fn finished(&self) -> Result<(), FileStorageError> {
        self.db.upload_finished(self.audio_id).map_err(|e| FileStorageError::Config(e.to_string()))
    }
    fn cancelled(&self) -> bool {
        self.cancel.as_ref().is_some_and(|c| c.load(std::sync::atomic::Ordering::Relaxed))
    }
    fn moved(&self, done: u64, total: u64) {
        if let Some(sink) = &self.sink {
            sink.report(crate::sync_client::Progress {
                stage: "upload".to_string(),
                done: 0,
                total: 0,
                bytes: done,
                sentence: format!("Uploading {}: {} of {} MiB", self.audio_id, done / (1024 * 1024), total.div_ceil(1024 * 1024)),
            });
        }
    }
}

/// The bucket key of a recording (Stage 13, FILE-18): by its content hash, so
/// two devices importing one file share one object. None when the hash is not
/// a SHA-256 in hex: a key is never made from the recording's id.
pub fn storage_key_for(prefix: Option<&str>, filename: &str, content_sha256: &str) -> Option<String> {
    if content_sha256.len() != 64 || !content_sha256.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let extension = audio_file_extension(filename);
    Some(match prefix {
        Some(p) => {
            let p = p.trim_end_matches('/');
            if p.is_empty() {
                format!("{}.{}", content_sha256, extension)
            } else {
                format!("{}/{}.{}", p, content_sha256, extension)
            }
        }
        None => format!("{}.{}", content_sha256, extension),
    })
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
    /// Number of files not attempted because three files failed every try and the batch stopped
    pub deferred: usize,
    /// Number of files not uploaded because they are larger than the account's upload limit (FILE-23)
    pub too_large: usize,
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
///
/// A cancel flag is read between parts and files, and a sink hears how far a
/// large file is (Stage 13). With encryption on (ENC-3) the recording key is
/// needed and every upload is encrypted; without it the run refuses before
/// touching the bucket.
pub async fn upload_pending_audio_files(
    db: &Database,
    audiofile_directory: &Path,
    cancel: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    sink: Option<std::sync::Arc<dyn crate::sync_client::ProgressSink>>,
    recording_key: Option<&crate::crypto::RecordingKey>,
    // This device's id, from its configuration: hashing a file states that it holds it (FILE-22)
    here: &str,
) -> Result<UploadPendingResult, FileStorageError> {
    let storage = create_storage_service(db)?.ok_or_else(|| {
        FileStorageError::Config(
            "Cloud storage is not enabled. Use 'storage configure-s3' first.".to_string(),
        )
    })?;
    let pending_files = db
        .get_audio_files_pending_upload()
        .map_err(|e| FileStorageError::Config(format!("Failed to get pending files: {}", e)))?;
    upload_files_with(&storage, db, audiofile_directory, pending_files, cancel, sink, recording_key, here).await
}

/// The sentence when encryption is on and this device holds no key.
pub const NO_RECORDING_KEY: &str = "Encryption is on for this account, but this device holds no recording key: import it (account recording-key import) before uploading";

/// Upload the given rows to a storage service: the loop behind
/// `upload_pending_audio_files` and "Re-upload existing recordings
/// encrypted" (ENC-3), tested against a store in memory.
pub async fn upload_files_with<S: FileStorageService>(
    storage: &S,
    db: &Database,
    audiofile_directory: &Path,
    pending_files: Vec<AudioFileRow>,
    cancel: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    sink: Option<std::sync::Arc<dyn crate::sync_client::ProgressSink>>,
    recording_key: Option<&crate::crypto::RecordingKey>,
    // This device's id, from its configuration: hashing a file states that it holds it (FILE-22)
    here: &str,
) -> Result<UploadPendingResult, FileStorageError> {
    let encrypt = db.encryption_on().map_err(|e| FileStorageError::Config(e.to_string()))?;
    if encrypt && recording_key.is_none() {
        return Err(FileStorageError::Config(NO_RECORDING_KEY.to_string()));
    }
    let limit = db.max_upload_bytes().map_err(|e| FileStorageError::Config(e.to_string()))?;

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
        let local_path = audio_local_path(audiofile_directory, &audio_file.disk_name);

        if !local_path.is_file() {
            tracing::debug!(
                audio_id = %audio_file.id,
                path = %local_path.display(),
                "Pending audio file is not on this device; another device will upload it"
            );
            result.skipped += 1;
            continue;
        }

        // The account's upload limit (FILE-23): a larger file stays where it is
        let size = std::fs::metadata(&local_path).map(|m| m.len()).unwrap_or(0);
        if size > limit {
            tracing::info!(audio_id = %audio_file.id, size_bytes = size, limit_bytes = limit, "Larger than the account's upload limit; not uploaded");
            result.too_large += 1;
            continue;
        }

        // The hash first (Stage 13): the key is by it, and a fetch verifies by it
        let hash = match &audio_file.content_sha256 {
            Some(h) => Some(h.clone()),
            None => match db.store_content_hash(&audio_file.id, audiofile_directory, here) {
                Ok(h) => Some(h),
                Err(e) => {
                    tracing::warn!("Could not hash {}: {}", audio_file.id, e);
                    None
                }
            },
        };
        // Generate storage key WITHOUT prefix - the storage service adds the prefix.
        // An encrypted object carries the suffix (ENC-3). Without a hash there is
        // no key: the file is reported and tried again at the next upload.
        let Some(mut storage_key) = hash.as_deref().and_then(|h| storage_key_for(None, &audio_file.filename, h)) else {
            result.failed += 1;
            result.errors.push(format!("{}: not uploaded, its content hash could not be calculated", audio_file.filename));
            continue;
        };
        if encrypt {
            storage_key.push_str(crate::crypto::OBJECT_SUFFIX);
        }

        // Already there? One request instead of a whole file (Stage 8): a
        // row can say "not uploaded" after a snapshot restore while the
        // object is in the bucket
        let full_key = storage.full_storage_key(&storage_key);
        // An object with the purge tag is waiting to be deleted, and one whose
        // tag cannot be read is not counted on: both are sent again
        if let (Ok(true), Ok(false)) = (storage.exists(&full_key).await, storage.purge_tagged(&full_key).await) {
            match db.update_audio_file_storage(&audio_file.id, storage.provider_name(), &full_key, encrypt) {
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

        if cancel.as_ref().is_some_and(|c| c.load(std::sync::atomic::Ordering::Relaxed)) {
            result.errors.push(crate::sync_client::CANCELLED.to_string());
            result.deferred = total - index;
            break;
        }
        // A large file goes in parts (Stage 13), so a failure loses one part and
        // not the file; an encrypted one always does, read through the cipher (ENC-3).
        // Tried three times, the third a minute after the second (FILE-14); the
        // parts already in the bucket are not sent again
        let large = std::fs::metadata(&local_path).map(|m| m.len() > PART_SIZE).unwrap_or(false);
        let mut try_number = 1;
        let uploaded = loop {
            let attempt = if let (true, Some(key)) = (encrypt, recording_key) {
                let journal = DatabaseJournal { db, audio_id: &audio_file.id, cancel: cancel.clone(), sink: sink.clone() };
                match crate::crypto::EncryptedView::open(&local_path, key) {
                    Ok(mut view) => storage.upload_in_parts(&mut view, &storage_key, &journal).await,
                    Err(e) => Err(FileStorageError::LocalFile(format!("Failed to open {}: {}", local_path.display(), e))),
                }
            } else if large {
                let journal = DatabaseJournal { db, audio_id: &audio_file.id, cancel: cancel.clone(), sink: sink.clone() };
                match std::fs::File::open(&local_path) {
                    Ok(mut file) => storage.upload_in_parts(&mut file, &storage_key, &journal).await,
                    Err(e) => Err(FileStorageError::LocalFile(format!("Failed to open {}: {}", local_path.display(), e))),
                }
            } else {
                storage.upload(&local_path, &storage_key).await
            };
            match &attempt {
                Err(e) if !e.is_local() && !e.to_string().ends_with(crate::sync_client::CANCELLED) && try_number < crate::transfer::TRIES => {
                    tracing::warn!(audio_id = %audio_file.id, "Upload failed (try {} of {}): {}; trying again", try_number, crate::transfer::TRIES, e);
                    if try_number + 1 == crate::transfer::TRIES {
                        tokio::time::sleep(crate::transfer::wait_before_last_try()).await;
                    }
                    try_number += 1;
                }
                _ => break attempt,
            }
        };
        match uploaded {
            Ok(upload) => {
                match db.update_audio_file_storage(&audio_file.id, &upload.provider, &upload.storage_key, encrypt) {
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
                let cancelled = e.to_string().ends_with(crate::sync_client::CANCELLED);
                result.errors.push(msg);
                result.failed += 1;

                if cancelled {
                    result.deferred = total - index - 1;
                    break;
                }
                // Three files failed every try: the bucket is out of reach, and
                // the rest wait for the next upload (FILE-14)
                if let Some(sentence) = crate::transfer::stop_after_failures(result.failed, total - index - 1, "upload") {
                    result.deferred = total - index - 1;
                    result.errors.push(sentence);
                    break;
                }
            }
        }
    }

    Ok(result)
}

/// "Re-upload existing recordings encrypted" (ENC-3): every recording in
/// the bucket as a plain object whose file is on this device goes up again
/// encrypted, one at a time, resumable by the parts journal; the plain
/// object is remembered for the purge tag. Needs encryption on and the key.
#[cfg(feature = "file-storage")]
pub async fn reupload_encrypted(
    db: &Database,
    audiofile_directory: &Path,
    cancel: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    sink: Option<std::sync::Arc<dyn crate::sync_client::ProgressSink>>,
    recording_key: Option<&crate::crypto::RecordingKey>,
    // This device's id, from its configuration: hashing a file states that it holds it (FILE-22)
    here: &str,
) -> Result<UploadPendingResult, FileStorageError> {
    let storage = create_storage_service(db)?.ok_or_else(|| FileStorageError::Config("Cloud storage is not enabled.".to_string()))?;
    reupload_encrypted_with(&storage, db, audiofile_directory, cancel, sink, recording_key, here).await
}

#[cfg(feature = "file-storage")]
pub async fn reupload_encrypted_with<S: FileStorageService>(
    storage: &S,
    db: &Database,
    audiofile_directory: &Path,
    cancel: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    sink: Option<std::sync::Arc<dyn crate::sync_client::ProgressSink>>,
    recording_key: Option<&crate::crypto::RecordingKey>,
    // This device's id, from its configuration: hashing a file states that it holds it (FILE-22)
    here: &str,
) -> Result<UploadPendingResult, FileStorageError> {
    if !db.encryption_on().map_err(|e| FileStorageError::Config(e.to_string()))? {
        return Err(FileStorageError::Config("Encryption is off; turn it on first".to_string()));
    }
    let plain_in_bucket: Vec<AudioFileRow> = db
        .get_all_audio_files()
        .map_err(|e| FileStorageError::Config(e.to_string()))?
        .into_iter()
        .filter(|row| row.deleted_at.is_none() && row.storage_key.is_some() && !row.storage_encrypted)
        .collect();
    let old_keys: Vec<(String, String)> = plain_in_bucket.iter().filter_map(|r| r.storage_key.clone().map(|k| (r.id.clone(), k))).collect();
    let result = upload_files_with(storage, db, audiofile_directory, plain_in_bucket, cancel, sink, recording_key, here).await?;
    for (id, old_key) in old_keys {
        if let Ok(Some(row)) = db.get_audio_file(&id) {
            if row.storage_encrypted && row.storage_key.as_deref() != Some(old_key.as_str()) {
                let _ = db.remember_purged_object(&old_key);
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
    recording_key: Option<&crate::crypto::RecordingKey>,
    // This device's id, from its configuration: it holds the file after the download (FILE-22)
    here: &str,
) -> Result<DownloadOutcome, FileStorageError> {
    // Names changed by a sync or a collision reach the disk first (FILE-15)
    if let Err(e) = db.apply_pending_file_renames(audiofile_directory) {
        tracing::warn!("Recording names were not all settled on disk: {}", e);
    }
    let audio_file = db
        .get_audio_file(audio_file_id)
        .map_err(|e| FileStorageError::Config(format!("Failed to read audio file record: {}", e)))?
        .ok_or_else(|| FileStorageError::NotFound(format!("Audio file record {} not found", audio_file_id)))?;

    let local_path = audio_local_path(audiofile_directory, &audio_file.disk_name);
    if local_path.is_file() {
        return Ok(DownloadOutcome::AlreadyLocal);
    }
    // Room on disk for the file under its name (FILE-15)
    let local_path = db
        .disk_path_for_writing(&audio_file.id, audiofile_directory)
        .map_err(|e| FileStorageError::LocalFile(e.to_string()))?;
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

    // Tried three times, the third a minute after the second (FILE-14); a
    // missing object is an answer and is not tried again
    let mut try_number = 1;
    let downloaded = loop {
        let attempt = storage.download(&storage_key, &local_path).await;
        match &attempt {
            Err(e) if !e.is_local() && !matches!(e, FileStorageError::NotFound(_)) && try_number < crate::transfer::TRIES => {
                tracing::warn!(audio_id = %audio_file.id, "Download failed (try {} of {}): {}; trying again", try_number, crate::transfer::TRIES, e);
                if try_number + 1 == crate::transfer::TRIES {
                    tokio::time::sleep(crate::transfer::wait_before_last_try()).await;
                }
                try_number += 1;
            }
            _ => break attempt,
        }
    };
    let bytes = match downloaded {
        Ok(bytes) => bytes,
        Err(FileStorageError::NotFound(key)) => {
            // The bucket does not hold it any more (FILE-22)
            if let Err(e) = db.set_file_location(&audio_file.id, crate::database::PLACE_CLOUD, false) {
                tracing::warn!("Could not record that the bucket lacks {}: {}", audio_file.id, e);
            }
            return Err(FileStorageError::NotFound(key));
        }
        Err(e) => return Err(e),
    };
    let bytes = decrypt_downloaded(&audio_file, &local_path, recording_key).await?.unwrap_or(bytes);
    if let Err(e) = verify_downloaded(&audio_file, &local_path) {
        // The object is not the recording: the bucket does not hold it (FILE-22)
        if let Err(e) = db.set_file_location(&audio_file.id, crate::database::PLACE_CLOUD, false) {
            tracing::warn!("Could not record that the bucket lacks {}: {}", audio_file.id, e);
        }
        return Err(e);
    }
    if let Err(e) = db.set_file_location(&audio_file.id, here, true) {
        tracing::warn!("Could not record that this device holds {}: {}", audio_file.id, e);
    }

    tracing::info!(audio_id = %audio_file.id, size_bytes = bytes, "Downloaded audio file");
    Ok(DownloadOutcome::Downloaded(bytes))
}

/// The recordings of a batch that are on this device after it (FILE-22).
#[cfg(feature = "file-storage")]
fn note_downloaded(db: &Database, audiofile_directory: &Path, asked: &[(String, String)], here: &str) {
    for (id, disk_name) in asked {
        if audio_local_path(audiofile_directory, disk_name).is_file() {
            if let Err(e) = db.set_file_location(id, here, true) {
                tracing::warn!("Could not record that this device holds {}: {}", id, e);
            }
        }
    }
}

/// A downloaded object that is encrypted (ENC-4): opened with the recording
/// key into the plain file, written whole before it exists; without the key
/// the bytes stay as the bucket holds them, marked by their header, and a
/// warning says so. Returns the plain length when it was decrypted.
async fn decrypt_downloaded(audio_file: &AudioFileRow, local_path: &Path, recording_key: Option<&crate::crypto::RecordingKey>) -> Result<Option<u64>, FileStorageError> {
    if !crate::crypto::file_is_encrypted(local_path) {
        return Ok(None);
    }
    let Some(key) = recording_key else {
        tracing::warn!(audio_id = %audio_file.id, "The object is encrypted and this device holds no recording key; kept as it is");
        return Ok(None);
    };
    let mut encrypted_name = local_path.file_name().map(|n| n.to_os_string()).unwrap_or_default();
    encrypted_name.push(crate::crypto::OBJECT_SUFFIX);
    let encrypted = local_path.with_file_name(encrypted_name);
    std::fs::rename(local_path, &encrypted)?;
    let opened = crate::crypto::decrypt_file(key, &encrypted, local_path);
    let _ = std::fs::remove_file(&encrypted);
    match opened {
        Ok(n) => Ok(Some(n)),
        Err(e) => Err(FileStorageError::Download(format!("The object of {} did not open with the recording key: {}", audio_file.id, e))),
    }
}

/// A downloaded file against the row's content hash (Stage 13): when the
/// row has one and the bytes differ, the file is removed and the download
/// reported as failed, so a wrong object never looks like the recording.
/// The words of a downloaded object whose hash is not the recording's.
const NOT_THE_RECORDING: &str = "is not the recording";

/// Whether a download failed because the bucket does not hold the recording:
/// no object, or an object with other bytes.
fn bucket_lacks(e: &FileStorageError) -> bool {
    match e {
        FileStorageError::NotFound(_) => true,
        FileStorageError::Download(words) => words.contains(NOT_THE_RECORDING),
        _ => false,
    }
}

/// Whether the bucket holds an object now, asked of the bucket itself
/// (FILE-26): it is there, and it is not waiting for the lifecycle rule.
pub async fn bucket_holds<S: FileStorageService>(storage: &S, storage_key: &str) -> Result<bool, FileStorageError> {
    if !storage.exists(storage_key).await? {
        return Ok(false);
    }
    Ok(!storage.purge_tagged(storage_key).await?)
}

fn verify_downloaded(audio_file: &AudioFileRow, local_path: &Path) -> Result<(), FileStorageError> {
    let Some(expected) = audio_file.content_sha256.as_deref() else { return Ok(()) };
    if crate::crypto::file_is_encrypted(local_path) {
        // Kept as the bucket holds it, for a device without the key (ENC-4); the hash is of the plain bytes
        return Ok(());
    }
    let actual = crate::transfer::file_sha256(local_path)
        .map_err(|e| FileStorageError::LocalFile(format!("Could not hash {}: {}", local_path.display(), e)))?;
    if actual != expected {
        let _ = std::fs::remove_file(local_path);
        return Err(FileStorageError::Download(format!(
            "The object for {} {}: its hash {} is not the row's {}",
            audio_file.id, NOT_THE_RECORDING, &actual[..12], &expected[..12]
        )));
    }
    Ok(())
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
    /// Number of files not attempted because three files failed every try and the batch stopped
    pub deferred: usize,
    /// Error messages for failed downloads
    pub errors: Vec<String>,
}

#[cfg(feature = "file-storage")]
async fn download_audio_file_set<S: FileStorageService>(
    db: &Database,
    storage: &S,
    audiofile_directory: &Path,
    audio_files: Vec<AudioFileRow>,
    recording_key: Option<&crate::crypto::RecordingKey>,
) -> DownloadMissingResult {
    let mut result = DownloadMissingResult::default();
    let total = audio_files.len();

    for (index, audio_file) in audio_files.into_iter().enumerate() {
        let local_path = audio_local_path(audiofile_directory, &audio_file.disk_name);
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

        // Tried three times, the third a minute after the second (FILE-14); an
        // object that is not there is an answer, not a failure of the link
        let mut try_number = 1;
        let downloaded = loop {
            let attempt = match storage.download(&storage_key, &local_path).await {
                Ok(bytes) => match decrypt_downloaded(&audio_file, &local_path, recording_key).await {
                    Ok(plain) => verify_downloaded(&audio_file, &local_path).map(|_| plain.unwrap_or(bytes)),
                    Err(e) => Err(e),
                },
                Err(e) => Err(e),
            };
            match &attempt {
                Err(e) if !e.is_local() && !matches!(e, FileStorageError::NotFound(_)) && try_number < crate::transfer::TRIES => {
                    tracing::warn!(audio_id = %audio_file.id, "Download failed (try {} of {}): {}; trying again", try_number, crate::transfer::TRIES, e);
                    if try_number + 1 == crate::transfer::TRIES {
                        tokio::time::sleep(crate::transfer::wait_before_last_try()).await;
                    }
                    try_number += 1;
                }
                _ => break attempt,
            }
        };
        match downloaded {
            Ok(bytes) => {
                tracing::info!(audio_id = %audio_file.id, size_bytes = bytes, "Downloaded audio file");
                result.downloaded += 1;
            }
            Err(e) => {
                // An object that is not there, or is not the recording: the bucket does not hold it (FILE-22)
                if bucket_lacks(&e) {
                    if let Err(e) = db.set_file_location(&audio_file.id, crate::database::PLACE_CLOUD, false) {
                        tracing::warn!("Could not record that the bucket lacks {}: {}", audio_file.id, e);
                    }
                }
                let msg = format!("Failed to download {}: {}", audio_file.id, e);
                tracing::error!("{}", msg);
                result.errors.push(msg);
                result.failed += 1;

                if let Some(sentence) = crate::transfer::stop_after_failures(result.failed, total - index - 1, "download") {
                    result.deferred = total - index - 1;
                    result.errors.push(sentence);
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
    recording_key: Option<&crate::crypto::RecordingKey>,
    here: &str,
) -> Result<DownloadMissingResult, FileStorageError> {
    // Names changed by a sync or a collision reach the disk first (FILE-15)
    if let Err(e) = db.apply_pending_file_renames(audiofile_directory) {
        tracing::warn!("Recording names were not all settled on disk: {}", e);
    }
    let audio_files = db
        .get_audio_files_for_note(note_id)
        .map_err(|e| FileStorageError::Config(format!("Failed to read audio files for note: {}", e)))?;

    let needs_cloud = audio_files.iter().any(|af| {
        !audio_local_path(audiofile_directory, &af.disk_name).is_file()
            && af.storage_key.is_some()
    });

    if !needs_cloud {
        // Nothing to fetch: report counts without touching the network.
        let mut result = DownloadMissingResult::default();
        for af in &audio_files {
            if audio_local_path(audiofile_directory, &af.disk_name).is_file() {
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

    let asked: Vec<(String, String)> = audio_files.iter().map(|a| (a.id.clone(), a.disk_name.clone())).collect();
    let result = download_audio_file_set(db, &storage, audiofile_directory, audio_files, recording_key).await;
    note_downloaded(db, audiofile_directory, &asked, here);
    Ok(result)
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
    recording_key: Option<&crate::crypto::RecordingKey>,
    here: &str,
) -> Result<DownloadMissingResult, FileStorageError> {
    // Names changed by a sync or a collision reach the disk first (FILE-15)
    if let Err(e) = db.apply_pending_file_renames(audiofile_directory) {
        tracing::warn!("Recording names were not all settled on disk: {}", e);
    }
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

    let asked: Vec<(String, String)> = audio_files.iter().map(|a| (a.id.clone(), a.disk_name.clone())).collect();
    let result = download_audio_file_set(db, &storage, audiofile_directory, audio_files, recording_key).await;
    note_downloaded(db, audiofile_directory, &asked, here);
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    const HASH: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    #[test]
    fn test_storage_key_for_with_prefix() {
        assert_eq!(storage_key_for(Some("audio"), "recording.mp3", HASH).unwrap(), format!("audio/{}.mp3", HASH));
    }

    #[test]
    fn test_storage_key_for_with_trailing_slash() {
        assert_eq!(storage_key_for(Some("audio/"), "recording.mp3", HASH).unwrap(), format!("audio/{}.mp3", HASH));
    }

    #[test]
    fn test_storage_key_for_no_prefix() {
        assert_eq!(storage_key_for(None, "recording.wav", HASH).unwrap(), format!("{}.wav", HASH));
    }

    #[test]
    fn a_key_is_by_the_content_hash_and_never_by_the_id() {
        let hash = "a".repeat(64);
        assert_eq!(storage_key_for(Some("audio"), "REC.MP3", &hash).unwrap(), format!("audio/{}.mp3", hash));
        assert_eq!(storage_key_for(None, "REC.MP3", "not a hash"), None);
        assert_eq!(storage_key_for(None, "REC.MP3", ""), None);
    }

    #[test]
    fn test_storage_key_for_empty_prefix() {
        assert_eq!(storage_key_for(Some(""), "test.flac", HASH).unwrap(), format!("{}.flac", HASH));
    }

    #[test]
    fn test_storage_key_for_no_extension() {
        assert_eq!(storage_key_for(Some("files"), "noextension", HASH).unwrap(), format!("files/{}.bin", HASH));
    }

    #[test]
    fn test_storage_key_for_uppercase_extension_is_lowercased() {
        assert_eq!(storage_key_for(Some("audio"), "REC.MP3", HASH).unwrap(), format!("audio/{}.mp3", HASH));
    }

    #[test]
    fn test_storage_key_for_multiple_dots() {
        assert_eq!(storage_key_for(Some("audio"), "my.recording.mp3", HASH).unwrap(), format!("audio/{}.mp3", HASH));
    }

    #[test]
    fn test_storage_key_for_hebrew_filename() {
        assert_eq!(storage_key_for(Some("audio"), "הקלטה של פגישה.OGG", HASH).unwrap(), format!("audio/{}.ogg", HASH));
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
    // An upload in parts (Stage 13, FILE-19) against a store in memory.
    // ------------------------------------------------------------------

    mod parts {
        use super::super::*;
        use std::cell::RefCell;
        use std::collections::HashMap;

        /// A bucket in memory: the parts of each upload, and the objects
        /// completed. `fail_part` makes that part number fail once.
        #[derive(Default)]
        struct MemoryStore {
            uploads: RefCell<HashMap<String, HashMap<u32, Vec<u8>>>>,
            objects: RefCell<HashMap<String, Vec<u8>>>,
            fail_part: RefCell<Option<u32>>,
            puts: RefCell<Vec<u32>>,
        }

        impl PartStore for MemoryStore {
            async fn begin(&self, key: &str) -> Result<String, FileStorageError> {
                let id = format!("upload-of-{}-{}", key, self.uploads.borrow().len() + 1);
                self.uploads.borrow_mut().insert(id.clone(), HashMap::new());
                Ok(id)
            }
            async fn put_part(&self, _key: &str, upload_id: &str, part_number: u32, bytes: Vec<u8>) -> Result<String, FileStorageError> {
                if self.fail_part.borrow_mut().take_if(|n| *n == part_number).is_some() {
                    return Err(FileStorageError::Network("the connection dropped".into()));
                }
                self.puts.borrow_mut().push(part_number);
                let etag = format!("\"{:x}\"", bytes.len() * 7919 + part_number as usize);
                self.uploads.borrow_mut().get_mut(upload_id).expect("begun").insert(part_number, bytes);
                Ok(etag)
            }
            async fn complete(&self, key: &str, upload_id: &str, parts: &[(u32, String)]) -> Result<(), FileStorageError> {
                let uploads = self.uploads.borrow();
                let stored = uploads.get(upload_id).expect("begun");
                let mut whole = Vec::new();
                for (n, etag) in parts {
                    let bytes = stored.get(n).ok_or_else(|| FileStorageError::Upload(format!("part {} missing", n)))?;
                    assert_eq!(etag, &format!("\"{:x}\"", bytes.len() * 7919 + *n as usize), "the tag of part {} is the one given", n);
                    whole.extend_from_slice(bytes);
                }
                self.objects.borrow_mut().insert(key.to_string(), whole);
                Ok(())
            }
        }

        /// A journal in memory, shaped like the database's.
        #[derive(Default)]
        struct MemoryJournal {
            begun: RefCell<Option<crate::database::UploadBegun>>,
            cancel: std::sync::atomic::AtomicBool,
            moved: RefCell<Vec<u64>>,
        }

        impl PartJournal for MemoryJournal {
            fn begun(&self) -> Result<Option<crate::database::UploadBegun>, FileStorageError> { Ok(self.begun.borrow().clone()) }
            fn begin(&self, storage_key: &str, upload_id: &str, part_size: u64) -> Result<(), FileStorageError> {
                *self.begun.borrow_mut() = Some(crate::database::UploadBegun { storage_key: storage_key.into(), upload_id: upload_id.into(), part_size, parts: vec![] });
                Ok(())
            }
            fn part_done(&self, part_number: u32, etag: &str) -> Result<(), FileStorageError> {
                self.begun.borrow_mut().as_mut().expect("begun").parts.push((part_number, etag.to_string()));
                Ok(())
            }
            fn finished(&self) -> Result<(), FileStorageError> { *self.begun.borrow_mut() = None; Ok(()) }
            fn cancelled(&self) -> bool { self.cancel.load(std::sync::atomic::Ordering::Relaxed) }
            fn moved(&self, done: u64, _total: u64) { self.moved.borrow_mut().push(done); }
        }

        fn file_of(len: usize) -> (tempfile::TempDir, std::path::PathBuf) {
            let temp = tempfile::TempDir::new().unwrap();
            let path = temp.path().join("הקלטה.ogg");
            let bytes: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
            std::fs::write(&path, &bytes).unwrap();
            (temp, path)
        }

        #[tokio::test]
        async fn a_failed_part_is_the_only_one_sent_again_and_the_object_is_the_whole_file() {
            let (_temp, path) = file_of(2500);
            let store = MemoryStore::default();
            *store.fail_part.borrow_mut() = Some(2);
            let journal = MemoryJournal::default();

            let failed = upload_in_parts(&store, &mut std::fs::File::open(&path).unwrap(), "audio/x.ogg", &journal, 1000).await;
            assert!(matches!(failed, Err(FileStorageError::Network(_))), "{:?}", failed.err());
            assert_eq!(journal.begun.borrow().as_ref().unwrap().parts.len(), 1, "part 1 is remembered");
            assert!(store.objects.borrow().is_empty(), "no object until every part is there");

            let total = upload_in_parts(&store, &mut std::fs::File::open(&path).unwrap(), "audio/x.ogg", &journal, 1000).await.unwrap();
            assert_eq!(total, 2500);
            assert_eq!(*store.puts.borrow(), vec![1, 2, 3], "part 1 was not sent twice");
            assert_eq!(store.objects.borrow()["audio/x.ogg"], std::fs::read(&path).unwrap());
            assert!(journal.begun.borrow().is_none(), "the journal is clear once complete");
            assert_eq!(*journal.moved.borrow(), vec![1000, 2000, 2500]);
        }

        #[tokio::test]
        async fn a_cancel_stops_before_the_next_part_and_a_changed_key_or_part_size_begins_again() {
            let (_temp, path) = file_of(2500);
            let store = MemoryStore::default();
            let journal = MemoryJournal::default();
            journal.cancel.store(true, std::sync::atomic::Ordering::Relaxed);
            let stopped = upload_in_parts(&store, &mut std::fs::File::open(&path).unwrap(), "audio/x.ogg", &journal, 1000).await;
            assert!(stopped.unwrap_err().to_string().ends_with(crate::sync_client::CANCELLED));
            assert!(store.puts.borrow().is_empty());
            assert!(journal.begun.borrow().is_some(), "the begun upload is kept for the next run");
            journal.cancel.store(false, std::sync::atomic::Ordering::Relaxed);

            // Another part size: the remembered upload does not fit, a new one begins
            upload_in_parts(&store, &mut std::fs::File::open(&path).unwrap(), "audio/x.ogg", &journal, 2000).await.unwrap();
            assert_eq!(*store.puts.borrow(), vec![1, 2]);
            assert_eq!(store.uploads.borrow().len(), 2, "the first upload was abandoned to the bucket's rule");
            assert_eq!(store.objects.borrow()["audio/x.ogg"].len(), 2500);
        }

        #[tokio::test]
        async fn the_database_journal_remembers_the_parts_of_one_recording() {
            let temp = tempfile::TempDir::new().unwrap();
            let db = Database::new(&temp.path().join("j.db")).unwrap();
            let id = db.create_audio_file("a.ogg", None, None, crate::models::FileOrigin::Imported, None).unwrap();
            let other = db.create_audio_file("b.ogg", None, None, crate::models::FileOrigin::Imported, None).unwrap();
            let journal = DatabaseJournal { db: &db, audio_id: &id, cancel: None, sink: None };
            assert!(journal.begun().unwrap().is_none());
            journal.begin("audio/a.ogg", "upload-1", 8).unwrap();
            journal.part_done(1, "\"e1\"").unwrap();
            journal.part_done(2, "\"e2\"").unwrap();
            let begun = journal.begun().unwrap().unwrap();
            assert_eq!((begun.storage_key.as_str(), begun.upload_id.as_str(), begun.part_size), ("audio/a.ogg", "upload-1", 8));
            assert_eq!(begun.parts, vec![(1, "\"e1\"".to_string()), (2, "\"e2\"".to_string())]);
            assert!(db.upload_begun(&other).unwrap().is_none(), "another recording's journal is its own");
            journal.begin("audio/a.ogg", "upload-2", 8).unwrap();
            assert!(journal.begun().unwrap().unwrap().parts.is_empty(), "a new upload forgets the old parts");
            journal.finished().unwrap();
            assert!(journal.begun().unwrap().is_none());
        }
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
            /// What an upload in parts put, by key
            uploaded: Mutex<std::collections::HashMap<String, Vec<u8>>>,
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
            async fn purge_tagged(&self, _k: &str) -> Result<bool, FileStorageError> { Ok(false) }
            async fn upload_in_parts(&self, source: &mut dyn crate::crypto::ByteSource, remote_key: &str, _journal: &dyn PartJournal) -> Result<UploadResult, FileStorageError> {
                let mut bytes = vec![0u8; source.len() as usize];
                source.read_at(0, &mut bytes)?;
                self.uploaded.lock().unwrap().insert(remote_key.to_string(), bytes);
                Ok(UploadResult { storage_key: remote_key.to_string(), provider: "fake".into(), size_bytes: source.len() })
            }
        }

        fn setup() -> (Database, TempDir) {
            let temp = TempDir::new().unwrap();
            let db = Database::new(&temp.path().join("test.db")).unwrap();
            (db, temp)
        }

        /// FILE-23: a file larger than the account's upload limit is not
        /// uploaded, is counted as such, and stays waiting; a smaller one goes.
        #[tokio::test]
        async fn a_file_over_the_accounts_upload_limit_stays_where_it_is() {
            let (db, temp) = setup();
            let dir = temp.path().join("audio");
            std::fs::create_dir_all(&dir).unwrap();
            db.set_file_storage_config("s3", Some(&serde_json::json!({"bucket": "b", "region": "eu-central-1", "access_key_id": "k", "secret_access_key": "s"}))).unwrap();
            db.set_max_upload_mb(1).unwrap();
            let big = row(&db, "הרצאה ארוכה.wav", false);
            std::fs::write(audio_local_path(&dir, &big.disk_name), vec![1u8; 1024 * 1024 + 1]).unwrap();
            let small = row(&db, "קצר.ogg", false);
            std::fs::write(audio_local_path(&dir, &small.disk_name), vec![2u8; 1024 * 1024]).unwrap();
            let storage = FakeStorage { objects: Default::default(), downloads: Mutex::new(0), fail_after: None, uploaded: Mutex::new(Default::default()) };
            let pending = db.get_audio_files_pending_upload().unwrap();
            let result = upload_files_with(&storage, &db, &dir, pending, None, None, None, "01a09526bbbb70808f15a84d31aaa8d2").await.unwrap();
            assert_eq!((result.uploaded, result.too_large, result.failed), (1, 1, 0), "{:?}", result.errors);
            assert!(db.get_audio_file(&big.id).unwrap().unwrap().storage_key.is_none());
            assert!(db.get_audio_file(&small.id).unwrap().unwrap().storage_key.is_some(), "exactly the limit is allowed");
            assert_eq!(db.places_holding(&small.id).unwrap().first().map(String::as_str), Some("cloud"));
        }

        fn row(db: &Database, filename: &str, in_cloud: bool) -> AudioFileRow {
            let id = db.create_audio_file(filename, None, None, crate::models::FileOrigin::Imported, None).unwrap();
            if in_cloud {
                let key = storage_key_for(Some("audio"), filename, &format!("{:0>64}", id.replace('-', ""))).unwrap();
                db.update_audio_file_storage(&id, "fake", &key, false).unwrap();
            }
            db.get_audio_file(&id).unwrap().unwrap()
        }

        #[tokio::test]
        async fn download_set_skips_local_and_not_in_cloud_and_downloads_the_rest() {
            let (db, temp) = setup();
            let dir = temp.path().join("audio");
            std::fs::create_dir_all(&dir).unwrap();

            let local = row(&db, "מקומי.MP3", true);
            std::fs::write(audio_local_path(&dir, &local.disk_name), b"x").unwrap();
            let not_uploaded = row(&db, "not-yet.ogg", false);
            let remote = row(&db, "בענן.WAV", true);

            let mut objects = std::collections::HashMap::new();
            objects.insert(remote.storage_key.clone().unwrap(), b"wav-bytes".to_vec());
            let storage = FakeStorage { objects, downloads: Mutex::new(0), fail_after: None, uploaded: Mutex::new(Default::default()) };

            let result = download_audio_file_set(&db, &storage, &dir, vec![local.clone(), not_uploaded.clone(), remote.clone()], None).await;
            assert_eq!(result.already_local, 1);
            assert_eq!(result.not_in_cloud, 1);
            assert_eq!(result.downloaded, 1);
            assert_eq!(result.failed, 0);
            assert!(result.errors.is_empty());
            // Downloaded to the path the row names (Stage 13)
            assert_eq!(remote.disk_name, "בענן.WAV", "an imported file keeps its own name (FILE-15)");
            assert!(dir.join(&remote.disk_name).is_file());
        }

        /// FILE-14: each file is tried three times; after three files failed
        /// every try the batch stops, and the rest are not attempted.
        #[tokio::test]
        async fn download_set_stops_after_three_files_failed_every_try() {
            let (db, temp) = setup();
            let dir = temp.path().join("audio");
            std::fs::create_dir_all(&dir).unwrap();

            let rows: Vec<AudioFileRow> = (0..6).map(|i| row(&db, &format!("f{}.mp3", i), true)).collect();
            let mut objects = std::collections::HashMap::new();
            for r in &rows {
                objects.insert(r.storage_key.clone().unwrap(), vec![1u8; 10]);
            }
            let storage = FakeStorage { objects, downloads: Mutex::new(0), fail_after: Some(1), uploaded: Mutex::new(Default::default()) };

            let result = download_audio_file_set(&db, &storage, &dir, rows, None).await;
            assert_eq!(result.downloaded, 1);
            assert_eq!(result.failed, 3, "{:?}", result.errors);
            assert_eq!(result.deferred, 2, "the files after the third failure are not attempted");
            assert_eq!(result.errors.len(), 4, "three failures and the sentence that stops: {:?}", result.errors);
            assert!(result.errors[3].starts_with("Stopped after 3 files failed; 2 file(s) not attempted"), "{:?}", result.errors);
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
            let storage = FakeStorage { objects, downloads: Mutex::new(0), fail_after: None, uploaded: Mutex::new(Default::default()) };

            let result = download_audio_file_set(&db, &storage, &dir, vec![missing, present], None).await;
            assert_eq!(result.failed, 1);
            assert_eq!(result.downloaded, 1);
            assert_eq!(result.deferred, 0);
        }

        #[tokio::test]
        async fn download_missing_is_noop_without_storage_config() {
            let (db, temp) = setup();
            let dir = temp.path().join("audio");
            row(&db, "a.mp3", false);
            let result = download_missing_audio_files(&db, &dir, None, "01a09526bbbb70808f15a84d31aaa8d2").await.unwrap();
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
                download_audio_file(&db, &dir, &pending.id, None, "01a09526bbbb70808f15a84d31aaa8d2").await.unwrap(),
                DownloadOutcome::NotInCloud
            );

            let local = row(&db, "local.mp3", true);
            std::fs::write(audio_local_path(&dir, &local.disk_name), b"x").unwrap();
            assert_eq!(
                download_audio_file(&db, &dir, &local.id, None, "01a09526bbbb70808f15a84d31aaa8d2").await.unwrap(),
                DownloadOutcome::AlreadyLocal
            );

            // In cloud, not local, no config on this device: a clear Config error
            let remote = row(&db, "remote.mp3", true);
            match download_audio_file(&db, &dir, &remote.id, None, "01a09526bbbb70808f15a84d31aaa8d2").await {
                Err(FileStorageError::Config(_)) => {}
                other => panic!("expected Config error, got {:?}", other.map(|_| ())),
            }

            // Unknown id
            match download_audio_file(&db, &dir, "00000000000070008000000000000099", None, "01a09526bbbb70808f15a84d31aaa8d2").await {
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

            let result = download_audio_files_for_note(&db, &dir, &note_id, None, "01a09526bbbb70808f15a84d31aaa8d2").await.unwrap();
            assert_eq!(result.not_in_cloud, 1);
            assert_eq!(result.downloaded, 0);
        }

        /// FILE-18: a downloaded object is compared with the row's content
        /// hash; different bytes are removed and reported, equal bytes kept.
        #[tokio::test]
        async fn a_download_is_verified_against_the_rows_content_hash() {
            let (db, temp) = setup();
            let dir = temp.path().join("audio");
            std::fs::create_dir_all(&dir).unwrap();
            let remote = row(&db, "הקלטה.ogg", true);
            std::fs::write(audio_local_path(&dir, &remote.disk_name), b"the recording").unwrap();
            let hash = db.store_content_hash(&remote.id, &dir, &crate::database::get_local_device_id().simple().to_string()).unwrap();
            assert_eq!(hash.len(), 64);
            std::fs::remove_file(audio_local_path(&dir, &remote.disk_name)).unwrap();
            let remote = db.get_audio_file(&remote.id).unwrap().unwrap();
            assert_eq!(remote.content_sha256.as_deref(), Some(hash.as_str()));

            let mut objects = std::collections::HashMap::new();
            objects.insert(remote.storage_key.clone().unwrap(), b"something else".to_vec());
            let storage = FakeStorage { objects, downloads: Mutex::new(0), fail_after: None, uploaded: Mutex::new(Default::default()) };
            let result = download_audio_file_set(&db, &storage, &dir, vec![remote.clone()], None).await;
            assert_eq!((result.downloaded, result.failed), (0, 1));
            assert!(result.errors[0].contains("not the recording"), "{:?}", result.errors);
            assert!(!dir.join(&remote.disk_name).exists(), "a wrong object is not left looking like the recording");

            let mut objects = std::collections::HashMap::new();
            objects.insert(remote.storage_key.clone().unwrap(), b"the recording".to_vec());
            let storage = FakeStorage { objects, downloads: Mutex::new(0), fail_after: None, uploaded: Mutex::new(Default::default()) };
            let result = download_audio_file_set(&db, &storage, &dir, vec![remote.clone()], None).await;
            assert_eq!((result.downloaded, result.failed), (1, 0), "{:?}", result.errors);
            assert!(dir.join(&remote.disk_name).is_file());
        }

        /// ENC-3, ENC-4: with encryption on, the object is `.enc` and opens
        /// only with the key; a download with the key gives the plain file
        /// back, one without keeps the bytes as the bucket holds them.
        #[tokio::test]
        async fn an_encrypted_upload_makes_an_enc_object_that_a_download_with_the_key_opens() {
            let (db, temp) = setup();
            let dir = temp.path().join("audio");
            std::fs::create_dir_all(&dir).unwrap();
            db.set_file_storage_config("s3", Some(&serde_json::json!({"bucket": "b", "region": "us-east-1", "access_key_id": "k", "secret_access_key": "s"}))).unwrap();
            assert!(!db.encryption_on().unwrap());
            db.set_encryption_on(true).unwrap();
            assert!(db.encryption_on().unwrap());
            let row = row(&db, "שיר.ogg", false);
            let plain: Vec<u8> = (0..(crate::crypto::CHUNK_PLAIN + 777)).map(|i| (i % 253) as u8).collect();
            std::fs::write(audio_local_path(&dir, &row.disk_name), &plain).unwrap();
            let hash = db.store_content_hash(&row.id, &dir, &crate::database::get_local_device_id().simple().to_string()).unwrap();
            let key = crate::crypto::RecordingKey::generate();
            let storage = FakeStorage { objects: Default::default(), downloads: Mutex::new(0), fail_after: None, uploaded: Mutex::new(Default::default()) };

            let without = upload_files_with(&storage, &db, &dir, vec![db.get_audio_file(&row.id).unwrap().unwrap()], None, None, None, "01a09526bbbb70808f15a84d31aaa8d2").await;
            assert!(matches!(&without, Err(FileStorageError::Config(m)) if m == NO_RECORDING_KEY), "{:?}", without.err());

            let result = upload_files_with(&storage, &db, &dir, vec![db.get_audio_file(&row.id).unwrap().unwrap()], None, None, Some(&key), "01a09526bbbb70808f15a84d31aaa8d2").await.unwrap();
            assert_eq!((result.uploaded, result.failed), (1, 0), "{:?}", result.errors);
            let after = db.get_audio_file(&row.id).unwrap().unwrap();
            let object_key = after.storage_key.clone().unwrap();
            assert_eq!(object_key, format!("{}.ogg{}", hash, crate::crypto::OBJECT_SUFFIX));
            assert!(after.storage_encrypted);
            let object = storage.uploaded.lock().unwrap()[&object_key].clone();
            assert!(crate::crypto::is_encrypted_header(&object) && object.len() as u64 == crate::crypto::encrypted_len(plain.len() as u64));
            let mut opened = Vec::new();
            crate::crypto::decrypt_stream(&key, &mut object.as_slice(), &mut opened).unwrap();
            assert_eq!(opened, plain);

            // A download with the key: the plain file, verified by its hash; without: kept as it is
            std::fs::remove_file(audio_local_path(&dir, &after.disk_name)).unwrap();
            let mut objects = std::collections::HashMap::new();
            objects.insert(object_key.clone(), object.clone());
            let bucket = FakeStorage { objects, downloads: Mutex::new(0), fail_after: None, uploaded: Mutex::new(Default::default()) };
            let result = download_audio_file_set(&db, &bucket, &dir, vec![after.clone()], Some(&key)).await;
            assert_eq!((result.downloaded, result.failed), (1, 0), "{:?}", result.errors);
            assert_eq!(std::fs::read(audio_local_path(&dir, &after.disk_name)).unwrap(), plain);
            std::fs::remove_file(audio_local_path(&dir, &after.disk_name)).unwrap();
            let result = download_audio_file_set(&db, &bucket, &dir, vec![after.clone()], None).await;
            assert_eq!((result.downloaded, result.failed), (1, 0), "{:?}", result.errors);
            assert!(crate::crypto::file_is_encrypted(&audio_local_path(&dir, &after.disk_name)), "a keyless device keeps the object as it is");
            let result = download_audio_file_set(&db, &bucket, &dir, vec![after.clone()], Some(&crate::crypto::RecordingKey::generate())).await;
            assert_eq!(result.already_local, 1, "the file is there, encrypted or not");
            std::fs::remove_file(audio_local_path(&dir, &after.disk_name)).unwrap();
            let result = download_audio_file_set(&db, &bucket, &dir, vec![after.clone()], Some(&crate::crypto::RecordingKey::generate())).await;
            assert_eq!((result.downloaded, result.failed), (0, 1), "the wrong key does not open it");
            assert!(!audio_local_path(&dir, &after.disk_name).exists());
        }

        /// ENC-3: "Re-upload existing recordings encrypted" sends the plain
        /// objects' files up again encrypted and remembers the plain objects
        /// for the purge tag.
        #[tokio::test]
        async fn reupload_encrypted_replaces_plain_objects_and_remembers_them_for_the_purge() {
            let (db, temp) = setup();
            let dir = temp.path().join("audio");
            std::fs::create_dir_all(&dir).unwrap();
            db.set_file_storage_config("s3", Some(&serde_json::json!({"bucket": "b", "region": "us-east-1", "access_key_id": "k", "secret_access_key": "s"}))).unwrap();
            let plain_here = row(&db, "here.ogg", true);
            std::fs::write(audio_local_path(&dir, &plain_here.disk_name), b"plain bytes here").unwrap();
            let plain_elsewhere = row(&db, "elsewhere.ogg", true);
            let key = crate::crypto::RecordingKey::generate();
            let storage = FakeStorage { objects: Default::default(), downloads: Mutex::new(0), fail_after: None, uploaded: Mutex::new(Default::default()) };
            assert!(reupload_encrypted_with(&storage, &db, &dir, None, None, Some(&key), "01a09526bbbb70808f15a84d31aaa8d2").await.is_err(), "encryption is off");
            db.set_encryption_on(true).unwrap();
            let result = reupload_encrypted_with(&storage, &db, &dir, None, None, Some(&key), "01a09526bbbb70808f15a84d31aaa8d2").await.unwrap();
            assert_eq!((result.uploaded, result.skipped, result.failed), (1, 1, 0), "{:?}", result.errors);
            let after = db.get_audio_file(&plain_here.id).unwrap().unwrap();
            assert!(after.storage_encrypted && after.storage_key.as_deref().unwrap().ends_with(".enc"));
            assert_eq!(db.purged_objects().unwrap(), vec![plain_here.storage_key.clone().unwrap()], "the plain object is remembered for the tag");
            let untouched = db.get_audio_file(&plain_elsewhere.id).unwrap().unwrap();
            assert!(!untouched.storage_encrypted, "a file another device holds is left for that device");
        }

        #[tokio::test]
        async fn upload_pending_skips_files_missing_locally() {
            let (db, temp) = setup();
            let dir = temp.path().join("audio");
            std::fs::create_dir_all(&dir).unwrap();
            // A record synced from another device: pending, but no local file.
            row(&db, "elsewhere.mp3", false);

            // Storage not configured -> Config error for the explicit call
            match upload_pending_audio_files(&db, &dir, None, None, None, "01a09526bbbb70808f15a84d31aaa8d2").await {
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
            let result = upload_pending_audio_files(&db, &dir, None, None, None, "01a09526bbbb70808f15a84d31aaa8d2").await.unwrap();
            assert_eq!(result.skipped, 1);
            assert_eq!(result.uploaded, 0);
            assert_eq!(result.failed, 0);
            assert!(result.errors.is_empty());
        }
    }
}
