//! Android-specific bindings for VoiceCore.
//!
//! This module provides a simplified API for the Android application,
//! exposed via UniFFI bindings.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};


use crate::config::Config;
use crate::database::Database;
use crate::search;
use crate::sync_client::SyncClient;
use crate::UUID_SHORT_LEN;

/// Format a Unix timestamp (i64) to "YYYY-MM-DD HH:MM:SS" string for display.
/// An instant, with the clock that was being read where it happened.
///
/// The core does not render dates: only the application knows the phone's
/// locale and whether it shows a 12 or 24-hour clock. It hands over the
/// instant and the offset that was in force when the action happened, and the
/// application draws them.
#[derive(Debug, Clone, uniffi::Record)]
pub struct Stamp {
    /// Seconds since the Unix epoch.
    pub at: i64,
    /// Seconds east of UTC where the action happened, when it was recorded.
    /// Without one, a reader falls back to its own timezone.
    pub offset: Option<i32>,
    /// IANA name of that timezone, e.g. "Asia/Jerusalem", when it was known.
    pub zone: Option<String>,
}

fn stamp(at: i64, offset: Option<i32>, zone: Option<String>) -> Stamp {
    Stamp { at, offset, zone }
}

fn stamp_opt(at: Option<i64>, offset: Option<i32>, zone: Option<String>) -> Option<Stamp> {
    at.map(|at| Stamp { at, offset, zone })
}

/// Error type exposed to Kotlin via UniFFI
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum VoiceCoreError {
    #[error("Database error: {msg}")]
    Database { msg: String },
    #[error("Configuration error: {msg}")]
    Config { msg: String },
    #[error("Sync error: {msg}")]
    Sync { msg: String },
    #[error("Validation error: {msg}")]
    Validation { msg: String },
    #[error("IO error: {msg}")]
    Io { msg: String },
}

impl From<crate::error::VoiceError> for VoiceCoreError {
    fn from(err: crate::error::VoiceError) -> Self {
        match err {
            crate::error::VoiceError::Database(e) => VoiceCoreError::Database {
                msg: e.to_string(),
            },
            crate::error::VoiceError::DatabaseOperation(msg) => {
                VoiceCoreError::Database { msg }
            }
            crate::error::VoiceError::Config(msg) => VoiceCoreError::Config { msg },
            crate::error::VoiceError::Sync(msg) => VoiceCoreError::Sync { msg },
            crate::error::VoiceError::Network(msg) => VoiceCoreError::Sync { msg },
            crate::error::VoiceError::Tls(msg) => VoiceCoreError::Sync { msg },
            crate::error::VoiceError::Validation { field, message } => {
                VoiceCoreError::Validation {
                    msg: format!("{}: {}", field, message),
                }
            }
            crate::error::VoiceError::Io(e) => VoiceCoreError::Io {
                msg: e.to_string(),
            },
            crate::error::VoiceError::Json(e) => VoiceCoreError::Database {
                msg: e.to_string(),
            },
            crate::error::VoiceError::Uuid(e) => VoiceCoreError::Validation {
                msg: e.to_string(),
            },
            crate::error::VoiceError::NotFound(msg) => VoiceCoreError::Database { msg },
            crate::error::VoiceError::Conflict(msg) => VoiceCoreError::Sync { msg },
            crate::error::VoiceError::Other(msg) => VoiceCoreError::Database { msg },
        }
    }
}

/// A note from the database
#[derive(Debug, Clone, uniffi::Record)]
pub struct NoteData {
    pub id: String,
    pub content: String,
    pub created_at: Stamp,
    pub modified_at: Option<Stamp>,
    pub deleted_at: Option<Stamp>,
    /// Cache for notes list pane display (JSON with date, marked, content_preview)
    pub list_display_cache: Option<String>,
}

/// An audio file from the database
#[derive(Debug, Clone, uniffi::Record)]
pub struct AudioFileData {
    pub id: String,
    pub imported_at: Stamp,
    pub filename: String,
    pub file_created_at: Option<Stamp>,
    /// How long the recording is, where it is known. The phone needs it to
    /// decide what work is worth doing on a recording: a waveform is offered
    /// rather than drawn past an hour, and transcription on the phone is
    /// capped (see VoiceFamily/TECHNICAL-DECISIONS.md).
    pub duration_seconds: Option<i64>,
    pub summary: Option<String>,
    pub device_id: String,
    pub modified_at: Option<Stamp>,
    pub deleted_at: Option<Stamp>,
    /// Cloud storage provider ("s3", "backblaze", etc.) or None for local-only
    pub storage_provider: Option<String>,
    /// Object key/path in cloud storage
    pub storage_key: Option<String>,
    /// When the file was uploaded to cloud storage; a machine event, so it
    /// carries no timezone of its own and a reader shows it in its own.
    pub storage_uploaded_at: Option<Stamp>,
}

/// A note-attachment association from the database
#[derive(Debug, Clone, uniffi::Record)]
pub struct NoteAttachmentData {
    pub id: String,
    pub note_id: String,
    pub attachment_id: String,
    pub attachment_type: String,
    pub created_at: Stamp,
    pub device_id: String,
    pub modified_at: Option<Stamp>,
    pub deleted_at: Option<Stamp>,
}

/// A transcription from the database
#[derive(Debug, Clone, uniffi::Record)]
pub struct TranscriptionData {
    pub id: String,
    pub audio_file_id: String,
    pub content: String,
    pub content_segments: Option<String>,
    pub service: String,
    pub service_arguments: Option<String>,
    pub service_response: Option<String>,
    pub state: String,
    pub device_id: String,
    pub created_at: Stamp,
    pub modified_at: Option<Stamp>,
    pub deleted_at: Option<Stamp>,
}

/// A tag from the database
#[derive(Debug, Clone, uniffi::Record)]
pub struct TagData {
    pub id: String,
    pub name: String,
    pub parent_id: Option<String>,
    pub created_at: Option<Stamp>,
    pub modified_at: Option<Stamp>,
}

/// Result of a search operation
#[derive(Debug, Clone, uniffi::Record)]
pub struct SearchResultData {
    pub notes: Vec<NoteData>,
    pub ambiguous_tags: Vec<String>,
    pub not_found_tags: Vec<String>,
}

/// Result of a tag change operation (add/remove tag from note)
#[derive(Debug, Clone, uniffi::Record)]
pub struct TagChangeResultData {
    /// Whether the tag association was actually changed
    pub changed: bool,
    /// The note ID that was affected
    pub note_id: String,
    /// Whether the list pane cache was rebuilt
    pub list_cache_rebuilt: bool,
}

/// Sync operation result
#[derive(Debug, Clone, uniffi::Record)]
pub struct SyncResultData {
    pub success: bool,
    pub notes_received: i32,
    pub notes_sent: i32,
    pub error_message: Option<String>,
    /// Non-fatal problems, e.g. a cloud upload that will be retried next sync
    pub warnings: Vec<String>,
}

/// Configuration for sync server connection
#[derive(Debug, Clone, uniffi::Record)]
pub struct SyncServerConfig {
    pub server_url: String,
    pub server_peer_id: String,
    pub device_id: String,
    pub device_name: String,
}

/// Result of importing an audio file
#[derive(Debug, Clone, uniffi::Record)]
pub struct ImportAudioResultData {
    /// The ID of the created note
    pub note_id: String,
    /// The ID of the created audio file record
    pub audio_file_id: String,
}

/// Generate a new UUID7 device ID
#[uniffi::export]
pub fn generate_device_id() -> String {
    uuid::Uuid::now_v7().simple().to_string()
}

/// Main client for Voice operations on Android
#[derive(uniffi::Object)]
pub struct VoiceClient {
    config: Arc<Mutex<Config>>,
    db: Arc<Mutex<Database>>,
}

#[uniffi::export]
impl VoiceClient {
    /// Create a new VoiceClient with the given data directory
    #[uniffi::constructor]
    pub fn new(data_dir: String) -> Result<Arc<Self>, VoiceCoreError> {
        let data_path = PathBuf::from(&data_dir);

        // Create directory if it doesn't exist
        std::fs::create_dir_all(&data_path).map_err(|e| VoiceCoreError::Io {
            msg: format!("Failed to create data directory: {}", e),
        })?;

        // Initialize config
        let config = Config::new(Some(data_path.clone()))?;

        // Initialize database
        let db_path = data_path.join("notes.db");
        let db = Database::new(&db_path)?;

        Ok(Arc::new(Self {
            config: Arc::new(Mutex::new(config)),
            db: Arc::new(Mutex::new(db)),
        }))
    }

    /// Tell the core which timezone this phone is in, so every timestamp it
    /// writes records the clock the user is reading. Android keeps the zone in
    /// its framework, where a native library cannot see it, so the application
    /// calls this at start and whenever the phone's timezone changes.
    pub fn set_local_timezone(&self, offset_seconds: i32, zone_name: Option<String>) {
        crate::timezone::set_local_timezone(offset_seconds, zone_name);
    }

    /// Get all notes from the local database
    pub fn get_all_notes(&self) -> Result<Vec<NoteData>, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        let notes = db.get_all_notes()?;

        Ok(notes
            .into_iter()
            .map(|n| NoteData {
                id: n.id,
                content: n.content,
                created_at: stamp(n.created_at, n.created_at_offset, n.created_at_zone.clone()),
                modified_at: stamp_opt(n.modified_at, n.modified_at_offset, n.modified_at_zone.clone()),
                deleted_at: stamp_opt(n.deleted_at, n.deleted_at_offset, n.deleted_at_zone.clone()),
                list_display_cache: n.list_display_cache,
            })
            .collect())
    }

    /// Get the count of notes in the database
    pub fn get_note_count(&self) -> Result<i32, VoiceCoreError> {
        let notes = self.get_all_notes()?;
        Ok(notes.len() as i32)
    }

    /// Configure sync settings
    pub fn configure_sync(&self, sync_config: SyncServerConfig) -> Result<(), VoiceCoreError> {
        let mut cfg = self.config.lock().unwrap();

        // Update device name
        cfg.set_device_name(&sync_config.device_name)?;

        // Add or update the peer
        cfg.add_peer(
            &sync_config.server_peer_id,
            "Sync Server",
            &sync_config.server_url,
            None,
            true, // allow_update
        )?;

        // Enable sync
        cfg.set_sync_enabled(true)?;

        Ok(())
    }

    /// Check if sync is configured
    pub fn is_sync_configured(&self) -> bool {
        let cfg = self.config.lock().unwrap();
        cfg.is_sync_enabled() && !cfg.peers().is_empty()
    }

    /// Get current sync configuration
    pub fn get_sync_config(&self) -> Option<SyncServerConfig> {
        let cfg = self.config.lock().unwrap();

        if !cfg.is_sync_enabled() {
            return None;
        }

        let peers = cfg.peers();
        if peers.is_empty() {
            return None;
        }

        let peer = &peers[0];
        Some(SyncServerConfig {
            server_url: peer.peer_url.clone(),
            server_peer_id: peer.peer_id.clone(),
            device_id: cfg.device_id_hex().to_string(),
            device_name: cfg.device_name().to_string(),
        })
    }

    /// Sync with the configured server: database changes both ways, no files.
    pub fn sync(&self) -> Result<SyncResultData, VoiceCoreError> {
        // Check if sync is configured
        {
            let cfg = self.config.lock().unwrap();
            if !cfg.is_sync_enabled() {
                return Err(VoiceCoreError::Sync {
                    msg: "Sync is not enabled".to_string(),
                });
            }
            if cfg.peers().is_empty() {
                return Err(VoiceCoreError::Sync {
                    msg: "No sync peers configured".to_string(),
                });
            }
        }

        // Get peer ID
        let peer_id = {
            let cfg = self.config.lock().unwrap();
            cfg.peers()[0].peer_id.clone()
        };

        // Create sync client
        let sync_client = SyncClient::new(self.db.clone(), self.config.clone())?;

        // Run sync in tokio runtime
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| VoiceCoreError::Sync {
                msg: format!("Failed to create runtime: {}", e),
            })?;

        let result = rt.block_on(async { sync_client.sync_with_peer(&peer_id).await });

        Ok(SyncResultData {
            success: result.success,
            notes_received: result.pulled as i32,
            notes_sent: result.pushed as i32,
            error_message: if result.errors.is_empty() {
                None
            } else {
                Some(result.errors.join("; "))
            },
            warnings: result.warnings,
        })
    }

    /// Clear sync state to force a full re-sync from scratch
    ///
    /// This deletes the sync peer record, causing the next sync to start
    /// from the beginning and fetch all data fresh.
    pub fn clear_sync_state(&self) -> Result<(), VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.clear_sync_peers()?;
        Ok(())
    }

    /// Reset sync timestamps to force re-fetching all data from peers
    ///
    /// Unlike clear_sync_state, this preserves peer configuration but clears
    /// the last_sync_at timestamps, causing the next sync to fetch all data.
    pub fn reset_sync_timestamps(&self) -> Result<(), VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.reset_sync_timestamps()?;
        Ok(())
    }

    /// Perform initial sync - fetches full dataset from server
    ///
    /// Unlike sync(), this ignores timestamps and fetches all data.
    /// Use this for first-time sync or to re-fetch everything.
    pub fn initial_sync(&self) -> Result<SyncResultData, VoiceCoreError> {
        // Check if sync is configured
        {
            let cfg = self.config.lock().unwrap();
            if !cfg.is_sync_enabled() {
                return Err(VoiceCoreError::Sync {
                    msg: "Sync is not enabled".to_string(),
                });
            }
            if cfg.peers().is_empty() {
                return Err(VoiceCoreError::Sync {
                    msg: "No sync peers configured".to_string(),
                });
            }
        }

        // Get peer ID
        let peer_id = {
            let cfg = self.config.lock().unwrap();
            cfg.peers()[0].peer_id.clone()
        };

        // Create sync client
        let sync_client = SyncClient::new(self.db.clone(), self.config.clone())?;

        // Run initial sync in tokio runtime
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| VoiceCoreError::Sync {
                msg: format!("Failed to create runtime: {}", e),
            })?;

        let result = rt.block_on(async { sync_client.initial_sync(&peer_id).await });

        Ok(SyncResultData {
            success: result.success,
            notes_received: result.pulled as i32,
            notes_sent: result.pushed as i32,
            error_message: if result.errors.is_empty() {
                None
            } else {
                Some(result.errors.join("; "))
            },
            warnings: result.warnings,
        })
    }

    /// Get the device ID
    pub fn get_device_id(&self) -> String {
        let cfg = self.config.lock().unwrap();
        cfg.device_id_hex().to_string()
    }

    /// Set the device ID (for importing from another installation)
    pub fn set_device_id(&self, device_id: String) -> Result<(), VoiceCoreError> {
        // Validate the device ID format
        if device_id.len() != 32 || !device_id.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(VoiceCoreError::Validation {
                msg: "Device ID must be 32 hex characters".to_string(),
            });
        }

        let mut cfg = self.config.lock().unwrap();
        cfg.set("device_id", &device_id)?;
        Ok(())
    }

    /// Set the device name
    pub fn set_device_name(&self, name: String) -> Result<(), VoiceCoreError> {
        let mut cfg = self.config.lock().unwrap();
        cfg.set_device_name(&name)?;
        Ok(())
    }

    /// Get the device name
    pub fn get_device_name(&self) -> String {
        let cfg = self.config.lock().unwrap();
        cfg.device_name().to_string()
    }

    /// Set the audio file directory for storing downloaded audio files
    pub fn set_audiofile_directory(&self, path: String) -> Result<(), VoiceCoreError> {
        let mut cfg = self.config.lock().unwrap();
        cfg.set_audiofile_directory(&path)?;
        Ok(())
    }

    /// Get the audio file directory
    pub fn get_audiofile_directory(&self) -> Option<String> {
        let cfg = self.config.lock().unwrap();
        cfg.audiofile_directory().map(|s| s.to_string())
    }

    /// Get all attachments for a note
    pub fn get_attachments_for_note(&self, note_id: String) -> Result<Vec<NoteAttachmentData>, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        let attachments = db.get_attachments_for_note(&note_id)?;

        Ok(attachments
            .into_iter()
            .map(|a| NoteAttachmentData {
                id: a.id,
                note_id: a.note_id,
                attachment_id: a.attachment_id,
                attachment_type: a.attachment_type,
                // The link's zones are stored but not surfaced yet
                created_at: stamp(a.created_at, None, None),
                device_id: a.device_id,
                modified_at: stamp_opt(a.modified_at, None, None),
                deleted_at: stamp_opt(a.deleted_at, None, None),
            })
            .collect())
    }

    /// Get all audio files for a note (via note_attachments)
    pub fn get_audio_files_for_note(&self, note_id: String) -> Result<Vec<AudioFileData>, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        let audio_files = db.get_audio_files_for_note(&note_id)?;

        Ok(audio_files
            .into_iter()
            .map(|a| AudioFileData {
                id: a.id,
                imported_at: stamp(a.imported_at, a.imported_at_offset, a.imported_at_zone.clone()),
                filename: a.filename,
                file_created_at: stamp_opt(a.file_created_at, a.file_created_at_offset, a.file_created_at_zone.clone()),
                duration_seconds: a.duration_seconds,
                summary: a.summary,
                device_id: a.device_id,
                modified_at: stamp_opt(a.modified_at, a.modified_at_offset, a.modified_at_zone.clone()),
                deleted_at: stamp_opt(a.deleted_at, a.deleted_at_offset, a.deleted_at_zone.clone()),
                storage_provider: a.storage_provider,
                storage_key: a.storage_key,
                storage_uploaded_at: stamp_opt(a.storage_uploaded_at, None, None),
            })
            .collect())
    }

    /// Get a single audio file by ID
    pub fn get_audio_file(&self, audio_file_id: String) -> Result<Option<AudioFileData>, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        let audio_file = db.get_audio_file(&audio_file_id)?;

        Ok(audio_file.map(|a| AudioFileData {
            id: a.id,
            imported_at: stamp(a.imported_at, a.imported_at_offset, a.imported_at_zone.clone()),
            filename: a.filename,
            file_created_at: stamp_opt(a.file_created_at, a.file_created_at_offset, a.file_created_at_zone.clone()),
            duration_seconds: a.duration_seconds,
            summary: a.summary,
            device_id: a.device_id,
            modified_at: stamp_opt(a.modified_at, a.modified_at_offset, a.modified_at_zone.clone()),
            deleted_at: stamp_opt(a.deleted_at, a.deleted_at_offset, a.deleted_at_zone.clone()),
            storage_provider: a.storage_provider,
            storage_key: a.storage_key,
            storage_uploaded_at: stamp_opt(a.storage_uploaded_at, None, None),
        }))
    }

    /// Get all audio files in the database (for debugging)
    pub fn get_all_audio_files(&self) -> Result<Vec<AudioFileData>, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        let audio_files = db.get_all_audio_files()?;

        Ok(audio_files
            .into_iter()
            .filter(|a| a.deleted_at.is_none())
            .map(|a| AudioFileData {
                id: a.id,
                imported_at: stamp(a.imported_at, a.imported_at_offset, a.imported_at_zone.clone()),
                filename: a.filename,
                file_created_at: stamp_opt(a.file_created_at, a.file_created_at_offset, a.file_created_at_zone.clone()),
                duration_seconds: a.duration_seconds,
                summary: a.summary,
                device_id: a.device_id,
                modified_at: stamp_opt(a.modified_at, a.modified_at_offset, a.modified_at_zone.clone()),
                deleted_at: stamp_opt(a.deleted_at, a.deleted_at_offset, a.deleted_at_zone.clone()),
                storage_provider: a.storage_provider,
                storage_key: a.storage_key,
                storage_uploaded_at: stamp_opt(a.storage_uploaded_at, None, None),
            })
            .collect())
    }

    /// Get the file path for an audio file (if audiofile_directory is configured)
    pub fn get_audio_file_path(&self, audio_file_id: String) -> Result<Option<String>, VoiceCoreError> {
        // Get audio file to determine extension
        let audio_file = {
            let db = self.db.lock().unwrap();
            db.get_audio_file(&audio_file_id)?
        };

        let audio_file = match audio_file {
            Some(a) => a,
            None => return Ok(None),
        };

        // Get audiofile directory
        let audiofile_dir = {
            let cfg = self.config.lock().unwrap();
            cfg.audiofile_directory().map(|s| s.to_string())
        };

        let audiofile_dir = match audiofile_dir {
            Some(d) => d,
            None => return Ok(None),
        };

        let path = crate::models::audio_local_path(
            std::path::Path::new(&audiofile_dir),
            &audio_file.id,
            &audio_file.filename,
        );

        // Only return path if file exists
        if path.is_file() {
            Ok(Some(path.to_string_lossy().to_string()))
        } else {
            Ok(None)
        }
    }

    // =========================================================================
    // Cloud Storage Methods
    // =========================================================================

    /// Get audio files that need to be uploaded to cloud storage.
    ///
    /// Returns files where storage_provider is NULL (not yet uploaded).
    pub fn get_audio_files_pending_upload(&self) -> Result<Vec<AudioFileData>, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        let audio_files = db.get_audio_files_pending_upload()?;

        Ok(audio_files
            .into_iter()
            .map(|a| AudioFileData {
                id: a.id,
                imported_at: stamp(a.imported_at, a.imported_at_offset, a.imported_at_zone.clone()),
                filename: a.filename,
                file_created_at: stamp_opt(a.file_created_at, a.file_created_at_offset, a.file_created_at_zone.clone()),
                duration_seconds: a.duration_seconds,
                summary: a.summary,
                device_id: a.device_id,
                modified_at: stamp_opt(a.modified_at, a.modified_at_offset, a.modified_at_zone.clone()),
                deleted_at: stamp_opt(a.deleted_at, a.deleted_at_offset, a.deleted_at_zone.clone()),
                storage_provider: a.storage_provider,
                storage_key: a.storage_key,
                storage_uploaded_at: stamp_opt(a.storage_uploaded_at, None, None),
            })
            .collect())
    }

    /// Update an audio file's cloud storage information after successful upload.
    ///
    /// # Arguments
    /// * `audio_file_id` - The audio file ID
    /// * `storage_provider` - The storage provider name (e.g., "s3", "backblaze")
    /// * `storage_key` - The object key/path in cloud storage
    ///
    /// # Returns
    /// True if the audio file was updated, false if not found.
    pub fn update_audio_file_storage(
        &self,
        audio_file_id: String,
        storage_provider: String,
        storage_key: String,
    ) -> Result<bool, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.update_audio_file_storage(&audio_file_id, &storage_provider, &storage_key)
            .map_err(|e| VoiceCoreError::Database {
                msg: e.to_string(),
            })
    }

    /// Set how long a recording is, for a row that never had it.
    ///
    /// Calculating data that was never calculated is a repair, not an edit by
    /// the user: see Settings -> "Calculate missing data". The length is read
    /// off the file on the device that has the file.
    ///
    /// # Returns
    /// True if the row was updated, false if there is no such recording.
    pub fn update_audio_file_duration(
        &self,
        audio_file_id: String,
        duration_seconds: i64,
    ) -> Result<bool, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.update_audio_file_duration(&audio_file_id, duration_seconds)
            .map_err(|e| VoiceCoreError::Database {
                msg: e.to_string(),
            })
    }

    /// Set when a recording was made, for a row that never had it.
    ///
    /// Unix seconds. The timezone it was made in is *not* written: it cannot be
    /// read off a file, and guessing it would state something false about where
    /// the recording was made.
    ///
    /// # Returns
    /// True if the row was updated, false if there is no such recording.
    pub fn update_audio_file_created_at(
        &self,
        audio_file_id: String,
        file_created_at: i64,
    ) -> Result<bool, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.update_audio_file_created_at(&audio_file_id, file_created_at)
            .map_err(|e| VoiceCoreError::Database {
                msg: e.to_string(),
            })
    }

    /// Clear an audio file's cloud storage information.
    ///
    /// This marks the file as local-only (not uploaded to cloud).
    ///
    /// # Returns
    /// True if the audio file was updated, false if not found.
    pub fn clear_audio_file_storage(&self, audio_file_id: String) -> Result<bool, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.clear_audio_file_storage(&audio_file_id)
            .map_err(|e| VoiceCoreError::Database {
                msg: e.to_string(),
            })
    }

    /// Get the file storage configuration from the database.
    ///
    /// Returns the configuration as JSON string, or None if not configured.
    pub fn get_file_storage_config(&self) -> Result<Option<String>, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        let config = db.get_file_storage_config()
            .map_err(|e| VoiceCoreError::Database {
                msg: e.to_string(),
            })?;
        Ok(config.map(|c| c.to_string()))
    }

    /// Set the file storage configuration in the database.
    ///
    /// # Arguments
    /// * `provider` - The storage provider ("s3", "none", etc.)
    /// * `config` - Optional JSON string with provider-specific configuration
    pub fn set_file_storage_config(&self, provider: String, config: Option<String>) -> Result<(), VoiceCoreError> {
        let db = self.db.lock().unwrap();
        let config_value: Option<serde_json::Value> = config
            .map(|c| serde_json::from_str(&c))
            .transpose()
            .map_err(|e| VoiceCoreError::Database {
                msg: format!("Invalid JSON: {}", e),
            })?;
        db.set_file_storage_config(&provider, config_value.as_ref())
            .map_err(|e| VoiceCoreError::Database {
                msg: e.to_string(),
            })
    }

    /// Get the file storage provider name.
    ///
    /// Returns "none" if not configured.
    pub fn get_file_storage_provider(&self) -> Result<String, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        let config = db.get_file_storage_config_struct()
            .map_err(|e| VoiceCoreError::Database {
                msg: e.to_string(),
            })?;
        Ok(config.provider)
    }

    /// Check if file storage is enabled (provider is not "none").
    pub fn is_file_storage_enabled(&self) -> Result<bool, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        let config = db.get_file_storage_config_struct()
            .map_err(|e| VoiceCoreError::Database {
                msg: e.to_string(),
            })?;
        Ok(config.is_enabled())
    }

    /// Update a note's content
    pub fn update_note(&self, note_id: String, content: String) -> Result<bool, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.update_note(&note_id, &content).map_err(|e| VoiceCoreError::Database {
            msg: e.to_string(),
        })
    }

    /// Delete a note (soft delete - sets deleted_at timestamp)
    pub fn delete_note(&self, note_id: String) -> Result<bool, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.delete_note(&note_id).map_err(|e| VoiceCoreError::Database {
            msg: e.to_string(),
        })
    }

    /// Merge two notes into one.
    /// Returns the surviving note ID (the one with earlier created_at).
    pub fn merge_notes(
        &self,
        note_id_1: String,
        note_id_2: String,
    ) -> Result<String, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.merge_notes(&note_id_1, &note_id_2)
            .map_err(|e| VoiceCoreError::Database {
                msg: e.to_string(),
            })
    }

    /// Delete a tag (soft delete - sets deleted_at timestamp)
    pub fn delete_tag(&self, tag_id: String) -> Result<bool, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.delete_tag(&tag_id).map_err(|e| VoiceCoreError::Database {
            msg: e.to_string(),
        })
    }

    /// Create a new tag
    ///
    /// # Arguments
    /// * `name` - The tag name
    /// * `parent_id` - Optional parent tag ID (None for root-level tag)
    ///
    /// # Returns
    /// The ID of the newly created tag
    pub fn create_tag(&self, name: String, parent_id: Option<String>) -> Result<String, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.create_tag(&name, parent_id.as_deref())
            .map_err(|e| VoiceCoreError::Database {
                msg: e.to_string(),
            })
    }

    /// Rename a tag
    ///
    /// # Arguments
    /// * `tag_id` - The tag ID
    /// * `new_name` - The new name for the tag
    ///
    /// # Returns
    /// True if the tag was renamed, false if not found
    pub fn rename_tag(&self, tag_id: String, new_name: String) -> Result<bool, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.rename_tag(&tag_id, &new_name)
            .map_err(|e| VoiceCoreError::Database {
                msg: e.to_string(),
            })
    }

    /// Move a tag to a different parent (or make it a root tag)
    ///
    /// # Arguments
    /// * `tag_id` - The tag ID to move
    /// * `new_parent_id` - The new parent ID, or None to make it a root tag
    ///
    /// # Returns
    /// True if the tag was moved, false if not found
    pub fn reparent_tag(&self, tag_id: String, new_parent_id: Option<String>) -> Result<bool, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.reparent_tag(&tag_id, new_parent_id.as_deref())
            .map_err(|e| VoiceCoreError::Database {
                msg: e.to_string(),
            })
    }

    /// Check if there are local changes that haven't been synced
    pub fn has_unsynced_changes(&self) -> Result<bool, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        let cfg = self.config.lock().unwrap();

        // If sync is not enabled or no peers configured, no unsynced changes to report
        if !cfg.is_sync_enabled() || cfg.peers().is_empty() {
            return Ok(false);
        }

        // Get peer's last sync time
        let peer_id = cfg.peers().first().map(|p| p.peer_id.as_str());
        let last_sync: Option<i64> = match peer_id {
            Some(id) => db.get_peer_last_sync(id).ok().flatten(),
            None => return Ok(false),
        };

        // If we've never synced with this peer, no baseline to compare against
        let last_sync = match last_sync {
            Some(ts) => ts,
            None => return Ok(false),
        };

        // Check if there are changes since last sync
        let (changes, _) = db.get_changes_since(Some(last_sync), 1)
            .map_err(|e| VoiceCoreError::Database { msg: e.to_string() })?;

        Ok(!changes.is_empty())
    }

    /// Debug method to see sync state details
    pub fn debug_sync_state(&self) -> Result<String, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        let cfg = self.config.lock().unwrap();

        let mut info = String::new();

        info.push_str(&format!("Sync enabled: {}\n", cfg.is_sync_enabled()));
        info.push_str(&format!("Peers count: {}\n", cfg.peers().len()));

        if let Some(peer) = cfg.peers().first() {
            info.push_str(&format!("Peer ID: {}...\n", &peer.peer_id[..UUID_SHORT_LEN.min(peer.peer_id.len())]));

            if let Ok(Some(last_sync)) = db.get_peer_last_sync(&peer.peer_id) {
                info.push_str(&format!("Last sync: {} ({})\n", last_sync, crate::timezone::format_at_offset(last_sync, None)));

                // Check changes with >= (current behavior)
                if let Ok((changes, _)) = db.get_changes_since(Some(last_sync), 10) {
                    info.push_str(&format!("Changes (>=): {}\n", changes.len()));

                    // Show details of first few changes
                    for (i, change) in changes.iter().take(3).enumerate() {
                        if let Some(entity_type) = change.get("entity_type").and_then(|v| v.as_str()) {
                            if let Some(timestamp) = change.get("timestamp").and_then(|v| v.as_i64()) {
                                info.push_str(&format!("  [{}] {}: {}\n", i, entity_type, timestamp));
                            }
                        }
                    }
                }

                // Check changes with > (exclusive)
                if let Ok((changes, _)) = db.get_changes_since_exclusive(Some(last_sync), 10) {
                    info.push_str(&format!("Changes (>): {}\n", changes.len()));
                }
            } else {
                info.push_str("Last sync: None\n");
            }
        }

        Ok(info)
    }

    // =========================================================================
    // Transcription Methods
    // =========================================================================

    /// Get all transcriptions for an audio file
    pub fn get_transcriptions_for_audio_file(&self, audio_file_id: String) -> Result<Vec<TranscriptionData>, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        let transcriptions = db.get_transcriptions_for_audio_file(&audio_file_id)?;

        Ok(transcriptions
            .into_iter()
            .map(|t| TranscriptionData {
                id: t.id,
                audio_file_id: t.audio_file_id,
                content: t.content,
                content_segments: t.content_segments,
                service: t.service,
                service_arguments: t.service_arguments,
                service_response: t.service_response,
                state: t.state,
                device_id: t.device_id,
                created_at: stamp(t.created_at, t.created_at_offset, t.created_at_zone.clone()),
                modified_at: stamp_opt(t.modified_at, None, None),
                deleted_at: stamp_opt(t.deleted_at, None, None),
            })
            .collect())
    }

    /// The most recent transcriptions, newest first.
    ///
    /// What the transcription queue shows under "Completed": the work that is
    /// done, with the `service_response` that records how long the recording
    /// was and what the work cost in clock time, processor time and memory.
    ///
    /// `service` narrows it to one service (`local_whisper` is work this phone
    /// did); None returns every service.
    pub fn get_recent_transcriptions(
        &self,
        service: Option<String>,
        limit: u32,
    ) -> Result<Vec<TranscriptionData>, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        let transcriptions = db.get_recent_transcriptions(service.as_deref(), limit)?;

        Ok(transcriptions
            .into_iter()
            .map(|t| TranscriptionData {
                id: t.id,
                audio_file_id: t.audio_file_id,
                content: t.content,
                content_segments: t.content_segments,
                service: t.service,
                service_arguments: t.service_arguments,
                service_response: t.service_response,
                state: t.state,
                device_id: t.device_id,
                created_at: stamp(t.created_at, t.created_at_offset, t.created_at_zone.clone()),
                modified_at: stamp_opt(t.modified_at, None, None),
                deleted_at: stamp_opt(t.deleted_at, None, None),
            })
            .collect())
    }

    /// The notes a recording is attached to, as hex ids.
    ///
    /// A recording is normally on one note. The queue view uses this to say
    /// which note each transcription belongs to, so the user can look at it.
    pub fn get_notes_for_audio_file(&self, audio_file_id: String) -> Result<Vec<String>, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.get_notes_for_audio_file(&audio_file_id)
            .map_err(|e| VoiceCoreError::Database { msg: e.to_string() })
    }

    /// Get a single transcription by ID
    pub fn get_transcription(&self, transcription_id: String) -> Result<Option<TranscriptionData>, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        let transcription = db.get_transcription(&transcription_id)?;

        Ok(transcription.map(|t| TranscriptionData {
            id: t.id,
            audio_file_id: t.audio_file_id,
            content: t.content,
            content_segments: t.content_segments,
            service: t.service,
            service_arguments: t.service_arguments,
            service_response: t.service_response,
            state: t.state,
            device_id: t.device_id,
            created_at: stamp(t.created_at, t.created_at_offset, t.created_at_zone.clone()),
            modified_at: stamp_opt(t.modified_at, None, None),
            deleted_at: stamp_opt(t.deleted_at, None, None),
        }))
    }

    /// Update a transcription's state
    ///
    /// State is a space-separated list of tags. Tags prefixed with `!` indicate false/negation.
    /// Example: "original !verified !verbatim !cleaned !polished"
    pub fn update_transcription_state(&self, transcription_id: String, state: String) -> Result<bool, VoiceCoreError> {
        let db = self.db.lock().unwrap();

        // Get existing transcription to preserve other fields
        let existing = db.get_transcription(&transcription_id)?
            .ok_or_else(|| VoiceCoreError::Database {
                msg: format!("Transcription not found: {}", transcription_id),
            })?;

        db.update_transcription(
            &transcription_id,
            &existing.content,
            existing.content_segments.as_deref(),
            existing.service_response.as_deref(),
            Some(&state),
        ).map_err(|e| VoiceCoreError::Database { msg: e.to_string() })
    }

    /// Update a transcription's content and optionally its state
    pub fn update_transcription(
        &self,
        transcription_id: String,
        content: String,
        state: Option<String>,
    ) -> Result<bool, VoiceCoreError> {
        let db = self.db.lock().unwrap();

        // Get existing transcription to preserve other fields
        let existing = db.get_transcription(&transcription_id)?
            .ok_or_else(|| VoiceCoreError::Database {
                msg: format!("Transcription not found: {}", transcription_id),
            })?;

        db.update_transcription(
            &transcription_id,
            &content,
            existing.content_segments.as_deref(),
            existing.service_response.as_deref(),
            state.as_deref(),
        ).map_err(|e| VoiceCoreError::Database { msg: e.to_string() })
    }

    /// Create a transcription record for an audio file (used by on-device
    /// transcription: a "Pending..." row first, then the result via
    /// `update_transcription_result`). Returns the new transcription id.
    pub fn create_transcription(
        &self,
        audio_file_id: String,
        content: String,
        content_segments: Option<String>,
        service: String,
        service_arguments: Option<String>,
        service_response: Option<String>,
    ) -> Result<String, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.create_transcription(
            &audio_file_id,
            &content,
            content_segments.as_deref(),
            &service,
            service_arguments.as_deref(),
            service_response.as_deref(),
            None,
        ).map_err(|e| VoiceCoreError::Database { msg: e.to_string() })
    }

    /// Store a finished (or failed) transcription: the text, the segments
    /// JSON and the service response, exactly as the desktop does.
    pub fn update_transcription_result(
        &self,
        transcription_id: String,
        content: String,
        content_segments: Option<String>,
        service_response: Option<String>,
    ) -> Result<bool, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.update_transcription(
            &transcription_id,
            &content,
            content_segments.as_deref(),
            service_response.as_deref(),
            None,
        ).map_err(|e| VoiceCoreError::Database { msg: e.to_string() })
    }

    /// Delete a transcription (a soft delete, like every other deletion
    /// here, so the removal travels to the other devices).
    ///
    /// The phone uses this to clear the "the app was closed before the
    /// transcription finished" placeholder once the recording really has
    /// been transcribed: the failed attempt is of no interest to anybody
    /// after that, and leaving it makes the note look transcribed twice.
    pub fn delete_transcription(&self, transcription_id: String) -> Result<bool, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.delete_transcription(&transcription_id)
            .map_err(|e| VoiceCoreError::Database { msg: e.to_string() })
    }

    // =========================================================================
    // Tag and Search Methods
    // =========================================================================

    /// Get all tags from the database
    pub fn get_all_tags(&self) -> Result<Vec<TagData>, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        let tags = db.get_all_tags()?;

        Ok(tags
            .into_iter()
            .map(|t| TagData {
                id: t.id,
                name: t.name,
                parent_id: t.parent_id,
                created_at: stamp_opt(t.created_at, None, None),
                modified_at: stamp_opt(t.modified_at, None, None),
            })
            .collect())
    }

    /// Get all tags for a specific note
    pub fn get_tags_for_note(&self, note_id: String) -> Result<Vec<TagData>, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        let tags = db.get_note_tags(&note_id)?;

        Ok(tags
            .into_iter()
            .map(|t| TagData {
                id: t.id,
                name: t.name,
                parent_id: t.parent_id,
                created_at: stamp_opt(t.created_at, None, None),
                modified_at: stamp_opt(t.modified_at, None, None),
            })
            .collect())
    }

    /// Add a tag to a note
    ///
    /// Creates a note_tag association between the note and tag.
    /// Returns TagChangeResultData with changed=true if tag was added,
    /// changed=false if it already existed.
    pub fn add_tag_to_note(&self, note_id: String, tag_id: String) -> Result<TagChangeResultData, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.add_tag_to_note(&note_id, &tag_id)
            .map(|result| TagChangeResultData {
                changed: result.changed,
                note_id: result.note_id,
                list_cache_rebuilt: result.list_cache_rebuilt,
            })
            .map_err(|e| VoiceCoreError::Database {
                msg: e.to_string(),
            })
    }

    /// Remove a tag from a note
    ///
    /// Soft-deletes the note_tag association between the note and tag.
    /// Returns TagChangeResultData with changed=true if tag was removed,
    /// changed=false if the association didn't exist.
    pub fn remove_tag_from_note(&self, note_id: String, tag_id: String) -> Result<TagChangeResultData, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.remove_tag_from_note(&note_id, &tag_id)
            .map(|result| TagChangeResultData {
                changed: result.changed,
                note_id: result.note_id,
                list_cache_rebuilt: result.list_cache_rebuilt,
            })
            .map_err(|e| VoiceCoreError::Database {
                msg: e.to_string(),
            })
    }

    // =========================================================================
    // Note marking (star/bookmark) methods
    // =========================================================================

    /// Check if a note is marked (starred/bookmarked)
    pub fn is_note_marked(&self, note_id: String) -> Result<bool, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.is_note_marked(&note_id)
            .map_err(|e| VoiceCoreError::Database {
                msg: e.to_string(),
            })
    }

    /// Mark a note (add the _system/_marked tag)
    ///
    /// Returns true if the note was marked, false if already marked.
    pub fn mark_note(&self, note_id: String) -> Result<bool, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.mark_note(&note_id)
            .map_err(|e| VoiceCoreError::Database {
                msg: e.to_string(),
            })
    }

    /// Unmark a note (remove the _system/_marked tag)
    ///
    /// Returns true if the note was unmarked, false if not marked.
    pub fn unmark_note(&self, note_id: String) -> Result<bool, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.unmark_note(&note_id)
            .map_err(|e| VoiceCoreError::Database {
                msg: e.to_string(),
            })
    }

    /// Toggle a note's marked state
    ///
    /// Returns the new marked state (true if now marked, false if now unmarked).
    pub fn toggle_note_marked(&self, note_id: String) -> Result<bool, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.toggle_note_marked(&note_id)
            .map_err(|e| VoiceCoreError::Database {
                msg: e.to_string(),
            })
    }

    // =========================================================================
    // Non-synced file tagging methods
    // =========================================================================

    /// Check if a note is tagged as too-big to sync
    pub fn is_note_too_big_to_sync(&self, note_id: String) -> Result<bool, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.is_note_too_big_to_sync(&note_id)
            .map_err(|e| VoiceCoreError::Database {
                msg: e.to_string(),
            })
    }

    /// Tag a note as too-big to sync (add the _system/_nonsynced/_too-big tag)
    ///
    /// Returns true if the tag was added, false if already tagged.
    pub fn tag_note_too_big(&self, note_id: String) -> Result<bool, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.tag_note_too_big(&note_id)
            .map_err(|e| VoiceCoreError::Database {
                msg: e.to_string(),
            })
    }

    /// Remove the too-big tag from a note
    ///
    /// Returns true if the tag was removed, false if not tagged.
    pub fn untag_note_too_big(&self, note_id: String) -> Result<bool, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.untag_note_too_big(&note_id)
            .map_err(|e| VoiceCoreError::Database {
                msg: e.to_string(),
            })
    }

    // =========================================================================
    // Sync configuration methods
    // =========================================================================

    /// Get the maximum sync file size in MB
    pub fn get_max_sync_file_size_mb(&self) -> Result<u32, VoiceCoreError> {
        let cfg = self.config.lock().unwrap();
        Ok(cfg.max_sync_file_size_mb())
    }

    /// Set the maximum sync file size in MB
    pub fn set_max_sync_file_size_mb(&self, size_mb: u32) -> Result<(), VoiceCoreError> {
        let mut cfg = self.config.lock().unwrap();
        cfg.set_max_sync_file_size_mb(size_mb)
            .map_err(|e| VoiceCoreError::Config {
                msg: e.to_string(),
            })
    }

    /// Rebuild the list pane display cache for a single note
    ///
    /// The cache stores pre-computed data for the Notes List display:
    /// date, marked status, and content preview (first 200 chars).
    pub fn rebuild_note_list_cache(&self, note_id: String) -> Result<(), VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.rebuild_note_list_cache(&note_id)
            .map_err(|e| VoiceCoreError::Database {
                msg: e.to_string(),
            })
    }

    /// Rebuild the list pane display cache for all notes
    ///
    /// Returns the number of notes processed.
    pub fn rebuild_all_note_list_caches(&self) -> Result<u32, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.rebuild_all_note_list_caches()
            .map_err(|e| VoiceCoreError::Database {
                msg: e.to_string(),
            })
    }

    /// Rebuild every display cache of one note: the note pane's and the list's.
    ///
    /// Used when calculating missing data, for a note written before the caches
    /// existed.
    pub fn rebuild_all_caches_for_note(&self, note_id: String) -> Result<(), VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.rebuild_all_caches_for_note(&note_id)
            .map_err(|e| VoiceCoreError::Database {
                msg: e.to_string(),
            })
    }

    /// Get the _system tag ID as a hex string
    ///
    /// Used for filtering system tags from UI display.
    pub fn get_system_tag_id_hex(&self) -> Result<String, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.get_system_tag_id_hex()
            .map_err(|e| VoiceCoreError::Database {
                msg: e.to_string(),
            })
    }

    /// Execute a search query
    ///
    /// Supports "tag:Name" syntax for tag filtering and free text search.
    /// Multiple tags can be combined: "tag:Work tag:Important meeting notes"
    pub fn search_notes(&self, query: String) -> Result<SearchResultData, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        let result = search::execute_search(&db, &query)?;

        Ok(SearchResultData {
            notes: result
                .notes
                .into_iter()
                .map(|n| NoteData {
                    id: n.id,
                    content: n.content,
                    created_at: stamp(n.created_at, n.created_at_offset, n.created_at_zone.clone()),
                    modified_at: stamp_opt(n.modified_at, n.modified_at_offset, n.modified_at_zone.clone()),
                    deleted_at: stamp_opt(n.deleted_at, n.deleted_at_offset, n.deleted_at_zone.clone()),
                    list_display_cache: n.list_display_cache,
                })
                .collect(),
            ambiguous_tags: result.ambiguous_tags,
            not_found_tags: result.not_found_tags,
        })
    }

    /// Get the types of unresolved conflicts for a specific note.
    ///
    /// Returns a list of conflict type strings (e.g., ["content", "delete"]).
    /// Returns an empty list if the note has no unresolved conflicts.
    pub fn get_note_conflict_types(&self, note_id: String) -> Result<Vec<String>, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.get_note_conflict_types(&note_id)
            .map_err(|e| VoiceCoreError::Database { msg: e.to_string() })
    }

    /// Every unresolved conflict that concerns a note: its content, its
    /// deletion, its tag links, its attachments and their transcriptions.
    pub fn get_note_conflicts(&self, note_id: String) -> Result<Vec<ConflictData>, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        let rows = db.get_note_conflicts(&note_id)
            .map_err(|e| VoiceCoreError::Database { msg: e.to_string() })?;
        Ok(rows.into_iter().map(conflict_to_data).collect())
    }

    /// All conflicts (unresolved only unless include_resolved), newest first.
    pub fn get_conflicts(&self, include_resolved: bool) -> Result<Vec<ConflictData>, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        let rows = db.get_conflicts(include_resolved)
            .map_err(|e| VoiceCoreError::Database { msg: e.to_string() })?;
        Ok(rows.into_iter().map(conflict_to_data).collect())
    }

    /// Number of unresolved conflicts in the whole database.
    pub fn get_unresolved_conflict_count(&self) -> Result<i64, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        let counts = db.get_unresolved_conflict_counts()
            .map_err(|e| VoiceCoreError::Database { msg: e.to_string() })?;
        Ok(*counts.get("total").unwrap_or(&0))
    }

    /// Accept the merged value of a conflict as it stands. The acceptance is
    /// a new version and reaches every peer on the next sync.
    pub fn accept_conflict(&self, conflict_id: String) -> Result<bool, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.accept_conflict(&conflict_id)
            .map_err(|e| VoiceCoreError::Database { msg: e.to_string() })
    }

    /// Accept every unresolved conflict of a note. Returns how many were accepted.
    pub fn accept_note_conflicts(&self, note_id: String) -> Result<i32, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        let rows = db.get_note_conflicts(&note_id)
            .map_err(|e| VoiceCoreError::Database { msg: e.to_string() })?;
        let mut n = 0;
        for c in rows {
            if db.accept_conflict(&c.id).map_err(|e| VoiceCoreError::Database { msg: e.to_string() })? {
                n += 1;
            }
        }
        Ok(n)
    }

    /// Resolve a conflict by writing a new value for its field.
    pub fn resolve_conflict_with_content(&self, conflict_id: String, content: String) -> Result<bool, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.resolve_conflict_with_content(&conflict_id, &content)
            .map_err(|e| VoiceCoreError::Database { msg: e.to_string() })
    }

    /// Every version of one field, oldest first (e.g. "note", note_id, "content").
    pub fn get_field_history(&self, entity_type: String, entity_id: String, field: String) -> Result<Vec<VersionData>, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        let rows = db.get_field_history(&entity_type, &entity_id, &field)
            .map_err(|e| VoiceCoreError::Database { msg: e.to_string() })?;
        Ok(rows.into_iter().map(version_to_data).collect())
    }

    /// One version by hex id, or None.
    pub fn get_version(&self, version_id: String) -> Result<Option<VersionData>, VoiceCoreError> {
        let bytes = crate::versions::hex_to_bytes(&version_id)
            .map_err(|e| VoiceCoreError::Validation { msg: e.to_string() })?;
        let db = self.db.lock().unwrap();
        let row = db.get_version(&bytes)
            .map_err(|e| VoiceCoreError::Database { msg: e.to_string() })?;
        Ok(row.map(version_to_data))
    }

    /// A synced setting (shared by every device), or None.
    pub fn get_setting(&self, key: String) -> Result<Option<String>, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.get_setting(&key)
            .map_err(|e| VoiceCoreError::Database { msg: e.to_string() })
    }

    /// Set a synced setting. Concurrent changes on two devices are merged and flagged.
    pub fn set_setting(&self, key: String, value: String) -> Result<(), VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.set_setting(&key, &value)
            .map_err(|e| VoiceCoreError::Database { msg: e.to_string() })
    }

    /// All synced settings.
    pub fn get_all_settings(&self) -> Result<std::collections::HashMap<String, String>, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.get_all_settings()
            .map_err(|e| VoiceCoreError::Database { msg: e.to_string() })
    }

    /// Filter notes by tag IDs.
    ///
    /// Returns notes that have ALL the specified tags.
    pub fn filter_notes(&self, tag_ids: Vec<String>) -> Result<Vec<NoteData>, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        let notes = db.filter_notes(&tag_ids)
            .map_err(|e| VoiceCoreError::Database { msg: e.to_string() })?;

        Ok(notes
            .into_iter()
            .map(|n| NoteData {
                id: n.id,
                content: n.content,
                created_at: stamp(n.created_at, n.created_at_offset, n.created_at_zone.clone()),
                modified_at: stamp_opt(n.modified_at, n.modified_at_offset, n.modified_at_zone.clone()),
                deleted_at: stamp_opt(n.deleted_at, n.deleted_at_offset, n.deleted_at_zone.clone()),
                list_display_cache: n.list_display_cache,
            })
            .collect())
    }

    // =========================================================================
    // Audio Import Methods
    // =========================================================================

    /// Import an audio file, creating all necessary database records.
    ///
    /// This creates:
    /// 1. An AudioFile record
    /// 2. A Note record (with created_at = file_created_at if provided)
    /// 3. A NoteAttachment linking them
    ///
    /// The Note's created_at will be set to file_created_at (the file's filesystem date).
    ///
    /// # Arguments
    /// * `filename` - Original filename of the audio file
    /// * `file_created_at` - Unix timestamp of when the file was created (optional)
    /// * `duration_seconds` - Duration of the audio file in seconds (optional)
    ///
    /// # Returns
    /// ImportAudioResultData with note_id and audio_file_id
    pub fn import_audio_file(
        &self,
        filename: String,
        file_created_at: Option<i64>,
        duration_seconds: Option<i64>,
    ) -> Result<ImportAudioResultData, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        let (note_id, audio_file_id) = db
            .import_audio_file(&filename, file_created_at, duration_seconds)
            .map_err(|e| VoiceCoreError::Database {
                msg: e.to_string(),
            })?;

        Ok(ImportAudioResultData {
            note_id,
            audio_file_id,
        })
    }

    // =========================================================================
    // Which attachment or transcription stands for its parent
    // =========================================================================

    /// Make one of a note's attachments the one that stands for it: the
    /// recording played when the note is opened, and the one whose
    /// transcription the notes list shows. Null goes back to the first one.
    pub fn set_primary_attachment(&self, note_id: String, attachment_id: Option<String>) -> Result<bool, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.set_primary_attachment(&note_id, attachment_id.as_deref())
            .map_err(|e| VoiceCoreError::Database { msg: e.to_string() })
    }

    /// The attachment that stands for this note, if one was chosen.
    pub fn get_primary_attachment(&self, note_id: String) -> Result<Option<String>, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.get_primary_attachment(&note_id)
            .map_err(|e| VoiceCoreError::Database { msg: e.to_string() })
    }

    /// Make one of a recording's transcriptions the one that stands for it.
    pub fn set_primary_transcription(&self, audio_file_id: String, transcription_id: Option<String>) -> Result<bool, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.set_primary_transcription(&audio_file_id, transcription_id.as_deref())
            .map_err(|e| VoiceCoreError::Database { msg: e.to_string() })
    }

    /// The transcription that stands for this recording, if one was chosen.
    pub fn get_primary_transcription(&self, audio_file_id: String) -> Result<Option<String>, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.get_primary_transcription(&audio_file_id)
            .map_err(|e| VoiceCoreError::Database { msg: e.to_string() })
    }

    // =========================================================================
    // The trash bin
    // =========================================================================

    /// The notes in the trash: deleted, still here, newest deletion first.
    pub fn get_deleted_notes(&self) -> Result<Vec<NoteData>, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        let notes = db.get_deleted_notes()?;
        Ok(notes
            .into_iter()
            .map(|n| NoteData {
                id: n.id,
                content: n.content,
                created_at: stamp(n.created_at, n.created_at_offset, n.created_at_zone.clone()),
                modified_at: stamp_opt(n.modified_at, n.modified_at_offset, n.modified_at_zone.clone()),
                deleted_at: stamp_opt(n.deleted_at, n.deleted_at_offset, n.deleted_at_zone.clone()),
                list_display_cache: n.list_display_cache,
            })
            .collect())
    }

    /// Take a note out of the trash. False when it was not in there.
    pub fn undelete_note(&self, note_id: String) -> Result<bool, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.undelete_note(&note_id)
            .map_err(|e| VoiceCoreError::Database { msg: e.to_string() })
    }

    /// Empty one note out of the trash for good.
    ///
    /// Returns the ids of the recordings that went with it, so the app can
    /// delete the files from the phone. The removal travels to the other
    /// devices and cannot be undone.
    pub fn purge_note(&self, note_id: String) -> Result<Vec<String>, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.purge_note(&note_id)
            .map_err(|e| VoiceCoreError::Database { msg: e.to_string() })
    }

    /// Import a recording into a note that already exists.
    ///
    /// The phone records inside the note now, so the note is there before
    /// the recording is: pressing Save attaches the file to that note rather
    /// than making a second one. Returns the new audio file id, which is
    /// also the name the file is stored under.
    pub fn import_audio_file_into_note(
        &self,
        note_id: String,
        filename: String,
        file_created_at: Option<i64>,
        duration_seconds: Option<i64>,
    ) -> Result<String, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.import_audio_file_into_note(&note_id, &filename, file_created_at, duration_seconds)
            .map_err(|e| VoiceCoreError::Database { msg: e.to_string() })
    }

    /// Create a new note with empty content
    ///
    /// Returns the ID of the created note as a hex string.
    pub fn create_note(&self, content: String) -> Result<String, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.create_note(&content)
            .map_err(|e| VoiceCoreError::Database {
                msg: e.to_string(),
            })
    }

    // =========================================================================
    // Cloud Storage Download Methods
    // =========================================================================

    /// Download one audio file from cloud storage, on demand.
    ///
    /// Returns a result with `downloaded = 1` when the file was fetched,
    /// `already_local = 1` when nothing was needed, or `not_in_cloud = 1`
    /// when the owning device has not uploaded the file yet. Errors (offline,
    /// storage not configured yet, object missing) are returned as an
    /// exception so the UI can show them.
    pub fn download_audio_file(&self, audio_file_id: String) -> Result<DownloadResultData, VoiceCoreError> {
        #[cfg(feature = "file-storage")]
        {
            use crate::file_storage::DownloadOutcome;
            let (client, dir, rt) = self.cloud_context()?;
            let outcome = rt.block_on(client.download_audio_file_from_cloud(&dir, &audio_file_id));
            match outcome {
                Ok(DownloadOutcome::Downloaded(_)) => Ok(DownloadResultData { downloaded: 1, ..Default::default() }),
                Ok(DownloadOutcome::AlreadyLocal) => Ok(DownloadResultData { already_local: 1, ..Default::default() }),
                Ok(DownloadOutcome::NotInCloud) => Ok(DownloadResultData { not_in_cloud: 1, ..Default::default() }),
                Err(e) => Err(VoiceCoreError::Sync { msg: e.to_string() }),
            }
        }

        #[cfg(not(feature = "file-storage"))]
        {
            let _ = audio_file_id;
            Err(VoiceCoreError::Config { msg: "File storage feature not enabled".to_string() })
        }
    }

    /// Download every audio file attached to a note that is in cloud storage
    /// but not on this device. This is the "media missing, download" action.
    pub fn download_audio_files_for_note(&self, note_id: String) -> Result<DownloadResultData, VoiceCoreError> {
        #[cfg(feature = "file-storage")]
        {
            let (client, dir, rt) = self.cloud_context()?;
            let result = rt.block_on(client.download_audio_files_for_note_from_cloud(&dir, &note_id));
            match result {
                Ok(r) => Ok(DownloadResultData::from(r)),
                Err(e) => Err(VoiceCoreError::Sync { msg: e.to_string() }),
            }
        }

        #[cfg(not(feature = "file-storage"))]
        {
            let _ = note_id;
            Err(VoiceCoreError::Config { msg: "File storage feature not enabled".to_string() })
        }
    }

    /// Upload every recording whose row says the bucket does not hold it yet.
    /// Runs only when the user asks; a sync never uploads.
    pub fn upload(&self) -> Result<UploadResultData, VoiceCoreError> {
        #[cfg(feature = "file-storage")]
        {
            let dir = {
                let cfg = self.config.lock().unwrap();
                cfg.audiofile_directory().map(std::path::PathBuf::from)
            };
            let dir = dir.ok_or_else(|| VoiceCoreError::Sync {
                msg: "No audio directory is configured".to_string(),
            })?;
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| VoiceCoreError::Sync { msg: format!("Failed to create runtime: {}", e) })?;
            let db = self.db.lock().unwrap();
            let result = rt.block_on(crate::file_storage::upload_pending_audio_files(&db, &dir));
            match result {
                Ok(r) => Ok(UploadResultData {
                    uploaded: r.uploaded as i32,
                    skipped: r.skipped as i32,
                    failed: r.failed as i32,
                    deferred: r.deferred as i32,
                    errors: r.errors,
                }),
                Err(e) => Err(VoiceCoreError::Sync { msg: e.to_string() }),
            }
        }

        #[cfg(not(feature = "file-storage"))]
        {
            Ok(UploadResultData {
                errors: vec!["File storage feature not enabled".to_string()],
                ..Default::default()
            })
        }
    }

    /// Download every non-deleted audio file that is in cloud storage but not
    /// on this device. Intended for explicit "fetch everything" actions; sync
    /// itself never does this on Android.
    pub fn download_missing_audio_files(&self) -> Result<DownloadResultData, VoiceCoreError> {
        #[cfg(feature = "file-storage")]
        {
            let (client, dir, rt) = self.cloud_context()?;
            let result = rt.block_on(client.download_missing_audio_files_from_cloud(&dir));
            match result {
                Ok(r) => Ok(DownloadResultData::from(r)),
                Err(e) => Err(VoiceCoreError::Sync { msg: e.to_string() }),
            }
        }

        #[cfg(not(feature = "file-storage"))]
        {
            Ok(DownloadResultData {
                errors: vec!["File storage feature not enabled".to_string()],
                ..Default::default()
            })
        }
    }

    /// Check if an audio file exists locally.
    ///
    /// Returns true if the file exists in the audiofile directory.
    pub fn audio_file_exists_locally(&self, audio_file_id: String) -> Result<bool, VoiceCoreError> {
        // Get audio file to determine extension
        let audio_file = {
            let db = self.db.lock().unwrap();
            db.get_audio_file(&audio_file_id)?
        };

        let audio_file = match audio_file {
            Some(a) => a,
            None => return Ok(false),
        };

        // Get audiofile directory
        let audiofile_dir = {
            let cfg = self.config.lock().unwrap();
            cfg.audiofile_directory().map(|s| s.to_string())
        };

        let audiofile_dir = match audiofile_dir {
            Some(d) => d,
            None => return Ok(false),
        };

        let path = crate::models::audio_local_path(
            std::path::Path::new(&audiofile_dir),
            &audio_file.id,
            &audio_file.filename,
        );
        Ok(path.is_file())
    }

    /// Check if an audio file is available in cloud storage.
    ///
    /// Returns true if storage_provider and storage_key are set.
    pub fn audio_file_in_cloud(&self, audio_file_id: String) -> Result<bool, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        let audio_file = db.get_audio_file(&audio_file_id)?;

        Ok(audio_file
            .map(|a| a.storage_provider.is_some() && a.storage_key.is_some())
            .unwrap_or(false))
    }
}

/// Private helpers (not exported through UniFFI).
impl VoiceClient {
    /// Resolve the audio file directory or return a Config error.
    fn require_audiofile_directory(&self) -> Result<std::path::PathBuf, VoiceCoreError> {
        let cfg = self.config.lock().unwrap();
        cfg.audiofile_directory()
            .map(std::path::PathBuf::from)
            .ok_or_else(|| VoiceCoreError::Config {
                msg: "Audio file directory not configured".to_string(),
            })
    }

    /// Everything a cloud storage operation needs: a sync client, the audio
    /// directory, and a single-threaded runtime to block on.
    #[cfg(feature = "file-storage")]
    fn cloud_context(
        &self,
    ) -> Result<(SyncClient, std::path::PathBuf, tokio::runtime::Runtime), VoiceCoreError> {
        let audiofile_dir = self.require_audiofile_directory()?;
        let sync_client = SyncClient::new(self.db.clone(), self.config.clone())?;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| VoiceCoreError::Io {
                msg: format!("Failed to create runtime: {}", e),
            })?;
        Ok((sync_client, audiofile_dir, rt))
    }
}

/// One immutable version of a field (see versions.rs).
#[derive(Debug, Clone, uniffi::Record)]
pub struct VersionData {
    pub id: String,
    pub entity_type: String,
    pub entity_id: String,
    pub field: String,
    pub parent_id: Option<String>,
    pub merge_parent_id: Option<String>,
    pub content: String,
    /// "text", "scalar", ... when this merge needed a human
    pub conflict_kind: Option<String>,
    pub device_id: Option<String>,
    pub device_name: Option<String>,
    /// Label for lists: device name, short id, or "merge"
    pub device_label: String,
    pub created_at: Stamp,
}

fn version_to_data(v: crate::versions::VersionRow) -> VersionData {
    let device_label = match (&v.device_name, &v.device_id) {
        (Some(n), _) if !n.is_empty() => n.clone(),
        (_, Some(i)) if !i.is_empty() => i.chars().take(8).collect(),
        _ if v.merge_parent_id.is_some() => "merge".to_string(),
        _ => "original".to_string(),
    };
    VersionData {
        id: v.id_hex(),
        entity_type: v.entity_type,
        entity_id: v.entity_id,
        field: v.field,
        parent_id: v.parent_id.as_deref().map(crate::versions::hex),
        merge_parent_id: v.merge_parent_id.as_deref().map(crate::versions::hex),
        content: v.content,
        conflict_kind: v.conflict_kind,
        device_id: v.device_id,
        device_name: v.device_name,
        device_label,
        created_at: stamp(v.created_at, v.created_at_offset, v.created_at_zone.clone()),
    }
}

/// A recorded disagreement between two versions of one field.
///
/// `kind` is one of "text", "scalar", "flags", "membership", "delete";
/// `display_kind` is what the user sees ("content", "delete", "tag",
/// "attachment", or the field name). Device labels fall back to a short id.
#[derive(Debug, Clone, uniffi::Record)]
pub struct ConflictData {
    pub id: String,
    pub entity_type: String,
    pub entity_id: String,
    pub field: String,
    pub kind: String,
    pub display_kind: String,
    /// One line: what disagreed and which devices did it
    pub description: String,
    pub device_a: String,
    pub device_b: String,
    pub base_version_id: Option<String>,
    pub version_a_id: String,
    pub version_b_id: String,
    pub merge_version_id: String,
    /// A conflict is noticed by the machine, so it carries no timezone of its
    /// own and a reader shows it in its own.
    pub created_at: Stamp,
    pub resolved_at: Option<Stamp>,
}

fn device_label(name: &Option<String>, id: &Option<String>) -> String {
    match (name, id) {
        (Some(n), _) if !n.is_empty() => n.clone(),
        (_, Some(i)) if !i.is_empty() => i.chars().take(8).collect(),
        _ => "unknown device".to_string(),
    }
}

fn conflict_to_data(c: crate::versions::ConflictRow) -> ConflictData {
    let entity = match c.entity_type.as_str() {
        "note" => "Note",
        "tag" => "Tag",
        "note_tag" => "Note tag",
        "note_attachment" => "Attachment",
        "transcription" => "Transcription",
        "audio_file" => "Audio file",
        "setting" => "Setting",
        other => other,
    };
    let display_kind = match (c.kind.as_str(), c.entity_type.as_str()) {
        ("delete", _) => "delete".to_string(),
        (_, "note_tag") => "tag".to_string(),
        (_, "note_attachment") => "attachment".to_string(),
        ("text", _) => "content".to_string(),
        _ => c.field.clone(),
    };
    let what = match c.kind.as_str() {
        "text" => format!("{} {} edited on both", entity, c.field),
        "delete" => format!("{} deleted on one, changed on the other", entity),
        "membership" => format!("{} removed on one, kept on the other", entity),
        _ => format!("{} {} changed on both", entity, c.field),
    };
    let device_a = device_label(&c.device_a_name, &c.device_a_id);
    let device_b = device_label(&c.device_b_name, &c.device_b_id);
    ConflictData {
        description: format!("{}: {} vs {}", what, device_a, device_b),
        id: c.id,
        entity_type: c.entity_type,
        entity_id: c.entity_id,
        field: c.field,
        kind: c.kind,
        display_kind,
        device_a,
        device_b,
        base_version_id: c.base_version_id,
        version_a_id: c.version_a_id,
        version_b_id: c.version_b_id,
        merge_version_id: c.merge_version_id,
        created_at: stamp(c.created_at, None, None),
        resolved_at: stamp_opt(c.resolved_at, None, None),
    }
}

/// Result of uploading recordings to the bucket
#[derive(Debug, Clone, Default, uniffi::Record)]
pub struct UploadResultData {
    /// Files uploaded in this run
    pub uploaded: i32,
    /// Pending rows whose file is not on this device (another device owns them)
    pub skipped: i32,
    /// Files that failed to upload
    pub failed: i32,
    /// Files not attempted because an earlier failure stopped the batch
    pub deferred: i32,
    /// One message per failure
    pub errors: Vec<String>,
}

/// Result of downloading audio files from cloud storage
#[derive(Debug, Clone, Default, uniffi::Record)]
pub struct DownloadResultData {
    /// Number of files successfully downloaded and verified
    pub downloaded: i32,
    /// Number of files that were already on this device
    pub already_local: i32,
    /// Number of files whose owning device has not uploaded them yet
    pub not_in_cloud: i32,
    /// Number of downloads that failed
    pub failed: i32,
    /// Error messages for any failed downloads
    pub errors: Vec<String>,
}

#[cfg(feature = "file-storage")]
impl From<crate::file_storage::DownloadMissingResult> for DownloadResultData {
    fn from(r: crate::file_storage::DownloadMissingResult) -> Self {
        Self {
            downloaded: r.downloaded as i32,
            already_local: r.already_local as i32,
            not_in_cloud: r.not_in_cloud as i32,
            failed: r.failed as i32,
            errors: r.errors,
        }
    }
}

#[cfg(test)]
mod transcription_binding_tests {
    use super::*;

    /// A phone's on-device transcription writes a pending row and then the
    /// Hebrew result with segments and the service response.
    #[test]
    fn create_then_update_transcription_result() {
        let dir = tempfile::tempdir().unwrap();
        let client = VoiceClient::new(dir.path().to_string_lossy().to_string()).unwrap();
        let imported = client
            .import_audio_file("Recording 2026-09-08 02-00-00.ogg".into(), Some(1_757_000_000), Some(4))
            .unwrap();

        let id = client
            .create_transcription(
                imported.audio_file_id.clone(),
                "Pending... (2026-09-08 02:00:00)".into(),
                None,
                "local_whisper".into(),
                Some(r#"{"provider_id":"local_whisper","model":"large-v3-q5_0","language":"he","beam_size":5,"device":"android"}"#.into()),
                None,
            )
            .unwrap();
        let pending = client.get_transcription(id.clone()).unwrap().unwrap();
        assert_eq!(pending.service, "local_whisper");
        assert!(pending.content.starts_with("Pending..."));
        assert!(pending.content_segments.is_none());

        let segments = r#"[{"text":"זוהי הקלטה מהטלפון","start_seconds":0.0,"end_seconds":2.5,"speaker":null,"confidence":null}]"#;
        let response = r#"{"elapsed_time":12.5,"segment_count":1,"model":"large-v3-q5_0","device":"android"}"#;
        assert!(client
            .update_transcription_result(id.clone(), "זוהי הקלטה מהטלפון".into(), Some(segments.into()), Some(response.into()))
            .unwrap());

        let done = client.get_transcription(id.clone()).unwrap().unwrap();
        assert_eq!(done.content, "זוהי הקלטה מהטלפון");
        assert_eq!(done.content_segments.as_deref(), Some(segments));
        assert_eq!(done.service_response.as_deref(), Some(response));
        assert_eq!(done.service_arguments.as_deref().map(|s| s.contains("\"language\":\"he\"")), Some(true));

        // A failed run keeps the segments empty and records the error
        assert!(client
            .update_transcription_result(id.clone(), "Error: המודל לא נטען".into(), None, Some(r#"{"error":"המודל לא נטען"}"#.into()))
            .unwrap());
        let failed = client.get_transcription(id).unwrap().unwrap();
        assert!(failed.content.starts_with("Error: "));
        // None leaves the earlier segments in place (same rule as the desktop)
        assert_eq!(failed.content_segments.as_deref(), Some(segments));

        let all = client.get_transcriptions_for_audio_file(imported.audio_file_id).unwrap();
        assert_eq!(all.len(), 1);
    }

    /// A recording made inside a note joins that note, and no other note is
    /// created. This is the phone's recorder: the note exists before the
    /// recording does, so the file has somewhere to go the moment the user
    /// presses Save.
    #[test]
    fn a_recording_made_in_a_note_joins_that_note() {
        let dir = tempfile::tempdir().unwrap();
        let client = VoiceClient::new(dir.path().to_string_lossy().to_string()).unwrap();
        let note_id = client.create_note("הערה עם הקלטה".into()).unwrap();
        let notes_before = client.get_all_notes().unwrap().len();

        let audio_id = client
            .import_audio_file_into_note(
                note_id.clone(),
                "Recording 2026-09-09 04-30-00.ogg".into(),
                Some(1_757_000_000),
                Some(12),
            )
            .unwrap();

        // No second note: the recording joined the one the user was in.
        assert_eq!(client.get_all_notes().unwrap().len(), notes_before);
        let files = client.get_audio_files_for_note(note_id.clone()).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].id, audio_id);
        assert_eq!(files[0].filename, "Recording 2026-09-09 04-30-00.ogg");
        assert_eq!(files[0].file_created_at.as_ref().map(|s| s.at), Some(1_757_000_000));

        // A second recording in the same note is added, not swapped in.
        client
            .import_audio_file_into_note(note_id.clone(), "Recording 2026-09-09 04-31-00.ogg".into(), None, Some(3))
            .unwrap();
        assert_eq!(client.get_audio_files_for_note(note_id).unwrap().len(), 2);
    }

    /// A note that is not there is said to be not there, rather than the
    /// recording being filed somewhere the user will never find it.
    #[test]
    fn a_recording_for_a_missing_note_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let client = VoiceClient::new(dir.path().to_string_lossy().to_string()).unwrap();
        let missing = "00000000000040008000000000000099".to_string();

        let result = client.import_audio_file_into_note(missing, "Recording.ogg".into(), None, Some(1));

        assert!(result.is_err(), "a recording must not be attached to a note that does not exist");
        // And nothing was left behind for it.
        assert!(client.get_all_notes().unwrap().is_empty());
    }

    /// The phone writes a placeholder when a transcription is interrupted and
    /// removes it once the recording has really been transcribed. Deleting is
    /// a soft delete, so the recording keeps only the good transcription and
    /// the removal travels to the other devices.
    #[test]
    fn a_deleted_transcription_leaves_only_the_good_one() {
        let dir = tempfile::tempdir().unwrap();
        let client = VoiceClient::new(dir.path().to_string_lossy().to_string()).unwrap();
        let imported = client
            .import_audio_file("Recording 2026-09-08 03-00-00.ogg".into(), Some(1_757_003_600), Some(9))
            .unwrap();

        let interrupted = client
            .create_transcription(
                imported.audio_file_id.clone(),
                "Error: the app was closed before the transcription finished".into(),
                None,
                "local_whisper".into(),
                None,
                None,
            )
            .unwrap();
        let good = client
            .create_transcription(
                imported.audio_file_id.clone(),
                "שלום, זו ההקלטה השנייה".into(),
                None,
                "local_whisper".into(),
                None,
                None,
            )
            .unwrap();
        assert_eq!(
            client.get_transcriptions_for_audio_file(imported.audio_file_id.clone()).unwrap().len(),
            2
        );

        assert!(client.delete_transcription(interrupted.clone()).unwrap());
        let left = client.get_transcriptions_for_audio_file(imported.audio_file_id).unwrap();
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].id, good);
        assert_eq!(left[0].content, "שלום, זו ההקלטה השנייה");

        // Deleting again says so rather than pretending to have done it.
        assert!(!client.delete_transcription(interrupted).unwrap());
    }
}
