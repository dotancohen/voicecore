//! Android-specific bindings for VoiceCore.
//!
//! This module provides a simplified API for the Android application,
//! exposed via UniFFI bindings.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use chrono::DateTime;

use crate::config::Config;
use crate::database::Database;
use crate::search;
use crate::sync_client::SyncClient;
use crate::UUID_SHORT_LEN;

/// Format a Unix timestamp (i64) to "YYYY-MM-DD HH:MM:SS" string for display.
fn format_timestamp(ts: i64) -> String {
    DateTime::from_timestamp(ts, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| "Unknown".to_string())
}

/// Format an optional Unix timestamp to an optional string.
fn format_timestamp_opt(ts: Option<i64>) -> Option<String> {
    ts.map(format_timestamp)
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
    pub created_at: String,
    pub modified_at: Option<String>,
    pub deleted_at: Option<String>,
    /// Cache for notes list pane display (JSON with date, marked, content_preview)
    pub list_display_cache: Option<String>,
}

/// An audio file from the database
#[derive(Debug, Clone, uniffi::Record)]
pub struct AudioFileData {
    pub id: String,
    pub imported_at: String,
    pub filename: String,
    pub file_created_at: Option<String>,
    pub summary: Option<String>,
    pub device_id: String,
    pub modified_at: Option<String>,
    pub deleted_at: Option<String>,
    /// Cloud storage provider ("s3", "backblaze", etc.) or None for local-only
    pub storage_provider: Option<String>,
    /// Object key/path in cloud storage
    pub storage_key: Option<String>,
    /// Unix timestamp when file was uploaded to cloud storage (as formatted string)
    pub storage_uploaded_at: Option<String>,
}

/// A note-attachment association from the database
#[derive(Debug, Clone, uniffi::Record)]
pub struct NoteAttachmentData {
    pub id: String,
    pub note_id: String,
    pub attachment_id: String,
    pub attachment_type: String,
    pub created_at: String,
    pub device_id: String,
    pub modified_at: Option<String>,
    pub deleted_at: Option<String>,
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
    pub created_at: String,
    pub modified_at: Option<String>,
    pub deleted_at: Option<String>,
}

/// A tag from the database
#[derive(Debug, Clone, uniffi::Record)]
pub struct TagData {
    pub id: String,
    pub name: String,
    pub parent_id: Option<String>,
    pub created_at: Option<String>,
    pub modified_at: Option<String>,
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
    data_dir: PathBuf,
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
            data_dir: data_path,
        }))
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
                created_at: format_timestamp(n.created_at),
                modified_at: format_timestamp_opt(n.modified_at),
                deleted_at: format_timestamp_opt(n.deleted_at),
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

    /// Perform sync with the configured server
    pub fn sync_now(&self) -> Result<SyncResultData, VoiceCoreError> {
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
    /// Unlike sync_now(), this ignores timestamps and fetches all data.
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
                created_at: format_timestamp(a.created_at),
                device_id: a.device_id,
                modified_at: format_timestamp_opt(a.modified_at),
                deleted_at: format_timestamp_opt(a.deleted_at),
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
                imported_at: format_timestamp(a.imported_at),
                filename: a.filename,
                file_created_at: format_timestamp_opt(a.file_created_at),
                summary: a.summary,
                device_id: a.device_id,
                modified_at: format_timestamp_opt(a.modified_at),
                deleted_at: format_timestamp_opt(a.deleted_at),
                storage_provider: a.storage_provider,
                storage_key: a.storage_key,
                storage_uploaded_at: format_timestamp_opt(a.storage_uploaded_at),
            })
            .collect())
    }

    /// Get a single audio file by ID
    pub fn get_audio_file(&self, audio_file_id: String) -> Result<Option<AudioFileData>, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        let audio_file = db.get_audio_file(&audio_file_id)?;

        Ok(audio_file.map(|a| AudioFileData {
            id: a.id,
            imported_at: format_timestamp(a.imported_at),
            filename: a.filename,
            file_created_at: format_timestamp_opt(a.file_created_at),
            summary: a.summary,
            device_id: a.device_id,
            modified_at: format_timestamp_opt(a.modified_at),
            deleted_at: format_timestamp_opt(a.deleted_at),
            storage_provider: a.storage_provider,
            storage_key: a.storage_key,
            storage_uploaded_at: format_timestamp_opt(a.storage_uploaded_at),
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
                imported_at: format_timestamp(a.imported_at),
                filename: a.filename,
                file_created_at: format_timestamp_opt(a.file_created_at),
                summary: a.summary,
                device_id: a.device_id,
                modified_at: format_timestamp_opt(a.modified_at),
                deleted_at: format_timestamp_opt(a.deleted_at),
                storage_provider: a.storage_provider,
                storage_key: a.storage_key,
                storage_uploaded_at: format_timestamp_opt(a.storage_uploaded_at),
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

        // Determine extension from filename
        let ext = audio_file
            .filename
            .rsplit('.')
            .next()
            .unwrap_or("bin");

        let path = std::path::Path::new(&audiofile_dir).join(format!("{}.{}", audio_file_id, ext));

        // Only return path if file exists
        if path.exists() {
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
                imported_at: format_timestamp(a.imported_at),
                filename: a.filename,
                file_created_at: format_timestamp_opt(a.file_created_at),
                summary: a.summary,
                device_id: a.device_id,
                modified_at: format_timestamp_opt(a.modified_at),
                deleted_at: format_timestamp_opt(a.deleted_at),
                storage_provider: a.storage_provider,
                storage_key: a.storage_key,
                storage_uploaded_at: format_timestamp_opt(a.storage_uploaded_at),
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
                info.push_str(&format!("Last sync: {} ({})\n", last_sync, format_timestamp(last_sync)));

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
                created_at: format_timestamp(t.created_at),
                modified_at: format_timestamp_opt(t.modified_at),
                deleted_at: format_timestamp_opt(t.deleted_at),
            })
            .collect())
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
            created_at: format_timestamp(t.created_at),
            modified_at: format_timestamp_opt(t.modified_at),
            deleted_at: format_timestamp_opt(t.deleted_at),
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
                created_at: format_timestamp_opt(t.created_at),
                modified_at: format_timestamp_opt(t.modified_at),
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
                created_at: format_timestamp_opt(t.created_at),
                modified_at: format_timestamp_opt(t.modified_at),
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
    /// date, marked status, and content preview (first 100 chars).
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
                    created_at: format_timestamp(n.created_at),
                    modified_at: format_timestamp_opt(n.modified_at),
                    deleted_at: format_timestamp_opt(n.deleted_at),
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
                created_at: format_timestamp(n.created_at),
                modified_at: format_timestamp_opt(n.modified_at),
                deleted_at: format_timestamp_opt(n.deleted_at),
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
}
