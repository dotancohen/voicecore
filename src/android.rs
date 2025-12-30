//! Android-specific bindings for VoiceCore.
//!
//! This module provides a simplified API for the Android application,
//! exposed via UniFFI bindings.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::config::Config;
use crate::database::Database;
use crate::sync_client::SyncClient;

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
                created_at: n.created_at,
                modified_at: n.modified_at,
                deleted_at: n.deleted_at,
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
                created_at: a.created_at,
                device_id: a.device_id,
                modified_at: a.modified_at,
                deleted_at: a.deleted_at,
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
                imported_at: a.imported_at,
                filename: a.filename,
                file_created_at: a.file_created_at,
                summary: a.summary,
                device_id: a.device_id,
                modified_at: a.modified_at,
                deleted_at: a.deleted_at,
            })
            .collect())
    }

    /// Get a single audio file by ID
    pub fn get_audio_file(&self, audio_file_id: String) -> Result<Option<AudioFileData>, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        let audio_file = db.get_audio_file(&audio_file_id)?;

        Ok(audio_file.map(|a| AudioFileData {
            id: a.id,
            imported_at: a.imported_at,
            filename: a.filename,
            file_created_at: a.file_created_at,
            summary: a.summary,
            device_id: a.device_id,
            modified_at: a.modified_at,
            deleted_at: a.deleted_at,
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
                imported_at: a.imported_at,
                filename: a.filename,
                file_created_at: a.file_created_at,
                summary: a.summary,
                device_id: a.device_id,
                modified_at: a.modified_at,
                deleted_at: a.deleted_at,
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

    /// Update a note's content
    pub fn update_note(&self, note_id: String, content: String) -> Result<bool, VoiceCoreError> {
        let db = self.db.lock().unwrap();
        db.update_note(&note_id, &content).map_err(|e| VoiceCoreError::Database {
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
        let last_sync = match peer_id {
            Some(id) => db.get_peer_last_sync(id).ok().flatten(),
            None => return Ok(false),
        };

        // If we've never synced with this peer, no baseline to compare against
        let last_sync = match last_sync {
            Some(ts) => ts,
            None => return Ok(false),
        };

        // Check if there are changes since last sync
        let (changes, _) = db.get_changes_since(Some(&last_sync), 1)
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
            info.push_str(&format!("Peer ID: {}...\n", &peer.peer_id[..8]));

            if let Ok(Some(last_sync)) = db.get_peer_last_sync(&peer.peer_id) {
                info.push_str(&format!("Last sync: {}\n", last_sync));

                // Check changes with >= (current behavior)
                if let Ok((changes, _)) = db.get_changes_since(Some(&last_sync), 10) {
                    info.push_str(&format!("Changes (>=): {}\n", changes.len()));

                    // Show details of first few changes
                    for (i, change) in changes.iter().take(3).enumerate() {
                        if let Some(entity_type) = change.get("entity_type").and_then(|v| v.as_str()) {
                            if let Some(timestamp) = change.get("timestamp").and_then(|v| v.as_str()) {
                                info.push_str(&format!("  [{}] {}: {}\n", i, entity_type, timestamp));
                            }
                        }
                    }
                }

                // Check changes with > (exclusive)
                if let Ok((changes, _)) = db.get_changes_since_exclusive(Some(&last_sync), 10) {
                    info.push_str(&format!("Changes (>): {}\n", changes.len()));
                }
            } else {
                info.push_str("Last sync: None\n");
            }
        }

        Ok(info)
    }
}
