//! Android-specific bindings for Voice Core.
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
}
