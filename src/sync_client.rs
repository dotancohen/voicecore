//! Sync client for Voice peer-to-peer synchronization.
//!
//! This module provides the client side of the sync protocol, allowing
//! this device to:
//! - Connect to peer sync servers
//! - Pull changes from peers
//! - Push local changes to peers
//! - Handle TOFU certificate verification

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::config::Config;
use crate::database::Database;
use crate::error::{VoiceError, VoiceResult};
use crate::models::SyncChange;
use crate::UUID_SHORT_LEN;

/// Result of a sync operation
#[derive(Debug, Clone, Default)]
pub struct SyncResult {
    pub success: bool,
    pub pulled: i64,
    pub pushed: i64,
    pub conflicts: i64,
    pub errors: Vec<String>,
}

impl SyncResult {
    pub fn success() -> Self {
        Self {
            success: true,
            ..Default::default()
        }
    }

    pub fn failure(error: impl Into<String>) -> Self {
        Self {
            success: false,
            errors: vec![error.into()],
            ..Default::default()
        }
    }
}

/// Information about a sync peer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub peer_id: String,
    pub peer_name: String,
    pub peer_url: String,
    pub certificate_fingerprint: Option<String>,
    pub last_sync_at: Option<String>,
}

/// Handshake request
#[derive(Debug, Serialize)]
struct HandshakeRequest {
    device_id: String,
    device_name: String,
    protocol_version: String,
}

/// Handshake response
#[derive(Debug, Deserialize)]
struct HandshakeResponse {
    device_id: String,
    device_name: String,
    protocol_version: String,
    last_sync_timestamp: Option<i64>,
    server_timestamp: Option<i64>,
    #[serde(default)]
    supports_audiofiles: bool,
}

/// Sync batch response
#[derive(Debug, Deserialize)]
struct SyncBatchResponse {
    changes: Vec<SyncChange>,
    from_timestamp: Option<i64>,
    to_timestamp: Option<i64>,
    device_id: String,
    device_name: Option<String>,
    is_complete: bool,
}

/// Apply request
#[derive(Debug, Serialize)]
struct ApplyRequest {
    device_id: String,
    device_name: String,
    changes: Vec<SyncChange>,
}

/// Apply response
#[derive(Debug, Deserialize)]
struct ApplyResponse {
    applied: i64,
    conflicts: i64,
    errors: Vec<String>,
}

/// Full sync response (complete dataset from peer)
#[derive(Debug, Deserialize)]
struct FullSyncResponse {
    notes: Vec<serde_json::Value>,
    tags: Vec<serde_json::Value>,
    note_tags: Vec<serde_json::Value>,
    audio_files: Option<Vec<serde_json::Value>>,
    note_attachments: Option<Vec<serde_json::Value>>,
    transcriptions: Option<Vec<serde_json::Value>>,
    device_id: String,
    device_name: Option<String>,
    timestamp: i64,
}

/// Sync client
pub struct SyncClient {
    db: Arc<Mutex<Database>>,
    config: Arc<Mutex<Config>>,
    client: Client,
    device_id: String,
    device_name: String,
}

impl SyncClient {
    /// Create a new sync client
    pub fn new(db: Arc<Mutex<Database>>, config: Arc<Mutex<Config>>) -> VoiceResult<Self> {
        let (device_id, device_name) = {
            let cfg = config.lock().unwrap();
            (cfg.device_id_hex().to_string(), cfg.device_name().to_string())
        };

        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .danger_accept_invalid_certs(true) // For TOFU - we verify fingerprints manually
            .build()
            .map_err(|e| VoiceError::Network(e.to_string()))?;

        Ok(Self {
            db,
            config,
            client,
            device_id,
            device_name,
        })
    }

    /// Perform full sync with a peer (bidirectional)
    pub async fn sync_with_peer(&self, peer_id: &str) -> SyncResult {
        let peer = {
            let config = self.config.lock().unwrap();
            config.get_peer(peer_id).cloned()
        };

        let peer = match peer {
            Some(p) => p,
            None => return SyncResult::failure(format!("Unknown peer: {}", peer_id)),
        };

        let peer_url = &peer.peer_url;
        let mut result = SyncResult::success();

        // Step 1: Handshake
        let handshake = match self.handshake(peer_url).await {
            Ok(h) => h,
            Err(e) => return SyncResult::failure(format!("Handshake failed: {}", e)),
        };

        // Use the OLDER of local and server timestamps (NULL = infinitely old)
        // This ensures that if either side has reset timestamps, we sync everything
        let local_last_sync = self.get_local_last_sync(peer_id);
        let last_sync = Self::older_timestamp(local_last_sync, handshake.last_sync_timestamp);
        let clock_skew = self.calculate_clock_skew(handshake.server_timestamp);

        // Adjust pull timestamp for clock skew
        let adjusted_since = self.adjust_timestamp_for_skew(last_sync, clock_skew);

        // Step 1b: Gather local changes BEFORE applying pull
        // This prevents the pull from overwriting local changes before they're pushed
        let local_changes_to_push = match self.get_changes_since(last_sync) {
            Ok(changes) => changes,
            Err(e) => {
                result.errors.push(format!("Failed to get local changes: {}", e));
                Vec::new()
            }
        };

        // Step 2: Pull changes
        let pulled_changes = match self.pull_changes(peer_url, adjusted_since).await {
            Ok((applied, conflicts, changes, errors)) => {
                result.pulled = applied;
                result.conflicts += conflicts;
                result.errors.extend(errors);
                changes
            }
            Err(e) => {
                result.errors.push(format!("Pull failed: {}", e));
                Vec::new()
            }
        };

        // Step 2b: Download audio files from cloud storage if configured
        {
            let audiofile_dir = {
                let config = self.config.lock().unwrap();
                config.audiofile_directory().map(std::path::PathBuf::from)
            };
            if let Some(dir) = audiofile_dir {
                // Download audio files from cloud storage (S3)
                let (downloaded, cloud_errors) = self
                    .download_audio_files_from_cloud(&dir)
                    .await;
                if downloaded > 0 {
                    tracing::info!("Downloaded {} audio files from cloud storage", downloaded);
                }
                result.errors.extend(cloud_errors);
            }
        }

        // Step 3: Push the pre-gathered local changes
        // We use push_changes_with_data instead of push_changes to avoid re-gathering
        // changes after the pull may have modified local state
        let pushed_changes = if local_changes_to_push.is_empty() {
            Vec::new()
        } else {
            tracing::debug!("Pushing {} changes to server...", local_changes_to_push.len());
            match self.push_changes_with_data(peer_url, &local_changes_to_push).await {
                Ok((applied, conflicts)) => {
                    tracing::debug!("Server response: applied={}, conflicts={}", applied, conflicts);
                    result.pushed = applied;
                    result.conflicts += conflicts;
                    local_changes_to_push
                }
                Err(e) => {
                    tracing::warn!("Push error: {}", e);
                    result.errors.push(format!("Push failed: {}", e));
                    Vec::new()
                }
            }
        };

        // Step 3b: Upload audio files to cloud storage if configured
        {
            let audiofile_dir = {
                let config = self.config.lock().unwrap();
                config.audiofile_directory().map(std::path::PathBuf::from)
            };
            if let Some(dir) = audiofile_dir {
                // Upload pending audio files to cloud storage (S3)
                let (uploaded, cloud_errors) = self
                    .upload_audio_files_to_cloud(&dir)
                    .await;
                if uploaded > 0 {
                    tracing::info!("Uploaded {} audio files to cloud storage", uploaded);
                }
                result.errors.extend(cloud_errors);
            }
        }

        // Update last sync time
        if let Err(e) = self.update_peer_sync_time(peer_id) {
            result.errors.push(format!("Failed to update sync time: {}", e));
        }

        result.success = result.errors.is_empty();
        result
    }

    /// Pull changes from a peer (one-way)
    pub async fn pull_from_peer(&self, peer_id: &str) -> SyncResult {
        let peer = {
            let config = self.config.lock().unwrap();
            config.get_peer(peer_id).cloned()
        };

        let peer = match peer {
            Some(p) => p,
            None => return SyncResult::failure(format!("Unknown peer: {}", peer_id)),
        };

        let peer_url = &peer.peer_url;
        let mut result = SyncResult::success();

        // Handshake
        let handshake = match self.handshake(peer_url).await {
            Ok(h) => h,
            Err(e) => return SyncResult::failure(format!("Handshake failed: {}", e)),
        };

        // Use the OLDER of local and server timestamps (NULL = infinitely old)
        let local_last_sync = self.get_local_last_sync(peer_id);
        let last_sync = Self::older_timestamp(local_last_sync, handshake.last_sync_timestamp);
        let clock_skew = self.calculate_clock_skew(handshake.server_timestamp);
        let adjusted_since = self.adjust_timestamp_for_skew(last_sync, clock_skew);

        // Pull
        let pulled_changes = match self.pull_changes(peer_url, adjusted_since).await {
            Ok((applied, conflicts, changes, errors)) => {
                result.pulled = applied;
                result.conflicts = conflicts;
                result.errors.extend(errors);
                changes
            }
            Err(e) => {
                result.success = false;
                result.errors.push(e.to_string());
                Vec::new()
            }
        };

        // Download audio files from cloud storage if configured
        if result.success {
            let audiofile_dir = {
                let config = self.config.lock().unwrap();
                config.audiofile_directory().map(std::path::PathBuf::from)
            };
            if let Some(dir) = audiofile_dir {
                // Download audio files from cloud storage (S3)
                let (downloaded, cloud_errors) = self
                    .download_audio_files_from_cloud(&dir)
                    .await;
                if downloaded > 0 {
                    tracing::info!("Downloaded {} audio files from cloud storage", downloaded);
                }
                result.errors.extend(cloud_errors);
            }
        }

        // Update last sync time
        if result.success {
            if let Err(e) = self.update_peer_sync_time(peer_id) {
                result.errors.push(format!("Failed to update sync time: {}", e));
            }
        }

        result.success = result.errors.is_empty();
        result
    }

    /// Push changes to a peer (one-way)
    pub async fn push_to_peer(&self, peer_id: &str) -> SyncResult {
        let peer = {
            let config = self.config.lock().unwrap();
            config.get_peer(peer_id).cloned()
        };

        let peer = match peer {
            Some(p) => p,
            None => return SyncResult::failure(format!("Unknown peer: {}", peer_id)),
        };

        let peer_url = &peer.peer_url;
        let mut result = SyncResult::success();

        // Handshake
        let handshake = match self.handshake(peer_url).await {
            Ok(h) => h,
            Err(e) => return SyncResult::failure(format!("Handshake failed: {}", e)),
        };

        // Push
        let pushed_changes = match self
            .push_changes(peer_url, handshake.last_sync_timestamp)
            .await
        {
            Ok((applied, conflicts, changes)) => {
                result.pushed = applied;
                result.conflicts = conflicts;
                changes
            }
            Err(e) => {
                result.success = false;
                result.errors.push(e.to_string());
                Vec::new()
            }
        };

        // Upload audio files to cloud storage if configured
        if result.success {
            let audiofile_dir = {
                let config = self.config.lock().unwrap();
                config.audiofile_directory().map(std::path::PathBuf::from)
            };
            if let Some(dir) = audiofile_dir {
                // Upload pending audio files to cloud storage (S3)
                let (uploaded, cloud_errors) = self
                    .upload_audio_files_to_cloud(&dir)
                    .await;
                if uploaded > 0 {
                    tracing::info!("Uploaded {} audio files to cloud storage", uploaded);
                }
                result.errors.extend(cloud_errors);
            }
        }

        // Update last sync time
        if result.success {
            if let Err(e) = self.update_peer_sync_time(peer_id) {
                result.errors.push(format!("Failed to update sync time: {}", e));
            }
        }

        result.success = result.errors.is_empty();
        result
    }

    /// Perform initial sync with a new peer (full dataset transfer)
    ///
    /// This is used for first-time sync when we need to get the complete
    /// dataset from a peer rather than incremental changes.
    pub async fn initial_sync(&self, peer_id: &str) -> SyncResult {
        let peer = {
            let config = self.config.lock().unwrap();
            config.get_peer(peer_id).cloned()
        };

        let peer = match peer {
            Some(p) => p,
            None => return SyncResult::failure(format!("Unknown peer: {}", peer_id)),
        };

        let peer_url = &peer.peer_url;
        let mut result = SyncResult::success();

        // Step 1: Handshake
        let _handshake = match self.handshake(peer_url).await {
            Ok(h) => h,
            Err(e) => return SyncResult::failure(format!("Handshake failed: {}", e)),
        };

        // Step 2: Get full dataset from peer
        let full_sync = match self.get_full_sync(peer_url).await {
            Ok(data) => data,
            Err(e) => return SyncResult::failure(format!("Full sync failed: {}", e)),
        };

        // Step 3: Convert full sync data to changes and apply
        let pulled_changes = self.convert_full_sync_to_changes(&full_sync);
        match self.apply_changes(&pulled_changes) {
            Ok((applied, conflicts, errors)) => {
                result.pulled = applied;
                result.conflicts = conflicts;
                result.errors.extend(errors);
            }
            Err(e) => {
                result.errors.push(format!("Failed to apply full sync: {}", e));
            }
        }

        // Step 4: Download audio files from cloud storage
        {
            let audiofile_dir = {
                let config = self.config.lock().unwrap();
                config.audiofile_directory().map(std::path::PathBuf::from)
            };
            if let Some(dir) = audiofile_dir {
                // Download audio files from cloud storage (S3)
                let (downloaded, cloud_errors) = self
                    .download_audio_files_from_cloud(&dir)
                    .await;
                if downloaded > 0 {
                    tracing::info!("Downloaded {} audio files from cloud storage", downloaded);
                }
                result.errors.extend(cloud_errors);
            }
        }

        // Step 5: Push all local changes (with since=None to get everything)
        let pushed_changes = match self.get_changes_since(None) {
            Ok(changes) => changes,
            Err(e) => {
                result.errors.push(format!("Failed to get local changes: {}", e));
                Vec::new()
            }
        };

        if !pushed_changes.is_empty() {
            match self.push_changes_with_data(peer_url, &pushed_changes).await {
                Ok((applied, conflicts)) => {
                    result.pushed = applied;
                    result.conflicts += conflicts;
                }
                Err(e) => {
                    result.errors.push(format!("Push failed: {}", e));
                }
            }

            // Step 6: Upload audio files to cloud storage
            {
                let audiofile_dir = {
                    let config = self.config.lock().unwrap();
                    config.audiofile_directory().map(std::path::PathBuf::from)
                };
                if let Some(dir) = audiofile_dir {
                    // Upload pending audio files to cloud storage (S3)
                    let (uploaded, cloud_errors) = self
                        .upload_audio_files_to_cloud(&dir)
                        .await;
                    if uploaded > 0 {
                        tracing::info!("Uploaded {} audio files to cloud storage", uploaded);
                    }
                    result.errors.extend(cloud_errors);
                }
            }
        }

        // Step 7: Update sync timestamp
        if let Err(e) = self.update_peer_sync_time(peer_id) {
            result.errors.push(format!("Failed to update sync time: {}", e));
        }

        result.success = result.errors.is_empty();
        result
    }

    /// Check if a peer is reachable
    pub async fn check_peer_status(&self, peer_id: &str) -> HashMap<String, serde_json::Value> {
        let peer = {
            let config = self.config.lock().unwrap();
            config.get_peer(peer_id).cloned()
        };

        let peer = match peer {
            Some(p) => p,
            None => {
                let mut result = HashMap::new();
                result.insert("reachable".to_string(), serde_json::Value::Bool(false));
                result.insert(
                    "error".to_string(),
                    serde_json::Value::String("Unknown peer".to_string()),
                );
                return result;
            }
        };

        match self
            .client
            .get(format!("{}/sync/status", peer.peer_url))
            .send()
            .await
        {
            Ok(response) => {
                if response.status().is_success() {
                    match response.json::<serde_json::Value>().await {
                        Ok(data) => {
                            let mut result = HashMap::new();
                            result.insert("reachable".to_string(), serde_json::Value::Bool(true));
                            if let Some(device_id) = data.get("device_id") {
                                result.insert("device_id".to_string(), device_id.clone());
                            }
                            if let Some(device_name) = data.get("device_name") {
                                result.insert("device_name".to_string(), device_name.clone());
                            }
                            if let Some(supports_audiofiles) = data.get("supports_audiofiles") {
                                result.insert("supports_audiofiles".to_string(), supports_audiofiles.clone());
                            }
                            result
                        }
                        Err(e) => {
                            let mut result = HashMap::new();
                            result.insert("reachable".to_string(), serde_json::Value::Bool(false));
                            result.insert(
                                "error".to_string(),
                                serde_json::Value::String(e.to_string()),
                            );
                            result
                        }
                    }
                } else {
                    let mut result = HashMap::new();
                    result.insert("reachable".to_string(), serde_json::Value::Bool(false));
                    result.insert(
                        "error".to_string(),
                        serde_json::Value::String(format!("HTTP {}", response.status())),
                    );
                    result
                }
            }
            Err(e) => {
                let mut result = HashMap::new();
                result.insert("reachable".to_string(), serde_json::Value::Bool(false));
                result.insert(
                    "error".to_string(),
                    serde_json::Value::String(e.to_string()),
                );
                result
            }
        }
    }

    // Internal methods

    async fn handshake(&self, peer_url: &str) -> VoiceResult<HandshakeResponse> {
        let request = HandshakeRequest {
            device_id: self.device_id.clone(),
            device_name: self.device_name.clone(),
            protocol_version: "1.0".to_string(),
        };

        let response = self
            .client
            .post(format!("{}/sync/handshake", peer_url))
            .json(&request)
            .send()
            .await
            .map_err(|e| VoiceError::Network(e.to_string()))?;

        if !response.status().is_success() {
            return Err(VoiceError::Sync(format!(
                "Handshake failed with status {}",
                response.status()
            )));
        }

        response
            .json::<HandshakeResponse>()
            .await
            .map_err(|e| VoiceError::Sync(format!("Failed to parse handshake response: {}", e)))
    }

    async fn pull_changes(
        &self,
        peer_url: &str,
        since: Option<i64>,
    ) -> VoiceResult<(i64, i64, Vec<SyncChange>, Vec<String>)> {
        let url = match since {
            Some(ts) => format!(
                "{}/sync/changes?since={}",
                peer_url,
                ts
            ),
            None => format!("{}/sync/changes", peer_url),
        };

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| VoiceError::Network(e.to_string()))?;

        if !response.status().is_success() {
            return Err(VoiceError::Sync(format!(
                "Pull failed with status {}",
                response.status()
            )));
        }

        let batch: SyncBatchResponse = response
            .json()
            .await
            .map_err(|e| VoiceError::Sync(format!("Failed to parse changes: {}", e)))?;

        // Clone changes before applying so we can return them for audio sync
        let changes = batch.changes.clone();

        // Log received changes
        tracing::debug!("Received {} changes to pull", changes.len());
        for change in &changes {
            tracing::trace!("  Pull: {} {} from {}", change.entity_type, &change.entity_id[..UUID_SHORT_LEN.min(change.entity_id.len())], change.device_id);
        }

        // Apply changes to local database
        let (applied, conflicts, errors) = self.apply_changes(&batch.changes)?;
        tracing::debug!("Applied {} changes, {} conflicts", applied, conflicts);

        Ok((applied, conflicts, changes, errors))
    }

    async fn push_changes(
        &self,
        peer_url: &str,
        since: Option<i64>,
    ) -> VoiceResult<(i64, i64, Vec<SyncChange>)> {
        // Get local changes
        let changes = self.get_changes_since(since)?;

        if changes.is_empty() {
            return Ok((0, 0, Vec::new()));
        }

        // Clone changes before moving into request so we can return them
        let pushed_changes = changes.clone();

        let request = ApplyRequest {
            device_id: self.device_id.clone(),
            device_name: self.device_name.clone(),
            changes,
        };

        let response = self
            .client
            .post(format!("{}/sync/apply", peer_url))
            .json(&request)
            .send()
            .await
            .map_err(|e| VoiceError::Network(e.to_string()))?;

        if !response.status().is_success() {
            return Err(VoiceError::Sync(format!(
                "Push failed with status {}",
                response.status()
            )));
        }

        let result: ApplyResponse = response
            .json()
            .await
            .map_err(|e| VoiceError::Sync(format!("Failed to parse apply response: {}", e)))?;

        Ok((result.applied, result.conflicts, pushed_changes))
    }

    fn apply_changes(&self, changes: &[SyncChange]) -> VoiceResult<(i64, i64, Vec<String>)> {
        let db = self.db.lock().unwrap();
        let mut applied = 0i64;
        let mut conflicts = 0i64;
        let mut errors = Vec::new();

        // Get last sync timestamp with this peer (use device_id as peer_id for self-tracking)
        let last_sync_at: Option<i64> = None; // For pull, we don't use last_sync filtering here

        for change in changes {
            let result = match change.entity_type.as_str() {
                "note" => self.apply_note_change(&db, change, last_sync_at),
                "tag" => self.apply_tag_change(&db, change, last_sync_at),
                "note_tag" => self.apply_note_tag_change(&db, change, last_sync_at),
                "audio_file" => self.apply_audio_file_change(&db, change),
                "note_attachment" => self.apply_note_attachment_change(&db, change),
                "transcription" => self.apply_transcription_change(&db, change),
                _ => continue,
            };

            match result {
                Ok(true) => applied += 1,
                Ok(false) => {} // Skipped
                Err(e) => {
                    errors.push(format!(
                        "Failed to apply {} {}: {}",
                        change.entity_type, change.entity_id, e
                    ));
                    conflicts += 1;
                }
            }
        }

        Ok((applied, conflicts, errors))
    }

    fn apply_note_change(
        &self,
        db: &Database,
        change: &SyncChange,
        last_sync_at: Option<i64>,
    ) -> VoiceResult<bool> {
        use crate::merge::merge_content;

        let note_id = &change.entity_id;
        let data = &change.data;

        // Parse timestamps from incoming data as integers
        let created_at = data["created_at"].as_i64().unwrap_or(0);
        let content = data["content"].as_str().unwrap_or("");
        let modified_at = data["modified_at"].as_i64();
        let deleted_at = data["deleted_at"].as_i64();

        // Check if local note exists and compare timestamps
        if let Ok(Some(existing)) = db.get_note_raw(note_id) {
            let local_modified = existing.get("modified_at")
                .and_then(|v| v.as_i64());
            let local_deleted = existing.get("deleted_at")
                .and_then(|v| v.as_i64());
            let local_content = existing.get("content")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let _local_device_id = existing.get("device_id")
                .and_then(|v| v.as_str());
            let local_time = local_modified.or(local_deleted);

            // Incoming timestamp: prefer modified_at, then deleted_at, then created_at
            let incoming_time = modified_at.or(deleted_at).or(Some(created_at));

            if let (Some(lt), Some(it)) = (local_time, incoming_time) {
                // If incoming is strictly older than local, skip
                if it < lt {
                    return Ok(false);
                }

                // If same timestamp but same content, skip (idempotent)
                if it == lt && content == local_content {
                    return Ok(false);
                }

                // Check for delete vs edit conflict
                // Case 1: Local is deleted, remote has edits
                if local_deleted.is_some() && deleted_at.is_none() && modified_at.is_some() {
                    // Remote edit is newer - resurrect with remote content
                    // Create delete conflict record
                    let _ = db.create_note_delete_conflict(
                        note_id,
                        content,                                 // surviving content (remote)
                        modified_at.unwrap_or(0),
                        Some(change.device_id.as_str()),         // surviving_device_id
                        change.device_name.as_deref(),           // surviving_device_name
                        Some(local_content),                     // deleted content
                        local_deleted.unwrap_or(0),
                        Some(&self.device_id),                   // deleting_device_id (local)
                        Some(&self.device_name),                 // deleting_device_name (local)
                    );
                    db.apply_sync_note(note_id, created_at, content, modified_at, None, None)?;
                    return Ok(true);
                }

                // Case 2: Local has edits, remote is deleted
                if local_deleted.is_none() && local_modified.is_some() && deleted_at.is_some() {
                    // Local edit should survive - create delete conflict
                    let _ = db.create_note_delete_conflict(
                        note_id,
                        local_content,                           // surviving content (local)
                        local_modified.unwrap_or(0),
                        Some(&self.device_id),                   // surviving_device_id (local)
                        Some(&self.device_name),                 // surviving_device_name (local)
                        Some(content),                           // deleted content
                        deleted_at.unwrap_or(0),
                        Some(change.device_id.as_str()),         // deleting_device_id
                        change.device_name.as_deref(),           // deleting_device_name
                    );
                    // Don't apply the delete - local edit survives
                    return Ok(false);
                }

                // Detect concurrent modifications:
                // 1. If we have last_sync_at and both local and incoming were modified after it
                // 2. OR if both have the exact same modified_at timestamp
                let is_concurrent = if let (Some(local_mod), Some(incoming_mod)) = (local_modified, modified_at) {
                    if let Some(last) = last_sync_at {
                        // Both modified since last sync = concurrent
                        local_mod > last && incoming_mod > last
                    } else {
                        // No sync history - only conflict if timestamps are exactly equal
                        local_mod == incoming_mod
                    }
                } else {
                    false
                };

                // Content conflict: concurrent modification with different content
                if is_concurrent && content != local_content && deleted_at.is_none() && local_deleted.is_none() {
                    if let (Some(local_mod), Some(incoming_mod)) = (local_modified, modified_at) {
                        // Create content conflict record before merging
                        let _ = db.create_note_content_conflict(
                            note_id,
                            local_content,
                            local_mod,
                            Some(&self.device_id),
                            Some(&self.device_name),
                            content,
                            incoming_mod,
                            Some(change.device_id.as_str()),
                            change.device_name.as_deref(),
                        );

                        let merge_result = merge_content(local_content, content, "LOCAL", "REMOTE");
                        // Use the newer timestamp for the merged content
                        let new_modified = if it > lt { Some(it) } else { Some(lt) };
                        db.apply_sync_note(note_id, created_at, &merge_result.content, new_modified, deleted_at, None)?;
                        return Ok(true);
                    }
                }
            }
        }

        // No conflict or local doesn't exist - just apply
        db.apply_sync_note(note_id, created_at, content, modified_at, deleted_at, None)?;
        Ok(true)
    }

    fn apply_tag_change(
        &self,
        db: &Database,
        change: &SyncChange,
        last_sync_at: Option<i64>,
    ) -> VoiceResult<bool> {
        let tag_id = &change.entity_id;
        let data = &change.data;

        let name = data["name"].as_str().unwrap_or("");
        let parent_id = data["parent_id"].as_str();
        // Parse timestamps from incoming data as integers
        let created_at = data["created_at"].as_i64().unwrap_or(0);
        let modified_at = data["modified_at"].as_i64();
        let deleted_at = data["deleted_at"].as_i64();

        // Check if local tag exists for conflict detection
        if let Ok(Some(existing)) = db.get_tag_raw(tag_id) {
            let local_name = existing.get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let local_parent_id = existing.get("parent_id")
                .and_then(|v| v.as_str());
            let local_modified = existing.get("modified_at")
                .and_then(|v| v.as_i64());
            let local_deleted = existing.get("deleted_at")
                .and_then(|v| v.as_i64());
            let _local_device_id = existing.get("device_id")
                .and_then(|v| v.as_str());

            let local_time = local_modified.or(local_deleted);
            let incoming_time = modified_at.or(deleted_at).or(Some(created_at));

            if let (Some(lt), Some(it)) = (local_time, incoming_time) {
                // If incoming is strictly older, skip
                if it < lt {
                    return Ok(false);
                }

                // If same timestamp and same values, skip (idempotent)
                if it == lt && name == local_name && parent_id == local_parent_id {
                    return Ok(false);
                }

                // Check for delete vs rename conflict
                if local_deleted.is_none() && deleted_at.is_some() && local_modified.is_some() {
                    // Local was renamed, remote wants to delete
                    // Keep local rename, record conflict
                    let _ = db.create_tag_delete_conflict(
                        tag_id,
                        local_name, // surviving
                        local_parent_id,
                        local_modified.unwrap_or(0),
                        Some(&self.device_id),
                        Some(&self.device_name),
                        deleted_at.unwrap_or(0),
                        Some(change.device_id.as_str()),
                        change.device_name.as_deref(),
                    );
                    return Ok(false);
                }

                if local_deleted.is_some() && deleted_at.is_none() && modified_at.is_some() {
                    // Local was deleted, remote was renamed - resurrect with remote
                    let _ = db.create_tag_delete_conflict(
                        tag_id,
                        name, // surviving (remote)
                        parent_id,
                        modified_at.unwrap_or(0),
                        Some(change.device_id.as_str()),
                        change.device_name.as_deref(),
                        local_deleted.unwrap_or(0),
                        Some(&self.device_id),
                        Some(&self.device_name),
                    );
                    // Resurrect by clearing deleted_at
                    db.apply_sync_tag_with_deleted(tag_id, name, parent_id, created_at, modified_at, None, None)?;
                    return Ok(true);
                }

                // Detect concurrent modifications:
                // 1. If we have last_sync_at and both local and incoming were modified after it
                // 2. OR if both have the exact same modified_at timestamp
                let is_concurrent = if let (Some(local_mod), Some(incoming_mod)) = (local_modified, modified_at) {
                    if let Some(last) = last_sync_at {
                        // Both modified since last sync = concurrent
                        local_mod > last && incoming_mod > last
                    } else {
                        // No sync history - only conflict if timestamps are exactly equal
                        local_mod == incoming_mod
                    }
                } else {
                    false
                };

                if is_concurrent && local_deleted.is_none() && deleted_at.is_none() {
                    if let (Some(local_mod), Some(incoming_mod)) = (local_modified, modified_at) {
                        // Check for concurrent rename conflict
                        if name != local_name {
                            // Both renamed to different names concurrently - merge with separator
                            let _ = db.create_tag_rename_conflict(
                                tag_id,
                                local_name,
                                local_mod,
                                Some(&self.device_id),
                                Some(&self.device_name),
                                name,
                                incoming_mod,
                                Some(change.device_id.as_str()),
                                change.device_name.as_deref(),
                            );

                            // Merge names with separator (deleted_at is None in this branch)
                            let merged_name = format!("{} | {}", local_name, name);
                            db.apply_sync_tag_with_deleted(tag_id, &merged_name, parent_id, created_at, modified_at, None, None)?;
                            return Ok(true);
                        }

                        // Check for parent conflict
                        if parent_id != local_parent_id {
                            let _ = db.create_tag_parent_conflict(
                                tag_id,
                                local_parent_id,
                                local_mod,
                                Some(&self.device_id),
                                Some(&self.device_name),
                                parent_id,
                                incoming_mod,
                                Some(change.device_id.as_str()),
                                change.device_name.as_deref(),
                            );
                            // Use remote parent
                        }
                    }
                }
            }
        }

        db.apply_sync_tag_with_deleted(tag_id, name, parent_id, created_at, modified_at, deleted_at, None)?;
        Ok(true)
    }

    fn apply_note_tag_change(
        &self,
        db: &Database,
        change: &SyncChange,
        _last_sync_at: Option<i64>,
    ) -> VoiceResult<bool> {
        // Parse entity_id (format: "note_id:tag_id")
        let parts: Vec<&str> = change.entity_id.split(':').collect();
        if parts.len() != 2 {
            tracing::warn!("note_tag: invalid entity_id format: {}", change.entity_id);
            return Ok(false);
        }

        let note_id = parts[0];
        let tag_id = parts[1];
        let data = &change.data;

        // Parse timestamps from incoming data as integers
        let created_at = data["created_at"].as_i64().unwrap_or(0);
        let modified_at = data["modified_at"].as_i64();
        let deleted_at = data["deleted_at"].as_i64();

        tracing::trace!(
            "note_tag: note={}... tag={}... created={} modified={:?} deleted={:?}",
            &note_id[..UUID_SHORT_LEN.min(note_id.len())],
            &tag_id[..UUID_SHORT_LEN.min(tag_id.len())],
            created_at,
            modified_at,
            deleted_at
        );

        // Determine incoming timestamp
        let incoming_time = deleted_at.or(modified_at).or(Some(created_at));

        // Check if local note_tag exists and compare timestamps
        if let Ok(Some(existing)) = db.get_note_tag_raw(note_id, tag_id) {
            let local_modified = existing.get("modified_at").and_then(|v| v.as_i64());
            let local_deleted = existing.get("deleted_at").and_then(|v| v.as_i64());
            let local_created = existing.get("created_at").and_then(|v| v.as_i64());
            let local_time = local_deleted.or(local_modified).or(local_created);

            tracing::trace!(
                "note_tag: existing local_time={:?} incoming_time={:?}",
                local_time, incoming_time
            );

            if let (Some(lt), Some(it)) = (local_time, incoming_time) {
                // If incoming is strictly older than local, skip
                if it < lt {
                    tracing::debug!("note_tag: skipped (incoming {} older than local {})", it, lt);
                    return Ok(false);
                }
                // If same timestamp, skip (idempotent)
                if it == lt {
                    tracing::trace!("note_tag: skipped (same timestamp {})", it);
                    return Ok(false);
                }
            }
        } else {
            tracing::trace!("note_tag: no existing local record, will insert");
        }

        db.apply_sync_note_tag(note_id, tag_id, created_at, modified_at, deleted_at, None)?;
        tracing::debug!(
            "note_tag: applied note={}... tag={}...",
            &note_id[..UUID_SHORT_LEN.min(note_id.len())],
            &tag_id[..UUID_SHORT_LEN.min(tag_id.len())]
        );
        Ok(true)
    }

    fn apply_audio_file_change(
        &self,
        db: &Database,
        change: &SyncChange,
    ) -> VoiceResult<bool> {
        let audio_id = &change.entity_id;
        let data = &change.data;

        // Parse timestamps from incoming data as integers
        let imported_at = data["imported_at"].as_i64().unwrap_or(0);
        let filename = data["filename"].as_str().unwrap_or("");
        let file_created_at = data["file_created_at"].as_i64();
        let duration_seconds = data["duration_seconds"].as_i64();
        let summary = data["summary"].as_str();
        let modified_at = data["modified_at"].as_i64();
        let deleted_at = data["deleted_at"].as_i64();
        let storage_provider = data["storage_provider"].as_str();
        let storage_key = data["storage_key"].as_str();
        let storage_uploaded_at = data["storage_uploaded_at"].as_i64();

        db.apply_sync_audio_file(
            audio_id,
            imported_at,
            filename,
            file_created_at,
            duration_seconds,
            summary,
            modified_at,
            deleted_at,
            None,  // sync_received_at - None for client-side operations
            storage_provider,
            storage_key,
            storage_uploaded_at,
        )?;
        Ok(true)
    }

    fn apply_note_attachment_change(
        &self,
        db: &Database,
        change: &SyncChange,
    ) -> VoiceResult<bool> {
        let attachment_link_id = &change.entity_id;
        let data = &change.data;

        let note_id = data["note_id"].as_str().unwrap_or("");
        let attachment_id = data["attachment_id"].as_str().unwrap_or("");
        let attachment_type = data["attachment_type"].as_str().unwrap_or("");
        // Parse timestamps from incoming data as integers
        let created_at = data["created_at"].as_i64().unwrap_or(0);
        let modified_at = data["modified_at"].as_i64();
        let deleted_at = data["deleted_at"].as_i64();

        db.apply_sync_note_attachment(
            attachment_link_id,
            note_id,
            attachment_id,
            attachment_type,
            created_at,
            modified_at,
            deleted_at,
            None,  // sync_received_at - None for client-side operations
        )?;
        Ok(true)
    }

    fn apply_transcription_change(
        &self,
        db: &Database,
        change: &SyncChange,
    ) -> VoiceResult<bool> {
        let transcription_id = &change.entity_id;
        let data = &change.data;

        let audio_file_id = data["audio_file_id"].as_str().unwrap_or("");
        let content = data["content"].as_str().unwrap_or("");
        let content_segments = data["content_segments"].as_str();
        let service = data["service"].as_str().unwrap_or("");
        let service_arguments = data["service_arguments"].as_str();
        let service_response = data["service_response"].as_str();
        let state = data["state"].as_str().unwrap_or(crate::database::DEFAULT_TRANSCRIPTION_STATE);
        let device_id = data["device_id"].as_str().unwrap_or("");
        // Parse timestamps from incoming data as integers
        let created_at = data["created_at"].as_i64().unwrap_or(0);
        let modified_at = data["modified_at"].as_i64();
        let deleted_at = data["deleted_at"].as_i64();

        db.apply_sync_transcription(
            transcription_id,
            audio_file_id,
            content,
            content_segments,
            service,
            service_arguments,
            service_response,
            state,
            device_id,
            created_at,
            modified_at,
            deleted_at,
            None,  // sync_received_at - None for client-side operations
        )?;
        Ok(true)
    }

    fn get_changes_since(&self, since: Option<i64>) -> VoiceResult<Vec<SyncChange>> {
        let db = self.db.lock().unwrap();
        let (changes, _) = db.get_changes_since(since, 10000)?;

        // Convert HashMap changes to SyncChange structs
        let sync_changes: Vec<SyncChange> = changes
            .into_iter()
            .filter_map(|c| {
                let entity_type = c.get("entity_type")?.as_str()?.to_string();
                let entity_id = c.get("entity_id")?.as_str()?.to_string();
                let operation = c.get("operation")?.as_str()?.to_string();
                let timestamp = c.get("timestamp")?.as_i64()?;
                let data = c.get("data")?.clone();

                Some(SyncChange {
                    entity_type,
                    entity_id,
                    operation,
                    data,
                    timestamp,
                    device_id: self.device_id.clone(),
                    device_name: Some(self.device_name.clone()),
                })
            })
            .collect();

        Ok(sync_changes)
    }

    /// Return the older of two timestamps, treating None as infinitely old.
    /// If either is None, returns None (meaning "sync everything").
    /// If both are Some, returns the smaller (older) timestamp.
    fn older_timestamp(a: Option<i64>, b: Option<i64>) -> Option<i64> {
        match (a, b) {
            (None, _) | (_, None) => None,
            (Some(ts_a), Some(ts_b)) => {
                Some(std::cmp::min(ts_a, ts_b))
            }
        }
    }

    fn calculate_clock_skew(&self, server_timestamp: Option<i64>) -> f64 {
        if let Some(server_ts) = server_timestamp {
            let local_time = Utc::now().timestamp();
            return (server_ts - local_time) as f64;
        }
        0.0
    }

    fn adjust_timestamp_for_skew(&self, timestamp: Option<i64>, clock_skew: f64) -> Option<i64> {
        let ts = timestamp?;

        // Always go back at least 2 seconds for race conditions
        let base_adjustment = 2;
        // Add 2x skew if significant (> 1 second)
        let skew_adjustment = if clock_skew.abs() > 1.0 {
            (2.0 * clock_skew.abs()) as i64
        } else {
            0
        };
        let total_adjustment = base_adjustment + skew_adjustment;

        Some(ts - total_adjustment)
    }

    fn update_peer_sync_time(&self, peer_id: &str) -> VoiceResult<()> {
        let peer_uuid = Uuid::parse_str(peer_id)?;
        let peer_bytes = peer_uuid.as_bytes().to_vec();

        let db = self.db.lock().unwrap();
        let conn = db.connection();

        // Try to update existing record (use Unix timestamp)
        let updated = conn.execute(
            "UPDATE sync_peers SET last_sync_at = strftime('%s', 'now') WHERE peer_id = ?",
            [&peer_bytes],
        )?;

        if updated == 0 {
            // Insert new record (use Unix timestamp)
            let config = self.config.lock().unwrap();
            if let Some(peer) = config.get_peer(peer_id) {
                conn.execute(
                    "INSERT INTO sync_peers (peer_id, peer_name, peer_url, last_sync_at) VALUES (?, ?, ?, strftime('%s', 'now'))",
                    rusqlite::params![peer_bytes, peer.peer_name, peer.peer_url],
                )?;
            }
        }

        Ok(())
    }

    /// Get the local record of when we last synced with a peer
    fn get_local_last_sync(&self, peer_id: &str) -> Option<i64> {
        let peer_uuid = Uuid::parse_str(peer_id).ok()?;
        let peer_bytes = peer_uuid.as_bytes().to_vec();

        let db = self.db.lock().ok()?;
        let conn = db.connection();

        conn.query_row(
            "SELECT last_sync_at FROM sync_peers WHERE peer_id = ?",
            [&peer_bytes],
            |row| row.get::<_, Option<i64>>(0),
        )
        .ok()
        .flatten()
    }

    /// Debug: public version of get_local_last_sync
    pub fn debug_get_local_last_sync(&self, peer_id: &str) -> Option<i64> {
        self.get_local_last_sync(peer_id)
    }

    /// Debug: public version of get_changes_since
    pub fn debug_get_changes_since(&self, since: Option<i64>) -> VoiceResult<Vec<SyncChange>> {
        self.get_changes_since(since)
    }

    /// Get full dataset from peer (for initial sync)
    async fn get_full_sync(&self, peer_url: &str) -> VoiceResult<FullSyncResponse> {
        let url = format!("{}/sync/full", peer_url);

        let response = self
            .client
            .get(&url)
            .header("X-Device-ID", &self.device_id)
            .header("X-Device-Name", &self.device_name)
            .send()
            .await
            .map_err(|e| VoiceError::Network(e.to_string()))?;

        if !response.status().is_success() {
            return Err(VoiceError::Sync(format!(
                "Full sync failed with status {}",
                response.status()
            )));
        }

        response
            .json::<FullSyncResponse>()
            .await
            .map_err(|e| VoiceError::Sync(format!("Failed to parse full sync response: {}", e)))
    }

    /// Convert full sync response to SyncChange format for applying
    fn convert_full_sync_to_changes(&self, full_sync: &FullSyncResponse) -> Vec<SyncChange> {
        let mut changes = Vec::new();

        // Convert notes
        for note in &full_sync.notes {
            if let Some(id) = note.get("id").and_then(|v| v.as_str()) {
                let timestamp = note
                    .get("modified_at")
                    .or_else(|| note.get("created_at"))
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);

                changes.push(SyncChange {
                    entity_type: "note".to_string(),
                    entity_id: id.to_string(),
                    operation: "create".to_string(),
                    data: note.clone(),
                    timestamp,
                    device_id: full_sync.device_id.clone(),
                    device_name: full_sync.device_name.clone(),
                });
            }
        }

        // Convert tags
        for tag in &full_sync.tags {
            if let Some(id) = tag.get("id").and_then(|v| v.as_str()) {
                let timestamp = tag
                    .get("modified_at")
                    .or_else(|| tag.get("created_at"))
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);

                changes.push(SyncChange {
                    entity_type: "tag".to_string(),
                    entity_id: id.to_string(),
                    operation: "create".to_string(),
                    data: tag.clone(),
                    timestamp,
                    device_id: full_sync.device_id.clone(),
                    device_name: full_sync.device_name.clone(),
                });
            }
        }

        // Convert note_tags
        for note_tag in &full_sync.note_tags {
            let note_id = note_tag.get("note_id").and_then(|v| v.as_str());
            let tag_id = note_tag.get("tag_id").and_then(|v| v.as_str());

            if let (Some(note_id), Some(tag_id)) = (note_id, tag_id) {
                let timestamp = note_tag
                    .get("modified_at")
                    .or_else(|| note_tag.get("created_at"))
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);

                changes.push(SyncChange {
                    entity_type: "note_tag".to_string(),
                    entity_id: format!("{}:{}", note_id, tag_id),
                    operation: "create".to_string(),
                    data: note_tag.clone(),
                    timestamp,
                    device_id: full_sync.device_id.clone(),
                    device_name: full_sync.device_name.clone(),
                });
            }
        }

        // Convert audio_files
        if let Some(audio_files) = &full_sync.audio_files {
            for audio in audio_files {
                if let Some(id) = audio.get("id").and_then(|v| v.as_str()) {
                    let timestamp = audio
                        .get("modified_at")
                        .or_else(|| audio.get("imported_at"))
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);

                    changes.push(SyncChange {
                        entity_type: "audio_file".to_string(),
                        entity_id: id.to_string(),
                        operation: "create".to_string(),
                        data: audio.clone(),
                        timestamp,
                        device_id: full_sync.device_id.clone(),
                        device_name: full_sync.device_name.clone(),
                    });
                }
            }
        }

        // Convert note_attachments
        if let Some(attachments) = &full_sync.note_attachments {
            for attachment in attachments {
                if let Some(id) = attachment.get("id").and_then(|v| v.as_str()) {
                    let timestamp = attachment
                        .get("modified_at")
                        .or_else(|| attachment.get("created_at"))
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);

                    changes.push(SyncChange {
                        entity_type: "note_attachment".to_string(),
                        entity_id: id.to_string(),
                        operation: "create".to_string(),
                        data: attachment.clone(),
                        timestamp,
                        device_id: full_sync.device_id.clone(),
                        device_name: full_sync.device_name.clone(),
                    });
                }
            }
        }

        // Convert transcriptions
        if let Some(transcriptions) = &full_sync.transcriptions {
            for transcription in transcriptions {
                if let Some(id) = transcription.get("id").and_then(|v| v.as_str()) {
                    let timestamp = transcription
                        .get("modified_at")
                        .or_else(|| transcription.get("created_at"))
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);

                    changes.push(SyncChange {
                        entity_type: "transcription".to_string(),
                        entity_id: id.to_string(),
                        operation: "create".to_string(),
                        data: transcription.clone(),
                        timestamp,
                        device_id: full_sync.device_id.clone(),
                        device_name: full_sync.device_name.clone(),
                    });
                }
            }
        }

        changes
    }

    /// Push pre-fetched changes to peer (used by initial_sync)
    async fn push_changes_with_data(
        &self,
        peer_url: &str,
        changes: &[SyncChange],
    ) -> VoiceResult<(i64, i64)> {
        if changes.is_empty() {
            return Ok((0, 0));
        }

        let request = ApplyRequest {
            device_id: self.device_id.clone(),
            device_name: self.device_name.clone(),
            changes: changes.to_vec(),
        };

        let response = self
            .client
            .post(format!("{}/sync/apply", peer_url))
            .json(&request)
            .send()
            .await
            .map_err(|e| VoiceError::Network(e.to_string()))?;

        if !response.status().is_success() {
            return Err(VoiceError::Sync(format!(
                "Push failed with status {}",
                response.status()
            )));
        }

        let result: ApplyResponse = response
            .json()
            .await
            .map_err(|e| VoiceError::Sync(format!("Failed to parse apply response: {}", e)))?;

        Ok((result.applied, result.conflicts))
    }

    /// Download an audio file from a peer.
    ///
    /// Args:
    ///     peer_url: Base URL of the peer sync server
    ///     audio_id: Audio file UUID hex string
    ///     dest_path: Local path to save the file
    ///
    /// Returns:
    ///     Number of bytes downloaded on success
    pub async fn download_audio_file(
        &self,
        peer_url: &str,
        audio_id: &str,
        dest_path: &std::path::Path,
    ) -> VoiceResult<u64> {
        use std::io::Write;

        let url = format!("{}/sync/audio/{}/file", peer_url, audio_id);

        let response = self
            .client
            .get(&url)
            .header("X-Device-ID", &self.device_id)
            .header("X-Device-Name", &self.device_name)
            .send()
            .await
            .map_err(|e| VoiceError::Network(format!("Failed to download audio {}: {}", audio_id, e)))?;

        if !response.status().is_success() {
            // Try to get error message from response body
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            let error_msg = if let Ok(json) = serde_json::from_str::<serde_json::Value>(&error_body) {
                json.get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&error_body)
                    .to_string()
            } else {
                error_body
            };
            return Err(VoiceError::Network(format!(
                "Failed to download audio {}: HTTP {} - {}",
                audio_id, status, error_msg
            )));
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|e| VoiceError::Network(format!("Failed to read audio response: {}", e)))?;

        let bytes_len = bytes.len() as u64;

        // Create parent directory if needed
        if let Some(parent) = dest_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                VoiceError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to create audio directory: {}", e),
                ))
            })?;
        }

        // Write to temp file first, then rename atomically
        let temp_path = dest_path.with_extension("tmp");
        let mut file = std::fs::File::create(&temp_path).map_err(|e| {
            VoiceError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to create temp file: {}", e),
            ))
        })?;
        file.write_all(&bytes).map_err(|e| {
            VoiceError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to write audio file: {}", e),
            ))
        })?;
        file.sync_all().map_err(|e| {
            VoiceError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to sync audio file: {}", e),
            ))
        })?;
        drop(file);

        // Rename atomically
        std::fs::rename(&temp_path, dest_path).map_err(|e| {
            // Clean up temp file on failure
            let _ = std::fs::remove_file(&temp_path);
            VoiceError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to rename audio file: {}", e),
            ))
        })?;

        Ok(bytes_len)
    }

    /// Upload an audio file to a peer.
    ///
    /// Args:
    ///     peer_url: Base URL of the peer sync server
    ///     audio_id: Audio file UUID hex string
    ///     source_path: Local path to the file to upload
    ///
    /// Returns:
    ///     Number of bytes uploaded on success
    pub async fn upload_audio_file(
        &self,
        peer_url: &str,
        audio_id: &str,
        source_path: &std::path::Path,
    ) -> VoiceResult<u64> {
        let url = format!("{}/sync/audio/{}/file", peer_url, audio_id);

        // Read file into memory
        let bytes = std::fs::read(source_path).map_err(|e| {
            VoiceError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to read audio file {}: {}", source_path.display(), e),
            ))
        })?;

        let bytes_len = bytes.len() as u64;

        let response = self
            .client
            .post(&url)
            .header("X-Device-ID", &self.device_id)
            .header("X-Device-Name", &self.device_name)
            .header("Content-Type", "application/octet-stream")
            .body(bytes)
            .send()
            .await
            .map_err(|e| {
                VoiceError::Network(format!("Failed to upload audio {}: {}", audio_id, e))
            })?;

        if !response.status().is_success() {
            // Try to get error message from response body
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            let error_msg =
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&error_body) {
                    json.get("error")
                        .and_then(|v| v.as_str())
                        .unwrap_or(&error_body)
                        .to_string()
                } else {
                    error_body
                };
            return Err(VoiceError::Network(format!(
                "Failed to upload audio {}: HTTP {} - {}",
                audio_id, status, error_msg
            )));
        }

        tracing::info!("Uploaded audio file {} ({} bytes)", audio_id, bytes_len);
        Ok(bytes_len)
    }

    /// Download binary files for audio_files that were pulled during sync.
    ///
    /// Args:
    ///     peer_url: Base URL of the peer sync server
    ///     pulled_changes: List of changes that were pulled
    ///     audiofile_directory: Directory to save audio files
    ///
    /// Returns:
    ///     List of error messages (empty if all succeeded)
    pub async fn sync_audio_files_after_pull(
        &self,
        peer_url: &str,
        pulled_changes: &[SyncChange],
        audiofile_directory: &std::path::Path,
    ) -> Vec<String> {
        let mut errors = Vec::new();

        for change in pulled_changes {
            // Only process audio_file creates/updates (not deletes)
            if change.entity_type != "audio_file" {
                continue;
            }
            if change.operation == "delete" {
                continue;
            }

            let audio_id = &change.entity_id;

            // Get filename from change data to determine extension
            let filename = change
                .data
                .get("filename")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown.bin");
            let ext = filename
                .rsplit('.')
                .next()
                .unwrap_or("bin");

            let dest_path = audiofile_directory.join(format!("{}.{}", audio_id, ext));

            // Skip if file already exists
            if dest_path.exists() {
                continue;
            }

            match self.download_audio_file(peer_url, audio_id, &dest_path).await {
                Ok(bytes) => {
                    tracing::info!("Downloaded audio file {} ({} bytes)", audio_id, bytes);
                }
                Err(e) => {
                    errors.push(format!("Failed to download audio {}: {}", audio_id, e));
                }
            }
        }

        errors
    }

    /// Download any audio files that exist in the database but are missing locally.
    ///
    /// This handles the case where audio file metadata was synced successfully but
    /// the binary download failed (e.g., due to permission issues). On subsequent
    /// syncs, this function will retry downloading missing files.
    ///
    /// Args:
    ///     peer_url: Base URL of the peer sync server
    ///     audiofile_directory: Directory to save audio files
    ///
    /// Returns:
    ///     List of error messages (empty if all succeeded)
    pub async fn download_missing_audio_files(
        &self,
        peer_url: &str,
        audiofile_directory: &std::path::Path,
    ) -> Vec<String> {
        let mut errors = Vec::new();

        // Get all audio files from the database
        let audio_files = {
            let db = self.db.lock().unwrap();
            match db.get_all_audio_files() {
                Ok(files) => files,
                Err(e) => {
                    errors.push(format!("Failed to get audio files from database: {}", e));
                    return errors;
                }
            }
        };

        for audio_file in audio_files {
            // Skip deleted audio files
            if audio_file.deleted_at.is_some() {
                continue;
            }

            let audio_id = &audio_file.id;

            // Get extension from filename
            let ext = audio_file
                .filename
                .rsplit('.')
                .next()
                .unwrap_or("bin");

            let dest_path = audiofile_directory.join(format!("{}.{}", audio_id, ext));

            // Skip if file already exists
            if dest_path.exists() {
                continue;
            }

            tracing::info!("Downloading missing audio file: {}", audio_id);
            match self.download_audio_file(peer_url, audio_id, &dest_path).await {
                Ok(bytes) => {
                    tracing::info!("Downloaded missing audio file {} ({} bytes)", audio_id, bytes);
                }
                Err(e) => {
                    errors.push(format!("Failed to download missing audio {}: {}", audio_id, e));
                }
            }
        }

        errors
    }

    /// Upload binary files for audio_files that were pushed during sync.
    ///
    /// Args:
    ///     peer_url: Base URL of the peer sync server
    ///     pushed_changes: List of changes that were pushed
    ///     audiofile_directory: Directory where audio files are stored
    ///
    /// Returns:
    ///     List of error messages (empty if all succeeded)
    pub async fn sync_audio_files_after_push(
        &self,
        peer_url: &str,
        pushed_changes: &[SyncChange],
        audiofile_directory: &std::path::Path,
    ) -> Vec<String> {
        let mut errors = Vec::new();

        // Get max file size from config
        let max_file_size = {
            let cfg = self.config.lock().unwrap();
            cfg.max_sync_file_size_bytes()
        };

        for change in pushed_changes {
            // Only process audio_file creates/updates (not deletes)
            if change.entity_type != "audio_file" {
                continue;
            }
            if change.operation == "delete" {
                continue;
            }

            let audio_id = &change.entity_id;

            // Get filename from change data to determine extension
            let filename = change
                .data
                .get("filename")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown.bin");
            let ext = filename.rsplit('.').next().unwrap_or("bin");

            let source_path = audiofile_directory.join(format!("{}.{}", audio_id, ext));

            // Skip if local file doesn't exist
            if !source_path.exists() {
                tracing::warn!(
                    "Audio file {} not found locally at {}, skipping upload",
                    audio_id,
                    source_path.display()
                );
                continue;
            }

            // Check file size
            let file_size = match std::fs::metadata(&source_path) {
                Ok(meta) => meta.len(),
                Err(e) => {
                    errors.push(format!("Failed to get file size for {}: {}", audio_id, e));
                    continue;
                }
            };

            // If file is too big, tag attached notes and skip upload
            if file_size > max_file_size {
                tracing::warn!(
                    "Audio file {} is too large ({} bytes > {} max), tagging notes as _too-big",
                    audio_id,
                    file_size,
                    max_file_size
                );

                // Tag all notes that have this audio file attached
                if let Ok(db) = self.db.lock() {
                    if let Ok(note_ids) = db.get_notes_for_audio_file(audio_id) {
                        for note_id in note_ids {
                            if let Err(e) = db.tag_note_too_big(&note_id) {
                                tracing::error!(
                                    "Failed to tag note {} as too-big: {}",
                                    note_id,
                                    e
                                );
                            } else {
                                tracing::info!(
                                    "Tagged note {} as _too-big due to large audio file {}",
                                    note_id,
                                    audio_id
                                );
                            }
                        }
                    }
                }

                errors.push(format!(
                    "Audio file {} is too large to sync ({} MB > {} MB limit)",
                    audio_id,
                    file_size / 1024 / 1024,
                    max_file_size / 1024 / 1024
                ));
                continue;
            }

            match self.upload_audio_file(peer_url, audio_id, &source_path).await {
                Ok(_) => {}
                Err(e) => {
                    errors.push(format!("Failed to upload audio {}: {}", audio_id, e));
                }
            }
        }

        errors
    }

    /// Upload pending audio files to cloud storage (S3).
    ///
    /// This is called during sync to upload any local files that haven't been
    /// uploaded to cloud storage yet. Files are identified by having
    /// storage_provider = NULL in the database.
    ///
    /// # Arguments
    /// * `audiofile_directory` - Directory where audio files are stored locally
    ///
    /// # Returns
    /// Tuple of (uploaded_count, error_messages)
    #[cfg(feature = "file-storage")]
    pub async fn upload_audio_files_to_cloud(
        &self,
        audiofile_directory: &std::path::Path,
    ) -> (usize, Vec<String>) {
        use crate::file_storage::{upload_pending_audio_files, FileStorageError};

        // Check if cloud storage is configured
        let is_enabled = {
            let db = match self.db.lock() {
                Ok(db) => db,
                Err(_) => return (0, vec!["Failed to lock database".to_string()]),
            };
            match db.get_file_storage_config_struct() {
                Ok(config) => config.is_enabled(),
                Err(_) => false,
            }
        };

        if !is_enabled {
            tracing::debug!("Cloud storage not configured, skipping upload");
            return (0, Vec::new());
        }

        // Use the file_storage module's upload function
        let db = match self.db.lock() {
            Ok(db) => db,
            Err(_) => return (0, vec!["Failed to lock database".to_string()]),
        };

        match upload_pending_audio_files(&db, audiofile_directory).await {
            Ok(result) => {
                if result.uploaded > 0 {
                    tracing::info!("Uploaded {} audio files to cloud storage", result.uploaded);
                }
                if result.failed > 0 {
                    tracing::warn!("Failed to upload {} audio files to cloud storage", result.failed);
                }
                (result.uploaded, result.errors)
            }
            Err(e) => {
                let msg = format!("Cloud storage upload failed: {}", e);
                tracing::error!("{}", msg);
                (0, vec![msg])
            }
        }
    }

    /// Download audio files from cloud storage (S3) that are missing locally.
    ///
    /// This is called during sync to download files that exist in the database
    /// (with storage_provider set) but don't exist locally.
    ///
    /// # Arguments
    /// * `audiofile_directory` - Directory to save audio files
    ///
    /// # Returns
    /// Tuple of (downloaded_count, error_messages)
    #[cfg(feature = "file-storage")]
    pub async fn download_audio_files_from_cloud(
        &self,
        audiofile_directory: &std::path::Path,
    ) -> (usize, Vec<String>) {
        use crate::file_storage_s3::{S3Config, S3StorageService};
        use crate::file_storage::FileStorageService;

        let mut downloaded = 0;
        let mut errors = Vec::new();

        // Get storage config
        let storage_config = {
            let db = match self.db.lock() {
                Ok(db) => db,
                Err(_) => return (0, vec!["Failed to lock database".to_string()]),
            };
            match db.get_file_storage_config_struct() {
                Ok(config) => config,
                Err(e) => return (0, vec![format!("Failed to get storage config: {}", e)]),
            }
        };

        if !storage_config.is_enabled() {
            tracing::debug!("Cloud storage not configured, skipping download");
            return (0, Vec::new());
        }

        if storage_config.provider != "s3" {
            return (0, vec![format!("Unsupported storage provider: {}", storage_config.provider)]);
        }

        // Create S3 service
        let bucket = match storage_config.s3_bucket() {
            Some(b) => b.to_string(),
            None => return (0, vec!["S3 bucket not configured".to_string()]),
        };
        let region = match storage_config.s3_region() {
            Some(r) => r.to_string(),
            None => return (0, vec!["S3 region not configured".to_string()]),
        };
        let access_key_id = match storage_config.s3_access_key_id() {
            Some(k) => k.to_string(),
            None => return (0, vec!["S3 access_key_id not configured".to_string()]),
        };
        let secret_access_key = match storage_config.s3_secret_access_key() {
            Some(k) => k.to_string(),
            None => return (0, vec!["S3 secret_access_key not configured".to_string()]),
        };
        let prefix = storage_config.s3_prefix().map(String::from);
        let endpoint = storage_config.s3_endpoint().map(String::from);

        let s3_config = S3Config {
            bucket,
            region,
            access_key_id,
            secret_access_key,
            prefix: prefix.clone(),
            endpoint,
        };

        let storage = match S3StorageService::new(s3_config) {
            Ok(s) => s,
            Err(e) => return (0, vec![format!("Failed to create S3 service: {}", e)]),
        };

        // Get audio files that have cloud storage info but are missing locally
        let audio_files = {
            let db = match self.db.lock() {
                Ok(db) => db,
                Err(_) => return (0, vec!["Failed to lock database".to_string()]),
            };
            match db.get_all_audio_files() {
                Ok(files) => files,
                Err(e) => return (0, vec![format!("Failed to get audio files: {}", e)]),
            }
        };

        for audio_file in audio_files {
            // Skip files without cloud storage info
            let storage_key = match &audio_file.storage_key {
                Some(key) => key.clone(),
                None => continue,
            };

            // Skip files that already exist locally
            let ext = audio_file.filename.rsplit('.').next().unwrap_or("bin");
            let local_path = audiofile_directory.join(format!("{}.{}", audio_file.id, ext));
            if local_path.exists() {
                continue;
            }

            tracing::info!(
                audio_id = %audio_file.id,
                storage_key = %storage_key,
                "Downloading audio file from cloud storage"
            );

            // Get pre-signed download URL
            let download_url = match storage.get_download_url(&storage_key).await {
                Ok(url) => url,
                Err(e) => {
                    errors.push(format!("Failed to get download URL for {}: {}", audio_file.id, e));
                    continue;
                }
            };

            // Download the file
            let client = reqwest::Client::new();
            let response = match client.get(&download_url.url).send().await {
                Ok(r) => r,
                Err(e) => {
                    errors.push(format!("Failed to download {}: {}", audio_file.id, e));
                    continue;
                }
            };

            if !response.status().is_success() {
                errors.push(format!(
                    "Download failed for {}: HTTP {}",
                    audio_file.id,
                    response.status()
                ));
                continue;
            }

            let bytes = match response.bytes().await {
                Ok(b) => b,
                Err(e) => {
                    errors.push(format!("Failed to read download response for {}: {}", audio_file.id, e));
                    continue;
                }
            };

            // Save to local file
            if let Err(e) = tokio::fs::write(&local_path, &bytes).await {
                errors.push(format!("Failed to save {} to {}: {}", audio_file.id, local_path.display(), e));
                continue;
            }

            tracing::info!(
                audio_id = %audio_file.id,
                size_bytes = bytes.len(),
                "Downloaded audio file from cloud storage"
            );
            downloaded += 1;
        }

        (downloaded, errors)
    }

    /// Stub for when file-storage feature is not enabled
    #[cfg(not(feature = "file-storage"))]
    pub async fn upload_audio_files_to_cloud(
        &self,
        _audiofile_directory: &std::path::Path,
    ) -> (usize, Vec<String>) {
        (0, Vec::new())
    }

    /// Stub for when file-storage feature is not enabled
    #[cfg(not(feature = "file-storage"))]
    pub async fn download_audio_files_from_cloud(
        &self,
        _audiofile_directory: &std::path::Path,
    ) -> (usize, Vec<String>) {
        (0, Vec::new())
    }
}

/// Sync with all configured peers
pub async fn sync_all_peers(
    db: Arc<Mutex<Database>>,
    config: Arc<Mutex<Config>>,
) -> HashMap<String, SyncResult> {
    let client = match SyncClient::new(db, config.clone()) {
        Ok(c) => c,
        Err(_) => return HashMap::new(),
    };

    let peers: Vec<String> = {
        let cfg = config.lock().unwrap();
        cfg.peers().iter().map(|p| p.peer_id.clone()).collect()
    };

    let mut results = HashMap::new();
    for peer_id in peers {
        let result = client.sync_with_peer(&peer_id).await;
        results.insert(peer_id, result);
    }

    results
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_db_and_config() -> (Arc<Mutex<Database>>, Arc<Mutex<Config>>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = Database::new(&db_path).unwrap();
        let config = Config::new(Some(temp_dir.path().to_path_buf())).unwrap();

        (
            Arc::new(Mutex::new(db)),
            Arc::new(Mutex::new(config)),
            temp_dir,
        )
    }

    mod sync_result_tests {
        use super::*;

        #[test]
        fn test_sync_result_success_default() {
            let result = SyncResult::success();
            assert!(result.success);
            assert_eq!(result.pulled, 0);
            assert_eq!(result.pushed, 0);
            assert_eq!(result.conflicts, 0);
            assert!(result.errors.is_empty());
        }

        #[test]
        fn test_sync_result_failure() {
            let result = SyncResult::failure("Test error".to_string());
            assert!(!result.success);
            assert_eq!(result.errors.len(), 1);
            assert_eq!(result.errors[0], "Test error");
        }

        #[test]
        fn test_sync_result_with_counts() {
            let mut result = SyncResult::success();
            result.pulled = 10;
            result.pushed = 5;
            result.conflicts = 2;

            assert!(result.success);
            assert_eq!(result.pulled, 10);
            assert_eq!(result.pushed, 5);
            assert_eq!(result.conflicts, 2);
        }

        #[test]
        fn test_sync_result_with_multiple_errors() {
            let mut result = SyncResult::failure("Error 1".to_string());
            result.errors.push("Error 2".to_string());
            result.errors.push("Error 3".to_string());

            assert!(!result.success);
            assert_eq!(result.errors.len(), 3);
        }
    }

    mod sync_client_init_tests {
        use super::*;

        #[test]
        fn test_client_creation() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config);
            assert!(client.is_ok());
        }

        #[test]
        fn test_client_has_device_info() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config).unwrap();

            // Device ID should be a valid hex string (32 chars = 16 bytes as hex)
            assert!(!client.device_id.is_empty());
            // Device name should be set
            assert!(!client.device_name.is_empty());
        }
    }

    mod clock_skew_tests {
        use super::*;

        #[test]
        fn test_calculate_clock_skew_no_timestamp() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config).unwrap();

            let skew = client.calculate_clock_skew(None);
            assert_eq!(skew, 0.0);
        }

        #[test]
        fn test_calculate_clock_skew_with_timestamp() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config).unwrap();

            // Server timestamp 100 seconds ahead of local
            let server_ts = Utc::now().timestamp() + 100;
            let skew = client.calculate_clock_skew(Some(server_ts));
            // Skew should be close to 100 (may vary slightly due to timing)
            assert!(skew > 99.0 && skew < 101.0);
        }

        #[test]
        fn test_adjust_timestamp_for_skew_none() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config).unwrap();

            let result = client.adjust_timestamp_for_skew(None, 0.0);
            assert!(result.is_none());
        }

        #[test]
        fn test_adjust_timestamp_for_skew_small_skew() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config).unwrap();

            // With small skew (< 1s), only apply 2 second base adjustment
            let ts = 1735689610; // 2025-01-01 12:00:10 UTC
            let result = client.adjust_timestamp_for_skew(Some(ts), 0.5);
            assert!(result.is_some());
            // Should be at least 2 seconds earlier
            let adjusted = result.unwrap();
            assert_eq!(adjusted, ts - 2); // Only base adjustment
        }

        #[test]
        fn test_adjust_timestamp_for_skew_large_skew() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config).unwrap();

            // With large skew (> 1s), apply base + 2x skew adjustment
            let ts = 1735689610; // 2025-01-01 12:00:10 UTC
            let result = client.adjust_timestamp_for_skew(Some(ts), 5.0);
            assert!(result.is_some());
            // Should be adjusted back by 2 + 2*5 = 12 seconds
            let adjusted = result.unwrap();
            assert_eq!(adjusted, ts - 12);
        }
    }

    mod sync_with_unknown_peer_tests {
        use super::*;

        #[tokio::test]
        async fn test_sync_with_unknown_peer() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config).unwrap();

            let result = client.sync_with_peer("00000000000070008000000000000099").await;
            assert!(!result.success);
            assert!(result.errors.iter().any(|e| e.contains("Unknown peer")));
        }

        #[tokio::test]
        async fn test_pull_from_unknown_peer() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config).unwrap();

            let result = client.pull_from_peer("00000000000070008000000000000099").await;
            assert!(!result.success);
            assert!(result.errors.iter().any(|e| e.contains("Unknown peer")));
        }

        #[tokio::test]
        async fn test_push_to_unknown_peer() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config).unwrap();

            let result = client.push_to_peer("00000000000070008000000000000099").await;
            assert!(!result.success);
            assert!(result.errors.iter().any(|e| e.contains("Unknown peer")));
        }

        #[tokio::test]
        async fn test_initial_sync_unknown_peer() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config).unwrap();

            let result = client.initial_sync("00000000000070008000000000000099").await;
            assert!(!result.success);
            assert!(result.errors.iter().any(|e| e.contains("Unknown peer")));
        }

        #[tokio::test]
        async fn test_check_peer_status_unknown_peer() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config).unwrap();

            let result = client.check_peer_status("00000000000070008000000000000099").await;
            // Should return None or error for unknown peer
            assert!(result.is_empty() || result.get("error").is_some());
        }
    }

    mod sync_all_peers_tests {
        use super::*;

        #[tokio::test]
        async fn test_sync_all_peers_empty() {
            let (db, config, _temp_dir) = create_test_db_and_config();

            let results = sync_all_peers(db, config).await;
            assert!(results.is_empty());
        }
    }

    mod get_changes_since_tests {
        use super::*;

        #[test]
        fn test_get_changes_since_empty_db() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config).unwrap();

            // Empty db still has system tags (_system, _marked, _nonsynced, _too-big)
            let changes = client.get_changes_since(None).unwrap();
            assert_eq!(changes.len(), 4);
            assert!(changes.iter().all(|c| c.entity_type == "tag"));
        }

        #[test]
        fn test_get_changes_since_with_future_timestamp() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config).unwrap();

            // Use far future timestamp - should return no changes
            let changes = client.get_changes_since(Some(4102444800)).unwrap(); // 2099-01-01
            assert!(changes.is_empty());
        }
    }

    mod convert_full_sync_tests {
        use super::*;

        #[test]
        fn test_convert_full_sync_empty() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config).unwrap();

            let full_sync = FullSyncResponse {
                notes: vec![],
                tags: vec![],
                note_tags: vec![],
                audio_files: None,
                note_attachments: None,
                transcriptions: None,
                device_id: "test".to_string(),
                device_name: Some("Test".to_string()),
                timestamp: 1735689600, // 2025-01-01 00:00:00 UTC
            };

            let changes = client.convert_full_sync_to_changes(&full_sync);
            assert!(changes.is_empty());
        }

        #[test]
        fn test_convert_full_sync_with_notes() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config).unwrap();

            let note = serde_json::json!({
                "id": "00000000000070008000000000000001",
                "content": "Test note",
                "created_at": 1735689600
            });

            let full_sync = FullSyncResponse {
                notes: vec![note],
                tags: vec![],
                note_tags: vec![],
                audio_files: None,
                note_attachments: None,
                transcriptions: None,
                device_id: "test".to_string(),
                device_name: Some("Test".to_string()),
                timestamp: 1735689600,
            };

            let changes = client.convert_full_sync_to_changes(&full_sync);
            assert_eq!(changes.len(), 1);
            assert_eq!(changes[0].entity_type, "note");
            assert_eq!(changes[0].operation, "create");
        }

        #[test]
        fn test_convert_full_sync_with_audio_files() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config).unwrap();

            let audio_file = serde_json::json!({
                "id": "00000000000070008000000000000002",
                "filename": "test.mp3",
                "imported_at": 1735689600
            });

            let full_sync = FullSyncResponse {
                notes: vec![],
                tags: vec![],
                note_tags: vec![],
                audio_files: Some(vec![audio_file]),
                note_attachments: None,
                transcriptions: None,
                device_id: "test".to_string(),
                device_name: Some("Test".to_string()),
                timestamp: 1735689600,
            };

            let changes = client.convert_full_sync_to_changes(&full_sync);
            assert_eq!(changes.len(), 1);
            assert_eq!(changes[0].entity_type, "audio_file");
        }
    }
}
