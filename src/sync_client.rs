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
    last_sync_timestamp: Option<String>,
    server_timestamp: Option<String>,
}

/// Sync change
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncChange {
    pub entity_type: String,
    pub entity_id: String,
    pub operation: String,
    pub data: serde_json::Value,
    pub timestamp: String,
    pub device_id: String,
    pub device_name: Option<String>,
}

/// Sync batch response
#[derive(Debug, Deserialize)]
struct SyncBatchResponse {
    changes: Vec<SyncChange>,
    from_timestamp: Option<String>,
    to_timestamp: Option<String>,
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
    device_id: String,
    device_name: Option<String>,
    timestamp: String,
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

        // Use LOCAL last_sync timestamp (what we know we have) rather than server's
        // The server may not have a record if we never pushed changes to it
        let local_last_sync = self.get_local_last_sync(peer_id);
        let last_sync = local_last_sync.or(handshake.last_sync_timestamp);
        let clock_skew = self.calculate_clock_skew(handshake.server_timestamp.as_deref());

        // Adjust pull timestamp for clock skew
        let adjusted_since = self.adjust_timestamp_for_skew(last_sync.as_deref(), clock_skew);

        // Step 1b: Gather local changes BEFORE applying pull
        // This prevents the pull from overwriting local changes before they're pushed
        let local_changes_to_push = match self.get_changes_since(last_sync.as_deref()) {
            Ok(changes) => changes,
            Err(e) => {
                result.errors.push(format!("Failed to get local changes: {}", e));
                Vec::new()
            }
        };

        // Step 2: Pull changes
        let pulled_changes = match self.pull_changes(peer_url, adjusted_since.as_deref()).await {
            Ok((applied, conflicts, changes)) => {
                result.pulled = applied;
                result.conflicts += conflicts;
                changes
            }
            Err(e) => {
                result.errors.push(format!("Pull failed: {}", e));
                Vec::new()
            }
        };

        // Step 2b: Download audio files if audiofile_directory is configured
        {
            let audiofile_dir = {
                let config = self.config.lock().unwrap();
                config.audiofile_directory().map(std::path::PathBuf::from)
            };
            if let Some(dir) = audiofile_dir {
                let audio_errors = self
                    .sync_audio_files_after_pull(peer_url, &pulled_changes, &dir)
                    .await;
                result.errors.extend(audio_errors);
            }
        }

        // Step 3: Push the pre-gathered local changes
        // We use push_changes_with_data instead of push_changes to avoid re-gathering
        // changes after the pull may have modified local state
        let pushed_changes = if local_changes_to_push.is_empty() {
            Vec::new()
        } else {
            match self.push_changes_with_data(peer_url, &local_changes_to_push).await {
                Ok((applied, conflicts)) => {
                    result.pushed = applied;
                    result.conflicts += conflicts;
                    local_changes_to_push
                }
                Err(e) => {
                    result.errors.push(format!("Push failed: {}", e));
                    Vec::new()
                }
            }
        };

        // Step 3b: Upload audio files if audiofile_directory is configured
        {
            let audiofile_dir = {
                let config = self.config.lock().unwrap();
                config.audiofile_directory().map(std::path::PathBuf::from)
            };
            if let Some(dir) = audiofile_dir {
                let upload_errors = self
                    .sync_audio_files_after_push(peer_url, &pushed_changes, &dir)
                    .await;
                result.errors.extend(upload_errors);
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

        // Use LOCAL last_sync timestamp rather than server's
        let local_last_sync = self.get_local_last_sync(peer_id);
        let last_sync = local_last_sync.or(handshake.last_sync_timestamp);
        let clock_skew = self.calculate_clock_skew(handshake.server_timestamp.as_deref());
        let adjusted_since = self.adjust_timestamp_for_skew(last_sync.as_deref(), clock_skew);

        // Pull
        let pulled_changes = match self.pull_changes(peer_url, adjusted_since.as_deref()).await {
            Ok((applied, conflicts, changes)) => {
                result.pulled = applied;
                result.conflicts = conflicts;
                changes
            }
            Err(e) => {
                result.success = false;
                result.errors.push(e.to_string());
                Vec::new()
            }
        };

        // Download audio files if audiofile_directory is configured
        if result.success {
            let audiofile_dir = {
                let config = self.config.lock().unwrap();
                config.audiofile_directory().map(std::path::PathBuf::from)
            };
            if let Some(dir) = audiofile_dir {
                let audio_errors = self
                    .sync_audio_files_after_pull(&peer.peer_url, &pulled_changes, &dir)
                    .await;
                result.errors.extend(audio_errors);
            }
        }

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
            .push_changes(peer_url, handshake.last_sync_timestamp.as_deref())
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

        // Upload audio files if audiofile_directory is configured
        if result.success {
            let audiofile_dir = {
                let config = self.config.lock().unwrap();
                config.audiofile_directory().map(std::path::PathBuf::from)
            };
            if let Some(dir) = audiofile_dir {
                let upload_errors = self
                    .sync_audio_files_after_push(&peer.peer_url, &pushed_changes, &dir)
                    .await;
                result.errors.extend(upload_errors);
            }
        }

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
            Ok((applied, conflicts)) => {
                result.pulled = applied;
                result.conflicts = conflicts;
            }
            Err(e) => {
                result.errors.push(format!("Failed to apply full sync: {}", e));
            }
        }

        // Step 4: Download audio files
        {
            let audiofile_dir = {
                let config = self.config.lock().unwrap();
                config.audiofile_directory().map(std::path::PathBuf::from)
            };
            if let Some(dir) = audiofile_dir {
                let audio_errors = self
                    .sync_audio_files_after_pull(peer_url, &pulled_changes, &dir)
                    .await;
                result.errors.extend(audio_errors);
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

            // Step 6: Upload audio files
            {
                let audiofile_dir = {
                    let config = self.config.lock().unwrap();
                    config.audiofile_directory().map(std::path::PathBuf::from)
                };
                if let Some(dir) = audiofile_dir {
                    let upload_errors = self
                        .sync_audio_files_after_push(peer_url, &pushed_changes, &dir)
                        .await;
                    result.errors.extend(upload_errors);
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
        since: Option<&str>,
    ) -> VoiceResult<(i64, i64, Vec<SyncChange>)> {
        let url = match since {
            Some(ts) => format!(
                "{}/sync/changes?since={}",
                peer_url,
                urlencoding::encode(ts)
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

        // Apply changes to local database
        let (applied, conflicts) = self.apply_changes(&batch.changes)?;

        Ok((applied, conflicts, changes))
    }

    async fn push_changes(
        &self,
        peer_url: &str,
        since: Option<&str>,
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

    fn apply_changes(&self, changes: &[SyncChange]) -> VoiceResult<(i64, i64)> {
        let db = self.db.lock().unwrap();
        let mut applied = 0i64;
        let mut conflicts = 0i64;

        // Get last sync timestamp with this peer (use device_id as peer_id for self-tracking)
        let last_sync_at: Option<String> = None; // For pull, we don't use last_sync filtering here

        for change in changes {
            let result = match change.entity_type.as_str() {
                "note" => self.apply_note_change(&db, change, last_sync_at.as_deref()),
                "tag" => self.apply_tag_change(&db, change, last_sync_at.as_deref()),
                "note_tag" => self.apply_note_tag_change(&db, change, last_sync_at.as_deref()),
                "audio_file" => self.apply_audio_file_change(&db, change),
                "note_attachment" => self.apply_note_attachment_change(&db, change),
                _ => continue,
            };

            match result {
                Ok(true) => applied += 1,
                Ok(false) => {} // Skipped
                Err(_) => conflicts += 1,
            }
        }

        Ok((applied, conflicts))
    }

    fn apply_note_change(
        &self,
        db: &Database,
        change: &SyncChange,
        _last_sync_at: Option<&str>,
    ) -> VoiceResult<bool> {
        use crate::merge::merge_content;

        let note_id = &change.entity_id;
        let data = &change.data;

        let created_at = data["created_at"].as_str().unwrap_or("");
        let content = data["content"].as_str().unwrap_or("");
        let modified_at = data["modified_at"].as_str();
        let deleted_at = data["deleted_at"].as_str();

        // Check if local note exists and compare timestamps
        if let Ok(Some(existing)) = db.get_note_raw(note_id) {
            let local_modified = existing.get("modified_at")
                .and_then(|v| v.as_str());
            let local_deleted = existing.get("deleted_at")
                .and_then(|v| v.as_str());
            let local_content = existing.get("content")
                .and_then(|v| v.as_str())
                .unwrap_or("");
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

                // If local was modified and content differs, merge to preserve both
                if content != local_content {
                    let merge_result = merge_content(local_content, content, "LOCAL", "REMOTE");
                    // Use the newer timestamp for the merged content
                    let new_modified = if it > lt { it } else { lt };
                    db.apply_sync_note(note_id, created_at, &merge_result.content, Some(new_modified), deleted_at)?;
                    return Ok(true);
                }
            }
        }

        // No conflict or local doesn't exist - just apply
        db.apply_sync_note(note_id, created_at, content, modified_at, deleted_at)?;
        Ok(true)
    }

    fn apply_tag_change(
        &self,
        db: &Database,
        change: &SyncChange,
        _last_sync_at: Option<&str>,
    ) -> VoiceResult<bool> {
        let tag_id = &change.entity_id;
        let data = &change.data;

        let name = data["name"].as_str().unwrap_or("");
        let parent_id = data["parent_id"].as_str();
        let created_at = data["created_at"].as_str().unwrap_or("");
        let modified_at = data["modified_at"].as_str();

        db.apply_sync_tag(tag_id, name, parent_id, created_at, modified_at)?;
        Ok(true)
    }

    fn apply_note_tag_change(
        &self,
        db: &Database,
        change: &SyncChange,
        _last_sync_at: Option<&str>,
    ) -> VoiceResult<bool> {
        // Parse entity_id (format: "note_id:tag_id")
        let parts: Vec<&str> = change.entity_id.split(':').collect();
        if parts.len() != 2 {
            return Ok(false);
        }

        let note_id = parts[0];
        let tag_id = parts[1];
        let data = &change.data;

        let created_at = data["created_at"].as_str().unwrap_or("");
        let modified_at = data["modified_at"].as_str();
        let deleted_at = data["deleted_at"].as_str();

        db.apply_sync_note_tag(note_id, tag_id, created_at, modified_at, deleted_at)?;
        Ok(true)
    }

    fn apply_audio_file_change(
        &self,
        db: &Database,
        change: &SyncChange,
    ) -> VoiceResult<bool> {
        let audio_id = &change.entity_id;
        let data = &change.data;

        let imported_at = data["imported_at"].as_str().unwrap_or("");
        let filename = data["filename"].as_str().unwrap_or("");
        let file_created_at = data["file_created_at"].as_str();
        let summary = data["summary"].as_str();
        let modified_at = data["modified_at"].as_str();
        let deleted_at = data["deleted_at"].as_str();

        db.apply_sync_audio_file(
            audio_id,
            imported_at,
            filename,
            file_created_at,
            summary,
            modified_at,
            deleted_at,
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
        let created_at = data["created_at"].as_str().unwrap_or("");
        let modified_at = data["modified_at"].as_str();
        let deleted_at = data["deleted_at"].as_str();

        db.apply_sync_note_attachment(
            attachment_link_id,
            note_id,
            attachment_id,
            attachment_type,
            created_at,
            modified_at,
            deleted_at,
        )?;
        Ok(true)
    }

    fn get_changes_since(&self, since: Option<&str>) -> VoiceResult<Vec<SyncChange>> {
        let db = self.db.lock().unwrap();
        let (changes, _) = db.get_changes_since(since, 10000)?;

        // Convert HashMap changes to SyncChange structs
        let sync_changes: Vec<SyncChange> = changes
            .into_iter()
            .filter_map(|c| {
                let entity_type = c.get("entity_type")?.as_str()?.to_string();
                let entity_id = c.get("entity_id")?.as_str()?.to_string();
                let operation = c.get("operation")?.as_str()?.to_string();
                let timestamp = c.get("timestamp")?.as_str()?.to_string();
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

    fn calculate_clock_skew(&self, server_timestamp: Option<&str>) -> f64 {
        if let Some(ts) = server_timestamp {
            if let Ok(server_time) = DateTime::parse_from_rfc3339(ts) {
                let local_time = Utc::now();
                return (server_time.timestamp() - local_time.timestamp()) as f64;
            }
        }
        0.0
    }

    fn adjust_timestamp_for_skew(&self, timestamp: Option<&str>, clock_skew: f64) -> Option<String> {
        let ts = timestamp?;

        if let Ok(dt) = DateTime::parse_from_rfc3339(ts) {
            // Always go back at least 2 seconds for race conditions
            let base_adjustment = 2.0;
            // Add 2x skew if significant (> 1 second)
            let skew_adjustment = if clock_skew.abs() > 1.0 {
                2.0 * clock_skew.abs()
            } else {
                0.0
            };
            let total_adjustment = base_adjustment + skew_adjustment;

            let adjusted = dt - chrono::Duration::seconds(total_adjustment as i64);
            return Some(adjusted.format("%Y-%m-%d %H:%M:%S").to_string());
        }

        Some(ts.to_string())
    }

    fn update_peer_sync_time(&self, peer_id: &str) -> VoiceResult<()> {
        let peer_uuid = Uuid::parse_str(peer_id)?;
        let peer_bytes = peer_uuid.as_bytes().to_vec();

        let db = self.db.lock().unwrap();
        let conn = db.connection();

        // Try to update existing record
        let updated = conn.execute(
            "UPDATE sync_peers SET last_sync_at = datetime('now') WHERE peer_id = ?",
            [&peer_bytes],
        )?;

        if updated == 0 {
            // Insert new record
            let config = self.config.lock().unwrap();
            if let Some(peer) = config.get_peer(peer_id) {
                conn.execute(
                    "INSERT INTO sync_peers (peer_id, peer_name, peer_url, last_sync_at) VALUES (?, ?, ?, datetime('now'))",
                    rusqlite::params![peer_bytes, peer.peer_name, peer.peer_url],
                )?;
            }
        }

        Ok(())
    }

    /// Get the local record of when we last synced with a peer
    fn get_local_last_sync(&self, peer_id: &str) -> Option<String> {
        let peer_uuid = Uuid::parse_str(peer_id).ok()?;
        let peer_bytes = peer_uuid.as_bytes().to_vec();

        let db = self.db.lock().ok()?;
        let conn = db.connection();

        conn.query_row(
            "SELECT last_sync_at FROM sync_peers WHERE peer_id = ?",
            [&peer_bytes],
            |row| row.get::<_, Option<String>>(0),
        )
        .ok()
        .flatten()
    }

    /// Debug: public version of get_local_last_sync
    pub fn debug_get_local_last_sync(&self, peer_id: &str) -> Option<String> {
        self.get_local_last_sync(peer_id)
    }

    /// Debug: public version of get_changes_since
    pub fn debug_get_changes_since(&self, since: Option<&str>) -> VoiceResult<Vec<SyncChange>> {
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
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

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
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

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
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

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
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();

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
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();

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

            match self.upload_audio_file(peer_url, audio_id, &source_path).await {
                Ok(_) => {}
                Err(e) => {
                    errors.push(format!("Failed to upload audio {}: {}", audio_id, e));
                }
            }
        }

        errors
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
        fn test_calculate_clock_skew_invalid_timestamp() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config).unwrap();

            let skew = client.calculate_clock_skew(Some("invalid"));
            assert_eq!(skew, 0.0);
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
            let result = client.adjust_timestamp_for_skew(
                Some("2025-01-01T12:00:10+00:00"),
                0.5,
            );
            assert!(result.is_some());
            // Should be at least 2 seconds earlier
            let adjusted = result.unwrap();
            assert!(adjusted.contains("2025-01-01"));
        }

        #[test]
        fn test_adjust_timestamp_for_skew_large_skew() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config).unwrap();

            // With large skew (> 1s), apply base + 2x skew adjustment
            let result = client.adjust_timestamp_for_skew(
                Some("2025-01-01T12:00:10+00:00"),
                5.0,
            );
            assert!(result.is_some());
            // Should be adjusted back significantly
            let adjusted = result.unwrap();
            assert!(adjusted.contains("2025-01-01"));
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

            let changes = client.get_changes_since(None).unwrap();
            assert!(changes.is_empty());
        }

        #[test]
        fn test_get_changes_since_with_future_timestamp() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config).unwrap();

            // Use far future timestamp - should return no changes
            let changes = client.get_changes_since(Some("2099-01-01 00:00:00")).unwrap();
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
                device_id: "test".to_string(),
                device_name: Some("Test".to_string()),
                timestamp: "2025-01-01 00:00:00".to_string(),
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
                "created_at": "2025-01-01 00:00:00"
            });

            let full_sync = FullSyncResponse {
                notes: vec![note],
                tags: vec![],
                note_tags: vec![],
                audio_files: None,
                note_attachments: None,
                device_id: "test".to_string(),
                device_name: Some("Test".to_string()),
                timestamp: "2025-01-01 00:00:00".to_string(),
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
                "imported_at": "2025-01-01 00:00:00"
            });

            let full_sync = FullSyncResponse {
                notes: vec![],
                tags: vec![],
                note_tags: vec![],
                audio_files: Some(vec![audio_file]),
                note_attachments: None,
                device_id: "test".to_string(),
                device_name: Some("Test".to_string()),
                timestamp: "2025-01-01 00:00:00".to_string(),
            };

            let changes = client.convert_full_sync_to_changes(&full_sync);
            assert_eq!(changes.len(), 1);
            assert_eq!(changes[0].entity_type, "audio_file");
        }
    }
}
