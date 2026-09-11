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

use chrono::Utc;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::config::Config;
use crate::database::Database;
use crate::error::{VoiceError, VoiceResult};
use crate::models::{audio_local_path, SyncChange};
use crate::UUID_SHORT_LEN;

/// Result of a sync operation
#[derive(Debug, Clone, Default)]
pub struct SyncResult {
    pub success: bool,
    pub pulled: i64,
    pub pushed: i64,
    pub conflicts: i64,
    /// Problems that made the sync incomplete or wrong (metadata level).
    pub errors: Vec<String>,
    /// Problems that did not affect the metadata sync, e.g. a cloud storage
    /// upload that could not be completed and will be retried next time.
    pub warnings: Vec<String>,
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
    /// Identity of the peer's database; a change voids our cursors
    #[serde(default)]
    database_id: Option<String>,
    /// End of the peer's feed at handshake time
    #[serde(default)]
    cursor: Option<i64>,
}

/// Sync batch response
#[derive(Debug, Deserialize)]
struct SyncBatchResponse {
    changes: Vec<SyncChange>,
    #[allow(dead_code)]
    from_timestamp: Option<i64>,
    #[allow(dead_code)]
    to_timestamp: Option<i64>,
    #[serde(default)]
    next_cursor: Option<i64>,
    #[serde(default)]
    #[allow(dead_code)]
    database_id: Option<String>,
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

/// Full sync response (complete dataset from peer). Kept for tools; the
/// client now pages the cursor feed from zero instead (see initial_sync).
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct FullSyncResponse {
    notes: Vec<serde_json::Value>,
    tags: Vec<serde_json::Value>,
    note_tags: Vec<serde_json::Value>,
    audio_files: Option<Vec<serde_json::Value>>,
    note_attachments: Option<Vec<serde_json::Value>>,
    transcriptions: Option<Vec<serde_json::Value>>,
    file_storage_config: Option<serde_json::Value>,
    field_versions: Option<Vec<serde_json::Value>>,
    device_id: String,
    device_name: Option<String>,
    timestamp: i64,
    #[serde(default)]
    cursor: Option<i64>,
    #[serde(default)]
    database_id: Option<String>,
}

/// Page size for the cursor feed, in both directions. Pages are fetched
/// until the peer reports the feed complete, so this only bounds one request.
const PULL_LIMIT: i64 = 10000;

/// Safety cap on pages per direction per sync (10000 * 1000 changes).
const MAX_PAGES: usize = 1000;

/// What an incremental pull produced.
struct PullOutcome {
    applied: i64,
    conflicts: i64,
    changes: Vec<SyncChange>,
    errors: Vec<String>,
    warnings: Vec<String>,
}

/// Cursor state for one peer, as stored in `sync_peers`.
#[derive(Debug, Clone, Default)]
struct PeerCursors {
    /// Our position in the peer's feed
    received: i64,
    /// Our own `seq` up to which the peer has everything
    sent: i64,
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
            // A page can be a few megabytes over a slow link
            .timeout(Duration::from_secs(180))
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

        // Step 0: Upload pending audio files to cloud storage FIRST
        // This ensures storage_provider/storage_key are set in the DB before
        // we gather local changes, so the metadata gets pushed to the server.
        // Cloud problems are warnings: the metadata sync must still proceed and
        // the upload is retried on the next sync.
        result.warnings.extend(self.upload_audio_files_to_cloud().await);

        // Step 1: Handshake, and find where we stand with this peer
        let handshake = match self.handshake(peer_url).await {
            Ok(h) => h,
            Err(e) => return SyncResult::failure(format!("Handshake failed: {}", e)),
        };
        let cursors = self.peer_cursors(peer_id, &handshake, &mut result);

        // Everything written locally up to here is what this sync pushes;
        // whatever the pull writes is the peer's own data coming back.
        let local_end = self.local_seq();

        // Step 2: Pull, page by page, saving the cursor after every page
        let pull = self.pull_all(peer_url, peer_id, &peer.peer_name, cursors.received).await;
        result.pulled = pull.applied;
        result.conflicts += pull.conflicts;
        result.errors.extend(pull.errors);
        result.warnings.extend(pull.warnings);

        // Step 2b: Mirror cloud audio files locally, only if this installation
        // opted in. Everyone else downloads on demand.
        result.warnings.extend(self.mirror_audio_files_from_cloud().await);

        // Step 3: Push our changes the peer has not seen, page by page
        let (pushed, conflicts, errors, warnings) = self.push_all(peer_url, peer_id, cursors.sent, local_end).await;
        result.pushed = pushed;
        result.conflicts += conflicts;
        result.errors.extend(errors);
        result.warnings.extend(warnings);

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

        let handshake = match self.handshake(peer_url).await {
            Ok(h) => h,
            Err(e) => return SyncResult::failure(format!("Handshake failed: {}", e)),
        };
        let cursors = self.peer_cursors(peer_id, &handshake, &mut result);

        let pull = self.pull_all(peer_url, peer_id, &peer.peer_name, cursors.received).await;
        result.pulled = pull.applied;
        result.conflicts = pull.conflicts;
        result.errors.extend(pull.errors);
        result.warnings.extend(pull.warnings);

        // Mirror cloud audio files locally if this installation opted in
        if result.errors.is_empty() {
            result.warnings.extend(self.mirror_audio_files_from_cloud().await);
        }

        if result.errors.is_empty() {
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

        // Step 0: Upload pending audio files to cloud storage FIRST
        // This ensures storage_provider/storage_key are included in the push
        result.warnings.extend(self.upload_audio_files_to_cloud().await);

        let handshake = match self.handshake(peer_url).await {
            Ok(h) => h,
            Err(e) => return SyncResult::failure(format!("Handshake failed: {}", e)),
        };
        let cursors = self.peer_cursors(peer_id, &handshake, &mut result);
        let local_end = self.local_seq();

        let (pushed, conflicts, errors, warnings) = self.push_all(peer_url, peer_id, cursors.sent, local_end).await;
        result.pushed = pushed;
        result.conflicts = conflicts;
        result.errors.extend(errors);
        result.warnings.extend(warnings);

        if result.errors.is_empty() {
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
    /// dataset from a peer rather than incremental changes. Afterwards the
    /// cursors point at the end of both feeds, so the next sync is incremental.
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
        let handshake = match self.handshake(peer_url).await {
            Ok(h) => h,
            Err(e) => return SyncResult::failure(format!("Handshake failed: {}", e)),
        };

        // Step 2: Pull the peer's whole feed from the beginning, page by
        // page. (One JSON document for the whole dataset, as /sync/full
        // returns, does not fit in memory for a large database; the paged
        // feed is resumable and bounded.)
        if let Err(e) = self.save_peer_cursors(peer_id, Some(0), Some(0), handshake.database_id.as_deref()) {
            result.errors.push(format!("Failed to reset cursors: {}", e));
        }
        let pull = self.pull_all(peer_url, peer_id, &peer.peer_name, 0).await;
        result.pulled = pull.applied;
        result.conflicts = pull.conflicts;
        result.errors.extend(pull.errors);
        result.warnings.extend(pull.warnings);

        // Step 4: Mirror cloud audio files locally if this installation opted in
        result.warnings.extend(self.mirror_audio_files_from_cloud().await);

        // Step 5: Upload pending audio files to cloud storage
        // This ensures storage_provider/storage_key are included in the push
        result.warnings.extend(self.upload_audio_files_to_cloud().await);

        // Step 6: Push everything we have (the peer de-duplicates)
        let local_end = self.local_seq();
        let (pushed, conflicts, errors, warnings) = self.push_all(peer_url, peer_id, 0, local_end).await;
        result.pushed = pushed;
        result.conflicts += conflicts;
        result.errors.extend(errors);
        result.warnings.extend(warnings);

        // Step 7: Update sync timestamp
        if let Err(e) = self.update_peer_sync_time(peer_id) {
            result.errors.push(format!("Failed to update sync time: {}", e));
        }

        result.success = result.errors.is_empty();
        result
    }

    /// Where we stand with a peer. If the peer's database identity changed
    /// (it was reset or replaced) both cursors restart from zero: everything
    /// is exchanged again, which is safe because applying is idempotent.
    fn peer_cursors(&self, peer_id: &str, handshake: &HandshakeResponse, result: &mut SyncResult) -> PeerCursors {
        let (received, sent, known_db) = {
            let db = self.db.lock().unwrap();
            db.get_peer_cursors(peer_id).unwrap_or((0, 0, None))
        };
        match (&handshake.database_id, &known_db) {
            (Some(now), Some(before)) if now != before => {
                let msg = format!(
                    "Peer {} has a new database ({} -> {}); exchanging everything again",
                    &peer_id[..UUID_SHORT_LEN.min(peer_id.len())],
                    &before[..UUID_SHORT_LEN.min(before.len())],
                    &now[..UUID_SHORT_LEN.min(now.len())]
                );
                tracing::warn!("{}", msg);
                result.warnings.push(msg);
                let _ = self.save_peer_cursors(peer_id, Some(0), Some(0), Some(now));
                PeerCursors { received: 0, sent: 0 }
            }
            (Some(now), None) => {
                let _ = self.save_peer_cursors(peer_id, None, None, Some(now));
                PeerCursors { received, sent }
            }
            _ => PeerCursors { received, sent },
        }
    }

    fn local_seq(&self) -> i64 {
        self.db.lock().ok().and_then(|db| db.current_seq().ok()).unwrap_or(0)
    }

    fn save_peer_cursors(&self, peer_id: &str, received: Option<i64>, sent: Option<i64>, database_id: Option<&str>) -> VoiceResult<()> {
        let peer_name = {
            let config = self.config.lock().unwrap();
            config.get_peer(peer_id).map(|p| p.peer_name.clone())
        };
        let db = self.db.lock().unwrap();
        db.set_peer_cursors(peer_id, peer_name.as_deref(), received, sent, database_id)
    }

    /// Pull every page after `cursor`, applying each and saving the cursor
    /// before fetching the next, so an interrupted sync resumes where it stopped.
    async fn pull_all(&self, peer_url: &str, peer_id: &str, peer_name: &str, mut cursor: i64) -> PullOutcome {
        let mut outcome = PullOutcome { applied: 0, conflicts: 0, changes: Vec::new(), errors: Vec::new(), warnings: Vec::new() };
        for page in 0..MAX_PAGES {
            match self.pull_page(peer_url, peer_id, peer_name, cursor).await {
                Ok((pull, next_cursor, complete)) => {
                    outcome.applied += pull.applied;
                    outcome.conflicts += pull.conflicts;
                    outcome.changes.extend(pull.changes);
                    outcome.errors.extend(pull.errors);
                    outcome.warnings.extend(pull.warnings);
                    cursor = next_cursor;
                    if let Err(e) = self.save_peer_cursors(peer_id, Some(cursor), None, None) {
                        outcome.errors.push(format!("Failed to save cursor: {}", e));
                        break;
                    }
                    if complete {
                        // Anything queued for retry (a row that arrived before
                        // the row it references) gets one more chance now,
                        // instead of waiting for the next sync.
                        if self.db.lock().map(|db| db.count_pending_sync_failures().unwrap_or(0)).unwrap_or(0) > 0 {
                            if let Ok((applied, conflicts, _)) = self.apply_changes_from(&[], peer_id, Some(peer_name)) {
                                outcome.applied += applied;
                                outcome.conflicts += conflicts;
                            }
                        }
                        break;
                    }
                    if page + 1 == MAX_PAGES {
                        outcome.warnings.push("Pull stopped after the page limit; run sync again to continue".to_string());
                    }
                }
                Err(e) => {
                    outcome.errors.push(format!("Pull failed: {}", e));
                    break;
                }
            }
        }
        outcome
    }

    /// Push our changes with `sent < seq <= upto`, page by page, saving the
    /// high-water mark after every page the peer accepted.
    async fn push_all(&self, peer_url: &str, peer_id: &str, mut sent: i64, upto: i64) -> (i64, i64, Vec<String>, Vec<String>) {
        let mut pushed = 0;
        let mut conflicts = 0;
        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        let mut server_queued = false;
        for _ in 0..MAX_PAGES {
            let (changes, next, complete) = {
                let db = self.db.lock().unwrap();
                match db.get_changes_after_seq_as_sync_changes(sent, Some(upto), PULL_LIMIT) {
                    Ok(page) => page,
                    Err(e) => {
                        errors.push(format!("Failed to get local changes: {}", e));
                        return (pushed, conflicts, errors, warnings);
                    }
                }
            };
            if changes.is_empty() {
                break;
            }
            let changes = self.stamp_origin(changes);
            let _ = &mut server_queued;
            tracing::debug!("Pushing {} changes to {}", changes.len(), &peer_id[..UUID_SHORT_LEN.min(peer_id.len())]);
            match self.push_changes_with_data(peer_url, &changes).await {
                Ok((applied, page_conflicts, server_errors)) => {
                    pushed += applied;
                    conflicts += page_conflicts;
                    server_queued |= !server_errors.is_empty();
                    warnings.extend(server_errors.into_iter().map(|e| format!("Server queued for retry: {}", e)));
                    sent = next;
                    if let Err(e) = self.save_peer_cursors(peer_id, None, Some(sent), None) {
                        errors.push(format!("Failed to save cursor: {}", e));
                        break;
                    }
                    if complete {
                        break;
                    }
                }
                Err(e) => {
                    tracing::warn!("Push error: {}", e);
                    errors.push(format!("Push failed: {}", e));
                    break;
                }
            }
        }
        // A passive server retries queued changes only when a batch arrives;
        // an empty batch now drains what this push left behind.
        if server_queued && errors.is_empty() {
            match self.push_changes_with_data_allow_empty(peer_url).await {
                Ok((applied, page_conflicts, still_failing)) => {
                    pushed += applied;
                    conflicts += page_conflicts;
                    if !still_failing.is_empty() {
                        warnings.push(format!("{} change(s) still queued on the peer", still_failing.len()));
                    }
                }
                Err(e) => warnings.push(format!("Could not ask the peer to retry queued changes: {}", e)),
            }
        }
        (pushed, conflicts, errors, warnings)
    }

    /// Post an empty batch: the peer retries whatever it queued.
    async fn push_changes_with_data_allow_empty(&self, peer_url: &str) -> VoiceResult<(i64, i64, Vec<String>)> {
        let request = ApplyRequest {
            device_id: self.device_id.clone(),
            device_name: self.device_name.clone(),
            changes: Vec::new(),
        };
        let response = self
            .client
            .post(format!("{}/sync/apply", peer_url))
            .json(&request)
            .send()
            .await
            .map_err(|e| VoiceError::Network(e.to_string()))?;
        if !response.status().is_success() {
            return Err(VoiceError::Sync(format!("Retry request failed with status {}", response.status())));
        }
        let result: ApplyResponse = response
            .json()
            .await
            .map_err(|e| VoiceError::Sync(format!("Failed to parse apply response: {}", e)))?;
        Ok((result.applied, result.conflicts, result.errors))
    }

    /// Fill in this device's identity on outgoing changes.
    fn stamp_origin(&self, changes: Vec<SyncChange>) -> Vec<SyncChange> {
        changes
            .into_iter()
            .map(|mut c| {
                if c.device_id.is_empty() {
                    c.device_id = self.device_id.clone();
                }
                if c.device_name.is_none() {
                    c.device_name = Some(self.device_name.clone());
                }
                c
            })
            .collect()
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
            protocol_version: "1.1".to_string(),
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

    /// One page of the peer's feed after `cursor`. Returns what was applied,
    /// the cursor to continue from, and whether the feed is exhausted.
    async fn pull_page(&self, peer_url: &str, peer_id: &str, peer_name: &str, cursor: i64) -> VoiceResult<(PullOutcome, i64, bool)> {
        let url = format!("{}/sync/changes?cursor={}&limit={}", peer_url, cursor, PULL_LIMIT);

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

        let next_cursor = match batch.next_cursor {
            Some(n) => n,
            None => {
                return Err(VoiceError::Sync(
                    "Peer does not support the cursor feed (protocol 1.1 or newer required)".to_string(),
                ))
            }
        };

        // Changes carry the sender's identity so that failures are queued
        // against the right peer
        let mut changes = batch.changes;
        for c in &mut changes {
            if c.device_id.is_empty() {
                c.device_id = batch.device_id.clone();
            }
            if c.device_name.is_none() {
                c.device_name = batch.device_name.clone().or_else(|| Some(peer_name.to_string()));
            }
        }

        tracing::debug!("Received {} changes from {} (cursor {} -> {})", changes.len(), &peer_id[..UUID_SHORT_LEN.min(peer_id.len())], cursor, next_cursor);
        for change in &changes {
            tracing::trace!("  Pull: {} {} from {}", change.entity_type, &change.entity_id[..UUID_SHORT_LEN.min(change.entity_id.len())], change.device_id);
        }

        let (applied, conflicts, errors) = self.apply_changes_from(&changes, peer_id, Some(peer_name))?;
        tracing::debug!("Applied {} changes, {} conflicts", applied, conflicts);

        Ok((
            PullOutcome { applied, conflicts, changes, errors, warnings: Vec::new() },
            next_cursor,
            batch.is_complete,
        ))
    }

    fn apply_changes_from(&self, changes: &[SyncChange], peer_id: &str, peer_name: Option<&str>) -> VoiceResult<(i64, i64, Vec<String>)> {
        let db = self.db.lock().unwrap();
        let sync_received_at = Utc::now().timestamp();
        let outcome = crate::sync_apply::apply_changes(&db, changes, peer_id, peer_name, sync_received_at)?;
        if outcome.retried_ok > 0 {
            tracing::info!("Applied {} previously failed changes", outcome.retried_ok);
        }
        Ok((outcome.applied, outcome.conflicts, outcome.errors))
    }

    fn apply_changes(&self, changes: &[SyncChange]) -> VoiceResult<(i64, i64, Vec<String>)> {
        let db = self.db.lock().unwrap();
        let sync_received_at = Utc::now().timestamp();
        // The peer is whoever sent the batch; the changes carry their origin device.
        let peer_id = changes
            .first()
            .map(|c| c.device_id.clone())
            .unwrap_or_else(|| self.device_id.clone());
        let peer_name = changes.first().and_then(|c| c.device_name.clone());
        let outcome = crate::sync_apply::apply_changes(&db, changes, &peer_id, peer_name.as_deref(), sync_received_at)?;
        if outcome.retried_ok > 0 {
            tracing::info!("Applied {} previously failed changes", outcome.retried_ok);
        }
        Ok((outcome.applied, outcome.conflicts, outcome.errors))
    }

    #[allow(dead_code)]
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
    #[allow(dead_code)]
    fn older_timestamp(a: Option<i64>, b: Option<i64>) -> Option<i64> {
        match (a, b) {
            (None, _) | (_, None) => None,
            (Some(ts_a), Some(ts_b)) => {
                Some(std::cmp::min(ts_a, ts_b))
            }
        }
    }

    #[allow(dead_code)]
    fn calculate_clock_skew(&self, server_timestamp: Option<i64>) -> f64 {
        if let Some(server_ts) = server_timestamp {
            let local_time = Utc::now().timestamp();
            return (server_ts - local_time) as f64;
        }
        0.0
    }

    #[allow(dead_code)]
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
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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

        // Convert field_versions (immutable history)
        if let Some(versions) = &full_sync.field_versions {
            for v in versions {
                if let Some(id) = v.get("id").and_then(|x| x.as_str()) {
                    let timestamp = v.get("created_at").and_then(|x| x.as_i64()).unwrap_or(0);
                    changes.push(SyncChange {
                        entity_type: "field_version".to_string(),
                        entity_id: id.to_string(),
                        operation: "create".to_string(),
                        data: v.clone(),
                        timestamp,
                        device_id: full_sync.device_id.clone(),
                        device_name: full_sync.device_name.clone(),
                    });
                }
            }
        }

        // Convert file_storage_config (single entity)
        if let Some(config) = &full_sync.file_storage_config {
            let timestamp = config
                .get("modified_at")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);

            changes.push(SyncChange {
                entity_type: "file_storage_config".to_string(),
                entity_id: "default".to_string(),
                operation: "update".to_string(),
                data: config.clone(),
                timestamp,
                device_id: full_sync.device_id.clone(),
                device_name: full_sync.device_name.clone(),
            });
        }

        changes
    }

    /// Push pre-fetched changes to peer (used by initial_sync)
    async fn push_changes_with_data(
        &self,
        peer_url: &str,
        changes: &[SyncChange],
    ) -> VoiceResult<(i64, i64, Vec<String>)> {
        if changes.is_empty() {
            return Ok((0, 0, Vec::new()));
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

        for err in &result.errors {
            tracing::warn!("Server could not apply a change (queued there for retry): {}", err);
        }

        Ok((result.applied, result.conflicts, result.errors))
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

            let dest_path = audio_local_path(audiofile_directory, audio_id, filename);

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

            let dest_path = audio_local_path(audiofile_directory, audio_id, &audio_file.filename);

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

            let source_path = audio_local_path(audiofile_directory, audio_id, filename);

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

    /// Audio file directory from config, if configured.
    fn audiofile_directory(&self) -> Option<std::path::PathBuf> {
        let config = self.config.lock().ok()?;
        config.audiofile_directory().map(std::path::PathBuf::from)
    }

    /// Upload pending audio files to cloud storage.
    ///
    /// Runs automatically before every push. Returns warnings only: a missing
    /// audio directory or storage configuration is not a problem, and any
    /// upload failure is retried on the next sync because the record stays
    /// pending (`storage_provider IS NULL`).
    #[cfg(feature = "file-storage")]
    pub async fn upload_audio_files_to_cloud(&self) -> Vec<String> {
        use crate::file_storage::{create_storage_service, upload_pending_audio_files};

        let dir = match self.audiofile_directory() {
            Some(d) => d,
            None => return Vec::new(),
        };

        let db = match self.db.lock() {
            Ok(db) => db,
            Err(_) => return vec!["Failed to lock database".to_string()],
        };

        match create_storage_service(&db) {
            Ok(Some(_)) => {}
            Ok(None) => {
                tracing::debug!("Cloud storage not configured, skipping upload");
                return Vec::new();
            }
            Err(e) => return vec![format!("Cloud storage configuration problem: {}", e)],
        }

        match upload_pending_audio_files(&db, &dir).await {
            Ok(result) => {
                if result.uploaded > 0 {
                    tracing::info!("Uploaded {} audio files to cloud storage", result.uploaded);
                }
                if result.skipped > 0 {
                    tracing::debug!(
                        "{} pending audio files are not on this device and were left for their owner",
                        result.skipped
                    );
                }
                if result.failed > 0 {
                    tracing::warn!("Failed to upload {} audio files to cloud storage", result.failed);
                }
                result.errors
            }
            Err(e) => {
                let msg = format!("Cloud storage upload failed: {}", e);
                tracing::error!("{}", msg);
                vec![msg]
            }
        }
    }

    /// Download every cloud audio file that is missing locally, if this
    /// installation is configured to mirror the cloud bucket
    /// (`sync.mirror_audio_files`). Otherwise does nothing. Returns warnings.
    #[cfg(feature = "file-storage")]
    pub async fn mirror_audio_files_from_cloud(&self) -> Vec<String> {
        let mirror = self.config.lock().map(|c| c.mirror_audio_files()).unwrap_or(false);
        if !mirror {
            return Vec::new();
        }
        let dir = match self.audiofile_directory() {
            Some(d) => d,
            None => return vec!["mirror_audio_files is enabled but audiofile_directory is not configured".to_string()],
        };
        match self.download_missing_audio_files_from_cloud(&dir).await {
            Ok(result) => result.errors,
            Err(e) => vec![format!("Cloud storage mirror failed: {}", e)],
        }
    }

    /// Download every non-deleted audio file that is in cloud storage but not
    /// in `audiofile_directory`. No-op when storage is not configured.
    #[cfg(feature = "file-storage")]
    pub async fn download_missing_audio_files_from_cloud(
        &self,
        audiofile_directory: &std::path::Path,
    ) -> Result<crate::file_storage::DownloadMissingResult, crate::file_storage::FileStorageError> {
        let db = self.db.lock().map_err(|_| {
            crate::file_storage::FileStorageError::Config("Failed to lock database".to_string())
        })?;
        let result = crate::file_storage::download_missing_audio_files(&db, audiofile_directory).await?;
        if result.downloaded > 0 {
            tracing::info!("Downloaded {} audio files from cloud storage", result.downloaded);
        }
        Ok(result)
    }

    /// Download a single audio file on demand.
    #[cfg(feature = "file-storage")]
    pub async fn download_audio_file_from_cloud(
        &self,
        audiofile_directory: &std::path::Path,
        audio_file_id: &str,
    ) -> Result<crate::file_storage::DownloadOutcome, crate::file_storage::FileStorageError> {
        let db = self.db.lock().map_err(|_| {
            crate::file_storage::FileStorageError::Config("Failed to lock database".to_string())
        })?;
        crate::file_storage::download_audio_file(&db, audiofile_directory, audio_file_id).await
    }

    /// Download all missing audio files attached to a note on demand.
    #[cfg(feature = "file-storage")]
    pub async fn download_audio_files_for_note_from_cloud(
        &self,
        audiofile_directory: &std::path::Path,
        note_id: &str,
    ) -> Result<crate::file_storage::DownloadMissingResult, crate::file_storage::FileStorageError> {
        let db = self.db.lock().map_err(|_| {
            crate::file_storage::FileStorageError::Config("Failed to lock database".to_string())
        })?;
        crate::file_storage::download_audio_files_for_note(&db, audiofile_directory, note_id).await
    }

    /// Stub for when file-storage feature is not enabled
    #[cfg(not(feature = "file-storage"))]
    pub async fn upload_audio_files_to_cloud(&self) -> Vec<String> {
        Vec::new()
    }

    /// Stub for when file-storage feature is not enabled
    #[cfg(not(feature = "file-storage"))]
    pub async fn mirror_audio_files_from_cloud(&self) -> Vec<String> {
        Vec::new()
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

            // Empty db still has system tags (_system, _marked, _nonsynced, _too-big),
            // each with its name and parent version
            let changes = client.get_changes_since(None).unwrap();
            let tags = changes.iter().filter(|c| c.entity_type == "tag").count();
            let versions = changes.iter().filter(|c| c.entity_type == "field_version").count();
            assert_eq!(tags, 4);
            assert_eq!(versions, 8);
            assert_eq!(changes.len(), 12);
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
                file_storage_config: None,
                field_versions: None,
                device_id: "test".to_string(),
                device_name: Some("Test".to_string()),
                timestamp: 1735689600,
                cursor: None,
                database_id: None, // 2025-01-01 00:00:00 UTC
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
                file_storage_config: None,
                field_versions: None,
                device_id: "test".to_string(),
                device_name: Some("Test".to_string()),
                timestamp: 1735689600,
                cursor: None,
                database_id: None,
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
                file_storage_config: None,
                field_versions: None,
                device_id: "test".to_string(),
                device_name: Some("Test".to_string()),
                timestamp: 1735689600,
                cursor: None,
                database_id: None,
            };

            let changes = client.convert_full_sync_to_changes(&full_sync);
            assert_eq!(changes.len(), 1);
            assert_eq!(changes[0].entity_type, "audio_file");
        }
    }
}
