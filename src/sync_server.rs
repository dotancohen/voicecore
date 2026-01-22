//! Sync server implementation using Axum.
//!
//! This module provides the server side of the sync protocol:
//! - /sync/handshake - Exchange device info
//! - /sync/changes - Get changes since timestamp
//! - /sync/apply - Apply changes from peer
//! - /sync/full - Get full dataset for initial sync
//! - /sync/status - Health check

use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};

use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::config::Config;
use crate::database::Database;
use crate::error::VoiceResult;
use crate::merge::merge_content;
use crate::models::SyncChange;
use crate::UUID_SHORT_LEN;

/// Server shutdown handle
static SHUTDOWN_TX: OnceLock<Mutex<Option<oneshot::Sender<()>>>> = OnceLock::new();

/// Shared server state
#[derive(Clone)]
struct AppState {
    db: Arc<Mutex<Database>>,
    config: Arc<Mutex<Config>>,
    device_id: String,
    device_name: String,
}

// Request/Response types

#[derive(Debug, Deserialize)]
struct HandshakeRequest {
    device_id: String,
    device_name: String,
    protocol_version: String,
}

#[derive(Debug, Serialize)]
struct HandshakeResponse {
    device_id: String,
    device_name: String,
    protocol_version: String,
    last_sync_timestamp: Option<i64>,
    server_timestamp: i64,
    supports_audiofiles: bool,
}

#[derive(Debug, Deserialize)]
struct ChangesQuery {
    since: Option<i64>,
    limit: Option<i64>,
}

#[derive(Debug, Serialize)]
struct ChangesResponse {
    changes: Vec<SyncChange>,
    from_timestamp: Option<i64>,
    to_timestamp: Option<i64>,
    device_id: String,
    device_name: String,
    is_complete: bool,
}

#[derive(Debug, Deserialize)]
struct ApplyRequest {
    device_id: String,
    device_name: String,
    changes: Vec<SyncChange>,
}

#[derive(Debug, Serialize)]
struct ApplyResponse {
    applied: i64,
    conflicts: i64,
    errors: Vec<String>,
}

#[derive(Debug, Serialize)]
struct StatusResponse {
    device_id: String,
    device_name: String,
    protocol_version: String,
    status: String,
    supports_audiofiles: bool,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

// Route handlers

async fn handshake(
    State(state): State<AppState>,
    Json(request): Json<HandshakeRequest>,
) -> impl IntoResponse {
    tracing::debug!(
        "Handshake from device_id={}... device_name={}",
        &request.device_id[..UUID_SHORT_LEN.min(request.device_id.len())],
        request.device_name
    );

    // Validate device_id
    if request.device_id.len() != 32 || !request.device_id.chars().all(|c| c.is_ascii_hexdigit()) {
        tracing::warn!("Invalid device_id format: {}", request.device_id);
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Invalid device_id format".to_string(),
            }),
        )
            .into_response();
    }

    // Get last sync timestamp for this peer
    let last_sync = get_peer_last_sync(&state.db, &request.device_id);
    tracing::debug!("Last sync with this peer: {:?}", last_sync);

    // Check if audiofile_directory is configured
    let supports_audiofiles = {
        let config = state.config.lock().ok();
        config.map(|c| c.audiofile_directory().is_some()).unwrap_or(false)
    };

    let response = HandshakeResponse {
        device_id: state.device_id.clone(),
        device_name: state.device_name.clone(),
        protocol_version: "1.0".to_string(),
        last_sync_timestamp: last_sync,
        server_timestamp: Utc::now().timestamp(),
        supports_audiofiles,
    };

    Json(response).into_response()
}

async fn get_changes(
    State(state): State<AppState>,
    Query(query): Query<ChangesQuery>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(1000).min(10000);
    tracing::debug!("GET /sync/changes since={:?} limit={}", query.since, limit);

    // Get changes from database
    let (changes, latest_timestamp) = {
        let db = state.db.lock().unwrap();
        match db.get_changes_since_as_sync_changes(query.since, limit) {
            Ok(result) => result,
            Err(e) => {
                tracing::error!("Failed to get changes: {}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: e.to_string(),
                    }),
                )
                    .into_response();
            }
        }
    };

    tracing::debug!(
        "Returning {} changes, to_timestamp={:?}",
        changes.len(),
        latest_timestamp
    );
    for change in &changes {
        tracing::trace!(
            "  {} {} {}",
            change.entity_type,
            &change.entity_id[..UUID_SHORT_LEN.min(change.entity_id.len())],
            change.operation
        );
    }

    let response = ChangesResponse {
        changes: changes.clone(),
        from_timestamp: query.since,
        to_timestamp: latest_timestamp,
        device_id: state.device_id.clone(),
        device_name: state.device_name.clone(),
        is_complete: (changes.len() as i64) < limit,
    };

    Json(response).into_response()
}

async fn apply_changes(
    State(state): State<AppState>,
    Json(request): Json<ApplyRequest>,
) -> impl IntoResponse {
    tracing::debug!(
        "POST /sync/apply from device_id={}... ({} changes)",
        &request.device_id[..UUID_SHORT_LEN.min(request.device_id.len())],
        request.changes.len()
    );
    for change in &request.changes {
        tracing::trace!(
            "  Incoming: {} {} {}",
            change.entity_type,
            &change.entity_id[..UUID_SHORT_LEN.min(change.entity_id.len())],
            change.operation
        );
    }

    // Validate device_id
    if request.device_id.len() != 32 || !request.device_id.chars().all(|c| c.is_ascii_hexdigit()) {
        tracing::warn!("Invalid device_id format: {}", request.device_id);
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Invalid device_id format".to_string(),
            }),
        )
            .into_response();
    }

    // Apply changes
    let (applied, conflicts, errors) = match apply_sync_changes(
        &state.db,
        &request.changes,
        &request.device_id,
        Some(request.device_name.as_str()),
        Some(&state.device_id),
        Some(&state.device_name),
    ) {
        Ok(result) => result,
        Err(e) => {
            tracing::error!("Failed to apply changes: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            )
                .into_response();
        }
    };

    tracing::debug!(
        "Applied {} changes, {} conflicts, {} errors",
        applied, conflicts, errors.len()
    );
    for err in &errors {
        tracing::warn!("  Error: {}", err);
    }

    // Update sync_peers to track when we last synced with this peer
    // This allows the handshake to return accurate last_sync_timestamp
    if let Ok(db) = state.db.lock() {
        let _ = db.update_peer_sync_time(&request.device_id, Some(&request.device_name));
    }

    let response = ApplyResponse {
        applied,
        conflicts,
        errors,
    };

    Json(response).into_response()
}

async fn get_full_sync(State(state): State<AppState>) -> impl IntoResponse {
    tracing::debug!("GET /sync/full (initial sync request)");

    // Get all notes, tags, and note_tags
    let mut data = match get_full_dataset(&state.db) {
        Ok(d) => d,
        Err(e) => {
            tracing::error!("Failed to get full dataset: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            )
                .into_response();
        }
    };

    // Log counts
    if let Some(obj) = data.as_object() {
        tracing::debug!(
            "Full sync: {} notes, {} tags, {} note_tags",
            obj.get("notes").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0),
            obj.get("tags").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0),
            obj.get("note_tags").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0)
        );
    }

    // Add required metadata fields that the client expects
    if let Some(obj) = data.as_object_mut() {
        obj.insert("device_id".to_string(), serde_json::Value::String(state.device_id.clone()));
        obj.insert("device_name".to_string(), serde_json::Value::String(state.device_name.clone()));
        obj.insert("timestamp".to_string(), serde_json::json!(chrono::Utc::now().timestamp()));
    }

    Json(data).into_response()
}

async fn status(State(state): State<AppState>) -> impl IntoResponse {
    // Check if audiofile_directory is configured
    let supports_audiofiles = {
        let config = state.config.lock().ok();
        config.map(|c| c.audiofile_directory().is_some()).unwrap_or(false)
    };

    Json(StatusResponse {
        device_id: state.device_id.clone(),
        device_name: state.device_name.clone(),
        protocol_version: "1.0".to_string(),
        status: "ok".to_string(),
        supports_audiofiles,
    })
}

/// Download an audio file
async fn download_audio_file(
    State(state): State<AppState>,
    Path(audio_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    tracing::debug!("GET /sync/audio/{}/file", &audio_id[..UUID_SHORT_LEN.min(audio_id.len())]);

    // Validate audio_id is a valid UUID
    let _uuid = Uuid::parse_str(&audio_id)
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid audio ID".to_string()))?;

    // Get audiofile_directory from config
    let audiofile_dir = {
        let config = state.config.lock()
            .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "Config lock error".to_string()))?;
        config.audiofile_directory().map(|s| s.to_string())
    };

    let audiofile_dir = audiofile_dir
        .ok_or_else(|| (StatusCode::NOT_FOUND, "audiofile_directory not configured".to_string()))?;

    // Look for file with any extension
    let dir_path = std::path::Path::new(&audiofile_dir);
    let pattern = format!("{}.*", audio_id);

    // Find the file
    let mut found_file: Option<std::path::PathBuf> = None;
    if let Ok(entries) = std::fs::read_dir(dir_path) {
        for entry in entries.flatten() {
            let file_name = entry.file_name();
            let name = file_name.to_string_lossy();
            if name.starts_with(&audio_id) && name.contains('.') {
                found_file = Some(entry.path());
                break;
            }
        }
    }

    let file_path = found_file
        .ok_or_else(|| (StatusCode::NOT_FOUND, format!("Audio file not found: {}", audio_id)))?;

    // Read file contents
    let contents = std::fs::read(&file_path)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to read file: {}", e)))?;

    Ok((StatusCode::OK, contents))
}

/// Upload an audio file
async fn upload_audio_file(
    State(state): State<AppState>,
    Path(audio_id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    tracing::debug!(
        "POST /sync/audio/{}/file ({} bytes)",
        &audio_id[..UUID_SHORT_LEN.min(audio_id.len())],
        body.len()
    );

    // Validate audio_id is a valid UUID
    let _uuid = Uuid::parse_str(&audio_id)
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid audio ID".to_string()))?;

    // Get audiofile_directory from config
    let audiofile_dir = {
        let config = state.config.lock()
            .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "Config lock error".to_string()))?;
        config.audiofile_directory().map(|s| s.to_string())
    };

    let audiofile_dir = audiofile_dir
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "audiofile_directory not configured".to_string()))?;

    // Get the extension from the database
    let extension = {
        let db = state.db.lock()
            .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "Database lock error".to_string()))?;

        let audio_file = db.get_audio_file(&audio_id)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Database error: {}", e)))?
            .ok_or_else(|| (StatusCode::NOT_FOUND, format!("Audio file record not found: {}", audio_id)))?;

        // Extract extension from filename
        audio_file.filename.rsplit('.').next()
            .map(|s| s.to_lowercase())
            .unwrap_or_else(|| "bin".to_string())
    };

    // Create audiofile_directory if it doesn't exist
    let dir_path = std::path::Path::new(&audiofile_dir);
    std::fs::create_dir_all(dir_path)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to create directory: {}", e)))?;

    // Write file
    let file_path = dir_path.join(format!("{}.{}", audio_id, extension));
    std::fs::write(&file_path, body.as_ref())
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to write file: {}", e)))?;

    Ok((StatusCode::OK, "OK"))
}

// Helper functions

fn get_peer_last_sync(db: &Arc<Mutex<Database>>, peer_id: &str) -> Option<i64> {
    let peer_uuid = Uuid::parse_str(peer_id).ok()?;
    let peer_bytes = peer_uuid.as_bytes().to_vec();

    let db = db.lock().ok()?;
    let conn = db.connection();

    conn.query_row(
        "SELECT last_sync_at FROM sync_peers WHERE peer_id = ?",
        [peer_bytes],
        |row| row.get::<_, Option<i64>>(0),
    )
    .ok()
    .flatten()
}


fn apply_sync_changes(
    db: &Arc<Mutex<Database>>,
    changes: &[SyncChange],
    peer_device_id: &str,
    peer_device_name: Option<&str>,
    local_device_id: Option<&str>,
    local_device_name: Option<&str>,
) -> VoiceResult<(i64, i64, Vec<String>)> {
    let db = db.lock().unwrap();
    let mut applied = 0i64;
    let mut conflicts = 0i64;
    let mut errors = Vec::new();

    // Get current Unix timestamp for sync_received_at
    // This is used to track when the server received the change (not when the change was made)
    let sync_received_at = Utc::now().timestamp();

    // Get last sync timestamp with this peer
    let last_sync_at = db.get_peer_last_sync(peer_device_id)?;
    tracing::trace!("Last sync with peer {}: {:?}", &peer_device_id[..UUID_SHORT_LEN.min(peer_device_id.len())], last_sync_at);

    for change in changes {
        let result = match change.entity_type.as_str() {
            "note" => apply_note_change(&db, change, last_sync_at, local_device_id, local_device_name, sync_received_at),
            "tag" => apply_tag_change(&db, change, last_sync_at, local_device_id, local_device_name, sync_received_at),
            "note_tag" => apply_note_tag_change(&db, change, last_sync_at, local_device_id, local_device_name, sync_received_at),
            "note_attachment" => apply_note_attachment_change(&db, change, last_sync_at, sync_received_at),
            "audio_file" => apply_audio_file_change(&db, change, last_sync_at, sync_received_at),
            "transcription" => apply_transcription_change(&db, change, last_sync_at, sync_received_at),
            _ => {
                tracing::warn!("Unknown entity type: {}", change.entity_type);
                errors.push(format!("Unknown entity type: {}", change.entity_type));
                continue;
            }
        };

        match result {
            Ok(ApplyResult::Applied) => {
                tracing::trace!(
                    "Applied: {} {} {}",
                    change.entity_type,
                    &change.entity_id[..UUID_SHORT_LEN.min(change.entity_id.len())],
                    change.operation
                );
                applied += 1;
            }
            Ok(ApplyResult::Conflict) => {
                tracing::debug!(
                    "Conflict: {} {} {}",
                    change.entity_type,
                    &change.entity_id[..UUID_SHORT_LEN.min(change.entity_id.len())],
                    change.operation
                );
                conflicts += 1;
            }
            Ok(ApplyResult::Skipped) => {
                tracing::trace!(
                    "Skipped: {} {} {}",
                    change.entity_type,
                    &change.entity_id[..UUID_SHORT_LEN.min(change.entity_id.len())],
                    change.operation
                );
            }
            Err(e) => {
                tracing::warn!(
                    "Error applying {} {}: {}",
                    change.entity_type, change.entity_id, e
                );
                errors.push(format!(
                    "Error applying {} {}: {}",
                    change.entity_type, change.entity_id, e
                ));
            }
        }
    }

    // Update peer's last sync timestamp
    db.update_peer_sync_time(peer_device_id, peer_device_name)?;

    Ok((applied, conflicts, errors))
}

#[derive(Debug, PartialEq)]
enum ApplyResult {
    Applied,
    Conflict,
    Skipped,
}

fn apply_note_change(
    db: &Database,
    change: &SyncChange,
    last_sync_at: Option<i64>,
    local_device_id: Option<&str>,
    local_device_name: Option<&str>,
    sync_received_at: i64,
) -> VoiceResult<ApplyResult> {
    let note_id = &change.entity_id;
    let data = &change.data;
    let remote_device_id = &change.device_id;
    let remote_device_name = change.device_name.as_deref();

    // Timestamps are now i64 (Unix seconds) - no string validation needed

    let existing = db.get_note_raw(note_id)?;

    match change.operation.as_str() {
        "create" => {
            // Check if existing note is deleted - if so, treat as conflict/resurrection
            if let Some(ref existing) = existing {
                let local_deleted = existing.get("deleted_at").and_then(|v| v.as_i64()).is_some();
                if local_deleted {
                    // Remote is creating (with deleted_at=None), local has deleted
                    // This is "local deleted, remote edited" scenario - resurrect with conflict
                    let remote_content = data["content"].as_str().unwrap_or("");
                    let modified_at = data["modified_at"].as_i64();
                    let local_deleted_at = existing.get("deleted_at").and_then(|v| v.as_i64()).unwrap_or(0);

                    // Create delete conflict for tracking
                    db.create_note_delete_conflict(
                        note_id,
                        remote_content,                          // surviving_content
                        modified_at.unwrap_or(0),                // surviving_modified_at
                        Some(remote_device_id.as_str()),         // surviving_device_id
                        remote_device_name,                      // surviving_device_name
                        None,                                    // deleted_content
                        local_deleted_at,                        // deleted_at
                        local_device_id,                         // deleting_device_id (local)
                        local_device_name,                       // deleting_device_name (local)
                    )?;

                    // Resurrect the note with remote content
                    db.apply_sync_note(
                        note_id,
                        data["created_at"].as_i64().unwrap_or(0),
                        remote_content,
                        modified_at,
                        None,  // Clear deleted_at to resurrect
                        Some(sync_received_at),
                    )?;

                    return Ok(ApplyResult::Conflict);
                }
                // Existing note is not deleted - skip duplicate create
                return Ok(ApplyResult::Skipped);
            }
            db.apply_sync_note(
                note_id,
                data["created_at"].as_i64().unwrap_or(0),
                data["content"].as_str().unwrap_or(""),
                data["modified_at"].as_i64(),
                data["deleted_at"].as_i64(),
                Some(sync_received_at),
            )?;
            Ok(ApplyResult::Applied)
        }
        "update" | "delete" => {
            let created_at = data["created_at"].as_i64().unwrap_or(0);
            let remote_content = data["content"].as_str().unwrap_or("");
            let modified_at = data["modified_at"].as_i64();
            let deleted_at = data["deleted_at"].as_i64();

            if existing.is_none() {
                db.apply_sync_note(note_id, created_at, remote_content, modified_at, deleted_at, Some(sync_received_at))?;
                return Ok(ApplyResult::Applied);
            }

            let existing = existing.unwrap();
            let local_content = existing.get("content")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let local_modified_at = existing.get("modified_at")
                .and_then(|v| v.as_i64());
            let local_deleted_at = existing.get("deleted_at")
                .and_then(|v| v.as_i64());

            // Check if local changed since last sync
            let local_time = local_modified_at.or(local_deleted_at);

            // Determine timestamp of incoming change (moved up for use in local_changed calc)
            // Include created_at as fallback for notes that were created but never modified
            let incoming_time = modified_at.or(deleted_at).or(Some(created_at));

            let local_changed = if let Some(last) = last_sync_at {
                // Have sync history - check if local changed since then
                local_time.map_or(false, |lt| lt > last)
            } else {
                // No sync history with this peer - this is initial sync
                // If content differs, we should treat it as a conflict to preserve both versions
                // This handles the case where a note was created locally but never synced
                let content_differs = !local_content.is_empty() && local_content != remote_content;
                if content_differs {
                    // Content differs with no sync history = conflict situation
                    true
                } else {
                    // Same content or empty local - compare timestamps
                    match (local_time, incoming_time) {
                        (Some(lt), Some(it)) => lt >= it,  // Local "changed" if >= incoming
                        (Some(_), None) => true,           // Local has time, incoming doesn't
                        (None, _) => false,                // No local time means no local change
                    }
                }
            };

            // If incoming change is before or at last_sync, skip
            if let (Some(last), Some(incoming)) = (last_sync_at, incoming_time) {
                if incoming <= last {
                    return Ok(ApplyResult::Skipped);
                }
            }

            if local_changed {
                // Both sides changed - create appropriate conflict
                let local_deleted = local_deleted_at.is_some();
                let remote_deleted = deleted_at.is_some();

                if local_deleted && !remote_deleted {
                    // Local deleted, remote edited - create delete conflict
                    // The remote side has surviving content
                    db.create_note_delete_conflict(
                        note_id,
                        remote_content,                          // surviving_content
                        modified_at.unwrap_or(0),                // surviving_modified_at
                        Some(remote_device_id.as_str()),         // surviving_device_id
                        remote_device_name,                      // surviving_device_name
                        None,                                    // deleted_content
                        local_deleted_at.unwrap_or(0),           // deleted_at
                        local_device_id,                         // deleting_device_id (local)
                        local_device_name,                       // deleting_device_name (local)
                    )?;
                    // Resurrect the note with remote content (clear deleted_at)
                    db.apply_sync_note(
                        note_id,
                        created_at,
                        remote_content,
                        modified_at,
                        None,  // Clear deleted_at to resurrect
                        Some(sync_received_at),
                    )?;
                    return Ok(ApplyResult::Conflict);
                } else if !local_deleted && remote_deleted {
                    // Local edited, remote deleted - create delete conflict
                    // The local side has surviving content
                    db.create_note_delete_conflict(
                        note_id,
                        local_content,                           // surviving_content
                        local_modified_at.unwrap_or(0),          // surviving_modified_at
                        local_device_id,                         // surviving_device_id (local)
                        local_device_name,                       // surviving_device_name (local)
                        None,                                    // deleted_content
                        deleted_at.unwrap_or(0),                 // deleted_at
                        Some(remote_device_id.as_str()),         // deleting_device_id
                        remote_device_name,                      // deleting_device_name
                    )?;
                    return Ok(ApplyResult::Conflict);
                } else if !local_deleted && !remote_deleted {
                    // Both edited - check if content is identical
                    if local_content == remote_content {
                        // Same content - no conflict needed, just apply to update timestamps
                        db.apply_sync_note(note_id, created_at, remote_content, modified_at, deleted_at, Some(sync_received_at))?;
                        return Ok(ApplyResult::Applied);
                    }
                    // Both sides have different content - merge with conflict markers
                    // This ensures no content is lost during sync
                    let merge_result = merge_content(
                        local_content,
                        remote_content,
                        "LOCAL",
                        "REMOTE",
                    );
                    let merged_content = &merge_result.content;
                    let now = Utc::now().timestamp();

                    // Create conflict record for resolution tracking
                    db.create_note_content_conflict(
                        note_id,
                        local_content,
                        local_modified_at.unwrap_or(0),
                        local_device_id,
                        local_device_name,
                        remote_content,
                        modified_at.unwrap_or(0),
                        Some(remote_device_id.as_str()),
                        remote_device_name,
                    )?;

                    // Apply merged content to note
                    db.apply_sync_note(note_id, created_at, merged_content, Some(now), deleted_at, Some(sync_received_at))?;
                    return Ok(ApplyResult::Conflict);
                }
                // Both deleted - no conflict, just apply
            }

            db.apply_sync_note(note_id, created_at, remote_content, modified_at, deleted_at, Some(sync_received_at))?;
            Ok(ApplyResult::Applied)
        }
        _ => Ok(ApplyResult::Skipped),
    }
}

fn apply_tag_change(
    db: &Database,
    change: &SyncChange,
    last_sync_at: Option<i64>,
    local_device_id: Option<&str>,
    local_device_name: Option<&str>,
    sync_received_at: i64,
) -> VoiceResult<ApplyResult> {
    let tag_id = &change.entity_id;
    let data = &change.data;
    let remote_device_id = &change.device_id;
    let remote_device_name = change.device_name.as_deref();

    // Timestamps are now i64 (Unix seconds) - no string validation needed

    let existing = db.get_tag_raw(tag_id)?;

    match change.operation.as_str() {
        "create" => {
            if existing.is_some() {
                return Ok(ApplyResult::Skipped);
            }
            db.apply_sync_tag(
                tag_id,
                data["name"].as_str().unwrap_or(""),
                data["parent_id"].as_str(),
                data["created_at"].as_i64().unwrap_or(0),
                data["modified_at"].as_i64(),
                Some(sync_received_at),
            )?;
            Ok(ApplyResult::Applied)
        }
        "update" => {
            let remote_name = data["name"].as_str().unwrap_or("");
            let remote_parent_id = data["parent_id"].as_str();
            let created_at = data["created_at"].as_i64().unwrap_or(0);
            let modified_at = data["modified_at"].as_i64();

            if existing.is_none() {
                db.apply_sync_tag(tag_id, remote_name, remote_parent_id, created_at, modified_at, Some(sync_received_at))?;
                return Ok(ApplyResult::Applied);
            }

            let existing = existing.unwrap();
            let local_name = existing.get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let local_parent_id = existing.get("parent_id")
                .and_then(|v| v.as_str());
            let local_modified_at = existing.get("modified_at")
                .and_then(|v| v.as_i64());

            // Check if local changed since last sync
            let local_changed = if let Some(last) = last_sync_at {
                // Have sync history - check if local changed since then
                local_modified_at.map_or(false, |lt| lt > last)
            } else {
                // No sync history - compare timestamps directly
                // Only "local changed" if local is newer or equal to incoming
                match (local_modified_at, modified_at) {
                    (Some(lt), Some(it)) => lt >= it,
                    (Some(_), None) => true,
                    (None, _) => false,
                }
            };

            // Check timestamp - skip if incoming is before last_sync
            if let (Some(last), Some(incoming)) = (last_sync_at, modified_at) {
                if incoming <= last {
                    return Ok(ApplyResult::Skipped);
                }
            }

            let mut has_name_conflict = false;
            let mut has_parent_conflict = false;
            let mut final_name = remote_name.to_string();
            let mut final_parent_id = remote_parent_id;

            // If local changed, check for conflicts
            if local_changed {
                // Check for name conflict
                if local_name != remote_name {
                    // Both renamed the tag differently
                    db.create_tag_rename_conflict(
                        tag_id,
                        local_name,
                        local_modified_at.unwrap_or(0),
                        local_device_id,
                        local_device_name,
                        remote_name,
                        modified_at.unwrap_or(0),
                        Some(remote_device_id.as_str()),
                        remote_device_name,
                    )?;
                    has_name_conflict = true;
                    // Combine both names so no rename is lost
                    final_name = format!("{} | {}", local_name, remote_name);
                }

                // Check for parent_id conflict
                if local_parent_id != remote_parent_id {
                    // Both moved the tag to different parents
                    db.create_tag_parent_conflict(
                        tag_id,
                        local_parent_id,
                        local_modified_at.unwrap_or(0),
                        local_device_id,
                        local_device_name,
                        remote_parent_id,
                        modified_at.unwrap_or(0),
                        Some(remote_device_id.as_str()),
                        remote_device_name,
                    )?;
                    has_parent_conflict = true;
                    // Keep local parent on conflict (user can resolve via UI)
                    final_parent_id = local_parent_id;
                }
            }

            let now = Utc::now().timestamp();
            let final_modified = if has_name_conflict || has_parent_conflict {
                Some(now)
            } else {
                modified_at
            };

            db.apply_sync_tag(tag_id, &final_name, final_parent_id, created_at, final_modified, Some(sync_received_at))?;

            if has_name_conflict || has_parent_conflict {
                return Ok(ApplyResult::Conflict);
            }
            Ok(ApplyResult::Applied)
        }
        "delete" => {
            // Tag deletion handling
            let deleted_at = data["deleted_at"].as_i64()
                .or_else(|| data["modified_at"].as_i64())
                .unwrap_or(0);

            if existing.is_none() {
                // Tag doesn't exist locally - nothing to delete
                return Ok(ApplyResult::Skipped);
            }

            let existing = existing.unwrap();
            let local_name = existing.get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let local_parent_id = existing.get("parent_id")
                .and_then(|v| v.as_str());
            let local_modified_at = existing.get("modified_at")
                .and_then(|v| v.as_i64());

            // Check if local changed since last sync
            let local_changed = if let Some(last) = last_sync_at {
                // Have sync history - check if local changed since then
                local_modified_at.map_or(false, |lt| lt > last)
            } else {
                // No sync history - compare timestamps directly
                // Only "local changed" if local is newer or equal to incoming delete
                match (local_modified_at, Some(deleted_at)) {
                    (Some(lt), Some(it)) => lt >= it,
                    (Some(_), None) => true,
                    (None, _) => false,
                }
            };

            if local_changed {
                // Local modified the tag, but remote wants to delete
                // Create a delete conflict - preserve the local version
                db.create_tag_delete_conflict(
                    tag_id,
                    local_name,                          // surviving_name
                    local_parent_id,                     // surviving_parent_id
                    local_modified_at.unwrap_or(0),      // surviving_modified_at
                    local_device_id,                     // surviving_device_id (local)
                    local_device_name,                   // surviving_device_name (local)
                    deleted_at,                          // deleted_at
                    Some(remote_device_id.as_str()),     // deleting_device_id
                    remote_device_name,                  // deleting_device_name
                )?;
                return Ok(ApplyResult::Conflict);
            }

            // No local changes - safe to delete (soft delete with original timestamp)
            let local_created_at = existing.get("created_at")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            db.apply_sync_tag_with_deleted(tag_id, local_name, local_parent_id, local_created_at, Some(deleted_at), Some(deleted_at), Some(sync_received_at))?;
            Ok(ApplyResult::Applied)
        }
        _ => Ok(ApplyResult::Skipped),
    }
}

fn apply_note_tag_change(
    db: &Database,
    change: &SyncChange,
    last_sync_at: Option<i64>,
    local_device_id: Option<&str>,
    local_device_name: Option<&str>,
    sync_received_at: i64,
) -> VoiceResult<ApplyResult> {
    // Parse entity_id (format: "note_id:tag_id")
    let parts: Vec<&str> = change.entity_id.split(':').collect();
    if parts.len() != 2 {
        tracing::warn!("note_tag: invalid entity_id format: {}", change.entity_id);
        return Ok(ApplyResult::Skipped);
    }

    let note_id = parts[0];
    let tag_id = parts[1];
    let data = &change.data;
    let remote_device_id = &change.device_id;
    let remote_device_name = change.device_name.as_deref();

    tracing::trace!(
        "note_tag: processing note={}... tag={}... op={}",
        &note_id[..UUID_SHORT_LEN.min(note_id.len())],
        &tag_id[..UUID_SHORT_LEN.min(tag_id.len())],
        change.operation
    );

    // Timestamps are now i64 (Unix seconds) - no string validation needed

    // Determine the timestamp of this incoming change
    let incoming_time = if change.operation == "delete" {
        data["deleted_at"].as_i64().or_else(|| data["modified_at"].as_i64())
    } else {
        data["modified_at"].as_i64().or_else(|| data["created_at"].as_i64())
    };

    // If this change happened before or at last_sync, skip it
    if let (Some(last), Some(incoming)) = (last_sync_at, incoming_time) {
        if incoming <= last {
            return Ok(ApplyResult::Skipped);
        }
    }

    let existing = db.get_note_tag_raw(note_id, tag_id)?;

    // Extract incoming timestamps first (needed for local_changed calculation)
    let created_at = data["created_at"].as_i64().unwrap_or(0);
    let modified_at = data["modified_at"].as_i64();
    let deleted_at = data["deleted_at"].as_i64();

    // Determine if local changed since last_sync
    let local_changed = if let Some(ref ex) = existing {
        let local_time = ex.get("modified_at")
            .and_then(|v| v.as_i64())
            .or_else(|| ex.get("deleted_at").and_then(|v| v.as_i64()))
            .or_else(|| ex.get("created_at").and_then(|v| v.as_i64()));
        let incoming_time = modified_at.or(deleted_at);

        if let Some(last) = last_sync_at {
            // Have sync history - check if local changed since then
            local_time.map_or(false, |lt| lt > last)
        } else {
            // No sync history - compare timestamps directly
            match (local_time, incoming_time) {
                (Some(lt), Some(it)) => lt >= it,
                (Some(_), None) => true,
                (None, _) => false,
            }
        }
    } else {
        false
    };

    match change.operation.as_str() {
        "create" => {
            if let Some(ref ex) = existing {
                if ex.get("deleted_at").and_then(|v| v.as_i64()).is_none() {
                    // Already active
                    return Ok(ApplyResult::Skipped);
                }
                // Local is deleted, remote wants active - reactivate
                let ex_created_at = ex.get("created_at").and_then(|v| v.as_i64()).unwrap_or(created_at);
                let local_modified_at = ex.get("modified_at").and_then(|v| v.as_i64());
                let local_deleted_at = ex.get("deleted_at").and_then(|v| v.as_i64());

                if local_changed {
                    // Create conflict record: local deleted, remote wants to reactivate
                    db.create_note_tag_conflict(
                        note_id,
                        tag_id,
                        Some(ex_created_at),
                        local_modified_at,
                        local_deleted_at,
                        local_device_id,
                        local_device_name,
                        Some(created_at),
                        modified_at,
                        None,  // remote is reactivating
                        Some(remote_device_id.as_str()),
                        remote_device_name,
                    )?;
                }

                db.apply_sync_note_tag(note_id, tag_id, ex_created_at, modified_at, None, Some(sync_received_at))?;
                return Ok(if local_changed { ApplyResult::Conflict } else { ApplyResult::Applied });
            }
            // New association
            db.apply_sync_note_tag(note_id, tag_id, created_at, modified_at, None, Some(sync_received_at))?;
            Ok(ApplyResult::Applied)
        }
        "delete" => {
            if existing.is_none() {
                // Create as deleted for sync consistency
                db.apply_sync_note_tag(note_id, tag_id, created_at, modified_at, deleted_at, Some(sync_received_at))?;
                return Ok(ApplyResult::Applied);
            }
            let ex = existing.unwrap();
            if ex.get("deleted_at").and_then(|v| v.as_i64()).is_some() {
                return Ok(ApplyResult::Skipped); // Already deleted
            }
            // Local is active, remote wants to delete
            if local_changed {
                // Both changed - favor preservation (keep active)
                let ex_created_at = ex.get("created_at").and_then(|v| v.as_i64());
                let local_modified_at = ex.get("modified_at").and_then(|v| v.as_i64());
                db.create_note_tag_conflict(
                    note_id,
                    tag_id,
                    ex_created_at,
                    local_modified_at,
                    None,  // local is active
                    local_device_id,
                    local_device_name,
                    Some(created_at),
                    modified_at,
                    deleted_at,  // remote wants to delete
                    Some(remote_device_id.as_str()),
                    remote_device_name,
                )?;
                return Ok(ApplyResult::Conflict);
            }
            // Apply the delete
            let ex_created_at = ex.get("created_at").and_then(|v| v.as_i64()).unwrap_or(created_at);
            db.apply_sync_note_tag(note_id, tag_id, ex_created_at, modified_at, deleted_at, Some(sync_received_at))?;
            Ok(ApplyResult::Applied)
        }
        "update" => {
            // Update operation - typically reactivation (deleted_at cleared)
            if existing.is_none() {
                db.apply_sync_note_tag(note_id, tag_id, created_at, modified_at, deleted_at, Some(sync_received_at))?;
                return Ok(ApplyResult::Applied);
            }

            let ex = existing.unwrap();
            let remote_deleted = deleted_at.is_some();
            let local_deleted = ex.get("deleted_at").and_then(|v| v.as_i64()).is_some();
            let ex_created_at = ex.get("created_at").and_then(|v| v.as_i64()).unwrap_or(created_at);

            if !remote_deleted && local_deleted {
                // Remote reactivated, local still deleted - reactivate
                if local_changed {
                    let local_modified_at = ex.get("modified_at").and_then(|v| v.as_i64());
                    let local_deleted_at = ex.get("deleted_at").and_then(|v| v.as_i64());
                    db.create_note_tag_conflict(
                        note_id,
                        tag_id,
                        Some(ex_created_at),
                        local_modified_at,
                        local_deleted_at,  // local is deleted
                        local_device_id,
                        local_device_name,
                        Some(created_at),
                        modified_at,
                        None,  // remote reactivating
                        Some(remote_device_id.as_str()),
                        remote_device_name,
                    )?;
                }
                db.apply_sync_note_tag(note_id, tag_id, ex_created_at, modified_at, None, Some(sync_received_at))?;
                return Ok(if local_changed { ApplyResult::Conflict } else { ApplyResult::Applied });
            }

            if remote_deleted && !local_deleted {
                // Remote wants to delete, local is active
                if local_changed {
                    let local_modified_at = ex.get("modified_at").and_then(|v| v.as_i64());
                    db.create_note_tag_conflict(
                        note_id,
                        tag_id,
                        Some(ex_created_at),
                        local_modified_at,
                        None,  // local is active
                        local_device_id,
                        local_device_name,
                        Some(created_at),
                        modified_at,
                        deleted_at,  // remote wants to delete
                        Some(remote_device_id.as_str()),
                        remote_device_name,
                    )?;
                    return Ok(ApplyResult::Conflict); // Keep active
                }
                db.apply_sync_note_tag(note_id, tag_id, ex_created_at, modified_at, deleted_at, Some(sync_received_at))?;
                return Ok(ApplyResult::Applied);
            }

            // Both have same deleted state - update timestamps
            db.apply_sync_note_tag(note_id, tag_id, ex_created_at, modified_at, deleted_at, Some(sync_received_at))?;
            Ok(ApplyResult::Applied)
        }
        _ => Ok(ApplyResult::Skipped),
    }
}

fn apply_note_attachment_change(
    db: &Database,
    change: &SyncChange,
    last_sync_at: Option<i64>,
    sync_received_at: i64,
) -> VoiceResult<ApplyResult> {
    let attachment_assoc_id = &change.entity_id;
    let data = &change.data;

    // Timestamps are now i64 (Unix seconds) - no string validation needed

    // Determine the timestamp of this incoming change
    let incoming_time = if change.operation == "delete" {
        data["deleted_at"].as_i64().or_else(|| data["modified_at"].as_i64())
    } else {
        data["modified_at"].as_i64().or_else(|| data["created_at"].as_i64())
    };

    // If this change happened before or at last_sync, skip it
    if let (Some(last), Some(incoming)) = (last_sync_at, incoming_time) {
        if incoming <= last {
            return Ok(ApplyResult::Skipped);
        }
    }

    let existing = db.get_note_attachment_raw(attachment_assoc_id)?;

    // Extract incoming data
    let id = data["id"].as_str().unwrap_or("");
    let note_id = data["note_id"].as_str().unwrap_or("");
    let attachment_id = data["attachment_id"].as_str().unwrap_or("");
    let attachment_type = data["attachment_type"].as_str().unwrap_or("");
    let created_at = data["created_at"].as_i64().unwrap_or(0);
    let modified_at = data["modified_at"].as_i64();
    let deleted_at = data["deleted_at"].as_i64();

    // Determine if local changed since last_sync
    let local_changed = if let Some(ref ex) = existing {
        let local_time = ex.get("modified_at")
            .and_then(|v| v.as_i64())
            .or_else(|| ex.get("deleted_at").and_then(|v| v.as_i64()))
            .or_else(|| ex.get("created_at").and_then(|v| v.as_i64()));
        let incoming_time = modified_at.or(deleted_at);

        if let Some(last) = last_sync_at {
            // Have sync history - check if local changed since then
            local_time.map_or(false, |lt| lt > last)
        } else {
            // No sync history - compare timestamps directly
            match (local_time, incoming_time) {
                (Some(lt), Some(it)) => lt >= it,
                (Some(_), None) => true,
                (None, _) => false,
            }
        }
    } else {
        false
    };

    match change.operation.as_str() {
        "create" => {
            if let Some(ref ex) = existing {
                if ex.get("deleted_at").and_then(|v| v.as_i64()).is_none() {
                    // Already active
                    return Ok(ApplyResult::Skipped);
                }
                // Local is deleted, remote wants active - reactivate
                db.apply_sync_note_attachment(id, note_id, attachment_id, attachment_type, created_at, modified_at, None, Some(sync_received_at))?;
                return Ok(if local_changed { ApplyResult::Conflict } else { ApplyResult::Applied });
            }
            // New association
            db.apply_sync_note_attachment(id, note_id, attachment_id, attachment_type, created_at, modified_at, None, Some(sync_received_at))?;
            Ok(ApplyResult::Applied)
        }
        "delete" => {
            if existing.is_none() {
                // Create as deleted for sync consistency
                db.apply_sync_note_attachment(id, note_id, attachment_id, attachment_type, created_at, modified_at, deleted_at, Some(sync_received_at))?;
                return Ok(ApplyResult::Applied);
            }
            let ex = existing.unwrap();
            if ex.get("deleted_at").and_then(|v| v.as_i64()).is_some() {
                return Ok(ApplyResult::Skipped); // Already deleted
            }
            // Local is active, remote wants to delete
            if local_changed {
                // Both changed - favor preservation (keep active)
                return Ok(ApplyResult::Conflict);
            }
            // Apply the delete
            db.apply_sync_note_attachment(id, note_id, attachment_id, attachment_type, created_at, modified_at, deleted_at, Some(sync_received_at))?;
            Ok(ApplyResult::Applied)
        }
        "update" => {
            if existing.is_none() {
                db.apply_sync_note_attachment(id, note_id, attachment_id, attachment_type, created_at, modified_at, deleted_at, Some(sync_received_at))?;
                return Ok(ApplyResult::Applied);
            }

            let ex = existing.unwrap();
            let remote_deleted = deleted_at.is_some();
            let local_deleted = ex.get("deleted_at").and_then(|v| v.as_i64()).is_some();

            if !remote_deleted && local_deleted {
                // Remote reactivated, local still deleted - reactivate
                db.apply_sync_note_attachment(id, note_id, attachment_id, attachment_type, created_at, modified_at, None, Some(sync_received_at))?;
                return Ok(if local_changed { ApplyResult::Conflict } else { ApplyResult::Applied });
            }

            if remote_deleted && !local_deleted {
                // Remote wants to delete, local is active
                if local_changed {
                    return Ok(ApplyResult::Conflict); // Keep active
                }
                db.apply_sync_note_attachment(id, note_id, attachment_id, attachment_type, created_at, modified_at, deleted_at, Some(sync_received_at))?;
                return Ok(ApplyResult::Applied);
            }

            // Both have same deleted state - update timestamps
            db.apply_sync_note_attachment(id, note_id, attachment_id, attachment_type, created_at, modified_at, deleted_at, Some(sync_received_at))?;
            Ok(ApplyResult::Applied)
        }
        _ => Ok(ApplyResult::Skipped),
    }
}

fn apply_audio_file_change(
    db: &Database,
    change: &SyncChange,
    last_sync_at: Option<i64>,
    sync_received_at: i64,
) -> VoiceResult<ApplyResult> {
    let audio_file_id = &change.entity_id;
    let data = &change.data;

    // Timestamps are now i64 (Unix seconds) - no string validation needed

    // Determine the timestamp of this incoming change
    let incoming_time = if change.operation == "delete" {
        data["deleted_at"].as_i64().or_else(|| data["modified_at"].as_i64())
    } else {
        data["modified_at"].as_i64().or_else(|| data["imported_at"].as_i64())
    };

    // If this change happened before or at last_sync, skip it
    if let (Some(last), Some(incoming)) = (last_sync_at, incoming_time) {
        if incoming <= last {
            return Ok(ApplyResult::Skipped);
        }
    }

    let existing = db.get_audio_file_raw(audio_file_id)?;

    // Extract incoming data
    let id = data["id"].as_str().unwrap_or("");
    let imported_at = data["imported_at"].as_i64().unwrap_or(0);
    let filename = data["filename"].as_str().unwrap_or("");
    let file_created_at = data["file_created_at"].as_i64();
    let duration_seconds = data["duration_seconds"].as_i64();
    let summary = data["summary"].as_str();
    let modified_at = data["modified_at"].as_i64();
    let deleted_at = data["deleted_at"].as_i64();

    // Determine if local changed since last_sync
    let local_changed = if let Some(ref ex) = existing {
        let local_time = ex.get("modified_at")
            .and_then(|v| v.as_i64())
            .or_else(|| ex.get("deleted_at").and_then(|v| v.as_i64()))
            .or_else(|| ex.get("imported_at").and_then(|v| v.as_i64()));
        let incoming_time = modified_at.or(deleted_at);

        if let Some(last) = last_sync_at {
            // Have sync history - check if local changed since then
            local_time.map_or(false, |lt| lt > last)
        } else {
            // No sync history - compare timestamps directly
            match (local_time, incoming_time) {
                (Some(lt), Some(it)) => lt >= it,
                (Some(_), None) => true,
                (None, _) => false,
            }
        }
    } else {
        false
    };

    match change.operation.as_str() {
        "create" => {
            if existing.is_some() {
                return Ok(ApplyResult::Skipped);
            }
            db.apply_sync_audio_file(id, imported_at, filename, file_created_at, duration_seconds, summary, modified_at, deleted_at, Some(sync_received_at))?;
            Ok(ApplyResult::Applied)
        }
        "update" | "delete" => {
            if existing.is_none() {
                db.apply_sync_audio_file(id, imported_at, filename, file_created_at, duration_seconds, summary, modified_at, deleted_at, Some(sync_received_at))?;
                return Ok(ApplyResult::Applied);
            }

            let existing = existing.unwrap();
            let local_deleted = existing.get("deleted_at").and_then(|v| v.as_i64()).is_some();
            let remote_deleted = deleted_at.is_some();

            // Audio files are simpler - no content conflicts (metadata + binary)
            // If both deleted, apply
            if local_deleted && remote_deleted {
                db.apply_sync_audio_file(id, imported_at, filename, file_created_at, duration_seconds, summary, modified_at, deleted_at, Some(sync_received_at))?;
                return Ok(ApplyResult::Applied);
            }

            // If local edited but remote deletes, create conflict (preserve local)
            if !local_deleted && remote_deleted && local_changed {
                return Ok(ApplyResult::Conflict);
            }

            // If local deleted but remote has updates (reactivation or edit)
            if local_deleted && !remote_deleted {
                db.apply_sync_audio_file(id, imported_at, filename, file_created_at, duration_seconds, summary, modified_at, deleted_at, Some(sync_received_at))?;
                return Ok(if local_changed { ApplyResult::Conflict } else { ApplyResult::Applied });
            }

            // Otherwise apply the update
            db.apply_sync_audio_file(id, imported_at, filename, file_created_at, duration_seconds, summary, modified_at, deleted_at, Some(sync_received_at))?;
            Ok(ApplyResult::Applied)
        }
        _ => Ok(ApplyResult::Skipped),
    }
}

fn apply_transcription_change(
    db: &Database,
    change: &SyncChange,
    last_sync_at: Option<i64>,
    sync_received_at: i64,
) -> VoiceResult<ApplyResult> {
    let transcription_id = &change.entity_id;
    let data = &change.data;

    // Timestamps are now i64 (Unix seconds) - no string validation needed

    // Determine the timestamp of this incoming change
    let incoming_time = if change.operation == "delete" {
        data["deleted_at"].as_i64().or_else(|| data["modified_at"].as_i64())
    } else {
        data["modified_at"].as_i64().or_else(|| data["created_at"].as_i64())
    };

    // If this change happened before or at last_sync, skip it
    if let (Some(last), Some(incoming)) = (last_sync_at, incoming_time) {
        if incoming <= last {
            return Ok(ApplyResult::Skipped);
        }
    }

    let existing = db.get_transcription_raw(transcription_id)?;

    // Extract incoming data
    let id = data["id"].as_str().unwrap_or("");
    let audio_file_id = data["audio_file_id"].as_str().unwrap_or("");
    let content = data["content"].as_str().unwrap_or("");
    let content_segments = data["content_segments"].as_str();
    let service = data["service"].as_str().unwrap_or("");
    let service_arguments = data["service_arguments"].as_str();
    let service_response = data["service_response"].as_str();
    let state = data["state"].as_str().unwrap_or(crate::database::DEFAULT_TRANSCRIPTION_STATE);
    let device_id = data["device_id"].as_str().unwrap_or("");
    let created_at = data["created_at"].as_i64().unwrap_or(0);
    let modified_at = data["modified_at"].as_i64();
    let deleted_at = data["deleted_at"].as_i64();

    // Determine if local changed since last_sync
    let local_changed = if let Some(ref ex) = existing {
        let local_time = ex.get("modified_at")
            .and_then(|v| v.as_i64())
            .or_else(|| ex.get("deleted_at").and_then(|v| v.as_i64()))
            .or_else(|| ex.get("created_at").and_then(|v| v.as_i64()));
        let incoming_time = modified_at.or(deleted_at);

        if let Some(last) = last_sync_at {
            local_time.map_or(false, |lt| lt > last)
        } else {
            match (local_time, incoming_time) {
                (Some(lt), Some(it)) => lt >= it,
                (Some(_), None) => true,
                (None, _) => false,
            }
        }
    } else {
        false
    };

    match change.operation.as_str() {
        "create" => {
            if existing.is_some() {
                return Ok(ApplyResult::Skipped);
            }
            db.apply_sync_transcription(id, audio_file_id, content, content_segments, service, service_arguments, service_response, state, device_id, created_at, modified_at, deleted_at, Some(sync_received_at))?;
            Ok(ApplyResult::Applied)
        }
        "update" | "delete" => {
            if existing.is_none() {
                db.apply_sync_transcription(id, audio_file_id, content, content_segments, service, service_arguments, service_response, state, device_id, created_at, modified_at, deleted_at, Some(sync_received_at))?;
                return Ok(ApplyResult::Applied);
            }

            let existing = existing.unwrap();
            let local_deleted = existing.get("deleted_at").and_then(|v| v.as_i64()).is_some();
            let remote_deleted = deleted_at.is_some();

            // If both deleted, apply
            if local_deleted && remote_deleted {
                db.apply_sync_transcription(id, audio_file_id, content, content_segments, service, service_arguments, service_response, state, device_id, created_at, modified_at, deleted_at, Some(sync_received_at))?;
                return Ok(ApplyResult::Applied);
            }

            // If local edited but remote deletes, create conflict (preserve local)
            if !local_deleted && remote_deleted && local_changed {
                return Ok(ApplyResult::Conflict);
            }

            // If local deleted but remote has updates
            if local_deleted && !remote_deleted {
                db.apply_sync_transcription(id, audio_file_id, content, content_segments, service, service_arguments, service_response, state, device_id, created_at, modified_at, deleted_at, Some(sync_received_at))?;
                return Ok(if local_changed { ApplyResult::Conflict } else { ApplyResult::Applied });
            }

            // Otherwise apply the update
            db.apply_sync_transcription(id, audio_file_id, content, content_segments, service, service_arguments, service_response, state, device_id, created_at, modified_at, deleted_at, Some(sync_received_at))?;
            Ok(ApplyResult::Applied)
        }
        _ => Ok(ApplyResult::Skipped),
    }
}

fn get_full_dataset(db: &Arc<Mutex<Database>>) -> VoiceResult<serde_json::Value> {
    let db = db.lock().unwrap();
    let conn = db.connection();

    // Get all notes
    let mut notes = Vec::new();
    let mut stmt = conn.prepare(
        "SELECT id, created_at, content, modified_at, deleted_at FROM notes",
    )?;
    let note_rows = stmt.query_map([], |row| {
        let id_bytes: Vec<u8> = row.get(0)?;
        let created_at: i64 = row.get(1)?;
        let content: String = row.get(2)?;
        let modified_at: Option<i64> = row.get(3)?;
        let deleted_at: Option<i64> = row.get(4)?;
        Ok((id_bytes, created_at, content, modified_at, deleted_at))
    })?;

    for row in note_rows {
        let (id_bytes, created_at, content, modified_at, deleted_at) = row?;
        let id_hex = crate::validation::uuid_bytes_to_hex(&id_bytes)?;
        notes.push(serde_json::json!({
            "id": id_hex,
            "created_at": created_at,
            "content": content,
            "modified_at": modified_at,
            "deleted_at": deleted_at,
        }));
    }

    // Get all tags
    let mut tags = Vec::new();
    let mut stmt = conn.prepare(
        "SELECT id, name, parent_id, created_at, modified_at FROM tags",
    )?;
    let tag_rows = stmt.query_map([], |row| {
        let id_bytes: Vec<u8> = row.get(0)?;
        let name: String = row.get(1)?;
        let parent_id_bytes: Option<Vec<u8>> = row.get(2)?;
        let created_at: Option<i64> = row.get(3)?;
        let modified_at: Option<i64> = row.get(4)?;
        Ok((id_bytes, name, parent_id_bytes, created_at, modified_at))
    })?;

    for row in tag_rows {
        let (id_bytes, name, parent_id_bytes, created_at, modified_at) = row?;
        let id_hex = crate::validation::uuid_bytes_to_hex(&id_bytes)?;
        let parent_id_hex = parent_id_bytes
            .map(|b| crate::validation::uuid_bytes_to_hex(&b))
            .transpose()?;
        tags.push(serde_json::json!({
            "id": id_hex,
            "name": name,
            "parent_id": parent_id_hex,
            "created_at": created_at,
            "modified_at": modified_at,
        }));
    }

    // Get all note_tags
    let mut note_tags = Vec::new();
    let mut stmt = conn.prepare(
        "SELECT note_id, tag_id, created_at, modified_at, deleted_at FROM note_tags",
    )?;
    let note_tag_rows = stmt.query_map([], |row| {
        let note_id_bytes: Vec<u8> = row.get(0)?;
        let tag_id_bytes: Vec<u8> = row.get(1)?;
        let created_at: i64 = row.get(2)?;
        let modified_at: Option<i64> = row.get(3)?;
        let deleted_at: Option<i64> = row.get(4)?;
        Ok((note_id_bytes, tag_id_bytes, created_at, modified_at, deleted_at))
    })?;

    for row in note_tag_rows {
        let (note_id_bytes, tag_id_bytes, created_at, modified_at, deleted_at) = row?;
        let note_id_hex = crate::validation::uuid_bytes_to_hex(&note_id_bytes)?;
        let tag_id_hex = crate::validation::uuid_bytes_to_hex(&tag_id_bytes)?;
        note_tags.push(serde_json::json!({
            "note_id": note_id_hex,
            "tag_id": tag_id_hex,
            "created_at": created_at,
            "modified_at": modified_at,
            "deleted_at": deleted_at,
        }));
    }

    // Get all note_attachments
    let mut note_attachments = Vec::new();
    let mut stmt = conn.prepare(
        "SELECT id, note_id, attachment_id, attachment_type, created_at, modified_at, deleted_at FROM note_attachments",
    )?;
    let note_attachment_rows = stmt.query_map([], |row| {
        let id_bytes: Vec<u8> = row.get(0)?;
        let note_id_bytes: Vec<u8> = row.get(1)?;
        let attachment_id_bytes: Vec<u8> = row.get(2)?;
        let attachment_type: String = row.get(3)?;
        let created_at: i64 = row.get(4)?;
        let modified_at: Option<i64> = row.get(5)?;
        let deleted_at: Option<i64> = row.get(6)?;
        Ok((id_bytes, note_id_bytes, attachment_id_bytes, attachment_type, created_at, modified_at, deleted_at))
    })?;

    for row in note_attachment_rows {
        let (id_bytes, note_id_bytes, attachment_id_bytes, attachment_type, created_at, modified_at, deleted_at) = row?;
        let id_hex = crate::validation::uuid_bytes_to_hex(&id_bytes)?;
        let note_id_hex = crate::validation::uuid_bytes_to_hex(&note_id_bytes)?;
        let attachment_id_hex = crate::validation::uuid_bytes_to_hex(&attachment_id_bytes)?;
        note_attachments.push(serde_json::json!({
            "id": id_hex,
            "note_id": note_id_hex,
            "attachment_id": attachment_id_hex,
            "attachment_type": attachment_type,
            "created_at": created_at,
            "modified_at": modified_at,
            "deleted_at": deleted_at,
        }));
    }

    // Get all audio_files
    let mut audio_files = Vec::new();
    let mut stmt = conn.prepare(
        "SELECT id, imported_at, filename, file_created_at, duration_seconds, summary, modified_at, deleted_at FROM audio_files",
    )?;
    let audio_file_rows = stmt.query_map([], |row| {
        let id_bytes: Vec<u8> = row.get(0)?;
        let imported_at: i64 = row.get(1)?;
        let filename: String = row.get(2)?;
        let file_created_at: Option<i64> = row.get(3)?;
        let duration_seconds: Option<f64> = row.get(4)?;
        let summary: Option<String> = row.get(5)?;
        let modified_at: Option<i64> = row.get(6)?;
        let deleted_at: Option<i64> = row.get(7)?;
        Ok((id_bytes, imported_at, filename, file_created_at, duration_seconds, summary, modified_at, deleted_at))
    })?;

    for row in audio_file_rows {
        let (id_bytes, imported_at, filename, file_created_at, duration_seconds, summary, modified_at, deleted_at) = row?;
        let id_hex = crate::validation::uuid_bytes_to_hex(&id_bytes)?;
        audio_files.push(serde_json::json!({
            "id": id_hex,
            "imported_at": imported_at,
            "filename": filename,
            "file_created_at": file_created_at,
            "duration_seconds": duration_seconds,
            "summary": summary,
            "modified_at": modified_at,
            "deleted_at": deleted_at,
        }));
    }

    // Get all transcriptions
    let mut transcriptions = Vec::new();
    let mut stmt = conn.prepare(
        "SELECT id, audio_file_id, content, content_segments, service, service_arguments, service_response, state, device_id, created_at, modified_at, deleted_at FROM transcriptions",
    )?;
    let transcription_rows = stmt.query_map([], |row| {
        let id_bytes: Vec<u8> = row.get(0)?;
        let audio_file_id_bytes: Vec<u8> = row.get(1)?;
        let content: String = row.get(2)?;
        let content_segments: Option<String> = row.get(3)?;
        let service: String = row.get(4)?;
        let service_arguments: Option<String> = row.get(5)?;
        let service_response: Option<String> = row.get(6)?;
        let state: String = row.get(7)?;
        let device_id_bytes: Vec<u8> = row.get(8)?;
        let created_at: i64 = row.get(9)?;
        let modified_at: Option<i64> = row.get(10)?;
        let deleted_at: Option<i64> = row.get(11)?;
        Ok((id_bytes, audio_file_id_bytes, content, content_segments, service, service_arguments, service_response, state, device_id_bytes, created_at, modified_at, deleted_at))
    })?;

    for row in transcription_rows {
        let (id_bytes, audio_file_id_bytes, content, content_segments, service, service_arguments, service_response, state, device_id_bytes, created_at, modified_at, deleted_at) = row?;
        let id_hex = crate::validation::uuid_bytes_to_hex(&id_bytes)?;
        let audio_file_id_hex = crate::validation::uuid_bytes_to_hex(&audio_file_id_bytes)?;
        let device_id_hex = crate::validation::uuid_bytes_to_hex(&device_id_bytes)?;
        transcriptions.push(serde_json::json!({
            "id": id_hex,
            "audio_file_id": audio_file_id_hex,
            "content": content,
            "content_segments": content_segments,
            "service": service,
            "service_arguments": service_arguments,
            "service_response": service_response,
            "state": state,
            "device_id": device_id_hex,
            "created_at": created_at,
            "modified_at": modified_at,
            "deleted_at": deleted_at,
        }));
    }

    Ok(serde_json::json!({
        "notes": notes,
        "tags": tags,
        "note_tags": note_tags,
        "note_attachments": note_attachments,
        "audio_files": audio_files,
        "transcriptions": transcriptions,
    }))
}

/// Apply sync changes from a peer to the local database.
///
/// This is the public API for applying changes, suitable for testing.
/// Returns (applied_count, conflict_count, errors).
pub fn apply_changes_from_peer(
    db: &Database,
    changes: &[SyncChange],
    peer_device_id: &str,
    peer_device_name: Option<&str>,
    local_device_id: Option<&str>,
    local_device_name: Option<&str>,
) -> VoiceResult<(i64, i64, Vec<String>)> {
    let mut applied = 0i64;
    let mut conflicts = 0i64;
    let mut errors = Vec::new();

    // Get last sync timestamp with this peer
    let last_sync_at = db.get_peer_last_sync(peer_device_id)?;

    // Get current Unix timestamp for sync_received_at
    let sync_received_at = Utc::now().timestamp();

    // Sort changes by dependency order to avoid FOREIGN KEY constraint failures:
    // 1. notes, tags, audio_files first (no dependencies) - order 0
    // 2. note_tags, note_attachments, transcriptions last (depend on notes, tags, audio_files) - order 1
    fn entity_order(entity_type: &str) -> u8 {
        match entity_type {
            "note" | "tag" | "audio_file" => 0,
            "note_tag" | "note_attachment" | "transcription" => 1,
            _ => 2,
        }
    }

    let mut sorted_changes: Vec<&SyncChange> = changes.iter().collect();
    sorted_changes.sort_by(|a, b| {
        let order_cmp = entity_order(&a.entity_type).cmp(&entity_order(&b.entity_type));
        if order_cmp != std::cmp::Ordering::Equal {
            order_cmp
        } else {
            a.timestamp.cmp(&b.timestamp)
        }
    });

    for change in sorted_changes {
        let result = match change.entity_type.as_str() {
            "note" => apply_note_change(db, change, last_sync_at, local_device_id, local_device_name, sync_received_at),
            "tag" => apply_tag_change(db, change, last_sync_at, local_device_id, local_device_name, sync_received_at),
            "note_tag" => apply_note_tag_change(db, change, last_sync_at, local_device_id, local_device_name, sync_received_at),
            "note_attachment" => apply_note_attachment_change(db, change, last_sync_at, sync_received_at),
            "audio_file" => apply_audio_file_change(db, change, last_sync_at, sync_received_at),
            "transcription" => apply_transcription_change(db, change, last_sync_at, sync_received_at),
            _ => {
                errors.push(format!("Unknown entity type: {}", change.entity_type));
                continue;
            }
        };

        match result {
            Ok(ApplyResult::Applied) => applied += 1,
            Ok(ApplyResult::Conflict) => conflicts += 1,
            Ok(ApplyResult::Skipped) => {}
            Err(e) => errors.push(format!(
                "Error applying {} {}: {}",
                change.entity_type, change.entity_id, e
            )),
        }
    }

    // Update peer's last sync timestamp
    db.update_peer_sync_time(peer_device_id, peer_device_name)?;

    Ok((applied, conflicts, errors))
}

/// Create the sync server router
pub fn create_router(
    db: Arc<Mutex<Database>>,
    config: Arc<Mutex<Config>>,
) -> Router {
    let (device_id, device_name) = {
        let cfg = config.lock().unwrap();
        (cfg.device_id_hex().to_string(), cfg.device_name().to_string())
    };

    let state = AppState {
        db,
        config,
        device_id,
        device_name,
    };

    Router::new()
        .route("/sync/handshake", post(handshake))
        .route("/sync/changes", get(get_changes))
        .route("/sync/apply", post(apply_changes))
        .route("/sync/full", get(get_full_sync))
        .route("/sync/status", get(status))
        .route("/sync/audio/:audio_id/file", get(download_audio_file))
        .route("/sync/audio/:audio_id/file", post(upload_audio_file))
        .with_state(state)
}

/// Start the sync server
pub async fn start_server(
    db: Arc<Mutex<Database>>,
    config: Arc<Mutex<Config>>,
    port: u16,
) -> VoiceResult<()> {
    let router = create_router(db, config);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    // Create shutdown channel
    let (tx, rx) = oneshot::channel::<()>();
    SHUTDOWN_TX.get_or_init(|| Mutex::new(Some(tx)));

    tracing::info!("Starting sync server on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| crate::error::VoiceError::Network(e.to_string()))?;

    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            rx.await.ok();
        })
        .await
        .map_err(|e| crate::error::VoiceError::Network(e.to_string()))?;

    Ok(())
}

/// Stop the sync server
pub fn stop_server() {
    if let Some(mutex) = SHUTDOWN_TX.get() {
        if let Ok(mut guard) = mutex.lock() {
            if let Some(tx) = guard.take() {
                let _ = tx.send(());
            }
        }
    }
}

// ============================================================================
// Tests - CRITICAL: Verify zero data loss in sync
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;
    use uuid::Uuid;

    /// Create a test database in a temporary directory
    fn create_test_db() -> (Database, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = Database::new(db_path.to_str().unwrap()).unwrap();
        (db, temp_dir)
    }

    /// Create a SyncChange for testing
    fn make_sync_change(
        entity_type: &str,
        entity_id: &str,
        operation: &str,
        data: serde_json::Value,
        device_id: &str,
    ) -> SyncChange {
        let timestamp = data.get("modified_at")
            .or_else(|| data.get("deleted_at"))
            .or_else(|| data.get("created_at"))
            .and_then(|v| v.as_i64())
            .unwrap_or(1735689600); // 2025-01-01 00:00:00 UTC

        SyncChange {
            entity_type: entity_type.to_string(),
            entity_id: entity_id.to_string(),
            operation: operation.to_string(),
            data,
            timestamp,
            device_id: device_id.to_string(),
            device_name: Some("Remote Device".to_string()),
        }
    }

    // =========================================================================
    // NOTE CONTENT CONFLICT TESTS
    // =========================================================================

    #[test]
    fn test_both_devices_edit_same_note_creates_conflict() {
        // CRITICAL: When both devices edit the same note, we MUST create a conflict
        // and merge content with conflict markers so NO data is lost.
        let (db, _temp) = create_test_db();

        // Create a note locally
        let note_id = db.create_note("Original content").unwrap();

        // Simulate local edit after initial sync
        db.update_note(&note_id, "Local edited content").unwrap();

        // Now receive a remote edit to the same note
        let remote_change = make_sync_change(
            "note",
            &note_id,
            "update",
            serde_json::json!({
                "id": note_id,
                "created_at": 1735689600,
                "content": "Remote edited content",
                "modified_at": 1735732800,
                "deleted_at": null,
            }),
            "00000000000070008000000000000099",
        );

        // Apply with no last_sync (meaning local changed since last sync)
        let result = apply_note_change(&db, &remote_change, None, None, None, 0).unwrap();

        // MUST be a conflict
        assert_eq!(result, ApplyResult::Conflict,
            "CRITICAL: Both devices edited same note - MUST create conflict!");

        // Verify content was MERGED with conflict markers (no data loss)
        let note = db.get_note(&note_id).unwrap().unwrap();
        assert!(note.content.contains("<<<<<<< LOCAL"),
            "CRITICAL: Merged content missing LOCAL marker!");
        assert!(note.content.contains("Local edited content"),
            "CRITICAL: Local content lost in merge!");
        assert!(note.content.contains("======="),
            "CRITICAL: Merged content missing separator!");
        assert!(note.content.contains("Remote edited content"),
            "CRITICAL: Remote content lost in merge!");
        assert!(note.content.contains(">>>>>>> REMOTE"),
            "CRITICAL: Merged content missing REMOTE marker!");

        // Verify a conflict record was created
        let conflicts = db.get_note_content_conflicts(false).unwrap();
        assert!(!conflicts.is_empty(),
            "CRITICAL: No conflict record created! User cannot resolve the conflict!");
    }

    #[test]
    fn test_remote_edit_after_sync_applies_cleanly() {
        // When only remote changed (local unchanged since last sync), apply cleanly
        let (db, _temp) = create_test_db();

        // Create a note
        let note_id = db.create_note("Original content").unwrap();

        // Receive remote edit with timestamp after last_sync
        let remote_change = make_sync_change(
            "note",
            &note_id,
            "update",
            serde_json::json!({
                "id": note_id,
                "created_at": 1735689600,
                "content": "Remote edited content",
                "modified_at": 1735732800,
                "deleted_at": null,
            }),
            "00000000000070008000000000000099",
        );

        // Apply with last_sync before the remote change (local unchanged)
        let result = apply_note_change(&db, &remote_change, Some(1735711200), None, None, 0).unwrap();

        // Should apply cleanly
        assert_eq!(result, ApplyResult::Applied);

        // Content should be updated
        let note = db.get_note(&note_id).unwrap().unwrap();
        assert_eq!(note.content.as_str(), "Remote edited content");
    }

    // =========================================================================
    // NOTE DELETE CONFLICT TESTS
    // =========================================================================

    #[test]
    fn test_local_edit_remote_delete_creates_conflict() {
        // CRITICAL: If local edited but remote deleted, MUST create conflict.
        // We cannot lose the local edits!
        let (db, _temp) = create_test_db();

        // Create a note
        let note_id = db.create_note("Original content").unwrap();

        // Local edits the note
        db.update_note(&note_id, "Important local edits").unwrap();

        // Remote tries to delete
        let remote_change = make_sync_change(
            "note",
            &note_id,
            "delete",
            serde_json::json!({
                "id": note_id,
                "created_at": 1735689600,
                "content": "Original content",
                "modified_at": null,
                "deleted_at": 1735732800,
            }),
            "00000000000070008000000000000099",
        );

        let result = apply_note_change(&db, &remote_change, None, None, None, 0).unwrap();

        // MUST be a conflict
        assert_eq!(result, ApplyResult::Conflict,
            "CRITICAL: Local edited, remote deleted - MUST create conflict!");

        // Verify note was NOT deleted
        let note = db.get_note(&note_id).unwrap();
        assert!(note.is_some(), "CRITICAL: Note was deleted! Local edits lost!");
        assert_eq!(note.unwrap().content.as_str(), "Important local edits",
            "CRITICAL: Local edits were lost!");

        // Verify conflict record exists
        let conflicts = db.get_note_delete_conflicts(false).unwrap();
        assert!(!conflicts.is_empty(),
            "CRITICAL: No delete conflict record! User cannot resolve!");
    }

    #[test]
    fn test_local_delete_remote_edit_creates_conflict() {
        // CRITICAL: If local deleted but remote has edits, MUST create conflict.
        // The remote edits must not be lost!
        let (db, _temp) = create_test_db();

        // Create and delete a note locally
        let note_id = db.create_note("Original content").unwrap();
        db.delete_note(&note_id).unwrap();

        // Remote sends an edit (they didn't know it was deleted)
        let remote_change = make_sync_change(
            "note",
            &note_id,
            "update",
            serde_json::json!({
                "id": note_id,
                "created_at": 1735689600,
                "content": "Remote made important edits",
                "modified_at": 1735732800,
                "deleted_at": null,
            }),
            "00000000000070008000000000000099",
        );

        let result = apply_note_change(&db, &remote_change, None, None, None, 0).unwrap();

        // MUST be a conflict
        assert_eq!(result, ApplyResult::Conflict,
            "CRITICAL: Local deleted, remote edited - MUST create conflict!");

        // Verify conflict record exists with surviving content
        let conflicts = db.get_note_delete_conflicts(false).unwrap();
        assert!(!conflicts.is_empty(),
            "CRITICAL: No delete conflict record! Remote edits could be lost!");
    }

    #[test]
    fn test_both_delete_same_note_no_conflict() {
        // When both sides delete, no conflict needed - they agree
        let (db, _temp) = create_test_db();

        // Create and delete locally
        let note_id = db.create_note("To be deleted").unwrap();
        db.delete_note(&note_id).unwrap();

        // Remote also deletes
        let remote_change = make_sync_change(
            "note",
            &note_id,
            "delete",
            serde_json::json!({
                "id": note_id,
                "created_at": 1735689600,
                "content": "To be deleted",
                "modified_at": null,
                "deleted_at": 1735732800,
            }),
            "00000000000070008000000000000099",
        );

        let result = apply_note_change(&db, &remote_change, None, None, None, 0).unwrap();

        // Should apply (both agree on deletion)
        assert_eq!(result, ApplyResult::Applied);
    }

    // =========================================================================
    // TAG RENAME CONFLICT TESTS
    // =========================================================================

    #[test]
    fn test_both_devices_rename_tag_creates_conflict() {
        // CRITICAL: When both devices rename the same tag differently,
        // we MUST create a conflict AND combine both names so no rename is lost.
        let (db, _temp) = create_test_db();

        // Create a tag
        let tag_id = db.create_tag("original_name", None).unwrap();

        // Local renames it
        db.rename_tag(&tag_id, "local_renamed").unwrap();

        // Remote tries to rename differently
        let remote_change = make_sync_change(
            "tag",
            &tag_id,
            "update",
            serde_json::json!({
                "id": tag_id,
                "name": "remote_renamed",
                "parent_id": null,
                "created_at": 1735689600,
                "modified_at": 1735732800,
            }),
            "00000000000070008000000000000099",
        );

        let result = apply_tag_change(&db, &remote_change, None, None, None, 0).unwrap();

        // MUST be a conflict
        assert_eq!(result, ApplyResult::Conflict,
            "CRITICAL: Both renamed tag differently - MUST create conflict!");

        // Verify tag name is COMBINED (no data loss)
        let tags = db.get_all_tags().unwrap();
        let tag = tags.iter().find(|t| t.id == tag_id).unwrap();
        assert_eq!(tag.name.as_str(), "local_renamed | remote_renamed",
            "CRITICAL: Tag name should combine both versions!");

        // Verify conflict record exists
        let conflicts = db.get_tag_rename_conflicts(false).unwrap();
        assert!(!conflicts.is_empty(),
            "CRITICAL: No rename conflict record! User cannot choose!");
    }

    #[test]
    fn test_both_rename_tag_same_name_no_conflict() {
        // When both sides rename to the SAME name, no conflict
        let (db, _temp) = create_test_db();

        let tag_id = db.create_tag("original", None).unwrap();
        db.rename_tag(&tag_id, "agreed_name").unwrap();

        let remote_change = make_sync_change(
            "tag",
            &tag_id,
            "update",
            serde_json::json!({
                "id": tag_id,
                "name": "agreed_name",  // Same name!
                "parent_id": null,
                "created_at": 1735689600,
                "modified_at": 1735732800,
            }),
            "00000000000070008000000000000099",
        );

        let result = apply_tag_change(&db, &remote_change, None, None, None, 0).unwrap();

        // Should apply cleanly - they agree
        assert_eq!(result, ApplyResult::Applied);
    }

    // =========================================================================
    // NOTE_TAG CONFLICT TESTS
    // =========================================================================

    #[test]
    fn test_note_tag_reactivation_applies() {
        // When remote reactivates a deleted note_tag that was deleted BEFORE last_sync,
        // it should apply (no local changes since last_sync)
        let (db, _temp) = create_test_db();

        let note_id = db.create_note("Test note").unwrap();
        let tag_id = db.create_tag("test_tag", None).unwrap();

        // Add and remove tag locally
        db.add_tag_to_note(&note_id, &tag_id).unwrap();
        db.remove_tag_from_note(&note_id, &tag_id).unwrap();

        // Remote reactivates (update with deleted_at=null)
        let entity_id = format!("{}:{}", note_id, tag_id);
        let remote_change = make_sync_change(
            "note_tag",
            &entity_id,
            "update",
            serde_json::json!({
                "note_id": note_id,
                "tag_id": tag_id,
                "created_at": 1735689600_i64,
                "modified_at": 4102401200_i64,  // Future time for the reactivation (2099-12-31)
                "deleted_at": null,  // Reactivated!
            }),
            "00000000000070008000000000000099",
        );

        // Use a far-future last_sync so local operations are considered "before last_sync"
        // This simulates: local deleted before last sync, then remote reactivates
        let result = apply_note_tag_change(&db, &remote_change, Some(4102358400), None, None, 0).unwrap();

        // Should apply since local hasn't changed since last_sync
        assert_eq!(result, ApplyResult::Applied);
    }

    // =========================================================================
    // COMPREHENSIVE DATA LOSS PREVENTION TESTS
    // =========================================================================

    #[test]
    fn test_no_silent_overwrites_ever() {
        // This test verifies the core invariant: we NEVER silently overwrite data.
        // Every conflict scenario must either:
        // 1. Create a conflict record AND merge content with markers, OR
        // 2. Be a case where both sides agree (same content, both deleted, etc.)

        let (db, _temp) = create_test_db();

        // Create test data
        let note_id = db.create_note("Important data").unwrap();

        // Edit locally
        db.update_note(&note_id, "My precious local edits").unwrap();

        // Try to overwrite with remote data
        let remote_change = make_sync_change(
            "note",
            &note_id,
            "update",
            serde_json::json!({
                "id": note_id,
                "created_at": 1735689600,
                "content": "Remote trying to overwrite",
                "modified_at": 1735732800,
                "deleted_at": null,
            }),
            "00000000000070008000000000000099",
        );

        // Apply with no last_sync (local changed)
        let result = apply_note_change(&db, &remote_change, None, None, None, 0).unwrap();

        // MUST be a conflict
        assert_eq!(result, ApplyResult::Conflict,
            "CRITICAL: Expected conflict when both sides edit!");

        // VERIFY: Content is MERGED with conflict markers (no data loss)
        let after = db.get_note(&note_id).unwrap().unwrap();
        let after_content = &after.content;

        // Both versions MUST be present
        assert!(after_content.contains("My precious local edits"),
            "CRITICAL DATA LOSS: Local content missing from merge!\n\
             Merged content: {}\n\
             Local content was silently lost!",
            after_content);

        assert!(after_content.contains("Remote trying to overwrite"),
            "CRITICAL DATA LOSS: Remote content missing from merge!\n\
             Merged content: {}\n\
             Remote content was silently lost!",
            after_content);

        // Conflict markers MUST be present
        assert!(after_content.contains("<<<<<<<") && after_content.contains(">>>>>>>"),
            "CRITICAL: Merge conflict markers missing!\n\
             Content: {}\n\
             User won't know there was a conflict!",
            after_content);
    }

    #[test]
    fn test_conflict_records_are_queryable() {
        // Users MUST be able to find and resolve conflicts
        let (db, _temp) = create_test_db();

        // Create a conflict scenario
        let note_id = db.create_note("Original").unwrap();
        db.update_note(&note_id, "Local edit").unwrap();

        let remote_change = make_sync_change(
            "note",
            &note_id,
            "update",
            serde_json::json!({
                "id": note_id,
                "created_at": 1735689600,
                "content": "Remote edit",
                "modified_at": 1735732800,
                "deleted_at": null,
            }),
            "00000000000070008000000000000099",
        );

        apply_note_change(&db, &remote_change, None, None, None, 0).unwrap();

        // MUST be able to query unresolved conflicts
        let conflicts = db.get_note_content_conflicts(false).unwrap();
        assert!(!conflicts.is_empty(), "Conflicts must be queryable!");

        // Conflict must have both versions
        let conflict = &conflicts[0];
        let local_content = conflict.get("local_content").and_then(|v| v.as_str()).unwrap_or("");
        let remote_content = conflict.get("remote_content").and_then(|v| v.as_str()).unwrap_or("");
        assert!(!local_content.is_empty(), "Conflict must have local content!");
        assert!(!remote_content.is_empty(), "Conflict must have remote content!");
    }

    #[test]
    fn test_old_remote_changes_are_skipped() {
        // Changes from before last_sync should be skipped
        let (db, _temp) = create_test_db();

        let note_id = db.create_note("Current content").unwrap();

        // Remote sends old change (before last_sync)
        let remote_change = make_sync_change(
            "note",
            &note_id,
            "update",
            serde_json::json!({
                "id": note_id,
                "created_at": 1735689600,
                "content": "Old remote content",
                "modified_at": 1735711200,  // Before last_sync (2025-01-01 06:00:00 UTC)
                "deleted_at": null,
            }),
            "00000000000070008000000000000099",
        );

        // last_sync is AFTER the remote change (2025-01-01 12:00:00 UTC)
        let result = apply_note_change(&db, &remote_change, Some(1735732800), None, None, 0).unwrap();

        assert_eq!(result, ApplyResult::Skipped, "Old changes should be skipped");

        // Content unchanged
        let note = db.get_note(&note_id).unwrap().unwrap();
        assert_eq!(note.content.as_str(), "Current content");
    }

    #[test]
    fn test_apply_sync_changes_counts_conflicts_correctly() {
        // The apply_sync_changes function must accurately report conflicts
        let (db, _temp) = create_test_db();
        let db = Arc::new(Mutex::new(db));

        // Create notes and edit them locally
        let note1_id;
        let note2_id;
        {
            let db = db.lock().unwrap();
            note1_id = db.create_note("Note 1 local").unwrap();
            note2_id = db.create_note("Note 2 local").unwrap();
            db.update_note(&note1_id, "Note 1 local edited").unwrap();
            db.update_note(&note2_id, "Note 2 local edited").unwrap();
        }

        // Send remote edits for both
        let changes = vec![
            make_sync_change(
                "note",
                &note1_id,
                "update",
                serde_json::json!({
                    "id": note1_id,
                    "created_at": 1735689600,
                    "content": "Note 1 remote edit",
                    "modified_at": 1735732800,
                    "deleted_at": null,
                }),
                "00000000000070008000000000000099",
            ),
            make_sync_change(
                "note",
                &note2_id,
                "update",
                serde_json::json!({
                    "id": note2_id,
                    "created_at": 1735689600,
                    "content": "Note 2 remote edit",
                    "modified_at": 1735732800,
                    "deleted_at": null,
                }),
                "00000000000070008000000000000099",
            ),
        ];

        let (applied, conflicts, errors) = apply_sync_changes(
            &db,
            &changes,
            "00000000000070008000000000000099",
            Some("Remote"),
            None,
            None,
        ).unwrap();

        // Both should be conflicts
        assert_eq!(conflicts, 2, "Expected 2 conflicts!");
        assert_eq!(applied, 0, "No changes should be applied - all conflicts!");
        assert!(errors.is_empty(), "Should be no errors");
    }

    // =========================================================================
    // P0: TAG PARENT_ID CONFLICT TESTS
    // =========================================================================

    #[test]
    fn test_both_devices_move_tag_to_different_parents_creates_conflict() {
        // CRITICAL P0: When both devices move a tag to different parents,
        // we MUST create a conflict. Cannot silently lose either parent choice.
        let (db, _temp) = create_test_db();

        // Create parent tags and a child tag
        let parent_a = db.create_tag("parent_a", None).unwrap();
        let parent_b = db.create_tag("parent_b", None).unwrap();
        let child_tag = db.create_tag("child", None).unwrap();

        // Local moves child to parent_a (1735776000 = 2025-01-02 00:00:00 UTC - later than remote)
        db.apply_sync_tag(&child_tag, "child", Some(&parent_a), 1735689600, Some(1735776000), None).unwrap();

        // Remote tries to move child to parent_b (at an earlier time)
        let remote_change = make_sync_change(
            "tag",
            &child_tag,
            "update",
            serde_json::json!({
                "id": child_tag,
                "name": "child",
                "parent_id": parent_b,
                "created_at": 1735689600_i64,
                "modified_at": 1735732800_i64,
            }),
            "00000000000070008000000000000099",
        );

        let result = apply_tag_change(&db, &remote_change, None, None, None, 0).unwrap();

        // MUST be a conflict
        assert_eq!(result, ApplyResult::Conflict,
            "CRITICAL P0: Both moved tag to different parents - MUST create conflict!");

        // Verify parent_a was NOT overwritten (local wins, conflict recorded)
        let tags = db.get_all_tags().unwrap();
        let tag = tags.iter().find(|t| t.id == child_tag).unwrap();
        assert_eq!(tag.parent_id.as_deref(), Some(parent_a.as_str()),
            "CRITICAL P0: Local parent choice was overwritten! Data loss!");

        // Verify conflict record exists
        let conflicts = db.get_tag_parent_conflicts(false).unwrap();
        assert!(!conflicts.is_empty(),
            "CRITICAL P0: No parent conflict record! User cannot choose!");
    }

    #[test]
    fn test_both_move_tag_to_same_parent_no_conflict() {
        // When both devices move tag to the SAME parent, no conflict needed
        let (db, _temp) = create_test_db();

        let parent = db.create_tag("parent", None).unwrap();
        let child = db.create_tag("child", None).unwrap();

        // Local moves to parent (1735038000 = 2024-12-24 10:00:00 UTC)
        db.apply_sync_tag(&child, "child", Some(&parent), 1735038000, Some(1735038000), None).unwrap();

        // Remote also moves to same parent
        let remote_change = make_sync_change(
            "tag",
            &child,
            "update",
            serde_json::json!({
                "id": child,
                "name": "child",
                "parent_id": parent,
                "created_at": 1735689600,
                "modified_at": 1735732800,
            }),
            "00000000000070008000000000000099",
        );

        let result = apply_tag_change(&db, &remote_change, None, None, None, 0).unwrap();

        // Should apply - they agree
        assert_eq!(result, ApplyResult::Applied);
    }

    // =========================================================================
    // P0: TAG DELETION CONFLICT TESTS
    // =========================================================================

    #[test]
    fn test_local_renames_tag_remote_deletes_creates_conflict() {
        // CRITICAL P0: If local renamed but remote deleted,
        // we MUST create conflict - cannot lose the rename.
        let (db, _temp) = create_test_db();

        let tag_id = db.create_tag("original", None).unwrap();

        // Local renames
        db.rename_tag(&tag_id, "renamed_locally").unwrap();

        // Remote tries to delete
        let remote_change = make_sync_change(
            "tag",
            &tag_id,
            "delete",
            serde_json::json!({
                "id": tag_id,
                "deleted_at": 1735732800,
            }),
            "00000000000070008000000000000099",
        );

        let result = apply_tag_change(&db, &remote_change, None, None, None, 0).unwrap();

        // MUST be a conflict
        assert_eq!(result, ApplyResult::Conflict,
            "CRITICAL P0: Local renamed, remote deleted - MUST create conflict!");

        // Verify tag was NOT deleted
        let tags = db.get_all_tags().unwrap();
        let tag = tags.iter().find(|t| t.id == tag_id);
        assert!(tag.is_some(), "CRITICAL P0: Tag was deleted! Rename lost!");
        assert_eq!(tag.unwrap().name.as_str(), "renamed_locally");

        // Verify conflict record exists
        let conflicts = db.get_tag_delete_conflicts(false).unwrap();
        assert!(!conflicts.is_empty(),
            "CRITICAL P0: No delete conflict record!");
    }

    #[test]
    fn test_tag_delete_applies_when_no_local_changes() {
        // When local hasn't changed, remote delete should apply
        let (db, _temp) = create_test_db();

        let tag_id = db.create_tag("to_delete", None).unwrap();

        // Remote deletes with timestamp AFTER last_sync
        let remote_change = make_sync_change(
            "tag",
            &tag_id,
            "delete",
            serde_json::json!({
                "id": tag_id,
                "deleted_at": 4102401200_i64,  // 2099 far future
            }),
            "00000000000070008000000000000099",
        );

        // Use far-future last_sync so local appears unchanged
        let result = apply_tag_change(&db, &remote_change, Some(4102358400), None, None, 0).unwrap();

        // Should apply
        assert_eq!(result, ApplyResult::Applied);

        // Tag should be gone
        let tags = db.get_all_tags().unwrap();
        let tag = tags.iter().find(|t| t.id == tag_id);
        assert!(tag.is_none(), "Tag should have been deleted");
    }

    // =========================================================================
    // P1: IDENTICAL CONTENT OPTIMIZATION TESTS
    // =========================================================================

    #[test]
    fn test_both_edit_to_same_content_no_conflict() {
        // P1: When both devices edit to identical content, no conflict needed
        let (db, _temp) = create_test_db();

        let note_id = db.create_note("Original").unwrap();

        // Local edits
        db.update_note(&note_id, "Same final content").unwrap();

        // Remote also edits to SAME content
        let remote_change = make_sync_change(
            "note",
            &note_id,
            "update",
            serde_json::json!({
                "id": note_id,
                "created_at": 1735689600,
                "content": "Same final content",  // Identical!
                "modified_at": 1735732800,
                "deleted_at": null,
            }),
            "00000000000070008000000000000099",
        );

        let result = apply_note_change(&db, &remote_change, None, None, None, 0).unwrap();

        // Should apply (not conflict) since content is identical
        assert_eq!(result, ApplyResult::Applied,
            "P1: Identical content should not create conflict!");

        // Verify no conflict was created
        let conflicts = db.get_note_content_conflicts(false).unwrap();
        assert!(conflicts.is_empty(),
            "P1: Unnecessary conflict created for identical content!");
    }

    #[test]
    fn test_different_content_still_creates_conflict() {
        // Ensure different content still creates conflict (regression test)
        let (db, _temp) = create_test_db();

        let note_id = db.create_note("Original").unwrap();

        // Local edits
        db.update_note(&note_id, "Local version").unwrap();

        // Remote edits differently
        let remote_change = make_sync_change(
            "note",
            &note_id,
            "update",
            serde_json::json!({
                "id": note_id,
                "created_at": 1735689600,
                "content": "Remote version",  // Different!
                "modified_at": 1735732800,
                "deleted_at": null,
            }),
            "00000000000070008000000000000099",
        );

        let result = apply_note_change(&db, &remote_change, None, None, None, 0).unwrap();

        // MUST be a conflict
        assert_eq!(result, ApplyResult::Conflict,
            "Different content must still create conflict!");
    }

    // =========================================================================
    // PARTIAL BATCH FAILURE TESTS
    // =========================================================================

    #[test]
    fn test_partial_batch_failure_continues_processing() {
        // CRITICAL: If change 5 of 10 fails, changes 1-4 should already be applied
        // and changes 6-10 should still be attempted and applied if valid.
        // We do NOT wrap everything in a transaction that rolls back on failure.
        let (db, _temp) = create_test_db();
        let db = std::sync::Arc::new(std::sync::Mutex::new(db));

        // Create 3 valid notes that will be created by changes 1, 3, 5
        let note1_id = uuid::Uuid::now_v7().simple().to_string();
        let note2_id = uuid::Uuid::now_v7().simple().to_string();
        let note3_id = uuid::Uuid::now_v7().simple().to_string();

        let changes = vec![
            // Change 1: Valid note create
            make_sync_change(
                "note",
                &note1_id,
                "create",
                serde_json::json!({
                    "id": note1_id,
                    "created_at": 1735689600,
                    "content": "Note 1 - should be created",
                    "modified_at": null,
                    "deleted_at": null,
                }),
                "00000000000070008000000000000099",
            ),
            // Change 2: Invalid entity type - will error
            make_sync_change(
                "invalid_type",
                "some_id",
                "create",
                serde_json::json!({}),
                "00000000000070008000000000000099",
            ),
            // Change 3: Valid note create - should still be processed after error
            make_sync_change(
                "note",
                &note2_id,
                "create",
                serde_json::json!({
                    "id": note2_id,
                    "created_at": 1735689600,
                    "content": "Note 2 - should be created despite earlier error",
                    "modified_at": null,
                    "deleted_at": null,
                }),
                "00000000000070008000000000000099",
            ),
            // Change 4: Another invalid type - will error
            make_sync_change(
                "bogus",
                "another_id",
                "update",
                serde_json::json!({}),
                "00000000000070008000000000000099",
            ),
            // Change 5: Valid note create - should still be processed
            make_sync_change(
                "note",
                &note3_id,
                "create",
                serde_json::json!({
                    "id": note3_id,
                    "created_at": 1735689600,
                    "content": "Note 3 - should be created despite multiple errors",
                    "modified_at": null,
                    "deleted_at": null,
                }),
                "00000000000070008000000000000099",
            ),
        ];

        let (applied, conflicts, errors) = apply_sync_changes(
            &db,
            &changes,
            "00000000000070008000000000000099",
            Some("Test Device"),
            None,
            None,
        ).unwrap();

        // Should have 3 successful applications
        assert_eq!(applied, 3, "All 3 valid notes should be applied!");

        // Should have 2 errors (the invalid entity types)
        assert_eq!(errors.len(), 2, "Should have 2 errors for invalid types");

        // Verify all 3 notes actually exist in the database
        let db_lock = db.lock().unwrap();

        let note1 = db_lock.get_note(&note1_id).unwrap();
        assert!(note1.is_some(), "Note 1 should exist - change before errors");

        let note2 = db_lock.get_note(&note2_id).unwrap();
        assert!(note2.is_some(), "Note 2 should exist - change after first error");

        let note3 = db_lock.get_note(&note3_id).unwrap();
        assert!(note3.is_some(), "Note 3 should exist - change after second error");
    }

    #[test]
    fn test_batch_with_all_failures_reports_all_errors() {
        // All changes fail - each should be reported individually
        let (db, _temp) = create_test_db();
        let db = std::sync::Arc::new(std::sync::Mutex::new(db));

        let changes = vec![
            make_sync_change("invalid1", "id1", "create", serde_json::json!({}), "00000000000070008000000000000099"),
            make_sync_change("invalid2", "id2", "create", serde_json::json!({}), "00000000000070008000000000000099"),
            make_sync_change("invalid3", "id3", "create", serde_json::json!({}), "00000000000070008000000000000099"),
        ];

        let (applied, conflicts, errors) = apply_sync_changes(
            &db,
            &changes,
            "00000000000070008000000000000099",
            Some("Test"),
            None,
            None,
        ).unwrap();

        assert_eq!(applied, 0);
        assert_eq!(conflicts, 0);
        assert_eq!(errors.len(), 3, "Each failure should be reported");
    }

    #[test]
    fn test_integer_timestamp_works() {
        // Verify i64 timestamps work correctly
        let (db, _temp) = create_test_db();

        // Create a change with properly formatted integer timestamps
        let valid_change = make_sync_change(
            "note",
            "00000000000000000000000000000001",
            "create",
            serde_json::json!({
                "content": "Test note",
                "created_at": 1735689600,  // 2025-01-01 00:00:00 UTC
                "modified_at": 1767225599,  // 2025-12-31 23:59:59 UTC
            }),
            "device123",
        );

        // Apply should succeed
        let result = apply_note_change(&db, &valid_change, None, None, None, 0);

        assert!(result.is_ok(), "Valid i64 timestamp should be accepted: {:?}", result);
        assert_eq!(result.unwrap(), ApplyResult::Applied, "Valid i64 timestamp should be applied");
    }

    // =========================================================================
    // TWO-INSTANCE SYNC TESTS - CRITICAL FOR VERIFYING SYNC PROPAGATION
    // These tests simulate two separate VoiceCore instances syncing through
    // a shared database (like the sync server scenario).
    // =========================================================================

    #[test]
    fn test_two_instances_sync_deleted_note() {
        // CRITICAL: This test verifies that when Instance A deletes a note,
        // the deletion is properly propagated to Instance B via sync.

        // Create two separate databases (simulating two devices)
        let (instance_a, _temp_a) = create_test_db();
        let (instance_b, _temp_b) = create_test_db();

        // Create a note on Instance A
        let note_id = instance_a.create_note("Shared note content").unwrap();
        let note = instance_a.get_note(&note_id).unwrap().unwrap();

        // Sync the note to Instance B (simulating initial sync)
        instance_b.apply_sync_note(
            &note_id,
            note.created_at,
            &note.content,
            None,
            None,
            None,
        ).unwrap();

        // Verify Instance B has the note
        let b_note = instance_b.get_note(&note_id).unwrap();
        assert!(b_note.is_some(), "Instance B should have the note after initial sync");

        // Now Instance A deletes the note
        instance_a.delete_note(&note_id).unwrap();

        // Verify Instance A sees the note as deleted
        let a_note = instance_a.get_note(&note_id).unwrap();
        assert!(a_note.is_none(), "Instance A should NOT see deleted note via get_note");

        // Get the changes from Instance A (this is what gets sent to the server)
        let (changes, _) = instance_a.get_changes_since(None, 100).unwrap();

        // Find the delete change for our note
        let delete_change = changes.iter().find(|c| {
            c.get("entity_type").and_then(|v| v.as_str()) == Some("note") &&
            c.get("entity_id").and_then(|v| v.as_str()) == Some(&note_id) &&
            c.get("operation").and_then(|v| v.as_str()) == Some("delete")
        });

        assert!(delete_change.is_some(),
            "CRITICAL: Instance A should report the delete operation in get_changes_since!");

        // Extract data from the change to apply to Instance B
        let change = delete_change.unwrap();
        let data = change.get("data").unwrap();
        let deleted_at = data.get("deleted_at").and_then(|v| v.as_i64());
        let modified_at = data.get("modified_at").and_then(|v| v.as_i64());
        let created_at = data.get("created_at").and_then(|v| v.as_i64()).unwrap_or(0);
        let content = data.get("content").and_then(|v| v.as_str()).unwrap_or("");

        // Apply the delete to Instance B
        instance_b.apply_sync_note(
            &note_id,
            created_at,
            content,
            modified_at,
            deleted_at,
            None,
        ).unwrap();

        // Verify Instance B now sees the note as deleted
        let b_note_after = instance_b.get_note(&note_id).unwrap();
        assert!(b_note_after.is_none(),
            "CRITICAL: Instance B should NOT see the note after syncing the delete!");
    }

    #[test]
    fn test_two_instances_sync_deleted_tag() {
        // CRITICAL: This test verifies that when Instance A deletes a tag,
        // the deletion is properly propagated to Instance B via sync.

        // Create two separate databases (simulating two devices)
        let (instance_a, _temp_a) = create_test_db();
        let (instance_b, _temp_b) = create_test_db();

        // Create a tag on Instance A
        let tag_id = instance_a.create_tag("Shared tag", None).unwrap();
        let tag = instance_a.get_tag(&tag_id).unwrap().unwrap();

        // Sync the tag to Instance B (simulating initial sync)
        instance_b.apply_sync_tag(
            &tag_id,
            &tag.name,
            None,
            tag.created_at.unwrap_or(0),
            None,
            None,
        ).unwrap();

        // Verify Instance B has the tag
        let b_tag = instance_b.get_tag(&tag_id).unwrap();
        assert!(b_tag.is_some(), "Instance B should have the tag after initial sync");

        // Now Instance A deletes the tag (soft delete)
        instance_a.delete_tag(&tag_id).unwrap();

        // Verify Instance A sees the tag as deleted
        let a_tag = instance_a.get_tag(&tag_id).unwrap();
        assert!(a_tag.is_none(), "Instance A should NOT see deleted tag via get_tag");

        // Get the changes from Instance A (this is what gets sent to the server)
        let (changes, _) = instance_a.get_changes_since(None, 100).unwrap();

        // Find the delete change for our tag
        let delete_change = changes.iter().find(|c| {
            c.get("entity_type").and_then(|v| v.as_str()) == Some("tag") &&
            c.get("entity_id").and_then(|v| v.as_str()) == Some(&tag_id) &&
            c.get("operation").and_then(|v| v.as_str()) == Some("delete")
        });

        assert!(delete_change.is_some(),
            "CRITICAL: Instance A should report the tag delete operation in get_changes_since!");

        // Extract data from the change to apply to Instance B
        let change = delete_change.unwrap();
        let data = change.get("data").unwrap();
        let deleted_at = data.get("deleted_at").and_then(|v| v.as_i64());
        let modified_at = data.get("modified_at").and_then(|v| v.as_i64());
        let created_at = data.get("created_at").and_then(|v| v.as_i64()).unwrap_or(0);
        let name = data.get("name").and_then(|v| v.as_str()).unwrap_or("");
        let parent_id = data.get("parent_id").and_then(|v| v.as_str());

        // Apply the delete to Instance B using the new method
        instance_b.apply_sync_tag_with_deleted(
            &tag_id,
            name,
            parent_id,
            created_at,
            modified_at,
            deleted_at,
            None,
        ).unwrap();

        // Verify Instance B now sees the tag as deleted
        let b_tag_after = instance_b.get_tag(&tag_id).unwrap();
        assert!(b_tag_after.is_none(),
            "CRITICAL: Instance B should NOT see the tag after syncing the delete!");
    }

    #[test]
    fn test_get_changes_since_includes_deleted_notes() {
        // Verify that get_changes_since properly reports deleted notes
        let (db, _temp) = create_test_db();

        // Create and delete a note
        let note_id = db.create_note("Test note").unwrap();
        db.delete_note(&note_id).unwrap();

        // Get changes
        let (changes, _) = db.get_changes_since(None, 100).unwrap();

        // Find the change for our note
        let note_change = changes.iter().find(|c| {
            c.get("entity_type").and_then(|v| v.as_str()) == Some("note") &&
            c.get("entity_id").and_then(|v| v.as_str()) == Some(&note_id)
        });

        assert!(note_change.is_some(), "Deleted note should appear in get_changes_since");

        let change = note_change.unwrap();
        assert_eq!(
            change.get("operation").and_then(|v| v.as_str()),
            Some("delete"),
            "Operation should be 'delete' for deleted note"
        );

        let data = change.get("data").unwrap();
        assert!(
            data.get("deleted_at").is_some() && !data.get("deleted_at").unwrap().is_null(),
            "deleted_at should be set in the data"
        );
    }

    #[test]
    fn test_get_changes_since_includes_deleted_tags() {
        // Verify that get_changes_since properly reports deleted tags
        let (db, _temp) = create_test_db();

        // Create and delete a tag
        let tag_id = db.create_tag("Test tag", None).unwrap();
        db.delete_tag(&tag_id).unwrap();

        // Get changes
        let (changes, _) = db.get_changes_since(None, 100).unwrap();

        // Find the change for our tag
        let tag_change = changes.iter().find(|c| {
            c.get("entity_type").and_then(|v| v.as_str()) == Some("tag") &&
            c.get("entity_id").and_then(|v| v.as_str()) == Some(&tag_id)
        });

        assert!(tag_change.is_some(), "Deleted tag should appear in get_changes_since");

        let change = tag_change.unwrap();
        assert_eq!(
            change.get("operation").and_then(|v| v.as_str()),
            Some("delete"),
            "Operation should be 'delete' for deleted tag"
        );

        let data = change.get("data").unwrap();
        assert!(
            data.get("deleted_at").is_some() && !data.get("deleted_at").unwrap().is_null(),
            "deleted_at should be set in the tag data"
        );
    }

    #[test]
    fn test_full_dataset_includes_deleted_tags() {
        // Verify that get_full_dataset properly includes deleted tags with their deleted_at
        let (db, _temp) = create_test_db();

        // Create a tag and delete it
        let tag_id = db.create_tag("Deleted tag", None).unwrap();
        db.delete_tag(&tag_id).unwrap();

        // Create a non-deleted tag for comparison
        let active_tag_id = db.create_tag("Active tag", None).unwrap();

        // Get full dataset
        let dataset = db.get_full_dataset().unwrap();
        let tags = dataset.get("tags").unwrap();

        // Find the deleted tag
        let deleted_tag = tags.iter().find(|t| {
            t.get("id").and_then(|v| v.as_str()) == Some(&tag_id)
        });

        assert!(deleted_tag.is_some(), "Full dataset should include deleted tags");

        let deleted_tag = deleted_tag.unwrap();
        assert!(
            deleted_tag.get("deleted_at").is_some() && !deleted_tag.get("deleted_at").unwrap().is_null(),
            "Deleted tag should have deleted_at in full dataset"
        );

        // Verify active tag doesn't have deleted_at
        let active_tag = tags.iter().find(|t| {
            t.get("id").and_then(|v| v.as_str()) == Some(&active_tag_id)
        });

        assert!(active_tag.is_some(), "Active tag should be in full dataset");
        let active_tag = active_tag.unwrap();
        assert!(
            active_tag.get("deleted_at").is_none() || active_tag.get("deleted_at").unwrap().is_null(),
            "Active tag should NOT have deleted_at set"
        );
    }

    // =========================================================================
    // ATTACHMENT SYNC TESTS
    // These tests verify that notes with attachments sync correctly.
    // =========================================================================

    #[test]
    fn test_two_instances_sync_note_with_audio_attachment() {
        // CRITICAL: Verify that a note with an audio attachment syncs correctly
        // between two instances

        let (instance_a, _temp_a) = create_test_db();
        let (instance_b, _temp_b) = create_test_db();

        // Create a note and audio file on Instance A
        let note_id = instance_a.create_note("Note with audio").unwrap();
        let audio_id = instance_a.create_audio_file("recording.mp3", Some(1735732800)).unwrap();

        // Attach audio to note
        let _attachment_id = instance_a.attach_to_note(&note_id, &audio_id, "audio_file").unwrap();

        // Get changes from Instance A
        let (changes, _) = instance_a.get_changes_since(None, 100).unwrap();

        // Should have: note, audio_file, note_attachment
        let note_change = changes.iter().find(|c| {
            c.get("entity_type").and_then(|v| v.as_str()) == Some("note") &&
            c.get("entity_id").and_then(|v| v.as_str()) == Some(&note_id)
        });
        assert!(note_change.is_some(), "Note should be in changes");

        let audio_change = changes.iter().find(|c| {
            c.get("entity_type").and_then(|v| v.as_str()) == Some("audio_file") &&
            c.get("entity_id").and_then(|v| v.as_str()) == Some(&audio_id)
        });
        assert!(audio_change.is_some(), "Audio file should be in changes");

        let attachment_change = changes.iter().find(|c| {
            c.get("entity_type").and_then(|v| v.as_str()) == Some("note_attachment")
        });
        assert!(attachment_change.is_some(), "Note attachment should be in changes");

        // Apply note to Instance B
        let note_data = note_change.unwrap().get("data").unwrap();
        instance_b.apply_sync_note(
            &note_id,
            note_data.get("created_at").and_then(|v| v.as_i64()).unwrap_or(0),
            note_data.get("content").and_then(|v| v.as_str()).unwrap_or(""),
            note_data.get("modified_at").and_then(|v| v.as_i64()),
            note_data.get("deleted_at").and_then(|v| v.as_i64()),
            None,
        ).unwrap();

        // Apply audio file to Instance B
        let audio_data = audio_change.unwrap().get("data").unwrap();
        instance_b.apply_sync_audio_file(
            &audio_id,
            audio_data.get("imported_at").and_then(|v| v.as_i64()).unwrap_or(0),
            audio_data.get("filename").and_then(|v| v.as_str()).unwrap_or(""),
            audio_data.get("file_created_at").and_then(|v| v.as_i64()),
            audio_data.get("duration_seconds").and_then(|v| v.as_i64()),
            audio_data.get("summary").and_then(|v| v.as_str()),
            audio_data.get("modified_at").and_then(|v| v.as_i64()),
            audio_data.get("deleted_at").and_then(|v| v.as_i64()),
            None,
        ).unwrap();

        // Apply attachment to Instance B
        let att_data = attachment_change.unwrap().get("data").unwrap();
        let att_entity_id = attachment_change.unwrap().get("entity_id").and_then(|v| v.as_str()).unwrap();
        instance_b.apply_sync_note_attachment(
            att_entity_id,
            att_data.get("note_id").and_then(|v| v.as_str()).unwrap_or(""),
            att_data.get("attachment_id").and_then(|v| v.as_str()).unwrap_or(""),
            att_data.get("attachment_type").and_then(|v| v.as_str()).unwrap_or(""),
            att_data.get("created_at").and_then(|v| v.as_i64()).unwrap_or(0),
            att_data.get("modified_at").and_then(|v| v.as_i64()),
            att_data.get("deleted_at").and_then(|v| v.as_i64()),
            None,
        ).unwrap();

        // Verify Instance B has the note
        let b_note = instance_b.get_note(&note_id).unwrap();
        assert!(b_note.is_some(), "Instance B should have the note");
        assert_eq!(b_note.unwrap().content, "Note with audio");

        // Verify Instance B has the audio file
        let b_audio = instance_b.get_audio_file(&audio_id).unwrap();
        assert!(b_audio.is_some(), "Instance B should have the audio file");
        assert_eq!(b_audio.unwrap().filename, "recording.mp3");

        // Verify the attachment relationship exists on Instance B
        let b_attachments = instance_b.get_attachments_for_note(&note_id).unwrap();
        assert_eq!(b_attachments.len(), 1, "Instance B should have 1 attachment on the note");
        assert_eq!(b_attachments[0].attachment_id, audio_id);
    }

    #[test]
    fn test_two_instances_sync_detached_attachment() {
        // Verify that detaching an attachment syncs correctly

        let (instance_a, _temp_a) = create_test_db();
        let (instance_b, _temp_b) = create_test_db();

        // Create note with attachment on Instance A
        let note_id = instance_a.create_note("Note with audio").unwrap();
        let audio_id = instance_a.create_audio_file("recording.mp3", None).unwrap();
        let attachment_id = instance_a.attach_to_note(&note_id, &audio_id, "audio_file").unwrap();

        // Sync to Instance B (initial sync)
        let (changes, _) = instance_a.get_changes_since(None, 100).unwrap();
        for change in &changes {
            let entity_type = change.get("entity_type").and_then(|v| v.as_str()).unwrap_or("");
            let data = change.get("data").unwrap();

            match entity_type {
                "note" => {
                    let id = change.get("entity_id").and_then(|v| v.as_str()).unwrap();
                    instance_b.apply_sync_note(
                        id,
                        data.get("created_at").and_then(|v| v.as_i64()).unwrap_or(0),
                        data.get("content").and_then(|v| v.as_str()).unwrap_or(""),
                        data.get("modified_at").and_then(|v| v.as_i64()),
                        data.get("deleted_at").and_then(|v| v.as_i64()),
                        None,
                    ).unwrap();
                }
                "audio_file" => {
                    let id = change.get("entity_id").and_then(|v| v.as_str()).unwrap();
                    instance_b.apply_sync_audio_file(
                        id,
                        data.get("imported_at").and_then(|v| v.as_i64()).unwrap_or(0),
                        data.get("filename").and_then(|v| v.as_str()).unwrap_or(""),
                        data.get("file_created_at").and_then(|v| v.as_i64()),
                        data.get("duration_seconds").and_then(|v| v.as_i64()),
                        data.get("summary").and_then(|v| v.as_str()),
                        data.get("modified_at").and_then(|v| v.as_i64()),
                        data.get("deleted_at").and_then(|v| v.as_i64()),
                        None,
                    ).unwrap();
                }
                "note_attachment" => {
                    let id = change.get("entity_id").and_then(|v| v.as_str()).unwrap();
                    instance_b.apply_sync_note_attachment(
                        id,
                        data.get("note_id").and_then(|v| v.as_str()).unwrap_or(""),
                        data.get("attachment_id").and_then(|v| v.as_str()).unwrap_or(""),
                        data.get("attachment_type").and_then(|v| v.as_str()).unwrap_or(""),
                        data.get("created_at").and_then(|v| v.as_i64()).unwrap_or(0),
                        data.get("modified_at").and_then(|v| v.as_i64()),
                        data.get("deleted_at").and_then(|v| v.as_i64()),
                        None,
                    ).unwrap();
                }
                _ => {}
            }
        }

        // Verify B has the attachment
        let b_attachments = instance_b.get_attachments_for_note(&note_id).unwrap();
        assert_eq!(b_attachments.len(), 1, "Instance B should have attachment after initial sync");

        // Now Instance A detaches the attachment
        instance_a.detach_from_note(&attachment_id).unwrap();

        // Get new changes
        let (changes2, _) = instance_a.get_changes_since(None, 100).unwrap();

        // Find the detach change
        let detach_change = changes2.iter().find(|c| {
            c.get("entity_type").and_then(|v| v.as_str()) == Some("note_attachment") &&
            c.get("operation").and_then(|v| v.as_str()) == Some("delete")
        });

        assert!(detach_change.is_some(), "Should have a delete operation for the attachment");

        // Apply the detach to Instance B
        let change = detach_change.unwrap();
        let data = change.get("data").unwrap();
        let id = change.get("entity_id").and_then(|v| v.as_str()).unwrap();
        instance_b.apply_sync_note_attachment(
            id,
            data.get("note_id").and_then(|v| v.as_str()).unwrap_or(""),
            data.get("attachment_id").and_then(|v| v.as_str()).unwrap_or(""),
            data.get("attachment_type").and_then(|v| v.as_str()).unwrap_or(""),
            data.get("created_at").and_then(|v| v.as_i64()).unwrap_or(0),
            data.get("modified_at").and_then(|v| v.as_i64()),
            data.get("deleted_at").and_then(|v| v.as_i64()),
            None,
        ).unwrap();

        // Verify Instance B no longer shows the attachment
        let b_attachments_after = instance_b.get_attachments_for_note(&note_id).unwrap();
        assert_eq!(b_attachments_after.len(), 0,
            "CRITICAL: Instance B should have 0 attachments after syncing the detach");
    }

    // =========================================================================
    // TRANSCRIPTION SYNC TESTS
    // =========================================================================

    #[test]
    fn test_two_instances_sync_transcription() {
        // Verify that transcriptions sync correctly between instances

        let (instance_a, _temp_a) = create_test_db();
        let (instance_b, _temp_b) = create_test_db();

        // Create an audio file on Instance A
        let audio_id = instance_a.create_audio_file("speech.mp3", None).unwrap();

        // Create a transcription for it
        let transcription_id = instance_a.create_transcription(
            &audio_id,
            "This is the transcribed text from the audio.",  // content
            None,                                              // content_segments
            "whisper",                                         // service
            None,                                              // service_arguments
            None,                                              // service_response
            None,                                              // state (uses default)
        ).unwrap();

        // Get changes from Instance A
        let (changes, _) = instance_a.get_changes_since(None, 100).unwrap();

        // Apply audio file to Instance B first
        let audio_change = changes.iter().find(|c| {
            c.get("entity_type").and_then(|v| v.as_str()) == Some("audio_file")
        }).unwrap();
        let audio_data = audio_change.get("data").unwrap();
        instance_b.apply_sync_audio_file(
            &audio_id,
            audio_data.get("imported_at").and_then(|v| v.as_i64()).unwrap_or(0),
            audio_data.get("filename").and_then(|v| v.as_str()).unwrap_or(""),
            audio_data.get("file_created_at").and_then(|v| v.as_i64()),
            audio_data.get("duration_seconds").and_then(|v| v.as_i64()),
            audio_data.get("summary").and_then(|v| v.as_str()),
            audio_data.get("modified_at").and_then(|v| v.as_i64()),
            audio_data.get("deleted_at").and_then(|v| v.as_i64()),
            None,
        ).unwrap();

        // Find and apply transcription
        let trans_change = changes.iter().find(|c| {
            c.get("entity_type").and_then(|v| v.as_str()) == Some("transcription")
        });

        assert!(trans_change.is_some(), "Transcription should be in changes");

        let trans_data = trans_change.unwrap().get("data").unwrap();
        instance_b.apply_sync_transcription(
            &transcription_id,
            trans_data.get("audio_file_id").and_then(|v| v.as_str()).unwrap_or(""),
            trans_data.get("content").and_then(|v| v.as_str()).unwrap_or(""),
            trans_data.get("content_segments").and_then(|v| v.as_str()),
            trans_data.get("service").and_then(|v| v.as_str()).unwrap_or(""),
            trans_data.get("service_arguments").and_then(|v| v.as_str()),
            trans_data.get("service_response").and_then(|v| v.as_str()),
            trans_data.get("state").and_then(|v| v.as_str()).unwrap_or(""),
            trans_data.get("device_id").and_then(|v| v.as_str()).unwrap_or(""),
            trans_data.get("created_at").and_then(|v| v.as_i64()).unwrap_or(0),
            trans_data.get("modified_at").and_then(|v| v.as_i64()),
            trans_data.get("deleted_at").and_then(|v| v.as_i64()),
            None,
        ).unwrap();

        // Verify Instance B has the transcription
        let b_transcriptions = instance_b.get_transcriptions_for_audio_file(&audio_id).unwrap();
        assert_eq!(b_transcriptions.len(), 1, "Instance B should have 1 transcription");
        assert_eq!(b_transcriptions[0].content, "This is the transcribed text from the audio.");
        assert_eq!(b_transcriptions[0].service, "whisper");
    }

    /// All entity types that should be synced. This is the authoritative list.
    /// If you add a new syncable entity type, add it here AND to get_changes_since.
    const ALL_SYNC_ENTITY_TYPES: &[&str] = &[
        "note",
        "tag",
        "note_tag",
        "note_attachment",
        "audio_file",
        "transcription",
    ];

    #[test]
    fn test_get_changes_since_returns_all_entity_types() {
        // CRITICAL TEST: Ensures get_changes_since returns ALL syncable entity types.
        // This test exists because we had a bug where transcriptions were missing
        // from get_changes_since, causing them to never sync to clients.
        //
        // If this test fails after adding a new entity type, you need to:
        // 1. Add the entity type to ALL_SYNC_ENTITY_TYPES above
        // 2. Add the query for that entity type in get_changes_since()
        // 3. Create test data for it below

        let (db, _temp) = create_test_db();

        // Create one of each entity type
        let note_id = db.create_note("Test note content").unwrap();
        let tag_id = db.create_tag("TestTag", None).unwrap();
        db.add_tag_to_note(&note_id, &tag_id).unwrap();
        let audio_id = db.create_audio_file("test.mp3", None).unwrap();
        let _attachment_id = db.attach_to_note(&note_id, &audio_id, "audio_file").unwrap();
        let _transcription_id = db.create_transcription(
            &audio_id,
            "Test transcription content",
            None,  // content_segments
            "whisper",
            None,  // service_arguments
            None,  // service_response
            None,  // state (uses default)
        ).unwrap();

        // Get all changes
        let (changes, _) = db.get_changes_since(None, 1000).unwrap();

        // Collect the entity types we got
        let mut found_types: std::collections::HashSet<String> = std::collections::HashSet::new();
        for change in &changes {
            if let Some(entity_type) = change.get("entity_type").and_then(|v| v.as_str()) {
                found_types.insert(entity_type.to_string());
            }
        }

        // Verify ALL expected entity types are present
        for expected_type in ALL_SYNC_ENTITY_TYPES {
            assert!(
                found_types.contains(*expected_type),
                "CRITICAL: get_changes_since is missing entity type '{}'. \
                 Found types: {:?}. \
                 This will cause {} entities to never sync to clients! \
                 Add the query for '{}' to get_changes_since().",
                expected_type,
                found_types,
                expected_type,
                expected_type
            );
        }

        // Also verify we didn't get any unexpected types
        for found_type in &found_types {
            assert!(
                ALL_SYNC_ENTITY_TYPES.contains(&found_type.as_str()),
                "Unexpected entity type '{}' in get_changes_since. \
                 If this is a new entity type, add it to ALL_SYNC_ENTITY_TYPES.",
                found_type
            );
        }
    }

    #[test]
    fn test_get_changes_since_returns_modified_transcription() {
        // Specific test for the bug where transcription state changes weren't syncing.
        // When a transcription is modified (e.g., state changed from "original" to "verified"),
        // it must appear in get_changes_since.

        let (db, _temp) = create_test_db();

        // Create audio file and transcription
        let audio_id = db.create_audio_file("test.mp3", None).unwrap();
        let transcription_id = db.create_transcription(
            &audio_id,
            "Test content",
            None,
            "whisper",
            None,
            None,
            None,  // state (uses default)
        ).unwrap();

        // Record the current time as our "last sync"
        let last_sync = chrono::Utc::now().timestamp();

        // Wait a moment to ensure the modification timestamp is later
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Modify the transcription (simulate changing state to "verified")
        let now = chrono::Utc::now().timestamp();
        let device_id = "00000000000000000000000000000000";  // Dummy device ID for test
        db.apply_sync_transcription(
            &transcription_id,
            &audio_id,
            "Test content",
            None,
            "whisper",
            None,
            None,
            "verified",  // Changed state
            device_id,
            now,  // created_at
            Some(now),  // modified_at
            None,  // deleted_at
            None,  // sync_received_at - None for local operations
        ).unwrap();

        // Get changes since last sync
        let (changes, _) = db.get_changes_since(Some(last_sync), 1000).unwrap();

        // Find the transcription change
        let trans_change = changes.iter().find(|c| {
            c.get("entity_type").and_then(|v| v.as_str()) == Some("transcription") &&
            c.get("entity_id").and_then(|v| v.as_str()) == Some(&transcription_id)
        });

        assert!(
            trans_change.is_some(),
            "Modified transcription must appear in get_changes_since! \
             This bug caused transcription state changes to never sync."
        );

        // Verify the state was updated
        let data = trans_change.unwrap().get("data").unwrap();
        assert_eq!(
            data.get("state").and_then(|v| v.as_str()),
            Some("verified"),
            "Transcription state should be 'verified'"
        );
    }
}
