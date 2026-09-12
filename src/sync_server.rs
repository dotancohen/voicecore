//! Sync server implementation using Axum.
//!
//! This module provides the server side of the sync protocol:
//! - /sync/handshake - Exchange device info
//! - /sync/changes - Get changes since timestamp
//! - /sync/apply - Apply changes from peer
//! - /sync/full - Get full dataset for initial sync
//! - /sync/status - Health check
//! - /sync/audio/:id/file - One recording's bytes: GET serves it to a fetching peer, POST receives it from a sending peer

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use axum::{
    body::Bytes,
    extract::{ConnectInfo, DefaultBodyLimit, Path, Query, Request, State},
    http::{HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::auth;
use crate::config::Config;
use crate::database::Database;
use crate::error::VoiceResult;
use crate::models::SyncChange;
use crate::sync_protocol::{
    codes, ApplyRequest, ApplyResponse, ChangesQuery, ChangesResponse, ErrorResponse, HandshakeRequest,
    HandshakeResponse, PairClaimRequest, PairClaimResponse, StatusResponse, PROTOCOL_VERSION,
};
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
    /// Refusals per source address, for the delay that slows a guesser
    /// (AUTH-5): count and the time of the last one.
    failures: Arc<Mutex<HashMap<IpAddr, (u32, Instant)>>>,
}

/// After this many refusals from one address, each further refusal waits
/// before answering; the wait doubles up to [`MAX_DELAY`].
const FREE_FAILURES: u32 = 3;
const MAX_DELAY: Duration = Duration::from_secs(8);
/// Counters older than this are forgotten, and the map is emptied when it
/// grows past [`MAX_TRACKED_ADDRESSES`], so memory stays bounded.
const FAILURE_MEMORY: Duration = Duration::from_secs(600);
const MAX_TRACKED_ADDRESSES: usize = 10_000;

/// The first characters of an id, for a sentence.
fn short(id: &str) -> &str {
    &id[..UUID_SHORT_LEN.min(id.len())]
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok()).map(str::trim).filter(|v| !v.is_empty())
}

/// The bearer token of an `Authorization` header, if there is one.
fn bearer(headers: &HeaderMap) -> Option<&str> {
    header(headers, "authorization")
        .and_then(|v| v.strip_prefix("Bearer ").or_else(|| v.strip_prefix("bearer ")))
        .map(str::trim)
        .filter(|v| !v.is_empty())
}

/// Count a refusal from an address and say how long to wait before
/// answering it (AUTH-5).
fn note_failure(state: &AppState, ip: IpAddr) -> Duration {
    let mut failures = state.failures.lock().unwrap();
    let now = Instant::now();
    if failures.len() > MAX_TRACKED_ADDRESSES {
        failures.clear();
    }
    let entry = failures.entry(ip).or_insert((0, now));
    if now.duration_since(entry.1) > FAILURE_MEMORY {
        entry.0 = 0;
    }
    entry.0 += 1;
    entry.1 = now;
    if entry.0 > FREE_FAILURES {
        let doublings = (entry.0 - FREE_FAILURES).min(3);
        Duration::from_secs(1 << doublings).min(MAX_DELAY)
    } else {
        Duration::ZERO
    }
}

/// `POST /pair/claim` (PAIR-3): a reading device presents the token from
/// the code and receives a key of its own. Authenticated by the token
/// alone, so it sits outside the device-key middleware; a wrong token
/// counts like any other refusal from that address.
async fn pair_claim(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Json(request): Json<PairClaimRequest>,
) -> Response {
    let admitted = {
        let db = state.db.lock().unwrap();
        crate::pairing::admit_by_token(
            &db,
            &request.token,
            &request.device_id,
            &request.device_name,
            &request.certificate_fingerprint,
            &request.addresses,
            &request.application,
        )
        .map(|key| (key, db.account_id().unwrap_or_default(), db.get_device_card(&state.device_id).ok().flatten()))
    };
    match admitted {
        Ok((device_key, account_id, own_card)) => {
            tracing::info!("Paired {} ({}) into account {}", short(&request.device_id), request.device_name, short(&account_id));
            if let Ok(mut failures) = state.failures.lock() {
                failures.remove(&addr.ip());
            }
            let (certificate_fingerprint, addresses) = own_card
                .map(|c| (c.certificate_fingerprint, c.addresses))
                .unwrap_or_default();
            Json(PairClaimResponse {
                account_id,
                device_key,
                device_id: state.device_id.clone(),
                device_name: state.device_name.clone(),
                certificate_fingerprint,
                addresses,
            })
            .into_response()
        }
        Err(sentence) => {
            let delay = note_failure(&state, addr.ip());
            tracing::warn!("Refused a pairing claim from {}: {}", addr.ip(), codes::TOKEN_INVALID);
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            (StatusCode::FORBIDDEN, Json(ErrorResponse::with_code(sentence, codes::TOKEN_INVALID))).into_response()
        }
    }
}

/// Every route but the health check passes through here (AUTH-3): the
/// account must be this one, the device must have a card that is not
/// revoked, and the key must hash to the card's hash. A refusal is a
/// sentence with its code, and repeated refusals from one address are
/// answered ever more slowly (AUTH-5). The key itself is never logged.
async fn require_device(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    request: Request,
    next: Next,
) -> Response {
    let headers = request.headers();
    let account = header(headers, auth::HEADER_ACCOUNT).map(str::to_string);
    let device = header(headers, auth::HEADER_DEVICE).map(str::to_string);
    let key = bearer(headers).map(str::to_string);
    let verdict = {
        let db = state.db.lock().unwrap();
        let own_account = db.account_id().unwrap_or_default();
        auth::verify_request(&db, &own_account, account.as_deref(), device.as_deref(), key.as_deref())
    };
    match verdict {
        Ok(_) => {
            if let Ok(mut failures) = state.failures.lock() {
                failures.remove(&addr.ip());
            }
            next.run(request).await
        }
        Err(refusal) => {
            let delay = note_failure(&state, addr.ip());
            tracing::warn!(
                "Refused {} from {} for device {}: {}",
                request.uri().path(),
                addr.ip(),
                device.as_deref().map(short).unwrap_or("-"),
                refusal.code
            );
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            (
                StatusCode::from_u16(refusal.status).unwrap_or(StatusCode::UNAUTHORIZED),
                Json(ErrorResponse::with_code(refusal.sentence, refusal.code)),
            )
                .into_response()
        }
    }
}

// Route handlers

async fn handshake(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<HandshakeRequest>,
) -> impl IntoResponse {
    tracing::debug!(
        "Handshake from device_id={}... device_name={} protocol_version={}",
        &request.device_id[..UUID_SHORT_LEN.min(request.device_id.len())],
        request.device_name,
        request.protocol_version
    );

    // Validate device_id
    if request.device_id.len() != 32 || !request.device_id.chars().all(|c| c.is_ascii_hexdigit()) {
        tracing::warn!("Invalid device_id format: {}", request.device_id);
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new("Invalid device_id format".to_string())),
        )
            .into_response();
    }

    // The body and the headers must agree about who is calling, or a device
    // could act under another's name with its own key.
    if let Some(named) = header(&headers, auth::HEADER_DEVICE) {
        if named != request.device_id {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::with_code(
                    format!("The handshake names device {} but the request was made by {} ({})", short(&request.device_id), short(named), codes::DEVICE_MISMATCH),
                    codes::DEVICE_MISMATCH,
                )),
            )
                .into_response();
        }
    }

    // The account check (ACCT-2, ACCT-3): the two sides must hold the same
    // account, or nothing is exchanged. Never adopts, never corrects.
    let own_account = {
        let db = state.db.lock().unwrap();
        db.account_id().unwrap_or_default()
    };
    if request.account_id.is_empty() {
        tracing::warn!("Handshake from {} named no account", short(&request.device_id));
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::with_code(
                format!("The handshake named no account ({})", codes::ACCOUNT_MISSING),
                codes::ACCOUNT_MISSING,
            )),
        )
            .into_response();
    }
    if request.account_id != own_account {
        tracing::warn!(
            "Refused {}: it holds account {}, this device holds {}",
            short(&request.device_id),
            short(&request.account_id),
            short(&own_account)
        );
        return (
            StatusCode::FORBIDDEN,
            Json(ErrorResponse::with_code(
                format!(
                    "This device holds account {}, not {}; nothing was exchanged ({})",
                    short(&own_account),
                    short(&request.account_id),
                    codes::ACCOUNT_MISMATCH
                ),
                codes::ACCOUNT_MISMATCH,
            )),
        )
            .into_response();
    }

    // A handshake starts a peer's operation, and what it applies afterwards
    // must be undoable (SNAP-3).
    {
        let db = state.db.lock().unwrap();
        if let Err(e) = db.snapshot_before("handshake") {
            tracing::warn!("Could not take a snapshot before the handshake: {}", e);
        }
        if let Err(e) = db.set_peer_account_id(&request.device_id, Some(&request.device_name), &request.account_id) {
            tracing::warn!("Could not record the peer's account: {}", e);
        }
    }

    // Get last sync timestamp for this peer
    let last_sync = get_peer_last_sync(&state.db, &request.device_id);
    tracing::debug!("Last sync with this peer: {:?}", last_sync);

    // Check if audiofile_directory is configured
    let supports_audiofiles = {
        let config = state.config.lock().ok();
        config.map(|c| c.audiofile_directory().is_some()).unwrap_or(false)
    };

    let (database_id, cursor) = {
        let db = state.db.lock().unwrap();
        (db.database_id().unwrap_or_default(), db.current_seq().unwrap_or(0))
    };

    let response = HandshakeResponse {
        device_id: state.device_id.clone(),
        device_name: state.device_name.clone(),
        protocol_version: PROTOCOL_VERSION.to_string(),
        account_id: own_account,
        last_sync_timestamp: last_sync,
        server_timestamp: Utc::now().timestamp(),
        supports_audiofiles,
        database_id,
        cursor,
    };

    Json(response).into_response()
}

async fn get_changes(
    State(state): State<AppState>,
    Query(query): Query<ChangesQuery>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(1000).min(10000);
    tracing::debug!("GET /sync/changes cursor={:?} since={:?} limit={}", query.cursor, query.since, limit);

    // Get changes from database: cursor feed when asked for, timestamp filter otherwise
    let (changes, latest_timestamp, next_cursor, is_complete, database_id) = {
        let db = state.db.lock().unwrap();
        let database_id = db.database_id().unwrap_or_default();
        let result = match query.cursor {
            Some(cursor) => db
                .get_changes_after_seq_as_sync_changes(cursor, None, limit)
                .map(|(changes, next, complete)| (changes, None, Some(next), complete)),
            None => db
                .get_changes_since_as_sync_changes(query.since, limit)
                .map(|(changes, latest)| {
                    let complete = (changes.len() as i64) < limit;
                    (changes, latest, None, complete)
                }),
        };
        match result {
            Ok((c, l, n, complete)) => (c, l, n, complete, database_id),
            Err(e) => {
                tracing::error!("Failed to get changes: {}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse::new(e.to_string())),
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
        changes,
        from_timestamp: query.since,
        to_timestamp: latest_timestamp,
        next_cursor,
        database_id,
        device_id: state.device_id.clone(),
        device_name: state.device_name.clone(),
        is_complete,
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
            Json(ErrorResponse::new("Invalid device_id format".to_string())),
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
                Json(ErrorResponse::new(e.to_string())),
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
                Json(ErrorResponse::new(e.to_string())),
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

    // Add required metadata fields that the client expects. The cursor is the
    // end of the feed at this moment: a client that applied this dataset can
    // continue incrementally from it.
    let (database_id, cursor) = {
        let db = state.db.lock().unwrap();
        (db.database_id().unwrap_or_default(), db.current_seq().unwrap_or(0))
    };
    if let Some(obj) = data.as_object_mut() {
        obj.insert("device_id".to_string(), serde_json::Value::String(state.device_id.clone()));
        obj.insert("device_name".to_string(), serde_json::Value::String(state.device_name.clone()));
        obj.insert("timestamp".to_string(), serde_json::json!(chrono::Utc::now().timestamp()));
        obj.insert("database_id".to_string(), serde_json::Value::String(database_id));
        obj.insert("cursor".to_string(), serde_json::json!(cursor));
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
        protocol_version: PROTOCOL_VERSION.to_string(),
        status: "ok".to_string(),
        supports_audiofiles,
    })
}

/// Download an audio file
async fn serve_audio_file(
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

/// Receive one recording sent by a peer (`send_audio_file` on the client).
async fn receive_audio_file(
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

        crate::models::audio_file_extension(&audio_file.filename)
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
    _local_device_id: Option<&str>,
    _local_device_name: Option<&str>,
) -> VoiceResult<(i64, i64, Vec<String>)> {
    let db = db.lock().unwrap();
    let sync_received_at = Utc::now().timestamp();
    let outcome = crate::sync_apply::apply_changes(&db, changes, peer_device_id, peer_device_name, sync_received_at)?;
    if outcome.retried_ok > 0 {
        tracing::info!("Applied {} previously failed changes", outcome.retried_ok);
    }
    // Update peer's last sync timestamp
    db.update_peer_sync_time(peer_device_id, peer_device_name)?;
    Ok((outcome.applied, outcome.conflicts, outcome.errors))
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
        "SELECT id, imported_at, filename, file_created_at, duration_seconds, summary, modified_at, deleted_at, storage_provider, storage_key, storage_uploaded_at FROM audio_files",
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
        let storage_provider: Option<String> = row.get(8)?;
        let storage_key: Option<String> = row.get(9)?;
        let storage_uploaded_at: Option<i64> = row.get(10)?;
        Ok((id_bytes, imported_at, filename, file_created_at, duration_seconds, summary, modified_at, deleted_at, storage_provider, storage_key, storage_uploaded_at))
    })?;

    for row in audio_file_rows {
        let (id_bytes, imported_at, filename, file_created_at, duration_seconds, summary, modified_at, deleted_at, storage_provider, storage_key, storage_uploaded_at) = row?;
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
            "storage_provider": storage_provider,
            "storage_key": storage_key,
            "storage_uploaded_at": storage_uploaded_at,
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

    // Every version: the complete history travels with the full dataset
    let field_versions: Vec<serde_json::Value> = db
        .get_versions_since(None, i64::MAX)?
        .into_iter()
        .map(|v| v.to_json())
        .collect();

    // Get file_storage_config (single row)
    let file_storage_config: Option<serde_json::Value> = conn.query_row(
        "SELECT provider, config, modified_at, device_id FROM file_storage_config WHERE id = 'default'",
        [],
        |row| {
            let provider: String = row.get(0)?;
            let config: Option<String> = row.get(1)?;
            let modified_at: Option<i64> = row.get(2)?;
            let device_id_bytes: Option<Vec<u8>> = row.get(3)?;
            Ok((provider, config, modified_at, device_id_bytes))
        },
    ).ok().map(|(provider, config, modified_at, device_id_bytes)| {
        let device_id_hex = device_id_bytes.and_then(|b| crate::validation::uuid_bytes_to_hex(&b).ok());
        let config_val: Option<serde_json::Value> = config.and_then(|s| serde_json::from_str(&s).ok());
        serde_json::json!({
            "id": "default",
            "provider": provider,
            "config": config_val,
            "modified_at": modified_at,
            "device_id": device_id_hex,
        })
    });

    Ok(serde_json::json!({
        "notes": notes,
        "tags": tags,
        "note_tags": note_tags,
        "note_attachments": note_attachments,
        "audio_files": audio_files,
        "transcriptions": transcriptions,
        "file_storage_config": file_storage_config,
        "field_versions": field_versions,
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
    _local_device_id: Option<&str>,
    _local_device_name: Option<&str>,
) -> VoiceResult<(i64, i64, Vec<String>)> {
    let sync_received_at = Utc::now().timestamp();
    let outcome = crate::sync_apply::apply_changes(db, changes, peer_device_id, peer_device_name, sync_received_at)?;
    db.update_peer_sync_time(peer_device_id, peer_device_name)?;
    Ok((outcome.applied, outcome.conflicts, outcome.errors))
}

/// Create the sync server router
pub fn create_router(
    db: Arc<Mutex<Database>>,
    config: Arc<Mutex<Config>>,
) -> Router {
    let (device_id, device_name, max_body_size) = {
        let cfg = config.lock().unwrap();
        (
            cfg.device_id_hex().to_string(),
            cfg.device_name().to_string(),
            cfg.max_sync_file_size_bytes() as usize,
        )
    };

    let state = AppState {
        db,
        config,
        device_id,
        device_name,
        failures: Arc::new(Mutex::new(HashMap::new())),
    };

    tracing::info!(
        "Sync server body limit: {} MB",
        max_body_size / 1024 / 1024
    );

    let authenticated = Router::new()
        .route("/sync/handshake", post(handshake))
        .route("/sync/changes", get(get_changes))
        .route("/sync/apply", post(apply_changes))
        .route("/sync/full", get(get_full_sync))
        .route("/sync/audio/:audio_id/file", get(serve_audio_file))
        .route("/sync/audio/:audio_id/file", post(receive_audio_file))
        .route_layer(middleware::from_fn_with_state(state.clone(), require_device));

    Router::new()
        .route("/sync/status", get(status))
        .route("/pair/claim", post(pair_claim))
        .merge(authenticated)
        // Body limit is configurable via sync.max_sync_file_size_mb in config
        .layer(DefaultBodyLimit::max(max_body_size))
        .with_state(state)
}

/// Start the sync server: HTTPS with this device's own certificate (made
/// under `certs/` if missing), or plain HTTP when `plain_http` is set, which
/// is allowed only on a loopback address, for a reverse proxy in front or a
/// test on this machine (AUTH-7). The listener's certificate fingerprint is
/// written to its own device card so peers can pin it from the card.
pub async fn start_server(
    db: Arc<Mutex<Database>>,
    config: Arc<Mutex<Config>>,
    host: &str,
    port: u16,
    plain_http: bool,
) -> VoiceResult<()> {
    let addr: SocketAddr = format!("{}:{}", host, port)
        .parse()
        .map_err(|e| crate::error::VoiceError::Network(format!("{} is not an address: {}", host, e)))?;
    if plain_http && !addr.ip().is_loopback() {
        return Err(crate::error::VoiceError::Network(format!(
            "Plain http is allowed only on this machine itself, not on {} ({})",
            host,
            codes::TLS_REQUIRED
        )));
    }

    let tls = if plain_http {
        None
    } else {
        let (cert_path, key_path, fingerprint) = {
            let cfg = config.lock().unwrap();
            crate::tls::ensure_server_certificate(&cfg, false)?
        };
        tracing::info!("Certificate fingerprint {}", fingerprint);
        Some(crate::tls::server_config(&cert_path, &key_path)?)
    };
    {
        let db_guard = db.lock().unwrap();
        let mut cfg = config.lock().unwrap();
        crate::auth::ensure_own_device_card(&db_guard, &mut cfg)?;
    }

    let router = create_router(db, config).into_make_service_with_connect_info::<SocketAddr>();

    // Create shutdown channel
    let (tx, rx) = oneshot::channel::<()>();
    SHUTDOWN_TX.get_or_init(|| Mutex::new(Some(tx)));
    let handle = axum_server::Handle::new();
    let stopper = handle.clone();
    tokio::spawn(async move {
        rx.await.ok();
        stopper.graceful_shutdown(Some(Duration::from_secs(5)));
    });

    tracing::info!("Starting sync server on {} ({})", addr, if plain_http { "plain http" } else { "https" });

    match tls {
        Some(server_config) => {
            let rustls = axum_server::tls_rustls::RustlsConfig::from_config(server_config);
            axum_server::bind_rustls(addr, rustls)
                .handle(handle)
                .serve(router)
                .await
                .map_err(|e| crate::error::VoiceError::Network(e.to_string()))?;
        }
        None => {
            axum_server::bind(addr)
                .handle(handle)
                .serve(router)
                .await
                .map_err(|e| crate::error::VoiceError::Network(e.to_string()))?;
        }
    }

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

    use crate::sync_apply::ALL_SYNC_ENTITY_TYPES;

    /// Create a test database in a temporary directory
    fn create_test_db() -> (Database, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = Database::new(db_path.to_str().unwrap()).unwrap();
        (db, temp_dir)
    }

    mod account_identity {
        use super::*;
        use axum::extract::{Json, State};
        use axum::http::StatusCode;
        use axum::response::IntoResponse;
        use crate::config::Config;
        use crate::sync_client::SyncClient;

        fn state_for(db: Database, dir: &TempDir) -> AppState {
            let config = Config::new(Some(dir.path().to_path_buf())).unwrap();
            let device_id = config.device_id_hex().to_string();
            AppState {
                db: Arc::new(Mutex::new(db)),
                config: Arc::new(Mutex::new(config)),
                device_id,
                device_name: "Server".to_string(),
                failures: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        fn request(account_id: &str) -> HandshakeRequest {
            HandshakeRequest {
                device_id: "00000000000070008000000000000099".to_string(),
                device_name: "Phone".to_string(),
                protocol_version: PROTOCOL_VERSION.to_string(),
                account_id: account_id.to_string(),
            }
        }

        async fn body_of(response: axum::response::Response) -> (StatusCode, serde_json::Value) {
            let status = response.status();
            let bytes = axum::body::to_bytes(response.into_body(), 1 << 20).await.unwrap();
            (status, serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null))
        }

        #[tokio::test]
        async fn the_same_account_is_let_in_and_told_the_account() {
            let (db, dir) = create_test_db();
            let account = db.account_id().unwrap();
            let state = state_for(db, &dir);
            let (status, body) = body_of(handshake(State(state.clone()), HeaderMap::new(), Json(request(&account))).await.into_response()).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body["account_id"], account);
            let db = state.db.lock().unwrap();
            assert_eq!(db.get_peer_account_id("00000000000070008000000000000099").unwrap(), Some(account));
            assert_eq!(db.list_snapshots().unwrap().len(), 1, "a snapshot before the peer's operation");
        }

        #[tokio::test]
        async fn another_account_is_refused_with_its_code() {
            let (db, dir) = create_test_db();
            let state = state_for(db, &dir);
            let other = "0199bbbbbbbb7000800000000000000b";
            let (status, body) = body_of(handshake(State(state.clone()), HeaderMap::new(), Json(request(other))).await.into_response()).await;
            assert_eq!(status, StatusCode::FORBIDDEN);
            assert_eq!(body["code"], codes::ACCOUNT_MISMATCH);
            assert!(body["error"].as_str().unwrap().contains("nothing was exchanged"));
            let db = state.db.lock().unwrap();
            assert_eq!(db.get_peer_account_id("00000000000070008000000000000099").unwrap(), None);
            assert!(db.list_snapshots().unwrap().is_empty());
        }

        #[tokio::test]
        async fn a_handshake_that_names_no_account_is_refused() {
            let (db, dir) = create_test_db();
            let state = state_for(db, &dir);
            let (status, body) = body_of(handshake(State(state), HeaderMap::new(), Json(request(""))).await.into_response()).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(body["code"], codes::ACCOUNT_MISSING);
        }

        /// The whole path, over a real socket: a device of one account syncs
        /// with a device of another, and nothing crosses in either direction.
        #[tokio::test]
        async fn a_mismatched_pair_exchanges_nothing() {
            let server_dir = TempDir::new().unwrap();
            let server_db = Database::new(server_dir.path().join("notes.db")).unwrap();
            server_db.create_note("של השרת").unwrap();
            let server_state_db = Arc::new(Mutex::new(server_db));
            let server_config = Arc::new(Mutex::new(Config::new(Some(server_dir.path().to_path_buf())).unwrap()));
            let server_device = server_config.lock().unwrap().device_id_hex().to_string();
            let router = create_router(server_state_db.clone(), server_config);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let serving = tokio::spawn(async move {
                axum::serve(listener, router.into_make_service_with_connect_info::<SocketAddr>()).await.unwrap()
            });

            let client_dir = TempDir::new().unwrap();
            let client_db = Database::new(client_dir.path().join("notes.db")).unwrap();
            client_db.create_note("של הלקוח").unwrap();
            let client_db = Arc::new(Mutex::new(client_db));
            let mut client_config = Config::new(Some(client_dir.path().to_path_buf())).unwrap();
            client_config.add_peer(&server_device, "Server", &url, None, true).unwrap();
            let client = SyncClient::new(client_db.clone(), Arc::new(Mutex::new(client_config))).unwrap();

            let result = client.sync_with_peer(&server_device).await;

            assert!(!result.success);
            // The headers name an account the server does not hold, so the
            // refusal comes before the handshake body is even read (AUTH-3).
            assert!(result.errors.iter().any(|e| e.contains(codes::ACCOUNT_UNKNOWN)), "{:?}", result.errors);
            assert_eq!(result.pulled, 0);
            assert_eq!(result.pushed, 0);
            assert_eq!(server_state_db.lock().unwrap().get_all_notes().unwrap().len(), 1, "the server kept only its own note");
            assert_eq!(client_db.lock().unwrap().get_all_notes().unwrap().len(), 1, "the client kept only its own note");
            assert_eq!(client_db.lock().unwrap().get_peer_cursors(&server_device).unwrap(), (0, 0, None));
            serving.abort();
        }
    }

    mod authentication {
        use super::*;
        use crate::auth;
        use crate::config::Config;
        use crate::sync_client::SyncClient;
        use crate::sync_protocol::codes;

        /// A device with its own database, config and key.
        struct Device {
            db: Arc<Mutex<Database>>,
            config: Arc<Mutex<Config>>,
            id: String,
            _dir: TempDir,
        }

        fn device(name: &str) -> Device {
            let dir = TempDir::new().unwrap();
            let db = Database::new(dir.path().join("notes.db")).unwrap();
            let mut config = Config::new(Some(dir.path().to_path_buf())).unwrap();
            config.set_device_name(name).unwrap();
            auth::ensure_own_device_card(&db, &mut config).unwrap();
            let id = config.device_id_hex().to_string();
            Device { db: Arc::new(Mutex::new(db)), config: Arc::new(Mutex::new(config)), id, _dir: dir }
        }

        /// Let `caller` into `server`'s account: its card, with its key hash.
        fn admit(server: &Device, caller: &Device) {
            let card = caller.db.lock().unwrap().get_device_card(&caller.id).unwrap().unwrap();
            server.db.lock().unwrap().write_device_card(&card).unwrap();
        }

        /// Serve a device on this machine, plain or with its own certificate.
        /// Returns the URL, the fingerprint (TLS only) and the task.
        fn serve(server: &Device, tls: bool) -> (String, String, tokio::task::JoinHandle<()>) {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let port = listener.local_addr().unwrap().port();
            let router = create_router(server.db.clone(), server.config.clone())
                .into_make_service_with_connect_info::<SocketAddr>();
            if tls {
                let (cert, key, fingerprint) = {
                    let cfg = server.config.lock().unwrap();
                    crate::tls::ensure_server_certificate(&cfg, false).unwrap()
                };
                let config = axum_server::tls_rustls::RustlsConfig::from_config(crate::tls::server_config(&cert, &key).unwrap());
                let task = tokio::spawn(async move {
                    axum_server::from_tcp_rustls(listener, config).serve(router).await.unwrap();
                });
                (format!("https://127.0.0.1:{}", port), fingerprint, task)
            } else {
                let task = tokio::spawn(async move {
                    axum_server::from_tcp(listener).serve(router).await.unwrap();
                });
                (format!("http://127.0.0.1:{}", port), String::new(), task)
            }
        }

        fn client(caller: &Device, server: &Device, url: &str, pin: Option<&str>) -> SyncClient {
            caller.config.lock().unwrap().add_peer(&server.id, "Server", url, pin, true).unwrap();
            SyncClient::new(caller.db.clone(), caller.config.clone()).unwrap()
        }

        /// Make the two devices one account, as pairing will.
        fn same_account(a: &Device, b: &Device) {
            let account = a.db.lock().unwrap().account_id().unwrap();
            b.db.lock().unwrap().move_to_account(&account).unwrap();
        }

        #[tokio::test]
        async fn the_health_check_is_open_and_everything_else_needs_a_key() {
            let server = device("Server");
            let (url, _, task) = serve(&server, false);
            let http = reqwest::Client::new();

            let status = http.get(format!("{}/sync/status", url)).send().await.unwrap();
            assert_eq!(status.status(), 200);

            let bare = http.get(format!("{}/sync/changes", url)).send().await.unwrap();
            assert_eq!(bare.status(), 404, "no account named");
            let body: serde_json::Value = bare.json().await.unwrap();
            assert_eq!(body["code"], codes::ACCOUNT_UNKNOWN);

            let account = server.db.lock().unwrap().account_id().unwrap();
            let no_key = http
                .get(format!("{}/sync/changes", url))
                .header(auth::HEADER_ACCOUNT, &account)
                .header(auth::HEADER_DEVICE, "00000000000070008000000000000099")
                .send()
                .await
                .unwrap();
            assert_eq!(no_key.status(), 401);
            let body: serde_json::Value = no_key.json().await.unwrap();
            assert_eq!(body["code"], codes::KEY_MISSING);
            task.abort();
        }

        #[tokio::test]
        async fn a_paired_device_syncs_and_an_unpaired_or_revoked_one_is_refused() {
            let server = device("Server");
            let phone = device("Phone");
            let stranger = device("Stranger");
            same_account(&server, &phone);
            same_account(&server, &stranger);
            admit(&server, &phone);
            server.db.lock().unwrap().create_note("על השרת").unwrap();
            let (url, _, task) = serve(&server, false);

            let result = client(&phone, &server, &url, None).sync_with_peer(&server.id).await;
            assert!(result.success, "{:?}", result.errors);
            assert_eq!(phone.db.lock().unwrap().get_all_notes().unwrap().len(), 1);

            let result = client(&stranger, &server, &url, None).sync_with_peer(&server.id).await;
            assert!(!result.success);
            assert!(result.errors.iter().any(|e| e.contains(codes::DEVICE_UNKNOWN)), "{:?}", result.errors);
            assert!(stranger.db.lock().unwrap().get_all_notes().unwrap().is_empty());

            server.db.lock().unwrap().revoke_device(&phone.id).unwrap();
            let result = client(&phone, &server, &url, None).sync_with_peer(&server.id).await;
            assert!(!result.success);
            assert!(result.errors.iter().any(|e| e.contains(codes::DEVICE_REVOKED)), "{:?}", result.errors);
            task.abort();
        }

        #[tokio::test]
        async fn a_wrong_key_is_refused_and_repeated_refusals_are_answered_slowly() {
            let server = device("Server");
            let phone = device("Phone");
            same_account(&server, &phone);
            admit(&server, &phone);
            let (url, _, task) = serve(&server, false);
            let account = server.db.lock().unwrap().account_id().unwrap();
            let http = reqwest::Client::new();
            let mut elapsed = Vec::new();
            for _ in 0..(FREE_FAILURES + 1) {
                let started = Instant::now();
                let resp = http
                    .get(format!("{}/sync/changes", url))
                    .header(auth::HEADER_ACCOUNT, &account)
                    .header(auth::HEADER_DEVICE, &phone.id)
                    .bearer_auth("not-the-key")
                    .send()
                    .await
                    .unwrap();
                elapsed.push(started.elapsed());
                assert_eq!(resp.status(), 401);
                let body: serde_json::Value = resp.json().await.unwrap();
                assert_eq!(body["code"], codes::KEY_WRONG);
            }
            assert!(elapsed[0] < Duration::from_millis(500), "the first refusals are immediate");
            assert!(elapsed[FREE_FAILURES as usize] >= Duration::from_secs(1), "the fourth waits: {:?}", elapsed);
            task.abort();
        }

        #[tokio::test]
        async fn a_handshake_must_name_the_device_that_makes_it() {
            let server = device("Server");
            let phone = device("Phone");
            same_account(&server, &phone);
            admit(&server, &phone);
            let (url, _, task) = serve(&server, false);
            let account = server.db.lock().unwrap().account_id().unwrap();
            let key = phone.config.lock().unwrap().device_key().to_string();
            let resp = reqwest::Client::new()
                .post(format!("{}/sync/handshake", url))
                .header(auth::HEADER_ACCOUNT, &account)
                .header(auth::HEADER_DEVICE, &phone.id)
                .bearer_auth(&key)
                .json(&HandshakeRequest {
                    device_id: "00000000000070008000000000000099".to_string(),
                    device_name: "Someone else".to_string(),
                    protocol_version: PROTOCOL_VERSION.to_string(),
                    account_id: account.clone(),
                })
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 400);
            let body: serde_json::Value = resp.json().await.unwrap();
            assert_eq!(body["code"], codes::DEVICE_MISMATCH);
            task.abort();
        }

        #[tokio::test]
        async fn tls_is_verified_by_the_pin_and_never_off() {
            let server = device("Server");
            let phone = device("Phone");
            same_account(&server, &phone);
            admit(&server, &phone);
            server.db.lock().unwrap().create_note("מוצפן").unwrap();
            let (url, fingerprint, task) = serve(&server, true);
            assert!(fingerprint.starts_with("SHA256:"));

            let result = client(&phone, &server, &url, Some(&fingerprint)).sync_with_peer(&server.id).await;
            assert!(result.success, "{:?}", result.errors);
            assert_eq!(phone.db.lock().unwrap().get_all_notes().unwrap().len(), 1);

            let wrong = fingerprint.replace(|c: char| c.is_ascii_hexdigit(), "0");
            let result = client(&phone, &server, &url, Some(&wrong)).sync_with_peer(&server.id).await;
            assert!(!result.success);
            assert!(result.errors.iter().any(|e| e.contains(codes::CERTIFICATE_MISMATCH)), "{:?}", result.errors);

            // No pin: the self-signed certificate is checked against the
            // system roots and fails, because verification is never off.
            phone.config.lock().unwrap().remove_peer(&server.id).unwrap();
            let result = client(&phone, &server, &url, None).sync_with_peer(&server.id).await;
            assert!(!result.success, "an unpinned self-signed certificate must not be accepted");
            task.abort();
        }

        #[tokio::test]
        async fn plain_http_is_refused_before_any_connection_unless_it_is_this_machine() {
            let phone = device("Phone");
            let server = device("Server");
            let client = client(&phone, &server, "http://10.255.255.1:8384", None);
            let result = client.sync_with_peer(&server.id).await;
            assert!(!result.success);
            assert!(result.errors.iter().any(|e| e.contains(codes::TLS_REQUIRED)), "{:?}", result.errors);

            let refused = start_server(server.db.clone(), server.config.clone(), "0.0.0.0", 0, true).await;
            assert!(refused.unwrap_err().to_string().contains(codes::TLS_REQUIRED));
        }
    }

    mod pairing {
        use super::*;
        use crate::auth;
        use crate::config::Config;
        use crate::sync_client::SyncClient;
        use crate::sync_protocol::codes;

        struct Device {
            db: Arc<Mutex<Database>>,
            config: Arc<Mutex<Config>>,
            id: String,
            _dir: TempDir,
        }

        fn device(name: &str) -> Device {
            let dir = TempDir::new().unwrap();
            let db = Database::new(dir.path().join("notes.db")).unwrap();
            let mut config = Config::new(Some(dir.path().to_path_buf())).unwrap();
            config.set_device_name(name).unwrap();
            auth::ensure_own_device_card(&db, &mut config).unwrap();
            let id = config.device_id_hex().to_string();
            Device { db: Arc::new(Mutex::new(db)), config: Arc::new(Mutex::new(config)), id, _dir: dir }
        }

        /// Serve with the device's own certificate; return its URL and the task.
        fn serve_tls(server: &Device) -> (String, tokio::task::JoinHandle<()>) {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let port = listener.local_addr().unwrap().port();
            let (cert, key, _) = {
                let cfg = server.config.lock().unwrap();
                crate::tls::ensure_server_certificate(&cfg, false).unwrap()
            };
            // The fingerprint is on the card once the certificate exists
            {
                let db = server.db.lock().unwrap();
                let mut cfg = server.config.lock().unwrap();
                auth::ensure_own_device_card(&db, &mut cfg).unwrap();
            }
            let router = create_router(server.db.clone(), server.config.clone())
                .into_make_service_with_connect_info::<SocketAddr>();
            let config = axum_server::tls_rustls::RustlsConfig::from_config(crate::tls::server_config(&cert, &key).unwrap());
            let task = tokio::spawn(async move {
                axum_server::from_tcp_rustls(listener, config).serve(router).await.unwrap();
            });
            (format!("https://127.0.0.1:{}", port), task)
        }

        #[tokio::test]
        async fn a_fresh_device_joins_from_the_code_and_then_syncs_both_ways() {
            let desk = device("Desk");
            desk.db.lock().unwrap().create_note("על השולחן").unwrap();
            let (url, task) = serve_tls(&desk);
            let setup = {
                let db = desk.db.lock().unwrap();
                let cfg = desk.config.lock().unwrap();
                crate::pairing::offer(&db, &cfg, vec![url.clone()]).unwrap()
            };
            assert!(!setup.certificate_fingerprint.is_empty(), "the listener's fingerprint is in the code");
            let text = setup.to_text();

            let phone = device("Phone");
            assert_ne!(phone.db.lock().unwrap().account_id().unwrap(), setup.account_id);
            let client = SyncClient::new(phone.db.clone(), phone.config.clone()).unwrap();

            let joined = client.join(&text).await.unwrap();

            assert_eq!(joined.account_id, setup.account_id);
            assert_eq!(joined.peer_id, desk.id);
            assert_eq!(phone.db.lock().unwrap().account_id().unwrap(), setup.account_id, "the phone took the account");
            let key = phone.config.lock().unwrap().device_key().to_string();
            assert_eq!(key.len(), 43);
            let card_on_desk = desk.db.lock().unwrap().get_device_card(&phone.id).unwrap().unwrap();
            assert_eq!(card_on_desk.key_hash, auth::key_hash(&key), "the desk holds the phone's key hash");
            let peers = phone.config.lock().unwrap().peers().to_vec();
            assert_eq!(peers.len(), 1);
            assert_eq!(peers[0].certificate_fingerprint.as_deref(), Some(setup.certificate_fingerprint.as_str()), "the fingerprint is pinned");

            // The token is spent
            let again = device("Another");
            let refused = SyncClient::new(again.db.clone(), again.config.clone()).unwrap().join(&text).await;
            assert!(refused.unwrap_err().to_string().contains(codes::TOKEN_INVALID));

            // And now an ordinary sync works, with the key and the pin
            phone.db.lock().unwrap().create_note("מהטלפון").unwrap();
            let result = client.sync_with_peer(&desk.id).await;
            assert!(result.success, "{:?}", result.errors);
            assert_eq!(phone.db.lock().unwrap().get_all_notes().unwrap().len(), 2);
            assert_eq!(desk.db.lock().unwrap().get_all_notes().unwrap().len(), 2);
            task.abort();
        }

        #[tokio::test]
        async fn a_device_with_notes_refuses_the_code_before_any_connection() {
            let desk = device("Desk");
            let setup = {
                let db = desk.db.lock().unwrap();
                let cfg = desk.config.lock().unwrap();
                crate::pairing::offer(&db, &cfg, vec!["https://127.0.0.1:1".to_string()]).unwrap()
            };
            let phone = device("Phone");
            phone.db.lock().unwrap().create_note("כבר יש לי").unwrap();
            let client = SyncClient::new(phone.db.clone(), phone.config.clone()).unwrap();
            let err = client.join(&setup.to_text()).await.unwrap_err().to_string();
            assert!(err.contains(codes::DEVICE_HOLDS_NOTES), "{}", err);
            assert!(desk.db.lock().unwrap().has_pairing_offer(chrono::Utc::now().timestamp()).unwrap(), "the code was not spent");
        }
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

    // =========================================================================
    // NOTE DELETE CONFLICT TESTS
    // =========================================================================

    // =========================================================================
    // TAG RENAME CONFLICT TESTS
    // =========================================================================

    // =========================================================================
    // NOTE_TAG CONFLICT TESTS
    // =========================================================================

    // =========================================================================
    // COMPREHENSIVE DATA LOSS PREVENTION TESTS
    // =========================================================================

    #[test]
    fn test_apply_sync_changes_counts_conflicts_correctly() {
        // apply_sync_changes must report the conflicts flagged by the merge
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let note1_id = a.create_note("Note 1").unwrap();
        let note2_id = a.create_note("Note 2").unwrap();
        exchange(&a, &b);

        a.update_note(&note1_id, "Note 1 local edited").unwrap();
        a.update_note(&note2_id, "Note 2 local edited").unwrap();
        b.update_note(&note1_id, "Note 1 remote edit").unwrap();
        b.update_note(&note2_id, "Note 2 remote edit").unwrap();

        let (changes, _) = b.get_changes_since_as_sync_changes(None, 100000).unwrap();
        let a = Arc::new(Mutex::new(a));
        let (applied, conflicts, errors) = apply_sync_changes(
            &a,
            &changes,
            DEV_B,
            Some("Remote"),
            None,
            None,
        ).unwrap();

        assert_eq!(conflicts, 2, "Expected 2 conflicts!");
        assert!(applied > 0, "the remote versions themselves are applied");
        assert!(errors.is_empty(), "Should be no errors");
        let db = a.lock().unwrap();
        assert_eq!(db.get_unresolved_conflict_counts().unwrap()["total"], 2);
    }

    // =========================================================================
    // P0: TAG PARENT_ID CONFLICT TESTS
    // =========================================================================

    // =========================================================================
    // P0: TAG DELETION CONFLICT TESTS
    // =========================================================================

    // =========================================================================
    // P1: IDENTICAL CONTENT OPTIMIZATION TESTS
    // =========================================================================

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

        let (applied, _, errors) = apply_sync_changes(
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
            audio_data.get("storage_provider").and_then(|v| v.as_str()),
            audio_data.get("storage_key").and_then(|v| v.as_str()),
            audio_data.get("storage_uploaded_at").and_then(|v| v.as_i64()),
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
        let note_id = instance_a.create_note("Note with attachment").unwrap();
        let audio_id = instance_a.create_audio_file("recording.mp3", None).unwrap();
        let attachment_id = instance_a.attach_to_note(&note_id, &audio_id, "audio_file").unwrap();

        // Initial sync A -> B
        push_all(&instance_a, &instance_b, DEV_A);
        let b_attachments = instance_b.get_attachments_for_note(&note_id).unwrap();
        assert_eq!(b_attachments.len(), 1, "Instance B should have attachment after initial sync");

        // Now Instance A detaches the attachment
        instance_a.detach_from_note(&attachment_id).unwrap();

        // The feed carries the detach
        let (changes2, _) = instance_a.get_changes_since(None, 100).unwrap();
        let detach_change = changes2.iter().find(|c| {
            c.get("entity_type").and_then(|v| v.as_str()) == Some("note_attachment") &&
            c.get("operation").and_then(|v| v.as_str()) == Some("delete")
        });
        assert!(detach_change.is_some(), "Should have a delete operation for the attachment");

        // Sync again A -> B
        push_all(&instance_a, &instance_b, DEV_A);

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
            audio_data.get("storage_provider").and_then(|v| v.as_str()),
            audio_data.get("storage_key").and_then(|v| v.as_str()),
            audio_data.get("storage_uploaded_at").and_then(|v| v.as_i64()),
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

        // Create file_storage_config
        let config_json = serde_json::json!({
            "bucket": "test-bucket",
            "region": "us-east-1",
        });
        db.set_file_storage_config("s3", Some(&config_json)).unwrap();

        // A note emptied out of the trash: the purge travels too, or the
        // other devices would keep the note for ever.
        let doomed = db.create_note("פתק שנמחק לתמיד").unwrap();
        db.delete_note(&doomed).unwrap();
        db.purge_note(&doomed).unwrap();

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
        db.update_transcription(&transcription_id, "Hello world", None, None, Some("verified")).unwrap();

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

    // =========================================================================
    // VERSIONED MERGE TESTS (Git-style history, see versions.rs)
    // =========================================================================

    /// Push every change from `from` to `to`, as a sync would.
    fn push_all(from: &Database, to: &Database, from_device: &str) -> (i64, i64, Vec<String>) {
        let (changes, _) = from.get_changes_since_as_sync_changes(None, 100000).unwrap();
        let changes: Vec<SyncChange> = changes
            .into_iter()
            .map(|mut c| {
                c.device_id = from_device.to_string();
                c.device_name = Some(format!("Device {}", &from_device[30..]));
                c
            })
            .collect();
        apply_changes_from_peer(to, &changes, from_device, None, None, None).unwrap()
    }

    const DEV_A: &str = "00000000000070008000000000000aaa";
    const DEV_B: &str = "00000000000070008000000000000bbb";

    /// Full exchange in both directions, twice, so that merges made on one side
    /// reach the other and any resulting conflict records line up.
    fn exchange(a: &Database, b: &Database) {
        push_all(a, b, DEV_A);
        push_all(b, a, DEV_B);
        push_all(a, b, DEV_A);
    }

    fn content(db: &Database, note_id: &str) -> String {
        db.get_note(note_id).unwrap().unwrap().content
    }

    fn head_hex(db: &Database, entity_type: &str, entity_id: &str, field: &str) -> String {
        crate::versions::hex(&db.head_id(entity_type, entity_id, field).unwrap().unwrap())
    }

    /// One attachment of a note can be marked as the one that stands for
    /// it, and the choice travels to the other devices.
    #[test]
    fn the_attachment_that_stands_for_a_note_is_remembered_and_travels() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let note_id = a.create_note("פתק עם שתי הקלטות").unwrap();
        let first = a.create_audio_file("first.ogg", None).unwrap();
        let second = a.create_audio_file("second.ogg", None).unwrap();
        let first_attachment = a.attach_to_note(&note_id, &first, "audio_file").unwrap();
        let second_attachment = a.attach_to_note(&note_id, &second, "audio_file").unwrap();
        exchange(&a, &b);

        // Nothing chosen: the note says so, and the reader falls back to the
        // first recording.
        assert_eq!(a.get_primary_attachment(&note_id).unwrap(), None);

        a.set_primary_attachment(&note_id, Some(&second_attachment)).unwrap();
        assert_eq!(a.get_primary_attachment(&note_id).unwrap().as_deref(), Some(second_attachment.as_str()));
        exchange(&a, &b);
        assert_eq!(
            b.get_primary_attachment(&note_id).unwrap().as_deref(),
            Some(second_attachment.as_str()),
            "the choice reached the other device"
        );

        // Changing it again travels too, and going back to none is a choice
        // like any other.
        a.set_primary_attachment(&note_id, Some(&first_attachment)).unwrap();
        exchange(&a, &b);
        assert_eq!(b.get_primary_attachment(&note_id).unwrap().as_deref(), Some(first_attachment.as_str()));

        a.set_primary_attachment(&note_id, None).unwrap();
        exchange(&a, &b);
        assert_eq!(b.get_primary_attachment(&note_id).unwrap(), None);
    }

    /// An attachment of another note cannot be made this note's primary.
    #[test]
    fn a_note_can_only_point_at_its_own_attachment() {
        let (db, _t) = create_test_db();
        let mine = db.create_note("הפתק שלי").unwrap();
        let other = db.create_note("פתק אחר").unwrap();
        let audio = db.create_audio_file("elsewhere.ogg", None).unwrap();
        let attachment = db.attach_to_note(&other, &audio, "audio_file").unwrap();

        assert!(db.set_primary_attachment(&mine, Some(&attachment)).is_err());
        assert_eq!(db.get_primary_attachment(&mine).unwrap(), None);
    }

    /// The same for the transcription that stands for a recording.
    #[test]
    fn the_transcription_that_stands_for_a_recording_travels() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let audio = a.create_audio_file("recording.ogg", None).unwrap();
        let first = a
            .create_transcription(&audio, "תמלול ראשון", None, "local_whisper", None, None, None)
            .unwrap();
        let second = a
            .create_transcription(&audio, "תמלול שני", None, "local_whisper", None, None, None)
            .unwrap();
        exchange(&a, &b);
        assert_eq!(a.get_primary_transcription(&audio).unwrap(), None);

        a.set_primary_transcription(&audio, Some(&second)).unwrap();
        exchange(&a, &b);
        assert_eq!(b.get_primary_transcription(&audio).unwrap().as_deref(), Some(second.as_str()));

        // And a transcription of another recording is refused.
        let elsewhere = a.create_audio_file("other.ogg", None).unwrap();
        assert!(a.set_primary_transcription(&elsewhere, Some(&first)).is_err());
    }

    // =================================================================
    // The trash bin
    // =================================================================

    /// A deleted note is in the trash, not gone: it is listed there, and
    /// recovering it puts it back in front of the user.
    #[test]
    fn a_deleted_note_waits_in_the_trash_and_can_be_recovered() {
        let (db, _t) = create_test_db();
        let note_id = db.create_note("פתק שנמחק בטעות").unwrap();
        assert!(db.get_deleted_notes().unwrap().is_empty());

        db.delete_note(&note_id).unwrap();
        assert!(db.get_note(&note_id).unwrap().is_none(), "a deleted note leaves the list");
        let trash = db.get_deleted_notes().unwrap();
        assert_eq!(trash.len(), 1);
        assert_eq!(trash[0].id, note_id);
        assert_eq!(trash[0].content, "פתק שנמחק בטעות");
        assert!(trash[0].deleted_at.is_some());

        assert!(db.undelete_note(&note_id).unwrap());
        assert!(db.get_deleted_notes().unwrap().is_empty());
        let back = db.get_note(&note_id).unwrap().unwrap();
        assert_eq!(back.content, "פתק שנמחק בטעות", "the text comes back as it was");
        assert!(back.deleted_at.is_none());

        // Recovering a note that is not in the trash says so rather than
        // pretending to have done something.
        assert!(!db.undelete_note(&note_id).unwrap());
    }

    /// A recovery reaches the other devices, and the note comes back there
    /// too.
    #[test]
    fn a_recovery_travels_to_the_other_device() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let note_id = a.create_note("פתק משותף").unwrap();
        exchange(&a, &b);
        a.delete_note(&note_id).unwrap();
        exchange(&a, &b);
        assert!(b.get_note(&note_id).unwrap().is_none(), "the delete travelled");
        assert_eq!(b.get_deleted_notes().unwrap().len(), 1, "and it is in B's trash");

        a.undelete_note(&note_id).unwrap();
        exchange(&a, &b);

        assert!(b.get_note(&note_id).unwrap().is_some(), "the recovery travelled");
        assert!(b.get_deleted_notes().unwrap().is_empty());
    }

    /// Emptying a note out of the trash takes its recordings and their
    /// transcriptions with it, and only a note that is already in the trash
    /// can be emptied.
    #[test]
    fn purging_a_note_removes_it_and_what_belonged_only_to_it() {
        let (db, _t) = create_test_db();
        let note_id = db.create_note("פתק עם הקלטה").unwrap();
        let audio_id = db.create_audio_file("recording.ogg", None).unwrap();
        db.attach_to_note(&note_id, &audio_id, "audio_file").unwrap();
        let transcription_id = db
            .create_transcription(&audio_id, "תמלול ההקלטה", None, "local_whisper", None, None, None)
            .unwrap();

        // A note that is still in the list cannot be emptied out of a trash
        // it is not in.
        assert!(db.purge_note(&note_id).is_err());

        db.delete_note(&note_id).unwrap();
        let removed_audio = db.purge_note(&note_id).unwrap();
        assert_eq!(removed_audio, vec![audio_id.clone()], "the caller is told which files to delete");

        assert!(db.get_note_raw(&note_id).unwrap().is_none(), "the note itself is gone");
        assert!(db.get_deleted_notes().unwrap().is_empty(), "and it has left the trash");
        assert!(db.get_audio_file(&audio_id).unwrap().is_none(), "its recording went with it");
        assert!(db.get_transcription(&transcription_id).unwrap().is_none(), "and the transcription");
        assert!(db.is_purged("note", &note_id).unwrap());
        assert!(db.is_purged("audio_file", &audio_id).unwrap());
    }

    /// A recording that another note still holds is not taken away with the
    /// note being emptied.
    #[test]
    fn purging_keeps_a_recording_another_note_still_holds() {
        let (db, _t) = create_test_db();
        let keeper = db.create_note("הפתק שנשאר").unwrap();
        let doomed = db.create_note("הפתק שנמחק").unwrap();
        let audio_id = db.create_audio_file("shared.ogg", None).unwrap();
        db.attach_to_note(&keeper, &audio_id, "audio_file").unwrap();
        db.attach_to_note(&doomed, &audio_id, "audio_file").unwrap();

        db.delete_note(&doomed).unwrap();
        let removed_audio = db.purge_note(&doomed).unwrap();

        assert!(removed_audio.is_empty(), "nothing to delete from disk");
        assert!(db.get_audio_file(&audio_id).unwrap().is_some(), "the recording stays");
        assert_eq!(db.get_audio_files_for_note(&keeper).unwrap().len(), 1, "and the note keeps it");
    }

    /// The purge travels, and nothing brings the note back afterwards: the
    /// peer that still had it stops offering it.
    #[test]
    fn a_purge_travels_and_the_note_does_not_come_back() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let note_id = a.create_note("פתק שיימחק לתמיד").unwrap();
        let audio_id = a.create_audio_file("gone.ogg", None).unwrap();
        a.attach_to_note(&note_id, &audio_id, "audio_file").unwrap();
        exchange(&a, &b);
        assert!(b.get_note(&note_id).unwrap().is_some());

        a.delete_note(&note_id).unwrap();
        a.purge_note(&note_id).unwrap();
        exchange(&a, &b);

        assert!(b.get_note_raw(&note_id).unwrap().is_none(), "B removed it too");
        assert!(b.get_audio_file(&audio_id).unwrap().is_none(), "with its recording");
        assert!(b.is_purged("note", &note_id).unwrap());

        // B sends everything it has back to A, including anything it kept
        // about that note. A must not take it back.
        exchange(&a, &b);
        assert!(a.get_note_raw(&note_id).unwrap().is_none(), "A did not resurrect it");
        assert!(b.get_note_raw(&note_id).unwrap().is_none());
    }

    /// A device that never heard of the note before the purge does not
    /// create it from the rows a third device is still sending.
    #[test]
    fn a_purged_note_is_refused_even_when_it_arrives_first() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let note_id = a.create_note("פתק שנמחק לפני שהגיע").unwrap();
        exchange(&a, &b);

        // B empties it out of its trash while A is away.
        b.delete_note(&note_id).unwrap();
        b.purge_note(&note_id).unwrap();

        // A, which still has the note alive, tells B all about it.
        push_all(&a, &b, DEV_A);
        assert!(b.get_note_raw(&note_id).unwrap().is_none(), "B keeps it removed");

        // And when B's purge reaches A, A removes it as well.
        push_all(&b, &a, DEV_B);
        assert!(a.get_note_raw(&note_id).unwrap().is_none());
    }

    #[test]
    fn test_versions_non_overlapping_edits_merge_cleanly_on_both_sides() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let note_id = a.create_note("שורה א\nשורה ב\nשורה ג\n").unwrap();
        exchange(&a, &b);
        assert_eq!(content(&b, &note_id), "שורה א\nשורה ב\nשורה ג\n");

        // Concurrent, non-overlapping edits
        a.update_note(&note_id, "שורה א (מחשב)\nשורה ב\nשורה ג\n").unwrap();
        b.update_note(&note_id, "שורה א\nשורה ב\nשורה ג (טלפון)\n").unwrap();
        exchange(&a, &b);

        let expected = "שורה א (מחשב)\nשורה ב\nשורה ג (טלפון)\n";
        assert_eq!(content(&a, &note_id), expected);
        assert_eq!(content(&b, &note_id), expected);
        assert_eq!(head_hex(&a, "note", &note_id, "content"), head_hex(&b, "note", &note_id, "content"), "identical merge version on both devices");
        assert!(a.get_conflicts(false).unwrap().is_empty());
        assert!(b.get_conflicts(false).unwrap().is_empty());
        // Both original edits are still in the history
        let history = a.get_field_history("note", &note_id, "content").unwrap();
        assert!(history.iter().any(|v| v.content.contains("(מחשב)") && !v.content.contains("(טלפון)")));
        assert!(history.iter().any(|v| v.content.contains("(טלפון)") && !v.content.contains("(מחשב)")));
    }

    #[test]
    fn test_versions_overlapping_edits_flag_identical_conflict_on_both_sides() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let note_id = a.create_note("כותרת\nגוף\n").unwrap();
        exchange(&a, &b);

        a.update_note(&note_id, "כותרת חדשה במחשב\nגוף\n").unwrap();
        b.update_note(&note_id, "כותרת חדשה בטלפון\nגוף\n").unwrap();
        exchange(&a, &b);

        let ca = content(&a, &note_id);
        let cb = content(&b, &note_id);
        assert_eq!(ca, cb, "both devices render the same merged text");
        assert!(ca.contains("<<<<<<< VERSION A\n"), "{}", ca);
        assert!(ca.contains("כותרת חדשה במחשב") && ca.contains("כותרת חדשה בטלפון"));
        assert!(ca.contains(">>>>>>> VERSION B\n"));
        assert!(ca.ends_with("גוף\n"));

        let conf_a = a.get_conflicts(false).unwrap();
        let conf_b = b.get_conflicts(false).unwrap();
        assert_eq!(conf_a.len(), 1);
        assert_eq!(conf_b.len(), 1);
        assert_eq!(conf_a[0].id, conf_b[0].id, "same conflict id everywhere");
        assert_eq!(conf_a[0].kind, "text");
        assert_eq!(a.get_note_conflict_types(&note_id).unwrap(), vec!["content".to_string()]);

        // Resolving by editing the note on A clears it on B after a sync
        a.update_note(&note_id, "כותרת משולבת\nגוף\n").unwrap();
        assert!(a.get_conflicts(false).unwrap().is_empty());
        exchange(&a, &b);
        assert_eq!(content(&b, &note_id), "כותרת משולבת\nגוף\n");
        assert!(b.get_conflicts(false).unwrap().is_empty());
        assert!(b.get_note_conflict_types(&note_id).unwrap().is_empty());
    }

    #[test]
    fn test_versions_accept_conflict_propagates() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let note_id = a.create_note("x\n").unwrap();
        exchange(&a, &b);
        a.update_note(&note_id, "xa\n").unwrap();
        b.update_note(&note_id, "xb\n").unwrap();
        exchange(&a, &b);
        let conflict = a.get_conflicts(false).unwrap().remove(0);

        assert!(a.accept_conflict(&conflict.id).unwrap());
        assert!(a.get_conflicts(false).unwrap().is_empty());
        let accepted = content(&a, &note_id);
        assert!(accepted.contains("<<<<<<<"), "accepted text keeps the markers until edited");

        exchange(&a, &b);
        assert!(b.get_conflicts(false).unwrap().is_empty());
        assert_eq!(content(&b, &note_id), accepted);
        assert_eq!(head_hex(&a, "note", &note_id, "content"), head_hex(&b, "note", &note_id, "content"));
    }

    #[test]
    fn test_versions_delete_without_concurrent_edit_propagates() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let note_id = a.create_note("למחיקה").unwrap();
        exchange(&a, &b);
        assert!(a.delete_note(&note_id).unwrap());
        exchange(&a, &b);
        assert!(b.get_note_raw(&note_id).unwrap().unwrap()["deleted_at"].is_i64());
        assert!(b.get_conflicts(false).unwrap().is_empty());
    }

    #[test]
    fn test_versions_delete_versus_edit_keeps_the_edit_and_flags() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let note_id = a.create_note("מקורי").unwrap();
        exchange(&a, &b);

        a.delete_note(&note_id).unwrap();
        b.update_note(&note_id, "מקורי ועוד").unwrap();
        exchange(&a, &b);

        for db in [&a, &b] {
            let note = db.get_note_raw(&note_id).unwrap().unwrap();
            assert!(note["deleted_at"].is_null(), "the edit wins over the concurrent delete");
            assert_eq!(note["content"].as_str().unwrap(), "מקורי ועוד");
            let kinds = db.get_note_conflict_types(&note_id).unwrap();
            assert!(kinds.contains(&"delete".to_string()), "{:?}", kinds);
        }
        assert_eq!(head_hex(&a, "note", &note_id, "deleted"), head_hex(&b, "note", &note_id, "deleted"));

        // Deleting again after seeing the edit is a normal delete
        a.delete_note(&note_id).unwrap();
        exchange(&a, &b);
        assert!(b.get_note_raw(&note_id).unwrap().unwrap()["deleted_at"].is_i64());
    }

    #[test]
    fn test_versions_tag_rename_both_sides_flags_scalar_conflict() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let tag_id = a.create_tag("עבודה", None).unwrap();
        exchange(&a, &b);

        a.rename_tag(&tag_id, "משרד").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1100));
        b.rename_tag(&tag_id, "פרויקטים").unwrap();
        exchange(&a, &b);

        let name_a = a.get_tag(&tag_id).unwrap().unwrap().name;
        let name_b = b.get_tag(&tag_id).unwrap().unwrap().name;
        assert_eq!(name_a, name_b);
        assert!(!name_a.contains('|'), "no combined names");
        assert_eq!(name_a, "פרויקטים", "the later rename stays live");
        let conflicts = a.get_conflicts(false).unwrap();
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].kind, "scalar");
        assert_eq!(conflicts[0].entity_type, "tag");
        // Both names remain in history
        let names: Vec<String> = a.get_field_history("tag", &tag_id, "name").unwrap().into_iter().map(|v| v.content).collect();
        assert!(names.contains(&"משרד".to_string()) && names.contains(&"פרויקטים".to_string()));
    }

    #[test]
    fn test_versions_tag_reparent_delete_and_new_tag_links_propagate() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let parent = a.create_tag("הורה", None).unwrap();
        let child = a.create_tag("ילד", None).unwrap();
        let note_id = a.create_note("פתק").unwrap();
        exchange(&a, &b);

        // Move, attach a brand-new tag, delete another: all in one batch
        a.reparent_tag(&child, Some(&parent)).unwrap();
        let fresh = a.create_tag("חדש", Some(&parent)).unwrap();
        a.add_tag_to_note(&note_id, &fresh).unwrap();
        let doomed = a.create_tag("למחיקה", None).unwrap();
        a.delete_tag(&doomed).unwrap();
        push_all(&a, &b, DEV_A);

        assert_eq!(b.get_tag(&child).unwrap().unwrap().parent_id.as_deref(), Some(parent.as_str()));
        let fresh_b = b.get_tag(&fresh).unwrap().expect("new tag arrived");
        assert_eq!(fresh_b.name, "חדש");
        assert_eq!(fresh_b.parent_id.as_deref(), Some(parent.as_str()));
        let note_tags: Vec<String> = b.get_note_tags(&note_id).unwrap().into_iter().map(|t| t.id).collect();
        assert!(note_tags.contains(&fresh), "link to the brand-new tag arrived with it");
        assert!(b.get_tag(&doomed).unwrap().is_none() || b.get_tag_raw(&doomed).unwrap().unwrap()["deleted_at"].is_i64());

        // Removing the tag on B propagates back
        b.remove_tag_from_note(&note_id, &fresh).unwrap();
        push_all(&b, &a, DEV_B);
        let note_tags_a: Vec<String> = a.get_note_tags(&note_id).unwrap().into_iter().map(|t| t.id).collect();
        assert!(!note_tags_a.contains(&fresh));
    }

    #[test]
    fn test_versions_tag_remove_versus_readd_keeps_link_and_flags() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let tag = a.create_tag("תג", None).unwrap();
        let note_id = a.create_note("פתק").unwrap();
        a.add_tag_to_note(&note_id, &tag).unwrap();
        exchange(&a, &b);

        // A removes; B removes and re-adds (so B's head says attached, A's says detached)
        a.remove_tag_from_note(&note_id, &tag).unwrap();
        b.remove_tag_from_note(&note_id, &tag).unwrap();
        b.add_tag_to_note(&note_id, &tag).unwrap();
        exchange(&a, &b);

        for db in [&a, &b] {
            let tags: Vec<String> = db.get_note_tags(&note_id).unwrap().into_iter().map(|t| t.id).collect();
            assert!(tags.contains(&tag), "link is kept, never silently dropped");
            let kinds = db.get_note_conflict_types(&note_id).unwrap();
            assert!(kinds.contains(&"tag".to_string()), "{:?}", kinds);
        }
    }

    #[test]
    fn test_versions_transcription_flags_and_text_merge() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let audio = a.create_audio_file("הקלטה.mp3", None).unwrap();
        let tr = a.create_transcription(&audio, "שלום\nעולם\n", None, "whisper", None, None, None).unwrap();
        exchange(&a, &b);

        // A verifies; B cleans and edits line 2. Flags and text merge independently.
        a.update_transcription(&tr, "שלום\nעולם\n", None, None, Some("original verified !verbatim !cleaned !polished")).unwrap();
        b.update_transcription(&tr, "שלום\nעולם!\n", None, None, Some("original !verified !verbatim cleaned !polished")).unwrap();
        exchange(&a, &b);

        for db in [&a, &b] {
            let t = db.get_transcription(&tr).unwrap().unwrap();
            assert_eq!(t.content, "שלום\nעולם!\n");
            assert_eq!(t.state, "original verified !verbatim cleaned !polished");
        }
        assert!(a.get_conflicts(false).unwrap().is_empty());
    }

    #[test]
    fn test_versions_no_silent_overwrite_when_pull_arrives_before_push() {
        // Device B edits offline, then pulls A's newer edit before pushing its own.
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let note_id = a.create_note("בסיס\nאמצע\nשורה שלישית\n").unwrap();
        exchange(&a, &b);

        b.update_note(&note_id, "בסיס\nאמצע\nשורה שלישית מהטלפון\n").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1100));
        a.update_note(&note_id, "בסיס מהמחשב\nאמצע\nשורה שלישית\n").unwrap();

        // Pull only (A -> B): B must keep its own edit.
        push_all(&a, &b, DEV_A);
        let cb = content(&b, &note_id);
        assert!(cb.contains("מהטלפון"), "B's unsynced edit survived the pull: {}", cb);
        assert!(cb.contains("מהמחשב"));
        assert!(b.get_conflicts(false).unwrap().is_empty());
    }

    #[test]
    fn test_versions_failed_change_is_queued_and_retried() {
        let (db, _t) = create_test_db();
        let bad = make_sync_change(
            "transcription",
            "00000000000070008000000000000123",
            "create",
            serde_json::json!({
                "id": "00000000000070008000000000000123",
                "audio_file_id": "not-a-uuid",
                "content": "x",
                "service": "whisper",
                "state": "original",
                "device_id": DEV_A,
                "created_at": 1735689600,
            }),
            DEV_A,
        );
        let (applied, _, errors) = apply_changes_from_peer(&db, &[bad], DEV_A, None, None, None).unwrap();
        assert_eq!(applied, 0);
        assert_eq!(errors.len(), 1, "{:?}", errors);
        assert_eq!(db.count_pending_sync_failures().unwrap(), 1, "kept for retry, not dropped");

        // Next batch retries it (still failing) and keeps it queued
        let (_, _, errors) = apply_changes_from_peer(&db, &[], DEV_A, None, None, None).unwrap();
        assert!(errors.is_empty());
        assert_eq!(db.count_pending_sync_failures().unwrap(), 1);
    }

    #[test]
    fn test_versions_first_edit_of_a_field_is_stamped_with_its_time() {
        // The first version of a field written by a device (here: the delete
        // tombstone of a note that was never deleted before) is a real edit and
        // must carry its timestamp, or the feed would show deleted_at = 0.
        let (db, _t) = create_test_db();
        let note_id = db.create_note("למחיקה").unwrap();
        db.delete_note(&note_id).unwrap();
        let raw = db.get_note_raw(&note_id).unwrap().unwrap();
        assert!(raw["deleted_at"].as_i64().unwrap_or(0) > 0, "{:?}", raw);
        assert!(raw["modified_at"].as_i64().unwrap_or(0) > 0, "{:?}", raw);
        let (changes, _) = db.get_changes_since(None, 100).unwrap();
        let del = changes.iter().find(|c| c["entity_type"] == "note" && c["operation"] == "delete").unwrap();
        assert!(del["data"]["deleted_at"].as_i64().unwrap() > 0);
    }

    #[test]
    fn test_files_storage_key_reaches_peer_that_edited_summary_first() {
        // A imports and uploads; B, unaware, edits the summary (a newer row);
        // after the exchange both know the cloud location and the summary.
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let audio = a.create_audio_file("הקלטה.mp3", None).unwrap();
        exchange(&a, &b);
        b.update_audio_file_summary(&audio, "סיכום מהטלפון").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1100));
        a.update_audio_file_storage(&audio, "s3", &format!("audio/{}.mp3", audio)).unwrap();
        exchange(&a, &b);
        exchange(&a, &b);
        for db in [&a, &b] {
            let row = db.get_audio_file_raw(&audio).unwrap().unwrap();
            assert_eq!(row["storage_key"].as_str().unwrap(), format!("audio/{}.mp3", audio));
            assert_eq!(row["summary"].as_str().unwrap(), "סיכום מהטלפון");
        }
    }

    #[test]
    fn test_files_older_audio_row_never_erases_storage_key() {
        let (a, _ta) = create_test_db();
        let audio = a.create_audio_file("הקלטה.mp3", None).unwrap();
        a.update_audio_file_storage(&audio, "s3", &format!("audio/{}.mp3", audio)).unwrap();
        // An older copy of the row (from a peer that never saw the upload)
        a.apply_sync_audio_file(&audio, 1735689600, "הקלטה.mp3", None, None, None, Some(1735689600), None, Some(1735689601), None, None, None, None).unwrap();
        let row = a.get_audio_file_raw(&audio).unwrap().unwrap();
        assert_eq!(row["storage_key"].as_str().unwrap(), format!("audio/{}.mp3", audio));
        assert_eq!(row["storage_provider"].as_str().unwrap(), "s3");
    }

    #[test]
    fn test_older_transcription_row_does_not_overwrite_newer_service_response() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let note = a.create_note("פתק").unwrap();
        let audio = a.create_audio_file("הקלטה.mp3", None).unwrap();
        a.attach_to_note(&note, &audio, "audio_file").unwrap();
        let tr = a.create_transcription(&audio, "טקסט", None, "whisper", None, Some("{\"run\":1}"), None).unwrap();
        exchange(&a, &b);
        std::thread::sleep(std::time::Duration::from_millis(1100));
        // A re-runs the service; B still holds the first response
        a.update_transcription(&tr, "טקסט", None, Some("{\"run\":2}"), None).unwrap();
        // B's (older) row reaches A first, then A's reaches B
        push_all(&b, &a, DEV_B);
        push_all(&a, &b, DEV_A);
        for db in [&a, &b] {
            let t = db.get_transcription(&tr).unwrap().unwrap();
            assert_eq!(t.service_response.as_deref(), Some("{\"run\":2}"), "the newer response must survive on every device");
        }
    }

    #[test]
    fn test_transcription_state_toggle_keeps_service_metadata() {
        let (a, _ta) = create_test_db();
        let audio = a.create_audio_file("הקלטה.mp3", None).unwrap();
        let tr = a.create_transcription(&audio, "טקסט", Some("[1,2]"), "whisper", None, Some("{\"ok\":true}"), None).unwrap();
        a.update_transcription(&tr, "טקסט", None, None, Some("original verified")).unwrap();
        let t = a.get_transcription(&tr).unwrap().unwrap();
        assert_eq!(t.content_segments.as_deref(), Some("[1,2]"));
        assert_eq!(t.service_response.as_deref(), Some("{\"ok\":true}"));
        assert!(t.state.contains("verified"));
    }

    #[test]
    fn test_older_attachment_row_does_not_move_attachment_back() {
        let (a, _ta) = create_test_db();
        let n1 = a.create_note("ראשון").unwrap();
        let n2 = a.create_note("שני").unwrap();
        let audio = a.create_audio_file("הקלטה.mp3", None).unwrap();
        let att = a.attach_to_note(&n1, &audio, "audio_file").unwrap();
        let created = a.get_attachment(&att).unwrap().unwrap().created_at;
        // The attachment was moved to n2 (a note merge) at t+100
        a.apply_sync_note_attachment(&att, &n2, &audio, "audio_file", created, Some(created + 100), None, Some(created + 100)).unwrap();
        // An older echo still says n1
        a.apply_sync_note_attachment(&att, &n1, &audio, "audio_file", created, Some(created + 50), None, Some(created + 150)).unwrap();
        assert_eq!(a.get_attachment(&att).unwrap().unwrap().note_id, n2);
    }

    #[test]
    fn test_versions_feed_carries_history_and_full_dataset_includes_it() {
        let (db, _t) = create_test_db();
        let note_id = db.create_note("א").unwrap();
        db.update_note(&note_id, "ב").unwrap();
        let (changes, _) = db.get_changes_since(None, 1000).unwrap();
        let versions: Vec<_> = changes.iter().filter(|c| c["entity_type"] == "field_version").collect();
        assert!(versions.len() >= 2, "root and edit versions in the feed: {}", versions.len());
        let full = db.get_full_dataset().unwrap();
        assert!(full["field_versions"].len() >= 2);
    }

    #[test]
    fn test_versions_migration_roots_are_identical_across_devices() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        // Pre-versioning data on both devices: the same note row
        for db in [&a, &b] {
            db.connection().execute(
                "INSERT INTO notes (id, created_at, content) VALUES (?, 1735689600, ?)",
                rusqlite::params![vec![7u8; 16], "תוכן ישן"],
            ).unwrap();
            db.connection().execute_batch("DELETE FROM field_heads; DELETE FROM field_versions;").unwrap();
            db.migrate_create_root_versions().unwrap();
        }
        let id = crate::versions::hex(&[7u8; 16]);
        assert_eq!(head_hex(&a, "note", &id, "content"), head_hex(&b, "note", &id, "content"));
        // And syncing afterwards merges nothing
        exchange(&a, &b);
        assert!(a.get_conflicts(false).unwrap().is_empty());
        assert_eq!(content(&a, &id), "תוכן ישן");
    }
}
