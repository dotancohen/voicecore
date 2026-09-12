//! The messages of the sync protocol, shared by the client (`sync_client.rs`)
//! and the server (`sync_server.rs`).
//!
//! One definition per message, so the two sides cannot drift apart. Fields a
//! peer may leave out carry `serde(default)`, so a response from an older
//! peer still parses; the sender always writes every field.

use serde::{Deserialize, Serialize};

use crate::models::SyncChange;

/// The protocol version both sides announce in the handshake and the status.
pub const PROTOCOL_VERSION: &str = "1.1";

/// `POST /sync/handshake` request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeRequest {
    pub device_id: String,
    pub device_name: String,
    pub protocol_version: String,
}

/// `POST /sync/handshake` response body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeResponse {
    pub device_id: String,
    pub device_name: String,
    pub protocol_version: String,
    pub last_sync_timestamp: Option<i64>,
    #[serde(default)]
    pub server_timestamp: i64,
    #[serde(default)]
    pub supports_audiofiles: bool,
    /// Identity of the responder's database; a change means the peer must
    /// forget its cursors.
    #[serde(default)]
    pub database_id: String,
    /// Current end of the responder's write-order feed.
    #[serde(default)]
    pub cursor: i64,
}

/// `GET /sync/changes` query parameters.
#[derive(Debug, Clone, Deserialize)]
pub struct ChangesQuery {
    /// Write-order cursor (primary). Takes precedence over `since`.
    pub cursor: Option<i64>,
    /// Timestamp filter (kept for tools and older clients).
    pub since: Option<i64>,
    pub limit: Option<i64>,
}

/// `GET /sync/changes` response body: one page of the feed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangesResponse {
    pub changes: Vec<SyncChange>,
    pub from_timestamp: Option<i64>,
    pub to_timestamp: Option<i64>,
    /// Pass back as `cursor` to continue (cursor mode only).
    pub next_cursor: Option<i64>,
    #[serde(default)]
    pub database_id: String,
    pub device_id: String,
    #[serde(default)]
    pub device_name: String,
    pub is_complete: bool,
}

/// `POST /sync/apply` request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplyRequest {
    pub device_id: String,
    pub device_name: String,
    pub changes: Vec<SyncChange>,
}

/// `POST /sync/apply` response body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplyResponse {
    pub applied: i64,
    pub conflicts: i64,
    pub errors: Vec<String>,
}

/// `GET /sync/status` response body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResponse {
    pub device_id: String,
    pub device_name: String,
    pub protocol_version: String,
    pub status: String,
    pub supports_audiofiles: bool,
}

/// The body of every error response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}
