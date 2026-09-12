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

/// The codes a refusal carries, so a sentence on a screen and a line in a
/// log can be matched to the rule that produced them.
pub mod codes {
    /// The handshake named no account.
    pub const ACCOUNT_MISSING: &str = "ACCOUNT_MISSING";
    /// The two sides hold different accounts; nothing is exchanged.
    pub const ACCOUNT_MISMATCH: &str = "ACCOUNT_MISMATCH";
    /// A database opened for one account already belongs to another.
    pub const ACCOUNT_DISAGREES: &str = "ACCOUNT_DISAGREES";
    /// The request names an account this server does not hold.
    pub const ACCOUNT_UNKNOWN: &str = "ACCOUNT_UNKNOWN";
    /// The request carries no device key, or no device id.
    pub const KEY_MISSING: &str = "KEY_MISSING";
    /// The account holds no card for the device named in the request.
    pub const DEVICE_UNKNOWN: &str = "DEVICE_UNKNOWN";
    /// The device's card is marked revoked.
    pub const DEVICE_REVOKED: &str = "DEVICE_REVOKED";
    /// The key does not hash to the device card's key hash.
    pub const KEY_WRONG: &str = "KEY_WRONG";
    /// The handshake body names a device other than the headers do.
    pub const DEVICE_MISMATCH: &str = "DEVICE_MISMATCH";
    /// A peer's address is plain http and not this machine.
    pub const TLS_REQUIRED: &str = "TLS_REQUIRED";
    /// The peer's certificate is not the pinned one.
    pub const CERTIFICATE_MISMATCH: &str = "CERTIFICATE_MISMATCH";
}

/// `POST /sync/handshake` request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeRequest {
    pub device_id: String,
    pub device_name: String,
    pub protocol_version: String,
    /// The account the caller holds (ACCT-2). Empty means "none named",
    /// which is refused.
    #[serde(default)]
    pub account_id: String,
}

/// `POST /sync/handshake` response body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeResponse {
    pub device_id: String,
    pub device_name: String,
    pub protocol_version: String,
    /// The account the responder holds (ACCT-2), so the caller can check it
    /// reached the account it meant to.
    #[serde(default)]
    pub account_id: String,
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
    /// One of [`codes`], or empty for an error that has none.
    #[serde(default)]
    pub code: String,
}

impl ErrorResponse {
    pub fn new(error: impl Into<String>) -> Self {
        Self { error: error.into(), code: String::new() }
    }

    pub fn with_code(error: impl Into<String>, code: &str) -> Self {
        Self { error: error.into(), code: code.to_string() }
    }
}
