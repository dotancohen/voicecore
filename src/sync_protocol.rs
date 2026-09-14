//! The messages of the sync protocol, shared by the client (`sync_client.rs`)
//! and the server (`sync_server.rs`).
//!
//! One definition per message, so the two sides cannot drift apart. The
//! sender always writes every field.

use serde::{Deserialize, Serialize};

use crate::models::SyncChange;

/// The protocol version both sides announce in the handshake and the status
/// (Stage 16): the headers, the device cards, pairing and the new entity
/// types changed the exchange, so a peer that announces `1.x` is refused
/// with `PROTOCOL_TOO_OLD`. Everything starts afresh; nothing negotiates
/// with version 1.
pub const PROTOCOL_VERSION: &str = "2.0";

/// The major version this build speaks; a peer below it is refused.
pub const PROTOCOL_MAJOR: u32 = 2;

/// The major number of a version text ("2.0" → 2), or None for nonsense.
pub fn protocol_major(version: &str) -> Option<u32> {
    version.trim().split('.').next().and_then(|m| m.parse().ok())
}

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
    /// No address is known for the peer yet (its card names none, and none was given).
    pub const NO_ADDRESS: &str = "NO_ADDRESS";
    /// The peer's certificate is not the pinned one.
    pub const CERTIFICATE_MISMATCH: &str = "CERTIFICATE_MISMATCH";
    /// The pairing token is unknown, spent, expired or mistyped.
    pub const TOKEN_INVALID: &str = "TOKEN_INVALID";
    /// The setup text could not be read.
    pub const SETUP_TEXT_INVALID: &str = "SETUP_TEXT_INVALID";
    /// This device holds notes of another account and will not be paired over them.
    pub const DEVICE_HOLDS_NOTES: &str = "DEVICE_HOLDS_NOTES";
    /// The peer speaks a protocol version below this build's (Stage 16)
    pub const PROTOCOL_TOO_OLD: &str = "PROTOCOL_TOO_OLD";
    /// The caller is not on a private network and this listener has no public address (LISTEN-3)
    pub const NOT_ON_LAN: &str = "NOT_ON_LAN";
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
    /// Which application calls (Stage 16): "voice", later "images".
    #[serde(default)]
    pub application: String,
    /// The entity types the caller wants and understands (Stage 16). Empty
    /// means every type; an image application declares `["tag"]` and
    /// receives and sends tags and nothing else.
    #[serde(default)]
    pub entity_types: Vec<String>,
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
    /// Which application answers (Stage 16)
    #[serde(default)]
    pub application: String,
    #[serde(default)]
    pub server_timestamp: i64,
    #[serde(default)]
    pub supports_audiofiles: bool,
    /// Bytes free on the responder's disk, for the connection check (Stage
    /// 12); 0 when unknown.
    #[serde(default)]
    pub free_bytes: u64,
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
    /// Write-order cursor; absent is the start of the feed.
    pub cursor: Option<i64>,
    pub limit: Option<i64>,
    /// Only these entity types, comma-separated (Stage 16); absent means every type.
    pub types: Option<String>,
}

/// `GET /sync/changes` response body: one page of the feed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangesResponse {
    pub changes: Vec<SyncChange>,
    /// Pass back as `cursor` to continue.
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

/// `POST /pair/claim` request body (PAIR-3): the reading device presents the
/// token from the code and describes itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairClaimRequest {
    pub token: String,
    /// The account the code was for, so a server that hosts several finds
    /// the right one
    #[serde(default)]
    pub account_id: String,
    pub device_id: String,
    pub device_name: String,
    #[serde(default)]
    pub certificate_fingerprint: String,
    /// JSON list of URLs the reader listens on, or empty
    #[serde(default)]
    pub addresses: String,
    #[serde(default)]
    pub application: String,
}

/// `POST /pair/claim` response body: the account, the key made for the
/// reader, and the shower's own card so the reader can reach it back.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairClaimResponse {
    pub account_id: String,
    pub device_key: String,
    pub device_id: String,
    pub device_name: String,
    #[serde(default)]
    pub certificate_fingerprint: String,
    #[serde(default)]
    pub addresses: String,
    /// The account's recording key (Stage 15, ENC-1), when the shower holds one
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recording_key: Option<String>,
}

/// `POST /pair/grant` request body (PAIR-5): the holder of an account gives
/// an empty device (a server) the account, a key it made for the device,
/// and its own card, so the device can serve the account and let the
/// holder in.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairGrantRequest {
    pub token: String,
    pub account_id: String,
    #[serde(default)]
    pub label: String,
    /// The key the empty device will use as its own for this account
    pub device_key: String,
    /// The holder's card, with its key hash
    pub holder_id: String,
    pub holder_name: String,
    #[serde(default)]
    pub holder_certificate_fingerprint: String,
    #[serde(default)]
    pub holder_addresses: String,
    pub holder_key_hash: String,
    /// The account's recording key (Stage 15, ENC-1), when the holder has one
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recording_key: Option<String>,
}

/// `POST /pair/grant` response body: the empty device's own card.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairGrantResponse {
    pub account_id: String,
    pub device_id: String,
    pub device_name: String,
    #[serde(default)]
    pub certificate_fingerprint: String,
    #[serde(default)]
    pub addresses: String,
    #[serde(default)]
    pub key_hash: String,
}

/// `POST /sync/audio/missing` request body (FILE-12): the recordings the
/// sender holds, so the receiver can say which it lacks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MissingFilesRequest {
    pub audio_ids: Vec<String>,
}

/// `POST /sync/audio/missing` response body: the ids the receiver lacks,
/// and for those it holds a part of, how many bytes it has (FILE-13).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MissingFilesResponse {
    pub missing: Vec<String>,
    #[serde(default)]
    pub partial: std::collections::HashMap<String, u64>,
}

/// The header a sender puts the whole file's hex SHA-256 in, so the
/// receiver can verify what it assembled (FILE-13).
pub const HEADER_FILE_SHA256: &str = "x-file-sha256";
/// `1` on a served recording whose bytes are encrypted with the recording
/// key (Stage 15, ENC-4): a device without the key kept the object as it was.
pub const HEADER_ENCRYPTED: &str = "x-voice-encrypted";

/// One id per operation (a button press), sent on every request of it and
/// written in every log line on both sides (Stage 12).
pub const HEADER_REQUEST_ID: &str = "x-request-id";

/// The shape of a request id: up to 32 characters of hex, made by the
/// initiator. Anything else is replaced by "-" in the logs.
pub fn request_id_or_dash(value: Option<&str>) -> String {
    match value {
        Some(v) if !v.is_empty() && v.len() <= 32 && v.chars().all(|c| c.is_ascii_hexdigit()) => v.to_string(),
        _ => "-".to_string(),
    }
}

/// One row of a connection check (Stage 12): what was checked, whether it
/// passed, a sentence, and the refusal code when there is one.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckRow {
    pub name: String,
    pub passed: bool,
    pub detail: String,
    #[serde(default)]
    pub code: String,
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

/// `POST /sync/audio/:id/keep` response (FILE-26): whether the device promised
/// to keep its copy of the recording while the caller removes its own, until
/// when (milliseconds), or why it did not.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeepResponse {
    pub holds: bool,
    #[serde(default)]
    pub until_ms: i64,
    #[serde(default)]
    pub reason: String,
}
