//! Database operations for Voice.
//!
//! This module provides all data access functionality using SQLite.
//! All methods return JSON-serializable types to support CLI, web server, and Python modes.
//!
//! UUIDs are stored as BLOB (16 bytes) and converted to hex strings for JSON output.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension, Row};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{VoiceError, VoiceResult};
use crate::models::SyncChange;

/// A peer known to hold a copy of a recording (Stage 10).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyRow {
    pub peer_id: String,
    pub at: i64,
}

/// What exists on this device only (Stage 10).
/// An upload in parts begun earlier (Stage 13), as the journal has it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadBegun {
    pub storage_key: String,
    pub upload_id: String,
    pub part_size: u64,
    /// The parts uploaded, each with the tag the bucket gave it
    pub parts: Vec<(u32, String)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NotDuplicated {
    pub notes: i64,
    pub recordings: i64,
}

/// A peer as `sync_peers` remembers it: when it was last reached, and by
/// which operation (Stage 10).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerSummary {
    pub peer_id: String,
    pub peer_name: Option<String>,
    pub last_reached_at: Option<i64>,
    pub last_operation: Option<String>,
}
use crate::versions::{
    note_tag_entity_id, ENTITY_AUDIO_FILE, ENTITY_NOTE, ENTITY_NOTE_ATTACHMENT, ENTITY_NOTE_TAG,
    ENTITY_TAG, ENTITY_TRANSCRIPTION, FIELD_ACTIVE, FIELD_CONTENT, FIELD_DELETED, FIELD_NAME,
    FIELD_PARENT, FIELD_PRIMARY_ATTACHMENT, FIELD_PRIMARY_TRANSCRIPTION, FIELD_STATE, FIELD_SUMMARY,
};
use crate::validation::{
    validate_note_id, validate_search_query, validate_tag_id, validate_tag_path,
};

// Global device ID for local operations
static LOCAL_DEVICE_ID: OnceLock<Uuid> = OnceLock::new();
static LOCAL_DEVICE_NAME: Mutex<Option<String>> = Mutex::new(None);

/// Set the human-readable name of this device, recorded on every version it creates.
pub fn set_local_device_name(name: &str) {
    if let Ok(mut n) = LOCAL_DEVICE_NAME.lock() {
        *n = Some(name.to_string());
    }
}

/// Name of this device, if configured.
pub fn get_local_device_name() -> Option<String> {
    LOCAL_DEVICE_NAME.lock().ok().and_then(|n| n.clone())
}

/// System tag name - parent of all hidden system tags (e.g., _marked)
pub const SYSTEM_TAG_NAME: &str = "_system";

/// Marked tag name - child of _system, used for starring/bookmarking notes
pub const MARKED_TAG_NAME: &str = "_marked";

/// Nonsynced tag name - child of _system, parent of tags for non-synced items
pub const NONSYNCED_TAG_NAME: &str = "_nonsynced";

/// Too-big tag name - child of _nonsynced, for files too large to sync
pub const TOO_BIG_TAG_NAME: &str = "_too-big";

/// Deterministic UUID for _system tag - same on all devices to prevent duplicates during sync.
pub const SYSTEM_TAG_UUID: &str = "a1b2c3d4-0000-5000-8000-000000000001";

/// Deterministic UUID for _marked tag - same on all devices to prevent duplicates during sync.
pub const MARKED_TAG_UUID: &str = "a1b2c3d4-0000-5000-8000-000000000002";

/// Deterministic UUID for _nonsynced tag - same on all devices to prevent duplicates during sync.
pub const NONSYNCED_TAG_UUID: &str = "a1b2c3d4-0000-5000-8000-000000000003";

/// Deterministic UUID for _too-big tag - same on all devices to prevent duplicates during sync.
pub const TOO_BIG_TAG_UUID: &str = "a1b2c3d4-0000-5000-8000-000000000004";

// =============================================================================
// Cache Registry - All di_cache_* fields in the database
// =============================================================================
// This registry lists all cache columns that can be rebuilt. When adding a new
// cache column to any table, add it here so it's included in rebuild-all-caches.

/// Describes a cache field in the database
#[derive(Debug, Clone)]
pub struct CacheFieldInfo {
    /// The table containing this cache field
    pub table: &'static str,
    /// The column name (e.g., "di_cache_note_pane_display")
    pub column: &'static str,
    /// Human-readable description
    pub description: &'static str,
}

/// Registry of all cache fields in the database.
/// Add new cache columns here when they are created.
pub static CACHE_REGISTRY: &[CacheFieldInfo] = &[
    CacheFieldInfo {
        table: "notes",
        column: "di_cache_note_pane_display",
        description: "Note pane display cache (tags, conflicts, attachments)",
    },
    CacheFieldInfo {
        table: "notes",
        column: "di_cache_note_list_pane_display",
        description: "Notes list pane display cache (date, marked, content_preview)",
    },
];

/// Summary of a cache rebuild operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheRebuildSummary {
    /// Number of notes processed
    pub notes_processed: u32,
    /// Number of cache fields rebuilt per note
    pub cache_fields_rebuilt: u32,
    /// Errors encountered during rebuild (if any)
    pub errors: Vec<String>,
}

/// Set the local device ID for database operations.
pub fn set_local_device_id(device_id: Uuid) {
    let _ = LOCAL_DEVICE_ID.set(device_id);
}

/// Get the local device ID, generating one if not set.
pub fn get_local_device_id() -> Uuid {
    *LOCAL_DEVICE_ID.get_or_init(Uuid::now_v7)
}


/// Every timestamp that carries the timezone of the action beside it, as
/// `<stamp>_offset` (seconds east of UTC) and `<stamp>_zone` (IANA name).
pub const STAMPED_COLUMNS: &[(&str, &[&str])] = &[
    ("notes", &["created_at", "modified_at", "deleted_at"]),
    ("tags", &["created_at", "modified_at", "deleted_at"]),
    ("note_tags", &["created_at", "modified_at", "deleted_at"]),
    ("note_attachments", &["created_at", "modified_at", "deleted_at"]),
    ("audio_files", &["imported_at", "file_created_at", "modified_at", "deleted_at"]),
    ("transcriptions", &["created_at", "modified_at", "deleted_at"]),
    ("field_versions", &["created_at"]),
];

/// Largest page of the cursor feed, in bytes of JSON (roughly). Kept well
/// under the server body limit and small enough for a slow link to finish
/// within the client timeout.
pub const FEED_BYTE_BUDGET: usize = 4 * 1024 * 1024;

/// How the change feed is filtered.
#[derive(Debug, Clone)]
pub enum FeedFilter {
    /// Historical timestamp filter (per-type limits)
    Since(Option<i64>),
    /// Write-order feed: `seq > cursor` and, when given, `seq <= upto`
    AfterSeq { cursor: i64, upto: Option<i64> },
}

/// A page of the change feed.
#[derive(Debug, Clone)]
pub struct ChangeFeed {
    pub changes: Vec<HashMap<String, serde_json::Value>>,
    pub latest_timestamp: Option<i64>,
    /// Pass back to continue after this page (cursor feed only)
    pub next_cursor: i64,
    /// False when the page was cut and more changes remain
    pub is_complete: bool,
}

/// Note data returned from database queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoteRow {
    pub id: String,
    pub created_at: i64,
    pub content: String,
    pub modified_at: Option<i64>,
    pub deleted_at: Option<i64>,
    pub tag_names: Option<String>,
    /// The timezone each timestamp was written in: seconds east of UTC and
    /// the IANA name, when the device that wrote it knew one.
    pub created_at_offset: Option<i32>,
    pub created_at_zone: Option<String>,
    pub modified_at_offset: Option<i32>,
    pub modified_at_zone: Option<String>,
    pub deleted_at_offset: Option<i32>,
    pub deleted_at_zone: Option<String>,
    pub display_cache: Option<String>,
    /// Cache for notes list pane display (JSON with date, marked, content_preview)
    pub list_display_cache: Option<String>,
}

/// Tag data returned from database queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagRow {
    pub id: String,
    pub name: String,
    pub parent_id: Option<String>,
    pub created_at: Option<i64>,
    pub modified_at: Option<i64>,
}

/// NoteAttachment data returned from database queries (junction table)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoteAttachmentRow {
    pub id: String,
    pub note_id: String,
    pub attachment_id: String,
    pub attachment_type: String,
    pub created_at: i64,
    pub device_id: String,
    pub modified_at: Option<i64>,
    pub deleted_at: Option<i64>,
}

/// AudioFile data returned from database queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioFileRow {
    pub id: String,
    pub imported_at: i64,
    pub filename: String,
    pub file_created_at: Option<i64>,
    pub duration_seconds: Option<i64>,
    pub summary: Option<String>,
    pub device_id: String,
    pub modified_at: Option<i64>,
    pub deleted_at: Option<i64>,
    /// Cloud storage provider ("s3", "backblaze", etc.) or None for local-only
    pub storage_provider: Option<String>,
    /// Object key/path in cloud storage
    pub storage_key: Option<String>,
    /// Unix timestamp when file was uploaded to cloud storage
    pub storage_uploaded_at: Option<i64>,
    /// The timezone each timestamp was written in.
    pub imported_at_offset: Option<i32>,
    pub imported_at_zone: Option<String>,
    pub file_created_at_offset: Option<i32>,
    pub file_created_at_zone: Option<String>,
    pub modified_at_offset: Option<i32>,
    pub modified_at_zone: Option<String>,
    pub deleted_at_offset: Option<i32>,
    pub deleted_at_zone: Option<String>,
    /// The file's name in the audio directory (Stage 13): local, never synced
    pub local_name: String,
    /// The SHA-256 of the file's bytes, lowercase hex (Stage 13): synced
    /// metadata, written by import and by recording; the bucket object is
    /// keyed by it and a fetched file is verified by it
    pub content_sha256: Option<String>,
    /// Whether the bucket object is encrypted with the recording key (Stage 15)
    pub storage_encrypted: bool,
}

/// Transcription data returned from database queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TranscriptionRow {
    pub id: String,
    pub audio_file_id: String,
    pub content: String,
    pub content_segments: Option<String>,
    pub service: String,
    pub service_arguments: Option<String>,
    pub service_response: Option<String>,
    pub state: String,
    pub device_id: String,
    pub created_at: i64,
    pub modified_at: Option<i64>,
    pub deleted_at: Option<i64>,
    /// The timezone the transcription was made in.
    pub created_at_offset: Option<i32>,
    pub created_at_zone: Option<String>,
}

/// Result of a tag change operation (add/remove tag from note)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagChangeResult {
    /// Whether the tag association was actually changed
    pub changed: bool,
    /// The note ID that was affected
    pub note_id: String,
    /// Whether the list pane cache was rebuilt
    pub list_cache_rebuilt: bool,
}

/// Default state for new transcriptions
pub const DEFAULT_TRANSCRIPTION_STATE: &str = "original !verified !verbatim !cleaned !polished";

/// Convert UUID bytes to hex string
fn uuid_bytes_to_hex(bytes: &[u8]) -> Option<String> {
    if bytes.len() == 16 {
        Some(Uuid::from_slice(bytes).ok()?.simple().to_string())
    } else {
        None
    }
}

/// An account id is 32 lowercase hex characters, like every other id.
pub fn validate_account_id(account_id: &str) -> VoiceResult<()> {
    if account_id.len() == 32 && account_id.chars().all(|c| c.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(VoiceError::validation("account_id", "must be 32 hex characters"))
    }
}

/// The first characters of an id, for a sentence.
fn short(id: &str) -> &str {
    &id[..crate::UUID_SHORT_LEN.min(id.len())]
}

/// Database wrapper for SQLite operations
pub struct Database {
    conn: Connection,
    /// Where the file is, so a snapshot can be written beside it. None for
    /// an in-memory database.
    path: Option<PathBuf>,
}

/// How many snapshots are kept beside a database (SNAP-2).
pub const SNAPSHOTS_KEPT: usize = 5;

/// Wrong tokens a shown code survives before it is withdrawn (PAIR-2).
pub const PAIRING_GUESSES_ALLOWED: i64 = 5;

/// One snapshot of a database, as listed by [`Database::list_snapshots`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotInfo {
    /// File name, `notes-<UTC time>.db`
    pub name: String,
    pub path: String,
    pub size_bytes: u64,
    /// Notes in the snapshot that are not in the trash
    pub note_count: i64,
}

impl Database {
    /// Create a new database connection
    pub fn new<P: AsRef<Path>>(db_path: P) -> VoiceResult<Self> {
        let path = db_path.as_ref().to_path_buf();
        let conn = Connection::open(&path)?;

        // Enable WAL mode for better concurrent access
        conn.execute_batch("PRAGMA journal_mode=WAL;")?;
        // A sync batch holds a write transaction for a moment; the GUI (or
        // the server, when the GUI writes) waits instead of failing with
        // "database is locked".
        conn.execute_batch("PRAGMA busy_timeout=10000;")?;

        // Checkpoint any pending WAL frames to ensure we see the latest data
        // from other connections that may have written and closed
        conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE);")?;

        let mut db = Self { conn, path: Some(path) };
        db.init_database()?;
        db.migrate_add_sync_received_at()?;
        db.migrate_timestamps_to_unix()?;
        db.migrate_add_storage_columns()?;
        db.migrate_add_file_storage_config_table()?;
        db.migrate_drop_legacy_conflict_tables()?;
        db.create_version_tables()?;
        db.migrate_create_root_versions()?;
        db.migrate_add_sync_sequence()?;
        db.migrate_add_timezone_columns()?;
        Ok(db)
    }

    /// Create an in-memory database (for testing)
    pub fn new_in_memory() -> VoiceResult<Self> {
        let conn = Connection::open_in_memory()?;
        let mut db = Self { conn, path: None };
        db.init_database()?;
        db.migrate_add_sync_received_at()?;
        db.migrate_timestamps_to_unix()?;
        db.migrate_add_storage_columns()?;
        db.migrate_add_file_storage_config_table()?;
        db.migrate_drop_legacy_conflict_tables()?;
        db.create_version_tables()?;
        db.migrate_create_root_versions()?;
        db.migrate_add_sync_sequence()?;
        db.migrate_add_timezone_columns()?;
        Ok(db)
    }

    /// Open a database that belongs to `account_id` (ACCT-4).
    ///
    /// A fresh database takes the id. One that already has it opens as usual.
    /// One that carries another account's id is refused, never corrected:
    /// the database is authoritative for its own account.
    pub fn new_for_account<P: AsRef<Path>>(db_path: P, account_id: &str) -> VoiceResult<Self> {
        validate_account_id(account_id)?;
        let db = Self::new(db_path)?;
        let existing = db.account_id()?;
        if existing == account_id {
            return Ok(db);
        }
        if db.has_notes()? || db.has_synced()? {
            return Err(VoiceError::Sync(format!(
                "This database belongs to account {}, not {} ({})",
                short(&existing),
                short(account_id),
                crate::sync_protocol::codes::ACCOUNT_DISAGREES
            )));
        }
        // Created a moment ago with a random id and never used: it is the
        // caller's to name.
        db.set_account_id(account_id)?;
        Ok(db)
    }

    /// Whether any note, in the trash or out of it, exists.
    fn has_notes(&self) -> VoiceResult<bool> {
        let n: i64 = self.conn.query_row("SELECT COUNT(*) FROM notes", [], |r| r.get(0))?;
        Ok(n > 0)
    }

    /// Whether this database has ever exchanged anything with a peer.
    fn has_synced(&self) -> VoiceResult<bool> {
        let n: i64 = self.conn.query_row("SELECT COUNT(*) FROM sync_peers", [], |r| r.get(0))?;
        Ok(n > 0)
    }

    /// Initialize database schema
    pub fn init_database(&mut self) -> VoiceResult<()> {
        self.conn.execute_batch(
            r#"
            -- Create notes table with UUID7 BLOB primary key
            -- All timestamps are Unix seconds (INTEGER) for timezone safety
            CREATE TABLE IF NOT EXISTS notes (
                id BLOB PRIMARY KEY,
                created_at INTEGER NOT NULL,
                content TEXT NOT NULL,
                modified_at INTEGER,
                deleted_at INTEGER,
                sync_received_at INTEGER,
                di_cache_note_pane_display TEXT,
                di_cache_note_list_pane_display TEXT,
                -- Timezone of each action; see migrate_add_timezone_columns
                created_at_offset INTEGER,
                created_at_zone TEXT,
                modified_at_offset INTEGER,
                modified_at_zone TEXT,
                deleted_at_offset INTEGER,
                deleted_at_zone TEXT
            );

            -- Create tags table with UUID7 BLOB primary key
            CREATE TABLE IF NOT EXISTS tags (
                id BLOB PRIMARY KEY,
                name TEXT NOT NULL,
                parent_id BLOB,
                created_at INTEGER NOT NULL,
                modified_at INTEGER,
                deleted_at INTEGER,
                sync_received_at INTEGER,
                -- Timezone of each action; see migrate_add_timezone_columns
                created_at_offset INTEGER,
                created_at_zone TEXT,
                modified_at_offset INTEGER,
                modified_at_zone TEXT,
                deleted_at_offset INTEGER,
                deleted_at_zone TEXT,
                FOREIGN KEY (parent_id) REFERENCES tags (id) ON DELETE CASCADE
            );

            -- Create note_tags junction table with timestamps for sync
            CREATE TABLE IF NOT EXISTS note_tags (
                note_id BLOB NOT NULL,
                tag_id BLOB NOT NULL,
                created_at INTEGER NOT NULL,
                modified_at INTEGER,
                deleted_at INTEGER,
                sync_received_at INTEGER,
                -- Timezone of each action; see migrate_add_timezone_columns
                created_at_offset INTEGER,
                created_at_zone TEXT,
                modified_at_offset INTEGER,
                modified_at_zone TEXT,
                deleted_at_offset INTEGER,
                deleted_at_zone TEXT,
                FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE,
                FOREIGN KEY (tag_id) REFERENCES tags (id) ON DELETE CASCADE,
                PRIMARY KEY (note_id, tag_id)
            );

            -- Create sync_peers table
            CREATE TABLE IF NOT EXISTS sync_peers (
                peer_id BLOB PRIMARY KEY,
                peer_name TEXT,
                peer_url TEXT NOT NULL,
                last_sync_at INTEGER,
                last_received_timestamp INTEGER,
                last_sent_timestamp INTEGER,
                certificate_fingerprint BLOB
            );

            -- Create conflicts_note_content table
            CREATE TABLE IF NOT EXISTS conflicts_note_content (
                id BLOB PRIMARY KEY,
                note_id BLOB NOT NULL,
                local_content TEXT NOT NULL,
                local_modified_at INTEGER NOT NULL,
                local_device_id BLOB,
                local_device_name TEXT,
                remote_content TEXT NOT NULL,
                remote_modified_at INTEGER NOT NULL,
                remote_device_id BLOB,
                remote_device_name TEXT,
                created_at INTEGER NOT NULL,
                resolved_at INTEGER,
                FOREIGN KEY (note_id) REFERENCES notes(id)
            );

            -- Create conflicts_note_delete table
            CREATE TABLE IF NOT EXISTS conflicts_note_delete (
                id BLOB PRIMARY KEY,
                note_id BLOB NOT NULL,
                surviving_content TEXT NOT NULL,
                surviving_modified_at INTEGER NOT NULL,
                surviving_device_id BLOB,
                surviving_device_name TEXT,
                deleted_content TEXT,
                deleted_at INTEGER NOT NULL,
                deleting_device_id BLOB,
                deleting_device_name TEXT,
                created_at INTEGER NOT NULL,
                resolved_at INTEGER,
                FOREIGN KEY (note_id) REFERENCES notes(id)
            );

            -- Create conflicts_tag_rename table
            CREATE TABLE IF NOT EXISTS conflicts_tag_rename (
                id BLOB PRIMARY KEY,
                tag_id BLOB NOT NULL,
                local_name TEXT NOT NULL,
                local_modified_at INTEGER NOT NULL,
                local_device_id BLOB,
                local_device_name TEXT,
                remote_name TEXT NOT NULL,
                remote_modified_at INTEGER NOT NULL,
                remote_device_id BLOB,
                remote_device_name TEXT,
                created_at INTEGER NOT NULL,
                resolved_at INTEGER,
                FOREIGN KEY (tag_id) REFERENCES tags(id)
            );

            -- Create conflicts_tag_parent table for parent_id conflicts
            CREATE TABLE IF NOT EXISTS conflicts_tag_parent (
                id BLOB PRIMARY KEY,
                tag_id BLOB NOT NULL,
                local_parent_id BLOB,
                local_modified_at INTEGER NOT NULL,
                local_device_id BLOB,
                local_device_name TEXT,
                remote_parent_id BLOB,
                remote_modified_at INTEGER NOT NULL,
                remote_device_id BLOB,
                remote_device_name TEXT,
                created_at INTEGER NOT NULL,
                resolved_at INTEGER,
                FOREIGN KEY (tag_id) REFERENCES tags(id)
            );

            -- Create conflicts_tag_delete table for rename vs delete conflicts
            CREATE TABLE IF NOT EXISTS conflicts_tag_delete (
                id BLOB PRIMARY KEY,
                tag_id BLOB NOT NULL,
                surviving_name TEXT NOT NULL,
                surviving_parent_id BLOB,
                surviving_modified_at INTEGER NOT NULL,
                surviving_device_id BLOB,
                surviving_device_name TEXT,
                deleted_at INTEGER NOT NULL,
                deleting_device_id BLOB,
                deleting_device_name TEXT,
                created_at INTEGER NOT NULL,
                resolved_at INTEGER,
                FOREIGN KEY (tag_id) REFERENCES tags(id)
            );

            -- Create conflicts_note_tag table
            CREATE TABLE IF NOT EXISTS conflicts_note_tag (
                id BLOB PRIMARY KEY,
                note_id BLOB NOT NULL,
                tag_id BLOB NOT NULL,
                local_created_at INTEGER,
                local_modified_at INTEGER,
                local_deleted_at INTEGER,
                local_device_id BLOB,
                local_device_name TEXT,
                remote_created_at INTEGER,
                remote_modified_at INTEGER,
                remote_deleted_at INTEGER,
                remote_device_id BLOB,
                remote_device_name TEXT,
                created_at INTEGER NOT NULL,
                resolved_at INTEGER,
                FOREIGN KEY (note_id) REFERENCES notes(id),
                FOREIGN KEY (tag_id) REFERENCES tags(id)
            );

            -- Create sync_failures table
            CREATE TABLE IF NOT EXISTS sync_failures (
                id BLOB PRIMARY KEY,
                peer_id BLOB NOT NULL,
                peer_name TEXT,
                entity_type TEXT NOT NULL,
                entity_id BLOB,
                operation TEXT NOT NULL,
                payload TEXT NOT NULL,
                error_message TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                resolved_at INTEGER,
                FOREIGN KEY (peer_id) REFERENCES sync_peers(peer_id)
            );

            -- Create note_attachments junction table (polymorphic association)
            CREATE TABLE IF NOT EXISTS note_attachments (
                id BLOB PRIMARY KEY,
                note_id BLOB NOT NULL,
                attachment_id BLOB NOT NULL,
                attachment_type TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                device_id BLOB NOT NULL,
                modified_at INTEGER,
                deleted_at INTEGER,
                sync_received_at INTEGER,
                -- Timezone of each action; see migrate_add_timezone_columns
                created_at_offset INTEGER,
                created_at_zone TEXT,
                modified_at_offset INTEGER,
                modified_at_zone TEXT,
                deleted_at_offset INTEGER,
                deleted_at_zone TEXT,
                FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE
            );

            -- Create audio_files table
            CREATE TABLE IF NOT EXISTS audio_files (
                id BLOB PRIMARY KEY,
                imported_at INTEGER NOT NULL,
                filename TEXT NOT NULL,
                file_created_at INTEGER,
                duration_seconds INTEGER,
                summary TEXT,
                device_id BLOB NOT NULL,
                modified_at INTEGER,
                deleted_at INTEGER,
                sync_received_at INTEGER,
                -- Cloud storage fields
                storage_provider TEXT,     -- "s3", "backblaze", etc. NULL = local only
                storage_key TEXT,          -- Object key/path in cloud storage
                storage_uploaded_at INTEGER, -- When file was uploaded to cloud storage
                -- Timezone of each action; see migrate_add_timezone_columns
                imported_at_offset INTEGER,
                imported_at_zone TEXT,
                file_created_at_offset INTEGER,
                file_created_at_zone TEXT,
                modified_at_offset INTEGER,
                modified_at_zone TEXT,
                deleted_at_offset INTEGER,
                deleted_at_zone TEXT
            );

            -- Create transcriptions table
            CREATE TABLE IF NOT EXISTS transcriptions (
                id BLOB PRIMARY KEY,
                audio_file_id BLOB NOT NULL,
                content TEXT NOT NULL,
                content_segments TEXT,
                service TEXT NOT NULL,
                service_arguments TEXT,
                service_response TEXT,
                state TEXT NOT NULL DEFAULT 'original !verified !verbatim !cleaned !polished',
                device_id BLOB NOT NULL,
                created_at INTEGER NOT NULL,
                modified_at INTEGER,
                deleted_at INTEGER,
                sync_received_at INTEGER,
                -- Timezone of each action; see migrate_add_timezone_columns
                created_at_offset INTEGER,
                created_at_zone TEXT,
                modified_at_offset INTEGER,
                modified_at_zone TEXT,
                deleted_at_offset INTEGER,
                deleted_at_zone TEXT,
                FOREIGN KEY (audio_file_id) REFERENCES audio_files (id) ON DELETE CASCADE
            );

            -- Create file_storage_config table (single-row config that syncs between devices)
            -- Uses a fixed ID ("default") since there's only one config
            CREATE TABLE IF NOT EXISTS file_storage_config (
                id TEXT PRIMARY KEY DEFAULT 'default',
                provider TEXT NOT NULL DEFAULT 'none',
                config TEXT,
                modified_at INTEGER,
                device_id BLOB,
                sync_received_at INTEGER
            );

            -- Create indexes
            CREATE INDEX IF NOT EXISTS idx_notes_created_at ON notes(created_at);
            CREATE INDEX IF NOT EXISTS idx_notes_deleted_at ON notes(deleted_at);
            CREATE INDEX IF NOT EXISTS idx_notes_modified_at ON notes(modified_at);
            CREATE INDEX IF NOT EXISTS idx_tags_parent_id ON tags(parent_id);
            CREATE INDEX IF NOT EXISTS idx_tags_name ON tags(LOWER(name));
            CREATE INDEX IF NOT EXISTS idx_tags_modified_at ON tags(modified_at);
            CREATE INDEX IF NOT EXISTS idx_note_tags_note ON note_tags(note_id);
            CREATE INDEX IF NOT EXISTS idx_note_tags_tag ON note_tags(tag_id);
            CREATE INDEX IF NOT EXISTS idx_note_tags_created_at ON note_tags(created_at);
            CREATE INDEX IF NOT EXISTS idx_note_tags_deleted_at ON note_tags(deleted_at);
            CREATE INDEX IF NOT EXISTS idx_note_tags_modified_at ON note_tags(modified_at);
            -- Composite indexes for tag search: covers all columns in EXISTS subquery
            CREATE INDEX IF NOT EXISTS idx_note_tags_search ON note_tags(note_id, tag_id, deleted_at);
            -- Reverse index for finding notes by tag
            CREATE INDEX IF NOT EXISTS idx_note_tags_by_tag ON note_tags(tag_id, note_id, deleted_at);
            CREATE INDEX IF NOT EXISTS idx_note_attachments_note_id ON note_attachments(note_id);
            CREATE INDEX IF NOT EXISTS idx_note_attachments_attachment_id ON note_attachments(attachment_id);
            CREATE INDEX IF NOT EXISTS idx_note_attachments_type ON note_attachments(attachment_type);
            CREATE INDEX IF NOT EXISTS idx_note_attachments_modified_at ON note_attachments(modified_at);
            CREATE INDEX IF NOT EXISTS idx_note_attachments_deleted_at ON note_attachments(deleted_at);
            CREATE INDEX IF NOT EXISTS idx_audio_files_modified_at ON audio_files(modified_at);
            CREATE INDEX IF NOT EXISTS idx_audio_files_deleted_at ON audio_files(deleted_at);
            -- Note: idx_audio_files_storage_provider is created in migrate_add_storage_columns
            CREATE INDEX IF NOT EXISTS idx_transcriptions_audio_file_id ON transcriptions(audio_file_id);
            CREATE INDEX IF NOT EXISTS idx_transcriptions_service ON transcriptions(service);
            CREATE INDEX IF NOT EXISTS idx_transcriptions_created_at ON transcriptions(created_at);
            CREATE INDEX IF NOT EXISTS idx_transcriptions_modified_at ON transcriptions(modified_at);
            CREATE INDEX IF NOT EXISTS idx_transcriptions_deleted_at ON transcriptions(deleted_at);
            -- Note: sync_received_at indexes are created in migrate_add_sync_received_at
            -- Note: idx_audio_files_storage_provider is created in migrate_add_storage_columns
            -- Note: idx_file_storage_config_sync_received_at is created in migrate_add_file_storage_config_table
            "#,
        )?;

        // Create system tags with deterministic UUIDs
        // These are the same on all devices to prevent duplicates during sync
        let system_uuid = Uuid::parse_str(SYSTEM_TAG_UUID)
            .map_err(|e| VoiceError::Other(format!("Invalid SYSTEM_TAG_UUID: {}", e)))?;
        let marked_uuid = Uuid::parse_str(MARKED_TAG_UUID)
            .map_err(|e| VoiceError::Other(format!("Invalid MARKED_TAG_UUID: {}", e)))?;
        let nonsynced_uuid = Uuid::parse_str(NONSYNCED_TAG_UUID)
            .map_err(|e| VoiceError::Other(format!("Invalid NONSYNCED_TAG_UUID: {}", e)))?;
        let too_big_uuid = Uuid::parse_str(TOO_BIG_TAG_UUID)
            .map_err(|e| VoiceError::Other(format!("Invalid TOO_BIG_TAG_UUID: {}", e)))?;
        let system_bytes = system_uuid.as_bytes().to_vec();
        let marked_bytes = marked_uuid.as_bytes().to_vec();
        let nonsynced_bytes = nonsynced_uuid.as_bytes().to_vec();
        let too_big_bytes = too_big_uuid.as_bytes().to_vec();

        // Insert system tags if they don't exist (OR IGNORE handles duplicates)
        // _system (root)
        self.conn.execute(
            "INSERT OR IGNORE INTO tags (id, name, parent_id, created_at) VALUES (?, ?, NULL, strftime('%s', 'now'))",
            params![&system_bytes, SYSTEM_TAG_NAME],
        )?;
        // _system/_marked
        self.conn.execute(
            "INSERT OR IGNORE INTO tags (id, name, parent_id, created_at) VALUES (?, ?, ?, strftime('%s', 'now'))",
            params![&marked_bytes, MARKED_TAG_NAME, &system_bytes],
        )?;
        // _system/_nonsynced
        self.conn.execute(
            "INSERT OR IGNORE INTO tags (id, name, parent_id, created_at) VALUES (?, ?, ?, strftime('%s', 'now'))",
            params![&nonsynced_bytes, NONSYNCED_TAG_NAME, &system_bytes],
        )?;
        // _system/_nonsynced/_too-big
        self.conn.execute(
            "INSERT OR IGNORE INTO tags (id, name, parent_id, created_at) VALUES (?, ?, ?, strftime('%s', 'now'))",
            params![&too_big_bytes, TOO_BIG_TAG_NAME, &nonsynced_bytes],
        )?;

        Ok(())
    }

    /// Normalize database data for consistency.
    ///
    /// This runs various normalization passes on the database:
    /// - Timestamp normalization (ISO 8601 -> SQLite format)
    /// - (Future: Unicode normalization, etc.)
    ///
    /// This should be run via `cli maintenance database-normalize`.
    pub fn normalize_database(&mut self) -> VoiceResult<()> {
        self.normalize_timestamps()?;
        // Future normalizations can be added here
        Ok(())
    }

    /// Normalize all datetime values to SQLite format (YYYY-MM-DD HH:MM:SS).
    ///
    /// This fixes timestamps that may have been stored in ISO 8601 format
    /// (with 'T' separator and/or microseconds) from earlier sync operations.
    /// String comparison of timestamps requires consistent format.
    ///
    /// This is idempotent - the WHERE clauses only match rows needing updates.
    fn normalize_timestamps(&mut self) -> VoiceResult<()> {
        // Normalize notes timestamps
        self.conn.execute_batch(
            r#"
            UPDATE notes SET created_at = REPLACE(SUBSTR(created_at, 1, 19), 'T', ' ')
            WHERE created_at LIKE '%T%';

            UPDATE notes SET modified_at = REPLACE(SUBSTR(modified_at, 1, 19), 'T', ' ')
            WHERE modified_at LIKE '%T%';

            UPDATE notes SET deleted_at = REPLACE(SUBSTR(deleted_at, 1, 19), 'T', ' ')
            WHERE deleted_at LIKE '%T%';
            "#,
        )?;

        // Normalize tags timestamps
        self.conn.execute_batch(
            r#"
            UPDATE tags SET created_at = REPLACE(SUBSTR(created_at, 1, 19), 'T', ' ')
            WHERE created_at LIKE '%T%';

            UPDATE tags SET modified_at = REPLACE(SUBSTR(modified_at, 1, 19), 'T', ' ')
            WHERE modified_at LIKE '%T%';
            "#,
        )?;

        // Normalize note_tags timestamps
        self.conn.execute_batch(
            r#"
            UPDATE note_tags SET created_at = REPLACE(SUBSTR(created_at, 1, 19), 'T', ' ')
            WHERE created_at LIKE '%T%';

            UPDATE note_tags SET modified_at = REPLACE(SUBSTR(modified_at, 1, 19), 'T', ' ')
            WHERE modified_at LIKE '%T%';

            UPDATE note_tags SET deleted_at = REPLACE(SUBSTR(deleted_at, 1, 19), 'T', ' ')
            WHERE deleted_at LIKE '%T%';
            "#,
        )?;

        // Normalize audio_files timestamps
        self.conn.execute_batch(
            r#"
            UPDATE audio_files SET imported_at = REPLACE(SUBSTR(imported_at, 1, 19), 'T', ' ')
            WHERE imported_at LIKE '%T%';

            UPDATE audio_files SET file_created_at = REPLACE(SUBSTR(file_created_at, 1, 19), 'T', ' ')
            WHERE file_created_at LIKE '%T%';

            UPDATE audio_files SET modified_at = REPLACE(SUBSTR(modified_at, 1, 19), 'T', ' ')
            WHERE modified_at LIKE '%T%';

            UPDATE audio_files SET deleted_at = REPLACE(SUBSTR(deleted_at, 1, 19), 'T', ' ')
            WHERE deleted_at LIKE '%T%';
            "#,
        )?;

        // Normalize note_attachments timestamps
        self.conn.execute_batch(
            r#"
            UPDATE note_attachments SET created_at = REPLACE(SUBSTR(created_at, 1, 19), 'T', ' ')
            WHERE created_at LIKE '%T%';

            UPDATE note_attachments SET modified_at = REPLACE(SUBSTR(modified_at, 1, 19), 'T', ' ')
            WHERE modified_at LIKE '%T%';

            UPDATE note_attachments SET deleted_at = REPLACE(SUBSTR(deleted_at, 1, 19), 'T', ' ')
            WHERE deleted_at LIKE '%T%';
            "#,
        )?;

        Ok(())
    }

    /// Migrate existing databases to add sync_received_at column.
    ///
    /// This column stores Unix timestamp (seconds since epoch) of when the server
    /// received a sync change. It's used to correctly track which changes need to
    /// be sent to clients, avoiding the bug where changes made before a client's
    /// last sync but pushed to server after are never sent.
    ///
    /// This is idempotent - it checks if the column exists before adding it.
    fn migrate_add_sync_received_at(&mut self) -> VoiceResult<()> {
        // Helper to check if a column exists in a table
        fn column_exists(conn: &Connection, table: &str, column: &str) -> bool {
            let sql = format!("PRAGMA table_info({})", table);
            let mut stmt = conn.prepare(&sql).unwrap();
            let rows = stmt.query_map([], |row| {
                let name: String = row.get(1)?;
                Ok(name)
            }).unwrap();
            for row in rows {
                if let Ok(name) = row {
                    if name == column {
                        return true;
                    }
                }
            }
            false
        }

        let tables = ["notes", "tags", "note_tags", "note_attachments", "audio_files", "transcriptions"];

        for table in tables {
            if !column_exists(&self.conn, table, "sync_received_at") {
                let sql = format!("ALTER TABLE {} ADD COLUMN sync_received_at INTEGER", table);
                self.conn.execute(&sql, [])?;

                // Create index for the new column
                let index_sql = format!(
                    "CREATE INDEX IF NOT EXISTS idx_{}_sync_received_at ON {}(sync_received_at)",
                    table, table
                );
                self.conn.execute(&index_sql, [])?;
            }
        }

        Ok(())
    }

    /// Migrate existing databases from TEXT datetime columns to INTEGER Unix timestamps.
    ///
    /// This converts all timestamp columns from "YYYY-MM-DD HH:MM:SS" TEXT format
    /// to Unix seconds INTEGER format. This is a one-way migration.
    ///
    /// The migration recreates each table with the new schema because SQLite doesn't
    /// support ALTER COLUMN to change types.
    fn migrate_timestamps_to_unix(&mut self) -> VoiceResult<()> {
        // Helper to check if a column is TEXT type (needs migration)
        fn column_is_text(conn: &Connection, table: &str, column: &str) -> bool {
            let sql = format!("PRAGMA table_info({})", table);
            if let Ok(mut stmt) = conn.prepare(&sql) {
                let rows = stmt.query_map([], |row| {
                    let name: String = row.get(1)?;
                    let col_type: String = row.get(2)?;
                    Ok((name, col_type))
                });
                if let Ok(rows) = rows {
                    for row in rows.flatten() {
                        if row.0 == column {
                            // Check if type contains TEXT, DATETIME, or similar string types
                            let type_upper = row.1.to_uppercase();
                            return type_upper.contains("TEXT")
                                || type_upper.contains("DATETIME")
                                || type_upper.contains("CHAR");
                        }
                    }
                }
            }
            false
        }

        // Check if migration is needed by looking at notes.created_at type
        if !column_is_text(&self.conn, "notes", "created_at") {
            // Already migrated or new database with INTEGER columns
            return Ok(());
        }

        tracing::info!("Migrating timestamps from TEXT to INTEGER (Unix seconds)...");

        // Migrate each table in a transaction
        let tx = self.conn.transaction()?;

        // --- notes table ---
        tx.execute_batch(r#"
            CREATE TABLE notes_new (
                id BLOB PRIMARY KEY,
                created_at INTEGER NOT NULL,
                content TEXT NOT NULL,
                modified_at INTEGER,
                deleted_at INTEGER,
                sync_received_at INTEGER,
                di_cache_note_pane_display TEXT,
                di_cache_note_list_pane_display TEXT
            );
            INSERT INTO notes_new SELECT
                id,
                COALESCE(CAST(strftime('%s', created_at) AS INTEGER), CAST(strftime('%s', 'now') AS INTEGER)),
                content,
                CAST(strftime('%s', modified_at) AS INTEGER),
                CAST(strftime('%s', deleted_at) AS INTEGER),
                sync_received_at,
                di_cache_note_pane_display,
                di_cache_note_list_pane_display
            FROM notes;
            DROP TABLE notes;
            ALTER TABLE notes_new RENAME TO notes;
        "#)?;

        // --- tags table ---
        tx.execute_batch(r#"
            CREATE TABLE tags_new (
                id BLOB PRIMARY KEY,
                name TEXT NOT NULL,
                parent_id BLOB,
                created_at INTEGER NOT NULL,
                modified_at INTEGER,
                deleted_at INTEGER,
                sync_received_at INTEGER,
                FOREIGN KEY (parent_id) REFERENCES tags_new (id) ON DELETE CASCADE
            );
            INSERT INTO tags_new SELECT
                id,
                name,
                parent_id,
                COALESCE(CAST(strftime('%s', created_at) AS INTEGER), CAST(strftime('%s', 'now') AS INTEGER)),
                CAST(strftime('%s', modified_at) AS INTEGER),
                CAST(strftime('%s', deleted_at) AS INTEGER),
                sync_received_at
            FROM tags;
            DROP TABLE tags;
            ALTER TABLE tags_new RENAME TO tags;
        "#)?;

        // --- note_tags table ---
        tx.execute_batch(r#"
            CREATE TABLE note_tags_new (
                note_id BLOB NOT NULL,
                tag_id BLOB NOT NULL,
                created_at INTEGER NOT NULL,
                modified_at INTEGER,
                deleted_at INTEGER,
                sync_received_at INTEGER,
                FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE,
                FOREIGN KEY (tag_id) REFERENCES tags (id) ON DELETE CASCADE,
                PRIMARY KEY (note_id, tag_id)
            );
            INSERT INTO note_tags_new SELECT
                note_id,
                tag_id,
                COALESCE(CAST(strftime('%s', created_at) AS INTEGER), CAST(strftime('%s', 'now') AS INTEGER)),
                CAST(strftime('%s', modified_at) AS INTEGER),
                CAST(strftime('%s', deleted_at) AS INTEGER),
                sync_received_at
            FROM note_tags;
            DROP TABLE note_tags;
            ALTER TABLE note_tags_new RENAME TO note_tags;
        "#)?;

        // --- sync_peers table ---
        tx.execute_batch(r#"
            CREATE TABLE sync_peers_new (
                peer_id BLOB PRIMARY KEY,
                peer_name TEXT,
                peer_url TEXT NOT NULL,
                last_sync_at INTEGER,
                last_received_timestamp INTEGER,
                last_sent_timestamp INTEGER,
                certificate_fingerprint BLOB
            );
            INSERT INTO sync_peers_new SELECT
                peer_id,
                peer_name,
                peer_url,
                CAST(strftime('%s', last_sync_at) AS INTEGER),
                CAST(strftime('%s', last_received_timestamp) AS INTEGER),
                CAST(strftime('%s', last_sent_timestamp) AS INTEGER),
                certificate_fingerprint
            FROM sync_peers;
            DROP TABLE sync_peers;
            ALTER TABLE sync_peers_new RENAME TO sync_peers;
        "#)?;

        // --- note_attachments table ---
        tx.execute_batch(r#"
            CREATE TABLE note_attachments_new (
                id BLOB PRIMARY KEY,
                note_id BLOB NOT NULL,
                attachment_id BLOB NOT NULL,
                attachment_type TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                device_id BLOB NOT NULL,
                modified_at INTEGER,
                deleted_at INTEGER,
                sync_received_at INTEGER,
                FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE
            );
            INSERT INTO note_attachments_new SELECT
                id,
                note_id,
                attachment_id,
                attachment_type,
                COALESCE(CAST(strftime('%s', created_at) AS INTEGER), CAST(strftime('%s', 'now') AS INTEGER)),
                device_id,
                CAST(strftime('%s', modified_at) AS INTEGER),
                CAST(strftime('%s', deleted_at) AS INTEGER),
                sync_received_at
            FROM note_attachments;
            DROP TABLE note_attachments;
            ALTER TABLE note_attachments_new RENAME TO note_attachments;
        "#)?;

        // --- audio_files table ---
        tx.execute_batch(r#"
            CREATE TABLE audio_files_new (
                id BLOB PRIMARY KEY,
                imported_at INTEGER NOT NULL,
                filename TEXT NOT NULL,
                file_created_at INTEGER,
                duration_seconds INTEGER,
                summary TEXT,
                device_id BLOB NOT NULL,
                modified_at INTEGER,
                deleted_at INTEGER,
                sync_received_at INTEGER
            );
            INSERT INTO audio_files_new SELECT
                id,
                COALESCE(CAST(strftime('%s', imported_at) AS INTEGER), CAST(strftime('%s', 'now') AS INTEGER)),
                filename,
                CAST(strftime('%s', file_created_at) AS INTEGER),
                duration_seconds,
                summary,
                device_id,
                CAST(strftime('%s', modified_at) AS INTEGER),
                CAST(strftime('%s', deleted_at) AS INTEGER),
                sync_received_at
            FROM audio_files;
            DROP TABLE audio_files;
            ALTER TABLE audio_files_new RENAME TO audio_files;
        "#)?;

        // --- transcriptions table ---
        tx.execute_batch(r#"
            CREATE TABLE transcriptions_new (
                id BLOB PRIMARY KEY,
                audio_file_id BLOB NOT NULL,
                content TEXT NOT NULL,
                content_segments TEXT,
                service TEXT NOT NULL,
                service_arguments TEXT,
                service_response TEXT,
                state TEXT NOT NULL DEFAULT 'original !verified !verbatim !cleaned !polished',
                device_id BLOB NOT NULL,
                created_at INTEGER NOT NULL,
                modified_at INTEGER,
                deleted_at INTEGER,
                sync_received_at INTEGER,
                FOREIGN KEY (audio_file_id) REFERENCES audio_files (id) ON DELETE CASCADE
            );
            INSERT INTO transcriptions_new SELECT
                id,
                audio_file_id,
                content,
                content_segments,
                service,
                service_arguments,
                service_response,
                state,
                device_id,
                COALESCE(CAST(strftime('%s', created_at) AS INTEGER), CAST(strftime('%s', 'now') AS INTEGER)),
                CAST(strftime('%s', modified_at) AS INTEGER),
                CAST(strftime('%s', deleted_at) AS INTEGER),
                sync_received_at
            FROM transcriptions;
            DROP TABLE transcriptions;
            ALTER TABLE transcriptions_new RENAME TO transcriptions;
        "#)?;

        // --- Conflict tables ---
        // Conflict tables are temporary data that gets regenerated during sync.
        // Drop and recreate them with the current schema rather than trying to migrate data.
        // This avoids issues with schema changes over time.

        // conflicts_note_content
        tx.execute_batch(r#"
            DROP TABLE IF EXISTS conflicts_note_content;
            CREATE TABLE conflicts_note_content (
                id BLOB PRIMARY KEY,
                note_id BLOB NOT NULL,
                local_content TEXT NOT NULL,
                local_modified_at INTEGER NOT NULL,
                local_device_id BLOB,
                local_device_name TEXT,
                remote_content TEXT NOT NULL,
                remote_modified_at INTEGER NOT NULL,
                remote_device_id BLOB,
                remote_device_name TEXT,
                created_at INTEGER NOT NULL,
                resolved_at INTEGER,
                FOREIGN KEY (note_id) REFERENCES notes(id)
            );
        "#)?;

        // conflicts_note_delete
        tx.execute_batch(r#"
            DROP TABLE IF EXISTS conflicts_note_delete;
            CREATE TABLE conflicts_note_delete (
                id BLOB PRIMARY KEY,
                note_id BLOB NOT NULL,
                surviving_content TEXT NOT NULL,
                surviving_modified_at INTEGER NOT NULL,
                surviving_device_id BLOB,
                surviving_device_name TEXT,
                deleted_content TEXT,
                deleted_at INTEGER NOT NULL,
                deleting_device_id BLOB,
                deleting_device_name TEXT,
                created_at INTEGER NOT NULL,
                resolved_at INTEGER,
                FOREIGN KEY (note_id) REFERENCES notes(id)
            );
        "#)?;

        // conflicts_tag_rename
        tx.execute_batch(r#"
            DROP TABLE IF EXISTS conflicts_tag_rename;
            CREATE TABLE conflicts_tag_rename (
                id BLOB PRIMARY KEY,
                tag_id BLOB NOT NULL,
                local_name TEXT NOT NULL,
                local_modified_at INTEGER NOT NULL,
                local_device_id BLOB,
                local_device_name TEXT,
                remote_name TEXT NOT NULL,
                remote_modified_at INTEGER NOT NULL,
                remote_device_id BLOB,
                remote_device_name TEXT,
                created_at INTEGER NOT NULL,
                resolved_at INTEGER,
                FOREIGN KEY (tag_id) REFERENCES tags(id)
            );
        "#)?;

        // conflicts_tag_parent
        tx.execute_batch(r#"
            DROP TABLE IF EXISTS conflicts_tag_parent;
            CREATE TABLE conflicts_tag_parent (
                id BLOB PRIMARY KEY,
                tag_id BLOB NOT NULL,
                local_parent_id BLOB,
                local_modified_at INTEGER NOT NULL,
                local_device_id BLOB,
                local_device_name TEXT,
                remote_parent_id BLOB,
                remote_modified_at INTEGER NOT NULL,
                remote_device_id BLOB,
                remote_device_name TEXT,
                created_at INTEGER NOT NULL,
                resolved_at INTEGER,
                FOREIGN KEY (tag_id) REFERENCES tags(id)
            );
        "#)?;

        // conflicts_tag_delete
        tx.execute_batch(r#"
            DROP TABLE IF EXISTS conflicts_tag_delete;
            CREATE TABLE conflicts_tag_delete (
                id BLOB PRIMARY KEY,
                tag_id BLOB NOT NULL,
                surviving_name TEXT NOT NULL,
                surviving_parent_id BLOB,
                surviving_modified_at INTEGER NOT NULL,
                surviving_device_id BLOB,
                surviving_device_name TEXT,
                deleted_at INTEGER NOT NULL,
                deleting_device_id BLOB,
                deleting_device_name TEXT,
                created_at INTEGER NOT NULL,
                resolved_at INTEGER,
                FOREIGN KEY (tag_id) REFERENCES tags(id)
            );
        "#)?;

        // conflicts_note_tag
        tx.execute_batch(r#"
            DROP TABLE IF EXISTS conflicts_note_tag;
            CREATE TABLE conflicts_note_tag (
                id BLOB PRIMARY KEY,
                note_id BLOB NOT NULL,
                tag_id BLOB NOT NULL,
                local_created_at INTEGER,
                local_modified_at INTEGER,
                local_deleted_at INTEGER,
                local_device_id BLOB,
                local_device_name TEXT,
                remote_created_at INTEGER,
                remote_modified_at INTEGER,
                remote_deleted_at INTEGER,
                remote_device_id BLOB,
                remote_device_name TEXT,
                created_at INTEGER NOT NULL,
                resolved_at INTEGER,
                FOREIGN KEY (note_id) REFERENCES notes(id),
                FOREIGN KEY (tag_id) REFERENCES tags(id)
            );
        "#)?;

        // sync_failures
        tx.execute_batch(r#"
            CREATE TABLE sync_failures_new (
                id BLOB PRIMARY KEY,
                peer_id BLOB NOT NULL,
                peer_name TEXT,
                entity_type TEXT NOT NULL,
                entity_id BLOB,
                operation TEXT NOT NULL,
                payload TEXT NOT NULL,
                error_message TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                resolved_at INTEGER,
                FOREIGN KEY (peer_id) REFERENCES sync_peers(peer_id)
            );
            INSERT INTO sync_failures_new SELECT
                id, peer_id, peer_name, entity_type, entity_id, operation, payload, error_message,
                CAST(strftime('%s', created_at) AS INTEGER),
                CAST(strftime('%s', resolved_at) AS INTEGER)
            FROM sync_failures;
            DROP TABLE sync_failures;
            ALTER TABLE sync_failures_new RENAME TO sync_failures;
        "#)?;

        // Recreate all indexes
        tx.execute_batch(r#"
            CREATE INDEX IF NOT EXISTS idx_notes_created_at ON notes(created_at);
            CREATE INDEX IF NOT EXISTS idx_notes_deleted_at ON notes(deleted_at);
            CREATE INDEX IF NOT EXISTS idx_notes_modified_at ON notes(modified_at);
            CREATE INDEX IF NOT EXISTS idx_notes_sync_received_at ON notes(sync_received_at);
            CREATE INDEX IF NOT EXISTS idx_tags_parent_id ON tags(parent_id);
            CREATE INDEX IF NOT EXISTS idx_tags_name ON tags(LOWER(name));
            CREATE INDEX IF NOT EXISTS idx_tags_modified_at ON tags(modified_at);
            CREATE INDEX IF NOT EXISTS idx_tags_sync_received_at ON tags(sync_received_at);
            CREATE INDEX IF NOT EXISTS idx_note_tags_note ON note_tags(note_id);
            CREATE INDEX IF NOT EXISTS idx_note_tags_tag ON note_tags(tag_id);
            CREATE INDEX IF NOT EXISTS idx_note_tags_created_at ON note_tags(created_at);
            CREATE INDEX IF NOT EXISTS idx_note_tags_deleted_at ON note_tags(deleted_at);
            CREATE INDEX IF NOT EXISTS idx_note_tags_modified_at ON note_tags(modified_at);
            CREATE INDEX IF NOT EXISTS idx_note_tags_search ON note_tags(note_id, tag_id, deleted_at);
            CREATE INDEX IF NOT EXISTS idx_note_tags_by_tag ON note_tags(tag_id, note_id, deleted_at);
            CREATE INDEX IF NOT EXISTS idx_note_tags_sync_received_at ON note_tags(sync_received_at);
            CREATE INDEX IF NOT EXISTS idx_note_attachments_note_id ON note_attachments(note_id);
            CREATE INDEX IF NOT EXISTS idx_note_attachments_attachment_id ON note_attachments(attachment_id);
            CREATE INDEX IF NOT EXISTS idx_note_attachments_type ON note_attachments(attachment_type);
            CREATE INDEX IF NOT EXISTS idx_note_attachments_modified_at ON note_attachments(modified_at);
            CREATE INDEX IF NOT EXISTS idx_note_attachments_deleted_at ON note_attachments(deleted_at);
            CREATE INDEX IF NOT EXISTS idx_note_attachments_sync_received_at ON note_attachments(sync_received_at);
            CREATE INDEX IF NOT EXISTS idx_audio_files_modified_at ON audio_files(modified_at);
            CREATE INDEX IF NOT EXISTS idx_audio_files_deleted_at ON audio_files(deleted_at);
            CREATE INDEX IF NOT EXISTS idx_audio_files_sync_received_at ON audio_files(sync_received_at);
            CREATE INDEX IF NOT EXISTS idx_transcriptions_audio_file_id ON transcriptions(audio_file_id);
            CREATE INDEX IF NOT EXISTS idx_transcriptions_service ON transcriptions(service);
            CREATE INDEX IF NOT EXISTS idx_transcriptions_created_at ON transcriptions(created_at);
            CREATE INDEX IF NOT EXISTS idx_transcriptions_modified_at ON transcriptions(modified_at);
            CREATE INDEX IF NOT EXISTS idx_transcriptions_deleted_at ON transcriptions(deleted_at);
            CREATE INDEX IF NOT EXISTS idx_transcriptions_sync_received_at ON transcriptions(sync_received_at);
        "#)?;

        tx.commit()?;

        tracing::info!("Timestamp migration completed successfully");
        Ok(())
    }

    /// Migrate existing databases to add cloud storage columns to audio_files.
    ///
    /// This adds three columns for tracking cloud storage state:
    /// - storage_provider: "s3", "backblaze", etc. (NULL = local only)
    /// - storage_key: Object key/path in cloud storage
    /// - storage_uploaded_at: Unix timestamp when file was uploaded
    ///
    /// This is idempotent - it checks if columns exist before adding them.
    fn migrate_add_storage_columns(&mut self) -> VoiceResult<()> {
        // Helper to check if a column exists in a table
        fn column_exists(conn: &Connection, table: &str, column: &str) -> bool {
            let sql = format!("PRAGMA table_info({})", table);
            let mut stmt = conn.prepare(&sql).unwrap();
            let rows = stmt.query_map([], |row| {
                let name: String = row.get(1)?;
                Ok(name)
            }).unwrap();
            for row in rows {
                if let Ok(name) = row {
                    if name == column {
                        return true;
                    }
                }
            }
            false
        }

        // Add storage_provider column if it doesn't exist
        if !column_exists(&self.conn, "audio_files", "storage_provider") {
            self.conn.execute(
                "ALTER TABLE audio_files ADD COLUMN storage_provider TEXT",
                [],
            )?;
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_audio_files_storage_provider ON audio_files(storage_provider)",
                [],
            )?;
            tracing::info!("Added storage_provider column to audio_files");
        }

        // Add storage_key column if it doesn't exist
        if !column_exists(&self.conn, "audio_files", "storage_key") {
            self.conn.execute(
                "ALTER TABLE audio_files ADD COLUMN storage_key TEXT",
                [],
            )?;
            tracing::info!("Added storage_key column to audio_files");
        }

        // Add storage_uploaded_at column if it doesn't exist
        if !column_exists(&self.conn, "audio_files", "storage_uploaded_at") {
            self.conn.execute(
                "ALTER TABLE audio_files ADD COLUMN storage_uploaded_at INTEGER",
                [],
            )?;
            tracing::info!("Added storage_uploaded_at column to audio_files");
        }

        Ok(())
    }

    /// Migrate existing databases to add file_storage_config table.
    ///
    /// This table stores cloud file storage configuration that syncs between devices.
    /// Uses a single row with id="default".
    ///
    /// This is idempotent - it checks if the table exists before creating it.
    fn migrate_add_file_storage_config_table(&mut self) -> VoiceResult<()> {
        // Check if table already exists
        let table_exists: bool = self.conn.query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='file_storage_config'",
            [],
            |_| Ok(true),
        ).unwrap_or(false);

        if !table_exists {
            self.conn.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS file_storage_config (
                    id TEXT PRIMARY KEY DEFAULT 'default',
                    provider TEXT NOT NULL DEFAULT 'none',
                    config TEXT,
                    modified_at INTEGER,
                    device_id BLOB,
                    sync_received_at INTEGER
                );
                CREATE INDEX IF NOT EXISTS idx_file_storage_config_sync_received_at ON file_storage_config(sync_received_at);
                "#,
            )?;
            tracing::info!("Created file_storage_config table");
        }

        Ok(())
    }

    // =========================================================================
    // UUID Prefix Resolution
    // =========================================================================
    // These methods resolve UUID prefixes (like Git's short commit hashes) to
    // full UUIDs. If the input is already a valid full UUID, it's returned as-is.
    // If the input is a prefix, it searches the database for matching entities.
    //
    // Two variants are provided:
    // - resolve_*: Returns error if not found or ambiguous
    // - try_resolve_*: Returns None if not found, error only if ambiguous

    /// Try to resolve an ID or prefix. Returns None if not found, Some(id) if unique match,
    /// or errors if ambiguous or invalid format.
    fn try_resolve_id(
        &self,
        id_or_prefix: &str,
        table: &str,
        field_name: &str,
        entity_name: &str,
        extra_where: &str,
    ) -> VoiceResult<Option<String>> {
        let prefix_lower = id_or_prefix.replace('-', "").to_lowercase();

        // Validate that input looks like a UUID prefix (hex chars only, max 32 chars)
        if prefix_lower.is_empty() {
            return Err(VoiceError::validation(field_name, "ID cannot be empty"));
        }
        if prefix_lower.len() > 32 {
            return Err(VoiceError::validation(
                field_name,
                format!("invalid {} ID format", entity_name),
            ));
        }
        if !prefix_lower.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(VoiceError::validation(
                field_name,
                format!("invalid {} ID format", entity_name),
            ));
        }

        let like_pattern = format!("{}%", prefix_lower);

        let sql = format!(
            "SELECT id FROM {} WHERE lower(hex(id)) LIKE ?1 {} LIMIT 2",
            table, extra_where
        );

        let mut stmt = self.conn.prepare(&sql)?;
        let results: Vec<Vec<u8>> = stmt
            .query_map([&like_pattern], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?;

        match results.len() {
            0 => Ok(None),
            1 => {
                let hex = uuid_bytes_to_hex(&results[0]).ok_or_else(|| {
                    VoiceError::validation(field_name, "invalid UUID in database")
                })?;
                Ok(Some(hex))
            }
            _ => Err(VoiceError::validation(
                field_name,
                format!("ambiguous {} prefix '{}'", entity_name, id_or_prefix),
            )),
        }
    }

    /// Resolve a note ID or prefix to a full note ID.
    pub fn resolve_note_id(&self, id_or_prefix: &str) -> VoiceResult<String> {
        self.try_resolve_id(id_or_prefix, "notes", "note_id", "note", "")?
            .ok_or_else(|| {
                VoiceError::validation(
                    "note_id",
                    format!("no note found matching prefix '{}'", id_or_prefix),
                )
            })
    }

    /// Try to resolve a note ID or prefix. Returns None if not found.
    pub fn try_resolve_note_id(&self, id_or_prefix: &str) -> VoiceResult<Option<String>> {
        self.try_resolve_id(id_or_prefix, "notes", "note_id", "note", "")
    }

    /// Resolve a tag ID or prefix to a full tag ID.
    pub fn resolve_tag_id(&self, id_or_prefix: &str) -> VoiceResult<String> {
        self.try_resolve_id(id_or_prefix, "tags", "tag_id", "tag", "")?
            .ok_or_else(|| {
                VoiceError::validation(
                    "tag_id",
                    format!("no tag found matching prefix '{}'", id_or_prefix),
                )
            })
    }

    /// Try to resolve a tag ID or prefix. Returns None if not found.
    pub fn try_resolve_tag_id(&self, id_or_prefix: &str) -> VoiceResult<Option<String>> {
        self.try_resolve_id(id_or_prefix, "tags", "tag_id", "tag", "")
    }

    /// Resolve an audio file ID or prefix to a full audio file ID.
    /// Only searches non-deleted audio files.
    pub fn resolve_audio_file_id(&self, id_or_prefix: &str) -> VoiceResult<String> {
        self.try_resolve_id(
            id_or_prefix,
            "audio_files",
            "audio_file_id",
            "audio file",
            "AND deleted_at IS NULL",
        )?
        .ok_or_else(|| {
            VoiceError::validation(
                "audio_file_id",
                format!("no audio file found matching prefix '{}'", id_or_prefix),
            )
        })
    }

    /// Try to resolve an audio file ID or prefix. Returns None if not found.
    /// Only searches non-deleted audio files.
    pub fn try_resolve_audio_file_id(&self, id_or_prefix: &str) -> VoiceResult<Option<String>> {
        self.try_resolve_id(
            id_or_prefix,
            "audio_files",
            "audio_file_id",
            "audio file",
            "AND deleted_at IS NULL",
        )
    }

    /// Try to resolve an audio file ID or prefix, including deleted files.
    fn try_resolve_audio_file_id_including_deleted(
        &self,
        id_or_prefix: &str,
    ) -> VoiceResult<Option<String>> {
        self.try_resolve_id(id_or_prefix, "audio_files", "audio_file_id", "audio file", "")
    }

    /// Resolve multiple tag IDs or prefixes to full tag UUIDs.
    /// Each ID can be a full UUID or a prefix.
    pub fn resolve_tag_ids(&self, tag_ids: &[String]) -> VoiceResult<Vec<Uuid>> {
        tag_ids
            .iter()
            .enumerate()
            .map(|(i, tag_id)| {
                let resolved = self.resolve_tag_id(tag_id).map_err(|e| {
                    VoiceError::validation("tag_ids", format!("item {}: {}", i, e))
                })?;
                Uuid::parse_str(&resolved).map_err(|_| {
                    VoiceError::validation("tag_ids", format!("item {}: invalid UUID", i))
                })
            })
            .collect()
    }

    /// Get the underlying connection (for advanced operations)
    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    /// Get all non-deleted notes with their associated tag names
    pub fn get_all_notes(&self) -> VoiceResult<Vec<NoteRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT
                n.id,
                n.created_at,
                n.content,
                n.modified_at,
                n.deleted_at,
                GROUP_CONCAT(t.name, ', ') as tag_names,
                NULL as di_cache_note_pane_display,
                n.di_cache_note_list_pane_display,
                n.created_at_offset,
                n.created_at_zone,
                n.modified_at_offset,
                n.modified_at_zone,
                n.deleted_at_offset,
                n.deleted_at_zone
            FROM notes n
            LEFT JOIN note_tags nt ON n.id = nt.note_id AND nt.deleted_at IS NULL
            LEFT JOIN tags t ON nt.tag_id = t.id
            WHERE n.deleted_at IS NULL
            GROUP BY n.id
            ORDER BY n.created_at DESC
            "#,
        )?;

        let notes = stmt
            .query_map([], |row| self.row_to_note(row))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(notes)
    }

    /// Get a note by ID (or ID prefix) with its associated tags
    pub fn get_note(&self, note_id: &str) -> VoiceResult<Option<NoteRow>> {
        // Use try_resolve to return None if not found (instead of error)
        let resolved_id = match self.try_resolve_note_id(note_id)? {
            Some(id) => id,
            None => return Ok(None),
        };
        let uuid = Uuid::parse_str(&resolved_id)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?;
        let uuid_bytes = uuid.as_bytes().to_vec();

        let mut stmt = self.conn.prepare(
            r#"
            SELECT
                n.id,
                n.created_at,
                n.content,
                n.modified_at,
                n.deleted_at,
                GROUP_CONCAT(t.name, ', ') as tag_names,
                n.di_cache_note_pane_display,
                n.di_cache_note_list_pane_display,
                n.created_at_offset,
                n.created_at_zone,
                n.modified_at_offset,
                n.modified_at_zone,
                n.deleted_at_offset,
                n.deleted_at_zone
            FROM notes n
            LEFT JOIN note_tags nt ON n.id = nt.note_id AND nt.deleted_at IS NULL
            LEFT JOIN tags t ON nt.tag_id = t.id
            WHERE n.id = ? AND n.deleted_at IS NULL
            GROUP BY n.id
            "#,
        )?;

        let mut rows = stmt.query_map([uuid_bytes], |row| self.row_to_note(row))?;
        match rows.next() {
            Some(Ok(note)) => Ok(Some(note)),
            Some(Err(e)) => Err(VoiceError::Database(e)),
            None => Ok(None),
        }
    }

    /// Create a new note
    pub fn create_note(&self, content: &str) -> VoiceResult<String> {
        let note_id = Uuid::now_v7();
        let uuid_bytes = note_id.as_bytes().to_vec();

        self.conn.execute(
            "INSERT INTO notes (id, content, created_at) VALUES (?, ?, strftime('%s', 'now'))",
            params![uuid_bytes, content],
        )?;

        let note_id_hex = note_id.simple().to_string();
        // The clock this device was reading when the note was made
        let _ = self.stamp_local_zone("notes", &note_id_hex, "created_at");

        // The root of this note's content history
        self.init_field(ENTITY_NOTE, &note_id_hex, FIELD_CONTENT, content)?;

        // Rebuild list cache for the new note
        let _ = self.rebuild_note_list_cache(&note_id_hex);

        Ok(note_id_hex)
    }

    /// Create a new note with a specific timestamp
    ///
    /// If `created_at` is provided (Unix timestamp), it will be used as the note's creation time.
    /// If `created_at` is None, the current time is used.
    pub fn create_note_with_timestamp(
        &self,
        content: &str,
        created_at: Option<i64>,
    ) -> VoiceResult<String> {
        let note_id = Uuid::now_v7();
        let uuid_bytes = note_id.as_bytes().to_vec();

        match created_at {
            Some(ts) => {
                self.conn.execute(
                    "INSERT INTO notes (id, content, created_at) VALUES (?, ?, ?)",
                    params![uuid_bytes, content, ts],
                )?;
            }
            None => {
                self.conn.execute(
                    "INSERT INTO notes (id, content, created_at) VALUES (?, ?, strftime('%s', 'now'))",
                    params![uuid_bytes, content],
                )?;
            }
        }

        let note_id_hex = note_id.simple().to_string();
        let _ = self.stamp_local_zone("notes", &note_id_hex, "created_at");

        // Rebuild list cache for the new note
        let _ = self.rebuild_note_list_cache(&note_id_hex);

        Ok(note_id_hex)
    }

    /// Import an audio file, creating all necessary records in one operation.
    ///
    /// This creates:
    /// 1. An AudioFile record
    /// 2. A Note record (with created_at = file_created_at if provided)
    /// 3. A NoteAttachment linking them
    ///
    /// Returns (note_id, audio_file_id) as hex strings.
    pub fn import_audio_file(
        &self,
        filename: &str,
        file_created_at: Option<i64>,
        duration_seconds: Option<i64>,
        audio_dir: Option<&Path>,
    ) -> VoiceResult<(String, String)> {
        // 1. Create audio file record; the file keeps its own name (FILE-15)
        let audio_file_id = self.create_audio_file_with_duration(filename, file_created_at, duration_seconds, crate::models::FileOrigin::Imported, audio_dir)?;

        // 2. Create note with file's creation date (empty content)
        let note_id = self.create_note_with_timestamp("", file_created_at)?;

        // 3. Attach audio file to note
        self.attach_to_note(&note_id, &audio_file_id, "audio_file")?;

        Ok((note_id, audio_file_id))
    }

    // =====================================================================
    // Which attachment or transcription stands for its parent
    // =====================================================================

    /// Make one of a note's attachments the one that stands for it: the
    /// recording played when the note is opened, and the one whose
    /// transcription the notes list shows.
    ///
    /// Pass `None` to go back to "the first one", which is what a note that
    /// has never been asked uses.
    pub fn set_primary_attachment(&self, note_id: &str, attachment_id: Option<&str>) -> VoiceResult<bool> {
        let resolved = self.resolve_note_id(note_id)?;
        let id_hex = Uuid::parse_str(&resolved)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?
            .simple()
            .to_string();
        let value = match attachment_id {
            Some(a) => {
                let attachment = Uuid::parse_str(a)
                    .map_err(|e| VoiceError::validation("attachment_id", e.to_string()))?;
                let belongs: Option<i64> = self
                    .conn
                    .query_row(
                        "SELECT 1 FROM note_attachments WHERE id = ? AND note_id = ? AND deleted_at IS NULL",
                        params![attachment.as_bytes().to_vec(), Uuid::parse_str(&resolved).unwrap().as_bytes().to_vec()],
                        |r| r.get(0),
                    )
                    .optional()?;
                if belongs.is_none() {
                    return Err(VoiceError::validation(
                        "attachment_id",
                        "That attachment does not belong to this note",
                    ));
                }
                attachment.simple().to_string()
            }
            None => String::new(),
        };
        self.set_field(ENTITY_NOTE, &id_hex, FIELD_PRIMARY_ATTACHMENT, &value, None)?;
        Ok(true)
    }

    /// Make one of a recording's transcriptions the one that stands for it.
    /// `None` goes back to the first one.
    pub fn set_primary_transcription(&self, audio_file_id: &str, transcription_id: Option<&str>) -> VoiceResult<bool> {
        let audio = Uuid::parse_str(audio_file_id)
            .map_err(|e| VoiceError::validation("audio_file_id", e.to_string()))?;
        let value = match transcription_id {
            Some(t) => {
                let transcription = Uuid::parse_str(t)
                    .map_err(|e| VoiceError::validation("transcription_id", e.to_string()))?;
                let belongs: Option<i64> = self
                    .conn
                    .query_row(
                        "SELECT 1 FROM transcriptions WHERE id = ? AND audio_file_id = ? AND deleted_at IS NULL",
                        params![transcription.as_bytes().to_vec(), audio.as_bytes().to_vec()],
                        |r| r.get(0),
                    )
                    .optional()?;
                if belongs.is_none() {
                    return Err(VoiceError::validation(
                        "transcription_id",
                        "That transcription does not belong to this recording",
                    ));
                }
                transcription.simple().to_string()
            }
            None => String::new(),
        };
        self.set_field(ENTITY_AUDIO_FILE, &audio.simple().to_string(), FIELD_PRIMARY_TRANSCRIPTION, &value, None)?;
        Ok(true)
    }

    /// The attachment that stands for this note, if one was chosen.
    pub fn get_primary_attachment(&self, note_id: &str) -> VoiceResult<Option<String>> {
        let resolved = self.resolve_note_id(note_id)?;
        let uuid = Uuid::parse_str(&resolved)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?;
        let chosen: Option<Option<Vec<u8>>> = self
            .conn
            .query_row(
                "SELECT primary_attachment_id FROM notes WHERE id = ?",
                params![uuid.as_bytes().to_vec()],
                |r| r.get(0),
            )
            .optional()?;
        Ok(chosen.flatten().and_then(|b| uuid_bytes_to_hex(&b)))
    }

    /// The transcription that stands for this recording, if one was chosen.
    pub fn get_primary_transcription(&self, audio_file_id: &str) -> VoiceResult<Option<String>> {
        let uuid = Uuid::parse_str(audio_file_id)
            .map_err(|e| VoiceError::validation("audio_file_id", e.to_string()))?;
        let chosen: Option<Option<Vec<u8>>> = self
            .conn
            .query_row(
                "SELECT primary_transcription_id FROM audio_files WHERE id = ?",
                params![uuid.as_bytes().to_vec()],
                |r| r.get(0),
            )
            .optional()?;
        Ok(chosen.flatten().and_then(|b| uuid_bytes_to_hex(&b)))
    }

    // =====================================================================
    // The trash bin
    // =====================================================================

    /// The notes in the trash: deleted, but still here, newest deletion
    /// first.
    ///
    /// A delete in this application has always been a soft delete, so every
    /// note that was ever deleted is still in the database with its history
    /// and its recordings. This is how the user sees them and gets them
    /// back.
    pub fn get_deleted_notes(&self) -> VoiceResult<Vec<NoteRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT
                n.id,
                n.created_at,
                n.content,
                n.modified_at,
                n.deleted_at,
                GROUP_CONCAT(t.name, ', ') as tag_names,
                NULL as di_cache_note_pane_display,
                n.di_cache_note_list_pane_display,
                n.created_at_offset,
                n.created_at_zone,
                n.modified_at_offset,
                n.modified_at_zone,
                n.deleted_at_offset,
                n.deleted_at_zone
            FROM notes n
            LEFT JOIN note_tags nt ON n.id = nt.note_id AND nt.deleted_at IS NULL
            LEFT JOIN tags t ON nt.tag_id = t.id
            WHERE n.deleted_at IS NOT NULL
            GROUP BY n.id
            -- The id breaks a tie: two notes deleted in the same second
            -- would otherwise come back in whatever order the table
            -- happened to hold them. Ids are time-ordered, so the newer
            -- note stays on top.
            ORDER BY n.deleted_at DESC, n.created_at DESC, n.id DESC
            "#,
        )?;
        let notes = stmt
            .query_map([], |row| self.row_to_note(row))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(notes)
    }

    /// Take a note out of the trash.
    ///
    /// Returns false when the note is not in the trash (already alive, or
    /// purged, or never existed). The recovery is a version like any other,
    /// so it reaches the other devices by the ordinary route.
    pub fn undelete_note(&self, note_id: &str) -> VoiceResult<bool> {
        let resolved = match self.try_resolve_note_id(note_id)? {
            Some(id) => id,
            None => return Ok(false),
        };
        let uuid = Uuid::parse_str(&resolved)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?;
        let id_hex = uuid.simple().to_string();
        let deleted: Option<Option<i64>> = self
            .conn
            .query_row(
                "SELECT deleted_at FROM notes WHERE id = ?",
                params![uuid.as_bytes().to_vec()],
                |row| row.get(0),
            )
            .optional()?;
        if deleted.flatten().is_none() {
            return Ok(false);
        }
        let restored = self.set_undeleted(ENTITY_NOTE, &id_hex)?;
        if restored {
            let _ = self.rebuild_note_cache(&resolved);
            let _ = self.rebuild_note_list_cache(&resolved);
        }
        Ok(restored)
    }

    /// Remove a note from the trash for good, with everything that belonged
    /// only to it.
    ///
    /// This is the one operation in the application that really destroys
    /// something. The note, its history, its tag links, its attachments and
    /// the recordings that hung on this note alone are removed from the
    /// database, and a `purges` row is written for each of them. Those rows
    /// travel to the other devices, which remove the same entities, and they
    /// stay for ever so that a peer which has not synced yet cannot bring
    /// any of it back.
    ///
    /// Returns the ids of the audio files that were removed, so the caller
    /// can delete the files themselves: the database does not know where
    /// each platform keeps them.
    ///
    /// A recording that is also attached to a note which is staying is left
    /// alone, along with its transcriptions.
    pub fn purge_note(&self, note_id: &str) -> VoiceResult<Vec<String>> {
        let resolved = self.resolve_note_id(note_id)?;
        let uuid = Uuid::parse_str(&resolved)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?;
        let note_bytes = uuid.as_bytes().to_vec();
        let deleted: Option<i64> = self
            .conn
            .query_row(
                "SELECT deleted_at FROM notes WHERE id = ?",
                params![&note_bytes],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        if deleted.is_none() {
            return Err(VoiceError::validation(
                "note_id",
                "Only a note in the trash can be deleted for good; delete it first",
            ));
        }

        // What goes: the note, its links, its attachments, and the audio
        // files (with their transcriptions) that no surviving note holds.
        let mut victims: Vec<(String, Vec<u8>)> = vec![(ENTITY_NOTE.to_string(), note_bytes.clone())];

        let tag_ids: Vec<Vec<u8>> = {
            let mut stmt = self.conn.prepare("SELECT tag_id FROM note_tags WHERE note_id = ?")?;
            let rows = stmt.query_map(params![&note_bytes], |r| r.get(0))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };

        let attachments: Vec<(Vec<u8>, Vec<u8>, String)> = {
            let mut stmt = self.conn.prepare(
                "SELECT id, attachment_id, attachment_type FROM note_attachments WHERE note_id = ?",
            )?;
            let rows = stmt.query_map(params![&note_bytes], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };

        let mut audio_ids: Vec<Vec<u8>> = Vec::new();
        for (attachment_row_id, attachment_id, attachment_type) in &attachments {
            victims.push((ENTITY_NOTE_ATTACHMENT.to_string(), attachment_row_id.clone()));
            if attachment_type != "audio_file" {
                continue;
            }
            // Is this recording held by any note that is staying?
            let held_elsewhere: i64 = self.conn.query_row(
                "SELECT COUNT(*) FROM note_attachments \
                 WHERE attachment_id = ? AND note_id != ? AND deleted_at IS NULL",
                params![attachment_id, &note_bytes],
                |r| r.get(0),
            )?;
            if held_elsewhere == 0 {
                audio_ids.push(attachment_id.clone());
            }
        }

        for audio_id in &audio_ids {
            victims.push((ENTITY_AUDIO_FILE.to_string(), audio_id.clone()));
            let transcriptions: Vec<Vec<u8>> = {
                let mut stmt = self
                    .conn
                    .prepare("SELECT id FROM transcriptions WHERE audio_file_id = ?")?;
                let rows = stmt.query_map(params![audio_id], |r| r.get(0))?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            };
            for t in transcriptions {
                victims.push((ENTITY_TRANSCRIPTION.to_string(), t));
            }
        }

        // The bucket objects of the recordings that go: tagged purged at the
        // next upload run, and deleted by the lifecycle rule a day later
        // (Stage 14). Remembered before the rows go.
        for audio_id in &audio_ids {
            let key: Option<Option<String>> = self
                .conn
                .query_row("SELECT storage_key FROM audio_files WHERE id = ? AND storage_key IS NOT NULL", params![audio_id], |r| r.get(0))
                .optional()?;
            if let Some(Some(key)) = key {
                self.conn.execute(
                    "INSERT OR IGNORE INTO purged_objects (storage_key, at) VALUES (?, ?)",
                    params![key, Utc::now().timestamp()],
                )?;
            }
        }

        let purged_at = Utc::now().timestamp();
        for (entity_type, entity_id) in &victims {
            self.purge_entity(entity_type, entity_id, purged_at)?;
        }
        let _ = tag_ids;

        Ok(audio_ids
            .iter()
            .filter_map(|b| uuid_bytes_to_hex(b))
            .collect())
    }

    /// Write down that an entity was removed for good.
    fn record_purge(&self, entity_type: &str, entity_id: &[u8], purged_at: i64) -> VoiceResult<()> {
        self.conn.execute(
            "INSERT OR IGNORE INTO purges \
             (entity_type, entity_id, purged_at, purged_at_offset, purged_at_zone, device_id) \
             VALUES (?, ?, ?, ?, ?, ?)",
            params![
                entity_type,
                entity_id.to_vec(),
                purged_at,
                crate::timezone::stamp_offset(),
                crate::timezone::stamp_zone(),
                get_local_device_id().as_bytes().to_vec(),
            ],
        )?;
        Ok(())
    }

    /// Whether this entity was removed for good, here or on another device.
    pub fn is_purged(&self, entity_type: &str, entity_id: &str) -> VoiceResult<bool> {
        let uuid = match Uuid::parse_str(entity_id) {
            Ok(u) => u,
            Err(_) => return Ok(false),
        };
        let found: Option<i64> = self
            .conn
            .query_row(
                "SELECT 1 FROM purges WHERE entity_type = ? AND entity_id = ?",
                params![entity_type, uuid.as_bytes().to_vec()],
                |r| r.get(0),
            )
            .optional()?;
        Ok(found.is_some())
    }

    /// Remove one entity for good: write the purge down, take the entity
    /// away, and do the same for everything that hangs on it.
    ///
    /// The cascade is written down rather than merely done, because those
    /// purge records travel: a device that had an attachment on the note
    /// which the device emptying the trash never saw records a purge for it
    /// too, and every device ends up removing the same set. Without that,
    /// two devices could each hold rows the other had removed and never
    /// agree again.
    fn purge_entity(&self, entity_type: &str, entity_id: &[u8], purged_at: i64) -> VoiceResult<()> {
        let already = self.purge_recorded(entity_type, entity_id)?;
        self.record_purge(entity_type, entity_id, purged_at)?;
        if already {
            // Its dependants were dealt with when it was first purged.
            self.remove_purged_entity(entity_type, entity_id)?;
            return Ok(());
        }
        let dependants: Vec<(String, Vec<u8>)> = match entity_type {
            ENTITY_NOTE => {
                let mut stmt = self
                    .conn
                    .prepare("SELECT id FROM note_attachments WHERE note_id = ?")?;
                let rows = stmt.query_map(params![entity_id.to_vec()], |r| r.get::<_, Vec<u8>>(0))?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
                    .into_iter()
                    .map(|id| (ENTITY_NOTE_ATTACHMENT.to_string(), id))
                    .collect()
            }
            ENTITY_AUDIO_FILE => {
                let mut stmt = self
                    .conn
                    .prepare("SELECT id FROM transcriptions WHERE audio_file_id = ?")?;
                let rows = stmt.query_map(params![entity_id.to_vec()], |r| r.get::<_, Vec<u8>>(0))?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
                    .into_iter()
                    .map(|id| (ENTITY_TRANSCRIPTION.to_string(), id))
                    .collect()
            }
            _ => Vec::new(),
        };
        for (dependant_type, dependant_id) in dependants {
            self.purge_entity(&dependant_type, &dependant_id, purged_at)?;
        }
        self.remove_purged_entity(entity_type, entity_id)?;
        Ok(())
    }

    /// Whether this exact entity already has a purge record here.
    fn purge_recorded(&self, entity_type: &str, entity_id: &[u8]) -> VoiceResult<bool> {
        let found: Option<i64> = self
            .conn
            .query_row(
                "SELECT 1 FROM purges WHERE entity_type = ? AND entity_id = ?",
                params![entity_type, entity_id.to_vec()],
                |r| r.get(0),
            )
            .optional()?;
        Ok(found.is_some())
    }

    /// Remove one entity and its history from this database.
    ///
    /// A purge cascades only along relationships that never move: a tag link
    /// belongs to its note for ever, and a transcription to its recording.
    /// An attachment can be moved from one note to another (that is what
    /// merging does), so removing "the attachments of this note" would
    /// remove different rows on different devices depending on which changes
    /// had arrived, and the two databases could never agree again.
    /// Attachments are therefore removed only when they are named, and the
    /// device that empties the trash names every one it can see.
    fn remove_purged_entity(&self, entity_type: &str, entity_id: &[u8]) -> VoiceResult<()> {
        let id_hex = uuid_bytes_to_hex(entity_id).unwrap_or_default();
        match entity_type {
            ENTITY_NOTE => {
                self.conn.execute("DELETE FROM note_tags WHERE note_id = ?", params![entity_id.to_vec()])?;
                self.conn.execute("DELETE FROM notes WHERE id = ?", params![entity_id.to_vec()])?;
                // A tag link is named by the pair of ids, so it cannot have a
                // purge record of its own; its history goes with the note's,
                // and every device does this the same way when the note's
                // purge arrives. Leaving it behind left one device with a
                // head the other did not have.
                let link_prefix = format!("{}:%", id_hex);
                for table in ["field_versions", "field_heads", "field_conflicts"] {
                    self.conn.execute(
                        &format!(
                            "DELETE FROM {table} WHERE entity_type = 'note_tag' AND entity_id LIKE ?"
                        ),
                        params![&link_prefix],
                    )?;
                }
            }
            ENTITY_AUDIO_FILE => {
                self.conn.execute("DELETE FROM audio_files WHERE id = ?", params![entity_id.to_vec()])?;
            }
            ENTITY_TRANSCRIPTION => {
                self.conn.execute("DELETE FROM transcriptions WHERE id = ?", params![entity_id.to_vec()])?;
            }
            ENTITY_NOTE_ATTACHMENT => {
                self.conn.execute("DELETE FROM note_attachments WHERE id = ?", params![entity_id.to_vec()])?;
            }
            _ => {}
        }
        // The history of a thing that is gone goes with it.
        self.conn.execute(
            "DELETE FROM field_versions WHERE entity_type = ? AND entity_id = ?",
            params![entity_type, &id_hex],
        )?;
        self.conn.execute(
            "DELETE FROM field_heads WHERE entity_type = ? AND entity_id = ?",
            params![entity_type, &id_hex],
        )?;
        self.conn.execute(
            "DELETE FROM field_conflicts WHERE entity_type = ? AND entity_id = ?",
            params![entity_type, &id_hex],
        )?;
        Ok(())
    }

    /// Apply a purge that arrived from another device.
    ///
    /// The entity is removed here too, and the purge is remembered so that
    /// nothing brings it back.
    ///
    /// A purge is obeyed exactly as it was sent, with no local judgement
    /// about whether this device would have removed the same things. The
    /// device that emptied the trash decided what went (a recording held by
    /// another note is never included), and every device must end up with
    /// the same database: a receiver that kept something back "to be safe"
    /// would leave two devices that could never agree again. A recording
    /// attached to another note in the same moment on another device is the
    /// one thing this can lose, and it is the price of a delete that really
    /// deletes.
    pub fn apply_purge(&self, entity_type: &str, entity_id: &str, purged_at: i64) -> VoiceResult<bool> {
        let uuid = Uuid::parse_str(entity_id)
            .map_err(|e| VoiceError::validation("entity_id", e.to_string()))?;
        let bytes = uuid.as_bytes().to_vec();
        self.purge_entity(entity_type, &bytes, purged_at)?;
        Ok(true)
    }

    /// Import a recording into a note that already exists.
    ///
    /// This is what the phone does now: the note is created first and the
    /// recording is made inside it, so the recorder has somewhere to put the
    /// file the moment the user presses Save. Returns the audio file id.
    ///
    /// `import_audio_file` above is the other way round, for the importer,
    /// which meets the file before there is any note.
    pub fn import_audio_file_into_note(
        &self,
        note_id: &str,
        filename: &str,
        file_created_at: Option<i64>,
        duration_seconds: Option<i64>,
        audio_dir: Option<&Path>,
    ) -> VoiceResult<String> {
        // Resolve first: attaching a recording to a note that is not there
        // would leave the file with no way back to the user.
        let resolved_id = self.resolve_note_id(note_id)?;
        // The recorder's file: named by its start and the tail of its id (FILE-15)
        let audio_file_id =
            self.create_audio_file_with_duration(filename, file_created_at, duration_seconds, crate::models::FileOrigin::Recorded, audio_dir)?;
        self.attach_to_note(&resolved_id, &audio_file_id, "audio_file")?;
        Ok(audio_file_id)
    }

    /// Update a note's content (accepts ID or ID prefix)
    pub fn update_note(&self, note_id: &str, content: &str) -> VoiceResult<bool> {
        // Use try_resolve to return false if not found (instead of error)
        let resolved_id = match self.try_resolve_note_id(note_id)? {
            Some(id) => id,
            None => return Ok(false),
        };
        let uuid = Uuid::parse_str(&resolved_id)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?;

        if content.trim().is_empty() {
            return Err(VoiceError::validation("content", "Note content cannot be empty"));
        }

        let exists: Option<i64> = self
            .conn
            .query_row(
                "SELECT 1 FROM notes WHERE id = ? AND deleted_at IS NULL",
                params![uuid.as_bytes().to_vec()],
                |row| row.get(0),
            )
            .optional()?;
        if exists.is_none() {
            return Ok(false);
        }

        // A new version whose parent is the current head; the head recompute
        // writes the content column and rebuilds the display caches.
        self.set_field(ENTITY_NOTE, &resolved_id, FIELD_CONTENT, content, None)?;
        Ok(true)
    }

    /// Soft-delete a note (accepts ID or ID prefix)
    pub fn delete_note(&self, note_id: &str) -> VoiceResult<bool> {
        // Use try_resolve to return false if not found (instead of error)
        let resolved_id = match self.try_resolve_note_id(note_id)? {
            Some(id) => id,
            None => return Ok(false),
        };
        let uuid = Uuid::parse_str(&resolved_id)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?;
        let alive: Option<i64> = self
            .conn
            .query_row(
                "SELECT 1 FROM notes WHERE id = ? AND deleted_at IS NULL",
                params![uuid.as_bytes().to_vec()],
                |row| row.get(0),
            )
            .optional()?;
        if alive.is_none() {
            return Ok(false);
        }
        // Tombstone version carrying what this device saw; a concurrent edit
        // elsewhere resurrects the note and flags a conflict instead of losing it.
        self.set_deleted(ENTITY_NOTE, &resolved_id)
    }

    /// Merge two notes into one.
    /// - Keeps the note with the earliest created_at timestamp
    /// - Concatenates content with "----------------" separator (skipped if one is empty)
    /// - Moves tags from victim to survivor (deduplicates)
    /// - Moves attachments from victim to survivor
    /// - Soft-deletes the victim note
    /// Returns the surviving note ID.
    pub fn merge_notes(&self, note_id_1: &str, note_id_2: &str) -> VoiceResult<String> {
        // 1. Resolve both note IDs
        let resolved_id_1 = self
            .try_resolve_note_id(note_id_1)?
            .ok_or_else(|| VoiceError::validation("note_id_1", "Note not found"))?;
        let resolved_id_2 = self
            .try_resolve_note_id(note_id_2)?
            .ok_or_else(|| VoiceError::validation("note_id_2", "Note not found"))?;

        // Check if same note
        if resolved_id_1 == resolved_id_2 {
            return Err(VoiceError::validation(
                "note_ids",
                "Cannot merge a note with itself",
            ));
        }

        let uuid_1 = Uuid::parse_str(&resolved_id_1)
            .map_err(|e| VoiceError::validation("note_id_1", e.to_string()))?;
        let uuid_2 = Uuid::parse_str(&resolved_id_2)
            .map_err(|e| VoiceError::validation("note_id_2", e.to_string()))?;
        let bytes_1 = uuid_1.as_bytes().to_vec();
        let bytes_2 = uuid_2.as_bytes().to_vec();

        // 2. Get both notes (must exist and not be deleted). The timestamps are
        // Unix seconds; reading them as text made every merge fail with "Note
        // not found", because the type error was reported as a missing row.
        let read_note = |bytes: &Vec<u8>| -> rusqlite::Result<(i64, String, Option<i64>)> {
            self.conn.query_row(
                "SELECT created_at, content, deleted_at FROM notes WHERE id = ?",
                params![bytes],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
        };
        let note_1 = read_note(&bytes_1)
            .map_err(|e| VoiceError::validation("note_id_1", format!("Note not found: {e}")))?;
        let note_2 = read_note(&bytes_2)
            .map_err(|e| VoiceError::validation("note_id_2", format!("Note not found: {e}")))?;

        // Check if either note is deleted
        if note_1.2.is_some() {
            return Err(VoiceError::validation("note_id_1", "Note is deleted"));
        }
        if note_2.2.is_some() {
            return Err(VoiceError::validation("note_id_2", "Note is deleted"));
        }

        // 3. Determine survivor (earlier created_at) and victim (later)
        let (survivor_id, survivor_bytes, survivor_content, victim_bytes, victim_content) =
            if note_1.0 <= note_2.0 {
                (resolved_id_1, bytes_1, note_1.1, bytes_2, note_2.1)
            } else {
                (resolved_id_2, bytes_2, note_2.1, bytes_1, note_1.1)
            };

        // 4. Build merged content
        let merged_content = if survivor_content.is_empty() && victim_content.is_empty() {
            String::new()
        } else if survivor_content.is_empty() {
            victim_content
        } else if victim_content.is_empty() {
            survivor_content
        } else {
            format!("{}\n----------------\n{}", survivor_content, victim_content)
        };

        // 5. Update survivor's content (a normal versioned edit)
        self.set_field(ENTITY_NOTE, &survivor_id, FIELD_CONTENT, &merged_content, None)?;
        let victim_id = uuid_bytes_to_hex(&victim_bytes).unwrap_or_default();

        // 6. Move tags from victim to survivor (with deduplication)
        // First, get all active tags on the victim
        let mut stmt = self.conn.prepare(
            "SELECT tag_id FROM note_tags WHERE note_id = ? AND deleted_at IS NULL",
        )?;
        let victim_tags: Vec<Vec<u8>> = stmt
            .query_map(params![&victim_bytes], |row| row.get(0))?
            .filter_map(|r| r.ok())
            .collect();

        for tag_bytes in victim_tags {
            let tag_hex = uuid_bytes_to_hex(&tag_bytes).unwrap_or_default();
            // Attach to the survivor (no-op if already attached) and detach from the victim.
            self.set_field(ENTITY_NOTE_TAG, &note_tag_entity_id(&survivor_id, &tag_hex), FIELD_ACTIVE, "1", None)?;
            self.set_field(ENTITY_NOTE_TAG, &note_tag_entity_id(&victim_id, &tag_hex), FIELD_ACTIVE, "0", None)?;
        }

        // 7. Move attachments from victim to survivor
        let zone = crate::timezone::local_zone();
        self.conn.execute(
            r#"
            UPDATE note_attachments
            SET note_id = ?, modified_at = strftime('%s', 'now'),
                modified_at_offset = ?, modified_at_zone = ?
            WHERE note_id = ? AND deleted_at IS NULL
            "#,
            params![&survivor_bytes, zone.offset_seconds, zone.name, &victim_bytes],
        )?;

        // 8. Soft-delete the victim note (tombstone version)
        self.set_deleted(ENTITY_NOTE, &victim_id)?;

        Ok(survivor_id)
    }

    /// Delete a tag (soft delete - sets deleted_at timestamp) (accepts ID or ID prefix)
    pub fn delete_tag(&self, tag_id: &str) -> VoiceResult<bool> {
        // Use try_resolve to return false if not found (instead of error)
        let resolved_id = match self.try_resolve_tag_id(tag_id)? {
            Some(id) => id,
            None => return Ok(false),
        };
        let uuid = Uuid::parse_str(&resolved_id)
            .map_err(|e| VoiceError::validation("tag_id", e.to_string()))?;
        let alive: Option<i64> = self
            .conn
            .query_row(
                "SELECT 1 FROM tags WHERE id = ? AND deleted_at IS NULL",
                params![uuid.as_bytes().to_vec()],
                |row| row.get(0),
            )
            .optional()?;
        if alive.is_none() {
            return Ok(false);
        }
        self.set_deleted(ENTITY_TAG, &resolved_id)
    }

    /// Get all tags with their hierarchy information (excludes deleted tags)
    pub fn get_all_tags(&self) -> VoiceResult<Vec<TagRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, parent_id, created_at, modified_at FROM tags WHERE deleted_at IS NULL ORDER BY name",
        )?;

        let rows = stmt.query_map([], |row| self.row_to_tag(row))?;
        let mut tags = Vec::new();
        for tag in rows {
            tags.push(tag?);
        }
        Ok(tags)
    }

    /// Get a single tag by ID (or ID prefix)
    pub fn get_tag(&self, tag_id: &str) -> VoiceResult<Option<TagRow>> {
        // Use try_resolve to return None if not found (instead of error)
        let resolved_id = match self.try_resolve_tag_id(tag_id)? {
            Some(id) => id,
            None => return Ok(None),
        };
        let uuid = Uuid::parse_str(&resolved_id)
            .map_err(|e| VoiceError::validation("tag_id", e.to_string()))?;
        let uuid_bytes = uuid.as_bytes().to_vec();

        let mut stmt = self.conn.prepare(
            "SELECT id, name, parent_id, created_at, modified_at FROM tags WHERE id = ? AND deleted_at IS NULL",
        )?;

        let mut rows = stmt.query_map([uuid_bytes], |row| self.row_to_tag(row))?;
        match rows.next() {
            Some(Ok(tag)) => Ok(Some(tag)),
            Some(Err(e)) => Err(VoiceError::Database(e)),
            None => Ok(None),
        }
    }

    /// Get all tags with a given name (case-insensitive, excludes deleted tags)
    pub fn get_tags_by_name(&self, name: &str) -> VoiceResult<Vec<TagRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, parent_id, created_at, modified_at FROM tags WHERE LOWER(name) = LOWER(?) AND deleted_at IS NULL",
        )?;

        let rows = stmt.query_map([name], |row| self.row_to_tag(row))?;
        let mut tags = Vec::new();
        for tag in rows {
            tags.push(tag?);
        }
        Ok(tags)
    }

    /// Get a tag by hierarchical path (case-insensitive)
    pub fn get_tag_by_path(&self, path: &str) -> VoiceResult<Option<TagRow>> {
        validate_tag_path(path)?;
        let parts: Vec<&str> = path.split('/').filter(|p| !p.trim().is_empty()).collect();

        if parts.is_empty() {
            return Ok(None);
        }

        let mut current_parent_id: Option<Vec<u8>> = None;

        for part in parts {
            let part = part.trim();
            let tag = if current_parent_id.is_none() {
                self.conn.query_row(
                    "SELECT id, name, parent_id, created_at, modified_at FROM tags WHERE LOWER(name) = LOWER(?) AND parent_id IS NULL AND deleted_at IS NULL",
                    [part],
                    |row| self.row_to_tag(row),
                )
            } else {
                self.conn.query_row(
                    "SELECT id, name, parent_id, created_at, modified_at FROM tags WHERE LOWER(name) = LOWER(?) AND parent_id = ? AND deleted_at IS NULL",
                    params![part, current_parent_id.as_ref().unwrap()],
                    |row| self.row_to_tag(row),
                )
            };

            match tag {
                Ok(t) => {
                    // Convert hex back to bytes for next iteration
                    current_parent_id = Some(Uuid::parse_str(&t.id)?.as_bytes().to_vec());
                }
                Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(None),
                Err(e) => return Err(VoiceError::Database(e)),
            }
        }

        // Return the final tag
        if let Some(ref id_bytes) = current_parent_id {
            let uuid = Uuid::from_slice(id_bytes)?;
            self.get_tag(&uuid.simple().to_string())
        } else {
            Ok(None)
        }
    }

    /// Get all tags matching a path (for ambiguous tag names)
    pub fn get_all_tags_by_path(&self, path: &str) -> VoiceResult<Vec<TagRow>> {
        validate_tag_path(path)?;
        let parts: Vec<&str> = path.split('/').filter(|p| !p.trim().is_empty()).collect();

        if parts.is_empty() {
            return Ok(vec![]);
        }

        // If just a simple name (no slashes), return all tags with that name
        if parts.len() == 1 {
            return self.get_tags_by_name(parts[0].trim());
        }

        // For full paths, navigate through hierarchy
        let first_part = parts[0].trim();
        let mut current_tags = self.get_tags_by_name_and_no_parent(first_part)?;

        if current_tags.is_empty() {
            return Ok(vec![]);
        }

        // Navigate through remaining parts
        for part in &parts[1..] {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            let mut next_tags = Vec::new();
            for tag in &current_tags {
                let children = self.get_tags_by_name_and_parent(part, &tag.id)?;
                next_tags.extend(children);
            }

            current_tags = next_tags;
            if current_tags.is_empty() {
                return Ok(vec![]);
            }
        }

        Ok(current_tags)
    }

    /// Get tags by name with no parent (root tags, excludes deleted tags)
    fn get_tags_by_name_and_no_parent(&self, name: &str) -> VoiceResult<Vec<TagRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, parent_id, created_at, modified_at FROM tags WHERE LOWER(name) = LOWER(?) AND parent_id IS NULL AND deleted_at IS NULL",
        )?;

        let rows = stmt.query_map([name], |row| self.row_to_tag(row))?;
        let mut tags = Vec::new();
        for tag in rows {
            tags.push(tag?);
        }
        Ok(tags)
    }

    /// Get tags by name and parent ID (excludes deleted tags)
    fn get_tags_by_name_and_parent(&self, name: &str, parent_id: &str) -> VoiceResult<Vec<TagRow>> {
        let uuid = validate_tag_id(parent_id)?;
        let uuid_bytes = uuid.as_bytes().to_vec();

        let mut stmt = self.conn.prepare(
            "SELECT id, name, parent_id, created_at, modified_at FROM tags WHERE LOWER(name) = LOWER(?) AND parent_id = ? AND deleted_at IS NULL",
        )?;

        let rows = stmt.query_map(params![name, uuid_bytes], |row| self.row_to_tag(row))?;
        let mut tags = Vec::new();
        for tag in rows {
            tags.push(tag?);
        }
        Ok(tags)
    }

    /// Check if a tag name is ambiguous (appears more than once)
    pub fn is_tag_name_ambiguous(&self, name: &str) -> VoiceResult<bool> {
        let tags = self.get_tags_by_name(name)?;
        Ok(tags.len() > 1)
    }

    /// Get all descendant tag IDs for a given tag using recursive CTE (accepts ID or ID prefix)
    pub fn get_tag_descendants(&self, tag_id: &str) -> VoiceResult<Vec<Vec<u8>>> {
        // Use try_resolve to return empty Vec if tag not found
        let resolved_id = match self.try_resolve_tag_id(tag_id)? {
            Some(id) => id,
            None => return Ok(Vec::new()),
        };
        let uuid = Uuid::parse_str(&resolved_id)
            .map_err(|e| VoiceError::validation("tag_id", e.to_string()))?;
        let uuid_bytes = uuid.as_bytes().to_vec();

        let mut stmt = self.conn.prepare(
            r#"
            WITH RECURSIVE tag_tree AS (
                SELECT id FROM tags WHERE id = ?
                UNION
                SELECT t.id FROM tags t
                JOIN tag_tree tt ON t.parent_id = tt.id
            )
            SELECT id FROM tag_tree
            "#,
        )?;

        let rows = stmt.query_map([uuid_bytes], |row| {
            let id: Vec<u8> = row.get(0)?;
            Ok(id)
        })?;

        let mut ids = Vec::new();
        for id in rows {
            ids.push(id?);
        }
        Ok(ids)
    }

    /// Filter notes by tag IDs or prefixes (including descendants)
    pub fn filter_notes(&self, tag_ids: &[String]) -> VoiceResult<Vec<NoteRow>> {
        if tag_ids.is_empty() {
            return self.get_all_notes();
        }

        let uuids = self.resolve_tag_ids(tag_ids)?;
        let placeholders = vec!["?"; uuids.len()].join(",");

        let query = format!(
            r#"
            SELECT DISTINCT
                n.id,
                n.created_at,
                n.content,
                n.modified_at,
                n.deleted_at,
                GROUP_CONCAT(t.name, ', ') as tag_names,
                NULL as di_cache_note_pane_display,
                n.di_cache_note_list_pane_display,
                n.created_at_offset,
                n.created_at_zone,
                n.modified_at_offset,
                n.modified_at_zone,
                n.deleted_at_offset,
                n.deleted_at_zone
            FROM notes n
            INNER JOIN note_tags nt ON n.id = nt.note_id AND nt.deleted_at IS NULL
            LEFT JOIN tags t ON nt.tag_id = t.id
            WHERE n.deleted_at IS NULL
              AND n.id IN (
                  SELECT note_id FROM note_tags
                  WHERE tag_id IN ({}) AND deleted_at IS NULL
              )
            GROUP BY n.id
            ORDER BY n.created_at DESC
            "#,
            placeholders
        );

        let mut stmt = self.conn.prepare(&query)?;
        let params: Vec<Vec<u8>> = uuids.iter().map(|u| u.as_bytes().to_vec()).collect();
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();

        let notes = stmt
            .query_map(params_refs.as_slice(), |row| self.row_to_note(row))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(notes)
    }

    /// Search notes by text content and/or tags using AND logic.
    /// Tag IDs can be full UUIDs or prefixes.
    pub fn search_notes(
        &self,
        text_query: Option<&str>,
        tag_id_groups: Option<&Vec<Vec<String>>>,
    ) -> VoiceResult<Vec<NoteRow>> {
        validate_search_query(text_query)?;

        let mut query = String::from(
            r#"
            SELECT DISTINCT
                n.id,
                n.created_at,
                n.content,
                n.modified_at,
                n.deleted_at,
                GROUP_CONCAT(t.name, ', ') as tag_names,
                NULL as di_cache_note_pane_display,
                n.di_cache_note_list_pane_display,
                n.created_at_offset,
                n.created_at_zone,
                n.modified_at_offset,
                n.modified_at_zone,
                n.deleted_at_offset,
                n.deleted_at_zone
            FROM notes n
            LEFT JOIN note_tags nt ON n.id = nt.note_id AND nt.deleted_at IS NULL
            LEFT JOIN tags t ON nt.tag_id = t.id
            WHERE n.deleted_at IS NULL
            "#,
        );

        let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        // Add text search condition
        if let Some(text) = text_query {
            if !text.trim().is_empty() {
                query.push_str(" AND LOWER(n.content) LIKE LOWER(?)");
                params.push(Box::new(format!("%{}%", text)));
            }
        }

        // Add tag filter conditions (AND logic)
        if let Some(groups) = tag_id_groups {
            for group in groups {
                if !group.is_empty() {
                    let uuids = self.resolve_tag_ids(group)?;
                    let placeholders = vec!["?"; uuids.len()].join(",");
                    query.push_str(&format!(
                        r#"
                        AND EXISTS (
                            SELECT 1 FROM note_tags
                            WHERE note_id = n.id AND tag_id IN ({}) AND deleted_at IS NULL
                        )
                        "#,
                        placeholders
                    ));
                    for uuid in uuids {
                        params.push(Box::new(uuid.as_bytes().to_vec()));
                    }
                }
            }
        }

        query.push_str(" GROUP BY n.id ORDER BY n.created_at DESC");

        let mut stmt = self.conn.prepare(&query)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params.iter().map(|p| p.as_ref()).collect();

        let notes = stmt
            .query_map(params_refs.as_slice(), |row| self.row_to_note(row))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(notes)
    }

    /// Create a new tag (parent_id accepts ID or ID prefix)
    pub fn create_tag(&self, name: &str, parent_id: Option<&str>) -> VoiceResult<String> {
        let tag_id = Uuid::now_v7();
        let uuid_bytes = tag_id.as_bytes().to_vec();

        let (parent_bytes, parent_hex) = match parent_id {
            Some(pid) => {
                let resolved_id = self.resolve_tag_id(pid)?;
                let uuid = Uuid::parse_str(&resolved_id).map_err(|e| VoiceError::validation("parent_id", e.to_string()))?;
                (Some(uuid.as_bytes().to_vec()), resolved_id)
            }
            None => (None, String::new()),
        };

        self.conn.execute(
            "INSERT INTO tags (id, name, parent_id, created_at) VALUES (?, ?, ?, strftime('%s', 'now'))",
            params![uuid_bytes, name, parent_bytes],
        )?;

        let tag_hex = tag_id.simple().to_string();
        self.init_field(ENTITY_TAG, &tag_hex, FIELD_NAME, name)?;
        self.init_field(ENTITY_TAG, &tag_hex, FIELD_PARENT, &parent_hex)?;

        let _ = self.stamp_local_zone("tags", &tag_hex, "created_at");
        Ok(tag_hex)
    }

    /// Rename a tag (accepts ID or ID prefix)
    pub fn rename_tag(&self, tag_id: &str, new_name: &str) -> VoiceResult<bool> {
        // Use try_resolve to return false if not found (instead of error)
        let resolved_id = match self.try_resolve_tag_id(tag_id)? {
            Some(id) => id,
            None => return Ok(false),
        };
        let uuid = Uuid::parse_str(&resolved_id)
            .map_err(|e| VoiceError::validation("tag_id", e.to_string()))?;
        let exists: Option<i64> = self
            .conn
            .query_row("SELECT 1 FROM tags WHERE id = ?", params![uuid.as_bytes().to_vec()], |row| row.get(0))
            .optional()?;
        if exists.is_none() {
            return Ok(false);
        }
        self.set_field(ENTITY_TAG, &resolved_id, FIELD_NAME, new_name, None)?;
        Ok(true)
    }

    /// Move a tag to a different parent (or make it a root tag)
    ///
    /// # Arguments
    /// * `tag_id` - ID or prefix of the tag to move
    /// * `new_parent_id` - ID or prefix of new parent, or None to make it a root tag
    ///
    /// # Returns
    /// True if the tag was moved, false if tag not found
    pub fn reparent_tag(&self, tag_id: &str, new_parent_id: Option<&str>) -> VoiceResult<bool> {
        // Use try_resolve to return false if not found
        let resolved_id = match self.try_resolve_tag_id(tag_id)? {
            Some(id) => id,
            None => return Ok(false),
        };
        let uuid = Uuid::parse_str(&resolved_id)
            .map_err(|e| VoiceError::validation("tag_id", e.to_string()))?;
        let tag_bytes = uuid.as_bytes().to_vec();

        // Resolve new parent if provided
        let parent_bytes: Option<Vec<u8>> = match new_parent_id {
            Some(pid) => {
                let resolved_parent = self.resolve_tag_id(pid)?;
                let parent_uuid = Uuid::parse_str(&resolved_parent)
                    .map_err(|e| VoiceError::validation("new_parent_id", e.to_string()))?;

                // Prevent circular reference: tag cannot be its own ancestor
                if resolved_parent == resolved_id {
                    return Err(VoiceError::validation("new_parent_id", "A tag cannot be its own parent"));
                }

                // Check if new parent is a descendant of this tag (would create a cycle)
                if self.is_tag_descendant_of(&resolved_parent, &resolved_id)? {
                    return Err(VoiceError::validation(
                        "new_parent_id",
                        "Cannot move tag under its own descendant",
                    ));
                }

                Some(parent_uuid.as_bytes().to_vec())
            }
            None => None,
        };

        let exists: Option<i64> = self
            .conn
            .query_row("SELECT 1 FROM tags WHERE id = ?", params![&tag_bytes], |row| row.get(0))
            .optional()?;
        if exists.is_none() {
            return Ok(false);
        }
        let parent_hex = parent_bytes.as_ref().and_then(|b| uuid_bytes_to_hex(b)).unwrap_or_default();
        self.set_field(ENTITY_TAG, &resolved_id, FIELD_PARENT, &parent_hex, None)?;
        Ok(true)
    }

    /// Check if a tag is a descendant of another tag
    fn is_tag_descendant_of(&self, potential_descendant: &str, potential_ancestor: &str) -> VoiceResult<bool> {
        let descendant_uuid = Uuid::parse_str(potential_descendant)
            .map_err(|e| VoiceError::validation("potential_descendant", e.to_string()))?;
        let ancestor_uuid = Uuid::parse_str(potential_ancestor)
            .map_err(|e| VoiceError::validation("potential_ancestor", e.to_string()))?;
        let descendant_bytes = descendant_uuid.as_bytes().to_vec();
        let ancestor_bytes = ancestor_uuid.as_bytes().to_vec();

        // Walk up the parent chain from descendant to see if we hit ancestor.
        // Bounded: a chain that loops (two offline devices each moving one tag
        // under the other, merged) would otherwise spin here for ever, with
        // the database lock held.
        let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        let mut current_id = descendant_bytes;
        loop {
            if !seen.insert(current_id.clone()) {
                break;
            }
            let parent: Option<Option<Vec<u8>>> = self
                .conn
                .query_row(
                    "SELECT parent_id FROM tags WHERE id = ? AND deleted_at IS NULL",
                    params![&current_id],
                    |row| row.get::<_, Option<Vec<u8>>>(0),
                )
                .optional()?;

            match parent {
                Some(Some(parent_id)) => {
                    if parent_id == ancestor_bytes {
                        return Ok(true);
                    }
                    current_id = parent_id;
                }
                _ => break,
            }
        }

        Ok(false)
    }

    /// Add a tag to a note (accepts ID or ID prefix for both)
    ///
    /// Returns a TagChangeResult indicating what was changed and whether caches were rebuilt.
    pub fn add_tag_to_note(&self, note_id: &str, tag_id: &str) -> VoiceResult<TagChangeResult> {
        // Use try_resolve to return unchanged result if either entity not found
        let resolved_note_id = match self.try_resolve_note_id(note_id)? {
            Some(id) => id,
            None => return Ok(TagChangeResult {
                changed: false,
                note_id: note_id.to_string(),
                list_cache_rebuilt: false,
            }),
        };
        let resolved_tag_id = match self.try_resolve_tag_id(tag_id)? {
            Some(id) => id,
            None => return Ok(TagChangeResult {
                changed: false,
                note_id: resolved_note_id.clone(),
                list_cache_rebuilt: false,
            }),
        };
        let note_uuid = Uuid::parse_str(&resolved_note_id)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?;
        let tag_uuid = Uuid::parse_str(&resolved_tag_id)
            .map_err(|e| VoiceError::validation("tag_id", e.to_string()))?;

        // Already attached?
        let active: Option<Option<i64>> = self
            .conn
            .query_row(
                "SELECT deleted_at FROM note_tags WHERE note_id = ? AND tag_id = ?",
                params![note_uuid.as_bytes().to_vec(), tag_uuid.as_bytes().to_vec()],
                |row| row.get::<_, Option<i64>>(0),
            )
            .optional()?;
        if let Some(None) = active {
            return Ok(TagChangeResult {
                changed: false,
                note_id: resolved_note_id,
                list_cache_rebuilt: false,
            });
        }

        // The membership version writes the row, bumps the note and rebuilds caches.
        let entity_id = note_tag_entity_id(&resolved_note_id, &resolved_tag_id);
        self.init_field(ENTITY_NOTE_TAG, &entity_id, FIELD_ACTIVE, "1")?;

        Ok(TagChangeResult {
            changed: true,
            note_id: resolved_note_id,
            list_cache_rebuilt: true,
        })
    }

    /// Remove a tag from a note (soft delete) (accepts ID or ID prefix for both)
    pub fn remove_tag_from_note(&self, note_id: &str, tag_id: &str) -> VoiceResult<TagChangeResult> {
        // Use try_resolve to return unchanged result if either entity not found
        let resolved_note_id = match self.try_resolve_note_id(note_id)? {
            Some(id) => id,
            None => return Ok(TagChangeResult {
                changed: false,
                note_id: note_id.to_string(),
                list_cache_rebuilt: false,
            }),
        };
        let resolved_tag_id = match self.try_resolve_tag_id(tag_id)? {
            Some(id) => id,
            None => return Ok(TagChangeResult {
                changed: false,
                note_id: resolved_note_id,
                list_cache_rebuilt: false,
            }),
        };
        let note_uuid = Uuid::parse_str(&resolved_note_id)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?;
        let tag_uuid = Uuid::parse_str(&resolved_tag_id)
            .map_err(|e| VoiceError::validation("tag_id", e.to_string()))?;

        let active: Option<Option<i64>> = self
            .conn
            .query_row(
                "SELECT deleted_at FROM note_tags WHERE note_id = ? AND tag_id = ?",
                params![note_uuid.as_bytes().to_vec(), tag_uuid.as_bytes().to_vec()],
                |row| row.get::<_, Option<i64>>(0),
            )
            .optional()?;
        if !matches!(active, Some(None)) {
            return Ok(TagChangeResult {
                changed: false,
                note_id: resolved_note_id,
                list_cache_rebuilt: false,
            });
        }

        let entity_id = note_tag_entity_id(&resolved_note_id, &resolved_tag_id);
        self.set_field(ENTITY_NOTE_TAG, &entity_id, FIELD_ACTIVE, "0", None)?;

        Ok(TagChangeResult {
            changed: true,
            note_id: resolved_note_id,
            list_cache_rebuilt: true,
        })
    }

    /// Get all active tags for a note (accepts ID or ID prefix)
    pub fn get_note_tags(&self, note_id: &str) -> VoiceResult<Vec<TagRow>> {
        // Use try_resolve to return empty Vec if note not found
        let resolved_id = match self.try_resolve_note_id(note_id)? {
            Some(id) => id,
            None => return Ok(Vec::new()),
        };
        let uuid = Uuid::parse_str(&resolved_id)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?;
        let uuid_bytes = uuid.as_bytes().to_vec();

        let mut stmt = self.conn.prepare(
            r#"
            SELECT t.id, t.name, t.parent_id, t.created_at, t.modified_at
            FROM tags t
            INNER JOIN note_tags nt ON t.id = nt.tag_id
            WHERE nt.note_id = ? AND nt.deleted_at IS NULL
            ORDER BY t.name
            "#,
        )?;

        let rows = stmt.query_map([uuid_bytes], |row| self.row_to_tag(row))?;
        let mut tags = Vec::new();
        for tag in rows {
            tags.push(tag?);
        }
        Ok(tags)
    }

    // ============================================================================
    // Note marking (starring/bookmarking) methods
    // ============================================================================

    /// Get the _system tag ID (deterministic UUID, created at init)
    fn get_system_tag_id(&self) -> VoiceResult<Vec<u8>> {
        let uuid = Uuid::parse_str(SYSTEM_TAG_UUID)
            .map_err(|e| VoiceError::Other(format!("Invalid SYSTEM_TAG_UUID: {}", e)))?;
        Ok(uuid.as_bytes().to_vec())
    }

    /// Get the _marked tag ID (deterministic UUID, created at init)
    fn get_marked_tag_id(&self) -> VoiceResult<Vec<u8>> {
        let uuid = Uuid::parse_str(MARKED_TAG_UUID)
            .map_err(|e| VoiceError::Other(format!("Invalid MARKED_TAG_UUID: {}", e)))?;
        Ok(uuid.as_bytes().to_vec())
    }

    /// Check if a note is marked (starred)
    pub fn is_note_marked(&self, note_id: &str) -> VoiceResult<bool> {
        let resolved_note_id = match self.try_resolve_note_id(note_id)? {
            Some(id) => id,
            None => return Ok(false),
        };

        let marked_tag_id = self.get_marked_tag_id()?;

        let note_uuid = Uuid::parse_str(&resolved_note_id)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?;
        let note_bytes = note_uuid.as_bytes().to_vec();

        let is_marked: bool = self.conn.query_row(
            "SELECT COUNT(*) > 0 FROM note_tags WHERE note_id = ? AND tag_id = ? AND deleted_at IS NULL",
            params![&note_bytes, &marked_tag_id],
            |row| row.get(0),
        )?;

        Ok(is_marked)
    }

    /// Mark a note (add the _marked tag)
    pub fn mark_note(&self, note_id: &str) -> VoiceResult<bool> {
        let marked_tag_id = self.get_marked_tag_id()?;
        let marked_tag_hex = crate::validation::uuid_bytes_to_hex(&marked_tag_id)?;
        let result = self.add_tag_to_note(note_id, &marked_tag_hex)?;

        // List cache is already rebuilt by add_tag_to_note
        Ok(result.changed)
    }

    /// Unmark a note (remove the _marked tag)
    pub fn unmark_note(&self, note_id: &str) -> VoiceResult<bool> {
        let marked_tag_id = self.get_marked_tag_id()?;
        let marked_tag_hex = crate::validation::uuid_bytes_to_hex(&marked_tag_id)?;
        let result = self.remove_tag_from_note(note_id, &marked_tag_hex)?;

        // List cache is already rebuilt by remove_tag_from_note
        Ok(result.changed)
    }

    /// Toggle a note's marked state, returns the new state
    pub fn toggle_note_marked(&self, note_id: &str) -> VoiceResult<bool> {
        if self.is_note_marked(note_id)? {
            self.unmark_note(note_id)?;
            Ok(false)
        } else {
            self.mark_note(note_id)?;
            Ok(true)
        }
    }

    /// Get the _system tag ID as hex string (for filtering in UI)
    pub fn get_system_tag_id_hex(&self) -> VoiceResult<String> {
        let bytes = self.get_system_tag_id()?;
        crate::validation::uuid_bytes_to_hex(&bytes)
    }

    // ============================================================================
    // Non-synced file tagging methods
    // ============================================================================

    /// Get the _too-big tag ID (deterministic UUID, created at init)
    fn get_too_big_tag_id(&self) -> VoiceResult<Vec<u8>> {
        let uuid = Uuid::parse_str(TOO_BIG_TAG_UUID)
            .map_err(|e| VoiceError::Other(format!("Invalid TOO_BIG_TAG_UUID: {}", e)))?;
        Ok(uuid.as_bytes().to_vec())
    }

    /// Get the _too-big tag ID as hex string
    pub fn get_too_big_tag_id_hex(&self) -> VoiceResult<String> {
        let bytes = self.get_too_big_tag_id()?;
        crate::validation::uuid_bytes_to_hex(&bytes)
    }

    /// Check if a note is tagged as too-big to sync
    pub fn is_note_too_big_to_sync(&self, note_id: &str) -> VoiceResult<bool> {
        let resolved_note_id = match self.try_resolve_note_id(note_id)? {
            Some(id) => id,
            None => return Ok(false),
        };

        let too_big_tag_id = self.get_too_big_tag_id()?;

        let note_uuid = Uuid::parse_str(&resolved_note_id)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?;
        let note_bytes = note_uuid.as_bytes().to_vec();

        let is_too_big: bool = self.conn.query_row(
            "SELECT COUNT(*) > 0 FROM note_tags WHERE note_id = ? AND tag_id = ? AND deleted_at IS NULL",
            params![&note_bytes, &too_big_tag_id],
            |row| row.get(0),
        )?;

        Ok(is_too_big)
    }

    /// Tag a note as too-big to sync (add the _too-big tag)
    pub fn tag_note_too_big(&self, note_id: &str) -> VoiceResult<bool> {
        let too_big_tag_id = self.get_too_big_tag_id()?;
        let too_big_tag_hex = crate::validation::uuid_bytes_to_hex(&too_big_tag_id)?;
        let result = self.add_tag_to_note(note_id, &too_big_tag_hex)?;
        Ok(result.changed)
    }

    /// Remove the too-big tag from a note
    pub fn untag_note_too_big(&self, note_id: &str) -> VoiceResult<bool> {
        let too_big_tag_id = self.get_too_big_tag_id()?;
        let too_big_tag_hex = crate::validation::uuid_bytes_to_hex(&too_big_tag_id)?;
        let result = self.remove_tag_from_note(note_id, &too_big_tag_hex)?;
        Ok(result.changed)
    }

    /// Start a write transaction for a sync batch (one fsync for thousands
    /// of statements instead of one per statement). Statement failures
    /// inside it roll back only that statement, so partial batches keep
    /// their independent-change semantics.
    pub fn begin_batch(&self) -> VoiceResult<()> {
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        Ok(())
    }

    pub fn commit_batch(&self) -> VoiceResult<()> {
        self.conn.execute_batch("COMMIT")?;
        Ok(())
    }

    pub fn rollback_batch(&self) {
        let _ = self.conn.execute_batch("ROLLBACK");
    }

    /// Close the database connection
    pub fn close(self) -> VoiceResult<()> {
        // Connection is closed when dropped
        Ok(())
    }

    // ============================================================================
    // Sync methods
    // ============================================================================

    /// Get the last sync timestamp for a peer
    pub fn get_peer_last_sync(&self, peer_device_id: &str) -> VoiceResult<Option<i64>> {
        let peer_uuid = Uuid::parse_str(peer_device_id)
            .map_err(|e| VoiceError::validation("peer_device_id", e.to_string()))?;
        let peer_bytes = peer_uuid.as_bytes().to_vec();

        let result: Option<Option<i64>> = self
            .conn
            .query_row(
                "SELECT last_sync_at FROM sync_peers WHERE peer_id = ?",
                params![peer_bytes],
                |row| row.get(0),
            )
            .optional()?;

        Ok(result.flatten())
    }

    /// Update the last sync timestamp for a peer
    pub fn update_peer_sync_time(&self, peer_device_id: &str, peer_name: Option<&str>) -> VoiceResult<()> {
        let peer_uuid = Uuid::parse_str(peer_device_id)
            .map_err(|e| VoiceError::validation("peer_device_id", e.to_string()))?;
        let peer_bytes = peer_uuid.as_bytes().to_vec();

        // Upsert the peer record (peer_url is NOT NULL, so we use empty string as default)
        self.conn.execute(
            r#"
            INSERT INTO sync_peers (peer_id, peer_name, peer_url, last_sync_at)
            VALUES (?, ?, '', strftime('%s', 'now'))
            ON CONFLICT(peer_id) DO UPDATE SET
                peer_name = COALESCE(excluded.peer_name, peer_name),
                last_sync_at = strftime('%s', 'now')
            "#,
            params![peer_bytes, peer_name],
        )?;

        Ok(())
    }

    /// Clear all sync peer records to force a full re-sync
    pub fn clear_sync_peers(&self) -> VoiceResult<()> {
        self.conn.execute("DELETE FROM sync_peers", [])?;
        Ok(())
    }

    /// Reset sync timestamps to NULL to force re-fetching all data
    /// Unlike clear_sync_peers, this preserves peer configuration
    pub fn reset_sync_timestamps(&self) -> VoiceResult<()> {
        self.conn.execute(
            "UPDATE sync_peers SET last_sync_at = NULL, last_received_cursor = NULL, last_sent_seq = NULL",
            [],
        )?;
        Ok(())
    }

    /// Get all changes since a timestamp (for sync)
    /// The `since` parameter is a Unix timestamp (seconds since epoch).
    /// Returns changes where: sync_received_at >= since OR modified_at >= since OR created_at >= since
    /// Each entity type gets its own `limit` (one busy type must not starve the others).
    pub fn get_changes_since(&self, since: Option<i64>, limit: i64) -> VoiceResult<(Vec<HashMap<String, serde_json::Value>>, Option<i64>)> {
        let feed = self.collect_changes(&FeedFilter::Since(since), limit)?;
        Ok((feed.changes, feed.latest_timestamp))
    }

    /// Changes in write order: every row and version whose `seq` is greater
    /// than `cursor` (and at most `upto`, when given), oldest first, at most
    /// `limit` in total. This is the primary feed: exact, resumable, and
    /// independent of clocks. `next_cursor` is the last `seq` returned (or
    /// `cursor` when nothing was); pass it back to continue.
    pub fn get_changes_after_seq(&self, cursor: i64, upto: Option<i64>, limit: i64) -> VoiceResult<ChangeFeed> {
        self.collect_changes(&FeedFilter::AfterSeq { cursor, upto }, limit)
    }

    /// The largest `seq` written so far (0 for an empty database).
    pub fn current_seq(&self) -> VoiceResult<i64> {
        Ok(self
            .conn
            .query_row("SELECT value FROM sync_sequence WHERE id = 1", [], |r| r.get(0))
            .optional()?
            .unwrap_or(0))
    }

    /// Random id of this database, minted when the sequence was created.
    /// A peer that sees a different id knows the database was reset and its
    /// cursors are void.
    pub fn database_id(&self) -> VoiceResult<String> {
        Ok(self
            .conn
            .query_row("SELECT value FROM sync_meta WHERE key = 'database_id'", [], |r| r.get(0))
            .optional()?
            .unwrap_or_default())
    }

    /// The account this database belongs to (ACCT-1): 32 hex characters,
    /// the same on every device of the account, different on every other.
    pub fn account_id(&self) -> VoiceResult<String> {
        Ok(self
            .conn
            .query_row("SELECT value FROM sync_meta WHERE key = 'account_id'", [], |r| r.get(0))
            .optional()?
            .unwrap_or_default())
    }

    fn set_account_id(&self, account_id: &str) -> VoiceResult<()> {
        validate_account_id(account_id)?;
        self.conn.execute(
            "INSERT INTO sync_meta (key, value) VALUES ('account_id', ?1)
             ON CONFLICT(key) DO UPDATE SET value = ?1",
            params![account_id],
        )?;
        Ok(())
    }

    /// Move this database, notes and all, to another account (ACCT-5).
    ///
    /// The deliberate way to merge two accounts, never reached by pairing:
    /// a snapshot is taken first, the account id is rewritten, and every
    /// peer's cursors are forgotten so that the next sync exchanges
    /// everything. The notes stay; their ids cannot collide.
    pub fn move_to_account(&self, account_id: &str) -> VoiceResult<()> {
        validate_account_id(account_id)?;
        self.snapshot_before("account move")?;
        self.set_account_id(account_id)?;
        self.conn.execute("DELETE FROM sync_peers", [])?;
        Ok(())
    }

    /// The account a known peer held at its last handshake, if recorded.
    pub fn get_peer_account_id(&self, peer_device_id: &str) -> VoiceResult<Option<String>> {
        let peer_uuid = Uuid::parse_str(peer_device_id)
            .map_err(|e| VoiceError::validation("peer_device_id", e.to_string()))?;
        Ok(self
            .conn
            .query_row(
                "SELECT peer_account_id FROM sync_peers WHERE peer_id = ?",
                params![peer_uuid.as_bytes().to_vec()],
                |r| r.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten())
    }

    /// Record the account a peer holds, after a handshake that agreed.
    pub fn set_peer_account_id(&self, peer_device_id: &str, peer_name: Option<&str>, account_id: &str) -> VoiceResult<()> {
        let peer_uuid = Uuid::parse_str(peer_device_id)
            .map_err(|e| VoiceError::validation("peer_device_id", e.to_string()))?;
        let peer_bytes = peer_uuid.as_bytes().to_vec();
        self.conn.execute(
            "INSERT OR IGNORE INTO sync_peers (peer_id, peer_name, peer_url) VALUES (?, ?, '')",
            params![peer_bytes, peer_name],
        )?;
        self.conn.execute(
            "UPDATE sync_peers SET peer_account_id = ?, peer_name = COALESCE(?, peer_name) WHERE peer_id = ?",
            params![account_id, peer_name, peer_bytes],
        )?;
        Ok(())
    }

    // ============================================================================
    // Pairing offers (PAIR-2): a token is single-use, lives ten minutes, and
    // dies after five wrong guesses. Only its hash is kept.
    // ============================================================================

    /// Record a freshly shown token. Older offers are dropped: one code at a
    /// time.
    pub fn offer_pairing_token(&self, token_hash: &str, expires_at: i64) -> VoiceResult<()> {
        self.conn.execute("DELETE FROM pairing_offers", [])?;
        self.conn.execute(
            "INSERT INTO pairing_offers (token_hash, expires_at, failures) VALUES (?, ?, 0)",
            params![token_hash, expires_at],
        )?;
        Ok(())
    }

    /// Withdraw every offer, when the code is hidden or spent.
    pub fn withdraw_pairing_offers(&self) -> VoiceResult<()> {
        self.conn.execute("DELETE FROM pairing_offers", [])?;
        Ok(())
    }

    /// Spend the offer whose hash this is, if it is live. A wrong hash
    /// counts against the live offer, and the fifth wrong one withdraws it.
    /// Returns true when the token was accepted and spent.
    pub fn spend_pairing_token(&self, token_hash: &str, now: i64) -> VoiceResult<bool> {
        self.conn.execute("DELETE FROM pairing_offers WHERE expires_at <= ?", params![now])?;
        let live: Option<(String, i64)> = self
            .conn
            .query_row("SELECT token_hash, failures FROM pairing_offers LIMIT 1", [], |r| Ok((r.get(0)?, r.get(1)?)))
            .optional()?;
        let Some((offered, failures)) = live else { return Ok(false) };
        if crate::auth::hashes_agree(&offered, token_hash) {
            self.conn.execute("DELETE FROM pairing_offers", [])?;
            return Ok(true);
        }
        if failures + 1 >= PAIRING_GUESSES_ALLOWED {
            self.conn.execute("DELETE FROM pairing_offers", [])?;
        } else {
            self.conn.execute("UPDATE pairing_offers SET failures = failures + 1", [])?;
        }
        Ok(false)
    }

    /// Whether a code is currently offered (live and not spent).
    pub fn has_pairing_offer(&self, now: i64) -> VoiceResult<bool> {
        let n: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM pairing_offers WHERE expires_at > ?", params![now], |r| r.get(0))?;
        Ok(n > 0)
    }

    // ============================================================================
    // Snapshots (SNAP-1..SNAP-4): a copy of the database before anything
    // irreversible, so that any sync, move or restore can be undone.
    // ============================================================================

    /// The directory snapshots are written to: `snapshots/` beside the
    /// database file. None for an in-memory database.
    pub fn snapshot_directory(&self) -> Option<PathBuf> {
        let path = self.path.as_ref()?;
        Some(path.parent().unwrap_or_else(|| Path::new(".")).join("snapshots"))
    }

    /// Copy the whole database into the snapshot directory with SQLite's
    /// backup API, which gives a consistent copy without blocking readers,
    /// and delete all but the newest [`SNAPSHOTS_KEPT`]. Returns the path.
    pub fn snapshot(&self) -> VoiceResult<PathBuf> {
        let dir = self
            .snapshot_directory()
            .ok_or_else(|| VoiceError::DatabaseOperation("An in-memory database has nowhere to snapshot to".to_string()))?;
        std::fs::create_dir_all(&dir)?;
        let stamp = Utc::now().format("%Y%m%d-%H%M%S%.3f");
        let mut path = dir.join(format!("notes-{}.db", stamp));
        let mut n = 1;
        while path.exists() {
            path = dir.join(format!("notes-{}-{}.db", stamp, n));
            n += 1;
        }
        {
            let mut dst = Connection::open(&path)?;
            let backup = rusqlite::backup::Backup::new(&self.conn, &mut dst)?;
            backup.run_to_completion(1000, std::time::Duration::from_millis(5), None)?;
        }
        for old in self.list_snapshots()?.into_iter().skip(SNAPSHOTS_KEPT) {
            let _ = std::fs::remove_file(&old.path);
        }
        Ok(path)
    }

    /// The periodic backup (SNAP-5): quiesce (the caller holds the database's
    /// lock, so no writer runs; the write-ahead log is checkpointed and
    /// truncated), copy with the backup API into `dir/notes-<time>.db`, and
    /// keep the newest `keep`. Readers are never blocked. Returns the path.
    pub fn backup_to(&self, dir: &Path, keep: usize) -> VoiceResult<PathBuf> {
        if self.path.is_none() {
            return Err(VoiceError::DatabaseOperation("An in-memory database has nothing to back up".to_string()));
        }
        std::fs::create_dir_all(dir)?;
        self.conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        let stamp = Utc::now().format("%Y%m%d-%H%M%S");
        let mut path = dir.join(format!("notes-{}.db", stamp));
        let mut n = 1;
        while path.exists() {
            path = dir.join(format!("notes-{}-{}.db", stamp, n));
            n += 1;
        }
        {
            let mut dst = Connection::open(&path)?;
            let backup = rusqlite::backup::Backup::new(&self.conn, &mut dst)?;
            backup.run_to_completion(1000, std::time::Duration::from_millis(5), None)?;
        }
        let mut copies = Self::backups_in(dir)?;
        copies.sort_by(|a, b| b.cmp(a));
        for old in copies.into_iter().skip(keep.max(1)) {
            let _ = std::fs::remove_file(&old);
        }
        Ok(path)
    }

    /// The backup copies in a directory, newest first.
    pub fn backups_in(dir: &Path) -> VoiceResult<Vec<PathBuf>> {
        let mut copies: Vec<PathBuf> = match std::fs::read_dir(dir) {
            Ok(entries) => entries
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.file_name().and_then(|n| n.to_str()).map(|n| n.starts_with("notes-") && n.ends_with(".db")).unwrap_or(false))
                .collect(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e.into()),
        };
        copies.sort_by(|a, b| b.cmp(a));
        Ok(copies)
    }

    /// Two tags with one path (name under name from the root) become one
    /// (Stage 1, the account move): the older id is kept, the other's notes
    /// and children move to it through the versioned links, and it is
    /// deleted. Returns how many were merged.
    pub fn merge_duplicate_tag_paths(&self) -> VoiceResult<usize> {
        let tags = self.get_all_tags()?;
        let by_id: HashMap<String, TagRow> = tags.iter().map(|t| (t.id.clone(), t.clone())).collect();
        fn path_of(tag: &TagRow, by_id: &HashMap<String, TagRow>) -> String {
            let mut parts = vec![tag.name.clone()];
            let mut parent = tag.parent_id.clone();
            let mut guard = 0;
            while let Some(pid) = parent {
                guard += 1;
                if guard > 64 {
                    break;
                }
                match by_id.get(&pid) {
                    Some(p) => {
                        parts.push(p.name.clone());
                        parent = p.parent_id.clone();
                    }
                    None => break,
                }
            }
            parts.reverse();
            parts.join("/")
        }
        let mut by_path: HashMap<String, Vec<TagRow>> = HashMap::new();
        for tag in &tags {
            by_path.entry(path_of(tag, &by_id)).or_default().push(tag.clone());
        }
        let mut merged = 0;
        // Shallow paths first, so a merged parent's children fold into the kept parent
        let mut groups: Vec<(String, Vec<TagRow>)> = by_path.into_iter().filter(|(_, g)| g.len() > 1).collect();
        groups.sort_by_key(|(path, _)| path.matches('/').count());
        for (_, mut group) in groups {
            group.sort_by(|a, b| a.id.cmp(&b.id));
            let keep = group[0].clone();
            for duplicate in &group[1..] {
                let notes: Vec<Vec<u8>> = {
                    let mut stmt = self.conn.prepare("SELECT note_id FROM note_tags WHERE tag_id = ? AND deleted_at IS NULL")?;
                    let dup = Uuid::parse_str(&duplicate.id).map_err(|e| VoiceError::validation("tag_id", e.to_string()))?;
                    let rows = stmt.query_map(params![dup.as_bytes().to_vec()], |r| r.get::<_, Vec<u8>>(0))?;
                    rows.collect::<rusqlite::Result<Vec<_>>>()?
                };
                for note in notes {
                    let note_hex = uuid_bytes_to_hex(&note).unwrap_or_default();
                    let _ = self.add_tag_to_note(&note_hex, &keep.id);
                    let _ = self.remove_tag_from_note(&note_hex, &duplicate.id);
                }
                for child in tags.iter().filter(|t| t.parent_id.as_deref() == Some(duplicate.id.as_str())) {
                    let _ = self.reparent_tag(&child.id, Some(&keep.id));
                }
                self.delete_tag(&duplicate.id)?;
                merged += 1;
            }
        }
        Ok(merged)
    }

    /// Take a snapshot and say why in the log; an in-memory database is
    /// skipped silently, because there is nothing on disk to lose.
    pub fn snapshot_before(&self, what: &str) -> VoiceResult<()> {
        if self.path.is_none() {
            return Ok(());
        }
        let path = self.snapshot()?;
        tracing::info!("Snapshot before {}: {}", what, path.display());
        Ok(())
    }

    /// Every snapshot beside this database, newest first.
    pub fn list_snapshots(&self) -> VoiceResult<Vec<SnapshotInfo>> {
        let dir = match self.snapshot_directory() {
            Some(d) if d.is_dir() => d,
            _ => return Ok(Vec::new()),
        };
        // Newest first, by the file's own time, so that two snapshots taken
        // within one second still list in the order they were made.
        let mut entries: Vec<(std::time::SystemTime, String)> = std::fs::read_dir(&dir)?
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name().to_string_lossy().to_string();
                if !(name.starts_with("notes-") && name.ends_with(".db")) {
                    return None;
                }
                let modified = e.metadata().and_then(|m| m.modified()).ok()?;
                Some((modified, name))
            })
            .collect();
        entries.sort();
        entries.reverse();
        let mut out = Vec::new();
        for (_, name) in entries {
            let path = dir.join(&name);
            let size_bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            let note_count = Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .and_then(|c| c.query_row("SELECT COUNT(*) FROM notes WHERE deleted_at IS NULL", [], |r| r.get::<_, i64>(0)))
                .unwrap_or(-1);
            out.push(SnapshotInfo { name, path: path.to_string_lossy().to_string(), size_bytes, note_count });
        }
        Ok(out)
    }

    /// Replace this database's contents with a snapshot's (SNAP-4). The
    /// state being replaced is snapshotted first, so a restore is itself
    /// undoable. `name` is a file name from [`Database::list_snapshots`].
    pub fn restore_snapshot(&mut self, name: &str) -> VoiceResult<()> {
        let dir = self
            .snapshot_directory()
            .ok_or_else(|| VoiceError::DatabaseOperation("An in-memory database has no snapshots".to_string()))?;
        if name.contains('/') || name.contains('\\') || !name.starts_with("notes-") || !name.ends_with(".db") {
            return Err(VoiceError::validation("snapshot", format!("{} is not a snapshot name", name)));
        }
        let path = dir.join(name);
        if !path.is_file() {
            return Err(VoiceError::NotFound(format!("No snapshot named {}", name)));
        }
        self.snapshot_before("restore")?;
        let src = Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        let backup = rusqlite::backup::Backup::new(&src, &mut self.conn)?;
        backup.run_to_completion(1000, std::time::Duration::from_millis(5), None)?;
        Ok(())
    }

    /// Per-peer cursor state: (cursor into the peer's feed, our own seq last
    /// pushed to the peer, the peer's database id we last saw).
    pub fn get_peer_cursors(&self, peer_device_id: &str) -> VoiceResult<(i64, i64, Option<String>)> {
        let peer_uuid = Uuid::parse_str(peer_device_id)
            .map_err(|e| VoiceError::validation("peer_device_id", e.to_string()))?;
        let row: Option<(Option<i64>, Option<i64>, Option<String>)> = self
            .conn
            .query_row(
                "SELECT last_received_cursor, last_sent_seq, peer_database_id FROM sync_peers WHERE peer_id = ?",
                params![peer_uuid.as_bytes().to_vec()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        let (c, s, d) = row.unwrap_or((None, None, None));
        Ok((c.unwrap_or(0), s.unwrap_or(0), d))
    }

    /// The entity types a peer declared in its handshake (Stage 16): apply
    /// accepts only those. Empty means every type.
    pub fn set_peer_entity_types(&self, peer_device_id: &str, peer_name: Option<&str>, types: &[String]) -> VoiceResult<()> {
        let peer_uuid = Uuid::parse_str(peer_device_id).map_err(|e| VoiceError::validation("peer_device_id", e.to_string()))?;
        let peer_bytes = peer_uuid.as_bytes().to_vec();
        self.conn.execute(
            "INSERT OR IGNORE INTO sync_peers (peer_id, peer_name, peer_url) VALUES (?, ?, '')",
            params![peer_bytes, peer_name],
        )?;
        self.conn.execute(
            "UPDATE sync_peers SET peer_entity_types = ? WHERE peer_id = ?",
            params![serde_json::to_string(types).unwrap_or_default(), peer_bytes],
        )?;
        Ok(())
    }

    /// The entity types a peer declared, or empty for every type.
    pub fn peer_entity_types(&self, peer_device_id: &str) -> VoiceResult<Vec<String>> {
        let peer_uuid = Uuid::parse_str(peer_device_id).map_err(|e| VoiceError::validation("peer_device_id", e.to_string()))?;
        let text: Option<Option<String>> = self
            .conn
            .query_row("SELECT peer_entity_types FROM sync_peers WHERE peer_id = ?", params![peer_uuid.as_bytes().to_vec()], |r| r.get(0))
            .optional()?;
        Ok(text.flatten().and_then(|t| serde_json::from_str(&t).ok()).unwrap_or_default())
    }

    /// Store a recording's content hash (Stage 13), computed from its file
    /// in the audio directory; the row is published again so it travels.
    /// Returns the hash.
    pub fn store_content_hash(&self, audio_id: &str, audio_dir: &Path) -> VoiceResult<String> {
        let row = self.get_audio_file(audio_id)?.ok_or_else(|| VoiceError::NotFound(audio_id.to_string()))?;
        let path = crate::models::audio_local_path(audio_dir, &row.local_name);
        let hash = crate::transfer::file_sha256(&path)?;
        self.set_content_hash(audio_id, &hash)?;
        Ok(hash)
    }

    /// Write a content hash that is known already.
    pub fn set_content_hash(&self, audio_id: &str, hash: &str) -> VoiceResult<()> {
        let id = Uuid::parse_str(audio_id).map_err(|e| VoiceError::validation("audio_id", e.to_string()))?;
        self.conn.execute(
            "UPDATE audio_files SET content_sha256 = ?, modified_at = COALESCE(modified_at, imported_at) WHERE id = ? AND (content_sha256 IS NULL OR content_sha256 != ?)",
            params![hash, id.as_bytes().to_vec(), hash],
        )?;
        Ok(())
    }

    /// The upload of a recording begun earlier and not finished (Stage 13):
    /// the storage key, the bucket's upload id, the part size, and the parts
    /// already uploaded with their tags. None when no upload is under way.
    pub fn upload_begun(&self, audio_id: &str) -> VoiceResult<Option<UploadBegun>> {
        let id = Uuid::parse_str(audio_id).map_err(|e| VoiceError::validation("audio_id", e.to_string()))?;
        let mut stmt = self.conn.prepare("SELECT storage_key, upload_id, part_size, part_number, etag FROM upload_parts WHERE audio_id = ? ORDER BY part_number")?;
        let rows = stmt.query_map([id.as_bytes().to_vec()], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, i64>(2)?, row.get::<_, i64>(3)?, row.get::<_, String>(4)?))
        })?;
        let mut begun: Option<UploadBegun> = None;
        for row in rows {
            let (storage_key, upload_id, part_size, part_number, etag) = row?;
            let entry = begun.get_or_insert_with(|| UploadBegun { storage_key, upload_id, part_size: part_size as u64, parts: Vec::new() });
            if part_number > 0 {
                entry.parts.push((part_number as u32, etag));
            }
        }
        Ok(begun)
    }

    /// Record that an upload in parts has begun; an earlier one of the same
    /// recording is forgotten (the bucket abandons it by its lifecycle rule).
    pub fn upload_begin(&self, audio_id: &str, storage_key: &str, upload_id: &str, part_size: u64) -> VoiceResult<()> {
        let id = Uuid::parse_str(audio_id).map_err(|e| VoiceError::validation("audio_id", e.to_string()))?;
        let id = id.as_bytes().to_vec();
        self.conn.execute("DELETE FROM upload_parts WHERE audio_id = ?", [&id])?;
        self.conn.execute(
            "INSERT INTO upload_parts (audio_id, storage_key, upload_id, part_size, part_number, etag) VALUES (?, ?, ?, ?, 0, '')",
            params![id, storage_key, upload_id, part_size as i64],
        )?;
        Ok(())
    }

    /// Record one part uploaded, with the tag the bucket gave it.
    pub fn upload_part_done(&self, audio_id: &str, part_number: u32, etag: &str) -> VoiceResult<()> {
        let id = Uuid::parse_str(audio_id).map_err(|e| VoiceError::validation("audio_id", e.to_string()))?;
        let id = id.as_bytes().to_vec();
        let begun: Option<(String, String, i64)> = self.conn.query_row(
            "SELECT storage_key, upload_id, part_size FROM upload_parts WHERE audio_id = ? AND part_number = 0",
            [&id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        ).optional()?;
        let (storage_key, upload_id, part_size) = begun.ok_or_else(|| VoiceError::NotFound(format!("no upload of {} has begun", audio_id)))?;
        self.conn.execute(
            "INSERT OR REPLACE INTO upload_parts (audio_id, storage_key, upload_id, part_size, part_number, etag) VALUES (?, ?, ?, ?, ?, ?)",
            params![id, storage_key, upload_id, part_size, part_number as i64, etag],
        )?;
        Ok(())
    }

    /// Forget the parts of a recording's upload: it is complete, or abandoned.
    pub fn upload_finished(&self, audio_id: &str) -> VoiceResult<()> {
        let id = Uuid::parse_str(audio_id).map_err(|e| VoiceError::validation("audio_id", e.to_string()))?;
        self.conn.execute("DELETE FROM upload_parts WHERE audio_id = ?", [id.as_bytes().to_vec()])?;
        Ok(())
    }

    /// Whether new uploads are encrypted (Stage 15, ENC-3): `encrypt` in the
    /// synced storage configuration, so every device of the account agrees.
    pub fn encryption_on(&self) -> VoiceResult<bool> {
        Ok(self.get_file_storage_config()?
            .and_then(|v| v.get("config").cloned())
            .and_then(|c| match c { serde_json::Value::String(text) => serde_json::from_str(&text).ok(), other => Some(other) })
            .and_then(|c: serde_json::Value| c.get("encrypt").and_then(|e| e.as_bool()))
            .unwrap_or(false))
    }

    /// Turn encryption of new uploads on or off, in the synced storage configuration.
    pub fn set_encryption_on(&self, on: bool) -> VoiceResult<()> {
        let saved = self.get_file_storage_config()?.ok_or_else(|| VoiceError::Config("No bucket is configured yet".to_string()))?;
        let provider = saved.get("provider").and_then(|p| p.as_str()).unwrap_or("none").to_string();
        let mut config = saved.get("config").cloned()
            .and_then(|c| match c { serde_json::Value::String(text) => serde_json::from_str(&text).ok(), other => Some(other) })
            .unwrap_or_else(|| serde_json::json!({}));
        if let Some(map) = config.as_object_mut() {
            map.insert("encrypt".to_string(), serde_json::Value::Bool(on));
        }
        self.set_file_storage_config(&provider, Some(&config))
    }

    /// Remember a bucket object to tag purged at the next upload run (Stage 14).
    pub fn remember_purged_object(&self, storage_key: &str) -> VoiceResult<()> {
        self.conn.execute("INSERT OR IGNORE INTO purged_objects (storage_key, at) VALUES (?, ?)", params![storage_key, Utc::now().timestamp()])?;
        Ok(())
    }

    /// The bucket objects of purged recordings not yet tagged (Stage 14).
    pub fn purged_objects(&self) -> VoiceResult<Vec<String>> {
        let mut stmt = self.conn.prepare("SELECT storage_key FROM purged_objects ORDER BY at")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// A purged object was tagged in the bucket; forget it.
    pub fn forget_purged_object(&self, storage_key: &str) -> VoiceResult<()> {
        self.conn.execute("DELETE FROM purged_objects WHERE storage_key = ?", params![storage_key])?;
        Ok(())
    }

    /// A peer holds a copy of a recording (Stage 10).
    pub fn record_copy(&self, audio_id: &str, peer_id: &str) -> VoiceResult<()> {
        let audio = Uuid::parse_str(audio_id).map_err(|e| VoiceError::validation("audio_id", e.to_string()))?;
        let peer = Uuid::parse_str(peer_id).map_err(|e| VoiceError::validation("peer_id", e.to_string()))?;
        self.conn.execute(
            "INSERT INTO audio_file_copies (audio_id, peer_id, at) VALUES (?1, ?2, ?3)
             ON CONFLICT(audio_id, peer_id) DO UPDATE SET at = ?3",
            params![audio.as_bytes().to_vec(), peer.as_bytes().to_vec(), Utc::now().timestamp()],
        )?;
        Ok(())
    }

    /// The peers known to hold a copy of a recording, and when that was
    /// learnt; the bucket is `storage_key` on the row, this device the file.
    pub fn copies_of(&self, audio_id: &str) -> VoiceResult<Vec<CopyRow>> {
        let audio = Uuid::parse_str(audio_id).map_err(|e| VoiceError::validation("audio_id", e.to_string()))?;
        let mut stmt = self.conn.prepare("SELECT peer_id, at FROM audio_file_copies WHERE audio_id = ? ORDER BY at")?;
        let rows = stmt.query_map(params![audio.as_bytes().to_vec()], |r| {
            let peer: Vec<u8> = r.get(0)?;
            Ok(CopyRow { peer_id: Uuid::from_slice(&peer).map(|u| u.simple().to_string()).unwrap_or_default(), at: r.get(1)? })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// What is on this device only (Stage 10): notes whose head version was
    /// written here and never sent to any peer, and recordings whose file
    /// is here, not in the bucket, and on no peer that this device knows of.
    /// A note's head content version is here only when it was not received
    /// by sync, it is either authored (it has a device) or a root made for a
    /// note row that was itself written here, and its `seq` is above every
    /// peer's sent cursor. `audio_dir` is where the files are, and without
    /// it no recording counts.
    pub fn not_duplicated(&self, audio_dir: Option<&Path>) -> VoiceResult<NotDuplicated> {
        let max_sent: i64 = self
            .conn
            .query_row("SELECT COALESCE(MAX(last_sent_seq), 0) FROM sync_peers", [], |r| r.get(0))?;
        let notes: i64 = self.conn.query_row(
            r#"SELECT COUNT(*) FROM notes n
               JOIN field_heads h ON h.entity_type = 'note' AND h.entity_id = lower(hex(n.id)) AND h.field = 'content'
               JOIN field_versions v ON v.id = h.head_id
               WHERE n.deleted_at IS NULL
                 AND v.sync_received_at IS NULL
                 AND (v.device_id IS NOT NULL OR n.sync_received_at IS NULL)
                 AND COALESCE(v.seq, 0) > ?1"#,
            params![max_sent],
            |r| r.get(0),
        )?;
        let mut recordings = 0i64;
        if let Some(dir) = audio_dir {
            let mut stmt = self.conn.prepare(
                r#"SELECT COALESCE(a.local_name, '') FROM audio_files a
                   WHERE a.deleted_at IS NULL AND a.storage_key IS NULL
                     AND NOT EXISTS (SELECT 1 FROM audio_file_copies c WHERE c.audio_id = a.id)"#,
            )?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            for row in rows {
                let local_name = row?;
                if !local_name.is_empty() && crate::models::audio_local_path(dir, &local_name).is_file() {
                    recordings += 1;
                }
            }
        }
        Ok(NotDuplicated { notes, recordings })
    }

    /// Record that an operation with a peer ran now (Stage 10): the peer's
    /// row gets the time and the operation's name.
    pub fn set_peer_last_operation(&self, peer_device_id: &str, peer_name: Option<&str>, peer_url: Option<&str>, operation: &str) -> VoiceResult<()> {
        let peer_uuid = Uuid::parse_str(peer_device_id).map_err(|e| VoiceError::validation("peer_device_id", e.to_string()))?;
        let peer_bytes = peer_uuid.as_bytes().to_vec();
        self.conn.execute(
            "INSERT OR IGNORE INTO sync_peers (peer_id, peer_name, peer_url) VALUES (?, ?, ?)",
            params![peer_bytes, peer_name, peer_url.unwrap_or("")],
        )?;
        self.conn.execute(
            "UPDATE sync_peers SET last_sync_at = ?, last_operation = ?, peer_name = COALESCE(?, peer_name) WHERE peer_id = ?",
            params![Utc::now().timestamp(), operation, peer_name, peer_bytes],
        )?;
        Ok(())
    }

    /// Every peer this device has dealt with: when it was last reached and
    /// what the last operation was (Stage 10).
    pub fn peer_summaries(&self) -> VoiceResult<Vec<PeerSummary>> {
        let mut stmt = self.conn.prepare("SELECT peer_id, peer_name, last_sync_at, last_operation FROM sync_peers ORDER BY last_sync_at DESC")?;
        let rows = stmt.query_map([], |r| {
            let peer: Vec<u8> = r.get(0)?;
            Ok(PeerSummary {
                peer_id: Uuid::from_slice(&peer).map(|u| u.simple().to_string()).unwrap_or_default(),
                peer_name: r.get(1)?,
                last_reached_at: r.get(2)?,
                last_operation: r.get(3)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Store cursor state for a peer (any `None` leaves that value alone).
    pub fn set_peer_cursors(
        &self,
        peer_device_id: &str,
        peer_name: Option<&str>,
        received_cursor: Option<i64>,
        sent_seq: Option<i64>,
        peer_database_id: Option<&str>,
    ) -> VoiceResult<()> {
        let peer_uuid = Uuid::parse_str(peer_device_id)
            .map_err(|e| VoiceError::validation("peer_device_id", e.to_string()))?;
        let peer_bytes = peer_uuid.as_bytes().to_vec();
        self.conn.execute(
            "INSERT OR IGNORE INTO sync_peers (peer_id, peer_name, peer_url) VALUES (?, ?, '')",
            params![peer_bytes, peer_name],
        )?;
        self.conn.execute(
            r#"UPDATE sync_peers SET
                 last_received_cursor = COALESCE(?, last_received_cursor),
                 last_sent_seq = COALESCE(?, last_sent_seq),
                 peer_database_id = COALESCE(?, peer_database_id),
                 peer_name = COALESCE(?, peer_name)
               WHERE peer_id = ?"#,
            params![received_cursor, sent_seq, peer_database_id, peer_name, peer_bytes],
        )?;
        Ok(())
    }

    /// Build the feed. `Since` keeps the historical per-type timestamp
    /// filter (used by the `since` query parameter and by tools); `AfterSeq`
    /// is the cursor feed used by the sync client. The cursor feed reads the
    /// sequence in small ranges so memory stays bounded by one page even when
    /// every change is a long transcription.
    fn collect_changes(&self, filter: &FeedFilter, limit: i64) -> VoiceResult<ChangeFeed> {
        let mut out = ChangeFeed { changes: Vec::new(), latest_timestamp: None, next_cursor: 0, is_complete: true };
        match filter {
            FeedFilter::Since(_) => {
                let (items, saturated) = self.collect_items(filter, limit)?;
                // Historical behaviour: per-type order, per-type limits.
                for (_, timestamp, c) in items {
                    if out.latest_timestamp.map_or(true, |t| timestamp > t) {
                        out.latest_timestamp = Some(timestamp);
                    }
                    out.changes.push(c);
                }
                out.is_complete = !saturated;
            }
            FeedFilter::AfterSeq { cursor, upto } => {
                out.next_cursor = *cursor;
                let end = upto.unwrap_or(i64::MAX).min(self.current_seq()?);
                const CHUNK: i64 = 512;
                let mut items: Vec<(i64, i64, HashMap<String, serde_json::Value>)> = Vec::new();
                let mut bytes = 0usize;
                let mut lo = *cursor;
                let mut truncated = false;
                'chunks: while lo < end && (items.len() as i64) < limit {
                    let hi = lo.saturating_add(CHUNK).min(end);
                    let (mut chunk, _) = self.collect_items(&FeedFilter::AfterSeq { cursor: lo, upto: Some(hi) }, CHUNK)?;
                    chunk.sort_by_key(|(seq, _, _)| *seq);
                    for item in chunk {
                        // A page is bounded in bytes as well as in count, so that
                        // thousands of long transcriptions never produce a body
                        // that exceeds the peer's limit or its timeout. At least
                        // one change always goes out, and the cursor stays exact.
                        let size = serde_json::to_string(&item.2).map(|s| s.len()).unwrap_or(0);
                        if !items.is_empty() && (bytes + size > FEED_BYTE_BUDGET || items.len() as i64 >= limit) {
                            truncated = true;
                            break 'chunks;
                        }
                        bytes += size;
                        items.push(item);
                    }
                    lo = hi;
                }
                out.is_complete = !truncated && lo >= end;
                out.next_cursor = items.last().map(|(seq, _, _)| *seq).unwrap_or(*cursor);
                if !truncated && lo >= end && items.is_empty() {
                    // Nothing after the cursor: report the end so the caller
                    // does not re-read empty ranges forever
                    out.next_cursor = (*cursor).max(end.min(*cursor));
                }
                for (_, timestamp, c) in items {
                    if out.latest_timestamp.map_or(true, |t| timestamp > t) {
                        out.latest_timestamp = Some(timestamp);
                    }
                    out.changes.push(c);
                }
            }
        }
        Ok(out)
    }

    /// Run the per-type feed queries for one filter; returns (seq, timestamp,
    /// change) items and whether any type hit `limit`.
    fn collect_items(&self, filter: &FeedFilter, limit: i64) -> VoiceResult<(Vec<(i64, i64, HashMap<String, serde_json::Value>)>, bool)> {
        // (seq, timestamp, change)
        let mut items: Vec<(i64, i64, HashMap<String, serde_json::Value>)> = Vec::new();
        let mut saturated = false;

        let ts_val = |v: Option<i64>| v.map_or(serde_json::Value::Null, |t| serde_json::Value::Number(t.into()));
        let str_val = |v: Option<String>| v.map_or(serde_json::Value::Null, serde_json::Value::String);
        let op = |modified_at: Option<i64>, deleted_at: Option<i64>| {
            if deleted_at.is_some() { "delete" } else if modified_at.is_some() { "update" } else { "create" }
        };
        /// Read `count` (offset, zone) pairs starting at column `at`: the
        /// timezone each of the row's timestamps was written in.
        fn read_zone_pairs(row: &rusqlite::Row, at: usize, count: usize) -> rusqlite::Result<Vec<(Option<i32>, Option<String>)>> {
            let mut pairs = Vec::with_capacity(count);
            for i in 0..count {
                pairs.push((
                    row.get::<_, Option<i64>>(at + i * 2)?.map(|v| v as i32),
                    row.get(at + i * 2 + 1)?,
                ));
            }
            Ok(pairs)
        }

        /// Put those pairs into the payload beside the timestamps they belong to.
        fn insert_zone_pairs(data: &mut serde_json::Map<String, serde_json::Value>, stamps: &[&str], pairs: &[(Option<i32>, Option<String>)]) {
            for (stamp, (offset, zone)) in stamps.iter().zip(pairs.iter()) {
                data.insert(
                    format!("{}_offset", stamp),
                    offset.map_or(serde_json::Value::Null, |o| serde_json::Value::Number(o.into())),
                );
                data.insert(
                    format!("{}_zone", stamp),
                    zone.clone().map_or(serde_json::Value::Null, serde_json::Value::String),
                );
            }
        }

        fn change(entity_type: &str, entity_id: String, operation: &str, timestamp: i64, seq: i64, data: serde_json::Map<String, serde_json::Value>) -> HashMap<String, serde_json::Value> {
            let mut c = HashMap::new();
            c.insert("entity_type".to_string(), serde_json::Value::String(entity_type.to_string()));
            c.insert("entity_id".to_string(), serde_json::Value::String(entity_id));
            c.insert("operation".to_string(), serde_json::Value::String(operation.to_string()));
            c.insert("timestamp".to_string(), serde_json::Value::Number(timestamp.into()));
            c.insert("seq".to_string(), serde_json::Value::Number(seq.into()));
            c.insert("data".to_string(), serde_json::Value::Object(data));
            c
        }

        // Notes
        {
            let rows: Vec<(Vec<u8>, i64, String, Option<i64>, Option<i64>, i64, Vec<(Option<i32>, Option<String>)>, Option<Vec<u8>>)> = self.feed_query(
                "id, created_at, content, modified_at, deleted_at, created_at_offset, created_at_zone, modified_at_offset, modified_at_zone, deleted_at_offset, deleted_at_zone, primary_attachment_id", "notes",
                "sync_received_at >= ?1 OR modified_at >= ?1 OR created_at >= ?1",
                "COALESCE(sync_received_at, modified_at, created_at)", "COALESCE(modified_at, created_at)",
                filter, limit,
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get::<_, Option<i64>>(12)?.unwrap_or(0), read_zone_pairs(row, 5, 3)?, row.get(11)?)),
            )?;
            saturated |= rows.len() as i64 >= limit;
            for (id_bytes, created_at, content, modified_at, deleted_at, seq, zones, primary_attachment) in rows {
                let timestamp = modified_at.unwrap_or(created_at);
                let id_hex = uuid_bytes_to_hex(&id_bytes).unwrap_or_default();
                let mut data = serde_json::Map::new();
                data.insert("id".to_string(), serde_json::Value::String(id_hex.clone()));
                data.insert("created_at".to_string(), serde_json::Value::Number(created_at.into()));
                data.insert("content".to_string(), serde_json::Value::String(content));
                data.insert("modified_at".to_string(), ts_val(modified_at));
                data.insert("deleted_at".to_string(), ts_val(deleted_at));
                data.insert(
                    "primary_attachment_id".to_string(),
                    str_val(primary_attachment.and_then(|b| uuid_bytes_to_hex(&b))),
                );
                insert_zone_pairs(&mut data, &["created_at", "modified_at", "deleted_at"], &zones);
                items.push((seq, timestamp, change("note", id_hex, op(modified_at, deleted_at), timestamp, seq, data)));
            }
        }

        // Tags
        {
            let rows: Vec<(Vec<u8>, String, Option<Vec<u8>>, i64, Option<i64>, Option<i64>, i64, Vec<(Option<i32>, Option<String>)>)> = self.feed_query(
                "id, name, parent_id, created_at, modified_at, deleted_at, created_at_offset, created_at_zone, modified_at_offset, modified_at_zone, deleted_at_offset, deleted_at_zone", "tags",
                "sync_received_at >= ?1 OR modified_at >= ?1 OR created_at >= ?1",
                "COALESCE(sync_received_at, modified_at, created_at)", "COALESCE(modified_at, created_at)",
                filter, limit,
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get::<_, Option<i64>>(12)?.unwrap_or(0), read_zone_pairs(row, 6, 3)?)),
            )?;
            saturated |= rows.len() as i64 >= limit;
            for (id_bytes, name, parent_bytes, created_at, modified_at, deleted_at, seq, zones) in rows {
                let timestamp = modified_at.unwrap_or(created_at);
                let id_hex = uuid_bytes_to_hex(&id_bytes).unwrap_or_default();
                let mut data = serde_json::Map::new();
                data.insert("id".to_string(), serde_json::Value::String(id_hex.clone()));
                data.insert("name".to_string(), serde_json::Value::String(name));
                data.insert("parent_id".to_string(), str_val(parent_bytes.and_then(|b| uuid_bytes_to_hex(&b))));
                data.insert("created_at".to_string(), serde_json::Value::Number(created_at.into()));
                data.insert("modified_at".to_string(), ts_val(modified_at));
                data.insert("deleted_at".to_string(), ts_val(deleted_at));
                insert_zone_pairs(&mut data, &["created_at", "modified_at", "deleted_at"], &zones);
                items.push((seq, timestamp, change("tag", id_hex, op(modified_at, deleted_at), timestamp, seq, data)));
            }
        }

        // Note-tag links
        {
            let rows: Vec<(Vec<u8>, Vec<u8>, i64, Option<i64>, Option<i64>, i64, Vec<(Option<i32>, Option<String>)>)> = self.feed_query(
                "note_id, tag_id, created_at, modified_at, deleted_at, created_at_offset, created_at_zone, modified_at_offset, modified_at_zone, deleted_at_offset, deleted_at_zone", "note_tags",
                "sync_received_at >= ?1 OR modified_at >= ?1 OR deleted_at >= ?1 OR created_at >= ?1",
                "COALESCE(sync_received_at, modified_at, deleted_at, created_at)", "COALESCE(modified_at, deleted_at, created_at)",
                filter, limit,
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get::<_, Option<i64>>(11)?.unwrap_or(0), read_zone_pairs(row, 5, 3)?)),
            )?;
            saturated |= rows.len() as i64 >= limit;
            for (note_bytes, tag_bytes, created_at, modified_at, deleted_at, seq, zones) in rows {
                let timestamp = modified_at.or(deleted_at).unwrap_or(created_at);
                let note_hex = uuid_bytes_to_hex(&note_bytes).unwrap_or_default();
                let tag_hex = uuid_bytes_to_hex(&tag_bytes).unwrap_or_default();
                let mut data = serde_json::Map::new();
                data.insert("note_id".to_string(), serde_json::Value::String(note_hex.clone()));
                data.insert("tag_id".to_string(), serde_json::Value::String(tag_hex.clone()));
                data.insert("created_at".to_string(), serde_json::Value::Number(created_at.into()));
                data.insert("modified_at".to_string(), ts_val(modified_at));
                data.insert("deleted_at".to_string(), ts_val(deleted_at));
                insert_zone_pairs(&mut data, &["created_at", "modified_at", "deleted_at"], &zones);
                items.push((seq, timestamp, change("note_tag", format!("{}:{}", note_hex, tag_hex), op(modified_at, deleted_at), timestamp, seq, data)));
            }
        }

        // Audio files
        {
            type AudioRow = (Vec<u8>, i64, String, Option<i64>, Option<String>, Option<i64>, Option<i64>, Option<String>, Option<String>, Option<i64>, i64, Vec<(Option<i32>, Option<String>)>, Option<Vec<u8>>, Option<String>, i64);
            let rows: Vec<AudioRow> = self.feed_query(
                "id, imported_at, filename, file_created_at, summary, modified_at, deleted_at, storage_provider, storage_key, storage_uploaded_at, imported_at_offset, imported_at_zone, file_created_at_offset, file_created_at_zone, modified_at_offset, modified_at_zone, deleted_at_offset, deleted_at_zone, primary_transcription_id, content_sha256, storage_encrypted", "audio_files",
                "sync_received_at >= ?1 OR modified_at >= ?1 OR imported_at >= ?1",
                "COALESCE(sync_received_at, modified_at, imported_at)", "COALESCE(modified_at, imported_at)",
                filter, limit,
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?, row.get(7)?, row.get(8)?, row.get(9)?, row.get::<_, Option<i64>>(21)?.unwrap_or(0), read_zone_pairs(row, 10, 4)?, row.get(18)?, row.get(19)?, row.get::<_, Option<i64>>(20)?.unwrap_or(0))),
            )?;
            saturated |= rows.len() as i64 >= limit;
            for (id_bytes, imported_at, filename, file_created_at, summary, modified_at, deleted_at, storage_provider, storage_key, storage_uploaded_at, seq, zones, primary_transcription, content_sha256, storage_encrypted) in rows {
                let timestamp = modified_at.unwrap_or(imported_at);
                let id_hex = uuid_bytes_to_hex(&id_bytes).unwrap_or_default();
                let mut data = serde_json::Map::new();
                data.insert("id".to_string(), serde_json::Value::String(id_hex.clone()));
                data.insert("imported_at".to_string(), serde_json::Value::Number(imported_at.into()));
                data.insert("filename".to_string(), serde_json::Value::String(filename));
                data.insert("file_created_at".to_string(), ts_val(file_created_at));
                data.insert("summary".to_string(), str_val(summary));
                data.insert("modified_at".to_string(), ts_val(modified_at));
                data.insert("deleted_at".to_string(), ts_val(deleted_at));
                data.insert("storage_provider".to_string(), str_val(storage_provider));
                data.insert("storage_key".to_string(), str_val(storage_key));
                data.insert("storage_uploaded_at".to_string(), ts_val(storage_uploaded_at));
                data.insert("content_sha256".to_string(), str_val(content_sha256));
                data.insert("storage_encrypted".to_string(), serde_json::Value::Bool(storage_encrypted != 0));
                data.insert(
                    "primary_transcription_id".to_string(),
                    str_val(primary_transcription.and_then(|b| uuid_bytes_to_hex(&b))),
                );
                insert_zone_pairs(&mut data, &["imported_at", "file_created_at", "modified_at", "deleted_at"], &zones);
                items.push((seq, timestamp, change("audio_file", id_hex, op(modified_at, deleted_at), timestamp, seq, data)));
            }
        }

        // Attachments
        {
            let rows: Vec<(Vec<u8>, Vec<u8>, Vec<u8>, String, i64, Option<i64>, Option<i64>, i64, Vec<(Option<i32>, Option<String>)>)> = self.feed_query(
                "id, note_id, attachment_id, attachment_type, created_at, modified_at, deleted_at, created_at_offset, created_at_zone, modified_at_offset, modified_at_zone, deleted_at_offset, deleted_at_zone", "note_attachments",
                "sync_received_at >= ?1 OR modified_at >= ?1 OR deleted_at >= ?1 OR created_at >= ?1",
                "COALESCE(sync_received_at, modified_at, deleted_at, created_at)", "COALESCE(modified_at, deleted_at, created_at)",
                filter, limit,
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?, row.get::<_, Option<i64>>(13)?.unwrap_or(0), read_zone_pairs(row, 7, 3)?)),
            )?;
            saturated |= rows.len() as i64 >= limit;
            for (id_bytes, note_bytes, att_bytes, attachment_type, created_at, modified_at, deleted_at, seq, zones) in rows {
                let timestamp = modified_at.or(deleted_at).unwrap_or(created_at);
                let id_hex = uuid_bytes_to_hex(&id_bytes).unwrap_or_default();
                let mut data = serde_json::Map::new();
                data.insert("id".to_string(), serde_json::Value::String(id_hex.clone()));
                data.insert("note_id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&note_bytes).unwrap_or_default()));
                data.insert("attachment_id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&att_bytes).unwrap_or_default()));
                data.insert("attachment_type".to_string(), serde_json::Value::String(attachment_type));
                data.insert("created_at".to_string(), serde_json::Value::Number(created_at.into()));
                data.insert("modified_at".to_string(), ts_val(modified_at));
                data.insert("deleted_at".to_string(), ts_val(deleted_at));
                insert_zone_pairs(&mut data, &["created_at", "modified_at", "deleted_at"], &zones);
                items.push((seq, timestamp, change("note_attachment", id_hex, op(modified_at, deleted_at), timestamp, seq, data)));
            }
        }

        // Transcriptions
        {
            type TrRow = (Vec<u8>, Vec<u8>, String, Option<String>, String, Option<String>, Option<String>, String, Vec<u8>, i64, Option<i64>, Option<i64>, i64, Vec<(Option<i32>, Option<String>)>);
            let rows: Vec<TrRow> = self.feed_query(
                "id, audio_file_id, content, content_segments, service, service_arguments, service_response, state, device_id, created_at, modified_at, deleted_at, created_at_offset, created_at_zone, modified_at_offset, modified_at_zone, deleted_at_offset, deleted_at_zone", "transcriptions",
                "sync_received_at >= ?1 OR modified_at >= ?1 OR created_at >= ?1",
                "COALESCE(sync_received_at, modified_at, created_at)", "COALESCE(modified_at, created_at)",
                filter, limit,
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?, row.get(7)?, row.get(8)?, row.get(9)?, row.get(10)?, row.get(11)?, row.get::<_, Option<i64>>(18)?.unwrap_or(0), read_zone_pairs(row, 12, 3)?)),
            )?;
            saturated |= rows.len() as i64 >= limit;
            for (id_bytes, audio_bytes, content, content_segments, service, service_arguments, service_response, state, device_bytes, created_at, modified_at, deleted_at, seq, zones) in rows {
                let timestamp = modified_at.unwrap_or(created_at);
                let id_hex = uuid_bytes_to_hex(&id_bytes).unwrap_or_default();
                let mut data = serde_json::Map::new();
                data.insert("id".to_string(), serde_json::Value::String(id_hex.clone()));
                data.insert("audio_file_id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&audio_bytes).unwrap_or_default()));
                data.insert("content".to_string(), serde_json::Value::String(content));
                data.insert("content_segments".to_string(), str_val(content_segments));
                data.insert("service".to_string(), serde_json::Value::String(service));
                data.insert("service_arguments".to_string(), str_val(service_arguments));
                data.insert("service_response".to_string(), str_val(service_response));
                data.insert("state".to_string(), serde_json::Value::String(state));
                data.insert("device_id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&device_bytes).unwrap_or_default()));
                data.insert("created_at".to_string(), serde_json::Value::Number(created_at.into()));
                data.insert("modified_at".to_string(), ts_val(modified_at));
                data.insert("deleted_at".to_string(), ts_val(deleted_at));
                insert_zone_pairs(&mut data, &["created_at", "modified_at", "deleted_at"], &zones);
                items.push((seq, timestamp, change("transcription", id_hex, op(modified_at, deleted_at), timestamp, seq, data)));
            }
        }

        // Cloud storage configuration (single row)
        {
            let rows: Vec<(String, Option<String>, Option<i64>, Option<Vec<u8>>, i64)> = self.feed_query(
                "provider, config, modified_at, device_id", "file_storage_config",
                "id = 'default' AND (sync_received_at >= ?1 OR modified_at >= ?1)",
                "modified_at", "modified_at",
                filter, limit,
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get::<_, Option<i64>>(4)?.unwrap_or(0))),
            )?;
            for (provider, config, modified_at, device_bytes, seq) in rows {
                let timestamp = modified_at.unwrap_or(0);
                let device_hex = device_bytes.and_then(|b| uuid_bytes_to_hex(&b));
                let mut data = serde_json::Map::new();
                data.insert("id".to_string(), serde_json::Value::String("default".to_string()));
                data.insert("provider".to_string(), serde_json::Value::String(provider));
                data.insert("config".to_string(), config.map_or(serde_json::Value::Null, |s| serde_json::from_str(&s).unwrap_or(serde_json::Value::Null)));
                data.insert("modified_at".to_string(), ts_val(modified_at));
                if let Some(d) = &device_hex {
                    data.insert("device_id".to_string(), serde_json::Value::String(d.clone()));
                }
                let mut c = change("file_storage_config", "default".to_string(), "update", timestamp, seq, data);
                if let Some(d) = device_hex {
                    c.insert("device_id".to_string(), serde_json::Value::String(d));
                }
                items.push((seq, timestamp, c));
            }
        }

        // Field versions: immutable history entries, one per edit. Applied
        // before entity rows on the receiving side (see apply order).
        {
            let versions = match filter {
                FeedFilter::Since(since) => self.get_versions_since(*since, limit)?.into_iter().map(|v| (0i64, v)).collect::<Vec<_>>(),
                FeedFilter::AfterSeq { cursor, upto } => self.get_versions_after_seq(*cursor, *upto, limit)?,
            };
            saturated |= versions.len() as i64 >= limit;
            for (seq, v) in versions {
                let timestamp = v.created_at;
                let mut c = HashMap::new();
                c.insert("entity_type".to_string(), serde_json::Value::String("field_version".to_string()));
                c.insert("entity_id".to_string(), serde_json::Value::String(v.id_hex()));
                c.insert("operation".to_string(), serde_json::Value::String("create".to_string()));
                c.insert("timestamp".to_string(), serde_json::Value::Number(timestamp.into()));
                c.insert("seq".to_string(), serde_json::Value::Number(seq.into()));
                c.insert("data".to_string(), v.to_json());
                items.push((seq, timestamp, c));
            }
        }

        // Purges: entities removed for good. They carry no data of their
        // own beyond what was removed and when, and a receiver that has
        // never heard of them ignores them (PROTO-2).
        {
            let rows: Vec<(String, Vec<u8>, i64, Option<i32>, Option<String>, i64)> = self.feed_query(
                "entity_type, entity_id, purged_at, purged_at_offset, purged_at_zone", "purges",
                "purged_at >= ?1", "purged_at", "purged_at",
                filter, limit,
                |row| Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get::<_, Option<i64>>(3)?.map(|v| v as i32),
                    row.get(4)?,
                    row.get::<_, Option<i64>>(5)?.unwrap_or(0),
                )),
            )?;
            saturated |= rows.len() as i64 >= limit;
            for (entity_type, id_bytes, purged_at, offset, zone, seq) in rows {
                let id_hex = uuid_bytes_to_hex(&id_bytes).unwrap_or_default();
                let mut data = serde_json::Map::new();
                data.insert("entity_type".to_string(), serde_json::Value::String(entity_type));
                data.insert("entity_id".to_string(), serde_json::Value::String(id_hex.clone()));
                data.insert("purged_at".to_string(), serde_json::Value::Number(purged_at.into()));
                data.insert(
                    "purged_at_offset".to_string(),
                    offset.map_or(serde_json::Value::Null, |o| serde_json::Value::Number(o.into())),
                );
                data.insert(
                    "purged_at_zone".to_string(),
                    zone.map_or(serde_json::Value::Null, serde_json::Value::String),
                );
                items.push((seq, purged_at, change("purge", id_hex, "delete", purged_at, seq, data)));
            }
        }

        Ok((items, saturated))
    }

    /// One feed query: `cols` (plus `seq` appended) from `table`, filtered
    /// and ordered according to `filter`.
    fn feed_query<T, F>(
        &self,
        cols: &str,
        table: &str,
        since_where: &str,
        since_order: &str,
        all_order: &str,
        filter: &FeedFilter,
        limit: i64,
        map: F,
    ) -> VoiceResult<Vec<T>>
    where
        F: Fn(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        let (sql, values): (String, Vec<rusqlite::types::Value>) = match filter {
            FeedFilter::Since(Some(ts)) => (
                format!("SELECT {}, seq FROM {} WHERE {} ORDER BY {} LIMIT ?2", cols, table, since_where, since_order),
                vec![(*ts).into(), limit.into()],
            ),
            FeedFilter::Since(None) => (
                format!("SELECT {}, seq FROM {} ORDER BY {} LIMIT ?1", cols, table, all_order),
                vec![limit.into()],
            ),
            FeedFilter::AfterSeq { cursor, upto } => (
                format!("SELECT {}, seq FROM {} WHERE seq > ?1 AND seq <= ?2 ORDER BY seq LIMIT ?3", cols, table),
                vec![(*cursor).into(), upto.unwrap_or(i64::MAX).into(), limit.into()],
            ),
        };
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(values.iter()), |row| map(row))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Get changes since a timestamp using exclusive comparison (>)
    /// This is used for checking unsynced changes where we want to exclude
    /// items that were synced at exactly the sync timestamp.
    pub fn get_changes_since_exclusive(&self, since: Option<i64>, limit: i64) -> VoiceResult<(Vec<HashMap<String, serde_json::Value>>, Option<i64>)> {
        // For exclusive comparison (>), we use get_changes_since with since + 1
        // This is equivalent to > since (i.e., >= since + 1)
        let adjusted_since = since.map(|ts| ts + 1);
        self.get_changes_since(adjusted_since, limit)
    }

    /// Get changes since a timestamp, returning SyncChange structs.
    /// Uses exclusive comparison (>) for incremental sync.
    /// This is the primary method for sync_server to use.
    pub fn get_changes_since_as_sync_changes(
        &self,
        since: Option<i64>,
        limit: i64,
    ) -> VoiceResult<(Vec<SyncChange>, Option<i64>)> {
        // Use exclusive comparison (>) for incremental sync
        let (changes, latest_timestamp) = if since.is_some() {
            self.get_changes_since_exclusive(since, limit)?
        } else {
            self.get_changes_since(None, limit)?
        };
        Ok((Self::feed_to_sync_changes(changes), latest_timestamp))
    }

    /// Cursor feed as `SyncChange`s: (changes, next_cursor, is_complete).
    pub fn get_changes_after_seq_as_sync_changes(
        &self,
        cursor: i64,
        upto: Option<i64>,
        limit: i64,
    ) -> VoiceResult<(Vec<SyncChange>, i64, bool)> {
        let feed = self.get_changes_after_seq(cursor, upto, limit)?;
        Ok((Self::feed_to_sync_changes(feed.changes), feed.next_cursor, feed.is_complete))
    }

    /// Convert feed maps to `SyncChange` structs (device fields are filled by
    /// the transport layer).
    pub fn feed_to_sync_changes(changes: Vec<HashMap<String, serde_json::Value>>) -> Vec<SyncChange> {
        changes
            .into_iter()
            .filter_map(|c| {
                let entity_type = c.get("entity_type")?.as_str()?.to_string();
                let entity_id = c.get("entity_id")?.as_str()?.to_string();
                let operation = c.get("operation")?.as_str().unwrap_or("create").to_string();
                let timestamp = c.get("timestamp")?.as_i64()?;
                let data = c.get("data").cloned().unwrap_or(serde_json::Value::Null);
                Some(SyncChange {
                    entity_type,
                    entity_id,
                    operation,
                    data,
                    timestamp,
                    device_id: String::new(),
                    device_name: None,
                })
            })
            .collect()
    }

    /// Get full dataset for initial sync
    pub fn get_full_dataset(&self) -> VoiceResult<HashMap<String, Vec<HashMap<String, serde_json::Value>>>> {
        let mut result = HashMap::new();

        // Get all notes
        let mut stmt = self.conn.prepare(
            r#"SELECT id, created_at, content, modified_at, deleted_at FROM notes"#
        )?;
        let note_rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, Option<i64>>(4)?,
            ))
        })?;

        let mut notes = Vec::new();
        for row in note_rows {
            let (id_bytes, created_at, content, modified_at, deleted_at) = row?;
            let mut note = HashMap::new();
            note.insert("id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&id_bytes).unwrap_or_default()));
            note.insert("created_at".to_string(), serde_json::json!(created_at));
            note.insert("content".to_string(), serde_json::Value::String(content));
            note.insert("modified_at".to_string(), modified_at.map_or(serde_json::Value::Null, |v| serde_json::json!(v)));
            note.insert("deleted_at".to_string(), deleted_at.map_or(serde_json::Value::Null, |v| serde_json::json!(v)));
            notes.push(note);
        }
        result.insert("notes".to_string(), notes);

        // Get all tags
        let mut stmt = self.conn.prepare(
            r#"SELECT id, name, parent_id, created_at, modified_at, deleted_at FROM tags"#
        )?;
        let tag_rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<Vec<u8>>>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, Option<i64>>(5)?,
            ))
        })?;

        let mut tags = Vec::new();
        for row in tag_rows {
            let (id_bytes, name, parent_id_bytes, created_at, modified_at, deleted_at) = row?;
            let mut tag = HashMap::new();
            tag.insert("id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&id_bytes).unwrap_or_default()));
            tag.insert("name".to_string(), serde_json::Value::String(name));
            tag.insert("parent_id".to_string(), parent_id_bytes.and_then(|b| uuid_bytes_to_hex(&b)).map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            tag.insert("created_at".to_string(), serde_json::json!(created_at));
            tag.insert("modified_at".to_string(), modified_at.map_or(serde_json::Value::Null, |v| serde_json::json!(v)));
            tag.insert("deleted_at".to_string(), deleted_at.map_or(serde_json::Value::Null, |v| serde_json::json!(v)));
            tags.push(tag);
        }
        result.insert("tags".to_string(), tags);

        // Get all note_tags
        let mut stmt = self.conn.prepare(
            r#"SELECT note_id, tag_id, created_at, modified_at, deleted_at FROM note_tags"#
        )?;
        let nt_rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, Option<i64>>(4)?,
            ))
        })?;

        let mut note_tags = Vec::new();
        for row in nt_rows {
            let (note_id_bytes, tag_id_bytes, created_at, modified_at, deleted_at) = row?;
            let mut nt = HashMap::new();
            nt.insert("note_id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&note_id_bytes).unwrap_or_default()));
            nt.insert("tag_id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&tag_id_bytes).unwrap_or_default()));
            nt.insert("created_at".to_string(), serde_json::json!(created_at));
            nt.insert("modified_at".to_string(), modified_at.map_or(serde_json::Value::Null, |v| serde_json::json!(v)));
            nt.insert("deleted_at".to_string(), deleted_at.map_or(serde_json::Value::Null, |v| serde_json::json!(v)));
            note_tags.push(nt);
        }
        result.insert("note_tags".to_string(), note_tags);

        // Get all audio_files
        let mut stmt = self.conn.prepare(
            r#"SELECT id, imported_at, filename, file_created_at, duration_seconds, summary, device_id, modified_at, deleted_at,
                      storage_provider, storage_key, storage_uploaded_at, content_sha256, storage_encrypted FROM audio_files"#
        )?;
        let af_rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, Option<f64>>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Vec<u8>>(6)?,
                row.get::<_, Option<i64>>(7)?,
                row.get::<_, Option<i64>>(8)?,
                row.get::<_, Option<String>>(9)?,
                row.get::<_, Option<String>>(10)?,
                row.get::<_, Option<i64>>(11)?,
                row.get::<_, Option<String>>(12)?,
                row.get::<_, Option<i64>>(13)?.unwrap_or(0),
            ))
        })?;

        let mut audio_files = Vec::new();
        for row in af_rows {
            let (id_bytes, imported_at, filename, file_created_at, duration_seconds, summary, device_id_bytes, modified_at, deleted_at, storage_provider, storage_key, storage_uploaded_at, content_sha256, storage_encrypted) = row?;
            let mut af = HashMap::new();
            af.insert("id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&id_bytes).unwrap_or_default()));
            af.insert("imported_at".to_string(), serde_json::json!(imported_at));
            af.insert("filename".to_string(), serde_json::Value::String(filename));
            af.insert("file_created_at".to_string(), file_created_at.map_or(serde_json::Value::Null, |v| serde_json::json!(v)));
            af.insert("duration_seconds".to_string(), duration_seconds.map_or(serde_json::Value::Null, |d| serde_json::json!(d)));
            af.insert("summary".to_string(), summary.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            af.insert("device_id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&device_id_bytes).unwrap_or_default()));
            af.insert("modified_at".to_string(), modified_at.map_or(serde_json::Value::Null, |v| serde_json::json!(v)));
            af.insert("deleted_at".to_string(), deleted_at.map_or(serde_json::Value::Null, |v| serde_json::json!(v)));
            af.insert("storage_provider".to_string(), storage_provider.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            af.insert("storage_key".to_string(), storage_key.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            af.insert("storage_uploaded_at".to_string(), storage_uploaded_at.map_or(serde_json::Value::Null, |v| serde_json::json!(v)));
            af.insert("content_sha256".to_string(), content_sha256.map_or(serde_json::Value::Null, serde_json::Value::String));
            af.insert("storage_encrypted".to_string(), serde_json::Value::Bool(storage_encrypted != 0));
            audio_files.push(af);
        }
        result.insert("audio_files".to_string(), audio_files);

        // Get all note_attachments
        let mut stmt = self.conn.prepare(
            r#"SELECT id, note_id, attachment_id, attachment_type, created_at, device_id, modified_at, deleted_at FROM note_attachments"#
        )?;
        let na_rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, Vec<u8>>(5)?,
                row.get::<_, Option<i64>>(6)?,
                row.get::<_, Option<i64>>(7)?,
            ))
        })?;

        let mut note_attachments = Vec::new();
        for row in na_rows {
            let (id_bytes, note_id_bytes, attachment_id_bytes, attachment_type, created_at, device_id_bytes, modified_at, deleted_at) = row?;
            let mut na = HashMap::new();
            na.insert("id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&id_bytes).unwrap_or_default()));
            na.insert("note_id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&note_id_bytes).unwrap_or_default()));
            na.insert("attachment_id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&attachment_id_bytes).unwrap_or_default()));
            na.insert("attachment_type".to_string(), serde_json::Value::String(attachment_type));
            na.insert("created_at".to_string(), serde_json::json!(created_at));
            na.insert("device_id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&device_id_bytes).unwrap_or_default()));
            na.insert("modified_at".to_string(), modified_at.map_or(serde_json::Value::Null, |v| serde_json::json!(v)));
            na.insert("deleted_at".to_string(), deleted_at.map_or(serde_json::Value::Null, |v| serde_json::json!(v)));
            note_attachments.push(na);
        }
        result.insert("note_attachments".to_string(), note_attachments);

        // Every version: the complete history travels with the full dataset
        let mut field_versions = Vec::new();
        for v in self.get_versions_since(None, i64::MAX)? {
            if let serde_json::Value::Object(map) = v.to_json() {
                field_versions.push(map.into_iter().collect::<HashMap<String, serde_json::Value>>());
            }
        }
        result.insert("field_versions".to_string(), field_versions);

        Ok(result)
    }

    // ============================================================================
    // Sync apply methods
    // ============================================================================

    /// Apply a sync change (used by sync server to apply remote changes)
    pub fn apply_sync_note(
        &self,
        note_id: &str,
        created_at: i64,
        content: &str,
        modified_at: Option<i64>,
        deleted_at: Option<i64>,
        sync_received_at: Option<i64>,
        // Which attachment stands for the note, when the sender named one.
        // A hint like every other row value (VER-4): the version is the
        // truth, and this only gives a root to a database that has none.
        primary_attachment_id: Option<&str>,
    ) -> VoiceResult<bool> {
        let uuid = Uuid::parse_str(note_id)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?;
        let uuid_bytes = uuid.as_bytes().to_vec();
        let id_hex = uuid.simple().to_string();

        // Row: create if missing; never overwrite versioned columns from a row.
        let existing: Option<i64> = self.conn
            .query_row("SELECT 1 FROM notes WHERE id = ?", params![&uuid_bytes], |row| row.get(0))
            .optional()?;
        if existing.is_some() {
            self.conn.execute(
                "UPDATE notes SET modified_at = NULLIF(MAX(COALESCE(modified_at, 0), COALESCE(?, 0)), 0), sync_received_at = COALESCE(?, sync_received_at) WHERE id = ?",
                params![modified_at, sync_received_at, uuid_bytes],
            )?;
        } else {
            self.conn.execute(
                "INSERT INTO notes (id, created_at, content, modified_at, deleted_at, sync_received_at) VALUES (?, ?, ?, ?, ?, ?)",
                params![uuid_bytes, created_at, content, modified_at, deleted_at, sync_received_at],
            )?;
        }

        // Peers that predate versioning send rows without history: give those
        // rows deterministic roots so every device converges on the same graph.
        self.ensure_root_version(ENTITY_NOTE, &id_hex, FIELD_CONTENT, content, modified_at.unwrap_or(created_at))?;
        if let Some(primary) = primary_attachment_id.filter(|p| !p.is_empty()) {
            self.ensure_root_version(
                ENTITY_NOTE,
                &id_hex,
                FIELD_PRIMARY_ATTACHMENT,
                primary,
                modified_at.unwrap_or(created_at),
            )?;
        }
        if let Some(d) = deleted_at {
            self.ensure_root_version(ENTITY_NOTE, &id_hex, FIELD_DELETED, "1", d)?;
        }

        // Heads are the authority for content and deletion.
        self.reapply_entity_heads(ENTITY_NOTE, &id_hex)?;

        // Rebuild caches after sync (only if not deleted)
        if deleted_at.is_none() {
            let _ = self.rebuild_note_list_cache(note_id);
            let _ = self.rebuild_note_cache(note_id);
        }

        Ok(true)
    }

    /// Apply a sync tag change
    pub fn apply_sync_tag(
        &self,
        tag_id: &str,
        name: &str,
        parent_id: Option<&str>,
        created_at: i64,
        modified_at: Option<i64>,
        sync_received_at: Option<i64>,
    ) -> VoiceResult<bool> {
        self.apply_sync_tag_with_deleted(tag_id, name, parent_id, created_at, modified_at, None, sync_received_at)
    }

    /// Apply a sync tag change including deleted_at timestamp
    pub fn apply_sync_tag_with_deleted(
        &self,
        tag_id: &str,
        name: &str,
        parent_id: Option<&str>,
        created_at: i64,
        modified_at: Option<i64>,
        deleted_at: Option<i64>,
        sync_received_at: Option<i64>,
    ) -> VoiceResult<bool> {
        let uuid = Uuid::parse_str(tag_id)
            .map_err(|e| VoiceError::validation("tag_id", e.to_string()))?;
        let uuid_bytes = uuid.as_bytes().to_vec();
        let id_hex = uuid.simple().to_string();

        let parent_bytes = match parent_id {
            Some(pid) => {
                let parent_uuid = Uuid::parse_str(pid)
                    .map_err(|e| VoiceError::validation("parent_id", e.to_string()))?;
                Some(parent_uuid.as_bytes().to_vec())
            }
            None => None,
        };

        let existing: Option<i64> = self.conn
            .query_row("SELECT 1 FROM tags WHERE id = ?", params![&uuid_bytes], |row| row.get(0))
            .optional()?;
        if existing.is_some() {
            self.conn.execute(
                "UPDATE tags SET modified_at = NULLIF(MAX(COALESCE(modified_at, 0), COALESCE(?, 0)), 0), sync_received_at = COALESCE(?, sync_received_at) WHERE id = ?",
                params![modified_at, sync_received_at, uuid_bytes],
            )?;
        } else {
            self.conn.execute(
                "INSERT INTO tags (id, name, parent_id, created_at, modified_at, deleted_at, sync_received_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                params![uuid_bytes, name, parent_bytes, created_at, modified_at, deleted_at, sync_received_at],
            )?;
        }

        let ts = modified_at.unwrap_or(created_at);
        self.ensure_root_version(ENTITY_TAG, &id_hex, FIELD_NAME, name, ts)?;
        self.ensure_root_version(ENTITY_TAG, &id_hex, FIELD_PARENT, parent_id.unwrap_or(""), ts)?;
        if let Some(d) = deleted_at {
            self.ensure_root_version(ENTITY_TAG, &id_hex, FIELD_DELETED, "1", d)?;
        }
        self.reapply_entity_heads(ENTITY_TAG, &id_hex)?;
        Ok(true)
    }

    /// Apply a sync note_tag change
    pub fn apply_sync_note_tag(
        &self,
        note_id: &str,
        tag_id: &str,
        created_at: i64,
        modified_at: Option<i64>,
        deleted_at: Option<i64>,
        sync_received_at: Option<i64>,
    ) -> VoiceResult<bool> {
        let note_uuid = Uuid::parse_str(note_id)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?;
        let tag_uuid = Uuid::parse_str(tag_id)
            .map_err(|e| VoiceError::validation("tag_id", e.to_string()))?;
        let note_bytes = note_uuid.as_bytes().to_vec();
        let tag_bytes = tag_uuid.as_bytes().to_vec();
        let note_hex = note_uuid.simple().to_string();
        let tag_hex = tag_uuid.simple().to_string();

        let existing: Option<i64> = self.conn
            .query_row(
                "SELECT 1 FROM note_tags WHERE note_id = ? AND tag_id = ?",
                params![&note_bytes, &tag_bytes],
                |row| row.get(0),
            )
            .optional()?;
        if existing.is_some() {
            self.conn.execute(
                "UPDATE note_tags SET modified_at = NULLIF(MAX(COALESCE(modified_at, 0), COALESCE(?, 0)), 0), sync_received_at = COALESCE(?, sync_received_at) WHERE note_id = ? AND tag_id = ?",
                params![modified_at, sync_received_at, note_bytes, tag_bytes],
            )?;
        } else {
            self.conn.execute(
                "INSERT INTO note_tags (note_id, tag_id, created_at, modified_at, deleted_at, sync_received_at) VALUES (?, ?, ?, ?, ?, ?)",
                params![note_bytes, tag_bytes, created_at, modified_at, deleted_at, sync_received_at],
            )?;
        }

        let entity_id = note_tag_entity_id(&note_hex, &tag_hex);
        let active = if deleted_at.is_some() { "0" } else { "1" };
        self.ensure_root_version(ENTITY_NOTE_TAG, &entity_id, FIELD_ACTIVE, active, deleted_at.or(modified_at).unwrap_or(created_at))?;
        self.reapply_entity_heads(ENTITY_NOTE_TAG, &entity_id)?;

        // Always rebuild note pane cache (tags list may have changed)
        let _ = self.rebuild_note_cache(&note_hex);
        let _ = self.rebuild_note_list_cache(&note_hex);

        Ok(true)
    }

    /// Get raw note data by ID (including deleted, for sync)
    pub fn get_note_raw(&self, note_id: &str) -> VoiceResult<Option<HashMap<String, serde_json::Value>>> {
        let uuid = validate_note_id(note_id)?;
        let uuid_bytes = uuid.as_bytes().to_vec();

        let result: Option<(Vec<u8>, i64, String, Option<i64>, Option<i64>)> = self.conn
            .query_row(
                "SELECT id, created_at, content, modified_at, deleted_at FROM notes WHERE id = ?",
                params![uuid_bytes],
                |row| Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                )),
            )
            .optional()?;

        match result {
            Some((id_bytes, created_at, content, modified_at, deleted_at)) => {
                let mut note = HashMap::new();
                note.insert("id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&id_bytes).unwrap_or_default()));
                note.insert("created_at".to_string(), serde_json::json!(created_at));
                note.insert("content".to_string(), serde_json::Value::String(content));
                note.insert("modified_at".to_string(), modified_at.map_or(serde_json::Value::Null, |v| serde_json::json!(v)));
                note.insert("deleted_at".to_string(), deleted_at.map_or(serde_json::Value::Null, |v| serde_json::json!(v)));
                Ok(Some(note))
            }
            None => Ok(None),
        }
    }

    /// Get raw tag data by ID (for sync)
    pub fn get_tag_raw(&self, tag_id: &str) -> VoiceResult<Option<HashMap<String, serde_json::Value>>> {
        let uuid = validate_tag_id(tag_id)?;
        let uuid_bytes = uuid.as_bytes().to_vec();

        let result: Option<(Vec<u8>, String, Option<Vec<u8>>, i64, Option<i64>)> = self.conn
            .query_row(
                "SELECT id, name, parent_id, created_at, modified_at FROM tags WHERE id = ?",
                params![uuid_bytes],
                |row| Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                )),
            )
            .optional()?;

        match result {
            Some((id_bytes, name, parent_id_bytes, created_at, modified_at)) => {
                let mut tag = HashMap::new();
                tag.insert("id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&id_bytes).unwrap_or_default()));
                tag.insert("name".to_string(), serde_json::Value::String(name));
                tag.insert("parent_id".to_string(), parent_id_bytes.and_then(|b| uuid_bytes_to_hex(&b)).map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
                tag.insert("created_at".to_string(), serde_json::json!(created_at));
                tag.insert("modified_at".to_string(), modified_at.map_or(serde_json::Value::Null, |v| serde_json::json!(v)));
                Ok(Some(tag))
            }
            None => Ok(None),
        }
    }

    /// Get raw note_tag data (for sync)
    pub fn get_note_tag_raw(&self, note_id: &str, tag_id: &str) -> VoiceResult<Option<HashMap<String, serde_json::Value>>> {
        let note_uuid = Uuid::parse_str(note_id)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?;
        let tag_uuid = Uuid::parse_str(tag_id)
            .map_err(|e| VoiceError::validation("tag_id", e.to_string()))?;
        let note_bytes = note_uuid.as_bytes().to_vec();
        let tag_bytes = tag_uuid.as_bytes().to_vec();

        let result: Option<(Vec<u8>, Vec<u8>, i64, Option<i64>, Option<i64>)> = self.conn
            .query_row(
                "SELECT note_id, tag_id, created_at, modified_at, deleted_at FROM note_tags WHERE note_id = ? AND tag_id = ?",
                params![note_bytes, tag_bytes],
                |row| Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                )),
            )
            .optional()?;

        match result {
            Some((note_id_bytes, tag_id_bytes, created_at, modified_at, deleted_at)) => {
                let mut nt = HashMap::new();
                nt.insert("note_id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&note_id_bytes).unwrap_or_default()));
                nt.insert("tag_id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&tag_id_bytes).unwrap_or_default()));
                nt.insert("created_at".to_string(), serde_json::json!(created_at));
                nt.insert("modified_at".to_string(), modified_at.map_or(serde_json::Value::Null, |v| serde_json::json!(v)));
                nt.insert("deleted_at".to_string(), deleted_at.map_or(serde_json::Value::Null, |v| serde_json::json!(v)));
                Ok(Some(nt))
            }
            None => Ok(None),
        }
    }

    // Conflict detection, records and resolution live in versions.rs.

    // Helper methods for row conversion

    fn row_to_note(&self, row: &Row) -> rusqlite::Result<NoteRow> {
        let id_bytes: Vec<u8> = row.get(0)?;
        let created_at: i64 = row.get(1)?;
        let content: String = row.get(2)?;
        let modified_at: Option<i64> = row.get(3)?;
        let deleted_at: Option<i64> = row.get(4)?;
        let tag_names: Option<String> = row.get(5)?;
        let display_cache: Option<String> = row.get(6)?;
        let list_display_cache: Option<String> = row.get(7)?;

        Ok(NoteRow {
            id: uuid_bytes_to_hex(&id_bytes).unwrap_or_default(),
            created_at,
            content,
            modified_at,
            deleted_at,
            tag_names,
            created_at_offset: row.get::<_, Option<i64>>(8)?.and_then(|o| i32::try_from(o).ok()),
            created_at_zone: row.get(9)?,
            modified_at_offset: row.get::<_, Option<i64>>(10)?.and_then(|o| i32::try_from(o).ok()),
            modified_at_zone: row.get(11)?,
            deleted_at_offset: row.get::<_, Option<i64>>(12)?.and_then(|o| i32::try_from(o).ok()),
            deleted_at_zone: row.get(13)?,
            display_cache,
            list_display_cache,
        })
    }

    fn row_to_tag(&self, row: &Row) -> rusqlite::Result<TagRow> {
        let id_bytes: Vec<u8> = row.get(0)?;
        let name: String = row.get(1)?;
        let parent_id_bytes: Option<Vec<u8>> = row.get(2)?;
        let created_at: Option<i64> = row.get(3)?;
        let modified_at: Option<i64> = row.get(4)?;

        Ok(TagRow {
            id: uuid_bytes_to_hex(&id_bytes).unwrap_or_default(),
            name,
            parent_id: parent_id_bytes.and_then(|b| uuid_bytes_to_hex(&b)),
            created_at,
            modified_at,
        })
    }

    // ========================================================================
    // NoteAttachment operations
    // ========================================================================

    /// Attach an attachment to a note
    pub fn attach_to_note(
        &self,
        note_id: &str,
        attachment_id: &str,
        attachment_type: &str,
    ) -> VoiceResult<String> {
        let note_uuid = validate_note_id(note_id)?;
        let attachment_uuid = Uuid::parse_str(attachment_id)
            .map_err(|e| VoiceError::validation("attachment_id", e.to_string()))?;
        let association_id = Uuid::now_v7();

        let device_id = get_local_device_id();
        let note_bytes = note_uuid.as_bytes().to_vec();

        self.conn.execute(
            r#"
            INSERT INTO note_attachments (id, note_id, attachment_id, attachment_type, created_at, device_id)
            VALUES (?, ?, ?, ?, strftime('%s', 'now'), ?)
            "#,
            params![
                association_id.as_bytes().to_vec(),
                &note_bytes,
                attachment_uuid.as_bytes().to_vec(),
                attachment_type,
                device_id.as_bytes().to_vec(),
            ],
        )?;

        // Update the parent Note's modified_at to trigger sync
        self.conn.execute(
            "UPDATE notes SET modified_at = strftime('%s', 'now') WHERE id = ?",
            params![note_bytes],
        )?;

        // Membership version: the start of this link's history
        let association_hex = association_id.simple().to_string();
        self.init_field(ENTITY_NOTE_ATTACHMENT, &association_hex, FIELD_ACTIVE, "1")?;

        // Rebuild display cache for the note
        let _ = self.rebuild_note_cache(note_id);
        let _ = self.stamp_local_zone("note_attachments", &association_hex, "created_at");

        Ok(association_hex)
    }

    /// Detach an attachment from a note (soft delete)
    pub fn detach_from_note(&self, association_id: &str) -> VoiceResult<bool> {
        let uuid = Uuid::parse_str(association_id)
            .map_err(|e| VoiceError::validation("association_id", e.to_string()))?;
        let uuid_bytes = uuid.as_bytes().to_vec();

        // Get the note_id before updating so we can update the note's modified_at
        let note_bytes: Option<Vec<u8>> = self
            .conn
            .query_row(
                "SELECT note_id FROM note_attachments WHERE id = ? AND deleted_at IS NULL",
                params![&uuid_bytes],
                |row| row.get(0),
            )
            .optional()?;
        let note_bytes = match note_bytes {
            Some(n) => n,
            None => return Ok(false),
        };

        self.set_field(ENTITY_NOTE_ATTACHMENT, &uuid.simple().to_string(), FIELD_ACTIVE, "0", None)?;

        // Update the parent Note's modified_at to trigger sync and rebuild cache
        self.conn.execute(
            "UPDATE notes SET modified_at = strftime('%s', 'now') WHERE id = ?",
            params![&note_bytes],
        )?;
        if let Some(note_id_hex) = uuid_bytes_to_hex(&note_bytes) {
            let _ = self.rebuild_note_cache(&note_id_hex);
        }

        Ok(true)
    }

    /// Get all attachments for a note (accepts ID or ID prefix)
    pub fn get_attachments_for_note(&self, note_id: &str) -> VoiceResult<Vec<NoteAttachmentRow>> {
        // Use try_resolve to return empty Vec if note not found
        let resolved_id = match self.try_resolve_note_id(note_id)? {
            Some(id) => id,
            None => return Ok(Vec::new()),
        };
        let uuid = Uuid::parse_str(&resolved_id)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?;
        let uuid_bytes = uuid.as_bytes().to_vec();

        let mut stmt = self.conn.prepare(
            r#"
            SELECT id, note_id, attachment_id, attachment_type, created_at, device_id, modified_at, deleted_at
            FROM note_attachments
            WHERE note_id = ? AND deleted_at IS NULL
            ORDER BY created_at DESC
            "#,
        )?;

        let rows = stmt.query_map([uuid_bytes], |row| self.row_to_note_attachment(row))?;
        let mut attachments = Vec::new();
        for attachment in rows {
            attachments.push(attachment?);
        }
        Ok(attachments)
    }

    /// Get a specific attachment association
    pub fn get_attachment(&self, association_id: &str) -> VoiceResult<Option<NoteAttachmentRow>> {
        let uuid = Uuid::parse_str(association_id)
            .map_err(|e| VoiceError::validation("association_id", e.to_string()))?;
        let uuid_bytes = uuid.as_bytes().to_vec();

        let mut stmt = self.conn.prepare(
            r#"
            SELECT id, note_id, attachment_id, attachment_type, created_at, device_id, modified_at, deleted_at
            FROM note_attachments
            WHERE id = ?
            "#,
        )?;

        let mut rows = stmt.query_map([uuid_bytes], |row| self.row_to_note_attachment(row))?;
        match rows.next() {
            Some(Ok(attachment)) => Ok(Some(attachment)),
            Some(Err(e)) => Err(VoiceError::Database(e)),
            None => Ok(None),
        }
    }

    fn row_to_note_attachment(&self, row: &Row) -> rusqlite::Result<NoteAttachmentRow> {
        let id_bytes: Vec<u8> = row.get(0)?;
        let note_id_bytes: Vec<u8> = row.get(1)?;
        let attachment_id_bytes: Vec<u8> = row.get(2)?;
        let attachment_type: String = row.get(3)?;
        let created_at: i64 = row.get(4)?;
        let device_id_bytes: Vec<u8> = row.get(5)?;
        let modified_at: Option<i64> = row.get(6)?;
        let deleted_at: Option<i64> = row.get(7)?;

        Ok(NoteAttachmentRow {
            id: uuid_bytes_to_hex(&id_bytes).unwrap_or_default(),
            note_id: uuid_bytes_to_hex(&note_id_bytes).unwrap_or_default(),
            attachment_id: uuid_bytes_to_hex(&attachment_id_bytes).unwrap_or_default(),
            attachment_type,
            created_at,
            device_id: uuid_bytes_to_hex(&device_id_bytes).unwrap_or_default(),
            modified_at,
            deleted_at,
        })
    }

    // ========================================================================
    // AudioFile operations
    // ========================================================================

    /// Create a new audio file record
    pub fn create_audio_file(
        &self,
        filename: &str,
        file_created_at: Option<i64>,
    ) -> VoiceResult<String> {
        self.create_audio_file_with_duration(filename, file_created_at, None, crate::models::FileOrigin::Imported, None)
    }

    /// Create a new audio file record with optional duration. The file's
    /// name in the audio folder (FILE-15): a recording's start and the tail
    /// of its id, or an imported file's own name; a name taken in the folder,
    /// by a row or by a file in `audio_dir`, gets ` (2)` and so on.
    pub fn create_audio_file_with_duration(
        &self,
        filename: &str,
        file_created_at: Option<i64>,
        duration_seconds: Option<i64>,
        origin: crate::models::FileOrigin,
        audio_dir: Option<&Path>,
    ) -> VoiceResult<String> {
        if origin == crate::models::FileOrigin::Imported && !crate::models::valid_file_name(filename) {
            return Err(VoiceError::validation("filename", format!("{:?} is not a file name", filename)));
        }
        let audio_file_id = Uuid::now_v7();
        let uuid_bytes = audio_file_id.as_bytes().to_vec();
        let device_id = get_local_device_id();

        self.conn.execute(
            r#"
            INSERT INTO audio_files (id, imported_at, filename, file_created_at, duration_seconds, device_id)
            VALUES (?, strftime('%s', 'now'), ?, ?, ?, ?)
            "#,
            params![
                uuid_bytes,
                filename,
                file_created_at,
                duration_seconds,
                device_id.as_bytes().to_vec(),
            ],
        )?;

        let id_hex = audio_file_id.simple().to_string();
        let _ = self.stamp_local_zone("audio_files", &id_hex, "imported_at");
        if file_created_at.is_some() {
            // The best this device can say about a file's own date: a file it
            // recorded itself was recorded here, and one copied from elsewhere
            // carries no zone of its own.
            let _ = self.stamp_local_zone("audio_files", &id_hex, "file_created_at");
        }
        // The file's name on this device (FILE-15)
        let wanted = match origin {
            crate::models::FileOrigin::Recorded => {
                let moment = file_created_at.unwrap_or_else(|| Utc::now().timestamp());
                crate::models::recording_file_name(&id_hex, filename, moment, crate::timezone::stamp_offset())
            }
            crate::models::FileOrigin::Imported => filename.to_string(),
        };
        let taken_by_rows: std::collections::HashSet<String> = {
            let mut stmt = self.conn.prepare("SELECT local_name FROM audio_files WHERE local_name IS NOT NULL AND local_name != '' AND id != ?")?;
            let rows = stmt.query_map([&uuid_bytes], |r| r.get::<_, String>(0))?;
            rows.collect::<Result<Vec<_>, _>>()?.into_iter().map(|n| n.to_lowercase()).collect()
        };
        // Compared without regard to case: the phone's shared storage does not tell "A" from "a"
        let local_name = crate::models::free_file_name(&wanted, |candidate| {
            taken_by_rows.contains(&candidate.to_lowercase()) || audio_dir.is_some_and(|dir| dir.join(candidate).exists())
        });
        self.conn.execute("UPDATE audio_files SET local_name = ? WHERE id = ?", params![local_name, uuid_bytes])?;
        Ok(id_hex)
    }

    /// Get an audio file by ID (accepts ID or ID prefix)
    pub fn get_audio_file(&self, audio_file_id: &str) -> VoiceResult<Option<AudioFileRow>> {
        // Use resolver that includes deleted files (so we can check deleted_at status)
        let resolved_id = match self.try_resolve_audio_file_id_including_deleted(audio_file_id)? {
            Some(id) => id,
            None => return Ok(None),
        };
        let uuid = Uuid::parse_str(&resolved_id)
            .map_err(|e| VoiceError::validation("audio_file_id", e.to_string()))?;
        let uuid_bytes = uuid.as_bytes().to_vec();

        let mut stmt = self.conn.prepare(
            r#"
            SELECT id, imported_at, filename, file_created_at, duration_seconds, summary, device_id, modified_at, deleted_at,
                   storage_provider, storage_key, storage_uploaded_at,
                   imported_at_offset, imported_at_zone, file_created_at_offset, file_created_at_zone,
                   modified_at_offset, modified_at_zone, deleted_at_offset, deleted_at_zone, local_name, content_sha256, storage_encrypted
            FROM audio_files
            WHERE id = ?
            "#,
        )?;

        let mut rows = stmt.query_map([uuid_bytes], |row| self.row_to_audio_file(row))?;
        match rows.next() {
            Some(Ok(audio_file)) => Ok(Some(audio_file)),
            Some(Err(e)) => Err(VoiceError::Database(e)),
            None => Ok(None),
        }
    }

    /// Get all audio files for a note (via note_attachments) (accepts ID or ID prefix)
    pub fn get_audio_files_for_note(&self, note_id: &str) -> VoiceResult<Vec<AudioFileRow>> {
        // Use try_resolve to return empty Vec if note not found
        let resolved_id = match self.try_resolve_note_id(note_id)? {
            Some(id) => id,
            None => return Ok(Vec::new()),
        };
        let uuid = Uuid::parse_str(&resolved_id)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?;
        let uuid_bytes = uuid.as_bytes().to_vec();

        let mut stmt = self.conn.prepare(
            r#"
            SELECT af.id, af.imported_at, af.filename, af.file_created_at, af.duration_seconds,
                   af.summary, af.device_id, af.modified_at, af.deleted_at,
                   af.storage_provider, af.storage_key, af.storage_uploaded_at,
                   af.imported_at_offset, af.imported_at_zone, af.file_created_at_offset, af.file_created_at_zone,
                   af.modified_at_offset, af.modified_at_zone, af.deleted_at_offset, af.deleted_at_zone, af.local_name, af.content_sha256, af.storage_encrypted
            FROM audio_files af
            INNER JOIN note_attachments na ON af.id = na.attachment_id
            WHERE na.note_id = ?
              AND na.attachment_type = 'audio_file'
              AND na.deleted_at IS NULL
              AND af.deleted_at IS NULL
            -- Oldest first: a note's recordings read as the conversation
            -- happened, and "the first recording" means the first one made.
            -- The id breaks a tie, so two recordings imported in the same
            -- second come back in the same order on every device.
            ORDER BY COALESCE(af.file_created_at, af.imported_at) ASC, af.id ASC
            "#,
        )?;

        let rows = stmt.query_map([uuid_bytes], |row| self.row_to_audio_file(row))?;
        let mut audio_files = Vec::new();
        for audio_file in rows {
            audio_files.push(audio_file?);
        }
        Ok(audio_files)
    }

    /// Get all note IDs that have an audio file attached (via note_attachments)
    pub fn get_notes_for_audio_file(&self, audio_file_id: &str) -> VoiceResult<Vec<String>> {
        let uuid = Uuid::parse_str(audio_file_id)
            .map_err(|e| VoiceError::validation("audio_file_id", e.to_string()))?;
        let uuid_bytes = uuid.as_bytes().to_vec();

        let mut stmt = self.conn.prepare(
            r#"
            SELECT note_id
            FROM note_attachments
            WHERE attachment_id = ?
              AND attachment_type = 'audio_file'
              AND deleted_at IS NULL
            "#,
        )?;

        let rows = stmt.query_map([uuid_bytes], |row| {
            let bytes: Vec<u8> = row.get(0)?;
            let uuid = Uuid::from_slice(&bytes).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Blob, Box::new(e))
            })?;
            Ok(uuid.simple().to_string())
        })?;

        let mut note_ids = Vec::new();
        for note_id in rows {
            note_ids.push(note_id?);
        }
        Ok(note_ids)
    }

    /// Get all audio files from the database (including deleted ones)
    pub fn get_all_audio_files(&self) -> VoiceResult<Vec<AudioFileRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT id, imported_at, filename, file_created_at, duration_seconds,
                   summary, device_id, modified_at, deleted_at,
                   storage_provider, storage_key, storage_uploaded_at,
                   imported_at_offset, imported_at_zone, file_created_at_offset, file_created_at_zone,
                   modified_at_offset, modified_at_zone, deleted_at_offset, deleted_at_zone, local_name, content_sha256, storage_encrypted
            FROM audio_files
            ORDER BY imported_at DESC
            "#,
        )?;

        let rows = stmt.query_map([], |row| self.row_to_audio_file(row))?;
        let mut audio_files = Vec::new();
        for audio_file in rows {
            audio_files.push(audio_file?);
        }
        Ok(audio_files)
    }

    /// Update an audio file's summary
    pub fn update_audio_file_summary(
        &self,
        audio_file_id: &str,
        summary: &str,
    ) -> VoiceResult<bool> {
        let uuid = Uuid::parse_str(audio_file_id)
            .map_err(|e| VoiceError::validation("audio_file_id", e.to_string()))?;
        let alive: Option<i64> = self
            .conn
            .query_row(
                "SELECT 1 FROM audio_files WHERE id = ? AND deleted_at IS NULL",
                params![uuid.as_bytes().to_vec()],
                |row| row.get(0),
            )
            .optional()?;
        if alive.is_none() {
            return Ok(false);
        }
        self.set_field(ENTITY_AUDIO_FILE, &uuid.simple().to_string(), FIELD_SUMMARY, summary, None)?;
        Ok(true)
    }

    /// Soft-delete an audio file (accepts ID or ID prefix)
    pub fn delete_audio_file(&self, audio_file_id: &str) -> VoiceResult<bool> {
        // Use try_resolve to return false if not found (instead of error)
        let resolved_id = match self.try_resolve_audio_file_id(audio_file_id)? {
            Some(id) => id,
            None => return Ok(false),
        };
        self.set_deleted(ENTITY_AUDIO_FILE, &resolved_id)
    }

    /// Update an audio file's duration
    pub fn update_audio_file_duration(
        &self,
        audio_file_id: &str,
        duration_seconds: i64,
    ) -> VoiceResult<bool> {
        let resolved_id = match self.try_resolve_audio_file_id(audio_file_id)? {
            Some(id) => id,
            None => return Ok(false),
        };
        let uuid = Uuid::parse_str(&resolved_id)
            .map_err(|e| VoiceError::validation("audio_file_id", e.to_string()))?;
        let uuid_bytes = uuid.as_bytes().to_vec();
        let device_id = get_local_device_id();

        let updated = self.conn.execute(
            r#"
            UPDATE audio_files
            SET duration_seconds = ?, modified_at = strftime('%s', 'now'), device_id = ?
            WHERE id = ? AND deleted_at IS NULL
            "#,
            params![duration_seconds, device_id.as_bytes().to_vec(), uuid_bytes],
        )?;

        // Rebuild list cache for any notes that have this audio file attached
        if updated > 0 {
            self.rebuild_caches_for_audio_file(&resolved_id);
        }

        Ok(updated > 0)
    }

    /// Set when a recording was made, for a row that never had it.
    ///
    /// Unversioned metadata, like the duration: read off the file or its name
    /// by whichever device has the file, merged per column by `modified_at`.
    /// Calculating it is a repair, not an edit by the user, so it is written
    /// directly rather than as a new version.
    pub fn update_audio_file_created_at(
        &self,
        audio_file_id: &str,
        file_created_at: i64,
    ) -> VoiceResult<bool> {
        let resolved_id = match self.try_resolve_audio_file_id(audio_file_id)? {
            Some(id) => id,
            None => return Ok(false),
        };
        let uuid = Uuid::parse_str(&resolved_id)
            .map_err(|e| VoiceError::validation("audio_file_id", e.to_string()))?;
        let uuid_bytes = uuid.as_bytes().to_vec();
        let device_id = get_local_device_id();

        let updated = self.conn.execute(
            r#"
            UPDATE audio_files
            SET file_created_at = ?, modified_at = strftime('%s', 'now'), device_id = ?
            WHERE id = ? AND deleted_at IS NULL
            "#,
            params![file_created_at, device_id.as_bytes().to_vec(), uuid_bytes],
        )?;

        if updated > 0 {
            // The date is shown on the note, so the caches that hold it change
            self.rebuild_caches_for_audio_file(&resolved_id);
        }

        Ok(updated > 0)
    }

    /// Get all audio files that are missing duration information
    pub fn get_audio_files_missing_duration(&self) -> VoiceResult<Vec<AudioFileRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT id, imported_at, filename, file_created_at, duration_seconds,
                   summary, device_id, modified_at, deleted_at,
                   storage_provider, storage_key, storage_uploaded_at,
                   imported_at_offset, imported_at_zone, file_created_at_offset, file_created_at_zone,
                   modified_at_offset, modified_at_zone, deleted_at_offset, deleted_at_zone, local_name, content_sha256, storage_encrypted
            FROM audio_files
            WHERE duration_seconds IS NULL AND deleted_at IS NULL
            ORDER BY imported_at DESC
            "#,
        )?;

        let rows = stmt.query_map([], |row| self.row_to_audio_file(row))?;
        let mut audio_files = Vec::new();
        for audio_file in rows {
            audio_files.push(audio_file?);
        }
        Ok(audio_files)
    }

    fn row_to_audio_file(&self, row: &Row) -> rusqlite::Result<AudioFileRow> {
        let id_bytes: Vec<u8> = row.get(0)?;
        let imported_at: i64 = row.get(1)?;
        let filename: String = row.get(2)?;
        let file_created_at: Option<i64> = row.get(3)?;
        let duration_seconds: Option<i64> = row.get(4)?;
        let summary: Option<String> = row.get(5)?;
        let device_id_bytes: Vec<u8> = row.get(6)?;
        let modified_at: Option<i64> = row.get(7)?;
        let deleted_at: Option<i64> = row.get(8)?;
        let storage_provider: Option<String> = row.get(9)?;
        let storage_key: Option<String> = row.get(10)?;
        let storage_uploaded_at: Option<i64> = row.get(11)?;

        Ok(AudioFileRow {
            id: uuid_bytes_to_hex(&id_bytes).unwrap_or_default(),
            imported_at,
            filename,
            file_created_at,
            duration_seconds,
            summary,
            device_id: uuid_bytes_to_hex(&device_id_bytes).unwrap_or_default(),
            modified_at,
            deleted_at,
            storage_provider,
            storage_key,
            storage_uploaded_at,
            imported_at_offset: row.get::<_, Option<i64>>(12)?.and_then(|o| i32::try_from(o).ok()),
            imported_at_zone: row.get(13)?,
            file_created_at_offset: row.get::<_, Option<i64>>(14)?.and_then(|o| i32::try_from(o).ok()),
            file_created_at_zone: row.get(15)?,
            modified_at_offset: row.get::<_, Option<i64>>(16)?.and_then(|o| i32::try_from(o).ok()),
            modified_at_zone: row.get(17)?,
            deleted_at_offset: row.get::<_, Option<i64>>(18)?.and_then(|o| i32::try_from(o).ok()),
            deleted_at_zone: row.get(19)?,
            local_name: row.get::<_, Option<String>>(20)?.unwrap_or_default(),
            content_sha256: row.get(21)?,
            storage_encrypted: row.get::<_, Option<i64>>(22)?.unwrap_or(0) != 0,
        })
    }

    // ========================================================================
    // Cloud Storage operations for AudioFile
    // ========================================================================

    /// Get all audio files that need to be uploaded to cloud storage.
    ///
    /// Returns files where storage_provider is NULL (not yet uploaded).
    /// Excludes deleted files.
    pub fn get_audio_files_pending_upload(&self) -> VoiceResult<Vec<AudioFileRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT id, imported_at, filename, file_created_at, duration_seconds,
                   summary, device_id, modified_at, deleted_at,
                   storage_provider, storage_key, storage_uploaded_at,
                   imported_at_offset, imported_at_zone, file_created_at_offset, file_created_at_zone,
                   modified_at_offset, modified_at_zone, deleted_at_offset, deleted_at_zone, local_name, content_sha256, storage_encrypted
            FROM audio_files
            WHERE storage_provider IS NULL AND deleted_at IS NULL
            ORDER BY imported_at DESC
            "#,
        )?;

        let rows = stmt.query_map([], |row| self.row_to_audio_file(row))?;
        let mut audio_files = Vec::new();
        for audio_file in rows {
            audio_files.push(audio_file?);
        }
        Ok(audio_files)
    }

    /// Update an audio file's cloud storage information after successful upload.
    ///
    /// This marks the file as uploaded to the specified cloud provider.
    pub fn update_audio_file_storage(
        &self,
        audio_file_id: &str,
        storage_provider: &str,
        storage_key: &str,
    ) -> VoiceResult<bool> {
        self.update_audio_file_storage_encrypted(audio_file_id, storage_provider, storage_key, false)
    }

    /// `update_audio_file_storage`, saying whether the object is encrypted (Stage 15).
    pub fn update_audio_file_storage_encrypted(
        &self,
        audio_file_id: &str,
        storage_provider: &str,
        storage_key: &str,
        encrypted: bool,
    ) -> VoiceResult<bool> {
        let resolved_id = match self.try_resolve_audio_file_id(audio_file_id)? {
            Some(id) => id,
            None => return Ok(false),
        };
        let uuid = Uuid::parse_str(&resolved_id)
            .map_err(|e| VoiceError::validation("audio_file_id", e.to_string()))?;
        let uuid_bytes = uuid.as_bytes().to_vec();
        let device_id = get_local_device_id();

        let updated = self.conn.execute(
            r#"
            UPDATE audio_files
            SET storage_provider = ?,
                storage_key = ?,
                storage_encrypted = ?,
                storage_uploaded_at = strftime('%s', 'now'),
                modified_at = strftime('%s', 'now'),
                device_id = ?
            WHERE id = ? AND deleted_at IS NULL
            "#,
            params![storage_provider, storage_key, encrypted as i64, device_id.as_bytes().to_vec(), uuid_bytes],
        )?;

        Ok(updated > 0)
    }

    /// Clear an audio file's cloud storage information.
    ///
    /// This marks the file as local-only (not uploaded to cloud).
    pub fn clear_audio_file_storage(&self, audio_file_id: &str) -> VoiceResult<bool> {
        let resolved_id = match self.try_resolve_audio_file_id(audio_file_id)? {
            Some(id) => id,
            None => return Ok(false),
        };
        let uuid = Uuid::parse_str(&resolved_id)
            .map_err(|e| VoiceError::validation("audio_file_id", e.to_string()))?;
        let uuid_bytes = uuid.as_bytes().to_vec();
        let device_id = get_local_device_id();

        let updated = self.conn.execute(
            r#"
            UPDATE audio_files
            SET storage_provider = NULL,
                storage_key = NULL,
                storage_uploaded_at = NULL,
                modified_at = strftime('%s', 'now'),
                device_id = ?
            WHERE id = ? AND deleted_at IS NULL
            "#,
            params![device_id.as_bytes().to_vec(), uuid_bytes],
        )?;

        Ok(updated > 0)
    }

    // ========================================================================
    // File Storage Configuration
    // ========================================================================

    /// Get the file storage configuration from the database.
    ///
    /// Returns None if no configuration has been set yet.
    pub fn get_file_storage_config(&self) -> VoiceResult<Option<serde_json::Value>> {
        let result = self.conn.query_row(
            "SELECT provider, config, modified_at, device_id FROM file_storage_config WHERE id = 'default'",
            [],
            |row| {
                let provider: String = row.get(0)?;
                let config: Option<String> = row.get(1)?;
                let modified_at: Option<i64> = row.get(2)?;
                let device_id_bytes: Option<Vec<u8>> = row.get(3)?;

                let config_value: serde_json::Value = config
                    .and_then(|c| serde_json::from_str(&c).ok())
                    .unwrap_or(serde_json::Value::Null);

                Ok(serde_json::json!({
                    "provider": provider,
                    "config": config_value,
                    "modified_at": modified_at,
                    "device_id": device_id_bytes.and_then(|b| uuid_bytes_to_hex(&b)),
                }))
            },
        );

        match result {
            Ok(val) => Ok(Some(val)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(VoiceError::Database(e)),
        }
    }

    /// Set the file storage configuration in the database.
    ///
    /// This uses INSERT OR REPLACE to create or update the single config row.
    pub fn set_file_storage_config(
        &self,
        provider: &str,
        config: Option<&serde_json::Value>,
    ) -> VoiceResult<()> {
        let device_id = get_local_device_id();
        let config_json = config.map(|c| c.to_string());

        self.conn.execute(
            r#"
            INSERT INTO file_storage_config (id, provider, config, modified_at, device_id)
            VALUES ('default', ?, ?, strftime('%s', 'now'), ?)
            ON CONFLICT(id) DO UPDATE SET
                provider = excluded.provider,
                config = excluded.config,
                modified_at = excluded.modified_at,
                device_id = excluded.device_id
            "#,
            params![provider, config_json, device_id.as_bytes().to_vec()],
        )?;

        Ok(())
    }

    /// Get file storage config as FileStorageConfig struct (for use with config.rs types).
    ///
    /// Returns a FileStorageConfig that can be used directly with the S3 storage service.
    pub fn get_file_storage_config_struct(&self) -> VoiceResult<crate::config::FileStorageConfig> {
        match self.get_file_storage_config()? {
            Some(val) => {
                let provider = val.get("provider")
                    .and_then(|v| v.as_str())
                    .unwrap_or("none")
                    .to_string();
                let config = val.get("config")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                Ok(crate::config::FileStorageConfig { provider, config })
            }
            None => Ok(crate::config::FileStorageConfig::default()),
        }
    }

    /// Apply file storage config from sync.
    ///
    /// This is used when receiving configuration from another device via sync.
    /// Uses LWW (Last Writer Wins) based on modified_at timestamp.
    pub fn apply_sync_file_storage_config(
        &self,
        provider: &str,
        config: Option<&serde_json::Value>,
        modified_at: Option<i64>,
        device_id: Option<&str>,
        sync_received_at: Option<i64>,
    ) -> VoiceResult<()> {
        let device_id_bytes = device_id
            .and_then(|id| Uuid::parse_str(id).ok())
            .map(|u| u.as_bytes().to_vec());
        let config_json = config.map(|c| c.to_string());

        // Check if we have existing config and compare timestamps
        let existing = self.conn.query_row(
            "SELECT modified_at FROM file_storage_config WHERE id = 'default'",
            [],
            |row| {
                let existing_modified_at: Option<i64> = row.get(0)?;
                Ok(existing_modified_at)
            },
        );

        let should_update = match existing {
            Ok(existing_modified_at) => {
                // LWW: Only update if incoming is newer or same
                match (modified_at, existing_modified_at) {
                    (Some(incoming), Some(existing)) => incoming >= existing,
                    (Some(_), None) => true,
                    (None, _) => true,
                }
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => true,
            Err(e) => return Err(VoiceError::Database(e)),
        };

        if should_update {
            self.conn.execute(
                r#"
                INSERT INTO file_storage_config (id, provider, config, modified_at, device_id, sync_received_at)
                VALUES ('default', ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    provider = excluded.provider,
                    config = excluded.config,
                    modified_at = excluded.modified_at,
                    device_id = excluded.device_id,
                    sync_received_at = excluded.sync_received_at
                "#,
                params![provider, config_json, modified_at, device_id_bytes, sync_received_at],
            )?;
        }

        Ok(())
    }

    // ========================================================================
    // Sync operations for NoteAttachment
    // ========================================================================

    /// Get raw note attachment data for sync (returns serde_json::Value)
    pub fn get_note_attachment_raw(&self, association_id: &str) -> VoiceResult<Option<serde_json::Value>> {
        let uuid = Uuid::parse_str(association_id)
            .map_err(|e| VoiceError::validation("association_id", e.to_string()))?;
        let uuid_bytes = uuid.as_bytes().to_vec();

        let mut stmt = self.conn.prepare(
            r#"
            SELECT id, note_id, attachment_id, attachment_type, created_at, device_id, modified_at, deleted_at
            FROM note_attachments
            WHERE id = ?
            "#,
        )?;

        let result = stmt.query_row([uuid_bytes], |row| {
            let id_bytes: Vec<u8> = row.get(0)?;
            let note_id_bytes: Vec<u8> = row.get(1)?;
            let attachment_id_bytes: Vec<u8> = row.get(2)?;
            let attachment_type: String = row.get(3)?;
            let created_at: i64 = row.get(4)?;
            let device_id_bytes: Vec<u8> = row.get(5)?;
            let modified_at: Option<i64> = row.get(6)?;
            let deleted_at: Option<i64> = row.get(7)?;

            Ok(serde_json::json!({
                "id": uuid_bytes_to_hex(&id_bytes).unwrap_or_default(),
                "note_id": uuid_bytes_to_hex(&note_id_bytes).unwrap_or_default(),
                "attachment_id": uuid_bytes_to_hex(&attachment_id_bytes).unwrap_or_default(),
                "attachment_type": attachment_type,
                "created_at": created_at,
                "device_id": uuid_bytes_to_hex(&device_id_bytes).unwrap_or_default(),
                "modified_at": modified_at,
                "deleted_at": deleted_at,
            }))
        });

        match result {
            Ok(val) => Ok(Some(val)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(VoiceError::Database(e)),
        }
    }

    /// Apply a note attachment from sync
    pub fn apply_sync_note_attachment(
        &self,
        id: &str,
        note_id: &str,
        attachment_id: &str,
        attachment_type: &str,
        created_at: i64,
        modified_at: Option<i64>,
        deleted_at: Option<i64>,
        sync_received_at: Option<i64>,
    ) -> VoiceResult<()> {
        let id_uuid = Uuid::parse_str(id)
            .map_err(|e| VoiceError::validation("id", e.to_string()))?;
        let note_uuid = validate_note_id(note_id)?;
        let attachment_uuid = Uuid::parse_str(attachment_id)
            .map_err(|e| VoiceError::validation("attachment_id", e.to_string()))?;
        let device_id = get_local_device_id();
        let id_hex = id_uuid.simple().to_string();

        // The link's target columns are not versioned (they only change when
        // notes are merged); deleted_at is owned by the membership version.
        self.conn.execute(
            r#"
            INSERT INTO note_attachments (id, note_id, attachment_id, attachment_type, created_at, device_id, modified_at, deleted_at, sync_received_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                -- The target changes only when notes are merged; an older row
                -- (an echo from a peer that has not seen the merge) must not
                -- move the attachment back. Two devices that merge the same
                -- attachment onto different notes within one second would
                -- otherwise each keep whichever row arrived last, so the tie
                -- is broken on the target itself: a rule every device computes
                -- the same way, whatever order the rows reach it in.
                note_id = CASE WHEN COALESCE(excluded.modified_at, 0) > COALESCE(note_attachments.modified_at, 0)
                                 OR (COALESCE(excluded.modified_at, 0) = COALESCE(note_attachments.modified_at, 0)
                                     AND excluded.note_id < note_attachments.note_id)
                               THEN excluded.note_id ELSE note_attachments.note_id END,
                attachment_id = CASE WHEN COALESCE(excluded.modified_at, 0) > COALESCE(note_attachments.modified_at, 0)
                                       OR (COALESCE(excluded.modified_at, 0) = COALESCE(note_attachments.modified_at, 0)
                                           AND excluded.note_id < note_attachments.note_id)
                                     THEN excluded.attachment_id ELSE note_attachments.attachment_id END,
                attachment_type = CASE WHEN COALESCE(excluded.modified_at, 0) > COALESCE(note_attachments.modified_at, 0)
                                         OR (COALESCE(excluded.modified_at, 0) = COALESCE(note_attachments.modified_at, 0)
                                             AND excluded.note_id < note_attachments.note_id)
                                       THEN excluded.attachment_type ELSE note_attachments.attachment_type END,
                modified_at = NULLIF(MAX(COALESCE(note_attachments.modified_at, 0), COALESCE(excluded.modified_at, 0)), 0),
                sync_received_at = COALESCE(excluded.sync_received_at, note_attachments.sync_received_at)
            "#,
            params![
                id_uuid.as_bytes().to_vec(),
                note_uuid.as_bytes().to_vec(),
                attachment_uuid.as_bytes().to_vec(),
                attachment_type,
                created_at,
                device_id.as_bytes().to_vec(),
                modified_at,
                deleted_at,
                sync_received_at,
            ],
        )?;

        // When the sender's target lost, it is holding a value this device has
        // already rejected, and nothing in its own row changed to tell it so.
        // Publishing this row again carries the winning target back, which
        // ends the disagreement: the loser adopts it and stops sending its own.
        let stored: Option<Vec<u8>> = self
            .conn
            .query_row(
                "SELECT note_id FROM note_attachments WHERE id = ?",
                params![id_uuid.as_bytes().to_vec()],
                |row| row.get(0),
            )
            .optional()?;
        if stored.as_deref() != Some(note_uuid.as_bytes().as_slice()) {
            self.republish("note_attachments", id_uuid.as_bytes())?;
        }

        let active = if deleted_at.is_some() { "0" } else { "1" };
        self.ensure_root_version(ENTITY_NOTE_ATTACHMENT, &id_hex, FIELD_ACTIVE, active, deleted_at.or(modified_at).unwrap_or(created_at))?;
        self.reapply_entity_heads(ENTITY_NOTE_ATTACHMENT, &id_hex)?;

        Ok(())
    }

    // ========================================================================
    // Sync operations for AudioFile
    // ========================================================================

    /// Get raw audio file data for sync (returns serde_json::Value)
    pub fn get_audio_file_raw(&self, audio_file_id: &str) -> VoiceResult<Option<serde_json::Value>> {
        let uuid = Uuid::parse_str(audio_file_id)
            .map_err(|e| VoiceError::validation("audio_file_id", e.to_string()))?;
        let uuid_bytes = uuid.as_bytes().to_vec();

        let mut stmt = self.conn.prepare(
            r#"
            SELECT id, imported_at, filename, file_created_at, summary, device_id, modified_at, deleted_at,
                   storage_provider, storage_key, storage_uploaded_at, content_sha256, storage_encrypted
            FROM audio_files
            WHERE id = ?
            "#,
        )?;

        let result = stmt.query_row([uuid_bytes], |row| {
            let id_bytes: Vec<u8> = row.get(0)?;
            let imported_at: i64 = row.get(1)?;
            let filename: String = row.get(2)?;
            let file_created_at: Option<i64> = row.get(3)?;
            let summary: Option<String> = row.get(4)?;
            let device_id_bytes: Vec<u8> = row.get(5)?;
            let modified_at: Option<i64> = row.get(6)?;
            let deleted_at: Option<i64> = row.get(7)?;
            let storage_provider: Option<String> = row.get(8)?;
            let storage_key: Option<String> = row.get(9)?;
            let storage_uploaded_at: Option<i64> = row.get(10)?;
            let content_sha256: Option<String> = row.get(11)?;
            let storage_encrypted: Option<i64> = row.get(12)?;

            Ok(serde_json::json!({
                "id": uuid_bytes_to_hex(&id_bytes).unwrap_or_default(),
                "imported_at": imported_at,
                "filename": filename,
                "file_created_at": file_created_at,
                "summary": summary,
                "device_id": uuid_bytes_to_hex(&device_id_bytes).unwrap_or_default(),
                "modified_at": modified_at,
                "deleted_at": deleted_at,
                "storage_provider": storage_provider,
                "storage_key": storage_key,
                "storage_uploaded_at": storage_uploaded_at,
                "content_sha256": content_sha256,
                "storage_encrypted": storage_encrypted.unwrap_or(0) != 0,
            }))
        });

        match result {
            Ok(val) => Ok(Some(val)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(VoiceError::Database(e)),
        }
    }

    /// Apply an audio file from sync
    pub fn apply_sync_audio_file(
        &self,
        id: &str,
        imported_at: i64,
        filename: &str,
        file_created_at: Option<i64>,
        duration_seconds: Option<i64>,
        summary: Option<&str>,
        modified_at: Option<i64>,
        deleted_at: Option<i64>,
        sync_received_at: Option<i64>,
        storage_provider: Option<&str>,
        storage_key: Option<&str>,
        storage_uploaded_at: Option<i64>,
        // Which transcription stands for this recording, when the sender
        // named one. A hint like every other row value (VER-4).
        primary_transcription_id: Option<&str>,
        // The offset the recording was made in, for its file name (Stage 13)
        file_created_at_offset: Option<i32>,
        // The content hash the sender knows (Stage 13); never erased by a row without one
        content_sha256: Option<&str>,
        // Whether the object is encrypted (Stage 15); travels with the storage key
        storage_encrypted: Option<bool>,
    ) -> VoiceResult<()> {
        let id_uuid = Uuid::parse_str(id)
            .map_err(|e| VoiceError::validation("id", e.to_string()))?;
        let device_id = get_local_device_id();
        // Named on arrival at the recording's own offset (Stage 13); kept once written
        let local_name = crate::models::recording_file_name(id, filename, file_created_at.unwrap_or(imported_at), file_created_at_offset);

        self.conn.execute(
            r#"
            INSERT INTO audio_files (id, imported_at, filename, file_created_at, duration_seconds, summary, device_id, modified_at, deleted_at, sync_received_at, storage_provider, storage_key, storage_uploaded_at, local_name, content_sha256, storage_encrypted)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                storage_encrypted = CASE WHEN excluded.storage_key IS NOT NULL
                                           AND (audio_files.storage_key IS NULL OR COALESCE(excluded.modified_at, 0) >= COALESCE(audio_files.modified_at, 0))
                                         THEN excluded.storage_encrypted ELSE audio_files.storage_encrypted END,
                local_name = COALESCE(NULLIF(audio_files.local_name, ''), excluded.local_name),
                content_sha256 = CASE WHEN excluded.content_sha256 IS NOT NULL
                                        AND (audio_files.content_sha256 IS NULL OR COALESCE(excluded.modified_at, 0) >= COALESCE(audio_files.modified_at, 0))
                                      THEN excluded.content_sha256 ELSE audio_files.content_sha256 END,
                -- Metadata written by the importing device: the newer row
                -- wins column by column, an older row fills in only what is
                -- missing here. summary and deleted_at are versioned: heads
                -- are reapplied below, so a row never writes them (writing
                -- them twice would publish the row again on every echo).
                filename = CASE WHEN COALESCE(excluded.modified_at, 0) >= COALESCE(audio_files.modified_at, 0)
                                THEN excluded.filename ELSE audio_files.filename END,
                file_created_at = CASE WHEN audio_files.file_created_at IS NULL
                                         OR (excluded.file_created_at IS NOT NULL AND COALESCE(excluded.modified_at, 0) >= COALESCE(audio_files.modified_at, 0))
                                       THEN excluded.file_created_at ELSE audio_files.file_created_at END,
                duration_seconds = CASE WHEN audio_files.duration_seconds IS NULL
                                          OR (excluded.duration_seconds IS NOT NULL AND COALESCE(excluded.modified_at, 0) >= COALESCE(audio_files.modified_at, 0))
                                        THEN excluded.duration_seconds ELSE audio_files.duration_seconds END,
                -- The cloud location is set once by the uploading device. It is
                -- never erased by a row without one (an echo, or a peer that
                -- edited the summary before receiving the upload), and only
                -- replaced by a newer row that has one (a re-upload).
                storage_provider = CASE WHEN excluded.storage_key IS NOT NULL
                                          AND (audio_files.storage_key IS NULL OR COALESCE(excluded.modified_at, 0) >= COALESCE(audio_files.modified_at, 0))
                                        THEN excluded.storage_provider ELSE audio_files.storage_provider END,
                storage_key = CASE WHEN excluded.storage_key IS NOT NULL
                                     AND (audio_files.storage_key IS NULL OR COALESCE(excluded.modified_at, 0) >= COALESCE(audio_files.modified_at, 0))
                                   THEN excluded.storage_key ELSE audio_files.storage_key END,
                storage_uploaded_at = CASE WHEN excluded.storage_key IS NOT NULL
                                             AND (audio_files.storage_key IS NULL OR COALESCE(excluded.modified_at, 0) >= COALESCE(audio_files.modified_at, 0))
                                           THEN excluded.storage_uploaded_at ELSE audio_files.storage_uploaded_at END,
                modified_at = NULLIF(MAX(COALESCE(audio_files.modified_at, 0), COALESCE(excluded.modified_at, 0)), 0),
                device_id = excluded.device_id,
                sync_received_at = COALESCE(excluded.sync_received_at, audio_files.sync_received_at)
            "#,
            params![
                id_uuid.as_bytes().to_vec(),
                imported_at,
                filename,
                file_created_at,
                duration_seconds,
                summary,
                device_id.as_bytes().to_vec(),
                modified_at,
                deleted_at,
                sync_received_at,
                storage_provider,
                storage_key,
                storage_uploaded_at,
                local_name,
                content_sha256,
                storage_encrypted.unwrap_or(false) as i64,
            ],
        )?;

        let ts = modified_at.unwrap_or(imported_at);
        if let Some(s) = summary {
            self.ensure_root_version(ENTITY_AUDIO_FILE, id, FIELD_SUMMARY, s, ts)?;
        }
        if let Some(primary) = primary_transcription_id.filter(|p| !p.is_empty()) {
            self.ensure_root_version(ENTITY_AUDIO_FILE, id, FIELD_PRIMARY_TRANSCRIPTION, primary, ts)?;
        }
        if let Some(d) = deleted_at {
            self.ensure_root_version(ENTITY_AUDIO_FILE, id, FIELD_DELETED, "1", d)?;
        }
        self.reapply_entity_heads(ENTITY_AUDIO_FILE, id)?;

        Ok(())
    }

    /// Apply a synced transcription (insert or update)
    pub fn apply_sync_transcription(
        &self,
        id: &str,
        audio_file_id: &str,
        content: &str,
        content_segments: Option<&str>,
        service: &str,
        service_arguments: Option<&str>,
        service_response: Option<&str>,
        state: &str,
        device_id: &str,
        created_at: i64,
        modified_at: Option<i64>,
        deleted_at: Option<i64>,
        sync_received_at: Option<i64>,
    ) -> VoiceResult<()> {
        let id_uuid = Uuid::parse_str(id)
            .map_err(|e| VoiceError::validation("id", e.to_string()))?;
        let audio_file_uuid = Uuid::parse_str(audio_file_id)
            .map_err(|e| VoiceError::validation("audio_file_id", e.to_string()))?;
        let device_uuid = Uuid::parse_str(device_id)
            .map_err(|e| VoiceError::validation("device_id", e.to_string()))?;
        let id_hex = id_uuid.simple().to_string();

        // Service metadata is not versioned; content, state and deletion are.
        self.conn.execute(
            r#"
            INSERT INTO transcriptions (id, audio_file_id, content, content_segments, service, service_arguments, service_response, state, device_id, created_at, modified_at, deleted_at, sync_received_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                -- Service metadata is not versioned: a newer row replaces it,
                -- an older row (an echo, or a peer that has not received a
                -- re-run yet) only fills in what is missing here.
                audio_file_id = excluded.audio_file_id,
                content_segments = CASE WHEN transcriptions.content_segments IS NULL
                                          OR (excluded.content_segments IS NOT NULL AND COALESCE(excluded.modified_at, 0) >= COALESCE(transcriptions.modified_at, 0))
                                        THEN excluded.content_segments ELSE transcriptions.content_segments END,
                service = CASE WHEN COALESCE(excluded.modified_at, 0) >= COALESCE(transcriptions.modified_at, 0)
                               THEN excluded.service ELSE transcriptions.service END,
                service_arguments = CASE WHEN transcriptions.service_arguments IS NULL
                                           OR (excluded.service_arguments IS NOT NULL AND COALESCE(excluded.modified_at, 0) >= COALESCE(transcriptions.modified_at, 0))
                                         THEN excluded.service_arguments ELSE transcriptions.service_arguments END,
                service_response = CASE WHEN transcriptions.service_response IS NULL
                                          OR (excluded.service_response IS NOT NULL AND COALESCE(excluded.modified_at, 0) >= COALESCE(transcriptions.modified_at, 0))
                                        THEN excluded.service_response ELSE transcriptions.service_response END,
                modified_at = NULLIF(MAX(COALESCE(transcriptions.modified_at, 0), COALESCE(excluded.modified_at, 0)), 0),
                sync_received_at = COALESCE(excluded.sync_received_at, transcriptions.sync_received_at)
            "#,
            params![
                id_uuid.as_bytes().to_vec(),
                audio_file_uuid.as_bytes().to_vec(),
                content,
                content_segments,
                service,
                service_arguments,
                service_response,
                state,
                device_uuid.as_bytes().to_vec(),
                created_at,
                modified_at,
                deleted_at,
                sync_received_at,
            ],
        )?;

        let ts = modified_at.unwrap_or(created_at);
        self.ensure_root_version(ENTITY_TRANSCRIPTION, &id_hex, FIELD_CONTENT, content, ts)?;
        self.ensure_root_version(ENTITY_TRANSCRIPTION, &id_hex, FIELD_STATE, state, ts)?;
        if let Some(d) = deleted_at {
            self.ensure_root_version(ENTITY_TRANSCRIPTION, &id_hex, FIELD_DELETED, "1", d)?;
        }
        self.reapply_entity_heads(ENTITY_TRANSCRIPTION, &id_hex)?;

        Ok(())
    }

    /// Get a transcription as raw JSON (for sync conflict detection)
    pub fn get_transcription_raw(&self, transcription_id: &str) -> VoiceResult<Option<serde_json::Value>> {
        let uuid = Uuid::parse_str(transcription_id)
            .map_err(|e| VoiceError::validation("transcription_id", e.to_string()))?;
        let uuid_bytes = uuid.as_bytes().to_vec();

        let mut stmt = self.conn.prepare(
            r#"
            SELECT id, audio_file_id, content, content_segments, service, service_arguments, service_response, state, device_id, created_at, modified_at, deleted_at,
                   created_at_offset, created_at_zone
            FROM transcriptions
            WHERE id = ?
            "#,
        )?;

        let result = stmt.query_row([uuid_bytes], |row| {
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

            Ok(serde_json::json!({
                "id": uuid_bytes_to_hex(&id_bytes).unwrap_or_default(),
                "audio_file_id": uuid_bytes_to_hex(&audio_file_id_bytes).unwrap_or_default(),
                "content": content,
                "content_segments": content_segments,
                "service": service,
                "service_arguments": service_arguments,
                "service_response": service_response,
                "state": state,
                "device_id": uuid_bytes_to_hex(&device_id_bytes).unwrap_or_default(),
                "created_at": created_at,
                "modified_at": modified_at,
                "deleted_at": deleted_at,
            }))
        });

        match result {
            Ok(val) => Ok(Some(val)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(VoiceError::Database(e)),
        }
    }

    // ========================================================================
    // Transcription methods
    // ========================================================================

    /// Create a new transcription for an audio file
    pub fn create_transcription(
        &self,
        audio_file_id: &str,
        content: &str,
        content_segments: Option<&str>,
        service: &str,
        service_arguments: Option<&str>,
        service_response: Option<&str>,
        state: Option<&str>,
    ) -> VoiceResult<String> {
        let id = Uuid::now_v7();
        let device_id = get_local_device_id();
        let now = Utc::now().timestamp();
        let state = state.unwrap_or(DEFAULT_TRANSCRIPTION_STATE);

        let audio_file_uuid = Uuid::parse_str(audio_file_id)
            .map_err(|e| VoiceError::validation("audio_file_id", e.to_string()))?;

        self.conn.execute(
            r#"
            INSERT INTO transcriptions (id, audio_file_id, content, content_segments, service, service_arguments, service_response, state, device_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
            params![
                id.as_bytes().to_vec(),
                audio_file_uuid.as_bytes().to_vec(),
                content,
                content_segments,
                service,
                service_arguments,
                service_response,
                state,
                device_id.as_bytes().to_vec(),
                now,
            ],
        )?;

        let id_hex = id.simple().to_string();
        let _ = self.stamp_local_zone("transcriptions", &id_hex, "created_at");
        self.init_field(ENTITY_TRANSCRIPTION, &id_hex, FIELD_CONTENT, content)?;
        self.init_field(ENTITY_TRANSCRIPTION, &id_hex, FIELD_STATE, state)?;

        // Rebuild display cache for associated note(s)
        self.rebuild_caches_for_audio_file(audio_file_id);

        Ok(id_hex)
    }

    /// Get a transcription by ID
    pub fn get_transcription(&self, transcription_id: &str) -> VoiceResult<Option<TranscriptionRow>> {
        let id_uuid = Uuid::parse_str(transcription_id)
            .map_err(|e| VoiceError::validation("transcription_id", e.to_string()))?;

        let result = self.conn.query_row(
            r#"
            SELECT id, audio_file_id, content, content_segments, service, service_arguments, service_response, state, device_id, created_at, modified_at, deleted_at,
                   created_at_offset, created_at_zone
            FROM transcriptions
            WHERE id = ? AND deleted_at IS NULL
            "#,
            params![id_uuid.as_bytes().to_vec()],
            |row| {
                let id_bytes: Vec<u8> = row.get(0)?;
                let audio_file_id_bytes: Vec<u8> = row.get(1)?;
                let device_id_bytes: Vec<u8> = row.get(8)?;
                Ok(TranscriptionRow {
                    id: uuid_bytes_to_hex(&id_bytes).unwrap_or_default(),
                    audio_file_id: uuid_bytes_to_hex(&audio_file_id_bytes).unwrap_or_default(),
                    content: row.get(2)?,
                    content_segments: row.get(3)?,
                    service: row.get(4)?,
                    service_arguments: row.get(5)?,
                    service_response: row.get(6)?,
                    state: row.get(7)?,
                    device_id: uuid_bytes_to_hex(&device_id_bytes).unwrap_or_default(),
                    created_at: row.get(9)?,
                    modified_at: row.get(10)?,
                    deleted_at: row.get(11)?,
                    created_at_offset: row.get::<_, Option<i64>>(12)?.and_then(|o| i32::try_from(o).ok()),
                    created_at_zone: row.get(13)?,
                })
            },
        );

        match result {
            Ok(transcription) => Ok(Some(transcription)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(VoiceError::Database(e)),
        }
    }

    /// Get all transcriptions for an audio file
    pub fn get_transcriptions_for_audio_file(&self, audio_file_id: &str) -> VoiceResult<Vec<TranscriptionRow>> {
        let audio_file_uuid = Uuid::parse_str(audio_file_id)
            .map_err(|e| VoiceError::validation("audio_file_id", e.to_string()))?;

        let mut stmt = self.conn.prepare(
            r#"
            SELECT id, audio_file_id, content, content_segments, service, service_arguments, service_response, state, device_id, created_at, modified_at, deleted_at,
                   created_at_offset, created_at_zone
            FROM transcriptions
            WHERE audio_file_id = ? AND deleted_at IS NULL
            ORDER BY created_at DESC
            "#,
        )?;

        let rows = stmt.query_map(params![audio_file_uuid.as_bytes().to_vec()], |row| {
            let id_bytes: Vec<u8> = row.get(0)?;
            let audio_file_id_bytes: Vec<u8> = row.get(1)?;
            let device_id_bytes: Vec<u8> = row.get(8)?;
            Ok(TranscriptionRow {
                id: uuid_bytes_to_hex(&id_bytes).unwrap_or_default(),
                audio_file_id: uuid_bytes_to_hex(&audio_file_id_bytes).unwrap_or_default(),
                content: row.get(2)?,
                content_segments: row.get(3)?,
                service: row.get(4)?,
                service_arguments: row.get(5)?,
                service_response: row.get(6)?,
                state: row.get(7)?,
                device_id: uuid_bytes_to_hex(&device_id_bytes).unwrap_or_default(),
                created_at: row.get(9)?,
                modified_at: row.get(10)?,
                deleted_at: row.get(11)?,
                created_at_offset: row.get::<_, Option<i64>>(12)?.and_then(|o| i32::try_from(o).ok()),
                created_at_zone: row.get(13)?,
            })
        })?;

        let mut transcriptions = Vec::new();
        for row in rows {
            transcriptions.push(row?);
        }

        Ok(transcriptions)
    }

    /// The most recent transcriptions, newest first.
    ///
    /// What a transcription queue view shows once the work is done: every
    /// finished and failed transcription in the order it was asked for, with
    /// its `service_response`, which is where the length of the recording and
    /// the clock, processor and memory cost of the work are recorded.
    ///
    /// `service` narrows it to one transcription service (`local_whisper` for
    /// work done on this device); `None` returns every service. `limit` is a
    /// screenful, not a history: a queue view shows the last few dozen.
    pub fn get_recent_transcriptions(
        &self,
        service: Option<&str>,
        limit: u32,
    ) -> VoiceResult<Vec<TranscriptionRow>> {
        let sql = format!(
            r#"
            SELECT id, audio_file_id, content, content_segments, service, service_arguments, service_response, state, device_id, created_at, modified_at, deleted_at,
                   created_at_offset, created_at_zone
            FROM transcriptions
            WHERE deleted_at IS NULL {}
            ORDER BY created_at DESC, rowid DESC
            LIMIT ?
            "#,
            if service.is_some() { "AND service = ?" } else { "" }
        );
        let mut stmt = self.conn.prepare(&sql)?;

        let read = |row: &rusqlite::Row| -> rusqlite::Result<TranscriptionRow> {
            let id_bytes: Vec<u8> = row.get(0)?;
            let audio_file_id_bytes: Vec<u8> = row.get(1)?;
            let device_id_bytes: Vec<u8> = row.get(8)?;
            Ok(TranscriptionRow {
                id: uuid_bytes_to_hex(&id_bytes).unwrap_or_default(),
                audio_file_id: uuid_bytes_to_hex(&audio_file_id_bytes).unwrap_or_default(),
                content: row.get(2)?,
                content_segments: row.get(3)?,
                service: row.get(4)?,
                service_arguments: row.get(5)?,
                service_response: row.get(6)?,
                state: row.get(7)?,
                device_id: uuid_bytes_to_hex(&device_id_bytes).unwrap_or_default(),
                created_at: row.get(9)?,
                modified_at: row.get(10)?,
                deleted_at: row.get(11)?,
                created_at_offset: row.get::<_, Option<i64>>(12)?.and_then(|o| i32::try_from(o).ok()),
                created_at_zone: row.get(13)?,
            })
        };

        let mut transcriptions = Vec::new();
        match service {
            Some(name) => {
                let rows = stmt.query_map(params![name, limit], read)?;
                for row in rows {
                    transcriptions.push(row?);
                }
            }
            None => {
                let rows = stmt.query_map(params![limit], read)?;
                for row in rows {
                    transcriptions.push(row?);
                }
            }
        }

        Ok(transcriptions)
    }

    /// Delete a transcription (soft delete)
    pub fn delete_transcription(&self, transcription_id: &str) -> VoiceResult<bool> {
        let id_uuid = Uuid::parse_str(transcription_id)
            .map_err(|e| VoiceError::validation("transcription_id", e.to_string()))?;
        let alive: Option<i64> = self
            .conn
            .query_row(
                "SELECT 1 FROM transcriptions WHERE id = ? AND deleted_at IS NULL",
                params![id_uuid.as_bytes().to_vec()],
                |row| row.get(0),
            )
            .optional()?;
        if alive.is_none() {
            return Ok(false);
        }
        self.set_deleted(ENTITY_TRANSCRIPTION, &id_uuid.simple().to_string())
    }

    /// Update a transcription's content, state, and service response
    ///
    /// Used to update a pending transcription after the transcription completes,
    /// or when the user edits the transcription content or state.
    pub fn update_transcription(
        &self,
        transcription_id: &str,
        content: &str,
        content_segments: Option<&str>,
        service_response: Option<&str>,
        state: Option<&str>,
    ) -> VoiceResult<bool> {
        let id_uuid = Uuid::parse_str(transcription_id)
            .map_err(|e| VoiceError::validation("transcription_id", e.to_string()))?;
        let id_hex = id_uuid.simple().to_string();

        // Non-versioned service metadata: None leaves a value alone (a state
        // toggle from the UI must not erase the segments or the service
        // response), and a real change stamps modified_at so that peers take
        // the newer value over an older echo.
        let count = self.conn.execute(
            r#"
            UPDATE transcriptions
            SET modified_at = CASE
                    WHEN (?1 IS NOT NULL AND ?1 IS NOT content_segments) OR (?2 IS NOT NULL AND ?2 IS NOT service_response)
                    THEN strftime('%s', 'now') ELSE modified_at END,
                content_segments = COALESCE(?1, content_segments),
                service_response = COALESCE(?2, service_response)
            WHERE id = ?3 AND deleted_at IS NULL
            "#,
            params![content_segments, service_response, id_uuid.as_bytes().to_vec()],
        )?;
        if count == 0 {
            return Ok(false);
        }

        // Versioned text and flags
        self.set_field(ENTITY_TRANSCRIPTION, &id_hex, FIELD_CONTENT, content, None)?;
        if let Some(state) = state {
            self.set_field(ENTITY_TRANSCRIPTION, &id_hex, FIELD_STATE, state, None)?;
        }

        // Rebuild display cache for associated note(s)
        if let Ok(Some(transcription)) = self.get_transcription(&id_hex) {
            self.rebuild_caches_for_audio_file(&transcription.audio_file_id);
        }

        Ok(true)
    }

    // =========================================================================
    // Note Display Cache
    // =========================================================================

    /// Rebuild the display cache for a single note.
    ///
    /// The cache stores pre-computed data needed for the Note pane display:
    /// - tags (with full paths)
    /// - conflicts
    /// - attachments with audio files and transcriptions
    ///
    /// This should be called after any mutation that affects the note's display.
    pub fn rebuild_note_cache(&self, note_id: &str) -> VoiceResult<()> {
        let resolved_id = self.resolve_note_id(note_id)?;
        let note_uuid = Uuid::parse_str(&resolved_id)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?;
        let note_bytes = note_uuid.as_bytes().to_vec();

        // 1. Get tags for this note with full paths
        let tags = self.get_note_tags_with_paths(&resolved_id)?;

        // 2. Get conflict types for this note
        let conflicts = self.get_note_conflict_types(&resolved_id)?;

        // 3. Get attachments with audio files and transcriptions
        let attachments = self.get_note_attachments_for_cache(&resolved_id)?;

        // 4. Build JSON cache
        let cache = serde_json::json!({
            "tags": tags,
            "conflicts": conflicts,
            "attachments": attachments,
            "cached_at": Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()
        });

        let cache_str = serde_json::to_string(&cache)
            .map_err(|e| VoiceError::Other(format!("Failed to serialize cache: {}", e)))?;

        // 5. Update the cache column
        self.conn.execute(
            "UPDATE notes SET di_cache_note_pane_display = ? WHERE id = ?",
            params![cache_str, note_bytes],
        )?;

        Ok(())
    }

    /// Rebuild the list pane display cache for a specific note.
    ///
    /// The cache contains pre-computed data for the notes list pane:
    /// - date: created_at timestamp
    /// - marked: whether the note has the _system/_marked tag
    /// - content_preview: first 200 characters of content
    ///
    /// This should be called after any mutation that affects the note's list display.
    pub fn rebuild_note_list_cache(&self, note_id: &str) -> VoiceResult<()> {
        let resolved_id = self.resolve_note_id(note_id)?;
        let note_uuid = Uuid::parse_str(&resolved_id)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?;
        let note_bytes = note_uuid.as_bytes().to_vec();

        // 1. Get note's created_at (Unix timestamp) and content
        let (created_at, content): (i64, String) = self.conn.query_row(
            "SELECT created_at, content FROM notes WHERE id = ? AND deleted_at IS NULL",
            params![&note_bytes],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).map_err(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => VoiceError::not_found("note", note_id),
            _ => VoiceError::Database(e),
        })?;

        // 2. Check if note is marked (has _system/_marked tag)
        let is_marked = self.is_note_marked(&resolved_id)?;

        // 3. Get content preview (first 200 chars, newlines replaced with spaces)
        let content_preview: String = content
            .chars()
            .take(200)
            .collect::<String>()
            .replace('\n', " ")
            .replace('\r', "");

        // 4. Get total duration of attached audio files (in seconds)
        let total_duration: Option<i64> = self.conn.query_row(
            r#"
            SELECT SUM(af.duration_seconds)
            FROM note_attachments na
            JOIN audio_files af ON na.attachment_id = af.id
            WHERE na.note_id = ?
              AND na.deleted_at IS NULL
              AND af.deleted_at IS NULL
              AND af.duration_seconds IS NOT NULL
            "#,
            params![&note_bytes],
            |row| row.get(0),
        ).unwrap_or(None);

        // 5. Get tags for this note (excluding system tags starting with _)
        let mut tag_stmt = self.conn.prepare(
            r#"
            SELECT t.id, t.name, t.parent_id
            FROM tags t
            JOIN note_tags nt ON t.id = nt.tag_id
            WHERE nt.note_id = ?
              AND nt.deleted_at IS NULL
              AND t.deleted_at IS NULL
              AND t.name NOT LIKE '\_%' ESCAPE '\'
            ORDER BY t.name
            "#,
        )?;
        let tag_rows = tag_stmt.query_map(params![&note_bytes], |row| {
            let id_bytes: Vec<u8> = row.get(0)?;
            let name: String = row.get(1)?;
            let parent_id: Option<Vec<u8>> = row.get(2)?;
            Ok((id_bytes, name, parent_id))
        })?;

        let mut tag_display_names: Vec<String> = Vec::new();
        for row in tag_rows {
            let (_id_bytes, name, parent_id) = row?;
            let display_name = self.get_tag_display_name(&name, parent_id.as_deref())?;
            tag_display_names.push(display_name);
        }

        // 6. Build JSON cache
        // The date reads as the clock read where the note was made, so a note
        // written at 15:20 in Jerusalem still says 15:20 after the user flies
        // to New York. Notes with no zone recorded fall back to this device's.
        let created_offset: Option<i64> = self.conn
            .query_row(
                "SELECT created_at_offset FROM notes WHERE id = ?",
                params![&note_bytes],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        let date_display = crate::timezone::format_at_offset(created_at, created_offset.and_then(|o| i32::try_from(o).ok()));
        let cache = serde_json::json!({
            "date": date_display,
            "marked": is_marked,
            "content_preview": content_preview,
            "duration_seconds": total_duration,
            "tags": tag_display_names,
            "cached_at": Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()
        });

        let cache_str = serde_json::to_string(&cache)
            .map_err(|e| VoiceError::Other(format!("Failed to serialize list cache: {}", e)))?;

        // 5. Update the cache column
        self.conn.execute(
            "UPDATE notes SET di_cache_note_list_pane_display = ? WHERE id = ?",
            params![cache_str, note_bytes],
        )?;

        Ok(())
    }

    /// Rebuild the list pane display cache for all notes.
    ///
    /// Returns the number of notes processed.
    pub fn rebuild_all_note_list_caches(&self) -> VoiceResult<u32> {
        // Get all non-deleted note IDs
        let mut stmt = self.conn.prepare(
            "SELECT id FROM notes WHERE deleted_at IS NULL"
        )?;

        let note_ids: Vec<String> = stmt
            .query_map([], |row| {
                let id_bytes: Vec<u8> = row.get(0)?;
                Ok(uuid_bytes_to_hex(&id_bytes).unwrap_or_default())
            })?
            .filter_map(|r| r.ok())
            .collect();

        let count = note_ids.len() as u32;

        for note_id in note_ids {
            if let Err(e) = self.rebuild_note_list_cache(&note_id) {
                // Log error but continue with other notes
                eprintln!("Warning: Failed to rebuild list cache for note {}: {}", note_id, e);
            }
        }

        Ok(count)
    }

    /// Rebuild the display cache for all notes.
    ///
    /// Returns the number of notes processed.
    pub fn rebuild_all_note_caches(&self) -> VoiceResult<u32> {
        // Get all non-deleted note IDs
        let mut stmt = self.conn.prepare(
            "SELECT id FROM notes WHERE deleted_at IS NULL"
        )?;

        let note_ids: Vec<String> = stmt
            .query_map([], |row| {
                let id_bytes: Vec<u8> = row.get(0)?;
                Ok(uuid_bytes_to_hex(&id_bytes).unwrap_or_default())
            })?
            .filter_map(|r| r.ok())
            .collect();

        let count = note_ids.len() as u32;

        for note_id in note_ids {
            if let Err(e) = self.rebuild_note_cache(&note_id) {
                // Log error but continue with other notes
                eprintln!("Warning: Failed to rebuild cache for note {}: {}", note_id, e);
            }
        }

        Ok(count)
    }

    /// Rebuild ALL cache fields for a single note.
    ///
    /// This rebuilds every cache column listed in CACHE_REGISTRY for the given note.
    /// Currently rebuilds: di_cache_note_pane_display, di_cache_note_list_pane_display
    pub fn rebuild_all_caches_for_note(&self, note_id: &str) -> VoiceResult<()> {
        // Rebuild note pane cache
        self.rebuild_note_cache(note_id)?;
        // Rebuild list pane cache
        self.rebuild_note_list_cache(note_id)?;
        Ok(())
    }

    /// Rebuild ALL cache fields for all notes in the database.
    ///
    /// This rebuilds every cache column listed in CACHE_REGISTRY.
    /// Returns a summary of notes processed.
    pub fn rebuild_all_database_caches(&self) -> VoiceResult<CacheRebuildSummary> {
        // Get all non-deleted note IDs
        let mut stmt = self.conn.prepare(
            "SELECT id FROM notes WHERE deleted_at IS NULL"
        )?;

        let note_ids: Vec<String> = stmt
            .query_map([], |row| {
                let id_bytes: Vec<u8> = row.get(0)?;
                Ok(uuid_bytes_to_hex(&id_bytes).unwrap_or_default())
            })?
            .filter_map(|r| r.ok())
            .collect();

        let total_notes = note_ids.len() as u32;
        let mut errors: Vec<String> = Vec::new();

        for note_id in &note_ids {
            // Rebuild all caches for this note
            if let Err(e) = self.rebuild_note_cache(note_id) {
                errors.push(format!("note_pane_cache for {}: {}", note_id, e));
            }
            if let Err(e) = self.rebuild_note_list_cache(note_id) {
                errors.push(format!("note_list_cache for {}: {}", note_id, e));
            }
        }

        Ok(CacheRebuildSummary {
            notes_processed: total_notes,
            cache_fields_rebuilt: CACHE_REGISTRY.len() as u32,
            errors,
        })
    }

    /// Get information about all registered cache fields.
    pub fn get_cache_registry_info() -> Vec<CacheFieldInfo> {
        CACHE_REGISTRY.to_vec()
    }

    /// Rebuild display caches for all notes associated with an audio file.
    ///
    /// This is called when transcriptions or audio file metadata are created/updated/deleted.
    /// Rebuild the caches of every note that carries this tag, or any tag
    /// beneath it.
    ///
    /// A note's cache holds the name of each tag on it, so renaming a tag
    /// leaves every one of those notes showing the old name until something
    /// else happens to them. Reparenting reaches further still: the path of
    /// every tag *below* the moved one changes too, so their notes are
    /// rebuilt as well.
    ///
    /// The walk down is bounded: a parent chain that loops (two devices each
    /// moving one tag under the other, merged) would otherwise never end.
    pub(crate) fn rebuild_caches_for_tag(&self, tag_id: &str) {
        let tag_uuid = match Uuid::parse_str(tag_id) {
            Ok(u) => u,
            Err(_) => return,
        };

        let note_ids: Vec<String> = match self.conn.prepare(
            r#"
            WITH RECURSIVE subtree(id, depth) AS (
                SELECT ?, 0
                UNION
                SELECT t.id, subtree.depth + 1
                FROM tags t
                JOIN subtree ON t.parent_id = subtree.id
                WHERE subtree.depth < 64
            )
            SELECT DISTINCT nt.note_id
            FROM note_tags nt
            JOIN subtree ON nt.tag_id = subtree.id
            WHERE nt.deleted_at IS NULL
            "#,
        ) {
            Ok(mut stmt) => stmt
                .query_map(params![tag_uuid.as_bytes().to_vec()], |row| {
                    let id_bytes: Vec<u8> = row.get(0)?;
                    Ok(uuid_bytes_to_hex(&id_bytes).unwrap_or_default())
                })
                .ok()
                .map(|rows| rows.filter_map(|r| r.ok()).collect())
                .unwrap_or_default(),
            Err(_) => return,
        };

        for note_id in note_ids {
            let _ = self.rebuild_note_cache(&note_id);
            let _ = self.rebuild_note_list_cache(&note_id);
        }
    }

    pub(crate) fn rebuild_caches_for_audio_file(&self, audio_file_id: &str) {
        // Find all notes that have this audio file attached
        let audio_uuid = match Uuid::parse_str(audio_file_id) {
            Ok(u) => u,
            Err(_) => return,
        };
        let audio_bytes = audio_uuid.as_bytes().to_vec();

        let note_ids: Vec<String> = match self.conn.prepare(
            r#"
            SELECT DISTINCT na.note_id
            FROM note_attachments na
            WHERE na.attachment_id = ? AND na.deleted_at IS NULL
            "#,
        ) {
            Ok(mut stmt) => {
                stmt.query_map(params![audio_bytes], |row| {
                    let id_bytes: Vec<u8> = row.get(0)?;
                    Ok(uuid_bytes_to_hex(&id_bytes).unwrap_or_default())
                })
                .ok()
                .map(|rows| rows.filter_map(|r| r.ok()).collect())
                .unwrap_or_default()
            }
            Err(_) => return,
        };

        for note_id in note_ids {
            let _ = self.rebuild_note_cache(&note_id);
        }
    }

    /// Get tags for a note with display names (minimal paths handling ambiguity).
    fn get_note_tags_with_paths(&self, note_id: &str) -> VoiceResult<Vec<serde_json::Value>> {
        let note_uuid = Uuid::parse_str(note_id)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?;
        let note_bytes = note_uuid.as_bytes().to_vec();

        let mut stmt = self.conn.prepare(
            r#"
            SELECT t.id, t.name, t.parent_id
            FROM tags t
            INNER JOIN note_tags nt ON t.id = nt.tag_id
            WHERE nt.note_id = ? AND nt.deleted_at IS NULL AND t.deleted_at IS NULL
              AND t.name NOT LIKE '\_%' ESCAPE '\'
            "#
        )?;

        let tags: Vec<(Vec<u8>, String, Option<Vec<u8>>)> = stmt
            .query_map(params![note_bytes], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            .filter_map(|r| r.ok())
            .collect();

        // Build display names for each tag (minimal path handling ambiguity)
        let mut result = Vec::new();
        for (id_bytes, name, parent_id) in tags {
            let tag_id = uuid_bytes_to_hex(&id_bytes).unwrap_or_default();
            let display_name = self.get_tag_display_name(&name, parent_id.as_deref())?;
            result.push(serde_json::json!({
                "id": tag_id,
                "name": name,
                "display_name": display_name
            }));
        }

        Ok(result)
    }

    /// Get the minimal display name for a tag, adding parent prefixes only if ambiguous.
    ///
    /// If the tag name is unique, returns just the name (e.g., "Paris").
    /// If ambiguous, adds parent prefixes recursively until unique (e.g., "France/Paris").
    fn get_tag_display_name(&self, tag_name: &str, parent_id: Option<&[u8]>) -> VoiceResult<String> {
        // Check if tag name is ambiguous
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM tags WHERE name = ? COLLATE NOCASE AND deleted_at IS NULL",
            params![tag_name],
            |row| row.get(0),
        )?;

        if count <= 1 {
            // Name is unique, just return it
            return Ok(tag_name.to_string());
        }

        // Name is ambiguous, need to add parent prefix
        // Build path going up until we have a unique path
        let mut path_parts = vec![tag_name.to_string()];
        let mut current_parent = parent_id.map(|p| p.to_vec());

        while let Some(parent_bytes) = current_parent {
            let result: Option<(String, Option<Vec<u8>>)> = self.conn.query_row(
                "SELECT name, parent_id FROM tags WHERE id = ? AND deleted_at IS NULL",
                params![&parent_bytes],
                |row| Ok((row.get(0)?, row.get(1)?)),
            ).optional()?;

            if let Some((parent_name, grandparent_id)) = result {
                path_parts.push(parent_name.clone());

                // Check if current path is now unique
                let current_path: String = {
                    let mut tmp = path_parts.clone();
                    tmp.reverse();
                    tmp.join("/")
                };

                // Count how many tags end with this path
                let path_count: i64 = self.conn.query_row(
                    r#"
                    WITH RECURSIVE tag_paths AS (
                        SELECT id, name, parent_id, name as path, 0 AS depth
                        FROM tags WHERE deleted_at IS NULL
                        UNION ALL
                        SELECT t.id, t.name, t.parent_id, p.name || '/' || tp.path, tp.depth + 1
                        FROM tags t
                        JOIN tag_paths tp ON t.id = tp.parent_id
                        JOIN tags p ON t.id = p.id
                        WHERE t.deleted_at IS NULL AND tp.depth < 64
                    )
                    SELECT COUNT(*) FROM tag_paths WHERE path = ? COLLATE NOCASE
                    "#,
                    params![&current_path],
                    |row| row.get(0),
                ).unwrap_or(0);

                if path_count <= 1 {
                    // Path is now unique
                    break;
                }

                current_parent = grandparent_id;
            } else {
                break;
            }
        }

        path_parts.reverse();
        Ok(path_parts.join("/"))
    }

    /// Get attachments for a note with audio file details and transcriptions for cache.
    fn get_note_attachments_for_cache(&self, note_id: &str) -> VoiceResult<Vec<serde_json::Value>> {
        let note_uuid = Uuid::parse_str(note_id)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?;
        let note_bytes = note_uuid.as_bytes().to_vec();

        // Get all attachments for this note
        let mut stmt = self.conn.prepare(
            r#"
            SELECT na.id, na.attachment_id, na.attachment_type
            FROM note_attachments na
            LEFT JOIN audio_files af ON af.id = na.attachment_id
            WHERE na.note_id = ? AND na.deleted_at IS NULL
            -- Oldest first, like everywhere else a note's recordings are
            -- listed, with the id breaking a tie so every device agrees.
            ORDER BY COALESCE(af.file_created_at, af.imported_at, na.created_at) ASC, na.id ASC
            "#
        )?;

        let attachments: Vec<(Vec<u8>, Vec<u8>, String)> = stmt
            .query_map(params![note_bytes], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            .filter_map(|r| r.ok())
            .collect();

        let mut result = Vec::new();
        for (attach_id_bytes, attachment_id_bytes, attachment_type) in attachments {
            let attach_id = uuid_bytes_to_hex(&attach_id_bytes).unwrap_or_default();
            let attachment_id = uuid_bytes_to_hex(&attachment_id_bytes).unwrap_or_default();

            let mut item = serde_json::json!({
                "id": attach_id,
                "type": attachment_type
            });

            if attachment_type == "audio_file" {
                if let Some(audio_data) = self.get_audio_file_for_cache(&attachment_id)? {
                    item["audio_file"] = audio_data;
                }
            }

            result.push(item);
        }

        Ok(result)
    }

    /// Get audio file details for cache including transcriptions.
    fn get_audio_file_for_cache(&self, audio_id: &str) -> VoiceResult<Option<serde_json::Value>> {
        let audio_uuid = Uuid::parse_str(audio_id)
            .map_err(|e| VoiceError::validation("audio_id", e.to_string()))?;
        let audio_bytes = audio_uuid.as_bytes().to_vec();

        // Note: imported_at and file_created_at are INTEGER (Unix timestamps)
        let audio: Option<(String, i64, Option<i64>, Option<String>)> = self.conn.query_row(
            r#"
            SELECT filename, imported_at, file_created_at, summary
            FROM audio_files
            WHERE id = ? AND deleted_at IS NULL
            "#,
            params![audio_bytes],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        ).optional()?;

        let Some((filename, imported_at, file_created_at, summary)) = audio else {
            return Ok(None);
        };

        // Get transcriptions for this audio file
        let transcriptions = self.get_transcriptions_for_cache(audio_id)?;

        // Check if file exists locally (we need the config for audiofile_directory)
        // For now, set to false - this will be updated when we have access to config
        let file_exists_local = false;

        Ok(Some(serde_json::json!({
            "id": audio_id,
            "filename": filename,
            "imported_at": imported_at,
            "file_created_at": file_created_at,
            "summary": summary,
            "file_exists_local": file_exists_local,
            "transcriptions": transcriptions,
            "waveform": serde_json::Value::Null
        })))
    }

    /// Get transcription metadata for cache (with content preview).
    ///
    /// Includes the first 200 characters of content as `content_preview`.
    /// Full content should be lazy-loaded via `get_transcription_content()`.
    fn get_transcriptions_for_cache(&self, audio_id: &str) -> VoiceResult<Vec<serde_json::Value>> {
        let audio_uuid = Uuid::parse_str(audio_id)
            .map_err(|e| VoiceError::validation("audio_id", e.to_string()))?;
        let audio_bytes = audio_uuid.as_bytes().to_vec();

        let mut stmt = self.conn.prepare(
            r#"
            SELECT id, service, state, created_at, content
            FROM transcriptions
            WHERE audio_file_id = ? AND deleted_at IS NULL
            ORDER BY created_at DESC
            "#
        )?;

        let transcriptions: Vec<serde_json::Value> = stmt
            .query_map(params![audio_bytes], |row| {
                let id_bytes: Vec<u8> = row.get(0)?;
                let service: String = row.get(1)?;
                let state: String = row.get(2)?;
                // created_at is INTEGER (Unix timestamp)
                let created_at: i64 = row.get(3)?;
                let content: String = row.get(4)?;

                // Truncate content to first 200 characters for preview
                let content_preview: String = if content.chars().count() > 200 {
                    content.chars().take(200).collect::<String>() + "…"
                } else {
                    content.clone()
                };

                Ok(serde_json::json!({
                    "id": uuid_bytes_to_hex(&id_bytes).unwrap_or_default(),
                    "service": service,
                    "state": state,
                    "created_at": created_at,
                    "content_preview": content_preview
                }))
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(transcriptions)
    }

    /// Get full transcription content by ID.
    ///
    /// Used for lazy-loading full content when displaying transcription.
    pub fn get_transcription_content(&self, transcription_id: &str) -> VoiceResult<Option<String>> {
        let trans_uuid = Uuid::parse_str(transcription_id)
            .map_err(|e| VoiceError::validation("transcription_id", e.to_string()))?;
        let trans_bytes = trans_uuid.as_bytes().to_vec();

        let content: Option<String> = self.conn.query_row(
            "SELECT content FROM transcriptions WHERE id = ? AND deleted_at IS NULL",
            params![trans_bytes],
            |row| row.get(0),
        ).optional()?;

        Ok(content)
    }

    /// Update the waveform data in a note's display cache.
    ///
    /// The waveform is an array of amplitude values (0-255) for visualization.
    /// This is called from Python after extracting the waveform with ffmpeg.
    ///
    /// # Arguments
    /// * `note_id` - The note ID
    /// * `audio_id` - The audio file ID whose waveform to update
    /// * `waveform` - Array of amplitude values (0-255), typically 150 values
    pub fn update_cache_waveform(&self, note_id: &str, audio_id: &str, waveform: Vec<u8>) -> VoiceResult<bool> {
        let resolved_id = self.resolve_note_id(note_id)?;
        let note_uuid = Uuid::parse_str(&resolved_id)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?;
        let note_bytes = note_uuid.as_bytes().to_vec();

        // Get current cache
        let cache_str: Option<String> = self.conn.query_row(
            "SELECT di_cache_note_pane_display FROM notes WHERE id = ? AND deleted_at IS NULL",
            params![note_bytes],
            |row| row.get(0),
        ).optional()?.flatten();

        let Some(cache_str) = cache_str else {
            return Ok(false);
        };

        // Parse and update
        let mut cache: serde_json::Value = serde_json::from_str(&cache_str)
            .map_err(|e| VoiceError::Other(format!("Failed to parse cache: {}", e)))?;

        // Find the audio file in attachments and update its waveform
        let mut updated = false;
        if let Some(attachments) = cache.get_mut("attachments").and_then(|a| a.as_array_mut()) {
            for attachment in attachments {
                if let Some(audio_file) = attachment.get_mut("audio_file") {
                    if audio_file.get("id").and_then(|id| id.as_str()) == Some(audio_id) {
                        // Convert waveform to JSON array
                        let waveform_json: Vec<serde_json::Value> = waveform.iter()
                            .map(|&v| serde_json::Value::Number(serde_json::Number::from(v)))
                            .collect();
                        audio_file["waveform"] = serde_json::Value::Array(waveform_json);
                        updated = true;
                        break;
                    }
                }
            }
        }

        if updated {
            let new_cache_str = serde_json::to_string(&cache)
                .map_err(|e| VoiceError::Other(format!("Failed to serialize cache: {}", e)))?;
            self.conn.execute(
                "UPDATE notes SET di_cache_note_pane_display = ? WHERE id = ?",
                params![new_cache_str, note_bytes],
            )?;
        }

        Ok(updated)
    }

    /// Get the display cache for a note.
    ///
    /// Returns the JSON string, or None if the cache is not populated.
    pub fn get_note_display_cache(&self, note_id: &str) -> VoiceResult<Option<String>> {
        let resolved_id = self.resolve_note_id(note_id)?;
        let note_uuid = Uuid::parse_str(&resolved_id)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?;
        let note_bytes = note_uuid.as_bytes().to_vec();

        let cache_str: Option<String> = self.conn.query_row(
            "SELECT di_cache_note_pane_display FROM notes WHERE id = ? AND deleted_at IS NULL",
            params![note_bytes],
            |row| row.get(0),
        ).optional()?
         .flatten();

        Ok(cache_str)
    }
}

// ============================================================================
#[cfg(test)]
mod tests {
    use super::*;

    /// FILE-15: an imported file keeps its own name, any POSIX name; a name
    /// taken by a row (without regard to case) or by a file in the folder
    /// gets ` (2)`, ` (3)` before its extension; a recording is named by its
    /// start and the tail of its id; a name that is no file name is refused
    /// before any row is made.
    #[test]
    fn an_imported_file_keeps_its_name_and_a_taken_name_gets_a_numbered_suffix() {
        use crate::models::FileOrigin;
        let temp = tempfile::TempDir::new().unwrap();
        let dir = temp.path().join("audio");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("על הדיסק.ogg"), b"a file no row names").unwrap();
        let db = Database::new(&temp.path().join("n.db")).unwrap();
        let name = |id: &str| db.get_audio_file(id).unwrap().unwrap().local_name;
        let import = |filename: &str| db.create_audio_file_with_duration(filename, None, None, FileOrigin::Imported, Some(&dir));

        assert_eq!(name(&import("שיחה.m4a").unwrap()), "שיחה.m4a");
        assert_eq!(name(&import("שיחה.m4a").unwrap()), "שיחה (2).m4a");
        assert_eq!(name(&import("Memo.M4A").unwrap()), "Memo.M4A", "the name is kept as it is, case and all");
        assert_eq!(name(&import("memo.m4a").unwrap()), "memo (2).m4a", "names differing only in case are one name on the phone's storage");
        assert_eq!(name(&import("על הדיסק.ogg").unwrap()), "על הדיסק (2).ogg", "a file already in the folder takes its name too");
        assert_eq!(name(&import(".hidden").unwrap()), ".hidden");
        assert_eq!(name(&import("no extension").unwrap()), "no extension");
        assert_eq!(name(&import("no extension").unwrap()), "no extension (2)");
        assert_eq!(name(&import("a.tar.gz").unwrap()), "a.tar.gz");
        assert_eq!(name(&import("a.tar.gz").unwrap()), "a.tar (2).gz");

        let before = db.get_all_audio_files().unwrap().len();
        for bad in ["", ".", "..", "a/b.ogg", "nul\0.ogg"] {
            assert!(import(bad).is_err(), "{:?} is not a file name", bad);
        }
        assert_eq!(db.get_all_audio_files().unwrap().len(), before, "no row is made for a refused name");

        let note = db.create_note("").unwrap();
        let recorded = db.import_audio_file_into_note(&note, "Recording 2026-09-13 10-00-00.ogg", Some(1757746800), Some(3), Some(&dir)).unwrap();
        let recorded_name = name(&recorded);
        assert!(recorded_name.ends_with(&format!("-{}.ogg", &recorded[recorded.len() - 8..])), "{}", recorded_name);
        assert_eq!(recorded_name.len(), "2026_09_13_10_00_00-".len() + 8 + ".ogg".len(), "{}", recorded_name);
    }

    /// FILE-15: a row from before the name column keeps the name its file
    /// has, `<id>.<ext>`; the migration invents no other.
    #[test]
    fn a_row_from_before_the_name_column_keeps_the_name_its_file_has() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("old.db");
        let id = {
            let db = Database::new(&path).unwrap();
            let id = db.create_audio_file("הקלטה ישנה.OGG", Some(1735689600)).unwrap();
            // The state of a database written before the column existed
            db.conn.execute("UPDATE audio_files SET local_name = NULL", []).unwrap();
            id
        };
        let db = Database::new(&path).unwrap();
        assert_eq!(db.get_audio_file(&id).unwrap().unwrap().local_name, format!("{}.ogg", id));
    }

    /// FILE-18: the content hash is computed from the file the row names,
    /// travels in the feed, and a row without one never erases it.
    #[test]
    fn the_content_hash_is_stored_from_the_file_and_travels_by_sync_and_is_never_erased() {
        let temp = tempfile::TempDir::new().unwrap();
        let dir = temp.path().join("audio");
        std::fs::create_dir_all(&dir).unwrap();
        let a = Database::new(&temp.path().join("a.db")).unwrap();
        let id = a.create_audio_file("שיחה.ogg", Some(1735689600)).unwrap();
        let row = a.get_audio_file(&id).unwrap().unwrap();
        assert!(row.content_sha256.is_none(), "not hashed before the file is there");
        std::fs::write(crate::models::audio_local_path(&dir, &row.local_name), b"bytes of the recording").unwrap();
        let hash = a.store_content_hash(&id, &dir).unwrap();
        assert_eq!(hash, crate::transfer::file_sha256(&crate::models::audio_local_path(&dir, &row.local_name)).unwrap());
        assert_eq!(a.get_audio_file(&id).unwrap().unwrap().content_sha256.as_deref(), Some(hash.as_str()));

        let (changes, _, _) = a.get_changes_after_seq_as_sync_changes(0, None, 100).unwrap();
        let audio = changes.iter().find(|c| c.entity_type == "audio_file" && c.entity_id == id).expect("the row is in the feed");
        assert_eq!(audio.data["content_sha256"].as_str(), Some(hash.as_str()), "the hash travels with the row");
        assert_eq!(a.get_audio_file_raw(&id).unwrap().unwrap()["content_sha256"].as_str(), Some(hash.as_str()));

        let b = Database::new(&temp.path().join("b.db")).unwrap();
        b.apply_sync_audio_file(&id, 1735689600, "שיחה.ogg", Some(1735689600), None, None, Some(1735689601), None, Some(1735689602), None, None, None, None, None, Some(&hash), None).unwrap();
        assert_eq!(b.get_audio_file(&id).unwrap().unwrap().content_sha256.as_deref(), Some(hash.as_str()));
        // A newer row without a hash: the hash stays
        b.apply_sync_audio_file(&id, 1735689600, "שיחה.ogg", Some(1735689600), None, None, Some(1735689700), None, Some(1735689701), None, None, None, None, None, None, None).unwrap();
        assert_eq!(b.get_audio_file(&id).unwrap().unwrap().content_sha256.as_deref(), Some(hash.as_str()), "never erased");
        // An older row with another hash: ignored; a newer one: taken
        let other = "b".repeat(64);
        b.apply_sync_audio_file(&id, 1735689600, "שיחה.ogg", Some(1735689600), None, None, Some(1735689650), None, Some(1735689702), None, None, None, None, None, Some(&other), None).unwrap();
        assert_eq!(b.get_audio_file(&id).unwrap().unwrap().content_sha256.as_deref(), Some(hash.as_str()), "an older row does not replace it");
        b.apply_sync_audio_file(&id, 1735689600, "שיחה.ogg", Some(1735689600), None, None, Some(1735689800), None, Some(1735689703), None, None, None, None, None, Some(&other), None).unwrap();
        assert_eq!(b.get_audio_file(&id).unwrap().unwrap().content_sha256.as_deref(), Some(other.as_str()), "a newer row does");
        assert!(b.get_full_dataset().unwrap()["audio_files"][0]["content_sha256"].is_string());
    }

    mod account_identity {
        use super::*;
        use tempfile::TempDir;

        const ACCOUNT_A: &str = "0199aaaaaaaa7000800000000000000a";
        const ACCOUNT_B: &str = "0199bbbbbbbb7000800000000000000b";

        #[test]
        fn a_new_database_has_an_account_id_of_its_own() {
            let db = Database::new_in_memory().unwrap();
            let id = db.account_id().unwrap();
            assert_eq!(id.len(), 32);
            assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
            let other = Database::new_in_memory().unwrap();
            assert_ne!(other.account_id().unwrap(), id, "two databases never share an account by chance");
        }

        #[test]
        fn a_fresh_database_takes_the_account_it_is_opened_for() {
            let dir = TempDir::new().unwrap();
            let path = dir.path().join("notes.db");
            let db = Database::new_for_account(&path, ACCOUNT_A).unwrap();
            assert_eq!(db.account_id().unwrap(), ACCOUNT_A);
            drop(db);
            let again = Database::new_for_account(&path, ACCOUNT_A).unwrap();
            assert_eq!(again.account_id().unwrap(), ACCOUNT_A);
        }

        #[test]
        fn a_database_with_notes_refuses_another_account_and_is_not_corrected() {
            let dir = TempDir::new().unwrap();
            let path = dir.path().join("notes.db");
            let db = Database::new_for_account(&path, ACCOUNT_A).unwrap();
            db.create_note("פתק ראשון").unwrap();
            drop(db);
            let err = match Database::new_for_account(&path, ACCOUNT_B) {
                Ok(_) => panic!("a database with notes must not change account"),
                Err(e) => e.to_string(),
            };
            assert!(err.contains("ACCOUNT_DISAGREES"), "{}", err);
            assert_eq!(Database::new(&path).unwrap().account_id().unwrap(), ACCOUNT_A);
        }

        #[test]
        fn an_account_id_must_be_thirty_two_hex_characters() {
            assert!(validate_account_id(ACCOUNT_A).is_ok());
            assert!(validate_account_id("short").is_err());
            assert!(validate_account_id("zzzzaaaaaaaa7000800000000000000a").is_err());
        }

        #[test]
        fn moving_to_another_account_keeps_the_notes_and_forgets_the_peers() {
            let dir = TempDir::new().unwrap();
            let db = Database::new_for_account(dir.path().join("notes.db"), ACCOUNT_A).unwrap();
            let note = db.create_note("נשאר").unwrap();
            let peer = "00000000000070008000000000000099";
            db.set_peer_cursors(peer, Some("Desk"), Some(7), Some(9), Some("db1")).unwrap();
            db.set_peer_account_id(peer, None, ACCOUNT_A).unwrap();

            db.move_to_account(ACCOUNT_B).unwrap();

            assert_eq!(db.account_id().unwrap(), ACCOUNT_B);
            assert!(db.get_note(&note).unwrap().is_some());
            assert_eq!(db.get_peer_cursors(peer).unwrap(), (0, 0, None));
            assert_eq!(db.get_peer_account_id(peer).unwrap(), None);
            assert_eq!(db.list_snapshots().unwrap().len(), 1, "the move was snapshotted first");
        }

        #[test]
        fn a_peer_s_account_is_remembered() {
            let db = Database::new_in_memory().unwrap();
            let peer = "00000000000070008000000000000099";
            assert_eq!(db.get_peer_account_id(peer).unwrap(), None);
            db.set_peer_account_id(peer, Some("Phone"), ACCOUNT_A).unwrap();
            assert_eq!(db.get_peer_account_id(peer).unwrap(), Some(ACCOUNT_A.to_string()));
        }
    }

    mod snapshots {
        use super::*;
        use tempfile::TempDir;

        #[test]
        fn an_in_memory_database_has_no_snapshots_and_skips_them_silently() {
            let db = Database::new_in_memory().unwrap();
            assert!(db.snapshot_directory().is_none());
            assert!(db.snapshot().is_err());
            db.snapshot_before("a test").unwrap();
            assert!(db.list_snapshots().unwrap().is_empty());
        }

        #[test]
        fn the_newest_five_are_kept_and_a_restore_brings_a_note_back() {
            let dir = TempDir::new().unwrap();
            let mut db = Database::new(dir.path().join("notes.db")).unwrap();
            assert_eq!(db.snapshot_directory().unwrap(), dir.path().join("snapshots"));
            let note = db.create_note("לפני המחיקה").unwrap();

            for _ in 0..6 {
                db.snapshot().unwrap();
            }
            let listed = db.list_snapshots().unwrap();
            assert_eq!(listed.len(), SNAPSHOTS_KEPT, "the sixth snapshot deletes the first");
            assert!(listed.iter().all(|s| s.note_count == 1));
            assert!(listed.iter().all(|s| s.size_bytes > 0));
            let newest = listed[0].name.clone();

            db.delete_note(&note).unwrap();
            assert!(db.get_note(&note).unwrap().is_none());

            db.restore_snapshot(&newest).unwrap();
            assert!(db.get_note(&note).unwrap().is_some(), "the note is back");
            // The state before the restore was snapshotted, so the restore is undoable too.
            let after = db.list_snapshots().unwrap();
            assert_eq!(after.len(), SNAPSHOTS_KEPT);
            assert!(after[0].note_count == 0, "the newest snapshot is the state the restore replaced");
        }

        #[test]
        fn a_restore_refuses_a_name_that_is_not_a_snapshot() {
            let dir = TempDir::new().unwrap();
            let mut db = Database::new(dir.path().join("notes.db")).unwrap();
            assert!(db.restore_snapshot("../notes.db").is_err());
            assert!(db.restore_snapshot("notes-nothing.db").is_err());
        }
    }

    #[test]
    fn test_create_database() {
        let db = Database::new_in_memory().unwrap();
        assert!(db.get_all_notes().unwrap().is_empty());
        // Database has system tags (_system, _marked, _nonsynced, _too-big) by default
        let tags = db.get_all_tags().unwrap();
        assert_eq!(tags.len(), 4);
        let tag_names: Vec<&str> = tags.iter().map(|t| t.name.as_str()).collect();
        assert!(tag_names.contains(&"_system"));
        assert!(tag_names.contains(&"_marked"));
        assert!(tag_names.contains(&"_nonsynced"));
        assert!(tag_names.contains(&"_too-big"));
    }

    #[test]
    fn test_get_recent_transcriptions_newest_first() {
        // What a transcription queue shows under "Completed": the work that is
        // done, newest first, whichever recording it belongs to.
        let db = Database::new_in_memory().unwrap();
        let first = db.create_audio_file("הקלטה-1.opus", None).unwrap();
        let second = db.create_audio_file("הקלטה-2.opus", None).unwrap();

        let older = db
            .create_transcription(&first, "שלום", None, "local_whisper", None, None, None)
            .unwrap();
        let newer = db
            .create_transcription(&second, "עולם", None, "local_whisper", None, None, None)
            .unwrap();

        let rows = db.get_recent_transcriptions(None, 10).unwrap();
        assert_eq!(rows.len(), 2);
        // Both were made in the same second, so the tie is broken by insertion
        // order: the newest row comes first either way.
        assert_eq!(rows[0].id, newer);
        assert_eq!(rows[1].id, older);
    }

    #[test]
    fn test_get_recent_transcriptions_by_service_and_limit() {
        let db = Database::new_in_memory().unwrap();
        let audio = db.create_audio_file("הקלטה.opus", None).unwrap();
        db.create_transcription(&audio, "a", None, "local_whisper", None, None, None)
            .unwrap();
        db.create_transcription(&audio, "b", None, "speechtext_ai", None, None, None)
            .unwrap();
        db.create_transcription(&audio, "c", None, "local_whisper", None, None, None)
            .unwrap();

        let local = db.get_recent_transcriptions(Some("local_whisper"), 10).unwrap();
        assert_eq!(local.len(), 2);
        assert!(local.iter().all(|t| t.service == "local_whisper"));

        let one = db.get_recent_transcriptions(None, 1).unwrap();
        assert_eq!(one.len(), 1, "a queue view is a screenful, not a history");
    }

    #[test]
    fn test_get_recent_transcriptions_leaves_out_deleted() {
        let db = Database::new_in_memory().unwrap();
        let audio = db.create_audio_file("הקלטה.opus", None).unwrap();
        let kept = db
            .create_transcription(&audio, "kept", None, "local_whisper", None, None, None)
            .unwrap();
        let gone = db
            .create_transcription(&audio, "gone", None, "local_whisper", None, None, None)
            .unwrap();
        db.delete_transcription(&gone).unwrap();

        let rows = db.get_recent_transcriptions(None, 10).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, kept);
    }

    #[test]
    fn test_create_and_get_note() {
        let db = Database::new_in_memory().unwrap();
        let note_id = db.create_note("Test content").unwrap();

        let note = db.get_note(&note_id).unwrap().unwrap();
        assert_eq!(note.content, "Test content");
        assert!(note.tag_names.is_none());
    }

    #[test]
    fn test_update_note() {
        let db = Database::new_in_memory().unwrap();
        let note_id = db.create_note("Original").unwrap();

        let updated = db.update_note(&note_id, "Updated").unwrap();
        assert!(updated);

        let note = db.get_note(&note_id).unwrap().unwrap();
        assert_eq!(note.content, "Updated");
    }

    #[test]
    fn test_delete_note() {
        let db = Database::new_in_memory().unwrap();
        let note_id = db.create_note("To delete").unwrap();

        let deleted = db.delete_note(&note_id).unwrap();
        assert!(deleted);

        // Note should not appear in get_all_notes
        let notes = db.get_all_notes().unwrap();
        assert!(notes.is_empty());

        // get_note should NOT return deleted notes (consistent with get_tag behavior)
        let note = db.get_note(&note_id).unwrap();
        assert!(note.is_none(), "get_note should not return deleted notes");

        // But should still be retrievable via get_note_raw for sync purposes
        let note_raw = db.get_note_raw(&note_id).unwrap();
        assert!(note_raw.is_some());
        let note_data = note_raw.unwrap();
        let deleted_at = note_data.get("deleted_at").and_then(|v| v.as_i64());
        assert!(deleted_at.is_some(), "deleted note should have deleted_at in raw data");
    }

    #[test]
    fn test_create_and_get_tag() {
        let db = Database::new_in_memory().unwrap();
        let tag_id = db.create_tag("Work", None).unwrap();

        let tag = db.get_tag(&tag_id).unwrap().unwrap();
        assert_eq!(tag.name, "Work");
        assert!(tag.parent_id.is_none());
    }

    #[test]
    fn test_tag_hierarchy() {
        let db = Database::new_in_memory().unwrap();
        let parent_id = db.create_tag("Europe", None).unwrap();
        let child_id = db.create_tag("France", Some(&parent_id)).unwrap();
        let grandchild_id = db.create_tag("Paris", Some(&child_id)).unwrap();

        // Get by path
        let tag = db.get_tag_by_path("Europe/France/Paris").unwrap().unwrap();
        assert_eq!(tag.id, grandchild_id);

        // Get descendants
        let descendants = db.get_tag_descendants(&parent_id).unwrap();
        assert_eq!(descendants.len(), 3); // Europe, France, Paris
    }

    #[test]
    fn test_add_tag_to_note() {
        let db = Database::new_in_memory().unwrap();
        let note_id = db.create_note("Test note").unwrap();
        let tag_id = db.create_tag("Work", None).unwrap();

        let result = db.add_tag_to_note(&note_id, &tag_id).unwrap();
        assert!(result.changed);

        let tags = db.get_note_tags(&note_id).unwrap();
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0].name, "Work");
    }

    #[test]
    fn test_search_notes() {
        let db = Database::new_in_memory().unwrap();
        let note1_id = db.create_note("Hello world").unwrap();
        db.create_note("Goodbye world").unwrap();
        let tag_id = db.create_tag("Greeting", None).unwrap();

        db.add_tag_to_note(&note1_id, &tag_id).unwrap();

        // Search by text
        let results = db.search_notes(Some("hello"), None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, note1_id);

        // Search by tag
        let tag_groups = vec![vec![tag_id.clone()]];
        let results = db.search_notes(None, Some(&tag_groups)).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, note1_id);
    }

    #[test]
    fn test_import_audio_file() {
        let db = Database::new_in_memory().unwrap();

        // Import an audio file with metadata
        let file_created_at = Some(1700000000); // Nov 14, 2023
        let duration_seconds = Some(120); // 2 minutes
        let (note_id, audio_file_id) = db
            .import_audio_file("recording.m4a", file_created_at, duration_seconds, None)
            .unwrap();

        // Verify the note was created
        let note = db.get_note(&note_id).unwrap().unwrap();
        assert_eq!(note.content, ""); // Empty content for imported audio
        assert_eq!(note.created_at, file_created_at.unwrap()); // Uses file creation date

        // Verify the audio file was created
        let audio_file = db.get_audio_file(&audio_file_id).unwrap().unwrap();
        assert_eq!(audio_file.filename, "recording.m4a");
        assert_eq!(audio_file.file_created_at, file_created_at);
        assert_eq!(audio_file.duration_seconds, duration_seconds);

        // Verify the attachment was created
        let attachments = db.get_audio_files_for_note(&note_id).unwrap();
        assert_eq!(attachments.len(), 1);
        assert_eq!(attachments[0].id, audio_file_id);
    }

    #[test]
    fn test_import_audio_file_without_metadata() {
        let db = Database::new_in_memory().unwrap();

        // Import without file creation date or duration
        let (note_id, audio_file_id) = db
            .import_audio_file("voice_memo.mp3", None, None, None)
            .unwrap();

        // Verify the note was created with current timestamp
        let note = db.get_note(&note_id).unwrap().unwrap();
        assert!(note.created_at > 0); // Has a timestamp

        // Verify the audio file was created
        let audio_file = db.get_audio_file(&audio_file_id).unwrap().unwrap();
        assert_eq!(audio_file.filename, "voice_memo.mp3");
        assert!(audio_file.file_created_at.is_none());
        assert!(audio_file.duration_seconds.is_none());
    }

    /// Merging keeps the older note, gains the newer note's text, takes over
    /// its recordings, and leaves nothing behind.
    #[test]
    fn merging_two_notes_keeps_both_texts_and_moves_the_recording() {
        let db = Database::new_in_memory().unwrap();
        // The older note is the one with a recording on it
        let (older, audio) = db
            .import_audio_file("הקלטה.ogg", Some(1_700_000_000), Some(5), None)
            .unwrap();
        db.update_note(&older, "הפגישה הראשונה").unwrap();
        let newer = db.create_note("הערה שנייה").unwrap();

        let survivor = db.merge_notes(&older, &newer).unwrap();

        assert_eq!(survivor, older, "the older note survives");
        let note = db.get_note(&survivor).unwrap().unwrap();
        assert!(note.content.contains("הפגישה הראשונה"), "{}", note.content);
        assert!(note.content.contains("הערה שנייה"), "{}", note.content);

        let files = db.get_audio_files_for_note(&survivor).unwrap();
        assert_eq!(files.len(), 1, "the recording came across");
        assert_eq!(files[0].id, audio);

        assert!(db.get_note(&newer).unwrap().is_none(), "the emptied note is gone");
    }

    /// The order the two are given in does not matter.
    #[test]
    fn merging_the_other_way_round_keeps_the_same_note() {
        let db = Database::new_in_memory().unwrap();
        let (older, _) = db
            .import_audio_file("הקלטה.ogg", Some(1_700_000_000), Some(5), None)
            .unwrap();
        let newer = db.create_note("הערה שנייה").unwrap();

        let survivor = db.merge_notes(&newer, &older).unwrap();
        assert_eq!(survivor, older, "still the older one");
    }

    #[test]
    fn test_create_note_with_timestamp() {
        let db = Database::new_in_memory().unwrap();

        // Create note with specific timestamp
        let timestamp = Some(1600000000); // Sep 13, 2020
        let note_id = db.create_note_with_timestamp("Test content", timestamp).unwrap();

        let note = db.get_note(&note_id).unwrap().unwrap();
        assert_eq!(note.content, "Test content");
        assert_eq!(note.created_at, timestamp.unwrap());
    }

    // =========================================================================
    // Cloud Storage Tests
    // =========================================================================

    #[test]
    fn test_audio_file_storage_columns_default_null() {
        let db = Database::new_in_memory().unwrap();

        // Create audio file
        let audio_id = db.create_audio_file("test.mp3", None).unwrap();

        // Verify storage columns are NULL by default
        let audio = db.get_audio_file(&audio_id).unwrap().unwrap();
        assert!(audio.storage_provider.is_none());
        assert!(audio.storage_key.is_none());
        assert!(audio.storage_uploaded_at.is_none());
    }

    #[test]
    fn test_update_audio_file_storage() {
        let db = Database::new_in_memory().unwrap();

        // Create audio file
        let audio_id = db.create_audio_file("test.mp3", None).unwrap();

        // Update storage info
        let updated = db.update_audio_file_storage(&audio_id, "s3", "audio/test.mp3").unwrap();
        assert!(updated);

        // Verify storage info was set
        let audio = db.get_audio_file(&audio_id).unwrap().unwrap();
        assert_eq!(audio.storage_provider, Some("s3".to_string()));
        assert_eq!(audio.storage_key, Some("audio/test.mp3".to_string()));
        assert!(audio.storage_uploaded_at.is_some());
        assert!(audio.modified_at.is_some());
    }

    #[test]
    fn test_clear_audio_file_storage() {
        let db = Database::new_in_memory().unwrap();

        // Create audio file and set storage
        let audio_id = db.create_audio_file("test.mp3", None).unwrap();
        db.update_audio_file_storage(&audio_id, "s3", "audio/test.mp3").unwrap();

        // Verify storage is set
        let audio = db.get_audio_file(&audio_id).unwrap().unwrap();
        assert!(audio.storage_provider.is_some());

        // Clear storage
        let cleared = db.clear_audio_file_storage(&audio_id).unwrap();
        assert!(cleared);

        // Verify storage is cleared
        let audio = db.get_audio_file(&audio_id).unwrap().unwrap();
        assert!(audio.storage_provider.is_none());
        assert!(audio.storage_key.is_none());
        assert!(audio.storage_uploaded_at.is_none());
    }

    #[test]
    fn test_get_audio_files_pending_upload() {
        let db = Database::new_in_memory().unwrap();

        // Create some audio files
        let audio_id1 = db.create_audio_file("file1.mp3", None).unwrap();
        let audio_id2 = db.create_audio_file("file2.mp3", None).unwrap();
        let audio_id3 = db.create_audio_file("file3.mp3", None).unwrap();

        // Upload one of them
        db.update_audio_file_storage(&audio_id2, "s3", "audio/file2.mp3").unwrap();

        // Get pending uploads
        let pending = db.get_audio_files_pending_upload().unwrap();
        assert_eq!(pending.len(), 2);

        // Verify the uploaded file is not in the list
        let pending_ids: Vec<_> = pending.iter().map(|a| a.id.as_str()).collect();
        assert!(pending_ids.contains(&audio_id1.as_str()));
        assert!(!pending_ids.contains(&audio_id2.as_str()));
        assert!(pending_ids.contains(&audio_id3.as_str()));
    }

    #[test]
    fn test_audio_file_storage_syncs_correctly() {
        let db = Database::new_in_memory().unwrap();

        // Create audio file with storage info via sync
        let audio_id = uuid::Uuid::now_v7().simple().to_string();
        db.apply_sync_audio_file(
            &audio_id,
            1700000000,  // imported_at
            "synced.mp3",
            None,        // file_created_at
            None,        // duration_seconds
            None,        // summary
            Some(1700000001), // modified_at
            None,        // deleted_at
            None,        // sync_received_at
            Some("s3"),  // storage_provider
            Some("audio/synced.mp3"), // storage_key
            Some(1700000002), // storage_uploaded_at,
            None,
            None,
            None,
            None,
        ).unwrap();

        // Verify storage info was applied
        let audio = db.get_audio_file(&audio_id).unwrap().unwrap();
        assert_eq!(audio.storage_provider, Some("s3".to_string()));
        assert_eq!(audio.storage_key, Some("audio/synced.mp3".to_string()));
        assert_eq!(audio.storage_uploaded_at, Some(1700000002));
    }

    #[test]
    fn test_get_changes_since_includes_storage_fields() {
        let db = Database::new_in_memory().unwrap();

        // Create audio file with storage info
        let audio_id = db.create_audio_file("test.mp3", None).unwrap();
        db.update_audio_file_storage(&audio_id, "s3", "audio/test.mp3").unwrap();

        // Get changes
        let (changes, _) = db.get_changes_since(None, 100).unwrap();

        // Find the audio_file change
        let audio_change = changes.iter().find(|c| {
            c.get("entity_type").and_then(|v| v.as_str()) == Some("audio_file")
        });

        assert!(audio_change.is_some());
        let data = audio_change.unwrap().get("data").unwrap();
        assert_eq!(data.get("storage_provider").and_then(|v| v.as_str()), Some("s3"));
        assert_eq!(data.get("storage_key").and_then(|v| v.as_str()), Some("audio/test.mp3"));
        assert!(data.get("storage_uploaded_at").and_then(|v| v.as_i64()).is_some());
    }

    #[test]
    fn test_file_storage_config_get_none() {
        let db = Database::new_in_memory().unwrap();

        // No config set yet
        let config = db.get_file_storage_config().unwrap();
        assert!(config.is_none());
    }

    #[test]
    fn test_file_storage_config_set_and_get() {
        let db = Database::new_in_memory().unwrap();

        let s3_config = serde_json::json!({
            "bucket": "my-bucket",
            "region": "us-east-1",
            "access_key_id": "AKIATEST",
            "secret_access_key": "secret123",
        });

        db.set_file_storage_config("s3", Some(&s3_config)).unwrap();

        let config = db.get_file_storage_config().unwrap().unwrap();
        assert_eq!(config.get("provider").and_then(|v| v.as_str()), Some("s3"));

        let stored_config = config.get("config").unwrap();
        assert_eq!(
            stored_config.get("bucket").and_then(|v| v.as_str()),
            Some("my-bucket")
        );
        assert_eq!(
            stored_config.get("region").and_then(|v| v.as_str()),
            Some("us-east-1")
        );
    }

    #[test]
    fn test_file_storage_config_update() {
        let db = Database::new_in_memory().unwrap();

        // Set initial config
        let s3_config = serde_json::json!({
            "bucket": "bucket-1",
            "region": "us-east-1",
        });
        db.set_file_storage_config("s3", Some(&s3_config)).unwrap();

        // Update config
        let new_config = serde_json::json!({
            "bucket": "bucket-2",
            "region": "eu-west-1",
        });
        db.set_file_storage_config("s3", Some(&new_config)).unwrap();

        let config = db.get_file_storage_config().unwrap().unwrap();
        let stored_config = config.get("config").unwrap();
        assert_eq!(
            stored_config.get("bucket").and_then(|v| v.as_str()),
            Some("bucket-2")
        );
        assert_eq!(
            stored_config.get("region").and_then(|v| v.as_str()),
            Some("eu-west-1")
        );
    }

    #[test]
    fn test_file_storage_config_struct() {
        let db = Database::new_in_memory().unwrap();

        // Default when no config
        let config = db.get_file_storage_config_struct().unwrap();
        assert_eq!(config.provider, "none");

        // Set S3 config
        let s3_config = serde_json::json!({
            "bucket": "test-bucket",
            "region": "us-west-2",
            "access_key_id": "AKIATEST",
            "secret_access_key": "secret",
            "prefix": "audio/",
        });
        db.set_file_storage_config("s3", Some(&s3_config)).unwrap();

        let config = db.get_file_storage_config_struct().unwrap();
        assert_eq!(config.provider, "s3");
        assert_eq!(config.s3_bucket(), Some("test-bucket"));
        assert_eq!(config.s3_region(), Some("us-west-2"));
        assert_eq!(config.s3_prefix(), Some("audio/"));
    }

    #[test]
    fn test_file_storage_config_disable() {
        let db = Database::new_in_memory().unwrap();

        // Set S3 config
        let s3_config = serde_json::json!({
            "bucket": "test-bucket",
        });
        db.set_file_storage_config("s3", Some(&s3_config)).unwrap();

        // Disable by setting to "none"
        db.set_file_storage_config("none", None).unwrap();

        let config = db.get_file_storage_config_struct().unwrap();
        assert_eq!(config.provider, "none");
        assert!(!config.is_enabled());
    }

}


// ============================================================================
// Versioning support: migrations, cache helpers, and the sync failure queue
// ============================================================================

impl Database {
    /// The pre-versioning conflict tables are replaced by `field_conflicts`,
    /// which is derived from the version graph on every device.
    /// Write-order sequence for the change feed (see `get_changes_after_seq`).
    ///
    /// Every syncable table gets a `seq` column; triggers stamp the next
    /// value on insert and on any change of a synced column. Cache columns
    /// and `sync_received_at` do not bump it, so applying an echo of our own
    /// data from a peer does not re-publish it. Idempotent.
    fn migrate_add_sync_sequence(&mut self) -> VoiceResult<()> {
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS sync_sequence (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                value INTEGER NOT NULL
            );
            INSERT OR IGNORE INTO sync_sequence (id, value) VALUES (1, 0);
            CREATE TABLE IF NOT EXISTS sync_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            "#,
        )?;
        let has_id: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM sync_meta WHERE key = 'database_id'",
            [],
            |r| r.get(0),
        )?;
        if has_id == 0 {
            self.conn.execute(
                "INSERT INTO sync_meta (key, value) VALUES ('database_id', ?)",
                params![Uuid::now_v7().simple().to_string()],
            )?;
        }
        // The account this database belongs to (ACCT-1). Minted here so that
        // every database has one from its first moment; a device that is
        // paired later takes the account's id instead (ACCT-4).
        let has_account: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM sync_meta WHERE key = 'account_id'",
            [],
            |r| r.get(0),
        )?;
        if has_account == 0 {
            self.conn.execute(
                "INSERT INTO sync_meta (key, value) VALUES ('account_id', ?)",
                params![Uuid::now_v7().simple().to_string()],
            )?;
        }
        // Pairing offers (PAIR-2): the hash of a token shown in a code, until
        // it is spent, expired or guessed at too often. Local, never synced.
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS pairing_offers (
                token_hash TEXT PRIMARY KEY,
                expires_at INTEGER NOT NULL,
                failures INTEGER NOT NULL DEFAULT 0
            );
            "#,
        )?;
        // The recording's file name on disk (FILE-15): local, never synced
        if !self.column_exists("audio_files", "local_name")? {
            self.conn.execute("ALTER TABLE audio_files ADD COLUMN local_name TEXT", [])?;
        }
        // The content hash (Stage 13): synced metadata, merged per column
        if !self.column_exists("audio_files", "content_sha256")? {
            self.conn.execute("ALTER TABLE audio_files ADD COLUMN content_sha256 TEXT", [])?;
        }
        // Whether the object is encrypted (Stage 15): travels with the storage key
        if !self.column_exists("audio_files", "storage_encrypted")? {
            self.conn.execute("ALTER TABLE audio_files ADD COLUMN storage_encrypted INTEGER NOT NULL DEFAULT 0", [])?;
        }
        {
            // A row from before this column names no file, but its file is
            // where the code of that time put it, `<id>.<ext>`. That name is
            // written down as it is: a migration never changes a name that
            // refers to a file outside the database (FILE-15).
            let unnamed: Vec<(Vec<u8>, String)> = {
                let mut stmt = self.conn.prepare("SELECT id, filename FROM audio_files WHERE local_name IS NULL OR local_name = ''")?;
                let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
                rows.collect::<Result<Vec<_>, _>>()?
            };
            for (id, filename) in unnamed {
                let id_hex = uuid_bytes_to_hex(&id).unwrap_or_default();
                let name = format!("{}.{}", id_hex, crate::models::audio_file_extension(&filename));
                self.conn.execute("UPDATE audio_files SET local_name = ? WHERE id = ?", params![name, id])?;
            }
        }

        // Which peers hold a copy of which recording (Stage 10): written by a
        // send, by a fetch, by a receive, and by a peer's missing-list
        // request. Local, never synced; it answers "is this one safe".
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS purged_objects (
                storage_key TEXT PRIMARY KEY,
                at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS upload_parts (
                audio_id BLOB NOT NULL,
                storage_key TEXT NOT NULL,
                upload_id TEXT NOT NULL,
                part_size INTEGER NOT NULL,
                part_number INTEGER NOT NULL,
                etag TEXT NOT NULL,
                PRIMARY KEY (audio_id, part_number)
            );
            CREATE TABLE IF NOT EXISTS audio_file_copies (
                audio_id BLOB NOT NULL,
                peer_id BLOB NOT NULL,
                at INTEGER NOT NULL,
                PRIMARY KEY (audio_id, peer_id)
            );
            "#,
        )?;
        for col in ["last_received_cursor INTEGER", "last_sent_seq INTEGER", "peer_database_id TEXT", "peer_account_id TEXT", "last_operation TEXT", "peer_entity_types TEXT"] {
            let name = col.split(' ').next().unwrap_or_default();
            if !self.column_exists("sync_peers", name)? {
                self.conn.execute(&format!("ALTER TABLE sync_peers ADD COLUMN {}", col), [])?;
            }
        }

        if !self.column_exists("field_versions", "published")? {
            self.conn.execute("ALTER TABLE field_versions ADD COLUMN published INTEGER NOT NULL DEFAULT 0", [])?;
        }
        // Which attachment stands for a note, and which transcription for a
        // recording. Empty until the user chooses one, and then it is the
        // one played and the one shown in the list.
        for (table, column) in [
            ("notes", "primary_attachment_id"),
            ("audio_files", "primary_transcription_id"),
        ] {
            if !self.column_exists(table, column)? {
                self.conn.execute(&format!("ALTER TABLE {table} ADD COLUMN {column} BLOB"), [])?;
            }
        }

        // The trash bin's floor: what has been removed for good, and must
        // not come back from a peer that has not heard yet. The rows are
        // kept for ever, which costs 40 bytes per purged entity.
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS purges (
                entity_type TEXT NOT NULL,
                entity_id BLOB NOT NULL,
                purged_at INTEGER NOT NULL,
                purged_at_offset INTEGER,
                purged_at_zone TEXT,
                device_id BLOB,
                seq INTEGER,
                PRIMARY KEY (entity_type, entity_id)
            );
            "#,
        )?;

        // (table, columns whose change means "publish again")
        let tables: [(&str, &[&str]); 9] = [
            ("field_versions", &["published"]),
            ("notes", &["content", "modified_at", "deleted_at", "primary_attachment_id"]),
            ("tags", &["name", "parent_id", "modified_at", "deleted_at"]),
            ("note_tags", &["modified_at", "deleted_at"]),
            ("note_attachments", &["modified_at", "deleted_at"]),
            ("audio_files", &["filename", "file_created_at", "duration_seconds", "summary", "modified_at", "deleted_at", "storage_provider", "storage_key", "storage_uploaded_at", "primary_transcription_id", "content_sha256", "storage_encrypted"]),
            ("transcriptions", &["content", "content_segments", "service_response", "state", "modified_at", "deleted_at"]),
            ("file_storage_config", &["provider", "config", "modified_at"]),
            // A purge is written once and never changed, so it only needs
            // the insert trigger.
            ("purges", &[]),
        ];
        for (table, cols) in tables {
            let fresh = !self.column_exists(table, "seq")?;
            if fresh {
                self.conn.execute(&format!("ALTER TABLE {} ADD COLUMN seq INTEGER", table), [])?;
            }
            self.conn.execute(
                &format!("CREATE INDEX IF NOT EXISTS idx_{}_seq ON {}(seq)", table, table),
                [],
            )?;
            if fresh {
                // Existing rows: versions first so that they precede their rows
                self.conn.execute(
                    &format!(
                        "UPDATE {t} SET seq = (SELECT value FROM sync_sequence WHERE id = 1) + rowid WHERE seq IS NULL",
                        t = table
                    ),
                    [],
                )?;
                self.conn.execute(
                    &format!(
                        "UPDATE sync_sequence SET value = COALESCE((SELECT MAX(seq) FROM {t}), value) WHERE id = 1",
                        t = table
                    ),
                    [],
                )?;
            }
            let bump = format!(
                "UPDATE sync_sequence SET value = value + 1 WHERE id = 1; \
                 UPDATE {t} SET seq = (SELECT value FROM sync_sequence WHERE id = 1) WHERE rowid = NEW.rowid;",
                t = table
            );
            self.conn.execute_batch(&format!(
                "CREATE TRIGGER IF NOT EXISTS trg_{t}_seq_insert AFTER INSERT ON {t} BEGIN {bump} END;",
                t = table, bump = bump
            ))?;
            if !cols.is_empty() {
                let of = cols.join(", ");
                let when = cols
                    .iter()
                    .map(|c| format!("NEW.{c} IS NOT OLD.{c}", c = c))
                    .collect::<Vec<_>>()
                    .join(" OR ");
                self.conn.execute_batch(&format!(
                    "CREATE TRIGGER IF NOT EXISTS trg_{t}_seq_update AFTER UPDATE OF {of} ON {t} WHEN {when} BEGIN {bump} END;",
                    t = table, of = of, when = when, bump = bump
                ))?;
            }
        }
        Ok(())
    }

    fn column_exists(&self, table: &str, column: &str) -> VoiceResult<bool> {
        let mut stmt = self.conn.prepare(&format!("PRAGMA table_info({})", table))?;
        let names = stmt.query_map([], |r| r.get::<_, String>(1))?;
        for n in names {
            if n? == column {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Attach the timezone that came with a row from a peer to each timestamp
    /// that actually took the peer's value.
    ///
    /// The `WHERE <stamp> = ?` clause is what makes this safe: when the upsert
    /// kept a value of its own, the peer's zone is not recorded against it.
    /// A peer that predates the timezone fields sends none, and the row keeps
    /// whatever it had.
    pub fn apply_zones_by_id(&self, table: &str, id_hex: &str, stamps: &[&str], data: &serde_json::Value) -> VoiceResult<()> {
        let id = Uuid::parse_str(id_hex)
            .map_err(|e| VoiceError::validation("id", e.to_string()))?
            .as_bytes()
            .to_vec();
        for stamp in stamps {
            let Some(ts) = data[*stamp].as_i64() else { continue };
            let offset = data[&format!("{}_offset", stamp)].as_i64();
            let zone = data[&format!("{}_zone", stamp)].as_str();
            if offset.is_none() && zone.is_none() {
                continue;
            }
            let sql = format!(
                "UPDATE {table} SET {stamp}_offset = ?, {stamp}_zone = ? WHERE id = ? AND {stamp} = ?"
            );
            self.conn.execute(&sql, params![offset, zone, id, ts])?;
        }
        Ok(())
    }

    /// The same for the note-tag link, which is keyed by its two ends.
    pub fn apply_zones_for_note_tag(&self, note_hex: &str, tag_hex: &str, stamps: &[&str], data: &serde_json::Value) -> VoiceResult<()> {
        let note = Uuid::parse_str(note_hex)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?
            .as_bytes()
            .to_vec();
        let tag = Uuid::parse_str(tag_hex)
            .map_err(|e| VoiceError::validation("tag_id", e.to_string()))?
            .as_bytes()
            .to_vec();
        for stamp in stamps {
            let Some(ts) = data[*stamp].as_i64() else { continue };
            let offset = data[&format!("{}_offset", stamp)].as_i64();
            let zone = data[&format!("{}_zone", stamp)].as_str();
            if offset.is_none() && zone.is_none() {
                continue;
            }
            let sql = format!(
                "UPDATE note_tags SET {stamp}_offset = ?, {stamp}_zone = ? WHERE note_id = ? AND tag_id = ? AND {stamp} = ?"
            );
            self.conn.execute(&sql, params![offset, zone, note, tag, ts])?;
        }
        Ok(())
    }

    /// Stamp the timezone of this device on a timestamp it has just written.
    pub fn stamp_local_zone(&self, table: &str, id_hex: &str, stamp: &str) -> VoiceResult<()> {
        let id = Uuid::parse_str(id_hex)
            .map_err(|e| VoiceError::validation("id", e.to_string()))?
            .as_bytes()
            .to_vec();
        let zone = crate::timezone::local_zone();
        let sql = format!(
            "UPDATE {table} SET {stamp}_offset = ?, {stamp}_zone = ? WHERE id = ? AND {stamp}_offset IS NULL"
        );
        self.conn.execute(&sql, params![zone.offset_seconds, zone.name, id])?;
        Ok(())
    }

    /// Send a row again although none of its values changed.
    ///
    /// The sequence triggers only fire when something is written, so a device
    /// that rejects a peer's value has nothing to send and the peer would keep
    /// its losing value for ever. Giving the row a new sequence number puts it
    /// back in the feed; the peer adopts the winning value, and because its own
    /// value then matches, the exchange stops there.
    fn republish(&self, table: &str, id: &[u8]) -> VoiceResult<()> {
        self.conn.execute("UPDATE sync_sequence SET value = value + 1 WHERE id = 1", [])?;
        let sql = format!(
            "UPDATE {table} SET seq = (SELECT value FROM sync_sequence WHERE id = 1) WHERE id = ?"
        );
        self.conn.execute(&sql, params![id.to_vec()])?;
        Ok(())
    }

    /// Whether a table already has a column.
    fn has_column(conn: &Connection, table: &str, column: &str) -> bool {
        let sql = format!("PRAGMA table_info({})", table);
        let Ok(mut stmt) = conn.prepare(&sql) else { return false };
        let Ok(rows) = stmt.query_map([], |row| row.get::<_, String>(1)) else { return false };
        let found = rows.filter_map(|r| r.ok()).any(|name| name == column);
        found
    }

    /// Put the timezone of the action next to every user-visible timestamp.
    ///
    /// A Unix timestamp is an instant and cannot say what the clock read where
    /// the action happened, so each one is followed by `<stamp>_offset`
    /// (seconds east of UTC at that moment) and `<stamp>_zone` (the IANA name
    /// when the device knew it). A note recorded at 15:20 in Jerusalem is then
    /// still shown as 15:20 from New York.
    ///
    /// Sync bookkeeping (`sync_received_at`, `last_sync_at`, `seq`) and the
    /// cloud upload time deliberately get none: no screen shows them, and they
    /// are machine events rather than something a person did.
    ///
    /// Rows written before this migration keep NULL, and a reader shows those
    /// in its own timezone, exactly as it did before.
    fn migrate_add_timezone_columns(&mut self) -> VoiceResult<()> {
        for (table, stamps) in STAMPED_COLUMNS {
            for stamp in stamps.iter() {
                for (suffix, kind) in [("offset", "INTEGER"), ("zone", "TEXT")] {
                    let column = format!("{}_{}", stamp, suffix);
                    if !Self::has_column(&self.conn, table, &column) {
                        self.conn.execute_batch(&format!(
                            "ALTER TABLE {} ADD COLUMN {} {}",
                            table, column, kind
                        ))?;
                    }
                }
            }
        }
        Ok(())
    }

    fn migrate_drop_legacy_conflict_tables(&mut self) -> VoiceResult<()> {
        self.conn.execute_batch(
            r#"
            DROP TABLE IF EXISTS conflicts_note_content;
            DROP TABLE IF EXISTS conflicts_note_delete;
            DROP TABLE IF EXISTS conflicts_tag_rename;
            DROP TABLE IF EXISTS conflicts_tag_parent;
            DROP TABLE IF EXISTS conflicts_tag_delete;
            DROP TABLE IF EXISTS conflicts_note_tag;
            "#,
        )?;
        Ok(())
    }

    /// Rebuild the display caches of every note that shows a transcription.
    pub(crate) fn rebuild_caches_for_transcription(&self, transcription_id: &str) {
        // Looked up directly so that a transcription that was just deleted
        // still refreshes the caches of its audio file and note.
        let audio: Option<Vec<u8>> = Uuid::parse_str(transcription_id).ok().and_then(|u| {
            self.conn
                .query_row(
                    "SELECT audio_file_id FROM transcriptions WHERE id = ?",
                    params![u.as_bytes().to_vec()],
                    |r| r.get(0),
                )
                .optional()
                .ok()
                .flatten()
        });
        if let Some(hex) = audio.and_then(|a| Uuid::from_slice(&a).ok()).map(|u| u.simple().to_string()) {
            self.rebuild_caches_for_audio_file(&hex);
        }
    }

    /// Remember a change from a peer that could not be applied, so it is
    /// retried on the next sync instead of being silently dropped.
    pub fn record_sync_failure(
        &self,
        peer_device_id: &str,
        peer_device_name: Option<&str>,
        change: &SyncChange,
        error: &str,
    ) -> VoiceResult<()> {
        let peer_bytes = Uuid::parse_str(peer_device_id)
            .map(|u| u.as_bytes().to_vec())
            .unwrap_or_else(|_| vec![0u8; 16]);
        let payload = serde_json::to_string(change)?;
        // The peer row may not exist yet on the first exchange (it is upserted
        // after the batch); the failure must still be queued.
        self.conn.execute(
            "INSERT OR IGNORE INTO sync_peers (peer_id, peer_name, peer_url) VALUES (?, ?, '')",
            params![peer_bytes, peer_device_name],
        )?;
        // One pending row per (peer, entity, operation): replace an older failure of the same change.
        self.conn.execute(
            "DELETE FROM sync_failures WHERE peer_id = ? AND entity_type = ? AND operation = ? AND resolved_at IS NULL AND payload = ?",
            params![peer_bytes, change.entity_type, change.operation, payload],
        )?;
        self.conn.execute(
            r#"
            INSERT INTO sync_failures (id, peer_id, peer_name, entity_type, entity_id, operation, payload, error_message, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, strftime('%s', 'now'))
            "#,
            params![
                Uuid::now_v7().as_bytes().to_vec(),
                peer_bytes,
                peer_device_name,
                change.entity_type,
                Uuid::parse_str(&change.entity_id).ok().map(|u| u.as_bytes().to_vec()),
                change.operation,
                payload,
                error,
            ],
        )?;
        Ok(())
    }

    /// Pending (unresolved) failures, oldest first: (failure id hex, change).
    pub fn get_pending_sync_failures(&self) -> VoiceResult<Vec<(String, SyncChange)>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, payload FROM sync_failures WHERE resolved_at IS NULL ORDER BY created_at, id",
        )?;
        let rows = stmt.query_map([], |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, String>(1)?)))?;
        let mut out = Vec::new();
        for row in rows {
            let (id, payload) = row?;
            match serde_json::from_str::<SyncChange>(&payload) {
                Ok(change) => out.push((uuid_bytes_to_hex(&id).unwrap_or_default(), change)),
                Err(e) => tracing::warn!("Unreadable sync failure payload: {}", e),
            }
        }
        Ok(out)
    }

    /// Mark a failure as dealt with.
    pub fn resolve_sync_failure(&self, failure_id: &str) -> VoiceResult<()> {
        let id = Uuid::parse_str(failure_id).map_err(|e| VoiceError::validation("failure_id", e.to_string()))?;
        self.conn.execute(
            "UPDATE sync_failures SET resolved_at = strftime('%s', 'now') WHERE id = ?",
            params![id.as_bytes().to_vec()],
        )?;
        Ok(())
    }

    /// Number of pending sync failures.
    pub fn count_pending_sync_failures(&self) -> VoiceResult<i64> {
        Ok(self.conn.query_row(
            "SELECT COUNT(*) FROM sync_failures WHERE resolved_at IS NULL",
            [],
            |row| row.get(0),
        )?)
    }
}

