//! Database operations for Voice.
//!
//! This module provides all data access functionality using SQLite.
//! All methods return JSON-serializable types to support CLI, web server, and Python modes.
//!
//! UUIDs are stored as BLOB (16 bytes) and converted to hex strings for JSON output.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};

use chrono::{DateTime, NaiveDateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension, Row};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{VoiceError, VoiceResult};
use crate::models::SyncChange;
use crate::validation::{
    validate_note_id, validate_search_query, validate_tag_id, validate_tag_path,
};

// Global device ID for local operations
static LOCAL_DEVICE_ID: OnceLock<Uuid> = OnceLock::new();

/// System tag name - parent of all hidden system tags (e.g., _marked)
pub const SYSTEM_TAG_NAME: &str = "_system";

/// Marked tag name - child of _system, used for starring/bookmarking notes
pub const MARKED_TAG_NAME: &str = "_marked";

/// Deterministic UUID for _system tag - same on all devices to prevent duplicates during sync.
pub const SYSTEM_TAG_UUID: &str = "a1b2c3d4-0000-5000-8000-000000000001";

/// Deterministic UUID for _marked tag - same on all devices to prevent duplicates during sync.
pub const MARKED_TAG_UUID: &str = "a1b2c3d4-0000-5000-8000-000000000002";

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


/// Note data returned from database queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoteRow {
    pub id: String,
    pub created_at: i64,
    pub content: String,
    pub modified_at: Option<i64>,
    pub deleted_at: Option<i64>,
    pub tag_names: Option<String>,
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

/// Parse SQLite datetime string, returning as-is
fn parse_sqlite_datetime(s: &str) -> String {
    // SQLite uses "YYYY-MM-DD HH:MM:SS" format - return unchanged
    s.to_string()
}

/// Database wrapper for SQLite operations
pub struct Database {
    conn: Connection,
}

impl Database {
    /// Create a new database connection
    pub fn new<P: AsRef<Path>>(db_path: P) -> VoiceResult<Self> {
        let conn = Connection::open(db_path)?;

        // Enable WAL mode for better concurrent access
        conn.execute_batch("PRAGMA journal_mode=WAL;")?;

        // Checkpoint any pending WAL frames to ensure we see the latest data
        // from other connections that may have written and closed
        conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE);")?;

        let mut db = Self { conn };
        db.init_database()?;
        db.migrate_add_sync_received_at()?;
        db.migrate_timestamps_to_unix()?;
        db.migrate_add_storage_columns()?;
        db.migrate_add_file_storage_config_table()?;
        Ok(db)
    }

    /// Create an in-memory database (for testing)
    pub fn new_in_memory() -> VoiceResult<Self> {
        let conn = Connection::open_in_memory()?;
        let mut db = Self { conn };
        db.init_database()?;
        db.migrate_add_sync_received_at()?;
        db.migrate_timestamps_to_unix()?;
        db.migrate_add_storage_columns()?;
        db.migrate_add_file_storage_config_table()?;
        Ok(db)
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
                di_cache_note_list_pane_display TEXT
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
                storage_uploaded_at INTEGER -- When file was uploaded to cloud
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
        let system_bytes = system_uuid.as_bytes().to_vec();
        let marked_bytes = marked_uuid.as_bytes().to_vec();

        // Insert system tags if they don't exist (OR IGNORE handles duplicates)
        self.conn.execute(
            "INSERT OR IGNORE INTO tags (id, name, parent_id, created_at) VALUES (?, ?, NULL, strftime('%s', 'now'))",
            params![&system_bytes, SYSTEM_TAG_NAME],
        )?;
        self.conn.execute(
            "INSERT OR IGNORE INTO tags (id, name, parent_id, created_at) VALUES (?, ?, ?, strftime('%s', 'now'))",
            params![&marked_bytes, MARKED_TAG_NAME, &system_bytes],
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
                n.di_cache_note_list_pane_display
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
                n.di_cache_note_list_pane_display
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
    ) -> VoiceResult<(String, String)> {
        // 1. Create audio file record
        let audio_file_id = self.create_audio_file_with_duration(filename, file_created_at, duration_seconds)?;

        // 2. Create note with file's creation date (empty content)
        let note_id = self.create_note_with_timestamp("", file_created_at)?;

        // 3. Attach audio file to note
        self.attach_to_note(&note_id, &audio_file_id, "audio_file")?;

        Ok((note_id, audio_file_id))
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
        let uuid_bytes = uuid.as_bytes().to_vec();

        if content.trim().is_empty() {
            return Err(VoiceError::validation("content", "Note content cannot be empty"));
        }

        let updated = self.conn.execute(
            r#"
            UPDATE notes
            SET content = ?, modified_at = strftime('%s', 'now')
            WHERE id = ? AND deleted_at IS NULL
            "#,
            params![content, uuid_bytes],
        )?;

        // Rebuild display caches if update succeeded
        if updated > 0 {
            let _ = self.rebuild_note_cache(&resolved_id);
            let _ = self.rebuild_note_list_cache(&resolved_id);
        }

        Ok(updated > 0)
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
        let uuid_bytes = uuid.as_bytes().to_vec();

        let deleted = self.conn.execute(
            r#"
            UPDATE notes
            SET deleted_at = strftime('%s', 'now'), modified_at = strftime('%s', 'now')
            WHERE id = ? AND deleted_at IS NULL
            "#,
            params![uuid_bytes],
        )?;

        Ok(deleted > 0)
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

        // 2. Get both notes (must exist and not be deleted)
        let note_1: (String, String, Option<String>) = self
            .conn
            .query_row(
                "SELECT created_at, content, deleted_at FROM notes WHERE id = ?",
                params![&bytes_1],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(|_| VoiceError::validation("note_id_1", "Note not found"))?;

        let note_2: (String, String, Option<String>) = self
            .conn
            .query_row(
                "SELECT created_at, content, deleted_at FROM notes WHERE id = ?",
                params![&bytes_2],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(|_| VoiceError::validation("note_id_2", "Note not found"))?;

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

        // 5. Update survivor's content
        self.conn.execute(
            "UPDATE notes SET content = ?, modified_at = strftime('%s', 'now') WHERE id = ?",
            params![&merged_content, &survivor_bytes],
        )?;

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
            // Check if survivor already has this tag (active)
            let survivor_has_tag: bool = self
                .conn
                .query_row(
                    "SELECT 1 FROM note_tags WHERE note_id = ? AND tag_id = ? AND deleted_at IS NULL",
                    params![&survivor_bytes, &tag_bytes],
                    |_| Ok(true),
                )
                .unwrap_or(false);

            if survivor_has_tag {
                // Soft-delete the victim's association (duplicate)
                self.conn.execute(
                    "UPDATE note_tags SET deleted_at = strftime('%s', 'now'), modified_at = strftime('%s', 'now') WHERE note_id = ? AND tag_id = ?",
                    params![&victim_bytes, &tag_bytes],
                )?;
            } else {
                // Check if survivor has a soft-deleted association we can reactivate
                let survivor_had_tag: bool = self
                    .conn
                    .query_row(
                        "SELECT 1 FROM note_tags WHERE note_id = ? AND tag_id = ? AND deleted_at IS NOT NULL",
                        params![&survivor_bytes, &tag_bytes],
                        |_| Ok(true),
                    )
                    .unwrap_or(false);

                if survivor_had_tag {
                    // Reactivate survivor's association
                    self.conn.execute(
                        "UPDATE note_tags SET deleted_at = NULL, modified_at = strftime('%s', 'now') WHERE note_id = ? AND tag_id = ?",
                        params![&survivor_bytes, &tag_bytes],
                    )?;
                    // Soft-delete victim's association
                    self.conn.execute(
                        "UPDATE note_tags SET deleted_at = strftime('%s', 'now'), modified_at = strftime('%s', 'now') WHERE note_id = ? AND tag_id = ?",
                        params![&victim_bytes, &tag_bytes],
                    )?;
                } else {
                    // Move the association to survivor
                    self.conn.execute(
                        "UPDATE note_tags SET note_id = ?, modified_at = strftime('%s', 'now') WHERE note_id = ? AND tag_id = ?",
                        params![&survivor_bytes, &victim_bytes, &tag_bytes],
                    )?;
                }
            }
        }

        // 7. Move attachments from victim to survivor
        self.conn.execute(
            "UPDATE note_attachments SET note_id = ?, modified_at = strftime('%s', 'now') WHERE note_id = ? AND deleted_at IS NULL",
            params![&survivor_bytes, &victim_bytes],
        )?;

        // 8. Soft-delete the victim note
        self.conn.execute(
            "UPDATE notes SET deleted_at = strftime('%s', 'now'), modified_at = strftime('%s', 'now') WHERE id = ?",
            params![&victim_bytes],
        )?;

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
        let uuid_bytes = uuid.as_bytes().to_vec();

        // Soft delete: set deleted_at and modified_at timestamps
        let deleted = self.conn.execute(
            r#"
            UPDATE tags
            SET deleted_at = strftime('%s', 'now'), modified_at = strftime('%s', 'now')
            WHERE id = ? AND deleted_at IS NULL
            "#,
            params![uuid_bytes],
        )?;

        Ok(deleted > 0)
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
                UNION ALL
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
                n.di_cache_note_list_pane_display
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
                n.di_cache_note_list_pane_display
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

        let parent_bytes = match parent_id {
            Some(pid) => {
                let resolved_id = self.resolve_tag_id(pid)?;
                let uuid = Uuid::parse_str(&resolved_id).map_err(|e| VoiceError::validation("parent_id", e.to_string()))?;
                Some(uuid.as_bytes().to_vec())
            }
            None => None,
        };

        self.conn.execute(
            "INSERT INTO tags (id, name, parent_id, created_at) VALUES (?, ?, ?, strftime('%s', 'now'))",
            params![uuid_bytes, name, parent_bytes],
        )?;

        Ok(tag_id.simple().to_string())
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
        let uuid_bytes = uuid.as_bytes().to_vec();

        let updated = self.conn.execute(
            "UPDATE tags SET name = ?, modified_at = strftime('%s', 'now') WHERE id = ?",
            params![new_name, uuid_bytes],
        )?;

        Ok(updated > 0)
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

        let updated = self.conn.execute(
            "UPDATE tags SET parent_id = ?, modified_at = strftime('%s', 'now') WHERE id = ?",
            params![parent_bytes, tag_bytes],
        )?;

        Ok(updated > 0)
    }

    /// Check if a tag is a descendant of another tag
    fn is_tag_descendant_of(&self, potential_descendant: &str, potential_ancestor: &str) -> VoiceResult<bool> {
        let descendant_uuid = Uuid::parse_str(potential_descendant)
            .map_err(|e| VoiceError::validation("potential_descendant", e.to_string()))?;
        let ancestor_uuid = Uuid::parse_str(potential_ancestor)
            .map_err(|e| VoiceError::validation("potential_ancestor", e.to_string()))?;
        let descendant_bytes = descendant_uuid.as_bytes().to_vec();
        let ancestor_bytes = ancestor_uuid.as_bytes().to_vec();

        // Walk up the parent chain from descendant to see if we hit ancestor
        let mut current_id = descendant_bytes;
        loop {
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
        let note_bytes = note_uuid.as_bytes().to_vec();
        let tag_bytes = tag_uuid.as_bytes().to_vec();

        // Check if association exists (including soft-deleted)
        let existing: Option<Option<i64>> = self
            .conn
            .query_row(
                "SELECT deleted_at FROM note_tags WHERE note_id = ? AND tag_id = ?",
                params![&note_bytes, &tag_bytes],
                |row| row.get::<_, Option<i64>>(0),
            )
            .optional()?;

        let changed = match existing {
            Some(deleted_at) => {
                if deleted_at.is_some() {
                    // Reactivate soft-deleted association
                    self.conn.execute(
                        "UPDATE note_tags SET deleted_at = NULL, modified_at = strftime('%s', 'now') WHERE note_id = ? AND tag_id = ?",
                        params![&note_bytes, &tag_bytes],
                    )?;
                    true
                } else {
                    // Already active
                    false
                }
            }
            None => {
                // Create new association
                self.conn.execute(
                    "INSERT INTO note_tags (note_id, tag_id, created_at) VALUES (?, ?, strftime('%s', 'now'))",
                    params![&note_bytes, &tag_bytes],
                )?;
                true
            }
        };

        // Update caches and return result
        let mut list_cache_rebuilt = false;
        if changed {
            // Update the parent Note's modified_at to trigger sync
            self.conn.execute(
                "UPDATE notes SET modified_at = strftime('%s', 'now') WHERE id = ?",
                params![note_bytes],
            )?;
            // Rebuild note pane cache (tags list changed)
            let _ = self.rebuild_note_cache(&resolved_note_id);
            // Always rebuild list cache since tags are displayed in the list pane
            let _ = self.rebuild_note_list_cache(&resolved_note_id);
            list_cache_rebuilt = true;
        }

        Ok(TagChangeResult {
            changed,
            note_id: resolved_note_id,
            list_cache_rebuilt,
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
        let note_bytes = note_uuid.as_bytes().to_vec();
        let tag_bytes = tag_uuid.as_bytes().to_vec();

        let updated = self.conn.execute(
            "UPDATE note_tags SET deleted_at = strftime('%s', 'now'), modified_at = strftime('%s', 'now') WHERE note_id = ? AND tag_id = ? AND deleted_at IS NULL",
            params![&note_bytes, &tag_bytes],
        )?;

        let changed = updated > 0;
        let mut list_cache_rebuilt = false;

        // Update the parent Note's modified_at to trigger sync
        if changed {
            self.conn.execute(
                "UPDATE notes SET modified_at = strftime('%s', 'now') WHERE id = ?",
                params![note_bytes],
            )?;
            // Rebuild note pane cache (tags list changed)
            let _ = self.rebuild_note_cache(&resolved_note_id);

            // Always rebuild list cache (tags are now shown in list view)
            let _ = self.rebuild_note_list_cache(&resolved_note_id);
            list_cache_rebuilt = true;
        }

        Ok(TagChangeResult {
            changed,
            note_id: resolved_note_id,
            list_cache_rebuilt,
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
        self.conn.execute("UPDATE sync_peers SET last_sync_at = NULL", [])?;
        Ok(())
    }

    /// Get all changes since a timestamp (for sync)
    /// The `since` parameter is a Unix timestamp (seconds since epoch).
    /// Returns changes where: sync_received_at >= since OR modified_at >= since OR created_at >= since
    pub fn get_changes_since(&self, since: Option<i64>, limit: i64) -> VoiceResult<(Vec<HashMap<String, serde_json::Value>>, Option<i64>)> {
        let mut changes = Vec::new();
        let mut latest_timestamp: Option<i64> = None;

        let since_ts: Option<i64> = since;

        // Get note changes
        // Fixed query: include entities where ANY timestamp is >= since (not just sync_received_at)
        let note_rows: Vec<(Vec<u8>, i64, String, Option<i64>, Option<i64>)> = if let Some(ts) = since_ts {
            let mut stmt = self.conn.prepare(
                r#"
                SELECT id, created_at, content, modified_at, deleted_at
                FROM notes
                WHERE sync_received_at >= ? OR modified_at >= ? OR created_at >= ?
                ORDER BY COALESCE(sync_received_at, modified_at, created_at)
                LIMIT ?
                "#,
            )?;
            let rows = stmt.query_map(params![ts, ts, ts, limit], |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                ))
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        } else {
            let mut stmt = self.conn.prepare(
                r#"
                SELECT id, created_at, content, modified_at, deleted_at
                FROM notes
                ORDER BY COALESCE(modified_at, created_at)
                LIMIT ?
                "#,
            )?;
            let rows = stmt.query_map(params![limit], |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                ))
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };

        for (id_bytes, created_at, content, modified_at, deleted_at) in note_rows {
            let timestamp = modified_at.unwrap_or(created_at);
            let operation = if deleted_at.is_some() {
                "delete"
            } else if modified_at.is_some() {
                "update"
            } else {
                "create"
            };

            let id_hex = uuid_bytes_to_hex(&id_bytes).unwrap_or_default();
            let mut change = HashMap::new();
            change.insert("entity_type".to_string(), serde_json::Value::String("note".to_string()));
            change.insert("entity_id".to_string(), serde_json::Value::String(id_hex.clone()));
            change.insert("operation".to_string(), serde_json::Value::String(operation.to_string()));
            change.insert("timestamp".to_string(), serde_json::Value::Number(timestamp.into()));

            let mut data = serde_json::Map::new();
            data.insert("id".to_string(), serde_json::Value::String(id_hex));
            data.insert("created_at".to_string(), serde_json::Value::Number(created_at.into()));
            data.insert("content".to_string(), serde_json::Value::String(content));
            data.insert("modified_at".to_string(), modified_at.map_or(serde_json::Value::Null, |ts| serde_json::Value::Number(ts.into())));
            data.insert("deleted_at".to_string(), deleted_at.map_or(serde_json::Value::Null, |ts| serde_json::Value::Number(ts.into())));
            change.insert("data".to_string(), serde_json::Value::Object(data));

            latest_timestamp = Some(timestamp);
            changes.push(change);
        }

        // Get tag changes
        // Fixed query: include entities where ANY timestamp is >= since
        let remaining = limit - changes.len() as i64;
        if remaining > 0 {
            let tag_rows: Vec<(Vec<u8>, String, Option<Vec<u8>>, i64, Option<i64>, Option<i64>)> = if let Some(ts) = since_ts {
                let mut stmt = self.conn.prepare(
                    r#"
                    SELECT id, name, parent_id, created_at, modified_at, deleted_at
                    FROM tags
                    WHERE sync_received_at >= ? OR modified_at >= ? OR created_at >= ?
                    ORDER BY COALESCE(sync_received_at, modified_at, created_at)
                    LIMIT ?
                    "#,
                )?;
                let rows = stmt.query_map(params![ts, ts, ts, remaining], |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<Vec<u8>>>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, Option<i64>>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                    ))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            } else {
                let mut stmt = self.conn.prepare(
                    r#"
                    SELECT id, name, parent_id, created_at, modified_at, deleted_at
                    FROM tags
                    ORDER BY COALESCE(modified_at, created_at)
                    LIMIT ?
                    "#,
                )?;
                let rows = stmt.query_map(params![remaining], |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<Vec<u8>>>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, Option<i64>>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                    ))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            };

            for (id_bytes, name, parent_id_bytes, created_at, modified_at, deleted_at) in tag_rows {
                let timestamp = modified_at.unwrap_or(created_at);
                let operation = if deleted_at.is_some() {
                    "delete"
                } else if modified_at.is_some() {
                    "update"
                } else {
                    "create"
                };

                let id_hex = uuid_bytes_to_hex(&id_bytes).unwrap_or_default();
                let parent_id_hex = parent_id_bytes.and_then(|b| uuid_bytes_to_hex(&b));

                let mut change = HashMap::new();
                change.insert("entity_type".to_string(), serde_json::Value::String("tag".to_string()));
                change.insert("entity_id".to_string(), serde_json::Value::String(id_hex.clone()));
                change.insert("operation".to_string(), serde_json::Value::String(operation.to_string()));
                change.insert("timestamp".to_string(), serde_json::Value::Number(timestamp.into()));

                let mut data = serde_json::Map::new();
                data.insert("id".to_string(), serde_json::Value::String(id_hex));
                data.insert("name".to_string(), serde_json::Value::String(name));
                data.insert("parent_id".to_string(), parent_id_hex.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
                data.insert("created_at".to_string(), serde_json::Value::Number(created_at.into()));
                data.insert("modified_at".to_string(), modified_at.map_or(serde_json::Value::Null, |ts| serde_json::Value::Number(ts.into())));
                data.insert("deleted_at".to_string(), deleted_at.map_or(serde_json::Value::Null, |ts| serde_json::Value::Number(ts.into())));
                change.insert("data".to_string(), serde_json::Value::Object(data));

                if latest_timestamp.is_none() || timestamp > latest_timestamp.unwrap() {
                    latest_timestamp = Some(timestamp);
                }
                changes.push(change);
            }
        }

        // Get note_tag changes
        // Fixed query: include entities where ANY timestamp is >= since
        let remaining = limit - changes.len() as i64;
        if remaining > 0 {
            let nt_rows: Vec<(Vec<u8>, Vec<u8>, i64, Option<i64>, Option<i64>)> = if let Some(ts) = since_ts {
                let mut stmt = self.conn.prepare(
                    r#"
                    SELECT note_id, tag_id, created_at, modified_at, deleted_at
                    FROM note_tags
                    WHERE sync_received_at >= ? OR modified_at >= ? OR deleted_at >= ? OR created_at >= ?
                    ORDER BY COALESCE(sync_received_at, modified_at, deleted_at, created_at)
                    LIMIT ?
                    "#,
                )?;
                let rows = stmt.query_map(params![ts, ts, ts, ts, remaining], |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                        row.get::<_, Option<i64>>(4)?,
                    ))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            } else {
                let mut stmt = self.conn.prepare(
                    r#"
                    SELECT note_id, tag_id, created_at, modified_at, deleted_at
                    FROM note_tags
                    ORDER BY COALESCE(modified_at, deleted_at, created_at)
                    LIMIT ?
                    "#,
                )?;
                let rows = stmt.query_map(params![remaining], |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                        row.get::<_, Option<i64>>(4)?,
                    ))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            };

            for (note_id_bytes, tag_id_bytes, created_at, modified_at, deleted_at) in nt_rows {
                let timestamp = modified_at
                    .or(deleted_at)
                    .unwrap_or(created_at);

                let operation = if deleted_at.is_some() {
                    "delete"
                } else if modified_at.is_some() {
                    "update"
                } else {
                    "create"
                };

                let note_id_hex = uuid_bytes_to_hex(&note_id_bytes).unwrap_or_default();
                let tag_id_hex = uuid_bytes_to_hex(&tag_id_bytes).unwrap_or_default();
                let entity_id = format!("{}:{}", note_id_hex, tag_id_hex);

                let mut change = HashMap::new();
                change.insert("entity_type".to_string(), serde_json::Value::String("note_tag".to_string()));
                change.insert("entity_id".to_string(), serde_json::Value::String(entity_id));
                change.insert("operation".to_string(), serde_json::Value::String(operation.to_string()));
                change.insert("timestamp".to_string(), serde_json::Value::Number(timestamp.into()));

                let mut data = serde_json::Map::new();
                data.insert("note_id".to_string(), serde_json::Value::String(note_id_hex));
                data.insert("tag_id".to_string(), serde_json::Value::String(tag_id_hex));
                data.insert("created_at".to_string(), serde_json::Value::Number(created_at.into()));
                data.insert("modified_at".to_string(), modified_at.map_or(serde_json::Value::Null, |ts| serde_json::Value::Number(ts.into())));
                data.insert("deleted_at".to_string(), deleted_at.map_or(serde_json::Value::Null, |ts| serde_json::Value::Number(ts.into())));
                change.insert("data".to_string(), serde_json::Value::Object(data));

                if latest_timestamp.is_none() || timestamp > latest_timestamp.unwrap() {
                    latest_timestamp = Some(timestamp);
                }
                changes.push(change);
            }
        }

        // Get audio_file changes
        // Fixed query: include entities where ANY timestamp is >= since
        let remaining = limit - changes.len() as i64;
        if remaining > 0 {
            let audio_rows: Vec<(Vec<u8>, i64, String, Option<i64>, Option<String>, Option<i64>, Option<i64>, Option<String>, Option<String>, Option<i64>)> = if let Some(ts) = since_ts {
                let mut stmt = self.conn.prepare(
                    r#"
                    SELECT id, imported_at, filename, file_created_at, summary, modified_at, deleted_at,
                           storage_provider, storage_key, storage_uploaded_at
                    FROM audio_files
                    WHERE sync_received_at >= ? OR modified_at >= ? OR imported_at >= ?
                    ORDER BY COALESCE(sync_received_at, modified_at, imported_at)
                    LIMIT ?
                    "#,
                )?;
                let rows = stmt.query_map(params![ts, ts, ts, remaining], |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                        row.get::<_, Option<i64>>(6)?,
                        row.get::<_, Option<String>>(7)?,
                        row.get::<_, Option<String>>(8)?,
                        row.get::<_, Option<i64>>(9)?,
                    ))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            } else {
                let mut stmt = self.conn.prepare(
                    r#"
                    SELECT id, imported_at, filename, file_created_at, summary, modified_at, deleted_at,
                           storage_provider, storage_key, storage_uploaded_at
                    FROM audio_files
                    ORDER BY COALESCE(modified_at, imported_at)
                    LIMIT ?
                    "#,
                )?;
                let rows = stmt.query_map(params![remaining], |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                        row.get::<_, Option<i64>>(6)?,
                        row.get::<_, Option<String>>(7)?,
                        row.get::<_, Option<String>>(8)?,
                        row.get::<_, Option<i64>>(9)?,
                    ))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            };

            for (id_bytes, imported_at, filename, file_created_at, summary, modified_at, deleted_at, storage_provider, storage_key, storage_uploaded_at) in audio_rows {
                let timestamp = modified_at.unwrap_or(imported_at);
                let operation = if deleted_at.is_some() {
                    "delete"
                } else if modified_at.is_some() {
                    "update"
                } else {
                    "create"
                };

                let id_hex = uuid_bytes_to_hex(&id_bytes).unwrap_or_default();
                let mut change = HashMap::new();
                change.insert("entity_type".to_string(), serde_json::Value::String("audio_file".to_string()));
                change.insert("entity_id".to_string(), serde_json::Value::String(id_hex.clone()));
                change.insert("operation".to_string(), serde_json::Value::String(operation.to_string()));
                change.insert("timestamp".to_string(), serde_json::Value::Number(timestamp.into()));

                let mut data = serde_json::Map::new();
                data.insert("id".to_string(), serde_json::Value::String(id_hex));
                data.insert("imported_at".to_string(), serde_json::Value::Number(imported_at.into()));
                data.insert("filename".to_string(), serde_json::Value::String(filename));
                data.insert("file_created_at".to_string(), file_created_at.map_or(serde_json::Value::Null, |ts| serde_json::Value::Number(ts.into())));
                data.insert("summary".to_string(), summary.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
                data.insert("modified_at".to_string(), modified_at.map_or(serde_json::Value::Null, |ts| serde_json::Value::Number(ts.into())));
                data.insert("deleted_at".to_string(), deleted_at.map_or(serde_json::Value::Null, |ts| serde_json::Value::Number(ts.into())));
                data.insert("storage_provider".to_string(), storage_provider.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
                data.insert("storage_key".to_string(), storage_key.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
                data.insert("storage_uploaded_at".to_string(), storage_uploaded_at.map_or(serde_json::Value::Null, |ts| serde_json::Value::Number(ts.into())));
                change.insert("data".to_string(), serde_json::Value::Object(data));

                if latest_timestamp.is_none() || timestamp > latest_timestamp.unwrap() {
                    latest_timestamp = Some(timestamp);
                }
                changes.push(change);
            }
        }

        // Get note_attachment changes
        // Fixed query: include entities where ANY timestamp is >= since
        let remaining = limit - changes.len() as i64;
        if remaining > 0 {
            let na_rows: Vec<(Vec<u8>, Vec<u8>, Vec<u8>, String, i64, Option<i64>, Option<i64>)> = if let Some(ts) = since_ts {
                let mut stmt = self.conn.prepare(
                    r#"
                    SELECT id, note_id, attachment_id, attachment_type, created_at, modified_at, deleted_at
                    FROM note_attachments
                    WHERE sync_received_at >= ? OR modified_at >= ? OR deleted_at >= ? OR created_at >= ?
                    ORDER BY COALESCE(sync_received_at, modified_at, deleted_at, created_at)
                    LIMIT ?
                    "#,
                )?;
                let rows = stmt.query_map(params![ts, ts, ts, ts, remaining], |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                        row.get::<_, Option<i64>>(6)?,
                    ))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            } else {
                let mut stmt = self.conn.prepare(
                    r#"
                    SELECT id, note_id, attachment_id, attachment_type, created_at, modified_at, deleted_at
                    FROM note_attachments
                    ORDER BY COALESCE(modified_at, deleted_at, created_at)
                    LIMIT ?
                    "#,
                )?;
                let rows = stmt.query_map(params![remaining], |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                        row.get::<_, Option<i64>>(6)?,
                    ))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            };

            for (id_bytes, note_id_bytes, attachment_id_bytes, attachment_type, created_at, modified_at, deleted_at) in na_rows {
                let timestamp = modified_at
                    .or(deleted_at)
                    .unwrap_or(created_at);

                let operation = if deleted_at.is_some() {
                    "delete"
                } else if modified_at.is_some() {
                    "update"
                } else {
                    "create"
                };

                let id_hex = uuid_bytes_to_hex(&id_bytes).unwrap_or_default();
                let note_id_hex = uuid_bytes_to_hex(&note_id_bytes).unwrap_or_default();
                let attachment_id_hex = uuid_bytes_to_hex(&attachment_id_bytes).unwrap_or_default();

                let mut change = HashMap::new();
                change.insert("entity_type".to_string(), serde_json::Value::String("note_attachment".to_string()));
                change.insert("entity_id".to_string(), serde_json::Value::String(id_hex.clone()));
                change.insert("operation".to_string(), serde_json::Value::String(operation.to_string()));
                change.insert("timestamp".to_string(), serde_json::Value::Number(timestamp.into()));

                let mut data = serde_json::Map::new();
                data.insert("id".to_string(), serde_json::Value::String(id_hex));
                data.insert("note_id".to_string(), serde_json::Value::String(note_id_hex));
                data.insert("attachment_id".to_string(), serde_json::Value::String(attachment_id_hex));
                data.insert("attachment_type".to_string(), serde_json::Value::String(attachment_type));
                data.insert("created_at".to_string(), serde_json::Value::Number(created_at.into()));
                data.insert("modified_at".to_string(), modified_at.map_or(serde_json::Value::Null, |ts| serde_json::Value::Number(ts.into())));
                data.insert("deleted_at".to_string(), deleted_at.map_or(serde_json::Value::Null, |ts| serde_json::Value::Number(ts.into())));
                change.insert("data".to_string(), serde_json::Value::Object(data));

                if latest_timestamp.is_none() || timestamp > latest_timestamp.unwrap() {
                    latest_timestamp = Some(timestamp);
                }
                changes.push(change);
            }
        }

        // Get transcription changes
        // Fixed query: include entities where ANY timestamp is >= since
        let remaining = limit - changes.len() as i64;
        if remaining > 0 {
            let transcription_rows: Vec<(Vec<u8>, Vec<u8>, String, Option<String>, String, Option<String>, Option<String>, String, Vec<u8>, i64, Option<i64>, Option<i64>)> = if let Some(ts) = since_ts {
                let mut stmt = self.conn.prepare(
                    r#"
                    SELECT id, audio_file_id, content, content_segments, service, service_arguments, service_response, state, device_id, created_at, modified_at, deleted_at
                    FROM transcriptions
                    WHERE sync_received_at >= ? OR modified_at >= ? OR created_at >= ?
                    ORDER BY COALESCE(sync_received_at, modified_at, created_at)
                    LIMIT ?
                    "#,
                )?;
                let rows = stmt.query_map(params![ts, ts, ts, remaining], |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, Vec<u8>>(8)?,
                        row.get::<_, i64>(9)?,
                        row.get::<_, Option<i64>>(10)?,
                        row.get::<_, Option<i64>>(11)?,
                    ))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            } else {
                let mut stmt = self.conn.prepare(
                    r#"
                    SELECT id, audio_file_id, content, content_segments, service, service_arguments, service_response, state, device_id, created_at, modified_at, deleted_at
                    FROM transcriptions
                    ORDER BY COALESCE(modified_at, created_at)
                    LIMIT ?
                    "#,
                )?;
                let rows = stmt.query_map(params![remaining], |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, Vec<u8>>(8)?,
                        row.get::<_, i64>(9)?,
                        row.get::<_, Option<i64>>(10)?,
                        row.get::<_, Option<i64>>(11)?,
                    ))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            };

            for (id_bytes, audio_file_id_bytes, content, content_segments, service, service_arguments, service_response, state, device_id_bytes, created_at, modified_at, deleted_at) in transcription_rows {
                let timestamp = modified_at.unwrap_or(created_at);
                let operation = if deleted_at.is_some() {
                    "delete"
                } else if modified_at.is_some() {
                    "update"
                } else {
                    "create"
                };

                let id_hex = uuid_bytes_to_hex(&id_bytes).unwrap_or_default();
                let audio_file_id_hex = uuid_bytes_to_hex(&audio_file_id_bytes).unwrap_or_default();
                let device_id_hex = uuid_bytes_to_hex(&device_id_bytes).unwrap_or_default();

                let mut change = HashMap::new();
                change.insert("entity_type".to_string(), serde_json::Value::String("transcription".to_string()));
                change.insert("entity_id".to_string(), serde_json::Value::String(id_hex.clone()));
                change.insert("operation".to_string(), serde_json::Value::String(operation.to_string()));
                change.insert("timestamp".to_string(), serde_json::Value::Number(timestamp.into()));

                let mut data = serde_json::Map::new();
                data.insert("id".to_string(), serde_json::Value::String(id_hex));
                data.insert("audio_file_id".to_string(), serde_json::Value::String(audio_file_id_hex));
                data.insert("content".to_string(), serde_json::Value::String(content));
                data.insert("content_segments".to_string(), content_segments.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
                data.insert("service".to_string(), serde_json::Value::String(service));
                data.insert("service_arguments".to_string(), service_arguments.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
                data.insert("service_response".to_string(), service_response.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
                data.insert("state".to_string(), serde_json::Value::String(state));
                data.insert("device_id".to_string(), serde_json::Value::String(device_id_hex));
                data.insert("created_at".to_string(), serde_json::Value::Number(created_at.into()));
                data.insert("modified_at".to_string(), modified_at.map_or(serde_json::Value::Null, |ts| serde_json::Value::Number(ts.into())));
                data.insert("deleted_at".to_string(), deleted_at.map_or(serde_json::Value::Null, |ts| serde_json::Value::Number(ts.into())));
                change.insert("data".to_string(), serde_json::Value::Object(data));

                if latest_timestamp.is_none() || timestamp > latest_timestamp.unwrap() {
                    latest_timestamp = Some(timestamp);
                }
                changes.push(change);
            }
        }

        Ok((changes, latest_timestamp))
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

        // Convert HashMap changes to SyncChange structs
        let sync_changes: Vec<SyncChange> = changes
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
            .collect();

        Ok((sync_changes, latest_timestamp))
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
                      storage_provider, storage_key, storage_uploaded_at FROM audio_files"#
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
            ))
        })?;

        let mut audio_files = Vec::new();
        for row in af_rows {
            let (id_bytes, imported_at, filename, file_created_at, duration_seconds, summary, device_id_bytes, modified_at, deleted_at, storage_provider, storage_key, storage_uploaded_at) = row?;
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
    ) -> VoiceResult<bool> {
        let uuid = Uuid::parse_str(note_id)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?;
        let uuid_bytes = uuid.as_bytes().to_vec();

        // Check if note exists
        let existing: Option<i64> = self.conn
            .query_row("SELECT 1 FROM notes WHERE id = ?", params![&uuid_bytes], |row| row.get(0))
            .optional()?;

        if existing.is_some() {
            // Update existing note
            self.conn.execute(
                "UPDATE notes SET content = ?, modified_at = ?, deleted_at = ?, sync_received_at = ? WHERE id = ?",
                params![content, modified_at, deleted_at, sync_received_at, uuid_bytes],
            )?;
        } else {
            // Insert new note
            self.conn.execute(
                "INSERT INTO notes (id, created_at, content, modified_at, deleted_at, sync_received_at) VALUES (?, ?, ?, ?, ?, ?)",
                params![uuid_bytes, created_at, content, modified_at, deleted_at, sync_received_at],
            )?;
        }

        // Rebuild caches after sync (only if not deleted)
        if deleted_at.is_none() {
            // List cache: content_preview may have changed
            let _ = self.rebuild_note_list_cache(note_id);
            // Note pane cache: conflicts may have been created during sync
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

        let parent_bytes = match parent_id {
            Some(pid) => {
                let parent_uuid = Uuid::parse_str(pid)
                    .map_err(|e| VoiceError::validation("parent_id", e.to_string()))?;
                Some(parent_uuid.as_bytes().to_vec())
            }
            None => None,
        };

        // Check if tag exists
        let existing: Option<i64> = self.conn
            .query_row("SELECT 1 FROM tags WHERE id = ?", params![&uuid_bytes], |row| row.get(0))
            .optional()?;

        if existing.is_some() {
            // Update existing tag
            self.conn.execute(
                "UPDATE tags SET name = ?, parent_id = ?, modified_at = ?, deleted_at = ?, sync_received_at = ? WHERE id = ?",
                params![name, parent_bytes, modified_at, deleted_at, sync_received_at, uuid_bytes],
            )?;
        } else {
            // Insert new tag
            self.conn.execute(
                "INSERT INTO tags (id, name, parent_id, created_at, modified_at, deleted_at, sync_received_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                params![uuid_bytes, name, parent_bytes, created_at, modified_at, deleted_at, sync_received_at],
            )?;
        }
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

        // Check if association exists
        let existing: Option<i64> = self.conn
            .query_row(
                "SELECT 1 FROM note_tags WHERE note_id = ? AND tag_id = ?",
                params![&note_bytes, &tag_bytes],
                |row| row.get(0),
            )
            .optional()?;

        if existing.is_some() {
            // Update existing association
            self.conn.execute(
                "UPDATE note_tags SET modified_at = ?, deleted_at = ?, sync_received_at = ? WHERE note_id = ? AND tag_id = ?",
                params![modified_at, deleted_at, sync_received_at, note_bytes, tag_bytes],
            )?;
        } else {
            // Insert new association
            self.conn.execute(
                "INSERT INTO note_tags (note_id, tag_id, created_at, modified_at, deleted_at, sync_received_at) VALUES (?, ?, ?, ?, ?, ?)",
                params![note_bytes, tag_bytes, created_at, modified_at, deleted_at, sync_received_at],
            )?;
        }

        // Always rebuild note pane cache (tags list changed)
        let _ = self.rebuild_note_cache(note_id);

        // Also rebuild list cache if this is the _marked tag (marked status changed)
        let marked_tag_id = self.get_marked_tag_id()?;
        if tag_bytes == marked_tag_id {
            let _ = self.rebuild_note_list_cache(note_id);
        }

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

    /// Create a conflict record for note content conflict
    pub fn create_note_content_conflict(
        &self,
        note_id: &str,
        local_content: &str,
        local_modified_at: i64,
        local_device_id: Option<&str>,
        local_device_name: Option<&str>,
        remote_content: &str,
        remote_modified_at: i64,
        remote_device_id: Option<&str>,
        remote_device_name: Option<&str>,
    ) -> VoiceResult<String> {
        let conflict_id = Uuid::now_v7();
        let conflict_bytes = conflict_id.as_bytes().to_vec();

        let note_uuid = Uuid::parse_str(note_id)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?;
        let note_bytes = note_uuid.as_bytes().to_vec();

        let local_device_bytes = local_device_id.and_then(|id| {
            Uuid::parse_str(id).ok().map(|u| u.as_bytes().to_vec())
        });
        let remote_device_bytes = remote_device_id.and_then(|id| {
            Uuid::parse_str(id).ok().map(|u| u.as_bytes().to_vec())
        });

        self.conn.execute(
            r#"
            INSERT INTO conflicts_note_content
            (id, note_id, local_content, local_modified_at, local_device_id, local_device_name,
             remote_content, remote_modified_at, remote_device_id, remote_device_name, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%s', 'now'))
            "#,
            params![
                conflict_bytes,
                note_bytes,
                local_content,
                local_modified_at,
                local_device_bytes,
                local_device_name,
                remote_content,
                remote_modified_at,
                remote_device_bytes,
                remote_device_name,
            ],
        )?;

        Ok(conflict_id.simple().to_string())
    }

    /// Create a conflict record for note delete conflict
    pub fn create_note_delete_conflict(
        &self,
        note_id: &str,
        surviving_content: &str,
        surviving_modified_at: i64,
        surviving_device_id: Option<&str>,
        surviving_device_name: Option<&str>,
        deleted_content: Option<&str>,
        deleted_at: i64,
        deleting_device_id: Option<&str>,
        deleting_device_name: Option<&str>,
    ) -> VoiceResult<String> {
        let conflict_id = Uuid::now_v7();
        let conflict_bytes = conflict_id.as_bytes().to_vec();

        let note_uuid = Uuid::parse_str(note_id)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?;
        let note_bytes = note_uuid.as_bytes().to_vec();

        let surviving_device_bytes = surviving_device_id.and_then(|id| {
            Uuid::parse_str(id).ok().map(|u| u.as_bytes().to_vec())
        });
        let deleting_device_bytes = deleting_device_id.and_then(|id| {
            Uuid::parse_str(id).ok().map(|u| u.as_bytes().to_vec())
        });

        self.conn.execute(
            r#"
            INSERT INTO conflicts_note_delete
            (id, note_id, surviving_content, surviving_modified_at, surviving_device_id,
             surviving_device_name, deleted_content, deleted_at, deleting_device_id,
             deleting_device_name, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%s', 'now'))
            "#,
            params![
                conflict_bytes,
                note_bytes,
                surviving_content,
                surviving_modified_at,
                surviving_device_bytes,
                surviving_device_name,
                deleted_content,
                deleted_at,
                deleting_device_bytes,
                deleting_device_name,
            ],
        )?;

        Ok(conflict_id.simple().to_string())
    }

    /// Create a conflict record for tag rename conflict
    pub fn create_tag_rename_conflict(
        &self,
        tag_id: &str,
        local_name: &str,
        local_modified_at: i64,
        local_device_id: Option<&str>,
        local_device_name: Option<&str>,
        remote_name: &str,
        remote_modified_at: i64,
        remote_device_id: Option<&str>,
        remote_device_name: Option<&str>,
    ) -> VoiceResult<String> {
        let conflict_id = Uuid::now_v7();
        let conflict_bytes = conflict_id.as_bytes().to_vec();

        let tag_uuid = Uuid::parse_str(tag_id)
            .map_err(|e| VoiceError::validation("tag_id", e.to_string()))?;
        let tag_bytes = tag_uuid.as_bytes().to_vec();

        let local_device_bytes = local_device_id.and_then(|id| {
            Uuid::parse_str(id).ok().map(|u| u.as_bytes().to_vec())
        });
        let remote_device_bytes = remote_device_id.and_then(|id| {
            Uuid::parse_str(id).ok().map(|u| u.as_bytes().to_vec())
        });

        self.conn.execute(
            r#"
            INSERT INTO conflicts_tag_rename
            (id, tag_id, local_name, local_modified_at, local_device_id, local_device_name,
             remote_name, remote_modified_at, remote_device_id, remote_device_name, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%s', 'now'))
            "#,
            params![
                conflict_bytes,
                tag_bytes,
                local_name,
                local_modified_at,
                local_device_bytes,
                local_device_name,
                remote_name,
                remote_modified_at,
                remote_device_bytes,
                remote_device_name,
            ],
        )?;

        Ok(conflict_id.simple().to_string())
    }

    /// Create a conflict record for note_tag conflict
    pub fn create_note_tag_conflict(
        &self,
        note_id: &str,
        tag_id: &str,
        local_created_at: Option<i64>,
        local_modified_at: Option<i64>,
        local_deleted_at: Option<i64>,
        local_device_id: Option<&str>,
        local_device_name: Option<&str>,
        remote_created_at: Option<i64>,
        remote_modified_at: Option<i64>,
        remote_deleted_at: Option<i64>,
        remote_device_id: Option<&str>,
        remote_device_name: Option<&str>,
    ) -> VoiceResult<String> {
        let conflict_id = Uuid::now_v7();
        let conflict_bytes = conflict_id.as_bytes().to_vec();

        let note_uuid = Uuid::parse_str(note_id)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?;
        let tag_uuid = Uuid::parse_str(tag_id)
            .map_err(|e| VoiceError::validation("tag_id", e.to_string()))?;
        let note_bytes = note_uuid.as_bytes().to_vec();
        let tag_bytes = tag_uuid.as_bytes().to_vec();

        let local_device_bytes = local_device_id.and_then(|id| {
            Uuid::parse_str(id).ok().map(|u| u.as_bytes().to_vec())
        });
        let remote_device_bytes = remote_device_id.and_then(|id| {
            Uuid::parse_str(id).ok().map(|u| u.as_bytes().to_vec())
        });

        self.conn.execute(
            r#"
            INSERT INTO conflicts_note_tag
            (id, note_id, tag_id,
             local_created_at, local_modified_at, local_deleted_at, local_device_id, local_device_name,
             remote_created_at, remote_modified_at, remote_deleted_at, remote_device_id, remote_device_name,
             created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%s', 'now'))
            "#,
            params![
                conflict_bytes,
                note_bytes,
                tag_bytes,
                local_created_at,
                local_modified_at,
                local_deleted_at,
                local_device_bytes,
                local_device_name,
                remote_created_at,
                remote_modified_at,
                remote_deleted_at,
                remote_device_bytes,
                remote_device_name,
            ],
        )?;

        Ok(conflict_id.simple().to_string())
    }

    /// Create a conflict record for tag parent_id conflict
    pub fn create_tag_parent_conflict(
        &self,
        tag_id: &str,
        local_parent_id: Option<&str>,
        local_modified_at: i64,
        local_device_id: Option<&str>,
        local_device_name: Option<&str>,
        remote_parent_id: Option<&str>,
        remote_modified_at: i64,
        remote_device_id: Option<&str>,
        remote_device_name: Option<&str>,
    ) -> VoiceResult<String> {
        let conflict_id = Uuid::now_v7();
        let conflict_bytes = conflict_id.as_bytes().to_vec();

        let tag_uuid = Uuid::parse_str(tag_id)
            .map_err(|e| VoiceError::validation("tag_id", e.to_string()))?;
        let tag_bytes = tag_uuid.as_bytes().to_vec();

        let local_parent_bytes = local_parent_id.and_then(|id| {
            Uuid::parse_str(id).ok().map(|u| u.as_bytes().to_vec())
        });
        let local_device_bytes = local_device_id.and_then(|id| {
            Uuid::parse_str(id).ok().map(|u| u.as_bytes().to_vec())
        });

        let remote_parent_bytes = remote_parent_id.and_then(|id| {
            Uuid::parse_str(id).ok().map(|u| u.as_bytes().to_vec())
        });
        let remote_device_bytes = remote_device_id.and_then(|id| {
            Uuid::parse_str(id).ok().map(|u| u.as_bytes().to_vec())
        });

        self.conn.execute(
            r#"
            INSERT INTO conflicts_tag_parent
            (id, tag_id, local_parent_id, local_modified_at, local_device_id, local_device_name,
             remote_parent_id, remote_modified_at, remote_device_id, remote_device_name, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%s', 'now'))
            "#,
            params![
                conflict_bytes,
                tag_bytes,
                local_parent_bytes,
                local_modified_at,
                local_device_bytes,
                local_device_name,
                remote_parent_bytes,
                remote_modified_at,
                remote_device_bytes,
                remote_device_name,
            ],
        )?;

        Ok(conflict_id.simple().to_string())
    }

    /// Create a conflict record for tag delete conflict (rename vs delete)
    pub fn create_tag_delete_conflict(
        &self,
        tag_id: &str,
        surviving_name: &str,
        surviving_parent_id: Option<&str>,
        surviving_modified_at: i64,
        surviving_device_id: Option<&str>,
        surviving_device_name: Option<&str>,
        deleted_at: i64,
        deleting_device_id: Option<&str>,
        deleting_device_name: Option<&str>,
    ) -> VoiceResult<String> {
        let conflict_id = Uuid::now_v7();
        let conflict_bytes = conflict_id.as_bytes().to_vec();

        let tag_uuid = Uuid::parse_str(tag_id)
            .map_err(|e| VoiceError::validation("tag_id", e.to_string()))?;
        let tag_bytes = tag_uuid.as_bytes().to_vec();

        let surviving_parent_bytes = surviving_parent_id.and_then(|id| {
            Uuid::parse_str(id).ok().map(|u| u.as_bytes().to_vec())
        });

        let surviving_device_bytes = surviving_device_id.and_then(|id| {
            Uuid::parse_str(id).ok().map(|u| u.as_bytes().to_vec())
        });

        let deleting_device_bytes = deleting_device_id.and_then(|id| {
            Uuid::parse_str(id).ok().map(|u| u.as_bytes().to_vec())
        });

        self.conn.execute(
            r#"
            INSERT INTO conflicts_tag_delete
            (id, tag_id, surviving_name, surviving_parent_id, surviving_modified_at,
             surviving_device_id, surviving_device_name,
             deleted_at, deleting_device_id, deleting_device_name, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%s', 'now'))
            "#,
            params![
                conflict_bytes,
                tag_bytes,
                surviving_name,
                surviving_parent_bytes,
                surviving_modified_at,
                surviving_device_bytes,
                surviving_device_name,
                deleted_at,
                deleting_device_bytes,
                deleting_device_name,
            ],
        )?;

        Ok(conflict_id.simple().to_string())
    }

    // ============================================================================
    // Conflict query and resolution methods
    // ============================================================================

    /// Get counts of unresolved conflicts by type
    pub fn get_unresolved_conflict_counts(&self) -> VoiceResult<HashMap<String, i64>> {
        let mut counts = HashMap::new();

        let note_content: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM conflicts_note_content WHERE resolved_at IS NULL",
            [],
            |row| row.get(0),
        )?;
        let note_delete: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM conflicts_note_delete WHERE resolved_at IS NULL",
            [],
            |row| row.get(0),
        )?;
        let tag_rename: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM conflicts_tag_rename WHERE resolved_at IS NULL",
            [],
            |row| row.get(0),
        )?;

        counts.insert("note_content".to_string(), note_content);
        counts.insert("note_delete".to_string(), note_delete);
        counts.insert("tag_rename".to_string(), tag_rename);
        counts.insert("total".to_string(), note_content + note_delete + tag_rename);

        Ok(counts)
    }

    /// Get the types of unresolved conflicts for a specific note.
    ///
    /// Returns a list of conflict type strings (e.g., ["content", "delete"]).
    /// Returns an empty list if the note has no unresolved conflicts.
    pub fn get_note_conflict_types(&self, note_id: &str) -> VoiceResult<Vec<String>> {
        let resolved_id = self.resolve_note_id(note_id)?;
        let note_uuid = Uuid::parse_str(&resolved_id)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?;
        let note_bytes = note_uuid.as_bytes().to_vec();

        let mut types = Vec::new();

        // Check for note content conflicts
        let content_count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM conflicts_note_content WHERE note_id = ? AND resolved_at IS NULL",
            params![note_bytes],
            |row| row.get(0),
        )?;
        if content_count > 0 {
            types.push("content".to_string());
        }

        // Check for note delete conflicts
        let delete_count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM conflicts_note_delete WHERE note_id = ? AND resolved_at IS NULL",
            params![note_bytes],
            |row| row.get(0),
        )?;
        if delete_count > 0 {
            types.push("delete".to_string());
        }

        Ok(types)
    }

    /// Get note content conflicts
    pub fn get_note_content_conflicts(&self, include_resolved: bool) -> VoiceResult<Vec<HashMap<String, serde_json::Value>>> {
        let query = if include_resolved {
            r#"SELECT id, note_id, local_content, local_modified_at, local_device_id,
                      local_device_name, remote_content, remote_modified_at,
                      remote_device_id, remote_device_name, created_at, resolved_at
               FROM conflicts_note_content ORDER BY created_at DESC"#
        } else {
            r#"SELECT id, note_id, local_content, local_modified_at, local_device_id,
                      local_device_name, remote_content, remote_modified_at,
                      remote_device_id, remote_device_name, created_at, resolved_at
               FROM conflicts_note_content WHERE resolved_at IS NULL ORDER BY created_at DESC"#
        };

        let mut stmt = self.conn.prepare(query)?;
        let rows = stmt.query_map([], |row| {
            let id: Vec<u8> = row.get(0)?;
            let note_id: Vec<u8> = row.get(1)?;
            let local_content: String = row.get(2)?;
            let local_modified_at: i64 = row.get(3)?;
            let local_device_id: Option<Vec<u8>> = row.get(4)?;
            let local_device_name: Option<String> = row.get(5)?;
            let remote_content: String = row.get(6)?;
            let remote_modified_at: i64 = row.get(7)?;
            let remote_device_id: Option<Vec<u8>> = row.get(8)?;
            let remote_device_name: Option<String> = row.get(9)?;
            let created_at: i64 = row.get(10)?;
            let resolved_at: Option<i64> = row.get(11)?;

            Ok((id, note_id, local_content, local_modified_at, local_device_id,
                local_device_name, remote_content, remote_modified_at,
                remote_device_id, remote_device_name, created_at, resolved_at))
        })?;

        let mut conflicts = Vec::new();
        for row in rows {
            let (id, note_id, local_content, local_modified_at, local_device_id,
                 local_device_name, remote_content, remote_modified_at,
                 remote_device_id, remote_device_name, created_at, resolved_at) = row?;

            let mut conflict = HashMap::new();
            conflict.insert("id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&id).unwrap_or_default()));
            conflict.insert("note_id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&note_id).unwrap_or_default()));
            conflict.insert("local_content".to_string(), serde_json::Value::String(local_content));
            conflict.insert("local_modified_at".to_string(), serde_json::json!(local_modified_at));
            conflict.insert("local_device_id".to_string(), local_device_id.and_then(|b| uuid_bytes_to_hex(&b)).map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("local_device_name".to_string(), local_device_name.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("remote_content".to_string(), serde_json::Value::String(remote_content));
            conflict.insert("remote_modified_at".to_string(), serde_json::json!(remote_modified_at));
            conflict.insert("remote_device_id".to_string(), remote_device_id.and_then(|b| uuid_bytes_to_hex(&b)).map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("remote_device_name".to_string(), remote_device_name.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("created_at".to_string(), serde_json::json!(created_at));
            conflict.insert("resolved_at".to_string(), resolved_at.map_or(serde_json::Value::Null, |v| serde_json::json!(v)));
            conflicts.push(conflict);
        }

        Ok(conflicts)
    }

    /// Get note delete conflicts
    pub fn get_note_delete_conflicts(&self, include_resolved: bool) -> VoiceResult<Vec<HashMap<String, serde_json::Value>>> {
        let query = if include_resolved {
            r#"SELECT id, note_id, surviving_content, surviving_modified_at,
                      surviving_device_id, surviving_device_name, deleted_content, deleted_at,
                      deleting_device_id, deleting_device_name, created_at, resolved_at
               FROM conflicts_note_delete ORDER BY created_at DESC"#
        } else {
            r#"SELECT id, note_id, surviving_content, surviving_modified_at,
                      surviving_device_id, surviving_device_name, deleted_content, deleted_at,
                      deleting_device_id, deleting_device_name, created_at, resolved_at
               FROM conflicts_note_delete WHERE resolved_at IS NULL ORDER BY created_at DESC"#
        };

        let mut stmt = self.conn.prepare(query)?;
        let rows = stmt.query_map([], |row| {
            let id: Vec<u8> = row.get(0)?;
            let note_id: Vec<u8> = row.get(1)?;
            let surviving_content: String = row.get(2)?;
            let surviving_modified_at: i64 = row.get(3)?;
            let surviving_device_id: Option<Vec<u8>> = row.get(4)?;
            let surviving_device_name: Option<String> = row.get(5)?;
            let deleted_content: Option<String> = row.get(6)?;
            let deleted_at: i64 = row.get(7)?;
            let deleting_device_id: Option<Vec<u8>> = row.get(8)?;
            let deleting_device_name: Option<String> = row.get(9)?;
            let created_at: i64 = row.get(10)?;
            let resolved_at: Option<i64> = row.get(11)?;

            Ok((id, note_id, surviving_content, surviving_modified_at,
                surviving_device_id, surviving_device_name, deleted_content, deleted_at,
                deleting_device_id, deleting_device_name, created_at, resolved_at))
        })?;

        let mut conflicts = Vec::new();
        for row in rows {
            let (id, note_id, surviving_content, surviving_modified_at,
                 surviving_device_id, surviving_device_name, deleted_content, deleted_at,
                 deleting_device_id, deleting_device_name, created_at, resolved_at) = row?;

            let mut conflict = HashMap::new();
            conflict.insert("id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&id).unwrap_or_default()));
            conflict.insert("note_id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&note_id).unwrap_or_default()));
            conflict.insert("surviving_content".to_string(), serde_json::Value::String(surviving_content));
            conflict.insert("surviving_modified_at".to_string(), serde_json::json!(surviving_modified_at));
            conflict.insert("surviving_device_id".to_string(), surviving_device_id.and_then(|b| uuid_bytes_to_hex(&b)).map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("surviving_device_name".to_string(), surviving_device_name.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("deleted_content".to_string(), deleted_content.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("deleted_at".to_string(), serde_json::json!(deleted_at));
            conflict.insert("deleting_device_id".to_string(), deleting_device_id.and_then(|b| uuid_bytes_to_hex(&b)).map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("deleting_device_name".to_string(), deleting_device_name.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("created_at".to_string(), serde_json::json!(created_at));
            conflict.insert("resolved_at".to_string(), resolved_at.map_or(serde_json::Value::Null, |v| serde_json::json!(v)));
            conflicts.push(conflict);
        }

        Ok(conflicts)
    }

    /// Get tag rename conflicts
    pub fn get_tag_rename_conflicts(&self, include_resolved: bool) -> VoiceResult<Vec<HashMap<String, serde_json::Value>>> {
        let query = if include_resolved {
            r#"SELECT id, tag_id, local_name, local_modified_at, local_device_id,
                      local_device_name, remote_name, remote_modified_at,
                      remote_device_id, remote_device_name, created_at, resolved_at
               FROM conflicts_tag_rename ORDER BY created_at DESC"#
        } else {
            r#"SELECT id, tag_id, local_name, local_modified_at, local_device_id,
                      local_device_name, remote_name, remote_modified_at,
                      remote_device_id, remote_device_name, created_at, resolved_at
               FROM conflicts_tag_rename WHERE resolved_at IS NULL ORDER BY created_at DESC"#
        };

        let mut stmt = self.conn.prepare(query)?;
        let rows = stmt.query_map([], |row| {
            let id: Vec<u8> = row.get(0)?;
            let tag_id: Vec<u8> = row.get(1)?;
            let local_name: String = row.get(2)?;
            let local_modified_at: i64 = row.get(3)?;
            let local_device_id: Option<Vec<u8>> = row.get(4)?;
            let local_device_name: Option<String> = row.get(5)?;
            let remote_name: String = row.get(6)?;
            let remote_modified_at: i64 = row.get(7)?;
            let remote_device_id: Option<Vec<u8>> = row.get(8)?;
            let remote_device_name: Option<String> = row.get(9)?;
            let created_at: i64 = row.get(10)?;
            let resolved_at: Option<i64> = row.get(11)?;

            Ok((id, tag_id, local_name, local_modified_at, local_device_id,
                local_device_name, remote_name, remote_modified_at,
                remote_device_id, remote_device_name, created_at, resolved_at))
        })?;

        let mut conflicts = Vec::new();
        for row in rows {
            let (id, tag_id, local_name, local_modified_at, local_device_id,
                 local_device_name, remote_name, remote_modified_at,
                 remote_device_id, remote_device_name, created_at, resolved_at) = row?;

            let mut conflict = HashMap::new();
            conflict.insert("id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&id).unwrap_or_default()));
            conflict.insert("tag_id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&tag_id).unwrap_or_default()));
            conflict.insert("local_name".to_string(), serde_json::Value::String(local_name));
            conflict.insert("local_modified_at".to_string(), serde_json::json!(local_modified_at));
            conflict.insert("local_device_id".to_string(), local_device_id.and_then(|b| uuid_bytes_to_hex(&b)).map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("local_device_name".to_string(), local_device_name.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("remote_name".to_string(), serde_json::Value::String(remote_name));
            conflict.insert("remote_modified_at".to_string(), serde_json::json!(remote_modified_at));
            conflict.insert("remote_device_id".to_string(), remote_device_id.and_then(|b| uuid_bytes_to_hex(&b)).map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("remote_device_name".to_string(), remote_device_name.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("created_at".to_string(), serde_json::json!(created_at));
            conflict.insert("resolved_at".to_string(), resolved_at.map_or(serde_json::Value::Null, |v| serde_json::json!(v)));
            conflicts.push(conflict);
        }

        Ok(conflicts)
    }

    /// Get tag parent conflicts
    pub fn get_tag_parent_conflicts(&self, include_resolved: bool) -> VoiceResult<Vec<HashMap<String, serde_json::Value>>> {
        let query = if include_resolved {
            r#"SELECT id, tag_id, local_parent_id, local_modified_at, local_device_id,
                      local_device_name, remote_parent_id, remote_modified_at,
                      remote_device_id, remote_device_name, created_at, resolved_at
               FROM conflicts_tag_parent ORDER BY created_at DESC"#
        } else {
            r#"SELECT id, tag_id, local_parent_id, local_modified_at, local_device_id,
                      local_device_name, remote_parent_id, remote_modified_at,
                      remote_device_id, remote_device_name, created_at, resolved_at
               FROM conflicts_tag_parent WHERE resolved_at IS NULL ORDER BY created_at DESC"#
        };

        let mut stmt = self.conn.prepare(query)?;
        let rows = stmt.query_map([], |row| {
            let id: Vec<u8> = row.get(0)?;
            let tag_id: Vec<u8> = row.get(1)?;
            let local_parent_id: Option<Vec<u8>> = row.get(2)?;
            let local_modified_at: i64 = row.get(3)?;
            let local_device_id: Option<Vec<u8>> = row.get(4)?;
            let local_device_name: Option<String> = row.get(5)?;
            let remote_parent_id: Option<Vec<u8>> = row.get(6)?;
            let remote_modified_at: i64 = row.get(7)?;
            let remote_device_id: Option<Vec<u8>> = row.get(8)?;
            let remote_device_name: Option<String> = row.get(9)?;
            let created_at: i64 = row.get(10)?;
            let resolved_at: Option<i64> = row.get(11)?;

            Ok((id, tag_id, local_parent_id, local_modified_at, local_device_id,
                local_device_name, remote_parent_id, remote_modified_at,
                remote_device_id, remote_device_name, created_at, resolved_at))
        })?;

        let mut conflicts = Vec::new();
        for row in rows {
            let (id, tag_id, local_parent_id, local_modified_at, local_device_id,
                 local_device_name, remote_parent_id, remote_modified_at,
                 remote_device_id, remote_device_name, created_at, resolved_at) = row?;

            let mut conflict = HashMap::new();
            conflict.insert("id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&id).unwrap_or_default()));
            conflict.insert("tag_id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&tag_id).unwrap_or_default()));
            conflict.insert("local_parent_id".to_string(), local_parent_id.and_then(|b| uuid_bytes_to_hex(&b)).map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("local_modified_at".to_string(), serde_json::json!(local_modified_at));
            conflict.insert("local_device_id".to_string(), local_device_id.and_then(|b| uuid_bytes_to_hex(&b)).map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("local_device_name".to_string(), local_device_name.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("remote_parent_id".to_string(), remote_parent_id.and_then(|b| uuid_bytes_to_hex(&b)).map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("remote_modified_at".to_string(), serde_json::json!(remote_modified_at));
            conflict.insert("remote_device_id".to_string(), remote_device_id.and_then(|b| uuid_bytes_to_hex(&b)).map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("remote_device_name".to_string(), remote_device_name.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("created_at".to_string(), serde_json::json!(created_at));
            conflict.insert("resolved_at".to_string(), resolved_at.map_or(serde_json::Value::Null, |v| serde_json::json!(v)));
            conflicts.push(conflict);
        }

        Ok(conflicts)
    }

    /// Get tag delete conflicts (rename vs delete)
    pub fn get_tag_delete_conflicts(&self, include_resolved: bool) -> VoiceResult<Vec<HashMap<String, serde_json::Value>>> {
        let query = if include_resolved {
            r#"SELECT id, tag_id, surviving_name, surviving_parent_id, surviving_modified_at,
                      surviving_device_id, surviving_device_name,
                      deleted_at, deleting_device_id, deleting_device_name,
                      created_at, resolved_at
               FROM conflicts_tag_delete ORDER BY created_at DESC"#
        } else {
            r#"SELECT id, tag_id, surviving_name, surviving_parent_id, surviving_modified_at,
                      surviving_device_id, surviving_device_name,
                      deleted_at, deleting_device_id, deleting_device_name,
                      created_at, resolved_at
               FROM conflicts_tag_delete WHERE resolved_at IS NULL ORDER BY created_at DESC"#
        };

        let mut stmt = self.conn.prepare(query)?;
        let rows = stmt.query_map([], |row| {
            let id: Vec<u8> = row.get(0)?;
            let tag_id: Vec<u8> = row.get(1)?;
            let surviving_name: String = row.get(2)?;
            let surviving_parent_id: Option<Vec<u8>> = row.get(3)?;
            let surviving_modified_at: i64 = row.get(4)?;
            let surviving_device_id: Option<Vec<u8>> = row.get(5)?;
            let surviving_device_name: Option<String> = row.get(6)?;
            let deleted_at: i64 = row.get(7)?;
            let deleting_device_id: Option<Vec<u8>> = row.get(8)?;
            let deleting_device_name: Option<String> = row.get(9)?;
            let created_at: i64 = row.get(10)?;
            let resolved_at: Option<i64> = row.get(11)?;

            Ok((id, tag_id, surviving_name, surviving_parent_id, surviving_modified_at,
                surviving_device_id, surviving_device_name,
                deleted_at, deleting_device_id, deleting_device_name, created_at, resolved_at))
        })?;

        let mut conflicts = Vec::new();
        for row in rows {
            let (id, tag_id, surviving_name, surviving_parent_id, surviving_modified_at,
                 surviving_device_id, surviving_device_name,
                 deleted_at, deleting_device_id, deleting_device_name, created_at, resolved_at) = row?;

            let mut conflict = HashMap::new();
            conflict.insert("id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&id).unwrap_or_default()));
            conflict.insert("tag_id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&tag_id).unwrap_or_default()));
            conflict.insert("surviving_name".to_string(), serde_json::Value::String(surviving_name));
            conflict.insert("surviving_parent_id".to_string(), surviving_parent_id.and_then(|b| uuid_bytes_to_hex(&b)).map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("surviving_modified_at".to_string(), serde_json::json!(surviving_modified_at));
            conflict.insert("surviving_device_id".to_string(), surviving_device_id.and_then(|b| uuid_bytes_to_hex(&b)).map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("surviving_device_name".to_string(), surviving_device_name.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("deleted_at".to_string(), serde_json::json!(deleted_at));
            conflict.insert("deleting_device_id".to_string(), deleting_device_id.and_then(|b| uuid_bytes_to_hex(&b)).map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("deleting_device_name".to_string(), deleting_device_name.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("created_at".to_string(), serde_json::json!(created_at));
            conflict.insert("resolved_at".to_string(), resolved_at.map_or(serde_json::Value::Null, |v| serde_json::json!(v)));
            conflicts.push(conflict);
        }

        Ok(conflicts)
    }

    /// Resolve a note content conflict
    pub fn resolve_note_content_conflict(&self, conflict_id: &str, new_content: &str) -> VoiceResult<bool> {
        let conflict_uuid = Uuid::parse_str(conflict_id)
            .map_err(|e| VoiceError::validation("conflict_id", e.to_string()))?;
        let conflict_bytes = conflict_uuid.as_bytes().to_vec();

        // Get the note_id for this conflict
        let note_id: Option<Vec<u8>> = self.conn.query_row(
            "SELECT note_id FROM conflicts_note_content WHERE id = ?",
            params![conflict_bytes],
            |row| row.get(0),
        ).optional()?;

        let note_id = match note_id {
            Some(id) => id,
            None => return Ok(false),
        };

        // Update the note content
        self.conn.execute(
            "UPDATE notes SET content = ?, modified_at = strftime('%s', 'now') WHERE id = ?",
            params![new_content, &note_id],
        )?;

        // Mark conflict as resolved
        self.conn.execute(
            "UPDATE conflicts_note_content SET resolved_at = strftime('%s', 'now') WHERE id = ?",
            params![conflict_bytes],
        )?;

        // Rebuild display cache
        if let Some(note_id_hex) = uuid_bytes_to_hex(&note_id) {
            let _ = self.rebuild_note_cache(&note_id_hex);
        }

        Ok(true)
    }

    /// Resolve a note delete conflict
    pub fn resolve_note_delete_conflict(&self, conflict_id: &str, restore_note: bool) -> VoiceResult<bool> {
        let conflict_uuid = Uuid::parse_str(conflict_id)
            .map_err(|e| VoiceError::validation("conflict_id", e.to_string()))?;
        let conflict_bytes = conflict_uuid.as_bytes().to_vec();

        // Get the note_id and surviving content for this conflict
        let row: Option<(Vec<u8>, String)> = self.conn.query_row(
            "SELECT note_id, surviving_content FROM conflicts_note_delete WHERE id = ?",
            params![conflict_bytes],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).optional()?;

        let (note_id, surviving_content) = match row {
            Some(r) => r,
            None => return Ok(false),
        };

        if restore_note {
            // Restore the note with surviving content
            self.conn.execute(
                "UPDATE notes SET content = ?, deleted_at = NULL, modified_at = strftime('%s', 'now') WHERE id = ?",
                params![surviving_content, &note_id],
            )?;
        }
        // If not restoring, the note stays deleted (no action needed)

        // Mark conflict as resolved
        self.conn.execute(
            "UPDATE conflicts_note_delete SET resolved_at = strftime('%s', 'now') WHERE id = ?",
            params![conflict_bytes],
        )?;

        // Rebuild display cache if note was restored
        if restore_note {
            if let Some(note_id_hex) = uuid_bytes_to_hex(&note_id) {
                let _ = self.rebuild_note_cache(&note_id_hex);
            }
        }

        Ok(true)
    }

    /// Resolve a tag rename conflict
    pub fn resolve_tag_rename_conflict(&self, conflict_id: &str, new_name: &str) -> VoiceResult<bool> {
        let conflict_uuid = Uuid::parse_str(conflict_id)
            .map_err(|e| VoiceError::validation("conflict_id", e.to_string()))?;
        let conflict_bytes = conflict_uuid.as_bytes().to_vec();

        // Get the tag_id for this conflict
        let tag_id: Option<Vec<u8>> = self.conn.query_row(
            "SELECT tag_id FROM conflicts_tag_rename WHERE id = ?",
            params![conflict_bytes],
            |row| row.get(0),
        ).optional()?;

        let tag_id = match tag_id {
            Some(id) => id,
            None => return Ok(false),
        };

        // Update the tag name
        self.conn.execute(
            "UPDATE tags SET name = ?, modified_at = strftime('%s', 'now') WHERE id = ?",
            params![new_name, tag_id],
        )?;

        // Mark conflict as resolved
        self.conn.execute(
            "UPDATE conflicts_tag_rename SET resolved_at = strftime('%s', 'now') WHERE id = ?",
            params![conflict_bytes],
        )?;

        Ok(true)
    }

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

        // Rebuild display cache for the note
        let _ = self.rebuild_note_cache(note_id);

        Ok(association_id.simple().to_string())
    }

    /// Detach an attachment from a note (soft delete)
    pub fn detach_from_note(&self, association_id: &str) -> VoiceResult<bool> {
        let uuid = Uuid::parse_str(association_id)
            .map_err(|e| VoiceError::validation("association_id", e.to_string()))?;
        let uuid_bytes = uuid.as_bytes().to_vec();
        let device_id = get_local_device_id();

        // Get the note_id before updating so we can update the note's modified_at
        let note_bytes: Option<Vec<u8>> = self
            .conn
            .query_row(
                "SELECT note_id FROM note_attachments WHERE id = ? AND deleted_at IS NULL",
                params![&uuid_bytes],
                |row| row.get(0),
            )
            .optional()?;

        let updated = self.conn.execute(
            r#"
            UPDATE note_attachments
            SET deleted_at = strftime('%s', 'now'), modified_at = strftime('%s', 'now'), device_id = ?
            WHERE id = ? AND deleted_at IS NULL
            "#,
            params![device_id.as_bytes().to_vec(), uuid_bytes],
        )?;

        // Update the parent Note's modified_at to trigger sync and rebuild cache
        if updated > 0 {
            if let Some(note_id) = note_bytes {
                self.conn.execute(
                    "UPDATE notes SET modified_at = strftime('%s', 'now') WHERE id = ?",
                    params![&note_id],
                )?;
                // Rebuild display cache for the note
                if let Some(note_id_hex) = uuid_bytes_to_hex(&note_id) {
                    let _ = self.rebuild_note_cache(&note_id_hex);
                }
            }
        }

        Ok(updated > 0)
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
        self.create_audio_file_with_duration(filename, file_created_at, None)
    }

    /// Create a new audio file record with optional duration
    pub fn create_audio_file_with_duration(
        &self,
        filename: &str,
        file_created_at: Option<i64>,
        duration_seconds: Option<i64>,
    ) -> VoiceResult<String> {
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

        Ok(audio_file_id.simple().to_string())
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
                   storage_provider, storage_key, storage_uploaded_at
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
                   af.storage_provider, af.storage_key, af.storage_uploaded_at
            FROM audio_files af
            INNER JOIN note_attachments na ON af.id = na.attachment_id
            WHERE na.note_id = ?
              AND na.attachment_type = 'audio_file'
              AND na.deleted_at IS NULL
              AND af.deleted_at IS NULL
            ORDER BY af.imported_at DESC
            "#,
        )?;

        let rows = stmt.query_map([uuid_bytes], |row| self.row_to_audio_file(row))?;
        let mut audio_files = Vec::new();
        for audio_file in rows {
            audio_files.push(audio_file?);
        }
        Ok(audio_files)
    }

    /// Get all audio files from the database (including deleted ones)
    pub fn get_all_audio_files(&self) -> VoiceResult<Vec<AudioFileRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT id, imported_at, filename, file_created_at, duration_seconds,
                   summary, device_id, modified_at, deleted_at,
                   storage_provider, storage_key, storage_uploaded_at
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
        let uuid_bytes = uuid.as_bytes().to_vec();
        let device_id = get_local_device_id();

        let updated = self.conn.execute(
            r#"
            UPDATE audio_files
            SET summary = ?, modified_at = strftime('%s', 'now'), device_id = ?
            WHERE id = ? AND deleted_at IS NULL
            "#,
            params![summary, device_id.as_bytes().to_vec(), uuid_bytes],
        )?;

        // Rebuild display cache for associated note(s)
        if updated > 0 {
            self.rebuild_caches_for_audio_file(audio_file_id);
        }

        Ok(updated > 0)
    }

    /// Soft-delete an audio file (accepts ID or ID prefix)
    pub fn delete_audio_file(&self, audio_file_id: &str) -> VoiceResult<bool> {
        // Use try_resolve to return false if not found (instead of error)
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
            SET deleted_at = strftime('%s', 'now'), modified_at = strftime('%s', 'now'), device_id = ?
            WHERE id = ? AND deleted_at IS NULL
            "#,
            params![device_id.as_bytes().to_vec(), uuid_bytes],
        )?;

        Ok(updated > 0)
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

    /// Get all audio files that are missing duration information
    pub fn get_audio_files_missing_duration(&self) -> VoiceResult<Vec<AudioFileRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT id, imported_at, filename, file_created_at, duration_seconds,
                   summary, device_id, modified_at, deleted_at,
                   storage_provider, storage_key, storage_uploaded_at
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
                   storage_provider, storage_key, storage_uploaded_at
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
                storage_uploaded_at = strftime('%s', 'now'),
                modified_at = strftime('%s', 'now'),
                device_id = ?
            WHERE id = ? AND deleted_at IS NULL
            "#,
            params![storage_provider, storage_key, device_id.as_bytes().to_vec(), uuid_bytes],
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

        self.conn.execute(
            r#"
            INSERT INTO note_attachments (id, note_id, attachment_id, attachment_type, created_at, device_id, modified_at, deleted_at, sync_received_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                note_id = excluded.note_id,
                attachment_id = excluded.attachment_id,
                attachment_type = excluded.attachment_type,
                modified_at = excluded.modified_at,
                deleted_at = excluded.deleted_at,
                device_id = excluded.device_id,
                sync_received_at = excluded.sync_received_at
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
                   storage_provider, storage_key, storage_uploaded_at
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
    ) -> VoiceResult<()> {
        let id_uuid = Uuid::parse_str(id)
            .map_err(|e| VoiceError::validation("id", e.to_string()))?;
        let device_id = get_local_device_id();

        self.conn.execute(
            r#"
            INSERT INTO audio_files (id, imported_at, filename, file_created_at, duration_seconds, summary, device_id, modified_at, deleted_at, sync_received_at, storage_provider, storage_key, storage_uploaded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                filename = excluded.filename,
                file_created_at = excluded.file_created_at,
                duration_seconds = excluded.duration_seconds,
                summary = excluded.summary,
                modified_at = excluded.modified_at,
                deleted_at = excluded.deleted_at,
                device_id = excluded.device_id,
                sync_received_at = excluded.sync_received_at,
                storage_provider = excluded.storage_provider,
                storage_key = excluded.storage_key,
                storage_uploaded_at = excluded.storage_uploaded_at
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
            ],
        )?;

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

        self.conn.execute(
            r#"
            INSERT INTO transcriptions (id, audio_file_id, content, content_segments, service, service_arguments, service_response, state, device_id, created_at, modified_at, deleted_at, sync_received_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                audio_file_id = excluded.audio_file_id,
                content = excluded.content,
                content_segments = excluded.content_segments,
                service = excluded.service,
                service_arguments = excluded.service_arguments,
                service_response = excluded.service_response,
                state = excluded.state,
                device_id = excluded.device_id,
                modified_at = excluded.modified_at,
                deleted_at = excluded.deleted_at,
                sync_received_at = excluded.sync_received_at
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

        Ok(())
    }

    /// Get a transcription as raw JSON (for sync conflict detection)
    pub fn get_transcription_raw(&self, transcription_id: &str) -> VoiceResult<Option<serde_json::Value>> {
        let uuid = Uuid::parse_str(transcription_id)
            .map_err(|e| VoiceError::validation("transcription_id", e.to_string()))?;
        let uuid_bytes = uuid.as_bytes().to_vec();

        let mut stmt = self.conn.prepare(
            r#"
            SELECT id, audio_file_id, content, content_segments, service, service_arguments, service_response, state, device_id, created_at, modified_at, deleted_at
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

        // Rebuild display cache for associated note(s)
        self.rebuild_caches_for_audio_file(audio_file_id);

        Ok(id.simple().to_string())
    }

    /// Get a transcription by ID
    pub fn get_transcription(&self, transcription_id: &str) -> VoiceResult<Option<TranscriptionRow>> {
        let id_uuid = Uuid::parse_str(transcription_id)
            .map_err(|e| VoiceError::validation("transcription_id", e.to_string()))?;

        let result = self.conn.query_row(
            r#"
            SELECT id, audio_file_id, content, content_segments, service, service_arguments, service_response, state, device_id, created_at, modified_at, deleted_at
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
            SELECT id, audio_file_id, content, content_segments, service, service_arguments, service_response, state, device_id, created_at, modified_at, deleted_at
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
            })
        })?;

        let mut transcriptions = Vec::new();
        for row in rows {
            transcriptions.push(row?);
        }

        Ok(transcriptions)
    }

    /// Delete a transcription (soft delete)
    pub fn delete_transcription(&self, transcription_id: &str) -> VoiceResult<bool> {
        let id_uuid = Uuid::parse_str(transcription_id)
            .map_err(|e| VoiceError::validation("transcription_id", e.to_string()))?;
        let now = Utc::now().timestamp();

        // Get the audio_file_id before deleting (for cache rebuild)
        let audio_file_id: Option<String> = self.conn.query_row(
            "SELECT audio_file_id FROM transcriptions WHERE id = ? AND deleted_at IS NULL",
            params![id_uuid.as_bytes().to_vec()],
            |row| {
                let bytes: Vec<u8> = row.get(0)?;
                Ok(uuid_bytes_to_hex(&bytes).unwrap_or_default())
            },
        ).optional()?;

        let count = self.conn.execute(
            r#"
            UPDATE transcriptions
            SET deleted_at = ?, modified_at = ?
            WHERE id = ? AND deleted_at IS NULL
            "#,
            params![now, now, id_uuid.as_bytes().to_vec()],
        )?;

        // Rebuild display cache for associated note(s)
        if count > 0 {
            if let Some(audio_id) = audio_file_id {
                self.rebuild_caches_for_audio_file(&audio_id);
            }
        }

        Ok(count > 0)
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
        let now = Utc::now().timestamp();

        // If state is provided, update it; otherwise keep existing
        let count = if let Some(state) = state {
            self.conn.execute(
                r#"
                UPDATE transcriptions
                SET content = ?, content_segments = ?, service_response = ?, state = ?, modified_at = ?
                WHERE id = ? AND deleted_at IS NULL
                "#,
                params![
                    content,
                    content_segments,
                    service_response,
                    state,
                    now,
                    id_uuid.as_bytes().to_vec()
                ],
            )?
        } else {
            self.conn.execute(
                r#"
                UPDATE transcriptions
                SET content = ?, content_segments = ?, service_response = ?, modified_at = ?
                WHERE id = ? AND deleted_at IS NULL
                "#,
                params![
                    content,
                    content_segments,
                    service_response,
                    now,
                    id_uuid.as_bytes().to_vec()
                ],
            )?
        };

        // Rebuild display cache for associated note(s)
        if count > 0 {
            if let Ok(Some(transcription)) = self.get_transcription(transcription_id) {
                self.rebuild_caches_for_audio_file(&transcription.audio_file_id);
            }
        }

        Ok(count > 0)
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
    /// - content_preview: first 100 characters of content
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
            let (id_bytes, name, parent_id) = row?;
            let tag_id = uuid_bytes_to_hex(&id_bytes).unwrap_or_default();
            let display_name = self.get_tag_display_name(&tag_id, &name, parent_id.as_deref())?;
            tag_display_names.push(display_name);
        }

        // 6. Build JSON cache
        // Format Unix timestamp as local time for display
        let date_display = chrono::DateTime::from_timestamp(created_at, 0)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| "Unknown".to_string());
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
    fn rebuild_caches_for_audio_file(&self, audio_file_id: &str) {
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
            let display_name = self.get_tag_display_name(&tag_id, &name, parent_id.as_deref())?;
            result.push(serde_json::json!({
                "id": tag_id,
                "name": name,
                "display_name": display_name
            }));
        }

        Ok(result)
    }

    /// Build the full hierarchical path for a tag.
    fn build_tag_path(&self, _id_bytes: &[u8], name: &str, parent_id: Option<&[u8]>) -> VoiceResult<String> {
        let mut path_parts = vec![name.to_string()];
        let mut current_parent = parent_id.map(|p| p.to_vec());

        // Walk up the parent chain
        while let Some(parent_bytes) = current_parent {
            let result: Option<(String, Option<Vec<u8>>)> = self.conn.query_row(
                "SELECT name, parent_id FROM tags WHERE id = ? AND deleted_at IS NULL",
                params![parent_bytes],
                |row| Ok((row.get(0)?, row.get(1)?)),
            ).optional()?;

            if let Some((parent_name, grandparent_id)) = result {
                path_parts.push(parent_name);
                current_parent = grandparent_id;
            } else {
                break;
            }
        }

        path_parts.reverse();
        Ok(path_parts.join("/"))
    }

    /// Get the minimal display name for a tag, adding parent prefixes only if ambiguous.
    ///
    /// If the tag name is unique, returns just the name (e.g., "Paris").
    /// If ambiguous, adds parent prefixes recursively until unique (e.g., "France/Paris").
    fn get_tag_display_name(&self, tag_id: &str, tag_name: &str, parent_id: Option<&[u8]>) -> VoiceResult<String> {
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
                        SELECT id, name, parent_id, name as path
                        FROM tags WHERE deleted_at IS NULL
                        UNION ALL
                        SELECT t.id, t.name, t.parent_id, p.name || '/' || tp.path
                        FROM tags t
                        JOIN tag_paths tp ON t.id = tp.parent_id
                        JOIN tags p ON t.id = p.id
                        WHERE t.deleted_at IS NULL
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
            WHERE na.note_id = ? AND na.deleted_at IS NULL
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
    /// Includes first 100 characters of content as `content_preview`.
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

    #[test]
    fn test_create_database() {
        let db = Database::new_in_memory().unwrap();
        assert!(db.get_all_notes().unwrap().is_empty());
        // Database has system tags (_system, _marked) by default
        let tags = db.get_all_tags().unwrap();
        assert_eq!(tags.len(), 2);
        let tag_names: Vec<&str> = tags.iter().map(|t| t.name.as_str()).collect();
        assert!(tag_names.contains(&"_system"));
        assert!(tag_names.contains(&"_marked"));
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
        let note2_id = db.create_note("Goodbye world").unwrap();
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
            .import_audio_file("recording.m4a", file_created_at, duration_seconds)
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
            .import_audio_file("voice_memo.mp3", None, None)
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
            Some(1700000002), // storage_uploaded_at
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
