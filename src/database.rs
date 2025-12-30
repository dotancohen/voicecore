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
use crate::validation::{
    validate_note_id, validate_search_query, validate_tag_id, validate_tag_path,
};

// Global device ID for local operations
static LOCAL_DEVICE_ID: OnceLock<Uuid> = OnceLock::new();

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
    pub created_at: String,
    pub content: String,
    pub modified_at: Option<String>,
    pub deleted_at: Option<String>,
    pub tag_names: Option<String>,
}

/// Tag data returned from database queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagRow {
    pub id: String,
    pub name: String,
    pub parent_id: Option<String>,
    pub created_at: Option<String>,
    pub modified_at: Option<String>,
}

/// NoteAttachment data returned from database queries (junction table)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoteAttachmentRow {
    pub id: String,
    pub note_id: String,
    pub attachment_id: String,
    pub attachment_type: String,
    pub created_at: String,
    pub device_id: String,
    pub modified_at: Option<String>,
    pub deleted_at: Option<String>,
}

/// AudioFile data returned from database queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioFileRow {
    pub id: String,
    pub imported_at: String,
    pub filename: String,
    pub file_created_at: Option<String>,
    pub summary: Option<String>,
    pub device_id: String,
    pub modified_at: Option<String>,
    pub deleted_at: Option<String>,
}

/// Convert UUID bytes to hex string
fn uuid_bytes_to_hex(bytes: &[u8]) -> Option<String> {
    if bytes.len() == 16 {
        Some(Uuid::from_slice(bytes).ok()?.simple().to_string())
    } else {
        None
    }
}

/// Parse SQLite datetime string to ISO format
fn parse_sqlite_datetime(s: &str) -> String {
    // SQLite uses "YYYY-MM-DD HH:MM:SS" format
    // Convert to ISO format with T separator
    s.replace(' ', "T")
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
        Ok(db)
    }

    /// Create an in-memory database (for testing)
    pub fn new_in_memory() -> VoiceResult<Self> {
        let conn = Connection::open_in_memory()?;
        let mut db = Self { conn };
        db.init_database()?;
        Ok(db)
    }

    /// Initialize database schema
    pub fn init_database(&mut self) -> VoiceResult<()> {
        self.conn.execute_batch(
            r#"
            -- Create notes table with UUID7 BLOB primary key
            CREATE TABLE IF NOT EXISTS notes (
                id BLOB PRIMARY KEY,
                created_at DATETIME NOT NULL,
                content TEXT NOT NULL,
                modified_at DATETIME,
                deleted_at DATETIME
            );

            -- Create tags table with UUID7 BLOB primary key
            CREATE TABLE IF NOT EXISTS tags (
                id BLOB PRIMARY KEY,
                name TEXT NOT NULL,
                parent_id BLOB,
                created_at DATETIME NOT NULL,
                modified_at DATETIME,
                FOREIGN KEY (parent_id) REFERENCES tags (id) ON DELETE CASCADE
            );

            -- Create note_tags junction table with timestamps for sync
            CREATE TABLE IF NOT EXISTS note_tags (
                note_id BLOB NOT NULL,
                tag_id BLOB NOT NULL,
                created_at DATETIME NOT NULL,
                modified_at DATETIME,
                deleted_at DATETIME,
                FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE,
                FOREIGN KEY (tag_id) REFERENCES tags (id) ON DELETE CASCADE,
                PRIMARY KEY (note_id, tag_id)
            );

            -- Create sync_peers table
            CREATE TABLE IF NOT EXISTS sync_peers (
                peer_id BLOB PRIMARY KEY,
                peer_name TEXT,
                peer_url TEXT NOT NULL,
                last_sync_at DATETIME,
                last_received_timestamp DATETIME,
                last_sent_timestamp DATETIME,
                certificate_fingerprint BLOB
            );

            -- Create conflicts_note_content table
            CREATE TABLE IF NOT EXISTS conflicts_note_content (
                id BLOB PRIMARY KEY,
                note_id BLOB NOT NULL,
                local_content TEXT NOT NULL,
                local_modified_at DATETIME NOT NULL,
                local_device_id BLOB,
                local_device_name TEXT,
                remote_content TEXT NOT NULL,
                remote_modified_at DATETIME NOT NULL,
                remote_device_id BLOB,
                remote_device_name TEXT,
                created_at DATETIME NOT NULL,
                resolved_at DATETIME,
                FOREIGN KEY (note_id) REFERENCES notes(id)
            );

            -- Create conflicts_note_delete table
            CREATE TABLE IF NOT EXISTS conflicts_note_delete (
                id BLOB PRIMARY KEY,
                note_id BLOB NOT NULL,
                surviving_content TEXT NOT NULL,
                surviving_modified_at DATETIME NOT NULL,
                surviving_device_id BLOB,
                surviving_device_name TEXT,
                deleted_content TEXT,
                deleted_at DATETIME NOT NULL,
                deleting_device_id BLOB,
                deleting_device_name TEXT,
                created_at DATETIME NOT NULL,
                resolved_at DATETIME,
                FOREIGN KEY (note_id) REFERENCES notes(id)
            );

            -- Create conflicts_tag_rename table
            CREATE TABLE IF NOT EXISTS conflicts_tag_rename (
                id BLOB PRIMARY KEY,
                tag_id BLOB NOT NULL,
                local_name TEXT NOT NULL,
                local_modified_at DATETIME NOT NULL,
                local_device_id BLOB,
                local_device_name TEXT,
                remote_name TEXT NOT NULL,
                remote_modified_at DATETIME NOT NULL,
                remote_device_id BLOB,
                remote_device_name TEXT,
                created_at DATETIME NOT NULL,
                resolved_at DATETIME,
                FOREIGN KEY (tag_id) REFERENCES tags(id)
            );

            -- Create conflicts_tag_parent table for parent_id conflicts
            CREATE TABLE IF NOT EXISTS conflicts_tag_parent (
                id BLOB PRIMARY KEY,
                tag_id BLOB NOT NULL,
                local_parent_id BLOB,
                local_modified_at DATETIME NOT NULL,
                local_device_id BLOB,
                local_device_name TEXT,
                remote_parent_id BLOB,
                remote_modified_at DATETIME NOT NULL,
                remote_device_id BLOB,
                remote_device_name TEXT,
                created_at DATETIME NOT NULL,
                resolved_at DATETIME,
                FOREIGN KEY (tag_id) REFERENCES tags(id)
            );

            -- Create conflicts_tag_delete table for rename vs delete conflicts
            CREATE TABLE IF NOT EXISTS conflicts_tag_delete (
                id BLOB PRIMARY KEY,
                tag_id BLOB NOT NULL,
                surviving_name TEXT NOT NULL,
                surviving_parent_id BLOB,
                surviving_modified_at DATETIME NOT NULL,
                surviving_device_id BLOB,
                surviving_device_name TEXT,
                deleted_at DATETIME NOT NULL,
                deleting_device_id BLOB,
                deleting_device_name TEXT,
                created_at DATETIME NOT NULL,
                resolved_at DATETIME,
                FOREIGN KEY (tag_id) REFERENCES tags(id)
            );

            -- Create conflicts_note_tag table
            CREATE TABLE IF NOT EXISTS conflicts_note_tag (
                id BLOB PRIMARY KEY,
                note_id BLOB NOT NULL,
                tag_id BLOB NOT NULL,
                local_created_at DATETIME,
                local_modified_at DATETIME,
                local_deleted_at DATETIME,
                remote_created_at DATETIME,
                remote_modified_at DATETIME,
                remote_deleted_at DATETIME,
                remote_device_id BLOB,
                remote_device_name TEXT,
                created_at DATETIME NOT NULL,
                resolved_at DATETIME,
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
                created_at DATETIME NOT NULL,
                resolved_at DATETIME,
                FOREIGN KEY (peer_id) REFERENCES sync_peers(peer_id)
            );

            -- Create note_attachments junction table (polymorphic association)
            CREATE TABLE IF NOT EXISTS note_attachments (
                id BLOB PRIMARY KEY,
                note_id BLOB NOT NULL,
                attachment_id BLOB NOT NULL,
                attachment_type TEXT NOT NULL,
                created_at DATETIME NOT NULL,
                device_id BLOB NOT NULL,
                modified_at DATETIME,
                deleted_at DATETIME,
                FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE
            );

            -- Create audio_files table
            CREATE TABLE IF NOT EXISTS audio_files (
                id BLOB PRIMARY KEY,
                imported_at DATETIME NOT NULL,
                filename TEXT NOT NULL,
                file_created_at DATETIME,
                summary TEXT,
                device_id BLOB NOT NULL,
                modified_at DATETIME,
                deleted_at DATETIME
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
            CREATE INDEX IF NOT EXISTS idx_note_attachments_note_id ON note_attachments(note_id);
            CREATE INDEX IF NOT EXISTS idx_note_attachments_attachment_id ON note_attachments(attachment_id);
            CREATE INDEX IF NOT EXISTS idx_note_attachments_type ON note_attachments(attachment_type);
            CREATE INDEX IF NOT EXISTS idx_note_attachments_modified_at ON note_attachments(modified_at);
            CREATE INDEX IF NOT EXISTS idx_note_attachments_deleted_at ON note_attachments(deleted_at);
            CREATE INDEX IF NOT EXISTS idx_audio_files_modified_at ON audio_files(modified_at);
            CREATE INDEX IF NOT EXISTS idx_audio_files_deleted_at ON audio_files(deleted_at);
            "#,
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
        self.migrate_normalize_timestamps()?;
        // Future normalizations can be added here
        Ok(())
    }

    /// Normalize all datetime values to SQLite format (YYYY-MM-DD HH:MM:SS).
    ///
    /// This migration fixes timestamps that may have been stored in ISO 8601 format
    /// (with 'T' separator and/or microseconds) from earlier sync operations.
    /// String comparison of timestamps requires consistent format.
    fn migrate_normalize_timestamps(&mut self) -> VoiceResult<()> {
        // Check if migration already ran (using a simple marker in pragmas)
        let version: i64 = self
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))?;

        // Version 1 = timestamps normalized
        if version >= 1 {
            return Ok(());
        }

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

        // Mark migration as complete
        self.conn.execute("PRAGMA user_version = 1", [])?;

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
            "SELECT id FROM {} WHERE lower(hex(id)) LIKE ?1 {} LIMIT 6",
            table, extra_where
        );

        let mut stmt = self.conn.prepare(&sql)?;
        let mut rows = stmt.query([&like_pattern])?;

        // Get first match
        let first_row = rows.next()?;
        let first = match first_row {
            None => return Ok(None), // Not found
            Some(row) => {
                let id_bytes: Vec<u8> = row.get(0)?;
                uuid_bytes_to_hex(&id_bytes).ok_or_else(|| {
                    VoiceError::validation(field_name, "invalid UUID in database")
                })?
            }
        };

        // Check for second match (ambiguous)
        let second_row = rows.next()?;
        if second_row.is_none() {
            return Ok(Some(first));
        }

        // Ambiguous - collect remaining matches for error message
        let second_bytes: Vec<u8> = second_row.unwrap().get(0)?;
        let mut previews = vec![format!("{}...", &first[..12.min(first.len())])];
        if let Some(hex) = uuid_bytes_to_hex(&second_bytes) {
            previews.push(format!("{}...", &hex[..12.min(hex.len())]));
        }

        let mut count = 2;
        while let Some(row) = rows.next()? {
            count += 1;
            if previews.len() < 5 {
                let id_bytes: Vec<u8> = row.get(0)?;
                if let Some(hex) = uuid_bytes_to_hex(&id_bytes) {
                    previews.push(format!("{}...", &hex[..12.min(hex.len())]));
                }
            }
        }

        let extra = if count > 5 {
            format!(" (and {} more)", count - 5)
        } else {
            String::new()
        };

        Err(VoiceError::validation(
            field_name,
            format!(
                "ambiguous prefix '{}' matches {} {}s: {}{}",
                id_or_prefix, count, entity_name, previews.join(", "), extra
            ),
        ))
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
                GROUP_CONCAT(t.name, ', ') as tag_names
            FROM notes n
            LEFT JOIN note_tags nt ON n.id = nt.note_id AND nt.deleted_at IS NULL
            LEFT JOIN tags t ON nt.tag_id = t.id
            WHERE n.deleted_at IS NULL
            GROUP BY n.id
            ORDER BY n.created_at DESC
            "#,
        )?;

        let rows = stmt.query_map([], |row| self.row_to_note(row))?;
        let mut notes = Vec::new();
        for note in rows {
            notes.push(note?);
        }
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
                GROUP_CONCAT(t.name, ', ') as tag_names
            FROM notes n
            LEFT JOIN note_tags nt ON n.id = nt.note_id AND nt.deleted_at IS NULL
            LEFT JOIN tags t ON nt.tag_id = t.id
            WHERE n.id = ?
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
            "INSERT INTO notes (id, content, created_at) VALUES (?, ?, datetime('now'))",
            params![uuid_bytes, content],
        )?;

        Ok(note_id.simple().to_string())
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
            SET content = ?, modified_at = datetime('now')
            WHERE id = ? AND deleted_at IS NULL
            "#,
            params![content, uuid_bytes],
        )?;

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
            SET deleted_at = datetime('now'), modified_at = datetime('now')
            WHERE id = ? AND deleted_at IS NULL
            "#,
            params![uuid_bytes],
        )?;

        Ok(deleted > 0)
    }

    /// Delete a tag (hard delete - cascades to note_tags) (accepts ID or ID prefix)
    pub fn delete_tag(&self, tag_id: &str) -> VoiceResult<bool> {
        // Use try_resolve to return false if not found (instead of error)
        let resolved_id = match self.try_resolve_tag_id(tag_id)? {
            Some(id) => id,
            None => return Ok(false),
        };
        let uuid = Uuid::parse_str(&resolved_id)
            .map_err(|e| VoiceError::validation("tag_id", e.to_string()))?;
        let uuid_bytes = uuid.as_bytes().to_vec();

        let deleted = self.conn.execute(
            "DELETE FROM tags WHERE id = ?",
            params![uuid_bytes],
        )?;

        Ok(deleted > 0)
    }

    /// Get all tags with their hierarchy information
    pub fn get_all_tags(&self) -> VoiceResult<Vec<TagRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, parent_id, created_at, modified_at FROM tags ORDER BY name",
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
            "SELECT id, name, parent_id, created_at, modified_at FROM tags WHERE id = ?",
        )?;

        let mut rows = stmt.query_map([uuid_bytes], |row| self.row_to_tag(row))?;
        match rows.next() {
            Some(Ok(tag)) => Ok(Some(tag)),
            Some(Err(e)) => Err(VoiceError::Database(e)),
            None => Ok(None),
        }
    }

    /// Get all tags with a given name (case-insensitive)
    pub fn get_tags_by_name(&self, name: &str) -> VoiceResult<Vec<TagRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, parent_id, created_at, modified_at FROM tags WHERE LOWER(name) = LOWER(?)",
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
                    "SELECT id, name, parent_id, created_at, modified_at FROM tags WHERE LOWER(name) = LOWER(?) AND parent_id IS NULL",
                    [part],
                    |row| self.row_to_tag(row),
                )
            } else {
                self.conn.query_row(
                    "SELECT id, name, parent_id, created_at, modified_at FROM tags WHERE LOWER(name) = LOWER(?) AND parent_id = ?",
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

    /// Get tags by name with no parent (root tags)
    fn get_tags_by_name_and_no_parent(&self, name: &str) -> VoiceResult<Vec<TagRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, parent_id, created_at, modified_at FROM tags WHERE LOWER(name) = LOWER(?) AND parent_id IS NULL",
        )?;

        let rows = stmt.query_map([name], |row| self.row_to_tag(row))?;
        let mut tags = Vec::new();
        for tag in rows {
            tags.push(tag?);
        }
        Ok(tags)
    }

    /// Get tags by name and parent ID
    fn get_tags_by_name_and_parent(&self, name: &str, parent_id: &str) -> VoiceResult<Vec<TagRow>> {
        let uuid = validate_tag_id(parent_id)?;
        let uuid_bytes = uuid.as_bytes().to_vec();

        let mut stmt = self.conn.prepare(
            "SELECT id, name, parent_id, created_at, modified_at FROM tags WHERE LOWER(name) = LOWER(?) AND parent_id = ?",
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
                GROUP_CONCAT(t.name, ', ') as tag_names
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

        let rows = stmt.query_map(params_refs.as_slice(), |row| self.row_to_note(row))?;
        let mut notes = Vec::new();
        for note in rows {
            notes.push(note?);
        }
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
                GROUP_CONCAT(t.name, ', ') as tag_names
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

        let rows = stmt.query_map(params_refs.as_slice(), |row| self.row_to_note(row))?;
        let mut notes = Vec::new();
        for note in rows {
            notes.push(note?);
        }
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
            "INSERT INTO tags (id, name, parent_id, created_at) VALUES (?, ?, ?, datetime('now'))",
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
            "UPDATE tags SET name = ?, modified_at = datetime('now') WHERE id = ?",
            params![new_name, uuid_bytes],
        )?;

        Ok(updated > 0)
    }

    /// Add a tag to a note (accepts ID or ID prefix for both)
    pub fn add_tag_to_note(&self, note_id: &str, tag_id: &str) -> VoiceResult<bool> {
        // Use try_resolve to return false if either entity not found
        let resolved_note_id = match self.try_resolve_note_id(note_id)? {
            Some(id) => id,
            None => return Ok(false),
        };
        let resolved_tag_id = match self.try_resolve_tag_id(tag_id)? {
            Some(id) => id,
            None => return Ok(false),
        };
        let note_uuid = Uuid::parse_str(&resolved_note_id)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?;
        let tag_uuid = Uuid::parse_str(&resolved_tag_id)
            .map_err(|e| VoiceError::validation("tag_id", e.to_string()))?;
        let note_bytes = note_uuid.as_bytes().to_vec();
        let tag_bytes = tag_uuid.as_bytes().to_vec();

        // Check if association exists (including soft-deleted)
        let existing: Option<Option<String>> = self
            .conn
            .query_row(
                "SELECT deleted_at FROM note_tags WHERE note_id = ? AND tag_id = ?",
                params![&note_bytes, &tag_bytes],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?;

        let changed = match existing {
            Some(deleted_at) => {
                if deleted_at.is_some() {
                    // Reactivate soft-deleted association
                    self.conn.execute(
                        "UPDATE note_tags SET deleted_at = NULL, modified_at = datetime('now') WHERE note_id = ? AND tag_id = ?",
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
                    "INSERT INTO note_tags (note_id, tag_id, created_at) VALUES (?, ?, datetime('now'))",
                    params![&note_bytes, &tag_bytes],
                )?;
                true
            }
        };

        // Update the parent Note's modified_at to trigger sync
        if changed {
            self.conn.execute(
                "UPDATE notes SET modified_at = datetime('now') WHERE id = ?",
                params![note_bytes],
            )?;
        }

        Ok(changed)
    }

    /// Remove a tag from a note (soft delete) (accepts ID or ID prefix for both)
    pub fn remove_tag_from_note(&self, note_id: &str, tag_id: &str) -> VoiceResult<bool> {
        // Use try_resolve to return false if either entity not found
        let resolved_note_id = match self.try_resolve_note_id(note_id)? {
            Some(id) => id,
            None => return Ok(false),
        };
        let resolved_tag_id = match self.try_resolve_tag_id(tag_id)? {
            Some(id) => id,
            None => return Ok(false),
        };
        let note_uuid = Uuid::parse_str(&resolved_note_id)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?;
        let tag_uuid = Uuid::parse_str(&resolved_tag_id)
            .map_err(|e| VoiceError::validation("tag_id", e.to_string()))?;
        let note_bytes = note_uuid.as_bytes().to_vec();
        let tag_bytes = tag_uuid.as_bytes().to_vec();

        let updated = self.conn.execute(
            "UPDATE note_tags SET deleted_at = datetime('now'), modified_at = datetime('now') WHERE note_id = ? AND tag_id = ? AND deleted_at IS NULL",
            params![&note_bytes, &tag_bytes],
        )?;

        // Update the parent Note's modified_at to trigger sync
        if updated > 0 {
            self.conn.execute(
                "UPDATE notes SET modified_at = datetime('now') WHERE id = ?",
                params![note_bytes],
            )?;
        }

        Ok(updated > 0)
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

    /// Close the database connection
    pub fn close(self) -> VoiceResult<()> {
        // Connection is closed when dropped
        Ok(())
    }

    // ============================================================================
    // Sync methods
    // ============================================================================

    /// Get the last sync timestamp for a peer
    pub fn get_peer_last_sync(&self, peer_device_id: &str) -> VoiceResult<Option<String>> {
        let peer_uuid = Uuid::parse_str(peer_device_id)
            .map_err(|e| VoiceError::validation("peer_device_id", e.to_string()))?;
        let peer_bytes = peer_uuid.as_bytes().to_vec();

        let result: Option<Option<String>> = self
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
            VALUES (?, ?, '', datetime('now'))
            ON CONFLICT(peer_id) DO UPDATE SET
                peer_name = COALESCE(excluded.peer_name, peer_name),
                last_sync_at = datetime('now')
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

    /// Get all changes since a timestamp (for sync)
    pub fn get_changes_since(&self, since: Option<&str>, limit: i64) -> VoiceResult<(Vec<HashMap<String, serde_json::Value>>, Option<String>)> {
        let mut changes = Vec::new();
        let mut latest_timestamp: Option<String> = None;

        // Get note changes
        let note_rows: Vec<(Vec<u8>, String, String, Option<String>, Option<String>)> = if let Some(since_ts) = since {
            let mut stmt = self.conn.prepare(
                r#"
                SELECT id, created_at, content, modified_at, deleted_at
                FROM notes
                WHERE modified_at >= ? OR (modified_at IS NULL AND created_at >= ?)
                ORDER BY COALESCE(modified_at, created_at)
                LIMIT ?
                "#,
            )?;
            let rows = stmt.query_map(params![since_ts, since_ts, limit], |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
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
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };

        for (id_bytes, created_at, content, modified_at, deleted_at) in note_rows {
            let timestamp = modified_at.clone().unwrap_or_else(|| created_at.clone());
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
            change.insert("timestamp".to_string(), serde_json::Value::String(timestamp.clone()));

            let mut data = serde_json::Map::new();
            data.insert("id".to_string(), serde_json::Value::String(id_hex));
            data.insert("created_at".to_string(), serde_json::Value::String(created_at));
            data.insert("content".to_string(), serde_json::Value::String(content));
            data.insert("modified_at".to_string(), modified_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            data.insert("deleted_at".to_string(), deleted_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            change.insert("data".to_string(), serde_json::Value::Object(data));

            latest_timestamp = Some(timestamp);
            changes.push(change);
        }

        // Get tag changes
        let remaining = limit - changes.len() as i64;
        if remaining > 0 {
            let tag_rows: Vec<(Vec<u8>, String, Option<Vec<u8>>, String, Option<String>)> = if let Some(since_ts) = since {
                let mut stmt = self.conn.prepare(
                    r#"
                    SELECT id, name, parent_id, created_at, modified_at
                    FROM tags
                    WHERE modified_at >= ? OR (modified_at IS NULL AND created_at >= ?)
                    ORDER BY COALESCE(modified_at, created_at)
                    LIMIT ?
                    "#,
                )?;
                let rows = stmt.query_map(params![since_ts, since_ts, remaining], |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<Vec<u8>>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            } else {
                let mut stmt = self.conn.prepare(
                    r#"
                    SELECT id, name, parent_id, created_at, modified_at
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
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            };

            for (id_bytes, name, parent_id_bytes, created_at, modified_at) in tag_rows {
                let timestamp = modified_at.clone().unwrap_or_else(|| created_at.clone());
                let operation = if modified_at.is_some() { "update" } else { "create" };

                let id_hex = uuid_bytes_to_hex(&id_bytes).unwrap_or_default();
                let parent_id_hex = parent_id_bytes.and_then(|b| uuid_bytes_to_hex(&b));

                let mut change = HashMap::new();
                change.insert("entity_type".to_string(), serde_json::Value::String("tag".to_string()));
                change.insert("entity_id".to_string(), serde_json::Value::String(id_hex.clone()));
                change.insert("operation".to_string(), serde_json::Value::String(operation.to_string()));
                change.insert("timestamp".to_string(), serde_json::Value::String(timestamp.clone()));

                let mut data = serde_json::Map::new();
                data.insert("id".to_string(), serde_json::Value::String(id_hex));
                data.insert("name".to_string(), serde_json::Value::String(name));
                data.insert("parent_id".to_string(), parent_id_hex.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
                data.insert("created_at".to_string(), serde_json::Value::String(created_at));
                data.insert("modified_at".to_string(), modified_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
                change.insert("data".to_string(), serde_json::Value::Object(data));

                if latest_timestamp.is_none() || timestamp > *latest_timestamp.as_ref().unwrap() {
                    latest_timestamp = Some(timestamp);
                }
                changes.push(change);
            }
        }

        // Get note_tag changes
        let remaining = limit - changes.len() as i64;
        if remaining > 0 {
            let nt_rows: Vec<(Vec<u8>, Vec<u8>, String, Option<String>, Option<String>)> = if let Some(since_ts) = since {
                let mut stmt = self.conn.prepare(
                    r#"
                    SELECT note_id, tag_id, created_at, modified_at, deleted_at
                    FROM note_tags
                    WHERE created_at >= ? OR deleted_at >= ? OR modified_at >= ?
                    ORDER BY COALESCE(modified_at, deleted_at, created_at)
                    LIMIT ?
                    "#,
                )?;
                let rows = stmt.query_map(params![since_ts, since_ts, since_ts, remaining], |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
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
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            };

            for (note_id_bytes, tag_id_bytes, created_at, modified_at, deleted_at) in nt_rows {
                let timestamp = modified_at.clone()
                    .or_else(|| deleted_at.clone())
                    .unwrap_or_else(|| created_at.clone());

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
                change.insert("timestamp".to_string(), serde_json::Value::String(timestamp.clone()));

                let mut data = serde_json::Map::new();
                data.insert("note_id".to_string(), serde_json::Value::String(note_id_hex));
                data.insert("tag_id".to_string(), serde_json::Value::String(tag_id_hex));
                data.insert("created_at".to_string(), serde_json::Value::String(created_at));
                data.insert("modified_at".to_string(), modified_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
                data.insert("deleted_at".to_string(), deleted_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
                change.insert("data".to_string(), serde_json::Value::Object(data));

                if latest_timestamp.is_none() || timestamp > *latest_timestamp.as_ref().unwrap() {
                    latest_timestamp = Some(timestamp);
                }
                changes.push(change);
            }
        }

        // Get audio_file changes
        let remaining = limit - changes.len() as i64;
        if remaining > 0 {
            let audio_rows: Vec<(Vec<u8>, String, String, Option<String>, Option<String>, Option<String>, Option<String>)> = if let Some(since_ts) = since {
                let mut stmt = self.conn.prepare(
                    r#"
                    SELECT id, imported_at, filename, file_created_at, summary, modified_at, deleted_at
                    FROM audio_files
                    WHERE modified_at >= ? OR (modified_at IS NULL AND imported_at >= ?)
                    ORDER BY COALESCE(modified_at, imported_at)
                    LIMIT ?
                    "#,
                )?;
                let rows = stmt.query_map(params![since_ts, since_ts, remaining], |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                    ))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            } else {
                let mut stmt = self.conn.prepare(
                    r#"
                    SELECT id, imported_at, filename, file_created_at, summary, modified_at, deleted_at
                    FROM audio_files
                    ORDER BY COALESCE(modified_at, imported_at)
                    LIMIT ?
                    "#,
                )?;
                let rows = stmt.query_map(params![remaining], |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                    ))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            };

            for (id_bytes, imported_at, filename, file_created_at, summary, modified_at, deleted_at) in audio_rows {
                let timestamp = modified_at.clone().unwrap_or_else(|| imported_at.clone());
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
                change.insert("timestamp".to_string(), serde_json::Value::String(timestamp.clone()));

                let mut data = serde_json::Map::new();
                data.insert("id".to_string(), serde_json::Value::String(id_hex));
                data.insert("imported_at".to_string(), serde_json::Value::String(imported_at));
                data.insert("filename".to_string(), serde_json::Value::String(filename));
                data.insert("file_created_at".to_string(), file_created_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
                data.insert("summary".to_string(), summary.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
                data.insert("modified_at".to_string(), modified_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
                data.insert("deleted_at".to_string(), deleted_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
                change.insert("data".to_string(), serde_json::Value::Object(data));

                if latest_timestamp.is_none() || timestamp > *latest_timestamp.as_ref().unwrap() {
                    latest_timestamp = Some(timestamp);
                }
                changes.push(change);
            }
        }

        // Get note_attachment changes
        let remaining = limit - changes.len() as i64;
        if remaining > 0 {
            let na_rows: Vec<(Vec<u8>, Vec<u8>, Vec<u8>, String, String, Option<String>, Option<String>)> = if let Some(since_ts) = since {
                let mut stmt = self.conn.prepare(
                    r#"
                    SELECT id, note_id, attachment_id, attachment_type, created_at, modified_at, deleted_at
                    FROM note_attachments
                    WHERE created_at >= ? OR deleted_at >= ? OR modified_at >= ?
                    ORDER BY COALESCE(modified_at, deleted_at, created_at)
                    LIMIT ?
                    "#,
                )?;
                let rows = stmt.query_map(params![since_ts, since_ts, since_ts, remaining], |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
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
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                    ))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            };

            for (id_bytes, note_id_bytes, attachment_id_bytes, attachment_type, created_at, modified_at, deleted_at) in na_rows {
                let timestamp = modified_at.clone()
                    .or_else(|| deleted_at.clone())
                    .unwrap_or_else(|| created_at.clone());

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
                change.insert("timestamp".to_string(), serde_json::Value::String(timestamp.clone()));

                let mut data = serde_json::Map::new();
                data.insert("id".to_string(), serde_json::Value::String(id_hex));
                data.insert("note_id".to_string(), serde_json::Value::String(note_id_hex));
                data.insert("attachment_id".to_string(), serde_json::Value::String(attachment_id_hex));
                data.insert("attachment_type".to_string(), serde_json::Value::String(attachment_type));
                data.insert("created_at".to_string(), serde_json::Value::String(created_at));
                data.insert("modified_at".to_string(), modified_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
                data.insert("deleted_at".to_string(), deleted_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
                change.insert("data".to_string(), serde_json::Value::Object(data));

                if latest_timestamp.is_none() || timestamp > *latest_timestamp.as_ref().unwrap() {
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
    pub fn get_changes_since_exclusive(&self, since: Option<&str>, limit: i64) -> VoiceResult<(Vec<HashMap<String, serde_json::Value>>, Option<String>)> {
        let since_ts = match since {
            Some(ts) => ts,
            None => return self.get_changes_since(None, limit),
        };

        let mut changes = Vec::new();
        let mut latest_timestamp: Option<String> = None;

        // Check notes with > (exclusive)
        let note_count: i64 = self.conn.query_row(
            r#"
            SELECT COUNT(*) FROM notes
            WHERE modified_at > ? OR (modified_at IS NULL AND created_at > ?)
            "#,
            params![since_ts, since_ts],
            |row| row.get(0),
        )?;

        if note_count > 0 {
            let mut stmt = self.conn.prepare(
                r#"
                SELECT id, created_at, content, modified_at, deleted_at
                FROM notes
                WHERE modified_at > ? OR (modified_at IS NULL AND created_at > ?)
                ORDER BY COALESCE(modified_at, created_at)
                LIMIT ?
                "#,
            )?;
            let rows = stmt.query_map(params![since_ts, since_ts, limit], |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            })?;

            for row in rows {
                let (id_bytes, created_at, content, modified_at, deleted_at) = row?;
                let timestamp = modified_at.clone().unwrap_or_else(|| created_at.clone());
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
                change.insert("timestamp".to_string(), serde_json::Value::String(timestamp.clone()));

                let mut data = serde_json::Map::new();
                data.insert("id".to_string(), serde_json::Value::String(id_hex));
                data.insert("created_at".to_string(), serde_json::Value::String(created_at));
                data.insert("content".to_string(), serde_json::Value::String(content));
                data.insert("modified_at".to_string(), modified_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
                data.insert("deleted_at".to_string(), deleted_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
                change.insert("data".to_string(), serde_json::Value::Object(data));

                latest_timestamp = Some(timestamp);
                changes.push(change);

                if changes.len() >= limit as usize {
                    return Ok((changes, latest_timestamp));
                }
            }
        }

        // Check audio_files with > (exclusive)
        let remaining = limit - changes.len() as i64;
        if remaining > 0 {
            let mut stmt = self.conn.prepare(
                r#"
                SELECT id, imported_at, filename, file_created_at, summary, modified_at, deleted_at
                FROM audio_files
                WHERE modified_at > ? OR (modified_at IS NULL AND imported_at > ?)
                ORDER BY COALESCE(modified_at, imported_at)
                LIMIT ?
                "#,
            )?;
            let rows = stmt.query_map(params![since_ts, since_ts, remaining], |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                ))
            })?;

            for row in rows {
                let (id_bytes, imported_at, _filename, _file_created_at, _summary, modified_at, deleted_at) = row?;
                let timestamp = modified_at.clone().unwrap_or_else(|| imported_at.clone());

                let id_hex = uuid_bytes_to_hex(&id_bytes).unwrap_or_default();
                let mut change = HashMap::new();
                change.insert("entity_type".to_string(), serde_json::Value::String("audio_file".to_string()));
                change.insert("entity_id".to_string(), serde_json::Value::String(id_hex));
                change.insert("timestamp".to_string(), serde_json::Value::String(timestamp.clone()));

                if latest_timestamp.is_none() || timestamp > *latest_timestamp.as_ref().unwrap() {
                    latest_timestamp = Some(timestamp);
                }
                changes.push(change);
            }
        }

        // Check note_attachments with > (exclusive)
        let remaining = limit - changes.len() as i64;
        if remaining > 0 {
            let mut stmt = self.conn.prepare(
                r#"
                SELECT id, note_id, attachment_id, attachment_type, created_at, modified_at, deleted_at
                FROM note_attachments
                WHERE modified_at > ? OR (modified_at IS NULL AND created_at > ?)
                ORDER BY COALESCE(modified_at, created_at)
                LIMIT ?
                "#,
            )?;
            let rows = stmt.query_map(params![since_ts, since_ts, remaining], |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                ))
            })?;

            for row in rows {
                let (id_bytes, _note_id_bytes, _attachment_id_bytes, _attachment_type, created_at, modified_at, _deleted_at) = row?;
                let timestamp = modified_at.clone().unwrap_or_else(|| created_at.clone());

                let id_hex = uuid_bytes_to_hex(&id_bytes).unwrap_or_default();
                let mut change = HashMap::new();
                change.insert("entity_type".to_string(), serde_json::Value::String("note_attachment".to_string()));
                change.insert("entity_id".to_string(), serde_json::Value::String(id_hex));
                change.insert("timestamp".to_string(), serde_json::Value::String(timestamp.clone()));

                if latest_timestamp.is_none() || timestamp > *latest_timestamp.as_ref().unwrap() {
                    latest_timestamp = Some(timestamp);
                }
                changes.push(change);
            }
        }

        Ok((changes, latest_timestamp))
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
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
            ))
        })?;

        let mut notes = Vec::new();
        for row in note_rows {
            let (id_bytes, created_at, content, modified_at, deleted_at) = row?;
            let mut note = HashMap::new();
            note.insert("id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&id_bytes).unwrap_or_default()));
            note.insert("created_at".to_string(), serde_json::Value::String(created_at));
            note.insert("content".to_string(), serde_json::Value::String(content));
            note.insert("modified_at".to_string(), modified_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            note.insert("deleted_at".to_string(), deleted_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            notes.push(note);
        }
        result.insert("notes".to_string(), notes);

        // Get all tags
        let mut stmt = self.conn.prepare(
            r#"SELECT id, name, parent_id, created_at, modified_at FROM tags"#
        )?;
        let tag_rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<Vec<u8>>>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
            ))
        })?;

        let mut tags = Vec::new();
        for row in tag_rows {
            let (id_bytes, name, parent_id_bytes, created_at, modified_at) = row?;
            let mut tag = HashMap::new();
            tag.insert("id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&id_bytes).unwrap_or_default()));
            tag.insert("name".to_string(), serde_json::Value::String(name));
            tag.insert("parent_id".to_string(), parent_id_bytes.and_then(|b| uuid_bytes_to_hex(&b)).map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            tag.insert("created_at".to_string(), serde_json::Value::String(created_at));
            tag.insert("modified_at".to_string(), modified_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
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
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
            ))
        })?;

        let mut note_tags = Vec::new();
        for row in nt_rows {
            let (note_id_bytes, tag_id_bytes, created_at, modified_at, deleted_at) = row?;
            let mut nt = HashMap::new();
            nt.insert("note_id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&note_id_bytes).unwrap_or_default()));
            nt.insert("tag_id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&tag_id_bytes).unwrap_or_default()));
            nt.insert("created_at".to_string(), serde_json::Value::String(created_at));
            nt.insert("modified_at".to_string(), modified_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            nt.insert("deleted_at".to_string(), deleted_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            note_tags.push(nt);
        }
        result.insert("note_tags".to_string(), note_tags);

        // Get all audio_files
        let mut stmt = self.conn.prepare(
            r#"SELECT id, imported_at, filename, file_created_at, summary, device_id, modified_at, deleted_at FROM audio_files"#
        )?;
        let af_rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Vec<u8>>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Option<String>>(7)?,
            ))
        })?;

        let mut audio_files = Vec::new();
        for row in af_rows {
            let (id_bytes, imported_at, filename, file_created_at, summary, device_id_bytes, modified_at, deleted_at) = row?;
            let mut af = HashMap::new();
            af.insert("id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&id_bytes).unwrap_or_default()));
            af.insert("imported_at".to_string(), serde_json::Value::String(imported_at));
            af.insert("filename".to_string(), serde_json::Value::String(filename));
            af.insert("file_created_at".to_string(), file_created_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            af.insert("summary".to_string(), summary.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            af.insert("device_id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&device_id_bytes).unwrap_or_default()));
            af.insert("modified_at".to_string(), modified_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            af.insert("deleted_at".to_string(), deleted_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
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
                row.get::<_, String>(4)?,
                row.get::<_, Vec<u8>>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Option<String>>(7)?,
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
            na.insert("created_at".to_string(), serde_json::Value::String(created_at));
            na.insert("device_id".to_string(), serde_json::Value::String(uuid_bytes_to_hex(&device_id_bytes).unwrap_or_default()));
            na.insert("modified_at".to_string(), modified_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            na.insert("deleted_at".to_string(), deleted_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
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
        created_at: &str,
        content: &str,
        modified_at: Option<&str>,
        deleted_at: Option<&str>,
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
                "UPDATE notes SET content = ?, modified_at = ?, deleted_at = ? WHERE id = ?",
                params![content, modified_at, deleted_at, uuid_bytes],
            )?;
        } else {
            // Insert new note
            self.conn.execute(
                "INSERT INTO notes (id, created_at, content, modified_at, deleted_at) VALUES (?, ?, ?, ?, ?)",
                params![uuid_bytes, created_at, content, modified_at, deleted_at],
            )?;
        }
        Ok(true)
    }

    /// Apply a sync tag change
    pub fn apply_sync_tag(
        &self,
        tag_id: &str,
        name: &str,
        parent_id: Option<&str>,
        created_at: &str,
        modified_at: Option<&str>,
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
                "UPDATE tags SET name = ?, parent_id = ?, modified_at = ? WHERE id = ?",
                params![name, parent_bytes, modified_at, uuid_bytes],
            )?;
        } else {
            // Insert new tag
            self.conn.execute(
                "INSERT INTO tags (id, name, parent_id, created_at, modified_at) VALUES (?, ?, ?, ?, ?)",
                params![uuid_bytes, name, parent_bytes, created_at, modified_at],
            )?;
        }
        Ok(true)
    }

    /// Apply a sync note_tag change
    pub fn apply_sync_note_tag(
        &self,
        note_id: &str,
        tag_id: &str,
        created_at: &str,
        modified_at: Option<&str>,
        deleted_at: Option<&str>,
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
                "UPDATE note_tags SET modified_at = ?, deleted_at = ? WHERE note_id = ? AND tag_id = ?",
                params![modified_at, deleted_at, note_bytes, tag_bytes],
            )?;
        } else {
            // Insert new association
            self.conn.execute(
                "INSERT INTO note_tags (note_id, tag_id, created_at, modified_at, deleted_at) VALUES (?, ?, ?, ?, ?)",
                params![note_bytes, tag_bytes, created_at, modified_at, deleted_at],
            )?;
        }
        Ok(true)
    }

    /// Get raw note data by ID (including deleted, for sync)
    pub fn get_note_raw(&self, note_id: &str) -> VoiceResult<Option<HashMap<String, serde_json::Value>>> {
        let uuid = validate_note_id(note_id)?;
        let uuid_bytes = uuid.as_bytes().to_vec();

        let result: Option<(Vec<u8>, String, String, Option<String>, Option<String>)> = self.conn
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
                note.insert("created_at".to_string(), serde_json::Value::String(created_at));
                note.insert("content".to_string(), serde_json::Value::String(content));
                note.insert("modified_at".to_string(), modified_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
                note.insert("deleted_at".to_string(), deleted_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
                Ok(Some(note))
            }
            None => Ok(None),
        }
    }

    /// Get raw tag data by ID (for sync)
    pub fn get_tag_raw(&self, tag_id: &str) -> VoiceResult<Option<HashMap<String, serde_json::Value>>> {
        let uuid = validate_tag_id(tag_id)?;
        let uuid_bytes = uuid.as_bytes().to_vec();

        let result: Option<(Vec<u8>, String, Option<Vec<u8>>, String, Option<String>)> = self.conn
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
                tag.insert("created_at".to_string(), serde_json::Value::String(created_at));
                tag.insert("modified_at".to_string(), modified_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
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

        let result: Option<(Vec<u8>, Vec<u8>, String, Option<String>, Option<String>)> = self.conn
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
                nt.insert("created_at".to_string(), serde_json::Value::String(created_at));
                nt.insert("modified_at".to_string(), modified_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
                nt.insert("deleted_at".to_string(), deleted_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
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
        local_modified_at: &str,
        remote_content: &str,
        remote_modified_at: &str,
        remote_device_id: Option<&str>,
        remote_device_name: Option<&str>,
    ) -> VoiceResult<String> {
        let conflict_id = Uuid::now_v7();
        let conflict_bytes = conflict_id.as_bytes().to_vec();

        let note_uuid = Uuid::parse_str(note_id)
            .map_err(|e| VoiceError::validation("note_id", e.to_string()))?;
        let note_bytes = note_uuid.as_bytes().to_vec();

        let remote_device_bytes = remote_device_id.and_then(|id| {
            Uuid::parse_str(id).ok().map(|u| u.as_bytes().to_vec())
        });

        self.conn.execute(
            r#"
            INSERT INTO conflicts_note_content
            (id, note_id, local_content, local_modified_at, local_device_id,
             remote_content, remote_modified_at, remote_device_id, remote_device_name, created_at)
            VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, datetime('now'))
            "#,
            params![
                conflict_bytes,
                note_bytes,
                local_content,
                local_modified_at,
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
        surviving_modified_at: &str,
        surviving_device_id: Option<&str>,
        deleted_content: Option<&str>,
        deleted_at: &str,
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
             deleted_content, deleted_at, deleting_device_id, deleting_device_name, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            "#,
            params![
                conflict_bytes,
                note_bytes,
                surviving_content,
                surviving_modified_at,
                surviving_device_bytes,
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
        local_modified_at: &str,
        remote_name: &str,
        remote_modified_at: &str,
        remote_device_id: Option<&str>,
        remote_device_name: Option<&str>,
    ) -> VoiceResult<String> {
        let conflict_id = Uuid::now_v7();
        let conflict_bytes = conflict_id.as_bytes().to_vec();

        let tag_uuid = Uuid::parse_str(tag_id)
            .map_err(|e| VoiceError::validation("tag_id", e.to_string()))?;
        let tag_bytes = tag_uuid.as_bytes().to_vec();

        let remote_device_bytes = remote_device_id.and_then(|id| {
            Uuid::parse_str(id).ok().map(|u| u.as_bytes().to_vec())
        });

        self.conn.execute(
            r#"
            INSERT INTO conflicts_tag_rename
            (id, tag_id, local_name, local_modified_at, local_device_id,
             remote_name, remote_modified_at, remote_device_id, remote_device_name, created_at)
            VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, datetime('now'))
            "#,
            params![
                conflict_bytes,
                tag_bytes,
                local_name,
                local_modified_at,
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
        local_created_at: Option<&str>,
        local_modified_at: Option<&str>,
        local_deleted_at: Option<&str>,
        remote_created_at: Option<&str>,
        remote_modified_at: Option<&str>,
        remote_deleted_at: Option<&str>,
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

        let remote_device_bytes = remote_device_id.and_then(|id| {
            Uuid::parse_str(id).ok().map(|u| u.as_bytes().to_vec())
        });

        self.conn.execute(
            r#"
            INSERT INTO conflicts_note_tag
            (id, note_id, tag_id,
             local_created_at, local_modified_at, local_deleted_at,
             remote_created_at, remote_modified_at, remote_deleted_at,
             remote_device_id, remote_device_name, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            "#,
            params![
                conflict_bytes,
                note_bytes,
                tag_bytes,
                local_created_at,
                local_modified_at,
                local_deleted_at,
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
        local_modified_at: &str,
        remote_parent_id: Option<&str>,
        remote_modified_at: &str,
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

        let remote_parent_bytes = remote_parent_id.and_then(|id| {
            Uuid::parse_str(id).ok().map(|u| u.as_bytes().to_vec())
        });

        let remote_device_bytes = remote_device_id.and_then(|id| {
            Uuid::parse_str(id).ok().map(|u| u.as_bytes().to_vec())
        });

        self.conn.execute(
            r#"
            INSERT INTO conflicts_tag_parent
            (id, tag_id, local_parent_id, local_modified_at, local_device_id,
             remote_parent_id, remote_modified_at, remote_device_id, remote_device_name, created_at)
            VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, datetime('now'))
            "#,
            params![
                conflict_bytes,
                tag_bytes,
                local_parent_bytes,
                local_modified_at,
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
        surviving_modified_at: &str,
        surviving_device_id: Option<&str>,
        surviving_device_name: Option<&str>,
        deleted_at: &str,
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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
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
            let local_modified_at: String = row.get(3)?;
            let local_device_id: Option<Vec<u8>> = row.get(4)?;
            let local_device_name: Option<String> = row.get(5)?;
            let remote_content: String = row.get(6)?;
            let remote_modified_at: String = row.get(7)?;
            let remote_device_id: Option<Vec<u8>> = row.get(8)?;
            let remote_device_name: Option<String> = row.get(9)?;
            let created_at: String = row.get(10)?;
            let resolved_at: Option<String> = row.get(11)?;

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
            conflict.insert("local_modified_at".to_string(), serde_json::Value::String(local_modified_at));
            conflict.insert("local_device_id".to_string(), local_device_id.and_then(|b| uuid_bytes_to_hex(&b)).map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("local_device_name".to_string(), local_device_name.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("remote_content".to_string(), serde_json::Value::String(remote_content));
            conflict.insert("remote_modified_at".to_string(), serde_json::Value::String(remote_modified_at));
            conflict.insert("remote_device_id".to_string(), remote_device_id.and_then(|b| uuid_bytes_to_hex(&b)).map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("remote_device_name".to_string(), remote_device_name.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("created_at".to_string(), serde_json::Value::String(created_at));
            conflict.insert("resolved_at".to_string(), resolved_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
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
            let surviving_modified_at: String = row.get(3)?;
            let surviving_device_id: Option<Vec<u8>> = row.get(4)?;
            let surviving_device_name: Option<String> = row.get(5)?;
            let deleted_content: Option<String> = row.get(6)?;
            let deleted_at: String = row.get(7)?;
            let deleting_device_id: Option<Vec<u8>> = row.get(8)?;
            let deleting_device_name: Option<String> = row.get(9)?;
            let created_at: String = row.get(10)?;
            let resolved_at: Option<String> = row.get(11)?;

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
            conflict.insert("surviving_modified_at".to_string(), serde_json::Value::String(surviving_modified_at));
            conflict.insert("surviving_device_id".to_string(), surviving_device_id.and_then(|b| uuid_bytes_to_hex(&b)).map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("surviving_device_name".to_string(), surviving_device_name.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("deleted_content".to_string(), deleted_content.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("deleted_at".to_string(), serde_json::Value::String(deleted_at));
            conflict.insert("deleting_device_id".to_string(), deleting_device_id.and_then(|b| uuid_bytes_to_hex(&b)).map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("deleting_device_name".to_string(), deleting_device_name.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("created_at".to_string(), serde_json::Value::String(created_at));
            conflict.insert("resolved_at".to_string(), resolved_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
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
            let local_modified_at: String = row.get(3)?;
            let local_device_id: Option<Vec<u8>> = row.get(4)?;
            let local_device_name: Option<String> = row.get(5)?;
            let remote_name: String = row.get(6)?;
            let remote_modified_at: String = row.get(7)?;
            let remote_device_id: Option<Vec<u8>> = row.get(8)?;
            let remote_device_name: Option<String> = row.get(9)?;
            let created_at: String = row.get(10)?;
            let resolved_at: Option<String> = row.get(11)?;

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
            conflict.insert("local_modified_at".to_string(), serde_json::Value::String(local_modified_at));
            conflict.insert("local_device_id".to_string(), local_device_id.and_then(|b| uuid_bytes_to_hex(&b)).map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("local_device_name".to_string(), local_device_name.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("remote_name".to_string(), serde_json::Value::String(remote_name));
            conflict.insert("remote_modified_at".to_string(), serde_json::Value::String(remote_modified_at));
            conflict.insert("remote_device_id".to_string(), remote_device_id.and_then(|b| uuid_bytes_to_hex(&b)).map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("remote_device_name".to_string(), remote_device_name.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("created_at".to_string(), serde_json::Value::String(created_at));
            conflict.insert("resolved_at".to_string(), resolved_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
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
            let local_modified_at: String = row.get(3)?;
            let local_device_id: Option<Vec<u8>> = row.get(4)?;
            let local_device_name: Option<String> = row.get(5)?;
            let remote_parent_id: Option<Vec<u8>> = row.get(6)?;
            let remote_modified_at: String = row.get(7)?;
            let remote_device_id: Option<Vec<u8>> = row.get(8)?;
            let remote_device_name: Option<String> = row.get(9)?;
            let created_at: String = row.get(10)?;
            let resolved_at: Option<String> = row.get(11)?;

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
            conflict.insert("local_modified_at".to_string(), serde_json::Value::String(local_modified_at));
            conflict.insert("local_device_id".to_string(), local_device_id.and_then(|b| uuid_bytes_to_hex(&b)).map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("local_device_name".to_string(), local_device_name.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("remote_parent_id".to_string(), remote_parent_id.and_then(|b| uuid_bytes_to_hex(&b)).map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("remote_modified_at".to_string(), serde_json::Value::String(remote_modified_at));
            conflict.insert("remote_device_id".to_string(), remote_device_id.and_then(|b| uuid_bytes_to_hex(&b)).map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("remote_device_name".to_string(), remote_device_name.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("created_at".to_string(), serde_json::Value::String(created_at));
            conflict.insert("resolved_at".to_string(), resolved_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
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
            let surviving_modified_at: String = row.get(4)?;
            let surviving_device_id: Option<Vec<u8>> = row.get(5)?;
            let surviving_device_name: Option<String> = row.get(6)?;
            let deleted_at: String = row.get(7)?;
            let deleting_device_id: Option<Vec<u8>> = row.get(8)?;
            let deleting_device_name: Option<String> = row.get(9)?;
            let created_at: String = row.get(10)?;
            let resolved_at: Option<String> = row.get(11)?;

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
            conflict.insert("surviving_modified_at".to_string(), serde_json::Value::String(surviving_modified_at));
            conflict.insert("surviving_device_id".to_string(), surviving_device_id.and_then(|b| uuid_bytes_to_hex(&b)).map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("surviving_device_name".to_string(), surviving_device_name.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("deleted_at".to_string(), serde_json::Value::String(deleted_at));
            conflict.insert("deleting_device_id".to_string(), deleting_device_id.and_then(|b| uuid_bytes_to_hex(&b)).map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("deleting_device_name".to_string(), deleting_device_name.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
            conflict.insert("created_at".to_string(), serde_json::Value::String(created_at));
            conflict.insert("resolved_at".to_string(), resolved_at.map_or(serde_json::Value::Null, |s| serde_json::Value::String(s)));
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
            "UPDATE notes SET content = ?, modified_at = datetime('now') WHERE id = ?",
            params![new_content, note_id],
        )?;

        // Mark conflict as resolved
        self.conn.execute(
            "UPDATE conflicts_note_content SET resolved_at = datetime('now') WHERE id = ?",
            params![conflict_bytes],
        )?;

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
                "UPDATE notes SET content = ?, deleted_at = NULL, modified_at = datetime('now') WHERE id = ?",
                params![surviving_content, note_id],
            )?;
        }
        // If not restoring, the note stays deleted (no action needed)

        // Mark conflict as resolved
        self.conn.execute(
            "UPDATE conflicts_note_delete SET resolved_at = datetime('now') WHERE id = ?",
            params![conflict_bytes],
        )?;

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
            "UPDATE tags SET name = ?, modified_at = datetime('now') WHERE id = ?",
            params![new_name, tag_id],
        )?;

        // Mark conflict as resolved
        self.conn.execute(
            "UPDATE conflicts_tag_rename SET resolved_at = datetime('now') WHERE id = ?",
            params![conflict_bytes],
        )?;

        Ok(true)
    }

    // Helper methods for row conversion

    fn row_to_note(&self, row: &Row) -> rusqlite::Result<NoteRow> {
        let id_bytes: Vec<u8> = row.get(0)?;
        let created_at: String = row.get(1)?;
        let content: String = row.get(2)?;
        let modified_at: Option<String> = row.get(3)?;
        let deleted_at: Option<String> = row.get(4)?;
        let tag_names: Option<String> = row.get(5)?;

        Ok(NoteRow {
            id: uuid_bytes_to_hex(&id_bytes).unwrap_or_default(),
            created_at: parse_sqlite_datetime(&created_at),
            content,
            modified_at: modified_at.map(|s| parse_sqlite_datetime(&s)),
            deleted_at: deleted_at.map(|s| parse_sqlite_datetime(&s)),
            tag_names,
        })
    }

    fn row_to_tag(&self, row: &Row) -> rusqlite::Result<TagRow> {
        let id_bytes: Vec<u8> = row.get(0)?;
        let name: String = row.get(1)?;
        let parent_id_bytes: Option<Vec<u8>> = row.get(2)?;
        let created_at: Option<String> = row.get(3)?;
        let modified_at: Option<String> = row.get(4)?;

        Ok(TagRow {
            id: uuid_bytes_to_hex(&id_bytes).unwrap_or_default(),
            name,
            parent_id: parent_id_bytes.and_then(|b| uuid_bytes_to_hex(&b)),
            created_at: created_at.map(|s| parse_sqlite_datetime(&s)),
            modified_at: modified_at.map(|s| parse_sqlite_datetime(&s)),
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
            VALUES (?, ?, ?, ?, datetime('now'), ?)
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
            "UPDATE notes SET modified_at = datetime('now') WHERE id = ?",
            params![note_bytes],
        )?;

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
            SET deleted_at = datetime('now'), modified_at = datetime('now'), device_id = ?
            WHERE id = ? AND deleted_at IS NULL
            "#,
            params![device_id.as_bytes().to_vec(), uuid_bytes],
        )?;

        // Update the parent Note's modified_at to trigger sync
        if updated > 0 {
            if let Some(note_id) = note_bytes {
                self.conn.execute(
                    "UPDATE notes SET modified_at = datetime('now') WHERE id = ?",
                    params![note_id],
                )?;
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
        let created_at: String = row.get(4)?;
        let device_id_bytes: Vec<u8> = row.get(5)?;
        let modified_at: Option<String> = row.get(6)?;
        let deleted_at: Option<String> = row.get(7)?;

        Ok(NoteAttachmentRow {
            id: uuid_bytes_to_hex(&id_bytes).unwrap_or_default(),
            note_id: uuid_bytes_to_hex(&note_id_bytes).unwrap_or_default(),
            attachment_id: uuid_bytes_to_hex(&attachment_id_bytes).unwrap_or_default(),
            attachment_type,
            created_at: parse_sqlite_datetime(&created_at),
            device_id: uuid_bytes_to_hex(&device_id_bytes).unwrap_or_default(),
            modified_at: modified_at.map(|s| parse_sqlite_datetime(&s)),
            deleted_at: deleted_at.map(|s| parse_sqlite_datetime(&s)),
        })
    }

    // ========================================================================
    // AudioFile operations
    // ========================================================================

    /// Create a new audio file record
    pub fn create_audio_file(
        &self,
        filename: &str,
        file_created_at: Option<&str>,
    ) -> VoiceResult<String> {
        let audio_file_id = Uuid::now_v7();
        let uuid_bytes = audio_file_id.as_bytes().to_vec();
        let device_id = get_local_device_id();

        self.conn.execute(
            r#"
            INSERT INTO audio_files (id, imported_at, filename, file_created_at, device_id)
            VALUES (?, datetime('now'), ?, ?, ?)
            "#,
            params![
                uuid_bytes,
                filename,
                file_created_at,
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
            SELECT id, imported_at, filename, file_created_at, summary, device_id, modified_at, deleted_at
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
            SELECT af.id, af.imported_at, af.filename, af.file_created_at, af.summary,
                   af.device_id, af.modified_at, af.deleted_at
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
            SELECT id, imported_at, filename, file_created_at, summary,
                   device_id, modified_at, deleted_at
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
            SET summary = ?, modified_at = datetime('now'), device_id = ?
            WHERE id = ? AND deleted_at IS NULL
            "#,
            params![summary, device_id.as_bytes().to_vec(), uuid_bytes],
        )?;

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
            SET deleted_at = datetime('now'), modified_at = datetime('now'), device_id = ?
            WHERE id = ? AND deleted_at IS NULL
            "#,
            params![device_id.as_bytes().to_vec(), uuid_bytes],
        )?;

        Ok(updated > 0)
    }

    fn row_to_audio_file(&self, row: &Row) -> rusqlite::Result<AudioFileRow> {
        let id_bytes: Vec<u8> = row.get(0)?;
        let imported_at: String = row.get(1)?;
        let filename: String = row.get(2)?;
        let file_created_at: Option<String> = row.get(3)?;
        let summary: Option<String> = row.get(4)?;
        let device_id_bytes: Vec<u8> = row.get(5)?;
        let modified_at: Option<String> = row.get(6)?;
        let deleted_at: Option<String> = row.get(7)?;

        Ok(AudioFileRow {
            id: uuid_bytes_to_hex(&id_bytes).unwrap_or_default(),
            imported_at: parse_sqlite_datetime(&imported_at),
            filename,
            file_created_at: file_created_at.map(|s| parse_sqlite_datetime(&s)),
            summary,
            device_id: uuid_bytes_to_hex(&device_id_bytes).unwrap_or_default(),
            modified_at: modified_at.map(|s| parse_sqlite_datetime(&s)),
            deleted_at: deleted_at.map(|s| parse_sqlite_datetime(&s)),
        })
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
            let created_at: String = row.get(4)?;
            let device_id_bytes: Vec<u8> = row.get(5)?;
            let modified_at: Option<String> = row.get(6)?;
            let deleted_at: Option<String> = row.get(7)?;

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
        created_at: &str,
        modified_at: Option<&str>,
        deleted_at: Option<&str>,
    ) -> VoiceResult<()> {
        let id_uuid = Uuid::parse_str(id)
            .map_err(|e| VoiceError::validation("id", e.to_string()))?;
        let note_uuid = validate_note_id(note_id)?;
        let attachment_uuid = Uuid::parse_str(attachment_id)
            .map_err(|e| VoiceError::validation("attachment_id", e.to_string()))?;
        let device_id = get_local_device_id();

        self.conn.execute(
            r#"
            INSERT INTO note_attachments (id, note_id, attachment_id, attachment_type, created_at, device_id, modified_at, deleted_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                note_id = excluded.note_id,
                attachment_id = excluded.attachment_id,
                attachment_type = excluded.attachment_type,
                modified_at = excluded.modified_at,
                deleted_at = excluded.deleted_at,
                device_id = excluded.device_id
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
            SELECT id, imported_at, filename, file_created_at, summary, device_id, modified_at, deleted_at
            FROM audio_files
            WHERE id = ?
            "#,
        )?;

        let result = stmt.query_row([uuid_bytes], |row| {
            let id_bytes: Vec<u8> = row.get(0)?;
            let imported_at: String = row.get(1)?;
            let filename: String = row.get(2)?;
            let file_created_at: Option<String> = row.get(3)?;
            let summary: Option<String> = row.get(4)?;
            let device_id_bytes: Vec<u8> = row.get(5)?;
            let modified_at: Option<String> = row.get(6)?;
            let deleted_at: Option<String> = row.get(7)?;

            Ok(serde_json::json!({
                "id": uuid_bytes_to_hex(&id_bytes).unwrap_or_default(),
                "imported_at": imported_at,
                "filename": filename,
                "file_created_at": file_created_at,
                "summary": summary,
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

    /// Apply an audio file from sync
    pub fn apply_sync_audio_file(
        &self,
        id: &str,
        imported_at: &str,
        filename: &str,
        file_created_at: Option<&str>,
        summary: Option<&str>,
        modified_at: Option<&str>,
        deleted_at: Option<&str>,
    ) -> VoiceResult<()> {
        let id_uuid = Uuid::parse_str(id)
            .map_err(|e| VoiceError::validation("id", e.to_string()))?;
        let device_id = get_local_device_id();

        self.conn.execute(
            r#"
            INSERT INTO audio_files (id, imported_at, filename, file_created_at, summary, device_id, modified_at, deleted_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                filename = excluded.filename,
                file_created_at = excluded.file_created_at,
                summary = excluded.summary,
                modified_at = excluded.modified_at,
                deleted_at = excluded.deleted_at,
                device_id = excluded.device_id
            "#,
            params![
                id_uuid.as_bytes().to_vec(),
                imported_at,
                filename,
                file_created_at,
                summary,
                device_id.as_bytes().to_vec(),
                modified_at,
                deleted_at,
            ],
        )?;

        Ok(())
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
        assert!(db.get_all_tags().unwrap().is_empty());
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

        // But should still be retrievable by ID
        let note = db.get_note(&note_id).unwrap().unwrap();
        assert!(note.deleted_at.is_some());
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

        let added = db.add_tag_to_note(&note_id, &tag_id).unwrap();
        assert!(added);

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
}
