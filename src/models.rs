//! Data models for Voice.
//!
//! This module defines the core entities: Note, Tag, NoteTag, NoteAttachment, and AudioFile.
//! All IDs are UUID7 stored as 16 bytes internally, converted to hex strings for JSON.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Supported audio file formats for import.
pub const AUDIO_FILE_FORMATS: &[&str] = &["mp3", "wav", "flac", "ogg", "opus", "m4a"];

/// Represents a note in the system.
///
/// Notes contain text content and metadata about creation, modification,
/// and deletion times. All timestamps are accurate to the second.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Note {
    /// Unique identifier for the note (UUID7 as bytes)
    pub id: Uuid,
    /// When the note was created (never NULL)
    pub created_at: DateTime<Utc>,
    /// The note text content
    pub content: String,
    /// UUID7 of the device that last modified this note
    pub device_id: Uuid,
    /// When the note was last modified (None if never modified)
    pub modified_at: Option<DateTime<Utc>>,
    /// When the note was deleted (None if not deleted, soft delete)
    pub deleted_at: Option<DateTime<Utc>>,
}

impl Note {
    /// Create a new note with the given content
    pub fn new(content: String, device_id: Uuid) -> Self {
        Self {
            id: Uuid::now_v7(),
            created_at: Utc::now(),
            content,
            device_id,
            modified_at: None,
            deleted_at: None,
        }
    }

    /// Get the note ID as a hex string
    pub fn id_hex(&self) -> String {
        self.id.simple().to_string()
    }

    /// Get the device ID as a hex string
    pub fn device_id_hex(&self) -> String {
        self.device_id.simple().to_string()
    }

    /// Check if the note is deleted
    pub fn is_deleted(&self) -> bool {
        self.deleted_at.is_some()
    }
}

/// Represents a tag in the hierarchical tag system.
///
/// Tags can have parent-child relationships, forming a tree structure.
/// A tag with parent_id=None is a root-level tag.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tag {
    /// Unique identifier for the tag (UUID7 as bytes)
    pub id: Uuid,
    /// Display name of the tag (must be unique within parent)
    pub name: String,
    /// UUID7 of the device that last modified this tag
    pub device_id: Uuid,
    /// ID of the parent tag (None for root tags)
    pub parent_id: Option<Uuid>,
    /// When the tag was created
    pub created_at: Option<DateTime<Utc>>,
    /// When the tag was last modified (None if never modified)
    pub modified_at: Option<DateTime<Utc>>,
}

impl Tag {
    /// Create a new tag with the given name
    pub fn new(name: String, device_id: Uuid, parent_id: Option<Uuid>) -> Self {
        Self {
            id: Uuid::now_v7(),
            name,
            device_id,
            parent_id,
            created_at: Some(Utc::now()),
            modified_at: None,
        }
    }

    /// Get the tag ID as a hex string
    pub fn id_hex(&self) -> String {
        self.id.simple().to_string()
    }

    /// Get the device ID as a hex string
    pub fn device_id_hex(&self) -> String {
        self.device_id.simple().to_string()
    }

    /// Get the parent ID as a hex string (if present)
    pub fn parent_id_hex(&self) -> Option<String> {
        self.parent_id.map(|id| id.simple().to_string())
    }
}

/// Represents the association between a note and a tag.
///
/// This is used for syncing note-tag relationships.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoteTag {
    /// UUID7 of the note
    pub note_id: Uuid,
    /// UUID7 of the tag
    pub tag_id: Uuid,
    /// When the association was created
    pub created_at: DateTime<Utc>,
    /// UUID7 of the device that created this association
    pub device_id: Uuid,
    /// When the association was modified (for sync tracking)
    pub modified_at: Option<DateTime<Utc>>,
    /// When the association was removed (None if active)
    pub deleted_at: Option<DateTime<Utc>>,
}

impl NoteTag {
    /// Create a new note-tag association
    pub fn new(note_id: Uuid, tag_id: Uuid, device_id: Uuid) -> Self {
        Self {
            note_id,
            tag_id,
            created_at: Utc::now(),
            device_id,
            modified_at: None,
            deleted_at: None,
        }
    }

    /// Get the note ID as a hex string
    pub fn note_id_hex(&self) -> String {
        self.note_id.simple().to_string()
    }

    /// Get the tag ID as a hex string
    pub fn tag_id_hex(&self) -> String {
        self.tag_id.simple().to_string()
    }

    /// Get the device ID as a hex string
    pub fn device_id_hex(&self) -> String {
        self.device_id.simple().to_string()
    }

    /// Check if the association is deleted
    pub fn is_deleted(&self) -> bool {
        self.deleted_at.is_some()
    }
}

/// Types of attachments that can be associated with notes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttachmentType {
    /// Audio file attachment
    AudioFile,
    /// Summary attachment (future)
    Summary,
}

impl AttachmentType {
    /// Convert to database string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            AttachmentType::AudioFile => "audio_file",
            AttachmentType::Summary => "summary",
        }
    }

    /// Parse from database string representation
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "audio_file" => Some(AttachmentType::AudioFile),
            "summary" => Some(AttachmentType::Summary),
            _ => None,
        }
    }
}

impl fmt::Display for AttachmentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Represents the association between a note and an attachment.
///
/// This is a junction table that links notes to their attachments (audio files, summaries, etc.).
/// An attachment can potentially be linked to multiple notes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoteAttachment {
    /// Unique identifier for this association (UUID7)
    pub id: Uuid,
    /// UUID7 of the note
    pub note_id: Uuid,
    /// UUID7 of the attachment (audio_file, summary, etc.)
    pub attachment_id: Uuid,
    /// Type of the attachment
    pub attachment_type: AttachmentType,
    /// When the association was created
    pub created_at: DateTime<Utc>,
    /// UUID7 of the device that created this association
    pub device_id: Uuid,
    /// When the association was modified (for sync tracking)
    pub modified_at: Option<DateTime<Utc>>,
    /// When the association was removed (None if active)
    pub deleted_at: Option<DateTime<Utc>>,
}

impl NoteAttachment {
    /// Create a new note-attachment association
    pub fn new(
        note_id: Uuid,
        attachment_id: Uuid,
        attachment_type: AttachmentType,
        device_id: Uuid,
    ) -> Self {
        Self {
            id: Uuid::now_v7(),
            note_id,
            attachment_id,
            attachment_type,
            created_at: Utc::now(),
            device_id,
            modified_at: None,
            deleted_at: None,
        }
    }

    /// Get the association ID as a hex string
    pub fn id_hex(&self) -> String {
        self.id.simple().to_string()
    }

    /// Get the note ID as a hex string
    pub fn note_id_hex(&self) -> String {
        self.note_id.simple().to_string()
    }

    /// Get the attachment ID as a hex string
    pub fn attachment_id_hex(&self) -> String {
        self.attachment_id.simple().to_string()
    }

    /// Get the device ID as a hex string
    pub fn device_id_hex(&self) -> String {
        self.device_id.simple().to_string()
    }

    /// Check if the association is deleted
    pub fn is_deleted(&self) -> bool {
        self.deleted_at.is_some()
    }
}

/// Represents an audio file entity.
///
/// Audio files are stored on disk and can be attached to notes via NoteAttachment.
/// The actual file is stored at `{audiofile_directory}/{id}.{extension}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AudioFile {
    /// Unique identifier for the audio file (UUID7)
    pub id: Uuid,
    /// When the file was imported into the system
    pub imported_at: DateTime<Utc>,
    /// Original filename from import
    pub filename: String,
    /// When the file was originally created (from filesystem metadata)
    pub file_created_at: Option<DateTime<Utc>>,
    /// Quick text summary of the audio content
    pub summary: Option<String>,
    /// UUID7 of the device that created/last modified this record
    pub device_id: Uuid,
    /// When the record was last modified
    pub modified_at: Option<DateTime<Utc>>,
    /// When the file was soft-deleted (None if active)
    pub deleted_at: Option<DateTime<Utc>>,
}

impl AudioFile {
    /// Create a new audio file record
    pub fn new(filename: String, file_created_at: Option<DateTime<Utc>>, device_id: Uuid) -> Self {
        Self {
            id: Uuid::now_v7(),
            imported_at: Utc::now(),
            filename,
            file_created_at,
            summary: None,
            device_id,
            modified_at: None,
            deleted_at: None,
        }
    }

    /// Get the audio file ID as a hex string
    pub fn id_hex(&self) -> String {
        self.id.simple().to_string()
    }

    /// Get the device ID as a hex string
    pub fn device_id_hex(&self) -> String {
        self.device_id.simple().to_string()
    }

    /// Get the file extension from the filename
    pub fn extension(&self) -> Option<&str> {
        self.filename.rsplit('.').next()
    }

    /// Get the stored filename (uuid.extension)
    pub fn stored_filename(&self) -> String {
        let ext = self.extension().unwrap_or("bin");
        format!("{}.{}", self.id_hex(), ext)
    }

    /// Check if the audio file is deleted
    pub fn is_deleted(&self) -> bool {
        self.deleted_at.is_some()
    }
}

/// Represents a change to be synced between peers.
///
/// This is used by the sync protocol to describe changes (create, update, delete)
/// to any entity type (note, tag, note_tag, audio_file, transcription, etc.).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncChange {
    /// Type of entity being changed (e.g., "note", "tag", "note_tag")
    pub entity_type: String,
    /// ID of the entity being changed
    pub entity_id: String,
    /// Operation type: "create", "update", or "delete"
    pub operation: String,
    /// Full entity data as JSON
    pub data: serde_json::Value,
    /// Timestamp of the change (Unix seconds)
    pub timestamp: i64,
    /// ID of the device that made this change
    pub device_id: String,
    /// Name of the device that made this change
    pub device_name: Option<String>,
}

/// Represents a transcription of an audio file.
///
/// Transcriptions are generated by various transcription services (local Whisper,
/// Google Cloud Speech, AssemblyAI, etc.). An AudioFile can have multiple
/// Transcriptions, allowing for comparison between different services or settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transcription {
    /// Unique identifier for the transcription (UUID7)
    pub id: Uuid,
    /// UUID7 of the audio file this transcription belongs to
    pub audio_file_id: Uuid,
    /// Full transcribed text content
    pub content: String,
    /// JSON string containing segment-level transcription data (timestamps, speakers, etc.)
    pub content_segments: Option<String>,
    /// Name of the transcription service used (e.g., "whisper", "google", "assemblyai")
    pub service: String,
    /// JSON string containing arguments passed to the service (language, speaker_count, model, etc.)
    pub service_arguments: Option<String>,
    /// JSON string containing response metadata from the service (duration, confidence, etc.)
    pub service_response: Option<String>,
    /// UUID7 of the device that created this transcription
    pub device_id: Uuid,
    /// When the transcription was created
    pub created_at: DateTime<Utc>,
    /// When the transcription was last modified
    pub modified_at: Option<DateTime<Utc>>,
    /// When the transcription was soft-deleted (None if active)
    pub deleted_at: Option<DateTime<Utc>>,
}

impl Transcription {
    /// Create a new transcription record
    pub fn new(
        audio_file_id: Uuid,
        content: String,
        service: String,
        device_id: Uuid,
    ) -> Self {
        Self {
            id: Uuid::now_v7(),
            audio_file_id,
            content,
            content_segments: None,
            service,
            service_arguments: None,
            service_response: None,
            device_id,
            created_at: Utc::now(),
            modified_at: None,
            deleted_at: None,
        }
    }

    /// Create a new transcription record with all optional fields
    pub fn new_full(
        audio_file_id: Uuid,
        content: String,
        content_segments: Option<String>,
        service: String,
        service_arguments: Option<String>,
        service_response: Option<String>,
        device_id: Uuid,
    ) -> Self {
        Self {
            id: Uuid::now_v7(),
            audio_file_id,
            content,
            content_segments,
            service,
            service_arguments,
            service_response,
            device_id,
            created_at: Utc::now(),
            modified_at: None,
            deleted_at: None,
        }
    }

    /// Get the transcription ID as a hex string
    pub fn id_hex(&self) -> String {
        self.id.simple().to_string()
    }

    /// Get the audio file ID as a hex string
    pub fn audio_file_id_hex(&self) -> String {
        self.audio_file_id.simple().to_string()
    }

    /// Get the device ID as a hex string
    pub fn device_id_hex(&self) -> String {
        self.device_id.simple().to_string()
    }

    /// Check if the transcription is deleted
    pub fn is_deleted(&self) -> bool {
        self.deleted_at.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_note_creation() {
        let device_id = Uuid::now_v7();
        let note = Note::new("Test content".to_string(), device_id);

        assert!(!note.id.is_nil());
        assert_eq!(note.content, "Test content");
        assert_eq!(note.device_id, device_id);
        assert!(note.modified_at.is_none());
        assert!(note.deleted_at.is_none());
        assert!(!note.is_deleted());
    }

    #[test]
    fn test_tag_creation() {
        let device_id = Uuid::now_v7();
        let tag = Tag::new("Work".to_string(), device_id, None);

        assert!(!tag.id.is_nil());
        assert_eq!(tag.name, "Work");
        assert!(tag.parent_id.is_none());
    }

    #[test]
    fn test_tag_with_parent() {
        let device_id = Uuid::now_v7();
        let parent = Tag::new("Work".to_string(), device_id, None);
        let child = Tag::new("Projects".to_string(), device_id, Some(parent.id));

        assert_eq!(child.parent_id, Some(parent.id));
    }

    #[test]
    fn test_note_tag_creation() {
        let device_id = Uuid::now_v7();
        let note = Note::new("Test".to_string(), device_id);
        let tag = Tag::new("Work".to_string(), device_id, None);
        let note_tag = NoteTag::new(note.id, tag.id, device_id);

        assert_eq!(note_tag.note_id, note.id);
        assert_eq!(note_tag.tag_id, tag.id);
        assert!(!note_tag.is_deleted());
    }

    #[test]
    fn test_id_hex_format() {
        let device_id = Uuid::now_v7();
        let note = Note::new("Test".to_string(), device_id);

        let hex = note.id_hex();
        assert_eq!(hex.len(), 32); // UUID without hyphens
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_attachment_type_conversion() {
        assert_eq!(AttachmentType::AudioFile.as_str(), "audio_file");
        assert_eq!(AttachmentType::Summary.as_str(), "summary");

        assert_eq!(
            AttachmentType::from_str("audio_file"),
            Some(AttachmentType::AudioFile)
        );
        assert_eq!(
            AttachmentType::from_str("summary"),
            Some(AttachmentType::Summary)
        );
        assert_eq!(AttachmentType::from_str("unknown"), None);
    }

    #[test]
    fn test_note_attachment_creation() {
        let device_id = Uuid::now_v7();
        let note_id = Uuid::now_v7();
        let attachment_id = Uuid::now_v7();

        let note_attachment = NoteAttachment::new(
            note_id,
            attachment_id,
            AttachmentType::AudioFile,
            device_id,
        );

        assert!(!note_attachment.id.is_nil());
        assert_eq!(note_attachment.note_id, note_id);
        assert_eq!(note_attachment.attachment_id, attachment_id);
        assert_eq!(note_attachment.attachment_type, AttachmentType::AudioFile);
        assert!(note_attachment.modified_at.is_none());
        assert!(note_attachment.deleted_at.is_none());
        assert!(!note_attachment.is_deleted());
    }

    #[test]
    fn test_audio_file_creation() {
        let device_id = Uuid::now_v7();
        let audio_file = AudioFile::new(
            "recording.mp3".to_string(),
            None,
            device_id,
        );

        assert!(!audio_file.id.is_nil());
        assert_eq!(audio_file.filename, "recording.mp3");
        assert!(audio_file.file_created_at.is_none());
        assert!(audio_file.summary.is_none());
        assert!(audio_file.modified_at.is_none());
        assert!(audio_file.deleted_at.is_none());
        assert!(!audio_file.is_deleted());
    }

    #[test]
    fn test_audio_file_extension() {
        let device_id = Uuid::now_v7();

        let mp3 = AudioFile::new("test.mp3".to_string(), None, device_id);
        assert_eq!(mp3.extension(), Some("mp3"));

        let flac = AudioFile::new("my.recording.flac".to_string(), None, device_id);
        assert_eq!(flac.extension(), Some("flac"));
    }

    #[test]
    fn test_audio_file_stored_filename() {
        let device_id = Uuid::now_v7();
        let audio_file = AudioFile::new("original.wav".to_string(), None, device_id);

        let stored = audio_file.stored_filename();
        assert!(stored.ends_with(".wav"));
        assert_eq!(stored.len(), 32 + 1 + 3); // uuid + dot + ext
    }

    #[test]
    fn test_audio_file_formats_constant() {
        assert!(AUDIO_FILE_FORMATS.contains(&"mp3"));
        assert!(AUDIO_FILE_FORMATS.contains(&"wav"));
        assert!(AUDIO_FILE_FORMATS.contains(&"flac"));
        assert!(AUDIO_FILE_FORMATS.contains(&"ogg"));
        assert!(AUDIO_FILE_FORMATS.contains(&"opus"));
        assert!(AUDIO_FILE_FORMATS.contains(&"m4a"));
        assert!(!AUDIO_FILE_FORMATS.contains(&"txt"));
    }

    #[test]
    fn test_transcription_creation() {
        let device_id = Uuid::now_v7();
        let audio_file_id = Uuid::now_v7();
        let transcription = Transcription::new(
            audio_file_id,
            "Hello world".to_string(),
            "whisper".to_string(),
            device_id,
        );

        assert!(!transcription.id.is_nil());
        assert_eq!(transcription.audio_file_id, audio_file_id);
        assert_eq!(transcription.content, "Hello world");
        assert!(transcription.content_segments.is_none());
        assert_eq!(transcription.service, "whisper");
        assert!(transcription.service_arguments.is_none());
        assert!(transcription.service_response.is_none());
        assert_eq!(transcription.device_id, device_id);
        assert!(transcription.modified_at.is_none());
        assert!(transcription.deleted_at.is_none());
        assert!(!transcription.is_deleted());
    }

    #[test]
    fn test_transcription_creation_full() {
        let device_id = Uuid::now_v7();
        let audio_file_id = Uuid::now_v7();
        let segments = r#"[{"text":"Hello","start":0.0,"end":1.0}]"#.to_string();
        let args = r#"{"language":"en","speaker_count":1}"#.to_string();
        let response = r#"{"duration":5.0,"confidence":0.95}"#.to_string();

        let transcription = Transcription::new_full(
            audio_file_id,
            "Hello world".to_string(),
            Some(segments.clone()),
            "google".to_string(),
            Some(args.clone()),
            Some(response.clone()),
            device_id,
        );

        assert_eq!(transcription.content_segments, Some(segments));
        assert_eq!(transcription.service, "google");
        assert_eq!(transcription.service_arguments, Some(args));
        assert_eq!(transcription.service_response, Some(response));
    }

    #[test]
    fn test_transcription_id_hex() {
        let device_id = Uuid::now_v7();
        let audio_file_id = Uuid::now_v7();
        let transcription = Transcription::new(
            audio_file_id,
            "Test".to_string(),
            "whisper".to_string(),
            device_id,
        );

        let hex = transcription.id_hex();
        assert_eq!(hex.len(), 32);
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));

        let audio_hex = transcription.audio_file_id_hex();
        assert_eq!(audio_hex.len(), 32);
        assert!(audio_hex.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
