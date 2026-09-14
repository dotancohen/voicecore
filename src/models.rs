//! Data models for Voice.
//!
//! This module defines the core entities: Note, Tag, NoteTag, NoteAttachment, and AudioFile.
//! All IDs are UUID7 stored as 16 bytes internally, converted to hex strings for JSON.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Supported audio file formats for import.
/// Every common audio format a recording may be imported in, by extension
/// (the owner's decision, 2026-09-13: support all common formats; a file whose
/// format is not here is skipped). The one list: the desktop and the phone read
/// it through their bindings. What each device can play is its player's
/// business; which formats it cannot play is reported, not guessed here.
pub const AUDIO_FILE_FORMATS: &[&str] = &[
    "3g2", "3ga", "3gp", "aac", "ac3", "aif", "aifc", "aiff", "amr", "ape", "au", "awb", "caf", "flac", "gsm",
    "m4a", "m4b", "mka", "mp2", "mp3", "mp4", "mpga", "oga", "ogg", "opus", "qcp", "snd", "spx", "wav", "wave",
    "weba", "webm", "wma", "wv",
];

/// Extension used on disk and in cloud storage when a filename has no extension.
pub const AUDIO_FILE_DEFAULT_EXTENSION: &str = "bin";

/// Normalised extension for an audio file's original filename.
///
/// Every code path that derives an on-disk or cloud object name from the
/// original filename MUST use this function so that all platforms agree.
/// The extension is lowercased because the Python and Android importers write
/// the local file with a lowercase extension regardless of the original case.
/// A filename without an extension maps to [`AUDIO_FILE_DEFAULT_EXTENSION`].
pub fn audio_file_extension(filename: &str) -> String {
    match filename.rsplit_once('.') {
        Some((stem, ext)) if !stem.is_empty() && !ext.is_empty() && !ext.contains('/') => {
            ext.to_ascii_lowercase()
        }
        _ => AUDIO_FILE_DEFAULT_EXTENSION.to_string(),
    }
}

/// How many characters of the id end a recording's file name: the random
/// tail of a UUID7, since its head is the clock and repeats within a minute.
pub const RECORDING_ID_TAIL: usize = 8;

/// The name of a recording's file on disk (Stage 13), a person can read:
/// `2026_09_21_14_30_59-abcdefgh.ogg`, the recording's start (its
/// `file_created_at`, at the offset it was recorded in; the import time
/// when the file carries no date), a hyphen, the last eight characters of
/// its id, and the extension of the original name. Written once to
/// `audio_files.disk_name`, which is local and the only way a file is
/// found.
pub fn recording_file_name(audio_id: &str, filename: &str, moment: i64, offset_seconds: Option<i32>) -> String {
    use chrono::{FixedOffset, Local, TimeZone};
    let offset = offset_seconds.and_then(FixedOffset::east_opt).unwrap_or_else(|| *Local::now().offset());
    let when = offset.timestamp_opt(moment, 0).single().unwrap_or_else(|| offset.timestamp_opt(0, 0).unwrap());
    let tail_start = audio_id.len().saturating_sub(RECORDING_ID_TAIL);
    format!("{}-{}.{}", when.format("%Y_%m_%d_%H_%M_%S"), &audio_id[tail_start..], audio_file_extension(filename))
}

/// Whether this application recorded a file or took it from elsewhere
/// (FILE-15): a recording is named by its start and the tail of its id, an
/// imported file keeps its own name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileOrigin {
    Recorded,
    Imported,
}

impl FileOrigin {
    /// The word a recording's `origin_kind` holds (FILE-25).
    pub fn as_str(&self) -> &'static str {
        match self {
            FileOrigin::Recorded => crate::database::ORIGIN_RECORDED,
            FileOrigin::Imported => crate::database::ORIGIN_IMPORTED,
        }
    }
}

/// Whether a name can be a file's name in the audio folder: any POSIX name,
/// which is anything but empty, `.`, `..`, or a name holding `/` or NUL.
pub fn valid_file_name(name: &str) -> bool {
    !name.is_empty() && name != "." && name != ".." && !name.contains('/') && !name.contains('\0')
}

/// A file name split at its extension: `("a.tar", Some("gz"))`, `("memo", None)`,
/// `(".hidden", None)`.
pub fn split_file_name(name: &str) -> (&str, Option<&str>) {
    match name.rsplit_once('.') {
        Some((stem, extension)) if !stem.is_empty() => (stem, Some(extension)),
        _ => (name, None),
    }
}

/// A colliding name with a recording's suffix (FILE-15): a hyphen and the
/// last eight characters of its id before the extension, or the whole id.
pub fn suffixed_name(name: &str, audio_id: &str, whole_id: bool) -> String {
    let tail = if whole_id { audio_id } else { &audio_id[audio_id.len().saturating_sub(RECORDING_ID_TAIL)..] };
    match split_file_name(name) {
        (stem, Some(extension)) => format!("{}-{}.{}", stem, tail, extension),
        (stem, None) => format!("{}-{}", stem, tail),
    }
}

/// Full local path of a recording: the audio directory and the row's
/// `disk_name`.
pub fn audio_local_path(audiofile_directory: &std::path::Path, disk_name: &str) -> std::path::PathBuf {
    audiofile_directory.join(disk_name)
}

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
/// The file is stored at `{audiofile_directory}/{disk_name}`, the name the row stores (FILE-15).
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

    /// Get the normalised (lowercase) file extension from the filename
    pub fn extension(&self) -> String {
        audio_file_extension(&self.filename)
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
        assert_eq!(mp3.extension(), "mp3");

        let flac = AudioFile::new("my.recording.flac".to_string(), None, device_id);
        assert_eq!(flac.extension(), "flac");

        let upper = AudioFile::new("REC.MP3".to_string(), None, device_id);
        assert_eq!(upper.extension(), "mp3");
    }

    #[test]
    fn test_audio_file_extension_helper() {
        assert_eq!(audio_file_extension("a.mp3"), "mp3");
        assert_eq!(audio_file_extension("A.MP3"), "mp3");
        assert_eq!(audio_file_extension("my.recording.OGG"), "ogg");
        assert_eq!(audio_file_extension("הקלטה.M4A"), "m4a");
        assert_eq!(audio_file_extension("noextension"), "bin");
        assert_eq!(audio_file_extension("trailingdot."), "bin");
        assert_eq!(audio_file_extension(".hidden"), "bin");
        assert_eq!(audio_file_extension(""), "bin");
    }

    /// FILE-15: a suffix goes before the extension and names no sequence; any POSIX name is a name.
    #[test]
    fn a_suffix_is_the_tail_of_the_id_before_the_extension() {
        let id = "0199aaaabbbbccccdddd0000ffff1234";
        assert_eq!(suffixed_name("הבית שלי.jpg", id, false), "הבית שלי-ffff1234.jpg");
        assert_eq!(suffixed_name("a.tar.gz", id, false), "a.tar-ffff1234.gz");
        assert_eq!(suffixed_name("בלי סיומת", id, false), "בלי סיומת-ffff1234");
        assert_eq!(suffixed_name(".hidden", id, true), format!(".hidden-{}", id));
        assert!(valid_file_name("שם עם רווחים.ogg") && valid_file_name(".hidden") && valid_file_name("a\nb") && valid_file_name("no extension"));
        assert!(!valid_file_name("") && !valid_file_name(".") && !valid_file_name("..") && !valid_file_name("a/b") && !valid_file_name("a\0b"));
    }

    #[test]
    fn a_recording_is_named_by_its_start_at_its_own_offset_and_the_tail_of_its_id() {
        // 2026-09-21 14:30:59 in a zone three hours east: 11:30:59 UTC
        let name = recording_file_name("0199aaaaaaaa7000800000000abcdefgh", "Voice Memo.WAV", 1789990259, Some(3 * 3600));
        assert_eq!(name, "2026_09_21_14_30_59-abcdefgh.wav");
        let p = audio_local_path(std::path::Path::new("/tmp/audio"), &name);
        assert_eq!(p, std::path::PathBuf::from("/tmp/audio/2026_09_21_14_30_59-abcdefgh.wav"));
        assert!(recording_file_name("short", "x", 0, Some(0)).starts_with("1970_01_01_00_00_00-short."));
    }

    #[test]
    fn test_audio_file_formats_constant() {
        assert!(AUDIO_FILE_FORMATS.contains(&"mp3"));
        assert!(AUDIO_FILE_FORMATS.contains(&"wav"));
        assert!(AUDIO_FILE_FORMATS.contains(&"flac"));
        assert!(AUDIO_FILE_FORMATS.contains(&"ogg"));
        assert!(AUDIO_FILE_FORMATS.contains(&"opus"));
        assert!(AUDIO_FILE_FORMATS.contains(&"m4a"));
        for common in ["3gp", "amr", "aac", "wma", "aiff", "awb", "webm", "mp4"] {
            assert!(AUDIO_FILE_FORMATS.contains(&common), "{}", common);
        }
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
