//! What the user should know about (ISSUE-1): computed from the database at
//! every look and never stored, so an issue that is dealt with is gone the
//! next time the list is read.
//!
//! - Recordings that are not in the account's bucket, and why: no bucket, over
//!   the account's upload limit, waiting for a device that holds the file to
//!   upload it, or no copy known anywhere.
//! - Orphaned rows: a transcription whose recording row is not there, an
//!   attachment whose note or recording row is not there, and a recording no
//!   note holds (a note in the trash still holds its recordings).
//! - Tags whose names contain whitespace.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::database::{Database, PLACE_CLOUD};
use crate::error::VoiceResult;

/// Why a recording is not in the bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NotInCloudReason {
    /// The account has no bucket configured
    NoBucket,
    /// The file is larger than the account's upload limit
    TooLarge,
    /// A device holds the file and has not uploaded it yet
    WaitingForUpload,
    /// No device and no bucket is known to hold the file
    NoCopyKnown,
    /// No place is known to hold the file, this device imported it, and its file
    /// is not in the audio folder: the import made the row and the file is gone
    ImportedHereFileMissing,
    /// The same for a recording this device's recorder made
    RecordedHereFileMissing,
}

impl NotInCloudReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            NotInCloudReason::NoBucket => "no_bucket",
            NotInCloudReason::TooLarge => "too_large",
            NotInCloudReason::WaitingForUpload => "waiting_for_upload",
            NotInCloudReason::NoCopyKnown => "no_copy_known",
            NotInCloudReason::ImportedHereFileMissing => "imported_here_file_missing",
            NotInCloudReason::RecordedHereFileMissing => "recorded_here_file_missing",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordingNotInCloud {
    pub audio_id: String,
    pub filename: String,
    /// Bytes, when a device has measured the file
    pub size_bytes: Option<i64>,
    pub reason: NotInCloudReason,
    /// The devices that hold the file, as last stated
    pub held_by: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrphanedTranscription {
    pub transcription_id: String,
    /// The recording row that is not there
    pub audio_file_id: String,
    /// The first eighty characters of the transcription
    pub content_start: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrphanedAttachment {
    pub attachment_id: String,
    pub note_id: String,
    pub target_id: String,
    pub attachment_type: String,
    pub note_missing: bool,
    pub target_missing: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrphanedRecording {
    pub audio_id: String,
    pub filename: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TagWithWhitespace {
    pub tag_id: String,
    pub name: String,
    /// The tag's full path, parents first
    pub path: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Issues {
    pub recordings_not_in_cloud: Vec<RecordingNotInCloud>,
    /// The account's upload limit the reasons were judged by
    pub max_upload_bytes: u64,
    pub orphaned_transcriptions: Vec<OrphanedTranscription>,
    pub orphaned_attachments: Vec<OrphanedAttachment>,
    pub orphaned_recordings: Vec<OrphanedRecording>,
    pub tags_with_whitespace: Vec<TagWithWhitespace>,
}

impl Issues {
    /// How many issues there are, every kind together.
    pub fn count(&self) -> usize {
        self.recordings_not_in_cloud.len()
            + self.orphaned_transcriptions.len()
            + self.orphaned_attachments.len()
            + self.orphaned_recordings.len()
            + self.tags_with_whitespace.len()
    }
}

/// Every issue, now. With an audio folder, this device's copies are compared
/// with the folder first (FILE-22), so "waiting for upload" names the devices
/// that really hold each file; `here` is this device's id.
pub fn issues(db: &Database, audio_dir: Option<&Path>, here: &str) -> VoiceResult<Issues> {
    if let Some(dir) = audio_dir {
        db.check_files_here(dir, here)?;
    }
    let conn = db.connection();
    let bucket = db
        .get_file_storage_config()?
        .and_then(|saved| saved.get("provider").and_then(|p| p.as_str()).map(|p| p != "none"))
        .unwrap_or(false);
    let max_upload_bytes = db.max_upload_bytes()?;

    // Recordings not in the bucket: the bucket's own statement decides
    let mut stmt = conn.prepare(
        "SELECT lower(hex(a.id)), a.filename, a.size_bytes FROM audio_files a
         WHERE a.deleted_at IS NULL
           AND NOT EXISTS (SELECT 1 FROM file_locations l WHERE l.audio_id = a.id AND l.place = ?1 AND l.present = 1)
         ORDER BY a.imported_at, a.id",
    )?;
    let rows: Vec<(String, String, Option<i64>)> = stmt
        .query_map([PLACE_CLOUD], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
        .collect::<Result<Vec<_>, _>>()?;
    drop(stmt);
    let mut recordings_not_in_cloud = Vec::new();
    for (audio_id, filename, size_bytes) in rows {
        let held_by: Vec<String> = db.places_holding(&audio_id)?.into_iter().filter(|p| p != PLACE_CLOUD).collect();
        let reason = if !bucket {
            NotInCloudReason::NoBucket
        } else if size_bytes.is_some_and(|n| n as u64 > max_upload_bytes) {
            NotInCloudReason::TooLarge
        } else if !held_by.is_empty() {
            NotInCloudReason::WaitingForUpload
        } else if let Some(kind) = match audio_dir {
            Some(dir) => db.made_here_but_missing(&audio_id, dir, here)?,
            None => None,
        } {
            if kind == crate::database::ORIGIN_RECORDED { NotInCloudReason::RecordedHereFileMissing } else { NotInCloudReason::ImportedHereFileMissing }
        } else {
            NotInCloudReason::NoCopyKnown
        };
        recordings_not_in_cloud.push(RecordingNotInCloud { audio_id, filename, size_bytes, reason, held_by });
    }

    let mut stmt = conn.prepare(
        "SELECT lower(hex(t.id)), lower(hex(t.audio_file_id)), substr(t.content, 1, 80) FROM transcriptions t
         WHERE t.deleted_at IS NULL AND NOT EXISTS (SELECT 1 FROM audio_files a WHERE a.id = t.audio_file_id)
         ORDER BY t.created_at, t.id",
    )?;
    let orphaned_transcriptions = stmt
        .query_map([], |r| Ok(OrphanedTranscription { transcription_id: r.get(0)?, audio_file_id: r.get(1)?, content_start: r.get(2)? }))?
        .collect::<Result<Vec<_>, _>>()?;
    drop(stmt);

    let mut stmt = conn.prepare(
        "SELECT lower(hex(na.id)), lower(hex(na.note_id)), lower(hex(na.attachment_id)), na.attachment_type,
                NOT EXISTS (SELECT 1 FROM notes n WHERE n.id = na.note_id),
                na.attachment_type = 'audio_file' AND NOT EXISTS (SELECT 1 FROM audio_files a WHERE a.id = na.attachment_id)
         FROM note_attachments na
         WHERE na.deleted_at IS NULL
           AND (NOT EXISTS (SELECT 1 FROM notes n WHERE n.id = na.note_id)
                OR (na.attachment_type = 'audio_file' AND NOT EXISTS (SELECT 1 FROM audio_files a WHERE a.id = na.attachment_id)))
         ORDER BY na.created_at, na.id",
    )?;
    let orphaned_attachments = stmt
        .query_map([], |r| {
            Ok(OrphanedAttachment {
                attachment_id: r.get(0)?,
                note_id: r.get(1)?,
                target_id: r.get(2)?,
                attachment_type: r.get(3)?,
                note_missing: r.get(4)?,
                target_missing: r.get(5)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    drop(stmt);

    let mut stmt = conn.prepare(
        "SELECT lower(hex(a.id)), a.filename FROM audio_files a
         WHERE a.deleted_at IS NULL
           AND NOT EXISTS (
               SELECT 1 FROM note_attachments na JOIN notes n ON n.id = na.note_id
               WHERE na.attachment_id = a.id AND (na.deleted_at IS NULL OR n.deleted_at IS NOT NULL))
         ORDER BY a.imported_at, a.id",
    )?;
    let orphaned_recordings = stmt
        .query_map([], |r| Ok(OrphanedRecording { audio_id: r.get(0)?, filename: r.get(1)? }))?
        .collect::<Result<Vec<_>, _>>()?;
    drop(stmt);

    let mut stmt = conn.prepare("SELECT lower(hex(id)), name FROM tags WHERE deleted_at IS NULL ORDER BY name, id")?;
    let tags: Vec<(String, String)> = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?.collect::<Result<Vec<_>, _>>()?;
    drop(stmt);
    let mut tags_with_whitespace = Vec::new();
    for (tag_id, name) in tags {
        if name.chars().any(char::is_whitespace) {
            let path = crate::search::get_tag_full_path(db, &tag_id)?;
            tags_with_whitespace.push(TagWithWhitespace { tag_id, name, path });
        }
    }

    Ok(Issues { recordings_not_in_cloud, max_upload_bytes, orphaned_transcriptions, orphaned_attachments, orphaned_recordings, tags_with_whitespace })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{FileOrigin, SyncChange};

    const HERE: &str = "01a09526bbbb70808f15a84d31aaa8d2";
    const PHONE: &str = "01a0952602bc70808f15a84d31aaa8d2";
    const PEER: &str = "01a09526cccc70808f15a84d31aaa8d2";

    fn open() -> (Database, tempfile::TempDir) {
        let temp = tempfile::TempDir::new().unwrap();
        (Database::new(temp.path().join("notes.db")).unwrap(), temp)
    }

    fn with_bucket(db: &Database) {
        db.set_file_storage_config("s3", Some(&serde_json::json!({"bucket": "voice-abc", "region": "eu-central-1", "access_key_id": "k", "secret_access_key": "s"}))).unwrap();
    }

    /// A change as a peer's sync delivers it.
    fn from_peer(db: &Database, entity_type: &str, entity_id: &str, data: serde_json::Value) {
        let change = SyncChange { entity_type: entity_type.to_string(), entity_id: entity_id.to_string(), operation: "create".to_string(), data, timestamp: 1_735_689_600, device_id: PEER.to_string(), device_name: None };
        let outcome = crate::sync_apply::apply_changes(db, &[change], PEER, None, 1_735_689_700).unwrap();
        assert!(outcome.errors.is_empty(), "{:?}", outcome.errors);
    }

    fn reasons(issues: &Issues) -> Vec<(String, NotInCloudReason)> {
        issues.recordings_not_in_cloud.iter().map(|r| (r.filename.clone(), r.reason)).collect()
    }

    /// Q1 of 2026-09-14: a recording this device imported, no place known to
    /// hold it, and its file not in the folder is told apart from a recording
    /// nobody is known to hold; a peer's recording is not.
    #[test]
    fn a_recording_imported_here_whose_file_is_gone_has_its_own_reason() {
        let (db, temp) = open();
        let dir = temp.path().join("audio");
        std::fs::create_dir_all(&dir).unwrap();
        with_bucket(&db);
        let here = crate::database::get_local_device_id().simple().to_string();
        let note = db.create_note("").unwrap();
        let gone = db.create_audio_file("נעלם.mp3", None, None, FileOrigin::Imported, Some(&dir)).unwrap();
        db.attach_to_note(&note, &gone, "audio_file").unwrap();
        let elsewhere = uuid::Uuid::now_v7().simple().to_string();
        from_peer(&db, "audio_file", &elsewhere, serde_json::json!({"imported_at": 1_735_689_600, "filename": "אצל מישהו.3gp", "disk_name": "אצל מישהו.3gp", "modified_at": 1_735_689_600}));

        let found = issues(&db, Some(&dir), &here).unwrap();
        let reason_of = |id: &str| found.recordings_not_in_cloud.iter().find(|r| r.audio_id == id).map(|r| r.reason);
        assert_eq!(reason_of(&gone), Some(NotInCloudReason::ImportedHereFileMissing));
        assert_eq!(reason_of(&elsewhere), Some(NotInCloudReason::NoCopyKnown));
        assert_eq!(NotInCloudReason::ImportedHereFileMissing.as_str(), "imported_here_file_missing");

        // Put back, it is this device's to upload
        std::fs::write(dir.join(db.get_audio_file(&gone).unwrap().unwrap().disk_name), b"audio").unwrap();
        let back = issues(&db, Some(&dir), &here).unwrap();
        assert_eq!(back.recordings_not_in_cloud.iter().find(|r| r.audio_id == gone).map(|r| r.reason), Some(NotInCloudReason::WaitingForUpload));
    }

    /// ISSUE-1: without a bucket every recording is "no bucket"; with one,
    /// each gets the reason that is true of it, and an uploaded or deleted
    /// recording is not listed.
    #[test]
    fn each_recording_not_in_the_bucket_has_the_reason_that_is_true_of_it() {
        let (db, temp) = open();
        let dir = temp.path().join("audio");
        std::fs::create_dir_all(&dir).unwrap();
        let recording = |name: &str, size: usize| {
            let note = db.create_note("").unwrap();
            let id = db.create_audio_file(name, None, None, FileOrigin::Imported, Some(&dir)).unwrap();
            db.attach_to_note(&note, &id, "audio_file").unwrap();
            std::fs::write(dir.join(db.get_audio_file(&id).unwrap().unwrap().disk_name), vec![3u8; size]).unwrap();
            db.store_content_hash(&id, &dir).unwrap();
            id
        };
        let big = recording("הרצאה ארוכה.wav", 2 * 1024 * 1024);
        let small = recording("פתק קולי.m4a", 1000);
        let uploaded = recording("בענן.ogg", 1000);

        let before = issues(&db, Some(&dir), HERE).unwrap();
        assert_eq!(before.recordings_not_in_cloud.len(), 3);
        assert!(before.recordings_not_in_cloud.iter().all(|r| r.reason == NotInCloudReason::NoBucket));

        with_bucket(&db);
        db.set_max_upload_mb(1).unwrap();
        db.update_audio_file_storage(&uploaded, "s3", "k.ogg", false).unwrap();
        // A recording known only by its row: a device that holds it said nothing
        let elsewhere = uuid::Uuid::now_v7().simple().to_string();
        from_peer(&db, "audio_file", &elsewhere, serde_json::json!({"imported_at": 1_735_689_600, "filename": "אצל מישהו.3gp", "disk_name": "אצל מישהו.3gp", "modified_at": 1_735_689_600}));
        // A deleted recording is not an issue
        let gone = recording("נמחק.ogg", 10);
        db.delete_audio_file(&gone).unwrap();

        let now = issues(&db, Some(&dir), HERE).unwrap();
        assert_eq!(now.max_upload_bytes, 1024 * 1024);
        // In import order: the peer's recording was imported in 2025, before these
        assert_eq!(reasons(&now), vec![
            ("אצל מישהו.3gp".to_string(), NotInCloudReason::NoCopyKnown),
            ("הרצאה ארוכה.wav".to_string(), NotInCloudReason::TooLarge),
            ("פתק קולי.m4a".to_string(), NotInCloudReason::WaitingForUpload),
        ]);
        let waiting = now.recordings_not_in_cloud.iter().find(|r| r.audio_id == small).unwrap();
        assert_eq!(waiting.held_by, vec![HERE.to_string()], "the folder was compared first");
        assert_eq!(waiting.size_bytes, Some(1000));
        assert!(now.recordings_not_in_cloud.iter().all(|r| r.audio_id != big || r.size_bytes == Some(2 * 1024 * 1024)));

        // The bucket losing an object makes the recording an issue again
        db.set_file_location(&uploaded, PLACE_CLOUD, false).unwrap();
        assert!(issues(&db, Some(&dir), HERE).unwrap().recordings_not_in_cloud.iter().any(|r| r.audio_id == uploaded));
    }

    /// ISSUE-1: a transcription whose recording is not there, and an
    /// attachment whose note or recording is not there, are orphaned.
    ///
    /// The core's SQLite enforces foreign keys, so today such a row is refused
    /// and waits for its parent. A database written with enforcement off
    /// holds them: Python's sqlite3 module and the sqlite3 tool leave foreign
    /// keys off, and the desktop application wrote its database through
    /// Python before the core existed. This database is opened the way those
    /// writers opened it, and the rows arrive through the same sync code.
    #[test]
    fn rows_whose_parent_is_not_there_are_orphaned() {
        let (db, _temp) = open();
        db.connection().execute_batch("PRAGMA foreign_keys = OFF").unwrap();
        let missing_recording = uuid::Uuid::now_v7().simple().to_string();
        let transcription = uuid::Uuid::now_v7().simple().to_string();
        from_peer(&db, "transcription", &transcription, serde_json::json!({
            "audio_file_id": missing_recording, "content": "שלום, זה תמלול בלי הקלטה", "service": "whisper",
            "device_id": PEER, "created_at": 1_735_689_600,
        }));
        let note = db.create_note("פתק").unwrap();
        let attachment_without_recording = uuid::Uuid::now_v7().simple().to_string();
        from_peer(&db, "note_attachment", &attachment_without_recording, serde_json::json!({
            "note_id": note, "attachment_id": missing_recording, "attachment_type": "audio_file", "created_at": 1_735_689_600,
        }));
        let recording = db.create_audio_file("קיים.ogg", None, None, FileOrigin::Imported, None).unwrap();
        let missing_note = uuid::Uuid::now_v7().simple().to_string();
        let attachment_without_note = uuid::Uuid::now_v7().simple().to_string();
        from_peer(&db, "note_attachment", &attachment_without_note, serde_json::json!({
            "note_id": missing_note, "attachment_id": recording, "attachment_type": "audio_file", "created_at": 1_735_689_601,
        }));

        db.connection().execute_batch("PRAGMA foreign_keys = ON").unwrap();
        let found = issues(&db, None, HERE).unwrap();
        assert_eq!(found.orphaned_transcriptions.len(), 1);
        assert_eq!(found.orphaned_transcriptions[0].transcription_id, transcription);
        assert_eq!(found.orphaned_transcriptions[0].audio_file_id, missing_recording);
        assert!(found.orphaned_transcriptions[0].content_start.starts_with("שלום"));
        let by_id: std::collections::HashMap<_, _> = found.orphaned_attachments.iter().map(|a| (a.attachment_id.clone(), a)).collect();
        assert_eq!(by_id.len(), 2);
        assert!(by_id[&attachment_without_recording].target_missing && !by_id[&attachment_without_recording].note_missing);
        assert!(by_id[&attachment_without_note].note_missing && !by_id[&attachment_without_note].target_missing);
        assert_eq!(found.orphaned_recordings.iter().map(|r| r.audio_id.clone()).collect::<Vec<_>>(), vec![recording.clone()], "held only by a note that is not there");

        // When the missing recording arrives, its transcription and attachment are orphans no more
        from_peer(&db, "audio_file", &missing_recording, serde_json::json!({"imported_at": 1_735_689_600, "filename": "הגיע.ogg", "disk_name": "הגיע.ogg", "modified_at": 1_735_689_600}));
        let later = issues(&db, None, HERE).unwrap();
        assert!(later.orphaned_transcriptions.is_empty());
        assert_eq!(later.orphaned_attachments.iter().map(|a| a.attachment_id.clone()).collect::<Vec<_>>(), vec![attachment_without_note]);
    }

    /// ISSUE-1: a recording no note holds is an issue; a recording of a note
    /// in the trash is not, because the trash still holds it.
    #[test]
    fn a_recording_no_note_holds_is_an_issue_and_one_in_the_trash_is_not() {
        let (db, _temp) = open();
        let kept = db.create_note("נשאר").unwrap();
        let trashed = db.create_note("בפח").unwrap();
        let in_kept = db.create_audio_file("בפתק.ogg", None, None, FileOrigin::Imported, None).unwrap();
        let in_trash = db.create_audio_file("בפח.ogg", None, None, FileOrigin::Imported, None).unwrap();
        let loose = db.create_audio_file("לבד.ogg", None, None, FileOrigin::Imported, None).unwrap();
        db.attach_to_note(&kept, &in_kept, "audio_file").unwrap();
        db.attach_to_note(&trashed, &in_trash, "audio_file").unwrap();
        let association = db.attach_to_note(&kept, &loose, "audio_file").unwrap();
        db.delete_note(&trashed).unwrap();
        assert!(issues(&db, None, HERE).unwrap().orphaned_recordings.is_empty());

        db.detach_from_note(&association).unwrap();
        let found = issues(&db, None, HERE).unwrap();
        assert_eq!(found.orphaned_recordings, vec![OrphanedRecording { audio_id: loose, filename: "לבד.ogg".to_string() }]);
        assert!(found.orphaned_attachments.is_empty(), "a detached attachment is not an orphan");
    }

    /// ISSUE-1: tags whose names contain a space, a tab or another whitespace
    /// character are listed with their paths; a deleted one is not.
    #[test]
    fn tags_whose_names_contain_whitespace_are_listed_with_their_paths() {
        let (db, _temp) = open();
        let parent = db.create_tag("עבודה", None).unwrap();
        let spaced = db.create_tag("פגישת צוות", Some(&parent)).unwrap();
        let tabbed = db.create_tag("a\tb", None).unwrap();
        let no_break = db.create_tag("x\u{00A0}y", None).unwrap();
        db.create_tag("בלי-רווח", None).unwrap();
        let deleted = db.create_tag("נמחק עם רווח", None).unwrap();
        db.delete_tag(&deleted).unwrap();

        let found = issues(&db, None, HERE).unwrap();
        let listed: std::collections::HashMap<_, _> = found.tags_with_whitespace.iter().map(|t| (t.tag_id.clone(), t)).collect();
        assert_eq!(listed.len(), 3, "{:?}", found.tags_with_whitespace);
        assert!(listed[&spaced].path.contains("עבודה") && listed[&spaced].path.contains("פגישת צוות"));
        assert!(listed.contains_key(&tabbed) && listed.contains_key(&no_break));
        assert_eq!(found.count(), 3);
        let _ = PHONE;
    }
}
