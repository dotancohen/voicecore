//! Applying a batch of sync changes, shared by the server and the client.
//!
//! Entity rows (notes, tags, links, audio files, transcriptions) carry only
//! their non-versioned columns; every editable value arrives as immutable
//! `field_version` entries and the heads are recomputed afterwards, which is
//! where merging and conflict detection happen (see `versions.rs`).
//!
//! Order within a batch: versions first (so the rows that follow find their
//! history and never need fallback roots), entity rows second, links last,
//! then one head recompute per touched field. A change that fails is stored in
//! `sync_failures` and retried on the next batch instead of being dropped.

use std::collections::HashSet;

use crate::database::Database;
use crate::error::VoiceResult;
use crate::models::SyncChange;
use crate::versions::{
    fields_for_entity, note_tag_entity_id, VersionRow, ENTITY_AUDIO_FILE, ENTITY_NOTE,
    ENTITY_NOTE_ATTACHMENT, ENTITY_NOTE_TAG, ENTITY_TAG, ENTITY_TRANSCRIPTION,
};

/// The timestamps that most entities carry, each with the timezone it was
/// written in.
const STAMPS3: &[&str] = &["created_at", "modified_at", "deleted_at"];

/// Result of applying one change.
#[derive(Debug, PartialEq, Eq)]
pub enum ApplyResult {
    Applied,
    Skipped,
}

/// Result of applying a batch.
#[derive(Debug, Default, Clone)]
pub struct ApplyOutcome {
    pub applied: i64,
    /// Conflicts flagged while recomputing heads for this batch
    pub conflicts: i64,
    /// Changes that could not be applied (queued for retry)
    pub errors: Vec<String>,
    /// Previously failed changes that succeeded this time
    pub retried_ok: i64,
}

/// Apply order: versions first (so rows that follow find their history and
/// never need fallback roots), then rows, then links.
fn entity_order(entity_type: &str) -> u8 {
    match entity_type {
        "field_version" => 0,
        "note" | "tag" | "audio_file" | "file_storage_config" => 1,
        "note_tag" | "note_attachment" | "transcription" => 2,
        _ => 3,
    }
}

/// Apply a batch from `peer_device_id`. `sync_received_at` is stamped on every
/// row and version so that this device relays them onward.
pub fn apply_changes(
    db: &Database,
    changes: &[SyncChange],
    peer_device_id: &str,
    peer_device_name: Option<&str>,
    sync_received_at: i64,
) -> VoiceResult<ApplyOutcome> {
    // One write transaction per batch: thousands of statements, one fsync.
    // A statement that fails inside it rolls back only itself, so the
    // independent-change semantics below are kept. Any error that escapes
    // rolls the batch back; the sender keeps its cursor and retries the page.
    db.begin_batch()?;
    match apply_changes_in_batch(db, changes, peer_device_id, peer_device_name, sync_received_at) {
        Ok(outcome) => {
            db.commit_batch()?;
            Ok(outcome)
        }
        Err(e) => {
            db.rollback_batch();
            Err(e)
        }
    }
}

fn apply_changes_in_batch(
    db: &Database,
    changes: &[SyncChange],
    peer_device_id: &str,
    peer_device_name: Option<&str>,
    sync_received_at: i64,
) -> VoiceResult<ApplyOutcome> {
    let mut outcome = ApplyOutcome::default();
    let mut touched: HashSet<(String, String, String)> = HashSet::new();
    let had_pending = db.count_pending_sync_failures()? > 0;
    // Conflicts can be flagged while a row is applied (a value the graph has
    // never seen gets its own root) as well as in the final recompute, so
    // count every record created during this batch.
    let conflicts_before = db.max_conflict_rowid()?;

    // 1. Retry anything that failed earlier.
    for (failure_id, change) in db.get_pending_sync_failures()? {
        // A change about something that has since been removed for good will
        // never apply. Without this it would be retried at every sync for
        // ever, and the failure would keep the fleet from ever settling.
        if purged_already(db, &change)? {
            db.resolve_sync_failure(&failure_id)?;
            continue;
        }
        match apply_one(db, &change, sync_received_at, &mut touched) {
            Ok(_) => {
                db.resolve_sync_failure(&failure_id)?;
                outcome.retried_ok += 1;
            }
            Err(e) => {
                tracing::debug!("Retry of {} {} still failing: {}", change.entity_type, change.entity_id, e);
            }
        }
    }

    // 2. This batch, in dependency order.
    let mut sorted: Vec<&SyncChange> = changes.iter().collect();
    sorted.sort_by(|a, b| {
        entity_order(&a.entity_type)
            .cmp(&entity_order(&b.entity_type))
            .then(a.timestamp.cmp(&b.timestamp))
    });

    for change in sorted {
        match apply_one(db, change, sync_received_at, &mut touched) {
            Ok(ApplyResult::Applied) => outcome.applied += 1,
            Ok(ApplyResult::Skipped) => {}
            Err(e) => {
                let msg = format!("Error applying {} {}: {}", change.entity_type, change.entity_id, e);
                tracing::warn!("{}", msg);
                // Queue known types for retry; an unknown type can never apply here.
                if ALL_SYNC_ENTITY_TYPES.contains(&change.entity_type.as_str()) {
                    if let Err(record_err) = db.record_sync_failure(peer_device_id, peer_device_name, change, &e.to_string()) {
                        tracing::error!("Could not queue failed change for retry: {}", record_err);
                        outcome.errors.push(format!("Could not queue failed change for retry: {}", record_err));
                    }
                }
                outcome.errors.push(msg);
            }
        }
    }

    // 3. Merge and flag. Then give a head to anything that was waiting for a
    // row that arrived in this batch: the deferred list is always cheap; the
    // full scan for headless fields only when something went wrong in this
    // batch or before it.
    let heads = db.recompute_heads(&touched)?;
    if heads.deferred > 0 || !outcome.errors.is_empty() || had_pending || outcome.retried_ok > 0 {
        db.recompute_headless_fields()?;
    } else {
        db.recompute_deferred_fields()?;
    }
    outcome.conflicts = db.count_conflicts_after_rowid(conflicts_before)?;

    Ok(outcome)
}

/// Whether this change is about something that was removed for good.
///
/// A version is judged by the entity it belongs to, not by its own id: the
/// history of a purged note is gone with it, and a copy arriving from a peer
/// would put part of it back.
fn purged_already(db: &Database, change: &SyncChange) -> VoiceResult<bool> {
    // What this change is about, and what it hangs on. A transcription of a
    // purged recording, or an attachment of a purged note, is dropped even
    // before its own purge arrives: it has nothing left to belong to, and
    // storing it would fail on the foreign key for ever.
    let mut candidates: Vec<(&str, &str)> = Vec::new();
    match change.entity_type.as_str() {
        "purge" => return Ok(false),
        "field_version" => {
            let of_type = change.data["entity_type"].as_str().unwrap_or("");
            let of_id = change.data["entity_id"].as_str().unwrap_or("");
            candidates.push((of_type, of_id));
            if of_type == ENTITY_NOTE_TAG {
                // A tag link is named by the pair of ids, which is not a
                // uuid; the note it belongs to is what can be purged.
                candidates.push((ENTITY_NOTE, of_id.split(':').next().unwrap_or("")));
            }
        }
        "note_tag" => {
            // Named by two ids; the note is the one that can be purged.
            candidates.push((ENTITY_NOTE, change.entity_id.split(':').next().unwrap_or("")));
        }
        ENTITY_TRANSCRIPTION => {
            candidates.push((ENTITY_TRANSCRIPTION, change.entity_id.as_str()));
            candidates.push((ENTITY_AUDIO_FILE, change.data["audio_file_id"].as_str().unwrap_or("")));
        }
        ENTITY_NOTE_ATTACHMENT => {
            // By name, and by the recording it points at, but never by its
            // note: an attachment moves between notes, so dropping it
            // because of the note it happens to name here would remove
            // different rows on different devices.
            candidates.push((ENTITY_NOTE_ATTACHMENT, change.entity_id.as_str()));
            candidates.push((ENTITY_AUDIO_FILE, change.data["attachment_id"].as_str().unwrap_or("")));
        }
        other => candidates.push((other, change.entity_id.as_str())),
    }
    for (entity_type, entity_id) in candidates {
        if entity_type.is_empty() || entity_id.is_empty() {
            continue;
        }
        if db.is_purged(entity_type, entity_id)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn touch_entity(touched: &mut HashSet<(String, String, String)>, entity_type: &str, entity_id: &str) {
    for field in fields_for_entity(entity_type) {
        touched.insert((entity_type.to_string(), entity_id.to_string(), field.to_string()));
    }
}

/// Apply a single change. Idempotent: applying the same change twice is a no-op.
pub fn apply_one(
    db: &Database,
    change: &SyncChange,
    sync_received_at: i64,
    touched: &mut HashSet<(String, String, String)>,
) -> VoiceResult<ApplyResult> {
    let data = &change.data;
    if !matches!(change.operation.as_str(), "create" | "update" | "delete") {
        tracing::warn!("Skipping change with unknown operation '{}' for {} {}", change.operation, change.entity_type, change.entity_id);
        return Ok(ApplyResult::Skipped);
    }
    // Nothing brings back what was removed for good. A peer that has not
    // heard of the purge yet keeps sending the rows it still has, and every
    // one of them is dropped here rather than resurrecting the entity.
    if purged_already(db, change)? {
        return Ok(ApplyResult::Skipped);
    }
    match change.entity_type.as_str() {
        "purge" => {
            let entity_type = data["entity_type"].as_str().unwrap_or("");
            let entity_id = data["entity_id"].as_str().unwrap_or("");
            if entity_type.is_empty() || entity_id.is_empty() {
                tracing::warn!("purge without an entity: {}", change.entity_id);
                return Ok(ApplyResult::Skipped);
            }
            db.apply_purge(entity_type, entity_id, data["purged_at"].as_i64().unwrap_or(sync_received_at))?;
            Ok(ApplyResult::Applied)
        }
        "field_version" => {
            let mut row = VersionRow::from_json(data)?;
            row.sync_received_at = Some(sync_received_at);
            let inserted = db.insert_version(&row)?;
            touched.insert((row.entity_type.clone(), row.entity_id.clone(), row.field.clone()));
            Ok(if inserted { ApplyResult::Applied } else { ApplyResult::Skipped })
        }
        "note" => {
            db.apply_sync_note(
                &change.entity_id,
                data["created_at"].as_i64().unwrap_or(0),
                data["content"].as_str().unwrap_or(""),
                data["modified_at"].as_i64(),
                data["deleted_at"].as_i64(),
                Some(sync_received_at),
                data["primary_attachment_id"].as_str(),
            )?;
            db.apply_zones_by_id("notes", &change.entity_id, STAMPS3, data)?;
            touch_entity(touched, ENTITY_NOTE, &change.entity_id);
            Ok(ApplyResult::Applied)
        }
        "tag" => {
            db.apply_sync_tag_with_deleted(
                &change.entity_id,
                data["name"].as_str().unwrap_or(""),
                data["parent_id"].as_str(),
                data["created_at"].as_i64().unwrap_or(0),
                data["modified_at"].as_i64(),
                data["deleted_at"].as_i64(),
                Some(sync_received_at),
            )?;
            db.apply_zones_by_id("tags", &change.entity_id, STAMPS3, data)?;
            touch_entity(touched, ENTITY_TAG, &change.entity_id);
            Ok(ApplyResult::Applied)
        }
        "note_tag" => {
            let parts: Vec<&str> = change.entity_id.split(':').collect();
            if parts.len() != 2 {
                tracing::warn!("note_tag: invalid entity_id format: {}", change.entity_id);
                return Ok(ApplyResult::Skipped);
            }
            db.apply_sync_note_tag(
                parts[0],
                parts[1],
                data["created_at"].as_i64().unwrap_or(0),
                data["modified_at"].as_i64(),
                data["deleted_at"].as_i64(),
                Some(sync_received_at),
            )?;
            db.apply_zones_for_note_tag(parts[0], parts[1], STAMPS3, data)?;
            touch_entity(touched, ENTITY_NOTE_TAG, &note_tag_entity_id(parts[0], parts[1]));
            Ok(ApplyResult::Applied)
        }
        "note_attachment" => {
            db.apply_sync_note_attachment(
                &change.entity_id,
                data["note_id"].as_str().unwrap_or(""),
                data["attachment_id"].as_str().unwrap_or(""),
                data["attachment_type"].as_str().unwrap_or(""),
                data["created_at"].as_i64().unwrap_or(0),
                data["modified_at"].as_i64(),
                data["deleted_at"].as_i64(),
                Some(sync_received_at),
            )?;
            db.apply_zones_by_id("note_attachments", &change.entity_id, STAMPS3, data)?;
            touch_entity(touched, ENTITY_NOTE_ATTACHMENT, &change.entity_id);
            Ok(ApplyResult::Applied)
        }
        "transcription" => {
            db.apply_sync_transcription(
                &change.entity_id,
                data["audio_file_id"].as_str().unwrap_or(""),
                data["content"].as_str().unwrap_or(""),
                data["content_segments"].as_str(),
                data["service"].as_str().unwrap_or(""),
                data["service_arguments"].as_str(),
                data["service_response"].as_str(),
                data["state"].as_str().unwrap_or(crate::database::DEFAULT_TRANSCRIPTION_STATE),
                data["device_id"].as_str().unwrap_or(""),
                data["created_at"].as_i64().unwrap_or(0),
                data["modified_at"].as_i64(),
                data["deleted_at"].as_i64(),
                Some(sync_received_at),
            )?;
            db.apply_zones_by_id("transcriptions", &change.entity_id, STAMPS3, data)?;
            touch_entity(touched, ENTITY_TRANSCRIPTION, &change.entity_id);
            Ok(ApplyResult::Applied)
        }
        "audio_file" => {
            // Every copy is applied: the upsert keeps the newer value per
            // metadata column, never erases a cloud location, and never
            // touches the versioned columns. Skipping older rows (as before)
            // left a peer that had edited the summary first without the
            // storage key, unable to download the file until the uploader
            // happened to change the record again.
            db.apply_sync_audio_file(
                &change.entity_id,
                data["imported_at"].as_i64().unwrap_or(0),
                data["filename"].as_str().unwrap_or(""),
                data["file_created_at"].as_i64(),
                data["duration_seconds"].as_i64(),
                data["summary"].as_str(),
                data["modified_at"].as_i64(),
                data["deleted_at"].as_i64(),
                Some(sync_received_at),
                data["storage_provider"].as_str(),
                data["storage_key"].as_str(),
                data["storage_uploaded_at"].as_i64(),
                data["primary_transcription_id"].as_str(),
                data["file_created_at_offset"].as_i64().and_then(|o| i32::try_from(o).ok()),
                data["content_sha256"].as_str(),
                data["storage_encrypted"].as_bool(),
            )?;
            db.apply_zones_by_id(
                "audio_files",
                &change.entity_id,
                &["imported_at", "file_created_at", "modified_at", "deleted_at"],
                data,
            )?;
            touch_entity(touched, ENTITY_AUDIO_FILE, &change.entity_id);
            Ok(ApplyResult::Applied)
        }
        "file_storage_config" => {
            let provider = data["provider"].as_str().unwrap_or("none");
            let config = data.get("config").and_then(|v| if v.is_null() { None } else { Some(v.clone()) });
            db.apply_sync_file_storage_config(
                provider,
                config.as_ref(),
                data["modified_at"].as_i64(),
                data["device_id"].as_str(),
                Some(sync_received_at),
            )?;
            Ok(ApplyResult::Applied)
        }
        other => Err(crate::error::VoiceError::Sync(format!("Unknown entity type: {}", other))),
    }
}

/// Every entity type the feed can carry. Keep in sync with `get_changes_since`.
pub const ALL_SYNC_ENTITY_TYPES: &[&str] = &[
    "note",
    "tag",
    "note_tag",
    "note_attachment",
    "audio_file",
    "transcription",
    "file_storage_config",
    "field_version",
    "purge",
];
