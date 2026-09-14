//! Versioned fields: Git-style history and merging for every editable field.
//!
//! Every editable value (note content, transcription text and state, audio
//! summary, tag name and parent, tag/attachment membership, deletion flags,
//! synced settings) is an append-only chain of immutable *versions*. A version
//! points at the version it was derived from (`parent_id`) and, for merges, at a
//! second parent (`merge_parent_id`). The current value of a field is its *head*.
//!
//! Versions are synced as ordinary create-only entities, so they can never
//! conflict in transit. When a device holds more than one leaf for a field the
//! leaves are merged against their lowest common ancestor exactly like Git:
//!
//! - text fields get a real three-way merge; non-overlapping edits merge
//!   silently, overlapping hunks are kept verbatim inside conflict markers and
//!   a conflict record is created;
//! - scalar fields (tag name, tag parent) either agree or are flagged;
//! - flag sets (transcription state) merge flag by flag;
//! - memberships (note-tag links, attachments) stay attached and are flagged;
//! - deletions never win over a concurrent edit: the entity is kept alive and
//!   flagged.
//!
//! Merge and root versions have ids derived by hashing their inputs, so every
//! device computes the identical version independently and converges without a
//! coordinator. A conflict is resolved by any later version that descends from
//! the merge version (a user edit, or an explicit "accept"), and that resolution
//! propagates through sync like any other version.
//!
//! Nothing is ever deleted from the history, so no edit is ever lost.

use std::collections::{HashMap, HashSet, VecDeque};

use rusqlite::{params, OptionalExtension};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::database::{get_this_device_id, get_this_device_name, Database};
use crate::error::{VoiceError, VoiceResult};
use crate::validation::uuid_bytes_to_hex;

// ============================================================================
// Registry
// ============================================================================

/// How a field's values are merged when two devices changed it concurrently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldKind {
    /// Multi-line text: three-way merge, conflict markers on overlap.
    Text,
    /// Single value: identical or flagged.
    Scalar,
    /// Space-separated flags with `!` negation (transcription state): merged flag by flag.
    Flags,
    /// "1" attached / "0" detached: a concurrent detach never wins silently.
    Membership,
    /// "1" deleted / "0" alive: a concurrent delete never wins over an edit.
    Deleted,
}

/// A device of the account (CARD-1). Every field is versioned; `revoked`
/// merges as Membership so that "1" always wins.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeviceCard {
    pub device_id: String,
    pub name: String,
    /// `SHA256:aa:bb:…` of the certificate the device listens with, or empty
    pub certificate_fingerprint: String,
    /// JSON list of URLs, or empty
    pub addresses: String,
    /// "1" while listening
    pub listens: String,
    /// Hex SHA-256 of the device's key
    pub key_hash: String,
    /// "1" once revoked
    pub revoked: String,
    /// "voice", or another application later
    pub application: String,
}

impl DeviceCard {
    pub fn is_revoked(&self) -> bool {
        self.revoked == "1"
    }

    fn from_row(row: &rusqlite::Row) -> rusqlite::Result<Self> {
        Ok(Self {
            device_id: row.get(0)?,
            name: row.get::<_, Option<String>>(1)?.unwrap_or_default(),
            certificate_fingerprint: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
            addresses: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
            listens: row.get::<_, Option<String>>(4)?.unwrap_or_default(),
            key_hash: row.get::<_, Option<String>>(5)?.unwrap_or_default(),
            revoked: row.get::<_, Option<String>>(6)?.unwrap_or_default(),
            application: row.get::<_, Option<String>>(7)?.unwrap_or_default(),
        })
    }
}

/// A registered versioned field.
#[derive(Debug, Clone, Copy)]
pub struct FieldSpec {
    pub entity_type: &'static str,
    pub field: &'static str,
    pub kind: FieldKind,
}

pub const ENTITY_NOTE: &str = "note";
pub const ENTITY_TRANSCRIPTION: &str = "transcription";
pub const ENTITY_AUDIO_FILE: &str = "audio_file";
pub const ENTITY_TAG: &str = "tag";
pub const ENTITY_NOTE_TAG: &str = "note_tag";
pub const ENTITY_NOTE_ATTACHMENT: &str = "note_attachment";
pub const ENTITY_SETTING: &str = "setting";
/// A device of the account: its card (CARD-1). The entity id is the device id.
pub const ENTITY_DEVICE: &str = "device";

pub const FIELD_CONTENT: &str = "content";
pub const FIELD_STATE: &str = "state";
pub const FIELD_SUMMARY: &str = "summary";
pub const FIELD_NAME: &str = "name";
pub const FIELD_PARENT: &str = "parent";
pub const FIELD_ACTIVE: &str = "active";
pub const FIELD_DELETED: &str = "deleted";
pub const FIELD_VALUE: &str = "value";
/// Device card fields (CARD-1).
pub const FIELD_CERTIFICATE_FINGERPRINT: &str = "certificate_fingerprint";
/// JSON list of the URLs the device listens on.
pub const FIELD_ADDRESSES: &str = "addresses";
/// "1" while the device listens, "0" otherwise.
pub const FIELD_LISTENS: &str = "listens";
/// Hex SHA-256 of the device's key (AUTH-2); the key itself is nowhere else.
pub const FIELD_KEY_HASH: &str = "key_hash";
/// "1" once any device revoked this one; Membership kind, so "1" always wins.
pub const FIELD_REVOKED: &str = "revoked";
/// Which application the device runs ("voice"; later others, see PROTO-11).
pub const FIELD_APPLICATION: &str = "application";
/// The attachment of a note that stands for it: the recording played when
/// the note is opened, and the one whose transcription the list shows.
pub const FIELD_PRIMARY_ATTACHMENT: &str = "primary_attachment";
/// The transcription of a recording that stands for it.
pub const FIELD_PRIMARY_TRANSCRIPTION: &str = "primary_transcription";

/// Every versioned field. Adding a new editable field means adding a line
/// here, a denormalisation arm in [`apply_head_to_entity`], and routing the
/// write path through [`Database::set_field`].
pub const FIELD_REGISTRY: &[FieldSpec] = &[
    FieldSpec { entity_type: ENTITY_NOTE, field: FIELD_CONTENT, kind: FieldKind::Text },
    FieldSpec { entity_type: ENTITY_NOTE, field: FIELD_PRIMARY_ATTACHMENT, kind: FieldKind::Scalar },
    FieldSpec { entity_type: ENTITY_NOTE, field: FIELD_DELETED, kind: FieldKind::Deleted },
    FieldSpec { entity_type: ENTITY_TRANSCRIPTION, field: FIELD_CONTENT, kind: FieldKind::Text },
    FieldSpec { entity_type: ENTITY_TRANSCRIPTION, field: FIELD_STATE, kind: FieldKind::Flags },
    FieldSpec { entity_type: ENTITY_TRANSCRIPTION, field: FIELD_DELETED, kind: FieldKind::Deleted },
    FieldSpec { entity_type: ENTITY_AUDIO_FILE, field: FIELD_SUMMARY, kind: FieldKind::Text },
    FieldSpec { entity_type: ENTITY_AUDIO_FILE, field: FIELD_PRIMARY_TRANSCRIPTION, kind: FieldKind::Scalar },
    FieldSpec { entity_type: ENTITY_AUDIO_FILE, field: FIELD_DELETED, kind: FieldKind::Deleted },
    FieldSpec { entity_type: ENTITY_TAG, field: FIELD_NAME, kind: FieldKind::Scalar },
    FieldSpec { entity_type: ENTITY_TAG, field: FIELD_PARENT, kind: FieldKind::Scalar },
    FieldSpec { entity_type: ENTITY_TAG, field: FIELD_DELETED, kind: FieldKind::Deleted },
    FieldSpec { entity_type: ENTITY_NOTE_TAG, field: FIELD_ACTIVE, kind: FieldKind::Membership },
    FieldSpec { entity_type: ENTITY_NOTE_ATTACHMENT, field: FIELD_ACTIVE, kind: FieldKind::Membership },
    FieldSpec { entity_type: ENTITY_SETTING, field: FIELD_VALUE, kind: FieldKind::Scalar },
    FieldSpec { entity_type: ENTITY_DEVICE, field: FIELD_NAME, kind: FieldKind::Scalar },
    FieldSpec { entity_type: ENTITY_DEVICE, field: FIELD_CERTIFICATE_FINGERPRINT, kind: FieldKind::Scalar },
    FieldSpec { entity_type: ENTITY_DEVICE, field: FIELD_ADDRESSES, kind: FieldKind::Scalar },
    FieldSpec { entity_type: ENTITY_DEVICE, field: FIELD_LISTENS, kind: FieldKind::Scalar },
    FieldSpec { entity_type: ENTITY_DEVICE, field: FIELD_KEY_HASH, kind: FieldKind::Scalar },
    FieldSpec { entity_type: ENTITY_DEVICE, field: FIELD_REVOKED, kind: FieldKind::Membership },
    FieldSpec { entity_type: ENTITY_DEVICE, field: FIELD_APPLICATION, kind: FieldKind::Scalar },
];

/// Look up how a field merges. `None` means the field is not versioned.
pub fn field_kind(entity_type: &str, field: &str) -> Option<FieldKind> {
    FIELD_REGISTRY
        .iter()
        .find(|s| s.entity_type == entity_type && s.field == field)
        .map(|s| s.kind)
}

/// The fields that make up an entity's editable state, deletion flag last.
pub fn fields_for_entity(entity_type: &str) -> Vec<&'static str> {
    let mut fields: Vec<&'static str> = FIELD_REGISTRY
        .iter()
        .filter(|s| s.entity_type == entity_type && s.field != FIELD_DELETED)
        .map(|s| s.field)
        .collect();
    if FIELD_REGISTRY
        .iter()
        .any(|s| s.entity_type == entity_type && s.field == FIELD_DELETED)
    {
        fields.push(FIELD_DELETED);
    }
    fields
}

/// Default transcription state, used to order flags canonically.
pub const DEFAULT_FLAG_ORDER: &[&str] = &["original", "verified", "verbatim", "cleaned", "polished"];

// ============================================================================
// Data types
// ============================================================================

/// One immutable version of a field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionRow {
    /// 16-byte id (UUID7 for local edits, hash-derived for roots and merges)
    pub id: Vec<u8>,
    pub entity_type: String,
    pub entity_id: String,
    pub field: String,
    pub parent_id: Option<Vec<u8>>,
    pub merge_parent_id: Option<Vec<u8>>,
    pub content: String,
    /// For `Deleted` tombstones: JSON `{field: head_id_hex}` of the entity's
    /// other fields as the deleting device saw them.
    pub context: Option<String>,
    /// For merge and resurrection versions that need a human: the conflict
    /// kind ("text", "scalar", "flags", "membership", "delete"). Travels with
    /// the version so every device records the same conflict.
    pub conflict_kind: Option<String>,
    pub device_id: Option<String>,
    pub device_name: Option<String>,
    pub created_at: i64,
    /// Seconds east of UTC on the device that wrote this version, and the
    /// IANA name of its timezone when it knew one. With `created_at` they
    /// give the clock the author was reading.
    pub created_at_offset: Option<i32>,
    pub created_at_zone: Option<String>,
    pub sync_received_at: Option<i64>,
    /// Derived version that an authored version builds on: travels in the
    /// feed (VER-10). Carried through relays so every device can complete the
    /// child.
    pub published: bool,
}

impl VersionRow {
    pub fn id_hex(&self) -> String {
        hex(&self.id)
    }
    pub fn parent_hex(&self) -> Option<String> {
        self.parent_id.as_ref().map(|p| hex(p))
    }
    pub fn merge_parent_hex(&self) -> Option<String> {
        self.merge_parent_id.as_ref().map(|p| hex(p))
    }

    /// JSON payload for the sync feed.
    /// A version derived by the engine rather than written by a device: a
    /// merge (two parents) or a resurrection (child of a tombstone). Derived
    /// versions have no device and a parent. They are not synced; every
    /// device recomputes them from the same authored versions and, because
    /// their ids are hashes, arrives at the same ones.
    pub fn is_derived(&self) -> bool {
        self.device_id.is_none() && self.parent_id.is_some()
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "id": self.id_hex(),
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "field": self.field,
            "parent_id": self.parent_hex(),
            "merge_parent_id": self.merge_parent_hex(),
            "content": self.content,
            "context": self.context,
            "conflict_kind": self.conflict_kind,
            "device_id": self.device_id,
            "device_name": self.device_name,
            "created_at": self.created_at,
            "created_at_offset": self.created_at_offset,
            "created_at_zone": self.created_at_zone,
            "published": self.published,
        })
    }

    /// Parse a sync feed payload.
    pub fn from_json(data: &serde_json::Value) -> VoiceResult<Self> {
        let id = data["id"].as_str().ok_or_else(|| VoiceError::validation("id", "missing"))?;
        let parse_id = |s: &str| -> VoiceResult<Vec<u8>> {
            let bytes = (0..s.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
                .collect::<Result<Vec<u8>, _>>()
                .map_err(|_| VoiceError::validation("version id", "not hex"))?;
            if bytes.len() != 16 {
                return Err(VoiceError::validation("version id", "must be 16 bytes"));
            }
            Ok(bytes)
        };
        Ok(Self {
            id: parse_id(id)?,
            entity_type: data["entity_type"].as_str().unwrap_or("").to_string(),
            entity_id: data["entity_id"].as_str().unwrap_or("").to_string(),
            field: data["field"].as_str().unwrap_or("").to_string(),
            parent_id: match data["parent_id"].as_str() {
                Some(p) => Some(parse_id(p)?),
                None => None,
            },
            merge_parent_id: match data["merge_parent_id"].as_str() {
                Some(p) => Some(parse_id(p)?),
                None => None,
            },
            content: data["content"].as_str().unwrap_or("").to_string(),
            context: data["context"].as_str().map(String::from),
            conflict_kind: data["conflict_kind"].as_str().map(String::from),
            device_id: data["device_id"].as_str().map(String::from),
            device_name: data["device_name"].as_str().map(String::from),
            created_at: data["created_at"].as_i64().unwrap_or(0),
            // Absent from devices older than the timezone fields
            created_at_offset: data["created_at_offset"].as_i64().and_then(|o| i32::try_from(o).ok()),
            created_at_zone: data["created_at_zone"].as_str().map(String::from),
            sync_received_at: None,
            published: data["published"].as_bool().unwrap_or(false),
        })
    }
}

/// A recorded conflict: a merge that needed a human.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictRow {
    pub id: String,
    pub entity_type: String,
    pub entity_id: String,
    pub field: String,
    /// "text", "scalar", "flags", "membership", "delete"
    pub kind: String,
    pub base_version_id: Option<String>,
    pub version_a_id: String,
    pub version_b_id: String,
    pub merge_version_id: String,
    pub device_a_id: Option<String>,
    pub device_a_name: Option<String>,
    pub device_b_id: Option<String>,
    pub device_b_name: Option<String>,
    pub created_at: i64,
    pub resolved_at: Option<i64>,
}

impl ConflictRow {
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "id": self.id,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "field": self.field,
            "kind": self.kind,
            "base_version_id": self.base_version_id,
            "version_a_id": self.version_a_id,
            "version_b_id": self.version_b_id,
            "merge_version_id": self.merge_version_id,
            "device_a_id": self.device_a_id,
            "device_a_name": self.device_a_name,
            "device_b_id": self.device_b_id,
            "device_b_name": self.device_b_name,
            "created_at": self.created_at,
            "resolved_at": self.resolved_at,
        })
    }
}

/// Outcome of recomputing one field's head (or a batch of them).
#[derive(Debug, Default, Clone)]
pub struct RecomputeOutcome {
    /// Fields whose row could not be written yet (retried later)
    pub deferred: usize,
    pub head_changed: bool,
    pub new_conflicts: usize,
    pub resolved_conflicts: usize,
}

// ============================================================================
// Helpers
// ============================================================================

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn derived_id(parts: &[&str]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    for p in parts {
        hasher.update(p.as_bytes());
        hasher.update([0u8]);
    }
    hasher.finalize()[..16].to_vec()
}

/// Deterministic id for a root version created from pre-existing data, so
/// that devices holding the same value produce the same root.
pub fn root_version_id(entity_type: &str, entity_id: &str, field: &str, content: &str) -> Vec<u8> {
    derived_id(&["root", entity_type, entity_id, field, content])
}

/// Deterministic id for a merge of two versions.
fn merge_version_id(a: &[u8], b: &[u8], content: &str) -> Vec<u8> {
    derived_id(&["merge", &hex(a), &hex(b), content])
}

/// Deterministic id for an explicit "accept the merged text" resolution.
fn accept_version_id(merge: &[u8]) -> Vec<u8> {
    derived_id(&["accept", &hex(merge)])
}

/// Deterministic id for the resurrection of an entity deleted concurrently with an edit.
/// One resurrection per tombstone: the id must not depend on which edit
/// was current when a device noticed, or devices would disagree.
fn resurrect_version_id(tombstone: &[u8]) -> Vec<u8> {
    derived_id(&["resurrect", &hex(tombstone)])
}

fn conflict_id_for(merge: &[u8]) -> Vec<u8> {
    derived_id(&["conflict", &hex(merge)])
}

/// Parse a transcription-style flag string into (flag -> true/false), in order.
fn parse_flags(s: &str) -> Vec<(String, bool)> {
    let mut out: Vec<(String, bool)> = Vec::new();
    for token in s.split_whitespace() {
        let (name, value) = match token.strip_prefix('!') {
            Some(n) => (n.to_string(), false),
            None => (token.to_string(), true),
        };
        if let Some(existing) = out.iter_mut().find(|(n, _)| *n == name) {
            existing.1 = value;
        } else {
            out.push((name, value));
        }
    }
    out
}

fn render_flags(flags: &HashMap<String, bool>) -> String {
    let mut names: Vec<&String> = flags.keys().collect();
    names.sort_by_key(|n| {
        let idx = DEFAULT_FLAG_ORDER.iter().position(|d| d == &n.as_str());
        (idx.is_none(), idx.unwrap_or(0), (*n).clone())
    });
    names
        .into_iter()
        .map(|n| if flags[n] { n.clone() } else { format!("!{}", n) })
        .collect::<Vec<_>>()
        .join(" ")
}

// ============================================================================
// Merge primitives
// ============================================================================

/// Result of merging two versions of one field.
#[derive(Debug, Clone)]
pub struct FieldMerge {
    pub content: String,
    /// Some(kind) when a human needs to look at it.
    pub conflict_kind: Option<&'static str>,
}

/// Three-way text merge with Git-style markers on overlap.
///
/// Markers are labelled `VERSION A` / `VERSION B`, never with device names,
/// so that every device renders byte-identical merge content.
pub fn three_way_text_merge(base: &str, a: &str, b: &str) -> FieldMerge {
    if a == b {
        return FieldMerge { content: a.to_string(), conflict_kind: None };
    }
    if a == base {
        return FieldMerge { content: b.to_string(), conflict_kind: None };
    }
    if b == base {
        return FieldMerge { content: a.to_string(), conflict_kind: None };
    }
    // diffy works on lines; a final line without a newline would have the
    // conflict marker glued to it ("text=======") and be lost as a line.
    // Merge with every side newline-terminated, then drop the added newline
    // when no side had one.
    fn terminated(s: &str) -> String {
        if s.is_empty() || s.ends_with('\n') { s.to_string() } else { format!("{}\n", s) }
    }
    let had_newline = [base, a, b].iter().any(|s| s.ends_with('\n'));
    let (nb, na, nbb) = (terminated(base), terminated(a), terminated(b));
    let strip = |mut text: String| {
        if !had_newline && text.ends_with('\n') {
            text.pop();
        }
        text
    };
    let mut options = diffy::MergeOptions::new();
    options.set_conflict_style(diffy::ConflictStyle::Merge);
    match options.merge(&nb, &na, &nbb) {
        Ok(clean) => FieldMerge { content: strip(clean), conflict_kind: None },
        Err(marked) => {
            let relabelled = marked
                .replace("<<<<<<< ours\n", "<<<<<<< VERSION A\n")
                .replace(">>>>>>> theirs\n", ">>>>>>> VERSION B\n")
                .replace("<<<<<<< ours", "<<<<<<< VERSION A")
                .replace(">>>>>>> theirs", ">>>>>>> VERSION B");
            FieldMerge { content: strip(relabelled), conflict_kind: Some("text") }
        }
    }
}

/// Merge two flag strings against their base, flag by flag.
fn merge_flags(base: &str, a: &str, b: &str, a_first: bool) -> FieldMerge {
    let base_f: HashMap<String, bool> = parse_flags(base).into_iter().collect();
    let a_f: HashMap<String, bool> = parse_flags(a).into_iter().collect();
    let b_f: HashMap<String, bool> = parse_flags(b).into_iter().collect();
    let mut names: Vec<String> = a_f.keys().chain(b_f.keys()).chain(base_f.keys()).cloned().collect();
    names.sort();
    names.dedup();

    let mut out = HashMap::new();
    let mut conflict = false;
    for name in names {
        let bv = base_f.get(&name).copied();
        let av = a_f.get(&name).copied();
        let bbv = b_f.get(&name).copied();
        let value = match (av, bbv) {
            (Some(x), Some(y)) if x == y => x,
            (Some(x), None) => x,
            (None, Some(y)) => y,
            (Some(x), Some(y)) => {
                if Some(x) == bv {
                    y
                } else if Some(y) == bv {
                    x
                } else {
                    conflict = true;
                    if a_first { x } else { y }
                }
            }
            (None, None) => bv.unwrap_or(false),
        };
        out.insert(name, value);
    }
    FieldMerge {
        content: render_flags(&out),
        conflict_kind: if conflict { Some("flags") } else { None },
    }
}

/// Merge two leaves of a field of the given kind. `a` and `b` are ordered by id;
/// `a_is_later` says which one was created later (used only to pick the live
/// value of a flagged scalar, where both values are kept in history anyway).
fn merge_leaves(kind: FieldKind, base: Option<&str>, a: &VersionRow, b: &VersionRow) -> FieldMerge {
    let a_is_later = (a.created_at, &a.id) > (b.created_at, &b.id);
    match kind {
        FieldKind::Text => three_way_text_merge(base.unwrap_or(""), &a.content, &b.content),
        FieldKind::Flags => merge_flags(base.unwrap_or(""), &a.content, &b.content, a_is_later),
        FieldKind::Scalar => {
            if a.content == b.content {
                FieldMerge { content: a.content.clone(), conflict_kind: None }
            } else if base == Some(a.content.as_str()) {
                FieldMerge { content: b.content.clone(), conflict_kind: None }
            } else if base == Some(b.content.as_str()) {
                FieldMerge { content: a.content.clone(), conflict_kind: None }
            } else {
                // Both changed to different values: keep the later one live,
                // the other stays in history, and flag it for the user.
                let live = if a_is_later { &a.content } else { &b.content };
                FieldMerge { content: live.clone(), conflict_kind: Some("scalar") }
            }
        }
        FieldKind::Membership => {
            // Two leaves means both sides acted on the link since they last
            // agreed. If they disagree now, a detach never wins silently: the
            // link stays and the user is asked (even when one side went back
            // to the base value by detaching and re-attaching).
            if a.content == b.content {
                FieldMerge { content: a.content.clone(), conflict_kind: None }
            } else {
                FieldMerge { content: "1".to_string(), conflict_kind: Some("membership") }
            }
        }
        FieldKind::Deleted => {
            // Same rule: concurrent delete and undelete keep the entity alive.
            if a.content == b.content {
                FieldMerge { content: a.content.clone(), conflict_kind: None }
            } else {
                FieldMerge { content: "0".to_string(), conflict_kind: Some("delete") }
            }
        }
    }
}

// ============================================================================
// Graph
// ============================================================================

/// All known versions of one field, indexed by id.
struct FieldGraph {
    versions: HashMap<Vec<u8>, VersionRow>,
}

impl FieldGraph {
    fn get(&self, id: &[u8]) -> Option<&VersionRow> {
        self.versions.get(id)
    }

    /// Ids whose whole ancestry is known.
    fn complete_ids(&self) -> HashSet<Vec<u8>> {
        let mut complete: HashSet<Vec<u8>> = HashSet::new();
        // Iterate until no change (graphs are small; a handful of passes).
        loop {
            let mut changed = false;
            for (id, v) in &self.versions {
                if complete.contains(id) {
                    continue;
                }
                let parents_ok = [&v.parent_id, &v.merge_parent_id]
                    .iter()
                    .all(|p| match p {
                        None => true,
                        Some(pid) => complete.contains(pid.as_slice()),
                    });
                if parents_ok {
                    complete.insert(id.clone());
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
        complete
    }

    /// Leaves among `ids`: versions not referenced as a parent by any version in `ids`.
    fn leaves(&self, ids: &HashSet<Vec<u8>>) -> Vec<&VersionRow> {
        let mut referenced: HashSet<&[u8]> = HashSet::new();
        for id in ids {
            if let Some(v) = self.versions.get(id) {
                if let Some(p) = &v.parent_id {
                    referenced.insert(p.as_slice());
                }
                if let Some(p) = &v.merge_parent_id {
                    referenced.insert(p.as_slice());
                }
            }
        }
        let mut leaves: Vec<&VersionRow> = ids
            .iter()
            .filter(|id| !referenced.contains(id.as_slice()))
            .filter_map(|id| self.versions.get(id))
            .collect();
        leaves.sort_by(|x, y| x.id.cmp(&y.id));
        leaves
    }

    /// Leaves of the *authored* graph: versions among `ids` that were written
    /// by a device (or are roots) and have no authored descendant. Derived
    /// versions (merges, resurrections) never take part in a fold as inputs:
    /// every device recomputes them, which makes the head a function of the
    /// authored versions alone, whatever order (or page size) they arrived in.
    fn authored_leaves(&self, ids: &HashSet<Vec<u8>>) -> Vec<&VersionRow> {
        let authored: Vec<&VersionRow> = ids
            .iter()
            .filter_map(|id| self.versions.get(id))
            .filter(|v| !v.is_derived())
            .collect();
        let mut covered: HashSet<Vec<u8>> = HashSet::new();
        for v in &authored {
            covered.extend(self.ancestors(&v.id));
        }
        let mut leaves: Vec<&VersionRow> = authored.into_iter().filter(|v| !covered.contains(&v.id)).collect();
        leaves.sort_by(|x, y| x.id.cmp(&y.id));
        leaves
    }

    /// Every ancestor id of `id` (excluding itself).
    fn ancestors(&self, id: &[u8]) -> HashSet<Vec<u8>> {
        let mut seen: HashSet<Vec<u8>> = HashSet::new();
        let mut queue: VecDeque<Vec<u8>> = VecDeque::new();
        if let Some(v) = self.versions.get(id) {
            for p in [&v.parent_id, &v.merge_parent_id].into_iter().flatten() {
                queue.push_back(p.clone());
            }
        }
        while let Some(cur) = queue.pop_front() {
            if !seen.insert(cur.clone()) {
                continue;
            }
            if let Some(v) = self.versions.get(&cur) {
                for p in [&v.parent_id, &v.merge_parent_id].into_iter().flatten() {
                    queue.push_back(p.clone());
                }
            }
        }
        seen
    }

    fn is_ancestor(&self, ancestor: &[u8], of: &[u8]) -> bool {
        self.ancestors(of).contains(ancestor)
    }

    /// Lowest common ancestor of two versions: the first ancestor of `b`
    /// (breadth-first, nearest first) that is also an ancestor-or-self of `a`.
    fn lowest_common_ancestor(&self, a: &[u8], b: &[u8]) -> Option<Vec<u8>> {
        let mut a_side = self.ancestors(a);
        a_side.insert(a.to_vec());
        if a_side.contains(b) {
            return Some(b.to_vec());
        }
        let mut seen: HashSet<Vec<u8>> = HashSet::new();
        let mut queue: VecDeque<Vec<u8>> = VecDeque::new();
        queue.push_back(b.to_vec());
        while let Some(cur) = queue.pop_front() {
            if !seen.insert(cur.clone()) {
                continue;
            }
            if cur.as_slice() != b && a_side.contains(&cur) {
                return Some(cur);
            }
            if let Some(v) = self.versions.get(&cur) {
                // Deterministic order: primary parent first.
                for p in [&v.parent_id, &v.merge_parent_id].into_iter().flatten() {
                    queue.push_back(p.clone());
                }
            }
        }
        None
    }
}

// ============================================================================
// Database integration
// ============================================================================

impl Database {
    // ------------------------------------------------------------------
    // Schema
    // ------------------------------------------------------------------

    /// Create the version tables. Idempotent.
    pub(crate) fn create_version_tables(&self) -> VoiceResult<()> {
        self.connection().execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS field_versions (
                id BLOB PRIMARY KEY,
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                field TEXT NOT NULL,
                parent_id BLOB,
                merge_parent_id BLOB,
                content TEXT NOT NULL,
                context TEXT,
                conflict_kind TEXT,
                device_id BLOB,
                device_name TEXT,
                created_at INTEGER NOT NULL,
                sync_received_at INTEGER,
                published INTEGER NOT NULL DEFAULT 0,
                -- Timezone the version was written in
                created_at_offset INTEGER,
                created_at_zone TEXT,
                seq INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_field_versions_entity
                ON field_versions(entity_type, entity_id, field);
            CREATE INDEX IF NOT EXISTS idx_field_versions_created_at ON field_versions(created_at);
            CREATE INDEX IF NOT EXISTS idx_field_versions_sync_received_at ON field_versions(sync_received_at);

            CREATE TABLE IF NOT EXISTS field_heads (
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                field TEXT NOT NULL,
                head_id BLOB NOT NULL,
                PRIMARY KEY (entity_type, entity_id, field)
            );

            CREATE TABLE IF NOT EXISTS field_conflicts (
                id BLOB PRIMARY KEY,
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                field TEXT NOT NULL,
                kind TEXT NOT NULL,
                base_version_id BLOB,
                version_a_id BLOB NOT NULL,
                version_b_id BLOB NOT NULL,
                merge_version_id BLOB NOT NULL,
                device_a_id BLOB,
                device_a_name TEXT,
                device_b_id BLOB,
                device_b_name TEXT,
                created_at INTEGER NOT NULL,
                resolved_at INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_field_conflicts_entity
                ON field_conflicts(entity_type, entity_id, resolved_at);

            CREATE TABLE IF NOT EXISTS synced_settings (
                key TEXT PRIMARY KEY,
                value TEXT,
                modified_at INTEGER
            );
            CREATE TABLE IF NOT EXISTS devices (
                device_id TEXT PRIMARY KEY,
                name TEXT,
                certificate_fingerprint TEXT,
                addresses TEXT,
                listens TEXT,
                key_hash TEXT,
                revoked TEXT,
                application TEXT,
                modified_at INTEGER
            );
            CREATE TABLE IF NOT EXISTS field_deferred (
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                field TEXT NOT NULL,
                PRIMARY KEY (entity_type, entity_id, field)
            );
            "#,
        )?;
        Ok(())
    }

    /// Ensure a field has at least one version; if it has none, create a
    /// hash-derived root from `content` and make it the head. Used by the
    /// system tags and by entity rows that arrive from a device before their versions.
    pub fn ensure_root_version(
        &self,
        entity_type: &str,
        entity_id: &str,
        field: &str,
        content: &str,
        created_at: i64,
    ) -> VoiceResult<bool> {
        if self.head_id(entity_type, entity_id, field)?.is_some() {
            // The field has history: the row is only a hint (its value is, or
            // will be, carried by a version). Devices always send versions, so
            // nothing is lost by ignoring the row value here.
            return Ok(false);
        }
        let existing: i64 = self.connection().query_row(
            "SELECT COUNT(*) FROM field_versions WHERE entity_type = ? AND entity_id = ? AND field = ?",
            params![entity_type, entity_id, field],
            |r| r.get(0),
        )?;
        if existing > 0 {
            // Versions exist but no head yet (arrived via sync): just recompute.
            self.recompute_head(entity_type, entity_id, field)?;
            return Ok(false);
        }
        let row = VersionRow {
            id: root_version_id(entity_type, entity_id, field, content),
            entity_type: entity_type.to_string(),
            entity_id: entity_id.to_string(),
            field: field.to_string(),
            parent_id: None,
            merge_parent_id: None,
            content: content.to_string(),
            context: None,
            conflict_kind: None,
            device_id: None,
            device_name: None,
            created_at,
            created_at_offset: crate::timezone::stamp_offset(),
            created_at_zone: crate::timezone::stamp_zone(),
            sync_received_at: None,
            published: false,
        };
        self.insert_version(&row)?;
        self.set_head(entity_type, entity_id, field, &row.id)?;
        Ok(true)
    }

    // ------------------------------------------------------------------
    // Low-level rows
    // ------------------------------------------------------------------

    /// Insert a version if its id is not known. Returns true when inserted.
    pub fn insert_version(&self, v: &VersionRow) -> VoiceResult<bool> {
        let device_bytes = v
            .device_id
            .as_deref()
            .and_then(|d| Uuid::parse_str(d).ok())
            .map(|u| u.as_bytes().to_vec());
        let n = self.connection().execute(
            r#"
            INSERT OR IGNORE INTO field_versions
            (id, entity_type, entity_id, field, parent_id, merge_parent_id, content, context,
             conflict_kind, device_id, device_name, created_at, sync_received_at, published,
             created_at_offset, created_at_zone)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
            params![
                v.id,
                v.entity_type,
                v.entity_id,
                v.field,
                v.parent_id,
                v.merge_parent_id,
                v.content,
                v.context,
                v.conflict_kind,
                device_bytes,
                v.device_name,
                v.created_at,
                v.sync_received_at,
                v.published as i64,
                v.created_at_offset,
                v.created_at_zone,
            ],
        )?;
        if n == 0 && v.published {
            // Already known here (derived locally); a device says an authored
            // version builds on it, so it must travel onward from here too.
            self.connection().execute(
                "UPDATE field_versions SET published = 1 WHERE id = ? AND published = 0",
                params![v.id],
            )?;
        }
        Ok(n > 0)
    }

    fn row_to_version(row: &rusqlite::Row) -> rusqlite::Result<VersionRow> {
        let device_bytes: Option<Vec<u8>> = row.get(9)?;
        Ok(VersionRow {
            id: row.get(0)?,
            entity_type: row.get(1)?,
            entity_id: row.get(2)?,
            field: row.get(3)?,
            parent_id: row.get(4)?,
            merge_parent_id: row.get(5)?,
            content: row.get(6)?,
            context: row.get(7)?,
            conflict_kind: row.get(8)?,
            device_id: device_bytes.and_then(|b| uuid_bytes_to_hex(&b).ok()),
            device_name: row.get(10)?,
            created_at: row.get(11)?,
            sync_received_at: row.get(12)?,
            published: row.get::<_, Option<i64>>(13)?.unwrap_or(0) != 0,
            created_at_offset: row.get::<_, Option<i64>>(14)?.and_then(|o| i32::try_from(o).ok()),
            created_at_zone: row.get(15)?,
        })
    }

    const VERSION_COLUMNS: &'static str = "id, entity_type, entity_id, field, parent_id, merge_parent_id, content, context, conflict_kind, device_id, device_name, created_at, sync_received_at, published, created_at_offset, created_at_zone";

    /// One version by id.
    pub fn get_version(&self, id: &[u8]) -> VoiceResult<Option<VersionRow>> {
        let sql = format!("SELECT {} FROM field_versions WHERE id = ?", Self::VERSION_COLUMNS);
        Ok(self
            .connection()
            .query_row(&sql, params![id], Self::row_to_version)
            .optional()?)
    }

    /// Every version of a field, oldest first.
    pub fn get_field_history(&self, entity_type: &str, entity_id: &str, field: &str) -> VoiceResult<Vec<VersionRow>> {
        let sql = format!(
            "SELECT {} FROM field_versions WHERE entity_type = ? AND entity_id = ? AND field = ? ORDER BY created_at, (parent_id IS NOT NULL), id",
            Self::VERSION_COLUMNS
        );
        let mut stmt = self.connection().prepare(&sql)?;
        let rows = stmt.query_map(params![entity_type, entity_id, field], Self::row_to_version)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    fn load_graph(&self, entity_type: &str, entity_id: &str, field: &str) -> VoiceResult<FieldGraph> {
        let versions = self
            .get_field_history(entity_type, entity_id, field)?
            .into_iter()
            .map(|v| (v.id.clone(), v))
            .collect();
        Ok(FieldGraph { versions })
    }

    /// Current head id of a field, if any.
    pub fn head_id(&self, entity_type: &str, entity_id: &str, field: &str) -> VoiceResult<Option<Vec<u8>>> {
        Ok(self
            .connection()
            .query_row(
                "SELECT head_id FROM field_heads WHERE entity_type = ? AND entity_id = ? AND field = ?",
                params![entity_type, entity_id, field],
                |r| r.get(0),
            )
            .optional()?)
    }

    /// Current head version of a field, if any.
    pub fn head_version(&self, entity_type: &str, entity_id: &str, field: &str) -> VoiceResult<Option<VersionRow>> {
        match self.head_id(entity_type, entity_id, field)? {
            Some(id) => self.get_version(&id),
            None => Ok(None),
        }
    }

    fn set_head(&self, entity_type: &str, entity_id: &str, field: &str, head: &[u8]) -> VoiceResult<()> {
        self.connection().execute(
            r#"
            INSERT INTO field_heads (entity_type, entity_id, field, head_id) VALUES (?, ?, ?, ?)
            ON CONFLICT(entity_type, entity_id, field) DO UPDATE SET head_id = excluded.head_id
            "#,
            params![entity_type, entity_id, field, head],
        )?;
        Ok(())
    }

    /// Versions in write order: `seq > cursor` (and `<= upto` when given).
    pub fn get_versions_after_seq(&self, cursor: i64, upto: Option<i64>, limit: i64) -> VoiceResult<Vec<(i64, VersionRow)>> {
        let sql = format!(
            "SELECT {}, seq FROM field_versions WHERE seq > ?1 AND seq <= ?2 AND (device_id IS NOT NULL OR parent_id IS NULL OR published = 1) ORDER BY seq LIMIT ?3",
            Self::VERSION_COLUMNS
        );
        let mut stmt = self.connection().prepare(&sql)?;
        let rows = stmt.query_map(params![cursor, upto.unwrap_or(i64::MAX), limit], |row| {
            let v = Self::row_to_version(row)?;
            // seq follows VERSION_COLUMNS, so it moves when that list grows
            let seq: Option<i64> = row.get(16)?;
            Ok((seq.unwrap_or(0), v))
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    // ------------------------------------------------------------------
    // Local writes
    // ------------------------------------------------------------------

    /// Record a local edit of a versioned field and make it the head.
    ///
    /// The new version's parent is the current head. Returns the version.
    /// A no-op (same content as the head) returns the head unchanged.
    pub fn set_field(
        &self,
        entity_type: &str,
        entity_id: &str,
        field: &str,
        content: &str,
        context: Option<String>,
    ) -> VoiceResult<VersionRow> {
        if field_kind(entity_type, field).is_none() {
            return Err(VoiceError::validation("field", format!("{}.{} is not a versioned field", entity_type, field)));
        }
        let head = self.head_version(entity_type, entity_id, field)?;
        if let Some(ref h) = head {
            if h.content == content && context.is_none() {
                return Ok(h.clone());
            }
        }
        let now = chrono::Utc::now().timestamp();
        let row = VersionRow {
            id: Uuid::now_v7().as_bytes().to_vec(),
            entity_type: entity_type.to_string(),
            entity_id: entity_id.to_string(),
            field: field.to_string(),
            parent_id: head.as_ref().map(|h| h.id.clone()),
            merge_parent_id: None,
            content: content.to_string(),
            context,
            conflict_kind: None,
            device_id: Some(get_this_device_id().simple().to_string()),
            device_name: get_this_device_name(),
            created_at: now,
            created_at_offset: crate::timezone::stamp_offset(),
            created_at_zone: crate::timezone::stamp_zone(),
            sync_received_at: None,
            published: false,
        };
        self.insert_version(&row)?;
        if let Some(parent) = &row.parent_id {
            self.publish_derived_ancestors(parent)?;
        }
        self.recompute_head(entity_type, entity_id, field)?;
        // A delete that did not see this edit loses to it (HEAD-6), on this
        // device just as on every device that receives the edit.
        if field != FIELD_DELETED && field_kind(entity_type, FIELD_DELETED).is_some() {
            self.recompute_head(entity_type, entity_id, FIELD_DELETED)?;
        }
        Ok(row)
    }

    /// Snapshot of an entity's non-deletion field heads, for tombstone context.
    /// First value of a field on a freshly created entity: a deterministic
    /// root (pre-existing data, no device, timestamp 0 for `modified_at`).
    /// If the field already has a history (e.g. a tag link re-added after
    /// removal) this is a normal edit instead.
    pub fn init_field(&self, entity_type: &str, entity_id: &str, field: &str, content: &str) -> VoiceResult<()> {
        if self.head_id(entity_type, entity_id, field)?.is_some() {
            self.set_field(entity_type, entity_id, field, content, None)?;
        } else {
            self.ensure_root_version(entity_type, entity_id, field, content, chrono::Utc::now().timestamp())?;
            if let Some(head) = self.head_version(entity_type, entity_id, field)? {
                self.apply_head_to_entity(entity_type, entity_id, field, &head)?;
            }
        }
        Ok(())
    }

    /// A derived version (merge or resurrection) that an authored version
    /// builds on must reach every device, or the authored version could never
    /// be completed there. Publishing bumps its `seq`, so it is sent before
    /// the child. Walks up through derived ancestors until an authored one.
    pub(crate) fn publish_derived_ancestors(&self, id: &[u8]) -> VoiceResult<()> {
        let mut current = Some(id.to_vec());
        let mut guard = 0;
        while let Some(cur) = current {
            guard += 1;
            if guard > 10_000 {
                break;
            }
            let v = match self.get_version(&cur)? {
                Some(v) => v,
                None => break,
            };
            if !v.is_derived() {
                break;
            }
            let n = self.connection().execute(
                "UPDATE field_versions SET published = 1 WHERE id = ? AND published = 0",
                params![cur],
            )?;
            if n == 0 {
                break; // already published, and so are its ancestors
            }
            // A merge has two parents; the merge_parent side is the other leaf
            if let Some(mp) = &v.merge_parent_id {
                self.publish_derived_ancestors(mp)?;
            }
            current = v.parent_id.clone();
        }
        Ok(())
    }

    pub fn entity_context(&self, entity_type: &str, entity_id: &str) -> VoiceResult<String> {
        let mut map = serde_json::Map::new();
        for field in fields_for_entity(entity_type) {
            if field == FIELD_DELETED {
                continue;
            }
            if let Some(h) = self.head_id(entity_type, entity_id, field)? {
                map.insert(field.to_string(), serde_json::Value::String(hex(&h)));
            }
        }
        Ok(serde_json::Value::Object(map).to_string())
    }

    /// Mark an entity deleted (a tombstone version carrying the context of what
    /// the deleting device saw). Returns false when already deleted.
    pub fn set_deleted(&self, entity_type: &str, entity_id: &str) -> VoiceResult<bool> {
        if let Some(h) = self.head_version(entity_type, entity_id, FIELD_DELETED)? {
            if h.content == "1" {
                return Ok(false);
            }
        }
        let context = self.entity_context(entity_type, entity_id)?;
        // The heads the delete saw must exist on every device for the
        // delete-versus-edit check to give the same answer everywhere
        if let Ok(serde_json::Value::Object(map)) = serde_json::from_str::<serde_json::Value>(&context) {
            for v in map.values() {
                if let Some(hex_id) = v.as_str() {
                    if let Ok(bytes) = hex_to_bytes(hex_id) {
                        self.publish_derived_ancestors(&bytes)?;
                    }
                }
            }
        }
        self.set_field(entity_type, entity_id, FIELD_DELETED, "1", Some(context))?;
        Ok(true)
    }

    /// Undelete an entity. Returns false when it was not deleted.
    pub fn set_undeleted(&self, entity_type: &str, entity_id: &str) -> VoiceResult<bool> {
        match self.head_version(entity_type, entity_id, FIELD_DELETED)? {
            Some(h) if h.content == "1" => {
                self.set_field(entity_type, entity_id, FIELD_DELETED, "0", None)?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    // ------------------------------------------------------------------
    // Head computation
    // ------------------------------------------------------------------

    /// Recompute the head of every touched field, deletion flags last so the
    /// delete-versus-edit check sees the freshest content heads.
    pub fn recompute_heads(&self, touched: &HashSet<(String, String, String)>) -> VoiceResult<RecomputeOutcome> {
        let mut total = RecomputeOutcome::default();
        let mut entities: HashSet<(String, String)> = HashSet::new();
        let mut ordered: Vec<&(String, String, String)> = touched.iter().collect();
        ordered.sort();
        // One field failing (typically a link whose note row has not arrived
        // yet) must not abort the batch: it is left without a head and picked
        // up by `recompute_headless_fields` once its dependencies exist.
        for (et, eid, field) in ordered.iter().filter(|(_, _, f)| f != FIELD_DELETED) {
            match self.recompute_head(et, eid, field) {
                Ok(o) => {
                    total.head_changed |= o.head_changed;
                    total.new_conflicts += o.new_conflicts;
                    total.resolved_conflicts += o.resolved_conflicts;
                }
                Err(e) => {
                    total.deferred += 1;
                    tracing::warn!("Deferred head of {} {} {}: {}", et, eid, field, e);
                }
            }
            entities.insert((et.clone(), eid.clone()));
        }
        for (et, eid, _) in ordered.iter().filter(|(_, _, f)| f == FIELD_DELETED) {
            entities.insert((et.clone(), eid.clone()));
        }
        let mut ents: Vec<_> = entities.into_iter().collect();
        ents.sort();
        for (et, eid) in ents {
            if field_kind(&et, FIELD_DELETED).is_some() {
                match self.recompute_head(&et, &eid, FIELD_DELETED) {
                    Ok(o) => {
                        total.head_changed |= o.head_changed;
                        total.new_conflicts += o.new_conflicts;
                        total.resolved_conflicts += o.resolved_conflicts;
                    }
                    Err(e) => {
                        total.deferred += 1;
                        tracing::warn!("Deferred head of {} {} deleted: {}", et, eid, e);
                    }
                }
            }
        }
        Ok(total)
    }

    /// Retry every field whose row write failed earlier (cheap: reads the
    /// small `field_deferred` table). Returns how many heads were set.
    pub fn recompute_deferred_fields(&self) -> VoiceResult<usize> {
        let mut stmt = self.connection().prepare(
            "SELECT entity_type, entity_id, field FROM field_deferred ORDER BY 1, 2, 3",
        )?;
        let fields: Vec<(String, String, String)> = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        self.recompute_fields(fields)
    }

    /// Give a head to every field that has versions but none yet (a version
    /// whose recompute failed before it could be recorded) and retry every
    /// deferred field. Scans the version table, so callers run it only when
    /// a batch had failures. Returns how many heads were set.
    pub fn recompute_headless_fields(&self) -> VoiceResult<usize> {
        let mut stmt = self.connection().prepare(
            r#"SELECT DISTINCT v.entity_type, v.entity_id, v.field FROM field_versions v
               WHERE NOT EXISTS (SELECT 1 FROM field_heads h
                                 WHERE h.entity_type = v.entity_type AND h.entity_id = v.entity_id AND h.field = v.field)
               UNION
               SELECT entity_type, entity_id, field FROM field_deferred
               ORDER BY 1, 2, 3"#,
        )?;
        let fields: Vec<(String, String, String)> = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        self.recompute_fields(fields)
    }

    fn recompute_fields(&self, fields: Vec<(String, String, String)>) -> VoiceResult<usize> {
        let mut done = 0;
        for (et, eid, field) in fields {
            match self.recompute_head(&et, &eid, &field) {
                Ok(_) => {
                    done += 1;
                    // The entity moved on: a tombstone that did not see this
                    // value must be re-evaluated (HEAD-6), as after any batch
                    if field != FIELD_DELETED && field_kind(&et, FIELD_DELETED).is_some() {
                        if let Err(e) = self.recompute_head(&et, &eid, FIELD_DELETED) {
                            tracing::debug!("Deleted head of {} {} still deferred: {}", et, eid, e);
                        }
                    }
                }
                Err(e) => tracing::debug!("Head of {} {} {} still deferred: {}", et, eid, field, e),
            }
        }
        Ok(done)
    }

    /// Recompute one field's head from its version graph, merging leaves as
    /// needed, then write the value into the entity's column.
    pub fn recompute_head(&self, entity_type: &str, entity_id: &str, field: &str) -> VoiceResult<RecomputeOutcome> {
        let kind = match field_kind(entity_type, field) {
            Some(k) => k,
            None => return Ok(RecomputeOutcome::default()),
        };
        let mut graph = self.load_graph(entity_type, entity_id, field)?;
        if graph.versions.is_empty() {
            return Ok(RecomputeOutcome::default());
        }
        let mut outcome = RecomputeOutcome::default();

        let mut complete = graph.complete_ids();
        if complete.is_empty() {
            // Only dangling versions (parents never received): treat them as
            // roots rather than hiding the data forever.
            complete = graph.versions.keys().cloned().collect();
        }

        // Fold the authored leaves pairwise, smallest ids first, until one
        // remains. Merge versions are deterministic (id = hash of parents and
        // content) so every device that holds the same authored versions
        // builds the same chain; stale merges from earlier partial states
        // stay in the table but never take part again.
        let mut chain: Vec<VersionRow> = graph.authored_leaves(&complete).into_iter().cloned().collect();
        if chain.is_empty() {
            chain = graph.leaves(&complete).into_iter().cloned().collect();
        }
        while chain.len() > 1 {
            let a = chain.remove(0);
            let b = chain.remove(0);
            let base_id = graph.lowest_common_ancestor(&a.id, &b.id);
            let base_content = base_id.as_ref().and_then(|id| graph.get(id)).map(|v| v.content.clone());
            let merged = merge_leaves(kind, base_content.as_deref(), &a, &b);
            let merge_row = VersionRow {
                id: merge_version_id(&a.id, &b.id, &merged.content),
                entity_type: entity_type.to_string(),
                entity_id: entity_id.to_string(),
                field: field.to_string(),
                parent_id: Some(a.id.clone()),
                merge_parent_id: Some(b.id.clone()),
                content: merged.content.clone(),
                context: None,
                conflict_kind: merged.conflict_kind.map(String::from),
                device_id: None,
                device_name: None,
                created_at: a.created_at.max(b.created_at),
                created_at_offset: crate::timezone::stamp_offset(),
                created_at_zone: crate::timezone::stamp_zone(),
                sync_received_at: None,
                published: false,
            };
            self.insert_version(&merge_row)?;
            graph.versions.insert(merge_row.id.clone(), merge_row.clone());
            // The merge goes back in id order so the fold is canonical
            let pos = chain.iter().position(|v| v.id > merge_row.id).unwrap_or(chain.len());
            chain.insert(pos, merge_row);
        }

        let head = match chain.into_iter().next() {
            Some(h) => h,
            None => return Ok(outcome),
        };

        // Delete-versus-edit: a tombstone that did not see the current heads
        // of the other fields loses to the edit and the entity is resurrected.
        // Decided before the head is written, so the row is written once
        // (writing "deleted" and then "alive" would publish the row again).
        let mut head = head;
        if kind == FieldKind::Deleted && head.content == "1" {
            if let Some(resurrect) = self.check_delete_vs_edit(entity_type, entity_id, &head)? {
                self.insert_version(&resurrect)?;
                graph.versions.insert(resurrect.id.clone(), resurrect.clone());
                head = resurrect;
            }
        }

        // Two devices moving tags under each other would form a cycle that
        // every hierarchy query loops on. The cycle is broken the same way on
        // every device: the tag with the largest id on the would-be cycle
        // keeps its move in history but gets a derived "no parent" head and a
        // conflict. Detected here so the row is written once.
        let mut cycle_loser_elsewhere: Option<String> = None;
        if entity_type == ENTITY_TAG && field == FIELD_PARENT && !head.content.is_empty() {
            if let Some(cycle) = self.tag_cycle_if_parent(entity_id, &head.content)? {
                let loser = cycle.iter().max().cloned().unwrap_or_default();
                if loser == entity_id {
                    let broken = VersionRow {
                        id: derived_id(&["cycle", entity_id, &hex(&head.id)]),
                        entity_type: entity_type.to_string(),
                        entity_id: entity_id.to_string(),
                        field: field.to_string(),
                        parent_id: Some(head.id.clone()),
                        merge_parent_id: None,
                        content: String::new(),
                        context: None,
                        conflict_kind: Some("scalar".to_string()),
                        device_id: None,
                        device_name: None,
                        created_at: head.created_at,
                        created_at_offset: crate::timezone::stamp_offset(),
                        created_at_zone: crate::timezone::stamp_zone(),
                        sync_received_at: None,
                        published: false,
                    };
                    self.insert_version(&broken)?;
                    graph.versions.insert(broken.id.clone(), broken.clone());
                    head = broken;
                } else {
                    cycle_loser_elsewhere = Some(loser);
                }
            }
        }

        // The row is written before the head is recorded: if the write fails
        // (a parent tag whose row has not arrived yet) the field is marked
        // deferred and retried after later batches, and the head still
        // matches the row in the meantime.
        if let Err(e) = self.apply_head_to_entity(entity_type, entity_id, field, &head) {
            self.connection().execute(
                "INSERT OR IGNORE INTO field_deferred (entity_type, entity_id, field) VALUES (?, ?, ?)",
                params![entity_type, entity_id, field],
            )?;
            return Err(e);
        }
        self.connection().execute(
            "DELETE FROM field_deferred WHERE entity_type = ? AND entity_id = ? AND field = ?",
            params![entity_type, entity_id, field],
        )?;
        let previous = self.head_id(entity_type, entity_id, field)?;
        if previous.as_deref() != Some(head.id.as_slice()) {
            self.set_head(entity_type, entity_id, field, &head.id)?;
            outcome.head_changed = true;
        }

        // The other tag on the cycle is the one that has to give way
        if let Some(loser) = cycle_loser_elsewhere {
            self.recompute_head(ENTITY_TAG, &loser, FIELD_PARENT)?;
        }

        // Every version that needed a human gets a conflict record here, whether
        // this device computed the merge or received it from a device, so the
        // same conflict is flagged everywhere with the same id.
        outcome.new_conflicts += self.ensure_conflict_records(&graph, &head.id)?;

        // Any open conflict whose merge version is now a strict ancestor of
        // the head has been dealt with (edited past, or accepted).
        outcome.resolved_conflicts += self.auto_resolve_conflicts(entity_type, entity_id, field, &graph)?;

        // The display caches list open conflicts, so they change when a
        // conflict is flagged or resolved even if the head value did not.
        if outcome.new_conflicts > 0 || outcome.resolved_conflicts > 0 {
            self.refresh_entity_caches(entity_type, entity_id);
        }

        Ok(outcome)
    }

    /// Rebuild the display caches that show an entity (and its conflicts).
    pub(crate) fn refresh_entity_caches(&self, entity_type: &str, entity_id: &str) {
        match entity_type {
            ENTITY_NOTE => {
                let _ = self.rebuild_note_cache(entity_id);
                let _ = self.rebuild_note_list_cache(entity_id);
            }
            ENTITY_NOTE_TAG => {
                if let Ok((note_hex, _)) = split_note_tag_entity_id(entity_id) {
                    let _ = self.rebuild_note_cache(&note_hex);
                    let _ = self.rebuild_note_list_cache(&note_hex);
                }
            }
            ENTITY_NOTE_ATTACHMENT => {
                if let Ok(id) = hex_to_bytes(entity_id) {
                    let note: Option<Vec<u8>> = self
                        .connection()
                        .query_row("SELECT note_id FROM note_attachments WHERE id = ?", params![id], |r| r.get(0))
                        .optional()
                        .ok()
                        .flatten();
                    if let Some(note_hex) = note.map(|n| hex(&n)) {
                        let _ = self.rebuild_note_cache(&note_hex);
                        let _ = self.rebuild_note_list_cache(&note_hex);
                    }
                }
            }
            // A tag's name and place are copied into the cache of every note
            // that carries it, so a rename or a move has to reach all of them.
            ENTITY_TAG => self.rebuild_caches_for_tag(entity_id),
            ENTITY_TRANSCRIPTION => self.rebuild_caches_for_transcription(entity_id),
            ENTITY_AUDIO_FILE => self.rebuild_caches_for_audio_file(entity_id),
            _ => {}
        }
    }

    /// Insert a conflict record for each version carrying a `conflict_kind`.
    fn ensure_conflict_records(&self, graph: &FieldGraph, head: &[u8]) -> VoiceResult<usize> {
        let mut inserted = 0;
        // Only merges in play (the head or its ancestors) are recorded: a merge
        // of a partial state that the fold no longer produces is not a
        // conflict anyone needs to see, and other devices never had it.
        let mut flagged: Vec<&VersionRow> = graph
            .versions
            .values()
            .filter(|v| v.conflict_kind.is_some())
            .filter(|v| v.id.as_slice() == head || graph.is_ancestor(&v.id, head))
            .collect();
        flagged.sort_by(|x, y| x.id.cmp(&y.id));
        for merge in flagged {
            let kind = merge.conflict_kind.clone().unwrap_or_default();
            let a = merge.parent_id.as_ref().and_then(|p| graph.get(p));
            let b = merge.merge_parent_id.as_ref().and_then(|p| graph.get(p));
            let (a, b, base) = match (a, b) {
                (Some(a), Some(b)) => (a, b, graph.lowest_common_ancestor(&a.id, &b.id)),
                (Some(a), None) => (a, merge, Some(a.id.clone())),
                _ => continue,
            };
            if self.record_conflict(merge, base.as_deref(), a, b, &kind)? {
                inserted += 1;
            }
        }
        Ok(inserted)
    }

    /// If making `parent` the parent of `tag` would close a cycle through the
    /// current tag rows, return the tags on that cycle (including `tag`).
    fn tag_cycle_if_parent(&self, tag: &str, parent: &str) -> VoiceResult<Option<Vec<String>>> {
        let mut chain = vec![tag.to_string()];
        let mut current = parent.to_string();
        for _ in 0..10_000 {
            if current == tag {
                return Ok(Some(chain));
            }
            if chain.contains(&current) {
                // An existing cycle elsewhere; not ours to break here
                return Ok(None);
            }
            chain.push(current.clone());
            let next: Option<Option<Vec<u8>>> = self
                .connection()
                .query_row(
                    "SELECT parent_id FROM tags WHERE id = ?",
                    params![hex_to_bytes(&current)?],
                    |r| r.get(0),
                )
                .optional()?;
            match next.flatten() {
                Some(p) => current = hex(&p),
                None => return Ok(None),
            }
        }
        Ok(None)
    }

    /// If `tombstone` (head of the deleted field, content "1") carries a context
    /// that is behind the current heads of the entity's other fields, build the
    /// deterministic resurrection version.
    fn check_delete_vs_edit(&self, entity_type: &str, entity_id: &str, tombstone: &VersionRow) -> VoiceResult<Option<VersionRow>> {
        let context: HashMap<String, String> = match &tombstone.context {
            Some(c) => serde_json::from_str(c).unwrap_or_default(),
            None => return Ok(None),
        };
        for field in fields_for_entity(entity_type) {
            if field == FIELD_DELETED {
                continue;
            }
            let head = match self.head_id(entity_type, entity_id, field)? {
                Some(h) => h,
                None => continue,
            };
            let seen_hex = match context.get(field) {
                Some(s) => s.clone(),
                None => continue, // field did not exist when deleting; ignore
            };
            if hex(&head) == seen_hex {
                continue;
            }
            // The field moved on after (or concurrently with) the delete.
            let graph = self.load_graph(entity_type, entity_id, field)?;
            let seen_bytes: Vec<u8> = (0..seen_hex.len())
                .step_by(2)
                .filter_map(|i| u8::from_str_radix(&seen_hex[i..i + 2], 16).ok())
                .collect();
            // Unknown "seen" version: it has not arrived yet, so nothing can be
            // said; the check runs again when it does (its arrival touches the
            // entity). Deciding "moved on" here would differ between devices.
            let moved_on = graph.versions.contains_key(&seen_bytes) && graph.is_ancestor(&seen_bytes, &head);
            if moved_on {
                return Ok(Some(VersionRow {
                    id: resurrect_version_id(&tombstone.id),
                    entity_type: entity_type.to_string(),
                    entity_id: entity_id.to_string(),
                    field: FIELD_DELETED.to_string(),
                    parent_id: Some(tombstone.id.clone()),
                    merge_parent_id: None,
                    content: "0".to_string(),
                    context: None,
                    conflict_kind: Some("delete".to_string()),
                    device_id: None,
                    device_name: None,
                    created_at: tombstone.created_at,
                    created_at_offset: crate::timezone::stamp_offset(),
                    created_at_zone: crate::timezone::stamp_zone(),
                    sync_received_at: None,
                published: false,
                }));
            }
        }
        Ok(None)
    }

    // ------------------------------------------------------------------
    // Conflicts
    // ------------------------------------------------------------------

    fn record_conflict(
        &self,
        merge: &VersionRow,
        base_id: Option<&[u8]>,
        a: &VersionRow,
        b: &VersionRow,
        kind: &str,
    ) -> VoiceResult<bool> {
        let id = conflict_id_for(&merge.id);
        let dev = |d: &Option<String>| d.as_deref().and_then(|s| Uuid::parse_str(s).ok()).map(|u| u.as_bytes().to_vec());
        let n = self.connection().execute(
            r#"
            INSERT OR IGNORE INTO field_conflicts
            (id, entity_type, entity_id, field, kind, base_version_id, version_a_id, version_b_id, merge_version_id,
             device_a_id, device_a_name, device_b_id, device_b_name, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
            params![
                id,
                merge.entity_type,
                merge.entity_id,
                merge.field,
                kind,
                base_id.map(|b| b.to_vec()),
                a.id,
                b.id,
                merge.id,
                dev(&a.device_id),
                a.device_name,
                dev(&b.device_id),
                b.device_name,
                chrono::Utc::now().timestamp(),
            ],
        )?;
        Ok(n > 0)
    }

    fn auto_resolve_conflicts(&self, entity_type: &str, entity_id: &str, field: &str, graph: &FieldGraph) -> VoiceResult<usize> {
        let head = match self.head_id(entity_type, entity_id, field)? {
            Some(h) => h,
            None => return Ok(0),
        };
        let open: Vec<(Vec<u8>, Vec<u8>)> = {
            let mut stmt = self.connection().prepare(
                "SELECT id, merge_version_id FROM field_conflicts WHERE entity_type = ? AND entity_id = ? AND field = ? AND resolved_at IS NULL",
            )?;
            let rows = stmt.query_map(params![entity_type, entity_id, field], |r| Ok((r.get(0)?, r.get(1)?)))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        let mut resolved = 0;
        // A conflict is done when its merge is no longer the head: either the
        // user moved past it (edit or accept) or more versions arrived and the
        // fold was rebuilt from the authored leaves (any remaining disagreement
        // is recorded again on the new merge).
        let _ = graph;
        for (cid, merge_id) in open {
            if merge_id != head {
                self.connection().execute(
                    "UPDATE field_conflicts SET resolved_at = ? WHERE id = ?",
                    params![chrono::Utc::now().timestamp(), cid],
                )?;
                resolved += 1;
            }
        }
        Ok(resolved)
    }

    fn row_to_conflict(row: &rusqlite::Row) -> rusqlite::Result<ConflictRow> {
        let id: Vec<u8> = row.get(0)?;
        let base: Option<Vec<u8>> = row.get(5)?;
        let a: Vec<u8> = row.get(6)?;
        let b: Vec<u8> = row.get(7)?;
        let m: Vec<u8> = row.get(8)?;
        let da: Option<Vec<u8>> = row.get(9)?;
        let db_: Option<Vec<u8>> = row.get(11)?;
        Ok(ConflictRow {
            id: hex(&id),
            entity_type: row.get(1)?,
            entity_id: row.get(2)?,
            field: row.get(3)?,
            kind: row.get(4)?,
            base_version_id: base.map(|b| hex(&b)),
            version_a_id: hex(&a),
            version_b_id: hex(&b),
            merge_version_id: hex(&m),
            device_a_id: da.and_then(|b| uuid_bytes_to_hex(&b).ok()),
            device_a_name: row.get(10)?,
            device_b_id: db_.and_then(|b| uuid_bytes_to_hex(&b).ok()),
            device_b_name: row.get(12)?,
            created_at: row.get(13)?,
            resolved_at: row.get(14)?,
        })
    }

    const CONFLICT_COLUMNS: &'static str = "id, entity_type, entity_id, field, kind, base_version_id, version_a_id, version_b_id, merge_version_id, device_a_id, device_a_name, device_b_id, device_b_name, created_at, resolved_at";

    /// All conflicts, newest first.
    /// Largest rowid in `field_conflicts` (0 when empty); pair with
    /// `count_conflicts_after_rowid` to count records created by a batch.
    pub fn max_conflict_rowid(&self) -> VoiceResult<i64> {
        Ok(self
            .connection()
            .query_row("SELECT COALESCE(MAX(rowid), 0) FROM field_conflicts", [], |r| r.get(0))?)
    }

    pub fn count_conflicts_after_rowid(&self, rowid: i64) -> VoiceResult<i64> {
        Ok(self
            .connection()
            .query_row("SELECT COUNT(*) FROM field_conflicts WHERE rowid > ?", params![rowid], |r| r.get(0))?)
    }

    pub fn get_conflicts(&self, include_resolved: bool) -> VoiceResult<Vec<ConflictRow>> {
        let sql = format!(
            "SELECT {} FROM field_conflicts {} ORDER BY created_at DESC, id",
            Self::CONFLICT_COLUMNS,
            if include_resolved { "" } else { "WHERE resolved_at IS NULL" }
        );
        let mut stmt = self.connection().prepare(&sql)?;
        let rows = stmt.query_map([], Self::row_to_conflict)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Unresolved conflicts of one entity.
    pub fn get_entity_conflicts(&self, entity_type: &str, entity_id: &str) -> VoiceResult<Vec<ConflictRow>> {
        let sql = format!(
            "SELECT {} FROM field_conflicts WHERE entity_type = ? AND entity_id = ? AND resolved_at IS NULL ORDER BY created_at DESC, id",
            Self::CONFLICT_COLUMNS
        );
        let mut stmt = self.connection().prepare(&sql)?;
        let rows = stmt.query_map(params![entity_type, entity_id], Self::row_to_conflict)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// One conflict by full id or unique prefix.
    pub fn get_conflict(&self, id_or_prefix: &str) -> VoiceResult<Option<ConflictRow>> {
        let all = self.get_conflicts(true)?;
        let matches: Vec<ConflictRow> = all.into_iter().filter(|c| c.id.starts_with(id_or_prefix)).collect();
        match matches.len() {
            0 => Ok(None),
            1 => Ok(matches.into_iter().next()),
            _ => Err(VoiceError::validation("conflict_id", "prefix is ambiguous")),
        }
    }

    /// Unresolved conflict counts by kind, plus "total".
    pub fn get_unresolved_conflict_counts(&self) -> VoiceResult<HashMap<String, i64>> {
        let mut counts: HashMap<String, i64> = HashMap::new();
        let mut stmt = self
            .connection()
            .prepare("SELECT kind, COUNT(*) FROM field_conflicts WHERE resolved_at IS NULL GROUP BY kind")?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?;
        let mut total = 0;
        for row in rows {
            let (kind, n) = row?;
            total += n;
            counts.insert(kind, n);
        }
        for kind in ["text", "scalar", "flags", "membership", "delete"] {
            counts.entry(kind.to_string()).or_insert(0);
        }
        counts.insert("total".to_string(), total);
        Ok(counts)
    }

    /// Accept a merge as-is: a new version identical to the merge, descending
    /// from it, which resolves the conflict here and on every device.
    pub fn accept_conflict(&self, conflict_id: &str) -> VoiceResult<bool> {
        let c = match self.get_conflict(conflict_id)? {
            Some(c) => c,
            None => return Ok(false),
        };
        if c.resolved_at.is_some() {
            return Ok(false);
        }
        let merge_bytes = hex_to_bytes(&c.merge_version_id)?;
        let head = self.head_version(&c.entity_type, &c.entity_id, &c.field)?;
        let head = match head {
            Some(h) => h,
            None => return Ok(false),
        };
        if head.id != merge_bytes {
            // The field already moved past the merge; recompute resolves it.
            self.recompute_head(&c.entity_type, &c.entity_id, &c.field)?;
            return Ok(true);
        }
        let accept = VersionRow {
            id: accept_version_id(&merge_bytes),
            entity_type: c.entity_type.clone(),
            entity_id: c.entity_id.clone(),
            field: c.field.clone(),
            parent_id: Some(merge_bytes.clone()),
            merge_parent_id: None,
            content: head.content.clone(),
            context: None,
            conflict_kind: None,
            device_id: Some(get_this_device_id().simple().to_string()),
            device_name: get_this_device_name(),
            created_at: chrono::Utc::now().timestamp(),
            created_at_offset: crate::timezone::stamp_offset(),
            created_at_zone: crate::timezone::stamp_zone(),
            sync_received_at: None,
            published: false,
        };
        self.insert_version(&accept)?;
        self.publish_derived_ancestors(&merge_bytes)?;
        self.recompute_head(&c.entity_type, &c.entity_id, &c.field)?;
        Ok(true)
    }

    /// Resolve a conflict with user-supplied content: a normal edit of the field.
    pub fn resolve_conflict_with_content(&self, conflict_id: &str, content: &str) -> VoiceResult<bool> {
        let c = match self.get_conflict(conflict_id)? {
            Some(c) => c,
            None => return Ok(false),
        };
        if c.resolved_at.is_some() {
            return Ok(false);
        }
        self.set_field(&c.entity_type, &c.entity_id, &c.field, content, None)?;
        // set_field recomputes; if the content equalled the head exactly, the
        // conflict is still open and needs an explicit accept.
        if self.get_conflict(conflict_id)?.map(|c| c.resolved_at.is_some()).unwrap_or(false) {
            Ok(true)
        } else {
            self.accept_conflict(conflict_id)
        }
    }

    /// Every unresolved conflict that concerns a note: its own fields, its
    /// tag links, its attachments, and the transcriptions attached to it.
    pub fn get_note_conflicts(&self, note_id: &str) -> VoiceResult<Vec<ConflictRow>> {
        let resolved_id = self.resolve_note_id(note_id)?;
        let note_bytes = hex_to_bytes(&resolved_id)?;
        let sql = format!(
            r#"SELECT {cols} FROM field_conflicts c
               WHERE c.resolved_at IS NULL AND (
                 (c.entity_type = 'note' AND c.entity_id = ?1)
                 OR (c.entity_type = 'note_tag' AND c.entity_id LIKE ?2)
                 OR (c.entity_type = 'note_attachment' AND c.entity_id IN (
                        SELECT lower(hex(na.id)) FROM note_attachments na WHERE na.note_id = ?3))
                 OR (c.entity_type = 'transcription' AND c.entity_id IN (
                        SELECT lower(hex(t.id)) FROM transcriptions t
                        JOIN note_attachments na ON na.attachment_id = t.audio_file_id
                        WHERE na.note_id = ?3 AND na.attachment_type = 'audio_file'))
               )
               ORDER BY c.created_at DESC, c.id"#,
            cols = Self::CONFLICT_COLUMNS
        );
        let mut stmt = self.connection().prepare(&sql)?;
        let rows = stmt.query_map(params![resolved_id, format!("{}:%", resolved_id), note_bytes], Self::row_to_conflict)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Kinds of unresolved conflict that concern a note: its own fields, its
    /// tag links, its attachments, and the transcriptions attached to it.
    pub fn get_note_conflict_types(&self, note_id: &str) -> VoiceResult<Vec<String>> {
        let resolved_id = self.resolve_note_id(note_id)?;
        let mut kinds: Vec<String> = Vec::new();
        for c in self.get_entity_conflicts(ENTITY_NOTE, &resolved_id)? {
            let k = match c.kind.as_str() {
                "text" => "content",
                "delete" => "delete",
                other => other,
            };
            if !kinds.iter().any(|x| x == k) {
                kinds.push(k.to_string());
            }
        }
        let tag_links: i64 = self.connection().query_row(
            "SELECT COUNT(*) FROM field_conflicts WHERE entity_type = 'note_tag' AND entity_id LIKE ? AND resolved_at IS NULL",
            params![format!("{}:%", resolved_id)],
            |r| r.get(0),
        )?;
        if tag_links > 0 {
            kinds.push("tag".to_string());
        }
        let attachments: i64 = self.connection().query_row(
            r#"SELECT COUNT(*) FROM field_conflicts c
               JOIN note_attachments na ON lower(hex(na.id)) = c.entity_id
               WHERE c.entity_type = 'note_attachment' AND na.note_id = ? AND c.resolved_at IS NULL"#,
            params![hex_to_bytes(&resolved_id)?],
            |r| r.get(0),
        )?;
        if attachments > 0 {
            kinds.push("attachment".to_string());
        }
        let transcriptions: i64 = self.connection().query_row(
            r#"SELECT COUNT(*) FROM field_conflicts c
               JOIN transcriptions t ON lower(hex(t.id)) = c.entity_id
               JOIN note_attachments na ON na.attachment_id = t.audio_file_id AND na.deleted_at IS NULL
               WHERE c.entity_type = 'transcription' AND na.note_id = ? AND c.resolved_at IS NULL"#,
            params![hex_to_bytes(&resolved_id)?],
            |r| r.get(0),
        )?;
        if transcriptions > 0 {
            kinds.push("transcription".to_string());
        }
        Ok(kinds)
    }

    // ------------------------------------------------------------------
    // Denormalisation into the entity tables
    // ------------------------------------------------------------------

    /// Write every head of an entity into its row (after the row was created
    /// or replaced by a sync).
    pub fn reapply_entity_heads(&self, entity_type: &str, entity_id: &str) -> VoiceResult<()> {
        for field in fields_for_entity(entity_type) {
            if let Some(head) = self.head_version(entity_type, entity_id, field)? {
                self.apply_head_to_entity(entity_type, entity_id, field, &head)?;
            }
        }
        Ok(())
    }

    /// Record which timezone a timestamp was written in.
    ///
    /// The `WHERE <stamp> = ?` clause means this does nothing when the update
    /// that came before kept an older value, so a zone never ends up
    /// describing a timestamp it did not arrive with.
    fn stamp_zone_by_id(&self, table: &str, stamp: &str, id: &[u8], ts: i64, head: &VersionRow) -> VoiceResult<()> {
        if ts <= 0 {
            return Ok(());
        }
        let sql = format!(
            "UPDATE {table} SET {stamp}_offset = ?, {stamp}_zone = ? WHERE id = ? AND {stamp} = ?"
        );
        self.connection()
            .execute(&sql, params![head.created_at_offset, head.created_at_zone, id, ts])?;
        Ok(())
    }

    /// The same for the note-tag link, which is keyed by its two ends.
    fn stamp_zone_note_tag(&self, stamp: &str, note: &[u8], tag: &[u8], ts: i64, head: &VersionRow) -> VoiceResult<()> {
        if ts <= 0 {
            return Ok(());
        }
        let sql = format!(
            "UPDATE note_tags SET {stamp}_offset = ?, {stamp}_zone = ? WHERE note_id = ? AND tag_id = ? AND {stamp} = ?"
        );
        self.connection().execute(
            &sql,
            params![head.created_at_offset, head.created_at_zone, note, tag, ts],
        )?;
        Ok(())
    }

    /// Write a head value into the column the rest of the application reads.
    pub(crate) fn apply_head_to_entity(&self, entity_type: &str, entity_id: &str, field: &str, head: &VersionRow) -> VoiceResult<()> {
        let conn = self.connection();
        // Hash roots (no parent, no device) describe data that already existed
        // and must not look like fresh edits; anything a device wrote is one.
        let ts = if head.parent_id.is_some() || head.device_id.is_some() { head.created_at } else { 0 };
        match (entity_type, field) {
            (ENTITY_NOTE, FIELD_CONTENT) => {
                let id = hex_to_bytes(entity_id)?;
                conn.execute(
                    "UPDATE notes SET content = ?, modified_at = NULLIF(MAX(COALESCE(modified_at, 0), ?), 0) WHERE id = ?",
                    params![head.content, ts, id],
                )?;
                self.stamp_zone_by_id("notes", "modified_at", &id, ts, head)?;
                let _ = self.rebuild_note_list_cache(entity_id);
                let _ = self.rebuild_note_cache(entity_id);
            }
            (ENTITY_NOTE, FIELD_DELETED) => {
                let id = hex_to_bytes(entity_id)?;
                if head.content == "1" {
                    conn.execute(
                        "UPDATE notes SET deleted_at = COALESCE(deleted_at, ?), modified_at = NULLIF(MAX(COALESCE(modified_at, 0), ?), 0) WHERE id = ?",
                        params![ts, ts, id],
                    )?;
                    self.stamp_zone_by_id("notes", "deleted_at", &id, ts, head)?;
                    self.stamp_zone_by_id("notes", "modified_at", &id, ts, head)?;
                } else {
                    conn.execute(
                        "UPDATE notes SET deleted_at = NULL, deleted_at_offset = NULL, deleted_at_zone = NULL, modified_at = NULLIF(MAX(COALESCE(modified_at, 0), ?), 0) WHERE id = ?",
                        params![ts, id],
                    )?;
                    self.stamp_zone_by_id("notes", "modified_at", &id, ts, head)?;
                    let _ = self.rebuild_note_list_cache(entity_id);
                    let _ = self.rebuild_note_cache(entity_id);
                }
            }
            (ENTITY_TRANSCRIPTION, FIELD_CONTENT) | (ENTITY_TRANSCRIPTION, FIELD_STATE) => {
                let id = hex_to_bytes(entity_id)?;
                let column = if field == FIELD_CONTENT { "content" } else { "state" };
                conn.execute(
                    &format!("UPDATE transcriptions SET {} = ?, modified_at = NULLIF(MAX(COALESCE(modified_at, 0), ?), 0) WHERE id = ?", column),
                    params![head.content, ts, id],
                )?;
                self.stamp_zone_by_id("transcriptions", "modified_at", &id, ts, head)?;
                self.rebuild_caches_for_transcription(entity_id);
            }
            (ENTITY_TRANSCRIPTION, FIELD_DELETED) => {
                let id = hex_to_bytes(entity_id)?;
                if head.content == "1" {
                    conn.execute(
                        "UPDATE transcriptions SET deleted_at = COALESCE(deleted_at, ?), modified_at = NULLIF(MAX(COALESCE(modified_at, 0), ?), 0) WHERE id = ?",
                        params![ts, ts, id],
                    )?;
                    self.stamp_zone_by_id("transcriptions", "deleted_at", &id, ts, head)?;
                    self.stamp_zone_by_id("transcriptions", "modified_at", &id, ts, head)?;
                } else {
                    conn.execute(
                        "UPDATE transcriptions SET deleted_at = NULL, deleted_at_offset = NULL, deleted_at_zone = NULL, modified_at = NULLIF(MAX(COALESCE(modified_at, 0), ?), 0) WHERE id = ?",
                        params![ts, id],
                    )?;
                    self.stamp_zone_by_id("transcriptions", "modified_at", &id, ts, head)?;
                }
                self.rebuild_caches_for_transcription(entity_id);
            }
            (ENTITY_NOTE, FIELD_PRIMARY_ATTACHMENT) => {
                // The head holds the attachment's id, or nothing at all when
                // the user has not chosen one and the first recording stands
                // for the note.
                let id = hex_to_bytes(entity_id)?;
                let chosen = hex_to_bytes(&head.content).ok();
                conn.execute(
                    "UPDATE notes SET primary_attachment_id = ?, modified_at = NULLIF(MAX(COALESCE(modified_at, 0), ?), 0) WHERE id = ?",
                    params![chosen, ts, id],
                )?;
                let _ = self.rebuild_note_list_cache(entity_id);
                let _ = self.rebuild_note_cache(entity_id);
            }
            (ENTITY_AUDIO_FILE, FIELD_PRIMARY_TRANSCRIPTION) => {
                let id = hex_to_bytes(entity_id)?;
                let chosen = hex_to_bytes(&head.content).ok();
                conn.execute(
                    "UPDATE audio_files SET primary_transcription_id = ?, modified_at = NULLIF(MAX(COALESCE(modified_at, 0), ?), 0) WHERE id = ?",
                    params![chosen, ts, id],
                )?;
                self.rebuild_caches_for_audio_file(entity_id);
            }
            (ENTITY_AUDIO_FILE, FIELD_SUMMARY) => {
                let id = hex_to_bytes(entity_id)?;
                conn.execute(
                    "UPDATE audio_files SET summary = ?, modified_at = NULLIF(MAX(COALESCE(modified_at, 0), ?), 0) WHERE id = ?",
                    params![head.content, ts, id],
                )?;
                self.stamp_zone_by_id("audio_files", "modified_at", &id, ts, head)?;
                self.rebuild_caches_for_audio_file(entity_id);
            }
            (ENTITY_AUDIO_FILE, FIELD_DELETED) => {
                let id = hex_to_bytes(entity_id)?;
                if head.content == "1" {
                    conn.execute(
                        "UPDATE audio_files SET deleted_at = COALESCE(deleted_at, ?), modified_at = NULLIF(MAX(COALESCE(modified_at, 0), ?), 0) WHERE id = ?",
                        params![ts, ts, id],
                    )?;
                    self.stamp_zone_by_id("audio_files", "deleted_at", &id, ts, head)?;
                    self.stamp_zone_by_id("audio_files", "modified_at", &id, ts, head)?;
                } else {
                    conn.execute(
                        "UPDATE audio_files SET deleted_at = NULL, deleted_at_offset = NULL, deleted_at_zone = NULL, modified_at = NULLIF(MAX(COALESCE(modified_at, 0), ?), 0) WHERE id = ?",
                        params![ts, id],
                    )?;
                    self.stamp_zone_by_id("audio_files", "modified_at", &id, ts, head)?;
                }
            }
            (ENTITY_TAG, FIELD_NAME) => {
                let id = hex_to_bytes(entity_id)?;
                conn.execute(
                    "UPDATE tags SET name = ?, modified_at = NULLIF(MAX(COALESCE(modified_at, 0), ?), 0) WHERE id = ?",
                    params![head.content, ts, id],
                )?;
                self.stamp_zone_by_id("tags", "modified_at", &id, ts, head)?;
                // Every note carrying this tag keeps a copy of its name, so
                // all of them are rebuilt; otherwise a renamed tag goes on
                // showing its old name on every note until something else
                // happens to that note.
                self.rebuild_caches_for_tag(entity_id);
            }
            (ENTITY_TAG, FIELD_PARENT) => {
                let id = hex_to_bytes(entity_id)?;
                let parent: Option<Vec<u8>> = if head.content.is_empty() { None } else { Some(hex_to_bytes(&head.content)?) };
                conn.execute(
                    "UPDATE tags SET parent_id = ?, modified_at = NULLIF(MAX(COALESCE(modified_at, 0), ?), 0) WHERE id = ?",
                    params![parent, ts, id],
                )?;
                self.stamp_zone_by_id("tags", "modified_at", &id, ts, head)?;
                // A move changes the path of this tag and of every tag under
                // it, so the notes of the whole subtree are rebuilt.
                self.rebuild_caches_for_tag(entity_id);
            }
            (ENTITY_TAG, FIELD_DELETED) => {
                let id = hex_to_bytes(entity_id)?;
                if head.content == "1" {
                    conn.execute(
                        "UPDATE tags SET deleted_at = COALESCE(deleted_at, ?), modified_at = NULLIF(MAX(COALESCE(modified_at, 0), ?), 0) WHERE id = ?",
                        params![ts, ts, id],
                    )?;
                    self.stamp_zone_by_id("tags", "deleted_at", &id, ts, head)?;
                    self.stamp_zone_by_id("tags", "modified_at", &id, ts, head)?;
                } else {
                    conn.execute(
                        "UPDATE tags SET deleted_at = NULL, deleted_at_offset = NULL, deleted_at_zone = NULL, modified_at = NULLIF(MAX(COALESCE(modified_at, 0), ?), 0) WHERE id = ?",
                        params![ts, id],
                    )?;
                    self.stamp_zone_by_id("tags", "modified_at", &id, ts, head)?;
                }
                // Deleted or recovered, the notes that carry it show it
                // differently, so their caches are rebuilt either way.
                self.rebuild_caches_for_tag(entity_id);
            }
            (ENTITY_NOTE_TAG, FIELD_ACTIVE) => {
                let (note_hex, tag_hex) = split_note_tag_entity_id(entity_id)?;
                let note = hex_to_bytes(&note_hex)?;
                let tag = hex_to_bytes(&tag_hex)?;
                let deleted_at: Option<i64> = if head.content == "1" { None } else { Some(ts) };
                // A root link (ts 0) is still created now, not at the epoch
                let created = if ts > 0 { ts } else { chrono::Utc::now().timestamp() };
                conn.execute(
                    r#"
                    INSERT INTO note_tags (note_id, tag_id, created_at, modified_at, deleted_at)
                    VALUES (?, ?, ?, NULLIF(?, 0), ?)
                    ON CONFLICT(note_id, tag_id) DO UPDATE SET
                        deleted_at = excluded.deleted_at,
                        modified_at = NULLIF(MAX(COALESCE(note_tags.modified_at, 0), COALESCE(excluded.modified_at, 0)), 0)
                    "#,
                    params![note, tag, created, ts, deleted_at],
                )?;
                self.stamp_zone_note_tag("created_at", &note, &tag, created, head)?;
                self.stamp_zone_note_tag("modified_at", &note, &tag, ts, head)?;
                if let Some(gone) = deleted_at {
                    self.stamp_zone_note_tag("deleted_at", &note, &tag, gone, head)?;
                }
                conn.execute(
                    "UPDATE notes SET modified_at = NULLIF(MAX(COALESCE(modified_at, 0), ?), 0) WHERE id = ?",
                    params![ts, note],
                )?;
                self.stamp_zone_by_id("notes", "modified_at", &note, ts, head)?;
                let _ = self.rebuild_note_cache(&note_hex);
                let _ = self.rebuild_note_list_cache(&note_hex);
            }
            (ENTITY_NOTE_ATTACHMENT, FIELD_ACTIVE) => {
                let id = hex_to_bytes(entity_id)?;
                let deleted_at: Option<i64> = if head.content == "1" { None } else { Some(ts) };
                conn.execute(
                    "UPDATE note_attachments SET deleted_at = ?, modified_at = NULLIF(MAX(COALESCE(modified_at, 0), ?), 0) WHERE id = ?",
                    params![deleted_at, ts, id],
                )?;
                if let Some(gone) = deleted_at {
                    self.stamp_zone_by_id("note_attachments", "deleted_at", &id, gone, head)?;
                }
                self.stamp_zone_by_id("note_attachments", "modified_at", &id, ts, head)?;
                let note: Option<Vec<u8>> = conn
                    .query_row("SELECT note_id FROM note_attachments WHERE id = ?", params![id], |r| r.get(0))
                    .optional()?;
                if let Some(n) = note {
                    if let Ok(nh) = uuid_bytes_to_hex(&n) {
                        let _ = self.rebuild_note_cache(&nh);
                    }
                }
            }
            (ENTITY_SETTING, FIELD_VALUE) => {
                conn.execute(
                    r#"
                    INSERT INTO synced_settings (key, value, modified_at) VALUES (?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET value = excluded.value, modified_at = excluded.modified_at
                    "#,
                    params![entity_id, head.content, ts],
                )?;
            }
            (ENTITY_DEVICE, field) => {
                // Every card field is a column of the same name; the list is
                // closed, so the column name never comes from data.
                let column = match field {
                    FIELD_NAME => "name",
                    FIELD_CERTIFICATE_FINGERPRINT => "certificate_fingerprint",
                    FIELD_ADDRESSES => "addresses",
                    FIELD_LISTENS => "listens",
                    FIELD_KEY_HASH => "key_hash",
                    FIELD_REVOKED => "revoked",
                    FIELD_APPLICATION => "application",
                    _ => return Ok(()),
                };
                // A revocation is one way (CARD-2): once the column says "1",
                // no version, however it arrived, writes "0" over it. The
                // Membership kind settles concurrent writes the same way.
                let set = if field == FIELD_REVOKED {
                    "revoked = CASE WHEN devices.revoked = '1' THEN '1' ELSE excluded.revoked END".to_string()
                } else {
                    format!("{col} = excluded.{col}", col = column)
                };
                conn.execute(
                    &format!(
                        "INSERT INTO devices (device_id, {col}, modified_at) VALUES (?, ?, ?)
                         ON CONFLICT(device_id) DO UPDATE SET {set}, modified_at = excluded.modified_at",
                        col = column,
                        set = set
                    ),
                    params![entity_id, head.content, ts],
                )?;
            }
            _ => {}
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Device cards (CARD-1..CARD-4): one per device of the account, each
    // field a version like any other, so the list of devices travels.
    // ------------------------------------------------------------------

    /// One device's card, as the `devices` table holds it.
    pub fn get_device_card(&self, device_id: &str) -> VoiceResult<Option<DeviceCard>> {
        Ok(self
            .connection()
            .query_row(
                "SELECT device_id, name, certificate_fingerprint, addresses, listens, key_hash, revoked, application
                 FROM devices WHERE device_id = ?",
                params![device_id],
                DeviceCard::from_row,
            )
            .optional()?)
    }

    /// Every card of the account, by device name.
    pub fn list_device_cards(&self) -> VoiceResult<Vec<DeviceCard>> {
        let mut stmt = self.connection().prepare(
            "SELECT device_id, name, certificate_fingerprint, addresses, listens, key_hash, revoked, application
             FROM devices ORDER BY name, device_id",
        )?;
        let rows = stmt.query_map([], DeviceCard::from_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Write or update the device's own card. A field with no history yet
    /// gets a deterministic root (VER-3), the same one a device that admits
    /// this device writes, so the two histories are one; a field that has a
    /// history gets an authored version when the value differs and nothing
    /// otherwise (VER-1). `revoked` is never written here: see
    /// [`Database::revoke_device`].
    pub fn write_device_card(&self, card: &DeviceCard) -> VoiceResult<()> {
        let id = card.device_id.as_str();
        self.init_field(ENTITY_DEVICE, id, FIELD_NAME, &card.name)?;
        self.init_field(ENTITY_DEVICE, id, FIELD_CERTIFICATE_FINGERPRINT, &card.certificate_fingerprint)?;
        self.init_field(ENTITY_DEVICE, id, FIELD_ADDRESSES, &card.addresses)?;
        self.init_field(ENTITY_DEVICE, id, FIELD_LISTENS, &card.listens)?;
        self.init_field(ENTITY_DEVICE, id, FIELD_KEY_HASH, &card.key_hash)?;
        self.init_field(ENTITY_DEVICE, id, FIELD_APPLICATION, &card.application)?;
        if self.head_id(ENTITY_DEVICE, id, FIELD_REVOKED)?.is_none() {
            self.init_field(ENTITY_DEVICE, id, FIELD_REVOKED, "0")?;
        }
        Ok(())
    }

    /// Write another device's card, as pairing does (CARD-3): each field
    /// becomes a deterministic root when it has no history yet, so the
    /// device's own later writes build on it and two devices that learn the
    /// same card write the same versions; nothing here conflicts with what
    /// the device says about itself. `listens` and `addresses` are the
    /// owner's alone and are only given their empty roots.
    pub fn admit_device_card(&self, card: &DeviceCard) -> VoiceResult<()> {
        let id = card.device_id.as_str();
        self.init_field(ENTITY_DEVICE, id, FIELD_NAME, &card.name)?;
        self.init_field(ENTITY_DEVICE, id, FIELD_CERTIFICATE_FINGERPRINT, &card.certificate_fingerprint)?;
        self.init_field(ENTITY_DEVICE, id, FIELD_KEY_HASH, &card.key_hash)?;
        self.init_field(ENTITY_DEVICE, id, FIELD_APPLICATION, &card.application)?;
        for (field, value) in [(FIELD_ADDRESSES, ""), (FIELD_LISTENS, "0"), (FIELD_REVOKED, "0")] {
            if self.head_id(ENTITY_DEVICE, id, field)?.is_none() {
                self.init_field(ENTITY_DEVICE, id, field, value)?;
            }
        }
        Ok(())
    }

    /// Mark a device revoked (AUTH-6). One way: the field's kind is
    /// Membership, so once any device wrote "1" every merge keeps it.
    pub fn revoke_device(&self, device_id: &str) -> VoiceResult<()> {
        if self.get_device_card(device_id)?.is_none() {
            return Err(VoiceError::NotFound(format!("No device card for {}", device_id)));
        }
        self.set_field(ENTITY_DEVICE, device_id, FIELD_REVOKED, "1", None)?;
        Ok(())
    }

    // ------------------------------------------------------------------
    // Synced settings
    // ------------------------------------------------------------------

    /// A synced setting (preferred languages, provider API keys, ...).
    pub fn get_setting(&self, key: &str) -> VoiceResult<Option<String>> {
        Ok(self
            .connection()
            .query_row("SELECT value FROM synced_settings WHERE key = ?", params![key], |r| r.get(0))
            .optional()?)
    }

    /// Set a synced setting; propagates to every device.
    pub fn set_setting(&self, key: &str, value: &str) -> VoiceResult<()> {
        self.set_field(ENTITY_SETTING, key, FIELD_VALUE, value, None)?;
        Ok(())
    }

    /// All synced settings.
    pub fn get_all_settings(&self) -> VoiceResult<HashMap<String, String>> {
        let mut stmt = self.connection().prepare("SELECT key, value FROM synced_settings")?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?)))?;
        let mut out = HashMap::new();
        for row in rows {
            let (k, v) = row?;
            if let Some(v) = v {
                out.insert(k, v);
            }
        }
        Ok(out)
    }
}

/// Entity id of a note-tag link: `"{note_hex}:{tag_hex}"`.
pub fn note_tag_entity_id(note_hex: &str, tag_hex: &str) -> String {
    format!("{}:{}", note_hex, tag_hex)
}

fn split_note_tag_entity_id(entity_id: &str) -> VoiceResult<(String, String)> {
    let mut parts = entity_id.split(':');
    match (parts.next(), parts.next()) {
        (Some(n), Some(t)) if !n.is_empty() && !t.is_empty() => Ok((n.to_string(), t.to_string())),
        _ => Err(VoiceError::validation("entity_id", "expected note:tag")),
    }
}

pub fn hex_to_bytes(s: &str) -> VoiceResult<Vec<u8>> {
    if s.len() % 2 != 0 {
        return Err(VoiceError::validation("hex", "odd length"));
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| VoiceError::validation("hex", "not hex")))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(id: u8, parent: Option<u8>, merge_parent: Option<u8>, content: &str, created_at: i64) -> VersionRow {
        VersionRow {
            id: vec![id; 16],
            entity_type: "note".into(),
            entity_id: "n".into(),
            field: "content".into(),
            parent_id: parent.map(|p| vec![p; 16]),
            merge_parent_id: merge_parent.map(|p| vec![p; 16]),
            content: content.into(),
            context: None,
            conflict_kind: None,
            device_id: None,
            device_name: None,
            created_at,
            created_at_offset: None,
            created_at_zone: None,
            sync_received_at: None,
            published: false,
        }
    }

    fn graph(rows: Vec<VersionRow>) -> FieldGraph {
        FieldGraph { versions: rows.into_iter().map(|r| (r.id.clone(), r)).collect() }
    }

    #[test]
    fn three_way_merges_non_overlapping_edits_cleanly() {
        let base = "שורה א\nשורה ב\nשורה ג\n";
        let a = "שורה א (עריכה במחשב)\nשורה ב\nשורה ג\n";
        let b = "שורה א\nשורה ב\nשורה ג (עריכה בטלפון)\n";
        let m = three_way_text_merge(base, a, b);
        assert!(m.conflict_kind.is_none());
        assert_eq!(m.content, "שורה א (עריכה במחשב)\nשורה ב\nשורה ג (עריכה בטלפון)\n");
    }

    #[test]
    fn three_way_flags_overlapping_edits_with_markers() {
        let base = "line\n";
        let m = three_way_text_merge(base, "line A\n", "line B\n");
        assert_eq!(m.conflict_kind, Some("text"));
        assert!(m.content.contains("<<<<<<< VERSION A\nline A\n=======\nline B\n>>>>>>> VERSION B\n"), "{}", m.content);
    }

    #[test]
    fn three_way_handles_missing_trailing_newline() {
        let m = three_way_text_merge("abc", "abd", "abe");
        assert_eq!(m.conflict_kind, Some("text"));
        assert!(m.content.starts_with("<<<<<<< VERSION A\nabd"), "{}", m.content);
    }

    #[test]
    fn three_way_same_change_on_both_sides_is_clean() {
        let m = three_way_text_merge("x\n", "y\n", "y\n");
        assert!(m.conflict_kind.is_none());
        assert_eq!(m.content, "y\n");
    }

    #[test]
    fn flags_merge_flag_by_flag() {
        let base = "original !verified !verbatim !cleaned !polished";
        let a = "original verified !verbatim !cleaned !polished";
        let b = "original !verified !verbatim cleaned !polished";
        let m = merge_flags(base, a, b, true);
        assert!(m.conflict_kind.is_none());
        assert_eq!(m.content, "original verified !verbatim cleaned !polished");

        let a = "original verified !verbatim !cleaned !polished";
        let b = "!original !verified !verbatim !cleaned !polished";
        // 'verified' changed by a only, 'original' changed by b only: clean
        let m = merge_flags(base, a, b, true);
        assert!(m.conflict_kind.is_none());
        assert_eq!(m.content, "!original verified !verbatim !cleaned !polished");
    }

    #[test]
    fn flags_conflict_when_same_flag_flipped_both_ways() {
        let base = "!verified";
        let m = merge_flags(base, "verified", "!verified", true);
        // b unchanged from base -> a wins, no conflict
        assert!(m.conflict_kind.is_none());
        assert_eq!(m.content, "verified");
        let m = merge_flags("", "verified", "!verified", false);
        assert_eq!(m.conflict_kind, Some("flags"));
    }

    #[test]
    fn scalar_and_membership_and_deleted_rules() {
        let a = v(1, None, None, "Work", 10);
        let b = v(2, None, None, "Office", 20);
        let m = merge_leaves(FieldKind::Scalar, Some("Old"), &a, &b);
        assert_eq!(m.conflict_kind, Some("scalar"));
        assert_eq!(m.content, "Office", "later version stays live");

        let m = merge_leaves(FieldKind::Scalar, Some("Work"), &a, &b);
        assert!(m.conflict_kind.is_none());
        assert_eq!(m.content, "Office");

        let a = v(1, None, None, "0", 10);
        let b = v(2, None, None, "1", 20);
        let m = merge_leaves(FieldKind::Membership, None, &a, &b);
        assert_eq!(m.content, "1");
        assert_eq!(m.conflict_kind, Some("membership"));

        let m = merge_leaves(FieldKind::Deleted, None, &a, &b);
        assert_eq!(m.content, "0");
        assert_eq!(m.conflict_kind, Some("delete"));

        let m = merge_leaves(FieldKind::Deleted, Some("0"), &a, &b);
        assert_eq!(m.content, "0", "two leaves disagreeing on deletion keep the entity alive");
        assert_eq!(m.conflict_kind, Some("delete"));
    }

    #[test]
    fn graph_completeness_leaves_and_lca() {
        // 1 <- 2 <- 3 ; 1 <- 4 ; 9 has unknown parent 8
        let g = graph(vec![
            v(1, None, None, "r", 1),
            v(2, Some(1), None, "a", 2),
            v(3, Some(2), None, "b", 3),
            v(4, Some(1), None, "c", 2),
            v(9, Some(8), None, "dangling", 5),
        ]);
        let complete = g.complete_ids();
        assert_eq!(complete.len(), 4);
        assert!(!complete.contains(&vec![9u8; 16]));
        let leaves = g.leaves(&complete);
        let leaf_ids: Vec<u8> = leaves.iter().map(|l| l.id[0]).collect();
        assert_eq!(leaf_ids, vec![3, 4]);
        assert_eq!(g.lowest_common_ancestor(&[3u8; 16], &[4u8; 16]), Some(vec![1u8; 16]));
        assert_eq!(g.lowest_common_ancestor(&[3u8; 16], &[2u8; 16]), Some(vec![2u8; 16]));
        assert!(g.is_ancestor(&[1u8; 16], &[3u8; 16]));
        assert!(!g.is_ancestor(&[3u8; 16], &[1u8; 16]));
    }

    #[test]
    fn derived_ids_are_stable_and_distinct() {
        let r1 = root_version_id("note", "n1", "content", "hello");
        let r2 = root_version_id("note", "n1", "content", "hello");
        let r3 = root_version_id("note", "n1", "content", "hello!");
        assert_eq!(r1, r2);
        assert_ne!(r1, r3);
        assert_eq!(r1.len(), 16);
        let m = merge_version_id(&[1; 16], &[2; 16], "x");
        assert_ne!(m, merge_version_id(&[2; 16], &[1; 16], "x"));
    }

    #[test]
    fn version_json_round_trip() {
        let mut row = v(7, Some(3), Some(4), "תוכן", 123);
        row.device_id = Some("00000000000070008000000000000001".into());
        row.device_name = Some("טלפון".into());
        row.context = Some("{\"content\":\"aa\"}".into());
        let back = VersionRow::from_json(&row.to_json()).unwrap();
        assert_eq!(back, row);
    }
}
