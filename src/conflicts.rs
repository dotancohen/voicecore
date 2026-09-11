//! Conflict listing and resolution for Voice sync.
//!
//! Conflicts are derived from the version graph (see `versions.rs`): a merge
//! that needed a human produces a `field_conflicts` row referencing the base,
//! the two sides and the merge version. Resolution is a new version that
//! descends from the merge, either the user's edited text or an explicit
//! "accept the merged text as-is".

use crate::database::Database;
use crate::error::VoiceResult;
pub use crate::versions::{ConflictRow, VersionRow};

/// Conflict manager
pub struct ConflictManager<'a> {
    db: &'a Database,
}

impl<'a> ConflictManager<'a> {
    pub fn new(db: &'a Database) -> Self {
        Self { db }
    }

    /// Unresolved conflict counts by kind, plus "total".
    pub fn get_unresolved_count(&self) -> VoiceResult<std::collections::HashMap<String, i64>> {
        self.db.get_unresolved_conflict_counts()
    }

    /// All conflicts, newest first.
    pub fn get_conflicts(&self, include_resolved: bool) -> VoiceResult<Vec<ConflictRow>> {
        self.db.get_conflicts(include_resolved)
    }

    /// The three versions behind a conflict: (base, a, b, merge).
    pub fn get_conflict_versions(
        &self,
        conflict: &ConflictRow,
    ) -> VoiceResult<(Option<VersionRow>, Option<VersionRow>, Option<VersionRow>, Option<VersionRow>)> {
        let get = |id: &Option<String>| -> VoiceResult<Option<VersionRow>> {
            match id {
                Some(h) => self.db.get_version(&crate::versions::hex_to_bytes(h)?),
                None => Ok(None),
            }
        };
        Ok((
            get(&conflict.base_version_id)?,
            get(&Some(conflict.version_a_id.clone()))?,
            get(&Some(conflict.version_b_id.clone()))?,
            get(&Some(conflict.merge_version_id.clone()))?,
        ))
    }

    /// Accept the merged value as-is.
    pub fn accept(&self, conflict_id: &str) -> VoiceResult<bool> {
        self.db.accept_conflict(conflict_id)
    }

    /// Resolve with user-supplied content.
    pub fn resolve_with_content(&self, conflict_id: &str, content: &str) -> VoiceResult<bool> {
        self.db.resolve_conflict_with_content(conflict_id, content)
    }
}
