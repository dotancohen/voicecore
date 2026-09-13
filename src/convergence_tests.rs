//! Property-based convergence tests (SYNC_SPECIFICATION.md, section 11).
//!
//! Several in-memory databases perform random edits, deletes, undeletes, tag
//! changes, link changes, attachments, transcriptions, audio summaries,
//! settings and accepts, exchanging changes through the cursor feed in random
//! order, with small pages, duplicate deliveries and, in some runs, only
//! through a hub or after a device was replaced. After every pair has
//! exchanged until nothing flows, every device must hold the same heads, rows
//! and open conflicts, every value ever written must exist everywhere, tag
//! parents must form a forest, and a further round must change nothing.
//!
//! No external crates: a xorshift generator keeps runs reproducible by seed.
//! `FLEET_DEBUG=1 FLEET_SEED=<n> cargo test debug_single_seed -- --nocapture`
//! traces one seed.

use std::collections::{HashMap, HashSet};

use crate::database::Database;
use crate::sync_apply::apply_changes;
use crate::versions::ENTITY_NOTE;

const DEVICE_IDS: [&str; 4] = [
    "0000000000007000800000000000000a",
    "0000000000007000800000000000000b",
    "0000000000007000800000000000000c",
    "0000000000007000800000000000000d",
];
const WORDS: [&str; 8] = ["שלום", "עולם", "פתק", "חשוב", "מהר", "בית", "עבודה", "מוזיקה"];
const FLAGS: [&str; 4] = ["verified", "verbatim", "cleaned", "polished"];

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(0x9E3779B97F4A7C15) | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn chance(&mut self, one_in: usize) -> bool {
        self.below(one_in) == 0
    }
    fn word(&mut self) -> &'static str {
        WORDS[self.below(WORDS.len())]
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Topology {
    /// Every device may exchange with every other
    Mesh,
    /// Devices exchange only with device 0 (a server); data relays through it
    Hub,
}

struct Options {
    devices: usize,
    steps: usize,
    page: i64,
    topology: Topology,
    /// One in N pages is delivered twice (cursor not advanced after apply)
    duplicate_one_in: usize,
}

/// The simulated fleet.
struct Fleet {
    dbs: Vec<Database>,
    /// cursor[(from, to)]: how far `to` has read `from`'s feed
    cursors: HashMap<(usize, usize), i64>,
    /// database_id of `from` as last seen by `to` (peer reset detection)
    known_db_ids: HashMap<(usize, usize), String>,
    /// Every value handed to a versioned field, for INV-2
    written: HashSet<(&'static str, String, &'static str, String)>,
    /// How many times each operation actually changed something.
    ///
    /// A random test proves nothing about an operation that silently does
    /// not happen: merging notes was broken for weeks while this harness
    /// generated merges on every run and threw the error away, so the whole
    /// region of the state space behind a merge went untested. These counts
    /// are asserted at the end of a run (`assert_operations_happened`).
    effects: HashMap<&'static str, usize>,
    opts: Options,
    debug: bool,
}

impl Fleet {
    fn new(opts: Options) -> Self {
        let dbs = (0..opts.devices).map(|_| Database::new_in_memory().unwrap()).collect();
        Fleet {
            dbs,
            cursors: HashMap::new(),
            known_db_ids: HashMap::new(),
            written: HashSet::new(),
            effects: HashMap::new(),
            debug: std::env::var("FLEET_DEBUG").is_ok(),
            opts,
        }
    }

    fn peers_of(&self, d: usize) -> Vec<usize> {
        match self.opts.topology {
            Topology::Mesh => (0..self.dbs.len()).filter(|&x| x != d).collect(),
            Topology::Hub => {
                if d == 0 {
                    (1..self.dbs.len()).collect()
                } else {
                    vec![0]
                }
            }
        }
    }

    /// One direction of an exchange via the cursor feed, with the client's
    /// reset detection. Returns applied count.
    fn push(&mut self, from: usize, to: usize, rng: &mut Rng) -> i64 {
        // Peer reset detection (PROTO-9): a new database_id voids the cursor
        let db_id = self.dbs[from].database_id().unwrap();
        let known = self.known_db_ids.get(&(from, to)).cloned();
        if known.is_some() && known.as_deref() != Some(db_id.as_str()) {
            self.cursors.insert((from, to), 0);
        }
        self.known_db_ids.insert((from, to), db_id);

        let mut cursor = *self.cursors.get(&(from, to)).unwrap_or(&0);
        let mut applied = 0;
        let mut duplicated_last = false;
        for _ in 0..10_000 {
            let (changes, next, complete) = self.dbs[from]
                .get_changes_after_seq_as_sync_changes(cursor, None, self.opts.page)
                .unwrap();
            let changes: Vec<_> = changes
                .into_iter()
                .map(|mut c| {
                    c.device_id = DEVICE_IDS[from].to_string();
                    c
                })
                .collect();
            // A row can arrive a page before the row it references (relayed
            // feeds are in the relay's apply order); such changes are queued
            // and retried on the next batch, so an empty batch is applied too
            // while anything is pending.
            let pending = self.dbs[to].count_pending_sync_failures().unwrap();
            if !changes.is_empty() || pending > 0 {
                let outcome = apply_changes(&self.dbs[to], &changes, DEVICE_IDS[from], None, 1_800_000_000).unwrap();
                for e in &outcome.errors {
                    assert!(e.contains("FOREIGN KEY"), "unexpected apply error: {}", e);
                }
                applied += outcome.applied + outcome.retried_ok;
                if self.debug {
                    eprintln!("  page {}->{}: {} changes, cursor {} -> {}, complete {}, applied {}", from, to, changes.len(), cursor, next, complete, outcome.applied);
                }
            }
            // Crash between apply and cursor save: the page is delivered again
            if !duplicated_last && !changes.is_empty() && self.opts.duplicate_one_in > 0 && rng.chance(self.opts.duplicate_one_in) {
                duplicated_last = true;
                continue;
            }
            duplicated_last = false;
            cursor = next;
            if complete {
                break;
            }
        }
        self.cursors.insert((from, to), cursor);
        applied
    }

    fn exchange(&mut self, a: usize, b: usize, rng: &mut Rng) -> i64 {
        self.push(a, b, rng) + self.push(b, a, rng)
    }

    fn pending_failures(&self) -> i64 {
        self.dbs.iter().map(|db| db.count_pending_sync_failures().unwrap()).sum()
    }

    /// Exchange along the topology until a whole round applies nothing.
    fn quiesce(&mut self, rng: &mut Rng) {
        for _ in 0..30 {
            let mut moved = 0;
            for a in 0..self.dbs.len() {
                for b in self.peers_of(a) {
                    moved += self.push(a, b, rng);
                }
            }
            if moved == 0 && self.pending_failures() == 0 {
                return;
            }
        }
        let mut detail = Vec::new();
        for (d, db) in self.dbs.iter().enumerate() {
            // The rows written most recently are the ones that keep flowing
            for sql in [
                "SELECT 'note', lower(hex(id)), content, quote(modified_at), quote(deleted_at), seq FROM notes ORDER BY seq DESC LIMIT 2",
                "SELECT 'tag', lower(hex(id)), name, quote(modified_at), quote(deleted_at), seq FROM tags ORDER BY seq DESC LIMIT 2",
                "SELECT 'note_tag', lower(hex(note_id)), lower(hex(tag_id)), quote(modified_at), quote(deleted_at), seq FROM note_tags ORDER BY seq DESC LIMIT 2",
                "SELECT 'attachment', lower(hex(id)), attachment_type, quote(modified_at), quote(deleted_at), seq FROM note_attachments ORDER BY seq DESC LIMIT 2",
                "SELECT 'audio', lower(hex(id)), quote(summary), quote(modified_at), quote(deleted_at), seq FROM audio_files ORDER BY seq DESC LIMIT 2",
                "SELECT 'transcription', lower(hex(id)), content || ' / ' || state, quote(modified_at), quote(deleted_at), seq FROM transcriptions ORDER BY seq DESC LIMIT 2",
                "SELECT 'version', lower(hex(id)), entity_type || ' ' || field || ' ' || content, quote(created_at), quote(device_id IS NULL), seq FROM field_versions ORDER BY seq DESC LIMIT 3",
            ] {
                let mut stmt = db.connection().prepare(sql).unwrap();
                for row in stmt.query_map([], |r| Ok(format!("device {}: {} {} {:?} mod={} del={} seq={}", d, r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?, r.get::<_, String>(3)?, r.get::<_, String>(4)?, r.get::<_, i64>(5)?))).unwrap() {
                    detail.push(row.unwrap());
                }
            }
            // entity_id is stored as the raw uuid bytes, and is NULL for a
            // change whose id is not a uuid, so it is read as hex here.
            let mut stmt = db.connection().prepare("SELECT entity_type, COALESCE(lower(hex(entity_id)), '(none)'), error_message FROM sync_failures WHERE resolved_at IS NULL").unwrap();
            for row in stmt.query_map([], |r| Ok(format!("device {}: pending {} {} -> {}", d, r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?))).unwrap() {
                detail.push(row.unwrap());
            }
        }
        panic!("fleet did not quiesce:\n{}", detail.join("\n"));
    }

    /// Replace a device's database with an empty one (lost phone, reinstall).
    /// Everything it held had been synced before, so nothing should be lost.
    fn reset_device(&mut self, d: usize) {
        self.dbs[d] = Database::new_in_memory().unwrap();
        // Its own cursors into the others are gone with the database
        for x in 0..self.dbs.len() {
            self.cursors.insert((x, d), 0);
            self.known_db_ids.remove(&(x, d));
        }
    }

    // ---- queries ----

    fn alive_notes(&self, d: usize) -> Vec<String> {
        self.dbs[d].get_all_notes().unwrap().into_iter().map(|n| n.id).collect()
    }

    fn deleted_notes(&self, d: usize) -> Vec<String> {
        let mut stmt = self.dbs[d].connection().prepare("SELECT lower(hex(id)) FROM notes WHERE deleted_at IS NOT NULL").unwrap();
        stmt.query_map([], |r| r.get::<_, String>(0)).unwrap().map(|r| r.unwrap()).collect()
    }

    fn user_tags(&self, d: usize) -> Vec<String> {
        self.dbs[d]
            .get_all_tags()
            .unwrap()
            .into_iter()
            .filter(|t| !t.name.starts_with('_'))
            .map(|t| t.id)
            .collect()
    }

    fn audio_files(&self, d: usize) -> Vec<String> {
        let mut stmt = self.dbs[d].connection().prepare("SELECT lower(hex(id)) FROM audio_files WHERE deleted_at IS NULL").unwrap();
        stmt.query_map([], |r| r.get::<_, String>(0)).unwrap().map(|r| r.unwrap()).collect()
    }

    fn attachments(&self, d: usize) -> Vec<String> {
        let mut stmt = self.dbs[d].connection().prepare("SELECT lower(hex(id)) FROM note_attachments WHERE deleted_at IS NULL").unwrap();
        stmt.query_map([], |r| r.get::<_, String>(0)).unwrap().map(|r| r.unwrap()).collect()
    }

    fn transcriptions(&self, d: usize) -> Vec<(String, String, String)> {
        let mut stmt = self.dbs[d].connection().prepare("SELECT lower(hex(id)), content, state FROM transcriptions WHERE deleted_at IS NULL").unwrap();
        stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap().map(|r| r.unwrap()).collect()
    }

    fn pick<'a>(rng: &mut Rng, items: &'a [String]) -> Option<&'a String> {
        if items.is_empty() { None } else { Some(&items[rng.below(items.len())]) }
    }

    fn step(&mut self, rng: &mut Rng) {
        let d = rng.below(self.dbs.len());
        let op = rng.below(27);
        if self.debug {
            eprintln!("step device {} op {}", d, op);
        }
        match op {
            0 | 1 => {
                let text = format!("{} {}\n{}\n", rng.word(), rng.word(), rng.word());
                let id = self.dbs[d].create_note(&text).unwrap();
                self.written.insert(("note", id, "content", text));
                self.note_effect("note");
            }
            2 | 3 | 4 => {
                let notes = self.alive_notes(d);
                let id = match Self::pick(rng, &notes) { Some(i) => i.clone(), None => return };
                let current = self.dbs[d].get_note(&id).unwrap().unwrap().content;
                let mut lines: Vec<String> = current.lines().map(String::from).collect();
                match rng.below(3) {
                    0 => lines.push(rng.word().to_string()),
                    1 if !lines.is_empty() => {
                        let i = rng.below(lines.len());
                        lines[i] = format!("{} {}", lines[i], rng.word());
                    }
                    _ => lines.insert(0, rng.word().to_string()),
                }
                let text = lines.join("\n") + "\n";
                self.dbs[d].update_note(&id, &text).unwrap();
                self.written.insert(("note", id, "content", text));
            }
            5 => {
                let notes = self.alive_notes(d);
                if let Some(id) = Self::pick(rng, &notes) {
                    self.dbs[d].delete_note(id).unwrap();
                }
            }
            6 => {
                let notes = self.deleted_notes(d);
                if let Some(id) = Self::pick(rng, &notes) {
                    self.dbs[d].set_undeleted(ENTITY_NOTE, id).unwrap();
                }
            }
            7 => {
                let name = format!("{}{}", rng.word(), rng.below(1000));
                let id = self.dbs[d].create_tag(&name, None).unwrap();
                self.written.insert(("tag", id, "name", name));
            }
            8 => {
                let tags = self.user_tags(d);
                if let Some(id) = Self::pick(rng, &tags) {
                    let name = format!("{}{}", rng.word(), rng.below(1000));
                    if self.dbs[d].rename_tag(id, &name).unwrap() {
                        self.written.insert(("tag", id.clone(), "name", name));
                    }
                }
            }
            9 => {
                let tags = self.user_tags(d);
                if tags.len() >= 2 {
                    let child = tags[rng.below(tags.len())].clone();
                    let parent = tags[rng.below(tags.len())].clone();
                    if child != parent {
                        // A move that would create a cycle is rejected locally
                        if self.dbs[d].reparent_tag(&child, Some(&parent)).is_ok() {
                            self.note_effect("reparent_tag");
                        }
                    }
                } else if let Some(child) = Self::pick(rng, &tags) {
                    let _ = self.dbs[d].reparent_tag(child, None);
                }
            }
            10 => {
                let tags = self.user_tags(d);
                if let Some(id) = Self::pick(rng, &tags) {
                    self.dbs[d].delete_tag(id).unwrap();
                }
            }
            11 | 12 => {
                let notes = self.alive_notes(d);
                let tags = self.user_tags(d);
                let (n, t) = match (Self::pick(rng, &notes), Self::pick(rng, &tags)) {
                    (Some(n), Some(t)) => (n.clone(), t.clone()),
                    _ => return,
                };
                if rng.below(2) == 0 {
                    // Adding a tag the note already has is a no-op, not an
                    // error, so only a real change is counted.
                    if self.dbs[d].add_tag_to_note(&n, &t).map(|r| r.changed).unwrap_or(false) {
                        self.note_effect("tag_note");
                    }
                } else {
                    // Remove a tag the note really has. Picking at random
                    // almost always named a tag that was not on the note, so
                    // the removal path was hardly ever reached: the run
                    // stayed green while testing nothing. When the note has
                    // no tags at all, one is put on first, so that this
                    // operation always does something.
                    let mut on_note: Vec<String> = self.dbs[d]
                        .get_note_tags(&n)
                        .unwrap()
                        .into_iter()
                        .map(|t| t.id)
                        .collect();
                    if on_note.is_empty() {
                        if self.dbs[d].add_tag_to_note(&n, &t).map(|r| r.changed).unwrap_or(false) {
                            self.note_effect("tag_note");
                            on_note.push(t.clone());
                        }
                    }
                    if let Some(target) = Self::pick(rng, &on_note).cloned() {
                        if self.dbs[d].remove_tag_from_note(&n, &target).map(|r| r.changed).unwrap_or(false) {
                            self.note_effect("untag_note");
                        }
                    }
                }
            }
            13 => {
                let key = format!("k{}", rng.below(3));
                let value = rng.word().to_string();
                self.dbs[d].set_setting(&key, &value).unwrap();
                self.written.insert(("setting", key, "value", value));
            }
            14 => {
                // New recording attached to a note
                let notes = self.alive_notes(d);
                if let Some(n) = Self::pick(rng, &notes) {
                    let n = n.clone();
                    let audio = self.dbs[d].create_audio_file(&format!("{}.mp3", rng.word()), None, None, crate::models::FileOrigin::Imported, None).unwrap();
                    self.dbs[d].attach_to_note(&n, &audio, "audio_file").unwrap();
                    self.note_effect("attachment");
                }
            }
            15 => {
                let atts = self.attachments(d);
                if let Some(a) = Self::pick(rng, &atts) {
                    self.dbs[d].detach_from_note(a).unwrap();
                }
            }
            16 => {
                let audios = self.audio_files(d);
                if let Some(a) = Self::pick(rng, &audios) {
                    let text = format!("{} {}\n", rng.word(), rng.word());
                    let id = self.dbs[d].create_transcription(a, &text, None, "whisper", None, None, None).unwrap();
                    self.written.insert(("transcription", id, "content", text));
                }
            }
            17 | 18 => {
                let trs = self.transcriptions(d);
                if trs.is_empty() {
                    return;
                }
                let (id, content, state) = trs[rng.below(trs.len())].clone();
                if rng.below(2) == 0 {
                    let text = format!("{}{}\n", content, rng.word());
                    self.dbs[d].update_transcription(&id, &text, None, None, None).unwrap();
                    self.written.insert(("transcription", id, "content", text));
                } else {
                    // Toggle one flag
                    let flag = FLAGS[rng.below(FLAGS.len())];
                    let mut flags: Vec<String> = state.split_whitespace().map(String::from).collect();
                    if let Some(pos) = flags.iter().position(|f| f == flag) {
                        flags.remove(pos);
                    } else {
                        flags.push(flag.to_string());
                    }
                    let new_state = flags.join(" ");
                    self.dbs[d].update_transcription(&id, &content, None, None, Some(&new_state)).unwrap();
                }
            }
            19 => {
                let trs = self.transcriptions(d);
                if !trs.is_empty() {
                    let (id, _, _) = trs[rng.below(trs.len())].clone();
                    self.dbs[d].delete_transcription(&id).unwrap();
                }
            }
            20 => {
                let audios = self.audio_files(d);
                if let Some(a) = Self::pick(rng, &audios) {
                    let summary = format!("סיכום {}", rng.word());
                    self.dbs[d].update_audio_file_summary(a, &summary).unwrap();
                    self.written.insert(("audio_file", a.clone(), "summary", summary));
                }
            }
            21 => {
                let audios = self.audio_files(d);
                if let Some(a) = Self::pick(rng, &audios) {
                    self.dbs[d].delete_audio_file(a).unwrap();
                }
            }
            22 => {
                // The importing device uploaded the file: cloud location set once
                let audios = self.audio_files(d);
                if let Some(a) = Self::pick(rng, &audios) {
                    let row = self.dbs[d].get_audio_file_raw(a).unwrap().unwrap();
                    if row["storage_key"].is_null() {
                        self.dbs[d].update_audio_file_storage(a, "s3", &format!("audio/{}.mp3", a), false).unwrap();
                    }
                }
            }
            24 => {
                // Empty one note out of the trash for good. What it took with
                // it is no longer expected to be anywhere, so it leaves the
                // "nothing was lost" ledger as well.
                // Empty something out of the trash. If the trash is empty,
                // delete a note first: this operation must really happen in
                // every run, or the region of the system behind it goes
                // untested while the run stays green.
                let mut deleted = self.deleted_notes(d);
                if deleted.is_empty() {
                    let alive = self.alive_notes(d);
                    if let Some(id) = Self::pick(rng, &alive) {
                        let id = id.clone();
                        self.dbs[d].delete_note(&id).unwrap();
                        deleted = vec![id];
                    }
                }
                if let Some(id) = Self::pick(rng, &deleted) {
                    let id = id.clone();
                    self.dbs[d].purge_note(&id).unwrap();
                    self.note_effect("purge");
                }
            }
            23 => {
                // Merge two notes: survivor gets the text, tags and attachments; the other is deleted
                let notes = self.alive_notes(d);
                if notes.len() >= 2 {
                    let a = notes[rng.below(notes.len())].clone();
                    let b = notes[rng.below(notes.len())].clone();
                    if a != b {
                        // Not `let _`: a merge of two alive notes on one
                        // device must succeed. Swallowing this error is
                        // exactly what hid the "Note not found" bug.
                        self.dbs[d]
                            .merge_notes(&a, &b)
                            .unwrap_or_else(|e| panic!("merging {} into {} failed: {}", b, a, e));
                        self.note_effect("merge");
                    }
                }
            }
            _ => {
                // Accept a random open conflict on this device
                let conflicts = self.dbs[d].get_conflicts(false).unwrap();
                if !conflicts.is_empty() {
                    let c = &conflicts[rng.below(conflicts.len())];
                    self.dbs[d].accept_conflict(&c.id).unwrap();
                }
            }
        }
        // Sometimes exchange with a random peer mid-way (partial syncs)
        if rng.below(4) == 0 {
            let peers = self.peers_of(d);
            let other = peers[rng.below(peers.len())];
            self.exchange(d, other, rng);
        }
    }

    fn snapshot(&self, d: usize) -> Vec<String> {
        let db = &self.dbs[d];
        let conn = db.connection();
        let mut out = Vec::new();
        let mut q = |sql: &str| {
            let mut stmt = conn.prepare(sql).unwrap();
            let rows = stmt
                .query_map([], |r| {
                    let n = r.as_ref().column_count();
                    let mut parts = Vec::new();
                    for i in 0..n {
                        let v: rusqlite::types::Value = r.get(i).unwrap();
                        parts.push(format!("{:?}", v));
                    }
                    Ok(parts.join("|"))
                })
                .unwrap();
            for row in rows {
                out.push(row.unwrap());
            }
        };
        q("SELECT entity_type, entity_id, field, hex(head_id) FROM field_heads ORDER BY 1, 2, 3");
        q("SELECT hex(id), content, deleted_at IS NULL FROM notes ORDER BY 1");
        q("SELECT hex(id), name, hex(parent_id), deleted_at IS NULL FROM tags ORDER BY 1");
        q("SELECT hex(note_id), hex(tag_id), deleted_at IS NULL FROM note_tags ORDER BY 1, 2");
        q("SELECT hex(id), hex(note_id), hex(attachment_id), deleted_at IS NULL FROM note_attachments ORDER BY 1");
        q("SELECT hex(id), filename, summary, storage_provider, storage_key, deleted_at IS NULL FROM audio_files ORDER BY 1");
        q("SELECT hex(id), hex(audio_file_id), content, state, deleted_at IS NULL FROM transcriptions ORDER BY 1");
        q("SELECT key, value FROM synced_settings ORDER BY 1");
        // Open conflicts must agree everywhere. Resolved records may differ:
        // a device that merged a partial page can have flagged and then
        // superseded a conflict the others never saw.
        q("SELECT hex(id), kind FROM field_conflicts WHERE resolved_at IS NULL ORDER BY 1");
        out
    }

    fn assert_converged(&self, seed: u64) {
        let first = self.snapshot(0);
        for d in 1..self.dbs.len() {
            let other = self.snapshot(d);
            if first != other {
                let diff: Vec<String> = first
                    .iter()
                    .filter(|l| !other.contains(l))
                    .chain(other.iter().filter(|l| !first.contains(l)))
                    .cloned()
                    .collect();
                // Dump the versions behind any differing head so the cause is visible
                let mut detail = Vec::new();
                for line in &diff {
                    let parts: Vec<&str> = line.split('|').collect();
                    if parts.len() == 4 && parts[0].starts_with("Text(") {
                        let et = parts[0].trim_start_matches("Text(\"").trim_end_matches("\")");
                        let eid = parts[1].trim_start_matches("Text(\"").trim_end_matches("\")");
                        let field = parts[2].trim_start_matches("Text(\"").trim_end_matches("\")");
                        for (x, db) in self.dbs.iter().enumerate() {
                            let head = db.head_version(et, eid, field).unwrap().map(|h| crate::versions::hex(&h.id)).unwrap_or_default();
                            detail.push(format!("device {} {} {} {}: head {}", x, et, eid, field, head));
                            for v in db.get_field_history(et, eid, field).unwrap() {
                                detail.push(format!("   {} parent={:?} merge={:?} device={:?} conflict={:?} content={:?} created={}",
                                    v.id_hex(), v.parent_hex(), v.merge_parent_hex(), v.device_id, v.conflict_kind, v.content, v.created_at));
                            }
                        }
                    }
                }
                panic!("seed {}: device 0 and {} differ:\n{}\n{}", seed, d, diff.join("\n"), detail.join("\n"));
            }
        }
    }

    fn assert_nothing_lost(&self, seed: u64) {
        for (d, db) in self.dbs.iter().enumerate() {
            for (et, eid, field, content) in &self.written {
                if self.was_purged(et, eid) {
                    continue;
                }
                let n: i64 = db
                    .connection()
                    .query_row(
                        "SELECT COUNT(*) FROM field_versions WHERE entity_type = ? AND entity_id = ? AND field = ? AND content = ?",
                        rusqlite::params![et, eid, field, content],
                        |r| r.get(0),
                    )
                    .unwrap();
                assert!(n > 0, "seed {}: device {} lost {} {} {} = {:?}", seed, d, et, eid, field, content);
            }
        }
    }

    fn assert_conflict_sides_reachable(&self, seed: u64) {
        let db = &self.dbs[0];
        for c in db.get_conflicts(false).unwrap() {
            if c.kind != "text" {
                continue;
            }
            let merged = db.head_version(&c.entity_type, &c.entity_id, &c.field).unwrap().unwrap().content;
            let merged_lines: HashSet<&str> = merged.lines().collect();
            let base_lines: HashSet<String> = c
                .base_version_id
                .as_ref()
                .and_then(|b| db.get_version(&crate::versions::hex_to_bytes(b).unwrap()).unwrap())
                .map(|v| v.content.lines().map(String::from).collect())
                .unwrap_or_default();
            for side in [&c.version_a_id, &c.version_b_id] {
                let v = db.get_version(&crate::versions::hex_to_bytes(side).unwrap()).unwrap().unwrap();
                // Every line a side added or changed (relative to the base)
                // must survive in the merged text; lines it left untouched may
                // legitimately be replaced by the other side's edit.
                for line in v.content.lines().filter(|l| !base_lines.contains(*l)) {
                    assert!(
                        merged_lines.contains(line),
                        "seed {}: line {:?} written on one side is missing from the merged text {:?}",
                        seed,
                        line,
                        merged
                    );
                }
            }
        }
    }

    /// Tag parents must form a forest on every device (a cycle would hang
    /// every recursive query on the hierarchy).
    fn assert_no_tag_cycles(&self, seed: u64) {
        for (d, db) in self.dbs.iter().enumerate() {
            let mut stmt = db.connection().prepare("SELECT lower(hex(id)), lower(hex(parent_id)) FROM tags").unwrap();
            let parents: HashMap<String, Option<String>> = stmt
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?)))
                .unwrap()
                .map(|r| r.unwrap())
                .collect();
            for start in parents.keys() {
                let mut cur = parents.get(start).cloned().flatten();
                let mut steps = 0;
                while let Some(p) = cur {
                    steps += 1;
                    assert!(steps <= parents.len(), "seed {}: device {} has a tag cycle through {}", seed, d, start);
                    cur = parents.get(&p).cloned().flatten();
                }
            }
        }
    }

    /// Stop expecting the values of things that were removed for good.
    ///
    /// `written` is the ledger behind "nothing was lost" (INV-2). A purge is
    /// the one operation that is allowed to lose something, so what it took
    /// is struck off the ledger; everything else must still be found on
    /// every device.
    /// Whether this entity was emptied out of the trash for good, anywhere.
    ///
    /// The databases are asked rather than the harness keeping its own
    /// books: a purge cascades to things that were created on another device
    /// and had not arrived yet, and only the purge records know the whole
    /// list. A value whose entity was purged is no longer expected on any
    /// device; that is what a purge means.
    fn was_purged(&self, entity_type: &str, entity_id: &str) -> bool {
        let purged_anywhere = |et: &str, eid: &str| {
            self.dbs.iter().any(|db| db.is_purged(et, eid).unwrap_or(false))
        };
        if purged_anywhere(entity_type, entity_id) {
            return true;
        }
        // A tag link is named by the pair of ids and goes with its note.
        entity_type == "note_tag"
            && entity_id
                .split(':')
                .next()
                .map(|note| purged_anywhere("note", note))
                .unwrap_or(false)
    }

    fn note_effect(&mut self, what: &'static str) {
        *self.effects.entry(what).or_insert(0) += 1;
    }

    fn assert_all(&self, seed: u64) {
        self.assert_converged(seed);
        self.assert_nothing_lost(seed);
        self.assert_conflict_sides_reachable(seed);
        self.assert_no_tag_cycles(seed);
    }

    fn assert_settled(&mut self, seed: u64, rng: &mut Rng) {
        // INV-4 / INV-5: another full round is a no-op
        let before = self.snapshot(0);
        let mut moved = 0;
        for a in 0..self.dbs.len() {
            for b in self.peers_of(a) {
                moved += self.push(a, b, rng);
            }
        }
        assert_eq!(moved, 0, "seed {}: a settled fleet exchanged {} changes", seed, moved);
        assert_eq!(before, self.snapshot(0), "seed {}: heads changed without new input", seed);
    }
}

/// Run one seed and return how many times each watched operation took
/// effect, so that a whole test can prove it covered them (see
/// [`assert_operations_covered`]). One seed is too small a sample: a
/// sixty-step run may never happen to hold two notes at once.
fn run(seed: u64, opts: Options) -> HashMap<&'static str, usize> {
    let mut rng = Rng::new(seed);
    let steps = opts.steps;
    let mut fleet = Fleet::new(opts);
    for _ in 0..steps {
        fleet.step(&mut rng);
    }
    fleet.quiesce(&mut rng);
    fleet.assert_all(seed);
    fleet.assert_settled(seed, &mut rng);
    fleet.effects.clone()
}

/// Add one run's counts into the running total for a test.
fn add_effects(total: &mut HashMap<&'static str, usize>, run: HashMap<&'static str, usize>) {
    for (what, n) in run {
        *total.entry(what).or_insert(0) += n;
    }
}

/// Fail the test if an operation it generates never actually happened.
///
/// Green randomised tests that exercise nothing are worse than no tests:
/// merging notes was broken for weeks while these tests generated merges on
/// every run, dropped the error, and reported success. The bug that hid
/// behind the dead merge (attachments that could never converge) then took
/// one run to appear once merging worked.
fn assert_operations_covered(total: &HashMap<&'static str, usize>, what_ran: &str) {
    for what in ["note", "merge", "tag_note", "untag_note", "reparent_tag", "attachment", "purge"] {
        assert!(
            total.get(what).copied().unwrap_or(0) > 0,
            "{}: not one {} took effect in the whole test; those code paths were not tested",
            what_ran,
            what
        );
    }
}

fn mesh(devices: usize, steps: usize, page: i64) -> Options {
    Options { devices, steps, page, topology: Topology::Mesh, duplicate_one_in: 0 }
}

#[test]
fn two_devices_converge() {
    let mut effects = HashMap::new();
    for seed in 1..=25 {
        add_effects(&mut effects, run(seed, mesh(2, 60, 7)));
    }
    assert_operations_covered(&effects, "two devices");
}

#[test]
fn three_devices_converge_with_partial_syncs() {
    let mut effects = HashMap::new();
    for seed in 100..=120 {
        add_effects(&mut effects, run(seed, mesh(3, 80, 5)));
    }
    assert_operations_covered(&effects, "three devices with partial syncs");
}

#[test]
fn four_devices_converge_with_large_pages() {
    let mut effects = HashMap::new();
    for seed in 200..=210 {
        add_effects(&mut effects, run(seed, mesh(4, 100, 1000)));
    }
    assert_operations_covered(&effects, "four devices with large pages");
}

#[test]
fn pages_of_one_and_duplicate_deliveries() {
    let mut effects = HashMap::new();
    for seed in 300..=312 {
        add_effects(&mut effects, run(seed, Options { devices: 3, steps: 60, page: 1, topology: Topology::Mesh, duplicate_one_in: 3 }));
    }
    assert_operations_covered(&effects, "pages of one with duplicate deliveries");
}

#[test]
fn hub_topology_relays_everything() {
    let mut effects = HashMap::new();
    for seed in 400..=415 {
        add_effects(&mut effects, run(seed, Options { devices: 4, steps: 90, page: 6, topology: Topology::Hub, duplicate_one_in: 5 }));
    }
    assert_operations_covered(&effects, "hub topology");
}

#[test]
fn replaced_device_recovers_everything() {
    for seed in 500..=510 {
        let mut rng = Rng::new(seed);
        let mut fleet = Fleet::new(Options { devices: 3, steps: 0, page: 8, topology: Topology::Hub, duplicate_one_in: 0 });
        for _ in 0..60 {
            fleet.step(&mut rng);
        }
        fleet.quiesce(&mut rng);
        fleet.assert_all(seed);
        // Device 2 loses its database and comes back empty
        fleet.reset_device(2);
        for _ in 0..30 {
            fleet.step(&mut rng);
        }
        fleet.quiesce(&mut rng);
        fleet.assert_all(seed);
        fleet.assert_settled(seed, &mut rng);
    }
}

#[test]
fn debug_single_seed() {
    if let Ok(seed) = std::env::var("FLEET_SEED") {
        let env = |k: &str, d: usize| std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d);
        let topology = if std::env::var("FLEET_TOPOLOGY").as_deref() == Ok("hub") { Topology::Hub } else { Topology::Mesh };
        run(seed.parse().unwrap(), Options {
            devices: env("FLEET_DEVICES", 3),
            steps: env("FLEET_STEPS", 80),
            page: env("FLEET_PAGE", 5) as i64,
            topology,
            duplicate_one_in: env("FLEET_DUP", 0),
        });
    }
}

#[test]
fn cursor_feed_is_exact_and_resumable() {
    // Every write appears exactly once when paging with any page size
    let db = Database::new_in_memory().unwrap();
    let n1 = db.create_note("א\n").unwrap();
    let t = db.create_tag("תגית", None).unwrap();
    db.add_tag_to_note(&n1, &t).unwrap();
    db.update_note(&n1, "א\nב\n").unwrap();
    db.set_setting("k", "v").unwrap();
    let end = db.current_seq().unwrap();
    assert!(end > 0);

    let all = db.get_changes_after_seq(0, None, 10_000).unwrap();
    assert!(all.is_complete);
    assert_eq!(all.next_cursor, end);

    for page in [1, 2, 3, 100] {
        let mut cursor = 0;
        let mut seen = Vec::new();
        loop {
            let feed = db.get_changes_after_seq(cursor, None, page).unwrap();
            for c in &feed.changes {
                seen.push((c["entity_type"].as_str().unwrap().to_string(), c["entity_id"].as_str().unwrap().to_string(), c["seq"].as_i64().unwrap()));
            }
            cursor = feed.next_cursor;
            if feed.is_complete {
                break;
            }
        }
        let expected: Vec<(String, String, i64)> = all
            .changes
            .iter()
            .map(|c| (c["entity_type"].as_str().unwrap().to_string(), c["entity_id"].as_str().unwrap().to_string(), c["seq"].as_i64().unwrap()))
            .collect();
        assert_eq!(seen, expected, "page size {}", page);
    }

    // Nothing after the end
    let empty = db.get_changes_after_seq(end, None, 10).unwrap();
    assert!(empty.changes.is_empty() && empty.is_complete && empty.next_cursor == end);
}

#[test]
fn cache_rebuilds_and_echoes_do_not_republish() {
    let a = Database::new_in_memory().unwrap();
    let b = Database::new_in_memory().unwrap();
    let n = a.create_note("טקסט\n").unwrap();
    let end = a.current_seq().unwrap();
    a.rebuild_note_cache(&n).unwrap();
    assert_eq!(a.current_seq().unwrap(), end, "cache rebuild must not bump the sequence");

    // B receives everything, then A receives B's echo of its own data
    let (changes, _, _) = a.get_changes_after_seq_as_sync_changes(0, None, 1000).unwrap();
    apply_changes(&b, &changes, DEVICE_IDS[0], None, 1_800_000_000).unwrap();
    let (echo, _, _) = b.get_changes_after_seq_as_sync_changes(0, None, 1000).unwrap();
    apply_changes(&a, &echo, DEVICE_IDS[1], None, 1_800_000_001).unwrap();
    assert_eq!(a.current_seq().unwrap(), end, "an echo of our own data must not bump the sequence");
}

#[test]
fn concurrent_tag_moves_never_form_a_cycle() {
    // A moves X under Y while B moves Y under X: after the exchange the
    // parents must still form a forest, identically on both devices, and the
    // hierarchy queries must terminate.
    let a = Database::new_in_memory().unwrap();
    let b = Database::new_in_memory().unwrap();
    let x = a.create_tag("איקס", None).unwrap();
    let y = a.create_tag("וואי", None).unwrap();
    let (changes, _, _) = a.get_changes_after_seq_as_sync_changes(0, None, 1000).unwrap();
    apply_changes(&b, &changes, DEVICE_IDS[0], None, 1_800_000_000).unwrap();

    a.reparent_tag(&x, Some(&y)).unwrap();
    b.reparent_tag(&y, Some(&x)).unwrap();

    let (from_a, _, _) = a.get_changes_after_seq_as_sync_changes(0, None, 1000).unwrap();
    let (from_b, _, _) = b.get_changes_after_seq_as_sync_changes(0, None, 1000).unwrap();
    apply_changes(&b, &from_a, DEVICE_IDS[0], None, 1_800_000_001).unwrap();
    apply_changes(&a, &from_b, DEVICE_IDS[1], None, 1_800_000_001).unwrap();

    for db in [&a, &b] {
        let px = db.get_tag(&x).unwrap().unwrap().parent_id;
        let py = db.get_tag(&y).unwrap().unwrap().parent_id;
        assert!(!(px.as_deref() == Some(y.as_str()) && py.as_deref() == Some(x.as_str())), "cycle: {:?} {:?}", px, py);
        // Must terminate
        let _ = db.get_tag_descendants(&x).unwrap();
        let _ = db.get_tag_descendants(&y).unwrap();
        assert_eq!(db.get_unresolved_conflict_counts().unwrap()["scalar"], 1, "the losing move is flagged");
    }
    assert_eq!(a.get_tag(&x).unwrap().unwrap().parent_id, b.get_tag(&x).unwrap().unwrap().parent_id);
    assert_eq!(a.get_tag(&y).unwrap().unwrap().parent_id, b.get_tag(&y).unwrap().unwrap().parent_id);
}

#[test]
fn feed_pages_are_bounded_in_bytes_and_lose_nothing() {
    // Six notes of ~1.5 MB each: a page limit of 10000 changes must still be
    // cut by the byte budget, and paging must deliver every byte.
    let a = Database::new_in_memory().unwrap();
    let b = Database::new_in_memory().unwrap();
    let big = "שורה ארוכה מאוד ".repeat(100_000); // ~2.9 MB of UTF-8
    let mut ids = Vec::new();
    for i in 0..6 {
        ids.push(a.create_note(&format!("{}\n{}", i, big)).unwrap());
    }
    let mut cursor = 0;
    let mut pages = 0;
    loop {
        let feed = a.get_changes_after_seq(cursor, None, 10_000).unwrap();
        let bytes: usize = feed.changes.iter().map(|c| serde_json::to_string(c).unwrap().len()).sum();
        assert!(bytes <= crate::database::FEED_BYTE_BUDGET || feed.changes.len() == 1, "page of {} bytes with {} changes", bytes, feed.changes.len());
        let (changes, _, _) = a.get_changes_after_seq_as_sync_changes(cursor, None, 10_000).unwrap();
        apply_changes(&b, &changes, DEVICE_IDS[0], None, 1_800_000_000).unwrap();
        pages += 1;
        cursor = feed.next_cursor;
        if feed.is_complete {
            break;
        }
        assert!(pages < 100, "runaway paging");
    }
    assert!(pages >= 3, "expected several pages, got {}", pages);
    for id in &ids {
        assert_eq!(b.get_note(id).unwrap().unwrap().content, a.get_note(id).unwrap().unwrap().content);
    }
}

#[test]
fn a_batch_is_one_transaction_and_a_bad_change_does_not_abort_it() {
    use crate::models::SyncChange;
    let a = Database::new_in_memory().unwrap();
    let b = Database::new_in_memory().unwrap();
    let n1 = a.create_note("ראשון\n").unwrap();
    let n2 = a.create_note("שני\n").unwrap();
    let (mut changes, _, _) = a.get_changes_after_seq_as_sync_changes(0, None, 1000).unwrap();
    // Slip an invalid change into the middle of the page
    changes.insert(changes.len() / 2, SyncChange {
        entity_type: "note".to_string(),
        entity_id: "not-an-id".to_string(),
        operation: "create".to_string(),
        data: serde_json::json!({"id": "not-an-id", "content": "x", "created_at": 1}),
        timestamp: 1,
        device_id: DEVICE_IDS[0].to_string(),
        device_name: None,
    });
    let outcome = apply_changes(&b, &changes, DEVICE_IDS[0], None, 1_800_000_000).unwrap();
    assert!(!outcome.errors.is_empty());
    assert_eq!(b.get_note(&n1).unwrap().unwrap().content, "ראשון\n");
    assert_eq!(b.get_note(&n2).unwrap().unwrap().content, "שני\n");
    // Nothing is left open: a second batch works and the write lock is free
    b.create_note("שלישי\n").unwrap();
}
