# Claude Code instructions for VoiceCore

The shared Rust core of the Voice Family. Read `../CLAUDE.md` first: the rules
there (tests, naming, the owner's data, data loss, the three checkouts of the
core) apply here too. The numbered rules this code implements are in
`../SYNC_SPECIFICATION.md`.

This repository is checked out three times: here, and as `submodules/voicecore`
in `Voice` and in `VoiceAndroid`. Make changes here, commit them, and move the
two submodule checkouts to the commit (`../CLAUDE.md`, "One core, three binding
layers").

## Running the tests

```bash
cd /home/dotancohen/Projects/VoiceFamily/VoiceCore
cargo test
```

Builds go to `VoiceFamily/.cargo-target` (`../TECHNICAL-DECISIONS.md` 7.4).
`cargo test convergence` runs the property tests alone (see "Versioned fields"
below).

## Logging

Use the `tracing` crate, never `eprintln!` or `println!`:

- `tracing::error!()` — errors that need attention
- `tracing::warn!()` — warnings
- `tracing::info!()` — important operational information (shown by default)
- `tracing::debug!()` — debugging information (shown with `--verbose`)
- `tracing::trace!()` — very detailed tracing

The sync server's `--verbose` flag enables DEBUG level logging. Keys never reach
a log: request logging redacts `Authorization`, and a key is never part of an
error string (AUTH-3).

## Database

The schema is created in `Database::init_database()` in `src/database.rs`, and
later columns and tables by the `migrate_*` functions called after it. The
conflict tables of the first sync design (`conflicts_note_content` and the
others) are still created there and dropped again by a migration; `field_conflicts`
replaced them.

### Transcription flags (the `state` field)

`transcriptions.state` is a space-separated string of flags. The field is named
`state` in the database and in the sync protocol and keeps that name
(`../TECHNICAL-DECISIONS.md` 1.4); everything above the database calls them
flags, because any number can be true at once. The five: `original`,
`verified`, `verbatim`, `cleaned`, `polished`. Their wording and the agreed
cases live in the applications (`Voice/src/core/transcription_flags.py`,
`VoiceAndroid/.../data/TranscriptionFlags.kt`) and in the contract fixture both
suites read (`../TECHNICAL-DECISIONS.md` 4.3).

### Note display cache

The `di_cache_note_pane_display` column of `notes` holds precomputed data for
the desktop's Note pane, as JSON:

- `tags` — each with `id`, `name` and `full_path`
- `conflicts` — conflict kinds, for example `["content", "delete"]`
- `attachments` — each with its nested `audio_file` and `transcriptions`
- `cached_at` — when the cache was built

It is built by `rebuild_note_cache()` in `database.rs` and read by
`_load_from_cache()` in `Voice/src/ui/note_pane.py`. A new field of the Note
pane that needs a query goes into both, and every mutation that affects it must
rebuild the cache. The cache is rebuilt by `update_note()`,
`add_tag_to_note()` and `remove_tag_from_note()`, `create_transcription()` and
`update_transcription()`, `accept_conflict()`, `resolve_conflict_with_content()`,
and any sync that flags or resolves a conflict (`refresh_entity_caches()` in
`versions.rs`). A cache is derived data and is never synced
(`../TECHNICAL-DECISIONS.md` 1.3).

On the desktop, `voice cli db-maintenance note-rebuild-caches <note_id>` rebuilds
one note's caches and `voice cli db-maintenance rebuild-all-caches` every note's.

## Sync

### Partial batch failures

When a batch of changes is applied and change 5 of 10 fails, changes 1 to 4 and
6 to 10 are still applied, and only change 5 is reported and queued for retry
(APPLY-2, APPLY-3). `sync_apply::apply_changes` runs a batch inside one
`BEGIN IMMEDIATE` transaction, and a statement that fails rolls back only
itself (APPLY-9); never open another transaction inside a row-apply path.

### Adding a syncable entity type or field

The rule is DM-1 in `SYNC_SPECIFICATION.md`. A new editable field, or a new
entity, touches all of these:

1. **`versions.rs`, `FIELD_REGISTRY`**: register every editable field with its
   merge kind. A field written with plain SQL is a bug: writes go through
   `set_field`, `set_deleted` and `init_field`, and `apply_head_to_entity`
   copies the head into the entity row.
2. **`database.rs`, `migrate_add_sync_sequence`**: give the table a `seq` column
   and its triggers, or the entity never appears in the cursor feed. Adding a
   synced column means adding it to that table's column list; an existing
   database's update trigger is rebuilt at its next open when its stored
   definition lacks the column.
3. **`database.rs`, `collect_changes`** (behind `get_changes_after_seq` and
   `get_changes_since`) and **`get_full_dataset`**: add the entity's rows to the
   feed and to the whole-dataset document.
4. **`sync_apply.rs`, `ALL_SYNC_ENTITY_TYPES`** and **`apply_changes`**: add the
   type to the list and a match arm that applies it. The server's
   `test_get_changes_since_returns_all_entity_types` fails if a listed type is
   missing from the feed.
5. **`android.rs`** and **`Voice/rust/voice-python/src/lib.rs`**: expose what the
   applications need.

Transcriptions were once missing from the feed and never reached the other
devices; the list and the test that walks it stop that from happening again.

### Recordings: never moved by a sync

Recording *metadata* syncs like every other entity. A recording's *file* moves
only when the user starts an action (`../TECHNICAL-DECISIONS.md` 4.5):

- **Upload** (`file_storage.rs`) copies files this device holds to the bucket:
  every row with no `storage_key`, not deleted, whose file is here and within
  the account's upload limit (FILE-2, FILE-23). Rows whose file is not here are
  skipped: another device holds them. A failed upload is reported and tried
  again at the next upload. After the first remote failure in a batch the batch
  stops (`deferred`).
- **Download** copies a file from the bucket when the user asks, unless
  `sync.mirror_audio_files` is on in that desktop's or server's `config.json`
  (local, never synced). Downloads go to `<file>.part`, are verified by size and
  content hash, then renamed (FILE-7, FILE-18).
- **Send** and **fetch** (`transfer.rs`, `sync_client.rs`, the file routes of
  `sync_server.rs`) move a file between two instances, streamed and resumable,
  and verified by hash (FILE-12, FILE-13). **Deliver** is sync then send;
  **exchange** is sync then send and fetch.
- "Cloud storage not configured" is a silent no-op for automatic paths and a
  clear error for the ones the user starts (FILE-10).
- A recording's file on disk is named by its row's `disk_name` (FILE-15): a
  recording made by Voice is `YYYY_MM_DD_HH_MM_SS-<last eight of the id>.<ext>`,
  an imported file keeps its own name, and a collision adds
  `-<last eight of the id>`. Find a file with `audio_local_path()` (`models.rs`),
  never from the id. The bucket object is `<content hash>.<ext>` (FILE-18), with
  `.enc` added when encrypted and `<id>.<ext>` only while no hash is known
  (`storage_key_for()` in `file_storage.rs`). `ext` comes from
  `audio_file_extension()` in `models.rs`: lowercase, last dot wins, `bin` when
  there is none. Never derive the extension by hand.
- Where each copy is travels in `file_locations` (FILE-22); the upload limit is
  the account's (FILE-23); what the user should know is computed by `issues.rs`
  (ISSUE-1).
- Every incoming `audio_file` row is applied through one upsert that merges per
  column: the newer row wins a metadata column, an older row only fills in
  NULLs, the cloud location is never erased by a row without one and only
  replaced by a newer row that has one, and versioned columns (summary,
  deletion) are never written from a row. Do not reintroduce "skip older rows":
  it left a peer that edited the summary first without the `storage_key`.
- rust-s3 must stay at 0.37 or newer (FILE-24): older versions load TLS roots
  from the operating system's certificate directory, which Android does not
  have. Its `put_object` keeps only the ETag of an answer, so requests whose
  error body matters (the wizard's round trip, the parts) are signed and sent by
  the core itself.

### Versioned fields (`versions.rs`)

Every editable value is a field with a Git-like history in `field_versions`
(append-only graph: `parent_id`, `merge_parent_id`), a head per field in
`field_heads`, and conflicts in `field_conflicts`. Entity rows (`notes.content`,
`tags.name`, ...) are denormalised copies of the heads, rewritten by
`apply_head_to_entity()`.

- **Write path:** every mutation goes through `set_field()` / `set_deleted()`
  (never `UPDATE notes SET content` directly).
- **Sync:** the feed carries `field_version` changes; `sync_apply.rs` applies
  versions first, rows second, links last, then `recompute_heads()`. Rows
  without history get a deterministic hash root (`ensure_root_version`), so
  data from before versioning merges cleanly.
- **Merge rules:** Text = diff3 (markers `<<<<<<< VERSION A` /
  `>>>>>>> VERSION B`, the same on every device); Scalar = later wins + flag;
  Flags = per flag; Membership and Deleted = disagreement keeps the link or the
  entity + flag; a delete that did not see a concurrent edit is resurrected
  (delete conflict).
- **Determinism:** root, merge, accept and resurrect version ids and conflict
  ids are hashes, so every device computes the same graph and the same conflict
  ids. `conflict_kind` travels on the merge version, so a device that only
  receives a merge still records the conflict.
- **Resolution:** any version descending from the merge (an edit, or
  `accept_conflict()`) resolves it everywhere. Saving a note with an open
  conflict resolves it. There is no keep-local or keep-remote: both sides are
  already in the merge.
- **Synced settings:** `synced_settings` is a versioned key/value store
  (`get_setting` / `set_setting`).
- **First edit of a field** (for example the first tombstone) has no parent but
  has a `device_id`; only hash roots (no parent, no device) are treated as data
  from before versioning, with timestamp 0.
- **Authored and derived:** merges and resurrections (a parent, no device) are
  derived and are not synced; every device recomputes them. Heads are folded
  from the *authored* leaves only (`authored_leaves`), so arrival order and page
  size cannot change the result. An authored version written on top of a derived
  one publishes it (`publish_derived_ancestors`) so peers can complete the
  child. Never make a derived version a fold input; never sync one that is not
  published.
- **Cursor feed:** `get_changes_after_seq(cursor, upto, limit)` is the sync
  feed. Every syncable table has a `seq` column stamped by triggers
  (`migrate_add_sync_sequence`): on insert, and on update of a synced column
  *when the value changed*. Row updates on the apply path use
  `NULLIF(MAX(...), 0)` forms so an echo of our own data writes nothing
  (otherwise rows ping-pong between peers for ever). `recompute_head` writes the
  entity row exactly once.
- **Property tests:** `cargo test convergence` runs random multi-device fleets
  (`src/convergence_tests.rs`): every entity type, pages down to 1, duplicate
  deliveries, a hub topology, a replaced device. Run them after any change to
  `versions.rs`, `sync_apply.rs`, the row-apply upserts or the feed; they found
  every regression so far. They must also stay quiescent: a row written with a
  changed value on every echo shows up as "fleet did not quiesce".
  `FLEET_DEBUG=1 FLEET_SEED=<n> [FLEET_TOPOLOGY=hub ...] cargo test debug_single_seed -- --nocapture`
  traces one seed.
- **Unversioned metadata** (transcription `service_response` and
  `content_segments`, a recording's file metadata, an attachment's target)
  merges per column by `modified_at` in the `apply_sync_*` upserts. A local
  update of such a column must stamp `modified_at` and treat `None` as "leave
  alone" (see `update_transcription`).
- **Tag cycles** from concurrent moves are broken deterministically in
  `recompute_head` (the largest id on the cycle loses; a derived head with no
  parent; a scalar conflict). Recursive tag queries use `UNION` or a depth
  limit; keep it that way.
- **Batches and pages:** feed pages are bounded by `FEED_BYTE_BUDGET` (4 MB) in
  both directions; keep any new feed content under it rather than raising the
  server's body limit. The client pages an initial sync from cursor zero;
  `/sync/full` is for tools only.

## History

Two defects of January 2026, kept because the rules they left behind still
hold. Timestamps were then text (`YYYY-MM-DD HH:MM:SS`) compared as strings;
since `migrate_timestamps_to_unix` every timestamp is an `INTEGER` of Unix
seconds (`SYNC_SPECIFICATION.md` 3.4), and `validate_datetime()` in
`validation.rs` checks them at the sync boundary.

- **A shared limit starved entity types** (fixed 2026-01-09). The timestamp
  feed took one limit for every entity type together, so a device with a
  thousand changed notes never sent its tags, links, recordings or
  transcriptions. `get_changes_since` now gives each entity type its own limit
  (PROTO-7). The cursor feed, which the clients use, has one global limit and
  loses nothing because it resumes.
- **A NULL `modified_at` hid rows from the timestamp feed** (fixed 2026-01-09).
  A row created on one device had `modified_at` NULL; a peer that stored it so,
  and whose other peers' `since` was later than its `created_at`, never offered
  it again. A row received by sync never keeps `modified_at` NULL when the
  incoming row had a timestamp (PROTO-6).
