# VoiceCore: The Rust Core Library for Voice

- **Purpose**: VoiceCore is the shared core of the Voice Family: notes with hierarchical tags, recordings and their transcriptions, versioned fields, accounts, pairing and device-to-device sync.
- **Architecture**: one Rust library used by the desktop and server (Python, through PyO3 in `Voice/rust/voice-python`) and by the Android application (Kotlin, through UniFFI in `src/android.rs`).
- **No system libraries**: SQLite is bundled and TLS is pure Rust (rustls), so no OpenSSL or other native library is needed.

The numbered rules this code implements (for example `PROTO-12`) are in
`../SYNC_SPECIFICATION.md`; the words used for sync are defined in
`../TECHNICAL-DECISIONS.md` 4.5.

## Features

- **Notes**: create, read, edit and delete, with deletion recorded as a versioned field.
- **Trash bin**: a deleted note keeps its history and its recordings and can be recovered (`get_deleted_notes`, `undelete_note`), or removed for good (`purge_note`), which travels to every device and cannot be undone (PURGE-1..PURGE-9).
- **Hierarchical tags**: a tree of tags with parent-child relationships.
- **Search**: notes by text and by tag, with hierarchical tag paths.
- **Versioned fields**: every editable value has a Git-like history; concurrent edits are merged three-way and flagged, never overwritten.
- **Accounts**: every database belongs to one account; one installation can hold several (ACCT-1..ACCT-10).
- **Device keys and cards**: every request between devices is authenticated by a device key whose hash is on the device's card (AUTH-1..AUTH-9, CARD-1..CARD-3).
- **Pairing**: a fresh device joins an account by reading a code (PAIR-1..PAIR-5).
- **Sync**: database changes exchanged with a device through a resumable cursor feed, protocol version 2.0 (PROTO-12, PROTO-13).
- **Recording files**: uploaded to and downloaded from a bucket (S3 or S3-compatible), or sent to and fetched from a device; a sync never moves a file (FILE-12, `TECHNICAL-DECISIONS.md` 4.5).
- **Encryption of recordings in the bucket**: optional, AES-256-GCM in chunks (ENC-1..ENC-4).
- **Snapshots and backups**: a copy of the database before anything that rewrites it in one step, and a periodic backup (SNAP-1..SNAP-5).
- **Issues**: what the user should know about, computed on request (ISSUE-1).
- **TLS**: every listener serves HTTPS with its own self-signed certificate, pinned by fingerprint (AUTH-7).

## Architecture

```
src/
├── lib.rs                # Library entry point and public API re-exports
├── models.rs             # Core data structures (Note, Tag, NoteTag, NoteAttachment, AudioFile, Transcription, SyncChange)
├── database.rs           # SQLite data access, the change feed, snapshots and backups
├── error.rs              # Error types
├── validation.rs         # Input validation
├── config.rs             # config.json: device identity, devices, backup, keys
├── accounts.rs           # Several accounts on one installation (feature "server")
├── auth.rs               # Device keys, device cards, request verification
├── pairing.rs            # Setup texts, pairing tokens, admission by token
├── sync_protocol.rs      # The protocol's messages, headers and refusal codes
├── sync_client.rs        # Sync, send, fetch, deliver, exchange, join, grant, check
├── sync_server.rs        # The listener (Axum): routes, authentication, hosting, backups
├── sync_apply.rs         # Applying a batch of sync changes (shared by server and client)
├── versions.rs           # Versioned fields: history, three-way merge, conflicts, device cards, settings
├── conflicts.rs          # Thin conflict layer over versions.rs
├── merge.rs              # Text merging algorithms
├── search.rs             # Parser for combined tag and text search queries
├── file_storage.rs       # Upload to and download from the bucket (feature "file-storage")
├── file_storage_s3.rs    # S3 and S3-compatible storage service (feature "file-storage")
├── bucket_setup.rs       # Creating and hardening the bucket (feature "file-storage")
├── transfer.rs           # Streamed, resumable, hash-verified file transfer between instances
├── crypto.rs             # Encryption of recordings in the bucket
├── issues.rs             # What the user should know about (ISSUE-1)
├── waveform.rs           # Waveform levels kept with a recording (FILE-20)
├── timezone.rs           # The timezone an action happened in
├── tls.rs                # Certificates, fingerprints, pinned TLS configurations
├── android.rs            # UniFFI bindings for Android (feature "uniffi")
├── convergence_tests.rs  # Property tests over random multi-device fleets (tests only)
├── timezone_tests.rs     # Timezone tests (tests only)
└── bin/uniffi-bindgen.rs # Kotlin binding generator (feature "uniffi")
```

### Times and timezones

Every timestamp is stored as an `INTEGER` count of seconds since the Unix
epoch: an instant, the same number on every device.

An instant alone cannot say what the clock read where something happened, so
each user-visible timestamp has two columns beside it:

| Column | Meaning |
|--------|---------|
| `<stamp>_offset` | Seconds east of UTC on the device at that moment (Jerusalem in summer is 10800) |
| `<stamp>_zone` | IANA name such as `Asia/Jerusalem`, when the device knew one |

They are written for `created_at`, `modified_at` and `deleted_at` on notes,
tags, note-tag links, attachments and transcriptions, for `imported_at` and
`file_created_at` on audio files, for `purged_at` on purges, and for
`created_at` on every field version. Sync bookkeeping (`sync_received_at`,
`last_sync_at`, `seq`) and the bucket upload time (`storage_uploaded_at`) have
none: no screen shows them (TZ-1, TZ-2).

This is what lets a note recorded at 15:20 in Jerusalem still read 15:20 after
its author flies to New York. A reader renders the instant at the recorded
offset; only a row with no offset, written by a device that never reported one,
falls back to the reader's own timezone.

The offset cannot be recovered from the instant afterwards, so the platform
tells the core its timezone with `timezone::set_local_timezone(offset, name)`
at start and whenever it changes (TZ-5). Android in particular keeps the zone
in its framework, where a native library cannot see it. Locale and the 12 or
24-hour preference are deliberately **not** stored: they belong to whoever is
reading, not to the event, and each interface applies its own.

### Module Overview

| Module | Purpose |
|--------|---------|
| `models` | Core data structures with UUID7 identifiers; `audio_local_path`, `audio_file_extension`, `AUDIO_FILE_FORMATS` (FILE-21) |
| `database` | SQLite persistence: the schema (`create_schema`, `SCHEMA_VERSION`), CRUD, queries, the cursor feed (`get_changes_after_seq`), row-apply upserts, where each copy of a recording is (`set_file_location`, `check_files_here`, `store_content_hash`, `made_here_but_missing`), the removal of a copy (`promise_to_keep`, `begin_removal`, `finish_removal`, `abandon_removal`, FILE-26), snapshots (`snapshot`, `restore_snapshot`), backups (`backup_to`), account identity (`account_id`, `move_to_account`) |
| `error` | `VoiceError` and `ValidationError` |
| `validation` | UUID, audio extension, tag name, tag path, note content and search query validation |
| `config` | `config.json`: device id and name, device key and recording key (wrapped by a `SecretWrapper` on the phone, AUTH-9), devices, sync settings, backup settings, public URL. The bucket's configuration is not here: it is in the database, synced (`file_storage_config`) |
| `accounts` | The account index `accounts.db` of an installation root, hosting offers, and `resolve` (ACCT-6..ACCT-9); compiled with feature `server` |
| `auth` | Device keys, key hashes, the device's own card, and `verify_request` (AUTH-1..AUTH-6, CARD-1, CARD-2) |
| `pairing` | Setup texts (`voice://pair?...`), tokens, `offer`, `offer_hosting`, `admit_by_token`, `check_can_join` (PAIR-1..PAIR-5) |
| `sync_protocol` | Request and response types, `PROTOCOL_VERSION`, refusal codes, header names, one definition for both sides |
| `sync_client` | `SyncClient`: `sync_with_device`, `pull_from_device`, `push_to_device`, `initial_sync`, `send_to_device`, `fetch_from_device`, `deliver`, `exchange`, `join`, `grant_host`, `move_to`, `check`, `adopt_devices_from_cards`, `remove_local_copy` (FILE-26), a device reached at the addresses on its card (LISTEN-4), cancel and progress (FILE-17) |
| `sync_server` | The listener: routes, the device-key middleware, the LAN gate, refusal delays, hosting several accounts (`IndexedAccounts`), the request log per hosted account, the periodic backup, the idle stop (LISTEN-5), where the listener can be reached (`listen_addresses`, LISTEN-4) |
| `sync_apply` | Applying incoming changes in dependency order, with retry of failures and the purge check |
| `versions` | Field version graph, three-way merge, conflict records, synced settings, device cards |
| `conflicts` | Thin conflict layer over `versions` |
| `merge` | Line-by-line diff and three-way merge algorithms |
| `search` | Parser for combined tag and text search queries (`parse_search_input`, `execute_search`) |
| `file_storage` | Upload to the bucket in parts (FILE-19), download, the storage key (`storage_key_for`), re-upload encrypted (ENC-3) |
| `file_storage_s3` | `S3StorageService` over rust-s3 0.37 or newer (FILE-24) |
| `bucket_setup` | The bucket wizard's calls: policy, lifecycle rules, TLS-only policy, signed requests, explained failures (BUCKET-1..BUCKET-5) |
| `transfer` | Part files, completion by length and SHA-256, `Range` and `Content-Range` parsing, free space checks, stall timeouts (FILE-13, FILE-14) |
| `crypto` | The recording key and the chunked AES-256-GCM format (ENC-1, ENC-2) |
| `issues` | Recordings not in the bucket and why, orphaned rows, tags whose names contain whitespace (ISSUE-1) |
| `waveform` | Encoding, decoding and drawing waveform levels (FILE-20) |
| `timezone` | The local offset and IANA zone name reported by the platform |
| `tls` | Self-signed certificate generation, fingerprints, the server configuration, the pinned client configuration |
| `android` | `VoiceClient` and the records, errors and callback interfaces exposed to Kotlin |

## Requirements

- Rust with the 2021 edition. `Cargo.toml` declares no minimum Rust version.
- No external system libraries (SQLite is bundled, TLS is rustls).

## Building

### Features

| Feature | Default | What it adds |
|---------|---------|--------------|
| `server` | yes | The listener: axum, axum-server, tower, tower-http, tokio-util; the `accounts` module |
| `desktop` | yes | Host name detection and the default configuration directory (`hostname`, `dirs`) |
| `file-storage` | yes | The bucket: rust-s3, tokio-util; the `file_storage`, `file_storage_s3` and `bucket_setup` modules |
| `uniffi` | no | The UniFFI bindings (`android` module) and the `uniffi-bindgen` binary |

Without `desktop`, `Config::new` requires a configuration directory.

### As a Standalone Library

```bash
cargo build --release
```

Builds go to `VoiceFamily/.cargo-target` (`../TECHNICAL-DECISIONS.md` 7.4), not
to a `target/` directory under the crate.

### Building Documentation

```bash
cargo doc --open
```

## Usage

### Basic Note Operations

```rust
use voicecore::{Config, Database};

// The configuration directory: the given one, or ~/.config/voice with feature "desktop".
// The second argument is an optional SecretWrapper (the phone's Keystore; none on the desktop).
let config = Config::new(None, None)?;
let db = Database::new(config.database_file())?;

// Create a note; returns its id as 32 hex characters
let note_id = db.create_note("My first note")?;

// Edit its content (a new version of the field)
db.update_note(&note_id, "Updated content")?;

// Read a note
if let Some(note) = db.get_note(&note_id)? {
    println!("Note content: {}", note.content);
}

// List all notes
let notes = db.get_all_notes()?;

// Delete a note: it goes to the trash and can be recovered
db.delete_note(&note_id)?;
db.undelete_note(&note_id)?;
```

### Tag Operations

```rust
// Create a root tag
let work_id = db.create_tag("Work", None)?;

// Create a child tag
let projects_id = db.create_tag("Projects", Some(&work_id))?;

// Add a tag to a note
db.add_tag_to_note(&note_id, &work_id)?;

// Read all tags, and the tags of one note
let tags = db.get_all_tags()?;
let note_tags = db.get_note_tags(&note_id)?;
```

### Search

```rust
// Parse and run a search as typed by the user: text and tag terms
// (hierarchical paths such as Europe/France/Paris are accepted)
let result = voicecore::search::execute_search(&db, "quarterly tag:Work")?;

// Or search directly: text, and groups of tag ids
let results = db.search_notes(Some("meeting notes"), None)?;
let groups = vec![vec![work_id.clone()]];
let results = db.search_notes(None, Some(&groups))?;
```

### Synchronization

```rust
use std::sync::{Arc, Mutex};
use voicecore::sync_client::SyncClient;

let db = Arc::new(Mutex::new(Database::new(&db_path)?));
let config = Arc::new(Mutex::new(Config::new(None, None)?));
let client = SyncClient::new(db.clone(), config.clone())?;

// Sync with a device: database changes only, both directions
let result = client.sync_with_device("from_device_id").await;
println!("Pulled: {}, Pushed: {}, Conflicts: {}, Request {}",
    result.pulled, result.pushed, result.conflicts, result.request_id);

// Deliver (sync, then send) and exchange (sync, then send and fetch)
let result = client.deliver("from_device_id").await;
let result = client.exchange("from_device_id").await;

// Listen for devices: HTTPS on this device's certificate; the last argument is plain_http,
// which is allowed only on a loopback address (AUTH-7)
voicecore::sync_server::start_server(db, config, "0.0.0.0", 8384, false).await?;
```

### Conflict Resolution

Every editable field is versioned (see `versions.rs`). A write creates a new
version whose parent is the current head; sync exchanges versions; when a field
has two leaves they are merged three-way from their lowest common ancestor and
the merge becomes the head. Merges that needed a human carry a `conflict_kind`,
so every device records the same conflict with the same id.

```rust
// Unresolved conflicts, newest first
for c in db.get_conflicts(false)? {
    println!("{} {} {} ({}): {} vs {}", c.entity_type, c.entity_id, c.field, c.kind,
             c.device_a_name.unwrap_or_default(), c.device_b_name.unwrap_or_default());
}

// Accept the merged value as it stands (a new version descending from the merge)
db.accept_conflict(&conflict_id)?;

// Or write a corrected value (a normal edit of the field)
db.resolve_conflict_with_content(&conflict_id, "טקסט מתוקן")?;

// Any later edit of the field resolves the conflict as well
db.update_note(&note_id, "טקסט מתוקן")?;

// History of one field
let versions = db.get_field_history("note", &note_id, "content")?;
```

Merge rules by field kind (`FIELD_REGISTRY` in `versions.rs`, MERGE-T, MERGE-S, MERGE-F, MERGE-M, MERGE-D):

| Kind | Fields | Concurrent change |
|------|--------|-------------------|
| Text | note.content, transcription.content, audio_file.summary | diff3 line merge; overlapping edits kept between `<<<<<<< VERSION A` / `>>>>>>> VERSION B` markers and flagged |
| Scalar | note.primary_attachment, audio_file.primary_transcription, tag.name, tag.parent, setting.value, device.name, device.certificate_fingerprint, device.addresses, device.listens, device.key_hash, device.application | later version wins, flagged |
| Flags | transcription.state | per-flag union/intersection against the base, flagged only when the same flag was toggled both ways |
| Membership | note_tag.active, note_attachment.active, device.revoked | disagreement keeps the link attached (a device stays revoked), flagged |
| Deleted | note.deleted, transcription.deleted, audio_file.deleted, tag.deleted | disagreement keeps the entity alive, flagged; a delete that did not see a concurrent edit is overridden by the edit (delete conflict) |

### Configuration

```rust
use std::path::PathBuf;

// Load or create config
let mut config = Config::new(Some(PathBuf::from("/path/to/config/dir")), None)?;

// Device identity
println!("Device ID: {}", config.device_id_hex());
println!("Device Name: {}", config.device_name());

// Add a sync device: id, name, URL, pinned certificate fingerprint, whether an existing entry may be replaced
config.add_device(
    "0199aaaaaaaa70008000000000000001",
    "HomeServer",
    "https://192.168.1.20:8384",
    Some("SHA256:aa:bb:..."),
    false,
)?;

// List configured devices
let devices = config.devices();

// Remove a device (returns whether one was removed)
config.remove_device("0199aaaaaaaa70008000000000000001")?;
```

## API Reference

### Core Types

The structures in `models.rs`. Database queries return row structures with
integer timestamps instead (`NoteRow`, `TagRow`, ... in `database.rs`).

#### Note

```rust
pub struct Note {
    pub id: Uuid,                           // UUID7 identifier
    pub created_at: DateTime<Utc>,          // Creation timestamp
    pub content: String,                    // Note content
    pub device_id: Uuid,                    // Device that last modified the note
    pub modified_at: Option<DateTime<Utc>>, // Last modification
    pub deleted_at: Option<DateTime<Utc>>,  // Deletion timestamp (in the trash)
}
```

#### Tag

```rust
pub struct Tag {
    pub id: Uuid,                           // UUID7 identifier
    pub name: String,                       // Tag name (at most 100 bytes)
    pub device_id: Uuid,                    // Device that last modified the tag
    pub parent_id: Option<Uuid>,            // Parent tag for hierarchy
    pub created_at: Option<DateTime<Utc>>,  // Creation timestamp
    pub modified_at: Option<DateTime<Utc>>, // Last modification
}
```

#### NoteTag

```rust
pub struct NoteTag {
    pub note_id: Uuid,                      // Associated note
    pub tag_id: Uuid,                       // Associated tag
    pub created_at: DateTime<Utc>,          // Association creation
    pub device_id: Uuid,                    // Creating device ID
    pub modified_at: Option<DateTime<Utc>>, // Last modification
    pub deleted_at: Option<DateTime<Utc>>,  // When the association was removed
}
```

### Error Types

```rust
pub enum VoiceError {
    Validation { field: String, message: String },
    Database(rusqlite::Error),
    DatabaseOperation(String),
    Sync(String),
    Network(String),
    Tls(String),
    Config(String),
    Io(std::io::Error),
    Json(serde_json::Error),
    Uuid(uuid::Error),
    NotFound(String),
    Conflict(String),
    Other(String),
}

pub struct ValidationError {
    pub field: String,
    pub message: String,
}
```

A refusal between devices carries, besides its sentence, one of the codes in
`sync_protocol::codes` (see "Refusal codes" below).

### Sync Types

```rust
pub struct SyncResult {
    pub success: bool,
    pub pulled: i64,              // Changes received from the device
    pub pushed: i64,              // Changes sent to the device
    pub conflicts: i64,           // Conflicts flagged
    pub sent: i64,                // Recordings sent to the device (deliver, exchange, send)
    pub fetched: i64,             // Recordings fetched from the device (exchange, fetch)
    pub bytes_moved: u64,         // Bytes of recordings moved in either direction
    pub errors: Vec<String>,      // Problems that made the operation incomplete
    pub warnings: Vec<String>,    // Problems that did not affect the database changes
    pub request_id: String,       // The operation's id, on every request and log line (DIAG-2)
    pub clock_skew_seconds: i64,  // The device's clock minus this device's, past one minute (DIAG-3)
}
```

## Database Schema

VoiceCore uses SQLite with UUID7 as BLOB primary keys. The schema is made in one
place, `Database::create_schema()` in `database.rs`, in one write transaction:
`create_tables`, `create_version_tables`, `create_sequence_triggers` (the `seq`
column's triggers of every syncable table), `create_identity` (`database_id` and
`account_id`) and `create_system_tags`. The file is then stamped
`PRAGMA user_version = SCHEMA_VERSION` (1).

Nothing converts a database written by another build. A database that has
tables and another schema number is refused, and not opened, with the sentence:

> This database was written by another version of Voice (schema N; this version reads schema 1) and is not opened. Start with an empty data directory.

### Core Tables

| Table | Purpose |
|-------|---------|
| `notes` | Note content with timestamps and deletion |
| `tags` | Hierarchical tag definitions |
| `note_tags` | Many-to-many note-tag associations |
| `note_attachments` | Recordings attached to notes |
| `audio_files` | Recording metadata: `disk_name`, `content_sha256`, `size_bytes`, `waveform_levels`, bucket location, `storage_encrypted`, `origin_device_id` and `origin_kind` (FILE-25) |
| `transcriptions` | Transcriptions of recordings, with their flags (`state`) |
| `devices` | Denormalised device cards (CARD-1) |

### Sync Infrastructure

| Table | Purpose |
|-------|---------|
| `sync_devices` | Known devices: cursors, last sync time and operation (PROOF-3), the remembered address, the account at the last agreeing handshake (ACCT-3), the entity types the device declared (PROTO-13) |
| `sync_failures` | Changes that could not be applied, kept for retry |
| `field_versions` | Append-only version graph of every editable field |
| `field_heads` | Current head version per field |
| `field_conflicts` | Merges that need a human, with the two versions and devices |
| `field_deferred` | Fields whose row could not be written yet (HEAD-7) |
| `synced_settings` | Settings shared by every device (denormalised heads) |
| `purges` | What was removed for good, so that no device can bring it back |
| `file_storage_config` | The single-row bucket configuration, synced |
| `file_locations` | Where each copy of a recording is, synced (FILE-22) |
| `sync_sequence`, `sync_meta` | Write-order counter, `database_id` and `account_id`; every syncable table has a `seq` column stamped by triggers |
| `upload_parts` | Journal of multipart uploads to the bucket (FILE-19) |
| `purged_objects` | Bucket objects waiting for their purge tag (BUCKET-2) |
| `pending_file_renames` | Renames of recordings waiting for a caller that knows the audio folder (FILE-15) |
| `file_holds` | Promises this device gave a device to keep its copy of a recording until a time, while that device removes its own (FILE-26); local, never synced |
| `file_removals` | Removals of this device's copy of a recording that are under way (FILE-26); local, never synced |
| `pairing_offers` | Hashes of the tokens of shown codes, local (PAIR-2) |

The account index `accounts.db` (tables `accounts` and `hosting_offers`) is a
separate file in the installation root, not part of an account's database.

## Accounts

Every database belongs to one account, `sync_meta.account_id`: 32 hex
characters, the same on every device of the account (ACCT-1). A database is
authoritative for its own account: `Database::new_for_account(path, id)` gives a
fresh, unused database the id, and a database that holds notes or has synced
under another id is refused with `ACCOUNT_DISAGREES` (ACCT-4). The only way a
database changes account is `move_to_account`, which takes a snapshot first,
rewrites the id and forgets every device (ACCT-5).

One installation root can hold several accounts (`accounts.rs`, ACCT-6..ACCT-9):

```text
<root>/
  config.json        machine: device id and name, listen port, backup, public URL
  accounts.db        the index, never synced
  certs/
  <account id>/      notes.db  config.json  audio/  snapshots/
```

`accounts::resolve(root, selector, create_default)` opens the account named by
id, unique id prefix or label, else the default. A root that holds `notes.db` or
`config.json` and no index is the account itself (the phone, a test directory).
The index is only an index: a disagreement with the database inside an account
directory is reported, never corrected. A **hosted** account is served for
someone else and is never the default.

## Authentication

Every device holds one **device key** per account: 32 random bytes as 43
base64url characters (`auth::generate_device_key`), in clear only in that
device's `config.json` (wrapped on the phone, AUTH-9). Every other device holds
its hex SHA-256 on the device's card (`key_hash`); hashes are compared in
constant time (AUTH-1, AUTH-2).

The **device card** is the entity `device`, id = the device id, with the
versioned fields `name`, `certificate_fingerprint`, `addresses` (JSON list of
URLs), `listens` (`"0"` / `"1"`), `key_hash`, `revoked` and `application`. The
cards travel in the feed as `field_version` changes, so every device of the
account knows every other (CARD-1). A device writes its own card at every start
(`auth::ensure_own_device_card`); `revoked` can be set by any device and cleared
by none (CARD-2). After every sync the cards become the device list (CARD-3,
`SyncClient::adopt_devices_from_cards`).

### Request headers

Every authenticated route carries:

| Header | Value |
|--------|-------|
| `X-Account-ID` | The account the request is for (`auth::HEADER_ACCOUNT`) |
| `X-Device-ID` | The calling device (`auth::HEADER_DEVICE`) |
| `Authorization` | `Bearer <device key>` |
| `X-Request-ID` | Optional: the operation's id, up to 32 hex characters, written in the logs of both sides (DIAG-2) |

The middleware `require_device` checks, in this order (`auth::verify_request`,
AUTH-3): an account the server does not hold (404 `ACCOUNT_UNKNOWN`), a missing
key or device (401 `KEY_MISSING`), a device with no card (401 `DEVICE_UNKNOWN`),
a revoked card (401 `DEVICE_REVOKED`), a key that does not hash to the card's
hash (401 `KEY_WRONG`). After three refusals from one address, each further
refusal is answered after a wait of 2, 4 and then 8 seconds; a success clears the
count; counts are forgotten after ten minutes and the table is emptied past ten
thousand addresses (AUTH-5). The key is never written to a log or an error.

A listener over one directory serves its one account (`SingleAccount`); a
listener over an indexed root (`start_hosting_server`) serves every account of
the index through `IndexedAccounts`, opening each on its first request and
keeping at most 64 open, and writes a request log `audit.log` beside each hosted
account's database, rotated at 5 MiB (AUTH-4, AUTH-8).

The LAN gate `lan_only_gate` refuses a caller whose address is not private, link-local
or loopback with 403 `NOT_ON_LAN` unless the machine has a `public_url`
(LISTEN-3). In `create_router_for` it is attached with `route_layer` to the open
routes (`/sync/status`, `/pair/claim`, `/pair/grant`); the authenticated routes
are merged into the router after that layer.

### Refusal codes

Every error response is `{"error": "<sentence>", "code": "<code>"}`; `code` is
empty for an error that has none. The codes (`sync_protocol::codes`):

| Code | Meaning |
|------|---------|
| `ACCOUNT_MISSING` | The handshake named no account (400) |
| `ACCOUNT_MISMATCH` | The two sides hold different accounts; nothing is exchanged (403) |
| `ACCOUNT_DISAGREES` | A database opened for one account already belongs to another |
| `ACCOUNT_UNKNOWN` | The request names an account this server does not hold (404) |
| `KEY_MISSING` | The request carries no device key, or no device id (401) |
| `DEVICE_UNKNOWN` | The account holds no card for the device (401) |
| `DEVICE_REVOKED` | The device's card is marked revoked (401) |
| `KEY_WRONG` | The key does not hash to the card's key hash (401) |
| `DEVICE_MISMATCH` | The handshake body names a device other than the headers do (400) |
| `TLS_REQUIRED` | Plain http to or on an address that is not this machine |
| `CERTIFICATE_MISMATCH` | The device's certificate is not the pinned one |
| `TOKEN_INVALID` | The pairing or grant token is unknown, spent, expired or mistyped (403) |
| `SETUP_TEXT_INVALID` | The setup text, or a grant, could not be read |
| `DEVICE_HOLDS_NOTES` | This device holds notes of another account and will not be paired over them |
| `PROTOCOL_TOO_OLD` | The device speaks a protocol version below 2 (426) |
| `NOT_ON_LAN` | The caller is not on a private network and this listener has no public address (403) |

## Pairing

The device that holds the account **shows a code** (`pairing::offer`): a setup
text `voice://pair?v=1&a=<account>&t=<token>&d=<device id>&u=<url,...>&f=<fingerprint>`,
also drawn as a QR code. The fingerprint's 32 bytes travel as 43 base64url
characters. The token lives ten minutes (`TOKEN_LIFETIME_SECONDS`); only its hash
is kept, in `pairing_offers`; the first right token spends it and the fifth wrong
token withdraws it (PAIR-1, PAIR-2).

- **Claim** (PAIR-3, PAIR-4): the reading device (`SyncClient::join`) refuses a
  code for another account when it holds notes (`DEVICE_HOLDS_NOTES`,
  `pairing::check_can_join`), then posts `/pair/claim` over TLS pinned to the
  fingerprint in the text. The shower (`pairing::admit_by_token`) makes a device
  key for the reader, writes the reader's card with the key's hash, and answers
  with the account id, the key, its own card and, when it holds one, the
  account's recording key (ENC-1). A code's `u=` carries every address of the
  showing device; the reading device tries each in turn, each with its own
  client, and remembers as the device's address the one that answered (LISTEN-4).
- **Grant** (PAIR-5): a server that holds no account shows a grant text (`g=1`,
  no account id; `pairing::offer_hosting`, token hashed in the root's
  `hosting_offers`). The holder (`SyncClient::grant_host`) makes a key for the
  server and posts `/pair/grant` with the account id, that key, its own card, a
  label and the recording key when it has one. The server registers the account
  as hosted, stores the key, admits the holder's card, writes its own card and
  answers with it.

### Where a listener can be reached

`sync_server::listen_addresses(host, port, plain_http)` (LISTEN-4) returns
`ListenAddresses` with four fields, used for the device's own card, for a code
and for a screen:

| Field | Meaning |
|-------|---------|
| `detected` | Whether this device found its address |
| `shown` | What a screen shows: the address found alone, or every candidate |
| `urls` | Every URL another device tries, in order |
| `sentence` | What a screen says beside `shown`; empty when the address was found |

A listener bound to one address reports that address. A listener bound to every
address takes as candidates the private IPv4 addresses of interfaces that can
carry a local network; interfaces whose names start with `docker`, `br-`,
`veth`, `virbr`, `vmnet`, `vboxnet`, `tun`, `tap`, `wg`, `zt`, `tailscale`,
`lxc`, `lxdbr`, `cni`, `flannel`, `podman`, `kube`, `dummy`, `rmnet`, `ccmni`,
`p2p`, `utun` and `ipsec` are left out, and a link-local address is a candidate
only when there is nothing else. The source address of this machine's route (a
UDP socket connected to `192.0.2.1`, nothing sent) is the address found, shown
alone and tried first; a single candidate is also the address found. Otherwise
every candidate is shown with the sentence "Only one of these addresses is
correct; this device could not tell which. Another device tries each of them in
turn." With no candidate the sentence is "No address on a local network was
found. Is this device on a network?". The host name, when it is not
`localhost`, is the last of `urls`.

When a device's remembered address does not answer (a network error, not a
refusal), the sync client tries each address on the device's device card in turn,
with the device's pinned certificate, and remembers for that device the one that
answers. Sync, pull, push and the initial sync all reach a device this way.

## Sync Protocol

This section documents the sync protocol for implementing new clients.

### Protocol Overview

Transport is HTTPS. A listener serves its own self-signed certificate
(`certs/server.crt`, made when missing); a caller verifies a device by its pinned
fingerprint, or against the system's root certificates when no fingerprint is
pinned. Plain http is accepted only to and on a loopback address (AUTH-7). All
timestamps are Unix seconds as JSON integers.

The protocol version is `2.0` (`sync_protocol::PROTOCOL_VERSION`). A handshake
from a device whose major version is below 2 is refused with HTTP 426 and
`PROTOCOL_TOO_OLD`, "Update Voice on <device name>"; nothing negotiates with
version 1 (PROTO-12). The handshake carries `application` and `entity_types`,
so another application can share the account's tags without ever seeing a note
(PROTO-13).

A **sync** exchanges database changes with a device, both directions, and moves no
file. Recording files move only by **upload** and **download** (the bucket) and
by **send** and **fetch** (a device); **deliver** is sync then send, and
**exchange** is sync then send and fetch (`TECHNICAL-DECISIONS.md` 4.5).

### Endpoints

| Method | Path | Authentication | Description |
|--------|------|----------------|-------------|
| `POST` | `/sync/handshake` | device key | Identity, account and protocol check; `database_id` and cursor |
| `GET` | `/sync/changes?cursor=<n>&limit=<m>&types=<a,b>` | device key | One page of the cursor feed |
| `POST` | `/sync/apply` | device key | Apply a batch of the caller's changes |
| `POST` | `/sync/audio/missing` | device key | Of the recording ids the caller names, the ones this instance lacks (FILE-12) |
| `GET` | `/sync/audio/<audio_id>/file` | device key | Fetch a recording's file from this device, resumable with `Range` (FILE-12, FILE-13) |
| `POST` | `/sync/audio/<audio_id>/file` | device key | Send a recording's file to this device, resumable with `Content-Range` (FILE-12, FILE-13) |
| `POST` | `/sync/audio/<audio_id>/keep` | device key | The caller is removing its copy of a recording; this device promises to keep its own for 10 minutes, or says why not (FILE-26) |
| `GET` | `/sync/status` | none | Health check and identity |
| `POST` | `/pair/claim` | token | A reading device claims a key with a code's token (PAIR-3) |
| `POST` | `/pair/grant` | token | A holder gives an empty server the account (PAIR-5) |

The changes feed is gzip-compressed when the caller sends
`Accept-Encoding: gzip`; the file routes never are (DIAG-5). The listener's body
limit (`sync.max_sync_file_size_mb`) applies to the JSON routes; a file sent to
`POST /sync/audio/<audio_id>/file` is read as a stream past it.

### Endpoint Details

#### POST /sync/handshake

**Request** (`HandshakeRequest`):
```json
{
    "device_id": "0199aaaaaaaa70008000000000000001",
    "device_name": "Phone",
    "protocol_version": "2.0",
    "account_id": "0199bbbbbbbb7000800000000000000b",
    "application": "voice",
    "entity_types": []
}
```

`entity_types` empty means every type. The server remembers the declared types
for the caller (`set_device_entity_types`).

**Response** (`HandshakeResponse`):
```json
{
    "device_id": "0199cccccccc70008000000000000002",
    "device_name": "Desk",
    "protocol_version": "2.0",
    "account_id": "0199bbbbbbbb7000800000000000000b",
    "application": "voice",
    "server_timestamp": 1705320000,
    "supports_audiofiles": true,
    "free_bytes": 52341234567,
    "database_id": "0193...",
    "cursor": 4821
}
```

The checks, in the order the handler runs them after the device-key middleware:
`device_id` not 32 hex characters (400), a protocol major version below 2 (426
`PROTOCOL_TOO_OLD`), an `X-Device-ID` header naming another device than the body
(400 `DEVICE_MISMATCH`), an empty `account_id` (400 `ACCOUNT_MISSING`), an
`account_id` other than the server's (403 `ACCOUNT_MISMATCH`). The server then
takes a snapshot (SNAP-3), records the caller's account, and compares its audio
folder with what it has stated about its own copies (FILE-22).

`database_id` identifies the database; when a device sees it change it forgets its
cursors and exchanges everything again (PROTO-9). `cursor` is the end of the
feed at handshake time. `supports_audiofiles` is `true` when the account has an
audio directory configured. `free_bytes` is the free space on the responder's
disk, 0 when unknown. `server_timestamp` is compared with the caller's clock; a
difference past one minute is reported (DIAG-3).

#### GET /sync/changes

`?cursor=<n>&limit=<m>` returns every row and every authored or published
version whose write-order sequence number is greater than `cursor`, oldest
first, at most `limit` changes and about 4 MB of JSON in total
(`FEED_BYTE_BUDGET`; at least one change), plus `next_cursor` (pass it back to
continue) and `is_complete`. `limit` counts the changes of every entity type
together; there is no limit per type, and nothing is skipped, because the next
page continues from `next_cursor`. Exact, resumable, and independent of clocks.
The Rust client loops until `is_complete`; an initial sync is the same loop from
cursor 0 (FLOW-1, FLOW-4).

**Query Parameters** (`ChangesQuery`):
- `cursor` (optional): write-order position; absent is the start of the feed.
- `limit` (optional): default 1000, maximum 10000, every entity type together.
- `types` (optional): comma-separated entity types; only those are returned, while the cursor still walks the whole feed (PROTO-13).

**Response** (`ChangesResponse`):
```json
{
    "changes": [ /* array of change objects */ ],
    "next_cursor": 5321,
    "database_id": "0193...",
    "device_id": "0199cccccccc70008000000000000002",
    "device_name": "Desk",
    "is_complete": false
}
```

A client that receives no `next_cursor` fails the sync with a sentence (FLOW-7).

#### POST /sync/apply

**Request** (`ApplyRequest`):
```json
{
    "device_id": "0199aaaaaaaa70008000000000000001",
    "device_name": "Phone",
    "changes": [ /* array of change objects */ ]
}
```

**Response** (`ApplyResponse`):
```json
{
    "applied": 15,
    "conflicts": 2,
    "errors": ["Error applying note_tag ...: ..."]
}
```

When the caller declared entity types in its handshake, changes of other types
are not applied, and a sentence in `errors` says how many (PROTO-13). A
`device_id` that is not 32 hex characters is refused with 400. The handler
answers 200 with this body whenever the batch ran; an error that escapes the
batch is 500. The server records the time of the sync with the caller
(`update_device_sync_time`).

#### GET /sync/status

Health check; needs no key.

**Response** (`StatusResponse`):
```json
{
    "device_id": "0199cccccccc70008000000000000002",
    "device_name": "Desk",
    "protocol_version": "2.0",
    "status": "ok",
    "supports_audiofiles": true
}
```

The status names no account: a single-account listener answers
`supports_audiofiles` for its one account; a listener that hosts several answers
`false`, and the handshake of each account answers for that account.

#### POST /sync/audio/missing

**Request** (`MissingFilesRequest`): `{"audio_ids": ["<hex id>", ...]}`, the
recordings the sender holds.

**Response** (`MissingFilesResponse`): `{"missing": ["<hex id>", ...], "partial": {"<hex id>": <bytes>}}`,
the ids the receiver lacks and, for those it holds a part of, how many bytes it
has. An id with no row on the receiver is left out (its sync has not arrived).
The receiver states that the caller holds every id it named (FILE-22).

#### GET /sync/audio/<audio_id>/file

Streams the file named by the row's `disk_name` from the audio directory, from
the byte a `Range: bytes=N-` header names (206 with `Content-Range`, else 200).
Headers: `Content-Length`, `Accept-Ranges: bytes`, and `X-File-SHA256` with the
whole file's hex SHA-256 (the row's `content_sha256`, computed and stored once
when the row has none). A file kept as the bucket holds it by a device without
the recording key is served as it is, with the hash of those bytes and
`X-Voice-Encrypted: 1` (ENC-4). 404 when the row or the file is not there; 400
when no audio directory is configured.

#### POST /sync/audio/<audio_id>/file

Streams the body into `<file>.part`, continuing from the part's length when a
`Content-Range: bytes N-M/total` header says so; a start other than the part's
length is 409. At the start of a file the receiver refuses (507) a file that
would leave less than 64 MB free (`transfer::FREE_SPACE_MARGIN`). When all bytes
are there, the part is verified against `X-File-SHA256` and renamed (200 `OK`);
a part of the file that is not the end answers 202 `PART` (FILE-13). The receiver
states that it holds the file and that the sender holds it (FILE-22).

#### POST /sync/audio/<audio_id>/keep

The caller is removing its own copy of a recording (FILE-26) and asks this device
to promise to keep its copy meanwhile. No body.

**Response** (`KeepResponse`), 200 whether or not the device promises; 400 when
`audio_id` is not a UUID:
```json
{
    "holds": true,
    "until_ms": 1705320600000,
    "reason": ""
}
```

`holds` is `true` when this device holds the whole file and promises to keep it
until `until_ms` (milliseconds since the epoch, `HOLD_MS`, ten minutes, from
now; kept in `file_holds`); it also states that it holds the file (FILE-22).
`holds` is `false`, with `reason` saying why, when no audio folder is set on this
device, when the recording or its file is not here, when the copy here is not whole, or when this device is removing its
own copy ("this device is removing its own copy"). While a promise lasts, this
device refuses to remove that copy itself (`begin_removal`), so two devices that
count on each other never both remove the file.

#### POST /pair/claim and POST /pair/grant

Bodies are `PairClaimRequest` / `PairClaimResponse` and `PairGrantRequest` /
`PairGrantResponse` in `sync_protocol.rs` (see "Pairing" above). A wrong token is
403 `TOKEN_INVALID` and counts against the address like any other refusal; a
claim for an account the server does not hold is 404 `ACCOUNT_UNKNOWN`; a
single-account listener refuses a grant with 409.

### Entity Types

The feed carries these entity types (`sync_apply::ALL_SYNC_ENTITY_TYPES`,
PROTO-1); the server test `test_get_changes_after_seq_returns_all_entity_types` fails
if the feed omits any of them:

| Entity Type | Description | Apply order |
|-------------|-------------|-------------|
| `field_version` | One immutable version of a versioned field (of any entity, including `device` and `setting`) | 0 |
| `note` | Note row | 1 |
| `tag` | Tag row | 1 |
| `audio_file` | Recording metadata (not the file) | 1 |
| `file_storage_config` | Single-row bucket configuration, entity_id `default` | 1 |
| `note_tag` | Note-to-tag association, entity_id `<note_id>:<tag_id>` | 2 |
| `note_attachment` | Note-to-recording association | 2 |
| `transcription` | Transcription of a recording | 2 |
| `file_location` | Whether a place holds a recording's file, entity_id `<audio_id>:<place>` | 2 |
| `purge` | An entity removed for good (PURGE-4) | 3 |

Device cards (`device`) and synced settings (`setting`) have no row entity type:
they travel only as `field_version` changes, and each receiver rewrites its
`devices` and `synced_settings` tables from the heads.

**Apply order** (`sync_apply::apply_changes`, APPLY-1): queued failures are
retried first; then the batch sorted by the order above, then by timestamp; then
one head recompute per touched field. Before applying any change, the receiver
drops it when it is about a purged entity (PURGE-5).

### Change Format

```json
{
    "entity_type": "note",
    "entity_id": "0199dddddddd70008000000000000003",
    "operation": "update",
    "data": { /* entity-specific data, see below */ },
    "timestamp": 1705314900,
    "device_id": "",
    "device_name": null
}
```

**Fields** (`models::SyncChange`):
- `entity_type`: one of the entity types listed above
- `entity_id`: the entity's id as 32 hex characters, or the composite ids shown above
- `operation`: `create` when `modified_at` is NULL, `update` when set, `delete` when `deleted_at` is set (PROTO-4); a `field_version` is always `create`, a `file_storage_config` and a `file_location` always `update`, a `purge` always `delete`
- `data`: entity-specific data (see below)
- `timestamp`: `modified_at`, else `created_at` (`imported_at` for audio files; `deleted_at` before `created_at` for links and attachments); `created_at` of a version; `purged_at` of a purge; `changed_at / 1000` of a file location
- `device_id`, `device_name`: the server's feed sends them empty (`""`, `null`)

Every timestamp in an entity payload is followed by `<stamp>_offset` and
`<stamp>_zone` (PROTO-3b); they are left out of the examples below except for
the first.

### Entity Data Formats

#### Note

```json
{
    "id": "0199...",
    "created_at": 1705314600,
    "created_at_offset": 7200,
    "created_at_zone": "Asia/Jerusalem",
    "content": "Note text content",
    "modified_at": 1705314900,
    "deleted_at": null,
    "primary_attachment_id": null
}
```

#### Tag

```json
{
    "id": "0199...",
    "name": "Work",
    "parent_id": null,
    "created_at": 1705314600,
    "modified_at": null,
    "deleted_at": null
}
```

#### Note Tag (Association)

```json
{
    "note_id": "0199...",
    "tag_id": "0199...",
    "created_at": 1705314600,
    "modified_at": null,
    "deleted_at": null
}
```

#### Audio File

```json
{
    "id": "0199...",
    "imported_at": 1705314600,
    "filename": "2024_01_15_10_30_00-0000abcd.m4a",
    "file_created_at": 1705314000,
    "summary": null,
    "modified_at": 1705314700,
    "deleted_at": null,
    "storage_provider": "s3",
    "storage_key": "3f5a...e1.m4a",
    "storage_uploaded_at": 1705314700,
    "content_sha256": "3f5a...e1",
    "storage_encrypted": false,
    "disk_name": "2024_01_15_10_30_00-0000abcd.m4a",
    "waveform_levels": "AAECAwQF...",
    "size_bytes": 482133,
    "primary_transcription_id": null,
    "origin_device_id": "0199aaaaaaaa70008000000000000001",
    "origin_kind": "recorded"
}
```

Timezone columns follow `imported_at`, `file_created_at`, `modified_at` and
`deleted_at`.

Only metadata travels through sync. `storage_provider`, `storage_key` and
`storage_uploaded_at` are set by the device that uploaded the file to the bucket
and are `null` until then (FILE-3). The receiver applies every incoming row
through one upsert that merges per column: the newer row wins a metadata column,
an older row only writes a column that is NULL, the bucket location is never
erased by a row without one, and the versioned columns (summary, deletion,
primary transcription) are never written from a row (FILE-9, DM-4).
`content_sha256` is never erased by a row without one (FILE-18); `size_bytes` is
never changed once known (FILE-23); `waveform_levels` is replaced only by a
newer row that has levels (FILE-20). `origin_device_id` (the installation that
made the row) and `origin_kind` (`"recorded"` by this application's recorder or
`"imported"` from a file that already existed) are written by the installation
that makes the row, set once on a receiver that has none, and never changed by a
later row; a row that names a malformed device or another kind changes nothing
(FILE-25).

The local file is named by the row's `disk_name` (FILE-15): a recording made by
Voice is `YYYY_MM_DD_HH_MM_SS-<last eight of the id>.<ext>`, an imported file
keeps its own name, and a collision adds `-<last eight of the id>`. Find a file
with `audio_local_path()` in `models.rs`, never from the id. The bucket object
is `<content hash>.<ext>` (FILE-18), with `.enc` added when encrypted (ENC-3);
a row with no hash has no key and is not uploaded (`storage_key_for` in
`file_storage.rs`); `ext` is from `audio_file_extension()` in `models.rs`
(lowercase, last dot wins, `bin` when absent).

#### Note Attachment

```json
{
    "id": "0199...",
    "note_id": "0199...",
    "attachment_id": "0199...",
    "attachment_type": "audio_file",
    "created_at": 1705314600,
    "modified_at": null,
    "deleted_at": null
}
```

#### Transcription

```json
{
    "id": "0199...",
    "audio_file_id": "0199...",
    "content": "Transcribed text content...",
    "content_segments": "[{\"start\": 0.0, \"end\": 2.5, \"text\": \"שלום\"}]",
    "service": "whisper",
    "service_arguments": "{\"language\": \"he\", \"model\": \"small\"}",
    "service_response": null,
    "state": "original",
    "device_id": "0199...",
    "created_at": 1705314600,
    "modified_at": null,
    "deleted_at": null
}
```

`state` is a space-separated list of the flags `original`, `verified`,
`verbatim`, `cleaned` and `polished` (`TECHNICAL-DECISIONS.md` 1.4, 4.3).

#### File Storage Config

```json
{
    "id": "default",
    "provider": "s3",
    "config": { "bucket": "voice-...", "region": "eu-central-1", "max_upload_mb": 100, "encrypt": false },
    "modified_at": 1705314600,
    "device_id": "0199..."
}
```

`config` is the JSON object stored in the row, or `null`: the provider's
settings and credentials, the account's upload limit `max_upload_mb` (FILE-23)
and the encryption switch `encrypt` (ENC-3). The row is applied by newest
`modified_at` (DM-5). `device_id` is present when the row has one.

#### Purge

```json
{
    "entity_type": "note",
    "entity_id": "0199...",
    "purged_at": 1705320000,
    "purged_at_offset": 7200,
    "purged_at_zone": "Asia/Jerusalem"
}
```

#### File Location

```json
{
    "audio_file_id": "0199...",
    "place": "cloud",
    "present": true,
    "changed_at": 1705314700123,
    "changed_by": "0199..."
}
```

`place` is a device id or `cloud`; `changed_at` is in milliseconds. The newest
statement about a place wins, then the larger device id, then presence (FILE-22).

#### Field Version

```json
{
    "id": "<version id, hex>",
    "entity_type": "note",
    "entity_id": "<note id>",
    "field": "content",
    "parent_id": "<hex or null>",
    "merge_parent_id": "<hex or null>",
    "content": "...",
    "context": null,
    "conflict_kind": null,
    "device_id": "<hex or null>",
    "device_name": "Phone",
    "created_at": 1735689600,
    "created_at_offset": 7200,
    "created_at_zone": "Asia/Jerusalem",
    "published": false
}
```

### Recording Files

A sync moves no file. Each of these is an action the user starts
(`TECHNICAL-DECISIONS.md` 4.5, FLOW-2):

- **Upload** (`file_storage::upload_pending_audio_files`): copies to the bucket
  every row with no `storage_key`, not deleted, whose file is on this device and
  within the account's upload limit (FILE-2, FILE-23). Rows whose file is not
  here are skipped: another device holds them. A file larger than one part (8 MiB)
  goes up in parts, journalled in `upload_parts` (FILE-19). A pending row whose
  object is already in the bucket gets its `storage_key` from one request
  (BUCKET-5); an object that is there but carries the purge tag, or whose tag
  cannot be read, is uploaded again. After the first remote failure in a batch
  the batch stops.
- **Download** (`download_audio_file`, `download_audio_files_for_note`,
  `download_missing_audio_files`): copies a file from the bucket to
  `<file>.part`, verifies it by size and, when the row has one, content hash,
  then renames it (FILE-7, FILE-18). A download, single or in a batch, that
  finds no object, or an object whose hash is not the recording's, states that
  the bucket does not hold the recording (FILE-22). `download_missing_audio_files` copies every
  missing file. The core stores the local setting `sync.mirror_audio_files`
  (`Config::mirror_audio_files`, never synced) for the application that runs this
  after a sync (FILE-5); `sync_with_device` itself never calls it.
- **Send** and **fetch** (`SyncClient::send_to_device`, `fetch_from_device`,
  `transfer.rs`, the file routes above): move a file between two instances of the
  account, streamed and never held in memory, resumable, verified by SHA-256
  (FILE-12, FILE-13). A sender first posts `/sync/audio/missing` with the ids it
  holds, so a thousand recordings cost one round trip.
- **Deliver** (`SyncClient::deliver`) is sync then send; **exchange**
  (`SyncClient::exchange`) is sync then send and fetch.

Where each copy is travels in `file_locations` (FILE-22). Hashing a file
(`Database::store_content_hash(audio_id, audio_dir, here)`, right after an import
or a recording copies it into the folder) states that this device, `here`, holds
it. Every recording names the installation that made it and how
(`origin_device_id`, `origin_kind`, FILE-25); `made_here_but_missing(audio_id,
audio_dir, here)` returns that kind when this device made the recording, no
place is known to hold it, and its file is not in the audio folder.

**Removing this device's copy** (`SyncClient::remove_local_copy`, FILE-26) deletes
the file only when another place confirms at that moment that it holds it:

1. `begin_removal` marks the removal in `file_removals`; it is refused while this
   device has promised a device to keep the copy ("this device promised <device>
   to keep its copy until <time>, while <device> removes its own").
2. The bucket is asked directly, when the row has a storage key: the object must
   exist and not carry the `voice-purged` tag. A bucket that does not hold it is
   stated so.
3. Otherwise each device stated to hold the file is asked with
   `POST /sync/audio/<audio_id>/keep`; the first that answers `holds: true`
   confirms.
4. On a confirmation `finish_removal` deletes the file, states that this device no
   longer holds it and clears the mark; the sentence is "Removed <disk name> from
   this device; <place> holds it". Without one, `abandon_removal` clears the mark
   and the refusal names what each place answered, for example "no other place is
   known to hold it", "the bucket does not hold it", "<device> could not be
   reached: …" or "no bucket is set up on this device".

When the bucket does not hold a file, removing a copy therefore needs a device
that holds it to be reachable at that moment.

"Cloud storage not configured" is a silent no-op for automatic paths and a clear
error for the ones the user starts (FILE-10). Recordings in the bucket can be
encrypted (`crypto.rs`): one recording key per account, chunks of one MiB under
AES-256-GCM, objects with the suffix `.enc`; a device without the key refuses to
upload while encryption is on (ENC-1..ENC-4). The bucket key may delete an object, and
nothing assumes it: a purged recording's object is tagged `voice-purged=1` and
the bucket's lifecycle rule deletes it a day later (BUCKET-2, BUCKET-4). An
object that another recording which stays also uses is not tagged.

### Sync Flow

`SyncClient::sync_with_device` (FLOW-1):

1. State this device's own copies of recordings: compare the audio folder with what this device has stated (FILE-22)
2. `POST /sync/handshake` at the remembered address, and when it does not answer, at each address on the device's card (LISTEN-4): refuse a responder of another account or of protocol 1.x; compare `database_id` with the stored one and restart both cursors from zero when it changed (PROTO-9); report clock skew (DIAG-3)
3. Note `local_end = current_seq()`: only what existed before the pull is pushed (FLOW-6)
4. Take a snapshot (SNAP-3)
5. Pull: `GET /sync/changes?cursor=<stored>` page by page until `is_complete`, applying each page and saving `next_cursor` after it (DIAG-1)
6. Push: pages of this device's changes with `last_sent_seq < seq <= local_end` to `POST /sync/apply`, saving the high-water mark after each accepted page
7. Push the rows of recordings the pull renamed (FILE-15)
8. Record the device's sync time and the last operation (PROOF-3)
9. Add, re-pin or remove devices from the device cards (CARD-3)

`initial_sync` is the same flow from cursor zero in both directions (FLOW-4).
`pull_from_device` and `push_to_device` run one direction (FLOW-3).

### Conflicts

Entity rows carry the current values for display; the truth is the
`field_version` entries in the same feed. A client applies versions first,
then rows, then links, then recomputes the head of every touched field, which is
where merging and conflict detection happen. A `field_version` change is
create-only and idempotent (`INSERT OR IGNORE`), so replaying a feed is safe
(PROTO-2).

Version ids are deterministic for roots (hash of entity, field and content),
merges (hash of both parents and the merged content), acceptances and
resurrections, so every device converges on the same graph and the same conflict
ids (VER-3, VER-5, VER-11).

Only *authored* versions travel: edits and accepts (they carry a device) and
roots. Merges and resurrections are *derived*: every device recomputes them
from the same authored versions and gets the same ids. A derived version that
an authored version builds on (an edit made on top of a merge) is marked
`published` and then travels too, ahead of its child. Heads are always folded
from the authored leaves, so they do not depend on the order or page size in
which versions arrived (VER-9, VER-10, HEAD-2).

The `apply` endpoint returns the number of conflicts flagged during the batch
(APPLY-5). A change that cannot be applied (a link whose note has not arrived
yet) is queued in `sync_failures` and retried at the start of the next batch
(APPLY-2, APPLY-3). A batch is one `BEGIN IMMEDIATE` transaction in which a
failing statement rolls back only itself (APPLY-9).

### Implementing a New Client

1. **Identity:** a UUID7 `device_id` and a `device_name`; a device key for the account, obtained by pairing (PAIR-3) or made when the device created the account (AUTH-1)
2. **Headers:** send `X-Account-ID`, `X-Device-ID` and `Authorization: Bearer <key>` on every authenticated request, over HTTPS with the device's pinned fingerprint
3. **Handshake:** announce protocol `2.0`, the account, the application and the entity types; refuse a responder of another account or a lower major version
4. **Sync state:** store `database_id`, the received cursor and the sent high-water mark per device; save each after every page
5. **All entity types:** apply every type in `ALL_SYNC_ENTITY_TYPES` in dependency order, versions before rows, and recompute heads afterwards; drop changes about purged entities
6. **Recording files:** move them only by upload and download (bucket) or send and fetch (device), as the user asks
7. **Conflicts:** never pick a winner; merge the version graph and record conflicts for the user to resolve
8. **Timestamps:** Unix seconds as integers, each user-visible one with its offset and zone

## Snapshots and Backups

- **Snapshots** (SNAP-1..SNAP-4): `Database::snapshot` copies the whole database
  with SQLite's backup API into `snapshots/` beside the file, named
  `notes-<UTC time>.db`; the newest five are kept (`SNAPSHOTS_KEPT`). A snapshot
  is taken before this device applies anything from a device (on the caller's side
  before the first pull, on the responder's side at the handshake), before
  `move_to_account` and before a restore. `restore_snapshot(name)` snapshots the
  current state first, so a restore can itself be undone. An in-memory database
  has no snapshots and skips them silently. The phone's restore
  (`VoiceClient::restore_snapshot`) compares the audio folder with the restored
  rows afterwards (FILE-22).
- **Periodic backup** (SNAP-5): `Database::backup_to(dir, keep)` holds the write
  lock, checkpoints and truncates the write-ahead log, copies the database with
  the backup API to `dir/notes-<time>.db`, and keeps the newest `keep`. The
  listener runs it every `backup.interval_hours` for every open account
  (`sync_server::spawn_periodic_backup`, `backup_open_accounts`); the default
  directory is `<root>/backups/<account id>/`. `backup_due` says whether an
  account's backup is due.

## Issues

`issues::issues(db, audio_dir, here)` computes, at every call and without
storing anything (ISSUE-1): recordings not in the bucket, each with its reason
(`no_bucket`, `too_large`, `waiting_for_upload` with the devices that hold it,
`no_copy_known`, and `imported_here_file_missing` or `recorded_here_file_missing`
when no place is known to hold it, this device made it, and its file is not in
the audio folder, FILE-25); transcriptions whose recording row is not there; attachments
whose note or recording row is not there; recordings no note holds (a note in the
trash still holds its recordings); tags whose names contain whitespace.

## Validation Rules

| Field | Rule |
|-------|------|
| UUID | Parsed by `uuid`; hyphens are removed first, so 32 hex characters or the hyphenated form |
| Tag name | Not empty after trimming, at most 100 bytes, no `/` |
| Tag path | Not empty, at most 500 bytes, at most 50 levels, each name at most 100 bytes |
| Note content | Not empty after trimming, at most 100,000 bytes |
| Search query | At most 500 bytes |

## Dependencies

### Production

| Category | Crates | Purpose |
|----------|--------|---------|
| Async | `tokio`, `futures-util`, `tokio-util` | Async runtime, streams, streamed file bodies |
| Database | `rusqlite` (bundled, backup) | SQLite driver and backup API |
| Serialization | `serde`, `serde_json` | JSON encoding |
| IDs | `uuid` (v7, v4) | UUID7 identifiers, random bytes |
| HTTP | `reqwest`, `axum`, `axum-server`, `tower`, `tower-http` | Client, server, HTTPS listener, gzip |
| TLS | `rustls`, `rustls-pemfile`, `rcgen` | Pure-Rust TLS (aws-lc on desktop, ring on Android), certificates |
| Bucket | `rust-s3`, `url` | S3 and S3-compatible storage, signed requests |
| Crypto | `sha2`, `ring`, `base64` | Hashes, AES-256-GCM, encoding |
| System | `fs4`, `if-addrs`, `hostname`, `dirs` | Free disk space, listening addresses, host name, configuration directory |
| Errors | `thiserror`, `anyhow` | Error types |
| Dates | `chrono` | Date and time |
| Merging | `diffy`, `similar` | Diff algorithms |
| Logging | `tracing`, `tracing-subscriber` | Logging |
| Encoding | `urlencoding` | Setup texts |
| Bindings | `uniffi` (feature `uniffi`) | Kotlin bindings |

### Development

| Crate | Purpose |
|-------|---------|
| `tempfile` | Temporary files for testing |

## Integration

### Python Bindings

The desktop and server use VoiceCore through PyO3, in the `voice-python` crate
of the `Voice` repository, which depends on the core checked out at
`Voice/submodules/voicecore`:

```
Voice/rust/voice-python/
├── Cargo.toml
└── src/
    └── lib.rs  # PyO3 bindings
```

Build with maturin:

```bash
cd Voice/rust/voice-python
maturin develop --release
```

### Android

The Android application uses VoiceCore through UniFFI: `src/android.rs` exposes
`VoiceClient` (constructed over a data directory with an optional
`KeystoreWrapper`), records such as `NoteData` and `SyncResultData`, the error
`VoiceCoreError`, and the callback interfaces `OperationProgress` and
`KeystoreWrapper`. The scaffolding is generated from the proc-macros
(`uniffi::setup_scaffolding!()`); `build.rs` generates nothing. Build with the
`uniffi` feature, generate the Kotlin bindings from the library with the
`uniffi-bindgen` binary, and build the Android library with cargo-ndk (see
`VoiceAndroid/CLAUDE.md` for the exact commands):

```bash
cargo ndk -t arm64-v8a -o <VoiceAndroid>/app/src/main/jniLibs build --release --features uniffi
```

The Kotlin bindings and the native library must be rebuilt together; bindings
without a matching library crash the phone with `UnsatisfiedLinkError`.

A change to the core is made and committed here, and the checkouts
`Voice/submodules/voicecore` and `VoiceAndroid/submodules/voicecore` are then
moved to that commit (`../CLAUDE.md`, "One core, three binding layers").

## Testing

```bash
# Run all tests
cargo test

# Run with output
cargo test -- --nocapture

# Run the tests whose names contain a text
cargo test purge

# Run the tests of one module
cargo test database::tests
```

Tests use `:memory:` databases and temporary directories, never
`~/.config/voice` (`TECHNICAL-DECISIONS.md` 7.6).

Some tests by the rule they cover:

| Rule | Tests |
|------|-------|
| The schema | `database.rs::tests::schema`: `a_new_database_has_this_build_s_schema_and_its_system_tags_are_in_the_feed`, `a_database_of_another_schema_is_refused_in_words` |
| FILE-25 | `database.rs::tests::origin`: `a_recording_names_the_installation_that_made_it_and_how`, `a_recording_made_here_whose_file_is_gone_is_named_with_how_it_was_made` |
| FILE-26 | `database.rs::tests::file_locations::two_devices_that_count_on_each_other_never_both_remove`, `sync_server.rs::tests::files_between_instances::a_copy_goes_only_when_a_device_promises_to_keep_its_own` |
| LISTEN-4 | `sync_server.rs::tests::listener::the_address_on_the_route_is_shown_alone_and_virtual_interfaces_are_left_out`, `sync_server.rs::tests::devices_from_cards::a_device_that_moved_is_reached_at_an_address_its_card_names`, `sync_server.rs::tests::pairing::a_code_whose_first_address_does_not_answer_is_claimed_at_the_next` |
| PROTO-1 | `sync_server.rs`: `test_get_changes_after_seq_returns_all_entity_types` |

### Convergence tests

```bash
cargo test convergence
```

`src/convergence_tests.rs` runs random multi-device fleets: every entity type,
pages down to 1, duplicate deliveries, a hub topology, a replaced device, then
checks that the fleet converges and quiesces (INV-1..INV-5, INV-10). Run them
after any change to `versions.rs`, `sync_apply.rs`, the row-apply upserts or the
feed. The same file holds `cursor_feed_is_exact_and_resumable`,
`cache_rebuilds_and_echoes_do_not_republish`,
`concurrent_tag_moves_never_form_a_cycle`,
`feed_pages_are_bounded_in_bytes_and_lose_nothing` and
`a_batch_is_one_transaction_and_a_bad_change_does_not_abort_it`.

To trace one seed:

```bash
FLEET_DEBUG=1 FLEET_SEED=<n> cargo test debug_single_seed -- --nocapture
```

The variables `FLEET_TOPOLOGY` (`hub`), `FLEET_DEVICES`, `FLEET_PAGE`,
`FLEET_DUP` and `FLEET_STEPS` set the fleet for that run.

## License

GPL version 3.0 or above

## Authorship

- Written by [Dotan Cohen](https://dotancohen.com).
- Extensive assistance from Anthropic Claude via Claude Code.

## Related Projects

- [Voice Desktop](https://github.com/dotancohen/voice) - Python desktop application using VoiceCore
- VoiceAndroid - Android application using VoiceCore
