# VoiceCore: The Rust Core Library for Voice

- **Purpose**: VoiceCore provides the foundational functionality for Voice, a note-taking application with hierarchical tags and peer-to-peer synchronization.
- **Architecture**: Pure Rust library designed for integration with Python bindings (desktop/server) and native Android applications.
- **No Dependencies on System Libraries**: Uses bundled SQLite and pure-Rust TLS, eliminating the need for OpenSSL or other native dependencies.

## Features

- **Note Management**: Full CRUD operations with soft-delete semantics for sync compatibility.
- **Trash bin**: a deleted note keeps its history and its recordings and can be recovered (`get_deleted_notes`, `undelete_note`), or removed for good (`purge_note`), which travels to every device and cannot be undone.
- **Hierarchical Tags**: Tree-structured tag system with parent-child relationships.
- **Full-Text Search**: Search notes by content and/or tag filters, with support for hierarchical tag paths.
- **Peer-to-Peer Sync**: Bidirectional synchronization protocol with multiple devices.
- **Versioned fields**: every editable value has a Git-like history; concurrent edits are merged three-way and flagged, never overwritten.
- **Configuration Management**: Device identity, peer management, and theme settings.
- **TLS/TOFU Security**: Self-signed certificate generation with Trust-On-First-Use verification.

## Architecture

```
src/
├── lib.rs              # Library entry point and public API re-exports
├── models.rs           # Core data structures (Note, Tag, NoteTag)
├── database.rs         # SQLite data access layer
├── error.rs            # Error types and handling
├── validation.rs       # Input validation utilities
├── config.rs           # Configuration management
├── sync_client.rs      # Peer-to-peer sync client
├── sync_server.rs      # Sync server (Axum-based)
├── sync_protocol.rs    # The protocol's messages, shared by client and server
├── versions.rs         # Versioned fields: history, three-way merge, conflicts
├── sync_apply.rs       # Applying a batch of sync changes (shared by server and client)
├── conflicts.rs        # Thin conflict manager over versions.rs
├── merge.rs            # Text merging algorithms
├── search.rs           # Search and filtering
├── timezone.rs         # The timezone an action happened in
└── tls.rs              # TLS/certificate management
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
`file_created_at` on audio files, and for `created_at` on every field version.
Sync bookkeeping (`sync_received_at`, `last_sync_at`, `seq`) and the cloud
upload time have none: no screen shows them.

This is what lets a note recorded at 15:20 in Jerusalem still read 15:20 after
its author flies to New York. A reader renders the instant at the recorded
offset; only a row with no offset, written before these columns existed or by
a device that never reported one, falls back to the reader's own timezone.

The offset cannot be recovered from the instant afterwards, so the platform
tells the core its timezone with `timezone::set_local_timezone(offset, name)`
at start and whenever it changes. Android in particular keeps the zone in its
framework, where a native library cannot see it. Locale and the 12 or 24-hour
preference are deliberately **not** stored: they belong to whoever is reading,
not to the event, and each interface applies its own.

### Module Overview

| Module | Purpose |
|--------|---------|
| `models` | Core data structures: `Note`, `Tag`, `NoteTag` with UUID7 identifiers |
| `database` | SQLite persistence with comprehensive CRUD and query operations |
| `error` | `VoiceError` enum and `ValidationError` for detailed error handling |
| `validation` | UUID, datetime, tag path, and content validation utilities |
| `config` | JSON-based configuration with device identity and peer management |
| `sync_client` | Async HTTP client for pulling/pushing changes to peers |
| `sync_server` | Axum-based REST server for receiving sync requests |
| `sync_protocol` | The request and response types of the protocol, one definition for both sides |
| `versions` | Field version graph, three-way merge, conflict records, synced settings |
| `sync_apply` | Applying incoming changes in dependency order with retry of failures |
| `conflicts` | Thin conflict manager over `versions` |
| `merge` | Line-by-line diff and 3-way merge algorithms |
| `search` | Parser for combined tag and text search queries |
| `tls` | Self-signed certificate generation and TOFU verification |

## Requirements

- Rust 1.70 or higher (2021 edition)
- No external system dependencies (SQLite is bundled)

## Building

### As a Standalone Library

```bash
cargo build --release
```

### Running Tests

```bash
cargo test
```

### Building Documentation

```bash
cargo doc --open
```

## Usage

### Basic Note Operations

```rust
use voicecore::{Database, Config};

// Initialize with default config directory (~/.config/voice)
let config = Config::new(None)?;
let mut db = Database::new(config.database_file())?;

// Create a note
let note_id = db.create_note("My first note")?;

// Update note content
db.update_note(&note_id, "Updated content")?;

// Get a note
let note = db.get_note(&note_id)?;
println!("Note content: {}", note.content);

// List all notes
let notes = db.get_notes()?;

// Delete a note (soft-delete for sync)
db.delete_note(&note_id)?;
```

### Tag Operations

```rust
// Create a root tag
let work_id = db.create_tag("Work", None)?;

// Create a child tag
let projects_id = db.create_tag("Projects", Some(&work_id))?;

// Associate a tag with a note
db.add_note_tag(&note_id, &work_id)?;

// Get all tags
let tags = db.get_tags()?;

// Get tags in hierarchical order
let tree = db.get_tags_hierarchical()?;
```

### Search

```rust
// Search by text content
let results = db.search_notes(Some("meeting notes"), None)?;

// Search by tag
let results = db.search_notes(None, Some(vec!["Work"]))?;

// Combined search (text AND tag)
let results = db.search_notes(Some("quarterly"), Some(vec!["Work", "Reports"]))?;

// Search with hierarchical tag path
let results = db.search_notes(None, Some(vec!["Europe/France/Paris"]))?;
```

### Synchronization

```rust
use voicecore::{SyncClient, SyncServer};
use std::sync::{Arc, Mutex};

// Initialize sync client
let db = Arc::new(Mutex::new(Database::new(&db_path)?));
let config = Arc::new(Mutex::new(Config::new(None)?));
let sync_client = SyncClient::new(db.clone(), config.clone())?;

// Sync with a peer
let result = sync_client.sync_with_peer("peer_device_id").await?;
println!("Pulled: {}, Pushed: {}, Conflicts: {}",
    result.pulled, result.pushed, result.conflicts);

// Start sync server
let server = SyncServer::new(db, config);
server.start("0.0.0.0", 8384).await?;
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
    println!("{} {} {}: {} vs {}", c.entity_type, c.entity_id, c.field,
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

Merge rules by field kind:

| Kind | Fields | Concurrent change |
|------|--------|-------------------|
| Text | note.content, transcription.content, audio_file.summary | diff3 line merge; overlapping edits kept between `<<<<<<< VERSION A` / `>>>>>>> VERSION B` markers and flagged |
| Scalar | tag.name, tag.parent, setting.value | later version wins, flagged |
| Flags | transcription.state | per-flag union/intersection against the base, flagged only when the same flag was toggled both ways |
| Membership | note_tag.active, note_attachment.active | disagreement keeps the link attached, flagged |
| Deleted | *.deleted | disagreement keeps the entity alive, flagged; a delete that did not see a concurrent edit is overridden by the edit (delete conflict) |

### Configuration

```rust
// Load or create config
let mut config = Config::new(Some("/path/to/config/dir"))?;

// Get device identity
println!("Device ID: {}", config.device_id());
println!("Device Name: {}", config.device_name());

// Add a sync peer
config.add_peer(
    "a1b2c3d4e5f67890",  // peer device ID
    "HomeServer",         // peer name
    "https://sync.example.com"  // peer URL
)?;

// List configured peers
let peers = config.get_peers();

// Remove a peer
config.remove_peer("a1b2c3d4e5f67890")?;
```

## API Reference

### Core Types

#### Note

```rust
pub struct Note {
    pub id: Uuid,                           // UUID7 identifier
    pub created_at: DateTime<Utc>,          // Creation timestamp
    pub content: String,                    // Note content (max 100KB)
    pub device_id: Uuid,                    // Creating device ID
    pub modified_at: Option<DateTime<Utc>>, // Last modification
    pub deleted_at: Option<DateTime<Utc>>,  // Soft-delete timestamp
}
```

#### Tag

```rust
pub struct Tag {
    pub id: Uuid,                           // UUID7 identifier
    pub name: String,                       // Tag name (max 255 chars)
    pub device_id: Uuid,                    // Creating device ID
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
    pub deleted_at: Option<DateTime<Utc>>,  // Soft-delete timestamp
}
```

### Error Types

```rust
pub enum VoiceError {
    Validation(ValidationError),  // Input validation failures
    Database(String),             // SQLite errors
    Sync(String),                 // Synchronization errors
    Network(String),              // HTTP/connection errors
    Tls(String),                  // Certificate errors
    Config(String),               // Configuration errors
    NotFound(String),             // Entity not found
    Conflict(String),             // Sync conflicts
}

pub enum ValidationError {
    InvalidUuid(String),
    InvalidDatetime(String),
    InvalidTagName(String),
    InvalidTagPath(String),
    ContentTooLong(usize),
    EmptyContent,
    // ... additional variants
}
```

### Sync Types

```rust
pub struct SyncResult {
    pub success: bool,
    pub pulled: i64,      // Changes received from peer
    pub pushed: i64,      // Changes sent to peer
    pub conflicts: i64,   // Conflicts detected
    pub errors: Vec<String>,
}

pub enum ResolutionChoice {
    KeepLocal,   // Use local version
    KeepRemote,  // Use remote version
    Merge,       // Manual merge (with conflict markers)
    KeepBoth,    // For delete conflicts: restore deleted note
}
```

## Database Schema

VoiceCore uses SQLite with UUID7 as BLOB primary keys.

### Core Tables

| Table | Purpose |
|-------|---------|
| `notes` | Note content with timestamps and soft-delete |
| `tags` | Hierarchical tag definitions |
| `note_tags` | Many-to-many note-tag associations |

### Sync Infrastructure

| Table | Purpose |
|-------|---------|
| `sync_peers` | Configured peer devices and last sync times |
| `sync_failures` | Failed sync operations for retry |
| `field_versions` | Append-only version graph of every editable field |
| `field_heads` | Current head version per field |
| `field_conflicts` | Merges that need a human, with the two versions and devices |
| `synced_settings` | Settings shared by every device (denormalised heads) |
| `purges` | What was removed for good, so that no peer can bring it back |
| `sync_sequence`, `sync_meta` | Write-order counter and `database_id` for the cursor feed; every syncable table has a `seq` column stamped by triggers |

### Schema Versioning

The database includes a `schema_version` table for migrations. Current schema version: 1.

## Sync Protocol

This section documents the sync protocol for implementing new clients (e.g., mobile apps, web clients).

### Protocol Overview

VoiceCore uses a bidirectional sync protocol where any device can act as both client and server. The protocol supports:
- Incremental sync (only changes since last sync)
- Full sync (complete dataset transfer for initial sync or recovery)
- Audio file metadata (binaries live in cloud storage, see below)
- Conflict detection and resolution

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/sync/handshake` | Device discovery and identity exchange |
| `GET` | `/sync/changes?since=<timestamp>&limit=<n>` | Pull changes since timestamp |
| `POST` | `/sync/apply` | Apply remote changes to local database |
| `GET` | `/sync/full` | Full dataset for initial sync |
| `GET` | `/sync/status` | Health check and server info |
| `GET` | `/sync/audio/<audio_id>/file` | Fetch a recording's file from this peer, resumable with `Range` (FILE-12, FILE-13) |
| `POST` | `/sync/audio/<audio_id>/file` | Send a recording's file to this peer, resumable with `Content-Range` (FILE-12, FILE-13) |

### Endpoint Details

#### POST /sync/handshake

Exchange device identities and determine last sync timestamp.

**Request:**
```json
{
    "device_id": "018d1234abcd5678...",
    "device_name": "My Android Phone",
    "protocol_version": "1.0"
}
```

**Response:**
```json
{
    "device_id": "018d5678efgh9012...",
    "device_name": "Home Server",
    "protocol_version": "1.0",
    "last_sync_timestamp": 1705314600,
    "server_timestamp": 1705320000,
    "supports_audiofiles": true,
    "database_id": "0193...",
    "cursor": 4821
}
```

`database_id` identifies the database; when a peer sees it change it forgets its cursors and exchanges everything again. `cursor` is the end of the feed at handshake time. `protocol_version` is `"1.1"`.

#### GET /sync/changes

Pull changes. Two modes:

**Cursor mode (primary, protocol 1.1):** `?cursor=<n>&limit=<m>` returns every row and authored version whose write-order sequence number is greater than `cursor`, oldest first, at most `limit` (max 10000) changes and about 4 MB of JSON in total, plus `next_cursor` (pass it back to continue) and `is_complete`. Exact, resumable, and independent of clocks. The Rust client only uses this mode and loops until `is_complete`; an initial sync is the same loop from cursor 0.

**Timestamp mode (tools, older clients):** `?since=<unix seconds>&limit=<m>` returns changes with any timestamp `>= since`, `limit` *per entity type*. If omitted, returns all changes.

**Query Parameters:**
- `cursor` (optional): write-order position; takes precedence over `since`.
- `since` (optional): Unix timestamp (seconds).
- `limit` (optional): Default 1000, maximum 10000.

**Response:**
```json
{
    "changes": [ /* array of SyncChange objects */ ],
    "from_timestamp": "2024-01-15 10:30:00",
    "to_timestamp": "2024-01-15 12:00:00",
    "device_id": "018d5678efgh9012...",
    "device_name": "Home Server",
    "is_complete": true
}
```

The response also carries `next_cursor` (cursor mode) and `database_id`. If `is_complete` is `false` in cursor mode, continue from `next_cursor`; in timestamp mode at least one entity type hit the limit and a full re-sync is needed.

#### POST /sync/apply

Apply changes from another device.

**Request:**
```json
{
    "device_id": "018d1234abcd5678...",
    "device_name": "My Android Phone",
    "changes": [ /* array of SyncChange objects */ ]
}
```

**Response:**
```json
{
    "applied": 15,
    "conflicts": 2,
    "errors": ["Error applying note abc123: validation failed"]
}
```

#### GET /sync/full

Get complete dataset for initial sync.

**Response:** Same format as `/sync/changes` but includes all data regardless of timestamps.

#### GET /sync/status

Health check endpoint.

**Response:**
```json
{
    "device_id": "018d5678efgh9012...",
    "device_name": "Home Server",
    "protocol_version": "1.0",
    "status": "ok",
    "supports_audiofiles": true
}
```

### Entity Types

The sync protocol supports these entity types:

| Entity Type | Description | Dependencies |
|-------------|-------------|--------------|
| `note` | Note content and metadata | None |
| `tag` | Tag definitions with hierarchy | None (parent_id is self-referential) |
| `audio_file` | Audio file metadata (not content) | None |
| `note_tag` | Note-to-tag associations | Requires note, tag |
| `note_attachment` | Note-to-attachment associations | Requires note, audio_file |
| `transcription` | Audio transcription text | Requires audio_file |
| `file_storage_config` | Single-row cloud storage configuration (provider + credentials), entity_id `default` | None |

**Dependency Order:** When applying changes, process entities in dependency order:
1. First: `note`, `tag`, `audio_file` (no dependencies)
2. Then: `note_tag`, `note_attachment`, `transcription` (depend on entities from step 1)

### Change Format

```json
{
    "entity_type": "note",
    "entity_id": "018d1234abcd5678901234567890abcd",
    "operation": "create",
    "data": {
        "content": "Meeting notes from today...",
        "created_at": "2024-01-15 10:30:00",
        "modified_at": "2024-01-15 10:30:00",
        "device_id": "018d5678efgh90123456789012345678"
    },
    "timestamp": "2024-01-15 10:30:00",
    "device_id": "018d5678efgh90123456789012345678"
}
```

**Fields:**
- `entity_type`: One of the entity types listed above
- `entity_id`: UUID7 hex string (32 characters, no hyphens)
- `operation`: `create`, `update`, or `delete`
- `data`: Entity-specific data (see below)
- `timestamp`: When the change occurred (server's `modified_at`)
- `device_id`: Device that made the change

### Entity Data Formats

#### Note

```json
{
    "content": "Note text content",
    "created_at": "2024-01-15 10:30:00",
    "modified_at": "2024-01-15 10:35:00",
    "deleted_at": null,
    "device_id": "018d..."
}
```

#### Tag

```json
{
    "name": "Work",
    "parent_id": null,
    "created_at": "2024-01-15 10:30:00",
    "modified_at": "2024-01-15 10:30:00",
    "deleted_at": null,
    "device_id": "018d..."
}
```

#### Note Tag (Association)

```json
{
    "note_id": "018d...",
    "tag_id": "018d...",
    "created_at": "2024-01-15 10:30:00",
    "modified_at": "2024-01-15 10:30:00",
    "deleted_at": null,
    "device_id": "018d..."
}
```

#### Audio File

```json
{
    "id": "018d...",
    "filename": "recording_2024-01-15.m4a",
    "imported_at": 1705314600,
    "file_created_at": 1705314000,
    "duration_seconds": 60,
    "summary": null,
    "modified_at": 1705314600,
    "deleted_at": null,
    "storage_provider": "s3",
    "storage_key": "audio/018d....m4a",
    "storage_uploaded_at": 1705314700
}
```

**Note:** Only metadata travels through the sync server. `storage_provider`/`storage_key` are set by the device that uploaded the binary to cloud storage; they are `null` until then. A device that receives a record with a `storage_key` can fetch the binary on demand with the cloud configuration it received through the `file_storage_config` entity. The local file is named by the row's `disk_name` (a recording made by Voice is `YYYY_MM_DD_HH_MM_SS-<last eight of the id>.<ext>`, an imported file keeps its own name), and the bucket object by the file's content hash, `<hash>.<ext>` (`storage_key_for` in `file_storage.rs`); `ext` is from `audio_file_extension()` in `models.rs` (lowercase, `bin` when absent).

#### Note Attachment

```json
{
    "note_id": "018d...",
    "attachment_id": "018d...",
    "attachment_type": "audio_file",
    "created_at": "2024-01-15 10:30:00",
    "modified_at": "2024-01-15 10:30:00",
    "deleted_at": null,
    "device_id": "018d..."
}
```

#### Transcription

```json
{
    "audio_file_id": "018d...",
    "language": "en",
    "text": "Transcribed text content...",
    "provider": "whisper",
    "model": "large-v3",
    "segments": "[{\"start\": 0.0, \"end\": 2.5, \"text\": \"Hello\"}]",
    "state": "original",
    "created_at": "2024-01-15 10:30:00",
    "modified_at": "2024-01-15 10:30:00",
    "device_id": "018d..."
}
```

### Audio File Transfer

Audio binaries do not go through the sync server. `file_storage.rs` uploads them to cloud storage (S3 or S3-compatible, `file_storage_s3.rs`) from the device that holds them, and downloads them on demand elsewhere:

- `upload_pending_audio_files()` runs before every push: records with `storage_provider IS NULL` whose file is on this device are uploaded; the rest are left for their owner. Failures are warnings and retried next sync; after the first remote failure the batch stops.
- `download_audio_file()` / `download_audio_files_for_note()` are the on-demand paths (CLI, TUI, GUI and Android buttons).
- `download_missing_audio_files()` fetches everything; only installations with `sync.mirror_audio_files = true` run it during sync.
- Downloads are streamed to `<file>.part`, size-verified and renamed into place.

The peer-to-peer endpoints `/sync/audio/<id>/file` remain in the server for a future peer-transfer provider but no current client calls them.

### Sync Flow

#### Initial Sync (First Connection)

1. `POST /sync/handshake` - Exchange device identities
2. `GET /sync/full` - Pull complete dataset from peer (includes `file_storage_config`)
3. Apply all changes locally (respecting dependency order)
4. If mirroring is enabled, download missing audio binaries from cloud storage
5. Upload pending local audio binaries to cloud storage
6. `POST /sync/apply` - Push local changes to peer

#### Incremental Sync

1. Upload pending local audio binaries to cloud storage (so the pushed metadata carries `storage_key`)
2. `POST /sync/handshake` - Exchange identities, get `last_sync_timestamp`
3. `GET /sync/changes?since=<last_sync_timestamp>&limit=10000` - Pull changes
4. Apply changes locally
5. If mirroring is enabled, download missing audio binaries from cloud storage
6. `POST /sync/apply` - Push local changes since last sync
7. Store the sync time as `last_sync_timestamp` for next sync

### Conflict Handling

Entity rows carry the current values for display; the truth is the
`field_version` entries in the same feed. A client applies versions first,
then rows, then recomputes the head of every touched field, which is where
merging and conflict detection happen. A `field_version` change is
create-only and idempotent (`INSERT OR IGNORE`), so replaying a feed is safe.

A `field_version` change looks like:

```json
{
  "entity_type": "field_version",
  "entity_id": "<version id, hex>",
  "operation": "create",
  "timestamp": 1735689600,
  "data": {
    "id": "<version id>", "entity_type": "note", "entity_id": "<note id>", "field": "content",
    "parent_id": "<hex or null>", "merge_parent_id": "<hex or null>",
    "content": "...", "context": null, "conflict_kind": null,
    "device_id": "<hex>", "device_name": "Phone", "created_at": 1735689600
  }
}
```

Version ids are deterministic for roots (hash of entity, field and content),
merges (hash of both parents and the merged content) and acceptances, so every
device converges on the same graph and the same conflict ids.

Only *authored* versions travel: edits and accepts (they carry a device) and
roots. Merges and resurrections are *derived*: every device recomputes them
from the same authored versions and gets the same ids. A derived version that
an authored version builds on (an edit made on top of a merge) is marked
`published` and then travels too, ahead of its child. Heads are always folded
from the authored leaves, so they do not depend on the order or page size in
which versions arrived.

The `apply` endpoint returns the number of conflicts flagged while recomputing
heads. A change that cannot be applied (a link whose note has not arrived yet)
is queued in `sync_failures` and retried on the next batch.

### Timestamp Format

All timestamps in the sync protocol are Unix timestamps in seconds (JSON integers). Display formatting (`YYYY-MM-DD HH:MM:SS`) happens in the UI layers only.

### Implementing a New Client

To implement sync in a new client:

1. **Store device identity:** Generate a UUID7 for `device_id`, store with `device_name`

2. **Track sync state:** Store `last_sync_timestamp` per peer

3. **Implement change tracking:** Track local changes since last sync (by `modified_at`)

4. **Handle all entity types:** Implement create/update/delete for all entity types, including `field_version` (apply versions before rows, then recompute heads)

5. **Respect dependency order:** Apply changes in correct order

6. **Handle audio files:** Upload local binaries to cloud storage before pushing; download on demand using the synced `file_storage_config`

7. **Handle conflicts:** Never pick a winner; merge the version graph and record conflicts for user resolution

8. **Validate timestamps:** Ensure all timestamps use correct format

## Validation Rules

| Field | Rule |
|-------|------|
| UUID | 32 hex chars (simple) or 36 chars with hyphens |
| Datetime | `YYYY-MM-DD HH:MM:SS` format, strict |
| Tag name | 1-255 characters, no forward slashes |
| Tag path | Slash-separated tag names |
| Note content | 1 - 102,400 bytes (100KB max) |

## Dependencies

### Production

| Category | Crates | Purpose |
|----------|--------|---------|
| Async | `tokio` | Async runtime |
| Database | `rusqlite` (bundled) | SQLite driver |
| Serialization | `serde`, `serde_json` | JSON encoding |
| IDs | `uuid` (v7) | UUID7 generation |
| HTTP | `reqwest`, `axum` | Client and server |
| TLS | `rustls`, `rcgen` | Pure-Rust TLS |
| Crypto | `sha2`, `base64` | Hashing and encoding |
| Errors | `thiserror`, `anyhow` | Error handling |
| Dates | `chrono` | Datetime operations |
| Merging | `diffy`, `similar` | Diff algorithms |

### Development

| Crate | Purpose |
|-------|---------|
| `tempfile` | Temporary files for testing |

## Integration

### Python Bindings

VoiceCore is designed for PyO3 integration via the `voice-python` crate:

```
rust/voice-python/
├── Cargo.toml
└── src/
    └── lib.rs  # PyO3 bindings
```

Build with maturin:

```bash
cd rust/voice-python
maturin develop --release
```

### Android

VoiceCore can be compiled for Android targets:

```bash
# Add Android targets
rustup target add aarch64-linux-android armv7-linux-androideabi

# Build with cargo-ndk
cargo ndk -t arm64-v8a -t armeabi-v7a build --release
```

## Testing

```bash
# Run all tests
cargo test

# Run with output
cargo test -- --nocapture

# Run specific test
cargo test test_create_note

# Run tests for a specific module
cargo test database::tests
```

## License

GPL version 3.0 or above

## Authorship

- Written by [Dotan Cohen](https://dotancohen.com).
- Extensive assistance from Anthropic Claude via Claude Code.

## Related Projects

- [Voice Desktop](https://github.com/dotancohen/voice) - Python desktop application using VoiceCore
- voice-android (planned) - Android application using VoiceCore
