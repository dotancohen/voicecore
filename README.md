# VoiceCore: The Rust Core Library for Voice

- **Purpose**: VoiceCore provides the foundational functionality for Voice, a note-taking application with hierarchical tags and peer-to-peer synchronization.
- **Architecture**: Pure Rust library designed for integration with Python bindings (desktop/server) and native Android applications.
- **No Dependencies on System Libraries**: Uses bundled SQLite and pure-Rust TLS, eliminating the need for OpenSSL or other native dependencies.

## Features

- **Note Management**: Full CRUD operations with soft-delete semantics for sync compatibility.
- **Hierarchical Tags**: Tree-structured tag system with parent-child relationships.
- **Full-Text Search**: Search notes by content and/or tag filters, with support for hierarchical tag paths.
- **Peer-to-Peer Sync**: Bidirectional synchronization protocol with multiple devices.
- **Conflict Resolution**: Automatic conflict detection with manual resolution strategies.
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
├── conflicts.rs        # Conflict resolution engine
├── merge.rs            # Text merging algorithms
├── search.rs           # Search and filtering
└── tls.rs              # TLS/certificate management
```

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
| `conflicts` | Detection and resolution of content, delete, and rename conflicts |
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
use voice_core::{Database, Config};

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
use voice_core::{SyncClient, SyncServer};
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

```rust
use voice_core::{ResolutionChoice};

// List pending conflicts
let conflicts = db.get_note_content_conflicts()?;

for conflict in conflicts {
    println!("Conflict on note {}: local vs remote", conflict.note_id);
    println!("Local: {}", conflict.local_content);
    println!("Remote: {}", conflict.remote_content);

    // Resolve by keeping local version
    db.resolve_note_content_conflict(&conflict.id, ResolutionChoice::KeepLocal)?;

    // Or keep remote
    db.resolve_note_content_conflict(&conflict.id, ResolutionChoice::KeepRemote)?;

    // Or merge (manual editing required)
    db.resolve_note_content_conflict(&conflict.id, ResolutionChoice::Merge)?;
}
```

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
| `conflicts_note_content` | Content conflicts awaiting resolution |
| `conflicts_note_delete` | Delete vs. edit conflicts |
| `conflicts_tag_rename` | Tag rename conflicts |

### Schema Versioning

The database includes a `schema_version` table for migrations. Current schema version: 1.

## Sync Protocol

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/sync/handshake` | Device discovery and identity exchange |
| `GET` | `/sync/changes?since=<timestamp>&limit=<n>` | Pull changes since timestamp |
| `POST` | `/sync/apply` | Apply remote changes to local database |
| `GET` | `/sync/full` | Full dataset for initial sync |
| `GET` | `/sync/status` | Health check and server info |

### Change Format

```json
{
    "entity_type": "note",
    "entity_id": "018d1234abcd...",
    "operation": "update",
    "data": { "content": "...", "modified_at": "..." },
    "timestamp": "2024-01-15 10:30:00",
    "device_id": "018d5678efgh..."
}
```

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

MIT License - see LICENSE file for details.

## Authorship

- Written by [Dotan Cohen](https://dotancohen.com).
- Extensive assistance from Anthropic Claude via Claude Code.

## Related Projects

- [Voice Desktop](https://github.com/dotancohen/voice) - Python desktop application using VoiceCore
- voice-android (planned) - Android application using VoiceCore
