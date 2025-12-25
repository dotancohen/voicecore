//! VoiceCore - Rust implementation of the Voice note-taking application core.
//!
//! This library provides the core functionality for Voice:
//! - Data models (Note, Tag, NoteTag)
//! - Database operations (SQLite)
//! - Sync protocol (client and server)
//! - Conflict resolution
//! - Configuration management
//!
//! This is a pure Rust library designed to be used by both Python (via PyO3
//! bindings in a separate crate) and native platforms (Android, iOS).
//!
//! # Feature Flags
//!
//! - `server`: Include HTTP server components (axum, tower). Not needed for mobile clients.
//! - `desktop`: Include desktop-specific features (hostname detection, config dir detection).
//! - `uniffi`: Generate UniFFI bindings for mobile platforms (Android, iOS).

pub mod config;
pub mod conflicts;
pub mod database;
pub mod error;
pub mod merge;
pub mod models;
pub mod search;
pub mod sync_client;
#[cfg(feature = "server")]
pub mod sync_server;
pub mod tls;
pub mod validation;

// Android-specific bindings module
#[cfg(feature = "uniffi")]
pub mod android;

// UniFFI scaffolding (must be at crate root)
#[cfg(feature = "uniffi")]
uniffi::setup_scaffolding!();

// Re-export commonly used types
pub use config::Config;
pub use database::Database;
pub use error::{ValidationError, VoiceError, VoiceResult};
pub use models::{Note, NoteTag, Tag};

// Re-export Android types when uniffi feature is enabled
#[cfg(feature = "uniffi")]
pub use android::{
    generate_device_id, NoteData, SyncResultData, SyncServerConfig, VoiceClient, VoiceCoreError,
};
