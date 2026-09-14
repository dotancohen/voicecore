//! Sync client for Voice device-to-device synchronization.
//!
//! This module provides the client side of the sync protocol, allowing
//! this device to:
//! - Connect to device sync servers
//! - Pull changes from devices
//! - Push local changes to devices
//! - Handle TOFU certificate verification

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::config::Config;
use crate::database::Database;
use crate::error::{VoiceError, VoiceResult};
use crate::models::{audio_local_path, SyncChange};
use crate::sync_protocol::{
    codes, ApplyRequest, ApplyResponse, ChangesResponse, ErrorResponse, HandshakeRequest, HandshakeResponse,
    PROTOCOL_VERSION,
};
use crate::UUID_SHORT_LEN;

/// Result of a sync operation
#[derive(Debug, Clone, Default)]
pub struct SyncResult {
    pub success: bool,
    pub pulled: i64,
    pub pushed: i64,
    pub conflicts: i64,
    /// Recordings sent to the device in this operation (deliver, exchange)
    pub sent: i64,
    /// Recordings fetched from the device in this operation (exchange, fetch)
    pub fetched: i64,
    /// Bytes of recordings moved in either direction
    pub bytes_moved: u64,
    /// Problems that made the sync incomplete or wrong (metadata level).
    pub errors: Vec<String>,
    /// Problems that did not affect the metadata sync, e.g. a cloud storage
    /// upload that could not be completed and will be retried next time.
    pub warnings: Vec<String>,
    /// The id of this operation, on every request of it and in every log
    /// line on both sides (Stage 12).
    pub request_id: String,
    /// The device's clock minus this device's, in seconds, when the two
    /// differ by more than a minute; 0 otherwise.
    pub clock_skew_seconds: i64,
}

/// A difference of clocks below this is not reported.
pub const CLOCK_SKEW_REPORTED_SECONDS: i64 = 60;

/// The sentence for a clock difference, or None below the threshold.
pub fn clock_skew_sentence(skew_seconds: i64, device_name: &str) -> Option<String> {
    if skew_seconds.abs() <= CLOCK_SKEW_REPORTED_SECONDS {
        return None;
    }
    let minutes = (skew_seconds.abs() + 30) / 60;
    let relation = if skew_seconds > 0 { "behind" } else { "ahead of" };
    Some(format!("This device's clock is {} minute{} {} {}'s", minutes, if minutes == 1 { "" } else { "s" }, relation, device_name))
}

/// A fresh operation id: 16 hex characters, unique enough for two logs.
pub fn new_request_id() -> String {
    Uuid::new_v4().simple().to_string()[..16].to_string()
}

impl SyncResult {
    pub fn success() -> Self {
        Self {
            success: true,
            ..Default::default()
        }
    }

    pub fn failure(error: impl Into<String>) -> Self {
        Self {
            success: false,
            errors: vec![error.into()],
            ..Default::default()
        }
    }
}

/// Information about a sync device
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceInfo {
    pub device_id: String,
    pub device_name: String,
    pub device_url: String,
    pub certificate_fingerprint: Option<String>,
    pub last_sync_at: Option<String>,
}


/// Page size for the cursor feed, in both directions. Pages are fetched
/// until the device reports the feed complete, so this only bounds one request.
pub const PULL_LIMIT: i64 = 10000;

/// Safety cap on pages per direction per sync (10000 * 1000 changes).
const MAX_PAGES: usize = 1000;

/// What an incremental pull produced.
struct PullOutcome {
    applied: i64,
    conflicts: i64,
    changes: Vec<SyncChange>,
    errors: Vec<String>,
    warnings: Vec<String>,
}

/// Cursor state for one device, as stored in `sync_devices`.
#[derive(Debug, Clone, Default)]
struct DeviceCursors {
    /// Our position in the device's feed
    received: i64,
    /// Our own `seq` up to which the device has everything
    sent: i64,
}

/// Sync client
pub struct SyncClient {
    db: Arc<Mutex<Database>>,
    config: Arc<Mutex<Config>>,
    /// One verified client per (device URL, pinned fingerprint)
    clients: Mutex<HashMap<String, Client>>,
    this_device_id: String,
    this_device_name: String,
    /// The id of the operation under way (Stage 12)
    request_id: Mutex<String>,
    /// Changes per page of the feed, [`PULL_LIMIT`] unless lowered
    page_size: Mutex<i64>,
    /// Set from another thread to stop the operation under way at the next
    /// page, file or chunk (Stage 4: cancel); cleared when one begins
    cancel: Arc<std::sync::atomic::AtomicBool>,
    /// Where progress is reported (Stage 4: progress), if anywhere
    progress: Mutex<Option<Arc<dyn ProgressSink>>>,
}

/// One step of an operation, as the interfaces show it: the stage ("sync",
/// "send", "fetch"), how many of how many, the bytes moved so far, and a
/// sentence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Progress {
    pub stage: String,
    pub done: i64,
    pub total: i64,
    pub bytes: u64,
    pub sentence: String,
}

/// Where an operation's progress goes: a notification, a label, a log.
pub trait ProgressSink: Send + Sync {
    fn report(&self, progress: Progress);
}

/// The sentence of a cancelled operation; the error carries it.
pub const CANCELLED: &str = "Cancelled";

/// Bytes between two progress reports of one file.
const PROGRESS_EVERY_BYTES: u64 = 1024 * 1024;

impl SyncClient {
    /// Give an empty device that cannot reach this one (a server) this
    /// device's account (PAIR-5): post to its grant text with a key made for
    /// it and this device's card, then add it as a device.
    pub async fn grant_host(&self, setup_text: &str, label: &str) -> VoiceResult<Joined> {
        self.begin_operation();
        let setup = crate::pairing::SetupText::parse(setup_text)?;
        if !setup.grant {
            return Err(VoiceError::validation("setup text", format!("This is a code to join with, not a grant text; use 'account join' ({})", codes::SETUP_TEXT_INVALID)));
        }
        let (account_id, own_card) = {
            let db = self.db.lock().unwrap();
            let mut config = self.config.lock().unwrap();
            let card = crate::auth::ensure_own_device_card(&db, &mut config)?;
            (db.account_id()?, card)
        };
        let server_key = crate::auth::generate_device_key();
        let request = crate::sync_protocol::PairGrantRequest {
            token: setup.token.clone(),
            account_id: account_id.clone(),
            label: label.to_string(),
            device_key: server_key.clone(),
            holder_id: own_card.device_id.clone(),
            holder_name: own_card.name.clone(),
            holder_certificate_fingerprint: own_card.certificate_fingerprint.clone(),
            holder_addresses: own_card.addresses.clone(),
            holder_key_hash: own_card.key_hash.clone(),
            recording_key: self.config.lock().ok().map(|c| c.recording_key_text().to_string()).filter(|k| !k.is_empty()),
        };
        let client = build_client(&setup.certificate_fingerprint, &setup.urls[0])?;
        let mut last_error = String::new();
        let mut reply: Option<(String, crate::sync_protocol::PairGrantResponse)> = None;
        for url in &setup.urls {
            check_scheme(url)?;
            match client.post(format!("{}/pair/grant", url.trim_end_matches('/'))).json(&request).send().await {
                Ok(response) if response.status().is_success() => {
                    let body = response.json().await.map_err(|e| VoiceError::Sync(format!("Could not read the grant reply: {}", e)))?;
                    reply = Some((url.clone(), body));
                    break;
                }
                Ok(response) => {
                    let body = response.text().await.unwrap_or_default();
                    let sentence = serde_json::from_str::<ErrorResponse>(&body).map(|r| r.error).unwrap_or(body);
                    return Err(VoiceError::Sync(if sentence.is_empty() { "The grant was refused".to_string() } else { sentence }));
                }
                Err(e) => last_error = describe(&e),
            }
        }
        let (url, reply) = reply.ok_or_else(|| VoiceError::Network(format!("Could not reach device {}: {}", &setup.device_id[..UUID_SHORT_LEN.min(setup.device_id.len())], last_error)))?;
        {
            let db = self.db.lock().unwrap();
            db.admit_device_card(&crate::versions::DeviceCard {
                device_id: reply.device_id.clone(),
                name: reply.device_name.clone(),
                certificate_fingerprint: reply.certificate_fingerprint.clone(),
                addresses: reply.addresses.clone(),
                listens: "1".to_string(),
                key_hash: crate::auth::key_hash(&server_key),
                revoked: "0".to_string(),
                application: crate::auth::APPLICATION_VOICE.to_string(),
            })?;
            let mut config = self.config.lock().unwrap();
            let pin = if setup.certificate_fingerprint.is_empty() { None } else { Some(setup.certificate_fingerprint.as_str()) };
            config.add_device(&reply.device_id, &reply.device_name, &url, pin, true)?;
            config.set_sync_enabled(true)?;
        }
        self.clients.lock().unwrap().clear();
        Ok(Joined { account_id, device_id: reply.device_id, device_name: reply.device_name, device_url: url })
    }
}

/// The first characters of an id, for a sentence.
fn short_id(id: &str) -> &str {
    &id[..UUID_SHORT_LEN.min(id.len())]
}

/// What a successful join gives back.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Joined {
    pub account_id: String,
    pub device_id: String,
    pub device_name: String,
    pub device_url: String,
}

/// Plain http is accepted only to this machine itself (AUTH-7).
fn check_scheme(device_url: &str) -> VoiceResult<()> {
    // A device paired but never heard listening has no address yet: say so in
    // words, not as the URL parser's "relative URL without a base" (D30)
    if device_url.trim().is_empty() {
        return Err(VoiceError::Network(format!(
            "no address is known for this device yet: it has not listened on this network, or its card has not arrived; sync with a device that knows it, or add its address ({})",
            codes::NO_ADDRESS
        )));
    }
    let url = reqwest::Url::parse(device_url)
        .map_err(|e| VoiceError::Network(format!("{} is not a URL: {}", device_url, e)))?;
    let host = url.host_str().unwrap_or("");
    let loopback = matches!(host, "localhost" | "127.0.0.1" | "[::1]" | "::1") || host.starts_with("127.");
    if url.scheme() == "http" && !loopback {
        return Err(VoiceError::Network(format!(
            "{} is plain http; a device key must not cross a network in clear ({})",
            device_url,
            codes::TLS_REQUIRED
        )));
    }
    Ok(())
}

/// A client verified by `pin` when there is one, by the system roots when
/// there is none. Verification is never off.
/// Three seconds to connect on this machine or the LAN, ten on the internet
/// (FILE-14); a private address is one of RFC 1918's or a loopback.
fn connect_timeout_for(device_url: &str) -> Duration {
    let host = reqwest::Url::parse(device_url).ok().and_then(|u| u.host_str().map(str::to_string)).unwrap_or_default();
    let near = host == "localhost"
        || host.parse::<std::net::IpAddr>().map(|ip| match ip {
            std::net::IpAddr::V4(v4) => v4.is_loopback() || v4.is_private() || v4.is_link_local(),
            std::net::IpAddr::V6(v6) => v6.is_loopback(),
        }).unwrap_or(false);
    Duration::from_secs(if near { 3 } else { 10 })
}

fn build_client(pin: &str, device_url: &str) -> VoiceResult<Client> {
    // A page can be a few megabytes over a slow link; a link that stops
    // moving for thirty seconds is dead, however long the page (FILE-14)
    let builder = Client::builder()
        .connect_timeout(connect_timeout_for(device_url))
        .read_timeout(Duration::from_secs(30))
        .timeout(Duration::from_secs(180));
    let builder = if pin.is_empty() {
        builder
    } else {
        builder.use_preconfigured_tls(crate::tls::pinned_client_config(pin)?)
    };
    builder.build().map_err(|e| VoiceError::Network(e.to_string()))
}

/// A request error with every cause behind it, so that a refused
/// certificate says so instead of "error sending request".
fn describe(error: &reqwest::Error) -> String {
    let mut parts = vec![error.to_string()];
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        let text = cause.to_string();
        if !parts.iter().any(|p| p.contains(&text)) {
            parts.push(text);
        }
        source = cause.source();
    }
    parts.join(": ")
}

impl SyncClient {
    /// Create a new sync client
    pub fn new(db: Arc<Mutex<Database>>, config: Arc<Mutex<Config>>) -> VoiceResult<Self> {
        let (this_device_id, this_device_name) = {
            let cfg = config.lock().unwrap();
            (cfg.this_device_id_hex().to_string(), cfg.this_device_name().to_string())
        };

        Ok(Self {
            db,
            config,
            clients: Mutex::new(HashMap::new()),
            this_device_id,
            this_device_name,
            request_id: Mutex::new(String::new()),
            page_size: Mutex::new(PULL_LIMIT),
            cancel: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            progress: Mutex::new(None),
        })
    }

    /// A client whose cancel flag the interface holds already, so an
    /// operation started later can be stopped from any thread.
    pub fn with_cancel(db: Arc<Mutex<Database>>, config: Arc<Mutex<Config>>, cancel: Arc<std::sync::atomic::AtomicBool>) -> VoiceResult<Self> {
        let mut client = Self::new(db, config)?;
        client.cancel = cancel;
        Ok(client)
    }

    /// The flag that cancels the operation under way when set from
    /// another thread; shared, so the interface keeps a copy.
    pub fn cancel_flag(&self) -> Arc<std::sync::atomic::AtomicBool> {
        self.cancel.clone()
    }

    /// Cancel the operation under way: it stops at the next page, file or
    /// chunk, a transfer under way stays resumable, and the result says so.
    pub fn cancel(&self) {
        self.cancel.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    fn cancelled(&self) -> bool {
        self.cancel.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Where progress is reported from now on.
    pub fn set_progress_sink(&self, sink: Option<Arc<dyn ProgressSink>>) {
        *self.progress.lock().unwrap() = sink;
    }

    fn report(&self, stage: &str, done: i64, total: i64, bytes: u64, sentence: impl Into<String>) {
        if let Some(sink) = self.progress.lock().unwrap().as_ref() {
            sink.report(Progress { stage: stage.to_string(), done, total, bytes, sentence: sentence.into() });
        }
    }

    /// Lower the page size of the feed, for a test of what a cut mid-sync
    /// leaves, or a device with little memory.
    pub fn set_page_size(&self, changes_per_page: i64) {
        *self.page_size.lock().unwrap() = changes_per_page.max(1);
    }

    fn page_size(&self) -> i64 {
        *self.page_size.lock().unwrap()
    }

    /// Start an operation: a new request id for every request of it.
    fn begin_operation(&self) -> String {
        let id = new_request_id();
        *self.request_id.lock().unwrap() = id.clone();
        self.cancel.store(false, std::sync::atomic::Ordering::SeqCst);
        id
    }

    /// The id of the operation under way.
    pub fn request_id(&self) -> String {
        self.request_id.lock().unwrap().clone()
    }

    /// The HTTP client for a device's URL (AUTH-7): plain http only to this
    /// machine itself; https verified against the fingerprint pinned for the
    /// device when there is one, against the system's root certificates when
    /// there is none. Verification is never off. Clients are kept per
    /// (URL, pin) so a changed pin builds a new one.
    fn client_for(&self, device_url: &str) -> VoiceResult<Client> {
        check_scheme(device_url)?;
        let pin = {
            let config = self.config.lock().unwrap();
            config
                .devices()
                .iter()
                .find(|p| p.device_url.trim_end_matches('/') == device_url.trim_end_matches('/'))
                .and_then(|p| p.certificate_fingerprint.clone())
                .unwrap_or_default()
        };
        let cache_key = format!("{}\n{}", device_url, pin);
        if let Some(client) = self.clients.lock().unwrap().get(&cache_key) {
            return Ok(client.clone());
        }
        let client = build_client(&pin, device_url)?;
        self.clients.lock().unwrap().insert(cache_key, client.clone());
        Ok(client)
    }

    /// Join an account from a setup text (PAIR-4): refuse if this device
    /// holds another account's notes, then present the token to the showing
    /// device over TLS pinned to the text's fingerprint, take the account id
    /// and the key it issues, and add the showing device as a device.
    pub async fn join(&self, setup_text: &str) -> VoiceResult<Joined> {
        self.begin_operation();
        let setup = crate::pairing::SetupText::parse(setup_text)?;
        {
            let db = self.db.lock().unwrap();
            crate::pairing::check_can_join(&db, &setup)?;
        }
        self.claim_and_take(&setup).await
    }

    /// **Move this device to another account** by its code (Stage 1): the
    /// deliberate way to merge two accounts, for a device that holds notes.
    /// A snapshot is taken, the code is claimed for a key of the other
    /// account, the account id is rewritten and every device forgotten, the
    /// notes stay (their ids cannot collide), the other account's tags are
    /// pulled and tags with one path become one, and then everything is
    /// exchanged. Returns what was joined and how many tags were merged.
    pub async fn move_to(&self, setup_text: &str) -> VoiceResult<(Joined, usize)> {
        self.begin_operation();
        let setup = crate::pairing::SetupText::parse(setup_text)?;
        if setup.grant {
            return Err(VoiceError::validation("setup text", format!("This is a grant text, not a code ({})", codes::SETUP_TEXT_INVALID)));
        }
        if self.account_id() == setup.account_id {
            return Err(VoiceError::validation("setup text", "That is already this device's account".to_string()));
        }
        let joined = self.claim_and_take(&setup).await?;
        // The other account's tags first, so tags with one path become one
        // before this device's notes travel
        let pulled = self.pull_from_device(&joined.device_id).await;
        if !pulled.success {
            return Err(VoiceError::Sync(format!("Moved, but the other account could not be read: {}", pulled.errors.join("; "))));
        }
        let merged = self.db.lock().unwrap().merge_duplicate_tag_paths()?;
        let exchanged = self.sync_with_device(&joined.device_id).await;
        if !exchanged.success {
            return Err(VoiceError::Sync(format!("Moved, but the first exchange failed: {}", exchanged.errors.join("; "))));
        }
        Ok((joined, merged))
    }

    /// Claim a code and take the account it names (PAIR-3, PAIR-4): the
    /// shared part of joining and moving.
    async fn claim_and_take(&self, setup: &crate::pairing::SetupText) -> VoiceResult<Joined> {
        let own_fingerprint = {
            let config = self.config.lock().unwrap();
            config
                .certs_dir()
                .ok()
                .map(|d| d.join("server.crt"))
                .filter(|p| p.is_file())
                .and_then(|p| crate::tls::compute_fingerprint(&p).ok())
                .unwrap_or_default()
        };
        if setup.grant {
            return Err(VoiceError::validation("setup text", format!("This is a grant text, shown by a device that holds no account; use 'account grant-host' with it ({})", codes::SETUP_TEXT_INVALID)));
        }
        let setup = setup.clone();
        let request = crate::sync_protocol::PairClaimRequest {
            token: setup.token.clone(),
            account_id: setup.account_id.clone(),
            device_id: self.this_device_id.clone(),
            device_name: self.this_device_name.clone(),
            certificate_fingerprint: own_fingerprint,
            addresses: String::new(),
            application: crate::auth::APPLICATION_VOICE.to_string(),
        };
        let mut last_error = String::new();
        let mut reply: Option<(String, crate::sync_protocol::PairClaimResponse)> = None;
        // Every address of the code in turn (LISTEN-4): the showing device may
        // not know which of its addresses this device reaches
        for url in &setup.urls {
            if let Err(e) = check_scheme(url) {
                last_error = format!("{}: {}", url, e);
                continue;
            }
            let client = build_client(&setup.certificate_fingerprint, url)?;
            match client.post(format!("{}/pair/claim", url.trim_end_matches('/'))).json(&request).send().await {
                Ok(response) if response.status().is_success() => {
                    let body: crate::sync_protocol::PairClaimResponse = response
                        .json()
                        .await
                        .map_err(|e| VoiceError::Sync(format!("Could not read the pairing reply: {}", e)))?;
                    reply = Some((url.clone(), body));
                    break;
                }
                Ok(response) => {
                    let body = response.text().await.unwrap_or_default();
                    let sentence = serde_json::from_str::<ErrorResponse>(&body).map(|r| r.error).unwrap_or(body);
                    return Err(VoiceError::Sync(if sentence.is_empty() { "The pairing was refused".to_string() } else { sentence }));
                }
                Err(e) => last_error = describe(&e),
            }
        }
        let (url, reply) = reply.ok_or_else(|| VoiceError::Network(format!("Could not reach device {}: {}", &setup.device_id[..UUID_SHORT_LEN.min(setup.device_id.len())], last_error)))?;
        if reply.account_id != setup.account_id {
            return Err(VoiceError::Sync(format!(
                "The device answered for account {}, but the code was for {} ({})",
                &reply.account_id[..UUID_SHORT_LEN.min(reply.account_id.len())],
                &setup.account_id[..UUID_SHORT_LEN.min(setup.account_id.len())],
                codes::ACCOUNT_MISMATCH
            )));
        }
        {
            let db = self.db.lock().unwrap();
            if db.account_id()? != reply.account_id {
                db.move_to_account(&reply.account_id)?;
            }
            db.admit_device_card(&crate::versions::DeviceCard {
                device_id: reply.device_id.clone(),
                name: reply.device_name.clone(),
                certificate_fingerprint: reply.certificate_fingerprint.clone(),
                addresses: reply.addresses.clone(),
                listens: "1".to_string(),
                key_hash: String::new(),
                revoked: "0".to_string(),
                application: crate::auth::APPLICATION_VOICE.to_string(),
            })?;
            let mut config = self.config.lock().unwrap();
            config.set_device_key(&reply.device_key)?;
            if let Some(key) = reply.recording_key.as_deref().filter(|k| !k.is_empty()) {
                config.set_recording_key(key)?;
            }
            let pin = if setup.certificate_fingerprint.is_empty() { None } else { Some(setup.certificate_fingerprint.as_str()) };
            config.add_device(&reply.device_id, &reply.device_name, &url, pin, true)?;
            config.set_sync_enabled(true)?;
            crate::auth::ensure_own_device_card(&db, &mut config)?;
        }
        self.clients.lock().unwrap().clear();
        Ok(Joined { account_id: reply.account_id, device_id: reply.device_id, device_name: reply.device_name, device_url: url })
    }

    /// Remove this device's copy of a recording to save space (FILE-26). The
    /// copy goes only when another place confirms, now, that it holds the
    /// file: the bucket, asked directly, or a device that holds it and
    /// promises to keep its copy while this one goes. Two devices that remove
    /// at once each refuse the other's request, so neither removes. Returns
    /// the sentence naming the place that confirmed.
    pub async fn remove_local_copy(&self, audio_id: &str) -> VoiceResult<String> {
        self.begin_operation();
        let (dir, here) = {
            let c = self.config.lock().unwrap();
            (c.audiofile_directory().map(std::path::PathBuf::from), c.this_device_id_hex().to_string())
        };
        let dir = dir.ok_or_else(|| VoiceError::validation("audio_dir", "the audio folder is not set"))?;
        let (row, holders) = {
            let db = self.db.lock().unwrap();
            let _ = db.apply_pending_file_renames(&dir);
            let row = db.get_audio_file(audio_id)?.ok_or_else(|| VoiceError::NotFound(audio_id.to_string()))?;
            if !crate::models::audio_local_path(&dir, &row.disk_name).is_file() {
                return Err(VoiceError::validation("audio_id", format!("{} is not on this device", row.disk_name)));
            }
            if let Err(reason) = db.begin_removal(&row.id)? {
                return Err(VoiceError::validation("audio_id", format!("{} was not removed: {}", row.disk_name, reason)));
            }
            let holders: Vec<String> = db
                .places_holding(&row.id)?
                .into_iter()
                .filter(|p| p != &here && p != crate::database::PLACE_CLOUD)
                .collect();
            (row, holders)
        };
        let mut tried: Vec<String> = Vec::new();
        let confirmed = self.confirm_held_elsewhere(&row, &holders, &mut tried).await;
        let db = self.db.lock().unwrap();
        match confirmed {
            Some(place) => {
                db.finish_removal(&row.id, &dir, &here)?;
                tracing::info!("Removed {} from this device; {} confirmed that it holds it", row.disk_name, place);
                Ok(format!("Removed {} from this device; {} holds it", row.disk_name, place))
            }
            None => {
                db.abandon_removal(&row.id)?;
                let why = if tried.is_empty() { "no other place is known to hold it".to_string() } else { tried.join("; ") };
                Err(VoiceError::validation(
                    "audio_id",
                    format!("{} was not removed: no other place confirmed that it holds the file now ({})", row.disk_name, why),
                ))
            }
        }
    }

    /// Ask the bucket, then each device said to hold the file, until one
    /// confirms (FILE-26). What each answered goes into `tried`.
    async fn confirm_held_elsewhere(&self, row: &crate::database::AudioFileRow, holders: &[String], tried: &mut Vec<String>) -> Option<String> {
        #[cfg(feature = "file-storage")]
        if let Some(key) = row.storage_key.clone().filter(|_| row.storage_provider.is_some()) {
            let storage = {
                let db = self.db.lock().unwrap();
                crate::file_storage::create_storage_service(&db)
            };
            match storage {
                Ok(Some(storage)) => match crate::file_storage::bucket_holds(&storage, &key).await {
                    Ok(true) => return Some("the bucket".to_string()),
                    Ok(false) => {
                        tried.push("the bucket does not hold it".to_string());
                        if let Err(e) = self.db.lock().unwrap().set_file_location(&row.id, crate::database::PLACE_CLOUD, false) {
                            tracing::warn!("Could not record that the bucket lacks {}: {}", row.id, e);
                        }
                    }
                    Err(e) => tried.push(format!("the bucket could not be asked: {}", e)),
                },
                Ok(None) => tried.push("no bucket is set up on this device".to_string()),
                Err(e) => tried.push(format!("the bucket could not be asked: {}", e)),
            }
        }
        for device_id in holders {
            let name = self
                .config
                .lock()
                .ok()
                .and_then(|c| c.get_device(device_id).map(|p| p.device_name.clone()))
                .filter(|n| !n.is_empty())
                .unwrap_or_else(|| short_id(device_id).to_string());
            let Some(url) = self.device_url_of(device_id).filter(|u| !u.is_empty()) else {
                tried.push(format!("{} has no address on this device", name));
                continue;
            };
            let client = match self.client_for(&url) {
                Ok(client) => client,
                Err(e) => {
                    tried.push(format!("{}: {}", name, e));
                    continue;
                }
            };
            let request = self
                .authed(client.post(format!("{}/sync/audio/{}/keep", url.trim_end_matches('/'), row.id)))
                .timeout(Duration::from_secs(15));
            match request.send().await {
                Ok(response) if response.status().is_success() => match response.json::<crate::sync_protocol::KeepResponse>().await {
                    Ok(answer) if answer.holds => return Some(name),
                    Ok(answer) => tried.push(format!("{} does not keep it: {}", name, answer.reason)),
                    Err(e) => tried.push(format!("the answer of {} could not be read: {}", name, e)),
                },
                Ok(response) => tried.push(format!("{} answered with status {}", name, response.status())),
                Err(e) => tried.push(format!("{} could not be reached: {}", name, describe(&e))),
            }
        }
        None
    }

    /// The three headers every request carries (AUTH-3): the account, the
    /// device, and the device's key as a bearer token.
    fn authed(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let key = self.config.lock().map(|c| c.device_key().to_string()).unwrap_or_default();
        request
            .header(crate::auth::HEADER_ACCOUNT, self.account_id())
            .header(crate::auth::HEADER_DEVICE, &self.this_device_id)
            .header("X-Device-Name", &self.this_device_name)
            .header(crate::sync_protocol::HEADER_REQUEST_ID, self.request_id())
            .bearer_auth(key)
    }

    /// Sync with a device: exchange database changes both ways. Files never
    /// move here; see `deliver` and `exchange`.
    /// One operation, one request id.
    pub async fn sync_with_device(&self, device_id: &str) -> SyncResult {
        // What this device holds is stated before the push (FILE-22): a file
        // removed from the folder by hand is known to be gone everywhere
        let (audio_dir, here) = match self.config.lock() {
            Ok(c) => (c.audiofile_directory().map(std::path::PathBuf::from), c.this_device_id_hex().to_string()),
            Err(_) => (None, String::new()),
        };
        if let Some(dir) = audio_dir {
            if let Err(e) = self.db.lock().unwrap().check_files_here(&dir, &here) {
                tracing::warn!("The files on this device were not compared with what it has stated: {}", e);
            }
        }
        self.begin_operation();
        self.sync_within_operation(device_id).await
    }

    /// The sync itself, under the request id of the operation under way
    /// (a deliver or an exchange begins one and then moves files under it).
    async fn sync_within_operation(&self, device_id: &str) -> SyncResult {
        let device = {
            let config = self.config.lock().unwrap();
            config.get_device(device_id).cloned()
        };

        let device = match device {
            Some(p) => p,
            None => return self.failed(format!("Unknown device: {}", device_id)),
        };

        let mut result = SyncResult::success();
        result.request_id = self.request_id();

        // Step 1: Handshake, and find where we stand with this device
        let (reached, handshake) = match self.reach(device_id, &device.device_url).await {
            Ok(r) => r,
            Err(e) => return self.failed(format!("Handshake failed: {}", e)),
        };
        let device_url = &reached;
        if let Err(sentence) = self.check_account(device_id, &handshake) {
            return self.failed(sentence);
        }
        if handshake.server_timestamp != 0 {
            let skew = handshake.server_timestamp - Utc::now().timestamp();
            if let Some(sentence) = clock_skew_sentence(skew, &device.device_name) {
                result.clock_skew_seconds = skew;
                result.warnings.push(sentence);
            }
        }
        let cursors = self.device_cursors(device_id, &handshake, &mut result);

        // Everything written locally up to here is what this sync pushes;
        // whatever the pull writes is the device's own data coming back.
        let local_end = self.local_seq();
        // Renames a collision brought by this pull makes here are this device's
        // own changes (FILE-15): forget any recorded before, push these after
        if let Ok(db) = self.db.lock() {
            db.take_renamed_recordings();
        }

        // Step 2: Pull, page by page, saving the cursor after every page
        self.snapshot_before("sync", &mut result);
        let pull = self.pull_all(device_url, device_id, &device.device_name, cursors.received).await;
        result.pulled = pull.applied;
        result.conflicts += pull.conflicts;
        result.errors.extend(pull.errors);
        result.warnings.extend(pull.warnings);

        // Step 3: Push our changes the device has not seen, page by page
        let (pushed, conflicts, errors, warnings) = self.push_all(device_url, device_id, cursors.sent, local_end).await;
        result.pushed = pushed;
        result.conflicts += conflicts;
        result.errors.extend(errors);
        result.warnings.extend(warnings);

        // Recordings the pull renamed: their rows are past the window above,
        // and the device needs them now to take the same names (FILE-15)
        let renamed = self.db.lock().map(|db| db.take_renamed_recordings()).unwrap_or_default();
        if !renamed.is_empty() {
            let changes = {
                let db = self.db.lock().unwrap();
                db.get_changes_for_recordings(&renamed)
            };
            match changes {
                Ok(changes) if !changes.is_empty() => {
                    let changes = self.stamp_origin(changes);
                    match self.push_changes_with_data(device_url, &changes).await {
                        Ok((applied, page_conflicts, server_errors)) => {
                            result.pushed += applied;
                            result.conflicts += page_conflicts;
                            result.warnings.extend(server_errors.into_iter().map(|e| format!("Server queued for retry: {}", e)));
                        }
                        Err(e) => result.errors.push(format!("Push of renamed recordings failed: {}", e)),
                    }
                }
                Ok(_) => {}
                Err(e) => result.errors.push(format!("Failed to read renamed recordings: {}", e)),
            }
        }

        // Update last sync time
        if let Err(e) = self.update_device_sync_time(device_id) {
            result.errors.push(format!("Failed to update sync time: {}", e));
        }
        if let Err(e) = self.config.lock().unwrap().set_last_device(device_id) {
            result.warnings.push(format!("Could not remember the last device: {}", e));
        }
        // The cards that arrived are devices now (Stage 5)
        if let Err(e) = self.adopt_devices_from_cards() {
            result.warnings.push(format!("The device list could not be read from the cards: {}", e));
        }

        result.success = result.errors.is_empty();
        result
    }

    /// **Check the connection** to a device (Stage 12): one row per thing
    /// that can be wrong, each with its refusal code, instead of a log
    /// search. Nothing is changed by a check.
    pub async fn check(&self, device_id: &str) -> Vec<crate::sync_protocol::CheckRow> {
        use crate::sync_protocol::CheckRow;
        fn row(name: &str, passed: bool, detail: impl Into<String>, code: &str) -> CheckRow {
            CheckRow { name: name.to_string(), passed, detail: detail.into(), code: code.to_string() }
        }
        self.begin_operation();
        let mut rows = Vec::new();
        let device = {
            let config = self.config.lock().unwrap();
            config.get_device(device_id).cloned()
        };
        let Some(device) = device else {
            rows.push(row("Device", false, format!("This device remembers no device {}", short_id(device_id)), ""));
            return rows;
        };
        let pinned = {
            let config = self.config.lock().unwrap();
            config.get_device(device_id).and_then(|p| p.certificate_fingerprint.clone()).unwrap_or_default()
        };
        let https = device.device_url.starts_with("https://");

        // 1. Reachable, and the certificate: one request without a key
        let client = match self.client_for(&device.device_url) {
            Ok(c) => c,
            Err(e) => {
                rows.push(row("Reachable", false, e.to_string(), codes::TLS_REQUIRED));
                return rows;
            }
        };
        match client.get(format!("{}/sync/status", device.device_url.trim_end_matches('/'))).timeout(Duration::from_secs(10)).send().await {
            Ok(response) if response.status().is_success() => {
                let body: serde_json::Value = response.json().await.unwrap_or(serde_json::Value::Null);
                let name = body.get("device_name").and_then(|v| v.as_str()).unwrap_or("?");
                let id = body.get("device_id").and_then(|v| v.as_str()).unwrap_or("");
                rows.push(row("Reachable", true, format!("{} answers at {}", name, device.device_url), ""));
                if !id.is_empty() && id != device_id {
                    rows.push(row("Device", false, format!("The device at {} is {}, not {}", device.device_url, short_id(id), short_id(device_id)), codes::DEVICE_MISMATCH));
                }
                if https {
                    rows.push(row("Certificate", true, if pinned.is_empty() { "Verified by the system's root certificates".to_string() } else { "The pinned fingerprint matches".to_string() }, ""));
                } else {
                    rows.push(row("Certificate", true, "Plain http on this machine itself; no certificate", ""));
                }
            }
            Ok(response) => {
                rows.push(row("Reachable", false, format!("{} answered with status {}", device.device_url, response.status()), ""));
                return rows;
            }
            Err(e) => {
                let sentence = describe(&e);
                let certificate = sentence.to_lowercase().contains("certificate") || sentence.to_lowercase().contains("fingerprint");
                if certificate {
                    rows.push(row("Reachable", true, format!("Something answers at {}", device.device_url), ""));
                    rows.push(row("Certificate", false, sentence, codes::CERTIFICATE_MISMATCH));
                } else {
                    rows.push(row("Reachable", false, format!("{} does not answer: {}", device.device_url, sentence), ""));
                }
                return rows;
            }
        }

        // 2. Account and key: the handshake, which is refused with a code
        match self.handshake(&device.device_url).await {
            Ok(handshake) => {
                match Self::check_protocol(&device.device_name, &handshake) {
                    Ok(()) => rows.push(row("Protocol", true, format!("Version {}", handshake.protocol_version), "")),
                    Err(sentence) => rows.push(row("Protocol", false, sentence, codes::PROTOCOL_TOO_OLD)),
                }
                let own = self.account_id();
                if handshake.account_id == own {
                    rows.push(row("Account", true, format!("The other device holds account {}", short_id(&own)), ""));
                } else {
                    rows.push(row("Account", false, format!("The other device holds account {}, this device {}", short_id(&handshake.account_id), short_id(&own)), codes::ACCOUNT_MISMATCH));
                }
                rows.push(row("Key", true, "This device's key is accepted", ""));
                if handshake.server_timestamp != 0 {
                    let skew = handshake.server_timestamp - Utc::now().timestamp();
                    match clock_skew_sentence(skew, &device.device_name) {
                        Some(sentence) => rows.push(row("Clock", false, sentence, "")),
                        None => rows.push(row("Clock", true, "The clocks agree to within a minute", "")),
                    }
                }
                if handshake.free_bytes > 0 {
                    let low = handshake.free_bytes < crate::transfer::FREE_SPACE_MARGIN;
                    rows.push(row("Free space there", !low, format!("{} MB free on {}", handshake.free_bytes / (1024 * 1024), device.device_name), ""));
                }
                rows.push(row("Recordings there", true, if handshake.supports_audiofiles { "The other device serves recordings" } else { "The other device serves notes only; no audio directory is configured there" }, ""));
            }
            Err(e) => {
                let sentence = e.to_string();
                let code = [codes::ACCOUNT_MISMATCH, codes::ACCOUNT_UNKNOWN, codes::ACCOUNT_MISSING, codes::ACCOUNT_DISAGREES]
                    .into_iter()
                    .find(|c| sentence.contains(c));
                match code {
                    Some(code) => rows.push(row("Account", false, sentence, code)),
                    None if sentence.contains(codes::PROTOCOL_TOO_OLD) => rows.push(row("Protocol", false, sentence, codes::PROTOCOL_TOO_OLD)),
                    None => {
                        let code = [codes::DEVICE_UNKNOWN, codes::DEVICE_REVOKED, codes::KEY_WRONG, codes::KEY_MISSING, codes::DEVICE_MISMATCH]
                            .into_iter()
                            .find(|c| sentence.contains(c))
                            .unwrap_or("");
                        rows.push(row("Key", false, sentence, code));
                    }
                }
            }
        }

        // 3. This side
        let here = self.audio_directory().unwrap_or_else(|| self.config.lock().unwrap().config_dir().to_path_buf());
        let free = crate::transfer::free_space(&here);
        rows.push(row("Free space here", free >= crate::transfer::FREE_SPACE_MARGIN, format!("{} MB free on this device", free / (1024 * 1024)), ""));
        #[cfg(feature = "server")]
        rows.push(row("Listener here", true, if crate::sync_server::server_running() { "This device is listening" } else { "This device is not listening; the other device cannot start an operation towards it" }, ""));
        rows
    }

    /// A failure that carries the id of the operation under way.
    fn failed(&self, sentence: String) -> SyncResult {
        let mut result = SyncResult::failure(sentence);
        result.request_id = self.request_id();
        result
    }

    /// Pull changes from a device (one-way)
    pub async fn pull_from_device(&self, device_id: &str) -> SyncResult {
        self.begin_operation();
        let device = {
            let config = self.config.lock().unwrap();
            config.get_device(device_id).cloned()
        };

        let device = match device {
            Some(p) => p,
            None => return self.failed(format!("Unknown device: {}", device_id)),
        };

        let mut result = SyncResult::success();
        result.request_id = self.request_id();

        let (reached, handshake) = match self.reach(device_id, &device.device_url).await {
            Ok(r) => r,
            Err(e) => return self.failed(format!("Handshake failed: {}", e)),
        };
        let device_url = &reached;
        if let Err(sentence) = self.check_account(device_id, &handshake) {
            return self.failed(sentence);
        }
        let cursors = self.device_cursors(device_id, &handshake, &mut result);

        self.snapshot_before("pull", &mut result);
        let pull = self.pull_all(device_url, device_id, &device.device_name, cursors.received).await;
        result.pulled = pull.applied;
        result.conflicts = pull.conflicts;
        result.errors.extend(pull.errors);
        result.warnings.extend(pull.warnings);

        if result.errors.is_empty() {
            if let Err(e) = self.update_device_sync_time(device_id) {
                result.errors.push(format!("Failed to update sync time: {}", e));
            }
        }

        result.success = result.errors.is_empty();
        result
    }

    /// Push changes to a device (one-way)
    pub async fn push_to_device(&self, device_id: &str) -> SyncResult {
        self.begin_operation();
        let device = {
            let config = self.config.lock().unwrap();
            config.get_device(device_id).cloned()
        };

        let device = match device {
            Some(p) => p,
            None => return self.failed(format!("Unknown device: {}", device_id)),
        };

        let mut result = SyncResult::success();
        result.request_id = self.request_id();

        let (reached, handshake) = match self.reach(device_id, &device.device_url).await {
            Ok(r) => r,
            Err(e) => return self.failed(format!("Handshake failed: {}", e)),
        };
        let device_url = &reached;
        if let Err(sentence) = self.check_account(device_id, &handshake) {
            return self.failed(sentence);
        }
        let cursors = self.device_cursors(device_id, &handshake, &mut result);
        let local_end = self.local_seq();

        let (pushed, conflicts, errors, warnings) = self.push_all(device_url, device_id, cursors.sent, local_end).await;
        result.pushed = pushed;
        result.conflicts = conflicts;
        result.errors.extend(errors);
        result.warnings.extend(warnings);

        if result.errors.is_empty() {
            if let Err(e) = self.update_device_sync_time(device_id) {
                result.errors.push(format!("Failed to update sync time: {}", e));
            }
        }

        result.success = result.errors.is_empty();
        result
    }

    /// Perform initial sync with a new device (full dataset transfer)
    ///
    /// This is used for first-time sync when we need to get the complete
    /// dataset from a device rather than incremental changes. Afterwards the
    /// cursors point at the end of both feeds, so the next sync is incremental.
    pub async fn initial_sync(&self, device_id: &str) -> SyncResult {
        self.begin_operation();
        let device = {
            let config = self.config.lock().unwrap();
            config.get_device(device_id).cloned()
        };

        let device = match device {
            Some(p) => p,
            None => return self.failed(format!("Unknown device: {}", device_id)),
        };

        let mut result = SyncResult::success();
        result.request_id = self.request_id();

        // Step 1: Handshake
        let (reached, handshake) = match self.reach(device_id, &device.device_url).await {
            Ok(r) => r,
            Err(e) => return self.failed(format!("Handshake failed: {}", e)),
        };
        let device_url = &reached;
        if let Err(sentence) = self.check_account(device_id, &handshake) {
            return self.failed(sentence);
        }

        // Step 2: Pull the device's whole feed from the beginning, page by
        // page: resumable, and bounded in memory for a large database.
        if let Err(e) = self.save_device_cursors(device_id, Some(0), Some(0), Some(handshake.database_id.as_str())) {
            result.errors.push(format!("Failed to reset cursors: {}", e));
        }
        self.snapshot_before("initial sync", &mut result);
        let pull = self.pull_all(device_url, device_id, &device.device_name, 0).await;
        result.pulled = pull.applied;
        result.conflicts = pull.conflicts;
        result.errors.extend(pull.errors);
        result.warnings.extend(pull.warnings);

        // Step 6: Push everything we have (the device de-duplicates)
        let local_end = self.local_seq();
        let (pushed, conflicts, errors, warnings) = self.push_all(device_url, device_id, 0, local_end).await;
        result.pushed = pushed;
        result.conflicts += conflicts;
        result.errors.extend(errors);
        result.warnings.extend(warnings);

        // Step 7: Update sync timestamp
        if let Err(e) = self.update_device_sync_time(device_id) {
            result.errors.push(format!("Failed to update sync time: {}", e));
        }

        result.success = result.errors.is_empty();
        result
    }

    /// Where we stand with a device. If the device's database identity changed
    /// (it was reset or replaced) both cursors restart from zero: everything
    /// is exchanged again, which is safe because applying is idempotent.
    /// The account this database belongs to.
    fn account_id(&self) -> String {
        self.db.lock().ok().and_then(|db| db.account_id().ok()).unwrap_or_default()
    }

    /// The account check on the caller's side (ACCT-3): the device must hold
    /// the same account as this database, or nothing is exchanged. Returns
    /// the sentence to refuse with. Never adopts.
    /// A responder of an older protocol is refused, in words (Stage 16).
    fn check_protocol(device_name: &str, handshake: &HandshakeResponse) -> Result<(), String> {
        if crate::sync_protocol::protocol_major(&handshake.protocol_version).unwrap_or(0) < crate::sync_protocol::PROTOCOL_MAJOR {
            return Err(format!("Update Voice on {} ({})", device_name, codes::PROTOCOL_TOO_OLD));
        }
        Ok(())
    }

    fn check_account(&self, device_id: &str, handshake: &HandshakeResponse) -> Result<(), String> {
        if let Err(sentence) = Self::check_protocol(&self.config.lock().map(|c| c.get_device(device_id).map(|p| p.device_name.clone()).unwrap_or_default()).unwrap_or_default(), handshake) {
            return Err(sentence);
        }
        let own = self.account_id();
        if handshake.account_id.is_empty() {
            return Err(format!("The other device named no account ({})", codes::ACCOUNT_MISSING));
        }
        if handshake.account_id != own {
            let before = self
                .db
                .lock()
                .ok()
                .and_then(|db| db.get_device_account_id(device_id).ok().flatten());
            let mut sentence = format!(
                "The other device holds account {}; this device holds {}; nothing was exchanged",
                &handshake.account_id[..UUID_SHORT_LEN.min(handshake.account_id.len())],
                &own[..UUID_SHORT_LEN.min(own.len())]
            );
            if let Some(before) = before {
                if before == own {
                    sentence.push_str(". This device used to hold this account; the device at its address has changed");
                }
            }
            sentence.push_str(&format!(" ({})", codes::ACCOUNT_MISMATCH));
            tracing::warn!("{}", sentence);
            return Err(sentence);
        }
        if let Ok(db) = self.db.lock() {
            let _ = db.set_device_account_id(device_id, Some(&handshake.device_name), &handshake.account_id);
        }
        Ok(())
    }

    /// A snapshot before this device applies anything (SNAP-3); an in-memory
    /// database has nothing to snapshot and is skipped.
    fn snapshot_before(&self, what: &str, result: &mut SyncResult) {
        if let Ok(db) = self.db.lock() {
            if let Err(e) = db.snapshot_before(what) {
                result.warnings.push(format!("Could not take a snapshot before {}: {}", what, e));
            }
        }
    }

    fn device_cursors(&self, device_id: &str, handshake: &HandshakeResponse, result: &mut SyncResult) -> DeviceCursors {
        let (received, sent, known_db) = {
            let db = self.db.lock().unwrap();
            db.get_device_cursors(device_id).unwrap_or((0, 0, None))
        };
        let now = handshake.database_id.as_str();
        match &known_db {
            Some(before) if !now.is_empty() && now != before => {
                let msg = format!(
                    "Device {} has a new database ({} -> {}); exchanging everything again",
                    &device_id[..UUID_SHORT_LEN.min(device_id.len())],
                    &before[..UUID_SHORT_LEN.min(before.len())],
                    &now[..UUID_SHORT_LEN.min(now.len())]
                );
                tracing::warn!("{}", msg);
                result.warnings.push(msg);
                let _ = self.save_device_cursors(device_id, Some(0), Some(0), Some(now));
                DeviceCursors { received: 0, sent: 0 }
            }
            None if !now.is_empty() => {
                let _ = self.save_device_cursors(device_id, None, None, Some(now));
                DeviceCursors { received, sent }
            }
            _ => DeviceCursors { received, sent },
        }
    }

    fn local_seq(&self) -> i64 {
        self.db.lock().ok().and_then(|db| db.current_seq().ok()).unwrap_or(0)
    }

    fn save_device_cursors(&self, device_id: &str, received: Option<i64>, sent: Option<i64>, database_id: Option<&str>) -> VoiceResult<()> {
        let device_name = {
            let config = self.config.lock().unwrap();
            config.get_device(device_id).map(|p| p.device_name.clone())
        };
        let db = self.db.lock().unwrap();
        db.set_device_cursors(device_id, device_name.as_deref(), received, sent, database_id)
    }

    /// Pull every page after `cursor`, applying each and saving the cursor
    /// before fetching the next, so an interrupted sync resumes where it stopped.
    async fn pull_all(&self, device_url: &str, device_id: &str, device_name: &str, mut cursor: i64) -> PullOutcome {
        let mut outcome = PullOutcome { applied: 0, conflicts: 0, changes: Vec::new(), errors: Vec::new(), warnings: Vec::new() };
        for page in 0..MAX_PAGES {
            if self.cancelled() {
                outcome.errors.push(CANCELLED.to_string());
                break;
            }
            match self.pull_page(device_url, device_id, device_name, cursor).await {
                Ok((pull, next_cursor, complete)) => {
                    outcome.applied += pull.applied;
                    outcome.conflicts += pull.conflicts;
                    outcome.changes.extend(pull.changes);
                    outcome.errors.extend(pull.errors);
                    outcome.warnings.extend(pull.warnings);
                    cursor = next_cursor;
                    self.report("sync", outcome.applied, 0, 0, format!("Received {} changes from {}", outcome.applied, device_name));
                    if let Err(e) = self.save_device_cursors(device_id, Some(cursor), None, None) {
                        outcome.errors.push(format!("Failed to save cursor: {}", e));
                        break;
                    }
                    if complete {
                        // Anything queued for retry (a row that arrived before
                        // the row it references) gets one more chance now,
                        // instead of waiting for the next sync.
                        if self.db.lock().map(|db| db.count_pending_sync_failures().unwrap_or(0)).unwrap_or(0) > 0 {
                            if let Ok((applied, conflicts, _)) = self.apply_changes_from(&[], device_id, Some(device_name)) {
                                outcome.applied += applied;
                                outcome.conflicts += conflicts;
                            }
                        }
                        // A refusal a retry has applied since is no failure: only the
                        // changes still queued stand as errors (Q2 of 2026-09-14)
                        let pending: Vec<(String, String)> = self
                            .db
                            .lock()
                            .ok()
                            .and_then(|db| db.get_pending_sync_failures().ok())
                            .map(|failures| failures.into_iter().map(|(_, change)| (change.entity_type, change.entity_id)).collect())
                            .unwrap_or_default();
                        outcome.errors = crate::sync_apply::errors_still_standing(std::mem::take(&mut outcome.errors), &pending);
                        // Tell the device this device holds its feed up to here (D29): it
                        // counts what it duplicated by the cursor a device asks after
                        self.confirm_received(device_url, cursor).await;
                        break;
                    }
                    if page + 1 == MAX_PAGES {
                        outcome.warnings.push("Pull stopped after the page limit; run sync again to continue".to_string());
                    }
                }
                Err(e) => {
                    outcome.errors.push(format!("Pull failed: {}", e));
                    break;
                }
            }
        }
        outcome
    }

    /// Push our changes with `sent < seq <= upto`, page by page, saving the
    /// high-water mark after every page the device accepted.
    async fn push_all(&self, device_url: &str, device_id: &str, mut sent: i64, upto: i64) -> (i64, i64, Vec<String>, Vec<String>) {
        let mut pushed = 0;
        let mut conflicts = 0;
        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        let mut server_queued = false;
        for _ in 0..MAX_PAGES {
            if self.cancelled() {
                errors.push(CANCELLED.to_string());
                break;
            }
            let (changes, next, complete) = {
                let db = self.db.lock().unwrap();
                match db.get_changes_after_seq_as_sync_changes(sent, Some(upto), self.page_size()) {
                    Ok(page) => page,
                    Err(e) => {
                        errors.push(format!("Failed to get local changes: {}", e));
                        return (pushed, conflicts, errors, warnings);
                    }
                }
            };
            if changes.is_empty() {
                break;
            }
            let changes = self.stamp_origin(changes);
            let _ = &mut server_queued;
            tracing::debug!("Pushing {} changes to {}", changes.len(), &device_id[..UUID_SHORT_LEN.min(device_id.len())]);
            match self.push_changes_with_data(device_url, &changes).await {
                Ok((applied, page_conflicts, server_errors)) => {
                    pushed += applied;
                    conflicts += page_conflicts;
                    server_queued |= !server_errors.is_empty();
                    warnings.extend(server_errors.into_iter().map(|e| format!("Server queued for retry: {}", e)));
                    sent = next;
                    if let Err(e) = self.save_device_cursors(device_id, None, Some(sent), None) {
                        errors.push(format!("Failed to save cursor: {}", e));
                        break;
                    }
                    if complete {
                        break;
                    }
                }
                Err(e) => {
                    tracing::warn!("Push error: {}", e);
                    errors.push(format!("Push failed: {}", e));
                    break;
                }
            }
        }
        // A passive server retries queued changes only when a batch arrives;
        // an empty batch now drains what this push left behind.
        if server_queued && errors.is_empty() {
            match self.push_changes_with_data_allow_empty(device_url).await {
                Ok((applied, page_conflicts, still_failing)) => {
                    pushed += applied;
                    conflicts += page_conflicts;
                    if !still_failing.is_empty() {
                        warnings.push(format!("{} change(s) still queued on the other device", still_failing.len()));
                    }
                }
                Err(e) => warnings.push(format!("Could not ask the other device to retry queued changes: {}", e)),
            }
        }
        (pushed, conflicts, errors, warnings)
    }

    /// Post an empty batch: the device retries whatever it queued.
    async fn push_changes_with_data_allow_empty(&self, device_url: &str) -> VoiceResult<(i64, i64, Vec<String>)> {
        let request = ApplyRequest {
            device_id: self.this_device_id.clone(),
            device_name: self.this_device_name.clone(),
            changes: Vec::new(),
        };
        let response = self
            .authed(self.client_for(device_url)?.post(format!("{}/sync/apply", device_url)))
            .json(&request)
            .send()
            .await
            .map_err(|e| VoiceError::Network(describe(&e)))?;
        if !response.status().is_success() {
            return Err(VoiceError::Sync(format!("Retry request failed with status {}", response.status())));
        }
        let result: ApplyResponse = response
            .json()
            .await
            .map_err(|e| VoiceError::Sync(format!("Failed to parse apply response: {}", e)))?;
        Ok((result.applied, result.conflicts, result.errors))
    }

    /// Fill in this device's identity on outgoing changes.
    fn stamp_origin(&self, changes: Vec<SyncChange>) -> Vec<SyncChange> {
        changes
            .into_iter()
            .map(|mut c| {
                if c.device_id.is_empty() {
                    c.device_id = self.this_device_id.clone();
                }
                if c.device_name.is_none() {
                    c.device_name = Some(self.this_device_name.clone());
                }
                c
            })
            .collect()
    }

    /// Check if a device is reachable
    pub async fn check_device_status(&self, device_id: &str) -> HashMap<String, serde_json::Value> {
        let device = {
            let config = self.config.lock().unwrap();
            config.get_device(device_id).cloned()
        };

        let device = match device {
            Some(p) => p,
            None => {
                let mut result = HashMap::new();
                result.insert("reachable".to_string(), serde_json::Value::Bool(false));
                result.insert(
                    "error".to_string(),
                    serde_json::Value::String("Unknown device".to_string()),
                );
                return result;
            }
        };

        let client = match self.client_for(&device.device_url) {
            Ok(c) => c,
            Err(e) => {
                let mut result = HashMap::new();
                result.insert("reachable".to_string(), serde_json::Value::Bool(false));
                result.insert("error".to_string(), serde_json::Value::String(e.to_string()));
                return result;
            }
        };
        match client
            .get(format!("{}/sync/status", device.device_url))
            .send()
            .await
        {
            Ok(response) => {
                if response.status().is_success() {
                    match response.json::<serde_json::Value>().await {
                        Ok(data) => {
                            let mut result = HashMap::new();
                            result.insert("reachable".to_string(), serde_json::Value::Bool(true));
                            if let Some(reported_device_id) = data.get("device_id") {
                                result.insert("device_id".to_string(), reported_device_id.clone());
                            }
                            if let Some(device_name) = data.get("device_name") {
                                result.insert("device_name".to_string(), device_name.clone());
                            }
                            if let Some(supports_audiofiles) = data.get("supports_audiofiles") {
                                result.insert("supports_audiofiles".to_string(), supports_audiofiles.clone());
                            }
                            result
                        }
                        Err(e) => {
                            let mut result = HashMap::new();
                            result.insert("reachable".to_string(), serde_json::Value::Bool(false));
                            result.insert(
                                "error".to_string(),
                                serde_json::Value::String(e.to_string()),
                            );
                            result
                        }
                    }
                } else {
                    let mut result = HashMap::new();
                    result.insert("reachable".to_string(), serde_json::Value::Bool(false));
                    result.insert(
                        "error".to_string(),
                        serde_json::Value::String(format!("HTTP {}", response.status())),
                    );
                    result
                }
            }
            Err(e) => {
                let mut result = HashMap::new();
                result.insert("reachable".to_string(), serde_json::Value::Bool(false));
                result.insert(
                    "error".to_string(),
                    serde_json::Value::String(e.to_string()),
                );
                result
            }
        }
    }

    // Internal methods

    async fn handshake(&self, device_url: &str) -> VoiceResult<HandshakeResponse> {
        let client = self.client_for(device_url)?;
        self.handshake_with(&client, device_url).await
    }

    /// The handshake with a device, at its remembered address, and when that
    /// address does not answer, at each address the device's card names, in
    /// turn, through a client pinned to the device's certificate (LISTEN-4). An
    /// address that answers for this device becomes the remembered one. Returns
    /// the address that answered and the handshake.
    async fn reach(&self, device_id: &str, remembered: &str) -> VoiceResult<(String, HandshakeResponse)> {
        let first = self.handshake(remembered).await;
        if !matches!(first, Err(VoiceError::Network(_))) {
            return first.map(|h| (remembered.to_string(), h));
        }
        let card = self.db.lock().ok().and_then(|db| db.get_device_card(device_id).ok().flatten());
        let addresses: Vec<String> = card
            .as_ref()
            .and_then(|c| serde_json::from_str::<Vec<String>>(&c.addresses).ok())
            .unwrap_or_default();
        let (name, pin) = {
            let config = self.config.lock().unwrap();
            let device = config.get_device(device_id);
            (
                device.map(|p| p.device_name.clone()).unwrap_or_default(),
                device.and_then(|p| p.certificate_fingerprint.clone())
                    .or_else(|| card.as_ref().map(|c| c.certificate_fingerprint.clone()).filter(|f| !f.is_empty()))
                    .unwrap_or_default(),
            )
        };
        for url in addresses.iter().filter(|u| u.trim_end_matches('/') != remembered.trim_end_matches('/')) {
            if check_scheme(url).is_err() {
                continue;
            }
            let Ok(client) = build_client(&pin, url) else { continue };
            match self.handshake_with(&client, url).await {
                Ok(handshake) if handshake.device_id == device_id => {
                    {
                        let mut config = self.config.lock().unwrap();
                        let pin = if pin.is_empty() { None } else { Some(pin.as_str()) };
                        config.add_device(device_id, &name, url, pin, true)?;
                    }
                    self.clients.lock().unwrap().clear();
                    tracing::info!("{} answered at {}; {} did not", short_id(device_id), url, remembered);
                    return Ok((url.clone(), handshake));
                }
                Ok(_) => tracing::debug!("Another device answers at {}", url),
                Err(e) => tracing::debug!("{} did not answer at {}: {}", short_id(device_id), url, e),
            }
        }
        first.map(|h| (remembered.to_string(), h))
    }

    async fn handshake_with(&self, client: &Client, device_url: &str) -> VoiceResult<HandshakeResponse> {
        let request = HandshakeRequest {
            device_id: self.this_device_id.clone(),
            device_name: self.this_device_name.clone(),
            protocol_version: PROTOCOL_VERSION.to_string(),
            account_id: self.account_id(),
            application: crate::auth::APPLICATION_VOICE.to_string(),
            // Voice wants every type; an image application would declare ["tag"]
            entity_types: Vec::new(),
        };

        let response = self
            .authed(client.post(format!("{}/sync/handshake", device_url)))
            .json(&request)
            .send()
            .await
            .map_err(|e| VoiceError::Network(describe(&e)))?;

        if !response.status().is_success() {
            let status = response.status();
            // A refusal carries a sentence and a code; pass both on.
            let body = response.text().await.unwrap_or_default();
            if let Ok(refusal) = serde_json::from_str::<ErrorResponse>(&body) {
                if !refusal.error.is_empty() {
                    return Err(VoiceError::Sync(refusal.error));
                }
            }
            return Err(VoiceError::Sync(format!("Handshake failed with status {}", status)));
        }

        response
            .json::<HandshakeResponse>()
            .await
            .map_err(|e| VoiceError::Sync(format!("Failed to parse handshake response: {}", e)))
    }

    /// One page of the device's feed after `cursor`. Returns what was applied,
    /// the cursor to continue from, and whether the feed is exhausted.
    /// One request for the changes after `cursor`, whose answer is not applied:
    /// the device learns this device holds its feed up to `cursor` (PROOF-1, D29).
    /// A failure changes nothing here; the next pull says the same.
    async fn confirm_received(&self, device_url: &str, cursor: i64) {
        let url = format!("{}/sync/changes?cursor={}&limit=1", device_url, cursor);
        let sent = match self.client_for(device_url) {
            Ok(client) => self.authed(client.get(&url)).send().await.map(|_| ()).map_err(|e| describe(&e)),
            Err(e) => Err(e.to_string()),
        };
        if let Err(e) = sent {
            tracing::debug!("The other device was not told this device holds its feed up to {}: {}", cursor, e);
        }
    }

    async fn pull_page(&self, device_url: &str, device_id: &str, device_name: &str, cursor: i64) -> VoiceResult<(PullOutcome, i64, bool)> {
        let url = format!("{}/sync/changes?cursor={}&limit={}", device_url, cursor, self.page_size());

        let response = self
            .authed(self.client_for(device_url)?.get(&url))
            .send()
            .await
            .map_err(|e| VoiceError::Network(describe(&e)))?;

        if !response.status().is_success() {
            return Err(VoiceError::Sync(format!(
                "Pull failed with status {}",
                response.status()
            )));
        }

        let batch: ChangesResponse = response
            .json()
            .await
            .map_err(|e| VoiceError::Sync(format!("Failed to parse changes: {}", e)))?;

        let next_cursor = match batch.next_cursor {
            Some(n) => n,
            None => {
                return Err(VoiceError::Sync(
                    "The other device's answer names no cursor to continue from".to_string(),
                ))
            }
        };

        // Changes carry the sender's identity so that failures are queued
        // against the right device
        let mut changes = batch.changes;
        for c in &mut changes {
            if c.device_id.is_empty() {
                c.device_id = batch.device_id.clone();
            }
            if c.device_name.is_none() {
                c.device_name = Some(if batch.device_name.is_empty() { device_name.to_string() } else { batch.device_name.clone() });
            }
        }

        tracing::debug!("Received {} changes from {} (cursor {} -> {})", changes.len(), &device_id[..UUID_SHORT_LEN.min(device_id.len())], cursor, next_cursor);
        for change in &changes {
            tracing::trace!("  Pull: {} {} from {}", change.entity_type, &change.entity_id[..UUID_SHORT_LEN.min(change.entity_id.len())], change.device_id);
        }

        let (applied, conflicts, errors) = self.apply_changes_from(&changes, device_id, Some(device_name))?;
        tracing::debug!("Applied {} changes, {} conflicts", applied, conflicts);

        Ok((
            PullOutcome { applied, conflicts, changes, errors, warnings: Vec::new() },
            next_cursor,
            batch.is_complete,
        ))
    }

    fn apply_changes_from(&self, changes: &[SyncChange], device_id: &str, device_name: Option<&str>) -> VoiceResult<(i64, i64, Vec<String>)> {
        let db = self.db.lock().unwrap();
        let sync_received_at = Utc::now().timestamp();
        let outcome = crate::sync_apply::apply_changes(&db, changes, device_id, device_name, sync_received_at)?;
        // Names changed by the device, or by a collision it brought, reach the disk (FILE-15)
        if let Some(dir) = self.config.lock().ok().and_then(|c| c.audiofile_directory().map(std::path::PathBuf::from)) {
            if let Err(e) = db.apply_pending_file_renames(&dir) {
                tracing::warn!("Recording names were not all settled on disk: {}", e);
            }
        }
        if outcome.retried_ok > 0 {
            tracing::info!("Applied {} previously failed changes", outcome.retried_ok);
        }
        Ok((outcome.applied, outcome.conflicts, outcome.errors))
    }

    fn update_device_sync_time(&self, device_id: &str) -> VoiceResult<()> {
        let (name, url) = {
            let config = self.config.lock().unwrap();
            config.get_device(device_id).map(|p| (p.device_name.clone(), p.device_url.clone())).unwrap_or_default()
        };
        self.db.lock().unwrap().set_device_last_operation(device_id, Some(&name), Some(&url), "sync")
    }

    /// Push pre-fetched changes to device (used by initial_sync)
    async fn push_changes_with_data(
        &self,
        device_url: &str,
        changes: &[SyncChange],
    ) -> VoiceResult<(i64, i64, Vec<String>)> {
        if changes.is_empty() {
            return Ok((0, 0, Vec::new()));
        }

        let request = ApplyRequest {
            device_id: self.this_device_id.clone(),
            device_name: self.this_device_name.clone(),
            changes: changes.to_vec(),
        };

        let response = self
            .authed(self.client_for(device_url)?.post(format!("{}/sync/apply", device_url)))
            .json(&request)
            .send()
            .await
            .map_err(|e| VoiceError::Network(describe(&e)))?;

        if !response.status().is_success() {
            return Err(VoiceError::Sync(format!(
                "Push failed with status {}",
                response.status()
            )));
        }

        let result: ApplyResponse = response
            .json()
            .await
            .map_err(|e| VoiceError::Sync(format!("Failed to parse apply response: {}", e)))?;

        for err in &result.errors {
            tracing::warn!("Server could not apply a change (queued there for retry): {}", err);
        }

        Ok((result.applied, result.conflicts, result.errors))
    }

    /// The client used for a recording's bytes: no overall timeout, because
    /// an eight-hour recording takes as long as it takes, but a read that
    /// stalls for thirty seconds is over (FILE-14).
    fn file_client_for(&self, device_url: &str) -> VoiceResult<Client> {
        check_scheme(device_url)?;
        let pin = self.pin_for(device_url);
        let cache_key = format!("file\n{}\n{}", device_url, pin);
        if let Some(client) = self.clients.lock().unwrap().get(&cache_key) {
            return Ok(client.clone());
        }
        let builder = Client::builder()
            .connect_timeout(connect_timeout_for(device_url))
            .read_timeout(Duration::from_secs(30));
        let builder = if pin.is_empty() {
            builder
        } else {
            builder.use_preconfigured_tls(crate::tls::pinned_client_config(&pin)?)
        };
        let client = builder.build().map_err(|e| VoiceError::Network(e.to_string()))?;
        self.clients.lock().unwrap().insert(cache_key, client.clone());
        Ok(client)
    }

    /// The client a recording's bytes are sent with: no overall timeout and no
    /// read timeout, because the connection reads nothing while a body goes
    /// out; [`crate::transfer::stall_of_upload`] ends a send that stops moving.
    fn send_client_for(&self, device_url: &str) -> VoiceResult<Client> {
        check_scheme(device_url)?;
        let pin = self.pin_for(device_url);
        let cache_key = format!("send\n{}\n{}", device_url, pin);
        if let Some(client) = self.clients.lock().unwrap().get(&cache_key) {
            return Ok(client.clone());
        }
        let builder = Client::builder()
            .connect_timeout(connect_timeout_for(device_url));
        let builder = if pin.is_empty() {
            builder
        } else {
            builder.use_preconfigured_tls(crate::tls::pinned_client_config(&pin)?)
        };
        let client = builder.build().map_err(|e| VoiceError::Network(e.to_string()))?;
        self.clients.lock().unwrap().insert(cache_key, client.clone());
        Ok(client)
    }

    fn pin_for(&self, device_url: &str) -> String {
        let config = self.config.lock().unwrap();
        config
            .devices()
            .iter()
            .find(|p| p.device_url.trim_end_matches('/') == device_url.trim_end_matches('/'))
            .and_then(|p| p.certificate_fingerprint.clone())
            .unwrap_or_default()
    }

    /// Fetch one recording from a device (FILE-12): streamed into
    /// `<file>.part`, continuing from the bytes already there, verified by
    /// the hash the device announces, and renamed when whole. Returns the
    /// bytes received in this call.
    pub async fn fetch_audio_file(
        &self,
        device_url: &str,
        audio_id: &str,
        dest_path: &std::path::Path,
        moved_before: u64,
        done: i64,
        total_files: i64,
    ) -> VoiceResult<u64> {
        use futures_util::StreamExt;
        use tokio::io::AsyncWriteExt;

        let url = format!("{}/sync/audio/{}/file", device_url, audio_id);
        if let Some(parent) = dest_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let have = crate::transfer::part_len(dest_path);
        let mut request = self.authed(self.file_client_for(device_url)?.get(&url));
        if have > 0 {
            request = request.header("Range", format!("bytes={}-", have));
        }
        let response = request
            .send()
            .await
            .map_err(|e| VoiceError::Network(format!("Failed to fetch audio {}: {}", audio_id, describe(&e))))?;
        let status = response.status();
        if !status.is_success() {
            let error_body = response.text().await.unwrap_or_default();
            let error_msg = serde_json::from_str::<ErrorResponse>(&error_body).map(|r| r.error).unwrap_or(error_body);
            return Err(VoiceError::Network(format!("Failed to fetch audio {}: HTTP {} - {}", audio_id, status, error_msg)));
        }
        // A server that ignored the Range starts over; so do we.
        let resumed = status == reqwest::StatusCode::PARTIAL_CONTENT;
        let start = if resumed { have } else { 0 };
        let remaining: Option<u64> = response.headers().get("content-length").and_then(|v| v.to_str().ok()).and_then(|v| v.parse().ok());
        let total = remaining.map(|r| start + r);
        let expected_hash = response
            .headers()
            .get(crate::sync_protocol::HEADER_FILE_SHA256)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let encrypted = response.headers().get(crate::sync_protocol::HEADER_ENCRYPTED).is_some();
        if let (Some(remaining), Some(parent)) = (remaining, dest_path.parent()) {
            crate::transfer::check_free_space(parent, remaining)?;
        }
        let part = crate::transfer::part_path(dest_path);
        let mut out = if resumed {
            tokio::fs::OpenOptions::new().append(true).open(&part).await?
        } else {
            tokio::fs::File::create(&part).await?
        };
        let mut stream = response.bytes_stream();
        let mut received = 0u64;
        let mut reported_at = 0u64;
        while let Some(chunk) = stream.next().await {
            if self.cancelled() {
                // The part stays; the next fetch continues from it
                out.flush().await?;
                return Err(VoiceError::Sync(CANCELLED.to_string()));
            }
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(e) => {
                    // What arrived stays in the part, for the next try to continue from
                    let _ = out.flush().await;
                    return Err(VoiceError::Network(format!("The fetch of {} stopped: {}", audio_id, describe(&e))));
                }
            };
            out.write_all(&chunk).await?;
            received += chunk.len() as u64;
            if received - reported_at >= PROGRESS_EVERY_BYTES {
                reported_at = received;
                self.report("fetch", done, total_files, moved_before + received, format!("Fetching recording {} of {}: {} MB", done + 1, total_files, (moved_before + received) / (1024 * 1024)));
            }
        }
        out.flush().await?;
        drop(out);
        let total = total.unwrap_or(start + received);
        crate::transfer::complete(dest_path, total, expected_hash.as_deref())?;
        // Bytes a keyless device kept as the bucket holds them (ENC-4): opened
        // here when this device holds the recording key, kept as they are otherwise
        if encrypted {
            let key = self.config.lock().ok().and_then(|c| c.recording_key());
            match key {
                Some(key) => {
                    let mut name = dest_path.file_name().map(|n| n.to_os_string()).unwrap_or_default();
                    name.push(crate::crypto::OBJECT_SUFFIX);
                    let enc = dest_path.with_file_name(name);
                    std::fs::rename(dest_path, &enc)?;
                    let opened = crate::crypto::decrypt_file(&key, &enc, dest_path);
                    let _ = std::fs::remove_file(&enc);
                    opened.map_err(|e| VoiceError::Sync(format!("The recording {} did not open with the recording key: {}", audio_id, e)))?;
                }
                None => tracing::warn!("Recording {} arrived encrypted and this device holds no recording key; kept as it is", audio_id),
            }
        }
        Ok(received)
    }

    /// Send one recording to a device (FILE-12): streamed from the file, from
    /// `from_byte` when the device already holds a part, with the size and
    /// the hash in headers so the device can verify. Returns the bytes sent.
    pub async fn send_audio_file(
        &self,
        device_url: &str,
        audio_id: &str,
        source_path: &std::path::Path,
        from_byte: u64,
        moved_before: u64,
        done: i64,
        total_files: i64,
    ) -> VoiceResult<u64> {
        use futures_util::StreamExt;
        use tokio::io::AsyncSeekExt;

        let url = format!("{}/sync/audio/{}/file", device_url, audio_id);
        let total = std::fs::metadata(source_path)
            .map_err(|e| VoiceError::Io(std::io::Error::new(e.kind(), format!("Failed to read audio file {}: {}", source_path.display(), e))))?
            .len();
        let from_byte = from_byte.min(total);
        // The row's hash (Stage 13), computed and stored once when it is missing,
        // instead of hashing the whole file before every send
        let hash = {
            let db = self.db.lock().unwrap();
            match db.get_audio_file(audio_id).ok().flatten().and_then(|r| r.content_sha256) {
                Some(h) => h,
                None => {
                    let h = crate::transfer::file_sha256(source_path)?;
                    let _ = db.set_content_hash(audio_id, &h);
                    h
                }
            }
        };
        let mut file = tokio::fs::File::open(source_path).await?;
        if from_byte > 0 {
            file.seek(std::io::SeekFrom::Start(from_byte)).await?;
        }
        // Counted and cancellable chunk by chunk: a cancel ends the body
        // early, the device keeps the part, and the next send continues from it
        let cancel = self.cancel.clone();
        let progress = self.progress.lock().unwrap().clone();
        let counted = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let counter = counted.clone();
        let stream = tokio_util::io::ReaderStream::with_capacity(file, crate::transfer::CHUNK)
            .take_while(move |_| {
                let go = !cancel.load(std::sync::atomic::Ordering::SeqCst);
                std::future::ready(go)
            })
            .inspect(move |chunk| {
                if let Ok(chunk) = chunk {
                    let sent_so_far = counter.fetch_add(chunk.len() as u64, std::sync::atomic::Ordering::SeqCst) + chunk.len() as u64;
                    if sent_so_far / PROGRESS_EVERY_BYTES != (sent_so_far - chunk.len() as u64) / PROGRESS_EVERY_BYTES {
                        if let Some(sink) = &progress {
                            sink.report(Progress { stage: "send".to_string(), done, total: total_files, bytes: moved_before + sent_so_far, sentence: format!("Sending recording {} of {}: {} MB", done + 1, total_files, (moved_before + sent_so_far) / (1024 * 1024)) });
                        }
                    }
                }
            });
        let mut request = self
            .authed(self.send_client_for(device_url)?.post(&url))
            .header("Content-Type", "application/octet-stream")
            .header("Content-Length", (total - from_byte).to_string())
            .header(crate::sync_protocol::HEADER_FILE_SHA256, &hash);
        if from_byte > 0 {
            request = request.header("Content-Range", format!("bytes {}-{}/{}", from_byte, total.saturating_sub(1), total));
        }
        let stalled = crate::transfer::stall_of_upload(counted.clone(), total - from_byte);
        let sent = tokio::select! {
            sent = request.body(reqwest::Body::wrap_stream(stream)).send() => sent,
            why = stalled => return Err(VoiceError::Network(format!("Failed to send audio {}: {}", audio_id, why))),
        };
        let response = match sent {
            Ok(response) => response,
            // A body ended early by a cancel is reported as a cancel, not a network failure
            Err(_) if self.cancelled() => return Err(VoiceError::Sync(CANCELLED.to_string())),
            Err(e) => return Err(VoiceError::Network(format!("Failed to send audio {}: {}", audio_id, describe(&e)))),
        };

        if self.cancelled() && counted.load(std::sync::atomic::Ordering::SeqCst) < total - from_byte {
            return Err(VoiceError::Sync(CANCELLED.to_string()));
        }
        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            let error_msg = serde_json::from_str::<ErrorResponse>(&error_body).map(|r| r.error).unwrap_or(error_body);
            return Err(VoiceError::Network(format!("Failed to send audio {}: HTTP {} - {}", audio_id, status, error_msg)));
        }
        tracing::info!("Sent audio file {} ({} bytes from byte {})", audio_id, total - from_byte, from_byte);
        Ok(total - from_byte)
    }

    /// Ask the device which of these recordings it lacks, and how much of each
    /// it already holds (FILE-12): one round trip for any number of files.
    async fn missing_on_device(&self, device_url: &str, audio_ids: Vec<String>) -> VoiceResult<crate::sync_protocol::MissingFilesResponse> {
        let response = self
            .authed(self.client_for(device_url)?.post(format!("{}/sync/audio/missing", device_url)))
            .json(&crate::sync_protocol::MissingFilesRequest { audio_ids })
            .send()
            .await
            .map_err(|e| VoiceError::Network(describe(&e)))?;
        if !response.status().is_success() {
            return Err(VoiceError::Sync(format!("The other device would not say which files it lacks: HTTP {}", response.status())));
        }
        response
            .json()
            .await
            .map_err(|e| VoiceError::Sync(format!("Could not read the other device's missing list: {}", e)))
    }

    /// A transfer is tried three times: the second try straight after the
    /// first, the third after a minute (FILE-14). A refusal (HTTP 4xx) and a
    /// cancel are not tried again.
    async fn with_retries<F, Fut>(&self, what: &str, mut attempt: F) -> VoiceResult<u64>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = VoiceResult<u64>>,
    {
        let mut last = None;
        for try_number in 1..=crate::transfer::TRIES {
            match attempt().await {
                Ok(n) => return Ok(n),
                Err(e) => {
                    let text = e.to_string();
                    let refused = text.contains("HTTP 4") || text.ends_with(CANCELLED);
                    let again = !refused && try_number < crate::transfer::TRIES;
                    tracing::warn!("{} failed (try {} of {}): {}{}", what, try_number, crate::transfer::TRIES, text, if again { "; trying again" } else { "" });
                    last = Some(e);
                    if !again {
                        break;
                    }
                    if try_number + 1 == crate::transfer::TRIES {
                        tokio::time::sleep(crate::transfer::wait_before_last_try()).await;
                    }
                }
            }
        }
        Err(last.unwrap_or_else(|| VoiceError::Sync(format!("{} failed", what))))
    }

    /// **Send** (terms): every recording this device holds that the device
    /// lacks, in one question and as many transfers. Returns (files, bytes,
    /// errors).
    pub async fn send_missing_to_device(&self, device_id: &str, device_url: &str, audiofile_directory: &std::path::Path) -> (i64, u64, Vec<String>) {
        let mut errors = Vec::new();
        let rows = match self.db.lock().unwrap().get_all_audio_files() {
            Ok(rows) => rows,
            Err(e) => return (0, 0, vec![format!("Failed to list recordings: {}", e)]),
        };
        let local: Vec<_> = rows
            .into_iter()
            .filter(|r| r.deleted_at.is_none())
            .map(|r| (r.id.clone(), audio_local_path(audiofile_directory, &r.disk_name)))
            .filter(|(_, path)| path.is_file())
            .collect();
        if local.is_empty() {
            return (0, 0, errors);
        }
        let missing = match self.missing_on_device(device_url, local.iter().map(|(id, _)| id.clone()).collect()).await {
            Ok(m) => m,
            Err(e) => return (0, 0, vec![e.to_string()]),
        };
        let mut sent = 0i64;
        let mut bytes = 0u64;
        let to_send: Vec<_> = local.into_iter().filter(|(id, _)| missing.missing.contains(id)).collect();
        let total = to_send.len() as i64;
        let mut failed_files = 0usize;
        for (index, (audio_id, path)) in to_send.into_iter().enumerate() {
            if self.cancelled() {
                errors.push(CANCELLED.to_string());
                break;
            }
            self.report("send", sent, total, bytes, format!("Sending recording {} of {}", sent + 1, total));
            let from = missing.partial.get(&audio_id).copied().unwrap_or(0);
            let what = format!("Send of {}", &audio_id[..UUID_SHORT_LEN.min(audio_id.len())]);
            let moved_before = bytes;
            // A try after a broken one continues from what the device holds now:
            // the offset asked before the first try is stale once bytes arrived (FILE-13)
            let first_try = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
            let attempt = || {
                let first_try = first_try.clone();
                let audio_id = audio_id.clone();
                let path = path.clone();
                async move {
                    let from = if first_try.swap(false, std::sync::atomic::Ordering::SeqCst) {
                        from
                    } else {
                        let now = self.missing_on_device(device_url, vec![audio_id.clone()]).await?;
                        if !now.missing.contains(&audio_id) {
                            // The broken try arrived whole after all
                            return Ok(0);
                        }
                        now.partial.get(&audio_id).copied().unwrap_or(0)
                    };
                    self.send_audio_file(device_url, &audio_id, &path, from, moved_before, sent, total).await
                }
            };
            match self.with_retries(&what, attempt).await {
                Ok(n) => {
                    sent += 1;
                    bytes += n;
                    self.note_copy(&audio_id, device_id);
                    self.report("send", sent, total, bytes, format!("Sent recording {} of {}", sent, total));
                }
                Err(e) if e.to_string().ends_with(CANCELLED) => {
                    errors.push(CANCELLED.to_string());
                    break;
                }
                Err(e) => {
                    errors.push(e.to_string());
                    failed_files += 1;
                    if let Some(sentence) = crate::transfer::stop_after_failures(failed_files, total as usize - index - 1, "send") {
                        errors.push(sentence);
                        break;
                    }
                }
            }
        }
        (sent, bytes, errors)
    }

    /// **Fetch** (terms): every recording the device holds that this device
    /// lacks. The rows say what exists; the device answers 404 for a file it
    /// does not hold, which is not an error. Returns (files, bytes, errors).
    pub async fn fetch_missing_from_device(&self, device_id: &str, device_url: &str, audiofile_directory: &std::path::Path) -> (i64, u64, Vec<String>) {
        let mut errors = Vec::new();
        let rows = match self.db.lock().unwrap().get_all_audio_files() {
            Ok(rows) => rows,
            Err(e) => return (0, 0, vec![format!("Failed to list recordings: {}", e)]),
        };
        let mut fetched = 0i64;
        let mut bytes = 0u64;
        let wanted: Vec<_> = rows
            .into_iter()
            .filter(|r| r.deleted_at.is_none() && !audio_local_path(audiofile_directory, &r.disk_name).is_file())
            .collect();
        let total = wanted.len() as i64;
        let mut failed_files = 0usize;
        for (index, row) in wanted.into_iter().enumerate() {
            if self.cancelled() {
                errors.push(CANCELLED.to_string());
                break;
            }
            // Room on disk for the file under its name (FILE-15)
            let path = match self.db.lock().unwrap().disk_path_for_writing(&row.id, audiofile_directory) {
                Ok(path) => path,
                Err(e) => {
                    errors.push(format!("No room for {}: {}", &row.id[..UUID_SHORT_LEN.min(row.id.len())], e));
                    continue;
                }
            };
            if path.is_file() {
                continue;
            }
            self.report("fetch", fetched, total, bytes, format!("Fetching recording {} of {}", fetched + 1, total));
            let what = format!("Fetch of {}", &row.id[..UUID_SHORT_LEN.min(row.id.len())]);
            let moved_before = bytes;
            match self.with_retries(&what, || self.fetch_audio_file(device_url, &row.id, &path, moved_before, fetched, total)).await {
                Ok(n) => {
                    fetched += 1;
                    bytes += n;
                    let here = self.config.lock().map(|c| c.this_device_id_hex().to_string()).unwrap_or_default();
                    if let Err(e) = self.db.lock().unwrap().set_file_location(&row.id, &here, true) {
                        tracing::warn!("Could not record that this device holds {}: {}", short_id(&row.id), e);
                    }
                    self.note_copy(&row.id, device_id);
                    self.report("fetch", fetched, total, bytes, format!("Fetched recording {} of {}", fetched, total));
                }
                Err(e) if e.to_string().ends_with(CANCELLED) => {
                    errors.push(CANCELLED.to_string());
                    break;
                }
                Err(e) if e.to_string().contains("HTTP 404") => {}
                Err(e) => {
                    errors.push(e.to_string());
                    failed_files += 1;
                    if let Some(sentence) = crate::transfer::stop_after_failures(failed_files, total as usize - index - 1, "fetch") {
                        errors.push(sentence);
                        break;
                    }
                }
            }
        }
        // The device learns what this device holds now (Stage 10): one
        // missing-list request, whose answer is not needed
        if fetched > 0 {
            let held: Vec<String> = match self.db.lock().unwrap().get_all_audio_files() {
                Ok(rows) => rows
                    .into_iter()
                    .filter(|r| r.deleted_at.is_none() && audio_local_path(audiofile_directory, &r.disk_name).is_file())
                    .map(|r| r.id)
                    .collect(),
                Err(_) => Vec::new(),
            };
            if !held.is_empty() {
                if let Err(e) = self.missing_on_device(device_url, held).await {
                    tracing::debug!("The other device was not told what this device holds: {}", e);
                }
            }
        }
        (fetched, bytes, errors)
    }

    /// The audio directory this device keeps recordings in.
    fn audio_directory(&self) -> Option<std::path::PathBuf> {
        self.config.lock().ok().and_then(|c| c.audiofile_directory().map(std::path::PathBuf::from))
    }

    fn device_url_of(&self, device_id: &str) -> Option<String> {
        self.config.lock().ok().and_then(|c| c.get_device(device_id).map(|p| p.device_url.clone()))
    }

    /// **Deliver**: sync, then send.
    pub async fn deliver(&self, device_id: &str) -> SyncResult {
        let mut result = self.sync_with_device(device_id).await;
        if !result.success {
            return result;
        }
        self.move_files(device_id, &mut result, true, false).await;
        self.record_operation(device_id, "deliver");
        result
    }

    /// **Exchange**: sync, then send and fetch.
    pub async fn exchange(&self, device_id: &str) -> SyncResult {
        let mut result = self.sync_with_device(device_id).await;
        if !result.success {
            return result;
        }
        self.move_files(device_id, &mut result, true, true).await;
        self.record_operation(device_id, "exchange");
        result
    }

    /// **Send** alone, or **fetch** alone, without a sync.
    pub async fn send_to_device(&self, device_id: &str) -> SyncResult {
        self.begin_operation();
        let mut result = SyncResult::success();
        self.move_files(device_id, &mut result, true, false).await;
        if result.success {
            self.record_operation(device_id, "send");
        }
        result
    }

    pub async fn fetch_from_device(&self, device_id: &str) -> SyncResult {
        self.begin_operation();
        let mut result = SyncResult::success();
        self.move_files(device_id, &mut result, false, true).await;
        if result.success {
            self.record_operation(device_id, "fetch");
        }
        result
    }

    /// A device holds a copy of a recording now (Stage 10).
    fn note_copy(&self, audio_id: &str, device_id: &str) {
        if let Err(e) = self.db.lock().unwrap().record_copy(audio_id, device_id) {
            tracing::warn!("Could not record that {} holds {}: {}", short_id(device_id), short_id(audio_id), e);
        }
    }

    /// The device's row remembers when it was last reached and by what, and
    /// the device becomes the one the visible button names (Stage 5).
    fn record_operation(&self, device_id: &str, operation: &str) {
        let (name, url) = {
            let config = self.config.lock().unwrap();
            config.get_device(device_id).map(|p| (p.device_name.clone(), p.device_url.clone())).unwrap_or_default()
        };
        if let Err(e) = self.db.lock().unwrap().set_device_last_operation(device_id, Some(&name), Some(&url), operation) {
            tracing::warn!("Could not record the {} with {}: {}", operation, short_id(device_id), e);
        }
        if let Err(e) = self.config.lock().unwrap().set_last_device(device_id) {
            tracing::warn!("Could not remember {} as the last device: {}", short_id(device_id), e);
        }
    }

    /// After a sync every card of the account is a device (Stage 5): a card
    /// without an entry gets one, with the card's name, its first address
    /// and its fingerprint; an entry re-pins its fingerprint from the card
    /// (which arrived over an authenticated connection); a revoked card's
    /// entry goes; a forgotten device stays forgotten. The remembered address
    /// is never replaced by the card's.
    pub fn adopt_devices_from_cards(&self) -> VoiceResult<usize> {
        let cards = self.db.lock().unwrap().list_device_cards()?;
        let mut config = self.config.lock().unwrap();
        let own = config.this_device_id_hex().to_string();
        let mut changed = 0;
        for card in cards {
            if card.device_id == own || config.is_forgotten(&card.device_id) {
                continue;
            }
            if card.revoked == "1" {
                if config.remove_device(&card.device_id)? {
                    changed += 1;
                }
                continue;
            }
            let first_address = serde_json::from_str::<Vec<String>>(&card.addresses).ok().and_then(|a| a.into_iter().next()).unwrap_or_default();
            let fingerprint = if card.certificate_fingerprint.is_empty() { None } else { Some(card.certificate_fingerprint.as_str()) };
            match config.get_device(&card.device_id).cloned() {
                None => {
                    let name = if card.name.is_empty() { short_id(&card.device_id).to_string() } else { card.name.clone() };
                    config.add_device(&card.device_id, &name, &first_address, fingerprint, false)?;
                    changed += 1;
                }
                Some(existing) => {
                    let url = if existing.device_url.is_empty() { first_address } else { existing.device_url.clone() };
                    let pin_changed = fingerprint.is_some() && existing.certificate_fingerprint.as_deref() != fingerprint;
                    if url != existing.device_url || pin_changed {
                        config.add_device(&card.device_id, &existing.device_name, &url, fingerprint, true)?;
                        changed += 1;
                    }
                }
            }
        }
        if changed > 0 {
            self.clients.lock().unwrap().clear();
        }
        Ok(changed)
    }

    async fn move_files(&self, device_id: &str, result: &mut SyncResult, send: bool, fetch: bool) {
        result.request_id = self.request_id();
        let Some(device_url) = self.device_url_of(device_id) else {
            result.errors.push(format!("Unknown device: {}", device_id));
            result.success = false;
            return;
        };
        let Some(dir) = self.audio_directory() else {
            result.errors.push("No audio directory is configured on this device".to_string());
            result.success = false;
            return;
        };
        if send {
            let (n, bytes, errors) = self.send_missing_to_device(device_id, &device_url, &dir).await;
            result.sent += n;
            result.bytes_moved += bytes;
            result.errors.extend(errors);
        }
        if fetch {
            let (n, bytes, errors) = self.fetch_missing_from_device(device_id, &device_url, &dir).await;
            result.fetched += n;
            result.bytes_moved += bytes;
            result.errors.extend(errors);
        }
        result.success = result.errors.is_empty();
    }

    /// Download every non-deleted audio file that is in cloud storage but not
    /// in `audiofile_directory`. No-op when storage is not configured.
    #[cfg(feature = "file-storage")]
    pub async fn download_missing_audio_files_from_cloud(
        &self,
        audiofile_directory: &std::path::Path,
    ) -> Result<crate::file_storage::DownloadMissingResult, crate::file_storage::FileStorageError> {
        let db = self.db.lock().map_err(|_| {
            crate::file_storage::FileStorageError::Config("Failed to lock database".to_string())
        })?;
        let key = self.config.lock().ok().and_then(|c| c.recording_key());
        let here = self.config.lock().map(|c| c.this_device_id_hex().to_string()).unwrap_or_default();
        let result = crate::file_storage::download_missing_audio_files(&db, audiofile_directory, key.as_ref(), &here).await?;
        if result.downloaded > 0 {
            tracing::info!("Downloaded {} audio files from cloud storage", result.downloaded);
        }
        Ok(result)
    }

    /// Download a single audio file on demand.
    #[cfg(feature = "file-storage")]
    pub async fn download_audio_file_from_cloud(
        &self,
        audiofile_directory: &std::path::Path,
        audio_file_id: &str,
    ) -> Result<crate::file_storage::DownloadOutcome, crate::file_storage::FileStorageError> {
        let db = self.db.lock().map_err(|_| {
            crate::file_storage::FileStorageError::Config("Failed to lock database".to_string())
        })?;
        let key = self.config.lock().ok().and_then(|c| c.recording_key());
        let here = self.config.lock().map(|c| c.this_device_id_hex().to_string()).unwrap_or_default();
        crate::file_storage::download_audio_file(&db, audiofile_directory, audio_file_id, key.as_ref(), &here).await
    }

    /// Download all missing audio files attached to a note on demand.
    #[cfg(feature = "file-storage")]
    pub async fn download_audio_files_for_note_from_cloud(
        &self,
        audiofile_directory: &std::path::Path,
        note_id: &str,
    ) -> Result<crate::file_storage::DownloadMissingResult, crate::file_storage::FileStorageError> {
        let db = self.db.lock().map_err(|_| {
            crate::file_storage::FileStorageError::Config("Failed to lock database".to_string())
        })?;
        let key = self.config.lock().ok().and_then(|c| c.recording_key());
        let here = self.config.lock().map(|c| c.this_device_id_hex().to_string()).unwrap_or_default();
        crate::file_storage::download_audio_files_for_note(&db, audiofile_directory, note_id, key.as_ref(), &here).await
    }

}

/// Sync with all configured devices
pub async fn sync_all_devices(
    db: Arc<Mutex<Database>>,
    config: Arc<Mutex<Config>>,
) -> HashMap<String, SyncResult> {
    let client = match SyncClient::new(db, config.clone()) {
        Ok(c) => c,
        Err(_) => return HashMap::new(),
    };

    let devices: Vec<String> = {
        let cfg = config.lock().unwrap();
        cfg.devices().iter().map(|p| p.device_id.clone()).collect()
    };

    let mut results = HashMap::new();
    for device_id in devices {
        let result = client.sync_with_device(&device_id).await;
        results.insert(device_id, result);
    }

    results
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_device_without_an_address_is_refused_in_words() {
        for empty in ["", "   "] {
            let text = super::check_scheme(empty).unwrap_err().to_string();
            assert!(text.contains("no address is known for this device yet"), "{}", text);
            assert!(text.contains(crate::sync_protocol::codes::NO_ADDRESS), "{}", text);
            assert!(!text.contains("is not a URL"), "{}", text);
        }
    }

    #[test]
    fn a_malformed_address_is_still_named_as_not_a_url() {
        let text = super::check_scheme("not a url").unwrap_err().to_string();
        assert!(text.contains("is not a URL"), "{}", text);
    }

    use super::*;
    use tempfile::TempDir;

    #[test]
    fn a_clock_difference_is_a_sentence_only_past_a_minute() {
        assert_eq!(clock_skew_sentence(59, "Desk"), None);
        assert_eq!(clock_skew_sentence(-60, "Desk"), None);
        assert_eq!(clock_skew_sentence(250, "Desk").unwrap(), "This device's clock is 4 minutes behind Desk's");
        assert_eq!(clock_skew_sentence(-61, "Desk").unwrap(), "This device's clock is 1 minute ahead of Desk's");
    }

    #[test]
    fn a_request_id_is_sixteen_hex_characters_and_never_repeats() {
        let a = new_request_id();
        let b = new_request_id();
        assert_eq!(a.len(), 16);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b);
    }

    fn create_test_db_and_config() -> (Arc<Mutex<Database>>, Arc<Mutex<Config>>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = Database::new(&db_path).unwrap();
        let config = Config::new(Some(temp_dir.path().to_path_buf()), None).unwrap();

        (
            Arc::new(Mutex::new(db)),
            Arc::new(Mutex::new(config)),
            temp_dir,
        )
    }

    mod sync_result_tests {
        use super::*;

        #[test]
        fn test_sync_result_success_default() {
            let result = SyncResult::success();
            assert!(result.success);
            assert_eq!(result.pulled, 0);
            assert_eq!(result.pushed, 0);
            assert_eq!(result.conflicts, 0);
            assert!(result.errors.is_empty());
        }

        #[test]
        fn test_sync_result_failure() {
            let result = SyncResult::failure("Test error".to_string());
            assert!(!result.success);
            assert_eq!(result.errors.len(), 1);
            assert_eq!(result.errors[0], "Test error");
        }

        #[test]
        fn test_sync_result_with_counts() {
            let mut result = SyncResult::success();
            result.pulled = 10;
            result.pushed = 5;
            result.conflicts = 2;

            assert!(result.success);
            assert_eq!(result.pulled, 10);
            assert_eq!(result.pushed, 5);
            assert_eq!(result.conflicts, 2);
        }

        #[test]
        fn test_sync_result_with_multiple_errors() {
            let mut result = SyncResult::failure("Error 1".to_string());
            result.errors.push("Error 2".to_string());
            result.errors.push("Error 3".to_string());

            assert!(!result.success);
            assert_eq!(result.errors.len(), 3);
        }
    }

    mod sync_client_init_tests {
        use super::*;

        #[test]
        fn test_client_creation() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config);
            assert!(client.is_ok());
        }

        #[test]
        fn test_client_has_device_info() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config).unwrap();

            // Device ID should be a valid hex string (32 chars = 16 bytes as hex)
            assert!(!client.this_device_id.is_empty());
            // Device name should be set
            assert!(!client.this_device_name.is_empty());
        }
    }

    mod sync_with_unknown_device_tests {
        use super::*;

        #[tokio::test]
        async fn test_sync_with_unknown_device() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config).unwrap();

            let result = client.sync_with_device("00000000000070008000000000000099").await;
            assert!(!result.success);
            assert!(result.errors.iter().any(|e| e.contains("Unknown device")));
        }

        #[tokio::test]
        async fn test_pull_from_unknown_device() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config).unwrap();

            let result = client.pull_from_device("00000000000070008000000000000099").await;
            assert!(!result.success);
            assert!(result.errors.iter().any(|e| e.contains("Unknown device")));
        }

        #[tokio::test]
        async fn test_push_to_unknown_device() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config).unwrap();

            let result = client.push_to_device("00000000000070008000000000000099").await;
            assert!(!result.success);
            assert!(result.errors.iter().any(|e| e.contains("Unknown device")));
        }

        #[tokio::test]
        async fn test_initial_sync_unknown_device() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config).unwrap();

            let result = client.initial_sync("00000000000070008000000000000099").await;
            assert!(!result.success);
            assert!(result.errors.iter().any(|e| e.contains("Unknown device")));
        }

        #[tokio::test]
        async fn test_check_device_status_unknown_device() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config).unwrap();

            let result = client.check_device_status("00000000000070008000000000000099").await;
            // Should return None or error for unknown device
            assert!(result.is_empty() || result.get("error").is_some());
        }
    }

    mod sync_all_devices_tests {
        use super::*;

        #[tokio::test]
        async fn test_sync_all_devices_empty() {
            let (db, config, _temp_dir) = create_test_db_and_config();

            let results = sync_all_devices(db, config).await;
            assert!(results.is_empty());
        }
    }

}
