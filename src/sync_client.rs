//! Sync client for Voice peer-to-peer synchronization.
//!
//! This module provides the client side of the sync protocol, allowing
//! this device to:
//! - Connect to peer sync servers
//! - Pull changes from peers
//! - Push local changes to peers
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
    /// Recordings sent to the peer in this operation (deliver, exchange)
    pub sent: i64,
    /// Recordings fetched from the peer in this operation (exchange, fetch)
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
    /// The peer's clock minus this device's, in seconds, when the two
    /// differ by more than a minute; 0 otherwise.
    pub clock_skew_seconds: i64,
}

/// A difference of clocks below this is not reported.
pub const CLOCK_SKEW_REPORTED_SECONDS: i64 = 60;

/// The sentence for a clock difference, or None below the threshold.
pub fn clock_skew_sentence(skew_seconds: i64, peer_name: &str) -> Option<String> {
    if skew_seconds.abs() <= CLOCK_SKEW_REPORTED_SECONDS {
        return None;
    }
    let minutes = (skew_seconds.abs() + 30) / 60;
    let relation = if skew_seconds > 0 { "behind" } else { "ahead of" };
    Some(format!("This device's clock is {} minute{} {} {}'s", minutes, if minutes == 1 { "" } else { "s" }, relation, peer_name))
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

/// Information about a sync peer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub peer_id: String,
    pub peer_name: String,
    pub peer_url: String,
    pub certificate_fingerprint: Option<String>,
    pub last_sync_at: Option<String>,
}


/// Page size for the cursor feed, in both directions. Pages are fetched
/// until the peer reports the feed complete, so this only bounds one request.
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

/// Cursor state for one peer, as stored in `sync_peers`.
#[derive(Debug, Clone, Default)]
struct PeerCursors {
    /// Our position in the peer's feed
    received: i64,
    /// Our own `seq` up to which the peer has everything
    sent: i64,
}

/// Sync client
pub struct SyncClient {
    db: Arc<Mutex<Database>>,
    config: Arc<Mutex<Config>>,
    /// One verified client per (peer URL, pinned fingerprint)
    clients: Mutex<HashMap<String, Client>>,
    device_id: String,
    device_name: String,
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
    /// it and this device's card, then add it as a peer.
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
            config.add_peer(&reply.device_id, &reply.device_name, &url, pin, true)?;
            config.set_sync_enabled(true)?;
        }
        self.clients.lock().unwrap().clear();
        Ok(Joined { account_id, peer_id: reply.device_id, peer_name: reply.device_name, peer_url: url })
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
    pub peer_id: String,
    pub peer_name: String,
    pub peer_url: String,
}

/// Plain http is accepted only to this machine itself (AUTH-7).
fn check_scheme(peer_url: &str) -> VoiceResult<()> {
    let url = reqwest::Url::parse(peer_url)
        .map_err(|e| VoiceError::Network(format!("{} is not a URL: {}", peer_url, e)))?;
    let host = url.host_str().unwrap_or("");
    let loopback = matches!(host, "localhost" | "127.0.0.1" | "[::1]" | "::1") || host.starts_with("127.");
    if url.scheme() == "http" && !loopback {
        return Err(VoiceError::Network(format!(
            "{} is plain http; a device key must not cross a network in clear ({})",
            peer_url,
            codes::TLS_REQUIRED
        )));
    }
    Ok(())
}

/// A client verified by `pin` when there is one, by the system roots when
/// there is none. Verification is never off.
/// Three seconds to connect on this machine or the LAN, ten on the internet
/// (FILE-14); a private address is one of RFC 1918's or a loopback.
fn connect_timeout_for(peer_url: &str) -> Duration {
    let host = reqwest::Url::parse(peer_url).ok().and_then(|u| u.host_str().map(str::to_string)).unwrap_or_default();
    let near = host == "localhost"
        || host.parse::<std::net::IpAddr>().map(|ip| match ip {
            std::net::IpAddr::V4(v4) => v4.is_loopback() || v4.is_private() || v4.is_link_local(),
            std::net::IpAddr::V6(v6) => v6.is_loopback(),
        }).unwrap_or(false);
    Duration::from_secs(if near { 3 } else { 10 })
}

fn build_client(pin: &str, peer_url: &str) -> VoiceResult<Client> {
    // A page can be a few megabytes over a slow link
    let builder = Client::builder()
        .connect_timeout(connect_timeout_for(peer_url))
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
        let (device_id, device_name) = {
            let cfg = config.lock().unwrap();
            (cfg.device_id_hex().to_string(), cfg.device_name().to_string())
        };

        Ok(Self {
            db,
            config,
            clients: Mutex::new(HashMap::new()),
            device_id,
            device_name,
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

    /// The HTTP client for a peer's URL (AUTH-7): plain http only to this
    /// machine itself; https verified against the fingerprint pinned for the
    /// peer when there is one, against the system's root certificates when
    /// there is none. Verification is never off. Clients are kept per
    /// (URL, pin) so a changed pin builds a new one.
    fn client_for(&self, peer_url: &str) -> VoiceResult<Client> {
        check_scheme(peer_url)?;
        let pin = {
            let config = self.config.lock().unwrap();
            config
                .peers()
                .iter()
                .find(|p| p.peer_url.trim_end_matches('/') == peer_url.trim_end_matches('/'))
                .and_then(|p| p.certificate_fingerprint.clone())
                .unwrap_or_default()
        };
        let cache_key = format!("{}\n{}", peer_url, pin);
        if let Some(client) = self.clients.lock().unwrap().get(&cache_key) {
            return Ok(client.clone());
        }
        let client = build_client(&pin, peer_url)?;
        self.clients.lock().unwrap().insert(cache_key, client.clone());
        Ok(client)
    }

    /// Join an account from a setup text (PAIR-4): refuse if this device
    /// holds another account's notes, then present the token to the showing
    /// device over TLS pinned to the text's fingerprint, take the account id
    /// and the key it issues, and add the showing device as a peer.
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
    /// account, the account id is rewritten and every peer forgotten, the
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
        let pulled = self.pull_from_peer(&joined.peer_id).await;
        if !pulled.success {
            return Err(VoiceError::Sync(format!("Moved, but the other account could not be read: {}", pulled.errors.join("; "))));
        }
        let merged = self.db.lock().unwrap().merge_duplicate_tag_paths()?;
        let exchanged = self.sync_with_peer(&joined.peer_id).await;
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
            device_id: self.device_id.clone(),
            device_name: self.device_name.clone(),
            certificate_fingerprint: own_fingerprint,
            addresses: String::new(),
            application: crate::auth::APPLICATION_VOICE.to_string(),
        };
        let client = build_client(&setup.certificate_fingerprint, &setup.urls[0])?;
        let mut last_error = String::new();
        let mut reply: Option<(String, crate::sync_protocol::PairClaimResponse)> = None;
        for url in &setup.urls {
            check_scheme(url)?;
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
            let pin = if setup.certificate_fingerprint.is_empty() { None } else { Some(setup.certificate_fingerprint.as_str()) };
            config.add_peer(&reply.device_id, &reply.device_name, &url, pin, true)?;
            config.set_sync_enabled(true)?;
            crate::auth::ensure_own_device_card(&db, &mut config)?;
        }
        self.clients.lock().unwrap().clear();
        Ok(Joined { account_id: reply.account_id, peer_id: reply.device_id, peer_name: reply.device_name, peer_url: url })
    }

    /// The three headers every request carries (AUTH-3): the account, the
    /// device, and the device's key as a bearer token.
    fn authed(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let key = self.config.lock().map(|c| c.device_key().to_string()).unwrap_or_default();
        request
            .header(crate::auth::HEADER_ACCOUNT, self.account_id())
            .header(crate::auth::HEADER_DEVICE, &self.device_id)
            .header("X-Device-Name", &self.device_name)
            .header(crate::sync_protocol::HEADER_REQUEST_ID, self.request_id())
            .bearer_auth(key)
    }

    /// Sync with a peer: exchange database changes both ways. Files never
    /// move here; see `deliver` and `exchange`.
    /// One operation, one request id.
    pub async fn sync_with_peer(&self, peer_id: &str) -> SyncResult {
        self.begin_operation();
        self.sync_within_operation(peer_id).await
    }

    /// The sync itself, under the request id of the operation under way
    /// (a deliver or an exchange begins one and then moves files under it).
    async fn sync_within_operation(&self, peer_id: &str) -> SyncResult {
        let peer = {
            let config = self.config.lock().unwrap();
            config.get_peer(peer_id).cloned()
        };

        let peer = match peer {
            Some(p) => p,
            None => return self.failed(format!("Unknown peer: {}", peer_id)),
        };

        let peer_url = &peer.peer_url;
        let mut result = SyncResult::success();
        result.request_id = self.request_id();

        // Step 1: Handshake, and find where we stand with this peer
        let handshake = match self.handshake(peer_url).await {
            Ok(h) => h,
            Err(e) => return self.failed(format!("Handshake failed: {}", e)),
        };
        if let Err(sentence) = self.check_account(peer_id, &handshake) {
            return self.failed(sentence);
        }
        if handshake.server_timestamp != 0 {
            let skew = handshake.server_timestamp - Utc::now().timestamp();
            if let Some(sentence) = clock_skew_sentence(skew, &peer.peer_name) {
                result.clock_skew_seconds = skew;
                result.warnings.push(sentence);
            }
        }
        let cursors = self.peer_cursors(peer_id, &handshake, &mut result);

        // Everything written locally up to here is what this sync pushes;
        // whatever the pull writes is the peer's own data coming back.
        let local_end = self.local_seq();

        // Step 2: Pull, page by page, saving the cursor after every page
        self.snapshot_before("sync", &mut result);
        let pull = self.pull_all(peer_url, peer_id, &peer.peer_name, cursors.received).await;
        result.pulled = pull.applied;
        result.conflicts += pull.conflicts;
        result.errors.extend(pull.errors);
        result.warnings.extend(pull.warnings);

        // Step 3: Push our changes the peer has not seen, page by page
        let (pushed, conflicts, errors, warnings) = self.push_all(peer_url, peer_id, cursors.sent, local_end).await;
        result.pushed = pushed;
        result.conflicts += conflicts;
        result.errors.extend(errors);
        result.warnings.extend(warnings);

        // Update last sync time
        if let Err(e) = self.update_peer_sync_time(peer_id) {
            result.errors.push(format!("Failed to update sync time: {}", e));
        }
        if let Err(e) = self.config.lock().unwrap().set_last_peer(peer_id) {
            result.warnings.push(format!("Could not remember the last peer: {}", e));
        }
        // The cards that arrived are peers now (Stage 5)
        if let Err(e) = self.adopt_peers_from_cards() {
            result.warnings.push(format!("The peer list could not be read from the cards: {}", e));
        }

        result.success = result.errors.is_empty();
        result
    }

    /// **Check the connection** to a peer (Stage 12): one row per thing
    /// that can be wrong, each with its refusal code, instead of a log
    /// search. Nothing is changed by a check.
    pub async fn check(&self, peer_id: &str) -> Vec<crate::sync_protocol::CheckRow> {
        use crate::sync_protocol::CheckRow;
        fn row(name: &str, passed: bool, detail: impl Into<String>, code: &str) -> CheckRow {
            CheckRow { name: name.to_string(), passed, detail: detail.into(), code: code.to_string() }
        }
        self.begin_operation();
        let mut rows = Vec::new();
        let peer = {
            let config = self.config.lock().unwrap();
            config.get_peer(peer_id).cloned()
        };
        let Some(peer) = peer else {
            rows.push(row("Peer", false, format!("No peer {} is remembered on this device", short_id(peer_id)), ""));
            return rows;
        };
        let pinned = {
            let config = self.config.lock().unwrap();
            config.get_peer(peer_id).and_then(|p| p.certificate_fingerprint.clone()).unwrap_or_default()
        };
        let https = peer.peer_url.starts_with("https://");

        // 1. Reachable, and the certificate: one request without a key
        let client = match self.client_for(&peer.peer_url) {
            Ok(c) => c,
            Err(e) => {
                rows.push(row("Reachable", false, e.to_string(), codes::TLS_REQUIRED));
                return rows;
            }
        };
        match client.get(format!("{}/sync/status", peer.peer_url.trim_end_matches('/'))).timeout(Duration::from_secs(10)).send().await {
            Ok(response) if response.status().is_success() => {
                let body: serde_json::Value = response.json().await.unwrap_or(serde_json::Value::Null);
                let name = body.get("device_name").and_then(|v| v.as_str()).unwrap_or("?");
                let id = body.get("device_id").and_then(|v| v.as_str()).unwrap_or("");
                rows.push(row("Reachable", true, format!("{} answers at {}", name, peer.peer_url), ""));
                if !id.is_empty() && id != peer_id {
                    rows.push(row("Device", false, format!("The device at {} is {}, not {}", peer.peer_url, short_id(id), short_id(peer_id)), codes::DEVICE_MISMATCH));
                }
                if https {
                    rows.push(row("Certificate", true, if pinned.is_empty() { "Verified by the system's root certificates".to_string() } else { "The pinned fingerprint matches".to_string() }, ""));
                } else {
                    rows.push(row("Certificate", true, "Plain http on this machine itself; no certificate", ""));
                }
            }
            Ok(response) => {
                rows.push(row("Reachable", false, format!("{} answered with status {}", peer.peer_url, response.status()), ""));
                return rows;
            }
            Err(e) => {
                let sentence = describe(&e);
                let certificate = sentence.to_lowercase().contains("certificate") || sentence.to_lowercase().contains("fingerprint");
                if certificate {
                    rows.push(row("Reachable", true, format!("Something answers at {}", peer.peer_url), ""));
                    rows.push(row("Certificate", false, sentence, codes::CERTIFICATE_MISMATCH));
                } else {
                    rows.push(row("Reachable", false, format!("{} does not answer: {}", peer.peer_url, sentence), ""));
                }
                return rows;
            }
        }

        // 2. Account and key: the handshake, which is refused with a code
        match self.handshake(&peer.peer_url).await {
            Ok(handshake) => {
                match Self::check_protocol(&peer.peer_name, &handshake) {
                    Ok(()) => rows.push(row("Protocol", true, format!("Version {}", handshake.protocol_version), "")),
                    Err(sentence) => rows.push(row("Protocol", false, sentence, codes::PROTOCOL_TOO_OLD)),
                }
                let own = self.account_id();
                if handshake.account_id == own {
                    rows.push(row("Account", true, format!("The peer holds account {}", short_id(&own)), ""));
                } else {
                    rows.push(row("Account", false, format!("The peer holds account {}, this device {}", short_id(&handshake.account_id), short_id(&own)), codes::ACCOUNT_MISMATCH));
                }
                rows.push(row("Key", true, "This device's key is accepted", ""));
                if handshake.server_timestamp != 0 {
                    let skew = handshake.server_timestamp - Utc::now().timestamp();
                    match clock_skew_sentence(skew, &peer.peer_name) {
                        Some(sentence) => rows.push(row("Clock", false, sentence, "")),
                        None => rows.push(row("Clock", true, "The clocks agree to within a minute", "")),
                    }
                }
                if handshake.free_bytes > 0 {
                    let low = handshake.free_bytes < crate::transfer::FREE_SPACE_MARGIN;
                    rows.push(row("Free space there", !low, format!("{} MB free on {}", handshake.free_bytes / (1024 * 1024), peer.peer_name), ""));
                }
                rows.push(row("Recordings there", true, if handshake.supports_audiofiles { "The peer serves recordings" } else { "The peer serves notes only; no audio directory is configured there" }, ""));
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
        rows.push(row("Listener here", true, if crate::sync_server::server_running() { "This device is listening" } else { "This device is not listening; the peer cannot start an operation towards it" }, ""));
        rows
    }

    /// A failure that carries the id of the operation under way.
    fn failed(&self, sentence: String) -> SyncResult {
        let mut result = SyncResult::failure(sentence);
        result.request_id = self.request_id();
        result
    }

    /// Pull changes from a peer (one-way)
    pub async fn pull_from_peer(&self, peer_id: &str) -> SyncResult {
        self.begin_operation();
        let peer = {
            let config = self.config.lock().unwrap();
            config.get_peer(peer_id).cloned()
        };

        let peer = match peer {
            Some(p) => p,
            None => return self.failed(format!("Unknown peer: {}", peer_id)),
        };

        let peer_url = &peer.peer_url;
        let mut result = SyncResult::success();
        result.request_id = self.request_id();

        let handshake = match self.handshake(peer_url).await {
            Ok(h) => h,
            Err(e) => return self.failed(format!("Handshake failed: {}", e)),
        };
        if let Err(sentence) = self.check_account(peer_id, &handshake) {
            return self.failed(sentence);
        }
        let cursors = self.peer_cursors(peer_id, &handshake, &mut result);

        self.snapshot_before("pull", &mut result);
        let pull = self.pull_all(peer_url, peer_id, &peer.peer_name, cursors.received).await;
        result.pulled = pull.applied;
        result.conflicts = pull.conflicts;
        result.errors.extend(pull.errors);
        result.warnings.extend(pull.warnings);

        if result.errors.is_empty() {
            if let Err(e) = self.update_peer_sync_time(peer_id) {
                result.errors.push(format!("Failed to update sync time: {}", e));
            }
        }

        result.success = result.errors.is_empty();
        result
    }

    /// Push changes to a peer (one-way)
    pub async fn push_to_peer(&self, peer_id: &str) -> SyncResult {
        self.begin_operation();
        let peer = {
            let config = self.config.lock().unwrap();
            config.get_peer(peer_id).cloned()
        };

        let peer = match peer {
            Some(p) => p,
            None => return self.failed(format!("Unknown peer: {}", peer_id)),
        };

        let peer_url = &peer.peer_url;
        let mut result = SyncResult::success();
        result.request_id = self.request_id();

        let handshake = match self.handshake(peer_url).await {
            Ok(h) => h,
            Err(e) => return self.failed(format!("Handshake failed: {}", e)),
        };
        if let Err(sentence) = self.check_account(peer_id, &handshake) {
            return self.failed(sentence);
        }
        let cursors = self.peer_cursors(peer_id, &handshake, &mut result);
        let local_end = self.local_seq();

        let (pushed, conflicts, errors, warnings) = self.push_all(peer_url, peer_id, cursors.sent, local_end).await;
        result.pushed = pushed;
        result.conflicts = conflicts;
        result.errors.extend(errors);
        result.warnings.extend(warnings);

        if result.errors.is_empty() {
            if let Err(e) = self.update_peer_sync_time(peer_id) {
                result.errors.push(format!("Failed to update sync time: {}", e));
            }
        }

        result.success = result.errors.is_empty();
        result
    }

    /// Perform initial sync with a new peer (full dataset transfer)
    ///
    /// This is used for first-time sync when we need to get the complete
    /// dataset from a peer rather than incremental changes. Afterwards the
    /// cursors point at the end of both feeds, so the next sync is incremental.
    pub async fn initial_sync(&self, peer_id: &str) -> SyncResult {
        self.begin_operation();
        let peer = {
            let config = self.config.lock().unwrap();
            config.get_peer(peer_id).cloned()
        };

        let peer = match peer {
            Some(p) => p,
            None => return self.failed(format!("Unknown peer: {}", peer_id)),
        };

        let peer_url = &peer.peer_url;
        let mut result = SyncResult::success();
        result.request_id = self.request_id();

        // Step 1: Handshake
        let handshake = match self.handshake(peer_url).await {
            Ok(h) => h,
            Err(e) => return self.failed(format!("Handshake failed: {}", e)),
        };
        if let Err(sentence) = self.check_account(peer_id, &handshake) {
            return self.failed(sentence);
        }

        // Step 2: Pull the peer's whole feed from the beginning, page by
        // page. (One JSON document for the whole dataset, as /sync/full
        // returns, does not fit in memory for a large database; the paged
        // feed is resumable and bounded.)
        if let Err(e) = self.save_peer_cursors(peer_id, Some(0), Some(0), Some(handshake.database_id.as_str())) {
            result.errors.push(format!("Failed to reset cursors: {}", e));
        }
        self.snapshot_before("initial sync", &mut result);
        let pull = self.pull_all(peer_url, peer_id, &peer.peer_name, 0).await;
        result.pulled = pull.applied;
        result.conflicts = pull.conflicts;
        result.errors.extend(pull.errors);
        result.warnings.extend(pull.warnings);

        // Step 6: Push everything we have (the peer de-duplicates)
        let local_end = self.local_seq();
        let (pushed, conflicts, errors, warnings) = self.push_all(peer_url, peer_id, 0, local_end).await;
        result.pushed = pushed;
        result.conflicts += conflicts;
        result.errors.extend(errors);
        result.warnings.extend(warnings);

        // Step 7: Update sync timestamp
        if let Err(e) = self.update_peer_sync_time(peer_id) {
            result.errors.push(format!("Failed to update sync time: {}", e));
        }

        result.success = result.errors.is_empty();
        result
    }

    /// Where we stand with a peer. If the peer's database identity changed
    /// (it was reset or replaced) both cursors restart from zero: everything
    /// is exchanged again, which is safe because applying is idempotent.
    /// The account this database belongs to.
    fn account_id(&self) -> String {
        self.db.lock().ok().and_then(|db| db.account_id().ok()).unwrap_or_default()
    }

    /// The account check on the caller's side (ACCT-3): the peer must hold
    /// the same account as this database, or nothing is exchanged. Returns
    /// the sentence to refuse with. Never adopts.
    /// A responder of an older protocol is refused, in words (Stage 16).
    fn check_protocol(peer_name: &str, handshake: &HandshakeResponse) -> Result<(), String> {
        if crate::sync_protocol::protocol_major(&handshake.protocol_version).unwrap_or(0) < crate::sync_protocol::PROTOCOL_MAJOR {
            return Err(format!("Update Voice on {} ({})", peer_name, codes::PROTOCOL_TOO_OLD));
        }
        Ok(())
    }

    fn check_account(&self, peer_id: &str, handshake: &HandshakeResponse) -> Result<(), String> {
        if let Err(sentence) = Self::check_protocol(&self.config.lock().map(|c| c.get_peer(peer_id).map(|p| p.peer_name.clone()).unwrap_or_default()).unwrap_or_default(), handshake) {
            return Err(sentence);
        }
        let own = self.account_id();
        if handshake.account_id.is_empty() {
            return Err(format!("The peer named no account ({})", codes::ACCOUNT_MISSING));
        }
        if handshake.account_id != own {
            let before = self
                .db
                .lock()
                .ok()
                .and_then(|db| db.get_peer_account_id(peer_id).ok().flatten());
            let mut sentence = format!(
                "The peer holds account {}; this device holds {}; nothing was exchanged",
                &handshake.account_id[..UUID_SHORT_LEN.min(handshake.account_id.len())],
                &own[..UUID_SHORT_LEN.min(own.len())]
            );
            if let Some(before) = before {
                if before == own {
                    sentence.push_str(". This peer used to hold this account; the device at its address has changed");
                }
            }
            sentence.push_str(&format!(" ({})", codes::ACCOUNT_MISMATCH));
            tracing::warn!("{}", sentence);
            return Err(sentence);
        }
        if let Ok(db) = self.db.lock() {
            let _ = db.set_peer_account_id(peer_id, Some(&handshake.device_name), &handshake.account_id);
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

    fn peer_cursors(&self, peer_id: &str, handshake: &HandshakeResponse, result: &mut SyncResult) -> PeerCursors {
        let (received, sent, known_db) = {
            let db = self.db.lock().unwrap();
            db.get_peer_cursors(peer_id).unwrap_or((0, 0, None))
        };
        let now = handshake.database_id.as_str();
        match &known_db {
            Some(before) if !now.is_empty() && now != before => {
                let msg = format!(
                    "Peer {} has a new database ({} -> {}); exchanging everything again",
                    &peer_id[..UUID_SHORT_LEN.min(peer_id.len())],
                    &before[..UUID_SHORT_LEN.min(before.len())],
                    &now[..UUID_SHORT_LEN.min(now.len())]
                );
                tracing::warn!("{}", msg);
                result.warnings.push(msg);
                let _ = self.save_peer_cursors(peer_id, Some(0), Some(0), Some(now));
                PeerCursors { received: 0, sent: 0 }
            }
            None if !now.is_empty() => {
                let _ = self.save_peer_cursors(peer_id, None, None, Some(now));
                PeerCursors { received, sent }
            }
            _ => PeerCursors { received, sent },
        }
    }

    fn local_seq(&self) -> i64 {
        self.db.lock().ok().and_then(|db| db.current_seq().ok()).unwrap_or(0)
    }

    fn save_peer_cursors(&self, peer_id: &str, received: Option<i64>, sent: Option<i64>, database_id: Option<&str>) -> VoiceResult<()> {
        let peer_name = {
            let config = self.config.lock().unwrap();
            config.get_peer(peer_id).map(|p| p.peer_name.clone())
        };
        let db = self.db.lock().unwrap();
        db.set_peer_cursors(peer_id, peer_name.as_deref(), received, sent, database_id)
    }

    /// Pull every page after `cursor`, applying each and saving the cursor
    /// before fetching the next, so an interrupted sync resumes where it stopped.
    async fn pull_all(&self, peer_url: &str, peer_id: &str, peer_name: &str, mut cursor: i64) -> PullOutcome {
        let mut outcome = PullOutcome { applied: 0, conflicts: 0, changes: Vec::new(), errors: Vec::new(), warnings: Vec::new() };
        for page in 0..MAX_PAGES {
            if self.cancelled() {
                outcome.errors.push(CANCELLED.to_string());
                break;
            }
            match self.pull_page(peer_url, peer_id, peer_name, cursor).await {
                Ok((pull, next_cursor, complete)) => {
                    outcome.applied += pull.applied;
                    outcome.conflicts += pull.conflicts;
                    outcome.changes.extend(pull.changes);
                    outcome.errors.extend(pull.errors);
                    outcome.warnings.extend(pull.warnings);
                    cursor = next_cursor;
                    self.report("sync", outcome.applied, 0, 0, format!("Received {} changes from {}", outcome.applied, peer_name));
                    if let Err(e) = self.save_peer_cursors(peer_id, Some(cursor), None, None) {
                        outcome.errors.push(format!("Failed to save cursor: {}", e));
                        break;
                    }
                    if complete {
                        // Anything queued for retry (a row that arrived before
                        // the row it references) gets one more chance now,
                        // instead of waiting for the next sync.
                        if self.db.lock().map(|db| db.count_pending_sync_failures().unwrap_or(0)).unwrap_or(0) > 0 {
                            if let Ok((applied, conflicts, _)) = self.apply_changes_from(&[], peer_id, Some(peer_name)) {
                                outcome.applied += applied;
                                outcome.conflicts += conflicts;
                            }
                        }
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
    /// high-water mark after every page the peer accepted.
    async fn push_all(&self, peer_url: &str, peer_id: &str, mut sent: i64, upto: i64) -> (i64, i64, Vec<String>, Vec<String>) {
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
            tracing::debug!("Pushing {} changes to {}", changes.len(), &peer_id[..UUID_SHORT_LEN.min(peer_id.len())]);
            match self.push_changes_with_data(peer_url, &changes).await {
                Ok((applied, page_conflicts, server_errors)) => {
                    pushed += applied;
                    conflicts += page_conflicts;
                    server_queued |= !server_errors.is_empty();
                    warnings.extend(server_errors.into_iter().map(|e| format!("Server queued for retry: {}", e)));
                    sent = next;
                    if let Err(e) = self.save_peer_cursors(peer_id, None, Some(sent), None) {
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
            match self.push_changes_with_data_allow_empty(peer_url).await {
                Ok((applied, page_conflicts, still_failing)) => {
                    pushed += applied;
                    conflicts += page_conflicts;
                    if !still_failing.is_empty() {
                        warnings.push(format!("{} change(s) still queued on the peer", still_failing.len()));
                    }
                }
                Err(e) => warnings.push(format!("Could not ask the peer to retry queued changes: {}", e)),
            }
        }
        (pushed, conflicts, errors, warnings)
    }

    /// Post an empty batch: the peer retries whatever it queued.
    async fn push_changes_with_data_allow_empty(&self, peer_url: &str) -> VoiceResult<(i64, i64, Vec<String>)> {
        let request = ApplyRequest {
            device_id: self.device_id.clone(),
            device_name: self.device_name.clone(),
            changes: Vec::new(),
        };
        let response = self
            .authed(self.client_for(peer_url)?.post(format!("{}/sync/apply", peer_url)))
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
                    c.device_id = self.device_id.clone();
                }
                if c.device_name.is_none() {
                    c.device_name = Some(self.device_name.clone());
                }
                c
            })
            .collect()
    }

    /// Check if a peer is reachable
    pub async fn check_peer_status(&self, peer_id: &str) -> HashMap<String, serde_json::Value> {
        let peer = {
            let config = self.config.lock().unwrap();
            config.get_peer(peer_id).cloned()
        };

        let peer = match peer {
            Some(p) => p,
            None => {
                let mut result = HashMap::new();
                result.insert("reachable".to_string(), serde_json::Value::Bool(false));
                result.insert(
                    "error".to_string(),
                    serde_json::Value::String("Unknown peer".to_string()),
                );
                return result;
            }
        };

        let client = match self.client_for(&peer.peer_url) {
            Ok(c) => c,
            Err(e) => {
                let mut result = HashMap::new();
                result.insert("reachable".to_string(), serde_json::Value::Bool(false));
                result.insert("error".to_string(), serde_json::Value::String(e.to_string()));
                return result;
            }
        };
        match client
            .get(format!("{}/sync/status", peer.peer_url))
            .send()
            .await
        {
            Ok(response) => {
                if response.status().is_success() {
                    match response.json::<serde_json::Value>().await {
                        Ok(data) => {
                            let mut result = HashMap::new();
                            result.insert("reachable".to_string(), serde_json::Value::Bool(true));
                            if let Some(device_id) = data.get("device_id") {
                                result.insert("device_id".to_string(), device_id.clone());
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

    async fn handshake(&self, peer_url: &str) -> VoiceResult<HandshakeResponse> {
        let request = HandshakeRequest {
            device_id: self.device_id.clone(),
            device_name: self.device_name.clone(),
            protocol_version: PROTOCOL_VERSION.to_string(),
            account_id: self.account_id(),
            application: crate::auth::APPLICATION_VOICE.to_string(),
            // Voice wants every type; an image application would declare ["tag"]
            entity_types: Vec::new(),
        };

        let response = self
            .authed(self.client_for(peer_url)?.post(format!("{}/sync/handshake", peer_url)))
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

    /// One page of the peer's feed after `cursor`. Returns what was applied,
    /// the cursor to continue from, and whether the feed is exhausted.
    async fn pull_page(&self, peer_url: &str, peer_id: &str, peer_name: &str, cursor: i64) -> VoiceResult<(PullOutcome, i64, bool)> {
        let url = format!("{}/sync/changes?cursor={}&limit={}", peer_url, cursor, self.page_size());

        let response = self
            .authed(self.client_for(peer_url)?.get(&url))
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
                    "Peer does not support the cursor feed (protocol 1.1 or newer required)".to_string(),
                ))
            }
        };

        // Changes carry the sender's identity so that failures are queued
        // against the right peer
        let mut changes = batch.changes;
        for c in &mut changes {
            if c.device_id.is_empty() {
                c.device_id = batch.device_id.clone();
            }
            if c.device_name.is_none() {
                c.device_name = Some(if batch.device_name.is_empty() { peer_name.to_string() } else { batch.device_name.clone() });
            }
        }

        tracing::debug!("Received {} changes from {} (cursor {} -> {})", changes.len(), &peer_id[..UUID_SHORT_LEN.min(peer_id.len())], cursor, next_cursor);
        for change in &changes {
            tracing::trace!("  Pull: {} {} from {}", change.entity_type, &change.entity_id[..UUID_SHORT_LEN.min(change.entity_id.len())], change.device_id);
        }

        let (applied, conflicts, errors) = self.apply_changes_from(&changes, peer_id, Some(peer_name))?;
        tracing::debug!("Applied {} changes, {} conflicts", applied, conflicts);

        Ok((
            PullOutcome { applied, conflicts, changes, errors, warnings: Vec::new() },
            next_cursor,
            batch.is_complete,
        ))
    }

    fn apply_changes_from(&self, changes: &[SyncChange], peer_id: &str, peer_name: Option<&str>) -> VoiceResult<(i64, i64, Vec<String>)> {
        let db = self.db.lock().unwrap();
        let sync_received_at = Utc::now().timestamp();
        let outcome = crate::sync_apply::apply_changes(&db, changes, peer_id, peer_name, sync_received_at)?;
        if outcome.retried_ok > 0 {
            tracing::info!("Applied {} previously failed changes", outcome.retried_ok);
        }
        Ok((outcome.applied, outcome.conflicts, outcome.errors))
    }

    fn update_peer_sync_time(&self, peer_id: &str) -> VoiceResult<()> {
        let (name, url) = {
            let config = self.config.lock().unwrap();
            config.get_peer(peer_id).map(|p| (p.peer_name.clone(), p.peer_url.clone())).unwrap_or_default()
        };
        self.db.lock().unwrap().set_peer_last_operation(peer_id, Some(&name), Some(&url), "sync")
    }

    /// Push pre-fetched changes to peer (used by initial_sync)
    async fn push_changes_with_data(
        &self,
        peer_url: &str,
        changes: &[SyncChange],
    ) -> VoiceResult<(i64, i64, Vec<String>)> {
        if changes.is_empty() {
            return Ok((0, 0, Vec::new()));
        }

        let request = ApplyRequest {
            device_id: self.device_id.clone(),
            device_name: self.device_name.clone(),
            changes: changes.to_vec(),
        };

        let response = self
            .authed(self.client_for(peer_url)?.post(format!("{}/sync/apply", peer_url)))
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
    fn file_client_for(&self, peer_url: &str) -> VoiceResult<Client> {
        check_scheme(peer_url)?;
        let pin = self.pin_for(peer_url);
        let cache_key = format!("file\n{}\n{}", peer_url, pin);
        if let Some(client) = self.clients.lock().unwrap().get(&cache_key) {
            return Ok(client.clone());
        }
        let builder = Client::builder()
            .connect_timeout(connect_timeout_for(peer_url))
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

    fn pin_for(&self, peer_url: &str) -> String {
        let config = self.config.lock().unwrap();
        config
            .peers()
            .iter()
            .find(|p| p.peer_url.trim_end_matches('/') == peer_url.trim_end_matches('/'))
            .and_then(|p| p.certificate_fingerprint.clone())
            .unwrap_or_default()
    }

    /// Fetch one recording from a peer (FILE-12): streamed into
    /// `<file>.part`, continuing from the bytes already there, verified by
    /// the hash the peer announces, and renamed when whole. Returns the
    /// bytes received in this call.
    pub async fn fetch_audio_file(
        &self,
        peer_url: &str,
        audio_id: &str,
        dest_path: &std::path::Path,
        moved_before: u64,
        done: i64,
        total_files: i64,
    ) -> VoiceResult<u64> {
        use futures_util::StreamExt;
        use tokio::io::AsyncWriteExt;

        let url = format!("{}/sync/audio/{}/file", peer_url, audio_id);
        if let Some(parent) = dest_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let have = crate::transfer::part_len(dest_path);
        let mut request = self.authed(self.file_client_for(peer_url)?.get(&url));
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
            let chunk = chunk.map_err(|e| VoiceError::Network(format!("The fetch of {} stopped: {}", audio_id, describe(&e))))?;
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
        Ok(received)
    }

    /// Send one recording to a peer (FILE-12): streamed from the file, from
    /// `from_byte` when the peer already holds a part, with the size and
    /// the hash in headers so the peer can verify. Returns the bytes sent.
    pub async fn send_audio_file(
        &self,
        peer_url: &str,
        audio_id: &str,
        source_path: &std::path::Path,
    ) -> VoiceResult<u64> {
        self.send_audio_file_from(peer_url, audio_id, source_path, 0, 0, 0, 1).await
    }

    pub async fn send_audio_file_from(
        &self,
        peer_url: &str,
        audio_id: &str,
        source_path: &std::path::Path,
        from_byte: u64,
        moved_before: u64,
        done: i64,
        total_files: i64,
    ) -> VoiceResult<u64> {
        use futures_util::StreamExt;
        use tokio::io::AsyncSeekExt;

        let url = format!("{}/sync/audio/{}/file", peer_url, audio_id);
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
        // early, the peer keeps the part, and the next send continues from it
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
            .authed(self.file_client_for(peer_url)?.post(&url))
            .header("Content-Type", "application/octet-stream")
            .header("Content-Length", (total - from_byte).to_string())
            .header(crate::sync_protocol::HEADER_FILE_SHA256, &hash);
        if from_byte > 0 {
            request = request.header("Content-Range", format!("bytes {}-{}/{}", from_byte, total.saturating_sub(1), total));
        }
        let response = match request.body(reqwest::Body::wrap_stream(stream)).send().await {
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

    /// Ask the peer which of these recordings it lacks, and how much of each
    /// it already holds (FILE-12): one round trip for any number of files.
    async fn missing_on_peer(&self, peer_url: &str, audio_ids: Vec<String>) -> VoiceResult<crate::sync_protocol::MissingFilesResponse> {
        let response = self
            .authed(self.client_for(peer_url)?.post(format!("{}/sync/audio/missing", peer_url)))
            .json(&crate::sync_protocol::MissingFilesRequest { audio_ids })
            .send()
            .await
            .map_err(|e| VoiceError::Network(describe(&e)))?;
        if !response.status().is_success() {
            return Err(VoiceError::Sync(format!("The peer would not say which files it lacks: HTTP {}", response.status())));
        }
        response
            .json()
            .await
            .map_err(|e| VoiceError::Sync(format!("Could not read the peer's missing list: {}", e)))
    }

    /// A transfer is tried up to three times, waiting one, two and four
    /// seconds between tries (FILE-14). A refusal (HTTP 4xx) is not retried.
    async fn with_retries<F, Fut>(&self, what: &str, mut attempt: F) -> VoiceResult<u64>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = VoiceResult<u64>>,
    {
        let mut wait = Duration::from_secs(1);
        let mut last = None;
        for tries_left in (0..3).rev() {
            match attempt().await {
                Ok(n) => return Ok(n),
                Err(e) => {
                    let text = e.to_string();
                    let refused = text.contains("HTTP 4") || text.ends_with(CANCELLED);
                    tracing::warn!("{} failed: {}{}", what, text, if tries_left > 0 && !refused { "; trying again" } else { "" });
                    last = Some(e);
                    if refused || tries_left == 0 {
                        break;
                    }
                    tokio::time::sleep(wait).await;
                    wait *= 2;
                }
            }
        }
        Err(last.unwrap_or_else(|| VoiceError::Sync(format!("{} failed", what))))
    }

    /// **Send** (terms): every recording this device holds that the peer
    /// lacks, in one question and as many transfers. Returns (files, bytes,
    /// errors).
    pub async fn send_missing_to_peer(&self, peer_id: &str, peer_url: &str, audiofile_directory: &std::path::Path) -> (i64, u64, Vec<String>) {
        let mut errors = Vec::new();
        let rows = match self.db.lock().unwrap().get_all_audio_files() {
            Ok(rows) => rows,
            Err(e) => return (0, 0, vec![format!("Failed to list recordings: {}", e)]),
        };
        let local: Vec<_> = rows
            .into_iter()
            .filter(|r| r.deleted_at.is_none())
            .map(|r| (r.id.clone(), audio_local_path(audiofile_directory, &r.local_name)))
            .filter(|(_, path)| path.is_file())
            .collect();
        if local.is_empty() {
            return (0, 0, errors);
        }
        let missing = match self.missing_on_peer(peer_url, local.iter().map(|(id, _)| id.clone()).collect()).await {
            Ok(m) => m,
            Err(e) => return (0, 0, vec![e.to_string()]),
        };
        let mut sent = 0i64;
        let mut bytes = 0u64;
        let to_send: Vec<_> = local.into_iter().filter(|(id, _)| missing.missing.contains(id)).collect();
        let total = to_send.len() as i64;
        for (audio_id, path) in to_send {
            if self.cancelled() {
                errors.push(CANCELLED.to_string());
                break;
            }
            self.report("send", sent, total, bytes, format!("Sending recording {} of {}", sent + 1, total));
            let from = missing.partial.get(&audio_id).copied().unwrap_or(0);
            let what = format!("Send of {}", &audio_id[..UUID_SHORT_LEN.min(audio_id.len())]);
            let moved_before = bytes;
            match self.with_retries(&what, || self.send_audio_file_from(peer_url, &audio_id, &path, from, moved_before, sent, total)).await {
                Ok(n) => {
                    sent += 1;
                    bytes += n;
                    self.note_copy(&audio_id, peer_id);
                    self.report("send", sent, total, bytes, format!("Sent recording {} of {}", sent, total));
                }
                Err(e) if e.to_string().ends_with(CANCELLED) => {
                    errors.push(CANCELLED.to_string());
                    break;
                }
                Err(e) => errors.push(e.to_string()),
            }
        }
        (sent, bytes, errors)
    }

    /// **Fetch** (terms): every recording the peer holds that this device
    /// lacks. The rows say what exists; the peer answers 404 for a file it
    /// does not hold, which is not an error. Returns (files, bytes, errors).
    pub async fn fetch_missing_from_peer(&self, peer_id: &str, peer_url: &str, audiofile_directory: &std::path::Path) -> (i64, u64, Vec<String>) {
        let mut errors = Vec::new();
        let rows = match self.db.lock().unwrap().get_all_audio_files() {
            Ok(rows) => rows,
            Err(e) => return (0, 0, vec![format!("Failed to list recordings: {}", e)]),
        };
        let mut fetched = 0i64;
        let mut bytes = 0u64;
        let wanted: Vec<_> = rows
            .into_iter()
            .filter(|r| r.deleted_at.is_none() && !audio_local_path(audiofile_directory, &r.local_name).is_file())
            .collect();
        let total = wanted.len() as i64;
        for row in wanted {
            if self.cancelled() {
                errors.push(CANCELLED.to_string());
                break;
            }
            let path = audio_local_path(audiofile_directory, &row.local_name);
            self.report("fetch", fetched, total, bytes, format!("Fetching recording {} of {}", fetched + 1, total));
            let what = format!("Fetch of {}", &row.id[..UUID_SHORT_LEN.min(row.id.len())]);
            let moved_before = bytes;
            match self.with_retries(&what, || self.fetch_audio_file(peer_url, &row.id, &path, moved_before, fetched, total)).await {
                Ok(n) => {
                    fetched += 1;
                    bytes += n;
                    self.note_copy(&row.id, peer_id);
                    self.report("fetch", fetched, total, bytes, format!("Fetched recording {} of {}", fetched, total));
                }
                Err(e) if e.to_string().ends_with(CANCELLED) => {
                    errors.push(CANCELLED.to_string());
                    break;
                }
                Err(e) if e.to_string().contains("HTTP 404") => {}
                Err(e) => errors.push(e.to_string()),
            }
        }
        // The peer learns what this device holds now (Stage 10): one
        // missing-list request, whose answer is not needed
        if fetched > 0 {
            let held: Vec<String> = match self.db.lock().unwrap().get_all_audio_files() {
                Ok(rows) => rows
                    .into_iter()
                    .filter(|r| r.deleted_at.is_none() && audio_local_path(audiofile_directory, &r.local_name).is_file())
                    .map(|r| r.id)
                    .collect(),
                Err(_) => Vec::new(),
            };
            if !held.is_empty() {
                if let Err(e) = self.missing_on_peer(peer_url, held).await {
                    tracing::debug!("The peer was not told what this device holds: {}", e);
                }
            }
        }
        (fetched, bytes, errors)
    }

    /// The audio directory this device keeps recordings in.
    fn audio_directory(&self) -> Option<std::path::PathBuf> {
        self.config.lock().ok().and_then(|c| c.audiofile_directory().map(std::path::PathBuf::from))
    }

    fn peer_url_of(&self, peer_id: &str) -> Option<String> {
        self.config.lock().ok().and_then(|c| c.get_peer(peer_id).map(|p| p.peer_url.clone()))
    }

    /// **Deliver**: sync, then send.
    pub async fn deliver(&self, peer_id: &str) -> SyncResult {
        let mut result = self.sync_with_peer(peer_id).await;
        if !result.success {
            return result;
        }
        self.move_files(peer_id, &mut result, true, false).await;
        self.record_operation(peer_id, "deliver");
        result
    }

    /// **Exchange**: sync, then send and fetch.
    pub async fn exchange(&self, peer_id: &str) -> SyncResult {
        let mut result = self.sync_with_peer(peer_id).await;
        if !result.success {
            return result;
        }
        self.move_files(peer_id, &mut result, true, true).await;
        self.record_operation(peer_id, "exchange");
        result
    }

    /// **Send** alone, or **fetch** alone, without a sync.
    pub async fn send_to_peer(&self, peer_id: &str) -> SyncResult {
        self.begin_operation();
        let mut result = SyncResult::success();
        self.move_files(peer_id, &mut result, true, false).await;
        if result.success {
            self.record_operation(peer_id, "send");
        }
        result
    }

    pub async fn fetch_from_peer(&self, peer_id: &str) -> SyncResult {
        self.begin_operation();
        let mut result = SyncResult::success();
        self.move_files(peer_id, &mut result, false, true).await;
        if result.success {
            self.record_operation(peer_id, "fetch");
        }
        result
    }

    /// A peer holds a copy of a recording now (Stage 10).
    fn note_copy(&self, audio_id: &str, peer_id: &str) {
        if let Err(e) = self.db.lock().unwrap().record_copy(audio_id, peer_id) {
            tracing::warn!("Could not record that {} holds {}: {}", short_id(peer_id), short_id(audio_id), e);
        }
    }

    /// The peer's row remembers when it was last reached and by what, and
    /// the peer becomes the one the visible button names (Stage 5).
    fn record_operation(&self, peer_id: &str, operation: &str) {
        let (name, url) = {
            let config = self.config.lock().unwrap();
            config.get_peer(peer_id).map(|p| (p.peer_name.clone(), p.peer_url.clone())).unwrap_or_default()
        };
        if let Err(e) = self.db.lock().unwrap().set_peer_last_operation(peer_id, Some(&name), Some(&url), operation) {
            tracing::warn!("Could not record the {} with {}: {}", operation, short_id(peer_id), e);
        }
        if let Err(e) = self.config.lock().unwrap().set_last_peer(peer_id) {
            tracing::warn!("Could not remember {} as the last peer: {}", short_id(peer_id), e);
        }
    }

    /// After a sync every card of the account is a peer (Stage 5): a card
    /// without an entry gets one, with the card's name, its first address
    /// and its fingerprint; an entry re-pins its fingerprint from the card
    /// (which arrived over an authenticated connection); a revoked card's
    /// entry goes; a forgotten peer stays forgotten. The remembered address
    /// is never replaced by the card's.
    pub fn adopt_peers_from_cards(&self) -> VoiceResult<usize> {
        let cards = self.db.lock().unwrap().list_device_cards()?;
        let mut config = self.config.lock().unwrap();
        let own = config.device_id_hex().to_string();
        let mut changed = 0;
        for card in cards {
            if card.device_id == own || config.is_forgotten(&card.device_id) {
                continue;
            }
            if card.revoked == "1" {
                if config.remove_peer(&card.device_id)? {
                    changed += 1;
                }
                continue;
            }
            let first_address = serde_json::from_str::<Vec<String>>(&card.addresses).ok().and_then(|a| a.into_iter().next()).unwrap_or_default();
            let fingerprint = if card.certificate_fingerprint.is_empty() { None } else { Some(card.certificate_fingerprint.as_str()) };
            match config.get_peer(&card.device_id).cloned() {
                None => {
                    let name = if card.name.is_empty() { short_id(&card.device_id).to_string() } else { card.name.clone() };
                    config.add_peer(&card.device_id, &name, &first_address, fingerprint, false)?;
                    changed += 1;
                }
                Some(existing) => {
                    let url = if existing.peer_url.is_empty() { first_address } else { existing.peer_url.clone() };
                    let pin_changed = fingerprint.is_some() && existing.certificate_fingerprint.as_deref() != fingerprint;
                    if url != existing.peer_url || pin_changed {
                        config.add_peer(&card.device_id, &existing.peer_name, &url, fingerprint, true)?;
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

    async fn move_files(&self, peer_id: &str, result: &mut SyncResult, send: bool, fetch: bool) {
        result.request_id = self.request_id();
        let Some(peer_url) = self.peer_url_of(peer_id) else {
            result.errors.push(format!("Unknown peer: {}", peer_id));
            result.success = false;
            return;
        };
        let Some(dir) = self.audio_directory() else {
            result.errors.push("No audio directory is configured on this device".to_string());
            result.success = false;
            return;
        };
        if send {
            let (n, bytes, errors) = self.send_missing_to_peer(peer_id, &peer_url, &dir).await;
            result.sent += n;
            result.bytes_moved += bytes;
            result.errors.extend(errors);
        }
        if fetch {
            let (n, bytes, errors) = self.fetch_missing_from_peer(peer_id, &peer_url, &dir).await;
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
        let result = crate::file_storage::download_missing_audio_files(&db, audiofile_directory).await?;
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
        crate::file_storage::download_audio_file(&db, audiofile_directory, audio_file_id).await
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
        crate::file_storage::download_audio_files_for_note(&db, audiofile_directory, note_id).await
    }

}

/// Sync with all configured peers
pub async fn sync_all_peers(
    db: Arc<Mutex<Database>>,
    config: Arc<Mutex<Config>>,
) -> HashMap<String, SyncResult> {
    let client = match SyncClient::new(db, config.clone()) {
        Ok(c) => c,
        Err(_) => return HashMap::new(),
    };

    let peers: Vec<String> = {
        let cfg = config.lock().unwrap();
        cfg.peers().iter().map(|p| p.peer_id.clone()).collect()
    };

    let mut results = HashMap::new();
    for peer_id in peers {
        let result = client.sync_with_peer(&peer_id).await;
        results.insert(peer_id, result);
    }

    results
}

#[cfg(test)]
mod tests {
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
        let config = Config::new(Some(temp_dir.path().to_path_buf())).unwrap();

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
            assert!(!client.device_id.is_empty());
            // Device name should be set
            assert!(!client.device_name.is_empty());
        }
    }

    mod sync_with_unknown_peer_tests {
        use super::*;

        #[tokio::test]
        async fn test_sync_with_unknown_peer() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config).unwrap();

            let result = client.sync_with_peer("00000000000070008000000000000099").await;
            assert!(!result.success);
            assert!(result.errors.iter().any(|e| e.contains("Unknown peer")));
        }

        #[tokio::test]
        async fn test_pull_from_unknown_peer() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config).unwrap();

            let result = client.pull_from_peer("00000000000070008000000000000099").await;
            assert!(!result.success);
            assert!(result.errors.iter().any(|e| e.contains("Unknown peer")));
        }

        #[tokio::test]
        async fn test_push_to_unknown_peer() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config).unwrap();

            let result = client.push_to_peer("00000000000070008000000000000099").await;
            assert!(!result.success);
            assert!(result.errors.iter().any(|e| e.contains("Unknown peer")));
        }

        #[tokio::test]
        async fn test_initial_sync_unknown_peer() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config).unwrap();

            let result = client.initial_sync("00000000000070008000000000000099").await;
            assert!(!result.success);
            assert!(result.errors.iter().any(|e| e.contains("Unknown peer")));
        }

        #[tokio::test]
        async fn test_check_peer_status_unknown_peer() {
            let (db, config, _temp_dir) = create_test_db_and_config();
            let client = SyncClient::new(db, config).unwrap();

            let result = client.check_peer_status("00000000000070008000000000000099").await;
            // Should return None or error for unknown peer
            assert!(result.is_empty() || result.get("error").is_some());
        }
    }

    mod sync_all_peers_tests {
        use super::*;

        #[tokio::test]
        async fn test_sync_all_peers_empty() {
            let (db, config, _temp_dir) = create_test_db_and_config();

            let results = sync_all_peers(db, config).await;
            assert!(results.is_empty());
        }
    }

}
