//! Sync server implementation using Axum.
//!
//! This module provides the server side of the sync protocol:
//! - /sync/handshake - Exchange device info
//! - /sync/changes - Get changes since timestamp
//! - /sync/apply - Apply changes from peer
//! - /sync/status - Health check
//! - /sync/audio/:id/file - One recording's bytes: GET serves it to a fetching peer, POST receives it from a sending peer

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::{
    body::Body,
    extract::{ConnectInfo, DefaultBodyLimit, Extension, Path, Query, Request, State},
    http::{HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::auth;
use crate::config::Config;
use crate::database::Database;
use crate::error::VoiceResult;
use crate::models::{audio_local_path, SyncChange};
use crate::sync_protocol::{
    codes, ApplyRequest, ApplyResponse, ChangesQuery, ChangesResponse, ErrorResponse, HandshakeRequest,
    HandshakeResponse, MissingFilesRequest, MissingFilesResponse, PairClaimRequest, PairClaimResponse, PairGrantRequest,
    PairGrantResponse, StatusResponse, HEADER_FILE_SHA256, HEADER_REQUEST_ID, PROTOCOL_VERSION,
};
use tracing::Instrument;
use crate::UUID_SHORT_LEN;

/// The running listener's shutdown handle; replaced by every start, taken
/// by a stop, so a listener can be started again after it stopped.
static SHUTDOWN_TX: Mutex<Option<oneshot::Sender<()>>> = Mutex::new(None);

/// When the listener last served a request, or started (Stage 6: the idle
/// stop). Seconds since the Unix epoch; 0 when no listener ran.
static LAST_ACTIVITY: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

fn note_activity() {
    LAST_ACTIVITY.store(Utc::now().timestamp(), std::sync::atomic::Ordering::Relaxed);
}

/// Seconds since the listener last served a request or started; None when
/// no listener has run in this process.
pub fn idle_seconds() -> Option<u64> {
    match LAST_ACTIVITY.load(std::sync::atomic::Ordering::Relaxed) {
        0 => None,
        at => Some(u64::try_from(Utc::now().timestamp() - at).unwrap_or(0)),
    }
}

/// The verified device behind a request, for the handlers that record
/// what it holds (Stage 10).
#[derive(Clone, Debug)]
pub struct CallerDevice(pub String);

/// One account as the server holds it open: its database and its config.
#[derive(Clone)]
pub struct AccountHandle {
    pub db: Arc<Mutex<Database>>,
    pub config: Arc<Mutex<Config>>,
}

/// Where the server finds the accounts it serves (AUTH-4, Stage 3).
pub trait AccountSource: Send + Sync {
    /// The account, opened, or None when this server does not hold it.
    fn account(&self, account_id: &str) -> Option<AccountHandle>;
    /// Take on a new account by grant (PAIR-5): register it, store the key
    /// the holder made for this device, and return it open. A device that
    /// does not host accounts answers with a sentence.
    fn take_hosted_account(&self, account_id: &str, label: &str, device_key: &str) -> Result<AccountHandle, String>;
    /// Spend a hosting token (PAIR-5), returning the label it was offered with.
    fn spend_hosting_token(&self, token: &str) -> Option<String>;
    /// Every account this server may serve, for the periodic work.
    fn served(&self) -> Vec<String>;
    /// The accounts open right now, for the record when the listener stops.
    fn open_handles(&self) -> Vec<AccountHandle>;
    /// The one account, when this server holds exactly one; a host that
    /// serves several answers None.
    fn single(&self) -> Option<AccountHandle>;
    /// Where the request log of an account is written (Stage 3): beside its
    /// database on a host; nowhere on a single-account listener, whose own
    /// log already carries the lines.
    fn audit_log_path(&self, account: &AccountHandle) -> Option<std::path::PathBuf>;
}

/// The one account of a single-directory installation.
pub struct SingleAccount {
    pub account_id: String,
    pub handle: AccountHandle,
}

impl AccountSource for SingleAccount {
    fn account(&self, account_id: &str) -> Option<AccountHandle> {
        if account_id == self.account_id {
            Some(self.handle.clone())
        } else {
            None
        }
    }

    fn take_hosted_account(&self, _account_id: &str, _label: &str, _device_key: &str) -> Result<AccountHandle, String> {
        Err(format!("This device holds one account and does not host others ({})", codes::ACCOUNT_UNKNOWN))
    }

    fn spend_hosting_token(&self, _token: &str) -> Option<String> {
        None
    }

    fn served(&self) -> Vec<String> {
        vec![self.account_id.clone()]
    }

    fn open_handles(&self) -> Vec<AccountHandle> {
        vec![self.handle.clone()]
    }

    fn single(&self) -> Option<AccountHandle> {
        Some(self.handle.clone())
    }

    fn audit_log_path(&self, _account: &AccountHandle) -> Option<std::path::PathBuf> {
        None
    }
}

/// Every account of an indexed root, opened on demand and kept open, at
/// most [`OPEN_ACCOUNTS_KEPT`] at a time (Stage 3). Nothing is opened at
/// start; the first request for an account opens it.
pub struct IndexedAccounts {
    root: std::path::PathBuf,
    listen_urls: Vec<String>,
    open: Mutex<Vec<(String, AccountHandle, Instant)>>,
}

/// How many hosted accounts stay open at once; the least recently used is
/// closed when another is needed.
pub const OPEN_ACCOUNTS_KEPT: usize = 64;

impl IndexedAccounts {
    pub fn new(root: &std::path::Path, listen_urls: Vec<String>) -> Self {
        Self { root: root.to_path_buf(), listen_urls, open: Mutex::new(Vec::new()) }
    }

    fn open_account(&self, account_id: &str) -> VoiceResult<AccountHandle> {
        let index = crate::accounts::AccountIndex::open(&self.root)?;
        let entry = index.find(account_id)?.ok_or_else(|| crate::error::VoiceError::NotFound(account_id.to_string()))?;
        if entry.account_id != account_id {
            return Err(crate::error::VoiceError::NotFound(account_id.to_string()));
        }
        let dir = index.directory(&entry.account_id);
        let db = Database::new_for_account(dir.join("notes.db"), &entry.account_id)?;
        let mut config = Config::open_account(&self.root, &dir)?;
        record_listening(&db, &mut config, &self.listen_urls, true)?;
        index.touch(&entry.account_id)?;
        Ok(AccountHandle { db: Arc::new(Mutex::new(db)), config: Arc::new(Mutex::new(config)) })
    }
}

impl AccountSource for IndexedAccounts {
    fn account(&self, account_id: &str) -> Option<AccountHandle> {
        let mut open = self.open.lock().unwrap();
        if let Some(entry) = open.iter_mut().find(|(id, _, _)| id == account_id) {
            entry.2 = Instant::now();
            return Some(entry.1.clone());
        }
        let handle = match self.open_account(account_id) {
            Ok(h) => h,
            Err(e) => {
                tracing::debug!("Account {} is not served here: {}", short(account_id), e);
                return None;
            }
        };
        if open.len() >= OPEN_ACCOUNTS_KEPT {
            if let Some(oldest) = open.iter().enumerate().min_by_key(|(_, (_, _, used))| *used).map(|(i, _)| i) {
                open.remove(oldest);
            }
        }
        open.push((account_id.to_string(), handle.clone(), Instant::now()));
        Some(handle)
    }

    fn take_hosted_account(&self, account_id: &str, label: &str, device_key: &str) -> Result<AccountHandle, String> {
        let index = crate::accounts::AccountIndex::open(&self.root).map_err(|e| e.to_string())?;
        if index.find(account_id).map_err(|e| e.to_string())?.is_some() {
            return Err(format!("This server already holds account {}", short(account_id)));
        }
        let label = if label.trim().is_empty() { format!("hosted-{}", &account_id[..8]) } else { label.trim().to_string() };
        index.register(account_id, &label, true).map_err(|e| e.to_string())?;
        let dir = index.directory(account_id);
        let mut config = Config::open_account(&self.root, &dir).map_err(|e| e.to_string())?;
        config.set_device_key(device_key).map_err(|e| e.to_string())?;
        drop(config);
        self.account(account_id).ok_or_else(|| "The account was registered but could not be opened".to_string())
    }

    fn spend_hosting_token(&self, token: &str) -> Option<String> {
        let index = crate::accounts::AccountIndex::open(&self.root).ok()?;
        index.spend_hosting_token(&auth::key_hash(token), Utc::now().timestamp()).ok().flatten()
    }

    fn served(&self) -> Vec<String> {
        crate::accounts::AccountIndex::open(&self.root)
            .and_then(|i| i.list())
            .map(|l| l.into_iter().map(|a| a.account_id).collect())
            .unwrap_or_default()
    }

    fn open_handles(&self) -> Vec<AccountHandle> {
        self.open.lock().unwrap().iter().map(|(_, h, _)| h.clone()).collect()
    }

    fn single(&self) -> Option<AccountHandle> {
        None
    }

    fn audit_log_path(&self, account: &AccountHandle) -> Option<std::path::PathBuf> {
        Some(account.config.lock().unwrap().config_dir().join("audit.log"))
    }
}

/// The request log of a hosted account is bounded like every other log
/// (TECHNICAL-DECISIONS 7.1): at this size it is renamed to `.1`, the
/// previous `.1` to `.2`, and the `.2` before that is gone.
pub const AUDIT_LOG_ROTATE_BYTES: u64 = 5 * 1024 * 1024;

/// One line per request to a hosted account: time, device, route, bytes in
/// and out, outcome and refusal code. Never a key, never content.
fn audit(path: &std::path::Path, line: &str) {
    use std::io::Write;
    if let Ok(meta) = std::fs::metadata(path) {
        if meta.len() >= AUDIT_LOG_ROTATE_BYTES {
            let older = path.with_extension("log.2");
            let old = path.with_extension("log.1");
            let _ = std::fs::remove_file(&older);
            let _ = std::fs::rename(&old, &older);
            let _ = std::fs::rename(path, &old);
        }
    }
    match std::fs::OpenOptions::new().create(true).append(true).open(path) {
        Ok(mut file) => {
            let _ = writeln!(file, "{}", line);
        }
        Err(e) => tracing::warn!("Could not write the audit log {}: {}", path.display(), e),
    }
}

/// The audit line of one request, written after the handler answered.
fn audit_request(
    state: &AppState,
    account: Option<&AccountHandle>,
    request_id: &str,
    device: Option<&str>,
    method: &str,
    path: &str,
    bytes_in: u64,
    response: &Response,
    code: &str,
) {
    let Some(account) = account else { return };
    let Some(log) = state.accounts.audit_log_path(account) else { return };
    let bytes_out = response
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .map(|n| n.to_string())
        .unwrap_or_else(|| "-".to_string());
    let outcome = response.status().as_u16();
    let line = format!(
        "{} {} {} {} {} in={} out={} {} {}",
        Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        request_id,
        device.unwrap_or("-"),
        method,
        path,
        bytes_in,
        bytes_out,
        outcome,
        if code.is_empty() { "-" } else { code }
    );
    audit(&log, &line);
}

/// Shared server state
#[derive(Clone)]
struct AppState {
    accounts: Arc<dyn AccountSource>,
    device_id: String,
    device_name: String,
    /// Whether only private addresses may call (LISTEN-3): true unless the
    /// machine has a public URL configured
    lan_only: bool,
    /// Refusals per source address, for the delay that slows a guesser
    /// (AUTH-5): count and the time of the last one.
    failures: Arc<Mutex<HashMap<IpAddr, (u32, Instant)>>>,
}

/// After this many refusals from one address, each further refusal waits
/// before answering; the wait doubles up to [`MAX_DELAY`].
const FREE_FAILURES: u32 = 3;
const MAX_DELAY: Duration = Duration::from_secs(8);
/// Counters older than this are forgotten, and the map is emptied when it
/// grows past [`MAX_TRACKED_ADDRESSES`], so memory stays bounded.
const FAILURE_MEMORY: Duration = Duration::from_secs(600);
const MAX_TRACKED_ADDRESSES: usize = 10_000;

/// The first characters of an id, for a sentence.
fn short(id: &str) -> &str {
    &id[..UUID_SHORT_LEN.min(id.len())]
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok()).map(str::trim).filter(|v| !v.is_empty())
}

/// The bearer token of an `Authorization` header, if there is one.
fn bearer(headers: &HeaderMap) -> Option<&str> {
    header(headers, "authorization")
        .and_then(|v| v.strip_prefix("Bearer ").or_else(|| v.strip_prefix("bearer ")))
        .map(str::trim)
        .filter(|v| !v.is_empty())
}

/// `POST /pair/grant` (PAIR-5): a holder gives this device, which holds no
/// account yet, its account: the token from this device's grant text, the
/// account id, a key made for this device, and the holder's card.
async fn pair_grant(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Json(request): Json<PairGrantRequest>,
) -> Response {
    let refuse = |status: StatusCode, sentence: String, code: &str| {
        (status, Json(ErrorResponse::with_code(sentence, code))).into_response()
    };
    let Some(offered_label) = state.accounts.spend_hosting_token(&request.token) else {
        let delay = note_failure(&state, addr.ip());
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        tracing::warn!("Refused a hosting grant from {}: {}", addr.ip(), codes::TOKEN_INVALID);
        return refuse(StatusCode::FORBIDDEN, format!("The grant text is not valid: it was spent, it expired, or it was mistyped ({})", codes::TOKEN_INVALID), codes::TOKEN_INVALID);
    };
    if crate::database::validate_account_id(&request.account_id).is_err() || request.device_key.is_empty() || request.holder_id.len() != 32 {
        return refuse(StatusCode::BAD_REQUEST, format!("The grant is incomplete ({})", codes::SETUP_TEXT_INVALID), codes::SETUP_TEXT_INVALID);
    }
    let label = if request.label.trim().is_empty() { offered_label } else { request.label.clone() };
    let account = match state.accounts.take_hosted_account(&request.account_id, &label, &request.device_key) {
        Ok(a) => a,
        Err(sentence) => return refuse(StatusCode::CONFLICT, sentence, codes::ACCOUNT_UNKNOWN),
    };
    let own = {
        let db = account.db.lock().unwrap();
        let mut config = account.config.lock().unwrap();
        if let Some(key) = request.recording_key.as_deref().filter(|k| !k.is_empty()) {
            if let Err(e) = config.set_recording_key(key) {
                return refuse(StatusCode::BAD_REQUEST, format!("The recording key in the grant is not one: {}", e), codes::SETUP_TEXT_INVALID);
            }
        }
        if let Err(e) = db.admit_device_card(&crate::versions::DeviceCard {
            device_id: request.holder_id.clone(),
            name: request.holder_name.clone(),
            certificate_fingerprint: request.holder_certificate_fingerprint.clone(),
            addresses: request.holder_addresses.clone(),
            listens: "0".to_string(),
            key_hash: request.holder_key_hash.clone(),
            revoked: "0".to_string(),
            application: auth::APPLICATION_VOICE.to_string(),
        }) {
            return refuse(StatusCode::INTERNAL_SERVER_ERROR, format!("Could not write the holder's card: {}", e), "");
        }
        match auth::ensure_own_device_card(&db, &mut config) {
            Ok(card) => card,
            Err(e) => return refuse(StatusCode::INTERNAL_SERVER_ERROR, format!("Could not write this device's card: {}", e), ""),
        }
    };
    tracing::info!("Now hosting account {} ({}) for {}", short(&request.account_id), label, short(&request.holder_id));
    Json(PairGrantResponse {
        account_id: request.account_id,
        device_id: own.device_id,
        device_name: own.name,
        certificate_fingerprint: own.certificate_fingerprint,
        addresses: own.addresses,
        key_hash: own.key_hash,
    })
    .into_response()
}

/// Whether a caller at `ip` may be served (LISTEN-3): from a private
/// network, link-local or this machine itself always; from anywhere else
/// only when the machine has a public URL configured.
pub fn source_allowed(ip: IpAddr, has_public_url: bool) -> bool {
    if has_public_url {
        return true;
    }
    match ip {
        IpAddr::V4(v4) => v4.is_private() || v4.is_loopback() || v4.is_link_local(),
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.to_ipv4_mapped().map(|v4| v4.is_private() || v4.is_loopback() || v4.is_link_local()).unwrap_or(false)
                || (v6.segments()[0] & 0xffc0) == 0xfe80
                || (v6.segments()[0] & 0xfe00) == 0xfc00
        }
    }
}

/// The gate of LISTEN-3, before any route.
async fn lan_only_gate(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    request: Request,
    next: Next,
) -> Response {
    note_activity();
    if state.lan_only && !source_allowed(addr.ip(), false) {
        tracing::warn!("Refused {} from {}: not a private address and no public URL is configured", request.uri().path(), addr.ip());
        return (
            StatusCode::FORBIDDEN,
            Json(ErrorResponse::with_code(
                format!("This device serves its own network only; it has no public address ({})", codes::NOT_ON_LAN),
                codes::NOT_ON_LAN,
            )),
        )
            .into_response();
    }
    next.run(request).await
}

/// Count a refusal from an address and say how long to wait before
/// answering it (AUTH-5).
fn note_failure(state: &AppState, ip: IpAddr) -> Duration {
    let mut failures = state.failures.lock().unwrap();
    let now = Instant::now();
    if failures.len() > MAX_TRACKED_ADDRESSES {
        failures.clear();
    }
    let entry = failures.entry(ip).or_insert((0, now));
    if now.duration_since(entry.1) > FAILURE_MEMORY {
        entry.0 = 0;
    }
    entry.0 += 1;
    entry.1 = now;
    if entry.0 > FREE_FAILURES {
        let doublings = (entry.0 - FREE_FAILURES).min(3);
        Duration::from_secs(1 << doublings).min(MAX_DELAY)
    } else {
        Duration::ZERO
    }
}

/// `POST /pair/claim` (PAIR-3): a reading device presents the token from
/// the code and receives a key of its own. Authenticated by the token
/// alone, so it sits outside the device-key middleware; a wrong token
/// counts like any other refusal from that address.
async fn pair_claim(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Json(request): Json<PairClaimRequest>,
) -> Response {
    let Some(account) = state.accounts.account(&request.account_id) else {
        let delay = note_failure(&state, addr.ip());
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        return (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse::with_code(format!("This server does not host your account ({})", codes::ACCOUNT_UNKNOWN), codes::ACCOUNT_UNKNOWN)),
        )
            .into_response();
    };
    let admitted = {
        let db = account.db.lock().unwrap();
        crate::pairing::admit_by_token(
            &db,
            &request.token,
            &request.device_id,
            &request.device_name,
            &request.certificate_fingerprint,
            &request.addresses,
            &request.application,
        )
        .map(|key| (key, db.account_id().unwrap_or_default(), db.get_device_card(&state.device_id).ok().flatten()))
    };
    match admitted {
        Ok((device_key, account_id, own_card)) => {
            tracing::info!("Paired {} ({}) into account {}", short(&request.device_id), request.device_name, short(&account_id));
            if let Ok(mut failures) = state.failures.lock() {
                failures.remove(&addr.ip());
            }
            let (certificate_fingerprint, addresses) = own_card
                .map(|c| (c.certificate_fingerprint, c.addresses))
                .unwrap_or_default();
            // The recording key travels to a device just let in (ENC-1)
            let recording_key = account.config.lock().ok().map(|c| c.recording_key_text().to_string()).filter(|k| !k.is_empty());
            Json(PairClaimResponse {
                account_id,
                device_key,
                device_id: state.device_id.clone(),
                device_name: state.device_name.clone(),
                certificate_fingerprint,
                addresses,
                recording_key,
            })
            .into_response()
        }
        Err(sentence) => {
            let delay = note_failure(&state, addr.ip());
            tracing::warn!("Refused a pairing claim from {}: {}", addr.ip(), codes::TOKEN_INVALID);
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            (StatusCode::FORBIDDEN, Json(ErrorResponse::with_code(sentence, codes::TOKEN_INVALID))).into_response()
        }
    }
}

/// Every route but the health check passes through here (AUTH-3): the
/// account must be this one, the device must have a card that is not
/// revoked, and the key must hash to the card's hash. A refusal is a
/// sentence with its code, and repeated refusals from one address are
/// answered ever more slowly (AUTH-5). The key itself is never logged.
async fn require_device(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    request: Request,
    next: Next,
) -> Response {
    let headers = request.headers();
    let account = header(headers, auth::HEADER_ACCOUNT).map(str::to_string);
    let device = header(headers, auth::HEADER_DEVICE).map(str::to_string);
    let key = bearer(headers).map(str::to_string);
    let handle = account.as_deref().filter(|a| !a.is_empty()).and_then(|a| state.accounts.account(a));
    let request_id = crate::sync_protocol::request_id_or_dash(header(headers, HEADER_REQUEST_ID));
    let span = tracing::info_span!("request", id = %request_id, device = %device.as_deref().map(short).unwrap_or("-"));
    let method = request.method().to_string();
    let path = request.uri().path().to_string();
    let bytes_in = headers
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    let verdict = match &handle {
        Some(handle) => {
            let db = handle.db.lock().unwrap();
            let own_account = db.account_id().unwrap_or_default();
            auth::verify_request(&db, &own_account, account.as_deref(), device.as_deref(), key.as_deref())
        }
        None => Err(auth::Refusal {
            status: 404,
            code: codes::ACCOUNT_UNKNOWN,
            sentence: format!("This server does not host your account ({})", codes::ACCOUNT_UNKNOWN),
        }),
    };
    match verdict {
        Ok(_) => {
            if let Ok(mut failures) = state.failures.lock() {
                failures.remove(&addr.ip());
            }
            let mut request = request;
            let handle = handle.expect("verified requests have an account");
            request.extensions_mut().insert(handle.clone());
            request.extensions_mut().insert(CallerDevice(device.clone().unwrap_or_default()));
            let response = next.run(request).instrument(span).await;
            audit_request(&state, Some(&handle), &request_id, device.as_deref(), &method, &path, bytes_in, &response, "");
            response
        }
        Err(refusal) => {
            let delay = note_failure(&state, addr.ip());
            let _entered = span.enter();
            tracing::warn!(
                "Refused {} from {} for device {}: {}",
                request.uri().path(),
                addr.ip(),
                device.as_deref().map(short).unwrap_or("-"),
                refusal.code
            );
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            let response = (
                StatusCode::from_u16(refusal.status).unwrap_or(StatusCode::UNAUTHORIZED),
                Json(ErrorResponse::with_code(refusal.sentence, refusal.code)),
            )
                .into_response();
            audit_request(&state, handle.as_ref(), &request_id, device.as_deref(), &method, &path, bytes_in, &response, refusal.code);
            response
        }
    }
}

// Route handlers

async fn handshake(
    State(state): State<AppState>,
    Extension(account): Extension<AccountHandle>,
    headers: HeaderMap,
    Json(request): Json<HandshakeRequest>,
) -> impl IntoResponse {
    tracing::debug!(
        "Handshake from device_id={}... device_name={} protocol_version={}",
        &request.device_id[..UUID_SHORT_LEN.min(request.device_id.len())],
        request.device_name,
        request.protocol_version
    );

    // Validate device_id
    if request.device_id.len() != 32 || !request.device_id.chars().all(|c| c.is_ascii_hexdigit()) {
        tracing::warn!("Invalid device_id format: {}", request.device_id);
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new("Invalid device_id format".to_string())),
        )
            .into_response();
    }

    // A peer of an older protocol is refused, in words (Stage 16)
    if crate::sync_protocol::protocol_major(&request.protocol_version).unwrap_or(0) < crate::sync_protocol::PROTOCOL_MAJOR {
        tracing::warn!("Refused device {}: protocol {} is older than {}", short(&request.device_id), request.protocol_version, PROTOCOL_VERSION);
        return (
            StatusCode::UPGRADE_REQUIRED,
            Json(ErrorResponse::with_code(
                format!("Update Voice on {} ({})", request.device_name, codes::PROTOCOL_TOO_OLD),
                codes::PROTOCOL_TOO_OLD,
            )),
        )
            .into_response();
    }

    // The body and the headers must agree about who is calling, or a device
    // could act under another's name with its own key.
    if let Some(named) = header(&headers, auth::HEADER_DEVICE) {
        if named != request.device_id {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::with_code(
                    format!("The handshake names device {} but the request was made by {} ({})", short(&request.device_id), short(named), codes::DEVICE_MISMATCH),
                    codes::DEVICE_MISMATCH,
                )),
            )
                .into_response();
        }
    }

    // The account check (ACCT-2, ACCT-3): the two sides must hold the same
    // account, or nothing is exchanged. Never adopts, never corrects.
    let own_account = {
        let db = account.db.lock().unwrap();
        db.account_id().unwrap_or_default()
    };
    if request.account_id.is_empty() {
        tracing::warn!("Handshake from {} named no account", short(&request.device_id));
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::with_code(
                format!("The handshake named no account ({})", codes::ACCOUNT_MISSING),
                codes::ACCOUNT_MISSING,
            )),
        )
            .into_response();
    }
    if request.account_id != own_account {
        tracing::warn!(
            "Refused {}: it holds account {}, this device holds {}",
            short(&request.device_id),
            short(&request.account_id),
            short(&own_account)
        );
        return (
            StatusCode::FORBIDDEN,
            Json(ErrorResponse::with_code(
                format!(
                    "This device holds account {}, not {}; nothing was exchanged ({})",
                    short(&own_account),
                    short(&request.account_id),
                    codes::ACCOUNT_MISMATCH
                ),
                codes::ACCOUNT_MISMATCH,
            )),
        )
            .into_response();
    }

    // A handshake starts a peer's operation, and what it applies afterwards
    // must be undoable (SNAP-3).
    {
        let db = account.db.lock().unwrap();
        if let Err(e) = db.snapshot_before("handshake") {
            tracing::warn!("Could not take a snapshot before the handshake: {}", e);
        }
        if let Err(e) = db.set_peer_account_id(&request.device_id, Some(&request.device_name), &request.account_id) {
            tracing::warn!("Could not record the peer's account: {}", e);
        }
    }

    // What this device holds is stated before the peer reads the feed (FILE-22):
    // a device that only serves syncs compares its folder too
    {
        let dir = account.config.lock().ok().and_then(|c| c.audiofile_directory().map(std::path::PathBuf::from));
        if let Some(dir) = dir {
            if let Err(e) = account.db.lock().unwrap().check_files_here(&dir, &state.device_id) {
                tracing::warn!("Could not compare the audio folder with the statements: {}", e);
            }
        }
    }

    // Whether recordings are served, and how much room the disk has
    let (supports_audiofiles, free_bytes) = {
        let config = account.config.lock().unwrap();
        let dir = config.audiofile_directory().map(std::path::PathBuf::from).unwrap_or_else(|| config.config_dir().to_path_buf());
        (config.audiofile_directory().is_some(), crate::transfer::free_space(&dir))
    };

    let (database_id, cursor) = {
        let db = account.db.lock().unwrap();
        (db.database_id().unwrap_or_default(), db.current_seq().unwrap_or(0))
    };

    // What the peer wants and understands (Stage 16): apply accepts only that
    if let Ok(db) = account.db.lock() {
        if let Err(e) = db.set_peer_entity_types(&request.device_id, Some(&request.device_name), &request.entity_types) {
            tracing::warn!("Could not remember the entity types of {}: {}", short(&request.device_id), e);
        }
    }

    let response = HandshakeResponse {
        device_id: state.device_id.clone(),
        device_name: state.device_name.clone(),
        protocol_version: PROTOCOL_VERSION.to_string(),
        account_id: own_account,
        application: auth::APPLICATION_VOICE.to_string(),
        server_timestamp: Utc::now().timestamp(),
        supports_audiofiles,
        free_bytes,
        database_id,
        cursor,
    };

    Json(response).into_response()
}

async fn get_changes(
    State(state): State<AppState>,
    Extension(account): Extension<AccountHandle>,
    caller: Option<Extension<CallerDevice>>,
    Query(query): Query<ChangesQuery>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(1000).min(10000);
    tracing::debug!("GET /sync/changes cursor={:?} limit={}", query.cursor, limit);

    // The cursor feed; no cursor is the start of it
    let (changes, next_cursor, is_complete, database_id) = {
        let db = account.db.lock().unwrap();
        let database_id = db.database_id().unwrap_or_default();
        // A device asking for the changes after a cursor holds everything up to
        // it: that is what this device has duplicated to it (PROOF-1, D29). A
        // phone pulling from this listener counts as much as a push to it.
        if let (Some(cursor), Some(Extension(CallerDevice(device)))) = (query.cursor, caller.as_ref()) {
            if !device.is_empty() {
                if let Err(e) = db.set_peer_cursors(device, None, None, Some(cursor), None) {
                    tracing::warn!("Could not note what {} holds: {}", device, e);
                }
            }
        }
        let result = db
            .get_changes_after_seq_as_sync_changes(query.cursor.unwrap_or(0), None, limit)
            .map(|(changes, next, complete)| (changes, Some(next), complete));
        match result {
            Ok((c, n, complete)) => (c, n, complete, database_id),
            Err(e) => {
                tracing::error!("Failed to get changes: {}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse::new(e.to_string())),
                )
                    .into_response();
            }
        }
    };

    // Only the types the caller asked for (Stage 16); the cursor still
    // walks the whole feed, so nothing is skipped for good
    let changes = match query.types.as_deref().map(|t| t.split(',').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect::<Vec<_>>()) {
        Some(wanted) if !wanted.is_empty() => changes.into_iter().filter(|c| wanted.iter().any(|w| w == &c.entity_type)).collect(),
        _ => changes,
    };

    tracing::debug!(
        "Returning {} changes, next_cursor={:?}",
        changes.len(),
        next_cursor
    );
    for change in &changes {
        tracing::trace!(
            "  {} {} {}",
            change.entity_type,
            &change.entity_id[..UUID_SHORT_LEN.min(change.entity_id.len())],
            change.operation
        );
    }

    let response = ChangesResponse {
        changes,
        next_cursor,
        database_id,
        device_id: state.device_id.clone(),
        device_name: state.device_name.clone(),
        is_complete,
    };

    Json(response).into_response()
}

async fn apply_changes(
    State(state): State<AppState>,
    Extension(account): Extension<AccountHandle>,
    Json(request): Json<ApplyRequest>,
) -> impl IntoResponse {
    tracing::debug!(
        "POST /sync/apply from device_id={}... ({} changes)",
        &request.device_id[..UUID_SHORT_LEN.min(request.device_id.len())],
        request.changes.len()
    );

    // Only the types the peer declared in its handshake (Stage 16)
    let declared = account.db.lock().ok().and_then(|db| db.peer_entity_types(&request.device_id).ok()).unwrap_or_default();
    let mut request = request;
    let mut undeclared = 0usize;
    if !declared.is_empty() {
        let before = request.changes.len();
        request.changes.retain(|c| declared.iter().any(|d| d == &c.entity_type));
        undeclared = before - request.changes.len();
        if undeclared > 0 {
            tracing::warn!("{} change(s) of types {} did not declare were not applied", undeclared, short(&request.device_id));
        }
    }
    for change in &request.changes {
        tracing::trace!(
            "  Incoming: {} {} {}",
            change.entity_type,
            &change.entity_id[..UUID_SHORT_LEN.min(change.entity_id.len())],
            change.operation
        );
    }

    // Validate device_id
    if request.device_id.len() != 32 || !request.device_id.chars().all(|c| c.is_ascii_hexdigit()) {
        tracing::warn!("Invalid device_id format: {}", request.device_id);
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new("Invalid device_id format".to_string())),
        )
            .into_response();
    }

    // Apply changes
    let (applied, conflicts, errors) = match apply_sync_changes(
        &account.db,
        &request.changes,
        &request.device_id,
        Some(request.device_name.as_str()),
        Some(&state.device_id),
        Some(&state.device_name),
    ) {
        Ok(result) => result,
        Err(e) => {
            tracing::error!("Failed to apply changes: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(e.to_string())),
            )
                .into_response();
        }
    };

    tracing::debug!(
        "Applied {} changes, {} conflicts, {} errors",
        applied, conflicts, errors.len()
    );
    for err in &errors {
        tracing::warn!("  Error: {}", err);
    }

    // Update sync_peers to track when we last synced with this peer
    if let Ok(db) = account.db.lock() {
        let _ = db.update_peer_sync_time(&request.device_id, Some(&request.device_name));
    }

    let mut errors = errors;
    if undeclared > 0 {
        errors.push(format!("{} change(s) of entity types this device did not declare in its handshake were not applied", undeclared));
    }
    let response = ApplyResponse {
        applied,
        conflicts,
        errors,
    };

    Json(response).into_response()
}

async fn status(State(state): State<AppState>) -> impl IntoResponse {
    // The health check names no account: a single-account listener answers
    // for its one account; a host answers false here and truthfully in the
    // handshake of each account.
    let supports_audiofiles = state
        .accounts
        .single()
        .map(|a| a.config.lock().map(|c| c.audiofile_directory().is_some()).unwrap_or(false))
        .unwrap_or(false);

    Json(StatusResponse {
        device_id: state.device_id.clone(),
        device_name: state.device_name.clone(),
        protocol_version: PROTOCOL_VERSION.to_string(),
        status: "ok".to_string(),
        supports_audiofiles,
    })
}

/// Where a recording's file is, from its row, or None if the row does not
/// exist or no audio directory is configured.
fn audio_path_for(account: &AccountHandle, audio_id: &str, for_writing: bool) -> Result<std::path::PathBuf, (StatusCode, String)> {
    Uuid::parse_str(audio_id).map_err(|_| (StatusCode::BAD_REQUEST, "Invalid audio ID".to_string()))?;
    let audiofile_dir = {
        let config = account.config.lock().map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "Config lock error".to_string()))?;
        config.audiofile_directory().map(|s| s.to_string())
    }
    .ok_or_else(|| (StatusCode::BAD_REQUEST, "audiofile_directory not configured".to_string()))?;
    let dir = std::path::Path::new(&audiofile_dir);
    let db = account.db.lock().map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "Database lock error".to_string()))?;
    let found = |db: &Database| {
        db.get_audio_file(audio_id)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Database error: {}", e)))?
            .ok_or_else(|| (StatusCode::NOT_FOUND, format!("Audio file record not found: {}", audio_id)))
    };
    found(&db)?;
    // Names changed by a sync reach the disk before the file is read or written (FILE-15)
    if for_writing {
        return db.disk_path_for_writing(audio_id, dir).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()));
    }
    if let Err(e) = db.apply_pending_file_renames(dir) {
        tracing::warn!("Recording names were not all settled on disk: {}", e);
    }
    Ok(audio_local_path(dir, &found(&db)?.disk_name))
}

/// `GET /sync/audio/:id/file`: stream one recording to a fetching peer
/// (FILE-12), from the byte a `Range: bytes=N-` header asks for, with the
/// whole file's size and SHA-256 in headers so the peer can verify.
async fn serve_audio_file(
    Extension(account): Extension<AccountHandle>,
    Path(audio_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, (StatusCode, String)> {
    tracing::debug!("GET /sync/audio/{}/file", short(&audio_id));
    let file_path = audio_path_for(&account, &audio_id, false)?;
    if !file_path.is_file() {
        return Err((StatusCode::NOT_FOUND, format!("Audio file not found: {}", audio_id)));
    }
    let total = std::fs::metadata(&file_path).map(|m| m.len()).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let start = header(&headers, "range").and_then(crate::transfer::parse_range_start).unwrap_or(0);
    if start > total {
        return Err((StatusCode::RANGE_NOT_SATISFIABLE, format!("The file is {} bytes", total)));
    }
    // A file kept as the bucket holds it, by a device without the key (ENC-4):
    // served as it is, its own hash, and a header that says so
    let encrypted = crate::crypto::file_is_encrypted(&file_path);
    // The row's hash (Stage 13), computed and stored once when it is missing
    let hash = if encrypted {
        crate::transfer::file_sha256(&file_path).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    } else {
        let db = account.db.lock().map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "Database lock error".to_string()))?;
        match db.get_audio_file(&audio_id).ok().flatten().and_then(|r| r.content_sha256) {
            Some(h) => h,
            None => {
                let h = crate::transfer::file_sha256(&file_path).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                let _ = db.set_content_hash(&audio_id, &h);
                h
            }
        }
    };
    let mut file = tokio::fs::File::open(&file_path).await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if start > 0 {
        use tokio::io::AsyncSeekExt;
        file.seek(std::io::SeekFrom::Start(start)).await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }
    let stream = tokio_util::io::ReaderStream::with_capacity(file, crate::transfer::CHUNK);
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = if start > 0 { StatusCode::PARTIAL_CONTENT } else { StatusCode::OK };
    let h = response.headers_mut();
    h.insert("content-type", "application/octet-stream".parse().unwrap());
    h.insert("content-length", (total - start).to_string().parse().unwrap());
    h.insert("accept-ranges", "bytes".parse().unwrap());
    h.insert(HEADER_FILE_SHA256, hash.parse().unwrap());
    if encrypted {
        h.insert(crate::sync_protocol::HEADER_ENCRYPTED, "1".parse().unwrap());
    }
    if start > 0 {
        h.insert("content-range", format!("bytes {}-{}/{}", start, total - 1, total).parse().unwrap());
    }
    Ok(response)
}

/// `POST /sync/audio/:id/file`: receive one recording sent by a peer
/// (FILE-12), streamed into `<file>.part`, continuing from the part's
/// length when a `Content-Range: bytes N-M/total` says so, verified by the
/// `X-File-SHA256` header before the rename (FILE-13).
async fn receive_audio_file(
    State(state): State<AppState>,
    Extension(account): Extension<AccountHandle>,
    Extension(caller): Extension<CallerDevice>,
    Path(audio_id): Path<String>,
    headers: HeaderMap,
    request: Request,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    use futures_util::StreamExt;
    use tokio::io::AsyncWriteExt;

    let file_path = audio_path_for(&account, &audio_id, true)?;
    let content_length: Option<u64> = header(&headers, "content-length").and_then(|v| v.parse().ok());
    let (start, total) = match header(&headers, "content-range").and_then(crate::transfer::parse_content_range) {
        Some((start, total)) => (start, total),
        None => (0, content_length.unwrap_or(0)),
    };
    let expected_hash = header(&headers, HEADER_FILE_SHA256).map(str::to_string);
    tracing::debug!("POST /sync/audio/{}/file from byte {} of {}", short(&audio_id), start, total);

    let dir = file_path.parent().unwrap_or_else(|| std::path::Path::new("."));
    std::fs::create_dir_all(dir).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to create directory: {}", e)))?;
    if start == 0 {
        crate::transfer::check_free_space(dir, total).map_err(|e| (StatusCode::INSUFFICIENT_STORAGE, e.to_string()))?;
    }
    let part = crate::transfer::part_path(&file_path);
    let have = crate::transfer::part_len(&file_path);
    if start != have {
        return Err((StatusCode::CONFLICT, format!("This device holds {} bytes of the file, not {}", have, start)));
    }
    let mut out = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&part)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to open the part file: {}", e)))?;
    let mut stream = request.into_body().into_data_stream();
    let mut written = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(e) => {
                // What arrived stays in the part, for the sender's next try to continue from
                let _ = out.flush().await;
                return Err((StatusCode::BAD_REQUEST, format!("The transfer stopped: {}", e)));
            }
        };
        out.write_all(&chunk).await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to write file: {}", e)))?;
        written += chunk.len() as u64;
    }
    out.flush().await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    drop(out);
    let now_have = start + written;
    if total == 0 || now_have < total {
        // Either the sender did not say the size, or this was a part of it.
        if total == 0 {
            crate::transfer::complete(&file_path, now_have, expected_hash.as_deref())
                .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
            note_here(&account, &audio_id, &state.device_id);
            note_copy(&account, &audio_id, &caller.0);
            return Ok((StatusCode::OK, "OK"));
        }
        return Ok((StatusCode::ACCEPTED, "PART"));
    }
    crate::transfer::complete(&file_path, total, expected_hash.as_deref())
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    tracing::info!("Received audio file {} ({} bytes)", short(&audio_id), total);
    note_here(&account, &audio_id, &state.device_id);
    note_copy(&account, &audio_id, &caller.0);
    Ok((StatusCode::OK, "OK"))
}

/// This device holds a file it received whole (FILE-22).
fn note_here(account: &AccountHandle, audio_id: &str, device_id: &str) {
    if let Err(e) = account.db.lock().unwrap().set_file_location(audio_id, device_id, true) {
        tracing::warn!("Could not record that this device holds {}: {}", short(audio_id), e);
    }
}

/// The sender of a whole file holds it (Stage 10).
fn note_copy(account: &AccountHandle, audio_id: &str, device_id: &str) {
    if device_id.is_empty() {
        return;
    }
    if let Err(e) = account.db.lock().unwrap().record_copy(audio_id, device_id) {
        tracing::warn!("Could not record that {} holds {}: {}", short(device_id), short(audio_id), e);
    }
}

/// `POST /sync/audio/:id/keep` (FILE-26): the caller is removing its copy of a
/// recording and asks this device to promise to keep its own meanwhile.
async fn keep_audio_file(
    State(state): State<AppState>,
    Extension(account): Extension<AccountHandle>,
    Extension(caller): Extension<CallerDevice>,
    Path(audio_id): Path<String>,
) -> Result<Json<crate::sync_protocol::KeepResponse>, (StatusCode, String)> {
    use crate::sync_protocol::KeepResponse;
    Uuid::parse_str(&audio_id).map_err(|_| (StatusCode::BAD_REQUEST, "Invalid audio ID".to_string()))?;
    let dir = account
        .config
        .lock()
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "Config lock error".to_string()))?
        .audiofile_directory()
        .map(std::path::PathBuf::from);
    let Some(dir) = dir else {
        return Ok(Json(KeepResponse { holds: false, until_ms: 0, reason: "no audio folder is set on this device".to_string() }));
    };
    let db = account.db.lock().map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "Database lock error".to_string()))?;
    if let Err(e) = db.apply_pending_file_renames(&dir) {
        tracing::warn!("Recording names were not all settled on disk: {}", e);
    }
    match db.promise_to_keep(&audio_id, &caller.0, &dir, &state.device_id) {
        Ok(Ok(until_ms)) => {
            tracing::info!("Promised {} to keep {} until {}", short(&caller.0), short(&audio_id), until_ms);
            Ok(Json(KeepResponse { holds: true, until_ms, reason: String::new() }))
        }
        Ok(Err(reason)) => Ok(Json(KeepResponse { holds: false, until_ms: 0, reason })),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

/// `POST /sync/audio/missing` (FILE-12): of the ids a sender holds, which
/// this device lacks, and how many bytes of each it already has in a part.
async fn missing_audio_files(
    Extension(account): Extension<AccountHandle>,
    Extension(caller): Extension<CallerDevice>,
    Json(request): Json<MissingFilesRequest>,
) -> Result<Json<MissingFilesResponse>, (StatusCode, String)> {
    let mut missing = Vec::new();
    let mut partial = std::collections::HashMap::new();
    for audio_id in request.audio_ids {
        let path = match audio_path_for(&account, &audio_id, false) {
            Ok(p) => p,
            Err((StatusCode::NOT_FOUND, _)) => {
                // No row yet: the sender's sync has not reached us; ask for it next time
                continue;
            }
            Err(e) => return Err(e),
        };
        // The sender said it holds this one (Stage 10)
        note_copy(&account, &audio_id, &caller.0);
        if path.is_file() {
            continue;
        }
        let have = crate::transfer::part_len(&path);
        if have > 0 {
            partial.insert(audio_id.clone(), have);
        }
        missing.push(audio_id);
    }
    Ok(Json(MissingFilesResponse { missing, partial }))
}

fn apply_sync_changes(
    db: &Arc<Mutex<Database>>,
    changes: &[SyncChange],
    peer_device_id: &str,
    peer_device_name: Option<&str>,
    _local_device_id: Option<&str>,
    _local_device_name: Option<&str>,
) -> VoiceResult<(i64, i64, Vec<String>)> {
    let db = db.lock().unwrap();
    let sync_received_at = Utc::now().timestamp();
    let outcome = crate::sync_apply::apply_changes(&db, changes, peer_device_id, peer_device_name, sync_received_at)?;
    if outcome.retried_ok > 0 {
        tracing::info!("Applied {} previously failed changes", outcome.retried_ok);
    }
    // Update peer's last sync timestamp
    db.update_peer_sync_time(peer_device_id, peer_device_name)?;
    Ok((outcome.applied, outcome.conflicts, outcome.errors))
}

/// Apply sync changes from a peer to the local database.
///
/// This is the public API for applying changes, suitable for testing.
/// Returns (applied_count, conflict_count, errors).
pub fn apply_changes_from_peer(
    db: &Database,
    changes: &[SyncChange],
    peer_device_id: &str,
    peer_device_name: Option<&str>,
    _local_device_id: Option<&str>,
    _local_device_name: Option<&str>,
) -> VoiceResult<(i64, i64, Vec<String>)> {
    let sync_received_at = Utc::now().timestamp();
    let outcome = crate::sync_apply::apply_changes(db, changes, peer_device_id, peer_device_name, sync_received_at)?;
    db.update_peer_sync_time(peer_device_id, peer_device_name)?;
    Ok((outcome.applied, outcome.conflicts, outcome.errors))
}

/// Create the sync server router
pub fn create_router(
    db: Arc<Mutex<Database>>,
    config: Arc<Mutex<Config>>,
) -> Router {
    let account_id = db.lock().unwrap().account_id().unwrap_or_default();
    let source: Arc<dyn AccountSource> = Arc::new(SingleAccount { account_id, handle: AccountHandle { db, config: config.clone() } });
    create_router_for(source, config)
}

/// The router over any set of accounts; `machine` is the machine's config
/// (the device identity, the body limit).
pub fn create_router_for(accounts: Arc<dyn AccountSource>, machine: Arc<Mutex<Config>>) -> Router {
    let (device_id, device_name, max_body_size) = {
        let cfg = machine.lock().unwrap();
        (
            cfg.device_id_hex().to_string(),
            cfg.device_name().to_string(),
            cfg.max_sync_file_size_bytes() as usize,
        )
    };

    let lan_only = machine.lock().unwrap().public_url().trim().is_empty();
    let state = AppState {
        accounts,
        device_id,
        device_name,
        lan_only,
        failures: Arc::new(Mutex::new(HashMap::new())),
    };

    tracing::info!(
        "Sync server body limit: {} MB",
        max_body_size / 1024 / 1024
    );

    let authenticated = Router::new()
        .route("/sync/handshake", post(handshake))
        // The feed is compressed when the caller accepts it (Stage 12); the
        // file routes never are, a recording is compressed already
        .route("/sync/changes", get(get_changes).layer(tower_http::compression::CompressionLayer::new().gzip(true)))
        .route("/sync/apply", post(apply_changes))
        .route("/sync/audio/missing", post(missing_audio_files))
        .route("/sync/audio/:audio_id/file", get(serve_audio_file))
        .route("/sync/audio/:audio_id/file", post(receive_audio_file))
        .route("/sync/audio/:audio_id/keep", post(keep_audio_file))
        .route_layer(middleware::from_fn_with_state(state.clone(), require_device));

    Router::new()
        .route("/sync/status", get(status))
        .route("/pair/claim", post(pair_claim))
        .route("/pair/grant", post(pair_grant))
        // Every route, the open ones too: a phone on hotel wifi is not a
        // server for the hotel (LISTEN-3)
        .route_layer(middleware::from_fn_with_state(state.clone(), lan_only_gate))
        .merge(authenticated)
        // The limit applies to the JSON routes; a recording streams past it
        // (FILE-12) because the file route reads its body as a stream
        .layer(DefaultBodyLimit::max(max_body_size))
        .with_state(state)
}

/// Where a listener is reachable, for its own card, for a code and for a
/// person typing the address: `https://<host>:<port>`. A listener bound to
/// one address reports that address; one bound to every address reports
/// this machine's host name.
pub fn listen_urls(host: &str, port: u16, plain_http: bool) -> Vec<String> {
    let scheme = if plain_http { "http" } else { "https" };
    if host != "0.0.0.0" && host != "::" && !host.is_empty() {
        return vec![format!("{}://{}:{}", scheme, host, port)];
    }
    let mut urls: Vec<String> = if_addrs::get_if_addrs()
        .map(|ifs| {
            ifs.into_iter()
                .filter(|i| !i.is_loopback())
                .filter_map(|i| match i.ip() {
                    std::net::IpAddr::V4(v4) if v4.is_private() || v4.is_link_local() => Some(format!("{}://{}:{}", scheme, v4, port)),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default();
    urls.sort();
    if let Some(name) = hostname_of_this_machine() {
        urls.push(format!("{}://{}:{}", scheme, name, port));
    }
    urls
}

/// Say on this device's own card that it listens at `urls`, or that it
/// stopped (CARD-1 `listens`, `addresses`).
pub fn record_listening(db: &Database, config: &mut Config, urls: &[String], listening: bool) -> VoiceResult<()> {
    let mut card = crate::auth::ensure_own_device_card(db, config)?;
    card.listens = if listening { "1" } else { "0" }.to_string();
    card.addresses = if listening { serde_json::to_string(urls).unwrap_or_default() } else { card.addresses };
    db.write_device_card(&card)
}

#[cfg(feature = "desktop")]
fn hostname_of_this_machine() -> Option<String> {
    hostname::get().ok().map(|h| h.to_string_lossy().to_string()).filter(|h| !h.is_empty())
}

#[cfg(not(feature = "desktop"))]
fn hostname_of_this_machine() -> Option<String> {
    None
}

/// Start the sync server: HTTPS with this device's own certificate (made
/// under `certs/` if missing), or plain HTTP when `plain_http` is set, which
/// is allowed only on a loopback address, for a reverse proxy in front or a
/// test on this machine (AUTH-7). The listener's certificate fingerprint is
/// written to its own device card so peers can pin it from the card.
pub async fn start_server(
    db: Arc<Mutex<Database>>,
    config: Arc<Mutex<Config>>,
    host: &str,
    port: u16,
    plain_http: bool,
) -> VoiceResult<()> {
    let account_id = db.lock().unwrap().account_id()?;
    let source: Arc<dyn AccountSource> = Arc::new(SingleAccount { account_id, handle: AccountHandle { db, config: config.clone() } });
    start_server_for(source, config, host, port, plain_http).await
}

/// Serve every account of an indexed root (Stage 3, hosting): the machine's
/// identity and certificate come from the root's own `config.json`, and each
/// account is opened on its first request. A root without an index gets one,
/// empty; no default account is made.
pub async fn start_hosting_server(root: &std::path::Path, host: &str, port: u16, plain_http: bool) -> VoiceResult<()> {
    crate::accounts::AccountIndex::open(root)?;
    let machine = Arc::new(Mutex::new(Config::new(Some(root.to_path_buf()), None)?));
    let urls = listen_urls(host, port, plain_http);
    let source: Arc<dyn AccountSource> = Arc::new(IndexedAccounts::new(root, urls));
    start_server_for(source, machine, host, port, plain_http).await
}

/// The listener over any set of accounts; `machine` holds the device
/// identity, the certificate and the body limit.
pub async fn start_server_for(
    accounts: Arc<dyn AccountSource>,
    machine: Arc<Mutex<Config>>,
    host: &str,
    port: u16,
    plain_http: bool,
) -> VoiceResult<()> {
    let addr: SocketAddr = format!("{}:{}", host, port)
        .parse()
        .map_err(|e| crate::error::VoiceError::Network(format!("{} is not an address: {}", host, e)))?;
    if plain_http && !addr.ip().is_loopback() {
        return Err(crate::error::VoiceError::Network(format!(
            "Plain http is allowed only on this machine itself, not on {} ({})",
            host,
            codes::TLS_REQUIRED
        )));
    }

    let tls = if plain_http {
        None
    } else {
        let (cert_path, key_path, fingerprint) = {
            let cfg = machine.lock().unwrap();
            crate::tls::ensure_server_certificate(&cfg, false)?
        };
        tracing::info!("Certificate fingerprint {}", fingerprint);
        Some(crate::tls::server_config(&cert_path, &key_path)?)
    };
    let urls = listen_urls(host, port, plain_http);
    for handle in accounts.open_handles() {
        let db_guard = handle.db.lock().unwrap();
        let mut cfg = handle.config.lock().unwrap();
        record_listening(&db_guard, &mut cfg, &urls, true)?;
    }
    let accounts_for_stop = accounts.clone();

    // The periodic backup of every open account (SNAP-5), for as long as
    // the listener runs; 0 hours turns it off
    let backup = machine.lock().unwrap().backup().clone();
    let backup_task = if backup.interval_hours > 0 {
        Some(spawn_periodic_backup(accounts.clone(), Duration::from_secs(u64::from(backup.interval_hours) * 3600), backup.keep as usize))
    } else {
        None
    };

    let router = create_router_for(accounts, machine).into_make_service_with_connect_info::<SocketAddr>();

    // Create shutdown channel; a previous listener's handle, if any, is dropped
    let (tx, rx) = oneshot::channel::<()>();
    *SHUTDOWN_TX.lock().unwrap() = Some(tx);
    let handle = axum_server::Handle::new();
    let stopper = handle.clone();
    tokio::spawn(async move {
        rx.await.ok();
        stopper.graceful_shutdown(Some(Duration::from_secs(5)));
    });

    tracing::info!("Starting sync server on {} ({})", addr, if plain_http { "plain http" } else { "https" });
    note_activity();

    let served = match tls {
        Some(server_config) => {
            let rustls = axum_server::tls_rustls::RustlsConfig::from_config(server_config);
            axum_server::bind_rustls(addr, rustls).handle(handle).serve(router).await
        }
        None => axum_server::bind(addr).handle(handle).serve(router).await,
    };
    *SHUTDOWN_TX.lock().unwrap() = None;
    if let Some(task) = backup_task {
        task.abort();
    }
    for handle in accounts_for_stop.open_handles() {
        let db_guard = handle.db.lock().unwrap();
        let mut cfg = handle.config.lock().unwrap();
        if let Err(e) = record_listening(&db_guard, &mut cfg, &[], false) {
            tracing::warn!("Could not record that the listener stopped: {}", e);
        }
    }
    served.map_err(|e| crate::error::VoiceError::Network(e.to_string()))?;
    Ok(())
}

/// Copy every open account's database to its backup directory, keeping the
/// newest `keep` (SNAP-5). Returns the copies made, with the accounts that
/// could not be copied as sentences.
pub fn backup_open_accounts(accounts: &dyn AccountSource, keep: usize) -> (Vec<std::path::PathBuf>, Vec<String>) {
    let mut made = Vec::new();
    let mut failed = Vec::new();
    for handle in accounts.open_handles() {
        let db = handle.db.lock().unwrap();
        let config = handle.config.lock().unwrap();
        let account = db.account_id().unwrap_or_default();
        let dir = config.backup_directory(&account);
        match db.backup_to(&dir, keep) {
            Ok(path) => {
                tracing::info!("Backed up account {} to {}", short(&account), path.display());
                made.push(path);
            }
            Err(e) => failed.push(format!("Account {} was not backed up: {}", short(&account), e)),
        }
    }
    (made, failed)
}

/// The periodic backup task (SNAP-5): every `interval`, every open account.
/// The first copy is made one interval after the start, not at once, so a
/// listener restarted often does not fill the directory.
pub fn spawn_periodic_backup(accounts: Arc<dyn AccountSource>, interval: Duration, keep: usize) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            let (_, failed) = backup_open_accounts(accounts.as_ref(), keep);
            for sentence in failed {
                tracing::warn!("{}", sentence);
            }
        }
    })
}

/// Whether an account's periodic backup is due: no copy yet, or the newest
/// older than the interval.
pub fn backup_due(config: &Config, account_id: &str) -> bool {
    let backup = config.backup();
    if backup.interval_hours == 0 {
        return false;
    }
    let dir = config.backup_directory(account_id);
    let newest = Database::backups_in(&dir).ok().and_then(|c| c.into_iter().next());
    match newest.and_then(|p| std::fs::metadata(p).ok()).and_then(|m| m.modified().ok()) {
        Some(modified) => modified.elapsed().map(|age| age >= Duration::from_secs(u64::from(backup.interval_hours) * 3600)).unwrap_or(true),
        None => true,
    }
}

/// Stop the sync server
pub fn stop_server() {
    if let Ok(mut guard) = SHUTDOWN_TX.lock() {
        if let Some(tx) = guard.take() {
            let _ = tx.send(());
        }
    }
}

/// Whether a listener is running in this process.
pub fn server_running() -> bool {
    SHUTDOWN_TX.lock().map(|g| g.is_some()).unwrap_or(false)
}

// ============================================================================
// Tests - CRITICAL: Verify zero data loss in sync
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;

    use crate::sync_apply::ALL_SYNC_ENTITY_TYPES;

    /// Create a test database in a temporary directory
    fn create_test_db() -> (Database, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = Database::new(db_path.to_str().unwrap()).unwrap();
        (db, temp_dir)
    }

    mod account_identity {
        use super::*;
        use axum::extract::{Json, State};
        use axum::http::StatusCode;
        use axum::response::IntoResponse;
        use crate::config::Config;
        use crate::sync_client::SyncClient;

        fn state_for(db: Database, dir: &TempDir) -> (AppState, AccountHandle) {
            let config = Config::new(Some(dir.path().to_path_buf()), None).unwrap();
            let device_id = config.device_id_hex().to_string();
            let account_id = db.account_id().unwrap();
            let handle = AccountHandle { db: Arc::new(Mutex::new(db)), config: Arc::new(Mutex::new(config)) };
            let state = AppState {
                accounts: Arc::new(SingleAccount { account_id, handle: handle.clone() }),
                device_id,
                device_name: "Server".to_string(),
                lan_only: true,
                failures: Arc::new(Mutex::new(HashMap::new())),
            };
            (state, handle)
        }

        fn request(account_id: &str) -> HandshakeRequest {
            HandshakeRequest {
                device_id: "00000000000070008000000000000099".to_string(),
                device_name: "Phone".to_string(),
                protocol_version: PROTOCOL_VERSION.to_string(),
                account_id: account_id.to_string(),
                application: String::new(),
                entity_types: Vec::new(),
            }
        }

        async fn body_of(response: axum::response::Response) -> (StatusCode, serde_json::Value) {
            let status = response.status();
            let bytes = axum::body::to_bytes(response.into_body(), 1 << 20).await.unwrap();
            (status, serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null))
        }

        #[tokio::test]
        async fn the_same_account_is_let_in_and_told_the_account() {
            let (db, dir) = create_test_db();
            let account = db.account_id().unwrap();
            let (state, handle) = state_for(db, &dir);
            let (status, body) = body_of(handshake(State(state), Extension(handle.clone()), HeaderMap::new(), Json(request(&account))).await.into_response()).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body["account_id"], account);
            let db = handle.db.lock().unwrap();
            assert_eq!(db.get_peer_account_id("00000000000070008000000000000099").unwrap(), Some(account));
            assert_eq!(db.list_snapshots().unwrap().len(), 1, "a snapshot before the peer's operation");
        }

        #[tokio::test]
        async fn another_account_is_refused_with_its_code() {
            let (db, dir) = create_test_db();
            let (state, handle) = state_for(db, &dir);
            let other = "0199bbbbbbbb7000800000000000000b";
            let (status, body) = body_of(handshake(State(state), Extension(handle.clone()), HeaderMap::new(), Json(request(other))).await.into_response()).await;
            assert_eq!(status, StatusCode::FORBIDDEN);
            assert_eq!(body["code"], codes::ACCOUNT_MISMATCH);
            assert!(body["error"].as_str().unwrap().contains("nothing was exchanged"));
            let db = handle.db.lock().unwrap();
            assert_eq!(db.get_peer_account_id("00000000000070008000000000000099").unwrap(), None);
            assert!(db.list_snapshots().unwrap().is_empty());
        }

        #[tokio::test]
        async fn a_handshake_that_names_no_account_is_refused() {
            let (db, dir) = create_test_db();
            let (state, handle) = state_for(db, &dir);
            let (status, body) = body_of(handshake(State(state), Extension(handle), HeaderMap::new(), Json(request(""))).await.into_response()).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(body["code"], codes::ACCOUNT_MISSING);
        }

        /// The whole path, over a real socket: a device of one account syncs
        /// with a device of another, and nothing crosses in either direction.
        #[tokio::test]
        async fn a_mismatched_pair_exchanges_nothing() {
            let server_dir = TempDir::new().unwrap();
            let server_db = Database::new(server_dir.path().join("notes.db")).unwrap();
            server_db.create_note("של השרת").unwrap();
            let server_state_db = Arc::new(Mutex::new(server_db));
            let server_config = Arc::new(Mutex::new(Config::new(Some(server_dir.path().to_path_buf()), None).unwrap()));
            let server_device = server_config.lock().unwrap().device_id_hex().to_string();
            let router = create_router(server_state_db.clone(), server_config);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let serving = tokio::spawn(async move {
                axum::serve(listener, router.into_make_service_with_connect_info::<SocketAddr>()).await.unwrap()
            });

            let client_dir = TempDir::new().unwrap();
            let client_db = Database::new(client_dir.path().join("notes.db")).unwrap();
            client_db.create_note("של הלקוח").unwrap();
            let client_db = Arc::new(Mutex::new(client_db));
            let mut client_config = Config::new(Some(client_dir.path().to_path_buf()), None).unwrap();
            client_config.add_peer(&server_device, "Server", &url, None, true).unwrap();
            let client = SyncClient::new(client_db.clone(), Arc::new(Mutex::new(client_config))).unwrap();

            let result = client.sync_with_peer(&server_device).await;

            assert!(!result.success);
            // The headers name an account the server does not hold, so the
            // refusal comes before the handshake body is even read (AUTH-3).
            assert!(result.errors.iter().any(|e| e.contains(codes::ACCOUNT_UNKNOWN)), "{:?}", result.errors);
            assert_eq!(result.pulled, 0);
            assert_eq!(result.pushed, 0);
            assert_eq!(server_state_db.lock().unwrap().get_all_notes().unwrap().len(), 1, "the server kept only its own note");
            assert_eq!(client_db.lock().unwrap().get_all_notes().unwrap().len(), 1, "the client kept only its own note");
            assert_eq!(client_db.lock().unwrap().get_peer_cursors(&server_device).unwrap(), (0, 0, None));
            serving.abort();
        }
    }

    mod authentication {
        use super::*;
        use crate::auth;
        use crate::config::Config;
        use crate::sync_client::SyncClient;
        use crate::sync_protocol::codes;

        /// A device with its own database, config and key.
        struct Device {
            db: Arc<Mutex<Database>>,
            config: Arc<Mutex<Config>>,
            id: String,
            _dir: TempDir,
        }

        fn device(name: &str) -> Device {
            let dir = TempDir::new().unwrap();
            let db = Database::new(dir.path().join("notes.db")).unwrap();
            let mut config = Config::new(Some(dir.path().to_path_buf()), None).unwrap();
            config.set_device_name(name).unwrap();
            auth::ensure_own_device_card(&db, &mut config).unwrap();
            let id = config.device_id_hex().to_string();
            Device { db: Arc::new(Mutex::new(db)), config: Arc::new(Mutex::new(config)), id, _dir: dir }
        }

        /// Let `caller` into `server`'s account: its card, with its key hash.
        fn admit(server: &Device, caller: &Device) {
            let card = caller.db.lock().unwrap().get_device_card(&caller.id).unwrap().unwrap();
            server.db.lock().unwrap().admit_device_card(&card).unwrap();
        }

        /// Serve a device on this machine, plain or with its own certificate.
        /// Returns the URL, the fingerprint (TLS only) and the task.
        fn serve(server: &Device, tls: bool) -> (String, String, tokio::task::JoinHandle<()>) {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let port = listener.local_addr().unwrap().port();
            let router = create_router(server.db.clone(), server.config.clone())
                .into_make_service_with_connect_info::<SocketAddr>();
            if tls {
                let (cert, key, fingerprint) = {
                    let cfg = server.config.lock().unwrap();
                    crate::tls::ensure_server_certificate(&cfg, false).unwrap()
                };
                let config = axum_server::tls_rustls::RustlsConfig::from_config(crate::tls::server_config(&cert, &key).unwrap());
                let task = tokio::spawn(async move {
                    axum_server::from_tcp_rustls(listener, config).serve(router).await.unwrap();
                });
                (format!("https://127.0.0.1:{}", port), fingerprint, task)
            } else {
                let task = tokio::spawn(async move {
                    axum_server::from_tcp(listener).serve(router).await.unwrap();
                });
                (format!("http://127.0.0.1:{}", port), String::new(), task)
            }
        }

        fn client(caller: &Device, server: &Device, url: &str, pin: Option<&str>) -> SyncClient {
            caller.config.lock().unwrap().add_peer(&server.id, "Server", url, pin, true).unwrap();
            SyncClient::new(caller.db.clone(), caller.config.clone()).unwrap()
        }

        /// Make the two devices one account, as pairing will.
        fn same_account(a: &Device, b: &Device) {
            let account = a.db.lock().unwrap().account_id().unwrap();
            b.db.lock().unwrap().move_to_account(&account).unwrap();
        }

        #[tokio::test]
        async fn the_health_check_is_open_and_everything_else_needs_a_key() {
            let server = device("Server");
            let (url, _, task) = serve(&server, false);
            let http = reqwest::Client::new();

            let status = http.get(format!("{}/sync/status", url)).send().await.unwrap();
            assert_eq!(status.status(), 200);

            let bare = http.get(format!("{}/sync/changes", url)).send().await.unwrap();
            assert_eq!(bare.status(), 404, "no account named");
            let body: serde_json::Value = bare.json().await.unwrap();
            assert_eq!(body["code"], codes::ACCOUNT_UNKNOWN);

            let account = server.db.lock().unwrap().account_id().unwrap();
            let no_key = http
                .get(format!("{}/sync/changes", url))
                .header(auth::HEADER_ACCOUNT, &account)
                .header(auth::HEADER_DEVICE, "00000000000070008000000000000099")
                .send()
                .await
                .unwrap();
            assert_eq!(no_key.status(), 401);
            let body: serde_json::Value = no_key.json().await.unwrap();
            assert_eq!(body["code"], codes::KEY_MISSING);
            task.abort();
        }

        #[tokio::test]
        async fn a_paired_device_syncs_and_an_unpaired_or_revoked_one_is_refused() {
            let server = device("Server");
            let phone = device("Phone");
            let stranger = device("Stranger");
            same_account(&server, &phone);
            same_account(&server, &stranger);
            admit(&server, &phone);
            server.db.lock().unwrap().create_note("על השרת").unwrap();
            let (url, _, task) = serve(&server, false);

            let result = client(&phone, &server, &url, None).sync_with_peer(&server.id).await;
            assert!(result.success, "{:?}", result.errors);
            assert_eq!(phone.db.lock().unwrap().get_all_notes().unwrap().len(), 1);

            let result = client(&stranger, &server, &url, None).sync_with_peer(&server.id).await;
            assert!(!result.success);
            assert!(result.errors.iter().any(|e| e.contains(codes::DEVICE_UNKNOWN)), "{:?}", result.errors);
            assert!(stranger.db.lock().unwrap().get_all_notes().unwrap().is_empty());

            server.db.lock().unwrap().revoke_device(&phone.id).unwrap();
            let result = client(&phone, &server, &url, None).sync_with_peer(&server.id).await;
            assert!(!result.success);
            assert!(result.errors.iter().any(|e| e.contains(codes::DEVICE_REVOKED)), "{:?}", result.errors);
            task.abort();
        }

        #[tokio::test]
        async fn a_wrong_key_is_refused_and_repeated_refusals_are_answered_slowly() {
            let server = device("Server");
            let phone = device("Phone");
            same_account(&server, &phone);
            admit(&server, &phone);
            let (url, _, task) = serve(&server, false);
            let account = server.db.lock().unwrap().account_id().unwrap();
            let http = reqwest::Client::new();
            let mut elapsed = Vec::new();
            for _ in 0..(FREE_FAILURES + 1) {
                let started = Instant::now();
                let resp = http
                    .get(format!("{}/sync/changes", url))
                    .header(auth::HEADER_ACCOUNT, &account)
                    .header(auth::HEADER_DEVICE, &phone.id)
                    .bearer_auth("not-the-key")
                    .send()
                    .await
                    .unwrap();
                elapsed.push(started.elapsed());
                assert_eq!(resp.status(), 401);
                let body: serde_json::Value = resp.json().await.unwrap();
                assert_eq!(body["code"], codes::KEY_WRONG);
            }
            assert!(elapsed[0] < Duration::from_millis(500), "the first refusals are immediate");
            assert!(elapsed[FREE_FAILURES as usize] >= Duration::from_secs(1), "the fourth waits: {:?}", elapsed);
            task.abort();
        }

        #[tokio::test]
        async fn a_handshake_must_name_the_device_that_makes_it() {
            let server = device("Server");
            let phone = device("Phone");
            same_account(&server, &phone);
            admit(&server, &phone);
            let (url, _, task) = serve(&server, false);
            let account = server.db.lock().unwrap().account_id().unwrap();
            let key = phone.config.lock().unwrap().device_key().to_string();
            let resp = reqwest::Client::new()
                .post(format!("{}/sync/handshake", url))
                .header(auth::HEADER_ACCOUNT, &account)
                .header(auth::HEADER_DEVICE, &phone.id)
                .bearer_auth(&key)
                .json(&HandshakeRequest {
                    device_id: "00000000000070008000000000000099".to_string(),
                    device_name: "Someone else".to_string(),
                    protocol_version: PROTOCOL_VERSION.to_string(),
                    account_id: account.clone(),
                    application: String::new(),
                    entity_types: Vec::new(),
                })
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 400);
            let body: serde_json::Value = resp.json().await.unwrap();
            assert_eq!(body["code"], codes::DEVICE_MISMATCH);
            task.abort();
        }

        #[tokio::test]
        async fn tls_is_verified_by_the_pin_and_never_off() {
            let server = device("Server");
            let phone = device("Phone");
            same_account(&server, &phone);
            admit(&server, &phone);
            server.db.lock().unwrap().create_note("מוצפן").unwrap();
            let (url, fingerprint, task) = serve(&server, true);
            assert!(fingerprint.starts_with("SHA256:"));

            let result = client(&phone, &server, &url, Some(&fingerprint)).sync_with_peer(&server.id).await;
            assert!(result.success, "{:?}", result.errors);
            assert_eq!(phone.db.lock().unwrap().get_all_notes().unwrap().len(), 1);

            let wrong = fingerprint.replace(|c: char| c.is_ascii_hexdigit(), "0");
            let result = client(&phone, &server, &url, Some(&wrong)).sync_with_peer(&server.id).await;
            assert!(!result.success);
            assert!(result.errors.iter().any(|e| e.contains(codes::CERTIFICATE_MISMATCH)), "{:?}", result.errors);

            // No pin: the self-signed certificate is checked against the
            // system roots and fails, because verification is never off.
            phone.config.lock().unwrap().remove_peer(&server.id).unwrap();
            let result = client(&phone, &server, &url, None).sync_with_peer(&server.id).await;
            assert!(!result.success, "an unpinned self-signed certificate must not be accepted");
            task.abort();
        }

        #[tokio::test]
        async fn plain_http_is_refused_before_any_connection_unless_it_is_this_machine() {
            let phone = device("Phone");
            let server = device("Server");
            let client = client(&phone, &server, "http://10.255.255.1:8384", None);
            let result = client.sync_with_peer(&server.id).await;
            assert!(!result.success);
            assert!(result.errors.iter().any(|e| e.contains(codes::TLS_REQUIRED)), "{:?}", result.errors);

            let refused = start_server(server.db.clone(), server.config.clone(), "0.0.0.0", 0, true).await;
            assert!(refused.unwrap_err().to_string().contains(codes::TLS_REQUIRED));
        }
    }

    mod pairing {
        use super::*;
        use crate::auth;
        use crate::config::Config;
        use crate::sync_client::SyncClient;
        use crate::sync_protocol::codes;

        struct Device {
            db: Arc<Mutex<Database>>,
            config: Arc<Mutex<Config>>,
            id: String,
            _dir: TempDir,
        }

        fn device(name: &str) -> Device {
            let dir = TempDir::new().unwrap();
            let db = Database::new(dir.path().join("notes.db")).unwrap();
            let mut config = Config::new(Some(dir.path().to_path_buf()), None).unwrap();
            config.set_device_name(name).unwrap();
            auth::ensure_own_device_card(&db, &mut config).unwrap();
            let id = config.device_id_hex().to_string();
            Device { db: Arc::new(Mutex::new(db)), config: Arc::new(Mutex::new(config)), id, _dir: dir }
        }

        /// Serve with the device's own certificate; return its URL and the task.
        fn serve_tls(server: &Device) -> (String, tokio::task::JoinHandle<()>) {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let port = listener.local_addr().unwrap().port();
            let (cert, key, _) = {
                let cfg = server.config.lock().unwrap();
                crate::tls::ensure_server_certificate(&cfg, false).unwrap()
            };
            // The fingerprint is on the card once the certificate exists
            {
                let db = server.db.lock().unwrap();
                let mut cfg = server.config.lock().unwrap();
                auth::ensure_own_device_card(&db, &mut cfg).unwrap();
            }
            let router = create_router(server.db.clone(), server.config.clone())
                .into_make_service_with_connect_info::<SocketAddr>();
            let config = axum_server::tls_rustls::RustlsConfig::from_config(crate::tls::server_config(&cert, &key).unwrap());
            let task = tokio::spawn(async move {
                axum_server::from_tcp_rustls(listener, config).serve(router).await.unwrap();
            });
            (format!("https://127.0.0.1:{}", port), task)
        }

        #[tokio::test]
        async fn a_fresh_device_joins_from_the_code_and_then_syncs_both_ways() {
            let desk = device("Desk");
            desk.db.lock().unwrap().create_note("על השולחן").unwrap();
            let (url, task) = serve_tls(&desk);
            let setup = {
                let db = desk.db.lock().unwrap();
                let cfg = desk.config.lock().unwrap();
                crate::pairing::offer(&db, &cfg, vec![url.clone()]).unwrap()
            };
            assert!(!setup.certificate_fingerprint.is_empty(), "the listener's fingerprint is in the code");
            let text = setup.to_text();

            let phone = device("Phone");
            assert_ne!(phone.db.lock().unwrap().account_id().unwrap(), setup.account_id);
            let client = SyncClient::new(phone.db.clone(), phone.config.clone()).unwrap();

            let joined = client.join(&text).await.unwrap();

            assert_eq!(joined.account_id, setup.account_id);
            assert_eq!(joined.peer_id, desk.id);
            assert_eq!(phone.db.lock().unwrap().account_id().unwrap(), setup.account_id, "the phone took the account");
            let key = phone.config.lock().unwrap().device_key().to_string();
            assert_eq!(key.len(), 43);
            let card_on_desk = desk.db.lock().unwrap().get_device_card(&phone.id).unwrap().unwrap();
            assert_eq!(card_on_desk.key_hash, auth::key_hash(&key), "the desk holds the phone's key hash");
            let peers = phone.config.lock().unwrap().peers().to_vec();
            assert_eq!(peers.len(), 1);
            assert_eq!(peers[0].certificate_fingerprint.as_deref(), Some(setup.certificate_fingerprint.as_str()), "the fingerprint is pinned");

            // The token is spent
            let again = device("Another");
            let refused = SyncClient::new(again.db.clone(), again.config.clone()).unwrap().join(&text).await;
            assert!(refused.unwrap_err().to_string().contains(codes::TOKEN_INVALID));

            // And now an ordinary sync works, with the key and the pin
            phone.db.lock().unwrap().create_note("מהטלפון").unwrap();
            let result = client.sync_with_peer(&desk.id).await;
            assert!(result.success, "{:?}", result.errors);
            assert_eq!(phone.db.lock().unwrap().get_all_notes().unwrap().len(), 2);
            assert_eq!(desk.db.lock().unwrap().get_all_notes().unwrap().len(), 2);
            task.abort();
        }

        /// ENC-1: the recording key reaches a device let in by the code.
        #[tokio::test]
        async fn the_recording_key_travels_to_a_device_let_in_by_the_code() {
            let desk = device("Desk");
            let key = crate::crypto::RecordingKey::generate().to_text();
            desk.config.lock().unwrap().set_recording_key(&key).unwrap();
            let (url, task) = serve_tls(&desk);
            let setup = {
                let db = desk.db.lock().unwrap();
                let cfg = desk.config.lock().unwrap();
                crate::pairing::offer(&db, &cfg, vec![url.clone()]).unwrap()
            };
            let phone = device("Phone");
            assert!(phone.config.lock().unwrap().recording_key().is_none());
            let client = SyncClient::new(phone.db.clone(), phone.config.clone()).unwrap();
            client.join(&setup.to_text()).await.unwrap();
            assert_eq!(phone.config.lock().unwrap().recording_key_text(), key, "the key came with the claim reply");
            task.abort();
        }

        #[tokio::test]
        async fn a_device_with_notes_moves_to_another_account_by_its_code_and_tags_with_one_path_become_one() {
            let desk = device("Desk");
            {
                let db = desk.db.lock().unwrap();
                let work = db.create_tag("עבודה", None).unwrap();
                let note = db.create_note("על השולחן").unwrap();
                db.add_tag_to_note(&note, &work).unwrap();
            }
            let (url, task) = serve_tls(&desk);
            let setup = {
                let db = desk.db.lock().unwrap();
                let cfg = desk.config.lock().unwrap();
                crate::pairing::offer(&db, &cfg, vec![url.clone()]).unwrap()
            };
            let phone = device("Phone");
            let phone_note = {
                let db = phone.db.lock().unwrap();
                let work = db.create_tag("עבודה", None).unwrap();
                let note = db.create_note("מהטלפון, בחשבון אחר").unwrap();
                db.add_tag_to_note(&note, &work).unwrap();
                note
            };
            let client = SyncClient::new(phone.db.clone(), phone.config.clone()).unwrap();
            assert!(client.join(&setup.to_text()).await.unwrap_err().to_string().contains(codes::DEVICE_HOLDS_NOTES), "a join refuses; a move is deliberate");

            let (joined, merged) = client.move_to(&setup.to_text()).await.unwrap();
            assert_eq!(joined.account_id, setup.account_id);
            assert_eq!(merged, 1, "the two 'עבודה' tags became one");
            assert_eq!(phone.db.lock().unwrap().account_id().unwrap(), setup.account_id);
            assert!(!phone.db.lock().unwrap().list_snapshots().unwrap().is_empty(), "a snapshot first");
            let desk_notes = desk.db.lock().unwrap().get_all_notes().unwrap();
            assert_eq!(desk_notes.len(), 2, "the phone's note reached the desk");
            let tags_on_desk = desk.db.lock().unwrap().get_all_tags().unwrap();
            assert_eq!(tags_on_desk.iter().filter(|t| t.name == "עבודה").count(), 1, "one tag on the desk, not two");
            let phone_tags = phone.db.lock().unwrap().get_note_tags(&phone_note).unwrap();
            assert_eq!(phone_tags.len(), 1, "the phone's note keeps its tag, now the shared one");
            task.abort();
        }

        #[tokio::test]
        async fn a_device_with_notes_refuses_the_code_before_any_connection() {
            let desk = device("Desk");
            let setup = {
                let db = desk.db.lock().unwrap();
                let cfg = desk.config.lock().unwrap();
                crate::pairing::offer(&db, &cfg, vec!["https://127.0.0.1:1".to_string()]).unwrap()
            };
            let phone = device("Phone");
            phone.db.lock().unwrap().create_note("כבר יש לי").unwrap();
            let client = SyncClient::new(phone.db.clone(), phone.config.clone()).unwrap();
            let err = client.join(&setup.to_text()).await.unwrap_err().to_string();
            assert!(err.contains(codes::DEVICE_HOLDS_NOTES), "{}", err);
            assert!(desk.db.lock().unwrap().has_pairing_offer(chrono::Utc::now().timestamp()).unwrap(), "the code was not spent");
        }
    }

    mod hosting {
        use super::*;
        use crate::auth;
        use crate::config::Config;
        use crate::sync_client::SyncClient;
        use crate::sync_protocol::codes;

        struct Device {
            db: Arc<Mutex<Database>>,
            config: Arc<Mutex<Config>>,
            id: String,
            _dir: TempDir,
        }

        fn device(name: &str) -> Device {
            let dir = TempDir::new().unwrap();
            let db = Database::new(dir.path().join("notes.db")).unwrap();
            let mut config = Config::new(Some(dir.path().to_path_buf()), None).unwrap();
            config.set_device_name(name).unwrap();
            auth::ensure_own_device_card(&db, &mut config).unwrap();
            let id = config.device_id_hex().to_string();
            Device { db: Arc::new(Mutex::new(db)), config: Arc::new(Mutex::new(config)), id, _dir: dir }
        }

        /// A server over an empty indexed root, serving plain http on this
        /// machine. Returns the root, the source, the machine config, the URL
        /// and the task.
        fn host() -> (TempDir, Arc<IndexedAccounts>, Arc<Mutex<Config>>, String, tokio::task::JoinHandle<()>) {
            let root = TempDir::new().unwrap();
            crate::accounts::AccountIndex::open(root.path()).unwrap();
            let mut machine = Config::new(Some(root.path().to_path_buf()), None).unwrap();
            machine.set_device_name("Server").unwrap();
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let url = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
            let source = Arc::new(IndexedAccounts::new(root.path(), vec![url.clone()]));
            let machine = Arc::new(Mutex::new(machine));
            let router = create_router_for(source.clone(), machine.clone()).into_make_service_with_connect_info::<SocketAddr>();
            let task = tokio::spawn(async move { axum_server::from_tcp(listener).serve(router).await.unwrap() });
            (root, source, machine, url, task)
        }

        #[tokio::test]
        async fn a_holder_grants_a_server_its_account_and_a_third_device_joins_through_the_server() {
            let (root, source, machine, url, task) = host();
            let index = crate::accounts::AccountIndex::open(root.path()).unwrap();
            let grant = crate::pairing::offer_hosting(&index, &machine.lock().unwrap(), Some("meirav"), vec![url.clone()]).unwrap();
            assert!(grant.grant);
            assert!(grant.account_id.is_empty(), "a server's text names no account");
            let text = grant.to_text();
            assert!(text.contains("g=1"));

            // The holder grants
            let desk = device("Desk");
            desk.db.lock().unwrap().create_note("על השולחן").unwrap();
            let account = desk.db.lock().unwrap().account_id().unwrap();
            let desk_client = SyncClient::new(desk.db.clone(), desk.config.clone()).unwrap();
            let granted = desk_client.grant_host(&text, "").await.unwrap();
            assert_eq!(granted.account_id, account);
            assert_eq!(granted.peer_name, "Server");
            let server_id = machine.lock().unwrap().device_id_hex().to_string();
            assert_eq!(granted.peer_id, server_id);

            let listed = index.list().unwrap();
            assert_eq!(listed.len(), 1);
            assert_eq!((listed[0].account_id.as_str(), listed[0].label.as_str(), listed[0].hosted, listed[0].is_default), (account.as_str(), "meirav", true, false));
            assert_eq!(index.default_account().unwrap(), None, "a host makes no default account");
            let hosted = source.account(&account).unwrap();
            assert_eq!(hosted.db.lock().unwrap().account_id().unwrap(), account);
            assert_eq!(hosted.config.lock().unwrap().device_key().len(), 43, "the server holds the key the holder made");
            let holder_card = hosted.db.lock().unwrap().get_device_card(&desk.id).unwrap().unwrap();
            assert_eq!(holder_card.key_hash, auth::key_hash(desk.config.lock().unwrap().device_key()));
            let server_card = desk.db.lock().unwrap().get_device_card(&server_id).unwrap().unwrap();
            assert_eq!(server_card.key_hash, auth::key_hash(hosted.config.lock().unwrap().device_key()), "the desk holds the hash of the key it made");
            assert_eq!(server_card.listens, "0", "a card's listens and addresses are the owner's fields; they arrive by sync");

            // The text is spent
            let another = device("Another");
            let refused = SyncClient::new(another.db.clone(), another.config.clone()).unwrap().grant_host(&text, "").await;
            assert!(refused.unwrap_err().to_string().contains(codes::TOKEN_INVALID));

            // The holder delivers to the server
            let result = desk_client.sync_with_peer(&server_id).await;
            assert!(result.success, "{:?}", result.errors);
            assert_eq!(result.request_id.len(), 16, "the operation has an id");
            assert_eq!(hosted.db.lock().unwrap().get_all_notes().unwrap().len(), 1);
            let sync_request_id = result.request_id.clone();
            let server_card = desk.db.lock().unwrap().get_device_card(&server_id).unwrap().unwrap();
            assert_eq!(server_card.listens, "1", "after a sync the desk knows the server listens");
            assert!(server_card.addresses.contains(&url), "{}", server_card.addresses);

            // The server shows a code for the hosted account and a phone joins through it
            let setup = {
                let db = hosted.db.lock().unwrap();
                let cfg = hosted.config.lock().unwrap();
                crate::pairing::offer(&db, &cfg, vec![url.clone()]).unwrap()
            };
            assert_eq!(setup.account_id, account);
            let phone = device("Phone");
            let phone_client = SyncClient::new(phone.db.clone(), phone.config.clone()).unwrap();
            let joined = phone_client.join(&setup.to_text()).await.unwrap();
            assert_eq!(joined.peer_id, server_id);
            phone.db.lock().unwrap().create_note("מהטלפון").unwrap();
            let result = phone_client.sync_with_peer(&server_id).await;
            assert!(result.success, "{:?}", result.errors);
            assert_eq!(phone.db.lock().unwrap().get_all_notes().unwrap().len(), 2, "the phone has the desk's note through the server");
            let result = desk_client.sync_with_peer(&server_id).await;
            assert!(result.success, "{:?}", result.errors);
            assert_eq!(desk.db.lock().unwrap().get_all_notes().unwrap().len(), 2, "and the desk has the phone's");

            // The audit log names the devices and the routes, never a key
            let audit = std::fs::read_to_string(index.directory(&account).join("audit.log")).unwrap();
            assert!(audit.contains(&format!("{} {} POST /sync/handshake in=", sync_request_id, desk.id)), "{}", audit);
            assert!(audit.contains(&format!("{} {} GET /sync/changes", sync_request_id, desk.id)), "the same id on every request of the operation: {}", audit);
            assert!(audit.contains(&format!("{} GET /sync/changes", phone.id)), "{}", audit);
            assert!(!audit.contains(desk.config.lock().unwrap().device_key()));
            assert!(!audit.contains("מהטלפון"));
            task.abort();
        }

        #[tokio::test]
        async fn a_host_refuses_a_claim_and_a_request_for_an_account_it_does_not_hold() {
            let (_root, _source, _machine, url, task) = host();
            let stranger = device("Stranger");
            let stranger_id = stranger.db.lock().unwrap().account_id().unwrap();
            let http = reqwest::Client::new();
            let response = http
                .get(format!("{}/sync/changes?cursor=0", url))
                .header(auth::HEADER_ACCOUNT, &stranger_id)
                .header(auth::HEADER_DEVICE, &stranger.id)
                .bearer_auth("x")
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 404);
            let body: ErrorResponse = response.json().await.unwrap();
            assert_eq!(body.code, codes::ACCOUNT_UNKNOWN);

            let claim = PairClaimRequest {
                token: "x".repeat(43),
                account_id: stranger_id,
                device_id: stranger.id.clone(),
                device_name: "Stranger".to_string(),
                certificate_fingerprint: String::new(),
                addresses: "[]".to_string(),
                application: String::new(),
            };
            let response = http.post(format!("{}/pair/claim", url)).json(&claim).send().await.unwrap();
            assert_eq!(response.status(), 404);

            // A grant text for another server is refused here
            let grant = PairGrantRequest {
                token: "y".repeat(43),
                account_id: "0".repeat(32),
                label: String::new(),
                device_key: "z".repeat(43),
                holder_id: stranger.id.clone(),
                holder_name: "Stranger".to_string(),
                holder_certificate_fingerprint: String::new(),
                holder_addresses: "[]".to_string(),
                holder_key_hash: String::new(),
                recording_key: None,
            };
            let response = http.post(format!("{}/pair/grant", url)).json(&grant).send().await.unwrap();
            assert_eq!(response.status(), 403);
            let body: ErrorResponse = response.json().await.unwrap();
            assert_eq!(body.code, codes::TOKEN_INVALID);
            task.abort();
        }

        #[tokio::test]
        async fn a_single_account_listener_takes_no_grant() {
            let desk = device("Desk");
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let url = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
            let router = create_router(desk.db.clone(), desk.config.clone()).into_make_service_with_connect_info::<SocketAddr>();
            let task = tokio::spawn(async move { axum_server::from_tcp(listener).serve(router).await.unwrap() });
            let grant = PairGrantRequest {
                token: "y".repeat(43),
                account_id: "0".repeat(32),
                label: String::new(),
                device_key: "z".repeat(43),
                holder_id: "1".repeat(32),
                holder_name: "Holder".to_string(),
                holder_certificate_fingerprint: String::new(),
                holder_addresses: "[]".to_string(),
                holder_key_hash: String::new(),
                recording_key: None,
            };
            let response = reqwest::Client::new().post(format!("{}/pair/grant", url)).json(&grant).send().await.unwrap();
            assert_eq!(response.status(), 403, "no token was ever offered");
            task.abort();
        }
    }

    mod periodic_backup {
        use super::*;
        use crate::config::Config;

        #[test]
        fn a_copy_is_made_the_newest_are_kept_and_the_due_check_reads_the_directory() {
            let dir = TempDir::new().unwrap();
            let db = Database::new(dir.path().join("notes.db")).unwrap();
            db.create_note("לגיבוי").unwrap();
            let mut config = Config::new(Some(dir.path().to_path_buf()), None).unwrap();
            let account = db.account_id().unwrap();
            assert!(backup_due(&config, &account), "nothing copied yet");
            let target = config.backup_directory(&account);
            assert_eq!(target, dir.path().join("backups").join(&account));

            let first = db.backup_to(&target, 2).unwrap();
            assert!(first.is_file());
            let copy = Database::new(&first).unwrap();
            assert_eq!(copy.get_all_notes().unwrap().len(), 1, "the copy holds the note");
            assert!(!backup_due(&config, &account), "just copied");
            db.backup_to(&target, 2).unwrap();
            db.backup_to(&target, 2).unwrap();
            assert_eq!(Database::backups_in(&target).unwrap().len(), 2, "only the newest two are kept");

            config.set_backup(crate::config::BackupConfig { interval_hours: 0, directory: String::new(), keep: 2 }).unwrap();
            assert!(!backup_due(&config, &account), "0 hours turns it off");
            config.set_backup(crate::config::BackupConfig { interval_hours: 24, directory: dir.path().join("elsewhere").to_string_lossy().to_string(), keep: 2 }).unwrap();
            assert_eq!(config.backup_directory(&account), dir.path().join("elsewhere").join(&account));
        }

        #[tokio::test]
        async fn the_task_copies_every_open_account_each_interval() {
            let dir = TempDir::new().unwrap();
            let db = Database::new(dir.path().join("notes.db")).unwrap();
            let account = db.account_id().unwrap();
            let config = Config::new(Some(dir.path().to_path_buf()), None).unwrap();
            let handle = AccountHandle { db: Arc::new(Mutex::new(db)), config: Arc::new(Mutex::new(config)) };
            let source: Arc<dyn AccountSource> = Arc::new(SingleAccount { account_id: account.clone(), handle: handle.clone() });
            let task = spawn_periodic_backup(source, Duration::from_millis(60), 3);
            tokio::time::sleep(Duration::from_millis(400)).await;
            task.abort();
            let target = handle.config.lock().unwrap().backup_directory(&account);
            let copies = Database::backups_in(&target).unwrap();
            assert!(!copies.is_empty() && copies.len() <= 3, "{} copies", copies.len());
        }
    }

    mod version_two {
        use super::*;
        use crate::auth;
        use crate::config::Config;
        use crate::sync_client::SyncClient;
        use crate::sync_protocol::{codes, HandshakeRequest};

        struct Device {
            db: Arc<Mutex<Database>>,
            config: Arc<Mutex<Config>>,
            id: String,
            _dir: TempDir,
        }

        fn device(name: &str) -> Device {
            let dir = TempDir::new().unwrap();
            let db = Database::new(dir.path().join("notes.db")).unwrap();
            let mut config = Config::new(Some(dir.path().to_path_buf()), None).unwrap();
            config.set_device_name(name).unwrap();
            auth::ensure_own_device_card(&db, &mut config).unwrap();
            let id = config.device_id_hex().to_string();
            Device { db: Arc::new(Mutex::new(db)), config: Arc::new(Mutex::new(config)), id, _dir: dir }
        }

        /// A and B of one account, B served plain http; A's raw client with its headers.
        async fn pair() -> (Device, Device, String, reqwest::Client, Vec<(String, String)>, tokio::task::JoinHandle<()>) {
            let a = device("A");
            let b = device("B");
            let account = a.db.lock().unwrap().account_id().unwrap();
            b.db.lock().unwrap().move_to_account(&account).unwrap();
            let card_a = a.db.lock().unwrap().get_device_card(&a.id).unwrap().unwrap();
            let card_b = b.db.lock().unwrap().get_device_card(&b.id).unwrap().unwrap();
            a.db.lock().unwrap().admit_device_card(&card_b).unwrap();
            b.db.lock().unwrap().admit_device_card(&card_a).unwrap();
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let url = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
            let router = create_router(b.db.clone(), b.config.clone()).into_make_service_with_connect_info::<SocketAddr>();
            let task = tokio::spawn(async move { axum_server::from_tcp(listener).serve(router).await.unwrap() });
            a.config.lock().unwrap().add_peer(&b.id, "B", &url, None, true).unwrap();
            let key = a.config.lock().unwrap().device_key().to_string();
            let headers = vec![
                (auth::HEADER_ACCOUNT.to_string(), account),
                (auth::HEADER_DEVICE.to_string(), a.id.clone()),
                ("authorization".to_string(), format!("Bearer {}", key)),
            ];
            (a, b, url, reqwest::Client::new(), headers, task)
        }

        fn with_headers(mut request: reqwest::RequestBuilder, headers: &[(String, String)]) -> reqwest::RequestBuilder {
            for (k, v) in headers {
                request = request.header(k, v);
            }
            request
        }

        #[tokio::test]
        async fn a_peer_of_version_one_is_refused_in_words_and_version_two_is_let_in() {
            let (a, _b, url, http, headers, task) = pair().await;
            let account = a.db.lock().unwrap().account_id().unwrap();
            let mut request = HandshakeRequest {
                device_id: a.id.clone(),
                device_name: "Old phone".to_string(),
                protocol_version: "1.1".to_string(),
                account_id: account.clone(),
                application: "voice".to_string(),
                entity_types: Vec::new(),
            };
            let response = with_headers(http.post(format!("{}/sync/handshake", url)), &headers).json(&request).send().await.unwrap();
            assert_eq!(response.status(), 426);
            let body: ErrorResponse = response.json().await.unwrap();
            assert_eq!(body.code, codes::PROTOCOL_TOO_OLD);
            assert_eq!(body.error, "Update Voice on Old phone (PROTOCOL_TOO_OLD)");

            request.protocol_version = "2.0".to_string();
            let response = with_headers(http.post(format!("{}/sync/handshake", url)), &headers).json(&request).send().await.unwrap();
            assert_eq!(response.status(), 200);
            let body: HandshakeResponse = response.json().await.unwrap();
            assert_eq!(body.protocol_version, "2.0");
            assert_eq!(body.application, "voice");
            task.abort();
        }

        #[tokio::test]
        async fn the_feed_narrows_to_the_types_asked_for_and_apply_refuses_undeclared_ones() {
            let (a, b, url, http, headers, task) = pair().await;
            {
                let db = b.db.lock().unwrap();
                db.create_note("פתק").unwrap();
                db.create_tag("תגית", None).unwrap();
            }
            let response = with_headers(http.get(format!("{}/sync/changes?cursor=0&limit=1000&types=tag", url)), &headers).send().await.unwrap();
            assert_eq!(response.status(), 200);
            let page: ChangesResponse = response.json().await.unwrap();
            assert!(!page.changes.is_empty());
            assert!(page.changes.iter().all(|c| c.entity_type == "tag"), "{:?}", page.changes.iter().map(|c| c.entity_type.clone()).collect::<Vec<_>>());
            assert!(page.next_cursor.unwrap_or(0) > 0, "the cursor walks the whole feed");

            // An application that declared tags only: a note it sends is not applied
            let account = a.db.lock().unwrap().account_id().unwrap();
            let handshake = HandshakeRequest {
                device_id: a.id.clone(),
                device_name: "Images".to_string(),
                protocol_version: PROTOCOL_VERSION.to_string(),
                account_id: account,
                application: "images".to_string(),
                entity_types: vec!["tag".to_string()],
            };
            let response = with_headers(http.post(format!("{}/sync/handshake", url)), &headers).json(&handshake).send().await.unwrap();
            assert_eq!(response.status(), 200);
            let (note_change, tag_change) = {
                let db = a.db.lock().unwrap();
                db.create_note("מהתמונות").unwrap();
                db.create_tag("צילומים", None).unwrap();
                let (changes, _, _) = db.get_changes_after_seq_as_sync_changes(0, None, 1000).unwrap();
                (
                    changes.iter().find(|c| c.entity_type == "note").cloned().unwrap(),
                    changes.iter().find(|c| c.entity_type == "tag").cloned().unwrap(),
                )
            };
            let apply = ApplyRequest { device_id: a.id.clone(), device_name: "Images".to_string(), changes: vec![note_change, tag_change] };
            let response = with_headers(http.post(format!("{}/sync/apply", url)), &headers).json(&apply).send().await.unwrap();
            assert_eq!(response.status(), 200);
            let body: ApplyResponse = response.json().await.unwrap();
            assert_eq!(body.applied, 1, "the tag");
            assert!(body.errors.iter().any(|e| e.contains("did not declare")), "{:?}", body.errors);
            assert!(b.db.lock().unwrap().get_all_notes().unwrap().iter().all(|n| n.content != "מהתמונות"));

            // Voice, declaring nothing, gets everything as before
            let client = SyncClient::new(a.db.clone(), a.config.clone()).unwrap();
            let result = client.sync_with_peer(&b.id).await;
            assert!(result.success, "{:?}", result.errors);
            assert!(b.db.lock().unwrap().get_all_notes().unwrap().iter().any(|n| n.content == "מהתמונות"));
            task.abort();
        }
    }

    mod lan_only {
        use super::*;

        #[test]
        fn private_link_local_and_loopback_callers_are_served_and_others_only_with_a_public_url() {
            for ip in ["127.0.0.1", "10.0.0.5", "172.16.4.4", "192.168.1.7", "169.254.1.1", "::1", "fe80::1", "fd12::1", "::ffff:192.168.1.7"] {
                assert!(source_allowed(ip.parse().unwrap(), false), "{}", ip);
            }
            for ip in ["8.8.8.8", "203.0.113.9", "2001:db8::1", "::ffff:8.8.8.8"] {
                assert!(!source_allowed(ip.parse().unwrap(), false), "{}", ip);
                assert!(source_allowed(ip.parse().unwrap(), true), "{} with a public URL", ip);
            }
        }
    }

    mod robustness {
        use super::*;
        use crate::auth;
        use crate::config::Config;
        use crate::sync_client::SyncClient;
        use crate::sync_protocol::codes;
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct Device {
            db: Arc<Mutex<Database>>,
            config: Arc<Mutex<Config>>,
            id: String,
            _dir: TempDir,
        }

        fn device(name: &str) -> Device {
            let dir = TempDir::new().unwrap();
            let db = Database::new(dir.path().join("notes.db")).unwrap();
            let mut config = Config::new(Some(dir.path().to_path_buf()), None).unwrap();
            config.set_device_name(name).unwrap();
            auth::ensure_own_device_card(&db, &mut config).unwrap();
            let id = config.device_id_hex().to_string();
            Device { db: Arc::new(Mutex::new(db)), config: Arc::new(Mutex::new(config)), id, _dir: dir }
        }

        /// Two devices of one account, mutually admitted; B served plain
        /// http through `wrap`, which may add a layer to the router.
        fn pair(wrap: impl FnOnce(Router) -> Router) -> (Device, Device, String, tokio::task::JoinHandle<()>) {
            let a = device("A");
            let b = device("B");
            let account = a.db.lock().unwrap().account_id().unwrap();
            b.db.lock().unwrap().move_to_account(&account).unwrap();
            let card_a = a.db.lock().unwrap().get_device_card(&a.id).unwrap().unwrap();
            let card_b = b.db.lock().unwrap().get_device_card(&b.id).unwrap().unwrap();
            a.db.lock().unwrap().admit_device_card(&card_b).unwrap();
            b.db.lock().unwrap().admit_device_card(&card_a).unwrap();
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let url = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
            let router = wrap(create_router(b.db.clone(), b.config.clone())).into_make_service_with_connect_info::<SocketAddr>();
            let task = tokio::spawn(async move { axum_server::from_tcp(listener).serve(router).await.unwrap() });
            a.config.lock().unwrap().add_peer(&b.id, "B", &url, None, true).unwrap();
            (a, b, url, task)
        }

        /// A page is committed before the next is requested: when the
        /// connection dies mid-sync, the cursor stands at the end of the
        /// last page applied, and the next sync continues from there.
        #[tokio::test]
        async fn a_sync_cut_after_the_first_page_keeps_that_page_and_continues_next_time() {
            let feed_requests = Arc::new(AtomicUsize::new(0));
            let counter = feed_requests.clone();
            let (a, b, _url, task) = pair(move |router| {
                router.layer(middleware::from_fn(move |request: Request, next: Next| {
                    let counter = counter.clone();
                    async move {
                        if request.uri().path() == "/sync/changes" && counter.fetch_add(1, Ordering::SeqCst) == 1 {
                            // The second page never arrives
                            return StatusCode::SERVICE_UNAVAILABLE.into_response();
                        }
                        next.run(request).await
                    }
                }))
            });
            // Several pages of changes on B: cards and a tag first, then notes
            let page = 40;
            let total = 43;
            {
                let db = b.db.lock().unwrap();
                for i in 0..total {
                    db.create_note(&format!("note {}", i)).unwrap();
                }
            }
            let client = SyncClient::new(a.db.clone(), a.config.clone()).unwrap();
            client.set_page_size(page as i64);
            let first = client.sync_with_peer(&b.id).await;
            assert!(!first.success, "the cut is an error, not silence");
            assert!(first.pulled > 0, "the first page was applied");
            let notes_after_cut = a.db.lock().unwrap().get_all_notes().unwrap().len();
            assert!(notes_after_cut > 0 && notes_after_cut < total, "{} of {} notes arrived before the cut", notes_after_cut, total);
            let (received, _sent, _) = a.db.lock().unwrap().get_peer_cursors(&b.id).unwrap();
            assert!(received > 0, "the first page's cursor was saved");

            let second = client.sync_with_peer(&b.id).await;
            assert!(second.success, "{:?}", second.errors);
            assert_eq!(a.db.lock().unwrap().get_all_notes().unwrap().len(), total, "nothing was lost");

            // B's feed, split at the cursor the cut left: the notes A had
            // after the cut are exactly the notes of the part before it, and
            // the second run asked only for the part after it
            let (notes_before, after) = {
                let db = b.db.lock().unwrap();
                let before = db.get_changes_after_seq_as_sync_changes(0, Some(received), 100_000).unwrap().0;
                let after = db.get_changes_after_seq_as_sync_changes(received, None, 100_000).unwrap().0;
                let notes: std::collections::HashSet<&str> = before.iter().filter(|c| c.entity_type == "note").map(|c| c.entity_id.as_str()).collect();
                (notes.len(), after.iter().filter(|c| c.device_id != a.id).count() as i64)
            };
            assert_eq!(notes_after_cut, notes_before, "the page before the cut was applied whole, and nothing past it");
            let (final_cursor, _, _) = a.db.lock().unwrap().get_peer_cursors(&b.id).unwrap();
            assert!(final_cursor > received, "the cursor moved on");
            let requests = feed_requests.load(Ordering::SeqCst) as i64;
            let whole_feed_pages = (received + after + page as i64 - 1) / page as i64 + 1;
            let pages_after = after / page as i64 + 1;
            // After the last page the client confirms the cursor it holds (D29):
            // one request of its own, not a page of the feed
            let confirmation = 1;
            assert!(requests <= 1 + 1 + pages_after + confirmation, "one page, the cut, then the rest from the saved cursor and the confirmation: {} requests for {} changes after the cut, {} pages", requests, after, pages_after);
            assert!(1 + 1 + pages_after + confirmation < 1 + 1 + whole_feed_pages + confirmation, "the bound tells a restart from zero apart");
            task.abort();
        }

        #[tokio::test]
        async fn the_feed_is_gzipped_when_the_caller_accepts_it_and_plain_otherwise() {
            let (a, b, url, task) = pair(|r| r);
            {
                let db = b.db.lock().unwrap();
                for i in 0..200 {
                    db.create_note(&format!("a note with enough text to be worth compressing, number {}", i)).unwrap();
                }
            }
            let key = a.config.lock().unwrap().device_key().to_string();
            let account = a.db.lock().unwrap().account_id().unwrap();
            let raw = reqwest::Client::builder().no_gzip().build().unwrap();
            let response = raw
                .get(format!("{}/sync/changes?cursor=0&limit=1000", url))
                .header(auth::HEADER_ACCOUNT, &account)
                .header(auth::HEADER_DEVICE, &a.id)
                .header("accept-encoding", "gzip")
                .bearer_auth(&key)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 200);
            assert_eq!(response.headers().get("content-encoding").map(|v| v.to_str().unwrap()), Some("gzip"));
            let compressed = response.bytes().await.unwrap().len();
            let response = raw
                .get(format!("{}/sync/changes?cursor=0&limit=1000", url))
                .header(auth::HEADER_ACCOUNT, &account)
                .header(auth::HEADER_DEVICE, &a.id)
                .bearer_auth(&key)
                .send()
                .await
                .unwrap();
            assert_eq!(response.headers().get("content-encoding"), None);
            let plain = response.bytes().await.unwrap().len();
            assert!(compressed * 4 < plain, "gzip: {} bytes, plain: {}", compressed, plain);

            // The ordinary client understands it: a sync brings every note
            let client = SyncClient::new(a.db.clone(), a.config.clone()).unwrap();
            let result = client.sync_with_peer(&b.id).await;
            assert!(result.success, "{:?}", result.errors);
            assert_eq!(a.db.lock().unwrap().get_all_notes().unwrap().len(), 200);
            task.abort();
        }

        #[tokio::test]
        async fn a_connection_check_names_what_passes_and_what_is_refused() {
            let (a, b, url, task) = pair(|r| r);
            let client = SyncClient::new(a.db.clone(), a.config.clone()).unwrap();
            let rows = client.check(&b.id).await;
            let by_name = |name: &str| rows.iter().find(|r| r.name == name).unwrap_or_else(|| panic!("no row {} in {:?}", name, rows));
            assert!(by_name("Reachable").passed, "{:?}", rows);
            assert!(by_name("Certificate").passed);
            assert!(by_name("Account").passed);
            assert!(by_name("Key").passed);
            assert!(by_name("Clock").passed);
            assert!(by_name("Free space here").passed);
            assert!(by_name("Free space there").passed);
            assert!(by_name("Listener here").detail.contains("not listening"));

            // A stranger of the same account: reachable, but its key is unknown there
            let stranger = device("Stranger");
            let account = a.db.lock().unwrap().account_id().unwrap();
            stranger.db.lock().unwrap().move_to_account(&account).unwrap();
            stranger.config.lock().unwrap().add_peer(&b.id, "B", &url, None, true).unwrap();
            let rows = SyncClient::new(stranger.db.clone(), stranger.config.clone()).unwrap().check(&b.id).await;
            let key = rows.iter().find(|r| r.name == "Key").unwrap();
            assert!(!key.passed);
            assert_eq!(key.code, codes::DEVICE_UNKNOWN);

            // Nobody at the address
            stranger.config.lock().unwrap().add_peer("00000000000070008000000000000001", "Nobody", "http://127.0.0.1:1", None, true).unwrap();
            let rows = SyncClient::new(stranger.db.clone(), stranger.config.clone()).unwrap().check("00000000000070008000000000000001").await;
            assert_eq!(rows.len(), 1);
            assert!(!rows[0].passed && rows[0].name == "Reachable", "{:?}", rows);
            task.abort();
        }
    }

    mod peers_from_cards {
        use super::*;
        use crate::auth;
        use crate::config::Config;
        use crate::sync_client::SyncClient;

        struct Device {
            db: Arc<Mutex<Database>>,
            config: Arc<Mutex<Config>>,
            id: String,
            _dir: TempDir,
        }

        fn device(name: &str) -> Device {
            let dir = TempDir::new().unwrap();
            let db = Database::new(dir.path().join("notes.db")).unwrap();
            let mut config = Config::new(Some(dir.path().to_path_buf()), None).unwrap();
            config.set_device_name(name).unwrap();
            auth::ensure_own_device_card(&db, &mut config).unwrap();
            let id = config.device_id_hex().to_string();
            Device { db: Arc::new(Mutex::new(db)), config: Arc::new(Mutex::new(config)), id, _dir: dir }
        }

        fn card_of(d: &Device) -> crate::versions::DeviceCard {
            d.db.lock().unwrap().get_device_card(&d.id).unwrap().unwrap()
        }

        #[tokio::test]
        async fn after_a_sync_every_card_is_a_peer_a_revoked_one_goes_and_a_forgotten_one_stays_away() {
            let a = device("A");
            let b = device("B");
            let c = device("C");
            let account = a.db.lock().unwrap().account_id().unwrap();
            for d in [&b, &c] {
                d.db.lock().unwrap().move_to_account(&account).unwrap();
            }
            // B knows A and C; C listens somewhere and says so on its card
            {
                let db = c.db.lock().unwrap();
                let mut cfg = c.config.lock().unwrap();
                record_listening(&db, &mut cfg, &["https://192.168.1.7:8384".to_string()], true).unwrap();
            }
            b.db.lock().unwrap().admit_device_card(&card_of(&a)).unwrap();
            b.db.lock().unwrap().admit_device_card(&card_of(&c)).unwrap();
            a.db.lock().unwrap().admit_device_card(&card_of(&b)).unwrap();
            // B's card on B carries the listening address and the fingerprint once it serves
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let url = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
            {
                let db = b.db.lock().unwrap();
                let mut cfg = b.config.lock().unwrap();
                record_listening(&db, &mut cfg, &[url.clone()], true).unwrap();
            }
            let router = create_router(b.db.clone(), b.config.clone()).into_make_service_with_connect_info::<SocketAddr>();
            let task = tokio::spawn(async move { axum_server::from_tcp(listener).serve(router).await.unwrap() });
            a.config.lock().unwrap().add_peer(&b.id, "B", &url, None, true).unwrap();

            // C's card, with its address, reaches B when C syncs; a card
            // admitted by hand carries no address (the owner's fields travel)
            c.db.lock().unwrap().admit_device_card(&card_of(&b)).unwrap();
            c.config.lock().unwrap().add_peer(&b.id, "B", &url, None, true).unwrap();
            let from_c = SyncClient::new(c.db.clone(), c.config.clone()).unwrap().sync_with_peer(&b.id).await;
            assert!(from_c.success, "{:?}", from_c.errors);

            let client = SyncClient::new(a.db.clone(), a.config.clone()).unwrap();
            let result = client.sync_with_peer(&b.id).await;
            assert!(result.success, "{:?}", result.errors);

            let peers = a.config.lock().unwrap().peers().to_vec();
            assert_eq!(peers.len(), 2, "B, and C from its card: {:?}", peers);
            let c_peer = peers.iter().find(|p| p.peer_id == c.id).expect("C is a peer now");
            assert_eq!(c_peer.peer_name, "C");
            assert_eq!(c_peer.peer_url, "https://192.168.1.7:8384", "the card's first address");
            assert_eq!(a.config.lock().unwrap().last_peer().map(|p| p.peer_id.clone()), Some(b.id.clone()), "the last peer is B");
            assert!(!peers.iter().any(|p| p.peer_id == a.id), "never itself");

            // Forgotten on A: gone, and the next sync does not bring it back
            assert!(a.config.lock().unwrap().forget_peer(&c.id).unwrap());
            let result = client.sync_with_peer(&b.id).await;
            assert!(result.success, "{:?}", result.errors);
            assert!(!a.config.lock().unwrap().peers().iter().any(|p| p.peer_id == c.id), "forgotten stays forgotten");
            // Added again by hand: no longer forgotten
            a.config.lock().unwrap().add_peer(&c.id, "C again", "https://192.168.1.7:8384", None, true).unwrap();
            assert!(!a.config.lock().unwrap().is_forgotten(&c.id));
            assert!(a.config.lock().unwrap().rename_peer(&c.id, "Meirav's phone").unwrap());
            let result = client.sync_with_peer(&b.id).await;
            assert!(result.success);
            assert_eq!(a.config.lock().unwrap().get_peer(&c.id).unwrap().peer_name, "Meirav's phone", "the local name stays over the card's");

            // Revoked on B: after the next sync it is no peer of A's
            b.db.lock().unwrap().revoke_device(&c.id).unwrap();
            let result = client.sync_with_peer(&b.id).await;
            assert!(result.success, "{:?}", result.errors);
            assert!(!a.config.lock().unwrap().peers().iter().any(|p| p.peer_id == c.id), "a revoked card's peer goes");
            task.abort();
        }
    }

    mod files_between_instances {
        use super::*;
        use crate::auth;
        use crate::config::Config;
        use crate::sync_client::SyncClient;

        struct Device {
            db: Arc<Mutex<Database>>,
            config: Arc<Mutex<Config>>,
            id: String,
            audio: std::path::PathBuf,
            _dir: TempDir,
        }

        fn device(name: &str) -> Device {
            let dir = TempDir::new().unwrap();
            let db = Database::new(dir.path().join("notes.db")).unwrap();
            let mut config = Config::new(Some(dir.path().to_path_buf()), None).unwrap();
            config.set_device_name(name).unwrap();
            let audio = dir.path().join("audio");
            std::fs::create_dir_all(&audio).unwrap();
            config.set_audiofile_directory(audio.to_str().unwrap()).unwrap();
            auth::ensure_own_device_card(&db, &mut config).unwrap();
            let id = config.device_id_hex().to_string();
            Device { db: Arc::new(Mutex::new(db)), config: Arc::new(Mutex::new(config)), id, audio, _dir: dir }
        }

        /// Two devices of one account, each admitted on the other, the
        /// second serving plain http on this machine.
        fn pair() -> (Device, Device, String, tokio::task::JoinHandle<()>) {
            let a = device("A");
            let b = device("B");
            let account = a.db.lock().unwrap().account_id().unwrap();
            b.db.lock().unwrap().move_to_account(&account).unwrap();
            let card_a = a.db.lock().unwrap().get_device_card(&a.id).unwrap().unwrap();
            let card_b = b.db.lock().unwrap().get_device_card(&b.id).unwrap().unwrap();
            a.db.lock().unwrap().admit_device_card(&card_b).unwrap();
            b.db.lock().unwrap().admit_device_card(&card_a).unwrap();
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let url = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
            let router = create_router(b.db.clone(), b.config.clone()).into_make_service_with_connect_info::<SocketAddr>();
            let task = tokio::spawn(async move { axum_server::from_tcp(listener).serve(router).await.unwrap() });
            a.config.lock().unwrap().add_peer(&b.id, "B", &url, None, true).unwrap();
            (a, b, url, task)
        }

        /// Where a recording's file is on `d`: what its row says (Stage 13).
        fn path_of(d: &Device, audio_id: &str) -> std::path::PathBuf {
            let disk_name = d.db.lock().unwrap().get_audio_file(audio_id).unwrap().unwrap().disk_name;
            audio_local_path(&d.audio, &disk_name)
        }

        /// A note with one recording of `size` bytes on `d`, under a name no
        /// other recording has: these tests move files, and two recordings
        /// with one name would collide (FILE-15), which the collision tests cover.
        fn recording(d: &Device, size: usize) -> (String, std::path::PathBuf) {
            static MADE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
            let name = format!("recording {}.ogg", MADE.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
            let content: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
            let source = d._dir.path().join(&name);
            std::fs::write(&source, &content).unwrap();
            let (_note_id, audio_id) = d.db.lock().unwrap().import_audio_file(&name, None, None, Some(d.audio.as_path())).unwrap();
            let path = path_of(d, &audio_id);
            std::fs::rename(&source, &path).unwrap();
            (audio_id, path)
        }

        /// ENC-4: a device without the key serves an encrypted file as it is,
        /// with the header, and a device with the key opens it on arrival.
        #[tokio::test]
        async fn an_encrypted_file_served_by_a_keyless_device_is_opened_by_one_with_the_key() {
            let (a, b, _url, task) = pair();
            let (id, path_b) = recording(&b, 250_000);
            let plain = std::fs::read(&path_b).unwrap();
            let key = crate::crypto::RecordingKey::generate();
            // B keeps the object as the bucket holds it: encrypted, no key of its own
            let mut view = crate::crypto::EncryptedView::open(&path_b, &key).unwrap();
            let mut encrypted = vec![0u8; crate::crypto::ByteSource::len(&view) as usize];
            crate::crypto::ByteSource::read_at(&mut view, 0, &mut encrypted).unwrap();
            std::fs::write(&path_b, &encrypted).unwrap();
            a.config.lock().unwrap().set_recording_key(&key.to_text()).unwrap();
            let client = SyncClient::new(a.db.clone(), a.config.clone()).unwrap();
            let result = client.exchange(&b.id).await;
            assert!(result.success, "{:?}", result.errors);
            assert_eq!(result.fetched, 1);
            assert_eq!(std::fs::read(path_of(&a, &id)).unwrap(), plain, "opened on arrival with the key");
            assert!(!crate::transfer::part_path(&path_of(&a, &id)).exists());
            assert!(crate::crypto::file_is_encrypted(&path_b), "B still keeps it as it came");
            task.abort();
        }

        /// FILE-15: two devices that each imported a file with one name reach
        /// the same two suffixed names after an exchange, and every copy on
        /// disk carries its row's name.
        #[tokio::test]
        async fn two_devices_that_imported_one_name_reach_the_same_suffixed_names() {
            let (a, b, _url, task) = pair();
            let import = |d: &Device, bytes: &[u8]| {
                let id = d.db.lock().unwrap().create_audio_file("MyHouse.jpg", None, None, crate::models::FileOrigin::Imported, Some(d.audio.as_path())).unwrap();
                std::fs::write(path_of(d, &id), bytes).unwrap();
                id
            };
            let on_a = import(&a, b"the house photographed on A");
            let on_b = import(&b, b"the house photographed on B");
            let client = SyncClient::new(a.db.clone(), a.config.clone()).unwrap();
            let result = client.exchange(&b.id).await;
            assert!(result.success, "{:?}", result.errors);
            let name = |d: &Device, id: &str| d.db.lock().unwrap().get_audio_file(id).unwrap().unwrap().disk_name;
            for d in [&a, &b] {
                assert_eq!(name(d, &on_a), crate::models::suffixed_name("MyHouse.jpg", &on_a, false));
                assert_eq!(name(d, &on_b), crate::models::suffixed_name("MyHouse.jpg", &on_b, false));
                assert!(!d.audio.join("MyHouse.jpg").exists(), "no file keeps the name that collided");
                assert_eq!(std::fs::read(path_of(d, &on_a)).unwrap(), b"the house photographed on A");
                assert_eq!(std::fs::read(path_of(d, &on_b)).unwrap(), b"the house photographed on B");
            }
            task.abort();
        }

        #[tokio::test]
        async fn exchange_moves_a_recording_each_way_and_a_second_run_moves_nothing() {
            let (a, b, _url, task) = pair();
            let (id_a, path_a) = recording(&a, 300_000);
            let (id_b, path_b) = recording(&b, 200_000);
            let client = SyncClient::new(a.db.clone(), a.config.clone()).unwrap();

            let result = client.exchange(&b.id).await;
            assert!(result.success, "{:?}", result.errors);
            assert_eq!((result.sent, result.fetched), (1, 1));
            assert_eq!(result.bytes_moved, 500_000);
            let on_b = path_of(&b, &id_a);
            assert_eq!(std::fs::read(&on_b).unwrap(), std::fs::read(&path_a).unwrap(), "A's recording arrived on B whole");
            let on_a = path_of(&a, &id_b);
            assert_eq!(std::fs::read(&on_a).unwrap(), std::fs::read(&path_b).unwrap(), "B's recording arrived on A whole");
            assert!(!crate::transfer::part_path(&on_a).exists());

            // Both sides now know where the copies are (Stage 10)
            let a_knows = |id: &str| a.db.lock().unwrap().copies_of(id, &a.id).unwrap().iter().map(|c| c.peer_id.clone()).collect::<Vec<_>>();
            let b_knows = |id: &str| b.db.lock().unwrap().copies_of(id, &b.id).unwrap().iter().map(|c| c.peer_id.clone()).collect::<Vec<_>>();
            assert_eq!(a_knows(&id_a), vec![b.id.clone()], "A sent its recording to B");
            assert_eq!(a_knows(&id_b), vec![b.id.clone()], "A fetched B's, so B holds it");
            assert_eq!(b_knows(&id_a), vec![a.id.clone()], "B received A's, so A holds it");
            assert_eq!(b_knows(&id_b), vec![a.id.clone()], "B served its own to A");
            assert_eq!(a.db.lock().unwrap().not_duplicated(Some(&a.audio), &a.id).unwrap(), crate::database::NotDuplicated { notes: 0, recordings: 0 }, "everything of A's is somewhere else too");
            let peers = a.db.lock().unwrap().peer_summaries().unwrap();
            assert_eq!(peers.len(), 1);
            assert_eq!(peers[0].last_operation.as_deref(), Some("exchange"));
            assert!(peers[0].last_reached_at.is_some());

            let again = client.exchange(&b.id).await;
            assert!(again.success);
            assert_eq!((again.sent, again.fetched, again.bytes_moved), (0, 0, 0), "nothing left to move");
            task.abort();
        }

        /// Progress reaches the sink, and a cancel from the sink stops the
        /// transfer at the next chunk, leaves the part, and the next
        /// operation continues from it.
        #[tokio::test]
        async fn progress_is_reported_and_a_cancel_stops_at_the_next_chunk_leaving_a_part_to_continue_from() {
            struct CancelAfterFirstReport {
                seen: Mutex<Vec<crate::sync_client::Progress>>,
                cancel: Arc<std::sync::atomic::AtomicBool>,
            }
            impl crate::sync_client::ProgressSink for CancelAfterFirstReport {
                fn report(&self, progress: crate::sync_client::Progress) {
                    let mut seen = self.seen.lock().unwrap();
                    if progress.stage == "send" && progress.bytes > 0 {
                        self.cancel.store(true, std::sync::atomic::Ordering::SeqCst);
                    }
                    seen.push(progress);
                }
            }
            let (a, b, _url, task) = pair();
            let (id_a, _path_a) = recording(&a, 3 * 1024 * 1024 + 7);
            let client = SyncClient::new(a.db.clone(), a.config.clone()).unwrap();
            let sink = Arc::new(CancelAfterFirstReport { seen: Mutex::new(Vec::new()), cancel: client.cancel_flag() });
            client.set_progress_sink(Some(sink.clone()));

            let cut = client.deliver(&b.id).await;
            assert!(!cut.success);
            assert!(cut.errors.iter().any(|e| e == crate::sync_client::CANCELLED), "{:?}", cut.errors);
            assert_eq!(cut.sent, 0);
            let seen = sink.seen.lock().unwrap().clone();
            assert!(seen.iter().any(|p| p.stage == "sync"), "the sync reported");
            assert!(seen.iter().any(|p| p.stage == "send" && p.bytes > 0), "the send reported bytes: {:?}", seen);
            let on_b = path_of(&b, &id_a);
            assert!(!on_b.is_file(), "the file did not arrive whole");

            // Without the cancelling sink, the next deliver sends only the
            // rest: what reached the peer before the cut stays as a part
            // (the peer finishes writing it after the client gave up, so it
            // is measured by the second send, not by looking at the disk)
            client.set_progress_sink(None);
            let again = client.deliver(&b.id).await;
            assert!(again.success, "{:?}", again.errors);
            assert_eq!(again.sent, 1);
            assert!(again.bytes_moved > 0 && again.bytes_moved < 3 * 1024 * 1024 + 7, "the part was continued from, not resent: {} bytes", again.bytes_moved);
            assert!(on_b.is_file());
            assert_eq!(std::fs::metadata(&on_b).unwrap().len(), 3 * 1024 * 1024 + 7, "and the whole arrived");
            assert!(crate::sync_server::idle_seconds().is_none() || crate::sync_server::idle_seconds().unwrap() < 60);
            task.abort();
        }

        #[tokio::test]
        async fn what_is_on_this_device_only_is_counted_until_it_is_elsewhere() {
            let (a, b, _url, task) = pair();
            assert_eq!(a.db.lock().unwrap().not_duplicated(Some(&a.audio), &a.id).unwrap(), crate::database::NotDuplicated { notes: 0, recordings: 0 });
            a.db.lock().unwrap().create_note("רק כאן").unwrap();
            let (_id, _path) = recording(&a, 1000);
            assert_eq!(a.db.lock().unwrap().not_duplicated(Some(&a.audio), &a.id).unwrap(), crate::database::NotDuplicated { notes: 1, recordings: 1 }, "a note and a recording, on this device only");
            assert_eq!(a.db.lock().unwrap().not_duplicated(None, &a.id).unwrap(), crate::database::NotDuplicated { notes: 1, recordings: 0 }, "without an audio directory no recording is counted");

            let client = SyncClient::new(a.db.clone(), a.config.clone()).unwrap();
            let synced = client.sync_with_peer(&b.id).await;
            assert!(synced.success, "{:?}", synced.errors);
            assert_eq!(a.db.lock().unwrap().not_duplicated(Some(&a.audio), &a.id).unwrap(), crate::database::NotDuplicated { notes: 0, recordings: 1 }, "the note was sent; the file was not");

            let delivered = client.deliver(&b.id).await;
            assert!(delivered.success, "{:?}", delivered.errors);
            assert_eq!(a.db.lock().unwrap().not_duplicated(Some(&a.audio), &a.id).unwrap(), crate::database::NotDuplicated { notes: 0, recordings: 0 });

            // A note that came from B is not "on this device only"; an edit of it here is, until sent
            b.db.lock().unwrap().create_note("מ-B").unwrap();
            let synced = client.sync_with_peer(&b.id).await;
            assert!(synced.success);
            assert_eq!(a.db.lock().unwrap().not_duplicated(Some(&a.audio), &a.id).unwrap().notes, 0);
            let from_b = a.db.lock().unwrap().get_all_notes().unwrap().into_iter().find(|n| n.content == "מ-B").unwrap();
            a.db.lock().unwrap().update_note(&from_b.id, "מ-B, ערוך כאן").unwrap();
            assert_eq!(a.db.lock().unwrap().not_duplicated(Some(&a.audio), &a.id).unwrap().notes, 1);
            task.abort();
        }

        /// FILE-18: a send stores the sender's hash once and the peer receives
        /// it with the row at the next sync; the fetched bytes match it.
        #[tokio::test]
        async fn a_send_stores_the_hash_once_and_the_peer_learns_it_by_sync() {
            let (a, b, _url, task) = pair();
            let (id, path) = recording(&a, 1000);
            assert!(a.db.lock().unwrap().get_audio_file(&id).unwrap().unwrap().content_sha256.is_none());
            let client = SyncClient::new(a.db.clone(), a.config.clone()).unwrap();
            let result = client.deliver(&b.id).await;
            assert!(result.success, "{:?}", result.errors);
            let expected = crate::transfer::file_sha256(&path).unwrap();
            assert_eq!(a.db.lock().unwrap().get_audio_file(&id).unwrap().unwrap().content_sha256.as_deref(), Some(expected.as_str()), "the sender stored its hash");
            let result = client.deliver(&b.id).await;
            assert!(result.success, "{:?}", result.errors);
            assert_eq!(b.db.lock().unwrap().get_audio_file(&id).unwrap().unwrap().content_sha256.as_deref(), Some(expected.as_str()), "the peer received it with the row");
            assert_eq!(crate::transfer::file_sha256(&path_of(&b, &id)).unwrap(), expected);
            task.abort();
        }

        /// FILE-22 between two instances: a delivery makes the receiver state
        /// its copy, and the sender learns it at the next sync; a fetch makes
        /// the fetcher state its copy; a copy removed to save space, and a file
        /// deleted by hand from the folder, are known on the other device.
        #[tokio::test]
        async fn where_each_copy_is_is_known_on_both_devices() {
            let (a, b, _url, task) = pair();
            let (id_a, path_a) = recording(&a, 4000);
            let (id_b, _) = recording(&b, 3000);
            let client = SyncClient::new(a.db.clone(), a.config.clone()).unwrap();
            let holding = |d: &Device, id: &str| -> Vec<String> {
                let mut places = d.db.lock().unwrap().places_holding(id).unwrap();
                places.sort();
                places
            };
            let mut both = vec![a.id.clone(), b.id.clone()];
            both.sort();

            let result = client.exchange(&b.id).await;
            assert!(result.success, "{:?}", result.errors);
            assert_eq!((result.sent, result.fetched), (1, 1));
            assert_eq!(holding(&b, &id_a), both, "the receiver states its copy, and the sender's");
            assert_eq!(holding(&a, &id_b), both, "the fetcher states its copy, and the peer's");
            let synced = client.sync_with_peer(&b.id).await;
            assert!(synced.success, "{:?}", synced.errors);
            for id in [&id_a, &id_b] {
                assert_eq!(holding(&a, id), both);
                assert_eq!(holding(&b, id), both);
            }

            // A copy removed to save space on A, and B's file deleted from B's folder by hand
            let removed = client.remove_local_copy(&id_a).await.unwrap();
            assert!(removed.contains("B holds it"), "{}", removed);
            assert!(!path_a.exists());
            std::fs::remove_file(path_of(&b, &id_b)).unwrap();
            let synced = client.sync_with_peer(&b.id).await;
            assert!(synced.success, "{:?}", synced.errors);
            let b_client = {
                // B learns A's removal from A's push; A learns B's deletion when B states it
                b.db.lock().unwrap().check_files_here(&b.audio, &b.id).unwrap();
                client.sync_with_peer(&b.id).await
            };
            assert!(b_client.success, "{:?}", b_client.errors);
            assert_eq!(holding(&b, &id_a), vec![b.id.clone()], "B knows A removed its copy");
            assert_eq!(holding(&a, &id_b), vec![a.id.clone()], "A knows B's file is gone");
            assert_eq!(a.db.lock().unwrap().file_locations(&id_a).unwrap(), b.db.lock().unwrap().file_locations(&id_a).unwrap());
            assert_eq!(a.db.lock().unwrap().file_locations(&id_b).unwrap(), b.db.lock().unwrap().file_locations(&id_b).unwrap());
            task.abort();
        }

        /// FILE-26 between two instances: a copy that no other place is known
        /// to hold stays; A's copy goes once B promises to keep its own, and B's
        /// own removal is refused while the promise lasts; a device that is
        /// removing its copy promises nothing, and the refusal says so.
        #[tokio::test]
        async fn a_copy_goes_only_when_a_peer_promises_to_keep_its_own() {
            let (a, b, _url, task) = pair();
            let (id, path_a) = recording(&a, 5000);
            let (id2, path2_a) = recording(&a, 3000);
            let client = SyncClient::new(a.db.clone(), a.config.clone()).unwrap();
            let refused = client.remove_local_copy(&id).await.unwrap_err().to_string();
            assert!(refused.contains("no other place is known to hold it"), "{}", refused);
            assert!(path_a.is_file());

            let delivered = client.deliver(&b.id).await;
            assert!(delivered.success, "{:?}", delivered.errors);
            let removed = client.remove_local_copy(&id).await.unwrap();
            assert!(removed.contains("B holds it"), "{}", removed);
            assert!(!path_a.exists());
            assert!(path_of(&b, &id).is_file());
            let refused = b.db.lock().unwrap().begin_removal(&id).unwrap().unwrap_err();
            assert!(refused.contains("promised A"), "{}", refused);

            assert_eq!(b.db.lock().unwrap().begin_removal(&id2).unwrap(), Ok(()));
            let refused = client.remove_local_copy(&id2).await.unwrap_err().to_string();
            assert!(refused.contains("B does not keep it: this device is removing its own copy"), "{}", refused);
            assert!(path2_a.is_file(), "nothing confirmed: the copy stays");
            assert!(path_of(&b, &id2).is_file());
            task.abort();
        }

        #[tokio::test]
        async fn deliver_sends_but_does_not_fetch_and_send_alone_needs_no_sync() {
            let (a, b, _url, task) = pair();
            let (id_a, _) = recording(&a, 1000);
            let (id_b, _) = recording(&b, 1000);
            let client = SyncClient::new(a.db.clone(), a.config.clone()).unwrap();

            let result = client.deliver(&b.id).await;
            assert!(result.success, "{:?}", result.errors);
            assert_eq!((result.sent, result.fetched), (1, 0));
            assert!(path_of(&b, &id_a).is_file());
            assert!(!path_of(&a, &id_b).is_file(), "deliver fetches nothing");

            let fetched = client.fetch_from_peer(&b.id).await;
            assert!(fetched.success, "{:?}", fetched.errors);
            assert_eq!(fetched.fetched, 1);
            assert!(path_of(&a, &id_b).is_file());
            task.abort();
        }

        #[tokio::test]
        async fn a_stopped_transfer_continues_from_the_part_in_both_directions() {
            let (a, b, _url, task) = pair();
            let (id_a, path_a) = recording(&a, 100_000);
            let (id_b, path_b) = recording(&b, 100_000);
            let client = SyncClient::new(a.db.clone(), a.config.clone()).unwrap();
            // The rows must be on both sides before files can move
            assert!(client.sync_with_peer(&b.id).await.success);

            // B already holds the first 40,000 bytes of A's recording: a send continues from there
            let on_b = path_of(&b, &id_a);
            std::fs::write(crate::transfer::part_path(&on_b), &std::fs::read(&path_a).unwrap()[..40_000]).unwrap();
            // A already holds the first 25,000 bytes of B's recording: a fetch continues from there
            let on_a = path_of(&a, &id_b);
            std::fs::write(crate::transfer::part_path(&on_a), &std::fs::read(&path_b).unwrap()[..25_000]).unwrap();

            let result = client.exchange(&b.id).await;
            assert!(result.success, "{:?}", result.errors);
            assert_eq!(result.bytes_moved, 60_000 + 75_000, "only the missing bytes crossed the wire");
            assert_eq!(std::fs::read(&on_b).unwrap(), std::fs::read(&path_a).unwrap());
            assert_eq!(std::fs::read(&on_a).unwrap(), std::fs::read(&path_b).unwrap());
            task.abort();
        }

        #[tokio::test]
        async fn a_corrupt_part_is_discarded_and_the_file_fetched_whole() {
            let (a, b, _url, task) = pair();
            let (id_b, path_b) = recording(&b, 50_000);
            let client = SyncClient::new(a.db.clone(), a.config.clone()).unwrap();
            assert!(client.sync_with_peer(&b.id).await.success);
            let on_a = path_of(&a, &id_b);
            std::fs::write(crate::transfer::part_path(&on_a), vec![0u8; 10_000]).unwrap();

            // The first attempt assembles a wrong file, finds the hash does not agree and discards the part;
            // the retry fetches it whole.
            let result = client.fetch_from_peer(&b.id).await;
            assert!(result.success, "{:?}", result.errors);
            assert_eq!(std::fs::read(&on_a).unwrap(), std::fs::read(&path_b).unwrap());
            assert_eq!(result.bytes_moved, 50_000, "only the attempt that succeeded counts");
            task.abort();
        }

        #[tokio::test]
        async fn the_missing_list_answers_in_one_round_trip() {
            let (a, b, url, task) = pair();
            let (id_a, _) = recording(&a, 100);
            let (id_b, _) = recording(&b, 100);
            let client = SyncClient::new(a.db.clone(), a.config.clone()).unwrap();
            assert!(client.sync_with_peer(&b.id).await.success);
            let account = a.db.lock().unwrap().account_id().unwrap();
            let key = a.config.lock().unwrap().device_key().to_string();
            let resp = reqwest::Client::new()
                .post(format!("{}/sync/audio/missing", url))
                .header(auth::HEADER_ACCOUNT, &account)
                .header(auth::HEADER_DEVICE, &a.id)
                .bearer_auth(&key)
                .json(&crate::sync_protocol::MissingFilesRequest { audio_ids: vec![id_a.clone(), id_b.clone(), "00000000000070008000000000000000".to_string()] })
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 200);
            let body: crate::sync_protocol::MissingFilesResponse = resp.json().await.unwrap();
            assert_eq!(body.missing, vec![id_a], "B lacks A's file, holds its own, and ignores an id it has no row for");
            task.abort();
        }
    }

    mod listener {
        use super::*;
        use crate::config::Config;

        fn free_port() -> u16 {
            std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
        }

        async fn wait_for(url: &str) -> bool {
            let http = reqwest::Client::new();
            for _ in 0..50 {
                if http.get(format!("{}/sync/status", url)).send().await.map(|r| r.status() == 200).unwrap_or(false) {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            false
        }

        #[tokio::test]
        async fn a_listener_says_so_on_its_card_stops_when_asked_and_can_start_again() {
            let dir = TempDir::new().unwrap();
            let db = Arc::new(Mutex::new(Database::new(dir.path().join("notes.db")).unwrap()));
            let config = Arc::new(Mutex::new(Config::new(Some(dir.path().to_path_buf()), None).unwrap()));
            let device_id = config.lock().unwrap().device_id_hex().to_string();
            let port = free_port();
            let url = format!("http://127.0.0.1:{}", port);

            for round in 0..2 {
                let (db2, config2) = (db.clone(), config.clone());
                let serving = tokio::spawn(async move { start_server(db2, config2, "127.0.0.1", port, true).await });
                assert!(wait_for(&url).await, "round {}: the listener answers", round);
                assert!(server_running());
                let card = db.lock().unwrap().get_device_card(&device_id).unwrap().unwrap();
                assert_eq!(card.listens, "1");
                assert!(card.addresses.contains(&url), "{}", card.addresses);

                stop_server();
                let outcome = tokio::time::timeout(Duration::from_secs(10), serving).await.expect("the listener stops").unwrap();
                assert!(outcome.is_ok(), "{:?}", outcome.err());
                assert!(!server_running());
                let card = db.lock().unwrap().get_device_card(&device_id).unwrap().unwrap();
                assert_eq!(card.listens, "0", "round {}: the card says the listener stopped", round);
            }
        }

        #[test]
        fn listen_urls_name_private_addresses_and_the_host() {
            let urls = listen_urls("0.0.0.0", 8384, false);
            assert!(urls.iter().all(|u| u.starts_with("https://") && u.ends_with(":8384")), "{:?}", urls);
            assert!(!urls.iter().any(|u| u.contains("127.0.0.1")), "loopback is not an address for a peer");
            assert_eq!(listen_urls("192.168.1.10", 1, true), vec!["http://192.168.1.10:1"]);
        }
    }

    /// Create a SyncChange for testing
    fn make_sync_change(
        entity_type: &str,
        entity_id: &str,
        operation: &str,
        data: serde_json::Value,
        device_id: &str,
    ) -> SyncChange {
        let timestamp = data.get("modified_at")
            .or_else(|| data.get("deleted_at"))
            .or_else(|| data.get("created_at"))
            .and_then(|v| v.as_i64())
            .unwrap_or(1735689600); // 2025-01-01 00:00:00 UTC

        SyncChange {
            entity_type: entity_type.to_string(),
            entity_id: entity_id.to_string(),
            operation: operation.to_string(),
            data,
            timestamp,
            device_id: device_id.to_string(),
            device_name: Some("Remote Device".to_string()),
        }
    }

    // =========================================================================
    // NOTE CONTENT CONFLICT TESTS
    // =========================================================================

    // =========================================================================
    // NOTE DELETE CONFLICT TESTS
    // =========================================================================

    // =========================================================================
    // TAG RENAME CONFLICT TESTS
    // =========================================================================

    // =========================================================================
    // NOTE_TAG CONFLICT TESTS
    // =========================================================================

    // =========================================================================
    // COMPREHENSIVE DATA LOSS PREVENTION TESTS
    // =========================================================================

    #[test]
    fn test_apply_sync_changes_counts_conflicts_correctly() {
        // apply_sync_changes must report the conflicts flagged by the merge
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let note1_id = a.create_note("Note 1").unwrap();
        let note2_id = a.create_note("Note 2").unwrap();
        exchange(&a, &b);

        a.update_note(&note1_id, "Note 1 local edited").unwrap();
        a.update_note(&note2_id, "Note 2 local edited").unwrap();
        b.update_note(&note1_id, "Note 1 remote edit").unwrap();
        b.update_note(&note2_id, "Note 2 remote edit").unwrap();

        let (changes, _, _) = b.get_changes_after_seq_as_sync_changes(0, None, 100000).unwrap();
        let a = Arc::new(Mutex::new(a));
        let (applied, conflicts, errors) = apply_sync_changes(
            &a,
            &changes,
            DEV_B,
            Some("Remote"),
            None,
            None,
        ).unwrap();

        assert_eq!(conflicts, 2, "Expected 2 conflicts!");
        assert!(applied > 0, "the remote versions themselves are applied");
        assert!(errors.is_empty(), "Should be no errors");
        let db = a.lock().unwrap();
        assert_eq!(db.get_unresolved_conflict_counts().unwrap()["total"], 2);
    }

    // =========================================================================
    // P0: TAG PARENT_ID CONFLICT TESTS
    // =========================================================================

    // =========================================================================
    // P0: TAG DELETION CONFLICT TESTS
    // =========================================================================

    // =========================================================================
    // P1: IDENTICAL CONTENT OPTIMIZATION TESTS
    // =========================================================================

    // =========================================================================
    // PARTIAL BATCH FAILURE TESTS
    // =========================================================================

    #[test]
    fn test_partial_batch_failure_continues_processing() {
        // CRITICAL: If change 5 of 10 fails, changes 1-4 should already be applied
        // and changes 6-10 should still be attempted and applied if valid.
        // We do NOT wrap everything in a transaction that rolls back on failure.
        let (db, _temp) = create_test_db();
        let db = std::sync::Arc::new(std::sync::Mutex::new(db));

        // Create 3 valid notes that will be created by changes 1, 3, 5
        let note1_id = uuid::Uuid::now_v7().simple().to_string();
        let note2_id = uuid::Uuid::now_v7().simple().to_string();
        let note3_id = uuid::Uuid::now_v7().simple().to_string();

        let changes = vec![
            // Change 1: Valid note create
            make_sync_change(
                "note",
                &note1_id,
                "create",
                serde_json::json!({
                    "id": note1_id,
                    "created_at": 1735689600,
                    "content": "Note 1 - should be created",
                    "modified_at": null,
                    "deleted_at": null,
                }),
                "00000000000070008000000000000099",
            ),
            // Change 2: Invalid entity type - will error
            make_sync_change(
                "invalid_type",
                "some_id",
                "create",
                serde_json::json!({}),
                "00000000000070008000000000000099",
            ),
            // Change 3: Valid note create - should still be processed after error
            make_sync_change(
                "note",
                &note2_id,
                "create",
                serde_json::json!({
                    "id": note2_id,
                    "created_at": 1735689600,
                    "content": "Note 2 - should be created despite earlier error",
                    "modified_at": null,
                    "deleted_at": null,
                }),
                "00000000000070008000000000000099",
            ),
            // Change 4: Another invalid type - will error
            make_sync_change(
                "bogus",
                "another_id",
                "update",
                serde_json::json!({}),
                "00000000000070008000000000000099",
            ),
            // Change 5: Valid note create - should still be processed
            make_sync_change(
                "note",
                &note3_id,
                "create",
                serde_json::json!({
                    "id": note3_id,
                    "created_at": 1735689600,
                    "content": "Note 3 - should be created despite multiple errors",
                    "modified_at": null,
                    "deleted_at": null,
                }),
                "00000000000070008000000000000099",
            ),
        ];

        let (applied, _, errors) = apply_sync_changes(
            &db,
            &changes,
            "00000000000070008000000000000099",
            Some("Test Device"),
            None,
            None,
        ).unwrap();

        // Should have 3 successful applications
        assert_eq!(applied, 3, "All 3 valid notes should be applied!");

        // Should have 2 errors (the invalid entity types)
        assert_eq!(errors.len(), 2, "Should have 2 errors for invalid types");

        // Verify all 3 notes actually exist in the database
        let db_lock = db.lock().unwrap();

        let note1 = db_lock.get_note(&note1_id).unwrap();
        assert!(note1.is_some(), "Note 1 should exist - change before errors");

        let note2 = db_lock.get_note(&note2_id).unwrap();
        assert!(note2.is_some(), "Note 2 should exist - change after first error");

        let note3 = db_lock.get_note(&note3_id).unwrap();
        assert!(note3.is_some(), "Note 3 should exist - change after second error");
    }

    #[test]
    fn test_batch_with_all_failures_reports_all_errors() {
        // All changes fail - each should be reported individually
        let (db, _temp) = create_test_db();
        let db = std::sync::Arc::new(std::sync::Mutex::new(db));

        let changes = vec![
            make_sync_change("invalid1", "id1", "create", serde_json::json!({}), "00000000000070008000000000000099"),
            make_sync_change("invalid2", "id2", "create", serde_json::json!({}), "00000000000070008000000000000099"),
            make_sync_change("invalid3", "id3", "create", serde_json::json!({}), "00000000000070008000000000000099"),
        ];

        let (applied, conflicts, errors) = apply_sync_changes(
            &db,
            &changes,
            "00000000000070008000000000000099",
            Some("Test"),
            None,
            None,
        ).unwrap();

        assert_eq!(applied, 0);
        assert_eq!(conflicts, 0);
        assert_eq!(errors.len(), 3, "Each failure should be reported");
    }

    // =========================================================================
    // TWO-INSTANCE SYNC TESTS - CRITICAL FOR VERIFYING SYNC PROPAGATION
    // These tests simulate two separate VoiceCore instances syncing through
    // a shared database (like the sync server scenario).
    // =========================================================================

    #[test]
    fn test_two_instances_sync_deleted_note() {
        // CRITICAL: This test verifies that when Instance A deletes a note,
        // the deletion is properly propagated to Instance B via sync.

        // Create two separate databases (simulating two devices)
        let (instance_a, _temp_a) = create_test_db();
        let (instance_b, _temp_b) = create_test_db();

        // Create a note on Instance A
        let note_id = instance_a.create_note("Shared note content").unwrap();
        let note = instance_a.get_note(&note_id).unwrap().unwrap();

        // Sync the note to Instance B (simulating initial sync)
        instance_b.apply_sync_note(
            &note_id,
            note.created_at,
            &note.content,
            None,
            None,
            None,
            None,
        ).unwrap();

        // Verify Instance B has the note
        let b_note = instance_b.get_note(&note_id).unwrap();
        assert!(b_note.is_some(), "Instance B should have the note after initial sync");

        // Now Instance A deletes the note
        instance_a.delete_note(&note_id).unwrap();

        // Verify Instance A sees the note as deleted
        let a_note = instance_a.get_note(&note_id).unwrap();
        assert!(a_note.is_none(), "Instance A should NOT see deleted note via get_note");

        // Get the changes from Instance A (this is what gets sent to the server)
        let changes = instance_a.get_changes_after_seq(0, None, 100).unwrap().changes;

        // Find the delete change for our note
        let delete_change = changes.iter().find(|c| {
            c.get("entity_type").and_then(|v| v.as_str()) == Some("note") &&
            c.get("entity_id").and_then(|v| v.as_str()) == Some(&note_id) &&
            c.get("operation").and_then(|v| v.as_str()) == Some("delete")
        });

        assert!(delete_change.is_some(),
            "CRITICAL: Instance A should report the delete operation in get_changes_after_seq!");

        // Extract data from the change to apply to Instance B
        let change = delete_change.unwrap();
        let data = change.get("data").unwrap();
        let deleted_at = data.get("deleted_at").and_then(|v| v.as_i64());
        let modified_at = data.get("modified_at").and_then(|v| v.as_i64());
        let created_at = data.get("created_at").and_then(|v| v.as_i64()).unwrap_or(0);
        let content = data.get("content").and_then(|v| v.as_str()).unwrap_or("");

        // Apply the delete to Instance B
        instance_b.apply_sync_note(
            &note_id,
            created_at,
            content,
            modified_at,
            deleted_at,
            None,
            None,
        ).unwrap();

        // Verify Instance B now sees the note as deleted
        let b_note_after = instance_b.get_note(&note_id).unwrap();
        assert!(b_note_after.is_none(),
            "CRITICAL: Instance B should NOT see the note after syncing the delete!");
    }

    #[test]
    fn test_two_instances_sync_deleted_tag() {
        // CRITICAL: This test verifies that when Instance A deletes a tag,
        // the deletion is properly propagated to Instance B via sync.

        // Create two separate databases (simulating two devices)
        let (instance_a, _temp_a) = create_test_db();
        let (instance_b, _temp_b) = create_test_db();

        // Create a tag on Instance A
        let tag_id = instance_a.create_tag("Shared tag", None).unwrap();
        let tag = instance_a.get_tag(&tag_id).unwrap().unwrap();

        // Sync the tag to Instance B (simulating initial sync)
        instance_b.apply_sync_tag(
            &tag_id,
            &tag.name,
            None,
            tag.created_at.unwrap_or(0),
            None,
            None,
        ).unwrap();

        // Verify Instance B has the tag
        let b_tag = instance_b.get_tag(&tag_id).unwrap();
        assert!(b_tag.is_some(), "Instance B should have the tag after initial sync");

        // Now Instance A deletes the tag (soft delete)
        instance_a.delete_tag(&tag_id).unwrap();

        // Verify Instance A sees the tag as deleted
        let a_tag = instance_a.get_tag(&tag_id).unwrap();
        assert!(a_tag.is_none(), "Instance A should NOT see deleted tag via get_tag");

        // Get the changes from Instance A (this is what gets sent to the server)
        let changes = instance_a.get_changes_after_seq(0, None, 100).unwrap().changes;

        // Find the delete change for our tag
        let delete_change = changes.iter().find(|c| {
            c.get("entity_type").and_then(|v| v.as_str()) == Some("tag") &&
            c.get("entity_id").and_then(|v| v.as_str()) == Some(&tag_id) &&
            c.get("operation").and_then(|v| v.as_str()) == Some("delete")
        });

        assert!(delete_change.is_some(),
            "CRITICAL: Instance A should report the tag delete operation in get_changes_after_seq!");

        // Extract data from the change to apply to Instance B
        let change = delete_change.unwrap();
        let data = change.get("data").unwrap();
        let deleted_at = data.get("deleted_at").and_then(|v| v.as_i64());
        let modified_at = data.get("modified_at").and_then(|v| v.as_i64());
        let created_at = data.get("created_at").and_then(|v| v.as_i64()).unwrap_or(0);
        let name = data.get("name").and_then(|v| v.as_str()).unwrap_or("");
        let parent_id = data.get("parent_id").and_then(|v| v.as_str());

        // Apply the delete to Instance B using the new method
        instance_b.apply_sync_tag_with_deleted(
            &tag_id,
            name,
            parent_id,
            created_at,
            modified_at,
            deleted_at,
            None,
        ).unwrap();

        // Verify Instance B now sees the tag as deleted
        let b_tag_after = instance_b.get_tag(&tag_id).unwrap();
        assert!(b_tag_after.is_none(),
            "CRITICAL: Instance B should NOT see the tag after syncing the delete!");
    }

    #[test]
    fn test_get_changes_after_seq_includes_deleted_notes() {
        // Verify that get_changes_after_seq properly reports deleted notes
        let (db, _temp) = create_test_db();

        // Create and delete a note
        let note_id = db.create_note("Test note").unwrap();
        db.delete_note(&note_id).unwrap();

        // Get changes
        let changes = db.get_changes_after_seq(0, None, 100).unwrap().changes;

        // Find the change for our note
        let note_change = changes.iter().find(|c| {
            c.get("entity_type").and_then(|v| v.as_str()) == Some("note") &&
            c.get("entity_id").and_then(|v| v.as_str()) == Some(&note_id)
        });

        assert!(note_change.is_some(), "Deleted note should appear in get_changes_after_seq");

        let change = note_change.unwrap();
        assert_eq!(
            change.get("operation").and_then(|v| v.as_str()),
            Some("delete"),
            "Operation should be 'delete' for deleted note"
        );

        let data = change.get("data").unwrap();
        assert!(
            data.get("deleted_at").is_some() && !data.get("deleted_at").unwrap().is_null(),
            "deleted_at should be set in the data"
        );
    }

    #[test]
    fn test_get_changes_after_seq_includes_deleted_tags() {
        // Verify that get_changes_after_seq properly reports deleted tags
        let (db, _temp) = create_test_db();

        // Create and delete a tag
        let tag_id = db.create_tag("Test tag", None).unwrap();
        db.delete_tag(&tag_id).unwrap();

        // Get changes
        let changes = db.get_changes_after_seq(0, None, 100).unwrap().changes;

        // Find the change for our tag
        let tag_change = changes.iter().find(|c| {
            c.get("entity_type").and_then(|v| v.as_str()) == Some("tag") &&
            c.get("entity_id").and_then(|v| v.as_str()) == Some(&tag_id)
        });

        assert!(tag_change.is_some(), "Deleted tag should appear in get_changes_after_seq");

        let change = tag_change.unwrap();
        assert_eq!(
            change.get("operation").and_then(|v| v.as_str()),
            Some("delete"),
            "Operation should be 'delete' for deleted tag"
        );

        let data = change.get("data").unwrap();
        assert!(
            data.get("deleted_at").is_some() && !data.get("deleted_at").unwrap().is_null(),
            "deleted_at should be set in the tag data"
        );
    }

    #[test]
    fn test_two_instances_sync_note_with_audio_attachment() {
        // CRITICAL: Verify that a note with an audio attachment syncs correctly
        // between two instances

        let (instance_a, _temp_a) = create_test_db();
        let (instance_b, _temp_b) = create_test_db();

        // Create a note and audio file on Instance A
        let note_id = instance_a.create_note("Note with audio").unwrap();
        let audio_id = instance_a.create_audio_file("recording.mp3", Some(1735732800), None, crate::models::FileOrigin::Imported, None).unwrap();

        // Attach audio to note
        let _attachment_id = instance_a.attach_to_note(&note_id, &audio_id, "audio_file").unwrap();

        // Get changes from Instance A
        let changes = instance_a.get_changes_after_seq(0, None, 100).unwrap().changes;

        // Should have: note, audio_file, note_attachment
        let note_change = changes.iter().find(|c| {
            c.get("entity_type").and_then(|v| v.as_str()) == Some("note") &&
            c.get("entity_id").and_then(|v| v.as_str()) == Some(&note_id)
        });
        assert!(note_change.is_some(), "Note should be in changes");

        let audio_change = changes.iter().find(|c| {
            c.get("entity_type").and_then(|v| v.as_str()) == Some("audio_file") &&
            c.get("entity_id").and_then(|v| v.as_str()) == Some(&audio_id)
        });
        assert!(audio_change.is_some(), "Audio file should be in changes");

        let attachment_change = changes.iter().find(|c| {
            c.get("entity_type").and_then(|v| v.as_str()) == Some("note_attachment")
        });
        assert!(attachment_change.is_some(), "Note attachment should be in changes");

        // Apply note to Instance B
        let note_data = note_change.unwrap().get("data").unwrap();
        instance_b.apply_sync_note(
            &note_id,
            note_data.get("created_at").and_then(|v| v.as_i64()).unwrap_or(0),
            note_data.get("content").and_then(|v| v.as_str()).unwrap_or(""),
            note_data.get("modified_at").and_then(|v| v.as_i64()),
            note_data.get("deleted_at").and_then(|v| v.as_i64()),
            None,
            None,
        ).unwrap();

        // Apply audio file to Instance B
        let audio_data = audio_change.unwrap().get("data").unwrap();
        instance_b.apply_sync_audio_file(&audio_id, audio_data.get("imported_at").and_then(|v| v.as_i64()).unwrap_or(0), audio_data.get("filename").and_then(|v| v.as_str()).unwrap_or(""), audio_data.get("file_created_at").and_then(|v| v.as_i64()), audio_data.get("duration_seconds").and_then(|v| v.as_i64()), audio_data.get("summary").and_then(|v| v.as_str()), audio_data.get("modified_at").and_then(|v| v.as_i64()), audio_data.get("deleted_at").and_then(|v| v.as_i64()), None, audio_data.get("storage_provider").and_then(|v| v.as_str()), audio_data.get("storage_key").and_then(|v| v.as_str()), audio_data.get("storage_uploaded_at").and_then(|v| v.as_i64()), None, None, None, None, None).unwrap();

        // Apply attachment to Instance B
        let att_data = attachment_change.unwrap().get("data").unwrap();
        let att_entity_id = attachment_change.unwrap().get("entity_id").and_then(|v| v.as_str()).unwrap();
        instance_b.apply_sync_note_attachment(
            att_entity_id,
            att_data.get("note_id").and_then(|v| v.as_str()).unwrap_or(""),
            att_data.get("attachment_id").and_then(|v| v.as_str()).unwrap_or(""),
            att_data.get("attachment_type").and_then(|v| v.as_str()).unwrap_or(""),
            att_data.get("created_at").and_then(|v| v.as_i64()).unwrap_or(0),
            att_data.get("modified_at").and_then(|v| v.as_i64()),
            att_data.get("deleted_at").and_then(|v| v.as_i64()),
            None,
        ).unwrap();

        // Verify Instance B has the note
        let b_note = instance_b.get_note(&note_id).unwrap();
        assert!(b_note.is_some(), "Instance B should have the note");
        assert_eq!(b_note.unwrap().content, "Note with audio");

        // Verify Instance B has the audio file
        let b_audio = instance_b.get_audio_file(&audio_id).unwrap();
        assert!(b_audio.is_some(), "Instance B should have the audio file");
        assert_eq!(b_audio.unwrap().filename, "recording.mp3");

        // Verify the attachment relationship exists on Instance B
        let b_attachments = instance_b.get_attachments_for_note(&note_id).unwrap();
        assert_eq!(b_attachments.len(), 1, "Instance B should have 1 attachment on the note");
        assert_eq!(b_attachments[0].attachment_id, audio_id);
    }

    #[test]
    fn test_two_instances_sync_detached_attachment() {
        // Verify that detaching an attachment syncs correctly
        let (instance_a, _temp_a) = create_test_db();
        let (instance_b, _temp_b) = create_test_db();

        // Create note with attachment on Instance A
        let note_id = instance_a.create_note("Note with attachment").unwrap();
        let audio_id = instance_a.create_audio_file("recording.mp3", None, None, crate::models::FileOrigin::Imported, None).unwrap();
        let attachment_id = instance_a.attach_to_note(&note_id, &audio_id, "audio_file").unwrap();

        // Initial sync A -> B
        push_all(&instance_a, &instance_b, DEV_A);
        let b_attachments = instance_b.get_attachments_for_note(&note_id).unwrap();
        assert_eq!(b_attachments.len(), 1, "Instance B should have attachment after initial sync");

        // Now Instance A detaches the attachment
        instance_a.detach_from_note(&attachment_id).unwrap();

        // The feed carries the detach
        let changes2 = instance_a.get_changes_after_seq(0, None, 100).unwrap().changes;
        let detach_change = changes2.iter().find(|c| {
            c.get("entity_type").and_then(|v| v.as_str()) == Some("note_attachment") &&
            c.get("operation").and_then(|v| v.as_str()) == Some("delete")
        });
        assert!(detach_change.is_some(), "Should have a delete operation for the attachment");

        // Sync again A -> B
        push_all(&instance_a, &instance_b, DEV_A);

        // Verify Instance B no longer shows the attachment
        let b_attachments_after = instance_b.get_attachments_for_note(&note_id).unwrap();
        assert_eq!(b_attachments_after.len(), 0,
            "CRITICAL: Instance B should have 0 attachments after syncing the detach");
    }

    // =========================================================================
    // TRANSCRIPTION SYNC TESTS
    // =========================================================================

    #[test]
    fn test_two_instances_sync_transcription() {
        // Verify that transcriptions sync correctly between instances

        let (instance_a, _temp_a) = create_test_db();
        let (instance_b, _temp_b) = create_test_db();

        // Create an audio file on Instance A
        let audio_id = instance_a.create_audio_file("speech.mp3", None, None, crate::models::FileOrigin::Imported, None).unwrap();

        // Create a transcription for it
        let transcription_id = instance_a.create_transcription(
            &audio_id,
            "This is the transcribed text from the audio.",  // content
            None,                                              // content_segments
            "whisper",                                         // service
            None,                                              // service_arguments
            None,                                              // service_response
            None,                                              // state (uses default)
        ).unwrap();

        // Get changes from Instance A
        let changes = instance_a.get_changes_after_seq(0, None, 100).unwrap().changes;

        // Apply audio file to Instance B first
        let audio_change = changes.iter().find(|c| {
            c.get("entity_type").and_then(|v| v.as_str()) == Some("audio_file")
        }).unwrap();
        let audio_data = audio_change.get("data").unwrap();
        instance_b.apply_sync_audio_file(&audio_id, audio_data.get("imported_at").and_then(|v| v.as_i64()).unwrap_or(0), audio_data.get("filename").and_then(|v| v.as_str()).unwrap_or(""), audio_data.get("file_created_at").and_then(|v| v.as_i64()), audio_data.get("duration_seconds").and_then(|v| v.as_i64()), audio_data.get("summary").and_then(|v| v.as_str()), audio_data.get("modified_at").and_then(|v| v.as_i64()), audio_data.get("deleted_at").and_then(|v| v.as_i64()), None, audio_data.get("storage_provider").and_then(|v| v.as_str()), audio_data.get("storage_key").and_then(|v| v.as_str()), audio_data.get("storage_uploaded_at").and_then(|v| v.as_i64()), None, None, None, None, None).unwrap();

        // Find and apply transcription
        let trans_change = changes.iter().find(|c| {
            c.get("entity_type").and_then(|v| v.as_str()) == Some("transcription")
        });

        assert!(trans_change.is_some(), "Transcription should be in changes");

        let trans_data = trans_change.unwrap().get("data").unwrap();
        instance_b.apply_sync_transcription(
            &transcription_id,
            trans_data.get("audio_file_id").and_then(|v| v.as_str()).unwrap_or(""),
            trans_data.get("content").and_then(|v| v.as_str()).unwrap_or(""),
            trans_data.get("content_segments").and_then(|v| v.as_str()),
            trans_data.get("service").and_then(|v| v.as_str()).unwrap_or(""),
            trans_data.get("service_arguments").and_then(|v| v.as_str()),
            trans_data.get("service_response").and_then(|v| v.as_str()),
            trans_data.get("state").and_then(|v| v.as_str()).unwrap_or(""),
            trans_data.get("device_id").and_then(|v| v.as_str()).unwrap_or(""),
            trans_data.get("created_at").and_then(|v| v.as_i64()).unwrap_or(0),
            trans_data.get("modified_at").and_then(|v| v.as_i64()),
            trans_data.get("deleted_at").and_then(|v| v.as_i64()),
            None,
        ).unwrap();

        // Verify Instance B has the transcription
        let b_transcriptions = instance_b.get_transcriptions_for_audio_file(&audio_id).unwrap();
        assert_eq!(b_transcriptions.len(), 1, "Instance B should have 1 transcription");
        assert_eq!(b_transcriptions[0].content, "This is the transcribed text from the audio.");
        assert_eq!(b_transcriptions[0].service, "whisper");
    }

    #[test]
    fn test_get_changes_after_seq_returns_all_entity_types() {
        // CRITICAL TEST: Ensures get_changes_after_seq returns ALL syncable entity types.
        // This test exists because we had a bug where transcriptions were missing
        // from get_changes_after_seq, causing them to never sync to clients.
        //
        // If this test fails after adding a new entity type, you need to:
        // 1. Add the entity type to ALL_SYNC_ENTITY_TYPES above
        // 2. Add the query for that entity type in get_changes_after_seq()
        // 3. Create test data for it below

        let (db, _temp) = create_test_db();

        // Create one of each entity type
        let note_id = db.create_note("Test note content").unwrap();
        let tag_id = db.create_tag("TestTag", None).unwrap();
        db.add_tag_to_note(&note_id, &tag_id).unwrap();
        let audio_id = db.create_audio_file("test.mp3", None, None, crate::models::FileOrigin::Imported, None).unwrap();
        let _attachment_id = db.attach_to_note(&note_id, &audio_id, "audio_file").unwrap();
        let _transcription_id = db.create_transcription(
            &audio_id,
            "Test transcription content",
            None,  // content_segments
            "whisper",
            None,  // service_arguments
            None,  // service_response
            None,  // state (uses default)
        ).unwrap();

        // Create file_storage_config
        let config_json = serde_json::json!({
            "bucket": "test-bucket",
            "region": "us-east-1",
        });
        db.set_file_storage_config("s3", Some(&config_json)).unwrap();
        // Where a copy is (FILE-22): the upload states that the bucket holds it
        db.update_audio_file_storage(&audio_id, "s3", "test.mp3", false).unwrap();

        // A note emptied out of the trash: the purge travels too, or the
        // other devices would keep the note for ever.
        let doomed = db.create_note("פתק שנמחק לתמיד").unwrap();
        db.delete_note(&doomed).unwrap();
        db.purge_note(&doomed).unwrap();

        // Get all changes
        let changes = db.get_changes_after_seq(0, None, 1000).unwrap().changes;

        // Collect the entity types we got
        let mut found_types: std::collections::HashSet<String> = std::collections::HashSet::new();
        for change in &changes {
            if let Some(entity_type) = change.get("entity_type").and_then(|v| v.as_str()) {
                found_types.insert(entity_type.to_string());
            }
        }

        // Verify ALL expected entity types are present
        for expected_type in ALL_SYNC_ENTITY_TYPES {
            assert!(
                found_types.contains(*expected_type),
                "CRITICAL: get_changes_after_seq is missing entity type '{}'. \
                 Found types: {:?}. \
                 This will cause {} entities to never sync to clients! \
                 Add the query for '{}' to get_changes_after_seq().",
                expected_type,
                found_types,
                expected_type,
                expected_type
            );
        }

        // Also verify we didn't get any unexpected types
        for found_type in &found_types {
            assert!(
                ALL_SYNC_ENTITY_TYPES.contains(&found_type.as_str()),
                "Unexpected entity type '{}' in get_changes_after_seq. \
                 If this is a new entity type, add it to ALL_SYNC_ENTITY_TYPES.",
                found_type
            );
        }
    }

    #[test]
    fn test_get_changes_after_seq_returns_modified_transcription() {
        // Specific test for the bug where transcription state changes weren't syncing.
        // When a transcription is modified (e.g., state changed from "original" to "verified"),
        // it must appear in get_changes_after_seq.

        let (db, _temp) = create_test_db();

        // Create audio file and transcription
        let audio_id = db.create_audio_file("test.mp3", None, None, crate::models::FileOrigin::Imported, None).unwrap();
        let transcription_id = db.create_transcription(
            &audio_id,
            "Test content",
            None,
            "whisper",
            None,
            None,
            None,  // state (uses default)
        ).unwrap();

        // Where the feed stood at the last sync
        let cursor = db.current_seq().unwrap();

        // Modify the transcription (simulate changing state to "verified")
        db.update_transcription(&transcription_id, "Hello world", None, None, Some("verified")).unwrap();

        // Get changes since last sync
        let changes = db.get_changes_after_seq(cursor, None, 1000).unwrap().changes;

        // Find the transcription change
        let trans_change = changes.iter().find(|c| {
            c.get("entity_type").and_then(|v| v.as_str()) == Some("transcription") &&
            c.get("entity_id").and_then(|v| v.as_str()) == Some(&transcription_id)
        });

        assert!(
            trans_change.is_some(),
            "Modified transcription must appear in get_changes_after_seq! \
             This bug caused transcription state changes to never sync."
        );

        // Verify the state was updated
        let data = trans_change.unwrap().get("data").unwrap();
        assert_eq!(
            data.get("state").and_then(|v| v.as_str()),
            Some("verified"),
            "Transcription state should be 'verified'"
        );
    }

    // =========================================================================
    // VERSIONED MERGE TESTS (Git-style history, see versions.rs)
    // =========================================================================

    /// Push every change from `from` to `to`, as a sync would.
    fn push_all(from: &Database, to: &Database, from_device: &str) -> (i64, i64, Vec<String>) {
        let (changes, _, _) = from.get_changes_after_seq_as_sync_changes(0, None, 100000).unwrap();
        let changes: Vec<SyncChange> = changes
            .into_iter()
            .map(|mut c| {
                c.device_id = from_device.to_string();
                c.device_name = Some(format!("Device {}", &from_device[30..]));
                c
            })
            .collect();
        apply_changes_from_peer(to, &changes, from_device, None, None, None).unwrap()
    }

    const DEV_A: &str = "00000000000070008000000000000aaa";
    const DEV_B: &str = "00000000000070008000000000000bbb";

    /// Full exchange in both directions, twice, so that merges made on one side
    /// reach the other and any resulting conflict records line up.
    fn exchange(a: &Database, b: &Database) {
        push_all(a, b, DEV_A);
        push_all(b, a, DEV_B);
        push_all(a, b, DEV_A);
    }

    fn content(db: &Database, note_id: &str) -> String {
        db.get_note(note_id).unwrap().unwrap().content
    }

    fn head_hex(db: &Database, entity_type: &str, entity_id: &str, field: &str) -> String {
        crate::versions::hex(&db.head_id(entity_type, entity_id, field).unwrap().unwrap())
    }

    /// One attachment of a note can be marked as the one that stands for
    /// it, and the choice travels to the other devices.
    #[test]
    fn the_attachment_that_stands_for_a_note_is_remembered_and_travels() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let note_id = a.create_note("פתק עם שתי הקלטות").unwrap();
        let first = a.create_audio_file("first.ogg", None, None, crate::models::FileOrigin::Imported, None).unwrap();
        let second = a.create_audio_file("second.ogg", None, None, crate::models::FileOrigin::Imported, None).unwrap();
        let first_attachment = a.attach_to_note(&note_id, &first, "audio_file").unwrap();
        let second_attachment = a.attach_to_note(&note_id, &second, "audio_file").unwrap();
        exchange(&a, &b);

        // Nothing chosen: the note says so, and the reader falls back to the
        // first recording.
        assert_eq!(a.get_primary_attachment(&note_id).unwrap(), None);

        a.set_primary_attachment(&note_id, Some(&second_attachment)).unwrap();
        assert_eq!(a.get_primary_attachment(&note_id).unwrap().as_deref(), Some(second_attachment.as_str()));
        exchange(&a, &b);
        assert_eq!(
            b.get_primary_attachment(&note_id).unwrap().as_deref(),
            Some(second_attachment.as_str()),
            "the choice reached the other device"
        );

        // Changing it again travels too, and going back to none is a choice
        // like any other.
        a.set_primary_attachment(&note_id, Some(&first_attachment)).unwrap();
        exchange(&a, &b);
        assert_eq!(b.get_primary_attachment(&note_id).unwrap().as_deref(), Some(first_attachment.as_str()));

        a.set_primary_attachment(&note_id, None).unwrap();
        exchange(&a, &b);
        assert_eq!(b.get_primary_attachment(&note_id).unwrap(), None);
    }

    /// An attachment of another note cannot be made this note's primary.
    #[test]
    fn a_note_can_only_point_at_its_own_attachment() {
        let (db, _t) = create_test_db();
        let mine = db.create_note("הפתק שלי").unwrap();
        let other = db.create_note("פתק אחר").unwrap();
        let audio = db.create_audio_file("elsewhere.ogg", None, None, crate::models::FileOrigin::Imported, None).unwrap();
        let attachment = db.attach_to_note(&other, &audio, "audio_file").unwrap();

        assert!(db.set_primary_attachment(&mine, Some(&attachment)).is_err());
        assert_eq!(db.get_primary_attachment(&mine).unwrap(), None);
    }

    /// The same for the transcription that stands for a recording.
    #[test]
    fn the_transcription_that_stands_for_a_recording_travels() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let audio = a.create_audio_file("recording.ogg", None, None, crate::models::FileOrigin::Imported, None).unwrap();
        let first = a
            .create_transcription(&audio, "תמלול ראשון", None, "local_whisper", None, None, None)
            .unwrap();
        let second = a
            .create_transcription(&audio, "תמלול שני", None, "local_whisper", None, None, None)
            .unwrap();
        exchange(&a, &b);
        assert_eq!(a.get_primary_transcription(&audio).unwrap(), None);

        a.set_primary_transcription(&audio, Some(&second)).unwrap();
        exchange(&a, &b);
        assert_eq!(b.get_primary_transcription(&audio).unwrap().as_deref(), Some(second.as_str()));

        // And a transcription of another recording is refused.
        let elsewhere = a.create_audio_file("other.ogg", None, None, crate::models::FileOrigin::Imported, None).unwrap();
        assert!(a.set_primary_transcription(&elsewhere, Some(&first)).is_err());
    }

    // =================================================================
    // The trash bin
    // =================================================================

    /// A deleted note is in the trash, not gone: it is listed there, and
    /// recovering it puts it back in front of the user.
    #[test]
    fn a_deleted_note_waits_in_the_trash_and_can_be_recovered() {
        let (db, _t) = create_test_db();
        let note_id = db.create_note("פתק שנמחק בטעות").unwrap();
        assert!(db.get_deleted_notes().unwrap().is_empty());

        db.delete_note(&note_id).unwrap();
        assert!(db.get_note(&note_id).unwrap().is_none(), "a deleted note leaves the list");
        let trash = db.get_deleted_notes().unwrap();
        assert_eq!(trash.len(), 1);
        assert_eq!(trash[0].id, note_id);
        assert_eq!(trash[0].content, "פתק שנמחק בטעות");
        assert!(trash[0].deleted_at.is_some());

        assert!(db.undelete_note(&note_id).unwrap());
        assert!(db.get_deleted_notes().unwrap().is_empty());
        let back = db.get_note(&note_id).unwrap().unwrap();
        assert_eq!(back.content, "פתק שנמחק בטעות", "the text comes back as it was");
        assert!(back.deleted_at.is_none());

        // Recovering a note that is not in the trash says so rather than
        // pretending to have done something.
        assert!(!db.undelete_note(&note_id).unwrap());
    }

    /// A recovery reaches the other devices, and the note comes back there
    /// too.
    #[test]
    fn a_recovery_travels_to_the_other_device() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let note_id = a.create_note("פתק משותף").unwrap();
        exchange(&a, &b);
        a.delete_note(&note_id).unwrap();
        exchange(&a, &b);
        assert!(b.get_note(&note_id).unwrap().is_none(), "the delete travelled");
        assert_eq!(b.get_deleted_notes().unwrap().len(), 1, "and it is in B's trash");

        a.undelete_note(&note_id).unwrap();
        exchange(&a, &b);

        assert!(b.get_note(&note_id).unwrap().is_some(), "the recovery travelled");
        assert!(b.get_deleted_notes().unwrap().is_empty());
    }

    /// Emptying a note out of the trash takes its recordings and their
    /// transcriptions with it, and only a note that is already in the trash
    /// can be emptied.
    #[test]
    fn purging_a_note_removes_it_and_what_belonged_only_to_it() {
        let (db, _t) = create_test_db();
        let note_id = db.create_note("פתק עם הקלטה").unwrap();
        let audio_id = db.create_audio_file("recording.ogg", None, None, crate::models::FileOrigin::Imported, None).unwrap();
        db.attach_to_note(&note_id, &audio_id, "audio_file").unwrap();
        let transcription_id = db
            .create_transcription(&audio_id, "תמלול ההקלטה", None, "local_whisper", None, None, None)
            .unwrap();

        // A note that is still in the list cannot be emptied out of a trash
        // it is not in.
        assert!(db.purge_note(&note_id).is_err());

        db.delete_note(&note_id).unwrap();
        let removed_audio: Vec<String> = db.purge_note(&note_id).unwrap().into_iter().map(|r| r.id).collect();
        assert_eq!(removed_audio, vec![audio_id.clone()], "the caller is told which files to delete");

        assert!(db.get_note_raw(&note_id).unwrap().is_none(), "the note itself is gone");
        assert!(db.get_deleted_notes().unwrap().is_empty(), "and it has left the trash");
        assert!(db.get_audio_file(&audio_id).unwrap().is_none(), "its recording went with it");
        assert!(db.get_transcription(&transcription_id).unwrap().is_none(), "and the transcription");
        assert!(db.is_purged("note", &note_id).unwrap());
        assert!(db.is_purged("audio_file", &audio_id).unwrap());
    }

    /// A recording that another note still holds is not taken away with the
    /// note being emptied.
    #[test]
    fn purging_keeps_a_recording_another_note_still_holds() {
        let (db, _t) = create_test_db();
        let keeper = db.create_note("הפתק שנשאר").unwrap();
        let doomed = db.create_note("הפתק שנמחק").unwrap();
        let audio_id = db.create_audio_file("shared.ogg", None, None, crate::models::FileOrigin::Imported, None).unwrap();
        db.attach_to_note(&keeper, &audio_id, "audio_file").unwrap();
        db.attach_to_note(&doomed, &audio_id, "audio_file").unwrap();

        db.delete_note(&doomed).unwrap();
        let removed_audio: Vec<String> = db.purge_note(&doomed).unwrap().into_iter().map(|r| r.id).collect();

        assert!(removed_audio.is_empty(), "nothing to delete from disk");
        assert!(db.get_audio_file(&audio_id).unwrap().is_some(), "the recording stays");
        assert_eq!(db.get_audio_files_for_note(&keeper).unwrap().len(), 1, "and the note keeps it");
    }

    /// The purge travels, and nothing brings the note back afterwards: the
    /// peer that still had it stops offering it.
    #[test]
    fn a_purge_travels_and_the_note_does_not_come_back() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let note_id = a.create_note("פתק שיימחק לתמיד").unwrap();
        let audio_id = a.create_audio_file("gone.ogg", None, None, crate::models::FileOrigin::Imported, None).unwrap();
        a.attach_to_note(&note_id, &audio_id, "audio_file").unwrap();
        exchange(&a, &b);
        assert!(b.get_note(&note_id).unwrap().is_some());

        a.delete_note(&note_id).unwrap();
        a.purge_note(&note_id).unwrap();
        exchange(&a, &b);

        assert!(b.get_note_raw(&note_id).unwrap().is_none(), "B removed it too");
        assert!(b.get_audio_file(&audio_id).unwrap().is_none(), "with its recording");
        assert!(b.is_purged("note", &note_id).unwrap());

        // B sends everything it has back to A, including anything it kept
        // about that note. A must not take it back.
        exchange(&a, &b);
        assert!(a.get_note_raw(&note_id).unwrap().is_none(), "A did not resurrect it");
        assert!(b.get_note_raw(&note_id).unwrap().is_none());
    }

    /// A device that never heard of the note before the purge does not
    /// create it from the rows a third device is still sending.
    #[test]
    fn a_purged_note_is_refused_even_when_it_arrives_first() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let note_id = a.create_note("פתק שנמחק לפני שהגיע").unwrap();
        exchange(&a, &b);

        // B empties it out of its trash while A is away.
        b.delete_note(&note_id).unwrap();
        b.purge_note(&note_id).unwrap();

        // A, which still has the note alive, tells B all about it.
        push_all(&a, &b, DEV_A);
        assert!(b.get_note_raw(&note_id).unwrap().is_none(), "B keeps it removed");

        // And when B's purge reaches A, A removes it as well.
        push_all(&b, &a, DEV_B);
        assert!(a.get_note_raw(&note_id).unwrap().is_none());
    }

    #[test]
    fn test_versions_non_overlapping_edits_merge_cleanly_on_both_sides() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let note_id = a.create_note("שורה א\nשורה ב\nשורה ג\n").unwrap();
        exchange(&a, &b);
        assert_eq!(content(&b, &note_id), "שורה א\nשורה ב\nשורה ג\n");

        // Concurrent, non-overlapping edits
        a.update_note(&note_id, "שורה א (מחשב)\nשורה ב\nשורה ג\n").unwrap();
        b.update_note(&note_id, "שורה א\nשורה ב\nשורה ג (טלפון)\n").unwrap();
        exchange(&a, &b);

        let expected = "שורה א (מחשב)\nשורה ב\nשורה ג (טלפון)\n";
        assert_eq!(content(&a, &note_id), expected);
        assert_eq!(content(&b, &note_id), expected);
        assert_eq!(head_hex(&a, "note", &note_id, "content"), head_hex(&b, "note", &note_id, "content"), "identical merge version on both devices");
        assert!(a.get_conflicts(false).unwrap().is_empty());
        assert!(b.get_conflicts(false).unwrap().is_empty());
        // Both original edits are still in the history
        let history = a.get_field_history("note", &note_id, "content").unwrap();
        assert!(history.iter().any(|v| v.content.contains("(מחשב)") && !v.content.contains("(טלפון)")));
        assert!(history.iter().any(|v| v.content.contains("(טלפון)") && !v.content.contains("(מחשב)")));
    }

    #[test]
    fn test_versions_overlapping_edits_flag_identical_conflict_on_both_sides() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let note_id = a.create_note("כותרת\nגוף\n").unwrap();
        exchange(&a, &b);

        a.update_note(&note_id, "כותרת חדשה במחשב\nגוף\n").unwrap();
        b.update_note(&note_id, "כותרת חדשה בטלפון\nגוף\n").unwrap();
        exchange(&a, &b);

        let ca = content(&a, &note_id);
        let cb = content(&b, &note_id);
        assert_eq!(ca, cb, "both devices render the same merged text");
        assert!(ca.contains("<<<<<<< VERSION A\n"), "{}", ca);
        assert!(ca.contains("כותרת חדשה במחשב") && ca.contains("כותרת חדשה בטלפון"));
        assert!(ca.contains(">>>>>>> VERSION B\n"));
        assert!(ca.ends_with("גוף\n"));

        let conf_a = a.get_conflicts(false).unwrap();
        let conf_b = b.get_conflicts(false).unwrap();
        assert_eq!(conf_a.len(), 1);
        assert_eq!(conf_b.len(), 1);
        assert_eq!(conf_a[0].id, conf_b[0].id, "same conflict id everywhere");
        assert_eq!(conf_a[0].kind, "text");
        assert_eq!(a.get_note_conflict_types(&note_id).unwrap(), vec!["content".to_string()]);

        // Resolving by editing the note on A clears it on B after a sync
        a.update_note(&note_id, "כותרת משולבת\nגוף\n").unwrap();
        assert!(a.get_conflicts(false).unwrap().is_empty());
        exchange(&a, &b);
        assert_eq!(content(&b, &note_id), "כותרת משולבת\nגוף\n");
        assert!(b.get_conflicts(false).unwrap().is_empty());
        assert!(b.get_note_conflict_types(&note_id).unwrap().is_empty());
    }

    #[test]
    fn test_versions_accept_conflict_propagates() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let note_id = a.create_note("x\n").unwrap();
        exchange(&a, &b);
        a.update_note(&note_id, "xa\n").unwrap();
        b.update_note(&note_id, "xb\n").unwrap();
        exchange(&a, &b);
        let conflict = a.get_conflicts(false).unwrap().remove(0);

        assert!(a.accept_conflict(&conflict.id).unwrap());
        assert!(a.get_conflicts(false).unwrap().is_empty());
        let accepted = content(&a, &note_id);
        assert!(accepted.contains("<<<<<<<"), "accepted text keeps the markers until edited");

        exchange(&a, &b);
        assert!(b.get_conflicts(false).unwrap().is_empty());
        assert_eq!(content(&b, &note_id), accepted);
        assert_eq!(head_hex(&a, "note", &note_id, "content"), head_hex(&b, "note", &note_id, "content"));
    }

    #[test]
    fn test_versions_delete_without_concurrent_edit_propagates() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let note_id = a.create_note("למחיקה").unwrap();
        exchange(&a, &b);
        assert!(a.delete_note(&note_id).unwrap());
        exchange(&a, &b);
        assert!(b.get_note_raw(&note_id).unwrap().unwrap()["deleted_at"].is_i64());
        assert!(b.get_conflicts(false).unwrap().is_empty());
    }

    #[test]
    fn test_versions_delete_versus_edit_keeps_the_edit_and_flags() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let note_id = a.create_note("מקורי").unwrap();
        exchange(&a, &b);

        a.delete_note(&note_id).unwrap();
        b.update_note(&note_id, "מקורי ועוד").unwrap();
        exchange(&a, &b);

        for db in [&a, &b] {
            let note = db.get_note_raw(&note_id).unwrap().unwrap();
            assert!(note["deleted_at"].is_null(), "the edit wins over the concurrent delete");
            assert_eq!(note["content"].as_str().unwrap(), "מקורי ועוד");
            let kinds = db.get_note_conflict_types(&note_id).unwrap();
            assert!(kinds.contains(&"delete".to_string()), "{:?}", kinds);
        }
        assert_eq!(head_hex(&a, "note", &note_id, "deleted"), head_hex(&b, "note", &note_id, "deleted"));

        // Deleting again after seeing the edit is a normal delete
        a.delete_note(&note_id).unwrap();
        exchange(&a, &b);
        assert!(b.get_note_raw(&note_id).unwrap().unwrap()["deleted_at"].is_i64());
    }

    #[test]
    fn test_versions_tag_rename_both_sides_flags_scalar_conflict() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let tag_id = a.create_tag("עבודה", None).unwrap();
        exchange(&a, &b);

        a.rename_tag(&tag_id, "משרד").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1100));
        b.rename_tag(&tag_id, "פרויקטים").unwrap();
        exchange(&a, &b);

        let name_a = a.get_tag(&tag_id).unwrap().unwrap().name;
        let name_b = b.get_tag(&tag_id).unwrap().unwrap().name;
        assert_eq!(name_a, name_b);
        assert!(!name_a.contains('|'), "no combined names");
        assert_eq!(name_a, "פרויקטים", "the later rename stays live");
        let conflicts = a.get_conflicts(false).unwrap();
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].kind, "scalar");
        assert_eq!(conflicts[0].entity_type, "tag");
        // Both names remain in history
        let names: Vec<String> = a.get_field_history("tag", &tag_id, "name").unwrap().into_iter().map(|v| v.content).collect();
        assert!(names.contains(&"משרד".to_string()) && names.contains(&"פרויקטים".to_string()));
    }

    #[test]
    fn test_versions_tag_reparent_delete_and_new_tag_links_propagate() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let parent = a.create_tag("הורה", None).unwrap();
        let child = a.create_tag("ילד", None).unwrap();
        let note_id = a.create_note("פתק").unwrap();
        exchange(&a, &b);

        // Move, attach a brand-new tag, delete another: all in one batch
        a.reparent_tag(&child, Some(&parent)).unwrap();
        let fresh = a.create_tag("חדש", Some(&parent)).unwrap();
        a.add_tag_to_note(&note_id, &fresh).unwrap();
        let doomed = a.create_tag("למחיקה", None).unwrap();
        a.delete_tag(&doomed).unwrap();
        push_all(&a, &b, DEV_A);

        assert_eq!(b.get_tag(&child).unwrap().unwrap().parent_id.as_deref(), Some(parent.as_str()));
        let fresh_b = b.get_tag(&fresh).unwrap().expect("new tag arrived");
        assert_eq!(fresh_b.name, "חדש");
        assert_eq!(fresh_b.parent_id.as_deref(), Some(parent.as_str()));
        let note_tags: Vec<String> = b.get_note_tags(&note_id).unwrap().into_iter().map(|t| t.id).collect();
        assert!(note_tags.contains(&fresh), "link to the brand-new tag arrived with it");
        assert!(b.get_tag(&doomed).unwrap().is_none() || b.get_tag_raw(&doomed).unwrap().unwrap()["deleted_at"].is_i64());

        // Removing the tag on B propagates back
        b.remove_tag_from_note(&note_id, &fresh).unwrap();
        push_all(&b, &a, DEV_B);
        let note_tags_a: Vec<String> = a.get_note_tags(&note_id).unwrap().into_iter().map(|t| t.id).collect();
        assert!(!note_tags_a.contains(&fresh));
    }

    #[test]
    fn test_versions_tag_remove_versus_readd_keeps_link_and_flags() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let tag = a.create_tag("תג", None).unwrap();
        let note_id = a.create_note("פתק").unwrap();
        a.add_tag_to_note(&note_id, &tag).unwrap();
        exchange(&a, &b);

        // A removes; B removes and re-adds (so B's head says attached, A's says detached)
        a.remove_tag_from_note(&note_id, &tag).unwrap();
        b.remove_tag_from_note(&note_id, &tag).unwrap();
        b.add_tag_to_note(&note_id, &tag).unwrap();
        exchange(&a, &b);

        for db in [&a, &b] {
            let tags: Vec<String> = db.get_note_tags(&note_id).unwrap().into_iter().map(|t| t.id).collect();
            assert!(tags.contains(&tag), "link is kept, never silently dropped");
            let kinds = db.get_note_conflict_types(&note_id).unwrap();
            assert!(kinds.contains(&"tag".to_string()), "{:?}", kinds);
        }
    }

    #[test]
    fn test_versions_transcription_flags_and_text_merge() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let audio = a.create_audio_file("הקלטה.mp3", None, None, crate::models::FileOrigin::Imported, None).unwrap();
        let tr = a.create_transcription(&audio, "שלום\nעולם\n", None, "whisper", None, None, None).unwrap();
        exchange(&a, &b);

        // A verifies; B cleans and edits line 2. Flags and text merge independently.
        a.update_transcription(&tr, "שלום\nעולם\n", None, None, Some("original verified !verbatim !cleaned !polished")).unwrap();
        b.update_transcription(&tr, "שלום\nעולם!\n", None, None, Some("original !verified !verbatim cleaned !polished")).unwrap();
        exchange(&a, &b);

        for db in [&a, &b] {
            let t = db.get_transcription(&tr).unwrap().unwrap();
            assert_eq!(t.content, "שלום\nעולם!\n");
            assert_eq!(t.state, "original verified !verbatim cleaned !polished");
        }
        assert!(a.get_conflicts(false).unwrap().is_empty());
    }

    #[test]
    fn test_versions_no_silent_overwrite_when_pull_arrives_before_push() {
        // Device B edits offline, then pulls A's newer edit before pushing its own.
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let note_id = a.create_note("בסיס\nאמצע\nשורה שלישית\n").unwrap();
        exchange(&a, &b);

        b.update_note(&note_id, "בסיס\nאמצע\nשורה שלישית מהטלפון\n").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1100));
        a.update_note(&note_id, "בסיס מהמחשב\nאמצע\nשורה שלישית\n").unwrap();

        // Pull only (A -> B): B must keep its own edit.
        push_all(&a, &b, DEV_A);
        let cb = content(&b, &note_id);
        assert!(cb.contains("מהטלפון"), "B's unsynced edit survived the pull: {}", cb);
        assert!(cb.contains("מהמחשב"));
        assert!(b.get_conflicts(false).unwrap().is_empty());
    }

    #[test]
    fn test_versions_failed_change_is_queued_and_retried() {
        let (db, _t) = create_test_db();
        let bad = make_sync_change(
            "transcription",
            "00000000000070008000000000000123",
            "create",
            serde_json::json!({
                "id": "00000000000070008000000000000123",
                "audio_file_id": "not-a-uuid",
                "content": "x",
                "service": "whisper",
                "state": "original",
                "device_id": DEV_A,
                "created_at": 1735689600,
            }),
            DEV_A,
        );
        let (applied, _, errors) = apply_changes_from_peer(&db, &[bad], DEV_A, None, None, None).unwrap();
        assert_eq!(applied, 0);
        assert_eq!(errors.len(), 1, "{:?}", errors);
        assert_eq!(db.count_pending_sync_failures().unwrap(), 1, "kept for retry, not dropped");

        // Next batch retries it (still failing) and keeps it queued
        let (_, _, errors) = apply_changes_from_peer(&db, &[], DEV_A, None, None, None).unwrap();
        assert!(errors.is_empty());
        assert_eq!(db.count_pending_sync_failures().unwrap(), 1);
    }

    #[test]
    fn test_versions_first_edit_of_a_field_is_stamped_with_its_time() {
        // The first version of a field written by a device (here: the delete
        // tombstone of a note that was never deleted before) is a real edit and
        // must carry its timestamp, or the feed would show deleted_at = 0.
        let (db, _t) = create_test_db();
        let note_id = db.create_note("למחיקה").unwrap();
        db.delete_note(&note_id).unwrap();
        let raw = db.get_note_raw(&note_id).unwrap().unwrap();
        assert!(raw["deleted_at"].as_i64().unwrap_or(0) > 0, "{:?}", raw);
        assert!(raw["modified_at"].as_i64().unwrap_or(0) > 0, "{:?}", raw);
        let changes = db.get_changes_after_seq(0, None, 100).unwrap().changes;
        let del = changes.iter().find(|c| c["entity_type"] == "note" && c["operation"] == "delete").unwrap();
        assert!(del["data"]["deleted_at"].as_i64().unwrap() > 0);
    }

    #[test]
    fn test_files_storage_key_reaches_peer_that_edited_summary_first() {
        // A imports and uploads; B, unaware, edits the summary (a newer row);
        // after the exchange both know the cloud location and the summary.
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let audio = a.create_audio_file("הקלטה.mp3", None, None, crate::models::FileOrigin::Imported, None).unwrap();
        exchange(&a, &b);
        b.update_audio_file_summary(&audio, "סיכום מהטלפון").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1100));
        a.update_audio_file_storage(&audio, "s3", &format!("audio/{}.mp3", audio), false).unwrap();
        exchange(&a, &b);
        exchange(&a, &b);
        for db in [&a, &b] {
            let row = db.get_audio_file_raw(&audio).unwrap().unwrap();
            assert_eq!(row["storage_key"].as_str().unwrap(), format!("audio/{}.mp3", audio));
            assert_eq!(row["summary"].as_str().unwrap(), "סיכום מהטלפון");
        }
    }

    #[test]
    fn test_files_older_audio_row_never_erases_storage_key() {
        let (a, _ta) = create_test_db();
        let audio = a.create_audio_file("הקלטה.mp3", None, None, crate::models::FileOrigin::Imported, None).unwrap();
        a.update_audio_file_storage(&audio, "s3", &format!("audio/{}.mp3", audio), false).unwrap();
        // An older copy of the row (from a peer that never saw the upload)
        a.apply_sync_audio_file(&audio, 1735689600, "הקלטה.mp3", None, None, None, Some(1735689600), None, Some(1735689601), None, None, None, None, None, None, None, None).unwrap();
        let row = a.get_audio_file_raw(&audio).unwrap().unwrap();
        assert_eq!(row["storage_key"].as_str().unwrap(), format!("audio/{}.mp3", audio));
        assert_eq!(row["storage_provider"].as_str().unwrap(), "s3");
    }

    #[test]
    fn test_older_transcription_row_does_not_overwrite_newer_service_response() {
        let (a, _ta) = create_test_db();
        let (b, _tb) = create_test_db();
        let note = a.create_note("פתק").unwrap();
        let audio = a.create_audio_file("הקלטה.mp3", None, None, crate::models::FileOrigin::Imported, None).unwrap();
        a.attach_to_note(&note, &audio, "audio_file").unwrap();
        let tr = a.create_transcription(&audio, "טקסט", None, "whisper", None, Some("{\"run\":1}"), None).unwrap();
        exchange(&a, &b);
        std::thread::sleep(std::time::Duration::from_millis(1100));
        // A re-runs the service; B still holds the first response
        a.update_transcription(&tr, "טקסט", None, Some("{\"run\":2}"), None).unwrap();
        // B's (older) row reaches A first, then A's reaches B
        push_all(&b, &a, DEV_B);
        push_all(&a, &b, DEV_A);
        for db in [&a, &b] {
            let t = db.get_transcription(&tr).unwrap().unwrap();
            assert_eq!(t.service_response.as_deref(), Some("{\"run\":2}"), "the newer response must survive on every device");
        }
    }

    #[test]
    fn test_transcription_state_toggle_keeps_service_metadata() {
        let (a, _ta) = create_test_db();
        let audio = a.create_audio_file("הקלטה.mp3", None, None, crate::models::FileOrigin::Imported, None).unwrap();
        let tr = a.create_transcription(&audio, "טקסט", Some("[1,2]"), "whisper", None, Some("{\"ok\":true}"), None).unwrap();
        a.update_transcription(&tr, "טקסט", None, None, Some("original verified")).unwrap();
        let t = a.get_transcription(&tr).unwrap().unwrap();
        assert_eq!(t.content_segments.as_deref(), Some("[1,2]"));
        assert_eq!(t.service_response.as_deref(), Some("{\"ok\":true}"));
        assert!(t.state.contains("verified"));
    }

    #[test]
    fn test_older_attachment_row_does_not_move_attachment_back() {
        let (a, _ta) = create_test_db();
        let n1 = a.create_note("ראשון").unwrap();
        let n2 = a.create_note("שני").unwrap();
        let audio = a.create_audio_file("הקלטה.mp3", None, None, crate::models::FileOrigin::Imported, None).unwrap();
        let att = a.attach_to_note(&n1, &audio, "audio_file").unwrap();
        let created = a.get_attachment(&att).unwrap().unwrap().created_at;
        // The attachment was moved to n2 (a note merge) at t+100
        a.apply_sync_note_attachment(&att, &n2, &audio, "audio_file", created, Some(created + 100), None, Some(created + 100)).unwrap();
        // An older echo still says n1
        a.apply_sync_note_attachment(&att, &n1, &audio, "audio_file", created, Some(created + 50), None, Some(created + 150)).unwrap();
        assert_eq!(a.get_attachment(&att).unwrap().unwrap().note_id, n2);
    }

    #[test]
    fn test_versions_feed_carries_history() {
        let (db, _t) = create_test_db();
        let note_id = db.create_note("א").unwrap();
        db.update_note(&note_id, "ב").unwrap();
        let changes = db.get_changes_after_seq(0, None, 1000).unwrap().changes;
        let versions: Vec<_> = changes.iter().filter(|c| c["entity_type"] == "field_version").collect();
        assert!(versions.len() >= 2, "root and edit versions in the feed: {}", versions.len());
    }
}
