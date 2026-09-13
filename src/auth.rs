//! Who may talk to whom (AUTH-1..AUTH-6).
//!
//! Every device holds one key per account, made when it created the account
//! or issued to it at pairing. The key travels only as a bearer token over
//! TLS; every other device holds its SHA-256 on the device's card (CARD-1),
//! which syncs like any other data, so any device of the account, and any
//! server that hosts it, can verify any other device.

use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::config::Config;
use crate::database::Database;
use crate::error::VoiceResult;
use crate::sync_protocol::codes;
use crate::versions::DeviceCard;

/// The header that names the account a request is for.
pub const HEADER_ACCOUNT: &str = "x-account-id";
/// The header that names the device making the request (and so its key).
pub const HEADER_DEVICE: &str = "x-device-id";

/// The application this core belongs to, written on every card it makes.
pub const APPLICATION_VOICE: &str = "voice";

/// A new device key: 32 bytes from the operating system's random source,
/// as 43 base64url characters.
pub fn generate_device_key() -> String {
    let mut bytes = Vec::with_capacity(32);
    bytes.extend_from_slice(Uuid::new_v4().as_bytes());
    bytes.extend_from_slice(Uuid::new_v4().as_bytes());
    base64::Engine::encode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, bytes)
}

/// The hash a card holds for a key: hex SHA-256. The key has 256 bits of
/// entropy, so a slow hash would buy nothing.
pub fn key_hash(key: &str) -> String {
    let digest = Sha256::digest(key.as_bytes());
    digest.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Compare two hashes in time that does not depend on where they differ.
pub fn hashes_agree(a: &str, b: &str) -> bool {
    let a = a.as_bytes();
    let b = b.as_bytes();
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Make sure this device has a key for the account and that its own card
/// says so, with its name, its certificate fingerprint when it has one, and
/// the application. Called at every start; writes nothing when nothing
/// changed.
pub fn ensure_own_device_card(db: &Database, config: &mut Config) -> VoiceResult<DeviceCard> {
    if config.device_key().is_empty() {
        config.set_device_key(&generate_device_key())?;
    }
    let certificate_fingerprint = config
        .certs_dir()
        .ok()
        .map(|d| d.join("server.crt"))
        .filter(|p| p.is_file())
        .and_then(|p| crate::tls::compute_fingerprint(&p).ok())
        .unwrap_or_default();
    let existing = db.get_device_card(config.device_id_hex())?;
    let card = DeviceCard {
        device_id: config.device_id_hex().to_string(),
        name: config.device_name().to_string(),
        certificate_fingerprint,
        addresses: existing.as_ref().map(|c| c.addresses.clone()).unwrap_or_default(),
        listens: existing.as_ref().map(|c| c.listens.clone()).unwrap_or_else(|| "0".to_string()),
        key_hash: key_hash(config.device_key()),
        revoked: existing.as_ref().map(|c| c.revoked.clone()).unwrap_or_else(|| "0".to_string()),
        application: APPLICATION_VOICE.to_string(),
    };
    db.write_device_card(&card)?;
    Ok(card)
}

/// Why a request was refused: the status, the code and the sentence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    pub status: u16,
    pub code: &'static str,
    pub sentence: String,
}

impl Refusal {
    fn new(status: u16, code: &'static str, sentence: impl Into<String>) -> Self {
        Self { status, code, sentence: sentence.into() }
    }
}

/// The check every authenticated request goes through (AUTH-3..AUTH-5).
///
/// `own_account` is the account this server holds; `account`, `device` and
/// `key` are what the request carried, if anything.
pub fn verify_request(
    db: &Database,
    own_account: &str,
    account: Option<&str>,
    device: Option<&str>,
    key: Option<&str>,
) -> Result<DeviceCard, Refusal> {
    let account = account.unwrap_or("");
    if account.is_empty() || account != own_account {
        return Err(Refusal::new(
            404,
            codes::ACCOUNT_UNKNOWN,
            format!("This server does not host your account ({})", codes::ACCOUNT_UNKNOWN),
        ));
    }
    let (device, key) = match (device, key) {
        (Some(d), Some(k)) if !d.is_empty() && !k.is_empty() => (d, k),
        _ => {
            return Err(Refusal::new(
                401,
                codes::KEY_MISSING,
                format!("The request carried no device key; pair again ({})", codes::KEY_MISSING),
            ))
        }
    };
    let card = match db.get_device_card(device) {
        Ok(Some(card)) => card,
        _ => {
            return Err(Refusal::new(
                401,
                codes::DEVICE_UNKNOWN,
                format!("This device is not paired with this account; pair again ({})", codes::DEVICE_UNKNOWN),
            ))
        }
    };
    if card.is_revoked() {
        return Err(Refusal::new(
            401,
            codes::DEVICE_REVOKED,
            format!("This device was revoked from the account ({})", codes::DEVICE_REVOKED),
        ));
    }
    if !hashes_agree(&key_hash(key), &card.key_hash) {
        return Err(Refusal::new(
            401,
            codes::KEY_WRONG,
            format!("The device key is wrong; pair again ({})", codes::KEY_WRONG),
        ));
    }
    Ok(card)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn card(device_id: &str, key: &str) -> DeviceCard {
        DeviceCard {
            device_id: device_id.to_string(),
            name: "Phone".to_string(),
            certificate_fingerprint: String::new(),
            addresses: String::new(),
            listens: "0".to_string(),
            key_hash: key_hash(key),
            revoked: "0".to_string(),
            application: APPLICATION_VOICE.to_string(),
        }
    }

    #[test]
    fn a_key_is_forty_three_url_safe_characters_and_never_repeats() {
        let a = generate_device_key();
        let b = generate_device_key();
        assert_eq!(a.len(), 43);
        assert!(a.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
        assert_ne!(a, b);
    }

    #[test]
    fn a_hash_is_hex_sha256_and_compares_in_constant_time() {
        let h = key_hash("k");
        assert_eq!(h.len(), 64);
        assert!(hashes_agree(&h, &key_hash("k")));
        assert!(!hashes_agree(&h, &key_hash("K")));
        assert!(!hashes_agree(&h, &h[..63]));
    }

    #[test]
    fn a_request_is_checked_in_the_order_account_key_device_revocation_hash() {
        let db = Database::new_in_memory().unwrap();
        let account = db.account_id().unwrap();
        let other = "0199bbbbbbbb7000800000000000000b";
        let device = "00000000000070008000000000000099";
        let key = generate_device_key();

        let refused = verify_request(&db, &account, Some(other), Some(device), Some(&key)).unwrap_err();
        assert_eq!((refused.status, refused.code), (404, codes::ACCOUNT_UNKNOWN));
        let refused = verify_request(&db, &account, Some(&account), Some(device), None).unwrap_err();
        assert_eq!((refused.status, refused.code), (401, codes::KEY_MISSING));
        let refused = verify_request(&db, &account, Some(&account), Some(device), Some(&key)).unwrap_err();
        assert_eq!((refused.status, refused.code), (401, codes::DEVICE_UNKNOWN));

        db.write_device_card(&card(device, &key)).unwrap();
        let refused = verify_request(&db, &account, Some(&account), Some(device), Some("wrong")).unwrap_err();
        assert_eq!((refused.status, refused.code), (401, codes::KEY_WRONG));
        let ok = verify_request(&db, &account, Some(&account), Some(device), Some(&key)).unwrap();
        assert_eq!(ok.device_id, device);

        db.revoke_device(device).unwrap();
        let refused = verify_request(&db, &account, Some(&account), Some(device), Some(&key)).unwrap_err();
        assert_eq!((refused.status, refused.code), (401, codes::DEVICE_REVOKED));
        assert!(refused.sentence.ends_with("(DEVICE_REVOKED)"));
    }

    #[test]
    fn a_card_travels_and_revocation_cannot_be_undone_by_the_revoked_device() {
        let db = Database::new_in_memory().unwrap();
        let device = "00000000000070008000000000000099";
        db.write_device_card(&card(device, "k")).unwrap();
        db.revoke_device(device).unwrap();
        // The revoked device writes "0" again, later: the column keeps "1"
        // (CARD-2), whatever the version graph says.
        db.set_field(crate::versions::ENTITY_DEVICE, device, crate::versions::FIELD_REVOKED, "0", None).unwrap();
        assert!(db.get_device_card(device).unwrap().unwrap().is_revoked());
    }

    #[test]
    fn the_own_card_is_made_once_and_updated_on_a_rename() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = Database::new(dir.path().join("notes.db")).unwrap();
        let mut config = Config::new(Some(dir.path().to_path_buf()), None).unwrap();
        assert!(config.device_key().is_empty());

        let first = ensure_own_device_card(&db, &mut config).unwrap();
        assert_eq!(first.key_hash, key_hash(config.device_key()));
        assert_eq!(first.application, "voice");
        let key = config.device_key().to_string();

        config.set_device_name("Desk").unwrap();
        let second = ensure_own_device_card(&db, &mut config).unwrap();
        assert_eq!(config.device_key(), key, "the key is made once");
        assert_eq!(second.name, "Desk");
        assert_eq!(db.list_device_cards().unwrap().len(), 1);
    }
}
