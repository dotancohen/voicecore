//! Pairing (PAIR-1..PAIR-5): how a fresh device joins an account.
//!
//! The device that holds the account shows a **setup text** (also as a QR
//! code): the account id, a single-use token that lives ten minutes, and
//! where the showing device listens. The reading device presents the token
//! over TLS, pinned to the fingerprint in the text, and receives a device
//! key of its own. No lasting secret is ever in the code.

use chrono::Utc;

use crate::auth;
use crate::config::Config;
use crate::database::Database;
use crate::error::{VoiceError, VoiceResult};
use crate::sync_protocol::codes;

/// How long a shown code is valid.
pub const TOKEN_LIFETIME_SECONDS: i64 = 600;

/// The scheme the setup text starts with; a phone opens it as a link.
pub const SETUP_TEXT_SCHEME: &str = "voice://pair?";

/// What a setup text says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetupText {
    pub version: u32,
    /// Empty in a grant text: the holder names the account (PAIR-5)
    pub account_id: String,
    /// A grant text: shown by the empty device (a server); the holder
    /// posts to it instead of claiming from it
    pub grant: bool,
    pub token: String,
    pub device_id: String,
    /// The URLs the showing device listens on, first one preferred
    pub urls: Vec<String>,
    /// `SHA256:aa:bb:…`, or empty for a device behind a real certificate
    pub certificate_fingerprint: String,
}

impl SetupText {
    /// The text as it is shown, copied and encoded in the QR code:
    /// `voice://pair?v=1&a=…&t=…&d=…&u=…&f=…`. The fingerprint's 32
    /// bytes travel as 43 base64url characters, not the colon form.
    pub fn to_text(&self) -> String {
        let mut pairs = vec![("v", self.version.to_string())];
        if self.grant {
            pairs.push(("g", "1".to_string()));
        } else {
            pairs.push(("a", self.account_id.clone()));
        }
        pairs.extend([
            ("t", self.token.clone()),
            ("d", self.device_id.clone()),
            ("u", self.urls.join(",")),
        ]);
        if !self.certificate_fingerprint.is_empty() {
            pairs.push(("f", compact_fingerprint(&self.certificate_fingerprint)));
        }
        let query: Vec<String> = pairs
            .into_iter()
            .map(|(k, v)| format!("{}={}", k, urlencoding::encode(&v)))
            .collect();
        format!("{}{}", SETUP_TEXT_SCHEME, query.join("&"))
    }

    /// Read a setup text. Whitespace around it and a pasted line break
    /// inside it are forgiven; anything else that is wrong is a sentence.
    pub fn parse(text: &str) -> VoiceResult<Self> {
        let compact: String = text.chars().filter(|c| !c.is_whitespace()).collect();
        let query = compact
            .strip_prefix(SETUP_TEXT_SCHEME)
            .ok_or_else(|| VoiceError::validation("setup text", format!("A setup text starts with {} ({})", SETUP_TEXT_SCHEME, codes::SETUP_TEXT_INVALID)))?;
        let mut version = 0u32;
        let mut account_id = String::new();
        let mut grant = false;
        let mut token = String::new();
        let mut device_id = String::new();
        let mut urls = Vec::new();
        let mut certificate_fingerprint = String::new();
        for pair in query.split('&') {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            let v = urlencoding::decode(v)
                .map_err(|_| VoiceError::validation("setup text", format!("Bad encoding ({})", codes::SETUP_TEXT_INVALID)))?
                .to_string();
            match k {
                "v" => version = v.parse().unwrap_or(0),
                "a" => account_id = v,
                "g" => grant = v == "1",
                "t" => token = v,
                "d" => device_id = v,
                "u" => urls = v.split(',').filter(|u| !u.is_empty()).map(str::to_string).collect(),
                "f" => certificate_fingerprint = expand_fingerprint(&v)?,
                _ => {}
            }
        }
        if version != 1 {
            return Err(VoiceError::validation("setup text", format!("Setup text version {} is not known ({})", version, codes::SETUP_TEXT_INVALID)));
        }
        if !grant {
            crate::database::validate_account_id(&account_id)
                .map_err(|_| VoiceError::validation("setup text", format!("The account id is malformed ({})", codes::SETUP_TEXT_INVALID)))?;
        }
        if token.is_empty() || device_id.len() != 32 || urls.is_empty() {
            return Err(VoiceError::validation("setup text", format!("The setup text is incomplete ({})", codes::SETUP_TEXT_INVALID)));
        }
        Ok(Self { version, account_id, grant, token, device_id, urls, certificate_fingerprint })
    }
}

/// `SHA256:aa:bb:…` → 43 base64url characters of the 32 bytes.
pub fn compact_fingerprint(fingerprint: &str) -> String {
    let hex: String = fingerprint.trim_start_matches("SHA256:").chars().filter(|c| *c != ':').collect();
    let bytes: Vec<u8> = (0..hex.len()).step_by(2).filter_map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok()).collect();
    base64::Engine::encode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, bytes)
}

/// The reverse of [`compact_fingerprint`].
pub fn expand_fingerprint(compact: &str) -> VoiceResult<String> {
    let bytes = base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, compact)
        .map_err(|_| VoiceError::validation("setup text", format!("The fingerprint is malformed ({})", codes::SETUP_TEXT_INVALID)))?;
    if bytes.len() != 32 {
        return Err(VoiceError::validation("setup text", format!("The fingerprint is not 32 bytes ({})", codes::SETUP_TEXT_INVALID)));
    }
    let hex: Vec<String> = bytes.iter().map(|b| format!("{:02x}", b)).collect();
    Ok(format!("SHA256:{}", hex.join(":")))
}

/// Show a code (PAIR-1): make a token, record its hash with its expiry, and
/// return the setup text. `urls` are where this device listens.
pub fn offer(db: &Database, config: &Config, urls: Vec<String>) -> VoiceResult<SetupText> {
    let token = auth::generate_device_key();
    let expires_at = Utc::now().timestamp() + TOKEN_LIFETIME_SECONDS;
    db.offer_pairing_token(&auth::key_hash(&token), expires_at)?;
    let certificate_fingerprint = config
        .certs_dir()
        .ok()
        .map(|d| d.join("server.crt"))
        .filter(|p| p.is_file())
        .and_then(|p| crate::tls::compute_fingerprint(&p).ok())
        .unwrap_or_default();
    Ok(SetupText {
        version: 1,
        account_id: db.account_id()?,
        grant: false,
        token,
        device_id: config.device_id_hex().to_string(),
        urls,
        certificate_fingerprint,
    })
}

/// Offer to host an account (PAIR-5): a grant text shown by a server that
/// holds no account yet; the holder posts to `/pair/grant`. The token's
/// hash lives in the index, so the listener finds it in another process.
pub fn offer_hosting(index: &crate::accounts::AccountIndex, config: &Config, label: Option<&str>, urls: Vec<String>) -> VoiceResult<SetupText> {
    let token = auth::generate_device_key();
    let expires_at = Utc::now().timestamp() + TOKEN_LIFETIME_SECONDS;
    index.offer_hosting_token(&auth::key_hash(&token), label, expires_at)?;
    let certificate_fingerprint = config
        .certs_dir()
        .ok()
        .map(|d| d.join("server.crt"))
        .filter(|p| p.is_file())
        .and_then(|p| crate::tls::compute_fingerprint(&p).ok())
        .unwrap_or_default();
    Ok(SetupText {
        version: 1,
        account_id: String::new(),
        grant: true,
        token,
        device_id: config.device_id_hex().to_string(),
        urls,
        certificate_fingerprint,
    })
}

/// Hide the code: withdraw the offer.
pub fn withdraw(db: &Database) -> VoiceResult<()> {
    db.withdraw_pairing_offers()
}

/// The showing side of a claim (PAIR-3): spend the token, make a key for the
/// reader, write its card. Returns the key, or the refusal sentence.
pub fn admit_by_token(
    db: &Database,
    token: &str,
    device_id: &str,
    device_name: &str,
    certificate_fingerprint: &str,
    addresses: &str,
    application: &str,
) -> Result<String, String> {
    let now = Utc::now().timestamp();
    let spent = db
        .spend_pairing_token(&auth::key_hash(token), now)
        .map_err(|e| format!("Could not check the token: {} ({})", e, codes::TOKEN_INVALID))?;
    if !spent {
        return Err(format!("The code is not valid: it was spent, it expired, or it was mistyped ({})", codes::TOKEN_INVALID));
    }
    if device_id.len() != 32 || !device_id.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(format!("The device id is malformed ({})", codes::SETUP_TEXT_INVALID));
    }
    let key = auth::generate_device_key();
    let card = crate::versions::DeviceCard {
        device_id: device_id.to_string(),
        name: device_name.to_string(),
        certificate_fingerprint: certificate_fingerprint.to_string(),
        addresses: addresses.to_string(),
        listens: "0".to_string(),
        key_hash: auth::key_hash(&key),
        revoked: "0".to_string(),
        application: if application.is_empty() { auth::APPLICATION_VOICE.to_string() } else { application.to_string() },
    };
    db.admit_device_card(&card).map_err(|e| format!("Could not write the device's card: {}", e))?;
    Ok(key)
}

/// The reading side, before any network (PAIR-4): a device that holds notes
/// of another account refuses the code.
pub fn check_can_join(db: &Database, setup: &SetupText) -> VoiceResult<()> {
    let own = db.account_id()?;
    if own == setup.account_id {
        return Ok(());
    }
    let notes: i64 = db
        .connection()
        .query_row("SELECT COUNT(*) FROM notes", [], |r| r.get(0))?;
    if notes > 0 {
        return Err(VoiceError::Sync(format!(
            "This device holds {} notes of account {}; the code is for account {}. Show this device's code to the other one instead, or use 'account move' ({})",
            notes,
            &own[..crate::UUID_SHORT_LEN.min(own.len())],
            &setup.account_id[..crate::UUID_SHORT_LEN.min(setup.account_id.len())],
            codes::DEVICE_HOLDS_NOTES
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> SetupText {
        SetupText {
            version: 1,
            account_id: "0199aaaaaaaa7000800000000000000a".to_string(),
            grant: false,
            token: "tok-en_123".to_string(),
            device_id: "00000000000070008000000000000001".to_string(),
            urls: vec!["https://192.168.1.10:8384".to_string(), "https://desk.local:8384".to_string()],
            certificate_fingerprint: "SHA256:00:11:22:33:44:55:66:77:88:99:aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99:aa:bb:cc:dd:ee:ff".to_string(),
        }
    }

    #[test]
    fn a_setup_text_round_trips_and_stays_short() {
        let text = sample().to_text();
        assert!(text.starts_with("voice://pair?v=1&a="));
        assert!(text.len() < 300, "{} bytes", text.len());
        assert!(!text.contains("&n="), "the name comes back in the reply, not in the code");
        assert!(!text.contains("SHA256:"), "the fingerprint travels compact");
        assert_eq!(SetupText::parse(&text).unwrap(), sample());
        assert_eq!(SetupText::parse(&format!("  {}\n", text.replace("&d=", "&\nd="))).unwrap(), sample(), "whitespace is forgiven");
    }

    #[test]
    fn a_bad_setup_text_says_what_is_wrong() {
        assert!(SetupText::parse("hello").unwrap_err().to_string().contains("SETUP_TEXT_INVALID"));
        assert!(SetupText::parse("voice://pair?v=2&a=x").unwrap_err().to_string().contains("version 2"));
        let mut broken = sample();
        broken.urls.clear();
        assert!(SetupText::parse(&broken.to_text()).unwrap_err().to_string().contains("incomplete"));
    }

    #[test]
    fn a_grant_text_names_no_account_and_says_so() {
        let mut grant = sample();
        grant.grant = true;
        grant.account_id = String::new();
        let text = grant.to_text();
        assert!(text.contains("&g=1&") && !text.contains("&a="));
        assert_eq!(SetupText::parse(&text).unwrap(), grant);
    }

    #[test]
    fn a_fingerprint_compacts_to_43_characters_and_back() {
        let compact = compact_fingerprint(&sample().certificate_fingerprint);
        assert_eq!(compact.len(), 43);
        assert_eq!(expand_fingerprint(&compact).unwrap(), sample().certificate_fingerprint);
        assert!(expand_fingerprint("short").is_err());
    }

    #[test]
    fn a_token_is_spent_once_dies_after_five_wrong_guesses_and_expires() {
        let db = Database::new_in_memory().unwrap();
        let now = Utc::now().timestamp();
        let hash = auth::key_hash("right");
        db.offer_pairing_token(&hash, now + 600).unwrap();
        assert!(db.has_pairing_offer(now).unwrap());
        assert!(!db.spend_pairing_token(&auth::key_hash("wrong"), now).unwrap());
        assert!(db.spend_pairing_token(&hash, now).unwrap(), "the right token is accepted");
        assert!(!db.spend_pairing_token(&hash, now).unwrap(), "and only once");

        db.offer_pairing_token(&hash, now + 600).unwrap();
        for _ in 0..crate::database::PAIRING_GUESSES_ALLOWED {
            assert!(!db.spend_pairing_token(&auth::key_hash("wrong"), now).unwrap());
        }
        assert!(!db.has_pairing_offer(now).unwrap(), "five wrong guesses withdraw the code");
        assert!(!db.spend_pairing_token(&hash, now).unwrap());

        db.offer_pairing_token(&hash, now + 600).unwrap();
        assert!(!db.spend_pairing_token(&hash, now + 601).unwrap(), "an expired token is dead");
    }

    #[test]
    fn a_device_with_notes_of_another_account_refuses_a_code() {
        let db = Database::new_in_memory().unwrap();
        let setup = sample();
        assert!(check_can_join(&db, &setup).is_ok(), "an empty device may join");
        db.create_note("יש כאן משהו").unwrap();
        let err = check_can_join(&db, &setup).unwrap_err().to_string();
        assert!(err.contains("DEVICE_HOLDS_NOTES"), "{}", err);
        assert!(err.contains("Show this device's code to the other one"));
    }

    #[test]
    fn admitting_by_token_writes_the_card_with_the_key_hash() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = Database::new(dir.path().join("notes.db")).unwrap();
        let config = Config::new(Some(dir.path().to_path_buf()), None).unwrap();
        let setup = offer(&db, &config, vec!["https://127.0.0.1:1".to_string()]).unwrap();
        assert_eq!(setup.account_id, db.account_id().unwrap());

        let key = admit_by_token(&db, &setup.token, "00000000000070008000000000000099", "Phone", "", "", "").unwrap();
        let card = db.get_device_card("00000000000070008000000000000099").unwrap().unwrap();
        assert_eq!(card.key_hash, auth::key_hash(&key));
        assert_eq!(card.application, "voice");
        let again = admit_by_token(&db, &setup.token, "00000000000070008000000000000098", "Other", "", "", "");
        assert!(again.unwrap_err().contains("TOKEN_INVALID"), "the token was spent");
    }
}
