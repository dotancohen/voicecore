//! Several accounts on one installation (Stage 2, ACCT-6..ACCT-9).
//!
//! ```text
//! <root>/
//!   config.json        machine: device id and name, listen port, backup, public URL
//!   accounts.db        this index, never synced
//!   certs/
//!   <account id>/      notes.db  config.json  audio/  snapshots/
//! ```
//!
//! The index is only an index: the database inside an account directory is
//! authoritative for its own account id, and a disagreement is reported,
//! never corrected. A phone has no index; its one directory is the account.

use std::path::{Path, PathBuf};

use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};
use uuid::Uuid;

use crate::config::Config;
use crate::database::{validate_account_id, Database};
use crate::error::{VoiceError, VoiceResult};

/// The index file's name under the root.
pub const INDEX_FILE: &str = "accounts.db";

/// One row of the index.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AccountEntry {
    pub account_id: String,
    pub label: String,
    pub is_default: bool,
    /// Served for someone else (Stage 3); never the default
    pub hosted: bool,
    pub created_at: i64,
    pub last_opened_at: Option<i64>,
}

/// The index of the accounts a root holds.
pub struct AccountIndex {
    conn: Connection,
    root: PathBuf,
}

impl AccountIndex {
    /// Whether `root` has an index.
    pub fn exists(root: &Path) -> bool {
        root.join(INDEX_FILE).is_file()
    }

    /// Open the index, making it if it is missing (mode 0600).
    pub fn open(root: &Path) -> VoiceResult<Self> {
        std::fs::create_dir_all(root)?;
        let path = root.join(INDEX_FILE);
        let fresh = !path.exists();
        let conn = Connection::open(&path)?;
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS accounts (
                account_id TEXT PRIMARY KEY,
                label TEXT NOT NULL UNIQUE,
                is_default INTEGER NOT NULL DEFAULT 0,
                hosted INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL,
                last_opened_at INTEGER
            );
            CREATE TABLE IF NOT EXISTS hosting_offers (
                token_hash TEXT PRIMARY KEY,
                label TEXT,
                expires_at INTEGER NOT NULL,
                failures INTEGER NOT NULL DEFAULT 0
            );
            "#,
        )?;
        #[cfg(unix)]
        if fresh {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
        }
        Ok(Self { conn, root: root.to_path_buf() })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The directory of an account: always `<root>/<account id>`.
    pub fn directory(&self, account_id: &str) -> PathBuf {
        self.root.join(account_id)
    }

    fn row(row: &rusqlite::Row) -> rusqlite::Result<AccountEntry> {
        Ok(AccountEntry {
            account_id: row.get(0)?,
            label: row.get(1)?,
            is_default: row.get::<_, i64>(2)? != 0,
            hosted: row.get::<_, i64>(3)? != 0,
            created_at: row.get(4)?,
            last_opened_at: row.get(5)?,
        })
    }

    const COLUMNS: &'static str = "account_id, label, is_default, hosted, created_at, last_opened_at";

    /// Every account, default first, then by label.
    pub fn list(&self) -> VoiceResult<Vec<AccountEntry>> {
        let mut stmt = self.conn.prepare(&format!("SELECT {} FROM accounts ORDER BY is_default DESC, label", Self::COLUMNS))?;
        let rows = stmt.query_map([], Self::row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// By id, by a unique prefix of the id, or by label.
    pub fn find(&self, selector: &str) -> VoiceResult<Option<AccountEntry>> {
        let selector = selector.trim();
        if selector.is_empty() {
            return Ok(None);
        }
        if let Some(entry) = self
            .conn
            .query_row(&format!("SELECT {} FROM accounts WHERE account_id = ? OR label = ?", Self::COLUMNS), params![selector, selector], Self::row)
            .optional()?
        {
            return Ok(Some(entry));
        }
        let mut stmt = self.conn.prepare(&format!("SELECT {} FROM accounts WHERE account_id LIKE ? || '%'", Self::COLUMNS))?;
        let matches: Vec<AccountEntry> = stmt.query_map(params![selector], Self::row)?.collect::<Result<_, _>>()?;
        Ok(if matches.len() == 1 { matches.into_iter().next() } else { None })
    }

    pub fn default_account(&self) -> VoiceResult<Option<AccountEntry>> {
        Ok(self
            .conn
            .query_row(&format!("SELECT {} FROM accounts WHERE is_default = 1", Self::COLUMNS), [], Self::row)
            .optional()?)
    }

    /// Make a new account (ACCT-7): a fresh id, its directory, a database
    /// that carries the id, and a config. The label must be unique. The first
    /// account that is not hosted becomes the default.
    pub fn create(&self, label: Option<&str>, hosted: bool) -> VoiceResult<AccountEntry> {
        let account_id = Uuid::now_v7().simple().to_string();
        let label = label.map(str::trim).filter(|l| !l.is_empty()).map(str::to_string).unwrap_or_else(|| {
            if hosted { format!("hosted-{}", &account_id[..8]) } else { "default".to_string() }
        });
        self.register(&account_id, &label, hosted)
    }

    /// Register an account whose id is already known: one this machine joins
    /// (PAIR-4) or hosts (Stage 3).
    pub fn register(&self, account_id: &str, label: &str, hosted: bool) -> VoiceResult<AccountEntry> {
        validate_account_id(account_id)?;
        if self.find_by_label(label)?.is_some() {
            return Err(VoiceError::validation("label", format!("An account is already labelled {}", label)));
        }
        let is_default = !hosted && self.default_account()?.is_none();
        let now = Utc::now().timestamp();
        let dir = self.directory(account_id);
        std::fs::create_dir_all(&dir)?;
        // The database carries the id from its first moment (ACCT-4)
        let _db = Database::new_for_account(dir.join("notes.db"), account_id)?;
        // The account's config, with the audio directory inside the account (ACCT-8)
        let mut config = Config::open_account(&self.root, &dir)?;
        if config.audiofile_directory().is_none() {
            let audio = dir.join("audio");
            std::fs::create_dir_all(&audio)?;
            config.set_audiofile_directory(&audio.to_string_lossy())?;
        }
        self.conn.execute(
            "INSERT INTO accounts (account_id, label, is_default, hosted, created_at) VALUES (?, ?, ?, ?, ?)",
            params![account_id, label, is_default as i64, hosted as i64, now],
        )?;
        Ok(AccountEntry { account_id: account_id.to_string(), label: label.to_string(), is_default, hosted, created_at: now, last_opened_at: None })
    }

    fn find_by_label(&self, label: &str) -> VoiceResult<Option<AccountEntry>> {
        Ok(self
            .conn
            .query_row(&format!("SELECT {} FROM accounts WHERE label = ?", Self::COLUMNS), params![label], Self::row)
            .optional()?)
    }

    pub fn set_default(&self, account_id: &str) -> VoiceResult<()> {
        let entry = self.find(account_id)?.ok_or_else(|| VoiceError::NotFound(format!("No account {}", account_id)))?;
        if entry.hosted {
            return Err(VoiceError::validation("account", "A hosted account is served for someone else and cannot be the default"));
        }
        self.conn.execute("UPDATE accounts SET is_default = 0", [])?;
        self.conn.execute("UPDATE accounts SET is_default = 1 WHERE account_id = ?", params![entry.account_id])?;
        Ok(())
    }

    pub fn set_hosted(&self, account_id: &str, hosted: bool) -> VoiceResult<()> {
        let entry = self.find(account_id)?.ok_or_else(|| VoiceError::NotFound(format!("No account {}", account_id)))?;
        self.conn.execute(
            "UPDATE accounts SET hosted = ?, is_default = CASE WHEN ? THEN 0 ELSE is_default END WHERE account_id = ?",
            params![hosted as i64, hosted as i64, entry.account_id],
        )?;
        Ok(())
    }

    /// Forget an account: the index row only; the directory stays.
    pub fn remove(&self, account_id: &str) -> VoiceResult<()> {
        let entry = self.find(account_id)?.ok_or_else(|| VoiceError::NotFound(format!("No account {}", account_id)))?;
        self.conn.execute("DELETE FROM accounts WHERE account_id = ?", params![entry.account_id])?;
        Ok(())
    }

    pub fn touch(&self, account_id: &str) -> VoiceResult<()> {
        self.conn.execute("UPDATE accounts SET last_opened_at = ? WHERE account_id = ?", params![Utc::now().timestamp(), account_id])?;
        Ok(())
    }

    /// Offer to host an account (PAIR-5): the hash of the token shown by
    /// `account host`, with the label the account gets. One offer at a time.
    pub fn offer_hosting_token(&self, token_hash: &str, label: Option<&str>, expires_at: i64) -> VoiceResult<()> {
        self.conn.execute("DELETE FROM hosting_offers", [])?;
        self.conn.execute(
            "INSERT INTO hosting_offers (token_hash, label, expires_at, failures) VALUES (?, ?, ?, 0)",
            params![token_hash, label, expires_at],
        )?;
        Ok(())
    }

    /// Spend the hosting offer, returning its label; None when the token is
    /// wrong, spent or expired. Five wrong tokens withdraw the offer.
    pub fn spend_hosting_token(&self, token_hash: &str, now: i64) -> VoiceResult<Option<String>> {
        self.conn.execute("DELETE FROM hosting_offers WHERE expires_at <= ?", params![now])?;
        let live: Option<(String, Option<String>, i64)> = self
            .conn
            .query_row("SELECT token_hash, label, failures FROM hosting_offers LIMIT 1", [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .optional()?;
        let Some((offered, label, failures)) = live else { return Ok(None) };
        if crate::auth::hashes_agree(&offered, token_hash) {
            self.conn.execute("DELETE FROM hosting_offers", [])?;
            return Ok(Some(label.unwrap_or_default()));
        }
        if failures + 1 >= crate::database::PAIRING_GUESSES_ALLOWED {
            self.conn.execute("DELETE FROM hosting_offers", [])?;
        } else {
            self.conn.execute("UPDATE hosting_offers SET failures = failures + 1", [])?;
        }
        Ok(None)
    }

}

/// What an installation opens (ACCT-6).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Resolved {
    /// The root is the account itself: a phone, or a directory that already
    /// holds a database and no index.
    Single { directory: PathBuf },
    /// One account of an indexed root.
    Account { root: PathBuf, directory: PathBuf, entry: AccountEntry },
}

impl Resolved {
    pub fn directory(&self) -> &Path {
        match self {
            Resolved::Single { directory } => directory,
            Resolved::Account { directory, .. } => directory,
        }
    }

    pub fn root(&self) -> &Path {
        match self {
            Resolved::Single { directory } => directory,
            Resolved::Account { root, .. } => root,
        }
    }

    /// Open the config the way the resolution says.
    pub fn open_config(&self) -> VoiceResult<Config> {
        match self {
            Resolved::Single { directory } => Config::new(Some(directory.clone()), None),
            Resolved::Account { root, directory, .. } => Config::open_account(root, directory),
        }
    }
}

/// Which account `root` opens for `selector` (ACCT-6):
///
/// - with an index: the selected account, or the default; with no default
///   and `create_default`, a new one is made and registered, so a first
///   desktop run is never a dead end;
/// - without an index, a root that already holds `notes.db` or `config.json`
///   is the account itself;
/// - an empty root gets an index and a default account (when
///   `create_default`).
pub fn resolve(root: &Path, selector: Option<&str>, create_default: bool) -> VoiceResult<Resolved> {
    let has_index = AccountIndex::exists(root);
    let single = !has_index && (root.join("notes.db").is_file() || root.join("config.json").is_file());
    if single {
        if let Some(sel) = selector.filter(|s| !s.trim().is_empty()) {
            return Err(VoiceError::validation("account", format!("{} holds one account and no index, so -a {} cannot select one", root.display(), sel)));
        }
        return Ok(Resolved::Single { directory: root.to_path_buf() });
    }
    if !has_index && !create_default {
        return Err(VoiceError::NotFound(format!("{} holds no account", root.display())));
    }
    let index = AccountIndex::open(root)?;
    let entry = match selector.map(str::trim).filter(|s| !s.is_empty()) {
        Some(sel) => index.find(sel)?.ok_or_else(|| VoiceError::NotFound(format!("No account is called or numbered {}; run 'account list'", sel)))?,
        None => match index.default_account()? {
            Some(entry) => entry,
            None if create_default => index.create(None, false)?,
            None => return Err(VoiceError::NotFound("This installation has no default account; run 'account create' or give -a".to_string())),
        },
    };
    index.touch(&entry.account_id)?;
    let directory = index.directory(&entry.account_id);
    // The database is authoritative for its own id (ACCT-4): a disagreement is an error, never corrected
    Database::new_for_account(directory.join("notes.db"), &entry.account_id)?;
    Ok(Resolved::Account { root: root.to_path_buf(), directory, entry })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_root_gets_an_index_and_a_default_account_once() {
        let dir = tempfile::TempDir::new().unwrap();
        let first = resolve(dir.path(), None, true).unwrap();
        let Resolved::Account { entry, directory, .. } = &first else { panic!("expected an account") };
        assert!(entry.is_default);
        assert_eq!(entry.label, "default");
        assert!(directory.join("notes.db").is_file());
        assert!(directory.join("config.json").is_file());
        assert!(directory.join("audio").is_dir(), "the audio directory is inside the account");
        assert!(AccountIndex::exists(dir.path()));
        assert!(dir.path().join("config.json").is_file(), "the machine's own file");

        let again = resolve(dir.path(), None, true).unwrap();
        assert_eq!(again.directory(), first.directory(), "the second run opens the same account");
        assert_eq!(AccountIndex::open(dir.path()).unwrap().list().unwrap().len(), 1);
    }

    #[test]
    fn a_directory_that_already_holds_a_database_is_the_account_itself() {
        let dir = tempfile::TempDir::new().unwrap();
        Database::new(dir.path().join("notes.db")).unwrap();
        assert_eq!(resolve(dir.path(), None, true).unwrap(), Resolved::Single { directory: dir.path().to_path_buf() });
        assert!(!AccountIndex::exists(dir.path()));
        let err = resolve(dir.path(), Some("x"), true).unwrap_err().to_string();
        assert!(err.contains("cannot select"), "{}", err);
    }

    #[test]
    fn accounts_are_found_by_id_prefix_or_label_and_labels_are_unique() {
        let dir = tempfile::TempDir::new().unwrap();
        let index = AccountIndex::open(dir.path()).unwrap();
        let a = index.create(Some("dotancohen"), false).unwrap();
        let b = index.create(Some("sillyberry"), false).unwrap();
        assert!(a.is_default && !b.is_default);
        assert_eq!(index.find("sillyberry").unwrap().unwrap().account_id, b.account_id);
        // Two ids minted in the same minute share their first characters (the
        // clock), so a short prefix is ambiguous and a longer one is not.
        assert!(index.find(&a.account_id[..8]).unwrap().is_none());
        assert_eq!(index.find(&a.account_id[..20]).unwrap().unwrap().account_id, a.account_id);
        assert!(index.find("nobody").unwrap().is_none());
        let err = index.create(Some("dotancohen"), false).unwrap_err().to_string();
        assert!(err.contains("already labelled"), "{}", err);

        index.set_default(&b.account_id).unwrap();
        assert_eq!(index.default_account().unwrap().unwrap().account_id, b.account_id);
        let resolved = resolve(dir.path(), Some("dotancohen"), false).unwrap();
        assert_eq!(resolved.directory(), index.directory(&a.account_id));
        let db = Database::new(resolved.directory().join("notes.db")).unwrap();
        assert_eq!(db.account_id().unwrap(), a.account_id, "the database carries the account it was made for");
    }

    #[test]
    fn a_hosted_account_is_never_the_default_and_a_disagreeing_database_is_reported() {
        let dir = tempfile::TempDir::new().unwrap();
        let index = AccountIndex::open(dir.path()).unwrap();
        let hosted = index.create(Some("meirav"), true).unwrap();
        assert!(hosted.hosted && !hosted.is_default);
        assert!(index.default_account().unwrap().is_none(), "a pure server has no default");
        assert!(index.set_default(&hosted.account_id).is_err());
        let err = resolve(dir.path(), None, false).unwrap_err().to_string();
        assert!(err.contains("no default account"), "{}", err);

        // Someone swapped the database for another account's: reported, not corrected
        let own = index.create(Some("own"), false).unwrap();
        let path = index.directory(&own.account_id).join("notes.db");
        std::fs::remove_file(&path).unwrap();
        let other = Database::new(&path).unwrap();
        other.create_note("של מישהו אחר").unwrap();
        drop(other);
        let err = resolve(dir.path(), Some("own"), false).unwrap_err().to_string();
        assert!(err.contains("ACCOUNT_DISAGREES"), "{}", err);
    }

    #[test]
    fn the_machine_settings_are_shared_by_every_account_of_a_root() {
        let dir = tempfile::TempDir::new().unwrap();
        let index = AccountIndex::open(dir.path()).unwrap();
        let a = index.create(Some("a"), false).unwrap();
        let b = index.create(Some("b"), false).unwrap();
        let mut config_a = Config::open_account(dir.path(), &index.directory(&a.account_id)).unwrap();
        let config_b = Config::open_account(dir.path(), &index.directory(&b.account_id)).unwrap();
        assert_eq!(config_a.this_device_id_hex(), config_b.this_device_id_hex(), "one device");
        assert_eq!(config_a.certs_dir().unwrap(), config_b.certs_dir().unwrap(), "one set of certificates");
        assert_eq!(config_a.root(), dir.path());
        config_a.set_this_device_name("Desk").unwrap();
        let reopened = Config::open_account(dir.path(), &index.directory(&b.account_id)).unwrap();
        assert_eq!(reopened.this_device_name(), "Desk", "a rename reaches every account through the machine's file");
        assert_ne!(config_a.audiofile_directory(), config_b.audiofile_directory(), "recordings never shared");
    }
}
