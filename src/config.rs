//! Configuration management for Voice.
//!
//! This module handles loading and saving application configuration to/from
//! a JSON file. The config directory can be customized.
//!
//! Includes sync-related configuration:
//! - device_id: UUID7 identifying this device (generated on first run)
//! - device_name: Human-readable device name
//! - sync: Peer configuration and sync settings

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{VoiceError, VoiceResult};

/// Theme colors configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThemeColours {
    /// Warning color for highlighting ambiguous tags
    #[serde(default = "default_warnings_color")]
    pub warnings: String,
    /// TUI border color when focused
    #[serde(default = "default_tui_border_focused")]
    pub tui_border_focused: String,
    /// TUI border color when unfocused
    #[serde(default = "default_tui_border_unfocused")]
    pub tui_border_unfocused: String,
    /// Warning color for dark theme
    #[serde(default)]
    pub warnings_dark: Option<String>,
    /// Warning color for light theme
    #[serde(default)]
    pub warnings_light: Option<String>,
}

impl Default for ThemeColours {
    fn default() -> Self {
        Self {
            warnings: default_warnings_color(),
            tui_border_focused: default_tui_border_focused(),
            tui_border_unfocused: default_tui_border_unfocused(),
            warnings_dark: None,
            warnings_light: None,
        }
    }
}

fn default_warnings_color() -> String {
    "#FFFF00".to_string()
}

fn default_tui_border_focused() -> String {
    "green".to_string()
}

fn default_tui_border_unfocused() -> String {
    "blue".to_string()
}

/// Themes configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Themes {
    #[serde(default)]
    pub colours: ThemeColours,
}

/// Peer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerConfig {
    pub peer_id: String,
    pub peer_name: String,
    pub peer_url: String,
    pub certificate_fingerprint: Option<String>,
}

/// Sync configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_server_port")]
    pub server_port: u16,
    #[serde(default)]
    pub peers: Vec<PeerConfig>,
    /// Maximum file size in MB for sync uploads (default 100MB)
    /// Files larger than this will be tagged as _system/_nonsynced/_too-big
    #[serde(default = "default_max_sync_file_size_mb")]
    pub max_sync_file_size_mb: u32,
    /// When true, every sync also downloads every audio file that is in cloud
    /// storage but missing locally, so this installation holds a complete copy
    /// of all media (a backup of the cloud bucket). Local-only setting, never
    /// synced to peers; intended for desktop and server installations only.
    #[serde(default)]
    pub mirror_audio_files: bool,
    /// This device's key for the account (AUTH-1): 43 base64url characters,
    /// held in clear only here, hashed on every other device's card. Empty
    /// in the file when a wrapper keeps it in `device_key_wrapped` (AUTH-9).
    #[serde(default)]
    pub device_key: String,
    /// The device key wrapped by the platform's key store (AUTH-9), base64url;
    /// only the phone writes it. The clear key is in memory alone.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub device_key_wrapped: String,
    /// The account's recording key (Stage 15, ENC-1): 43 base64url characters,
    /// carried to every device by pairing; empty until encryption was set up.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub recording_key: String,
    /// The recording key wrapped by the platform's key store (AUTH-9)
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub recording_key_wrapped: String,
    /// Whether this device exported the recording key once (ENC-1): the
    /// encryption switch stays off until it did. Local.
    #[serde(default)]
    pub recording_key_exported: bool,
    /// The peer of the last operation (Stage 5): the one visible button
    /// names it. Local.
    #[serde(default)]
    pub last_peer_id: String,
    /// Peers forgotten on this device (Stage 5): their cards do not bring
    /// them back to the list until the user adds them again. Local.
    #[serde(default)]
    pub forgotten_peers: Vec<String>,
    /// Hours of silence after which the listener stops itself (Stage 6);
    /// 0, the default, means never. Stopping, not starting: it only saves
    /// the battery of a user who forgets.
    #[serde(default)]
    pub listener_idle_stop_hours: u32,
}

fn default_server_port() -> u16 {
    8384
}

fn default_max_sync_file_size_mb() -> u32 {
    100 // 100 MB default
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            server_port: default_server_port(),
            peers: Vec::new(),
            max_sync_file_size_mb: default_max_sync_file_size_mb(),
            mirror_audio_files: false,
            device_key: String::new(),
            device_key_wrapped: String::new(),
            recording_key: String::new(),
            recording_key_wrapped: String::new(),
            recording_key_exported: false,
            last_peer_id: String::new(),
            forgotten_peers: Vec::new(),
            listener_idle_stop_hours: 0,
        }
    }
}

/// The periodic backup of every open account's database (SNAP-5).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupConfig {
    /// Hours between copies; 0 turns the backup off
    #[serde(default = "default_backup_interval_hours")]
    pub interval_hours: u32,
    /// Where the copies go; empty means `<root>/backups/<account id>/`
    #[serde(default)]
    pub directory: String,
    /// How many copies are kept per account
    #[serde(default = "default_backup_keep")]
    pub keep: u32,
}

fn default_backup_interval_hours() -> u32 {
    24
}

fn default_backup_keep() -> u32 {
    30
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self { interval_hours: default_backup_interval_hours(), directory: String::new(), keep: default_backup_keep() }
    }
}

/// The bucket's configuration as the database stores it for the account
/// (synced; see `Database::get_file_storage_config`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileStorageConfig {
    /// Storage provider: "s3", "none" (local only)
    #[serde(default = "default_file_storage_provider")]
    pub provider: String,
    /// Provider-specific configuration as JSON
    /// For S3: { "bucket": "...", "region": "...", "access_key_id": "...", "secret_access_key": "...", "prefix": "...", "endpoint": "..." }
    #[serde(default)]
    pub config: serde_json::Value,
}

fn default_file_storage_provider() -> String {
    "none".to_string()
}

impl Default for FileStorageConfig {
    fn default() -> Self {
        Self {
            provider: default_file_storage_provider(),
            config: serde_json::Value::Null,
        }
    }
}

impl FileStorageConfig {
    /// Create an S3 storage configuration
    pub fn s3(
        bucket: &str,
        region: &str,
        access_key_id: &str,
        secret_access_key: &str,
        prefix: Option<&str>,
        endpoint: Option<&str>,
    ) -> Self {
        Self {
            provider: "s3".to_string(),
            config: serde_json::json!({
                "bucket": bucket,
                "region": region,
                "access_key_id": access_key_id,
                "secret_access_key": secret_access_key,
                "prefix": prefix,
                "endpoint": endpoint,
            }),
        }
    }

    /// Check if storage is configured (not "none")
    pub fn is_enabled(&self) -> bool {
        self.provider != "none"
    }

    /// Get S3-specific config fields (if provider is "s3")
    pub fn s3_bucket(&self) -> Option<&str> {
        self.config.get("bucket").and_then(|v| v.as_str())
    }

    pub fn s3_region(&self) -> Option<&str> {
        self.config.get("region").and_then(|v| v.as_str())
    }

    pub fn s3_access_key_id(&self) -> Option<&str> {
        self.config.get("access_key_id").and_then(|v| v.as_str())
    }

    pub fn s3_secret_access_key(&self) -> Option<&str> {
        self.config.get("secret_access_key").and_then(|v| v.as_str())
    }

    pub fn s3_prefix(&self) -> Option<&str> {
        self.config.get("prefix").and_then(|v| v.as_str())
    }

    pub fn s3_endpoint(&self) -> Option<&str> {
        self.config.get("endpoint").and_then(|v| v.as_str())
    }
}

fn default_transcription_config() -> serde_json::Value {
    serde_json::json!({
        "preferred_languages": [],
        "providers": {}
    })
}

/// Main configuration structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigData {
    /// Path to the database file
    #[serde(default)]
    pub database_file: String,
    /// Default interface (null = auto-detect)
    pub default_interface: Option<String>,
    /// Window geometry for GUI
    pub window_geometry: Option<String>,
    /// Component implementations (future use)
    #[serde(default)]
    pub implementations: HashMap<String, String>,
    /// Theme configuration
    #[serde(default)]
    pub themes: Themes,
    /// Device ID (UUID7 hex)
    #[serde(default = "generate_device_id")]
    pub device_id: String,
    /// Human-readable device name
    #[serde(default = "get_default_device_name")]
    pub device_name: String,
    /// Sync configuration
    #[serde(default)]
    pub sync: SyncConfig,
    /// Server certificate fingerprint
    pub server_certificate_fingerprint: Option<String>,
    /// Directory for storing audio files
    pub audiofile_directory: Option<String>,
    /// Transcription configuration (stored as generic JSON - voicecore doesn't interpret this)
    #[serde(default = "default_transcription_config")]
    pub transcription: serde_json::Value,
    /// The periodic backup (SNAP-5): machine-level, desktop and server only
    #[serde(default)]
    pub backup: BackupConfig,
    /// Where this machine is reachable from the internet, for a server behind
    /// a real certificate; empty on a LAN device
    #[serde(default)]
    pub public_url: String,
}

fn generate_device_id() -> String {
    Uuid::now_v7().simple().to_string()
}

/// The name a phone's configuration starts with. The phone application
/// replaces it on its first start with the name Android gives the phone
/// (UI-11): the core, compiled for Android, cannot read Android's settings.
pub const PHONE_PLACEHOLDER_DEVICE_NAME: &str = "Voice Mobile";

/// What a computer says about itself, each part absent when it says nothing.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct NameSources {
    /// The name the user gave the computer in its settings: the pretty
    /// hostname on Linux, the Computer Name on macOS
    pub set_name: Option<String>,
    /// The hostname, also a name its user gave it
    pub hostname: Option<String>,
    /// The system: "Ubuntu", "Fedora Linux", "Mac", "Windows"
    pub system: Option<String>,
    /// "desktop", "laptop", "tablet" or "server"
    pub kind: Option<&'static str>,
    pub ipv4: Option<std::net::Ipv4Addr>,
    pub ipv6: Option<std::net::Ipv6Addr>,
}

/// The animals a device is named after when nothing names it (UI-11).
pub const CUTE_ANIMALS: &[&str] = &[
    "Wombat", "Otter", "Quokka", "Panda", "Koala", "Hedgehog", "Penguin", "Fox", "Owl", "Seal", "Lemur", "Capybara",
    "Alpaca", "Hamster", "Rabbit", "Squirrel", "Dolphin", "Puffin", "Sloth", "Meerkat", "Raccoon", "Chinchilla",
    "Fennec", "Duckling",
];

/// Whether a name tells the user which device this is: never empty and never
/// "localhost" (Android's hostname, and a Linux machine's when unset).
pub fn usable_host_name(name: &str) -> bool {
    let name = name.trim();
    !name.is_empty() && !name.to_ascii_lowercase().starts_with("localhost")
}

/// A new installation's device name (UI-11), the first of: the name the user
/// gave the device (set in the system, or its hostname); its type ("Ubuntu
/// desktop"); an animal with the ends of its addresses ("Wombat 81:4c 7.21").
/// `animal` chooses the animal. Never "localhost".
pub fn device_name_from(sources: &NameSources, animal: usize) -> String {
    for given in [&sources.set_name, &sources.hostname] {
        if let Some(name) = given.as_deref().map(str::trim).filter(|n| usable_host_name(n)) {
            return name.to_string();
        }
    }
    if let Some(kind) = sources.kind {
        return match sources.system.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            Some(system) => format!("{} {}", system, kind),
            None => format!("{}{}", kind[..1].to_uppercase(), &kind[1..]),
        };
    }
    animal_name(animal, sources.ipv6, sources.ipv4)
}

/// An animal followed by the last two bytes of the IPv6 address in hex and the
/// last two numbers of the IPv4 address: "Wombat 81:4c 7.21". A missing
/// address is left out.
pub fn animal_name(animal: usize, ipv6: Option<std::net::Ipv6Addr>, ipv4: Option<std::net::Ipv4Addr>) -> String {
    let mut name = CUTE_ANIMALS[animal % CUTE_ANIMALS.len()].to_string();
    if let Some(v6) = ipv6 {
        let o = v6.octets();
        name.push_str(&format!(" {:02x}:{:02x}", o[14], o[15]));
    }
    if let Some(v4) = ipv4 {
        let o = v4.octets();
        name.push_str(&format!(" {}.{}", o[2], o[3]));
    }
    name
}

/// The addresses a name ends with, from this device's interface addresses:
/// never a loopback address; a link-local one only when there is no other.
pub fn addresses_for_name(ips: &[std::net::IpAddr]) -> (Option<std::net::Ipv6Addr>, Option<std::net::Ipv4Addr>) {
    use std::net::IpAddr;
    let v6: Vec<_> = ips.iter().filter_map(|ip| match ip { IpAddr::V6(a) if !a.is_loopback() && !a.is_unspecified() => Some(*a), _ => None }).collect();
    let v4: Vec<_> = ips.iter().filter_map(|ip| match ip { IpAddr::V4(a) if !a.is_loopback() && !a.is_unspecified() => Some(*a), _ => None }).collect();
    let v6_link_local = |a: &std::net::Ipv6Addr| (a.segments()[0] & 0xffc0) == 0xfe80;
    let ipv6 = v6.iter().find(|a| !v6_link_local(a)).or_else(|| v6.first()).copied();
    let ipv4 = v4.iter().find(|a| !a.is_link_local()).or_else(|| v4.first()).copied();
    (ipv6, ipv4)
}

/// This device's interface addresses, as `if_addrs` finds them.
fn this_device_addresses() -> (Option<std::net::Ipv6Addr>, Option<std::net::Ipv4Addr>) {
    let ips: Vec<std::net::IpAddr> = if_addrs::get_if_addrs().map(|ifs| ifs.into_iter().map(|i| i.ip()).collect()).unwrap_or_default();
    addresses_for_name(&ips)
}

/// A choice of animal that differs between installations.
fn random_animal() -> usize {
    // The last bytes of a UUIDv7 are random
    let bytes = Uuid::now_v7();
    let b = bytes.as_bytes();
    usize::from(b[14]) << 8 | usize::from(b[15])
}

/// The animal name of this device, for a phone whose Android settings name
/// nothing (UI-11).
pub fn fallback_device_name() -> String {
    let (ipv6, ipv4) = this_device_addresses();
    animal_name(random_animal(), ipv6, ipv4)
}

/// The kind of computer from the SMBIOS chassis type Linux shows in
/// /sys/class/dmi/id/chassis_type; None for a number that says nothing useful.
pub fn chassis_kind(chassis_type: &str) -> Option<&'static str> {
    match chassis_type.trim().parse::<u32>().ok()? {
        3 | 4 | 5 | 6 | 7 | 13 | 15 | 16 | 24 | 35 | 36 => Some("desktop"),
        8 | 9 | 10 | 14 | 31 | 32 => Some("laptop"),
        11 | 30 => Some("tablet"),
        17 | 23 | 25 | 28 | 29 => Some("server"),
        _ => None,
    }
}

/// A value from a KEY=value file such as /etc/os-release or /etc/machine-info,
/// its quotes removed; None when the key is absent or empty.
pub fn key_value_of(text: &str, key: &str) -> Option<String> {
    text.lines().find_map(|line| {
        let value = line.trim().strip_prefix(key)?.strip_prefix('=')?.trim();
        let value = value.trim_matches(|c| c == '"' || c == '\'').trim();
        (!value.is_empty()).then(|| value.to_string())
    })
}

#[cfg(not(target_os = "android"))]
fn this_host_name() -> Option<String> {
    #[cfg(feature = "desktop")]
    {
        hostname::get().ok().map(|h| h.to_string_lossy().to_string())
    }
    #[cfg(not(feature = "desktop"))]
    {
        std::fs::read_to_string("/proc/sys/kernel/hostname").ok()
    }
}

/// The output of a command, trimmed; None when it cannot run or says nothing.
#[cfg(target_os = "macos")]
fn output_of(program: &str, args: &[&str]) -> Option<String> {
    let out = std::process::Command::new(program).args(args).output().ok()?;
    let text = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (out.status.success() && !text.is_empty()).then_some(text)
}

/// Everything this computer says about itself.
#[cfg(not(target_os = "android"))]
fn name_sources() -> NameSources {
    let (ipv6, ipv4) = this_device_addresses();
    #[cfg(target_os = "linux")]
    {
        let read = |path: &str| std::fs::read_to_string(path).ok();
        NameSources {
            set_name: read("/etc/machine-info").and_then(|t| key_value_of(&t, "PRETTY_HOSTNAME")),
            system: Some(read("/etc/os-release").and_then(|t| key_value_of(&t, "NAME")).unwrap_or_else(|| "Linux".to_string())),
            kind: read("/sys/class/dmi/id/chassis_type").and_then(|t| chassis_kind(&t)),
            hostname: this_host_name(),
            ipv6,
            ipv4,
        }
    }
    #[cfg(target_os = "macos")]
    {
        let model = output_of("sysctl", &["-n", "hw.model"]).unwrap_or_default();
        NameSources {
            set_name: output_of("scutil", &["--get", "ComputerName"]),
            system: Some("Mac".to_string()),
            // Older models say "MacBookPro16,1"; newer ones ("Mac14,2") do not say
            kind: model.starts_with("MacBook").then_some("laptop"),
            hostname: this_host_name(),
            ipv6,
            ipv4,
        }
    }
    #[cfg(target_os = "windows")]
    {
        NameSources {
            set_name: None,
            system: Some("Windows".to_string()),
            kind: None,
            hostname: std::env::var("COMPUTERNAME").ok().or_else(this_host_name),
            ipv6,
            ipv4,
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        NameSources { hostname: this_host_name(), ipv6, ipv4, ..NameSources::default() }
    }
}

fn get_default_device_name() -> String {
    #[cfg(target_os = "android")]
    {
        PHONE_PLACEHOLDER_DEVICE_NAME.to_string()
    }
    #[cfg(not(target_os = "android"))]
    {
        device_name_from(&name_sources(), random_animal())
    }
}

impl Default for ConfigData {
    fn default() -> Self {
        Self {
            database_file: String::new(),
            default_interface: None,
            window_geometry: None,
            implementations: HashMap::new(),
            themes: Themes::default(),
            device_id: generate_device_id(),
            device_name: get_default_device_name(),
            sync: SyncConfig::default(),
            server_certificate_fingerprint: None,
            audiofile_directory: None,
            transcription: default_transcription_config(),
            backup: BackupConfig::default(),
            public_url: String::new(),
        }
    }
}

/// Configuration manager
///
/// One directory holds an account: its `config.json`, `notes.db`, `audio/`.
/// On a desktop with several accounts the **machine** settings (the device
/// identity, the listen port, the certificates, the backup) live in the
/// root's `config.json` and override what an account's file says (Stage 2);
/// on the phone, and in a single-directory installation, the root is the
/// account and one file holds both.
pub struct Config {
    config_dir: PathBuf,
    config_file: PathBuf,
    /// The machine's `config.json`, when it is a different file
    machine_file: Option<PathBuf>,
    /// Where `certs/` lives: the root
    certs_root: PathBuf,
    data: ConfigData,
    /// The platform's key store wrapping the device key on disk (AUTH-9); none on the desktop
    wrapper: Option<std::sync::Arc<dyn SecretWrapper>>,
}

/// A platform key store that wraps a secret before it is written and
/// unwraps it after it is read (AUTH-9): the Android Keystore on the phone.
pub trait SecretWrapper: Send + Sync + std::fmt::Debug {
    fn wrap(&self, clear: &[u8]) -> Result<Vec<u8>, String>;
    fn unwrap(&self, wrapped: &[u8]) -> Result<Vec<u8>, String>;
}

/// A secret wrapped for the disk, base64url; empty stays empty.
fn wrap_secret(wrapper: &dyn SecretWrapper, what: &str, clear: &str) -> VoiceResult<String> {
    if clear.is_empty() {
        return Ok(String::new());
    }
    let wrapped = wrapper.wrap(clear.as_bytes()).map_err(|e| VoiceError::Config(format!("The {} could not be wrapped: {}", what, e)))?;
    Ok(base64::Engine::encode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, wrapped))
}

/// The clear secret, from the wrapped one in the file; empty when the file holds none.
fn unwrap_secret(wrapper: &dyn SecretWrapper, what: &str, wrapped: &str) -> VoiceResult<String> {
    if wrapped.is_empty() {
        return Ok(String::new());
    }
    let bytes = base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, wrapped.as_bytes())
        .map_err(|e| VoiceError::Config(format!("The wrapped {} is not base64: {}", what, e)))?;
    let opened = wrapper.unwrap(&bytes).map_err(|e| VoiceError::Config(format!("The {} could not be unwrapped: {}", what, e)))?;
    String::from_utf8(opened).map_err(|_| VoiceError::Config(format!("The unwrapped {} is not text", what)))
}

impl Config {
    /// Create a new configuration manager
    ///
    /// On mobile platforms (without the `desktop` feature), `config_dir` is required.
    /// With a platform key store (`wrapper`: the phone's Keystore; none on the
    /// desktop) the keys are wrapped on disk (AUTH-9) and unwrapped into memory.
    pub fn new(config_dir: Option<PathBuf>, wrapper: Option<std::sync::Arc<dyn SecretWrapper>>) -> VoiceResult<Self> {
        let config_dir = match config_dir {
            Some(dir) => dir,
            None => {
                #[cfg(feature = "desktop")]
                {
                    dirs::config_dir()
                        .unwrap_or_else(|| PathBuf::from("."))
                        .join("voice")
                }
                #[cfg(not(feature = "desktop"))]
                {
                    return Err(VoiceError::Config(
                        "config_dir is required on mobile platforms".to_string(),
                    ));
                }
            }
        };

        fs::create_dir_all(&config_dir)?;
        let config_file = config_dir.join("config.json");

        let mut data = if config_file.exists() {
            match fs::read_to_string(&config_file) {
                Ok(content) => serde_json::from_str(&content).unwrap_or_else(|_| {
                    let mut default = ConfigData::default();
                    default.database_file = config_dir.join("notes.db").to_string_lossy().to_string();
                    default
                }),
                Err(_) => {
                    let mut default = ConfigData::default();
                    default.database_file = config_dir.join("notes.db").to_string_lossy().to_string();
                    default
                }
            }
        } else {
            let mut default = ConfigData::default();
            default.database_file = config_dir.join("notes.db").to_string_lossy().to_string();
            default
        };

        // Ensure database_file is set even if config.json exists but doesn't have it
        if data.database_file.is_empty() {
            data.database_file = config_dir.join("notes.db").to_string_lossy().to_string();
        }

        if let Some(wrapper) = &wrapper {
            data.sync.device_key = unwrap_secret(wrapper.as_ref(), "device key", &data.sync.device_key_wrapped)?;
            data.sync.recording_key = unwrap_secret(wrapper.as_ref(), "recording key", &data.sync.recording_key_wrapped)?;
        }

        let config = Self {
            certs_root: config_dir.clone(),
            config_dir,
            config_file,
            machine_file: None,
            data,
            wrapper,
        };

        // Save the default configuration if there is none yet
        if !config.config_file.exists() {
            config.save()?;
        }

        // Every version this process writes carries the device identity from
        // the config, so a conflict can name the devices that disagreed.
        if let Ok(uuid) = config.device_id() {
            crate::database::set_local_device_id(uuid);
        }
        crate::database::set_local_device_name(config.device_name());

        Ok(config)
    }

    /// Open an account directory under a root that holds the machine's
    /// settings (Stage 2). The account's file is loaded as usual; the device
    /// id and name, the listen port, the backup and the public URL come from
    /// the root's `config.json` (made with defaults if missing), and the
    /// certificates live under the root.
    pub fn open_account(root: &Path, account_dir: &Path) -> VoiceResult<Self> {
        let machine = Self::new(Some(root.to_path_buf()), None)?;
        let mut config = Self::new(Some(account_dir.to_path_buf()), None)?;
        config.machine_file = Some(machine.config_file.clone());
        config.certs_root = root.to_path_buf();
        config.data.device_id = machine.data.device_id.clone();
        config.data.device_name = machine.data.device_name.clone();
        config.data.sync.server_port = machine.data.sync.server_port;
        config.data.backup = machine.data.backup.clone();
        config.data.public_url = machine.data.public_url.clone();
        if let Ok(uuid) = config.device_id() {
            crate::database::set_local_device_id(uuid);
        }
        crate::database::set_local_device_name(config.device_name());
        Ok(config)
    }

    /// The root that holds the machine's settings and certificates: the
    /// account directory itself on a single-directory installation.
    pub fn root(&self) -> &Path {
        &self.certs_root
    }

    /// The periodic backup settings (machine-level).
    pub fn backup(&self) -> &BackupConfig {
        &self.data.backup
    }

    /// Where an account's periodic backups go (SNAP-5): the configured
    /// directory, else `<root>/backups/<account id>/`. Never inside the
    /// recordings folder.
    pub fn backup_directory(&self, account_id: &str) -> PathBuf {
        let configured = self.data.backup.directory.trim();
        if configured.is_empty() {
            self.certs_root.join("backups").join(account_id)
        } else {
            PathBuf::from(configured).join(account_id)
        }
    }

    pub fn set_backup(&mut self, backup: BackupConfig) -> VoiceResult<()> {
        self.data.backup = backup;
        self.save()
    }

    /// Where this machine is reachable from the internet, or empty.
    pub fn public_url(&self) -> &str {
        &self.data.public_url
    }

    pub fn set_public_url(&mut self, url: &str) -> VoiceResult<()> {
        self.data.public_url = url.to_string();
        self.save()
    }

    /// Save configuration to file. The machine-level fields go to the
    /// machine's file as well when that is a different file.
    pub fn save(&self) -> VoiceResult<()> {
        let content = match &self.wrapper {
            // The keys leave memory wrapped only (AUTH-9)
            Some(wrapper) => {
                let mut on_disk = self.data.clone();
                on_disk.sync.device_key_wrapped = wrap_secret(wrapper.as_ref(), "device key", &self.data.sync.device_key)?;
                on_disk.sync.device_key = String::new();
                on_disk.sync.recording_key_wrapped = wrap_secret(wrapper.as_ref(), "recording key", &self.data.sync.recording_key)?;
                on_disk.sync.recording_key = String::new();
                serde_json::to_string_pretty(&on_disk)?
            }
            None => serde_json::to_string_pretty(&self.data)?,
        };
        fs::write(&self.config_file, content)?;
        if let Some(machine_file) = &self.machine_file {
            let mut machine: ConfigData = fs::read_to_string(machine_file)
                .ok()
                .and_then(|c| serde_json::from_str(&c).ok())
                .unwrap_or_default();
            machine.device_id = self.data.device_id.clone();
            machine.device_name = self.data.device_name.clone();
            machine.sync.server_port = self.data.sync.server_port;
            machine.backup = self.data.backup.clone();
            machine.public_url = self.data.public_url.clone();
            fs::write(machine_file, serde_json::to_string_pretty(&machine)?)?;
        }
        Ok(())
    }

    /// Get the configuration directory path
    pub fn config_dir(&self) -> &Path {
        &self.config_dir
    }

    /// Get the database file path
    pub fn database_file(&self) -> &str {
        &self.data.database_file
    }

    /// Get the device ID as bytes
    pub fn device_id(&self) -> VoiceResult<Uuid> {
        Uuid::parse_str(&self.data.device_id)
            .map_err(|e| VoiceError::Config(format!("Invalid device_id: {}", e)))
    }

    /// Get the device ID as hex string
    pub fn device_id_hex(&self) -> &str {
        &self.data.device_id
    }

    /// Get the human-readable device name
    pub fn device_name(&self) -> &str {
        &self.data.device_name
    }

    /// Set the device name
    pub fn set_device_name(&mut self, name: &str) -> VoiceResult<()> {
        self.data.device_name = name.to_string();
        crate::database::set_local_device_name(name);
        self.save()
    }

    /// Get sync configuration
    pub fn sync_config(&self) -> &SyncConfig {
        &self.data.sync
    }

    /// Check if sync is enabled
    pub fn is_sync_enabled(&self) -> bool {
        self.data.sync.enabled
    }

    /// This device's key for the account, or empty before one was made.
    pub fn device_key(&self) -> &str {
        &self.data.sync.device_key
    }

    /// Store this device's key (made by `auth::ensure_own_device_card`, or
    /// issued at pairing).
    pub fn set_device_key(&mut self, key: &str) -> VoiceResult<()> {
        self.data.sync.device_key = key.to_string();
        self.save()
    }

    /// The account's recording key (Stage 15), or None until encryption was set up here.
    pub fn recording_key(&self) -> Option<crate::crypto::RecordingKey> {
        if self.data.sync.recording_key.is_empty() {
            None
        } else {
            crate::crypto::RecordingKey::from_text(&self.data.sync.recording_key).ok()
        }
    }

    /// The recording key's text, for pairing replies and export; empty when there is none.
    pub fn recording_key_text(&self) -> &str {
        &self.data.sync.recording_key
    }

    /// Keep the account's recording key: made here, imported, or received at pairing.
    pub fn set_recording_key(&mut self, text: &str) -> VoiceResult<()> {
        let key = crate::crypto::RecordingKey::from_text(text)?;
        self.data.sync.recording_key = key.to_text();
        self.save()
    }

    /// Whether the recording key was exported from this device once (ENC-1).
    pub fn recording_key_exported(&self) -> bool {
        self.data.sync.recording_key_exported
    }

    pub fn set_recording_key_exported(&mut self, exported: bool) -> VoiceResult<()> {
        self.data.sync.recording_key_exported = exported;
        self.save()
    }

    /// Enable or disable sync
    pub fn set_sync_enabled(&mut self, enabled: bool) -> VoiceResult<()> {
        self.data.sync.enabled = enabled;
        self.save()
    }

    /// Get the sync server port
    pub fn sync_server_port(&self) -> u16 {
        self.data.sync.server_port
    }

    /// Set the sync server port
    pub fn set_sync_server_port(&mut self, port: u16) -> VoiceResult<()> {
        self.data.sync.server_port = port;
        self.save()
    }

    /// Get the maximum sync file size in MB
    pub fn max_sync_file_size_mb(&self) -> u32 {
        self.data.sync.max_sync_file_size_mb
    }

    /// Get the maximum sync file size in bytes
    pub fn max_sync_file_size_bytes(&self) -> u64 {
        u64::from(self.data.sync.max_sync_file_size_mb) * 1024 * 1024
    }

    /// Set the maximum sync file size in MB
    pub fn set_max_sync_file_size_mb(&mut self, size_mb: u32) -> VoiceResult<()> {
        self.data.sync.max_sync_file_size_mb = size_mb;
        self.save()
    }

    /// Whether this installation mirrors every cloud audio file locally on sync
    pub fn mirror_audio_files(&self) -> bool {
        self.data.sync.mirror_audio_files
    }

    /// Enable or disable mirroring of all cloud audio files on sync
    pub fn set_mirror_audio_files(&mut self, enabled: bool) -> VoiceResult<()> {
        self.data.sync.mirror_audio_files = enabled;
        self.save()
    }

    /// Get list of sync peers
    pub fn peers(&self) -> &[PeerConfig] {
        &self.data.sync.peers
    }

    /// Add a new sync peer
    pub fn add_peer(
        &mut self,
        peer_id: &str,
        peer_name: &str,
        peer_url: &str,
        certificate_fingerprint: Option<&str>,
        allow_update: bool,
    ) -> VoiceResult<()> {
        // Validate peer_id format
        if peer_id.len() != 32 || !peer_id.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(VoiceError::validation("peer_id", "must be 32 hex characters"));
        }

        // Added by hand or by pairing: no longer forgotten (Stage 5)
        self.data.sync.forgotten_peers.retain(|p| p != peer_id);

        // Check if peer already exists
        if let Some(existing) = self.data.sync.peers.iter_mut().find(|p| p.peer_id == peer_id) {
            if !allow_update {
                return Err(VoiceError::validation("peer_id", "peer already exists"));
            }
            existing.peer_name = peer_name.to_string();
            existing.peer_url = peer_url.to_string();
            if let Some(fp) = certificate_fingerprint {
                existing.certificate_fingerprint = Some(fp.to_string());
            }
        } else {
            self.data.sync.peers.push(PeerConfig {
                peer_id: peer_id.to_string(),
                peer_name: peer_name.to_string(),
                peer_url: peer_url.to_string(),
                certificate_fingerprint: certificate_fingerprint.map(String::from),
            });
        }

        self.save()
    }

    /// Remove a sync peer
    pub fn remove_peer(&mut self, peer_id: &str) -> VoiceResult<bool> {
        let original_len = self.data.sync.peers.len();
        self.data.sync.peers.retain(|p| p.peer_id != peer_id);
        let removed = self.data.sync.peers.len() < original_len;
        if removed {
            self.save()?;
        }
        Ok(removed)
    }

    /// Forget a peer on this device (Stage 5): it leaves the list, and its
    /// card does not bring it back until it is added again by hand or by
    /// pairing. Returns whether it was in the list.
    pub fn forget_peer(&mut self, peer_id: &str) -> VoiceResult<bool> {
        let removed = self.remove_peer(peer_id)?;
        if !self.data.sync.forgotten_peers.iter().any(|p| p == peer_id) {
            self.data.sync.forgotten_peers.push(peer_id.to_string());
        }
        if self.data.sync.last_peer_id == peer_id {
            self.data.sync.last_peer_id.clear();
        }
        self.save()?;
        Ok(removed)
    }

    /// Whether a peer was forgotten here (Stage 5).
    pub fn is_forgotten(&self, peer_id: &str) -> bool {
        self.data.sync.forgotten_peers.iter().any(|p| p == peer_id)
    }

    /// The local name of a peer (Stage 5), shown in place of its card's.
    pub fn rename_peer(&mut self, peer_id: &str, name: &str) -> VoiceResult<bool> {
        let name = name.trim();
        if name.is_empty() {
            return Err(VoiceError::validation("name", "A peer's name cannot be empty"));
        }
        if let Some(peer) = self.data.sync.peers.iter_mut().find(|p| p.peer_id == peer_id) {
            peer.peer_name = name.to_string();
            self.save()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Hours of silence after which the listener stops itself; 0 means never.
    pub fn listener_idle_stop_hours(&self) -> u32 {
        self.data.sync.listener_idle_stop_hours
    }

    pub fn set_listener_idle_stop_hours(&mut self, hours: u32) -> VoiceResult<()> {
        self.data.sync.listener_idle_stop_hours = hours;
        self.save()
    }

    /// The peer of the last operation (Stage 5), if it is still in the list.
    pub fn last_peer(&self) -> Option<&PeerConfig> {
        self.get_peer(&self.data.sync.last_peer_id.clone())
    }

    pub fn set_last_peer(&mut self, peer_id: &str) -> VoiceResult<()> {
        if self.data.sync.last_peer_id != peer_id {
            self.data.sync.last_peer_id = peer_id.to_string();
            self.save()?;
        }
        Ok(())
    }

    /// Get a specific peer by ID
    pub fn get_peer(&self, peer_id: &str) -> Option<&PeerConfig> {
        self.data.sync.peers.iter().find(|p| p.peer_id == peer_id)
    }

    /// Update a peer's certificate fingerprint
    pub fn update_peer_certificate(&mut self, peer_id: &str, fingerprint: &str) -> VoiceResult<bool> {
        if let Some(peer) = self.data.sync.peers.iter_mut().find(|p| p.peer_id == peer_id) {
            peer.certificate_fingerprint = Some(fingerprint.to_string());
            self.save()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Get the certificates directory
    pub fn certs_dir(&self) -> VoiceResult<PathBuf> {
        let certs_dir = self.certs_root.join("certs");
        fs::create_dir_all(&certs_dir)?;
        Ok(certs_dir)
    }

    /// Get TUI colors
    pub fn tui_colors(&self) -> (&str, &str) {
        (
            &self.data.themes.colours.tui_border_focused,
            &self.data.themes.colours.tui_border_unfocused,
        )
    }

    /// Get warning color based on theme
    pub fn warning_color(&self, theme: &str) -> &str {
        match theme {
            "light" => self
                .data
                .themes
                .colours
                .warnings_light
                .as_deref()
                .unwrap_or(&self.data.themes.colours.warnings),
            _ => self
                .data
                .themes
                .colours
                .warnings_dark
                .as_deref()
                .unwrap_or(&self.data.themes.colours.warnings),
        }
    }

    /// Get the audio file directory path
    pub fn audiofile_directory(&self) -> Option<&str> {
        self.data.audiofile_directory.as_deref()
    }

    /// Set the audio file directory path
    pub fn set_audiofile_directory(&mut self, path: &str) -> VoiceResult<()> {
        self.data.audiofile_directory = Some(path.to_string());
        self.save()
    }

    /// Clear the audio file directory path
    pub fn clear_audiofile_directory(&mut self) -> VoiceResult<()> {
        self.data.audiofile_directory = None;
        self.save()
    }

    /// Get the audio file trash directory path (audiofile_directory + "_trash")
    pub fn audiofile_trash_directory(&self) -> Option<PathBuf> {
        self.data.audiofile_directory.as_ref().map(|dir| {
            let path = PathBuf::from(dir);
            let parent = path.parent().unwrap_or(Path::new(""));
            let name = path.file_name().unwrap_or_default().to_string_lossy();
            parent.join(format!("{}_trash", name))
        })
    }

    /// Get transcription configuration as raw JSON
    ///
    /// Voicecore stores this data but doesn't interpret it - the transcription
    /// module is responsible for understanding the structure.
    pub fn transcription_json(&self) -> &serde_json::Value {
        &self.data.transcription
    }

    /// Set transcription configuration from raw JSON
    pub fn set_transcription_json(&mut self, value: serde_json::Value) -> VoiceResult<()> {
        self.data.transcription = value;
        self.save()
    }

    /// Get a configuration value
    pub fn get(&self, key: &str) -> Option<String> {
        match key {
            "database_file" => Some(self.data.database_file.clone()),
            "default_interface" => self.data.default_interface.clone(),
            "device_id" => Some(self.data.device_id.clone()),
            "device_name" => Some(self.data.device_name.clone()),
            "server_certificate_fingerprint" => self.data.server_certificate_fingerprint.clone(),
            "audiofile_directory" => self.data.audiofile_directory.clone(),
            _ => None,
        }
    }

    /// Set a configuration value
    pub fn set(&mut self, key: &str, value: &str) -> VoiceResult<()> {
        match key {
            "database_file" => self.data.database_file = value.to_string(),
            "default_interface" => self.data.default_interface = Some(value.to_string()),
            "device_name" => self.data.device_name = value.to_string(),
            "server_certificate_fingerprint" => {
                self.data.server_certificate_fingerprint = Some(value.to_string())
            }
            "audiofile_directory" => self.data.audiofile_directory = Some(value.to_string()),
            _ => return Err(VoiceError::Config(format!("Unknown config key: {}", key))),
        }
        self.save()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A wrapper for the test: every byte flipped, so the file plainly does
    /// not hold the key and the unwrap plainly needs the wrapper.
    #[derive(Debug)]
    struct Flip;
    impl SecretWrapper for Flip {
        fn wrap(&self, clear: &[u8]) -> Result<Vec<u8>, String> { Ok(clear.iter().map(|b| !b).collect()) }
        fn unwrap(&self, wrapped: &[u8]) -> Result<Vec<u8>, String> { Ok(wrapped.iter().map(|b| !b).collect()) }
    }

    /// AUTH-9: under a wrapper the device key is on disk wrapped only, and
    /// comes back through the wrapper; without one, as before.
    #[test]
    fn the_device_key_is_written_wrapped_and_read_back_through_the_wrapper() {
        let temp = tempfile::TempDir::new().unwrap();
        let dir = temp.path().to_path_buf();
        let wrapper: std::sync::Arc<dyn SecretWrapper> = std::sync::Arc::new(Flip);
        let mut config = Config::new(Some(dir.clone()), Some(wrapper.clone())).unwrap();
        config.set_device_key("kEy0123456789abcdefghijklmnopqrstuvwxyzABC").unwrap();
        let file = std::fs::read_to_string(dir.join("config.json")).unwrap();
        assert!(!file.contains("kEy0123456789"), "the clear key is not in the file");
        assert!(file.contains("device_key_wrapped"));
        let json: serde_json::Value = serde_json::from_str(&file).unwrap();
        assert_eq!(json["sync"]["device_key"], "");

        let mut again = Config::new(Some(dir.clone()), Some(wrapper.clone())).unwrap();
        assert_eq!(again.device_key(), "kEy0123456789abcdefghijklmnopqrstuvwxyzABC");
        // The recording key is wrapped the same way (Stage 15)
        let recording = crate::crypto::RecordingKey::generate().to_text();
        again.set_recording_key(&recording).unwrap();
        let file = std::fs::read_to_string(dir.join("config.json")).unwrap();
        assert!(!file.contains(&recording) && file.contains("recording_key_wrapped"));
        assert_eq!(Config::new(Some(dir.clone()), Some(wrapper)).unwrap().recording_key_text(), recording);
        let without = Config::new(Some(dir.clone()), None).unwrap();
        assert_eq!(without.device_key(), "", "without the wrapper the key is not readable");

    }

    fn sources(set_name: Option<&str>, hostname: Option<&str>, system: Option<&str>, kind: Option<&'static str>) -> NameSources {
        NameSources { set_name: set_name.map(str::to_string), hostname: hostname.map(str::to_string), system: system.map(str::to_string), kind, ..NameSources::default() }
    }

    #[test]
    fn a_name_the_user_gave_is_the_device_name() {
        assert_eq!(device_name_from(&sources(Some(" המחשב של דותן "), Some("teva-2025"), Some("Ubuntu"), Some("desktop")), 0), "המחשב של דותן");
        assert_eq!(device_name_from(&sources(None, Some("teva-2025"), Some("Ubuntu"), Some("desktop")), 0), "teva-2025");
        assert_eq!(device_name_from(&sources(None, Some("DESKTOP-7Q3K2LM"), Some("Windows"), None), 0), "DESKTOP-7Q3K2LM");
    }

    #[test]
    fn without_a_given_name_the_device_type_names_it() {
        assert_eq!(device_name_from(&sources(None, None, Some("Ubuntu"), Some("desktop")), 0), "Ubuntu desktop");
        assert_eq!(device_name_from(&sources(None, Some("localhost"), Some("Mac"), Some("laptop")), 0), "Mac laptop");
        assert_eq!(device_name_from(&sources(Some(""), None, None, Some("laptop")), 0), "Laptop");
    }

    #[test]
    fn without_a_name_or_a_type_an_animal_and_the_ends_of_the_addresses_name_it() {
        let v6: std::net::Ipv6Addr = "2a0d:6fc0:12:3400::814c".parse().unwrap();
        let v4: std::net::Ipv4Addr = "192.168.7.21".parse().unwrap();
        let unnamed = NameSources { hostname: Some("localhost".to_string()), system: Some("Windows".to_string()), ipv6: Some(v6), ipv4: Some(v4), ..NameSources::default() };
        assert_eq!(device_name_from(&unnamed, 0), "Wombat 81:4c 7.21");
        assert_eq!(animal_name(1, None, Some(v4)), "Otter 7.21");
        assert_eq!(animal_name(2, Some("fe80::1:7".parse().unwrap()), None), "Quokka 00:07");
        assert_eq!(animal_name(CUTE_ANIMALS.len(), None, None), "Wombat", "the choice goes round the list");
    }

    #[test]
    fn localhost_is_never_the_device_name() {
        for given in ["localhost", "LOCALHOST", "localhost.localdomain", "  ", ""] {
            let name = device_name_from(&sources(Some(given), Some(given), None, None), 3);
            assert!(name.starts_with("Panda"), "{:?} gave {:?}", given, name);
        }
        let here = get_default_device_name();
        assert!(usable_host_name(&here), "this machine's default name is {:?}", here);
    }

    #[test]
    fn a_name_ends_with_real_addresses_and_link_local_only_when_there_is_nothing_else() {
        use std::net::IpAddr;
        let ips: Vec<IpAddr> = ["127.0.0.1", "::1", "fe80::aa:1", "169.254.3.4", "10.0.5.9", "2a0d:6fc0::beef"].iter().map(|s| s.parse().unwrap()).collect();
        assert_eq!(addresses_for_name(&ips), (Some("2a0d:6fc0::beef".parse().unwrap()), Some("10.0.5.9".parse().unwrap())));
        let only_link_local: Vec<IpAddr> = ["::1", "fe80::aa:1", "169.254.3.4"].iter().map(|s| s.parse().unwrap()).collect();
        assert_eq!(addresses_for_name(&only_link_local), (Some("fe80::aa:1".parse().unwrap()), Some("169.254.3.4".parse().unwrap())));
        assert_eq!(addresses_for_name(&["127.0.0.1".parse().unwrap()]), (None, None));
    }

    #[test]
    fn the_chassis_type_says_desktop_laptop_tablet_or_server() {
        assert_eq!(chassis_kind("6\n"), Some("desktop"));
        assert_eq!(chassis_kind("3"), Some("desktop"));
        assert_eq!(chassis_kind("10"), Some("laptop"));
        assert_eq!(chassis_kind("31"), Some("laptop"));
        assert_eq!(chassis_kind("30"), Some("tablet"));
        assert_eq!(chassis_kind("23"), Some("server"));
        assert_eq!(chassis_kind("1"), None, "Other");
        assert_eq!(chassis_kind("2"), None, "Unknown");
        assert_eq!(chassis_kind("שולחני"), None);
        assert_eq!(chassis_kind(""), None);
    }

    #[test]
    fn a_value_is_read_from_os_release_and_machine_info_without_its_quotes() {
        let os_release = "PRETTY_NAME=\"Ubuntu 24.04.4 LTS\"\nNAME=\"Ubuntu\"\nVERSION_ID=\"24.04\"\n";
        assert_eq!(key_value_of(os_release, "NAME").as_deref(), Some("Ubuntu"));
        assert_eq!(key_value_of(os_release, "PRETTY_NAME").as_deref(), Some("Ubuntu 24.04.4 LTS"));
        assert_eq!(key_value_of("PRETTY_HOSTNAME='המחשב של דותן'\n", "PRETTY_HOSTNAME").as_deref(), Some("המחשב של דותן"));
        assert_eq!(key_value_of("PRETTY_HOSTNAME=\n", "PRETTY_HOSTNAME"), None);
        assert_eq!(key_value_of("ICON_NAME=computer-desktop\n", "PRETTY_HOSTNAME"), None);
    }

    #[test]
    fn a_forgotten_peer_is_remembered_as_such_until_added_again() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut config = Config::new(Some(dir.path().to_path_buf()), None).unwrap();
        let id = "0199aaaaaaaa7000800000000000000a";
        config.add_peer(id, "Desk", "https://desk:8384", None, false).unwrap();
        config.set_last_peer(id).unwrap();
        assert_eq!(config.last_peer().unwrap().peer_id, id);
        assert!(config.forget_peer(id).unwrap());
        assert!(config.is_forgotten(id));
        assert!(config.last_peer().is_none(), "the last peer is not a forgotten one");
        assert!(!config.forget_peer(id).unwrap(), "already gone");
        let again = Config::new(Some(dir.path().to_path_buf()), None).unwrap();
        assert!(again.is_forgotten(id), "written to the file");
        config.add_peer(id, "Desk", "https://desk:8384", None, false).unwrap();
        assert!(!config.is_forgotten(id));
        assert!(config.rename_peer(id, "Study").unwrap());
        assert_eq!(config.get_peer(id).unwrap().peer_name, "Study");
        assert!(config.rename_peer(id, "  ").is_err());
        assert!(!config.rename_peer("0199aaaaaaaa7000800000000000000b", "x").unwrap());
    }
    use tempfile::TempDir;

    #[test]
    fn test_default_config() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::new(Some(temp_dir.path().to_path_buf()), None).unwrap();

        assert!(!config.device_id_hex().is_empty());
        assert!(!config.device_name().is_empty());
        assert!(!config.is_sync_enabled());
        assert_eq!(config.sync_server_port(), 8384);
    }

    #[test]
    fn test_add_peer() {
        let temp_dir = TempDir::new().unwrap();
        let mut config = Config::new(Some(temp_dir.path().to_path_buf()), None).unwrap();

        let peer_id = "0".repeat(32);
        config
            .add_peer(&peer_id, "Test Peer", "https://example.com:8384", None, false)
            .unwrap();

        let peer = config.get_peer(&peer_id).unwrap();
        assert_eq!(peer.peer_name, "Test Peer");
        assert_eq!(peer.peer_url, "https://example.com:8384");
    }

    #[test]
    fn test_remove_peer() {
        let temp_dir = TempDir::new().unwrap();
        let mut config = Config::new(Some(temp_dir.path().to_path_buf()), None).unwrap();

        let peer_id = "0".repeat(32);
        config
            .add_peer(&peer_id, "Test Peer", "https://example.com:8384", None, false)
            .unwrap();

        let removed = config.remove_peer(&peer_id).unwrap();
        assert!(removed);
        assert!(config.get_peer(&peer_id).is_none());
    }

    #[test]
    fn test_invalid_peer_id() {
        let temp_dir = TempDir::new().unwrap();
        let mut config = Config::new(Some(temp_dir.path().to_path_buf()), None).unwrap();

        let result = config.add_peer("invalid", "Test", "https://example.com", None, false);
        assert!(result.is_err());
    }

    #[test]
    fn test_config_persistence() {
        let temp_dir = TempDir::new().unwrap();

        {
            let mut config = Config::new(Some(temp_dir.path().to_path_buf()), None).unwrap();
            config.set_device_name("Test Device").unwrap();
            config.set_sync_enabled(true).unwrap();
        }

        {
            let config = Config::new(Some(temp_dir.path().to_path_buf()), None).unwrap();
            assert_eq!(config.device_name(), "Test Device");
            assert!(config.is_sync_enabled());
        }
    }

    #[test]
    fn test_audiofile_directory_default_none() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::new(Some(temp_dir.path().to_path_buf()), None).unwrap();

        assert!(config.audiofile_directory().is_none());
        assert!(config.audiofile_trash_directory().is_none());
    }

    #[test]
    fn test_set_audiofile_directory() {
        let temp_dir = TempDir::new().unwrap();
        let mut config = Config::new(Some(temp_dir.path().to_path_buf()), None).unwrap();

        config.set_audiofile_directory("/home/user/audiofiles").unwrap();

        assert_eq!(config.audiofile_directory(), Some("/home/user/audiofiles"));
        assert_eq!(
            config.audiofile_trash_directory(),
            Some(PathBuf::from("/home/user/audiofiles_trash"))
        );
    }

    #[test]
    fn test_audiofile_directory_persistence() {
        let temp_dir = TempDir::new().unwrap();

        {
            let mut config = Config::new(Some(temp_dir.path().to_path_buf()), None).unwrap();
            config.set_audiofile_directory("/path/to/audio").unwrap();
        }

        {
            let config = Config::new(Some(temp_dir.path().to_path_buf()), None).unwrap();
            assert_eq!(config.audiofile_directory(), Some("/path/to/audio"));
        }
    }

    #[test]
    fn test_clear_audiofile_directory() {
        let temp_dir = TempDir::new().unwrap();
        let mut config = Config::new(Some(temp_dir.path().to_path_buf()), None).unwrap();

        config.set_audiofile_directory("/path/to/audio").unwrap();
        assert!(config.audiofile_directory().is_some());

        config.clear_audiofile_directory().unwrap();
        assert!(config.audiofile_directory().is_none());
    }

    #[test]
    fn test_audiofile_directory_via_get_set() {
        let temp_dir = TempDir::new().unwrap();
        let mut config = Config::new(Some(temp_dir.path().to_path_buf()), None).unwrap();

        // Initially None
        assert!(config.get("audiofile_directory").is_none());

        // Set via set()
        config.set("audiofile_directory", "/audio/files").unwrap();
        assert_eq!(config.get("audiofile_directory"), Some("/audio/files".to_string()));
    }

    #[test]
    fn test_transcription_config_defaults() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::new(Some(temp_dir.path().to_path_buf()), None).unwrap();

        // Transcription config is stored as generic JSON
        let transcription = config.transcription_json();
        assert!(transcription.is_object());

        // Default should have empty preferred languages
        let languages = transcription.get("preferred_languages").unwrap();
        assert!(languages.as_array().unwrap().is_empty());

        // Default providers should be empty object
        let providers = transcription.get("providers").unwrap();
        assert!(providers.is_object());
        assert!(providers.as_object().unwrap().is_empty());
    }

    #[test]
    fn test_set_transcription_json() {
        let temp_dir = TempDir::new().unwrap();
        let mut config = Config::new(Some(temp_dir.path().to_path_buf()), None).unwrap();

        let new_config = serde_json::json!({
            "preferred_languages": ["he", "en"],
            "providers": {
                "whisper": {
                    "model_path": "/path/to/model.bin"
                }
            }
        });

        config.set_transcription_json(new_config).unwrap();

        let transcription = config.transcription_json();
        let languages = transcription.get("preferred_languages").unwrap().as_array().unwrap();
        assert_eq!(languages.len(), 2);
        assert_eq!(languages[0], "he");
        assert_eq!(languages[1], "en");
    }

    #[test]
    fn test_transcription_config_persistence() {
        let temp_dir = TempDir::new().unwrap();

        {
            let mut config = Config::new(Some(temp_dir.path().to_path_buf()), None).unwrap();
            let new_config = serde_json::json!({
                "preferred_languages": ["ar", "en"],
                "providers": {
                    "whisper": {
                        "model_path": "/models/whisper.bin"
                    }
                }
            });
            config.set_transcription_json(new_config).unwrap();
        }

        {
            let config = Config::new(Some(temp_dir.path().to_path_buf()), None).unwrap();
            let transcription = config.transcription_json();
            let languages = transcription.get("preferred_languages").unwrap().as_array().unwrap();
            assert_eq!(languages.len(), 2);
            assert_eq!(languages[0], "ar");
            assert_eq!(languages[1], "en");
            assert_eq!(
                transcription.get("providers").unwrap()
                    .get("whisper").unwrap()
                    .get("model_path").unwrap()
                    .as_str().unwrap(),
                "/models/whisper.bin"
            );
        }
    }

    #[test]
    fn test_mirror_audio_files_default_and_persistence() {
        let temp_dir = TempDir::new().unwrap();
        {
            let mut config = Config::new(Some(temp_dir.path().to_path_buf()), None).unwrap();
            assert!(!config.mirror_audio_files());
            config.set_mirror_audio_files(true).unwrap();
        }
        {
            let config = Config::new(Some(temp_dir.path().to_path_buf()), None).unwrap();
            assert!(config.mirror_audio_files());
        }
    }

}
