//! TLS certificate management for Voice sync.
//!
//! This module handles:
//! - Self-signed certificate generation
//! - Certificate fingerprint computation
//! - Trust On First Use (TOFU) verification
//! - SSL context creation

use std::fs;
use std::path::Path;

use sha2::{Digest, Sha256};

use crate::config::Config;
use crate::sync_protocol::codes;
use crate::error::{VoiceError, VoiceResult};

/// Certificate validity period (10 years in days)
pub const CERT_VALIDITY_DAYS: u32 = 3650;

/// Compute SHA-256 fingerprint from DER-encoded certificate data
pub fn compute_fingerprint_from_der(der_data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(der_data);
    let result = hasher.finalize();

    let hex_parts: Vec<String> = result.iter().map(|b| format!("{:02x}", b)).collect();
    format!("SHA256:{}", hex_parts.join(":"))
}

/// Compute SHA-256 fingerprint from PEM certificate file
pub fn compute_fingerprint(cert_path: &Path) -> VoiceResult<String> {
    let pem_data = fs::read(cert_path)?;
    compute_fingerprint_from_pem(&pem_data)
}

/// Compute SHA-256 fingerprint from PEM certificate data
pub fn compute_fingerprint_from_pem(pem_data: &[u8]) -> VoiceResult<String> {
    // Find the base64 content between BEGIN and END markers
    let pem_str = std::str::from_utf8(pem_data)
        .map_err(|e| VoiceError::Tls(format!("Invalid PEM encoding: {}", e)))?;

    let start_marker = "-----BEGIN CERTIFICATE-----";
    let end_marker = "-----END CERTIFICATE-----";

    let start = pem_str
        .find(start_marker)
        .ok_or_else(|| VoiceError::Tls("No certificate found in PEM".to_string()))?
        + start_marker.len();
    let end = pem_str
        .find(end_marker)
        .ok_or_else(|| VoiceError::Tls("Invalid PEM format".to_string()))?;

    let base64_content: String = pem_str[start..end]
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();

    // Decode base64 to get DER
    use base64::Engine;
    let der_data = base64::engine::general_purpose::STANDARD
        .decode(&base64_content)
        .map_err(|e| VoiceError::Tls(format!("Invalid base64 in PEM: {}", e)))?;

    Ok(compute_fingerprint_from_der(&der_data))
}

/// Verify that a certificate matches an expected fingerprint
pub fn verify_fingerprint(cert_path: &Path, expected_fingerprint: &str) -> VoiceResult<bool> {
    let actual_fingerprint = compute_fingerprint(cert_path)?;
    Ok(actual_fingerprint.to_lowercase() == expected_fingerprint.to_lowercase())
}

/// The crypto provider this build of rustls uses: aws-lc on the desktop,
/// ring on Android (see Cargo.toml). Every TLS configuration is built with
/// it explicitly, so no process-wide default has to be installed.
pub fn crypto_provider() -> std::sync::Arc<rustls::crypto::CryptoProvider> {
    #[cfg(not(target_os = "android"))]
    {
        std::sync::Arc::new(rustls::crypto::aws_lc_rs::default_provider())
    }
    #[cfg(target_os = "android")]
    {
        std::sync::Arc::new(rustls::crypto::ring::default_provider())
    }
}

/// The TLS configuration a listener serves with: its own certificate and
/// key from `certs/`, no client certificates (the device key in the request
/// is what authenticates the caller, AUTH-3).
pub fn server_config(cert_path: &Path, key_path: &Path) -> VoiceResult<std::sync::Arc<rustls::ServerConfig>> {
    use std::io::BufReader;
    let certs = rustls_pemfile::certs(&mut BufReader::new(fs::File::open(cert_path)?))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| VoiceError::Tls(format!("Cannot read the certificate: {}", e)))?;
    let key = rustls_pemfile::private_key(&mut BufReader::new(fs::File::open(key_path)?))
        .map_err(|e| VoiceError::Tls(format!("Cannot read the key: {}", e)))?
        .ok_or_else(|| VoiceError::Tls("The key file holds no private key".to_string()))?;
    let config = rustls::ServerConfig::builder_with_provider(crypto_provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| VoiceError::Tls(e.to_string()))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| VoiceError::Tls(e.to_string()))?;
    Ok(std::sync::Arc::new(config))
}

/// Verifies a peer by the fingerprint pinned for it and nothing else
/// (AUTH-7): the certificate is self-signed, so no root could vouch for it,
/// and the pin came over a channel that already authenticated the peer.
#[derive(Debug)]
struct PinnedVerifier {
    fingerprint: String,
    provider: std::sync::Arc<rustls::crypto::CryptoProvider>,
}

impl rustls::client::danger::ServerCertVerifier for PinnedVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        let actual = compute_fingerprint_from_der(end_entity.as_ref());
        if actual.eq_ignore_ascii_case(&self.fingerprint) {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General(format!(
                "The peer's certificate is {}, not the pinned {} ({})",
                actual, self.fingerprint, codes::CERTIFICATE_MISMATCH
            )))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.provider.signature_verification_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.provider.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider.signature_verification_algorithms.supported_schemes()
    }
}

/// A client configuration that accepts exactly one certificate: the one
/// whose SHA-256 fingerprint is `fingerprint`.
pub fn pinned_client_config(fingerprint: &str) -> VoiceResult<rustls::ClientConfig> {
    let provider = crypto_provider();
    let verifier = std::sync::Arc::new(PinnedVerifier { fingerprint: fingerprint.to_string(), provider: provider.clone() });
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| VoiceError::Tls(e.to_string()))?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    Ok(config)
}

/// Generate a self-signed certificate
///
/// Returns (cert_pem, key_pem) as strings
pub fn generate_self_signed_cert(
    cert_path: &Path,
    key_path: &Path,
    common_name: &str,
    device_id: Option<&str>,
) -> VoiceResult<(String, String)> {
    use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair, SanType, IsCa};

    // Create certificate parameters
    let mut params = CertificateParams::default();

    // Set distinguished name
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, common_name);
    if let Some(id) = device_id {
        dn.push(DnType::OrganizationalUnitName, id);
    }
    params.distinguished_name = dn;

    // Set validity period (10 years)
    params.not_before = rcgen::date_time_ymd(2024, 1, 1);
    params.not_after = rcgen::date_time_ymd(2034, 1, 1);

    // Add Subject Alternative Names
    params.subject_alt_names = vec![
        SanType::DnsName(common_name.try_into().map_err(|e| {
            VoiceError::Tls(format!("Invalid common name: {}", e))
        })?),
        SanType::DnsName("localhost".try_into().unwrap()),
    ];

    // Not a CA - this is an end-entity certificate
    params.is_ca = IsCa::NoCa;

    // Generate key pair
    let key_pair = KeyPair::generate()
        .map_err(|e| VoiceError::Tls(format!("Failed to generate key pair: {}", e)))?;

    // Generate certificate
    let cert = params.self_signed(&key_pair)
        .map_err(|e| VoiceError::Tls(format!("Failed to generate certificate: {}", e)))?;

    let cert_pem = cert.pem();
    let key_pem = key_pair.serialize_pem();

    // Write to files
    fs::create_dir_all(cert_path.parent().unwrap_or(Path::new(".")))?;
    fs::write(cert_path, &cert_pem)?;
    fs::write(key_path, &key_pem)?;

    // Set restrictive permissions on key file (Unix only)
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(key_path)?.permissions();
        perms.set_mode(0o600);
        fs::set_permissions(key_path, perms)?;
    }

    Ok((cert_pem, key_pem))
}

/// Ensure server certificate exists
///
/// Returns (cert_path, key_path, fingerprint)
pub fn ensure_server_certificate(
    config: &Config,
    force_regenerate: bool,
) -> VoiceResult<(std::path::PathBuf, std::path::PathBuf, String)> {
    let certs_dir = config.certs_dir()?;
    let cert_path = certs_dir.join("server.crt");
    let key_path = certs_dir.join("server.key");

    if force_regenerate || !cert_path.exists() || !key_path.exists() {
        // Generate new certificate
        let device_name = config.device_name();
        let device_id = config.device_id_hex();
        generate_self_signed_cert(&cert_path, &key_path, &device_name, Some(&device_id))?;
    }

    // Compute fingerprint of certificate
    let fingerprint = compute_fingerprint(&cert_path)?;

    Ok((cert_path, key_path, fingerprint))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Sample self-signed certificate for testing
    const TEST_CERT_PEM: &str = r#"-----BEGIN CERTIFICATE-----
MIIBkTCB+wIJAKHBfpIgb5OJMA0GCSqGSIb3DQEBCwUAMBExDzANBgNVBAMMBnZv
aWNlMB4XDTIxMDEwMTAwMDAwMFoXDTMxMDEwMTAwMDAwMFowETEPMA0GA1UEAwwG
dm9pY2UwXDANBgkqhkiG9w0BAQEFAANLADBIAkEAyA8dF9VzOdqmGqKJLqJBNnvS
9BgqA8L5rqZxVQ8jFnqe5T0lKLqaA9xVtJvA8eHKjqhMvREXGVCrOqPeGhqZrwID
AQABo1MwUTAdBgNVHQ4EFgQUvCgqF3jqPmqTEYCTiEzxJqG6hwowHwYDVR0jBBgw
FoAUvCgqF3jqPmqTEYCTiEzxJqG6hwowDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG
9w0BAQsFAANBAJBF3J8cLqJBNnvS9BgqA8L5rqZxVQ8jFnqe5T0lKLqaA9xVtJvA
8eHKjqhMvREXGVCrOqPeGhqZrw8dF9VzOdqmGqI=
-----END CERTIFICATE-----"#;

    #[test]
    fn test_compute_fingerprint_from_pem() {
        let result = compute_fingerprint_from_pem(TEST_CERT_PEM.as_bytes());
        assert!(result.is_ok());
        let fingerprint = result.unwrap();
        assert!(fingerprint.starts_with("SHA256:"));
        // Fingerprint should have 64 hex chars separated by colons
        let parts: Vec<&str> = fingerprint[7..].split(':').collect();
        assert_eq!(parts.len(), 32);
    }

    #[test]
    fn test_fingerprint_format() {
        let der_data = vec![0u8; 32]; // Dummy data
        let fingerprint = compute_fingerprint_from_der(&der_data);
        assert!(fingerprint.starts_with("SHA256:"));
    }
}
