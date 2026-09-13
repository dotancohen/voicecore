//! The bucket, made and hardened (Stage 8, Stage 14).
//!
//! Everything the wizard does to a bucket lives here, so the desktop's
//! wizard, the command line and a test drive the same calls: make the
//! bucket private, block public access, turn default encryption on, refuse
//! anything not over TLS, set the lifecycle rules, and prove the key works
//! with a round trip. The wizard's words are in the desktop; the calls and
//! the meaning of a failure are here.

use std::time::Duration;

use s3::creds::Credentials;
use s3::serde_types::{AbortIncompleteMultipartUpload, And, BucketLifecycleConfiguration, Expiration, LifecycleFilter, LifecycleRule, Tag, Transition};
use s3::{Bucket, BucketConfiguration, Region};
use sha2::{Digest, Sha256};

use crate::sync_protocol::CheckRow;

/// The key and where the bucket lives.
#[derive(Debug, Clone)]
pub struct BucketKey {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub region: String,
    /// A custom endpoint for an S3-compatible service; None for Amazon
    pub endpoint: Option<String>,
}

/// The tag a purge puts on an object; the lifecycle rule deletes tagged
/// objects a day later (Stage 14).
pub const PURGED_TAG: (&str, &str) = ("voice-purged", "1");

/// Days before an object moves to the infrequent-access class (Stage 8).
pub const INFREQUENT_ACCESS_AFTER_DAYS: u32 = 30;
/// Days a purged object stays before the lifecycle rule deletes it.
pub const PURGED_EXPIRE_AFTER_DAYS: u32 = 1;
/// Days after which an incomplete multipart upload is abandoned.
pub const ABANDON_MULTIPART_AFTER_DAYS: i32 = 2;

/// The regions the wizard offers, nearest first once measured.
pub const REGIONS: &[&str] = &[
    "eu-central-1", "eu-west-1", "eu-west-2", "eu-west-3", "eu-north-1", "eu-south-1",
    "il-central-1", "me-south-1", "me-central-1",
    "us-east-1", "us-east-2", "us-west-1", "us-west-2", "ca-central-1", "sa-east-1",
    "ap-south-1", "ap-southeast-1", "ap-southeast-2", "ap-northeast-1", "ap-northeast-2", "af-south-1",
];

/// The policy the wizard shows for the console (Stage 14): buckets named
/// `voice-*` only; the object operations, tagging, multipart, the bucket's
/// own settings; and nothing that deletes.
pub fn policy_text() -> String {
    serde_json::to_string_pretty(&serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "VoiceBuckets",
                "Effect": "Allow",
                "Action": [
                    "s3:CreateBucket",
                    "s3:ListBucket",
                    "s3:GetBucketLocation",
                    "s3:ListBucketMultipartUploads",
                    "s3:PutLifecycleConfiguration",
                    "s3:GetLifecycleConfiguration",
                    "s3:PutBucketPublicAccessBlock",
                    "s3:GetBucketPublicAccessBlock",
                    "s3:PutEncryptionConfiguration",
                    "s3:GetEncryptionConfiguration",
                    "s3:PutBucketPolicy",
                    "s3:GetBucketPolicy"
                ],
                "Resource": "arn:aws:s3:::voice-*"
            },
            {
                "Sid": "VoiceObjects",
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject",
                    "s3:GetObject",
                    "s3:PutObjectTagging",
                    "s3:GetObjectTagging",
                    "s3:AbortMultipartUpload",
                    "s3:ListMultipartUploadParts"
                ],
                "Resource": "arn:aws:s3:::voice-*/*"
            }
        ]
    }))
    .unwrap_or_default()
}

/// The pasted key id, without the whitespace and the "Access key ID:"
/// label that come along when it is copied from the console.
pub fn clean_key_id(text: &str) -> String {
    clean_credential(text, &["access key id", "access key"])
}

/// The pasted secret, the same way.
pub fn clean_secret(text: &str) -> String {
    clean_credential(text, &["secret access key", "secret"])
}

fn clean_credential(text: &str, labels: &[&str]) -> String {
    let mut value = text.trim().to_string();
    let lower = value.to_lowercase();
    for label in labels {
        if lower.starts_with(label) {
            value = value[label.len()..].trim_start_matches([':', ' ', '\t', '=']).trim().to_string();
            break;
        }
    }
    value.split_whitespace().collect::<Vec<_>>().join("")
}

/// A bucket name nobody has yet, most likely: `voice-` and six characters.
pub fn suggest_bucket_name() -> String {
    let random = uuid::Uuid::new_v4().simple().to_string();
    format!("voice-{}", &random[..6])
}

/// Whether a name may be a bucket's: 3 to 63 characters, lowercase letters,
/// digits and hyphens, starting and ending with a letter or digit, and
/// starting with `voice-` so the key may touch it.
pub fn bucket_name_allowed(name: &str) -> Result<(), String> {
    if !name.starts_with("voice-") {
        return Err("The name must start with voice-, the only buckets the key may touch".to_string());
    }
    if name.len() < 3 || name.len() > 63 {
        return Err("A bucket name is 3 to 63 characters".to_string());
    }
    if !name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-') {
        return Err("A bucket name has lowercase letters, digits and hyphens only".to_string());
    }
    if name.ends_with('-') {
        return Err("A bucket name cannot end with a hyphen".to_string());
    }
    Ok(())
}

/// What a failure means, in words (Stage 8): the code the service sent, or
/// the shape of the error, turned into what to do.
pub fn explain_error(text: &str) -> String {
    let lower = text.to_lowercase();
    if lower.contains("signaturedoesnotmatch") {
        return "The secret is wrong, or has a space on the end.".to_string();
    }
    if lower.contains("invalidaccesskeyid") {
        return "The key id is wrong; it starts with AKIA and is 20 characters.".to_string();
    }
    if lower.contains("permanentredirect") || lower.contains("http 301") || lower.contains("authorizationheadermalformed") {
        return "The bucket is in another region.".to_string();
    }
    if lower.contains("bucketalreadyexists") || lower.contains("bucketalreadyownedbyyou") {
        return "That bucket name is taken; try another.".to_string();
    }
    if lower.contains("accessdenied") {
        return "The key is not allowed to do this: paste the policy text on the user in the console, then try again.".to_string();
    }
    if lower.contains("http 403") || lower.contains("non 2**") {
        return "The service refused the key: the secret may be wrong (or have a space on the end), or the policy is not on the user yet.".to_string();
    }
    if lower.contains("nosuchbucket") {
        return "There is no bucket of that name in this region.".to_string();
    }
    if lower.contains("dns") || lower.contains("failed to lookup") || lower.contains("name or service not known") || lower.contains("nodename nor servname") {
        return "The address could not be found: check the endpoint.".to_string();
    }
    if lower.contains("certificate") {
        return "The connection's certificate was not accepted: check the endpoint, and that it is https.".to_string();
    }
    if lower.contains("timed out") || lower.contains("connection refused") || lower.contains("network is unreachable") {
        return "The service did not answer: check the connection.".to_string();
    }
    text.to_string()
}

/// A refusal explained with the endpoint in mind: over `http://`, a bucket the
/// wizard hardened refuses every request (its policy allows only TLS), which
/// otherwise reads exactly like a wrong key. Anything else as [`explain_error`].
pub fn explain_refusal(key: &BucketKey, text: &str) -> String {
    let lower = text.to_lowercase();
    // A cause the service names (a wrong secret, an unknown key id, no such
    // bucket, another region, a taken name) is that cause, over any address
    let named = ["signaturedoesnotmatch", "invalidaccesskeyid", "nosuchbucket", "permanentredirect", "authorizationheadermalformed", "bucketalreadyexists", "bucketalreadyownedbyyou"]
        .iter()
        .any(|code| lower.contains(code));
    let refused = !named && (lower.contains("accessdenied") || lower.contains("http 403") || lower.contains("non 2**"));
    let plain = key.endpoint.as_deref().is_some_and(|e| e.trim().to_lowercase().starts_with("http://"));
    if refused && plain {
        return "The service refused the request, and the endpoint is http://: a bucket hardened by the wizard accepts only https:// connections, so use the https:// address. If the address is https:// already, the key may be wrong or lack the policy.".to_string();
    }
    explain_error(text)
}

/// The region the wizard proposes: the one whose S3 endpoint answers a TCP
/// connection fastest, or None when none answers within two seconds.
pub async fn nearest_region(regions: &[&str]) -> Option<String> {
    let mut best: Option<(String, Duration)> = None;
    for region in regions {
        let host = format!("s3.{}.amazonaws.com:443", region);
        let started = std::time::Instant::now();
        let connected = tokio::time::timeout(Duration::from_secs(2), tokio::net::TcpStream::connect(&host)).await;
        if let Ok(Ok(_)) = connected {
            let took = started.elapsed();
            if best.as_ref().map(|(_, t)| took < *t).unwrap_or(true) {
                best = Some((region.to_string(), took));
            }
        }
    }
    best.map(|(r, _)| r)
}

fn region_of(key: &BucketKey) -> Result<Region, String> {
    match &key.endpoint {
        Some(endpoint) => Ok(Region::Custom { region: key.region.clone(), endpoint: endpoint.clone() }),
        None => key.region.parse().map_err(|e| format!("{} is not a region: {}", key.region, e)),
    }
}

fn credentials_of(key: &BucketKey) -> Result<Credentials, String> {
    Credentials::new(Some(&key.access_key_id), Some(&key.secret_access_key), None, None, None).map_err(|e| e.to_string())
}

fn bucket_of(key: &BucketKey, name: &str) -> Result<Box<Bucket>, String> {
    let mut bucket = Bucket::new(name, region_of(key)?, credentials_of(key)?).map_err(|e| e.to_string())?;
    if key.endpoint.is_some() {
        bucket = bucket.with_path_style();
    }
    bucket.with_request_timeout(Duration::from_secs(60)).map_err(|e| e.to_string())
}

/// Make the bucket, private (Stage 8 step 4). An endpoint other than
/// Amazon's is used as given.
pub async fn create_bucket(key: &BucketKey, name: &str) -> Result<(), String> {
    bucket_name_allowed(name)?;
    if key.endpoint.is_some() && key.region.trim() == "us-east-1" {
        // Amazon's first region takes no location constraint. The library
        // leaves it out only for the region it knows by name, and a region
        // behind an endpoint is one it does not know, so it would send
        // "us-east-1" and be refused: the bucket is made by a signed PUT
        // with no body instead
        let url = bucket_url(key, name);
        let (status, body) = signed(key, "PUT", &url, b"", None).await?;
        return if (200..300).contains(&status) {
            Ok(())
        } else {
            Err(explain_refusal(key, &format!("HTTP {}: {}", status, body)))
        };
    }
    let region = region_of(key)?;
    let credentials = credentials_of(key)?;
    let config = BucketConfiguration::private();
    let response = if key.endpoint.is_some() {
        Bucket::create_with_path_style(name, region, credentials, config).await
    } else {
        Bucket::create(name, region, credentials, config).await
    }
    .map_err(|e| explain_refusal(key, &e.to_string()))?;
    if response.success() {
        Ok(())
    } else {
        Err(explain_refusal(key, &format!("HTTP {}: {}", response.response_code, response.response_text)))
    }
}

/// Whether a bucket of this name answers this key: Some(true) when it
/// exists and is ours, Some(false) when there is none, None when the answer
/// was neither (taken by someone else, or a refusal).
pub async fn bucket_exists(key: &BucketKey, name: &str) -> Result<bool, String> {
    let url = format!("{}/?location", bucket_url(key, name));
    match signed(key, "GET", &url, b"", None).await? {
        (status, _) if (200..300).contains(&status) => Ok(true),
        (404, _) => Ok(false),
        (403, body) if body.contains("AccessDenied") => Err("That bucket name is taken by someone else, or the key may not read it.".to_string()),
        (status, body) => Err(explain_refusal(key, &format!("HTTP {}: {}", status, body))),
    }
}

/// The three lifecycle rules (Stage 8, Stage 13, Stage 14): everything to
/// the infrequent-access class after thirty days; purged objects gone after
/// a day; abandoned multipart uploads gone after two.
pub fn lifecycle_rules() -> BucketLifecycleConfiguration {
    BucketLifecycleConfiguration::new(vec![
        LifecycleRule::builder("Enabled")
            .id("voice-infrequent-access")
            .filter(LifecycleFilter::new(None, None, None, Some(String::new()), None))
            .transition(vec![Transition { date: None, days: Some(INFREQUENT_ACCESS_AFTER_DAYS), storage_class: Some("STANDARD_IA".to_string()) }])
            .build(),
        LifecycleRule::builder("Enabled")
            .id("voice-purged")
            .filter(LifecycleFilter::new(None, None, None, None, Some(Tag::new(PURGED_TAG.0, PURGED_TAG.1))))
            .expiration(Expiration { date: None, days: Some(PURGED_EXPIRE_AFTER_DAYS), expired_object_delete_marker: None })
            .build(),
        LifecycleRule::builder("Enabled")
            .id("voice-abandoned-uploads")
            .filter(LifecycleFilter::new(None, None, None, Some(String::new()), None))
            .abort_incomplete_multipart_upload(AbortIncompleteMultipartUpload { days_after_initiation: Some(ABANDON_MULTIPART_AFTER_DAYS) })
            .build(),
    ])
}

/// Set the lifecycle rules on the bucket.
pub async fn set_lifecycle(key: &BucketKey, name: &str) -> Result<(), String> {
    let bucket = bucket_of(key, name)?;
    let _ = And::new(None, None, None, None); // the type is part of the filter's shape
    let response = bucket.put_bucket_lifecycle(lifecycle_rules()).await.map_err(|e| explain_refusal(key, &e.to_string()))?;
    answered(Some(key), &response, "Lifecycle")
}

/// Write a small object, read it back, compare, and tag it purged so the
/// lifecycle rule removes it (the key cannot delete, Stage 14). Returns the
/// object's key.
pub async fn round_trip(key: &BucketKey, name: &str, prefix: Option<&str>) -> Result<String, String> {
    let bucket = bucket_of(key, name)?;
    let object = format!("{}voice-setup-check-{}.txt", prefix.map(|p| p.trim_end_matches('/').to_string() + "/").unwrap_or_default(), chrono::Utc::now().timestamp());
    let content = format!("Voice checked this bucket at {}", chrono::Utc::now().to_rfc3339());
    // The write is the core's own signed request, which reads the whole
    // answer: the library keeps no body of a refused upload, and without the
    // service's code a wrong secret cannot be told from a refusal by policy
    let object_url = format!("{}/{}", bucket_url(key, name), uri_encode(&object, true));
    let (status, body) = signed(key, "PUT", &object_url, content.as_bytes(), Some("text/plain")).await.map_err(|e| format!("Write: {}", e))?;
    if !(200..300).contains(&status) {
        return Err(format!("Write: {}", explain_refusal(key, &format!("HTTP {}: {}", status, body))));
    }
    let read = bucket.get_object(&object).await.map_err(|e| format!("Read back: {}", explain_refusal(key, &e.to_string())))?;
    answered(Some(key), &read, "Read back")?;
    if read.as_slice() != content.as_bytes() {
        return Err("What was read back is not what was written".to_string());
    }
    let tagged = bucket.put_object_tagging(&object, &[PURGED_TAG]).await.map_err(|e| format!("Tag: {}", explain_refusal(key, &e.to_string())))?;
    answered(Some(key), &tagged, "Tag")?;
    Ok(object)
}

/// A refusal the library hands back as an answer rather than an error,
/// explained with the endpoint in mind when the key is known.
fn answered(key: Option<&BucketKey>, response: &s3::request::ResponseData, what: &str) -> Result<(), String> {
    let status = response.status_code();
    if (200..300).contains(&status) {
        Ok(())
    } else {
        let text = format!("HTTP {}: {}", status, response.as_str().unwrap_or_default());
        Err(format!("{}: {}", what, key.map_or_else(|| explain_error(&text), |k| explain_refusal(k, &text))))
    }
}

/// Tag an object purged: the lifecycle rule deletes it a day later.
pub async fn tag_purged(bucket: &Bucket, key: &str) -> Result<(), String> {
    let response = bucket.put_object_tagging(key, &[PURGED_TAG]).await.map_err(|e| explain_error(&e.to_string()))?;
    answered(None, &response, "Tag")
}

// ---------------------------------------------------------------------------
// The bucket's own settings: three requests rust-s3 has no call for, signed
// here (AWS signature version 4) and sent with reqwest.
// ---------------------------------------------------------------------------

const PUBLIC_ACCESS_BLOCK: &str = r#"<PublicAccessBlockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><BlockPublicAcls>true</BlockPublicAcls><IgnorePublicAcls>true</IgnorePublicAcls><BlockPublicPolicy>true</BlockPublicPolicy><RestrictPublicBuckets>true</RestrictPublicBuckets></PublicAccessBlockConfiguration>"#;

const ENCRYPTION: &str = r#"<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"#;

/// The bucket policy that refuses any request not made over TLS.
pub fn tls_only_policy(bucket: &str) -> String {
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "VoiceTlsOnly",
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:*",
            "Resource": [format!("arn:aws:s3:::{}", bucket), format!("arn:aws:s3:::{}/*", bucket)],
            "Condition": {"Bool": {"aws:SecureTransport": "false"}}
        }]
    })
    .to_string()
}

/// Where a bucket's own settings are addressed: virtual-hosted on Amazon,
/// path style on a custom endpoint.
pub(crate) fn bucket_url(key: &BucketKey, name: &str) -> String {
    match &key.endpoint {
        Some(endpoint) => format!("{}/{}", endpoint.trim_end_matches('/'), name),
        None => format!("https://{}.s3.{}.amazonaws.com", name, key.region),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn sha256_hex(data: &[u8]) -> String {
    hex(&Sha256::digest(data))
}

/// HMAC-SHA256, from the hash alone.
fn hmac_sha256(key: &[u8], message: &[u8]) -> Vec<u8> {
    const BLOCK: usize = 64;
    let mut key_block = [0u8; BLOCK];
    if key.len() > BLOCK {
        key_block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        key_block[..key.len()].copy_from_slice(key);
    }
    let mut inner = Sha256::new();
    inner.update(key_block.iter().map(|b| b ^ 0x36).collect::<Vec<u8>>());
    inner.update(message);
    let inner_hash = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(key_block.iter().map(|b| b ^ 0x5c).collect::<Vec<u8>>());
    outer.update(inner_hash);
    outer.finalize().to_vec()
}

pub(crate) fn uri_encode(text: &str, keep_slash: bool) -> String {
    let mut out = String::new();
    for byte in text.bytes() {
        let c = byte as char;
        if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' || c == '~' || (keep_slash && c == '/') {
            out.push(c);
        } else {
            out.push_str(&format!("%{:02X}", byte));
        }
    }
    out
}

/// A request signed with AWS signature version 4 (`s3` service). The
/// headers given are signed, plus `host`, `x-amz-date` and
/// `x-amz-content-sha256`. Returns the headers to send, the given ones
/// included.
pub fn sign_request(
    method: &str,
    url: &str,
    region: &str,
    access_key_id: &str,
    secret_access_key: &str,
    payload: &[u8],
    extra_headers: &[(&str, &str)],
    at: chrono::DateTime<chrono::Utc>,
) -> Vec<(String, String)> {
    let parsed = url::Url::parse(url).expect("a URL");
    let host = match parsed.port() {
        Some(port) => format!("{}:{}", parsed.host_str().unwrap_or_default(), port),
        None => parsed.host_str().unwrap_or_default().to_string(),
    };
    let amz_date = at.format("%Y%m%dT%H%M%SZ").to_string();
    let date = at.format("%Y%m%d").to_string();
    let payload_hash = sha256_hex(payload);

    let mut headers: Vec<(String, String)> = extra_headers.iter().map(|(k, v)| (k.to_lowercase(), v.trim().to_string())).collect();
    headers.push(("host".to_string(), host));
    headers.push(("x-amz-content-sha256".to_string(), payload_hash.clone()));
    headers.push(("x-amz-date".to_string(), amz_date.clone()));
    headers.sort();

    let canonical_uri = if parsed.path().is_empty() { "/".to_string() } else { uri_encode(parsed.path(), true) };
    let mut query: Vec<(String, String)> = parsed.query_pairs().map(|(k, v)| (uri_encode(&k, false), uri_encode(&v, false))).collect();
    if query.is_empty() {
        if let Some(raw) = parsed.query() {
            // A bare sub-resource such as `?lifecycle`
            for part in raw.split('&').filter(|p| !p.is_empty()) {
                query.push((uri_encode(part, false), String::new()));
            }
        }
    }
    query.sort();
    let canonical_query = query.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join("&");
    let canonical_headers = headers.iter().map(|(k, v)| format!("{}:{}\n", k, v)).collect::<String>();
    let signed_headers = headers.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>().join(";");
    let canonical_request = format!("{}\n{}\n{}\n{}\n{}\n{}", method, canonical_uri, canonical_query, canonical_headers, signed_headers, payload_hash);

    let scope = format!("{}/{}/s3/aws4_request", date, region);
    let string_to_sign = format!("AWS4-HMAC-SHA256\n{}\n{}\n{}", amz_date, scope, sha256_hex(canonical_request.as_bytes()));
    let k_date = hmac_sha256(format!("AWS4{}", secret_access_key).as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, b"s3");
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature = hex(&hmac_sha256(&k_signing, string_to_sign.as_bytes()));
    let authorization = format!("AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}", access_key_id, scope, signed_headers, signature);

    let mut out: Vec<(String, String)> = headers.into_iter().filter(|(k, _)| k != "host").collect();
    out.push(("authorization".to_string(), authorization));
    out
}

async fn signed(key: &BucketKey, method: &str, url: &str, payload: &[u8], content_type: Option<&str>) -> Result<(u16, String), String> {
    let answer = send_signed(key, method, url, payload, content_type, Duration::from_secs(30)).await?;
    Ok((answer.status, answer.body))
}

/// A link that moves no byte for this long is dead (FILE-14): the same
/// thirty seconds as a transfer between devices.
pub const STALL_TIMEOUT: Duration = Duration::from_secs(30);

/// A signed GET whose body is read as it arrives: no overall timeout, because
/// a long recording over a slow link takes as long as it takes, but a
/// connection that moves nothing for [`STALL_TIMEOUT`] ends the download.
pub(crate) async fn get_signed_stream(key: &BucketKey, url: &str) -> Result<reqwest::Response, String> {
    let headers = sign_request("GET", url, &key.region, &key.access_key_id, &key.secret_access_key, b"", &[], chrono::Utc::now());
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(STALL_TIMEOUT)
        .build()
        .map_err(|e| e.to_string())?;
    let mut request = client.get(url);
    for (k, v) in headers {
        request = request.header(k, v);
    }
    request.send().await.map_err(|e| explain_error(&e.to_string()))
}

/// A signed request whose body is counted as the connection takes it, ended
/// by [`crate::transfer::stall_of_upload`] when it stops moving, and by
/// `backstop` in any case: a part over a slow uplink is not a dead link.
pub(crate) async fn send_signed_watched(key: &BucketKey, method: &str, url: &str, payload: Vec<u8>, content_type: Option<&str>, backstop: Duration) -> Result<SignedAnswer, String> {
    use futures_util::StreamExt;
    let mut extra: Vec<(&str, &str)> = Vec::new();
    if let Some(ct) = content_type {
        extra.push(("content-type", ct));
    }
    let headers = sign_request(method, url, &key.region, &key.access_key_id, &key.secret_access_key, &payload, &extra, chrono::Utc::now());
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(backstop)
        .build()
        .map_err(|e| e.to_string())?;
    let length = payload.len() as u64;
    let method = reqwest::Method::from_bytes(method.as_bytes()).map_err(|e| e.to_string())?;
    let mut request = client.request(method, url).header("content-length", length.to_string());
    for (k, v) in headers {
        request = request.header(k, v);
    }
    let moved = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let counter = moved.clone();
    let body = tokio_util::io::ReaderStream::with_capacity(std::io::Cursor::new(payload), 64 * 1024).inspect(move |chunk| {
        if let Ok(chunk) = chunk {
            counter.fetch_add(chunk.len() as u64, std::sync::atomic::Ordering::SeqCst);
        }
    });
    let stalled = crate::transfer::stall_of_upload(moved, length);
    let response = tokio::select! {
        sent = request.body(reqwest::Body::wrap_stream(body)).send() => sent.map_err(|e| explain_error(&e.to_string()))?,
        why = stalled => return Err(why),
    };
    let status = response.status().as_u16();
    let etag = response.headers().get("etag").and_then(|v| v.to_str().ok()).map(|v| v.to_string());
    let body = response.text().await.unwrap_or_default();
    Ok(SignedAnswer { status, body, etag })
}

/// What a signed request came back with.
pub(crate) struct SignedAnswer {
    pub status: u16,
    pub body: String,
    /// The object's or part's tag, as the service gave it
    pub etag: Option<String>,
}

/// One request signed with the key (SigV4), sent, and read whole.
pub(crate) async fn send_signed(key: &BucketKey, method: &str, url: &str, payload: &[u8], content_type: Option<&str>, timeout: Duration) -> Result<SignedAnswer, String> {
    let mut extra: Vec<(&str, &str)> = Vec::new();
    if let Some(ct) = content_type {
        extra.push(("content-type", ct));
    }
    let headers = sign_request(method, url, &key.region, &key.access_key_id, &key.secret_access_key, payload, &extra, chrono::Utc::now());
    let client = reqwest::Client::builder().timeout(timeout).build().map_err(|e| e.to_string())?;
    let mut request = match method {
        "PUT" => client.put(url),
        "GET" => client.get(url),
        "POST" => client.post(url),
        "DELETE" => client.delete(url),
        _ => client.request(reqwest::Method::from_bytes(method.as_bytes()).map_err(|e| e.to_string())?, url),
    };
    for (k, v) in headers {
        request = request.header(k, v);
    }
    let response = request.body(payload.to_vec()).send().await.map_err(|e| explain_error(&e.to_string()))?;
    let status = response.status().as_u16();
    let etag = response.headers().get("etag").and_then(|v| v.to_str().ok()).map(|v| v.to_string());
    let body = response.text().await.unwrap_or_default();
    Ok(SignedAnswer { status, body, etag })
}

fn row(name: &str, passed: bool, detail: impl Into<String>) -> CheckRow {
    CheckRow { name: name.to_string(), passed, detail: detail.into(), code: String::new() }
}

/// Harden a bucket (Stage 14): block public access, default encryption, a
/// policy that refuses anything not over TLS; each verified after it is
/// set, and reported as a line.
pub async fn harden_bucket(key: &BucketKey, name: &str) -> Vec<CheckRow> {
    let base = bucket_url(key, name);
    let mut rows = Vec::new();
    let steps: [(&str, &str, String, &str, &str); 3] = [
        ("Public access blocked", "publicAccessBlock", PUBLIC_ACCESS_BLOCK.to_string(), "application/xml", "<BlockPublicAcls>true</BlockPublicAcls>"),
        ("Encrypted at rest", "encryption", ENCRYPTION.to_string(), "application/xml", "AES256"),
        ("TLS only", "policy", tls_only_policy(name), "application/json", "aws:SecureTransport"),
    ];
    for (label, sub_resource, body, content_type, proof) in steps {
        let url = format!("{}/?{}", base, sub_resource);
        match signed(key, "PUT", &url, body.as_bytes(), Some(content_type)).await {
            Ok((status, text)) if (200..300).contains(&status) => {
                // Verified: read it back
                match signed(key, "GET", &url, b"", None).await {
                    Ok((200, read)) if read.contains(proof) => rows.push(row(label, true, "Set and verified")),
                    Ok((status, read)) => rows.push(row(label, false, format!("Set, but reading it back gave HTTP {}: {}", status, explain_refusal(key, &read)))),
                    Err(e) => rows.push(row(label, false, format!("Set, but could not be read back: {}", e))),
                }
            }
            Ok((status, text)) => rows.push(row(label, false, format!("HTTP {}: {}", status, explain_refusal(key, &text)))),
            Err(e) => rows.push(row(label, false, e)),
        }
    }
    rows
}

/// The bucket as it is (Stage 8 "Test everything", the checklist): whether
/// it answers this key, the round trip, and whether each hardening setting
/// and the lifecycle rules are in place.
pub async fn check_bucket(key: &BucketKey, name: &str, prefix: Option<&str>) -> Vec<CheckRow> {
    let mut rows = Vec::new();
    match bucket_exists(key, name).await {
        Ok(true) => rows.push(row("Bucket", true, format!("{} answers this key", name))),
        Ok(false) => {
            rows.push(row("Bucket", false, format!("There is no bucket {} in {}", name, key.region)));
            return rows;
        }
        Err(e) => {
            rows.push(row("Bucket", false, e));
            return rows;
        }
    }
    match round_trip(key, name, prefix).await {
        Ok(_) => rows.push(row("Round trip", true, "A small object was written, read back and tagged purged")),
        Err(e) => rows.push(row("Round trip", false, e)),
    }
    let base = bucket_url(key, name);
    for (label, sub_resource, proof) in [
        ("Public access blocked", "publicAccessBlock", "<BlockPublicAcls>true</BlockPublicAcls>"),
        ("Encrypted at rest", "encryption", "AES256"),
        ("TLS only", "policy", "aws:SecureTransport"),
        ("Lifecycle rules", "lifecycle", "voice-purged"),
    ] {
        match signed(key, "GET", &format!("{}/?{}", base, sub_resource), b"", None).await {
            Ok((200, text)) if text.contains(proof) => rows.push(row(label, true, "In place")),
            Ok((200, _)) => rows.push(row(label, false, "Set, but not as the wizard sets it")),
            Ok((404, _)) => rows.push(row(label, false, "Not set; run the wizard's hardening again")),
            Ok((status, text)) => rows.push(row(label, false, format!("HTTP {}: {}", status, explain_refusal(key, &text)))),
            Err(e) => rows.push(row(label, false, e)),
        }
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_policy_lets_the_key_touch_voice_buckets_only_and_never_delete() {
        let text = policy_text();
        assert!(text.contains("arn:aws:s3:::voice-*"));
        assert!(text.contains("s3:CreateBucket") && text.contains("s3:PutObject") && text.contains("s3:PutObjectTagging"));
        assert!(!text.contains("Delete"), "{}", text);
        assert!(!text.contains("\"s3:*\""));
        let policy = tls_only_policy("voice-abc123");
        assert!(policy.contains("aws:SecureTransport") && policy.contains("\"Effect\":\"Deny\""));
    }

    #[test]
    fn a_pasted_key_loses_its_label_and_whitespace() {
        assert_eq!(clean_key_id("  Access key ID: AKIAIOSFODNN7EXAMPLE \n"), "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(clean_secret("Secret access key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY  "), "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        assert_eq!(clean_secret("wJalr XUtnFEMI"), "wJalrXUtnFEMI", "a space in the middle is a paste accident too");
    }

    #[test]
    fn a_suggested_name_is_allowed_and_a_wrong_one_is_told_why() {
        let name = suggest_bucket_name();
        assert!(name.starts_with("voice-") && name.len() == 12);
        assert!(bucket_name_allowed(&name).is_ok());
        assert!(bucket_name_allowed("notes").unwrap_err().contains("voice-"));
        assert!(bucket_name_allowed("voice-Big").unwrap_err().contains("lowercase"));
        assert!(bucket_name_allowed("voice-").unwrap_err().contains("hyphen"));
    }

    /// Over http://, a refusal names the hardened bucket's TLS-only policy
    /// before it blames the key; over https:// it is the key as before.
    #[test]
    fn a_refusal_over_plain_http_names_the_tls_only_policy() {
        let plain = BucketKey { access_key_id: "AKIAIOSFODNN7EXAMPLE".into(), secret_access_key: "s".into(), region: "eu-central-1".into(), endpoint: Some("http://127.0.0.1:9000".into()) };
        let tls = BucketKey { endpoint: Some("https://s3.example.com".into()), ..plain.clone() };
        let amazon = BucketKey { endpoint: None, ..plain.clone() };
        let refused = "Got HTTP 403 with content '<Error><Code>AccessDenied</Code></Error>'";
        assert!(explain_refusal(&plain, refused).contains("accepts only https://"), "{}", explain_refusal(&plain, refused));
        assert!(explain_refusal(&plain, "HTTP 403: ").contains("https://"));
        assert_eq!(explain_refusal(&tls, refused), explain_error(refused));
        assert_eq!(explain_refusal(&amazon, refused), explain_error(refused));
        // As the service sends them: the status, then the code that names the cause
        assert_eq!(explain_refusal(&plain, "HTTP 403: <Error><Code>SignatureDoesNotMatch</Code></Error>"), "The secret is wrong, or has a space on the end.", "a wrong secret is not a refusal by policy");
        assert_eq!(explain_refusal(&plain, "Got HTTP 403 with content '<Code>InvalidAccessKeyId</Code>'"), "The key id is wrong; it starts with AKIA and is 20 characters.");
        assert_eq!(explain_refusal(&plain, "HTTP 404: <Code>NoSuchBucket</Code>"), "There is no bucket of that name in this region.");
    }

    #[test]
    fn failures_are_explained_in_words() {
        assert_eq!(explain_error("HTTP 403: <Code>SignatureDoesNotMatch</Code>"), "The secret is wrong, or has a space on the end.");
        assert!(explain_error("Got HTTP 403 with content ''").contains("secret may be wrong"));
        assert!(explain_error("<Code>AccessDenied</Code>").contains("policy text"));
        assert_eq!(explain_error("<Code>PermanentRedirect</Code>"), "The bucket is in another region.");
        assert_eq!(explain_error("error sending request: failed to lookup address information"), "The address could not be found: check the endpoint.");
        assert_eq!(explain_error("<Code>BucketAlreadyExists</Code>"), "That bucket name is taken; try another.");
        assert_eq!(explain_error("something else"), "something else");
    }

    /// The worked example of the AWS documentation ("Signature Calculations
    /// for the Authorization Header", GET object): the signature it prints.
    #[test]
    fn the_signature_matches_the_documented_example() {
        let at = chrono::DateTime::parse_from_rfc3339("2013-05-24T00:00:00Z").unwrap().with_timezone(&chrono::Utc);
        let headers = sign_request(
            "GET",
            "https://examplebucket.s3.amazonaws.com/test.txt",
            "us-east-1",
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            b"",
            &[("Range", "bytes=0-9")],
            at,
        );
        let authorization = headers.iter().find(|(k, _)| k == "authorization").map(|(_, v)| v.clone()).unwrap();
        assert_eq!(
            authorization,
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    #[test]
    fn the_lifecycle_has_the_three_rules() {
        let rules = lifecycle_rules().rules;
        assert_eq!(rules.iter().map(|r| r.id.clone().unwrap()).collect::<Vec<_>>(), vec!["voice-infrequent-access", "voice-purged", "voice-abandoned-uploads"]);
        assert_eq!(rules[0].transition.as_ref().unwrap()[0].days, Some(30));
        assert_eq!(rules[1].expiration.as_ref().unwrap().days, Some(1));
        assert_eq!(rules[1].filter.as_ref().unwrap().tag.as_ref().unwrap().key, "voice-purged");
        assert_eq!(rules[2].abort_incomplete_multipart_upload.as_ref().unwrap().days_after_initiation, Some(2));
    }

    #[test]
    fn the_bucket_url_is_virtual_hosted_on_amazon_and_path_style_elsewhere() {
        let amazon = BucketKey { access_key_id: "a".into(), secret_access_key: "b".into(), region: "eu-central-1".into(), endpoint: None };
        assert_eq!(bucket_url(&amazon, "voice-x"), "https://voice-x.s3.eu-central-1.amazonaws.com");
        let other = BucketKey { endpoint: Some("http://127.0.0.1:9000/".into()), ..amazon };
        assert_eq!(bucket_url(&other, "voice-x"), "http://127.0.0.1:9000/voice-x");
    }
}
