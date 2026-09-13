//! Moving one recording's bytes between two instances (FILE-12, FILE-13):
//! streamed, never held in memory, resumable, and verified by hash.
//!
//! Both directions write to `<final>.part` beside the final name and rename
//! when the bytes are all there and the hash agrees. A transfer that stops
//! leaves the part behind, and the next one continues from its length.

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::error::{VoiceError, VoiceResult};

/// Bytes read at a time when hashing or streaming a file.
pub const CHUNK: usize = 1 << 16;

/// Free space to keep beyond the file being written.
pub const FREE_SPACE_MARGIN: u64 = 64 * 1024 * 1024;

/// The part file a transfer of `path` accumulates into.
pub fn part_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().map(|n| n.to_os_string()).unwrap_or_default();
    name.push(".part");
    path.with_file_name(name)
}

/// How many bytes of `path` a part file already holds.
pub fn part_len(path: &Path) -> u64 {
    std::fs::metadata(part_path(path)).map(|m| m.len()).unwrap_or(0)
}

/// Hex SHA-256 of a whole file, read in chunks.
pub fn file_sha256(path: &Path) -> VoiceResult<String> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; CHUNK];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().iter().map(|b| format!("{:02x}", b)).collect())
}

/// The bytes free on the disk that holds `dir` (its parent when it does
/// not exist yet), or 0 when the answer cannot be had.
pub fn free_space(dir: &Path) -> u64 {
    let probe = if dir.exists() { dir.to_path_buf() } else { dir.parent().map(Path::to_path_buf).unwrap_or_else(|| PathBuf::from(".")) };
    fs4::available_space(&probe).unwrap_or(0)
}

/// Refuse to write `needed` bytes into `dir` when the disk would be left
/// with less than the margin (FILE-14). The sentence names both numbers.
pub fn check_free_space(dir: &Path, needed: u64) -> VoiceResult<()> {
    let probe = if dir.exists() { dir.to_path_buf() } else { dir.parent().map(Path::to_path_buf).unwrap_or_else(|| PathBuf::from(".")) };
    let available = match fs4::available_space(&probe) {
        Ok(a) => a,
        Err(_) => return Ok(()),
    };
    if available < needed.saturating_add(FREE_SPACE_MARGIN) {
        return Err(VoiceError::Io(std::io::Error::new(
            std::io::ErrorKind::StorageFull,
            format!(
                "Not enough free space: {} MB needed (plus a {} MB margin), {} MB free on {}",
                needed / 1024 / 1024,
                FREE_SPACE_MARGIN / 1024 / 1024,
                available / 1024 / 1024,
                probe.display()
            ),
        )));
    }
    Ok(())
}

/// Finish a transfer: the part must be `total` bytes and, when a hash was
/// announced, hash to it; then it becomes the file. A part that does not
/// match is deleted, so the next attempt starts clean.
pub fn complete(path: &Path, total: u64, expected_sha256: Option<&str>) -> VoiceResult<()> {
    let part = part_path(path);
    let len = std::fs::metadata(&part).map(|m| m.len()).unwrap_or(0);
    if len != total {
        return Err(VoiceError::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!("The transfer of {} stopped at {} of {} bytes", path.display(), len, total),
        )));
    }
    if let Some(expected) = expected_sha256.filter(|e| !e.is_empty()) {
        let actual = file_sha256(&part)?;
        if !actual.eq_ignore_ascii_case(expected) {
            let _ = std::fs::remove_file(&part);
            return Err(VoiceError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("The transfer of {} arrived corrupted (hash mismatch); the part was discarded", path.display()),
            )));
        }
    }
    {
        let file = std::fs::File::open(&part)?;
        file.sync_all()?;
    }
    std::fs::rename(&part, path)?;
    Ok(())
}

/// Parse `Range: bytes=N-` (only the open-ended form a resume needs).
pub fn parse_range_start(header: &str) -> Option<u64> {
    let rest = header.trim().strip_prefix("bytes=")?;
    let (start, end) = rest.split_once('-')?;
    if !end.is_empty() {
        return None;
    }
    start.trim().parse().ok()
}

/// Parse `Content-Range: bytes N-M/total` into (start, total).
pub fn parse_content_range(header: &str) -> Option<(u64, u64)> {
    let rest = header.trim().strip_prefix("bytes ")?;
    let (range, total) = rest.split_once('/')?;
    let (start, _end) = range.split_once('-')?;
    Some((start.trim().parse().ok()?, total.trim().parse().ok()?))
}

/// An upload is over when no byte of its body has moved for this long (FILE-14).
pub const SEND_STALL: std::time::Duration = std::time::Duration::from_secs(30);

/// The other side verifies a whole file before it answers; this long after
/// the last byte without an answer, the upload is over. A phone hashes two
/// gigabytes in well under a minute. On a dead link the last byte "goes" long
/// before it arrives, into the socket buffers, so this wait is what a frozen
/// send costs per try.
pub const ANSWER_AFTER_LAST_BYTE: std::time::Duration = std::time::Duration::from_secs(60);

/// Watch an upload's body as it is taken by the connection, and return why
/// it stalled: nothing moved for [`SEND_STALL`] before the last byte, or no
/// answer for [`ANSWER_AFTER_LAST_BYTE`] after it. Never returns while bytes
/// keep moving, however slowly: a read timeout cannot tell a slow upload from
/// a dead one, because the connection reads nothing while the body goes out.
pub async fn stall_of_upload(moved: std::sync::Arc<std::sync::atomic::AtomicU64>, body_len: u64) -> String {
    use std::sync::atomic::Ordering;
    let mut last = moved.load(Ordering::SeqCst);
    let mut since = tokio::time::Instant::now();
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let now = moved.load(Ordering::SeqCst);
        if now != last {
            last = now;
            since = tokio::time::Instant::now();
            continue;
        }
        let all_sent = now >= body_len;
        let limit = if all_sent { ANSWER_AFTER_LAST_BYTE } else { SEND_STALL };
        if since.elapsed() >= limit {
            return if all_sent {
                format!("no answer for {} seconds after the last byte", limit.as_secs())
            } else {
                format!("nothing moved for {} seconds after {} of {} bytes", limit.as_secs(), now, body_len)
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_part_lives_beside_the_file_and_completion_checks_length_and_hash() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("0123.ogg");
        assert_eq!(part_path(&path), dir.path().join("0123.ogg.part"));
        assert_eq!(part_len(&path), 0);

        std::fs::write(part_path(&path), b"hello").unwrap();
        assert_eq!(part_len(&path), 5);
        let short = complete(&path, 6, None).unwrap_err().to_string();
        assert!(short.contains("stopped at 5 of 6"), "{}", short);

        let wrong = complete(&path, 5, Some("00")).unwrap_err().to_string();
        assert!(wrong.contains("corrupted"), "{}", wrong);
        assert!(!part_path(&path).exists(), "a corrupt part is discarded");

        std::fs::write(part_path(&path), b"hello").unwrap();
        let hash = file_sha256(&part_path(&path)).unwrap();
        assert_eq!(hash, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824");
        complete(&path, 5, Some(&hash.to_uppercase())).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"hello");
        assert!(!part_path(&path).exists());
    }

    #[test]
    fn range_headers_parse_only_what_a_resume_uses() {
        assert_eq!(parse_range_start("bytes=1024-"), Some(1024));
        assert_eq!(parse_range_start("bytes=0-99"), None);
        assert_eq!(parse_range_start("items=1-"), None);
        assert_eq!(parse_content_range("bytes 1024-2047/4096"), Some((1024, 4096)));
        assert_eq!(parse_content_range("bytes */4096"), None);
    }

    #[test]
    fn free_space_is_checked_with_a_margin() {
        let dir = tempfile::TempDir::new().unwrap();
        assert!(check_free_space(dir.path(), 1).is_ok());
        let err = check_free_space(dir.path(), u64::MAX / 2).unwrap_err().to_string();
        assert!(err.contains("Not enough free space"), "{}", err);
        assert!(err.contains("MB free on"));
    }
}
