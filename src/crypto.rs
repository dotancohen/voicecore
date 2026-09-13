//! Optional encryption of recordings in the bucket (Stage 15, ENC-1..4).
//!
//! One recording key per account, 32 random bytes. A file is encrypted in
//! chunks of one MiB with AES-256-GCM: a random 12-byte file nonce in the
//! header, and for chunk `i` a nonce whose last four bytes are the file
//! nonce's XOR `i`, so no two chunks of one file share a nonce under the
//! key; the header, the chunk index and whether the chunk is the last are
//! the associated data, so a chunk moved, repeated or cut off fails to
//! open. Memory is one chunk, never the file (TECHNICAL-DECISIONS 3.1).
//!
//! On the wire and in the bucket: `VOICEENC` (8) `1` (1) nonce (12), then
//! each chunk's ciphertext followed by its 16-byte tag. An object of an
//! encrypted recording carries the suffix `.enc`.

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM};

use crate::error::{VoiceError, VoiceResult};

pub const MAGIC: &[u8; 8] = b"VOICEENC";
pub const VERSION: u8 = 1;
pub const NONCE_LEN: usize = 12;
pub const HEADER_LEN: usize = 8 + 1 + NONCE_LEN;
pub const TAG_LEN: usize = 16;
pub const KEY_LEN: usize = 32;
/// Plain bytes per chunk.
pub const CHUNK_PLAIN: usize = 1024 * 1024;
/// Encrypted bytes per full chunk.
pub const CHUNK_ENC: usize = CHUNK_PLAIN + TAG_LEN;
/// The suffix of an encrypted object in the bucket.
pub const OBJECT_SUFFIX: &str = ".enc";

/// A recording key: 32 bytes.
#[derive(Clone)]
pub struct RecordingKey(pub [u8; KEY_LEN]);

impl std::fmt::Debug for RecordingKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RecordingKey(…)")
    }
}

impl RecordingKey {
    /// A new key from the operating system's random source.
    pub fn generate() -> Self {
        let mut bytes = [0u8; KEY_LEN];
        bytes[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        bytes[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        Self(bytes)
    }

    /// The key as it is exported, shown and typed: 43 base64url characters.
    pub fn to_text(&self) -> String {
        base64::Engine::encode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, self.0)
    }

    /// A key from its text; spaces and line breaks are ignored.
    pub fn from_text(text: &str) -> VoiceResult<Self> {
        let cleaned: String = text.chars().filter(|c| !c.is_whitespace()).collect();
        let bytes = base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, cleaned.as_bytes())
            .map_err(|_| VoiceError::validation("recording key", "not the 43 characters of a recording key"))?;
        let bytes: [u8; KEY_LEN] = bytes.try_into().map_err(|_| VoiceError::validation("recording key", "a recording key is 32 bytes"))?;
        Ok(Self(bytes))
    }

    fn cipher(&self) -> LessSafeKey {
        LessSafeKey::new(UnboundKey::new(&AES_256_GCM, &self.0).expect("32 bytes"))
    }
}

/// The encrypted length of a file of `plain_len` bytes.
pub fn encrypted_len(plain_len: u64) -> u64 {
    HEADER_LEN as u64 + plain_len + TAG_LEN as u64 * chunk_count(plain_len)
}

/// How many chunks a file of `plain_len` bytes makes; an empty file is one empty chunk.
pub fn chunk_count(plain_len: u64) -> u64 {
    plain_len.div_ceil(CHUNK_PLAIN as u64).max(1)
}

/// Whether the bytes begin as an encrypted recording does.
pub fn is_encrypted_header(bytes: &[u8]) -> bool {
    bytes.len() >= HEADER_LEN && &bytes[..8] == MAGIC && bytes[8] == VERSION
}

/// Whether the file begins as an encrypted recording does.
pub fn file_is_encrypted(path: &Path) -> bool {
    let mut head = [0u8; HEADER_LEN];
    std::fs::File::open(path).and_then(|mut f| f.read_exact(&mut head)).map(|_| is_encrypted_header(&head)).unwrap_or(false)
}

fn header_of(nonce: &[u8; NONCE_LEN]) -> [u8; HEADER_LEN] {
    let mut header = [0u8; HEADER_LEN];
    header[..8].copy_from_slice(MAGIC);
    header[8] = VERSION;
    header[9..].copy_from_slice(nonce);
    header
}

fn nonce_of(file_nonce: &[u8; NONCE_LEN], index: u64) -> Nonce {
    let mut n = *file_nonce;
    let counter = (index as u32).to_be_bytes();
    for (b, c) in n[8..].iter_mut().zip(counter) {
        *b ^= c;
    }
    Nonce::assume_unique_for_key(n)
}

fn aad_of(header: &[u8; HEADER_LEN], index: u64, last: bool) -> Vec<u8> {
    let mut aad = Vec::with_capacity(HEADER_LEN + 9);
    aad.extend_from_slice(header);
    aad.extend_from_slice(&index.to_be_bytes());
    aad.push(u8::from(last));
    aad
}

/// One chunk encrypted: the ciphertext then its tag.
pub fn encrypt_chunk(key: &RecordingKey, file_nonce: &[u8; NONCE_LEN], index: u64, last: bool, plain: &[u8]) -> Vec<u8> {
    let header = header_of(file_nonce);
    let mut buf = plain.to_vec();
    key.cipher()
        .seal_in_place_append_tag(nonce_of(file_nonce, index), Aad::from(aad_of(&header, index, last)), &mut buf)
        .expect("sealing cannot fail for a chunk");
    buf
}

/// One chunk opened, or an error when the key, the place or the bytes are wrong.
pub fn decrypt_chunk(key: &RecordingKey, file_nonce: &[u8; NONCE_LEN], index: u64, last: bool, encrypted: &[u8]) -> VoiceResult<Vec<u8>> {
    let header = header_of(file_nonce);
    let mut buf = encrypted.to_vec();
    let plain_len = key
        .cipher()
        .open_in_place(nonce_of(file_nonce, index), Aad::from(aad_of(&header, index, last)), &mut buf)
        .map_err(|_| VoiceError::validation("recording", format!("chunk {} did not open: wrong key, or the bytes were changed", index)))?
        .len();
    buf.truncate(plain_len);
    Ok(buf)
}

/// Bytes read at any offset: a plain file, or the encrypted view of one.
pub trait ByteSource {
    fn len(&self) -> u64;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> std::io::Result<()>;
}

impl ByteSource for std::fs::File {
    fn len(&self) -> u64 {
        self.metadata().map(|m| m.len()).unwrap_or(0)
    }
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> std::io::Result<()> {
        self.seek(SeekFrom::Start(offset))?;
        self.read_exact(buf)
    }
}

/// The encrypted form of a plain file, readable at any offset without a
/// second copy on disk: each chunk is encrypted when a read touches it, so
/// an upload in parts of a large recording never holds more than one chunk.
pub struct EncryptedView {
    file: std::fs::File,
    plain_len: u64,
    key: RecordingKey,
    nonce: [u8; NONCE_LEN],
    /// The last chunk built, so a read that spans it twice encrypts once
    cached: Option<(u64, Vec<u8>)>,
}

impl EncryptedView {
    pub fn open(path: &Path, key: &RecordingKey) -> VoiceResult<Self> {
        let file = std::fs::File::open(path)?;
        let plain_len = file.metadata()?.len();
        let mut nonce = [0u8; NONCE_LEN];
        nonce.copy_from_slice(&uuid::Uuid::new_v4().as_bytes()[..NONCE_LEN]);
        Ok(Self { file, plain_len, key: key.clone(), nonce, cached: None })
    }

    fn chunk(&mut self, index: u64) -> std::io::Result<&[u8]> {
        if self.cached.as_ref().map(|(i, _)| *i) != Some(index) {
            let start = index * CHUNK_PLAIN as u64;
            let len = (self.plain_len - start.min(self.plain_len)).min(CHUNK_PLAIN as u64) as usize;
            let mut plain = vec![0u8; len];
            if len > 0 {
                self.file.seek(SeekFrom::Start(start))?;
                self.file.read_exact(&mut plain)?;
            }
            let last = index + 1 == chunk_count(self.plain_len);
            self.cached = Some((index, encrypt_chunk(&self.key, &self.nonce, index, last, &plain)));
        }
        Ok(&self.cached.as_ref().expect("just built").1)
    }
}

impl ByteSource for EncryptedView {
    fn len(&self) -> u64 {
        encrypted_len(self.plain_len)
    }

    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> std::io::Result<()> {
        let total = self.len();
        if offset + buf.len() as u64 > total {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "past the end of the encrypted file"));
        }
        let header = header_of(&self.nonce);
        let mut filled = 0usize;
        let mut at = offset;
        while filled < buf.len() {
            if at < HEADER_LEN as u64 {
                let take = ((HEADER_LEN as u64 - at) as usize).min(buf.len() - filled);
                buf[filled..filled + take].copy_from_slice(&header[at as usize..at as usize + take]);
                filled += take;
                at += take as u64;
                continue;
            }
            let body_at = at - HEADER_LEN as u64;
            let index = body_at / CHUNK_ENC as u64;
            let within = (body_at % CHUNK_ENC as u64) as usize;
            let chunk = self.chunk(index)?;
            let take = (chunk.len() - within).min(buf.len() - filled);
            buf[filled..filled + take].copy_from_slice(&chunk[within..within + take]);
            filled += take;
            at += take as u64;
        }
        Ok(())
    }
}

/// Decrypt a stream into a writer, one chunk at a time. Returns the plain length.
pub fn decrypt_stream(key: &RecordingKey, src: &mut dyn Read, dst: &mut dyn Write) -> VoiceResult<u64> {
    let mut header = [0u8; HEADER_LEN];
    src.read_exact(&mut header).map_err(|_| VoiceError::validation("recording", "shorter than an encrypted recording's header"))?;
    if !is_encrypted_header(&header) {
        return Err(VoiceError::validation("recording", "not an encrypted recording"));
    }
    let mut nonce = [0u8; NONCE_LEN];
    nonce.copy_from_slice(&header[9..]);
    let mut written = 0u64;
    let mut index = 0u64;
    let mut buf = vec![0u8; CHUNK_ENC + 1];
    let mut carry: Option<u8> = None;
    loop {
        let mut have = 0usize;
        if let Some(b) = carry.take() {
            buf[0] = b;
            have = 1;
        }
        // Read one chunk and one byte more, to know whether it is the last
        while have < CHUNK_ENC + 1 {
            let n = src.read(&mut buf[have..CHUNK_ENC + 1])?;
            if n == 0 {
                break;
            }
            have += n;
        }
        let last = have <= CHUNK_ENC;
        let chunk_len = if last { have } else { CHUNK_ENC };
        if !last {
            carry = Some(buf[CHUNK_ENC]);
        }
        if chunk_len < TAG_LEN {
            return Err(VoiceError::validation("recording", "cut off before the end"));
        }
        let plain = decrypt_chunk(key, &nonce, index, last, &buf[..chunk_len])?;
        dst.write_all(&plain)?;
        written += plain.len() as u64;
        index += 1;
        if last {
            break;
        }
    }
    Ok(written)
}

/// Decrypt a file into another, written whole before it exists at `dst`.
pub fn decrypt_file(key: &RecordingKey, src: &Path, dst: &Path) -> VoiceResult<u64> {
    let mut input = std::fs::File::open(src)?;
    let part = crate::transfer::part_path(dst);
    let written = {
        let mut output = std::fs::File::create(&part)?;
        match decrypt_stream(key, &mut input, &mut output) {
            Ok(n) => {
                output.sync_all()?;
                n
            }
            Err(e) => {
                drop(output);
                let _ = std::fs::remove_file(&part);
                return Err(e);
            }
        }
    };
    std::fs::rename(&part, dst)?;
    Ok(written)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i * 7 % 251) as u8).collect()
    }

    fn view_bytes(view: &mut EncryptedView) -> Vec<u8> {
        let mut out = vec![0u8; view.len() as usize];
        view.read_at(0, &mut out).unwrap();
        out
    }

    /// ENC-2: the encrypted stream opens to the plain file, at every chunk
    /// boundary shape, and the view reads the same bytes at any offset.
    #[test]
    fn a_file_round_trips_at_every_boundary_and_the_view_reads_at_any_offset() {
        let key = RecordingKey::generate();
        let temp = tempfile::TempDir::new().unwrap();
        for len in [0usize, 1, CHUNK_PLAIN - 1, CHUNK_PLAIN, CHUNK_PLAIN + 1, 2 * CHUNK_PLAIN + 12345] {
            let path = temp.path().join(format!("{}.ogg", len));
            std::fs::write(&path, bytes(len)).unwrap();
            let mut view = EncryptedView::open(&path, &key).unwrap();
            assert_eq!(view.len(), encrypted_len(len as u64));
            let whole = view_bytes(&mut view);
            assert!(is_encrypted_header(&whole));
            // The same stream read in odd pieces from odd offsets
            let mut pieced = Vec::new();
            let mut at = 0u64;
            for piece in [21usize, 1000, 5, CHUNK_ENC, 3].iter().cycle() {
                if at >= whole.len() as u64 { break; }
                let take = (*piece as u64).min(whole.len() as u64 - at) as usize;
                let mut buf = vec![0u8; take];
                view.read_at(at, &mut buf).unwrap();
                pieced.extend_from_slice(&buf);
                at += take as u64;
            }
            assert_eq!(pieced, whole, "len {}", len);
            let mut plain = Vec::new();
            let n = decrypt_stream(&key, &mut whole.as_slice(), &mut plain).unwrap();
            assert_eq!(n as usize, len);
            assert_eq!(plain, bytes(len), "len {}", len);
        }
    }

    /// ENC-2: a wrong key, a changed byte, a swapped chunk and a cut-off end all refuse.
    #[test]
    fn the_wrong_key_a_change_a_swap_and_a_cut_are_refused() {
        let key = RecordingKey::generate();
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("הקלטה.ogg");
        std::fs::write(&path, bytes(2 * CHUNK_PLAIN + 100)).unwrap();
        let whole = view_bytes(&mut EncryptedView::open(&path, &key).unwrap());
        let open = |k: &RecordingKey, data: &[u8]| decrypt_stream(k, &mut &data[..], &mut Vec::new());
        assert!(open(&key, &whole).is_ok());
        assert!(open(&RecordingKey::generate(), &whole).is_err(), "another key");
        let mut changed = whole.clone();
        changed[HEADER_LEN + 10] ^= 1;
        assert!(open(&key, &changed).is_err(), "a changed byte");
        let mut swapped = whole.clone();
        let (a, b) = (HEADER_LEN, HEADER_LEN + CHUNK_ENC);
        let first = swapped[a..a + CHUNK_ENC].to_vec();
        let second = swapped[b..b + CHUNK_ENC].to_vec();
        swapped[a..a + CHUNK_ENC].copy_from_slice(&second);
        swapped[b..b + CHUNK_ENC].copy_from_slice(&first);
        assert!(open(&key, &swapped).is_err(), "chunks swapped");
        assert!(open(&key, &whole[..whole.len() - 50]).is_err(), "cut off");
        assert!(open(&key, &whole[..HEADER_LEN + CHUNK_ENC]).is_err(), "cut at a chunk boundary: the last flag is missing");
        assert!(open(&key, b"not encrypted at all").is_err());
    }

    /// ENC-1: the key's text form round-trips and a wrong one is refused.
    #[test]
    fn the_key_text_is_43_characters_and_round_trips() {
        let key = RecordingKey::generate();
        let text = key.to_text();
        assert_eq!(text.len(), 43);
        assert_eq!(RecordingKey::from_text(&format!(" {}\n", text)).unwrap().0, key.0);
        assert!(RecordingKey::from_text("short").is_err());
        assert!(RecordingKey::from_text(&"a".repeat(44)).is_err());
    }

    /// ENC-2: two files never share a nonce, and a decrypted file is written whole or not at all.
    #[test]
    fn files_get_their_own_nonce_and_decrypt_file_writes_whole() {
        let key = RecordingKey::generate();
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("a.ogg");
        std::fs::write(&path, b"same bytes").unwrap();
        let one = view_bytes(&mut EncryptedView::open(&path, &key).unwrap());
        let two = view_bytes(&mut EncryptedView::open(&path, &key).unwrap());
        assert_ne!(one, two);
        let enc = temp.path().join("a.enc");
        std::fs::write(&enc, &one).unwrap();
        let out = temp.path().join("out.ogg");
        assert_eq!(decrypt_file(&key, &enc, &out).unwrap(), 10);
        assert_eq!(std::fs::read(&out).unwrap(), b"same bytes");
        assert!(file_is_encrypted(&enc) && !file_is_encrypted(&out));
        let bad = temp.path().join("bad.enc");
        std::fs::write(&bad, &one[..one.len() - 3]).unwrap();
        let out2 = temp.path().join("out2.ogg");
        assert!(decrypt_file(&key, &bad, &out2).is_err());
        assert!(!out2.exists() && !crate::transfer::part_path(&out2).exists());
    }
}
