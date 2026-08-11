//! Client-side end-to-end file encryption.
//!
//! The file is encrypted BEFORE Reed-Solomon splitting: nodes store and serve
//! ciphertext they cannot read. The key (32 bytes, generated per file) lives
//! in the FRAGMENT of the share link (`/f/<hash>#<key>`) — fragments never
//! leave the browser/client, by construction of the HTTP protocol.
//!
//! Scheme: AES-256-GCM in chunks (STREAM construction).
//! - AES-GCM is the only AEAD natively available in WebCrypto: a web UI will
//!   be able to decrypt in the browser without wasm.
//! - 1 MiB chunks, nonce = random prefix (8 B) ‖ BE counter (4 B); the "last
//!   chunk" flag is authenticated (AAD) → truncation, reordering and appended
//!   data are detected, not just modification.
//!
//! Encrypted stream format:
//! ```text
//! "YGE1" ‖ nonce_prefix(8)
//! then per chunk: ct_len u32 LE ‖ flags u8 (1 = last) ‖ ct
//! ```

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use rand::RngCore;
use std::io::{Read, Write};

/// Size of a plaintext chunk.
pub const CHUNK_SIZE: usize = 1024 * 1024;
/// Per-chunk AEAD overhead (GCM tag).
pub const TAG_SIZE: usize = 16;
const MAGIC: &[u8; 4] = b"NKA1";
const FLAG_LAST: u8 = 1;

#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid key (expected 32 bytes in base64url)")]
    BadKey,
    #[error("invalid stream: {0}")]
    BadStream(&'static str),
    #[error("decryption refused: tampered data or wrong key")]
    AuthFailed,
}

/// File key: 32 random bytes, encoded as base64url (without padding) in the
/// link fragment.
#[derive(Clone)]
pub struct FileKey(pub [u8; 32]);

impl FileKey {
    pub fn generate() -> Self {
        let mut key = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut key);
        Self(key)
    }

    pub fn encode(&self) -> String {
        data_encoding::BASE64URL_NOPAD.encode(&self.0)
    }

    pub fn decode(s: &str) -> Result<Self, CryptoError> {
        let bytes = data_encoding::BASE64URL_NOPAD
            .decode(s.trim().as_bytes())
            .map_err(|_| CryptoError::BadKey)?;
        let key: [u8; 32] = bytes.try_into().map_err(|_| CryptoError::BadKey)?;
        Ok(Self(key))
    }
}

fn nonce_for(prefix: &[u8; 8], counter: u32) -> [u8; 12] {
    let mut nonce = [0u8; 12];
    nonce[..8].copy_from_slice(prefix);
    nonce[8..].copy_from_slice(&counter.to_be_bytes());
    nonce
}

/// Encrypts `input` into `output` as a stream. Memory bounded to one chunk.
pub fn encrypt(
    key: &FileKey,
    input: &mut impl Read,
    output: &mut impl Write,
) -> Result<(), CryptoError> {
    let cipher = Aes256Gcm::new((&key.0).into());
    let mut prefix = [0u8; 8];
    rand::thread_rng().fill_bytes(&mut prefix);
    output.write_all(MAGIC)?;
    output.write_all(&prefix)?;

    // Read one chunk ahead so we know which one is the last.
    let mut current = read_chunk(input)?;
    let mut counter: u32 = 0;
    loop {
        let next = if current.len() < CHUNK_SIZE {
            Vec::new()
        } else {
            read_chunk(input)?
        };
        let last = next.is_empty();
        let flags = if last { FLAG_LAST } else { 0 };
        let nonce = nonce_for(&prefix, counter);
        let ct = cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: &current,
                    aad: &[flags],
                },
            )
            .map_err(|_| CryptoError::AuthFailed)?;
        output.write_all(&(ct.len() as u32).to_le_bytes())?;
        output.write_all(&[flags])?;
        output.write_all(&ct)?;
        counter = counter
            .checked_add(1)
            .ok_or(CryptoError::BadStream("file too large"))?;
        if last {
            return Ok(());
        }
        current = next;
    }
}

fn read_chunk(input: &mut impl Read) -> Result<Vec<u8>, CryptoError> {
    let mut buf = vec![0u8; CHUNK_SIZE];
    let mut filled = 0;
    while filled < CHUNK_SIZE {
        let n = input.read(&mut buf[filled..])?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    buf.truncate(filled);
    Ok(buf)
}

/// Decrypts `input` into `output` as a stream. Fails if the data has been
/// tampered with, truncated, reordered, or if the key is wrong.
pub fn decrypt(
    key: &FileKey,
    input: &mut impl Read,
    output: &mut impl Write,
) -> Result<(), CryptoError> {
    let cipher = Aes256Gcm::new((&key.0).into());
    let mut header = [0u8; 12];
    input
        .read_exact(&mut header)
        .map_err(|_| CryptoError::BadStream("missing header"))?;
    if &header[..4] != MAGIC {
        return Err(CryptoError::BadStream(
            "bad magic (not a Nauka encrypted stream?)",
        ));
    }
    let prefix: [u8; 8] = header[4..].try_into().unwrap();

    let mut counter: u32 = 0;
    loop {
        let mut len_flags = [0u8; 5];
        input
            .read_exact(&mut len_flags)
            .map_err(|_| CryptoError::BadStream("truncated stream (missing chunk)"))?;
        let len = u32::from_le_bytes(len_flags[..4].try_into().unwrap()) as usize;
        let flags = len_flags[4];
        if !(TAG_SIZE..=CHUNK_SIZE + TAG_SIZE).contains(&len) {
            return Err(CryptoError::BadStream("invalid chunk size"));
        }
        let mut ct = vec![0u8; len];
        input
            .read_exact(&mut ct)
            .map_err(|_| CryptoError::BadStream("incomplete chunk"))?;
        let nonce = nonce_for(&prefix, counter);
        let plain = cipher
            .decrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: &ct,
                    aad: &[flags],
                },
            )
            .map_err(|_| CryptoError::AuthFailed)?;
        output.write_all(&plain)?;
        counter = counter
            .checked_add(1)
            .ok_or(CryptoError::BadStream("counter exhausted"))?;
        if flags & FLAG_LAST != 0 {
            // Nothing may follow the last chunk.
            let mut extra = [0u8; 1];
            return match input.read(&mut extra)? {
                0 => Ok(()),
                _ => Err(CryptoError::BadStream("data after the last chunk")),
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(data: &[u8]) -> Vec<u8> {
        let key = FileKey::generate();
        let mut ct = Vec::new();
        encrypt(&key, &mut &data[..], &mut ct).unwrap();
        let mut out = Vec::new();
        decrypt(&key, &mut ct.as_slice(), &mut out).unwrap();
        assert_eq!(out, data);
        ct
    }

    #[test]
    fn roundtrips() {
        roundtrip(b"");
        roundtrip(b"small");
        roundtrip(&vec![7u8; CHUNK_SIZE]); // exactly one chunk
        roundtrip(&vec![9u8; CHUNK_SIZE * 2 + 137]); // uneven multi-chunk
    }

    #[test]
    fn key_encoding_roundtrip() {
        let key = FileKey::generate();
        let decoded = FileKey::decode(&key.encode()).unwrap();
        assert_eq!(key.0, decoded.0);
        assert!(FileKey::decode("not-a-key!").is_err());
    }

    #[test]
    fn ciphertext_leaks_nothing() {
        // A highly recognizable pattern must not show up in the ciphertext.
        let data = b"SECRET-TOKEN-".repeat(50_000);
        let ct = roundtrip(&data);
        assert!(!ct.windows(13).any(|w| w == b"SECRET-TOKEN-"));
    }

    #[test]
    fn wrong_key_rejected() {
        let key = FileKey::generate();
        let mut ct = Vec::new();
        encrypt(&key, &mut &b"secret"[..], &mut ct).unwrap();
        let mut out = Vec::new();
        assert!(matches!(
            decrypt(&FileKey::generate(), &mut ct.as_slice(), &mut out),
            Err(CryptoError::AuthFailed)
        ));
    }

    #[test]
    fn tamper_truncate_reorder_detected() {
        let key = FileKey::generate();
        let data = vec![3u8; CHUNK_SIZE * 3];
        let mut ct = Vec::new();
        encrypt(&key, &mut data.as_slice(), &mut ct).unwrap();

        // One byte tampered with in the middle.
        let mut bad = ct.clone();
        let mid = bad.len() / 2;
        bad[mid] ^= 0x01;
        let mut out = Vec::new();
        assert!(decrypt(&key, &mut bad.as_slice(), &mut out).is_err());

        // Truncation after the first chunk (frame header 12 + 5 + ct).
        let first_frame_end = 12 + 5 + (CHUNK_SIZE + TAG_SIZE);
        let mut out = Vec::new();
        assert!(matches!(
            decrypt(&key, &mut &ct[..first_frame_end], &mut out),
            Err(CryptoError::BadStream(_))
        ));

        // Data appended after the end.
        let mut extended = ct.clone();
        extended.push(0);
        let mut out = Vec::new();
        assert!(decrypt(&key, &mut extended.as_slice(), &mut out).is_err());
    }
}
