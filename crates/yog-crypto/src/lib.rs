//! Chiffrement de bout en bout des fichiers, côté client.
//!
//! Le fichier est chiffré AVANT le découpage Reed-Solomon : les nœuds
//! stockent et servent du ciphertext qu'ils ne peuvent pas lire. La clé
//! (32 octets, générée par fichier) vit dans le FRAGMENT du lien de
//! partage (`/f/<hash>#<clé>`) — les fragments ne quittent jamais le
//! navigateur/client, par construction du protocole HTTP.
//!
//! Schéma : AES-256-GCM en chunks (construction STREAM).
//! - AES-GCM est le seul AEAD natif de WebCrypto : une UI web pourra
//!   déchiffrer dans le navigateur sans wasm.
//! - Chunks de 1 Mio, nonce = préfixe aléatoire (8 o) ‖ compteur BE (4 o) ;
//!   le flag « dernier chunk » est authentifié (AAD) → troncature,
//!   réordonnancement et ajout de données sont détectés, pas seulement la
//!   modification.
//!
//! Format du flux chiffré :
//! ```text
//! "YGE1" ‖ préfixe_nonce(8)
//! puis par chunk : longueur_ct u32 LE ‖ flags u8 (1 = dernier) ‖ ct
//! ```

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use rand::RngCore;
use std::io::{Read, Write};

/// Taille d'un chunk de plaintext.
pub const CHUNK_SIZE: usize = 1024 * 1024;
/// Surcoût AEAD par chunk (tag GCM).
pub const TAG_SIZE: usize = 16;
const MAGIC: &[u8; 4] = b"YGE1";
const FLAG_LAST: u8 = 1;

#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("clé invalide (attendu 32 octets en base64url)")]
    BadKey,
    #[error("flux invalide: {0}")]
    BadStream(&'static str),
    #[error("déchiffrement refusé: données altérées ou mauvaise clé")]
    AuthFailed,
}

/// Clé de fichier : 32 octets aléatoires, encodés en base64url (sans
/// padding) dans le fragment du lien.
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

/// Chiffre `input` vers `output` en streaming. Mémoire bornée à un chunk.
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

    // Lecture avec un chunk d'avance pour connaître le dernier.
    let mut current = read_chunk(input)?;
    let mut counter: u32 = 0;
    loop {
        let next = if current.len() < CHUNK_SIZE { Vec::new() } else { read_chunk(input)? };
        let last = next.is_empty();
        let flags = if last { FLAG_LAST } else { 0 };
        let nonce = nonce_for(&prefix, counter);
        let ct = cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload { msg: &current, aad: &[flags] },
            )
            .map_err(|_| CryptoError::AuthFailed)?;
        output.write_all(&(ct.len() as u32).to_le_bytes())?;
        output.write_all(&[flags])?;
        output.write_all(&ct)?;
        counter = counter.checked_add(1).ok_or(CryptoError::BadStream("fichier trop grand"))?;
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

/// Déchiffre `input` vers `output` en streaming. Échoue si les données sont
/// altérées, tronquées, réordonnées ou si la clé est mauvaise.
pub fn decrypt(
    key: &FileKey,
    input: &mut impl Read,
    output: &mut impl Write,
) -> Result<(), CryptoError> {
    let cipher = Aes256Gcm::new((&key.0).into());
    let mut header = [0u8; 12];
    input.read_exact(&mut header).map_err(|_| CryptoError::BadStream("en-tête absent"))?;
    if &header[..4] != MAGIC {
        return Err(CryptoError::BadStream("mauvais magic (pas un flux yogfile ?)"));
    }
    let prefix: [u8; 8] = header[4..].try_into().unwrap();

    let mut counter: u32 = 0;
    loop {
        let mut len_flags = [0u8; 5];
        input
            .read_exact(&mut len_flags)
            .map_err(|_| CryptoError::BadStream("flux tronqué (chunk manquant)"))?;
        let len = u32::from_le_bytes(len_flags[..4].try_into().unwrap()) as usize;
        let flags = len_flags[4];
        if len < TAG_SIZE || len > CHUNK_SIZE + TAG_SIZE {
            return Err(CryptoError::BadStream("taille de chunk invalide"));
        }
        let mut ct = vec![0u8; len];
        input.read_exact(&mut ct).map_err(|_| CryptoError::BadStream("chunk incomplet"))?;
        let nonce = nonce_for(&prefix, counter);
        let plain = cipher
            .decrypt(Nonce::from_slice(&nonce), Payload { msg: &ct, aad: &[flags] })
            .map_err(|_| CryptoError::AuthFailed)?;
        output.write_all(&plain)?;
        counter = counter.checked_add(1).ok_or(CryptoError::BadStream("compteur épuisé"))?;
        if flags & FLAG_LAST != 0 {
            // Rien ne doit suivre le dernier chunk.
            let mut extra = [0u8; 1];
            return match input.read(&mut extra)? {
                0 => Ok(()),
                _ => Err(CryptoError::BadStream("données après le dernier chunk")),
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
        roundtrip(b"petit");
        roundtrip(&vec![7u8; CHUNK_SIZE]); // pile un chunk
        roundtrip(&vec![9u8; CHUNK_SIZE * 2 + 137]); // multi-chunks inégal
    }

    #[test]
    fn key_encoding_roundtrip() {
        let key = FileKey::generate();
        let decoded = FileKey::decode(&key.encode()).unwrap();
        assert_eq!(key.0, decoded.0);
        assert!(FileKey::decode("pas-une-clé!").is_err());
    }

    #[test]
    fn ciphertext_leaks_nothing() {
        // Un motif très reconnaissable ne doit pas apparaître chiffré.
        let data = b"MOTIF-SECRET-".repeat(50_000);
        let ct = roundtrip(&data);
        assert!(!ct.windows(13).any(|w| w == b"MOTIF-SECRET-"));
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

        // Altération d'un octet au milieu.
        let mut bad = ct.clone();
        let mid = bad.len() / 2;
        bad[mid] ^= 0x01;
        let mut out = Vec::new();
        assert!(decrypt(&key, &mut bad.as_slice(), &mut out).is_err());

        // Troncature après le premier chunk (frame header 12 + 5 + ct).
        let first_frame_end = 12 + 5 + (CHUNK_SIZE + TAG_SIZE);
        let mut out = Vec::new();
        assert!(matches!(
            decrypt(&key, &mut &ct[..first_frame_end], &mut out),
            Err(CryptoError::BadStream(_))
        ));

        // Données ajoutées après la fin.
        let mut extended = ct.clone();
        extended.push(0);
        let mut out = Vec::new();
        assert!(decrypt(&key, &mut extended.as_slice(), &mut out).is_err());
    }
}
