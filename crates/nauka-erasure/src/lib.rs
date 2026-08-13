//! Nauka's Reed-Solomon core: splitting into stripes, k+m encoding,
//! loss-tolerant reconstruction, integrity verified with BLAKE3.
//!
//! No I/O here: this crate only ever handles bytes. Storage and networking
//! live in other crates.

use reed_solomon_erasure::galois_8::ReedSolomon;
use serde::{Deserialize, Serialize};

/// Default size of a data shard within a stripe (1 MiB).
/// A stripe therefore covers `data_shards * SHARD_SIZE` bytes of the file.
pub const DEFAULT_SHARD_SIZE: usize = 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum ErasureError {
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("not enough shards: {available} available, {needed} required")]
    NotEnoughShards { available: usize, needed: usize },
    #[error("integrity violation: {0}")]
    IntegrityViolation(String),
    #[error("reed-solomon error: {0}")]
    ReedSolomon(#[from] reed_solomon_erasure::Error),
}

/// Encoding parameters, set at the cluster level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErasureConfig {
    /// k: number of data shards per stripe.
    pub data_shards: usize,
    /// m: number of parity shards per stripe. The file survives the loss of
    /// any m shards per stripe.
    pub parity_shards: usize,
    /// Shard size in bytes.
    pub shard_size: usize,
}

impl Default for ErasureConfig {
    fn default() -> Self {
        Self {
            data_shards: 4,
            parity_shards: 2,
            shard_size: DEFAULT_SHARD_SIZE,
        }
    }
}

impl ErasureConfig {
    pub fn validate(&self) -> Result<(), ErasureError> {
        if self.data_shards == 0 || self.parity_shards == 0 {
            return Err(ErasureError::InvalidConfig(
                "data_shards and parity_shards must be > 0".into(),
            ));
        }
        if self.data_shards + self.parity_shards > 255 {
            return Err(ErasureError::InvalidConfig(
                "data_shards + parity_shards must fit in GF(2^8), max 255".into(),
            ));
        }
        if self.shard_size == 0 {
            return Err(ErasureError::InvalidConfig("shard_size must be > 0".into()));
        }
        Ok(())
    }

    pub fn total_shards(&self) -> usize {
        self.data_shards + self.parity_shards
    }

    /// Data bytes covered by a full stripe.
    pub fn stripe_data_len(&self) -> usize {
        self.data_shards * self.shard_size
    }

    /// The config for a payload that fits in ONE stripe: shards sized to
    /// the content instead of [`DEFAULT_SHARD_SIZE`]. Shards are
    /// zero-padded to `shard_size`, so a fixed size makes every small
    /// file cost a full stripe on disk — measured on a live cluster: 340
    /// sub-4-MiB files paying 6 MiB each, 1.9 GiB of pure padding on
    /// 3.3 GiB of data. The manifest already carries `shard_size` and
    /// the decoder already honours it, so densifying is free of any
    /// format change; it only applies to single-stripe files because the
    /// size is per-manifest, not per-stripe.
    pub fn densified_for(&self, single_stripe_len: usize) -> ErasureConfig {
        debug_assert!(single_stripe_len <= self.stripe_data_len());
        ErasureConfig {
            shard_size: single_stripe_len.div_ceil(self.data_shards).max(1),
            ..*self
        }
    }

    /// The config for a SMALL payload: replication instead of striping.
    /// `data_shards = 1` turns Reed-Solomon into n-copies-any-one-wins
    /// with zero new machinery — placement, scrubbing, GC, proofs and
    /// verification all see ordinary shards. A 4 KiB file in 4+2 was six
    /// micro-shards and k round-trips per read; as 1+2 it is three full
    /// copies and ONE round-trip. The 3x overhead is capped by the
    /// caller's threshold, and the loss tolerance (any 2 of 3) matches
    /// the wide config's.
    pub fn replicated_for(&self, len: usize) -> ErasureConfig {
        ErasureConfig {
            data_shards: 1,
            parity_shards: self.parity_shards,
            shard_size: len.max(1),
        }
    }
}

/// Content identifier: BLAKE3 hash, hex-encoded.
pub type ContentHash = String;

pub fn hash_bytes(data: &[u8]) -> ContentHash {
    blake3::hash(data).to_hex().to_string()
}

/// An encoded shard, ready to be dispatched to a node.
#[derive(Debug, Clone)]
pub struct Shard {
    /// Index within the stripe: [0, k) = data, [k, k+m) = parity.
    pub index: usize,
    pub hash: ContentHash,
    pub data: Vec<u8>,
}

/// Metadata of an encoded stripe (without the bytes).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StripeMeta {
    /// Number of real file bytes covered by this stripe (the last stripe is
    /// usually partial before padding).
    pub data_len: usize,
    /// Hash of every shard, indexed by position within the stripe.
    pub shard_hashes: Vec<ContentHash>,
}

/// Manifest of an encoded file: everything needed to reconstruct it and prove
/// its integrity, without the bytes themselves.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileManifest {
    /// BLAKE3 of the complete original file.
    pub file_hash: ContentHash,
    pub file_size: u64,
    /// Display name (supplied at upload time). Does not feed into the hash.
    #[serde(default)]
    pub name: Option<String>,
    /// Optional expiration (Unix timestamp, seconds): past it, the file is
    /// dropped from the registry and its shards purged.
    #[serde(default)]
    pub expires_at: Option<u64>,
    pub config: ErasureConfig,
    pub stripes: Vec<StripeMeta>,
}

/// Encodes one stripe: `data` (≤ stripe_data_len bytes) → k+m shards.
pub fn encode_stripe(data: &[u8], cfg: &ErasureConfig) -> Result<Vec<Shard>, ErasureError> {
    cfg.validate()?;
    if data.is_empty() || data.len() > cfg.stripe_data_len() {
        return Err(ErasureError::InvalidConfig(format!(
            "stripe of {} bytes, expected between 1 and {}",
            data.len(),
            cfg.stripe_data_len()
        )));
    }

    // k data shards, zero-padded to shard_size.
    let mut shards: Vec<Vec<u8>> = (0..cfg.data_shards)
        .map(|i| {
            let start = (i * cfg.shard_size).min(data.len());
            let end = ((i + 1) * cfg.shard_size).min(data.len());
            let mut buf = data[start..end].to_vec();
            buf.resize(cfg.shard_size, 0);
            buf
        })
        .collect();
    // ...then m parity shards, computed in place.
    shards.extend(std::iter::repeat_with(|| vec![0u8; cfg.shard_size]).take(cfg.parity_shards));

    let rs = ReedSolomon::new(cfg.data_shards, cfg.parity_shards)?;
    rs.encode(&mut shards)?;

    Ok(shards
        .into_iter()
        .enumerate()
        .map(|(index, data)| Shard {
            index,
            hash: hash_bytes(&data),
            data,
        })
        .collect())
}

/// Reconstructs the original bytes of a stripe from at least k shards.
///
/// `shards[i]` is `None` when shard i is lost. Every shard present is checked
/// against its manifest hash before reconstruction: a corrupted shard is
/// treated as lost rather than silently corrupting the output.
pub fn decode_stripe(
    mut shards: Vec<Option<Vec<u8>>>,
    meta: &StripeMeta,
    cfg: &ErasureConfig,
) -> Result<Vec<u8>, ErasureError> {
    cfg.validate()?;
    if shards.len() != cfg.total_shards() {
        return Err(ErasureError::InvalidConfig(format!(
            "{} shard slots provided, {} expected",
            shards.len(),
            cfg.total_shards()
        )));
    }

    // Drop any shard whose hash does not match the manifest.
    for (i, slot) in shards.iter_mut().enumerate() {
        if let Some(data) = slot {
            if data.len() != cfg.shard_size || hash_bytes(data) != meta.shard_hashes[i] {
                *slot = None;
            }
        }
    }

    let available = shards.iter().filter(|s| s.is_some()).count();
    if available < cfg.data_shards {
        return Err(ErasureError::NotEnoughShards {
            available,
            needed: cfg.data_shards,
        });
    }

    let rs = ReedSolomon::new(cfg.data_shards, cfg.parity_shards)?;
    rs.reconstruct(&mut shards)?;

    // Check the reconstructed shards really do match the manifest.
    for (i, slot) in shards.iter().enumerate().take(cfg.data_shards) {
        let data = slot
            .as_ref()
            .expect("reconstruct guarantees the data shards");
        if hash_bytes(data) != meta.shard_hashes[i] {
            return Err(ErasureError::IntegrityViolation(format!(
                "reconstructed shard {i} does not match the manifest hash"
            )));
        }
    }

    let mut out = Vec::with_capacity(meta.data_len);
    for slot in shards.into_iter().take(cfg.data_shards) {
        out.extend_from_slice(&slot.unwrap());
    }
    out.truncate(meta.data_len);
    Ok(out)
}

/// Encodes a whole file into stripes. Returns the manifest and, per stripe, the
/// shards to dispatch.
pub fn encode_file(
    data: &[u8],
    cfg: &ErasureConfig,
) -> Result<(FileManifest, Vec<Vec<Shard>>), ErasureError> {
    cfg.validate()?;
    if data.is_empty() {
        return Err(ErasureError::InvalidConfig("empty file".into()));
    }

    let mut stripes_meta = Vec::new();
    let mut stripes_shards = Vec::new();
    for chunk in data.chunks(cfg.stripe_data_len()) {
        let shards = encode_stripe(chunk, cfg)?;
        stripes_meta.push(StripeMeta {
            data_len: chunk.len(),
            shard_hashes: shards.iter().map(|s| s.hash.clone()).collect(),
        });
        stripes_shards.push(shards);
    }

    Ok((
        FileManifest {
            file_hash: hash_bytes(data),
            file_size: data.len() as u64,
            name: None,
            expires_at: None,
            config: *cfg,
            stripes: stripes_meta,
        },
        stripes_shards,
    ))
}

/// Reconstructs a whole file from its stripes (shards possibly missing or
/// corrupted), then verifies the file-wide hash.
pub fn decode_file(
    manifest: &FileManifest,
    stripes: Vec<Vec<Option<Vec<u8>>>>,
) -> Result<Vec<u8>, ErasureError> {
    if stripes.len() != manifest.stripes.len() {
        return Err(ErasureError::InvalidConfig(format!(
            "{} stripes provided, {} expected",
            stripes.len(),
            manifest.stripes.len()
        )));
    }

    let mut out = Vec::with_capacity(manifest.file_size as usize);
    for (shards, meta) in stripes.into_iter().zip(&manifest.stripes) {
        out.extend(decode_stripe(shards, meta, &manifest.config)?);
    }

    if hash_bytes(&out) != manifest.file_hash {
        return Err(ErasureError::IntegrityViolation(
            "reconstructed file hash differs from the manifest".into(),
        ));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng};

    fn cfg_small() -> ErasureConfig {
        ErasureConfig {
            data_shards: 4,
            parity_shards: 2,
            shard_size: 1024,
        }
    }

    fn random_bytes(len: usize, seed: u64) -> Vec<u8> {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        (0..len).map(|_| rng.gen()).collect()
    }

    fn to_slots(shards: &[Shard]) -> Vec<Option<Vec<u8>>> {
        shards.iter().map(|s| Some(s.data.clone())).collect()
    }

    #[test]
    fn densified_shards_track_the_content() {
        let cfg = cfg_small(); // shard_size 1024, stripe 4096
                               // 300 bytes → shards of ceil(300/4) = 75 bytes, not 1024.
        let dense = cfg.densified_for(300);
        assert_eq!(dense.shard_size, 75);
        assert_eq!(dense.data_shards, cfg.data_shards);
        let data = random_bytes(300, 7);
        let (manifest, stripes) = encode_file(&data, &dense).unwrap();
        assert_eq!(stripes.len(), 1);
        let on_disk: usize = stripes[0].iter().map(|s| s.data.len()).sum();
        assert_eq!(on_disk, 6 * 75, "6 shards of 75 bytes, no fixed padding");
        // Loss of any 2 shards still reconstructs byte-for-byte.
        let mut slots = to_slots(&stripes[0]);
        slots[0] = None;
        slots[4] = None;
        assert_eq!(decode_file(&manifest, vec![slots]).unwrap(), data);
    }

    #[test]
    fn densified_edges_hold() {
        let cfg = cfg_small();
        // A 1-byte payload still yields a valid config (shard_size ≥ 1).
        assert_eq!(cfg.densified_for(1).shard_size, 1);
        cfg.densified_for(1).validate().unwrap();
        let data = random_bytes(1, 9);
        let (manifest, stripes) = encode_file(&data, &cfg.densified_for(1)).unwrap();
        assert_eq!(
            decode_file(&manifest, vec![to_slots(&stripes[0])]).unwrap(),
            data
        );
        // An exactly-full stripe densifies to the same size — a no-op.
        assert_eq!(
            cfg.densified_for(cfg.stripe_data_len()).shard_size,
            cfg.shard_size
        );
    }

    #[test]
    fn roundtrip_simple() {
        let cfg = cfg_small();
        let data = random_bytes(3000, 1);
        let (manifest, stripes) = encode_file(&data, &cfg).unwrap();
        let slots = stripes.iter().map(|s| to_slots(s)).collect();
        assert_eq!(decode_file(&manifest, slots).unwrap(), data);
    }

    #[test]
    fn roundtrip_multi_stripe_uneven() {
        let cfg = cfg_small();
        // Two full stripes, then a partial one holding a single byte.
        let data = random_bytes(cfg.stripe_data_len() * 2 + 1, 2);
        let (manifest, stripes) = encode_file(&data, &cfg).unwrap();
        assert_eq!(manifest.stripes.len(), 3);
        let slots = stripes.iter().map(|s| to_slots(s)).collect();
        assert_eq!(decode_file(&manifest, slots).unwrap(), data);
    }

    #[test]
    fn survives_loss_of_any_m_shards() {
        let cfg = cfg_small();
        let data = random_bytes(cfg.stripe_data_len(), 3);
        let (manifest, stripes) = encode_file(&data, &cfg).unwrap();

        // Every possible pair of lost shards (m = 2).
        for a in 0..cfg.total_shards() {
            for b in (a + 1)..cfg.total_shards() {
                let mut slots = to_slots(&stripes[0]);
                slots[a] = None;
                slots[b] = None;
                let decoded = decode_stripe(slots, &manifest.stripes[0], &cfg).unwrap();
                assert_eq!(decoded, data, "failed with shards {a} and {b} lost");
            }
        }
    }

    #[test]
    fn fails_cleanly_beyond_m_losses() {
        let cfg = cfg_small();
        let data = random_bytes(500, 4);
        let (manifest, stripes) = encode_file(&data, &cfg).unwrap();
        let mut slots = to_slots(&stripes[0]);
        slots[0] = None;
        slots[1] = None;
        slots[2] = None;
        match decode_stripe(slots, &manifest.stripes[0], &cfg) {
            Err(ErasureError::NotEnoughShards {
                available: 3,
                needed: 4,
            }) => {}
            other => panic!("expected NotEnoughShards, got {other:?}"),
        }
    }

    #[test]
    fn corrupted_shard_detected_and_repaired() {
        let cfg = cfg_small();
        let data = random_bytes(cfg.stripe_data_len(), 5);
        let (manifest, stripes) = encode_file(&data, &cfg).unwrap();

        // Corrupt one shard: it must be caught by its hash and reconstructed.
        let mut slots = to_slots(&stripes[0]);
        slots[1].as_mut().unwrap()[42] ^= 0xFF;
        let decoded = decode_stripe(slots, &manifest.stripes[0], &cfg).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn corruption_plus_losses_beyond_m_fails() {
        let cfg = cfg_small();
        let data = random_bytes(cfg.stripe_data_len(), 6);
        let (manifest, stripes) = encode_file(&data, &cfg).unwrap();

        // 2 lost + 1 corrupted = 3 unavailable > m: must fail cleanly.
        let mut slots = to_slots(&stripes[0]);
        slots[0] = None;
        slots[5] = None;
        slots[2].as_mut().unwrap()[0] ^= 0x01;
        assert!(matches!(
            decode_stripe(slots, &manifest.stripes[0], &cfg),
            Err(ErasureError::NotEnoughShards { .. })
        ));
    }

    #[test]
    fn invalid_config_rejected() {
        let bad = ErasureConfig {
            data_shards: 0,
            parity_shards: 2,
            shard_size: 1024,
        };
        assert!(matches!(
            encode_stripe(b"x", &bad),
            Err(ErasureError::InvalidConfig(_))
        ));
        let too_many = ErasureConfig {
            data_shards: 200,
            parity_shards: 100,
            shard_size: 1024,
        };
        assert!(matches!(
            encode_stripe(b"x", &too_many),
            Err(ErasureError::InvalidConfig(_))
        ));
    }
}
