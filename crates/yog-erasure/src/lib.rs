//! Cœur Reed-Solomon de yogfile : découpage en stripes, encodage k+m,
//! reconstruction tolérante aux pertes, intégrité vérifiée par BLAKE3.
//!
//! Aucune I/O ici : cette crate ne manipule que des octets. Le stockage
//! et le réseau vivent dans d'autres crates.

use reed_solomon_erasure::galois_8::ReedSolomon;
use serde::{Deserialize, Serialize};

/// Taille par défaut d'un shard de données au sein d'une stripe (1 MiB).
/// Une stripe couvre donc `data_shards * SHARD_SIZE` octets du fichier.
pub const DEFAULT_SHARD_SIZE: usize = 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum ErasureError {
    #[error("configuration invalide: {0}")]
    InvalidConfig(String),
    #[error("shards insuffisants: {available} disponibles, {needed} requis")]
    NotEnoughShards { available: usize, needed: usize },
    #[error("intégrité violée: {0}")]
    IntegrityViolation(String),
    #[error("erreur reed-solomon: {0}")]
    ReedSolomon(#[from] reed_solomon_erasure::Error),
}

/// Paramètres d'encodage, définis au niveau du cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErasureConfig {
    /// k : nombre de shards de données par stripe.
    pub data_shards: usize,
    /// m : nombre de shards de parité par stripe. Le fichier survit à la
    /// perte de n'importe quels m shards par stripe.
    pub parity_shards: usize,
    /// Taille d'un shard en octets.
    pub shard_size: usize,
}

impl Default for ErasureConfig {
    fn default() -> Self {
        Self { data_shards: 4, parity_shards: 2, shard_size: DEFAULT_SHARD_SIZE }
    }
}

impl ErasureConfig {
    pub fn validate(&self) -> Result<(), ErasureError> {
        if self.data_shards == 0 || self.parity_shards == 0 {
            return Err(ErasureError::InvalidConfig(
                "data_shards et parity_shards doivent être > 0".into(),
            ));
        }
        if self.data_shards + self.parity_shards > 255 {
            return Err(ErasureError::InvalidConfig(
                "data_shards + parity_shards doit tenir sur GF(2^8), max 255".into(),
            ));
        }
        if self.shard_size == 0 {
            return Err(ErasureError::InvalidConfig("shard_size doit être > 0".into()));
        }
        Ok(())
    }

    pub fn total_shards(&self) -> usize {
        self.data_shards + self.parity_shards
    }

    /// Octets de données couverts par une stripe complète.
    pub fn stripe_data_len(&self) -> usize {
        self.data_shards * self.shard_size
    }
}

/// Identifiant de contenu : hash BLAKE3, hex.
pub type ContentHash = String;

pub fn hash_bytes(data: &[u8]) -> ContentHash {
    blake3::hash(data).to_hex().to_string()
}

/// Un shard encodé, prêt à être dispatché sur un nœud.
#[derive(Debug, Clone)]
pub struct Shard {
    /// Index dans la stripe : [0, k) = données, [k, k+m) = parité.
    pub index: usize,
    pub hash: ContentHash,
    pub data: Vec<u8>,
}

/// Métadonnées d'une stripe encodée (sans les octets).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StripeMeta {
    /// Nombre d'octets réels du fichier couverts par cette stripe
    /// (la dernière stripe est généralement partielle avant padding).
    pub data_len: usize,
    /// Hash de chaque shard, indexé par position dans la stripe.
    pub shard_hashes: Vec<ContentHash>,
}

/// Manifest d'un fichier encodé : tout ce qu'il faut pour le reconstruire
/// et prouver son intégrité, sans les octets eux-mêmes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileManifest {
    /// BLAKE3 du fichier original complet.
    pub file_hash: ContentHash,
    pub file_size: u64,
    /// Nom d'affichage (fourni à l'upload). N'entre pas dans le hash.
    #[serde(default)]
    pub name: Option<String>,
    /// Expiration optionnelle (timestamp Unix, secondes) : au-delà, le
    /// fichier est retiré du registre et ses shards purgés.
    #[serde(default)]
    pub expires_at: Option<u64>,
    pub config: ErasureConfig,
    pub stripes: Vec<StripeMeta>,
}

/// Encode une stripe : `data` (≤ stripe_data_len octets) → k+m shards.
pub fn encode_stripe(data: &[u8], cfg: &ErasureConfig) -> Result<Vec<Shard>, ErasureError> {
    cfg.validate()?;
    if data.is_empty() || data.len() > cfg.stripe_data_len() {
        return Err(ErasureError::InvalidConfig(format!(
            "stripe de {} octets, attendu entre 1 et {}",
            data.len(),
            cfg.stripe_data_len()
        )));
    }

    // k shards de données, zero-padded à shard_size.
    let mut shards: Vec<Vec<u8>> = (0..cfg.data_shards)
        .map(|i| {
            let start = (i * cfg.shard_size).min(data.len());
            let end = ((i + 1) * cfg.shard_size).min(data.len());
            let mut buf = data[start..end].to_vec();
            buf.resize(cfg.shard_size, 0);
            buf
        })
        .collect();
    // + m shards de parité, calculés en place.
    shards.extend(std::iter::repeat_with(|| vec![0u8; cfg.shard_size]).take(cfg.parity_shards));

    let rs = ReedSolomon::new(cfg.data_shards, cfg.parity_shards)?;
    rs.encode(&mut shards)?;

    Ok(shards
        .into_iter()
        .enumerate()
        .map(|(index, data)| Shard { index, hash: hash_bytes(&data), data })
        .collect())
}

/// Reconstruit les octets originaux d'une stripe à partir d'au moins k shards.
///
/// `shards[i]` est `None` si le shard i est perdu. Chaque shard présent est
/// vérifié contre son hash du manifest avant reconstruction : un shard
/// corrompu est traité comme perdu plutôt que de corrompre silencieusement
/// la sortie.
pub fn decode_stripe(
    mut shards: Vec<Option<Vec<u8>>>,
    meta: &StripeMeta,
    cfg: &ErasureConfig,
) -> Result<Vec<u8>, ErasureError> {
    cfg.validate()?;
    if shards.len() != cfg.total_shards() {
        return Err(ErasureError::InvalidConfig(format!(
            "{} slots de shards fournis, {} attendus",
            shards.len(),
            cfg.total_shards()
        )));
    }

    // Écarte tout shard dont le hash ne correspond pas au manifest.
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

    // Vérifie que les shards reconstruits correspondent bien au manifest.
    for (i, slot) in shards.iter().enumerate().take(cfg.data_shards) {
        let data = slot.as_ref().expect("reconstruct garantit les shards de données");
        if hash_bytes(data) != meta.shard_hashes[i] {
            return Err(ErasureError::IntegrityViolation(format!(
                "shard reconstruit {i} ne correspond pas au hash du manifest"
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

/// Encode un fichier complet en stripes. Retourne le manifest et, par stripe,
/// les shards à dispatcher.
pub fn encode_file(
    data: &[u8],
    cfg: &ErasureConfig,
) -> Result<(FileManifest, Vec<Vec<Shard>>), ErasureError> {
    cfg.validate()?;
    if data.is_empty() {
        return Err(ErasureError::InvalidConfig("fichier vide".into()));
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

/// Reconstruit un fichier complet depuis ses stripes (shards possiblement
/// manquants ou corrompus), puis vérifie le hash global du fichier.
pub fn decode_file(
    manifest: &FileManifest,
    stripes: Vec<Vec<Option<Vec<u8>>>>,
) -> Result<Vec<u8>, ErasureError> {
    if stripes.len() != manifest.stripes.len() {
        return Err(ErasureError::InvalidConfig(format!(
            "{} stripes fournies, {} attendues",
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
            "hash du fichier reconstruit différent du manifest".into(),
        ));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng};

    fn cfg_small() -> ErasureConfig {
        ErasureConfig { data_shards: 4, parity_shards: 2, shard_size: 1024 }
    }

    fn random_bytes(len: usize, seed: u64) -> Vec<u8> {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        (0..len).map(|_| rng.gen()).collect()
    }

    fn to_slots(shards: &[Shard]) -> Vec<Option<Vec<u8>>> {
        shards.iter().map(|s| Some(s.data.clone())).collect()
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
        // 2 stripes pleines + une partielle d'un seul octet.
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

        // Toutes les paires de shards perdus possibles (m = 2).
        for a in 0..cfg.total_shards() {
            for b in (a + 1)..cfg.total_shards() {
                let mut slots = to_slots(&stripes[0]);
                slots[a] = None;
                slots[b] = None;
                let decoded =
                    decode_stripe(slots, &manifest.stripes[0], &cfg).unwrap();
                assert_eq!(decoded, data, "échec avec shards {a} et {b} perdus");
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
            Err(ErasureError::NotEnoughShards { available: 3, needed: 4 }) => {}
            other => panic!("attendu NotEnoughShards, obtenu {other:?}"),
        }
    }

    #[test]
    fn corrupted_shard_detected_and_repaired() {
        let cfg = cfg_small();
        let data = random_bytes(cfg.stripe_data_len(), 5);
        let (manifest, stripes) = encode_file(&data, &cfg).unwrap();

        // Corrompt un shard : il doit être détecté via son hash et reconstruit.
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

        // 2 perdus + 1 corrompu = 3 indisponibles > m : doit échouer proprement.
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
        let bad = ErasureConfig { data_shards: 0, parity_shards: 2, shard_size: 1024 };
        assert!(matches!(encode_stripe(b"x", &bad), Err(ErasureError::InvalidConfig(_))));
        let too_many = ErasureConfig { data_shards: 200, parity_shards: 100, shard_size: 1024 };
        assert!(matches!(encode_stripe(b"x", &too_many), Err(ErasureError::InvalidConfig(_))));
    }
}
