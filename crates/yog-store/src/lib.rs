//! Stockage local d'un nœud yogfile : shards content-addressed sur disque
//! + manifests JSON. Chaque lecture de shard revérifie son hash — un shard
//! corrompu sur disque est signalé, jamais servi.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use yog_erasure::{hash_bytes, ContentHash, FileManifest};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("shard introuvable: {0}")]
    ShardNotFound(ContentHash),
    #[error("manifest introuvable: {0}")]
    ManifestNotFound(ContentHash),
    #[error("shard corrompu sur disque: attendu {expected}, obtenu {actual}")]
    CorruptShard { expected: ContentHash, actual: ContentHash },
}

/// Store on-disk d'un nœud.
///
/// Layout :
/// ```text
/// root/
///   shards/ab/cdef... (préfixe 2 hex → fanout des répertoires)
///   manifests/<file_hash>.json
/// ```
pub struct ShardStore {
    root: PathBuf,
}

impl ShardStore {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, StoreError> {
        let root = root.into();
        fs::create_dir_all(root.join("shards"))?;
        fs::create_dir_all(root.join("manifests"))?;
        Ok(Self { root })
    }

    fn shard_path(&self, hash: &str) -> PathBuf {
        self.root.join("shards").join(&hash[..2]).join(&hash[2..])
    }

    fn manifest_path(&self, file_hash: &str) -> PathBuf {
        self.root.join("manifests").join(format!("{file_hash}.json"))
    }

    /// Écrit un shard (idempotent : même contenu → même hash → même chemin).
    /// Retourne le hash du contenu. Écriture atomique via fichier temporaire.
    pub fn put_shard(&self, data: &[u8]) -> Result<ContentHash, StoreError> {
        let hash = hash_bytes(data);
        let path = self.shard_path(&hash);
        if path.exists() {
            return Ok(hash);
        }
        fs::create_dir_all(path.parent().unwrap())?;
        // Pas de fsync par shard : un shard perdu sur crash machine est
        // exactement ce que l'erasure coding + le scrubber savent réparer.
        // Le fsync par écriture divise le débit d'ingestion par ~20.
        write_atomic(&path, data, false)?;
        Ok(hash)
    }

    /// Lit un shard et vérifie son intégrité avant de le retourner.
    pub fn get_shard(&self, hash: &str) -> Result<Vec<u8>, StoreError> {
        let path = self.shard_path(hash);
        let data = fs::read(&path)
            .map_err(|_| StoreError::ShardNotFound(hash.to_string()))?;
        let actual = hash_bytes(&data);
        if actual != hash {
            return Err(StoreError::CorruptShard { expected: hash.to_string(), actual });
        }
        Ok(data)
    }

    pub fn has_shard(&self, hash: &str) -> bool {
        self.shard_path(hash).exists()
    }

    pub fn delete_shard(&self, hash: &str) -> Result<(), StoreError> {
        match fs::remove_file(self.shard_path(hash)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub fn put_manifest(&self, manifest: &FileManifest) -> Result<(), StoreError> {
        let path = self.manifest_path(&manifest.file_hash);
        // Les manifests sont rares et précieux : fsync conservé.
        write_atomic(&path, serde_json::to_string_pretty(manifest)?.as_bytes(), true)?;
        Ok(())
    }

    pub fn get_manifest(&self, file_hash: &str) -> Result<FileManifest, StoreError> {
        let path = self.manifest_path(file_hash);
        let data = fs::read(&path)
            .map_err(|_| StoreError::ManifestNotFound(file_hash.to_string()))?;
        Ok(serde_json::from_slice(&data)?)
    }

    /// Tous les hashes de shards stockés localement (parcours du fanout).
    pub fn list_shards(&self) -> Result<Vec<ContentHash>, StoreError> {
        let mut out = Vec::new();
        for prefix in fs::read_dir(self.root.join("shards"))? {
            let prefix = prefix?;
            if !prefix.file_type()?.is_dir() {
                continue;
            }
            let p = prefix.file_name().to_string_lossy().to_string();
            for entry in fs::read_dir(prefix.path())? {
                let name = entry?.file_name().to_string_lossy().to_string();
                if !name.ends_with(".tmp") {
                    out.push(format!("{p}{name}"));
                }
            }
        }
        out.sort();
        Ok(out)
    }

    /// Supprime un manifest local (idempotent).
    pub fn delete_manifest(&self, file_hash: &str) -> Result<(), StoreError> {
        match fs::remove_file(self.manifest_path(file_hash)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub fn list_manifests(&self) -> Result<Vec<ContentHash>, StoreError> {
        let mut out = Vec::new();
        for entry in fs::read_dir(self.root.join("manifests"))? {
            let name = entry?.file_name();
            if let Some(hash) = name.to_string_lossy().strip_suffix(".json") {
                out.push(hash.to_string());
            }
        }
        out.sort();
        Ok(out)
    }
}

fn write_atomic(path: &Path, data: &[u8], sync: bool) -> Result<(), StoreError> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = fs::File::create(&tmp)?;
        f.write_all(data)?;
        if sync {
            f.sync_all()?;
        }
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use yog_erasure::{encode_file, ErasureConfig};

    #[test]
    fn shard_roundtrip_and_integrity() {
        let dir = tempfile::tempdir().unwrap();
        let store = ShardStore::open(dir.path()).unwrap();

        let hash = store.put_shard(b"hello shard").unwrap();
        assert!(store.has_shard(&hash));
        assert_eq!(store.get_shard(&hash).unwrap(), b"hello shard");

        // Corruption silencieuse sur disque → détectée à la lecture.
        fs::write(store.shard_path(&hash), b"tampered!!").unwrap();
        assert!(matches!(store.get_shard(&hash), Err(StoreError::CorruptShard { .. })));

        store.delete_shard(&hash).unwrap();
        assert!(!store.has_shard(&hash));
        assert!(matches!(store.get_shard(&hash), Err(StoreError::ShardNotFound(_))));
    }

    #[test]
    fn manifest_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = ShardStore::open(dir.path()).unwrap();

        let cfg = ErasureConfig { data_shards: 2, parity_shards: 1, shard_size: 64 };
        let (manifest, _) = encode_file(b"some file content", &cfg).unwrap();
        store.put_manifest(&manifest).unwrap();

        let loaded = store.get_manifest(&manifest.file_hash).unwrap();
        assert_eq!(loaded.file_hash, manifest.file_hash);
        assert_eq!(loaded.file_size, manifest.file_size);
        assert_eq!(store.list_manifests().unwrap(), vec![manifest.file_hash]);
    }
}
