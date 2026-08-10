//! Local storage for a Nauka node: content-addressed shards on disk, alongside
//! JSON manifests. Every shard read re-checks its hash — a shard corrupted on
//! disk is reported, never served.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use nauka_erasure::{hash_bytes, ContentHash, FileManifest};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("shard not found: {0}")]
    ShardNotFound(ContentHash),
    #[error("manifest not found: {0}")]
    ManifestNotFound(ContentHash),
    #[error("corrupted shard on disk: expected {expected}, got {actual}")]
    CorruptShard {
        expected: ContentHash,
        actual: ContentHash,
    },
}

/// On-disk store of a node.
///
/// Layout:
/// ```text
/// root/
///   shards/ab/cdef... (2-hex prefix → directory fanout)
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
        self.root
            .join("manifests")
            .join(format!("{file_hash}.json"))
    }

    /// Writes a shard (idempotent: same content → same hash → same path).
    /// Returns the content hash. Atomic write through a temporary file.
    pub fn put_shard(&self, data: &[u8]) -> Result<ContentHash, StoreError> {
        let hash = hash_bytes(data);
        let path = self.shard_path(&hash);
        if path.exists() {
            return Ok(hash);
        }
        fs::create_dir_all(path.parent().unwrap())?;
        // No per-shard fsync: a shard lost to a machine crash is exactly what
        // erasure coding and the scrubber know how to repair. Fsyncing every
        // write divides ingest throughput by ~20.
        write_atomic(&path, data, false)?;
        Ok(hash)
    }

    /// Reads a shard and verifies its integrity before handing it back.
    pub fn get_shard(&self, hash: &str) -> Result<Vec<u8>, StoreError> {
        let path = self.shard_path(hash);
        let data = fs::read(&path).map_err(|_| StoreError::ShardNotFound(hash.to_string()))?;
        let actual = hash_bytes(&data);
        if actual != hash {
            // Silent disk corruption, caught by the on-read verification.
            // Counted here so a rotting disk shows up as a climbing counter
            // long before enough shards die to threaten a stripe.
            metrics::counter!("nauka_store_corrupt_shards_total").increment(1);
            return Err(StoreError::CorruptShard {
                expected: hash.to_string(),
                actual,
            });
        }
        Ok(data)
    }

    /// Register the HELP/TYPE text of the store metrics. The store has no
    /// init hook of its own; the node calls this once at startup.
    pub fn describe_metrics() {
        metrics::describe_counter!(
            "nauka_store_corrupt_shards_total",
            "Shards whose bytes failed BLAKE3 verification on read — silent disk corruption. The scrubber heals them; the counter is the disk's health record."
        );
    }

    pub fn has_shard(&self, hash: &str) -> bool {
        self.shard_path(hash).exists()
    }

    /// Time elapsed since the shard file was written. `None` when the shard
    /// is missing or the filesystem cannot answer — callers deciding whether
    /// to DELETE must treat `None` as "too young", never the reverse.
    pub fn shard_age(&self, hash: &str) -> Option<std::time::Duration> {
        fs::metadata(self.shard_path(hash))
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.elapsed().ok())
    }

    /// Time elapsed since the manifest was written locally. Used to tell a
    /// freshly uploaded file from a deleted one when the replicated
    /// registry disagrees with the local store.
    pub fn manifest_age(&self, file_hash: &str) -> Option<std::time::Duration> {
        fs::metadata(self.manifest_path(file_hash))
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.elapsed().ok())
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
        // Manifests are rare and precious: keep the fsync here.
        write_atomic(
            &path,
            serde_json::to_string_pretty(manifest)?.as_bytes(),
            true,
        )?;
        Ok(())
    }

    pub fn get_manifest(&self, file_hash: &str) -> Result<FileManifest, StoreError> {
        let path = self.manifest_path(file_hash);
        let data =
            fs::read(&path).map_err(|_| StoreError::ManifestNotFound(file_hash.to_string()))?;
        Ok(serde_json::from_slice(&data)?)
    }

    /// Every shard hash stored locally (walks the fanout).
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

    /// Deletes a local manifest (idempotent).
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
    use nauka_erasure::{encode_file, ErasureConfig};

    #[test]
    fn shard_roundtrip_and_integrity() {
        let dir = tempfile::tempdir().unwrap();
        let store = ShardStore::open(dir.path()).unwrap();

        let hash = store.put_shard(b"hello shard").unwrap();
        assert!(store.has_shard(&hash));
        assert_eq!(store.get_shard(&hash).unwrap(), b"hello shard");

        // Silent on-disk corruption → caught at read time.
        fs::write(store.shard_path(&hash), b"tampered!!").unwrap();
        assert!(matches!(
            store.get_shard(&hash),
            Err(StoreError::CorruptShard { .. })
        ));

        store.delete_shard(&hash).unwrap();
        assert!(!store.has_shard(&hash));
        assert!(matches!(
            store.get_shard(&hash),
            Err(StoreError::ShardNotFound(_))
        ));
    }

    #[test]
    fn manifest_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = ShardStore::open(dir.path()).unwrap();

        let cfg = ErasureConfig {
            data_shards: 2,
            parity_shards: 1,
            shard_size: 64,
        };
        let (manifest, _) = encode_file(b"some file content", &cfg).unwrap();
        store.put_manifest(&manifest).unwrap();

        let loaded = store.get_manifest(&manifest.file_hash).unwrap();
        assert_eq!(loaded.file_hash, manifest.file_hash);
        assert_eq!(loaded.file_size, manifest.file_size);
        assert_eq!(store.list_manifests().unwrap(), vec![manifest.file_hash]);
    }
}
