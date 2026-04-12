#![allow(clippy::result_large_err)]
//! State persistence for Nauka.
//!
//! Three backends, in increasing order of where they live:
//! - **`EmbeddedDb`** — embedded SurrealDB on disk via the SurrealKV backend.
//!   Used for **bootstrap** state that must be readable before any cluster
//!   exists (mesh identity, hypervisor identity, peers, WireGuard keys).
//!   This is the long-term replacement for `LocalDb`.
//! - **`LocalDb`** — legacy JSON file store. Still in use; will be retired
//!   by P1.10–P1.12 (sifrah/nauka#200 → sifrah/nauka#202).
//! - **`ClusterDb`** — TiKV-backed distributed raw KV store for shared state
//!   (orgs, projects, VMs, VPCs, etc.). Will be retired by P2.16
//!   (sifrah/nauka#220) once the SurrealDB SDK in `kv-tikv` mode (P2.x) takes
//!   over.
//!
//! # SurrealDB namespace / database conventions
//!
//! Per ADR 0003 (sifrah/nauka#190):
//!
//! - Local SurrealKV: namespace `nauka`, database `bootstrap`
//! - Distributed TiKV (Phase 2): namespace `nauka`, database `cluster`
//!
//! These literals are exported as the [`NAUKA_NS`], [`BOOTSTRAP_DB`], and
//! [`CLUSTER_DB`] constants so call sites never inline the strings.
//!
//! # Usage — `EmbeddedDb` (SurrealKV-backed bootstrap state)
//!
//! ```no_run
//! # use nauka_state::EmbeddedDb;
//! # use std::path::Path;
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let db = EmbeddedDb::open(Path::new("/var/lib/nauka/bootstrap.skv")).await?;
//! db.client().query("INFO FOR DB").await?;
//! db.shutdown().await?;
//! # Ok(()) }
//! ```
//!
//! # Usage — `LocalDb` (legacy JSON, to be retired)
//!
//! ```no_run
//! use nauka_state::LocalDb;
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct Peer { name: String }
//!
//! let db = LocalDb::open("fabric").unwrap();
//! db.set("state", "main", &Peer { name: "node-1".into() }).unwrap();
//! let peer: Option<Peer> = db.get("state", "main").unwrap();
//! ```

pub mod cluster;
pub mod embedded;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde::de::DeserializeOwned;
use serde::Serialize;

pub use cluster::ClusterDb;
pub use embedded::EmbeddedDb;

// ═══════════════════════════════════════════════════
// SurrealDB namespace / database constants (ADR 0003)
// ═══════════════════════════════════════════════════

/// SurrealDB namespace used by all Nauka SurrealDB instances.
///
/// Per ADR 0003 (sifrah/nauka#190), Nauka writes only to this single
/// namespace. Multi-tenancy lives at the row level inside the cluster
/// database, not at the namespace level.
pub const NAUKA_NS: &str = "nauka";

/// SurrealDB database name for the local SurrealKV-backed bootstrap state.
///
/// Used by [`EmbeddedDb::open`].
pub const BOOTSTRAP_DB: &str = "bootstrap";

/// SurrealDB database name for the distributed TiKV-backed cluster state.
///
/// Reserved for the Phase 2 TiKV-backed `EmbeddedDb` constructor (P2.2,
/// sifrah/nauka#206). Defined here in P1.2 so the constants live in one
/// place and the cluster-side issue can refer to the existing symbol.
pub const CLUSTER_DB: &str = "cluster";

// ═══════════════════════════════════════════════════
// Errors
// ═══════════════════════════════════════════════════

/// Error type returned by every fallible nauka-state operation.
///
/// Variants are deliberately coarse-grained: callers either care about the
/// specific failure mode (e.g. `NotFound` vs everything else) or they
/// propagate the error opaquely. Anything finer than these four variants
/// stays in the inner message string.
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    /// A SurrealDB-side or generic backend error: connection failures,
    /// query errors, internal engine errors, anything that does not fit
    /// the more specific variants below.
    #[error("database error: {0}")]
    Database(String),

    /// A schema-level error: a `DEFINE TABLE` constraint was violated, an
    /// `ASSERT` clause failed, a unique index conflicted, a SCHEMAFULL field
    /// type mismatched, etc. The inner string is the human-readable detail.
    ///
    /// Mapped from `surrealdb::Error` variants where the engine reports
    /// `is_validation()` (parse / invalid params / SCHEMAFULL violations) or
    /// `is_already_exists()` (unique-index conflicts during writes).
    #[error("schema error: {0}")]
    Schema(String),

    /// A "record / table / namespace / database does not exist" error.
    ///
    /// Mapped from `surrealdb::Error::is_not_found()`.
    #[error("not found: {0}")]
    NotFound(String),

    /// A serde / JSON serialization error from the legacy `LocalDb` and
    /// `ClusterDb` paths. Will go away when those layers are deleted in
    /// P1.12 (sifrah/nauka#202) and P2.16 (sifrah/nauka#220).
    #[error("serialization error: {0}")]
    Serialization(String),

    /// A filesystem-level error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<surrealdb::Error> for StateError {
    /// Best-effort classification of `surrealdb::Error` into the right
    /// `StateError` variant.
    ///
    /// The mapping is conservative: only `is_not_found()`, `is_validation()`,
    /// and `is_already_exists()` get specific variants. Everything else
    /// (query errors, connection errors, internal errors, thrown errors,
    /// not-allowed, configuration, serialization-on-the-wire) becomes
    /// `Database` because callers shouldn't need to distinguish them — they
    /// either want to know "did the record exist?" / "did the schema
    /// reject this?" or they propagate the error opaquely.
    fn from(err: surrealdb::Error) -> Self {
        let msg = format!("{err}");
        let details = err.details();
        if details.is_not_found() {
            StateError::NotFound(msg)
        } else if details.is_validation() || details.is_already_exists() {
            StateError::Schema(msg)
        } else {
            StateError::Database(msg)
        }
    }
}

pub type Result<T> = std::result::Result<T, StateError>;

// ═══════════════════════════════════════════════════
// LocalDb — JSON file store for bootstrap state
// ═══════════════════════════════════════════════════

fn nauka_dir() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".nauka")
}

/// Store layout: `~/.nauka/{layer}.json`
/// Internal format: `{ "table/key": <json_value>, ... }`
type StoreMap = HashMap<String, serde_json::Value>;

/// Local JSON file-backed store. Used for bootstrap state that must
/// be available before TiKV starts (mesh identity, WG keys, peers).
///
/// Thread-safe via `Arc<Mutex<...>>`. Clone is cheap.
#[derive(Clone, Debug)]
pub struct LocalDb {
    path: PathBuf,
    data: Arc<Mutex<StoreMap>>,
}

impl LocalDb {
    /// Open (or create) a local store for a layer.
    /// Stores at `~/.nauka/{layer}.json`.
    pub fn open(layer: &str) -> Result<Self> {
        let dir = nauka_dir();
        std::fs::create_dir_all(&dir)?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700));
        }

        let path = dir.join(format!("{layer}.json"));
        Self::open_at(&path)
    }

    /// Open at a specific path.
    pub fn open_at(path: &std::path::Path) -> Result<Self> {
        let data = if path.exists() {
            let contents = std::fs::read_to_string(path)?;
            serde_json::from_str(&contents).map_err(|e| StateError::Serialization(e.to_string()))?
        } else {
            StoreMap::new()
        };

        #[cfg(unix)]
        if path.exists() {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
        }

        Ok(Self {
            path: path.to_path_buf(),
            data: Arc::new(Mutex::new(data)),
        })
    }

    /// Set a value (serialized to JSON).
    pub fn set<T: Serialize>(&self, table: &str, key: &str, value: &T) -> Result<()> {
        let json_value =
            serde_json::to_value(value).map_err(|e| StateError::Serialization(e.to_string()))?;

        let compound_key = format!("{table}/{key}");

        let mut data = self.data.lock().unwrap_or_else(|e| e.into_inner());
        data.insert(compound_key, json_value);
        self.flush(&data)
    }

    /// Get a value (deserialized from JSON).
    pub fn get<T: DeserializeOwned>(&self, table: &str, key: &str) -> Result<Option<T>> {
        let compound_key = format!("{table}/{key}");
        let data = self.data.lock().unwrap_or_else(|e| e.into_inner());

        match data.get(&compound_key) {
            Some(val) => {
                let parsed = serde_json::from_value(val.clone())
                    .map_err(|e| StateError::Serialization(e.to_string()))?;
                Ok(Some(parsed))
            }
            None => Ok(None),
        }
    }

    /// Delete a key.
    pub fn delete(&self, table: &str, key: &str) -> Result<()> {
        let compound_key = format!("{table}/{key}");
        let mut data = self.data.lock().unwrap_or_else(|e| e.into_inner());
        data.remove(&compound_key);
        self.flush(&data)
    }

    /// Check if a key exists.
    pub fn exists(&self, table: &str, key: &str) -> Result<bool> {
        let compound_key = format!("{table}/{key}");
        let data = self.data.lock().unwrap_or_else(|e| e.into_inner());
        Ok(data.contains_key(&compound_key))
    }

    /// Flush to disk.
    fn flush(&self, data: &StoreMap) -> Result<()> {
        let json = serde_json::to_string_pretty(data)
            .map_err(|e| StateError::Serialization(e.to_string()))?;
        std::fs::write(&self.path, json)?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&self.path, std::fs::Permissions::from_mode(0o600));
        }

        Ok(())
    }
}

// Backward compat alias — hypervisor code uses LayerDb
pub type LayerDb = LocalDb;

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestPeer {
        name: String,
        zone: String,
    }

    #[test]
    fn open_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.json");
        let db = LocalDb::open_at(&path).unwrap();
        db.set(
            "peers",
            "n1",
            &TestPeer {
                name: "n1".into(),
                zone: "fsn1".into(),
            },
        )
        .unwrap();
        assert!(path.exists());
    }

    #[test]
    fn set_and_get() {
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open_at(&dir.path().join("test.json")).unwrap();

        let peer = TestPeer {
            name: "node-1".into(),
            zone: "fsn1".into(),
        };
        db.set("peers", "n1", &peer).unwrap();

        let loaded: Option<TestPeer> = db.get("peers", "n1").unwrap();
        assert_eq!(loaded, Some(peer));
    }

    #[test]
    fn get_missing() {
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open_at(&dir.path().join("test.json")).unwrap();
        let result: Option<TestPeer> = db.get("peers", "nope").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn delete_key() {
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open_at(&dir.path().join("test.json")).unwrap();

        db.set("peers", "n1", &"value").unwrap();
        assert!(db.exists("peers", "n1").unwrap());

        db.delete("peers", "n1").unwrap();
        assert!(!db.exists("peers", "n1").unwrap());
    }

    #[test]
    fn exists_check() {
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open_at(&dir.path().join("test.json")).unwrap();

        assert!(!db.exists("peers", "n1").unwrap());
        db.set("peers", "n1", &"value").unwrap();
        assert!(db.exists("peers", "n1").unwrap());
    }

    #[test]
    fn persistence_across_opens() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.json");

        {
            let db = LocalDb::open_at(&path).unwrap();
            db.set(
                "state",
                "main",
                &TestPeer {
                    name: "n1".into(),
                    zone: "fsn1".into(),
                },
            )
            .unwrap();
        }

        {
            let db = LocalDb::open_at(&path).unwrap();
            let loaded: Option<TestPeer> = db.get("state", "main").unwrap();
            assert_eq!(loaded.unwrap().name, "n1");
        }
    }

    #[test]
    fn overwrite_value() {
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open_at(&dir.path().join("test.json")).unwrap();

        db.set("k", "v", &"first").unwrap();
        db.set("k", "v", &"second").unwrap();

        let val: Option<String> = db.get("k", "v").unwrap();
        assert_eq!(val, Some("second".into()));
    }

    #[test]
    fn multiple_tables() {
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open_at(&dir.path().join("test.json")).unwrap();

        db.set("peers", "n1", &"peer-data").unwrap();
        db.set("mesh", "id", &"mesh-data").unwrap();

        let p: Option<String> = db.get("peers", "n1").unwrap();
        let m: Option<String> = db.get("mesh", "id").unwrap();
        assert_eq!(p, Some("peer-data".into()));
        assert_eq!(m, Some("mesh-data".into()));
    }

    #[test]
    fn serde_roundtrip_complex() {
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open_at(&dir.path().join("test.json")).unwrap();

        let data = serde_json::json!({
            "name": "node-1",
            "peers": ["n2", "n3"],
            "nested": { "key": "value" },
        });
        db.set("state", "main", &data).unwrap();

        let loaded: Option<serde_json::Value> = db.get("state", "main").unwrap();
        assert_eq!(loaded.unwrap()["name"], "node-1");
    }

    #[test]
    fn open_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.json");
        // Don't create the file — open should create it
        let db = LocalDb::open_at(&path).unwrap();
        assert!(!db.exists("any", "key").unwrap());
    }

    #[test]
    fn clone_is_cheap() {
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open_at(&dir.path().join("test.json")).unwrap();
        db.set("k", "v", &"value").unwrap();

        let db2 = db.clone();
        let val: Option<String> = db2.get("k", "v").unwrap();
        assert_eq!(val, Some("value".into()));
    }

    // ─── Error mapping (P1.3, sifrah/nauka#193) ──────────────────────

    #[test]
    fn error_mapping_not_found() {
        // surrealdb::Error::not_found is the canonical "record/table/ns/db
        // does not exist" constructor. It must round-trip into our
        // StateError::NotFound variant.
        let surreal_err =
            surrealdb::Error::not_found("record `vm:does_not_exist`".to_string(), None);
        let state_err: StateError = surreal_err.into();
        assert!(
            matches!(state_err, StateError::NotFound(_)),
            "expected NotFound, got: {state_err:?}"
        );
    }

    #[test]
    fn error_mapping_validation_to_schema() {
        // SCHEMAFULL constraint failures, parse errors, and ASSERT failures
        // all surface as `Error::validation`. They map to StateError::Schema
        // because the caller's recourse is "fix the schema or the input",
        // not "retry against the backend".
        let surreal_err =
            surrealdb::Error::validation("DEFINE FIELD ... ASSERT failed".to_string(), None);
        let state_err: StateError = surreal_err.into();
        assert!(
            matches!(state_err, StateError::Schema(_)),
            "expected Schema, got: {state_err:?}"
        );
    }

    #[test]
    fn error_mapping_already_exists_to_schema() {
        // Unique-index conflicts on writes are also schema-level signals
        // for the caller (the constraint exists, the input violates it).
        let surreal_err =
            surrealdb::Error::already_exists("unique index `vm_name` violated".to_string(), None);
        let state_err: StateError = surreal_err.into();
        assert!(
            matches!(state_err, StateError::Schema(_)),
            "expected Schema, got: {state_err:?}"
        );
    }

    #[test]
    fn error_mapping_internal_to_database() {
        // Internal/connection/query/etc. errors all collapse to Database.
        // Internal is the most "catch-all" of the surrealdb variants and
        // is the right canary for the default-mapping path.
        let surreal_err = surrealdb::Error::internal("transaction conflict".to_string());
        let state_err: StateError = surreal_err.into();
        assert!(
            matches!(state_err, StateError::Database(_)),
            "expected Database, got: {state_err:?}"
        );
    }

    #[test]
    fn error_mapping_connection_to_database() {
        // Connection errors are operationally distinct ("the cluster is
        // gone") but the call site doesn't get a different recovery path
        // — it has to retry or surface the error to the user. Database
        // is the right bucket.
        let surreal_err = surrealdb::Error::connection("router uninitialised".to_string(), None);
        let state_err: StateError = surreal_err.into();
        assert!(
            matches!(state_err, StateError::Database(_)),
            "expected Database, got: {state_err:?}"
        );
    }

    #[test]
    fn error_mapping_question_mark_via_into() {
        // Sanity check that the `?` operator picks up the From impl
        // automatically (i.e. that it's a `From`, not a one-off helper).
        fn returns_state_result() -> Result<()> {
            Err(surrealdb::Error::not_found("missing".to_string(), None))?;
            Ok(())
        }
        let err = returns_state_result().unwrap_err();
        assert!(matches!(err, StateError::NotFound(_)));
    }

    #[test]
    fn error_mapping_preserves_message() {
        // The inner String of each variant should carry the original
        // message so logs and CLI output stay readable.
        let surreal_err =
            surrealdb::Error::not_found("record `vm:abc` does not exist".to_string(), None);
        let state_err: StateError = surreal_err.into();
        let rendered = format!("{state_err}");
        assert!(
            rendered.contains("vm:abc"),
            "rendered error should keep the original detail, got: {rendered}"
        );
    }
}
