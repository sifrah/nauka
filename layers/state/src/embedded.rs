//! Embedded SurrealDB wrapper used for Nauka's local bootstrap state.
//!
//! Wraps `surrealdb::Surreal<Db>` with a Nauka-friendly lifecycle: a single
//! `open` constructor that creates the on-disk SurrealKV datastore at a given
//! path and selects the `nauka` / `bootstrap` namespace/database (per ADR
//! 0003, sifrah/nauka#190), an accessor for the underlying SDK client, and
//! an explicit `shutdown` step.
//!
//! This is the long-term replacement for [`crate::LocalDb`] (the
//! JSON-file-backed bootstrap store). The migration of every existing
//! `LocalDb` call site is tracked by P1.10 → P1.12 (sifrah/nauka#200,
//! sifrah/nauka#201, sifrah/nauka#202). Until those land, both backends
//! coexist.
//!
//! P1.2 (sifrah/nauka#192) introduces only the wrapper struct and its
//! lifecycle. The companion deliverables — error mapping (P1.3,
//! sifrah/nauka#193), default-path helpers (P1.4, sifrah/nauka#194),
//! comprehensive CRUD/persistence tests (P1.5, sifrah/nauka#195), the
//! initial `.surql` schema (P1.6, sifrah/nauka#196), and applying that
//! schema at open time (P1.7, sifrah/nauka#197) — each ship as their own
//! issue.
//!
//! # Example
//!
//! ```no_run
//! # use nauka_state::EmbeddedDb;
//! # use std::path::Path;
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let db = EmbeddedDb::open(Path::new("/var/lib/nauka/bootstrap.skv")).await?;
//!
//! // Use the underlying SurrealDB SDK directly:
//! let _: Vec<surrealdb::types::Value> = db
//!     .client()
//!     .query("INFO FOR DB")
//!     .await?
//!     .take(0)?;
//!
//! db.shutdown().await?;
//! # Ok(()) }
//! ```

use std::path::{Path, PathBuf};

use surrealdb::engine::local::{Db, SurrealKv};
use surrealdb::Surreal;

use crate::{Result, BOOTSTRAP_DB, NAUKA_NS};

/// Embedded SurrealDB instance, persisted to disk via the SurrealKV backend.
///
/// Cheap to clone: every clone shares the same underlying `Surreal<Db>`
/// connection (which itself uses an `Arc` internally).
#[derive(Clone)]
pub struct EmbeddedDb {
    inner: Surreal<Db>,
    path: PathBuf,
}

impl EmbeddedDb {
    /// Open (or create) the SurrealKV-backed database at `path` and select
    /// the `nauka` / `bootstrap` namespace/database.
    ///
    /// The parent directory is created if it doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Io`] if the parent directory cannot be created,
    /// or one of [`StateError::NotFound`] / [`StateError::Schema`] /
    /// [`StateError::Database`] (depending on the underlying SurrealDB
    /// failure mode) if SurrealDB cannot open the datastore or switch to
    /// the configured namespace/database. The classification is done by the
    /// `From<surrealdb::Error>` impl in `lib.rs` (P1.3, sifrah/nauka#193).
    pub async fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }

        let path_str = path.to_string_lossy().into_owned();
        let inner = Surreal::new::<SurrealKv>(path_str.as_str()).await?;

        inner.use_ns(NAUKA_NS).use_db(BOOTSTRAP_DB).await?;

        Ok(Self {
            inner,
            path: path.to_path_buf(),
        })
    }

    /// Borrow the underlying SurrealDB SDK client.
    ///
    /// All SurrealQL queries flow through this. The wrapper does not
    /// re-export every SDK method — call sites that need the full SDK
    /// surface go through `db.client().query(...)` etc.
    pub fn client(&self) -> &Surreal<Db> {
        &self.inner
    }

    /// Path of the on-disk SurrealKV datastore.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Shut down the wrapper, dropping the SDK client.
    ///
    /// Dropping the inner [`Surreal<Db>`] is what actually closes the
    /// datastore — the SurrealDB SDK's background router task exits
    /// when its sender side goes away, and the SurrealKV engine flushes
    /// pending writes on drop. This method exists so call sites have an
    /// explicit, named way to do that, instead of relying on `Drop` order.
    ///
    /// Returns `Ok(())` once the inner client has been dropped. Future
    /// versions may flush, fsync, or wait for outstanding tasks here, so
    /// the signature is async + `Result` even though the body is trivial
    /// today.
    pub async fn shutdown(self) -> Result<()> {
        // Explicit drop of the inner client. The compiler would do this
        // anyway when `self` goes out of scope, but naming the step makes
        // the intent visible at every call site.
        drop(self.inner);
        Ok(())
    }
}

impl std::fmt::Debug for EmbeddedDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbeddedDb")
            .field("path", &self.path)
            .field("ns", &NAUKA_NS)
            .field("db", &BOOTSTRAP_DB)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// P1.2 smoke test: open the wrapper at a temp path and shut it down.
    ///
    /// Comprehensive CRUD / persistence tests live in P1.5
    /// (sifrah/nauka#195). This one only covers the lifecycle skeleton
    /// the issue actually adds.
    #[tokio::test]
    async fn open_and_shutdown_smoke() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("smoke.skv");

        let db = EmbeddedDb::open(&path)
            .await
            .expect("open should succeed at a writable temp path");

        assert_eq!(db.path(), path);

        // The on-disk datastore should now exist (SurrealKV creates a
        // directory at the given path).
        assert!(path.exists(), "datastore path should exist after open");

        // Confirm the SDK client returned a usable handle by issuing a
        // no-side-effect query.
        let _ = db
            .client()
            .query("INFO FOR DB")
            .await
            .expect("INFO FOR DB should succeed against the freshly-opened bootstrap db");

        db.shutdown()
            .await
            .expect("shutdown should always succeed in P1.2");
    }
}
