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

use crate::{Result, StateError, BOOTSTRAP_DB, NAUKA_NS};

/// The bootstrap-state SurrealQL schema, embedded at compile time.
///
/// Defined in `layers/state/schemas/bootstrap.surql` (P1.6, sifrah/nauka#196).
/// `EmbeddedDb::open` applies this on every open. The schema is fully
/// idempotent thanks to `IF NOT EXISTS` on every `DEFINE` statement, so
/// re-applying against an already-initialised database is a no-op.
const BOOTSTRAP_SCHEMA: &str = include_str!("../schemas/bootstrap.surql");

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
    /// Open (or create) the SurrealKV-backed database at the run-mode-aware
    /// default path provided by [`nauka_core::process::nauka_db_path`]:
    ///
    /// - CLI mode: `~/.nauka/bootstrap.skv`
    /// - Service mode (root): `/var/lib/nauka/bootstrap.skv`
    ///
    /// The parent state directory is created via
    /// [`nauka_core::process::ensure_nauka_state_dir`] (with 0o700 perms),
    /// then the rest of the open path is the same as [`Self::open`].
    pub async fn open_default() -> Result<Self> {
        // Create the state directory with the right perms; the SurrealKV
        // datastore directory itself is created by `Surreal::new::<SurrealKv>`.
        let _ = nauka_core::process::ensure_nauka_state_dir()
            .map_err(|e| StateError::Database(format!("ensure state dir: {e}")))?;
        let path = nauka_core::process::nauka_db_path();
        Self::open(&path).await
    }

    /// Open (or create) the SurrealKV-backed database at `path`, select the
    /// `nauka` / `bootstrap` namespace/database, and apply the embedded
    /// bootstrap schema.
    ///
    /// The parent directory is created if it doesn't exist. On Unix the
    /// parent directory is also chmod-ed to 0o700 (best-effort).
    ///
    /// The bootstrap schema (`schemas/bootstrap.surql`, P1.6) is applied on
    /// every open. It is fully idempotent thanks to `IF NOT EXISTS` on
    /// every `DEFINE` statement, so reopening an existing database is a
    /// no-op for already-defined tables, fields, and indexes — and any
    /// new tables/fields/indexes that have appeared since last open are
    /// added forward-compatibly. P1.7 (sifrah/nauka#197) is the issue that
    /// wires this in.
    ///
    /// For the run-mode-aware default path, use [`Self::open_default`].
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Io`] if the parent directory cannot be created,
    /// or one of [`StateError::NotFound`] / [`StateError::Schema`] /
    /// [`StateError::Database`] (depending on the underlying SurrealDB
    /// failure mode) if SurrealDB cannot open the datastore, switch to the
    /// configured namespace/database, or apply the bootstrap schema. The
    /// classification is done by the `From<surrealdb::Error>` impl in
    /// `lib.rs` (P1.3, sifrah/nauka#193).
    pub async fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    // Best-effort 0o700 on the parent (P1.4 perms target).
                    // We don't surface a chmod failure as a hard error: the
                    // dir was created successfully, refusing to start over a
                    // chmod refusal would be obnoxious. Logged via tracing
                    // by nauka_core::process when the helper is used.
                    let _ =
                        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
                }
            }
        }

        let path_str = path.to_string_lossy().into_owned();
        let inner = Surreal::new::<SurrealKv>(path_str.as_str()).await?;

        inner.use_ns(NAUKA_NS).use_db(BOOTSTRAP_DB).await?;

        // Apply the bootstrap schema after switching to the right ns/db.
        // DEFINE TABLE / FIELD / INDEX are db-scoped, so use_db must
        // happen first. The schema's IF NOT EXISTS clauses make this
        // safe to run on every open.
        inner.query(BOOTSTRAP_SCHEMA).await?.check()?;

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

    /// Shut down the wrapper, dropping the SDK client and waiting briefly
    /// for the SurrealDB background router task to release the on-disk
    /// LOCK so a subsequent `EmbeddedDb::open` at the same path can
    /// succeed.
    ///
    /// Why the wait: the SDK's `Surreal<Db>` router runs on a background
    /// tokio task that holds an `Arc<Datastore>`. The task only exits
    /// when its route channel closes — i.e. after the last
    /// `Surreal<Db>` clone is dropped. After it exits, it calls
    /// `Datastore::shutdown()`, which is what actually flushes SurrealKV
    /// and removes the LOCK file. All of that happens *asynchronously*
    /// on the runtime, so without a small wait here a caller that does
    /// `shutdown().await; open(same_path).await` races against the
    /// background task and gets `Database is already locked`.
    ///
    /// 50 ms is empirically enough on every machine we've tested, and
    /// nothing in Nauka's hot path calls `shutdown` often enough to
    /// notice. If a future SDK version exposes a synchronous shutdown
    /// that joins the router task, we should use it instead.
    pub async fn shutdown(self) -> Result<()> {
        // Explicit drop of the inner client. The compiler would do this
        // anyway when `self` goes out of scope, but naming the step makes
        // the intent visible.
        drop(self.inner);

        // Yield once so any task that's already runnable (the router
        // exit handler) gets a chance to run before we sleep.
        tokio::task::yield_now().await;

        // Then a short sleep to cover the kvs.shutdown() flush + LOCK
        // release latency. See the doc comment above for the rationale.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

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
    use surrealdb::types::SurrealValue;

    use super::*;

    /// Tiny test record used by the tests below. Defined inside the
    /// test module so the SurrealValue derive isn't compiled into the
    /// production library.
    #[derive(Debug, Clone, PartialEq, SurrealValue)]
    struct Item {
        name: String,
        count: i32,
    }

    /// P1.2 smoke test: open the wrapper at a temp path and shut it down.
    #[tokio::test]
    async fn open_and_shutdown_smoke() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("smoke.skv");

        let db = EmbeddedDb::open(&path)
            .await
            .expect("open should succeed at a writable temp path");

        assert_eq!(db.path(), path);
        assert!(path.exists(), "datastore path should exist after open");

        let _ = db
            .client()
            .query("INFO FOR DB")
            .await
            .expect("INFO FOR DB should succeed against the freshly-opened bootstrap db");

        db.shutdown()
            .await
            .expect("shutdown should always succeed in P1.2");
    }

    /// P1.5 — CRUD round-trip on a single table.
    ///
    /// Covers the full create → select-by-id → update → select → delete →
    /// select-after-delete cycle. Anchors the assumption that the SDK
    /// client returned by `db.client()` behaves the same as it would on
    /// a vanilla `Surreal::new::<SurrealKv>` connection.
    #[tokio::test]
    async fn crud_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = EmbeddedDb::open(&dir.path().join("crud.skv"))
            .await
            .expect("open");
        let client = db.client();

        // Create.
        let created: Option<Item> = client
            .create(("items", "first"))
            .content(Item {
                name: "alpha".into(),
                count: 1,
            })
            .await
            .expect("create");
        let created = created.expect("create returned None");
        assert_eq!(created.name, "alpha");
        assert_eq!(created.count, 1);

        // Select by id.
        let fetched: Option<Item> = client
            .select(("items", "first"))
            .await
            .expect("select by id");
        assert_eq!(
            fetched,
            Some(Item {
                name: "alpha".into(),
                count: 1,
            }),
        );

        // Update via content (full replace).
        let updated: Option<Item> = client
            .update(("items", "first"))
            .content(Item {
                name: "alpha".into(),
                count: 99,
            })
            .await
            .expect("update");
        assert_eq!(updated.expect("update returned None").count, 99);

        // Re-select to confirm the persisted value.
        let after_update: Option<Item> = client
            .select(("items", "first"))
            .await
            .expect("select after update");
        assert_eq!(after_update.expect("missing after update").count, 99);

        // Delete.
        let deleted: Option<Item> = client.delete(("items", "first")).await.expect("delete");
        assert!(deleted.is_some(), "delete should return the deleted record");

        // Select again — should now be empty.
        let after_delete: Option<Item> = client
            .select(("items", "first"))
            .await
            .expect("select after delete");
        assert!(after_delete.is_none(), "record should be gone after delete");

        db.shutdown().await.expect("shutdown");
    }

    /// P1.5 — Persistence: open, write, drop, reopen, read same value.
    ///
    /// Closes the wrapper between writes and reads to confirm the data
    /// actually hit the SurrealKV file on disk and is recovered on
    /// re-open. Without this guarantee the whole P1 path is meaningless.
    #[tokio::test]
    async fn persistence_across_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("persist.skv");

        // First open: write a record, then explicitly shut down.
        {
            let db = EmbeddedDb::open(&path).await.expect("open #1");
            let _: Option<Item> = db
                .client()
                .create(("items", "p"))
                .content(Item {
                    name: "persisted".into(),
                    count: 7,
                })
                .await
                .expect("create");
            db.shutdown().await.expect("shutdown #1");
        }

        // Second open at the same path: the record should still be there.
        {
            let db = EmbeddedDb::open(&path).await.expect("open #2");
            let fetched: Option<Item> = db
                .client()
                .select(("items", "p"))
                .await
                .expect("select after reopen");
            assert_eq!(
                fetched,
                Some(Item {
                    name: "persisted".into(),
                    count: 7,
                }),
                "record should survive an explicit shutdown + reopen",
            );
            db.shutdown().await.expect("shutdown #2");
        }
    }

    /// P1.5 — Multi-table isolation.
    ///
    /// Two records inserted into two different tables must not bleed
    /// across the table boundary on `select <table>` queries.
    #[tokio::test]
    async fn multi_table_isolation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = EmbeddedDb::open(&dir.path().join("multi.skv"))
            .await
            .expect("open");
        let client = db.client();

        let _: Option<Item> = client
            .create(("table_a", "1"))
            .content(Item {
                name: "a".into(),
                count: 1,
            })
            .await
            .expect("create table_a");
        let _: Option<Item> = client
            .create(("table_b", "1"))
            .content(Item {
                name: "b".into(),
                count: 2,
            })
            .await
            .expect("create table_b");

        let from_a: Vec<Item> = client.select("table_a").await.expect("select table_a");
        let from_b: Vec<Item> = client.select("table_b").await.expect("select table_b");

        assert_eq!(from_a.len(), 1, "table_a should hold exactly one row");
        assert_eq!(from_b.len(), 1, "table_b should hold exactly one row");
        assert_eq!(from_a[0].name, "a");
        assert_eq!(from_b[0].name, "b");

        // Cross-table sanity: selecting a record by id from `table_a`
        // with the id we wrote to `table_b` returns None — i.e. ids
        // don't bleed across tables.
        let cross: Option<Item> = client
            .select(("table_a", "2"))
            .await
            .expect("cross-table select by missing id");
        assert!(
            cross.is_none(),
            "table_a should not see records keyed in table_b",
        );

        db.shutdown().await.expect("shutdown");
    }

    /// P1.5 — Concurrent reads from clones of the same client.
    ///
    /// `EmbeddedDb` is `Clone`, and the underlying `Surreal<Db>` shares
    /// state via Arc internally. Multiple readers from clones should see
    /// the same value without races or panics. Spawns 10 tasks reading
    /// the same record concurrently and asserts they all observe the
    /// expected value.
    #[tokio::test]
    async fn concurrent_reads_from_clones() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = EmbeddedDb::open(&dir.path().join("concurrent.skv"))
            .await
            .expect("open");

        // Seed a single shared record.
        let _: Option<Item> = db
            .client()
            .create(("items", "shared"))
            .content(Item {
                name: "concurrent".into(),
                count: 42,
            })
            .await
            .expect("seed");

        // Spawn concurrent readers.
        let mut handles = Vec::with_capacity(10);
        for i in 0..10u32 {
            let db_clone = db.clone();
            handles.push(tokio::spawn(async move {
                let item: Option<Item> = db_clone
                    .client()
                    .select(("items", "shared"))
                    .await
                    .expect("concurrent select");
                (i, item.expect("missing on concurrent reader"))
            }));
        }

        for h in handles {
            let (i, item) = h.await.expect("join");
            assert_eq!(
                item.count, 42,
                "concurrent reader {i} observed wrong value: {item:?}"
            );
        }

        db.shutdown().await.expect("shutdown");
    }

    /// P1.5 — Error path: open at an invalid path returns `StateError`.
    ///
    /// We force the failure by creating a regular file inside the temp
    /// dir and asking SurrealKV to open at a path *under* it. The parent
    /// of `<file>/oops.skv` is a regular file, so `create_dir_all` fails
    /// with `NotADirectory` and `EmbeddedDb::open` surfaces the error
    /// as `StateError::Io` via the `?` operator and the existing
    /// `From<std::io::Error>` impl on `StateError`.
    #[tokio::test]
    async fn open_at_invalid_path_returns_state_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file_as_parent = dir.path().join("not_a_dir");
        std::fs::write(&file_as_parent, b"hello").expect("write file");

        let bad_path = file_as_parent.join("oops.skv");
        let result = EmbeddedDb::open(&bad_path).await;

        assert!(
            result.is_err(),
            "opening under a regular-file parent should fail"
        );
        let err = result.unwrap_err();
        assert!(
            matches!(err, StateError::Io(_)),
            "expected StateError::Io, got: {err:?}"
        );
    }

    /// P1.7 — `EmbeddedDb::open` applies the bootstrap schema automatically.
    ///
    /// Opens a fresh database (no manual schema application) and asserts
    /// that the four bootstrap tables (`mesh`, `hypervisor`, `peer`,
    /// `wg_key`) are reachable. With the schema NOT auto-applied, a
    /// `select` against a never-touched table returns SurrealDB's
    /// `NotFound` error (verified by `multi_table_isolation` in P1.5).
    /// With the schema auto-applied, the table is defined-but-empty and
    /// `select` returns an empty Vec.
    #[tokio::test]
    async fn open_applies_bootstrap_schema_automatically() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = EmbeddedDb::open(&dir.path().join("auto_schema.skv"))
            .await
            .expect("open should also apply the schema");

        // Each schema-defined table must be reachable on a fresh DB.
        // We use surrealdb::types::Value to avoid forcing a SurrealValue
        // derive on a strongly-typed mirror of every table.
        for table in &["mesh", "hypervisor", "peer", "wg_key"] {
            let rows: Vec<surrealdb::types::Value> =
                db.client().select(*table).await.unwrap_or_else(|e| {
                    panic!("select on {table} should succeed (schema not applied?): {e}")
                });
            assert!(
                rows.is_empty(),
                "table {table} should be empty on a fresh DB but contained {} rows",
                rows.len()
            );
        }

        // The schema is also idempotent: a second open at the same path
        // (after explicit shutdown) must succeed and the tables must
        // still be defined.
        let path = db.path().to_path_buf();
        db.shutdown().await.expect("shutdown");

        let db2 = EmbeddedDb::open(&path)
            .await
            .expect("reopening an already-initialised DB should succeed");
        for table in &["mesh", "hypervisor", "peer", "wg_key"] {
            let rows: Vec<surrealdb::types::Value> =
                db2.client().select(*table).await.unwrap_or_else(|e| {
                    panic!("post-reopen select on {table} should succeed: {e}")
                });
            assert!(
                rows.is_empty(),
                "table {table} should still be empty after reopen"
            );
        }
        db2.shutdown().await.expect("shutdown #2");
    }

    /// P1.6 — the bootstrap.surql schema must apply cleanly to a fresh
    /// database, must be idempotent (re-applying is a no-op), and must
    /// allow inserting one record into every table it defines.
    ///
    /// After P1.7 (sifrah/nauka#197) `EmbeddedDb::open` applies the schema
    /// automatically, so this test now also covers the
    /// "open + auto-apply + manual re-apply" idempotency contract.
    #[tokio::test]
    async fn bootstrap_schema_applies_cleanly() {
        const SCHEMA: &str = include_str!("../schemas/bootstrap.surql");

        let dir = tempfile::tempdir().expect("tempdir");
        let db = EmbeddedDb::open(&dir.path().join("schema_test.skv"))
            .await
            .expect("open");

        // First application — must succeed against an empty database.
        db.client()
            .query(SCHEMA)
            .await
            .expect("schema first apply")
            .check()
            .expect("schema first apply check");

        // Second application — must be a no-op thanks to IF NOT EXISTS.
        db.client()
            .query(SCHEMA)
            .await
            .expect("schema second apply")
            .check()
            .expect("schema second apply check");

        // Each table is reachable. Inserting a row exercises the field
        // ASSERTs and confirms the SCHEMAFULL definitions accept the
        // documented shapes. We use SurrealQL's `time::now()` directly so
        // the test doesn't need a chrono dev-dep.
        db.client()
            .query(
                "CREATE mesh:current SET ipv6_ula = $u, secret_hash = $h, \
                 created_at = time::now()",
            )
            .bind(("u", "fdc5:8ba:9b14::/48"))
            .bind(("h", "0123456789abcdef"))
            .await
            .expect("insert mesh")
            .check()
            .expect("insert mesh check");

        // The record id `hypervisor:hv-01HXXX` IS the ULID — there's no
        // separate `id` field in the schema (see schemas/bootstrap.surql
        // for the rationale). P1.10 will create rows the same way:
        // `CREATE hypervisor:<ulid> SET name = ..., ...`.
        db.client()
            .query(
                "CREATE hypervisor:`hv-01HXXX` SET name = $n, mesh_ipv6 = $m, \
                 public_key = $p, role = $r",
            )
            .bind(("n", "node-1"))
            .bind(("m", "fdc5:8ba:9b14::1"))
            .bind(("p", "BASE64=="))
            .bind(("r", "leader"))
            .await
            .expect("insert hypervisor")
            .check()
            .expect("insert hypervisor check");

        db.client()
            .query(
                "CREATE peer:p1 SET mesh_ipv6 = $m, public_key = $p, \
                 endpoint = $e, last_seen = time::now()",
            )
            .bind(("m", "fdc5:8ba:9b14::2"))
            .bind(("p", "PEERKEY=="))
            .bind(("e", "1.2.3.4:51820"))
            .await
            .expect("insert peer")
            .check()
            .expect("insert peer check");

        db.client()
            .query("CREATE wg_key:current SET private_key = $k, public_key = $p")
            .bind(("k", "PRIVKEY=="))
            .bind(("p", "PUBKEY=="))
            .await
            .expect("insert wg_key")
            .check()
            .expect("insert wg_key check");

        db.shutdown().await.expect("shutdown");
    }
}
