//! Embedded SurrealDB wrapper used for Nauka's local bootstrap state.
//!
//! Wraps `surrealdb::Surreal<Db>` with a Nauka-friendly lifecycle: a single
//! `open` constructor that creates the on-disk SurrealKV datastore at a given
//! path and selects the `nauka` / `bootstrap` namespace/database (per ADR
//! 0003, sifrah/nauka#190), an accessor for the underlying SDK client, and
//! an explicit `shutdown` step.
//!
//! This is the production backend for Nauka's bootstrap state. The legacy
//! JSON-file store shipped alongside it during P1.2–P1.10; P1.11
//! (sifrah/nauka#201) migrated every caller to `EmbeddedDb`, and P1.12
//! (sifrah/nauka#202) will delete the legacy code outright.
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

use std::fs::{OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

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

        // Acquire the SurrealKV datastore with retry-on-lock-contention.
        //
        // # Why this exists
        //
        // SurrealKV takes an OS-level exclusive flock on `<path>/LOCK`
        // whenever a `Datastore` is open. Only one process can hold the
        // flock at a time. In Nauka, that matters because the fabric
        // layer has concurrent consumers of the same `bootstrap.skv`:
        //
        // - Ad-hoc CLI invocations (`nauka hypervisor status`, etc.)
        // - The `nauka-forge` daemon running reconcile cycles
        // - The `nauka-announce` listener
        //
        // Each of them opens the DB for a short span (read state, do
        // work, `shutdown().await`) but their windows overlap, so an
        // open that happens to collide with another process's brief
        // write window would fail with
        // `Other("Database at ... LOCK is already locked by another process")`.
        //
        // Before P1.11 (sifrah/nauka#201), this was masked because every
        // CLI caller used the JSON-file `LocalDb` backend, which doesn't
        // use an exclusive flock. P1.11 migrates every caller to
        // `EmbeddedDb`, so the contention becomes real.
        //
        // The fix: retry the open on "already locked" with exponential
        // backoff up to a 5-second deadline, matching the pattern used
        // by `shutdown` in `wait_for_surrealkv_lock_release`. 5 s is the
        // same "fast timeout" budget used for health checks — long
        // enough to ride out a forge reconcile cycle that briefly holds
        // the lock, short enough that a stuck holder surfaces as a
        // clear error instead of hanging forever.
        let path_str = path.to_string_lossy().into_owned();
        let inner = open_datastore_with_retry(&path_str).await?;

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

    /// Shut down the wrapper, dropping the SDK client and waiting for the
    /// SurrealDB background router task to finish closing the datastore
    /// so a subsequent `EmbeddedDb::open` at the same path can succeed
    /// — and, critically, so any writes committed through this handle
    /// are durably on disk before we return.
    ///
    /// # Why this function exists
    ///
    /// The SDK's `Surreal<Db>` router runs on a background tokio task
    /// that owns an `Arc<Datastore>`. `drop(Surreal<Db>)` is
    /// fire-and-forget: it closes the route channel, the router task
    /// sees `route_rx.recv()` return `Err`, breaks out of its loop,
    /// and **then** runs `Datastore::shutdown().await` which:
    ///
    /// 1. Shuts down the commit coordinator and WAL background flusher
    /// 2. Calls `Tree::close()` which flushes all memtables to SSTables
    /// 3. Closes the WAL
    /// 4. Drops the `LockFile`, releasing SurrealKV's OS-level exclusive
    ///    flock on `<path>/LOCK`
    ///
    /// None of this is synchronous with `drop(self.inner)`. A caller that
    /// does `shutdown().await; open(same_path).await` races the
    /// background task. Previous versions of this method used
    /// `tokio::time::sleep(50 ms)` as a heuristic — enough on CI Linux,
    /// **not** enough on Mac arm64 under contention, where it manifested
    /// as data loss (`load_async` returning `None` after a round-trip
    /// reopen, reproducible at 77% with `--test-threads=2`). See
    /// sifrah/nauka#255 for the investigation.
    ///
    /// The deterministic signal that step 4 has happened is the release
    /// of the OS-level flock on `<path>/LOCK`. We poll for that release
    /// by trying to acquire the same flock ourselves via
    /// [`std::fs::File::try_lock`]. When `try_lock` succeeds, the
    /// previous `Datastore::shutdown()` chain has definitively run to
    /// completion, which implies every prior commit is durable and the
    /// next open can proceed without a lock-held error.
    ///
    /// # Cost
    ///
    /// Fast path: a single `yield_now` + `try_lock` (~sub-ms). Slow path:
    /// exponential backoff from 1 ms to a 50 ms ceiling, up to a hard
    /// 5-second deadline after which the call returns
    /// [`StateError::Database`]. 5 s is generous relative to the
    /// ~10–100 ms that a real `Datastore::shutdown()` takes even on a
    /// contended runtime, and still well under the "fast timeout" Nauka
    /// convention for health checks.
    pub async fn shutdown(self) -> Result<()> {
        // Explicit drop of the inner client. The compiler would do this
        // anyway when `self` goes out of scope, but naming the step
        // makes the handoff to the router task visible.
        let path = self.path.clone();
        drop(self.inner);

        wait_for_surrealkv_lock_release(&path).await
    }
}

/// Open the SurrealKV datastore at `path_str`, retrying on flock
/// contention until a 5-second deadline elapses.
///
/// See the rationale block in [`EmbeddedDb::open`] for the full context.
/// The short version: SurrealKV holds a process-exclusive flock while a
/// datastore is open, and Nauka's CLI / forge daemon / announce listener
/// all touch the same `bootstrap.skv`, so brief overlaps are normal. We
/// retry the open rather than surface a hard error for every such
/// overlap.
async fn open_datastore_with_retry(path_str: &str) -> Result<Surreal<Db>> {
    /// Upper bound on the total wait. Same "fast timeout" budget as
    /// `shutdown` (see `wait_for_surrealkv_lock_release`). Generous
    /// relative to the real fast-path cost of a forge reconcile cycle
    /// (~10–100 ms of flock-held time) but short enough that a truly
    /// stuck holder surfaces as a clear error instead of a hang.
    const MAX_WAIT: Duration = Duration::from_secs(5);
    /// First retry after an initial miss.
    const MIN_BACKOFF: Duration = Duration::from_millis(5);
    /// Backoff ceiling. Caps worst-case polling pressure at ~20 Hz.
    const MAX_BACKOFF: Duration = Duration::from_millis(50);

    let deadline = Instant::now() + MAX_WAIT;
    let mut backoff = MIN_BACKOFF;

    loop {
        match Surreal::new::<SurrealKv>(path_str).await {
            Ok(db) => return Ok(db),
            Err(err) => {
                // SurrealKV surfaces flock contention as an "other"
                // backend error with a message containing the literal
                // path to `<datastore>/LOCK`. There's no structured
                // variant we can match on, so fall back to a substring
                // check. If the message shape changes in a future
                // surrealkv release the test
                // `open_retries_on_flock_contention` will flag it.
                let msg = err.to_string();
                let is_lock_contention = msg.contains("already locked");

                if !is_lock_contention || Instant::now() >= deadline {
                    return Err(err.into());
                }

                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

/// Poll until SurrealKV's OS-level exclusive flock on `<path>/LOCK` is
/// released by the previous `Datastore`, or until the 5-second deadline
/// elapses.
///
/// See [`EmbeddedDb::shutdown`] for the full rationale. The short version:
/// the SurrealDB SDK's `Surreal<Db>` router runs its `kvs.shutdown()`
/// chain asynchronously after `drop(Surreal<Db>)`, and the release of
/// the `LockFile` (surrealkv 0.21 / `src/lockfile.rs`) is the last step
/// of that chain. Successfully acquiring the same flock from here proves
/// the chain has run to completion.
async fn wait_for_surrealkv_lock_release(path: &Path) -> Result<()> {
    /// Upper bound on the total wait. Generous relative to the actual
    /// `Datastore::shutdown()` cost (10–100 ms typical, even on contended
    /// runtimes) but well under the "fast timeout" Nauka convention.
    const MAX_WAIT: Duration = Duration::from_secs(5);
    /// First retry after the fast-path `try_lock` miss.
    const MIN_BACKOFF: Duration = Duration::from_millis(1);
    /// Backoff ceiling. Caps worst-case polling pressure at ~20 Hz.
    const MAX_BACKOFF: Duration = Duration::from_millis(50);

    let lock_path = path.join("LOCK");
    let deadline = Instant::now() + MAX_WAIT;
    let mut backoff = MIN_BACKOFF;

    loop {
        // Give any already-runnable task (the router exit handler) a
        // chance to make progress before we poll. Cheap in the common
        // case where the router task is already parked.
        tokio::task::yield_now().await;

        // If the LOCK file doesn't exist, the datastore was never
        // initialised far enough to create it (e.g. `open` failed
        // upstream of the LSM tree build). There is nothing to wait
        // for — treat as already released.
        if !lock_path.exists() {
            return Ok(());
        }

        // Try to acquire the OS-level exclusive flock that SurrealKV
        // was holding on the same file. When `try_lock` returns
        // `Ok(())`, the previous datastore's `kvs.shutdown()` chain
        // has definitively run to completion and dropped its
        // `LockFile`; release immediately so a subsequent open() at
        // the same path can acquire it.
        //
        // ENOENT / other `open` errors between the `exists()` check
        // above and this call are benign races: if the error persists
        // the deadline check below will surface it, otherwise the next
        // iteration picks up the real state.
        if let Ok(file) = OpenOptions::new().read(true).write(true).open(&lock_path) {
            match file.try_lock() {
                Ok(()) => {
                    // Success: release our probe lock and return. The
                    // explicit `unlock()` isn't strictly necessary
                    // (dropping `file` closes the fd and releases the
                    // flock) but makes the intent readable.
                    let _ = file.unlock();
                    return Ok(());
                }
                Err(TryLockError::WouldBlock) => {
                    // Router task hasn't reached `LockFile::drop` yet.
                    // Fall through to the backoff sleep.
                }
                Err(TryLockError::Error(io_err)) => {
                    // Real I/O error (not lock contention). Surface it
                    // immediately rather than looping against a broken
                    // filesystem.
                    return Err(io_err.into());
                }
            }
        }

        if Instant::now() >= deadline {
            return Err(StateError::Database(format!(
                "EmbeddedDb::shutdown timed out after {}s waiting for \
                 SurrealKV LOCK release at {}",
                MAX_WAIT.as_secs(),
                lock_path.display(),
            )));
        }

        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
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
            .expect("cross-table select");
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

    /// P1.11 — `EmbeddedDb::open` retries on SurrealKV flock contention.
    ///
    /// Opens the same datastore twice in sequence. The first open is
    /// then shut down (which releases the SurrealKV flock), and we
    /// kick off a parallel open *before* `shutdown` returns. The
    /// parallel open must not fail immediately with "already locked":
    /// the retry loop in `open_datastore_with_retry` should re-try
    /// until the router task's `kvs.shutdown()` chain has released
    /// the flock, then succeed.
    ///
    /// We deliberately DO NOT use `shutdown` on the second handle after
    /// `drop(first)` — the shutdown's probe-lock would race the second
    /// open's flock acquisition and make the test flaky. Only the
    /// second handle is explicitly shut down at the end.
    #[tokio::test]
    async fn open_retries_on_flock_contention() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("contention.skv");

        // First handle — held briefly, then dropped.
        let db_a = EmbeddedDb::open(&path).await.expect("open #1");

        // Spawn the second open before db_a is gone; it should spin in
        // the retry loop until db_a's router releases the flock.
        let path_clone = path.clone();
        let joiner = tokio::spawn(async move {
            EmbeddedDb::open(&path_clone)
                .await
                .expect("second open should succeed after retry")
        });

        // Let the second open spin on the retry loop a bit, then drop
        // db_a (without calling `shutdown` to avoid racing the second
        // open on the probe-lock — see the doc comment above).
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(db_a);

        let db_b = joiner.await.expect("join second open");
        db_b.shutdown().await.expect("shutdown #2");
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
