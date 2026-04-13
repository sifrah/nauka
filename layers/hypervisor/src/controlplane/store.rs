//! Distributed state over the cluster-side [`EmbeddedDb`].
//!
//! `ClusterDb` is a **thin wrapper** around [`nauka_state::EmbeddedDb`]
//! configured with the SurrealDB TiKv backend. It preserves the legacy
//! `put`/`get`/`delete`/`list`/`scan_keys`/`exists`/`batch_put` raw-KV
//! surface so the ~dozen stores that still live on it
//! (`layers/org`, `layers/network`, `layers/compute`) keep compiling
//! unchanged while P2.9–P2.14 (sifrah/nauka#213–#218) migrate them one
//! at a time to native SurrealDB SDK calls.
//!
//! # Why a wrapper, not a direct replacement
//!
//! P2.8 (sifrah/nauka#212) is the bridge step: it swaps the underlying
//! transport from `tikv-client::RawClient` to `EmbeddedDb<TiKv>` without
//! touching any caller. That keeps the risk surface of this PR small
//! — all the "migrate one store to SurrealQL" churn lands in its own
//! dedicated ticket, so each cascade step can be reviewed, tested, and
//! rolled back independently. P2.16 (sifrah/nauka#220) is the one that
//! finally deletes this file once the cascade is done.
//!
//! New code **must not** use `put`/`get`/`delete`/etc. — prefer the
//! [`ClusterDb::embedded`] accessor and go through the native SurrealDB
//! SDK. The legacy methods exist only to keep the in-flight cascade
//! compiling.
//!
//! # Storage shape
//!
//! Each `(namespace, key)` pair maps to one SurrealDB record at
//! `{namespace}:{key}` with the Rust value stored under a single `data`
//! field. Wrapping the value under `data` keeps the JSON bridge free of
//! the auto-added SurrealDB `id` field — the caller's struct can have
//! its own `id` without colliding with the record id.
//!
//! The namespace-as-table convention means the catch-all legacy
//! namespaces (`_reg_v2`, `vm-idx`, `storage/regions`, ...) all live on
//! their own SCHEMALESS tables, separate from the SCHEMAFULL resource
//! tables that P2.5 (sifrah/nauka#209) introduced (`org`, `vpc`, `vm`,
//! ...). SurrealDB is happy to hold SCHEMALESS and SCHEMAFULL tables
//! side by side in the same database, so the migration tickets can
//! move each store to its SCHEMAFULL home incrementally without
//! disturbing the rest.
//!
//! # Usage
//!
//! ```no_run
//! use nauka_hypervisor::controlplane::ClusterDb;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let db = ClusterDb::connect(&["http://[fd01::1]:2379"]).await?;
//! db.put("vms", "vm-001", &serde_json::json!({"name": "web-1"})).await?;
//! let vm: Option<serde_json::Value> = db.get("vms", "vm-001").await?;
//! # Ok(())
//! # }
//! ```

use std::net::Ipv6Addr;

use serde::de::DeserializeOwned;
use serde::Serialize;

use nauka_core::error::NaukaError;
use nauka_state::EmbeddedDb;

/// Distributed KV store, backed by an `EmbeddedDb` on the SurrealDB
/// TiKv engine.
///
/// Cheap to clone: [`EmbeddedDb`] is itself `Clone` (the inner
/// `Surreal<Db>` is reference-counted internally), so every clone
/// shares the same TiKv connection pool.
#[derive(Clone)]
pub struct ClusterDb {
    db: EmbeddedDb,
}

impl ClusterDb {
    /// Connect to a TiKV cluster via PD endpoints.
    ///
    /// `pd_endpoints` are the same `http://[ipv6]:2379` strings that
    /// the rest of Nauka produces via
    /// [`nauka_state::pd_endpoints_for`] and
    /// [`crate::fabric::state::FabricState::pd_endpoints`]. The
    /// wrapper hands them straight to
    /// [`EmbeddedDb::open_tikv`](nauka_state::EmbeddedDb::open_tikv),
    /// which knows how to strip the `http://` prefix and try each
    /// endpoint in order until one answers.
    pub async fn connect(pd_endpoints: &[&str]) -> Result<Self, NaukaError> {
        let db = EmbeddedDb::open_tikv(pd_endpoints)
            .await
            .map_err(|e| NaukaError::internal(format!("TiKV connect failed: {e}")))?;
        Ok(Self { db })
    }

    /// Connect to a TiKV cluster from a slice of mesh IPv6 addresses
    /// plus the PD client port.
    ///
    /// Convenience wrapper around [`Self::connect`] that builds the
    /// `http://[ipv6]:port` list via
    /// [`nauka_state::pd_endpoints_for`]. `controlplane::connect()`
    /// uses this to avoid duplicating the endpoint-formatting dance
    /// at every call site.
    pub async fn connect_from_addresses(
        addrs: &[Ipv6Addr],
        pd_client_port: u16,
    ) -> Result<Self, NaukaError> {
        let endpoints = nauka_state::pd_endpoints_for(addrs, pd_client_port);
        let refs: Vec<&str> = endpoints.iter().map(|s| s.as_str()).collect();
        Self::connect(&refs).await
    }

    /// Borrow the underlying [`EmbeddedDb`] for native SurrealDB SDK
    /// access.
    ///
    /// **Use this for new code.** The legacy
    /// `put`/`get`/`list`/`delete`/`scan_keys`/`exists`/`batch_put`
    /// methods are kept on this type only for the in-flight migration
    /// of P2.9–P2.14 (sifrah/nauka#213–#218); once every caller has
    /// moved to the native SurrealDB SDK, P2.16 (sifrah/nauka#220)
    /// deletes the wrapper entirely.
    pub fn embedded(&self) -> &EmbeddedDb {
        &self.db
    }

    /// Build a `ClusterDb` from a pre-opened [`EmbeddedDb`].
    ///
    /// Test-only constructor: production code goes through
    /// [`Self::connect`] / [`Self::connect_from_addresses`]. Unit
    /// tests use this to wrap a SurrealKV-backed `EmbeddedDb` so the
    /// legacy `put/get/list/...` methods can be exercised without a
    /// live TiKV cluster — the SurrealDB SDK surface is identical
    /// across the SurrealKV and TiKv backends, so the SurrealKV path
    /// is a faithful stand-in for testing the wrapper logic.
    #[cfg(test)]
    pub fn for_tests(db: EmbeddedDb) -> Self {
        Self { db }
    }

    /// Lazily define the SCHEMALESS catch-all table for a legacy
    /// namespace.
    ///
    /// Idempotent thanks to `IF NOT EXISTS`. The namespace is inlined
    /// into the SurrealQL string via [`EscapeIdent`] rather than a
    /// bind parameter because `DEFINE TABLE` resolves its name at
    /// parse time — we cannot bind it.
    ///
    /// Namespaces are always compile-time constants from the caller
    /// side (`"_reg_v2"`, `"storage/regions"`, ...) so the inlining
    /// is not a SurrealQL injection risk. `EscapeIdent` wraps any
    /// non-alphanumeric name in backticks to keep SurrealDB happy
    /// with the legacy `foo/bar`-shaped namespace literals.
    async fn ensure_table(&self, namespace: &str) -> Result<(), NaukaError> {
        let ident = EscapeIdent(namespace);
        let stmt = format!("DEFINE TABLE IF NOT EXISTS {ident} SCHEMALESS");
        self.db
            .client()
            .query(stmt)
            .await
            .map_err(|e| NaukaError::internal(format!("DEFINE TABLE failed: {e}")))?
            .check()
            .map_err(|e| NaukaError::internal(format!("DEFINE TABLE check failed: {e}")))?;
        Ok(())
    }

    /// Put a serializable value.
    ///
    /// Round-trips the value through `serde_json::Value` then hands
    /// it to SurrealDB under a `data` wrapper field, mirroring the
    /// JSON-bridge pattern used by
    /// [`crate::fabric::state::FabricState::save`].
    pub async fn put<T: Serialize>(
        &self,
        namespace: &str,
        key: &str,
        value: &T,
    ) -> Result<(), NaukaError> {
        self.ensure_table(namespace).await?;

        let json = serde_json::to_value(value)
            .map_err(|e| NaukaError::internal(format!("serialize: {e}")))?;

        self.db
            .client()
            .query("UPSERT type::record($tbl, $id) CONTENT { data: $data }")
            .bind(("tbl", namespace.to_string()))
            .bind(("id", key.to_string()))
            .bind(("data", json))
            .await
            .map_err(|e| NaukaError::internal(format!("UPSERT failed: {e}")))?
            .check()
            .map_err(|e| NaukaError::internal(format!("UPSERT check failed: {e}")))?;

        Ok(())
    }

    /// Get a deserializable value.
    ///
    /// Returns `Ok(None)` when the record (or its table) does not
    /// exist. The "table doesn't exist" path matters for read-mostly
    /// callers that hit `get` before anything has ever been written
    /// to the namespace — SurrealDB's SELECT refuses to run against
    /// an undefined table, so we lazily define it first.
    pub async fn get<T: DeserializeOwned>(
        &self,
        namespace: &str,
        key: &str,
    ) -> Result<Option<T>, NaukaError> {
        self.ensure_table(namespace).await?;

        let mut response = self
            .db
            .client()
            .query("SELECT data FROM type::record($tbl, $id)")
            .bind(("tbl", namespace.to_string()))
            .bind(("id", key.to_string()))
            .await
            .map_err(|e| NaukaError::internal(format!("SELECT failed: {e}")))?;

        let row: Option<serde_json::Value> = response
            .take(0)
            .map_err(|e| NaukaError::internal(format!("take row: {e}")))?;

        let Some(row) = row else {
            return Ok(None);
        };

        // The row is `{"data": <value>}`. A missing / null `data`
        // means the record exists but the wrapper content field is
        // absent — treat that as "no value" for the legacy callers
        // that expect `Option<T>`.
        let Some(data) = row.get("data").cloned() else {
            return Ok(None);
        };
        if data.is_null() {
            return Ok(None);
        }

        let value: T = serde_json::from_value(data)
            .map_err(|e| NaukaError::internal(format!("deserialize: {e}")))?;
        Ok(Some(value))
    }

    /// Delete a key. Idempotent: deleting a missing key is `Ok(())`.
    pub async fn delete(&self, namespace: &str, key: &str) -> Result<(), NaukaError> {
        self.ensure_table(namespace).await?;

        self.db
            .client()
            .query("DELETE type::record($tbl, $id)")
            .bind(("tbl", namespace.to_string()))
            .bind(("id", key.to_string()))
            .await
            .map_err(|e| NaukaError::internal(format!("DELETE failed: {e}")))?
            .check()
            .map_err(|e| NaukaError::internal(format!("DELETE check failed: {e}")))?;

        Ok(())
    }

    /// List all values under `{namespace}/{prefix}…`.
    ///
    /// Returns `(key, value)` pairs where `key` is the full record id
    /// within the namespace (same shape as the legacy TiKV/raw-KV
    /// `list` that the callers were written against — keys look like
    /// `"org/abc-123"`, not just `"abc-123"`).
    ///
    /// The implementation fetches every record in the table and
    /// filters client-side on the id prefix. That matches the
    /// fallback the TiKV `scan` path already used because TiKV's raw
    /// scan didn't enforce end-key bounds correctly under the Rust
    /// client's keyspace encoding. Legacy tables are bounded in size
    /// (a few dozen rows at most), so the cost is negligible.
    pub async fn list<T: DeserializeOwned>(
        &self,
        namespace: &str,
        prefix: &str,
    ) -> Result<Vec<(String, T)>, NaukaError> {
        self.ensure_table(namespace).await?;

        let rows = self.fetch_table_rows(namespace).await?;

        let mut results = Vec::new();
        for (key, data) in rows {
            if !key.starts_with(prefix) {
                continue;
            }
            if data.is_null() {
                continue;
            }
            let value: T = serde_json::from_value(data)
                .map_err(|e| NaukaError::internal(format!("deserialize: {e}")))?;
            results.push((key, value));
        }
        Ok(results)
    }

    /// Scan record ids under `{namespace}/{prefix}…`.
    ///
    /// Returns the id-only half of [`Self::list`]. Same prefix /
    /// client-side-filter semantics, same return shape (keys are
    /// full, un-stripped).
    pub async fn scan_keys(
        &self,
        namespace: &str,
        prefix: &str,
    ) -> Result<Vec<String>, NaukaError> {
        self.ensure_table(namespace).await?;

        let rows = self.fetch_table_rows(namespace).await?;
        let keys = rows
            .into_iter()
            .map(|(k, _)| k)
            .filter(|k| k.starts_with(prefix))
            .collect();
        Ok(keys)
    }

    /// Check if a key exists.
    pub async fn exists(&self, namespace: &str, key: &str) -> Result<bool, NaukaError> {
        // `get::<serde_json::Value>` already performs the cheapest
        // existence-check query we have (`SELECT data FROM
        // type::record(...)`) and will return `Ok(None)` for a
        // missing row / table. Piggybacking keeps the two code paths
        // in lockstep — if the `get` query ever changes shape, this
        // follows along for free.
        Ok(self
            .get::<serde_json::Value>(namespace, key)
            .await?
            .is_some())
    }

    /// Batch put multiple key-value pairs.
    ///
    /// The SurrealDB TiKv backend does not expose the tikv-client
    /// `batch_put` primitive, so this issues one `UPSERT` per entry
    /// sequentially. The legacy callers (none in-tree as of P2.8)
    /// only used this for tiny batches, so the throughput loss is
    /// acceptable for the cascade window.
    pub async fn batch_put<T: Serialize>(
        &self,
        namespace: &str,
        entries: &[(&str, &T)],
    ) -> Result<(), NaukaError> {
        for (key, value) in entries {
            self.put(namespace, key, *value).await?;
        }
        Ok(())
    }

    /// Internal: fetch every row in a table as `(id_string, data)`.
    ///
    /// Runs `SELECT record::id(id) AS __key, data FROM type::table($tbl)`
    /// and unpacks the resulting `serde_json::Value` rows into a flat
    /// `Vec`. The `__key` alias pulls just the id portion via
    /// `record::id()` — that sidesteps the SurrealQL ident-escaping
    /// dance (`` `org/abc-123` `` vs `org/abc-123`) that a plain
    /// `SELECT *` would surface through the JSON bridge.
    async fn fetch_table_rows(
        &self,
        namespace: &str,
    ) -> Result<Vec<(String, serde_json::Value)>, NaukaError> {
        let mut response = self
            .db
            .client()
            .query("SELECT record::id(id) AS __key, data FROM type::table($tbl)")
            .bind(("tbl", namespace.to_string()))
            .await
            .map_err(|e| NaukaError::internal(format!("SELECT failed: {e}")))?;

        let rows: Vec<serde_json::Value> = response
            .take(0)
            .map_err(|e| NaukaError::internal(format!("take rows: {e}")))?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let Some(key) = row.get("__key").and_then(|v| v.as_str()) else {
                continue;
            };
            let data = row.get("data").cloned().unwrap_or(serde_json::Value::Null);
            out.push((key.to_string(), data));
        }
        Ok(out)
    }
}

/// SurrealQL-ident escape for catch-all namespace names.
///
/// If the name is a plain alphanumeric/underscore identifier it goes
/// through unquoted; otherwise it is wrapped in backticks (SurrealDB's
/// standard ident quoting, see
/// `surrealdb-types::utils::escape::EscapeSqonIdent`). Backticks in the
/// name itself are backslash-escaped.
///
/// This is only used for `DEFINE TABLE IF NOT EXISTS <name>` statements,
/// where SurrealDB resolves the name at parse time and we cannot bind
/// it as a parameter. Everywhere else we use `type::record($tbl, $id)`
/// / `type::table($tbl)` bind-param helpers.
struct EscapeIdent<'a>(&'a str);

impl std::fmt::Display for EscapeIdent<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.0;
        let needs_quoting = s.is_empty()
            || s.starts_with(|c: char| c.is_ascii_digit())
            || s.contains(|c: char| !c.is_ascii_alphanumeric() && c != '_');

        if !needs_quoting {
            return f.write_str(s);
        }

        f.write_str("`")?;
        for c in s.chars() {
            if c == '`' || c == '\\' {
                f.write_str("\\")?;
            }
            std::fmt::Write::write_char(f, c)?;
        }
        f.write_str("`")
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    /// Small serializable item used across the put/get/list/scan_keys
    /// round-trip tests below. Has its own `id` field to exercise the
    /// `data` wrapper (the wrapper is what lets user structs keep
    /// their own `id` without colliding with the SurrealDB record id).
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Item {
        id: String,
        name: String,
        count: i32,
    }

    /// Open a temp SurrealKV-backed `EmbeddedDb`, wrap it in a
    /// `ClusterDb`, and return the lot.
    ///
    /// Returns `(TempDir, EmbeddedDb, ClusterDb)`. The raw
    /// `EmbeddedDb` is kept around so we can call `shutdown()` on it
    /// at end of test — `EmbeddedDb::shutdown(self)` releases the
    /// SurrealKV OS-level flock on the backing LOCK file, which is a
    /// hard requirement for the harness to not leak temp dirs across
    /// tests. Because `EmbeddedDb` is Arc-backed, cloning it into
    /// `ClusterDb` just bumps the refcount — to actually run the
    /// shutdown chain (drop the inner `Surreal<Db>` router task) we
    /// need the last Arc reference, so callers must `drop(db)` the
    /// `ClusterDb` before awaiting `embedded.shutdown()`.
    ///
    /// [`shutdown_cluster_db`] encapsulates that contract so the
    /// individual test bodies stay readable.
    async fn temp_cluster_db() -> (tempfile::TempDir, EmbeddedDb, ClusterDb) {
        let dir = tempfile::tempdir().expect("tempdir");
        let embedded = EmbeddedDb::open(&dir.path().join("test.skv"))
            .await
            .expect("open EmbeddedDb at temp path");
        let cluster = ClusterDb::for_tests(embedded.clone());
        (dir, embedded, cluster)
    }

    /// Drop a `ClusterDb` and shut down its backing `EmbeddedDb`.
    ///
    /// The drop-before-shutdown order is what lets SurrealKV's LOCK
    /// release: `shutdown(self)` only runs the router's
    /// `kvs.shutdown()` chain when the last Arc reference to the
    /// inner `Surreal<Db>` is dropped, so we have to get rid of the
    /// `ClusterDb` clone first. See the doc on
    /// [`EmbeddedDb::shutdown`](nauka_state::EmbeddedDb::shutdown)
    /// for the full rationale.
    async fn shutdown_cluster_db(db: ClusterDb, embedded: EmbeddedDb) {
        drop(db);
        embedded.shutdown().await.expect("EmbeddedDb shutdown");
    }

    // ─── EscapeIdent (formatter for DEFINE TABLE) ─────────────────

    #[test]
    fn escape_ident_plain() {
        assert_eq!(format!("{}", EscapeIdent("vms")), "vms");
    }

    #[test]
    fn escape_ident_underscore() {
        assert_eq!(format!("{}", EscapeIdent("_reg_v2")), "_reg_v2");
    }

    #[test]
    fn escape_ident_dash_quoted() {
        assert_eq!(format!("{}", EscapeIdent("vm-idx")), "`vm-idx`");
    }

    #[test]
    fn escape_ident_slash_quoted() {
        assert_eq!(
            format!("{}", EscapeIdent("storage/regions")),
            "`storage/regions`"
        );
    }

    #[test]
    fn escape_ident_digit_start_quoted() {
        assert_eq!(format!("{}", EscapeIdent("1foo")), "`1foo`");
    }

    #[test]
    fn escape_ident_empty_quoted() {
        assert_eq!(format!("{}", EscapeIdent("")), "``");
    }

    #[test]
    fn escape_ident_with_backtick() {
        // Real namespaces never contain backticks, but we still
        // escape them correctly for safety.
        assert_eq!(format!("{}", EscapeIdent("a`b")), "`a\\`b`");
    }

    // ─── put/get round-trip (P2.8 acceptance) ─────────────────────

    #[tokio::test]
    async fn put_get_roundtrip_simple_ns() {
        let (_d, embedded, db) = temp_cluster_db().await;
        let item = Item {
            id: "item-1".into(),
            name: "widget".into(),
            count: 42,
        };

        db.put("vms", "item-1", &item).await.unwrap();

        let loaded: Option<Item> = db.get("vms", "item-1").await.unwrap();
        assert_eq!(loaded, Some(item));

        shutdown_cluster_db(db, embedded).await;
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        let (_d, embedded, db) = temp_cluster_db().await;

        let loaded: Option<Item> = db.get("vms", "nope").await.unwrap();
        assert!(loaded.is_none());

        shutdown_cluster_db(db, embedded).await;
    }

    /// P2.8 — `get` against a table that has never been written to
    /// must return `Ok(None)`, not error out. The lazy
    /// `DEFINE TABLE IF NOT EXISTS` in `ensure_table` is what makes
    /// that contract hold.
    #[tokio::test]
    async fn get_from_fresh_table_returns_none() {
        let (_d, embedded, db) = temp_cluster_db().await;

        let loaded: Option<Item> = db.get("never_written", "nope").await.unwrap();
        assert!(loaded.is_none());

        shutdown_cluster_db(db, embedded).await;
    }

    /// The legacy TiKV path stored values under the compound key
    /// `{namespace}/{key}`. The wrapper preserves the two-argument
    /// call shape but now uses `{namespace}` as the SurrealDB table
    /// and `{key}` as the record id. Keys with `/` in them must still
    /// round-trip cleanly — they're used by the `_reg_v2` store
    /// pattern (`_reg_v2 / org/abc-123`).
    #[tokio::test]
    async fn put_get_roundtrip_slash_key() {
        let (_d, embedded, db) = temp_cluster_db().await;
        let item = Item {
            id: "abc-123".into(),
            name: "org-widget".into(),
            count: 7,
        };

        db.put("_reg_v2", "org/abc-123", &item).await.unwrap();

        let loaded: Option<Item> = db.get("_reg_v2", "org/abc-123").await.unwrap();
        assert_eq!(loaded, Some(item));

        shutdown_cluster_db(db, embedded).await;
    }

    /// Namespace that contains `/` (e.g. `storage/regions`) — the
    /// DEFINE TABLE statement needs backtick-escaping for this to
    /// reach SurrealDB in a valid form.
    #[tokio::test]
    async fn put_get_roundtrip_slash_namespace() {
        let (_d, embedded, db) = temp_cluster_db().await;
        let item = Item {
            id: "eu-central".into(),
            name: "fsn1".into(),
            count: 1,
        };

        db.put("storage/regions", "eu-central", &item)
            .await
            .unwrap();
        let loaded: Option<Item> = db.get("storage/regions", "eu-central").await.unwrap();
        assert_eq!(loaded, Some(item));

        shutdown_cluster_db(db, embedded).await;
    }

    /// The second `put` at the same `(ns, key)` must overwrite — the
    /// legacy callers rely on `put` having UPSERT semantics.
    #[tokio::test]
    async fn put_overwrites_previous_value() {
        let (_d, embedded, db) = temp_cluster_db().await;
        let v1 = Item {
            id: "w".into(),
            name: "first".into(),
            count: 1,
        };
        let v2 = Item {
            id: "w".into(),
            name: "second".into(),
            count: 2,
        };

        db.put("vms", "w", &v1).await.unwrap();
        db.put("vms", "w", &v2).await.unwrap();

        let loaded: Option<Item> = db.get("vms", "w").await.unwrap();
        assert_eq!(loaded, Some(v2));

        shutdown_cluster_db(db, embedded).await;
    }

    #[tokio::test]
    async fn delete_removes_entry() {
        let (_d, embedded, db) = temp_cluster_db().await;
        let item = Item {
            id: "goner".into(),
            name: "bye".into(),
            count: 0,
        };

        db.put("vms", "goner", &item).await.unwrap();
        assert!(db.get::<Item>("vms", "goner").await.unwrap().is_some());

        db.delete("vms", "goner").await.unwrap();
        assert!(db.get::<Item>("vms", "goner").await.unwrap().is_none());

        shutdown_cluster_db(db, embedded).await;
    }

    #[tokio::test]
    async fn delete_missing_is_idempotent() {
        let (_d, embedded, db) = temp_cluster_db().await;

        // Delete a key that was never written, twice. Neither call
        // should error.
        db.delete("vms", "never_existed").await.unwrap();
        db.delete("vms", "never_existed").await.unwrap();

        shutdown_cluster_db(db, embedded).await;
    }

    #[tokio::test]
    async fn exists_follows_put_delete() {
        let (_d, embedded, db) = temp_cluster_db().await;
        let item = Item {
            id: "x".into(),
            name: "x".into(),
            count: 1,
        };

        assert!(!db.exists("vms", "x").await.unwrap());
        db.put("vms", "x", &item).await.unwrap();
        assert!(db.exists("vms", "x").await.unwrap());
        db.delete("vms", "x").await.unwrap();
        assert!(!db.exists("vms", "x").await.unwrap());

        shutdown_cluster_db(db, embedded).await;
    }

    // ─── list / scan_keys ─────────────────────────────────────────

    #[tokio::test]
    async fn list_all_under_prefix() {
        let (_d, embedded, db) = temp_cluster_db().await;

        // Two records under `org/`, one under `proj/` — the `org/`
        // prefix filter should only match the first two.
        for id in ["org/a", "org/b", "proj/a"] {
            db.put(
                "_reg_v2",
                id,
                &Item {
                    id: id.into(),
                    name: id.into(),
                    count: 1,
                },
            )
            .await
            .unwrap();
        }

        let mut listed: Vec<(String, Item)> = db.list("_reg_v2", "org/").await.unwrap();
        listed.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].0, "org/a");
        assert_eq!(listed[1].0, "org/b");
        assert_eq!(listed[0].1.name, "org/a");

        shutdown_cluster_db(db, embedded).await;
    }

    #[tokio::test]
    async fn scan_keys_returns_full_ids_under_prefix() {
        let (_d, embedded, db) = temp_cluster_db().await;

        for id in ["org/a", "org/b", "proj/a"] {
            db.put(
                "_reg_v2",
                id,
                &Item {
                    id: id.into(),
                    name: id.into(),
                    count: 1,
                },
            )
            .await
            .unwrap();
        }

        let mut keys = db.scan_keys("_reg_v2", "org/").await.unwrap();
        keys.sort();
        assert_eq!(keys, vec!["org/a".to_string(), "org/b".to_string()]);

        // Same, different prefix.
        let proj = db.scan_keys("_reg_v2", "proj/").await.unwrap();
        assert_eq!(proj, vec!["proj/a".to_string()]);

        // Empty prefix returns everything.
        let all = db.scan_keys("_reg_v2", "").await.unwrap();
        assert_eq!(all.len(), 3);

        shutdown_cluster_db(db, embedded).await;
    }

    #[tokio::test]
    async fn scan_keys_empty_table() {
        let (_d, embedded, db) = temp_cluster_db().await;
        let keys = db.scan_keys("never_touched", "").await.unwrap();
        assert!(keys.is_empty());
        shutdown_cluster_db(db, embedded).await;
    }

    #[tokio::test]
    async fn list_empty_table() {
        let (_d, embedded, db) = temp_cluster_db().await;
        let rows: Vec<(String, Item)> = db.list("never_touched", "").await.unwrap();
        assert!(rows.is_empty());
        shutdown_cluster_db(db, embedded).await;
    }

    #[tokio::test]
    async fn batch_put_round_trip() {
        let (_d, embedded, db) = temp_cluster_db().await;

        let a = Item {
            id: "a".into(),
            name: "alpha".into(),
            count: 1,
        };
        let b = Item {
            id: "b".into(),
            name: "beta".into(),
            count: 2,
        };

        db.batch_put("vms", &[("a", &a), ("b", &b)]).await.unwrap();

        assert_eq!(db.get::<Item>("vms", "a").await.unwrap(), Some(a));
        assert_eq!(db.get::<Item>("vms", "b").await.unwrap(), Some(b));

        shutdown_cluster_db(db, embedded).await;
    }

    // ─── embedded() accessor ──────────────────────────────────────

    /// P2.8 — `ClusterDb::embedded()` returns a usable `EmbeddedDb`
    /// handle that new callers (P2.9+) can use to run native
    /// SurrealDB SDK queries without going through the legacy
    /// `put/get/list/delete` wrapper.
    #[tokio::test]
    async fn embedded_accessor_returns_usable_handle() {
        let (_d, embedded, db) = temp_cluster_db().await;

        // Go through the wrapper first to define the table and
        // write a row.
        db.put(
            "probe",
            "row-1",
            &Item {
                id: "r".into(),
                name: "row".into(),
                count: 3,
            },
        )
        .await
        .unwrap();

        // Now drop down to the native SurrealDB SDK via the
        // `embedded()` accessor and read the same row back via a
        // raw query, proving the handle is live.
        let mut response = db
            .embedded()
            .client()
            .query("SELECT data FROM type::record($tbl, $id)")
            .bind(("tbl", "probe".to_string()))
            .bind(("id", "row-1".to_string()))
            .await
            .unwrap();

        let row: Option<serde_json::Value> = response.take(0).unwrap();
        let row = row.expect("row must exist");
        assert_eq!(
            row.get("data")
                .and_then(|d| d.get("name"))
                .and_then(|n| n.as_str()),
            Some("row")
        );

        shutdown_cluster_db(db, embedded).await;
    }
}
