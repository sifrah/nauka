# nauka-state

State persistence for Nauka. This crate is the only place in the workspace
that knows how Nauka's data is laid out on disk, and it exposes two
storage backends — one local, one distributed — that together cover every
piece of state a Nauka deployment needs.

- [`EmbeddedDb`](src/embedded.rs) — embedded SurrealDB over the SurrealKV
  on-disk engine. Per-node bootstrap state.
- [`ClusterDb`](src/cluster.rs) — TiKV-backed raw KV store for shared
  cluster state. Replaced by `EmbeddedDb` over the `kv-tikv` backend in
  Phase 2 (sifrah/nauka#206 / sifrah/nauka#220).

The two backends never hold the same data: bootstrap state lives locally
on each node, everything else lives in the cluster store. There is no
synchronization or conflict resolution between the tiers.

## Backend 1 — `EmbeddedDb` over SurrealKV

`EmbeddedDb` is the single source of truth for everything a node needs
to read **before** the mesh is up: mesh identity, hypervisor identity,
the WireGuard keypair, the peer list, and the storage region registry.
It is a thin wrapper around `surrealdb::Surreal<Db>` that adds a
Nauka-friendly lifecycle (`open` / `client` / `shutdown`), a fixed
`nauka` / `bootstrap` namespace/database (per ADR 0003,
sifrah/nauka#190), and automatic application of the bootstrap schema.

### On-disk layout

The datastore is a directory of SurrealKV files. Its location depends on
how the binary is invoked:

| Run mode    | Path                              |
|-------------|-----------------------------------|
| CLI mode    | `~/.nauka/bootstrap.skv/`         |
| Service mode (root) | `/var/lib/nauka/bootstrap.skv/` |

The two-mode resolution is performed by
[`nauka_core::process::nauka_db_path`](../core/src/process.rs). Use
[`EmbeddedDb::open_default`](src/embedded.rs) to get the right path
automatically; use [`EmbeddedDb::open`](src/embedded.rs) when you need to
target a specific directory (for example in tests).

The parent state directory is created with mode `0o700` so that only
the owning user can read it. This matters because the database holds
WireGuard private keys and the mesh secret hash.

### Schema

The bootstrap schema lives in [`schemas/bootstrap.surql`](schemas/bootstrap.surql)
and is embedded into the binary at compile time via `include_str!`.
`EmbeddedDb::open` runs it on every open. Every `DEFINE` statement uses
`IF NOT EXISTS`, so re-applying against an already-initialised database
is a no-op and adding new fields in later releases is forward-compatible.

The schema currently defines four SCHEMAFULL tables:

- `mesh` — singleton row at `mesh:current` with the mesh's IPv6 ULA
  prefix, a hash of the mesh secret, and creation timestamp.
- `hypervisor` — this node's identity, keyed by its ULID record id.
  Holds name, mesh IPv6, WireGuard public key, and role.
- `peer` — every other node this one knows about, keyed by mesh IPv6.
  Holds public key, optional public endpoint, and last-seen handshake.
- `wg_key` — this node's WireGuard private/public keypair (singleton).

A SCHEMALESS `fabric` table backs the JSON-bridge `FabricState` used by
the hypervisor layer, and a SCHEMALESS `regions` table backs the storage
region registry.

### Concurrency

SurrealKV holds an OS-level exclusive flock on `<path>/LOCK` while a
`Datastore` is open, so only one process can hold the database at a
time. Nauka has multiple short-lived consumers of the same
`bootstrap.skv` (ad-hoc CLI calls, the `nauka-forge` reconcile loop,
the announce listener), so `EmbeddedDb::open` retries on flock
contention with exponential backoff up to a 5-second deadline, and
`EmbeddedDb::shutdown` polls the same flock until the previous handle's
`Datastore::shutdown()` chain has run to completion. Both deadlines
match the "fast timeout" budget Nauka uses for health checks.

### Code example

```rust
use nauka_state::EmbeddedDb;
use std::path::Path;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open (or create) the SurrealKV datastore. The bootstrap schema
    // is applied automatically. Use `EmbeddedDb::open_default()` to
    // pick up the run-mode-aware default path instead.
    let db = EmbeddedDb::open(Path::new("/var/lib/nauka/bootstrap.skv")).await?;

    // Drop down to the SurrealDB SDK for queries. The wrapper
    // intentionally does not re-export every SDK method.
    let _: Vec<surrealdb::types::Value> = db
        .client()
        .query("SELECT * FROM peer")
        .await?
        .take(0)?;

    // Always shut down explicitly. `shutdown()` waits for SurrealKV's
    // background router to release the flock so a subsequent open at
    // the same path will succeed and every committed write is durably
    // on disk before this returns.
    db.shutdown().await?;
    Ok(())
}
```

## Backend 2 — `ClusterDb` over TiKV

`ClusterDb` is a raw key-value client for a TiKV cluster reachable over
the mesh. It serializes values as JSON under compound keys of the form
`{namespace}/{key}` and is the current home for all shared state that
must be visible across every node: orgs, projects, VMs, VPCs, subnets,
volumes, and so on.

This backend is **transitional**. Phase 2 replaces it with an
`EmbeddedDb` instance configured with the SurrealDB SDK's `kv-tikv`
backend (P2.1 / sifrah/nauka#205, P2.16 / sifrah/nauka#220). After
that migration, both bootstrap and cluster state will go through the
same `EmbeddedDb` API and `ClusterDb` will be deleted. New code should
prefer SurrealQL-flavoured patterns over raw KV ones to make that
transition cheap.

`ClusterDb::connect` takes one or more PD endpoints (TiKV's coordination
service) on the mesh's IPv6 ULA range. PD is responsible for shard
discovery and request routing — callers point at PD, not at individual
TiKV nodes. A typical endpoint looks like `http://[fd01::1]:2379`.

## Errors

Every fallible call returns [`Result<T, StateError>`](src/lib.rs). The
variants are deliberately coarse:

- `StateError::Database` — connection failures, query errors, internal
  engine errors, anything that does not fit the more specific variants.
- `StateError::Schema` — SCHEMAFULL violations, `ASSERT` failures, and
  unique-index conflicts. Mapped from `surrealdb::Error::is_validation()`
  and `is_already_exists()`.
- `StateError::NotFound` — record / table / namespace / database does
  not exist. Mapped from `surrealdb::Error::is_not_found()`.
- `StateError::Serialization` — serde / JSON serialization errors,
  surfaced today by the `ClusterDb` JSON layer and the `EmbeddedDb`
  JSON-bridge tables.
- `StateError::Io` — filesystem-level errors (`From<std::io::Error>`).

The classification of `surrealdb::Error` into these variants is
conservative: only the three explicit predicates above get specific
variants, everything else collapses to `Database`.

## Constants

The SurrealDB namespace and database names are exported as constants so
no call site inlines the string literal:

- `NAUKA_NS = "nauka"` — the only namespace Nauka writes to.
- `BOOTSTRAP_DB = "bootstrap"` — the local SurrealKV database.
- `CLUSTER_DB = "cluster"` — reserved for the Phase 2 TiKV-backed
  `EmbeddedDb` constructor.

## File layout

```
layers/state/
├── README.md
├── Cargo.toml
├── schemas/
│   └── bootstrap.surql      # SCHEMAFULL DDL applied by EmbeddedDb::open
└── src/
    ├── lib.rs               # StateError, constants, re-exports
    ├── embedded.rs          # EmbeddedDb (SurrealKV)
    └── cluster.rs           # ClusterDb (TiKV, transitional)
```
