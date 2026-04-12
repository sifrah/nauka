//! Spike binary for P0.1 (sifrah/nauka#185), repurposed in P1.2 to be the
//! Hetzner-side smoke test for [`nauka_state::EmbeddedDb`].
//!
//! The spike's purpose evolves with each phase that touches the same code:
//!
//! - **P0.1**: prove that `surrealdb` 3.0.5 with `kv-surrealkv` (+ `kv-tikv`)
//!   cross-compiles to `x86_64-unknown-linux-musl` and runs as a
//!   statically-linked binary on a real Hetzner Ubuntu host. ✅ done.
//! - **P1.1**: drop `kv-tikv` from the production feature set. ✅ done.
//! - **P1.2** (this version): exercise the new [`EmbeddedDb`] wrapper
//!   end-to-end on Hetzner — open at a temp path, run a CRUD round-trip
//!   via `db.client()`, shut down cleanly. The wrapper is the production
//!   code path that all of Phase 1 builds on, so an actual run on Hetzner
//!   is the strongest signal that the cross-compile + the wrapper + the
//!   SDK still all agree about how to talk to SurrealKV.
//!
//! What this binary does on each run:
//! 1. Print build / runtime info
//! 2. `EmbeddedDb::open` at `$TMPDIR/nauka-p0-1-spike.skv`
//! 3. CRUD round-trip via the wrapped client (`create`, `select id`,
//!    `select all`)
//! 4. `EmbeddedDb::shutdown`
//! 5. Wipe the temp datastore so re-runs are clean

use std::path::PathBuf;

use nauka_state::EmbeddedDb;
use surrealdb::types::SurrealValue;

#[derive(Debug, SurrealValue)]
struct SpikeRecord {
    name: String,
    answer: i32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("== nauka p0-1 spike (P1.2 — EmbeddedDb wrapper) ==");
    println!("target_arch    = {}", std::env::consts::ARCH);
    println!("target_os      = {}", std::env::consts::OS);
    println!("target_env     = {}", std::env::consts::FAMILY);
    println!("surrealdb_dep  = 3.0.5 (kv-surrealkv only)");

    // Open the wrapper at a temp path.
    let path: PathBuf = std::env::temp_dir().join("nauka-p0-1-spike.skv");
    println!("skv_path       = {}", path.display());

    let db = EmbeddedDb::open(&path).await?;
    println!("ns/db          = nauka/bootstrap (auto-selected by EmbeddedDb::open)");

    // Round-trip via the underlying SDK client.
    let client = db.client();

    let created: Option<SpikeRecord> = client
        .create(("spike_record", "first"))
        .content(SpikeRecord {
            name: "p0-1".into(),
            answer: 42,
        })
        .await?;
    println!("created        = {created:?}");

    let fetched: Option<SpikeRecord> = client.select(("spike_record", "first")).await?;
    println!("fetched        = {fetched:?}");

    let all: Vec<SpikeRecord> = client.select("spike_record").await?;
    println!("all_count      = {}", all.len());

    // Explicit shutdown via the wrapper, then wipe the datastore directory
    // so re-running the spike on the same Hetzner box is idempotent.
    db.shutdown().await?;
    let _ = std::fs::remove_dir_all(&path);

    println!("== p0-1 spike OK ==");
    Ok(())
}
