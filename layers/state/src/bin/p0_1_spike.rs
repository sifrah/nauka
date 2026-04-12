//! Spike binary for P0.1 (#185), kept around as a re-validation tool for the
//! Phase 1 SurrealKV build chain.
//!
//! Goal: prove that `surrealdb` 3.0.5 with the `kv-surrealkv` feature can be
//! cross-compiled to `x86_64-unknown-linux-musl` and that the resulting
//! statically-linked binary runs on a real Hetzner Ubuntu host.
//!
//! What this does:
//! 1. Print build/runtime info (target, surrealdb version)
//! 2. Open an in-process SurrealKV datastore at a temp path
//! 3. Run a CRUD round-trip with SurrealQL
//!
//! Originally (P0.1) this binary also imported `surrealdb::engine::local::TiKv`
//! purely to keep the linker symbol alive and prove both backends could
//! coexist in the build graph. P1.1 (sifrah/nauka#191) drops `kv-tikv` from
//! the production feature set, so the TiKv import is gone. The P0.3 spike
//! (`p0-3-spike`) still exercises that backend, but it now requires the
//! local `spike-tikv` feature to compile.

use std::path::PathBuf;

use surrealdb::engine::local::SurrealKv;
use surrealdb::types::SurrealValue;
use surrealdb::Surreal;

#[derive(Debug, SurrealValue)]
struct SpikeRecord {
    name: String,
    answer: i32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("== nauka p0-1 spike ==");
    println!("target_arch    = {}", std::env::consts::ARCH);
    println!("target_os      = {}", std::env::consts::OS);
    println!("target_env     = {}", std::env::consts::FAMILY);
    println!("surrealdb_dep  = 3.0.5 (kv-surrealkv only — P1.1)");

    // Open a SurrealKV datastore at a temp path.
    let path: PathBuf = std::env::temp_dir().join("nauka-p0-1-spike.skv");
    let path_str = path.to_string_lossy().into_owned();
    println!("skv_path       = {path_str}");

    let db = Surreal::new::<SurrealKv>(path_str.as_str()).await?;
    db.use_ns("nauka").use_db("spike").await?;

    // Round-trip: create → select.
    let created: Option<SpikeRecord> = db
        .create(("spike_record", "first"))
        .content(SpikeRecord {
            name: "p0-1".into(),
            answer: 42,
        })
        .await?;
    println!("created        = {:?}", created);

    let fetched: Option<SpikeRecord> = db.select(("spike_record", "first")).await?;
    println!("fetched        = {:?}", fetched);

    let all: Vec<SpikeRecord> = db.select("spike_record").await?;
    println!("all_count      = {}", all.len());

    // Cleanup the temp dir so reruns are clean.
    drop(db);
    let _ = std::fs::remove_dir_all(&path);

    println!("== p0-1 spike OK ==");
    Ok(())
}
