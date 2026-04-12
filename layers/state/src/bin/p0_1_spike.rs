//! Spike binary for P0.1 (#185).
//!
//! Goal: prove that `surrealdb` 3.0.5 with `kv-surrealkv` + `kv-tikv` features
//! can be cross-compiled to `x86_64-unknown-linux-musl` and that the resulting
//! binary runs on a real Hetzner Ubuntu host.
//!
//! What this does:
//! 1. Print build/runtime info (target, surrealdb version)
//! 2. Open an in-process SurrealKV datastore at a temp path
//! 3. Run a CRUD round-trip with SurrealQL
//! 4. Confirm both code paths (`SurrealKv` engine marker + the `TiKv` import)
//!    actually link into the binary
//!
//! It does NOT connect to TiKV — that's P0.3 (#187). Linking the symbol is
//! enough to validate the build chain for the spike.

use std::path::PathBuf;

use surrealdb::engine::local::{SurrealKv, TiKv};
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
    println!("surrealdb_dep  = 3.0.5 (kv-surrealkv + kv-tikv)");

    // Force the TiKv marker to be referenced so the linker keeps the symbol.
    // We don't actually open a TiKv connection (that needs a real cluster).
    let _tikv_marker = std::any::type_name::<TiKv>();
    println!("tikv_marker    = {_tikv_marker}");

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
