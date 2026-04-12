//! Spike binary for P0.3 (#187).
//!
//! Goal: prove that the SurrealDB SDK with `kv-tikv` (using
//! `surrealdb-tikv-client v0.3.0-surreal.4`) can connect to the existing
//! Nauka PD/TiKV v8.5.5 cluster and successfully run a CRUD round-trip via
//! SurrealQL.
//!
//! Usage:
//!     p0-3-spike <pd_endpoint>
//!
//! Where <pd_endpoint> is `host:port` for one PD member, e.g.
//! `[fdc5:8ba:9b14:6e94:9cbb:1220:ed:af72]:2379`. The TiKv engine takes a
//! single PD address and discovers the rest of the cluster on its own.
//!
//! What this does:
//!   1. Open a `Surreal<Db>` client with the `TiKv` engine pointing at the
//!      given PD endpoint.
//!   2. `use_ns("p0_3_spike").use_db("test")` so we don't pollute any real
//!      Nauka namespace.
//!   3. Run a CRUD round-trip: create → select-by-id → select-all → delete.
//!   4. Measure and print latencies for each step.
//!   5. Exit 0 on success, non-zero on failure.

use std::time::Instant;

use surrealdb::engine::local::TiKv;
use surrealdb::types::SurrealValue;
use surrealdb::Surreal;

#[derive(Debug, SurrealValue)]
struct SpikeRecord {
    name: String,
    answer: i32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pd = std::env::args()
        .nth(1)
        .ok_or_else(|| "usage: p0-3-spike <pd_endpoint>  (e.g. [fdc5:...]:2379)".to_string())?;

    println!("== nauka p0-3 spike (kv-tikv compat check) ==");
    println!("target_arch    = {}", std::env::consts::ARCH);
    println!("target_os      = {}", std::env::consts::OS);
    println!("surrealdb_dep  = 3.0.5 (kv-tikv via surrealdb-tikv-client v0.3.0-surreal.4)");
    println!("pd_endpoint    = {pd}");

    // Step 1: connect.
    let t0 = Instant::now();
    let db = Surreal::new::<TiKv>(pd.as_str()).await?;
    println!("connect_ms     = {}", t0.elapsed().as_millis());

    let t1 = Instant::now();
    db.use_ns("p0_3_spike").use_db("test").await?;
    println!("use_ns_db_ms   = {}", t1.elapsed().as_millis());

    // Step 2: create a record with a known id so we can clean up.
    let t2 = Instant::now();
    let created: Option<SpikeRecord> = db
        .create(("p0_3_record", "first"))
        .content(SpikeRecord {
            name: "p0-3".into(),
            answer: 42,
        })
        .await?;
    println!("create_ms      = {}", t2.elapsed().as_millis());
    println!("create_value   = {:?}", created);

    // Step 3: read it back by id.
    let t3 = Instant::now();
    let fetched: Option<SpikeRecord> = db.select(("p0_3_record", "first")).await?;
    println!("select_id_ms   = {}", t3.elapsed().as_millis());
    println!("select_id_val  = {:?}", fetched);
    if fetched.is_none() {
        return Err("select-by-id returned None — round-trip failed".into());
    }

    // Step 4: select all (should contain at least 1).
    let t4 = Instant::now();
    let all: Vec<SpikeRecord> = db.select("p0_3_record").await?;
    println!("select_all_ms  = {}", t4.elapsed().as_millis());
    println!("select_all_n   = {}", all.len());

    // Step 5: cleanup the test record so re-runs are clean.
    let t5 = Instant::now();
    let _deleted: Option<SpikeRecord> = db.delete(("p0_3_record", "first")).await?;
    println!("delete_ms      = {}", t5.elapsed().as_millis());

    println!("total_ms       = {}", t0.elapsed().as_millis());
    println!("== p0-3 spike OK ==");
    Ok(())
}
