//! Hetzner end-to-end spike for P2.4 (sifrah/nauka#208).
//!
//! Goal: prove that `EmbeddedDb::open_tikv` — the Phase-2 cluster-side
//! constructor added by P2.2 (sifrah/nauka#206) — can talk to a real
//! PD/TiKV v8.5.5 cluster running on the same Hetzner node and
//! round-trip a record through the SurrealDB SDK.
//!
//! The acceptance criterion from the ticket is:
//!
//! > First end-to-end test: from a Hetzner node with PD/TiKV v8.5.5
//! > already running, open a SurrealDB SDK client and round-trip a
//! > record.
//! > - `EmbeddedDb::open_tikv` returns a usable client
//! > - CREATE + SELECT + DELETE round-trip works
//! > - Latency is sane (< 50ms p50 for a single-key write)
//!
//! What this binary does on each run:
//!
//! 1. Print build / runtime / target info for the PR body.
//! 2. Open the local `EmbeddedDb` at its default path
//!    (`/var/lib/nauka/bootstrap.skv` in service mode), load the
//!    FabricState blob directly from the `fabric:state` row, and
//!    build the canonical PD endpoint list via
//!    [`nauka_state::pd_endpoints_for`].
//! 3. Open `EmbeddedDb::open_tikv(&endpoints)` against the live
//!    PD/TiKV cluster.
//! 4. Run a CREATE → SELECT → UPDATE → DELETE round-trip on a
//!    throwaway `p2_4_test:rec1` record, asserting every intermediate
//!    state.
//! 5. Run 20 single-key writes in a tight loop, sort the latencies,
//!    assert that the p50 is **strictly less than 50 ms**. If it
//!    exceeds the threshold the binary fails loudly — that's a real
//!    signal worth surfacing.
//! 6. Clean up the 20-key fixture, shut the TiKv handle down, shut
//!    the bootstrap handle down, exit 0.
//!
//! Returns non-zero on any failure. The test runner captures stdout
//! into the PR description.
//!
//! The spike stays in `nauka-state` so it doesn't have to take a
//! dependency on `nauka-hypervisor` (which would be an upward layer
//! dependency and therefore forbidden). FabricState is read directly
//! from the `fabric:state` row as raw JSON and only the two fields
//! the spike actually needs — `hypervisor.mesh_ipv6` and
//! `peers.peers[*].mesh_ipv6` — are extracted. The canonical
//! `FabricState::pd_endpoints` helper (P2.3 sifrah/nauka#207) is the
//! reference implementation for the same contract inside the
//! hypervisor layer.

use std::net::Ipv6Addr;
use std::time::{Duration, Instant};

use nauka_core::process::{is_service_mode, nauka_db_path};
use nauka_state::{pd_endpoints_for, EmbeddedDb};
use surrealdb::types::SurrealValue;

/// PD client port (matches `nauka_hypervisor::controlplane::PD_CLIENT_PORT`).
/// Hard-coded here to keep the spike layer-clean — redefining a constant
/// is cheaper than an upward dependency on the hypervisor crate.
const PD_CLIENT_PORT: u16 = 2379;

/// Throwaway record type used for the CRUD round-trip. Kept in the
/// spike binary so the production library never compiles a
/// `SurrealValue` derive that only exists to serve a Hetzner smoke
/// test.
#[derive(Debug, Clone, PartialEq, SurrealValue)]
struct SpikeRecord {
    name: String,
    answer: i32,
}

/// Table used for the main round-trip record (CREATE/SELECT/UPDATE/DELETE).
const TEST_TABLE: &str = "p2_4_test";
/// Record id for the single shared round-trip row.
const TEST_RECORD: &str = "rec1";
/// Table used for the latency-loop writes. Deliberately distinct from
/// the main round-trip table so the final assertion on the main
/// record's lifecycle doesn't get polluted by loop rows.
const LATENCY_TABLE: &str = "p2_4_latency";
/// Number of single-key writes to measure in the latency loop.
const LATENCY_ITERATIONS: usize = 20;
/// p50 latency threshold from the ticket acceptance criteria.
const P50_THRESHOLD: Duration = Duration::from_millis(50);
/// Hard upper bound on any TiKv operation in this spike. Generous
/// relative to the real path (single-digit ms on a hot PD leader on
/// the mesh) but short enough that a hang surfaces as a loud failure
/// instead of a silent test-runner timeout.
const OP_TIMEOUT: Duration = Duration::from_secs(30);

fn print_help() {
    println!("nauka p2-4 spike — EmbeddedDb<TiKv> Hetzner end-to-end round-trip");
    println!();
    println!("Exercises EmbeddedDb::open_tikv against the local PD/TiKV cluster");
    println!("discovered through FabricState on a real Hetzner node. Must run");
    println!("as root in service mode so the default bootstrap path");
    println!("(/var/lib/nauka/bootstrap.skv) is reachable.");
    println!();
    println!("USAGE:");
    println!("    p2-4-spike [FLAGS]");
    println!();
    println!("FLAGS:");
    println!("    -h, --help    Print this help and exit (no side effects)");
    println!();
    println!("The binary exits 0 on success, non-zero on any failure. On");
    println!("success the CRUD round-trip and the 20-iteration latency loop");
    println!("both pass, and the reported p50 is strictly < 50 ms.");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(arg) = std::env::args().nth(1) {
        match arg.as_str() {
            "-h" | "--help" => {
                print_help();
                return Ok(());
            }
            other => {
                eprintln!("error: unknown flag: {other}");
                eprintln!("hint:  run `--help` for usage");
                std::process::exit(2);
            }
        }
    }

    println!("== nauka p2-4 spike (EmbeddedDb<TiKv> Hetzner round-trip) ==");
    println!("target_arch    = {}", std::env::consts::ARCH);
    println!("target_os      = {}", std::env::consts::OS);
    println!("target_env     = {}", std::env::consts::FAMILY);
    println!("surrealdb_dep  = 3.0.5 (kv-surrealkv + kv-tikv)");
    println!(
        "run_mode       = {}",
        if is_service_mode() {
            "service (root)"
        } else {
            "cli (user)"
        }
    );
    println!("bootstrap_path = {}", nauka_db_path().display());

    // ─── Phase 1: open bootstrap db, load PD endpoints ────────────
    println!("--- Phase 1: load FabricState for PD discovery ---");
    let bootstrap = EmbeddedDb::open_default().await?;
    println!("bootstrap_open = OK");

    let mesh_addrs = load_pd_mesh_addrs(&bootstrap).await?;
    println!("self_mesh_ipv6 = {}", mesh_addrs[0]);
    println!("peer_count     = {}", mesh_addrs.len() - 1);

    let endpoints = pd_endpoints_for(&mesh_addrs, PD_CLIENT_PORT);
    let endpoint_refs: Vec<&str> = endpoints.iter().map(|s| s.as_str()).collect();
    println!("pd_endpoints   = {endpoint_refs:?}");

    // Release the local bootstrap flock before we touch the cluster —
    // keeps the subsequent `open_tikv` decoupled from any local write
    // that might race with a forge reconcile cycle on the same node.
    bootstrap.shutdown().await?;
    println!("bootstrap_shut = OK");

    // ─── Phase 2: open_tikv against the live cluster ──────────────
    println!("--- Phase 2: EmbeddedDb::open_tikv ---");
    let t_connect = Instant::now();
    let tikv = tokio::time::timeout(OP_TIMEOUT, EmbeddedDb::open_tikv(&endpoint_refs))
        .await
        .map_err(|_| format!("open_tikv timed out after {}s", OP_TIMEOUT.as_secs()))??;
    println!("open_tikv_ms   = {}", t_connect.elapsed().as_millis());
    println!("tikv_handle    = {tikv:?}");

    // ─── Phase 3: CRUD round-trip ────────────────────────────────
    println!("--- Phase 3: CRUD round-trip on {TEST_TABLE}:{TEST_RECORD} ---");
    let client = tikv.client();

    // Belt-and-braces cleanup from any prior crashed run. Ignore the
    // result — a fresh cluster has nothing to delete.
    let _: Option<SpikeRecord> = client
        .delete((TEST_TABLE, TEST_RECORD))
        .await
        .unwrap_or(None);

    // CREATE.
    let t_create = Instant::now();
    let created: Option<SpikeRecord> = client
        .create((TEST_TABLE, TEST_RECORD))
        .content(SpikeRecord {
            name: "p2-4".into(),
            answer: 42,
        })
        .await?;
    println!("create_ms      = {}", t_create.elapsed().as_millis());
    let created = created.ok_or("CREATE returned None")?;
    assert_eq!(created.name, "p2-4", "CREATE returned the wrong name");
    assert_eq!(created.answer, 42, "CREATE returned the wrong answer");

    // SELECT — exactly what was written.
    let t_select = Instant::now();
    let fetched: Option<SpikeRecord> = client.select((TEST_TABLE, TEST_RECORD)).await?;
    println!("select_ms      = {}", t_select.elapsed().as_millis());
    let fetched = fetched.ok_or("SELECT returned None after CREATE")?;
    assert_eq!(
        fetched, created,
        "SELECT returned a different record than CREATE"
    );

    // UPDATE — full-content replace.
    let t_update = Instant::now();
    let updated: Option<SpikeRecord> = client
        .update((TEST_TABLE, TEST_RECORD))
        .content(SpikeRecord {
            name: "p2-4".into(),
            answer: 99,
        })
        .await?;
    println!("update_ms      = {}", t_update.elapsed().as_millis());
    let updated = updated.ok_or("UPDATE returned None")?;
    assert_eq!(updated.answer, 99, "UPDATE did not persist the new value");

    // SELECT again — confirm the UPDATE made it to disk.
    let after_update: Option<SpikeRecord> = client.select((TEST_TABLE, TEST_RECORD)).await?;
    let after_update = after_update.ok_or("SELECT returned None after UPDATE")?;
    assert_eq!(
        after_update.answer, 99,
        "post-UPDATE SELECT still sees old value"
    );

    // DELETE.
    let t_delete = Instant::now();
    let deleted: Option<SpikeRecord> = client.delete((TEST_TABLE, TEST_RECORD)).await?;
    println!("delete_ms      = {}", t_delete.elapsed().as_millis());
    let _ = deleted.ok_or("DELETE returned None")?;

    // SELECT after DELETE — must be gone.
    let after_delete: Option<SpikeRecord> = client.select((TEST_TABLE, TEST_RECORD)).await?;
    assert!(
        after_delete.is_none(),
        "record still visible after DELETE: {after_delete:?}"
    );

    println!("crud_round_trip= OK");

    // ─── Phase 4: latency loop (20 single-key writes) ────────────
    println!("--- Phase 4: latency loop ({LATENCY_ITERATIONS} single-key writes) ---");
    let mut samples: Vec<Duration> = Vec::with_capacity(LATENCY_ITERATIONS);
    for i in 0..LATENCY_ITERATIONS {
        let id = format!("lat_{i:03}");
        let start = Instant::now();
        let _: Option<SpikeRecord> = tokio::time::timeout(
            OP_TIMEOUT,
            client
                .create((LATENCY_TABLE, id.as_str()))
                .content(SpikeRecord {
                    name: format!("latency_{i}"),
                    answer: i as i32,
                }),
        )
        .await
        .map_err(|_| format!("latency iter {i} timed out after {}s", OP_TIMEOUT.as_secs()))??;
        samples.push(start.elapsed());
    }

    // Sort + p50 (median) computation. For an even-sized sample we
    // pick the lower median — the ticket says "p50" not "median of
    // two", and the stricter pick means marginal passes can't be a
    // rounding-artifact win. For 20 samples the p50 is the 10th
    // element in the sorted list (index 9).
    samples.sort();
    let p50 = samples[samples.len() / 2 - 1];
    let min = samples[0];
    let max = *samples.last().unwrap();
    // p95 for posterity — not asserted, but useful in the PR body.
    let p95_index = ((samples.len() as f64 * 0.95).ceil() as usize).saturating_sub(1);
    let p95 = samples[p95_index.min(samples.len() - 1)];

    println!("latency_n      = {}", samples.len());
    println!("latency_min_ms = {:.2}", ms(min));
    println!("latency_p50_ms = {:.2}", ms(p50));
    println!("latency_p95_ms = {:.2}", ms(p95));
    println!("latency_max_ms = {:.2}", ms(max));

    // Cleanup the latency-loop fixtures before we assert, so a failed
    // run leaves the cluster clean for the next attempt.
    for i in 0..LATENCY_ITERATIONS {
        let id = format!("lat_{i:03}");
        let _: Option<SpikeRecord> = client
            .delete((LATENCY_TABLE, id.as_str()))
            .await
            .unwrap_or(None);
    }

    if p50 >= P50_THRESHOLD {
        eprintln!(
            "error: p50 latency {:.2} ms >= threshold {} ms",
            ms(p50),
            P50_THRESHOLD.as_millis()
        );
        return Err(format!(
            "p50 latency regression: {:.2} ms exceeds {}-ms budget",
            ms(p50),
            P50_THRESHOLD.as_millis()
        )
        .into());
    }

    // ─── Phase 5: shutdown ────────────────────────────────────────
    println!("--- Phase 5: shutdown ---");
    tikv.shutdown().await?;
    println!("tikv_shutdown  = OK");

    println!("== p2-4 spike OK ==");
    Ok(())
}

/// Load the PD mesh IPv6 list (self first, peers after) from the
/// `fabric:state` row stored in the local SurrealKV datastore.
///
/// Mirrors the contract of `FabricState::pd_endpoints` in the
/// hypervisor crate: self is always index 0, peers follow in
/// peer-list order. The two code paths are kept in lockstep by the
/// acceptance criteria — if they ever diverge, the spike will fail
/// on a real cluster.
async fn load_pd_mesh_addrs(db: &EmbeddedDb) -> Result<Vec<Ipv6Addr>, Box<dyn std::error::Error>> {
    // The fabric table is SCHEMALESS and lazily created on first use
    // by `FabricState::save`, but `hypervisor init` has already run
    // before we get here, so the row exists. Define-if-not-exists is
    // still cheap and keeps this spike safe to re-run against an
    // uninitialised node for debugging.
    db.client()
        .query("DEFINE TABLE IF NOT EXISTS fabric SCHEMALESS")
        .await?
        .check()?;

    let mut response = db
        .client()
        .query("SELECT * FROM type::record($tbl, $id)")
        .bind(("tbl", "fabric"))
        .bind(("id", "state"))
        .await?;
    let row: Option<serde_json::Value> = response.take(0)?;
    let row = row.ok_or("fabric:state missing — run `nauka hypervisor init` first")?;

    let self_ipv6_str = row
        .get("hypervisor")
        .and_then(|h| h.get("mesh_ipv6"))
        .and_then(|v| v.as_str())
        .ok_or("fabric:state.hypervisor.mesh_ipv6 missing")?;
    let self_ipv6: Ipv6Addr = self_ipv6_str
        .parse()
        .map_err(|e| format!("hypervisor.mesh_ipv6 `{self_ipv6_str}` is not a valid IPv6: {e}"))?;

    let mut addrs = vec![self_ipv6];

    // Peers live at `peers.peers[]` — see PeerList in
    // layers/hypervisor/src/fabric/peer.rs. Missing or empty list is
    // fine: on a brand-new single-node cluster there are no peers.
    if let Some(peer_array) = row
        .get("peers")
        .and_then(|p| p.get("peers"))
        .and_then(|a| a.as_array())
    {
        for peer in peer_array {
            if let Some(s) = peer.get("mesh_ipv6").and_then(|v| v.as_str()) {
                match s.parse::<Ipv6Addr>() {
                    Ok(ipv6) => addrs.push(ipv6),
                    Err(e) => {
                        return Err(format!("peer.mesh_ipv6 `{s}` is not a valid IPv6: {e}").into())
                    }
                }
            }
        }
    }

    Ok(addrs)
}

/// Floating-point milliseconds for pretty-printing.
fn ms(d: Duration) -> f64 {
    (d.as_secs_f64()) * 1_000.0
}
