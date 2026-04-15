//! Integration test for #299 — parallel joins against a shared
//! `EmbeddedDb` handle, plus concurrent read access.
//!
//! This is the regression test the issue's acceptance criteria asks
//! for. It exercises the post-#299 invariants:
//!
//! 1. Multiple `peering_client::join` calls running concurrently all
//!    succeed *and* every one shows up in the final state.
//!    Before #299 this was the "1-of-N-joins-succeeds-TLS-reset-on-
//!    the-rest" failure mode — the listener was strictly serial,
//!    every joiner past the first hit the per-request bootstrap.skv
//!    flock on the accept path, and the `load → modify → save` race
//!    on the single-blob `fabric:state` record silently dropped
//!    updates.
//! 2. While parallel joins are in flight, concurrent `status_view`
//!    reads against the same `EmbeddedDb` handle never fail with
//!    "Database ... is already locked". This is the in-process
//!    analogue of the `cp-status` / `status` operator command
//!    running while the daemon is accepting joins.
//!
//! We do **not** install `nauka.service` here — that would require
//! root and systemd. Instead we run the individual listeners
//! directly against a temp `EmbeddedDb`, which is exactly the shape
//! `fabric::daemon::run` has on a real node minus the process
//! isolation and systemd integration.

use std::sync::Arc;
use std::time::Duration;

use nauka_hypervisor::fabric::backend::NetworkMode;
use nauka_hypervisor::fabric::mesh;
use nauka_hypervisor::fabric::peer::PeerList;
use nauka_hypervisor::fabric::peering::JoinRequest;
use nauka_hypervisor::fabric::peering_client;
use nauka_hypervisor::fabric::peering_server;
use nauka_hypervisor::fabric::state::FabricState;
use nauka_state::EmbeddedDb;

/// Number of joiners we drive at once. Matches the issue's
/// "5 joiners concurrent" scenario but stays small enough for a
/// unit test.
const CONCURRENCY: usize = 4;

async fn temp_db() -> (tempfile::TempDir, EmbeddedDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = EmbeddedDb::open(&dir.path().join("test.skv"))
        .await
        .unwrap();
    (dir, db)
}

async fn init_bootstrap_node(db: &EmbeddedDb) -> String {
    let (mesh_id, secret) = mesh::create_mesh();
    let hv = mesh::create_hypervisor(&mesh::CreateHypervisorConfig {
        name: "bootstrap",
        region: "eu",
        zone: "test",
        port: 51820,
        endpoint: None,
        fabric_interface: "",
        mesh_prefix: &mesh_id.prefix,
        ipv6_block: None,
        ipv4_public: None,
    })
    .unwrap();

    let pin = secret.derive_pin();

    let state = FabricState {
        mesh: mesh_id,
        hypervisor: hv,
        secret: secret.to_string(),
        peers: PeerList::new(),
        network_mode: NetworkMode::Mock,
        node_state: nauka_hypervisor::fabric::NodeState::default(),
        max_pd_members: 3,
    };
    state.save(db).await.unwrap();

    pin
}

/// Build a throwaway 32-byte base64-encoded WireGuard key. Different
/// each call so every joiner claims a distinct peer identity.
fn fake_wg_key(seed: u64) -> String {
    use base64::Engine as _;
    let mut bytes = [0u8; 32];
    for (i, b) in bytes.iter_mut().enumerate() {
        *b = ((seed.wrapping_mul(31337).wrapping_add(i as u64)) & 0xff) as u8;
    }
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

#[tokio::test]
async fn parallel_joins_plus_concurrent_state_reads() {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let (_dir, db) = temp_db().await;
    let pin = init_bootstrap_node(&db).await;

    // Bind first, then pass the listener into `serve` so the test
    // can read back the allocated port.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peering_addr = listener.local_addr().unwrap();

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Spawn the peering server against the pre-bound listener. The
    // server shares the same `db` handle we use for status reads.
    let server_db = db.clone();
    let server_pin = pin.clone();
    let server = tokio::spawn(async move {
        peering_server::serve(
            listener,
            server_db,
            server_pin,
            Duration::from_secs(30),
            0, // unlimited accepts
            shutdown_rx,
        )
        .await
    });

    // ── Parallel joiners ──
    let mut join_handles = Vec::with_capacity(CONCURRENCY);
    for i in 0..CONCURRENCY {
        let target = peering_addr.to_string();
        let pin = pin.clone();
        let wg_key = fake_wg_key(i as u64);
        let name = format!("joiner-{i}");
        join_handles.push(tokio::spawn(async move {
            let req = JoinRequest {
                name,
                region: "eu".into(),
                zone: "test".into(),
                wg_public_key: wg_key,
                wg_port: 51820 + i as u16 + 1,
                endpoint: Some(format!("127.0.0.1:{}", 51820 + i as u16 + 1)),
                pin: Some(pin),
                trace_id: Some(format!("parallel_test_{i}")),
            };
            peering_client::join(&target, req).await
        }));
    }

    // ── Concurrent state reads against the same db clone ──
    //
    // The acceptance criterion from #299: while parallel joins are
    // in flight, a concurrent reader must never fail with "Database
    // ... is already locked". We drive 8 tasks each doing 5 tight
    // `FabricState::load` calls against the shared handle. None is
    // allowed to error out.
    //
    // We deliberately use `FabricState::load` rather than the
    // full `status_view` because the latter also pokes PD over
    // HTTP, and in a test without a real cluster those calls would
    // saturate the runtime with curl timeouts. The DB-flock
    // contention we care about is 100% in `FabricState::load`.
    let read_errors: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(Default::default());
    let mut read_handles = Vec::new();
    for _ in 0..8 {
        let db = db.clone();
        let errors = read_errors.clone();
        read_handles.push(tokio::spawn(async move {
            for _ in 0..5 {
                match FabricState::load(&db).await {
                    Ok(_) => tokio::time::sleep(Duration::from_millis(5)).await,
                    Err(e) => {
                        errors.lock().unwrap().push(e.to_string());
                        return;
                    }
                }
            }
        }));
    }

    // Wait for joiners.
    let mut ok = 0usize;
    let mut errors = Vec::new();
    for h in join_handles {
        match h.await.expect("join task panicked") {
            Ok(_) => ok += 1,
            Err(e) => errors.push(e.to_string()),
        }
    }
    assert_eq!(
        ok, CONCURRENCY,
        "expected all {CONCURRENCY} joins to succeed; errors = {errors:?}"
    );

    // Wait for concurrent readers and check they never saw a lock
    // error. Snapshot the errors into an owned Vec before any await
    // so the std Mutex guard doesn't live across an await point.
    for h in read_handles {
        h.await.unwrap();
    }
    let errs: Vec<String> = read_errors.lock().unwrap().clone();
    assert!(
        errs.is_empty(),
        "concurrent FabricState::load calls should never fail while joins are in flight, but: {errs:?}"
    );

    // State reflects all joiners — proves the write_lock around
    // `handle_join` prevents the load→modify→save race that
    // silently dropped updates before #299.
    let state = FabricState::load(&db).await.unwrap().unwrap();
    assert_eq!(
        state.peers.len(),
        CONCURRENCY,
        "expected {CONCURRENCY} peers in state, got {}",
        state.peers.len()
    );

    // Shut the listener down cleanly so its db clone is released
    // before we call `db.shutdown()` on our last clone.
    shutdown_tx.send(true).unwrap();
    let accepted = server.await.unwrap().unwrap();
    assert_eq!(accepted, CONCURRENCY);

    db.shutdown().await.unwrap();
}
