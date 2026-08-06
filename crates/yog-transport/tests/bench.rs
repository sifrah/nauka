//! Micro-bench du transport : débit brut de put_shard sur loopback.
//! `cargo test -p yog-transport --release --test bench -- --ignored --nocapture`

use std::sync::Arc;
use std::time::Instant;

use yog_store::ShardStore;
use yog_transport::server::{make_endpoint, serve_endpoint};
use yog_transport::PeerClient;

#[tokio::test]
#[ignore]
async fn raw_quinn_single_stream() {
    // Débit quinn brut, sans notre protocole : un stream, 256 Mo d'affilée.
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ShardStore::open(dir.path()).unwrap());
    let endpoint = make_endpoint("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = endpoint.local_addr().unwrap();

    // Serveur : accepte une connexion, draine le stream.
    let server = endpoint.clone();
    tokio::spawn(async move {
        let _keep = store;
        let conn = server.accept().await.unwrap().await.unwrap();
        let (mut send, mut recv) = conn.accept_bi().await.unwrap();
        let mut buf = vec![0u8; 1024 * 1024];
        let mut total = 0usize;
        while let Ok(Some(n)) = recv.read(&mut buf).await {
            total += n;
        }
        send.write_all(&(total as u64).to_le_bytes()).await.unwrap();
        send.finish().unwrap();
        let _ = conn.closed().await;
    });

    let client = PeerClient::connect(addr).await.unwrap();
    let conn = client.connection();
    let (mut send, mut recv) = conn.open_bi().await.unwrap();
    let chunk = vec![0xABu8; 4 * 1024 * 1024];
    const TOTAL: usize = 256 * 1024 * 1024;
    let start = Instant::now();
    let mut sent = 0;
    while sent < TOTAL {
        send.write_all(&chunk).await.unwrap();
        sent += chunk.len();
    }
    send.finish().unwrap();
    let mut ack = [0u8; 8];
    recv.read_exact(&mut ack).await.unwrap();
    let secs = start.elapsed().as_secs_f64();
    assert_eq!(u64::from_le_bytes(ack) as usize, TOTAL);
    println!("quinn brut: {:.0} Mo/s", TOTAL as f64 / 1_000_000.0 / secs);
    let stats = conn.stats();
    println!(
        "path: rtt={:?} cwnd={} mtu={} sent={} lost={} congestion_events={}",
        stats.path.rtt,
        stats.path.cwnd,
        stats.path.current_mtu,
        stats.path.sent_packets,
        stats.path.lost_packets,
        stats.path.congestion_events,
    );
}

#[tokio::test]
#[ignore]
async fn single_put_shard_latency() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ShardStore::open(dir.path()).unwrap());
    let endpoint = make_endpoint("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = endpoint.local_addr().unwrap();
    tokio::spawn(serve_endpoint(store, endpoint, None));
    let client = PeerClient::connect(addr).await.unwrap();

    for size in [1024usize, 64 * 1024, 1024 * 1024] {
        let mut data = vec![7u8; size];
        // Warmup
        data[..8].copy_from_slice(&u64::MAX.to_le_bytes());
        client.put_shard(data.clone()).await.unwrap();
        let start = Instant::now();
        for i in 0..10u64 {
            data[..8].copy_from_slice(&i.to_le_bytes());
            client.put_shard(data.clone()).await.unwrap();
        }
        println!("taille {size}: {:?}/op", start.elapsed() / 10);
    }
}

#[tokio::test]
#[ignore]
async fn raw_put_shard_throughput() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ShardStore::open(dir.path()).unwrap());
    let endpoint = make_endpoint("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = endpoint.local_addr().unwrap();
    tokio::spawn(serve_endpoint(store, endpoint, None));

    let client = PeerClient::connect(addr).await.unwrap();

    const SHARD: usize = 1024 * 1024;
    const COUNT: usize = 256; // 256 MiB
    const IN_FLIGHT: usize = 64;

    // Shards uniques (pas de dédup possible côté store).
    let mut base = vec![0u8; SHARD];
    for (i, b) in base.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }

    let start = Instant::now();
    let mut set: tokio::task::JoinSet<anyhow::Result<String>> = tokio::task::JoinSet::new();
    for i in 0..COUNT {
        let mut data = base.clone();
        data[..8].copy_from_slice(&(i as u64).to_le_bytes());
        let c = client.clone();
        while set.len() >= IN_FLIGHT {
            set.join_next().await.unwrap().unwrap().unwrap();
        }
        set.spawn(async move { c.put_shard(data).await });
    }
    while let Some(r) = set.join_next().await {
        r.unwrap().unwrap();
    }
    let secs = start.elapsed().as_secs_f64();
    println!(
        "{} MiB en {:.2}s → {:.0} Mo/s",
        COUNT,
        secs,
        (COUNT * SHARD) as f64 / 1_000_000.0 / secs
    );
}
