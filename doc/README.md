# Documentation

Two projects, one code base for now:

- **Nauka** — the **engine**: a single Rust binary that forms a
  self-organizing cluster, splits every file into Reed-Solomon shards
  scattered across the nodes, and guarantees end-to-end integrity. Whatever
  happens (dead node, rotting disk, region wiped out), as long as k shards
  per stripe survive somewhere, the file comes back byte-for-byte
  identical. This is what will be released under AGPL-3.0.
- **Yogfile** — the file-sharing **service** built on top of it:
  end-to-end encryption, share links, video player, web interface. Powered
  by Nauka.

The `nauka-*` crates are the engine; the HTTP API, the webui and
client-side encryption belong to the service.

The whole operator experience is two commands:

```
nauka keygen --out nauka-keys        # once
nauka --keys ./nauka-keys serve      # on every machine — the same command
```

Nodes find each other on the BitTorrent DHT (Mainline), elect a founder if
the cluster does not exist yet, authenticate one another (Ed25519 mTLS),
divide up the shards, then repair and rebalance themselves continuously. No
central server, no side infrastructure, no configuration file.

## Table of contents

| Document | Contents |
|---|---|
| [architecture.md](architecture.md) | Overview, crates, invariants, upload/download flows |
| [erasure-core.md](erasure-core.md) | Reed-Solomon, stripes, BLAKE3 integrity, content-addressed storage |
| [transport.md](transport.md) | QUIC (quinn), inter-node protocol, throughput tuning |
| [consensus.md](consensus.md) | Raft (openraft), persistence, dedicated network plane |
| [cluster.md](cluster.md) | HRW placement, self-healing, GC, live membership changes |
| [identity-and-discovery.md](identity-and-discovery.md) | Cluster keys, mTLS, derived node-id, Mainline DHT, genesis election |
| [api-http.md](api-http.md) | Public API: upload, download, listing |
| [encryption.md](encryption.md) | End-to-end: AES-GCM on the client, the key in the link fragment |
| [operations.md](operations.md) | Deployment, CLI reference, ports, troubleshooting, known limitations |
| [decisions.md](decisions.md) | Structural choices and stress-test lessons |
| [backlog.md](backlog.md) | Upcoming work: innovations and consolidation, prioritized |

## At a glance

```
                       ┌────────── one yogfile node (a single binary) ──────────┐
       user ───HTTP───▶│ API :8080  ─┐                                          │
                       │             ▼                                          │
 other nodes ──QUIC───▶│ :7311 data ─┼─▶ nauka-erasure (Reed-Solomon k+m, BLAKE3)│
 (Ed25519 mTLS)        │             │   nauka-store  (content-addressed shards) │
                       │ :7312 Raft ─┼─▶ nauka-raft   (durable openraft, redb)   │
Mainline DHT ◀──UDP───▶│             └─▶ nauka-cluster (HRW placement, heal, GC) │
  (discovery)          │                 nauka-discovery (pkarr, genesis, IP)    │
                       └────────────────────────────────────────────────────────┘
```

## Checking that everything works

```
cargo test            # 48 tests (unit + integration, local DHT included)
cargo test --release  # same, optimized (raft/stress tests run faster there)

# Transport benchmarks (throughput measurements, not run by default):
cargo test -p nauka-transport --release --test bench -- --ignored --nocapture
```
