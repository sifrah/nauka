# Nauka

**A distributed storage engine that heals itself — one binary, one key,
zero configuration.**

Nauka splits every file into Reed-Solomon shards scattered across the nodes
of a cluster. As long as `k` shards per stripe survive somewhere, the file
comes back **byte-for-byte identical** — dead node, rotting disk, entire
region wiped out.

Nodes find each other on the BitTorrent DHT, elect a founder if the cluster
does not exist yet, authenticate one another, spread data according to disk
capacity and network distance, then repair and rebalance themselves
continuously. No central server, no side infrastructure, no configuration
file.

```bash
nauka keygen --out ./nauka-keys      # once
scp -r nauka-keys vps:/etc/          # on every machine
nauka --keys /etc/nauka-keys serve   # the same command everywhere
```

That's it. The cluster forms itself.

## What sets it apart

|  | Nauka | Garage | MinIO | IPFS |
|---|:---:|:---:|:---:|:---:|
| Erasure coding (no ×3 replication) | ✅ | ❌ | ✅ | ❌ |
| Self-healing | ✅ | partial | ✅ | ❌ |
| Config-free cluster formation | ✅ | ❌ | ❌ | ✅ |
| Single binary | ✅ | ✅ | ~ | ~ |
| Capacity-weighted placement | ✅ | ✅ | ❌ | ❌ |
| Topology-aware placement | ✅ | ❌ | ❌ | ❌ |

**Durability.** 4+2 by default: every stripe survives the loss of any 2
shards out of 6, for a 50% storage overhead — where ×3 replication charges
200% for the same tolerance. BLAKE3 integrity is checked at every boundary:
a corrupted shard is caught on read and treated as lost, never served.

**Zero configuration.** A node's identity is derived from its Ed25519
public key. Its address is auto-detected. The cluster is found on the
Mainline DHT under a key derived from the cluster key — nothing else to
distribute, not even a URL. If no cluster exists yet, a genesis election
picks the founder, with no privileged node.

**Smart placement.** Rendezvous hashing weighted by declared disk capacity:
every node fills to the same percentage. And nodes learn their network
positions from the round-trip times they measure (Vivaldi coordinates, no
GeoIP database) in order to **pull the shards of a stripe apart** — a file
survives the loss of a region, not merely of a machine.

**Proofs, not claims.** A node can assert it still holds what it has
quietly lost. Nauka demands `blake3(nonce ‖ bytes)` proofs of possession
before it gives up any redundancy, and audits its peers continuously by
sampling.

## Getting started

```bash
curl -sSfL https://sh.getnauka.com | sh     # or a .deb / .rpm from the releases
```

From source:

```bash
cargo build --release          # binary lands in target/release/nauka
cargo test                     # 48 tests (unit + integration)
```

Installation, deployment, CLI reference and troubleshooting:
[getnauka.com/install](https://getnauka.com/install/),
[getnauka.com/deploy](https://getnauka.com/deploy/) and
[getnauka.com/operations](https://getnauka.com/operations/).

## Documentation

The full documentation lives at **[getnauka.com](https://getnauka.com)**; its
sources are in [`site/src/content/docs/`](site/src/content/docs/).

| Document | Contents |
|---|---|
| [Install](https://getnauka.com/install/) | Install script, packages, source build, provenance |
| [Deploy a cluster](https://getnauka.com/deploy/) | Keys, ports, systemd, cluster sizing |
| [Architecture](https://getnauka.com/architecture/) | Crates, invariants, upload/download flows |
| [Erasure coding and storage](https://getnauka.com/erasure-core/) | Reed-Solomon, stripes, integrity, storage |
| [Transport](https://getnauka.com/transport/) | QUIC, inter-node protocol, throughput tuning |
| [Consensus](https://getnauka.com/consensus/) | Durable Raft, dedicated network plane |
| [Cluster](https://getnauka.com/cluster/) | Placement, healing, attestation, topology-aware placement |
| [Identity and discovery](https://getnauka.com/identity-and-discovery/) | mTLS, DHT, genesis election |
| [HTTP API](https://getnauka.com/api-http/) | Public API, deletion, expiry |
| [End-to-end encryption](https://getnauka.com/encryption/) | End-to-end, threat model |
| [Operations](https://getnauka.com/operations/) | Deployment, CLI, known limitations |
| [Design decisions](https://getnauka.com/decisions/) | Structural choices and stress-test lessons |
| [Backlog](https://getnauka.com/backlog/) | Upcoming work |

## Yogfile

[Yogfile](https://github.com/sifrah/yogfile) is the file-sharing service
built on top of Nauka: end-to-end encryption in the browser, share links
whose key never leaves the client, encrypted video playback with seeking.
Its code currently lives in this repository
(`crates/nauka-node/src/api.rs`, `webui/`) and will be split out.

## Status

Young, but serious. The foundation is proven by integration tests that kill
processes, cut power to the whole cluster, saturate the network and corrupt
disks on purpose. What is still missing before production use is spelled
out without hedging in
[Operations](https://getnauka.com/operations/#known-limitations-v1) and the
[Backlog](https://getnauka.com/backlog/) — chiefly API authentication, NAT
traversal and an S3 API.

## License

[AGPL-3.0](LICENSE). The web interface derives from the
[ZeroFS](https://github.com/Barre/ZeroFS) webui (AGPL-3.0) — see
[`webui/ATTRIBUTION.md`](webui/ATTRIBUTION.md).
