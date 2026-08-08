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

## Egress budgets

Storage placement balances a stock — bytes on disk against declared
capacity. Egress budgets balance the matching flow: bytes served to
clients against a declared monthly allowance, for nodes on metered links
(a 20 TB/month dedicated server, a capped home connection).

```bash
NAUKA_EGRESS_QUOTA=20TB nauka serve …      # or --egress-quota 20TB
```

Plain bytes and human sizes are accepted (`500GB`, `1.5TB`, `512MiB`).
Unset means unmetered. How it works:

- every node counts the bytes it serves (S3 GETs and `/f/…` downloads)
  and publishes the counter into the replicated state alongside its
  budget, the same way capacities are declared;
- when a node reconstructs a file, any k of the k+m shards of a stripe
  decode identically — it asks first for the shards held by the nodes
  with the most budget headroom (its own shards first: those are free);
- counters reset at each calendar-month boundary (UTC), matching how
  providers bill, and survive node restarts through the replicated state.

A node past its budget is **deprioritized, never refused**: serving the
file always wins over saving a node's bandwidth, so an exhausted budget
shifts load while alternatives exist and yields when they don't. The
`egress declared:` log line reports each node's month-to-date total.

## CDN: stripe cache and offload redirects

Two opt-in features turn the cluster into a content-delivery network,
built on the same primitives as everything else.

**Per-node stripe cache** — decoded stripes that crossed the cluster
once are kept on local disk and served directly afterwards:

```bash
NAUKA_CACHE_SIZE=10GB nauka serve …        # or --cache-size 10GB
```

Because content is addressed by BLAKE3, a cache entry can never go
stale: an overwritten S3 object is a new manifest hash, so the old
entry simply stops being asked for and ages out by LRU. Stripes that
decode from local shards are not cached — they are already free. The
cache follows the registry: entries of deleted or banned content are
swept alongside the shard GC. Reconstruct once per region, serve many
times locally.

**Budget-driven offload redirects** — when a node has spent its monthly
egress budget, a presigned GET of a large object (≥ 8 MiB) is answered
with a `302` to a freshly signed URL on the member with the most budget
headroom. The credential registry is replicated, so any node can sign a
URL any other node will honour; the egress leaves the right machine and
the client follows without noticing. Small objects, requests where no
better-funded member exists, and header-signed SDK requests (which do
not re-sign across hosts) are served directly. Combined with the cache,
this is the routing layer of a CDN: DNS/anycast brings the client to a
nearby node, the 302 moves the egress to the node that should pay it,
and the cache makes the second hit free.

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
