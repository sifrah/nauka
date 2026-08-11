# Nauka

**A distributed storage engine that heals itself — one binary, one key.**

Nauka splits every file into Reed-Solomon shards scattered across the nodes
of a cluster. As long as `k` shards per stripe survive somewhere, the file
comes back **byte-for-byte identical** — dead node, rotting disk, entire
region wiped out.

Nodes authenticate one another with mTLS, spread data according to disk
capacity and network distance, and repair and rebalance themselves
continuously. Membership is managed explicitly from the CLI — no central
server, no side infrastructure, no discovery layer to depend on.

```bash
# First machine — installs nauka and founds a systemd-managed cluster:
curl -sSfL https://sh.getnauka.com | sh

# Grow it from that machine — provisions the target over SSH and joins it:
nauka node add <new-ip>:7311
```

## What sets it apart

|  | Nauka | Garage | MinIO | IPFS |
|---|:---:|:---:|:---:|:---:|
| Erasure coding (no ×3 replication) | ✅ | ❌ | ✅ | ❌ |
| Self-healing | ✅ | partial | ✅ | ❌ |
| Single binary | ✅ | ✅ | ~ | ~ |
| Capacity-weighted placement | ✅ | ✅ | ❌ | ❌ |
| Topology-aware placement | ✅ | ❌ | ❌ | ❌ |

**Durability.** 4+2 by default: every stripe survives the loss of any 2
shards out of 6, for a 50% storage overhead — where ×3 replication charges
200% for the same tolerance. BLAKE3 integrity is checked at every boundary:
a corrupted shard is caught on read and treated as lost, never served.

**Explicit, race-free membership.** A node's identity is derived from its
Ed25519 public key. The first `serve` on a blank data dir founds a
single-node cluster; every other node is added deliberately with
`nauka node add`, which provisions it over SSH and joins it to the Raft
group. Membership changes go through consensus — there is no discovery
layer and therefore no split-brain to guard against.

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

On a systemd Linux run as root, the installer finishes the job: `nauka init`
creates a dedicated user, generates the cluster token, and enables a hardened
systemd service — the node starts immediately, founds a single-node cluster,
and comes back after every reboot. Anywhere else (laptop, non-root, no
systemd) only the binary is installed; run `sudo nauka init` yourself when
ready, or set `NAUKA_NO_INIT=1` to keep the installer to the binary alone.

The service is configured in `/etc/nauka/nauka.env`, stores under
`/var/lib/nauka`, and logs to `journalctl -u nauka`. To run a node by hand
instead — no systemd, a terminal in the foreground:

```bash
NAUKA_TOKEN=$(nauka token) nauka serve --advertise <ip>:7311
```

From source:

```bash
cargo build --release                   # the storage engine, no S3 endpoint
cargo build --release --features s3     # add the S3-compatible endpoint
cargo test --workspace
```

The S3-compatible endpoint (SigV4, multipart, SSE-C) is an opt-in cargo
feature: the default engine serves its native HTTP API and nothing more.

Nauka is the storage engine. It exposes an HTTP API (`POST /api/upload`,
`GET /f/{hash}`, `GET /api/status`) and — with `--features s3` — an
S3-compatible endpoint; a user-facing web application belongs in a product
built on top, not in the engine.

## Egress budgets

Storage placement balances a stock — bytes on disk against declared
capacity. Egress budgets balance the matching flow: bytes served to
clients against a declared monthly allowance, for nodes on metered links
(a 20 TB/month dedicated server, a capped home connection).

```bash
NAUKA_EGRESS_QUOTA=20TB nauka serve …      # or --egress-quota 20TB
```

Plain bytes and human sizes are accepted (`500GB`, `1.5TB`, `512MiB`).
Under systemd, set the variable in `/etc/nauka/nauka.env` and
`systemctl restart nauka`. Unset means unmetered. A node past its budget is **deprioritized, never
refused**: serving the file always wins over saving a node's bandwidth, so
an exhausted budget shifts load while alternatives exist and yields when
they don't.

## Per-node stripe cache

Decoded stripes that crossed the cluster once are kept on local disk and
served directly afterwards:

```bash
NAUKA_CACHE_SIZE=10GB nauka serve …        # or --cache-size 10GB
```

Same thing under systemd: the variable goes in `/etc/nauka/nauka.env`.
Because content is addressed by BLAKE3, a cache entry can never go stale;
entries of deleted content age out by LRU and are swept alongside the
shard GC. Reconstruct once per region, serve many times locally.

## Documentation

The full documentation lives at **[getnauka.com](https://getnauka.com)**; its
sources are in [`site/src/content/docs/`](site/src/content/docs/).

| Document | Contents |
|---|---|
| [Install](https://getnauka.com/install/) | Install script, packages, source build, provenance |
| [Deploy a cluster](https://getnauka.com/deploy/) | Keys, ports, systemd, `node add`, cluster sizing |
| [Architecture](https://getnauka.com/architecture/) | Crates, invariants, upload/download flows |
| [Erasure coding and storage](https://getnauka.com/erasure-core/) | Reed-Solomon, stripes, integrity, storage |
| [Transport](https://getnauka.com/transport/) | QUIC, inter-node protocol, throughput tuning |
| [Consensus](https://getnauka.com/consensus/) | Durable Raft, dedicated network plane |
| [Cluster](https://getnauka.com/cluster/) | Placement, healing, attestation, topology-aware placement |
| [HTTP API](https://getnauka.com/api-http/) | Public API, deletion, expiry |
| [Operations](https://getnauka.com/operations/) | Deployment, CLI, known limitations |
| [Design decisions](https://getnauka.com/decisions/) | Structural choices and stress-test lessons |

## Status

Young, but serious. The foundation is proven by integration tests that kill
processes, cut power to the whole cluster, saturate the network and corrupt
disks on purpose. What is still missing before production use — chiefly API
authentication and NAT traversal — is spelled out without hedging in
[Operations](https://getnauka.com/operations/#known-limitations-v1).

## License

[AGPL-3.0](LICENSE).
