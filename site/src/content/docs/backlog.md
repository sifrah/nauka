---
title: "Backlog"
description: "What is already shipped, the innovations that set the project apart, and the consolidation work still ahead — in priority order."
---

The work items, sorted. Three sections: what is **shipped**, the
**innovations** (what differentiates the product) and the
**consolidation** work (debt and expected features). Descending priority
within each section.

## Shipped

| Work item | Where |
|---|---|
| Web interface (drag and drop, key ring, share links) | `webui/`, derived from ZeroFS (AGPL-3.0) |
| Encrypted video streaming with seeking | `/stream/{hash}` Service Worker + server-side Range |
| End-to-end encryption | `nauka-crypto` (Rust) ↔ WebCrypto (browser), cross-compatibility proven |
| Disk-capacity-weighted placement | `nauka-cluster/placement` (WRH) |
| Storage attestation (proofs of possession) | `nauka-cluster/audit` + hardened GC |
| Topology-aware placement without GeoIP (Vivaldi coordinates) | `nauka-cluster/vivaldi` + `stripe_owners_geo` |
| Deletion, expiry (TTL) and blocking by hash | `DELETE /f/{hash}`, `ttl=`, `nauka ban/unban` |
| Zero-config DHT discovery + genesis election | `nauka-discovery` |
| Cryptographic identity + cluster mTLS | `nauka-transport/tls` |
| Durable Raft consensus + dedicated network plane | `nauka-raft` |

## Innovations

### 1. S3-compatible API — *the adoption multiplier*
Exposing a subset of the S3 API brings in an entire ecosystem at once:
rclone, restic, Velero, Terraform, the AWS SDKs, Docker registries,
Thanos/Loki. A self-hoster points `restic` at yogfile and gets an
**encrypted, erasure-coded, geo-distributed, self-healing** backup in one
command. That is the road Garage took — except Garage replicates ×3 (+200%)
where we do 4+2 (+50%), with topology-aware placement and attestation on
top.

No conflict with zero-knowledge: restic and rclone already encrypt on the
client, so the "the server cannot read it" property is preserved. The two
worlds coexist — S3 for infrastructure and tooling, the native API + webui
for consumer-facing end-to-end sharing.

To build:
1. **Mutable indirection** `(bucket, key) → file_hash` in the Raft state —
   the only genuine semantic addition (S3 overwrites keys, our store is
   immutable and content-addressed).
2. A useful subset: PUT/GET/HEAD/DELETE object, ListObjectsV2,
   CreateBucket, and **multipart upload** (the SDKs depend on it).
3. **SigV4**: AWS signing settles consolidation item B at the same time —
   S3 access keys *are* the authentication system.

**Effort: medium-high** (1–2 sessions). The prerequisite (deletion) is in
place.

### 2. The cluster hosts its own UI — *low effort*
The interface is currently served from `webui/dist` on each node's disk.
Storing it **as a file inside the cluster** (uploaded and signed by the
operator, served by any node) removes frontend deployment entirely and
makes updates atomic. **Effort: low.** Conceptually clean — "the
distributed storage that serves itself".

### 3. Direct-to-shard upload/download (torrent style)
The client (CLI first, then the browser via wasm) does the Reed-Solomon
encoding itself and pushes each shard straight to its owner, in parallel —
the gateway only records the manifest. Symmetric on download (pull from 4
nodes at once). Upload at the cluster's aggregate speed rather than one
server's. Storj's architecture, nonexistent in self-hosted form.
**Effort: medium-high** (direct-write authorization protocol, wasm RS for
the browser).

### 4. Cluster peering — the "BGP of storage"
Two independent clusters sign an agreement and host extra parity for each
other. End-to-end encryption lets you hand unreadable bytes to a peer you
do not need to trust; in exchange, you survive a total local disaster.
Mutualized disaster recovery, with no contract and no blockchain. A
category that does not exist yet. **Effort: high** (identity federation,
inter-cluster placement, accounting). Attestation, the prerequisite, is
already in place.

### 5. Adaptive re-striping
Re-encode existing files to a different k+m scheme when the cluster changes
scale (e.g. 4+2 → 8+3 at 11+ nodes: more tolerance at a lower overhead).
Read back → re-encode → new version in the registry → GC the old one.
Neither Ceph nor MinIO does this either (profile frozen per pool).
**Effort: medium.** Manual first (`cluster-restripe`), automatable
afterwards.

### 6. Optional Tor transport (arti)
Embedded .onion access in pure Rust via arti, as a pluggable transport
(`--tor`) — never a mandatory dependency. Serves the anti-censorship niche
without weighing down the main product. Yggdrasil: ruled out (no Rust
implementation, and a Go sidecar means the ChainRage mess we got rid of);
its real benefit (nodes without a public IP) is covered by native NAT
traversal instead (see B).

## Consolidation

### A. Authentication and quotas on the HTTP API — *the most urgent*
A prerequisite for any public exposure. Upload tokens, per-key quotas, rate
limiting. (In the meantime: a reverse proxy.)

### B. Native NAT traversal (QUIC hole punching + relays)
Opens the product up to home machines — the real self-hosted market. Makes
Yggdrasil permanently unnecessary. **Effort: high** (signaling over the
DHT, optional iroh-style relays).

### C. Full-disk safeguard
Refuse writes beyond ~95% and spill over to the next node in the HRW
ranking; the scrubber repatriates the shards when space frees up.

### D. MediaSource Extensions (fMP4) for the player fallback
Service Worker streaming covers the nominal case. The "decrypt everything
in memory" fallback (when the worker is unavailable) is still capped at
600 MB. MSE + fMP4 would lift that ceiling and improve browser
compatibility.

### E. Offline certificate issuance
The cluster key never leaves the admin workstation; each node receives a
pre-signed certificate. Shrinks the blast radius of a compromised node.

### F. Fair queuing between concurrent uploads
Large streams starve small ones (observed during the 15 GB stress test —
harmless, just unfair). Per-connection scheduling on the server side.

### G. Open source preparation
- **Final name**: `chainrage` (available on GitHub) vs `nauka` (existing
  namesakes, nothing blocking) vs something else.
- **License**: settled — the entire repository under **AGPL-3.0**
  ([`LICENSE`](https://github.com/sifrah/nauka/blob/main/LICENSE) at the
  root, `license = "AGPL-3.0-only"` in the Cargo workspace).
- Showcase README with the kill-demo as a GIF (`rm -rf` a node → automatic
  repair; two terminals → a self-formed cluster).
- GitHub Actions CI (`cargo test`, `npm run build`), merge `empty` into
  `main`.
