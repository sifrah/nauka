---
title: "Transport"
description: "The QUIC protocol spoken between nodes: request types, the two separate network planes, mTLS, and the throughput tuning."
---

## Protocol

One exchange = **one bidirectional QUIC stream**: the client writes a
`Request`, the server answers with a `Response`, framed as
`u32 little-endian length + bincode` (maximum message size: 64 MiB).
Streams on the same connection are multiplexed — several shards fly in
parallel with no head-of-line blocking between them. ALPN: `nauka/0`.

```
Request:  Ping | PutShard(bytes) | GetShard(hash) | HasShard(hash)
        | ProveShard{hash, nonce} | PutManifest(m) | GetManifest(hash)
        | Raft(RaftRpc)
Response: Pong | PutShardOk(hash) | Shard(Option<bytes>) | Has(bool)
        | Proof(Option<[u8;32]>) | PutManifestOk | Manifest(Option<m>)
        | Raft(bytes) | Error(str)
```

- `GetShard` on a shard that is **corrupted server-side returns `None`**
  (as if absent): the client rebuilds it with Reed-Solomon instead of
  receiving wrong bytes.
- `ProveShard` answers `blake3(nonce ‖ shard bytes)` — unlike `HasShard`
  it cannot be faked without re-reading the bytes, because the challenger
  draws a fresh nonce every time. It backs the possession proofs that
  gate the rebalancing GC and the attestation audits.
- Raft RPCs (`AppendEntries | Vote | InstallSnapshot | Admin`) are opaque
  bincode payloads as far as the transport is concerned, handed to the
  local openraft engine through the `RaftHandler` trait.

Files uploaded with `nauka upload` are encrypted client-side before
Reed-Solomon splitting: an AES-256-GCM stream whose first four bytes are
the magic **`NKA1`** (key in the link fragment — see
[Encryption](/encryption/)). The transport neither knows nor cares:
nodes store and serve ciphertext they cannot read.

## The two network planes

Every node opens **two QUIC endpoints**:

| Plane | Port | UDP socket buffers | Role |
|---|---|---|---|
| Data | P (`--listen`, default 7311) | 8 MiB | shards, manifests, admin RPCs |
| Consensus | **P + 1** | 1 MiB | Raft RPCs exclusively |

Why: under saturation, Raft heartbeats queued behind megabytes of shards
in the same socket → timeouts → re-elections at peak load. Separate
sockets mean separate kernel queues, and the small buffers on the
consensus plane **bound the queuing delay** — a heartbeat waiting behind
8 MB of shards is a dead heartbeat. The consensus plane refuses every
non-Raft request and has no access to the store: a port collision cannot
turn it into a rogue data plane. It fails loudly at bind instead.

Operational consequences: open **P and P+1 over UDP** — nothing between
nodes uses TCP (8080/tcp is the client-facing HTTP API, 8333/tcp only
exists with the `s3` feature). Space `--listen` ports at least 2 apart
when several nodes share a host.

Regression test (`nauka-raft/tests/priority.rs`): bulk shard traffic
pushed through while registry writes were running — 0 leader changes,
0 failed writes.

## mTLS

There is one identity model: **every inter-node link is mTLS against the
cluster CA**. `serve` refuses to start without an identity — there is no
unauthenticated mode to misconfigure into.

- The CA is an Ed25519 key (CN `nauka-cluster-ca`), created by `nauka
  keygen` as a key directory — or **derived deterministically from the
  cluster token** (`nauka1_…`): the token IS the cluster, one string to
  copy instead of a directory.
- Each node has its own Ed25519 keypair (`node.key`, auto-generated) and
  a certificate signed by the CA, SAN `node.nauka`. What is verified is
  cluster membership (the CA signature), not the address.
- Servers require a client certificate signed by the CA; clients verify
  the server against the same CA. Holding the token or key = belonging
  to the cluster; a stranger's connection dies at the handshake.
- The Raft node-id is derived from the node's public key: identity is
  proven, not declared. See [Identity](/identity/).

Accepted v1 limitation: the CA lives on every node, so any member can
mint certificates — the blast radius of a shared secret, on a genuinely
authenticated and encrypted link.

## Throughput

The tuning, in the order each bottleneck was found:

1. **Jumbo datagrams**: quinn's 1472-byte default caps the MTU regardless
   of discovery. The ceiling is 9000 (a standard jumbo frame) — asking
   for more only helps loopback and wedges some virtualised hosts. The
   connection starts at a safe 1200 and lets MTU discovery climb: forcing
   a large initial MTU stalls silently on any path that cannot carry it.
2. **BBR instead of Cubic**: on fast links with small buffers Cubic
   collapses on loss (measured: 7 MB/s, 5k losses); BBR measures the
   actual bandwidth and paces its sends. Initial window 256 KiB —
   moderate on purpose, because OS receive buffers (Linux
   `net.core.rmem_max`, often 208 kB) drop anything bolder.
3. **8 MiB UDP socket buffers** on the data plane: system defaults
   (macOS sends with 9216 bytes) overflow on shard bursts.
4. **2 s keep-alive + 30 s idle timeout**: a silent connection under
   congestion no longer dies quietly; failures are loud and the
   idempotent retries take over.

Measured on a 3-node Scaleway×2 + Hetzner cluster: a 1 GiB WAN upload in
36 s (~30 MB/s, 256 stripes, `degraded_shards: 0`), read back in 38 s
byte-identical, and a degraded read with one node down in 24 s —
reconstructed from 4 of 6 shards. Micro-benchmarks:

```
cargo test -p nauka-transport --release --test bench -- --ignored --nocapture
```
