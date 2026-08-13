---
title: "Architecture"
description: "The crate layering, the invariants that hold the system together, and the upload and download flows end to end."
---

## The crates

The workspace is cut into strict layers — each crate knows only about the
ones below it:

| Crate | Role | Depends on |
|---|---|---|
| `nauka-erasure` | Pure core (zero I/O): Reed-Solomon encoding by stripes, reconstruction, BLAKE3 integrity | — |
| `nauka-store` | On-disk storage for one node: content-addressed shards, JSON manifests | nauka-erasure |
| `nauka-transport` | Inter-node QUIC (quinn): shard/manifest/Raft protocol, mTLS, the two network planes | nauka-erasure, nauka-store |
| `nauka-cluster` | Placement (weighted rendezvous + Vivaldi coordinates), healing, liveness, attestation | nauka-erasure, nauka-store, nauka-transport |
| `nauka-raft` | openraft consensus: the replicated registry, durable redb log | nauka-erasure, nauka-transport, nauka-cluster, nauka-s3 |
| `nauka-crypto` | Client-side end-to-end encryption (AES-256-GCM STREAM, magic `NKA1`) | — |
| `nauka-s3` | S3 data model (buckets, credentials, naming rules), replicated by Raft; the endpoint is behind the `s3` cargo feature | nauka-erasure |
| `nauka-node` | The binary: CLI, server, HTTP API, orchestration of everything above | all of them |

## System invariants

1. **Integrity is verified at every boundary.** Every shard has a BLAKE3
   hash; every file has a global hash. A shard is re-verified on every disk
   read and treated as lost if it does not match — never served silently —
   and the reconstructed file is re-checked against the manifest's hash
   before being handed back.
2. **Placement is a pure function.** "Who owns shard i of stripe s?" is
   computed from (stripe content hash, indices, member list with weights
   and coordinates) — same answer on every node, zero coordination. All
   convergence (healing, GC, rebalancing) falls out of this invariant.
3. **Consensus carries metadata only.** The Raft log replicates the
   registry — never shard bytes, which travel directly over QUIC.
   Consensus stays lightweight no matter how much data is stored.
4. **Content is the address.** A shard is stored under its own hash:
   idempotent writes, dedup for free, safe to resend.
5. **Identity is proven.** The Raft node-id is derived from the node's
   Ed25519 public key (first 8 bytes of blake3(pubkey)) — not declared by
   a flag. Admission is mTLS with the cluster CA, nothing else.

## The two network planes

Every node opens two QUIC endpoints: the data plane on `--listen`
(default 7311/udp) for shards, manifests and admin RPCs, and a consensus
plane on port+1 that serves only Raft. Details and the reason for the
split are in [Transport](/transport/).

## Upload flow (`POST /api/upload` on any node)

Encoding is overlapped with reception — it starts on the first complete
stripe, not after the last byte:

```
client ──POST /api/upload──▶ node N (any of them)
  0. quorum gate: if the manifest provably cannot be committed,
     refuse NOW ("no quorum") — before any encoding work is spent
  1. the body streams into an elastic buffer (bounded RAM window from a
     global pool; a disk spool absorbs what the encoder cannot drain)
  2. per complete stripe (4 MiB of data): encode_stripe → 4 data + 2
     parity shards. Placement is keyed on the stripe's content, so the
     owners are known the moment it is encoded:
       owner == N     → local write
       owner == other → onto that peer's bounded send queue
     A busy peer backpressures the encoder; a FAILED peer trips a
     breaker and its shards are parked locally (degraded, not lost —
     the scrubber completes them later)
  3. after the last byte: any stripe with fewer than k=4 shards placed
     aborts the upload — parked-on-one-node is not durability
  4. the manifest is registered LAST, in the Raft registry (written
     locally first, so the file is readable here right away)
  5. response: { hash, size, stripes, …, degraded_shards, link }
     degraded_shards = 0 means every shard reached its owner
```

## Download flow (`GET /f/{hash}` on any node)

```
client ──GET /f/<hash>──▶ node N
  1. manifest: local store, else the replicated registry
  2. the FIRST stripe is reconstructed before any status is sent: a file
     with too many shards gone gets an honest 503, not a truncated 200
  3. stripes are reconstructed through an ordered READ-AHEAD pipeline
     (6 in flight): per stripe, the local cache is consulted, then the
     closest neighbor's cache (one local transfer beats k far ones),
     then the k data shards are fetched as a HEDGED race — parity
     joins after an adaptive delay (3x the learned fetch latency) or on
     the first failure, and the first k valid shards win, so a slow
     peer loses the race instead of stalling it. On a healthy cluster
     not one parity byte crosses the wire. decode_stripe: any k valid
     shards out of 6 are enough. Range reads take a cheaper door when
     the window fits in a subset of data shards (the layout is
     contiguous): only the covering shards are fetched, BLAKE3-checked,
     no reconstruction at all
  4. global BLAKE3 recomputed on the fly, compared to the manifest —
     the pipeline yields strictly in order, so the check is unchanged
```

Decoded stripes that crossed the cluster land in the per-node cache —
on by default, sized to the free disk, content-addressed so never
stale — and neighbors [cooperate](/egress-and-cache/): only one node
per region pays a cold read. Range requests fetch only the stripes
covering the range, through the same pipeline.

## Where state lives

| State | Where | Authority |
|---|---|---|
| Registry: manifests, capacities, coordinates, egress ledgers, bans, S3 view | Raft state machine, replicated on every node | **the truth** |
| Shards | each node's `data-dir/shards`, content-addressed | placement says who *should* hold what |
| Local manifests | `data-dir/manifests` | a cache of the registry, materialized by the scrubber |

Every `--scrub-interval` (30 s) each node converges toward that truth:
manifests in the registry but missing locally are materialized; shards
this node owns but lacks are regenerated from any k; shards it no longer
owns are released — only after the real owner has proven possession with
a `blake3(nonce ‖ bytes)` challenge. Node dies, node added, node removed:
the same three steps absorb all of it. See [Cluster](/cluster/).
