---
title: "Architecture"
description: "The crate layering, the invariants that hold the system together, and the upload, download and self-healing flows end to end."
---

## The crates

The workspace is cut into strict layers — each crate knows only about the
ones below it:

| Crate | Role | Depends on |
|---|---|---|
| `nauka-erasure` | Pure core (zero I/O): Reed-Solomon encoding by stripes, reconstruction, BLAKE3 integrity | — |
| `nauka-store` | On-disk storage for one node: content-addressed shards, JSON manifests | nauka-erasure |
| `nauka-transport` | Inter-node QUIC (quinn): shard/manifest/Raft protocol, mTLS, throughput tuning | nauka-erasure, nauka-store |
| `nauka-raft` | openraft consensus: replicated file registry + membership, durable redb storage | nauka-erasure, nauka-transport |
| `nauka-cluster` | Cluster logic: rendezvous-hash placement, self-healing, rebalancing GC | nauka-erasure, nauka-store, nauka-transport |
| `nauka-discovery` | Mainline DHT rendezvous (pkarr): publishing/resolving seeds, public IP detection | — (pkarr, mainline) |
| `nauka-node` | The binary: CLI, server, HTTP API, orchestration of everything above | all of them |

## System invariants

1. **Integrity is verified at every boundary.** Every shard has a BLAKE3
   hash; every file has a global hash. A shard is re-verified on every disk
   read and discarded if it does not match (treated as lost, never used
   silently), and the reconstructed file is re-checked against the
   manifest's hash before being handed back.
2. **Placement is a pure function.** "Who should hold shard i of stripe s
   of file f?" is computed from (file hash, indices, sorted member list) —
   same answer on every node, with zero coordination. All cluster
   convergence (healing, GC, rebalancing) falls out of this invariant.
3. **Consensus carries metadata only.** The Raft log replicates the
   manifest registry and the membership — never shard bytes, which travel
   directly over QUIC. Consensus stays lightweight no matter how much data
   is stored.
4. **Content is the address.** A shard is stored under its own hash
   (content-addressed): idempotent writes, dedup for free, safe to resend.
5. **Discovery ≠ admission.** The public DHT hands out addresses; mTLS with
   the cluster key decides who gets in. A stranger can find the cluster,
   not join it.
6. **Identity is proven.** The Raft node-id is derived from the node's
   Ed25519 public key (first 8 bytes of blake3(pubkey)) — not declared by a
   CLI flag.

## Upload flow (`POST /api/upload` on any node)

```
client ──POST /api/upload──▶ node N (any of them)
  1. N buffers the stream into data-dir/tmp, hashing as it goes
     (placement is keyed on the file's final hash)
  2. N reads the buffer back stripe by stripe (4 MiB of data by default):
       encode_stripe → k=4 data shards + m=2 parity shards (1 MiB each)
       for each shard: owner = HRW(file_hash, stripe, index, members)
         owner == N     → local write
         owner == other → put_shard over QUIC to it (retried, idempotent)
  3. N writes the manifest locally, then records it in the Raft
     registry (local write if leader, otherwise forwarded to the leader)
  4. response: { hash, size, name, link: "/f/<hash>" }
```

## Download flow (`GET /f/{hash}` on any node)

```
client ──GET /f/<hash>──▶ node N
  1. manifest: local store, otherwise the in-memory replicated registry
  2. for each stripe (streamed, one stripe in memory at a time):
       for each shard: local? otherwise get_shard from each member
       (timeouts; an unreachable peer is remembered and not retried)
       decode_stripe: ≥ k valid shards are enough — missing and corrupted
       ones are rebuilt by Reed-Solomon
  3. global hash recomputed on the fly, compared against the manifest
```

## A node's background loop (consensus mode)

Every `--scrub-interval` seconds (default 30 s):

1. **Materialization**: manifests present in the Raft registry but absent
   from the local store are written to it (a node that missed an upload
   catches up).
2. **Scrub (acquisition)**: for every shard this node owns according to
   placement — missing or corrupted? → gather ≥ k shards of the stripe from
   the cluster, decode, re-encode, verify the hash, store.
3. **GC (release)**: for every local shard this node no longer owns (the
   view changed) — deleted only once ALL current owners have confirmed they
   hold their copy.

These three steps make any topology change automatic: node dies → its
shards are regenerated elsewhere; node added → it acquires its share and
the others release theirs; node removed → the cluster absorbs the load
again.
