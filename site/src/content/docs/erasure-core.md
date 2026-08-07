---
title: "Erasure coding and storage"
description: "How a file is cut into Reed-Solomon stripes, what a manifest carries, and how content-addressed shards are written and verified on disk."
---

## nauka-erasure — the pure core (zero I/O)

### Splitting into stripes

A file is cut into **stripes** of `data_shards × shard_size` bytes
(default: 4 × 1 MiB = 4 MiB of data per stripe). Each stripe is encoded
independently into `k` data shards + `m` parity shards (Reed-Solomon over
GF(2⁸), `reed-solomon-erasure` crate with SIMD).

- The last stripe is usually partial: zero-padded at encode time, with
  `data_len` in the metadata to truncate at decode time.
- GF(2⁸) constraint: `k + m ≤ 255`.
- Cluster default: **4+2** — every stripe survives the loss of any 2 shards
  out of 6, for a 50% storage overhead.

### Configuration

```rust
ErasureConfig { data_shards: 4, parity_shards: 2, shard_size: 1 MiB }
```

Set at the cluster level (same parameters for every file) and embedded in
every manifest — changing the config does not break existing files, each
one carries its own.

### The manifest

Everything needed to rebuild and prove a file, minus its bytes:

```
FileManifest {
  file_hash:  BLAKE3 of the whole file (the global identifier)
  file_size:  actual size in bytes
  name:       optional display name (not part of the hash)
  config:     ErasureConfig in use
  stripes: [ { data_len, shard_hashes: [BLAKE3 of each shard] } … ]
}
```

### Reconstruction and integrity

`decode_stripe(slots, meta, cfg)` takes one slot per shard
(`Some(bytes)` / `None` if lost):

1. Every shard present is checked against its hash from the manifest — a
   **corrupted shard is treated as lost** (never used).
2. If ≥ k valid shards remain, Reed-Solomon rebuilds the missing ones.
3. Reconstructed data shards are re-verified against the manifest.
4. Otherwise: `NotEnoughShards { available, needed }` — a clean failure,
   never corrupted output.

`decode_file` chains the stripes together, then verifies the file's global
hash. Property proven by the test suite: losing any m shards per stripe →
identical reconstruction; losing m+1 → clean refusal; silent corruption →
detected and repaired.

## nauka-store — a node's on-disk storage

Data-dir layout:

```
data-dir/
  shards/ab/cdef…      # content-addressed, 2 hex chars of the hash as fanout
  manifests/<hash>.json
  raft/                # redb log + snapshot (see Consensus)
  tmp/                 # upload buffers from the HTTP API
  node.key             # the node's Ed25519 identity (--keys mode)
```

Properties:

- **Content-addressed**: the hash IS the path. `put_shard` is idempotent
  and dedups for free (two files sharing an identical shard store it once).
- **Atomic writes**: temp file + `rename` — a half-written shard is never
  visible.
- **No fsync on shards** (a measured trade-off: fsyncing each 1 MiB shard
  divides ingest throughput by ~20, and a shard lost to a machine crash is
  exactly what the scrubber knows how to repair). Manifests, rare and
  precious, stay fsynced.
- **Verified on every read**: `get_shard` recomputes the hash; disk
  corruption (bit rot) yields `CorruptShard`, never wrong bytes.
