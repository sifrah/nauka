---
title: "Egress budgets & cache"
description: "Monthly egress budgets for metered links — deprioritized, never refused — and a content-addressed stripe cache that cannot go stale."
---

Two knobs for clusters whose nodes do not live in the same rack: a monthly
egress budget for metered links, and a local stripe cache for reads that
cross the WAN once and should not cross it twice.

## Egress budgets

Storage placement balances a stock — bytes on disk against declared
capacity. Egress budgets balance the matching flow: bytes *served* against
a declared monthly allowance, for nodes on metered links — a 20 TB/month
dedicated server, a capped home connection, a cloud VM whose provider
bills every byte out.

```bash
NAUKA_EGRESS_QUOTA=20TB nauka serve …     # or --egress-quota 20TB
```

Plain bytes and human sizes are accepted (`500GB`, `1.5TB`, `512MiB`).
Unset means unmetered. On a systemd deployment, set it in
`/etc/nauka/nauka.env` and `systemctl restart nauka`.

The semantics are deliberate: a node past its budget is **deprioritized,
never refused**. Reads prefer pulling shards from nodes with budget to
spare; an exhausted budget shifts load while alternatives exist and yields
when they don't — serving the file always wins over saving a node's
bandwidth bill.

The ledger is replicated in the cluster state, so a mid-month restart does
not zero the count, and every node routes around the same numbers. Watch it
per node: `nauka_egress_served_bytes` vs `nauka_egress_quota_bytes` on
[the metrics endpoint](/monitoring/).

## Per-node stripe cache

Reading a file reconstructs its stripes from shards that may live an ocean
away. The stripe cache keeps decoded stripes on local disk after they
crossed the cluster once:

The cache is **on by default**, sized automatically at 10% of the free
disk at startup (floor 1GB, cap 50GB; a nearly-full disk gets none —
the shard store always has priority). Override or disable it
explicitly:

```bash
NAUKA_CACHE_SIZE=10GB nauka serve …       # fixed budget (or --cache-size)
NAUKA_CACHE_SIZE=0 nauka serve …          # cache disabled
```

The property that makes an always-on cache safe is content
addressing: a stripe is cached under the hash of its content, so a cache
entry **cannot go stale** — there is no invalidation protocol because there
is nothing to invalidate. Entries of deleted content age out by LRU and are
swept alongside the shard GC.

The pattern it serves: reconstruct once per region, serve many times
locally. A node fronting readers in one geography pulls each hot stripe
across the WAN a single time; every following read is local disk.

**And the caches cooperate.** Before paying `k` shard fetches from far
owners, a node asks its closest neighbor (by network coordinates, under
30 ms) whether it already holds the decoded stripe: one local transfer
instead of four distant ones. The lookup rides the authenticated
inter-node transport (a decoded stripe is content, and content has
owners — it is never exposed over public HTTP), answers from cache only
(a miss never triggers a reconstruction on the neighbor), and the bytes
are verified by re-encoding them against the manifest's shard hashes —
the transport authenticates the peer, never the content. Net effect:
only one node per region ever pays the cold read; measured on a
tri-region cluster, the second node of a region reads a file its
sibling holds at 77 MB/s where a lone node reconstructs at 20.

**And the cache listens for intent.** Two signals queue a background
warm of the whole file, at low priority (two stripes in flight, never
competing with a paying read) and bounded (a file bigger than a
quarter of the cache budget is skipped): publishing into a public-read
space warms the node that took the publish — that gesture says "this
is about to be served" — and three partial reads of the same file
within fifteen minutes warm it fully, because seeks are how players
and download managers announce the rest of their plans. Measured: a
publish left the node serving the bare URL at 1.25 GB/s before the
first visitor; three 1 MB range reads turned the next full GET into
888 MB/s of local disk.

## Verified Range extent cache

Small and random Range reads have a different access pattern. Reading 4 KiB
cannot safely trust only those 4 KiB with the current manifest format: the
manifest contains one BLAKE3 hash per shard, not a Merkle tree of smaller
blocks. Nauka therefore reads and verifies the complete covering shard once,
keeps that verified extent in a bounded RAM LRU, and returns the exact byte
slice requested. Concurrent cold reads of the same shard or stripe are fused
into one load. On a single-node cluster this still avoids repeated disk reads
and BLAKE3 work; on a distributed cluster it also avoids duplicate transfers
and Reed-Solomon reconstructions.

The RAM cache is independent from the decoded-stripe disk cache above. It is
enabled by default with a 128 MB budget:

```bash
NAUKA_EXTENT_CACHE_SIZE=512MiB nauka serve …  # or --extent-cache-size 512MiB
NAUKA_EXTENT_CACHE_SIZE=0 nauka serve …       # disable the RAM cache
```

Plain bytes and human sizes are accepted. The default keeps the working set
bounded: payloads, keys and a conservative allocation overhead are charged to
the budget, the table is capped at 4,096 entries, and at most 32 distinct cold
loads may run concurrently. Eviction never affects correctness because shards,
manifests and Reed-Solomon remain the source of truth. Monitor
`nauka_extent_cache_bytes`, `nauka_extent_cache_accounted_bytes`,
`nauka_extent_cache_hits_total`,
`nauka_extent_singleflight_waiters_total`, and compare
`nauka_range_backend_bytes_total` with `nauka_range_requested_bytes_total`
to see the real backend amplification.

S3 customer-key encryption is the exception to windowed reconstruction: Nauka
must authenticate and decrypt the encrypted segments before slicing the
plaintext response. Those reads keep the full-download path and do not pollute
the optimized Range-cache amplification counters.

## Using both together

A metered node with a cache is the intended combination: the cache slashes
the *inbound* reconstruction traffic its readers cause, and the budget
shapes the *outbound* serving traffic other regions cause. Neither knob
affects durability — placement, healing and proofs are untouched; these
only decide who pays for bandwidth.
