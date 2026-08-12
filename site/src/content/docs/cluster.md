---
title: "Placement and healing"
description: "Weighted rendezvous placement, geo-aware spread from network coordinates, self-healing scrubs, rebalancing GC, purge and storage attestation."
---

## Placement by weighted rendezvous hashing (WRH)

"Who should hold shard i of stripe s of file f?" is a **pure function** of
`(file_hash, stripe_idx, shard_idx, weighted view)`:

1. For stripe s, every node gets a score `-weight / ln(h)`, where `h` is a
   uniform (0,1) derived from `blake3(node_id ‖ "\0" ‖ file_hash/s)` and
   `weight` is its **declared disk capacity** (statvfs of the data dir, or
   an explicit `--capacity`; declared within seconds of joining — the
   100 GiB default only covers that brief window). Nodes are ranked by
   descending score.
2. Shard i goes to the node at rank `i mod n`.

The `ln` is a hand-rolled implementation using only basic IEEE operations
(+, −, ×, ÷): libm's `ln` varies from platform to platform, and placement
must be bit-for-bit identical on every node.

Capacity is **declared and near-static**, never *free* space — weighting
by free space would make placement oscillate on every write. The target
equilibrium is that every node fills to the same **percentage**; a change
is re-declared and rebalancing follows via scrub+GC, moving only the
shards that migrate *towards* the changed node.

### Capacity vs durability — the exact semantics

| Size | Behavior |
|---|---|
| `n > k+m` | each stripe picks its k+m hosts out of n: selection proportional to capacity, 1 shard/node/stripe |
| `k+m ≥ n` | every node hosts every stripe; weights decide who takes the "extra" shards |
| `n ≤ k+m` forced case (e.g. n=3, 4+2) | strict 2/2/2 anti-affinity **whatever the weights** — piling > m shards of one stripe onto the big node would make it a single point of failure. Durability first, capacity second; the smallest disk sets the pace |

Every node computes the same placement from the same replicated view —
zero coordination, anti-affinity as soon as `n ≥ k+m`, uniform spread,
incremental stability on membership changes.

## Geo-aware spread: Vivaldi network coordinates

WRH spreads by capacity but has no idea **where** the nodes are — nothing
stops one stripe from landing entirely inside one datacenter. Nauka fixes
that with no GeoIP database and no configuration: every node measures RTT
to its peers (min of 3 pings per pass, so a scheduler hiccup does not read
as geography) and adjusts its position in a Euclidean space where distance
predicts latency (Vivaldi, SIGCOMM'04). As with WRH's `ln`, the square root
is hand-rolled — libm differences across platforms would make two nodes
rank shards differently and fight over them. A node is excluded from
geo-aware placement until its estimated error settles below 0.5.

Placement then applies a **local reordering** of the WRH ranking: for each
shard, if the nominal holder sits within `NEARBY_MS` (15 ms) of a node
already picked for this stripe, a more distant candidate in the same band
of the ranking is preferred. Load balance and anti-affinity are preserved
exactly; without settled coordinates, placement is identical to nominal
WRH. Payoff: a file survives the loss of an entire region.

### Published coordinates are snapped and sticky

Positions live in the Raft state, so every node places from the same
values. They enter it **snapped to a 5 ms grid** and **sticky**: the live
position must drift more than 4 ms from the published point before it is
republished. The reason is v0.5.22's lesson — placement re-runs on every
scrub pass, and a raw position drifting by fractions of a millisecond
re-decided shard ownership each time: the scrubber pushed what the GC had
just released, two nodes chasing each other (observed live: a 3-node
cluster oscillating by a gigabyte per pass). Snapped and sticky, ownership
moves only when a node has genuinely moved — and then it moves once.

## Self-healing (scrub)

Every node checks the shards **it owns**, every 30 s by default
(`--scrub-interval`): a shard that is missing or fails its BLAKE3 check is
regenerated — collect ≥ k valid shards of the stripe, decode, re-encode,
verify against the manifest, store — and pushed to its owners. An
unrecoverable shard (fewer than k survivors *for now*) is retried on the
next pass; nodes come back.

## Rebalancing GC

The "release" counterpart of the scrub, for topology changes: a node
deletes a local shard it no longer owns **only after every current owner
proves possession** — `blake3(nonce ‖ bytes)`, verified against the local
copy. A random nonce cannot be precomputed or replayed, and unlike a
declarative `has_shard`, the proof cannot lie. No proof, no deletion:
redundancy never drops on the strength of a false claim.

## Purge of deleted, expired and banned files

The replicated registry is the truth; local manifests are a cache. Local
manifests absent from the registry (deleted, expired TTL, banned) are
dropped, then shards no live manifest references are deleted — after a 1 h
grace, because a young unreferenced shard belongs to an upload still in
flight whose manifest does not exist yet. The purge runs **only with a
known leader and zero apply lag**: a node lagging behind the log would read
legitimate files as orphans. When in doubt, nothing is deleted. Abandoned
ingest spools are swept with the same grace.

## Storage attestation

The GC's nonce challenge only works for someone who already holds the
bytes. In steady state each shard has one holder, so peers additionally run
**continuous sampled audits**: each scrub pass, download up to 3 shards the
peer owns according to placement and check them against the manifest.
Storage is content-addressed — cheating would mean producing bytes with a
prescribed BLAKE3, a preimage. `missing` that persists is an alert;
`failed` (wrong bytes) is a serious anomaly, logged at `warn`.

## Live membership changes

- **`nauka node add ip:port`** is convergent: a healthy member is
  re-affirmed idempotently; a waiting node is provisioned and joined
  (learner first, promoted to voter once caught up); a wiped machine
  returning under a fresh identity joins while the stale same-address
  identity is **evicted in the same membership change** — no phantom
  voters.
- **`nauka node remove id`** drains: the node leaves the membership but
  keeps serving reads while the others re-replicate its share. You shut it
  down afterwards.

Sequence measured for real: 3 nodes at 16/16/16 shards → `node add` of a
4th → 12/12/12/12 within a few cycles → `node remove` of the 3rd →
16/16/16 on the survivors → the removed node shut down → file downloaded
again, intact.
