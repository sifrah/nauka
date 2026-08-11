---
title: "Cluster"
description: "Weighted rendezvous placement, self-healing scrubs, rebalancing GC, storage attestation and topology-aware placement from network coordinates."
---

## Placement by weighted rendezvous hashing (WRH)

"Who should hold shard i of stripe s of file f?" is a **pure function** of
`(file_hash, stripe_idx, shard_idx, weighted view)`:

1. For stripe s, every node gets a score `-weight / ln(h)`, where `h` is a
   uniform (0,1) derived from `blake3(node_id ‖ "\0" ‖ file_hash/s)` and
   `weight` is its **declared disk capacity** (kept in the Raft state via
   the `UpdateNodeStats` command; 100 GiB by default until a node declares
   otherwise). Nodes are ranked by descending score.
2. Shard i goes to the node at rank `i mod n`.

The `ln` is a hand-rolled implementation using only basic IEEE operations
(+, −, ×, ÷): libm's `ln` varies from platform to platform, and placement
must be bit-for-bit identical on every node.

### Capacity vs durability — the exact semantics

The probability of landing **at the top** of the ranking is proportional to
weight. What that implies depends on cluster size:

| Size | Behavior |
|---|---|
| `n > k+m` | each stripe picks its k+m hosts out of n: selection **fully proportional** to capacity, 1 shard/node/stripe |
| `k+m ≥ n` | every node hosts every stripe; weights decide who takes the "extra" shards (between ⌊(k+m)/n⌋ and ⌈(k+m)/n⌉) |
| forced case (e.g. n=3, 4+2) | strict 2/2/2 anti-affinity **whatever the weights** — piling > m shards of one stripe onto the big node would turn it into a single point of failure. Durability first, capacity second (deliberate, tested choice) |

Measured (4 nodes, 3×50 GB + 1×350 GB, 288 shards): 66/63/66/93 — the big
node saturates the anti-affinity ceiling (~33%), the small ones drop to
~22%.

Capacity is **declared and near-static** (the size of the data-dir's
filesystem via statvfs, or an explicit `--capacity`), never *free* space:
weighting by free space would make placement oscillate on every write. The
target equilibrium is that every node fills to the same **percentage**. A
capacity change (>1%) is re-declared and rebalancing follows via scrub+GC,
like any other view change — WRH guarantees that only the shards migrating
*towards* the modified node move at all (tested: doubling one weight
relocates ~1/6 of the shards, with zero movement between unchanged nodes).

Properties:

- **Zero coordination**: every node computes the same placement from the
  same view (the Raft membership, sorted).
- **Anti-affinity**: shards of one stripe land on distinct nodes as soon as
  `n ≥ k+m` — losing a node costs at most 1 shard per stripe (with 3 nodes
  and 4+2: 2 shards per stripe, still ≤ m).
- **Spread**: the ranking changes from stripe to stripe → uniform load
  (measured: 16/16/16 on 3 nodes, 12/12/12/12 on 4).
- **Incremental stability**: adding or removing a node relocates only the
  strictly necessary shards (an HRW property), not the whole cluster.

## Self-healing (scrub)

Every node periodically checks the shards **it owns**:

```
for each manifest known locally:
  for each (stripe, shard) whose owner == me:
    local get_shard OK?             → nothing to do
    missing OR corrupted (hash)?    → repair:
      collect the stripe's shards (local first, then peers, nominal
      owner first) until ≥ k are valid
      decode_stripe → encode_stripe again → the regenerated shard must
      match the manifest hash → stored
```

Per-pass report: `shards_checked / healed / unrecoverable`. An
unrecoverable shard (fewer than k survivors *for now*) is retried on the
next pass — nodes can come back.

## Rebalancing GC

The "release" counterpart of the scrub, for topology changes:

```
for each local shard:
  referenced by no manifest        → skipped (orphan, out of scope for v1)
  owned by me                      → kept
  otherwise: do ALL of its current owners (a shard can be shared by
  several files) supply PROOF — blake3(nonce ‖ bytes), verified against
  our own local copy?
    yes → delete locally
    no (or unreachable) → kept, retried later
```

The "everyone proves, otherwise we keep it" rule guarantees that we never
reduce the cluster's real redundancy by releasing too early — and a proof,
unlike a declarative `has_shard`, cannot lie (see
[Storage attestation](#storage-attestation)).

## Live membership changes

- **`node add ip:port`**: the target is provisioned over SSH, then joins as
  a **learner** (catches up on the log and the snapshot without voting
  rights), then is **promoted to voter**. Rebalancing follows automatically
  over the next scrub/GC cycles.
- **`node remove id`**: the node leaves the membership but **stays up
  during the drain** — it keeps serving reads while the others
  re-replicate its share. You shut it down afterwards.

Sequence measured for real: 3 nodes at 16/16/16 shards → `node add` of a
4th → 12/12/12/12 within a few cycles → `node remove` of the 3rd →
16/16/16 on the survivors → the removed node shut down → file downloaded
again, intact.

## Static mode (no consensus)

`serve --peers a,b,c` without `--node-id`: cluster view frozen in the
configuration, periodic heartbeats + scrub, no replicated registry
(manifests are replicated to every node at upload time by `put-remote`).
Kept for minimal deployments and tests; consensus mode is the nominal one.

## Storage attestation

`has_shard` is declarative: a node can answer "yes" when its disk has been
wiped or silently corrupted. Two complementary proof mechanisms close that
hole — and complete the promise of weighted placement (*declared* capacity
→ capacity *honored*).

### 1. Nonce challenge — used by the GC

`ProveShard { hash, nonce }`: the peer must return
`blake3(nonce ‖ bytes)`. The nonce is drawn at random every time:
impossible to precompute or replay, impossible to produce without actually
re-reading the bytes.

Only verifiable by someone who already holds the bytes — which is exactly
the situation of the **rebalancing GC**: before releasing its copy, a node
now demands this proof from every current owner (instead of a plain
`has_shard`). No proof, no deletion. Redundancy can no longer drop on the
strength of a false claim.

### 2. Sampled audit — continuous monitoring

In steady state, each shard has exactly **one** holder: nobody else has the
bytes to verify a challenge. So the auditor samples shards the peer
**owns according to placement**, downloads them and checks their hash
against the manifest. Since storage is content-addressed, cheating would
mean producing bytes with a prescribed BLAKE3 — a preimage.

Bounded cost: `SAMPLE_PER_PEER` (3) shards per peer per scrub pass.

Reading the reports:

| Field | Meaning |
|---|---|
| `proved` | possession proven (hash matches the manifest) |
| `missing` | the peer fails to supply a shard it owns — transient if its scrubber is lagging, **an alert if it persists** |
| `failed` | bytes with the wrong hash: a serious anomaly, logged at `warn` |
| `unreachable` | peer unreachable — not a fault |

Observed in the field: healthy cluster at `6/6 possessions proven` →
`rm -rf` of one node's shards → `3/6 proven, 3 missing` → back to `6/6`
once its scrubber had regenerated everything.

## Topology-aware placement: Vivaldi network coordinates

Weighted WRH spreads by capacity, but it has no idea **where** the nodes
are: nothing stops the shards of one stripe from landing on three machines
in the same datacenter — correlated failures. Topology-aware placement
fixes that with no GeoIP database and no configuration.

### How positions are learned

Every node measures the round-trip time to its peers (a QUIC ping, on each
background pass) and adjusts its position in a Euclidean space where
**distance predicts latency** (the Vivaldi algorithm, SIGCOMM'04): too far
from a fast peer → it moves closer; too close to a slow peer → it moves
away. A "height" models the incompressible last-mile access cost.

The position is published into the Raft state (`UpdateNodeCoord`) as soon
as it moves appreciably — so **every node computes placement from the same
values**, a non-negotiable condition for scrub and GC not to contradict
each other. As with WRH's `ln`, the square root is hand-rolled (Newton,
basic IEEE operations): libm implementations differ across platforms, and
two nodes that ranked differently would fight over shards.

Each coordinate carries an **estimated error**; until that error drops
below 0.5 (`is_settled`), the node is excluded from topology-aware
placement.

### How placement uses it

`stripe_owners_geo` starts from the WRH ranking (capacity, determinism,
minimal migration) and applies nothing but a **local reordering**: for each
shard, if the nominal holder sits within `NEARBY_MS` (15 ms) of a node
already picked for this stripe, a more distant candidate is preferred — as
long as it belongs to the same "band" of the ranking, which preserves load
balance and anti-affinity exactly.

Guarantees (tested):

- two regions of 3 nodes, 6 shards per stripe: **no stripe fits inside a
  single region**, the split is consistently 3/3;
- load stays uniform to within ±2% per node;
- **without reliable coordinates, placement is identical to nominal WRH** —
  topology awareness cannot degrade a cluster that has not converged;
- single-region cluster: no effect, no instability.

Payoff: a file survives the loss of an **entire region**, not merely of a
machine. Bonus still to come: steering reads to the nearest node (the
coordinates are already there).
