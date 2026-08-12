---
title: "Durability & consistency"
description: "The contract: what survives what, what a write means, what a read guarantees — and how each claim was tested."
---

A storage engine owes you a contract, not adjectives. This page is Nauka's:
each claim states the mechanism that enforces it and the way it was tested.

## What a write means

`POST /api/upload` answers when every stripe is encoded and its shards are
on their owner nodes — the response's `degraded_shards` field is the honest
count of shards that could NOT be delivered (a dead peer mid-upload):

- **`degraded_shards: 0`** — the file is fully replicated: any 2 shards per
  stripe can vanish right now and the file survives.
- **`degraded_shards: n`** — the write landed under-replicated; the missing
  shards are parked locally and the scrubber completes them. Degraded, not
  lost — but you are told.

The manifest is registered in the replicated registry (Raft) **last**: a
file either appears whole or not at all. A truncated upload never becomes an
object. If the cluster cannot commit the registry write — no leader, no
quorum — the upload is refused immediately with a retryable error rather
than left hanging.

## What a read guarantees

Content is addressed by BLAKE3 at two levels: every shard by its own hash,
every file by the hash of its bytes. On read:

- each shard is checked against its manifest hash — a corrupted shard is
  treated as **lost**, never decoded;
- any `k = 4` intact shards per stripe reconstruct the stripe;
- the CLI (`nauka get`, `nauka verify`) re-hashes the delivered bytes and
  refuses to keep output that does not hash back to its address.

There is no "read repair returned stale data" class of bug to reason about:
a hash either matches or the read fails loudly.

## What survives what

With the default 4+2 profile, each stripe becomes 6 shards and survives the
loss of **any 2**. What that buys in real failures depends on how many
nodes carry the stripe:

| Failure | 3-node cluster | 6-node cluster | Verified how |
|---|---|---|---|
| 1 disk sector / corrupted shard | ✅ caught on read, healed | ✅ | corruption tests in CI |
| 1 node down (temporarily) | ✅ reads reconstruct, writes route around it | ✅ | measured: 1 GiB read with a node down, 24 s |
| 1 node destroyed | ✅ healed from survivors | ✅ | wiped a live node twice; files intact |
| 2 nodes destroyed at once | ❌ (2 nodes = 4 shards lost > m) | ✅ with 1 shard/node | arithmetic |
| whole-cluster power loss | ✅ all state is durable on disk | ✅ | power-cut integration tests |
| a region, if nodes span regions | shards are pulled apart across distant nodes | ✅ | [placement](/cluster/) |

Two rules of thumb fall out of the table:

- **n ≤ 6 nodes**: every node carries a slice of every stripe, so the
  cluster tolerates ⌊m / shards-per-node⌋ simultaneous node losses — one,
  with 3 nodes. Add nodes to raise it.
- The floor is arithmetic, not implementation: surviving `t` of `N` nodes
  requires storing at least `N/(N−t)`× the data. Nauka's Reed-Solomon sits
  exactly on that bound.

## Healing, continuously

Every 30 seconds (configurable) each node scrubs: it walks every manifest,
verifies its own shards, regenerates missing or corrupt ones from the
survivors and pushes them to their owners. Redundancy is only ever
*released* against a *proof*: before a node deletes a shard the new owner
must answer a `blake3(nonce ‖ bytes)` challenge — a peer cannot free your
safety margin by merely claiming it holds the data. Peers are also audited
continuously by sampling, so quiet corruption on a node that never reads
is still found.

## Consistency model

The registry — which files exist, their manifests, bans, capacities — is a
Raft state machine: **linearizable writes, quorum-committed**. A file listed
by `/api/files` is durably registered on a majority of voters. Reads of file
*bytes* are as fresh as the registry entry that names them — and since
content is immutable under its hash, there is nothing staler than "absent".

When quorum is lost, writes fail fast and reads keep working from whatever
shards are reachable. The engine never trades away the W in CAP silently:
you get an explicit `no quorum` refusal, not a write that may or may not
have happened.

## The receipts

The claims above are exercised by integration tests that kill processes,
cut power to the whole cluster, saturate the network and corrupt shards on
purpose — and by operating a real 3-node, two-provider cluster through
node wipes, replacements, reboots and a deliberately induced quorum loss.
What is still missing before production use is listed bluntly in
[Operations](/operations/#known-limitations-v1).
