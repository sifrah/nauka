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

```bash
NAUKA_CACHE_SIZE=10GB nauka serve …       # or --cache-size 10GB
```

Unset means disabled. The property that makes it safe is content
addressing: a stripe is cached under the hash of its content, so a cache
entry **cannot go stale** — there is no invalidation protocol because there
is nothing to invalidate. Entries of deleted content age out by LRU and are
swept alongside the shard GC.

The pattern it serves: reconstruct once per region, serve many times
locally. A node fronting readers in one geography pulls each hot stripe
across the WAN a single time; every following read is local disk.

## Using both together

A metered node with a cache is the intended combination: the cache slashes
the *inbound* reconstruction traffic its readers cause, and the budget
shapes the *outbound* serving traffic other regions cause. Neither knob
affects durability — placement, healing and proofs are untouched; these
only decide who pays for bandwidth.
