---
title: "Consensus"
description: "What the Raft log replicates, how membership changes and cluster birth work, and how writes behave when quorum is lost."
---

## What Raft replicates (and what it does not)

The replicated state machine is the cluster's registry — **metadata
only**:

| Entry | Content | Why replicated |
|---|---|---|
| Manifests | `file_hash → FileManifest` | the truth about what the cluster stores; local manifests are a cache of it |
| Capacities | declared disk capacity per node | the weight for weighted placement — every node must place identically |
| Coordinates | Vivaldi network coordinates per node | geo-aware placement, same argument |
| Egress ledgers | bytes served per node, per month | a mid-month restart must not reset the budget |
| Bans | `hash → reason` | banned content must be refused by every node, not just one |
| S3 view | buckets, credentials, in-flight uploads (`s3` feature) | any node answers S3 requests |

Shard bytes **never** go through the consensus log — they travel over
the QUIC data plane. A manifest weighs a few KiB whatever the file's
size: consensus stays lightweight at any storage scale. Raft RPCs ride
their own QUIC endpoint on **port+1** — see [Transport](/transport/).

openraft parameters: heartbeat 500 ms, election timeout 1.5–3 s,
snapshot every 256 log entries, 64 entries kept behind the snapshot so a
lagging follower catches up from the log instead of a full transfer.

## Persistence (data-dir/raft/)

| Item | Backing | Durability |
|---|---|---|
| Log + vote | `raft-log.redb` (redb) | **fsync before the ack** — an acknowledged vote or entry must survive a crash |
| State machine | memory | rebuilt at startup: snapshot + log replay — no fsync on the apply path |
| Snapshot | `snapshot.bin` | atomic write (tmp + fsync + rename) |

## Founding a cluster

Cluster birth is explicit — there is no discovery, so there is nobody to
race:

- `serve` on a **blank data dir founds a single-node cluster**. Before
  founding, it **pre-binds every socket** it will need (data plane,
  consensus plane, HTTP) and releases them: founding writes a cluster's
  birth into the Raft log irreversibly, so a busy port must fail with
  nothing written — founding first and failing to bind after would leave
  a 1-node fork behind for a later start to resurrect.
- `serve --join` does not found: it starts, advertises itself, and
  **waits to be added** by `nauka node add` from a member (a reminder is
  logged every 30 s). This is what `node add` provisions on the target.
- A node with existing Raft state neither founds nor waits — it resumes.

## Membership changes

`nauka node add <ip>` provisions the machine over SSH, then performs one
join path: **AddLearner** (the node catches up without voting), then
**ChangeMembership** promotes it to voter once the learner entry has
committed. The command is **convergent** — it describes a desired state
rather than an action:

- already a healthy member → re-affirmed, idempotently;
- an unjoined node waiting with `--join` → provisioned in place;
- a **wiped machine returning under a fresh id** (reinstall regenerates
  `node.key`) → the old identity at the same address is **evicted in the
  same membership change**. Keeping it would inflate quorum with a
  phantom voter forever — one that even reads as alive, since liveness
  is probed per address and the new node answers there;
- state from **another cluster** → refused (`--force` wipes it first).

`nauka node remove <id>` drains: the node keeps serving while the others
re-replicate its shards, then it can be shut down. Ids are visible in
`nauka status`, which also warns when two members share an address.

## Quorum and write refusal

Every registry write goes through the leader (a follower forwards).
Quorum is a majority of voters — but waiting out a 4 s commit timeout to
learn the cluster is down is a bad answer, so writes are **refused fast**
instead:

- Peers ping each other on the data plane every 5 s (min-of-3); a member
  missing ~15 s of probes reads as down. This map feeds **placement
  only** — membership, votes and identity are untouched by it.
- Before an upload encodes a single stripe, the `can_commit_write` gate
  checks: leader known, and a majority of members alive on that map. If
  not, the client gets **"no quorum"** immediately — no work is wasted
  on any node for a manifest that provably cannot commit.
- The gate is optimistic: an unprobed peer counts as alive, so it never
  manufactures a false refusal on a healthy cluster. Quorum lost
  mid-flight still surfaces as a commit timeout — an availability
  failure, reported as one.

## Admin RPCs

Carried as `RaftRpc::Admin` on the consensus plane; the CLI reaches any
node and follows leader redirects across elections:

```
Init(members)                  cluster birth (once, on one node)
AddLearner { id, addr }        add as learner (catches up without voting)
ChangeMembership([ids])        change the set of voters
Write(cmd)                     write to the registry
Metrics                        id, leader, members, capacities, applied index
ListManifests                  registry keys
```

Covered by tests (`nauka-raft/tests/`): leader crash under live traffic
(re-election ~2 s, nothing committed is lost), resurrection from an
empty disk (snapshot + log catch-up), and full-cluster `kill -9` with
restart from the data dirs — registry intact, cluster writable again.
