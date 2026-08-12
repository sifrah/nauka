---
title: "Growing and shrinking"
description: "Adding machines, retiring them, replacing a dead one — and the phantom-voter accident this design exists to prevent."
---

Membership is explicit: machines enter and leave the cluster because you
said so, through consensus, with the data movement handled for you.

## Adding a machine

From any existing member, point `node add` at a machine you can SSH into:

```bash
nauka node add 51.15.241.235:7311
```

```text
✓ connected to root@51.15.241.235
✓ target is blank
✓ binary installed (17.63 MiB)   ━━━━━━━━━━ 17.54 MiB (4.8 MiB/s)
✓ installing the cluster identity and unit
✓ node 4921565673213893068 is up
✓ joined the Raft cluster as a voting member
```

The target gets the binary this very command is running (a static build runs
on any x86-64 Linux), the hardened systemd unit, and the cluster identity —
then joins as a learner, catches up, and is promoted to voter. Shards start
rebalancing toward it on the next scrub passes; nothing else to do.

The identity handed over is the one *you* hold: the token from
`/etc/nauka/nauka.env` on an initialized machine, or `NAUKA_TOKEN`/`--keys`
if you run the command from elsewhere. SSH uses your agent — `ssh -A` when
driving from a laptop. Host keys are pinned on first contact and refused
loudly if they ever change.

## `node add` converges — it does not just add

Run it against a machine in *any* state and it does the right thing:

| Target state | What happens |
|---|---|
| blank machine | provisioned and joined (the normal case) |
| healthy member of this cluster | re-affirmed, idempotently — nothing to undo |
| node waiting to be added (`--join`) | provisioned in place, keeps its identity |
| **wiped machine coming back** | joins under its NEW identity; the stale one is **evicted in the same membership change** |
| member of another cluster | refused — wiping someone else's node is not this command's call (`--force` if you insist) |

The fourth row is the load-bearing one, and it comes from a real accident.
A node's identity derives from its key; wipe the disk and the machine
returns as a *different node at the same address*. An early version kept
the old identity in the voter set — a **phantom voter**: counted for quorum
forever, reported alive forever (liveness probes the address, which the new
node answers). Two phantoms at one address made quorum arithmetically
unreachable and wedged a live cluster for writes. `nauka status` now flags
members sharing an address, and `node add` evicts the stale identity
atomically with the join.

## Draining a machine before you remove it

`node remove` already keeps a node serving while its shards re-replicate,
so a straight removal is safe. But when you want to retire a machine
deliberately — decommission a server, move a region — `node disable`
drains it first, reversibly:

```bash
nauka node disable 51.15.222.206:7311
```

The node stays a full member: it still votes, still serves reads, still
holds its registry. What changes is that it leaves the **placement
view** — no new shard is ever sent to it, every shard it holds gains a
new owner elsewhere, the scrubbers migrate them, and its own GC releases
each one once the new owner has proven possession. Its store drains to
zero while the cluster never dips below full redundancy. Watch it empty
in [`nauka top`](/monitoring/) — the row is tagged `draining` — and at
0 B, `node remove` is instant and truly safe.

Changed your mind? `nauka node enable 51.15.222.206:7311` puts it back
in the view and shards migrate toward it again. Nothing was lost.

## Removing a machine

Ids are in `nauka status`. Removal drains rather than amputates:

```bash
nauka node remove 4921565673213893068
```

```text
✓ node 4921565673213893068 removed
Leave it running long enough for the scrubs to re-replicate its shards,
then shut it down.
```

The removed node keeps serving reads while the survivors rebuild its share
of the redundancy — watch `scrub:` lines settle in the journal, or simply
watch `nauka status` byte counts, then power it off. Removing a node that
is already dead works too: removal never requires talking to the machine
being removed.

Do not remove more than `m = 2` nodes' worth of shards at a time, and on a
3-node cluster that means: one at a time, waiting for clean scrub passes
in between.

## Replacing a machine

Two equivalent paths:

- **Same address** — reinstall the OS, then `nauka node add <ip>:7311
  --force` from a member: wipes whatever half-state remains, provisions,
  joins under the new identity, evicts the old one. One command.
- **New address** — `nauka node add <new-ip>:7311`, wait for scrubs to
  settle, then `nauka node remove <old-id>`.

Either way the invariant holds: **one voting identity per address**, and
redundancy is never released before the replacement proves it holds the
data.

## What to expect while it settles

Rebalancing is deliberately unhurried — it competes with real traffic for
disk and egress. A joining node declares its real capacity within seconds
and fills toward the same percentage as everyone else over the following
scrub passes. `nauka status` tells you at any moment who is in, who is
alive, and who holds how much.
