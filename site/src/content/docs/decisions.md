---
title: "Design decisions"
description: "The structural choices behind the system, why the alternatives were rejected, and the lessons paid for during stress testing."
---

A journal of the choices that shaped the system, with their reasoning — and
the lessons paid for in debugging. Worth reading before proposing a change:
most "why not X?" questions already have an answer here.

## Architectural choices

**Reed-Solomon by stripes rather than replication.** 4+2 = survives 2
losses for a 50% storage overhead, where ×3 replication charges 200% for
the same tolerance. Splitting into stripes (4 MiB) enables streaming and
bounds memory use.

**Embedded Raft (openraft) rather than gossip or a central coordinator.**
Strongly consistent metadata, no external dependency, tolerates a failed
minority. Gossip would have been simpler but eventually consistent on
placement — unacceptable for a file registry.

**QUIC (quinn) rather than gRPC/TCP.** Native stream multiplexing (hundreds
of shards in parallel over one connection), 0-RTT resumption, built-in TLS
encryption, and a single protocol for data + consensus + admin. Price paid:
the tuning (see the lessons below).

**Rendezvous hashing rather than ring-based consistent hashing.** No table
to replicate, no vnodes, natural per-stripe anti-affinity, and a view
change relocates only the strict minimum.

**WRH weighted by TOTAL declared capacity, never by free space.** Weighting
by free space would make placement depend on what was just placed →
endless oscillation. With total capacity, the equilibrium is "the same fill
percentage everywhere". The weights live in the Raft state (a shared view
is mandatory: placement computed from divergent local measurements would
make scrub and GC contradict each other). And the score's `ln` is
implemented with basic IEEE operations: libm implementations differ across
platforms, and two nodes that rank differently fight over shards. Finally:
anti-affinity wins over capacity when the two conflict (small cluster) — a
big node concentrating more than m shards of a stripe would become a single
point of failure.

**Mainline DHT + pkarr for the rendezvous — later removed.** Early
versions had nodes discover each other on the BitTorrent DHT. The
technology choice inside that feature was sound: kubo is written in Go
(not embeddable), rust-ipfs is dead, and IPFS is wildly oversized for
publishing ~200 bytes of addresses — the Mainline DHT is older, bigger and
more reliable, and pkarr puts Ed25519-signed DNS records on it, keyed by a
pair derived from the cluster key (nothing extra to distribute). What did
not survive was the premise. Once membership became an explicit CLI act —
`nauka node add` provisions the machine over SSH and takes it through
consensus — a background rendezvous was a second, weaker authority on who
is in the cluster, always one step behind the Raft membership it tried to
anticipate. The discovery layer we removed taught us the real lesson:
membership is a decision, not an observation, and a storage engine should
have exactly one source of truth about it.

**Genesis election — removed with discovery.** No designated node: signed
candidacies on the DHT, the lowest node-id founded the cluster after 12 s
unchallenged, a dead candidate was replaced after 45 s. It existed to make
"the same command on every machine" true, which was the pitch of that era.
It also made cluster birth a 12-second distributed race whose failure
modes (a partition during genesis, two founders) had to be reasoned about
forever after. Today birth is deterministic: the first `serve` on a blank
data dir founds a single-node cluster, `--join` waits to be added instead,
and `node add` grows from there. Two ways to create a cluster is one too
many; an explicit act beats an elegant election.

**The node-id is derived from the public key.** `u64 = blake3(pubkey)[..8]`.
An identity that is proven (mTLS) and computed, instead of an integer
handed down by decree. Borrowed from ChainRage's UUIDv8 — minus the
geography part, which waits for region-aware placement.

**fsync: yes for consensus, no for shards.** An acknowledged Raft vote or
entry must survive a crash (Raft correctness). A shard lost to a machine
crash, on the other hand, is exactly what the scrubber knows how to repair
— and per-shard fsync divided ingest by ~20.

## Lessons paid for (a stress-test chronology)

1. **`cargo test --release` does not rebuild the binaries.** One hour of
   perf debugging against a stale binary. Always run
   `cargo build --release -p nauka-node` before a demo.
2. **quinn's MTU is capped by `max_udp_payload_size`** (1472 B by default),
   not only by `initial_mtu` / path discovery. This was THE bottleneck:
   6 → 83 MB/s once lifted. Path stats (`Connection::stats()`) were what
   made it visible (`mtu=1472`).
3. **Cubic collapses on fast links with small buffers** (5,495 losses,
   526 ms RTT of bufferbloat, MTU black-holed). BBR paces and holds the
   throughput.
4. **macOS: the default UDP send buffer is 9216 bytes.** Always size the
   sockets yourself.
5. **The data plane starves consensus** when they share a socket:
   heartbeats timing out, re-election in the middle of a 15 GB burst.
   Hence the dedicated QUIC plane (port+1, small buffers = bounded delay)
   and the regression test that floods the cluster and checks for zero
   leader changes.
6. **A port collision can be silent.** Node 2 was dying at bind time while
   its traffic was absorbed by node 1's consensus plane (which also served
   the data protocol). Two safeguards: the consensus plane serves ONLY
   Raft, and co-hosted nodes must space their `--listen` ports by at
   least 2 — the collision fails loudly at bind time instead.
7. **With no timeout and no memory of failure, one dead node blocks
   everything.** The API download retried a connection to the vanished node
   for every single shard. Rule: timeouts everywhere (3 s connect, 20 s
   shard), and a failing peer is marked for the duration of the request.
8. **A serialization asymmetry gets billed at restart.** `purge` wrote
   `Some(LogId)` where startup read back a bare `LogId` → phantom index
   (24618) → crash on the first restart after a purge. The double-outage
   persistence tests caught it.
9. **Failures must be loud.** Explicit keep-alive + idle timeout
   everywhere: a connection that lingers is worse than a dead one, because
   idempotent retries know how to handle the latter.
10. **Liveness keyed by address masks a phantom voter.** A wiped and
    reinstalled machine comes back with a fresh node id at its old
    address. The old id stayed a voter; the pinger, keyed by address, saw
    the address answering and read both identities as alive — but quorum
    math counts ids. Three machines, four voters, two sharing one address:
    one more real failure freezes writes while `status` reads green.
    Liveness measures machines; membership counts keys — the two must
    never be joined on the address. Fixes: `/api/status` reports one row
    per member id (and `status` warns when two share an address), and
    `node add` became convergent — the same membership change that admits
    the returning machine's new identity evicts the stale one at that
    address, so the phantom cannot outlive the reinstall.
11. **Stabilize inputs, not comparisons.** Vivaldi coordinates drift a
    little on every RTT sample; placement is a pure function of them, so
    every wiggle re-ranked stripe owners and scrub and GC chased each
    other, moving the same shards back and forth. Tolerance at the
    comparison site cannot fix this: each node would be tolerating around
    a different local value, and two nodes that rank differently fight
    (the same reason the WRH `ln` is hand-rolled). The fix belongs at the
    source: coordinates are published into the Raft state snapped to a
    5 ms grid and sticky — republished only after more than 4 ms of real
    drift, fed by min-of-3 pings. Every node computes from identical
    inputs, and ownership stopped flapping.
12. **The doors first, the cluster second.** `serve` used to found a
    single-node cluster on a blank data dir and only then bind its
    sockets. A busy port aborted the start — after the founding was on
    disk. The retry, on a no-longer-blank data dir, came up as an
    already-founded, unreachable cluster instead of failing. Now `serve`
    pre-binds every socket before founding anything: a busy port fails
    loudly with nothing written. Never persist a commitment you are not
    yet able to serve.

## Accepted debt

Spelled out bluntly in [Known limitations](/operations/#known-limitations-v1), alongside the
lines of innovation.
