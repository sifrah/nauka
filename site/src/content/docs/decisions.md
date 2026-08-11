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

**Mainline DHT + pkarr rather than IPFS for the rendezvous.** The ChainRage
inheritance without its stack: kubo is written in Go (not embeddable),
rust-ipfs is dead, and IPFS is wildly oversized for publishing ~200 bytes
of addresses. The BitTorrent DHT is older, bigger and more reliable — and
pkarr puts Ed25519-signed DNS records on it. The DHT keypair is **derived
from the cluster key**: nothing extra to distribute, not even a URL.

**Genesis election rather than a `--bootstrap` flag.** No designated node:
signed candidacies on the DHT, the lowest node-id founds the cluster after
12 s unchallenged, and a dead candidate is replaced after 45 s. The same
command everywhere is a product decision ("turnkey") as much as a technical
one.

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

## Accepted debt

Consolidated and prioritized in the [Backlog](/backlog/), alongside the
lines of innovation.
