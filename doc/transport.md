# Inter-node QUIC transport

## Protocol

One exchange = **one bidirectional QUIC stream**: the client writes a
`Request`, the server answers with a `Response`, framed as
`u32 little-endian length + bincode` (maximum message size: 64 MiB).
Streams on the same connection are multiplexed — several shards fly in
parallel with no head-of-line blocking between them.

```
Request:  Ping | PutShard(bytes) | GetShard(hash) | HasShard(hash)
        | PutManifest(m) | GetManifest(hash) | Raft(RaftRpc)
Response: Pong | PutShardOk(hash) | Shard(Option<bytes>) | Has(bool)
        | PutManifestOk | Manifest(Option<m>) | Raft(bytes) | Error(str)
```

Behavioral details:

- `GetShard` on a shard that is **corrupted server-side returns `None`**
  (as if absent): the client rebuilds it with Reed-Solomon instead of
  receiving wrong bytes.
- Raft RPCs (`RaftRpc::{AppendEntries,Vote,InstallSnapshot,Admin}`) are
  opaque bincode payloads as far as the transport is concerned, handed to
  the local openraft engine through the `RaftHandler` trait wired into the
  server.
- ALPN: `yog/0`.

## The two network planes

Every node in consensus mode opens **two QUIC endpoints**:

| Plane | Port | UDP socket buffers | Role |
|---|---|---|---|
| Data | P (default 7311) | 8 MiB | shards, manifests, admin RPCs |
| Consensus | **P + 1** | 1 MiB | Raft RPCs exclusively |

Why: under saturation, Raft heartbeats queued behind megabytes of shards in
the same socket → timeouts → re-elections at peak load (observed during the
15 GB stress test). Separate sockets mean separate kernel queues, and the
small buffers on the consensus plane **bound the queuing delay**. The
consensus plane **refuses every non-Raft request**: a port collision cannot
turn it into a bogus data plane.

Operational consequence: open **P and P+1 over UDP**, and space ports at
least 2 apart if several nodes share a host.

Regression test (`nauka-raft/tests/priority.rs`): 2.2 GB pushed through in
12 s while registry writes were running — 0 leader changes, 0 failed
writes.

## Throughput tuning (lessons from the 15 GB stress test)

Throughput went from **6 MB/s to ~120 MB/s** by lifting four bottlenecks in
the order they were discovered:

1. **`max_udp_payload_size`** (the real culprit, ×10): quinn's default
   (1472 B) capped the MTU regardless of `initial_mtu` and of path
   discovery. Raised to 65527; the discovered MTU reaches ~16k on loopback,
   jumbo frames in a datacenter, and falls back cleanly to 1200 over the
   internet.
2. **BBR instead of Cubic**: on a fast link with small buffers, Cubic
   collapses on loss (measured: 7 MB/s, 5,495 losses, 526 ms RTT of
   bufferbloat); BBR measures the actual bandwidth and paces its sends.
   Initial window 4 MiB.
3. **8 MiB UDP socket buffers**: the macOS default send buffer is… 9216
   bytes.
4. **Explicit 2 s keep-alive + 30 s idle timeout**: a silent connection
   under congestion no longer dies quietly; failures are loud and the
   (idempotent) retries take over.

Reproducible micro-benchmarks:

```
cargo test -p nauka-transport --release --test bench -- --ignored --nocapture
# raw_quinn_single_stream   : raw quinn throughput + path stats (rtt, cwnd, mtu, loss)
# raw_put_shard_throughput  : throughput of the pipelined put_shard protocol
# single_put_shard_latency  : latency by payload size
```

## TLS

Two modes, chosen when the process starts (see
[identity-and-discovery.md](identity-and-discovery.md)):

- **Cluster mTLS** (keys provided): Ed25519 certificates signed by the
  cluster key, mutual verification, SNI `node.nauka`.
- **Insecure** (no keys): self-signed certificate, client verification
  disabled — encrypted link, unauthenticated peers. Kept for development,
  with a warning at startup.
