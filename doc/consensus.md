# Raft consensus

## What Raft replicates (and what it does not)

The replicated state machine holds **metadata only**:

- the **file registry**: `file_hash → FileManifest` (`RegisterManifest` /
  `UnregisterManifest` commands);
- the cluster **membership** (handled natively by openraft).

Shard bytes **never** go through the consensus log — they travel directly
over the QUIC data plane. A manifest weighs a few KiB whatever the file's
size: consensus stays lightweight at any storage scale.

The replicated registry is the **source of truth**: every node
materializes the manifests it discovers there, then its scrubber goes and
fetches the shards it owns. A node that missed an upload converges on its
own.

## openraft parameters

```
heartbeat_interval        500 ms
election_timeout          1.5 – 3 s
snapshot_policy           LogsSinceLast(256)
max_in_snapshot_log_to_keep  64
```

Network: RPCs (`append_entries`, `vote`, `install_snapshot`) ride on our
own QUIC, over the **dedicated consensus plane (port+1)** — see
[transport.md](transport.md).

## Persistence (data-dir/raft/)

| Item | Backing | Durability |
|---|---|---|
| Log + vote + committed + last_purged | `raft-log.redb` (redb) | **fsync before the ack** — a Raft correctness requirement: an acknowledged vote or entry must survive a crash |
| State machine (registry) | memory | rebuilt at startup: snapshot + log replay by openraft — **no fsync on the apply path** |
| Snapshot | `snapshot.bin` | atomic write (tmp + fsync + rename) |

The redb log stays bounded: a snapshot every 256 entries, then a purge
(keeping 64 entries of slack so that lagging followers can catch up from
the log rather than from a full snapshot).

Scenarios covered by the tests:

- **Leader crash under live traffic** → re-election in ~2 s, writes resume,
  zero loss of anything already committed.
- **Resurrection from empty state** (disk lost) → full catch-up from the
  leader (snapshot + log).
- **Total cluster outage** (all n nodes down, `kill -9` included) →
  restart from the data-dirs, registry intact, cluster writable again.
  Tested both as a pure log replay AND as snapshot + purge + leftovers.

## Writes and administration

Every write goes through the leader. Two paths:

- **Node side**: `RaftApp::write(cmd)` — local `client_write` if leader,
  otherwise forwarded to the leader over the transport (used by the HTTP
  API).
- **CLI client side**: `admin_via_leader(peers, req)` — tries each peer,
  follows `ForwardTo` redirects, retries across leader changes.

Admin RPCs (carried by `RaftRpc::Admin`):

```
Init(members)                  cluster initialization (once)
AddLearner { id, addr }        add as learner (catches up without voting)
ChangeMembership([ids])        change the set of voters
Write(cmd)                     write to the registry
Metrics                        id, leader, members, applied index
ListManifests                  registry keys
```

## Measured performance

- ~1,300–1,500 registry writes/s (32 concurrent writers, debug build,
  before redb persistence — the durable version pays one fsync per append
  batch, amortized by openraft's batching).
- 500 concurrent writes converged across 3 nodes: see
  `nauka-raft/tests/stress.rs`.
