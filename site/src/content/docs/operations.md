---
title: "Operations"
description: "Deploying a cluster, the CLI reference, the ports to open, health checks, backup and restore, and the known limitations of v1."
---

## Typical deployment (N VPSes)

```bash
# 1. First machine — installs nauka and founds a systemd-managed cluster:
curl -sSfL https://sh.getnauka.com | sh

# 2. Every next machine, run from the first one:
nauka node add <ip>:7311
```

That's it. `init` (run by the installer) generates the cluster identity
and founds the cluster; `node add` provisions each target over SSH —
binary, systemd unit, identity — and takes it through consensus to a
voting member. The full flow, including the key-directory alternative to
the token, is on the [Deploy](/deploy/) page.

**Firewall — the step that catches everyone out:** open the listen port AND
the next one over **UDP** (default: `7311/udp` and `7312/udp`), as well as
the HTTP port over TCP (default `8080/tcp`). All inter-node traffic is
QUIC, hence UDP. Several nodes on the same host: space the ports at least 2
apart (the consensus plane of one node would collide with the data plane
of the next).

## CLI reference

Global options: `--data-dir <dir>` (default `./nauka-data`),
`--keys <dir>` (enables mTLS + derived identity).

| Command | Role |
|---|---|
| `keygen --out <dir>` | generates the cluster key (refuses to overwrite) |
| `node-info` | this node's node-id + fingerprint (requires `--keys`) |
| `serve` | starts the node (options below) |
| `put <file>` / `get <hash> -o f` / `verify <hash>` / `list` | local operations (no network) |
| `put-remote <file> --peers a,b,c` | encodes and dispatches from the client machine |
| `get-remote <hash> --peers a,b,c -o f` | rebuilds from the reachable peers |
| `init` | founds the first node on this machine, systemd-managed |
| `node add <ip:port>` | provisions a machine over SSH and joins it (learner → voter) |
| `node remove <id>` | live removal (drained by the scrubs) |
| `status [--api <url>]` | members, leader, liveness, capacities, stored bytes |

`serve` options:

| Option | Default | Role |
|---|---|---|
| `--listen` | `0.0.0.0:7311` | QUIC data socket (consensus = port+1) |
| `--advertise` | `--listen` | address advertised to the other nodes |
| `--http` / `--no-http` | `0.0.0.0:8080` | public HTTP API |
| `--scrub-interval` | `30` s | healing + GC cadence |
| `--capacity` | size of the data-dir's filesystem | weight for weighted placement, in bytes |
| `--join` | off | wait to be added by a member instead of founding a cluster on a blank data dir |
| `--egress-quota` | unmetered | monthly egress budget (deprioritized past it, never refused) |
| `--cache-size` | disabled | disk budget of the local stripe cache |

## Health and diagnostics

- `status --api http://<node>:8080`: has a leader been elected? are all
  members present and alive?
- Node logs: `scrub: X checked, Y regenerated, Z unrecoverable` (Y > 0
  means a real repair happened; a persistent Z means too many dead nodes),
  `gc: N shards released` (rebalancing), and `peer … unreachable`
  warnings.
- `verify <hash>` (local): can the file be rebuilt from what this node can
  see?
- The `/api/files` endpoint must return the same list on every node (give
  or take a few hundred ms of replication lag).

## Backup and restore

- **Back up**: the key directory (`cluster-ca.key` above all — losing it
  makes it impossible to add any new node or client machine), and ideally
  the `node.key` files (otherwise a reinstalled node takes on a new
  identity, and the old one has to be retired with `node remove`).
- **Data-dirs rebuild themselves**: a node with a blank disk that restarts
  with its keys rejoins, and healing gives it its share back. (Do not wipe
  more than m nodes at a time!)
- A total cluster shutdown (power cut) is covered: all the state that
  matters is durable in the data-dirs.

## Known limitations (v1)

| Limitation | Workaround / plan |
|---|---|
| No NAT traversal (hole punching/relays) | nodes with a public IP or a forwarded port; relays to come |
| `put-remote`/`get-remote` require explicit `--peers` | go through the HTTP API, or read the addresses from `status` |
| No DELETE/expiry on the API side; orphan-shard GC not implemented | coming along with registry purging |
| HTTP API with no authentication and no quotas | reverse proxy in the meantime |
| Cluster key present on every node | offline certificate issuance to come |
| Unfair bandwidth sharing between concurrent uploads (large streams dominate) | harmless — fair queuing is in the backlog |
| At n ≤ k+m nodes, capacity cannot override anti-affinity (see [Cluster](/cluster/)) | add nodes, or accept that the smallest disk sets the limit |
| No write refusal on a full disk (~95% safeguard) | watch disk usage; safeguard to come |
