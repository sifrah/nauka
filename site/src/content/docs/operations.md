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
QUIC, hence UDP. Add `8333/tcp` only for a binary built with the `s3`
feature, and `9100/tcp` only if you widened `--metrics` beyond loopback.
Several nodes on the same host: space the ports at least 2 apart (the
consensus plane of one node would collide with the data plane of the
next).

## CLI reference

Global options: `--data-dir <dir>` (default `./nauka-data`),
`--keys <dir>` (cluster key directory), `--token` (cluster token — prefer
the `NAUKA_TOKEN` environment variable, command lines are visible in
`ps`). Commands run on an initialized machine inherit the identity from
`/etc/nauka/nauka.env` automatically — no re-exporting.

Everywhere a hash is expected, a **unique prefix** works (≥ 4 characters,
git-style), resolved against the local store first, then the cluster.

| Command | Role |
|---|---|
| `init` | founds the first node on this machine, systemd-managed (root + systemd Linux) |
| `node add <ip:port>` | provisions a machine over SSH and joins it (learner → voter) |
| `node remove <id>` | live removal (drained by the scrubs; ids in `nauka status`) |
| `serve` | starts a node by hand (options below) |
| `status [--api <url>] [--json]` | members, leader, liveness, capacities, stored bytes; `--json` for scripts |
| `list [--full] [--local]` | the cluster's files, 16-char hashes (`--full` for 64; `--local` for this machine's own store) |
| `get <hash> -o f` | local store first, else downloaded from the cluster and BLAKE3-verified client-side |
| `verify <hash>` | local check, else the cluster serves the file and the hash is verified end-to-end |
| `put <file>` | encode into this machine's LOCAL store (standalone use, no network) |
| `upload <file>` | client-side AES-256-GCM, then upload — prints a share link with the key in the fragment |
| `download <link> -o f` | download + decrypt + verify a share link |
| `token` | generate a cluster token (the one string that IS the cluster) |
| `keygen --out <dir>` | the key-directory alternative to the token (refuses to overwrite) |
| `node-info` | this node's node-id + fingerprint |
| `ban <hash> --reason "…"` / `unban <hash>` | block a file cluster-wide without reading it (410 on GET) |
| `update [--check]` | self-update from the latest release, checksum verified |

`serve` options:

| Option | Default | Role |
|---|---|---|
| `--listen` | `0.0.0.0:7311` | QUIC data socket (consensus = port+1) |
| `--advertise` | `--listen` | address advertised to the other nodes — the node's placement identity. Give it a real, reachable address; a wildcard advertise warns |
| `--http` / `--no-http` | `0.0.0.0:8080` | public HTTP API |
| `--metrics` / `--no-metrics` | `127.0.0.1:9100` | Prometheus endpoint. Loopback by default on purpose: the exposition describes topology, capacities and peer addresses. Widen it only towards a private scrape network |
| `--scrub-interval` | `30` s | healing + GC cadence |
| `--capacity` | size of the data-dir's filesystem | weight for weighted placement, in bytes |
| `--join` | off | wait to be added by a member instead of founding a cluster on a blank data dir (what `node add` passes to provisioned machines) |
| `--egress-quota` | unmetered | monthly egress budget (env `NAUKA_EGRESS_QUOTA`; human sizes like `500GB`, `20TB`; deprioritized past it, never refused) |
| `--cache-size` | auto: 10% of free disk (1GB floor, 50GB cap) | disk budget of the local stripe cache (env `NAUKA_CACHE_SIZE`; `0` disables; content-addressed so never stale, LRU) |
| `NAUKA_SMALL_THRESHOLD` | 131072 | files at or under this many bytes are replicated (1+m full copies, one-round-trip reads) instead of striped; `0` disables |
| `--no-dns` | DNS on | disables the built-in geo-DNS front door (env `NAUKA_NO_DNS=true\|false`); a failed :53 bind only warns |

`serve` pre-binds every socket before founding anything: a busy port fails
loudly with nothing written to the data dir. On a blank data dir the first
`serve` **founds** a single-node cluster.

## Health and diagnostics

- `nauka status [--api http://<node>:8080]`: has a leader been elected?
  are all members present and alive (●)? It also warns when two members
  share an address — the signature of a stale identity left behind by a
  reinstalled machine. Plain HTTP, no cluster identity needed; `--json`
  passes through the raw [`/api/status`](/api-http/#get-apistatus)
  document for scripts and monitoring.
- Prometheus metrics on `127.0.0.1:9100` (unless moved): scrub results,
  shard fetch sources, degraded writes, egress ledgers.
- Node logs (`journalctl -u nauka`): `scrub: X checked, Y regenerated, Z
  unrecoverable` (Y > 0 means a real repair happened; a persistent Z means
  too many dead nodes), `gc: N shards released` (rebalancing), and
  `peer … unreachable` warnings.
- `verify <hash>`: can the file be rebuilt — from the local store if it is
  here, otherwise served by the cluster and hash-checked end-to-end?
- `/api/files` must return the same list on every node (give or take a few
  hundred ms of replication lag).

## Backup and restore

- **Back up the cluster identity above all** — the token, or
  `cluster-ca.key` for key-directory deployments. Losing it means no new
  node and no new client machine can ever join; the running cluster keeps
  running, but it can no longer grow or be administered from a fresh
  machine.
- The `node.key` files are optional. A reinstalled node without its old
  key takes on a new identity; the stale one is either retired with
  `node remove <id>`, or — if the machine comes back at the same address
  via `node add` — evicted automatically in the same membership change
  that admits the new identity.
- **Data-dirs rebuild themselves**: a node with a blank disk that rejoins
  gets its share back through healing. (Do not wipe more than m nodes at
  a time!)
- A total cluster shutdown (power cut) is covered: all the state that
  matters is durable in the data-dirs.

## Known limitations (v1)

| Limitation | Workaround / plan |
|---|---|
| HTTP API with no authentication and no quotas — public API = public files | reverse proxy in the meantime |
| No NAT traversal (hole punching/relays) | nodes with a public IP or a forwarded port; relays to come |
| The cluster key is present on every node — one compromised machine is a compromised cluster membership | offline certificate issuance to come |
| A full disk is not refused: writes fail late instead of early | watch disk usage; a safeguard is planned |
| Unfair bandwidth sharing between concurrent uploads (large streams dominate) | harmless to durability — fair queuing is in the backlog |
