---
title: "CLI reference"
description: "Every nauka command with a real example — cluster lifecycle, files, identity, moderation, maintenance."
---

One binary, `nauka`. Global flags: `--data-dir <dir>` (default
`./nauka-data`, only used by local-store commands), `--token` (prefer the
`NAUKA_TOKEN` env var — argv is visible in `ps`), `--keys <dir>`. On an
initialized machine, commands that need the cluster identity inherit it
from `/etc/nauka/nauka.env` automatically.

Everywhere a file hash is expected, a **unique prefix** works (≥ 4
characters, git-style): `nauka get 18445b -o x`.

## Cluster lifecycle

| Command | What it does |
|---|---|
| `init` | turns this machine into the first node: dedicated user, identity in `/etc/nauka`, hardened systemd unit, founds the cluster. Root + systemd only; `--advertise <ip>:7311` to override detection |
| `node add <ip>:7311` | provisions a machine over SSH and joins it — [convergent](/growing/): blank → joined, member → re-affirmed, replaced → stale identity evicted. `--ssh-user`, `--force` (wipe), `--peers` |
| `node remove <id>` | drains a member out of the cluster; shut the machine down after the scrubs settle |
| `serve` | runs a node in the foreground — what the systemd unit calls. `--listen 0.0.0.0:7311`, `--advertise`, `--join` (wait to be added instead of founding), `--http`, `--metrics`, `--scrub-interval 30`, `--capacity`, [`--egress-quota`, `--cache-size`](/egress-and-cache/). Co-hosted nodes: space `--listen` ports by ≥ 2 |
| `status` | members, leader, liveness, capacities, stored bytes — plain HTTP, no identity needed. `--api <url>`, `--json` |

```bash
$ nauka status
cluster — 3 nodes, 3 alive · 353 files, 3.34 GiB stored · 117.79 GiB capacity
  ● 163.172.181.194:7311           35.04 GiB  355474566507203597  (this node)
  ● 2.28.25.61:7311       leader   74.77 GiB  14193759605909860198
  ● 51.158.64.90:7311               7.98 GiB  6618550476704767285
```

## Identity

| Command | What it does |
|---|---|
| `token` | prints a fresh cluster token to stdout (pipeable into a secret store). The token IS the cluster — treat it like a password |
| `keygen --out <dir>` | the file-based alternative: an Ed25519 cluster CA to copy to each machine and pass with `--keys`. Refuses to overwrite |
| `node-info` | this node's Raft id and fingerprint. On a server, reads the *service's* identity |

## Files

| Command | What it does |
|---|---|
| `list` | the cluster's files — short hash, size, name. `--full` for 64-char hashes, `--local` for this machine's own store, `--api <url>` |
| `get <hash> -o <file>` | local store first, else downloaded from the cluster and **BLAKE3-verified locally** — bytes that do not hash back are deleted, not delivered |
| `verify <hash>` | local integrity check, or — for a cluster file — has a node reconstruct and serve it, hash verified end-to-end |
| `put <file>` | the standalone encoder: Reed-Solomon into the LOCAL store, no network. `--data-shards`, `--parity-shards` |
| `upload <file>` | client-side end-to-end encryption (AES-256-GCM) then upload; prints a share link whose `#fragment` carries the key — [the server never sees it](/encryption/) |
| `download <link> -o <file>` | fetches a share link and decrypts it locally |

```bash
$ nauka get 18445b -o screenshot.png
✓ downloaded from the cluster, BLAKE3 verified: 4.51 MiB → screenshot.png
```

## Moderation

| Command | What it does |
|---|---|
| `ban <hash> --reason <ref>` | removes the file from the registry; downloads answer 410 with the reason; shards purged at the next GC. Honors a report without reading the content |
| `unban <hash>` | lifts the ban |

## Maintenance

| Command | What it does |
|---|---|
| `update` | self-update from the latest release — checksum verified, atomic replace; restart the service to run it. `--check` only reports |
| `--version` | the installed version, offline |

```bash
$ nauka update
update available: 0.5.23 → v0.5.24
downloading nauka-0.5.24-x86_64-unknown-linux-gnu.tar.gz…
checksum verified
nauka 0.5.23 → v0.5.24 installed at /usr/local/bin/nauka
restart the node to run it: systemctl restart nauka
```

## Exit codes and scripting

Commands exit 0 on success, 1 on any failure, with the error chain on
stderr. Machine-readable surfaces: `status --json`, the token on stdout
(reminders go to stderr), and the [HTTP API](/api-http/) for everything a
script should really be doing.
