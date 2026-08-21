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
| `node disable <ip:port>` | drains a member WITHOUT removing it: it stays a voter and keeps serving reads, but leaves the placement view — the others take over its shards (proof-gated) and its store empties. Reversible with `node enable`. Watch it in `nauka top`; at 0 B, removal is instant |
| `node enable <ip:port>` | puts a drained node back into the placement view; shards migrate back over the next scrubs |
| `node remove <id>` | drops a member from the cluster; after a `disable` drain, instant and safe |
| `serve` | runs a node in the foreground — what the systemd unit calls. `--listen 0.0.0.0:7311`, `--advertise`, `--join` (wait to be added instead of founding), `--http`, `--metrics`, `--scrub-interval 30`, `--capacity`, [`--egress-quota`, `--cache-size`, `--extent-cache-size`](/egress-and-cache/). Co-hosted nodes: space `--listen` ports by ≥ 2 |
| `status` | members, leader, liveness, capacities, stored bytes. Remote operator reads require the cluster identity. `--api <url>`, `--json` |
| `top` | the authenticated live, full-screen cluster view (htop-style): per-node fill with sparklines, migration rates during a rebalance, the registry one keypress away (`2`, type to filter). `--api <url>`, `--interval <s>` |

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

## Organisations & spaces

The [multi-tenant layer](/multi-tenant/): organisations are the
engine's clients, spaces their storage units, keys what signs their
requests.

| Command | What it does |
|---|---|
| `org create <name>` | creates an organisation (lowercase, digits, dashes) |
| `org list` | organisations and their spaces, with status |
| `org suspend <name>` / `org resume <name>` | cuts / restores everything under the org, cluster-wide |
| `org rm <name>` | deletes the org — refused while it still has spaces |
| `space create <org>/<name>` | creates a space; `--public` serves its files bare (direct links) |
| `space list [org]` / `space suspend` / `space resume` / `space rm` | same verbs, space-scoped — `rm` refused while the space still references files |
| `space files <org>/<name>` | the files the space references, with sizes and total |
| `space key add <org>/<name> --role admin\|signer` | generates a keypair locally, registers the public half; prints the private key ONCE. `--name` for rotation handles, `--public-key <hex>` to register an externally-generated key |
| `space key ls` / `space key rm <space> <name-or-prefix>` | list keys / revoke one — its signatures die cluster-wide |
| `space sign <space> --key nsk_…` | signs a write offline and prints the `X-Nauka-*` headers plus a ready-to-paste curl. `--method`, `--path`, `--content-hash` to bind the exact bytes |
| `space link <space> <hash> --key nsk_…` | mints a signed READ link offline (`--ttl` seconds or absolute `--exp`, `--rate` bytes/s and `--conc` max simultaneous connections, both bound into the signature); works with `signer` and `admin` keys |
| `space set <space> --rate-default <bytes/s\|off> --quota <bytes\|off> --egress-quota <bytes\|off>` | the space's policies: bare-read speed, storage cap (uploads refused past it), monthly egress cap (reads crawl past it) |
| `space usage <space>` / `org usage <org>` | consumption against the caps |
| `org set <org> --quota <bytes\|off>` | cap on the sum of the org's spaces |
| `space publish <space> <hash> [--to <space>] --key nsk_…` | references an existing file from another space of the same org (no re-upload) — publish to a public space, or adopt an unowned legacy file without `--to` |

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
