---
title: "HTTP API"
description: "Upload, download, listing, deletion, expiry and banning over HTTP, plus range requests and the exact status schema."
---

Every node exposes the API (default `0.0.0.0:8080`, tunable with
`--http <addr>`, disabled with `--no-http`). **Any node is a complete
entry point** — upload, download and listing give the same result
everywhere.

Authentication is arriving in stages with the
[multi-tenant layer](/multi-tenant/). Today: **writes can be signed
per-space** with Ed25519 keys (see below) — and during the transition,
unsigned uploads and open reads are still accepted. The flip to
private-by-default will be its own explicit, documented change. Until
then, put the API behind a reverse proxy if you need access control
today. Content confidentiality is a separate, already-solved problem:
[end-to-end encryption](/encryption/) keeps the nodes blind to what
they store.

## `POST /api/upload?name=<name>&ttl=<seconds>`

Body: the file's raw bytes. With curl use `-T <file>`:

```
curl -T video.mp4 "http://node1:8080/api/upload?name=video.mp4"
```

`-T` streams from disk. `--data-binary @file` also works but buffers the
**whole file in the client's RAM** before sending a byte — a 1 GiB upload
kills the client on a small machine before the server sees anything. The
server side is streaming either way: the node encodes stripe by stripe as
the body arrives and pushes each shard to its owner (itself included),
memory bounded to a few stripes whatever the file's size.

To upload **into a space**, attach the four signature headers
(`X-Nauka-Space`, `X-Nauka-Key`, `X-Nauka-Timestamp`,
`X-Nauka-Signature`, plus `X-Nauka-Content-Hash` to bind the exact
bytes) — `nauka space sign` prints them, and the
[multi-tenant page](/multi-tenant/#signed-writes) specifies the exact
canonical string for implementing it in your own backend. An `admin`
key is required; `401`/`403` answers carry the reason and the remedy.
A signed upload records the space's [reference](/multi-tenant/#files-belong-to-spaces-references)
on the file (the response then carries `"space"`), and `GET /api/files`
lists each file's `spaces` — same bytes uploaded by two spaces = one
set of shards, two references. Past the space's (or org's)
[storage quota](/multi-tenant/#quotas-storage-refused-egress-throttled),
the upload is refused `403` with the numbers; past the space's monthly
egress quota, reads slow to a crawl and carry
`X-Nauka-Throttled: egress-quota`.

`200` response:

```json
{
  "hash": "988f6e61…",
  "size": 30000000,
  "name": "video.mp4",
  "stripes": 8,
  "data_shards": 4,
  "parity_shards": 2,
  "link": "/f/988f6e61…",
  "degraded_shards": 0
}
```

`hash` is the BLAKE3 of the whole file — the file's permanent address.
`degraded_shards` counts the shards that could not be delivered to their
owner node during the upload: `0` means the write is fully replicated;
anything above means a node was down or slow, the missing redundancy is
parked on the ingesting node, and the scrubber completes it in the
background. The upload is aborted, never silently under-protected, if a
stripe cannot reach at least k placed shards.

Errors:

- `415 Unsupported Media Type` — the body was a multipart form. This
  endpoint takes raw bytes; accepting multipart would store the form
  framing, boundary and headers included, verbatim as the object.
- `400 Bad Request` — empty body (a typoed curl, a missing file).
- `503 Service Unavailable` — the registry cannot commit the write right
  now (no leader, no quorum). Transient: retry.
- `500` — this upload genuinely failed.

**Name semantics.** The name is a per-hash slot in the registry, not part
of the content's identity. Re-uploading existing content with `?name=`
sets the name; re-uploading it **without** `?name=` preserves the existing
one — the second uploader rarely means "unname it".

`ttl=<seconds>` gives the file an expiry; see
[expiry](#ttl--post-apiuploadttlseconds) below.

## `GET /f/{hash}` — and who may call it

Reads follow [ownership](/multi-tenant/#signed-read-links-owned-files-are-private).
A file referenced by an active **public-read** space is served bare. A
file referenced by private spaces only takes a **signed link** —
`?space=<org/space>&exp=<unix>&sig=<hex>` plus optional `&rate=`
(bytes/s, signed: un-removable), Ed25519 over
`nauka-link-v1\n{hash}\n{space}\n{exp}\n{rate|-}`, minted offline by
the space's backend (or `nauka space link`). Bare public reads obey
the space's `rate_default`. `403` otherwise, with the remedy.
Unowned pre-tenant files are still served bare during the transition.
`HEAD` obeys the same gate.

Reconstructs the file and serves it, **streaming** (one stripe in memory
at a time), from the whole cluster: local shards first, then fetched from
the other members. k valid shards per stripe are enough — dead nodes and
corrupted shards are compensated by Reed-Solomon, invisibly to the client.

```
curl -o video.mp4 http://node3:8080/f/988f6e61…
```

- `Content-Length`: the file's exact size.
- `Content-Disposition: attachment; filename="<name>"` if the file has a
  name in the registry.
- Integrity: the global hash is recomputed during the stream; an
  unreachable peer is written off for the duration of the request (3 s
  connection timeout, 20 s per shard) instead of being retried for every
  shard.
- The first stripe is reconstructed **before** the status line is sent:
  a file that is currently unrecoverable answers an honest
  `503 Service Unavailable` rather than a `200` followed by a truncated
  body. A stripe failing later in the stream still truncates — nothing
  better exists mid-stream.

Status codes:

| Code | Meaning |
|---|---|
| `200` / `206` | served (whole file / requested range) |
| `404` | hash unknown to the registry |
| `410 Gone` + `content removed: <reason>` | banned (`nauka ban`) |
| `410 Gone` + `file expired` | TTL elapsed |
| `410 Gone` + `file deleted` | removed from the registry |
| `416` | requested range outside the file (`Content-Range: bytes */<size>` attached) |
| `503` | too many shards currently missing to reconstruct |

`HEAD /f/{hash}` answers the same headers (`Content-Length`,
`Accept-Ranges: bytes`) without a body, and the same `410`s.

### Partial requests (Range)

`GET /f/{hash}` accepts `Range: bytes=…` and answers `206 Partial Content`
with `Content-Range`. Suffix ranges work (`bytes=-500` = the last 500
bytes); an end past the file is clamped; a range that starts past the end
is `416`. Only the stripes intersecting the range are fetched from the
cluster and decoded — reading 64 bytes in the middle of an 81 MB file
costs a single round trip, not the file.

Useful for resuming downloads and for media playback.

## `DELETE /f/{hash}`

Deletion follows [ownership](/multi-tenant/#files-belong-to-spaces-references).
A file **referenced by spaces** requires a signed DELETE from one of
them (the same `X-Nauka-*` headers, method `DELETE`, path `/f/<hash>` —
`nauka space sign --method DELETE --path /f/<hash>` prints them): it
releases *that space's reference*, `204`. The content itself only
disappears with its **last** reference — then the registry entry drops
and each node's GC purges the orphaned shards on its following passes.
An unsigned DELETE on an owned file gets `403` naming the owners.

A legacy file (no references) keeps the open pre-tenant behavior:
unsigned `DELETE` → `204`, `404` if the hash is unknown, and `GET`
answers `410 Gone` after.

## `GET /api/status`

The cluster as this node sees it. This is what `nauka status` reads — no
cluster identity needed, plain HTTP:

```json
{
  "self_addr": "10.0.0.1:7311",
  "self_node_id": 13816319000459994208,
  "leader": "10.0.0.1:7311",
  "nodes": [
    {
      "addr": "10.0.0.1:7311",
      "id": 13816319000459994208,
      "capacity_bytes": 197586380800,
      "is_leader": true,
      "is_self": true,
      "is_alive": true
    },
    {
      "addr": "10.0.0.2:7311",
      "id": 4443749509604496789,
      "capacity_bytes": 98793190400,
      "is_leader": false,
      "is_self": false,
      "is_alive": false
    }
  ],
  "files": 12,
  "total_bytes": 3210987654
}
```

- `id` is the member's Raft id — the value `nauka node remove <id>`
  takes. It is exposed here precisely so it can be read over plain HTTP.
  There is one row **per member**, not per address: two members can share
  an address (a replaced machine whose stale identity lingers), and rows
  keyed by address would collapse them into an indistinguishable
  duplicate.
- `is_alive` is **this node's** view from its own pinger, not a
  cluster-wide verdict: `false` once the peer has missed ~15 s of probes.
  A member reads as down for placement — it takes no new shards — but
  stays a full member; membership only changes through
  `node add`/`node remove`. The map is optimistic: a peer nobody has
  probed yet reads alive.
- `files` / `total_bytes` come from the replicated registry.

## `GET /api/files`

The replicated registry (this node's local copy, possibly a few hundred
ms behind the leader). Expired files are filtered out:

```json
[
  { "hash": "988f6e61…", "size": 30000000,
    "name": "video.mp4", "link": "/f/988f6e61…" }
]
```

## Expiry and banning

### TTL — `POST /api/upload?ttl=<seconds>`

The manifest carries an `expires_at`. Past it the file disappears from
`/api/files`, `GET` answers `410 Gone` with `file expired`, and the purge
reclaims the shards cluster-wide.

### Banning — `nauka ban <hash> --reason "…"`

To honor a takedown notice or a legal order **without ever reading the
content**: the hash is banned in the Raft state, the file leaves the
registry, `GET` answers `410 Gone` with `content removed: <reason>`, the
shards are purged, and **re-uploading the same content is refused** (the
registry rejects the manifest, and the upload fails). `nauka unban <hash>`
lifts the measure.

Accepted structural limitation: a ban targets that content byte for byte
only — a re-upload encrypted under a different key yields a different hash.
See [End-to-end encryption](/encryption/#legal-requests-what-the-operator-can-hand-over).

### Purge safety

A node purges **only** when its registry is trustworthy (member of the
cluster, leader known, replication caught up): a freshly started node
whose registry is still empty erases nothing — otherwise it would destroy
the cluster. A shard referenced by another live file is never deleted.

## `POST /f/{hash}/refs?to=<org/space>`

Adds a space's [reference](/multi-tenant/#direct-links-publish-without-re-uploading)
to an existing file — publish to a public-read space, or adopt an
unowned legacy file (`to` = the signing space). Signed (`X-Nauka-*`
headers, admin key; the canonical path includes the `?to=` query).
Chain of custody enforced: the signer must already reference the file,
and the target must belong to the same organisation. Revocation is the
signed `DELETE /f/{hash}` of that reference.

## `GET /api/orgs`

The replicated [organisation/space registry](/multi-tenant/): orgs,
spaces and their policies, and each space's **public** keys (hex, with
role and name — private halves never exist server-side). This is what
`nauka org list` and `nauka space key ls` read.

## What does not exist yet (v1)

- Read-side authentication: signed read links, public spaces,
  private-by-default — the next stages of the
  [multi-tenant layer](/multi-tenant/). Write-side signing exists today.
- Quotas and rate limiting (per-space, also part of that series) — the
  reverse proxy is the interim answer.
- Multipart uploads / resuming an interrupted upload.
