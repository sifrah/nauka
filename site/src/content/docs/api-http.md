---
title: "HTTP API"
description: "Upload, download, listing, deletion, expiry and banning over HTTP, plus range requests, the web interface and the encrypted media player."
---

Every node in consensus mode exposes the API (default `0.0.0.0:8080`,
tunable with `--http <addr>`, disabled with `--no-http`). **Any node is a
complete entry point** — upload, download and listing give the same result
everywhere.

The API currently has **no authentication** (v1): put it behind a reverse
proxy if you need one, until the accounts/quotas layer lands.

## `POST /api/upload?name=<name>`

Body: the file's raw bytes. With curl prefer `-T <file>` (streams from
disk); `--data-binary @file` buffers the WHOLE file in the client's RAM
first, which kills large uploads on small machines.

The node buffers the stream to disk (`data-dir/tmp`, BLAKE3 hash computed
on the fly), encodes stripe by stripe, pushes each shard to its HRW owner
(itself included), writes the manifest locally, then records it in the Raft
registry (via the leader). Memory stays bounded to a few stripes whatever
the file's size.

```
curl -X POST -T video.mp4 \
  "http://node1:8080/api/upload?name=video.mp4"
```

`200` response:

```json
{
  "hash": "988f6e61…",
  "size": 30000000,
  "name": "video.mp4",
  "stripes": 8,
  "data_shards": 4,
  "parity_shards": 2,
  "link": "/f/988f6e61…"
}
```

Errors: `500` with a text message (empty file, shard undeliverable after
retries, registry unreachable, …).

## `GET /f/{hash}`

Downloads the file, rebuilt in a **streaming** fashion (one stripe in
memory at a time) from the whole cluster: local shards first, then fetched
from the other members. k valid shards per stripe are enough — dead nodes
and corrupted shards are compensated by Reed-Solomon, invisibly to the
client.

- `Content-Length`: the file's exact size.
- `Content-Disposition: attachment; filename="<name>"` if a name was
  supplied at upload time.
- Integrity: the global hash is recomputed during the stream; an
  unreachable peer is remembered for the duration of the request (3 s
  connection timeout, 20 s transfer timeout) and is not contacted again for
  every shard.
- `404` if the hash is unknown to the registry.

```
curl -o video.mp4 http://node3:8080/f/988f6e61…
```

## `GET /api/files`

The replicated registry (the node's local state, possibly a few hundred ms
behind the leader):

```json
[
  { "hash": "988f6e61…", "size": 30000000,
    "name": "video.mp4", "link": "/f/988f6e61…" }
]
```

## What does not exist yet (v1)

- `DELETE` / file expiry (`UnregisterManifest` already exists on the Raft
  side; what is missing is orphan-shard cleanup).
- Authentication, quotas, rate limiting.
- Multipart uploads / resuming an interrupted upload.

## Web interface

Every node serves the webui (if `webui/dist` exists, or via
`--webui <dir>`): Files (drag-and-drop encrypted upload, local key ring,
share links), Cluster (live status via `GET /api/status`), and
`/d/{hash}#key` (download + decryption in the browser).

The interface derives from the **ZeroFS** webui
([Barre/ZeroFS](https://github.com/Barre/ZeroFS), AGPL-3.0) — see
[`webui/ATTRIBUTION.md`](https://github.com/sifrah/nauka/blob/main/webui/ATTRIBUTION.md).
Browser-side encryption (WebCrypto AES-256-GCM) is bit-for-bit compatible
with `nauka-crypto`: a file uploaded from the CLI decrypts in the browser
and vice versa.

Build it with `cd webui && npm install && npm run build`.

### Partial requests (Range)

`GET /f/{hash}` accepts `Range: bytes=…` and answers `206 Partial Content`
with `Content-Range` (`416` if the range falls outside the file;
`Accept-Ranges: bytes` advertised everywhere, `HEAD` included). Only the
stripes intersecting the range are fetched from the cluster and decoded —
reading 64 bytes in the middle of an 81 MB file costs a single round trip
(measured: ~400 ms on a local cluster, instead of the entire file).

Useful for resuming downloads and for media playback.

### Encrypted media player (`/w/{hash}#key`)

**Nominal mode — streaming.** A Service Worker serves `/stream/{hash}` in
cleartext from the ciphertext: for every range the `<video>` element asks
for, only the relevant AES-GCM chunks are pulled from the cluster (a Range
request over the ciphertext), decrypted and handed back. **Nothing is
loaded ahead of time** — playback starts immediately and a seek costs a
single round trip, whatever the file's size. The key reaches the worker
through IndexedDB, never over the network.

Two traps we hit and fixed, worth knowing before touching
`webui/public/sw-stream.js`:

- a Service Worker's in-memory state is **volatile** (the browser stops it
  between events) — hence IndexedDB rather than a `Map`;
- a worker that streams one response for tens of seconds gets **killed**
  (the player receives a 503) — hence responses capped at 4 MiB, returned
  as 206, which the player stitches together.

**Fallback.** If playback has not started after 6 s (worker unavailable,
restrictive browser), the player silently switches to full in-memory
decryption + a Blob URL: robust, but it has to wait for the entire file, so
it is capped at 600 MB. A "streaming" badge in the interface shows which
mode is active.

## Deletion, expiry and banning

### `DELETE /f/{hash}`
Removes the file from the replicated registry (`204 No Content`, `404` if
it is unknown). Every node then purges the manifests and shards that have
become orphans on its next background pass. Measured: 6/6/6 shards →
0/0/0 in a single cycle on a 3-node cluster.

### TTL — `POST /api/upload?ttl=<seconds>`
The manifest carries an `expires_at`. The **leader** removes expired files
from the registry (once for the whole cluster), and the purge follows
everywhere. Expired files vanish from the listing and are no longer served.

### Banning — `nauka ban <hash> --reason "…"`
To honor a takedown notice or a legal order **without ever reading the
content**: the hash is banned in the Raft state, the file leaves the
registry, `GET` answers **`410 Gone` with the reason**, the shards are
purged, and **re-uploading the same content is refused** (the registry
rejects the manifest). `nauka-node unban <hash>` lifts the measure.

Accepted structural limitation: a ban targets that content byte for byte
only — a re-upload encrypted under a different key yields a different hash.
See [End-to-end encryption](/encryption/#legal-requests-what-the-operator-can-hand-over).

### Purge safety
A node purges **only** if its registry is trustworthy (member of the
cluster, leader known): a freshly started node whose registry is still
empty erases nothing — otherwise it would destroy the cluster. A shard
referenced by another live file is never deleted (tested).
