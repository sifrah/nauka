---
title: "End-to-end encryption"
description: "Client-side AES-256-GCM, why the key belongs in the URL fragment, what a node can and cannot see, and what an operator is able to hand over."
---

**Nodes store and serve bytes they cannot read.** The file is encrypted on
the client BEFORE Reed-Solomon splitting; the server shards, scatters,
repairs and serves ciphertext, never seeing the content, the key, or (by
default) even the file name.

## Usage

```
# encrypt locally, then upload — prints the complete link:
nauka upload plans.pdf --api http://node1:8080
→ http://node1:8080/f/4fae2bb2…#RO_5yMPbAwtIn0kl1UVHQeG…

# download + decrypt + verify (the complete link, with the #…):
nauka download "http://node3:8080/f/4fae2bb2…#RO_5yMPb…" -o plans.pdf
```

The link works from **any node** (swap the host, the hash and the key stay
the same).

## Why the fragment (#) is the right place for the key

By construction of HTTP, **the fragment is never sent to the server** —
not in the request, not in the logs, not to proxies. Anyone holding the
complete link can decrypt; anyone holding only the hash (the nodes, a
registry snoop) can do nothing. This is the "the link IS the capability"
model popularized by Mega and Firefox Send.

## Cryptographic scheme

- **Key**: 32 random bytes per file, base64url-encoded in the fragment.
- **AES-256-GCM in 1 MiB chunks** (STREAM construction): nonce = random
  prefix (8 B) ‖ big-endian counter (4 B); the "last chunk" flag lives in
  the additional authenticated data (AAD). As a result, modification,
  truncation, reordering and appended data are all detected — not just
  flipped bytes.
- **Why AES-GCM and not XChaCha20**: it is the only AEAD native to
  WebCrypto — a browser client (a product built on the engine; the engine
  itself ships no web interface) can decrypt with no wasm library.
  (AES-NI / ARMv8-crypto make it fast everywhere.)
- Formats: header `"NKA1" ‖ prefix(8)`, then per chunk
  `u32 little-endian length ‖ u8 flags ‖ ciphertext(+16 B tag)`.
  Total overhead: ~16 B/MiB + a 12 B header (~0.002%).

Server-side BLAKE3 hashing (shard integrity, healing, dedup) applies to the
**ciphertext** — the two integrity layers are independent: the cluster
proves it returns the bytes it was given, the AEAD proves those are the
bytes the sender encrypted.

## What the server sees / does not see

| Visible server-side | Invisible |
|---|---|
| ciphertext size (≈ real size) | file content |
| ciphertext hash | decryption key |
| timestamps, access frequency | file name (unless `--name` is passed) |

`--name` deliberately publishes a cleartext name (shown in `/api/files` and
in `Content-Disposition`) — by default, nothing.

## Limitations and accepted trade-offs

- **Lose the link, lose the file.** No recovery is possible; that is the
  zero-knowledge bargain.
- **No cross-file dedup**: two uploads of the same file yield two different
  ciphertexts (different keys). Convergent encryption would allow it but
  reveals that two contents are equal — rejected.
- Size and access patterns remain observable (padding and cover traffic are
  out of scope).
- `curl` can still upload cleartext through the raw API — encryption is
  client-side by nature. `nauka upload` always applies it; the raw
  `POST /api/upload` stores whatever bytes it is given.

## Legal requests: what the operator can hand over

| Can hand over | Cannot hand over |
|---|---|
| the ciphertexts (reconstructed encrypted files) | the cleartext content |
| hashes, sizes, timestamps | decryption keys |
| network logs, if the operator keeps any | — |
| deletion / blocking of a hash (`DELETE /f/{hash}`, `nauka ban`) | — |

The key never transited to the server (it lives in the URL fragment, which
HTTP does not send): there is **nothing to seize** on the nodes that would
allow decryption. Authorities obtain the content through the **complete
link** — the uploader's or a recipient's device, the messaging app it
travelled through — and not from the hosting provider.

Corollary for the operator: document this design and provide an abuse
contact point. Deletion and blocking by hash exist —
[`DELETE /f/{hash}` and `nauka ban`](/api-http/#expiry-and-banning) —
so a takedown can be honored without ever reading the content.
Key-disclosure obligations target whoever
holds the key — the user, not the host. This is not legal advice: have it
reviewed by a lawyer according to your country and your status (host vs.
publisher).
