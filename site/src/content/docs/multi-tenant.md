---
title: "Organisations & spaces"
description: "The multi-tenant model: organisations, storage spaces, Ed25519 keys with roles, and signed writes — no shared secrets anywhere."
---

Nauka is an engine serving **applications**, not end users. The
multi-tenant model has exactly two levels:

- An **organisation** is the engine's client: a file-sharing product, a
  gateway, a backup tool. It is the unit of contract — suspend it and
  everything under it goes dark on every node.
- A **space** is the operational unit inside an organisation
  (`myapp/uploads`), and it carries the policies: its own keys, its
  visibility, and (coming) its quotas.

Your application's *own* users never appear in Nauka. Who may download
what, who paid, who is over quota — your application decides all of
that in its own database, and expresses the decision by signing (or not
signing) a request. That boundary is what keeps the replicated registry
small enough to live on every node, which is what makes every check
below **local**: no auth service, no round-trip, no single point of
failure.

Split spaces by *usage*, not by customer: `myapp/uploads`,
`myapp/thumbnails`, `myapp/archives`. Spaces are counted in dozens per
organisation — anything that scales with your customer count belongs in
your database, not in the cluster state.

## Creating an organisation and its spaces

```bash
nauka org create myapp
nauka space create myapp/uploads            # private (the default)
nauka space create myapp/public --public    # served bare, no signature
nauka org list
```

Names are lowercase letters, digits and dashes, 1–32 chars per segment.
`org suspend` / `space suspend` cut reads and writes cluster-wide within
one replication round-trip; `resume` lifts it. An organisation with
spaces cannot be deleted — removal is space by space, deliberately.

## Keys: Ed25519, generated on YOUR machine

```bash
$ nauka space key add myapp/uploads --role admin --name backend
key backend (admin) registered on myapp/uploads
  public : e61b3e9a…122f
  private: nsk_9f2c…
  ^ shown ONCE and stored NOWHERE — put it in your application's secret store now.
```

The private key is generated locally and **never transmitted**. The
cluster replicates only the public half: a compromised node can verify
signatures, it can never mint one. There are no shared secrets anywhere
in the system.

Two roles:

| Role | May do | Belongs on |
|---|---|---|
| `signer` | sign read links only (coming with signed links) | exposed surfaces — web frontends |
| `admin` | authenticated writes, plus everything a signer can | backends, kept warm |

A leaked frontend key can hand out temporary downloads; it can never
write or destroy. Rotation is a non-event: `key add` the new one, move
your app, `key rm` the old — several keys coexist, and removing one
kills its signatures cluster-wide in one round-trip. You can also
register a key generated elsewhere with `--public-key <hex>` (the
private half then never touches this machine either).

```bash
nauka space key ls myapp/uploads
nauka space key rm myapp/uploads backend     # by name, or public-key prefix
```

## Signed writes

An upload for a space carries four headers, an Ed25519 signature over:

```text
{method}\n{path}\n{space}\n{timestamp}\n{content_hash or "-"}
```

`timestamp` is unix seconds; nodes accept ±300 s, so a captured
signature dies in minutes. Signing is **offline** — your backend holds
the private key and computes a signature; possession of the key IS the
permission. No call to Nauka, no token endpoint, no OAuth dance:

```bash
$ nauka space sign myapp/uploads --key nsk_9f2c…
X-Nauka-Space: myapp/uploads
X-Nauka-Key: e61b3e9a…122f
X-Nauka-Timestamp: 1755072000
X-Nauka-Signature: 74d1…

# valid 300s — example:
curl -T file.bin 'http://<node>:8080/api/upload' \
  -H 'X-Nauka-Space: myapp/uploads' -H 'X-Nauka-Key: e61b…' \
  -H 'X-Nauka-Timestamp: 1755072000' -H 'X-Nauka-Signature: 74d1…'
```

Bind the exact bytes when you can: pass `--content-hash <blake3>` (the
`X-Nauka-Content-Hash` header) and the signature covers the content
itself. A body that hashes to anything else is rejected `403` and the
upload discarded — a captured signature can then push *nothing*.
Without the hash, the signature covers method, path, space and time
only; fine for trusted paths, weaker on hostile networks.

Writes require an `admin` key. A `signer` key gets `403` with the
remedy; a forged signature, a stale timestamp or a revoked key get
`401`; a suspended space gets `403`. Every check is answered locally by
whichever node received the request, from the replicated registry.

## Files belong to spaces: references

A signed upload does two things: stores the content (if new) and
records that **your space references this hash**. References are the
ownership model, and they compose with content addressing:

- **Deduplication stays global.** Two spaces uploading the same bytes
  share one set of shards — the second upload writes a reference and
  nothing else. Your storage bill (coming with quotas) counts *your
  references*, the disk stores each content once.
- **A file dies with its last reference.** `DELETE /f/<hash>` signed by
  a space releases *that space's* reference; the content disappears
  from the cluster only when no space references it any more — then the
  registry entry drops and the GC reclaims the shards.
- **Deletion is scoped by ownership.** A file referenced by spaces can
  only be released by a signed DELETE from one of them
  (`nauka space sign --method DELETE --path /f/<hash>`); an unsigned
  DELETE on it gets `403` naming the owners. Pre-tenant legacy files
  (no references) keep the open behavior until the private-by-default
  flip.
- **A space with references cannot be `space rm`'d** — emptying a
  space is a deliberate act, like deleting an organisation.

`nauka space files <org>/<name>` lists what a space references;
`GET /api/files` now carries each file's `spaces`.

## The transition, honestly

Uploads **without** `X-Nauka-Space` are still accepted, and reads are
still open (`GET /f/<hash>` serves anyone who knows the hash). This is
deliberate: the engine is mid-transition to multi-tenant, and the
switch to private-by-default will be its own explicit, documented flip
— not a surprise buried in a minor release. Coming next, in order:
signed read links, public spaces with revocable direct links, per-link
rate limits, and per-space quotas.
