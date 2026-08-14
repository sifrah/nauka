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
nauka-write-v1\n{method}\n{path}\n{space}\n{timestamp}\n{content_hash or "-"}
```

(The `nauka-write-v1` prefix is domain separation: a write signature
can never double as a read link, nor the reverse — the two canonical
strings disagree on their first bytes by construction.)

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

## Signed read links: owned files are private

A file referenced by spaces is **no longer served bare**. Reading it
takes a signed link:

```text
/f/<hash>?space=<org/space>&exp=<unix>&sig=<hex>
```

The signature is Ed25519 over:

```text
nauka-link-v1\n{hash}\n{space}\n{exp}\n{rate or "-"}[\n{conc}]
```

`rate` is an optional per-connection speed ceiling in **bytes/s**,
carried as `&rate=` in the URL and **inside the signed string**: the
recipient of a throttled link cannot remove or raise it by editing the
URL. This is the freemium primitive — your backend signs `rate=1048576`
for free users and omits it for premium, and the serving node paces the
stream (backpressure keeps the internal stripe fetches at the same
speed; no buffering at full speed behind the scenes).

`conc` closes the loophole `rate` leaves open: download accelerators
(aria2, IDM) split a file into N parallel range requests, each politely
under its own `rate`. Sign `conc=2` and the node serves at most two
simultaneous connections **for that link** — the real ceiling becomes
`rate x conc` whatever the client does. It rides as `&conc=` in the URL
and, when present, as a sixth line of the signed string (links signed
before `conc` existed stay valid — and nobody can strip or edit the
parameter without killing the signature). Past the cap the node answers
`429` with `Retry-After`; a slot frees the instant its connection ends,
clean or not. The count is **per node**: a client fanning out across
the DNS answer can reach `conc x nodes-in-answer` — accounted, and
still a hard wall against single-target accelerators.

Your backend mints links **offline** — check your own database (does
*this user* get *this file*, for how long?), then sign; no call to
Nauka, microseconds, and the link is the capability. `nauka space link
<space> <hash> --key nsk_… --ttl 900` does it from a shell. Both key
roles may sign links — that is exactly what `signer` keys are for.

Any node verifies locally, four questions against the replicated
registry: does the space reference this file? space and org active?
not expired? signature (including rate and conc) valid under one of
the space's keys? A link
dies at its `exp`, with its key (`space key rm`), or with its space
(`space suspend`) — the last two cluster-wide in one round-trip.

Who can read what, today:

| The file is… | `GET /f/<hash>` bare | with a valid signed link |
|---|---|---|
| referenced by an active **public-read** space | ✅ served (direct link) | ✅ |
| referenced by private spaces only | ❌ 403 | ✅ |
| suspended (space or org) | ❌ 403 | ❌ 403 |
| unowned (no references) | ❌ 403 — adopt it | — |

One deliberate exception: **a node's own loopback reads everything** —
whoever holds a shell on a node holds its disk anyway, and operator
tooling (`nauka verify`) exercises the real read path. Writes stay
strict everywhere, loopback included.

## Direct links: publish without re-uploading

"Making a file public" is adding a **reference from a public-read
space** — the bytes never move:

```bash
nauka space publish myapp/uploads <hash> --to myapp/cdn --key nsk_…
```

Behind it: `POST /f/<hash>/refs?to=<space>`, signed by an admin key of
a space that **already references** the file (the signature covers the
full path including `?to=`, so a captured request cannot be aimed at
another target). References never cross organisations — no space can
annex another tenant's content. The bare URL `/f/<hash>` then serves
worldwide; **revoking** it is a signed DELETE of the public space's
reference (the private reference survives, the file goes dark for the
world), or suspending the public space, or removing it — each
cluster-wide in one round-trip.

The same endpoint handles **adoption**: an unowned pre-tenant file can
be claimed by the signing space itself (`space publish <space> <hash>
--key …`, no `--to`) — the migration path that turns legacy files into
owned ones before the final flip.

## Quotas: storage refused, egress throttled

Two caps per space, one per organisation — each enforced locally by
whichever node takes the request, from the replicated registry:

- **Storage** (`nauka space set <space> --quota <bytes>`, and
  `nauka org set <org> --quota` for the sum of its spaces): an upload
  or a publish that would push the space past its cap is **refused**
  with the numbers and the remedy. Quotas count *logical* bytes — the
  sum of the sizes a space references. Deduplication stays physical:
  two spaces referencing the same file store it once but each counts
  it in full, so sharing never becomes a quota loophole.
- **Egress per month** (`--egress-quota <bytes>`): every served read
  is attributed to the space whose grant allowed it (the link's space,
  or the public space that served bare), accumulated per node and
  folded into the replicated ledger. Past the cap, reads are **slowed
  to a crawl, never cut** — a throttled link hurts less than a dead
  one on someone's page. The response says why:
  `X-Nauka-Throttled: egress-quota`. The month rolls over in UTC.

`nauka space usage <space>` and `nauka org usage <org>` show
consumption against the caps. This ledger is also the shape of the
future bill.

## The 0.6 flip: no more anonymous anything

Since 0.6.0, **every upload belongs to a space** — a request without
valid `X-Nauka-*` signature headers answers `401` with the four
commands that fix it. Files left over from the anonymous era are served
to nobody (loopback aside) until a space **adopts** them:

```bash
nauka space publish <org>/<space> <hash> --key nsk_…
```

Adoption is instant, moves no bytes, and the file follows its space's
rules from then on. Unsigned deletion is operator-only (loopback, and
only for unowned files); everything else about deletion follows
ownership, as above.
