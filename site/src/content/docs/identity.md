---
title: "Identity and membership"
description: "The cluster token, node identities derived from public keys, mutual TLS on both QUIC planes, and how initialized machines carry their identity."
---

## The cluster token

```
nauka token
  → nauka1_9f2K…   (one string — THE secret)
```

**The token IS the cluster.** It carries 32 random bytes (base64url), and
the Ed25519 cluster CA is derived from it deterministically
(`blake3::derive_key("nauka cluster-ca v1", secret)`): every holder of the
token computes the exact same key, so nodes need to share nothing else.
The `1` in the prefix is the derivation scheme version — bumping it would
change every derived key, so it never moves silently.

Owning the token means belonging to the cluster: it is the only thing you
ever hand to a machine or a client. Back it up above everything else —
losing it means never being able to add a node or a client again.

The file-based equivalent still exists: `nauka keygen --out ./nauka-keys`
produces `cluster-ca.key` (0600) and `cluster-ca.pem`, used via the global
`--keys`. Same entropy, same role; the token changes the ergonomics — one
string (`--token`, env `NAUKA_TOKEN`) instead of a directory to copy
around. Internally a token is materialized into those same two files, so
both paths converge.

## Node identity

Everything else is derived, never declared:

| Derived item | How |
|---|---|
| Node keypair | auto-generated Ed25519 (`data-dir/node.key`, 0600), certificate signed by the cluster CA at startup |
| **Raft node-id** | `u64` = first 8 bytes of `blake3(node pubkey)`, little-endian — identity is proven, not declared |
| Fingerprint | full hex `blake3(pubkey)` (shown by `nauka node-info`) |

Because the id lives in `node.key`, a **wiped or reinstalled machine comes
back as a new identity** — it cannot impersonate its former self, and the
old id would linger as a phantom voter. That is handled at the membership
layer: `nauka node add` on the returning machine joins the fresh identity
and **evicts the stale same-address one in the same membership change**.
`node.key` is therefore optional in backups; the token is not.

## mTLS on both QUIC planes

Data plane and consensus plane both require it:

- the server demands a client certificate **signed by the cluster CA**;
- the client verifies the server against the same CA (SAN `node.nauka` —
  what is being verified is cluster membership, not an address).

A peer with no certificate, or one bearing a certificate from *another*
cluster, dies during the handshake (tested). CLI commands that talk to the
cluster (`node add`, `node remove`, `ban`, …) authenticate with an
**ephemeral identity** signed by the same CA on the fly: a client is a
short-lived cluster member, cryptographically indistinguishable from a
node at the transport layer.

**Accepted v1 limitation**: the cluster key is on every node — any holder
can issue certificates. The blast radius is that of a shared secret, but
the link is genuinely authenticated and encrypted. The natural next step
is offline per-node issuance, where the CA never leaves the admin
workstation.

## How initialized machines carry their identity

`nauka init` (what the installer runs) writes the identity once, into
`/etc/nauka/nauka.env`:

```
NAUKA_TOKEN=nauka1_…
NAUKA_ARGS=--advertise 203.0.113.7:7311
```

The systemd unit reads it, and so do CLI commands run on that machine:
`node add`, `node remove`, `node-info`, `ban` and `unban` **inherit the
identity automatically** — no `--token` or `--keys` to repeat. From
anywhere else, pass `--token` (or set `NAUKA_TOKEN`) explicitly.

`nauka status` also requires the cluster identity when it reads a remote
node. The CLI signs a short request proof; the private identity itself is
never sent over HTTP. The node's own loopback remains available to local
health checks.

## Membership in short

| Action | Command | Identity involved |
|---|---|---|
| Found a cluster | first `serve` on a blank data dir | node.key generated, cert signed by the CA |
| Add a machine | `nauka node add ip:7311` | provisions over SSH, joins as learner, promotes to voter; evicts a stale same-address id |
| Remove a machine | `nauka node remove id` | drains — the node serves while others re-replicate, then you shut it down |
| Inspect | `nauka status` / `nauka node-info` | cluster identity / local key |

Admission is mTLS, end of story: no certificate signed by this cluster's
CA, no membership — whatever the network can see.
