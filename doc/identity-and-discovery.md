# Cryptographic identity and discovery

## The cluster key

```
nauka keygen --out ./nauka-keys
  → nauka-keys/cluster-ca.key   (Ed25519 CA, mode 0600 — THE secret)
  → nauka-keys/cluster-ca.pem   (root certificate)
```

**Owning this directory means belonging to the cluster.** It is the only
thing you ever distribute to machines (scp). Everything else is derived:

| Derived item | How |
|---|---|
| Node identity | auto-generated Ed25519 keypair (`data-dir/node.key`, 0600), certificate signed by the CA at startup |
| **Raft node-id** | `u64` = first 8 bytes of `blake3(node pubkey)` — identity is proven, not declared (`--node-id` is ignored with a warning if it disagrees) |
| Fingerprint | full hex `blake3(pubkey)` (shown by `node-info`) |
| Cluster DHT identity | pkarr keypair = `blake3("nauka-discovery-v1" ‖ CA key)` — deterministic: everyone holding the keys publishes to and resolves from the same place |

## mTLS

On **both QUIC planes** (data and consensus):

- the server requires a client certificate **signed by the cluster key**;
- the client verifies the server against the CA (SNI `node.nauka`).

A client with no certificate, or one bearing a certificate from *another*
cluster, dies during the handshake (tested). CLI commands (`put-remote`,
`cluster-metrics`, …) authenticate with an ephemeral identity signed by the
same CA (global `--keys`).

**Accepted v1 limitation**: the cluster key is distributed to every node —
any holder can issue certificates. The blast radius is that of a shared
secret, but the link is genuinely authenticated and encrypted. The natural
next step: offline per-node certificate issuance (the CA never leaves the
admin workstation).

## Discovery over the Mainline DHT (pkarr)

No infrastructure: the cluster publishes a **signed DNS record** (TXT
records) under its pkarr key, straight into the BitTorrent Mainline DHT
(~10M nodes, 20 years old).

- `_seeds`: up to 8 member addresses (any reachable seed is enough — the
  full membership comes from the cluster itself afterwards). Republished by
  the **leader every 2 min** (a heartbeat — DHT records evaporate on their
  own).
- `_genesis`: a founding candidacy (see below). Overwritten by the seed
  publication (same pkarr record).

What is public: the seeds' IP addresses (stored on third-party BitTorrent
nodes) — unfindable without the cluster's public key, and unusable without
a certificate. Data never touches the DHT.

**Discovery ≠ admission**: the DHT locates, mTLS authorizes.

## A node's life cycle (`serve` with `--keys`, everything implicit)

```
already a member (durable Raft state) ────────────────────▶ serve
otherwise, loop (5 s):
  _seeds non-empty?  → join: AddLearner, then promotion to voter
                       through the leader (redirects followed) → serve
  _seeds empty:
    _genesis:
      candidate with id < mine   → I stand down (but if it never
                                    founds anything: declared dead after
                                    45 s, and I take over)
      candidate with id > mine   → I (re)publish my own candidacy
      my candidacy, unchallenged
      for ≥ 12 s                 → I FOUND the cluster (single member)
      no candidacy at all        → I publish mine
```

The lowest node-id wins genesis — deterministic, with no designated node
and no flag. Validated on the real Mainline with two nodes started
simultaneously against a fresh key: exactly one cluster emerged.

Residual split-brain window: two simultaneous candidacies, neither of which
propagates over the DHT within 12 s — unlikely, and it concerns only the
very first minute of a cluster's life, never an established one.

## Public IP auto-detection

Without `--advertise`, the node asks the DHT itself for its address:
Mainline nodes report the address they see us from (BEP42) and the client
takes a consensus over them. No third-party service (no ipify). It falls
back to the listen address, with an explicit message, if the DHT has not
converged. The detected address is only reachable if the UDP ports are
open — the startup log says so.

## Network modes, in short

| Command | Behavior |
|---|---|
| `serve --keys k` | **the nominal mode**: mTLS + derived node-id + DHT discovery + genesis + auto IP |
| `serve --keys k --no-discover` | same without the DHT (static/air-gapped cluster, manual init) |
| `serve --keys k --peers a,b` | static mTLS mode (passing `--peers` disables the DHT) |
| `serve` (no keys) | legacy insecure mode (development only, warns) |
