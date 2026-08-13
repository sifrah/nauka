---
title: "Quickstart"
description: "A real cluster in five minutes: found the first node, add a second machine, store a file, kill a node, read the file anyway."
---

Five minutes, two machines, one file that survives a dead node. Every command
on this page was run for real before being written down.

## 1. Found the cluster (first machine)

On a Linux box with systemd, as root:

```bash
curl -sSfL https://sh.getnauka.com | sh
```

The installer places the binary, then hands over to `nauka init`: a dedicated
`nauka` user, the cluster identity in `/etc/nauka/nauka.env`, a hardened
systemd unit — started now, back after every reboot. The node founds a
single-node cluster and prints its token:

```text
cluster founded — this machine is node 355474566507203597
  service : nauka.service (enabled: restarts on failure and on reboot)
  data    : /var/lib/nauka
  config  : /etc/nauka/nauka.env
  api     : http://163.172.181.194:8080
  token   : nauka1_…
            anyone holding the token is a member — treat it like a password
```

Save the token somewhere safe. It IS the cluster: every certificate derives
from it, and losing it means never adding another machine.

Not root, no systemd, on a laptop? The installer stops at the binary and
tells you so; run `sudo nauka init` when ready, or set `NAUKA_NO_INIT=1` to
keep it that way.

## 2. Grow it (from the first machine)

Point `node add` at any machine you can SSH into as root — a blank cloud
instance is the normal case:

```bash
nauka node add 51.158.64.90:7311
```

```text
✓ connected to root@51.158.64.90
✓ target is blank
✓ binary installed (17.63 MiB)
✓ installing the cluster identity and unit
✓ node 6618550476704767285 is up
✓ joined the Raft cluster as a voting member
```

It copies the binary, installs the same systemd unit and identity, starts
the node in join-wait mode, and takes it through consensus to a voting
member. Your forwarded SSH agent key is what reaches the target — connect
with `ssh -A` if you drive this from your laptop.

Check the result from any node:

```bash
nauka status
```

```text
cluster — 2 nodes, 2 alive · 0 files, 0 B stored · 43.02 GiB capacity
  ● 163.172.181.194:7311  leader   35.04 GiB  355474566507203597  (this node)
  ● 51.158.64.90:7311               7.98 GiB  6618550476704767285
```

## 3. Create your space (once)

Files belong to [spaces](/multi-tenant/). Thirty seconds of setup, once:

```bash
nauka org create myapp
nauka space create myapp/files
nauka space key add myapp/files --role admin --name main
```

```text
key main (admin) registered on myapp/files
  public : e61b3e9a…
  private: nsk_9f2c…
  ^ shown ONCE and stored NOWHERE — put it in your secret store now.
```

The private key stays on YOUR machine — the cluster only ever holds the
public half.

## 4. Store and read a file

Sign the upload (offline — no server round-trip) and send it:

```bash
nauka space sign myapp/files --key nsk_9f2c…
# prints the X-Nauka-* headers and a ready-to-paste curl:
curl -T report.pdf 'http://163.172.181.194:8080/api/upload' \
  -H 'X-Nauka-Space: myapp/files' -H 'X-Nauka-Key: e61b…' \
  -H 'X-Nauka-Timestamp: 1755…' -H 'X-Nauka-Signature: 74d1…'
```

```json
{"hash":"adc885d25a8c5694…","size":81920,"stripes":1,"data_shards":4,
 "parity_shards":2,"link":"/f/adc885d25a8c5694…","degraded_shards":0,
 "space":"myapp/files"}
```

`degraded_shards: 0` means every shard reached its owner — the write is
fully replicated. Reading takes a **signed link** (your file is private
by default), valid on **any** node:

```bash
nauka space link myapp/files adc885d25a8c5694… --key nsk_9f2c… --ttl 3600
curl -O "http://51.158.64.90:8080/f/adc885d25a8c5694…?space=myapp/files&exp=…&sig=…"
```

Want a permanent public URL instead? Reference the file from a
public-read space — see [direct links](/multi-tenant/#direct-links-publish-without-re-uploading).
The read is verified by construction either way: content is addressed
by BLAKE3, so bytes that come back are bytes that hash back.

## 5. Kill a node, read the file anyway

Stop one node — unplug it, `systemctl stop nauka`, whatever you like:

```bash
nauka status
```

```text
cluster — 2 nodes, 1 alive · 1 file, 80 KiB stored · 43.02 GiB capacity
  ● 163.172.181.194:7311  leader   35.04 GiB  …  (this node)
  ● 51.158.64.90:7311               7.98 GiB  …
```

The download still answers — the file is reconstructed from the surviving
shards. (Measured on a real cluster: a 1 GiB file read through a dead node
in 24 seconds, byte-identical.) When the node comes back, the scrubber
refills it; nothing to do.

## Where next

- [Durability & consistency](/durability/) — exactly what survives what.
- [Deploy](/deploy/) — ports, keys-instead-of-token, sizing, the long version.
- [Growing and shrinking](/growing/) — replacing machines, retiring them.
- [Organisations & spaces](/multi-tenant/) — keys, signed links, direct
  links, rate limits, quotas: the whole multi-tenant model.
- [HTTP API](/api-http/) — upload options, ranges, TTLs, signatures.
