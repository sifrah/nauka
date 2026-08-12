---
title: "Monitoring & metrics"
description: "nauka status for humans, /api/status for scripts, Prometheus on 9100 for dashboards — and the four signals worth alerting on."
---

Three faces, same truth: a CLI for humans, JSON for scripts, Prometheus for
dashboards.

## For humans: `nauka status`

```bash
nauka status
```

```text
cluster — 3 nodes, 3 alive · 353 files, 3.34 GiB stored · 117.79 GiB capacity
  ● 163.172.181.194:7311           35.04 GiB  355474566507203597  (this node)
  ● 2.28.25.61:7311       leader   74.77 GiB  14193759605909860198
  ● 51.158.64.90:7311               7.98 GiB  6618550476704767285
```

Plain HTTP against the local node's API — no cluster identity needed, works
from anywhere that can reach a node (`--api http://<node>:8080`). A red `●`
is a member missing liveness probes; a **⚠ shares its address with another
member** is a stale identity that should be retired (see
[Growing and shrinking](/growing/)). No leader line means the cluster
cannot commit writes right now.

## For scripts: `--json` and the API

```bash
nauka status --json          # the node's raw report, passed through
curl -s http://<node>:8080/api/status
```

Fields worth acting on: `leader` (null = unavailable for writes),
`nodes[].is_alive` (this node's probe view), `nodes[].id` (what
`node remove` takes), `files` / `total_bytes`.

## For dashboards: Prometheus on :9100

Every node exposes `/metrics` on `127.0.0.1:9100` — **loopback by
default**, because the exposition describes cluster topology, capacities
and peer addresses, which have no business on a public interface. Widen it
deliberately for a private scrape network: `--metrics <addr>`; disable it
entirely with `--no-metrics`.

The families that matter:

| Series | Meaning |
|---|---|
| `nauka_raft_role` | 4 = leader, 2 = follower; exactly one 4 per healthy cluster |
| `nauka_raft_last_applied` / `nauka_raft_commit_index` | applied vs committed log — a lasting gap is a node falling behind |
| `nauka_raft_leader_changes_total` | one per genuine election; a counter that climbs is instability |
| `nauka_raft_rpc_failures_total{peer,kind}` | unreachable / timeout / rejected, per peer — the first place WAN trouble shows |
| `nauka_writes_degraded_total` / `nauka_write_shards_undelivered_total` | how often writes land under-replicated, and how much repair debt each leaves |
| `nauka_gc_orphans_purged_total` | reclaimed shards of deleted/expired/banned files |
| `nauka_staged_bytes` | locally-acked upload bytes not yet dispersed |
| `nauka_egress_served_bytes` / `nauka_egress_quota_bytes` | the monthly ledger vs its budget, per node |

## The four alerts worth having

1. **No leader** — `max(nauka_raft_role) < 4` for more than a minute: the
   cluster refuses writes.
2. **A node persistently behind** — `commit_index − last_applied` growing:
   its registry view is stale (and its purge stands down until it catches
   up — by design).
3. **Degraded writes trending** — `rate(nauka_writes_degraded_total)` > 0
   over an hour: some peer is unreachable at write time; find it in
   `rpc_failures_total`.
4. **Disk headroom on the smallest node** — with ≤ 6 nodes every node
   carries a slice of every stripe, so the smallest disk caps the cluster.
   Watch it with node_exporter; Nauka does not yet refuse writes on a full
   disk (a [known limitation](/operations/#known-limitations-v1)).

## Logs

`journalctl -u nauka`. The lines that matter are terse and greppable:
`scrub: X checked, Y regenerated, Z unrecoverable` (a persistent Z means
too many dead nodes), `gc: N shards released`, `purge: N manifest(s), M
orphan shard(s)`, `capacity declared`, and `peer … unreachable`. Startup
prints an aligned banner — version, data dir, listen, advertise, http — so
the first screen of a journal answers "what is this node".
