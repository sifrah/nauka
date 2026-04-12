# ADR 0002 — Migration strategy: `ClusterDb` (RawClient) → SurrealDB (TransactionClient)

**Issue:** sifrah/nauka#189
**Epic:** sifrah/nauka#183
**Status:** Accepted
**Date:** 2026-04-12

## Context

When P2 (`EmbeddedDb<TiKv>`, sifrah/nauka#183) lands, the persistence layer
that today writes to TiKV via `nauka_state::ClusterDb` (`tikv-client::RawClient`,
RawKV mode) is replaced by the SurrealDB SDK in `kv-tikv` mode
(`surrealdb-tikv-client::TransactionClient`, TxnKV mode).

The two modes share the same physical TiKV cluster but **do not share their
on-disk encoding**:

- **RawKV (RawClient)**: keys are written as bytes directly under a single
  raw column family. No MVCC layer, no SurrealDB metadata, no schema.
- **TxnKV (TransactionClient + SurrealDB)**: keys go through MVCC, SurrealDB
  prepends its own bookkeeping (namespace IDs, table IDs, version stamps),
  and table definitions live in SurrealDB metadata that the v3 SDK insists
  must already be in v3 format on attach.

P0.3 (sifrah/nauka#187) demonstrated this empirically: a v3 SurrealDB SDK
attached to a TiKV cluster that already had v2.x SurrealDB metadata in it
errored out with `data stored on disk is out-of-date (Expected: 3, Actual:
2)`. The same kind of failure would happen the moment a v3 SurrealDB SDK
tried to operate on a TiKV cluster that already had `ClusterDb`-shaped raw
keys at the well-known prefixes.

We need a strategy for moving from "current world" to "post-P2 world" that:

1. Doesn't lose state we care about
2. Doesn't require dangerous one-shot rituals during the actual P2 deploy
3. Doesn't bake in maintenance burden (a migration tool that has to be kept
   in sync with every schema change forever)
4. Is realistic for the current maturity of Nauka (not yet in production)

## Inventory of what would be migrated

Verified by `grep -rn 'ClusterDb' layers/` and the namespace constants in
each store. The full list of TiKV-resident state today:

| Layer | Namespace prefixes | Purpose |
|---|---|---|
| `org` | `org`, `org-idx` | Org records + name→id index |
| `org/project` | `proj`, `proj-idx` | Project records + index |
| `org/project/env` | `env`, `env-idx` | Environment records + index |
| `network/vpc` | `vpc`, `vpc-idx` | VPC records + index |
| `network/vpc/peering` | `vpcpeer`, `vpcpeer-idx` | VPC peering records |
| `network/vpc/natgw` | `natgw`, `natgw-idx`, `natgw-ipv6` | NAT GW records + IPv6 alloc |
| `network/vpc/subnet` | `sub`, `sub-idx`, `ipam` | Subnet records + IP allocations |
| `compute/vm` | `vm`, `vm-idx` | VM records + index |
| (cross-cutting) | `_reg_v2` | Per-type ID registry |
| (cross-cutting) | `_reg` | Legacy registry + counters (e.g. `vni-counter`) |
| (forge) | `forge.*` | Reconciler observability state |
| (storage) | `storage.*` | ZeroFS S3 storage state |

That is **nine resource types** plus the cross-cutting registries and counters.
Every record is stored as JSON-encoded bytes under a key `{namespace}/{id}`.

This is non-trivial state. A real "export-and-replay" migration tool would
have to know every namespace, every JSON shape, and every cross-reference.
The cost of writing and maintaining that tool is the central question of
this ADR.

## Options evaluated

### Option A — Wipe (cluster reset)

```text
1. Stop nauka and the cluster
2. Delete /var/lib/nauka/{pd,tikv}
3. nauka hypervisor init  →  fresh PD/TiKV cluster
4. P2.x code paths come up against an empty cluster
5. Re-create whatever resources you actually need (or accept they're gone)
```

**Pros**

- Trivial. Works on every cluster regardless of what's in it.
- No new code to write, test, or maintain.
- Sidesteps the SurrealDB v2 → v3 metadata problem from P0.3 by definition —
  there's nothing left for the v3 SDK to misread.
- Free for Nauka right now: no production deployment exists; the only state
  in any nauka cluster is dev/test scratch.
- Aligns with the schema-driven Phase 3 (sifrah/nauka#223 ff): the .surql
  schemas will likely change the JSON shape of resources anyway, which a
  field-level export-replay tool would have to track.

**Cons**

- Lossy. Anything stored in the cluster at the moment of wipe is gone. No
  rollback, no "oops let me re-read the old data".
- Unacceptable for any cluster with state worth keeping (production, shared
  staging, lab clusters with hand-curated data).
- Breaks any external consumer that relied on the cluster being long-lived.

### Option B — Export + reimport script

```text
1. Build a one-shot binary (e.g. nauka-migrate-tikv-to-surrealdb)
2. The binary opens BOTH:
     a) tikv-client::RawClient against the existing cluster
     b) Surreal<TiKv> via the SurrealDB SDK against the same cluster
        (or a sibling cluster — see Option C for that variant)
3. For every namespace listed above:
     - scan all (key, value) pairs via RawClient
     - serde_json::from_slice into the typed Rust struct
     - re-create the equivalent record via SurrealQL CREATE on the
       SurrealDB side, applying any field-level transforms required by
       the new schema
4. Stop nauka, run the migration binary, verify counts match, restart
   nauka against the new SurrealDB-shaped data
```

**Pros**

- Preserves state. Existing orgs, projects, envs, VPCs, VMs, etc. all
  survive the cut-over.
- Idempotent if written carefully (re-running the migration should be a
  no-op once done).
- Provides an audit trail (counts before / counts after / validation).

**Cons**

- Real work: ~500–1000 lines of Rust, plus tests, plus integration tests
  against a real TiKV cluster. Comparable in scope to writing a small
  database driver.
- Has to know every namespace and every Rust struct shape — i.e. it has
  to be re-touched **every time a schema changes**, which Phase 3
  (sifrah/nauka#223 ff) is going to do extensively.
- Bridges two incompatible client libraries (`tikv-client 0.4` and
  `surrealdb-tikv-client 0.3`) in the same binary, which P0.1 already
  showed is technically possible but inflates the binary size.
- Cross-references between resources (org → project → env → vpc → subnet
  → vm) have to be migrated in topological order to avoid dangling
  references.
- The binary is dead code the day after the migration runs. We delete it
  or it rots.

### Option C — Coexistence on a parallel cluster

```text
1. Provision a second PD/TiKV cluster alongside the existing one
2. Bring up nauka with the SurrealDB SDK pointing at the new cluster
3. At runtime, the application reads from the old cluster (via ClusterDb)
   and writes to the new one (via SurrealDB) until everything has been
   touched
4. Decommission the old cluster
```

**Pros**

- Zero downtime cut-over (in theory).
- Each cluster's correctness is independent — bugs in the new SDK code
  don't corrupt the old data.

**Cons**

- Doubles the operational footprint: two PD/TiKV clusters running on every
  node, twice the disk, twice the systemd units, twice the failure modes.
- Nauka's mesh + bootstrap layer assumes one cluster per mesh. Supporting
  two would require non-trivial changes to `nauka hypervisor init`,
  `controlplane::connect()`, `controlplane::service::install`, and the
  systemd unit templates — a parallel control plane.
- Requires Nauka to keep `ClusterDb` and `EmbeddedDb<TiKv>` working in the
  same binary indefinitely (or at least for the migration window). Both
  TiKV clients would coexist for the entire migration, doubling the binary
  size and the dependency graph maintenance burden.
- Shifts the migration problem from "one offline step" to "an online dual
  write that has to run for days" — a much higher class of operational risk.
- The application-level "read-from-old, write-to-new" pattern requires
  invasive changes to every store in `layers/org`, `layers/network`,
  `layers/compute` — exactly the layers we're trying to *simplify* in
  Phase 3.

## Decision

**Nauka uses Option A (wipe), with a one-time TiUP BR safety backup
before any wipe of a cluster that may contain state someone cares about.**

In other words:

1. The default migration step on any nauka cluster going from
   pre-P2 to post-P2 is: stop, wipe `/var/lib/nauka/{pd,tikv}`, re-`init`.
2. Before the wipe, `tiup br` takes a full backup of the existing
   PD/TiKV cluster to S3 (the same Hetzner buckets Nauka already uses
   for region storage). This is a **safety net**, not a migration plan
   — the backup exists so a human can recover individual records by
   hand if a wipe is regretted, not so that nauka has automated
   round-trip restore.
3. If, in some future where Nauka has real production state and a
   wipe is no longer acceptable, we **graduate to Option B (export +
   reimport)**. The triggers and the work required are documented
   below.

Option C is rejected. The operational cost of parallel clusters is
disproportionate to the migration window we actually need.

## Rationale

1. **Zero production state today.** Nauka v2.0.0 is not yet running
   anything anyone depends on. Every existing cluster is a dev/test
   playground that the team explicitly stood up for spike work
   (P0.1, P0.3, future P2.x). Wiping is free.

2. **The schema is going to move anyway.** Phase 3 (sifrah/nauka#223 ff)
   moves the source of truth for resource types from Rust `ResourceDef`
   structs to `.surql` files. Field names, default values, validation
   rules, and indexes will all be re-considered as part of that move.
   A field-level export-replay tool built today would need to be updated
   *during* Phase 3, every time a schema lands. The cost of writing the
   tool is paid up front; the cost of *maintaining* it is paid forever.

3. **The wipe path is the only path that's truly version-agnostic.**
   The export-replay tool would have to know about both the old `ClusterDb`
   schema *and* the new SurrealDB-managed schema. Wipe doesn't care.
   When SurrealDB v4 ships in a few years, wipe still works; the tool
   would need a v3-to-v4 update.

4. **We aren't in a hurry that requires zero downtime.** Per the
   per-ticket workflow on the SurrealDB migration epic, every issue
   ships behind a Hetzner test on a fresh cluster. The migration of
   any specific cluster from pre-P2 to post-P2 is a planned offline
   event, not a hot upgrade.

5. **The BR safety net is enough for the few clusters that matter.**
   For clusters that have state worth preserving — by judgement call,
   not by policy — the operator runs `tiup br backup` to S3 before the
   wipe. If a record needs to be recovered after the fact, a human
   restores the backup to a sandbox cluster, queries it via
   `tiup ctl tikv` or `tiup ctl pd`, extracts the value as JSON, and
   re-creates the record in the new SurrealDB-shaped cluster by hand
   via `nauka <resource> create`. This is slow and manual but it
   correctly handles the rare case without paying the maintenance cost
   of the export-replay tool.

## Operational steps — the wipe procedure

This is the procedure to run on every node of a cluster being migrated
from pre-P2 to post-P2.

### Pre-flight (do once for the whole cluster)

1. **Decide whether the data is worth a BR backup.** Default is "no, the
   cluster is dev/test" → skip to step 3. If "yes, somebody is going to
   ask for an org back later":

   ```bash
   # Run from any node that has tiup installed (all nauka nodes do)
   tiup br backup full \
     --pd "[$MESH_IPV6]:2379" \
     --storage "s3://syfrah-storage-eu-central-fsn1/nauka-pre-p2-backup-$(date +%Y%m%d-%H%M%S)" \
     --s3.endpoint "https://fsn1.your-objectstorage.com" \
     --send-credentials-to-tikv=false
   ```

   The S3 credentials in `~/.aws/credentials` profile `[hetzner-s3]`
   already work; copy them into the binary's environment if you don't
   want `--send-credentials-to-tikv=false` to require local creds.
   Verify the backup exists in S3 before proceeding.

2. **Snapshot the resource counts** so you can sanity-check the
   post-migration state if you do choose to manually restore something:

   ```bash
   ssh root@$NODE 'nauka org list --format json | jq length'
   ssh root@$NODE 'nauka project list --format json | jq length'
   # ... etc
   ```

   These counts go into the migration runbook entry for this cluster.

3. **Stop nauka cleanly on every node, in this order**:

   ```bash
   systemctl stop nauka-forge
   systemctl stop nauka-announce
   systemctl stop nauka-tikv
   systemctl stop nauka-pd
   ```

   `nauka-wg` may stay up — the mesh itself doesn't need to be torn down
   for a TiKV reset.

### Per-node wipe

On every node of the cluster:

```bash
rm -rf /var/lib/nauka/pd /var/lib/nauka/tikv
```

If you also want to drop SurrealDB local state (covered by the
`SurrealKv` side of P1, sifrah/nauka#191), also:

```bash
rm -rf /var/lib/nauka/bootstrap.skv
```

(This file does not exist on pre-P1 clusters; it's a future addition.)

### Re-init

On the bootstrap node (typically `node-1`):

```bash
nauka hypervisor init \
  --region eu --zone fsn1 --peering \
  --s3-endpoint https://fsn1.your-objectstorage.com \
  --s3-bucket   syfrah-storage-eu-central-fsn1 \
  --s3-access-key "$AWS_ACCESS_KEY_ID" \
  --s3-secret-key "$AWS_SECRET_ACCESS_KEY"
```

On the remaining nodes (in any order, but one at a time so PD quorum
elects cleanly):

```bash
nauka hypervisor join \
  --target $NODE_1_PUBLIC_IPV4 \
  --pin    $JOIN_PIN_FROM_INIT \
  --region eu --zone fsn1
```

### Post-init validation

```bash
# PD healthy?
curl -sf "http://[$MESH_IPV6]:2379/pd/api/v1/health"

# Stores up?
curl -sf "http://[$MESH_IPV6]:2379/pd/api/v1/stores" | jq '.count'

# Application empty?
nauka org list   # expect: empty
nauka vm list    # expect: empty

# (Once P2.x has landed:)
SURREAL_TIKV_API_VERSION=1 \
  /root/p0-3-spike "[$MESH_IPV6]:2379"
# expect: == p0-3 spike OK ==
```

If any of these fail, **do not** proceed to load real data into the new
cluster. Investigate, optionally restore the BR backup to a sandbox
cluster, and either fix the bootstrap or rerun the wipe.

## Triggers for revisiting (when to graduate to Option B)

Re-open this ADR (or write a successor) and build the export+reimport
tool **before** any of the following becomes true:

1. **A nauka cluster runs production traffic.** Defined as: a stranger
   to the team has put a record into the cluster and would notice or
   complain if it disappeared.

2. **A nauka cluster has been in continuous use for more than ~30 days
   with hand-curated state.** A lab cluster that has accumulated weeks
   of carefully-set-up orgs/VPCs/VMs is worth preserving even if it
   isn't strictly "production".

3. **A user (anyone outside the team) has installed Nauka and run
   `nauka hypervisor init`** on their own infrastructure. At that
   point we can no longer rely on coordinating wipes — we have to
   ship a migration that works against installations we can't see.

4. **Any of P2.5 / P2.6 schema decisions land** (sifrah/nauka#209 / #210)
   if the schemas turn out to be field-compatible enough that an
   export-replay tool would only need a thin transform layer. If the
   shapes match closely, the tool's maintenance cost drops and Option B
   becomes more attractive.

When the trigger fires, the export-replay tool to build is roughly:

```rust
// nauka-migrate-tikv-to-surrealdb (one-shot binary, deleted after use)
//
// Args: --from-pd <pd_endpoint> --to-pd <pd_endpoint> [--dry-run]
//
// 1. Open both clients
//      let raw  = RawClient::new(vec![from_pd]).await?;
//      let surr = Surreal::new::<TiKv>(to_pd.as_str()).await?;
//      surr.use_ns("nauka").use_db("cluster").await?;
//
// 2. For each namespace, in topological order
//      (org → project → env → vpc → subnet → vm → ipam → vpcpeer → natgw)
//      let pairs = raw.scan(format!("{ns}/").into()..end_key).await?;
//      for (key, value) in pairs {
//          let typed: <ResourceType> = serde_json::from_slice(&value)?;
//          if !dry_run {
//              surr.create((<table>, &typed.id)).content(typed).await?;
//          }
//      }
//
// 3. Print before/after counts. Exit non-zero on any mismatch.
```

Building this is real work (estimated 1–2 weeks for one engineer
including tests + Hetzner validation) but the design is straightforward.
The decision in this ADR is "don't build this **yet**", not "this is
impossible".

## What does NOT change

- `LocalDb` (the JSON file at `~/.nauka/{layer}.json`) is **out of scope**
  for this ADR. P1.x replaces `LocalDb` with `EmbeddedDb<SurrealKv>` at
  the bootstrap layer, which is also a wipe operation per node (the
  bootstrap state is a few KB and can be re-derived by re-running
  `nauka hypervisor init` / `join`).

- `ClusterDb` itself is not deleted by this ADR. P2.16 (sifrah/nauka#220
  — "Remove `layers/state/src/cluster.rs`") is the issue that physically
  removes the type. This ADR commits us to **not running any data
  through it after the wipe**, but the type continues to compile until
  P2.16 lands.

- The choice of TiKV API version is owned by ADR 0001
  (sifrah/nauka#188). This ADR assumes API V1, consistent with that
  decision.

## References

- `nauka_state::ClusterDb` — `layers/state/src/cluster.rs`
- TiUP BR (Backup & Restore): https://docs.pingcap.com/tidb/stable/br-snapshot-guide/
- P0.1 spike (musl + surrealdb build chain): sifrah/nauka#185
- P0.3 spike (the v2-vs-v3 datastore-format incompatibility): sifrah/nauka#187
- P0.4 ADR (TiKV API V1 chosen): sifrah/nauka#188
- P1.x — replace LocalDb: sifrah/nauka#191 ff
- P2.16 — remove ClusterDb: sifrah/nauka#220
- P2.5 / P2.6 — cluster schema definition / deployment: sifrah/nauka#209 / #210
