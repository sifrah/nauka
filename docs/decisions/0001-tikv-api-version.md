# ADR 0001 — TiKV API V1 vs V2 (keyspaces) for the embedded SurrealDB SDK

**Issue:** sifrah/nauka#188
**Epic:** sifrah/nauka#183
**Status:** Accepted
**Date:** 2026-04-12

## Context

When P2 (`EmbeddedDb<TiKv>`) lands, the SurrealDB SDK linked into Nauka will
talk directly to the TiKV cluster that `nauka hypervisor init` already
bootstraps. Before we wire that up, we need to pick the **TiKV cluster API
version** and document the choice — this is a cluster-init-time decision that
is non-trivial to change later.

TiKV exposes two cluster API versions:

| | API V1 | API V2 |
|---|---|---|
| Introduced | TiKV 1.0 (legacy default) | TiKV 6.1 (2022), GA since ~6.5 |
| Key space | One flat key space per cluster | Multiple **keyspaces** within one cluster |
| Multi-tenancy at TiKV layer | None | Hard isolation per keyspace |
| MVCC | Yes (transactional) | Yes (transactional, always on) |
| TTL | Optional via `storage.enable-ttl = true` | Required (always enabled) |
| Config flag | None (default) | `storage.api-version = 2` + `storage.enable-ttl = true` in pd.toml/tikv.toml |
| Migration V1 ↔ V2 | One-way: requires BR backup + wipe + BR restore (or fresh re-init) | Same |
| Recommended by upstream for new clusters | No (legacy) | Yes |

The two modes are **mutually exclusive on a single cluster**. A TiKV cluster
is bootstrapped with one API version and stays on that version until it is
wiped or migrated via Backup-and-Restore (BR).

### How SurrealDB selects the API version

`surrealdb-core` reads two environment variables at runtime — verified in
[`surrealdb/core/src/kvs/tikv/cnf.rs`](https://github.com/surrealdb/surrealdb/blob/v3.0.5/surrealdb/core/src/kvs/tikv/cnf.rs)
in v3.0.5:

```rust
pub(super) static TIKV_API_VERSION: LazyLock<u8> =
    lazy_env_parse!("SURREAL_TIKV_API_VERSION", u8, 1);

pub(super) static TIKV_KEYSPACE: LazyLock<Option<String>> =
    lazy_env_parse!("SURREAL_TIKV_KEYSPACE", Option<String>);
```

And applies them in
[`surrealdb/core/src/kvs/tikv/mod.rs`](https://github.com/surrealdb/surrealdb/blob/v3.0.5/surrealdb/core/src/kvs/tikv/mod.rs#L62):

```rust
let config = match *cnf::TIKV_API_VERSION {
    2 => match *cnf::TIKV_KEYSPACE {
        Some(ref keyspace) => Config::default().with_keyspace(keyspace),
        None                 => Config::default().with_default_keyspace(),
    },
    1 => Config::default(),
    _ => return Err(Error::Datastore("Invalid TiKV API version".into())),
};
```

So:

- `SURREAL_TIKV_API_VERSION` defaults to **`1`**
- `SURREAL_TIKV_KEYSPACE` defaults to **`None`**
- Setting `SURREAL_TIKV_API_VERSION=2` without a keyspace uses TiKV's
  "default keyspace"
- Setting `SURREAL_TIKV_API_VERSION=2` with a keyspace name isolates that
  data from any other keyspace on the same cluster

Both the SDK side (env vars) and the cluster side (`storage.api-version` in
pd.toml/tikv.toml) must agree, otherwise the SDK fails to attach.

### Current Nauka state (verified empirically)

Today's `nauka hypervisor init` does **not** set `storage.api-version` in the
generated `pd.toml` or `tikv.toml`. Verified on `node-1` of the fresh cluster
provisioned for the P0.3 spike (sifrah/nauka#187):

```bash
$ grep -i "api[_-]\?version\|keyspace" /etc/nauka/pd.toml /etc/nauka/tikv.toml
(no api-version config)

$ curl -s "http://[fd54:...]:2379/pd/api/v1/config/cluster-version"
"8.5.5"
```

The cluster runs **PD/TiKV v8.5.5** with **no explicit API version
configuration**, which means it uses the TiKV default — **API V1**.

`grep -rn 'api-version' layers/ bin/` in the Nauka source returns nothing.
There is no code path that opts into V2.

## Decision

**Nauka will use TiKV API V1.** The defaults in `nauka hypervisor init` and
the env vars in the nauka systemd unit are explicitly the V1 defaults
(`SURREAL_TIKV_API_VERSION=1`, no keyspace).

This matches what the cluster already does. No change to `nauka init` is
required for the API-version dimension.

## Rationale

1. **Zero diff to current Nauka behaviour.** `nauka hypervisor init` already
   produces a V1 cluster. Choosing V1 means we ship the rest of P2 without
   touching the TiKV bootstrap code, the systemd unit, or the cluster lifecycle
   docs. P2.7 (sifrah/nauka#211 — "Implement schema deployment to the
   cluster") becomes a strict no-op for the API-version concern.

2. **Multi-tenancy is already handled at higher layers.** The case for V2 is
   keyspace-level hard isolation. Nauka's tenancy story (orgs, users,
   resource ownership) lives in the **application** layer (`layers/org`) and
   the **SurrealDB namespace** layer. Adding a third isolation tier at TiKV
   keyspaces is redundant and would require keeping all three in sync. The
   future-multi-tenancy ADR — sifrah/nauka#190 (P0.6 — "SurrealDB namespace
   strategy") — picks the right layer for tenant isolation, and TiKV is not it.

3. **V2 does not solve the v2-data migration problem from P0.3.** P0.3
   (sifrah/nauka#187) found that the v3 SurrealDB SDK refuses to attach to a
   TiKV cluster that already has v2.x SurrealDB metadata in it. V2 keyspaces
   would in theory let us write v3 data to a fresh keyspace and leave the
   v2 data orphaned, but:
   - The keyspace switch requires re-bootstrapping the cluster anyway
     (V1 ↔ V2 migration is destructive)
   - If we're re-bootstrapping, we may as well wipe the v2 data
   - P0.5 (sifrah/nauka#189 — "Migration strategy: RawClient → TransactionClient")
     handles this concern at the right level

4. **V1 is the upstream SurrealDB default.** The SDK ships with
   `SURREAL_TIKV_API_VERSION=1` as the default. Picking V1 means we run on
   the most-tested code path in `surrealdb-tikv-client v0.3.0-surreal.4` —
   the one most users hit, the one most likely to be exercised by SurrealDB's
   own CI.

5. **Switching V1 → V2 later is supported.** TiKV documents BR (Backup and
   Restore) for V1 → V2 migration. If multi-cluster sharing of TiKV ever
   becomes a real requirement for Nauka, we can re-evaluate at that point and
   migrate. The cost is bounded and well-understood — it does not become
   harder to do later just because we waited.

## Trade-offs accepted

1. **No TiKV-level tenant isolation.** If we ever want a single physical TiKV
   cluster to host multiple independent Nauka deployments (think:
   "dev / staging / prod sharing one TiKV"), we cannot use keyspaces to keep
   them apart. We would have to either run separate TiKV clusters or migrate
   to V2.

2. **No keyspace-based migration trick.** When upgrading from one major
   SurrealDB version to the next, we cannot use a fresh keyspace as a
   sandboxed "v_new" landing zone next to the old data. We will always have
   to wipe or BR-migrate. We accept this cost because (a) the migration is
   rare (major SurrealDB versions are years apart), (b) the wipe path is
   acceptable for the kind of state Nauka stores (control-plane metadata,
   not customer data), and (c) BR is supported as a fallback.

3. **No alignment with TiKV upstream's "recommended for new deployments"
   stance.** TiKV documentation prefers V2 for new deployments. We are
   explicitly going against that recommendation because the benefits don't
   apply to Nauka's single-tenant control-plane use case.

## Implications

### For `nauka hypervisor init`

No change. Continue to bootstrap PD/TiKV without `storage.api-version`. The
default (V1) is correct.

### For the nauka systemd unit (after P2 lands)

When the SurrealDB SDK becomes part of the running `nauka` binary in P2.x,
the systemd unit must NOT set the V2 env vars. The defaults are correct, so
no `Environment=` lines for `SURREAL_TIKV_API_VERSION` or
`SURREAL_TIKV_KEYSPACE` are needed.

For clarity, P2.x may still pin them explicitly:

```ini
Environment=SURREAL_TIKV_API_VERSION=1
```

This is optional but documents the intent at runtime so a future ops engineer
who runs `systemctl cat nauka` sees the API version commitment without having
to read this ADR.

### For tests and spikes

The P0.1 (sifrah/nauka#185) and P0.3 (sifrah/nauka#187) spike binaries
already work against the V1 defaults. No change needed. P0.4 ships nothing
runtime — it's pure documentation.

### For `nauka doctor` (added in P2.17, sifrah/nauka#221)

`nauka doctor` should report the active TiKV API version, so that any
mismatch between the cluster (V1 only at init) and a misconfigured env var
(`SURREAL_TIKV_API_VERSION=2`) surfaces with a clear error rather than a
generic connection failure. Adding this check is part of P2.17, not this ADR.

## Migration path TO V2 (if we ever need to switch)

This section exists so a future engineer can flip the choice without
re-doing the research.

### Pre-flight

1. Confirm the multi-tenancy or keyspace-isolation use case that requires V2.
   Document it in a follow-up ADR before changing anything.
2. Check that the TiKV version in use still supports V2 (it does in v6.5+,
   and we're on v8.5.5).
3. Confirm that all SurrealDB clients (Nauka itself + any external consumer)
   can be upgraded to point at the new keyspace simultaneously.

### The migration itself

There are two viable strategies:

#### Strategy A — Fresh re-init (lossy, fast)

For dev / test / lab clusters with no production data:

```bash
# 1. Stop nauka and the cluster
nauka hypervisor leave   # on each node, or systemctl stop nauka-tikv nauka-pd

# 2. Wipe the data dirs
rm -rf /var/lib/nauka/pd /var/lib/nauka/tikv

# 3. Edit the bootstrap defaults so nauka init writes V2
#    (this is a one-line code change to layers/hypervisor/src/controlplane/service.rs)
#    that adds:
#       [storage]
#       api-version = 2
#       enable-ttl = true
#    to both pd.toml and tikv.toml.

# 4. Re-init
nauka hypervisor init --region eu --zone fsn1 --peering ...

# 5. Set the env var on the systemd unit (or in the binary's launch wrapper)
SURREAL_TIKV_API_VERSION=2
SURREAL_TIKV_KEYSPACE=nauka       # optional, defaults to "default"
```

**Cost**: full data loss. Acceptable for dev clusters, fatal for any cluster
with non-trivial state.

#### Strategy B — BR (Backup & Restore) migration (preserves data)

For clusters with state worth keeping:

```bash
# 1. Take a full backup with TiKV BR (br tool from tiup)
br backup full --pd "[mesh-ipv6]:2379" --storage "s3://..." --send-credentials-to-tikv=false

# 2. Stop nauka, wipe, re-init with V2 (steps 1-4 from Strategy A)

# 3. Restore from backup into the new V2 cluster
br restore full --pd "[mesh-ipv6]:2379" --storage "s3://..." --send-credentials-to-tikv=false

# 4. Start nauka with the V2 env vars
```

**Cost**: time to back up + restore (proportional to data size), brief
downtime, requires verification step before declaring success. The BR tool
is part of the TiUP install Nauka already uses, so no extra software is
needed.

### Validation after switch

```bash
curl -s "http://[mesh-ipv6]:2379/pd/api/v1/config" | jq '.storage."api-version"'
# expect: 2

# Run the P0.3-style spike with V2 env vars set:
SURREAL_TIKV_API_VERSION=2 SURREAL_TIKV_KEYSPACE=nauka \
    /root/p0-3-spike "[mesh-ipv6]:2379"
# expect: complete CRUD round-trip, "== p0-3 spike OK =="
```

## When to revisit this decision

Re-open this ADR (or write a successor) if any of the following happens:

- Multi-tenant TiKV becomes a requirement (e.g., one TiKV cluster shared by
  Nauka + another product)
- A new SurrealDB major version (v4.x, etc.) recommends or requires V2
- TiKV deprecates V1 (no signal of that today)
- A cluster wipe is needed for an unrelated reason — at that point switching
  to V2 is essentially free, so we should reconsider opportunistically

## References

- TiKV API V2 announcement: https://tikv.org/blog/tikv-api-v2/
- TiKV BR (backup & restore) docs: https://tikv.org/docs/latest/concepts/explore-tikv-features/backup-restore/
- SurrealDB env vars: `surrealdb/core/src/kvs/tikv/cnf.rs` in
  [surrealdb/surrealdb v3.0.5](https://github.com/surrealdb/surrealdb/blob/v3.0.5/surrealdb/core/src/kvs/tikv/cnf.rs)
- P0.3 spike (cluster compat verified): sifrah/nauka#187
- P0.5 (data migration): sifrah/nauka#189
- P0.6 (namespace strategy): sifrah/nauka#190
- P2.17 (`nauka doctor`): sifrah/nauka#221
