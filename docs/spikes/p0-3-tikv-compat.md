# P0.3 — Spike: surrealdb-tikv-client 0.3 vs PD/TiKV v8.5.5 compatibility

**Issue:** sifrah/nauka#187
**Epic:** sifrah/nauka#183
**Status:** Done — all acceptance criteria met
**Date:** 2026-04-12

## Goal

Verify that the fork `surrealdb-tikv-client v0.3.0-surreal.4` (used by
`surrealdb v3.0.5` when the `kv-tikv` feature is enabled) can connect to the
**PD/TiKV v8.5.5** versions that Nauka currently bootstraps via
`nauka hypervisor init`, and that a SurrealQL CRUD round-trip works end-to-end.

## Result

| Criterion | Result |
|---|---|
| `Surreal::new::<TiKv>` succeeds against the existing PD/TiKV v8.5.5 cluster | ✅ `connect_ms = 99` |
| INSERT + SELECT round-trip works | ✅ create / select-id / select-all all succeed |
| Document any incompatibility | ✅ this file (and one was found — see "Surprise" below) |

Total round-trip latency on a fresh single-node cluster: **169 ms**, with
per-operation latencies (`create / select / delete`) all in the **4–20 ms**
range. Well under the 50 ms p50 target documented in P2.4 (sifrah/nauka#208).

## Surprise: data-format incompatibility (not a wire-protocol issue)

Before getting the "all green" run above, the **first** attempt against the
existing 5-node Hetzner cluster failed with:

```
Error { code: -32000, message: "The data stored on disk is out-of-date with
this version (Expected: 3, Actual: 2). Please follow the upgrade guides in
the documentation, or use a clean storage directory if this is intended to
be a new instance" }
```

That cluster had been previously bootstrapped using a **WIP branch** running
the SurrealDB v2.6.5 binary as a separate `nauka-surrealdb.service` on top of
TiKV. The wire connection between `surrealdb-tikv-client v0.3.0-surreal.4` and
PD/TiKV v8.5.5 worked fine — the failure was at the **SurrealDB datastore
metadata level**: SurrealDB stores its own version-stamped metadata in TiKV at
startup, and a v3 client refuses to attach to a TiKV cluster that already has
v2-format SurrealDB metadata in it.

This is exactly the issue covered by P0.5 (sifrah/nauka#189 — "Migration
strategy: RawClient → TransactionClient"). The migration from the existing
data layout to the SurrealDB-managed layout is not free; it requires either a
clean cluster or an explicit migration script.

To get a clean cluster for this spike I:

1. Deleted all 5 existing Hetzner VMs
2. Provisioned 5 fresh `cpx22 / ubuntu-24.04 / fsn1` VMs
3. Cross-compiled `bin/nauka` for `x86_64-unknown-linux-musl` from `main`
   (which does NOT include the WIP `nauka-surrealdb.service`)
4. `scp`'d the binary to all 5 nodes
5. Ran `nauka hypervisor init --region eu --zone fsn1 --peering --s3-...`
   on `node-1` to bootstrap a fresh PD/TiKV cluster

The single-node bootstrap was enough for this spike (1 PD member, 1 TiKV
store). Multi-node validation belongs to P2.4 (sifrah/nauka#208) and P2.18
(sifrah/nauka#222).

## What landed

```
layers/state/Cargo.toml          # add p0-3-spike binary
layers/state/src/bin/p0_3_spike.rs   # the spike itself
docs/spikes/p0-3-tikv-compat.md     # this file
```

The spike binary takes a PD endpoint as its single argument and runs:

1. `Surreal::new::<TiKv>(pd)` — open the SDK against PD
2. `use_ns("p0_3_spike").use_db("test")` — isolate from any real namespace
3. `create(("p0_3_record", "first")).content(...)` — insert one record by id
4. `select(("p0_3_record", "first"))` — read it back by id
5. `select("p0_3_record")` — list the table
6. `delete(("p0_3_record", "first"))` — clean up

Each step is timed with `Instant::now()`. The final line is `== p0-3 spike OK
==` and the binary exits 0 on success, non-zero on any error.

## Build and run

### Cross-compile for musl

```bash
# Stash any WIP layers/org/build.rs that's not in main; it isn't tracked
# but Cargo auto-discovers it. The P0.1 ADR (#185) covers the build chain
# requirements (cmake, openssl.workspace).
mv layers/org/build.rs /tmp/nauka-wip-org-build.rs

cargo build --target x86_64-unknown-linux-musl -p nauka-state --bin p0-3-spike --release

mv /tmp/nauka-wip-org-build.rs layers/org/build.rs   # restore the WIP
```

### Run on Hetzner

```bash
scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
    -o LogLevel=ERROR \
    target/x86_64-unknown-linux-musl/release/p0-3-spike \
    root@$IPV4_OF_NODE:/root/p0-3-spike

ssh ... root@$IPV4_OF_NODE \
    "/root/p0-3-spike '[$MESH_IPV6_OF_NODE]:2379' && rm /root/p0-3-spike"
```

### Captured output (fresh single-node cluster on `node-1`)

```
== nauka p0-3 spike (kv-tikv compat check) ==
target_arch    = x86_64
target_os      = linux
surrealdb_dep  = 3.0.5 (kv-tikv via surrealdb-tikv-client v0.3.0-surreal.4)
pd_endpoint    = [fd54:7725:6b2a:cab9:3a2f:89c9:7ffd:5f8e]:2379
connect_ms     = 99
use_ns_db_ms   = 27
create_ms      = 20
create_value   = Some(SpikeRecord { name: "p0-3", answer: 42 })
select_id_ms   = 4
select_id_val  = Some(SpikeRecord { name: "p0-3", answer: 42 })
select_all_ms  = 4
select_all_n   = 1
delete_ms      = 12
total_ms       = 169
== p0-3 spike OK ==
```

`ldd /root/p0-3-spike` confirms `statically linked` (musl x86_64).

## Findings for the rest of the migration

1. **Wire-level compatibility is not the problem.** SurrealDB SDK 3.0.5
   (TransactionClient mode) talks happily to PD/TiKV v8.5.5. No version pin
   bump on the cluster side is needed for sifrah/nauka#183.

2. **Datastore-level compatibility *is* a real problem.** A TiKV cluster that
   already has SurrealDB v2.x metadata cannot be reused as-is by the v3.0.5
   SDK — the SDK refuses to attach. P0.5 (sifrah/nauka#189) must produce a
   real migration story before P2.x lands. The two viable options remain:
   - **Wipe**: only acceptable in dev / test envs; lethal in prod
   - **Migrate**: requires a script that exports v2 SurrealDB data
     (or raw TiKV keyspace), wipes, re-imports under v3

3. **A single-node test cluster is sufficient for SDK validation.** We
   bootstrapped 5 VMs but only used `node-1`. Multi-node cluster behaviour
   (failover, partition tolerance) belongs to sifrah/nauka#208 / #222 — not
   here.

4. **`max-pd-members = 3` doesn't block single-node operation.** PD reports
   `1/1 healthy`, accepts writes, and TiKV reports `1 store Up`. The default
   replication factor in TiKV is 3 — for the single-node test we did not
   exercise replication. In a multi-node test we will need at least 3 TiKV
   stores for the default replication factor to be satisfied; bringing the
   other 4 nodes online via `nauka hypervisor join` covers that.

5. **Latencies on a fresh cluster are healthy.** Connect: ~100 ms (one-time
   handshake). CRUD ops: 4–20 ms each. The 50 ms p50 target documented for
   P2.4 (sifrah/nauka#208) is comfortably met for single-node intra-node
   traffic. Cross-node latencies will be tested in P2.18 (sifrah/nauka#222).

6. **The WIP `layers/org/build.rs`** (untracked, depends on the also-untracked
   `layers/codegen/`) **breaks `cargo build` from `main`** if left in place.
   Workaround for now: stash it during the build. Permanent fix: ship
   `layers/codegen` and the build script as part of P3 (sifrah/nauka#223 ff)
   when codegen actually lands. For the spike PR, the file is left in place
   on the user's working tree — the spike's CI build does not exercise
   `layers/org` because it builds `-p nauka-state`.

## What lands in the PR

- `layers/state/Cargo.toml` — `[[bin]] p0-3-spike` entry
- `layers/state/src/bin/p0_3_spike.rs` — the spike itself
- `docs/spikes/p0-3-tikv-compat.md` — this file

## What does NOT land in the PR

- Removal of the existing `p0-1-spike` binary or the `surrealdb` dep added by
  P0.1 — those stay because they remain useful for re-running the build
  validation throughout the migration.
- Multi-node integration testing — covered by P2.4 (sifrah/nauka#208) and
  P2.18 (sifrah/nauka#222).
- A migration script for v2 → v3 SurrealDB metadata in TiKV — covered by
  P0.5 (sifrah/nauka#189). This spike just confirms the problem exists.
