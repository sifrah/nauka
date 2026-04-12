# P0.1 — Spike: cross-compile musl x86_64 with surrealdb 3.0.5

**Issue:** sifrah/nauka#185
**Epic:** sifrah/nauka#183
**Status:** Done — all acceptance criteria met
**Date:** 2026-04-12

## Goal

Prove that `surrealdb` 3.0.5 with both `kv-surrealkv` and `kv-tikv` features can be
cross-compiled to `x86_64-unknown-linux-musl` and that the resulting binary runs
on a real Hetzner Ubuntu host with no glibc dependency.

## Result

| Criterion | Result |
|---|---|
| `cargo build --target x86_64-unknown-linux-musl -p nauka-state` succeeds | ✅ 4m14s clean release build |
| Binary runs on Hetzner Ubuntu without glibc deps | ✅ ran on `node-1` (Ubuntu 24.04, x86_64) |
| `ldd` confirms musl only | ✅ `statically linked` |
| Document extra setup for musl/cmake | ✅ this file |

## Setup needed

### Local (macOS dev box)

```bash
brew install cmake          # required by aws-lc-sys (jsonwebtoken → surrealdb-core)
brew install musl-cross     # provides x86_64-linux-musl-gcc, used as the linker
rustup target add x86_64-unknown-linux-musl
```

The repo's `.cargo/config.toml` already wires the linker:

```toml
[target.x86_64-unknown-linux-musl]
linker = "x86_64-linux-musl-gcc"
```

### CI (GitHub Actions, ubuntu-latest)

Currently the `Nightly` workflow only installs `musl-tools` for the musl target.
Once `nauka-state` pulls `surrealdb`, the runner also needs `cmake`:

```yaml
- name: Install musl-tools and cmake
  if: matrix.target == 'x86_64-unknown-linux-musl'
  run: sudo apt-get update && sudo apt-get install -y musl-tools cmake
```

This change is tracked separately by sifrah/nauka#204 (P1.14 — CI: install cmake in
GitHub Actions runners). It is **not** part of this spike's PR — the spike only
needs to prove the build chain works locally + on Hetzner.

## What `nauka-state` looks like during the spike

The spike adds a temporary binary `p0-1-spike` and the necessary deps so the
build chain can be exercised. None of this is consumed by production code yet —
P1.1 (sifrah/nauka#191) is the issue that turns the dependency into the real
`EmbeddedDb` integration.

```toml
# layers/state/Cargo.toml
[dependencies]
# ... existing ...
openssl.workspace = true     # forces vendored OpenSSL into the graph
surrealdb = { version = "3.0.5", default-features = false, features = ["kv-surrealkv", "kv-tikv"] }

[[bin]]
name = "p0-1-spike"
path = "src/bin/p0_1_spike.rs"
```

The `openssl.workspace = true` line is **critical**. Both `tikv-client v0.4.0`
(existing) and `surrealdb-tikv-client v0.3.0-surreal.4` (pulled by the spike)
transitively depend on `openssl-sys` via `prometheus → reqwest → hyper-tls →
native-tls`. Without a workspace-vendored OpenSSL in the graph, the cross-compile
fails with:

```
Could not find directory of OpenSSL installation
```

The workspace already declares `openssl = { version = "0.10", features = ["vendored"] }`,
but it must be opted into per crate. The existing `bin/nauka` does so, which is
why the existing nauka musl builds work — `nauka-state` did not need it before
because nothing in `nauka-state` itself pulled an openssl-touching dep.

## Build commands actually used

```bash
# Native (Mac arm64) sanity check first — fast feedback for derive errors
cargo build -p nauka-state --bin p0-1-spike

# The real test — cross-compile to musl
cargo build --target x86_64-unknown-linux-musl -p nauka-state --bin p0-1-spike --release
```

Output binary: `target/x86_64-unknown-linux-musl/release/p0-1-spike` (61 MB).

## Hetzner test

```bash
scp target/x86_64-unknown-linux-musl/release/p0-1-spike root@46.225.149.211:/root/p0-1-spike

ssh root@46.225.149.211 '
  ldd /root/p0-1-spike
  chmod +x /root/p0-1-spike
  /root/p0-1-spike
  rm /root/p0-1-spike
'
```

Output:

```
statically linked
== nauka p0-1 spike ==
target_arch    = x86_64
target_os      = linux
target_env     = unix
surrealdb_dep  = 3.0.5 (kv-surrealkv + kv-tikv)
tikv_marker    = surrealdb::engine::local::TiKv
skv_path       = /tmp/nauka-p0-1-spike.skv
created        = Some(SpikeRecord { name: "p0-1", answer: 42 })
fetched        = Some(SpikeRecord { name: "p0-1", answer: 42 })
all_count      = 1
== p0-1 spike OK ==
```

The spike opens an in-process `SurrealKv` datastore in `/tmp`, runs a CRUD round
trip via SurrealQL, and references `surrealdb::engine::local::TiKv` so the
linker keeps the `kv-tikv` feature symbols. It does **not** connect to a TiKV
cluster — that validation belongs to P0.3 (sifrah/nauka#187).

## Native deps confirmation

`cargo tree -p nauka-state -i openssl-sys` after the spike additions:

```
openssl-sys v0.9.112
├── native-tls v0.2.18
│   ├── hyper-tls v0.6.0
│   │   └── reqwest v0.12.28
│   │       └── prometheus v0.13.4
│   │           ├── surrealdb-tikv-client v0.3.0-surreal.4
│   │           │   └── surrealdb-core v3.0.5
│   │           │       └── surrealdb v3.0.5
│   │           │           └── nauka-state v2.0.0
│   │           └── tikv-client v0.4.0
│   │               └── nauka-state v2.0.0
```

Beyond `openssl-sys`, no other surprise native deps surfaced. `cmake` is
required only for `aws-lc-sys` (via `jsonwebtoken`, forced by `surrealdb-core`),
which is what the documentation predicted.

## What lands in the PR

- `layers/state/Cargo.toml` — `surrealdb` + `openssl.workspace = true` + `[[bin]]`
- `layers/state/src/bin/p0_1_spike.rs` — the spike binary
- `docs/spikes/p0-1-musl-cross-compile.md` — this file

## What does NOT land in the PR

- Removal of the spike binary or rolling back the deps. The deps stay because
  P1.1 (sifrah/nauka#191) will use them anyway, and the spike binary makes
  re-validating the build trivial during the rest of the migration.
- CI changes (cmake install). Tracked separately by P1.14 (sifrah/nauka#204).

## Risks identified for next phases

1. **Build size** — the spike binary is 61 MB release, which is large for a
   single binary. The full `nauka` binary will pick up surrealdb on top of all
   existing deps; a stripped, LTO build is probably needed. Track in P1 work.

2. **TiKV client coexistence** — `tikv-client 0.4` and `surrealdb-tikv-client
   0.3.x` both compiled together. Workable but adds binary bloat. P2.16
   (sifrah/nauka#220) removes the old one.

3. **Build time** — clean release cross-compile for the spike took 4m14s. Full
   workspace builds will be longer. CI cache (`Swatinem/rust-cache@v2`) is
   already in place which mitigates incremental builds.

4. **CI runners** — `cmake` install must be added before the `kv-tikv` /
   `kv-surrealkv` deps land in any crate compiled by CI. P1.14 (sifrah/nauka#204)
   handles this.
