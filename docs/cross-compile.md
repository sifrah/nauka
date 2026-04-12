# Cross-compiling Nauka for Linux musl x86_64

This is the operational reference for building Nauka binaries that can run on
the Hetzner production target. Hetzner nodes are Ubuntu x86_64 but Nauka ships
**statically linked musl** binaries to avoid glibc version drift between dev
machines and prod.

The original investigation lives in
[docs/spikes/p0-1-musl-cross-compile.md](spikes/p0-1-musl-cross-compile.md)
(P0.1, sifrah/nauka#185). This page is the durable how-to that survives once
the P1.x migration is done — keep it up to date.

## Toolchain requirements

### macOS (dev workstation)

Install once, with Homebrew:

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

You should not need to set `CC` or `AR` env vars manually; `cc-rs` picks the
right toolchain via the `x86_64-linux-musl-` prefix.

### Linux (CI runners and dev VMs)

On Debian / Ubuntu:

```bash
sudo apt-get update
sudo apt-get install -y musl-tools cmake build-essential
rustup target add x86_64-unknown-linux-musl
```

The CI workflow (`.github/workflows/nightly.yml`) installs `musl-tools`
already. `cmake` is also needed once a crate that pulls `aws-lc-sys` (which
includes everything in `nauka-state` after P1.1) is built — that's tracked by
P1.14 (sifrah/nauka#204).

### Vendored OpenSSL is not optional

Both `tikv-client 0.4` (legacy `ClusterDb`) and `surrealdb-tikv-client 0.3`
(future P2 path, gated behind `spike-tikv` for now) transitively depend on
`openssl-sys` via `prometheus → reqwest → native-tls`. Without vendored
OpenSSL in the dependency graph, the cross-compile fails with:

```
Could not find directory of OpenSSL installation
```

The workspace `Cargo.toml` declares `openssl = { version = "0.10", features
= ["vendored"] }`, but **each crate that needs it must opt in** with
`openssl.workspace = true`. Today the crates that do are:

- `bin/nauka` — for the existing musl release path
- `layers/state` — added in P0.1 (sifrah/nauka#185) when `nauka-state`
  started pulling SurrealDB

If you add a new crate that ends up in the dependency closure of
`tikv-client` or `surrealdb-tikv-client`, you must also add
`openssl.workspace = true` to its `Cargo.toml`.

## Building

The canonical command for building the `nauka-state` library and its bins
for musl:

```bash
cargo build --target x86_64-unknown-linux-musl -p nauka-state --release
```

For the full `nauka` binary (the one that ships to Hetzner):

```bash
cargo build --target x86_64-unknown-linux-musl -p nauka --release
```

The `nauka-state` build covers the `EmbeddedDb` wrapper plus the spike
binaries. The `nauka` build covers the full CLI plus all dependent layers.

The output binaries land at:

```
target/x86_64-unknown-linux-musl/release/nauka
target/x86_64-unknown-linux-musl/release/p0-1-spike
```

## Verifying the binary is statically linked

```bash
file target/x86_64-unknown-linux-musl/release/p0-1-spike
# expect: ELF 64-bit LSB pie executable, x86-64 [...] static-pie linked
```

On a Hetzner node:

```bash
ldd /root/p0-1-spike
# expect: statically linked  (or "not a dynamic executable")
```

## Hetzner smoke test

The standard "did the cross-compile work" smoke test, used by every Phase 1
ticket:

```bash
TARGET_BINARY="target/x86_64-unknown-linux-musl/release/p0-1-spike"
HETZNER_NODE="root@<node-ipv4>"

scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
    -o LogLevel=ERROR \
    "$TARGET_BINARY" "$HETZNER_NODE":/root/p0-1-spike

ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
    -o LogLevel=ERROR "$HETZNER_NODE" '
        chmod +x /root/p0-1-spike
        /root/p0-1-spike --help    # exit-zero, no side effects
        /root/p0-1-spike            # full Phase A + Phase B run
        rm /root/p0-1-spike
    '
```

If the binary loads, prints `--help`, then completes both phases (Phase A
exercises `EmbeddedDb::open` at a temp path, Phase B exercises
`EmbeddedDb::open_default` against `/var/lib/nauka/bootstrap.skv`), the
cross-compile chain is healthy.

## Common failures and what they mean

### `Could not find directory of OpenSSL installation`

A crate in your dependency closure pulls `openssl-sys` but no crate has
opted into `openssl.workspace = true`. Add `openssl.workspace = true` to the
`Cargo.toml` of the crate you're building (or any crate it depends on
within the workspace).

### `linker x86_64-linux-musl-gcc not found`

`musl-cross` (macOS) or `musl-tools` (Debian/Ubuntu) is not installed.
Install per "Toolchain requirements" above.

### `no such file or directory: target/x86_64-unknown-linux-musl/...`

The musl Rust target isn't installed. Run
`rustup target add x86_64-unknown-linux-musl`.

### `cmake: command not found` (during `aws-lc-sys` build)

Install `cmake` per "Toolchain requirements" above. This is the
direct trigger for the P1.14 issue (sifrah/nauka#204) on the CI side.

### Build succeeds but `ldd` shows dynamic libraries on the Hetzner node

You probably built for `x86_64-unknown-linux-gnu` instead of
`x86_64-unknown-linux-musl`. Double-check the `--target` flag.

## Build sizes (for context)

| Binary | Features | Size |
|---|---|---|
| `p0-1-spike` (debug) | `kv-surrealkv` only | ~250 MB |
| `p0-1-spike` (release) | `kv-surrealkv` only | ~53 MB |
| `p0-1-spike` (release) | `kv-surrealkv` + `spike-tikv` | ~61 MB (P0.1 baseline) |

The `~53 MB` release binary is dominated by SurrealDB's parser, query
planner, and storage layer. It's not yet stripped or LTO-optimised; the
real `nauka` shipping binary will be smaller after `[profile.release]
lto = "fat"` and `strip = true` land — that's a P3+ concern, not a Phase 1
blocker.

## See also

- ADR 0000 — BSL 1.1 license audit (sifrah/nauka#186): why we link
  `surrealdb-core` despite it being source-available rather than fully OSI
- ADR 0001 — TiKV API V1 (sifrah/nauka#188): which env vars / which surreal
  features Nauka uses
- P0.1 spike — original investigation (sifrah/nauka#185): more context on
  why each toolchain piece is needed
- P1.14 (sifrah/nauka#204): CI is missing `cmake`; this PR doesn't fix that
  but the doc above is the authoritative reference for what to install
