# Release Process

Nauka uses a 3-channel release model: **nightly**, **beta**, and **stable**.

## Channels

| Channel | Audience | Stability | How it's created |
|---------|----------|-----------|-----------------|
| **nightly** | Developers, CI | May break | Automatic on every push to `main` |
| **beta** | Internal testing, early adopters | Should work | Manual promotion from nightly |
| **stable** | Production | Must work | Manual promotion from beta |

## Version scheme

All versions follow semver: `MAJOR.MINOR.PATCH`

- Nightly: `v0.1.0-nightly.42`
- Beta: `v0.1.0-beta.3`
- Stable: `v0.1.0`

The base version is auto-calculated from commit messages since the last stable tag:
- `BREAKING` or `breaking:` in commit → major bump
- `feat:` in commit → minor bump
- Anything else → patch bump

## Flow

```
push to main
     │
     ▼
  nightly (auto)
     │
     ▼  promote (manual)
   beta
     │
     ▼  promote (manual)
  stable
```

## Creating a nightly

Automatic. Every push to `main` that touches source code (`layers/*/src/**`, `bin/*/src/**`, `Cargo.toml`, `Cargo.lock`) triggers the **Nightly** workflow.

It builds for 4 targets:
- `x86_64-unknown-linux-musl`
- `aarch64-unknown-linux-musl`
- `x86_64-apple-darwin`
- `aarch64-apple-darwin`

Then creates a GitHub release with the archives and checksums.

## Promoting nightly → beta

1. Go to **Actions** → **Promote**
2. Click **Run workflow**
3. Enter the nightly tag (e.g. `v0.1.0-nightly.5`)
4. Select **beta**
5. Run

This re-tags the same artifacts — no rebuild.

## Promoting beta → stable

Same process, but select **stable**. Only beta tags can be promoted to stable.

Promotion path is enforced:
- `nightly → beta` (can't skip to stable)
- `beta → stable`

## Installing a specific channel

```sh
# Stable (default)
curl -fsSL https://github.com/sifrah/nauka/raw/main/scripts/install.sh | sh

# Beta
curl -fsSL https://github.com/sifrah/nauka/raw/main/scripts/install.sh | sh -s -- --beta

# Nightly
curl -fsSL https://github.com/sifrah/nauka/raw/main/scripts/install.sh | sh -s -- --nightly
```

## Documentation

Each channel has its own docs version at:
- `https://sifrah.github.io/nauka/stable/`
- `https://sifrah.github.io/nauka/beta/`
- `https://sifrah.github.io/nauka/nightly/`

Docs are built automatically:
- Push to `main` → updates **nightly** docs
- Release published → updates the corresponding channel docs
