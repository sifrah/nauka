# P1.9 — Hetzner integration test for `EmbeddedDb<SurrealKv>`

**Issue:** sifrah/nauka#199
**Epic:** sifrah/nauka#183
**Status:** Done — all acceptance criteria met
**Date:** 2026-04-12

## Goal

Run a real CRUD cycle on a freshly-provisioned Hetzner VM, exit the binary,
re-launch it, and verify the data created by the first invocation is still
there. This is the strongest proof that `EmbeddedDb<SurrealKv>` actually
persists to disk on the production-target OS, not just in some long-lived
in-memory buffer that the in-process P1.5 tests can't distinguish from real
persistence.

## Result

| Criterion | Result |
|---|---|
| Provision an x86 Hetzner VM via `hcloud` | ✅ `node-p1-9` (cpx22 / ubuntu-24.04 / fsn1) |
| Cross-compile, scp, run an integration test binary | ✅ 54 MB statically-linked musl x86_64 |
| Cycle: open → create 3 records → select → restart → reopen → select returns same 3 records | ✅ Phase 1 (seed) + Phase 2 (verify) both exit 0 |
| Cleanup: tear down VM after test | ✅ `hcloud server delete node-p1-9` |
| Logs uploaded as artifact | ✅ this file (full captured stdout below) |

## How the test ran

The integration test binary `p1-9-integration` (in
`layers/state/src/bin/p1_9_integration.rs`) takes a `<phase>` argument:

- **`seed`** — opens `EmbeddedDb` at `/var/lib/nauka/p1-9-integration.skv`,
  creates three fixture records (`alpha`, `beta`, `gamma`), in-process
  selects them back, exits.
- **`verify`** — opens the *same* path in a *new* process, lists every
  record, asserts that all three are still there with their original
  values, exits.

The "restart" between the two phases is the binary process exiting and being
re-invoked by the test runner. That's a real process boundary — the kernel
reaps the seed process, then the runner starts the verify process from
scratch with no shared in-memory state.

## Test runner sequence

```bash
# Provision a fresh VM (cpx22 / ubuntu-24.04 / fsn1)
hcloud server create --name node-p1-9 --type cpx22 --image ubuntu-24.04 \
    --location fsn1 --ssh-key ifrah.sacha@gmail.com

# Cross-compile to musl x86_64
cargo build --target x86_64-unknown-linux-musl -p nauka-state \
    --bin p1-9-integration --release

# scp the binary
scp target/x86_64-unknown-linux-musl/release/p1-9-integration \
    root@$NEW_VM_IP:/root/p1-9-integration

# Run the seed phase, then the verify phase, capturing stdout
ssh root@$NEW_VM_IP 'set -e
    rm -rf /var/lib/nauka/p1-9-integration.skv
    chmod +x /root/p1-9-integration
    /root/p1-9-integration seed                # Phase 1
    ls -la /var/lib/nauka/p1-9-integration.skv/
    /root/p1-9-integration verify              # Phase 2 (post-restart)
    rm -rf /var/lib/nauka/p1-9-integration.skv
    rm /root/p1-9-integration
'

# Tear down the VM
hcloud server delete node-p1-9
```

## Captured logs (the artifact)

```
===== HOST INFO =====
node-p1-9
Linux node-p1-9 6.8.0-106-generic #106-Ubuntu SMP PREEMPT_DYNAMIC Fri Mar  6 07:58:08 UTC 2026 x86_64 x86_64 x86_64 GNU/Linux
PRETTY_NAME="Ubuntu 24.04.4 LTS"

===== BINARY INSPECTION =====
-rwxr-xr-x 1 root root 54M Apr 12 18:02 /root/p1-9-integration
	statically linked

===== ENSURE CLEAN STATE =====
(no /var/lib/nauka/p1-9-integration.skv)

===== PHASE 1: SEED =====
== nauka p1-9 integration ==
phase          = seed
path           = /var/lib/nauka/p1-9-integration.skv
target_arch    = x86_64
target_os      = linux
expected_count = 3
opened         = /var/lib/nauka/p1-9-integration.skv
created        = id=alpha name=first count=1
created        = id=beta name=second count=2
created        = id=gamma name=third count=3
seed_count     = 3
== p1-9 seed OK ==
(seed exit: 0)

===== INTERMEDIATE: ON-DISK STATE =====
total 28
drwxr-xr-x 6 root root 4096 Apr 12 18:02 .
drwx------ 3 root root 4096 Apr 12 18:02 ..
-rw-r--r-- 1 root root    5 Apr 12 18:02 LOCK
drwxr-xr-x 2 root root 4096 Apr 12 18:02 manifest
drwxr-xr-x 2 root root 4096 Apr 12 18:02 sstables
drwxr-xr-x 2 root root 4096 Apr 12 18:02 vlog
drwxr-x--- 2 root root 4096 Apr 12 18:02 wal
perms=755 owner=root:root

===== PHASE 2: VERIFY (post-restart) =====
== nauka p1-9 integration ==
phase          = verify
path           = /var/lib/nauka/p1-9-integration.skv
target_arch    = x86_64
target_os      = linux
expected_count = 3
opened         = /var/lib/nauka/p1-9-integration.skv
verify_count   = 3
verified       = id=alpha name=first count=1
verified       = id=beta name=second count=2
verified       = id=gamma name=third count=3
verify_status  = all 3 records intact across restart
== p1-9 verify OK ==
(verify exit: 0)

===== CLEANUP =====
done
```

## What the on-disk state shows

After the seed phase exited, SurrealKV had laid down its actual on-disk
structure under `/var/lib/nauka/p1-9-integration.skv/`:

```
LOCK         (5 bytes — datastore lock file released cleanly on exit)
manifest/    (the SurrealKV manifest directory)
sstables/    (sorted-string tables — where the records actually live)
vlog/        (value log)
wal/         (write-ahead log, mode 750 — protected)
```

The `LOCK` file was correctly released between phases — the verify phase
opened the same path with no `Database is already locked` error. That's the
P1.5 shutdown wait fix paying off in real conditions: the seed process drops
the `Surreal<Db>` client, the SDK background router task exits, calls
`Datastore::shutdown()` which flushes SurrealKV and removes the lock, all
of that within the 50ms wait we baked into `EmbeddedDb::shutdown`.

## Findings

1. **Persistence works.** The data written by the seed process is observable
   from a freshly-launched verify process. The bytes are on disk, not in
   shared memory or in some long-lived tokio task buffer.

2. **The shutdown wait holds up under real conditions.** The P1.5 fix to
   `EmbeddedDb::shutdown` (drop + yield + 50ms sleep) is enough to release
   the SurrealKV LOCK file between two binary invocations on a real
   filesystem. No tweaks needed.

3. **Default file perms on the SurrealKV-created files are 0o755 / 0o644.**
   The `EmbeddedDb::open` parent-dir chmod from P1.4 sets the *containing*
   `/var/lib/nauka/p1-9-integration.skv/` directory to the umask SurrealKV
   inherits, which on this Hetzner box is `0o755`. The acceptance criterion
   on P1.4 (#194) said "0700 dir, 0600 file", which we hit on the
   `/var/lib/nauka/` parent (set by `ensure_nauka_state_dir`), but not on
   the SurrealKV directory itself or its sstables. **Follow-up**: tighten
   permissions on the SurrealKV directory in a future ticket — not a
   blocker for P1.9, but worth tracking. The WAL subdir at `0o750` is the
   most-protected piece of the on-disk state today.

4. **Total binary size is 54 MB** for `kv-surrealkv` only, statically
   linked, not stripped, not LTO. Consistent with the P0.1 baseline. Same
   note as in `docs/cross-compile.md`: stripping + LTO will come in a
   later phase.

## What lands in the PR

- `layers/state/src/bin/p1_9_integration.rs` — the integration binary
- `layers/state/Cargo.toml` — `[[bin]]` entry for `p1-9-integration`
- `docs/spikes/p1-9-hetzner-integration.md` — this file (the artifact)

## What does NOT land

- The VM. `node-p1-9` was deleted at the end of the test as required by the
  cleanup criterion.
- Any `dist-check`-style debug binary. P1.9 only ships the integration
  binary itself.

## Follow-ups

- **Permissions on the SurrealKV directory.** The parent state dir is
  `0o700` per P1.4, but the SurrealKV subdirectory and its contents inherit
  the process umask (`0o755` / `0o644` on this Hetzner box). Tighten in a
  future ticket — does not affect this PR's acceptance criteria.
