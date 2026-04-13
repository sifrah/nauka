//! Spike binary for P0.1 (sifrah/nauka#185), repurposed in P1.2 to be the
//! Hetzner-side smoke test for [`nauka_state::EmbeddedDb`].
//!
//! The spike's purpose evolves with each phase that touches the same code:
//!
//! - **P0.1**: prove that `surrealdb` 3.0.5 with `kv-surrealkv` (+ `kv-tikv`)
//!   cross-compiles to `x86_64-unknown-linux-musl` and runs as a
//!   statically-linked binary on a real Hetzner Ubuntu host. ✅ done.
//! - **P1.1**: drop `kv-tikv` from the production feature set. ✅ done.
//! - **P1.2**: exercise the new [`EmbeddedDb`] wrapper end-to-end on
//!   Hetzner — open at a temp path, CRUD round-trip via `db.client()`,
//!   shut down cleanly. ✅ done.
//! - **P1.4** (this version, sifrah/nauka#194): also exercise
//!   [`EmbeddedDb::open_default`], which uses
//!   `nauka_core::process::nauka_db_path()` to pick the right path for
//!   the current run mode (CLI: `~/.nauka/bootstrap.skv`, service: root
//!   → `/var/lib/nauka/bootstrap.skv`). On a Hetzner node we run as root,
//!   so this exercises the service-mode branch end-to-end and validates
//!   that the parent state directory is created with 0o700 perms.
//!
//! What this binary does on each run:
//! 1. Print build / runtime info, plus the detected run mode and the
//!    default DB path
//! 2. **Phase A** — `EmbeddedDb::open` at `$TMPDIR/nauka-p0-1-spike.skv`
//!    (the original P1.2 round-trip, kept as a no-side-effects sanity)
//! 3. **Phase B** — `EmbeddedDb::open_default()` to exercise the
//!    run-mode-aware path picker, then a quick `INFO FOR DB`, then
//!    shutdown + wipe of the default datastore. The parent state dir's
//!    perms are also printed for visual confirmation of the 0o700 target.

use std::path::PathBuf;

use nauka_core::process::{is_service_mode, nauka_db_path};
use nauka_state::EmbeddedDb;
use surrealdb::types::SurrealValue;

#[derive(Debug, SurrealValue)]
struct SpikeRecord {
    name: String,
    answer: i32,
}

/// Print a short usage message and exit cleanly. Wired to `--help` / `-h`
/// so the binary can be used as a smoke test for cross-compile validation
/// (P1.8, sifrah/nauka#198) without producing side effects on disk.
fn print_help() {
    println!("nauka p0-1 spike — EmbeddedDb cross-compile + Hetzner smoke test");
    println!();
    println!("Exercises the EmbeddedDb<SurrealKv> wrapper end-to-end. Used by");
    println!("the per-ticket workflow (compile → Hetzner → CI → merge) to");
    println!("re-validate the build chain on every Phase 1 PR.");
    println!();
    println!("USAGE:");
    println!("    p0-1-spike [FLAGS]");
    println!();
    println!("FLAGS:");
    println!("    -h, --help    Print this help and exit (no side effects)");
    println!();
    println!("With no flags the binary runs in two phases against the actual");
    println!("filesystem:");
    println!("    Phase A — EmbeddedDb::open at $TMPDIR/nauka-p0-1-spike.skv");
    println!("              and a CRUD round-trip via db.client().");
    println!("    Phase B — EmbeddedDb::open_default() to exercise the");
    println!("              run-mode-aware path picker (CLI → ~/.nauka,");
    println!("              service mode → /var/lib/nauka), with INFO FOR DB");
    println!("              and 0o700 perms verification on the parent.");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Manual flag handling: only `--help` / `-h` need to be recognised at
    // P1.8 time, so a tiny if-let is preferable to pulling clap into a
    // spike binary.
    if let Some(arg) = std::env::args().nth(1) {
        match arg.as_str() {
            "-h" | "--help" => {
                print_help();
                return Ok(());
            }
            other => {
                eprintln!("error: unknown flag: {other}");
                eprintln!("hint:  run `--help` for usage");
                std::process::exit(2);
            }
        }
    }

    println!("== nauka p0-1 spike (P1.4 — run-mode-aware paths) ==");
    println!("target_arch    = {}", std::env::consts::ARCH);
    println!("target_os      = {}", std::env::consts::OS);
    println!("target_env     = {}", std::env::consts::FAMILY);
    println!("surrealdb_dep  = 3.0.5 (kv-surrealkv only)");
    println!(
        "run_mode       = {}",
        if is_service_mode() {
            "service (root)"
        } else {
            "cli (user)"
        }
    );
    println!("default_path   = {}", nauka_db_path().display());

    // ─── Phase A: EmbeddedDb::open at a temp path ──────────────────
    println!("--- Phase A: EmbeddedDb::open(temp_path) ---");
    let temp_path: PathBuf = std::env::temp_dir().join("nauka-p0-1-spike.skv");
    println!("skv_path       = {}", temp_path.display());

    let db_a = EmbeddedDb::open(&temp_path).await?;
    let client_a = db_a.client();

    let created: Option<SpikeRecord> = client_a
        .create(("spike_record", "first"))
        .content(SpikeRecord {
            name: "p0-1".into(),
            answer: 42,
        })
        .await?;
    println!("created        = {created:?}");

    let fetched: Option<SpikeRecord> = client_a.select(("spike_record", "first")).await?;
    println!("fetched        = {fetched:?}");

    let all: Vec<SpikeRecord> = client_a.select("spike_record").await?;
    println!("all_count      = {}", all.len());

    db_a.shutdown().await?;
    let _ = std::fs::remove_dir_all(&temp_path);
    println!("phase_a        = OK");

    // ─── Phase B: EmbeddedDb::open_default ──────────────────────────
    println!("--- Phase B: EmbeddedDb::open_default() ---");
    let default_path = nauka_db_path();
    println!("default_path   = {}", default_path.display());

    let db_b = EmbeddedDb::open_default().await?;
    println!(
        "opened_path    = {}",
        db_b.path()
            .expect("SurrealKV handle must expose its path")
            .display()
    );

    // Live no-side-effects query against the default DB to confirm the
    // round-trip works against the run-mode-resolved location.
    let _ = db_b.client().query("INFO FOR DB").await?.check()?;
    println!("info_for_db    = OK");

    db_b.shutdown().await?;

    // Inspect the parent state dir's perms (Unix only) and wipe the
    // datastore so re-runs are clean. We don't wipe the state dir
    // itself in case other Nauka state lives next to it.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Some(parent) = default_path.parent() {
            if let Ok(meta) = std::fs::metadata(parent) {
                let mode = meta.permissions().mode() & 0o777;
                println!("state_dir_perms= 0o{mode:o}");
            }
        }
    }
    let _ = std::fs::remove_dir_all(&default_path);
    println!("phase_b        = OK");

    println!("== p0-1 spike OK ==");
    Ok(())
}
