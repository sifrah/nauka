//! Hetzner integration test binary for P1.9 (sifrah/nauka#199).
//!
//! Demonstrates that data written by `EmbeddedDb<SurrealKv>` survives
//! a process restart on a real Hetzner Ubuntu host. The same binary is
//! invoked twice from the test runner:
//!
//! 1. **`p1-9-integration seed`** — open the datastore at
//!    `/var/lib/nauka/p1-9-integration.skv`, create three records,
//!    select them back to verify the in-process round-trip, exit.
//! 2. **`p1-9-integration verify`** — re-open the *same* datastore
//!    in a *new* process, list every record, assert that all three
//!    survived the restart with their original values, exit.
//!
//! Between the two invocations the binary process exits, the
//! kernel reaps it, and the test runner re-launches it. That gap is
//! the "restart" that P1.5's in-process `persistence_across_reopen`
//! test cannot exercise — only an actual process boundary proves
//! the data is on disk and not just in some long-lived in-memory
//! buffer.
//!
//! Cleanup is the test runner's job: provision the VM, run the
//! binary in both phases, capture stdout, then delete the VM.

use std::path::PathBuf;

use nauka_state::EmbeddedDb;
use surrealdb::types::SurrealValue;

#[derive(Debug, Clone, PartialEq, SurrealValue)]
struct Record {
    name: String,
    count: i64,
}

/// The fixed test fixture: three records the seed phase writes and the
/// verify phase asserts on. Defined as a constant so any drift between
/// the phases would be a compile error rather than a runtime mismatch.
const RECORDS: &[(&str, &str, i64)] = &[
    ("alpha", "first", 1),
    ("beta", "second", 2),
    ("gamma", "third", 3),
];

const TEST_PATH: &str = "/var/lib/nauka/p1-9-integration.skv";
const TABLE: &str = "p1_9_record";

fn print_help() {
    println!("nauka p1-9 integration — EmbeddedDb<SurrealKv> persist-across-restart");
    println!();
    println!("Used by P1.9 (sifrah/nauka#199) to prove that data written via");
    println!("EmbeddedDb<SurrealKv> survives a process restart on a real");
    println!("Hetzner host. Run by the test runner in two phases.");
    println!();
    println!("USAGE:");
    println!("    p1-9-integration <PHASE>");
    println!();
    println!("PHASES:");
    println!("    seed     Open the datastore, create 3 records, select them");
    println!("             back, exit. Datastore is at:");
    println!("             {TEST_PATH}");
    println!("    verify   Open the datastore in a fresh process, list all");
    println!("             records, assert the 3 records from `seed` are");
    println!("             still there with their original values, exit 0.");
    println!("    -h       Print this help and exit.");
    println!("    --help");
    println!();
    println!("The test runner is responsible for the surrounding ritual:");
    println!("provision the VM, scp the binary, run `seed`, run `verify`, then");
    println!("delete the VM. The binary itself does not touch hcloud.");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let phase = match std::env::args().nth(1).as_deref() {
        Some("-h") | Some("--help") => {
            print_help();
            return Ok(());
        }
        Some("seed") => "seed",
        Some("verify") => "verify",
        Some(other) => {
            eprintln!("error: unknown phase: {other}");
            eprintln!("hint:  run `--help` for usage");
            std::process::exit(2);
        }
        None => {
            eprintln!("error: phase required (seed | verify)");
            eprintln!("hint:  run `--help` for usage");
            std::process::exit(2);
        }
    };

    let path = PathBuf::from(TEST_PATH);
    println!("== nauka p1-9 integration ==");
    println!("phase          = {phase}");
    println!("path           = {}", path.display());
    println!("target_arch    = {}", std::env::consts::ARCH);
    println!("target_os      = {}", std::env::consts::OS);
    println!("expected_count = {}", RECORDS.len());

    let db = EmbeddedDb::open(&path).await?;
    println!(
        "opened         = {}",
        db.path()
            .expect("SurrealKV handle must expose its path")
            .display()
    );

    match phase {
        "seed" => seed(&db).await?,
        "verify" => verify(&db).await?,
        _ => unreachable!(),
    }

    db.shutdown().await?;
    println!("== p1-9 {phase} OK ==");
    Ok(())
}

async fn seed(db: &EmbeddedDb) -> Result<(), Box<dyn std::error::Error>> {
    // The test runner is responsible for delivering a clean
    // /var/lib/nauka/p1-9-integration.skv before invoking `seed` —
    // attempting to pre-clean records here would actually error,
    // because SurrealDB returns NotFound on `delete` against a
    // never-touched table. Trust the runner.

    // Create the three fixture records.
    for (id, name, count) in RECORDS {
        let created: Option<Record> = db
            .client()
            .create((TABLE, *id))
            .content(Record {
                name: (*name).into(),
                count: *count,
            })
            .await?;
        let created = created.expect("create returned None");
        println!(
            "created        = id={id} name={} count={}",
            created.name, created.count
        );
    }

    // In-process round-trip: select-by-id for each record and assert
    // the round-trip works before we even consider the restart case.
    for (id, name, count) in RECORDS {
        let fetched: Option<Record> = db.client().select((TABLE, *id)).await?;
        let fetched =
            fetched.unwrap_or_else(|| panic!("seed: in-process select for id={id} returned None"));
        assert_eq!(fetched.name, *name, "seed: name mismatch for id={id}");
        assert_eq!(fetched.count, *count, "seed: count mismatch for id={id}");
    }

    let all: Vec<Record> = db.client().select(TABLE).await?;
    println!("seed_count     = {}", all.len());
    assert_eq!(
        all.len(),
        RECORDS.len(),
        "seed: expected {} records, got {}",
        RECORDS.len(),
        all.len()
    );
    Ok(())
}

async fn verify(db: &EmbeddedDb) -> Result<(), Box<dyn std::error::Error>> {
    // Cross-process verification: this binary was started fresh after
    // the seed phase exited. Reading the same records here proves the
    // data is on disk and not in some long-lived in-memory state.

    let all: Vec<Record> = db.client().select(TABLE).await?;
    println!("verify_count   = {}", all.len());
    assert_eq!(
        all.len(),
        RECORDS.len(),
        "verify: expected {} records after restart, got {}",
        RECORDS.len(),
        all.len()
    );

    for (id, name, count) in RECORDS {
        let fetched: Option<Record> = db.client().select((TABLE, *id)).await?;
        let fetched = fetched.unwrap_or_else(|| {
            panic!("verify: post-restart select for id={id} returned None — data lost?")
        });
        assert_eq!(
            fetched.name, *name,
            "verify: name mismatch for id={id} (lost write?)"
        );
        assert_eq!(
            fetched.count, *count,
            "verify: count mismatch for id={id} (corrupted?)"
        );
        println!(
            "verified       = id={id} name={} count={}",
            fetched.name, fetched.count
        );
    }

    println!(
        "verify_status  = all {} records intact across restart",
        RECORDS.len()
    );
    Ok(())
}
