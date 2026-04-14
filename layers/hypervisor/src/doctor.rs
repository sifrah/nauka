//! Hypervisor doctor — diagnose the health of all sub-systems.
//!
//! Runs sequential checks against fabric, controlplane, storage, and system.
//! Outputs a human-readable report with OK / WARN / FAIL for each check.

use std::net::Ipv6Addr;
use std::path::Path;
use std::process::Command;

use crate::controlplane;
use crate::fabric;
use crate::storage;

// ═══════════════════════════════════════════════════
// Check result types
// ═══════════════════════════════════════════════════

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CheckStatus {
    Ok,
    Warn,
    Fail,
    Skip,
}

#[derive(Debug, Clone)]
pub struct Check {
    pub name: String,
    pub status: CheckStatus,
    pub detail: String,
}

#[derive(Debug, Default)]
pub struct DoctorReport {
    pub checks: Vec<(String, Vec<Check>)>, // (section_name, checks)
}

impl DoctorReport {
    fn add_section(&mut self, name: &str) -> &mut Vec<Check> {
        self.checks.push((name.to_string(), Vec::new()));
        &mut self.checks.last_mut().unwrap().1
    }

    pub fn ok_count(&self) -> usize {
        self.checks
            .iter()
            .flat_map(|(_, c)| c)
            .filter(|c| c.status == CheckStatus::Ok)
            .count()
    }

    pub fn warn_count(&self) -> usize {
        self.checks
            .iter()
            .flat_map(|(_, c)| c)
            .filter(|c| c.status == CheckStatus::Warn)
            .count()
    }

    pub fn fail_count(&self) -> usize {
        self.checks
            .iter()
            .flat_map(|(_, c)| c)
            .filter(|c| c.status == CheckStatus::Fail)
            .count()
    }

    /// Render the report to stderr.
    pub fn print(&self) {
        for (section, checks) in &self.checks {
            eprintln!();
            eprintln!("  {section}");
            for check in checks {
                let icon = match check.status {
                    CheckStatus::Ok => "\x1b[32m✓\x1b[0m",
                    CheckStatus::Warn => "\x1b[33m!\x1b[0m",
                    CheckStatus::Fail => "\x1b[31m✗\x1b[0m",
                    CheckStatus::Skip => "\x1b[90m-\x1b[0m",
                };
                eprintln!("    {icon} {}: {}", check.name, check.detail);
            }
        }
        eprintln!();
        eprintln!(
            "  {} passed, {} warnings, {} errors",
            self.ok_count(),
            self.warn_count(),
            self.fail_count()
        );
    }
}

fn ok(name: &str, detail: &str) -> Check {
    Check {
        name: name.into(),
        status: CheckStatus::Ok,
        detail: detail.into(),
    }
}

fn warn(name: &str, detail: &str) -> Check {
    Check {
        name: name.into(),
        status: CheckStatus::Warn,
        detail: detail.into(),
    }
}

fn fail(name: &str, detail: &str) -> Check {
    Check {
        name: name.into(),
        status: CheckStatus::Fail,
        detail: detail.into(),
    }
}

fn skip(name: &str, detail: &str) -> Check {
    Check {
        name: name.into(),
        status: CheckStatus::Skip,
        detail: detail.into(),
    }
}

// ═══════════════════════════════════════════════════
// Run all checks
// ═══════════════════════════════════════════════════

pub async fn run() -> DoctorReport {
    let mut report = DoctorReport::default();

    // Load state from the SurrealKV-backed EmbeddedDb. We open it here so
    // both the fabric check and the storage check see the same snapshot,
    // then drop it before running the other checks so the flock is free.
    let (state, storage_statuses) = load_doctor_context().await;

    let mesh_ipv6 = state.as_ref().map(|s| s.hypervisor.mesh_ipv6);

    check_fabric(&mut report, &state);
    check_controlplane(&mut report, mesh_ipv6.as_ref());
    check_surrealdb(&mut report).await;
    check_storage(&mut report, &storage_statuses);
    check_system(&mut report);

    report
}

/// Load the single-pass doctor snapshot: fabric state + region statuses.
///
/// Both are read through a single `EmbeddedDb` handle that is then
/// explicitly shut down, so the rest of the doctor run doesn't contend
/// with the SurrealKV flock.
async fn load_doctor_context() -> (
    Option<fabric::state::FabricState>,
    Vec<storage::ops::RegionStatus>,
) {
    let db = match nauka_state::EmbeddedDb::open_default().await {
        Ok(db) => db,
        Err(_) => return (None, Vec::new()),
    };

    let state = fabric::state::FabricState::load(&db).await.ok().flatten();
    let statuses = storage::ops::status(&db).await;

    let _ = db.shutdown().await;
    (state, statuses)
}

// ═══════════════════════════════════════════════════
// Fabric checks
// ═══════════════════════════════════════════════════

fn check_fabric(report: &mut DoctorReport, state: &Option<fabric::state::FabricState>) {
    let checks = report.add_section("Fabric");

    let network_mode = state.as_ref().map(|s| s.network_mode).unwrap_or_default();
    let backend = fabric::backend::create_backend(network_mode);

    match network_mode {
        fabric::backend::NetworkMode::WireGuard => {
            // wireguard-tools installed
            if cmd_exists("wg") {
                checks.push(ok("wireguard-tools", "installed"));
            } else {
                checks.push(fail("wireguard-tools", "not installed"));
            }

            // Config file
            if Path::new("/etc/wireguard/nauka0.conf").exists() {
                checks.push(ok("wg config", "/etc/wireguard/nauka0.conf exists"));
            } else {
                checks.push(fail("wg config", "missing"));
            }
        }
        fabric::backend::NetworkMode::Direct => {
            checks.push(ok("network mode", "direct (no tunnel)"));
        }
        fabric::backend::NetworkMode::Mock => {
            checks.push(ok("network mode", "mock (testing)"));
        }
    }

    // Interface/service up (backend-agnostic)
    if backend.is_up() {
        checks.push(ok("fabric network", "up"));
    } else {
        checks.push(fail("fabric network", "down"));
    }

    if backend.is_active() {
        checks.push(ok("fabric service", "active"));
    } else {
        checks.push(warn("fabric service", "stopped"));
    }

    // Peer health
    if let Some(state) = state {
        let total = state.peers.len();
        if total == 0 {
            checks.push(ok("peers", "no peers configured"));
        } else {
            let active = state.peers.active_count();
            let unreachable = state.peers.unreachable_count();
            if unreachable == 0 {
                checks.push(ok("peers", &format!("{total}/{total} reachable")));
            } else {
                checks.push(warn(
                    "peers",
                    &format!("{active}/{total} reachable, {unreachable} unreachable"),
                ));
            }

            // Ping test (sample first 3 peers)
            let mut ping_ok = 0;
            let mut ping_fail = 0;
            for peer in state.peers.peers.iter().take(3) {
                if ping6(&peer.mesh_ipv6) {
                    ping_ok += 1;
                } else {
                    ping_fail += 1;
                }
            }
            if ping_fail == 0 {
                checks.push(ok("mesh ping", &format!("{ping_ok}/{ping_ok} responded")));
            } else {
                checks.push(warn(
                    "mesh ping",
                    &format!("{ping_ok}/{} responded", ping_ok + ping_fail),
                ));
            }
        }
    } else {
        checks.push(skip("peers", "not initialized"));
    }
}

// ═══════════════════════════════════════════════════
// Controlplane checks
// ═══════════════════════════════════════════════════

fn check_controlplane(report: &mut DoctorReport, mesh_ipv6: Option<&Ipv6Addr>) {
    let checks = report.add_section("Controlplane");

    // TiUP installed
    if Path::new("/opt/nauka/tiup/bin/tiup").exists() {
        checks.push(ok("tiup", "installed"));
    } else {
        checks.push(skip("tiup", "not installed"));
        return;
    }

    // PD service
    if controlplane::service::pd_is_active() {
        checks.push(ok("pd service", "active"));
    } else if Path::new("/etc/systemd/system/nauka-pd.service").exists() {
        checks.push(warn("pd service", "installed but stopped"));
    } else {
        checks.push(ok("pd service", "not running (tikv-only node)"));
    }

    // TiKV service
    if controlplane::service::tikv_is_active() {
        checks.push(ok("tikv service", "active"));
    } else if Path::new("/etc/systemd/system/nauka-tikv.service").exists() {
        checks.push(fail("tikv service", "installed but stopped"));
    } else {
        checks.push(fail("tikv service", "not installed"));
    }

    // Version checks — compare installed vs expected
    match controlplane::service::installed_pd_version() {
        Some(ref v) if v == controlplane::PD_VERSION => {
            checks.push(ok("pd version", &format!("{v} (expected)")));
        }
        Some(ref v) => {
            checks.push(warn(
                "pd version",
                &format!("{v} (expected {})", controlplane::PD_VERSION),
            ));
        }
        None => {
            checks.push(skip("pd version", "not installed"));
        }
    }

    match controlplane::service::installed_tikv_version() {
        Some(ref v) if v == controlplane::TIKV_VERSION => {
            checks.push(ok("tikv version", &format!("{v} (expected)")));
        }
        Some(ref v) => {
            checks.push(warn(
                "tikv version",
                &format!("{v} (expected {})", controlplane::TIKV_VERSION),
            ));
        }
        None => {
            checks.push(skip("tikv version", "not installed"));
        }
    }

    // PD health + leader
    if let Some(ip) = mesh_ipv6 {
        let client = controlplane::pd_client::PdClient::from_mesh(ip);

        if let Ok(health) = client.get_health() {
            let total = health.len();
            let healthy_count = health.iter().filter(|h| h.healthy).count();
            if healthy_count == total {
                checks.push(ok("pd health", &format!("{total}/{total} healthy")));
            } else {
                checks.push(warn(
                    "pd health",
                    &format!("{healthy_count}/{total} healthy"),
                ));
            }

            // Leader
            let leader = health.iter().find(|h| h.healthy).map(|h| h.name.as_str());
            if let Some(name) = leader {
                checks.push(ok("pd leader", name));
            } else {
                checks.push(fail("pd leader", "no leader elected"));
            }
        } else {
            checks.push(warn("pd health", "could not reach PD API"));
        }

        // PD quorum health — are all members healthy?
        if let Ok(members) = client.get_members() {
            let total = members.len();
            if total < 3 {
                checks.push(warn(
                    "pd quorum",
                    &format!("only {total} member(s) — need 3 for fault tolerance"),
                ));
            } else {
                checks.push(ok("pd quorum", &format!("{total} members — quorum intact")));
            }
        }

        // TiKV store registration + health
        // Use get_stores_with_states to see all stores including offline/tombstoned
        if let Ok(stores) = client.get_stores_with_states(&[0, 1, 2, 3]) {
            checks.push(ok("tikv stores", &format!("{} registered", stores.len())));

            let mut tombstoned = 0u64;
            let mut offline = 0u64;
            let mut down = 0u64;
            for store in &stores {
                match store.state_name.as_str() {
                    "Tombstone" => tombstoned += 1,
                    "Offline" => offline += 1,
                    "Down" => down += 1,
                    _ => {}
                }
            }
            if tombstoned > 0 {
                checks.push(fail(
                    "tikv tombstoned",
                    &format!("{tombstoned} store(s) tombstoned — data may be under-replicated"),
                ));
            }
            if down > 0 {
                checks.push(fail("tikv down", &format!("{down} store(s) down")));
            }
            if offline > 0 {
                checks.push(warn(
                    "tikv offline",
                    &format!("{offline} store(s) offline — regions migrating"),
                ));
            }
            if tombstoned == 0 && offline == 0 && down == 0 {
                checks.push(ok("tikv store health", "all stores Up"));
            }
        }
    }
}

// ═══════════════════════════════════════════════════
// SurrealDB checks (P2.17 — sifrah/nauka#221)
// ═══════════════════════════════════════════════════

/// Verify the cluster-side SurrealDB is reachable, that the configured
/// namespace/database is selected, and that every expected schema
/// table from [`nauka_state::CLUSTER_TABLE_NAMES`] is present.
///
/// All three checks degrade gracefully: if `controlplane::connect`
/// fails (PD/TiKV down, cluster not initialised, …) the rest of the
/// section is skipped with an actionable hint pointing at the next
/// command to run, not a stack trace.
async fn check_surrealdb(report: &mut DoctorReport) {
    let checks = report.add_section("SurrealDB");

    // 1. Connectivity — go through the same controlplane helper every
    //    other layer uses so we exercise the real connect path
    //    (PD endpoint discovery, TiKv handshake, NS/DB select).
    let db = match controlplane::connect().await {
        Ok(db) => {
            checks.push(ok(
                "connectivity",
                "cluster reachable via controlplane::connect",
            ));
            db
        }
        Err(e) => {
            let msg = e.to_string();
            // Two distinct failure modes → two distinct fixes.
            let hint = if msg.contains("not initialized") {
                "run `nauka hypervisor init` to bootstrap a cluster"
            } else {
                "check PD/TiKV services with `nauka hypervisor cp-status`"
            };
            checks.push(fail("connectivity", &format!("{msg} — {hint}")));
            checks.push(skip("namespace/database", "skipped (no connection)"));
            checks.push(skip("schemas", "skipped (no connection)"));
            return;
        }
    };

    // 2. NS/DB sanity — `INFO FOR DB` returns the catalog of the
    //    currently-selected database, which is exactly the namespace
    //    [`nauka_state::EmbeddedDb::open_tikv`] selected on connect.
    //    A failure here means we connected but the use_ns/use_db call
    //    silently no-op'd (shouldn't happen) or the user pointed us at
    //    a database that exists but lacks SCHEMAFULL definitions.
    let info_value: Option<serde_json::Value> = match db.client().query("INFO FOR DB").await {
        Ok(mut res) => match res.take::<Option<serde_json::Value>>(0) {
            Ok(Some(v)) => {
                checks.push(ok(
                    "namespace/database",
                    &format!(
                        "{}/{} selected",
                        nauka_state::NAUKA_NS,
                        nauka_state::CLUSTER_DB
                    ),
                ));
                Some(v)
            }
            Ok(None) => {
                checks.push(fail("namespace/database", "INFO FOR DB returned no rows"));
                None
            }
            Err(e) => {
                checks.push(fail(
                    "namespace/database",
                    &format!("INFO FOR DB take failed: {e}"),
                ));
                None
            }
        },
        Err(e) => {
            checks.push(fail(
                "namespace/database",
                &format!(
                    "INFO FOR DB failed: {e} — schemas are not applied, \
                     re-run `nauka hypervisor init` on the bootstrap node"
                ),
            ));
            None
        }
    };

    // 3. Schemas — every table in the canonical list must appear in
    //    INFO FOR DB's `tables` map. Anything missing is actionable:
    //    upgrade the binary or re-bootstrap.
    if let Some(info) = info_value {
        let present: std::collections::HashSet<&str> = info
            .get("tables")
            .and_then(|t| t.as_object())
            .map(|m| m.keys().map(String::as_str).collect())
            .unwrap_or_default();

        // P2/#286: the canonical list of expected tables is whatever
        // was registered via `nauka_state::SchemaRegistration` at link
        // time. Walking the registry keeps this check in lockstep with
        // the schemas actually applied by `apply_cluster_schemas`.
        let expected: Vec<&str> = nauka_state::registrations()
            .iter()
            .map(|r| r.name)
            .collect();
        let missing: Vec<&str> = expected
            .iter()
            .copied()
            .filter(|t| !present.contains(t))
            .collect();

        if missing.is_empty() {
            checks.push(ok(
                "schemas",
                &format!("all {} cluster tables present", expected.len()),
            ));
        } else {
            checks.push(fail(
                "schemas",
                &format!(
                    "missing tables: {} — re-run `nauka hypervisor init` to apply schemas",
                    missing.join(", ")
                ),
            ));
        }
    } else {
        checks.push(skip("schemas", "skipped (INFO FOR DB unavailable)"));
    }

    let _ = db.shutdown().await;
}

// ═══════════════════════════════════════════════════
// Storage checks
// ═══════════════════════════════════════════════════

fn check_storage(report: &mut DoctorReport, statuses: &[storage::ops::RegionStatus]) {
    let checks = report.add_section("Storage");

    // ZeroFS installed
    if storage::service::is_installed() {
        checks.push(ok("zerofs", "installed"));
    } else {
        checks.push(skip("zerofs", "not installed"));
    }

    // Region configs — already fetched once by `load_doctor_context` so
    // the doctor doesn't open a second EmbeddedDb handle here.
    if statuses.is_empty() {
        checks.push(ok("regions", "none configured"));
    } else {
        for s in statuses {
            if s.active {
                checks.push(ok(
                    &format!("region {}", s.region),
                    &format!("active ({})", s.s3_bucket),
                ));
            } else {
                checks.push(warn(
                    &format!("region {}", s.region),
                    &format!("stopped ({})", s.s3_bucket),
                ));
            }
        }
    }
}

// ═══════════════════════════════════════════════════
// System checks
// ═══════════════════════════════════════════════════

fn check_system(report: &mut DoctorReport) {
    let checks = report.add_section("System");

    // Disk space
    if let Some(free_mb) = disk_free_mb("/var/lib/nauka") {
        if free_mb > 1024 {
            checks.push(ok("disk space", &format!("{}GB free", free_mb / 1024)));
        } else if free_mb > 256 {
            checks.push(warn("disk space", &format!("{free_mb}MB free (low)")));
        } else {
            checks.push(fail("disk space", &format!("{free_mb}MB free (critical)")));
        }
    }

    // Log directory
    if Path::new("/var/log/nauka").exists() {
        let log_file = Path::new("/var/log/nauka/nauka.log");
        if log_file.exists() || std::fs::File::create("/var/log/nauka/.doctor-test").is_ok() {
            let _ = std::fs::remove_file("/var/log/nauka/.doctor-test");
            checks.push(ok("logs", "/var/log/nauka writable"));
        } else {
            checks.push(warn("logs", "/var/log/nauka not writable"));
        }
    } else {
        checks.push(warn("logs", "/var/log/nauka missing"));
    }

    // State directory — SurrealKV-backed EmbeddedDb on disk.
    //
    // The legacy JSON check (`~/.nauka/hypervisor.json`) went away with
    // P1.11 (sifrah/nauka#201). We now point at the SurrealKV datastore
    // directory (CLI mode: `~/.nauka/bootstrap.skv`, service mode:
    // `/var/lib/nauka/bootstrap.skv`). The doctor treats a live LOCK
    // file inside that directory as a healthy signal — SurrealKV keeps
    // its on-disk state under this path whenever an EmbeddedDb has been
    // opened at least once.
    let state_path = nauka_core::process::nauka_db_path();
    if state_path.exists() {
        if state_path.join("LOCK").exists() {
            checks.push(ok("state dir", "bootstrap.skv present"));
        } else {
            checks.push(warn(
                "state dir",
                "bootstrap.skv exists but LOCK file missing",
            ));
        }
    } else {
        checks.push(skip("state dir", "not initialized"));
    }
}

// ═══════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════

fn cmd_exists(cmd: &str) -> bool {
    Command::new("which")
        .arg(cmd)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn ping6(addr: &Ipv6Addr) -> bool {
    Command::new("ping6")
        .args(["-c", "1", "-W", "2", &addr.to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn disk_free_mb(path: &str) -> Option<u64> {
    let output = Command::new("df")
        .args(["-m", "--output=avail", path])
        .output()
        .ok()?;
    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        stdout
            .lines()
            .last()
            .and_then(|l| l.trim().parse::<u64>().ok())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_counts() {
        let mut report = DoctorReport::default();
        let checks = report.add_section("Test");
        checks.push(ok("a", "good"));
        checks.push(ok("b", "good"));
        checks.push(warn("c", "meh"));
        checks.push(fail("d", "bad"));

        assert_eq!(report.ok_count(), 2);
        assert_eq!(report.warn_count(), 1);
        assert_eq!(report.fail_count(), 1);
    }

    #[test]
    fn check_constructors() {
        let c = ok("test", "detail");
        assert_eq!(c.status, CheckStatus::Ok);
        assert_eq!(c.name, "test");

        let c = warn("test", "detail");
        assert_eq!(c.status, CheckStatus::Warn);

        let c = fail("test", "detail");
        assert_eq!(c.status, CheckStatus::Fail);

        let c = skip("test", "detail");
        assert_eq!(c.status, CheckStatus::Skip);
    }

    #[test]
    fn cmd_exists_check() {
        assert!(cmd_exists("ls"));
        assert!(!cmd_exists("nonexistent_command_xyz"));
    }

    #[test]
    fn disk_free_check() {
        // / should always have some free space
        let free = disk_free_mb("/");
        assert!(free.is_some());
        assert!(free.unwrap() > 0);
    }

    #[tokio::test]
    async fn run_doctor_no_panic() {
        // On a test system, doctor should run without panic
        let report = run().await;
        // 5 sections: Fabric, Controlplane, SurrealDB (P2.17), Storage, System.
        assert_eq!(report.checks.len(), 5);
    }
}
