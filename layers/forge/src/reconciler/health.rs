//! Health monitor — continuous health monitoring via doctor checks.
//!
//! Runs a subset of doctor checks every 60s (every other reconcile cycle)
//! and writes structured results to `/var/lib/nauka/health.json`.
//! Logs at WARN for degraded status, ERROR for critical (3 consecutive failures).

use std::net::Ipv6Addr;
use std::path::Path;
use std::process::Command;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use nauka_hypervisor::controlplane;
use nauka_hypervisor::fabric;
use nauka_state::EmbeddedDb;

/// Path where health status is written.
const HEALTH_FILE: &str = "/var/lib/nauka/health.json";

/// Number of consecutive failures before escalating to critical.
const CRITICAL_THRESHOLD: u32 = 3;

/// Tracks consecutive failure count across cycles.
static CONSECUTIVE_FAILURES: Mutex<u32> = Mutex::new(0);

/// Overall health status.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Critical,
}

/// A single health check result.
#[derive(Debug, Clone, serde::Serialize)]
pub struct HealthCheck {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

/// Full health report written to disk.
#[derive(Debug, serde::Serialize)]
pub struct HealthReport {
    pub timestamp: String,
    pub status: HealthStatus,
    pub checks: Vec<HealthCheck>,
}

/// Run health checks if this cycle is eligible (every other cycle = ~60s).
///
/// Called after all reconcilers complete. Returns true if checks ran.
pub async fn run_if_due(cycle: u64, mesh_ipv6: &Ipv6Addr) -> bool {
    // Run every other cycle (60s at 30s intervals).
    if !cycle.is_multiple_of(2) {
        return false;
    }

    let report = run_checks(mesh_ipv6).await;
    let status = report.status;

    // Update consecutive failure tracker
    let consecutive = {
        let mut guard = CONSECUTIVE_FAILURES
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if status == HealthStatus::Healthy {
            *guard = 0;
        } else {
            *guard += 1;
        }
        *guard
    };

    // Determine final status: escalate to critical after CRITICAL_THRESHOLD consecutive failures
    let final_status = if consecutive >= CRITICAL_THRESHOLD && status != HealthStatus::Healthy {
        HealthStatus::Critical
    } else {
        status
    };

    let final_report = HealthReport {
        timestamp: report.timestamp,
        status: final_status,
        checks: report.checks,
    };

    // Write to disk
    write_report(&final_report);

    // Log based on status
    match final_status {
        HealthStatus::Healthy => {
            tracing::debug!("health: all checks passed");
        }
        HealthStatus::Degraded => {
            let failed: Vec<_> = final_report
                .checks
                .iter()
                .filter(|c| !c.passed)
                .map(|c| c.name.as_str())
                .collect();
            tracing::warn!(
                failed_checks = ?failed,
                consecutive_failures = consecutive,
                "health: degraded"
            );
        }
        HealthStatus::Critical => {
            let failed: Vec<_> = final_report
                .checks
                .iter()
                .filter(|c| !c.passed)
                .map(|c| c.name.as_str())
                .collect();
            tracing::error!(
                failed_checks = ?failed,
                consecutive_failures = consecutive,
                "health: critical — {} consecutive failures",
                consecutive
            );
        }
    }

    true
}

/// Run the core health checks and return a report.
async fn run_checks(mesh_ipv6: &Ipv6Addr) -> HealthReport {
    let checks = vec![
        check_pd_health(mesh_ipv6),
        check_tikv_stores(mesh_ipv6),
        check_disk_space(),
        check_fabric_up().await,
        check_pd_service(),
        check_tikv_service(),
    ];

    let has_fail = checks.iter().any(|c| !c.passed);
    let status = if has_fail {
        HealthStatus::Degraded
    } else {
        HealthStatus::Healthy
    };

    HealthReport {
        timestamp: iso8601_now(),
        status,
        checks,
    }
}

/// Check PD health via its API (no shell out to curl — reuse the same pattern as reconcilers).
fn check_pd_health(mesh_ipv6: &Ipv6Addr) -> HealthCheck {
    let pd_url = format!(
        "http://[{}]:{}/pd/api/v1/health",
        mesh_ipv6,
        controlplane::PD_CLIENT_PORT,
    );

    match curl_json(&pd_url) {
        Some(body) => {
            if let Some(members) = body.as_array() {
                let healthy = members
                    .iter()
                    .filter(|m| m["health"].as_bool().unwrap_or(false))
                    .count();
                let total = members.len();
                if healthy == total {
                    HealthCheck {
                        name: "pd_health".into(),
                        passed: true,
                        detail: format!("{total}/{total} healthy"),
                    }
                } else {
                    HealthCheck {
                        name: "pd_health".into(),
                        passed: false,
                        detail: format!("{healthy}/{total} healthy"),
                    }
                }
            } else {
                HealthCheck {
                    name: "pd_health".into(),
                    passed: false,
                    detail: "unexpected response format".into(),
                }
            }
        }
        None => HealthCheck {
            name: "pd_health".into(),
            passed: false,
            detail: "PD API unreachable".into(),
        },
    }
}

/// Check TiKV store states via PD API.
fn check_tikv_stores(mesh_ipv6: &Ipv6Addr) -> HealthCheck {
    let stores_url = format!(
        "http://[{}]:{}/pd/api/v1/stores",
        mesh_ipv6,
        controlplane::PD_CLIENT_PORT,
    );

    match curl_json(&stores_url) {
        Some(body) => {
            if let Some(stores) = body["stores"].as_array() {
                let mut down = 0u64;
                let mut offline = 0u64;
                let mut tombstoned = 0u64;
                let mut up = 0u64;

                for store in stores {
                    match store["store"]["state_name"].as_str() {
                        Some("Up") => up += 1,
                        Some("Down") => down += 1,
                        Some("Offline") => offline += 1,
                        Some("Tombstone") => tombstoned += 1,
                        _ => {}
                    }
                }

                if down > 0 || tombstoned > 0 {
                    HealthCheck {
                        name: "tikv_stores".into(),
                        passed: false,
                        detail: format!(
                            "{up} up, {down} down, {offline} offline, {tombstoned} tombstoned"
                        ),
                    }
                } else {
                    HealthCheck {
                        name: "tikv_stores".into(),
                        passed: true,
                        detail: format!("{up} up, {offline} offline"),
                    }
                }
            } else {
                HealthCheck {
                    name: "tikv_stores".into(),
                    passed: true,
                    detail: "no stores registered".into(),
                }
            }
        }
        None => HealthCheck {
            name: "tikv_stores".into(),
            passed: false,
            detail: "PD API unreachable".into(),
        },
    }
}

/// Check disk space on /var/lib/nauka.
fn check_disk_space() -> HealthCheck {
    match disk_free_mb("/var/lib/nauka") {
        Some(free_mb) if free_mb > 1024 => HealthCheck {
            name: "disk_space".into(),
            passed: true,
            detail: format!("{}GB free", free_mb / 1024),
        },
        Some(free_mb) if free_mb > 256 => HealthCheck {
            name: "disk_space".into(),
            passed: true,
            detail: format!("{free_mb}MB free (low)"),
        },
        Some(free_mb) => HealthCheck {
            name: "disk_space".into(),
            passed: false,
            detail: format!("{free_mb}MB free (critical)"),
        },
        None => HealthCheck {
            name: "disk_space".into(),
            passed: true,
            detail: "could not read disk usage".into(),
        },
    }
}

/// Check that the fabric network interface is up.
async fn check_fabric_up() -> HealthCheck {
    let network_mode = match EmbeddedDb::open_default().await {
        Ok(db) => {
            let state = fabric::state::FabricState::load(&db).await.ok().flatten();
            let _ = db.shutdown().await;
            state.map(|s| s.network_mode).unwrap_or_default()
        }
        Err(_) => fabric::NetworkMode::default(),
    };
    let backend = fabric::backend::create_backend(network_mode);

    if backend.is_up() {
        HealthCheck {
            name: "fabric_network".into(),
            passed: true,
            detail: "up".into(),
        }
    } else {
        HealthCheck {
            name: "fabric_network".into(),
            passed: false,
            detail: "down".into(),
        }
    }
}

/// Check PD service is active.
fn check_pd_service() -> HealthCheck {
    if controlplane::service::pd_is_active() {
        HealthCheck {
            name: "pd_service".into(),
            passed: true,
            detail: "active".into(),
        }
    } else if Path::new("/etc/systemd/system/nauka-pd.service").exists() {
        HealthCheck {
            name: "pd_service".into(),
            passed: false,
            detail: "installed but stopped".into(),
        }
    } else {
        // Not installed means this is a tikv-only node — not a failure.
        HealthCheck {
            name: "pd_service".into(),
            passed: true,
            detail: "not installed (tikv-only node)".into(),
        }
    }
}

/// Check TiKV service is active.
fn check_tikv_service() -> HealthCheck {
    if controlplane::service::tikv_is_active() {
        HealthCheck {
            name: "tikv_service".into(),
            passed: true,
            detail: "active".into(),
        }
    } else if Path::new("/etc/systemd/system/nauka-tikv.service").exists() {
        HealthCheck {
            name: "tikv_service".into(),
            passed: false,
            detail: "installed but stopped".into(),
        }
    } else {
        HealthCheck {
            name: "tikv_service".into(),
            passed: false,
            detail: "not installed".into(),
        }
    }
}

/// Write the health report to disk as JSON.
fn write_report(report: &HealthReport) {
    // Ensure parent directory exists
    let _ = std::fs::create_dir_all("/var/lib/nauka");

    match serde_json::to_string_pretty(report) {
        Ok(json) => {
            if let Err(e) = std::fs::write(HEALTH_FILE, json) {
                tracing::warn!(error = %e, "failed to write health report");
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to serialize health report");
        }
    }
}

/// Generate an ISO 8601 timestamp for the current time.
fn iso8601_now() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();

    // Convert epoch seconds to UTC date/time components
    let days = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Days since 1970-01-01 to Y-M-D (simplified calendar arithmetic)
    let (year, month, day) = days_to_ymd(days);

    format!("{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}Z")
}

/// Convert days since Unix epoch to (year, month, day).
fn days_to_ymd(days: u64) -> (u64, u64, u64) {
    // Algorithm from http://howardhinnant.github.io/date_algorithms.html
    let z = days + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

// ===== Helpers (matching doctor.rs style) =====

fn curl_json(url: &str) -> Option<serde_json::Value> {
    let output = Command::new("curl")
        .args(["-sf", "--max-time", "5", url])
        .output()
        .ok()?;
    if output.status.success() {
        serde_json::from_slice(&output.stdout).ok()
    } else {
        None
    }
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
    fn iso8601_format() {
        let ts = iso8601_now();
        // Should match YYYY-MM-DDTHH:MM:SSZ
        assert!(ts.ends_with('Z'));
        assert_eq!(ts.len(), 20);
        assert_eq!(&ts[4..5], "-");
        assert_eq!(&ts[7..8], "-");
        assert_eq!(&ts[10..11], "T");
    }

    #[test]
    fn days_to_ymd_epoch() {
        assert_eq!(days_to_ymd(0), (1970, 1, 1));
    }

    #[test]
    fn days_to_ymd_known_date() {
        // 2026-04-10 is day 20553 since epoch
        assert_eq!(days_to_ymd(20553), (2026, 4, 10));
    }

    #[test]
    fn health_status_serializes() {
        assert_eq!(
            serde_json::to_string(&HealthStatus::Healthy).unwrap(),
            "\"healthy\""
        );
        assert_eq!(
            serde_json::to_string(&HealthStatus::Degraded).unwrap(),
            "\"degraded\""
        );
        assert_eq!(
            serde_json::to_string(&HealthStatus::Critical).unwrap(),
            "\"critical\""
        );
    }

    #[test]
    fn health_report_serializes() {
        let report = HealthReport {
            timestamp: "2026-04-10T20:00:00Z".into(),
            status: HealthStatus::Healthy,
            checks: vec![HealthCheck {
                name: "test".into(),
                passed: true,
                detail: "ok".into(),
            }],
        };
        let json = serde_json::to_string(&report).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["status"], "healthy");
        assert_eq!(parsed["checks"][0]["name"], "test");
        assert!(parsed["checks"][0]["passed"].as_bool().unwrap());
    }

    #[test]
    fn consecutive_failure_tracking() {
        // Reset state
        *CONSECUTIVE_FAILURES.lock().unwrap() = 0;

        // Simulate healthy
        {
            let mut guard = CONSECUTIVE_FAILURES.lock().unwrap();
            *guard = 0;
        }
        assert_eq!(*CONSECUTIVE_FAILURES.lock().unwrap(), 0);

        // Simulate 3 failures
        for _ in 0..3 {
            let mut guard = CONSECUTIVE_FAILURES.lock().unwrap();
            *guard += 1;
        }
        assert_eq!(*CONSECUTIVE_FAILURES.lock().unwrap(), 3);

        // Reset on healthy
        {
            let mut guard = CONSECUTIVE_FAILURES.lock().unwrap();
            *guard = 0;
        }
        assert_eq!(*CONSECUTIVE_FAILURES.lock().unwrap(), 0);
    }
}
