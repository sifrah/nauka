//! Typed PD (Placement Driver) HTTP client.
//!
//! Centralises all PD API calls behind a single `PdClient` struct with
//! typed request/response types, retry with exponential backoff, and
//! automatic endpoint failover.
//!
//! Still shells out to `curl` (no HTTP crate dependency) but all the
//! JSON parsing, error handling, and retry logic is in one place.

use std::net::Ipv6Addr;
use std::process::Command;

use nauka_core::error::NaukaError;
use serde::{Deserialize, Serialize};

// ═══════════════════════════════════════════════════
// Response types
// ═══════════════════════════════════════════════════

/// A PD cluster member.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PdMember {
    pub member_id: u64,
    pub name: String,
    pub peer_urls: Vec<String>,
    pub client_urls: Vec<String>,
    pub is_leader: bool,
    pub is_healthy: bool,
}

/// A TiKV store registered with PD.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TiKVStore {
    pub id: u64,
    pub address: String,
    pub state_name: String,
    pub capacity: String,
}

/// Region statistics from PD.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RegionStats {
    pub count: u64,
    pub empty_count: u64,
    pub miss_peer: u64,
    pub extra_peer: u64,
}

/// PD health entry (from /pd/api/v1/health).
#[derive(Debug, Clone)]
pub struct HealthEntry {
    pub member_id: u64,
    pub name: String,
    pub healthy: bool,
}

// ═══════════════════════════════════════════════════
// PdClient
// ═══════════════════════════════════════════════════

/// Typed HTTP client for the PD API.
///
/// Wraps `curl` with retry, endpoint failover, and typed responses.
/// Thread-safe and cheap to clone.
#[derive(Debug, Clone)]
pub struct PdClient {
    endpoints: Vec<String>,
    /// Default timeout per request in seconds.
    timeout_secs: u64,
}

impl PdClient {
    /// Create a client with multiple PD endpoints for failover.
    pub fn new(endpoints: Vec<String>) -> Self {
        Self {
            endpoints,
            timeout_secs: 10,
        }
    }

    /// Shortcut: single-node PD at the given mesh IPv6.
    pub fn from_mesh(mesh_ipv6: &Ipv6Addr) -> Self {
        Self::new(vec![format!(
            "http://[{}]:{}",
            mesh_ipv6,
            super::PD_CLIENT_PORT
        )])
    }

    /// Override the default per-request timeout (seconds).
    pub fn with_timeout(mut self, secs: u64) -> Self {
        self.timeout_secs = secs;
        self
    }

    // ───────────────────────────────────────────────
    // Health
    // ───────────────────────────────────────────────

    /// Check if any PD endpoint is reachable and healthy.
    pub fn is_healthy(&self) -> bool {
        self.get("/pd/api/v1/health").is_ok()
    }

    /// Get the health status of all PD members.
    pub fn get_health(&self) -> Result<Vec<HealthEntry>, NaukaError> {
        let val = self.get("/pd/api/v1/health")?;
        let arr = val
            .as_array()
            .ok_or_else(|| NaukaError::internal("PD health: expected array"))?;

        Ok(arr
            .iter()
            .map(|m| HealthEntry {
                member_id: m["member_id"].as_u64().unwrap_or(0),
                name: m["name"].as_str().unwrap_or("").to_string(),
                healthy: m["health"].as_bool().unwrap_or(false),
            })
            .collect())
    }

    // ───────────────────────────────────────────────
    // Members
    // ───────────────────────────────────────────────

    /// Get all PD members with health and leader status merged in.
    pub fn get_members(&self) -> Result<Vec<PdMember>, NaukaError> {
        let val = self.get("/pd/api/v1/members")?;

        let leader_id = val["leader"]["member_id"].as_u64().unwrap_or(0);

        let members = val["members"]
            .as_array()
            .ok_or_else(|| NaukaError::internal("PD members: expected array"))?;

        // Best-effort: merge health info
        let health = self.get_health().unwrap_or_default();
        let healthy_ids: std::collections::HashSet<u64> = health
            .iter()
            .filter(|h| h.healthy)
            .map(|h| h.member_id)
            .collect();

        Ok(members
            .iter()
            .map(|m| {
                let mid = m["member_id"].as_u64().unwrap_or(0);
                PdMember {
                    member_id: mid,
                    name: m["name"].as_str().unwrap_or("").to_string(),
                    peer_urls: m["peer_urls"]
                        .as_array()
                        .map(|a| {
                            a.iter()
                                .filter_map(|v| v.as_str().map(String::from))
                                .collect()
                        })
                        .unwrap_or_default(),
                    client_urls: m["client_urls"]
                        .as_array()
                        .map(|a| {
                            a.iter()
                                .filter_map(|v| v.as_str().map(String::from))
                                .collect()
                        })
                        .unwrap_or_default(),
                    is_leader: mid == leader_id,
                    is_healthy: healthy_ids.contains(&mid),
                }
            })
            .collect())
    }

    /// Check if a PD member with the given mesh IPv6 exists.
    pub fn member_exists(&self, mesh_ipv6: &Ipv6Addr) -> bool {
        let our_ip = mesh_ipv6.to_string();
        self.get_members()
            .map(|members| {
                members
                    .iter()
                    .any(|m| m.peer_urls.iter().any(|url| url.contains(&our_ip)))
            })
            .unwrap_or(false)
    }

    /// Find the member ID for a given mesh IPv6.
    pub fn find_member_id(&self, mesh_ipv6: &Ipv6Addr) -> Option<u64> {
        let our_ip = mesh_ipv6.to_string();
        self.get_members().ok()?.into_iter().find_map(|m| {
            if m.member_id > 0 && m.peer_urls.iter().any(|url| url.contains(&our_ip)) {
                Some(m.member_id)
            } else {
                None
            }
        })
    }

    /// Delete a PD member by ID.
    pub fn delete_member_by_id(&self, member_id: u64) -> Result<(), NaukaError> {
        self.delete(&format!("/pd/api/v1/members/id/{member_id}"))
    }

    /// Delete a PD member by name.
    pub fn delete_member_by_name(&self, name: &str) -> Result<(), NaukaError> {
        self.delete(&format!("/pd/api/v1/members/name/{name}"))
    }

    /// Add a new PD member with the given peer URLs.
    pub fn add_member(&self, peer_urls: &[String]) -> Result<(), NaukaError> {
        let body = serde_json::json!({ "peerURLs": peer_urls }).to_string();
        self.post("/pd/api/v1/members", &body)?;
        Ok(())
    }

    // ───────────────────────────────────────────────
    // Stores
    // ───────────────────────────────────────────────

    /// Get all TiKV stores (default: Up state only).
    pub fn get_stores(&self) -> Result<Vec<TiKVStore>, NaukaError> {
        self.get_stores_with_path("/pd/api/v1/stores")
    }

    /// Get TiKV stores in specific states (0=Up, 1=Disconnected, 2=Offline).
    pub fn get_stores_with_states(&self, states: &[u32]) -> Result<Vec<TiKVStore>, NaukaError> {
        let params: Vec<String> = states.iter().map(|s| format!("state={s}")).collect();
        let path = format!("/pd/api/v1/stores?{}", params.join("&"));
        self.get_stores_with_path(&path)
    }

    fn get_stores_with_path(&self, path: &str) -> Result<Vec<TiKVStore>, NaukaError> {
        let val = self.get(path)?;
        let stores = val["stores"].as_array().cloned().unwrap_or_default();

        Ok(stores
            .iter()
            .map(|s| TiKVStore {
                id: s["store"]["id"].as_u64().unwrap_or(0),
                address: s["store"]["address"].as_str().unwrap_or("").to_string(),
                state_name: s["store"]["state_name"].as_str().unwrap_or("").to_string(),
                capacity: s["status"]["capacity"].as_str().unwrap_or("").to_string(),
            })
            .collect())
    }

    /// Count TiKV stores in Up state.
    pub fn count_active_stores(&self) -> usize {
        self.get_stores()
            .map(|stores| stores.iter().filter(|s| s.state_name == "Up").count())
            .unwrap_or(0)
    }

    /// Find the store ID for a given TiKV address (e.g., `[fd01::1]:20160`).
    ///
    /// Searches stores in Up, Disconnected, and Offline states.
    pub fn find_store_id(&self, tikv_addr: &str) -> Option<u64> {
        self.get_stores_with_states(&[0, 1, 2])
            .ok()?
            .into_iter()
            .find(|s| s.address == tikv_addr && s.id > 0)
            .map(|s| s.id)
    }

    /// Delete (offline) a TiKV store by ID.
    pub fn delete_store(&self, store_id: u64) -> Result<(), NaukaError> {
        self.delete(&format!("/pd/api/v1/store/{store_id}"))
    }

    /// Force-delete a TiKV store (for recovery).
    pub fn force_delete_store(&self, store_id: u64) -> Result<(), NaukaError> {
        self.delete(&format!("/pd/api/v1/store/{store_id}?force=true"))
    }

    /// Purge tombstoned stores.
    pub fn remove_tombstone(&self) -> Result<(), NaukaError> {
        self.delete("/pd/api/v1/stores/remove-tombstone")
    }

    // ───────────────────────────────────────────────
    // Regions
    // ───────────────────────────────────────────────

    /// Get region statistics.
    pub fn get_region_stats(&self) -> Result<RegionStats, NaukaError> {
        let val = self.get("/pd/api/v1/stats/region")?;
        Ok(RegionStats {
            count: val["count"].as_u64().unwrap_or(0),
            empty_count: val["empty_count"].as_u64().unwrap_or(0),
            miss_peer: val["miss_peer_region_count"].as_u64().unwrap_or(0),
            extra_peer: val["extra_peer_region_count"].as_u64().unwrap_or(0),
        })
    }

    /// Get all regions.
    pub fn get_regions(&self) -> Result<serde_json::Value, NaukaError> {
        self.get("/pd/api/v1/regions")
    }

    /// Get regions for a specific store.
    pub fn get_store_regions(&self, store_id: u64) -> Result<serde_json::Value, NaukaError> {
        self.get(&format!("/pd/api/v1/regions/store/{store_id}"))
    }

    /// Count regions on a specific store. Returns 0 if store is gone.
    pub fn count_store_regions(&self, store_id: u64) -> usize {
        self.get_store_regions(store_id)
            .ok()
            .and_then(|v| v["regions"].as_array().map(|a| a.len()))
            .unwrap_or(0)
    }

    // ───────────────────────────────────────────────
    // Replication config
    // ───────────────────────────────────────────────

    /// Get current max-replicas setting.
    pub fn get_max_replicas(&self) -> Result<usize, NaukaError> {
        let val = self.get("/pd/api/v1/config/replicate")?;
        Ok(val["max-replicas"].as_u64().unwrap_or(3) as usize)
    }

    /// Set max-replicas if it differs from the current value.
    pub fn set_max_replicas(&self, target: usize) -> Result<(), NaukaError> {
        if target == 0 {
            return Ok(());
        }

        let current = self.get_max_replicas()?;
        if target != current {
            tracing::info!(current, target, "adjusting max-replicas");
            let payload = format!("{{\"max-replicas\": {target}}}");
            self.post("/pd/api/v1/config/replicate", &payload)?;
        }
        Ok(())
    }

    // ───────────────────────────────────────────────
    // Unsafe recovery
    // ───────────────────────────────────────────────

    /// Remove failed stores via PD's unsafe recovery API.
    pub fn unsafe_remove_failed_stores(&self, store_ids: &[u64]) -> Result<(), NaukaError> {
        let stores_json = store_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let body = format!("{{\"stores\": [{stores_json}]}}");
        self.post("/pd/api/v1/admin/unsafe/remove-failed-stores", &body)?;
        Ok(())
    }

    // ───────────────────────────────────────────────
    // Raw access (for callers that need the raw JSON)
    // ───────────────────────────────────────────────

    /// Raw GET request returning parsed JSON.
    pub fn api_get(&self, path: &str) -> Result<serde_json::Value, NaukaError> {
        self.get(path)
    }

    // ═══════════════════════════════════════════════
    // Internal HTTP with retry + failover
    // ═══════════════════════════════════════════════

    fn get(&self, path: &str) -> Result<serde_json::Value, NaukaError> {
        let body = self.http("GET", path, None)?;
        serde_json::from_str(&body)
            .map_err(|e| NaukaError::internal(format!("PD API parse error: {e}")))
    }

    fn post(&self, path: &str, body: &str) -> Result<serde_json::Value, NaukaError> {
        let resp = self.http("POST", path, Some(body))?;
        // Some POST endpoints return empty body on success
        if resp.trim().is_empty() {
            Ok(serde_json::Value::Null)
        } else {
            serde_json::from_str(&resp)
                .map_err(|e| NaukaError::internal(format!("PD API parse error: {e}")))
        }
    }

    fn delete(&self, path: &str) -> Result<(), NaukaError> {
        self.http("DELETE", path, None)?;
        Ok(())
    }

    /// Execute an HTTP request against PD with retry and endpoint failover.
    ///
    /// - Tries each endpoint in order
    /// - Retries up to 3 times per endpoint with 1s/2s/4s backoff
    /// - Returns the response body on success
    fn http(&self, method: &str, path: &str, body: Option<&str>) -> Result<String, NaukaError> {
        let timeout = self.timeout_secs.to_string();
        let backoffs = [1, 2, 4];
        let mut last_err = String::from("no PD endpoints configured");

        for endpoint in &self.endpoints {
            let url = format!("{endpoint}{path}");

            for (attempt, &delay) in backoffs.iter().enumerate() {
                let mut args: Vec<&str> = vec!["-s", "--fail-with-body", "--max-time", &timeout];

                if method != "GET" {
                    args.extend_from_slice(&["-X", method]);
                }

                if let Some(b) = body {
                    args.extend_from_slice(&["-H", "Content-Type: application/json", "-d", b]);
                }

                args.push(&url);

                let result = Command::new("curl").args(&args).output();

                match result {
                    Ok(output) => {
                        let stdout = String::from_utf8_lossy(&output.stdout).to_string();

                        if output.status.success() {
                            return Ok(stdout);
                        }

                        // curl exit code: non-zero means HTTP or connection error.
                        // Check if this is a retriable connection error (exit 7, 28, 56)
                        // or a non-retriable HTTP error (exit 22 = HTTP >= 400).
                        let exit_code = output.status.code().unwrap_or(1);
                        if exit_code == 22 {
                            // HTTP error (4xx/5xx) — don't retry, return error
                            let detail = if stdout.is_empty() {
                                String::from_utf8_lossy(&output.stderr).to_string()
                            } else {
                                stdout
                            };
                            return Err(NaukaError::internal(format!(
                                "PD API error: {method} {path}: {detail}"
                            )));
                        }

                        // Connection error — retry
                        last_err = format!(
                            "{method} {url}: curl exit {exit_code} (attempt {}/{})",
                            attempt + 1,
                            backoffs.len()
                        );
                    }
                    Err(e) => {
                        last_err = format!("{method} {url}: {e}");
                    }
                }

                if attempt + 1 < backoffs.len() {
                    std::thread::sleep(std::time::Duration::from_secs(delay));
                }
            }
        }

        Err(NaukaError::internal(format!(
            "PD API unreachable after retries: {last_err}"
        )))
    }

    /// Quick connectivity check — single attempt, short timeout.
    pub fn ping(&self) -> bool {
        for endpoint in &self.endpoints {
            let url = format!("{endpoint}/pd/api/v1/health");
            let ok = Command::new("curl")
                .args(["-sf", "--max-time", "3", &url])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .map(|s| s.success())
                .unwrap_or(false);
            if ok {
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_mesh_builds_correct_endpoint() {
        let ip: Ipv6Addr = "fd01::1".parse().unwrap();
        let client = PdClient::from_mesh(&ip);
        assert_eq!(client.endpoints.len(), 1);
        assert!(client.endpoints[0].contains("fd01::1"));
        assert!(client.endpoints[0].contains(&super::super::PD_CLIENT_PORT.to_string()));
    }

    #[test]
    fn new_with_multiple_endpoints() {
        let client = PdClient::new(vec![
            "http://[fd01::1]:2379".into(),
            "http://[fd01::2]:2379".into(),
        ]);
        assert_eq!(client.endpoints.len(), 2);
    }

    #[test]
    fn with_timeout_overrides_default() {
        let client = PdClient::from_mesh(&"fd01::1".parse().unwrap()).with_timeout(30);
        assert_eq!(client.timeout_secs, 30);
    }
}
