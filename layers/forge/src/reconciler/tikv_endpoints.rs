//! TiKV PD-endpoint reconciler — keeps tikv.toml in sync with fabric state.
//!
//! PD endpoints are written into tikv.toml at install time. If PD members
//! change (scale, leave, rejoin), TiKV cannot discover new endpoints without
//! a config update and restart. This reconciler detects drift and fixes it.
//!
//! Runs as a pre-flight step (before TiKV connect) because stale endpoints
//! can prevent TiKV from reaching PD at all.

use std::net::Ipv6Addr;
use std::path::Path;
use std::process::Command;

use nauka_hypervisor::controlplane;
use nauka_hypervisor::controlplane::service::{generate_tikv_conf, TikvConfig, TIKV_CONF_PATH};

/// Check if TiKV's PD endpoints match the current fabric state.
/// If they differ, rewrite tikv.toml and restart TiKV.
///
/// Returns `true` if the config was updated (TiKV was restarted).
pub fn sync_pd_endpoints(mesh_ipv6: &Ipv6Addr, peer_ipv6s: &[Ipv6Addr]) -> bool {
    // Only run if TiKV is installed (tikv.toml exists)
    if !Path::new(TIKV_CONF_PATH).exists() {
        return false;
    }

    // Build expected PD endpoints from fabric state (same logic as connect())
    let self_endpoint = format!("http://[{}]:{}", mesh_ipv6, controlplane::PD_CLIENT_PORT,);
    let mut expected_endpoints = vec![self_endpoint];
    for ip in peer_ipv6s {
        expected_endpoints.push(format!("http://[{}]:{}", ip, controlplane::PD_CLIENT_PORT,));
    }
    expected_endpoints.sort();

    // Read current tikv.toml and extract pd.endpoints
    let current_conf = match std::fs::read_to_string(TIKV_CONF_PATH) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "failed to read tikv.toml");
            return false;
        }
    };
    let mut current_endpoints = parse_pd_endpoints(&current_conf);
    current_endpoints.sort();

    // If endpoints match, nothing to do
    if current_endpoints == expected_endpoints {
        return false;
    }

    // Endpoints differ — rewrite config and restart TiKV
    tracing::warn!(
        current = ?current_endpoints,
        expected = ?expected_endpoints,
        "PD endpoints in tikv.toml differ from fabric state — updating config"
    );

    let tikv_cfg = TikvConfig {
        mesh_ipv6: *mesh_ipv6,
        pd_endpoints: expected_endpoints,
    };

    if let Err(e) = std::fs::write(TIKV_CONF_PATH, generate_tikv_conf(&tikv_cfg)) {
        tracing::error!(error = %e, "failed to write tikv.toml");
        return false;
    }

    // Restart TiKV to pick up new endpoints
    match Command::new("systemctl")
        .args(["restart", "nauka-tikv"])
        .output()
    {
        Ok(o) if o.status.success() => {
            tracing::warn!("TiKV restarted with updated PD endpoints");
            true
        }
        Ok(o) => {
            let stderr = String::from_utf8_lossy(&o.stderr);
            tracing::error!(error = %stderr, "systemctl restart nauka-tikv failed");
            false
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to restart TiKV");
            false
        }
    }
}

/// Parse the `pd.endpoints` list from tikv.toml content.
///
/// Looks for lines like: `endpoints = ["http://[::1]:2379", "http://[::2]:2379"]`
fn parse_pd_endpoints(conf: &str) -> Vec<String> {
    for line in conf.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("endpoints") {
            // Extract the part after '='
            if let Some(rhs) = trimmed.split_once('=') {
                let value = rhs.1.trim();
                // Strip surrounding brackets
                let inner = value.trim_start_matches('[').trim_end_matches(']');
                return inner
                    .split(',')
                    .map(|s| s.trim().trim_matches('"').to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
            }
        }
    }
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_empty_config() {
        assert!(parse_pd_endpoints("").is_empty());
    }

    #[test]
    fn parse_single_endpoint() {
        let conf = r#"
[pd]
endpoints = ["http://[fd00::1]:2379"]
"#;
        assert_eq!(
            parse_pd_endpoints(conf),
            vec!["http://[fd00::1]:2379".to_string()]
        );
    }

    #[test]
    fn parse_multiple_endpoints() {
        let conf = r#"
[pd]
endpoints = ["http://[fd00::1]:2379", "http://[fd00::2]:2379", "http://[fd00::3]:2379"]
"#;
        let eps = parse_pd_endpoints(conf);
        assert_eq!(eps.len(), 3);
        assert_eq!(eps[0], "http://[fd00::1]:2379");
        assert_eq!(eps[1], "http://[fd00::2]:2379");
        assert_eq!(eps[2], "http://[fd00::3]:2379");
    }

    #[test]
    fn parse_no_pd_section() {
        let conf = r#"
[server]
addr = "[::1]:20160"
"#;
        assert!(parse_pd_endpoints(conf).is_empty());
    }
}
