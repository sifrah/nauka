//! Controlplane backup — tar.gz snapshots of PD/TiKV data to S3.
//!
//! Two modes:
//! - **Cold** (default): stop the service, tar quiescent data, restart.
//!   Guarantees a consistent snapshot at the cost of brief downtime.
//! - **Hot** (`--hot`): tar while the service is running.
//!   The archive may contain torn writes but avoids any downtime.

use std::process::Command;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use nauka_core::error::NaukaError;
use serde::{Deserialize, Serialize};

use crate::storage::region::RegionStorage;

const PD_DATA_DIR: &str = "/var/lib/nauka/pd";
const TIKV_DATA_DIR: &str = "/var/lib/nauka/tikv";
const BACKUP_TMP_DIR: &str = "/tmp/nauka-backup";
const TIKV_MASTER_KEY_PATH: &str = "/etc/nauka/tikv-master-key";

const PD_SERVICE: &str = "nauka-pd";
const TIKV_SERVICE: &str = "nauka-tikv";

// ═══════════════════════════════════════════════════
// systemctl helpers
// ═══════════════════════════════════════════════════

fn run_systemctl(args: &[&str]) -> Result<(), NaukaError> {
    let output = Command::new("systemctl")
        .args(args)
        .output()
        .map_err(|e| NaukaError::internal(format!("systemctl failed: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(NaukaError::internal(format!(
            "systemctl {} failed: {}",
            args.join(" "),
            stderr.trim()
        )));
    }

    Ok(())
}

/// Drop guard that restarts a systemd service when dropped.
///
/// Guarantees the service comes back up even if archiving or upload panics/fails.
struct ServiceGuard {
    service: &'static str,
    stopped_at: Instant,
}

impl ServiceGuard {
    /// Stop `service` and return a guard that will restart it on drop.
    fn stop(service: &'static str) -> Result<Self, NaukaError> {
        tracing::info!("stopping {service} for consistent backup");
        run_systemctl(&["stop", service])?;
        Ok(Self {
            service,
            stopped_at: Instant::now(),
        })
    }

    /// Explicitly restart the service and log the downtime.
    /// Returns the downtime duration.
    fn restart(self) -> Result<std::time::Duration, NaukaError> {
        let dt = self.stopped_at.elapsed();
        tracing::info!(
            "restarting {} (downtime: {:.1}s)",
            self.service,
            dt.as_secs_f64()
        );
        run_systemctl(&["start", self.service])?;
        // Prevent the Drop impl from running a second start.
        std::mem::forget(self);
        Ok(dt)
    }
}

impl Drop for ServiceGuard {
    fn drop(&mut self) {
        let dt = self.stopped_at.elapsed();
        tracing::warn!(
            "ServiceGuard dropped — force-restarting {} (downtime: {:.1}s)",
            self.service,
            dt.as_secs_f64()
        );
        let _ = Command::new("systemctl")
            .args(["start", self.service])
            .status();
    }
}

/// Info about a backup stored in S3.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupInfo {
    pub key: String,
    pub size: u64,
    pub last_modified: String,
}

/// Generate an ISO-8601-ish timestamp for backup naming.
fn timestamp_label() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Use a filename-safe format without colons (colons break S3 signature v4).
    // Format: 2026-04-10T215830Z
    let output = Command::new("date")
        .args(["-u", "-d", &format!("@{secs}"), "+%Y-%m-%dT%H%M%SZ"])
        .output();

    match output {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).trim().to_string(),
        _ => {
            // Fallback (e.g. macOS date doesn't support -d @epoch)
            let output = Command::new("date")
                .args(["-u", "+%Y-%m-%dT%H%M%SZ"])
                .output();
            match output {
                Ok(o) if o.status.success() => {
                    String::from_utf8_lossy(&o.stdout).trim().to_string()
                }
                _ => format!("{secs}"),
            }
        }
    }
}

/// Create a tar.gz archive of a directory.
///
/// When `hot` is true, tolerate tar exit-code 1 ("files changed during read")
/// and suppress the corresponding warning. When false (cold backup), the
/// service is already stopped so any failure is a real error.
fn create_archive(data_dir: &str, archive_path: &str, hot: bool) -> Result<(), NaukaError> {
    std::fs::create_dir_all(BACKUP_TMP_DIR)
        .map_err(|e| NaukaError::internal(format!("failed to create tmp dir: {e}")))?;

    if !std::path::Path::new(data_dir).exists() {
        return Err(NaukaError::internal(format!(
            "data directory does not exist: {data_dir}"
        )));
    }

    let mut args = vec!["czf", archive_path];
    if hot {
        args.push("--warning=no-file-changed");
    }
    args.extend_from_slice(&["-C", data_dir, "."]);

    let output = Command::new("tar")
        .args(&args)
        .output()
        .map_err(|e| NaukaError::internal(format!("tar failed: {e}")))?;

    if hot {
        // tar exit code 1 means "some files changed during archiving" which is
        // expected for live PD/TiKV data. Only fail on exit code 2+ (real errors).
        if let Some(code) = output.status.code() {
            if code >= 2 {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(NaukaError::internal(format!("tar failed: {stderr}")));
            }
        } else if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(NaukaError::internal(format!("tar failed: {stderr}")));
        }
    } else if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(NaukaError::internal(format!("tar failed: {stderr}")));
    }

    Ok(())
}

/// Encrypt a tar.gz archive with AES-256-CBC using the TiKV master key.
///
/// Produces `{archive_path}.enc` and removes the unencrypted original.
fn encrypt_archive(archive_path: &str) -> Result<String, NaukaError> {
    if !std::path::Path::new(TIKV_MASTER_KEY_PATH).exists() {
        return Err(NaukaError::internal(format!(
            "master key not found at {TIKV_MASTER_KEY_PATH} — cannot encrypt backup"
        )));
    }

    let enc_path = format!("{archive_path}.enc");

    let output = Command::new("openssl")
        .args([
            "enc",
            "-aes-256-cbc",
            "-salt",
            "-pbkdf2",
            "-in",
            archive_path,
            "-out",
            &enc_path,
            "-pass",
            &format!("file:{TIKV_MASTER_KEY_PATH}"),
        ])
        .output()
        .map_err(|e| NaukaError::internal(format!("openssl encrypt failed: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(NaukaError::internal(format!(
            "openssl encrypt failed: {stderr}"
        )));
    }

    // Remove unencrypted archive
    let _ = std::fs::remove_file(archive_path);

    Ok(enc_path)
}

/// Decrypt an AES-256-CBC encrypted archive using the TiKV master key.
///
/// Produces the decrypted file at `output_path`.
fn decrypt_archive(enc_path: &str, output_path: &str) -> Result<(), NaukaError> {
    if !std::path::Path::new(TIKV_MASTER_KEY_PATH).exists() {
        return Err(NaukaError::internal(format!(
            "master key not found at {TIKV_MASTER_KEY_PATH} — cannot decrypt backup"
        )));
    }

    let output = Command::new("openssl")
        .args([
            "enc",
            "-d",
            "-aes-256-cbc",
            "-pbkdf2",
            "-in",
            enc_path,
            "-out",
            output_path,
            "-pass",
            &format!("file:{TIKV_MASTER_KEY_PATH}"),
        ])
        .output()
        .map_err(|e| NaukaError::internal(format!("openssl decrypt failed: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(NaukaError::internal(format!(
            "openssl decrypt failed: {stderr}"
        )));
    }

    Ok(())
}

/// Upload a file to S3 using curl with AWS Signature V4.
fn s3_upload(config: &RegionStorage, local_path: &str, s3_key: &str) -> Result<(), NaukaError> {
    let bucket = &config.s3_bucket;
    let endpoint = config.s3_endpoint.trim_end_matches('/');

    // Use the s3 region if set, otherwise extract from endpoint
    let region = if config.s3_region.is_empty() {
        "us-east-1"
    } else {
        &config.s3_region
    };

    // Build the S3 URL
    let url = format!("{endpoint}/{bucket}/{s3_key}");

    // Use curl with aws-sigv4 for authenticated upload.
    // -s suppresses progress, --fail-with-body returns non-zero on HTTP errors
    // while still capturing the response body for diagnostics.
    // UNSIGNED-PAYLOAD avoids content-hash mismatch on large files.
    // Credentials are passed via env vars so they don't appear in `ps aux`.
    let output = Command::new("curl")
        .env("AWS_ACCESS_KEY_ID", &config.s3_access_key)
        .env("AWS_SECRET_ACCESS_KEY", &config.s3_secret_key)
        .args([
            "-s",
            "--fail-with-body",
            "--max-time",
            "600",
            "-X",
            "PUT",
            "-T",
            local_path,
            "-H",
            "x-amz-content-sha256: UNSIGNED-PAYLOAD",
            "--aws-sigv4",
            &format!("aws:amz:{region}:s3"),
            "-u",
            ":",
            &url,
        ])
        .output()
        .map_err(|e| NaukaError::internal(format!("curl upload failed: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(NaukaError::internal(format!(
            "S3 upload failed ({}): {} {}",
            url,
            stderr.trim(),
            stdout.trim()
        )));
    }

    Ok(())
}

/// List objects in S3 under a given prefix using curl with AWS Signature V4.
fn s3_list(config: &RegionStorage, prefix: &str) -> Result<String, NaukaError> {
    let bucket = &config.s3_bucket;
    let endpoint = config.s3_endpoint.trim_end_matches('/');

    let region = if config.s3_region.is_empty() {
        "us-east-1"
    } else {
        &config.s3_region
    };

    let url = format!("{endpoint}/{bucket}?prefix={prefix}&list-type=2");

    // Credentials are passed via env vars so they don't appear in `ps aux`.
    let output = Command::new("curl")
        .env("AWS_ACCESS_KEY_ID", &config.s3_access_key)
        .env("AWS_SECRET_ACCESS_KEY", &config.s3_secret_key)
        .args([
            "-sf",
            "--max-time",
            "30",
            "--aws-sigv4",
            &format!("aws:amz:{region}:s3"),
            "-u",
            ":",
            &url,
        ])
        .output()
        .map_err(|e| NaukaError::internal(format!("S3 list failed: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(NaukaError::internal(format!(
            "S3 list failed: {}",
            stderr.trim()
        )));
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

/// Delete an object from S3 using curl with AWS Signature V4.
fn s3_delete(config: &RegionStorage, s3_key: &str) -> Result<(), NaukaError> {
    let bucket = &config.s3_bucket;
    let endpoint = config.s3_endpoint.trim_end_matches('/');

    let region = if config.s3_region.is_empty() {
        "us-east-1"
    } else {
        &config.s3_region
    };

    let url = format!("{endpoint}/{bucket}/{s3_key}");

    // Credentials are passed via env vars so they don't appear in `ps aux`.
    let output = Command::new("curl")
        .env("AWS_ACCESS_KEY_ID", &config.s3_access_key)
        .env("AWS_SECRET_ACCESS_KEY", &config.s3_secret_key)
        .args([
            "-sf",
            "--max-time",
            "30",
            "-X",
            "DELETE",
            "--aws-sigv4",
            &format!("aws:amz:{region}:s3"),
            "-u",
            ":",
            &url,
        ])
        .output()
        .map_err(|e| NaukaError::internal(format!("S3 delete failed: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(NaukaError::internal(format!(
            "S3 delete failed: {}",
            stderr.trim()
        )));
    }

    Ok(())
}

/// Download an object from S3 to a local file using curl with AWS Signature V4.
fn s3_download(config: &RegionStorage, s3_key: &str, local_path: &str) -> Result<(), NaukaError> {
    let bucket = &config.s3_bucket;
    let endpoint = config.s3_endpoint.trim_end_matches('/');

    let region = if config.s3_region.is_empty() {
        "us-east-1"
    } else {
        &config.s3_region
    };

    let url = format!("{endpoint}/{bucket}/{s3_key}");

    let output = Command::new("curl")
        .args([
            "-sf",
            "--max-time",
            "600",
            "-o",
            local_path,
            "--aws-sigv4",
            &format!("aws:amz:{region}:s3"),
            "-u",
            ":",
            &url,
        ])
        .env("AWS_ACCESS_KEY_ID", &config.s3_access_key)
        .env("AWS_SECRET_ACCESS_KEY", &config.s3_secret_key)
        .output()
        .map_err(|e| NaukaError::internal(format!("S3 download failed: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(NaukaError::internal(format!(
            "S3 download failed ({}): {}",
            url,
            stderr.trim()
        )));
    }

    Ok(())
}

/// Create a snapshot of the PD data directory, encrypt, and upload to S3.
///
/// When `hot` is false (default), PD is stopped before archiving and restarted
/// afterwards. The `ServiceGuard` guarantees restart even on error/panic.
///
/// Returns the S3 key of the uploaded backup.
pub fn backup_pd(config: &RegionStorage, hot: bool) -> Result<String, NaukaError> {
    let ts = timestamp_label();
    let archive_name = format!("pd-snapshot-{ts}.tar.gz");
    let archive_path = format!("{BACKUP_TMP_DIR}/{archive_name}");
    let s3_key = format!("backups/pd/{archive_name}.enc");

    let mode = if hot { "hot" } else { "cold" };
    tracing::info!("creating PD backup ({mode}): {s3_key}");

    // Stop PD for a consistent snapshot (guard restarts on drop).
    let guard = if hot {
        None
    } else {
        Some(ServiceGuard::stop(PD_SERVICE)?)
    };

    create_archive(PD_DATA_DIR, &archive_path, hot)?;

    // Restart PD immediately — upload can happen while it recovers.
    if let Some(g) = guard {
        let dt = g.restart()?;
        tracing::info!("PD downtime for backup: {:.1}s", dt.as_secs_f64());
    }

    let enc_path = encrypt_archive(&archive_path)?;
    s3_upload(config, &enc_path, &s3_key)?;

    // Clean up encrypted temp file
    let _ = std::fs::remove_file(&enc_path);

    tracing::info!("PD backup uploaded: {s3_key}");
    Ok(s3_key)
}

/// Create a snapshot of the TiKV data directory, encrypt, and upload to S3.
///
/// When `hot` is false (default), TiKV is stopped before archiving and
/// restarted afterwards. The `ServiceGuard` guarantees restart even on
/// error/panic.
///
/// Returns the S3 key of the uploaded backup.
pub fn backup_tikv(config: &RegionStorage, hot: bool) -> Result<String, NaukaError> {
    let ts = timestamp_label();
    let archive_name = format!("tikv-snapshot-{ts}.tar.gz");
    let archive_path = format!("{BACKUP_TMP_DIR}/{archive_name}");
    let s3_key = format!("backups/tikv/{archive_name}.enc");

    let mode = if hot { "hot" } else { "cold" };
    tracing::info!("creating TiKV backup ({mode}): {s3_key}");

    // Stop TiKV for a consistent snapshot (guard restarts on drop).
    let guard = if hot {
        None
    } else {
        Some(ServiceGuard::stop(TIKV_SERVICE)?)
    };

    create_archive(TIKV_DATA_DIR, &archive_path, hot)?;

    // Restart TiKV immediately — upload can happen while it recovers.
    if let Some(g) = guard {
        let dt = g.restart()?;
        tracing::info!("TiKV downtime for backup: {:.1}s", dt.as_secs_f64());
    }

    let enc_path = encrypt_archive(&archive_path)?;
    s3_upload(config, &enc_path, &s3_key)?;

    // Clean up encrypted temp file
    let _ = std::fs::remove_file(&enc_path);

    tracing::info!("TiKV backup uploaded: {s3_key}");
    Ok(s3_key)
}

/// List available backups from S3.
///
/// Parses the S3 ListObjectsV2 XML response to extract backup info.
/// Handles both legacy `.tar.gz` and encrypted `.tar.gz.enc` keys.
pub fn list_backups(config: &RegionStorage) -> Result<Vec<BackupInfo>, NaukaError> {
    let xml = s3_list(config, "backups/")?;
    parse_s3_list_xml(&xml)
}

/// Returns `true` if the backup key is an encrypted archive.
pub fn is_encrypted_backup(key: &str) -> bool {
    key.ends_with(".tar.gz.enc")
}

/// Parse S3 ListObjectsV2 XML response into BackupInfo entries.
fn parse_s3_list_xml(xml: &str) -> Result<Vec<BackupInfo>, NaukaError> {
    let mut backups = Vec::new();

    // Simple XML parsing — extract <Key>, <Size>, <LastModified> from each <Contents>
    for contents in xml.split("<Contents>").skip(1) {
        let key = extract_xml_tag(contents, "Key").unwrap_or_default();
        let size: u64 = extract_xml_tag(contents, "Size")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let last_modified = extract_xml_tag(contents, "LastModified").unwrap_or_default();

        if !key.is_empty() {
            backups.push(BackupInfo {
                key,
                size,
                last_modified,
            });
        }
    }

    // Sort by key (which includes timestamp) descending
    backups.sort_by(|a, b| b.key.cmp(&a.key));

    Ok(backups)
}

/// Extract the text content of an XML tag (simple, no nested tags).
fn extract_xml_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml[start..end].to_string())
}

/// Delete backups older than `retention_days` days.
///
/// Returns the number of backups deleted.
pub fn cleanup_old_backups(config: &RegionStorage, retention_days: u32) -> Result<u32, NaukaError> {
    let backups = list_backups(config)?;
    let mut deleted = 0u32;

    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let retention_secs = u64::from(retention_days) * 86400;

    for backup in &backups {
        // Parse the timestamp from the key: backups/pd/pd-snapshot-2026-04-10T19:00:00Z.tar.gz
        // or from LastModified field
        if let Some(backup_ts) = parse_backup_timestamp(&backup.last_modified) {
            if now_secs.saturating_sub(backup_ts) > retention_secs
                && s3_delete(config, &backup.key).is_ok()
            {
                tracing::info!("deleted old backup: {}", backup.key);
                deleted += 1;
            }
        }
    }

    Ok(deleted)
}

/// Download and decrypt a backup from S3, then extract it to the target directory.
///
/// `s3_key` should be the full key (e.g. `backups/pd/pd-snapshot-...tar.gz.enc`).
/// `target_dir` is where the tar.gz contents will be extracted.
pub fn restore_backup(
    config: &RegionStorage,
    s3_key: &str,
    target_dir: &str,
) -> Result<(), NaukaError> {
    std::fs::create_dir_all(BACKUP_TMP_DIR)
        .map_err(|e| NaukaError::internal(format!("failed to create tmp dir: {e}")))?;

    let is_encrypted = s3_key.ends_with(".enc");

    // Determine local file names
    let local_enc = format!("{BACKUP_TMP_DIR}/restore-download.tar.gz.enc");
    let local_tar = format!("{BACKUP_TMP_DIR}/restore-download.tar.gz");

    let download_path = if is_encrypted { &local_enc } else { &local_tar };

    tracing::info!("downloading backup: {s3_key}");
    s3_download(config, s3_key, download_path)?;

    if is_encrypted {
        tracing::info!("decrypting backup");
        decrypt_archive(&local_enc, &local_tar)?;
        let _ = std::fs::remove_file(&local_enc);
    }

    // Extract
    std::fs::create_dir_all(target_dir)
        .map_err(|e| NaukaError::internal(format!("failed to create target dir: {e}")))?;

    let output = Command::new("tar")
        .args(["xzf", &local_tar, "-C", target_dir])
        .output()
        .map_err(|e| NaukaError::internal(format!("tar extract failed: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(NaukaError::internal(format!(
            "tar extract failed: {stderr}"
        )));
    }

    // Clean up
    let _ = std::fs::remove_file(&local_tar);

    tracing::info!("backup restored to {target_dir}");
    Ok(())
}

/// Parse an ISO-8601 timestamp string into Unix seconds.
/// Handles format: 2026-04-10T19:00:00.000Z or 2026-04-10T19:00:00Z
fn parse_backup_timestamp(ts: &str) -> Option<u64> {
    // Use `date` command to parse ISO-8601
    let output = Command::new("date")
        .args(["-u", "-d", ts, "+%s"])
        .output()
        .ok()?;

    if output.status.success() {
        let s = String::from_utf8_lossy(&output.stdout);
        return s.trim().parse().ok();
    }

    None
}

/// Format bytes into a human-readable string.
pub fn format_size(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{bytes} B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_xml_list() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Contents>
    <Key>backups/pd/pd-snapshot-2026-04-10T190000Z.tar.gz.enc</Key>
    <Size>1048576</Size>
    <LastModified>2026-04-10T19:00:00.000Z</LastModified>
  </Contents>
  <Contents>
    <Key>backups/tikv/tikv-snapshot-2026-04-10T190000Z.tar.gz.enc</Key>
    <Size>52428800</Size>
    <LastModified>2026-04-10T19:00:00.000Z</LastModified>
  </Contents>
</ListBucketResult>"#;

        let backups = parse_s3_list_xml(xml).unwrap();
        assert_eq!(backups.len(), 2);
        assert!(backups[0].key.contains("tikv")); // sorted descending
        assert_eq!(backups[1].size, 1048576);
    }

    #[test]
    fn parse_xml_list_mixed_extensions() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Contents>
    <Key>backups/pd/pd-snapshot-2026-04-09T120000Z.tar.gz</Key>
    <Size>500000</Size>
    <LastModified>2026-04-09T12:00:00.000Z</LastModified>
  </Contents>
  <Contents>
    <Key>backups/pd/pd-snapshot-2026-04-10T190000Z.tar.gz.enc</Key>
    <Size>1048576</Size>
    <LastModified>2026-04-10T19:00:00.000Z</LastModified>
  </Contents>
</ListBucketResult>"#;

        let backups = parse_s3_list_xml(xml).unwrap();
        assert_eq!(backups.len(), 2);
        // Encrypted one sorts after legacy (descending)
        assert!(is_encrypted_backup(&backups[0].key));
        assert!(!is_encrypted_backup(&backups[1].key));
    }

    #[test]
    fn extract_xml_tag_works() {
        assert_eq!(
            extract_xml_tag("<Key>foo/bar</Key>", "Key"),
            Some("foo/bar".to_string())
        );
        assert_eq!(extract_xml_tag("<Key>foo</Key>", "Missing"), None);
    }

    #[test]
    fn format_size_display() {
        assert_eq!(format_size(500), "500 B");
        assert_eq!(format_size(1536), "1.5 KB");
        assert_eq!(format_size(1048576), "1.0 MB");
        assert_eq!(format_size(1073741824), "1.00 GB");
    }

    #[test]
    fn timestamp_label_not_empty() {
        let ts = timestamp_label();
        assert!(!ts.is_empty());
    }
}
