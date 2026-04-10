//! Controlplane backup — tar.gz snapshots of PD/TiKV data to S3.
//!
//! Creates compressed archives of PD and TiKV data directories
//! and uploads them to the configured S3 bucket with timestamped keys.

use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use nauka_core::error::NaukaError;
use serde::{Deserialize, Serialize};

use crate::storage::region::RegionStorage;

const PD_DATA_DIR: &str = "/var/lib/nauka/pd";
const TIKV_DATA_DIR: &str = "/var/lib/nauka/tikv";
const BACKUP_TMP_DIR: &str = "/tmp/nauka-backup";

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
fn create_archive(data_dir: &str, archive_path: &str) -> Result<(), NaukaError> {
    std::fs::create_dir_all(BACKUP_TMP_DIR)
        .map_err(|e| NaukaError::internal(format!("failed to create tmp dir: {e}")))?;

    if !std::path::Path::new(data_dir).exists() {
        return Err(NaukaError::internal(format!(
            "data directory does not exist: {data_dir}"
        )));
    }

    let output = Command::new("tar")
        .args([
            "czf",
            archive_path,
            "--warning=no-file-changed",
            "-C",
            data_dir,
            ".",
        ])
        .output()
        .map_err(|e| NaukaError::internal(format!("tar failed: {e}")))?;

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
    let output = Command::new("curl")
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
            &format!("{}:{}", config.s3_access_key, config.s3_secret_key),
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

    let output = Command::new("curl")
        .args([
            "-sf",
            "--max-time",
            "30",
            "--aws-sigv4",
            &format!("aws:amz:{region}:s3"),
            "-u",
            &format!("{}:{}", config.s3_access_key, config.s3_secret_key),
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

    let output = Command::new("curl")
        .args([
            "-sf",
            "--max-time",
            "30",
            "-X",
            "DELETE",
            "--aws-sigv4",
            &format!("aws:amz:{region}:s3"),
            "-u",
            &format!("{}:{}", config.s3_access_key, config.s3_secret_key),
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

/// Create a snapshot of the PD data directory and upload to S3.
///
/// Returns the S3 key of the uploaded backup.
pub fn backup_pd(config: &RegionStorage) -> Result<String, NaukaError> {
    let ts = timestamp_label();
    let archive_name = format!("pd-snapshot-{ts}.tar.gz");
    let archive_path = format!("{BACKUP_TMP_DIR}/{archive_name}");
    let s3_key = format!("backups/pd/{archive_name}");

    tracing::info!("creating PD backup: {s3_key}");

    create_archive(PD_DATA_DIR, &archive_path)?;
    s3_upload(config, &archive_path, &s3_key)?;

    // Clean up temp file
    let _ = std::fs::remove_file(&archive_path);

    tracing::info!("PD backup uploaded: {s3_key}");
    Ok(s3_key)
}

/// Create a snapshot of the TiKV data directory and upload to S3.
///
/// Returns the S3 key of the uploaded backup.
pub fn backup_tikv(config: &RegionStorage) -> Result<String, NaukaError> {
    let ts = timestamp_label();
    let archive_name = format!("tikv-snapshot-{ts}.tar.gz");
    let archive_path = format!("{BACKUP_TMP_DIR}/{archive_name}");
    let s3_key = format!("backups/tikv/{archive_name}");

    tracing::info!("creating TiKV backup: {s3_key}");

    create_archive(TIKV_DATA_DIR, &archive_path)?;
    s3_upload(config, &archive_path, &s3_key)?;

    // Clean up temp file
    let _ = std::fs::remove_file(&archive_path);

    tracing::info!("TiKV backup uploaded: {s3_key}");
    Ok(s3_key)
}

/// List available backups from S3.
///
/// Parses the S3 ListObjectsV2 XML response to extract backup info.
pub fn list_backups(config: &RegionStorage) -> Result<Vec<BackupInfo>, NaukaError> {
    let xml = s3_list(config, "backups/")?;
    parse_s3_list_xml(&xml)
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
    <Key>backups/pd/pd-snapshot-2026-04-10T190000Z.tar.gz</Key>
    <Size>1048576</Size>
    <LastModified>2026-04-10T19:00:00.000Z</LastModified>
  </Contents>
  <Contents>
    <Key>backups/tikv/tikv-snapshot-2026-04-10T190000Z.tar.gz</Key>
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
