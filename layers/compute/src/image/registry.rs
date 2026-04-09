//! Image registry — download images from GitHub Releases.
//!
//! Images are stored in the sifrah/nauka-images repo as GitHub Release assets.
//! Each image is a tar.gz of a rootfs directory.

use std::path::Path;
use std::process::Command;

const IMAGES_DIR: &str = "/opt/nauka/images";
const GITHUB_REPO: &str = "sifrah/nauka-images";

/// Pull an image from the GitHub registry.
///
/// Downloads the tar.gz from GitHub Releases and extracts to /opt/nauka/images/{name}/
pub fn pull(name: &str) -> anyhow::Result<u64> {
    let arch = std::env::consts::ARCH;
    let arch_name = match arch {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        _ => arch,
    };

    let image_dir = format!("{IMAGES_DIR}/{name}");

    // Check if already exists
    if Path::new(&image_dir).join("bin/sh").exists() {
        tracing::info!(name, "image already exists");
        let size = dir_size(&image_dir);
        return Ok(size);
    }

    // Download from GitHub Releases
    let asset_name = format!("{name}-{arch_name}.tar.gz");
    let url = format!("https://github.com/{GITHUB_REPO}/releases/download/latest/{asset_name}");

    tracing::info!(name, url = url.as_str(), "pulling image");

    let tmp_file = format!("/tmp/nauka-image-{name}.tar.gz");

    // Download with curl (follows redirects, GitHub Releases redirect to CDN)
    let status = Command::new("curl")
        .args(["-fsSL", "-o", &tmp_file, &url])
        .status()
        .map_err(|e| anyhow::anyhow!("download failed: {e}"))?;

    if !status.success() {
        anyhow::bail!("image '{name}' not found in registry ({})", url);
    }

    // Extract
    std::fs::create_dir_all(&image_dir)?;
    let status = Command::new("tar")
        .args(["-xzf", &tmp_file, "-C", &image_dir])
        .status()
        .map_err(|e| anyhow::anyhow!("extract failed: {e}"))?;

    if !status.success() {
        let _ = std::fs::remove_dir_all(&image_dir);
        let _ = std::fs::remove_file(&tmp_file);
        anyhow::bail!("failed to extract image '{name}'");
    }

    let _ = std::fs::remove_file(&tmp_file);

    let size = dir_size(&image_dir);
    tracing::info!(name, size_mb = size / 1024 / 1024, "image ready");

    Ok(size)
}

/// Delete an image from the local store.
pub fn delete(name: &str) -> anyhow::Result<()> {
    let image_dir = format!("{IMAGES_DIR}/{name}");
    if Path::new(&image_dir).exists() {
        std::fs::remove_dir_all(&image_dir)?;
        tracing::info!(name, "image deleted");
    }
    Ok(())
}

/// List locally available images.
pub fn list() -> Vec<(String, u64)> {
    let dir = match std::fs::read_dir(IMAGES_DIR) {
        Ok(d) => d,
        Err(_) => return vec![],
    };

    dir.filter_map(|entry| {
        let entry = entry.ok()?;
        let name = entry.file_name().to_string_lossy().to_string();
        let path = entry.path();
        if path.is_dir() && path.join("bin/sh").exists() {
            let size = dir_size(&path.to_string_lossy());
            Some((name, size))
        } else {
            None
        }
    })
    .collect()
}

/// Check if an image exists locally.
pub fn exists(name: &str) -> bool {
    Path::new(&format!("{IMAGES_DIR}/{name}/bin/sh")).exists()
}

/// Get total size of a directory in bytes.
fn dir_size(path: &str) -> u64 {
    Command::new("du")
        .args(["-sb", path])
        .output()
        .ok()
        .and_then(|o| {
            String::from_utf8_lossy(&o.stdout)
                .split_whitespace()
                .next()
                .and_then(|s| s.parse().ok())
        })
        .unwrap_or(0)
}
