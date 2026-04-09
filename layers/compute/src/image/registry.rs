//! Image registry — download images from GitHub Releases.
//!
//! Images are stored in the sifrah/nauka-images repo as GitHub Release assets.
//! Each image is a tar.gz of a rootfs directory.

use std::path::Path;
use std::process::Command;

use futures_util::StreamExt;
use nauka_core::ui;

const IMAGES_DIR: &str = "/opt/nauka/images";
const GITHUB_REPO: &str = "sifrah/nauka-images";

/// Pull an image from the GitHub registry.
///
/// Downloads the tar.gz from GitHub Releases and extracts to /opt/nauka/images/{name}/
pub async fn pull(name: &str) -> anyhow::Result<u64> {
    let arch = std::env::consts::ARCH;
    let arch_name = match arch {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        _ => arch,
    };

    let image_dir = format!("{IMAGES_DIR}/{name}");

    // Check if already exists
    if Path::new(&image_dir).join("bin/sh").exists() {
        let size = dir_size(&image_dir);
        return Ok(size);
    }

    // Determine image type from runtime (container if gVisor, vm if KVM)
    let image_type = if Path::new("/dev/kvm").exists() {
        "vm"
    } else {
        "container"
    };

    // Download from GitHub Releases
    let asset_name = format!("{name}-{image_type}-{arch_name}.tar.gz");
    let url = format!("https://github.com/{GITHUB_REPO}/releases/download/latest/{asset_name}");

    let tmp_file = format!("/tmp/nauka-image-{name}.tar.gz");

    // Stream download with progress bar
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()?;

    let resp = client.get(&url).send().await?;

    if !resp.status().is_success() {
        anyhow::bail!("image '{name}' not found in registry ({url})");
    }

    let total_size = resp.content_length().unwrap_or(0);
    let pb = ui::progress(&format!("Downloading {name}"), total_size);

    let mut stream = resp.bytes_stream();
    let mut file = tokio::fs::File::create(&tmp_file).await?;

    use tokio::io::AsyncWriteExt;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk).await?;
        pb.inc(chunk.len() as u64);
    }
    file.flush().await?;
    drop(file);

    ui::progress_finish(
        &pb,
        &format!("Downloaded {name} ({})", format_size(total_size)),
    );

    // Extract
    let sp = ui::spinner(&format!("Extracting {name}"));
    std::fs::create_dir_all(&image_dir)?;
    let status = Command::new("tar")
        .args(["-xzf", &tmp_file, "-C", &image_dir])
        .status()
        .map_err(|e| anyhow::anyhow!("extract failed: {e}"))?;

    if !status.success() {
        let _ = std::fs::remove_dir_all(&image_dir);
        let _ = std::fs::remove_file(&tmp_file);
        ui::finish_fail(&sp, &format!("Failed to extract {name}"));
        anyhow::bail!("failed to extract image '{name}'");
    }

    let _ = std::fs::remove_file(&tmp_file);
    let size = dir_size(&image_dir);
    ui::finish_ok(&sp, &format!("Extracted {name} ({})", format_size(size)));

    Ok(size)
}

/// Delete an image from the local store.
pub fn delete(name: &str) -> anyhow::Result<()> {
    let image_dir = format!("{IMAGES_DIR}/{name}");
    if Path::new(&image_dir).exists() {
        std::fs::remove_dir_all(&image_dir)?;
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

/// List images available in the remote registry (from manifest.json).
pub async fn catalog() -> anyhow::Result<Vec<CatalogEntry>> {
    let arch = std::env::consts::ARCH;
    let arch_name = match arch {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        _ => arch,
    };

    let url = format!("https://github.com/{GITHUB_REPO}/releases/download/latest/manifest.json");

    let client = reqwest::Client::builder()
        .user_agent("nauka")
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()?;

    let resp = client.get(&url).send().await?;
    if !resp.status().is_success() {
        anyhow::bail!("failed to fetch image catalog from registry");
    }

    let manifest: serde_json::Value = resp.json().await?;
    let images = manifest["images"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("invalid manifest format"))?;

    let mut entries = Vec::new();

    for image in images {
        let name = image["name"].as_str().unwrap_or_default();
        let image_type = image["type"].as_str().unwrap_or("container");
        let description = image["description"].as_str().unwrap_or_default();
        let logo = image["logo"].as_str().unwrap_or_default();
        let archs = image["arch"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        // Only show images available for this architecture
        if archs.iter().any(|a| a == arch_name) {
            let size = image["sizes"][arch_name].as_u64().unwrap_or(0);
            entries.push(CatalogEntry {
                name: name.to_string(),
                size,
                image_type: image_type.to_string(),
                arch: arch_name.to_string(),
                description: description.to_string(),
                logo: logo.to_string(),
                local: exists(name),
            });
        }
    }

    Ok(entries)
}

/// An image available in the remote registry.
pub struct CatalogEntry {
    pub name: String,
    pub size: u64,
    pub image_type: String,
    pub arch: String,
    pub description: String,
    pub logo: String,
    pub local: bool,
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

fn format_size(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.1} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.0} MB", bytes as f64 / 1_048_576.0)
    } else {
        format!("{} KB", bytes / 1024)
    }
}
