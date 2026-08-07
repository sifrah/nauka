//! Self-update: `nauka update` replaces the running binary with the latest
//! GitHub release.
//!
//! The flow mirrors what install.sh does, without depending on it: query
//! the latest release, pick the tarball for this platform, verify its
//! SHA256 against the published SHA256SUMS.txt, then swap the binary
//! atomically (write next to it, rename over). The running process keeps
//! its old inode — a restart picks up the new version.

use anyhow::{bail, Context, Result};
use sha2::Digest;

const REPO: &str = "sifrah/nauka";
const CURRENT: &str = env!("CARGO_PKG_VERSION");

/// Target triple of THIS build, matching the release asset names.
fn target_triple() -> Result<String> {
    let arch = std::env::consts::ARCH; // "x86_64" | "aarch64"
    match std::env::consts::OS {
        "linux" => Ok(format!("{arch}-unknown-linux-gnu")),
        "macos" => Ok(format!("{arch}-apple-darwin")),
        other => bail!("no published binaries for {other}"),
    }
}

/// "v0.2.0" → (0, 2, 0). Tolerates a missing "v".
fn parse_version(tag: &str) -> Option<(u64, u64, u64)> {
    let mut it = tag.trim().trim_start_matches('v').splitn(3, '.');
    Some((
        it.next()?.parse().ok()?,
        it.next()?.parse().ok()?,
        // "1-rc1" and friends: take the leading digits.
        it.next()?
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect::<String>()
            .parse()
            .ok()?,
    ))
}

#[derive(serde::Deserialize)]
struct Release {
    tag_name: String,
    assets: Vec<Asset>,
}

#[derive(serde::Deserialize)]
struct Asset {
    name: String,
    browser_download_url: String,
}

async fn latest_release(http: &reqwest::Client) -> Result<Release> {
    let url = format!("https://api.github.com/repos/{REPO}/releases/latest");
    let resp = http
        .get(&url)
        .header(reqwest::header::USER_AGENT, format!("nauka/{CURRENT}"))
        .send()
        .await
        .context("reaching the GitHub API")?;
    if !resp.status().is_success() {
        bail!("GitHub API answered {}", resp.status());
    }
    resp.json().await.context("parsing the release")
}

async fn fetch(http: &reqwest::Client, url: &str) -> Result<Vec<u8>> {
    let resp = http
        .get(url)
        .header(reqwest::header::USER_AGENT, format!("nauka/{CURRENT}"))
        .send()
        .await?;
    if !resp.status().is_success() {
        bail!("download failed: {} on {url}", resp.status());
    }
    Ok(resp.bytes().await?.to_vec())
}

/// The published SHA256 for `name`, from the release's SHA256SUMS.txt.
fn published_sha256(sums: &str, name: &str) -> Option<String> {
    sums.lines().find_map(|l| {
        let mut parts = l.split_whitespace();
        let hash = parts.next()?;
        let file = parts.next()?;
        (file.trim_start_matches('*') == name).then(|| hash.to_lowercase())
    })
}

/// Extracts the `nauka` binary from the tarball, in memory.
fn extract_binary(tarball: &[u8]) -> Result<Vec<u8>> {
    let gz = flate2::read::GzDecoder::new(tarball);
    let mut archive = tar::Archive::new(gz);
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?;
        if path.file_name().is_some_and(|n| n == "nauka") {
            let mut buf = Vec::new();
            std::io::Read::read_to_end(&mut entry, &mut buf)?;
            return Ok(buf);
        }
    }
    bail!("no `nauka` binary inside the release tarball");
}

pub async fn run(check_only: bool) -> Result<()> {
    let triple = target_triple()?;
    let http = reqwest::Client::new();

    let release = latest_release(&http).await?;
    let latest = parse_version(&release.tag_name)
        .with_context(|| format!("unreadable tag: {}", release.tag_name))?;
    let current = parse_version(CURRENT).context("unreadable built-in version")?;

    if latest <= current {
        println!(
            "nauka {CURRENT} — already the latest release ({})",
            release.tag_name
        );
        return Ok(());
    }
    println!("update available: {CURRENT} → {}", release.tag_name);
    if check_only {
        println!("run `nauka update` to install it");
        return Ok(());
    }

    let version = release.tag_name.trim_start_matches('v');
    let asset_name = format!("nauka-{version}-{triple}.tar.gz");
    let asset = release
        .assets
        .iter()
        .find(|a| a.name == asset_name)
        .with_context(|| format!("release has no asset {asset_name}"))?;
    let sums = release
        .assets
        .iter()
        .find(|a| a.name == "SHA256SUMS.txt")
        .context("release has no SHA256SUMS.txt")?;

    println!("downloading {asset_name}…");
    let tarball = fetch(&http, &asset.browser_download_url).await?;
    let sums_txt = String::from_utf8(fetch(&http, &sums.browser_download_url).await?)?;
    let expected = published_sha256(&sums_txt, &asset_name)
        .with_context(|| format!("{asset_name} missing from SHA256SUMS.txt"))?;
    let actual: String = sha2::Sha256::digest(&tarball)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    if actual != expected {
        bail!("checksum mismatch on {asset_name}: expected {expected}, got {actual}");
    }
    println!("checksum verified");

    let binary = extract_binary(&tarball)?;

    // Atomic swap: write next to the current executable (same filesystem),
    // set the mode, rename over. The running process keeps its inode.
    let exe = std::env::current_exe()
        .context("locating the current binary")?
        .canonicalize()
        .context("resolving the binary path")?;
    let tmp = exe.with_extension("update-tmp");
    (|| -> std::io::Result<()> {
        std::fs::write(&tmp, &binary)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755))?;
        }
        std::fs::rename(&tmp, &exe)
    })()
    .map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        if e.kind() == std::io::ErrorKind::PermissionDenied {
            anyhow::anyhow!(
                "no write permission on {} — re-run with sudo: sudo nauka update",
                exe.display()
            )
        } else {
            anyhow::Error::from(e).context(format!("replacing {}", exe.display()))
        }
    })?;

    println!(
        "nauka {CURRENT} → {} installed at {}",
        release.tag_name,
        exe.display()
    );
    if std::path::Path::new("/etc/systemd/system/nauka.service").exists() {
        println!("restart the node to run it: systemctl restart nauka");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_parse_and_compare() {
        assert_eq!(parse_version("v0.2.0"), Some((0, 2, 0)));
        assert_eq!(parse_version("1.12.3"), Some((1, 12, 3)));
        assert_eq!(parse_version("v0.2.1-rc1"), Some((0, 2, 1)));
        assert!(parse_version("v1.2.3").unwrap() > parse_version("v1.2.2").unwrap());
        assert!(parse_version("v0.10.0").unwrap() > parse_version("v0.9.9").unwrap());
        assert_eq!(parse_version("nonsense"), None);
    }

    #[test]
    fn sums_file_lookup() {
        let sums = "abc123  nauka-0.2.0-x86_64-unknown-linux-gnu.tar.gz\n\
                    def456 *nauka-0.2.0-aarch64-apple-darwin.tar.gz\n";
        assert_eq!(
            published_sha256(sums, "nauka-0.2.0-x86_64-unknown-linux-gnu.tar.gz").as_deref(),
            Some("abc123")
        );
        assert_eq!(
            published_sha256(sums, "nauka-0.2.0-aarch64-apple-darwin.tar.gz").as_deref(),
            Some("def456"),
            "the sha256sum binary-mode `*` prefix must not defeat the lookup"
        );
        assert_eq!(published_sha256(sums, "absent.tar.gz"), None);
    }
}
