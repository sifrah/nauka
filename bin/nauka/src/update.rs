//! Self-update command — downloads and replaces the running binary.
//!
//! Detects the current channel (nightly, beta, stable) from the embedded
//! build version and stays on it by default. `--channel` overrides.

use anyhow::{bail, Context, Result};
use sha2::{Digest, Sha256};
use std::io::Read;

use nauka_core::version::{backup_current_binary, Channel, Version};

const REPO: &str = "sifrah/nauka";
const GITHUB_API: &str = "https://api.github.com";

/// Build the `nauka update` clap command.
pub fn command() -> clap::Command {
    clap::Command::new("update")
        .about("Update nauka to the latest version")
        .arg(
            clap::Arg::new("channel")
                .long("channel")
                .short('c')
                .help("Target channel (nightly, beta, stable). Defaults to current channel")
                .value_name("CHANNEL"),
        )
        .arg(
            clap::Arg::new("force")
                .long("force")
                .short('f')
                .help("Force update even if already up to date")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            clap::Arg::new("yes")
                .long("yes")
                .short('y')
                .help("Skip confirmation prompt")
                .action(clap::ArgAction::SetTrue),
        )
}

/// Run the self-update flow.
pub async fn run(matches: &clap::ArgMatches) -> Result<()> {
    let current = Version::current();
    let force = matches.get_flag("force");
    let skip_confirm = matches.get_flag("yes");

    // Determine target channel
    let channel: Channel = match matches.get_one::<String>("channel") {
        Some(ch) => ch.parse().map_err(|e| anyhow::anyhow!("{e}"))?,
        None => current.channel,
    };

    eprintln!("  Current version: {current} ({channel})");

    // Find latest release for channel
    let (tag, version) = find_latest_release(channel).await?;
    eprintln!("  Latest {channel}: {version}");

    // Check if update is needed
    if !force && !version.is_newer_than(&current) && channel == current.channel {
        eprintln!("  Already up to date.");
        return Ok(());
    }

    // Confirm
    if !skip_confirm {
        eprint!("  Update {current} -> {version}? [y/N] ");
        let mut buf = String::new();
        std::io::stdin().read_line(&mut buf)?;
        if !buf.trim().eq_ignore_ascii_case("y") {
            eprintln!("  Aborted.");
            return Ok(());
        }
    }

    // Detect platform
    let target = detect_target()?;
    let archive_name = format!("nauka-{tag}-{target}.tar.gz");
    let archive_url = format!("https://github.com/{REPO}/releases/download/{tag}/{archive_name}");
    let checksums_url = format!("https://github.com/{REPO}/releases/download/{tag}/SHA256SUMS.txt");

    let client = reqwest::Client::builder()
        .user_agent("nauka-self-update")
        .build()?;

    // Download archive
    eprint!("  Downloading {archive_name}...");
    let archive_bytes = client
        .get(&archive_url)
        .send()
        .await?
        .error_for_status()
        .context("failed to download release archive")?
        .bytes()
        .await?;
    eprintln!(" done ({:.1} MB)", archive_bytes.len() as f64 / 1_048_576.0);

    // Download and verify checksum
    eprint!("  Verifying checksum...");
    let checksums_text = client
        .get(&checksums_url)
        .send()
        .await?
        .error_for_status()
        .context("failed to download SHA256SUMS.txt")?
        .text()
        .await?;

    let expected_hash = checksums_text
        .lines()
        .find(|line| line.contains(&archive_name))
        .and_then(|line| line.split_whitespace().next())
        .ok_or_else(|| anyhow::anyhow!("no checksum found for {archive_name}"))?;

    let mut hasher = Sha256::new();
    hasher.update(&archive_bytes);
    let actual_hash = format!("{:x}", hasher.finalize());

    if actual_hash != expected_hash {
        bail!("checksum mismatch:\n  expected: {expected_hash}\n  actual:   {actual_hash}");
    }
    eprintln!(" ok");

    // Extract binary from tarball
    eprint!("  Extracting...");
    let decoder = flate2::read::GzDecoder::new(&archive_bytes[..]);
    let mut archive = tar::Archive::new(decoder);
    let mut new_binary = Vec::new();

    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?;
        if path.file_name().and_then(|n| n.to_str()) == Some("nauka") {
            entry.read_to_end(&mut new_binary)?;
            break;
        }
    }

    if new_binary.is_empty() {
        bail!("archive does not contain 'nauka' binary");
    }
    eprintln!(" done");

    // Backup current binary
    eprint!("  Backing up current binary...");
    backup_current_binary().map_err(|e| anyhow::anyhow!("{e}"))?;
    eprintln!(" done");

    // Replace current binary
    eprint!("  Installing...");
    let current_exe = std::env::current_exe()?;

    // On Unix: remove then write (avoids "Text file busy")
    std::fs::remove_file(&current_exe).context("failed to remove current binary")?;
    std::fs::write(&current_exe, &new_binary).context("failed to write new binary")?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&current_exe, std::fs::Permissions::from_mode(0o755))?;
    }

    eprintln!(" done");
    eprintln!("  Updated to {version}");

    Ok(())
}

/// Find the latest GitHub release for a channel.
/// Returns (tag, parsed version).
async fn find_latest_release(channel: Channel) -> Result<(String, Version)> {
    let client = reqwest::Client::builder()
        .user_agent("nauka-self-update")
        .build()?;

    match channel {
        Channel::Stable => {
            // Use the /releases/latest endpoint (returns latest non-prerelease)
            let url = format!("{GITHUB_API}/repos/{REPO}/releases/latest");
            let release: serde_json::Value = client
                .get(&url)
                .header("Accept", "application/vnd.github+json")
                .send()
                .await?
                .error_for_status()
                .context("failed to fetch latest stable release")?
                .json()
                .await?;

            let tag = release["tag_name"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("no tag_name in release response"))?
                .to_string();
            let version = Version::parse(&tag)?;
            Ok((tag, version))
        }
        _ => {
            // List recent releases and find the latest matching channel
            let url = format!("{GITHUB_API}/repos/{REPO}/releases?per_page=30");
            let releases: Vec<serde_json::Value> = client
                .get(&url)
                .header("Accept", "application/vnd.github+json")
                .send()
                .await?
                .error_for_status()
                .context("failed to fetch releases")?
                .json()
                .await?;

            let channel_str = channel.as_str();
            for release in &releases {
                if let Some(tag) = release["tag_name"].as_str() {
                    if tag.contains(channel_str) {
                        let version = Version::parse(tag)?;
                        if version.channel == channel {
                            return Ok((tag.to_string(), version));
                        }
                    }
                }
            }

            bail!("no {channel} release found");
        }
    }
}

/// Detect the target triple for the current platform.
fn detect_target() -> Result<String> {
    let arch = if cfg!(target_arch = "x86_64") {
        "x86_64"
    } else if cfg!(target_arch = "aarch64") {
        "aarch64"
    } else {
        bail!("unsupported architecture");
    };

    let os = if cfg!(target_os = "linux") {
        "unknown-linux-musl"
    } else if cfg!(target_os = "macos") {
        "apple-darwin"
    } else {
        bail!("unsupported operating system");
    };

    Ok(format!("{arch}-{os}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_target_succeeds() {
        let target = detect_target().unwrap();
        assert!(
            target.contains("apple-darwin") || target.contains("linux-musl"),
            "unexpected target: {target}"
        );
    }

    #[test]
    fn update_command_parses() {
        let cmd = command();
        let matches = cmd
            .try_get_matches_from(["update", "--channel", "nightly", "--force"])
            .unwrap();
        assert_eq!(matches.get_one::<String>("channel").unwrap(), "nightly");
        assert!(matches.get_flag("force"));
    }
}
