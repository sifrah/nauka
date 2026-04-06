//! Version management and update channels.
//!
//! Supports 4 channels: nightly, beta, rc, stable.
//! Versions are semver with channel suffix: `v2.1.0-nightly.47`, `v2.1.0`, etc.
//!
//! ```
//! use nauka_core::version::{Version, Channel};
//!
//! let v = Version::parse("2.1.0-beta.3").unwrap();
//! assert_eq!(v.channel, Channel::Beta);
//! assert_eq!(v.major, 2);
//! assert!(v > Version::parse("2.1.0-beta.2").unwrap());
//! ```

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;

use crate::error::NaukaError;

/// Release channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Channel {
    Nightly,
    Beta,
    Rc,
    Stable,
}

impl Channel {
    /// Stability rank (higher = more stable).
    pub fn rank(&self) -> u8 {
        match self {
            Self::Nightly => 0,
            Self::Beta => 1,
            Self::Rc => 2,
            Self::Stable => 3,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Nightly => "nightly",
            Self::Beta => "beta",
            Self::Rc => "rc",
            Self::Stable => "stable",
        }
    }
}

impl fmt::Display for Channel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Channel {
    type Err = NaukaError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "nightly" | "dev" => Ok(Self::Nightly),
            "beta" => Ok(Self::Beta),
            "rc" => Ok(Self::Rc),
            "stable" | "latest" => Ok(Self::Stable),
            _ => Err(NaukaError::validation(format!(
                "unknown channel '{s}'. Must be: nightly, beta, rc, stable"
            ))),
        }
    }
}

/// A parsed version.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Version {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
    pub channel: Channel,
    /// Build number within channel (e.g., 47 in dev.47). 0 for stable.
    pub build: u32,
}

impl Version {
    /// Parse a version string. Accepts with or without `v` prefix.
    pub fn parse(s: &str) -> Result<Self, NaukaError> {
        let s = s.strip_prefix('v').unwrap_or(s);

        // Split semver from pre-release
        let (semver, pre) = if let Some(pos) = s.find('-') {
            (&s[..pos], Some(&s[pos + 1..]))
        } else {
            (s, None)
        };

        // Parse major.minor.patch
        let parts: Vec<&str> = semver.split('.').collect();
        if parts.len() != 3 {
            return Err(NaukaError::validation(format!(
                "invalid version '{s}': expected MAJOR.MINOR.PATCH"
            )));
        }

        let major: u32 = parts[0]
            .parse()
            .map_err(|_| NaukaError::validation(format!("invalid major version: {}", parts[0])))?;
        let minor: u32 = parts[1]
            .parse()
            .map_err(|_| NaukaError::validation(format!("invalid minor version: {}", parts[1])))?;
        let patch: u32 = parts[2]
            .parse()
            .map_err(|_| NaukaError::validation(format!("invalid patch version: {}", parts[2])))?;

        // Parse pre-release channel
        let (channel, build) = match pre {
            None => (Channel::Stable, 0),
            Some(pre) => {
                let pre_parts: Vec<&str> = pre.splitn(2, '.').collect();
                let ch: Channel = pre_parts[0].parse()?;
                let build: u32 = if pre_parts.len() > 1 {
                    pre_parts[1].parse().map_err(|_| {
                        NaukaError::validation(format!("invalid build number: {}", pre_parts[1]))
                    })?
                } else {
                    0
                };
                (ch, build)
            }
        };

        Ok(Self {
            major,
            minor,
            patch,
            channel,
            build,
        })
    }

    /// Current binary version (injected at build time or from Cargo.toml).
    ///
    /// CI sets `NAUKA_VERSION` (e.g. `2.0.0-nightly.5`) at compile time.
    /// Falls back to `CARGO_PKG_VERSION` for local builds.
    pub fn current() -> Self {
        let version_str =
            option_env!("NAUKA_VERSION").unwrap_or(env!("CARGO_PKG_VERSION"));
        Self::parse(version_str).unwrap_or(Self {
            major: 0,
            minor: 0,
            patch: 0,
            channel: Channel::Nightly,
            build: 0,
        })
    }

    /// Is this a pre-release version?
    pub fn is_prerelease(&self) -> bool {
        self.channel != Channel::Stable
    }

    /// Check if this version is newer than another.
    pub fn is_newer_than(&self, other: &Self) -> bool {
        self > other
    }

    /// Format as GitHub tag: `v2.1.0-beta.3` or `v2.1.0`.
    pub fn tag(&self) -> String {
        format!("v{self}")
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)?;
        if self.channel != Channel::Stable {
            write!(f, "-{}.{}", self.channel, self.build)?;
        }
        Ok(())
    }
}

impl FromStr for Version {
    type Err = NaukaError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl Ord for Version {
    fn cmp(&self, other: &Self) -> Ordering {
        self.major
            .cmp(&other.major)
            .then(self.minor.cmp(&other.minor))
            .then(self.patch.cmp(&other.patch))
            .then(self.channel.rank().cmp(&other.channel.rank()))
            .then(self.build.cmp(&other.build))
    }
}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Feature flag — runtime gating of experimental features.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureFlags {
    flags: std::collections::HashMap<String, bool>,
}

impl FeatureFlags {
    pub fn new() -> Self {
        Self {
            flags: std::collections::HashMap::new(),
        }
    }

    /// Check if a feature is enabled.
    pub fn is_enabled(&self, name: &str) -> bool {
        self.flags.get(name).copied().unwrap_or(false)
    }

    /// Enable a feature.
    pub fn enable(&mut self, name: impl Into<String>) {
        self.flags.insert(name.into(), true);
    }

    /// Disable a feature.
    pub fn disable(&mut self, name: impl Into<String>) {
        self.flags.insert(name.into(), false);
    }
}

impl Default for FeatureFlags {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════
// 5. Version compatibility
// ═══════════════════════════════════════════════════

/// Check if two versions are compatible for cluster membership.
///
/// Rule: nodes must share the same major version.
/// Minor/patch differences are allowed (rolling upgrades).
pub fn is_compatible(a: &Version, b: &Version) -> bool {
    a.major == b.major
}

/// Version compatibility result with details.
#[derive(Debug, Clone)]
pub enum Compatibility {
    /// Fully compatible — same major.minor
    Compatible,
    /// Compatible but different minor — may have feature differences
    MinorDifference { local: String, remote: String },
    /// Incompatible — different major version
    Incompatible { local: String, remote: String },
}

/// Check detailed compatibility between local and remote version.
pub fn check_compatibility(local: &Version, remote: &Version) -> Compatibility {
    if local.major != remote.major {
        return Compatibility::Incompatible {
            local: local.to_string(),
            remote: remote.to_string(),
        };
    }
    if local.minor != remote.minor {
        return Compatibility::MinorDifference {
            local: local.to_string(),
            remote: remote.to_string(),
        };
    }
    Compatibility::Compatible
}

// ═══════════════════════════════════════════════════
// 4. Changelog between versions
// ═══════════════════════════════════════════════════

/// Version diff — what changed between two versions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionDiff {
    pub from: String,
    pub to: String,
    pub channel_change: Option<(String, String)>,
    pub is_upgrade: bool,
    pub is_downgrade: bool,
    pub major_bump: bool,
    pub minor_bump: bool,
    pub patch_bump: bool,
}

impl VersionDiff {
    pub fn between(from: &Version, to: &Version) -> Self {
        let channel_change = if from.channel != to.channel {
            Some((from.channel.to_string(), to.channel.to_string()))
        } else {
            None
        };

        Self {
            from: from.to_string(),
            to: to.to_string(),
            channel_change,
            is_upgrade: to > from,
            is_downgrade: to < from,
            major_bump: to.major > from.major,
            minor_bump: to.major == from.major && to.minor > from.minor,
            patch_bump: to.major == from.major && to.minor == from.minor && to.patch > from.patch,
        }
    }
}

// ═══════════════════════════════════════════════════
// 3. Rollback support
// ═══════════════════════════════════════════════════

/// Path to the backup binary (for rollback).
pub fn backup_binary_path() -> PathBuf {
    crate::process::nauka_dir().join("nauka.backup")
}

/// Create a backup of the current binary before update.
pub fn backup_current_binary() -> Result<(), NaukaError> {
    let current = std::env::current_exe()
        .map_err(|e| NaukaError::internal(format!("failed to get current exe: {e}")))?;
    let backup = backup_binary_path();
    std::fs::copy(&current, &backup)
        .map_err(|e| NaukaError::internal(format!("failed to backup binary: {e}")))?;
    Ok(())
}

/// Rollback to the backup binary.
pub fn rollback() -> Result<(), NaukaError> {
    let backup = backup_binary_path();
    if !backup.exists() {
        return Err(NaukaError::not_found("binary", "nauka.backup"));
    }
    let current = std::env::current_exe()
        .map_err(|e| NaukaError::internal(format!("failed to get current exe: {e}")))?;
    std::fs::copy(&backup, &current)
        .map_err(|e| NaukaError::internal(format!("rollback failed: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Channel ──

    #[test]
    fn channel_parse() {
        assert_eq!("nightly".parse::<Channel>().unwrap(), Channel::Nightly);
        assert_eq!("dev".parse::<Channel>().unwrap(), Channel::Nightly);
        assert_eq!("beta".parse::<Channel>().unwrap(), Channel::Beta);
        assert_eq!("rc".parse::<Channel>().unwrap(), Channel::Rc);
        assert_eq!("stable".parse::<Channel>().unwrap(), Channel::Stable);
        assert_eq!("latest".parse::<Channel>().unwrap(), Channel::Stable);
    }

    #[test]
    fn channel_invalid() {
        assert!("nope".parse::<Channel>().is_err());
    }

    #[test]
    fn channel_rank() {
        assert!(Channel::Nightly.rank() < Channel::Beta.rank());
        assert!(Channel::Beta.rank() < Channel::Rc.rank());
        assert!(Channel::Rc.rank() < Channel::Stable.rank());
    }

    // ── Version parsing ──

    #[test]
    fn parse_stable() {
        let v = Version::parse("2.1.0").unwrap();
        assert_eq!(v.major, 2);
        assert_eq!(v.minor, 1);
        assert_eq!(v.patch, 0);
        assert_eq!(v.channel, Channel::Stable);
        assert_eq!(v.build, 0);
    }

    #[test]
    fn parse_with_v_prefix() {
        let v = Version::parse("v2.1.0-beta.3").unwrap();
        assert_eq!(v.major, 2);
        assert_eq!(v.channel, Channel::Beta);
        assert_eq!(v.build, 3);
    }

    #[test]
    fn parse_nightly() {
        let v = Version::parse("2.1.0-nightly.47").unwrap();
        assert_eq!(v.channel, Channel::Nightly);
        assert_eq!(v.build, 47);
    }

    #[test]
    fn parse_dev_alias() {
        let v = Version::parse("2.1.0-dev.47").unwrap();
        assert_eq!(v.channel, Channel::Nightly);
        assert_eq!(v.build, 47);
    }

    #[test]
    fn parse_rc() {
        let v = Version::parse("2.1.0-rc.1").unwrap();
        assert_eq!(v.channel, Channel::Rc);
        assert_eq!(v.build, 1);
    }

    #[test]
    fn parse_invalid() {
        assert!(Version::parse("not-a-version").is_err());
        assert!(Version::parse("1.2").is_err());
        assert!(Version::parse("1.2.3.4").is_err());
    }

    // ── Display ──

    #[test]
    fn display_stable() {
        let v = Version::parse("2.1.0").unwrap();
        assert_eq!(v.to_string(), "2.1.0");
    }

    #[test]
    fn display_prerelease() {
        let v = Version::parse("2.1.0-beta.3").unwrap();
        assert_eq!(v.to_string(), "2.1.0-beta.3");
    }

    #[test]
    fn tag() {
        let v = Version::parse("2.1.0-beta.3").unwrap();
        assert_eq!(v.tag(), "v2.1.0-beta.3");
    }

    // ── Ordering ──

    #[test]
    fn ordering_semver() {
        let a = Version::parse("1.0.0").unwrap();
        let b = Version::parse("2.0.0").unwrap();
        assert!(b > a);
    }

    #[test]
    fn ordering_minor() {
        let a = Version::parse("2.0.0").unwrap();
        let b = Version::parse("2.1.0").unwrap();
        assert!(b > a);
    }

    #[test]
    fn ordering_patch() {
        let a = Version::parse("2.1.0").unwrap();
        let b = Version::parse("2.1.1").unwrap();
        assert!(b > a);
    }

    #[test]
    fn ordering_channel() {
        let nightly = Version::parse("2.1.0-nightly.1").unwrap();
        let beta = Version::parse("2.1.0-beta.1").unwrap();
        let rc = Version::parse("2.1.0-rc.1").unwrap();
        let stable = Version::parse("2.1.0").unwrap();
        assert!(nightly < beta);
        assert!(beta < rc);
        assert!(rc < stable);
    }

    #[test]
    fn ordering_build() {
        let a = Version::parse("2.1.0-nightly.1").unwrap();
        let b = Version::parse("2.1.0-nightly.47").unwrap();
        assert!(b > a);
    }

    #[test]
    fn is_newer() {
        let old = Version::parse("2.0.0").unwrap();
        let new = Version::parse("2.1.0").unwrap();
        assert!(new.is_newer_than(&old));
        assert!(!old.is_newer_than(&new));
    }

    // ── Feature flags ──

    #[test]
    fn feature_flags_default_off() {
        let f = FeatureFlags::new();
        assert!(!f.is_enabled("async_vm"));
    }

    #[test]
    fn feature_flags_enable_disable() {
        let mut f = FeatureFlags::new();
        f.enable("async_vm");
        assert!(f.is_enabled("async_vm"));
        f.disable("async_vm");
        assert!(!f.is_enabled("async_vm"));
    }

    // ── Current version ──

    #[test]
    fn current_version_parses() {
        let v = Version::current();
        // Should parse without panic
        let _ = v.to_string();
    }

    #[test]
    fn is_prerelease() {
        assert!(Version::parse("2.0.0-nightly.1").unwrap().is_prerelease());
        assert!(Version::parse("2.0.0-beta.1").unwrap().is_prerelease());
        assert!(!Version::parse("2.0.0").unwrap().is_prerelease());
    }

    // ── Serde ──

    #[test]
    fn version_serde() {
        let v = Version::parse("2.1.0-beta.3").unwrap();
        let json = serde_json::to_string(&v).unwrap();
        let back: Version = serde_json::from_str(&json).unwrap();
        assert_eq!(back, v);
    }

    #[test]
    fn channel_serde() {
        let c = Channel::Beta;
        let json = serde_json::to_string(&c).unwrap();
        assert_eq!(json, "\"beta\"");
    }

    // ── #5: Compatibility ──

    #[test]
    fn compatible_same_major() {
        let a = Version::parse("2.0.0").unwrap();
        let b = Version::parse("2.5.3").unwrap();
        assert!(is_compatible(&a, &b));
    }

    #[test]
    fn incompatible_different_major() {
        let a = Version::parse("2.0.0").unwrap();
        let b = Version::parse("3.0.0").unwrap();
        assert!(!is_compatible(&a, &b));
    }

    #[test]
    fn check_compatibility_compatible() {
        let a = Version::parse("2.1.0").unwrap();
        let b = Version::parse("2.1.5").unwrap();
        assert!(matches!(
            check_compatibility(&a, &b),
            Compatibility::Compatible
        ));
    }

    #[test]
    fn check_compatibility_minor_diff() {
        let a = Version::parse("2.1.0").unwrap();
        let b = Version::parse("2.3.0").unwrap();
        assert!(matches!(
            check_compatibility(&a, &b),
            Compatibility::MinorDifference { .. }
        ));
    }

    #[test]
    fn check_compatibility_incompatible() {
        let a = Version::parse("2.0.0").unwrap();
        let b = Version::parse("3.0.0").unwrap();
        assert!(matches!(
            check_compatibility(&a, &b),
            Compatibility::Incompatible { .. }
        ));
    }

    // ── #4: Version diff ──

    #[test]
    fn version_diff_upgrade() {
        let from = Version::parse("2.0.0").unwrap();
        let to = Version::parse("2.1.0").unwrap();
        let diff = VersionDiff::between(&from, &to);
        assert!(diff.is_upgrade);
        assert!(!diff.is_downgrade);
        assert!(diff.minor_bump);
        assert!(!diff.major_bump);
    }

    #[test]
    fn version_diff_downgrade() {
        let from = Version::parse("2.1.0").unwrap();
        let to = Version::parse("2.0.0").unwrap();
        let diff = VersionDiff::between(&from, &to);
        assert!(diff.is_downgrade);
        assert!(!diff.is_upgrade);
    }

    #[test]
    fn version_diff_channel_change() {
        let from = Version::parse("2.1.0-nightly.47").unwrap();
        let to = Version::parse("2.1.0-beta.1").unwrap();
        let diff = VersionDiff::between(&from, &to);
        assert!(diff.channel_change.is_some());
        let (from_ch, to_ch) = diff.channel_change.unwrap();
        assert_eq!(from_ch, "nightly");
        assert_eq!(to_ch, "beta");
    }

    #[test]
    fn version_diff_major_bump() {
        let from = Version::parse("2.5.0").unwrap();
        let to = Version::parse("3.0.0").unwrap();
        let diff = VersionDiff::between(&from, &to);
        assert!(diff.major_bump);
    }

    // ── #3: Rollback ──

    #[test]
    fn backup_path() {
        let p = backup_binary_path();
        assert!(p.to_str().unwrap().contains("nauka.backup"));
    }
}
