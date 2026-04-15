//! Process utilities — paths, directories, run-mode detection.
//!
//! Run-mode-aware helpers for both CLI invocations and the long-lived
//! hypervisor daemon (`nauka.service`). WireGuard runs in the kernel.
//!
//! # Run modes
//!
//! Nauka can run in one of two modes:
//!
//! - **CLI mode** — invoked by a normal user from a shell. State is
//!   stored under `$HOME/.nauka/`.
//! - **Service mode** — invoked as `root` by systemd. State is stored
//!   under `/var/lib/nauka/`.
//!
//! The two modes are detected by [`is_service_mode`] (root effective
//! UID == service mode). Helpers like [`nauka_db_path`] (added in P1.4,
//! sifrah/nauka#194) use this detection automatically so the same
//! `nauka` binary picks the right path no matter how it was launched.

use std::path::{Path, PathBuf};

use crate::error::NaukaError;

/// Default nauka directory **for CLI mode**: `~/.nauka`.
///
/// Service-mode callers should use [`nauka_state_dir`] or one of the
/// path helpers ([`nauka_db_path`], etc.) instead — those are
/// run-mode-aware.
pub fn nauka_dir() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".nauka")
}

/// Ensure ~/.nauka exists with 0o700 permissions.
pub fn ensure_nauka_dir() -> Result<(), NaukaError> {
    let dir = nauka_dir();
    std::fs::create_dir_all(&dir).map_err(NaukaError::from)?;
    set_dir_perms(&dir);
    Ok(())
}

/// Path of the hypervisor daemon's Unix control socket.
///
/// - CLI mode → `$HOME/.nauka/ctl.sock`
/// - Service mode → `/run/nauka/ctl.sock`
///
/// The parent directory is **not** created by this function. In service
/// mode it is populated by systemd via `RuntimeDirectory=nauka` on
/// `nauka.service`; in CLI mode [`ensure_nauka_dir`] / the daemon
/// creates `~/.nauka` before binding.
pub fn socket_path() -> PathBuf {
    if is_service_mode() {
        PathBuf::from("/run/nauka/ctl.sock")
    } else {
        nauka_dir().join("ctl.sock")
    }
}

// ─────────────────────────────────────────────────────────────────────
// Run mode detection (P1.4, sifrah/nauka#194)
// ─────────────────────────────────────────────────────────────────────

/// `true` when the current process is running as root, which is what
/// "service mode" means for Nauka in practice (systemd starts everything
/// as root). `false` for normal CLI invocations from a user shell.
///
/// On non-Unix targets this always returns `false` because Nauka's only
/// supported deployment target for service mode is Linux + systemd.
pub fn is_service_mode() -> bool {
    #[cfg(unix)]
    {
        // SAFETY: `geteuid` has no preconditions and never fails on
        // any POSIX system. It just reads a thread-local value.
        unsafe { libc::geteuid() == 0 }
    }
    #[cfg(not(unix))]
    {
        false
    }
}

/// State root directory for the current run mode.
///
/// - CLI mode → `$HOME/.nauka` (or `./.nauka` if `$HOME` is unset)
/// - Service mode → `/var/lib/nauka`
pub fn nauka_state_dir() -> PathBuf {
    if is_service_mode() {
        PathBuf::from("/var/lib/nauka")
    } else {
        nauka_dir()
    }
}

/// Path of the on-disk SurrealKV datastore used by `EmbeddedDb`.
///
/// - CLI mode → `$HOME/.nauka/bootstrap.skv`
/// - Service mode → `/var/lib/nauka/bootstrap.skv`
///
/// The parent directory is **not** created by this function — call
/// [`ensure_nauka_state_dir`] first if you need it on disk with the
/// right permissions, or rely on `EmbeddedDb::open` (P1.2,
/// sifrah/nauka#192) which creates parents on demand.
pub fn nauka_db_path() -> PathBuf {
    nauka_state_dir().join("bootstrap.skv")
}

/// Ensure the run-mode-appropriate state directory exists with
/// 0o700 permissions on Unix.
///
/// Returns the directory path on success.
pub fn ensure_nauka_state_dir() -> Result<PathBuf, NaukaError> {
    let dir = nauka_state_dir();
    std::fs::create_dir_all(&dir).map_err(NaukaError::from)?;
    set_dir_perms(&dir);
    Ok(dir)
}

/// Apply 0o700 permissions to a directory on Unix. Best-effort: a
/// permission failure is logged via `tracing::warn!` but does not
/// surface as an error, because the alternative is to refuse to start
/// over a chmod failure on a directory we just successfully created.
fn set_dir_perms(dir: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Err(e) = std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)) {
            tracing::warn!(
                target: "nauka_core::process",
                path = %dir.display(),
                error = %e,
                "failed to chmod 0o700"
            );
        }
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nauka_dir_path() {
        let d = nauka_dir();
        assert!(d.to_str().unwrap().contains(".nauka"));
    }

    #[test]
    fn socket_path_run_mode_aware() {
        let p = socket_path();
        // In test environment we run as non-root so we land in CLI mode.
        // The service-mode branch is exercised manually via Hetzner smoke
        // tests; asserting on it here would require faking geteuid().
        if is_service_mode() {
            assert_eq!(p, PathBuf::from("/run/nauka/ctl.sock"));
        } else {
            assert!(p.to_str().unwrap().ends_with(".nauka/ctl.sock"));
        }
    }

    #[test]
    fn ensure_dir_creates() {
        let _ = ensure_nauka_dir();
    }

    #[test]
    fn db_path_filename_is_bootstrap_skv() {
        let p = nauka_db_path();
        assert_eq!(
            p.file_name().and_then(|s| s.to_str()),
            Some("bootstrap.skv"),
            "db path must end with bootstrap.skv, got: {}",
            p.display()
        );
    }

    #[test]
    fn db_path_lives_in_state_dir() {
        let p = nauka_db_path();
        let parent = p.parent().expect("db path must have a parent");
        assert_eq!(parent, nauka_state_dir());
    }

    #[test]
    fn state_dir_matches_run_mode() {
        // Whichever mode the test process happens to run in, the path
        // must match the expected branch. Tests typically run as the
        // current user (not root), so this exercises CLI mode in CI;
        // a manual `sudo cargo test` exercises the other branch.
        if is_service_mode() {
            assert_eq!(nauka_state_dir(), PathBuf::from("/var/lib/nauka"));
        } else {
            assert_eq!(nauka_state_dir(), nauka_dir());
        }
    }

    #[test]
    fn db_path_is_under_state_dir_in_both_modes() {
        // Defensive: just confirm the prefix relationship holds.
        let db = nauka_db_path();
        assert!(
            db.starts_with(nauka_state_dir()),
            "{} must be a child of {}",
            db.display(),
            nauka_state_dir().display()
        );
    }
}
