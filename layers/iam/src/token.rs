//! File-backed JWT store at `~/.config/nauka/token` (`mode 0600`).
//!
//! The CLI is the only user of this module. The daemon never reads or
//! writes `~/.config/nauka/token` — it receives the JWT from the CLI
//! over the IPC channel (later IAM phases) and validates against
//! SurrealDB.

use std::fs;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;

use crate::error::IamError;

/// Resolve the token file path, honouring `NAUKA_CONFIG_DIR` for
/// integration tests that can't write to `$HOME` (CI sandboxes,
/// hermetic test harnesses). Falls back to
/// `$XDG_CONFIG_HOME/nauka/token` and then `$HOME/.config/nauka/token`.
pub fn token_path() -> Result<PathBuf, IamError> {
    if let Some(dir) = std::env::var_os("NAUKA_CONFIG_DIR") {
        return Ok(PathBuf::from(dir).join("token"));
    }
    let base = if let Some(xdg) = std::env::var_os("XDG_CONFIG_HOME") {
        PathBuf::from(xdg)
    } else {
        let home = std::env::var_os("HOME").ok_or_else(|| {
            IamError::Token(
                "cannot resolve token path: neither HOME nor XDG_CONFIG_HOME is set".into(),
            )
        })?;
        PathBuf::from(home).join(".config")
    };
    Ok(base.join("nauka").join("token"))
}

/// Write the JWT to disk at mode `0600`. Creates parent dirs if
/// needed; overwrites any existing token.
pub fn save_token(jwt: &str) -> Result<(), IamError> {
    let path = token_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| IamError::Token(format!("create {}: {e}", parent.display())))?;
    }
    // Rewrite from scratch at 0600. `truncate(true)` ensures that
    // shrinking the new JWT doesn't leave old bytes behind.
    let mut f = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&path)
        .map_err(|e| IamError::Token(format!("open {}: {e}", path.display())))?;
    f.write_all(jwt.as_bytes())
        .map_err(|e| IamError::Token(format!("write {}: {e}", path.display())))?;
    Ok(())
}

/// Load the JWT, or return `None` if the file does not exist.
pub fn load_token() -> Result<Option<String>, IamError> {
    let path = token_path()?;
    match fs::read_to_string(&path) {
        Ok(s) => Ok(Some(s.trim_end_matches('\n').to_string())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(IamError::Token(format!("read {}: {e}", path.display()))),
    }
}

/// Remove the token file. Idempotent — missing file is not an error.
pub fn delete_token() -> Result<(), IamError> {
    let path = token_path()?;
    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(IamError::Token(format!("remove {}: {e}", path.display()))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Mutex;

    // `NAUKA_CONFIG_DIR` is process-global, so tests that set it must
    // not run concurrently — cargo's default threaded harness would
    // otherwise race (one test's `remove_var` clears the value
    // another test is depending on). Serialise with a shared mutex.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_config<R>(f: impl FnOnce(&std::path::Path) -> R) -> R {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let dir = tempfile::tempdir().expect("tempdir");
        // SAFETY: set_var is unsafe in multi-threaded contexts; the
        // mutex above serialises every test that touches this var.
        unsafe {
            std::env::set_var("NAUKA_CONFIG_DIR", dir.path());
        }
        let result = f(dir.path());
        unsafe {
            std::env::remove_var("NAUKA_CONFIG_DIR");
        }
        drop(dir);
        result
    }

    #[test]
    fn save_creates_file_with_0600_mode() {
        with_config(|path| {
            save_token("abc.def.ghi").unwrap();
            let file = path.join("token");
            let md = fs::metadata(&file).unwrap();
            let mode = md.permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "token file must be 0600, got {mode:o}");
            assert_eq!(fs::read_to_string(&file).unwrap(), "abc.def.ghi");
        });
    }

    #[test]
    fn load_returns_none_when_missing() {
        with_config(|_| {
            assert!(load_token().unwrap().is_none());
        });
    }

    #[test]
    fn round_trip_save_then_load() {
        with_config(|_| {
            save_token("tok").unwrap();
            assert_eq!(load_token().unwrap().as_deref(), Some("tok"));
        });
    }

    #[test]
    fn delete_is_idempotent() {
        with_config(|_| {
            delete_token().unwrap();
            save_token("x").unwrap();
            delete_token().unwrap();
            assert!(load_token().unwrap().is_none());
        });
    }
}
