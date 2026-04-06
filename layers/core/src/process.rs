//! Process utilities — paths, directories.
//!
//! No daemon, no fork, no PID file. Nauka is a CLI orchestrator,
//! not a daemon. WireGuard runs in the kernel.

use std::path::PathBuf;

use crate::error::NaukaError;

/// Default nauka directory.
pub fn nauka_dir() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".nauka")
}

/// Ensure ~/.nauka exists with 0o700 permissions.
pub fn ensure_nauka_dir() -> Result<(), NaukaError> {
    let dir = nauka_dir();
    std::fs::create_dir_all(&dir).map_err(NaukaError::from)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700));
    }
    Ok(())
}

/// Default control socket path (for future API server).
pub fn socket_path() -> PathBuf {
    nauka_dir().join("control.sock")
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
    fn socket_path_in_dir() {
        let p = socket_path();
        assert!(p.to_str().unwrap().contains(".nauka/control.sock"));
    }

    #[test]
    fn ensure_dir_creates() {
        let _ = ensure_nauka_dir();
    }
}
