//! Control socket server — runs inside the hypervisor daemon.
//!
//! Binds to [`super::protocol::socket_path`], spawns a fresh task per
//! incoming connection, dispatches each [`ControlRequest`] against the
//! daemon's long-lived [`EmbeddedDb`] handle, and writes back a
//! [`ControlResponse`]. No state is kept across connections.

use std::path::Path;

use tokio::net::{UnixListener, UnixStream};
use tokio::sync::watch;

use nauka_core::error::NaukaError;
use nauka_state::EmbeddedDb;

use super::protocol::{socket_path, ControlRequest, ControlResponse};
use crate::fabric::ops;
use crate::fabric::peering_server::{read_json, write_json};
use crate::fabric::state::FabricState;

/// Run the control socket accept loop.
///
/// Returns when `shutdown` transitions to `true`. The socket file is
/// removed on exit so the next daemon start can rebind without a stale
/// `Address already in use`.
///
/// `shutdown_trigger` is the *sender* side of the same watch channel
/// the daemon uses for its own shutdown — when a client sends
/// [`ControlRequest::Shutdown`] we flip it to `true`, which wakes the
/// daemon's main `shutdown.changed()` future and starts the clean
/// teardown sequence.
pub async fn run(
    db: EmbeddedDb,
    shutdown_trigger: watch::Sender<bool>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), NaukaError> {
    let path = socket_path();
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|e| {
                NaukaError::internal(format!(
                    "control socket parent dir {}: {e}",
                    parent.display()
                ))
            })?;
        }
    }

    // Clear any stale socket from a previous crash — `bind` refuses to
    // clobber an existing file, and we don't want operator CLIs to
    // hang on an orphan path.
    let _ = std::fs::remove_file(&path);

    let listener = UnixListener::bind(&path)
        .map_err(|e| NaukaError::internal(format!("bind {}: {e}", path.display())))?;

    set_socket_perms(&path);

    tracing::info!(path = %path.display(), "control socket listening");

    loop {
        if *shutdown.borrow() {
            break;
        }

        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    tracing::info!("control socket shutting down");
                    break;
                }
            }
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((stream, _addr)) => {
                        let db = db.clone();
                        let trigger = shutdown_trigger.clone();
                        tokio::spawn(async move {
                            handle_one(stream, db, trigger).await;
                        });
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "control socket accept error");
                    }
                }
            }
        }
    }

    let _ = std::fs::remove_file(&path);
    Ok(())
}

/// Chmod the control socket to 0o660 so only root (service mode) or
/// the owner (CLI mode) can talk to it. systemd's `RuntimeDirectory`
/// already locks `/run/nauka` to 0o750 in service mode; this is a
/// defence-in-depth on the socket file itself.
fn set_socket_perms(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Err(e) = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o660)) {
            tracing::warn!(
                path = %path.display(),
                error = %e,
                "control socket chmod 0660 failed"
            );
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
}

/// Read one request, dispatch it, write the response. Errors during
/// read/write are logged and the connection is dropped; the client
/// will see `EOF` on its read.
async fn handle_one(mut stream: UnixStream, db: EmbeddedDb, shutdown_trigger: watch::Sender<bool>) {
    let req: ControlRequest = match read_json(&mut stream).await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "control: read request failed");
            return;
        }
    };

    let resp = dispatch(&db, req, &shutdown_trigger).await;

    if let Err(e) = write_json(&mut stream, &resp).await {
        tracing::warn!(error = %e, "control: write response failed");
    }
}

/// Operation dispatch table. Each arm either pulls data out of the
/// daemon's live `EmbeddedDb` or mutates it (drain, enable, update,
/// shutdown) and returns a JSON envelope.
async fn dispatch(
    db: &EmbeddedDb,
    req: ControlRequest,
    shutdown_trigger: &watch::Sender<bool>,
) -> ControlResponse {
    match req {
        ControlRequest::Ping => ControlResponse::ok(serde_json::Value::Null),

        ControlRequest::Status => match ops::status_view(db).await {
            Ok(v) => ControlResponse::ok(v),
            Err(e) => ControlResponse::err(e.to_string()),
        },

        ControlRequest::List => match ops::list_view(db).await {
            Ok(v) => ControlResponse::ok(v),
            Err(e) => ControlResponse::err(e.to_string()),
        },

        ControlRequest::Get { name } => match ops::get_view(db, &name).await {
            Ok(v) => ControlResponse::ok(v),
            Err(e) => ControlResponse::err(e.to_string()),
        },

        ControlRequest::MeshIpv6 => match FabricState::load(db).await {
            Ok(Some(state)) => {
                ControlResponse::ok(serde_json::json!(state.hypervisor.mesh_ipv6.to_string()))
            }
            Ok(None) => ControlResponse::err("not initialized"),
            Err(e) => ControlResponse::err(e.to_string()),
        },

        ControlRequest::Drain => match ops::drain(db).await {
            Ok(()) => ControlResponse::ok(serde_json::Value::Null),
            Err(e) => ControlResponse::err(e.to_string()),
        },

        ControlRequest::Enable => match ops::enable(db).await {
            Ok(()) => ControlResponse::ok(serde_json::Value::Null),
            Err(e) => ControlResponse::err(e.to_string()),
        },

        ControlRequest::Update {
            ipv6_block,
            ipv4_public,
            name,
        } => {
            let cfg = ops::UpdateConfig {
                ipv6_block,
                ipv4_public,
                name,
            };
            match ops::update(db, &cfg).await {
                Ok(hv) => ControlResponse::ok(serde_json::json!({
                    "name": hv.name,
                    "id": hv.id.as_str(),
                    "region": hv.region,
                    "zone": hv.zone,
                    "mesh_ipv6": hv.mesh_ipv6.to_string(),
                    "ipv6_block": hv.ipv6_block,
                    "ipv4_public": hv.ipv4_public,
                })),
                Err(e) => ControlResponse::err(e.to_string()),
            }
        }

        ControlRequest::Shutdown => {
            // Flip the daemon's shutdown channel; the accept loop and
            // every spawned listener is watching it and will exit.
            // Reply `ok` *before* we close the socket so the client
            // sees a clean acknowledgement.
            let _ = shutdown_trigger.send(true);
            ControlResponse::ok(serde_json::Value::Null)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn set_perms_on_missing_file_is_a_warning_not_panic() {
        // Best-effort chmod: a missing file must not crash the server.
        set_socket_perms(std::path::Path::new("/tmp/nauka-does-not-exist"));
    }
}
