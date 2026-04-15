//! Control socket client — used by every operator-facing CLI handler.
//!
//! The operator runs `nauka hypervisor status` (or `list`, `get`,
//! `cp-status`, …) in a terminal. In the new daemon model the daemon
//! holds the flock, so every read path that used to `open_default()`
//! must route through the Unix socket when the daemon is up — and
//! seamlessly fall back to a direct DB open when it is not (first-time
//! bootstrap, test harness, recovery).
//!
//! [`forward_or_fallback`] is the single helper every handler calls.
//! It also decouples CLI and daemon: if the binary is upgraded on disk
//! but the running daemon is still the previous version, the CLI still
//! talks to it over the same wire protocol.

use std::future::Future;
use std::time::Duration;

use tokio::net::UnixStream;

use super::protocol::{socket_path, ControlRequest, ControlResponse};
use crate::fabric::peering_server::{read_json, write_json};

/// Errors returned by [`forward_or_fallback`]. Only `SocketMissing`
/// and `ConnectRefused` trigger the fallback path — everything else
/// surfaces to the caller as an error.
#[derive(Debug)]
pub enum ClientError {
    /// The socket file does not exist. Daemon is not running (or has
    /// not been installed yet, which is the case during `init`).
    SocketMissing,
    /// The socket file exists but connecting to it failed — most
    /// commonly `ECONNREFUSED` after a daemon crash left a stale path.
    ConnectRefused(std::io::Error),
    /// Transport error while talking to the daemon (read/write failed).
    Transport(String),
    /// The daemon answered with `ok == false`.
    Daemon(String),
    /// Response payload did not deserialise into the expected type.
    BadPayload(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::SocketMissing => write!(f, "daemon socket not present"),
            ClientError::ConnectRefused(e) => write!(f, "daemon socket refused connection: {e}"),
            ClientError::Transport(m) => write!(f, "daemon transport: {m}"),
            ClientError::Daemon(m) => write!(f, "daemon error: {m}"),
            ClientError::BadPayload(m) => write!(f, "daemon payload: {m}"),
        }
    }
}

impl std::error::Error for ClientError {}

/// How long we wait for the daemon to accept + answer a request
/// before giving up and surfacing a transport error. The daemon
/// should respond in the low-ms range for every supported request;
/// 5 s matches the Nauka "fast timeout" convention for operator
/// commands.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Send a single request to the daemon and parse the response.
///
/// Returns the raw `serde_json::Value` so callers can project it into
/// whatever concrete type they want. Returns [`ClientError::SocketMissing`]
/// / [`ClientError::ConnectRefused`] when the daemon isn't up —
/// callers typically route that to the fallback path.
pub async fn send(req: &ControlRequest) -> Result<serde_json::Value, ClientError> {
    let path = socket_path();
    if !path.exists() {
        return Err(ClientError::SocketMissing);
    }

    let connect = UnixStream::connect(&path);
    let mut stream = match tokio::time::timeout(REQUEST_TIMEOUT, connect).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            // Classify ECONNREFUSED / ENOENT as "daemon not up" so the
            // fallback path still kicks in even if a stale socket file
            // lingered.
            if e.kind() == std::io::ErrorKind::ConnectionRefused
                || e.kind() == std::io::ErrorKind::NotFound
            {
                return Err(ClientError::ConnectRefused(e));
            }
            return Err(ClientError::Transport(format!("connect: {e}")));
        }
        Err(_) => return Err(ClientError::Transport("connect timeout".into())),
    };

    let io = async {
        write_json(&mut stream, req)
            .await
            .map_err(|e| ClientError::Transport(format!("write: {e}")))?;
        let resp: ControlResponse = read_json(&mut stream)
            .await
            .map_err(|e| ClientError::Transport(format!("read: {e}")))?;
        Ok::<_, ClientError>(resp)
    };

    let resp = match tokio::time::timeout(REQUEST_TIMEOUT, io).await {
        Ok(res) => res?,
        Err(_) => return Err(ClientError::Transport("request timeout".into())),
    };

    if !resp.ok {
        return Err(ClientError::Daemon(
            resp.error.unwrap_or_else(|| "unknown error".into()),
        ));
    }

    Ok(resp.data)
}

/// Try to forward the operation over the control socket. If the
/// daemon is not running, invoke `fallback` instead. Transport,
/// daemon, and payload errors are all propagated as `anyhow::Error`
/// so CLI handlers keep their existing error shape.
///
/// `decode` converts the raw JSON value returned by the daemon (or
/// produced by the fallback path) into the CLI handler's strongly
/// typed return value. `fallback` is invoked **lazily** — if the
/// daemon handles the request, `fallback` never runs and the caller
/// never opens the DB.
pub async fn forward_or_fallback<T, FbFut, DecFn>(
    req: ControlRequest,
    fallback: impl FnOnce() -> FbFut,
    decode: DecFn,
) -> anyhow::Result<T>
where
    FbFut: Future<Output = anyhow::Result<serde_json::Value>>,
    DecFn: FnOnce(serde_json::Value) -> anyhow::Result<T>,
{
    match send(&req).await {
        Ok(value) => decode(value),
        Err(ClientError::SocketMissing) | Err(ClientError::ConnectRefused(_)) => {
            let value = fallback().await?;
            decode(value)
        }
        Err(other) => Err(anyhow::anyhow!("{other}")),
    }
}

/// Best-effort liveness probe. Returns `true` if a daemon answered a
/// [`ControlRequest::Ping`] within the request timeout, `false`
/// otherwise. Used by the CLI to decide whether to print "daemon
/// running" vs "daemon not installed" hints.
pub async fn is_daemon_up() -> bool {
    matches!(send(&ControlRequest::Ping).await, Ok(_))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn send_returns_socket_missing_when_path_absent() {
        // Override: in CLI mode socket_path() points at ~/.nauka/ctl.sock.
        // In the test environment we're not root and there's no daemon, so
        // the path usually doesn't exist. Just confirm we get the expected
        // error class.
        let path = socket_path();
        if path.exists() {
            // If a real daemon is up on this test host, skip.
            return;
        }
        let result = send(&ControlRequest::Ping).await;
        assert!(matches!(
            result,
            Err(ClientError::SocketMissing) | Err(ClientError::ConnectRefused(_))
        ));
    }

    #[tokio::test]
    async fn forward_or_fallback_runs_fallback_when_daemon_missing() {
        let path = socket_path();
        if path.exists() {
            return;
        }
        let called = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = called.clone();
        let result: anyhow::Result<i32> = forward_or_fallback(
            ControlRequest::Ping,
            || async move {
                flag.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(serde_json::json!(42))
            },
            |v| Ok(v.as_i64().unwrap() as i32),
        )
        .await;
        assert_eq!(result.unwrap(), 42);
        assert!(called.load(std::sync::atomic::Ordering::SeqCst));
    }
}
