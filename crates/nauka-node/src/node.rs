//! `nauka node add <ip>` / `node remove <ip>` — grow and shrink the cluster
//! from one command, run on any existing member.
//!
//! The design is deliberately the automation of what an operator would do
//! by hand: shell out to the system `ssh`/`scp`, which already carry the
//! forwarded agent key, host-key policy and connection multiplexing. A
//! Rust SSH library would reimplement all of that, worse. `node add`
//! provisions the target over SSH — binary, systemd unit, cluster identity
//! — starts it in join-wait mode, then does the Raft membership change
//! locally against the cluster.

use std::net::SocketAddr;
use std::process::Stdio;

use anyhow::{bail, Context, Result};
use tokio::io::AsyncWriteExt;

/// The systemd unit installed on a provisioned node. Same as the packaged
/// one, but ExecStart points at /usr/local/bin (where `node add` puts the
/// binary) and the cluster args come from the env file `node add` writes.
const UNIT: &str = include_str!("../../../packaging/nauka.service");

pub struct AddOpts {
    /// Address the new node advertises to the cluster — its identity.
    pub target: SocketAddr,
    /// SSH login for provisioning (usually root).
    pub ssh_user: String,
    /// Existing members to drive the Raft membership change through.
    pub peers: Vec<SocketAddr>,
    /// Cluster token, if the operator uses one (else keys are copied).
    pub token: Option<String>,
    /// Cluster key directory, if the operator uses keys instead of a token.
    pub keys_dir: Option<std::path::PathBuf>,
    /// Wipe an existing data dir on the target instead of refusing.
    pub force: bool,
}

/// `user@host` for ssh/scp from the SSH login and the advertised IP.
fn ssh_host(o: &AddOpts) -> String {
    format!("{}@{}", o.ssh_user, o.target.ip())
}

/// Run a command on the target over ssh, capturing output. The forwarded
/// agent and host-key policy come from the caller's ssh environment.
async fn ssh_run(o: &AddOpts, script: &str) -> Result<String> {
    // The script is one remote-shell argument. Passing `sh -c <script>` as
    // separate ssh args would make the remote sh treat the script's first
    // word as $0 and drop it — ssh already runs its single command argument
    // through the login shell, so hand it the script directly.
    let out = tokio::process::Command::new("ssh")
        .args(["-o", "BatchMode=yes", "-o", "ConnectTimeout=15"])
        .arg(ssh_host(o))
        .arg(script)
        .output()
        .await
        .context("running ssh — is it installed and your key forwarded?")?;
    if !out.status.success() {
        bail!(
            "remote command failed on {}: {}",
            o.target.ip(),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Pipe bytes to a file on the target (avoids a second scp round-trip and
/// keeps secrets off the argv). `mode` is applied after the write.
async fn ssh_write_file(o: &AddOpts, path: &str, contents: &[u8], mode: &str) -> Result<()> {
    let mut child = tokio::process::Command::new("ssh")
        .args(["-o", "BatchMode=yes", "-o", "ConnectTimeout=15"])
        .arg(ssh_host(o))
        .arg(format!("cat > {path} && chmod {mode} {path}"))
        .stdin(Stdio::piped())
        .spawn()
        .context("running ssh for a file write")?;
    child
        .stdin
        .as_mut()
        .expect("stdin piped")
        .write_all(contents)
        .await?;
    let status = child.wait().await?;
    if !status.success() {
        bail!("writing {path} on {} failed", o.target.ip());
    }
    Ok(())
}

/// scp a local file to the target and verify it landed intact — transfers
/// to fresh cloud instances truncate silently often enough that an
/// unchecked copy is a real hazard.
async fn scp_verified(o: &AddOpts, local: &std::path::Path, remote: &str) -> Result<()> {
    let want = sha256_file(local)?;
    for attempt in 1..=3 {
        let status = tokio::process::Command::new("scp")
            .args(["-o", "BatchMode=yes", "-o", "ConnectTimeout=15"])
            .arg(local)
            .arg(format!("{}:{remote}", ssh_host(o)))
            .status()
            .await
            .context("running scp")?;
        if status.success() {
            let got = ssh_run(o, &format!("sha256sum {remote} | cut -d' ' -f1"))
                .await?
                .trim()
                .to_string();
            if got == want {
                return Ok(());
            }
            eprintln!("  copy of {remote} was truncated (attempt {attempt}) — retrying");
        }
    }
    bail!(
        "could not copy {remote} to {} intact after 3 tries",
        o.target.ip()
    )
}

fn sha256_file(path: &std::path::Path) -> Result<String> {
    use sha2::{Digest, Sha256};
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    Ok(hex::encode(Sha256::digest(&bytes)))
}

pub async fn add(o: AddOpts) -> Result<()> {
    // The identity to hand the target. Exactly one of token/keys must be
    // present — it is how the invoking member itself authenticates.
    if o.token.is_none() && o.keys_dir.is_none() {
        bail!("no cluster identity to give the new node: set NAUKA_TOKEN or pass --keys");
    }
    let ip = o.target.ip();
    println!("provisioning {ip} over ssh ({}@{ip})…", o.ssh_user);

    // 1. Reachable?
    ssh_run(&o, "true")
        .await
        .with_context(|| format!("cannot ssh to {}@{ip}", o.ssh_user))?;

    // 2. Blank data dir, or --force. A target that already holds Raft state
    //    belongs to a cluster; founding a join on top would either fork or
    //    resume the wrong cluster. Refuse loudly.
    // Any non-empty redb in the raft dir means this node already has
    // consensus state — match by glob, not by a hardcoded filename, so a
    // future rename of the log file cannot silently defeat the check.
    let state = ssh_run(
        &o,
        "ls -1 /var/lib/nauka/raft/*.redb 2>/dev/null | head -1 | grep -q . \
         && echo HAS_STATE || echo BLANK",
    )
    .await?;
    if state.trim() == "HAS_STATE" {
        if o.force {
            println!("  --force: wiping the existing data dir on {ip}");
            ssh_run(
                &o,
                "systemctl stop nauka 2>/dev/null; rm -rf /var/lib/nauka/*",
            )
            .await?;
        } else {
            bail!(
                "{ip} already has cluster state — it belongs to a cluster already. \
                 Re-run with --force to wipe and re-add it."
            );
        }
    }

    // 3. The binary this very process is running — a static musl build runs
    //    on any x86_64 Linux, so the common same-fleet case needs no
    //    cross-compilation.
    let self_exe = std::env::current_exe().context("finding my own binary to deploy")?;
    println!("  copying the nauka binary…");
    ssh_run(&o, "mkdir -p /var/lib/nauka /etc/nauka").await?;
    // Land it under a temp name and rename into place: overwriting a
    // RUNNING binary directly fails with ETXTBSY (a re-add of a live node,
    // or a retry). rename is atomic and unlinks the busy inode safely.
    scp_verified(&o, &self_exe, "/usr/local/bin/nauka.new").await?;
    ssh_run(
        &o,
        "chmod 755 /usr/local/bin/nauka.new && mv /usr/local/bin/nauka.new /usr/local/bin/nauka",
    )
    .await?;

    // 4. Dedicated user + directory ownership (mirrors the deb postinst).
    ssh_run(
        &o,
        "getent passwd nauka >/dev/null || adduser --system --group --no-create-home \
         --home /var/lib/nauka --gecos 'Nauka storage node' nauka >/dev/null; \
         chown nauka:nauka /var/lib/nauka && chmod 750 /var/lib/nauka; \
         chown root:nauka /etc/nauka && chmod 750 /etc/nauka",
    )
    .await?;

    // 5. Cluster identity → the env file, plus the join args. Token and
    //    keys are mutually exclusive on the CLI, so hand the target EXACTLY
    //    one. The token is preferred when both are somehow present (a token
    //    materializes a key dir locally, so the invoking process may hold
    //    both — the token is the source of truth).
    println!("  installing the cluster identity and unit…");
    let mut env = String::new();
    if let Some(token) = &o.token {
        env.push_str(&format!("NAUKA_TOKEN={token}\n"));
    } else if let Some(dir) = &o.keys_dir {
        for name in ["cluster-ca.key", "cluster-ca.pem"] {
            let p = dir.join(name);
            if p.exists() {
                scp_verified(&o, &p, &format!("/etc/nauka/{name}")).await?;
            }
        }
        ssh_run(
            &o,
            "chown root:nauka /etc/nauka/cluster-ca.* && chmod 640 /etc/nauka/cluster-ca.*",
        )
        .await?;
        env.push_str("NAUKA_KEYS_ARG=--keys /etc/nauka\n");
    }
    // Advertise its own address, and WAIT to be added rather than found a
    // cluster of its own.
    env.push_str(&format!("NAUKA_ARGS=--advertise {} --join\n", o.target));
    ssh_write_file(&o, "/etc/nauka/nauka.env", env.as_bytes(), "640").await?;
    ssh_run(&o, "chown root:nauka /etc/nauka/nauka.env").await?;

    // 6. systemd unit, pointed at /usr/local/bin, enabled and started.
    let unit = UNIT.replace("/usr/bin/nauka", "/usr/local/bin/nauka");
    ssh_write_file(
        &o,
        "/etc/systemd/system/nauka.service",
        unit.as_bytes(),
        "644",
    )
    .await?;
    ssh_run(
        &o,
        "systemctl daemon-reload && systemctl enable --now nauka",
    )
    .await?;

    // 7. Wait for it to be up, and read its Raft id from its own HTTP
    //    status — no cluster identity needed for that query, unlike
    //    shelling `node-info`, which would need the token in the remote
    //    environment.
    println!("  waiting for {ip} to come up…");
    let status_url = format!("http://{}:8080/api/status", o.target.ip());
    let mut node_id: Option<u64> = None;
    for _ in 0..15 {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let Ok(resp) = reqwest::get(&status_url).await else {
            continue;
        };
        let Ok(body) = resp.json::<serde_json::Value>().await else {
            continue;
        };
        if let Some(id) = body.get("self_node_id").and_then(|v| v.as_u64()) {
            node_id = Some(id);
            break;
        }
    }
    let node_id = node_id
        .context("the new node did not come up in time — check `journalctl -u nauka` on it")?;

    // 8. The Raft membership change, driven through an existing member.
    println!("  adding node {node_id} to the cluster…");
    join_member(&o.peers, node_id, o.target).await?;
    println!("done — {ip} is a voting member. Shard rebalancing follows on the next scrub passes.");
    Ok(())
}

/// AddLearner then promote to voter — the same two-step `cluster-add`
/// performs, reused so there is one join path in the codebase.
async fn join_member(peers: &[SocketAddr], id: u64, addr: SocketAddr) -> Result<()> {
    use nauka_raft::types::{AdminRequest, AdminResponse};
    match nauka_raft::admin_via_leader(
        peers,
        &AdminRequest::AddLearner {
            id,
            addr: addr.to_string(),
        },
    )
    .await?
    {
        AdminResponse::Ok(_) => {}
        other => bail!("add-learner: {other:?}"),
    }
    let current = match nauka_raft::admin_via_leader(peers, &AdminRequest::Metrics).await? {
        AdminResponse::Metrics { members, .. } => members,
        other => bail!("metrics: {other:?}"),
    };
    let mut ids: Vec<u64> = current.keys().copied().collect();
    if !ids.contains(&id) {
        ids.push(id);
    }
    match nauka_raft::admin_via_leader(peers, &AdminRequest::ChangeMembership(ids)).await? {
        AdminResponse::Ok(_) => Ok(()),
        other => bail!("change-membership: {other:?}"),
    }
}

pub struct RemoveOpts {
    pub node_id: u64,
    pub peers: Vec<SocketAddr>,
}

/// Drop a node from the voter set. Its shards are re-replicated by the
/// remaining nodes' scrubbers; the machine can be shut down afterward.
/// Does NOT touch the target over ssh — a removed node may be already dead,
/// and forcing an ssh round-trip would make removing a dead node fail.
pub async fn remove(o: RemoveOpts) -> Result<()> {
    use nauka_raft::types::{AdminRequest, AdminResponse};
    let current = match nauka_raft::admin_via_leader(&o.peers, &AdminRequest::Metrics).await? {
        AdminResponse::Metrics { members, .. } => members,
        other => bail!("metrics: {other:?}"),
    };
    let ids: Vec<u64> = current
        .keys()
        .copied()
        .filter(|i| *i != o.node_id)
        .collect();
    if ids.len() == current.len() {
        bail!("node {} is not a member of the cluster", o.node_id);
    }
    match nauka_raft::admin_via_leader(&o.peers, &AdminRequest::ChangeMembership(ids)).await? {
        AdminResponse::Ok(_) => {
            println!(
                "node {} removed — leave it running long enough for the scrubs to \
                 re-replicate its shards, then shut it down",
                o.node_id
            );
            Ok(())
        }
        other => bail!("change-membership: {other:?}"),
    }
}
