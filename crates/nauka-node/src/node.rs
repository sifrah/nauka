//! `nauka node add <ip>` / `node remove <ip>` — grow and shrink the cluster
//! from one command, run on any existing member.
//!
//! The design is deliberately the automation of what an operator would do
//! by hand: shell out to the system `ssh`, which already carries the
//! forwarded agent key, host-key policy and connection multiplexing. A
//! Rust SSH library would reimplement all of that, worse. `node add`
//! provisions the target over SSH — binary, systemd unit, cluster identity
//! — starts it in join-wait mode, then does the Raft membership change
//! locally against the cluster.

use std::io::IsTerminal;
use std::net::SocketAddr;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use console::style;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use tokio::io::AsyncWriteExt;

/// Step-by-step terminal UI: an animated spinner per step on a tty,
/// plain chronological lines everywhere else (CI logs, `ssh` without
/// `-t`, pipes) — progress display must never corrupt captured output.
struct Ui {
    fancy: Option<MultiProgress>,
}

struct Step {
    bar: Option<ProgressBar>,
    label: String,
}

impl Ui {
    fn new() -> Self {
        Self {
            fancy: std::io::stderr().is_terminal().then(MultiProgress::new),
        }
    }

    fn spinner_style() -> ProgressStyle {
        ProgressStyle::with_template("{spinner:.cyan} {msg}")
            .expect("static template")
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏", " "])
    }

    fn step(&self, label: &str) -> Step {
        let bar = self.fancy.as_ref().map(|mp| {
            let pb = mp.add(ProgressBar::new_spinner());
            pb.set_style(Self::spinner_style());
            pb.set_message(label.to_string());
            pb.enable_steady_tick(Duration::from_millis(80));
            pb
        });
        if bar.is_none() {
            eprintln!("  {label}…");
        }
        Step {
            bar,
            label: label.to_string(),
        }
    }

    /// A byte-progress step; falls back to one plain line off-tty.
    fn transfer(&self, label: &str, total: u64) -> Step {
        let bar = self.fancy.as_ref().map(|mp| {
            let pb = mp.add(ProgressBar::new(total));
            pb.set_style(
                ProgressStyle::with_template(
                    "{spinner:.cyan} {msg} {bar:24.cyan/238} {bytes}/{total_bytes} ({bytes_per_sec})",
                )
                .expect("static template")
                .progress_chars("━╸ ")
                .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏", " "]),
            );
            pb.set_message(label.to_string());
            pb.enable_steady_tick(Duration::from_millis(80));
            pb
        });
        if bar.is_none() {
            eprintln!("  {label}…");
        }
        Step {
            bar,
            label: label.to_string(),
        }
    }
}

impl Step {
    /// Finish the step, replacing the animation with a green check mark.
    fn done(self) {
        let label = self.label.clone();
        self.done_as(&label);
    }

    /// Finish with a different closing line than the running label.
    fn done_as(mut self, text: &str) {
        if let Some(pb) = self.bar.take() {
            pb.set_style(ProgressStyle::with_template("{msg}").expect("static template"));
            pb.finish_with_message(format!("{} {text}", style("✓").green().bold()));
        } else if text != self.label {
            eprintln!("  {text}");
        }
    }
}

/// A step dropped without `done()` died on an error: leave a red mark
/// where the spinner was, so the failed step is identifiable after the
/// anyhow chain prints below the bars.
impl Drop for Step {
    fn drop(&mut self) {
        if let Some(pb) = self.bar.take() {
            if !pb.is_finished() {
                pb.set_style(ProgressStyle::with_template("{msg}").expect("static template"));
                pb.abandon_with_message(format!(
                    "{} {}",
                    style("✗").red().bold(),
                    self.label.clone()
                ));
            }
        }
    }
}

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

/// Options for every ssh/scp of the provisioning. accept-new and not the
/// default ask-and-refuse: the target of `node add` is typically a fresh
/// machine whose host key nobody has seen yet, and under BatchMode the
/// prompt degrades into a bare failure. First contact pins the key; a
/// key that CHANGES afterwards still refuses loudly.
const SSH_OPTS: &[&str] = &[
    "-o",
    "BatchMode=yes",
    "-o",
    "ConnectTimeout=15",
    "-o",
    "StrictHostKeyChecking=accept-new",
];

/// Run a command on the target over ssh, capturing output. The forwarded
/// agent and host-key policy come from the caller's ssh environment.
async fn ssh_run(o: &AddOpts, script: &str) -> Result<String> {
    // The script is one remote-shell argument. Passing `sh -c <script>` as
    // separate ssh args would make the remote sh treat the script's first
    // word as $0 and drop it — ssh already runs its single command argument
    // through the login shell, so hand it the script directly.
    let out = tokio::process::Command::new("ssh")
        .args(SSH_OPTS)
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
        .args(SSH_OPTS)
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

/// Push bytes to a file on the target through ssh and verify they landed
/// intact — transfers to fresh cloud instances truncate silently often
/// enough that an unchecked copy is a real hazard. Streams in chunks so a
/// progress bar can follow along.
async fn ssh_push_bytes(
    o: &AddOpts,
    remote: &str,
    bytes: &[u8],
    mode: &str,
    bar: Option<&ProgressBar>,
) -> Result<()> {
    use sha2::{Digest, Sha256};
    let want = hex::encode(Sha256::digest(bytes));
    for _attempt in 1..=3 {
        if let Some(b) = bar {
            b.set_position(0);
        }
        let mut child = tokio::process::Command::new("ssh")
            .args(SSH_OPTS)
            .arg(ssh_host(o))
            .arg(format!("cat > {remote} && chmod {mode} {remote}"))
            .stdin(Stdio::piped())
            .spawn()
            .context("running ssh for a file push")?;
        {
            let stdin = child.stdin.as_mut().expect("stdin piped");
            let mut pushed = true;
            for chunk in bytes.chunks(256 * 1024) {
                if stdin.write_all(chunk).await.is_err() {
                    // A dropped connection mid-stream is a retry, not an
                    // abort — same rationale as a truncated copy.
                    pushed = false;
                    break;
                }
                if let Some(b) = bar {
                    b.inc(chunk.len() as u64);
                }
            }
            if !pushed {
                let _ = child.wait().await;
                continue;
            }
        }
        drop(child.stdin.take());
        if !child.wait().await?.success() {
            continue;
        }
        let got = ssh_run(o, &format!("sha256sum {remote} | cut -d' ' -f1"))
            .await?
            .trim()
            .to_string();
        if got == want {
            return Ok(());
        }
    }
    bail!(
        "could not copy {remote} to {} intact after 3 tries",
        o.target.ip()
    )
}

pub async fn add(o: AddOpts) -> Result<()> {
    // The identity to hand the target. Exactly one of token/keys must be
    // present — it is how the invoking member itself authenticates.
    if o.token.is_none() && o.keys_dir.is_none() {
        bail!("no cluster identity to give the new node: set NAUKA_TOKEN or pass --keys");
    }
    let ip = o.target.ip();
    let ui = Ui::new();
    eprintln!(
        "{} {}",
        style("nauka").bold(),
        style(format!("— adding {ip} to the cluster")).dim()
    );

    // 1. Reachable?
    let step = ui.step(&format!("connecting to {}@{ip}", o.ssh_user));
    ssh_run(&o, "true")
        .await
        .with_context(|| format!("cannot ssh to {}@{ip}", o.ssh_user))?;
    step.done_as(&format!("connected to {}@{ip}", o.ssh_user));

    // 2. Blank data dir, or --force. A target that already holds Raft state
    //    belongs to a cluster; founding a join on top would either fork or
    //    resume the wrong cluster. Refuse loudly.
    // Any non-empty redb in the raft dir means this node already has
    // consensus state — match by glob, not by a hardcoded filename, so a
    // future rename of the log file cannot silently defeat the check.
    let step = ui.step("checking the target is blank");
    let state = ssh_run(
        &o,
        "ls -1 /var/lib/nauka/raft/*.redb 2>/dev/null | head -1 | grep -q . \
         && echo HAS_STATE || echo BLANK",
    )
    .await?;
    if state.trim() == "HAS_STATE" {
        if o.force {
            ssh_run(
                &o,
                "systemctl stop nauka 2>/dev/null; rm -rf /var/lib/nauka/*",
            )
            .await?;
            step.done_as("existing data dir wiped (--force)");
        } else {
            // A target that already runs a node of THIS cluster is not an
            // error to bounce off — `node add` converges: whatever id the
            // machine currently runs under becomes the one voter at that
            // address (a learner stuck mid-join gets promoted, stale
            // same-address identities get evicted). Only then comes the
            // hard refusal: state from some OTHER cluster is not this
            // command's to wipe. And never suggest --force for a healthy
            // member — that wipes its shards and swaps its identity,
            // which is the exact accident this check exists to prevent.
            let machine = ssh_run(
                &o,
                "curl -sf --max-time 5 http://127.0.0.1:8080/api/status 2>/dev/null || true",
            )
            .await
            .ok()
            .and_then(|body| serde_json::from_str::<serde_json::Value>(&body).ok());
            let machine_id = machine
                .as_ref()
                .and_then(|v| v.get("self_node_id").and_then(|i| i.as_u64()));
            // A node that answers with NO leader and no peers is not "in a
            // cluster" at all — it started once (which alone creates raft
            // files, hence HAS_STATE) and is waiting to be added. That is
            // this command's happy path, not a refusal: re-provision it in
            // place, keeping its identity.
            let machine_waiting = machine.as_ref().is_some_and(|v| {
                v.get("leader").is_some_and(|l| l.is_null())
                    && v.get("nodes")
                        .and_then(|n| n.as_array())
                        .is_none_or(|n| n.len() <= 1)
            });
            let members = current_members(&o.peers).await.unwrap_or_default();
            match machine_id {
                Some(id) if members.contains_key(&id) => {
                    step.done_as(&format!(
                        "{ip} already runs node {id} of this cluster — converging its membership"
                    ));
                    let step = ui.step("ensuring it is the one voter at this address");
                    join_member(&o.peers, id, o.target).await?;
                    step.done_as("membership converged");
                    eprintln!(
                        "\n{}",
                        style(format!("{ip} is a voting member of the cluster.")).bold()
                    );
                    return Ok(());
                }
                Some(_) if machine_waiting => {
                    step.done_as(&format!(
                        "{ip} runs an unjoined node waiting to be added — provisioning in place"
                    ));
                }
                Some(id) => bail!(
                    "{ip} runs node {id}, which belongs to another cluster (it has its own \
                     leader or peers). Wiping it is not this command's call; if you are \
                     sure, re-run with --force."
                ),
                None => match members.values().any(|a| *a == o.target.to_string()) {
                    true => bail!(
                        "{ip} holds cluster state and is registered here, but its node is \
                         not answering. Check it first: `systemctl status nauka` on the \
                         machine. To retire it instead: `nauka node remove <id>` \
                         (ids: `nauka status`)."
                    ),
                    false => bail!(
                        "{ip} holds cluster state but is NOT a member here — it likely \
                         belongs to another cluster. If you are sure it is fair game, \
                         re-run with --force to wipe and add it."
                    ),
                },
            }
        }
    } else {
        step.done_as("target is blank");
    }

    // 3. The binary this very process is running — a static musl build runs
    //    on any x86_64 Linux, so the common same-fleet case needs no
    //    cross-compilation. Landed under a temp name and renamed into
    //    place: overwriting a RUNNING binary directly fails with ETXTBSY
    //    (a re-add of a live node, or a retry); rename is atomic and
    //    unlinks the busy inode safely.
    let self_exe = std::env::current_exe().context("finding my own binary to deploy")?;
    let binary =
        std::fs::read(&self_exe).with_context(|| format!("reading {}", self_exe.display()))?;
    ssh_run(&o, "mkdir -p /var/lib/nauka /etc/nauka").await?;
    let step = ui.transfer("copying the nauka binary", binary.len() as u64);
    ssh_push_bytes(
        &o,
        "/usr/local/bin/nauka.new",
        &binary,
        "755",
        step.bar.as_ref(),
    )
    .await?;
    ssh_run(&o, "mv /usr/local/bin/nauka.new /usr/local/bin/nauka").await?;
    step.done_as(&format!(
        "binary installed ({})",
        indicatif::HumanBytes(binary.len() as u64)
    ));

    // 4. Dedicated user + directory ownership (mirrors the deb postinst).
    let step = ui.step("installing the cluster identity and unit");
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
    let mut env = String::new();
    if let Some(token) = &o.token {
        env.push_str(&format!("NAUKA_TOKEN={token}\n"));
    } else if let Some(dir) = &o.keys_dir {
        for name in ["cluster-ca.key", "cluster-ca.pem"] {
            let p = dir.join(name);
            if p.exists() {
                let bytes =
                    std::fs::read(&p).with_context(|| format!("reading {}", p.display()))?;
                ssh_push_bytes(&o, &format!("/etc/nauka/{name}"), &bytes, "640", None).await?;
            }
        }
        ssh_run(&o, "chown root:nauka /etc/nauka/cluster-ca.*").await?;
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
    step.done();

    // 7. Wait for it to be up, and read its Raft id from its own HTTP
    //    status — no cluster identity needed for that query, unlike
    //    shelling `node-info`, which would need the token in the remote
    //    environment.
    let step = ui.step(&format!("waiting for {ip} to come up"));
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
    step.done_as(&format!("node {node_id} is up"));

    // 8. The Raft membership change, driven through an existing member.
    let step = ui.step("joining the Raft cluster (learner → voter)");
    join_member(&o.peers, node_id, o.target).await?;
    step.done_as("joined the Raft cluster as a voting member");

    eprintln!(
        "\n{} {}",
        style(format!("{ip} is a member of the cluster.")).bold(),
        style("Shards rebalance over the next scrub passes.").dim()
    );
    Ok(())
}

/// Current membership (id → address) as the leader reports it.
async fn current_members(peers: &[SocketAddr]) -> Result<std::collections::BTreeMap<u64, String>> {
    use nauka_raft::types::{AdminRequest, AdminResponse};
    match nauka_raft::admin_via_leader(peers, &AdminRequest::Metrics).await? {
        AdminResponse::Metrics { members, .. } => Ok(members),
        other => bail!("metrics: {other:?}"),
    }
}

/// AddLearner then promote to voter, one join path in the codebase.
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
    let current = current_members(peers).await?;
    // A member already registered at the joining node's ADDRESS is the
    // machine being replaced: a wipe regenerates node.key, so the same
    // machine comes back under a fresh id. Keeping the old id would leave
    // a phantom voter that inflates quorum forever — and reads alive
    // forever, because liveness is probed per address and the NEW node
    // answers that address. Evict it in the same membership change.
    let addr_str = addr.to_string();
    let mut ids: Vec<u64> = Vec::new();
    for (mid, maddr) in &current {
        if *mid != id && *maddr == addr_str {
            eprintln!(
                "{}",
                style(format!(
                    "  evicting stale member {mid} — same address, replaced identity"
                ))
                .yellow()
            );
            continue;
        }
        ids.push(*mid);
    }
    if !ids.contains(&id) {
        ids.push(id);
    }
    // The add-learner above is itself a membership entry, and openraft
    // refuses a second change until it commits. On a healthy cluster that
    // is milliseconds — retry through the transient instead of surfacing
    // it. Bounded: a cluster that cannot commit the learner entry within
    // 30 s has a real quorum problem the operator must hear about.
    let mut last_err: Option<anyhow::Error> = None;
    for _ in 0..15 {
        match nauka_raft::admin_via_leader(peers, &AdminRequest::ChangeMembership(ids.clone()))
            .await
        {
            Ok(AdminResponse::Ok(_)) => {
                // Clear any stale `disabled` flag at this address before
                // the node takes traffic. The flag is keyed by address and
                // outlives a `node remove`, so a machine returning at an
                // address someone once drained — or a cloud IP reused by a
                // new instance — would otherwise rejoin pre-disabled and
                // silently take no shards. Joining means active.
                let _ = nauka_raft::write_via_leader(
                    peers,
                    nauka_raft::types::AppCommand::SetNodeDisabled {
                        addr: addr.to_string(),
                        disabled: false,
                    },
                )
                .await;
                return Ok(());
            }
            Ok(other) => bail!("change-membership: {other:?}"),
            Err(e) if e.to_string().contains("configuration change") => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(e) => return Err(e),
        }
    }
    Err(last_err
        .unwrap_or_else(|| anyhow::anyhow!("change-membership kept being refused"))
        .context(
            "the cluster could not commit the membership change within 30 s — it likely \
             lacks a quorum of LIVE voters (check `nauka status` for members marked down \
             or sharing an address)",
        ))
}

/// The removal-safety verdict from a node's `/api/removal-check`.
#[derive(serde::Deserialize)]
pub struct RemovalSafety {
    pub k: usize,
    pub reliable_nodes: usize,
    pub safe: bool,
    pub at_risk: usize,
    /// Files below k shards even counting every reachable disk — dead
    /// before this removal, not because of it. Default keeps the client
    /// compatible with a node that predates the field.
    #[serde(default)]
    pub already_lost: usize,
    pub reason: String,
    pub sample: Vec<RemovalSampleFile>,
}

#[derive(serde::Deserialize)]
pub struct RemovalSampleFile {
    pub hash: String,
    pub name: Option<String>,
    pub shards_left: usize,
}

/// Ask a node whether removing/draining `target` would leave any file
/// unrecoverable. Tries each peer's HTTP API on the conventional 8080.
/// None means no node could answer — an unknowable verdict the caller
/// must treat as a reason to stop, not to proceed.
pub async fn removal_safety(peers: &[SocketAddr], target: SocketAddr) -> Option<RemovalSafety> {
    let client = reqwest::Client::new();
    for peer in peers {
        let url = format!(
            "http://{}:8080/api/removal-check?target={}",
            peer.ip(),
            target
        );
        if let Ok(resp) = client
            .get(&url)
            .timeout(Duration::from_secs(10))
            .send()
            .await
        {
            if let Ok(v) = resp.json::<RemovalSafety>().await {
                return Some(v);
            }
        }
    }
    None
}

/// Print the verdict; unless `force`, refuse an unsafe or unverifiable
/// action. This is the guard that stops a stray removal from deleting the
/// last copies of a file.
pub fn guard_removal(
    action: &str,
    target: SocketAddr,
    v: &Option<RemovalSafety>,
    force: bool,
) -> Result<()> {
    match v {
        Some(s) if s.safe => {
            eprintln!(
                "{} safe — every file keeps at least {} shards on {} other reliable node(s)",
                style("✓").green().bold(),
                s.k,
                s.reliable_nodes
            );
            if s.already_lost > 0 {
                eprintln!(
                    "{}",
                    style(format!(
                        "  note: {} file(s) are already unrecoverable on every disk — \
                         not because of this removal. `nauka rm` them to clear the alarm.",
                        s.already_lost
                    ))
                    .yellow()
                );
            }
            Ok(())
        }
        Some(s) => {
            eprintln!(
                "{} {action} {target} is UNSAFE: {}",
                style("✗").red().bold(),
                s.reason
            );
            for f in s.sample.iter().take(6) {
                eprintln!(
                    "    {} {} — would keep only {}/{} shards",
                    &f.hash[..f.hash.len().min(16)],
                    f.name.as_deref().unwrap_or("—"),
                    f.shards_left,
                    s.k
                );
            }
            if s.at_risk > s.sample.len() {
                eprintln!("    … and {} more file(s)", s.at_risk - s.sample.len());
            }
            if force {
                eprintln!(
                    "{}",
                    style("  --force: proceeding despite the risk of permanent data loss").red()
                );
                Ok(())
            } else {
                bail!(
                    "refused, to protect your data. Drain the node first \
                     (`nauka node disable {target}`, watch it empty in `nauka top`), bring any \
                     down node back online, or pass --force to override."
                )
            }
        }
        None => {
            if force {
                eprintln!(
                    "{}",
                    style("  could not verify safety (no node answered) — --force, proceeding")
                        .yellow()
                );
                Ok(())
            } else {
                bail!(
                    "could not reach any node to check whether this is safe — retry, or pass \
                     --force to proceed without the check."
                )
            }
        }
    }
}

pub struct RemoveOpts {
    pub node_id: u64,
    pub peers: Vec<SocketAddr>,
    /// Override the safety pre-flight (data-loss risk accepted).
    pub force: bool,
}

/// Drop a node from the voter set. Its shards are re-replicated by the
/// remaining nodes' scrubbers; the machine can be shut down afterward.
/// Does NOT touch the target over ssh — a removed node may be already dead,
/// and forcing an ssh round-trip would make removing a dead node fail.
pub async fn remove(o: RemoveOpts) -> Result<()> {
    use nauka_raft::types::{AdminRequest, AdminResponse};
    let ui = Ui::new();
    let current = match nauka_raft::admin_via_leader(&o.peers, &AdminRequest::Metrics).await? {
        AdminResponse::Metrics { members, .. } => members,
        other => bail!("metrics: {other:?}"),
    };
    let removed_addr = current.get(&o.node_id).cloned();
    let ids: Vec<u64> = current
        .keys()
        .copied()
        .filter(|i| *i != o.node_id)
        .collect();
    if ids.len() == current.len() {
        bail!("node {} is not a member of the cluster", o.node_id);
    }
    // Pre-flight: would removing this node leave any file with fewer than
    // k shards on the nodes that stay? Removal is the destructive one —
    // the node's copies leave the cluster with it — so this runs before
    // the membership change, and refuses unless --force.
    if let Some(addr) = &removed_addr {
        if let Ok(target) = addr.parse::<SocketAddr>() {
            let verdict = removal_safety(&o.peers, target).await;
            guard_removal("removing", target, &verdict, o.force)?;
        }
    }
    let step = ui.step(&format!("removing node {} from the membership", o.node_id));
    match nauka_raft::admin_via_leader(&o.peers, &AdminRequest::ChangeMembership(ids)).await? {
        AdminResponse::Ok(_) => {
            // Don't leave a `disabled` entry behind for an address that is
            // no longer a member — it would silently pre-disable whatever
            // rejoins there (a returning machine, a reused cloud IP).
            if let Some(addr) = removed_addr {
                let _ = nauka_raft::write_via_leader(
                    &o.peers,
                    nauka_raft::types::AppCommand::SetNodeDisabled {
                        addr,
                        disabled: false,
                    },
                )
                .await;
            }
            step.done_as(&format!("node {} removed", o.node_id));
            eprintln!(
                "{}",
                style(
                    "Leave it running long enough for the scrubs to re-replicate \
                     its shards, then shut it down."
                )
                .dim()
            );
            Ok(())
        }
        other => bail!("change-membership: {other:?}"),
    }
}

/// `nauka status` — the cluster as one node's HTTP API reports it. Plain
/// HTTP on purpose: works from anywhere that can reach a node, without
/// the cluster identity, which is exactly what a quick health check needs.
pub async fn status(api: &str, json: bool) -> Result<()> {
    #[derive(serde::Deserialize)]
    struct Node {
        addr: String,
        /// default: a pre-0.6 node does not serve the field yet.
        #[serde(default)]
        id: Option<u64>,
        #[serde(default)]
        disabled: bool,
        capacity_bytes: u64,
        is_leader: bool,
        is_self: bool,
        is_alive: bool,
    }
    #[derive(serde::Deserialize)]
    struct Status {
        leader: Option<String>,
        nodes: Vec<Node>,
        files: usize,
        total_bytes: u64,
    }

    let url = format!("{}/api/status", api.trim_end_matches('/'));
    let body = reqwest::Client::new()
        .get(&url)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .with_context(|| format!("no node answering at {url}"))?
        .error_for_status()?
        .text()
        .await
        .context("reading the status payload")?;
    if json {
        // Raw passthrough, exactly what the node said: the machine-facing
        // face of this command must not re-encode what it reports on.
        println!("{}", body.trim_end());
        return Ok(());
    }
    let s: Status = serde_json::from_str(&body).context("unexpected status payload")?;

    let alive = s.nodes.iter().filter(|n| n.is_alive).count();
    let capacity: u64 = s.nodes.iter().map(|n| n.capacity_bytes).sum();
    println!(
        "{} — {} node{}, {} alive · {} file{}, {} stored · {} capacity",
        style("cluster").bold(),
        s.nodes.len(),
        if s.nodes.len() == 1 { "" } else { "s" },
        alive,
        s.files,
        if s.files == 1 { "" } else { "s" },
        indicatif::HumanBytes(s.total_bytes),
        indicatif::HumanBytes(capacity),
    );
    if s.leader.is_none() {
        println!("{}", style("no leader elected — cluster unavailable").red());
    }

    let width = s.nodes.iter().map(|n| n.addr.len()).max().unwrap_or(0);
    for n in &s.nodes {
        let health = if n.is_alive {
            style("●").green()
        } else {
            style("●").red()
        };
        let role = if n.disabled {
            style("draining").yellow().to_string()
        } else if n.is_leader {
            style("leader  ").cyan().to_string()
        } else {
            "        ".to_string()
        };
        let id = match n.id {
            Some(id) => id.to_string(),
            None => "joining…".to_string(),
        };
        // Two members on one address = a replaced machine whose previous
        // identity was never evicted. The liveness probe cannot tell them
        // apart (it pings the address, which the live one answers), so
        // this is the one place the phantom is visible — say it.
        let shared = s
            .nodes
            .iter()
            .filter(|m| m.addr == n.addr && m.id != n.id)
            .count()
            > 0;
        println!(
            "  {health} {addr:width$}  {role}  {cap:>10}  {id}{me}{warn}",
            addr = n.addr,
            cap = indicatif::HumanBytes(n.capacity_bytes).to_string(),
            id = style(id).dim(),
            me = if n.is_self {
                style("  (this node)").dim().to_string()
            } else {
                String::new()
            },
            warn = if shared {
                style("  ⚠ shares its address with another member — stale identity? (`nauka node remove <id>`)")
                    .yellow()
                    .to_string()
            } else {
                String::new()
            },
        );
    }
    Ok(())
}

/// The data dir of the systemd deployment, when this machine has one. The
/// unit sets NAUKA_DATA_DIR=/var/lib/nauka (Environment=), overridable in
/// the env file. Commands that inherit the service identity must speak
/// about the service's store, not about a ./nauka-data in their cwd —
/// `node-info` would otherwise print the identity of a store nobody
/// serves.
pub fn service_data_dir() -> Option<std::path::PathBuf> {
    let env = std::fs::read_to_string("/etc/nauka/nauka.env").ok()?;
    let dir = env
        .lines()
        .find_map(|l| {
            l.strip_prefix("NAUKA_DATA_DIR=")
                .map(|v| v.trim().to_string())
        })
        .unwrap_or_else(|| "/var/lib/nauka".to_string());
    Some(std::path::PathBuf::from(dir))
}

/// Flip a member's draining state. Disabling starts an automatic,
/// redundancy-safe evacuation: the node leaves the placement view, the
/// scrubbers migrate its shards to their new owners, and its own GC
/// releases each one once the owner has proven possession.
pub async fn set_disabled(peers: &[SocketAddr], target: SocketAddr, disabled: bool) -> Result<()> {
    let ui = Ui::new();
    let verb = if disabled { "disabling" } else { "enabling" };
    let step = ui.step(&format!("{verb} {target} in the replicated state"));
    // Refuse a typo'd address outright: draining an addr nobody has is a
    // silent no-op that reads as success.
    let members = current_members(peers).await?;
    if !members.values().any(|a| *a == target.to_string()) {
        bail!("{target} is not a member of this cluster (addresses: `nauka status`)");
    }
    let resp = nauka_raft::write_via_leader(
        peers,
        nauka_raft::types::AppCommand::SetNodeDisabled {
            addr: target.to_string(),
            disabled,
        },
    )
    .await?;
    if !resp.ok {
        bail!("the cluster refused the change");
    }
    if disabled {
        step.done_as(&format!("{target} is draining"));
        eprintln!(
            "{}",
            style(
                "It stays a member and keeps serving reads while the others take over its \
                 shards. Watch it empty in `nauka top`; at 0 B of live shards, `nauka node \
                 remove` is instant and safe."
            )
            .dim()
        );
        // Draining never loses data on its own — it only releases against
        // a proof. But it also cannot FINISH if the cluster is too small
        // to re-host this node's copies: warn so the operator is not left
        // wondering why it never reaches zero.
        if let Some(v) = removal_safety(peers, target).await {
            if !v.safe {
                eprintln!(
                    "{} {}",
                    style("note:").yellow().bold(),
                    style(format!(
                        "this node cannot fully drain right now — {} — its last copies of {} \
                         file(s) will stay until then.",
                        v.reason, v.at_risk
                    ))
                    .yellow()
                );
            }
        }
    } else {
        step.done_as(&format!("{target} is back in the placement view"));
        eprintln!(
            "{}",
            style("Shards will migrate back toward it over the next scrub passes.").dim()
        );
    }
    Ok(())
}

pub struct InitOpts {
    /// Address advertised to future members (default: the default-route
    /// address, port 7311).
    pub advertise: Option<SocketAddr>,
    /// Cluster token to install, if the operator already has one (a machine
    /// re-initialized into an existing cluster's identity). Default:
    /// generate a fresh one — this IS the birth of a new cluster.
    pub token: Option<String>,
}

/// Run a shell snippet locally, capturing stderr for the error message.
async fn local_run(script: &str) -> Result<String> {
    let out = tokio::process::Command::new("sh")
        .arg("-c")
        .arg(script)
        .output()
        .await
        .with_context(|| format!("running: {script}"))?;
    if !out.status.success() {
        bail!(
            "command failed: {script}: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// The address this machine reaches the outside world from. Connecting a
/// UDP socket sends no packet — it only asks the kernel which source
/// address the default route would pick.
fn default_route_ip() -> Result<std::net::IpAddr> {
    let s = std::net::UdpSocket::bind("0.0.0.0:0").context("binding a probe socket")?;
    s.connect("1.1.1.1:80").context("no default route")?;
    Ok(s.local_addr()?.ip())
}

/// Identity left behind by `nauka init`, so that `node add` run on the
/// same machine works without re-exporting the token. Returns
/// (token, keys_dir) — at most one is Some, mirroring the CLI exclusivity.
pub fn identity_from_env_file() -> (Option<String>, Option<std::path::PathBuf>) {
    let Ok(env) = std::fs::read_to_string("/etc/nauka/nauka.env") else {
        return (None, None);
    };
    for line in env.lines() {
        if let Some(token) = line.strip_prefix("NAUKA_TOKEN=") {
            if !token.trim().is_empty() {
                return (Some(token.trim().to_string()), None);
            }
        }
        if let Some(dir) = line.strip_prefix("NAUKA_KEYS_ARG=--keys ") {
            if !dir.trim().is_empty() {
                return (None, Some(std::path::PathBuf::from(dir.trim())));
            }
        }
    }
    (None, None)
}

/// `nauka init` — turn this machine into the first node of a cluster:
/// the local counterpart of what `add` does over SSH. Dedicated user,
/// cluster identity in /etc/nauka, hardened systemd unit enabled and
/// started. The service founds a single-node cluster on its blank data
/// dir; the cluster then grows with `nauka node add`.
pub async fn init(o: InitOpts) -> Result<()> {
    // Guards first, before touching anything. This command exists for
    // servers; a laptop gets a clear refusal, not half a system service.
    if !cfg!(target_os = "linux") {
        bail!(
            "`nauka init` sets up a systemd service — Linux only.\n\
             Run a node by hand instead: NAUKA_TOKEN=$(nauka token) nauka serve"
        );
    }
    if !std::path::Path::new("/run/systemd/system").is_dir() {
        bail!("systemd is not running on this machine — run a node by hand: nauka serve");
    }
    // geteuid, not a $USER string: sudo keeps USER=root only sometimes.
    if unsafe { libc::geteuid() } != 0 {
        bail!("`nauka init` writes system directories — run it as root (sudo nauka init)");
    }
    if std::path::Path::new("/etc/systemd/system/nauka.service").exists() {
        bail!(
            "this machine already runs a nauka service (`systemctl status nauka`).\n\
             To reconfigure it, edit /etc/nauka/nauka.env and `systemctl restart nauka`."
        );
    }
    // Same refusal as `add`: existing Raft state means this machine already
    // belongs to a cluster, and re-founding on top would fork it.
    let has_state = std::fs::read_dir("/var/lib/nauka/raft")
        .map(|d| {
            d.flatten()
                .any(|e| e.path().extension().is_some_and(|x| x == "redb"))
        })
        .unwrap_or(false);
    if has_state {
        bail!(
            "/var/lib/nauka already holds cluster state — this machine is a member already.\n\
             To restart it: systemctl start nauka. To wipe it, remove /var/lib/nauka first."
        );
    }

    let advertise = match o.advertise {
        Some(a) => a,
        None => {
            let ip = default_route_ip()
                .context("could not detect this machine's address — pass --advertise <ip>:7311")?;
            SocketAddr::new(ip, 7311)
        }
    };
    if advertise.ip().is_loopback() {
        bail!("the detected address is loopback — pass --advertise <public-ip>:7311");
    }

    // 1. Dedicated user + directories (same commands `add` runs remotely,
    //    same layout as the deb postinst).
    println!("initializing the first node of a cluster on this machine…");
    local_run(
        "getent passwd nauka >/dev/null || adduser --system --group --no-create-home \
         --home /var/lib/nauka --gecos 'Nauka storage node' nauka >/dev/null",
    )
    .await?;
    std::fs::create_dir_all("/var/lib/nauka").context("creating /var/lib/nauka")?;
    std::fs::create_dir_all("/etc/nauka").context("creating /etc/nauka")?;
    local_run(
        "chown nauka:nauka /var/lib/nauka && chmod 750 /var/lib/nauka && \
         chown root:nauka /etc/nauka && chmod 750 /etc/nauka",
    )
    .await?;

    // 2. The binary must live outside /home and /root: the unit runs with
    //    ProtectHome=true, which would hide a home-dir binary from the
    //    service. Copy under a temp name and rename into place — the same
    //    ETXTBSY dodge as `add`, for re-init after a wipe while an old
    //    process lingers.
    let self_exe = std::env::current_exe()
        .and_then(|p| p.canonicalize())
        .context("finding my own binary")?;
    let system_bin = std::path::Path::new("/usr/local/bin/nauka");
    let exec_path = if self_exe == system_bin || self_exe == std::path::Path::new("/usr/bin/nauka")
    {
        self_exe
    } else {
        std::fs::copy(&self_exe, "/usr/local/bin/nauka.new")
            .context("copying the binary to /usr/local/bin")?;
        local_run(
            "chmod 755 /usr/local/bin/nauka.new && mv /usr/local/bin/nauka.new /usr/local/bin/nauka",
        )
        .await?;
        system_bin.to_path_buf()
    };

    // 3. Cluster identity → the env file, 640 root:nauka like everything
    //    else under /etc/nauka. The token never goes through argv.
    let token = o.token.unwrap_or_else(nauka_transport::generate_token);
    let env = format!("NAUKA_TOKEN={token}\nNAUKA_ARGS=--advertise {advertise}\n");
    std::fs::write("/etc/nauka/nauka.env", env).context("writing /etc/nauka/nauka.env")?;
    local_run("chown root:nauka /etc/nauka/nauka.env && chmod 640 /etc/nauka/nauka.env").await?;

    // 4. The unit, pointed at wherever the binary actually is, enabled so
    //    it comes back on reboot, started now. No --join: the first serve
    //    on a blank data dir founds the cluster.
    let unit = UNIT.replace("/usr/bin/nauka", &exec_path.to_string_lossy());
    std::fs::write("/etc/systemd/system/nauka.service", unit)
        .context("writing the systemd unit")?;
    local_run("systemctl daemon-reload && systemctl enable --now nauka").await?;

    // 5. Wait for the node to answer, same probe as `add` (15 × 2 s).
    println!("  waiting for the node to come up…");
    let mut node_id: Option<u64> = None;
    for _ in 0..15 {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let Ok(resp) = reqwest::get("http://127.0.0.1:8080/api/status").await else {
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
    let node_id =
        node_id.context("the node did not come up in time — check `journalctl -u nauka`")?;

    println!("\ncluster founded — this machine is node {node_id}");
    println!("  service : nauka.service (enabled: restarts on failure and on reboot)");
    println!("  data    : /var/lib/nauka");
    println!("  config  : /etc/nauka/nauka.env");
    println!("  api     : http://{}:8080", advertise.ip());
    println!("  token   : {token}");
    println!("            anyone holding the token is a member — treat it like a password");
    println!("\ngrow the cluster from this machine:");
    println!("  nauka node add <ip>:7311");
    Ok(())
}

// ── Organisations and spaces ─────────────────────────────────────────
// The engine's multi-tenant registry (see AUTH series). All writes go
// through the Raft leader like every other admin command; reads go
// through any node's HTTP API. An organisation is an APPLICATION — its
// end users never appear here.

/// Lowercase letters, digits, dashes, 1–32 chars: names end up in URLs
/// and in the replicated state, so they are locked down from day one.
fn validate_slug(kind: &str, s: &str) -> Result<()> {
    let ok = !s.is_empty()
        && s.len() <= 32
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && !s.starts_with('-')
        && !s.ends_with('-');
    if !ok {
        bail!("invalid {kind} name {s:?}: lowercase letters, digits and dashes, 1-32 chars");
    }
    Ok(())
}

/// Splits and validates a full `org/name` space path.
fn split_space_path(name: &str) -> Result<(&str, &str)> {
    let (org, space) = name
        .split_once('/')
        .with_context(|| format!("expected org/name, got {name:?} (e.g. yogfile/uploads)"))?;
    validate_slug("organisation", org)?;
    validate_slug("space", space)?;
    Ok((org, space))
}

/// The `/api/orgs` view: both maps, exactly as replicated.
#[derive(serde::Deserialize)]
struct OrgsView {
    orgs: std::collections::BTreeMap<String, nauka_raft::types::OrgRecord>,
    spaces: std::collections::BTreeMap<String, nauka_raft::types::SpaceRecord>,
    #[serde(default)]
    usage: std::collections::BTreeMap<String, UsageRow>,
}

#[derive(serde::Deserialize, Default, Clone, Copy)]
struct UsageRow {
    #[serde(default)]
    storage_bytes: u64,
    #[serde(default)]
    egress_month_bytes: u64,
}

/// Reads the replicated org/space registry from the first peer whose
/// HTTP API answers.
async fn fetch_orgs(peers: &[SocketAddr]) -> Result<OrgsView> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    for peer in peers {
        let url = format!("http://{}:8080/api/orgs", peer.ip());
        if let Ok(resp) = client.get(&url).send().await {
            if let Ok(view) = resp.json::<OrgsView>().await {
                return Ok(view);
            }
        }
    }
    bail!("no node answered /api/orgs (tried {} peer(s))", peers.len());
}

async fn write_command(
    peers: &[SocketAddr],
    cmd: nauka_raft::types::AppCommand,
) -> Result<nauka_raft::types::AppResponse> {
    let resp = nauka_raft::write_via_leader(peers, cmd).await?;
    if !resp.ok {
        bail!(
            "{}",
            resp.info
                .as_deref()
                .unwrap_or("the cluster refused the change")
        );
    }
    Ok(resp)
}

pub async fn org_create(peers: &[SocketAddr], name: &str) -> Result<()> {
    validate_slug("organisation", name)?;
    let view = fetch_orgs(peers).await?;
    if view.orgs.contains_key(name) {
        bail!("organisation {name} already exists");
    }
    write_command(
        peers,
        nauka_raft::types::AppCommand::UpsertOrg {
            name: name.to_string(),
            record: Default::default(),
        },
    )
    .await?;
    println!("organisation {name} created");
    println!("  next: nauka space create {name}/<space>");
    Ok(())
}

pub async fn org_set_suspended(peers: &[SocketAddr], name: &str, suspended: bool) -> Result<()> {
    let view = fetch_orgs(peers).await?;
    let mut record = view
        .orgs
        .get(name)
        .cloned()
        .with_context(|| format!("no organisation named {name}"))?;
    record.suspended = suspended;
    write_command(
        peers,
        nauka_raft::types::AppCommand::UpsertOrg {
            name: name.to_string(),
            record,
        },
    )
    .await?;
    if suspended {
        println!("organisation {name} SUSPENDED — all its spaces are dark cluster-wide");
    } else {
        println!("organisation {name} active again");
    }
    Ok(())
}

pub async fn org_delete(peers: &[SocketAddr], name: &str) -> Result<()> {
    write_command(
        peers,
        nauka_raft::types::AppCommand::DeleteOrg {
            name: name.to_string(),
        },
    )
    .await?;
    println!("organisation {name} deleted");
    Ok(())
}

pub async fn space_create(peers: &[SocketAddr], name: &str, public: bool) -> Result<()> {
    let (org, _) = split_space_path(name)?;
    let view = fetch_orgs(peers).await?;
    if view.spaces.contains_key(name) {
        bail!("space {name} already exists");
    }
    if !view.orgs.contains_key(org) {
        bail!("no organisation named {org} — create it first: nauka org create {org}");
    }
    write_command(
        peers,
        nauka_raft::types::AppCommand::UpsertSpace {
            name: name.to_string(),
            record: nauka_raft::types::SpaceRecord {
                org: org.to_string(),
                public_read: public,
                ..Default::default()
            },
        },
    )
    .await?;
    let visibility = if public { "public-read" } else { "private" };
    println!("space {name} created ({visibility})");
    Ok(())
}

pub async fn space_set_suspended(peers: &[SocketAddr], name: &str, suspended: bool) -> Result<()> {
    let view = fetch_orgs(peers).await?;
    let mut record = view
        .spaces
        .get(name)
        .cloned()
        .with_context(|| format!("no space named {name}"))?;
    record.suspended = suspended;
    write_command(
        peers,
        nauka_raft::types::AppCommand::UpsertSpace {
            name: name.to_string(),
            record,
        },
    )
    .await?;
    if suspended {
        println!("space {name} SUSPENDED cluster-wide");
    } else {
        println!("space {name} active again");
    }
    Ok(())
}

pub async fn space_delete(peers: &[SocketAddr], name: &str) -> Result<()> {
    write_command(
        peers,
        nauka_raft::types::AppCommand::DeleteSpace {
            name: name.to_string(),
        },
    )
    .await?;
    println!("space {name} deleted");
    Ok(())
}

/// `org list` and `space list`: one tree, orgs then their spaces —
/// filtered to one org when asked.
pub async fn org_list(peers: &[SocketAddr], only_org: Option<&str>) -> Result<()> {
    let view = fetch_orgs(peers).await?;
    let orgs: Vec<(&String, &nauka_raft::types::OrgRecord)> = view
        .orgs
        .iter()
        .filter(|(name, _)| only_org.is_none_or(|o| o == name.as_str()))
        .collect();
    if orgs.is_empty() {
        match only_org {
            Some(o) => println!("no organisation named {o}"),
            None => println!("no organisations yet — nauka org create <name>"),
        }
        return Ok(());
    }
    for (name, org) in orgs {
        let status = if org.suspended {
            style("SUSPENDED").red().bold().to_string()
        } else {
            style("active").green().to_string()
        };
        println!("{} — {status}", style(name).bold());
        let mut any = false;
        for (path, s) in view.spaces.iter().filter(|(_, s)| s.org == *name) {
            any = true;
            let mut tags: Vec<&str> = Vec::new();
            if s.suspended {
                tags.push("SUSPENDED");
            }
            tags.push(if s.public_read {
                "public-read"
            } else {
                "private"
            });
            println!("  {path}  [{}]", tags.join(", "));
        }
        if !any {
            println!("  (no spaces)");
        }
    }
    Ok(())
}

// ── Space keys ───────────────────────────────────────────────────────

/// One key as `/api/orgs` serves it (public material only).
#[derive(serde::Deserialize)]
struct KeyView {
    public_key: String,
    role: String,
    name: String,
}

/// The `/api/orgs` view including keys (additive to [`OrgsView`]; kept
/// separate so old nodes' answers still parse for the org commands).
#[derive(serde::Deserialize)]
struct OrgsKeysView {
    #[serde(default)]
    space_keys: std::collections::BTreeMap<String, Vec<KeyView>>,
}

async fn fetch_space_keys(
    peers: &[SocketAddr],
) -> Result<std::collections::BTreeMap<String, Vec<KeyView>>> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    for peer in peers {
        let url = format!("http://{}:8080/api/orgs", peer.ip());
        if let Ok(resp) = client.get(&url).send().await {
            if let Ok(view) = resp.json::<OrgsKeysView>().await {
                return Ok(view.space_keys);
            }
        }
    }
    bail!("no node answered /api/orgs (tried {} peer(s))", peers.len());
}

fn parse_role(role: &str) -> Result<nauka_raft::types::SpaceKeyRole> {
    match role {
        "signer" => Ok(nauka_raft::types::SpaceKeyRole::Signer),
        "admin" => Ok(nauka_raft::types::SpaceKeyRole::Admin),
        other => bail!("unknown role {other:?}: use `signer` (read links only) or `admin`"),
    }
}

pub async fn space_key_add(
    peers: &[SocketAddr],
    space: &str,
    role: &str,
    name: Option<&str>,
    public_key_hex: Option<&str>,
) -> Result<()> {
    split_space_path(space)?;
    let role_parsed = parse_role(role)?;
    let (secret, public) = match public_key_hex {
        Some(hex_str) => (None, crate::spaceauth::parse_public_hex(hex_str)?),
        None => {
            let (secret, public) = crate::spaceauth::generate();
            (Some(secret), public)
        }
    };
    let name = name
        .map(str::to_string)
        .unwrap_or_else(|| format!("{role}-{}", &hex::encode(public)[..6]));
    write_command(
        peers,
        nauka_raft::types::AppCommand::AddSpaceKey {
            space: space.to_string(),
            key: nauka_raft::types::SpaceKey {
                public_key: public,
                role: role_parsed,
                name: name.clone(),
            },
        },
    )
    .await?;
    println!("key {name} ({role}) registered on {space}");
    println!("  public : {}", hex::encode(public));
    if let Some(secret) = secret {
        println!("  private: {secret}");
        println!(
            "{}",
            style(
                "  ^ shown ONCE and stored NOWHERE — put it in your application's secret \
                 store now. Lost = rotate: `space key add` a new one, `space key rm` this \
                 one."
            )
            .yellow()
        );
    }
    Ok(())
}

pub async fn space_key_ls(peers: &[SocketAddr], space: &str) -> Result<()> {
    split_space_path(space)?;
    let all = fetch_space_keys(peers).await?;
    match all.get(space) {
        None => println!("no keys on {space} — nauka space key add {space} --role admin"),
        Some(keys) => {
            for k in keys {
                println!("{}  {}  {}", k.public_key, k.role, k.name);
            }
        }
    }
    Ok(())
}

pub async fn space_key_rm(peers: &[SocketAddr], space: &str, selector: &str) -> Result<()> {
    split_space_path(space)?;
    let all = fetch_space_keys(peers).await?;
    let keys = all
        .get(space)
        .with_context(|| format!("no keys on space {space}"))?;
    let matches: Vec<&KeyView> = keys
        .iter()
        .filter(|k| k.name == selector || k.public_key.starts_with(&selector.to_lowercase()))
        .collect();
    let key = match matches.as_slice() {
        [one] => one,
        [] => bail!("no key on {space} matches {selector:?} (see `space key ls {space}`)"),
        _ => bail!("{selector:?} is ambiguous on {space} — use the full name or a longer prefix"),
    };
    let public = crate::spaceauth::parse_public_hex(&key.public_key)?;
    write_command(
        peers,
        nauka_raft::types::AppCommand::RemoveSpaceKey {
            space: space.to_string(),
            public_key: public,
        },
    )
    .await?;
    println!(
        "key {} removed from {space} — its signatures are dead cluster-wide",
        key.name
    );
    Ok(())
}

/// Offline signing: no peers, no network. What Yogfile's backend does in
/// code, expressed as a command for scripts and humans.
pub fn space_sign(
    space: &str,
    secret: &str,
    method: &str,
    path: &str,
    content_hash: Option<&str>,
) -> Result<()> {
    split_space_path(space)?;
    let sk = crate::spaceauth::parse_secret(secret)?;
    let public = hex::encode(sk.verifying_key().to_bytes());
    let timestamp = crate::spaceauth::unix_now();
    let canonical = crate::spaceauth::canonical_write(method, path, space, timestamp, content_hash);
    let signature = crate::spaceauth::sign(&sk, &canonical);
    println!("X-Nauka-Space: {space}");
    println!("X-Nauka-Key: {public}");
    println!("X-Nauka-Timestamp: {timestamp}");
    if let Some(h) = content_hash {
        println!("X-Nauka-Content-Hash: {h}");
    }
    println!("X-Nauka-Signature: {signature}");
    eprintln!();
    let hash_flag = content_hash
        .map(|h| format!(" -H 'X-Nauka-Content-Hash: {h}'"))
        .unwrap_or_default();
    eprintln!(
        "{}",
        style(format!(
            "# valid {}s — example:\n\
             curl -T <file> 'http://<node>:8080{path}' \\\n\
            \x20 -H 'X-Nauka-Space: {space}' -H 'X-Nauka-Key: {public}' \\\n\
            \x20 -H 'X-Nauka-Timestamp: {timestamp}'{hash_flag} \\\n\
            \x20 -H 'X-Nauka-Signature: {signature}'",
            crate::spaceauth::MAX_CLOCK_SKEW
        ))
        .dim()
    );
    Ok(())
}

// ── File references ──────────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct FileRow {
    hash: String,
    size: u64,
    name: Option<String>,
    #[serde(default)]
    spaces: Vec<String>,
}

/// `space files`: the files a space references, from any node's API.
pub async fn space_files(peers: &[SocketAddr], space: &str) -> Result<()> {
    split_space_path(space)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let mut rows: Option<Vec<FileRow>> = None;
    for peer in peers {
        let url = format!("http://{}:8080/api/files", peer.ip());
        if let Ok(resp) = client.get(&url).send().await {
            if let Ok(v) = resp.json::<Vec<FileRow>>().await {
                rows = Some(v);
                break;
            }
        }
    }
    let rows = rows.context("no node answered /api/files")?;
    let mut total: u64 = 0;
    let mut count = 0usize;
    for f in rows.iter().filter(|f| f.spaces.iter().any(|s| s == space)) {
        count += 1;
        total += f.size;
        println!(
            "{}  {:>10}  {}",
            &f.hash[..16],
            human_bytes(f.size),
            f.name.as_deref().unwrap_or("—")
        );
    }
    if count == 0 {
        println!("{space} references no files");
    } else {
        println!(
            "{count} file(s), {} referenced by {space}",
            human_bytes(total)
        );
    }
    Ok(())
}

fn human_bytes(b: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = b as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{b} B")
    } else {
        format!("{v:.2} {}", UNITS[u])
    }
}

/// Offline link minting: the read-side twin of [`space_sign`]. The URL
/// is a capability — it carries its expiry and its proof, and any node
/// verifies it locally.
pub fn space_link(
    space: &str,
    hash: &str,
    secret: &str,
    ttl: u64,
    exp: Option<u64>,
    rate: Option<u64>,
    conc: Option<u32>,
) -> Result<()> {
    split_space_path(space)?;
    if hash.len() != 64 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
        bail!("expected the file's FULL 64-hex BLAKE3 hash (links sign the exact hash)");
    }
    if conc == Some(0) {
        bail!("--conc 0 would be a link nobody can open — use at least 1");
    }
    let sk = crate::spaceauth::parse_secret(secret)?;
    let exp = exp.unwrap_or_else(|| crate::spaceauth::unix_now() + ttl);
    let canonical = crate::spaceauth::canonical_link(hash, space, exp, rate, conc);
    let sig = crate::spaceauth::sign(&sk, &canonical);
    let mut query = format!("space={space}&exp={exp}");
    if let Some(r) = rate {
        query.push_str(&format!("&rate={r}"));
    }
    if let Some(c) = conc {
        query.push_str(&format!("&conc={c}"));
    }
    println!("/f/{hash}?{query}&sig={sig}");
    eprintln!(
        "{}",
        style(format!(
            "# dies at {exp} (unix). Full URL: http://<node>:8080/f/{}…",
            &hash[..8]
        ))
        .dim()
    );
    Ok(())
}

/// `space publish`: sign a ref-add and POST it — the "make it public
/// without re-uploading" gesture (and, with `to` = the signing space,
/// the adoption of an unowned legacy file).
pub async fn space_publish(
    peers: &[SocketAddr],
    space: &str,
    hash: &str,
    to: Option<&str>,
    secret: &str,
) -> Result<()> {
    split_space_path(space)?;
    let to = to.unwrap_or(space);
    split_space_path(to)?;
    if hash.len() != 64 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
        bail!("expected the file's FULL 64-hex BLAKE3 hash");
    }
    let sk = crate::spaceauth::parse_secret(secret)?;
    let public = hex::encode(sk.verifying_key().to_bytes());
    let timestamp = crate::spaceauth::unix_now();
    let signed_path = format!("/f/{hash}/refs?to={to}");
    let canonical = crate::spaceauth::canonical_write("POST", &signed_path, space, timestamp, None);
    let signature = crate::spaceauth::sign(&sk, &canonical);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    for peer in peers {
        let url = format!("http://{}:8080{signed_path}", peer.ip());
        let resp = client
            .post(&url)
            .header("X-Nauka-Space", space)
            .header("X-Nauka-Key", &public)
            .header("X-Nauka-Timestamp", timestamp)
            .header("X-Nauka-Signature", &signature)
            .send()
            .await;
        let Ok(resp) = resp else { continue };
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if status.is_success() {
            if to == space {
                println!("{} now references {}", to, &hash[..16]);
            } else {
                println!(
                    "{} now references {} (shared from {space})",
                    to,
                    &hash[..16]
                );
            }
            return Ok(());
        }
        bail!("{status}: {body}");
    }
    bail!("no node answered (tried {} peer(s))", peers.len());
}

/// `space set`: read-modify-write of a space's policies.
fn parse_cap(kind: &str, v: &str) -> Result<Option<u64>> {
    match v {
        "off" => Ok(None),
        n => Ok(Some(n.parse::<u64>().with_context(|| {
            format!("{kind} is a number of bytes, or `off`")
        })?)),
    }
}

pub async fn space_set(
    peers: &[SocketAddr],
    name: &str,
    rate_default: Option<&str>,
    quota: Option<&str>,
    egress_quota: Option<&str>,
) -> Result<()> {
    split_space_path(name)?;
    let view = fetch_orgs(peers).await?;
    let mut record = view
        .spaces
        .get(name)
        .cloned()
        .with_context(|| format!("no space named {name}"))?;
    let mut changed = false;
    if let Some(rate) = rate_default {
        record.rate_default = parse_cap("rate-default", rate)?;
        changed = true;
    }
    if let Some(q) = quota {
        record.quota_bytes = parse_cap("quota", q)?;
        changed = true;
    }
    if let Some(q) = egress_quota {
        record.egress_quota_bytes = parse_cap("egress-quota", q)?;
        changed = true;
    }
    if !changed {
        bail!("nothing to change (see --help for the available policies)");
    }
    write_command(
        peers,
        nauka_raft::types::AppCommand::UpsertSpace {
            name: name.to_string(),
            record: record.clone(),
        },
    )
    .await?;
    println!(
        "{name}: quota {} · egress/month {} · bare-read rate {}",
        record
            .quota_bytes
            .map(human_bytes)
            .unwrap_or_else(|| "off".into()),
        record
            .egress_quota_bytes
            .map(human_bytes)
            .unwrap_or_else(|| "off".into()),
        record
            .rate_default
            .map(|r| format!("{} /s", human_bytes(r)))
            .unwrap_or_else(|| "off".into()),
    );
    Ok(())
}

fn cap_str(v: Option<u64>) -> String {
    v.map(human_bytes).unwrap_or_else(|| "∞".into())
}

/// `space usage`: consumption against quotas, from any node.
pub async fn space_usage(peers: &[SocketAddr], name: &str) -> Result<()> {
    split_space_path(name)?;
    let view = fetch_orgs(peers).await?;
    let record = view
        .spaces
        .get(name)
        .with_context(|| format!("no space named {name}"))?;
    let u = view.usage.get(name).copied().unwrap_or_default();
    println!(
        "storage : {} / {}",
        human_bytes(u.storage_bytes),
        cap_str(record.quota_bytes)
    );
    println!(
        "egress  : {} / {} this month",
        human_bytes(u.egress_month_bytes),
        cap_str(record.egress_quota_bytes)
    );
    Ok(())
}

/// `org set`: the organisation-level storage cap.
pub async fn org_set(peers: &[SocketAddr], name: &str, quota: Option<&str>) -> Result<()> {
    let view = fetch_orgs(peers).await?;
    let mut record = view
        .orgs
        .get(name)
        .cloned()
        .with_context(|| format!("no organisation named {name}"))?;
    let Some(q) = quota else {
        bail!("nothing to change (--quota <bytes|off>)");
    };
    record.quota_bytes = parse_cap("quota", q)?;
    write_command(
        peers,
        nauka_raft::types::AppCommand::UpsertOrg {
            name: name.to_string(),
            record: record.clone(),
        },
    )
    .await?;
    println!("{name}: quota {}", cap_str(record.quota_bytes));
    Ok(())
}

/// `org usage`: every space of the org, plus the total against the cap.
pub async fn org_usage(peers: &[SocketAddr], name: &str) -> Result<()> {
    let view = fetch_orgs(peers).await?;
    let org = view
        .orgs
        .get(name)
        .with_context(|| format!("no organisation named {name}"))?;
    let mut total: u64 = 0;
    for (path, _) in view.spaces.iter().filter(|(_, r)| r.org == name) {
        let u = view.usage.get(path).copied().unwrap_or_default();
        total += u.storage_bytes;
        println!(
            "{path}  storage {}  egress {} this month",
            human_bytes(u.storage_bytes),
            human_bytes(u.egress_month_bytes)
        );
    }
    println!(
        "total: {} / {}",
        human_bytes(total),
        cap_str(org.quota_bytes)
    );
    Ok(())
}
