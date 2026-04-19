mod cli_out;

use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Arg, ArgAction, Command};
use nauka_core::logging::{self, LogMode};
use nauka_core::LogNaukaErr;
use nauka_hypervisor::daemon::{
    init_hypervisor, join_hypervisor, leave_hypervisor, run_daemon, SetupConfig,
};
use nauka_hypervisor::mesh;
use nauka_hypervisor::systemd;
use nauka_state::Database;
use tracing::Instrument;

#[tokio::main]
async fn main() {
    // Peek at args to pick the logging mode: the systemd-run daemon
    // subcommand needs INFO from nauka crates (lifecycle → journald);
    // every other invocation is a short-lived CLI that should stay
    // quiet and let `cli_out` own user-facing output.
    let mode = if std::env::args().any(|a| a == "daemon") {
        LogMode::Daemon
    } else {
        LogMode::Cli
    };
    logging::init(mode);

    // One trace_id per CLI/daemon invocation. Every event under `run`
    // inherits it via the span breadcrumb, so
    // `journalctl | grep trace_id=<uuid>` returns the full story of
    // this invocation.
    let trace_id = nauka_core::new_trace_id();
    let span = tracing::info_span!("cli", trace_id = %trace_id);

    let exit_code = match run().instrument(span).await {
        Ok(()) => 0,
        Err(e) => {
            cli_out::error(format_args!("{e:#}"));
            1
        }
    };
    // One-shot CLIs: exit immediately instead of waiting for SurrealDB's
    // background tasks to drain. Without this, `init`/`join` hang after
    // printing "service: … (running)" on some hosts while SurrealKV's
    // async shutdown deadlocks on the dying tokio runtime.
    std::process::exit(exit_code);
}

async fn run() -> Result<()> {
    let app = Command::new("nauka")
        .about("Nauka — turn dedicated servers into a programmable cloud")
        .version(option_env!("NAUKA_VERSION").unwrap_or(env!("CARGO_PKG_VERSION")))
        .arg_required_else_help(true)
        .subcommand(hypervisor_cmd())
        .subcommand(mesh_cmd())
        .subcommand(login_cmd())
        .subcommand(logout_cmd())
        .subcommand(whoami_cmd())
        .subcommand(user_cmd())
        .subcommand(org_cmd())
        .subcommand(project_cmd())
        .subcommand(env_cmd())
        .subcommand(role_cmd())
        .subcommand(service_account_cmd())
        .subcommand(token_cmd())
        .subcommand(audit_cmd())
        .subcommand(password_cmd())
        .subcommand(session_cmd());

    match app.get_matches().subcommand() {
        Some(("hypervisor", sub)) => handle_hypervisor(sub).await,
        Some(("mesh", sub)) => handle_mesh(sub).await,
        Some(("login", sub)) => cmd_login(sub).await,
        Some(("logout", _)) => cmd_logout().await,
        Some(("whoami", _)) => cmd_whoami().await,
        Some(("user", sub)) => handle_user(sub).await,
        Some(("org", sub)) => handle_org(sub).await,
        Some(("project", sub)) => handle_project(sub).await,
        Some(("env", sub)) => handle_env(sub).await,
        Some(("role", sub)) => handle_role(sub).await,
        Some(("service-account", sub)) => handle_service_account(sub).await,
        Some(("token", sub)) => handle_token(sub).await,
        Some(("audit", sub)) => handle_audit(sub).await,
        Some(("password", sub)) => handle_password(sub).await,
        Some(("session", sub)) => handle_session(sub).await,
        _ => anyhow::bail!("unknown subcommand — run 'nauka --help'"),
    }
}

async fn open_db() -> Result<Arc<Database>> {
    let db = Arc::new(Database::open(None).await?);
    // The only hand-written schema left is `nauka_state::SCHEMA`
    // (Raft's internal `_raft_*` tables). Every user-facing resource
    // flows through `#[resource]` + `ALL_RESOURCES`; every `DEFINE
    // ACCESS` through `#[access]` + `ALL_ACCESS_DEFS`; every
    // `DEFINE FUNCTION` through `ALL_DB_FUNCTIONS`.
    //
    // Functions load BEFORE tables: any `PERMISSIONS` clause that
    // calls `fn::iam::can` (or future helpers) must resolve the
    // function at parse time. Access definitions come last because
    // they can refer to both tables and functions.
    let functions = nauka_core::function_definitions();
    let cluster = nauka_core::cluster_schemas();
    let local = nauka_core::local_schemas();
    let access = nauka_core::access_definitions();
    nauka_state::load_schemas(
        &db,
        &[nauka_state::SCHEMA, &functions, &cluster, &local, &access],
    )
    .await?;
    Ok(db)
}

fn hypervisor_cmd() -> Command {
    Command::new("hypervisor")
        .about("Manage this hypervisor — create a mesh, join one, inspect, leave")
        .arg_required_else_help(true)
        .subcommand(
            Command::new("init")
                .about("Create a new mesh and start the hypervisor service")
                .arg(Arg::new("port").long("port").default_value("51820"))
                .arg(
                    Arg::new("interface")
                        .long("interface")
                        .default_value("nauka0"),
                ),
        )
        .subcommand(
            Command::new("join")
                .about("Join an existing mesh and start the hypervisor service")
                .arg(
                    Arg::new("host")
                        .required(true)
                        .help("Public IP of an existing node"),
                )
                .arg(Arg::new("pin").long("pin").required(true))
                .arg(Arg::new("port").long("port").default_value("51820"))
                .arg(
                    Arg::new("interface")
                        .long("interface")
                        .default_value("nauka0"),
                ),
        )
        .subcommand(Command::new("status").about("Show cluster membership and local state"))
        .subcommand(
            Command::new("list")
                .about("List every hypervisor in the mesh (generated CLI — via the API)"),
        )
        .subcommand(
            Command::new("get")
                .about("Fetch one hypervisor by its public key (generated CLI — via the API)")
                .arg(
                    Arg::new("public-key")
                        .required(true)
                        .help("Peer's base64 public key"),
                ),
        )
        .subcommand(
            Command::new("leave")
                .about("Leave the mesh — stop service, wipe state, remove unit file")
                .arg(
                    Arg::new("interface")
                        .long("interface")
                        .default_value("nauka0"),
                ),
        )
        .subcommand(
            Command::new("daemon")
                .about("(internal) Long-running service — systemd invokes this")
                .hide(true)
                .arg(
                    Arg::new("foreground")
                        .long("foreground")
                        .action(ArgAction::SetTrue)
                        .help("Run without systemd (dev/test only)"),
                ),
        )
        .subcommand(
            Command::new("mesh")
                .about("Low-level WireGuard mesh controls")
                .arg_required_else_help(true)
                .subcommand(
                    Command::new("status")
                        .about("Show the WireGuard interface status")
                        .arg(
                            Arg::new("interface")
                                .long("interface")
                                .default_value("nauka0"),
                        ),
                )
                .subcommand(Command::new("restart").about("Restart the hypervisor service"))
                .subcommand(Command::new("stop").about("Stop the hypervisor service")),
        )
        .subcommand(
            Command::new("peer")
                .about("Manage peers in the mesh")
                .arg_required_else_help(true)
                .subcommand(
                    Command::new("remove")
                        .about("Remove a peer from the mesh")
                        .arg(
                            Arg::new("public-key")
                                .long("public-key")
                                .required(true)
                                .help("Peer's base64 public key"),
                        ),
                ),
        )
        .subcommand(
            Command::new("debug")
                .about("Operator escape hatches — not part of the stable CLI")
                .arg_required_else_help(true)
                .subcommand(
                    Command::new("raft-write")
                        .about("Send arbitrary SurQL through Raft (loopback only)")
                        .arg(
                            Arg::new("query")
                                .required(true)
                                .help("SurQL statement, e.g. \"UPDATE <table> SET ...\""),
                        ),
                ),
        )
}

async fn handle_hypervisor(matches: &clap::ArgMatches) -> Result<()> {
    match matches.subcommand() {
        Some(("init", sub)) => cmd_init(sub).await,
        Some(("join", sub)) => cmd_join(sub).await,
        Some(("list", _)) => cmd_hypervisor_list().await,
        Some(("get", sub)) => cmd_hypervisor_get(sub).await,
        Some(("status", _)) => cmd_status().await,
        Some(("leave", sub)) => cmd_leave(sub).await,
        Some(("daemon", _)) => cmd_daemon().await,
        Some(("mesh", sub)) => cmd_mesh(sub).await,
        Some(("peer", sub)) => cmd_peer(sub).await,
        Some(("debug", sub)) => cmd_debug(sub).await,
        _ => anyhow::bail!("unknown hypervisor subcommand"),
    }
}

// -------- Generated-style CLI (via SDK) — see #355 (342-B2). --------

fn api_client() -> Result<nauka_api_client::Client> {
    let jwt = require_token()?;
    nauka_api_client::Client::new(API_BASE_URL, jwt).map_err(|e| anyhow::anyhow!("{e}"))
}

async fn cmd_hypervisor_list() -> Result<()> {
    let client = api_client()?;
    let rows = client
        .hypervisor()
        .list()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    cli_out::section(&format!("hypervisors ({}):", rows.len()));
    for h in &rows {
        let endpoint = h.endpoint.as_deref().unwrap_or("-");
        cli_out::say(format_args!(
            "  {}  {}  via {}",
            h.public_key, h.address, endpoint
        ));
    }
    Ok(())
}

async fn cmd_hypervisor_get(sub: &clap::ArgMatches) -> Result<()> {
    let pk = sub.get_one::<String>("public-key").unwrap().clone();
    let client = api_client()?;
    let h = client
        .hypervisor()
        .get(&pk)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    cli_out::pair("public key", &h.public_key);
    cli_out::pair("node id", h.node_id.to_string());
    cli_out::pair("raft addr", &h.raft_addr);
    cli_out::pair("address", &h.address);
    cli_out::pair("endpoint", h.endpoint.as_deref().unwrap_or("-"));
    cli_out::pair("allowed ips", h.allowed_ips.join(","));
    cli_out::pair(
        "keepalive",
        h.keepalive
            .map(|k| k.to_string())
            .unwrap_or_else(|| "-".into()),
    );
    Ok(())
}

fn mesh_cmd() -> Command {
    Command::new("mesh")
        .about("Inspect the local node's mesh identity (generated CLI — via the API)")
        .arg_required_else_help(true)
        .subcommand(
            Command::new("get")
                .about("Show the local mesh record (secrets are masked by the API)"),
        )
}

async fn handle_mesh(matches: &clap::ArgMatches) -> Result<()> {
    match matches.subcommand() {
        Some(("get", _)) => cmd_mesh_get().await,
        _ => anyhow::bail!("unknown mesh subcommand"),
    }
}

async fn cmd_mesh_get() -> Result<()> {
    let client = api_client()?;
    let list = client
        .mesh()
        .list()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let Some(m) = list.into_iter().next() else {
        anyhow::bail!("no mesh state on this node — run `nauka hypervisor init` first");
    };
    cli_out::pair("mesh id", &m.mesh_id);
    cli_out::pair("interface", &m.interface_name);
    cli_out::pair("listen port", m.listen_port.to_string());
    cli_out::pair(
        "ca cert",
        if m.ca_cert.is_some() { "yes" } else { "no" },
    );
    cli_out::pair(
        "tls cert",
        if m.tls_cert.is_some() { "yes" } else { "no" },
    );
    Ok(())
}

fn parse_setup(sub: &clap::ArgMatches) -> Result<SetupConfig> {
    let port: u16 = sub
        .get_one::<String>("port")
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(51820);
    let interface = sub
        .get_one::<String>("interface")
        .cloned()
        .unwrap_or_else(|| "nauka0".into());
    Ok(SetupConfig {
        interface_name: interface,
        listen_port: port,
        join_port: mesh::DEFAULT_JOIN_PORT,
    })
}

async fn cmd_init(sub: &clap::ArgMatches) -> Result<()> {
    check_not_already_in_mesh()?;
    let db = open_db().await?;
    let config = parse_setup(sub)?;
    let summary = init_hypervisor(db, config)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Give SurrealKV a moment to flush + release the LOCK file before the
    // systemd-managed daemon tries to open the same path.
    drop_db_and_wait().await;

    install_and_start_service().context("install systemd unit")?;

    cli_out::say("mesh created");
    cli_out::pair("mesh", &summary.mesh_id);
    cli_out::pair("public key", &summary.public_key);
    cli_out::pair("address", &summary.address);
    cli_out::pair("raft", &summary.raft_addr);
    cli_out::blank();
    cli_out::pair("join pin", &summary.pin);
    cli_out::blank();
    cli_out::say(format_args!("service: {} (running)", systemd::UNIT_NAME));
    cli_out::say(format_args!(
        "logs:    journalctl -u {} -f",
        systemd::UNIT_NAME
    ));
    Ok(())
}

async fn cmd_join(sub: &clap::ArgMatches) -> Result<()> {
    check_not_already_in_mesh()?;
    let db = open_db().await?;
    let config = parse_setup(sub)?;
    let host = sub.get_one::<String>("host").unwrap().clone();
    let pin = sub.get_one::<String>("pin").unwrap().clone();
    let summary = join_hypervisor(db, &host, &pin, config)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    drop_db_and_wait().await;

    install_and_start_service().context("install systemd unit")?;

    cli_out::say("joined mesh");
    cli_out::pair("mesh", &summary.mesh_id);
    cli_out::pair("public key", &summary.public_key);
    cli_out::pair("address", &summary.address);
    cli_out::pair("raft", &summary.raft_addr);
    cli_out::blank();
    cli_out::say(format_args!("service: {} (running)", systemd::UNIT_NAME));
    cli_out::say(format_args!(
        "logs:    journalctl -u {} -f",
        systemd::UNIT_NAME
    ));
    Ok(())
}

/// SurrealKV holds a kernel-level LOCK file on the DB directory. If the
/// CLI's DB handle isn't fully released before the systemd-managed daemon
/// starts, that daemon crash-loops on "Database is already locked". The
/// setup functions drop their Raft handles cleanly, but SurrealDB's
/// internal async tasks may still be winding down — give them a beat.
async fn drop_db_and_wait() {
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
}

/// Bail early with a clear message if this node already has a service
/// installed, instead of letting SurrealKV's "Database already locked"
/// error leak to the user. Only checks the unit file — `/var/lib/nauka`
/// can exist from a previously-failed attempt without meaning the node
/// is actually in a mesh.
fn check_not_already_in_mesh() -> Result<()> {
    if std::path::Path::new("/etc/systemd/system/nauka-hypervisor.service").exists() {
        anyhow::bail!(
            "this node already has hypervisor state — run 'nauka hypervisor leave' first"
        );
    }
    Ok(())
}

fn install_and_start_service() -> Result<()> {
    systemd::write_unit_file().map_err(|e| anyhow::anyhow!("{e}"))?;
    systemd::daemon_reload().map_err(|e| anyhow::anyhow!("{e}"))?;
    systemd::enable_and_start().map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

async fn cmd_status() -> Result<()> {
    // The daemon holds an exclusive SurrealKV lock, so the CLI can't open
    // the DB directly while the service is running. Ask the daemon over
    // its loopback IPC port instead.
    let v = mesh::request_status(mesh::DEFAULT_JOIN_PORT)?;

    let mesh_id = v.get("mesh_id").and_then(|x| x.as_str()).unwrap_or("?");
    let pk = v.get("public_key").and_then(|x| x.as_str()).unwrap_or("?");
    let addr = v.get("address").and_then(|x| x.as_str()).unwrap_or("?");
    let peering_open = v
        .get("peering_open")
        .and_then(|x| x.as_bool())
        .unwrap_or(false);
    let empty = Vec::<serde_json::Value>::new();
    let hypervisors = v
        .get("hypervisors")
        .and_then(|x| x.as_array())
        .unwrap_or(&empty);

    cli_out::pair("mesh", mesh_id);
    cli_out::pair("public key", pk);
    cli_out::pair("address", addr);
    cli_out::pair(
        "peering",
        if peering_open {
            "open (accepts joins)"
        } else {
            "closed"
        },
    );
    cli_out::section(&format!("hypervisors ({}):", hypervisors.len()));
    for h in hypervisors {
        let hpk = h.get("public_key").and_then(|x| x.as_str()).unwrap_or("?");
        let haddr = h.get("address").and_then(|x| x.as_str()).unwrap_or("?");
        let ep = h.get("endpoint").and_then(|x| x.as_str()).unwrap_or("-");
        let is_self = if hpk == pk { " (self)" } else { "" };
        cli_out::say(format_args!("  {hpk} at {haddr} via {ep}{is_self}"));
    }
    Ok(())
}

async fn cmd_leave(sub: &clap::ArgMatches) -> Result<()> {
    let iface = sub.get_one::<String>("interface").unwrap().clone();
    // Ask the daemon to broadcast a DELETE for self + wait briefly for
    // Raft to replicate. Best-effort — the daemon may already be down.
    if mesh::request_leave(mesh::DEFAULT_JOIN_PORT).warn().is_ok() {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
    systemd::stop_and_disable().map_err(|e| anyhow::anyhow!("{e}"))?;
    // Give SurrealKV's LOCK file time to release after the daemon exits.
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Now safe to open the DB and wipe state.
    leave_hypervisor(&iface)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    cli_out::say("hypervisor left mesh — systemd unit removed, local state wiped");
    Ok(())
}

async fn cmd_daemon() -> Result<()> {
    let db = open_db().await?;
    // Spawn the axum REST + GraphQL server on loopback alongside
    // every other daemon task. TLS + real listener address land
    // in 342-D; for now `127.0.0.1:4000` is the single convention
    // the CLI's `nauka-api-client` dials.
    run_daemon(db, |db, raft| async move {
        let deps = nauka_api::Deps::new(db, Some(raft));
        let app = nauka_api::router(deps);
        let listener = match tokio::net::TcpListener::bind(API_SERVER_ADDR).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(
                    event = "api.listener.bind_failed",
                    addr = API_SERVER_ADDR,
                    error = %e,
                    "axum listener bind failed — daemon continues without REST/GraphQL"
                );
                return Vec::new();
            }
        };
        tracing::info!(event = "api.listener.ready", addr = API_SERVER_ADDR);
        vec![tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app).await {
                tracing::error!(
                    event = "api.serve.exited",
                    error = %e,
                    "axum::serve returned an error"
                );
            }
        })]
    })
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))
}

/// Loopback address the daemon binds and the generated CLI dials.
/// Hardcoded in 342-B2; 342-D swaps in TLS + configurable port.
const API_SERVER_ADDR: &str = "127.0.0.1:4000";
const API_BASE_URL: &str = "http://127.0.0.1:4000";

async fn cmd_mesh(sub: &clap::ArgMatches) -> Result<()> {
    match sub.subcommand() {
        Some(("status", s)) => {
            let iface = s.get_one::<String>("interface").unwrap();
            let status = mesh::Mesh::interface_status(iface)?;
            cli_out::say(format_args!("{status:#?}"));
            Ok(())
        }
        Some(("restart", _)) => {
            std::process::Command::new("systemctl")
                .args(["restart", systemd::UNIT_NAME])
                .status()
                .context("spawn systemctl")?;
            Ok(())
        }
        Some(("stop", _)) => {
            std::process::Command::new("systemctl")
                .args(["stop", systemd::UNIT_NAME])
                .status()
                .context("spawn systemctl")?;
            Ok(())
        }
        _ => anyhow::bail!("unknown mesh subcommand"),
    }
}

async fn cmd_peer(sub: &clap::ArgMatches) -> Result<()> {
    match sub.subcommand() {
        Some(("remove", rm)) => {
            let pk = rm.get_one::<String>("public-key").unwrap();
            mesh::request_peer_removal(mesh::DEFAULT_JOIN_PORT, pk)?;
            cli_out::say("peer removal requested");
            Ok(())
        }
        _ => anyhow::bail!("unknown peer subcommand"),
    }
}

async fn cmd_debug(sub: &clap::ArgMatches) -> Result<()> {
    match sub.subcommand() {
        Some(("raft-write", rw)) => {
            let query = rw.get_one::<String>("query").unwrap();
            mesh::request_raft_write(mesh::DEFAULT_JOIN_PORT, query)?;
            cli_out::say("raft write ok");
            Ok(())
        }
        _ => anyhow::bail!("unknown debug subcommand"),
    }
}

// -------- IAM (login / logout / whoami / user create) --------

fn login_cmd() -> Command {
    Command::new("login")
        .about("Sign in to the local Nauka cluster and store the JWT")
        .arg(
            Arg::new("email")
                .long("email")
                .required(true)
                .help("Email of an existing user"),
        )
}

fn logout_cmd() -> Command {
    Command::new("logout").about("Delete the locally-stored session token")
}

fn whoami_cmd() -> Command {
    Command::new("whoami").about("Print the stored token's subject + expiry")
}

fn user_cmd() -> Command {
    Command::new("user")
        .about("Manage users on this cluster")
        .arg_required_else_help(true)
        .subcommand(
            Command::new("create")
                .about("Create a new user (prompts for password)")
                .arg(Arg::new("email").long("email").required(true))
                .arg(
                    Arg::new("display-name")
                        .long("display-name")
                        .required(true)
                        .help("Human-readable name shown in UIs and audit logs"),
                ),
        )
        .subcommand(Command::new("list").about("List users visible to the caller"))
        .subcommand(
            Command::new("deactivate")
                .about("Block future signins for a user (IAM-9)")
                .arg(Arg::new("email").long("email").required(true))
                .arg(
                    Arg::new("reason")
                        .long("reason")
                        .required(true)
                        .help("Why the user is being deactivated — audited"),
                ),
        )
        .subcommand(
            Command::new("activate")
                .about("Re-enable signins for a deactivated user (IAM-9)")
                .arg(Arg::new("email").long("email").required(true))
                .arg(
                    Arg::new("reason")
                        .long("reason")
                        .required(true)
                        .help("Why the user is being reactivated — audited"),
                ),
        )
}

async fn handle_user(matches: &clap::ArgMatches) -> Result<()> {
    match matches.subcommand() {
        Some(("create", sub)) => cmd_user_create(sub).await,
        Some(("list", _)) => cmd_user_list().await,
        Some(("deactivate", sub)) => cmd_user_set_active(sub, false).await,
        Some(("activate", sub)) => cmd_user_set_active(sub, true).await,
        _ => anyhow::bail!("unknown user subcommand"),
    }
}

async fn cmd_user_list() -> Result<()> {
    let jwt = require_token()?;
    let req = serde_json::json!({ "iam_user_list": true, "jwt": jwt });
    let resp =
        mesh::request_json(mesh::DEFAULT_JOIN_PORT, req).map_err(|e| anyhow::anyhow!("{e}"))?;
    let empty = Vec::new();
    let rows = resp
        .get("users")
        .and_then(|x| x.as_array())
        .unwrap_or(&empty);
    cli_out::section(&format!("users ({}):", rows.len()));
    for u in rows {
        let email = u.get("email").and_then(|x| x.as_str()).unwrap_or("?");
        let dn = u
            .get("display_name")
            .and_then(|x| x.as_str())
            .unwrap_or("?");
        let active = u.get("active").and_then(|x| x.as_bool()).unwrap_or(true);
        let verified = u
            .get("email_verified_at")
            .and_then(|x| x.as_str())
            .unwrap_or("-");
        let status = if active { "active  " } else { "DISABLED" };
        cli_out::say(format_args!(
            "  {email:<30}  {status}  {dn:<24}  verified={verified}"
        ));
    }
    Ok(())
}

async fn cmd_user_set_active(sub: &clap::ArgMatches, active: bool) -> Result<()> {
    let jwt = require_token()?;
    let email = sub.get_one::<String>("email").unwrap().clone();
    let reason = sub.get_one::<String>("reason").unwrap().clone();
    let req = serde_json::json!({
        "iam_user_set_active": true,
        "jwt": jwt,
        "email": email,
        "active": active,
        "reason": reason,
    });
    mesh::request_json(mesh::DEFAULT_JOIN_PORT, req).map_err(|e| anyhow::anyhow!("{e}"))?;
    let verb = if active { "activated" } else { "deactivated" };
    cli_out::say(format_args!("user {verb}: {email}"));
    cli_out::pair("reason", reason);
    Ok(())
}

/// Read a password.
///
/// - **Interactive** (stdin is a TTY): prompt on `/dev/tty` with echo
///   disabled via `rpassword::prompt_password`.
/// - **Piped** (stdin is not a TTY): read one line from stdin with no
///   prompt — this is the `echo pass | nauka login` scripting path.
///   `rpassword` would otherwise fail with `ENXIO` ("No such device
///   or address") because it requires `/dev/tty`; falling back to
///   stdin is what kubectl/gh/docker all do.
///
/// We deliberately accept passwords on stdin when it is piped rather
/// than requiring an explicit `--password-stdin` flag — the intent is
/// unambiguous (you cannot type to a pipe) and the alternative is a
/// worse script ergonomics for the same security properties.
fn read_password(prompt: &str) -> Result<String> {
    use std::io::{BufRead, IsTerminal};
    if std::io::stdin().is_terminal() {
        rpassword::prompt_password(prompt).context("read password")
    } else {
        let mut line = String::new();
        std::io::stdin()
            .lock()
            .read_line(&mut line)
            .context("read password from stdin")?;
        // Trim the line terminator(s); Windows clients that pipe via
        // `echo` would otherwise sneak a `\r` into the hash input.
        Ok(line
            .trim_end_matches('\n')
            .trim_end_matches('\r')
            .to_string())
    }
}

async fn cmd_login(sub: &clap::ArgMatches) -> Result<()> {
    let email = sub.get_one::<String>("email").unwrap().clone();
    let password = read_password(&format!("Password for {email}: "))?;
    let jwt = mesh::request_iam_signin(mesh::DEFAULT_JOIN_PORT, &email, &password)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    nauka_iam::save_token(&jwt).map_err(|e| anyhow::anyhow!("{e}"))?;
    cli_out::say(format_args!("logged in as {email}"));
    let path = nauka_iam::token_path().map_err(|e| anyhow::anyhow!("{e}"))?;
    cli_out::pair("token", path.display());
    Ok(())
}

async fn cmd_logout() -> Result<()> {
    nauka_iam::delete_token().map_err(|e| anyhow::anyhow!("{e}"))?;
    cli_out::say("logged out");
    Ok(())
}

async fn cmd_whoami() -> Result<()> {
    let Some(jwt) = nauka_iam::load_token().map_err(|e| anyhow::anyhow!("{e}"))? else {
        cli_out::say("not logged in");
        return Ok(());
    };
    let claims = nauka_iam::decode_claims(&jwt).map_err(|e| anyhow::anyhow!("{e}"))?;
    match claims.email() {
        Some(email) => cli_out::pair("email", email),
        None => cli_out::pair("subject", claims.id.as_deref().unwrap_or("?")),
    }
    if let Some(exp) = claims.exp {
        cli_out::pair("exp", exp);
    }
    if let Some(ac) = claims.access.as_deref() {
        cli_out::pair("access", ac);
    }
    Ok(())
}

// -------- IAM-2: org / project / env --------

fn org_cmd() -> Command {
    Command::new("org")
        .about("Manage orgs (IAM-2)")
        .arg_required_else_help(true)
        .subcommand(
            Command::new("create")
                .about("Create a new org owned by the logged-in user")
                .arg(Arg::new("slug").long("slug").required(true))
                .arg(Arg::new("display-name").long("display-name").required(true)),
        )
        .subcommand(Command::new("list").about("List orgs visible to the logged-in user"))
}

fn project_cmd() -> Command {
    Command::new("project")
        .about("Manage projects (IAM-2)")
        .arg_required_else_help(true)
        .subcommand(
            Command::new("create")
                .about("Create a project under an org")
                .arg(Arg::new("org").long("org").required(true).help("Org slug"))
                .arg(Arg::new("slug").long("slug").required(true))
                .arg(Arg::new("display-name").long("display-name").required(true)),
        )
        .subcommand(Command::new("list").about("List projects visible to the logged-in user"))
}

fn env_cmd() -> Command {
    Command::new("env")
        .about("Manage environments (IAM-2)")
        .arg_required_else_help(true)
        .subcommand(
            Command::new("create")
                .about("Create an environment under a project")
                .arg(
                    Arg::new("project")
                        .long("project")
                        .required(true)
                        .help("Project uid (`<org>-<slug>`)"),
                )
                .arg(Arg::new("slug").long("slug").required(true))
                .arg(Arg::new("display-name").long("display-name").required(true)),
        )
        .subcommand(Command::new("list").about("List envs visible to the logged-in user"))
}

/// Load the stored JWT or bail — every IAM-2 command requires one.
fn require_token() -> Result<String> {
    nauka_iam::load_token()
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .ok_or_else(|| anyhow::anyhow!("not logged in — run `nauka login --email <you>` first"))
}

async fn handle_org(matches: &clap::ArgMatches) -> Result<()> {
    match matches.subcommand() {
        Some(("create", sub)) => {
            let jwt = require_token()?;
            let slug = sub.get_one::<String>("slug").unwrap().clone();
            let display = sub.get_one::<String>("display-name").unwrap().clone();
            let req = serde_json::json!({
                "iam_org_create": true,
                "jwt": jwt,
                "slug": slug,
                "display_name": display,
            });
            let resp = mesh::request_json(mesh::DEFAULT_JOIN_PORT, req)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            let org = resp
                .get("org")
                .ok_or_else(|| anyhow::anyhow!("no org in response"))?;
            cli_out::say(format_args!(
                "org created: {}",
                org.get("slug").and_then(|x| x.as_str()).unwrap_or("?")
            ));
            cli_out::pair(
                "owner",
                org.get("owner").and_then(|x| x.as_str()).unwrap_or("?"),
            );
            Ok(())
        }
        Some(("list", _)) => {
            let jwt = require_token()?;
            let req = serde_json::json!({ "iam_org_list": true, "jwt": jwt });
            let resp = mesh::request_json(mesh::DEFAULT_JOIN_PORT, req)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            let empty = Vec::new();
            let rows = resp
                .get("orgs")
                .and_then(|x| x.as_array())
                .unwrap_or(&empty);
            cli_out::section(&format!("orgs ({}):", rows.len()));
            for o in rows {
                let slug = o.get("slug").and_then(|x| x.as_str()).unwrap_or("?");
                let dn = o
                    .get("display_name")
                    .and_then(|x| x.as_str())
                    .unwrap_or("?");
                cli_out::say(format_args!("  {slug:<16}  {dn}"));
            }
            Ok(())
        }
        _ => anyhow::bail!("unknown org subcommand"),
    }
}

async fn handle_project(matches: &clap::ArgMatches) -> Result<()> {
    match matches.subcommand() {
        Some(("create", sub)) => {
            let jwt = require_token()?;
            let org = sub.get_one::<String>("org").unwrap().clone();
            let slug = sub.get_one::<String>("slug").unwrap().clone();
            let display = sub.get_one::<String>("display-name").unwrap().clone();
            let req = serde_json::json!({
                "iam_project_create": true,
                "jwt": jwt,
                "org": org,
                "slug": slug,
                "display_name": display,
            });
            let resp = mesh::request_json(mesh::DEFAULT_JOIN_PORT, req)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            let p = resp
                .get("project")
                .ok_or_else(|| anyhow::anyhow!("no project in response"))?;
            cli_out::say(format_args!(
                "project created: {}",
                p.get("uid").and_then(|x| x.as_str()).unwrap_or("?")
            ));
            Ok(())
        }
        Some(("list", _)) => {
            let jwt = require_token()?;
            let req = serde_json::json!({ "iam_project_list": true, "jwt": jwt });
            let resp = mesh::request_json(mesh::DEFAULT_JOIN_PORT, req)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            let empty = Vec::new();
            let rows = resp
                .get("projects")
                .and_then(|x| x.as_array())
                .unwrap_or(&empty);
            cli_out::section(&format!("projects ({}):", rows.len()));
            for p in rows {
                let uid = p.get("uid").and_then(|x| x.as_str()).unwrap_or("?");
                let slug = p.get("slug").and_then(|x| x.as_str()).unwrap_or("?");
                let org = p.get("org").and_then(|x| x.as_str()).unwrap_or("?");
                let dn = p
                    .get("display_name")
                    .and_then(|x| x.as_str())
                    .unwrap_or("?");
                cli_out::say(format_args!("  {uid:<24}  {slug:<10}  {org:<16}  {dn}"));
            }
            Ok(())
        }
        _ => anyhow::bail!("unknown project subcommand"),
    }
}

async fn handle_env(matches: &clap::ArgMatches) -> Result<()> {
    match matches.subcommand() {
        Some(("create", sub)) => {
            let jwt = require_token()?;
            let project = sub.get_one::<String>("project").unwrap().clone();
            let slug = sub.get_one::<String>("slug").unwrap().clone();
            let display = sub.get_one::<String>("display-name").unwrap().clone();
            let req = serde_json::json!({
                "iam_env_create": true,
                "jwt": jwt,
                "project": project,
                "slug": slug,
                "display_name": display,
            });
            let resp = mesh::request_json(mesh::DEFAULT_JOIN_PORT, req)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            let e = resp
                .get("env")
                .ok_or_else(|| anyhow::anyhow!("no env in response"))?;
            cli_out::say(format_args!(
                "env created: {}",
                e.get("uid").and_then(|x| x.as_str()).unwrap_or("?")
            ));
            Ok(())
        }
        Some(("list", _)) => {
            let jwt = require_token()?;
            let req = serde_json::json!({ "iam_env_list": true, "jwt": jwt });
            let resp = mesh::request_json(mesh::DEFAULT_JOIN_PORT, req)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            let empty = Vec::new();
            let rows = resp
                .get("envs")
                .and_then(|x| x.as_array())
                .unwrap_or(&empty);
            cli_out::section(&format!("envs ({}):", rows.len()));
            for e in rows {
                let uid = e.get("uid").and_then(|x| x.as_str()).unwrap_or("?");
                let slug = e.get("slug").and_then(|x| x.as_str()).unwrap_or("?");
                let proj = e.get("project").and_then(|x| x.as_str()).unwrap_or("?");
                let dn = e
                    .get("display_name")
                    .and_then(|x| x.as_str())
                    .unwrap_or("?");
                cli_out::say(format_args!("  {uid:<24}  {slug:<12}  {proj:<24}  {dn}"));
            }
            Ok(())
        }
        _ => anyhow::bail!("unknown env subcommand"),
    }
}

// -------- IAM-3: role list / bind / unbind / bindings --------

fn role_cmd() -> Command {
    Command::new("role")
        .about("Manage roles and role bindings (IAM-3)")
        .arg_required_else_help(true)
        .subcommand(Command::new("list").about("List roles visible to the logged-in user"))
        .subcommand(
            Command::new("bind")
                .about("Attach a role to a principal at an Org scope")
                .arg(
                    Arg::new("principal")
                        .long("principal")
                        .required(true)
                        .help("User email"),
                )
                .arg(
                    Arg::new("role")
                        .long("role")
                        .required(true)
                        .help("Role slug (e.g. `viewer`, `editor`)"),
                )
                .arg(Arg::new("org").long("org").required(true).help("Org slug"))
                .arg(
                    Arg::new("reason")
                        .long("reason")
                        .required(true)
                        .help("Why this binding is being granted — audited (IAM-9)"),
                ),
        )
        .subcommand(
            Command::new("unbind")
                .about("Remove a role binding")
                .arg(Arg::new("principal").long("principal").required(true))
                .arg(Arg::new("role").long("role").required(true))
                .arg(Arg::new("org").long("org").required(true)),
        )
        .subcommand(Command::new("bindings").about("List role bindings visible to the user"))
}

async fn handle_role(matches: &clap::ArgMatches) -> Result<()> {
    match matches.subcommand() {
        Some(("list", _)) => {
            let jwt = require_token()?;
            let req = serde_json::json!({ "iam_role_list": true, "jwt": jwt });
            let resp = mesh::request_json(mesh::DEFAULT_JOIN_PORT, req)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            let empty = Vec::new();
            let rows = resp
                .get("roles")
                .and_then(|x| x.as_array())
                .unwrap_or(&empty);
            cli_out::section(&format!("roles ({}):", rows.len()));
            for r in rows {
                let slug = r.get("slug").and_then(|x| x.as_str()).unwrap_or("?");
                let kind = r.get("kind").and_then(|x| x.as_str()).unwrap_or("?");
                let pcount = r
                    .get("permissions")
                    .and_then(|x| x.as_array())
                    .map(|a| a.len())
                    .unwrap_or(0);
                cli_out::say(format_args!(
                    "  {slug:<24}  {kind:<10}  {pcount} permissions"
                ));
            }
            Ok(())
        }
        Some(("bind", sub)) => {
            let jwt = require_token()?;
            let principal = sub.get_one::<String>("principal").unwrap().clone();
            let role = sub.get_one::<String>("role").unwrap().clone();
            let org = sub.get_one::<String>("org").unwrap().clone();
            let reason = sub.get_one::<String>("reason").unwrap().clone();
            let req = serde_json::json!({
                "iam_role_bind": true,
                "jwt": jwt,
                "principal": principal,
                "role": role,
                "org": org,
                "reason": reason,
            });
            mesh::request_json(mesh::DEFAULT_JOIN_PORT, req).map_err(|e| anyhow::anyhow!("{e}"))?;
            cli_out::say(format_args!(
                "bound {principal} to role {role} in org {org}"
            ));
            cli_out::pair("reason", reason);
            Ok(())
        }
        Some(("unbind", sub)) => {
            let jwt = require_token()?;
            let principal = sub.get_one::<String>("principal").unwrap().clone();
            let role = sub.get_one::<String>("role").unwrap().clone();
            let org = sub.get_one::<String>("org").unwrap().clone();
            let req = serde_json::json!({
                "iam_role_unbind": true,
                "jwt": jwt,
                "principal": principal,
                "role": role,
                "org": org,
            });
            mesh::request_json(mesh::DEFAULT_JOIN_PORT, req).map_err(|e| anyhow::anyhow!("{e}"))?;
            cli_out::say(format_args!(
                "unbound {principal} from role {role} in org {org}"
            ));
            Ok(())
        }
        Some(("bindings", _)) => {
            let jwt = require_token()?;
            let req = serde_json::json!({ "iam_bindings_list": true, "jwt": jwt });
            let resp = mesh::request_json(mesh::DEFAULT_JOIN_PORT, req)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            let empty = Vec::new();
            let rows = resp
                .get("bindings")
                .and_then(|x| x.as_array())
                .unwrap_or(&empty);
            cli_out::section(&format!("bindings ({}):", rows.len()));
            for b in rows {
                let principal = b.get("principal").and_then(|x| x.as_str()).unwrap_or("?");
                let role = b.get("role").and_then(|x| x.as_str()).unwrap_or("?");
                let org = b.get("org").and_then(|x| x.as_str()).unwrap_or("?");
                cli_out::say(format_args!("  {principal:<28}  {role:<16}  {org}"));
            }
            Ok(())
        }
        _ => anyhow::bail!("unknown role subcommand"),
    }
}

// -------- IAM-4: service accounts + API tokens --------

fn service_account_cmd() -> Command {
    Command::new("service-account")
        .about("Manage service accounts (IAM-4)")
        .arg_required_else_help(true)
        .subcommand(
            Command::new("create")
                .about("Create a service account under an org")
                .arg(Arg::new("org").long("org").required(true).help("Org slug"))
                .arg(
                    Arg::new("slug")
                        .long("slug")
                        .required(true)
                        .help("Per-org slug (record id becomes `<org>-<slug>`)"),
                )
                .arg(Arg::new("display-name").long("display-name").required(true)),
        )
        .subcommand(Command::new("list").about("List service accounts visible to the caller"))
}

fn token_cmd() -> Command {
    Command::new("token")
        .about("Manage API tokens for service accounts (IAM-4)")
        .arg_required_else_help(true)
        .subcommand(
            Command::new("create")
                .about("Mint a new API token (plaintext shown ONCE — save it)")
                .arg(
                    Arg::new("service-account")
                        .long("service-account")
                        .required(true)
                        .help("SA scoped slug, e.g. `acme-ci`"),
                )
                .arg(
                    Arg::new("name")
                        .long("name")
                        .required(true)
                        .help("Human-readable token label"),
                ),
        )
        .subcommand(Command::new("list").about("List tokens visible to the caller (no secrets)"))
        .subcommand(
            Command::new("revoke")
                .about("Delete a token — future signins with it will be rejected")
                .arg(Arg::new("token-id").long("token-id").required(true)),
        )
}

async fn handle_service_account(matches: &clap::ArgMatches) -> Result<()> {
    match matches.subcommand() {
        Some(("create", sub)) => {
            let jwt = require_token()?;
            let org = sub.get_one::<String>("org").unwrap().clone();
            let slug = sub.get_one::<String>("slug").unwrap().clone();
            let display = sub.get_one::<String>("display-name").unwrap().clone();
            let req = serde_json::json!({
                "iam_sa_create": true,
                "jwt": jwt,
                "org": org,
                "slug": slug,
                "display_name": display,
            });
            let resp = mesh::request_json(mesh::DEFAULT_JOIN_PORT, req)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            let sa = resp
                .get("service_account")
                .ok_or_else(|| anyhow::anyhow!("no service_account in response"))?;
            cli_out::say(format_args!(
                "service account created: {}",
                sa.get("slug").and_then(|x| x.as_str()).unwrap_or("?")
            ));
            Ok(())
        }
        Some(("list", _)) => {
            let jwt = require_token()?;
            let req = serde_json::json!({ "iam_sa_list": true, "jwt": jwt });
            let resp = mesh::request_json(mesh::DEFAULT_JOIN_PORT, req)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            let empty = Vec::new();
            let rows = resp
                .get("service_accounts")
                .and_then(|x| x.as_array())
                .unwrap_or(&empty);
            cli_out::section(&format!("service accounts ({}):", rows.len()));
            for s in rows {
                let slug = s.get("slug").and_then(|x| x.as_str()).unwrap_or("?");
                let org = s.get("org").and_then(|x| x.as_str()).unwrap_or("?");
                let dn = s
                    .get("display_name")
                    .and_then(|x| x.as_str())
                    .unwrap_or("?");
                cli_out::say(format_args!("  {slug:<24}  {org:<16}  {dn}"));
            }
            Ok(())
        }
        _ => anyhow::bail!("unknown service-account subcommand"),
    }
}

async fn handle_token(matches: &clap::ArgMatches) -> Result<()> {
    match matches.subcommand() {
        Some(("create", sub)) => {
            let jwt = require_token()?;
            let sa = sub.get_one::<String>("service-account").unwrap().clone();
            let name = sub.get_one::<String>("name").unwrap().clone();
            let req = serde_json::json!({
                "iam_token_create": true,
                "jwt": jwt,
                "service_account": sa,
                "name": name,
            });
            let resp = mesh::request_json(mesh::DEFAULT_JOIN_PORT, req)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            let plaintext = resp
                .get("plaintext")
                .and_then(|x| x.as_str())
                .ok_or_else(|| anyhow::anyhow!("daemon did not return a plaintext token"))?;
            // Loud divider + blank lines around the token so the
            // operator notices; this is the only time the secret
            // is visible.
            cli_out::say(format_args!("token `{name}` minted for {sa}"));
            cli_out::blank();
            cli_out::say("╔═ SAVE THIS NOW — will not be shown again ═╗");
            cli_out::say(plaintext);
            cli_out::say("╚════════════════════════════════════════════╝");
            Ok(())
        }
        Some(("list", _)) => {
            let jwt = require_token()?;
            let req = serde_json::json!({ "iam_token_list": true, "jwt": jwt });
            let resp = mesh::request_json(mesh::DEFAULT_JOIN_PORT, req)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            let empty = Vec::new();
            let rows = resp
                .get("tokens")
                .and_then(|x| x.as_array())
                .unwrap_or(&empty);
            cli_out::section(&format!("api tokens ({}):", rows.len()));
            for t in rows {
                let tid = t.get("token_id").and_then(|x| x.as_str()).unwrap_or("?");
                let name = t.get("name").and_then(|x| x.as_str()).unwrap_or("?");
                let sa = t
                    .get("service_account")
                    .and_then(|x| x.as_str())
                    .unwrap_or("?");
                cli_out::say(format_args!("  {tid:<24}  {name:<20}  {sa}"));
            }
            Ok(())
        }
        Some(("revoke", sub)) => {
            let jwt = require_token()?;
            let token_id = sub.get_one::<String>("token-id").unwrap().clone();
            let req = serde_json::json!({
                "iam_token_revoke": true,
                "jwt": jwt,
                "token_id": token_id,
            });
            mesh::request_json(mesh::DEFAULT_JOIN_PORT, req).map_err(|e| anyhow::anyhow!("{e}"))?;
            cli_out::say(format_args!("token {token_id} revoked"));
            Ok(())
        }
        _ => anyhow::bail!("unknown token subcommand"),
    }
}

// -------- IAM-5: audit log --------

fn audit_cmd() -> Command {
    Command::new("audit")
        .about("Inspect the hash-chained audit log (IAM-5)")
        .arg_required_else_help(true)
        .subcommand(
            Command::new("list")
                .about("List recent audit events, newest first")
                .arg(
                    Arg::new("limit")
                        .long("limit")
                        .default_value("50")
                        .help("Maximum number of events to return"),
                ),
        )
}

async fn handle_audit(matches: &clap::ArgMatches) -> Result<()> {
    match matches.subcommand() {
        Some(("list", sub)) => {
            let jwt = require_token()?;
            let limit: usize = sub
                .get_one::<String>("limit")
                .map(|s| s.parse())
                .transpose()?
                .unwrap_or(50);
            let req = serde_json::json!({
                "iam_audit_list": true,
                "jwt": jwt,
                "limit": limit,
            });
            let resp = mesh::request_json(mesh::DEFAULT_JOIN_PORT, req)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            let empty = Vec::new();
            let rows = resp
                .get("events")
                .and_then(|x| x.as_array())
                .unwrap_or(&empty);
            cli_out::section(&format!("audit events ({}):", rows.len()));
            for e in rows {
                let action = e.get("action").and_then(|x| x.as_str()).unwrap_or("?");
                let actor = e.get("actor").and_then(|x| x.as_str()).unwrap_or("?");
                let target = e.get("target").and_then(|x| x.as_str()).unwrap_or("?");
                let at = e.get("at").and_then(|x| x.as_str()).unwrap_or("?");
                let outcome = e.get("outcome").and_then(|x| x.as_str()).unwrap_or("?");
                let hash = e.get("hash").and_then(|x| x.as_str()).unwrap_or("?");
                let short_hash = if hash.len() >= 8 { &hash[..8] } else { hash };
                cli_out::say(format_args!(
                    "  {at}  {action:<6}  {actor:<32}  {target:<40}  {outcome}  {short_hash}"
                ));
            }
            Ok(())
        }
        _ => anyhow::bail!("unknown audit subcommand"),
    }
}

// -------- IAM-7: password lifecycle --------

fn password_cmd() -> Command {
    Command::new("password")
        .about("Password lifecycle operations (IAM-7)")
        .arg_required_else_help(true)
        .subcommand(
            Command::new("reset-request")
                .about("Request a password-reset token (admin reads it from the daemon journal until IAM-7b adds email delivery)")
                .arg(Arg::new("email").long("email").required(true)),
        )
        .subcommand(
            Command::new("reset")
                .about("Consume a reset token and set a new password")
                .arg(Arg::new("token-id").long("token-id").required(true))
                .arg(Arg::new("email").long("email").required(true).help("Email of the account being reset (used for the login nudge printed on success)")),
        )
}

async fn handle_password(matches: &clap::ArgMatches) -> Result<()> {
    match matches.subcommand() {
        Some(("reset-request", sub)) => {
            let email = sub.get_one::<String>("email").unwrap().clone();
            let req = serde_json::json!({
                "iam_password_reset_request": true,
                "email": email,
            });
            mesh::request_json(mesh::DEFAULT_JOIN_PORT, req).map_err(|e| anyhow::anyhow!("{e}"))?;
            // Always the same message — no enumeration oracle.
            cli_out::say("if that email is registered, a reset token has been minted");
            cli_out::say("(admin: look for `iam.password.reset_request.minted` in journalctl)");
            Ok(())
        }
        Some(("reset", sub)) => {
            let token_id = sub.get_one::<String>("token-id").unwrap().clone();
            let email = sub.get_one::<String>("email").unwrap().clone();
            let new_password = read_password("New password: ")?;
            let confirm = read_password("Confirm password: ")?;
            if new_password != confirm {
                anyhow::bail!("passwords do not match");
            }
            let req = serde_json::json!({
                "iam_password_reset": true,
                "token_id": token_id,
                "new_password": new_password,
            });
            mesh::request_json(mesh::DEFAULT_JOIN_PORT, req).map_err(|e| anyhow::anyhow!("{e}"))?;
            cli_out::say("password updated");
            cli_out::say(format_args!(
                "  run `nauka login --email {email}` to mint a fresh JWT"
            ));
            Ok(())
        }
        _ => anyhow::bail!("unknown password subcommand"),
    }
}

// -------- IAM-8: session inventory --------

fn session_cmd() -> Command {
    Command::new("session")
        .about("Inspect active sessions (IAM-8)")
        .arg_required_else_help(true)
        .subcommand(Command::new("list").about("List the current user's active sessions"))
}

async fn handle_session(matches: &clap::ArgMatches) -> Result<()> {
    match matches.subcommand() {
        Some(("list", _)) => {
            let jwt = require_token()?;
            let req = serde_json::json!({
                "iam_session_list": true,
                "jwt": jwt,
            });
            let resp = mesh::request_json(mesh::DEFAULT_JOIN_PORT, req)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            let empty = Vec::new();
            let rows = resp
                .get("sessions")
                .and_then(|x| x.as_array())
                .unwrap_or(&empty);
            cli_out::section(&format!("active sessions ({}):", rows.len()));
            for s in rows {
                let uid = s.get("uid").and_then(|x| x.as_str()).unwrap_or("?");
                let ip = s.get("ip").and_then(|x| x.as_str()).unwrap_or("?");
                let ua = s.get("user_agent").and_then(|x| x.as_str()).unwrap_or("?");
                let at = s
                    .get("last_active_at")
                    .and_then(|x| x.as_str())
                    .unwrap_or("?");
                cli_out::say(format_args!("  {uid}  {ip:<16}  {ua:<8}  {at}"));
            }
            Ok(())
        }
        _ => anyhow::bail!("unknown session subcommand"),
    }
}

async fn cmd_user_create(sub: &clap::ArgMatches) -> Result<()> {
    let email = sub.get_one::<String>("email").unwrap().clone();
    let display_name = sub.get_one::<String>("display-name").unwrap().clone();
    let password = read_password(&format!("New password for {email}: "))?;
    let confirm = read_password("Confirm password: ")?;
    if password != confirm {
        anyhow::bail!("passwords do not match");
    }
    let jwt = mesh::request_iam_signup(mesh::DEFAULT_JOIN_PORT, &email, &password, &display_name)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    // Persist the JWT right away — creating a user implies they
    // should be able to act as that user immediately, and the round
    // trip from signup to login would otherwise force a second
    // password prompt.
    nauka_iam::save_token(&jwt).map_err(|e| anyhow::anyhow!("{e}"))?;
    cli_out::say(format_args!("user created: {email}"));
    cli_out::say(format_args!("  (also logged in as {email})"));
    Ok(())
}
