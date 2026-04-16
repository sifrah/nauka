use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Arg, ArgAction, Command};
use nauka_hypervisor::daemon::{
    init_hypervisor, join_hypervisor, leave_hypervisor, run_daemon, SetupConfig,
};
use nauka_hypervisor::mesh;
use nauka_hypervisor::systemd;
use nauka_state::Database;

#[tokio::main]
async fn main() -> Result<()> {
    // stderr so it coexists with println! on stdout. Level overridable via RUST_LOG.
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();

    let app = Command::new("nauka")
        .about("Nauka — turn dedicated servers into a programmable cloud")
        .version(option_env!("NAUKA_VERSION").unwrap_or(env!("CARGO_PKG_VERSION")))
        .arg_required_else_help(true)
        .subcommand(hypervisor_cmd());

    match app.get_matches().subcommand() {
        Some(("hypervisor", sub)) => handle_hypervisor(sub).await,
        _ => anyhow::bail!("unknown subcommand — run 'nauka --help'"),
    }
}

async fn open_db() -> Result<Arc<Database>> {
    let db = Arc::new(Database::open(None).await?);
    nauka_state::load_schemas(&db, &[nauka_state::SCHEMA, nauka_hypervisor::SCHEMA]).await?;
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
                .arg(Arg::new("interface").long("interface").default_value("nauka0")),
        )
        .subcommand(
            Command::new("join")
                .about("Join an existing mesh and start the hypervisor service")
                .arg(Arg::new("host").required(true).help("Public IP of an existing node"))
                .arg(Arg::new("pin").long("pin").required(true))
                .arg(Arg::new("port").long("port").default_value("51820"))
                .arg(Arg::new("interface").long("interface").default_value("nauka0")),
        )
        .subcommand(Command::new("status").about("Show cluster membership and local state"))
        .subcommand(
            Command::new("leave")
                .about("Leave the mesh — stop service, wipe state, remove unit file")
                .arg(Arg::new("interface").long("interface").default_value("nauka0")),
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
                        .arg(Arg::new("interface").long("interface").default_value("nauka0")),
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
                                .help("SurQL statement, e.g. \"UPDATE hypervisor SET ...\""),
                        ),
                ),
        )
}

async fn handle_hypervisor(matches: &clap::ArgMatches) -> Result<()> {
    match matches.subcommand() {
        Some(("init", sub)) => cmd_init(sub).await,
        Some(("join", sub)) => cmd_join(sub).await,
        Some(("status", _)) => cmd_status().await,
        Some(("leave", sub)) => cmd_leave(sub).await,
        Some(("daemon", _)) => cmd_daemon().await,
        Some(("mesh", sub)) => cmd_mesh(sub).await,
        Some(("peer", sub)) => cmd_peer(sub).await,
        Some(("debug", sub)) => cmd_debug(sub).await,
        _ => anyhow::bail!("unknown hypervisor subcommand"),
    }
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
    let db = open_db().await?;
    let config = parse_setup(sub)?;
    let summary = init_hypervisor(db, config)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Give SurrealKV a moment to flush + release the LOCK file before the
    // systemd-managed daemon tries to open the same path.
    drop_db_and_wait().await;

    install_and_start_service().context("install systemd unit")?;

    println!("mesh created");
    println!("  mesh:       {}", summary.mesh_id);
    println!("  public key: {}", summary.public_key);
    println!("  address:    {}", summary.address);
    println!("  raft:       {}", summary.raft_addr);
    println!();
    println!("  join pin:   {}", summary.pin);
    println!();
    println!("service: {} (running)", systemd::UNIT_NAME);
    println!("logs:    journalctl -u {} -f", systemd::UNIT_NAME);
    Ok(())
}

async fn cmd_join(sub: &clap::ArgMatches) -> Result<()> {
    let db = open_db().await?;
    let config = parse_setup(sub)?;
    let host = sub.get_one::<String>("host").unwrap().clone();
    let pin = sub.get_one::<String>("pin").unwrap().clone();
    let summary = join_hypervisor(db, &host, &pin, config)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    drop_db_and_wait().await;

    install_and_start_service().context("install systemd unit")?;

    println!("joined mesh");
    println!("  mesh:       {}", summary.mesh_id);
    println!("  public key: {}", summary.public_key);
    println!("  address:    {}", summary.address);
    println!("  raft:       {}", summary.raft_addr);
    println!();
    println!("service: {} (running)", systemd::UNIT_NAME);
    println!("logs:    journalctl -u {} -f", systemd::UNIT_NAME);
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
    let peering_open = v.get("peering_open").and_then(|x| x.as_bool()).unwrap_or(false);
    let empty = Vec::<serde_json::Value>::new();
    let hypervisors = v
        .get("hypervisors")
        .and_then(|x| x.as_array())
        .unwrap_or(&empty);

    println!("mesh:       {mesh_id}");
    println!("public key: {pk}");
    println!("address:    {addr}");
    println!(
        "peering:    {}",
        if peering_open { "open (accepts joins)" } else { "closed" }
    );
    println!();
    println!("hypervisors ({}):", hypervisors.len());
    for h in hypervisors {
        let hpk = h.get("public_key").and_then(|x| x.as_str()).unwrap_or("?");
        let haddr = h.get("address").and_then(|x| x.as_str()).unwrap_or("?");
        let ep = h.get("endpoint").and_then(|x| x.as_str()).unwrap_or("-");
        let is_self = if hpk == pk { " (self)" } else { "" };
        println!("  {hpk} at {haddr} via {ep}{is_self}");
    }
    Ok(())
}

async fn cmd_leave(sub: &clap::ArgMatches) -> Result<()> {
    let iface = sub.get_one::<String>("interface").unwrap().clone();
    // Ask the daemon to broadcast a DELETE for self + wait briefly for
    // Raft to replicate. Best-effort — the daemon may already be down.
    if let Err(e) = mesh::request_leave(mesh::DEFAULT_JOIN_PORT) {
        eprintln!("  ! cluster leave notification failed (continuing): {e}");
    } else {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
    systemd::stop_and_disable().map_err(|e| anyhow::anyhow!("{e}"))?;
    // Give SurrealKV's LOCK file time to release after the daemon exits.
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Now safe to open the DB and wipe state.
    leave_hypervisor(&iface)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
}

async fn cmd_daemon() -> Result<()> {
    let db = open_db().await?;
    run_daemon(db).await.map_err(|e| anyhow::anyhow!("{e}"))
}

async fn cmd_mesh(sub: &clap::ArgMatches) -> Result<()> {
    match sub.subcommand() {
        Some(("status", s)) => {
            let iface = s.get_one::<String>("interface").unwrap();
            let status = mesh::Mesh::interface_status(iface)?;
            println!("{status:#?}");
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
            println!("peer removal requested");
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
            println!("raft write ok");
            Ok(())
        }
        _ => anyhow::bail!("unknown debug subcommand"),
    }
}
