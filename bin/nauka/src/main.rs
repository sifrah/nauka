use std::sync::Arc;

use anyhow::Result;
use clap::{Arg, Command};
use nauka_hypervisor::daemon::{
    run_daemon, run_daemon_join, run_daemon_restart, run_mesh_down, DaemonConfig,
};
use nauka_state::Database;

#[tokio::main]
async fn main() -> Result<()> {
    // Init tracing. Defaults to `info` level; override via `RUST_LOG`.
    // Output goes to stderr so it coexists with println! on stdout.
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
        .subcommand(mesh_cmd());

    let matches = app.get_matches();

    match matches.subcommand() {
        Some(("mesh", sub)) => handle_mesh(sub).await,
        _ => anyhow::bail!("unknown subcommand — run 'nauka --help'"),
    }
}

async fn open_db() -> Result<Arc<Database>> {
    let db = Arc::new(Database::open(None).await?);
    nauka_state::load_schemas(&db, &[nauka_state::SCHEMA, nauka_hypervisor::SCHEMA]).await?;
    Ok(db)
}

fn mesh_cmd() -> Command {
    Command::new("mesh")
        .about("WireGuard mesh networking")
        .arg_required_else_help(true)
        .subcommand(
            Command::new("up")
                .about("Create a new mesh and run daemon")
                .arg(Arg::new("port").long("port").default_value("51820"))
                .arg(Arg::new("interface").long("interface").default_value("nauka0")),
        )
        .subcommand(
            Command::new("join")
                .about("Join an existing mesh and run daemon")
                .arg(Arg::new("host").required(true).help("Public IP of an existing node"))
                .arg(Arg::new("pin").long("pin").required(true))
                .arg(Arg::new("port").long("port").default_value("51820"))
                .arg(Arg::new("interface").long("interface").default_value("nauka0")),
        )
        .subcommand(Command::new("start").about("Restart daemon from saved state"))
        .subcommand(
            Command::new("down")
                .about("Permanently teardown mesh and delete state")
                .arg(Arg::new("interface").long("interface").default_value("nauka0")),
        )
        .subcommand(
            Command::new("status")
                .about("Show mesh interface status")
                .arg(Arg::new("interface").long("interface").default_value("nauka0")),
        )
        .subcommand(
            Command::new("peer")
                .about("Manage mesh peers")
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
}

async fn handle_mesh(matches: &clap::ArgMatches) -> Result<()> {
    match matches.subcommand() {
        Some(("up", sub)) => {
            let db = open_db().await?;
            let port: u16 = sub.get_one::<String>("port").unwrap().parse()?;
            let iface = sub.get_one::<String>("interface").unwrap().clone();
            run_daemon(db, DaemonConfig {
                interface_name: iface,
                listen_port: port,
                ..Default::default()
            })
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
        }
        Some(("join", sub)) => {
            let db = open_db().await?;
            let host = sub.get_one::<String>("host").unwrap().clone();
            let pin = sub.get_one::<String>("pin").unwrap().clone();
            let port: u16 = sub.get_one::<String>("port").unwrap().parse()?;
            let iface = sub.get_one::<String>("interface").unwrap().clone();
            run_daemon_join(
                db,
                &host,
                &pin,
                iface,
                port,
                nauka_hypervisor::mesh::DEFAULT_JOIN_PORT,
            )
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
        }
        Some(("start", _)) => {
            let db = open_db().await?;
            run_daemon_restart(db)
        }
            .await
            .map_err(|e| anyhow::anyhow!("{e}")),
        Some(("down", sub)) => {
            let db = open_db().await?;
            let iface = sub.get_one::<String>("interface").unwrap();
            run_mesh_down(db, iface)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))
        }
        Some(("status", sub)) => {
            let iface = sub.get_one::<String>("interface").unwrap();
            let status = nauka_hypervisor::mesh::Mesh::interface_status(iface)?;
            println!("{status:#?}");
            Ok(())
        }
        Some(("peer", sub)) => match sub.subcommand() {
            Some(("remove", rm)) => {
                let pk = rm.get_one::<String>("public-key").unwrap();
                nauka_hypervisor::mesh::request_peer_removal(
                    nauka_hypervisor::mesh::DEFAULT_JOIN_PORT,
                    pk,
                )?;
                println!("peer removal requested");
                Ok(())
            }
            _ => anyhow::bail!("unknown peer subcommand"),
        },
        _ => anyhow::bail!("unknown mesh subcommand"),
    }
}
