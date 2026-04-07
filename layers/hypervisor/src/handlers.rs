//! Hypervisor resource definition + handlers.
//!
//! Handlers delegate to fabric::ops. No plumbing here — just
//! translate OperationRequest → fabric call → OperationResponse.

use std::future::Future;
use std::pin::Pin;

use nauka_core::resource::*;
use nauka_state::LayerDb;

use nauka_core::ui;

use crate::controlplane;
use crate::fabric;
use crate::storage;

/// Build the hypervisor ResourceDef.
pub fn resource_def() -> ResourceDef {
    ResourceDef::build("hypervisor", "Manage hypervisors (compute hosts)")
        .alias("hv")
        .alias("node")
        .plural("hypervisors")
        // Lifecycle
        .action("init", "Initialize a new cluster")
        .op(|op| {
            op.with_arg(OperationArg::optional(
                "name",
                FieldDef::string("name", "Node name (defaults to hostname)"),
            ))
            .with_arg(OperationArg::optional(
                "region",
                FieldDef::string("region", "Region label").with_default("default"),
            ))
            .with_arg(OperationArg::optional(
                "zone",
                FieldDef::string("zone", "Zone label").with_default("default"),
            ))
            .with_arg(OperationArg::optional(
                "port",
                FieldDef::integer(
                    "port",
                    "Listen port (also uses port+1 for peering, port+2 for announce)",
                )
                .with_default("51820"),
            ))
            .with_arg(OperationArg::optional(
                "mode",
                FieldDef::string("mode", "Network mode: wireguard (default), direct, mock")
                    .with_default("wireguard"),
            ))
            .with_arg(OperationArg::optional(
                "interface",
                FieldDef::string(
                    "interface",
                    "Fabric network interface (e.g., eth1). Auto-detected if omitted",
                ),
            ))
            .with_arg(OperationArg::optional(
                "endpoint",
                FieldDef::string(
                    "endpoint",
                    "Public endpoint IP for peering (auto-detected if omitted)",
                ),
            ))
            .with_arg(OperationArg::required(
                "s3-endpoint",
                FieldDef::string("s3-endpoint", "S3 endpoint URL for region storage"),
            ))
            .with_arg(OperationArg::required(
                "s3-bucket",
                FieldDef::string("s3-bucket", "S3 bucket name for region storage"),
            ))
            .with_arg(OperationArg::required(
                "s3-access-key",
                FieldDef::secret("s3-access-key", "S3 access key"),
            ))
            .with_arg(OperationArg::required(
                "s3-secret-key",
                FieldDef::secret("s3-secret-key", "S3 secret key"),
            ))
            .with_arg(OperationArg::optional(
                "s3-region",
                FieldDef::string("s3-region", "S3 region (e.g., eu-central-1)").with_default(""),
            ))
            .with_arg(OperationArg::optional(
                "peering",
                FieldDef::flag(
                    "peering",
                    "Start peering listener after init (accepts joins)",
                ),
            ))
            .with_output(OutputKind::Resource)
            .with_example("nauka hypervisor init --name my-cloud --region eu --zone fsn1 --s3-endpoint https://s3.eu.example.com --s3-bucket nauka-eu --s3-access-key AKID --s3-secret-key SECRET --peering")
        })
        .action("join", "Join an existing cluster")
        .op(|op| {
            op.with_arg(OperationArg::required(
                "target",
                FieldDef::string("target", "IP or IP:port of an existing node"),
            ))
            .with_arg(OperationArg::optional(
                "name",
                FieldDef::string("name", "Node name (defaults to hostname)"),
            ))
            .with_arg(OperationArg::optional(
                "pin",
                FieldDef::string("pin", "PIN for auto-accept"),
            ))
            .with_arg(OperationArg::optional(
                "region",
                FieldDef::string("region", "Region label").with_default("default"),
            ))
            .with_arg(OperationArg::optional(
                "zone",
                FieldDef::string("zone", "Zone label").with_default("default"),
            ))
            .with_arg(OperationArg::optional(
                "port",
                FieldDef::integer(
                    "port",
                    "Listen port (also uses port+1 for peering, port+2 for announce)",
                )
                .with_default("51820"),
            ))
            .with_arg(OperationArg::optional(
                "mode",
                FieldDef::string("mode", "Network mode: wireguard (default), direct, mock")
                    .with_default("wireguard"),
            ))
            .with_output(OutputKind::Resource)
            .with_example(
                "nauka hypervisor join --target 46.224.166.60 --pin G7CCZX --region eu --zone nbg1",
            )
        })
        .action("status", "Show hypervisor status")
        .op(|op| op.with_output(OutputKind::Resource))
        .action("start", "Start hypervisor services (fabric, storage, tikv)")
        .action("stop", "Stop hypervisor services (fabric, storage, tikv)")
        .action("leave", "Leave the cluster and uninstall services")
        .op(|op| op.with_confirm())
        // CRUD
        .list()
        .op(|op| op.with_example("nauka hypervisor list"))
        .get()
        .op(|op| op.with_example("nauka hypervisor get HYPERVISOR-1"))
        .action("peering", "Start peering listener to accept new nodes")
        .op(|op| {
            op.with_arg(OperationArg::optional(
                "timeout",
                FieldDef::integer("timeout", "Listener timeout in seconds").with_default("3600"),
            ))
            .with_example("nauka hypervisor peering")
        })
        .action("doctor", "Diagnose hypervisor health")
        .action("announce-listen", "Run the announce listener (internal)")
        .op(|op| {
            op.with_arg(OperationArg::optional(
                "port",
                FieldDef::integer("port", "Announce listen port").with_default("51822"),
            ))
        })
        // Future
        .action("drain", "Evacuate all VMs before maintenance")
        .op(|op| op.with_confirm())
        .action("enable", "Enable for VM scheduling")
        // Table
        .column("NAME", "name")
        .column("REGION", "region")
        .column("ZONE", "zone")
        .column_def(ColumnDef::new("STATE", "state").with_format(DisplayFormat::Status))
        .column("CPU", "cpu")
        .column("MEMORY", "memory")
        .column("VMs", "vms")
        .empty_message("No hypervisors found. Initialize with: nauka hypervisor init")
        .detail_section(
            None,
            vec![
                DetailField::new("Name", "name"),
                DetailField::new("ID", "id"),
                DetailField::new("Region", "region"),
                DetailField::new("Zone", "zone"),
                DetailField::new("Address", "mesh_ipv6"),
                DetailField::new("State", "state").with_format(DisplayFormat::Status),
            ],
        )
        .done()
}

pub fn handler() -> HandlerFn {
    Box::new(|req: OperationRequest| -> Pin<Box<dyn Future<Output = anyhow::Result<OperationResponse>> + Send>> {
        Box::pin(async move {
            match req.operation.as_str() {
                "init" => handle_init(req).await,
                "status" => handle_status().await,
                "start" => handle_start().await,
                "stop" => handle_stop().await,
                "leave" => handle_leave().await,
                "list" => handle_list().await,
                "get" => handle_get(req).await,
                "join" => handle_join(req).await,
                "peering" => handle_peering(req).await,
                "doctor" => handle_doctor().await,
                "announce-listen" => handle_announce_listen(req).await,
                "drain" => Ok(OperationResponse::Message("drain: not yet implemented".into())),
                "enable" => Ok(OperationResponse::Message("enable: not yet implemented".into())),
                other => Ok(OperationResponse::Message(format!("unknown: {other}"))),
            }
        })
    })
}

pub fn registration() -> ResourceRegistration {
    ResourceRegistration {
        def: resource_def(),
        handler: handler(),
    }
}

// ═══════════════════════════════════════════════════
// Thin handlers — delegate to fabric::ops
// ═══════════════════════════════════════════════════

async fn handle_init(req: OperationRequest) -> anyhow::Result<OperationResponse> {
    let region = req
        .fields
        .get("region")
        .map(|s| s.as_str())
        .unwrap_or("default");
    let zone = req
        .fields
        .get("zone")
        .map(|s| s.as_str())
        .unwrap_or("default");
    let port: u16 = req
        .fields
        .get("port")
        .and_then(|s| s.parse().ok())
        .unwrap_or(51820);

    let network_mode: fabric::NetworkMode = req
        .fields
        .get("mode")
        .map(|s| s.as_str())
        .unwrap_or("wireguard")
        .parse()
        .unwrap_or_default();

    let fabric_interface = req
        .fields
        .get("interface")
        .map(|s| s.as_str())
        .unwrap_or("");

    let endpoint = req.fields.get("endpoint").cloned();

    let s3_endpoint = req
        .fields
        .get("s3-endpoint")
        .ok_or_else(|| anyhow::anyhow!("missing required field: s3-endpoint"))?
        .clone();
    let s3_bucket = req
        .fields
        .get("s3-bucket")
        .ok_or_else(|| anyhow::anyhow!("missing required field: s3-bucket"))?
        .clone();
    let s3_access_key = req
        .fields
        .get("s3-access-key")
        .ok_or_else(|| anyhow::anyhow!("missing required field: s3-access-key"))?
        .clone();
    let s3_secret_key = req
        .fields
        .get("s3-secret-key")
        .ok_or_else(|| anyhow::anyhow!("missing required field: s3-secret-key"))?
        .clone();
    let s3_region = req.fields.get("s3-region").cloned().unwrap_or_default();

    let node_name = req
        .fields
        .get("name")
        .filter(|s| !s.is_empty())
        .cloned()
        .unwrap_or_else(|| {
            hostname::get()
                .ok()
                .and_then(|h| h.into_string().ok())
                .map(|h| h.to_lowercase())
                .unwrap_or_else(|| "node".to_string())
        });

    let peering = req
        .fields
        .get("peering")
        .map(|s| s == "true")
        .unwrap_or(false);

    let db = open_db()?;
    let init_cfg = fabric::ops::InitConfig {
        node_name: &node_name,
        region,
        zone,
        port,
        network_mode,
        fabric_interface,
        endpoint,
    };

    // Generate a deterministic encryption password from the S3 secret key + region.
    // All nodes in the same region will derive the same password.
    let encryption_password = {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(s3_secret_key.as_bytes());
        hasher.update(b":zerofs:");
        hasher.update(region.as_bytes());
        format!("{:x}", hasher.finalize())
    };

    let region_storage = storage::region::RegionStorage {
        region: region.to_string(),
        s3_endpoint,
        s3_bucket,
        s3_access_key,
        s3_secret_key,
        s3_region,
        encryption_password,
        is_default: true,
    };

    // Init: 2 steps (fabric) + 4 steps (control plane) + 2 steps (storage) = 8
    let step_count = if network_mode == fabric::NetworkMode::WireGuard {
        8
    } else {
        2 // fabric only
    };
    let steps = ui::Steps::new(step_count);

    let result = fabric::ops::init(&db, &init_cfg, &steps)?;

    // Bootstrap control plane (TiKV) on the mesh — only in WireGuard mode
    if network_mode == fabric::NetworkMode::WireGuard {
        controlplane::ops::bootstrap(&node_name, &result.hypervisor.mesh_ipv6, &steps)?;
    }

    // Publish region storage config to distributed KV, then setup local storage
    if network_mode == fabric::NetworkMode::WireGuard {
        steps.set("Publishing storage config");
        let pd_endpoint = format!(
            "http://[{}]:{}",
            result.hypervisor.mesh_ipv6,
            controlplane::PD_CLIENT_PORT,
        );
        storage::ops::publish_region_config(&[pd_endpoint.as_str()], &region_storage).await?;
        steps.inc();
    }

    if network_mode == fabric::NetworkMode::WireGuard {
        steps.set("Setting up storage");
        storage::ops::setup_region(&db, region_storage.clone())?;
        steps.inc();
    }

    // Install persistent announce listener (don't start if --peering will run its own)
    if !peering {
        if let Err(e) = fabric::announce::install_service(port) {
            tracing::warn!(error = %e, "announce service install failed");
        }
    }

    steps.finish("Hypervisor initialized");
    eprintln!();
    eprintln!("  name     {}", result.hypervisor.name);
    eprintln!("  id       {}", result.hypervisor.id.as_str());
    eprintln!("  mesh     {}", result.mesh.id);
    eprintln!("  region   {region}/{zone}");
    eprintln!("  address  {}", result.hypervisor.mesh_ipv6);
    eprintln!("  pin      {}", result.pin);
    eprintln!();

    if peering {
        let peering_port = port + 1;
        eprintln!("  Peering active on port {peering_port}");
        eprintln!("  Nodes can join with:");
        eprintln!(
            "    nauka hypervisor join --target <this-ip>:{peering_port} --pin {}",
            result.pin
        );
        eprintln!();
        eprintln!("  Waiting for joins... (Ctrl+C to stop)");
        eprintln!();

        // Block here listening for joins (DB opened per-request, no lock held)
        let accepted = fabric::ops::listen_for_peers(
            &result.pin,
            peering_port,
            3600, // 1 hour timeout
        )
        .await?;

        eprintln!("  {} node(s) joined.", accepted);

        // Peering session ended — install announce service for persistent listening
        if let Err(e) = fabric::announce::install_service(port) {
            tracing::warn!(error = %e, "announce service install failed");
        }
    } else {
        eprintln!("  To accept joins on this node:");
        eprintln!("    nauka hypervisor peering");
        eprintln!();
        eprintln!("  Or from another node:");
        eprintln!(
            "    nauka hypervisor join --target <this-ip> --pin {}",
            result.pin
        );
    }

    Ok(OperationResponse::Resource(serde_json::json!({
        "name": result.hypervisor.name,
        "id": result.hypervisor.id.as_str(),
        "region": region,
        "zone": zone,
        "mesh_ipv6": result.hypervisor.mesh_ipv6.to_string(),
        "state": "available",
        "pin": result.pin,
    })))
}

async fn handle_join(req: OperationRequest) -> anyhow::Result<OperationResponse> {
    let target = req
        .fields
        .get("target")
        .ok_or_else(|| anyhow::anyhow!("missing required field: target"))?
        .clone();
    let region = req
        .fields
        .get("region")
        .map(|s| s.as_str())
        .unwrap_or("default");
    let zone = req
        .fields
        .get("zone")
        .map(|s| s.as_str())
        .unwrap_or("default");
    let port: u16 = req
        .fields
        .get("port")
        .and_then(|s| s.parse().ok())
        .unwrap_or(51820);
    let pin = req.fields.get("pin").map(|s| s.as_str());

    let network_mode: fabric::NetworkMode = req
        .fields
        .get("mode")
        .map(|s| s.as_str())
        .unwrap_or("wireguard")
        .parse()
        .unwrap_or_default();

    let node_name = req
        .fields
        .get("name")
        .filter(|s| !s.is_empty())
        .cloned()
        .unwrap_or_else(|| {
            hostname::get()
                .ok()
                .and_then(|h| h.into_string().ok())
                .map(|h| h.to_lowercase())
                .unwrap_or_else(|| "node".to_string())
        });

    let db = open_db()?;
    let join_cfg = fabric::ops::JoinConfig {
        target: &target,
        node_name: &node_name,
        region,
        zone,
        port,
        pin,
        network_mode,
    };

    // Join: 2 steps (fabric) + 3 steps (control plane) + 1 step (storage) + 1 step (announce) = 7
    let step_count = if network_mode == fabric::NetworkMode::WireGuard {
        7
    } else {
        3
    };
    let steps = ui::Steps::new(step_count);

    let result = fabric::ops::join(&db, &join_cfg, &steps).await?;

    // Join control plane — only in WireGuard mode
    if network_mode == fabric::NetworkMode::WireGuard {
        let state = fabric::state::FabricState::load(&db)
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .ok_or_else(|| anyhow::anyhow!("state missing after join"))?;
        let pd_endpoints: Vec<String> = state
            .peers
            .peers
            .iter()
            .map(|p| format!("http://[{}]:{}", p.mesh_ipv6, controlplane::PD_CLIENT_PORT))
            .collect();
        let peer_count = state.peers.len();
        controlplane::ops::join(
            &node_name,
            &result.hypervisor.mesh_ipv6,
            &pd_endpoints,
            peer_count,
            &steps,
        )?;
    }

    // Fetch region storage config from distributed KV and setup locally
    if network_mode == fabric::NetworkMode::WireGuard {
        steps.set("Setting up storage");
        let state = fabric::state::FabricState::load(&db)
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .ok_or_else(|| anyhow::anyhow!("state missing after join"))?;
        let pd_endpoints: Vec<String> = state
            .peers
            .peers
            .iter()
            .map(|p| format!("http://[{}]:{}", p.mesh_ipv6, controlplane::PD_CLIENT_PORT))
            .collect();
        let pd_refs: Vec<&str> = pd_endpoints.iter().map(|s| s.as_str()).collect();

        let region_config = storage::ops::fetch_region_config(&pd_refs, region)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "no storage backend configured for region '{region}'.\n\n\
                     An S3 backend must be registered before nodes can join this region.\n\
                     On the init node, re-initialize with storage flags:\n\n\
                     \x20 nauka hypervisor init --region {region} \\\n\
                     \x20   --s3-endpoint <URL> --s3-bucket <BUCKET> \\\n\
                     \x20   --s3-access-key <KEY> --s3-secret-key <SECRET>"
                )
            })?;

        storage::ops::setup_region(&db, region_config)?;
        steps.inc();
    }

    // Install persistent announce listener
    if let Err(e) = fabric::announce::install_service(port) {
        tracing::warn!(error = %e, "announce service install failed");
    }

    // Self-announce to all peers (ensures they know about us even if the
    // peering server's announce arrived before their listener was ready).
    // Re-read state after a delay to include peers discovered via incoming announces.
    steps.set("Announcing to peers");
    {
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        // Re-open DB to get latest state (may include peers from incoming announces)
        let db = open_db()?;
        let state = fabric::state::FabricState::load(&db)
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .ok_or_else(|| anyhow::anyhow!("state missing after join"))?;
        let self_info = fabric::peering::PeerInfo {
            name: state.hypervisor.name.clone(),
            region: state.hypervisor.region.clone(),
            zone: state.hypervisor.zone.clone(),
            wg_public_key: state.hypervisor.wg_public_key.clone(),
            wg_port: state.hypervisor.wg_port,
            endpoint: state.hypervisor.endpoint.clone(),
            mesh_ipv6: state.hypervisor.mesh_ipv6,
        };
        let peers: Vec<_> = state.peers.peers.clone();
        let (ok, fail) = fabric::announce::broadcast_new_peer(
            &self_info,
            &state.hypervisor.name,
            &peers,
            state.hypervisor.wg_port,
        )
        .await;
        if ok > 0 || fail > 0 {
            tracing::info!(successes = ok, failures = fail, "self-announce to peers");
        }
    }
    steps.inc();

    steps.finish("Joined cluster");
    eprintln!();
    eprintln!("  name     {}", result.hypervisor.name);
    eprintln!("  id       {}", result.hypervisor.id.as_str());
    eprintln!("  region   {region}/{zone}");
    eprintln!("  address  {}", result.hypervisor.mesh_ipv6);
    eprintln!("  peers    {}", result.peer_count);
    eprintln!();

    Ok(OperationResponse::Resource(serde_json::json!({
        "name": result.hypervisor.name,
        "id": result.hypervisor.id.as_str(),
        "region": region,
        "zone": zone,
        "mesh_ipv6": result.hypervisor.mesh_ipv6.to_string(),
        "state": "available",
        "peers": result.peer_count,
    })))
}

async fn handle_peering(req: OperationRequest) -> anyhow::Result<OperationResponse> {
    let timeout_secs: u64 = req
        .fields
        .get("timeout")
        .and_then(|s| s.parse().ok())
        .unwrap_or(3600);

    let db = open_db()?;
    let state = fabric::state::FabricState::load(&db)
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .ok_or_else(|| anyhow::anyhow!("not initialized. Run 'nauka hypervisor init' first."))?;

    // Derive PIN from secret
    let secret: nauka_core::crypto::MeshSecret = state.secret.parse()?;
    let pin = secret.derive_pin();
    let peering_port = state.hypervisor.wg_port + 1;

    // Drop DB before starting listener (release lock!)
    drop(db);

    eprintln!();
    eprintln!("  Peering active on port {peering_port}");
    eprintln!("  PIN: {pin}");
    eprintln!();
    eprintln!("  Nodes can join with:");
    eprintln!("    nauka hypervisor join --target <this-ip>:{peering_port} --pin {pin}");
    eprintln!();
    eprintln!("  Waiting for joins... (Ctrl+C to stop)");
    eprintln!();

    let accepted = fabric::ops::listen_for_peers(&pin, peering_port, timeout_secs).await?;

    Ok(OperationResponse::Message(format!(
        "{accepted} node(s) joined."
    )))
}

async fn handle_status() -> anyhow::Result<OperationResponse> {
    let db = open_db()?;
    let s = fabric::ops::status(&db)?;

    // Control plane status (best-effort — may not be installed yet)
    let mesh_ip: std::net::Ipv6Addr = s
        .mesh_ipv6
        .parse()
        .map_err(|_| anyhow::anyhow!("corrupt state: invalid mesh_ipv6 '{}'", s.mesh_ipv6))?;
    let cp = controlplane::ops::status(&mesh_ip);
    let (pd_active, tikv_active, pd_members, tikv_stores, leader) = match cp {
        Ok(cs) => (
            cs.pd_active,
            cs.tikv_active,
            cs.pd_members,
            cs.tikv_stores,
            cs.leader,
        ),
        Err(_) => (false, false, 0, 0, None),
    };

    Ok(OperationResponse::Resource(serde_json::json!({
        "name": s.hypervisor_name,
        "id": s.hypervisor_id,
        "region": s.region,
        "zone": s.zone,
        "mesh_ipv6": s.mesh_ipv6,
        "state": if s.service_active && tikv_active { &s.state } else if s.service_active { "degraded" } else { "down" },
        "wg": if s.service_active { "running" } else { "stopped" },
        "wg_interface": s.wg_interface_up,
        "peers": s.peer_count,
        "wg_port": s.wg_port,
        "rx_bytes": s.rx_bytes,
        "tx_bytes": s.tx_bytes,
        "pd": if pd_active { "running" } else { "stopped" },
        "tikv": if tikv_active { "running" } else { "stopped" },
        "pd_members": pd_members,
        "tikv_stores": tikv_stores,
        "leader": leader,
    })))
}

async fn handle_start() -> anyhow::Result<OperationResponse> {
    let db = open_db()?;
    fabric::ops::start(&db)?;
    let _ = fabric::announce::start_service();
    if let Err(e) = controlplane::ops::start() {
        eprintln!("  Warning: control plane: {e}");
    }
    if let Err(e) = storage::ops::start_all(&db) {
        eprintln!("  Warning: storage: {e}");
    }
    Ok(OperationResponse::Message("all services started.".into()))
}

async fn handle_announce_listen(req: OperationRequest) -> anyhow::Result<OperationResponse> {
    let port: u16 = req
        .fields
        .get("port")
        .and_then(|s| s.parse().ok())
        .unwrap_or(51822);

    let bind_addr: std::net::SocketAddr = format!("[::]:{port}")
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid port"))?;

    let db_opener = || {
        let dir = nauka_core::process::nauka_dir();
        let _ = std::fs::create_dir_all(&dir);
        nauka_state::LayerDb::open("hypervisor")
            .map_err(|e| nauka_core::error::NaukaError::internal(e.to_string()))
    };

    // This blocks forever (until killed by systemd)
    fabric::announce::listen(db_opener, bind_addr).await?;

    Ok(OperationResponse::None)
}

async fn handle_doctor() -> anyhow::Result<OperationResponse> {
    let report = crate::doctor::run();
    report.print();
    Ok(OperationResponse::None)
}

async fn handle_stop() -> anyhow::Result<OperationResponse> {
    let db = open_db()?;
    let _ = storage::ops::stop_all(&db);
    let _ = controlplane::ops::stop();
    let _ = fabric::announce::stop_service();
    fabric::ops::stop(&db)?;
    Ok(OperationResponse::Message("all services stopped.".into()))
}

async fn handle_leave() -> anyhow::Result<OperationResponse> {
    let db = open_db()?;
    // Get mesh IPv6 for TiKV deregistration before leaving
    let mesh_ipv6 = fabric::state::FabricState::load(&db)
        .ok()
        .flatten()
        .map(|s| s.hypervisor.mesh_ipv6);

    let steps = ui::Steps::new(4);

    // Storage first (stop ZeroFS instances)
    steps.set("Stopping storage");
    let _ = storage::ops::leave();
    steps.inc();

    // Controlplane (deregister TiKV store, then uninstall)
    steps.set("Leaving control plane");
    if let Some(ipv6) = mesh_ipv6 {
        let _ = controlplane::ops::leave_with_mesh(&ipv6);
    } else {
        let _ = controlplane::ops::leave();
    }
    steps.inc();

    // Notify peers + tear down mesh
    steps.set("Notifying peers");
    let _ = fabric::announce::uninstall_service();
    fabric::ops::leave(&db).await?;
    steps.inc();

    // Final cleanup
    steps.set("Removing services");
    steps.inc();

    steps.finish("Left the cluster");
    Ok(OperationResponse::None)
}

async fn handle_list() -> anyhow::Result<OperationResponse> {
    let db = open_db()?;
    let state = match fabric::state::FabricState::load(&db).map_err(|e| anyhow::anyhow!("{e}"))? {
        Some(s) => s,
        None => return Ok(OperationResponse::ResourceList(vec![])),
    };

    let backend = fabric::backend::create_backend(state.network_mode);
    let self_state = if backend.is_up() { "available" } else { "down" };

    let mut items = vec![serde_json::json!({
        "name": state.hypervisor.name,
        "region": state.hypervisor.region,
        "zone": state.hypervisor.zone,
        "state": self_state,
        "cpu": "0/0",
        "memory": "0/0",
        "vms": 0,
    })];

    for peer in &state.peers.peers {
        items.push(serde_json::json!({
            "name": peer.name,
            "region": peer.region,
            "zone": peer.zone,
            "state": peer_state_label(peer),
            "cpu": "0/0",
            "memory": "0/0",
            "vms": 0,
        }));
    }

    Ok(OperationResponse::ResourceList(items))
}

async fn handle_get(req: OperationRequest) -> anyhow::Result<OperationResponse> {
    let name = req
        .name
        .ok_or_else(|| anyhow::anyhow!("missing hypervisor name"))?;
    let db = open_db()?;
    let state = fabric::state::FabricState::load(&db)
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .ok_or_else(|| anyhow::anyhow!("not initialized"))?;

    let backend = fabric::backend::create_backend(state.network_mode);

    if state.hypervisor.name == name {
        return Ok(OperationResponse::Resource(serde_json::json!({
            "name": state.hypervisor.name,
            "id": state.hypervisor.id.as_str(),
            "region": state.hypervisor.region,
            "zone": state.hypervisor.zone,
            "mesh_ipv6": state.hypervisor.mesh_ipv6.to_string(),
            "state": if backend.is_up() { "available" } else { "down" },
        })));
    }

    if let Some(peer) = state.peers.find_by_name(&name) {
        return Ok(OperationResponse::Resource(serde_json::json!({
            "name": peer.name,
            "id": peer.id.as_str(),
            "region": peer.region,
            "zone": peer.zone,
            "mesh_ipv6": peer.mesh_ipv6.to_string(),
            "state": peer_state_label(peer),
        })));
    }

    anyhow::bail!("hypervisor '{name}' not found")
}

/// Map peer status to user-facing label.
fn peer_state_label(peer: &fabric::peer::Peer) -> &'static str {
    match peer.status {
        fabric::peer::PeerStatus::Active => "available",
        fabric::peer::PeerStatus::Unreachable => "unreachable",
        fabric::peer::PeerStatus::Removed => "removed",
    }
}

fn open_db() -> anyhow::Result<LayerDb> {
    let dir = nauka_core::process::nauka_dir();
    std::fs::create_dir_all(&dir)?;
    LayerDb::open("hypervisor").map_err(|e| anyhow::anyhow!("{e}"))
}
