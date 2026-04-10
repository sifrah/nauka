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
                "ipv6-block",
                FieldDef::string("ipv6-block", "Public IPv6 /64 block from hosting provider (e.g., 2a01:4f8:c012:abcd::/64)"),
            ))
            .with_arg(OperationArg::optional(
                "ipv4-public",
                FieldDef::string("ipv4-public", "Public IPv4 address of this server"),
            ))
            .with_arg(OperationArg::optional(
                "peering",
                FieldDef::flag(
                    "peering",
                    "Start peering listener after init (accepts joins)",
                ),
            ))
            .with_output(OutputKind::Resource)
            .with_progress(ProgressHint::Steps(11))
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
            .with_arg(OperationArg::optional(
                "ipv6-block",
                FieldDef::string("ipv6-block", "Public IPv6 /64 block from hosting provider (e.g., 2a01:4f8:c012:abcd::/64)"),
            ))
            .with_arg(OperationArg::optional(
                "ipv4-public",
                FieldDef::string("ipv4-public", "Public IPv4 address of this server"),
            ))
            .with_output(OutputKind::Resource)
            .with_progress(ProgressHint::Steps(10))
            .with_example(
                "nauka hypervisor join --target 46.224.166.60 --pin G7CCZX --region eu --zone nbg1",
            )
        })
        .action("update", "Update hypervisor configuration")
        .op(|op| {
            op.with_arg(OperationArg::optional(
                "ipv6-block",
                FieldDef::string("ipv6-block", "Public IPv6 /64 block (e.g., 2a01:4f8:c012:abcd::/64)"),
            ))
            .with_arg(OperationArg::optional(
                "ipv4-public",
                FieldDef::string("ipv4-public", "Public IPv4 address"),
            ))
            .with_arg(OperationArg::optional(
                "name",
                FieldDef::string("name", "Node name"),
            ))
            .with_output(OutputKind::Resource)
            .with_progress(ProgressHint::Spinner("Updating hypervisor..."))
            .with_example("nauka hypervisor update --ipv6-block 2a01:4f8:c012:abcd::/64")
        })
        .action("status", "Show hypervisor status")
        .op(|op| op.with_output(OutputKind::Resource))
        .action("start", "Start hypervisor services (fabric, storage, tikv)")
        .op(|op| op.with_output(OutputKind::Message).with_progress(ProgressHint::Spinner("Starting hypervisor services...")))
        .action("stop", "Stop hypervisor services (fabric, storage, tikv)")
        .op(|op| op.with_output(OutputKind::Message).with_progress(ProgressHint::Spinner("Stopping hypervisor services...")))
        .action("leave", "Leave the cluster and uninstall services")
        .op(|op| op.with_confirm().with_progress(ProgressHint::Steps(4)))
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
        .action("backup", "Create a backup of PD and TiKV data to S3")
        .op(|op| {
            op.with_output(OutputKind::Message)
                .with_progress(ProgressHint::Spinner("Creating backup..."))
                .with_example("nauka hypervisor backup")
        })
        .action("backup-list", "List available PD/TiKV backups in S3")
        .op(|op| {
            op.with_output(OutputKind::Message)
                .with_example("nauka hypervisor backup-list")
        })
        .action("cp-status", "Show control plane (PD/TiKV) cluster status")
        .op(|op| op.with_output(OutputKind::Message))
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
        .sort_by("name")
        .detail_section(
            None,
            vec![
                DetailField::new("Name", "name"),
                DetailField::new("ID", "id"),
                DetailField::new("Region", "region"),
                DetailField::new("Zone", "zone"),
                DetailField::new("Address", "mesh_ipv6"),
                DetailField::new("State", "state").with_format(DisplayFormat::Status),
                DetailField::new("CPU", "cpu"),
                DetailField::new("Memory", "memory"),
                DetailField::new("VMs", "vms"),
                DetailField::new("Created", "created_at").with_format(DisplayFormat::Timestamp),
            ],
        )
        .done()
}

pub fn handler() -> HandlerFn {
    Box::new(|req: OperationRequest| -> Pin<Box<dyn Future<Output = anyhow::Result<OperationResponse>> + Send>> {
        Box::pin(async move {
            match req.operation.as_str() {
                "init" => handle_init(req).await,
                "update" => handle_update(req).await,
                "status" => handle_status().await,
                "start" => handle_start().await,
                "stop" => handle_stop().await,
                "leave" => handle_leave().await,
                "list" => handle_list().await,
                "get" => handle_get(req).await,
                "join" => handle_join(req).await,
                "peering" => handle_peering(req).await,
                "backup" => handle_backup().await,
                "backup-list" => handle_backup_list().await,
                "cp-status" => handle_cp_status().await,
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
        children: vec![],
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

    let ipv6_block = req.fields.get("ipv6-block").cloned().or_else(|| {
        let detected = crate::detect::detect_ipv6_block();
        if let Some(ref v) = detected {
            eprintln!("  Auto-detected IPv6 block: {v}");
        }
        detected
    });
    let ipv4_public = req.fields.get("ipv4-public").cloned().or_else(|| {
        let detected = crate::detect::detect_ipv4_public();
        if let Some(ref v) = detected {
            eprintln!("  Auto-detected IPv4 address: {v}");
        }
        detected
    });

    let db = open_db()?;
    let init_cfg = fabric::ops::InitConfig {
        node_name: &node_name,
        region,
        zone,
        port,
        network_mode,
        fabric_interface,
        endpoint,
        ipv6_block,
        ipv4_public,
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

    // Init: 2 (fabric) + 4 (control plane) + 2 (storage) + 3 (compute+forge) = 11
    let step_count = if network_mode == fabric::NetworkMode::WireGuard {
        11
    } else {
        5 // fabric + compute + forge
    };
    let steps = ui::Steps::new(step_count);

    let result = fabric::ops::init(&db, &init_cfg, &steps)?;
    // Track how far we got for rollback on failure
    let fabric_initialized = true;
    let mut controlplane_initialized = false;
    let mut storage_initialized = false;

    // Rollback closure — undoes completed steps in reverse order
    let rollback = |fabric: bool, cp: bool, stor: bool| {
        tracing::warn!("init failed — rolling back");
        if stor {
            tracing::info!("rollback: uninstalling storage");
            let _ = storage::ops::leave();
        }
        if cp {
            tracing::info!("rollback: uninstalling control plane");
            let _ = controlplane::service::uninstall();
        }
        if fabric {
            tracing::info!("rollback: removing fabric state and interface");
            let backend = fabric::backend::create_backend(network_mode);
            let _ = backend.teardown();
            let _ = fabric::state::FabricState::delete(&db);
        }
    };

    // Bootstrap control plane (TiKV) on the mesh — only in WireGuard mode
    if network_mode == fabric::NetworkMode::WireGuard {
        if let Err(e) =
            controlplane::ops::bootstrap(&node_name, &result.hypervisor.mesh_ipv6, &steps)
        {
            steps.finish_err(&format!("Control plane failed: {e}"));
            rollback(fabric_initialized, false, false);
            return Err(e.into());
        }
        controlplane_initialized = true;
    }

    // Publish region storage config to distributed KV, then setup local storage
    if network_mode == fabric::NetworkMode::WireGuard {
        steps.set("Publishing storage config");
        let pd_endpoint = format!(
            "http://[{}]:{}",
            result.hypervisor.mesh_ipv6,
            controlplane::PD_CLIENT_PORT,
        );
        if let Err(e) =
            storage::ops::publish_region_config(&[pd_endpoint.as_str()], &region_storage).await
        {
            steps.finish_err(&format!("Storage config failed: {e}"));
            rollback(fabric_initialized, controlplane_initialized, false);
            anyhow::bail!("{e}");
        }
        steps.inc();
    }

    if network_mode == fabric::NetworkMode::WireGuard {
        steps.set("Setting up storage");
        if let Err(e) = storage::ops::setup_region(&db, region_storage.clone()) {
            steps.finish_err(&format!("Storage setup failed: {e}"));
            rollback(fabric_initialized, controlplane_initialized, false);
            return Err(e.into());
        }
        storage_initialized = true;
        steps.inc();
    }

    // Non-critical steps — warn but don't rollback
    if let Err(e) = crate::compute_setup::install(&steps) {
        tracing::warn!(error = %e, "compute setup failed (VMs won't work until fixed)");
    }

    if !peering {
        if let Err(e) = fabric::announce::install_service(port) {
            tracing::warn!(error = %e, "announce service install failed");
        }
    }

    // Suppress unused variable warnings
    let _ = (fabric_initialized, storage_initialized);

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

    let ipv6_block = req.fields.get("ipv6-block").cloned().or_else(|| {
        let detected = crate::detect::detect_ipv6_block();
        if let Some(ref v) = detected {
            eprintln!("  Auto-detected IPv6 block: {v}");
        }
        detected
    });
    let ipv4_public = req.fields.get("ipv4-public").cloned().or_else(|| {
        let detected = crate::detect::detect_ipv4_public();
        if let Some(ref v) = detected {
            eprintln!("  Auto-detected IPv4 address: {v}");
        }
        detected
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
        ipv6_block,
        ipv4_public,
    };

    // Join: 2 (fabric) + 3 (control plane) + 1 (storage) + 3 (compute+forge) + 1 (announce) = 10
    let step_count = if network_mode == fabric::NetworkMode::WireGuard {
        10
    } else {
        6
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
        let all_peer_infos: Vec<(&str, std::net::Ipv6Addr)> = state
            .peers
            .peers
            .iter()
            .map(|p| (p.name.as_str(), p.mesh_ipv6))
            .collect();
        controlplane::ops::join(
            &node_name,
            &result.hypervisor.mesh_ipv6,
            &pd_endpoints,
            peer_count,
            &all_peer_infos,
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

    // Install compute runtime + base image
    if let Err(e) = crate::compute_setup::install(&steps) {
        tracing::warn!(error = %e, "compute setup failed (VMs won't work until fixed)");
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

async fn handle_update(req: OperationRequest) -> anyhow::Result<OperationResponse> {
    let db = open_db()?;

    let ipv6_block = req.fields.get("ipv6-block").cloned();
    let ipv4_public = req.fields.get("ipv4-public").cloned();
    let name = req.fields.get("name").cloned();

    let cfg = fabric::ops::UpdateConfig {
        ipv6_block,
        ipv4_public,
        name,
    };

    let hv = fabric::ops::update(&db, &cfg)?;

    Ok(OperationResponse::Resource(serde_json::json!({
        "name": hv.name,
        "id": hv.id.as_str(),
        "region": hv.region,
        "zone": hv.zone,
        "mesh_ipv6": hv.mesh_ipv6.to_string(),
        "ipv6_block": hv.ipv6_block,
        "ipv4_public": hv.ipv4_public,
        "state": "available",
    })))
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

async fn handle_backup() -> anyhow::Result<OperationResponse> {
    let db = open_db()?;

    // Get S3 config from region registry
    let registry =
        storage::region::RegionRegistry::load(&db).map_err(|e| anyhow::anyhow!("{e}"))?;
    let config = registry
        .default_region()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no storage region configured.\n\n\
                 Initialize the cluster first with: nauka hypervisor init"
            )
        })?
        .clone();

    let mut results = Vec::new();

    // Backup PD
    match controlplane::backup::backup_pd(&config) {
        Ok(key) => results.push(format!("PD backup:   {key}")),
        Err(e) => results.push(format!("PD backup failed: {e}")),
    }

    // Backup TiKV
    match controlplane::backup::backup_tikv(&config) {
        Ok(key) => results.push(format!("TiKV backup: {key}")),
        Err(e) => results.push(format!("TiKV backup failed: {e}")),
    }

    Ok(OperationResponse::Message(results.join("\n")))
}

async fn handle_backup_list() -> anyhow::Result<OperationResponse> {
    let db = open_db()?;

    let registry =
        storage::region::RegionRegistry::load(&db).map_err(|e| anyhow::anyhow!("{e}"))?;
    let config = registry.default_region().ok_or_else(|| {
        anyhow::anyhow!(
            "no storage region configured.\n\n\
                 Initialize the cluster first with: nauka hypervisor init"
        )
    })?;

    let backups = controlplane::backup::list_backups(config).map_err(|e| anyhow::anyhow!("{e}"))?;

    if backups.is_empty() {
        return Ok(OperationResponse::Message("No backups found.".to_string()));
    }

    let mut lines = vec![format!("{:<65}  {:>10}  {}", "KEY", "SIZE", "MODIFIED")];
    lines.push("-".repeat(95));
    for b in &backups {
        lines.push(format!(
            "{:<65}  {:>10}  {}",
            b.key,
            controlplane::backup::format_size(b.size),
            b.last_modified,
        ));
    }

    Ok(OperationResponse::Message(lines.join("\n")))
}

async fn handle_cp_status() -> anyhow::Result<OperationResponse> {
    let db = open_db()?;
    let state = fabric::state::FabricState::load(&db)
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .ok_or_else(|| {
            anyhow::anyhow!("cluster not initialized. Run 'nauka hypervisor init' first.")
        })?;

    let mesh_ipv6 = state.hypervisor.mesh_ipv6;
    let pd_url = format!("http://[{}]:{}", mesh_ipv6, controlplane::PD_CLIENT_PORT);

    // --- PD Members ---
    let members_json = cp_api_get(&pd_url, "/pd/api/v1/members");
    let health_json = cp_api_get(&pd_url, "/pd/api/v1/health");

    // Build a set of healthy member IDs from the health endpoint
    let healthy_ids: std::collections::HashSet<u64> = health_json
        .as_ref()
        .ok()
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|h| h["member_id"].as_u64()).collect())
        .unwrap_or_default();

    let leader_id = members_json
        .as_ref()
        .ok()
        .and_then(|v| v["leader"]["member_id"].as_u64())
        .unwrap_or(0);

    println!("\n  PD Members");
    println!(
        "  {:<16} {:<42} {:<10} ROLE",
        "NAME", "CLIENT URL", "HEALTH"
    );
    println!("  {}", "-".repeat(78));

    if let Ok(ref val) = members_json {
        if let Some(members) = val["members"].as_array() {
            for m in members {
                let name = m["name"].as_str().unwrap_or("-");
                let client_urls = m["client_urls"]
                    .as_array()
                    .and_then(|a| a.first())
                    .and_then(|u| u.as_str())
                    .unwrap_or("-");
                let mid = m["member_id"].as_u64().unwrap_or(0);
                let health = if healthy_ids.contains(&mid) {
                    "healthy"
                } else {
                    "unhealthy"
                };
                let role = if mid == leader_id {
                    "leader"
                } else {
                    "follower"
                };
                println!("  {:<16} {:<42} {:<10} {}", name, client_urls, health, role);
            }
        }
    } else {
        println!("  (PD API unreachable)");
    }

    // --- TiKV Stores ---
    let stores_json = cp_api_get(&pd_url, "/pd/api/v1/stores");

    println!("\n  TiKV Stores");
    println!("  {:<8} {:<42} {:<12} CAPACITY", "ID", "ADDRESS", "STATE");
    println!("  {}", "-".repeat(72));

    if let Ok(ref val) = stores_json {
        if let Some(stores) = val["stores"].as_array() {
            for s in stores {
                let id = s["store"]["id"].as_u64().unwrap_or(0);
                let addr = s["store"]["address"].as_str().unwrap_or("-");
                let state_name = s["store"]["state_name"].as_str().unwrap_or("-");
                let capacity = s["status"]["capacity"].as_str().unwrap_or("-");
                println!("  {:<8} {:<42} {:<12} {}", id, addr, state_name, capacity);
            }
        }
    } else {
        println!("  (PD API unreachable)");
    }

    // --- Region Stats ---
    let regions_json = cp_api_get(&pd_url, "/pd/api/v1/stats/region");

    println!("\n  Region Stats");
    println!("  {}", "-".repeat(40));

    if let Ok(ref val) = regions_json {
        let count = val["count"].as_u64().unwrap_or(0);
        let empty = val["empty_count"].as_u64().unwrap_or(0);
        let miss_peer = val["miss_peer_region_count"].as_u64().unwrap_or(0);
        let extra_peer = val["extra_peer_region_count"].as_u64().unwrap_or(0);
        let healthy = count.saturating_sub(miss_peer).saturating_sub(extra_peer);

        println!("  Total:      {count}");
        println!("  Healthy:    {healthy}");
        println!("  Empty:      {empty}");
        println!("  Miss-peer:  {miss_peer}");
        println!("  Extra-peer: {extra_peer}");
    } else {
        println!("  (PD API unreachable)");
    }

    println!();
    Ok(OperationResponse::None)
}

/// Query PD HTTP API (used by cp-status handler).
fn cp_api_get(pd_url: &str, path: &str) -> Result<serde_json::Value, anyhow::Error> {
    let url = format!("{pd_url}{path}");
    let output = std::process::Command::new("curl")
        .args(["-sf", "--max-time", "5", &url])
        .output()
        .map_err(|e| anyhow::anyhow!("curl failed: {e}"))?;

    if !output.status.success() {
        anyhow::bail!("PD API request failed: {path}");
    }

    serde_json::from_slice(&output.stdout).map_err(|e| anyhow::anyhow!("PD API parse failed: {e}"))
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
