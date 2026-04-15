//! Hypervisor resource definition + handlers.
//!
//! Handlers delegate to fabric::ops. No plumbing here — just
//! translate OperationRequest → fabric call → OperationResponse.

use std::future::Future;
use std::pin::Pin;

use nauka_core::resource::*;
use nauka_state::EmbeddedDb;

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
                "max-pd-members",
                FieldDef::integer(
                    "max-pd-members",
                    "Maximum PD (Placement Driver) members (1, 3, 5, or 7)",
                )
                .with_default("3"),
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
        .action("backup", "Create a backup of PD and TiKV data to S3")
        .op(|op| {
            op.with_arg(OperationArg::optional(
                "hot",
                FieldDef::flag(
                    "hot",
                    "Hot backup: tar while services run (faster, but may have torn writes)",
                ),
            ))
            .with_output(OutputKind::Message)
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
        .action("upgrade-check", "Check TiKV/PD versions and upgrade readiness")
        .op(|op| {
            op.with_output(OutputKind::Message)
                .with_example("nauka hypervisor upgrade-check")
        })
        .action("upgrade", "Rolling upgrade of PD/TiKV on this node")
        .op(|op| {
            op.with_output(OutputKind::Message)
                .with_progress(ProgressHint::Steps(8))
                .with_example("nauka hypervisor upgrade")
        })
        .action("doctor", "Diagnose hypervisor health")
        .action(
            "daemon",
            "Run the hypervisor daemon (installed as nauka.service)",
        )
        .op(|op| op.with_output(OutputKind::None))
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
                "backup" => handle_backup(req).await,
                "backup-list" => handle_backup_list().await,
                "cp-status" => handle_cp_status().await,
                "upgrade-check" => handle_upgrade_check().await,
                "upgrade" => handle_upgrade().await,
                "doctor" => handle_doctor().await,
                "daemon" => handle_daemon().await,
                "drain" => handle_drain().await,
                "enable" => handle_enable().await,
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

    // #299: the old `--peering` flag used to keep an ephemeral
    // listener running inline after `init`. The hypervisor daemon
    // now takes over that role (installed at the end of this
    // handler) and listens continuously, gated by the PIN and
    // per-IP rate-limited, so the flag is no longer meaningful.
    // We silently ignore any `peering` field that may still show
    // up from scripted callers or stale docs.
    let _ = req.fields.get("peering");

    let max_pd_members: usize = req
        .fields
        .get("max-pd-members")
        .and_then(|s| s.parse().ok())
        .unwrap_or(controlplane::DEFAULT_MAX_PD_MEMBERS);
    if !controlplane::VALID_PD_MEMBER_COUNTS.contains(&max_pd_members) {
        anyhow::bail!(
            "invalid --max-pd-members value: {max_pd_members}. Must be one of: 1, 3, 5, 7"
        );
    }

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

    let db = open_db().await?;
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
        max_pd_members,
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

    // Init: 2 (fabric) + 5 (control plane) + 2 (storage) + 3 (compute+forge) = 12
    //
    // The control plane consumed 4 steps through P2.6 and gained a fifth
    // step — "Applying cluster schemas" — in P2.7 (sifrah/nauka#211) per
    // ADR 0004 (sifrah/nauka#210).
    let step_count = if network_mode == fabric::NetworkMode::WireGuard {
        12
    } else {
        5 // fabric + compute + forge
    };
    let steps = ui::Steps::new(step_count);

    let result = fabric::ops::init(&db, &init_cfg, &steps).await?;
    // Track how far we got for rollback on failure
    let fabric_initialized = true;
    let mut controlplane_initialized = false;
    let mut storage_initialized = false;

    // Rollback helper — undoes completed steps in reverse order. Written
    // as an async fn rather than a closure because `FabricState::delete`
    // is now async and rustc cannot infer the return type of a closure
    // that `.await`s across its body.
    async fn rollback(
        db: &EmbeddedDb,
        network_mode: fabric::NetworkMode,
        fabric_initialized: bool,
        controlplane_initialized: bool,
        storage_initialized: bool,
    ) {
        tracing::warn!("init failed — rolling back");
        if storage_initialized {
            tracing::info!("rollback: uninstalling storage");
            let _ = storage::ops::leave();
        }
        if controlplane_initialized {
            tracing::info!("rollback: uninstalling control plane");
            let _ = controlplane::service::uninstall();
        }
        if fabric_initialized {
            tracing::info!("rollback: removing fabric state and interface");
            let backend = fabric::backend::create_backend(network_mode);
            let _ = backend.teardown();
            let _ = fabric::state::FabricState::delete(db).await;
        }
    }

    // Bootstrap control plane (TiKV) on the mesh — only in WireGuard mode
    if network_mode == fabric::NetworkMode::WireGuard {
        if let Err(e) =
            controlplane::ops::bootstrap(&node_name, &result.hypervisor.mesh_ipv6, &steps).await
        {
            steps.finish_err(&format!("Control plane failed: {e}"));
            rollback(&db, network_mode, fabric_initialized, false, false).await;
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
            rollback(
                &db,
                network_mode,
                fabric_initialized,
                controlplane_initialized,
                false,
            )
            .await;
            anyhow::bail!("{e}");
        }
        steps.inc();
    }

    if network_mode == fabric::NetworkMode::WireGuard {
        steps.set("Setting up storage");
        if let Err(e) = storage::ops::setup_region(&db, region_storage.clone()).await {
            steps.finish_err(&format!("Storage setup failed: {e}"));
            rollback(
                &db,
                network_mode,
                fabric_initialized,
                controlplane_initialized,
                false,
            )
            .await;
            return Err(e.into());
        }
        storage_initialized = true;
        steps.inc();
    }

    // Suppress unused variable warnings
    let _ = (fabric_initialized, storage_initialized);

    // Release our in-process flock BEFORE installing the daemon: the
    // daemon opens `bootstrap.skv` on startup and would otherwise
    // race us. `shutdown()` polls the LOCK file for release so the
    // next open is guaranteed to succeed. From this point on, the
    // daemon owns the handle.
    db.shutdown().await.ok();

    // Install and start `nauka.service`. The daemon hosts the
    // peering TCP listener on `port + 1`, the announce listener on
    // `port + 2`, the health loop, the mesh reconciler, and the
    // operator control socket — all under one long-lived process.
    if let Err(e) = fabric::daemon::install_service() {
        tracing::warn!(error = %e, "hypervisor daemon install failed");
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
    eprintln!("  Daemon running. Nodes can join with:");
    eprintln!(
        "    nauka hypervisor join --target <this-ip> --pin {}",
        result.pin
    );
    eprintln!();

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

    let db = open_db().await?;
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
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .ok_or_else(|| anyhow::anyhow!("state missing after join"))?;
        let pd_endpoints: Vec<String> = state
            .peers
            .peers
            .iter()
            .map(|p| format!("http://[{}]:{}", p.mesh_ipv6, controlplane::PD_CLIENT_PORT))
            .collect();
        let peer_count = state.peers.len();
        let all_peer_infos: Vec<(&str, std::net::Ipv6Addr, u16)> = state
            .peers
            .peers
            .iter()
            .map(|p| (p.name.as_str(), p.mesh_ipv6, p.wg_port))
            .collect();
        controlplane::ops::join(
            &node_name,
            &result.hypervisor.mesh_ipv6,
            &pd_endpoints,
            peer_count,
            &all_peer_infos,
            state.max_pd_members,
            &steps,
        )?;
    }

    // Self-announce to all peers BEFORE storage setup.
    //
    // Why first: the peering server on the accepting node broadcasts our
    // PeerAnnounce to every existing peer, but that is best-effort — if
    // any recipient's announce listener is busy or the TCP connect fails,
    // that recipient never learns about us and its WireGuard config stays
    // without our public key. Packets we send to it over wg0 then get
    // dropped, which manifests later as `open_tikv: handshake timed out`
    // if that recipient happens to be the PD we (or the SDK) picks for
    // the storage step (sifrah/nauka#293).
    //
    // Doing the self-announce here, before storage setup, makes the join
    // establish bidirectional WireGuard peerage with every peer directly
    // — one TCP dial per peer, not relying on the accepting node's
    // broadcast — so that by the time `open_tikv` runs, the joining node
    // can actually reach every PD member.
    //
    // IMPORTANT (#282): this step must still run BEFORE
    // `announce::install_service`. That call does `systemctl enable --now`,
    // which spawns the forge service, which opens `bootstrap.skv` and
    // holds its flock. If we re-open the DB here after that, we lose
    // the race and the whole join exits 1 despite being semantically
    // successful. Reuse the already-open `db` handle.
    steps.set("Announcing to peers");
    {
        let state = fabric::state::FabricState::load(&db)
            .await
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

    // Fetch region storage config from distributed KV and setup locally
    if network_mode == fabric::NetworkMode::WireGuard {
        steps.set("Setting up storage");
        let state = fabric::state::FabricState::load(&db)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .ok_or_else(|| anyhow::anyhow!("state missing after join"))?;
        let pd_endpoints: Vec<String> = state
            .peers
            .peers
            .iter()
            .map(|p| format!("http://[{}]:{}", p.mesh_ipv6, controlplane::PD_CLIENT_PORT))
            .collect();

        // sifrah/nauka#293 — filter to currently-reachable PD endpoints
        // before handing the list to the TiKV SDK. Without this, the
        // SDK wastes its 10s handshake timeout on peers whose WG
        // propagation is still in flight, or on synthesized PD URLs
        // for TiKV-only peers that don't run PD at all. The helper
        // queries the first reachable PD for the authoritative member
        // list, so synthesized phantoms are dropped fast.
        let reachable_pds = controlplane::ops::wait_reachable_pds(&pd_endpoints, 30);
        if reachable_pds.is_empty() {
            return Err(anyhow::anyhow!(
                "no reachable PD endpoints after 30s (checked {})",
                pd_endpoints.len()
            ));
        }
        let pd_refs: Vec<&str> = reachable_pds.iter().map(|s| s.as_str()).collect();

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

        storage::ops::setup_region(&db, region_config).await?;
        steps.inc();
    }

    // Release our in-process flock BEFORE installing the hypervisor
    // daemon. The daemon opens `bootstrap.skv` as soon as
    // `systemctl --now` starts it, and would otherwise race our
    // handle. `EmbeddedDb::shutdown` polls the LOCK file for release,
    // guaranteeing the daemon's open will succeed on first try.
    // (Supersedes #282 which worked around this by carefully
    // ordering self-announce before the announce-service install.)
    db.shutdown().await.ok();

    if let Err(e) = fabric::daemon::install_service() {
        tracing::warn!(error = %e, "hypervisor daemon install failed");
    }

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

async fn handle_update(req: OperationRequest) -> anyhow::Result<OperationResponse> {
    let ipv6_block = req.fields.get("ipv6-block").cloned();
    let ipv4_public = req.fields.get("ipv4-public").cloned();
    let name = req.fields.get("name").cloned();

    let req = fabric::control::ControlRequest::Update {
        ipv6_block: ipv6_block.clone(),
        ipv4_public: ipv4_public.clone(),
        name: name.clone(),
    };

    let value = fabric::control::forward_or_fallback(
        req,
        || async move {
            let db = open_db().await?;
            let cfg = fabric::ops::UpdateConfig {
                ipv6_block,
                ipv4_public,
                name,
            };
            let hv = fabric::ops::update(&db, &cfg).await?;
            Ok(serde_json::json!({
                "name": hv.name,
                "id": hv.id.as_str(),
                "region": hv.region,
                "zone": hv.zone,
                "mesh_ipv6": hv.mesh_ipv6.to_string(),
                "ipv6_block": hv.ipv6_block,
                "ipv4_public": hv.ipv4_public,
            }))
        },
        Ok,
    )
    .await?;

    // The control-server response shape matches the fallback JSON
    // above but does not include the synthetic "state" field — add
    // it here so CLI output stays identical regardless of which path
    // served the request.
    let mut value = value;
    if let Some(obj) = value.as_object_mut() {
        obj.insert(
            "state".to_string(),
            serde_json::Value::String("available".to_string()),
        );
    }

    Ok(OperationResponse::Resource(value))
}

async fn handle_status() -> anyhow::Result<OperationResponse> {
    let value = fabric::control::forward_or_fallback(
        fabric::control::ControlRequest::Status,
        || async {
            let db = open_db().await?;
            fabric::ops::status_view(&db)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))
        },
        Ok,
    )
    .await?;

    Ok(OperationResponse::Resource(value))
}

async fn handle_start() -> anyhow::Result<OperationResponse> {
    // If the daemon is running, stop it briefly so we can open the
    // DB directly for the boot-up checks. We restart it at the end.
    let daemon_was_installed = fabric::daemon::is_service_installed();
    let daemon_was_active = fabric::daemon::is_service_active();
    if daemon_was_active {
        let _ = fabric::daemon::stop_service();
        // Give systemd a moment to actually release the LOCK.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    let db = open_db().await?;
    fabric::ops::start(&db).await?;
    if let Err(e) = controlplane::ops::start() {
        eprintln!("  Warning: control plane: {e}");
    }
    if let Err(e) = storage::ops::start_all(&db).await {
        eprintln!("  Warning: storage: {e}");
    }
    db.shutdown().await.ok();

    if daemon_was_installed {
        let _ = fabric::daemon::start_service();
    }
    Ok(OperationResponse::Message("all services started.".into()))
}

async fn handle_drain() -> anyhow::Result<OperationResponse> {
    fabric::control::forward_or_fallback(
        fabric::control::ControlRequest::Drain,
        || async {
            let db = open_db().await?;
            fabric::ops::drain(&db).await?;
            Ok(serde_json::Value::Null)
        },
        |_| Ok(()),
    )
    .await?;
    Ok(OperationResponse::Message(
        "node set to draining — no new VMs will be scheduled.".into(),
    ))
}

async fn handle_enable() -> anyhow::Result<OperationResponse> {
    fabric::control::forward_or_fallback(
        fabric::control::ControlRequest::Enable,
        || async {
            let db = open_db().await?;
            fabric::ops::enable(&db).await?;
            Ok(serde_json::Value::Null)
        },
        |_| Ok(()),
    )
    .await?;
    Ok(OperationResponse::Message(
        "node set to available — ready for VM scheduling.".into(),
    ))
}

async fn handle_daemon() -> anyhow::Result<OperationResponse> {
    fabric::daemon::run().await?;
    Ok(OperationResponse::None)
}

async fn handle_backup(req: OperationRequest) -> anyhow::Result<OperationResponse> {
    let hot = req.fields.get("hot").map(|s| s == "true").unwrap_or(false);

    // Scope the local EmbeddedDb so its SurrealKV flock is released
    // before `backup_logical` calls `controlplane::connect`, which
    // opens `bootstrap.skv` a second time to resolve PD endpoints.
    // Without the shutdown, the second open deadlocks on the flock.
    let config = {
        let db = open_db().await?;
        let registry = storage::region::RegionRegistry::load(&db)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let config = registry
            .default_region()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "no storage region configured.\n\n\
                     Initialize the cluster first with: nauka hypervisor init"
                )
            })?
            .clone();
        db.shutdown().await.ok();
        config
    };

    let mode = if hot { "hot" } else { "cold (consistent)" };
    let mut results = vec![format!("Backup mode: {mode}")];

    // Backup PD
    match controlplane::backup::backup_pd(&config, hot) {
        Ok(key) => results.push(format!("PD backup:   {key}")),
        Err(e) => results.push(format!("PD backup failed: {e}")),
    }

    // Backup TiKV
    match controlplane::backup::backup_tikv(&config, hot) {
        Ok(key) => results.push(format!("TiKV backup: {key}")),
        Err(e) => results.push(format!("TiKV backup failed: {e}")),
    }

    // P2.15 (sifrah/nauka#219): logical SurrealQL dump alongside the
    // physical tar backups. Survives independently: if the cluster is
    // reachable we capture a portable logical snapshot, otherwise we
    // fall through with a note and the tar backups still stand.
    match controlplane::backup::backup_logical(&config).await {
        Ok(key) => results.push(format!("Logical backup: {key}")),
        Err(e) => results.push(format!("Logical backup failed: {e}")),
    }

    Ok(OperationResponse::Message(results.join("\n")))
}

async fn handle_backup_list() -> anyhow::Result<OperationResponse> {
    let db = open_db().await?;

    let registry = storage::region::RegionRegistry::load(&db)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
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
    // #299: ask the daemon for mesh_ipv6 over the control socket so
    // we never touch bootstrap.skv while the daemon holds it. When
    // there is no daemon (init hasn't finished yet, recovery, test
    // harness) we fall back to a direct open + immediate shutdown —
    // which is still safe because nothing else is holding the flock
    // in that case.
    let mesh_ipv6_str: String = fabric::control::forward_or_fallback(
        fabric::control::ControlRequest::MeshIpv6,
        || async {
            let db = open_db().await?;
            let state = fabric::state::FabricState::load(&db)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .ok_or_else(|| {
                    anyhow::anyhow!("cluster not initialized. Run 'nauka hypervisor init' first.")
                })?;
            let ip = state.hypervisor.mesh_ipv6.to_string();
            db.shutdown().await.ok();
            Ok(serde_json::json!(ip))
        },
        |v| {
            v.as_str()
                .map(|s| s.to_string())
                .ok_or_else(|| anyhow::anyhow!("mesh_ipv6: expected string"))
        },
    )
    .await?;
    let mesh_ipv6: std::net::Ipv6Addr = mesh_ipv6_str
        .parse()
        .map_err(|_| anyhow::anyhow!("corrupt mesh_ipv6: {mesh_ipv6_str}"))?;

    // Tighter per-request timeout than the default 10s — cp-status is
    // an operator-facing read path and should fail fast if PD is stuck
    // rather than hold the terminal for 30 s across three endpoints.
    let client = controlplane::pd_client::PdClient::from_mesh(&mesh_ipv6).with_timeout(5);

    // --- PD Members ---
    let members_result = client.get_members();

    println!("\n  PD Members");
    println!(
        "  {:<16} {:<42} {:<10} ROLE",
        "NAME", "CLIENT URL", "HEALTH"
    );
    println!("  {}", "-".repeat(78));

    if let Ok(ref members) = members_result {
        for m in members {
            let client_url = m.client_urls.first().map(|s| s.as_str()).unwrap_or("-");
            let health = if m.is_healthy { "healthy" } else { "unhealthy" };
            let role = if m.is_leader { "leader" } else { "follower" };
            let name = if m.name.is_empty() { "-" } else { &m.name };
            println!("  {:<16} {:<42} {:<10} {}", name, client_url, health, role);
        }
    } else {
        println!("  (PD API unreachable)");
    }

    // --- TiKV Stores ---
    let stores_result = client.get_stores();

    println!("\n  TiKV Stores");
    println!("  {:<8} {:<42} {:<12} CAPACITY", "ID", "ADDRESS", "STATE");
    println!("  {}", "-".repeat(72));

    if let Ok(ref stores) = stores_result {
        for s in stores {
            let addr = if s.address.is_empty() {
                "-"
            } else {
                &s.address
            };
            let state_name = if s.state_name.is_empty() {
                "-"
            } else {
                &s.state_name
            };
            let capacity = if s.capacity.is_empty() {
                "-"
            } else {
                &s.capacity
            };
            println!("  {:<8} {:<42} {:<12} {}", s.id, addr, state_name, capacity);
        }
    } else {
        println!("  (PD API unreachable)");
    }

    // --- Region Stats ---
    let stats_result = client.get_region_stats();

    println!("\n  Region Stats");
    println!("  {}", "-".repeat(40));

    if let Ok(ref stats) = stats_result {
        let healthy = stats
            .count
            .saturating_sub(stats.miss_peer)
            .saturating_sub(stats.extra_peer);

        println!("  Total:      {}", stats.count);
        println!("  Healthy:    {healthy}");
        println!("  Empty:      {}", stats.empty_count);
        println!("  Miss-peer:  {}", stats.miss_peer);
        println!("  Extra-peer: {}", stats.extra_peer);
    } else {
        println!("  (PD API unreachable)");
    }

    println!();
    Ok(OperationResponse::None)
}

async fn handle_upgrade_check() -> anyhow::Result<OperationResponse> {
    let mut lines = Vec::new();

    lines.push("  Component versions:".to_string());
    lines.push(String::new());

    // Expected versions
    let expected_pd = controlplane::PD_VERSION;
    let expected_tikv = controlplane::TIKV_VERSION;

    // Installed versions
    let installed_pd = controlplane::service::installed_pd_version();
    let installed_tikv = controlplane::service::installed_tikv_version();

    let pd_match = installed_pd.as_deref() == Some(expected_pd);
    let tikv_match = installed_tikv.as_deref() == Some(expected_tikv);

    let pd_installed = installed_pd.as_deref().unwrap_or("not installed");
    let tikv_installed = installed_tikv.as_deref().unwrap_or("not installed");

    let pd_icon = if pd_match {
        "\x1b[32m✓\x1b[0m"
    } else {
        "\x1b[33m!\x1b[0m"
    };
    let tikv_icon = if tikv_match {
        "\x1b[32m✓\x1b[0m"
    } else {
        "\x1b[33m!\x1b[0m"
    };

    lines.push(format!(
        "    {pd_icon} PD     installed: {pd_installed:<12} expected: {expected_pd}"
    ));
    lines.push(format!(
        "    {tikv_icon} TiKV   installed: {tikv_installed:<12} expected: {expected_tikv}"
    ));

    // Service status
    lines.push(String::new());
    lines.push("  Service status:".to_string());
    lines.push(String::new());

    let pd_active = controlplane::service::pd_is_active();
    let tikv_active = controlplane::service::tikv_is_active();
    lines.push(format!(
        "    PD:   {}",
        if pd_active { "running" } else { "stopped" }
    ));
    lines.push(format!(
        "    TiKV: {}",
        if tikv_active { "running" } else { "stopped" }
    ));

    // Verdict
    lines.push(String::new());
    if pd_match && tikv_match {
        lines.push(
            "  \x1b[32mAll components at expected versions. No upgrade needed.\x1b[0m".to_string(),
        );
    } else {
        lines
            .push("  \x1b[33mVersion mismatch detected. Upgrade may be needed.\x1b[0m".to_string());
        lines.push("  Run 'nauka hypervisor upgrade' to perform a rolling upgrade.".to_string());
    }

    let output = lines.join("\n");
    eprintln!("{output}");
    Ok(OperationResponse::None)
}

async fn handle_upgrade() -> anyhow::Result<OperationResponse> {
    let db = open_db().await?;
    let state = fabric::state::FabricState::load(&db)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .ok_or_else(|| {
            anyhow::anyhow!("cluster not initialized. Run 'nauka hypervisor init' first.")
        })?;

    let mesh_ipv6 = state.hypervisor.mesh_ipv6;
    let pd_client = controlplane::pd_client::PdClient::from_mesh(&mesh_ipv6);
    let has_pd = controlplane::service::has_pd_unit();

    let target_pd = controlplane::PD_VERSION;
    let target_tikv = controlplane::TIKV_VERSION;

    // ── Step 1: Check if upgrade is needed ──────────────────
    let steps = ui::Steps::new(8);
    steps.set("Checking versions");

    let installed_pd = controlplane::service::installed_pd_version();
    let installed_tikv = controlplane::service::installed_tikv_version();

    let pd_needs = installed_pd.as_deref() != Some(target_pd);
    let tikv_needs = installed_tikv.as_deref() != Some(target_tikv);

    if !pd_needs && !tikv_needs {
        steps.finish("Already at latest version");
        return Ok(OperationResponse::Message(format!(
            "Already at latest version (PD {target_pd}, TiKV {target_tikv}). Nothing to do."
        )));
    }

    let installed_pd_str = installed_pd.clone().unwrap_or_else(|| "unknown".into());
    let installed_tikv_str = installed_tikv.clone().unwrap_or_else(|| "unknown".into());
    steps.inc();

    // ── Step 2: Pre-flight — verify cluster healthy ─────────
    steps.set("Pre-flight health check");

    // PD health
    if !pd_client.is_healthy() {
        steps.finish_err("PD health check failed");
        anyhow::bail!("pre-flight failed: PD not healthy");
    }

    // All TiKV stores Up
    let stores = pd_client.get_stores().map_err(|e| {
        steps.finish_err("Failed to get stores");
        anyhow::anyhow!("pre-flight failed: {e}")
    })?;
    for store in &stores {
        if store.state_name != "Up" {
            steps.finish_err("TiKV store not Up");
            anyhow::bail!(
                "pre-flight failed: store {} is {} (expected Up)",
                store.address,
                store.state_name
            );
        }
    }
    steps.inc();

    // ── Step 3: Drain this node ─────────────────────────────
    steps.set("Draining node");
    // Ignore "already draining" errors — the node may already be drained.
    let drain_result = fabric::ops::drain(&db).await;
    if let Err(ref e) = drain_result {
        let msg = e.to_string();
        if !msg.contains("already draining") {
            steps.finish_err("Drain failed");
            anyhow::bail!("drain failed: {e}");
        }
    }
    steps.inc();

    // From here on, if anything fails, attempt rollback:
    // restart old services and re-enable the node. Awaits the
    // `EmbeddedDb` path, so it's an async helper rather than a closure.
    async fn upgrade_rollback(db: &EmbeddedDb, steps: &ui::Steps, reason: &str) {
        steps.finish_err(reason);
        tracing::warn!("upgrade failed, attempting rollback: {reason}");
        let _ = controlplane::service::start();
        // Re-enable scheduling (best-effort, ignore error). Drive the
        // async path directly through the existing EmbeddedDb handle so
        // we don't race ourselves on the SurrealKV flock.
        if let Ok(Some(mut st)) = fabric::state::FabricState::load(db).await {
            st.node_state = fabric::state::NodeState::Available;
            let _ = st.save(db).await;
        }
    }

    // ── Step 4: Create backup ───────────────────────────────
    steps.set("Creating backup");
    let registry = storage::region::RegionRegistry::load(&db)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"));
    match registry {
        Ok(reg) => {
            if let Some(config) = reg.default_region() {
                if let Err(e) = controlplane::backup::backup_pd(config, false) {
                    tracing::warn!("PD backup failed (non-fatal): {e}");
                }
                if let Err(e) = controlplane::backup::backup_tikv(config, false) {
                    tracing::warn!("TiKV backup failed (non-fatal): {e}");
                }
                if let Err(e) = controlplane::backup::backup_logical(config).await {
                    tracing::warn!("logical backup failed (non-fatal): {e}");
                }
            } else {
                tracing::warn!("no storage region configured, skipping backup");
            }
        }
        Err(e) => {
            tracing::warn!("could not load region registry, skipping backup: {e}");
        }
    }
    steps.inc();

    // ── Step 5: Stop services ───────────────────────────────
    steps.set("Stopping TiKV and PD");
    if let Err(e) = controlplane::service::stop() {
        upgrade_rollback(&db, &steps, &format!("stop failed: {e}")).await;
        anyhow::bail!("failed to stop services: {e}");
    }
    steps.inc();

    // ── Step 6: Install new versions via TiUP ───────────────
    steps.set(&format!("Installing PD {target_pd} + TiKV {target_tikv}"));
    if let Err(e) = controlplane::service::install_version(target_pd, target_tikv) {
        upgrade_rollback(&db, &steps, &format!("install failed: {e}")).await;
        anyhow::bail!("binary install failed: {e}");
    }

    // Regenerate systemd units so `tiup pd`/`tiup tikv` picks up the
    // new version automatically (TiUP uses the latest installed version).
    if let Err(e) = controlplane::service::regenerate_units(has_pd) {
        upgrade_rollback(&db, &steps, &format!("unit regeneration failed: {e}")).await;
        anyhow::bail!("systemd unit regeneration failed: {e}");
    }
    steps.inc();

    // ── Step 7: Start services ──────────────────────────────
    steps.set("Starting PD and TiKV");
    if let Err(e) = controlplane::service::start() {
        upgrade_rollback(&db, &steps, &format!("start failed: {e}")).await;
        anyhow::bail!("failed to start services after upgrade: {e}");
    }
    steps.inc();

    // ── Step 8: Wait for health ─────────────────────────────
    steps.set("Waiting for cluster health");

    if has_pd {
        if let Err(e) = controlplane::service::wait_pd_ready(&mesh_ipv6, 60) {
            upgrade_rollback(&db, &steps, &format!("PD health timeout: {e}")).await;
            anyhow::bail!("PD did not become healthy after upgrade: {e}");
        }
    }

    if let Err(e) = controlplane::service::wait_store_up(&mesh_ipv6, 120) {
        upgrade_rollback(&db, &steps, &format!("TiKV store timeout: {e}")).await;
        anyhow::bail!("TiKV store did not come back Up after upgrade: {e}");
    }
    steps.inc();

    // ── Re-enable scheduling ────────────────────────────────
    // Ignore "already available" errors.
    let _ = fabric::ops::enable(&db).await;

    steps.finish("Upgrade complete");

    let msg = format!(
        "Upgraded PD {} -> {} and TiKV {} -> {}. Node re-enabled for scheduling.",
        installed_pd_str, target_pd, installed_tikv_str, target_tikv,
    );
    Ok(OperationResponse::Message(msg))
}

async fn handle_doctor() -> anyhow::Result<OperationResponse> {
    let report = crate::doctor::run().await;
    report.print();
    Ok(OperationResponse::None)
}

async fn handle_stop() -> anyhow::Result<OperationResponse> {
    // Stop the daemon first so we can open the DB without flock
    // contention for the rest of the teardown.
    let _ = fabric::daemon::request_shutdown_and_wait(std::time::Duration::from_secs(5)).await;
    let _ = fabric::daemon::stop_service();

    let db = open_db().await?;
    let _ = storage::ops::stop_all(&db).await;
    let _ = controlplane::ops::stop();
    fabric::ops::stop(&db).await?;
    Ok(OperationResponse::Message("all services stopped.".into()))
}

async fn handle_leave() -> anyhow::Result<OperationResponse> {
    let steps = ui::Steps::new(5);

    // 1. Stop the daemon if it's running so the bootstrap.skv flock
    //    is free before we open the DB directly for teardown.
    steps.set("Stopping daemon");
    if let Err(e) =
        fabric::daemon::request_shutdown_and_wait(std::time::Duration::from_secs(10)).await
    {
        tracing::warn!(error = %e, "daemon shutdown request failed, continuing");
    }
    steps.inc();

    let db = open_db().await?;
    // Get mesh IPv6 for TiKV deregistration before leaving
    let mesh_ipv6 = fabric::state::FabricState::load(&db)
        .await
        .ok()
        .flatten()
        .map(|s| s.hypervisor.mesh_ipv6);

    // 2. Storage (stop ZeroFS instances)
    steps.set("Stopping storage");
    let _ = storage::ops::leave();
    steps.inc();

    // 3. Controlplane (deregister TiKV store, then uninstall). The
    //    `leave_with_mesh` variant reads peer state via its own
    //    short-lived EmbeddedDb handle — safe now that our daemon is
    //    no longer holding the flock.
    steps.set("Leaving control plane");
    if let Some(ipv6) = mesh_ipv6 {
        let _ = controlplane::ops::leave_with_mesh(&ipv6).await;
    } else {
        let _ = controlplane::ops::leave();
    }
    steps.inc();

    // 4. Notify peers + tear down mesh
    steps.set("Notifying peers");
    fabric::ops::leave(&db).await?;
    steps.inc();

    // Release our DB handle before uninstalling the daemon unit, so
    // `daemon-reload` doesn't race anything still holding `LOCK`.
    db.shutdown().await.ok();

    // 5. Final cleanup: remove the daemon systemd unit.
    steps.set("Removing services");
    let _ = fabric::daemon::uninstall_service();
    steps.inc();

    steps.finish("Left the cluster");
    Ok(OperationResponse::None)
}

async fn handle_list() -> anyhow::Result<OperationResponse> {
    let value = fabric::control::forward_or_fallback(
        fabric::control::ControlRequest::List,
        || async {
            let db = open_db().await?;
            fabric::ops::list_view(&db)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))
        },
        Ok,
    )
    .await?;

    let items: Vec<serde_json::Value> = value.as_array().cloned().unwrap_or_default();
    Ok(OperationResponse::ResourceList(items))
}

async fn handle_get(req: OperationRequest) -> anyhow::Result<OperationResponse> {
    let name = req
        .name
        .ok_or_else(|| anyhow::anyhow!("missing hypervisor name"))?;
    let name_for_fallback = name.clone();

    let value = fabric::control::forward_or_fallback(
        fabric::control::ControlRequest::Get { name },
        || async move {
            let db = open_db().await?;
            fabric::ops::get_view(&db, &name_for_fallback)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))
        },
        Ok,
    )
    .await?;

    Ok(OperationResponse::Resource(value))
}

async fn open_db() -> anyhow::Result<EmbeddedDb> {
    EmbeddedDb::open_default()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
}
