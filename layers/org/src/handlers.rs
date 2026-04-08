//! Resource definitions + handlers for Org, Project, Environment.
//!
//! Each ResourceDef auto-generates CLI commands (clap) and API routes (axum).
//! Handlers are thin — they delegate to store operations.

use std::future::Future;
use std::pin::Pin;

use nauka_core::resource::*;
use nauka_hypervisor::controlplane;
use nauka_hypervisor::fabric;
use nauka_state::LocalDb;

use crate::store::OrgStore;

// ═══════════════════════════════════════════════════
// Connect to ClusterDb via PD endpoints from fabric state
// ═══════════════════════════════════════════════════

async fn connect_store() -> anyhow::Result<OrgStore> {
    let dir = nauka_core::process::nauka_dir();
    let _ = std::fs::create_dir_all(&dir);
    let db = LocalDb::open("hypervisor")?;

    let state = fabric::state::FabricState::load(&db)
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "cluster not initialized.\n\n\
                 Initialize a cluster first with:\n\
                 \x20 nauka hypervisor init"
            )
        })?;

    // Build PD endpoints from self + peers
    let self_endpoint = format!(
        "http://[{}]:{}",
        state.hypervisor.mesh_ipv6,
        controlplane::PD_CLIENT_PORT,
    );
    let mut endpoints = vec![self_endpoint];
    for peer in &state.peers.peers {
        endpoints.push(format!(
            "http://[{}]:{}",
            peer.mesh_ipv6,
            controlplane::PD_CLIENT_PORT,
        ));
    }
    let refs: Vec<&str> = endpoints.iter().map(|s| s.as_str()).collect();

    let cluster_db = controlplane::ClusterDb::connect(&refs).await?;
    Ok(OrgStore::new(cluster_db))
}

// ═══════════════════════════════════════════════════
// Org
// ═══════════════════════════════════════════════════

fn org_def() -> ResourceDef {
    ResourceDef::build("org", "Manage organizations")
        .alias("organization")
        .plural("orgs")
        .scope_global()
        .crud()
        .column("NAME", "name")
        .column("ID", "id")
        .column_def(ColumnDef::new("CREATED", "created_at").with_format(DisplayFormat::Timestamp))
        .empty_message("No organizations found. Create one with: nauka org create <name>")
        .detail_section(
            None,
            vec![
                DetailField::new("Name", "name"),
                DetailField::new("ID", "id"),
                DetailField::new("Created", "created_at").with_format(DisplayFormat::Timestamp),
            ],
        )
        .done()
}

fn org_handler() -> HandlerFn {
    Box::new(
        |req: OperationRequest| -> Pin<
            Box<dyn Future<Output = anyhow::Result<OperationResponse>> + Send>,
        > {
            Box::pin(async move {
                match req.operation.as_str() {
                    "create" => {
                        let name = req
                            .name
                            .ok_or_else(|| anyhow::anyhow!("missing org name"))?;
                        nauka_core::validate::name(&name)?;
                        let store = connect_store().await?;
                        let org = store.create_org(&name).await?;
                        Ok(OperationResponse::Resource(serde_json::json!({
                            "name": org.name,
                            "id": org.id.as_str(),
                            "created_at": org.created_at,
                        })))
                    }
                    "list" => {
                        let store = connect_store().await?;
                        let orgs = store.list_orgs().await?;
                        let items: Vec<serde_json::Value> = orgs
                            .iter()
                            .map(|o| {
                                serde_json::json!({
                                    "name": o.name,
                                    "id": o.id.as_str(),
                                    "created_at": o.created_at,
                                })
                            })
                            .collect();
                        Ok(OperationResponse::ResourceList(items))
                    }
                    "get" => {
                        let name = req
                            .name
                            .ok_or_else(|| anyhow::anyhow!("missing org name or ID"))?;
                        let store = connect_store().await?;
                        let org = store
                            .get_org(&name)
                            .await?
                            .ok_or_else(|| anyhow::anyhow!("org '{name}' not found"))?;
                        Ok(OperationResponse::Resource(serde_json::json!({
                            "name": org.name,
                            "id": org.id.as_str(),
                            "created_at": org.created_at,
                        })))
                    }
                    "delete" => {
                        let name = req
                            .name
                            .ok_or_else(|| anyhow::anyhow!("missing org name or ID"))?;
                        let store = connect_store().await?;
                        store.delete_org(&name).await?;
                        Ok(OperationResponse::Message(format!(
                            "org '{name}' deleted."
                        )))
                    }
                    other => Ok(OperationResponse::Message(format!("unknown: {other}"))),
                }
            })
        },
    )
}

pub fn org_registration() -> ResourceRegistration {
    ResourceRegistration {
        def: org_def(),
        handler: org_handler(),
    }
}

// ═══════════════════════════════════════════════════
// Project
// ═══════════════════════════════════════════════════

fn project_def() -> ResourceDef {
    ResourceDef::build("project", "Manage projects within an organization")
        .plural("projects")
        .parent("org", "--org", "Organization")
        .crud()
        .column("NAME", "name")
        .column("ORG", "org_name")
        .column("ID", "id")
        .column_def(ColumnDef::new("CREATED", "created_at").with_format(DisplayFormat::Timestamp))
        .empty_message(
            "No projects found. Create one with: nauka project create <name> --org <org>",
        )
        .detail_section(
            None,
            vec![
                DetailField::new("Name", "name"),
                DetailField::new("ID", "id"),
                DetailField::new("Organization", "org_name"),
                DetailField::new("Created", "created_at").with_format(DisplayFormat::Timestamp),
            ],
        )
        .done()
}

fn project_handler() -> HandlerFn {
    Box::new(
        |req: OperationRequest| -> Pin<
            Box<dyn Future<Output = anyhow::Result<OperationResponse>> + Send>,
        > {
            Box::pin(async move {
                match req.operation.as_str() {
                    "create" => {
                        let name = req
                            .name
                            .ok_or_else(|| anyhow::anyhow!("missing project name"))?;
                        let org = req
                            .scope
                            .get("org")
                            .ok_or_else(|| anyhow::anyhow!("--org is required"))?
                            .to_string();
                        nauka_core::validate::name(&name)?;
                        let store = connect_store().await?;
                        let project = store.create_project(&name, &org).await?;
                        Ok(OperationResponse::Resource(serde_json::json!({
                            "name": project.name,
                            "id": project.id.as_str(),
                            "org_name": project.org_name,
                            "created_at": project.created_at,
                        })))
                    }
                    "list" => {
                        let org = req.scope.get("org").map(|s| s.to_string());
                        let store = connect_store().await?;
                        let projects = store.list_projects(org.as_deref()).await?;
                        let items: Vec<serde_json::Value> = projects
                            .iter()
                            .map(|p| {
                                serde_json::json!({
                                    "name": p.name,
                                    "id": p.id.as_str(),
                                    "org_name": p.org_name,
                                    "created_at": p.created_at,
                                })
                            })
                            .collect();
                        Ok(OperationResponse::ResourceList(items))
                    }
                    "get" => {
                        let name = req
                            .name
                            .ok_or_else(|| anyhow::anyhow!("missing project name or ID"))?;
                        let org = req.scope.get("org").map(|s| s.to_string());
                        let store = connect_store().await?;
                        let project = store
                            .get_project(&name, org.as_deref())
                            .await?
                            .ok_or_else(|| anyhow::anyhow!("project '{name}' not found"))?;
                        Ok(OperationResponse::Resource(serde_json::json!({
                            "name": project.name,
                            "id": project.id.as_str(),
                            "org_name": project.org_name,
                            "created_at": project.created_at,
                        })))
                    }
                    "delete" => {
                        let name = req
                            .name
                            .ok_or_else(|| anyhow::anyhow!("missing project name or ID"))?;
                        let org = req
                            .scope
                            .get("org")
                            .ok_or_else(|| anyhow::anyhow!("--org is required"))?
                            .to_string();
                        let store = connect_store().await?;
                        store.delete_project(&name, &org).await?;
                        Ok(OperationResponse::Message(format!(
                            "project '{name}' deleted."
                        )))
                    }
                    other => Ok(OperationResponse::Message(format!("unknown: {other}"))),
                }
            })
        },
    )
}

pub fn project_registration() -> ResourceRegistration {
    ResourceRegistration {
        def: project_def(),
        handler: project_handler(),
    }
}

// ═══════════════════════════════════════════════════
// Environment
// ═══════════════════════════════════════════════════

fn env_def() -> ResourceDef {
    ResourceDef::build("env", "Manage environments within a project")
        .alias("environment")
        .plural("environments")
        .parent("org", "--org", "Organization")
        .parent_required("project", "--project", "Project")
        .crud()
        .column("NAME", "name")
        .column("PROJECT", "project_name")
        .column("ORG", "org_name")
        .column("ID", "id")
        .column_def(ColumnDef::new("CREATED", "created_at").with_format(DisplayFormat::Timestamp))
        .empty_message(
            "No environments found. Create one with: nauka env create <name> --project <project> --org <org>",
        )
        .detail_section(
            None,
            vec![
                DetailField::new("Name", "name"),
                DetailField::new("ID", "id"),
                DetailField::new("Project", "project_name"),
                DetailField::new("Organization", "org_name"),
                DetailField::new("Created", "created_at").with_format(DisplayFormat::Timestamp),
            ],
        )
        .done()
}

fn env_handler() -> HandlerFn {
    Box::new(
        |req: OperationRequest| -> Pin<
            Box<dyn Future<Output = anyhow::Result<OperationResponse>> + Send>,
        > {
            Box::pin(async move {
                match req.operation.as_str() {
                    "create" => {
                        let name = req
                            .name
                            .ok_or_else(|| anyhow::anyhow!("missing environment name"))?;
                        let org = req
                            .scope
                            .get("org")
                            .ok_or_else(|| anyhow::anyhow!("--org is required"))?
                            .to_string();
                        let project = req
                            .scope
                            .get("project")
                            .ok_or_else(|| anyhow::anyhow!("--project is required"))?
                            .to_string();
                        nauka_core::validate::name(&name)?;
                        let store = connect_store().await?;
                        let env = store.create_env(&name, &project, &org).await?;
                        Ok(OperationResponse::Resource(serde_json::json!({
                            "name": env.name,
                            "id": env.id.as_str(),
                            "project_name": env.project_name,
                            "org_name": env.org_name,
                            "created_at": env.created_at,
                        })))
                    }
                    "list" => {
                        let org = req.scope.get("org").map(|s| s.to_string());
                        let project = req.scope.get("project").map(|s| s.to_string());
                        let store = connect_store().await?;
                        let envs = store
                            .list_envs(project.as_deref(), org.as_deref())
                            .await?;
                        let items: Vec<serde_json::Value> = envs
                            .iter()
                            .map(|e| {
                                serde_json::json!({
                                    "name": e.name,
                                    "id": e.id.as_str(),
                                    "project_name": e.project_name,
                                    "org_name": e.org_name,
                                    "created_at": e.created_at,
                                })
                            })
                            .collect();
                        Ok(OperationResponse::ResourceList(items))
                    }
                    "get" => {
                        let name = req
                            .name
                            .ok_or_else(|| anyhow::anyhow!("missing environment name or ID"))?;
                        let org = req.scope.get("org").map(|s| s.to_string());
                        let project = req.scope.get("project").map(|s| s.to_string());
                        let store = connect_store().await?;
                        let env = store
                            .get_env(&name, project.as_deref(), org.as_deref())
                            .await?
                            .ok_or_else(|| {
                                anyhow::anyhow!("environment '{name}' not found")
                            })?;
                        Ok(OperationResponse::Resource(serde_json::json!({
                            "name": env.name,
                            "id": env.id.as_str(),
                            "project_name": env.project_name,
                            "org_name": env.org_name,
                            "created_at": env.created_at,
                        })))
                    }
                    "delete" => {
                        let name = req
                            .name
                            .ok_or_else(|| anyhow::anyhow!("missing environment name or ID"))?;
                        let org = req
                            .scope
                            .get("org")
                            .ok_or_else(|| anyhow::anyhow!("--org is required"))?
                            .to_string();
                        let project = req
                            .scope
                            .get("project")
                            .ok_or_else(|| anyhow::anyhow!("--project is required"))?
                            .to_string();
                        let store = connect_store().await?;
                        store.delete_env(&name, &project, &org).await?;
                        Ok(OperationResponse::Message(format!(
                            "environment '{name}' deleted."
                        )))
                    }
                    other => Ok(OperationResponse::Message(format!("unknown: {other}"))),
                }
            })
        },
    )
}

pub fn env_registration() -> ResourceRegistration {
    ResourceRegistration {
        def: env_def(),
        handler: env_handler(),
    }
}
