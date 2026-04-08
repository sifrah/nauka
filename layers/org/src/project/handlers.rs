//! Project resource definition + handler.

use std::future::Future;
use std::pin::Pin;

use nauka_core::resource::*;

use super::store::ProjectStore;

pub fn resource_def() -> ResourceDef {
    ResourceDef::build("project", "Manage projects within an organization")
        .plural("projects")
        .parent("org", "--org", "Organization")
        .crud()
        .column("NAME", "name")
        .column("ORG", "org_name")
        .column("ID", "id")
        .column_def(ColumnDef::new("CREATED", "created_at").with_format(DisplayFormat::Timestamp))
        .empty_message(
            "No projects found. Create one with: nauka org project create <name> --org <org>",
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

pub fn handler() -> HandlerFn {
    Box::new(
        |req: OperationRequest| -> Pin<
            Box<dyn Future<Output = anyhow::Result<OperationResponse>> + Send>,
        > {
            Box::pin(async move {
                let store = ProjectStore::new(crate::connect_cluster_db().await?);
                match req.operation.as_str() {
                    "create" => {
                        let name = req.name.ok_or_else(|| anyhow::anyhow!("missing name"))?;
                        let org = req.scope.get("org").ok_or_else(|| anyhow::anyhow!("--org is required"))?.to_string();
                        nauka_core::validate::name(&name)?;
                        let project = store.create(&name, &org).await?;
                        Ok(OperationResponse::Resource(serde_json::json!({
                            "name": project.name, "id": project.id.as_str(),
                            "org_id": project.org_id.as_str(),
                            "org_name": project.org_name,
                            "created_at": crate::to_iso8601(project.created_at),
                            "updated_at": crate::to_iso8601(project.updated_at),
                            "status": project.status,
                            "labels": project.labels,
                        })))
                    }
                    "list" => {
                        let org = req.scope.get("org").map(|s| s.to_string());
                        let projects = store.list(org.as_deref()).await?;
                        let items: Vec<serde_json::Value> = projects.iter().map(|p| serde_json::json!({
                            "name": p.name, "id": p.id.as_str(),
                            "org_id": p.org_id.as_str(),
                            "org_name": p.org_name,
                            "created_at": crate::to_iso8601(p.created_at),
                            "updated_at": crate::to_iso8601(p.updated_at),
                            "status": p.status,
                            "labels": p.labels,
                        })).collect();
                        Ok(OperationResponse::ResourceList(items))
                    }
                    "get" => {
                        let name = req.name.ok_or_else(|| anyhow::anyhow!("missing name or ID"))?;
                        let org = req.scope.get("org").map(|s| s.to_string());
                        let project = store.get(&name, org.as_deref()).await?
                            .ok_or_else(|| anyhow::anyhow!("project '{name}' not found"))?;
                        Ok(OperationResponse::Resource(serde_json::json!({
                            "name": project.name, "id": project.id.as_str(),
                            "org_id": project.org_id.as_str(),
                            "org_name": project.org_name,
                            "created_at": crate::to_iso8601(project.created_at),
                            "updated_at": crate::to_iso8601(project.updated_at),
                            "status": project.status,
                            "labels": project.labels,
                        })))
                    }
                    "delete" => {
                        let name = req.name.ok_or_else(|| anyhow::anyhow!("missing name or ID"))?;
                        let org = req.scope.get("org").ok_or_else(|| anyhow::anyhow!("--org is required"))?.to_string();
                        store.delete(&name, &org).await?;
                        Ok(OperationResponse::Message(format!("project '{name}' deleted.")))
                    }
                    other => Ok(OperationResponse::Message(format!("unknown: {other}"))),
                }
            })
        },
    )
}

pub fn registration() -> ResourceRegistration {
    ResourceRegistration {
        def: resource_def(),
        handler: handler(),
        children: vec![super::env::handlers::registration()],
    }
}
