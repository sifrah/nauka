//! Org resource definition + handler.

use std::future::Future;
use std::pin::Pin;

use nauka_core::resource::*;

use crate::store::OrgStore;

pub fn resource_def() -> ResourceDef {
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

pub fn handler() -> HandlerFn {
    Box::new(
        |req: OperationRequest| -> Pin<
            Box<dyn Future<Output = anyhow::Result<OperationResponse>> + Send>,
        > {
            Box::pin(async move {
                let store = OrgStore::new(crate::connect_cluster_db().await?);
                match req.operation.as_str() {
                    "create" => {
                        let name = req.name.ok_or_else(|| anyhow::anyhow!("missing name"))?;
                        nauka_core::validate::name(&name)?;
                        let org = store.create(&name).await?;
                        Ok(OperationResponse::Resource(serde_json::json!({
                            "name": org.name, "id": org.id.as_str(),
                            "created_at": crate::to_iso8601(org.created_at),
                            "updated_at": crate::to_iso8601(org.updated_at),
                            "status": org.status,
                            "labels": org.labels,
                        })))
                    }
                    "list" => {
                        let orgs = store.list().await?;
                        let items: Vec<serde_json::Value> = orgs.iter().map(|o| serde_json::json!({
                            "name": o.name, "id": o.id.as_str(),
                            "created_at": crate::to_iso8601(o.created_at),
                            "updated_at": crate::to_iso8601(o.updated_at),
                            "status": o.status,
                            "labels": o.labels,
                        })).collect();
                        Ok(OperationResponse::ResourceList(items))
                    }
                    "get" => {
                        let name = req.name.ok_or_else(|| anyhow::anyhow!("missing name or ID"))?;
                        let org = store.get(&name).await?
                            .ok_or_else(|| anyhow::anyhow!("org '{name}' not found"))?;
                        Ok(OperationResponse::Resource(serde_json::json!({
                            "name": org.name, "id": org.id.as_str(),
                            "created_at": crate::to_iso8601(org.created_at),
                            "updated_at": crate::to_iso8601(org.updated_at),
                            "status": org.status,
                            "labels": org.labels,
                        })))
                    }
                    "delete" => {
                        let name = req.name.ok_or_else(|| anyhow::anyhow!("missing name or ID"))?;
                        store.delete(&name).await?;
                        Ok(OperationResponse::Message(format!("org '{name}' deleted.")))
                    }
                    other => Ok(OperationResponse::Message(format!("unknown: {other}"))),
                }
            })
        },
    )
}
