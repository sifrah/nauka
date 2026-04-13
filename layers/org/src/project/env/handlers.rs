//! Environment resource definition + handler.

use std::future::Future;
use std::pin::Pin;

use nauka_core::resource::*;

use super::store::EnvStore;

pub fn resource_def() -> ResourceDef {
    ResourceDef::build("env", "Manage environments within a project")
        .alias("environment")
        .plural("environments")
        .parent("org", "--org", "Organization")
        .parent("project", "--project", "Project")
        .create()
        .op(|op| {
            op.with_progress(ProgressHint::Spinner("Creating environment..."))
              .with_example("nauka org project env create production --project backend --org acme")
        })
        .list()
        .get()
        .delete()
        .op(|op| op.with_progress(ProgressHint::Spinner("Deleting environment...")))
        .column("NAME", "name")
        .column("PROJECT", "project_name")
        .column("ORG", "org_name")
        .column("ID", "id")
        .column_def(ColumnDef::new("CREATED", "created_at").with_format(DisplayFormat::Timestamp))
        .empty_message(
            "No environments found. Create one with: nauka org project env create <name> --project <project> --org <org>",
        )
        .sort_by("name")
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

pub fn handler() -> HandlerFn {
    Box::new(
        |req: OperationRequest| -> Pin<
            Box<dyn Future<Output = anyhow::Result<OperationResponse>> + Send>,
        > {
            Box::pin(async move {
                // P2.11 (sifrah/nauka#215): EnvStore now takes an
                // EmbeddedDb directly; reach the cluster handle via
                // the wrapper's `.embedded()` accessor.
                let cluster_db = nauka_hypervisor::controlplane::connect().await?;
                let store = EnvStore::new(cluster_db.embedded().clone());
                match req.operation.as_str() {
                    "create" => {
                        let name = req.name.ok_or_else(|| anyhow::anyhow!("missing name"))?;
                        let org = req.scope.get("org").ok_or_else(|| anyhow::anyhow!("--org is required"))?.to_string();
                        let project = req.scope.get("project").ok_or_else(|| anyhow::anyhow!("--project is required"))?.to_string();
                        nauka_core::validate::name(&name)?;
                        let env = store.create(&name, &project, &org).await?;
                        Ok(OperationResponse::Resource(env.to_api_json()))
                    }
                    "list" => {
                        let org = req.scope.get("org").map(|s| s.to_string());
                        let project = req.scope.get("project").map(|s| s.to_string());
                        let envs = store.list(project.as_deref(), org.as_deref()).await?;
                        let items: Vec<serde_json::Value> =
                            envs.iter().map(|e| e.to_api_json()).collect();
                        Ok(OperationResponse::ResourceList(items))
                    }
                    "get" => {
                        let name = req.name.ok_or_else(|| anyhow::anyhow!("missing name or ID"))?;
                        let org = req.scope.get("org").map(|s| s.to_string());
                        let project = req.scope.get("project").map(|s| s.to_string());
                        let env = store.get(&name, project.as_deref(), org.as_deref()).await?
                            .ok_or_else(|| anyhow::anyhow!("environment '{name}' not found"))?;
                        Ok(OperationResponse::Resource(env.to_api_json()))
                    }
                    "delete" => {
                        let name = req.name.ok_or_else(|| anyhow::anyhow!("missing name or ID"))?;
                        let org = req.scope.get("org").ok_or_else(|| anyhow::anyhow!("--org is required"))?.to_string();
                        let project = req.scope.get("project").ok_or_else(|| anyhow::anyhow!("--project is required"))?.to_string();
                        store.delete(&name, &project, &org).await?;
                        Ok(OperationResponse::Message(format!("environment '{name}' deleted.")))
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
        children: vec![],
    }
}
