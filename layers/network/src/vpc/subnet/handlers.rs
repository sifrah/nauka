use std::future::Future;
use std::pin::Pin;

use nauka_core::resource::*;

use super::store::SubnetStore;

pub fn resource_def() -> ResourceDef {
    ResourceDef::build("subnet", "Manage subnets within a VPC")
        .plural("subnets")
        .parent("org", "--org", "Organization")
        .parent("vpc", "--vpc", "VPC")
        .create()
        .op(|op| {
            op.with_arg(OperationArg::required(
                "cidr",
                FieldDef::cidr("cidr", "Subnet CIDR block (e.g. 10.0.1.0/24)"),
            ))
            .with_progress(ProgressHint::Spinner("Creating subnet..."))
            .with_example("nauka vpc subnet create web --vpc prod-net --cidr 10.0.1.0/24")
        })
        .list()
        .get()
        .delete()
        .op(|op| op.with_progress(ProgressHint::Spinner("Deleting subnet...")))
        .column("NAME", "name")
        .column("CIDR", "cidr")
        .column("GATEWAY", "gateway")
        .column("VPC", "vpc_name")
        .column("ORG", "org_name")
        .column("ID", "id")
        .column_def(ColumnDef::new("CREATED", "created_at").with_format(DisplayFormat::Timestamp))
        .empty_message(
            "No subnets found. Create one with: nauka vpc subnet create <name> --vpc <vpc> --cidr <cidr>",
        )
        .sort_by("name")
        .detail_section(
            None,
            vec![
                DetailField::new("Name", "name"),
                DetailField::new("ID", "id"),
                DetailField::new("CIDR", "cidr"),
                DetailField::new("Gateway", "gateway"),
                DetailField::new("VPC", "vpc_name"),
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
                // P2.12 (sifrah/nauka#216): SubnetStore now takes an
                // EmbeddedDb directly; reach the cluster handle via
                // the wrapper's `.embedded()` accessor.
                let cluster_db = nauka_hypervisor::controlplane::connect().await?;
                let store = SubnetStore::new(cluster_db.embedded().clone());
                match req.operation.as_str() {
                    "create" => {
                        let name = req.name.ok_or_else(|| anyhow::anyhow!("missing name"))?;
                        let vpc = req
                            .scope
                            .get("vpc")
                            .ok_or_else(|| anyhow::anyhow!("--vpc is required"))?
                            .to_string();
                        let org = req.scope.get("org").map(|s| s.to_string());
                        let cidr = req
                            .fields
                            .get("cidr")
                            .ok_or_else(|| anyhow::anyhow!("--cidr is required"))?
                            .clone();
                        nauka_core::validate::name(&name)?;
                        let subnet =
                            store.create(&name, &vpc, org.as_deref(), &cidr).await?;
                        Ok(OperationResponse::Resource(subnet.to_api_json()))
                    }
                    "list" => {
                        let vpc = req.scope.get("vpc").map(|s| s.to_string());
                        let org = req.scope.get("org").map(|s| s.to_string());
                        let subs = store.list(vpc.as_deref(), org.as_deref()).await?;
                        let items: Vec<serde_json::Value> =
                            subs.iter().map(|s| s.to_api_json()).collect();
                        Ok(OperationResponse::ResourceList(items))
                    }
                    "get" => {
                        let name = req
                            .name
                            .ok_or_else(|| anyhow::anyhow!("missing name or ID"))?;
                        let vpc = req.scope.get("vpc").map(|s| s.to_string());
                        let org = req.scope.get("org").map(|s| s.to_string());
                        let subnet = store
                            .get(&name, vpc.as_deref(), org.as_deref())
                            .await?
                            .ok_or_else(|| anyhow::anyhow!("subnet '{name}' not found"))?;
                        Ok(OperationResponse::Resource(subnet.to_api_json()))
                    }
                    "delete" => {
                        let name = req
                            .name
                            .ok_or_else(|| anyhow::anyhow!("missing name or ID"))?;
                        let vpc = req
                            .scope
                            .get("vpc")
                            .ok_or_else(|| anyhow::anyhow!("--vpc is required"))?
                            .to_string();
                        let org = req.scope.get("org").map(|s| s.to_string());
                        store.delete(&name, &vpc, org.as_deref()).await?;
                        Ok(OperationResponse::Message(format!(
                            "subnet '{name}' deleted."
                        )))
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
