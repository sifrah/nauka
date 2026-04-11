use std::future::Future;
use std::pin::Pin;

use nauka_core::resource::*;

use super::store::PeeringStore;

pub fn resource_def() -> ResourceDef {
    ResourceDef::build("peering", "Manage VPC peering connections")
        .plural("peerings")
        .parent("org", "--org", "Organization")
        .parent("vpc", "--vpc", "VPC")
        .create()
        .op(|op| {
            op.with_arg(OperationArg::required(
                "peer-vpc",
                FieldDef::string("peer-vpc", "VPC to peer with"),
            ))
            .with_output(OutputKind::Resource)
            .with_progress(ProgressHint::Spinner("Creating peering..."))
            .with_example("nauka vpc peering create --org acme --vpc prod-net --peer-vpc dev-net")
        })
        .list()
        .get()
        .delete()
        .op(|op| op.with_progress(ProgressHint::Spinner("Deleting peering...")))
        .column("NAME", "name")
        .column("VPC", "vpc_name")
        .column("PEER", "peer_vpc_name")
        .column_def(ColumnDef::new("STATE", "state").with_format(DisplayFormat::Status))
        .column("ORG", "org_name")
        .column("ID", "id")
        .column_def(ColumnDef::new("CREATED", "created_at").with_format(DisplayFormat::Timestamp))
        .empty_message("No peerings found.")
        .sort_by("name")
        .detail_section(
            None,
            vec![
                DetailField::new("Name", "name"),
                DetailField::new("ID", "id"),
                DetailField::new("VPC", "vpc_name"),
                DetailField::new("Peer VPC", "peer_vpc_name"),
                DetailField::new("Organization", "org_name"),
                DetailField::new("State", "state").with_format(DisplayFormat::Status),
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
                let store = PeeringStore::new(nauka_hypervisor::controlplane::connect().await?);
                match req.operation.as_str() {
                    "create" => {
                        let vpc = req
                            .scope
                            .get("vpc")
                            .ok_or_else(|| anyhow::anyhow!("--vpc is required"))?
                            .to_string();
                        let org = req.scope.get("org").map(|s| s.to_string());
                        let peer_vpc = req
                            .fields
                            .get("peer-vpc")
                            .ok_or_else(|| anyhow::anyhow!("--peer-vpc is required"))?
                            .clone();
                        let peering =
                            store.create(&vpc, &peer_vpc, org.as_deref()).await?;
                        Ok(OperationResponse::Resource(peering.to_api_json()))
                    }
                    "list" => {
                        let vpc = req.scope.get("vpc").map(|s| s.to_string());
                        let peerings = store.list(vpc.as_deref()).await?;
                        let items: Vec<serde_json::Value> =
                            peerings.iter().map(|p| p.to_api_json()).collect();
                        Ok(OperationResponse::ResourceList(items))
                    }
                    "get" => {
                        let name = req
                            .name
                            .ok_or_else(|| anyhow::anyhow!("missing peering ID"))?;
                        let peering = store
                            .get(&name)
                            .await?
                            .ok_or_else(|| anyhow::anyhow!("peering '{name}' not found"))?;
                        Ok(OperationResponse::Resource(peering.to_api_json()))
                    }
                    "delete" => {
                        let name = req
                            .name
                            .ok_or_else(|| anyhow::anyhow!("missing peering ID"))?;
                        store.delete(&name).await?;
                        Ok(OperationResponse::Message(format!(
                            "peering '{name}' deleted."
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
