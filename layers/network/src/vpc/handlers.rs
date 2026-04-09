use std::future::Future;
use std::pin::Pin;

use nauka_core::resource::*;

use super::store::VpcStore;

pub fn resource_def() -> ResourceDef {
    ResourceDef::build("vpc", "Manage virtual private clouds")
        .plural("vpcs")
        .parent("org", "--org", "Organization")
        .create()
        .op(|op| {
            op.with_arg(OperationArg::required(
                "cidr",
                FieldDef::cidr("cidr", "VPC CIDR block (e.g. 10.0.0.0/16)"),
            ))
            .with_arg(OperationArg::optional(
                "project",
                FieldDef::string("project", "Scope to a project"),
            ))
            .with_arg(OperationArg::optional(
                "env",
                FieldDef::string("env", "Scope to an environment"),
            ))
            .with_example("nauka vpc create prod-net --org acme --cidr 10.0.0.0/16")
        })
        .list()
        .get()
        .delete()
        .column("NAME", "name")
        .column("CIDR", "cidr")
        .column("ORG", "org_name")
        .column("VNI", "vni")
        .column("ID", "id")
        .column_def(ColumnDef::new("CREATED", "created_at").with_format(DisplayFormat::Timestamp))
        .empty_message(
            "No VPCs found. Create one with: nauka vpc create <name> --org <org> --cidr <cidr>",
        )
        .detail_section(
            None,
            vec![
                DetailField::new("Name", "name"),
                DetailField::new("ID", "id"),
                DetailField::new("CIDR", "cidr"),
                DetailField::new("VNI", "vni"),
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
                let store = VpcStore::new(nauka_hypervisor::controlplane::connect().await?);
                match req.operation.as_str() {
                    "create" => {
                        let name = req.name.ok_or_else(|| anyhow::anyhow!("missing name"))?;
                        let org = req
                            .scope
                            .get("org")
                            .ok_or_else(|| anyhow::anyhow!("--org is required"))?
                            .to_string();
                        let cidr = req
                            .fields
                            .get("cidr")
                            .ok_or_else(|| anyhow::anyhow!("--cidr is required"))?
                            .clone();
                        let project = req.fields.get("project").cloned();
                        let env = req.fields.get("env").cloned();
                        nauka_core::validate::name(&name)?;
                        let vpc = store.create(&name, &org, &cidr, project, env).await?;
                        Ok(OperationResponse::Resource(vpc.to_api_json()))
                    }
                    "list" => {
                        let org = req.scope.get("org").map(|s| s.to_string());
                        let vpcs = store.list(org.as_deref()).await?;
                        let items: Vec<serde_json::Value> =
                            vpcs.iter().map(|v| v.to_api_json()).collect();
                        Ok(OperationResponse::ResourceList(items))
                    }
                    "get" => {
                        let name = req
                            .name
                            .ok_or_else(|| anyhow::anyhow!("missing name or ID"))?;
                        let org = req.scope.get("org").map(|s| s.to_string());
                        let vpc = store
                            .get(&name, org.as_deref())
                            .await?
                            .ok_or_else(|| anyhow::anyhow!("vpc '{name}' not found"))?;
                        Ok(OperationResponse::Resource(vpc.to_api_json()))
                    }
                    "delete" => {
                        let name = req
                            .name
                            .ok_or_else(|| anyhow::anyhow!("missing name or ID"))?;
                        let org = req
                            .scope
                            .get("org")
                            .ok_or_else(|| anyhow::anyhow!("--org is required"))?
                            .to_string();
                        store.delete(&name, &org).await?;
                        Ok(OperationResponse::Message(format!("vpc '{name}' deleted.")))
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
        children: vec![
            super::subnet::handlers::registration(),
            super::peering::handlers::registration(),
            super::natgw::handlers::registration(),
        ],
    }
}
