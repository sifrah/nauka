use std::future::Future;
use std::pin::Pin;

use nauka_core::resource::*;

use super::store::NatGwStore;

pub fn resource_def() -> ResourceDef {
    ResourceDef::build(
        "nat-gateway",
        "Manage NAT gateways for outbound internet access",
    )
    .plural("nat-gateways")
    .parent("org", "--org", "Organization")
    .parent("vpc", "--vpc", "VPC")
    .create()
    .op(|op| {
        op.with_arg(OperationArg::optional(
            "hypervisor",
            FieldDef::string(
                "hypervisor",
                "Hypervisor to provision on (auto-selected if omitted)",
            ),
        ))
        .with_example("nauka vpc nat-gateway create egress --vpc prod-net --org acme")
    })
    .list()
    .get()
    .delete()
    .column("NAME", "name")
    .column("VPC", "vpc_name")
    .column("PUBLIC IPv6", "public_ipv6")
    .column_def(ColumnDef::new("STATE", "state").with_format(DisplayFormat::Status))
    .column("ID", "id")
    .column_def(ColumnDef::new("CREATED", "created_at").with_format(DisplayFormat::Timestamp))
    .empty_message("No NAT gateways found.")
    .detail_section(
        None,
        vec![
            DetailField::new("Name", "name"),
            DetailField::new("ID", "id"),
            DetailField::new("VPC", "vpc_name"),
            DetailField::new("Public IPv6", "public_ipv6"),
            DetailField::new("Hypervisor", "hypervisor_id"),
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
                let store = NatGwStore::new(nauka_hypervisor::controlplane::connect().await?);
                match req.operation.as_str() {
                    "create" => {
                        let name = req.name.ok_or_else(|| anyhow::anyhow!("missing name"))?;
                        let vpc = req
                            .scope
                            .get("vpc")
                            .ok_or_else(|| anyhow::anyhow!("--vpc is required"))?
                            .to_string();
                        let org = req
                            .scope
                            .get("org")
                            .ok_or_else(|| anyhow::anyhow!("--org is required"))?
                            .to_string();

                        nauka_core::validate::name(&name)?;

                        // Resolve hypervisor with an IPv6 block
                        let (hv_id, ipv6_block) = resolve_hypervisor(
                            req.fields.get("hypervisor").map(|s| s.as_str()),
                        )?;

                        let natgw = store
                            .create(&name, &vpc, &org, &hv_id, &ipv6_block)
                            .await?;
                        Ok(OperationResponse::Resource(natgw.to_api_json()))
                    }
                    "list" => {
                        let vpc = req.scope.get("vpc").map(|s| s.to_string());
                        let natgws = store.list(vpc.as_deref()).await?;
                        let items: Vec<serde_json::Value> =
                            natgws.iter().map(|n| n.to_api_json()).collect();
                        Ok(OperationResponse::ResourceList(items))
                    }
                    "get" => {
                        let name = req
                            .name
                            .ok_or_else(|| anyhow::anyhow!("missing name or ID"))?;
                        let vpc = req.scope.get("vpc").map(|s| s.to_string());
                        let org = req.scope.get("org").map(|s| s.to_string());
                        let natgw = store
                            .get(&name, vpc.as_deref(), org.as_deref())
                            .await?
                            .ok_or_else(|| anyhow::anyhow!("nat-gateway '{name}' not found"))?;
                        Ok(OperationResponse::Resource(natgw.to_api_json()))
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
                        let org = req
                            .scope
                            .get("org")
                            .ok_or_else(|| anyhow::anyhow!("--org is required"))?
                            .to_string();
                        store.delete(&name, &vpc, &org).await?;
                        Ok(OperationResponse::Message(format!(
                            "nat-gateway '{name}' deleted."
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

/// Resolve the hypervisor to use for the NAT gateway.
///
/// Reads the local fabric state to find a hypervisor with an ipv6_block.
/// If `preferred` is provided, validates it matches the local node.
fn resolve_hypervisor(preferred: Option<&str>) -> anyhow::Result<(String, String)> {
    let dir = nauka_core::process::nauka_dir();
    let _ = std::fs::create_dir_all(&dir);
    let db = nauka_state::LayerDb::open("hypervisor")
        .map_err(|e| anyhow::anyhow!("cannot open hypervisor state: {e}"))?;
    let state = nauka_hypervisor::fabric::state::FabricState::load(&db)
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .ok_or_else(|| {
            anyhow::anyhow!("hypervisor not initialized. Run 'nauka hypervisor init' first.")
        })?;

    let hv = &state.hypervisor;

    if let Some(pref) = preferred {
        if pref != hv.name && pref != hv.id.as_str() {
            anyhow::bail!(
                "hypervisor '{pref}' is not the local node. NAT gateways are provisioned locally."
            );
        }
    }

    let ipv6_block = hv.ipv6_block.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "hypervisor '{}' has no IPv6 block configured. \
             Re-initialize with --ipv6-block to enable NAT gateways.",
            hv.name
        )
    })?;

    Ok((hv.id.to_string(), ipv6_block.clone()))
}
