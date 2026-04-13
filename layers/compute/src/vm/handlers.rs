use std::future::Future;
use std::pin::Pin;

use nauka_core::resource::*;

use super::store::VmStore;
use super::types::VmState;

pub fn resource_def() -> ResourceDef {
    ResourceDef::build("vm", "Manage virtual machines")
        .plural("vms")
        .parent("org", "--org", "Organization")
        .create()
        .op(|op| {
            op.with_arg(OperationArg::required(
                "project",
                FieldDef::string("project", "Project"),
            ))
            .with_arg(OperationArg::required(
                "env",
                FieldDef::string("env", "Environment"),
            ))
            .with_arg(OperationArg::required(
                "vpc",
                FieldDef::string("vpc", "VPC"),
            ))
            .with_arg(OperationArg::required(
                "subnet",
                FieldDef::string("subnet", "Subnet"),
            ))
            .with_arg(OperationArg::required(
                "cpu",
                FieldDef::integer("cpu", "Number of vCPUs"),
            ))
            .with_arg(OperationArg::required(
                "memory",
                FieldDef::integer("memory", "Memory in MB"),
            ))
            .with_arg(OperationArg::required(
                "disk",
                FieldDef::integer("disk", "Disk size in GB"),
            ))
            .with_arg(OperationArg::required(
                "image",
                FieldDef::string("image", "OS image (e.g. ubuntu-24.04)"),
            ))
            .with_arg(OperationArg::optional(
                "region",
                FieldDef::string("region", "Region").with_default("default"),
            ))
            .with_arg(OperationArg::optional(
                "zone",
                FieldDef::string("zone", "Zone").with_default("default"),
            ))
            .with_progress(ProgressHint::Spinner("Creating VM..."))
            .with_example(
                "nauka vm create web-1 --org acme --project backend --env production --vpc prod-net --subnet web --cpu 2 --memory 4096 --disk 40 --image ubuntu-24.04",
            )
        })
        .list()
        .op(|op| {
            op.with_arg(OperationArg::optional(
                "project",
                FieldDef::string("project", "Filter by project"),
            ))
            .with_arg(OperationArg::optional(
                "env",
                FieldDef::string("env", "Filter by environment"),
            ))
        })
        .get()
        .delete()
        .op(|op| op.with_progress(ProgressHint::Spinner("Deleting VM...")))
        .action("start", "Start a stopped VM")
        .op(|op| {
            op.with_output(OutputKind::Resource)
              .with_progress(ProgressHint::Spinner("Starting VM..."))
        })
        .action("stop", "Stop a running VM")
        .op(|op| {
            op.with_output(OutputKind::Resource)
              .with_progress(ProgressHint::Spinner("Stopping VM..."))
        })
        .column("NAME", "name")
        .column_def(ColumnDef::new("STATE", "state").with_format(DisplayFormat::Status))
        .column("CPU", "vcpus")
        .column("MEM", "memory_mb")
        .column("IMAGE", "image")
        .column("ENV", "env_name")
        .column("ID", "id")
        .column_def(ColumnDef::new("CREATED", "created_at").with_format(DisplayFormat::Timestamp))
        .empty_message("No VMs found. Create one with: nauka vm create <name> --org <org> --project <project> --env <env> --vpc <vpc> --subnet <subnet> --cpu <n> --memory <mb> --disk <gb> --image <image>")
        .sort_by("name")
        .detail_section(None, vec![
            DetailField::new("Name", "name"),
            DetailField::new("ID", "id"),
            DetailField::new("State", "state").with_format(DisplayFormat::Status),
            DetailField::new("Image", "image"),
            DetailField::new("vCPUs", "vcpus"),
            DetailField::new("Memory", "memory_mb"),
            DetailField::new("Disk", "disk_gb"),
            DetailField::new("VPC", "vpc_name"),
            DetailField::new("Subnet", "subnet_name"),
            DetailField::new("Private IP", "private_ip"),
            DetailField::new("Organization", "org_name"),
            DetailField::new("Project", "project_name"),
            DetailField::new("Environment", "env_name"),
            DetailField::new("Region", "region"),
            DetailField::new("Zone", "zone"),
            DetailField::new("Hypervisor", "hypervisor_id"),
            DetailField::new("Created", "created_at").with_format(DisplayFormat::Timestamp),
        ])
        .done()
}

pub fn handler() -> HandlerFn {
    Box::new(
        |req: OperationRequest| -> Pin<
            Box<dyn Future<Output = anyhow::Result<OperationResponse>> + Send>,
        > {
            Box::pin(async move {
                // P2.13 (sifrah/nauka#217): VmStore now takes an
                // EmbeddedDb directly; reach the cluster handle via
                // the wrapper's `.embedded()` accessor.
                let cluster_db = nauka_hypervisor::controlplane::connect().await?;
                let store = VmStore::new(cluster_db.embedded().clone());
                match req.operation.as_str() {
                    "create" => {
                        let name = req.name.ok_or_else(|| anyhow::anyhow!("missing name"))?;
                        let org = req
                            .scope
                            .get("org")
                            .ok_or_else(|| anyhow::anyhow!("--org is required"))?
                            .to_string();
                        let project = req
                            .fields
                            .get("project")
                            .ok_or_else(|| anyhow::anyhow!("--project is required"))?
                            .clone();
                        let env = req
                            .fields
                            .get("env")
                            .ok_or_else(|| anyhow::anyhow!("--env is required"))?
                            .clone();
                        let vpc = req
                            .fields
                            .get("vpc")
                            .ok_or_else(|| anyhow::anyhow!("--vpc is required"))?
                            .clone();
                        let subnet = req
                            .fields
                            .get("subnet")
                            .ok_or_else(|| anyhow::anyhow!("--subnet is required"))?
                            .clone();
                        let cpu: u32 = req
                            .fields
                            .get("cpu")
                            .ok_or_else(|| anyhow::anyhow!("--cpu is required"))?
                            .parse()
                            .map_err(|_| anyhow::anyhow!("--cpu must be a number"))?;
                        let memory: u32 = req
                            .fields
                            .get("memory")
                            .ok_or_else(|| anyhow::anyhow!("--memory is required"))?
                            .parse()
                            .map_err(|_| anyhow::anyhow!("--memory must be a number"))?;
                        let disk: u32 = req
                            .fields
                            .get("disk")
                            .ok_or_else(|| anyhow::anyhow!("--disk is required"))?
                            .parse()
                            .map_err(|_| anyhow::anyhow!("--disk must be a number"))?;
                        let image = req
                            .fields
                            .get("image")
                            .ok_or_else(|| anyhow::anyhow!("--image is required"))?
                            .clone();
                        let region = req
                            .fields
                            .get("region")
                            .cloned()
                            .unwrap_or_else(|| "default".to_string());
                        let zone = req
                            .fields
                            .get("zone")
                            .cloned()
                            .unwrap_or_else(|| "default".to_string());

                        nauka_core::validate::name(&name)?;
                        // P2.13 (sifrah/nauka#217): scheduling lives
                        // outside `VmStore::create` so the store
                        // stays decoupled from live fabric state
                        // and can be unit-tested. The handler
                        // resolves a hypervisor via the scheduler
                        // immediately before persisting the row.
                        let hypervisor_id = crate::scheduler::schedule(&region, &zone).await?;
                        let vm = store
                            .create(
                                &name, &org, &project, &env, &vpc, &subnet, cpu, memory, disk,
                                &image, &region, &zone, hypervisor_id,
                            )
                            .await?;
                        Ok(OperationResponse::Resource(vm.to_api_json()))
                    }
                    "list" => {
                        let org = req.scope.get("org").map(|s| s.to_string());
                        let project = req.fields.get("project").cloned();
                        let env = req.fields.get("env").cloned();
                        let vms = store
                            .list(org.as_deref(), project.as_deref(), env.as_deref())
                            .await?;
                        let items: Vec<serde_json::Value> =
                            vms.iter().map(|v| v.to_api_json()).collect();
                        Ok(OperationResponse::ResourceList(items))
                    }
                    "get" => {
                        let name = req
                            .name
                            .ok_or_else(|| anyhow::anyhow!("missing name or ID"))?;
                        let org = req.scope.get("org").map(|s| s.to_string());
                        let vm = store
                            .get(&name, org.as_deref(), None, None)
                            .await?
                            .ok_or_else(|| anyhow::anyhow!("vm '{name}' not found"))?;
                        Ok(OperationResponse::Resource(vm.to_api_json()))
                    }
                    "delete" => {
                        let name = req
                            .name
                            .ok_or_else(|| anyhow::anyhow!("missing name or ID"))?;
                        let org = req.scope.get("org").map(|s| s.to_string());
                        store.delete(&name, org.as_deref(), None, None).await?;
                        Ok(OperationResponse::Message(format!("vm '{name}' deleted.")))
                    }
                    "start" => {
                        let name = req
                            .name
                            .ok_or_else(|| anyhow::anyhow!("missing name"))?;
                        let org = req.scope.get("org").map(|s| s.to_string());
                        let vm = store
                            .update_state(&name, VmState::Running, org.as_deref(), None, None)
                            .await?;
                        Ok(OperationResponse::Resource(vm.to_api_json()))
                    }
                    "stop" => {
                        let name = req
                            .name
                            .ok_or_else(|| anyhow::anyhow!("missing name"))?;
                        let org = req.scope.get("org").map(|s| s.to_string());
                        let vm = store
                            .update_state(&name, VmState::Stopped, org.as_deref(), None, None)
                            .await?;
                        Ok(OperationResponse::Resource(vm.to_api_json()))
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
        children: vec![crate::image::handlers::registration()],
    }
}
