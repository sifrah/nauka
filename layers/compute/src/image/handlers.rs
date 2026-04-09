//! Image resource definition + handler.
//!
//! CLI: nauka vm image pull/list/delete

use std::future::Future;
use std::pin::Pin;

use nauka_core::resource::*;

use super::registry;

pub fn resource_def() -> ResourceDef {
    ResourceDef::build("image", "Manage OS images")
        .plural("images")
        .action("pull", "Pull an image from the registry")
        .op(|op| {
            op.with_arg(OperationArg::required(
                "name",
                FieldDef::string("name", "Image name (e.g., ubuntu-24.04)"),
            ))
            .with_output(OutputKind::Resource)
            .with_example("nauka vm image pull ubuntu-24.04")
        })
        .action("list", "List locally available images")
        .op(|op| op.with_output(OutputKind::ResourceList))
        .action("catalog", "List images available in the registry")
        .op(|op| op.with_output(OutputKind::ResourceList))
        .action("delete", "Delete a local image")
        .op(|op| {
            op.with_arg(OperationArg::required(
                "name",
                FieldDef::string("name", "Image name to delete"),
            ))
        })
        .column("NAME", "name")
        .column("TYPE", "type")
        .column("SIZE", "size")
        .column("ARCH", "arch")
        .column("LOCAL", "local")
        .empty_message("No images found. Pull one with: nauka vm image pull ubuntu-24.04")
        .detail_section(
            None,
            vec![
                DetailField::new("Name", "name"),
                DetailField::new("Size", "size"),
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
                match req.operation.as_str() {
                    "pull" => {
                        let name = req
                            .fields
                            .get("name")
                            .ok_or_else(|| anyhow::anyhow!("--name is required"))?
                            .clone();
                        let size = registry::pull(&name).await?;
                        Ok(OperationResponse::Resource(serde_json::json!({
                            "name": name,
                            "status": "ready",
                            "size_bytes": size,
                            "size": format_size(size),
                        })))
                    }
                    "list" => {
                        let arch = match std::env::consts::ARCH {
                            "x86_64" => "amd64",
                            "aarch64" => "arm64",
                            other => other,
                        };
                        let image_type = if std::path::Path::new("/dev/kvm").exists() {
                            "vm"
                        } else {
                            "container"
                        };
                        let images = registry::list();
                        let items: Vec<serde_json::Value> = images
                            .iter()
                            .map(|(name, size)| {
                                serde_json::json!({
                                    "name": name,
                                    "type": image_type,
                                    "size": format_size(*size),
                                    "arch": arch,
                                    "local": "✓",
                                })
                            })
                            .collect();
                        Ok(OperationResponse::ResourceList(items))
                    }
                    "catalog" => {
                        let entries = registry::catalog().await?;
                        let items: Vec<serde_json::Value> = entries
                            .iter()
                            .map(|e| {
                                serde_json::json!({
                                    "name": e.name,
                                    "type": e.image_type,
                                    "arch": e.arch,
                                    "local": if e.local { "✓" } else { "✗" },
                                })
                            })
                            .collect();
                        Ok(OperationResponse::ResourceList(items))
                    }
                    "delete" => {
                        let name = req
                            .fields
                            .get("name")
                            .ok_or_else(|| anyhow::anyhow!("--name is required"))?
                            .clone();
                        registry::delete(&name)?;
                        Ok(OperationResponse::Message(format!(
                            "image '{name}' deleted."
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

fn format_size(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.1} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.0} MB", bytes as f64 / 1_048_576.0)
    } else {
        format!("{} KB", bytes / 1024)
    }
}
