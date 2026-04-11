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
            op.with_output(OutputKind::Resource)
                .with_progress(ProgressHint::Download)
                .with_example("nauka vm image pull ubuntu-24.04")
        })
        .action("list", "List locally available images")
        .op(|op| op.with_output(OutputKind::ResourceList))
        .action("catalog", "List images available in the registry")
        .op(|op| op.with_output(OutputKind::ResourceList))
        .action("delete", "Delete a local image")
        .op(|op| op.with_progress(ProgressHint::Spinner("Deleting image...")))
        .column("NAME", "name")
        .column("TYPE", "image_type")
        .column("SIZE", "size")
        .column("ARCH", "arch")
        .column("LOCAL", "local")
        .empty_message("No images found. Pull one with: nauka vm image pull ubuntu-24.04")
        .sort_by("name")
        .detail_section(
            None,
            vec![
                DetailField::new("Name", "name"),
                DetailField::new("ID", "id"),
                DetailField::new("Type", "image_type"),
                DetailField::new("Size", "size"),
                DetailField::new("Arch", "arch"),
                DetailField::new("Local", "local"),
                DetailField::new("Pulled", "created_at").with_format(DisplayFormat::Timestamp),
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
                            .name
                            .ok_or_else(|| anyhow::anyhow!("missing name"))?;
                        let size = registry::pull(&name).await?;
                        Ok(OperationResponse::Resource(serde_json::json!({
                            "id": name,
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
                                    "id": name,
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
                                    "id": e.name,
                                    "name": e.name,
                                    "type": e.image_type,
                                    "size": format_size(e.size),
                                    "arch": e.arch,
                                    "local": if e.local { "✓" } else { "✗" },
                                })
                            })
                            .collect();
                        Ok(OperationResponse::ResourceList(items))
                    }
                    "delete" => {
                        let name = req
                            .name
                            .ok_or_else(|| anyhow::anyhow!("missing name"))?;
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
