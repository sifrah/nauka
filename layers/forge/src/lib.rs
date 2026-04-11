//! Forge — per-node resource reconciler.
//!
//! The Forge runs as a daemon on each hypervisor node. It reads desired
//! state from TiKV, compares with actual system state, and converges
//! the delta by creating/deleting resources.
//!
//! CLI: `nauka forge run`, `nauka forge status`

pub mod daemon;
pub mod observer;
pub mod reconciler;
pub mod service;
pub mod types;

use std::future::Future;
use std::pin::Pin;

use nauka_core::resource::*;

inventory::submit!(LayerRegistration(registration));

/// Register the forge as a CLI resource.
pub fn registration() -> ResourceRegistration {
    let def = ResourceDef::build("forge", "Per-node resource reconciler")
        .plural("forges")
        .action("run", "Start the reconciliation daemon")
        .action("status", "Show reconciler status")
        .action("reconcile", "Trigger a single reconciliation cycle")
        .done();

    let handler: HandlerFn = Box::new(
        |req: OperationRequest| -> Pin<
            Box<dyn Future<Output = anyhow::Result<OperationResponse>> + Send>,
        > {
            Box::pin(async move {
                match req.operation.as_str() {
                    "run" => {
                        daemon::run().await?;
                        Ok(OperationResponse::None)
                    }
                    "status" => Ok(OperationResponse::Message(
                        "forge: not running (use 'nauka forge run')".into(),
                    )),
                    "reconcile" => {
                        let result = daemon::run_once().await?;
                        Ok(OperationResponse::Message(result))
                    }
                    other => Ok(OperationResponse::Message(format!("unknown: {other}"))),
                }
            })
        },
    );

    ResourceRegistration {
        def,
        handler,
        children: vec![],
    }
}
