//! Dump OpenAPI spec as JSON to stdout.
//! Used by the docs build to generate the REST API reference.

use nauka_core::api::openapi_spec;
use nauka_core::resource::ResourceRegistry;

fn build_registry() -> ResourceRegistry {
    let mut registry = ResourceRegistry::new();
    registry.register(nauka_hypervisor::handlers::registration());
    registry
}

fn main() {
    let registry = build_registry();
    let spec = openapi_spec(registry.as_slice(), "/admin/v1");
    println!("{}", serde_json::to_string_pretty(&spec).unwrap());
}
