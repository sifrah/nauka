//! Dump OpenAPI spec as JSON to stdout.
//! Used by the docs build to generate the REST API reference.
//!
//! Layer crates are linked via `extern crate` to ensure their
//! `inventory::submit!` registrations are collected. Adding a new
//! layer to Cargo.toml + an extern crate line here is all it takes.

// Force-link layer crates so inventory collects their registrations.
extern crate nauka_compute;
extern crate nauka_forge;
extern crate nauka_hypervisor;
extern crate nauka_network;
extern crate nauka_org;

mod registry;

use nauka_core::api::openapi_spec;

fn main() {
    let registry = registry::build_registry();
    let spec = openapi_spec(registry.as_slice(), "/admin/v1");
    println!("{}", serde_json::to_string_pretty(&spec).unwrap());
}
