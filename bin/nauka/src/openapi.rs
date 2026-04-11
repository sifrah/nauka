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

    // Split registrations: hypervisor → /platform/v1, rest → /cloud/v1
    let (platform, cloud): (Vec<_>, Vec<_>) = registry
        .into_registrations()
        .into_iter()
        .partition(|r| r.def.identity.kind == "hypervisor");

    let platform_spec = openapi_spec(&platform, "/platform/v1");
    let cloud_spec = openapi_spec(&cloud, "/cloud/v1");

    // Merge specs
    let mut merged = platform_spec;
    if let (Some(merged_paths), Some(cloud_paths)) = (
        merged["paths"].as_object_mut(),
        cloud_spec["paths"].as_object(),
    ) {
        for (k, v) in cloud_paths {
            merged_paths.insert(k.clone(), v.clone());
        }
    }
    if let (Some(merged_schemas), Some(cloud_schemas)) = (
        merged["components"]["schemas"].as_object_mut(),
        cloud_spec["components"]["schemas"].as_object(),
    ) {
        for (k, v) in cloud_schemas {
            merged_schemas.insert(k.clone(), v.clone());
        }
    }
    if let (Some(merged_tags), Some(cloud_tags)) =
        (merged["tags"].as_array_mut(), cloud_spec["tags"].as_array())
    {
        merged_tags.extend(cloud_tags.iter().cloned());
    }
    if let (Some(merged_groups), Some(cloud_groups)) = (
        merged["x-tagGroups"].as_array_mut(),
        cloud_spec["x-tagGroups"].as_array(),
    ) {
        for group in cloud_groups {
            merged_groups.push(group.clone());
        }
    }

    println!("{}", serde_json::to_string_pretty(&merged).unwrap());
}
