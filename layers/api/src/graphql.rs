//! GraphQL schema builder.
//!
//! The schema is assembled at `router()` time by iterating
//! [`nauka_core::api::ALL_GQL_TYPES`] and dispatching to each
//! resource's hand-written resolvers. 342-A ships with one
//! resource (`Hypervisor`); each subsequent phase (342-B / 342-C)
//! adds a match arm below. The refactor to a fully trait-driven
//! registrar belongs in 342-B once a second resource reveals the
//! right shape.
//!
//! Why hand-written per-resource resolvers in 342-A: async-graphql's
//! dynamic API can't be driven by `impl Trait for R` alone — it
//! needs concrete `Object` / `InputObject` construction. Pushing
//! that construction into the `#[resource]` macro would require
//! every resource crate to depend on async-graphql, which is a hard
//! layer boundary. Instead, keep the resolvers here where the
//! async-graphql dep already lives.

use async_graphql::dynamic::{Object, Schema};
use nauka_core::api::ALL_GQL_TYPES;

use crate::{hypervisor, Deps};

/// Assemble the runtime GraphQL schema. Resources absent from
/// `ALL_GQL_TYPES` silently don't appear in the schema — that's the
/// entire point of the distributed slice. Unknown tables (resources
/// from a future phase not yet wired here) log a warning so the gap
/// is visible rather than silent.
pub fn build_schema(deps: Deps) -> Schema {
    let mut builder = Schema::build("Query", Some("Mutation"), None).data(deps);
    let mut query = Object::new("Query");
    let mut mutation = Object::new("Mutation");

    for desc in ALL_GQL_TYPES.iter() {
        match desc.table {
            "hypervisor" => {
                let (b, q, m) = hypervisor::register_gql(builder, query, mutation);
                builder = b;
                query = q;
                mutation = m;
            }
            other => {
                // Not yet wired — 342-B / 342-C add arms for the
                // resources they migrate. Warn once so the gap
                // surfaces in journalctl instead of silently
                // truncating the schema.
                tracing::debug!(
                    event = "api.graphql.unregistered_resource",
                    table = other,
                    "resource declared in ALL_GQL_TYPES but no GraphQL resolvers wired"
                );
            }
        }
    }

    builder
        .register(query)
        .register(mutation)
        .finish()
        .expect("GraphQL schema construction failed — this is a programming error")
}
