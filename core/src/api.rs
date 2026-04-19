//! API surface contract — see issue #354 (342-A).
//!
//! The resource macro (`#[resource]`) emits registrations into three
//! distributed slices declared here, so `layers/api` can discover
//! every registered resource at link time without anyone maintaining
//! a hand-written list.
//!
//! Descriptors are **metadata only**. Full handler code and GraphQL
//! resolvers live in `layers/api`, parameterized by the resource type
//! `R: Resource + ResourceOps`. Keeping this module free of axum /
//! async-graphql deps preserves the "lower layers don't depend on
//! higher layers" invariant — resource crates pull in *nothing* new
//! when they gain an API surface.
//!
//! ## Why fn pointers for CLI but metadata-only for REST/GraphQL
//!
//! - Clap `Command` is a runtime value with no generic parameters,
//!   so the macro can emit a `fn() -> clap::Command` directly.
//! - Axum handlers and async-graphql resolvers need type-generic
//!   code (`Handler<R>`, `SimpleObject for R`) that can't be
//!   stored in an object-safe trait. `layers/api` reads the
//!   metadata and dispatches by table name to the right generic
//!   monomorphization.

use linkme::distributed_slice;

/// Re-exported so the `#[resource]` macro can emit `fn() -> Command`
/// without forcing every resource crate to add `clap` to its
/// `[dependencies]`. Pointing at the same `clap::Command` type means
/// `layers/api-client` consuming the descriptor gets the exact clap
/// type it expects.
pub use clap::Command;

/// A verb exposed on a resource's REST/GraphQL surface. Custom verbs
/// (`start`, `stop`) are carried out-of-band in
/// [`ApiResourceDescriptor::custom_actions`] — this enum covers the
/// five CRUD verbs every resource shares a shape for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Verb {
    Create,
    Get,
    List,
    Update,
    Delete,
}

impl Verb {
    pub const ALL: &'static [Verb] = &[
        Verb::Create,
        Verb::Get,
        Verb::List,
        Verb::Update,
        Verb::Delete,
    ];

    /// Case-insensitive parse — the macro accepts `api_verbs = "create, GET, list"`.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "create" => Some(Verb::Create),
            "get" => Some(Verb::Get),
            "list" => Some(Verb::List),
            "update" => Some(Verb::Update),
            "delete" => Some(Verb::Delete),
            _ => None,
        }
    }

    /// String form used in URLs / GraphQL field names / OpenAPI ops.
    pub fn as_str(&self) -> &'static str {
        match self {
            Verb::Create => "create",
            Verb::Get => "get",
            Verb::List => "list",
            Verb::Update => "update",
            Verb::Delete => "delete",
        }
    }
}

/// REST / GraphQL metadata for one resource. Registered into
/// [`ALL_API_RESOURCES`] by the `#[resource]` macro whenever the
/// resource exposes at least one verb.
#[derive(Debug)]
pub struct ApiResourceDescriptor {
    /// SurrealDB table name — matches [`crate::resource::Resource::TABLE`].
    pub table: &'static str,
    /// REST path prefix, e.g. `/hypervisors`. Defaults to
    /// `"/<table>s"` when the resource author leaves `api_path`
    /// unset. The macro validates the shape (must start with `/`,
    /// URL-safe chars only).
    pub path: &'static str,
    /// CRUD verbs exposed. Empty when the resource is internal
    /// (`api_verbs = ""`). Custom actions live in
    /// [`custom_actions`](Self::custom_actions).
    pub verbs: &'static [Verb],
    /// Non-CRUD actions — one per entry in
    /// `#[resource(custom_actions = "…")]`. Each action gets a
    /// `POST {path}/:id/{action}` REST route and a `{action}{Pascal}`
    /// GraphQL mutation.
    pub custom_actions: &'static [&'static str],
}

/// GraphQL type registration for one resource. Kept separate from
/// [`ApiResourceDescriptor`] so a future feature can expose a resource
/// over only one of REST / GraphQL without carrying the other's
/// metadata.
#[derive(Debug)]
pub struct GqlResourceDescriptor {
    pub table: &'static str,
    /// PascalCase name used in the GraphQL schema
    /// (e.g. `"Hypervisor"`). Derived from the Rust struct name by
    /// the macro.
    pub gql_type_name: &'static str,
    pub verbs: &'static [Verb],
    pub custom_actions: &'static [&'static str],
}

/// CLI subcommand factory for one resource. The stored function
/// builds a `clap::Command` tree at call time — concrete values with
/// no generic parameters, so a plain fn pointer is sufficient.
///
/// `layers/api-client` + `bin/nauka` consume this to assemble the
/// generated CLI subcommands; see 342-B for the wiring.
#[derive(Debug)]
pub struct CliCommandDescriptor {
    pub table: &'static str,
    /// Subcommand name — defaults to `snake_case(struct_ident)`, can
    /// be overridden via `#[resource(api_cli = "…")]`.
    pub name: &'static str,
    /// Called by `bin/nauka` at startup to build the clap subcommand.
    /// Wiring of the handler to the HTTP client happens in 342-B.
    pub build: fn() -> Command,
}

/// Every resource that opts into a REST / GraphQL surface registers
/// one descriptor here. `layers/api` iterates this slice at boot and
/// mounts the matching generic handler specialisation for each.
#[distributed_slice]
pub static ALL_API_RESOURCES: [&'static ApiResourceDescriptor] = [..];

#[distributed_slice]
pub static ALL_GQL_TYPES: [&'static GqlResourceDescriptor] = [..];

#[distributed_slice]
pub static ALL_CLI_COMMANDS: [&'static CliCommandDescriptor] = [..];

/// Lookup helper — fetch the descriptor for a given resource by
/// table name. Returns `None` if the resource is internal
/// (`api_verbs = ""`) or not registered.
pub fn api_resource(table: &str) -> Option<&'static ApiResourceDescriptor> {
    ALL_API_RESOURCES.iter().copied().find(|d| d.table == table)
}

pub fn gql_resource(table: &str) -> Option<&'static GqlResourceDescriptor> {
    ALL_GQL_TYPES.iter().copied().find(|d| d.table == table)
}

pub fn cli_command(table: &str) -> Option<&'static CliCommandDescriptor> {
    ALL_CLI_COMMANDS.iter().copied().find(|d| d.table == table)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verb_parse_accepts_case_variations() {
        assert_eq!(Verb::parse("create"), Some(Verb::Create));
        assert_eq!(Verb::parse(" CREATE "), Some(Verb::Create));
        assert_eq!(Verb::parse("GeT"), Some(Verb::Get));
        assert_eq!(Verb::parse("list"), Some(Verb::List));
        assert_eq!(Verb::parse("update"), Some(Verb::Update));
        assert_eq!(Verb::parse("delete"), Some(Verb::Delete));
    }

    #[test]
    fn verb_parse_rejects_nonsense() {
        assert_eq!(Verb::parse(""), None);
        assert_eq!(Verb::parse("nuke"), None);
        assert_eq!(Verb::parse("start"), None); // custom_actions handle this
    }

    #[test]
    fn empty_registry_returns_none() {
        // nauka-core defines no resources of its own, so lookups
        // against a non-existent table must return None without
        // panicking.
        assert!(api_resource("vm").is_none());
        assert!(gql_resource("vm").is_none());
        assert!(cli_command("vm").is_none());
    }
}
