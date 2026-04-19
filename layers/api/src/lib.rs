//! Nauka API layer — axum REST + GraphQL server built on top of
//! the `#[resource]` descriptors. See issue #354 (342-A) for the
//! decisions behind this split.
//!
//! 342-A wires a **single** resource (`Hypervisor`) end-to-end so
//! the contract is validated before generalizing. Subsequent phases
//! (342-B adds `Mesh`, 342-C adds the IAM resources) each add a
//! match arm on the resource table name in the REST + GraphQL
//! dispatch — the distributed slices in `nauka_core::api` drive
//! which resources exist, and this crate provides the typed handlers.
//!
//! ## Layering
//!
//! - [`Deps`] carries the server-side handles (DB + Raft) every
//!   handler needs. Constructed once at boot, cloned freely.
//! - [`Principal`] is the authenticated caller, populated by
//!   [`principal::require_auth`] middleware from the `Authorization:
//!   Bearer` header.
//! - [`NaukaApiError`] is the uniform error type; its
//!   [`IntoResponse`](axum::response::IntoResponse) impl uses
//!   [`nauka_core::NaukaError::event_name`] as the JSON error code,
//!   so a response body like `{"error": "state.db", ...}` stays
//!   consistent with the `tracing` event stream.

pub mod deps;
pub mod error;
pub mod principal;

mod graphql;
mod hypervisor;
mod router;

pub use deps::Deps;
pub use error::NaukaApiError;
pub use principal::Principal;
pub use router::router;
