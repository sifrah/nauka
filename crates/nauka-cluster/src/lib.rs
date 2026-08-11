//! Nauka cluster layer: deterministic placement via rendezvous hashing,
//! peer liveness, and self-healing. Membership and metadata are Raft
//! (openraft); this crate is the placement/repair half that runs on top.

pub mod audit;
pub mod healer;
pub mod health;
pub mod placement;
pub mod telemetry;
pub mod vivaldi;
