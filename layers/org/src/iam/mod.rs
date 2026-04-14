//! IAM — identity & access management grouping.
//!
//! Placeholder module today: the `iam` table is a per-org configuration
//! scope whose concrete identity resources (user, future role/policy) live
//! as nested modules underneath. No store or handler yet — this module
//! exists so the schema registry has a home for
//! [`include_str!("definition.surql")`].

pub mod user;
