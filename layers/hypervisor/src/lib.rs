#![deny(clippy::print_stdout, clippy::print_stderr)]

pub mod daemon;
pub mod mesh;
pub mod systemd;

pub const SCHEMA: &str = include_str!("../definition.surql");
