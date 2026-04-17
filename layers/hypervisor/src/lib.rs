#![deny(clippy::print_stdout, clippy::print_stderr)]

pub mod daemon;
pub mod mesh;
pub mod systemd;

pub const SCHEMA: &str = concat!(
    include_str!("mesh/definition.surql"),
    "\n",
    include_str!("definition.surql"),
);
