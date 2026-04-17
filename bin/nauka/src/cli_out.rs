//! User-facing CLI output. The only sanctioned `println!`/`eprintln!`
//! call site in the tree — see ADR 0005.

use std::fmt::Display;

/// Print a single line to stdout.
#[allow(clippy::print_stdout)]
pub fn say(msg: impl Display) {
    println!("{msg}");
}

/// Print an empty line to stdout — visual separation between groups.
#[allow(clippy::print_stdout)]
pub fn blank() {
    println!();
}

/// Print a key/value pair aligned in a two-column table. Keys pad to
/// 11 characters plus colon so values line up at column 14, matching
/// the pre-existing CLI output format.
#[allow(clippy::print_stdout)]
pub fn pair(key: &str, value: impl Display) {
    println!("  {:<11} {value}", format!("{key}:"));
}

/// Print a blank line followed by a section title.
#[allow(clippy::print_stdout)]
pub fn section(title: &str) {
    println!();
    println!("{title}");
}

/// Print a fatal error message to stderr. Used from `main()` on exit
/// when the command returned `Err`.
#[allow(clippy::print_stderr)]
pub fn error(msg: impl Display) {
    eprintln!("Error: {msg}");
}
