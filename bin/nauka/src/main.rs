use anyhow::Result;
use clap::{Arg, Command};

fn main() -> Result<()> {
    let app = Command::new("nauka")
        .about("Nauka — turn dedicated servers into a programmable cloud")
        .version(option_env!("NAUKA_VERSION").unwrap_or(env!("CARGO_PKG_VERSION")))
        .arg_required_else_help(true)
        .subcommand(
            Command::new("hello")
                .about("Print a greeting (smoke test while v2 rebuilds)")
                .arg(
                    Arg::new("name")
                        .long("name")
                        .value_name("NAME")
                        .help("Name to greet")
                        .default_value("world"),
                ),
        );

    let matches = app.get_matches();

    match matches.subcommand() {
        Some(("hello", sub)) => {
            let name = sub.get_one::<String>("name").map(String::as_str).unwrap_or("world");
            println!("hello, {name}");
            Ok(())
        }
        _ => {
            anyhow::bail!("no subcommand given. Run 'nauka --help'.");
        }
    }
}
