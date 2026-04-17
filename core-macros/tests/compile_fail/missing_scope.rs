// `#[resource]` requires a `scope = "…"` argument.

use nauka_core_macros::resource;

#[resource(table = "no_scope")]
struct NoScope {
    #[id]
    pub k: String,
}

fn main() {}
