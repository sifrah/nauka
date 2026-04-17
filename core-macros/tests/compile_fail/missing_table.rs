// `#[resource]` requires a `table = "…"` argument.

use nauka_core_macros::resource;

#[resource(scope = "local")]
struct NoTable {
    #[id]
    pub k: String,
}

fn main() {}
