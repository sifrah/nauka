// Unknown keys on `#[resource]` are rejected — silent typos are
// dangerous (e.g. `scop = "cluster"` would otherwise default to local).

use nauka_core_macros::resource;

#[resource(table = "extra", scope = "local", banana = "yes")]
struct Extra {
    #[id]
    pub k: String,
}

fn main() {}
