// `updated_at` is injected by `#[resource]`. Hand-declaring it would
// shadow the contract.

use nauka_core_macros::resource;

#[resource(table = "clash2", scope = "local")]
struct Clash2 {
    #[id]
    pub k: String,
    pub updated_at: String,
}

fn main() {}
