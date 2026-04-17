// `created_at` is injected by `#[resource]`. Hand-declaring it would
// shadow the contract.

use nauka_core_macros::resource;

#[resource(table = "clash", scope = "local")]
struct Clash {
    #[id]
    pub k: String,
    pub created_at: String,
}

fn main() {}
