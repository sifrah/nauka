// `version` is injected by `#[resource]`. Hand-declaring it would
// shadow the contract.

use nauka_core_macros::resource;

#[resource(table = "clash3", scope = "local")]
struct Clash3 {
    #[id]
    pub k: String,
    pub version: u32,
}

fn main() {}
