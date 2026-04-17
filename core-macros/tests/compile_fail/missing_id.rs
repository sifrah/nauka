// A resource with no `#[id]` field must not compile — the natural
// key is part of the contract.

use nauka_core_macros::resource;

#[resource(table = "no_id", scope = "local")]
struct NoId {
    pub name: String,
}

fn main() {}
