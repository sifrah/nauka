// `#[assert]` requires a string-literal argument with the SurrealQL
// predicate.

use nauka_core_macros::resource;

#[resource(table = "x", scope = "local")]
struct X {
    #[id]
    pub k: String,
    #[assert]
    pub count: u32,
}

fn main() {}
