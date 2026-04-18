// `#[resource]` only supports structs with named fields — tuple
// structs cannot carry the `#[id]` marker meaningfully.

use nauka_core_macros::resource;

#[resource(table = "tup", scope = "local")]
struct Tup(String, u64);

fn main() {}
