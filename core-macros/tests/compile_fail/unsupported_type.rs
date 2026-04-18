// Unsupported field types are rejected at compile time — the macro
// only knows how to map a closed set to SurrealQL.

use nauka_core_macros::resource;

struct CustomThing;

#[resource(table = "weirdtype", scope = "local")]
struct WeirdType {
    #[id]
    pub k: String,
    pub thing: CustomThing,
}

fn main() {}
