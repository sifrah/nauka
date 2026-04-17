// Table names must be snake_case — PascalCase is rejected.

use nauka_core_macros::resource;

#[resource(table = "MyTable", scope = "local")]
struct MyTable {
    #[id]
    pub k: String,
}

fn main() {}
