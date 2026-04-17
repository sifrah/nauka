// Hyphens are not allowed in table names — snake_case only.

use nauka_core_macros::resource;

#[resource(table = "my-table", scope = "local")]
struct KebabTable {
    #[id]
    pub k: String,
}

fn main() {}
