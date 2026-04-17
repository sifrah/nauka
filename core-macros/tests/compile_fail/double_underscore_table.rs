// Doubled underscores in table names are rejected — they look like
// reserved names and are easy to typo.

use nauka_core_macros::resource;

#[resource(table = "my__table", scope = "local")]
struct DoubleUnderscore {
    #[id]
    pub k: String,
}

fn main() {}
