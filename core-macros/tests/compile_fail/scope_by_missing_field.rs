// `scope_by` must name a real field of the struct.

use nauka_core_macros::resource;

#[resource(table = "t", scope = "cluster", scope_by = "ghost")]
struct T {
    #[id]
    pub key: String,
}

fn main() {}
