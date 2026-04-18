// `#[unique]` on a `Vec<T>` makes no sense — UNIQUE indexes apply to
// scalar columns.

use nauka_core_macros::resource;

#[resource(table = "uvec", scope = "local")]
struct UVec {
    #[id]
    pub k: String,
    #[unique]
    pub tags: Vec<String>,
}

fn main() {}
