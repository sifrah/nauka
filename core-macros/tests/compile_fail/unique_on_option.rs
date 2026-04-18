// `#[unique]` on an `Option<T>` is rejected — pick the inner scalar
// type explicitly if you really mean a partial-unique index.

use nauka_core_macros::resource;

#[resource(table = "uopt", scope = "local")]
struct UOpt {
    #[id]
    pub k: String,
    #[unique]
    pub maybe: Option<String>,
}

fn main() {}
