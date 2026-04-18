// `scope` must be `"local"` or `"cluster"`.

use nauka_core_macros::resource;

#[resource(table = "weird", scope = "regional")]
struct Weird {
    #[id]
    pub k: String,
}

fn main() {}
