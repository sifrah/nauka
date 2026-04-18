// Two `#[assert]` attributes on the same field are ambiguous.

use nauka_core_macros::resource;

#[resource(table = "x", scope = "local")]
struct X {
    #[id]
    pub k: String,
    #[assert("$value > 0")]
    #[assert("$value < 100")]
    pub count: u32,
}

fn main() {}
