// `cascade_delete` names a field that doesn't exist on the struct —
// must be rejected at macro-expansion time.

use nauka_core_macros::resource;

#[resource(
    table = "x",
    scope = "local",
    cascade_delete = "nonexistent"
)]
struct X {
    #[id]
    pub k: String,
}

fn main() {}
