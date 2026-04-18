// `scope_by` and `permissions` both produce a PERMISSIONS clause —
// setting both at the same time is ambiguous.

use nauka_core_macros::resource;

#[allow(dead_code)]
struct Ref<T>(T);

#[resource(
    table = "t",
    scope = "cluster",
    scope_by = "parent",
    permissions = "$auth != NONE"
)]
struct T {
    #[id]
    pub key: String,
    pub parent: Ref<String>,
}

fn main() {}
