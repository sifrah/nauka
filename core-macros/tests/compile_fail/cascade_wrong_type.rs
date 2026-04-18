// `cascade_delete` targets a field that isn't `Ref<T>` /
// `Option<Ref<T>>` / `Vec<Ref<T>>` — cascading on a plain String
// makes no sense and must be a compile error.

use nauka_core_macros::resource;

#[resource(
    table = "x",
    scope = "local",
    cascade_delete = "not_a_ref"
)]
struct X {
    #[id]
    pub k: String,
    pub not_a_ref: String,
}

fn main() {}
