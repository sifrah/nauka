// `scope_by` must target a `Ref<T>` (or `Option<Ref<T>>`) — a plain
// `String` cannot be dereferenced in SurrealQL's `$this.field`
// authorization check.

use nauka_core_macros::resource;

#[resource(table = "t", scope = "cluster", scope_by = "owner")]
struct T {
    #[id]
    pub key: String,
    pub owner: String,
}

fn main() {}
