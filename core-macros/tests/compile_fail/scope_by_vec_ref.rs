// `scope_by` identifies a SINGLE authorization parent — a
// `Vec<Ref<T>>` would conflate multiple parents and have no natural
// SurrealQL translation.

use nauka_core_macros::resource;

// Dummy `Ref<T>` stand-in — trybuild fixtures don't depend on
// `nauka_core`; we only care about the macro-expansion diagnostic.
#[allow(dead_code)]
struct Ref<T>(T);

#[resource(table = "child", scope = "cluster", scope_by = "parents")]
struct Child {
    #[id]
    pub id: String,
    pub parents: Vec<Ref<String>>,
}

fn main() {}
