// `custom_actions` entries splice into permission names and
// SurrealQL — enforce snake_case so no PascalCase / hyphenated
// identifiers leak through.

use nauka_core_macros::resource;

#[resource(table = "vm", scope = "cluster", custom_actions = "start, StopVM")]
struct Vm {
    #[id]
    pub name: String,
}

fn main() {}
