// `restrict_delete` entries must be in `table:field` form — a bare
// entry must be rejected.

use nauka_core_macros::resource;

#[resource(
    table = "x",
    scope = "local",
    restrict_delete = "bare_entry_no_colon"
)]
struct X {
    #[id]
    pub k: String,
}

fn main() {}
