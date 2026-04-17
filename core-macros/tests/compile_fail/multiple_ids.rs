// Two `#[id]` fields are ambiguous — there must be exactly one
// natural key per resource.

use nauka_core_macros::resource;

#[resource(table = "two_ids", scope = "local")]
struct TwoIds {
    #[id]
    pub a: String,
    #[id]
    pub b: String,
}

fn main() {}
