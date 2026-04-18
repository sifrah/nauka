// Unknown `#[access]` keys are a typo risk — fail fast rather than
// silently drop.

use nauka_core_macros::access;

#[access(
    name = "x",
    type = "record",
    signin = "SELECT * FROM x",
    jwt_duration = "1h",
    session_duration = "24h",
    typo_here = "oops"
)]
struct X;

fn main() {}
