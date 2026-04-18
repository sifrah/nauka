// `#[access]` requires a `signin = "…"` clause — without it, no one
// can authenticate.

use nauka_core_macros::access;

#[access(
    name = "x",
    type = "record",
    jwt_duration = "1h",
    session_duration = "24h"
)]
struct X;

fn main() {}
