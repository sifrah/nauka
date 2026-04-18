// `#[access]` requires a `name = "…"` argument.

use nauka_core_macros::access;

#[access(
    type = "record",
    signin = "SELECT * FROM x",
    jwt_duration = "1h",
    session_duration = "24h"
)]
struct X;

fn main() {}
