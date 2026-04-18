// Durations must use one of SurrealDB's unit suffixes: `s`, `m`,
// `h`, `d`, `w`, `y`. A bare number is invalid.

use nauka_core_macros::access;

#[access(
    name = "x",
    type = "record",
    signin = "SELECT * FROM x",
    jwt_duration = "60",
    session_duration = "24h"
)]
struct X;

fn main() {}
