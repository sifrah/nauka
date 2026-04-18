// Only `type = "record"` is accepted today — `"jwt"` is reserved for
// post-MVP OIDC/SSO work.

use nauka_core_macros::access;

#[access(
    name = "x",
    type = "jwt",
    signin = "SELECT * FROM x",
    jwt_duration = "1h",
    session_duration = "24h"
)]
struct X;

fn main() {}
