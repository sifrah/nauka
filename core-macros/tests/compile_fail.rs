//! Compile-fail harness for the Resource contract — see ADR 0006.
//!
//! Each `tests/compile_fail/*.rs` file is a deliberate violation of
//! one contract invariant. The macro must reject it with a clear
//! diagnostic (captured in the matching `.stderr` file).
//!
//! A passing build of these tests is what proves the contract is
//! actually enforced — without them, "the macro could in principle
//! reject this" is just a comment.

#[test]
fn compile_fail() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/*.rs");
}
