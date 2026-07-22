#[test]
fn borrowed_async_responses_do_not_compile() {
    trybuild::TestCases::new().compile_fail("tests/ui/*.rs");
}
