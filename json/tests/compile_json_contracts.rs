//! ```
//! # mod common;
//! use common::members::Price;
//! use sark_json::writing::TryEncode as _;
//!
//! mod hygiene {
//!     const __bytes: usize = 0;
//!     const __size: usize = 0;
//!     #[sark_json::json]
//!     pub struct Json { pub value: u64 }
//! }
//!
//! #[sark_json::json(encode)]
//! pub struct Generic<const r#__FIRST: usize, const __w: usize = 0, const __size: usize = 0> {
//!     #[field(flatten)]
//!     pub price: Price,
//!     pub token: sark_json::Token<r#__FIRST>,
//! }
//!
//! fn main() {
//!     let value = Generic::<8> {
//!         price: Price { price: 3, quantity: 4 },
//!         token: sark_json::Text::try_new(sark_json::Token::new()).expect("text"),
//!     };
//!     assert!(value.try_encode().is_ok());
//! }
//! ```
//!
//! ```compile_fail
//! #[sark_json::json]
//! struct Renamed {
//!     #[field(name = "same")]
//!     first: u64,
//!     #[field(name = "same")]
//!     second: u64,
//! }
//! ```
//!
//! ```compile_fail
//! #[sark_json::json]
//! struct Row {
//!     id: u64,
//! }
//!
//! #[sark_json::json]
//! struct Array {
//!     #[field(seq, nested)]
//!     rows: [Row; 1],
//! }
//! ```
//!
//! ```compile_fail
//! #[sark_json::json(encode)]
//! struct Collision {
//!     value: u64,
//!     #[field(name = "value")]
//!     other: u64,
//! }
//! ```

#[cfg(test)]
use dope_test::checks::{Error, Suite};

#[test]
fn contracts() -> Result<(), Error> {
    use std::env;

    let suite = Suite::new(
        env!("CARGO_MANIFEST_DIR"),
        file!(),
        &["o3", "sark-http", "sark-json"],
    );
    let exe = env::current_exe().expect("current test executable");
    let output = suite
        .try_build(&exe, "sark-json")?
        .output()
        .expect("run cargo");
    let suite = suite.try_collect(&exe, output)?;
    let output = compile(
        &mut suite.compiler(),
        include_str!("common/compile_generic.rs"),
    );
    assert!(!output.status.success());
    let errors = String::from_utf8(output.stderr).expect("UTF-8 diagnostics");
    assert!(
        errors.lines().any(|line| {
            line == "error: generic JSON declarations require `encode` or `try_encode`"
        }),
        "{errors}"
    );
    let status = suite.try_rustdoc()?.status().expect("run rustdoc");
    assert!(status.success(), "JSON wire names: {status}");
    Ok(())
}

fn compile(command: &mut std::process::Command, source: &str) -> std::process::Output {
    let mut child = command.spawn().expect("start compiler");
    let written = child
        .stdin
        .take()
        .map(|mut input| std::io::Write::write_all(&mut input, source.as_bytes()));
    let output = child.wait_with_output().expect("reap compiler");
    written
        .expect("compiler stdin")
        .expect("write compiler source");
    output
}
