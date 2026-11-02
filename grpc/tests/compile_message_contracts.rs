//! ```compile_fail,E0277
//! include!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/common/forged.rs"));
//! fn main() {}
//! ```

use dope_test::checks::{Error, Suite};

#[test]
fn contracts() -> Result<(), Error> {
    let suite = Suite::new(env!("CARGO_MANIFEST_DIR"), file!(), &["sark-grpc"]);
    let exe = std::env::current_exe()?;
    let output = suite.try_build(&exe, env!("CARGO_PKG_NAME"))?.output()?;
    let artifacts = suite.try_collect(&exe, output)?;
    assert_eq!(
        artifacts.try_errors(include_str!("compile_message_contracts.rs"))?,
        1
    );
    Ok(())
}
