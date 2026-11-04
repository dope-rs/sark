//! ```compile_fail,E0277
//! include!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/common/forged.rs"));
//! fn main() {}
//! ```

use dope_test::checks::{Error, Suite};

#[test]
fn contracts() -> Result<(), Error> {
    let suite = Suite::new(env!("CARGO_MANIFEST_DIR"), file!(), &["sark-grpc"]);
    let exe = std::env::current_exe().expect("current test executable");
    let output = suite
        .try_build(&exe, env!("CARGO_PKG_NAME"))?
        .output()
        .expect("run cargo");
    let artifacts = suite.try_collect(&exe, output)?;
    assert_eq!(
        artifacts.try_errors(include_str!("compile_message_contracts.rs"), compile)?,
        1
    );
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
