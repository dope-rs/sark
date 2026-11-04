use dope_test::checks::{Error, Suite};

#[test]
fn contracts() -> Result<(), Error> {
    use std::env;

    let suite = Suite::new(env!("CARGO_MANIFEST_DIR"), file!(), &["sark-gen"]);
    let exe = env::current_exe().expect("current test executable");
    let output = suite
        .try_build(&exe, env!("CARGO_PKG_NAME"))?
        .output()
        .expect("run cargo");
    let suite = suite.try_collect(&exe, output)?;
    for (source, expected) in [
        (
            include_str!("common/compile_message_tag.rs"),
            "error: invalid protobuf field tag",
        ),
        (
            include_str!("common/compile_message_bytes.rs"),
            "error: Message requires a bytes field",
        ),
    ] {
        let output = compile(&mut suite.compiler(), source);
        assert!(!output.status.success());
        let errors = String::from_utf8(output.stderr).expect("UTF-8 diagnostics");
        assert!(errors.lines().any(|line| line == expected), "{errors}");
    }
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
