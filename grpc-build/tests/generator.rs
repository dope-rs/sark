pub mod common;

use common::service;
use quote::ToTokens;

#[test]
fn preserves_type_scope() {
    let source = "__SarkGrpcProofMethod0Input";
    let mut schema = service::streaming();
    schema.methods[0].input_type = source.into();
    let file = service::generate([schema]).expect("generated service");
    let alias = file
        .items
        .iter()
        .find_map(|item| match item {
            syn::Item::Type(alias) if alias.ty.to_token_stream().to_string() == source => {
                Some(alias)
            }
            _ => None,
        })
        .expect("original message type retained at caller scope");
    assert_ne!(alias.ident, source);
}

#[test]
fn rejects_duplicate_paths() {
    let file = service::generate([service::duplicate_paths()]).expect("generated diagnostic");
    let [syn::Item::Macro(error)] = file.items.as_slice() else {
        panic!("expected a single diagnostic");
    };
    assert!(
        error
            .mac
            .path
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "compile_error")
    );
    let message = (syn::parse2::<syn::LitStr>(error.mac.tokens.clone()))
        .expect("the diagnostic must contain a literal message");
    assert_eq!(
        message.value(),
        "duplicate gRPC route at method indices 0 and 1"
    );
}

#[test]
fn rejects_factory_name() {
    let file = service::generate([service::empty("1Proof")]).expect("generated diagnostic");
    assert!(matches!(file.items.as_slice(), [syn::Item::Macro(error)]
        if error.mac.path.segments.last().is_some_and(|segment| segment.ident == "compile_error")));
}

#[test]
fn runs_generated_services() -> Result<(), dope_test::checks::Error> {
    use std::{env, path::Path, process::Command};

    use dope_test::{
        checks::{Suite, try_compile},
        harness::support::file::Directory,
    };

    let routes = service::generate(service::routes()).expect("generated routes");
    let messages = service::generate(service::messages()).expect("generated messages");
    let collisions = service::collisions().expect("generated colliding service");
    let bindings = syn::parse_file(include_str!("common/bindings.rs")).expect("service fixtures");
    let schema = syn::parse_file(include_str!("common/schema.rs")).expect("message fixtures");
    let source = quote::quote! {
        pub mod common {
            pub mod runtime;
            pub mod schema {
                #schema
                #messages
            }
        }
        pub mod bindings {
            #routes
            #collisions
            #bindings
        }

        fn main() {
            use std::{cell::Cell, mem};
            use bindings::*;
            use common::runtime::execute;
            use sark_grpc::{Code, server::service::handler::Handler};

            let mut calls = Cell::new(Vec::new());
            let routes = proof_routes(Service { calls: &calls });
            assert_eq!(mem::size_of_val(&routes), mem::size_of::<Service<'_>>());
            assert_eq!(mem::size_of::<<ProofRoutes<Service<'static>> as Handler>::Call>(), 1);
            execute(routes, &[
                (b"/sark.test.Proof/Alpha", Code::Ok),
                (b"/sark.test.Proof/Alpine", Code::Ok),
                (b"/sark.test.Proof/Beta", Code::Ok),
                (b"/sark.test.Proof/Type", Code::Ok),
                (b"/sark.test.Proof/Call", Code::Ok),
                (b"/sark.test.Proof/Unknown", Code::Ok),
                (b"/sark.test.Proof/Alph", Code::Unimplemented),
                (b"/sark.test.Proof/AlphaX", Code::Unimplemented),
                (b"/sark.test.Proof/Alpha?x", Code::Unimplemented),
                (b"/sark.test.Other/Alpha", Code::Unimplemented),
            ]);
            assert_eq!(calls.get_mut().as_slice(), &[0, 1, 2, 3, 4, 5]);

            let calls = Cell::new(0);
            let mut paths = Cell::new(Vec::new());
            execute(_123_routes(Shapes { calls: &calls, paths: &paths }), &[
                (b"/sark.test._123/_1", Code::Ok),
                (b"/sark.test._123/_2", Code::Ok),
                (b"/sark.test._123/_3", Code::Ok),
                (b"/sark.test._123/_4", Code::Ok),
            ]);
            assert_eq!(calls.get(), 4);
            assert_eq!(paths.get_mut().as_slice(), &[b"/sark.test._123/_4".to_vec()]);
            execute(empty_routes(Empty), &[
                (b"/sark.test.Empty/Missing", Code::Unimplemented),
            ]);
            let routes: CollisionRoutes__<Empty> = collision_routes(Empty);
            execute(routes, &[
                (b"/sark.Test.Collision/Echo", Code::Ok),
                (b"/sark.test.Collision/Echo", Code::Unimplemented),
            ]);
        }
    };
    let suite = Suite::new(
        env!("CARGO_MANIFEST_DIR"),
        file!(),
        &[
            "dope",
            "dope-test",
            "sark-core",
            "sark-grpc",
            "sark-h2",
            "sark-http",
            "prost",
            "sark-gen",
        ],
    );
    let exe = env::current_exe()?;
    let output = suite.try_build(&exe, env!("CARGO_PKG_NAME"))?.output()?;
    let artifacts = suite.try_collect(&exe, output)?;
    let directory = Directory::try_with(
        exe.parent().expect("test executable directory"),
        "grpc-generated",
        |result| result.expect("remove generated executable"),
    )
    .expect("generated executable directory");
    let binary = directory.path().join("generated");
    let mut compiler = artifacts.compiler();
    if exe
        .parent()
        .and_then(Path::parent)
        .is_some_and(|profile| profile.ends_with("release"))
    {
        compiler.args(["-Clto=fat", "-Copt-level=3", "-Ccodegen-units=1"]);
    }
    compiler
        .arg("-Dunused_variables")
        .arg(format!("--emit=link={}", binary.display()));
    let compiled = try_compile(&mut compiler, &source.to_string())?;
    assert!(
        compiled.status.success(),
        "{}",
        String::from_utf8_lossy(&compiled.stderr)
    );
    let status = Command::new(&binary).status()?;
    assert!(status.success(), "generated services: {status}");
    directory.try_close().expect("remove generated executable");
    Ok(())
}
