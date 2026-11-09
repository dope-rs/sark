pub mod common;

use common::service;
use quote::ToTokens;

#[test]
fn preserves_type_scope() {
    let source = "__SarkGrpcProofMethod0Input";
    let mut schema = service::streaming();
    schema.methods[0].input_type = source.into();
    let file = service::generate(schema).expect("generated service");
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
    let file = service::generate(service::duplicate_paths()).expect("generated diagnostic");
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
    let file = service::generate(service::empty("1Proof")).expect("generated diagnostic");
    assert!(matches!(file.items.as_slice(), [syn::Item::Macro(error)]
        if error.mac.path.segments.last().is_some_and(|segment| segment.ident == "compile_error")));
}
