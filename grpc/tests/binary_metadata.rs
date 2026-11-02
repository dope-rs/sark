use common::headers;
use sark_http::field::Indexing;
pub mod common;

use sark_grpc::{
    metadata::Metadata,
    protocol::{Block, request},
};
use sark_http::field::metadata::map::Owned;

#[test]
fn emits_binary_unpadded() {
    let metadata = headers::try_sensitive_metadata().expect("sensitive metadata");

    let fields =
        Block::request(b"/service/method", None, &metadata).expect("valid request metadata");
    let trace = fields
        .iter()
        .find(|field| field.name == b"trace-bin")
        .expect("binary metadata field");

    assert_eq!(trace.value, b"AAEC/w");
    assert_eq!(trace.indexing, Indexing::Never);
    let head = request::Head::from_fields(&fields).expect("sensitive request metadata");
    assert_eq!(
        head.metadata()
            .entries()
            .next()
            .expect("binary entry")
            .indexing,
        Indexing::Never
    );
}

#[test]
fn accepts_binary_forms() {
    let fields = headers::try_request([
        Owned::try_new(b"padded-bin", b"AAEC/w==").expect("owned field"),
        Owned::try_new(b"plain-bin", b"eHl6").expect("owned field"),
        Owned::try_new(b"joined-bin", b"YQ==,YmM,ZA==").expect("owned field"),
    ])
    .expect("request fields");
    let head = request::Head::from_fields(&fields).expect("valid binary metadata");

    assert_eq!(
        head.metadata().values(b"padded-bin").next(),
        Some([0, 1, 2, 255].as_slice())
    );
    assert_eq!(
        head.metadata().values(b"plain-bin").next(),
        Some(b"xyz".as_slice())
    );
    assert_eq!(
        head.metadata().values(b"joined-bin").collect::<Vec<_>>(),
        [b"a".as_slice(), b"bc".as_slice(), b"d".as_slice()]
    );
}

#[test]
fn roundtrips_large_binary() {
    let raw = headers::PAYLOAD;
    let mut metadata = Metadata::new();
    metadata
        .push(b"trace-bin", raw)
        .expect("bounded binary metadata");
    let fields = Block::request(b"/service/method", None, &metadata).expect("request fields");
    let head = request::Head::from_fields(&fields).expect("decoded request fields");

    assert_eq!(head.metadata().values(b"trace-bin").next(), Some(raw));
}

#[test]
fn drops_invalid_ascii() {
    let fields = headers::try_request([
        Owned::try_new(b"x-good", b"visible").expect("owned field"),
        Owned::try_new(b"x-bad", b"bad\x80value").expect("owned field"),
        Owned::try_new(b"grpc-future", b"ignored").expect("owned field"),
    ])
    .expect("request fields");
    let head = request::Head::from_fields(&fields).expect("valid request");
    let metadata = head.metadata();

    assert_eq!(
        metadata.values(b"x-good").next(),
        Some(b"visible".as_slice())
    );
    assert_eq!(metadata.values(b"x-bad").next(), None);
    assert_eq!(metadata.values(b"grpc-future").next(), None);
}

#[test]
fn rejects_malformed_binary() {
    for value in [b"A".as_slice(), b"@@==".as_slice(), b"YR==".as_slice()] {
        let fields =
            headers::try_request([Owned::try_new(b"trace-bin", value).expect("owned field")])
                .expect("request fields");
        assert!(request::Head::from_fields(&fields).is_err());
    }
}
