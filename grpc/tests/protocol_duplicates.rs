pub mod common;

use common::headers;
use sark_grpc::{
    Code, Reason,
    details::{Diagnostic, Fault},
    protocol::{request, response},
};
use sark_http::field::metadata::map::Owned;

#[test]
fn rejects_duplicate_path() {
    let fields = vec![
        Owned::try_new(b":method", b"POST").expect("owned field"),
        Owned::try_new(b":scheme", b"https").expect("owned field"),
        Owned::try_new(b":path", b"/service/method").expect("owned field"),
        Owned::try_new(b":path", b"/service/other").expect("owned field"),
        Owned::try_new(b"content-type", b"application/grpc").expect("owned field"),
        Owned::try_new(b"te", b"trailers").expect("owned field"),
    ];

    let error = (request::Head::from_fields(&fields))
        .err()
        .expect("duplicate :path must be rejected");
    assert!((error.code()) == (Code::InvalidArgument));
}

#[test]
fn keeps_request_slots() {
    let fields = vec![
        Owned::try_new(b":method", b"POST").expect("owned field"),
        Owned::try_new(b":scheme", b"https").expect("owned field"),
        Owned::try_new(b":path", b"/service/method").expect("owned field"),
        Owned::try_new(b":authority", b"localhost").expect("owned field"),
        Owned::try_new(b"content-type", b"application/grpc+proto").expect("owned field"),
        Owned::try_new(b"te", b"trailers").expect("owned field"),
    ];

    let head = request::Head::from_fields(&fields).expect("valid request head");
    assert_eq!(head.path(), b"/service/method");
    assert_eq!(head.authority(), Some(b"localhost".as_slice()));
}

#[test]
fn rejects_response_field() {
    let fields = vec![
        Owned::try_new(b":method", b"POST").expect("owned field"),
        Owned::try_new(b":scheme", b"https").expect("owned field"),
        Owned::try_new(b":path", b"/service/method").expect("owned field"),
        Owned::try_new(b"content-type", b"application/grpc+proto").expect("owned field"),
        Owned::try_new(b"te", b"trailers").expect("owned field"),
        Owned::try_new(b"grpc-message", b"not-a-request-field").expect("owned field"),
    ];

    let error = (request::Head::from_fields(&fields))
        .err()
        .expect("response field in request must be rejected");
    assert!((error.code()) == (Code::InvalidArgument));
}

#[test]
fn rejects_early_status() {
    let fields = vec![
        Owned::try_new(b":status", b"200").expect("owned field"),
        Owned::try_new(b"content-type", b"application/grpc+proto").expect("owned field"),
        Owned::try_new(b"grpc-status", b"0").expect("owned field"),
    ];

    let error = (response::Head::from_fields(&fields))
        .err()
        .expect("status in an open response must be rejected");
    assert!((error.code()) == (Code::Internal));
}

#[test]
fn rejects_ignored_controls() {
    for value in headers::RAW_CONTROL_VALUES {
        let fields =
            headers::try_request(
                [Owned::try_new(b"grpc-ignored", value).expect("raw field bytes")],
            )
            .expect("request fields");
        let error = request::Head::from_fields(&fields)
            .err()
            .expect("ignored gRPC fields still require HTTP value admission");
        assert!(matches!(
            Diagnostic::from(&error),
            Diagnostic::Local {
                code: Code::InvalidArgument,
                reason: Reason::FieldSection,
            }
        ));
    }
}

#[test]
fn retains_te_errors() {
    for value in [b"TrAiLeRs".as_slice(), b"trailers, trailers"] {
        let mut fields = headers::try_request([]).expect("request fields");
        *fields.last_mut().expect("TE field") = Owned::try_new(b"te", value).expect("owned field");
        let error = request::Head::from_fields(&fields)
            .err()
            .expect("strict gRPC TE");
        assert!(matches!(
            Diagnostic::from(&error),
            Diagnostic::SectionStored(Fault::Te)
        ));
    }
    let fields = headers::try_request([Owned::try_new(b"te", b"trailers").expect("owned field")])
        .expect("request fields");
    let error = request::Head::from_fields(&fields)
        .err()
        .expect("duplicate gRPC TE");
    assert!(matches!(
        Diagnostic::from(&error),
        Diagnostic::SectionStored(Fault::Duplicate)
    ));
}
