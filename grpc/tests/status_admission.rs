use sark_h2::conn;
pub mod common;

use common::headers;
use sark_grpc::{
    Code, Status,
    details::{Binary, Diagnostic, RichStatus},
};
use sark_http::field::base64;

#[test]
fn rejects_conflict() {
    let error =
        Status::from_fields(&headers::try_details(headers::CONFLICT).expect("status fields"))
            .expect_err("contradictory status");
    assert!(matches!(
        Diagnostic::from(&error),
        Diagnostic::RichStatus(RichStatus::Code {
            status: Code::InvalidArgument,
            details: 7
        })
    ));
}

#[test]
fn rejects_default_code() {
    let error =
        Status::from_fields(&headers::try_details(headers::DEFAULT_CODE).expect("status fields"))
            .expect_err("omitted protobuf code defaults to zero");
    assert!(matches!(
        Diagnostic::from(&error),
        Diagnostic::RichStatus(RichStatus::Code {
            status: Code::InvalidArgument,
            details: 0
        })
    ));
}

#[test]
fn rejects_details_on_ok() {
    let error =
        Status::from_fields(&headers::try_details(headers::OK_DETAILS).expect("status fields"))
            .expect_err("details on OK");
    assert!(matches!(
        Diagnostic::from(&error),
        Diagnostic::RichStatus(RichStatus::Ok)
    ));
}

#[test]
fn retains_decode_error() {
    let error = Status::from_fields(
        &headers::try_details(headers::MALFORMED_DETAILS).expect("status fields"),
    )
    .expect_err("incomplete protobuf field");
    assert!(matches!(
        Diagnostic::from(&error),
        Diagnostic::RichStatus(RichStatus::Decode(_))
    ));
}

#[test]
fn retains_binary_error() {
    for value in headers::BAD_BINARY {
        let error = Status::from_fields(&headers::try_binary(value).expect("binary fields"))
            .expect_err("invalid base64");
        assert!(matches!(
            Diagnostic::from(&error),
            Diagnostic::Binary {
                field: Binary::Metadata,
                error: base64::Error::Byte {
                    index: 0,
                    byte: b'@'
                },
            }
        ));
        assert_eq!(error.code(), Code::InvalidArgument);
    }
}

#[test]
fn retains_connection_error() {
    let status = Status::from(headers::CONNECTION);
    assert!(matches!(
        Diagnostic::from(&status),
        Diagnostic::Connection(conn::Error::StreamLimit {
            active: 2,
            limit: 1
        })
    ));
    assert_eq!(status.code(), Code::ResourceExhausted);
}
