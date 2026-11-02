use common::headers;
use sark_grpc::{
    details::{Binary, Diagnostic},
    metadata::{Error, Metadata},
};
use sark_http::field::{Indexing, base64};
pub mod common;

use sark_grpc::{Code, Reason, Status, protocol::Block};
use sark_http::field::{Encode as _, base64::Encoder, metadata::map::Owned};

#[test]
fn decodes_absent_message() {
    let fields = vec![Owned::try_new(b"grpc-status", b"0").expect("owned field")];
    let status = Status::from_fields(&fields).expect("status without a message is valid");
    assert!((status.code()) == (Code::Ok));
    assert_eq!(status.message(), None);

    let mut metadata = Metadata::new();
    std::assert_matches!(
        metadata.push(b"grpc-status", b"0"),
        Err(Error::ReservedName)
    );
}

#[test]
fn encodes_status_codes() {
    for value in 0..=16 {
        let code = Code::from_u8(value).expect("defined status code");
        let status = Status::from_code(code);
        let fields = Block::trailers(&status, &Metadata::new()).expect("status trailers");
        let field = fields
            .iter()
            .find(|field| field.name == b"grpc-status")
            .expect("status field");
        assert_eq!(field.value, value.to_string().as_bytes());
    }
}

#[test]
fn owns_wire_status() {
    let fields = headers::try_status().expect("status fields");
    let status = Status::from_fields(&fields).expect("wire status fields are valid");

    assert!((status.code()) == (Code::InvalidArgument));
    assert_eq!(status.message(), Some("terminal \u{2603}\n"));
    assert_eq!(status.details(), Some(headers::DETAILS));
    assert_eq!(
        status.metadata().values(b"x-trace").next(),
        Some(b"42".as_slice())
    );
    let output = Block::trailers(&status, &Metadata::new()).expect("output status");
    assert!(output.iter().all(|field| field.indexing == Indexing::Never));
}

#[test]
fn folds_large_details() {
    let raw = headers::LARGE;
    let encoder = Encoder::padded(raw);
    let mut encoded = Vec::with_capacity(encoder.encoded_len());
    encoder.write(|block| encoded.extend_from_slice(block));
    let fields = vec![
        Owned::try_new(b"grpc-status", b"3").expect("owned field"),
        Owned::try_new(b"grpc-status-details-bin", &encoded).expect("owned field"),
    ];
    let status = Status::from_fields(&fields).expect("large status details are valid");

    assert_eq!(status.details(), Some(raw));
}

#[test]
fn keeps_malformed_escapes() {
    let fields = vec![
        Owned::try_new(b"grpc-status", b"0").expect("owned field"),
        Owned::try_new(b"grpc-message", b"bad%2%FF").expect("owned field"),
    ];
    let status = Status::from_fields(&fields).expect("malformed escapes remain valid status data");

    assert_eq!(status.message(), Some("bad%2?"));
}

#[test]
fn rejects_control_values() {
    for name in headers::GRPC_VALUE_FIELDS {
        for value in headers::RAW_CONTROL_VALUES {
            let fields =
                headers::try_trailers([Owned::try_new(name, value).expect("raw field bytes")])
                    .expect("status fields");
            let error = Status::from_fields(&fields)
                .expect_err("raw HTTP control bytes must be rejected before gRPC folding");
            assert!(matches!(
                Diagnostic::from(&error),
                Diagnostic::Local {
                    code: Code::InvalidArgument,
                    reason: Reason::FieldSection,
                }
            ));
        }
    }
}

#[test]
fn omits_local_message() {
    let metadata = Metadata::new();
    let source = Status::from_code(Code::InvalidArgument);
    let fields = Block::trailers(&source, &metadata).expect("local status forms valid trailers");
    assert!(
        fields
            .iter()
            .find(|field| field.name == b"grpc-message")
            .is_none()
    );

    let status = Status::from_fields(&fields).expect("generated trailers form a valid status");
    assert_eq!(status.message(), None);
}

#[test]
fn rejects_bad_details() {
    let fields = headers::try_details(headers::BAD_BASE64_DETAILS).expect("status fields");
    let status = (Status::from_fields(&fields)).expect_err("bad status details must be rejected");

    assert!((status.code()) == (Code::Internal));
    assert!(matches!(
        Diagnostic::from(&status),
        Diagnostic::Binary {
            field: Binary::Details,
            error: base64::Error::Length { actual: 1 },
        }
    ));
}

#[test]
fn rejects_duplicate_status() {
    let fields = vec![
        Owned::try_new(b"grpc-status", b"0").expect("owned field"),
        Owned::try_new(b"grpc-status", b"1").expect("owned field"),
    ];
    let status =
        (Status::from_fields(&fields)).expect_err("duplicate status fields must be rejected");
    assert!((status.code()) == (Code::InvalidArgument));
}

#[test]
fn validates_status_domain() {
    for value in 0..=u8::MAX {
        let text = value.to_string();
        let fields = vec![Owned::try_new(b"grpc-status", text.as_bytes()).expect("owned field")];
        let result = Status::from_fields(&fields);
        if value <= 16 {
            let status = result.expect("represented status is valid");
            assert_eq!(status.code().into_u8(), value);
        } else {
            let error = result.expect_err("unrepresented status is rejected");
            assert!((error.code()) == (Code::Internal));
        }
    }
}

#[test]
fn requires_status_value() {
    let empty = Vec::<Owned>::new();
    let error = Status::from_fields(&empty).expect_err("absent status is rejected");
    assert!((error.code()) == (Code::Internal));

    for (value, code) in headers::INVALID_STATUS_VALUES {
        let fields = vec![Owned::try_new(b"grpc-status", value).expect("owned field")];
        let error = Status::from_fields(&fields).expect_err("malformed status is rejected");
        assert_eq!(error.code(), code);
    }
}
