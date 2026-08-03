use sark_core::http::Field;
use sark_h3::qpack::Encoder;
use sark_h3::{Conn, ConnError, Frame, Role, StreamId, TYPE_HEADERS};

fn headers(fields: &[Field<'_>]) -> Vec<u8> {
    let mut block = Vec::new();
    Encoder::new().encode(fields.iter().copied(), &mut block);
    let mut wire = Vec::new();
    Frame::encode(TYPE_HEADERS, &block, &mut wire).unwrap();
    wire
}

fn ingest_request(fields: &[Field<'_>]) -> Result<(), ConnError> {
    Conn::with_role(Role::Server).ingest_stream_owned(StreamId::new(0), headers(fields), false)
}

#[test]
fn request_field_sections_enforce_the_shared_h2_h3_rules() {
    assert_eq!(
        ingest_request(&[Field::new(b":method", b"GET"), Field::new(b":path", b"/"),]),
        Err(ConnError::Message),
        "the request pseudo-header set must be complete",
    );
    assert_eq!(
        ingest_request(&[
            Field::new(b":method", b"GET"),
            Field::new(b":scheme", b"https"),
            Field::new(b":path", b"/"),
            Field::new(b"x-regular", b"1"),
            Field::new(b":authority", b"example.test"),
        ]),
        Err(ConnError::Message),
        "pseudo-headers cannot follow regular fields",
    );
    assert_eq!(
        ingest_request(&[
            Field::new(b":method", b"GET"),
            Field::new(b":scheme", b"https"),
            Field::new(b":path", b"/"),
            Field::new(b"Connection", b"close"),
        ]),
        Err(ConnError::Message),
        "uppercase and connection-specific fields are forbidden",
    );
    assert!(
        ingest_request(&[
            Field::new(b":method", b"GET"),
            Field::new(b":scheme", b"https"),
            Field::new(b":path", b"/"),
            Field::new(b"te", b"trailers"),
        ])
        .is_ok(),
    );
}
