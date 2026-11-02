use sark_core::manifold::stream::Boundary;
use sark_h2::{
    frame,
    frame::{
        controls::{Priority, RstStream},
        layout,
        layout::{Data, Header, Headers, Sharing},
        numbers::{Flags, Length, ParseError, Type},
        payload::Payload,
    },
    stream::Id,
};

pub mod common;

#[test]
fn headers_self_dependency() {
    assert!(
        Payload::parse(common::protocol::session::SELF_DEPENDENT_HEADERS)
            == Err(ParseError::Dependency {
                stream: Id::FIRST_CLIENT,
                dependency: Id::FIRST_CLIENT,
            })
    );
    assert!(
        Headers::new(
            Id::FIRST_CLIENT,
            Boundary::More,
            frame::Block::End,
            Some(
                layout::Priority::parse(
                    &common::protocol::session::SELF_DEPENDENT_HEADERS[frame::HEADER_LEN..],
                )
                .expect("priority fixture"),
            ),
            &[],
        ) == Err(ParseError::Dependency {
            stream: Id::FIRST_CLIENT,
            dependency: Id::FIRST_CLIENT,
        })
    );
}

#[test]
fn header_be() {
    let h = Header {
        length: Length::new(0x010203).expect("fixture domain"),
        kind: Type::Headers,
        flags: Flags(0x25),
        stream_id: Id::new(0x0a_0b_0c_0d).expect("fixture domain"),
    };
    let out = h.wire_bytes();
    assert_eq!(out, [1, 2, 3, 1, 0x25, 0x0a, 0x0b, 0x0c, 0x0d]);
    let result = Header::parse(&out);
    let back = result.expect("valid frame fixture must parse");
    assert!((back) == (h));
}

#[test]
fn header_r_bit() {
    let bytes = [0x00, 0x00, 0x00, 0x06, 0x00, 0xff, 0xff, 0xff, 0xff];
    let result = Header::parse(&bytes);
    let h = result.expect("valid frame fixture must parse");
    assert_eq!(h.stream_id.into_u32(), 0x7fff_ffff);
}

#[test]
fn header_short() {
    assert!(
        Header::parse(&[0u8; 8])
            == Err(ParseError::Need {
                actual: 8,
                required: 9,
            })
    );
}

#[test]
fn header_bad_type() {
    let bytes = [0, 0, 0, 0xff, 0, 0, 0, 0, 0];
    assert!(Header::parse(&bytes) == Err(ParseError::BadType(0xff)));
}

#[test]
fn data_zero_stream() {
    let h = Header {
        length: Length::new(1).expect("fixture domain"),
        kind: Type::Data,
        flags: Flags(0),
        stream_id: Id::new(0).expect("fixture domain"),
    };
    assert!(
        Data::parse(h, b"x")
            == Err(ParseError::Stream {
                kind: h.kind,
                actual: h.stream_id,
            })
    );
}

#[test]
fn data_padding() {
    let mut bytes = Vec::new();
    let pad_len: u8 = 3;
    let inner: &[u8] = b"abc";
    let len = 1 + inner.len() + pad_len as usize;
    bytes.extend_from_slice(
        &Header {
            length: Length::new(len as u32).expect("fixture domain"),
            kind: Type::Data,
            flags: Flags(Flags::PADDED | Flags::END_STREAM),
            stream_id: Id::new(5).expect("fixture domain"),
        }
        .wire_bytes(),
    );
    bytes.push(pad_len);
    bytes.extend_from_slice(inner);
    bytes.extend_from_slice(&[0u8; 3]);
    let result = Payload::parse(&bytes);
    let (frame, used) = result.expect("valid frame fixture must parse");
    assert_eq!(used, bytes.len());
    let data = match frame {
        Payload::Data(data) => Some(data),
        _ => None,
    }
    .expect("fixture must parse as DATA");
    assert_eq!(data.payload(), inner);
    assert!(matches!(data.boundary(), Boundary::End));
    assert!((data.stream_id()) == (Id::new(5).expect("fixture domain")));
}

#[test]
fn padding_overflow() {
    let payload = [10u8, 1, 2];
    let h = Header {
        length: Length::new(3).expect("fixture domain"),
        kind: Type::Data,
        flags: Flags(Flags::PADDED),
        stream_id: Id::new(1).expect("fixture domain"),
    };
    assert!(
        Data::parse(h, &payload)
            == Err(ParseError::Padding {
                actual: 3,
                required: 11,
            })
    );
}

#[test]
fn headers_priority_padded() {
    let result = Headers::new(
        Id::new(11).expect("fixture domain"),
        Boundary::End,
        frame::Block::End,
        Some(layout::Priority {
            sharing: Sharing::Exclusive,
            dependency: Id::new(5).expect("fixture domain"),
            weight: 200,
        }),
        b"\x82\x86\x84\x40\x88block",
    );
    let f = result.expect("valid frame fixture must parse");
    let mut bytes = Vec::new();
    let pad_len: u8 = 4;
    let overhead = 5;
    let len = 1 + overhead + f.block().len() + pad_len as usize;
    bytes.extend_from_slice(
        &Header {
            length: Length::new(len as u32).expect("fixture domain"),
            kind: Type::Headers,
            flags: Flags(Flags::END_STREAM | Flags::END_HEADERS | Flags::PRIORITY | Flags::PADDED),
            stream_id: Id::new(11).expect("fixture domain"),
        }
        .wire_bytes(),
    );
    bytes.push(pad_len);
    bytes.extend_from_slice(&[0x80, 0, 0, 5, 200]);
    bytes.extend_from_slice(f.block());
    bytes.extend_from_slice(&[0u8; 4]);
    let result = Payload::parse(&bytes);
    let (parsed, used) = result.expect("valid frame fixture must parse");
    assert_eq!(used, bytes.len());
    assert!((parsed) == (Payload::Headers(f)));
}

#[test]
fn priority_zero_stream() {
    let h = Header {
        length: Length::new(5).expect("fixture domain"),
        kind: Type::Priority,
        flags: Flags(0),
        stream_id: Id::new(0).expect("fixture domain"),
    };
    assert!(
        Priority::parse(h, &[0, 0, 0, 0, 0])
            == Err(ParseError::Stream {
                kind: h.kind,
                actual: h.stream_id,
            })
    );
}

#[test]
fn priority_self_dependency() {
    let h = Header {
        length: Length::new(5).expect("fixture domain"),
        kind: Type::Priority,
        flags: Flags(0),
        stream_id: Id::new(1).expect("fixture domain"),
    };
    assert!(
        Priority::parse(h, &[0, 0, 0, 1, 0])
            == Err(ParseError::Dependency {
                stream: h.stream_id,
                dependency: Id::new(1).expect("fixture domain"),
            })
    );
}

#[test]
fn rst_zero_stream() {
    let h = Header {
        length: Length::new(4).expect("fixture domain"),
        kind: Type::RstStream,
        flags: Flags(0),
        stream_id: Id::new(0).expect("fixture domain"),
    };
    assert!(
        RstStream::parse(h, &[0, 0, 0, 0])
            == Err(ParseError::Stream {
                kind: h.kind,
                actual: h.stream_id,
            })
    );
}
