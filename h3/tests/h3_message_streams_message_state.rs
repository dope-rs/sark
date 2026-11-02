use core::slice;

use dope_quic::wire::{stream::Id, varint::VarInt};
use sark_http::field::literal;

use crate::common::{
    conn::{Events, build::Fields as _},
    field::entry,
    message::{
        Frame as _,
        promise::{Authority, Push as _},
    },
    stream::Stream as _,
};

pub mod common;

use sark_core::manifold::stream::{Boundary, Section};
use sark_h3::{
    conn::{
        Connection,
        event::{Event, Role},
        io::{Receive, Send},
    },
    frame::{TYPE_DATA, TYPE_HEADERS},
    qpack::encoder::Encoder,
};
use sark_http::{field::metadata::strings::Encoding, payload::response::Body as _};

#[test]
fn fragmented_decode() {
    let mut encoder = Encoder::new();
    encoder.set_encoding(Encoding::Huffman);
    let mut block = Vec::new();
    encoder
        .encode(
            0,
            [
                literal!(entry(b":method", b"GET")),
                literal!(entry(b":scheme", b"https")),
                literal!(entry(b":authority", b"example.test")),
                literal!(entry(b":path", b"/fragmented")),
                literal!(entry(b"x-fragmented-name", b"fragmented-huffman-value")),
            ],
            &mut block,
        )
        .expect("fixed fragmented request must encode");
    let wire = (&block).try_frame(TYPE_HEADERS).expect("message fixture");
    let mut conn =
        Connection::with_role(Role::Server).expect("default H3 server configuration must be valid");
    for (index, byte) in wire.iter().enumerate() {
        let boundary = if index + 1 == wire.len() {
            Boundary::End
        } else {
            Boundary::More
        };
        Receive::new(&mut conn)
            .borrowed(
                Id::new(0).expect("stream id"),
                slice::from_ref(byte),
                boundary,
            )
            .expect("each fragment must extend the valid request frame");
    }

    assert!(matches!(Events::new(&mut conn)
            .poll_token()
            .expect("fragmented request must poll"), Some(Event::Headers {
            section: Section::Initial,
            message,
            ..
        }) if matches!(message.boundary(), Boundary::End)));
    assert!(
        Events::new(&mut conn)
            .poll_token()
            .expect("drained fragmented request must poll")
            .is_none()
    );
}

#[test]
fn streams_share_states() {
    let trailers = [literal!(entry(b"x-trailer", b"done"))]
        .try_block()
        .expect("message fixture");
    let mut block = Vec::new();
    Encoder::new()
        .encode(
            0,
            [
                literal!(entry(b":method", b"GET")),
                literal!(entry(b":scheme", b"https")),
                literal!(entry(b":authority", b"example.test")),
                literal!(entry(b":path", b"/")),
            ],
            &mut block,
        )
        .expect("fixed request headers must encode");
    let mut wire = (&block).try_frame(TYPE_HEADERS).expect("message fixture");
    wire.extend(b"body".try_frame(TYPE_DATA).expect("message fixture"));
    wire.extend(
        (&trailers)
            .try_frame(TYPE_HEADERS)
            .expect("message fixture"),
    );

    let mut conn =
        Connection::with_role(Role::Server).expect("default H3 server configuration must be valid");
    Receive::new(&mut conn)
        .owned(Id::new(0).expect("stream id"), wire, Boundary::End)
        .expect("fixed request stream must be valid");
    assert!(matches!(Events::new(&mut conn)
            .poll_token()
            .expect("request headers must poll"), Some(Event::Headers {
            section: Section::Initial,
            message,
            ..
        }) if message.boundary() == Boundary::More));
    assert!(matches!(Events::new(&mut conn)
            .poll_token()
            .expect("request body must poll"), Some(Event::Data {
            data,
            message,
            ..
        }) if data.segments().flatten().copied().eq(b"body".iter().copied()) && message.boundary() == Boundary::More));
    assert!(matches!(Events::new(&mut conn)
            .poll_token()
            .expect("request trailers must poll"), Some(Event::Headers {
            section: Section::Trailing,
            message,
            ..
        }) if matches!(message.boundary(), Boundary::End)));
    assert!(
        Events::new(&mut conn)
            .poll_token()
            .expect("drained request stream must poll")
            .is_none()
    );

    let mut wire = [literal!(entry(b":status", b"200"))]
        .try_block()
        .expect("message fixture")
        .try_frame(TYPE_HEADERS)
        .expect("message fixture");
    wire.extend(b"body".try_frame(TYPE_DATA).expect("message fixture"));
    wire.extend(
        (&trailers)
            .try_frame(TYPE_HEADERS)
            .expect("message fixture"),
    );
    let mut conn = Connection::with_push::<Authority>(Role::Client).expect("client push authority");
    conn.allow_push(Role::Client, 9).expect("push grant");
    Send::new(&mut conn)
        .headers(Id::ZERO, Authority, Boundary::End)
        .expect("originating GET request");
    assert!(
        conn.events()
            .poll_write()
            .expect("request output")
            .is_some()
    );
    Receive::new(&mut conn)
        .owned(
            Id::ZERO,
            VarInt::new(9)
                .expect("push id")
                .try_promise(&Authority.try_request().expect("promised GET"))
                .expect("push promise"),
            Boundary::More,
        )
        .expect("promise admission");
    assert!(matches!(
        Events::new(&mut conn)
            .poll_token()
            .expect("accepted promise"),
        Some(Event::PushPromise { push_id: 9, .. })
    ));
    Receive::new(&mut conn)
        .owned(
            Id::new(3).expect("stream id"),
            VarInt::new(9)
                .expect("push id")
                .try_stream(&wire)
                .expect("push wire"),
            Boundary::End,
        )
        .expect("fixed push stream must be valid");
    assert!(matches!(Events::new(&mut conn)
            .poll_token()
            .expect("push headers must poll"), Some(Event::PushHeaders {
            push_id: 9,
            section: Section::Initial,
            message,
            ..
        }) if message.boundary() == Boundary::More));
    assert!(matches!(Events::new(&mut conn)
            .poll_token()
            .expect("push body must poll"), Some(Event::PushData {
            push_id: 9,
            data,
            message,
            ..
        }) if data.segments().flatten().copied().eq(b"body".iter().copied()) && message.boundary() == Boundary::More));
    assert!(matches!(Events::new(&mut conn)
            .poll_token()
            .expect("push trailers must poll"), Some(Event::PushHeaders {
            push_id: 9,
            section: Section::Trailing,
            message,
            ..
        }) if matches!(message.boundary(), Boundary::End)));
    assert!(
        Events::new(&mut conn)
            .poll_token()
            .expect("drained push stream must poll")
            .is_none()
    );
}
