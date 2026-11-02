use sark_h3::{conn::io::Step, frame::TYPE_HEADERS};
use sark_http::field::literal;

use crate::common::{
    conn::Events,
    field::entry,
    ledger::{Section as _, owner::Held},
    message::Frame as _,
};
pub mod common;

use std::{collections::VecDeque, num::NonZero};

use dope_quic::wire::stream::Id;
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn::{
        Connection, Error,
        event::{Event, Frame, Phase, Role, Site, Token, Violation},
        options::{Config, Decode, Ingress, Qpack},
    },
    frame::{Payload, STREAM_TYPE_QPACK_ENCODER, TYPE_DATA},
    link::{Pump, Recv as _},
    qpack::encoder::Encoder,
};
use sark_http::payload::response::Body as _;

#[test]
fn quantum_preserves_owner() {
    let stream = Id::new(2).expect("stream id");
    let mut receiver =
        Connection::with_role(Role::Server).expect("fixed H3 transport scenario must succeed");
    let mut link = Held::default();
    link.try_push(stream.get(), vec![0x21])
        .expect("retained input");
    for _ in 0..33 {
        link.try_push(stream.get(), vec![0xaa])
            .expect("retained input");
    }

    Pump::new(&mut link, &mut receiver)
        .event(stream)
        .expect("fixed H3 transport scenario must succeed");
    assert_eq!(link.incoming.get(&stream.get()).map(VecDeque::len), Some(2));
    assert!(sark_h3::conn::io::Receive::new(&mut receiver).is_blocked(stream));

    assert!(matches!(
        receiver
            .events()
            .poll_token()
            .expect("fixed H3 transport scenario must succeed"),
        Step::More
    ));
    Pump::new(&mut link, &mut receiver)
        .credit()
        .expect("fixed H3 transport scenario must succeed");

    assert_eq!(link.incoming.get(&stream.get()).map(VecDeque::len), Some(0));
    assert!(!sark_h3::conn::io::Receive::new(&mut receiver).is_blocked(stream));
    assert_eq!(link.accepted.len(), 34);
    assert!(link.rejected.is_empty());
    assert_eq!(link.released.len(), 34);
    assert_eq!(
        link.released.iter().map(|(_, bytes)| bytes).sum::<usize>(),
        34
    );
}

#[test]
fn quantum_preserves_pending_credit() {
    let stream = Id::new(2).expect("stream id");
    let next = Id::new(6).expect("stream id");
    let mut receiver = Connection::with_role(Role::Server).expect("server");
    let mut link = Held::default();
    link.try_push(stream.get(), vec![0x21])
        .expect("stream type");
    for _ in 0..63 {
        link.try_push(stream.get(), vec![0xaa]).expect("segment");
    }
    link.try_push(next.get(), vec![0x21]).expect("stream type");

    Pump::new(&mut link, &mut receiver)
        .event(stream)
        .expect("first receive quantum");
    Pump::new(&mut link, &mut receiver)
        .event(next)
        .expect("second stream receive");
    assert_eq!(link.accepted.len(), 65);
    assert_eq!(link.released.len(), 63);

    // The last pull in the credit quantum refills the one-slot receipt ring.
    let (id, bytes, resume) = receiver
        .events()
        .poll_credit()
        .expect("queued credit")
        .expect("credit remains after the quantum");
    assert_eq!((id, bytes), (stream, 1));
    assert!(matches!(resume, sark_h3::conn::io::Resume::Idle));
    link.release(id, NonZero::new(bytes).expect("nonempty credit"))
        .expect("release queued credit");
    Pump::new(&mut link, &mut receiver)
        .credit()
        .expect("deferred owner resumes");
    assert_eq!(link.released.len(), 65);
    assert!(!sark_h3::conn::io::Receive::new(&mut receiver).is_blocked(stream));
    assert!(link.rejected.is_empty());
}

#[test]
fn saturation_resumes_owner() {
    let mut block = Vec::new();
    Encoder::new()
        .encode(
            0,
            [
                literal!(entry(b":method", b"POST")),
                literal!(entry(b":scheme", b"https")),
                literal!(entry(b":authority", b"example.test")),
                literal!(entry(b":path", b"/saturated")),
            ],
            &mut block,
        )
        .expect("fixed H3 transport scenario must succeed");
    let mut wire = block.try_frame(TYPE_HEADERS).expect("HEADERS frame");
    Payload::encode(TYPE_DATA, b"retained-body", &mut wire)
        .expect("fixed H3 transport scenario must succeed");
    let wire_len = wire.len();

    let mut receiver = Connection::with_config(
        Role::Server,
        Config {
            ingress: Ingress {
                event_capacity: 1,
                ..Ingress::default()
            },
            ..Config::default()
        },
    )
    .expect("fixed H3 transport scenario must succeed");
    let mut link = Held::default();
    link.try_push(0, wire).expect("retained input");
    link.finished.insert(0);

    Pump::new(&mut link, &mut receiver)
        .event(Id::new(0).expect("stream id"))
        .expect("fixed H3 transport scenario must succeed");
    assert_eq!(link.accepted, [(0, wire_len)]);
    assert!(link.rejected.is_empty());
    assert!(
        matches!(Events::new(&mut receiver).poll_token().expect("fixed H3 transport scenario must succeed"), Some(Event::Headers { message, .. })
            if message.stream_id() == Id::new(0).expect("stream id").into() && message.boundary() == Boundary::More)
    );
    assert!(
        matches!(Events::new(&mut receiver).poll_token().expect("fixed H3 transport scenario must succeed"), Some(Event::Data { message, data })
            if message.stream_id() == Id::new(0).expect("stream id").into()
                && matches!(message.boundary(), Boundary::End)
                && data.segments().flatten().copied().eq(b"retained-body".iter().copied()))
    );
    Pump::new(&mut link, &mut receiver)
        .credit()
        .expect("fixed H3 transport scenario must succeed");
    assert_eq!(
        link.released.iter().map(|(_, n)| n).sum::<usize>(),
        wire_len
    );
    assert!(link.rejected.is_empty());
}

#[test]
fn token_survives_ingress() {
    let mut block = Vec::new();
    Encoder::new()
        .encode(
            0,
            [
                literal!(entry(b":method", b"GET")),
                literal!(entry(b":scheme", b"https")),
                literal!(entry(b":authority", b"example.test")),
                literal!(entry(b":path", b"/resident")),
            ],
            &mut block,
        )
        .expect("fixed H3 transport scenario must succeed");
    let wire = block.try_frame(TYPE_HEADERS).expect("HEADERS frame");
    let wire_len = wire.len();
    let token = Token::new((1 << 62) | 1).expect("fixed H3 transport scenario must succeed");
    let mut receiver =
        Connection::with_role(Role::Server).expect("fixed H3 transport scenario must succeed");
    let mut link = Held::default();
    link.try_push(0, wire).expect("retained input");
    link.finished.insert(0);

    Pump::new(&mut link, &mut receiver)
        .token(Id::new(0).expect("stream id"), token.raw())
        .expect("fixed H3 transport scenario must succeed");
    assert_eq!(link.accepted, [(0, wire_len)]);
    assert!(
        matches!(Events::new(&mut receiver).poll_token().expect("fixed H3 transport scenario must succeed"), Some(Event::Headers { message, .. })
            if message.stream_id() == token && matches!(message.boundary(), Boundary::End))
    );
    assert!(
        Events::new(&mut receiver)
            .poll_token()
            .expect("fixed H3 transport scenario must succeed")
            .is_none()
    );
}

#[test]
fn error_keeps_handoff() {
    let mut wire = Vec::new();
    Payload::encode(TYPE_DATA, b"data-before-headers", &mut wire)
        .expect("fixed H3 transport scenario must succeed");
    let wire_len = wire.len();
    let mut receiver =
        Connection::with_role(Role::Server).expect("fixed H3 transport scenario must succeed");
    let mut link = Held::default();
    link.try_push(0, wire).expect("retained input");

    assert!(
        Pump::new(&mut link, &mut receiver).event(Id::new(0).expect("stream id"))
            == Err(Error::FrameUnexpected(Violation::Receive(Frame {
                stream_id: 0,
                kind: TYPE_DATA.get(),
                role: Role::Server,
                site: Site::Request(Phase::Idle),
            })))
    );
    assert_eq!(link.accepted, [(0, wire_len)]);
    assert!(link.rejected.is_empty());
}

#[test]
fn pump_commits_credit() {
    let dynamic = literal!(entry(b"x-pump-credit", b"retained"));
    let mut sender =
        Encoder::try_bounded(256, 8).expect("fixed H3 transport scenario must succeed");
    sender.set_blocked(2);
    sender
        .encode_for(8, [dynamic])
        .expect("fixed H3 transport scenario must succeed");
    let setup = sender.take_wire();
    let section = sender
        .encode_for(
            0,
            [
                literal!(entry(b":method", b"GET")),
                literal!(entry(b":scheme", b"https")),
                literal!(entry(b":authority", b"example.test")),
                literal!(entry(b":path", b"/pump")),
                dynamic,
            ],
        )
        .expect("fixed H3 transport scenario must succeed");
    let request = section
        .into_wire()
        .try_frame(TYPE_HEADERS)
        .expect("HEADERS frame");
    let req_len = request.len();
    let second = sender
        .encode_for(
            4,
            [
                literal!(entry(b":method", b"GET")),
                literal!(entry(b":scheme", b"https")),
                literal!(entry(b":authority", b"example.test")),
                literal!(entry(b":path", b"/pump-two")),
                dynamic,
            ],
        )
        .expect("fixed H3 transport scenario must succeed")
        .into_wire()
        .try_frame(TYPE_HEADERS)
        .expect("HEADERS frame");
    let next_len = second.len();
    let mut data = Vec::new();
    Payload::encode(TYPE_DATA, b"after-unblock", &mut data)
        .expect("fixed H3 transport scenario must succeed");
    let data_len = data.len();
    let mut wire = Vec::new();
    STREAM_TYPE_QPACK_ENCODER.encode(&mut wire).expect("varint");
    wire.extend_from_slice(&setup);
    let wire_len = wire.len();

    let mut receiver = Connection::with_config(
        Role::Server,
        Config {
            qpack: Qpack {
                decoder: Decode {
                    table_bytes: 256,
                    blocked_streams: 2,
                },
                ..Qpack::default()
            },
            ..Config::default()
        },
    )
    .expect("fixed H3 transport scenario must succeed");
    let mut link = Held::default();
    link.try_push(0, request).expect("retained input");
    link.try_push(0, data).expect("retained input");
    link.try_push(4, second).expect("retained input");
    link.finished.insert(0);
    link.finished.insert(4);
    Pump::new(&mut link, &mut receiver)
        .event(Id::new(0).expect("stream id"))
        .expect("fixed H3 transport scenario must succeed");
    Pump::new(&mut link, &mut receiver)
        .event(Id::new(4).expect("stream id"))
        .expect("fixed H3 transport scenario must succeed");
    assert!(link.released.is_empty());
    assert!(
        Events::new(&mut receiver)
            .poll_token()
            .expect("fixed H3 transport scenario must succeed")
            .is_none()
    );
    Pump::new(&mut link, &mut receiver)
        .event(Id::new(0).expect("stream id"))
        .expect("fixed H3 transport scenario must succeed");
    assert_eq!(link.incoming.get(&0).map(VecDeque::len), Some(1));

    link.try_push(2, wire).expect("retained input");
    Pump::new(&mut link, &mut receiver)
        .event(Id::new(2).expect("stream id"))
        .expect("fixed H3 transport scenario must succeed");
    assert_eq!(
        link.released,
        [(2, wire_len), (0, req_len), (4, next_len), (0, data_len),]
    );
    assert!(
        matches!(Events::new(&mut receiver).poll_token().expect("fixed H3 transport scenario must succeed"), Some(Event::Headers {
            message,
            ..
        }) if message.stream_id() == Id::new(0).expect("stream id").into() && message.boundary() == Boundary::More)
    );
    assert!(
        matches!(Events::new(&mut receiver).poll_token().expect("fixed H3 transport scenario must succeed"), Some(Event::Headers {
            message,
            ..
        }) if message.stream_id() == Id::new(4).expect("stream id").into() && matches!(message.boundary(), Boundary::End))
    );
    assert!(
        matches!(Events::new(&mut receiver).poll_token().expect("fixed H3 transport scenario must succeed"), Some(Event::Data {
            data,
            message,
            ..
        }) if data.segments().flatten().copied().eq(b"after-unblock".iter().copied())
            && matches!(message.boundary(), Boundary::End))
    );
    assert!(
        Events::new(&mut receiver)
            .poll_token()
            .expect("fixed H3 transport scenario must succeed")
            .is_none()
    );
}
