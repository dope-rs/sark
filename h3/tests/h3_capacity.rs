use dope_quic::errors::Code;
use sark_h3::VarInt;
use sark_http::field::literal;

use crate::common::{
    conn::{Connection as _, Events},
    field::entry,
    message::{
        capacity::{Capacity as _, Request},
        promise::{Authority, Push as _},
    },
    stream::Stream as _,
};

pub mod common;
use std::num::NonZero;

use dope_quic::wire::stream::Id;
use sark_core::{
    manifold,
    manifold::stream::{Boundary, Section},
};
use sark_h3::{
    conn::{
        Connection, Error,
        event::{Event, Frame, Phase, Role, Site, Violation},
        io,
        io::{Receive, Send},
        options::{Config, Ingress, Streams, plan},
    },
    frame::{
        Payload, STREAM_TYPE_CONTROL, STREAM_TYPE_PUSH, TYPE_DATA, TYPE_HEADERS, TYPE_SETTINGS,
    },
    qpack::encoder::Encoder,
    stream::UniType,
};
use sark_http::{field::metadata::section::rules::RetainRaw, payload::response::Body as _};

#[test]
fn reuses_bounded_queues() {
    let mut events = Connection::try_bounded().expect("capacity connection");
    let result =
        Receive::new(&mut events).stop(Id::new(0).expect("stream ID"), None, Code::from_u16(1));
    assert!(result.is_ok(), "first stop must fit the event queue");
    let (error, charge) = Receive::new(&mut events)
        .stop(Id::new(4).expect("stream ID"), None, Code::from_u16(1))
        .expect_err("full event queue rejects the stop");
    assert!(
        error
            == Error::EventCapacity {
                len: 1,
                capacity: 1
            }
    );
    assert!(charge.is_empty());
    assert!(
        Events::new(&mut events)
            .poll_token()
            .expect("capacity event")
            .is_some()
    );
    let result =
        Receive::new(&mut events).stop(Id::new(4).expect("stream ID"), None, Code::from_u16(1));
    assert!(
        result.is_ok(),
        "second stop must reuse the drained event queue"
    );

    let mut writes = Connection::try_bounded().expect("capacity connection");
    (Send::new(&mut writes).data(Id::new(0).expect("stream ID"), b"a", Boundary::More))
        .expect("capacity fixture satisfies its declared invariants");
    assert!(
        Send::new(&mut writes).data(
            Id::new(0).expect("stream ID"),
            b"b",
            manifold::stream::Boundary::More
        ) == Err(Error::QueueRegular {
            queued: 1,
            additional: 1,
            limit: 1,
        })
    );
    assert!(
        (writes.events().poll_write())
            .expect("capacity fixture satisfies its declared invariants")
            .is_some()
    );
    (Send::new(&mut writes).data(Id::new(0).expect("stream ID"), b"c", Boundary::More))
        .expect("capacity fixture satisfies its declared invariants");

    let mut critical = Connection::try_bounded().expect("capacity connection");
    Send::new(&mut critical)
        .data(Id::ZERO, b"a", Boundary::More)
        .expect("regular slot occupied");
    (critical.control().open(Id::new(3).expect("stream ID")))
        .expect("capacity fixture satisfies its declared invariants");
    assert!(matches!(
        critical
            .control()
            .open_encoder(Id::new(7).expect("stream ID")),
        Err(Error::Egress(o3::depot::queue::group::Error::Entries(
            o3::depot::capacity::Shortage {
                requested: NonZero::<usize>::MIN,
                available: 0,
            }
        )))
    ));
    assert!(
        (critical.events().poll_write())
            .expect("capacity fixture satisfies its declared invariants")
            .is_some()
    );
    (critical
        .control()
        .open_encoder(Id::new(7).expect("stream ID")))
    .expect("capacity fixture satisfies its declared invariants");
}

#[test]
fn headers_share_slot() {
    let mut conn = Connection::try_bounded().expect("capacity connection");
    (Receive::new(&mut conn).owned(
        Id::new(0).expect("stream ID"),
        Request(&[]).try_encode().expect("capacity headers"),
        Boundary::End,
    ))
    .expect("capacity fixture satisfies its declared invariants");
    headers(&mut conn, Id::new(0).expect("stream ID"), Boundary::End);
    assert!(
        Events::new(&mut conn)
            .poll_token()
            .expect("capacity event")
            .is_none()
    );
}

#[test]
fn coalesced_is_atomic() {
    let mut conn = Connection::try_bounded().expect("capacity connection");
    (Receive::new(&mut conn).owned(
        Id::new(0).expect("stream ID"),
        Request(&[b"body"]).try_encode().expect("capacity data"),
        Boundary::End,
    ))
    .expect("capacity fixture satisfies its declared invariants");
    headers(&mut conn, Id::new(0).expect("stream ID"), Boundary::More);
    assert!(
        matches!(Events::new(&mut conn).poll_token().expect("capacity event"), Some(Event::Data { message, data })
            if message.stream_id() == Id::new(0).expect("stream ID").into()
                && message.boundary() == Boundary::End
                && data.segments().flatten().copied().eq(b"body".iter().copied()))
    );
    assert!(
        Events::new(&mut conn)
            .poll_token()
            .expect("capacity event")
            .is_none()
    );
}

#[test]
fn step_yields_before_event() {
    let mut wire = Vec::new();
    Payload::encode(VarInt::from_u8(0x21), &[], &mut wire).expect("capacity frame");
    Payload::encode(VarInt::from_u8(0x22), &[], &mut wire).expect("capacity frame");
    wire.extend_from_slice(&Request(&[]).try_encode().expect("capacity headers"));

    let mut conn = Connection::try_bounded().expect("capacity connection");
    (Receive::new(&mut conn).owned(Id::new(0).expect("stream ID"), wire, Boundary::End))
        .expect("capacity fixture satisfies its declared invariants");

    assert!(matches!(
        conn.events().poll_token().expect("capacity step"),
        io::Step::More
    ));
    assert!(
        matches!(conn.events().poll_token().expect("capacity step"), io::Step::Item(Event::Headers { message, .. })
            if matches!(message.boundary(), Boundary::End))
    );
    assert!(matches!(
        conn.events().poll_token().expect("capacity step"),
        io::Step::Idle
    ));
}

#[test]
fn step_yields_before_idle() {
    let mut wire = Vec::new();
    STREAM_TYPE_CONTROL.encode(&mut wire).expect("varint");
    Payload::encode(TYPE_SETTINGS, &[], &mut wire).expect("capacity frame");
    Payload::encode(VarInt::from_u8(0x21), &[], &mut wire).expect("capacity frame");

    let mut conn = Connection::try_bounded().expect("capacity connection");
    (Receive::new(&mut conn).owned(Id::new(2).expect("stream ID"), wire, Boundary::More))
        .expect("capacity fixture satisfies its declared invariants");

    assert!(matches!(
        conn.events().poll_token().expect("capacity step"),
        io::Step::Item(Event::Settings(_))
    ));
    assert!(matches!(
        conn.events().poll_token().expect("capacity step"),
        io::Step::More
    ));
    assert!(matches!(
        conn.events().poll_credit().expect("capacity fixture satisfies its declared invariants"),
        Some((stream, 0, io::Resume::Ready)) if stream == Id::new(2).expect("stream ID")
    ));
    assert!(matches!(
        conn.events().poll_token().expect("capacity step"),
        io::Step::Idle
    ));
}

#[test]
fn ready_streams_rotate() {
    let mut conn = (Connection::with_config(
        Role::Server,
        Config {
            streams: Streams { capacity: 2 },
            ingress: Ingress {
                event_capacity: 1,
                ..Ingress::default()
            },
            ..Config::default()
        },
    ))
    .expect("capacity fixture satisfies its declared invariants");
    (Receive::new(&mut conn).owned(
        Id::new(0).expect("stream ID"),
        Request(&[b"a", b"b"]).try_encode().expect("capacity data"),
        Boundary::End,
    ))
    .expect("capacity fixture satisfies its declared invariants");
    (Receive::new(&mut conn).owned(
        Id::new(4).expect("stream ID"),
        Request(&[]).try_encode().expect("capacity headers"),
        Boundary::End,
    ))
    .expect("capacity fixture satisfies its declared invariants");

    headers(&mut conn, Id::new(0).expect("stream ID"), Boundary::More);
    assert!(
        matches!(Events::new(&mut conn).poll_token().expect("capacity event"), Some(Event::Data { message, data })
            if message.stream_id() == Id::new(0).expect("stream ID").into() && data.segments().flatten().copied().eq(b"a".iter().copied()))
    );
    headers(&mut conn, Id::new(4).expect("stream ID"), Boundary::End);
    assert!(
        matches!(Events::new(&mut conn).poll_token().expect("capacity event"), Some(Event::Data { message, data })
            if message.stream_id() == Id::new(0).expect("stream ID").into()
                && data.segments().flatten().copied().eq(b"b".iter().copied())
                && matches!(message.boundary(), Boundary::End))
    );
    assert!(
        Events::new(&mut conn)
            .poll_token()
            .expect("capacity event")
            .is_none()
    );
}

#[test]
fn segmentation_matches() {
    fn drain(conn: &mut Connection<'static>) -> Vec<(u8, Boundary, Vec<u8>)> {
        let mut events = Vec::new();
        while let Some(event) = Events::new(conn).poll_token().expect("capacity event") {
            match event {
                Event::Headers { message, .. } => {
                    events.push((0, message.boundary(), Vec::new()));
                }
                Event::Data { message, data } => {
                    events.push((
                        1,
                        message.boundary(),
                        data.segments().flatten().copied().collect::<Vec<_>>(),
                    ));
                }
                other => assert!(matches!(other, Event::Headers { .. } | Event::Data { .. })),
            }
        }
        events
    }

    let mut joined = Connection::try_bounded().expect("capacity connection");
    (Receive::new(&mut joined).owned(
        Id::new(0).expect("stream ID"),
        Request(&[b"same-wire"])
            .try_encode()
            .expect("capacity data"),
        Boundary::End,
    ))
    .expect("capacity fixture satisfies its declared invariants");
    let joined = drain(&mut joined);

    let mut split = Connection::try_bounded().expect("capacity connection");
    (Receive::new(&mut split).owned(
        Id::new(0).expect("stream ID"),
        Request(&[]).try_encode().expect("capacity headers"),
        Boundary::More,
    ))
    .expect("capacity fixture satisfies its declared invariants");
    let first = Events::new(&mut split)
        .poll_token()
        .expect("capacity event")
        .expect("fragmented headers event is present");
    let message = match first {
        Event::Headers { message, .. } => Some(message),
        _ => None,
    };
    let message = message.expect("fragmented request starts with headers");
    let mut events = vec![(0, message.boundary(), Vec::new())];
    let mut data = Vec::new();
    Payload::encode(TYPE_DATA, b"same-wire", &mut data).expect("capacity frame");
    (Receive::new(&mut split).owned(Id::new(0).expect("stream ID"), data, Boundary::End))
        .expect("capacity fixture satisfies its declared invariants");
    events.extend(drain(&mut split));

    assert!(
        joined == events,
        "fragmented input must preserve every event boundary"
    );
}

#[test]
fn push_is_atomic() {
    let mut block = Vec::new();
    let mut encoder = Encoder::new();
    (encoder.encode(3, [literal!(entry(b":status", b"200"))], &mut block))
        .expect("capacity fixture satisfies its declared invariants");
    let mut wire = Vec::new();
    STREAM_TYPE_PUSH.encode(&mut wire).expect("varint");
    VarInt::ZERO.encode(&mut wire).expect("varint");
    Payload::encode(TYPE_HEADERS, &block, &mut wire).expect("capacity frame");
    Payload::encode(TYPE_DATA, b"push-body", &mut wire).expect("capacity frame");

    let mut conn = plan::Config::new(Config::single())
        .expect("capacity configuration")
        .build::<RetainRaw, Authority>(Role::Client)
        .expect("capacity fixture satisfies its declared invariants");
    conn.allow_push(Role::Client, 0).expect("push grant");
    Send::new(&mut conn)
        .headers(Id::ZERO, Authority, Boundary::End)
        .expect("originating GET request");
    assert!(
        conn.events()
            .poll_write()
            .expect("request output")
            .is_some()
    );
    let mut origin = Vec::new();
    Payload::encode(TYPE_HEADERS, &block, &mut origin).expect("origin response");
    origin.extend(
        VarInt::ZERO
            .try_promise(&Authority.try_request().expect("promised GET"))
            .expect("push promise"),
    );
    Receive::new(&mut conn)
        .owned(Id::ZERO, origin, Boundary::End)
        .expect("finished origin and promise");
    assert!(matches!(
        Events::new(&mut conn)
            .poll_token()
            .expect("origin response"),
        Some(Event::Headers { .. })
    ));
    assert!(matches!(
        Events::new(&mut conn)
            .poll_token()
            .expect("accepted promise"),
        Some(Event::PushPromise { push_id: 0, .. })
    ));
    (Receive::new(&mut conn).owned(Id::new(3).expect("stream ID"), wire, Boundary::End))
        .expect("capacity fixture satisfies its declared invariants");

    assert!(
        matches!(Events::new(&mut conn).poll_token().expect("capacity event"), Some(Event::PushHeaders {
            message,
            push_id: 0,
            ..
        }) if message.stream_id() == Id::new(3).expect("stream ID").into() && message.boundary() == Boundary::More)
    );
    assert!(
        matches!(Events::new(&mut conn).poll_token().expect("capacity event"), Some(Event::PushData {
            message,
            push_id: 0,
            data,
        }) if message.stream_id() == Id::new(3).expect("stream ID").into()
            && matches!(message.boundary(), Boundary::End)
            && data.segments().flatten().copied().eq(b"push-body".iter().copied()))
    );
    assert!(
        Events::new(&mut conn)
            .poll_token()
            .expect("capacity event")
            .is_none()
    );
}

#[test]
fn resume_reports_error() {
    let mut wire = Request(&[]).try_encode().expect("capacity headers");
    Payload::encode(TYPE_SETTINGS, &[], &mut wire).expect("capacity frame");
    let mut conn = Connection::try_bounded().expect("capacity connection");

    (Receive::new(&mut conn).owned(Id::new(0).expect("stream ID"), wire, Boundary::End))
        .expect("capacity fixture satisfies its declared invariants");
    headers(&mut conn, Id::new(0).expect("stream ID"), Boundary::More);
    assert!(
        matches!(Events::new(&mut conn).poll_token(), Err(error) if error == Error::FrameUnexpected(Violation::Receive(Frame {
            stream_id: 0,
            kind: TYPE_SETTINGS.get(),
            role: Role::Server,
            site: Site::Request(Phase::Headers),
        })))
    );
}

#[test]
fn fin_uses_fallback() {
    let mut conn = Connection::try_bounded().expect("capacity connection");
    (Receive::new(&mut conn).owned(
        Id::new(0).expect("stream ID"),
        Request(&[]).try_encode().expect("capacity headers"),
        Boundary::More,
    ))
    .expect("capacity fixture satisfies its declared invariants");
    headers(&mut conn, Id::new(0).expect("stream ID"), Boundary::More);

    (Receive::new(&mut conn).borrowed(Id::new(0).expect("stream ID"), &[], Boundary::End))
        .expect("capacity fixture satisfies its declared invariants");
    assert!(
        matches!(Events::new(&mut conn).poll_token().expect("capacity event"), Some(Event::Finished { stream_id }) if stream_id == Id::new(0).expect("stream ID").into())
    );
    assert!(
        Events::new(&mut conn)
            .poll_token()
            .expect("capacity event")
            .is_none()
    );
}

#[test]
fn stream_slots_are_reused() {
    let mut conn = Connection::try_bounded().expect("capacity connection");
    (Receive::new(&mut conn).borrowed(Id::new(0).expect("stream ID"), &[], Boundary::More))
        .expect("capacity fixture satisfies its declared invariants");
    assert!(
        Receive::new(&mut conn).borrowed(Id::new(4).expect("stream ID"), &[], Boundary::More)
            == Err(Error::StreamCapacity {
                stream_id: 4,
                active: 1,
                capacity: 1,
            })
    );
    let result =
        Receive::new(&mut conn).reset(Id::new(0).expect("stream ID"), None, Code::from_u16(0));
    assert!(result.is_ok(), "reset must retire the resident stream");
    assert!(
        matches!(Events::new(&mut conn).poll_token().expect("capacity event"), Some(Event::Reset { stream_id, .. }) if stream_id == Id::new(0).expect("stream ID").into())
    );
    (Receive::new(&mut conn).borrowed(Id::new(4).expect("stream ID"), &[], Boundary::More))
        .expect("capacity fixture satisfies its declared invariants");
}

#[test]
fn critical_is_singleton() {
    let mut conn = Connection::try_bounded().expect("capacity connection");
    let mut kind = Vec::new();
    STREAM_TYPE_CONTROL.encode(&mut kind).expect("varint");
    (Receive::new(&mut conn).borrowed(Id::new(2).expect("stream ID"), &kind, Boundary::More))
        .expect("capacity fixture satisfies its declared invariants");
    let (error, charge) = Receive::new(&mut conn)
        .reset(Id::new(2).expect("stream ID"), None, Code::from_u16(0))
        .expect_err("critical stream cannot reset");
    assert!(error == Error::ClosedCriticalStream(2));
    assert!(charge.is_empty());
    assert!(
        Receive::new(&mut conn).borrowed(Id::new(6).expect("stream ID"), &kind, Boundary::More)
            == Err(Error::CriticalExists {
                kind: UniType::Control,
                existing: 2,
                attempted: 6,
            })
    );
}

fn headers(conn: &mut Connection<'_>, stream: Id, boundary: Boundary) {
    let event = Events::new(conn)
        .poll_token()
        .expect("capacity event")
        .expect("request headers event is present");
    assert!(matches!(&event, Event::Headers {
        message, section: Section::Initial, ..
    } if message.stream_id() == stream.into() && message.boundary() == boundary));
}
