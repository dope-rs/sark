pub mod common;

use std::{
    slice,
    time::{Duration, Instant},
};

use common::{
    conn::{
        Events,
        bridge::Pair,
        build::{Fields as _, Settings as _},
        payload::Prefix as _,
        wire::{Ingress, Peer},
    },
    field::entry,
    message::{Frame as _, capacity::Request},
};
use dope_quic::{
    errors::Code,
    wire::{stream::Id, varint::VarInt},
};
use o3::buffer::{resident, storage};
use sark_core::manifold::stream::{Boundary, Section};
use sark_h3::{
    conn::{
        Connection, Error,
        event::{Completion, Event, Role},
        io::{Receive, Resume, Step},
        options,
    },
    frame::{self, TYPE_DATA},
    link::{Handoff, Status},
    manifold::Session,
};
use sark_http::{
    field::{literal, metadata::block::ContentError},
    payload::response::Body as _,
};

#[test]
fn data_releases_fragments() {
    let mut config = options::Config::default();
    config.ingress.retained_bytes = 1 << 20;
    config.ingress.event_capacity = 1;
    let mut conn = Connection::with_config(Role::Server, config).expect("connection");
    Receive::new(&mut conn)
        .owned(
            Id::ZERO,
            [
                literal!(entry(b":method", b"POST")),
                literal!(entry(b":scheme", b"https")),
                literal!(entry(b":authority", b"example.test")),
                literal!(entry(b":path", b"/")),
                literal!(entry(b"content-length", b"2097152")),
            ]
            .try_headers()
            .expect("request"),
            Boundary::More,
        )
        .expect("headers");
    assert!(matches!(
        Events::new(&mut conn).poll_token().expect("headers"),
        Some(Event::Headers { .. })
    ));

    let header = TYPE_DATA
        .try_prefix(VarInt::new(2 << 20).expect("DATA length"))
        .expect("DATA header");
    for (index, byte) in header.iter().enumerate() {
        let bytes = resident::Shared::from(
            storage::Shared::try_copy(slice::from_ref(byte)).expect("header owner"),
        );
        assert!(matches!(
            Receive::new(&mut conn).hold(Id::ZERO, bytes, Boundary::More),
            Handoff::Accepted(Ok(Status::Open))
        ));
        let expected =
            (index + 1 == header.len()).then_some((Id::ZERO, header.len(), Resume::Idle));
        assert!(conn.events().poll_credit().expect("header credit") == expected);
        assert!(
            Events::new(&mut conn)
                .poll_token()
                .expect("no empty DATA")
                .is_none()
        );
    }

    for boundary in [Boundary::More, Boundary::End] {
        if boundary == Boundary::End {
            Receive::new(&mut conn)
                .stop(Id::new(4).expect("other stream"), None, Code::from_u16(0))
                .expect("fill event queue");
        }
        let mut owner = vec![b'x'; 1 << 20];
        let pointer = owner.as_ptr();
        let bytes = resident::Shared::from(storage::Shared::try_from(&mut owner).expect("owner"));
        let expected = if boundary == Boundary::End {
            Status::Suspended
        } else {
            Status::Open
        };
        assert!(
            matches!(Receive::new(&mut conn).hold(Id::ZERO, bytes, boundary),
            Handoff::Accepted(Ok(status)) if status == expected)
        );
        if boundary == Boundary::End {
            assert!(
                conn.events()
                    .poll_credit()
                    .expect("parked credit")
                    .is_none()
            );
            assert!(matches!(
                Events::new(&mut conn).poll_token().expect("queue vacancy"),
                Some(Event::Stopped { .. })
            ));
        }
        let event = Events::new(&mut conn)
            .poll_token()
            .expect("chunk")
            .expect("DATA");
        let (message, data) = match event {
            Event::Data { message, data } => Some((message, data)),
            _ => None,
        }
        .expect("DATA");
        assert_eq!(data.len(), 1 << 20);
        assert_eq!(
            data.segments().next().expect("original owner").as_ptr(),
            pointer
        );
        assert!(message.boundary() == boundary);
        assert!(
            conn.events().poll_credit().expect("chunk credit")
                == Some((Id::ZERO, 1 << 20, Resume::Idle))
        );
        assert!(
            conn.events()
                .poll_credit()
                .expect("settled credit")
                .is_none()
        );
    }
    assert!(
        Events::new(&mut conn)
            .poll_token()
            .expect("one completion")
            .is_none()
    );
}

#[test]
fn content_length_rejects_inconsistent_body() {
    let trailers = [literal!(entry(b"x-trailer", b"done"))]
        .try_headers()
        .expect("trailers");
    for (declared, data, tail, boundary, error) in [
        (
            &b"1"[..],
            &b"xx"[..],
            None,
            Boundary::More,
            ContentError::Exceeded {
                remaining: 1,
                actual: 2,
            },
        ),
        (
            &b"2"[..],
            &b"x"[..],
            Some(&[][..]),
            Boundary::End,
            ContentError::Incomplete { remaining: 1 },
        ),
        (
            &b"2"[..],
            &b"x"[..],
            Some(trailers.as_slice()),
            Boundary::More,
            ContentError::Incomplete { remaining: 1 },
        ),
    ] {
        let mut config = options::Config::default();
        config.ingress.event_capacity = 1;
        let mut conn = Connection::with_config(Role::Server, config).expect("connection");
        Receive::new(&mut conn)
            .owned(
                Id::ZERO,
                [
                    literal!(entry(b":method", b"POST")),
                    literal!(entry(b":scheme", b"https")),
                    literal!(entry(b":authority", b"example.test")),
                    literal!(entry(b":path", b"/")),
                    entry(b"content-length", declared).expect("length"),
                ]
                .try_headers()
                .expect("request"),
                Boundary::More,
            )
            .expect("headers");
        assert!(matches!(
            Events::new(&mut conn).poll_token().expect("headers"),
            Some(Event::Headers { .. })
        ));
        if tail.is_some() {
            Receive::new(&mut conn)
                .owned(
                    Id::ZERO,
                    data.try_frame(TYPE_DATA).expect("DATA"),
                    Boundary::More,
                )
                .expect("body prefix");
            assert!(matches!(
                Events::new(&mut conn).poll_token().expect("body prefix"),
                Some(Event::Data { data: body, .. }) if body.len() == data.len()
            ));
        }
        Receive::new(&mut conn)
            .stop(Id::new(4).expect("other stream"), None, Code::from_u16(0))
            .expect("fill event queue");
        match tail {
            Some(tail) => Receive::new(&mut conn).borrowed(Id::ZERO, tail, boundary),
            None => Receive::new(&mut conn).owned(
                Id::ZERO,
                data.try_frame(TYPE_DATA).expect("DATA"),
                Boundary::More,
            ),
        }
        .expect("park until rejection can be published");
        assert!(matches!(
            Events::new(&mut conn).poll_token().expect("queue vacancy"),
            Some(Event::Stopped { .. })
        ));
        assert!(matches!(
            Events::new(&mut conn).poll_token().expect("stream rejection"),
            Some(Event::Rejected { stream_id, error: actual })
                if stream_id == Id::ZERO.into() && actual == error
        ));
        assert!(Receive::new(&mut conn).is_blocked(Id::ZERO));
        assert!(
            Events::new(&mut conn)
                .poll_token()
                .expect("no invalid payload or completion")
                .is_none()
        );
    }
}

#[test]
fn unknown_releases_chunks() {
    for id in [Id::ZERO, Id::new(2).expect("control stream")] {
        let mut config = options::Config::default();
        config.ingress.retained_bytes = 64;
        let mut conn = Connection::with_config(Role::Server, config).expect("connection");
        let initial = if id == Id::ZERO {
            Request(&[]).try_encode().expect("request")
        } else {
            frame::Settings::default().try_wire().expect("settings")
        };
        Receive::new(&mut conn)
            .owned(id, initial, Boundary::More)
            .expect("initial frame");
        assert!(
            Events::new(&mut conn)
                .poll_token()
                .expect("initial event")
                .is_some()
        );
        let header = VarInt::from_u8(0x21)
            .try_prefix(VarInt::new(1024).expect("unknown length"))
            .expect("unknown header");
        Receive::new(&mut conn)
            .owned(id, header, Boundary::More)
            .expect("unknown header");
        for _ in 0..16 {
            let bytes =
                resident::Shared::from(storage::Shared::try_copy(&[0x06; 64]).expect("owner"));
            assert!(matches!(
                Receive::new(&mut conn).hold(id, bytes, Boundary::More),
                Handoff::Accepted(Ok(Status::Open))
            ));
            assert!(
                conn.events().poll_credit().expect("unknown credit")
                    == Some((id, 64, Resume::Idle))
            );
        }
        assert!(
            conn.events()
                .poll_credit()
                .expect("settled credit")
                .is_none()
        );
        assert!(
            Events::new(&mut conn)
                .poll_token()
                .expect("no unknown event")
                .is_none()
        );
        if id == Id::ZERO {
            Receive::new(&mut conn)
                .owned(
                    id,
                    b"body".try_frame(TYPE_DATA).expect("body"),
                    Boundary::End,
                )
                .expect("next frame");
            assert!(matches!(Events::new(&mut conn).poll_token().expect("body"),
                Some(Event::Data { message, data }) if data.len() == 4 && message.boundary() == Boundary::End));
        } else {
            assert_eq!(
                Receive::new(&mut conn).borrowed(id, &[], Boundary::End),
                Err(Error::ClosedCriticalStream(id.get()))
            );
        }
    }
}

#[test]
fn fin_rejects_remainder() {
    for kind in [TYPE_DATA, VarInt::from_u8(0x21)] {
        for tail in [false, true] {
            let mut conn = Connection::with_role(Role::Server).expect("connection");
            Receive::new(&mut conn)
                .owned(
                    Id::ZERO,
                    Request(&[]).try_encode().expect("request"),
                    Boundary::More,
                )
                .expect("headers");
            let _ = Events::new(&mut conn).poll_token().expect("headers");
            let header = kind.try_prefix(VarInt::from_u8(10)).expect("header");
            Receive::new(&mut conn)
                .owned(Id::ZERO, header, Boundary::More)
                .expect("admitted header");
            let bytes: &[u8] = if tail { b"x" } else { b"" };
            assert_eq!(
                Receive::new(&mut conn).borrowed(Id::ZERO, bytes, Boundary::End),
                Err(Error::Incomplete {
                    stream_id: 0,
                    buffered: bytes.len(),
                    cause: frame::ParseError::NeedMore
                })
            );
            assert!(
                Events::new(&mut conn)
                    .poll_token()
                    .expect("no premature completion")
                    .is_none()
            );
        }
    }
}

#[test]
fn data_stops_at_trailers() {
    let mut conn = Connection::with_role(Role::Server).expect("connection");
    Receive::new(&mut conn)
        .owned(
            Id::ZERO,
            Request(&[]).try_encode().expect("request"),
            Boundary::More,
        )
        .expect("headers");
    let _ = Events::new(&mut conn).poll_token().expect("headers");
    let mut first = TYPE_DATA.try_prefix(VarInt::from_u8(4)).expect("header");
    first.extend_from_slice(b"ab");
    Receive::new(&mut conn)
        .owned(Id::ZERO, first, Boundary::More)
        .expect("first fragment");
    assert!(
        matches!(Events::new(&mut conn).poll_token().expect("first DATA"),
        Some(Event::Data { message, data }) if data.len() == 2 && message.boundary() == Boundary::More)
    );
    let mut last = b"cd".to_vec();
    last.extend(
        [literal!(entry(b"x-trailer", b"done"))]
            .try_headers()
            .expect("trailers"),
    );
    Receive::new(&mut conn)
        .owned(Id::ZERO, last, Boundary::End)
        .expect("DATA and trailers");
    assert!(
        matches!(Events::new(&mut conn).poll_token().expect("last DATA"),
        Some(Event::Data { message, data }) if data.segments().flatten().copied().eq(b"cd".iter().copied()) && message.boundary() == Boundary::More)
    );
    assert!(
        matches!(Events::new(&mut conn).poll_token().expect("trailers"),
        Some(Event::Headers { message, section: Section::Trailing, .. }) if message.boundary() == Boundary::End)
    );
    assert!(
        Events::new(&mut conn)
            .poll_token()
            .expect("one completion")
            .is_none()
    );
}

#[test]
fn rejects_oversized_header() {
    for kind in [TYPE_DATA, VarInt::from_u8(0x21)] {
        let mut config = options::Config::default();
        config.protocol.max_frame_bytes = 64;
        let mut conn = Connection::with_config(Role::Server, config).expect("connection");
        let header = kind.try_prefix(VarInt::from_u8(65)).expect("header");
        assert_eq!(
            Receive::new(&mut conn).owned(Id::ZERO, header, Boundary::More),
            Err(Error::Parse(frame::ParseError::FrameTooLarge))
        );
    }
}

#[test]
fn data_spans_quic_window() {
    let Pair {
        mut server,
        mut client,
        mut space,
    } = Pair::try_new(|_| {}).expect("QUIC pair");
    let mut h3 = Session::with_role(Role::Server).expect("H3 session");
    let mut request = Request(&[]).try_encode().expect("request");
    frame::Payload::encode(TYPE_DATA, &vec![b'x'; 2 << 20], &mut request)
        .expect("large DATA frame");
    client.with_scope(|mut scope| {
        let mut streams = scope.streams();
        let stream = streams.send().open_bidi().expect("request stream");
        streams
            .send()
            .send(stream.send(), &request)
            .expect("request bytes");
        streams.send().finish(stream.send()).expect("request FIN");
    });
    let mut received = 0;
    let mut chunks = 0;
    let mut finished = false;
    let mut now = Instant::now();
    for _ in 0..4096 {
        now = client
            .view()
            .recovery()
            .send_at(now + Duration::from_millis(1));
        Peer::new(&mut space, &mut client)
            .try_send(&mut server, now, |_| {})
            .expect("client transfer");
        Ingress::new(&mut h3)
            .try_receive(&mut server)
            .expect("H3 ingress");
        server.with_scope(|mut scope| {
            while let Step::Item(()) | Step::More = h3
                .ingress()
                .consume(&mut scope, |event| {
                    if let Event::Data { data, .. } = event {
                        assert!(data.segments().flatten().all(|byte| *byte == b'x'));
                        received += data.len();
                        chunks += 1;
                    }
                    if matches!(event.completion(), Completion::End(_)) {
                        assert!(!finished);
                        finished = true;
                    }
                })
                .expect("consume H3 event")
            {}
        });
        if finished {
            break;
        }
        now = server
            .view()
            .recovery()
            .send_at(now + Duration::from_millis(1));
        Peer::new(&mut space, &mut server)
            .try_send(&mut client, now, |_| {})
            .expect("credit transfer");
    }
    assert!(finished);
    assert_eq!(received, 2 << 20);
    assert!(chunks > 1);
}
