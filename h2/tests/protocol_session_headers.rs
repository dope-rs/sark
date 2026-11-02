use common::{
    headers::Authority,
    hpack::{Fields, GET},
    protocol::{
        Session,
        conn::Controls,
        session::{Frame, OK_RESPONSE},
    },
};
use sark_h2::conn::{Continuation, event::Event};
pub mod common;

use sark_core::manifold::{
    client::event::{self, Classify as _},
    stream::{Boundary, Section},
};
use sark_h2::{
    ClientRole, ServerRole,
    conn::{
        Error, Protocol,
        config::{Config, Settings},
        machine::Connection,
    },
    frame,
    frame::{Ack, numbers::ErrorCode},
    stream,
    stream::Id,
};
use sark_http::field::metadata::map::record::Entry;

#[test]
fn rejects_unpromised() {
    for limit in [None, Some(0)] {
        for chain in [frame::Block::End, frame::Block::More] {
            let mut conn = Connection::<ClientRole>::with_config(Config {
                local_settings: Settings {
                    max_concurrent_streams: limit,
                    ..Settings::DEFAULT
                },
                ..Config::default()
            })
            .expect("client fixture");
            Session::new(&mut conn).try_prime().expect("primed client");
            let error = conn
                .ingress()
                .ingest(
                    &Frame::new(2, OK_RESPONSE)
                        .try_headers(Boundary::End, chain)
                        .expect("response headers"),
                )
                .expect_err("response requires an admitted stream");
            assert_eq!(error, Error::Protocol(Protocol::Idle(Id::FIRST_SERVER)));
            assert_eq!(error.kind(), event::Error::Protocol);
            assert_eq!(ErrorCode::from(&error), ErrorCode::ProtocolError);
            assert!(!error.is_pressure());
            assert_eq!(conn.status().active_len(), 0);
            assert!(conn.ingress().poll_event().is_none());
        }
    }
}

#[test]
fn accepts_promised() {
    for chain in [frame::Block::End, frame::Block::More] {
        let mut conn = Connection::<ClientRole>::with_push::<Authority>().expect("client fixture");
        Session::new(&mut conn).try_prime().expect("primed client");
        Session::new(&mut conn).try_start().expect("local request");
        let block = Fields::new(&GET).try_encode().expect("promised request");
        conn.ingress()
            .ingest(
                &Frame::new(1, &block)
                    .try_push(2, frame::Block::End)
                    .expect("push promise"),
            )
            .expect("admitted promise");
        assert!(matches!(
            conn.ingress().poll_event(),
            Some(Event::PushPromise { .. })
        ));
        conn.ingress()
            .ingest(
                &Frame::new(2, OK_RESPONSE)
                    .try_headers(Boundary::End, chain)
                    .expect("promised response"),
            )
            .expect("admitted response");
        if matches!(chain, frame::Block::More) {
            conn.ingress()
                .ingest(
                    &Frame::new(2, &[])
                        .try_continue(frame::Block::End)
                        .expect("response continuation"),
                )
                .expect("completed response");
        }
        assert!(matches!(
            conn.ingress().poll_event(),
            Some(Event::Headers {
                stream_id,
                boundary: Boundary::End,
                section: Section::Initial,
                ..
            }) if stream_id.id() == Id::FIRST_SERVER
        ));
    }
}

#[test]
fn headers_self_dependency() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    assert_eq!(
        conn.ingress()
            .ingest(common::protocol::session::SELF_DEPENDENT_HEADERS),
        Err(Error::ParseError(frame::numbers::ParseError::Dependency {
            stream: Id::FIRST_CLIENT,
            dependency: Id::FIRST_CLIENT,
        }))
    );
}

#[test]
fn headers_open_stream() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");

    let block = Fields::new(&GET).try_encode().expect("protocol fixture");
    let frame = Frame::new(1, &block)
        .try_headers(Boundary::More, frame::Block::End)
        .expect("protocol fixture");
    conn.ingress()
        .ingest(&frame)
        .expect("protocol scenario precondition");

    let ev = conn
        .ingress()
        .poll_event()
        .expect("scenario emits the expected event");
    let (id, headers, end, section) = match ev {
        Event::Headers {
            stream_id: id,
            head,
            boundary,
            section,
            ..
        } => Some((id, head.into_parts().0, boundary, section)),
        _ => None,
    }
    .expect("scenario must emit Headers");
    assert!((id) == (Id::new(1).expect("fixture domain")));
    assert!(matches!(end, Boundary::More));
    assert!(matches!(section, Section::Initial));
    assert!(headers.iter().any(|h| h.name == b":method"));
    assert!(
        (conn
            .status()
            .stream_state(Id::new(1).expect("fixture domain")))
            == (Some(stream::State::Open))
    );
}

#[test]
fn headers_close_remote() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");

    let block = Fields::new(&GET).try_encode().expect("protocol fixture");
    let frame = Frame::new(1, &block)
        .try_headers(Boundary::End, frame::Block::End)
        .expect("protocol fixture");
    conn.ingress()
        .ingest(&frame)
        .expect("protocol scenario precondition");
    assert!(
        conn.ingress().poll_event().is_some(),
        "scenario emits the expected event"
    );
    assert!(
        (conn
            .status()
            .stream_state(Id::new(1).expect("fixture domain")))
            == (Some(stream::State::HalfClosedRemote))
    );
}

#[test]
fn continuation_assembles() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");

    let block = Fields::new(&GET).try_encode().expect("protocol fixture");
    let split_a = &block[..2];
    let split_b = &block[2..4];
    let split_c = &block[4..];

    conn.ingress()
        .ingest(
            &Frame::new(1, split_a)
                .try_headers(Boundary::More, frame::Block::More)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    assert!(conn.ingress().poll_event().is_none());
    conn.ingress()
        .ingest(
            &Frame::new(1, split_b)
                .try_continue(frame::Block::More)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    assert!(conn.ingress().poll_event().is_none());
    conn.ingress()
        .ingest(
            &Frame::new(1, split_c)
                .try_continue(frame::Block::End)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    let ev = conn
        .ingress()
        .poll_event()
        .expect("scenario emits the expected event");
    let (id, headers) = match ev {
        Event::Headers {
            stream_id: id,
            head,
            ..
        } => Some((id, head.into_parts().0)),
        _ => None,
    }
    .expect("scenario must emit Headers");
    assert!((id) == (Id::new(1).expect("fixture domain")));
    assert!(headers.iter().any(|h| h.name == b":method"));
}

#[test]
fn fused_owners_match() {
    let block = Fields::new(&[
        Entry::new(b":method", b"GET"),
        Entry::new(b":scheme", b"https"),
        Entry::new(b":path", b"/segmented/cursor"),
        Entry::new(
            b"x-segmented",
            b"a-value-crossing-many-continuation-and-owner-boundaries",
        ),
    ])
    .try_encode()
    .expect("protocol fixture");
    let chunks: Vec<_> = block.chunks(3).collect();
    let mut frames = Vec::with_capacity(chunks.len());
    let chain = if chunks.len() == 1 {
        frame::Block::End
    } else {
        frame::Block::More
    };
    frames.push(
        Frame::new(1, chunks[0])
            .try_headers(Boundary::End, chain)
            .expect("protocol fixture"),
    );
    for (index, chunk) in chunks[1..].iter().enumerate() {
        let chain = if index + 2 == chunks.len() {
            frame::Block::End
        } else {
            frame::Block::More
        };
        frames.push(
            Frame::new(1, chunk)
                .try_continue(chain)
                .expect("protocol fixture"),
        );
    }

    let decode = |fused: bool| {
        let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
        Session::new(&mut conn)
            .try_prime()
            .expect("protocol fixture");
        if fused {
            let mut wire = Vec::new();
            for frame in &frames {
                wire.extend_from_slice(frame);
            }
            conn.ingress()
                .ingest(&wire)
                .expect("protocol scenario precondition");
        } else {
            for frame in &frames {
                conn.ingress()
                    .ingest(frame)
                    .expect("protocol scenario precondition");
            }
        }
        let headers = match conn
            .ingress()
            .poll_event()
            .expect("segmented header block must complete as Headers")
        {
            Event::Headers { head, .. } => Some(head.into_parts().0),
            _ => None,
        }
        .expect("segmented header block must complete as Headers");
        headers
            .iter()
            .map(|field| (field.name.to_vec(), field.value.to_vec()))
            .collect::<Vec<_>>()
    };

    let split = decode(false);
    let fused = decode(true);
    assert_eq!(fused, split);
    assert!(fused.iter().any(|(name, value)| {
        name == b"x-segmented"
            && value == b"a-value-crossing-many-continuation-and-owner-boundaries"
    }));
}

#[test]
fn all_hpack_splits() {
    let block = Fields::new(&[
        Entry::new(b":method", b"GET"),
        Entry::new(b":scheme", b"https"),
        Entry::new(b":path", b"/fragmented"),
        Entry::new(b"x-streaming", b"abcdefghijklmnopqrstuvwxyz"),
    ])
    .try_encode()
    .expect("protocol fixture");

    for split in 0..=block.len() {
        let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
        Session::new(&mut conn)
            .try_prime()
            .expect("protocol fixture");
        conn.ingress()
            .ingest(
                &Frame::new(1, &block[..split])
                    .try_headers(Boundary::More, frame::Block::More)
                    .expect("protocol fixture"),
            )
            .expect("protocol scenario precondition");
        assert!(conn.ingress().poll_event().is_none());
        conn.ingress()
            .ingest(
                &Frame::new(1, &block[split..])
                    .try_continue(frame::Block::End)
                    .expect("protocol fixture"),
            )
            .expect("protocol scenario precondition");
        let headers = match conn
            .ingress()
            .poll_event()
            .expect("each HPACK boundary must emit Headers")
        {
            Event::Headers { head, .. } => Some(head.into_parts().0),
            _ => None,
        }
        .expect("each HPACK boundary must emit Headers");
        assert!(headers.iter().any(|field| {
            field.name == b"x-streaming" && field.value == b"abcdefghijklmnopqrstuvwxyz"
        }));
    }
}

#[test]
fn all_receive_splits() {
    let block = Fields::new(&[
        Entry::new(b":method", b"GET"),
        Entry::new(b":scheme", b"https"),
        Entry::new(b":path", b"/owners"),
    ])
    .try_encode()
    .expect("protocol fixture");
    let frame = Frame::new(1, &block)
        .try_headers(Boundary::More, frame::Block::End)
        .expect("protocol fixture");

    for split in 0..=frame.len() {
        let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
        Session::new(&mut conn)
            .try_prime()
            .expect("protocol fixture");
        conn.ingress()
            .ingest(&frame[..split])
            .expect("protocol scenario precondition");
        conn.ingress()
            .ingest(&frame[split..])
            .expect("protocol scenario precondition");
        assert!(matches!(
            conn.ingress().poll_event(),
            Some(Event::Headers { .. })
        ));
    }
}

#[test]
fn rejects_foreign_frame() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");

    let block = Fields::new(&GET).try_encode().expect("protocol fixture");
    conn.ingress()
        .ingest(
            &Frame::new(1, &block[..1])
                .try_headers(Boundary::More, frame::Block::More)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    assert!(matches!(
        conn.ingress().ingest(
            &Controls::new(Ack::Required)
                .try_ping([0u8; 8])
                .expect("ping fixture")
        ),
        Err(Error::Continuation(Continuation::Type(6)))
    ));
}

#[test]
fn rejects_interleaved_data() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");

    let block = Fields::new(&GET).try_encode().expect("protocol fixture");
    conn.ingress()
        .ingest(
            &Frame::new(1, &block[..1])
                .try_headers(Boundary::More, frame::Block::More)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    assert!(matches!(
        conn.ingress().ingest(
            &Frame::new(1, b"x")
                .try_data(Boundary::More)
                .expect("protocol fixture")
        ),
        Err(Error::Continuation(Continuation::Type(0)))
    ));
}

#[test]
fn rejects_wrong_stream() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");

    let block = Fields::new(&GET).try_encode().expect("protocol fixture");
    conn.ingress()
        .ingest(
            &Frame::new(1, &block[..1])
                .try_headers(Boundary::More, frame::Block::More)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    assert!(matches!(
        conn.ingress()
            .ingest(&Frame::new(3, &block[1..]).try_continue(frame::Block::End).expect("protocol fixture")),
        Err(Error::Continuation(Continuation::Stream {
            expected,
            actual,
        })) if expected == Id::new(1).expect("fixture domain") && actual == Id::new(3).expect("fixture domain")
    ));
}
