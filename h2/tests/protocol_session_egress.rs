use common::{
    hpack::{Fields, GET},
    protocol::{
        Session,
        conn::Control,
        session::{Authority, Frame},
    },
};
use sark_h2::conn::event::Event;
use sark_http::field;

use crate::common::headers::entry;

pub mod common;

use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ClientRole, ServerRole,
    conn::{Error, machine::Connection},
    frame,
    frame::{layout::Header, numbers::ErrorCode},
    stream,
    stream::Id,
};

#[test]
fn reset_removes_stream() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");

    let block = Fields::new(&GET).try_encode().expect("protocol fixture");
    conn.ingress()
        .ingest(
            &Frame::new(1, &block)
                .try_headers(Boundary::More, frame::Block::End)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    assert!(
        conn.ingress().poll_event().is_some(),
        "scenario emits the expected event"
    );
    assert!(
        conn.status()
            .has_stream(Id::new(1).expect("fixture domain"))
    );

    conn.ingress()
        .ingest(
            &Control::new(1)
                .try_reset(ErrorCode::Cancel)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    let ev = conn
        .ingress()
        .poll_event()
        .expect("scenario emits the expected event");
    let (id, error) = match ev {
        Event::StreamReset {
            stream_id: id,
            error,
            ..
        } => Some((id, error)),
        _ => None,
    }
    .expect("scenario must emit StreamReset");
    assert!((id) == (Id::new(1).expect("fixture domain")));
    assert!((error) == (ErrorCode::Cancel));
    assert!(
        !conn
            .status()
            .has_stream(Id::new(1).expect("fixture domain"))
    );
}

#[test]
fn first_local_stream() {
    let mut conn = Connection::<ClientRole>::with_push::<Authority>().expect("client fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");

    let id = conn
        .streams()
        .headers()
        .request(
            [
                field::literal!(entry(b":method", b"GET")),
                field::literal!(entry(b":scheme", b"http")),
                field::literal!(entry(b":path", b"/")),
                field::literal!(entry(b":authority", b"x")),
            ],
            Boundary::End,
        )
        .expect("protocol scenario precondition");
    assert!((id) == (Id::new(1).expect("fixture domain")));
    let out = conn.output().first();
    let h = Header::parse(out).expect("protocol scenario precondition");
    assert!((h.kind) == (frame::numbers::Type::Headers));
    assert!((h.stream_id) == (Id::new(1).expect("fixture domain")));
    assert!(
        (conn
            .status()
            .stream_state(Id::new(1).expect("fixture domain")))
            == (Some(stream::State::HalfClosedLocal))
    );
}

#[test]
fn next_local_stream() {
    let mut conn = Connection::<ClientRole>::with_push::<Authority>().expect("client fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");

    let id1 = conn
        .streams()
        .headers()
        .request([field::literal!(entry(b":method", b"GET"))], Boundary::End)
        .expect("protocol scenario precondition");
    let id2 = conn
        .streams()
        .headers()
        .request([field::literal!(entry(b":method", b"GET"))], Boundary::End)
        .expect("protocol scenario precondition");
    assert!((id1) == (Id::new(1).expect("fixture domain")));
    assert!((id2) == (Id::new(3).expect("fixture domain")));
}

#[test]
fn response_emits_headers() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");

    let block = Fields::new(&GET).try_encode().expect("protocol fixture");
    conn.ingress()
        .ingest(
            &Frame::new(1, &block)
                .try_headers(Boundary::End, frame::Block::End)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    assert!(
        conn.ingress().poll_event().is_some(),
        "scenario emits the expected event"
    );
    conn.transfer().drain(usize::MAX);

    conn.streams()
        .headers()
        .respond(
            Id::new(1).expect("fixture domain"),
            [field::literal!(entry(b":status", b"200"))],
            Boundary::End,
        )
        .expect("protocol scenario precondition");
    let out = conn.output().first();
    let h = Header::parse(out).expect("protocol scenario precondition");
    assert!((h.kind) == (frame::numbers::Type::Headers));
    assert!((h.stream_id) == (Id::new(1).expect("fixture domain")));
    assert!(
        !conn
            .status()
            .has_stream(Id::new(1).expect("fixture domain"))
    );
}

#[test]
fn trailers_emit_final() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");

    conn.ingress()
        .ingest(
            &Frame::new(
                1,
                &Fields::new(&GET).try_encode().expect("protocol fixture"),
            )
            .try_headers(Boundary::End, frame::Block::End)
            .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    assert!(
        conn.ingress().poll_event().is_some(),
        "scenario emits the expected event"
    );
    conn.streams()
        .headers()
        .respond(
            Id::new(1).expect("fixture domain"),
            [field::literal!(entry(b":status", b"200"))],
            Boundary::More,
        )
        .expect("protocol scenario precondition");
    conn.transfer().drain(usize::MAX);

    conn.streams()
        .headers()
        .trail(
            Id::new(1).expect("fixture domain"),
            [field::literal!(entry(b"grpc-status", b"0"))],
        )
        .expect("protocol scenario precondition");

    let h = Header::parse(conn.output().first()).expect("protocol scenario precondition");
    assert!((h.kind) == (frame::numbers::Type::Headers));
    assert!((h.stream_id) == (Id::new(1).expect("fixture domain")));
    assert!(
        !conn
            .status()
            .has_stream(Id::new(1).expect("fixture domain"))
    );
}

#[test]
fn trailers_close_request() {
    let mut conn = Connection::<ClientRole>::with_push::<Authority>().expect("client fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");

    let id = conn
        .streams()
        .headers()
        .request(
            [
                field::literal!(entry(b":method", b"POST")),
                field::literal!(entry(b":scheme", b"http")),
                field::literal!(entry(b":path", b"/svc/method")),
                field::literal!(entry(b":authority", b"x")),
            ],
            Boundary::More,
        )
        .expect("protocol scenario precondition");
    conn.transfer().drain(usize::MAX);

    conn.streams()
        .headers()
        .trail(id, [field::literal!(entry(b"x-client-trailer", b"v"))])
        .expect("protocol scenario precondition");

    let h = Header::parse(conn.output().first()).expect("protocol scenario precondition");
    assert!((h.kind) == (frame::numbers::Type::Headers));
    assert!((h.stream_id) == (id));
    assert!((conn.status().stream_state(id)) == (Some(stream::State::HalfClosedLocal)));
}

#[test]
fn rejects_bad_trailers() {
    let mut conn = Connection::<ClientRole>::with_push::<Authority>().expect("client fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let id = conn
        .streams()
        .headers()
        .request(
            [
                field::literal!(entry(b":method", b"POST")),
                field::literal!(entry(b":scheme", b"http")),
                field::literal!(entry(b":path", b"/")),
            ],
            Boundary::End,
        )
        .expect("protocol scenario precondition");
    conn.transfer().drain(usize::MAX);

    assert!(
        conn.streams()
            .headers()
            .trail(id, [field::literal!(entry(b"x-trailer", b"v"))])
            == Err(Error::Transition(
                id,
                stream::Error::StreamClosed {
                    state: stream::State::HalfClosedLocal,
                    event: stream::Event::Headers {
                        boundary: Boundary::End,
                    },
                    side: stream::Side::Local,
                },
            ))
    );
    assert!(conn.output().first().is_empty());
    assert!((conn.status().stream_state(id)) == (Some(stream::State::HalfClosedLocal)));
}

#[test]
fn rejects_bad_response() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    conn.ingress()
        .ingest(
            &Frame::new(
                1,
                &Fields::new(&GET).try_encode().expect("protocol fixture"),
            )
            .try_headers(Boundary::More, frame::Block::End)
            .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    assert!(
        conn.ingress().poll_event().is_some(),
        "scenario emits the expected event"
    );
    conn.streams()
        .headers()
        .respond(
            Id::new(1).expect("fixture domain"),
            [field::literal!(entry(b":status", b"200"))],
            Boundary::End,
        )
        .expect("protocol scenario precondition");
    conn.transfer().drain(usize::MAX);

    assert!(
        conn.streams().headers().respond(
            Id::new(1).expect("fixture domain"),
            [field::literal!(entry(b":status", b"204"))],
            Boundary::More,
        ) == Err(Error::Transition(
            Id::new(1).expect("fixture domain"),
            stream::Error::StreamClosed {
                state: stream::State::HalfClosedLocal,
                event: stream::Event::Headers {
                    boundary: Boundary::More,
                },
                side: stream::Side::Local,
            },
        ))
    );
    assert!(conn.output().first().is_empty());
    assert!(
        (conn
            .status()
            .stream_state(Id::new(1).expect("fixture domain")))
            == (Some(stream::State::HalfClosedLocal))
    );
}
