use common::{
    hpack::{Fields, GET},
    protocol::{
        Session,
        session::{Frame, Wire},
    },
};
use sark_h2::{Endpoint, conn::event::Event};

use crate::common::headers::entry;
pub mod common;

use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ClientRole,
    conn::{Association, Error, Protocol, config, config::Config, machine::Connection},
    frame,
    frame::numbers::ErrorCode,
    stream,
    stream::Id,
};
use sark_http::field::{self, metadata::map::record::Entry};

use crate::common::protocol::session::Authority;

#[test]
fn rejects_early_followup() {
    let mut conn = Connection::<ClientRole>::with_push::<Authority>().expect("client fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let block = Fields::new(&GET).try_encode().expect("protocol fixture");

    assert!(matches!(
        conn.ingress()
            .ingest(&Frame::new(1, &block[..1]).try_push(2, frame::Block::More).expect("protocol fixture")),
        Err(Error::Protocol(Protocol::Association(Association::Idle(id)))) if id == Id::new(1).expect("fixture domain")
    ));
    assert!(
        !conn
            .status()
            .has_stream(Id::new(2).expect("fixture domain"))
    );
    assert!(conn.ingress().poll_event().is_none());
}

#[test]
fn rejects_nested_push() {
    let mut conn = Connection::<ClientRole>::with_push::<Authority>().expect("client fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    conn.streams()
        .headers()
        .request([field::literal!(entry(b":method", b"GET"))], Boundary::End)
        .expect("protocol scenario precondition");
    conn.transfer().drain(usize::MAX);

    conn.ingress()
        .ingest(
            &Frame::new(
                1,
                &Fields::new(&GET).try_encode().expect("protocol fixture"),
            )
            .try_push(2, frame::Block::End)
            .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    assert!(
        conn.ingress().poll_event().is_some(),
        "scenario emits the expected event"
    );
    let response = Fields::new(&[Entry::new(b":status", b"200")])
        .try_encode()
        .expect("protocol fixture");
    conn.ingress()
        .ingest(
            &Frame::new(2, &response)
                .try_headers(Boundary::More, frame::Block::End)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    assert!(
        conn.ingress().poll_event().is_some(),
        "scenario emits the expected event"
    );
    assert!(
        (conn
            .status()
            .stream_state(Id::new(2).expect("fixture domain")))
            == (Some(stream::State::HalfClosedLocal))
    );

    assert!(matches!(
        conn.ingress()
            .ingest(&Frame::new(2, &Fields::new(&GET).try_encode().expect("protocol fixture")).try_push(4, frame::Block::End).expect("protocol fixture")),
        Err(Error::Protocol(Protocol::Association(Association::Peer(id)))) if id == Id::new(2).expect("fixture domain")
    ));
    assert!(
        !conn
            .status()
            .has_stream(Id::new(4).expect("fixture domain"))
    );
}

#[test]
fn rejects_after_remote() {
    let mut conn = Connection::<ClientRole>::with_push::<Authority>().expect("client fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    conn.streams()
        .headers()
        .request([field::literal!(entry(b":method", b"GET"))], Boundary::More)
        .expect("protocol scenario precondition");
    conn.transfer().drain(usize::MAX);
    let response = Fields::new(&[Entry::new(b":status", b"200")])
        .try_encode()
        .expect("protocol fixture");
    conn.ingress()
        .ingest(
            &Frame::new(1, &response)
                .try_headers(Boundary::End, frame::Block::End)
                .expect("protocol fixture"),
        )
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

    assert!(matches!(
        conn.ingress()
            .ingest(&Frame::new(1, &Fields::new(&GET).try_encode().expect("protocol fixture")).try_push(2, frame::Block::End).expect("protocol fixture")),
        Err(Error::Protocol(Protocol::Association(Association::State {
            stream: id,
            state: stream::State::HalfClosedRemote,
        }))) if id == Id::new(1).expect("fixture domain")
    ));
    assert!(
        !conn
            .status()
            .has_stream(Id::new(2).expect("fixture domain"))
    );
}

#[test]
fn accepts_push_after_close() {
    let mut conn = Connection::<ClientRole>::with_push::<Authority>().expect("client fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    conn.streams()
        .headers()
        .request([field::literal!(entry(b":method", b"GET"))], Boundary::End)
        .expect("protocol scenario precondition");
    conn.transfer().drain(usize::MAX);
    let response = Fields::new(&[Entry::new(b":status", b"200")])
        .try_encode()
        .expect("protocol fixture");
    conn.ingress()
        .ingest(
            &Frame::new(1, &response)
                .try_headers(Boundary::End, frame::Block::End)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    assert!(
        conn.ingress().poll_event().is_some(),
        "scenario emits the expected event"
    );
    assert!(
        !conn
            .status()
            .has_stream(Id::new(1).expect("fixture domain"))
    );

    conn.ingress()
        .ingest(
            &Frame::new(
                1,
                &Fields::new(&GET).try_encode().expect("protocol fixture"),
            )
            .try_push(2, frame::Block::End)
            .expect("protocol fixture"),
        )
        .expect("closed parent admits a new promised stream");
    assert!(
        matches!(conn.ingress().poll_event(), Some(Event::PushPromise {
            stream_id,
            promised_stream_id,
            ..
        }) if stream_id == Id::FIRST_CLIENT && promised_stream_id == Id::FIRST_SERVER)
    );
    assert_eq!(
        conn.status().stream_state(Id::FIRST_SERVER),
        Some(stream::State::ReservedRemote)
    );
}

#[test]
fn accepts_push_after_many_resets() {
    let mut conn = Connection::<ClientRole>::with_push::<Authority>().expect("client fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    for _ in 0..257 {
        let id = conn
            .streams()
            .headers()
            .request([field::literal!(entry(b":method", b"GET"))], Boundary::More)
            .expect("local request");
        let key = conn.status().stream_key(id).expect("active fixture stream");
        conn.streams()
            .try_cancel(key, ErrorCode::Cancel)
            .expect("active stream cancellation");
        conn.transfer().drain(usize::MAX);
    }
    assert_eq!(conn.status().active_len(), 0);

    let push = Frame::new(
        1,
        &Fields::new(&GET).try_encode().expect("protocol fixture"),
    )
    .try_push(2, frame::Block::End)
    .expect("protocol fixture");
    conn.ingress()
        .ingest(&push)
        .expect("old closed parent admits a new promised stream");

    assert!(
        matches!(conn.ingress().poll_event(), Some(Event::PushPromise {
            stream_id,
            promised_stream_id,
            ..
        }) if stream_id == Id::new(1).expect("fixture domain") && promised_stream_id == Id::new(2).expect("fixture domain"))
    );
    assert!(
        (conn
            .status()
            .stream_state(Id::new(2).expect("fixture domain")))
            == (Some(stream::State::ReservedRemote))
    );
    let key = conn
        .status()
        .stream_key(Id::FIRST_SERVER)
        .expect("promised stream");
    conn.streams()
        .try_cancel(key, ErrorCode::Cancel)
        .expect("promised stream cancellation");
    conn.transfer().drain(usize::MAX);
    assert_eq!(
        conn.ingress().ingest(&push),
        Err(Error::Protocol(Protocol::Peer {
            actual: Id::FIRST_SERVER,
            last: Id::FIRST_SERVER,
            endpoint: Endpoint::Client,
        }))
    );
}

#[test]
fn rejects_reused_promise() {
    let mut conn = Connection::<ClientRole>::with_push::<Authority>().expect("client fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    conn.streams()
        .headers()
        .request([field::literal!(entry(b":method", b"GET"))], Boundary::End)
        .expect("protocol scenario precondition");
    conn.transfer().drain(usize::MAX);
    let invalid = Fields::new(&[
        Entry::new(b":scheme", b"http"),
        Entry::new(b":path", b"/missing-method"),
        Entry::new(b":authority", b"x"),
    ])
    .try_encode()
    .expect("protocol fixture");
    conn.ingress()
        .ingest(
            &Frame::new(1, &invalid)
                .try_push(2, frame::Block::End)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    assert!(conn.ingress().poll_event().is_none());
    assert!(
        (Wire::new(conn.output().first())
            .try_reset()
            .expect("protocol fixture"))
            == (Some((
                Id::new(2).expect("fixture domain"),
                ErrorCode::ProtocolError
            )))
    );
    conn.transfer().drain(usize::MAX);

    assert!(matches!(
        conn.ingress()
            .ingest(&Frame::new(1, &Fields::new(&GET).try_encode().expect("protocol fixture")).try_push(2, frame::Block::End).expect("protocol fixture")),
        Err(Error::Protocol(Protocol::Peer {
            actual,
            last,
            endpoint: Endpoint::Client,
        })) if actual == Id::new(2).expect("fixture domain") && last == Id::new(2).expect("fixture domain")
    ));
    assert!(
        !conn
            .status()
            .has_stream(Id::new(2).expect("fixture domain"))
    );
    assert!(conn.ingress().poll_event().is_none());
}

#[test]
fn refused_push_drains_head() {
    let mut conn = Connection::<ClientRole>::try_push::<Authority>(config::Config {
        stream_capacity: 1,
        ..Config::default()
    })
    .expect("protocol scenario precondition");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    conn.streams()
        .headers()
        .request([field::literal!(entry(b":method", b"GET"))], Boundary::End)
        .expect("protocol scenario precondition");
    conn.transfer().drain(usize::MAX);
    conn.ingress()
        .ingest(
            &Frame::new(
                1,
                &Fields::new(&GET).try_encode().expect("protocol fixture"),
            )
            .try_push(2, frame::Block::End)
            .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    assert!(
        (Wire::new(conn.output().first())
            .try_reset()
            .expect("protocol fixture"))
            == (Some((
                Id::new(2).expect("fixture domain"),
                ErrorCode::RefusedStream
            )))
    );
    conn.transfer().drain(usize::MAX);
    let response = Fields::new(&[Entry::new(b":status", b"200")])
        .try_encode()
        .expect("protocol fixture");

    conn.ingress()
        .ingest(
            &Frame::new(2, &response)
                .try_headers(Boundary::More, frame::Block::End)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");

    assert!(conn.ingress().poll_event().is_none());
    assert!(
        (Wire::new(conn.output().first())
            .try_reset()
            .expect("protocol fixture"))
            == (Some((Id::new(2).expect("fixture domain"), ErrorCode::StreamClosed)))
    );
}
