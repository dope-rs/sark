use common::{
    hpack::{Fields, GET},
    protocol::{
        Session,
        session::{Frame, Wire},
    },
};

use crate::common::headers::entry;

pub mod common;

use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ClientRole,
    conn::{Association, Error, Protocol, machine::Connection},
    frame,
    frame::numbers::ErrorCode,
    stream::Id,
};
use sark_http::field::{self, metadata::map::record::Entry};

use crate::common::protocol::session::Authority;

#[test]
fn push_requires_policy() {
    let mut conn = Connection::<ClientRole>::try_new().expect("protocol scenario precondition");
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
    assert!(
        !conn
            .status()
            .has_stream(Id::new(2).expect("fixture domain"))
    );
}

#[test]
fn requires_push_authority() {
    let mut conn = Connection::<ClientRole>::with_push::<Authority>().expect("client fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    conn.streams()
        .headers()
        .request([field::literal!(entry(b":method", b"GET"))], Boundary::End)
        .expect("protocol scenario precondition");
    conn.transfer().drain(usize::MAX);
    let block = Fields::new(&[
        Entry::new(b":method", b"GET"),
        Entry::new(b":scheme", b"http"),
        Entry::new(b":path", b"/missing-authority"),
    ])
    .try_encode()
    .expect("protocol fixture");

    conn.ingress()
        .ingest(
            &Frame::new(1, &block)
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
    assert!(
        !conn
            .status()
            .has_stream(Id::new(2).expect("fixture domain"))
    );
}

#[test]
fn enforces_push_origin() {
    let mut conn = Connection::<ClientRole>::with_push::<Authority>().expect("client fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    conn.streams()
        .headers()
        .request([field::literal!(entry(b":method", b"GET"))], Boundary::End)
        .expect("protocol scenario precondition");
    conn.transfer().drain(usize::MAX);
    let block = Fields::new(&[
        Entry::new(b":method", b"GET"),
        Entry::new(b":scheme", b"http"),
        Entry::new(b":path", b"/foreign"),
        Entry::new(b":authority", b"foreign.example"),
    ])
    .try_encode()
    .expect("protocol fixture");

    conn.ingress()
        .ingest(
            &Frame::new(1, &block)
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
    assert!(
        !conn
            .status()
            .has_stream(Id::new(2).expect("fixture domain"))
    );
}

#[test]
fn rejects_push_on_idle() {
    let mut conn = Connection::<ClientRole>::with_push::<Authority>().expect("client fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");

    assert!(matches!(
        conn.ingress()
            .ingest(&Frame::new(1, &Fields::new(&GET).try_encode().expect("protocol fixture")).try_push(2, frame::Block::End).expect("protocol fixture")),
        Err(Error::Protocol(Protocol::Association(Association::Idle(id)))) if id == Id::new(1).expect("fixture domain")
    ));
    assert!(
        !conn
            .status()
            .has_stream(Id::new(2).expect("fixture domain"))
    );
    assert!(conn.ingress().poll_event().is_none());
}
