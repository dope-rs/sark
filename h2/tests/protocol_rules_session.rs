use common::{
    hpack::Fields,
    protocol::{Session, rules::Request, session::Frame},
};
use sark_h2::{
    ServerRole,
    conn::{event::Event, machine::Connection},
    stream::Id,
};
pub mod common;

use sark_core::manifold::stream::Boundary;
use sark_h2::{frame::Block, hpack::encode::Encoder};
use sark_http::field::metadata::map::record::Entry;

#[test]
fn continues_after_failure() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let bad = Fields::new(&[
        Entry::new(b":scheme", b"http"),
        Entry::new(b":path", b"/"),
        Entry::new(b":authority", b"x"),
    ])
    .try_encode()
    .expect("protocol fixture");
    conn.ingress()
        .ingest(
            &Frame::new(1, &bad)
                .try_headers(Boundary::End, Block::End)
                .expect("protocol wire fixture"),
        )
        .expect("invalid request HEADERS frame fits ingress");
    assert!(conn.ingress().poll_event().is_none());

    let good = Request::new(&[]).try_encode().expect("protocol fixture");
    conn.ingress()
        .ingest(
            &Frame::new(3, &good)
                .try_headers(Boundary::End, Block::End)
                .expect("protocol wire fixture"),
        )
        .expect("valid request HEADERS frame fits ingress");
    let ev = conn.ingress().poll_event().expect("good Headers");
    let id = match ev {
        Event::Headers { stream_id: id, .. } => Some(id),
        _ => None,
    }
    .expect("good request must emit Headers");
    assert!((id) == (Id::new(3).expect("fixture domain")));
}

#[test]
fn keeps_dynamic_updates() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let mut encoder = Encoder::try_new(4096).expect("HPACK encoder capacity");
    let bad = Fields::new(&[
        Entry::new(b":method", b"GET"),
        Entry::new(b":scheme", b"http"),
        Entry::new(b":path", b"/"),
        Entry::new(b"connection", b"close"),
        Entry::new(b"x-dynamic", b"retained"),
    ])
    .try_with(&mut encoder)
    .expect("HPACK table");
    conn.ingress()
        .ingest(
            &Frame::new(1, &bad)
                .try_headers(Boundary::End, Block::End)
                .expect("protocol wire fixture"),
        )
        .expect("invalid dynamic-table HEADERS frame fits ingress");
    assert!(conn.ingress().poll_event().is_none());

    let good = Fields::new(&[
        Entry::new(b":method", b"GET"),
        Entry::new(b":scheme", b"http"),
        Entry::new(b":path", b"/"),
        Entry::new(b"x-dynamic", b"retained"),
    ])
    .try_with(&mut encoder)
    .expect("HPACK table");
    conn.ingress()
        .ingest(
            &Frame::new(3, &good)
                .try_headers(Boundary::End, Block::End)
                .expect("protocol wire fixture"),
        )
        .expect("valid dynamic-table HEADERS frame fits ingress");
    let (id, headers) = match conn
        .ingress()
        .poll_event()
        .expect("dynamic-table request must emit Headers")
    {
        Event::Headers {
            stream_id: id,
            head,
            ..
        } => Some((id, head.into_parts().0)),
        _ => None,
    }
    .expect("dynamic-table request must emit Headers");
    assert!((id) == (Id::new(3).expect("fixture domain")));
    assert!(
        headers
            .iter()
            .any(|h| h.name == b"x-dynamic" && h.value == b"retained")
    );
}
