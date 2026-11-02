use common::{
    hpack::Fields,
    protocol::{Session, rules::Wire, session::Frame},
};
use sark_h2::stream::Id;
pub mod common;

use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ServerRole,
    conn::machine::Connection,
    frame::{Block, numbers::ErrorCode},
};
use sark_http::field::metadata::map::record::Entry;

#[test]
fn rejects_missing_path() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let block = Fields::new(&[
        Entry::new(b":method", b"GET"),
        Entry::new(b":scheme", b"http"),
        Entry::new(b":authority", b"x"),
    ])
    .try_encode()
    .expect("protocol fixture");
    let frame = Frame::new(1, &block)
        .try_headers(Boundary::End, Block::End)
        .expect("protocol wire fixture");
    conn.ingress()
        .ingest(&frame)
        .expect("missing-path HEADERS frame fits ingress");
    assert!(
        !conn
            .status()
            .has_stream(Id::new(1).expect("fixture domain"))
    );
    assert!(conn.ingress().poll_event().is_none());
    let rst = Wire::new(conn.output().first())
        .try_reset()
        .expect("protocol wire fixture")
        .expect("RST_STREAM emitted");
    assert!(
        (rst)
            == (
                Id::new(1).expect("fixture domain"),
                ErrorCode::ProtocolError
            )
    );
}

#[test]
fn rejects_empty_path() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let block = Fields::new(&[
        Entry::new(b":method", b"GET"),
        Entry::new(b":scheme", b"http"),
        Entry::new(b":path", b""),
        Entry::new(b":authority", b"x"),
    ])
    .try_encode()
    .expect("protocol fixture");
    let frame = Frame::new(1, &block)
        .try_headers(Boundary::End, Block::End)
        .expect("protocol wire fixture");
    conn.ingress()
        .ingest(&frame)
        .expect("empty-path HEADERS frame fits ingress");
    assert!(conn.ingress().poll_event().is_none());
    let rst = Wire::new(conn.output().first())
        .try_reset()
        .expect("protocol wire fixture")
        .expect("RST_STREAM emitted");
    assert!(
        (rst)
            == (
                Id::new(1).expect("fixture domain"),
                ErrorCode::ProtocolError
            )
    );
}

#[test]
fn rejects_duplicate_method() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let block = Fields::new(&[
        Entry::new(b":method", b"GET"),
        Entry::new(b":method", b"POST"),
        Entry::new(b":scheme", b"http"),
        Entry::new(b":path", b"/"),
        Entry::new(b":authority", b"x"),
    ])
    .try_encode()
    .expect("protocol fixture");
    let frame = Frame::new(1, &block)
        .try_headers(Boundary::End, Block::End)
        .expect("protocol wire fixture");
    conn.ingress()
        .ingest(&frame)
        .expect("duplicate-method HEADERS frame fits ingress");
    assert!(conn.ingress().poll_event().is_none());
    let rst = Wire::new(conn.output().first())
        .try_reset()
        .expect("protocol wire fixture")
        .expect("RST_STREAM emitted");
    assert!(
        (rst)
            == (
                Id::new(1).expect("fixture domain"),
                ErrorCode::ProtocolError
            )
    );
}
