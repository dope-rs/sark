use common::{
    hpack::Fields,
    protocol::{
        Session,
        rules::{Request, Wire},
        session::Frame,
    },
};
use sark_h2::{
    ServerRole,
    conn::{event::Event, machine::Connection},
    stream::Id,
};
pub mod common;

use sark_core::manifold::stream::Boundary;
use sark_h2::{frame, frame::numbers::ErrorCode};
use sark_http::field::metadata::map::record::Entry;

#[test]
fn rejects_unknown_pseudo() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let block = Fields::new(&[
        Entry::new(b":method", b"GET"),
        Entry::new(b":scheme", b"http"),
        Entry::new(b":path", b"/"),
        Entry::new(b":foo", b"bar"),
    ])
    .try_encode()
    .expect("protocol fixture");
    let frame = Frame::new(1, &block)
        .try_headers(Boundary::End, frame::Block::End)
        .expect("protocol wire fixture");
    conn.ingress()
        .ingest(&frame)
        .expect("unknown-pseudo HEADERS frame fits ingress");
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
fn rejects_status_pseudo() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let block = Fields::new(&[
        Entry::new(b":method", b"GET"),
        Entry::new(b":scheme", b"http"),
        Entry::new(b":path", b"/"),
        Entry::new(b":status", b"200"),
    ])
    .try_encode()
    .expect("protocol fixture");
    let frame = Frame::new(1, &block)
        .try_headers(Boundary::End, frame::Block::End)
        .expect("protocol wire fixture");
    conn.ingress()
        .ingest(&frame)
        .expect("status-pseudo HEADERS frame fits ingress");
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
fn rejects_late_pseudo() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let block = Fields::new(&[
        Entry::new(b":method", b"GET"),
        Entry::new(b":scheme", b"http"),
        Entry::new(b"content-type", b"text/plain"),
        Entry::new(b":path", b"/"),
    ])
    .try_encode()
    .expect("protocol fixture");
    let frame = Frame::new(1, &block)
        .try_headers(Boundary::End, frame::Block::End)
        .expect("protocol wire fixture");
    conn.ingress()
        .ingest(&frame)
        .expect("late-pseudo HEADERS frame fits ingress");
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
fn rejects_connection() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let block = Request::new(&[(b"connection", b"keep-alive")])
        .try_encode()
        .expect("protocol fixture");
    let frame = Frame::new(1, &block)
        .try_headers(Boundary::End, frame::Block::End)
        .expect("protocol wire fixture");
    conn.ingress()
        .ingest(&frame)
        .expect("connection HEADERS frame fits ingress");
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
fn rejects_keep_alive() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let block = Request::new(&[(b"keep-alive", b"timeout=5")])
        .try_encode()
        .expect("protocol fixture");
    let frame = Frame::new(1, &block)
        .try_headers(Boundary::End, frame::Block::End)
        .expect("protocol wire fixture");
    conn.ingress()
        .ingest(&frame)
        .expect("keep-alive HEADERS frame fits ingress");
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
fn rejects_transfer_field() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let block = Request::new(&[(b"transfer-encoding", b"chunked")])
        .try_encode()
        .expect("protocol fixture");
    let frame = Frame::new(1, &block)
        .try_headers(Boundary::End, frame::Block::End)
        .expect("protocol wire fixture");
    conn.ingress()
        .ingest(&frame)
        .expect("transfer-encoding HEADERS frame fits ingress");
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
fn accepts_te_trailers() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let block = Request::new(&[(b"te", b"TrAiLeRs")])
        .try_encode()
        .expect("protocol fixture");
    let frame = Frame::new(1, &block)
        .try_headers(Boundary::End, frame::Block::End)
        .expect("protocol wire fixture");
    conn.ingress()
        .ingest(&frame)
        .expect("TE trailers HEADERS frame fits ingress");
    let ev = conn.ingress().poll_event().expect("Headers event");
    assert!(matches!(ev, Event::Headers { .. }));
    assert!(
        Wire::new(conn.output().first())
            .try_reset()
            .expect("protocol wire fixture")
            .is_none()
    );
}

#[test]
fn rejects_te_gzip() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let block = Request::new(&[(b"te", b"gzip")])
        .try_encode()
        .expect("protocol fixture");
    let frame = Frame::new(1, &block)
        .try_headers(Boundary::End, frame::Block::End)
        .expect("protocol wire fixture");
    conn.ingress()
        .ingest(&frame)
        .expect("TE gzip HEADERS frame fits ingress");
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
