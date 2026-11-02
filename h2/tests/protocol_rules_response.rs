use common::{
    hpack::Fields,
    protocol::{
        Session,
        rules::{Settings, Wire},
        session::Frame,
    },
};
use sark_h2::conn::event::Event;
pub mod common;
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ClientRole,
    conn::machine::Connection,
    frame::{Block, numbers::ErrorCode},
};
use sark_http::field::metadata::map::record::Entry;

#[test]
fn accepts_status() {
    let mut conn = Connection::<ClientRole>::try_new().expect("client fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let id = Session::new(&mut conn)
        .try_open()
        .expect("protocol fixture");
    let block = Fields::new(&[Entry::new(b":status", b"200")])
        .try_encode()
        .expect("protocol fixture");
    let frame = Frame::new(id.into_u32(), &block)
        .try_headers(Boundary::End, Block::End)
        .expect("protocol wire fixture");
    conn.ingress()
        .ingest(&frame)
        .expect("response HEADERS frame fits ingress");
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
fn rejects_header_limit() {
    let mut conn = Settings::limited(41)
        .try_connect::<ClientRole>()
        .expect("header limit fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let id = Session::new(&mut conn)
        .try_open()
        .expect("protocol fixture");
    let block = Fields::new(&[Entry::new(b":status", b"200")])
        .try_encode()
        .expect("protocol fixture");
    let frame = Frame::new(id.into_u32(), &block)
        .try_headers(Boundary::End, Block::End)
        .expect("protocol wire fixture");
    conn.ingress()
        .ingest(&frame)
        .expect("response HEADERS frame fits ingress");
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::StreamReset {
            stream_id, error: ErrorCode::ProtocolError, ..
        }) if stream_id == id
    ));
    let rst = Wire::new(conn.output().first())
        .try_reset()
        .expect("protocol wire fixture")
        .expect("RST_STREAM emitted");
    assert!((rst) == (id, ErrorCode::ProtocolError));
    assert!(!conn.status().has_stream(id));
}

#[test]
fn rejects_missing_status() {
    let mut conn = Connection::<ClientRole>::try_new().expect("client fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let id = Session::new(&mut conn)
        .try_open()
        .expect("protocol fixture");
    let block = Fields::new(&[Entry::new(b"content-type", b"text/plain")])
        .try_encode()
        .expect("protocol fixture");
    let frame = Frame::new(id.into_u32(), &block)
        .try_headers(Boundary::End, Block::End)
        .expect("protocol wire fixture");
    conn.ingress()
        .ingest(&frame)
        .expect("response HEADERS frame fits ingress");
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::StreamReset {
            stream_id, error: ErrorCode::ProtocolError, ..
        }) if stream_id == id
    ));
    let rst = Wire::new(conn.output().first())
        .try_reset()
        .expect("protocol wire fixture")
        .expect("RST_STREAM emitted");
    assert!((rst) == (id, ErrorCode::ProtocolError));
}

#[test]
fn rejects_uppercase() {
    let mut conn = Connection::<ClientRole>::try_new().expect("client fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let id = Session::new(&mut conn)
        .try_open()
        .expect("protocol fixture");
    let block = Fields::new(&[
        Entry::new(b":status", b"200"),
        Entry::new(b"Content-Type", b"text/plain"),
    ])
    .try_encode()
    .expect("protocol fixture");
    let frame = Frame::new(id.into_u32(), &block)
        .try_headers(Boundary::End, Block::End)
        .expect("protocol wire fixture");
    conn.ingress()
        .ingest(&frame)
        .expect("response HEADERS frame fits ingress");
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::StreamReset {
            stream_id, error: ErrorCode::ProtocolError, ..
        }) if stream_id == id
    ));
    let rst = Wire::new(conn.output().first())
        .try_reset()
        .expect("protocol wire fixture")
        .expect("RST_STREAM emitted");
    assert!((rst) == (id, ErrorCode::ProtocolError));
}

#[test]
fn rejects_request_pseudo() {
    let mut conn = Connection::<ClientRole>::try_new().expect("client fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let id = Session::new(&mut conn)
        .try_open()
        .expect("protocol fixture");
    let block = Fields::new(&[
        Entry::new(b":status", b"200"),
        Entry::new(b":method", b"GET"),
    ])
    .try_encode()
    .expect("protocol fixture");
    let frame = Frame::new(id.into_u32(), &block)
        .try_headers(Boundary::End, Block::End)
        .expect("protocol wire fixture");
    conn.ingress()
        .ingest(&frame)
        .expect("response HEADERS frame fits ingress");
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::StreamReset {
            stream_id, error: ErrorCode::ProtocolError, ..
        }) if stream_id == id
    ));
    let rst = Wire::new(conn.output().first())
        .try_reset()
        .expect("protocol wire fixture")
        .expect("RST_STREAM emitted");
    assert!((rst) == (id, ErrorCode::ProtocolError));
}

#[test]
fn rejects_unknown_pseudo() {
    let mut conn = Connection::<ClientRole>::try_new().expect("client fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let id = Session::new(&mut conn)
        .try_open()
        .expect("protocol fixture");
    let block = Fields::new(&[Entry::new(b":status", b"200"), Entry::new(b":foo", b"bar")])
        .try_encode()
        .expect("protocol fixture");
    let frame = Frame::new(id.into_u32(), &block)
        .try_headers(Boundary::End, Block::End)
        .expect("protocol wire fixture");
    conn.ingress()
        .ingest(&frame)
        .expect("response HEADERS frame fits ingress");
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::StreamReset {
            stream_id, error: ErrorCode::ProtocolError, ..
        }) if stream_id == id
    ));
    let rst = Wire::new(conn.output().first())
        .try_reset()
        .expect("protocol wire fixture")
        .expect("RST_STREAM emitted");
    assert!((rst) == (id, ErrorCode::ProtocolError));
}
