use common::{
    hpack::Fields,
    protocol::{
        self,
        Session,
        rules::{INVALID_RESPONSES, Settings, Wire},
        session::Frame,
    },
};
use sark_h2::conn::event::Event;
pub mod common;
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ClientRole,
    frame::{Block, numbers::ErrorCode},
};
use sark_http::field::metadata::map::record::Entry;

#[test]
fn accepts_status() {
    let mut conn = protocol::try_connection::<ClientRole>().expect("protocol fixture");
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
        .try_connect()
        .expect("header limit fixture");
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
fn rejects_invalid_response_fields() {
    for &fields in INVALID_RESPONSES {
        let mut conn = protocol::try_connection::<ClientRole>().expect("protocol fixture");
        let id = Session::new(&mut conn)
            .try_open()
            .expect("protocol fixture");
        let block = Fields::new(fields).try_encode().expect("protocol fixture");
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
        assert_eq!(rst, (id, ErrorCode::ProtocolError));
    }
}
