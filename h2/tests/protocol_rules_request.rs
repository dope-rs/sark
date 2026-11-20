use common::{
    hpack::Fields,
    protocol::{
        self,
        Session,
        rules::{INVALID_REQUESTS, Request, Retained, Settings, Wire},
        session::Frame,
    },
};
use sark_h2::{conn::event::Event, stream::Id};
pub mod common;
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ServerRole,
    conn::{
        config::{Config, Validated},
        machine::Connection,
    },
    frame::{Block, numbers::ErrorCode},
};
use sark_http::field::metadata::map::record::Entry;

use crate::common::protocol::rules::Declared;

#[test]
fn rejects_invalid_request_fields() {
    for &fields in INVALID_REQUESTS {
        let mut conn = protocol::try_connection::<ServerRole>().expect("protocol fixture");
        let block = Fields::new(fields).try_encode().expect("protocol fixture");
        let frame = Frame::new(1, &block)
            .try_headers(Boundary::End, Block::End)
            .expect("protocol wire fixture");
        conn.ingress().ingest(&frame).expect("request ingress");
        assert!(conn.ingress().poll_event().is_none(), "{fields:?}");
        assert_eq!(
            Wire::new(conn.output().first())
                .try_reset()
                .expect("protocol wire fixture"),
            Some((Id::new(1).expect("stream"), ErrorCode::ProtocolError)),
            "{fields:?}",
        );
    }
}

#[test]
fn rejects_missing_path() {
    let mut conn = protocol::try_connection::<ServerRole>().expect("protocol fixture");
    let [method, scheme, _, authority] = common::hpack::GET;
    let block = Fields::new(&[method, scheme, authority])
        .try_encode()
        .expect("protocol fixture");
    let frame = Frame::new(1, &block)
        .try_headers(Boundary::End, Block::End)
        .expect("protocol wire fixture");
    conn.ingress()
        .ingest(&frame)
        .expect("missing-path HEADERS frame fits ingress");
    let stream = Id::new(1).expect("fixture domain");
    assert!(!conn.status().has_stream(stream));
    assert!(conn.ingress().poll_event().is_none());
    assert_eq!(
        Wire::new(conn.output().first())
            .try_reset()
            .expect("protocol wire fixture"),
        Some((stream, ErrorCode::ProtocolError)),
    );
}

#[test]
fn accepts_request_with_or_without_te_trailers() {
    let trailers: &[(&[u8], &[u8])] = &[(b"te", b"TrAiLeRs")];
    for extra in [&[][..], trailers] {
        let mut conn = protocol::try_connection::<ServerRole>().expect("protocol fixture");
        let block = Request::new(extra).try_encode().expect("protocol fixture");
        let frame = Frame::new(1, &block)
            .try_headers(Boundary::End, Block::End)
            .expect("protocol wire fixture");
        conn.ingress().ingest(&frame).expect("request ingress");
        let event = conn.ingress().poll_event().expect("Headers event");
        assert!(matches!(event, Event::Headers { .. }), "{extra:?}");
        assert!(
            Wire::new(conn.output().first())
                .try_reset()
                .expect("protocol wire fixture")
                .is_none(),
            "{extra:?}",
        );
    }
}

#[test]
fn retains_declared_fields() {
    let mut conn = Connection::<ServerRole, Declared>::from_plan(
        Validated::new(Config::default()).expect("default H2 configuration must be valid"),
    )
    .expect("declared-field plan accepts default configuration");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let frame = Frame::new(
        1,
        &Request::new(&[(b"x-ignored", b"discard-me"), (b"x-name", b"alice")])
            .try_encode()
            .expect("protocol fixture"),
    )
    .try_headers(Boundary::End, Block::End)
    .expect("protocol wire fixture");
    conn.ingress()
        .ingest(&frame)
        .expect("declared-field HEADERS frame fits ingress");
    let event = conn.ingress().poll_event().expect("Headers event");
    if let Event::Headers { head, .. } = event {
        let (headers, selected, _) = head.into_parts();
        let selected = selected.into_inner();
        assert_eq!(selected, 14);
        let retained = Retained::new(&headers)
            .try_collect()
            .expect("protocol fixture");
        assert!(retained.iter().any(|(_, value)| *value == b"alice"));
        assert!(!retained.iter().any(|(_, value)| *value == b"discard-me"));
        assert_eq!(
            retained.iter().map(|(_, value)| value.len()).sum::<usize>(),
            selected
        );
    } else {
        assert!(matches!(event, Event::Headers { .. }));
    }
}

/// Never-indexed x-name bytes exercise the declared tag's direct writer.
#[test]
fn rejects_tagged_value() {
    let mut conn = Connection::<ServerRole, Declared>::from_plan(
        Validated::new(Config::default()).expect("configuration"),
    )
    .expect("connection");
    Session::new(&mut conn).try_prime().expect("preface");
    let block = Fields::new(&[
        Entry::new(b":method", b"GET"),
        Entry::new(b":scheme", b"http"),
        Entry::new(b":path", b"/"),
        Entry::new(b":authority", b"x"),
        Entry::new(b"x-name", b"\0"),
    ])
    .encode_never();
    let frame = Frame::new(1, &block)
        .try_headers(Boundary::End, Block::End)
        .expect("HEADERS frame");
    conn.ingress()
        .ingest(&frame)
        .expect("malformed request rejected");
    assert!(conn.ingress().poll_event().is_none());
    assert_eq!(
        Wire::new(conn.output().first()).try_reset().expect("wire"),
        Some((Id::new(1).expect("stream"), ErrorCode::ProtocolError)),
    );
}

#[test]
fn accepts_header_limit() {
    let max = Request::new(&[]).size();
    let mut conn: Connection<ServerRole> = Settings::limited(max)
        .try_connect()
        .expect("header limit fixture");
    let frame = Frame::new(
        1,
        &Request::new(&[]).try_encode().expect("protocol fixture"),
    )
    .try_headers(Boundary::End, Block::End)
    .expect("protocol wire fixture");
    conn.ingress()
        .ingest(&frame)
        .expect("limit-sized HEADERS frame fits ingress");
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
    let max = Request::new(&[]).size() - 1;
    let mut conn: Connection<ServerRole> = Settings::limited(max)
        .try_connect()
        .expect("header limit fixture");
    let frame = Frame::new(
        1,
        &Request::new(&[]).try_encode().expect("protocol fixture"),
    )
    .try_headers(Boundary::End, Block::End)
    .expect("protocol wire fixture");
    conn.ingress()
        .ingest(&frame)
        .expect("oversized HEADERS frame fits ingress");
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
    assert!(
        !conn
            .status()
            .has_stream(Id::new(1).expect("fixture domain"))
    );
}
