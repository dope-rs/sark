use common::{
    hpack::Fields,
    protocol::{
        Session,
        rules::{Request, Settings, Wire},
        session::{self, Frame},
    },
};
use sark_h2::{
    ServerRole,
    conn::{config, event::Event, machine::Connection},
    frame::payload::Continuation,
    stream::Id,
};
pub mod common;

use sark_core::manifold::stream::{Boundary, Section};
use sark_h2::{frame, frame::numbers::ErrorCode};
use sark_http::field::metadata::{map::record::Entry, section::rules};

#[test]
fn accepts_trailers() {
    for len in [991, 992] {
        let plan = config::Validated::new(config::Config {
            local_settings: config::Settings {
                max_header_list_size: Some(1024),
                ..config::Settings::DEFAULT
            },
            ..config::Config::default()
        })
        .expect("header limits");
        let mut conn = Connection::<ServerRole, rules::RetainRaw<26>>::from_plan(plan)
            .expect("prefixed header storage");
        Session::new(&mut conn).try_prime().expect("preface");
        let initial = Request::new(&[]).try_encode().expect("request head");
        conn.ingress()
            .ingest(
                &Frame::new(1, &initial)
                    .try_headers(Boundary::More, frame::Block::End)
                    .expect("request frame"),
            )
            .expect("request admission");
        assert!(matches!(
            conn.ingress().poll_event(),
            Some(Event::Headers { .. })
        ));
        conn.ingress()
            .ingest(
                &Frame::new(1, b"x")
                    .try_data(Boundary::More)
                    .expect("request DATA frame"),
            )
            .expect("request DATA admission");
        assert!(matches!(
            conn.ingress().poll_event(),
            Some(Event::Data { .. })
        ));
        conn.transfer().drain(usize::MAX);
        let value = vec![b'a'; len];
        let fields = Fields::new(&[Entry::new(b"x", &value)]).encode_never();
        conn.ingress()
            .ingest(
                &Frame::new(1, &fields)
                    .try_headers(Boundary::End, frame::Block::End)
                    .expect("trailer frame"),
            )
            .expect("trailer admission");
        let event = conn.ingress().poll_event();
        if len == 991 {
            assert!(matches!(
                event,
                Some(Event::Headers {
                    head,
                    section: Section::Trailing,
                    boundary: Boundary::End,
                    ..
                }) if head.fields().get(b"x") == Some(value.as_slice())
            ));
            assert!(
                Wire::new(conn.output().first())
                    .try_reset()
                    .expect("protocol wire fixture")
                    .is_none()
            );
        } else {
            assert!(matches!(
                event,
                Some(Event::StreamReset {
                    error: ErrorCode::ProtocolError,
                    validation: Some(rejected),
                    ..
                }) if rejected.admit == sark_h2::hpack::decode::limit::Admission::Exceeded
            ));
        }
    }
}

#[test]
fn rejects_pseudo_trailer() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let initial = Frame::new(
        1,
        &Request::new(&[]).try_encode().expect("protocol fixture"),
    )
    .try_headers(Boundary::More, frame::Block::End)
    .expect("protocol wire fixture");
    conn.ingress()
        .ingest(&initial)
        .expect("initial request HEADERS frame fits ingress");
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::Headers { .. })
    ));
    conn.ingress()
        .ingest(
            &session::Frame::new(1, b"x")
                .try_data(Boundary::More)
                .expect("protocol wire fixture"),
        )
        .expect("request DATA frame fits ingress");
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::Data { .. })
    ));
    conn.transfer().drain(usize::MAX);

    let block = Fields::new(&[Entry::new(b":method", b"GET")])
        .try_encode()
        .expect("protocol fixture");
    let trailing = Frame::new(1, &block)
        .try_headers(Boundary::End, frame::Block::End)
        .expect("protocol wire fixture");
    conn.ingress()
        .ingest(&trailing)
        .expect("pseudo trailer HEADERS frame fits ingress");
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::StreamReset {
            stream_id, error: ErrorCode::ProtocolError, ..
        }) if stream_id == Id::new(1).expect("fixture domain")
    ));
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
fn validates_continuation() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let block = Fields::new(&[
        Entry::new(b":scheme", b"http"),
        Entry::new(b":path", b"/"),
        Entry::new(b":authority", b"x"),
    ])
    .try_encode()
    .expect("protocol fixture");
    let split = block.len() / 2;
    conn.ingress()
        .ingest(
            &Frame::new(1, &block[..split])
                .try_headers(Boundary::End, frame::Block::More)
                .expect("protocol wire fixture"),
        )
        .expect("partial request HEADERS frame fits ingress");
    assert!(conn.ingress().poll_event().is_none());
    conn.ingress()
        .ingest(&{
            let mut out = Vec::new();
            Continuation::new(
                Id::new(1).expect("fixture domain"),
                frame::Block::End,
                &block[split..],
            )
            .expect("remaining request bytes form a valid CONTINUATION")
            .encode(&mut out)
            .expect("frame allocation");
            out
        })
        .expect("request CONTINUATION frame fits ingress");
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
fn rejects_split_limit() {
    let max = Request::new(&[]).size() - 1;
    let mut conn = Settings::limited(max)
        .try_connect::<ServerRole>()
        .expect("header limit fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let block = Request::new(&[]).try_encode().expect("protocol fixture");
    let split = block.len() / 2;
    conn.ingress()
        .ingest(
            &Frame::new(1, &block[..split])
                .try_headers(Boundary::End, frame::Block::More)
                .expect("protocol wire fixture"),
        )
        .expect("partial oversized HEADERS frame fits ingress");
    assert!(conn.ingress().poll_event().is_none());
    conn.ingress()
        .ingest(&{
            let mut out = Vec::new();
            Continuation::new(
                Id::new(1).expect("fixture domain"),
                frame::Block::End,
                &block[split..],
            )
            .expect("remaining oversized bytes form a valid CONTINUATION")
            .encode(&mut out)
            .expect("frame allocation");
            out
        })
        .expect("oversized CONTINUATION frame fits ingress");
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
