use common::{
    hpack::{Fields, GET},
    protocol::{
        Session,
        conn::Controls,
        session::{Frame, Wire},
    },
};
use sark_h2::conn::event::Event;

use crate::common::headers::entry;
pub mod common;

use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ClientRole,
    conn::{
        Error, Protocol, config,
        config::{Config, Settings},
        machine::Connection,
    },
    frame,
    frame::{Ack, numbers::ErrorCode},
    stream,
    stream::Id,
};
use sark_http::field::{self, metadata::map::record::Entry};

use crate::common::protocol::session::Authority;

#[test]
fn accepts_push_before_ack() {
    let mut local = Settings::DEFAULT;
    local.enable_push = config::Push::Disabled;
    let mut conn = Connection::<ClientRole>::try_push::<Authority>(config::Config {
        local_settings: local,
        recv_window_target: 65_535,
        ..Config::default()
    })
    .expect("protocol scenario precondition");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");

    conn.streams()
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
    conn.transfer().drain(usize::MAX);
    let block = Fields::new(&[
        Entry::new(b":method", b"GET"),
        Entry::new(b":scheme", b"http"),
        Entry::new(b":path", b"/a"),
        Entry::new(b":authority", b"x"),
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

    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::PushPromise { .. })
    ));
    assert!(
        (conn
            .status()
            .stream_state(Id::new(2).expect("fixture domain")))
            == (Some(stream::State::ReservedRemote))
    );
}

#[test]
fn rejects_push_after_ack() {
    let mut local = Settings::DEFAULT;
    local.enable_push = config::Push::Disabled;
    let mut conn = Connection::<ClientRole>::with_config(Config {
        local_settings: local,
        recv_window_target: 65_535,
        ..Config::default()
    })
    .expect("protocol scenario precondition");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    conn.ingress()
        .ingest(
            &Controls::new(Ack::Complete)
                .try_settings(&[])
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::SettingsAck)
    ));

    conn.streams()
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
    conn.transfer().drain(usize::MAX);
    let block = Fields::new(&[
        Entry::new(b":method", b"GET"),
        Entry::new(b":scheme", b"http"),
        Entry::new(b":path", b"/a"),
        Entry::new(b":authority", b"x"),
    ])
    .try_encode()
    .expect("protocol fixture");

    assert!(matches!(
        conn.ingress().ingest(
            &Frame::new(1, &block)
                .try_push(2, frame::Block::End)
                .expect("protocol fixture")
        ),
        Err(Error::Protocol(Protocol::Push(
            sark_h2::conn::Push::Disabled
        )))
    ));
    assert!(
        (conn
            .status()
            .stream_state(Id::new(2).expect("fixture domain")))
        .is_none()
    );
    assert!(conn.ingress().poll_event().is_none());
}

#[test]
fn push_respects_capacity() {
    let mut conn = Connection::<ClientRole>::try_push::<Authority>(config::Config {
        local_settings: Settings::DEFAULT,
        recv_window_target: 65_535,
        stream_capacity: 2,
        ..Config::default()
    })
    .expect("protocol scenario precondition");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    conn.streams()
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
    conn.transfer().drain(usize::MAX);

    let block = Fields::new(&GET).try_encode().expect("protocol fixture");
    conn.ingress()
        .ingest(
            &Frame::new(1, &block[..1])
                .try_push(2, frame::Block::More)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    conn.ingress()
        .ingest(
            &Frame::new(1, &block[1..])
                .try_continue(frame::Block::End)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    assert!(
        (conn
            .status()
            .stream_state(Id::new(2).expect("fixture domain")))
            == (Some(stream::State::ReservedRemote))
    );

    while conn.ingress().poll_event().is_some() {}
    conn.ingress()
        .ingest(
            &Frame::new(1, &block)
                .try_push(4, frame::Block::End)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    assert!(
        (Wire::new(conn.output().first())
            .try_reset()
            .expect("protocol fixture"))
            == (Some((
                Id::new(4).expect("fixture domain"),
                ErrorCode::RefusedStream
            )))
    );
    assert!(
        !conn
            .status()
            .has_stream(Id::new(4).expect("fixture domain"))
    );
}

#[test]
fn all_push_hpack_splits() {
    let block = Fields::new(&[
        Entry::new(b":method", b"GET"),
        Entry::new(b":scheme", b"http"),
        Entry::new(b":path", b"/promised"),
        Entry::new(b":authority", b"example.com"),
    ])
    .try_encode()
    .expect("protocol fixture");

    for split in 0..=block.len() {
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
                &Frame::new(1, &block[..split])
                    .try_push(2, frame::Block::More)
                    .expect("protocol fixture"),
            )
            .expect("protocol scenario precondition");
        assert!(conn.ingress().poll_event().is_none());
        conn.ingress()
            .ingest(
                &Frame::new(1, &block[split..])
                    .try_continue(frame::Block::End)
                    .expect("protocol fixture"),
            )
            .expect("protocol scenario precondition");

        let (pushed, headers) = match conn
            .ingress()
            .poll_event()
            .expect("each HPACK boundary must emit PushPromise")
        {
            Event::PushPromise {
                promised_stream_id: pushed,
                head,
                ..
            } => Some((pushed, head.into_parts().0)),
            _ => None,
        }
        .expect("each HPACK boundary must emit PushPromise");
        assert!((pushed) == (Id::new(2).expect("fixture domain")));
        assert!(
            headers
                .iter()
                .any(|field| { field.name == b":path" && field.value == b"/promised" })
        );
    }
}
