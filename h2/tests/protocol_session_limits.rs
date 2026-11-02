use common::{
    hpack::{Fields, GET},
    protocol::{
        Session,
        conn::{Control, Controls},
        session::{Authority, Frame, Wire},
    },
};
use sark_h2::Endpoint;

use crate::common::headers::entry;
pub mod common;

use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ClientRole, ServerRole,
    conn::{
        Error, Protocol, config,
        config::{Config, Settings},
        machine::Connection,
    },
    frame,
    frame::{
        Ack,
        layout::Header,
        numbers::{ErrorCode, SettingId},
    },
    stream,
    stream::Id,
};
use sark_http::field::{self, metadata::map::record::Entry};

#[test]
fn reuses_resource_capacity() {
    let mut local = Settings::DEFAULT;
    local.max_concurrent_streams = Some(10);
    let mut conn = Connection::<ServerRole>::with_config(config::Config {
        local_settings: local,
        recv_window_target: 65_535,
        stream_capacity: 1,
        ..Config::default()
    })
    .expect("protocol scenario precondition");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let block = Fields::new(&GET).try_encode().expect("protocol fixture");

    conn.ingress()
        .ingest(
            &Frame::new(1, &block)
                .try_headers(Boundary::End, frame::Block::End)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    while conn.ingress().poll_event().is_some() {}
    conn.transfer().drain(usize::MAX);
    conn.ingress()
        .ingest(
            &Frame::new(3, &block)
                .try_headers(Boundary::End, frame::Block::End)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    assert!(
        (Wire::new(conn.output().first())
            .try_reset()
            .expect("protocol fixture"))
            == (Some((
                Id::new(3).expect("fixture domain"),
                ErrorCode::RefusedStream
            )))
    );

    conn.streams()
        .headers()
        .respond(
            Id::new(1).expect("fixture domain"),
            [field::literal!(entry(b":status", b"200"))],
            Boundary::End,
        )
        .expect("protocol scenario precondition");
    assert_eq!(conn.status().active_len(), 0);
    conn.transfer().drain(usize::MAX);
    conn.ingress()
        .ingest(
            &Frame::new(5, &block)
                .try_headers(Boundary::End, frame::Block::End)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    assert!(
        conn.status()
            .has_stream(Id::new(5).expect("fixture domain"))
    );
}

#[test]
fn reuses_peer_limit() {
    let mut conn = Connection::<ClientRole>::with_config(config::Config {
        local_settings: Settings::DEFAULT,
        recv_window_target: 65_535,
        stream_capacity: 2,
        ..Config::default()
    })
    .expect("protocol scenario precondition");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    conn.ingress()
        .ingest(
            &Controls::new(Ack::Required)
                .try_settings(&[(SettingId::MaxConcurrentStreams as u16, 1)])
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    while conn.ingress().poll_event().is_some() {}

    let request = [field::literal!(entry(b":method", b"GET"))];
    assert!(
        (conn
            .streams()
            .headers()
            .request(request, Boundary::End)
            .expect("protocol scenario precondition"))
            == (Id::new(1).expect("fixture domain"))
    );
    assert!(matches!(
        conn.streams().headers().request(request, Boundary::End),
        Err(error @ Error::StreamLimit {
            active: 1,
            limit: 1
        }) if error.is_pressure()
    ));

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
    while conn.ingress().poll_event().is_some() {}
    assert!(
        (conn
            .streams()
            .headers()
            .request(request, Boundary::End)
            .expect("protocol scenario precondition"))
            == (Id::new(3).expect("fixture domain"))
    );
}

#[test]
fn zero_limit_keeps_slots() {
    let mut local = Settings::DEFAULT;
    local.max_concurrent_streams = Some(0);
    let mut conn = Connection::<ServerRole>::with_config(config::Config {
        local_settings: local,
        recv_window_target: 65_535,
        stream_capacity: 1,
        ..Config::default()
    })
    .expect("protocol scenario precondition");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    conn.ingress()
        .ingest(
            &Frame::new(
                1,
                &Fields::new(&GET).try_encode().expect("protocol fixture"),
            )
            .try_headers(Boundary::End, frame::Block::End)
            .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    assert_eq!(conn.status().active_len(), 0);
    assert!(
        (Wire::new(conn.output().first())
            .try_reset()
            .expect("protocol fixture"))
            == (Some((
                Id::new(1).expect("fixture domain"),
                ErrorCode::RefusedStream
            )))
    );
}

#[test]
fn goaway_tracks_peer_id() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");

    let block = Fields::new(&GET).try_encode().expect("protocol fixture");
    conn.ingress()
        .ingest(
            &Frame::new(1, &block)
                .try_headers(Boundary::End, frame::Block::End)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    assert!(
        conn.ingress().poll_event().is_some(),
        "scenario emits the expected event"
    );
    conn.ingress()
        .ingest(
            &Frame::new(3, &block)
                .try_headers(Boundary::End, frame::Block::End)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    assert!(
        conn.ingress().poll_event().is_some(),
        "scenario emits the expected event"
    );
    conn.ingress()
        .ingest(
            &Frame::new(5, &block)
                .try_headers(Boundary::End, frame::Block::End)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    assert!(
        conn.ingress().poll_event().is_some(),
        "scenario emits the expected event"
    );

    conn.transfer().drain(usize::MAX);
    conn.control().goaway(ErrorCode::NoError);

    let out = conn.output().first();
    let h = Header::parse(out).expect("protocol scenario precondition");
    assert!((h.kind) == (frame::numbers::Type::GoAway));
    let payload = &out[9..];
    let last = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) & 0x7fff_ffff;
    assert_eq!(last, 5);
}

#[test]
fn window_update_expands() {
    let mut conn = Connection::<ClientRole>::with_push::<Authority>().expect("client fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");

    let id = conn
        .streams()
        .headers()
        .request(
            [field::literal!(entry(b":method", b"POST"))],
            Boundary::More,
        )
        .expect("protocol scenario precondition");
    let before = conn
        .status()
        .send_for(id)
        .expect("protocol scenario precondition")
        .value();
    conn.ingress()
        .ingest(
            &Control::new(id.into_u32())
                .try_window(1000)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    let after = conn
        .status()
        .send_for(id)
        .expect("protocol scenario precondition")
        .value();
    assert_eq!(after, before + 1000);
}

#[test]
fn server_rejects_push() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");

    let block = Fields::new(&GET).try_encode().expect("protocol fixture");
    conn.ingress()
        .ingest(
            &Frame::new(1, &block)
                .try_headers(Boundary::More, frame::Block::End)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    assert!(
        conn.ingress().poll_event().is_some(),
        "scenario emits the expected event"
    );

    let pp_block = Fields::new(&[Entry::new(b":path", b"/a")])
        .try_encode()
        .expect("protocol fixture");
    assert!(matches!(
        conn.ingress().ingest(
            &Frame::new(1, &pp_block)
                .try_push(2, frame::Block::End)
                .expect("protocol fixture")
        ),
        Err(Error::Protocol(Protocol::Push(sark_h2::conn::Push::Server)))
    ));
}

#[test]
fn reset_emits_and_evicts() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");

    let block = Fields::new(&GET).try_encode().expect("protocol fixture");
    conn.ingress()
        .ingest(
            &Frame::new(1, &block)
                .try_headers(Boundary::More, frame::Block::End)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    assert!(
        conn.ingress().poll_event().is_some(),
        "scenario emits the expected event"
    );
    conn.transfer().drain(usize::MAX);

    let key = conn
        .status()
        .stream_key(Id::new(1).expect("fixture domain"))
        .expect("active fixture stream");
    conn.streams()
        .try_cancel(key, ErrorCode::Cancel)
        .expect("active stream cancellation");
    assert!(
        !conn
            .status()
            .has_stream(Id::new(1).expect("fixture domain"))
    );
    let out = conn.output().first();
    let h = Header::parse(out).expect("protocol scenario precondition");
    assert!((h.kind) == (frame::numbers::Type::RstStream));
}

#[test]
fn rejects_peer_parity() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");

    let block = Fields::new(&GET).try_encode().expect("protocol fixture");
    assert!(matches!(
        conn.ingress()
            .ingest(&Frame::new(2, &block).try_headers(Boundary::More, frame::Block::End).expect("protocol fixture")),
        Err(Error::Protocol(Protocol::Peer {
            actual,
            last,
            endpoint: Endpoint::Server,
        })) if actual == Id::new(2).expect("fixture domain") && last == stream::Id::CONNECTION
    ));
}
