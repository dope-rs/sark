use common::{
    hpack::{Fields, GET},
    protocol::{
        Session,
        conn::Control,
        session::{Frame, Wire},
    },
};
use sark_h2::frame::numbers::{Length, Type};

use crate::common::headers::entry;
pub mod common;

use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ServerRole,
    conn::{Error, Protocol, machine::Connection},
    frame,
    frame::{
        layout::Header,
        numbers::{ErrorCode, Flags},
    },
    stream,
    stream::Id,
};
use sark_http::field::{self, metadata::map::record::Entry};

#[test]
fn idle_data_is_conn_error() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    assert!(matches!(
        conn.ingress()
            .ingest(&Frame::new(7, b"x").try_data(Boundary::More).expect("protocol fixture")),
        Err(Error::Protocol(Protocol::Idle(id))) if id == Id::new(7).expect("fixture domain")
    ));
}

#[test]
fn idle_reset_is_conn_error() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    assert!(matches!(
        conn.ingress()
            .ingest(&Control::new(7).try_reset(ErrorCode::Cancel).expect("protocol fixture")),
        Err(Error::Protocol(Protocol::Idle(id))) if id == Id::new(7).expect("fixture domain")
    ));
}

#[test]
fn idle_window_errors() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    assert!(matches!(
        conn.ingress().ingest(&Control::new(7).try_window(100).expect("protocol fixture")),
        Err(Error::Protocol(Protocol::Idle(id))) if id == Id::new(7).expect("fixture domain")
    ));
}

#[test]
fn reset_data_emits_closed() {
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
    conn.transfer().drain(usize::MAX);
    let conn_old = conn.status().recv_window().value();

    conn.ingress()
        .ingest(
            &Frame::new(1, b"x")
                .try_data(Boundary::More)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    let rst = Wire::new(conn.output().first())
        .try_reset()
        .expect("protocol fixture")
        .expect("RST emitted");
    assert!((rst) == (Id::new(1).expect("fixture domain"), ErrorCode::StreamClosed));
    assert_eq!(conn.status().recv_window().value(), conn_old - 1);
}

#[test]
fn reset_headers_close() {
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
    conn.transfer().drain(usize::MAX);

    conn.ingress()
        .ingest(
            &Frame::new(1, &block)
                .try_headers(Boundary::End, frame::Block::End)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    let rst = Wire::new(conn.output().first())
        .try_reset()
        .expect("protocol fixture")
        .expect("RST emitted");
    assert!((rst) == (Id::new(1).expect("fixture domain"), ErrorCode::StreamClosed));
}

#[test]
fn closed_data_emits_reset() {
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
    conn.streams()
        .headers()
        .respond(
            Id::new(1).expect("fixture domain"),
            [field::literal!(entry(b":status", b"200"))],
            Boundary::End,
        )
        .expect("protocol scenario precondition");
    assert!(
        !conn
            .status()
            .has_stream(Id::new(1).expect("fixture domain"))
    );
    conn.transfer().drain(usize::MAX);
    let window = conn.status().recv_window().value();

    conn.ingress()
        .ingest(
            &Frame::new(1, b"x")
                .try_data(Boundary::More)
                .expect("protocol fixture"),
        )
        .expect("closed DATA keeps connection flow control");
    assert_eq!(conn.status().recv_window().value(), window - 1);
    assert_eq!(
        Wire::new(conn.output().first())
            .try_reset()
            .expect("protocol fixture"),
        Some((Id::FIRST_CLIENT, ErrorCode::StreamClosed))
    );
    assert!(conn.ingress().poll_event().is_none());
}

#[test]
fn closed_headers_emit_reset() {
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
    conn.streams()
        .headers()
        .respond(
            Id::new(1).expect("fixture domain"),
            [field::literal!(entry(b":status", b"200"))],
            Boundary::End,
        )
        .expect("protocol scenario precondition");
    assert!(
        !conn
            .status()
            .has_stream(Id::new(1).expect("fixture domain"))
    );
    conn.transfer().drain(usize::MAX);

    conn.ingress()
        .ingest(
            &Frame::new(1, &block)
                .try_headers(Boundary::End, frame::Block::End)
                .expect("protocol fixture"),
        )
        .expect("closed HEADERS keep HPACK state");
    assert_eq!(
        Wire::new(conn.output().first())
            .try_reset()
            .expect("protocol fixture"),
        Some((Id::FIRST_CLIENT, ErrorCode::StreamClosed))
    );
    assert!(conn.ingress().poll_event().is_none());
}

#[test]
fn closed_reset_is_ignored() {
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
    conn.ingress()
        .ingest(
            &Control::new(1)
                .try_reset(ErrorCode::Cancel)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    assert!(
        conn.ingress().poll_event().is_some(),
        "scenario emits the expected event"
    );
    assert!(
        !conn
            .status()
            .has_stream(Id::new(1).expect("fixture domain"))
    );
    conn.transfer().drain(usize::MAX);

    conn.ingress()
        .ingest(
            &Control::new(1)
                .try_reset(ErrorCode::Cancel)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    assert!(conn.ingress().poll_event().is_none());
    assert!(conn.output().first().is_empty());
}

#[test]
fn remote_data_resets() {
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
    assert!(
        (conn
            .status()
            .stream_state(Id::new(1).expect("fixture domain")))
            == (Some(stream::State::HalfClosedRemote))
    );
    conn.transfer().drain(usize::MAX);
    let conn_old = conn.status().recv_window().value();

    conn.ingress()
        .ingest(
            &Frame::new(1, b"x")
                .try_data(Boundary::More)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    let rst = Wire::new(conn.output().first())
        .try_reset()
        .expect("protocol fixture")
        .expect("RST emitted");
    assert!((rst) == (Id::new(1).expect("fixture domain"), ErrorCode::StreamClosed));
    assert_eq!(conn.status().recv_window().value(), conn_old - 1);
    assert!(
        !conn
            .status()
            .has_stream(Id::new(1).expect("fixture domain"))
    );
}

#[test]
fn remote_headers_reset() {
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
    assert!(
        (conn
            .status()
            .stream_state(Id::new(1).expect("fixture domain")))
            == (Some(stream::State::HalfClosedRemote))
    );
    conn.transfer().drain(usize::MAX);

    let trailing = Fields::new(&[Entry::new(b"x-trailer", b"v")])
        .try_encode()
        .expect("protocol fixture");
    conn.ingress()
        .ingest(
            &Frame::new(1, &trailing)
                .try_headers(Boundary::End, frame::Block::End)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    let rst = Wire::new(conn.output().first())
        .try_reset()
        .expect("protocol fixture")
        .expect("RST emitted");
    assert!((rst) == (Id::new(1).expect("fixture domain"), ErrorCode::StreamClosed));
    assert!(
        !conn
            .status()
            .has_stream(Id::new(1).expect("fixture domain"))
    );
}

#[test]
fn rejects_bad_reset_size() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let mut bytes = Vec::new();
    bytes.extend_from_slice(
        &Header {
            length: Length::new(3).expect("fixture domain"),
            kind: Type::RstStream,
            flags: Flags(0),
            stream_id: Id::new(1).expect("fixture domain"),
        }
        .wire_bytes(),
    );
    bytes.extend_from_slice(&[0u8; 3]);
    assert!(matches!(
        conn.ingress().ingest(&bytes),
        Err(Error::ParseError(frame::numbers::ParseError::FrameSize {
            kind: frame::numbers::Type::RstStream,
            actual: 3,
            expected: 4,
        }))
    ));
}

#[test]
fn caps_continuations() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let cap = conn
        .settings()
        .local()
        .max_header_list_size
        .expect("protocol scenario precondition") as usize;

    let first = vec![0u8; cap - 1];
    conn.ingress()
        .ingest(
            &Frame::new(1, &first)
                .try_headers(Boundary::More, frame::Block::More)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");

    let flood = vec![0u8; 4096];
    assert!(matches!(
        conn.ingress()
            .ingest(&Frame::new(1, &flood).try_continue(frame::Block::More).expect("protocol fixture")),
        Err(Error::HeaderList {
            used,
            incoming: 4096,
            limit,
        }) if used == cap - 1 && limit == cap
    ));
}
