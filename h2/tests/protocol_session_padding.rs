use common::{
    hpack::{Fields, GET},
    protocol::{
        Session,
        session::{Frame, Wire},
    },
};
use sark_h2::{conn::event::Event, frame::numbers::Limit};

pub mod common;

use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ServerRole,
    conn::{
        Error,
        config::{Config, Settings},
        machine::Connection,
    },
    flow::Receive,
    frame::{Block, numbers::ErrorCode},
    stream::Id,
};

#[test]
fn padding_debits_wire() {
    let mut conn = (Connection::<ServerRole>::with_config(Config {
        local_settings: Settings {
            initial_window_size: 4,
            ..Settings::DEFAULT
        },
        recv_window_target: 65_535,
        ..Config::default()
    }))
    .expect("connection settings must fit");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let block = Fields::new(&GET).try_encode().expect("protocol fixture");
    (conn.ingress().ingest(
        &Frame::new(1, &block)
            .try_headers(Boundary::More, Block::End)
            .expect("protocol fixture"),
    ))
    .as_ref()
    .expect("test operation must succeed");
    assert!(conn.ingress().poll_event().is_some());
    conn.transfer().drain(usize::MAX);
    let conn_old = conn.status().recv_window().value();
    (conn.ingress().ingest(
        &Frame::new(1, b"x")
            .try_padded(Boundary::End, 2)
            .expect("protocol fixture"),
    ))
    .as_ref()
    .expect("test operation must succeed");

    assert!(matches!(conn.ingress().poll_event(), Some(Event::Data {
            data,
            boundary: Boundary::End,
            ..
        })
            if data.segments().flat_map(|part| part.as_ref().iter()).eq(b"x".iter())));
    assert_eq!(conn.status().recv_window().value(), conn_old - 4);
    assert_eq!(
        conn.status()
            .recv_for(Id::new(1).expect("fixture domain"))
            .map(|flow| flow.value()),
        Some(0)
    );
}

#[test]
fn padding_resets_stream() {
    let mut conn = (Connection::<ServerRole>::with_config(Config {
        local_settings: Settings {
            initial_window_size: 3,
            ..Settings::DEFAULT
        },
        recv_window_target: 65_535,
        ..Config::default()
    }))
    .expect("connection settings must fit");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let block = Fields::new(&GET).try_encode().expect("protocol fixture");
    (conn.ingress().ingest(
        &Frame::new(1, &block)
            .try_headers(Boundary::More, Block::End)
            .expect("protocol fixture"),
    ))
    .as_ref()
    .expect("test operation must succeed");
    assert!(conn.ingress().poll_event().is_some());
    conn.transfer().drain(usize::MAX);
    let conn_old = conn.status().recv_window().value();
    (conn.ingress().ingest(
        &Frame::new(1, b"x")
            .try_padded(Boundary::More, 2)
            .expect("protocol fixture"),
    ))
    .as_ref()
    .expect("test operation must succeed");

    assert!(
        (Wire::new(conn.output().first())
            .try_reset()
            .expect("protocol fixture"))
            == (Some((Id::new(1).expect("fixture domain"), ErrorCode::FlowControl)))
    );
    assert_eq!(conn.status().recv_window().value(), conn_old - 4);
    assert!(
        !conn
            .status()
            .has_stream(Id::new(1).expect("fixture domain"))
    );
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::StreamReset {
            stream_id,
            error: ErrorCode::FlowControl,
            retired: None,
            validation: None,
        }) if stream_id.id() == Id::FIRST_CLIENT
    ));
    assert!(conn.ingress().poll_event().is_none());
}

#[test]
fn padding_overflows_conn() {
    let mut conn = (Connection::<ServerRole>::with_config(Config {
        local_settings: Settings {
            max_frame_size: Limit::MAX,
            ..Settings::DEFAULT
        },
        recv_window_target: 65_535,
        ..Config::default()
    }))
    .expect("connection settings must fit");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let block = Fields::new(&GET).try_encode().expect("protocol fixture");
    (conn.ingress().ingest(
        &Frame::new(1, &block)
            .try_headers(Boundary::More, Block::End)
            .expect("protocol fixture"),
    ))
    .as_ref()
    .expect("test operation must succeed");
    assert!(conn.ingress().poll_event().is_some());
    conn.transfer().drain(usize::MAX);
    let content = vec![0; 65_534];
    assert!(matches!(
        conn.ingress().ingest(
            &Frame::new(1, &content)
                .try_padded(Boundary::More, 1)
                .expect("protocol fixture")
        ),
        Err(Error::Receive(Receive::Stalled {
            available: 65_535,
            amount: 65_536,
        }))
    ));
    (conn.ingress().resume())
        .as_ref()
        .expect("test operation must succeed");
}
