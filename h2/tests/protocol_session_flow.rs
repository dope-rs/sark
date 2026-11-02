use common::{
    hpack::{Fields, GET},
    protocol::{
        Session,
        session::{Frame, Wire},
    },
};
use sark_h2::{
    conn::event::Event,
    frame::numbers::{Limit, Type},
};
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
    frame,
    frame::{Block, layout::Header, numbers::ErrorCode},
    stream,
    stream::Id,
};
use sark_http::field::metadata::map::record::Entry;

#[test]
fn data_emits_updates() {
    let mut conn = (Connection::<ServerRole>::with_config(Config {
        local_settings: Settings {
            initial_window_size: 20_000,
            ..Settings::DEFAULT
        },
        recv_window_target: 20_000,
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

    let payload = b"hello world!";
    (conn.ingress().ingest(
        &Frame::new(1, payload)
            .try_data(Boundary::More)
            .expect("protocol fixture"),
    ))
    .as_ref()
    .expect("test operation must succeed");
    let (id, data, end) = match conn
        .ingress()
        .poll_event()
        .expect("scenario must emit DATA")
    {
        Event::Data {
            stream_id: id,
            data,
            boundary: end,
        } => Some((id, data, end)),
        _ => None,
    }
    .expect("scenario must emit DATA");
    assert!((id) == (Id::new(1).expect("fixture domain")));
    assert!(
        data.segments()
            .flat_map(|part| part.as_ref().iter())
            .eq(payload.iter())
    );
    assert!(matches!(end, Boundary::More));

    let count_wu = |out: &[u8]| {
        let mut pos = 0;
        let mut wu = 0;
        while pos < out.len() {
            let h = (Header::parse(&out[pos..])).expect("captured frame has a complete header");
            if h.kind == Type::WindowUpdate {
                wu += 1;
            }
            pos += 9 + h.length.into_usize();
        }
        wu
    };

    assert_eq!(count_wu(conn.output().first()), 0);
    conn.transfer().drain(usize::MAX);

    let chunk = vec![0u8; 12_000];
    (conn.ingress().ingest(
        &Frame::new(1, &chunk)
            .try_data(Boundary::More)
            .expect("protocol fixture"),
    ))
    .as_ref()
    .expect("test operation must succeed");
    assert!(conn.ingress().poll_event().is_some());
    assert_eq!(count_wu(conn.output().first()), 2);
}

#[test]
fn handshake_expands_window() {
    let conn = Connection::<ServerRole>::try_new().expect("server fixture");
    let target = conn.status().recv_window().available() as u32;
    let inc = (Wire::new(conn.output().first())
        .try_window()
        .expect("complete outbound frames"))
    .expect("handshake emits a connection WINDOW_UPDATE");
    assert!(inc > 0);
    assert_eq!(inc, target - 65_535);
}

#[test]
fn upload_replenishes() {
    let mut conn = (Connection::<ServerRole>::with_config(Config {
        local_settings: Settings {
            initial_window_size: 40_000,
            ..Settings::DEFAULT
        },
        recv_window_target: 40_000,
        ..Config::default()
    }))
    .expect("connection settings must fit");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");

    let block = Fields::new(&[
        Entry::new(b":method", b"POST"),
        Entry::new(b":scheme", b"http"),
        Entry::new(b":path", b"/"),
        Entry::new(b":authority", b"x"),
    ])
    .try_encode()
    .expect("protocol fixture");
    (conn.ingress().ingest(
        &Frame::new(1, &block)
            .try_headers(Boundary::More, Block::End)
            .expect("protocol fixture"),
    ))
    .as_ref()
    .expect("test operation must succeed");
    assert!(conn.ingress().poll_event().is_some());
    conn.transfer().drain(usize::MAX);

    let mut read = 0usize;
    for _ in 0..20 {
        let chunk = vec![7u8; 10_000];
        (conn.ingress().ingest(
            &Frame::new(1, &chunk)
                .try_data(Boundary::More)
                .expect("protocol fixture"),
        ))
        .as_ref()
        .expect("test operation must succeed");
        while let Some(ev) = conn.ingress().poll_event() {
            if let Event::Data { data, .. } = ev {
                read += data.len();
            }
        }
    }
    assert_eq!(read, 200_000);
    assert!(
        Wire::new(conn.output().first())
            .try_window()
            .expect("complete outbound frames")
            .is_some()
    );
}

#[test]
fn data_closes_remote() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
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
    (conn.ingress().ingest(
        &Frame::new(1, b"x")
            .try_data(Boundary::End)
            .expect("protocol fixture"),
    ))
    .as_ref()
    .expect("test operation must succeed");
    assert!(conn.ingress().poll_event().is_some());
    assert!(
        (conn
            .status()
            .stream_state(Id::new(1).expect("fixture domain")))
            == (Some(stream::State::HalfClosedRemote))
    );
}

#[test]
fn rejects_recv_overflow() {
    let mut local = Settings::DEFAULT;
    local.max_frame_size = Limit::MAX;
    let mut conn = (Connection::<ServerRole>::with_config(Config {
        local_settings: local,
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

    let huge = vec![0u8; 65_536];
    assert!(matches!(
        conn.ingress().ingest(
            &Frame::new(1, &huge)
                .try_data(Boundary::More)
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

#[test]
fn backpressure_keeps_flow() {
    let mut conn = (Connection::<ServerRole>::with_config(Config {
        local_settings: Settings {
            initial_window_size: 1,
            ..Settings::DEFAULT
        },
        recv_window_target: 65_535,
        outbound_capacity: 63,
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
    for opaque in 0..3 {
        (conn.control().ping([opaque; 8]))
            .as_ref()
            .expect("test operation must succeed");
    }
    let conn_old = conn.status().recv_window();
    let flow_old = (conn.status().recv_for(Id::new(1).expect("fixture domain")))
        .expect("the open stream has receive credit");
    let frame = Frame::new(1, &[])
        .try_padded(Boundary::More, 1)
        .expect("protocol fixture");

    let error = conn.ingress().ingest(&frame);
    assert!(
        error
            .as_ref()
            .is_err_and(|error| matches!(error, Error::OutputAppend(cause)
        if cause.into_parts().0.get() == 13 && cause.into_parts().1 == 12)),
        "the stream reset needs thirteen free output bytes"
    );
    assert!((conn.status().recv_window()) == (conn_old));
    assert!((conn.status().recv_for(Id::new(1).expect("fixture domain"))) == (Some(flow_old)));
    assert!(
        conn.status()
            .has_stream(Id::new(1).expect("fixture domain"))
    );
    conn.transfer().drain(usize::MAX);
    (conn.ingress().resume())
        .as_ref()
        .expect("test operation must succeed");
    assert_eq!(conn.status().recv_window().value(), conn_old.value() - 2);
    assert!(
        !conn
            .status()
            .has_stream(Id::new(1).expect("fixture domain"))
    );
    assert!(
        (Wire::new(conn.output().first())
            .try_reset()
            .expect("protocol fixture"))
            == (Some((Id::new(1).expect("fixture domain"), ErrorCode::FlowControl)))
    );
    assert!(
        matches!(conn.ingress().poll_event(), Some(Event::StreamReset {
        stream_id: stream, error: ErrorCode::FlowControl, ..
    }) if stream.id() == Id::FIRST_CLIENT)
    );
    assert!(conn.ingress().poll_event().is_none());
}

#[test]
fn rejects_large_frame() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");

    let bytes = [0, 0x40, 1, 0, 0, 0, 0, 0, 1];
    assert!(matches!(
        conn.ingress().ingest(&bytes),
        Err(Error::FrameLimit { kind, actual, limit })
            if (kind, actual, limit) == (0, 16_385, 16_384)
    ));
}

#[test]
fn refuses_stream_over_cap() {
    let mut local = Settings::DEFAULT;
    local.max_concurrent_streams = Some(3);
    let mut conn = (Connection::<ServerRole>::with_config(Config {
        local_settings: local,
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
    (conn.ingress().ingest(
        &Frame::new(3, &block)
            .try_headers(Boundary::More, Block::End)
            .expect("protocol fixture"),
    ))
    .as_ref()
    .expect("test operation must succeed");
    assert!(conn.ingress().poll_event().is_some());
    (conn.ingress().ingest(
        &Frame::new(5, &block)
            .try_headers(Boundary::More, Block::End)
            .expect("protocol fixture"),
    ))
    .as_ref()
    .expect("test operation must succeed");
    assert!(conn.ingress().poll_event().is_some());
    assert_eq!(conn.status().active_len(), 3);

    conn.transfer().drain(usize::MAX);
    (conn.ingress().ingest(
        &Frame::new(7, &block)
            .try_headers(Boundary::More, Block::End)
            .expect("protocol fixture"),
    ))
    .as_ref()
    .expect("test operation must succeed");
    assert!(conn.ingress().poll_event().is_none());
    let out = conn.output().first();
    let h = (Header::parse(out)).expect("captured frame has a complete header");
    assert!((h.kind) == (frame::numbers::Type::RstStream));
    assert!((h.stream_id) == (Id::new(7).expect("fixture domain")));
    let payload = &out[9..9 + 4];
    let err = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    assert_eq!(err, ErrorCode::RefusedStream as u32);
    assert_eq!(conn.status().active_len(), 3);
}
