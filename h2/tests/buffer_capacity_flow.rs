use sark_h2::{
    conn::{DataBlock, event::Event},
    stream::State,
};
pub mod common;

use o3::buffer::storage::Shared;
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ClientRole,
    conn::{
        DataOutcome, Error,
        config::{Config, Settings},
        machine::Connection,
    },
    frame::numbers::Type,
};

use crate::common::capacity;

#[test]
fn rejected_data_is_atomic() {
    let mut client = Connection::<ClientRole>::with_config(Config {
        outbound_capacity: 128,
        ..Config::default()
    })
    .expect("construct the constrained data client");
    client.transfer().drain(usize::MAX);
    let id = capacity::try_start(&mut client).expect("capacity fixture");
    client.transfer().drain(usize::MAX);
    for opaque in 0..7 {
        client
            .control()
            .ping([opaque; 8])
            .expect("fill data output with a ping");
    }

    let bytes = Shared::try_copy(b"x").expect("owned DATA fixture");
    let out_len = client.output().len();
    let conn_win = client.status().send_window();
    let flow = client
        .status()
        .send_for(id)
        .expect("find the open request stream window");
    let state = client.status().stream_state(id);
    assert!(matches!(
        client.streams().try_parts(id, b"x", &[], Boundary::End),
        Ok(DataOutcome::Blocked(DataBlock::Egress))
    ));
    assert!(matches!(
        client.streams().try_shared(id, &bytes, Boundary::End),
        Ok(DataOutcome::Blocked(DataBlock::Egress))
    ));
    assert_eq!(client.output().len(), out_len);
    assert!((client.status().send_window()) == (conn_win));
    assert!((client.status().send_for(id)) == (Some(flow)));
    assert!((client.status().stream_state(id)) == (state));

    client.transfer().drain(usize::MAX);
    assert!(matches!(
        client
            .streams()
            .try_shared(id, &bytes, Boundary::End),
        Ok(DataOutcome::Sent(written)) if written.get() == 1
    ));
    assert_eq!(client.status().send_window().value(), conn_win.value() - 1);
    assert_eq!(
        client
            .status()
            .send_for(id)
            .expect("find the updated request stream window")
            .value(),
        flow.value() - 1
    );
    assert!((client.status().stream_state(id)) == (Some(State::HalfClosedLocal)));
}

#[test]
fn data_ignores_headroom() {
    let (mut client, mut server, id) = capacity::Upload::new(Config {
        recv_window_target: 65_535,
        outbound_capacity: 64,
        ..Config::default()
    })
    .try_connect()
    .expect("capacity fixture");
    for opaque in 0..3 {
        server
            .control()
            .ping([opaque; 8])
            .expect("fill unrelated server output with a ping");
    }
    let out_len = server.output().len();
    assert_eq!(out_len, 51);

    assert!(matches!(client
        .streams()
        .try_send(id, b"small", Boundary::More)
        .expect("queue data that needs no window update"), DataOutcome::Sent(n) if n.get() == 5));
    server
        .ingress()
        .ingest(client.output().first())
        .expect("ingest data without consuming egress headroom");
    assert_eq!(server.output().len(), out_len);
    assert!(
        matches!(server.ingress().poll_event(), Some(Event::Data { data, .. }) if data.segments().flat_map(|part| part.as_ref().iter()).eq(b"small".iter()))
    );
}

#[test]
fn end_reserves_conn_update() {
    let (mut client, mut server, id) = capacity::Upload::new(Config {
        local_settings: Settings {
            initial_window_size: 20,
            ..Settings::DEFAULT
        },
        recv_window_target: 20,
        outbound_capacity: 64,
        ..Config::default()
    })
    .try_connect()
    .expect("capacity fixture");
    for opaque in 0..3 {
        server
            .control()
            .ping([opaque; 8])
            .expect("reserve output with a ping");
    }

    assert!(matches!(client
        .streams()
        .try_send(id, &[7; 10], Boundary::End)
        .expect("queue the final data frame"), DataOutcome::Sent(n) if n.get() == 10));
    server
        .ingress()
        .ingest(client.output().first())
        .expect("ingest final data with one window update");
    assert_eq!(server.output().len(), 64);
    assert_eq!(
        capacity::Wire::new(server.output().first())
            .try_count(Type::WindowUpdate)
            .expect("capacity fixture"),
        1
    );
    assert!((server.status().stream_state(id)) == (Some(State::HalfClosedRemote)));
}

#[test]
fn window_batch_is_atomic() {
    let (mut client, mut server, id) = capacity::Upload::new(Config {
        local_settings: Settings {
            initial_window_size: 20,
            ..Settings::DEFAULT
        },
        recv_window_target: 20,
        outbound_capacity: 70,
        ..Config::default()
    })
    .try_connect()
    .expect("capacity fixture");
    for opaque in 0..3 {
        server
            .control()
            .ping([opaque; 8])
            .expect("reserve output with a ping");
    }
    let conn_win = server.status().recv_window();
    let flow = server
        .status()
        .recv_for(id)
        .expect("find the request receive window");
    let state = server.status().stream_state(id);

    assert!(matches!(client
        .streams()
        .try_send(id, &[9; 10], Boundary::More)
        .expect("queue data that needs two window updates"), DataOutcome::Sent(n) if n.get() == 10));
    let error = server.ingress().ingest(client.output().first());
    assert!(
        error
            .as_ref()
            .is_err_and(|error| matches!(error, Error::OutputAppend(cause)
                if cause.into_parts().0.get() == 13 && cause.into_parts().1 == 6
            )),
        "two window updates must exceed output capacity"
    );
    assert_eq!(server.output().len(), 51);
    assert!((server.status().recv_window()) == (conn_win));
    assert!((server.status().recv_for(id)) == (Some(flow)));
    assert!((server.status().stream_state(id)) == (state));
    assert!(server.ingress().poll_event().is_none());

    server.transfer().drain(usize::MAX);
    server
        .ingress()
        .resume()
        .expect("commit both window updates after releasing output");
    assert_eq!(
        capacity::Wire::new(server.output().first())
            .try_count(Type::WindowUpdate)
            .expect("capacity fixture"),
        2
    );
    assert!((server.status().recv_window()) == (conn_win));
    assert!((server.status().recv_for(id)) == (Some(flow)));
    assert!(
        matches!(server.ingress().poll_event(), Some(Event::Data { data, .. }) if data.segments().flat_map(|part| part.as_ref().iter()).eq([9; 10].iter()))
    );
}
