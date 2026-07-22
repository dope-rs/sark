use sark_h2::conn::{Config, ConnError};
use sark_h2::frame::Ping;
use sark_h2::hpack::Header;
use sark_h2::{ClientRole, Conn, ServerRole, conn};

fn start_request(client: &mut Conn<ClientRole>) -> sark_h2::StreamId {
    client
        .start_request(
            &[
                Header::new(b":method", b"POST"),
                Header::new(b":scheme", b"http"),
                Header::new(b":path", b"/"),
                Header::new(b":authority", b"localhost"),
            ],
            false,
        )
        .unwrap()
}

#[test]
fn event_capacity_is_hard_bound() {
    let mut client = Conn::<ClientRole>::with_config(Config {
        event_capacity: 1,
        ..Config::default()
    });
    assert!(client.poll_event().is_some());
    let mut server = Conn::<ServerRole>::new();
    server.ping([1; 8]).unwrap();
    server.ping([2; 8]).unwrap();

    let error = client.ingest(server.outbound()).unwrap_err();
    assert_eq!(error, ConnError::Overload);
}

#[test]
fn event_backpressure_does_not_reapply_frames() {
    let mut client = Conn::<ClientRole>::with_config(Config {
        event_capacity: 1,
        ..Config::default()
    });
    client.poll_event().unwrap();
    let mut server = Conn::<ServerRole>::new();
    server.ping([1; 8]).unwrap();
    server.ping([2; 8]).unwrap();

    assert_eq!(client.ingest(server.outbound()), Err(ConnError::Overload));
    assert_eq!(client.poll_event(), Some(conn::Event::SettingsApplied));
    assert_eq!(client.resume(), Err(ConnError::Overload));
    assert_eq!(
        client.poll_event(),
        Some(conn::Event::Ping {
            ack: false,
            opaque: [1; 8],
        })
    );
    client.resume().unwrap();
    assert_eq!(
        client.poll_event(),
        Some(conn::Event::Ping {
            ack: false,
            opaque: [2; 8],
        })
    );
    assert!(client.poll_event().is_none());
}

#[test]
fn data_capacity_is_hard_bound() {
    let mut client = Conn::<ClientRole>::new();
    let stream_id = start_request(&mut client);
    client.send_data(stream_id, b"one", false).unwrap();
    client.send_data(stream_id, b"two", true).unwrap();

    let mut server = Conn::<ServerRole>::with_config(Config {
        event_capacity: 8,
        data_capacity: 1,
        ..Config::default()
    });
    let error = server.ingest(client.outbound()).unwrap_err();
    assert_eq!(error, ConnError::Overload);
}

#[test]
fn data_backpressure_retries_only_the_uncommitted_frame() {
    let mut client = Conn::<ClientRole>::new();
    let stream_id = start_request(&mut client);
    client.send_data(stream_id, b"one", false).unwrap();
    client.send_data(stream_id, b"two", true).unwrap();

    let mut server = Conn::<ServerRole>::with_config(Config {
        event_capacity: 8,
        data_capacity: 1,
        ..Config::default()
    });
    assert_eq!(server.ingest(client.outbound()), Err(ConnError::Overload));
    let mut first = None;
    while let Some(event) = server.poll_event() {
        if let conn::Event::Data { data, .. } = event {
            first = Some(data.as_ref().to_vec());
        }
    }
    assert_eq!(first.as_deref(), Some(b"one".as_slice()));

    server.resume().unwrap();
    let event = server.poll_event().unwrap();
    match event {
        conn::Event::Data {
            data, end_stream, ..
        } => {
            assert_eq!(data, b"two");
            assert!(end_stream);
        }
        event => panic!("unexpected event: {event:?}"),
    }
    assert!(server.poll_event().is_none());
}

#[test]
fn outbound_wrap_exposes_two_slices_without_compaction() {
    let mut conn = Conn::<ServerRole>::with_config(Config {
        outbound_capacity: 64,
        ..Config::default()
    });
    conn.drain_outbound(conn.outbound().len());
    conn.ping([1; 8]).unwrap();
    conn.ping([2; 8]).unwrap();
    conn.ping([3; 8]).unwrap();
    conn.drain_outbound(40);
    conn.ping([4; 8]).unwrap();
    conn.ping([5; 8]).unwrap();

    let (first, second) = conn.outbound_slices();
    assert!(!first.is_empty());
    assert!(!second.is_empty());
    let expected = [first, second].concat();
    let mut actual = [0; 64];
    let written = conn.drain_into(&mut actual);
    assert_eq!(&actual[..written], expected);
    assert!(conn.outbound().is_empty());
}

#[test]
fn inbound_wrap_parses_frame_across_both_slices() {
    let client = Conn::<ClientRole>::new();
    let mut server = Conn::<ServerRole>::with_config(Config {
        inbound_capacity: 64,
        ..Config::default()
    });
    for chunk in client.outbound().chunks(32) {
        server.ingest(chunk).unwrap();
    }
    while server.poll_event().is_some() {}

    let mut frames = Vec::new();
    for byte in 1..=4 {
        Ping {
            ack: false,
            opaque: [byte; 8],
        }
        .encode(&mut frames);
    }
    server.ingest(&frames[..52]).unwrap();
    server.ingest(&frames[52..]).unwrap();

    let mut seen = Vec::new();
    while let Some(event) = server.poll_event() {
        if let conn::Event::Ping { opaque, .. } = event {
            seen.push(opaque[0]);
        }
    }
    assert_eq!(seen, [1, 2, 3, 4]);
}
