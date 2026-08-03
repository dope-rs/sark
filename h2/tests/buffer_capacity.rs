use o3::buffer::Shared;
use sark_h2::conn::{Config, ConnError};
use sark_h2::frame::{Continuation, FrameHeader, Headers, Ping, Type};
use sark_h2::hpack::{Encoder, Header};
use sark_h2::{ClientRole, ConfigError, Conn, ServerRole, Settings, ValidatedConfig, conn};

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

fn connected_upload(config: Config) -> (Conn<ClientRole>, Conn<ServerRole>, sark_h2::StreamId) {
    let mut client = Conn::<ClientRole>::new();
    let stream_id = start_request(&mut client);
    let mut server = Conn::<ServerRole>::with_config(config).unwrap();
    let outbound = client.outbound();
    let settings = FrameHeader::parse(&outbound[sark_h2::CLIENT_PREFACE.len()..]).unwrap();
    let handshake_len =
        sark_h2::CLIENT_PREFACE.len() + sark_h2::frame::HEADER_LEN + settings.length.as_usize();
    server.ingest(&outbound[..handshake_len]).unwrap();
    while server.poll_event().is_some() {}
    server.drain_outbound(usize::MAX);
    server.ingest(&outbound[handshake_len..]).unwrap();
    while server.poll_event().is_some() {}
    server.drain_outbound(usize::MAX);
    client.drain_outbound(usize::MAX);
    (client, server, stream_id)
}

fn count_frames(mut bytes: &[u8], kind: Type) -> usize {
    let mut count = 0;
    while !bytes.is_empty() {
        let header = FrameHeader::parse(bytes).unwrap();
        count += usize::from(header.kind == kind);
        bytes = &bytes[sark_h2::frame::HEADER_LEN + header.length.as_usize()..];
    }
    count
}

fn headers_frame(
    encoder: &mut Encoder,
    stream_id: sark_h2::StreamId,
    headers: &[Header<'_>],
) -> Vec<u8> {
    let mut block = Vec::new();
    encoder.encode(headers.iter().copied(), &mut block);
    let mut frame = Vec::new();
    Headers::new(stream_id, true, true, None, &block)
        .unwrap()
        .encode(&mut frame);
    frame
}

fn server_config_error(config: Config) -> ConfigError {
    match ValidatedConfig::<ServerRole>::new(config) {
        Ok(_) => panic!("configuration unexpectedly validated"),
        Err(error) => error,
    }
}

#[test]
fn config_validation_closes_every_initial_state_bound() {
    assert_eq!(
        server_config_error(Config {
            event_capacity: 0,
            ..Config::default()
        }),
        ConfigError::ZeroCapacity("event")
    );
    assert_eq!(
        server_config_error(Config {
            recv_window_target: 0x8000_0000,
            ..Config::default()
        }),
        ConfigError::ReceiveWindowOverflow
    );
    assert_eq!(
        server_config_error(Config {
            local_settings: Settings {
                initial_window_size: 0x8000_0000,
                ..Settings::DEFAULT
            },
            ..Config::default()
        }),
        ConfigError::InitialWindowOverflow
    );
    assert_eq!(
        server_config_error(Config {
            local_settings: Settings {
                max_frame_size: 16_383,
                ..Settings::DEFAULT
            },
            ..Config::default()
        }),
        ConfigError::InvalidMaxFrameSize
    );
    assert!(matches!(
        server_config_error(Config {
            outbound_capacity: 1,
            ..Config::default()
        }),
        ConfigError::OutboundTooSmall { .. }
    ));
    assert_eq!(
        server_config_error(Config {
            outbound_capacity: usize::MAX,
            ..Config::default()
        }),
        ConfigError::OutboundCapacityOverflow
    );
}

#[test]
fn validated_config_is_a_reusable_construction_proof() {
    let config = ValidatedConfig::<ServerRole>::new(Config::default()).unwrap();
    let _first = Conn::from_config(config);
    let _second = Conn::from_config(config);
}

#[test]
fn event_capacity_is_hard_bound() {
    let mut client = Conn::<ClientRole>::with_config(Config {
        event_capacity: 1,
        ..Config::default()
    })
    .unwrap();
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
    })
    .unwrap();
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
    })
    .unwrap();
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
    })
    .unwrap();
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
fn header_pool_grows_to_its_bound_and_reuses_released_slots() {
    let mut client = Conn::<ClientRole>::new();
    for _ in 0..3 {
        start_request(&mut client);
    }
    let mut server = Conn::<ServerRole>::with_config(Config {
        event_capacity: 8,
        header_capacity: 2,
        ..Config::default()
    })
    .unwrap();

    assert_eq!(server.ingest(client.outbound()), Err(ConnError::Overload));
    let first_batch = std::iter::from_fn(|| server.poll_event())
        .filter(|event| matches!(event, conn::Event::Headers { .. }))
        .count();
    assert_eq!(first_batch, 2);

    server.resume().unwrap();
    let second_batch = std::iter::from_fn(|| server.poll_event())
        .filter(|event| matches!(event, conn::Event::Headers { .. }))
        .count();
    assert_eq!(second_batch, 1);
}

#[test]
fn outbound_wrap_exposes_two_slices_without_compaction() {
    let mut conn = Conn::<ServerRole>::with_config(Config {
        outbound_capacity: 64,
        ..Config::default()
    })
    .unwrap();
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
fn rejected_header_block_does_not_desynchronize_hpack() {
    let mut client = Conn::<ClientRole>::new();
    let stream_id = start_request(&mut client);
    let mut server = Conn::<ServerRole>::with_config(Config {
        outbound_capacity: 64,
        ..Config::default()
    })
    .unwrap();
    client.ingest(server.outbound()).unwrap();
    while client.poll_event().is_some() {}
    server.drain_outbound(usize::MAX);
    server.ingest(client.outbound()).unwrap();
    while server.poll_event().is_some() {}
    client.ingest(server.outbound()).unwrap();
    while client.poll_event().is_some() {}
    server.drain_outbound(usize::MAX);

    server.ping([1; 8]).unwrap();
    server.ping([2; 8]).unwrap();
    server.ping([3; 8]).unwrap();
    let headers = [
        Header::new(b":status", b"200"),
        Header::new(b"x-backpressure", b"retained"),
    ];
    assert_eq!(
        server.send_response(stream_id, headers, false),
        Err(ConnError::Overload)
    );

    server.drain_outbound(usize::MAX);
    server.send_response(stream_id, headers, false).unwrap();
    client.ingest(server.outbound()).unwrap();
    let decoded = std::iter::from_fn(|| client.poll_event()).find_map(|event| match event {
        conn::Event::Headers { headers, .. } => Some(headers),
        _ => None,
    });
    assert!(
        decoded
            .unwrap()
            .iter()
            .any(|header| { header.name == b"x-backpressure" && header.value == b"retained" })
    );
}

#[test]
fn rejected_request_releases_stream_and_preserves_id() {
    let mut client = Conn::<ClientRole>::with_config(Config {
        outbound_capacity: 128,
        ..Config::default()
    })
    .unwrap();
    client.drain_outbound(usize::MAX);
    for opaque in 0..7 {
        client.ping([opaque; 8]).unwrap();
    }
    let headers = [
        Header::new(b":method", b"GET"),
        Header::new(b":scheme", b"http"),
        Header::new(b":path", b"/"),
        Header::new(b":authority", b"x"),
    ];

    assert_eq!(
        client.start_request(&headers, true),
        Err(ConnError::Overload)
    );
    assert_eq!(client.active_count(), 0);
    assert!(!client.has_stream(sark_h2::StreamId::FIRST_CLIENT));

    client.drain_outbound(usize::MAX);
    assert_eq!(
        client.start_request(&headers, true).unwrap(),
        sark_h2::StreamId::FIRST_CLIENT
    );
}

#[test]
fn rejected_data_preserves_flow_and_stream_state() {
    let mut client = Conn::<ClientRole>::with_config(Config {
        outbound_capacity: 128,
        ..Config::default()
    })
    .unwrap();
    client.drain_outbound(usize::MAX);
    let stream_id = start_request(&mut client);
    client.drain_outbound(usize::MAX);
    for opaque in 0..7 {
        client.ping([opaque; 8]).unwrap();
    }

    let outbound_len = client.outbound_len();
    let conn_window = client.send_window();
    let stream_window = client.stream_send_window(stream_id).unwrap();
    let state = client.stream_state(stream_id);
    assert_eq!(
        client.send_data_parts(stream_id, b"x", &[], true),
        Err(ConnError::Overload)
    );
    assert_eq!(
        client.send_data_shared(stream_id, &Shared::from_static(b"x"), true),
        Err(ConnError::Overload)
    );
    assert_eq!(client.outbound_len(), outbound_len);
    assert_eq!(client.send_window(), conn_window);
    assert_eq!(client.stream_send_window(stream_id), Some(stream_window));
    assert_eq!(client.stream_state(stream_id), state);

    client.drain_outbound(usize::MAX);
    assert_eq!(
        client
            .send_data_shared(stream_id, &Shared::from_static(b"x"), true)
            .unwrap(),
        1
    );
    assert_eq!(client.send_window().value, conn_window.value - 1);
    assert_eq!(
        client.stream_send_window(stream_id).unwrap().value,
        stream_window.value - 1
    );
    assert_eq!(
        client.stream_state(stream_id),
        Some(sark_h2::stream::State::HalfClosedLocal)
    );
}

#[test]
fn data_without_window_update_ignores_irrelevant_egress_headroom() {
    let (mut client, mut server, stream_id) = connected_upload(Config {
        recv_window_target: 65_535,
        outbound_capacity: 64,
        ..Config::default()
    });
    for opaque in 0..3 {
        server.ping([opaque; 8]).unwrap();
    }
    let outbound_len = server.outbound_len();
    assert_eq!(outbound_len, 51);

    client.send_data(stream_id, b"small", false).unwrap();
    server.ingest(client.outbound()).unwrap();
    assert_eq!(server.outbound_len(), outbound_len);
    assert!(matches!(
        server.poll_event(),
        Some(conn::Event::Data { data, .. }) if data == b"small"
    ));
}

#[test]
fn valid_headers_ignore_irrelevant_egress_headroom() {
    let (mut client, mut server, open_stream) = connected_upload(Config {
        recv_window_target: 65_535,
        outbound_capacity: 64,
        ..Config::default()
    });
    for opaque in 0..3 {
        server.ping([opaque; 8]).unwrap();
    }
    server
        .reset_stream(open_stream, sark_h2::frame::ErrorCode::Cancel)
        .unwrap();
    let outbound_len = server.outbound_len();
    assert_eq!(outbound_len, 64);

    let stream_id = start_request(&mut client);
    server.ingest(client.outbound()).unwrap();
    assert_eq!(server.outbound_len(), outbound_len);
    assert!(matches!(
        server.poll_event(),
        Some(conn::Event::Headers {
            stream_id: received,
            ..
        }) if received == stream_id
    ));
}

#[test]
fn committed_header_reset_uses_control_slack_and_advances_hpack_once() {
    let mut server = Conn::<ServerRole>::with_config(Config {
        outbound_capacity: 60,
        ..Config::default()
    })
    .unwrap();
    server.drain_outbound(usize::MAX);
    server.ingest(sark_h2::CLIENT_PREFACE).unwrap();
    while server.poll_event().is_some() {}
    for opaque in 0..3 {
        server.ping([opaque; 8]).unwrap();
    }
    assert_eq!(server.outbound_len(), 51);

    let mut encoder = Encoder::new(4096);
    let valid = [
        Header::new(b":method", b"GET"),
        Header::new(b":scheme", b"http"),
        Header::new(b":path", b"/"),
        Header::new(b"x-base", b"good"),
    ];
    server
        .ingest(&headers_frame(
            &mut encoder,
            sark_h2::StreamId::FIRST_CLIENT,
            &valid,
        ))
        .unwrap();
    while server.poll_event().is_some() {}

    let invalid = [Header::new(b"x-shift", b"discarded")];
    assert_eq!(
        server.ingest(&headers_frame(
            &mut encoder,
            sark_h2::StreamId::new(3).unwrap(),
            &invalid,
        )),
        Err(ConnError::Overload)
    );
    assert_eq!(server.outbound_len(), 64);
    assert_eq!(count_frames(server.outbound(), Type::RstStream), 1);
    server.drain_outbound(usize::MAX);
    server.resume().unwrap();
    assert_eq!(server.outbound_len(), 0);

    let next = headers_frame(&mut encoder, sark_h2::StreamId::new(5).unwrap(), &valid);
    server.ingest(&next).unwrap();
    let headers = std::iter::from_fn(|| server.poll_event()).find_map(|event| match event {
        conn::Event::Headers { headers, .. } => Some(headers),
        _ => None,
    });
    assert!(
        headers
            .unwrap()
            .iter()
            .any(|header| header.name == b"x-base" && header.value == b"good")
    );
}

#[test]
fn refused_headers_still_advance_hpack() {
    let mut local_settings = Settings::DEFAULT;
    local_settings.max_concurrent_streams = Some(1);
    let mut server = Conn::<ServerRole>::with_config(Config {
        local_settings,
        recv_window_target: 65_535,
        ..Config::default()
    })
    .unwrap();
    server.drain_outbound(usize::MAX);
    server.ingest(sark_h2::CLIENT_PREFACE).unwrap();
    while server.poll_event().is_some() {}

    let mut encoder = Encoder::new(4096);
    let base = [
        Header::new(b":method", b"GET"),
        Header::new(b":scheme", b"http"),
        Header::new(b":path", b"/"),
        Header::new(b"x-base", b"good"),
    ];
    server
        .ingest(&headers_frame(
            &mut encoder,
            sark_h2::StreamId::FIRST_CLIENT,
            &base,
        ))
        .unwrap();
    while server.poll_event().is_some() {}

    let refused = [
        Header::new(b":method", b"GET"),
        Header::new(b":scheme", b"http"),
        Header::new(b":path", b"/refused"),
        Header::new(b"x-shift", b"discarded"),
    ];
    server
        .ingest(&headers_frame(
            &mut encoder,
            sark_h2::StreamId::new(3).unwrap(),
            &refused,
        ))
        .unwrap();
    assert_eq!(count_frames(server.outbound(), Type::RstStream), 1);
    server.drain_outbound(usize::MAX);

    server
        .send_response(
            sark_h2::StreamId::FIRST_CLIENT,
            [Header::new(b":status", b"204")],
            true,
        )
        .unwrap();
    server.drain_outbound(usize::MAX);

    server
        .ingest(&headers_frame(
            &mut encoder,
            sark_h2::StreamId::new(5).unwrap(),
            &base,
        ))
        .unwrap();
    let headers = std::iter::from_fn(|| server.poll_event()).find_map(|event| match event {
        conn::Event::Headers { headers, .. } => Some(headers),
        _ => None,
    });
    assert!(
        headers
            .unwrap()
            .iter()
            .any(|header| header.name == b"x-base" && header.value == b"good")
    );
}

#[test]
fn fragmented_refused_headers_finish_before_reset() {
    let mut local_settings = Settings::DEFAULT;
    local_settings.max_concurrent_streams = Some(0);
    let mut server = Conn::<ServerRole>::with_config(Config {
        local_settings,
        recv_window_target: 65_535,
        ..Config::default()
    })
    .unwrap();
    server.drain_outbound(usize::MAX);
    server.ingest(sark_h2::CLIENT_PREFACE).unwrap();
    while server.poll_event().is_some() {}

    let mut encoder = Encoder::new(4096);
    let headers = [
        Header::new(b":method", b"GET"),
        Header::new(b":scheme", b"http"),
        Header::new(b":path", b"/fragmented"),
        Header::new(b"x-dynamic", b"decoded"),
    ];
    let mut block = Vec::new();
    encoder.encode(headers, &mut block);
    let split = 1;
    let stream_id = sark_h2::StreamId::FIRST_CLIENT;
    let mut first = Vec::new();
    Headers::new(stream_id, true, false, None, &block[..split])
        .unwrap()
        .encode(&mut first);
    let mut last = Vec::new();
    Continuation::new(stream_id, true, &block[split..])
        .unwrap()
        .encode(&mut last);

    server.ingest(&first).unwrap();
    assert_eq!(server.outbound_len(), 0);
    server.ingest(&last).unwrap();
    assert_eq!(count_frames(server.outbound(), Type::RstStream), 1);
    assert!(server.poll_event().is_none());
}

#[test]
fn end_stream_reserves_only_the_connection_window_update() {
    let (mut client, mut server, stream_id) = connected_upload(Config {
        local_settings: Settings {
            initial_window_size: 20,
            ..Settings::DEFAULT
        },
        recv_window_target: 20,
        outbound_capacity: 64,
        ..Config::default()
    });
    for opaque in 0..3 {
        server.ping([opaque; 8]).unwrap();
    }

    client.send_data(stream_id, &[7; 10], true).unwrap();
    server.ingest(client.outbound()).unwrap();
    assert_eq!(server.outbound_len(), 64);
    assert_eq!(count_frames(server.outbound(), Type::WindowUpdate), 1);
    assert_eq!(
        server.stream_state(stream_id),
        Some(sark_h2::stream::State::HalfClosedRemote)
    );
}

#[test]
fn rejected_window_update_batch_preserves_the_inbound_transaction() {
    let (mut client, mut server, stream_id) = connected_upload(Config {
        local_settings: Settings {
            initial_window_size: 20,
            ..Settings::DEFAULT
        },
        recv_window_target: 20,
        outbound_capacity: 70,
        ..Config::default()
    });
    for opaque in 0..3 {
        server.ping([opaque; 8]).unwrap();
    }
    let conn_window = server.recv_window();
    let stream_window = server.stream_recv_window(stream_id).unwrap();
    let state = server.stream_state(stream_id);

    client.send_data(stream_id, &[9; 10], false).unwrap();
    assert_eq!(server.ingest(client.outbound()), Err(ConnError::Overload));
    assert_eq!(server.outbound_len(), 51);
    assert_eq!(server.recv_window(), conn_window);
    assert_eq!(server.stream_recv_window(stream_id), Some(stream_window));
    assert_eq!(server.stream_state(stream_id), state);
    assert!(server.poll_event().is_none());

    server.drain_outbound(usize::MAX);
    server.resume().unwrap();
    assert_eq!(count_frames(server.outbound(), Type::WindowUpdate), 2);
    assert_eq!(server.recv_window(), conn_window);
    assert_eq!(server.stream_recv_window(stream_id), Some(stream_window));
    assert!(matches!(
        server.poll_event(),
        Some(conn::Event::Data { data, .. }) if data == [9; 10]
    ));
}

#[test]
fn inbound_wrap_parses_frame_across_both_slices() {
    let client = Conn::<ClientRole>::new();
    let mut server = Conn::<ServerRole>::with_config(Config {
        inbound_capacity: 64,
        ..Config::default()
    })
    .unwrap();
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
