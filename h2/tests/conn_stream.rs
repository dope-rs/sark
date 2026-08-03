mod common;

use common::{flen, sid, win};
use sark_h2::frame::{self, SettingId};
use sark_h2::hpack::{Encoder, Header};
use sark_h2::{
    CLIENT_PREFACE, ClientRole, Conn, ConnError, ErrorCode, FrameHeader, ServerRole, Settings,
    StreamId, conn, stream,
};

fn server() -> Conn<ServerRole> {
    Conn::<ServerRole>::new()
}

fn client() -> Conn<ClientRole> {
    Conn::<ClientRole>::new()
}

fn settings_frame_bytes(params: &[(u16, u32)], ack: bool) -> Vec<u8> {
    let mut payload = Vec::new();
    for (id, val) in params {
        payload.extend_from_slice(&id.to_be_bytes());
        payload.extend_from_slice(&val.to_be_bytes());
    }
    let mut out = Vec::new();
    frame::Settings::new(ack, &payload)
        .unwrap()
        .encode(&mut out);
    out
}

fn prime_server(conn: &mut Conn<ServerRole>) {
    conn.drain_outbound(conn.outbound().len());
    conn.ingest(CLIENT_PREFACE).unwrap();
    while conn.poll_event().is_some() {}
}

fn prime_client(conn: &mut Conn<ClientRole>) {
    conn.drain_outbound(conn.outbound().len());
    while conn.poll_event().is_some() {}
}

fn encode_hpack(headers: &[Header<'_>]) -> Vec<u8> {
    let mut enc = Encoder::new(4096);
    let mut out = Vec::new();
    enc.encode(headers.iter().copied(), &mut out);
    out
}

fn headers_frame_bytes(
    stream_id: u32,
    end_stream: bool,
    end_headers: bool,
    block: &[u8],
) -> Vec<u8> {
    let mut out = Vec::new();
    frame::Headers::new(sid(stream_id), end_stream, end_headers, None, block)
        .unwrap()
        .encode(&mut out);
    out
}

fn continuation_bytes(stream_id: u32, end_headers: bool, block: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    frame::Continuation::new(sid(stream_id), end_headers, block)
        .unwrap()
        .encode(&mut out);
    out
}

fn data_frame_bytes(stream_id: u32, end_stream: bool, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    frame::Data::new(sid(stream_id), end_stream, payload)
        .unwrap()
        .encode(&mut out);
    out
}

fn rst_frame_bytes(stream_id: u32, error: ErrorCode) -> Vec<u8> {
    let mut out = Vec::new();
    frame::RstStream::new(sid(stream_id), error)
        .unwrap()
        .encode(&mut out);
    out
}

fn push_promise_bytes(stream_id: u32, promised: u32, end_headers: bool, block: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    frame::PushPromise::new(sid(stream_id), sid(promised), end_headers, block)
        .unwrap()
        .encode(&mut out);
    out
}

fn window_update_bytes(stream_id: u32, increment: u32) -> Vec<u8> {
    let mut out = Vec::new();
    frame::WindowUpdate {
        stream_id: sid(stream_id),
        increment: win(increment),
    }
    .encode(&mut out);
    out
}

fn ping_frame_bytes(opaque: [u8; 8]) -> Vec<u8> {
    let mut out = Vec::new();
    sark_h2::frame::Ping { ack: false, opaque }.encode(&mut out);
    out
}

#[test]
fn headers_recv_opens_stream() {
    let mut conn = server();
    prime_server(&mut conn);

    let block = encode_hpack(&[
        Header {
            name: b":method",
            value: b"GET",
        },
        Header {
            name: b":scheme",
            value: b"http",
        },
        Header {
            name: b":path",
            value: b"/",
        },
        Header {
            name: b":authority",
            value: b"x",
        },
    ]);
    let frame = headers_frame_bytes(1, false, true, &block);
    conn.ingest(&frame).unwrap();

    let ev = conn.poll_event().unwrap();
    match ev {
        conn::Event::Headers {
            stream_id,
            headers,
            end_stream,
            trailing,
            ..
        } => {
            assert_eq!(stream_id, sid(1));
            assert!(!end_stream);
            assert!(!trailing);
            assert!(headers.iter().any(|h| h.name == b":method"));
        }
        _ => panic!("expected Headers"),
    }
    assert_eq!(conn.stream_state(sid(1)), Some(stream::State::Open));
}

#[test]
fn headers_end_stream_recv_half_closed_remote() {
    let mut conn = server();
    prime_server(&mut conn);

    let block = encode_hpack(&[
        Header {
            name: b":method",
            value: b"GET",
        },
        Header {
            name: b":scheme",
            value: b"http",
        },
        Header {
            name: b":path",
            value: b"/",
        },
        Header {
            name: b":authority",
            value: b"x",
        },
    ]);
    let frame = headers_frame_bytes(1, true, true, &block);
    conn.ingest(&frame).unwrap();
    let _ = conn.poll_event().unwrap();
    assert_eq!(
        conn.stream_state(sid(1)),
        Some(stream::State::HalfClosedRemote)
    );
}

#[test]
fn headers_with_continuation_assembles_block() {
    let mut conn = server();
    prime_server(&mut conn);

    let block = encode_hpack(&[
        Header {
            name: b":method",
            value: b"GET",
        },
        Header {
            name: b":scheme",
            value: b"http",
        },
        Header {
            name: b":path",
            value: b"/",
        },
        Header {
            name: b":authority",
            value: b"x",
        },
    ]);
    let split_a = &block[..2];
    let split_b = &block[2..4];
    let split_c = &block[4..];

    conn.ingest(&headers_frame_bytes(1, false, false, split_a))
        .unwrap();
    assert!(conn.poll_event().is_none());
    conn.ingest(&continuation_bytes(1, false, split_b)).unwrap();
    assert!(conn.poll_event().is_none());
    conn.ingest(&continuation_bytes(1, true, split_c)).unwrap();
    let ev = conn.poll_event().unwrap();
    match ev {
        conn::Event::Headers {
            stream_id, headers, ..
        } => {
            assert_eq!(stream_id, sid(1));
            assert!(headers.iter().any(|h| h.name == b":method"));
        }
        _ => panic!("expected Headers"),
    }
}

#[test]
fn every_hpack_boundary_streams_across_continuation() {
    let block = encode_hpack(&[
        Header {
            name: b":method",
            value: b"GET",
        },
        Header {
            name: b":scheme",
            value: b"https",
        },
        Header {
            name: b":path",
            value: b"/fragmented",
        },
        Header {
            name: b"x-streaming",
            value: b"abcdefghijklmnopqrstuvwxyz",
        },
    ]);

    for split in 0..=block.len() {
        let mut conn = server();
        prime_server(&mut conn);
        conn.ingest(&headers_frame_bytes(1, false, false, &block[..split]))
            .unwrap();
        assert!(conn.poll_event().is_none());
        conn.ingest(&continuation_bytes(1, true, &block[split..]))
            .unwrap();
        let Some(conn::Event::Headers { headers, .. }) = conn.poll_event() else {
            panic!("expected Headers at HPACK boundary {split}");
        };
        assert!(headers.iter().any(|field| {
            field.name == b"x-streaming" && field.value == b"abcdefghijklmnopqrstuvwxyz"
        }));
    }
}

#[test]
fn every_receive_boundary_decodes_without_coalescing_the_frame() {
    let block = encode_hpack(&[
        Header {
            name: b":method",
            value: b"GET",
        },
        Header {
            name: b":scheme",
            value: b"https",
        },
        Header {
            name: b":path",
            value: b"/owners",
        },
    ]);
    let frame = headers_frame_bytes(1, false, true, &block);

    for split in 0..=frame.len() {
        let mut conn = server();
        prime_server(&mut conn);
        conn.ingest(&frame[..split]).unwrap();
        conn.ingest(&frame[split..]).unwrap();
        assert!(matches!(
            conn.poll_event(),
            Some(conn::Event::Headers { .. })
        ));
    }
}

#[test]
fn continuation_interleaved_other_frame_errors() {
    let mut conn = server();
    prime_server(&mut conn);

    let block = encode_hpack(&[
        Header {
            name: b":method",
            value: b"GET",
        },
        Header {
            name: b":scheme",
            value: b"http",
        },
        Header {
            name: b":path",
            value: b"/",
        },
        Header {
            name: b":authority",
            value: b"x",
        },
    ]);
    conn.ingest(&headers_frame_bytes(1, false, false, &block[..1]))
        .unwrap();
    let err = conn.ingest(&ping_frame_bytes([0u8; 8])).unwrap_err();
    assert_eq!(err, ConnError::Continuation);
}

#[test]
fn continuation_interleaved_data_errors() {
    let mut conn = server();
    prime_server(&mut conn);

    let block = encode_hpack(&[
        Header {
            name: b":method",
            value: b"GET",
        },
        Header {
            name: b":scheme",
            value: b"http",
        },
        Header {
            name: b":path",
            value: b"/",
        },
        Header {
            name: b":authority",
            value: b"x",
        },
    ]);
    conn.ingest(&headers_frame_bytes(1, false, false, &block[..1]))
        .unwrap();
    let err = conn.ingest(&data_frame_bytes(1, false, b"x")).unwrap_err();
    assert_eq!(err, ConnError::Continuation);
}

#[test]
fn continuation_wrong_stream_id_errors() {
    let mut conn = server();
    prime_server(&mut conn);

    let block = encode_hpack(&[
        Header {
            name: b":method",
            value: b"GET",
        },
        Header {
            name: b":scheme",
            value: b"http",
        },
        Header {
            name: b":path",
            value: b"/",
        },
        Header {
            name: b":authority",
            value: b"x",
        },
    ]);
    conn.ingest(&headers_frame_bytes(1, false, false, &block[..1]))
        .unwrap();
    let err = conn
        .ingest(&continuation_bytes(3, true, &block[1..]))
        .unwrap_err();
    assert_eq!(err, ConnError::Continuation);
}

#[test]
fn data_recv_emits_event_and_window_updates() {
    let mut conn = Conn::<ServerRole>::with_local_settings(
        Settings {
            initial_window_size: 20_000,
            ..Settings::DEFAULT
        },
        20_000,
    )
    .unwrap();
    prime_server(&mut conn);

    let block = encode_hpack(&[
        Header {
            name: b":method",
            value: b"GET",
        },
        Header {
            name: b":scheme",
            value: b"http",
        },
        Header {
            name: b":path",
            value: b"/",
        },
        Header {
            name: b":authority",
            value: b"x",
        },
    ]);
    conn.ingest(&headers_frame_bytes(1, false, true, &block))
        .unwrap();
    let _ = conn.poll_event().unwrap();
    conn.drain_outbound(conn.outbound().len());

    let payload = b"hello world!";
    conn.ingest(&data_frame_bytes(1, false, payload)).unwrap();
    let ev = conn.poll_event().unwrap();
    match ev {
        conn::Event::Data {
            stream_id,
            data,
            end_stream,
        } => {
            assert_eq!(stream_id, sid(1));
            assert_eq!(data, payload);
            assert!(!end_stream);
        }
        _ => panic!("expected Data"),
    }

    let count_wu = |out: &[u8]| {
        let mut pos = 0;
        let mut wu = 0;
        while pos < out.len() {
            let h = FrameHeader::parse(&out[pos..]).unwrap();
            if h.kind == frame::Type::WindowUpdate {
                wu += 1;
            }
            pos += 9 + h.length.as_usize();
        }
        wu
    };

    assert_eq!(count_wu(conn.outbound()), 0);
    conn.drain_outbound(conn.outbound().len());

    let chunk = vec![0u8; 12_000];
    conn.ingest(&data_frame_bytes(1, false, &chunk)).unwrap();
    let _ = conn.poll_event().unwrap();
    assert_eq!(count_wu(conn.outbound()), 2);
}

fn conn_window_update_increment(out: &[u8]) -> Option<u32> {
    let mut pos = 0;
    while pos < out.len() {
        let h = FrameHeader::parse(&out[pos..]).unwrap();
        if h.kind == frame::Type::WindowUpdate && h.stream_id == sid(0) {
            let b = &out[pos + 9..pos + 13];
            return Some(u32::from_be_bytes([b[0], b[1], b[2], b[3]]) & 0x7fff_ffff);
        }
        pos += 9 + h.length.as_usize();
    }
    None
}

#[test]
fn initial_handshake_bumps_connection_window() {
    let conn = server();
    let target = conn.recv_window().available() as u32;
    let inc = conn_window_update_increment(conn.outbound())
        .expect("handshake emits a connection WINDOW_UPDATE");
    assert!(inc > 0);
    assert_eq!(inc, target - 65_535);
}

#[test]
fn sustained_upload_auto_replenishes_without_stall() {
    let mut conn = Conn::<ServerRole>::with_local_settings(
        Settings {
            initial_window_size: 40_000,
            ..Settings::DEFAULT
        },
        40_000,
    )
    .unwrap();
    prime_server(&mut conn);

    let block = encode_hpack(&[
        Header {
            name: b":method",
            value: b"POST",
        },
        Header {
            name: b":scheme",
            value: b"http",
        },
        Header {
            name: b":path",
            value: b"/",
        },
        Header {
            name: b":authority",
            value: b"x",
        },
    ]);
    conn.ingest(&headers_frame_bytes(1, false, true, &block))
        .unwrap();
    let _ = conn.poll_event().unwrap();
    conn.drain_outbound(conn.outbound().len());

    let mut delivered = 0usize;
    for _ in 0..20 {
        let chunk = vec![7u8; 10_000];
        conn.ingest(&data_frame_bytes(1, false, &chunk)).unwrap();
        while let Some(ev) = conn.poll_event() {
            if let conn::Event::Data { data, .. } = ev {
                delivered += data.len();
            }
        }
    }
    assert_eq!(delivered, 200_000);
    assert!(conn_window_update_increment(conn.outbound()).is_some());
}

#[test]
fn data_end_stream_to_half_closed_remote() {
    let mut conn = server();
    prime_server(&mut conn);

    let block = encode_hpack(&[
        Header {
            name: b":method",
            value: b"GET",
        },
        Header {
            name: b":scheme",
            value: b"http",
        },
        Header {
            name: b":path",
            value: b"/",
        },
        Header {
            name: b":authority",
            value: b"x",
        },
    ]);
    conn.ingest(&headers_frame_bytes(1, false, true, &block))
        .unwrap();
    let _ = conn.poll_event().unwrap();
    conn.ingest(&data_frame_bytes(1, true, b"x")).unwrap();
    let _ = conn.poll_event().unwrap();
    assert_eq!(
        conn.stream_state(sid(1)),
        Some(stream::State::HalfClosedRemote)
    );
}

#[test]
fn data_exceeding_recv_window_flow_control() {
    let mut local = Settings::DEFAULT;
    local.max_frame_size = 16_777_215;
    let mut conn = Conn::<ServerRole>::with_local_settings(local, 65_535).unwrap();
    conn.drain_outbound(conn.outbound().len());
    conn.ingest(CLIENT_PREFACE).unwrap();
    while conn.poll_event().is_some() {}

    let block = encode_hpack(&[
        Header {
            name: b":method",
            value: b"GET",
        },
        Header {
            name: b":scheme",
            value: b"http",
        },
        Header {
            name: b":path",
            value: b"/",
        },
        Header {
            name: b":authority",
            value: b"x",
        },
    ]);
    conn.ingest(&headers_frame_bytes(1, false, true, &block))
        .unwrap();
    let _ = conn.poll_event().unwrap();
    conn.drain_outbound(conn.outbound().len());

    let huge = vec![0u8; 65_536];
    let err = conn.ingest(&data_frame_bytes(1, false, &huge)).unwrap_err();
    assert_eq!(err, ConnError::FlowControl);
    conn.resume().unwrap();
}

#[test]
fn frame_exceeding_max_frame_size_errors() {
    let mut conn = server();
    prime_server(&mut conn);

    let mut bytes = Vec::new();
    let big_len: u32 = 16_385;
    FrameHeader {
        length: flen(big_len),
        kind: frame::Type::Data,
        flags: sark_h2::Flags(0),
        stream_id: sid(1),
    }
    .encode(&mut bytes);
    bytes.extend_from_slice(&vec![0u8; big_len as usize]);
    let err = conn.ingest(&bytes).unwrap_err();
    assert_eq!(err, ConnError::FrameSize);
}

#[test]
fn max_concurrent_streams_refuses_new() {
    let mut local = Settings::DEFAULT;
    local.max_concurrent_streams = Some(3);
    let mut conn = Conn::<ServerRole>::with_local_settings(local, 65_535).unwrap();
    conn.drain_outbound(conn.outbound().len());
    conn.ingest(CLIENT_PREFACE).unwrap();
    while conn.poll_event().is_some() {}

    let block = encode_hpack(&[
        Header {
            name: b":method",
            value: b"GET",
        },
        Header {
            name: b":scheme",
            value: b"http",
        },
        Header {
            name: b":path",
            value: b"/",
        },
        Header {
            name: b":authority",
            value: b"x",
        },
    ]);
    conn.ingest(&headers_frame_bytes(1, false, true, &block))
        .unwrap();
    let _ = conn.poll_event().unwrap();
    conn.ingest(&headers_frame_bytes(3, false, true, &block))
        .unwrap();
    let _ = conn.poll_event().unwrap();
    conn.ingest(&headers_frame_bytes(5, false, true, &block))
        .unwrap();
    let _ = conn.poll_event().unwrap();
    assert_eq!(conn.active_count(), 3);

    conn.drain_outbound(conn.outbound().len());
    conn.ingest(&headers_frame_bytes(7, false, true, &block))
        .unwrap();
    assert!(conn.poll_event().is_none());
    let out = conn.outbound();
    let h = FrameHeader::parse(out).unwrap();
    assert_eq!(h.kind, frame::Type::RstStream);
    assert_eq!(h.stream_id, sid(7));
    let payload = &out[9..9 + 4];
    let err = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    assert_eq!(err, ErrorCode::RefusedStream as u32);
    assert_eq!(conn.active_count(), 3);
}

#[test]
fn rst_stream_recv_removes_stream() {
    let mut conn = server();
    prime_server(&mut conn);

    let block = encode_hpack(&[
        Header {
            name: b":method",
            value: b"GET",
        },
        Header {
            name: b":scheme",
            value: b"http",
        },
        Header {
            name: b":path",
            value: b"/",
        },
        Header {
            name: b":authority",
            value: b"x",
        },
    ]);
    conn.ingest(&headers_frame_bytes(1, false, true, &block))
        .unwrap();
    let _ = conn.poll_event().unwrap();
    assert!(conn.has_stream(sid(1)));

    conn.ingest(&rst_frame_bytes(1, ErrorCode::Cancel)).unwrap();
    let ev = conn.poll_event().unwrap();
    match ev {
        conn::Event::StreamReset { stream_id, error } => {
            assert_eq!(stream_id, sid(1));
            assert_eq!(error, ErrorCode::Cancel);
        }
        _ => panic!("expected StreamReset"),
    }
    assert!(!conn.has_stream(sid(1)));
}

#[test]
fn client_start_request_yields_first_local_stream_id() {
    let mut conn = client();
    prime_client(&mut conn);

    let id = conn
        .start_request(
            &[
                Header {
                    name: b":method",
                    value: b"GET",
                },
                Header {
                    name: b":scheme",
                    value: b"http",
                },
                Header {
                    name: b":path",
                    value: b"/",
                },
                Header {
                    name: b":authority",
                    value: b"x",
                },
            ],
            true,
        )
        .unwrap();
    assert_eq!(id, sid(1));
    let out = conn.outbound();
    let h = FrameHeader::parse(out).unwrap();
    assert_eq!(h.kind, frame::Type::Headers);
    assert_eq!(h.stream_id, sid(1));
    assert_eq!(
        conn.stream_state(sid(1)),
        Some(stream::State::HalfClosedLocal)
    );
}

#[test]
fn client_start_request_second_stream_id_step_two() {
    let mut conn = client();
    prime_client(&mut conn);

    let id1 = conn
        .start_request(
            &[Header {
                name: b":method",
                value: b"GET",
            }],
            true,
        )
        .unwrap();
    let id2 = conn
        .start_request(
            &[Header {
                name: b":method",
                value: b"GET",
            }],
            true,
        )
        .unwrap();
    assert_eq!(id1, sid(1));
    assert_eq!(id2, sid(3));
}

#[test]
fn server_send_response_emits_headers() {
    let mut conn = server();
    prime_server(&mut conn);

    let block = encode_hpack(&[
        Header {
            name: b":method",
            value: b"GET",
        },
        Header {
            name: b":scheme",
            value: b"http",
        },
        Header {
            name: b":path",
            value: b"/",
        },
        Header {
            name: b":authority",
            value: b"x",
        },
    ]);
    conn.ingest(&headers_frame_bytes(1, true, true, &block))
        .unwrap();
    let _ = conn.poll_event().unwrap();
    conn.drain_outbound(conn.outbound().len());

    conn.send_response(
        sid(1),
        [Header {
            name: b":status",
            value: b"200",
        }],
        true,
    )
    .unwrap();
    let out = conn.outbound();
    let h = FrameHeader::parse(out).unwrap();
    assert_eq!(h.kind, frame::Type::Headers);
    assert_eq!(h.stream_id, sid(1));
    assert!(!conn.has_stream(sid(1)));
}

#[test]
fn send_trailers_emits_final_headers() {
    let mut conn = server();
    prime_server(&mut conn);

    conn.ingest(&headers_frame_bytes(1, true, true, &full_block()))
        .unwrap();
    let _ = conn.poll_event().unwrap();
    conn.send_response(
        sid(1),
        [Header {
            name: b":status",
            value: b"200",
        }],
        false,
    )
    .unwrap();
    conn.drain_outbound(conn.outbound().len());

    conn.send_trailers(
        sid(1),
        &[Header {
            name: b"grpc-status",
            value: b"0",
        }],
    )
    .unwrap();

    let h = FrameHeader::parse(conn.outbound()).unwrap();
    assert_eq!(h.kind, frame::Type::Headers);
    assert_eq!(h.stream_id, sid(1));
    assert!(!conn.has_stream(sid(1)));
}

#[test]
fn client_send_trailers_closes_request_body() {
    let mut conn = client();
    prime_client(&mut conn);

    let id = conn
        .start_request(
            &[
                Header {
                    name: b":method",
                    value: b"POST",
                },
                Header {
                    name: b":scheme",
                    value: b"http",
                },
                Header {
                    name: b":path",
                    value: b"/svc/method",
                },
                Header {
                    name: b":authority",
                    value: b"x",
                },
            ],
            false,
        )
        .unwrap();
    conn.drain_outbound(conn.outbound().len());

    conn.send_trailers(
        id,
        &[Header {
            name: b"x-client-trailer",
            value: b"v",
        }],
    )
    .unwrap();

    let h = FrameHeader::parse(conn.outbound()).unwrap();
    assert_eq!(h.kind, frame::Type::Headers);
    assert_eq!(h.stream_id, id);
    assert_eq!(conn.stream_state(id), Some(stream::State::HalfClosedLocal));
}

#[test]
fn invalid_client_trailers_do_not_emit_headers() {
    let mut conn = client();
    prime_client(&mut conn);
    let id = conn
        .start_request(
            &[
                Header::new(b":method", b"POST"),
                Header::new(b":scheme", b"http"),
                Header::new(b":path", b"/"),
            ],
            true,
        )
        .unwrap();
    conn.drain_outbound(usize::MAX);

    assert_eq!(
        conn.send_trailers(id, &[Header::new(b"x-trailer", b"v")]),
        Err(ConnError::Protocol)
    );
    assert!(conn.outbound().is_empty());
    assert_eq!(conn.stream_state(id), Some(stream::State::HalfClosedLocal));
}

#[test]
fn invalid_data_does_not_consume_flow_credit() {
    let mut conn = client();
    prime_client(&mut conn);
    let id = conn
        .start_request(
            &[
                Header::new(b":method", b"POST"),
                Header::new(b":scheme", b"http"),
                Header::new(b":path", b"/"),
            ],
            true,
        )
        .unwrap();
    conn.drain_outbound(usize::MAX);
    let conn_window = conn.send_window();
    let stream_window = conn.stream_send_window(id).unwrap();

    assert_eq!(
        conn.send_data(id, b"rejected", false),
        Err(ConnError::Protocol)
    );
    assert!(conn.outbound().is_empty());
    assert_eq!(conn.send_window(), conn_window);
    assert_eq!(conn.stream_send_window(id), Some(stream_window));
    assert_eq!(conn.stream_state(id), Some(stream::State::HalfClosedLocal));
}

#[test]
fn invalid_server_response_does_not_emit_headers() {
    let mut conn = server();
    prime_server(&mut conn);
    conn.ingest(&headers_frame_bytes(1, false, true, &full_block()))
        .unwrap();
    let _ = conn.poll_event().unwrap();
    conn.send_response(sid(1), [Header::new(b":status", b"200")], true)
        .unwrap();
    conn.drain_outbound(usize::MAX);

    assert_eq!(
        conn.send_response(sid(1), [Header::new(b":status", b"204")], false),
        Err(ConnError::Protocol)
    );
    assert!(conn.outbound().is_empty());
    assert_eq!(
        conn.stream_state(sid(1)),
        Some(stream::State::HalfClosedLocal)
    );
}

#[test]
fn send_data_chunks_on_stalled_window() {
    let mut conn = client();
    prime_client(&mut conn);

    let id = conn
        .start_request(
            &[Header {
                name: b":method",
                value: b"POST",
            }],
            false,
        )
        .unwrap();
    conn.drain_outbound(conn.outbound().len());

    let to_send = vec![1u8; 80_000];
    let n1 = conn.send_data(id, &to_send, false).unwrap();
    assert!(n1 <= 16_384);
    assert!(n1 > 0);

    let _ = conn.send_data(id, &to_send[n1..], false).unwrap();
    let _ = conn.send_data(id, &to_send, false).unwrap();
}

#[test]
fn send_data_resumes_after_window_update() {
    let mut conn = client();
    prime_client(&mut conn);

    let id = conn
        .start_request(
            &[Header {
                name: b":method",
                value: b"POST",
            }],
            false,
        )
        .unwrap();
    conn.drain_outbound(conn.outbound().len());

    let mut total_sent = 0;
    let buf = vec![7u8; 100_000];
    loop {
        let n = conn.send_data(id, &buf[total_sent..], false).unwrap();
        if n == 0 {
            break;
        }
        total_sent += n;
        if total_sent >= buf.len() {
            break;
        }
    }
    assert!(total_sent <= 65_535);

    conn.ingest(&window_update_bytes(0, 100_000)).unwrap();
    conn.ingest(&window_update_bytes(id.as_u32(), 100_000))
        .unwrap();

    let n = conn.send_data(id, &buf[total_sent..], true).unwrap();
    assert!(n > 0);
}

#[test]
fn settings_iws_change_adjusts_stream_send_window() {
    let mut conn = server();
    prime_server(&mut conn);

    let block = encode_hpack(&[
        Header {
            name: b":method",
            value: b"GET",
        },
        Header {
            name: b":scheme",
            value: b"http",
        },
        Header {
            name: b":path",
            value: b"/",
        },
        Header {
            name: b":authority",
            value: b"x",
        },
    ]);
    conn.ingest(&headers_frame_bytes(1, false, true, &block))
        .unwrap();
    let _ = conn.poll_event().unwrap();
    conn.ingest(&headers_frame_bytes(3, false, true, &block))
        .unwrap();
    let _ = conn.poll_event().unwrap();

    let s1_before = conn.stream_send_window(sid(1)).unwrap().value;
    let s3_before = conn.stream_send_window(sid(3)).unwrap().value;

    let peer = settings_frame_bytes(&[(SettingId::InitialWindowSize as u16, 100_000)], false);
    conn.ingest(&peer).unwrap();

    let delta = 100_000i32 - 65_535;
    assert_eq!(
        conn.stream_send_window(sid(1)).unwrap().value,
        s1_before + delta
    );
    assert_eq!(
        conn.stream_send_window(sid(3)).unwrap().value,
        s3_before + delta
    );
}

#[test]
fn client_push_promise_recv_registers_promised_stream() {
    let mut conn = client();
    prime_client(&mut conn);

    let id = conn
        .start_request(
            &[
                Header {
                    name: b":method",
                    value: b"GET",
                },
                Header {
                    name: b":scheme",
                    value: b"http",
                },
                Header {
                    name: b":path",
                    value: b"/",
                },
                Header {
                    name: b":authority",
                    value: b"x",
                },
            ],
            true,
        )
        .unwrap();
    let _ = id;
    conn.drain_outbound(conn.outbound().len());

    let block = encode_hpack(&[
        Header {
            name: b":method",
            value: b"GET",
        },
        Header {
            name: b":scheme",
            value: b"http",
        },
        Header {
            name: b":path",
            value: b"/a",
        },
        Header {
            name: b":authority",
            value: b"x",
        },
    ]);
    conn.ingest(&push_promise_bytes(1, 2, true, &block))
        .unwrap();

    let ev = conn.poll_event().unwrap();
    match ev {
        conn::Event::PushPromise {
            stream_id,
            promised_stream_id,
            ..
        } => {
            assert_eq!(stream_id, sid(1));
            assert_eq!(promised_stream_id, sid(2));
        }
        _ => panic!("expected PushPromise"),
    }
    assert_eq!(
        conn.stream_state(sid(2)),
        Some(stream::State::ReservedRemote)
    );
}

#[test]
fn fragmented_push_promise_respects_stream_capacity() {
    let mut conn = Conn::<ClientRole>::with_config(conn::Config {
        local_settings: Settings::DEFAULT,
        recv_window_target: 65_535,
        stream_capacity: 2,
        ..conn::Config::default()
    })
    .unwrap();
    prime_client(&mut conn);
    conn.start_request(
        &[
            Header {
                name: b":method",
                value: b"GET",
            },
            Header {
                name: b":scheme",
                value: b"http",
            },
            Header {
                name: b":path",
                value: b"/",
            },
            Header {
                name: b":authority",
                value: b"x",
            },
        ],
        true,
    )
    .unwrap();
    conn.drain_outbound(conn.outbound().len());

    let block = full_block();
    conn.ingest(&push_promise_bytes(1, 2, false, &block[..1]))
        .unwrap();
    conn.ingest(&continuation_bytes(1, true, &block[1..]))
        .unwrap();
    assert_eq!(
        conn.stream_state(sid(2)),
        Some(stream::State::ReservedRemote)
    );

    while conn.poll_event().is_some() {}
    conn.ingest(&push_promise_bytes(1, 4, true, &block))
        .unwrap();
    assert_eq!(
        first_outbound_rst(conn.outbound()),
        Some((sid(4), ErrorCode::RefusedStream))
    );
    assert!(!conn.has_stream(sid(4)));
}

#[test]
fn every_hpack_boundary_streams_across_push_promise() {
    let block = encode_hpack(&[
        Header {
            name: b":method",
            value: b"GET",
        },
        Header {
            name: b":scheme",
            value: b"http",
        },
        Header {
            name: b":path",
            value: b"/promised",
        },
        Header {
            name: b":authority",
            value: b"example.com",
        },
    ]);

    for split in 0..=block.len() {
        let mut conn = client();
        prime_client(&mut conn);
        conn.start_request(&[Header::new(b":method", b"GET")], true)
            .unwrap();
        conn.drain_outbound(conn.outbound().len());

        conn.ingest(&push_promise_bytes(1, 2, false, &block[..split]))
            .unwrap();
        assert!(conn.poll_event().is_none());
        conn.ingest(&continuation_bytes(1, true, &block[split..]))
            .unwrap();

        let Some(conn::Event::PushPromise {
            promised_stream_id,
            headers,
            ..
        }) = conn.poll_event()
        else {
            panic!("expected PushPromise at HPACK boundary {split}");
        };
        assert_eq!(promised_stream_id, sid(2));
        assert!(
            headers
                .iter()
                .any(|field| { field.name == b":path" && field.value == b"/promised" })
        );
    }
}

#[test]
fn resource_capacity_is_reused_after_stream_close() {
    let mut local = Settings::DEFAULT;
    local.max_concurrent_streams = Some(10);
    let mut conn = Conn::<ServerRole>::with_config(conn::Config {
        local_settings: local,
        recv_window_target: 65_535,
        stream_capacity: 1,
        ..conn::Config::default()
    })
    .unwrap();
    prime_server(&mut conn);
    let block = full_block();

    conn.ingest(&headers_frame_bytes(1, true, true, &block))
        .unwrap();
    while conn.poll_event().is_some() {}
    conn.drain_outbound(conn.outbound().len());
    conn.ingest(&headers_frame_bytes(3, true, true, &block))
        .unwrap();
    assert_eq!(
        first_outbound_rst(conn.outbound()),
        Some((sid(3), ErrorCode::RefusedStream))
    );

    conn.send_response(
        sid(1),
        [Header {
            name: b":status",
            value: b"200",
        }],
        true,
    )
    .unwrap();
    assert_eq!(conn.active_count(), 0);
    conn.drain_outbound(conn.outbound().len());
    conn.ingest(&headers_frame_bytes(5, true, true, &block))
        .unwrap();
    assert!(conn.has_stream(sid(5)));
}

#[test]
fn peer_limit_is_reused_after_local_stream_close() {
    let mut conn = Conn::<ClientRole>::with_config(conn::Config {
        local_settings: Settings::DEFAULT,
        recv_window_target: 65_535,
        stream_capacity: 2,
        ..conn::Config::default()
    })
    .unwrap();
    prime_client(&mut conn);
    conn.ingest(&settings_frame_bytes(
        &[(SettingId::MaxConcurrentStreams as u16, 1)],
        false,
    ))
    .unwrap();
    while conn.poll_event().is_some() {}

    let request = [Header {
        name: b":method",
        value: b"GET",
    }];
    assert_eq!(conn.start_request(&request, true).unwrap(), sid(1));
    assert_eq!(
        conn.start_request(&request, true),
        Err(ConnError::StreamLimit)
    );

    let response = encode_hpack(&[Header {
        name: b":status",
        value: b"200",
    }]);
    conn.ingest(&headers_frame_bytes(1, true, true, &response))
        .unwrap();
    while conn.poll_event().is_some() {}
    assert_eq!(conn.start_request(&request, true).unwrap(), sid(3));
}

#[test]
fn zero_protocol_limit_keeps_resource_capacity_valid() {
    let mut local = Settings::DEFAULT;
    local.max_concurrent_streams = Some(0);
    let mut conn = Conn::<ServerRole>::with_config(conn::Config {
        local_settings: local,
        recv_window_target: 65_535,
        stream_capacity: 1,
        ..conn::Config::default()
    })
    .unwrap();
    prime_server(&mut conn);
    conn.ingest(&headers_frame_bytes(1, true, true, &full_block()))
        .unwrap();
    assert_eq!(conn.active_count(), 0);
    assert_eq!(
        first_outbound_rst(conn.outbound()),
        Some((sid(1), ErrorCode::RefusedStream))
    );
}

#[test]
fn goaway_last_stream_id_reflects_highest_peer_id() {
    let mut conn = server();
    prime_server(&mut conn);

    let block = encode_hpack(&[
        Header {
            name: b":method",
            value: b"GET",
        },
        Header {
            name: b":scheme",
            value: b"http",
        },
        Header {
            name: b":path",
            value: b"/",
        },
        Header {
            name: b":authority",
            value: b"x",
        },
    ]);
    conn.ingest(&headers_frame_bytes(1, true, true, &block))
        .unwrap();
    let _ = conn.poll_event().unwrap();
    conn.ingest(&headers_frame_bytes(3, true, true, &block))
        .unwrap();
    let _ = conn.poll_event().unwrap();
    conn.ingest(&headers_frame_bytes(5, true, true, &block))
        .unwrap();
    let _ = conn.poll_event().unwrap();

    conn.drain_outbound(conn.outbound().len());
    conn.goaway(ErrorCode::NoError, b"bye").unwrap();

    let out = conn.outbound();
    let h = FrameHeader::parse(out).unwrap();
    assert_eq!(h.kind, frame::Type::GoAway);
    let payload = &out[9..];
    let last = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) & 0x7fff_ffff;
    assert_eq!(last, 5);
}

#[test]
fn stream_window_update_increases_send_window() {
    let mut conn = client();
    prime_client(&mut conn);

    let id = conn
        .start_request(
            &[Header {
                name: b":method",
                value: b"POST",
            }],
            false,
        )
        .unwrap();
    let before = conn.stream_send_window(id).unwrap().value;
    conn.ingest(&window_update_bytes(id.as_u32(), 1000))
        .unwrap();
    let after = conn.stream_send_window(id).unwrap().value;
    assert_eq!(after, before + 1000);
}

#[test]
fn server_push_promise_recv_protocol_error() {
    let mut conn = server();
    prime_server(&mut conn);

    let block = encode_hpack(&[
        Header {
            name: b":method",
            value: b"GET",
        },
        Header {
            name: b":scheme",
            value: b"http",
        },
        Header {
            name: b":path",
            value: b"/",
        },
        Header {
            name: b":authority",
            value: b"x",
        },
    ]);
    conn.ingest(&headers_frame_bytes(1, false, true, &block))
        .unwrap();
    let _ = conn.poll_event().unwrap();

    let pp_block = encode_hpack(&[Header {
        name: b":path",
        value: b"/a",
    }]);
    let err = conn
        .ingest(&push_promise_bytes(1, 2, true, &pp_block))
        .unwrap_err();
    assert_eq!(err, ConnError::Protocol);
}

#[test]
fn reset_stream_user_emits_frame_and_evicts() {
    let mut conn = server();
    prime_server(&mut conn);

    let block = encode_hpack(&[
        Header {
            name: b":method",
            value: b"GET",
        },
        Header {
            name: b":scheme",
            value: b"http",
        },
        Header {
            name: b":path",
            value: b"/",
        },
        Header {
            name: b":authority",
            value: b"x",
        },
    ]);
    conn.ingest(&headers_frame_bytes(1, false, true, &block))
        .unwrap();
    let _ = conn.poll_event().unwrap();
    conn.drain_outbound(conn.outbound().len());

    conn.reset_stream(sid(1), ErrorCode::Cancel).unwrap();
    assert!(!conn.has_stream(sid(1)));
    let out = conn.outbound();
    let h = FrameHeader::parse(out).unwrap();
    assert_eq!(h.kind, frame::Type::RstStream);
}

#[test]
fn server_wrong_parity_peer_stream_protocol_error() {
    let mut conn = server();
    prime_server(&mut conn);

    let block = encode_hpack(&[
        Header {
            name: b":method",
            value: b"GET",
        },
        Header {
            name: b":scheme",
            value: b"http",
        },
        Header {
            name: b":path",
            value: b"/",
        },
        Header {
            name: b":authority",
            value: b"x",
        },
    ]);
    let err = conn
        .ingest(&headers_frame_bytes(2, false, true, &block))
        .unwrap_err();
    assert_eq!(err, ConnError::Protocol);
}

fn full_block() -> Vec<u8> {
    encode_hpack(&[
        Header {
            name: b":method",
            value: b"GET",
        },
        Header {
            name: b":scheme",
            value: b"http",
        },
        Header {
            name: b":path",
            value: b"/",
        },
        Header {
            name: b":authority",
            value: b"x",
        },
    ])
}

fn first_outbound_rst(out: &[u8]) -> Option<(StreamId, ErrorCode)> {
    let mut pos = 0;
    while pos < out.len() {
        let h = FrameHeader::parse(&out[pos..]).ok()?;
        let total = 9 + h.length.as_usize();
        if h.kind == frame::Type::RstStream {
            let payload = &out[pos + 9..pos + total];
            let err = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
            return Some((h.stream_id, ErrorCode::from_u32(err)));
        }
        pos += total;
    }
    None
}

#[test]
fn idle_stream_data_yields_connection_protocol_error() {
    let mut conn = server();
    prime_server(&mut conn);
    let err = conn.ingest(&data_frame_bytes(7, false, b"x")).unwrap_err();
    assert_eq!(err, ConnError::Protocol);
}

#[test]
fn idle_stream_rst_yields_connection_protocol_error() {
    let mut conn = server();
    prime_server(&mut conn);
    let err = conn
        .ingest(&rst_frame_bytes(7, ErrorCode::Cancel))
        .unwrap_err();
    assert_eq!(err, ConnError::Protocol);
}

#[test]
fn idle_stream_window_update_yields_connection_protocol_error() {
    let mut conn = server();
    prime_server(&mut conn);
    let err = conn.ingest(&window_update_bytes(7, 100)).unwrap_err();
    assert_eq!(err, ConnError::Protocol);
}

#[test]
fn closed_after_rst_data_emits_stream_closed_rst() {
    let mut conn = server();
    prime_server(&mut conn);
    let block = full_block();
    conn.ingest(&headers_frame_bytes(1, false, true, &block))
        .unwrap();
    let _ = conn.poll_event().unwrap();
    conn.reset_stream(sid(1), ErrorCode::Cancel).unwrap();
    assert!(!conn.has_stream(sid(1)));
    conn.drain_outbound(conn.outbound().len());

    conn.ingest(&data_frame_bytes(1, false, b"x")).unwrap();
    let rst = first_outbound_rst(conn.outbound()).expect("RST emitted");
    assert_eq!(rst, (sid(1), ErrorCode::StreamClosed));
}

#[test]
fn closed_after_rst_headers_emits_stream_closed_rst() {
    let mut conn = server();
    prime_server(&mut conn);
    let block = full_block();
    conn.ingest(&headers_frame_bytes(1, false, true, &block))
        .unwrap();
    let _ = conn.poll_event().unwrap();
    conn.reset_stream(sid(1), ErrorCode::Cancel).unwrap();
    assert!(!conn.has_stream(sid(1)));
    conn.drain_outbound(conn.outbound().len());

    conn.ingest(&headers_frame_bytes(1, true, true, &block))
        .unwrap();
    let rst = first_outbound_rst(conn.outbound()).expect("RST emitted");
    assert_eq!(rst, (sid(1), ErrorCode::StreamClosed));
}

#[test]
fn closed_after_end_stream_data_yields_connection_stream_closed_error() {
    let mut conn = server();
    prime_server(&mut conn);
    let block = full_block();
    conn.ingest(&headers_frame_bytes(1, true, true, &block))
        .unwrap();
    let _ = conn.poll_event().unwrap();
    conn.send_response(
        sid(1),
        [Header {
            name: b":status",
            value: b"200",
        }],
        true,
    )
    .unwrap();
    assert!(!conn.has_stream(sid(1)));
    conn.drain_outbound(conn.outbound().len());

    let err = conn.ingest(&data_frame_bytes(1, false, b"x")).unwrap_err();
    assert_eq!(err, ConnError::StreamClosed);
    conn.resume().unwrap();
}

#[test]
fn closed_after_end_stream_headers_yields_connection_stream_closed_error() {
    let mut conn = server();
    prime_server(&mut conn);
    let block = full_block();
    conn.ingest(&headers_frame_bytes(1, true, true, &block))
        .unwrap();
    let _ = conn.poll_event().unwrap();
    conn.send_response(
        sid(1),
        [Header {
            name: b":status",
            value: b"200",
        }],
        true,
    )
    .unwrap();
    assert!(!conn.has_stream(sid(1)));
    conn.drain_outbound(conn.outbound().len());

    let err = conn
        .ingest(&headers_frame_bytes(1, true, true, &block))
        .unwrap_err();
    assert_eq!(err, ConnError::StreamClosed);
}

#[test]
fn closed_stream_rst_ignored() {
    let mut conn = server();
    prime_server(&mut conn);
    let block = full_block();
    conn.ingest(&headers_frame_bytes(1, false, true, &block))
        .unwrap();
    let _ = conn.poll_event().unwrap();
    conn.ingest(&rst_frame_bytes(1, ErrorCode::Cancel)).unwrap();
    let _ = conn.poll_event().unwrap();
    assert!(!conn.has_stream(sid(1)));
    conn.drain_outbound(conn.outbound().len());

    conn.ingest(&rst_frame_bytes(1, ErrorCode::Cancel)).unwrap();
    assert!(conn.poll_event().is_none());
    assert!(conn.outbound().is_empty());
}

#[test]
fn half_closed_remote_data_emits_stream_closed_rst() {
    let mut conn = server();
    prime_server(&mut conn);
    let block = full_block();
    conn.ingest(&headers_frame_bytes(1, true, true, &block))
        .unwrap();
    let _ = conn.poll_event().unwrap();
    assert_eq!(
        conn.stream_state(sid(1)),
        Some(stream::State::HalfClosedRemote)
    );
    conn.drain_outbound(conn.outbound().len());

    conn.ingest(&data_frame_bytes(1, false, b"x")).unwrap();
    let rst = first_outbound_rst(conn.outbound()).expect("RST emitted");
    assert_eq!(rst, (sid(1), ErrorCode::StreamClosed));
    assert!(!conn.has_stream(sid(1)));
}

#[test]
fn half_closed_remote_headers_emits_stream_closed_rst() {
    let mut conn = server();
    prime_server(&mut conn);
    let block = full_block();
    conn.ingest(&headers_frame_bytes(1, true, true, &block))
        .unwrap();
    let _ = conn.poll_event().unwrap();
    assert_eq!(
        conn.stream_state(sid(1)),
        Some(stream::State::HalfClosedRemote)
    );
    conn.drain_outbound(conn.outbound().len());

    let trailing = encode_hpack(&[Header {
        name: b"x-trailer",
        value: b"v",
    }]);
    conn.ingest(&headers_frame_bytes(1, true, true, &trailing))
        .unwrap();
    let rst = first_outbound_rst(conn.outbound()).expect("RST emitted");
    assert_eq!(rst, (sid(1), ErrorCode::StreamClosed));
    assert!(!conn.has_stream(sid(1)));
}

#[test]
fn rst_with_bad_length_yields_frame_size_error() {
    let mut conn = server();
    prime_server(&mut conn);
    let mut bytes = Vec::new();
    FrameHeader {
        length: flen(3),
        kind: frame::Type::RstStream,
        flags: sark_h2::Flags(0),
        stream_id: sid(1),
    }
    .encode(&mut bytes);
    bytes.extend_from_slice(&[0u8; 3]);
    let err = conn.ingest(&bytes).unwrap_err();
    assert!(matches!(
        err,
        ConnError::ParseError(frame::ParseError::FrameSize)
    ));
}

#[test]
fn continuation_flood_exceeding_header_cap_rejected() {
    let mut conn = server();
    prime_server(&mut conn);
    let cap = conn.local_settings().max_header_list_size.unwrap() as usize;

    let first = vec![0u8; cap - 1];
    conn.ingest(&headers_frame_bytes(1, false, false, &first))
        .unwrap();

    let flood = vec![0u8; 4096];
    let err = conn
        .ingest(&continuation_bytes(1, false, &flood))
        .unwrap_err();
    assert_eq!(err, ConnError::HeaderListTooLarge);
}

#[test]
fn post_goaway_peer_headers_refused_not_fatal() {
    let mut conn = server();
    prime_server(&mut conn);
    conn.goaway(ErrorCode::NoError, b"").unwrap();
    conn.drain_outbound(conn.outbound().len());

    let block = full_block();
    conn.ingest(&headers_frame_bytes(3, false, true, &block))
        .unwrap();

    assert!(!conn.has_stream(sid(3)));
    assert_eq!(
        first_outbound_rst(conn.outbound()),
        Some((sid(3), ErrorCode::RefusedStream))
    );
}

mod hardening {
    use sark_h2::frame::Flags;

    use super::*;

    #[test]
    fn rapid_reset_flood_triggers_overload() {
        let mut conn = server();
        prime_server(&mut conn);
        let block = full_block();
        let mut id = 1u32;
        let mut triggered = false;
        for _ in 0..200 {
            let mut bytes = headers_frame_bytes(id, false, true, &block);
            bytes.extend_from_slice(&rst_frame_bytes(id, ErrorCode::Cancel));
            match conn.ingest(&bytes) {
                Ok(()) => {
                    while conn.poll_event().is_some() {}
                    conn.drain_outbound(conn.outbound().len());
                }
                Err(e) => {
                    assert_eq!(e, ConnError::Overload);
                    triggered = true;
                    break;
                }
            }
            id += 2;
        }
        assert!(triggered, "rapid HEADERS+RST flood must trigger Overload");
    }

    #[test]
    fn ping_flood_without_draining_outbound_is_bounded() {
        let mut conn = server();
        prime_server(&mut conn);
        conn.drain_outbound(conn.outbound().len());
        let ping = ping_frame_bytes([1, 2, 3, 4, 5, 6, 7, 8]);
        let mut triggered = false;
        for _ in 0..400_000 {
            match conn.ingest(&ping) {
                Ok(()) => while conn.poll_event().is_some() {},
                Err(e) => {
                    assert_eq!(e, ConnError::Overload);
                    triggered = true;
                    break;
                }
            }
        }
        assert!(
            triggered,
            "PING flood with a non-draining peer must force-close"
        );
        assert!(
            conn.outbound().len() <= (1 << 20) + 17,
            "outbound must stay bounded, got {}",
            conn.outbound().len()
        );
    }

    #[test]
    fn completed_streams_leave_tracking_bounded() {
        let mut conn = server();
        prime_server(&mut conn);
        let block = full_block();
        let mut id = 1u32;
        for _ in 0..1000 {
            conn.ingest(&headers_frame_bytes(id, true, true, &block))
                .unwrap();
            while conn.poll_event().is_some() {}
            conn.send_response(
                sid(id),
                [Header {
                    name: b":status",
                    value: b"200",
                }],
                true,
            )
            .unwrap();
            conn.drain_outbound(conn.outbound().len());
            id += 2;
        }
        assert_eq!(conn.active_count(), 0);
        assert_eq!(
            conn.tracked_closed_count(),
            0,
            "normally-completed streams must not accumulate in tracking state"
        );
    }

    #[test]
    fn oversized_declared_frame_rejected_before_buffering() {
        let mut conn = server();
        prime_server(&mut conn);
        let mut hdr = Vec::new();
        FrameHeader {
            length: flen(16_777_000),
            kind: frame::Type::Data,
            flags: Flags(0),
            stream_id: sid(1),
        }
        .encode(&mut hdr);
        let err = conn.ingest(&hdr).unwrap_err();
        assert_eq!(err, ConnError::FrameSize);
    }

    #[test]
    fn oversized_unknown_frame_rejected_before_buffering() {
        let mut conn = server();
        prime_server(&mut conn);
        let big: u32 = 16_777_000;
        let mut hdr = vec![(big >> 16) as u8, (big >> 8) as u8, big as u8, 0xFF, 0x00];
        hdr.extend_from_slice(&0u32.to_be_bytes());
        let err = conn.ingest(&hdr).unwrap_err();
        assert_eq!(err, ConnError::FrameSize);
    }

    #[test]
    fn empty_non_final_continuation_rejected() {
        let mut conn = server();
        prime_server(&mut conn);
        let block = full_block();
        conn.ingest(&headers_frame_bytes(1, false, false, &block))
            .unwrap();
        let err = conn.ingest(&continuation_bytes(1, false, &[])).unwrap_err();
        assert_eq!(err, ConnError::Continuation);
    }

    #[test]
    fn continuation_frame_count_cap_rejected() {
        let mut conn = server();
        prime_server(&mut conn);
        conn.ingest(&headers_frame_bytes(1, false, false, &[]))
            .unwrap();
        let mut err = None;
        for _ in 0..200 {
            match conn.ingest(&continuation_bytes(1, false, &[0x00])) {
                Ok(()) => {}
                Err(e) => {
                    err = Some(e);
                    break;
                }
            }
        }
        assert_eq!(err, Some(ConnError::Overload));
    }

    #[test]
    fn default_server_enforces_finite_stream_limit() {
        let mut conn = server();
        prime_server(&mut conn);
        assert_eq!(conn.local_settings().max_concurrent_streams, Some(256));
        let block = full_block();
        let mut id = 1u32;
        for _ in 0..256 {
            conn.ingest(&headers_frame_bytes(id, false, true, &block))
                .unwrap();
            while conn.poll_event().is_some() {}
            id += 2;
        }
        assert_eq!(conn.active_count(), 256);
        conn.drain_outbound(conn.outbound().len());
        conn.ingest(&headers_frame_bytes(id, false, true, &block))
            .unwrap();
        let rst = first_outbound_rst(conn.outbound()).expect("RST emitted for refused stream");
        assert_eq!(rst.1, ErrorCode::RefusedStream);
        assert_eq!(conn.active_count(), 256);
    }
}
