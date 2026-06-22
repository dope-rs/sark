use sark_h2::frame::{
    Continuation as ContinuationFrame, Data as DataFrame, Headers as HeadersFrame,
    PushPromise as PushPromiseFrame, RstStream as RstStreamFrame, SettingId,
    Settings as SettingsFrame, WindowUpdate as WindowUpdateFrame,
};
use sark_h2::hpack::{Encoder, Header};
use sark_h2::{
    CLIENT_PREFACE, ClientRole, Conn, ConnError, ErrorCode, FrameHeader, ServerRole, Settings,
    StreamId, conn, frame, stream,
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
    SettingsFrame {
        ack,
        params: &payload,
    }
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
    HeadersFrame {
        stream_id: StreamId(stream_id),
        end_stream,
        end_headers,
        priority: None,
        block_fragment: block,
    }
    .encode(&mut out);
    out
}

fn continuation_bytes(stream_id: u32, end_headers: bool, block: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    ContinuationFrame {
        stream_id: StreamId(stream_id),
        end_headers,
        block_fragment: block,
    }
    .encode(&mut out);
    out
}

fn data_frame_bytes(stream_id: u32, end_stream: bool, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    DataFrame {
        stream_id: StreamId(stream_id),
        end_stream,
        payload,
    }
    .encode(&mut out);
    out
}

fn rst_frame_bytes(stream_id: u32, error: ErrorCode) -> Vec<u8> {
    let mut out = Vec::new();
    RstStreamFrame {
        stream_id: StreamId(stream_id),
        error,
    }
    .encode(&mut out);
    out
}

fn push_promise_bytes(stream_id: u32, promised: u32, end_headers: bool, block: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    PushPromiseFrame {
        stream_id: StreamId(stream_id),
        promised_stream_id: StreamId(promised),
        end_headers,
        block_fragment: block,
    }
    .encode(&mut out);
    out
}

fn window_update_bytes(stream_id: u32, increment: u32) -> Vec<u8> {
    let mut out = Vec::new();
    WindowUpdateFrame {
        stream_id: StreamId(stream_id),
        increment,
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
        } => {
            assert_eq!(stream_id, StreamId(1));
            assert!(!end_stream);
            assert!(!trailing);
            assert!(headers.iter().any(|h| h.name == b":method"));
        }
        _ => panic!("expected Headers"),
    }
    assert_eq!(conn.stream_state(StreamId(1)), Some(stream::State::Open));
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
        conn.stream_state(StreamId(1)),
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
            assert_eq!(stream_id, StreamId(1));
            assert!(headers.iter().any(|h| h.name == b":method"));
        }
        _ => panic!("expected Headers"),
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

    let payload = b"hello world!";
    conn.ingest(&data_frame_bytes(1, false, payload)).unwrap();
    let ev = conn.poll_event().unwrap();
    match ev {
        conn::Event::Data {
            stream_id,
            data,
            end_stream,
        } => {
            assert_eq!(stream_id, StreamId(1));
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
            pos += 9 + h.length as usize;
        }
        wu
    };

    assert_eq!(count_wu(conn.outbound()), 0);

    conn.release_capacity(StreamId(1), payload.len()).unwrap();
    assert_eq!(count_wu(conn.outbound()), 0);

    conn.drain_outbound(conn.outbound().len());
    for _ in 0..3 {
        let chunk = vec![0u8; 12_000];
        conn.ingest(&data_frame_bytes(1, false, &chunk)).unwrap();
        let _ = conn.poll_event().unwrap();
        conn.release_capacity(StreamId(1), chunk.len()).unwrap();
    }
    assert_eq!(count_wu(conn.outbound()), 2);
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
        conn.stream_state(StreamId(1)),
        Some(stream::State::HalfClosedRemote)
    );
}

#[test]
fn data_exceeding_recv_window_flow_control() {
    let mut local = Settings::DEFAULT;
    local.max_frame_size = 16_777_215;
    let mut conn = Conn::<ServerRole>::with_local_settings(local);
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
}

#[test]
fn frame_exceeding_max_frame_size_errors() {
    let mut conn = server();
    prime_server(&mut conn);

    let mut bytes = Vec::new();
    let big_len: u32 = 16_385;
    FrameHeader {
        length: big_len,
        kind: frame::Type::Data,
        flags: sark_h2::Flags(0),
        stream_id: StreamId(1),
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
    let mut conn = Conn::<ServerRole>::with_local_settings(local);
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
    assert_eq!(h.stream_id, StreamId(7));
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
    assert!(conn.has_stream(StreamId(1)));

    conn.ingest(&rst_frame_bytes(1, ErrorCode::Cancel)).unwrap();
    let ev = conn.poll_event().unwrap();
    match ev {
        conn::Event::StreamReset { stream_id, error } => {
            assert_eq!(stream_id, StreamId(1));
            assert_eq!(error, ErrorCode::Cancel);
        }
        _ => panic!("expected StreamReset"),
    }
    assert!(!conn.has_stream(StreamId(1)));
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
    assert_eq!(id, StreamId(1));
    let out = conn.outbound();
    let h = FrameHeader::parse(out).unwrap();
    assert_eq!(h.kind, frame::Type::Headers);
    assert_eq!(h.stream_id, StreamId(1));
    assert_eq!(
        conn.stream_state(StreamId(1)),
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
    assert_eq!(id1, StreamId(1));
    assert_eq!(id2, StreamId(3));
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
        StreamId(1),
        &[Header {
            name: b":status",
            value: b"200",
        }],
        true,
    )
    .unwrap();
    let out = conn.outbound();
    let h = FrameHeader::parse(out).unwrap();
    assert_eq!(h.kind, frame::Type::Headers);
    assert_eq!(h.stream_id, StreamId(1));
    assert!(!conn.has_stream(StreamId(1)));
}

#[test]
fn send_trailers_emits_final_headers() {
    let mut conn = server();
    prime_server(&mut conn);

    conn.ingest(&headers_frame_bytes(1, true, true, &full_block()))
        .unwrap();
    let _ = conn.poll_event().unwrap();
    conn.send_response(
        StreamId(1),
        &[Header {
            name: b":status",
            value: b"200",
        }],
        false,
    )
    .unwrap();
    conn.drain_outbound(conn.outbound().len());

    conn.send_trailers(
        StreamId(1),
        &[Header {
            name: b"grpc-status",
            value: b"0",
        }],
    )
    .unwrap();

    let h = FrameHeader::parse(conn.outbound()).unwrap();
    assert_eq!(h.kind, frame::Type::Headers);
    assert_eq!(h.stream_id, StreamId(1));
    assert!(!conn.has_stream(StreamId(1)));
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
    conn.ingest(&window_update_bytes(id.0, 100_000)).unwrap();

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

    let s1_before = conn.stream_send_window(StreamId(1)).unwrap().value;
    let s3_before = conn.stream_send_window(StreamId(3)).unwrap().value;

    let peer = settings_frame_bytes(&[(SettingId::InitialWindowSize as u16, 100_000)], false);
    conn.ingest(&peer).unwrap();

    let delta = 100_000i32 - 65_535;
    assert_eq!(
        conn.stream_send_window(StreamId(1)).unwrap().value,
        s1_before + delta
    );
    assert_eq!(
        conn.stream_send_window(StreamId(3)).unwrap().value,
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
            assert_eq!(stream_id, StreamId(1));
            assert_eq!(promised_stream_id, StreamId(2));
        }
        _ => panic!("expected PushPromise"),
    }
    assert_eq!(
        conn.stream_state(StreamId(2)),
        Some(stream::State::ReservedRemote)
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
    conn.goaway(ErrorCode::NoError, b"bye");

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
    conn.ingest(&window_update_bytes(id.0, 1000)).unwrap();
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

    conn.reset_stream(StreamId(1), ErrorCode::Cancel).unwrap();
    assert!(!conn.has_stream(StreamId(1)));
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
        let total = 9 + h.length as usize;
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
    conn.reset_stream(StreamId(1), ErrorCode::Cancel).unwrap();
    assert!(!conn.has_stream(StreamId(1)));
    conn.drain_outbound(conn.outbound().len());

    conn.ingest(&data_frame_bytes(1, false, b"x")).unwrap();
    let rst = first_outbound_rst(conn.outbound()).expect("RST emitted");
    assert_eq!(rst, (StreamId(1), ErrorCode::StreamClosed));
}

#[test]
fn closed_after_rst_headers_emits_stream_closed_rst() {
    let mut conn = server();
    prime_server(&mut conn);
    let block = full_block();
    conn.ingest(&headers_frame_bytes(1, false, true, &block))
        .unwrap();
    let _ = conn.poll_event().unwrap();
    conn.reset_stream(StreamId(1), ErrorCode::Cancel).unwrap();
    assert!(!conn.has_stream(StreamId(1)));
    conn.drain_outbound(conn.outbound().len());

    conn.ingest(&headers_frame_bytes(1, true, true, &block))
        .unwrap();
    let rst = first_outbound_rst(conn.outbound()).expect("RST emitted");
    assert_eq!(rst, (StreamId(1), ErrorCode::StreamClosed));
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
        StreamId(1),
        &[Header {
            name: b":status",
            value: b"200",
        }],
        true,
    )
    .unwrap();
    assert!(!conn.has_stream(StreamId(1)));
    conn.drain_outbound(conn.outbound().len());

    let err = conn.ingest(&data_frame_bytes(1, false, b"x")).unwrap_err();
    assert_eq!(err, ConnError::StreamClosed);
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
        StreamId(1),
        &[Header {
            name: b":status",
            value: b"200",
        }],
        true,
    )
    .unwrap();
    assert!(!conn.has_stream(StreamId(1)));
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
    assert!(!conn.has_stream(StreamId(1)));
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
        conn.stream_state(StreamId(1)),
        Some(stream::State::HalfClosedRemote)
    );
    conn.drain_outbound(conn.outbound().len());

    conn.ingest(&data_frame_bytes(1, false, b"x")).unwrap();
    let rst = first_outbound_rst(conn.outbound()).expect("RST emitted");
    assert_eq!(rst, (StreamId(1), ErrorCode::StreamClosed));
    assert!(!conn.has_stream(StreamId(1)));
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
        conn.stream_state(StreamId(1)),
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
    assert_eq!(rst, (StreamId(1), ErrorCode::StreamClosed));
    assert!(!conn.has_stream(StreamId(1)));
}

#[test]
fn rst_with_bad_length_yields_frame_size_error() {
    let mut conn = server();
    prime_server(&mut conn);
    let mut bytes = Vec::new();
    FrameHeader {
        length: 3,
        kind: frame::Type::RstStream,
        flags: sark_h2::Flags(0),
        stream_id: StreamId(1),
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
    conn.goaway(ErrorCode::NoError, b"");
    conn.drain_outbound(conn.outbound().len());

    let block = full_block();
    conn.ingest(&headers_frame_bytes(3, false, true, &block))
        .unwrap();

    assert!(!conn.has_stream(StreamId(3)));
    assert_eq!(
        first_outbound_rst(conn.outbound()),
        Some((StreamId(3), ErrorCode::RefusedStream))
    );
}
