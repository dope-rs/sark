use sark_h2::frame::{
    self, Continuation as ContinuationFrame, Data as DataFrame, Headers as HeadersFrame,
};
use sark_h2::hpack::{Encoder, Header};
use sark_h2::{
    CLIENT_PREFACE, ClientRole, Conn, ErrorCode, FrameHeader, ServerRole, Settings, StreamId, conn,
};

fn server() -> Conn<ServerRole> {
    Conn::<ServerRole>::new()
}

fn client() -> Conn<ClientRole> {
    Conn::<ClientRole>::new()
}

fn client_with_header_list_limit(max: u32) -> Conn<ClientRole> {
    Conn::<ClientRole>::with_local_settings(Settings {
        max_header_list_size: Some(max),
        ..Settings::DEFAULT
    })
}

fn server_with_header_list_limit(max: u32) -> Conn<ServerRole> {
    Conn::<ServerRole>::with_local_settings(Settings {
        max_header_list_size: Some(max),
        ..Settings::DEFAULT
    })
}

fn prime_server(conn: &mut Conn<ServerRole>) {
    conn.drain_outbound(conn.outbound().len());
    conn.ingest(CLIENT_PREFACE).unwrap();
    while conn.poll_event().is_some() {}
    conn.drain_outbound(conn.outbound().len());
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

fn drain_send_headers_request(headers: &[Header<'_>], stream_id: u32) -> Vec<u8> {
    let block = encode_hpack(headers);
    headers_frame_bytes(stream_id, true, true, &block)
}

fn full_req(extra: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut hdrs: Vec<Header<'_>> = vec![
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
    ];
    for (n, v) in extra {
        hdrs.push(Header { name: n, value: v });
    }
    encode_hpack(&hdrs)
}

fn full_req_header_list_size(extra: &[(&[u8], &[u8])]) -> u32 {
    let base = [
        (b":method".as_slice(), b"GET".as_slice()),
        (b":scheme".as_slice(), b"http".as_slice()),
        (b":path".as_slice(), b"/".as_slice()),
        (b":authority".as_slice(), b"x".as_slice()),
    ];
    base.into_iter()
        .chain(extra.iter().copied())
        .map(|(n, v)| n.len() + v.len() + 32)
        .sum::<usize>() as u32
}

#[test]
fn req_full_pseudo_set_accepted() {
    let mut conn = server();
    prime_server(&mut conn);
    let frame = headers_frame_bytes(1, true, true, &full_req(&[]));
    conn.ingest(&frame).unwrap();
    let ev = conn.poll_event().expect("Headers event");
    assert!(matches!(ev, conn::Event::Headers { .. }));
    assert!(first_outbound_rst(conn.outbound()).is_none());
}

#[test]
fn req_header_list_size_at_limit_accepted() {
    let max = full_req_header_list_size(&[]);
    let mut conn = server_with_header_list_limit(max);
    prime_server(&mut conn);
    let frame = headers_frame_bytes(1, true, true, &full_req(&[]));
    conn.ingest(&frame).unwrap();
    let ev = conn.poll_event().expect("Headers event");
    assert!(matches!(ev, conn::Event::Headers { .. }));
    assert!(first_outbound_rst(conn.outbound()).is_none());
}

#[test]
fn req_header_list_size_over_limit_rejected() {
    let max = full_req_header_list_size(&[]) - 1;
    let mut conn = server_with_header_list_limit(max);
    prime_server(&mut conn);
    let frame = headers_frame_bytes(1, true, true, &full_req(&[]));
    conn.ingest(&frame).unwrap();
    assert!(conn.poll_event().is_none());
    let rst = first_outbound_rst(conn.outbound()).expect("RST_STREAM emitted");
    assert_eq!(rst, (StreamId(1), ErrorCode::ProtocolError));
    assert!(!conn.has_stream(StreamId(1)));
}

#[test]
fn req_uppercase_name_rejected() {
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
        Header {
            name: b"Host",
            value: b"x",
        },
    ]);
    let frame = headers_frame_bytes(1, true, true, &block);
    conn.ingest(&frame).unwrap();
    assert!(conn.poll_event().is_none());
    let rst = first_outbound_rst(conn.outbound()).expect("RST_STREAM emitted");
    assert_eq!(rst, (StreamId(1), ErrorCode::ProtocolError));
}

#[test]
fn req_missing_method_rejected() {
    let mut conn = server();
    prime_server(&mut conn);
    let block = encode_hpack(&[
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
    assert!(conn.poll_event().is_none());
    let rst = first_outbound_rst(conn.outbound()).expect("RST_STREAM emitted");
    assert_eq!(rst, (StreamId(1), ErrorCode::ProtocolError));
}

#[test]
fn req_missing_scheme_rejected() {
    let mut conn = server();
    prime_server(&mut conn);
    let block = encode_hpack(&[
        Header {
            name: b":method",
            value: b"GET",
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
    assert!(conn.poll_event().is_none());
    let rst = first_outbound_rst(conn.outbound()).expect("RST_STREAM emitted");
    assert_eq!(rst, (StreamId(1), ErrorCode::ProtocolError));
}

#[test]
fn req_missing_path_rejected() {
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
            name: b":authority",
            value: b"x",
        },
    ]);
    let frame = headers_frame_bytes(1, true, true, &block);
    conn.ingest(&frame).unwrap();
    assert!(conn.poll_event().is_none());
    let rst = first_outbound_rst(conn.outbound()).expect("RST_STREAM emitted");
    assert_eq!(rst, (StreamId(1), ErrorCode::ProtocolError));
}

#[test]
fn req_empty_path_rejected() {
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
            value: b"",
        },
        Header {
            name: b":authority",
            value: b"x",
        },
    ]);
    let frame = headers_frame_bytes(1, true, true, &block);
    conn.ingest(&frame).unwrap();
    assert!(conn.poll_event().is_none());
    let rst = first_outbound_rst(conn.outbound()).expect("RST_STREAM emitted");
    assert_eq!(rst, (StreamId(1), ErrorCode::ProtocolError));
}

#[test]
fn req_duplicate_method_rejected() {
    let mut conn = server();
    prime_server(&mut conn);
    let block = encode_hpack(&[
        Header {
            name: b":method",
            value: b"GET",
        },
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
    let frame = headers_frame_bytes(1, true, true, &block);
    conn.ingest(&frame).unwrap();
    assert!(conn.poll_event().is_none());
    let rst = first_outbound_rst(conn.outbound()).expect("RST_STREAM emitted");
    assert_eq!(rst, (StreamId(1), ErrorCode::ProtocolError));
}

#[test]
fn req_duplicate_scheme_rejected() {
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
            name: b":scheme",
            value: b"https",
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
    assert!(conn.poll_event().is_none());
    let rst = first_outbound_rst(conn.outbound()).expect("RST_STREAM emitted");
    assert_eq!(rst, (StreamId(1), ErrorCode::ProtocolError));
}

#[test]
fn req_duplicate_path_rejected() {
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
            name: b":path",
            value: b"/x",
        },
        Header {
            name: b":authority",
            value: b"x",
        },
    ]);
    let frame = headers_frame_bytes(1, true, true, &block);
    conn.ingest(&frame).unwrap();
    assert!(conn.poll_event().is_none());
    let rst = first_outbound_rst(conn.outbound()).expect("RST_STREAM emitted");
    assert_eq!(rst, (StreamId(1), ErrorCode::ProtocolError));
}

#[test]
fn req_unknown_pseudo_rejected() {
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
            name: b":foo",
            value: b"bar",
        },
    ]);
    let frame = headers_frame_bytes(1, true, true, &block);
    conn.ingest(&frame).unwrap();
    assert!(conn.poll_event().is_none());
    let rst = first_outbound_rst(conn.outbound()).expect("RST_STREAM emitted");
    assert_eq!(rst, (StreamId(1), ErrorCode::ProtocolError));
}

#[test]
fn req_status_pseudo_rejected() {
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
            name: b":status",
            value: b"200",
        },
    ]);
    let frame = headers_frame_bytes(1, true, true, &block);
    conn.ingest(&frame).unwrap();
    assert!(conn.poll_event().is_none());
    let rst = first_outbound_rst(conn.outbound()).expect("RST_STREAM emitted");
    assert_eq!(rst, (StreamId(1), ErrorCode::ProtocolError));
}

#[test]
fn req_pseudo_after_regular_rejected() {
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
            name: b"content-type",
            value: b"text/plain",
        },
        Header {
            name: b":path",
            value: b"/",
        },
    ]);
    let frame = headers_frame_bytes(1, true, true, &block);
    conn.ingest(&frame).unwrap();
    assert!(conn.poll_event().is_none());
    let rst = first_outbound_rst(conn.outbound()).expect("RST_STREAM emitted");
    assert_eq!(rst, (StreamId(1), ErrorCode::ProtocolError));
}

#[test]
fn req_connection_header_rejected() {
    let mut conn = server();
    prime_server(&mut conn);
    let block = full_req(&[(b"connection", b"keep-alive")]);
    let frame = headers_frame_bytes(1, true, true, &block);
    conn.ingest(&frame).unwrap();
    assert!(conn.poll_event().is_none());
    let rst = first_outbound_rst(conn.outbound()).expect("RST_STREAM emitted");
    assert_eq!(rst, (StreamId(1), ErrorCode::ProtocolError));
}

#[test]
fn req_keep_alive_header_rejected() {
    let mut conn = server();
    prime_server(&mut conn);
    let block = full_req(&[(b"keep-alive", b"timeout=5")]);
    let frame = headers_frame_bytes(1, true, true, &block);
    conn.ingest(&frame).unwrap();
    assert!(conn.poll_event().is_none());
    let rst = first_outbound_rst(conn.outbound()).expect("RST_STREAM emitted");
    assert_eq!(rst, (StreamId(1), ErrorCode::ProtocolError));
}

#[test]
fn req_transfer_encoding_rejected() {
    let mut conn = server();
    prime_server(&mut conn);
    let block = full_req(&[(b"transfer-encoding", b"chunked")]);
    let frame = headers_frame_bytes(1, true, true, &block);
    conn.ingest(&frame).unwrap();
    assert!(conn.poll_event().is_none());
    let rst = first_outbound_rst(conn.outbound()).expect("RST_STREAM emitted");
    assert_eq!(rst, (StreamId(1), ErrorCode::ProtocolError));
}

#[test]
fn req_te_trailers_ok() {
    let mut conn = server();
    prime_server(&mut conn);
    let block = full_req(&[(b"te", b"trailers")]);
    let frame = headers_frame_bytes(1, true, true, &block);
    conn.ingest(&frame).unwrap();
    let ev = conn.poll_event().expect("Headers event");
    assert!(matches!(ev, conn::Event::Headers { .. }));
    assert!(first_outbound_rst(conn.outbound()).is_none());
}

#[test]
fn req_te_gzip_rejected() {
    let mut conn = server();
    prime_server(&mut conn);
    let block = full_req(&[(b"te", b"gzip")]);
    let frame = headers_frame_bytes(1, true, true, &block);
    conn.ingest(&frame).unwrap();
    assert!(conn.poll_event().is_none());
    let rst = first_outbound_rst(conn.outbound()).expect("RST_STREAM emitted");
    assert_eq!(rst, (StreamId(1), ErrorCode::ProtocolError));
}

#[test]
fn req_trailing_no_pseudo_ok() {
    let mut conn = server();
    prime_server(&mut conn);
    let initial = headers_frame_bytes(1, false, true, &full_req(&[]));
    conn.ingest(&initial).unwrap();
    assert!(matches!(
        conn.poll_event(),
        Some(conn::Event::Headers { .. })
    ));
    conn.ingest(&data_frame_bytes(1, false, b"x")).unwrap();
    let _ = conn.poll_event();
    conn.drain_outbound(conn.outbound().len());

    let trailing_block = encode_hpack(&[Header {
        name: b"x-trailer",
        value: b"v",
    }]);
    let trailing = headers_frame_bytes(1, true, true, &trailing_block);
    conn.ingest(&trailing).unwrap();
    let ev = conn.poll_event().expect("trailing Headers event");
    assert!(matches!(
        ev,
        conn::Event::Headers {
            trailing: true,
            end_stream: true,
            ..
        }
    ));
    assert!(first_outbound_rst(conn.outbound()).is_none());
}

#[test]
fn req_trailing_with_pseudo_rejected() {
    let mut conn = server();
    prime_server(&mut conn);
    let initial = headers_frame_bytes(1, false, true, &full_req(&[]));
    conn.ingest(&initial).unwrap();
    assert!(matches!(
        conn.poll_event(),
        Some(conn::Event::Headers { .. })
    ));
    conn.ingest(&data_frame_bytes(1, false, b"x")).unwrap();
    let _ = conn.poll_event();
    conn.drain_outbound(conn.outbound().len());

    let trailing_block = encode_hpack(&[Header {
        name: b":method",
        value: b"GET",
    }]);
    let trailing = headers_frame_bytes(1, true, true, &trailing_block);
    conn.ingest(&trailing).unwrap();
    assert!(conn.poll_event().is_none());
    let rst = first_outbound_rst(conn.outbound()).expect("RST_STREAM emitted");
    assert_eq!(rst, (StreamId(1), ErrorCode::ProtocolError));
}

#[test]
fn req_continuation_validates_on_assembly() {
    let mut conn = server();
    prime_server(&mut conn);
    let block = encode_hpack(&[
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
    let split = block.len() / 2;
    conn.ingest(&headers_frame_bytes(1, true, false, &block[..split]))
        .unwrap();
    assert!(conn.poll_event().is_none());
    conn.ingest(&{
        let mut out = Vec::new();
        ContinuationFrame {
            stream_id: StreamId(1),
            end_headers: true,
            block_fragment: &block[split..],
        }
        .encode(&mut out);
        out
    })
    .unwrap();
    assert!(conn.poll_event().is_none());
    let rst = first_outbound_rst(conn.outbound()).expect("RST_STREAM emitted");
    assert_eq!(rst, (StreamId(1), ErrorCode::ProtocolError));
}

#[test]
fn req_continuation_header_list_size_over_limit_rejected_on_assembly() {
    let max = full_req_header_list_size(&[]) - 1;
    let mut conn = server_with_header_list_limit(max);
    prime_server(&mut conn);
    let block = full_req(&[]);
    let split = block.len() / 2;
    conn.ingest(&headers_frame_bytes(1, true, false, &block[..split]))
        .unwrap();
    assert!(conn.poll_event().is_none());
    conn.ingest(&{
        let mut out = Vec::new();
        ContinuationFrame {
            stream_id: StreamId(1),
            end_headers: true,
            block_fragment: &block[split..],
        }
        .encode(&mut out);
        out
    })
    .unwrap();
    assert!(conn.poll_event().is_none());
    let rst = first_outbound_rst(conn.outbound()).expect("RST_STREAM emitted");
    assert_eq!(rst, (StreamId(1), ErrorCode::ProtocolError));
    assert!(!conn.has_stream(StreamId(1)));
}

#[test]
fn resp_full_status_ok() {
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
    conn.drain_outbound(conn.outbound().len());
    let block = encode_hpack(&[Header {
        name: b":status",
        value: b"200",
    }]);
    let frame = headers_frame_bytes(id.0, true, true, &block);
    conn.ingest(&frame).unwrap();
    let ev = conn.poll_event().expect("Headers event");
    assert!(matches!(ev, conn::Event::Headers { .. }));
    assert!(first_outbound_rst(conn.outbound()).is_none());
}

#[test]
fn resp_header_list_size_over_limit_rejected() {
    let mut conn = client_with_header_list_limit(41);
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
    conn.drain_outbound(conn.outbound().len());
    let block = encode_hpack(&[Header {
        name: b":status",
        value: b"200",
    }]);
    let frame = headers_frame_bytes(id.0, true, true, &block);
    conn.ingest(&frame).unwrap();
    assert!(conn.poll_event().is_none());
    let rst = first_outbound_rst(conn.outbound()).expect("RST_STREAM emitted");
    assert_eq!(rst, (id, ErrorCode::ProtocolError));
    assert!(!conn.has_stream(id));
}

#[test]
fn resp_missing_status_rejected() {
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
    conn.drain_outbound(conn.outbound().len());
    let block = encode_hpack(&[Header {
        name: b"content-type",
        value: b"text/plain",
    }]);
    let frame = headers_frame_bytes(id.0, true, true, &block);
    conn.ingest(&frame).unwrap();
    assert!(conn.poll_event().is_none());
    let rst = first_outbound_rst(conn.outbound()).expect("RST_STREAM emitted");
    assert_eq!(rst, (id, ErrorCode::ProtocolError));
}

#[test]
fn resp_uppercase_rejected() {
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
    conn.drain_outbound(conn.outbound().len());
    let block = encode_hpack(&[
        Header {
            name: b":status",
            value: b"200",
        },
        Header {
            name: b"Content-Type",
            value: b"text/plain",
        },
    ]);
    let frame = headers_frame_bytes(id.0, true, true, &block);
    conn.ingest(&frame).unwrap();
    assert!(conn.poll_event().is_none());
    let rst = first_outbound_rst(conn.outbound()).expect("RST_STREAM emitted");
    assert_eq!(rst, (id, ErrorCode::ProtocolError));
}

#[test]
fn resp_request_pseudo_rejected() {
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
    conn.drain_outbound(conn.outbound().len());
    let block = encode_hpack(&[
        Header {
            name: b":status",
            value: b"200",
        },
        Header {
            name: b":method",
            value: b"GET",
        },
    ]);
    let frame = headers_frame_bytes(id.0, true, true, &block);
    conn.ingest(&frame).unwrap();
    assert!(conn.poll_event().is_none());
    let rst = first_outbound_rst(conn.outbound()).expect("RST_STREAM emitted");
    assert_eq!(rst, (id, ErrorCode::ProtocolError));
}

#[test]
fn resp_unknown_pseudo_rejected() {
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
    conn.drain_outbound(conn.outbound().len());
    let block = encode_hpack(&[
        Header {
            name: b":status",
            value: b"200",
        },
        Header {
            name: b":foo",
            value: b"bar",
        },
    ]);
    let frame = headers_frame_bytes(id.0, true, true, &block);
    conn.ingest(&frame).unwrap();
    assert!(conn.poll_event().is_none());
    let rst = first_outbound_rst(conn.outbound()).expect("RST_STREAM emitted");
    assert_eq!(rst, (id, ErrorCode::ProtocolError));
}

#[test]
fn server_stream_evicted_after_validation_failure() {
    let mut conn = server();
    prime_server(&mut conn);
    let _ = drain_send_headers_request;
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
            name: b":authority",
            value: b"x",
        },
    ]);
    let frame = headers_frame_bytes(1, true, true, &block);
    conn.ingest(&frame).unwrap();
    assert!(!conn.has_stream(StreamId(1)));
}

#[test]
fn server_continues_after_validation_failure() {
    let mut conn = server();
    prime_server(&mut conn);
    let bad = encode_hpack(&[
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
    conn.ingest(&headers_frame_bytes(1, true, true, &bad))
        .unwrap();
    assert!(conn.poll_event().is_none());

    let good = full_req(&[]);
    conn.ingest(&headers_frame_bytes(3, true, true, &good))
        .unwrap();
    let ev = conn.poll_event().expect("good Headers");
    if let conn::Event::Headers { stream_id, .. } = ev {
        assert_eq!(stream_id, StreamId(3));
    } else {
        panic!("expected Headers");
    }
}
