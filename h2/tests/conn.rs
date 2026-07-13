use sark_h2::frame::{
    Frame, GoAway as GoAwayFrame, Ping as PingFrame, SettingId, Settings as SettingsFrame,
    WindowUpdate as WindowUpdateFrame,
};
use sark_h2::{
    CLIENT_PREFACE, ClientRole, Conn, ConnError, ErrorCode, FrameHeader, ServerRole, Settings,
    StreamId, conn, frame,
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
    let frame = SettingsFrame {
        ack,
        params: &payload,
    };
    frame.encode(&mut out);
    out
}

fn ping_frame_bytes(opaque: [u8; 8], ack: bool) -> Vec<u8> {
    let mut out = Vec::new();
    PingFrame { ack, opaque }.encode(&mut out);
    out
}

fn goaway_frame_bytes(last: u32, err: ErrorCode, debug: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    GoAwayFrame {
        last_stream_id: StreamId(last),
        error: err,
        debug,
    }
    .encode(&mut out);
    out
}

fn window_update_bytes(stream_id: u32, inc: u32) -> Vec<u8> {
    let mut out = Vec::new();
    WindowUpdateFrame {
        stream_id: StreamId(stream_id),
        increment: inc,
    }
    .encode(&mut out);
    out
}

fn drain_initial_settings_frame(conn_outbound: &[u8]) -> (FrameHeader, usize) {
    let h = FrameHeader::parse(conn_outbound).unwrap();
    assert_eq!(h.kind, frame::Type::Settings);
    let total = 9 + h.length as usize;
    (h, total)
}

#[test]
fn server_new_no_preface_in_outbound() {
    let conn = server();
    let out = conn.outbound();
    let h = FrameHeader::parse(out).unwrap();
    assert_eq!(h.kind, frame::Type::Settings);
    assert!(h.flags.0 == 0);
}

#[test]
fn client_new_emits_preface_then_settings() {
    let conn = client();
    let out = conn.outbound();
    assert!(out.len() >= CLIENT_PREFACE.len());
    assert_eq!(&out[..CLIENT_PREFACE.len()], CLIENT_PREFACE);
    let after = &out[CLIENT_PREFACE.len()..];
    let h = FrameHeader::parse(after).unwrap();
    assert_eq!(h.kind, frame::Type::Settings);
    assert!(h.flags.0 == 0);
}

#[test]
fn client_new_emits_preface_complete_event() {
    let mut conn = client();
    let ev = conn.poll_event().unwrap();
    assert_eq!(ev, conn::Event::PrefaceComplete);
}

#[test]
fn server_ingest_full_preface_emits_event() {
    let mut conn = server();
    conn.ingest(CLIENT_PREFACE).unwrap();
    let ev = conn.poll_event().unwrap();
    assert_eq!(ev, conn::Event::PrefaceComplete);
}

#[test]
fn server_ingest_bad_preface() {
    let mut conn = server();
    let mut bad = CLIENT_PREFACE.to_vec();
    *bad.last_mut().unwrap() = b'X';
    let err = conn.ingest(&bad).unwrap_err();
    assert_eq!(err, ConnError::BadPreface);
}

#[test]
fn server_partial_preface_needmore() {
    let mut conn = server();
    let partial = &CLIENT_PREFACE[..23];
    conn.ingest(partial).unwrap();
    assert!(conn.poll_event().is_none());
}

#[test]
fn server_chunked_preface_then_event() {
    let mut conn = server();
    conn.ingest(&CLIENT_PREFACE[..10]).unwrap();
    assert!(conn.poll_event().is_none());
    conn.ingest(&CLIENT_PREFACE[10..]).unwrap();
    assert_eq!(conn.poll_event().unwrap(), conn::Event::PrefaceComplete);
}

#[test]
fn server_peer_settings_applied_emits_ack_and_event() {
    let mut conn = server();
    let (_h, _init_total) = drain_initial_settings_frame(conn.outbound());
    conn.drain_outbound(conn.outbound().len());
    conn.ingest(CLIENT_PREFACE).unwrap();
    assert_eq!(conn.poll_event().unwrap(), conn::Event::PrefaceComplete);

    let peer = settings_frame_bytes(&[(SettingId::InitialWindowSize as u16, 100_000)], false);
    conn.ingest(&peer).unwrap();
    assert_eq!(conn.peer_settings().initial_window_size, 100_000);
    let out = conn.outbound();
    let (_, parsed) = Frame::parse(out).unwrap();
    let _ = parsed;
    let h = FrameHeader::parse(out).unwrap();
    assert_eq!(h.kind, frame::Type::Settings);
    assert!(h.flags.has(sark_h2::Flags::ACK));
    assert_eq!(h.length, 0);
    assert_eq!(conn.poll_event().unwrap(), conn::Event::SettingsApplied);
}

#[test]
fn server_peer_settings_ack_event() {
    let mut conn = server();
    conn.drain_outbound(conn.outbound().len());
    conn.ingest(CLIENT_PREFACE).unwrap();
    conn.poll_event();

    let ack = settings_frame_bytes(&[], true);
    conn.ingest(&ack).unwrap();
    assert!(conn.outbound().is_empty());
    assert_eq!(conn.poll_event().unwrap(), conn::Event::SettingsAck);
}

#[test]
fn server_initial_window_setting_leaves_conn_send_window() {
    let mut conn = server();
    conn.drain_outbound(conn.outbound().len());
    conn.ingest(CLIENT_PREFACE).unwrap();
    conn.poll_event();

    let initial = conn.send_window().value;
    let peer = settings_frame_bytes(&[(SettingId::InitialWindowSize as u16, 100_000)], false);
    conn.ingest(&peer).unwrap();
    assert_eq!(conn.send_window().value, initial);
    assert_eq!(conn.peer_settings().initial_window_size, 100_000);
}

#[test]
fn server_settings_max_frame_size_out_of_range() {
    let mut conn = server();
    conn.drain_outbound(conn.outbound().len());
    conn.ingest(CLIENT_PREFACE).unwrap();
    conn.poll_event();

    let peer = settings_frame_bytes(&[(SettingId::MaxFrameSize as u16, 15_000)], false);
    let err = conn.ingest(&peer).unwrap_err();
    assert_eq!(err, ConnError::BadSettings);
}

#[test]
fn server_settings_enable_push_two_is_bad() {
    let mut conn = server();
    conn.drain_outbound(conn.outbound().len());
    conn.ingest(CLIENT_PREFACE).unwrap();
    conn.poll_event();

    let peer = settings_frame_bytes(&[(SettingId::EnablePush as u16, 2)], false);
    let err = conn.ingest(&peer).unwrap_err();
    assert_eq!(err, ConnError::BadSettings);
}

#[test]
fn server_settings_payload_not_multiple_of_six() {
    let mut conn = server();
    conn.drain_outbound(conn.outbound().len());
    conn.ingest(CLIENT_PREFACE).unwrap();
    conn.poll_event();

    let bad_payload = vec![0u8; 7];
    let mut bytes = Vec::new();
    SettingsFrame {
        ack: false,
        params: &bad_payload,
    }
    .encode(&mut bytes);
    let err = conn.ingest(&bytes).unwrap_err();
    assert!(matches!(err, ConnError::ParseError(_)));
}

#[test]
fn server_settings_ack_with_payload_is_bad() {
    let mut conn = server();
    conn.drain_outbound(conn.outbound().len());
    conn.ingest(CLIENT_PREFACE).unwrap();
    conn.poll_event();

    let payload = vec![0u8; 6];
    let mut bytes = Vec::new();
    SettingsFrame {
        ack: true,
        params: &payload,
    }
    .encode(&mut bytes);
    let err = conn.ingest(&bytes).unwrap_err();
    assert!(matches!(err, ConnError::ParseError(_)));
}

#[test]
fn server_ping_pong() {
    let mut conn = server();
    conn.drain_outbound(conn.outbound().len());
    conn.ingest(CLIENT_PREFACE).unwrap();
    conn.poll_event();

    let opaque = [1u8, 2, 3, 4, 5, 6, 7, 8];
    let ping = ping_frame_bytes(opaque, false);
    conn.ingest(&ping).unwrap();
    let out = conn.outbound();
    let h = FrameHeader::parse(out).unwrap();
    assert_eq!(h.kind, frame::Type::Ping);
    assert!(h.flags.has(sark_h2::Flags::ACK));
    let pong_payload = &out[9..9 + 8];
    assert_eq!(pong_payload, &opaque);
    assert_eq!(
        conn.poll_event().unwrap(),
        conn::Event::Ping { ack: false, opaque }
    );
}

#[test]
fn server_ping_ack_no_pong() {
    let mut conn = server();
    conn.drain_outbound(conn.outbound().len());
    conn.ingest(CLIENT_PREFACE).unwrap();
    conn.poll_event();

    let opaque = [9u8; 8];
    let ack = ping_frame_bytes(opaque, true);
    conn.ingest(&ack).unwrap();
    assert!(conn.outbound().is_empty());
    assert_eq!(
        conn.poll_event().unwrap(),
        conn::Event::Ping { ack: true, opaque }
    );
}

#[test]
fn server_caller_emits_goaway() {
    let mut conn = server();
    conn.drain_outbound(conn.outbound().len());
    conn.goaway(ErrorCode::ProtocolError, b"oops");
    let out = conn.outbound();
    let h = FrameHeader::parse(out).unwrap();
    assert_eq!(h.kind, frame::Type::GoAway);
    assert!(conn.goaway_sent());
}

#[test]
fn server_peer_goaway_received() {
    let mut conn = server();
    conn.drain_outbound(conn.outbound().len());
    conn.ingest(CLIENT_PREFACE).unwrap();
    conn.poll_event();

    let goaway = goaway_frame_bytes(7, ErrorCode::InternalError, b"down");
    conn.ingest(&goaway).unwrap();
    assert_eq!(conn.goaway_received(), Some(ErrorCode::InternalError));
    let ev = conn.poll_event().unwrap();
    match ev {
        conn::Event::GoAway {
            last_stream_id,
            error,
            debug,
        } => {
            assert_eq!(last_stream_id, StreamId(7));
            assert_eq!(error, ErrorCode::InternalError);
            assert_eq!(debug, b"down");
        }
        _ => panic!("expected GoAway"),
    }
}

#[test]
fn server_conn_window_update_increases() {
    let mut conn = server();
    conn.drain_outbound(conn.outbound().len());
    conn.ingest(CLIENT_PREFACE).unwrap();
    conn.poll_event();

    let initial = conn.send_window().value;
    let wu = window_update_bytes(0, 1000);
    conn.ingest(&wu).unwrap();
    assert_eq!(conn.send_window().value, initial + 1000);
}

#[test]
fn server_stream_window_update_on_idle_protocol_error() {
    let mut conn = server();
    conn.drain_outbound(conn.outbound().len());
    conn.ingest(CLIENT_PREFACE).unwrap();
    conn.poll_event();

    let wu = window_update_bytes(5, 100);
    let err = conn.ingest(&wu).unwrap_err();
    assert_eq!(err, ConnError::Protocol);
}

#[test]
fn server_window_update_zero_protocol() {
    let mut conn = server();
    conn.drain_outbound(conn.outbound().len());
    conn.ingest(CLIENT_PREFACE).unwrap();
    conn.poll_event();

    let mut payload = Vec::new();
    payload.extend_from_slice(&0u32.to_be_bytes());
    let mut bytes = Vec::new();
    FrameHeader {
        length: 4,
        kind: frame::Type::WindowUpdate,
        flags: sark_h2::Flags(0),
        stream_id: StreamId(0),
    }
    .encode(&mut bytes);
    bytes.extend_from_slice(&payload);
    let err = conn.ingest(&bytes).unwrap_err();
    assert!(matches!(err, ConnError::ParseError(_)));
}

#[test]
fn server_headers_bad_hpack_block_yields_error() {
    let mut conn = server();
    conn.drain_outbound(conn.outbound().len());
    conn.ingest(CLIENT_PREFACE).unwrap();
    conn.poll_event();

    let payload = vec![0u8; 4];
    let mut bytes = Vec::new();
    FrameHeader {
        length: payload.len() as u32,
        kind: frame::Type::Headers,
        flags: sark_h2::Flags(sark_h2::Flags::END_HEADERS | sark_h2::Flags::END_STREAM),
        stream_id: StreamId(1),
    }
    .encode(&mut bytes);
    bytes.extend_from_slice(&payload);
    let err = conn.ingest(&bytes).unwrap_err();
    assert!(matches!(err, ConnError::Hpack(_)));
}

#[test]
fn server_data_without_open_stream_protocol_error() {
    let mut conn = server();
    conn.drain_outbound(conn.outbound().len());
    conn.ingest(CLIENT_PREFACE).unwrap();
    conn.poll_event();

    let payload = vec![0u8; 16];
    let mut bytes = Vec::new();
    FrameHeader {
        length: payload.len() as u32,
        kind: frame::Type::Data,
        flags: sark_h2::Flags(0),
        stream_id: StreamId(1),
    }
    .encode(&mut bytes);
    bytes.extend_from_slice(&payload);
    let err = conn.ingest(&bytes).unwrap_err();
    assert_eq!(err, ConnError::Protocol);
}

#[test]
fn server_rst_stream_on_idle_stream_protocol_error() {
    let mut conn = server();
    conn.drain_outbound(conn.outbound().len());
    conn.ingest(CLIENT_PREFACE).unwrap();
    conn.poll_event();

    let payload = (ErrorCode::Cancel as u32).to_be_bytes();
    let mut bytes = Vec::new();
    FrameHeader {
        length: 4,
        kind: frame::Type::RstStream,
        flags: sark_h2::Flags(0),
        stream_id: StreamId(1),
    }
    .encode(&mut bytes);
    bytes.extend_from_slice(&payload);
    let err = conn.ingest(&bytes).unwrap_err();
    assert_eq!(err, ConnError::Protocol);
}

#[test]
fn server_unknown_frame_type_ignored() {
    let mut conn = server();
    conn.drain_outbound(conn.outbound().len());
    conn.ingest(CLIENT_PREFACE).unwrap();
    conn.poll_event();

    let payload = vec![0u8; 4];
    let mut bytes = vec![0, 0, payload.len() as u8, 0x99, 0];
    bytes.extend_from_slice(&0u32.to_be_bytes());
    bytes.extend_from_slice(&payload);
    conn.ingest(&bytes).unwrap();
    assert!(conn.poll_event().is_none());
    let ack = settings_frame_bytes(&[], true);
    conn.ingest(&ack).unwrap();
    assert_eq!(conn.poll_event().unwrap(), conn::Event::SettingsAck);
}

#[test]
fn outbound_drain_progresses() {
    let mut conn = server();
    let total = conn.outbound().len();
    assert!(total > 0);
    conn.drain_outbound(5);
    assert_eq!(conn.outbound().len(), total - 5);
    conn.drain_outbound(usize::MAX);
    assert!(conn.outbound().is_empty());
}

#[test]
fn caller_ping_appends_frame() {
    let mut conn = server();
    let before = conn.outbound().len();
    conn.ping([7u8; 8]);
    let after = conn.outbound();
    assert!(after.len() > before);
    let frame_start = &after[before..];
    let h = FrameHeader::parse(frame_start).unwrap();
    assert_eq!(h.kind, frame::Type::Ping);
    assert_eq!(h.flags.0, 0);
}

#[test]
fn client_ingest_settings() {
    let mut conn = client();
    conn.poll_event();
    conn.drain_outbound(conn.outbound().len());

    let peer = settings_frame_bytes(&[(SettingId::HeaderTableSize as u16, 8192)], false);
    conn.ingest(&peer).unwrap();
    assert_eq!(conn.peer_settings().header_table_size, 8192);
    let out = conn.outbound();
    let h = FrameHeader::parse(out).unwrap();
    assert_eq!(h.kind, frame::Type::Settings);
    assert!(h.flags.has(sark_h2::Flags::ACK));
    assert_eq!(conn.poll_event().unwrap(), conn::Event::SettingsApplied);
}

#[test]
fn client_peer_ping_pong() {
    let mut conn = client();
    conn.poll_event();
    conn.drain_outbound(conn.outbound().len());

    let opaque = [42u8; 8];
    let ping = ping_frame_bytes(opaque, false);
    conn.ingest(&ping).unwrap();
    let out = conn.outbound();
    let h = FrameHeader::parse(out).unwrap();
    assert_eq!(h.kind, frame::Type::Ping);
    assert!(h.flags.has(sark_h2::Flags::ACK));
}

#[test]
fn settings_default_values() {
    let s = Settings::DEFAULT;
    assert_eq!(s.header_table_size, 4096);
    assert!(s.enable_push);
    assert_eq!(s.max_concurrent_streams, None);
    assert_eq!(s.initial_window_size, 65_535);
    assert_eq!(s.max_frame_size, 16_384);
    assert_eq!(s.max_header_list_size, None);
}

#[test]
fn settings_apply_unknown_id_is_handled_via_iter() {
    let mut s = Settings::DEFAULT;
    s.apply(SettingId::HeaderTableSize, 1024).unwrap();
    assert_eq!(s.header_table_size, 1024);
    s.apply(SettingId::EnablePush, 0).unwrap();
    assert!(!s.enable_push);
    s.apply(SettingId::MaxConcurrentStreams, 128).unwrap();
    assert_eq!(s.max_concurrent_streams, Some(128));
}

#[test]
fn settings_apply_iws_too_large_flow_control() {
    let mut s = Settings::DEFAULT;
    let err = s
        .apply(SettingId::InitialWindowSize, 0x8000_0000)
        .unwrap_err();
    assert_eq!(err, ConnError::FlowControl);
}

#[test]
fn server_unknown_setting_id_ignored_in_payload() {
    let mut conn = server();
    conn.drain_outbound(conn.outbound().len());
    conn.ingest(CLIENT_PREFACE).unwrap();
    conn.poll_event();

    let peer = settings_frame_bytes(
        &[
            (0xff_ff, 0xdeadbeef),
            (SettingId::HeaderTableSize as u16, 9999),
        ],
        false,
    );
    conn.ingest(&peer).unwrap();
    assert_eq!(conn.peer_settings().header_table_size, 9999);
    assert_eq!(conn.poll_event().unwrap(), conn::Event::SettingsApplied);
}

#[test]
fn server_pipelined_frames_processed() {
    let mut conn = server();
    conn.drain_outbound(conn.outbound().len());

    let mut feed = Vec::new();
    feed.extend_from_slice(CLIENT_PREFACE);
    feed.extend_from_slice(&settings_frame_bytes(&[], false));
    feed.extend_from_slice(&ping_frame_bytes([3u8; 8], false));
    conn.ingest(&feed).unwrap();

    let mut events = Vec::new();
    while let Some(e) = conn.poll_event() {
        events.push(e);
    }
    assert_eq!(events.len(), 3);
    assert_eq!(events[0], conn::Event::PrefaceComplete);
    assert_eq!(events[1], conn::Event::SettingsApplied);
    assert_eq!(
        events[2],
        conn::Event::Ping {
            ack: false,
            opaque: [3u8; 8]
        }
    );
}
