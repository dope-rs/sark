use sark_h2::frame::{
    Continuation, Data, GoAway, Headers, ParseError, Ping, Priority, PriorityFields, PushPromise,
    RstStream, SettingId, Settings, WindowUpdate,
};
use sark_h2::{ErrorCode, Flags, Frame, FrameHeader, StreamId, frame};

fn roundtrip(frame: Frame<'_>) -> Vec<u8> {
    let mut out = Vec::new();
    frame.encode(&mut out);
    let (parsed, consumed) = Frame::parse(&out).unwrap();
    assert_eq!(consumed, out.len());
    assert_eq!(parsed, frame);
    out
}

#[test]
fn header_be() {
    let h = FrameHeader {
        length: 0x010203,
        kind: frame::Type::Headers,
        flags: Flags(0x25),
        stream_id: StreamId(0x0a_0b_0c_0d),
    };
    let mut out = Vec::new();
    h.encode(&mut out);
    assert_eq!(out.len(), 9);
    assert_eq!(&out[0..3], &[0x01, 0x02, 0x03]);
    assert_eq!(out[3], frame::Type::Headers as u8);
    assert_eq!(out[4], 0x25);
    assert_eq!(&out[5..9], &[0x0a, 0x0b, 0x0c, 0x0d]);
    let back = FrameHeader::parse(&out).unwrap();
    assert_eq!(back, h);
}

#[test]
fn header_r_bit() {
    let bytes = [0x00, 0x00, 0x00, 0x06, 0x00, 0xff, 0xff, 0xff, 0xff];
    let h = FrameHeader::parse(&bytes).unwrap();
    assert_eq!(h.stream_id.0, 0x7fff_ffff);
}

#[test]
fn header_short() {
    assert_eq!(FrameHeader::parse(&[0u8; 8]), Err(ParseError::NeedMore));
}

#[test]
fn header_bad_type() {
    let bytes = [0, 0, 0, 0xff, 0, 0, 0, 0, 0];
    assert_eq!(FrameHeader::parse(&bytes), Err(ParseError::BadType(0xff)));
}

#[test]
fn data_roundtrip() {
    let f = Data {
        stream_id: StreamId(7),
        end_stream: true,
        payload: b"hello world",
    };
    roundtrip(Frame::Data(f));
}

#[test]
fn data_zero_stream() {
    let h = FrameHeader {
        length: 1,
        kind: frame::Type::Data,
        flags: Flags(0),
        stream_id: StreamId(0),
    };
    assert_eq!(Data::parse(h, b"x"), Err(ParseError::Protocol));
}

#[test]
fn data_padding() {
    let mut bytes = Vec::new();
    let pad_len: u8 = 3;
    let inner: &[u8] = b"abc";
    let payload_len = 1 + inner.len() + pad_len as usize;
    FrameHeader {
        length: payload_len as u32,
        kind: frame::Type::Data,
        flags: Flags(Flags::PADDED | Flags::END_STREAM),
        stream_id: StreamId(5),
    }
    .encode(&mut bytes);
    bytes.push(pad_len);
    bytes.extend_from_slice(inner);
    bytes.extend_from_slice(&[0u8; 3]);
    let (frame, used) = Frame::parse(&bytes).unwrap();
    assert_eq!(used, bytes.len());
    match frame {
        Frame::Data(d) => {
            assert_eq!(d.payload, inner);
            assert!(d.end_stream);
            assert_eq!(d.stream_id, StreamId(5));
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn padding_overflow() {
    let payload = [10u8, 1, 2];
    let h = FrameHeader {
        length: 3,
        kind: frame::Type::Data,
        flags: Flags(Flags::PADDED),
        stream_id: StreamId(1),
    };
    assert_eq!(Data::parse(h, &payload), Err(ParseError::Padding));
}

#[test]
fn headers_plain() {
    let f = Headers {
        stream_id: StreamId(3),
        end_stream: false,
        end_headers: true,
        priority: None,
        block_fragment: b"\x82\x86\x84",
    };
    roundtrip(Frame::Headers(f));
}

#[test]
fn headers_priority_padded() {
    let f = Headers {
        stream_id: StreamId(11),
        end_stream: true,
        end_headers: true,
        priority: Some(PriorityFields {
            exclusive: true,
            dependency: StreamId(5),
            weight: 200,
        }),
        block_fragment: b"\x82\x86\x84\x40\x88block",
    };
    let mut out = Vec::new();
    f.encode(&mut out);
    let mut bytes = Vec::new();
    let pad_len: u8 = 4;
    let priority_len = 5;
    let payload_len = 1 + priority_len + f.block_fragment.len() + pad_len as usize;
    FrameHeader {
        length: payload_len as u32,
        kind: frame::Type::Headers,
        flags: Flags(Flags::END_STREAM | Flags::END_HEADERS | Flags::PRIORITY | Flags::PADDED),
        stream_id: StreamId(11),
    }
    .encode(&mut bytes);
    bytes.push(pad_len);
    f.priority.unwrap().encode(&mut bytes);
    bytes.extend_from_slice(f.block_fragment);
    bytes.extend_from_slice(&[0u8; 4]);
    let (parsed, used) = Frame::parse(&bytes).unwrap();
    assert_eq!(used, bytes.len());
    assert_eq!(parsed, Frame::Headers(f));
}

#[test]
fn priority_size() {
    let h = FrameHeader {
        length: 4,
        kind: frame::Type::Priority,
        flags: Flags(0),
        stream_id: StreamId(1),
    };
    assert_eq!(
        Priority::parse(h, &[0, 0, 0, 0]),
        Err(ParseError::FrameSize)
    );
}

#[test]
fn priority_zero_stream() {
    let h = FrameHeader {
        length: 5,
        kind: frame::Type::Priority,
        flags: Flags(0),
        stream_id: StreamId(0),
    };
    assert_eq!(
        Priority::parse(h, &[0, 0, 0, 0, 0]),
        Err(ParseError::Protocol)
    );
}

#[test]
fn priority_roundtrip() {
    let f = Priority {
        stream_id: StreamId(9),
        fields: PriorityFields {
            exclusive: false,
            dependency: StreamId(2),
            weight: 15,
        },
    };
    roundtrip(Frame::Priority(f));
}

#[test]
fn rst_roundtrip() {
    let f = RstStream {
        stream_id: StreamId(13),
        error: ErrorCode::Cancel,
    };
    roundtrip(Frame::RstStream(f));
}

#[test]
fn rst_size() {
    let h = FrameHeader {
        length: 3,
        kind: frame::Type::RstStream,
        flags: Flags(0),
        stream_id: StreamId(1),
    };
    assert_eq!(RstStream::parse(h, &[0, 0, 0]), Err(ParseError::FrameSize));
}

#[test]
fn rst_zero_stream() {
    let h = FrameHeader {
        length: 4,
        kind: frame::Type::RstStream,
        flags: Flags(0),
        stream_id: StreamId(0),
    };
    assert_eq!(
        RstStream::parse(h, &[0, 0, 0, 0]),
        Err(ParseError::Protocol)
    );
}

#[test]
fn error_unknown() {
    assert_eq!(ErrorCode::from_u32(0xdead_beef), ErrorCode::InternalError);
}

#[test]
fn settings_empty_ack() {
    let f = Settings {
        ack: true,
        params: &[],
    };
    roundtrip(Frame::Settings(f));
}

#[test]
fn settings_multi() {
    let mut params = Vec::new();
    for (id, val) in [
        (SettingId::HeaderTableSize as u16, 4096u32),
        (SettingId::EnablePush as u16, 0),
        (SettingId::MaxConcurrentStreams as u16, 100),
        (SettingId::InitialWindowSize as u16, 65535),
        (SettingId::MaxFrameSize as u16, 16384),
        (SettingId::MaxHeaderListSize as u16, 8192),
    ] {
        params.extend_from_slice(&id.to_be_bytes());
        params.extend_from_slice(&val.to_be_bytes());
    }
    let f = Settings {
        ack: false,
        params: &params,
    };
    roundtrip(Frame::Settings(f));
    let mut it = f.iter();
    assert_eq!(it.next(), Some((Some(SettingId::HeaderTableSize), 4096)));
    assert_eq!(it.next(), Some((Some(SettingId::EnablePush), 0)));
    assert_eq!(
        it.next(),
        Some((Some(SettingId::MaxConcurrentStreams), 100))
    );
    assert_eq!(it.next(), Some((Some(SettingId::InitialWindowSize), 65535)));
    assert_eq!(it.next(), Some((Some(SettingId::MaxFrameSize), 16384)));
    assert_eq!(it.next(), Some((Some(SettingId::MaxHeaderListSize), 8192)));
    assert_eq!(it.next(), None);
}

#[test]
fn settings_unknown_id() {
    let mut params = Vec::new();
    params.extend_from_slice(&0xabcd_u16.to_be_bytes());
    params.extend_from_slice(&42u32.to_be_bytes());
    let f = Settings {
        ack: false,
        params: &params,
    };
    let mut it = f.iter();
    assert_eq!(it.next(), Some((None, 42)));
    assert_eq!(it.next(), None);
}

#[test]
fn settings_ack_with_payload() {
    let h = FrameHeader {
        length: 6,
        kind: frame::Type::Settings,
        flags: Flags(Flags::ACK),
        stream_id: StreamId(0),
    };
    assert_eq!(Settings::parse(h, &[0u8; 6]), Err(ParseError::FrameSize));
}

#[test]
fn settings_bad_len() {
    let h = FrameHeader {
        length: 5,
        kind: frame::Type::Settings,
        flags: Flags(0),
        stream_id: StreamId(0),
    };
    assert_eq!(Settings::parse(h, &[0u8; 5]), Err(ParseError::FrameSize));
}

#[test]
fn settings_stream_id() {
    let h = FrameHeader {
        length: 0,
        kind: frame::Type::Settings,
        flags: Flags(Flags::ACK),
        stream_id: StreamId(3),
    };
    assert_eq!(Settings::parse(h, &[]), Err(ParseError::Protocol));
}

#[test]
fn push_roundtrip() {
    let f = PushPromise {
        stream_id: StreamId(3),
        promised_stream_id: StreamId(4),
        end_headers: true,
        block_fragment: b"\x82\x86",
    };
    roundtrip(Frame::PushPromise(f));
}

#[test]
fn push_zero_stream() {
    let h = FrameHeader {
        length: 4,
        kind: frame::Type::PushPromise,
        flags: Flags(0),
        stream_id: StreamId(0),
    };
    assert_eq!(
        PushPromise::parse(h, &[0, 0, 0, 1]),
        Err(ParseError::Protocol)
    );
}

#[test]
fn ping_roundtrip() {
    let f = Ping {
        ack: true,
        opaque: [1, 2, 3, 4, 5, 6, 7, 8],
    };
    roundtrip(Frame::Ping(f));
}

#[test]
fn ping_size() {
    let h = FrameHeader {
        length: 7,
        kind: frame::Type::Ping,
        flags: Flags(0),
        stream_id: StreamId(0),
    };
    assert_eq!(Ping::parse(h, &[0u8; 7]), Err(ParseError::FrameSize));
}

#[test]
fn ping_stream() {
    let h = FrameHeader {
        length: 8,
        kind: frame::Type::Ping,
        flags: Flags(0),
        stream_id: StreamId(1),
    };
    assert_eq!(Ping::parse(h, &[0u8; 8]), Err(ParseError::Protocol));
}

#[test]
fn goaway_empty() {
    let f = GoAway {
        last_stream_id: StreamId(0),
        error: ErrorCode::NoError,
        debug: &[],
    };
    roundtrip(Frame::GoAway(f));
}

#[test]
fn goaway_debug() {
    let f = GoAway {
        last_stream_id: StreamId(101),
        error: ErrorCode::ProtocolError,
        debug: b"goodbye",
    };
    roundtrip(Frame::GoAway(f));
}

#[test]
fn goaway_short() {
    let h = FrameHeader {
        length: 7,
        kind: frame::Type::GoAway,
        flags: Flags(0),
        stream_id: StreamId(0),
    };
    assert_eq!(GoAway::parse(h, &[0u8; 7]), Err(ParseError::FrameSize));
}

#[test]
fn window_roundtrip() {
    let f = WindowUpdate {
        stream_id: StreamId(7),
        increment: 65_535,
    };
    roundtrip(Frame::WindowUpdate(f));
}

#[test]
fn window_conn() {
    let f = WindowUpdate {
        stream_id: StreamId(0),
        increment: 1,
    };
    roundtrip(Frame::WindowUpdate(f));
}

#[test]
fn window_zero() {
    let h = FrameHeader {
        length: 4,
        kind: frame::Type::WindowUpdate,
        flags: Flags(0),
        stream_id: StreamId(1),
    };
    assert_eq!(
        WindowUpdate::parse(h, &[0, 0, 0, 0]),
        Err(ParseError::ZeroIncrement)
    );
}

#[test]
fn window_size() {
    let h = FrameHeader {
        length: 3,
        kind: frame::Type::WindowUpdate,
        flags: Flags(0),
        stream_id: StreamId(1),
    };
    assert_eq!(
        WindowUpdate::parse(h, &[0, 0, 0]),
        Err(ParseError::FrameSize)
    );
}

#[test]
fn ping_size_beats_protocol() {
    let h = FrameHeader {
        length: 6,
        kind: frame::Type::Ping,
        flags: Flags(0),
        stream_id: StreamId(1),
    };
    assert_eq!(Ping::parse(h, &[0u8; 6]), Err(ParseError::FrameSize));
}

#[test]
fn priority_size_beats_protocol() {
    let h = FrameHeader {
        length: 4,
        kind: frame::Type::Priority,
        flags: Flags(0),
        stream_id: StreamId(0),
    };
    assert_eq!(
        Priority::parse(h, &[0, 0, 0, 0]),
        Err(ParseError::FrameSize)
    );
}

#[test]
fn rst_size_beats_protocol() {
    let h = FrameHeader {
        length: 3,
        kind: frame::Type::RstStream,
        flags: Flags(0),
        stream_id: StreamId(0),
    };
    assert_eq!(RstStream::parse(h, &[0, 0, 0]), Err(ParseError::FrameSize));
}

#[test]
fn settings_size_beats_protocol() {
    let h = FrameHeader {
        length: 5,
        kind: frame::Type::Settings,
        flags: Flags(0),
        stream_id: StreamId(3),
    };
    assert_eq!(Settings::parse(h, &[0u8; 5]), Err(ParseError::FrameSize));
}

#[test]
fn window_r_bit() {
    let h = FrameHeader {
        length: 4,
        kind: frame::Type::WindowUpdate,
        flags: Flags(0),
        stream_id: StreamId(1),
    };
    let w = WindowUpdate::parse(h, &[0xff, 0xff, 0xff, 0xff]).unwrap();
    assert_eq!(w.increment, 0x7fff_ffff);
}

#[test]
fn cont_roundtrip() {
    let f = Continuation {
        stream_id: StreamId(3),
        end_headers: true,
        block_fragment: b"\x82\x86",
    };
    roundtrip(Frame::Continuation(f));
}

#[test]
fn cont_zero_stream() {
    let h = FrameHeader {
        length: 0,
        kind: frame::Type::Continuation,
        flags: Flags(0),
        stream_id: StreamId(0),
    };
    assert_eq!(Continuation::parse(h, &[]), Err(ParseError::Protocol));
}

#[test]
fn parse_need_more() {
    let f = Ping {
        ack: false,
        opaque: [9u8; 8],
    };
    let mut out = Vec::new();
    Frame::Ping(f).encode(&mut out);
    assert_eq!(
        Frame::parse(&out[..out.len() - 1]),
        Err(ParseError::NeedMore)
    );
}

#[test]
fn parse_two() {
    let mut out = Vec::new();
    Frame::Ping(Ping {
        ack: false,
        opaque: [1u8; 8],
    })
    .encode(&mut out);
    let first_len = out.len();
    Frame::WindowUpdate(WindowUpdate {
        stream_id: StreamId(0),
        increment: 1024,
    })
    .encode(&mut out);
    let (f1, n1) = Frame::parse(&out).unwrap();
    assert_eq!(n1, first_len);
    assert!(matches!(f1, Frame::Ping(p) if p.opaque == [1u8; 8]));
    let (f2, n2) = Frame::parse(&out[n1..]).unwrap();
    assert_eq!(n1 + n2, out.len());
    assert!(matches!(f2, Frame::WindowUpdate(w) if w.increment == 1024));
}
