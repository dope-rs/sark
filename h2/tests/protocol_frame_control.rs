use sark_h2::{
    frame,
    frame::{
        Ack,
        controls::Settings,
        layout::Header,
        numbers::{ErrorCode, Flags, Length, ParseError, Type},
        payload::{GoAway, Ping, PushPromise, WindowUpdate},
    },
    stream::Id,
};

#[test]
fn error_unknown() {
    assert!((ErrorCode::from_u32(0xdead_beef)) == (ErrorCode::InternalError));
}

#[test]
fn settings_unknown_id() {
    let mut params = Vec::new();
    params.extend_from_slice(&0xabcd_u16.to_be_bytes());
    params.extend_from_slice(&42u32.to_be_bytes());
    let result = Settings::new(Ack::Required, &params);
    let f = result.expect("valid frame fixture must parse");
    let mut it = f.iter();
    assert!((it.next()) == (Some((None, 42))));
    assert!((it.next()).is_none());
}

#[test]
fn rejects_ack_payload() {
    let h = Header {
        length: Length::new(6).expect("fixture domain"),
        kind: Type::Settings,
        flags: Flags(Flags::ACK),
        stream_id: Id::new(0).expect("fixture domain"),
    };
    assert!(
        Settings::parse(h, &[0u8; 6])
            == Err(ParseError::SettingsSize {
                actual: 6,
                ack: Ack::Complete,
            })
    );
}

#[test]
fn settings_stream_id() {
    let h = Header {
        length: Length::new(0).expect("fixture domain"),
        kind: Type::Settings,
        flags: Flags(Flags::ACK),
        stream_id: Id::new(3).expect("fixture domain"),
    };
    assert!(
        Settings::parse(h, &[])
            == Err(ParseError::Connection {
                kind: h.kind,
                actual: h.stream_id,
            })
    );
}

#[test]
fn push_zero_stream() {
    let h = Header {
        length: Length::new(4).expect("fixture domain"),
        kind: Type::PushPromise,
        flags: Flags(0),
        stream_id: Id::new(0).expect("fixture domain"),
    };
    assert!(
        PushPromise::parse(h, &[0, 0, 0, 1])
            == Err(ParseError::Stream {
                kind: h.kind,
                actual: h.stream_id,
            })
    );
}

#[test]
fn push_zero_promised() {
    let h = Header {
        length: Length::new(4).expect("fixture domain"),
        kind: Type::PushPromise,
        flags: Flags(0),
        stream_id: Id::new(1).expect("fixture domain"),
    };
    assert!(
        PushPromise::parse(h, &[0, 0, 0, 0])
            == Err(ParseError::Promise {
                stream: h.stream_id,
                promised: Id::new(0).expect("fixture domain"),
            })
    );
}

#[test]
fn ping_stream() {
    let h = Header {
        length: Length::new(8).expect("fixture domain"),
        kind: Type::Ping,
        flags: Flags(0),
        stream_id: Id::new(1).expect("fixture domain"),
    };
    assert!(
        Ping::parse(h, &[0u8; 8])
            == Err(ParseError::Connection {
                kind: h.kind,
                actual: h.stream_id,
            })
    );
}

#[test]
fn goaway_short() {
    let h = Header {
        length: Length::new(7).expect("fixture domain"),
        kind: Type::GoAway,
        flags: Flags(0),
        stream_id: Id::new(0).expect("fixture domain"),
    };
    assert!(
        GoAway::parse(h, &[0u8; 7])
            == Err(ParseError::FrameMinimum {
                kind: frame::numbers::Type::GoAway,
                actual: 7,
                minimum: 8,
            })
    );
}

#[test]
fn window_zero() {
    let h = Header {
        length: Length::new(4).expect("fixture domain"),
        kind: Type::WindowUpdate,
        flags: Flags(0),
        stream_id: Id::new(1).expect("fixture domain"),
    };
    assert!(WindowUpdate::parse(h, &[0, 0, 0, 0]) == Err(ParseError::ZeroIncrement));
}

#[test]
fn window_size() {
    let h = Header {
        length: Length::new(3).expect("fixture domain"),
        kind: Type::WindowUpdate,
        flags: Flags(0),
        stream_id: Id::new(1).expect("fixture domain"),
    };
    assert!(
        WindowUpdate::parse(h, &[0, 0, 0])
            == Err(ParseError::FrameSize {
                kind: frame::numbers::Type::WindowUpdate,
                actual: 3,
                expected: 4,
            })
    );
}
