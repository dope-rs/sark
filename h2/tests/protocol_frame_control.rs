pub mod common;

use common::protocol::session::header;
use sark_h2::{
    frame,
    frame::{
        Ack,
        controls::Settings,
        numbers::{ErrorCode, Flags, ParseError, Type},
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
    let h = header(6, Type::Settings, Flags::ACK, 0);
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
    let h = header(0, Type::Settings, Flags::ACK, 3);
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
    let h = header(4, Type::PushPromise, 0, 0);
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
    let h = header(4, Type::PushPromise, 0, 1);
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
    let h = header(8, Type::Ping, 0, 1);
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
    let h = header(7, Type::GoAway, 0, 0);
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
    let h = header(4, Type::WindowUpdate, 0, 1);
    assert!(WindowUpdate::parse(h, &[0, 0, 0, 0]) == Err(ParseError::ZeroIncrement));
}

#[test]
fn window_size() {
    let h = header(3, Type::WindowUpdate, 0, 1);
    assert!(
        WindowUpdate::parse(h, &[0, 0, 0])
            == Err(ParseError::FrameSize {
                kind: frame::numbers::Type::WindowUpdate,
                actual: 3,
                expected: 4,
            })
    );
}
