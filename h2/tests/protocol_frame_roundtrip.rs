use sark_core::manifold::stream::Boundary;
use sark_h2::{
    frame,
    frame::{
        Ack,
        controls::{Priority, RstStream, Settings},
        layout,
        layout::{Data, Headers, Sharing},
        numbers::{ErrorCode, Increment, SettingId},
        payload::{Continuation, GoAway, Payload, Ping, PushPromise, WindowUpdate},
    },
    stream::Id,
};

#[test]
fn data_roundtrip() {
    let result = Data::new(
        Id::new(7).expect("fixture domain"),
        Boundary::End,
        b"hello world",
    );
    let f = result.expect("valid frame fixture must parse");
    assert_roundtrip(Payload::Data(f));
}

#[test]
fn headers_plain() {
    let result = Headers::new(
        Id::new(3).expect("fixture domain"),
        Boundary::More,
        frame::Block::End,
        None,
        b"\x82\x86\x84",
    );
    let f = result.expect("valid frame fixture must parse");
    assert_roundtrip(Payload::Headers(f));
}

#[test]
fn priority_roundtrip() {
    let result = Priority::new(
        Id::new(9).expect("fixture domain"),
        layout::Priority {
            sharing: Sharing::Shared,
            dependency: Id::new(2).expect("fixture domain"),
            weight: 15,
        },
    );
    let f = result.expect("valid frame fixture must parse");
    assert_roundtrip(Payload::Priority(f));
}

#[test]
fn rst_roundtrip() {
    let result = RstStream::new(Id::new(13).expect("fixture domain"), ErrorCode::Cancel);
    let f = result.expect("valid frame fixture must parse");
    assert_roundtrip(Payload::RstStream(f));
}

#[test]
fn settings_empty_ack() {
    let result = Settings::new(Ack::Complete, &[]);
    let f = result.expect("valid frame fixture must parse");
    assert_roundtrip(Payload::Settings(f));
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
    let result = Settings::new(Ack::Required, &params);
    let f = result.expect("valid frame fixture must parse");
    assert_roundtrip(Payload::Settings(f));
    let mut it = f.iter();
    assert!((it.next()) == (Some((Some(SettingId::HeaderTableSize), 4096))));
    assert!((it.next()) == (Some((Some(SettingId::EnablePush), 0))));
    assert!((it.next()) == (Some((Some(SettingId::MaxConcurrentStreams), 100))));
    assert!((it.next()) == (Some((Some(SettingId::InitialWindowSize), 65535))));
    assert!((it.next()) == (Some((Some(SettingId::MaxFrameSize), 16384))));
    assert!((it.next()) == (Some((Some(SettingId::MaxHeaderListSize), 8192))));
    assert!((it.next()).is_none());
}

#[test]
fn push_roundtrip() {
    let result = PushPromise::new(
        Id::new(3).expect("fixture domain"),
        Id::new(4).expect("fixture domain"),
        frame::Block::End,
        b"\x82\x86",
    );
    let f = result.expect("valid frame fixture must parse");
    assert_roundtrip(Payload::PushPromise(f));
}

#[test]
fn ping_roundtrip() {
    let f = Ping {
        ack: Ack::Complete,
        opaque: [1, 2, 3, 4, 5, 6, 7, 8],
    };
    assert_roundtrip(Payload::Ping(f));
}

#[test]
fn goaway_empty() {
    let result = GoAway::new(Id::new(0).expect("fixture domain"), ErrorCode::NoError, &[]);
    let f = result.expect("valid frame fixture must parse");
    assert_roundtrip(Payload::GoAway(f));
}

#[test]
fn goaway_debug() {
    let result = GoAway::new(
        Id::new(101).expect("fixture domain"),
        ErrorCode::ProtocolError,
        b"goodbye",
    );
    let f = result.expect("valid frame fixture must parse");
    assert_roundtrip(Payload::GoAway(f));
}

#[test]
fn window_roundtrip() {
    let f = WindowUpdate {
        stream_id: Id::new(7).expect("fixture domain"),
        increment: Increment::new(65_535).expect("fixture domain"),
    };
    assert_roundtrip(Payload::WindowUpdate(f));
}

#[test]
fn window_conn() {
    let f = WindowUpdate {
        stream_id: Id::new(0).expect("fixture domain"),
        increment: Increment::new(1).expect("fixture domain"),
    };
    assert_roundtrip(Payload::WindowUpdate(f));
}

#[test]
fn cont_roundtrip() {
    let result = Continuation::new(
        Id::new(3).expect("fixture domain"),
        frame::Block::End,
        b"\x82\x86",
    );
    let f = result.expect("valid frame fixture must parse");
    assert_roundtrip(Payload::Continuation(f));
}

fn assert_roundtrip(frame: Payload<'_>) {
    let mut out = Vec::new();
    frame.encode(&mut out).expect("frame allocation");
    let result = Payload::parse(&out);
    let (parsed, consumed) = result.expect("valid frame fixture must parse");
    assert_eq!(consumed, out.len());
    assert!((parsed) == (frame));
}
