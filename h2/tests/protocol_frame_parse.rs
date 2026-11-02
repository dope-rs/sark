use sark_h2::{
    frame,
    frame::{
        Ack,
        controls::{Priority, RstStream, Settings},
        layout::Header,
        numbers::{Flags, Increment, Length, ParseError, Type},
        payload::{Continuation, Payload, Ping, WindowUpdate},
    },
    stream::Id,
};

#[test]
fn ping_size_beats_protocol() {
    let h = Header {
        length: Length::new(7).expect("fixture domain"),
        kind: Type::Ping,
        flags: Flags(0),
        stream_id: Id::new(1).expect("fixture domain"),
    };
    assert!(
        Ping::parse(h, &[0u8; 7])
            == Err(ParseError::FrameSize {
                kind: frame::numbers::Type::Ping,
                actual: 7,
                expected: 8,
            })
    );
}

#[test]
fn priority_checks_size() {
    let h = Header {
        length: Length::new(4).expect("fixture domain"),
        kind: Type::Priority,
        flags: Flags(0),
        stream_id: Id::new(0).expect("fixture domain"),
    };
    assert!(
        Priority::parse(h, &[0, 0, 0, 0])
            == Err(ParseError::FrameSize {
                kind: frame::numbers::Type::Priority,
                actual: 4,
                expected: 5,
            })
    );
}

#[test]
fn rst_size_beats_protocol() {
    let h = Header {
        length: Length::new(3).expect("fixture domain"),
        kind: Type::RstStream,
        flags: Flags(0),
        stream_id: Id::new(0).expect("fixture domain"),
    };
    assert!(
        RstStream::parse(h, &[0, 0, 0])
            == Err(ParseError::FrameSize {
                kind: frame::numbers::Type::RstStream,
                actual: 3,
                expected: 4,
            })
    );
}

#[test]
fn settings_checks_size() {
    let h = Header {
        length: Length::new(5).expect("fixture domain"),
        kind: Type::Settings,
        flags: Flags(0),
        stream_id: Id::new(3).expect("fixture domain"),
    };
    assert!(
        Settings::parse(h, &[0u8; 5])
            == Err(ParseError::SettingsSize {
                actual: 5,
                ack: Ack::Required,
            })
    );
}

#[test]
fn window_r_bit() {
    let h = Header {
        length: Length::new(4).expect("fixture domain"),
        kind: Type::WindowUpdate,
        flags: Flags(0),
        stream_id: Id::new(1).expect("fixture domain"),
    };
    let result = WindowUpdate::parse(h, &[0xff, 0xff, 0xff, 0xff]);
    let w = result.expect("valid frame fixture must parse");
    assert_eq!(w.increment.get(), 0x7fff_ffff);
}

#[test]
fn cont_zero_stream() {
    let h = Header {
        length: Length::new(0).expect("fixture domain"),
        kind: Type::Continuation,
        flags: Flags(0),
        stream_id: Id::new(0).expect("fixture domain"),
    };
    assert!(
        Continuation::parse(h, &[])
            == Err(ParseError::Stream {
                kind: h.kind,
                actual: h.stream_id,
            })
    );
}

#[test]
fn parse_need_more() {
    let f = Ping {
        ack: Ack::Required,
        opaque: [9u8; 8],
    };
    let mut out = Vec::new();
    Payload::Ping(f).encode(&mut out).expect("frame allocation");
    assert!(
        Payload::parse(&out[..out.len() - 1])
            == Err(ParseError::Need {
                actual: 16,
                required: 17,
            })
    );
}

#[test]
fn parse_two() {
    let mut out = Vec::new();
    Payload::Ping(Ping {
        ack: Ack::Required,
        opaque: [1u8; 8],
    })
    .encode(&mut out)
    .expect("frame allocation");
    let first = out.len();
    Payload::WindowUpdate(WindowUpdate {
        stream_id: Id::new(0).expect("fixture domain"),
        increment: Increment::new(1024).expect("fixture domain"),
    })
    .encode(&mut out)
    .expect("frame allocation");
    let result = Payload::parse(&out);
    let (f1, n1) = result.expect("valid frame fixture must parse");
    assert_eq!(n1, first);
    assert!(matches!(f1, Payload::Ping(p) if p.opaque == [1u8; 8]));
    let result = Payload::parse(&out[n1..]);
    let (f2, n2) = result.expect("valid frame fixture must parse");
    assert_eq!(n1 + n2, out.len());
    assert!(matches!(f2, Payload::WindowUpdate(w) if w.increment.get() == 1024));
}
