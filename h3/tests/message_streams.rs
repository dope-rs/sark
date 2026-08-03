use dope_quic::varint::VarInt;
use sark_core::http::Field;
use sark_h3::qpack::{DecoderError, Encoder, EncoderInstruction};
use sark_h3::{
    Config, Conn, ConnError, Event, Frame, HeaderSection, Role, STREAM_TYPE_CONTROL,
    STREAM_TYPE_PUSH, STREAM_TYPE_QPACK_ENCODER, Settings, StreamId, TYPE_CANCEL_PUSH, TYPE_DATA,
    TYPE_HEADERS, TYPE_PUSH_PROMISE, TYPE_SETTINGS,
};

fn frame(kind: VarInt, payload: &[u8]) -> Vec<u8> {
    let mut wire = Vec::new();
    Frame::encode(kind, payload, &mut wire).unwrap();
    wire
}

fn push_wire(push_id: u64, frames: &[u8]) -> Vec<u8> {
    let mut wire = Vec::new();
    STREAM_TYPE_PUSH.encode(&mut wire);
    VarInt::new(push_id).unwrap().encode(&mut wire);
    wire.extend_from_slice(frames);
    wire
}

fn push_promise_frame(push_id: u64, block: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    VarInt::new(push_id).unwrap().encode(&mut payload);
    payload.extend_from_slice(block);
    frame(TYPE_PUSH_PROMISE, &payload)
}

fn encoder_wire(instructions: &[u8]) -> Vec<u8> {
    let mut wire = Vec::new();
    STREAM_TYPE_QPACK_ENCODER.encode(&mut wire);
    wire.extend_from_slice(instructions);
    wire
}

fn cancel_push_control_wire(push_id: u64) -> Vec<u8> {
    let mut wire = Vec::new();
    STREAM_TYPE_CONTROL.encode(&mut wire);
    Frame::encode(TYPE_SETTINGS, &[], &mut wire).unwrap();
    Frame::encode_varint(TYPE_CANCEL_PUSH, VarInt::new(push_id).unwrap(), &mut wire).unwrap();
    wire
}

fn settings_control_wire(settings: &Settings) -> Vec<u8> {
    let mut payload = Vec::new();
    settings.encode(&mut payload).unwrap();
    let mut wire = Vec::new();
    STREAM_TYPE_CONTROL.encode(&mut wire);
    Frame::encode(TYPE_SETTINGS, &payload, &mut wire).unwrap();
    wire
}

fn header_block(name: &[u8], value: &[u8]) -> Vec<u8> {
    let mut block = Vec::new();
    Encoder::new().encode([Field::new(name, value)], &mut block);
    block
}

fn request_header_block() -> Vec<u8> {
    let mut block = Vec::new();
    Encoder::new().encode(
        [
            Field::new(b":method", b"GET"),
            Field::new(b":scheme", b"https"),
            Field::new(b":path", b"/"),
        ],
        &mut block,
    );
    block
}

fn blocked_header(request: bool) -> (Vec<u8>, Vec<u8>) {
    let field = Field::new(b"x-dynamic", b"retained");
    let mut encoder = Encoder::with_dynamic_capacity(256);
    encoder.set_dynamic_capacity(256).unwrap();
    encoder.set_max_blocked_streams(1);
    let mut first = Vec::new();
    encoder.encode([field], &mut first);
    let instructions = encoder.take_encoder_instructions();
    let mut referenced = Vec::new();
    if request {
        encoder.encode(
            [
                Field::new(b":method", b"GET"),
                Field::new(b":scheme", b"https"),
                Field::new(b":path", b"/"),
                field,
            ],
            &mut referenced,
        );
    } else {
        encoder.encode([Field::new(b":status", b"200"), field], &mut referenced);
    }
    (referenced, instructions)
}

fn dynamic_receiver(role: Role) -> Conn {
    Conn::with_config(
        role,
        Config {
            local_settings: Settings {
                qpack_max_table_capacity: 256,
                qpack_blocked_streams: 1,
                ..Settings::default()
            },
            ..Config::default()
        },
    )
    .unwrap()
}

#[test]
fn request_and_push_streams_share_message_transitions() {
    let trailers = header_block(b"x-trailer", b"done");
    let mut request_frames = frame(TYPE_HEADERS, &request_header_block());
    request_frames.extend(frame(TYPE_DATA, b"body"));
    request_frames.extend(frame(TYPE_HEADERS, &trailers));

    let mut request = Conn::with_role(Role::Server);
    request
        .ingest_stream_owned(StreamId::new(0), request_frames, true)
        .unwrap();
    assert!(matches!(
        request.poll_event(),
        Some(Event::Headers {
            section: HeaderSection::Initial,
            ..
        })
    ));
    assert!(matches!(
        request.poll_event(),
        Some(Event::Data { data, .. }) if data.as_slice() == b"body"
    ));
    assert!(matches!(
        request.poll_event(),
        Some(Event::Headers {
            section: HeaderSection::TrailingEnd,
            ..
        })
    ));
    assert!(matches!(
        request.poll_event(),
        Some(Event::Finished {
            stream_id: StreamId(0)
        })
    ));

    let mut push_frames = frame(TYPE_HEADERS, &header_block(b":status", b"200"));
    push_frames.extend(frame(TYPE_DATA, b"body"));
    push_frames.extend(frame(TYPE_HEADERS, &trailers));
    let mut push = Conn::with_role(Role::Client);
    push.ingest_stream_owned(StreamId::new(3), push_wire(9, &push_frames), true)
        .unwrap();
    assert!(matches!(
        push.poll_event(),
        Some(Event::PushHeaders {
            push_id: 9,
            section: HeaderSection::Initial,
            ..
        })
    ));
    assert!(matches!(
        push.poll_event(),
        Some(Event::PushData {
            push_id: 9,
            data,
            ..
        }) if data.as_slice() == b"body"
    ));
    assert!(matches!(
        push.poll_event(),
        Some(Event::PushHeaders {
            push_id: 9,
            section: HeaderSection::TrailingEnd,
            ..
        })
    ));
    assert!(matches!(
        push.poll_event(),
        Some(Event::PushFinished {
            stream_id: StreamId(3),
            push_id: 9,
        })
    ));
}

#[test]
fn qpack_unblocks_request_and_push_streams_through_the_same_parser() {
    let (request_block, request_instructions) = blocked_header(true);
    let mut request = dynamic_receiver(Role::Server);
    request
        .ingest_stream_owned(StreamId::new(0), frame(TYPE_HEADERS, &request_block), false)
        .unwrap();
    assert!(request.poll_event().is_none());
    request
        .ingest_stream_owned(StreamId::new(2), encoder_wire(&request_instructions), false)
        .unwrap();
    assert!(matches!(
        request.poll_event(),
        Some(Event::Headers {
            section: HeaderSection::Initial,
            ..
        })
    ));

    let (push_block, push_instructions) = blocked_header(false);
    let mut push = dynamic_receiver(Role::Client);
    push.ingest_stream_owned(
        StreamId::new(3),
        push_wire(11, &frame(TYPE_HEADERS, &push_block)),
        false,
    )
    .unwrap();
    assert!(push.poll_event().is_none());
    push.ingest_stream_owned(StreamId::new(7), encoder_wire(&push_instructions), false)
        .unwrap();
    assert!(matches!(
        push.poll_event(),
        Some(Event::PushHeaders {
            push_id: 11,
            section: HeaderSection::Initial,
            ..
        })
    ));
}

#[test]
fn qpack_encoder_capacity_is_bounded_by_peer_settings() {
    let mut conn = dynamic_receiver(Role::Server);
    assert_eq!(conn.set_qpack_encoder_capacity(0), Err(ConnError::Protocol));
    conn.start_qpack_encoder_stream(StreamId::new(3)).unwrap();
    assert!(conn.poll_write().is_some());
    assert_eq!(
        conn.set_qpack_encoder_capacity(1),
        Err(ConnError::Qpack(DecoderError::EncoderStream))
    );

    let peer = Settings {
        qpack_max_table_capacity: 64,
        ..Settings::default()
    };
    conn.ingest_stream_owned(StreamId::new(2), settings_control_wire(&peer), false)
        .unwrap();
    assert_eq!(conn.poll_event(), Some(Event::Settings(peer)));
    conn.set_qpack_encoder_capacity(64).unwrap();
    assert_eq!(
        conn.set_qpack_encoder_capacity(65),
        Err(ConnError::Qpack(DecoderError::EncoderStream))
    );
}

#[test]
fn qpack_encoder_instructions_survive_write_backpressure() {
    let mut conn = Conn::with_config(
        Role::Server,
        Config {
            write_capacity: 1,
            ..Config::default()
        },
    )
    .unwrap();
    conn.start_qpack_encoder_stream(StreamId::new(3)).unwrap();
    let peer = Settings {
        qpack_max_table_capacity: 64,
        ..Settings::default()
    };
    conn.ingest_stream_owned(StreamId::new(2), settings_control_wire(&peer), false)
        .unwrap();
    assert_eq!(conn.poll_event(), Some(Event::Settings(peer)));

    assert_eq!(
        conn.set_qpack_encoder_capacity(64),
        Err(ConnError::Overload)
    );
    assert!(conn.poll_write().is_some());
    assert_eq!(
        conn.send_headers(StreamId::new(0), [Field::new(b":status", b"200")], false,),
        Err(ConnError::Overload)
    );
    let instruction = conn.poll_write().unwrap();
    assert_eq!(instruction.stream_id, StreamId::new(3));
    assert!(matches!(
        EncoderInstruction::decode(instruction.prefix.as_slice()),
        Ok((EncoderInstruction::SetCapacity(64), _))
    ));

    conn.send_headers(StreamId::new(0), [Field::new(b":status", b"200")], false)
        .unwrap();
}

#[test]
fn push_promise_is_a_compile_time_request_stream_capability() {
    let mut promised = Vec::new();
    Encoder::new().encode(
        [
            Field::new(b":method", b"GET"),
            Field::new(b":scheme", b"https"),
            Field::new(b":path", b"/asset"),
        ],
        &mut promised,
    );
    let promise = push_promise_frame(17, &promised);

    let mut request = Conn::with_role(Role::Client);
    request
        .ingest_stream_owned(StreamId::new(0), promise.clone(), false)
        .unwrap();
    assert!(matches!(
        request.poll_event(),
        Some(Event::PushPromise {
            stream_id: StreamId(0),
            push_id: 17,
            ..
        })
    ));

    let mut push = Conn::with_role(Role::Client);
    assert_eq!(
        push.ingest_stream_owned(StreamId::new(3), push_wire(9, &promise), false),
        Err(ConnError::FrameUnexpected)
    );
}

#[test]
fn cancel_push_is_received_only_by_servers() {
    let wire = cancel_push_control_wire(17);
    let mut client = Conn::with_role(Role::Client);
    assert_eq!(
        client.ingest_stream_owned(StreamId::new(3), wire.clone(), false),
        Err(ConnError::FrameUnexpected)
    );

    let mut server = Conn::with_role(Role::Server);
    server
        .ingest_stream_owned(StreamId::new(2), wire, false)
        .unwrap();
    assert!(matches!(server.poll_event(), Some(Event::Settings(_))));
    assert_eq!(server.poll_event(), Some(Event::CancelPush { push_id: 17 }));
}
