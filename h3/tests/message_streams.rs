use dope_quic::varint::VarInt;
use sark_core::http::Field;
use sark_h3::qpack::Encoder;
use sark_h3::{
    Conn, ConnError, Event, Frame, Role, STREAM_TYPE_PUSH, STREAM_TYPE_QPACK_ENCODER, Settings,
    StreamId, TYPE_DATA, TYPE_HEADERS, TYPE_PUSH_PROMISE,
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

fn header_block(name: &[u8], value: &[u8]) -> Vec<u8> {
    let mut block = Vec::new();
    Encoder::new().encode([Field::new(name, value)], &mut block);
    block
}

fn blocked_header() -> (Vec<u8>, Vec<u8>) {
    let field = Field::new(b"x-dynamic", b"retained");
    let mut encoder = Encoder::with_dynamic_capacity(256);
    encoder.set_dynamic_capacity(256).unwrap();
    encoder.set_max_blocked_streams(1);
    let mut first = Vec::new();
    encoder.encode([field], &mut first);
    let instructions = encoder.take_encoder_instructions();
    let mut referenced = Vec::new();
    encoder.encode([field], &mut referenced);
    (referenced, instructions)
}

fn dynamic_receiver(role: Role) -> Conn {
    Conn::with_role_and_settings(
        role,
        Settings {
            qpack_max_table_capacity: 256,
            qpack_blocked_streams: 1,
            ..Settings::default()
        },
    )
}

#[test]
fn request_and_push_streams_share_message_transitions() {
    let headers = header_block(b":status", b"200");
    let trailers = header_block(b"x-trailer", b"done");
    let mut frames = frame(TYPE_HEADERS, &headers);
    frames.extend(frame(TYPE_DATA, b"body"));
    frames.extend(frame(TYPE_HEADERS, &trailers));

    let mut request = Conn::with_role(Role::Server);
    request
        .ingest_stream_owned(StreamId::new(0), frames.clone(), true)
        .unwrap();
    assert!(matches!(
        request.poll_event(),
        Some(Event::Headers {
            trailing: false,
            ..
        })
    ));
    assert!(matches!(
        request.poll_event(),
        Some(Event::Data { data, .. }) if data.as_slice() == b"body"
    ));
    assert!(matches!(
        request.poll_event(),
        Some(Event::Headers { trailing: true, .. })
    ));
    assert!(matches!(
        request.poll_event(),
        Some(Event::Finished {
            stream_id: StreamId(0)
        })
    ));

    let mut push = Conn::with_role(Role::Client);
    push.ingest_stream_owned(StreamId::new(3), push_wire(9, &frames), true)
        .unwrap();
    assert!(matches!(
        push.poll_event(),
        Some(Event::PushHeaders {
            push_id: 9,
            trailing: false,
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
            trailing: true,
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
    let (request_block, request_instructions) = blocked_header();
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
            trailing: false,
            ..
        })
    ));

    let (push_block, push_instructions) = blocked_header();
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
            trailing: false,
            ..
        })
    ));
}

#[test]
fn push_promise_is_a_compile_time_request_stream_capability() {
    let promise = push_promise_frame(17, &header_block(b":path", b"/asset"));

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
