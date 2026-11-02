use core::num::NonZero;

use sark_http::field::literal;

use crate::common::{
    conn::{Events, build::Settings as _, payload::Write as _},
    field::entry,
    message::promise::Authority,
    qpack,
    qpack::{NeverValue, Wire as _},
};
pub mod common;

use dope_quic::wire::{stream::Id, varint};
use dope_test::alloc::Tracker;
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn::{
        Connection, Error,
        event::{Event, Role},
        io::{Receive, Send},
        options,
        options::plan,
    },
    frame::{self, ErrorCode, ParseError, Payload, Settings},
    qpack::{
        decoder::Decoder,
        encoder::{self, Encoder},
        stream::{self, Instruction},
    },
};
use sark_http::field::{
    self, Indexing, Lower, Value,
    metadata::{block::Connect, section::rules, strings::Encoding},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn generated_failure() {
    let value = vec![b'v'; 8192];
    let source = NeverValue(
        literal!(Lower::try_bytes(b"x-generated")),
        Value::new(value.as_slice()).expect("valid value"),
    );
    let mut encoder = Encoder::new();
    let mut output = b"prefix".to_vec();
    let failed = Tracker::fail_after(1, || encoder.encode(0, &source, &mut output));
    assert_eq!(
        failed,
        Err(encoder::Error::Allocation(o3::alloc::Error::Exhausted))
    );
    assert_eq!(output, b"prefix");

    encoder.encode(0, &source, &mut output).expect("retry");
    let mut decoder = Decoder::try_new(value.len() + 64).expect("decoder");
    let fields = decoder.decode(&output[6..]).expect("generated field");
    assert!(
        fields.iter().eq([entry(b"x-generated", &value)
            .expect("field")
            .with_index(Indexing::Never)
            .as_record()])
    );
}

#[test]
fn peer_count_wrap() {
    let mut sender = Connection::with_config(
        Role::Server,
        options::Config {
            qpack: options::Qpack {
                encoder: options::Encode {
                    table_bytes: 256,
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .expect("sender");
    Send::new(&mut sender)
        .headers(
            Id::ZERO,
            [literal!(entry(b":status", b"200"))],
            Boundary::More,
        )
        .expect("initial static section");
    assert!(
        sender
            .events()
            .poll_write()
            .expect("initial write")
            .is_some()
    );
    let settings = Settings::new(
        varint::VarInt::from_u16(4096),
        None,
        varint::VarInt::from_u8(1),
        Connect::Disabled,
    );
    Receive::new(&mut sender)
        .owned(
            Id::new(2).expect("control id"),
            settings.try_wire().expect("settings"),
            Boundary::More,
        )
        .expect("settings input");
    assert!(matches!(
        Events::new(&mut sender)
            .poll_token()
            .expect("settings event"),
        Some(Event::Settings(_))
    ));
    let id = Id::new(3).expect("encoder id");
    sender.control().open_encoder(id).expect("encoder stream");
    sender
        .events()
        .poll_write()
        .expect("stream prefix")
        .expect("stream prefix");
    sender.control().set_capacity(256).expect("active capacity");
    let mut decoder = Decoder::try_dynamic(256, 4096).expect("decoder");
    let capacity = sender
        .events()
        .poll_write()
        .expect("capacity")
        .expect("capacity")
        .into_bytes();
    decoder.ingest(&capacity).expect("capacity input");
    let feedback = Id::new(6).expect("decoder id");
    Receive::new(&mut sender)
        .borrowed(feedback, &[3], Boundary::More)
        .expect("decoder stream prefix");
    for ordinal in 0..16 {
        let name = format!("x-{ordinal}");
        Send::new(&mut sender)
            .headers(
                Id::ZERO,
                [entry(name.as_bytes(), b"v").expect("field fixture")],
                Boundary::More,
            )
            .expect("literal section");
        while let Some(write) = sender.events().poll_write().expect("write") {
            if write.stream_id() == id {
                decoder.ingest(&write.into_bytes()).expect("insertion");
            }
        }
        assert_eq!(decoder.insert_count(), ordinal + 1);
        Receive::new(&mut sender)
            .borrowed(feedback, &[1], Boundary::More)
            .expect("insertion receipt");
    }
    Send::new(&mut sender)
        .headers(Id::ZERO, [literal!(entry(b"x-15", b"v"))], Boundary::More)
        .expect("dynamic section");
    let wire = sender
        .events()
        .poll_write()
        .expect("header")
        .expect("header")
        .into_bytes();
    let block = match Payload::parse(&wire, 256).expect("frame") {
        (Payload::Headers(block), _) => Some(block),
        _ => None,
    }
    .expect("headers");
    let fields = decoder
        .decode(block)
        .expect("peer maximum decodes wrapped reference");
    assert!(
        fields
            .iter()
            .eq([literal!(entry(b"x-15", b"v")).as_record()])
    );
}

#[test]
fn never_index_forward() {
    let mut decoder = Decoder::try_dynamic(256, 256).expect("decoder");
    decoder
        .ingest(b"\x3f\xe1\x01\x41x\x01y")
        .expect("table entry");
    let fields = decoder
        .decode(b"\x02\x80\x08\x01z")
        .expect("never-index literal");
    assert!(
        fields.iter().eq([literal!(entry(b"x", b"z"))
            .with_index(Indexing::Never)
            .as_record()])
    );
    for encoded in [
        b"\x02\x00\x60\x01z".as_slice(),
        b"\x00\x00\x31x\x01z".as_slice(),
    ] {
        let fields = decoder.decode(encoded).expect("other literal forms");
        assert!(
            fields.iter().eq([literal!(entry(b"x", b"z"))
                .with_index(Indexing::Never)
                .as_record()])
        );
    }
    let mut encoder = Encoder::try_table(256).expect("forward encoder");
    encoder.set_blocked(1);
    encoder
        .encode_for(0, [literal!(entry(b"x", b"z"))])
        .expect("seed matching entry");
    drop(encoder.take_wire());
    let section = encoder
        .encode_for(
            4,
            fields
                .iter()
                .map(|field| {
                    entry(field.name, field.value)
                        .expect("field fixture")
                        .with_index(field.indexing)
                })
                .collect::<Vec<_>>(),
        )
        .expect("forward never-index field");
    assert_eq!(section.lines()[0] & 0x30, 0x30);
    assert!(encoder.take_wire().is_empty());
    let source = NeverValue(
        field::literal!(Lower::try_bytes(b"x")),
        field::literal!(Value::try_bytes(b"z")),
    );
    let section = encoder
        .encode_for(8, source)
        .expect("value never-index field");
    assert_eq!(section.lines()[0] & 0x30, 0x30);

    let mut conn = Connection::with_config(
        Role::Client,
        options::Config {
            qpack: options::Qpack {
                decoder: options::Decode {
                    table_bytes: 256,
                    blocked_streams: 1,
                },
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .expect("connection");
    Send::new(&mut conn)
        .headers(Id::ZERO, Authority, Boundary::End)
        .expect("originating request");
    Receive::new(&mut conn)
        .owned(
            Id::new(3).expect("encoder id"),
            b"\x02\x3f\xe1\x01\x41x\x01y".to_vec(),
            Boundary::More,
        )
        .expect("encoder input");
    Events::new(&mut conn)
        .poll_token()
        .expect("insertion processed");
    Receive::new(&mut conn)
        .owned(
            Id::ZERO,
            b"\x01\x06\x02\x80\xd9\x08\x01z".to_vec(),
            Boundary::More,
        )
        .expect("response input");
    let fields = match Events::new(&mut conn)
        .poll_token()
        .expect("planned never-index literal")
    {
        Some(Event::Headers { head, .. }) => Some(head.into_parts().0),
        _ => None,
    }
    .expect("headers");
    assert_eq!(
        fields
            .iter()
            .find(|field| field.name == b"x")
            .expect("literal field")
            .indexing,
        Indexing::Never
    );
}

#[test]
fn cancel_without_refs() {
    for mut encoder in [
        Encoder::new(),
        Encoder::try_table(256).expect("dynamic encoder"),
    ] {
        encoder
            .encode_for(0, [literal!(entry(b":status", b"200"))])
            .expect("static section");
        let mut cancel = Vec::new();
        Instruction::StreamCancellation { stream_id: 0 }.encode(&mut cancel);
        assert_eq!(encoder.ingest(&cancel), Ok(cancel.len()));
        let mut ack = Vec::new();
        Instruction::SectionAcknowledgment { stream_id: 0 }.encode(&mut ack);
        assert_eq!(
            encoder.ingest(&ack),
            Err(stream::Error::UnknownSection { stream_id: 0 })
        );
    }
}

#[test]
fn initial_capacity_zero() {
    let mut conn = Connection::with_config(
        Role::Server,
        options::Config {
            qpack: options::Qpack {
                decoder: options::Decode {
                    table_bytes: 256,
                    blocked_streams: 1,
                },
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .expect("connection");
    let error = Receive::new(&mut conn)
        .owned(
            Id::new(2).expect("encoder id"),
            b"\x02\x41x\x01y".to_vec(),
            Boundary::More,
        )
        .and_then(|()| Events::new(&mut conn).poll_token().map(|_| ()))
        .expect_err("capacity instruction required");
    assert_eq!(error.error_code(), ErrorCode::QpackEncoderStream);
}

#[test]
fn bounded_payload_error() {
    let short = varint::Error::Underflow {
        actual: 0,
        required: 1,
    };
    assert!(
        matches!(Payload::parse(b"\x07\x00", 256), Err(ParseError::PayloadInteger(error)) if error == short)
    );
    let mut conn = Connection::with_role(Role::Server).expect("connection");
    Receive::new(&mut conn)
        .owned(
            Id::new(2).expect("control id"),
            b"\x00\x04\x02\x40".to_vec(),
            Boundary::More,
        )
        .expect("partial settings payload");
    let error = Receive::new(&mut conn)
        .owned(
            Id::new(2).expect("control id"),
            b"\x01\x00".to_vec(),
            Boundary::More,
        )
        .and_then(|()| Events::new(&mut conn).poll_token().map(|_| ()))
        .expect_err("bounded settings integer");
    assert!(matches!(error, Error::Parse(ParseError::PayloadInteger(cause)) if cause == short));
    assert_eq!(error.error_code(), ErrorCode::Payload);
}

#[test]
fn reserved_frames() {
    for kind in [2, 6, 8, 9] {
        assert!(matches!(
            Payload::parse(&[kind, 0], 256),
            Err(ParseError::ReservedFrame { .. })
        ));
        let mut conn = Connection::with_role(Role::Server).expect("connection");
        let error = Receive::new(&mut conn)
            .owned(Id::ZERO, vec![kind, 0], Boundary::More)
            .and_then(|()| Events::new(&mut conn).poll_token().map(|_| ()))
            .expect_err("reserved frame type");
        assert_eq!(error.error_code(), ErrorCode::FrameUnexpected);
    }
    assert!(matches!(
        Payload::parse(&[0x21, 0], 256),
        Ok((Payload::Unknown { .. }, 2))
    ));
}

#[test]
fn discarded_value_grammar() {
    for encoding in [Encoding::Plain, Encoding::Huffman] {
        let block = encoding.bad_literal().expect("invalid literal fixture");
        let mut wire = Vec::new();
        Payload::encode(frame::TYPE_HEADERS, &block, &mut wire).expect("headers");
        let mut conn: Connection<'_, rules::Discard> = plan::Config::new(Default::default())
            .expect("plan")
            .build(Role::Server)
            .expect("connection");
        let error = Receive::new(&mut conn)
            .owned(Id::ZERO, wire, Boundary::More)
            .and_then(|()| Events::new(&mut conn).poll_token().map(|_| ()))
            .expect_err("discard does not bypass value validation");
        assert_eq!(error.error_code(), ErrorCode::Message);
    }
}

#[test]
fn indexed_value_grammar() {
    let (code, block) = qpack::Instruction::bad_indexed().expect("invalid indexed fixture");
    let mut conn: Connection<'_, rules::Discard> = plan::Config::new(options::Config {
        qpack: options::Qpack {
            decoder: options::Decode {
                table_bytes: 256,
                blocked_streams: 1,
            },
            ..Default::default()
        },
        ..Default::default()
    })
    .expect("plan")
    .build(Role::Server)
    .expect("connection");
    Receive::new(&mut conn)
        .owned(Id::new(2).expect("encoder id"), code, Boundary::More)
        .expect("table input");
    Events::new(&mut conn).poll_token().expect("table updated");

    let mut wire = Vec::new();
    Payload::encode(frame::TYPE_HEADERS, &block, &mut wire).expect("headers");
    let error = Receive::new(&mut conn)
        .owned(Id::ZERO, wire, Boundary::More)
        .and_then(|()| Events::new(&mut conn).poll_token().map(|_| ()))
        .expect_err("indexed value validity reaches discarded policy");
    assert_eq!(error.error_code(), ErrorCode::Message);
}

#[test]
fn decoder_stream_progress() {
    let mut encoder = Encoder::new();
    assert_eq!(encoder.ingest(b"\x40\x7f"), Ok(1));
    assert_eq!(encoder.ingest(b"\x7f\x00"), Ok(2));
    assert!(matches!(
        Instruction::decode(b"\x00"),
        Err(stream::Error::ZeroIncrement)
    ));
    assert!(
        Instruction::decode(b"\x01")
            == Ok(Some((
                Instruction::InsertCountIncrement {
                    increment: NonZero::<u64>::MIN
                },
                1,
            )))
    );
}
