use core::num::NonZero;

use dope_quic::errors::Code;
use sark_h3::{conn::io::Resume, frame::TYPE_HEADERS};
use sark_http::field::literal;

use crate::common::{
    conn::{Connection as _, Events, payload::Write as _},
    field::entry,
    ledger::Section as _,
    message::Frame as _,
    qpack,
};

pub mod common;

use dope_quic::wire::stream::Id;
use o3::buffer::{resident, storage};
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn::{
        Connection, Error,
        event::{Event, Role},
        io::Receive,
        options::{Config, Decode, Egress, Qpack, Streams},
        output::WritePrefix,
    },
    frame::{STREAM_TYPE_QPACK_DECODER, STREAM_TYPE_QPACK_ENCODER},
    link::{Handoff, Status},
    qpack::{encoder::Encoder, stream::Instruction},
};

#[test]
fn cancel_partial_headers() {
    let field = literal!(entry(b"x-retained", b"value"));
    let mut sender = Encoder::try_bounded(256, 1).expect("encoder");
    sender.set_blocked(1);
    sender.encode_for(8, [field]).expect("insert");
    let _ = sender.take_wire();
    let section = sender.encode_for(0, [field]).expect("section");
    assert!(section.is_dynamic());
    let mut wire = section.into_wire().try_frame(TYPE_HEADERS).expect("frame");
    wire.pop().expect("partial field section");
    let mut receiver = Connection::try_limited().expect("connection");
    receiver
        .control()
        .open_decoder(Id::new(3).expect("decoder"))
        .expect("prefix");
    receiver
        .control()
        .open_encoder(Id::new(7).expect("encoder"))
        .expect("wire budget full");
    Receive::new(&mut receiver)
        .owned(Id::ZERO, wire, Boundary::More)
        .expect("partial headers");
    let error = Receive::new(&mut receiver).reset(Id::ZERO, None, Code::from_u16(42));
    assert!(matches!(error, Err((Error::Feedback(_), _))));
    let _ = receiver
        .events()
        .poll_write()
        .expect("prefix")
        .expect("queued prefix");
    assert!(
        Receive::new(&mut receiver)
            .reset(Id::ZERO, None, Code::from_u16(42))
            .is_ok()
    );
    let cancel = receiver
        .events()
        .poll_write()
        .expect("feedback")
        .expect("cancel")
        .into_bytes();
    assert!(
        Instruction::decode(&cancel)
            == Ok(Some((Instruction::StreamCancellation { stream_id: 0 }, 1)))
    );
    sender.ingest(&cancel).expect("release references");
    assert!(
        sender
            .encode_for(4, [field])
            .expect("reused section")
            .is_dynamic()
    );
}

#[test]
fn reset_unblocks_next() {
    let dynamic = literal!(entry(b"x-dynamic", b"retained"));
    let mut sender = Encoder::try_bounded(256, 8).expect("fixed H3 QPACK scenario must succeed");
    sender.set_blocked(1);
    sender
        .encode_for(8, [dynamic])
        .expect("fixed H3 QPACK scenario must succeed");
    let code = sender.take_wire();
    let blocked = sender
        .encode_for(
            0,
            [
                literal!(entry(b":method", b"GET")),
                literal!(entry(b":scheme", b"https")),
                literal!(entry(b":authority", b"example.test")),
                literal!(entry(b":path", b"/blocked")),
                dynamic,
            ],
        )
        .expect("fixed H3 QPACK scenario must succeed");
    assert!(blocked.is_dynamic());

    let mut receiver = Connection::with_config(
        Role::Server,
        Config {
            qpack: Qpack {
                decoder: Decode {
                    table_bytes: 256,
                    blocked_streams: 1,
                },
                ..Qpack::default()
            },
            egress: Egress {
                write_capacity: 2,
                ..Egress::default()
            },
            ..Config::default()
        },
    )
    .expect("fixed H3 QPACK scenario must succeed");
    Receive::new(&mut receiver)
        .owned(
            Id::new(0).expect("stream id"),
            blocked
                .into_wire()
                .try_frame(TYPE_HEADERS)
                .expect("HEADERS frame"),
            Boundary::More,
        )
        .expect("fixed H3 QPACK scenario must succeed");
    assert!(
        Events::new(&mut receiver)
            .poll_token()
            .expect("fixed H3 QPACK scenario must succeed")
            .is_none()
    );
    let result =
        Receive::new(&mut receiver).reset(Id::new(0).expect("stream id"), None, Code::from_u16(42));
    assert!(
        result.is_ok(),
        "reset must cancel the blocked field section"
    );
    assert!(
        receiver
            .events()
            .poll_write()
            .expect("fixed H3 QPACK scenario must succeed")
            .is_none()
    );

    receiver
        .control()
        .open(Id::new(7).expect("stream id"))
        .expect("fixed H3 QPACK scenario must succeed");
    receiver
        .control()
        .open_decoder(Id::new(3).expect("stream id"))
        .expect("fixed H3 QPACK scenario must succeed");
    let output = receiver
        .events()
        .poll_write()
        .expect("fixed H3 QPACK scenario must succeed")
        .expect("QPACK decoder stream");
    assert!(output.stream_id() == Id::new(3).expect("stream id"));
    assert!(matches!(&output.prefix, WritePrefix::Inline(_)));
    let wire = output.into_bytes();
    let mut prefix = Vec::new();
    STREAM_TYPE_QPACK_DECODER
        .encode(&mut prefix)
        .expect("varint");
    let cancel = wire
        .strip_prefix(prefix.as_slice())
        .expect("decoder stream type prefix")
        .to_vec();
    assert!(
        Instruction::decode(&cancel)
            == Ok(Some((Instruction::StreamCancellation { stream_id: 0 }, 1,)))
    );
    assert!(
        receiver
            .events()
            .poll_write()
            .expect("fixed H3 QPACK scenario must succeed")
            .expect("fixed H3 QPACK scenario must succeed")
            .stream_id()
            == Id::new(7).expect("stream id")
    );
    assert!(matches!(
        Events::new(&mut receiver)
            .poll_token()
            .expect("fixed H3 QPACK scenario must succeed"),
        Some(Event::Reset {
            stream_id,
            error_code,
            ..
        }) if stream_id == Id::ZERO.into() && error_code == Code::from_u16(42)
    ));
    assert_eq!(
        sender
            .ingest(&cancel)
            .expect("fixed H3 QPACK scenario must succeed"),
        cancel.len()
    );

    let mut input = Vec::new();
    STREAM_TYPE_QPACK_ENCODER
        .encode(&mut input)
        .expect("varint");
    input.extend_from_slice(&code);
    Receive::new(&mut receiver)
        .owned(Id::new(2).expect("stream id"), input, Boundary::More)
        .expect("fixed H3 QPACK scenario must succeed");
    let output = receiver
        .events()
        .poll_write()
        .expect("fixed H3 QPACK scenario must succeed")
        .expect("insert count increment");
    assert!(matches!(&output.prefix, WritePrefix::Inline(_)));
    let feedback = output.into_bytes();
    assert_eq!(
        sender
            .ingest(&feedback)
            .expect("fixed H3 QPACK scenario must succeed"),
        feedback.len()
    );

    let next = sender
        .encode_for(
            4,
            [
                literal!(entry(b":method", b"GET")),
                literal!(entry(b":scheme", b"https")),
                literal!(entry(b":authority", b"example.test")),
                literal!(entry(b":path", b"/next")),
                dynamic,
            ],
        )
        .expect("fixed H3 QPACK scenario must succeed");
    assert!(next.is_dynamic());
    Receive::new(&mut receiver)
        .owned(
            Id::new(4).expect("stream id"),
            next.into_wire()
                .try_frame(TYPE_HEADERS)
                .expect("HEADERS frame"),
            Boundary::End,
        )
        .expect("fixed H3 QPACK scenario must succeed");
    let event = Events::new(&mut receiver)
        .poll_token()
        .expect("fixed H3 QPACK scenario must succeed");
    assert!(
        match event {
            Some(Event::Headers { message, head, .. }) => {
                message.stream_id() == Id::new(4).expect("stream id").into()
                    && head
                        .into_parts()
                        .0
                        .iter()
                        .any(|field| field == dynamic.as_record())
            }
            _ => false,
        },
        "expected headers for the next stream"
    );
}

#[test]
fn feedback_resumes_commit() {
    let mut receiver = Connection::with_config(
        Role::Client,
        Config {
            qpack: Qpack {
                decoder: Decode {
                    table_bytes: 128,
                    ..Decode::default()
                },
                ..Qpack::default()
            },
            egress: Egress {
                wire_bytes: 2,
                ..Egress::default()
            },
            ..Config::default()
        },
    )
    .expect("fixed H3 QPACK scenario must succeed");
    receiver
        .control()
        .open_decoder(Id::new(2).expect("stream id"))
        .expect("fixed H3 QPACK scenario must succeed");
    receiver
        .control()
        .open_encoder(Id::new(6).expect("encoder stream"))
        .expect("wire budget full");

    let mut input = Vec::new();
    STREAM_TYPE_QPACK_ENCODER
        .encode(&mut input)
        .expect("varint");
    qpack::Instruction::Capacity(128)
        .try_encode(&mut input)
        .expect("QPACK fixture");
    qpack::Instruction::Insert {
        name: b"x-transaction",
        value: b"once",
    }
    .try_encode(&mut input)
    .expect("QPACK fixture");
    qpack::Instruction::Duplicate(0)
        .try_encode(&mut input)
        .expect("QPACK fixture");
    Receive::new(&mut receiver)
        .owned(Id::new(3).expect("stream id"), input, Boundary::More)
        .expect("fixed H3 QPACK scenario must succeed");
    assert!(Receive::new(&mut receiver).is_blocked(Id::new(3).expect("stream id")));

    let prefix = receiver
        .events()
        .poll_write()
        .expect("fixed H3 QPACK scenario must succeed")
        .expect("decoder stream type owner");
    assert!(prefix.stream_id() == Id::new(2).expect("stream id"));
    let mut expected = Vec::new();
    STREAM_TYPE_QPACK_DECODER
        .encode(&mut expected)
        .expect("varint");
    assert_eq!(prefix.into_bytes(), expected);
    assert!(
        !Receive::new(&mut receiver).is_blocked(Id::new(3).expect("stream id")),
        "accepting the prefix must resume the exact retained encoder transaction"
    );

    let feedback = receiver
        .events()
        .poll_write()
        .expect("fixed H3 QPACK scenario must succeed")
        .expect("insert count feedback produced by the resumed commit");
    assert!(feedback.stream_id() == Id::new(2).expect("stream id"));
    assert!(
        Instruction::decode(&feedback.into_bytes())
            == Ok(Some((
                Instruction::InsertCountIncrement {
                    increment: NonZero::<u64>::MIN.saturating_add(1)
                },
                1,
            )))
    );
}

#[test]
fn credit_waits_for_unblock() {
    let dynamic = literal!(entry(b"x-credit", b"retained"));
    let mut sender = Encoder::try_bounded(256, 8).expect("fixed H3 QPACK scenario must succeed");
    sender.set_blocked(1);
    sender
        .encode_for(8, [dynamic])
        .expect("fixed H3 QPACK scenario must succeed");
    let code = sender.take_wire();
    let section = sender
        .encode_for(
            0,
            [
                literal!(entry(b":method", b"GET")),
                literal!(entry(b":scheme", b"https")),
                literal!(entry(b":authority", b"example.test")),
                literal!(entry(b":path", b"/credit")),
                dynamic,
            ],
        )
        .expect("fixed H3 QPACK scenario must succeed");
    let mut wire = section
        .into_wire()
        .try_frame(TYPE_HEADERS)
        .expect("HEADERS frame");
    let wire_len = wire.len();
    let owner =
        (storage::Shared::try_from(&mut wire)).expect("fixed H3 QPACK scenario must succeed");

    let mut receiver = Connection::with_config(
        Role::Server,
        Config {
            streams: Streams { capacity: 1 },
            qpack: Qpack {
                decoder: Decode {
                    table_bytes: 256,
                    blocked_streams: 1,
                },
                ..Qpack::default()
            },
            ..Config::default()
        },
    )
    .expect("fixed H3 QPACK scenario must succeed");
    assert!(matches!(
        Receive::new(&mut receiver).hold(
            Id::new(0).expect("stream id"),
            resident::Shared::from(owner),
            Boundary::More,
        ),
        Handoff::Accepted(Ok(Status::Suspended))
    ));
    assert!(
        receiver
            .events()
            .poll_credit()
            .expect("fixed H3 QPACK scenario must succeed")
            .is_none()
    );

    let mut input = Vec::new();
    Receive::new(&mut receiver)
        .borrowed(
            Id::new(2).expect("stream id"),
            &[0xc0, 0, 0, 0],
            Boundary::More,
        )
        .expect("fragmented critical stream type has reserved admission");
    input.extend_from_slice(&[0, 0, 0, 2]);
    input.extend_from_slice(&code);
    Receive::new(&mut receiver)
        .owned(Id::new(2).expect("stream id"), input, Boundary::More)
        .expect("fixed H3 QPACK scenario must succeed");
    assert!(
        receiver
            .events()
            .poll_credit()
            .expect("fixed H3 QPACK scenario must succeed")
            == Some((Id::new(0).expect("stream id"), wire_len, Resume::Ready))
    );
    assert!(
        receiver
            .events()
            .poll_credit()
            .expect("fixed H3 QPACK scenario must succeed")
            .is_none()
    );
    assert!(
        matches!(Events::new(&mut receiver).poll_token().expect("fixed H3 QPACK scenario must succeed"), Some(Event::Headers {
            message,
            ..
        }) if message.stream_id() == Id::new(0).expect("stream id").into() && message.boundary() == Boundary::More)
    );
}
