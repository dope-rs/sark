use core::num::NonZero;

use sark_h3::conn::Error;
use sark_http::field::literal;

use crate::common::{
    conn::{Events, build::Fields as _},
    field::entry,
    ledger::{Encoder as _, Section as _},
    message::promise::Authority,
    qpack,
};
pub mod common;

use dope_quic::wire::stream::Id;
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn::{
        Connection,
        event::{Event, Role},
        io::Receive,
        options::{Config, Decode, Qpack},
    },
    frame::{STREAM_TYPE_QPACK_DECODER, STREAM_TYPE_QPACK_ENCODER},
    qpack::{decoder, encoder::Encoder, stream::Instruction},
};
use sark_http::payload::compress::dynamic::insert;

#[test]
fn blocked_stream_fifo() {
    let a = literal!(entry(b"x-a", b"a"));
    let b = literal!(entry(b"x-b", b"b"));
    let mut encoder = Encoder::try_dynamic(256, 8, 1).expect("QPACK encoder");
    assert!(!encoder.take_wire().is_empty());

    assert!(
        !encoder
            .encode_for(0, [a])
            .expect("fixed QPACK test contract must hold")
            .is_dynamic()
    );
    assert!(!encoder.take_wire().is_empty());
    assert!(
        encoder
            .encode_for(0, [a])
            .expect("fixed QPACK test contract must hold")
            .is_dynamic()
    );

    assert!(
        !encoder
            .encode_for(4, [b])
            .expect("fixed QPACK test contract must hold")
            .is_dynamic()
    );
    assert!(!encoder.take_wire().is_empty());
    assert!(
        !encoder
            .encode_for(4, [b])
            .expect("fixed QPACK test contract must hold")
            .is_dynamic()
    );

    assert!(
        encoder
            .encode_for(0, [b])
            .expect("fixed QPACK test contract must hold")
            .is_dynamic()
    );
    let (used, len) = encoder
        .try_feedback(Instruction::SectionAcknowledgment { stream_id: 0 })
        .expect("QPACK feedback");
    assert_eq!(used, len);
    assert!(
        !encoder
            .encode_for(4, [b])
            .expect("fixed QPACK test contract must hold")
            .is_dynamic()
    );

    let (used, len) = encoder
        .try_feedback(Instruction::SectionAcknowledgment { stream_id: 0 })
        .expect("QPACK feedback");
    assert_eq!(used, len);
    assert!(
        encoder
            .encode_for(4, [b])
            .expect("fixed QPACK test contract must hold")
            .is_dynamic()
    );
}

#[test]
fn ack_advances_fifo() {
    let a = literal!(entry(b"x-a", b"a"));
    let b = literal!(entry(b"x-b", b"b"));
    let mut encoder = Encoder::try_dynamic(256, 8, 2).expect("QPACK encoder");
    assert!(!encoder.take_wire().is_empty());

    encoder
        .encode_for(8, [a])
        .expect("fixed QPACK test contract must hold");
    assert!(!encoder.take_wire().is_empty());
    assert!(
        encoder
            .encode_for(0, [a])
            .expect("fixed QPACK test contract must hold")
            .is_dynamic()
    );
    encoder
        .encode_for(8, [b])
        .expect("fixed QPACK test contract must hold");
    assert!(!encoder.take_wire().is_empty());
    assert!(
        encoder
            .encode_for(0, [b])
            .expect("fixed QPACK test contract must hold")
            .is_dynamic()
    );

    encoder.set_blocked(0);
    let (used, len) = encoder
        .try_feedback(Instruction::SectionAcknowledgment { stream_id: 0 })
        .expect("QPACK feedback");
    assert_eq!(used, len);
    assert!(
        !encoder
            .encode_for(4, [b])
            .expect("fixed QPACK test contract must hold")
            .is_dynamic(),
        "the first ACK must prove only the first section's insert count"
    );

    let (used, len) = encoder
        .try_feedback(Instruction::SectionAcknowledgment { stream_id: 0 })
        .expect("QPACK feedback");
    assert_eq!(used, len);
    assert!(
        encoder
            .encode_for(12, [b])
            .expect("fixed QPACK test contract must hold")
            .is_dynamic(),
        "the second FIFO ACK releases the newer required insert count"
    );
}

#[test]
fn cancel_reuses_slot() {
    let field = literal!(entry(b"x-slot", b"one"));
    let mut encoder = Encoder::try_dynamic(256, 1, 8).expect("QPACK encoder");
    assert!(!encoder.take_wire().is_empty());

    encoder
        .encode_for(0, [field])
        .expect("fixed QPACK test contract must hold");
    assert!(!encoder.take_wire().is_empty());
    assert!(
        encoder
            .encode_for(0, [field])
            .expect("fixed QPACK test contract must hold")
            .is_dynamic()
    );
    assert!(
        !encoder
            .encode_for(4, [field])
            .expect("fixed QPACK test contract must hold")
            .is_dynamic(),
        "ledger exhaustion must degrade compression, not response liveness"
    );

    let (used, len) = encoder
        .try_feedback(Instruction::StreamCancellation { stream_id: 0 })
        .expect("QPACK feedback");
    assert_eq!(used, len);
    assert!(
        encoder
            .encode_for(4, [field])
            .expect("fixed QPACK test contract must hold")
            .is_dynamic()
    );

    let mut stale = Vec::new();
    Instruction::StreamCancellation { stream_id: 0 }.encode(&mut stale);
    assert_eq!(
        encoder.ingest(&stale),
        Ok(stale.len()),
        "a stale stream id cannot release a recycled generational slot"
    );
}

#[test]
fn cancel_after_request_fin_is_delivered() {
    let mut conn = Connection::with_role(Role::Server).expect("connection");
    let request = Authority.try_headers().expect("request HEADERS");
    Receive::new(&mut conn)
        .borrowed(Id::ZERO, &request, Boundary::End)
        .expect("completed request");
    while Events::new(&mut conn)
        .poll_token()
        .expect("request")
        .is_some()
    {}

    let mut feedback = vec![STREAM_TYPE_QPACK_DECODER.get() as u8];
    Instruction::StreamCancellation { stream_id: 0 }.encode(&mut feedback);
    Receive::new(&mut conn)
        .borrowed(
            Id::new(2).expect("decoder stream"),
            &feedback,
            Boundary::More,
        )
        .expect("cancellation after request FIN");
    assert!(matches!(
        Events::new(&mut conn).poll_token().expect("typed cancellation"),
        Some(Event::QpackCancelled { stream_id }) if stream_id == Id::ZERO
    ));
}

/// A zero required insert count cannot expose an existing post-base entry.
#[test]
fn zero_rejects_dynamic() {
    let mut decoder = decoder::Decoder::try_dynamic(256, 128).expect("test decoder counts");
    let mut input = Vec::new();
    qpack::Instruction::Capacity(128)
        .try_encode(&mut input)
        .expect("QPACK fixture");
    decoder
        .ingest(&input)
        .expect("fixed QPACK test contract must hold");
    input.clear();
    qpack::Instruction::Insert {
        name: b"x-present",
        value: b"must-not-be-visible",
    }
    .try_encode(&mut input)
    .expect("QPACK fixture");
    decoder
        .ingest(&input)
        .expect("fixed QPACK test contract must hold");
    assert_eq!(decoder.insert_count(), 1);

    assert_eq!(
        decoder
            .decode(&[0x00, 0x00, 0x10])
            .err()
            .expect("dynamic reference requires a nonzero insertion count"),
        decoder::Error::DynamicDisabled {
            byte: 0x10,
            index: 0,
        }
    );
}

#[test]
fn reject_rolls_back() {
    let mut decoder = decoder::Decoder::try_dynamic(1024, 64).expect("test decoder counts");
    let mut input = Vec::new();
    qpack::Instruction::Insert {
        name: b"x-invalid",
        value: b"before-capacity",
    }
    .try_encode(&mut input)
    .expect("QPACK fixture");

    assert_eq!(
        decoder.ingest(&input),
        Err(decoder::ingress::error::Error::TableCapacity(
            insert::Capacity {
                required: 32,
                limit: 0,
            }
        ))
    );
    assert_eq!(decoder.insert_count(), 0);
}

#[test]
fn error_preserves_budget() {
    let mut receiver = Connection::with_config(
        Role::Server,
        Config {
            qpack: Qpack {
                decoder: Decode {
                    table_bytes: 128,
                    ..Decode::default()
                },
                ..Qpack::default()
            },
            ..Config::default()
        },
    )
    .expect("fixed QPACK test contract must hold");
    let mut input = Vec::new();
    STREAM_TYPE_QPACK_ENCODER
        .encode(&mut input)
        .expect("varint");
    qpack::Instruction::Capacity(128)
        .try_encode(&mut input)
        .expect("QPACK fixture");
    qpack::Instruction::Insert {
        name: b"x-before-error",
        value: b"retained",
    }
    .try_encode(&mut input)
    .expect("QPACK fixture");
    input.push(0x01);

    assert!(
        Receive::new(&mut receiver).owned(Id::new(2).expect("stream id"), input, Boundary::More)
            == Err(Error::QpackEncoderStream(
                decoder::ingress::error::Error::DynamicReference {
                    relative: 1,
                    resident: 1,
                }
            ))
    );
    assert_eq!(receiver.wire_len(), 1);
    receiver
        .control()
        .open_decoder(Id::new(3).expect("stream id"))
        .expect("fixed QPACK test contract must hold");
    assert!(
        receiver
            .events()
            .poll_write()
            .expect("fixed QPACK test contract must hold")
            .expect("decoder stream feedback owner")
            .stream_id()
            == Id::new(3).expect("stream id")
    );
    assert_eq!(receiver.wire_len(), 0);
}

#[test]
fn refs_block_eviction() {
    let a = literal!(entry(b"x-a", b"a"));
    let b = literal!(entry(b"x-b", b"b"));
    let mut encoder = Encoder::try_dynamic(64, 4, 2).expect("QPACK encoder");
    assert!(!encoder.take_wire().is_empty());

    encoder
        .encode_for(8, [a])
        .expect("fixed QPACK test contract must hold");
    assert!(!encoder.take_wire().is_empty());

    encoder
        .encode_for(16, [b])
        .expect("unreferenced field remains encodable");
    assert!(
        encoder.take_wire().is_empty(),
        "an unacknowledged insertion cannot be evicted before any field references it"
    );

    assert!(
        encoder
            .encode_for(0, [a, b])
            .expect("fixed QPACK test contract must hold")
            .is_dynamic()
    );
    assert!(
        encoder.take_wire().is_empty(),
        "a later field cannot evict an entry referenced by its own section"
    );
    encoder
        .encode_for(4, [b])
        .expect("fixed QPACK test contract must hold");
    assert!(
        encoder.take_wire().is_empty(),
        "another section cannot evict an outstanding reference"
    );

    let (used, len) = encoder
        .try_feedback(Instruction::StreamCancellation { stream_id: 0 })
        .expect("QPACK feedback");
    assert_eq!(used, len);
    encoder
        .encode_for(12, [b])
        .expect("fixed QPACK test contract must hold");
    assert!(
        encoder.take_wire().is_empty(),
        "cancellation releases references without acknowledging table receipt"
    );
    let (used, len) = encoder
        .try_feedback(Instruction::InsertCountIncrement {
            increment: NonZero::<u64>::MIN,
        })
        .expect("insertion receipt");
    assert_eq!(used, len);
    assert!(
        encoder
            .encode_for(20, [a, b])
            .expect("received entry stays referenced within the section")
            .is_dynamic()
    );
    assert!(
        encoder.take_wire().is_empty(),
        "same-section references protect received entries"
    );
    encoder
        .encode_for(24, [b])
        .expect("another section remains encodable");
    assert!(
        encoder.take_wire().is_empty(),
        "outstanding references protect received entries"
    );
    let (used, len) = encoder
        .try_feedback(Instruction::StreamCancellation { stream_id: 20 })
        .expect("received section cancellation");
    assert_eq!(used, len);
    encoder
        .encode_for(28, [b])
        .expect("received unreferenced entry can be evicted");
    assert!(!encoder.take_wire().is_empty());
}

#[test]
fn selected_ref_eviction() {
    let a = literal!(entry(b"x-a", b"a"));
    let b = literal!(entry(b"x-b", b"b"));
    let mut encoder = Encoder::try_dynamic(72, 4, 0).expect("QPACK encoder");
    assert!(!encoder.take_wire().is_empty());
    for stream in [0, 4] {
        assert!(
            !encoder
                .encode_for(stream, [a])
                .expect("speculative insertion")
                .is_dynamic()
        );
    }
    assert!(!encoder.take_wire().is_empty());
    let (used, len) = encoder
        .try_feedback(Instruction::InsertCountIncrement {
            increment: NonZero::new(2).expect("insert count"),
        })
        .expect("received duplicate entries");
    assert_eq!(used, len);
    assert_eq!(
        encoder
            .encode_for(8, [a])
            .expect("newest received reference")
            .lines(),
        &[0x80]
    );
    encoder
        .encode_for(12, [b])
        .expect("unreferenced older duplicate can be evicted");
    assert_eq!(&encoder.take_wire()[..], b"\x43x-b\x01b");
}
