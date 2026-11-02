use core::num::NonZero;

use sark_http::field::literal;

use crate::common::{
    field::entry,
    ledger::{Encoder as _, Section as _},
};
pub mod common;

use dope_test::alloc::Tracker;
use sark_h3::qpack::{
    encoder::Encoder,
    stream::{self, Instruction},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn releases_at_frontier() {
    let a = literal!(entry(b"x-a", b"a"));
    let b = literal!(entry(b"x-b", b"b"));
    let c = literal!(entry(b"x-c", b"c"));
    let mut harness = Encoder::try_dynamic(1024, 16, 8).expect("QPACK encoder");
    assert!(!harness.take_wire().is_empty());

    assert!(
        !harness
            .encode_for(60, [a])
            .expect("test section encodes")
            .is_dynamic()
    );
    assert!(!harness.take_wire().is_empty());
    assert!(
        harness
            .encode_for(0, [a])
            .expect("test section encodes")
            .is_dynamic()
    );

    assert!(
        !harness
            .encode_for(64, [b])
            .expect("test section encodes")
            .is_dynamic()
    );
    assert!(!harness.take_wire().is_empty());
    assert!(
        harness
            .encode_for(4, [b])
            .expect("test section encodes")
            .is_dynamic()
    );

    assert!(
        !harness
            .encode_for(68, [c])
            .expect("test section encodes")
            .is_dynamic()
    );
    assert!(!harness.take_wire().is_empty());
    harness.set_blocked(1);
    assert!(
        !harness
            .encode_for(8, [c])
            .expect("test section encodes")
            .is_dynamic()
    );

    let (used, len) = harness
        .try_feedback(Instruction::InsertCountIncrement {
            increment: NonZero::<u64>::MIN,
        })
        .expect("QPACK feedback");
    assert_eq!(used, len);
    assert!(
        !harness
            .encode_for(12, [c])
            .expect("test section encodes")
            .is_dynamic(),
        "one blocked stream must remain at the second threshold"
    );

    let (used, len) = harness
        .try_feedback(Instruction::InsertCountIncrement {
            increment: NonZero::<u64>::MIN,
        })
        .expect("QPACK feedback");
    assert_eq!(used, len);
    assert!(
        harness
            .encode_for(16, [c])
            .expect("test section encodes")
            .is_dynamic(),
        "reaching the second threshold must release both prior streams"
    );
}

#[test]
fn holds_until_last_section() {
    let a = literal!(entry(b"x-a", b"a"));
    let b = literal!(entry(b"x-b", b"b"));
    let c = literal!(entry(b"x-c", b"c"));
    let mut harness = Encoder::try_dynamic(1024, 16, 1).expect("QPACK encoder");
    assert!(!harness.take_wire().is_empty());

    assert!(
        !harness
            .encode_for(60, [a])
            .expect("test section encodes")
            .is_dynamic()
    );
    assert!(
        harness
            .encode_for(0, [a])
            .expect("test section encodes")
            .is_dynamic()
    );
    assert!(
        !harness
            .encode_for(64, [b])
            .expect("test section encodes")
            .is_dynamic()
    );
    assert!(
        harness
            .encode_for(0, [b])
            .expect("test section encodes")
            .is_dynamic()
    );
    assert!(
        !harness
            .encode_for(4, [b])
            .expect("test section encodes")
            .is_dynamic()
    );

    let (used, len) = harness
        .try_feedback(Instruction::InsertCountIncrement {
            increment: NonZero::<u64>::MIN,
        })
        .expect("QPACK feedback");
    assert_eq!(used, len);
    assert!(
        !harness
            .encode_for(8, [b])
            .expect("test section encodes")
            .is_dynamic(),
        "the later section must keep its stream in the blocked limit"
    );

    let (used, len) = harness
        .try_feedback(Instruction::InsertCountIncrement {
            increment: NonZero::<u64>::MIN,
        })
        .expect("QPACK feedback");
    assert_eq!(used, len);
    assert!(
        !harness
            .encode_for(68, [c])
            .expect("test section encodes")
            .is_dynamic()
    );
    assert!(
        harness
            .encode_for(12, [c])
            .expect("test section encodes")
            .is_dynamic(),
        "the last threshold must release the stream exactly once"
    );
}

#[test]
fn slots_survive_reuse() {
    const STREAMS: u64 = 64;

    let field = literal!(entry(b"x-stable", b"slot"));
    let mut harness = Encoder::try_dynamic(1024, STREAMS as usize, STREAMS).expect("QPACK encoder");
    assert!(!harness.take_wire().is_empty());
    assert!(
        !harness
            .encode_for(400, [field])
            .expect("test section encodes")
            .is_dynamic()
    );
    assert!(!harness.take_wire().is_empty());

    for index in 0..STREAMS {
        assert!(
            harness
                .encode_for(index * 4, [field])
                .expect("test section encodes")
                .is_dynamic()
        );
    }

    let mut wire = Vec::new();
    for index in (0..STREAMS).step_by(2) {
        Instruction::StreamCancellation {
            stream_id: index * 4,
        }
        .encode(&mut wire);
    }
    Instruction::InsertCountIncrement {
        increment: NonZero::<u64>::MIN,
    }
    .encode(&mut wire);
    for index in (1..STREAMS).step_by(2) {
        Instruction::SectionAcknowledgment {
            stream_id: index * 4,
        }
        .encode(&mut wire);
    }
    assert!(harness.ingest(&wire) == Ok(wire.len()));

    for index in 0..STREAMS {
        assert!(
            harness
                .encode_for(800 + index * 4, [field])
                .expect("test section encodes")
                .is_dynamic()
        );
    }
    assert!(
        !harness
            .encode_for(1200, [field])
            .expect("test section encodes")
            .is_dynamic(),
        "the section slab must expose its exact fixed capacity"
    );

    let mut stale = Vec::new();
    Instruction::SectionAcknowledgment { stream_id: 0 }.encode(&mut stale);
    assert!(harness.ingest(&stale) == Err(stream::Error::UnknownSection { stream_id: 0 }));
    let (used, len) = harness
        .try_feedback(Instruction::StreamCancellation { stream_id: 800 })
        .expect("QPACK feedback");
    assert_eq!(used, len);
    assert!(
        harness
            .encode_for(1204, [field])
            .expect("test section encodes")
            .is_dynamic(),
        "a rejected instruction must not erase the section ledger"
    );
}

#[test]
fn ledger_churn_is_stable() {
    let (mut encoder, (allocs, _)) = Tracker::usage(|| {
        Encoder::try_bounded(256, 1024).expect("QPACK allocation bounds must be valid")
    });
    assert_eq!(allocs, 2);
    assert!(!encoder.take_wire().is_empty());

    let field = literal!(entry(b"x-churn", b"stable"));
    let mut harness = Encoder::try_dynamic(256, 1, 1).expect("QPACK encoder");
    assert!(!harness.take_wire().is_empty());
    let mut output = Vec::with_capacity(64);
    harness
        .encode(4, [field], &mut output)
        .expect("initial field section must encode");
    assert!(!harness.take_wire().is_empty());
    output.clear();

    let mut ack = Vec::with_capacity(8);
    Instruction::SectionAcknowledgment { stream_id: 4 }.encode(&mut ack);
    let mut cancel = Vec::with_capacity(8);
    Instruction::StreamCancellation { stream_id: 4 }.encode(&mut cancel);

    let (_, (allocs, _)) = Tracker::usage(|| {
        for turn in 0..1024 {
            harness
                .encode(4, [field], &mut output)
                .expect("reused field section must encode");
            let input = if turn & 1 == 0 { &ack } else { &cancel };
            assert!(harness.ingest(input) == Ok(input.len()));
            output.clear();
        }
    });
    assert_eq!(allocs, 0);
}

#[test]
fn flat_sections_keep_stream_identity() {
    let field = literal!(entry(b"x-shared", b"value"));
    let mut harness = Encoder::try_dynamic(1024, 4, 2).expect("QPACK encoder");
    assert!(!harness.take_wire().is_empty());
    let mut output = Vec::new();
    harness
        .encode(400, [field], &mut output)
        .expect("seed insertion");
    assert_eq!(output[0], 0);
    for stream in [4, 8, 4, 8] {
        output.clear();
        harness
            .encode(stream, [field], &mut output)
            .expect("test section encodes");
        assert_ne!(output[0], 0);
    }
    let (used, len) = harness
        .try_feedback(Instruction::StreamCancellation { stream_id: 4 })
        .expect("QPACK feedback");
    assert_eq!(used, len);
    let (used, len) = harness
        .try_feedback(Instruction::SectionAcknowledgment { stream_id: 8 })
        .expect("QPACK feedback");
    assert_eq!(used, len);
    let (used, len) = harness
        .try_feedback(Instruction::SectionAcknowledgment { stream_id: 8 })
        .expect("QPACK feedback");
    assert_eq!(used, len);
    for stream in [12, 16, 12, 16] {
        output.clear();
        harness
            .encode(stream, [field], &mut output)
            .expect("test section encodes");
        assert_ne!(output[0], 0);
    }
    output.clear();
    harness
        .encode(20, [field], &mut output)
        .expect("test section encodes");
    assert_eq!(output[0], 0);
}

#[test]
fn cancels_after_rebucket() {
    let fields = [
        literal!(entry(b"x-first", b"one")),
        literal!(entry(b"x-second", b"two")),
        literal!(entry(b"x-third", b"three")),
    ];
    let mut harness = Encoder::try_dynamic(1024, 4, 2).expect("QPACK encoder");
    assert!(!harness.take_wire().is_empty());
    for field in fields {
        harness.encode_for(60, [field]).expect("seed insertion");
    }
    for stream in [0, 4] {
        assert!(
            harness
                .encode_for(stream, [fields[2]])
                .expect("blocked section")
                .is_dynamic()
        );
    }
    let (used, len) = harness
        .try_feedback(Instruction::InsertCountIncrement {
            increment: NonZero::<u64>::MIN.saturating_add(1),
        })
        .expect("QPACK feedback");
    assert_eq!(used, len);
    for stream in [4, 0] {
        let (used, len) = harness
            .try_feedback(Instruction::StreamCancellation { stream_id: stream })
            .expect("QPACK feedback");
        assert_eq!(used, len);
    }
    harness.set_blocked(1);
    assert!(
        harness
            .encode_for(8, [fields[2]])
            .expect("released blocked capacity")
            .is_dynamic()
    );
}
