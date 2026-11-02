use o3::buffer::{segments, storage};
use sark_grpc::message::{Deframer, Error, Payload};

#[test]
fn rejected_header() {
    for (flag, max, expected) in [
        (2, 1, Error::BadCompressionFlag(2)),
        (0, 0, Error::MessageTooLarge { len: 1, max: 0 }),
    ] {
        let pool = segments::Pool::try_new(segments::Plan {
            segments: 1,
            chains: 0,
        })
        .expect("one input segment");
        let mut source = pool.try_chain().expect("input owner");
        source
            .try_push(storage::Shared::try_copy(&[flag, 0, 0, 0, 1, b'x']).expect("wire bytes"))
            .expect("input slot");
        let mut input = Payload::from(source);
        let mut deframer = Deframer::new(max);
        for _ in 0..2 {
            assert_eq!(
                deframer.next(&mut input, &pool).err(),
                Some(expected.clone())
            );
        }
        assert_eq!(input.len(), 1);
        assert!(!deframer.is_idle());
        let mut empty = Payload::from(pool.try_chain().expect("empty input owner"));
        assert_eq!(deframer.next(&mut empty, &pool).err(), Some(expected));
    }
}

#[test]
fn retry_capacity() {
    let inputs = segments::Pool::try_new(segments::Plan {
        segments: 1,
        chains: 0,
    })
    .expect("one input segment");
    let wire = storage::Shared::try_copy(&[0, 0, 0, 0, 1, b'x']).expect("wire bytes");
    let pointer = wire.as_slice().as_ptr().wrapping_add(5);
    let mut source = inputs.into_chain();
    source.try_push(wire).expect("input slot");
    let mut input = Payload::from(source);
    let full = segments::Pool::try_new(segments::Plan {
        segments: 0,
        chains: 0,
    })
    .expect("empty output capacity");
    let mut deframer = Deframer::new(1);
    assert!(matches!(
        deframer.next(&mut input, &full),
        Err(Error::Transfer(segments::Error::SegmentCapacity(shortage)))
            if shortage.requested.get() == 1 && shortage.available == 0
    ));
    let ready = segments::Pool::try_new(segments::Plan {
        segments: 1,
        chains: 0,
    })
    .expect("one output segment");
    let frame = deframer
        .next(&mut input, &ready)
        .expect("capacity retry")
        .expect("retained admitted header")
        .into_plain()
        .expect("uncompressed frame must become plain");
    let bytes = frame.payload.contiguous().expect("one retained segment");
    assert_eq!(bytes, b"x");
    assert_eq!(bytes.as_ptr(), pointer);
    assert!(deframer.is_idle());
    assert!(input.is_empty());
}

#[test]
fn retries_partial_body() {
    let inputs = segments::Pool::try_new(segments::Plan {
        segments: 2,
        chains: 0,
    })
    .expect("two input segments");
    let outputs = segments::Pool::try_new(segments::Plan {
        segments: 2,
        chains: 1,
    })
    .expect("two output segments and one descriptor");
    let mut held = outputs.try_chain().expect("held output owner");
    held.try_push(storage::Shared::try_copy(b"held").expect("held bytes"))
        .expect("held output slot");
    let mut source = inputs.try_chain().expect("first input owner");
    source
        .try_push(storage::Shared::try_copy(&[0, 0, 0, 0, 2, b'a']).expect("partial wire bytes"))
        .expect("first input slot");
    let mut first = Payload::from(source);
    let mut deframer = Deframer::new(2);
    assert!(
        deframer
            .next(&mut first, &outputs)
            .expect("partial body")
            .is_none()
    );
    let mut source = inputs.into_chain();
    source
        .try_push(storage::Shared::try_copy(b"b").expect("remaining wire byte"))
        .expect("second input slot");
    let mut input = Payload::from(source);
    assert!(matches!(
        deframer.next(&mut input, &outputs),
        Err(Error::Transfer(segments::Error::SegmentCapacity(shortage)))
            if shortage.requested.get() == 1 && shortage.available == 0
    ));
    assert_eq!(input.len(), 1);
    drop(held);
    let frame = deframer
        .next(&mut input, &outputs)
        .expect("partial body retry")
        .expect("complete retained body");
    assert_eq!(
        frame
            .payload
            .segments()
            .flat_map(|part| part.as_slice().iter().copied())
            .collect::<Vec<_>>(),
        b"ab"
    );
    assert!(input.is_empty());
    assert!(deframer.is_idle());
}
