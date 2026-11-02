pub mod common;

use std::num::NonZero;

use common::{
    Bytes as _,
    schema::{
        DepthNode, Escaped, Exact, INVALID_IGNORED_VALUES, Optional, Ordered,
        PLAIN_SEQUENCE_ESCAPED, RAW_SEQUENCE, RawSequence, SAME_LENGTH_NAMES, SEQUENCE, Scalars,
        Sequence,
    },
};
use dope_test::alloc::Tracker;
use o3::{alloc, buffer::storage::Shared};
use sark_json::{
    Error,
    parsing::{self, Decode, Request},
    segment::{Decoder, Progress},
    writing::TryEncode,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn borrowed_scalars() {
    let value = Tracker::fail_after(0, || {
        <Scalars as Decode>::decode_ref(br#"{"value":7,"flag":true,"token":[1]}"#)
    })
    .expect("scalar view transfer without allocation");
    assert_eq!(value.value, Some(7));
    assert!(value.flag);
    assert_eq!(value.token.as_bytes(), b"[1]");
}

#[test]
fn same_length_names() {
    let value = <Scalars as Request>::decode(SAME_LENGTH_NAMES).expect("name selection");
    assert_eq!(value.value, Some(7));
    assert_eq!(value.token.as_bytes(), b"[1]");
    assert!(value.flag);
}

#[test]
fn ignored_value_grammar() {
    for input in INVALID_IGNORED_VALUES {
        assert!(matches!(
            <Scalars as Request>::decode(input),
            Err(parsing::Error::Parse(Error::Invalid)),
        ));
    }
}

#[test]
fn complete_document() {
    for input in [b"{\"value\":1}junk".as_slice(), b"{\"value\":1,}"] {
        assert!(<Optional as Request>::decode(input).is_err());
        assert!(<Ordered as Decode>::decode_ref(input).is_err());
    }
    assert!(<Exact as Request>::decode(b"{\"value\":1").is_err());
    assert!(<Exact as Request>::decode(b"{garbage\"value\":1}").is_err());
    assert_eq!(
        <Exact as Request>::decode(b"{\"value\":1} \r\n\t")
            .expect("complete exact document")
            .value,
        1,
    );
}

#[test]
fn optional_presence() {
    let first = <Optional as Request>::decode(b"{\"value\":null,\"value\":7}")
        .expect("first null retained");
    assert_eq!(first.value, None);
    assert_eq!(
        <Optional as Decode>::decode_ref(b"{}")
            .expect("missing optional")
            .value,
        None,
    );
    assert_eq!(
        <Ordered as Request>::decode(b"{\"value\":7}")
            .expect("optional value")
            .value,
        Some(7),
    );
    for (input, expected) in [
        (br#"{"value":null,"value":7}"#.as_slice(), None),
        (
            br#"{"value":18446744073709551615,"ignored":18446744073709551616,"value":-1.5e+2}"#,
            Some(u64::MAX),
        ),
    ] {
        let mut decoder =
            Decoder::<Optional>::new(input.try_segments().expect("segments")).expect("decoder");
        assert!(matches!(
            decoder.poll(NonZero::<usize>::MAX),
            Progress::Ready(Optional { value }) if value == expected
        ));
    }
}

#[test]
fn sequence_alloc_failure() {
    let input = SEQUENCE;
    for allows in [0, 1] {
        let result = Tracker::fail_after(allows, || <Sequence as Request>::decode(input));
        assert!(matches!(
            result,
            Err(parsing::Error::Parse(Error::Allocation(
                alloc::Error::Exhausted,
            ))),
        ));
    }
    assert!(matches!(
        Tracker::fail_after(0, || <Sequence as Request>::decode(PLAIN_SEQUENCE_ESCAPED)),
        Err(parsing::Error::Parse(Error::Allocation(
            alloc::Error::Exhausted,
        ))),
    ));
    let (decoded, activity) = Tracker::activity(|| <Sequence as Request>::decode(input));
    let decoded = decoded.expect("sequence");
    assert_eq!(decoded.values.len(), 9);
    assert!(activity.allocations < decoded.values.len());
    assert_eq!(decoded.values[0].as_ptr(), input[12..].as_ptr());
}

#[test]
fn plain_sequence() {
    assert!(matches!(
        <Sequence as Decode>::decode(
            Shared::try_copy(PLAIN_SEQUENCE_ESCAPED).expect("plain sequence owner"),
        ),
        Err(parsing::Error::Parse(Error::Invalid)),
    ));
}

#[test]
fn raw_sequence() {
    assert_eq!(
        <RawSequence as Request>::decode(RAW_SEQUENCE)
            .expect("borrowed raw sequence")
            .try_encode()
            .expect("borrowed raw sequence encoding"),
        RAW_SEQUENCE,
    );
    let input = RAW_SEQUENCE.try_segments().expect("raw sequence owners");
    let mut decoder = Decoder::<RawSequence>::new(input).expect("raw decoder");
    let decoded = loop {
        let next = decoder.poll(NonZero::<usize>::MIN);
        assert!(!matches!(next, Progress::NeedInput | Progress::Failed(_)));
        if let Progress::Ready(value) = next {
            break value;
        }
    };
    assert_eq!(
        decoded.try_encode().expect("raw sequence encoding"),
        RAW_SEQUENCE
    );
}

#[test]
fn known_depth() {
    let valid = DepthNode::json(NonZero::new(64).expect("nodes"), "", 0);
    <DepthNode as Request>::decode(valid.as_bytes()).expect("known nesting within limit");
    let invalid = DepthNode::json(NonZero::new(65).expect("nodes"), "", 0);
    assert!(matches!(
        <DepthNode as Request>::decode(invalid.as_bytes()),
        Err(parsing::Error::Parse(Error::Depth)),
    ));
}

#[test]
fn raw_depth() {
    for field in ["ignored", "extra"] {
        let valid = DepthNode::json(NonZero::new(63).expect("nodes"), field, 4);
        <DepthNode as Request>::decode(valid.as_bytes()).expect("remaining nesting allowance");
        let invalid = DepthNode::json(NonZero::new(63).expect("nodes"), field, 5);
        assert!(matches!(
            <DepthNode as Request>::decode(invalid.as_bytes()),
            Err(parsing::Error::Parse(Error::Depth)),
        ));
    }
}

#[test]
fn escaped_wire_name() {
    let wire = Escaped {
        __sark_json_marker: 7,
    }
    .try_encode()
    .expect("encode escaped name");
    assert_eq!(wire, br#"{"quote\"slash\\line\u000a\u001f":7}"#);
    assert_eq!(
        <Escaped as Request>::decode(&wire)
            .expect("borrowed escaped name")
            .__sark_json_marker,
        7,
    );
    assert_eq!(
        <Escaped as Decode>::decode(Shared::try_copy(&wire).expect("owner"),)
            .expect("owned escaped name")
            .__sark_json_marker,
        7,
    );
}
