pub mod common;

use std::num::NonZero;

use common::{
    Bytes as _,
    members::OwnedItem,
    nested::Node,
    schema::{
        FIELD_VALUES, MISSING_FIELDS, NON_STRING_FIELDS, NULL_CHILDREN, NULL_FLAG, RenamedSequence,
        SIGNED_INVALID, SIGNED_VALUES, Scalars, Signed,
    },
};
use o3::buffer::storage::Shared;
use sark_json::{
    Error,
    parsing::{self, Decode, Request},
    segment::{Decoder, Progress},
};

#[test]
fn signed_fields() {
    for (input, expected) in SIGNED_VALUES {
        let value = <Signed as Request>::decode(input).expect("signed request");
        assert_eq!((value.price, value.quantity), expected);
        let mut decoder =
            Decoder::<Signed>::new(input.try_segments().expect("segments")).expect("decoder");
        assert!(matches!(
            decoder.poll(NonZero::<usize>::MAX),
            Progress::Ready(value) if (value.price, value.quantity) == expected,
        ));
    }
    for input in SIGNED_INVALID {
        assert!(matches!(
            <Signed as Request>::decode(input),
            Err(parsing::Error::Parse(Error::Invalid)),
        ));
        let mut decoder =
            Decoder::<Signed>::new(input.try_segments().expect("segments")).expect("decoder");
        let error = loop {
            match decoder.poll(NonZero::<usize>::MAX) {
                Progress::Pending => {}
                Progress::Failed(error) => break error,
                _ => panic!("invalid signed value"),
            }
        };
        assert!(matches!(error, parsing::Error::Parse(Error::Invalid)));
    }
}

#[test]
fn field_null_presence() {
    for (input, expected) in FIELD_VALUES {
        let value = <Scalars as Decode>::decode(Shared::try_copy(input).expect("input owner"))
            .expect("field projection");
        assert_eq!(value.value, expected);
        assert_eq!(value.token.as_bytes(), b"null");
    }
}

#[test]
fn required_scalar_errors() {
    for input in [
        br#"{"value":-0,"flag":true,"token":null}"#.as_slice(),
        br#"{"value":-1,"flag":true,"token":null}"#,
    ] {
        assert!(matches!(
            <Scalars as Decode>::decode(Shared::try_copy(input).expect("input owner")),
            Err(parsing::Error::Parse(Error::Invalid)),
        ));
    }
    assert!(matches!(
        <Scalars as Decode>::decode(Shared::try_copy(MISSING_FIELDS).expect("input owner")),
        Err(parsing::Error::Missing("flag")),
    ));
    assert!(matches!(
        <Scalars as Decode>::decode(Shared::try_copy(NULL_FLAG).expect("input owner"),),
        Err(parsing::Error::Parse(Error::Invalid)),
    ));
}

#[test]
fn required_compound_errors() {
    assert!(matches!(
        <Node as Decode>::decode(Shared::try_copy(MISSING_FIELDS).expect("input owner")),
        Err(parsing::Error::Missing("children")),
    ));
    assert!(matches!(
        <Node as Decode>::decode(Shared::try_copy(NULL_CHILDREN).expect("input owner"),),
        Err(parsing::Error::Parse(Error::Invalid)),
    ));
}

#[test]
fn string_fields_reject_other_values() {
    for input in NON_STRING_FIELDS {
        assert!(matches!(
            <OwnedItem as Request>::decode(input),
            Err(parsing::Error::Parse(Error::Invalid)),
        ));
        assert!(matches!(
            <RenamedSequence as Request>::decode(input),
            Err(parsing::Error::Parse(Error::Invalid)),
        ));
        assert!(matches!(
            <OwnedItem as Decode>::decode(Shared::try_copy(input).expect("owner")),
            Err(parsing::Error::Parse(Error::Invalid)),
        ));
        assert!(matches!(
            <RenamedSequence as Decode>::decode(Shared::try_copy(input).expect("owner")),
            Err(parsing::Error::Parse(Error::Invalid)),
        ));
    }
}
