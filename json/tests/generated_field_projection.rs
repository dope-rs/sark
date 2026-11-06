pub mod common;

use common::{
    members::OwnedItem,
    nested::Node,
    schema::{
        FIELD_VALUES, MISSING_FIELDS, NON_STRING_FIELDS, NULL_CHILDREN, NULL_FLAG, RenamedSequence,
        Scalars,
    },
};
use o3::buffer::storage::Shared;
use sark_json::{
    Error,
    parsing::{self, Decode, Request},
};

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
