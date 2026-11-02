pub mod common;

use common::{
    nested::Node,
    schema::{FIELD_VALUES, MISSING_FIELDS, NULL_CHILDREN, NULL_FLAG, Scalars},
};
use o3::buffer::storage::Shared;
use sark_json::{
    Error,
    parsing::{self, Decode},
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
