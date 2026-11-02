pub mod common;

use common::schema::{Escaped, MISSING_FIELDS, RenamedSequence};
use o3::buffer::storage::Shared;
use sark_json::parsing::{Decode, Error, Request};

#[test]
fn missing_fields_use_wire_names() {
    assert!(matches!(
        <Escaped as Request>::decode(MISSING_FIELDS),
        Err(Error::Missing("quote\"slash\\line\n\u{1f}")),
    ));
    assert!(matches!(
        <Escaped as Decode>::decode(Shared::try_copy(MISSING_FIELDS).expect("input owner")),
        Err(Error::Missing("quote\"slash\\line\n\u{1f}")),
    ));
    assert!(matches!(
        <RenamedSequence as Decode>::decode_ref(MISSING_FIELDS),
        Err(Error::Missing("wire-values")),
    ));
}
