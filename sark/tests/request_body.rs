pub mod common;
use common::request::*;
use sark::{
    core::{
        body::admit::Retention,
        route::{Project, Schema},
    },
    http::request::body,
};

#[test]
fn selects_storage() {
    assert_eq!(Raw::BODY_RETENTION, Retention::Retained);
    assert_eq!(Json::BODY_RETENTION, Retention::Retained);
    assert_eq!(Len::BODY_RETENTION, Retention::Discarded);
}

#[test]
fn decodes_json() {
    let parsed = <Json as Project>::parse_body(body::View::Slice(br#"{"value":42}"#))
        .expect("JSON request projection");
    assert_eq!(parsed.value, 42);
}

#[test]
fn borrowed_json_is_wired() {
    let bytes = br#"{"text":"alpha","number":42}"#;
    let body =
        <Plain as Project>::parse_body(body::View::Slice(bytes)).expect("request body parses");
    assert!(std::ptr::eq(body.text.as_slice(), &bytes[9..14]));
    assert!(std::ptr::eq(body.number.as_slice(), &bytes[25..27]));
}
