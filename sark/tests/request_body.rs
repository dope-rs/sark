pub mod common;
use common::request::*;
use sark::core::{
    body::{Contiguous, Project as _, admit::Retention},
    route::{Project, Schema},
};

#[test]
fn selects_storage() {
    assert_eq!(Raw::BODY_RETENTION, Retention::Retained);
    assert_eq!(Json::BODY_RETENTION, Retention::Retained);
    assert_eq!(Len::BODY_RETENTION, Retention::Discarded);
}

#[test]
fn decodes_json() {
    let (head, mut body) = projection::<Json>(br#"{"value":42}"#);
    Contiguous::try_project(&mut body, (), |scope| {
        let request = Json::try_view(&head, scope).expect("JSON request");
        assert_eq!(request.body.value, 42);
    })
    .expect("contiguous body");
}

#[test]
fn borrowed_json_is_wired() {
    let bytes = br#"{"text":"alpha","number":42}"#;
    let (head, mut body) = projection::<Plain>(bytes);
    Contiguous::try_project(&mut body, (), |scope| {
        let request = Plain::try_view(&head, scope).expect("borrowed JSON request");
        assert!(std::ptr::eq(request.body.text.as_slice(), &bytes[9..14]));
        assert!(std::ptr::eq(request.body.number.as_slice(), &bytes[25..27]));
    })
    .expect("contiguous body");
}
