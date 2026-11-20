use crate::common::nested::Empty;
pub mod common;
use o3::buffer::{storage, write::Slice};
use sark_http::payload::response::encoded::Plan;
use sark_json::{
    parsing,
    writing::{Body, TrySize, Writer},
};

use crate::common::encode::*;

#[test]
fn escapes_adjacent() {
    let (value, expected) = ADJACENT;
    let mut out = Vec::new();
    assert!(Writer::new(&mut out).put_str(value).is_continue());
    assert_eq!(out, expected);
    assert_eq!(value.try_size().expect("size"), out.len());
}

#[test]
fn rejects_overflow() {
    let plan = Plan::try_exact(Body::new(Length::<0>))
        .map_err(|rejected| rejected.discard())
        .expect("size");
    let error = plan
        .write(&mut Slice::new(&mut []))
        .expect_err("overflow must report the exact sink failure");
    assert!(
        matches!(error, storage::BuildError::Build(_)),
        "unexpected overflow failure: {error:?}"
    );
    if let storage::BuildError::Build(error) = error {
        let (required, room) = error.into_parts();
        assert_eq!((required.get(), room), (1, 0));
    }
}

#[test]
fn empty_view_exact() {
    let value = <Empty as parsing::Request>::decode(b"{}").expect("decode empty JSON object");
    let plan = Plan::try_exact(Body::new(value))
        .map_err(|rejected| rejected.discard())
        .expect("size");
    assert_eq!(plan.limit(), 2);
    let mut bytes = [0; 2];
    plan.write(&mut Slice::new(&mut bytes))
        .expect("write exact empty JSON object");
    assert_eq!(&bytes, b"{}");
}
