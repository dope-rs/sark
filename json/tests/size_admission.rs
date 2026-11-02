pub mod common;

use std::{cell::Cell, num::NonZero};

use common::encode::{Document, Value};
use sark_http::payload::response::exact;
use sark_json::writing::{Body, EncodeError, TryEncode};

#[test]
fn rejects_with_owner() {
    let sizes = Cell::new(0);
    let drops = Cell::new(0);
    let body = Body::new(Document {
        value: Value {
            len: usize::MAX,
            sizes: &sizes,
            drops: &drops,
        },
    });
    let rejected = exact::Plan::try_new(body).err().expect("overflow");
    assert_eq!(
        *rejected.error(),
        exact::Overflow {
            current: 9,
            added: NonZero::<usize>::MAX
        }
    );
    assert_eq!(drops.get(), 0);
    drop(rejected);
    assert_eq!(drops.get(), 1);
}

#[test]
fn sizes_once() {
    let sizes = Cell::new(0);
    let drops = Cell::new(0);
    let body = Body::new(Document {
        value: Value {
            len: 4,
            sizes: &sizes,
            drops: &drops,
        },
    });
    let plan = exact::Plan::try_new(body)
        .map_err(|rejected| rejected.discard())
        .expect("size");
    assert_eq!(plan.len(), 14);
    let encoded = plan.into_shared().expect("encode");
    assert_eq!(encoded.as_ref(), br#"{"value":true}"#);
    assert_eq!(sizes.get(), 1);
}

#[test]
fn rejects_size_mismatch() {
    let sizes = Cell::new(0);
    let drops = Cell::new(0);
    let value = Value {
        len: 1,
        sizes: &sizes,
        drops: &drops,
    };
    let mut out = b"prefix".to_vec();
    assert!(matches!(
        value.try_append(&mut out),
        Err(EncodeError::Capacity(_))
    ));
    assert_eq!(out, b"prefix");
}
