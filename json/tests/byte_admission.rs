pub mod common;

use common::values::{ESCAPED_TEXT, INVALID_PLAIN, INVALID_RAW, INVALID_TEXT};
use dope_test::alloc::Tracker;
use o3::buffer::write;
use sark_json::{Plain, PlainError, Raw, Text, writing::Writer};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn text_rejects_with_owner() {
    let owner = INVALID_TEXT.to_vec();
    let ptr = owner.as_ptr();
    let (owner, error) = Text::try_new(owner)
        .expect_err("invalid UTF-8")
        .into_parts();
    assert_eq!(error.valid_up_to(), 0);
    assert_eq!((owner.as_ptr(), owner.as_slice()), (ptr, INVALID_TEXT));
}

#[test]
fn plain_rejects_with_owner() {
    let owner = INVALID_PLAIN.to_vec();
    let ptr = owner.as_ptr();
    let (owner, error) = Plain::try_new(owner)
        .expect_err("escape required")
        .into_parts();
    assert_eq!(
        error,
        PlainError::Escape {
            index: 1,
            byte: b'"'
        }
    );
    assert_eq!((owner.as_ptr(), owner.as_slice()), (ptr, INVALID_PLAIN));
}

#[test]
fn raw_rejects_with_owner() {
    let owner = INVALID_RAW.to_vec();
    let ptr = owner.as_ptr();
    let (owner, error) = Raw::try_new(owner)
        .expect_err("invalid grammar")
        .into_parts();
    assert!(matches!(error, sark_json::Error::Invalid));
    assert_eq!((owner.as_ptr(), owner.as_slice()), (ptr, INVALID_RAW));
}

#[test]
fn admitted_writes_no_alloc() {
    let value = Text::try_new(INVALID_PLAIN)
        .map_err(|error| error.discard())
        .expect("UTF-8");
    let mut output = [0; ESCAPED_TEXT.len()];
    let mut sink = write::Slice::new(&mut output);
    let (complete, usage) = Tracker::usage(|| Writer::new(&mut sink).put_str(&value).is_continue());
    assert!(complete);
    assert_eq!(usage, (0, 0));
    assert_eq!(output.as_slice(), ESCAPED_TEXT);
}
