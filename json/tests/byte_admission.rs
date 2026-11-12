pub mod common;

use std::num::NonZero;

use common::values::{ESCAPED_TEXT, INVALID_PLAIN, INVALID_RAW, INVALID_TEXT, RAW_TEXT};
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
fn raw_from_text() {
    for input in [RAW_TEXT, r#""é한\uD83D\uDE00""#] {
        let original = (input.as_ptr(), input.as_bytes());
        let (raw, usage) =
            Tracker::usage(|| Raw::try_from(Text::from(input)).expect("raw grammar"));
        assert_eq!(usage, (0, 0));
        assert_eq!((raw.as_ptr(), raw.as_slice()), original);
    }

    for input in common::decode::INVALID
        .iter()
        .copied()
        .chain(["é".as_bytes(), b"null true"])
    {
        let (owner, error) = match std::str::from_utf8(input) {
            Ok(text) => {
                let (owner, error) = Raw::try_from(Text::from(text))
                    .expect_err("invalid grammar")
                    .into_parts();
                (owner.into_inner(), error)
            }
            Err(_) => Raw::try_new(input).expect_err("invalid UTF-8").into_parts(),
        };
        assert!(matches!(error, sark_json::Error::Invalid));
        assert_eq!((owner.as_ptr(), owner), (input.as_ptr(), input));
    }

    let text = Text::from(common::nested::Node::json(NonZero::new(65).expect("depth")));
    let original = (text.as_ptr(), text.len());
    let (text, error) = Raw::try_from(text).expect_err("depth limit").into_parts();
    assert!(matches!(error, sark_json::Error::Depth));
    assert_eq!((text.as_ptr(), text.len()), original);
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
