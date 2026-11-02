use common::{
    Bytes as _,
    decode::{General, PlainBody},
};
pub mod common;

use dope_test::alloc::Tracker;
use o3::buffer::storage::AllocError;
use sark_json::{parsing::Request, writing::TryEncode};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn plain_borrows() {
    let input = br#"{"text":"alpha","number":42}"#;
    let (body, (alloc, _)) =
        Tracker::usage(|| <PlainBody as Request>::decode(input).expect("decode"));
    assert_eq!(alloc, 0);
    assert_eq!(body.text, b"alpha");
    assert_eq!(body.number, b"42");
    assert!(body.text.as_slice().is_within(input));
    assert!(body.number.as_slice().is_within(input));
    assert_eq!(body.try_encode().expect("encode").as_slice(), input);
    assert_eq!(
        size_of_val(&body.text),
        size_of::<&[u8]>(),
        "plain request fields stay two-word borrowed slices",
    );
}

#[test]
fn general_storage() {
    let wire = br#"{"text":"alpha","number":42}"#;
    let (mut plain, (alloc, _)) =
        Tracker::usage(|| <General as Request>::decode(wire).expect("plain decode"));
    assert_eq!(alloc, 0);
    assert_eq!(plain.text.as_slice(), b"alpha");
    assert!(plain.text.as_slice().is_within(wire));
    assert!(plain.number.as_slice().is_within(wire));
    assert_eq!(
        size_of_val(&plain.text),
        3 * size_of::<usize>(),
        "general request JSON bytes stay a three-word Cow",
    );
    let rejected = Tracker::fail_after(0, || plain.text.try_share())
        .expect_err("borrowed copy allocation fails");
    let (text, error) = rejected.into_parts();
    assert!(matches!(error, AllocError::Exhausted(_)));
    plain.text = text;
    assert!(plain.text.as_slice().is_within(wire));
    let owned = plain
        .text
        .try_share()
        .map_err(|rejected| rejected.discard())
        .expect("copy plain field into shared test storage");
    assert_eq!(owned.as_slice(), b"alpha");
    assert!(!owned.as_slice().is_within(wire));

    let wire = br#"{"text":"a\"b","number":42}"#;
    let (mut escaped, (alloc, _)) =
        Tracker::usage(|| <General as Request>::decode(wire).expect("escaped decode"));
    assert_eq!(
        alloc, 1,
        "only the escaped field receives a decode allocation",
    );
    assert_eq!(escaped.text.as_slice(), b"a\"b");
    assert!(!escaped.text.as_slice().is_within(wire));
    assert!(escaped.number.as_slice().is_within(wire));
    assert_eq!(escaped.try_encode().expect("encode").as_slice(), wire);

    let ptr = escaped.text.as_slice().as_ptr();
    let rejected = Tracker::fail_after(0, || escaped.text.try_share())
        .expect_err("owned control block allocation fails");
    let (text, error) = rejected.into_parts();
    assert!(matches!(error, AllocError::Exhausted(_)));
    escaped.text = text;
    assert_eq!(escaped.text.as_slice().as_ptr(), ptr);
    let owned = escaped
        .text
        .try_share()
        .map_err(|rejected| rejected.discard())
        .expect("reuse escaped field shared test storage");
    assert_eq!(owned.as_slice(), b"a\"b");
}
