use std::borrow::Cow;

use dope_test::alloc::Tracker;
use sark_http::field::{self, Entry, Lower, Name, Value};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn borrows_canonical_name() {
    let name = field::literal!(Name::try_bytes(b"x-token"));
    let (lower, usage) = Tracker::usage(|| Lower::<Cow<'_, [u8]>>::try_from(name));
    let lower = lower.expect("canonical name");
    assert_eq!(usage, (0, 0));
    assert_eq!(lower.as_bytes().as_ptr(), name.get().as_ptr());
    let entry = Entry::new(lower.borrow(), field::literal!(Value::try_bytes(b"value")));
    assert_eq!(entry.as_record().name, name.get());
}

#[test]
fn normalizes_at_admission() {
    let name = field::literal!(Name::try_bytes(b"X-Token"));
    assert!(Tracker::fail_after(0, || Lower::<Cow<'_, [u8]>>::try_from(name)).is_err());
    let lower = Lower::<Cow<'_, [u8]>>::try_from(name).expect("normalized name");
    let entry = Entry::new(lower.borrow(), field::literal!(Value::try_bytes(b"value")));
    assert_eq!(entry.as_record().name, b"x-token");
    assert_eq!(name.get(), b"X-Token");
}
