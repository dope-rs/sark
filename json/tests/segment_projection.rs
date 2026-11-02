use std::num::NonZero;
pub mod common;

use common::{
    Bytes as _,
    segment::{Projected, RawField},
};
use dope_test::alloc::Tracker;
use sark_json::{
    Error, parsing,
    segment::{Decoder, Progress},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn scalar_has_no_raw_owner() {
    let mut decoder = Decoder::<RawField>::new(br#"{"value":123}"#.try_split(9).expect("segments"))
        .expect("decoder");
    assert!(matches!(
        decoder.poll(NonZero::<usize>::MAX),
        Progress::Failed(parsing::Error::Parse(Error::Invalid))
    ));
}

#[test]
fn segments_raw_owner() {
    let value = {
        let mut decoder = Decoder::<RawField<true>>::new(
            br#"{"value":[1,2,3]}"#.try_split(11).expect("segments"),
        )
        .expect("decoder");
        match decoder.poll(NonZero::<usize>::MAX) {
            Progress::Ready(value) => value,
            _ => panic!("raw owner"),
        }
    };
    assert_eq!(value.0.as_slice(), b"[1,2,3]");
}

#[test]
fn split_budget() {
    let mut decoder =
        Decoder::<Projected>::new(br#"{"value":[1,2,3]}"#.try_segments().expect("segments"))
            .expect("decoder");
    let (value, (allocs, _)) = Tracker::usage(|| {
        loop {
            match decoder.poll(NonZero::<usize>::MIN) {
                Progress::Pending => {}
                Progress::Ready(value) => break Some(value),
                _ => break None,
            }
        }
    });
    assert_eq!(value.expect("projection").pending, b"[1,2,3]".len() - 1);
    assert_eq!(allocs, 1);
}

#[test]
fn projection_capacity() {
    let mut decoder = Decoder::<Projected>::new(br#"{"value":{}}"#.try_split(9).expect("segments"))
        .expect("decoder");
    let (result, (allocs, _)) = Tracker::usage(|| decoder.poll(NonZero::<usize>::MAX));
    assert!(matches!(result, Progress::Ready(_)));
    assert_eq!(allocs, 1);
}
