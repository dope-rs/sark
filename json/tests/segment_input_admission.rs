pub mod common;

use std::{iter, num::NonZero};

use common::segment::{Number, Short};
use dope_test::alloc::Tracker;
use o3::{
    alloc,
    buffer::{resident, storage},
};
use sark_json::{
    Error,
    segment::{Decoder, Nested, Outcome, Progress, Push, Transfer},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn understated_input() {
    let owners = Short(iter::once(b"[]".as_slice()));
    let result = Tracker::fail_after(0, || Nested::<Number>::new(owners));
    assert!(matches!(
        result,
        Err(Error::Allocation(alloc::Error::Exhausted)),
    ));

    let owners = Short(iter::once(b"[]".as_slice()));
    let nested = Nested::<Number>::new(owners).expect("understated input");
    assert!(matches!(
        nested.step(NonZero::<usize>::MAX),
        Outcome::Ready(values) if values.is_empty()
    ));
}

#[test]
fn push_retries() {
    let decoder = Decoder::<Number>::try_stream().expect("decoder");
    let owner = storage::Shared::try_copy(br#"{"value":123}"#).expect("input owner");
    let pointer = owner.as_slice().as_ptr();
    let owner = resident::Shared::from(owner);
    let (state, owner) = match Tracker::fail_after(0, || decoder.push(owner)) {
        Transfer::Rejected {
            state,
            error:
                Push::Allocation {
                    error: alloc::Error::Exhausted,
                    owner,
                },
        } => Some((state, owner)),
        _ => None,
    }
    .expect("input reservation exhausted");
    assert_eq!(owner.as_slice().as_ptr(), pointer);
    let decoder = match state.push(owner) {
        Transfer::Accepted(decoder) => Some(decoder),
        Transfer::Rejected { .. } => None,
    }
    .expect("retry admitted");
    assert!(matches!(
        decoder.close().poll(NonZero::<usize>::MAX),
        Progress::Ready(Number { value: 123 })
    ));
}
