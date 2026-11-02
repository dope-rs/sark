use std::num::NonZero;
pub mod common;

use common::{
    Bytes as _,
    segment::{Number, Wide},
};
use o3::buffer::{resident, storage};
use sark_json::segment::{Decoder, Progress, Transfer};

#[test]
fn lookup_yields() {
    let decoder = Decoder::<Wide>::try_stream().expect("decoder");
    let owner = storage::Shared::try_copy(br#"{"value""#).expect("owner");
    let mut decoder = match decoder.push(resident::Shared::from(owner)) {
        Transfer::Accepted(decoder) => Some(decoder),
        Transfer::Rejected { .. } => None,
    }
    .expect("push");
    assert!(matches!(
        decoder.poll(NonZero::<usize>::MAX),
        Progress::NeedInput
    ));
    let owner = storage::Shared::try_copy(b":").expect("owner");
    let mut decoder = match decoder.push(resident::Shared::from(owner)) {
        Transfer::Accepted(decoder) => Some(decoder),
        Transfer::Rejected { .. } => None,
    }
    .expect("push");
    for _ in 0..64 {
        assert!(matches!(
            decoder.poll(NonZero::<usize>::MIN),
            Progress::Pending
        ));
    }
    assert!(matches!(
        decoder.poll(NonZero::<usize>::MAX),
        Progress::NeedInput
    ));
    let owner = storage::Shared::try_copy(b"123}").expect("owner");
    let decoder = match decoder.push(resident::Shared::from(owner)) {
        Transfer::Accepted(decoder) => Some(decoder),
        Transfer::Rejected { .. } => None,
    }
    .expect("push");
    assert!(matches!(
        decoder.close().poll(NonZero::<usize>::MAX),
        Progress::Ready(Wide { value: 123 })
    ));
}

#[test]
fn escaped_name_keeps_owner() {
    let bytes = br#"{"\u0076alue":123}"#;
    let mut decoder =
        Decoder::<Number>::new(bytes.try_segments().expect("segments")).expect("decoder");
    let mut released = 0;
    let value = loop {
        let progress = decoder.poll(NonZero::<usize>::MIN);
        let chunks = decoder.take_chunks().into_raw();
        assert!(chunks <= 1);
        released += chunks as usize;
        match progress {
            Progress::Pending => {}
            Progress::Ready(value) => break Some(value),
            _ => break None,
        }
    }
    .expect("decode");
    assert_eq!(value.value, 123);
    assert_eq!(released, bytes.len());
}
