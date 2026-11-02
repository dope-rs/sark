use std::num::NonZero;
pub mod common;

use common::{
    Bytes as _,
    decode::{self, General, Selective},
};
use sark_json::{
    Error,
    parsing::{self, Request},
    segment::{Decoder, Progress},
};

#[test]
fn raw_value_spans() {
    for &(input, expected) in decode::RAW {
        let borrowed = <General as Request>::decode(input).expect("raw value");
        assert_eq!(borrowed.number, expected);
        assert!(borrowed.number.as_slice().is_within(input));
        let mut decoder =
            Decoder::<General>::new(input.try_segments().expect("segments")).expect("decoder");
        let value = loop {
            match decoder.poll(NonZero::<usize>::MIN) {
                Progress::Pending => {}
                Progress::Ready(value) => break Some(value),
                _ => break None,
            }
        }
        .expect("decode");
        assert_eq!(value.number.as_slice(), expected);
    }
}

#[test]
fn validate_ignored_values() {
    for &input in decode::INVALID {
        assert!(matches!(
            <Selective as Request>::decode(input),
            Err(parsing::Error::Parse(Error::Invalid))
        ));
        let mut decoder =
            Decoder::<Selective>::new(input.try_segments().expect("segments")).expect("decoder");
        let error = loop {
            match decoder.poll(NonZero::<usize>::MIN) {
                Progress::Pending => {}
                Progress::Failed(error) => break Some(error),
                _ => break None,
            }
        }
        .expect("failure");
        assert!(matches!(error, parsing::Error::Parse(Error::Invalid)));
    }
}

#[test]
fn unicode_segments() {
    let borrowed = <decode::Strings as Request>::decode(decode::UNICODE).expect("request");
    assert_eq!(borrowed.values[0].as_slice(), decode::DECODED);
    let mut decoder =
        Decoder::<decode::Strings>::new(decode::UNICODE.try_segments().expect("segments"))
            .expect("decoder");
    let value = loop {
        match decoder.poll(NonZero::<usize>::MIN) {
            Progress::Pending => {}
            Progress::Ready(value) => break Some(value),
            _ => break None,
        }
    }
    .expect("unicode");
    assert_eq!(value.values[0].as_slice(), decode::DECODED);
    assert_eq!(value.values[1].as_slice(), b"plain");
}
