use std::{num::NonZero, pin::Pin};

use o3::base::cell::region::Token;
pub mod common;

use dope_test::alloc::Tracker;
use o3::buffer::{resident, storage};
use sark_json::{
    input::Input as _,
    parsing, segment,
    segment::{Decode, Decoder, Nested, Outcome, Progress, Push, Transfer},
};

use crate::common::{Bytes as _, segment::*};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn rejects_late_owner() {
    let decoder = Decoder::<Compound>::try_stream().expect("decoder").close();
    let stored = (storage::Shared::try_copy(b"{}")).expect("allocate bounded JSON fixture");
    let owner = resident::Shared::from(stored);
    assert!(
        matches!(
            decoder.push(owner),
            Transfer::Rejected { error: Push::Complete(owner), .. }
                if owner.as_slice() == b"{}"
        ),
        "closed decoder must reject input and return the original bytes"
    );
}

fn drive<'d, T: Decode>(
    decoder: &mut Decoder<'d, T>,
    budget: NonZero<usize>,
) -> Result<T, parsing::Error<'d>> {
    let mut turns = 0;
    loop {
        assert!(
            turns < 16_384,
            "decoder must complete within its bounded test turns"
        );
        turns += 1;
        match decoder.poll(budget) {
            Progress::Ready(value) => return Ok(value),
            Progress::Pending => {}
            Progress::NeedInput => {
                return Err(sark_json::Error::Invalid.into());
            }
            Progress::Failed(error) => return Err(error),
        }
    }
}

#[test]
fn flat_delimiters() {
    let bad = br#"{"value":[}}"#;
    let mut index = 0;
    assert!(bad.as_slice().skip_value(&mut index).is_err());

    let valid = br#"{"value":"[}]\\\"","nested":[{}]}tail"#;
    let mut index = 0;
    valid
        .as_slice()
        .skip_value(&mut index)
        .expect("skip valid nested JSON value");
    assert_eq!(&valid[index..], b"tail");
}

#[test]
fn flat_depth_limit() {
    let accepted = Compound::arrays(NonZero::new(128).expect("nesting depth"));
    let mut index = 0;
    let (result, (alloc, bytes)) = Tracker::usage(|| accepted.as_slice().skip_value(&mut index));
    result.expect("accept JSON nesting at the depth limit");
    assert_eq!(index, accepted.len());
    assert_eq!((alloc, bytes), (0, 0));

    let rejected = Compound::arrays(NonZero::new(129).expect("nesting depth"));
    let mut index = 0;
    let (result, (alloc, bytes)) = Tracker::usage(|| rejected.as_slice().skip_value(&mut index));
    assert!(result.is_err());
    assert_eq!((alloc, bytes), (0, 0));
}

#[test]
fn scoped_admission() {
    const PAYLOAD: usize = 64 * 1024;
    let mut domain = resident::Domain::new();
    Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let budget = domain
            .get_mut()
            .try_budget(PAYLOAD, token.proof())
            .expect("resident test domain must admit its budget");
        let mut input = vec![b' '; PAYLOAD];
        input[..11].copy_from_slice(br#"{"value":1}"#);
        let owner = (resident::Exact::try_copy(&budget, &input))
            .expect("the scoped JSON fixture must fit its resident budget");
        let owner = owner.into_shared();
        let segments = vec![owner];

        let (mut decoder, (_, bytes)) =
            Tracker::usage(|| Decoder::<Number>::from_scoped(segments).expect("decoder input"));
        assert!(
            bytes < 4096,
            "decoder admission copied a {PAYLOAD}-byte driver owner ({bytes} bytes allocated)",
        );
        let number =
            drive(&mut decoder, segment::DEFAULT_STEP_BUDGET).expect("decode scoped JSON fixture");
        assert_eq!(number.value, 1);
    });
}

#[test]
fn segmented_delimiters() {
    let bad = br#"{"value":{[}}}"#;
    for split in 0..=bad.len() {
        let mut decoder = Decoder::<Compound>::new(bad.try_split(split).expect("JSON fragments"))
            .expect("decoder input");
        assert!(
            drive(&mut decoder, NonZero::<usize>::MIN).is_err(),
            "crossed delimiters accepted at split {split}"
        );
    }

    let valid = br#"{"value":{"text":"[}]\\\"","nested":[{}]}}"#;
    for split in 0..=valid.len() {
        let mut decoder = Decoder::<Compound>::new(valid.try_split(split).expect("JSON fragments"))
            .expect("decoder input");
        drive(&mut decoder, NonZero::<usize>::MIN)
            .expect("decode valid JSON at every fragment boundary");
    }
}

#[test]
fn segmented_depth_limit() {
    let accepted = Compound::object(NonZero::new(128).expect("nesting depth"));
    let segments = accepted
        .try_segments()
        .expect("copy one-byte JSON test fragments");
    let mut decoder = Decoder::<Compound>::new(segments).expect("decoder input");
    drive(&mut decoder, NonZero::<usize>::MIN).expect("decode one-byte fragments at depth limit");

    let mut rejected = Compound::object(NonZero::new(129).expect("nesting depth"));
    let rejected =
        storage::Shared::try_from(&mut rejected).expect("store over-depth JSON test input");
    let mut decoder = Decoder::<Compound>::new(vec![rejected]).expect("decoder input");
    assert!(drive(&mut decoder, NonZero::<usize>::MAX).is_err());
}

#[test]
fn rejects_delimiters() {
    let bad =
        (storage::Shared::try_copy(br#"[{"value":[}}]"#)).expect("allocate bounded JSON fixture");
    let bad = Nested::<Number>::one(bad).expect("sequence input");
    let Outcome::Pending(bad) = bad.step(NonZero::<usize>::MAX) else {
        panic!("malformed sequence must release its input");
    };
    assert!(matches!(
        bad.step(NonZero::<usize>::MAX),
        Outcome::Failed(sark_json::parsing::Error::Parse(sark_json::Error::Invalid))
    ));
}
