use std::num::NonZero;
pub mod common;

use common::{
    nested::Message,
    segment::{self, COPIED},
};
use o3::buffer::{resident, storage};
use sark_json::segment::{Decoder, Outcome, Progress, frames::Frames};

#[test]
fn scoped_field_budget() {
    let bytes = segment::MESSAGE.concat();
    for budget in [1, 7, 8, 9] {
        let budget = NonZero::new(budget).expect("budget");
        let mut decoder =
            Decoder::<Message>::from_scoped(vec![resident::Shared::from(bytes.as_slice())])
                .expect("decoder");
        for _ in 0..(bytes.len() + COPIED.len() - 1) / budget.get() {
            assert!(matches!(decoder.poll(budget), Progress::Pending));
        }
        let value = match decoder.poll(NonZero::<usize>::MAX) {
            Progress::Ready(value) => Some(value),
            _ => None,
        }
        .expect("decode");
        assert_eq!(value.message.as_slice(), COPIED);
    }
}

#[test]
fn scoped_array_budget() {
    let bytes = segment::ARRAY.concat();
    for budget in [1, 7, 8, 9] {
        let budget = NonZero::new(budget).expect("budget");
        let mut frames =
            Frames::from_scoped(vec![resident::Shared::from(bytes.as_slice())]).expect("frames");
        for _ in 0..(bytes.len() + COPIED.len() - 1) / budget.get() {
            frames = match frames.step(budget) {
                Outcome::Pending(next) => Some(next),
                _ => None,
            }
            .expect("budget");
        }
        let values = match frames.step(NonZero::<usize>::MAX) {
            Outcome::Ready(values) => Some(values),
            _ => None,
        }
        .expect("decode");
        assert_eq!(values[0].as_slice(), COPIED);
    }
}

/// Parsing and replay charge each wire byte; completion charges each input owner.
#[test]
fn escaped_replay_budget() {
    let bytes = segment::ESCAPED_ARRAY;
    for (chunk, budget) in [
        (
            NonZero::new(bytes.len()).expect("escaped fixture length"),
            NonZero::<usize>::MIN.saturating_add(8),
        ),
        (
            NonZero::<usize>::MIN.saturating_add(2),
            NonZero::<usize>::MIN,
        ),
    ] {
        let mut frames = Frames::from_scoped(
            bytes
                .chunks(chunk.get())
                .map(resident::Shared::from)
                .collect(),
        )
        .expect("frames");
        let charged = bytes.len() + bytes.len() - 4 + bytes.len().div_ceil(chunk.get());
        for _ in 0..(charged - 1) / budget.get() {
            frames = match frames.step(budget) {
                Outcome::Pending(next) => Some(next),
                _ => None,
            }
            .expect("budget");
        }
        let values = match frames.step(budget) {
            Outcome::Ready(values) => Some(values),
            _ => None,
        }
        .expect("decode");
        assert_eq!(values[0].as_slice(), segment::ESCAPED_VALUE);
    }
}

#[test]
fn retains_array_view() {
    for budget in [NonZero::<usize>::MIN, NonZero::<usize>::MAX] {
        let owner = storage::Shared::try_copy(&segment::ARRAY.concat()).expect("owner");
        let expected = owner.as_ptr().wrapping_add(2);
        let mut frames = Frames::from_scoped(vec![resident::Shared::from(owner)]).expect("frames");
        let values = loop {
            match frames.step(budget) {
                Outcome::Pending(next) => frames = next,
                Outcome::Ready(values) => break Ok(values),
                Outcome::Failed(error) => break Err(error),
            }
        }
        .expect("array decode");
        assert_eq!(values[0].as_ptr(), expected);
        assert_eq!(values[0].as_slice(), COPIED);
    }
}
