pub mod common;

use std::{num::NonZero, pin::Pin};

use common::{
    Bytes as _,
    decode::Number,
    schema::{RawSequence, Sequence},
    segment,
};
use dope_test::alloc::Tracker;
use o3::{base::cell::region::Token, buffer::resident};
use sark_json::{
    Error, parsing,
    segment::{Decoder, Nested, Outcome, Progress, Push, Transfer, frames::Frames},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

/// One copied field or final pair of tables owns at most two allocations.
#[test]
fn scalar_release_budget() {
    let owners = segment::SCALARS.try_segments().expect("scalar owners");
    let mut decoder = Decoder::<segment::Scalars>::new(owners).expect("scalar decoder");
    let mut released = 0;
    let mut ending = 0;
    let value = loop {
        if released == segment::SCALARS.len() {
            ending += 1;
        }
        let (progress, activity) = Tracker::activity(|| decoder.poll(NonZero::<usize>::MIN));
        released += decoder.take_bytes();
        assert!(activity.deallocations <= 2, "{activity:?}");
        assert!(!matches!(progress, Progress::NeedInput));
        match progress {
            Progress::Pending | Progress::NeedInput => {}
            Progress::Ready(value) => break Ok(value),
            Progress::Failed(error) => break Err(error),
        }
    }
    .expect("scalar decode");
    assert!(ending >= segment::SCALARS.len() + 2 * 8);
    assert_eq!(value.a.as_bytes(), b"[1]");
    assert_eq!(value.h.as_bytes(), b"[1]");
}

#[test]
fn compound_release_budget() {
    let mut domain = resident::Domain::new();
    Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let resident = domain
            .get_mut()
            .try_budget(segment::COMPOUND.len(), token.proof())
            .expect("resident budget");
        let owners = segment::COMPOUND
            .try_scoped(&resident)
            .expect("compound owners");
        let mut decoder = Decoder::<Sequence>::from_scoped(owners).expect("compound decoder");
        let value = loop {
            let before = resident.resident();
            let progress = decoder.poll(NonZero::<usize>::MIN);
            assert!(before - resident.resident() <= 1);
            assert!(!matches!(progress, Progress::NeedInput));
            match progress {
                Progress::Pending | Progress::NeedInput => {}
                Progress::Ready(value) => break Ok(value),
                Progress::Failed(error) => break Err(error),
            }
        }
        .expect("compound decode");
        assert_eq!(resident.resident(), 0);
        assert_eq!(value.values.len(), segment::VALUES.len());
        for (value, expected) in value.values.iter().zip(segment::VALUES) {
            assert_eq!(value.as_slice(), expected);
        }
    });
}

#[test]
fn frames_release_budget() {
    let mut domain = resident::Domain::new();
    Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let resident = domain
            .get_mut()
            .try_budget(segment::FRAMES.len(), token.proof())
            .expect("resident budget");
        let owners = segment::FRAMES.try_scoped(&resident).expect("frame owners");
        let mut frames = Frames::from_scoped(owners).expect("frames");
        let values = loop {
            let before = resident.resident();
            let outcome = frames.step(NonZero::<usize>::MIN);
            assert!(before - resident.resident() <= 1);
            match outcome {
                Outcome::Pending(next) => frames = next,
                Outcome::Ready(values) => break Ok(values),
                Outcome::Failed(error) => break Err(error),
            }
        }
        .expect("frames decode");
        assert_eq!(resident.resident(), 0);
        assert_eq!(values.len(), segment::VALUES.len());
        for (value, expected) in values.iter().zip(segment::VALUES) {
            assert_eq!(value.as_slice(), expected);
        }
    });
}

#[test]
fn nested_release_budget() {
    let mut domain = resident::Domain::new();
    Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let resident = domain
            .get_mut()
            .try_budget(segment::NESTED.len(), token.proof())
            .expect("resident budget");
        let owners = segment::NESTED
            .try_scoped(&resident)
            .expect("nested owners");
        let mut sequence = Nested::<Number>::new(owners).expect("nested sequence");
        let values = loop {
            let before = resident.resident();
            let outcome = sequence.step(NonZero::<usize>::MIN);
            assert!(before - resident.resident() <= 1);
            match outcome {
                Outcome::Pending(next) => sequence = next,
                Outcome::Ready(values) => break Ok(values),
                Outcome::Failed(error) => break Err(error),
            }
        }
        .expect("nested decode");
        assert_eq!(resident.resident(), 0);
        assert_eq!(
            values
                .into_iter()
                .map(|value| value.value)
                .collect::<Vec<_>>(),
            [1, 2],
        );
    });
}

#[test]
fn scanner_error_release_budget() {
    for bytes in [segment::INVALID_FIRST, segment::INVALID_AFTER_VALUE] {
        let mut domain = resident::Domain::new();
        Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
            let resident = domain
                .get_mut()
                .try_budget(bytes.len(), token.proof())
                .expect("resident budget");
            let measurement = Tracker::begin();
            let owners = bytes.try_scoped(&resident).expect("input owners");
            let mut decoder = Decoder::<segment::Number>::from_scoped(owners).expect("decoder");
            let mut released = 0;
            let error = loop {
                let before = resident.resident();
                let (progress, activity) =
                    Tracker::activity(|| decoder.poll(NonZero::<usize>::MIN));
                assert!(before - resident.resident() <= 1);
                // One owner and the two completed metadata tables may be freed.
                assert!(activity.deallocations <= 3, "{activity:?}");
                let bytes = decoder.take_bytes();
                let chunks = decoder.take_chunks().into_raw() as usize;
                assert!(chunks <= 1);
                assert_eq!(bytes, chunks);
                released += chunks;
                match progress {
                    Progress::Pending => {}
                    Progress::Failed(error) => break error,
                    _ => panic!("invalid input must fail after bounded cleanup"),
                }
            };
            assert!(matches!(error, parsing::Error::Parse(Error::Invalid)));
            assert_eq!(released, bytes.len());
            assert_eq!(resident.resident(), 0);
            assert_eq!(decoder.take_bytes(), 0);
            assert_eq!(decoder.take_chunks().into_raw(), 0);
            drop(decoder);
            let activity = measurement.finish();
            assert_eq!(activity.allocations, activity.deallocations, "{activity:?}");
        });
    }
}

#[test]
fn failed_scan_preserves_rejected_owner() {
    let mut domain = resident::Domain::new();
    Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let resident = domain
            .get_mut()
            .try_budget(
                segment::INVALID_FIRST.len() + segment::COPIED.len(),
                token.proof(),
            )
            .expect("resident budget");
        let measurement = Tracker::begin();
        let owners = segment::INVALID_FIRST
            .try_scoped(&resident)
            .expect("input owners");
        let held = owners[0].try_clone().expect("retained input owner");
        let mut decoder = Decoder::<segment::Number>::from_scoped(owners).expect("decoder");
        assert!(matches!(
            decoder.poll(NonZero::<usize>::MIN),
            Progress::Pending
        ));
        assert_eq!(resident.resident(), segment::INVALID_FIRST.len());
        assert_eq!(decoder.take_bytes(), 0);
        assert_eq!(decoder.take_chunks().into_raw(), 0);
        let foreign = resident::Exact::try_copy(&resident, segment::COPIED)
            .expect("foreign owner")
            .into_shared();
        let pointer = foreign.as_ptr();
        let (mut decoder, foreign) = match decoder.push(foreign) {
            Transfer::Rejected {
                state,
                error: Push::Complete(owner),
            } => (state, owner),
            _ => panic!("failed scan must return the new owner"),
        };
        assert_eq!(foreign.as_ptr(), pointer);
        let error = loop {
            match decoder.poll(NonZero::<usize>::MIN) {
                Progress::Pending => {}
                Progress::Failed(error) => break error,
                _ => panic!("failed scan must preserve its original error"),
            }
        };
        assert!(matches!(error, parsing::Error::Parse(Error::Invalid)));
        assert_eq!(decoder.take_bytes(), segment::INVALID_FIRST.len());
        assert_eq!(
            decoder.take_chunks().into_raw() as usize,
            segment::INVALID_FIRST.len()
        );
        drop(decoder);
        assert_eq!(resident.resident(), segment::COPIED.len() + 1);
        assert_eq!(held.as_slice(), &segment::INVALID_FIRST[..1]);
        assert_eq!(foreign.as_slice(), segment::COPIED);
        drop(held);
        assert_eq!(resident.resident(), segment::COPIED.len());
        drop(foreign);
        assert_eq!(resident.resident(), 0);
        let activity = measurement.finish();
        assert_eq!(activity.allocations, activity.deallocations, "{activity:?}");
    });
}

#[test]
fn failed_scan_drop_releases_remaining_owners() {
    let mut domain = resident::Domain::new();
    Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let resident = domain
            .get_mut()
            .try_budget(segment::INVALID_FIRST.len(), token.proof())
            .expect("resident budget");
        let measurement = Tracker::begin();
        let owners = segment::INVALID_FIRST
            .try_scoped(&resident)
            .expect("input owners");
        let mut decoder = Decoder::<segment::Number>::from_scoped(owners).expect("decoder");
        let mut released = 0;
        while released == 0 {
            assert!(matches!(
                decoder.poll(NonZero::<usize>::MIN),
                Progress::Pending
            ));
            let chunks = decoder.take_chunks().into_raw() as usize;
            assert_eq!(decoder.take_bytes(), chunks);
            released += chunks;
        }
        assert!(released < segment::INVALID_FIRST.len());
        assert_eq!(resident.resident(), segment::INVALID_FIRST.len() - released);
        drop(decoder);
        assert_eq!(resident.resident(), 0);
        let activity = measurement.finish();
        assert_eq!(activity.allocations, activity.deallocations, "{activity:?}");
    });
}

#[test]
fn materialize_error_release_budget() {
    let mut domain = resident::Domain::new();
    Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let resident = domain
            .get_mut()
            .try_budget(segment::INVALID_MATERIALIZE.len(), token.proof())
            .expect("resident budget");
        let measurement = Tracker::begin();
        let owners = segment::INVALID_MATERIALIZE
            .try_scoped(&resident)
            .expect("input owners");
        let mut decoder = Decoder::<Sequence>::from_scoped(owners).expect("decoder");
        let mut released = 0;
        let error = loop {
            let before = resident.resident();
            let (progress, activity) = Tracker::activity(|| decoder.poll(NonZero::<usize>::MIN));
            assert!(before - resident.resident() <= 1);
            assert!(activity.deallocations <= 3, "{activity:?}");
            let bytes = decoder.take_bytes();
            let chunks = decoder.take_chunks().into_raw() as usize;
            assert!(chunks <= 1);
            assert_eq!(bytes, chunks);
            released += chunks;
            assert!(released <= segment::INVALID_MATERIALIZE.len());
            match progress {
                Progress::Pending => {}
                Progress::Failed(error) => break error,
                _ => panic!("plain array must reject the escaped element"),
            }
        };
        assert!(matches!(error, parsing::Error::Parse(Error::Invalid)));
        assert_eq!(resident.resident(), 0);
        drop(decoder);
        let activity = measurement.finish();
        assert_eq!(activity.allocations, activity.deallocations, "{activity:?}");
    });
}

#[test]
fn frames_error_release_budget() {
    let mut domain = resident::Domain::new();
    Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let resident = domain
            .get_mut()
            .try_budget(segment::INVALID_FRAMES.len(), token.proof())
            .expect("resident budget");
        let measurement = Tracker::begin();
        let owners = segment::INVALID_FRAMES
            .try_scoped(&resident)
            .expect("input owners");
        let mut frames = Frames::from_scoped(owners).expect("frames");
        let error = loop {
            let before = resident.resident();
            let (outcome, activity) = Tracker::activity(|| frames.step(NonZero::<usize>::MIN));
            assert!(before - resident.resident() <= 1);
            assert!(activity.deallocations <= 3, "{activity:?}");
            match outcome {
                Outcome::Pending(next) => frames = next,
                Outcome::Failed(error) => break error,
                Outcome::Ready(_) => panic!("invalid array must fail"),
            }
        };
        assert!(matches!(error, parsing::Error::Parse(Error::Invalid)));
        assert_eq!(resident.resident(), 0);
        let activity = measurement.finish();
        assert_eq!(activity.allocations, activity.deallocations, "{activity:?}");
    });
}

#[test]
fn nested_error_release_budget() {
    let mut domain = resident::Domain::new();
    Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let resident = domain
            .get_mut()
            .try_budget(segment::INVALID_NESTED.len(), token.proof())
            .expect("resident budget");
        let measurement = Tracker::begin();
        let owners = segment::INVALID_NESTED
            .try_scoped(&resident)
            .expect("input owners");
        let mut nested = Nested::<RawSequence>::new(owners).expect("nested sequence");
        let error = loop {
            let before = resident.resident();
            let (outcome, activity) = Tracker::activity(|| nested.step(NonZero::<usize>::MIN));
            assert!(before - resident.resident() <= 1);
            // Terminal cleanup also drops the empty input table, child Box,
            // and value table, alongside the last live input owner.
            let limit = if matches!(&outcome, Outcome::Failed(_)) {
                4
            } else {
                3
            };
            assert!(activity.deallocations <= limit, "{activity:?}");
            match outcome {
                Outcome::Pending(next) => nested = next,
                Outcome::Failed(error) => break error,
                Outcome::Ready(_) => panic!("missing nested field must fail"),
            }
        };
        assert!(
            matches!(error, parsing::Error::Missing("values")),
            "{error:?}"
        );
        assert_eq!(resident.resident(), 0);
        let activity = measurement.finish();
        assert_eq!(activity.allocations, activity.deallocations, "{activity:?}");
    });
}

#[test]
fn released_states_do_not_resume() {
    let mut domain = resident::Domain::new();
    Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let resident = domain
            .get_mut()
            .try_budget(
                segment::COMPOUND.len().max(segment::INVALID_FIRST.len()),
                token.proof(),
            )
            .expect("resident budget");

        let owners = segment::FRAMES.try_scoped(&resident).expect("frame owners");
        let mut frames = Frames::from_scoped(owners).expect("frames");
        assert!(frames.release(&mut 0).is_pending());
        assert!(frames.release(&mut 1).is_pending());
        assert_eq!(resident.resident(), segment::FRAMES.len() - 1);
        for _ in 0..segment::FRAMES.len() {
            frames = match frames.step(NonZero::<usize>::MIN) {
                Outcome::Pending(next) => next,
                _ => panic!("released frames must not resume parsing"),
            };
        }
        assert!(frames.release(&mut 0).is_ready());
        assert_eq!(resident.resident(), 0);
        drop(frames);

        let owners = segment::NESTED
            .try_scoped(&resident)
            .expect("nested owners");
        let mut nested = Nested::<Number>::new(owners).expect("nested sequence");
        assert!(nested.release(&mut 0).is_pending());
        assert!(nested.release(&mut 1).is_pending());
        assert_eq!(resident.resident(), segment::NESTED.len() - 1);
        for _ in 0..segment::NESTED.len() {
            nested = match nested.step(NonZero::<usize>::MIN) {
                Outcome::Pending(next) => next,
                _ => panic!("released nested input must not resume parsing"),
            };
        }
        assert!(nested.release(&mut 0).is_ready());
        assert_eq!(resident.resident(), 0);
        drop(nested);

        let owners = segment::INVALID_FIRST
            .try_scoped(&resident)
            .expect("decoder owners");
        let mut decoder = Decoder::<segment::Number>::from_scoped(owners).expect("decoder");
        assert!(decoder.release(&mut 0).is_pending());
        assert!(decoder.release(&mut 1).is_pending());
        assert!(decoder.release(&mut 1).is_pending());
        assert_eq!(resident.resident(), segment::INVALID_FIRST.len() - 1);
        for _ in 0..segment::INVALID_FIRST.len() {
            assert!(matches!(
                decoder.poll(NonZero::<usize>::MIN),
                Progress::Pending
            ));
        }
        assert!(decoder.release(&mut 0).is_ready());
        assert_eq!(resident.resident(), 0);
        drop(decoder);

        let owners = segment::PROJECT
            .try_scoped(&resident)
            .expect("projection owners");
        let mut decoder =
            Decoder::<segment::Projected<true>>::from_scoped(owners).expect("decoder");
        for _ in 0..8 * segment::PROJECT.len() {
            assert!(matches!(
                decoder.poll(NonZero::<usize>::MIN),
                Progress::Pending
            ));
        }
        assert_eq!(resident.resident(), 0);
        assert!(decoder.release(&mut 0).is_ready());
        drop(decoder);

        let owners = segment::COMPOUND
            .try_scoped(&resident)
            .expect("materializer owners");
        let mut decoder = Decoder::<segment::CancelSequence>::from_scoped(owners).expect("decoder");
        for _ in 0..8 * segment::COMPOUND.len() {
            assert!(matches!(
                decoder.poll(NonZero::<usize>::MIN),
                Progress::Pending
            ));
        }
        assert_eq!(resident.resident(), 0);
        assert!(decoder.release(&mut 0).is_ready());
    });
}
