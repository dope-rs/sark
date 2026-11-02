use std::{cell::Cell, convert::identity, iter, num::NonZero};

use common::ready;
use sark_core::body::{
    credit::raw::{Account, Ledger},
    input,
    input::Checked,
    stream::{Admission, Drive, Push, Stream},
};

pub mod common;

use core::convert::Infallible;

use dope_test::alloc::Tracker;
use o3::buffer::{resident, segments};
use sark_core::{
    body,
    body::{
        admit::{Length, Plan, Retention, Track},
        buffered, credit,
    },
    manifold::stream::Boundary,
};
use sark_http::payload::owned;

use crate::common::{
    CREDIT, LIMITS, Retained as _, Seed as _, Store,
    ready::{Invalid, Ready},
    trace::Trace,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

fn discarded(trace: &Trace, amounts: &[credit::Amount], total: credit::Amount) {
    assert_eq!(trace.changes.len(), amounts.len() + 1);
    for (change, amount) in trace.changes.iter().zip(amounts) {
        assert!(*change == credit::Mutation::Reserved(*amount));
    }
    assert!(
        trace.changes.last()
            == Some(&credit::Mutation::Released {
                credit: total,
                disposition: credit::Disposition::Discarded,
            })
    );
}

fn admit<'d, O: credit::Origin>(
    body: Stream<'d, Ready<'d>, O>,
    ledger: &mut Ledger<'_, O>,
) -> Vec<owned::Segment<'d>> {
    let (step, evidence) = body.try_drive(ledger, credit::Disabled::new(), NonZero::<usize>::MIN);
    let Drive::Ready(ready) = step else {
        panic!("finished builder returns its ready owner");
    };
    let Admission::Accepted(segments) = ready.try_admit(ledger, evidence) else {
        panic!("admitted ready body");
    };
    assert!(ledger.retained().is_empty());
    segments
}

#[test]
fn failure_keeps_credit() {
    Account::new().scope(|mut ledger| {
        let open = Track::try_start(6).expect("unfinished body fixture");
        let mut body = buffered::State::<(), _>::start(open, &ledger);
        let mut trace = Trace {
            changes: Vec::with_capacity(3),
        };

        let ((), allocs) = Tracker::count(|| {
            let (empty, pushed) = body.seed(&mut ledger, &mut trace);
            assert!(empty && matches!(pushed, buffered::Push::Accepted));
            assert!(ledger.retained() == CREDIT);

            let error = body.try_push(
                &mut ledger,
                &mut trace,
                input::Input::new((), 3, segments::Count::ONE),
                LIMITS,
                LIMITS,
                |store, ()| {
                    assert!(store.is_some());
                    buffered::Retain::Rejected {
                        storage: store,
                        input: (),
                        error: Store,
                    }
                },
            );

            assert!(matches!(
                error,
                buffered::Push::Rejected {
                    input: (),
                    error: buffered::Error::Retain(Store)
                }
            ));
            let retained = CREDIT;
            assert!(ledger.retained() == retained);
            assert!(trace.changes == [credit::Mutation::Reserved(retained)]);
            let retried = body.try_push(
                &mut ledger,
                &mut trace,
                input::Input::new((), 3, segments::Count::ONE),
                LIMITS,
                LIMITS,
                |storage, ()| {
                    assert!(storage.is_some());
                    buffered::Retain::<(), (), Infallible>::Accepted { storage }
                },
            );
            assert!(matches!(retried, buffered::Push::Accepted));
            let total = ledger.retained();
            assert_eq!(total.bytes(), 6);
            assert_eq!(total.chunks().into_raw(), 2);
            assert!(matches!(
                body.discard(&mut ledger, &mut trace),
                credit::Discard::Applied
            ));
            assert!(ledger.retained().is_empty());
            discarded(&trace, &[retained, retained], total);
        });
        assert_eq!(allocs, 0);
    });
}

#[test]
fn abandon_then_reset() {
    let mut account = Account::new();
    account.scope(|mut ledger| {
        let open = Track::try_start(3).expect("unfinished body fixture");
        let mut body = buffered::State::<(), _>::start(open, &ledger);
        let mut trace = Trace::default();
        let (empty, pushed) = body.seed(&mut ledger, &mut trace);
        assert!(empty && matches!(pushed, buffered::Push::Accepted));
        body.abandon(&mut trace);
        assert!(ledger.retained() == CREDIT);
        discarded(&trace, &[CREDIT], CREDIT);
    });
    assert_eq!(account.retained(), CREDIT);
    account = Account::new();
    assert!(account.retained().is_empty());
}

#[test]
fn admit_returns_owner() {
    Account::new().scope(|mut ledger| {
        let valid = Cell::new(true);
        let open = Track::try_start(3).expect("unfinished body fixture");
        let build = Ready::new(Ok(()), &valid);
        let body = Stream::start(open, build, &ledger);
        let segments = owned::Segments::from(resident::Shared::from(ready::BYTES));
        let Push::Accepted(body) = body.try_push(
            &mut ledger,
            credit::Disabled::new(),
            input::Input::new(segments, 3, segments::Count::ONE),
            Boundary::More,
            (LIMITS, LIMITS),
            identity,
        ) else {
            panic!("required body transition");
        };
        let body::Transfer::Accepted(body) = body.try_finish(&mut ledger, credit::Disabled::new())
        else {
            panic!("required body transition");
        };
        assert_eq!(admit(body, &mut ledger).len(), 1);
        assert!(valid.get());
    });
}

#[test]
fn keeps_push_failure() {
    Account::new().scope(|mut ledger| {
        let valid = Cell::new(true);
        let open = Track::try_start(3).expect("unfinished body fixture");
        let build = Ready::new(Err(Invalid), &valid);
        let body = Stream::start(open, build, &ledger);
        let mut trace = Trace::default();
        let segment = owned::Segment::from(resident::Shared::from(b"abc".as_slice()));
        let Push::Rejected {
            build: body,
            error: Invalid,
        } = body.try_push(
            &mut ledger,
            &mut trace,
            input::Input::new(segment, 3, segments::Count::ONE),
            Boundary::More,
            (LIMITS, LIMITS),
            |part| Checked::new(iter::once(part)),
        )
        else {
            panic!("exact builder failure");
        };
        let retained = CREDIT;
        assert!(ledger.retained() == retained);
        assert!(trace.changes == [credit::Mutation::Reserved(retained)]);
        assert!(matches!(
            body.discard(&mut ledger, &mut trace),
            credit::Discard::Applied
        ));
        assert!(ledger.retained().is_empty());
        discarded(&trace, &[retained], retained);
        assert!(valid.get());
    });
}

#[test]
fn buffered_owner_moves() {
    Account::new().scope(|mut ledger| {
        let valid = Cell::new(true);
        let open = Track::try_start(3).expect("unfinished body fixture");
        let segment = owned::Segment::from(resident::Shared::from(ready::BYTES));
        let pointer = segment.as_slice().as_ptr();
        let (buffered, pushed) = open.retain(segment, &mut ledger);
        assert!(matches!(pushed, buffered::Push::Accepted));
        assert!(ledger.retained() == CREDIT);

        let stream = buffered
            .try_stream(Ready::new(Ok(()), &valid), Boundary::End, Checked::new)
            .ok()
            .expect("buffered owner must become a stream");
        assert!(ledger.retained() == CREDIT);

        let ((step, evidence), allocs) = Tracker::count(|| {
            stream.try_drive(&mut ledger, credit::Disabled, NonZero::<usize>::MIN)
        });
        assert_eq!(allocs, 0);
        let Drive::Ready(ready) = step else {
            panic!("completion retains a ready owner");
        };
        assert_eq!(ledger.retained(), CREDIT);

        let Admission::Accepted(segments) = ready.try_admit(&mut ledger, evidence) else {
            panic!("the issuing ledger admits the retained output");
        };
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].as_slice().as_ptr(), pointer);
        assert!(ledger.retained().is_empty());
        assert!(valid.get());
    });
}

#[test]
fn adopts_finished_once() {
    Account::new().scope(|mut ledger| {
        let valid = Cell::new(true);
        use sark_core::body::admit::Stream;
        let plan = Plan::new(
            Retention::Discarded,
            16,
            Length::Known(3),
            Stream::Streaming,
        );
        let done = plan.try_admit(16, 3).expect("finished body fixture");
        let buffered = buffered::State::<(), _>::ended(done, &ledger);
        let stream = buffered.try_stream(Ready::new(Ok(()), &valid), Boundary::End, |()| {
            Checked::new(iter::empty::<owned::Segment<'_>>())
        });
        let Ok(stream) = stream else {
            panic!("adopt the existing end without ending input again");
        };
        let body::Transfer::Rejected {
            build,
            error: body::Error::InputFinished,
        } = stream.try_finish(&mut ledger, credit::Disabled::new())
        else {
            panic!("the adopted input must remain finished");
        };
        assert!(admit(build, &mut ledger).is_empty());
        assert!(valid.get());
    });
}

#[test]
fn keeps_build_failure() {
    Account::new().scope(|mut ledger| {
        let valid = Cell::new(true);
        let open = Track::try_start(3).expect("unfinished body fixture");
        let segment = owned::Segment::from(resident::Shared::from(b"abc".as_slice()));
        let (buffered, pushed) = open.retain(segment, &mut ledger);
        assert!(matches!(pushed, buffered::Push::Accepted));

        let build = Ready::new(Err(Invalid), &valid);
        let (stream, error) = (buffered.try_stream(build, Boundary::More, Checked::new))
            .err()
            .expect("builder failure must be preserved")
            .into_parts();
        assert!(matches!(error, buffered::Transition::Build(Invalid)));
        assert!(ledger.retained() == CREDIT);
        assert!(matches!(
            stream.discard(&mut ledger, &mut credit::Disabled::new()),
            credit::Discard::Applied
        ));
        assert!(ledger.retained().is_empty());
        assert!(valid.get());
    });
}

#[test]
fn keeps_body_failure() {
    Account::new().scope(|mut ledger| {
        let valid = Cell::new(true);
        let open = Track::try_start(3).expect("unfinished body fixture");
        let mut buffered = buffered::State::<Vec<owned::Segment<'_>>, _>::start(open, &ledger);
        let segment = owned::Segment::from(resident::Shared::from(b"abc".as_slice()));
        let pushed = buffered.try_push(
        &mut ledger,
        credit::Disabled::new(),
        input::Input::new(segment, 3, segments::Count::ONE),
        LIMITS,
        LIMITS,
        |storage, _| {
            assert!(storage.is_none());
            buffered::Retain::<Vec<owned::Segment<'_>>, owned::Segment<'_>, Infallible>::Accepted {
                storage: Some(Vec::new()),
            }
        },
    );
        assert!(matches!(pushed, buffered::Push::Accepted));

        let build = Ready::new(Ok(()), &valid);
        let (stream, error) = (buffered.try_stream(build, Boundary::More, Checked::new))
            .err()
            .expect("body failure must be preserved")
            .into_parts();
        assert!(matches!(
            error,
            buffered::Transition::Body(body::Error::SegmentMismatch)
        ));
        assert!(ledger.retained() == CREDIT);
        assert!(matches!(
            stream.discard(&mut ledger, &mut credit::Disabled::new()),
            credit::Discard::Applied
        ));
        assert!(ledger.retained().is_empty());
        assert!(valid.get());
    });
}

#[test]
fn completion_keeps_owners() {
    use o3::buffer::pool;

    for (declared, payload) in [(4, b"abc".as_slice()), (3, b"ab"), (3, b"abc")] {
        Account::new().scope(|mut ledger| {
            let open = Track::try_start(declared).expect("unfinished body fixture");
            let pool = pool::Pool::try_new(1, 8).expect("one retained input slot");
            let source = pool
                .try_copy(payload)
                .expect("input fits slot")
                .expect("slot is vacant");
            let pointer = source.as_ref().as_ptr();
            let source = owned::Segments::from(source.into_shared());
            let mut buffered = buffered::State::start(open, &ledger);
            let mut trace = Trace {
                changes: Vec::with_capacity(2),
            };
            let pushed = buffered.try_push(
                &mut ledger,
                &mut trace,
                input::Input::new(source, 3, segments::Count::ONE),
                LIMITS,
                LIMITS,
                |storage, source| {
                    assert!(storage.is_none());
                    buffered::Retain::<_, _, Infallible>::Accepted {
                        storage: Some(source),
                    }
                },
            );
            assert!(matches!(pushed, buffered::Push::Accepted));
            assert_eq!(ledger.retained(), CREDIT);
            assert_eq!(pool.available(), 0);
            let (result, allocs) = Tracker::count(|| buffered.try_finish(&mut ledger, &mut trace));
            assert_eq!(allocs, 0);
            assert_eq!(pool.available(), 0);
            let kind = match result {
                Ok(mut body) => {
                    use body::Project as _;
                    assert_eq!((declared, payload), (3, b"abc".as_slice()));
                    body::Contiguous::try_project(&mut body, (), |view| {
                        assert_eq!(view.into_inner().as_slice().as_ptr(), pointer);
                    })
                    .expect("the admitted body retains its original owner");
                    assert!(ledger.retained().is_empty());
                    drop(body);
                    credit::Disposition::Admitted
                }
                Err(rejected) => {
                    assert_eq!(trace.changes.len(), 1);
                    assert_eq!(ledger.retained(), CREDIT);
                    match &rejected {
                        buffered::Rejected::Admission { state, error } => {
                            assert_eq!(declared, 4);
                            assert_eq!(state.len(), 3);
                            assert!(matches!(
                                error,
                                body::Error::LengthMismatch {
                                    actual: 3,
                                    declared: 4
                                }
                            ));
                        }
                        buffered::Rejected::Storage { failure, .. } => {
                            assert_eq!(payload, b"ab");
                            assert!(matches!(failure.error(), body::Error::SegmentMismatch));
                        }
                        actual => assert!(matches!(
                            actual,
                            buffered::Rejected::Admission { .. }
                                | buffered::Rejected::Storage { .. }
                        )),
                    }
                    let released = rejected.try_release(&mut ledger, &mut trace);
                    assert!(released.is_ok(), "original ledger releases input credit");
                    assert_eq!(pool.available(), 0);
                    drop(released);
                    credit::Disposition::Discarded
                }
            };
            assert_eq!(pool.available(), 1);
            assert!(ledger.retained().is_empty());
            assert!(
                trace.changes
                    == [
                        credit::Mutation::Reserved(CREDIT),
                        credit::Mutation::Released {
                            credit: CREDIT,
                            disposition: kind
                        },
                    ]
            );
        });
    }
}
