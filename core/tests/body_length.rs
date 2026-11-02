pub mod common;

use core::convert::Infallible;

use o3::buffer::segments;
use sark_core::{
    body::{
        Error,
        admit::{Length, Open, Plan, Retention, Start, Stream, Track},
        buffered, credit,
        credit::raw::Account,
        input,
    },
    manifold::stream::Boundary,
};

fn push(open: Track<Open>, bytes: usize) -> Result<(), Error> {
    Account::new().scope(|mut ledger| {
        let mut body = buffered::State::<(), _>::start(open, &ledger);
        let limits = credit::Limits::new(usize::MAX, usize::MAX);
        let mut retained = 0;
        let result = body.try_push(
            &mut ledger,
            credit::Disabled::new(),
            input::Input::new((), bytes, segments::Count::EMPTY),
            limits,
            limits,
            |store, ()| {
                retained += 1;
                assert!(store.is_none(), "discarded body cannot retain storage");
                buffered::Retain::<(), (), Infallible>::Accepted { storage: store }
            },
        );
        assert_eq!(retained, 0);
        match result {
            buffered::Push::Accepted | buffered::Push::Discarded(()) => Ok(()),
            buffered::Push::Rejected { input: (), error } => Err(match error {
                buffered::Error::Body(error) => error,
                buffered::Error::Retain(never) => match never {},
            }),
        }
    })
}

#[test]
fn declared_overrun() {
    let open = common::discarded(16, Some(4)).expect("declared input must remain open");
    assert!(matches!(
        push(open, 5),
        Err(Error::LengthMismatch {
            actual: 5,
            declared: 4
        })
    ));
}

#[test]
fn declared_underrun_first() {
    Account::new().scope(|mut ledger| {
        let open = common::discarded(16, Some(4)).expect("declared input must remain open");
        let mut body = buffered::State::<(), _>::start(open, &ledger);
        let limits = credit::Limits::new(usize::MAX, usize::MAX);
        let mut retained = 0;
        let result = body.try_push(
            &mut ledger,
            credit::Disabled::new(),
            input::Input::new((), 3, segments::Count::EMPTY),
            limits,
            limits,
            |store, ()| {
                retained += 1;
                assert!(store.is_none(), "discarded body cannot retain storage");
                buffered::Retain::<(), (), Infallible>::Accepted { storage: store }
            },
        );
        assert!(matches!(result, buffered::Push::Discarded(())));
        assert_eq!(retained, 0);

        let failure = body
            .try_finish(&mut ledger, &mut credit::Disabled::new())
            .expect_err("declared underrun must reject completion");
        assert!(matches!(
            failure.error(),
            Error::LengthMismatch {
                actual: 3,
                declared: 4
            }
        ));
        match &failure {
            buffered::Rejected::Admission { state, .. } => {
                assert_eq!(state.len(), 3);
                assert!(state.retention() == Retention::Discarded);
            }
            outcome => assert!(matches!(outcome, buffered::Rejected::Admission { .. })),
        }
        assert!(ledger.retained().is_empty());
        drop(
            failure
                .try_release(&mut ledger, &mut credit::Disabled::new())
                .expect("discarded input retains zero credit"),
        );
    });
}

#[test]
fn limit_overrun() {
    let open = common::discarded(4, None).expect("unknown input must remain open");
    assert!(matches!(
        push(open, 5),
        Err(Error::TooLarge {
            actual: 5,
            limit: 4
        })
    ));
}

#[test]
fn declared_over_limit() {
    let start =
        Plan::new(Retention::Discarded, 4, Length::Known(8), Stream::Buffered).try_open(usize::MAX);
    assert!(matches!(
        start,
        Err(Error::TooLarge {
            actual: 8,
            limit: 4
        })
    ));
}

#[test]
fn finished_rejects_length() {
    let start = Plan::new(Retention::Discarded, 4, Length::Known(1), Stream::Buffered)
        .try_start(usize::MAX, Boundary::End);
    assert!(matches!(
        start,
        Err(Error::LengthMismatch {
            actual: 0,
            declared: 1
        })
    ));
}

#[test]
fn limit_precedes_finished() {
    let start = Plan::new(Retention::Discarded, 4, Length::Known(8), Stream::Buffered)
        .try_start(4, Boundary::End);
    assert!(matches!(
        start,
        Err(Error::TooLarge {
            actual: 8,
            limit: 4
        })
    ));
}

#[test]
fn finished_accepts_empty() {
    let absent = Plan::new(Retention::Discarded, 4, Length::Unknown, Stream::Buffered)
        .try_start(4, Boundary::End);
    let zero = Plan::new(Retention::Discarded, 4, Length::Known(0), Stream::Buffered)
        .try_start(4, Boundary::End);
    assert!(matches!(absent, Ok(Start::Finished(_))));
    assert!(matches!(zero, Ok(Start::Finished(_))));
}

#[test]
fn admit_declared_length() {
    let short =
        Plan::new(Retention::Discarded, 8, Length::Known(4), Stream::Buffered).try_admit(8, 3);
    assert!(matches!(
        short,
        Err(Error::LengthMismatch {
            actual: 3,
            declared: 4
        })
    ));
    let exact = Plan::new(Retention::Discarded, 8, Length::Known(4), Stream::Buffered)
        .try_admit(8, 4)
        .expect("complete declared body");
    assert_eq!(exact.len(), 4);
    assert_eq!(exact.bound(), 4);
}

#[test]
fn finished_retains_limit() {
    Account::new().scope(|mut ledger| {
        let done = match Plan::new(Retention::Discarded, 8, Length::Unknown, Stream::Buffered)
            .try_start(4, Boundary::End)
            .expect("empty finished body")
        {
            Start::Finished(done) => done,
            outcome => {
                assert!(matches!(outcome, Start::Finished(_)), "finished phase");
                return;
            }
        };
        let mut body = buffered::State::<(), _>::ended(done, &ledger);
        let limits = credit::Limits::new(usize::MAX, usize::MAX);
        let result = body.try_push(
            &mut ledger,
            credit::Disabled::new(),
            input::Input::new((), 1, segments::Count::EMPTY),
            limits,
            limits,
            |storage, ()| buffered::Retain::<(), (), Infallible>::Accepted { storage },
        );
        assert!(matches!(
            result,
            buffered::Push::Rejected {
                input: (),
                error: buffered::Error::Body(Error::InputFinished),
            }
        ));
        assert_eq!(body.len(), 0);
        assert_eq!(body.max_body(), 4);
    });
}

#[test]
fn overflow_keeps_operands() {
    Account::new().scope(|mut ledger| {
        let open = common::discarded(usize::MAX, None).expect("unbounded input");
        let mut body = buffered::State::<(), _>::start(open, &ledger);
        let limits = credit::Limits::new(usize::MAX, usize::MAX);
        for added in [usize::MAX, 1] {
            let result = body.try_push(
                &mut ledger,
                credit::Disabled::new(),
                input::Input::new((), added, segments::Count::EMPTY),
                limits,
                limits,
                |storage, ()| buffered::Retain::<(), (), Infallible>::Accepted { storage },
            );
            if added == usize::MAX {
                assert!(matches!(result, buffered::Push::Discarded(())));
            } else {
                assert!(matches!(
                    result,
                    buffered::Push::Rejected {
                        input: (),
                        error: buffered::Error::Body(Error::Overflow {
                            current: usize::MAX,
                            added: 1
                        }),
                    }
                ));
            }
            assert_eq!(body.len(), usize::MAX);
        }
    });
}

#[test]
fn admit_keeps_policy() {
    for mode in [Retention::Retained, Retention::Discarded] {
        for stream in [Stream::Buffered, Stream::Streaming] {
            for length in [Length::Known(3), Length::Unknown] {
                let open = Plan::new(mode, 8, length, stream)
                    .try_open(4)
                    .expect("admitted policies");
                assert!(open.retention() == mode);
                assert!(open.stream() == stream);
                assert_eq!(
                    open.bound(),
                    match length {
                        Length::Known(value) => value,
                        Length::Unknown => 4,
                    }
                );
            }
        }
    }
}
