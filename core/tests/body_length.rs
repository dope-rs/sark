pub mod common;

use sark_core::{
    body::{
        Error,
        admit::{Length, Plan, Retention, Start, Stream},
        buffered, credit,
        credit::raw::Account,
    },
    manifold::stream::Boundary,
};

#[test]
fn rejects_body_overruns() {
    for (max, declared, expected) in [
        (
            16,
            Some(4),
            Error::LengthMismatch {
                actual: 5,
                declared: 4,
            },
        ),
        (
            4,
            None,
            Error::TooLarge {
                actual: 5,
                limit: 4,
            },
        ),
    ] {
        Account::new().scope(|mut ledger| {
            let open = common::discarded(max, declared).expect("open discarded input");
            let mut body = buffered::State::start(open, &ledger);
            let (
                false,
                buffered::Push::Rejected {
                    input: (),
                    error: buffered::Error::Body(error),
                },
            ) = common::push_discarded(&mut body, &mut ledger, 5)
            else {
                panic!("overrun must return the rejected input and body error");
            };
            assert_eq!(error, expected);
        });
    }
}

#[test]
fn declared_underrun_first() {
    Account::new().scope(|mut ledger| {
        let open = common::discarded(16, Some(4)).expect("declared input must remain open");
        let mut body = buffered::State::<(), _>::start(open, &ledger);
        assert!(matches!(
            common::push_discarded(&mut body, &mut ledger, 3),
            (false, buffered::Push::Discarded(()))
        ));

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
        let buffered::Rejected::Admission { state, .. } = &failure else {
            panic!("declared underrun must return the admission state");
        };
        assert_eq!(state.len(), 3);
        assert!(state.retention() == Retention::Discarded);
        assert!(ledger.retained().is_empty());
        drop(
            failure
                .try_release(&mut ledger, &mut credit::Disabled::new())
                .expect("discarded input retains zero credit"),
        );
    });
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
        let Start::Finished(done) =
            Plan::new(Retention::Discarded, 8, Length::Unknown, Stream::Buffered)
                .try_start(4, Boundary::End)
                .expect("empty finished body")
        else {
            panic!("finished phase");
        };
        let mut body = buffered::State::<(), _>::ended(done, &ledger);
        let (retained, result) = common::push_discarded(&mut body, &mut ledger, 1);
        assert!(!retained);
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
        for added in [usize::MAX, 1] {
            let (retained, result) = common::push_discarded(&mut body, &mut ledger, added);
            assert!(!retained);
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
