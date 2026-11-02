pub mod common;

use std::{cell::Cell, convert::Infallible};

use common::ready::Ready;
use o3::buffer::segments;
use sark_core::{
    body::{self, admit, buffered, credit, credit::raw::Account, input, stream},
    manifold::stream::Boundary,
};
use sark_http::payload::owned;

#[test]
fn counts_discarded_total() {
    let valid = Cell::new(true);
    for counted in [0, 1, 3] {
        Account::new().scope(|mut ledger| {
            let open = admit::Plan::new(
                admit::Retention::Discarded,
                3,
                admit::Length::Known(3),
                admit::Stream::Streaming,
            )
            .try_open(3)
            .expect("admitted body");
            let body = stream::Stream::start(open, Ready::new(Ok(()), &valid), &ledger);
            let body = match body.try_discard(
                &mut ledger,
                credit::Disabled,
                owned::Segments::new(),
                counted,
                Boundary::More,
            ) {
                body::Transfer::Accepted(body) => Some(body),
                body::Transfer::Rejected { .. } => None,
            }
            .expect("discard within declared length");
            let body = if counted == 3 {
                match body.try_exact(&mut ledger, credit::Disabled, 2) {
                    body::Transfer::Rejected { build, error } => {
                        assert_eq!(error, body::Error::SegmentMismatch);
                        Some(build)
                    }
                    body::Transfer::Accepted(_) => None,
                }
                .expect("shrinking total returns original builder")
            } else {
                body
            };
            let body = match body.try_exact(&mut ledger, credit::Disabled, 3) {
                body::Transfer::Accepted(body) => Some(body),
                body::Transfer::Rejected { .. } => None,
            }
            .expect("final total includes previously counted input");
            assert!(matches!(
                body.discard(&mut ledger, &mut credit::Disabled),
                credit::Discard::Applied
            ));
        });
    }
    assert!(valid.get());
}

#[test]
fn buffered_total_preserves_credit_and_finishes_once() {
    for retention in [admit::Retention::Discarded, admit::Retention::Retained] {
        Account::new().scope(|mut ledger| {
            let open = admit::Plan::new(
                retention,
                3,
                admit::Length::Known(3),
                admit::Stream::Buffered,
            )
            .try_open(3)
            .expect("admitted body");
            let mut body = buffered::State::<&[u8], _>::start(open, &ledger);
            let retained = retention == admit::Retention::Retained;
            if retained {
                let limits = credit::Limits::new(3, 1);
                let pushed = body.try_push(
                    &mut ledger,
                    credit::Disabled,
                    input::Input::new(b"abc".as_slice(), 3, segments::Count::ONE),
                    limits,
                    limits,
                    |_, input| buffered::Retain::<_, _, Infallible>::Accepted {
                        storage: Some(input),
                    },
                );
                assert!(matches!(pushed, buffered::Push::Accepted));
            }
            let body::Transfer::Rejected { build: body, error } = body.try_exact(2) else {
                panic!("incorrect final length accepted");
            };
            assert_eq!(
                error,
                if retained {
                    body::Error::SegmentMismatch
                } else {
                    body::Error::LengthMismatch {
                        actual: 2,
                        declared: 3,
                    }
                }
            );
            let body::Transfer::Accepted(body) = body.try_exact(3) else {
                panic!("final total rejected");
            };
            assert_eq!(body.len(), 3);
            assert_eq!(ledger.retained().is_empty(), !retained);
            let body::Transfer::Rejected { build: body, error } = body.try_exact(3) else {
                panic!("repeated final total accepted");
            };
            assert_eq!(error, body::Error::InputFinished);
            let body = body
                .try_finish(&mut ledger, &mut credit::Disabled)
                .expect("admitted total and storage remain usable");
            assert_eq!(body.len(), 3);
            assert_eq!(body.into_source(), if retained { &b"abc"[..] } else { &[] });
            assert!(ledger.retained().is_empty());
        });
    }
}
