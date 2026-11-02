use std::{cell::Cell, convert::identity, mem, num::NonZero};
pub mod common;

use core::convert::Infallible;

use o3::buffer::{pool, segments};
use sark_core::{
    body::{admit::Track, buffered, credit, credit::raw::Account, input, stream::Drive},
    manifold::stream::Boundary,
};
use sark_http::payload::owned;

use crate::common::{
    CREDIT, LIMITS, Retained as _,
    ready::Ready,
    trace::{Released, Trace},
};

#[test]
fn constructs_by_policy() {
    use credit::Evidence as _;

    let disabled = credit::Disabled::new();
    let amount = credit::Disabled::credit(CREDIT.bytes(), CREDIT.chunks());
    assert!(amount.is_empty());
    assert!(<Infallible as credit::Mode>::head().is_none());
    assert_eq!(mem::size_of_val(&disabled), 0);

    let mut trace = Trace::default();
    trace.head();
    trace.reserved(credit::Amount::default());
    trace.build_ready();
    assert!(trace.changes == [credit::Mutation::Head, credit::Mutation::BuildReady]);
}

#[test]
fn release_follows_drop() {
    for phase in 0..3 {
        Account::new().scope(|mut ledger| {
            let valid = Cell::new(true);
            let pool = pool::Pool::try_new(1, 8).expect("one input slot");
            let source = pool
                .try_copy(b"abc")
                .expect("input fits")
                .expect("vacant slot");
            let source = owned::Segments::from(source.into_shared());
            let open = Track::try_start(3).expect("admitted body");
            let mut body = buffered::State::start(open, &ledger);
            let pushed = body.try_push(
                &mut ledger,
                credit::Disabled,
                input::Input::new(source, 3, segments::Count::ONE),
                LIMITS,
                LIMITS,
                |_, source| buffered::Retain::<_, _, Infallible>::Accepted {
                    storage: Some(source),
                },
            );
            assert!(matches!(pushed, buffered::Push::Accepted));
            let mut slots = None;
            let mut event = Released(|| slots = Some(pool.available()));
            let released = if phase == 0 {
                body.discard(&mut ledger, &mut event)
            } else {
                let body = body
                    .try_stream(Ready::new(Ok(()), &valid), Boundary::End, identity)
                    .ok()
                    .expect("buffered input transfers");
                if phase == 2 {
                    let (step, _) =
                        body.try_drive(&mut ledger, credit::Disabled, NonZero::<usize>::MIN);
                    let ready = match step {
                        Drive::Ready(ready) => Some(ready),
                        _ => None,
                    }
                    .expect("completed input owns its ready payload");
                    ready.discard(&mut ledger, &mut event)
                } else {
                    body.discard(&mut ledger, &mut event)
                }
            };
            assert!(matches!(released, credit::Discard::Applied));
            assert_eq!(slots, Some(1));
            assert!(ledger.retained().is_empty());
        });
    }
}
