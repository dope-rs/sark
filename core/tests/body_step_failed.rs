use std::{cell::Cell, num::NonZero};

use o3::{
    buffer::{pool, resident, segments},
    mem::Rejected,
};
use sark_core::{
    body::{
        self, admit::Track, buffered, credit, credit::raw::Account, flow, input::Checked,
        stream::Drive,
    },
    manifold::stream::Boundary,
};
use sark_http::payload::owned;

pub mod common;

use common::{
    CREDIT, Retained as _,
    ready::{BYTES, Failed, Invalid, Ready},
    trace::Trace,
};

#[test]
fn retains_failed_owner() {
    for released in [
        credit::Amount::default(),
        credit::Amount::new(4, segments::Count::ONE),
    ] {
        Account::new().scope(|mut ledger| {
            let dropped = Cell::new((0, 0));
            let pool = pool::Pool::try_new(1, 8).expect("one input slot");
            let source = pool
                .try_copy(b"abc")
                .expect("input fits")
                .expect("vacant slot");
            let open = Track::try_start(3).expect("admitted body");
            let segment = owned::Segment::from(resident::Shared::from(source.into_shared()));
            let (buffered, pushed) = open.retain(segment, &mut ledger);
            assert!(matches!(pushed, buffered::Push::Accepted));
            let body = buffered
                .try_stream(Failed::new(&dropped, released), Boundary::End, Checked::new)
                .map_err(Rejected::discard)
                .expect("retained input becomes a stream");
            let (step, mut evidence) =
                body.try_drive(&mut ledger, credit::Disabled, NonZero::<usize>::MIN);
            let (stream, error) = match step {
                Drive::Failed { stream, error } => Some((stream, error)),
                _ => None,
            }
            .expect("failed construction retains its stream and exact error");
            let expected = if released.is_empty() {
                flow::Error::Build(Invalid)
            } else {
                flow::Error::Release {
                    build: Invalid,
                    error: credit::Error::ByteUnderflow {
                        scope: credit::Scope::Request,
                        current: 3,
                        removed: 4,
                    },
                }
            };
            assert_eq!(error, expected);
            assert_eq!(dropped.get(), (0, 0));
            assert_eq!(pool.available(), 0);
            assert_eq!(ledger.retained(), CREDIT);
            assert!(matches!(
                stream.discard(&mut ledger, &mut evidence),
                credit::Discard::Applied
            ));
            assert_eq!(dropped.get(), (1, 1));
            assert_eq!(pool.available(), 1);
            assert!(ledger.retained().is_empty());
        });
    }
}

#[test]
fn rejects_ready_before_input_end_on_retry() {
    Account::new().scope(|mut ledger| {
        let valid = Cell::new(true);
        let pool = pool::Pool::try_new(1, 8).expect("one input slot");
        let source = pool
            .try_copy(BYTES)
            .expect("input fits")
            .expect("vacant slot");
        let open = Track::try_start(BYTES.len()).expect("admitted body");
        let segment = owned::Segment::from(resident::Shared::from(source.into_shared()));
        let (buffered, pushed) = open.retain(segment, &mut ledger);
        assert!(matches!(pushed, buffered::Push::Accepted));
        let mut stream = buffered
            .try_stream(Ready::new(Ok(()), &valid), Boundary::More, Checked::new)
            .map_err(Rejected::discard)
            .expect("retained input becomes a stream");
        let mut evidence = Trace::default();
        for _ in 0..2 {
            let Drive::Rejected {
                stream: next,
                error: body::Error::InputIncomplete,
            } = stream.try_step(&mut ledger, &mut evidence, NonZero::<usize>::MIN)
            else {
                panic!("ready output requires finished input");
            };
            stream = next;
        }
        assert!(evidence.changes.is_empty());
        assert_eq!(ledger.retained(), CREDIT);
        assert_eq!(pool.available(), 0);
        assert!(matches!(
            stream.discard(&mut ledger, &mut evidence),
            credit::Discard::Applied
        ));
        assert_eq!(pool.available(), 1);
        assert!(ledger.retained().is_empty());
        assert!(
            evidence.changes
                == [credit::Mutation::Released {
                    credit: CREDIT,
                    disposition: credit::Disposition::Discarded,
                }]
        );
    });
}
