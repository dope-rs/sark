use std::{cell::Cell, convert::identity, iter};

use common::{Retained as _, ready};
use o3::buffer::{resident, segments};
use sark_core::{
    body::{self, admit::Track, credit, credit::raw::Account, input, stream},
    manifold::stream::Boundary,
};
use sark_http::payload::owned;

pub mod common;

use common::ready::{Invalid, Ready};

#[test]
fn rejects_wrong_dimensions() {
    let valid = Cell::new(true);
    for (bytes, chunks) in [(4, segments::Count::ONE), (3, segments::Count::EMPTY)] {
        Account::new().scope(|mut ledger| {
            let open = Track::try_start(bytes).expect("admitted length");
            let body = stream::Stream::start(open, Ready::new(Err(Invalid), &valid), &ledger);
            let result = body.try_push(
                &mut ledger,
                credit::Disabled,
                input::Input::new(
                    owned::Segments::from(resident::Shared::from(ready::BYTES)),
                    bytes,
                    chunks,
                ),
                Boundary::More,
                (common::LIMITS, common::LIMITS),
                identity,
            );
            match result {
                stream::Push::Failed {
                    build,
                    error: body::Error::SegmentMismatch,
                } => {
                    assert!(matches!(
                        build.discard(&mut ledger, &mut credit::Disabled),
                        credit::Discard::Applied
                    ));
                }
                actual => assert!(matches!(
                    actual,
                    stream::Push::Failed {
                        error: body::Error::SegmentMismatch,
                        ..
                    }
                )),
            }
        });
    }
    assert!(valid.get());
}

#[test]
fn rejects_checked_upper_bounds_before_push() {
    let valid = Cell::new(true);
    for (bytes, chunks) in [(2, segments::Count::ONE), (3, segments::Count::EMPTY)] {
        Account::new().scope(|mut ledger| {
            let open = Track::try_start(bytes).expect("admitted length");
            let body = stream::Stream::start(open, Ready::new(Err(Invalid), &valid), &ledger);
            let stream::Push::Failed {
                build,
                error: body::Error::SegmentMismatch,
            } = body.try_push(
                &mut ledger,
                credit::Disabled,
                input::Input::new(
                    owned::Segment::from(resident::Shared::from(ready::BYTES)),
                    bytes,
                    chunks,
                ),
                Boundary::More,
                (common::LIMITS, common::LIMITS),
                |part| input::Checked::new(iter::once(part)),
            )
            else {
                panic!("oversized input must fail before reaching the rejecting builder");
            };
            assert!(matches!(
                build.discard(&mut ledger, &mut credit::Disabled),
                credit::Discard::Applied
            ));
        });
    }
}
