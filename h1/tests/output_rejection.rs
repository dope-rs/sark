use std::num::NonZero;

use dope::manifold::network::listener::conn::write::{self, body, error};
use o3::{base::num::bounded, buffer::resident::bound};
use sark_h1::server::payload::{self, write::Failure};

#[test]
fn capacity_cause() {
    let rejected = payload::write::reject::<(), bound::Unbounded, ()>(error::Error::Capacity(
        write::Reject::Header,
    ));
    assert!(matches!(rejected, Failure::Capacity(write::Reject::Header)));
}

#[test]
fn mismatch_receipt() {
    let cause = bounded::Error {
        value: 7,
        incoming: NonZero::new(5).expect("nonzero addition"),
        maximum: 8,
    };
    let rejected = payload::write::reject::<(), bound::Unbounded, _>(error::Error::Mismatch(
        error::Mismatch::Bounded {
            cause: error::Bounded::Prefix(cause),
            draft: write::Draft::new(0, None, 19),
        },
    ));
    assert!(matches!(
        rejected,
        Failure::Rejected {
            cause: body::Failure::Mismatch(error::Cause::Prefix(actual)),
            receipt: 19,
        } if actual == cause
    ));
}
