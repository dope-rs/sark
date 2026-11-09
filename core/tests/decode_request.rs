pub mod common;
use std::convert::Infallible;

use dope_test::alloc::Tracker;
use o3::buffer::segments;
use sark_core::{
    body::{
        buffered,
        credit::{self, raw::Account},
        input,
    },
    decode::{exec, request, state},
};

use crate::common::decoders::*;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn retains_without_alloc() {
    let bytes: &[u8] = b"route-bound";
    let ((), alloc) = Tracker::count(|| {
        Account::new().scope(|mut ledger| {
            let admitted = try_admit(Prepared { len: bytes.len() }).expect("route admission");
            let request = match admitted.into_request() {
                request::Body::Open(request) => Some(request),
                request::Body::Finished(_) => None,
            }
            .expect("nonempty request must remain open");
            let request = request
                .try_map_body(|open| {
                    let mut state = buffered::State::start(open, &ledger);
                    let limit = credit::Limits::new(bytes.len(), 1);
                    let pushed = state.try_push(
                        &mut ledger,
                        credit::Disabled::new(),
                        input::Input::new(bytes, bytes.len(), segments::Count::ONE),
                        limit,
                        limit,
                        |_, bytes| buffered::Retain::<_, _, Infallible>::Accepted {
                            storage: Some(bytes),
                        },
                    );
                    let finished = state.try_finish(&mut ledger, &mut credit::Disabled::new());
                    assert!(matches!(pushed, buffered::Push::Accepted));
                    finished
                })
                .expect("buffered request must finish")
                .ready();
            assert_eq!(request.prepared().len, bytes.len());
            assert_eq!(request.body().len(), bytes.len());
            let source = request.into_body().into_source();
            assert_eq!(source, bytes);
            assert_eq!(source.as_ptr(), bytes.as_ptr());
        })
    });

    assert_eq!(alloc, 0);
}

#[test]
fn finish_is_zero_alloc() {
    let routed = state::Uniform::<_, Buffered>::new(7usize, request::Body::Open(11usize));
    let (finished, alloc) = Tracker::count(|| state::Flow::finish(routed));

    assert_eq!(alloc, 0);
    let finished = finished
        .map_err(|finished| {
            <state::Uniforms<usize> as state::Homogeneous<usize, exec::Sync>>::fold(finished, Take)
        })
        .expect("open routed body must finish");
    let finished = (state::Flow::finish(finished))
        .err()
        .expect("finished routed body must reject a second finish");
    let output =
        <state::Uniforms<usize> as state::Homogeneous<usize, exec::Sync>>::fold(finished, Take);
    assert_eq!(output, (7, 11, true));
}

#[test]
fn sync_fold_is_zero_alloc() {
    let routed =
        state::Uniform::<_, Buffered>::new(Prepared { len: 23 }, request::Body::Finished(29));
    let (output, alloc) = Tracker::count(|| {
        <state::Uniforms<Prepared> as state::Homogeneous<Prepared, exec::Sync>>::fold(routed, Take)
    });

    assert_eq!(alloc, 0);
    assert!(output == (Prepared { len: 23 }, 29, true));
}
