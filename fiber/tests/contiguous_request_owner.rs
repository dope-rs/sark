pub mod common;

use std::num::NonZero;

use dope::core::{
    BranchError,
    driver::{self, life::recovery::Policy, schedule::ready::task::owner, settings},
};
use dope_test::{
    alloc::Tracker,
    scope::{Driver, Error},
};
use o3::buffer::{resident, storage};
use sark_core::{
    body::{Step, Transfer},
    route::{Push as _, TaskBuild as _},
};
use sark_http::{
    payload::owned::{Segment, flatten},
    request::Seq,
};

use crate::common::{
    h1::chunked,
    route::chunked::apps::{SegmentedReq, WholeReq},
};
use common::server::recover;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn bounded_owner_transfer() {
    let mut reports = Vec::new();
    Driver::new(settings::Config::for_profile::<sark_fiber::Throughput>()).with_driver(
        |driver| {
            let binding = driver::Binding::new(
                driver.driver_ref().timer(),
                owner::Capacity::new(NonZero::<u32>::MIN),
            );
            let ids = Seq::new(binding.sequence());
            for cancel in [false, true] {
                let (outcome, activity) = Tracker::activity(|| {
                    let mut build =
                        WholeReq::try_builder(ids.try_next().expect("request identity"))
                            .expect("request fields");
                    for bytes in chunked::PARTS {
                        let segment = Segment::from(resident::Shared::from(
                            storage::Shared::try_copy(bytes).expect("body owner"),
                        ));
                        let next = match build.push_body(segment) {
                            Transfer::Accepted(build) => Ok(build),
                            Transfer::Rejected { error, .. } => Err(error),
                        }
                        .expect("body transfer");
                        build = next;
                    }
                    let (mut build, released) =
                        match build.finish_body(6).step(NonZero::<usize>::MIN) {
                            Step::Pending { build, released } => Ok((build, released)),
                            Step::Failed { build, error, .. } => Err(Some((build, error).1)),
                            Step::NeedInput { .. } | Step::Ready { .. } => Err(None),
                        }
                        .expect("one byte budget must yield");
                    assert!(released.is_empty());
                    let late = storage::Shared::try_copy(chunked::LATE).expect("late owner");
                    let pointer = late.as_slice().as_ptr();
                    let (next, (late, error)) =
                        match build.push_body(Segment::from(resident::Shared::from(late))) {
                            Transfer::Rejected { build, error } => Some((build, error)),
                            Transfer::Accepted(_) => None,
                        }
                        .expect("finished input must reject a segment");
                    assert_eq!(error, flatten::Error::Finished);
                    assert_eq!(late.as_slice().as_ptr(), pointer);
                    assert_eq!(late.as_slice(), chunked::LATE);
                    drop(late);
                    build = next;
                    if cancel {
                        drop(build);
                        return Ok(());
                    }
                    let mut polls = 1;
                    loop {
                        polls += 1;
                        assert!(polls <= 8, "bounded body must complete");
                        match build.step(NonZero::<usize>::MIN) {
                            Step::Pending {
                                build: next,
                                released,
                            } => {
                                assert!(released.is_empty());
                                build = next;
                            }
                            Step::Ready { output, released } => {
                                assert!(released.is_empty());
                                assert_eq!(
                                    output.into_parts().1.payload.as_slice(),
                                    chunked::JOINED
                                );
                                break;
                            }
                            Step::Failed { build, error, .. } => {
                                return Err(Some((build, error).1));
                            }
                            Step::NeedInput { .. } => return Err(None),
                        }
                    }
                    Ok(())
                });
                outcome.expect("closed body must progress");
                assert_eq!(activity.allocated_bytes, activity.deallocated_bytes);
            }

            for cancel in [false, true] {
                let (_, activity) = Tracker::activity(|| {
                    let owner = storage::Shared::try_copy(chunked::SINGLE).expect("body owner");
                    let pointer = owner.as_slice().as_ptr();
                    let build =
                        match SegmentedReq::try_builder(ids.try_next().expect("request identity"))
                            .expect("request fields")
                            .push_body(Segment::from(resident::Shared::from(owner)))
                        {
                            Transfer::Accepted(build) => Ok(build),
                            Transfer::Rejected { error, .. } => Err(error),
                        }
                        .expect("body transfer");
                    let build = match build.step(NonZero::<usize>::MIN) {
                        Step::NeedInput { build, released } => {
                            assert!(released.is_empty());
                            Some(build)
                        }
                        Step::Failed { error, .. } => match error {},
                        Step::Ready { .. } | Step::Pending { .. } => None,
                    }
                    .expect("open input must wait");
                    let mut build = build.finish_body(chunked::SINGLE.len());
                    for bytes in [b"".as_slice(), chunked::LATE.as_slice()] {
                        let late = Segment::from(resident::Shared::from(
                            storage::Shared::try_copy(bytes).expect("late owner"),
                        ));
                        let late_pointer = late.as_slice().as_ptr();
                        let ((next, (late, error)), allocs) = Tracker::count(|| {
                            match build.push_body(late) {
                                Transfer::Rejected { build, error } => Some((build, error)),
                                Transfer::Accepted(_) => None,
                            }
                            .expect("finished input must reject every segment")
                        });
                        assert_eq!(allocs, 0);
                        assert_eq!(error, flatten::Error::Finished);
                        assert_eq!(late.as_slice().as_ptr(), late_pointer);
                        assert_eq!(late.as_slice(), bytes);
                        drop(late);
                        build = next;
                    }
                    if cancel {
                        drop(build);
                        return;
                    }
                    let (step, allocs) = Tracker::count(|| build.step(NonZero::<usize>::MIN));
                    let (output, released) = match step {
                        Step::Ready { output, released } => Some((output, released)),
                        Step::Failed { error, .. } => match error {},
                        Step::NeedInput { .. } | Step::Pending { .. } => None,
                    }
                    .expect("finished segmented input must complete");
                    assert!(released.is_empty());
                    assert_eq!(allocs, 0);
                    let request = output.into_parts().1;
                    assert_eq!(request.len.len(), chunked::SINGLE.len());
                    assert_eq!(request.payload.segment(0), Some(chunked::SINGLE.as_slice()));
                    assert_eq!(request.payload.segment(1), None);
                    assert_eq!(
                        request
                            .payload
                            .segment(0)
                            .expect("retained segment")
                            .as_ptr(),
                        pointer
                    );
                });
                assert_eq!(activity.allocated_bytes, activity.deallocated_bytes);
            }

            let owner = storage::Shared::try_copy(chunked::SINGLE).expect("single owner");
            let pointer = owner.as_slice().as_ptr();
            let build = match WholeReq::try_builder(ids.try_next().expect("request identity"))
                .expect("request fields")
                .push_body(Segment::from(resident::Shared::from(owner)))
            {
                Transfer::Accepted(build) => Ok(build),
                Transfer::Rejected { error, .. } => Err(error),
            }
            .expect("single owner transfer");
            let (step, allocs) =
                Tracker::count(|| build.finish_body(6).step(NonZero::<usize>::MIN));
            let (output, released) = match step {
                Step::Ready { output, released } => Ok((output, released)),
                Step::Failed { build, error, .. } => Err(Some((build, error).1)),
                Step::NeedInput { .. } | Step::Pending { .. } => Err(None),
            }
            .expect("single owner needs no copy");
            assert!(released.is_empty());
            assert_eq!(allocs, 0);
            assert_eq!(output.into_parts().1.payload.as_slice().as_ptr(), pointer);
        },
        Policy::new(|event| reports.push(event), recover),
        |result| {
            result
                .as_ref()
                .map_err(|error| match error {
                    Error::Create(error) => BranchError::First(error.kind()),
                    Error::Close(error) => BranchError::Second(error.cause()),
                })
                .expect("driver");
        },
    );
    assert!(
        reports.is_empty(),
        "identity-only scope returned I/O: {reports:?}"
    );
}
