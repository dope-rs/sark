pub mod common;

use std::convert::Infallible;

use common::response::handoff::Tasks;
use dope_core::{
    BranchError,
    driver::{life::recovery, settings},
};
use dope_test::{
    alloc::Tracker,
    scope::{Driver, Error},
};
use o3::buffer::{bytes::scope, hold, pool, segments, storage};
use sark_http::{
    owner::{
        evidence,
        handoff::{self, Project as _, input::Input},
    },
    payload::{fixed, owned, response::Body as _},
    request::{self, body::Ref},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn projects_request_owner() {
    ids(|ids| {
        let id = ids.try_next().expect("request identity");
        let bytes = storage::Shared::try_copy(b"prefixpongtrailer").expect("request fixture");
        let body = request::Bytes::from_shared(id, bytes);
        let expected = body.as_slice()[6..].as_ptr();
        let ((ready, charge), allocs) = Tracker::count(|| {
            let rejected = body.project(6..64).err().expect("range exceeds request");
            let (body, error) = rejected.into_parts();
            assert_eq!((error.start(), error.end(), error.len()), (6, 64, 17));
            assert_eq!(body.as_slice(), b"prefixpongtrailer");
            assert!(body.id() == id);
            let body = body.project(6..10);
            assert!(body.is_ok(), "response range fits request");
            let body = body.ok().expect("response range fits request");
            assert_eq!(body.as_slice(), b"pong");
            assert_eq!(body.as_slice().as_ptr(), expected);
            assert!(body.id() == id);
            body.try_resolve(&handoff::Slot::task(id))
                .expect("request projection resolves")
                .try_finish()
                .expect("request projection finishes")
        });
        assert_eq!(allocs, 0);
        assert!(matches!(ready, handoff::Ready::Linked(_)));
        assert!(charge != hold::Charge::default());
    });
}

#[test]
fn links_matching_request() {
    ids(|ids| {
        let id = ids
            .try_next()
            .expect("the first request identity must be available");
        let body =
            storage::Shared::try_copy(b"request").expect("the request fixture must allocate");
        let body = request::Segments::new(id, owned::Segments::from(body));
        let expected = body.charge();
        let slot = handoff::Slot::task(id);
        let plan = body
            .try_resolve(&slot)
            .expect("the response body must resolve");

        let (ready, charge) = plan.try_finish().expect("the matching request must finish");
        assert!(matches!(ready, handoff::Ready::Linked(_)));

        assert!(expected != hold::Charge::default());
        assert!(charge == expected);
    });
}

#[test]
fn segments_foreign_request() {
    ids(|ids| {
        let expected = ids.try_next().expect("expected request identity");
        let foreign = ids.try_next().expect("foreign request identity");
        let body = storage::Shared::try_copy(b"foreign")
            .expect("the foreign request fixture must allocate");
        let body = request::Segments::new(foreign, owned::Segments::from(body));
        let slot = handoff::Slot::task(expected);
        let plan = body
            .try_resolve(&slot)
            .expect("the response body must resolve");

        let (ready, charge) = plan.try_finish().expect("the foreign request must finish");
        assert!(matches!(ready, handoff::Ready::Segmented(_)));

        assert!(charge == hold::Charge::default());
    });
}

#[test]
fn accepts_static_body() {
    let request = owned::Segments::new();
    let body = fixed::Body::new(b"static");
    let slot = handoff::Slot::new(Ref::from(&request));
    let plan = body
        .try_resolve(&slot)
        .expect("the response body must resolve");
    let ready = plan.finish().expect("the static response must finish");
    assert!(ready.retention() == hold::Charge::default());
    let body = match ready {
        handoff::Ready::Fixed(body) => Some(body),
        _ => None,
    }
    .expect("the static body must remain fixed");
    assert_eq!(body.as_slice(), b"static");
}

#[test]
fn releases_unrelated_head() {
    let pool = pool::Pool::try_new(1, 16).expect("the request head pool must allocate");
    let mut lease = pool
        .acquire()
        .expect("the unused pool must provide its lease");
    let append = lease
        .try_append(b"request-head")
        .expect("the request head must fit its lease");
    append.commit();
    scope::Owner::with(lease.freeze().into_shared(), |head| {
        let slot = handoff::Slot::empty().with_head(head.view());
        let plan = fixed::Body::new(b"static")
            .try_resolve(&slot)
            .expect("the static response must resolve");
        assert!(pool.acquire().is_none());
        let plan = plan.resolve_head(head);
        pool.acquire().expect("released head returns its lease");
        let body = match plan.finish().expect("static response handoff") {
            handoff::Ready::Fixed(body) => Some(body),
            _ => None,
        }
        .expect("the static body must remain fixed");
        assert_eq!(body.as_slice(), b"static");
    });
}

#[test]
fn defers_request_owner() {
    let owner = storage::Shared::try_copy(b"request").expect("the request fixture must allocate");
    let owner = owned::Segments::from(owner);
    let ready = Input::with(owner, |input| {
        let (pending, owner) = input.segmented((), |(), request, slot| {
            request
                .try_resolve(&slot)
                .expect("the response body must resolve")
                .detach()
        });
        pending.finish(owner)
    });
    let body = match ready {
        handoff::Ready::Linked(body) => Some(body),
        _ => None,
    }
    .expect("the request-owned response must stay linked");
    assert_eq!(body.as_slice(), Some(b"request".as_slice()));
}

#[test]
fn records_live_charge() {
    let owner = storage::Shared::try_copy(b"request").expect("the request fixture must allocate");
    let owner = owned::Segments::from(owner);
    let expected = owner.charge();
    let request = Ref::from(&owner);
    let slot = handoff::Slot::new(request);
    let plan = request
        .try_resolve(&slot)
        .expect("the response body must resolve");

    let (result, activity) = Tracker::measure(|| plan.try_finish());
    assert_eq!(activity, [0; 4]);
    let (ready, charge) = result.expect("the charged request owner must finish");
    assert!(matches!(ready, handoff::Ready::Linked(_)));

    assert!(charge == expected);
}

#[test]
fn retains_request_slice() {
    let owner = storage::Shared::try_copy(b"abcdef").expect("the request fixture must allocate");
    let owner = owned::Segments::from(owner);
    let source = owner
        .as_slice()
        .expect("the single request segment must be contiguous");
    let slice = &source[1..5];
    let slot = handoff::Slot::new(Ref::from(&owner));
    let plan = (slice)
        .try_resolve(&slot)
        .expect("the response body must resolve");
    let ready = plan
        .finish()
        .expect("the borrowed request slice must finish");
    let retained = match ready {
        handoff::Ready::Linked(retained) => Some(retained),
        _ => None,
    }
    .expect("the request slice must stay linked");
    let bytes = retained
        .as_slice()
        .expect("the retained request slice must stay contiguous");
    assert_eq!(bytes, b"bcde");
    assert_eq!(bytes.as_ptr(), slice.as_ptr());
}

#[test]
fn retains_head_offsets() {
    let owner = storage::Shared::try_copy(b"GET /items HTTP/1.1\r\n\r\n")
        .expect("the request head fixture must allocate");
    let pointer = owner.as_ref()[4..10].as_ptr();
    let (ready, allocs) = Tracker::count(|| {
        scope::Owner::with(owner, |head| {
            let slot = handoff::Slot::empty().with_head(head.view());
            let plan = head.as_ref()[4..10].try_resolve(&slot)?;
            Ok::<_, owned::Error>(plan.resolve_head(head))
        })
    });
    assert_eq!(allocs, 0);
    let plan = ready.expect("the selected head must resolve without a copy");
    let body = match plan.finish().expect("selected head handoff") {
        handoff::Ready::Retained(body) => Some(body),
        _ => None,
    }
    .expect("the selected head must retain its original owner");
    assert_eq!(body.as_ref(), b"/items");
    assert_eq!(body.as_ref().as_ptr(), pointer);
}

#[test]
fn copies_foreign_borrow() {
    let request = owned::Segments::new();
    let foreign = Vec::from(&b"foreign"[..]);
    let source = foreign.as_ptr();
    let slot = handoff::Slot::new(Ref::from(&request));
    let plan = (foreign.as_slice())
        .try_resolve(&slot)
        .expect("the response body must resolve");
    let ready = plan.finish().expect("the foreign borrow must finish");
    let copied = match ready {
        handoff::Ready::Copied(copied) => Some(copied),
        _ => None,
    }
    .expect("the foreign borrow must become an owned copy");
    let (body, copy) = copied.into_parts();
    assert!(
        (copy)
            == (evidence::Copy {
                read: 7,
                written: 7,
                cause: evidence::Cause::Borrowed,
            })
    );
    let bytes = body.as_ref();
    assert_eq!(bytes, b"foreign");
    assert_ne!(bytes.as_ptr(), source);
}

#[test]
fn keeps_response_segments() {
    let first = storage::Shared::try_copy(b"abc").expect("first response segment");
    let second = storage::Shared::try_copy(b"def").expect("second response segment");
    let pointers = [first.as_ref().as_ptr(), second.as_ref().as_ptr()];
    let response = owned::Segments::from(first);
    let response = response.try_extend(owned::Segments::from(second));
    let response = response.expect("response segment assembly must succeed");
    let request = owned::Segments::new();

    let slot = handoff::Slot::new(Ref::from(&request));
    let plan = response
        .try_resolve(&slot)
        .expect("the response body must resolve");
    let ready = plan.finish().expect("the segmented response must finish");
    let retained = match ready {
        handoff::Ready::Segmented(retained) => Some(retained),
        _ => None,
    }
    .expect("the segmented response must retain its segments");
    assert!(retained.count() == segments::Count::TWO);
    let first = retained.segment(0).expect("first response segment");
    let second = retained.segment(1).expect("second response segment");
    assert_eq!(first.as_ptr(), pointers[0]);
    assert_eq!(second.as_ptr(), pointers[1]);
}

#[test]
fn keeps_request_segments() {
    let first = storage::Shared::try_copy(b"abc").expect("first request segment");
    let second = storage::Shared::try_copy(b"def").expect("second request segment");
    let pointers = [first.as_ref().as_ptr(), second.as_ref().as_ptr()];
    let request = owned::Segments::from(first);
    let request = request.try_extend(owned::Segments::from(second));
    let request = request.expect("request segment assembly must succeed");
    let view = Ref::from(&request);
    let slot = handoff::Slot::new(view);

    let plan = view
        .try_resolve(&slot)
        .expect("the response body must resolve");
    let ready = plan.finish().expect("the segmented request must finish");
    let retained = match ready {
        handoff::Ready::Linked(retained) => Some(retained),
        _ => None,
    }
    .expect("the request segments must remain linked");

    assert!(retained.count() == segments::Count::TWO);
    let first = retained.segment(0).expect("first request segment");
    let second = retained.segment(1).expect("second request segment");
    assert_eq!(first.as_ptr(), pointers[0]);
    assert_eq!(second.as_ptr(), pointers[1]);
}

fn ids(run: impl for<'d> FnOnce(request::Seq<'d>)) {
    let mut reports = Vec::new();
    Driver::new(settings::Config::for_profile::<Tasks>()).with_driver(
        |driver| Tasks::with_ids(driver, run),
        recovery::Policy::new(
            |event| reports.push(event),
            |failure: recovery::Recovery<'_, '_>| -> Infallible {
                panic!("identity scope recovery: {:?}", failure.cause())
            },
        ),
        |result| {
            result
                .as_ref()
                .map_err(|error| match error {
                    Error::Create(error) => BranchError::First(error.kind()),
                    Error::Close(error) => BranchError::Second(error.cause()),
                })
                .expect("identity scope lifetime");
        },
    );
    assert!(
        reports.is_empty(),
        "identity-only scope returned I/O: {reports:?}"
    );
}
