pub mod common;

use std::{convert::Infallible, ops};

use common::requests::{HEAD_LEN, WIRE, ids, shorten, try_layout, try_retain};
use dope_core::driver::life::recovery;
use dope_test::{alloc::Tracker, scope};
use o3::buffer::storage;
use sark_core::request::{BindError, Head, View, frame, raw::Parsed};
use sark_http::{
    payload::response::{self, Body as _},
    request::{Bound, Error, Form, Layout, Target, body::Ref, span::Span},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn rejects_foreign_span() {
    let span = (Span::checked(0, WIRE.len() + 1)).expect("test operation must succeed");
    let target = (Target::from_span(Form::Path, span)).expect("test operation must succeed");
    let result = Head::<_>::bind(WIRE, Layout::new(target, None, Some(span)));
    assert!(matches!(
        result,
        Err(BindError::Layout(Error::InvalidRange))
    ));
}

#[test]
fn splits_without_alloc() {
    const {
        assert!(
            std::mem::size_of::<Parsed<Head<storage::Shared>, common::requests::Schema>>()
                == std::mem::size_of::<((), (), storage::Shared, Layout)>()
        );
    }
    let mut reports = Vec::new();
    ids(
        |ids| {
            let id = (ids.try_next()).expect("request id namespace must remain available");
            let request = storage::Shared::try_copy(WIRE).expect("test operation must succeed");
            let owner = request.as_ref().as_ptr();
            let (retained, alloc) = Tracker::count(|| try_retain(id, request));
            let retained = retained.expect("retained request");

            assert_eq!(alloc, 0);
            let head = retained
                .try_frame_at(0..HEAD_LEN)
                .expect("head projection must fit the retained frame");
            assert_eq!(head.as_slice().as_ptr(), owner);
            assert!(matches!(
                retained.try_frame_at(ops::Range { start: 2, end: 1 }),
                Err(frame::Error::Order {
                    start: 2,
                    end: 1,
                    capacity: HEAD_LEN,
                })
            ));
            assert!(matches!(
                retained.try_frame_at(0..HEAD_LEN + 1),
                Err(frame::Error::Bounds {
                    start: 0,
                    end,
                    capacity: HEAD_LEN,
                }) if end == HEAD_LEN + 1
            ));
            let body = retained.into_body();
            assert_eq!(body.as_slice(), Some(&WIRE[HEAD_LEN..]));
        },
        recovery::Policy::new(
            |event| reports.push(event),
            |error: recovery::Recovery<'_, '_>| -> Infallible {
                panic!("native recovery: {:?}", error.cause())
            },
        ),
        |result| {
            result
                .as_ref()
                .map_err(scope::Error::cause)
                .expect("driver lifetime");
        },
    );
    assert!(
        reports.is_empty(),
        "identity-only scope returned I/O: {reports:?}"
    );
}

#[test]
fn reports_ranges() {
    let layout = try_layout().expect("request layout");
    let head = Head::<_>::bind(WIRE, layout).expect("request layout must bind to its wire storage");
    let view = View::from_head(head, Ref::discarded(4));
    assert!(matches!(
        view.try_path(ops::Range { start: 2, end: 1 }),
        Err(frame::Error::Order {
            start: 2,
            end: 1,
            capacity: 6,
        })
    ));
    assert!(matches!(
        view.try_path(0..7),
        Err(frame::Error::Bounds {
            start: 0,
            end: 7,
            capacity: 6,
        })
    ));
}

#[test]
fn shortens_borrow() {
    let layout = try_layout().expect("request layout");
    let head = Head::<_>::bind(WIRE, layout).expect("request layout must bind to its wire storage");
    let request = View::from_head(head, Ref::discarded(4));
    let scope = ();
    let view = shorten(request, &scope);
    let path = view
        .try_path(0..6)
        .expect("borrowed path must retain the source lifetime");
    assert_eq!(response::Body::as_slice(&path), Some(b"/items".as_slice()));
}

#[test]
fn keeps_bound_owner() {
    let layout = try_layout().expect("request layout");
    let bound = Bound::try_bind(WIRE, layout).expect("bound owner");
    let head = Head::<_>::try_bound(bound).expect("request head");
    assert_eq!(head.borrow().path(), b"/items");
    assert!(std::ptr::eq(head.as_ref().as_ptr(), WIRE.as_ptr()));

    let bound = Bound::try_bind(WIRE, layout).expect("bound owner");
    assert!(matches!(
        Head::<_, frame::proof::Peer>::try_bound(bound),
        Err(BindError::Layout(Error::MissingAuthority))
    ));
}
