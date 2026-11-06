pub mod common;

use common::requests::{Plan as _, Segments};
use dope_test::alloc::Tracker;
use o3::buffer::{hold::ranges, storage};
use sark_core::body::{self, admit, admit::Retention};
use sark_http::{
    payload::{owned, response::Body as _},
    request::body::{Error, Ref},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn segments_keep_owners() {
    let input = Vec::from(b"_ab__cde_");
    let ranges = [(1, 3), (5, 8)];
    let ranges = ranges::Set::try_new(input.as_slice(), &ranges, 5).expect("range admission");
    let source = Segments::new(ranges);
    let mut body = Retention::Retained
        .plan(5)
        .try_finish(5, 5, source)
        .expect("body admission");
    let result = <body::Segmented as body::Project>::try_project(&mut body, (), |view| {
        let view = view.into_inner();
        (
            view.segments().map(<[u8]>::to_vec).collect::<Vec<_>>(),
            view.len(),
        )
    });
    let (segments, len) = result.expect("segmented projection");
    assert_eq!(segments, [b"ab".as_slice(), b"cde".as_slice()]);
    assert_eq!(len, 5);
    assert_eq!(body.into_source().coalesces, 0);
}

#[test]
fn contiguous_coalesces() {
    let input = Vec::from(b"_ab__cde_");
    let ranges = [(1, 3), (5, 8)];
    let ranges = ranges::Set::try_new(input.as_slice(), &ranges, 5).expect("range admission");
    let source = Segments::new(ranges);
    let mut body = Retention::Retained
        .plan(5)
        .try_finish(5, 5, source)
        .expect("body admission");
    let result = <body::Contiguous as body::Project>::try_project(&mut body, (), |view| {
        let view = view.into_inner();
        (view.as_slice().to_vec(), view.as_slice().len())
    });
    let (bytes, len) = result.expect("contiguous projection");
    assert_eq!(bytes, b"abcde");
    assert_eq!(len, 5);
    assert_eq!(body.into_source().coalesces, 1);
}

#[test]
fn discarded_keeps_len() {
    let mut body = Retention::Discarded
        .plan(5)
        .try_finish(5, 5, owned::Segments::new())
        .expect("discarded admission");
    assert_eq!(body.len(), 5);
    let result = <body::Discarded as body::Project>::try_project(&mut body, (), |view| {
        Ref::from(view.into_inner())
    });
    let view = result.expect("discarded projection");
    assert_eq!(view.len(), 5);
    assert!(view.count().is_empty());
    assert!(view.as_slice().is_none());
    assert!(view.segments().next().is_none());
    std::assert_matches!(view.try_copy().err(), Some(owned::Error::Discarded));
    assert!(body.into_source().is_empty());
}

#[test]
fn retained_moves_storage() {
    let owner = storage::Shared::try_copy(b"retained").expect("storage allocation");
    let pointer = owner.as_ptr();
    let segments = owned::Segments::from(owner);
    let mut body = Retention::Retained
        .plan(8)
        .try_finish(8, 8, segments)
        .expect("retained admission");
    let context = Box::new(pointer);
    let address = &*context as *const *const u8;
    let (result, count) = Tracker::count(|| {
        <body::Contiguous as body::Project>::try_project(&mut body, context, |scope| {
            let (context, view) = scope.into_parts();
            (context, view.into_inner().as_slice().as_ptr())
        })
    });
    assert_eq!(count, 0);
    let (context, actual) = result.expect("retained projection");
    assert_eq!(&*context as *const *const u8, address);
    assert_eq!(actual, pointer);
    assert_eq!(body.len(), 8);
    assert_eq!(
        body.into_source().as_slice().map(<[u8]>::as_ptr),
        Some(pointer)
    );
}

#[test]
fn rejection_keeps_owner() {
    let input = Vec::from(b"_ab__cde_");
    let ranges = [(1, 3), (5, 8)];
    let ranges = ranges::Set::try_new(input.as_slice(), &ranges, 5).expect("range admission");
    let source = Ref::from_ranges(&ranges);
    let mut body = Retention::Retained
        .plan(5)
        .try_finish(5, 5, source)
        .expect("body admission");
    let owner = Box::new(37);
    let pointer = &*owner as *const i32;
    let mut called = false;
    let result = <body::Contiguous as body::Project>::try_project(&mut body, owner, |scope| {
        called = true;
        scope.into_parts().0
    });
    let failure = result.expect_err("projection rejection");
    assert!(!called, "a rejected projection must not call its consumer");
    let (owner, error) = failure.into_parts();
    assert!(
        matches!(error, Error::Borrowed { len: 5, count } if count.into_raw() == 2),
        "exact borrowed rejection",
    );
    assert_eq!(&*owner as *const i32, pointer);
    assert_eq!(*owner, 37);
    let source = body.into_source();
    assert_eq!(source.len(), 5);
    assert_eq!(source.segments().next(), Some(b"ab".as_slice()));
}

#[test]
fn admission_returns_source() {
    let source = storage::Shared::try_copy(b"four").expect("body owner");
    let pointer = source.as_ptr();
    let source = owned::Segments::from(source);
    let (result, allocs) = Tracker::count(|| Retention::Retained.plan(3).try_finish(3, 4, source));
    assert_eq!(allocs, 0);
    let failure = result.expect_err("declared length rejection");
    assert!(matches!(failure, admit::Error::Admission(_)));
    if let admit::Error::Admission(failure) = failure {
        let (source, error) = failure.into_parts();
        assert!(matches!(
            error,
            body::Error::LengthMismatch {
                actual: 4,
                declared: 3
            }
        ));
        assert_eq!(source.as_slice().map(<[u8]>::as_ptr), Some(pointer));
        assert_eq!(source.as_slice(), Some(b"four".as_slice()));
    }
}

#[test]
fn completion_keeps_source() {
    let source = storage::Shared::try_copy(b"four").expect("body owner");
    let pointer = source.as_ptr();
    let source = owned::Segments::from(source);
    let (result, allocs) = Tracker::count(|| Retention::Retained.plan(5).try_finish(5, 5, source));
    assert_eq!(allocs, 0);
    let failure = result.expect_err("retained length rejection");
    assert!(matches!(failure, admit::Error::Storage(_)));
    if let admit::Error::Storage(failure) = failure {
        let ((track, source), error) = failure.into_parts();
        assert!(matches!(error, body::Error::SegmentMismatch));
        assert_eq!((track.len(), track.bound()), (5, 5));
        assert!(track.retention() == Retention::Retained);
        assert_eq!(source.as_slice().map(<[u8]>::as_ptr), Some(pointer));
        assert_eq!(source.as_slice(), Some(b"four".as_slice()));
    }
}
