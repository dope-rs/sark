use dope_test::alloc::Tracker;
use hold::ranges;
use o3::buffer::{Residency as _, hold, resident, segments::assembly, storage};
use sark_http::{
    payload::{owned, response::Body as _},
    request::body,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn mixed_charge_tracks_live() {
    let result = storage::Shared::try_copy(b"resident");
    let resident = result.expect("resident allocation must succeed");
    let expected = hold::Charge::from_owner(resident.len(), resident.resident());
    let body = owned::Segments::new();
    let result = body.try_push(owned::Segment::from(resident::Shared::from(
        b"static-prefix" as &[u8],
    )));
    let body = result.expect("the segment must append");
    let result = body.try_push(owned::Segment::from(resident::Shared::from(resident)));
    let body = result.expect("the segment must append");
    let result = body.try_push(owned::Segment::from(resident::Shared::from(
        b"static-suffix" as &[u8],
    )));
    let body = result.expect("the segment must append");

    assert!(body.charge() == expected);
    let mut segments = body.into_iter();
    assert_eq!(
        segments.byte_len(),
        b"static-prefixresidentstatic-suffix".len()
    );
    assert!(segments.charge() == expected);

    assert_eq!(segments.next().as_deref(), Some(&b"static-prefix"[..]));
    assert_eq!(segments.byte_len(), b"residentstatic-suffix".len());
    assert!(segments.charge() == expected);

    assert_eq!(segments.next().as_deref(), Some(&b"resident"[..]));
    assert_eq!(segments.byte_len(), b"static-suffix".len());
    assert!(segments.charge() == hold::Charge::default());

    assert_eq!(segments.next().as_deref(), Some(&b"static-suffix"[..]));
    assert_eq!(segments.byte_len(), 0);
    assert!(segments.charge() == hold::Charge::default());
    assert!(segments.next().is_none());
}

#[test]
fn static_has_no_charge() {
    let bytes = b"program-lifetime".as_slice();
    let body = owned::Segments::from(resident::Shared::from(bytes));
    assert!(body.charge() == hold::Charge::default());
    let taken = body.take().expect("static body");
    assert!(taken.is_single());
    assert!(std::ptr::eq(taken.front().as_slice(), bytes));
    assert!(owned::Segments::new().take().is_none());
}

#[test]
fn segments_walk_once() {
    let sources = [b"a".as_slice(), b"bc".as_slice(), b"def".as_slice()];
    let mut body = owned::Segments::new();
    for bytes in sources {
        let result = body.try_push(owned::Segment::from(resident::Shared::from(bytes as &[u8])));
        let next = result.expect("each physical segment must append");
        body = next;
    }

    let mut owned = body.segments();
    assert_eq!(owned.len(), 3);
    assert_eq!(owned.next(), Some(b"a".as_slice()));
    assert_eq!(owned.len(), 2);
    assert_eq!(
        owned.collect::<Vec<_>>(),
        [b"bc".as_slice(), b"def".as_slice()]
    );

    let mut refs = body::Ref::from(&body).segments();
    assert_eq!(refs.len(), 3);
    assert_eq!(refs.next(), Some(b"a".as_slice()));
    assert_eq!(
        refs.collect::<Vec<_>>(),
        [b"bc".as_slice(), b"def".as_slice()]
    );

    let taken = body.take().expect("segmented body");
    assert!(!taken.is_single());
    let shape = taken.into_shape();
    let actual = shape.segments();
    assert_eq!(actual.len(), sources.len());
    assert!(
        actual
            .zip(sources)
            .all(|(actual, expected)| std::ptr::eq(actual, expected))
    );

    let source: &[u8] = b"_ab__cde_";
    let empty = body::Ref::from_slice(&source[1..1]);
    assert_eq!(
        empty.as_slice().map(<[u8]>::as_ptr),
        Some(source[1..].as_ptr())
    );
    assert_eq!(empty.count().into_raw(), 0);
    assert!(empty.segment(0).is_none());
    let ranges = [(1, 3), (5, 8)];
    let result = ranges::Set::try_new(source, &ranges, 5);
    let borrowed = result.expect("valid test ranges must bind");
    let parts = body::Ref::from_ranges(&borrowed)
        .segments()
        .collect::<Vec<_>>();
    assert_eq!(parts, [b"ab".as_slice(), b"cde".as_slice()]);
}

#[test]
fn tracks_backings() {
    let result = storage::Shared::try_copy(b"retained");
    let retained = result.expect("the retained source must allocate");
    let retained = owned::Segments::from(retained);
    assert_eq!(retained.backings().count(), 1);

    let fixed = owned::Segments::from(resident::Shared::from(b"static" as &[u8]));
    assert_eq!(fixed.backings().count(), 0);
}

#[test]
fn push_is_atomic() {
    let body = owned::Segments::from(resident::Shared::from(b"first" as &[u8]));
    let segment = owned::Segment::from(resident::Shared::from(b"second" as &[u8]));

    let result = Tracker::fail_after(0, || body.try_push(segment));
    let rejected = result.expect_err("metadata allocation must fail");
    let ((body, segment), error) = rejected.into_parts();

    assert!(matches!(error, assembly::Error::Metadata(_)));
    assert_eq!(segment.as_slice(), b"second");
    assert!(body.segments().flatten().eq(b"first"));
}

#[test]
fn extend_is_atomic() {
    let body = owned::Segments::from(resident::Shared::from(b"first" as &[u8]));
    let source = owned::Segments::from(resident::Shared::from(b"second" as &[u8]));

    let result = Tracker::fail_after(0, || body.try_extend(source));
    let rejected = result.expect_err("metadata allocation must fail");
    let ((body, source), error) = rejected.into_parts();

    assert!(matches!(error, assembly::Error::Metadata(_)));
    assert!(body.segments().flatten().eq(b"first"));
    assert!(source.segments().flatten().eq(b"second"));
}

#[test]
fn extend_moves() {
    let body = owned::Segments::from(resident::Shared::from(b"first" as &[u8]));
    let source = owned::Segments::from(resident::Shared::from(b"second" as &[u8]));

    let result = body.try_extend(source);
    let body = result.expect("segmented source must append");

    assert!(body.segments().flatten().eq(b"firstsecond"));
}

#[test]
fn extend_orders_segments() {
    let body = owned::Segments::from(resident::Shared::from(b"first" as &[u8]));
    let result = owned::Segments::new().try_push(owned::Segment::from(resident::Shared::from(
        b"second" as &[u8],
    )));
    let source = result.expect("first source segment must retain");
    let result = source.try_push(owned::Segment::from(resident::Shared::from(
        b"third" as &[u8],
    )));
    let source = result.expect("second source segment must retain");

    let result = body.try_extend(source);
    let body = result.expect("segmented source must append");

    assert_eq!(
        body.segments().collect::<Vec<_>>(),
        [
            b"first".as_slice(),
            b"second".as_slice(),
            b"third".as_slice()
        ],
    );
}
