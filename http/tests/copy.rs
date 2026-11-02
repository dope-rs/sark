use dope_test::alloc::Tracker;
use o3::{
    alloc,
    buffer::{hold::ranges, storage},
};
use sark_http::{
    payload::{owned, response::Body as _},
    request::body,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn copies_each_view() {
    let source = b"_ab__cde_";
    let ranges = ranges::Set::try_new(source.as_slice(), &[(1, 3), (5, 8)], 5);
    let ranges = (ranges).expect("the two ranges must describe five source bytes");
    let bytes = (storage::Shared::try_copy(b"abcde")).expect("the retained source must allocate");
    let retained = owned::Segments::from(bytes);
    let views = [
        body::Ref::from_slice(b"abcde"),
        body::Ref::from_ranges(&ranges),
        body::Ref::from(&retained),
    ];
    for view in views {
        let ((), counts) = Tracker::counts(|| {
            let mut segments = view.segments();
            assert_eq!(segments.len(), view.count().into_raw() as usize);
            while segments.next().is_some() {}
            assert_eq!(segments.len(), 0);
            assert!(segments.next().is_none());
            let copy =
                (view.try_copy()).expect("every body representation must copy its logical bytes");
            assert_eq!(copy.as_ref(), b"abcde");
        });
        assert_eq!(counts, [1, 1]);
    }
    assert_eq!(retained.segments().next(), Some(b"abcde".as_slice()));
}

#[test]
fn empty_and_discarded() {
    let ((), counts) = Tracker::counts(|| {
        Tracker::fail_after(0, || {
            let empty = owned::Segments::new();
            for view in [body::Ref::from_slice(&[]), body::Ref::from(&empty)] {
                let mut segments = view.segments();
                assert_eq!(segments.len(), 0);
                assert!(segments.next().is_none());
            }
            let copy = (body::Ref::from_slice(&[]).try_copy())
                .expect("an empty body must copy without allocation");
            assert!(copy.is_empty());
            for len in [0, 5] {
                assert!(matches!(
                    body::Ref::discarded(len).try_copy(),
                    Err(owned::Error::Discarded)
                ));
            }
        });
    });
    assert_eq!(counts, [0, 0]);
}

#[test]
fn exhaustion_keeps_source() {
    let source = [1, 2, 3];
    let view = body::Ref::from_slice(&source);
    let (result, counts) = Tracker::counts(|| Tracker::fail_after(0, || view.try_copy()));
    assert!(matches!(
        result,
        Err(owned::Error::Build(storage::BuildError::Exhausted(
            alloc::Exhausted
        )))
    ));
    assert_eq!(view.as_slice(), Some(source.as_slice()));
    assert_eq!(counts, [0, 0]);
}
