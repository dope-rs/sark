use std::ops::ControlFlow;

use dope_test::alloc::Tracker;
use o3::{
    alloc,
    buffer::{hold::ranges, storage, write::Slice},
};
use sark_http::{
    owner::handoff,
    payload::{owned, response::Body as _},
    request::body,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn inline_handoff_preserves_copy_leaves() {
    let source = *b"abcde";
    let ranges = ranges::Set::try_new(source.as_slice(), &[(0, 5)], 5).expect("range");
    let slot = handoff::Slot::empty();
    let mut out = [0; 5];
    Tracker::fail_after(0, || {
        for result in [
            source
                .as_slice()
                .try_resolve_into(&slot, &mut Slice::new(&mut out)),
            std::str::from_utf8(&source)
                .expect("utf8")
                .try_resolve_into(&slot, &mut Slice::new(&mut out)),
            body::Ref::from_slice(&source).try_resolve_into(&slot, &mut Slice::new(&mut out)),
            body::Ref::from_ranges(&ranges).try_resolve_into(&slot, &mut Slice::new(&mut out)),
        ] {
            assert!(matches!(result, Ok(ControlFlow::Break(5))));
        }
        assert_eq!(out, source);
        assert!(matches!(
            source
                .as_slice()
                .try_resolve_into(&slot, &mut Slice::new(&mut out[..4])),
            Err(owned::Error::Storage(storage::AllocError::Exhausted(
                alloc::Exhausted
            )))
        ));
        assert!(matches!(
            Tracker::fail_after(0, || body::Ref::from_slice(&source)
                .try_resolve_into(&slot, &mut Slice::new(&mut out[..4]))),
            Err(owned::Error::Build(storage::BuildError::Exhausted(
                alloc::Exhausted
            )))
        ));
        assert!(matches!(
            body::Ref::discarded(5).try_resolve_into(&slot, &mut Slice::new(&mut out)),
            Err(owned::Error::Discarded)
        ));
        assert!(matches!(
            body::Ref::discarded(0).try_resolve_into(&slot, &mut Slice::new(&mut out)),
            Ok(ControlFlow::Continue(_))
        ));
    });
}

#[test]
fn inline_handoff_keeps_existing_owners() {
    let owner = storage::Shared::try_copy(b"abcde").expect("owner");
    let retained = owned::Segments::from(owner);
    let view = body::Ref::from(&retained);
    let slot = handoff::Slot::new(view);
    let mut out = [0; 5];
    let ((), counts) = Tracker::counts(|| {
        for result in [
            view.try_resolve_into(&slot, &mut Slice::new(&mut out)),
            view.as_slice()
                .expect("contiguous")
                .try_resolve_into(&slot, &mut Slice::new(&mut out)),
        ] {
            let Ok(ControlFlow::Continue(plan)) = result else {
                panic!("retained placement");
            };
            let handoff::Ready::Linked(body) = plan.finish().expect("retained owner") else {
                panic!("linked owner");
            };
            assert_eq!(
                body.as_slice().expect("contiguous").as_ptr(),
                view.as_slice().expect("contiguous").as_ptr()
            );
        }
    });
    assert_eq!(counts, [0, 0]);
    assert_eq!(out, [0; 5]);
    let source = Vec::from(b"abcde".as_slice());
    let pointer = source.as_ptr();
    let Ok(ControlFlow::Continue(plan)) = source.try_resolve_into(&slot, &mut Slice::new(&mut out))
    else {
        panic!("owned placement");
    };
    let handoff::Ready::Owned(body) = plan.finish().expect("owned body") else {
        panic!("owned body");
    };
    assert_eq!(body.as_ptr(), pointer);
}

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
