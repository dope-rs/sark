pub mod common;

use std::{num::NonZero, ops::ControlFlow};

use common::head::{self, budget};
use dope_test::alloc::Tracker;
use o3::buffer::{
    bytes::window::Snapshot,
    storage::AllocError,
    write::{Sink as _, consume},
};
use sark_http::{
    h1::{
        field::scan::{Progress, State},
        head::{
            Boundary, Limit, ParseError,
            line::{self, Request},
            request::scan,
        },
    },
    method::Known,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn retains_line_and_fields() {
    let input = b"GET /items?q=1 HTTP/1.1\r\nHost: example.test\r\n";
    let request = Request::parse(input).expect("line").expect("complete");
    let (source, allocs) = Tracker::count(|| request.try_snapshot::<256>(NonZero::<usize>::MIN));
    let source = source.expect("owner");
    assert_eq!(allocs, 1);
    let state = State::try_new(source, request.end(), 0, 2, 64)
        .ok()
        .expect("fields");
    let mut fields = 0;
    let progress = state
        .advance(|_| {
            fields += 1;
            Ok::<_, ()>(())
        })
        .ok()
        .expect("prefix");
    let Progress::More(mut state) = progress else {
        assert!(matches!(progress, Progress::More(_)));
        return;
    };
    state.try_extend(b"\r\nBODY").expect("append");
    let progress = state
        .advance(|_| {
            fields += 1;
            Ok::<_, ()>(())
        })
        .ok()
        .expect("suffix");
    let Progress::Complete(complete) = progress else {
        assert!(matches!(progress, Progress::Complete(_)));
        return;
    };
    let (source, _, end) = complete.into_parts();
    source.with_shared(|view, retained| {
        assert_eq!(retained.method(), request.method());
        assert_eq!(retained.target(), request.target());
        assert_eq!(retained.path(), Some(b"/items".as_slice()));
        assert_eq!(retained.query(), Some(b"q=1".as_slice()));
        assert_eq!(&view.as_slice()[end.get() as usize..], b"BODY");
    });
    assert_eq!(fields, 1);
}

#[test]
fn continues_borrowed_line() {
    let input = budget::REQUEST;
    let line = Request::parse_with(input, Known::Post.bit())
        .expect("line")
        .expect("complete");
    let end = line.end();
    let error = scan::State::try_request(line, Limit::DEFAULT.with_bytes(end - 1))
        .err()
        .expect("line limit");
    assert_eq!(
        error,
        ParseError::Boundary(Boundary::TooLarge {
            actual: end,
            limit: end - 1,
        }),
    );
    let state = scan::State::try_request(line, Limit::DEFAULT).expect("fields");
    let mut fields = 0;
    let progress = state
        .advance(|_| {
            fields += 1;
            Ok::<_, ()>(())
        })
        .expect("head");
    let scan::Progress::Complete(head) = progress else {
        assert!(matches!(progress, scan::Progress::Complete(_)));
        return;
    };
    assert_eq!(head.as_ref().bytes().as_ptr(), input.as_ptr());
    assert_eq!(fields, 2);
}

#[test]
fn retains_scanned_fields() {
    let input = &budget::REQUEST[..budget::REQUEST.len() - 2];
    let request = Request::parse(input).expect("line").expect("complete");
    let state = scan::State::try_request(request, Limit::DEFAULT).expect("fields");
    let mut fields = 0;
    let progress = state
        .advance(|_| {
            fields += 1;
            Ok::<_, ()>(())
        })
        .expect("prefix");
    let state = match progress {
        scan::Progress::More(state) => Some(state),
        scan::Progress::Complete(_) => None,
    }
    .expect("partial");
    let (snapshot, allocs) = Tracker::count(|| state.try_snapshot::<256>(NonZero::<usize>::MIN));
    let mut snapshot = snapshot.expect("owner");
    assert_eq!(allocs, 1);
    snapshot.try_extend(b"\r\n").expect("suffix");
    let progress = snapshot
        .advance(|_| {
            fields += 1;
            Ok::<_, ()>(())
        })
        .ok()
        .expect("head");
    let head = match progress {
        scan::Progress::Complete(head) => Some(head),
        scan::Progress::More(_) => None,
    }
    .expect("complete");
    assert_eq!(head.as_ref().bytes(), budget::REQUEST);
    assert_eq!(fields, 2);
}

#[test]
fn snapshot_rejects_before_allocation() {
    let request = Request::parse(budget::REQUEST)
        .expect("line")
        .expect("complete");
    let state = scan::State::try_request(request, Limit::DEFAULT).expect("fields");
    let (errors, allocs) = Tracker::count(|| {
        [
            request.try_snapshot::<8>(NonZero::<usize>::MIN).err(),
            state.try_snapshot::<8>(NonZero::<usize>::MIN).err(),
        ]
    });
    assert_eq!(allocs, 0);
    for error in errors {
        assert_eq!(
            error,
            Some(AllocError::Capacity(consume::Error::new(
                NonZero::new(budget::REQUEST.len()).expect("request"),
                8,
            ))),
        );
    }
}

#[test]
fn continues_owned_line() {
    let input = budget::REQUEST;
    for (methods, matched) in [
        (Known::Get.bit(), Some(Known::Get)),
        (Known::Post.bit(), None),
    ] {
        let mut source = Snapshot::<256>::new();
        source.write_slices([input, input]).expect("input");
        let pointer = source.as_ref().as_ptr();
        let source = match line::Owned::try_parse(source, methods).ok().expect("line") {
            ControlFlow::Continue(source) => Some(source),
            ControlFlow::Break(_) => None,
        }
        .expect("complete");
        assert!(source.request().matched() == matched);
        assert_eq!(source.request().method().as_ptr(), pointer);
        let end = source.request().end();
        let rejected = scan::State::try_line(source, Limit::DEFAULT.with_bytes(end - 1))
            .err()
            .expect("line limit");
        let (source, error) = rejected.into_parts();
        assert_eq!(
            error,
            ParseError::Boundary(Boundary::TooLarge {
                actual: end,
                limit: end - 1,
            }),
        );
        let state = scan::State::try_line(source, Limit::DEFAULT.with_bytes(input.len()))
            .ok()
            .expect("fields");
        let mut fields = 0;
        let progress = state
            .advance(|_| {
                fields += 1;
                Ok::<_, ()>(())
            })
            .ok()
            .expect("head");
        let scan::Progress::Complete(head) = progress else {
            assert!(matches!(progress, scan::Progress::Complete(_)));
            return;
        };
        let route = head.into_route();
        assert_eq!(route.as_ref().bytes().as_ptr(), pointer);
        assert_eq!(route.as_ref().body(), input);
        assert_eq!(fields, 2);
    }
}

#[test]
fn continues_owned_fields() {
    let input = budget::REQUEST;
    let mut source =
        Snapshot::<256>::try_with(NonZero::new(128).expect("capacity")).expect("owner");
    source
        .write_slice(&input[..input.len() - 2])
        .expect("prefix");
    let pointer = source.as_ref().as_ptr();
    let source = match line::Owned::try_parse(source, Known::ALL)
        .ok()
        .expect("line")
    {
        ControlFlow::Continue(source) => Some(source),
        ControlFlow::Break(_) => None,
    }
    .expect("complete");
    let state = scan::State::try_line(source, Limit::DEFAULT)
        .ok()
        .expect("fields");
    let progress = state.advance(|_| Ok::<_, ()>(())).ok().expect("prefix");
    let scan::Progress::More(mut state) = progress else {
        assert!(matches!(progress, scan::Progress::More(_)));
        return;
    };
    state.try_extend(&input[input.len() - 2..]).expect("suffix");
    let progress = state.advance(|_| Ok::<_, ()>(())).ok().expect("head");
    let scan::Progress::Complete(head) = progress else {
        assert!(matches!(progress, scan::Progress::Complete(_)));
        return;
    };
    assert_eq!(head.as_ref().bytes(), input);
    assert_eq!(head.as_ref().bytes().as_ptr(), pointer);
}

#[test]
fn returns_unparsed_owner() {
    let input = head::ESCAPED_REQUEST_LINE;
    let mut source = Snapshot::<256>::new();
    source
        .write_slices([&input[..input.len() - 4], &input[input.len() - 2..]])
        .expect("input");
    let pointer = source.as_ref().as_ptr();
    assert!(matches!(Request::parse(source.as_ref()), Ok(None)));
    let source = match line::Owned::try_parse(source, Known::ALL)
        .ok()
        .expect("partial")
    {
        ControlFlow::Break(source) => Some(source),
        ControlFlow::Continue(_) => None,
    }
    .expect("partial");
    assert_eq!(source.as_ref().as_ptr(), pointer);

    let mut source = Snapshot::<256>::new();
    source.write_slice(head::CONNECT_ORIGIN).expect("input");
    let pointer = source.as_ref().as_ptr();
    let expected = Request::parse(source.as_ref()).err().expect("target");
    let rejected = line::Owned::try_parse(source, Known::ALL)
        .err()
        .expect("target");
    let (source, error) = rejected.into_parts();
    assert_eq!(source.as_ref().as_ptr(), pointer);
    assert_eq!(source.as_ref(), head::CONNECT_ORIGIN);
    assert_eq!(error, expected);
}
