use core::slice;
use std::{convert::Infallible, num::NonZero, pin::Pin};

use dope::core::driver::{self, life::recovery, schedule::ready::task::owner};
use dope_test::scope;
use sark::{
    core::{
        body::{self, Step},
        request::raw::Parsed,
        route,
        route::{TaskBuild, raw::PathCapture},
    },
    http::{
        h1::target::Target,
        o3::buffer::{Complete as _, storage::Shared},
        payload::owned::Segments,
        request,
        request::{Form, Layout},
    },
    request::Head,
};
use sark_h1::{
    Input, pump,
    pump::Egress,
    route::Driver,
    server::deadline,
    session::{self, Conn, Dispatch as _},
};

pub mod common;

use common::router::*;
use dope_test::alloc::Tracker;
use sark::core::body::admit::{Length, Plan, Retention, Stream};
use sark_h1::wire::{head, output};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

fn inline_body(egress: pump::Egress<'_>, want: &[u8], out: &[u8]) -> usize {
    let Egress::Inline { prefix } = egress else {
        panic!("inline response expected");
    };
    assert!(out[..prefix].ends_with(want));
    prefix
}

#[test]
fn borrowed_h1_allocations() {
    let req: &[u8] =
        b"POST /echo HTTP/1.1\r\nHost: origin.example\r\nContent-Length: 13\r\n\r\nborrowed-body";
    let body = &req[req.len() - 13..];
    let state = State {
        body_ptr: body.as_ptr() as usize,
        path_ptr: 0,
    };
    let router = Router::new(&state);
    let mut driver = Driver::try_new(&router, *b"Thu, 01 Jan 1970 00:00:00 GMT")
        .expect("route caches must bind");
    let mut conn = Conn::default();
    let mut write = [0_u8; 4096];

    let (cycle, allocs) =
        Tracker::count(|| driver.ingest(&mut conn, req, &mut write, head::Connection::KeepAlive));

    let egress = cycle
        .state
        .expect("allocation route must produce transport output");
    let used = inline_body(egress, body, &write);
    assert_eq!(allocs, 0);
    assert!(matches!(cycle.progress.head(), deadline::Head::Ready));
    assert!(write[..used].starts_with(b"HTTP/1.1 200"));
}

#[test]
fn chunk_ranges_keep_source() {
    for (copies, frame) in CHUNKED_FRAMES.into_iter().enumerate() {
        let mut bytes = vec![0; 64];
        bytes.extend_from_slice(frame);
        bytes.extend_from_slice(frame);
        let owner = Shared::try_from(&mut bytes).expect("shifted pipeline owner");
        let window = owner
            .view()
            .try_get(64..owner.len())
            .expect("request window");
        for input in [Input::Borrowed(window.as_slice()), Input::Buffered(window)] {
            let body_ptr = if copies == 0 {
                let start = frame
                    .windows(13)
                    .position(|part| part == b"borrowed-body")
                    .expect("contiguous body");
                input.as_slice()[start..].as_ptr() as usize
            } else {
                0
            };
            let state = State {
                body_ptr,
                path_ptr: 0,
            };
            let router = Router::new(&state);
            let mut driver = Driver::try_new(&router, *b"Thu, 01 Jan 1970 00:00:00 GMT")
                .expect("route caches must bind");
            let mut conn = Conn::default();
            let mut write = [0_u8; 4096];
            for warm in [false, true] {
                let (outcome, allocs) = Tracker::count(|| {
                    Pin::new(&mut &mut driver).try_consume(
                        session::Permit::new(),
                        input,
                        &mut write,
                        &mut conn,
                    )
                });
                let written = match outcome {
                    session::RouteOutcome::Outcome(session::Outcome::Complete {
                        response:
                            output::Complete::Inline { written }
                            | output::Complete::Copy { written, .. },
                        ..
                    }) => Some(written),
                    _ => None,
                }
                .expect("completed chunk response");
                assert!(write[..written].starts_with(b"HTTP/1.1 200"));
                assert!(write[..written].ends_with(b"borrowed-body"));
                if warm {
                    assert_eq!(allocs, copies);
                }
                conn.retire();
            }
        }
        let state = State {
            body_ptr: 0,
            path_ptr: 0,
        };
        let router = Router::new(&state);
        let mut driver = Driver::try_new(&router, *b"Thu, 01 Jan 1970 00:00:00 GMT")
            .expect("route caches must bind");
        let mut conn = Conn::default();
        let mut write = [0_u8; 4096];
        let cycle = driver.ingest(
            &mut conn,
            window.as_slice(),
            &mut write,
            head::Connection::KeepAlive,
        );
        let used = inline_body(
            cycle.state.expect("chunk pipeline"),
            b"borrowed-body",
            &write,
        );
        assert_eq!(
            write[..used]
                .windows(b"HTTP/1.1 200".len())
                .filter(|part| *part == b"HTTP/1.1 200")
                .count(),
            2
        );
    }
}

fn drip_allocations(req: &'static [u8], head: usize) -> usize {
    let state = State {
        body_ptr: 0,
        path_ptr: 0,
    };
    let router = Router::new(&state);
    let mut driver = Driver::try_new(&router, *b"Thu, 01 Jan 1970 00:00:00 GMT")
        .expect("route caches must bind");
    let mut conn = Conn::default();
    let mut write = [0_u8; 4096];

    let warm = driver.ingest(&mut conn, req, &mut write, head::Connection::KeepAlive);
    assert!(matches!(warm.state, Ok(Egress::Inline { .. })));

    let (done, allocs) = Tracker::count(|| {
        let prefix = driver.ingest(
            &mut conn,
            &req[..head],
            &mut write,
            head::Connection::KeepAlive,
        );
        assert!(matches!(prefix.state, Ok(Egress::None)));
        let mut done = false;
        for byte in &req[head..] {
            let cycle = driver.ingest(
                &mut conn,
                slice::from_ref(byte),
                &mut write,
                head::Connection::KeepAlive,
            );
            done = matches!(cycle.state, Ok(Egress::Inline { .. }));
        }
        done
    });
    assert!(done);
    allocs
}

#[test]
fn fragmented_allocations() {
    const FIXED: &[u8] =
        b"POST /echo HTTP/1.1\r\nHost: local\r\nContent-Length: 13\r\n\r\nborrowed-body";
    const CHUNKED: &[u8] = b"POST /echo HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\n\r\nd\r\nborrowed-body\r\n0\r\n\r\n";
    let fhead = FIXED
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("fixed request head terminator")
        + 4;
    let chead = CHUNKED
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("chunked request head terminator")
        + 4;

    let fixed = drip_allocations(FIXED, fhead);
    let chunks = drip_allocations(CHUNKED, chead);
    assert!(fixed <= 2, "fixed allocated {fixed}");
    assert!(chunks <= 2, "chunked allocated {chunks}");
}

#[test]
fn absolute_form_allocates() {
    let req: &[u8] = b"POST http://origin.example/echo/item-42?q=1 HTTP/1.1\r\nHost: ignored.example\r\nContent-Length: 13\r\n\r\nborrowed-body";
    let body = &req[req.len() - 13..];
    let off = req
        .windows(b"item-42".len())
        .position(|window| window == b"item-42")
        .expect("parameter is present in the request target");
    let state = State {
        body_ptr: body.as_ptr() as usize,
        path_ptr: req.as_ptr().wrapping_add(off) as usize,
    };
    let router = Router::new(&state);
    let mut driver = Driver::try_new(&router, *b"Thu, 01 Jan 1970 00:00:00 GMT")
        .expect("route caches must bind");
    let mut conn = Conn::default();
    let mut write = [0_u8; 4096];

    let (cycle, allocs) =
        Tracker::count(|| driver.ingest(&mut conn, req, &mut write, head::Connection::KeepAlive));
    let used = inline_body(
        cycle.state.expect("borrowed route must be accepted"),
        body,
        &write,
    );

    assert_eq!(allocs, 0);
    assert!(write[..used].starts_with(b"HTTP/1.1 200"));
}

#[test]
fn reordered_path_allocates() {
    let req: &[u8] = b"POST /users/beta/teams/alpha HTTP/1.1\r\nHost: origin.example\r\nContent-Length: 13\r\n\r\nborrowed-body";
    let body = &req[req.len() - 13..];
    let off = req
        .windows(b"beta".len())
        .position(|window| window == b"beta")
        .expect("user parameter");
    let state = State {
        body_ptr: body.as_ptr() as usize,
        path_ptr: req.as_ptr().wrapping_add(off) as usize,
    };
    let router = Router::new(&state);
    let mut driver = Driver::try_new(&router, *b"Thu, 01 Jan 1970 00:00:00 GMT")
        .expect("route caches must bind");
    let mut conn = Conn::default();
    let mut write = [0_u8; 4096];

    let (cycle, allocs) =
        Tracker::count(|| driver.ingest(&mut conn, req, &mut write, head::Connection::KeepAlive));
    let used = inline_body(
        cycle.state.expect("reordered route must be accepted"),
        body,
        &write,
    );
    assert_eq!(allocs, 0);
    assert!(write[..used].starts_with(b"HTTP/1.1 200"));
}

#[test]
fn named_path_survives_body() {
    use std::cell::Cell;

    use common::sync::{NAMED_PATH_FRAGMENTS, Router, State};

    let state = State {
        request_body: Cell::new(0),
        request_head: Cell::new(0),
        invalid_path: Cell::new(false),
        header_error: Cell::new(None),
        dispatches: Cell::new(0),
        large_body: Shared::new(),
    };
    let router = Router::new(&state);
    let mut driver = Driver::try_new(&router, *b"Thu, 01 Jan 1970 00:00:00 GMT")
        .expect("route caches must bind");
    let mut conn = Conn::default();
    let mut write = [0_u8; 4096];
    let [line, headers, body, end] = NAMED_PATH_FRAGMENTS;

    for fragment in [line, headers, body] {
        let cycle = driver.ingest(&mut conn, fragment, &mut write, head::Connection::KeepAlive);
        assert!(matches!(cycle.state, Ok(Egress::None)));
        assert_eq!(state.request_head.get(), 0);
    }

    let cycle = driver.ingest(&mut conn, end, &mut write, head::Connection::KeepAlive);
    let used = inline_body(
        cycle.state.expect("fragmented named path response"),
        b"beta",
        &write,
    );
    assert!(write[..used].starts_with(b"HTTP/1.1 200"));
    assert_ne!(state.request_head.get(), 0);
    assert!(
        !state.invalid_path.get(),
        "named captures must bind by name"
    );
}

#[test]
fn retained_metadata() {
    let wire = b"POST http://origin.example/echo/item-42?q=1 HTTP/1.1\r\nHost: ignored.example\r\nContent-Length: 13\r\n\r\nborrowed-body";
    let hlen = wire
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|offset| offset + 4)
        .expect("complete request head");
    let start = b"POST ".len();
    let end = wire[start..]
        .iter()
        .position(|byte| *byte == b' ')
        .map(|offset| start + offset)
        .expect("complete request target");
    let target = Target::parse(b"POST", &wire[start..end])
        .and_then(|plan| plan.shifted(start))
        .expect("valid absolute-form target");
    let auth = target.authority().expect("target authority");
    let path = target.path_span();
    let owner = Shared::try_copy(wire).expect("wire owner allocation");
    let ptr = owner.as_slice().as_ptr();
    let target =
        request::Target::from_span(Form::Path, path).expect("representable request target");
    let layout = Layout::new(target, Some(auth), Some(path));
    let mut reports = Vec::new();
    scope::Driver::new(Default::default()).with_driver(
        |driver| {
            let binding = driver::Binding::new(
                driver.driver_ref().timer(),
                owner::Capacity::new(NonZero::<u32>::MIN),
            );
            let ids = request::Seq::new(binding.sequence());
            let blen = wire.len() - hlen;
            let track = Plan::new(
                Retention::Retained,
                blen,
                Length::Known(blen),
                Stream::Buffered,
            )
            .try_admit(blen, blen)
            .expect("test body admission");
            let request = owner;
            let tail = request
                .try_clone()
                .expect("body owner reference")
                .project(hlen..wire.len())
                .expect("body range");
            let head = Head::bind(request.project(0..hlen).expect("head range"), layout)
                .expect("request layout");
            let body = body::Body::try_new(track, Segments::from(tail)).expect("body admission");
            // SAFETY: this head retains the validated wire; ParamReq has no headers,
            // and its only capture is `item-42` at 6..13 within `/echo/item-42`.
            let parsed = unsafe {
                Parsed::<_, ParamReq>::new(
                    head,
                    <ParamReqParamsRaw as route::Params>::from_match((PathCapture::new(6, 13),)),
                    ParamReqHeadersRaw::default(),
                )
            };
            let req = parsed
                .with_id(ids.try_next().expect("request id"))
                .with_body(body);
            let build =
                <ParamReq as route::Own>::start_task(req).expect("retained request builder");
            let step = TaskBuild::step(build, NonZero::<usize>::MAX);
            let ready = if let Step::Ready { output, released } = step {
                Some((output, released))
            } else {
                None
            }
            .expect("non-JSON request must be immediately ready");
            assert!(ready.1.is_empty());
            let req = ready.0.into_parts().1;

            assert_eq!(req.id.as_slice(), b"item-42");
            assert_eq!(req.payload.as_slice(), b"borrowed-body");
            assert_eq!(
                req.id.as_slice().as_ptr(),
                ptr.wrapping_add(path.start() + 6)
            );
            assert_eq!(req.payload.as_slice().as_ptr(), ptr.wrapping_add(hlen));
        },
        recovery::Policy::new(
            |event| reports.push(event),
            |failure: recovery::Recovery<'_, '_>| -> Infallible {
                panic!("native recovery: {:?}", failure.cause())
            },
        ),
        |result| {
            result
                .as_ref()
                .map_err(scope::Error::cause)
                .expect("identity driver scope");
        },
    );
    assert!(
        reports.is_empty(),
        "unexpected identity-scope I/O: {reports:?}"
    );
}

fn cold_status(wire: &[u8], split: usize) -> u16 {
    let state = State {
        body_ptr: 0,
        path_ptr: 0,
    };
    let router = Router::new(&state);
    let mut driver = Driver::try_new(&router, *b"Thu, 01 Jan 1970 00:00:00 GMT")
        .expect("route caches must bind");
    let mut conn = Conn::default();
    let mut write = [0_u8; 4096];

    let mut status = None;
    for chunk in [&wire[..split], &wire[split..]] {
        let cycle = driver.ingest(&mut conn, chunk, &mut write, head::Connection::KeepAlive);
        if let Ok(Egress::Static { prefix: 0, body }) = cycle.state {
            let wire = body.as_slice();
            status = Some(
                if wire.starts_with(b"HTTP/1.1 400") {
                    Some(400)
                } else if wire.starts_with(b"HTTP/1.1 404") {
                    Some(404)
                } else {
                    None
                }
                .expect("cold route must emit a known error response"),
            );
            break;
        }
    }
    status.expect("cold route must commit a response")
}

#[test]
fn protocol_precedes_route() {
    let miss = b"GET /missing HTTP/1.1\r\n\r\n";
    let dup = b"GET /missing HTTP/1.1\r\nHost: one.example\r\nHost: two.example\r\n\r\n";
    let valid = b"GET /missing HTTP/1.1\r\nHost: origin.example\r\n\r\n";
    let bad = b"PATCH /echo HTTP/1.1\r\n\r\n";
    let good = b"PATCH /echo HTTP/1.1\r\nHost: origin.example\r\n\r\n";

    for split in 0..=miss.len() {
        assert_eq!(cold_status(miss, split), 400, "missing Host split {split}");
    }
    for split in 0..=dup.len() {
        assert_eq!(cold_status(dup, split), 400, "duplicate Host split {split}");
    }
    for split in 0..=valid.len() {
        assert_eq!(cold_status(valid, split), 404, "valid split {split}");
    }
    for split in 0..=bad.len() {
        assert_eq!(
            cold_status(bad, split),
            400,
            "unknown method missing Host split {split}"
        );
    }
    for split in 0..=good.len() {
        assert_eq!(
            cold_status(good, split),
            404,
            "unknown method valid split {split}"
        );
    }
}
