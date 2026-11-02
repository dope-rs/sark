pub mod common;

use std::cell::Cell;

use buffer::Residency as _;
use common::guard::*;
use dope_test::alloc::Tracker;
use sark::http::o3::buffer;
use sark_h1::{
    pump::{Body, Egress},
    route::Driver,
    session::Conn,
    wire::head,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn guard_copies_borrowed() {
    let calls = Cell::new(0);
    let state = State::<_, 1> { calls: &calls };
    let router = Router::new(&state);
    let mut driver =
        Driver::try_new(&router, *b"Thu, 01 Jan 1970 00:00:00 GMT").expect("H1 driver");
    let mut conn = Conn::default();
    let req = Request::wire();
    let start = b"GET ".len();
    let end = req[start..]
        .iter()
        .position(|byte| *byte == b' ')
        .map(|offset| start + offset)
        .expect("request target end");
    let path = &req[start..end];
    let mut write = [0_u8; 256];

    let (cycle, alloc) =
        Tracker::count(|| driver.ingest(&mut conn, &req, &mut write, head::Connection::KeepAlive));
    let Egress::Write {
        body: Body::Shared(body),
        ..
    } = cycle
        .state
        .expect("borrowed request head must become one exact shared body")
    else {
        panic!("borrowed request head must become one exact shared body");
    };

    assert_eq!(alloc, 1);
    assert_eq!(body.as_slice(), path);
    assert_eq!(body.resident(), path.len());
    assert_ne!(body.as_slice().as_ptr(), path.as_ptr());
    assert_eq!(state.calls.get(), 0);
}

#[test]
fn guard_moves_buffered() {
    let calls = Cell::new(0);
    let state = State::<_, 1> { calls: &calls };
    let router = Router::new(&state);
    let mut driver =
        Driver::try_new(&router, *b"Thu, 01 Jan 1970 00:00:00 GMT").expect("H1 driver");
    let mut conn = Conn::default();
    let req = Request::wire();
    let mut write = [0_u8; 256];

    let retained = driver.retain(&mut conn, &req);
    assert!(matches!(retained.state, Ok(Egress::Wake)));

    let (cycle, alloc) =
        Tracker::count(|| driver.ingest(&mut conn, &[], &mut write, head::Connection::KeepAlive));
    let Egress::Write {
        body: Body::Shared(body),
        ..
    } = cycle
        .state
        .expect("buffered request head must retain its existing backing")
    else {
        panic!("buffered request head must retain its existing backing");
    };

    assert_eq!(alloc, 0);
    let start = b"GET ".len();
    let end = req[start..]
        .iter()
        .position(|byte| *byte == b' ')
        .map(|offset| start + offset)
        .expect("request target end");
    assert_eq!(body.as_slice(), &req[start..end]);
    assert_eq!(state.calls.get(), 0);
}
