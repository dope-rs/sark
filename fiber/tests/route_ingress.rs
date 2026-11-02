pub mod common;

use dope_test::alloc::Tracker;
use sark_h1::{pump, route::Driver, session::Conn, wire::head};

use crate::common::{
    DATE,
    h1::chunked::{self, CapacityRouter, Request},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn cached_driver_admission() {
    use crate::common::route::skip::{CachedRouter, FULL};

    let router = CachedRouter::new(&());
    assert!(matches!(
        Tracker::fail_after(0, || Driver::try_new(&router, *DATE)),
        Err(o3::alloc::Error::Exhausted)
    ));

    let mut driver = Driver::try_new(&router, *DATE).expect("cached driver");
    let mut conn = Conn::default();
    let mut cold = [0; 256];
    let first = driver.ingest(&mut conn, FULL, &mut cold, head::Connection::KeepAlive);
    let prefix = match first.state {
        Ok(pump::Egress::Write {
            prefix,
            body: pump::Body::None,
        }) => Some(prefix),
        _ => None,
    }
    .expect("cold cached response");
    assert!(cold[..prefix].starts_with(b"HTTP/1.1 200 OK\r\n"));
    assert!(cold[..prefix].ends_with(b"\r\n\r\nok"));
    assert!(matches!(
        pump::Drain::complete(&mut conn),
        pump::Completion::None
    ));

    let mut warm = [0; 256];
    let next = Tracker::fail_after(0, || {
        driver.ingest(&mut conn, FULL, &mut warm, head::Connection::KeepAlive)
    });
    assert!(matches!(
        next.state,
        Ok(pump::Egress::Write {
            prefix: written,
            body: pump::Body::None,
        }) if written == prefix
    ));
    assert_eq!(&warm[..prefix], &cold[..prefix]);
}

#[test]
fn reserves_larger_route() {
    let seen = chunked::State::default();
    let router = CapacityRouter::new(&seen);
    let (driver, allocs) = Tracker::count(|| Driver::try_new(&router, *DATE));
    let mut driver = driver.expect("route driver");
    assert_eq!(allocs, 0);
    let mut conn = Conn::default();
    let mut write = [0; 2048];
    let first = Request::wire(chunked::ONE, 1);
    let next = Request::wire(chunked::MANY, 1024);
    let cycle = driver.ingest(&mut conn, &first, &mut write, head::Connection::KeepAlive);
    assert!(matches!(cycle.state, Ok(pump::Egress::Write { .. })));
    assert_eq!(seen.one.get(), None);

    let (cycle, allocs) =
        Tracker::count(|| driver.ingest(&mut conn, &next, &mut write, head::Connection::KeepAlive));
    assert!(matches!(cycle.state, Ok(pump::Egress::Write { .. })));
    assert_eq!(allocs, 1);
    assert_eq!(seen.many.get(), None);
}

#[test]
fn reserves_page_once() {
    let seen = chunked::State::default();
    let router = CapacityRouter::new(&seen);
    let mut driver = Driver::try_new(&router, *DATE).expect("route driver");
    let mut conn = Conn::default();
    let mut write = [0; 2048];
    let wire = Request::wire(chunked::MANY, 1024);
    let split = wire.len() - 5;
    let cycle = driver.ingest(
        &mut conn,
        &wire[..split],
        &mut write,
        head::Connection::KeepAlive,
    );
    assert!(matches!(cycle.state, Ok(pump::Egress::None)));

    let (cycle, allocs) = Tracker::count(|| {
        driver.ingest(
            &mut conn,
            &wire[split..],
            &mut write,
            head::Connection::KeepAlive,
        )
    });
    assert!(matches!(cycle.state, Ok(pump::Egress::Write { .. })));
    assert_eq!(allocs, 1);
    assert_eq!(seen.many.get(), None);
}

#[test]
fn excludes_chunk_framing() {
    use crate::common::h1::chunked::{Router, WIRE};
    for split in [WIRE.len(), WIRE.len() - 10] {
        let seen = chunked::State::default();
        let router = Router::new(&seen);
        let mut driver = Driver::try_new(&router, *DATE).expect("route driver");
        let mut conn = Conn::default();
        let mut write = [0; 2048];
        let mut cycle = driver.ingest(
            &mut conn,
            &WIRE[..split],
            &mut write,
            head::Connection::KeepAlive,
        );
        if split < WIRE.len() {
            assert!(matches!(cycle.state, Ok(pump::Egress::None)));
            cycle = driver.ingest(
                &mut conn,
                &WIRE[split..],
                &mut write,
                head::Connection::KeepAlive,
            );
        }
        let prefix = match cycle.state {
            Ok(pump::Egress::Write {
                prefix,
                body: pump::Body::None,
            }) => Some(prefix),
            _ => None,
        }
        .expect("inline response");
        assert!(write[..prefix].starts_with(b"HTTP/1.1 200 OK\r\n"));
        assert!(write[..prefix].ends_with(b"\r\n\r\nok"));
        assert!(!seen.invalid.get());
    }
}

#[test]
fn frames_final_body() {
    use sark_h1::{pump, route::Driver, session::Conn, wire::head};

    use crate::common::{
        DATE,
        h1::chunked::{Router, WIRE},
    };

    let seen = chunked::State::default();
    let router = Router::new(&seen);
    let mut driver = Driver::try_new(&router, *DATE).expect("route driver");
    let mut conn = Conn::default();
    let mut write = [0; 2048];
    let split = chunked::SPLIT;
    for part in [&WIRE[..8], &WIRE[8..split]] {
        let first = driver.ingest(&mut conn, part, &mut write, head::Connection::KeepAlive);
        assert!(matches!(first.state, Ok(pump::Egress::None)));
    }
    let last = driver.ingest(
        &mut conn,
        &WIRE[split..],
        &mut write,
        head::Connection::KeepAlive,
    );
    assert!(matches!(last.state, Ok(pump::Egress::Write { .. })));
    assert!(!seen.invalid.get());
}
