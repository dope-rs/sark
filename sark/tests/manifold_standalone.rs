use std::mem;

use sark::http::h1::codec::canned;
use sark_h1::{
    pump,
    pump::{Body, Egress},
    server::deadline::Head,
    session::Conn,
};

pub mod common;

use std::marker::PhantomData;

use common::stand::*;
use dope_test::alloc::Tracker;
use sark_h1::{route::Driver, wire::head};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[track_caller]
fn assert_reply(cycle: &pump::Cycle<'_>, write: &[u8], body: &[u8]) {
    use sark_h1::pump::{Body, Egress};

    assert!(
        matches!(
            &cycle.state,
            Ok(Egress::Write {
                body: Body::None,
                ..
            })
        ),
        "request must produce an inline reply"
    );
    let Ok(Egress::Write {
        prefix,
        body: Body::None,
    }) = &cycle.state
    else {
        return;
    };
    assert!(*prefix <= write.len());
    let reply = &write[..*prefix];
    assert!(reply.starts_with(b"HTTP/1.1 200 OK\r\n"));
    assert!(
        reply.ends_with(body),
        "expected {body:?}, received {reply:?}"
    );
}

fn shorten_router_lifetime<'short>(
    proof: PhantomData<Router<'static, 'static, 'static>>,
    scope: &'short (),
) -> PhantomData<Router<'short, 'static, 'static>> {
    let () = *scope;
    proof
}

#[test]
fn router_state_covariant() {
    let proof = shorten_router_lifetime(PhantomData, &());
    assert_eq!(mem::size_of_val(&proof), 0);
}

#[test]
fn router_drives_borrow() {
    const REQUEST: &[u8] =
        b"POST /echo HTTP/1.1\r\nHost: local\r\nContent-Length: 13\r\n\r\nborrowed-body";
    let body = &REQUEST[REQUEST.len() - 13..];
    let state = State::new(body, true);
    {
        let router = Router::new(&state);
        let mut driver = Driver::try_new(&router, *b"Thu, 01 Jan 1970 00:00:00 GMT")
            .expect("route caches must bind");
        let mut conn = Conn::default();
        let mut write = [0_u8; 4096];

        let cycle = driver.ingest(&mut conn, REQUEST, &mut write, head::Connection::KeepAlive);

        assert_reply(&cycle, &write, body);
        assert!(cycle.progress.head() == Head::Ready);
        assert!(matches!(cycle.connection, head::Connection::KeepAlive));
    }
}

#[test]
fn router_uses_param_trie() {
    let state = State::new(b"", false);
    let router = Router::new(&state);
    let mut driver = Driver::try_new(&router, *b"Thu, 01 Jan 1970 00:00:00 GMT")
        .expect("route caches must bind");
    let mut write = [0_u8; 4096];
    for (path, status, body) in [
        ("/", "200", ""),
        ("/?q=1", "200", ""),
        ("/items/42", "200", "42"),
        ("/items/", "200", ""),
        ("/items/?q=1", "200", ""),
        ("/items//42", "404", ""),
        ("/items/42/", "404", ""),
        ("/items/%2F?q=1", "200", "%2F"),
        ("/items/new", "200", ""),
        ("/items/next", "200", ""),
        ("/items/ne", "200", "ne"),
        ("/items/newer", "200", "newer"),
    ] {
        let request = format!("GET {path} HTTP/1.1\r\nHost: local\r\n\r\n");
        let mut conn = Conn::default();
        let cycle = driver.ingest(
            &mut conn,
            request.as_bytes(),
            &mut write,
            head::Connection::KeepAlive,
        );
        let response = match cycle.state {
            Ok(Egress::Write {
                prefix,
                body: Body::None,
            }) => Some(&write[..prefix]),
            Ok(Egress::Write {
                prefix: 0,
                body: Body::Static(body),
            }) => Some(body.as_slice()),
            _ => None,
        }
        .expect("router response must be complete");
        assert_eq!(response.get(9..12), Some(status.as_bytes()), "{path}");
        let start = response
            .windows(4)
            .position(|bytes| bytes == b"\r\n\r\n")
            .expect("response headers must finish")
            + 4;
        assert_eq!(&response[start..], body.as_bytes(), "{path}");
    }
}

#[test]
fn chunked_finishes_once() {
    let want = b"chunked-body";
    let state = State::new(want, false);
    {
        let router = Router::new(&state);
        let mut driver = Driver::try_new(&router, *b"Thu, 01 Jan 1970 00:00:00 GMT")
            .expect("route caches must bind");
        let mut conn = Conn::default();
        let mut write = [0_u8; 4096];

        let first = driver.ingest(
            &mut conn,
            b"POST /echo HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\n\r\n7\r\nchu",
            &mut write,
            head::Connection::KeepAlive,
        );
        assert!(matches!(first.state, Ok(Egress::None)));

        let middle = driver.ingest(
            &mut conn,
            b"nked\r\n5\r\n-bo",
            &mut write,
            head::Connection::KeepAlive,
        );
        assert!(matches!(middle.state, Ok(Egress::None)));

        let final_ = driver.ingest(
            &mut conn,
            b"dy\r\n0\r\n\r\n",
            &mut write,
            head::Connection::KeepAlive,
        );
        assert_reply(&final_, &write, want);
    }
}

#[test]
fn expect_continue_once() {
    for (head, body) in [
        (
            b"POST /echo HTTP/1.1\r\nHost: local\r\nContent-Length: 4\r\nExpect: 100-continue\r\n\r\n"
                .as_slice(),
            b"body".as_slice(),
        ),
        (
            b"POST /echo HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\nExpect: 100-continue\r\n\r\n",
            b"4\r\nbody\r\n0\r\n\r\n",
        ),
    ] {
        let state = State::new(b"body", false);
        {
let router = Router::new(&state);
let mut driver = Driver::try_new(&router, *b"Thu, 01 Jan 1970 00:00:00 GMT").expect("route caches must bind");
        let mut conn = Conn::default();
        let mut write = [0_u8; 4096];

        let first = driver.ingest(
            &mut conn,
            head,
            &mut write,
            head::Connection::KeepAlive,
        );
        assert!(matches!(first.state, Ok(Egress::Write {
                prefix: 0,
                body: Body::Static(value),
            }) if value == sark_h1::CONTINUE_100));

        let final_ = driver.ingest(
            &mut conn,
            body,
            &mut write,
            head::Connection::KeepAlive,
        );
        assert_reply(&final_, &write, b"body");
            }
}

    for req in [
        b"POST /echo HTTP/1.1\r\nHost: local\r\nContent-Length: 4\r\nExpect: 100-continue\r\n\r\nbody"
            .as_slice(),
        b"POST /echo HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\nExpect: 100-continue\r\n\r\n4\r\nbody\r\n0\r\n\r\n",
    ] {
        let state = State::new(b"body", false);
        {
let router = Router::new(&state);
let mut driver = Driver::try_new(&router, *b"Thu, 01 Jan 1970 00:00:00 GMT").expect("route caches must bind");
        let mut conn = Conn::default();
        let mut write = [0_u8; 4096];
        let reply = driver.ingest(
            &mut conn,
            req,
            &mut write,
            head::Connection::KeepAlive,
        );
        assert_reply(&reply, &write, b"body");
            }
}

    let state = State::new(b"body", false);
    {
        let router = Router::new(&state);
        let mut driver = Driver::try_new(&router, *b"Thu, 01 Jan 1970 00:00:00 GMT")
            .expect("route caches must bind");
        let mut conn = Conn::default();
        let mut write = [0_u8; 4096];
        let bad = driver.ingest(
            &mut conn,
            b"POST /echo HTTP/1.1\r\nHost: local\r\nContent-Length: 4\r\nExpect: fancy\r\n\r\n",
            &mut write,
            head::Connection::KeepAlive,
        );
        assert!(matches!(bad.state, Ok(Egress::Write {
                prefix: 0,
                body: Body::Static(value),
            }) if value == canned::BAD_REQUEST));
        assert!(matches!(bad.connection, head::Connection::Close));
    }
}

fn assert_all_splits(req: &[u8], want: &'static [u8]) {
    for split in 1..req.len() {
        let state = State::new(want, false);
        {
            let router = Router::new(&state);
            let mut driver = Driver::try_new(&router, *b"Thu, 01 Jan 1970 00:00:00 GMT")
                .expect("route caches must bind");
            let mut conn = Conn::default();
            let mut write = [0_u8; 4096];

            let first = driver.ingest(
                &mut conn,
                &req[..split],
                &mut write,
                head::Connection::KeepAlive,
            );
            assert!(
                matches!(first.state, Ok(Egress::None)),
                "split {split} emitted before the terminal request byte"
            );
            let second = driver.ingest(
                &mut conn,
                &req[split..],
                &mut write,
                head::Connection::KeepAlive,
            );
            assert_reply(&second, &write, want);
        }
    }
}

#[test]
fn framing_split_invariant() {
    const FIXED: &[u8] =
        b"POST /echo HTTP/1.1\r\nHost: local\r\nContent-Length: 13\r\n\r\nborrowed-body";
    const CHUNKED: &[u8] = b"POST /echo HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\n\r\n7\r\nchunked\r\n5\r\n-body\r\n0\r\n\r\n";

    assert_all_splits(FIXED, b"borrowed-body");
    assert_all_splits(CHUNKED, b"chunked-body");
}

#[test]
fn connection_pending() {
    const PARTIAL: &[u8] =
        b"POST /echo HTTP/1.1\r\nHost: local\r\nContent-Length: 13\r\n\r\nborrowed";
    let state = State::new(b"borrowed-body", false);
    {
        let router = Router::new(&state);
        let mut driver = Driver::try_new(&router, *b"Thu, 01 Jan 1970 00:00:00 GMT")
            .expect("route caches must bind");
        let mut first = Conn::default();
        let mut second = Conn::default();
        let mut write = [0_u8; 4096];

        let cycle = driver.ingest(&mut first, PARTIAL, &mut write, head::Connection::KeepAlive);
        assert!(matches!(cycle.state, Ok(Egress::None)));

        let cycle = driver.ingest(
            &mut second,
            PARTIAL,
            &mut write,
            head::Connection::KeepAlive,
        );
        assert!(matches!(cycle.state, Ok(Egress::None)));

        let cycle = driver.ingest(
            &mut first,
            b"-body",
            &mut write,
            head::Connection::KeepAlive,
        );
        assert_reply(&cycle, &write, b"borrowed-body");

        let cycle = driver.ingest(
            &mut second,
            b"-body",
            &mut write,
            head::Connection::KeepAlive,
        );
        assert_reply(&cycle, &write, b"borrowed-body");
    }
}

#[test]
fn framing_reuses_ranges() {
    const CHUNKED: &[u8] =
        b"POST /echo HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\n\r\n";
    const FIXED: &[u8] = b"POST /echo HTTP/1.1\r\nHost: local\r\nContent-Length: 4\r\n\r\n";
    let state = State::new(b"body", false);
    {
        let router = Router::new(&state);
        let mut driver = Driver::try_new(&router, *b"Thu, 01 Jan 1970 00:00:00 GMT")
            .expect("route caches must bind");
        let mut conn = Conn::default();
        let mut write = [0_u8; 4096];
        let mut baseline = 0;
        for (turn, header) in [CHUNKED, CHUNKED, FIXED, CHUNKED, FIXED, CHUNKED]
            .into_iter()
            .enumerate()
        {
            let (_, alloc) = Tracker::count(|| {
                let first =
                    driver.ingest(&mut conn, header, &mut write, head::Connection::KeepAlive);
                assert!(matches!(first.state, Ok(Egress::None)));
                let body = if header == CHUNKED {
                    b"4\r\nbody\r\n0\r\n\r\n".as_slice()
                } else {
                    b"body"
                };
                let cycle = driver.ingest(&mut conn, body, &mut write, head::Connection::KeepAlive);
                assert!(matches!(cycle.connection, head::Connection::KeepAlive));
                assert_reply(&cycle, &write, b"body");
            });
            if turn == 1 {
                baseline = alloc;
            } else if turn == 3 || turn == 5 {
                assert_eq!(
                    alloc, baseline,
                    "fixed frames must preserve the chunk range allocation"
                );
            }
        }
    }
}

#[test]
fn ingress_resumes() {
    const REQUEST: &[u8] = b"GET /items/42 HTTP/1.1\r\nHost: local\r\n\r\n";
    let state = State::new(b"unused", false);
    {
        let router = Router::new(&state);
        let mut driver = Driver::try_new(&router, *b"Thu, 01 Jan 1970 00:00:00 GMT")
            .expect("route caches must bind");
        let mut conn = Conn::default();

        let held = driver.retain(&mut conn, REQUEST);
        assert!(matches!(held.state, Ok(Egress::Wake)));

        let mut write = [0_u8; 4096];
        let cycle = driver.ingest(&mut conn, &[], &mut write, head::Connection::KeepAlive);
        assert_reply(&cycle, &write, b"42");
    }
}
