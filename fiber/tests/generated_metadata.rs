pub mod common;

use std::{
    net::{Shutdown, TcpStream},
    time::Duration,
};

use common::server::recover;
use common::{
    route::{select::SelectiveDispatch, skip::SkipDispatch},
    server::{self, Plan, peer::Endpoint as _},
};
use dope::core::driver::life::recovery::Policy;
use dope_test::harness::server::Pair;
use sark_fiber::Config;

#[test]
fn selects_requested_fields() {
    let harness = Pair::bind().expect("bind isolated listener");
    let bind = harness.addr();
    let server = server::Plan::new(bind, Duration::from_secs(10)).build();

    harness
        .check(
            |context, listener| {
                let mut reports = Plan::try_reports(&server).expect("server descriptor claims");
                let result = SelectiveDispatch::serve(
                    server.with_bound(listener),
                    context,
                    (),
                    Config::new(server::MAX_CONNECTIONS),
                    Policy::new(|event| reports.push(event), recover),
                    |result| {
                        result
                            .as_ref()
                            .map(|_| ())
                            .map_err(|error| error.cause())
                            .expect("server")
                    },
                );
                assert!(
                    reports.events().expect("reclaimed resources").is_empty(),
                    "server retirement: {reports:?}"
                );
                result
            },
            |addr| {
                TcpStream::connect(addr)
                    .expect("connect early-close peer")
                    .shutdown(Shutdown::Both)
                    .expect("close early peer");
                assert_eq!(
                    addr.try_exchange(b"/minimal?ignored=yes")
                        .expect("minimal response")
                        .get(9..12),
                    Some(b"200".as_slice()),
                );
                assert_eq!(
                    addr.try_exchange(b"/items/42?tag=rust")
                        .expect("item response")
                        .get(9..12),
                    Some(b"418".as_slice()),
                );
                assert!(
                    addr.try_exchange(b"/items/new?tag=rust")
                        .expect("literal response")
                        .ends_with(b"minimal"),
                    "the strict literal route must win over its parameter fallback",
                );
                for (path, body) in [
                    (b"/items/4".as_slice(), b"routed".as_slice()),
                    (b"/items/4?tag=rust", b"routed"),
                    (b"/items/42suffix", b"minimal"),
                    (b"/items/42suffix?tag=rust", b"minimal"),
                    (b"/items/42suffix.", b"routed"),
                    (b"/items/42suffix.gz", b"full"),
                    (b"/items/42suffix.gx?tag=rust", b"routed"),
                ] {
                    assert!(
                        addr.try_exchange(path)
                            .expect("prefix response")
                            .ends_with(body),
                        "wrong route for {path:?}",
                    );
                }
                for path in [b"/items/4/".as_slice(), b"/items/4/2suffix"] {
                    assert_eq!(
                        addr.try_exchange(path)
                            .expect("prefix delimiter response")
                            .get(9..12),
                        Some(b"404".as_slice()),
                    );
                }

                let unused = b"/minimal?ignored=\x01";
                assert_eq!(
                    addr.try_exchange(unused)
                        .expect("invalid selective response")
                        .get(9..12),
                    Some(b"400".as_slice()),
                    "selective extraction must not bypass protocol-level target validation",
                );

                let full_bad = b"/full?ignored=\x01";
                assert_eq!(
                    addr.try_exchange(full_bad)
                        .expect("invalid full response")
                        .get(9..12),
                    Some(b"400".as_slice()),
                );
            },
            |result| {
                assert!(
                    result.is_ok(),
                    "selective request server shutdown: {:?}",
                    result.err()
                );
            },
        )
        .expect("harness");
}

#[test]
fn skip_shapes_headers() {
    let harness = Pair::bind().expect("bind isolated listener");
    let bind = harness.addr();
    let server = server::Plan::new(bind, Duration::from_secs(10)).build();

    harness
        .check(
            |context, listener| {
                let mut reports = Plan::try_reports(&server).expect("server descriptor claims");
                let result = SkipDispatch::serve(
                    server.with_bound(listener),
                    context,
                    (),
                    Config::new(server::MAX_CONNECTIONS),
                    Policy::new(|event| reports.push(event), recover),
                    |result| {
                        result
                            .as_ref()
                            .map(|_| ())
                            .map_err(|error| error.cause())
                            .expect("server")
                    },
                );
                assert!(
                    reports.events().expect("reclaimed resources").is_empty(),
                    "server retirement: {reports:?}"
                );
                result
            },
            |bind| {
                let lean =
                    String::from_utf8(bind.try_get("/lean").expect("lean response")).expect("utf8");
                assert!(lean.starts_with("HTTP/1.1 200 "), "lean status: {lean:?}");
                assert!(
                    lean.contains("Content-Length: 2\r\n"),
                    "lean content-length: {lean:?}"
                );
                assert!(
                    lean.ends_with("\r\n\r\nok"),
                    "lean body must arrive: {lean:?}"
                );
                assert!(!lean.contains("Server:"), "lean must omit Server: {lean:?}");
                assert!(!lean.contains("Date:"), "lean must omit Date: {lean:?}");
                assert!(
                    lean.contains("content-type: text/plain\r\n"),
                    "lean keeps content-type: {lean:?}"
                );

                let full =
                    String::from_utf8(bind.try_get("/full").expect("full response")).expect("utf8");
                assert!(
                    full.contains("Server: sark\r\n"),
                    "full keeps Server: {full:?}"
                );
                assert!(full.contains("Date: "), "full keeps Date: {full:?}");
                assert!(
                    !full.contains("Mon, 01 Jan 2000"),
                    "full Date must be patched at the live offset, not the placeholder: {full:?}"
                );
                assert!(
                    full.ends_with("\r\n\r\nok"),
                    "full body must arrive: {full:?}"
                );

                let no_srv =
                    String::from_utf8(bind.try_get("/no-server").expect("no-server response"))
                        .expect("utf8");
                assert!(
                    !no_srv.contains("Server:"),
                    "no-server must omit Server: {no_srv:?}"
                );
                assert!(
                    no_srv.contains("Date: "),
                    "no-server keeps patchable Date: {no_srv:?}"
                );
                assert!(
                    !no_srv.contains("Mon, 01 Jan 2000"),
                    "no-server Date must be patched at the shifted offset: {no_srv:?}"
                );
                assert!(
                    no_srv.ends_with("\r\n\r\nok"),
                    "no-server body must arrive: {no_srv:?}"
                );

                let no_date =
                    String::from_utf8(bind.try_get("/no-date").expect("no-date response"))
                        .expect("utf8");
                assert!(
                    no_date.contains("Server: sark\r\n"),
                    "no-date keeps Server: {no_date:?}"
                );
                assert!(
                    !no_date.contains("Date:"),
                    "no-date must omit Date: {no_date:?}"
                );
                assert!(
                    no_date.ends_with("\r\n\r\nok"),
                    "no-date body must arrive: {no_date:?}"
                );
            },
            |result| {
                assert!(
                    result.is_ok(),
                    "skip_shapes_headers server shutdown: {:?}",
                    result.err()
                );
            },
        )
        .expect("harness");
}
