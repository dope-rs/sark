pub mod common;

use std::{
    cell::Cell,
    convert::Infallible,
    io::{Read, Write},
    net::TcpStream,
    thread,
    time::{Duration, Instant},
};

use dope::core::driver::life::recovery::Policy;
use dope_test::harness::{peer::Peer, server::Pair};
use sark_fiber::Config;

use crate::common::{
    retained::State,
    route::{
        chunked::wire::Request,
        glue::storage::{self, GlobalBudgetApp, MixedStorageApp},
    },
    server::{self, Plan},
};
use common::server::recover;

#[test]
fn routes_dispatch_heterogeneous() {
    let harness = Pair::bind().expect("bind isolated listener");
    let bind = harness.addr();
    let server = server::Plan::new(bind, Duration::from_secs(10)).build();

    harness
        .run(
            |context, listener| {
                let mut reports = Plan::try_reports(&server).expect("server descriptor claims");
                let failures = Cell::new(Vec::new());
                let result = MixedStorageApp::serve(
                    server.with_bound(listener),
                    context,
                    State(&failures),
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
                let failures = failures.into_inner();
                assert!(failures.is_empty(), "storage: {failures:?}");
                assert!(
                    result.is_ok(),
                    "routes_dispatch_heterogeneous server shutdown: {:?}",
                    result.err()
                );
                Ok::<_, Infallible>(())
            },
            |bind| {
                for (path, expected) in [
                    ("/narrow", "narrow"),
                    ("/wide", "wide"),
                    ("/stream", "stream"),
                ] {
                    let mut stream = TcpStream::connect(bind).expect("connect");
                    stream
                        .write_all(&Request::new(path).get())
                        .expect("write route request");
                    let mut response = String::new();
                    stream
                        .read_to_string(&mut response)
                        .expect("read route response");
                    assert!(response.contains("200 OK"), "{path}: {response}");
                    assert!(response.contains(expected), "{path}: {response}");
                }
            },
        )
        .expect("harness");
}

#[test]
fn quota_is_reusable() {
    let harness = Pair::bind().expect("bind isolated listener");
    let bind = harness.addr();
    let server = server::Plan::new(bind, Duration::from_secs(10)).build();

    harness
        .run(
            |context, listener| {
                let mut reports = Plan::try_reports(&server).expect("server descriptor claims");
                let failures = Cell::new(Vec::new());
                let result = GlobalBudgetApp::serve(
                    server.with_bound(listener),
                    context,
                    State(&failures),
                    Config::new(1),
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
                let failures = failures.into_inner();
                assert!(failures.is_empty(), "storage: {failures:?}");
                assert!(
                    result.is_ok(),
                    "quota_is_reusable server shutdown: {:?}",
                    result.err()
                );
                Ok::<_, Infallible>(())
            },
            |bind| {
                {
                    let mut held = TcpStream::connect(bind).expect("connect held task");
                    held.set_nodelay(true).expect("configure held task socket");
                    held.write_all(storage::HELD)
                        .expect("write held task request");

                    let mut quick =
                        TcpStream::connect(bind).expect("connect blocked quick request");
                    quick
                        .set_read_timeout(Some(Duration::from_secs(2)))
                        .expect("set blocked quick deadline");
                    quick
                        .write_all(storage::QUICK)
                        .expect("write blocked quick request");
                    let mut response = String::new();
                    quick
                        .read_to_string(&mut response)
                        .expect("read blocked quick response");
                    assert!(response.contains("503"), "response: {response}");

                    let mut stream =
                        TcpStream::connect(bind).expect("connect blocked stream request");
                    stream
                        .set_read_timeout(Some(Duration::from_secs(2)))
                        .expect("set blocked stream deadline");
                    stream
                        .write_all(storage::STREAM)
                        .expect("write blocked stream request");
                    let mut response = String::new();
                    stream
                        .read_to_string(&mut response)
                        .expect("read blocked stream response");
                    assert!(response.contains("503"), "response: {response}");

                    Peer::try_reset(held).expect("reset held task connection");
                }

                let deadline = Instant::now() + Duration::from_secs(2);
                loop {
                    let mut retry = TcpStream::connect(bind).expect("connect retry request");
                    retry
                        .set_read_timeout(Some(Duration::from_secs(2)))
                        .expect("set retry deadline");
                    retry
                        .write_all(storage::STREAM)
                        .expect("write retry request");
                    let mut response = String::new();
                    retry
                        .read_to_string(&mut response)
                        .expect("read retry response");
                    if response.contains("200 OK") {
                        assert!(response.contains("stream"), "response: {response}");
                        break;
                    }
                    assert!(response.contains("503"), "response: {response}");
                    assert!(
                        Instant::now() < deadline,
                        "closed task did not return its global admission slot",
                    );
                    thread::sleep(Duration::from_millis(10));
                }
            },
        )
        .expect("harness");
}
