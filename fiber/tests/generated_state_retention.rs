pub mod common;

use std::{
    cell::Cell,
    convert::Infallible,
    io::{Read, Write},
    marker::PhantomData,
    net::TcpStream,
    time::Duration,
};

use common::server::recover;
use common::{
    route::glue::task::{self, generics},
    server::{self, Plan},
};
use dope::{
    core::driver::life::recovery::Policy,
    net::wire::{self, domain},
    runtime::{executor::session, topology},
};
use dope_fiber::task::storage::scope::Brand;
use dope_test::harness::server::Pair;
use sark_fiber::Config;

#[test]
fn retains_generic_body() {
    let harness = Pair::bind().expect("bind isolated listener");
    let plan = server::Plan::new(harness.addr(), Duration::from_secs(5)).build();
    harness
        .check(
            |context, listener| {
                let first = String::from("generic ");
                let second = String::from("response");
                let encoded = Cell::new(0);
                let state = generics::State::<_, 2> {
                    source: task::State {
                        first: first.as_bytes(),
                        second: second.as_bytes(),
                        encoded: &encoded,
                    },
                };
                let mut reports =
                    server::Plan::try_reports(&plan).expect("server descriptor claims");
                let result = plan.with_bound(listener).prepare(()).map(|prepared| {
                    let mut finish = session::Completion::new();
                    prepared.enter_with(
                        context,
                        {
                            let (state, config, report) =
                                (&state, Config::new(server::MAX_CONNECTIONS), &mut finish);
                            move |worker| {
                                worker.lend(|plan, stage| {
                                    let wire =
                                        domain::Local::<wire::Identity, _>::new(wire::Identity);
                                    Brand::with(|brand| {
                                        let app = generics::App::new(brand, state, config);
                                        plan.install(stage, wire, app, |root, _wait, stage| {
                                            let result = stage.request(report, root);
                                            result
                                                .as_ref()
                                                .map_err(|error| error.cause())
                                                .expect("session");
                                            result.ok().expect("checked session").expect("drive")
                                        })
                                    })
                                })
                            }
                        },
                        Policy::new(|event| reports.push(event), recover),
                        |result| {
                            result
                                .as_ref()
                                .map_err(|error| error.cause())
                                .expect("topology");
                            drop(result.ok().expect("checked topology").expect("install"));
                        },
                    )
                });
                assert!(
                    reports.events().expect("reclaimed resources").is_empty(),
                    "server retirement: {reports:?}"
                );
                assert_eq!(encoded.get(), 1);
                result
            },
            |address| {
                let mut socket = TcpStream::connect(address).expect("connect generic task client");
                socket
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .expect("set response deadline");
                socket
                    .write_all(b"GET / HTTP/1.1\r\nHost: task\r\nConnection: close\r\n\r\n")
                    .expect("write task request");
                let mut response = Vec::new();
                socket
                    .read_to_end(&mut response)
                    .expect("read completed response");
                assert!(response.ends_with(b"generic response"));
            },
            |result| {
                assert!(
                    result.is_ok(),
                    "server preparation failed: {:?}",
                    result.err()
                )
            },
        )
        .expect("generic task harness");
}

#[test]
fn retains_scoped_body() {
    let harness = Pair::bind().expect("bind scoped listener");
    let plan = server::Plan::new(harness.addr(), Duration::from_secs(5)).build();
    harness
        .run(
            |context, listener| {
                let mut reports =
                    server::Plan::try_reports(&plan).expect("server descriptor claims");
                let prepared = plan
                    .with_bound(listener)
                    .prepare(())
                    .expect("server resources");
                let mut finish = session::Completion::new();
                prepared.enter_with(
                    context,
                    |worker| {
                        worker.lend(|plan, stage| {
                            let first = String::from("scoped ");
                            let second = String::from("response");
                            let encoded = Cell::new(0);
                            let state = generics::Scoped::<_, 2> {
                                config: Cell::<&'static str>::new("independent"),
                                observed: Cell::new(None),
                                driver: PhantomData,
                                source: task::State {
                                    first: first.as_bytes(),
                                    second: second.as_bytes(),
                                    encoded: &encoded,
                                },
                            };
                            let report = &mut finish;
                            let result = {
                                let (wire, state, config, report) = (
                                    domain::Local::<wire::Identity, _>::new(wire::Identity),
                                    &state,
                                    Config::new(server::MAX_CONNECTIONS),
                                    &mut *report,
                                );
                                Brand::with(|brand| {
                                    let app = generics::ScopedApp::new(brand, state, config);
                                    plan.install(stage, wire, app, |root, _wait, stage| {
                                        let result = stage.request(report, root);
                                        result
                                            .as_ref()
                                            .map_err(|error| error.cause())
                                            .expect("session");
                                        result.ok().expect("checked session").expect("drive")
                                    })
                                })
                            }
                            .map_err(topology::Failure::Install);
                            assert_eq!(encoded.get(), 1);
                            assert_eq!(state.observed.get(), Some("independent"));
                            result
                        })
                    },
                    Policy::new(|event| reports.push(event), recover),
                    |result| {
                        let result = result.map_err(topology::RunError::from);
                        result
                            .as_ref()
                            .map_err(|error| error.cause())
                            .expect("topology")
                            .as_ref()
                            .map_err(|error| error.cause())
                            .expect("worker");
                    },
                );
                assert!(
                    reports.events().expect("reclaimed resources").is_empty(),
                    "server retirement: {reports:?}"
                );
                Ok::<(), Infallible>(())
            },
            |address| {
                let mut socket = TcpStream::connect(address).expect("connect scoped task client");
                socket
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .expect("set response deadline");
                socket
                    .write_all(b"GET / HTTP/1.1\r\nHost: task\r\nConnection: close\r\n\r\n")
                    .expect("write task request");
                let mut response = Vec::new();
                socket
                    .read_to_end(&mut response)
                    .expect("read scoped response");
                assert!(response.ends_with(b"scoped response"));
            },
        )
        .expect("scoped task harness");
}

#[test]
fn retains_state_to_finish() {
    let harness = Pair::bind().expect("bind isolated listener");
    let plan = server::Plan::new(harness.addr(), Duration::from_secs(5)).build();
    harness
        .check(
            |context, listener| {
                let first = String::from("retained ");
                let second = String::from("response");
                let encoded = Cell::new(0);
                let state = task::State {
                    first: first.as_bytes(),
                    second: second.as_bytes(),
                    encoded: &encoded,
                };
                let mut reports = Plan::try_reports(&plan).expect("server descriptor claims");
                let result = task::App::serve(
                    plan.with_bound(listener),
                    context,
                    state,
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
                assert_eq!(encoded.get(), task::CASES.len());
                result
            },
            |address| {
                for (request, status) in task::CASES {
                    let mut socket = TcpStream::connect(address).expect("connect task client");
                    socket
                        .set_read_timeout(Some(Duration::from_secs(3)))
                        .expect("set response deadline");
                    socket.write_all(request).expect("write task request");
                    let mut response = Vec::new();
                    socket
                        .read_to_end(&mut response)
                        .expect("read completed response");
                    assert!(response.starts_with(status));
                    let body = response
                        .windows(4)
                        .position(|bytes| bytes == b"\r\n\r\n")
                        .expect("response headers")
                        + 4;
                    assert_eq!(&response[body..], b"retained response");
                }
            },
            |result| {
                assert!(
                    result.is_ok(),
                    "server preparation failed: {:?}",
                    result.err()
                );
            },
        )
        .expect("task harness");
}
