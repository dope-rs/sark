pub mod common;

use std::{cell::Cell, convert::Infallible, marker::PhantomData, time::Duration};

use common::{
    route::glue::task::{self, generics},
    server::{self, peer::request_to_eof},
};
use dope::{
    net::wire::{self, domain},
    runtime::executor::session,
};
use dope_fiber::task::storage::scope::Brand;
use dope_test::harness::server::Pair;
use sark_fiber::Config;

#[test]
fn retains_generic_body() {
    let harness = Pair::bind().expect("bind isolated listener");
    let plan = server::Plan::new(harness.addr(), Duration::from_secs(5)).build();
    harness
        .run(
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
                let result = server::try_serve::<generics::App, _>(
                    plan,
                    context,
                    listener,
                    state,
                    server::MAX_CONNECTIONS,
                );
                assert_eq!(encoded.get(), 1);
                result
            },
            |address| {
                let response = request_to_eof(address, task::CASES[0].0, Duration::from_secs(3))
                    .expect("generic response");
                assert!(response.ends_with(b"generic response"));
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
                                    domain::Local::new(wire::Identity),
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
                            };
                            assert_eq!(encoded.get(), 1);
                            assert_eq!(state.observed.get(), Some("independent"));
                            result
                        })
                    },
                    server::reporting(&mut reports),
                    |result| {
                        result
                            .as_ref()
                            .map_err(|error| error.kind().map(|output| output.as_ref().map(|_| ())))
                            .expect("topology")
                            .as_ref()
                            .expect("worker");
                    },
                );
                server::reclaimed(&reports);
                Ok::<(), Infallible>(())
            },
            |address| {
                let response = request_to_eof(address, task::CASES[0].0, Duration::from_secs(3))
                    .expect("scoped response");
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
        .run(
            |context, listener| {
                let first = String::from("retained ");
                let second = String::from("response");
                let encoded = Cell::new(0);
                let state = task::State {
                    first: first.as_bytes(),
                    second: second.as_bytes(),
                    encoded: &encoded,
                };
                let result = server::try_serve::<task::App, _>(
                    plan,
                    context,
                    listener,
                    state,
                    server::MAX_CONNECTIONS,
                );
                assert_eq!(encoded.get(), task::CASES.len());
                result
            },
            |address| {
                for (request, status) in task::CASES {
                    let response = request_to_eof(address, request, Duration::from_secs(3))
                        .expect("task response");
                    assert!(response.starts_with(status));
                    let body = response
                        .windows(4)
                        .position(|bytes| bytes == b"\r\n\r\n")
                        .expect("response headers")
                        + 4;
                    assert_eq!(&response[body..], b"retained response");
                }
            },
        )
        .expect("task harness");
}
