use dope::core::driver::life::recovery;
pub mod common;

use std::{
    cell::Cell,
    convert::Infallible,
    time::{Duration, Instant},
};

use dope::{
    core::{
        driver::route::{FrameworkTag, KeyTag, Tag, kind, table::conn},
        io::socket::Backlog,
    },
    manifold::timing,
    runtime::{executor::session, topology, topology::lending::Stage},
};
use dope_test::{
    harness::{
        server::Pair,
        signals::{LocalGate, Progress as _},
    },
    reclaim::Reports,
};
use sark_grpc::server::{
    Bank, Plan,
    config::{Limits, Output},
    mode,
    service::handler::output::Duplex,
};
use sark_h2::server::runtime::modes;

use crate::common::{
    codec::Bytes,
    grpc::{
        State,
        call::egress::{reply::Reply, streams},
        settings::Settings,
    },
};

#[test]
fn bidi_ingress() {
    let harness = Pair::bind().expect("gRPC bidi harness");
    let bind = harness.addr();
    let config = Settings {
        bind,
        connections: conn::Capacity::fixed::<4>(),
        backlog: Backlog::fixed::<16>(),
        limits: Limits::default()
            .with_output(Output::try_new(1, 9).expect("one-message bidi output budget")),
    };

    harness
        .run(
            move |context, listener| {
                let drops = LocalGate::new();
                let dropped = drops.handle();
                let handler = State::new(Duplex::new(streams::Echo, Bytes), Some(dropped));
                let bank = Bank::try_new(config.connections.count(), config.limits)
                    .expect("gRPC readiness bank");
                let prepared = config
                    .h2::<KeyTag<0>>()
                    .expect("H2 plan")
                    .with_bound(listener)
                    .with_plain::<_, modes::Serve<Infallible>>(Plan::new(
                        handler,
                        &bank,
                        mode::Direct,
                    ))
                    .for_install::<timing::Throughput>()
                    .expect("server resources");
                let mut reports =
                    Reports::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
                let mut finish = session::Completion::new();
                prepared.enter_with(
                    context,
                    |worker| {
                        worker
                            .lend(|plan, stage| {
                                plan.install(stage, |app, stage| {
                                    let (app, wait) = app.into_parts();
                                    let watch = wait.watch();
                                    watch.finish(Stage::with_app(stage, &mut finish, app, |app| {
                                        let result = wait.run(app);
                                        assert_eq!(dropped.hits(), 1);
                                        result
                                    }))
                                })
                            })
                            .map_err(topology::Failure::Install)?
                    },
                    recovery::Policy::new(|report| reports.push(report), common::recover),
                    |result| {
                        (match result {
                            Ok(result) => result.map_err(topology::RunError::Failure),
                            Err(error) => Err(topology::RunError::from(error)),
                        })
                        .as_ref()
                        .map(|_| ())
                        .map_err(topology::RunError::cause)
                        .expect("gRPC runtime");
                    },
                );
                assert!(
                    reports.events().expect("retirement owners").is_empty(),
                    "unexpected server retirement: {reports:?}"
                );
                Ok::<(), Infallible>(())
            },
            move |address| {
                let reply = Reply::try_exchange(address, b"/proof/Chat", &[b"one", b"two"], true)
                    .expect("gRPC exchange");
                assert_eq!(reply.messages, vec![b"eno".to_vec(), b"owt".to_vec()]);
            },
        )
        .expect("bidi E2E");
}
#[test]
fn late_ingress() {
    let harness = Pair::bind().expect("gRPC early-completion harness");
    let bind = harness.addr();
    let config = Settings {
        bind,
        connections: conn::Capacity::fixed::<4>(),
        backlog: Backlog::fixed::<16>(),
        limits: Limits::default(),
    };

    harness
        .run(
            move |context, listener| {
                let drops = LocalGate::new();
                let dropped = drops.handle();
                let ingress = Cell::new(0);
                let polls = Cell::new(0);
                let error = Cell::new(None);
                let handler = State::new(
                    Duplex::new(streams::Early, Bytes),
                    streams::BidiCounts::new(dropped, &ingress, &polls, &error),
                );
                let bank = Bank::try_new(config.connections.count(), config.limits)
                    .expect("gRPC readiness bank");
                let prepared = config
                    .h2::<KeyTag<0>>()
                    .expect("H2 plan")
                    .with_bound(listener)
                    .with_plain::<_, modes::Serve<Infallible>>(Plan::new(
                        handler,
                        &bank,
                        mode::Direct,
                    ))
                    .for_install::<timing::Throughput>()
                    .expect("server resources");
                let mut reports =
                    Reports::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
                let mut finish = session::Completion::new();
                prepared.enter_with(
                    context,
                    |worker| {
                        worker
                            .lend(|plan, stage| {
                                plan.install(stage, |app, stage| {
                                    let (app, wait) = app.into_parts();
                                    let watch = wait.watch();
                                    watch.finish(Stage::with_app(stage, &mut finish, app, |app| {
                                        let result = wait.run(app);
                                        assert_eq!(dropped.hits(), 1);
                                        assert_eq!(ingress.get(), 0);
                                        assert_eq!(error.get(), None);
                                        assert_eq!(polls.get(), 1);
                                        result
                                    }))
                                })
                            })
                            .map_err(topology::Failure::Install)?
                    },
                    recovery::Policy::new(|report| reports.push(report), common::recover),
                    |result| {
                        (match result {
                            Ok(result) => result.map_err(topology::RunError::Failure),
                            Err(error) => Err(topology::RunError::from(error)),
                        })
                        .as_ref()
                        .map(|_| ())
                        .map_err(topology::RunError::cause)
                        .expect("gRPC runtime");
                    },
                );
                assert_eq!(error.get(), None);
                assert!(
                    reports.events().expect("retirement owners").is_empty(),
                    "unexpected server retirement: {reports:?}"
                );
                Ok::<(), Infallible>(())
            },
            move |address| {
                let reply = Reply::try_exchange(address, b"/proof/Early", &[b"late"], true)
                    .expect("gRPC exchange");
                assert!(reply.messages.is_empty());
            },
        )
        .expect("early terminal E2E");
}
#[test]
fn bidi_timer() {
    let harness = Pair::bind().expect("gRPC deferred-output harness");
    let bind = harness.addr();
    let config = Settings {
        bind,
        connections: conn::Capacity::fixed::<4>(),
        backlog: Backlog::fixed::<16>(),
        limits: Limits::default(),
    };

    harness
        .run(
            move |context, listener| {
                let drops = LocalGate::new();
                let dropped = drops.handle();
                let ingress = Cell::new(0);
                let polls = Cell::new(0);
                let error = Cell::new(None);
                let handler = State::new(
                    Duplex::new(streams::Wake, Bytes),
                    streams::BidiCounts::new(dropped, &ingress, &polls, &error),
                );
                let bank = Bank::try_new(config.connections.count(), config.limits)
                    .expect("gRPC readiness bank");
                let prepared = config
                    .h2::<KeyTag<0>>()
                    .expect("H2 plan")
                    .with_bound(listener)
                    .with_plain::<_, modes::Serve<Infallible>>(Plan::new(
                        handler,
                        &bank,
                        mode::Direct,
                    ))
                    .for_install::<timing::Throughput>()
                    .expect("server resources");
                let mut reports =
                    Reports::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
                let mut finish = session::Completion::new();
                prepared.enter_with(
                    context,
                    |worker| {
                        worker
                            .lend(|plan, stage| {
                                plan.install(stage, |app, stage| {
                                    let (app, wait) = app.into_parts();
                                    let watch = wait.watch();
                                    watch.finish(Stage::with_app(stage, &mut finish, app, |app| {
                                        let result = wait.run(app);
                                        assert_eq!(dropped.hits(), 1);
                                        assert_eq!(ingress.get(), 0);
                                        assert_eq!(error.get(), None);
                                        assert_eq!(polls.get(), 3);
                                        result
                                    }))
                                })
                            })
                            .map_err(topology::Failure::Install)?
                    },
                    recovery::Policy::new(|report| reports.push(report), common::recover),
                    |result| {
                        (match result {
                            Ok(result) => result.map_err(topology::RunError::Failure),
                            Err(error) => Err(topology::RunError::from(error)),
                        })
                        .as_ref()
                        .map(|_| ())
                        .map_err(topology::RunError::cause)
                        .expect("gRPC runtime");
                    },
                );
                assert_eq!(error.get(), None);
                assert!(
                    reports.events().expect("retirement owners").is_empty(),
                    "unexpected server retirement: {reports:?}"
                );
                Ok::<(), Infallible>(())
            },
            move |address| {
                let started = Instant::now();
                let reply = Reply::try_exchange(address, b"/proof/Wake", &[], false)
                    .expect("gRPC exchange");
                assert_eq!(reply.messages, vec![b"wake".to_vec()]);
                assert!(started.elapsed() >= Duration::from_millis(5));
            },
        )
        .expect("driver-timer bidi E2E");
}
