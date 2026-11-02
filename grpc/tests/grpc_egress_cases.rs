use dope::core::driver::life::recovery;
pub mod common;

use std::{
    cell::Cell,
    convert::Infallible,
    sync::mpsc,
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
    fibers,
    harness::{
        server::Pair,
        signals::{LocalGate, Progress as _},
    },
    reclaim::Reports,
};
use sark_core::manifold::stream::Section;
use sark_grpc::{
    Code, Status,
    server::{
        Bank, Plan,
        config::{Limits, Output},
        mode,
        service::handler::output::Streaming,
    },
};
use sark_h2::server::runtime::modes;

use crate::common::{
    codec::Bytes,
    grpc::{
        State,
        call::egress::{capacity::Waiters, failure, reply::Reply, streams},
        settings::Settings,
    },
};

#[test]
fn backpressure() {
    let harness = Pair::bind().expect("gRPC server-streaming harness");
    let bind = harness.addr();
    let config = Settings {
        bind,
        connections: conn::Capacity::fixed::<4>(),
        backlog: Backlog::fixed::<16>(),
        limits: Limits::default()
            .with_output(Output::try_new(1, 13).expect("one-message server-stream output budget")),
    };

    let (checked, complete) = mpsc::sync_channel(1);
    harness
        .run(
            move |context, listener| {
                let drops = LocalGate::new();
                let dropped = drops.handle();
                let mut requests = streams::Observed::new(Vec::new());
                let handler = State::new(
                    Streaming::new(streams::Backpressure(&requests), Bytes),
                    dropped,
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
                                    Stage::with_app(stage, &mut finish, app, |mut app| {
                                        assert_eq!(
                                            fibers::Harness::new(Duration::from_secs(2))
                                                .try_wait(&mut app, &dropped, 1)
                                                .expect("progress drive"),
                                            fibers::Wait::Reached
                                        );
                                        assert_eq!(dropped.hits(), 1);
                                        let mut observed = requests.take();
                                        let request = observed.pop();
                                        let repeated = !observed.is_empty();
                                        requests.set(observed);
                                        let (body, metadata, fields) = request.expect("request");
                                        fields.expect("response metadata");
                                        assert!(!repeated, "request repeated");
                                        assert_eq!(body, b"request");
                                        assert_eq!(
                                            metadata.as_deref(),
                                            Some(b"retained-through-response".as_slice())
                                        );
                                        checked.send(()).expect("retirement observer");
                                        app.run()
                                    })?
                                    .map_err(session::Error::Drive)
                                })
                            })
                            .map_err(topology::Failure::Install)?
                            .map_err(topology::Failure::Run)
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
                    requests.get_mut().is_empty(),
                    "request repeated after completion"
                );
                assert!(
                    reports.events().expect("retirement owners").is_empty(),
                    "unexpected server retirement: {reports:?}"
                );
                Ok::<(), Infallible>(())
            },
            move |address| {
                let reply = Reply::try_exchange(address, b"/proof/Stream", &[b"request"], true)
                    .expect("gRPC exchange");
                let initial = reply.headers.iter().any(|(section, fields)| {
                    matches!(section, Section::Initial)
                        && fields.get(b"x-owner-initial") == Some(b"retained".as_slice())
                });
                let trailing = reply.headers.iter().any(|(section, fields)| {
                    matches!(section, Section::Trailing)
                        && fields.get(b"x-owner-trailer") == Some(b"retained".as_slice())
                });
                assert_eq!(reply.messages, vec![vec![0; 8], vec![1; 8], vec![2; 8]],);
                assert!(initial, "initial metadata crossed the real H2 connection");
                assert!(trailing, "trailing metadata crossed the real H2 connection");
                complete
                    .recv_timeout(Duration::from_secs(2))
                    .expect("producer retired before shutdown");
            },
        )
        .expect("server-streaming E2E");
}
#[test]
fn status() {
    let harness = Pair::bind().expect("gRPC generated-status harness");
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
                let mut requests = Cell::new(Vec::new());
                let handler = State::new(Streaming::new(streams::Failure(&requests), Bytes), ());
                let retired = config
                    .try_serve_on(handler, context, listener, common::recover, |result| {
                        result.expect("gRPC runtime")
                    })
                    .expect("gRPC readiness bank");
                assert!(
                    retired.is_empty(),
                    "unexpected server retirement: {retired:?}"
                );
                assert_eq!(requests.get_mut().as_slice(), &[b"request".to_vec()]);
                Ok::<(), Infallible>(())
            },
            |address| {
                let reply = Reply::try_exchange(address, b"/proof/Status", &[b"request"], true)
                    .expect("gRPC exchange");
                let (_, fields) = reply
                    .headers
                    .iter()
                    .find(|(section, fields)| {
                        *section == Section::Trailing && fields.get(b"grpc-status").is_some()
                    })
                    .expect("generated status trailer crossed TCP/H2");
                let status = Status::from_fields(fields).expect("generated status fields decode");
                assert_eq!(status.code(), Code::InvalidArgument);
                assert_eq!(status.message(), None);
                assert!(reply.messages.is_empty());
            },
        )
        .expect("generated status TCP E2E");
}
#[test]
fn encode_rollback() {
    let pair = Pair::bind().expect("codec rollback server");
    let settings = Settings::new(pair.addr(), Limits::default());
    pair.run(
        move |context, listener| {
            let codes = Cell::new([None; 2]);
            let retired = settings
                .try_serve_on(
                    failure::Encoded(&codes),
                    context,
                    listener,
                    common::recover,
                    |result| result.expect("gRPC runtime"),
                )
                .expect("server bank");
            assert!(
                retired.is_empty(),
                "unexpected server retirement: {retired:?}"
            );
            assert_eq!(codes.get(), [Some(Code::DataLoss); 2]);
            Ok::<_, Infallible>(())
        },
        |address| {
            let [(failed, status), (kept, ready)] = Waiters::try_pair(address)
                .expect("paired codec requests")
                .try_release()
                .expect("paired codec responses");
            assert_eq!(status.expect("codec failure").code(), Code::DataLoss);
            assert!(failed.is_empty());
            assert_eq!(ready.expect("healthy sibling").code(), Code::Ok);
            assert_eq!(
                Bytes::try_messages(&kept).expect("preserved staged response"),
                [b"kept".to_vec()]
            );
        },
    )
    .expect("codec rollback");
}

#[test]
fn stream_timer() {
    let harness = Pair::bind().expect("gRPC server-stream deferred-output harness");
    let bind = harness.addr();
    let config = Settings {
        bind,
        connections: conn::Capacity::fixed::<4>(),
        backlog: Backlog::fixed::<16>(),
        limits: Limits::default(),
    };

    let (checked, complete) = mpsc::sync_channel(1);
    harness
        .run(
            move |context, listener| {
                let drops = LocalGate::new();
                let dropped = drops.handle();
                let polls = Cell::new(0);
                let error = Cell::new(None);
                let mut requests = Cell::new(Vec::new());
                let handler = State::new(
                    Streaming::new(streams::Timed(&requests), Bytes),
                    streams::Counts::new(dropped, &polls, &error),
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
                                    Stage::with_app(stage, &mut finish, app, |mut app| {
                                        assert_eq!(
                                            fibers::Harness::new(Duration::from_secs(2))
                                                .try_wait(&mut app, &dropped, 1)
                                                .expect("progress drive"),
                                            fibers::Wait::Reached
                                        );
                                        assert_eq!(dropped.hits(), 1);
                                        assert_eq!(error.get(), None);
                                        assert_eq!(polls.get(), 3);
                                        checked.send(()).expect("retirement observer");
                                        app.run()
                                    })?
                                    .map_err(session::Error::Drive)
                                })
                            })
                            .map_err(topology::Failure::Install)?
                            .map_err(topology::Failure::Run)
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
                assert_eq!(requests.get_mut().as_slice(), &[b"request".to_vec()]);
                assert!(
                    reports.events().expect("retirement owners").is_empty(),
                    "unexpected server retirement: {reports:?}"
                );
                Ok::<(), Infallible>(())
            },
            move |address| {
                let started = Instant::now();
                let reply = Reply::try_exchange(address, b"/proof/WakeServer", &[b"request"], true)
                    .expect("gRPC exchange");
                assert_eq!(reply.messages, vec![b"wake".to_vec()]);
                assert!(started.elapsed() >= Duration::from_millis(5));
                complete
                    .recv_timeout(Duration::from_secs(2))
                    .expect("producer retired before shutdown");
            },
        )
        .expect("driver-timer server-stream E2E");
}
