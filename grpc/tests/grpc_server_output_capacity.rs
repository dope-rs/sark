use dope::core::driver::life::recovery;
pub mod common;

use std::{convert::Infallible, sync::mpsc, time::Duration};

use common::{
    codec::Bytes,
    grpc::{
        call::egress::{
            capacity::{Capacity, Failed, Waiters},
            reply::Reply,
        },
        settings::Settings,
    },
};
use dope::{
    core::driver::route::{FrameworkTag, KeyTag, Tag, kind},
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
use sark_grpc::{
    Code,
    server::{
        Bank, Plan,
        config::{Limits, Output},
        mode,
    },
};
use sark_h2::server::runtime::modes;

#[test]
fn impossible_capacity() {
    let pair = Pair::bind().expect("capacity server");
    let limits =
        Limits::default().with_output(Output::try_new(1, 5).expect("empty message capacity"));
    let settings = Settings::new(pair.addr(), limits);
    pair.run(
        move |context, listener| {
            let retired = settings
                .try_serve_on(
                    Capacity::default(),
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
            Ok::<_, Infallible>(())
        },
        |address| {
            let reply = Reply::try_exchange(address, b"/capacity/ready", &[], false)
                .expect("terminal capacity response");
            assert_eq!(
                reply.status().expect("response status").code(),
                Code::ResourceExhausted
            );
            assert!(reply.messages.is_empty());
        },
    )
    .expect("impossible capacity");
}

#[test]
fn failed_response_stops() {
    for live in [false, true] {
        let pair = Pair::bind().expect("capacity server");
        let limits =
            Limits::default().with_output(Output::try_new(1, 5).expect("empty message capacity"));
        let settings = Settings::new(pair.addr(), limits);
        pair.run(
            move |context, listener| {
                let output = LocalGate::new();
                let output = output.handle();
                let retired = settings
                    .try_serve_on(
                        Failed(output),
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
                assert_eq!(output.hits(), u32::from(live));
                Ok::<_, Infallible>(())
            },
            |address| {
                if live {
                    let [(failed_body, failed), (ready_body, ready)] = Waiters::try_pair(address)
                        .expect("paired capacity requests")
                        .try_release()
                        .expect("paired capacity responses");
                    assert!(failed_body.is_empty() && ready_body.is_empty());
                    assert_eq!(
                        failed.expect("failed output").code(),
                        Code::ResourceExhausted
                    );
                    assert_eq!(ready.expect("healthy sibling").code(), Code::Ok);
                } else {
                    let reply = Reply::try_exchange(address, b"/capacity/produce", &[], true)
                        .expect("terminal capacity response");
                    assert_eq!(
                        reply.status().expect("response status").code(),
                        Code::ResourceExhausted
                    );
                }
            },
        )
        .expect("failed response production");
    }
}

#[test]
fn released_waiters() {
    for cancel in [false, true] {
        let pair = Pair::bind().expect("waiter server");
        let limits =
            Limits::default().with_output(Output::try_new(1, 9).expect("one response capacity"));
        let settings = Settings::new(pair.addr(), limits);
        let (release, ready) = mpsc::sync_channel(1);
        pair.run(
            move |context, listener| {
                let counts = LocalGate::new();
                let blocked = counts.handle();
                let resumed = LocalGate::new();
                let resumed = resumed.handle();
                let attempts = LocalGate::new();
                let attempts = attempts.handle();
                let handler = Capacity {
                    blocked: Some(blocked),
                    resumed: Some(resumed),
                    attempts: Some(attempts),
                };
                let bank = Bank::try_new(settings.connections.count(), settings.limits)
                    .expect("server bank");
                let prepared = settings
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
                                        let waiters = if cancel { 2 } else { 3 };
                                        assert_eq!(
                                            fibers::Harness::new(Duration::from_secs(2))
                                                .try_wait(&mut app, &blocked, waiters)
                                                .expect("progress drive"),
                                            fibers::Wait::Reached
                                        );
                                        assert_eq!(blocked.hits(), waiters);
                                        release.send(()).expect("flow release observer");
                                        if cancel {
                                            assert_eq!(
                                                fibers::Harness::new(Duration::from_secs(2))
                                                    .try_wait(&mut app, &resumed, 1)
                                                    .expect("cancelled capacity drive"),
                                                fibers::Wait::Reached
                                            );
                                            assert_eq!(attempts.hits(), 2);
                                            release
                                                .send(())
                                                .expect("cancellation release observer");
                                        } else {
                                            assert_eq!(
                                                fibers::Harness::new(Duration::from_secs(2))
                                                    .try_wait(&mut app, &attempts, 2)
                                                    .expect("partial credit drive"),
                                                fibers::Wait::Reached
                                            );
                                            fibers::TEST
                                                .try_pause(&mut app, Duration::from_millis(10))
                                                .expect("exhausted sweep is quiescent");
                                            assert_eq!(attempts.hits(), 2);
                                            release.send(()).expect("partial credit observer");
                                        }
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
                    reports.events().expect("retirement owners").is_empty(),
                    "unexpected server retirement: {reports:?}"
                );
                Ok::<_, Infallible>(())
            },
            move |address| {
                if cancel {
                    let waiters = Waiters::try_pair(address).expect("zero-window requests");
                    ready
                        .recv_timeout(Duration::from_secs(2))
                        .expect("two blocked producers");
                    let waiter = waiters.try_cancel().expect("cancel held response");
                    ready
                        .recv_timeout(Duration::from_secs(2))
                        .expect("waiter resumed before window release");
                    let [(body, status)] = waiter.try_release().expect("resumed response");
                    assert_eq!(status.expect("complete response").code(), Code::Ok);
                    assert_eq!(
                        Bytes::try_messages(&body).expect("ready message"),
                        [vec![1; 4]]
                    );
                    return;
                }
                let mut waiters = Waiters::try_new(address).expect("zero-window requests");
                ready
                    .recv_timeout(Duration::from_secs(2))
                    .expect("three blocked producers");
                waiters.try_partial().expect("one byte of real credit");
                ready
                    .recv_timeout(Duration::from_secs(2))
                    .expect("one retry per blocked producer");
                let [(held, first), (empty, second), (ready, third)] =
                    waiters.try_release().expect("waiter responses");
                assert!(
                    [first, second, third]
                        .into_iter()
                        .all(|status| status.expect("complete response").code() == Code::Ok)
                );
                assert_eq!(
                    Bytes::try_messages(&held).expect("held message"),
                    [vec![0; 4]]
                );
                assert!(empty.is_empty());
                assert_eq!(
                    Bytes::try_messages(&ready).expect("ready message"),
                    [vec![1; 4]]
                );
            },
        )
        .expect("waiter progress");
    }
}
