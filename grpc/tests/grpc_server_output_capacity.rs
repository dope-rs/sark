use dope::core::driver::life::recovery;
pub mod common;

use std::{convert::Infallible, time::Duration};

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
    core::{
        driver::route::{FrameworkTag, KeyTag, Tag, kind},
        io::socket::Listener,
    },
    manifold::timing,
    runtime::{executor::session, topology::lending::Stage},
};
use dope_fiber::abi::race::Either;
use dope_test::{
    fibers,
    harness::{
        scenario::process::Deadline,
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
                    let mut waiters = Waiters::try_pair(address).expect("paired capacity requests");
                    waiters.try_window(64).expect("response credit");
                    let [(failed_body, failed), (ready_body, ready)] =
                        waiters.try_read().expect("paired capacity responses");
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
        Deadline::with(Duration::from_secs(30), |_| {
            let listener = Listener::bind(([127, 0, 0, 1], 0).into()).expect("waiter server");
            let address = listener.local_addr();
            let limits = Limits::default()
                .with_output(Output::try_new(1, 9).expect("one response capacity"));
            let settings = Settings::new(address, limits);
            let counts = LocalGate::new();
            let blocked = counts.handle();
            let resumed = LocalGate::new();
            let resumed = resumed.handle();
            let attempts = LocalGate::new();
            let attempts = attempts.handle();
            let completed = LocalGate::new();
            let completed = completed.handle();
            let handler = Capacity {
                blocked: Some(blocked),
                resumed: Some(resumed),
                attempts: Some(attempts),
                completed: Some(completed),
            };
            let bank =
                Bank::try_new(settings.connections.count(), settings.limits).expect("server bank");
            let prepared = settings
                .h2::<KeyTag<0>>()
                .expect("H2 plan")
                .with_bound(listener)
                .with_plain::<_, modes::Serve<Infallible>>(Plan::new(handler, &bank, mode::Direct))
                .for_install::<timing::Throughput>()
                .expect("server resources");
            let mut reports =
                Reports::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
            let mut finish = session::Completion::new();
            let waiters = prepared.with_local(
                |worker| {
                    worker.lend(|plan, stage| {
                        plan.install(stage, |app, stage| {
                            let (app, wait) = app.into_parts();
                            let result = Stage::with_app(stage, &mut finish, app, |mut app| {
                                let waiters = if cancel {
                                    Either::Left(
                                        Waiters::try_pair(address).expect("zero-window requests"),
                                    )
                                } else {
                                    Either::Right(
                                        Waiters::try_new(address).expect("zero-window requests"),
                                    )
                                };
                                let want = if cancel { 2 } else { 3 };
                                assert_eq!(
                                    fibers::Harness::new(Duration::from_secs(2))
                                        .try_wait(&mut app, &blocked, want)
                                        .expect("progress drive"),
                                    fibers::Wait::Reached
                                );
                                assert_eq!(blocked.hits(), want);
                                let waiters = match waiters {
                                    Either::Left(waiters) => {
                                        let mut waiter =
                                            waiters.try_cancel().expect("cancel held response");
                                        assert_eq!(
                                            fibers::Harness::new(Duration::from_secs(2))
                                                .try_wait(&mut app, &resumed, 1)
                                                .expect("cancelled capacity drive"),
                                            fibers::Wait::Reached
                                        );
                                        assert_eq!(attempts.hits(), 2);
                                        waiter.try_window(64).expect("response credit");
                                        Either::Left(waiter)
                                    }
                                    Either::Right(mut waiters) => {
                                        waiters.try_window(1).expect("one byte of real credit");
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
                                        waiters.try_window(64).expect("response credit");
                                        Either::Right(waiters)
                                    }
                                };
                                let want = if cancel { 1 } else { 3 };
                                assert_eq!(
                                    fibers::Harness::new(Duration::from_secs(2))
                                        .try_wait(&mut app, &completed, want)
                                        .expect("responses complete before cleanup"),
                                    fibers::Wait::Reached
                                );
                                assert_eq!(completed.hits(), want);
                                waiters
                            });
                            assert!(wait.watch().error().is_none(), "listener accept");
                            result
                        })
                    })
                },
                recovery::Policy::new(|report| reports.push(report), common::recover),
                |result| match result {
                    Ok(Ok(Ok(waiters))) => waiters,
                    Ok(Ok(Err(error))) => panic!("gRPC session: {:?}", error.cause()),
                    Ok(Err(error)) => panic!("gRPC install: {error:?}"),
                    Err(error) => panic!(
                        "gRPC runtime: {:?}",
                        error.kind().map(|output| {
                            output.as_ref().map(|session| {
                                session.as_ref().map(|_| ()).map_err(session::Error::cause)
                            })
                        })
                    ),
                },
            );
            assert!(
                reports.events().expect("retirement owners").is_empty(),
                "unexpected server retirement: {reports:?}"
            );
            match waiters {
                Either::Left(waiter) => {
                    let [(body, status)] = waiter.try_read().expect("resumed response");
                    assert_eq!(status.expect("complete response").code(), Code::Ok);
                    assert_eq!(
                        Bytes::try_messages(&body).expect("ready message"),
                        [vec![1; 4]]
                    );
                }
                Either::Right(waiters) => {
                    let [(held, first), (empty, second), (ready, third)] =
                        waiters.try_read().expect("waiter responses");
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
                }
            }
        })
        .expect("waiter deadline");
    }
}
