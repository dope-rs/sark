use std::{cell, convert, io::Read, marker::PhantomPinned, net::TcpListener, rc};

use common::{
    app::{self, meter::Meter},
    client::{Probe, topology},
};
use dope::{
    core::{
        driver::{
            life::recovery,
            route::{FrameworkTag, Tag, kind},
        },
        io::{Reclaimed, socket::Backlog},
    },
    manifold::{app::env::quota, timing},
    runtime::{
        executor::session::Completion,
        topology::{Plan, lending},
    },
};
use dope_test::harness::scenario::run::{Entry, Task as _};
use dope_test::{alloc::Tracker, harness::server::Pair, reclaim};
use sark_ws::{
    client,
    client::{channel, headers, mode},
    server,
};

use crate::common::app::Reply;

pub mod common;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn opening_has_one_owner() {
    let mut retired = Vec::new();
    {
        let sock = TcpListener::bind("127.0.0.1:0").expect("bind allocation probe");
        let addr = sock.local_addr().expect("allocation probe address");
        let (proof, request_len) = Entry(move || {
            let (mut stream, _) = sock.accept().expect("accept allocation probe");
            let req = app::Upgrade::new(&stream)
                .try_head()
                .expect("WebSocket operation");
            app::Upgrade::new(&stream)
                .try_respond(
                    &app::Head::new(&req)
                        .try_accept()
                        .expect("WebSocket operation"),
                )
                .expect("WebSocket operation");
            while stream.read(&mut [0; 64]).expect("client close") != 0 {}
            req.len()
        })
        .with(|| {
            let proof = rc::Rc::new(cell::Cell::new(None));
            let sink = rc::Rc::clone(&proof);
            let auth = headers::Field::new("Authorization", String::from("Bearer token"))
                .expect("valid borrowed authorization field");
            let config = client::Config::new("127.0.0.1", "/ws");
            let config = config.expect("WebSocket configuration");
            let opened = topology::Plan::new(addr, config, 1, 16).enter(
                move |domain| {
                    domain.run(
                        channel::App::<_, mode::Observe>::new(Meter { proof: sink, auth }),
                        |mut app| {
                            app.wait()?
                                .as_ref()
                                .map_err(|cause| cause.cause())
                                .expect("WebSocket wait");
                            Ok::<_, topology::WaitError>(())
                        },
                    )
                },
                recovery::Policy::new(|event| retired.push(event), common::recover),
                |result| result.expect("WebSocket execution"),
            );
            opened
                .as_ref()
                .expect("WebSocket operation")
                .as_ref()
                .expect("WebSocket operation");
            proof.take().expect("handshake allocation proof")
        })
        .expect("allocation probe server");

        assert_eq!(proof.count, 1);
        assert_eq!(proof.len, request_len);
        assert!(proof.opened);
        assert_eq!(proof.messages, [0; 4]);
        assert!(proof.close.is_some());
    };
    assert!(
        retired
            .iter()
            .all(|event| matches!(event, Reclaimed::Shutdown)),
        "unexpected reports WebSocket client completion: {retired:?}"
    );
}

#[test]
fn codec_round_trip() {
    {
        let rig = Pair::bind().expect("harness");
        let addr = rig.addr();
        let cfg = server::Config {
            bind: addr,
            max_connections: 1024,
            per_ip: Some(quota::PerIp::fixed::<512>()),
            backlog: Backlog::fixed::<4096>(),
            path: "/ws".into(),
            max_frame_payload: 16 * 1024 * 1024,
            max_message_size: 16 * 1024 * 1024,
        };

        rig.run(
            |ctx, sock| {
                let failures = cell::RefCell::new(Vec::new());
                let failed = |error| failures.borrow_mut().push(error);
                let install = server::Plan::new(cfg)
                    .expect("WebSocket listener plan")
                    .with_bound(Some(sock))
                    .with_handler(Reply {
                        text: "pong",
                        _pin: PhantomPinned,
                        failed: &failed,
                    });
                let prepared = Plan::new((), install)
                    .for_install::<timing::Throughput>()
                    .expect("WebSocket layout");
                let mut reports = reclaim::Reports::new(
                    prepared.layout(),
                    [<FrameworkTag<kind::Any> as Tag>::ROUTE],
                );
                prepared.enter_with(
                    ctx,
                    |worker| {
                        let mut finish = Completion::new();
                        let result = worker.with(lending::Clean(&mut finish));
                        result
                            .expect("WebSocket installation")
                            .expect("WebSocket session");
                    },
                    recovery::Policy::new(|event| reports.push(event), common::recover),
                    |outcome| {
                        outcome
                            .as_ref()
                            .map_err(|error| error.cause())
                            .expect("WebSocket topology");
                    },
                );
                assert!(
                    failures.borrow().is_empty(),
                    "WebSocket reply: {:?}",
                    failures.borrow()
                );
                assert!(
                    reports.events().expect("valid reclamation").is_empty(),
                    "unexpected reports server operations: {reports:?}"
                );
                Ok::<(), convert::Infallible>(())
            },
            |addr| {
                let mut retired = Vec::new();
                let config = client::Config::new("127.0.0.1", "/ws");
                let config = config.expect("WebSocket configuration");
                let seen = Probe::new(addr, config).run(
                    common::client::Goal::Echo("ping"),
                    recovery::Policy::new(|event| retired.push(event), common::recover),
                );
                assert_eq!(seen.reply(), Some("pong"));
                assert!(
                    retired
                        .iter()
                        .all(|event| matches!(event, Reclaimed::Shutdown)),
                    "unexpected reports WebSocket client completion: {retired:?}"
                );
            },
        )
        .expect("harness");
    };
}
