pub mod common;

use std::{array, cell::Cell, net::SocketAddr, task::Poll, time::Duration};

use common::h2::recover;
use common::{
    h2::{
        apps::{DUPLEX_LAYOUT, H2TaskApp, LARGE_JSON, Layout as _},
        events::{Count, Log},
        facts::{GuardProbe, Identity, ROUTE, State},
        peer::{Peer, Sender},
        server::{CONFIG, Request, Server},
    },
    route::keep::events::diagnose,
    server::Plan,
};
use dope::{
    core::{
        driver::{life::recovery, route::KeyTag},
        io::socket::Listener,
    },
    manifold::{app::Host, timing},
    net::wire,
    runtime::{executor::session, topology},
};
use dope_fiber::{
    abi::poll_fn,
    task::{done::Once, storage::scope::Brand},
};
use dope_test::{
    fibers,
    harness::{server::Pair, signals::Progress as _},
};
use sark_core::{deadline, life::mode, manifold::stream::Boundary};
use sark_h2::conn::config::{self, capacity};

#[test]
fn pre_fin_yields() {
    let harness = Pair::bind().expect("H2 harness");
    harness
        .check(
            move |context, listener| {
                Server::new(CONFIG, 8, State::default()).try_run(
                    context,
                    listener,
                    mode::Disabled,
                    recover,
                    |result| result.expect("H2 execution"),
                )
            },
            |address| {
                let (mut socket, mut client) =
                    Peer::try_connect(address, Duration::from_secs(3)).expect("H2 peer connection");
                let large = client
                    .streams()
                    .headers()
                    .request(
                        Request::new(b"/large")
                            .try_headers()
                            .expect("valid request fields"),
                        Boundary::More,
                    )
                    .expect("start large H2 request");
                Sender::new(&mut client)
                    .try_send(large, LARGE_JSON, Boundary::End)
                    .expect("H2 peer send");
                let quick = client
                    .streams()
                    .headers()
                    .request(
                        Request::new(b"/quick")
                            .try_headers()
                            .expect("valid request fields"),
                        Boundary::End,
                    )
                    .expect("start quick H2 request");
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("H2 peer flush");

                let (order, bodies) = Peer::new(&mut socket, &mut client)
                    .try_statuses(&[large, quick], b"200")
                    .expect("H2 peer statuses");
                let bodies = <[Vec<u8>; 2]>::try_from(bodies).expect("two response bodies");
                assert_eq!(bodies[0], b"large");
                assert_eq!(bodies[1], b"quick");
                assert_eq!(
                    order.first(),
                    Some(&1),
                    "cooperative JSON work must not head-of-line block a later stream",
                );
            },
            |result| result.expect("H2 setup"),
        )
        .expect("H2 fairness runtime");
}

#[test]
fn completion_yields_build() {
    const BURST: usize = 64;

    let harness = Pair::bind().expect("H2 harness");
    harness
        .check(
            move |context, listener| {
                Server::new(CONFIG, 128, State::default()).try_run(
                    context,
                    listener,
                    mode::Disabled,
                    recover,
                    |result| result.expect("H2 execution"),
                )
            },
            |address| {
                let (mut task_io, mut task) =
                    Peer::try_connect(address, Duration::from_secs(3)).expect("H2 peer connection");
                let (mut burst_io, mut peer) =
                    Peer::try_connect(address, Duration::from_secs(3)).expect("H2 peer connection");

                let build = task
                    .streams()
                    .headers()
                    .request(
                        Request::new(b"/large")
                            .try_headers()
                            .expect("valid request fields"),
                        Boundary::More,
                    )
                    .expect("start cooperative H2 build");
                Sender::new(&mut task)
                    .try_send(build, LARGE_JSON, Boundary::End)
                    .expect("H2 peer send");
                Peer::new(&mut task_io, &mut task)
                    .try_flush()
                    .expect("H2 peer flush");

                let mut burst = Vec::with_capacity(BURST);
                for _ in 0..BURST {
                    burst.push(
                        peer.streams()
                            .headers()
                            .request(
                                Request::new(b"/quick")
                                    .try_headers()
                                    .expect("valid request fields"),
                                Boundary::End,
                            )
                            .expect("start H2 completion burst member"),
                    );
                }
                Peer::new(&mut burst_io, &mut peer)
                    .try_flush()
                    .expect("H2 peer flush");

                let (_, body) = Peer::new(&mut task_io, &mut task)
                    .try_response(build)
                    .expect("H2 peer response");
                assert_eq!(
                    body, b"large",
                    "completion delivery on another owner must not starve request construction",
                );

                let (_, bodies) = Peer::new(&mut burst_io, &mut peer)
                    .try_statuses(&burst, b"200")
                    .expect("H2 peer statuses");
                assert_eq!(bodies.len(), BURST);
                assert!(bodies.iter().all(|body| body == b"quick"));
            },
            |result| result.expect("H2 setup"),
        )
        .expect("H2 completion/build fairness runtime");
}

#[test]
fn close_cancels_once() {
    let listener = Listener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("listener");
    let address = listener.local_addr();
    let server = Server::new(CONFIG, 2, State::default());
    let plan = server.try_plan(address).expect("plan");
    let mut reports = Plan::try_reports(&plan).expect("claims");
    let prepared = topology::Plan::new((), plan.with_bound(listener))
        .for_receive::<timing::Throughput>()
        .expect("resources");
    let facts = Cell::new(Vec::new());
    let mut done = session::Completion::new();
    prepared.enter_with(
        (),
        |worker| {
            worker.lend(|plan, stage| {
                Brand::with(|brand| {
                    let router = H2TaskApp::multiplexed::<wire::Identity, KeyTag<ROUTE>, _>(
                        brand,
                        &server.state,
                        sark_fiber::Config::new(2),
                    );
                    server.state.layout.set(Some(router.layout()));
                    plan.tasks(router, 2)
                        .observe(Log(&facts, diagnose))
                        .try_install(stage, |listener, stage| {
                            stage
                                .with_quiesce(
                                    &mut done,
                                    Host::new(listener),
                                    Duration::from_secs(3),
                                    |mut app| {
                                        let (mut socket, mut client) =
                                            Peer::try_connect(address, Duration::from_secs(3))
                                                .expect("H2 peer connection");
                                        let first = client
                                            .streams()
                                            .headers()
                                            .request(
                                                Request::new(b"/blocked")
                                                    .try_headers()
                                                    .expect("fields"),
                                                Boundary::End,
                                            )
                                            .expect("first task");
                                        let second = client
                                            .streams()
                                            .headers()
                                            .request(
                                                Request::new(b"/blocked")
                                                    .try_headers()
                                                    .expect("fields"),
                                                Boundary::End,
                                            )
                                            .expect("second task");
                                        assert_ne!(first, second);
                                        Peer::new(&mut socket, &mut client)
                                            .try_flush()
                                            .expect("H2 peer flush");
                                        assert_eq!(
                                            fibers::TEST
                                                .try_wait(
                                                    &mut app,
                                                    &Count {
                                                        facts: &facts,
                                                        event: "task-admitted",
                                                    },
                                                    2,
                                                )
                                                .expect("H2 progress"),
                                            fibers::Wait::Reached
                                        );
                                        assert_eq!(
                                            Count {
                                                facts: &facts,
                                                event: "task-admitted"
                                            }
                                            .hits(),
                                            2
                                        );
                                        drop(socket);
                                        drop(client);
                                        assert_eq!(
                                            fibers::TEST
                                                .try_wait(
                                                    &mut app,
                                                    &Count {
                                                        facts: &facts,
                                                        event: "task-cancelled",
                                                    },
                                                    2,
                                                )
                                                .expect("H2 progress"),
                                            fibers::Wait::Reached
                                        );
                                        let observed = facts.take();
                                        let mut cancels = observed
                                            .iter()
                                            .filter_map(|&(kind, stream)| {
                                                (kind == "task-cancelled")
                                                    .then_some(Identity::from(stream).wire())
                                            })
                                            .collect::<Vec<_>>();
                                        facts.set(observed);
                                        cancels.sort_unstable();
                                        let mut expected = [
                                            u64::from(first.into_u32()),
                                            u64::from(second.into_u32()),
                                        ];
                                        expected.sort_unstable();
                                        assert_eq!(cancels, expected);
                                    },
                                )
                                .expect("runtime");
                        })
                        .expect("install");
                })
            })
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        |result| {
            result
                .as_ref()
                .map_err(|error| error.cause())
                .expect("H2 topology");
        },
    );
    assert_eq!(server.state.observed(), (false, None, Some(DUPLEX_LAYOUT)));
    assert!(
        reports.events().expect("H2 reclaim ownership").is_empty(),
        "retirement: {reports:?}"
    );
}

#[test]
fn burst_wakes_siblings() {
    const TASKS: usize = 5;
    let listener = Listener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("listener");
    let address = listener.local_addr();
    let server = Server::new(CONFIG, TASKS, State::default());
    let plan = server.try_plan(address).expect("plan");
    let mut reports = Plan::try_reports(&plan).expect("claims");
    let prepared = topology::Plan::new((), plan.with_bound(listener))
        .for_receive::<timing::Throughput>()
        .expect("resources");
    let facts = Cell::new(Vec::new());
    let mut done = session::Completion::new();
    prepared.enter_with(
        (),
        |worker| {
            worker.lend(|plan, stage| {
                Brand::with(|brand| {
                    let router = H2TaskApp::multiplexed::<wire::Identity, KeyTag<ROUTE>, _>(
                        brand,
                        &server.state,
                        sark_fiber::Config::new(TASKS),
                    );
                    server.state.layout.set(Some(router.layout()));
                    plan.tasks(router, TASKS)
                        .observe(Log(&facts, diagnose))
                        .try_install(stage, |listener, stage| {
                            let start = stage.driver().driver_ref().clock().now().into();
                            stage
                                .with_at_for(
                                    &mut done,
                                    Host::new(listener),
                                    start,
                                    Duration::from_secs(3),
                                    |mut app| {
                                        let (mut socket, mut client) =
                                            Peer::try_connect(address, Duration::from_secs(3))
                                                .expect("H2 peer connection");
                                        for _ in 0..TASKS {
                                            client
                                                .streams()
                                                .headers()
                                                .request(
                                                    Request::new(b"/parked")
                                                        .try_headers()
                                                        .expect("fields"),
                                                    Boundary::End,
                                                )
                                                .expect("parked task");
                                        }
                                        Peer::new(&mut socket, &mut client)
                                            .try_flush()
                                            .expect("H2 peer flush");
                                        assert!(client.output().is_empty());
                                        let pending = Count {
                                            facts: &facts,
                                            event: "task-poll-pending",
                                        };
                                        app.drive_for(
                                            start,
                                            Duration::from_secs(3),
                                            Once::new(poll_fn(|context| {
                                                if pending.hits() < TASKS as u32 {
                                                    context.wake();
                                                    return Poll::Pending;
                                                }
                                                Poll::Ready(())
                                            })),
                                        )
                                        .expect("first waits")
                                        .into_output();
                                        assert_eq!(pending.hits(), TASKS as u32);
                                        let next = start
                                            .try_add(Duration::from_secs(1))
                                            .expect("second wait time");
                                        let mut yielded = false;
                                        let snapshot = app
                                            .drive_for(
                                                next,
                                                Duration::from_secs(3),
                                                Once::new(poll_fn(|context| {
                                                    if !yielded {
                                                        yielded = true;
                                                        context.wake();
                                                        return Poll::Pending;
                                                    }
                                                    Poll::Ready(pending.hits() - TASKS as u32)
                                                })),
                                            )
                                            .expect("second waits")
                                            .into_output();
                                        assert_eq!(snapshot, TASKS as u32);
                                        drop(socket);
                                        drop(client);
                                        let cancels = Count {
                                            facts: &facts,
                                            event: "task-cancelled",
                                        };
                                        app.drive_for(
                                            next,
                                            Duration::from_secs(3),
                                            Once::new(poll_fn(|context| {
                                                if cancels.hits() < TASKS as u32 {
                                                    context.wake();
                                                    return Poll::Pending;
                                                }
                                                Poll::Ready(())
                                            })),
                                        )
                                        .expect("cancelled waits")
                                        .into_output();
                                        assert_eq!(cancels.hits(), TASKS as u32);
                                    },
                                )
                                .expect("runtime");
                        })
                        .expect("install");
                })
            })
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        |result| {
            result
                .as_ref()
                .map_err(|error| error.cause())
                .expect("H2 topology");
        },
    );
    assert_eq!(server.state.observed(), (false, None, Some(DUPLEX_LAYOUT)));
    assert!(
        reports.events().expect("H2 reclaim ownership").is_empty(),
        "retirement: {reports:?}"
    );
}

#[test]
fn renewals_precede_ready_tasks() {
    // One renewal must survive the listener's bounded maintenance turn.
    const STREAMS: usize = dope::core::driver::schedule::LIMIT + 1;
    let listener = Listener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("listener");
    let address = listener.local_addr();
    let protocol = config::Config {
        stream_capacity: STREAMS + 1,
        payload_capacity: capacity::Payload::new(STREAMS, STREAMS + 1)
            .expect("retained body owners"),
        ..Default::default()
    };
    let server = Server::new(CONFIG, 1, State::default()).with_config(protocol);
    let plan = server.try_plan(address).expect("plan");
    let mut reports = Plan::try_reports(&plan).expect("claims");
    let prepared = topology::Plan::new((), plan.with_bound(listener))
        .for_receive::<timing::Throughput>()
        .expect("resources");
    let facts = Cell::new(Vec::new());
    let mut done = session::Completion::new();
    prepared.enter_with(
        (),
        |worker| {
            worker.lend(|plan, stage| {
                Brand::with(|brand| {
                    let router = H2TaskApp::multiplexed::<wire::Identity, KeyTag<ROUTE>, _>(
                        brand,
                        &server.state,
                        sark_fiber::Config::new(1),
                    );
                    server.state.layout.set(Some(router.layout()));
                    let timer = deadline::config::Config::try_new(
                        deadline::Phase::BodyIdle,
                        Duration::from_secs(1),
                    )
                    .expect("body deadline")
                    .try_with(deadline::Phase::Handler, Duration::from_secs(1))
                    .expect("handler deadline");
                    plan.tasks(router, 1)
                        .observe(Log(&facts, diagnose))
                        .try_due(stage, timer, |listener, stage| {
                            let start = stage.driver().driver_ref().clock().now().into();
                            stage
                                .with_at_for(
                                    &mut done,
                                    Host::new(listener),
                                    start,
                                    Duration::from_secs(3),
                                    |mut app| {
                                        let mut socket =
                                            Peer::try_open(address, Duration::from_secs(3))
                                                .expect("H2 peer connection");
                                        let mut client = sark_h2::conn::machine::Connection::<
                                            sark_h2::ClientRole,
                                        >::with_config(
                                            protocol
                                        )
                                        .expect("H2 peer capacity");
                                        let streams = array::from_fn::<_, STREAMS, _>(|_| {
                                            client
                                                .streams()
                                                .headers()
                                                .request(
                                                    Request::new(b"/segmented")
                                                        .try_headers()
                                                        .expect("fields"),
                                                    Boundary::More,
                                                )
                                                .expect("body stream")
                                        });
                                        Peer::new(&mut socket, &mut client)
                                            .try_flush()
                                            .expect("body heads");
                                        let heads = Count {
                                            facts: &facts,
                                            event: "head-admitted",
                                        };
                                        app.drive_for(
                                            start,
                                            Duration::from_secs(3),
                                            Once::new(poll_fn(|context| {
                                                if heads.hits() < STREAMS as u32 {
                                                    context.wake();
                                                    return Poll::Pending;
                                                }
                                                Poll::Ready(())
                                            })),
                                        )
                                        .expect("body admissions")
                                        .into_output();

                                        let parked = client
                                            .streams()
                                            .headers()
                                            .request(
                                                Request::new(b"/parked")
                                                    .try_headers()
                                                    .expect("fields"),
                                                Boundary::End,
                                            )
                                            .expect("parked task");
                                        Peer::new(&mut socket, &mut client)
                                            .try_flush()
                                            .expect("parked request");
                                        let handler = start
                                            .try_add(Duration::from_millis(250))
                                            .expect("handler start time");
                                        let pending = Count {
                                            facts: &facts,
                                            event: "task-poll-pending",
                                        };
                                        app.drive_for(
                                            handler,
                                            Duration::from_secs(3),
                                            Once::new(poll_fn(|context| {
                                                if pending.hits() == 0 {
                                                    context.wake();
                                                    return Poll::Pending;
                                                }
                                                Poll::Ready(())
                                            })),
                                        )
                                        .expect("registered task timer")
                                        .into_output();

                                        for stream in streams {
                                            Sender::new(&mut client)
                                                .try_send(stream, b"first-", Boundary::More)
                                                .expect("timely body progress");
                                        }
                                        Peer::new(&mut socket, &mut client)
                                            .try_flush()
                                            .expect("partial body");
                                        let partial = start
                                            .try_add(Duration::from_millis(500))
                                            .expect("partial body time");
                                        app.drive_for(
                                            partial,
                                            Duration::from_secs(3),
                                            Once::new(poll_fn(|context| {
                                                let observed = facts.take();
                                                let ready = streams.iter().all(|stream| {
                                                    observed.iter().any(|&(kind, identity)| {
                                                        kind == "pre-admission-reserved"
                                                            && Identity::from(identity).wire()
                                                                == u64::from(stream.into_u32())
                                                    })
                                                });
                                                facts.set(observed);
                                                if !ready {
                                                    context.wake();
                                                    return Poll::Pending;
                                                }
                                                Poll::Ready(())
                                            })),
                                        )
                                        .expect("lazy body renewal")
                                        .into_output();

                                        let due = handler
                                            .try_add(Duration::from_secs(1))
                                            .expect("handler expiry time");
                                        let cancelled = Count {
                                            facts: &facts,
                                            event: "task-cancelled",
                                        };
                                        app.drive_for(
                                            due,
                                            Duration::from_secs(3),
                                            Once::new(poll_fn(|context| {
                                                if cancelled.hits() == 0 {
                                                    context.wake();
                                                    return Poll::Pending;
                                                }
                                                Poll::Ready(())
                                            })),
                                        )
                                        .expect("expiry before task repoll")
                                        .into_output();
                                        assert_eq!(pending.hits(), 1);
                                        let observed = facts.take();
                                        let expired = observed
                                            .iter()
                                            .filter_map(|&(kind, stream)| {
                                                (kind == "exchange-deadline-expired")
                                                    .then_some(Identity::from(stream).wire())
                                            })
                                            .collect::<Vec<_>>();
                                        assert_eq!(expired, [u64::from(parked.into_u32())]);
                                    },
                                )
                                .expect("runtime");
                        })
                        .expect("install");
                })
            })
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        |result| {
            result
                .as_ref()
                .map_err(|error| error.cause())
                .expect("H2 topology");
        },
    );
    assert_eq!(server.state.observed(), (false, None, Some(DUPLEX_LAYOUT)));
    assert!(
        reports.events().expect("H2 reclaim ownership").is_empty(),
        "retirement: {reports:?}"
    );
}

#[test]
fn deferred_guard_resumes() {
    let mut config = CONFIG;
    config.max_outbound_bytes = 1;
    Pair::bind()
        .expect("H2 harness")
        .check(
            move |context, listener| {
                let state = State::with_guard(GuardProbe {
                    padded: true,
                    ..GuardProbe::default()
                });
                let server = Server::new(config, 1, state).with_config(config::Config {
                    hpack_encoder: capacity::Encoder {
                        header_list: 9 << 10,
                        ..Default::default()
                    },
                    ..Default::default()
                });
                server
                    .try_run(context, listener, mode::Disabled, recover, |result| {
                        result.expect("H2 execution")
                    })
                    .map(|()| {
                        let guard = server.state.guard.as_ref().expect("guard fixture");
                        (guard.checks.get(), guard.handlers.get())
                    })
            },
            |address| {
                let (mut socket, mut client) =
                    Peer::try_connect(address, Duration::from_secs(3)).expect("H2 peer");
                let streams = array::from_fn::<_, 4, _>(|_| {
                    client
                        .streams()
                        .headers()
                        .request(
                            Request::new(b"/guarded/body")
                                .try_headers()
                                .expect("request fields"),
                            Boundary::End,
                        )
                        .expect("guarded request")
                });
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("guarded request burst");
                let (_, bodies) = Peer::new(&mut socket, &mut client)
                    .try_statuses(&streams, b"401")
                    .expect("every deferred guard resumes after egress release");
                assert!(bodies.iter().all(|body| body == b"guarded"));
            },
            |result| assert_eq!(result.expect("H2 setup"), (4, 0)),
        )
        .expect("deferred guard runtime");
}
