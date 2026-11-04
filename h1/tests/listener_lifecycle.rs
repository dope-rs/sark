pub mod common;

use std::{cell::Cell, net::Shutdown, ptr, rc::Rc};

use common::{
    app::{App, Event, Facts, PREFIX},
    session::handoffs,
};
use dope::{
    core::{
        driver::{
            life::recovery::Policy,
            route::{FrameworkTag, KeyTag, Tag, kind, table::conn::Capacity},
        },
        io::socket::{Backlog, option},
        platform::wake,
    },
    manifold::{
        app::{Host, env::Bundle},
        network::listener::{
            Listener, config, conn,
            conn::write::{self, body},
            topology::Direct,
        },
        timing::Throughput,
    },
    net::{tcp, wire::Identity},
    runtime::topology,
};
use dope_test::{
    fibers,
    harness::{
        GUARD,
        peer::Peer,
        signals::{Gate, Progress as _},
    },
    reclaim,
};
use sark_h1::{
    pump,
    server::{conn::driver::Flow, date::Clock},
    switch,
    wire::head,
};
use sark_http::h1::switch::Kind;

#[test]
fn closes_and_retries() {
    for (early, flights, reject) in [
        (false, 1, false),
        (true, 1, false),
        (false, 0, false),
        (true, 0, false),
        (false, 1, true),
        (false, 0, true),
    ] {
        type Env = Bundle<tcp::Tcp, Identity, Throughput>;
        let facts = Facts::default();
        let clock = Clock::new().expect("clock");
        let reason =
            b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec();
        let checks = Cell::new([0; 7]);
        let observe = |event: Event<'_>| {
            assert!(
                !matches!(event, Event::Rejected(_) | Event::Input(_)),
                "unexpected rejection or input callback"
            );
            let index = match event {
                Event::Progress(Flow::Continue, actual, closing) => {
                    assert!(ptr::eq(actual.expect("deferred reason"), reason.as_slice()));
                    assert_eq!(closing, early);
                    0
                }
                Event::Progress(Flow::Closed, actual, closing) => {
                    assert!(actual.is_none());
                    assert!(closing);
                    1
                }
                Event::Close(actual) => {
                    assert!(actual.is_none());
                    2
                }
                Event::Stopped => {
                    assert!(facts.failure.get().is_some());
                    3
                }
                Event::Charge(charge, previous) => {
                    assert!(!charge.is_empty());
                    assert!(previous.is_none());
                    4
                }
                Event::Cancelled => {
                    assert!(facts.failure.get().is_some());
                    5
                }
                Event::Retired(failure, response) => {
                    match facts.failure.get() {
                        None => assert!(failure.is_none()),
                        Some(expected) => {
                            let Some(conn::Failure::Write(body::Failure::Bound {
                                charge,
                                maximum,
                            })) = failure
                            else {
                                panic!("exact owner bound failure must reach final retirement");
                            };
                            assert_eq!(charge.bytes().get(), expected.logical_bytes);
                            assert_eq!(charge.resident().get(), expected.resident_bytes);
                            assert_eq!(*maximum, 0);
                            assert_eq!(facts.rejected.get(), 1);
                        }
                    }
                    assert_eq!(response, 0);
                    6
                }
                Event::Rejected(_) | Event::Input(_) => return,
            };
            let mut counts = checks.get();
            counts[index] += 1;
            checks.set(counts);
        };
        let setup = Direct::<KeyTag<0>, Env, _>::new(
            config::Prepared {
                connections: Capacity::fixed::<2>(),
                per_ip: None,
                direct_flights: flights.into(),
                bind: ([127, 0, 0, 1], 0).into(),
                backlog: Backlog::fixed::<8>(),
                stream: Default::default(),
                transport: Default::default(),
                egress: Default::default(),
            },
            App {
                reason: &reason,
                facts: &facts,
                close_early: early,
                clock: &clock,
                observe: &observe,
            },
        );
        let prepared = topology::Plan::new((), setup)
            .for_install::<Throughput>()
            .expect("listener operation");
        let mut reports = reclaim::Reports::<1>::new(
            prepared.layout(),
            [<FrameworkTag<kind::Any> as Tag>::ROUTE],
        );
        let mut done = Default::default();
        let (prepared, completion, timeout) = (prepared, &mut done, GUARD);
        prepared.with_local(
            |worker| {
                worker.lend(|setup, stage| {
                    setup.install(stage, |root, stage| {
                        topology::lending::Stage::with_quiesce(
                            stage,
                            completion,
                            root,
                            timeout,
                            |mut app| {
                                let addr = app.inspect(Host::manifold, |server| {
                                    Listener::local_addr(server.get_ref())
                                });
                                Peer::at(addr)
                                    .with(
                                        |socket| {
                                            socket
                                                .write_all(if reject { b"C" } else { b"AB" })
                                                .expect("listener operation");
                                            socket
                                                .shutdown(Shutdown::Write)
                                                .expect("listener operation");
                                            let mut reply = Vec::new();
                                            socket
                                                .read_to_end(&mut reply)
                                                .expect("listener operation");
                                            reply
                                        },
                                        |peer| {
                                            assert_eq!(
                                                fibers::TEST
                                                    .try_wait(&mut app, &facts.closed, 1)
                                                    .expect("listener progress"),
                                                fibers::Wait::Reached
                                            );
                                            let reply = peer.finish().expect("listener operation");
                                            if reject {
                                                assert!(reply.is_empty());
                                            } else {
                                                assert_eq!(&reply[..PREFIX.len()], PREFIX);
                                                assert_eq!(&reply[PREFIX.len()..], reason);
                                            }
                                        },
                                    )
                                    .expect("listener operation");
                            },
                        )
                    })
                })
            },
            Policy::new(|report| reports.push(report), common::recover),
            |result| {
                let installed = result
                    .as_ref()
                    .map_err(topology::EnterError::cause)
                    .expect("listener topology");
                let session = installed.as_ref().expect("listener installation");
                session
                    .as_ref()
                    .map_err(|error| error.cause())
                    .expect("listener session");
            },
        );
        assert_eq!(facts.routed.get(), 1);
        assert_eq!(facts.rejected.get(), 1);
        if reject {
            assert_eq!(facts.permits.get(), 0);
            assert_eq!(facts.deferred.get(), 0);
            assert_eq!(facts.committed.get(), 0);
            assert!(facts.failure.get().is_some());
        } else {
            assert!(facts.permits.get() > 0);
            assert!(facts.deferred.get() > 0);
            assert_eq!(facts.committed.get(), 1);
            assert!(facts.failure.get().is_none());
        }
        assert_eq!(facts.released.get(), 1);
        assert_eq!(facts.closed.hits(), 1);
        assert_eq!(
            checks.get(),
            [
                facts.deferred.get(),
                facts.committed.get(),
                1,
                usize::from(reject),
                usize::from(reject),
                usize::from(reject),
                1,
            ]
        );
        assert!(
            reports.events().expect("valid reclamation").is_empty(),
            "{reports:?}"
        );
    }
}

#[test]
fn switch_moves_tail_once() {
    type Env = Bundle<tcp::Tcp, Identity, Throughput>;

    let sent = Gate::new();
    let moved = Gate::new();
    let closed = Gate::new();
    let raw = Rc::new(Cell::new(Vec::new()));
    let owner = Rc::new(Cell::new(None));
    let checks = Cell::new([0; 9]);
    let observe = |event: handoffs::Event<'_, '_>| {
        assert!(
            !matches!(event, handoffs::Event::Preflight(_)),
            "unexpected preflight failure"
        );
        let index = match event {
            handoffs::Event::Switch(mode, input) => {
                assert_eq!(mode, switch::Mode::Raw(Kind::Connect));
                assert!(input.is_none(), "affine tail stayed out of HTTP input");
                0
            }
            handoffs::Event::Raw(staged) => {
                assert!(staged.is_ok(), "drain must commit the raw owner");
                1
            }
            handoffs::Event::Owner(actual) => {
                assert_eq!(actual, 7);
                2
            }
            handoffs::Event::Cycle(cycle) => {
                assert!(matches!(cycle.connection, head::Connection::KeepAlive));
                assert!(
                    matches!(
                        &cycle.state,
                        Ok(pump::Egress::Write {
                            body: pump::Body::Static(_),
                            ..
                        })
                    ),
                    "switch must publish its exact static response owner"
                );
                3
            }
            handoffs::Event::Commit(commit) => {
                let Ok(write::Outcome::Committed(receipt)) = commit else {
                    panic!("switching response must commit exactly once");
                };
                assert!(
                    receipt.state.is_ok(),
                    "switch response committed its protocol state"
                );
                assert!(
                    matches!(&receipt.tail, pump::Tail::Handoff { .. }),
                    "switching response must carry its exact suffix"
                );
                4
            }
            handoffs::Event::Retained(tail) => {
                assert!(tail.is_ok(), "retain tail: {:?}", tail.as_ref().err());
                5
            }
            handoffs::Event::Restricted(result) => {
                assert!(result.is_ok(), "restrict tail: {result:?}");
                6
            }
            handoffs::Event::Staged(staged) => {
                assert!(staged.is_ok(), "raw owner must remain staged until drain");
                7
            }
            handoffs::Event::Replaced(previous) => {
                assert!(previous.is_none(), "one affine switch tail");
                8
            }
            handoffs::Event::Preflight(_) => return,
        };
        let mut counts = checks.get();
        counts[index] += 1;
        checks.set(counts);
    };

    let send_buf = (option::Buffer::try_from(1024)).expect("socket send buffer");
    let config = config::Prepared {
        connections: Capacity::fixed::<4>(),
        per_ip: None,
        direct_flights: 1.into(),
        bind: ([127, 0, 0, 1], 0).into(),
        backlog: Backlog::fixed::<128>(),
        stream: tcp::StreamConfig {
            send_buffer_size: Some(send_buf),
            no_delay: Some(option::Switch::On),
            ..Default::default()
        },
        transport: Default::default(),
        egress: Default::default(),
    };
    let setup = Direct::<KeyTag<0>, Env, _>::new(
        config,
        handoffs::App {
            sent: sent.clone(),
            moved: moved.clone(),
            closed: closed.clone(),
            raw: raw.clone(),
            owner: owner.clone(),
            observe: &observe,
        },
    );
    let prepared = topology::Plan::new((), setup)
        .for_install::<Throughput>()
        .expect("handoff listener layout");
    let mut reports =
        reclaim::Reports::<1>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    let mut done = Default::default();
    let (prepared, completion, timeout) = (prepared, &mut done, GUARD);
    prepared.with_local(
        |worker| {
            worker.lend(|setup, stage| {
                setup.install(stage, |root, stage| {
                    topology::lending::Stage::with_quiesce(
                        stage,
                        completion,
                        root,
                        timeout,
                        |mut app| {
                            let addr = app.inspect(Host::manifold, |server| {
                                Listener::local_addr(server.get_ref())
                            });
                            let (wait, release) =
                                wake::Ends::blocking().expect("handoff peer gate").split();
                            Peer::at(addr)
                                .with(
                                    move |socket| {
                                        socket.read_wait(Some(GUARD)).expect("read deadline");
                                        socket.write_wait(Some(GUARD)).expect("write deadline");
                                        socket.write_all(b"Rtail").expect("listener operation");
                                        wait.wait_timeout(GUARD).expect("handoff admission");
                                        socket.write_all(b"later").expect("listener operation");
                                        socket
                                            .shutdown(Shutdown::Write)
                                            .expect("listener operation");
                                        Peer::try_read_all(socket)
                                    },
                                    |peer| {
                                        assert_eq!(
                                            fibers::TEST
                                                .try_wait(&mut app, &sent, 1)
                                                .expect("listener progress"),
                                            fibers::Wait::Reached
                                        );
                                        assert_eq!(
                                            moved.hits(),
                                            0,
                                            "send progress forged a commit"
                                        );
                                        assert_eq!(owner.get(), None);
                                        release.notify().expect("release handoff peer");
                                        assert_eq!(
                                            fibers::TEST
                                                .try_wait(&mut app, &moved, 1)
                                                .expect("listener progress"),
                                            fibers::Wait::Reached
                                        );
                                        assert_eq!(
                                            fibers::TEST
                                                .try_wait(&mut app, &closed, 1)
                                                .expect("listener progress"),
                                            fibers::Wait::Reached
                                        );
                                        let reply = peer
                                            .finish()
                                            .expect("listener operation")
                                            .expect("listener operation");
                                        assert_eq!(
                                            reply.as_slice(),
                                            handoffs::SWITCH_BYTES.as_slice()
                                        );
                                        assert_eq!(owner.get(), Some(7));
                                        assert_eq!(raw.take().as_slice(), b"taillater");
                                    },
                                )
                                .expect("handoff peer")
                        },
                    )
                })
            })
        },
        Policy::new(|report| reports.push(report), common::recover),
        |result| {
            let installed = result
                .as_ref()
                .map_err(topology::EnterError::cause)
                .expect("listener topology");
            let session = installed.as_ref().expect("listener installation");
            session
                .as_ref()
                .map_err(|error| error.cause())
                .expect("listener session");
        },
    );
    assert!(
        reports.events().expect("valid reclamation").is_empty(),
        "unexpected reclaimed server operations: {reports:?}"
    );
    let [
        switched,
        live,
        received,
        cycles,
        commits,
        retained,
        limited,
        staged,
        replaced,
    ] = checks.get();
    assert!(received > 0);
    assert_eq!(
        [
            switched, live, cycles, commits, retained, limited, staged, replaced
        ],
        [1; 8]
    );
}
