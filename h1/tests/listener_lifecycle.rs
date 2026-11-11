pub mod common;

use std::{cell::Cell, net::Shutdown, rc::Rc, time::Instant};

use common::{
    app::{App, Facts, PREFIX},
    session::handoffs,
};
use dope::{
    core::{
        driver::{
            life::recovery::Policy,
            route::{FrameworkTag, KeyTag, Tag, kind, table::conn::Capacity},
        },
        io::socket::{Backlog, option},
    },
    manifold::{
        app::{Host, env::Bundle},
        network::listener::{Listener, config, topology::Direct},
        timing::Throughput,
    },
    net::{tcp, wire::Identity},
    runtime::topology,
};
use dope_test::{
    fibers,
    harness::{
        GUARD,
        peer::{self, Peer},
        signals::{Gate, Progress as _},
    },
    reclaim,
};

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
        let reason =
            b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec();
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
                        let (root, wait) = root.into_parts();
                        let result = topology::lending::Stage::with_quiesce(
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
                        );
                        assert!(wait.watch().error().is_none(), "listener accept");
                        result
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
            facts.checks.each_ref().map(Cell::get),
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
    let checks: [Cell<usize>; 9] = Default::default();

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
            checks: &checks,
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
                    let (root, wait) = root.into_parts();
                    let result = topology::lending::Stage::with_quiesce(
                        stage,
                        completion,
                        root,
                        timeout,
                        |mut app| {
                            let addr = app.inspect(Host::manifold, |server| {
                                Listener::local_addr(server.get_ref())
                            });
                            let socket = Peer::at(addr).try_connect().expect("handoff peer");
                            socket.read_wait(Some(GUARD)).expect("read deadline");
                            socket.write_wait(Some(GUARD)).expect("write deadline");
                            socket.write_all(b"Rtail").expect("listener operation");
                            assert_eq!(
                                fibers::TEST
                                    .try_wait(&mut app, &sent, 1)
                                    .expect("listener progress"),
                                fibers::Wait::Reached
                            );
                            assert_eq!(moved.hits(), 0, "send progress forged a commit");
                            assert_eq!(owner.get(), None);
                            socket.write_all(b"later").expect("listener operation");
                            socket
                                .shutdown(Shutdown::Write)
                                .expect("listener operation");
                            socket.nonblock(true);
                            let deadline = Instant::now() + GUARD;
                            let mut reply = Vec::new();
                            loop {
                                match socket.read_to_end(&mut reply) {
                                    Ok(_) => break,
                                    Err(peer::Error::Syscall(error))
                                        if error.is_block() || error.is_intr() => {}
                                    Err(error) => panic!("listener operation: {error:?}"),
                                }
                                let remaining = deadline
                                    .checked_duration_since(Instant::now())
                                    .expect("listener deadline");
                                fibers::Harness::new(remaining)
                                    .try_run(&mut app, fibers::Yield::new())
                                    .expect("listener progress");
                            }
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
                            assert_eq!(reply.as_slice(), handoffs::SWITCH_BYTES.as_slice());
                            assert_eq!(owner.get(), Some(7));
                            assert_eq!(raw.take().as_slice(), b"taillater");
                        },
                    );
                    assert!(wait.watch().error().is_none(), "listener accept");
                    result
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
    ] = checks.each_ref().map(Cell::get);
    assert!(received > 0);
    assert_eq!(
        [
            switched, live, cycles, commits, retained, limited, staged, replaced
        ],
        [1; 8]
    );
}
