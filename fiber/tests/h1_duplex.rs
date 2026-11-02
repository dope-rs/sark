pub mod common;

use std::{
    cell::Cell,
    convert::Infallible,
    io::{self, Read, Write},
    marker::PhantomData,
    net::{Ipv4Addr, Shutdown, SocketAddr, TcpStream},
    time::Duration,
};

use common::server::recover;
use common::{
    client::runtime::sockets,
    h1::{
        DUPLEX_STATE, MultiDuplexApplication, SingleDuplexApplication,
        branches::{self, Kind},
    },
    route::body::tunnels::{ConnectDispatch, Duplex, Receive as _, Snapshot, State},
    server::{self, Plan},
    support,
};
use dope::{
    core::{
        driver::{life::recovery::Policy, route::KeyTag},
        io::socket::Listener,
    },
    manifold::app::receive::limits::Budget,
    net::wire::{self, domain},
    runtime::{executor::session, topology},
};
use dope_fiber::task::storage::scope::Brand;
use dope_test::{
    fibers,
    harness::{
        peer::{Peer, Stream},
        server::Pair,
    },
};
use dope_tls::tls::endpoint;
use o3::buffer::hold::limits::Bound;
use sark_fiber::Config;

fn serve(end: bool, client: impl FnOnce(&mut &Stream<'_>) -> io::Result<()> + Send) -> Snapshot {
    let listener = Listener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("listener");
    let address = listener.local_addr();
    let plan = Plan::new(address, Duration::from_secs(10))
        .build()
        .gated_scoped();
    let mut reports = Plan::try_reports(&plan).expect("claims");
    let prepared = plan.with_bound(listener).prepare(()).expect("resources");
    let counters = Duplex::default();
    let state = State {
        counters: &counters,
        finish_output_on_input: end,
        hold_output_on_finish: false,
    };
    let mut done = session::Completion::new();
    prepared.enter_with(
        (),
        |worker| {
            worker.lend(|plan, stage| {
                Brand::with(|brand| {
                    let router = ConnectDispatch::new(
                        brand,
                        &state,
                        Config::new(server::MAX_CONNECTIONS)
                            .with_budget(Budget::new(Bound::new(4096), 0)),
                    );
                    plan.install(
                        stage,
                        domain::Local::<wire::Identity, _>::new(wire::Identity),
                        router,
                        |root, stage| {
                            stage
                                .with_quiesce(&mut done, root, Duration::from_secs(5), |mut app| {
                                    Peer::at(address)
                                        .with(client, |peer| {
                                            assert_eq!(
                                                fibers::TEST
                                                    .try_wait(
                                                        &mut app,
                                                        &state.counters.closed.handle(),
                                                        1,
                                                    )
                                                    .expect("closed session"),
                                                fibers::Wait::Reached
                                            );
                                            peer.finish()
                                                .expect("peer finish")
                                                .expect("CONNECT peer");
                                        })
                                        .expect("peer");
                                })
                                .expect("runtime");
                        },
                    )
                    .expect("install");
                })
            })
        },
        Policy::new(|event| reports.push(event), recover),
        |result| {
            result
                .as_ref()
                .map_err(|error| error.cause())
                .expect("server");
        },
    );
    assert!(
        reports.events().expect("reclaimed resources").is_empty(),
        "retirement: {reports:?}"
    );
    state.counters.snapshot()
}

#[test]
fn handoff_keeps_ownership() {
    let seen = serve(false, |socket| {
        socket.write_all(
            b"CONNECT upstream.example:443 HTTP/1.1\r\n\
                  Host: upstream.example:443\r\n\
                  \r\n\
                  early",
        )?;

        let mut received = Vec::new();
        socket.read_until(&mut received, b"\r\n\r\nearlyready")?;
        assert!(
            received.starts_with(b"HTTP/1.1 200 Connection Established\r\n\r\n"),
            "unexpected CONNECT response: {:?}",
            String::from_utf8_lossy(&received)
        );
        socket.write_all(b"later")?;
        socket.read_until(&mut received, b"earlyreadylater")?;
        socket.shutdown(Shutdown::Write)?;
        socket.read_to_end(&mut received)?;
        assert!(received.ends_with(b"finished"));
        Ok(())
    });
    assert_eq!(seen.capability, 1);
    assert_eq!(seen.input_finished, 1);
}

#[test]
fn tail_precedes_finish() {
    let seen = serve(false, |socket| {
        socket.write_all(
            b"CONNECT upstream.example:443 HTTP/1.1\r\n\
                  Host: upstream.example:443\r\n\
                  \r\n\
                  tail-before-fin",
        )?;
        socket.shutdown(Shutdown::Write)?;

        let mut received = Vec::new();
        socket.read_to_end(&mut received)?;
        assert!(received.starts_with(b"HTTP/1.1 200 Connection Established\r\n\r\n"));
        assert!(received.ends_with(b"tail-before-finreadyfinished"));
        Ok(())
    });
    assert_eq!(seen.input_finished, 1);
}

#[test]
fn input_outlives_output() {
    let seen = serve(true, |socket| {
        socket.write_all(
            b"CONNECT upstream.example:443 HTTP/1.1\r\n\
                  Host: upstream.example:443\r\n\
                  \r\n",
        )?;

        let mut received = Vec::new();
        socket.read_until(&mut received, b"\r\n\r\nready")?;
        socket.write_all(b"first")?;
        socket.read_to_end(&mut received)?;
        assert!(received.ends_with(b"first"));
        socket.write_all(b"after-finish")?;
        socket.shutdown(Shutdown::Write)?;
        Ok(())
    });

    assert_eq!(seen.after_finish, b"after-finish".len(),);
    assert_eq!(seen.input_finished, 1);
}

#[test]
fn tls_finish_after_switch() {
    for prefix in [
        b"".as_slice(),
        b"GET /wait HTTP/1.1\r\nHost: x\r\n\r\n".as_slice(),
    ] {
        let harness = Pair::bind().expect("TLS duplex listener");
        let plan = Plan::new(harness.addr(), Duration::from_secs(10)).build();
        harness
            .run(
                |context, listener| {
                    let mut reports = Plan::try_reports(&plan).expect("TLS duplex claims");
                    let counters = Duplex::default();
                    let state = State {
                        counters: &counters,
                        finish_output_on_input: false,
                        hold_output_on_finish: false,
                    };
                    let result = ConnectDispatch::serve_with(
                        plan.with_bound(listener),
                        endpoint::Config::from(
                            support::tls().server().expect("TLS server config"),
                        ),
                        context,
                        state,
                        Config::new(server::MAX_CONNECTIONS)
                            .with_budget(Budget::new(Bound::new(4096), 0)),
                        Policy::new(|event| reports.push(event), recover),
                        |result| {
                            result
                                .as_ref()
                                .map(|_| ())
                                .map_err(|error| error.cause())
                                .expect("TLS duplex runtime");
                        },
                    );
                    assert!(result.is_ok(), "TLS duplex server");
                    drop(result);
                    assert_eq!(counters.snapshot().input_finished, 1);
                    assert!(counters.failures.take().is_empty());
                    assert!(counters.timer_failures.take().is_empty());
                    assert!(reports.events().expect("TLS duplex reclaimed").is_empty());
                    Ok::<(), Infallible>(())
                },
                |address| {
                    let client = sockets::Tls::connect(
                        address,
                        support::tls().client().expect("TLS client config"),
                        Ipv4Addr::LOCALHOST.into(),
                    )
                    .expect("TLS duplex connect");
                    let mut input = prefix.to_vec();
                    input.extend_from_slice(
                        b"CONNECT upstream.example:443 HTTP/1.1\r\nHost: upstream.example:443\r\n\r\ntail-before-fin",
                    );
                    let received = client.close_after(&input).expect("TLS duplex close");
                    assert!(
                        received.ends_with(b"tail-before-finreadyfinished"),
                        "TLS switch with prefix {:?}: received {:?}",
                        String::from_utf8_lossy(prefix),
                        String::from_utf8_lossy(&received),
                    );
                },
            )
            .expect("TLS duplex harness");
    }
}

#[test]
fn drain_stops_input() {
    use sark_core::life::session;

    let harness = Pair::bind().expect("draining duplex listener");
    let lifetime = session::Config::try_new(Duration::from_millis(100), Duration::from_millis(250))
        .expect("duplex lifetime");
    let plan = Plan::new(harness.addr(), Duration::from_secs(10))
        .build()
        .session(lifetime);
    harness
        .run(
            |context, listener| {
                let mut reports = Plan::try_reports(&plan).expect("draining duplex claims");
                let counters = Duplex::default();
                let state = State {
                    counters: &counters,
                    finish_output_on_input: false,
                    hold_output_on_finish: true,
                };
                let result = ConnectDispatch::serve(
                    plan.with_bound(listener),
                    context,
                    state,
                    Config::new(server::MAX_CONNECTIONS)
                        .with_budget(Budget::new(Bound::new(4096), 0)),
                    Policy::new(|event| reports.push(event), recover),
                    |result| {
                        result
                            .as_ref()
                            .map(|_| ())
                            .map_err(|error| error.cause())
                            .expect("draining duplex runtime");
                    },
                );
                assert!(result.is_ok(), "draining duplex server");
                drop(result);
                let seen = counters.snapshot();
                assert_eq!(seen.input_finished, 1);
                assert_eq!(seen.after_finish, 0);
                assert!(
                    reports
                        .events()
                        .expect("draining duplex reclaimed")
                        .is_empty()
                );
                Ok::<(), Infallible>(())
            },
            |address| {
                let mut socket = TcpStream::connect(address).expect("draining duplex connect");
                socket
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .expect("drain timeout");
                socket.write_all(
                    b"CONNECT upstream.example:443 HTTP/1.1\r\nHost: upstream.example:443\r\n\r\n",
                ).expect("draining CONNECT");
                let expected = b"HTTP/1.1 200 Connection Established\r\n\r\nreadyfinished";
                let mut received = vec![0; expected.len()];
                socket
                    .read_exact(&mut received)
                    .expect("duplex input finish");
                assert_eq!(received, expected);
                socket
                    .write_all(b"after-finish")
                    .expect("late duplex input");
                let mut tail = Vec::new();
                if let Err(cause) = socket.read_to_end(&mut tail) {
                    assert_eq!(
                        cause.kind(),
                        io::ErrorKind::ConnectionReset,
                        "draining duplex close: {cause:?}",
                    );
                }
                assert!(tail.is_empty());
            },
        )
        .expect("draining duplex harness");
}

#[test]
fn runs_scoped_duplex() {
    for kind in [Kind::Single, Kind::Multiple, Kind::Conditional] {
        let harness = Pair::bind().expect("bind scoped duplex listener");
        let plan = server::Plan::new(harness.addr(), Duration::from_secs(5))
            .build()
            .gated_scoped();
        harness
            .run(
                |context, listener| {
                    let mut reports =
                        server::Plan::try_reports(&plan).expect("server descriptor claims");
                    let prepared = plan.with_bound(listener).prepare(()).expect("server resources");
                    let mut finish = session::Completion::new();
                    prepared.enter_with(
                        context,
                        |worker| {
                            worker.lend(|plan, stage| {
                                let state = DUPLEX_STATE;
                                let config = Config::new(server::MAX_CONNECTIONS)
                                    .with_budget(Budget::new(Bound::new(4096), 0));
                                let wire = domain::Local::<wire::Identity, _>::new(wire::Identity);
                                let result = match kind {
                                    Kind::Multiple => {
                                        let (state, config, report) = (&state, config, &mut finish);
                                        Brand::with(|brand| {
                                            let app = MultiDuplexApplication::__scoped_new(brand, state, config);
                                            plan.install(stage, wire, app, |root, stage| {
                                                let result = stage.request(report, root);
                                                result.as_ref().map_err(|error| error.cause()).expect("session");
                                                result.ok().expect("checked session").expect("drive")
                                            })
                                        })
                                    }
                                    Kind::Single => {
                                        let (state, config, report) = (&state, config, &mut finish);
                                        Brand::with(|brand| {
                                            let app = SingleDuplexApplication::__scoped_new(brand, state, config);
                                            plan.install(stage, wire, app, |root, stage| {
                                                let result = stage.request(report, root);
                                                result.as_ref().map_err(|error| error.cause()).expect("session");
                                                result.ok().expect("checked session").expect("drive")
                                            })
                                        })
                                    }
                                    Kind::Conditional => {
                                        let bytes = String::from("borrowed");
                                        let state = branches::State {
                                            bytes: bytes.as_bytes(),
                                            source: Cell::new(0),
                                            driver: PhantomData,
                                        };
                                        let _ = branches::App::new::<_, wire::Identity, KeyTag<0>>(
                                            &state, config,
                                        );
                                        let (state, config, report) = (&state, config, &mut finish);
                                        let result = Brand::with(|brand| {
                                            let app = branches::App::__scoped_new(brand, state, config);
                                            plan.install(stage, wire, app, |root, stage| {
                                                let result = stage.request(report, root);
                                                result.as_ref().map_err(|error| error.cause()).expect("session");
                                                result.ok().expect("checked session").expect("drive")
                                            })
                                        });
                                        assert_eq!(state.source.get(), bytes.len());
                                        result
                                    }
                                };
                                result.map_err(topology::Failure::Install)
                            })
                        },
                        Policy::new(|event| reports.push(event), recover),
                        |result| {
                            let result = result.map_err(topology::RunError::from);
                            result.as_ref().map_err(|error| error.cause()).expect("topology")
                                .as_ref().map_err(|error| error.cause()).expect("worker");
                        },
                    );
                    assert!(reports.events().expect("reclaimed resources").is_empty(), "server retirement: {reports:?}");
                    Ok::<(), Infallible>(())
                },
                |address| {
                    let mut socket = TcpStream::connect(address).expect("connect scoped duplex client");
                    socket
                        .set_read_timeout(Some(Duration::from_secs(3)))
                        .expect("set response deadline");
                    socket
                        .write_all(
                            b"CONNECT upstream.example:443 HTTP/1.1\r\nHost: upstream.example:443\r\n\r\n",
                        )
                        .expect("write CONNECT request");
                    socket.shutdown(Shutdown::Write).expect("finish duplex input");
                    let mut response = Vec::new();
                    socket.read_to_end(&mut response).expect("read CONNECT response");
                    assert!(response.starts_with(b"HTTP/1.1 200 Connection Established\r\n\r\n"));
                },
            )
            .expect("scoped duplex harness");
    }
}
