pub mod common;

use std::{
    cell::Cell,
    io::{Read, Write},
    net::TcpStream,
    time::Duration,
};

use common::server::recover;
use common::{
    h1::terminal::{__W, App, CASES},
    server::{self, Plan},
};
use dope::core::driver::life::recovery::Policy;
use dope_test::harness::server::Pair;

#[test]
fn terminal_stops_pipeline() {
    let pair = Pair::bind().expect("listener");
    let plan = Plan::new(pair.addr(), Duration::from_secs(10)).build();
    pair.check(
        |context, listener| {
            let calls = Cell::new(0);
            let mut reports = Plan::try_reports(&plan).expect("claims");
            let result = App::serve(
                plan.with_bound(listener),
                context,
                __W(&calls),
                sark_fiber::Config::new(server::MAX_CONNECTIONS),
                Policy::new(|event| reports.push(event), recover),
                |result| {
                    result
                        .as_ref()
                        .map(|_| ())
                        .map_err(|error| error.cause())
                        .expect("server teardown")
                },
            );
            assert_eq!(
                calls.get(),
                1,
                "only the successful response resumes ingress"
            );
            assert!(reports.events().expect("reclaimed resources").is_empty());
            result
        },
        |address| {
            for (request, status, count) in CASES {
                let mut socket = TcpStream::connect(address).expect("connect");
                socket
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .expect("timeout");
                socket.write_all(request).expect("pipeline");
                let mut response = Vec::new();
                socket.read_to_end(&mut response).expect("closed response");
                assert!(response.starts_with(status), "{response:?}");
                assert_eq!(
                    response
                        .windows(b"HTTP/1.1".len())
                        .filter(|bytes| *bytes == b"HTTP/1.1")
                        .count(),
                    count,
                );
            }
        },
        |result| result.expect("server"),
    )
    .expect("pair");
}

#[test]
#[ignore = "requires Linux native listener shutdown"]
fn accept_failure_finishes_generated_server() {
    use std::{net::SocketAddr, os::fd::AsFd as _};

    use dope::{
        core::{
            Backend, Errno,
            dope_sys::io::socket::{How, Socket as _},
            io::{event::net::accept, socket::Listener},
            platform::wake,
        },
        manifold::network::listener::AcceptError,
        runtime::shutdown,
    };
    use dope_test::harness::{
        GUARD,
        scenario::{
            process::Deadline,
            run::{Entry, Task as _},
        },
    };
    use sark_h1::server::run;

    assert_eq!(
        std::env::consts::OS,
        "linux",
        "listener shutdown requires Linux"
    );
    let listener = Listener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("listener");
    let fault = server::accept::duplicate(listener.local_addr()).expect("fault descriptor");
    let plan = Plan::new(listener.local_addr(), Duration::from_secs(10)).build();
    Deadline::with(GUARD, |_| {
        let (context, stop, ready) = shutdown::ReadyPair::new().expect("shutdown").split();
        let guard = context
            .as_fd()
            .try_clone_to_owned()
            .expect("shutdown reader");
        let (finished, notify) = wake::Ends::blocking().expect("completion").split();
        let ((), served) = Entry(move || {
            let calls = Cell::new(0);
            App::serve(
                plan.with_bound(listener),
                context,
                __W(&calls),
                sark_fiber::Config::new(server::MAX_CONNECTIONS),
                Policy::new(drop, recover),
                |result| {
                    let error = result.err().expect("terminal listener failure");
                    assert!(matches!(
                        error.cause(),
                        run::RunCause::Failure(run::Cause::Execute(run::ExecutionError::Accept {
                            cause: AcceptError::Completion(accept::Failure::Syscall(error)),
                            drive: None,
                        })) if *error == Errno::INVALID_ARGUMENT
                    ));
                    notify.notify().expect("server finished");
                },
            )
        })
        .with(|| {
            ready.wait_timeout(GUARD).expect("installed server");
            Backend::shutdown::<Errno>(fault.as_fd(), How::READ).expect("stop native listener");
            finished
                .wait_timeout(GUARD)
                .expect("finish before external shutdown");
            stop.fire().expect("external shutdown after failure");
        })
        .expect("server worker");
        served.expect("server preparation");
        drop(guard);
    })
    .expect("terminal server deadline");
}

#[test]
#[ignore = "requires Linux native listener shutdown"]
fn observes_either_listener_on_one_driver() {
    use std::{net::SocketAddr, ops::ControlFlow, os::fd::AsFd as _};

    use dope::{
        core::{
            Backend, Errno,
            dope_sys::io::socket::{How, Socket as _},
            driver::{
                route::{KeyTag, Tag},
                storage,
            },
            io::{
                Reclaimed,
                event::{batch::Flow, net::accept},
                socket::Listener,
            },
        },
        manifold::network::listener::AcceptError,
        net::wire::{self, domain},
        runtime::{executor::session, topology},
    };
    use dope_fiber::{
        abi::{
            self,
            race::{Either, Race},
        },
        task::{done::Once, storage::scope::Brand},
    };
    use dope_test::harness::{GUARD, scenario::process::Deadline};
    use sark_h1::server::date;

    assert_eq!(std::env::consts::OS, "linux");
    for selected in 0..2 {
        Deadline::with(GUARD, |_| {
            let first = Listener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                .expect("first listener");
            let second = Listener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                .expect("second listener");
            let address = if selected == 0 { first.local_addr() } else { second.local_addr() };
            let mut fault = Some(server::accept::duplicate(address).expect("fault descriptor"));
            let first = Plan::new(first.local_addr(), Duration::from_secs(10))
                .build()
                .with_bound(first);
            let second = Plan::new(second.local_addr(), Duration::from_secs(10))
                .build_for::<KeyTag<2>, KeyTag<3>>()
                .with_bound(second);
            let profile = first.config();
            let storage = storage::With::new((), date::Clock::new().expect("Date clock"));
            let plans = (first, second);
            let mut reports = Plan::try_reports(&plans).expect("reclaim reports");
            let prepared = topology::Plan::new(storage, plans)
                .with_install(profile)
                .expect("combined claims");
            let calls = Cell::new(0);
            let state = __W(&calls);
            let mut finish = session::Completion::new();
            prepared.enter_with(
                (),
                |worker| worker.lend(|(first, second), stage| {
                    Brand::with(|first_brand| Brand::with(|second_brand| {
                        let first_app = App::new::<wire::Identity, KeyTag<0>>(
                            first_brand, &state, sark_fiber::Config::new(server::MAX_CONNECTIONS),
                        );
                        let second_app = App::new::<wire::Identity, KeyTag<2>>(
                            second_brand, &state, sark_fiber::Config::new(server::MAX_CONNECTIONS),
                        );
                        first.install(stage, domain::Local::new(wire::Identity), first_app,
                            |first, mut first_wait, stage| {
                                second.install(stage, domain::Local::new(wire::Identity), second_app,
                                    |second, mut second_wait, stage| {
                                        let watches = [first.watch(), second.watch()];
                                        let result = stage.with_app(
                                            &mut finish,
                                            server::accept::Listeners::new(first, second),
                                            |app| {
                                                let first = abi::poll_fn(move |cx| {
                                                    first_wait.poll(cx.as_ref().completion())
                                                });
                                                let second = abi::poll_fn(move |cx| {
                                                    let result = second_wait.poll(cx.as_ref().completion());
                                                    // Both waiters are registered before the one native fault.
                                                    if let Some(fault) = fault.take() {
                                                        Backend::shutdown::<Errno>(fault.as_fd(), How::READ)
                                                            .expect("stop selected listener");
                                                    }
                                                    result
                                                });
                                                app.drive_until(Once::new(Race::new(first, second)))
                                            },
                                        );
                                        let result = result.as_ref().map_err(|error| error.cause())
                                            .expect("combined session retirement");
                                        let (observed, cause) = match result.as_ref().expect("drive") {
                                            ControlFlow::Break(Either::Left(cause)) => (0, cause),
                                            ControlFlow::Break(Either::Right(cause)) => (1, cause),
                                            ControlFlow::Continue(_) => panic!("unexpected shutdown"),
                                        };
                                        assert_eq!(observed, selected);
                                        assert!(matches!(cause,
                                            AcceptError::Completion(accept::Failure::Syscall(error))
                                                if *error == Errno::INVALID_ARGUMENT
                                        ));
                                        assert!(matches!(watches[selected].error(),
                                            Some(AcceptError::Completion(accept::Failure::Syscall(error)))
                                                if error == Errno::INVALID_ARGUMENT
                                        ));
                                        assert!(watches[1 - selected].error().is_none());
                                    },
                                ).expect("second install");
                            },
                        ).expect("first install");
                    }));
                }),
                Policy::new(|event| reports.push(event), recover),
                |result| {
                    result.as_ref().map_err(|error| error.cause()).expect("combined topology");
                },
            );
            let route = [<KeyTag<0> as Tag>::ROUTE, <KeyTag<2> as Tag>::ROUTE][selected];
            // The consumed terminal failure also reaches the scope's mandatory observer.
            assert!(matches!(
                reports.events().expect("reclaimed resources"),
                [Reclaimed::AcceptRoute {
                    target,
                    cause: accept::Failure::Syscall(error),
                    flow: Flow::Last,
                }] if target.route() == route && *error == Errno::INVALID_ARGUMENT
            ), "{reports:?}");
        }).expect("combined listener deadline");
    }
}
