pub mod common;

use std::{
    cell::Cell,
    io::{Read, Write},
    net::{Shutdown, SocketAddr, TcpStream},
    time::Duration,
};

use common::server::recover;
use common::{
    route::{
        body::session::LifetimeDispatch,
        chunked::peer::Peer,
        keep::{
            KeepAliveDispatch,
            events::{Log, MALFORMED_CHUNK, PENDING, Reservation, diagnose, diagnose_h1},
        },
    },
    server::{self, Plan},
    stacks::{self, Count, Probe, Step},
};
use dope::{
    core::{driver::life::recovery::Policy, io::socket::Listener},
    net::wire::{self, domain},
    runtime::executor::session,
};
use dope_fiber::task::storage::scope::Brand;
use dope_test::{
    fibers,
    harness::{server::Pair, signals::LocalGate},
};
use o3::base::time::Positive;
use sark_core::{
    deadline::{self, config},
    life::session::Config,
};

#[test]
fn retires_body() {
    let listener = Listener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("listener");
    let address = listener.local_addr();
    let plan = Plan::new(address, Duration::from_secs(10)).build();
    let mut reports = Plan::try_reports(&plan).expect("claims");
    let prepared = plan.with_bound(listener).prepare(()).expect("resources");
    let events = Cell::new(Vec::new());
    let audits = Cell::new(None);
    let balance = Cell::new(None);
    let ready = LocalGate::new();
    let closed = LocalGate::new();
    let mut done = session::Completion::new();
    prepared.enter_with(
        (),
        |worker| {
            worker.lend(|plan, stage| {
                let router = KeepAliveDispatch::with_observer(
                    &(),
                    sark_fiber::Config::new(server::MAX_CONNECTIONS),
                    Reservation::new(
                        Log(&events, &audits, diagnose),
                        ready.handle(),
                        closed.handle(),
                        &balance,
                    ),
                );
                plan.install(
                    stage,
                    domain::Local::<wire::Identity, _>::new(wire::Identity),
                    router,
                    |root, stage| {
                        stage
                            .with_quiesce(&mut done, root, Duration::from_secs(3), |mut app| {
                                let mut socket = TcpStream::connect(address).expect("connect");
                                socket
                                    .set_read_timeout(Some(Duration::from_secs(3)))
                                    .expect("timeout");
                                socket.write_all(&PENDING.concat()).expect("first chunk");
                                assert_eq!(
                                    fibers::TEST
                                        .try_wait(&mut app, &ready.handle(), 1)
                                        .expect("body admission"),
                                    fibers::Wait::Reached
                                );
                                let recorded = events.take();
                                let released = recorded.contains(&"pre-admission-released");
                                events.set(recorded);
                                assert!(!released);
                                socket
                                    .write_all(MALFORMED_CHUNK)
                                    .expect("invalid next chunk");
                                assert_eq!(
                                    fibers::TEST
                                        .try_wait(&mut app, &closed.handle(), 1)
                                        .expect("session close"),
                                    fibers::Wait::Reached
                                );
                                let mut response = String::new();
                                socket
                                    .read_to_string(&mut response)
                                    .expect("closed response");
                                assert!(response.starts_with("HTTP/1.1 400 "));
                            })
                            .expect("runtime");
                    },
                )
                .expect("install");
            })
        },
        Policy::new(|event| reports.push(event), recover),
        |result| {
            result
                .as_ref()
                .map_err(|error| error.cause())
                .expect("server teardown");
        },
    );
    assert!(
        reports.events().expect("reclaimed resources").is_empty(),
        "retirement: {reports:?}"
    );
    assert_eq!(audits.get(), None);
    assert_eq!(balance.get(), None);
    let events = events.into_inner();
    for expected in [
        "pre-admission-released",
        "request-owner-scope-closed",
        "terminal",
        "exchange-retired",
    ] {
        assert_eq!(
            events.iter().filter(|event| **event == expected).count(),
            1,
            "{expected}: {events:?}"
        );
    }
}

#[test]
fn retires_idle_generation() {
    let age =
        (Positive::try_new(Duration::from_millis(100))).expect("the fixture window is positive");
    let drain =
        (Positive::try_new(Duration::from_millis(250))).expect("the fixture window is positive");
    let harness = Pair::bind().expect("bind H1 lifetime listener");
    let bind = harness.addr();
    let lifetime = Config::new(age, drain);
    let server = server::Plan::new(bind, Duration::from_secs(10))
        .build()
        .session(lifetime);

    harness
        .check(
            |context, listener| {
                let mut reports = Plan::try_reports(&server).expect("server descriptor claims");
                let result = LifetimeDispatch::serve(
                    server.with_bound(listener),
                    context,
                    (),
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
                assert!(
                    reports.events().expect("reclaimed resources").is_empty(),
                    "server retirement: {reports:?}"
                );
                result
            },
            |bind| {
                let mut first = TcpStream::connect(bind).expect("connect first H1 generation");
                first
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .expect("bound first-generation read timeout");
                first
                    .write_all(b"GET /ping HTTP/1.1\r\nHost: lifetime\r\n\r\n")
                    .expect("request");
                assert_eq!(
                    Peer::new(&mut first, &mut Vec::new())
                        .try_response()
                        .expect("lifetime response"),
                    (200, b"pong".to_vec())
                );
                let mut eof = [0_u8; 1];
                assert_eq!(first.read(&mut eof).expect("read server-driven EOF"), 0);

                let mut second = TcpStream::connect(bind).expect("connect second H1 generation");
                second
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .expect("bound second-generation read timeout");
                second
                    .write_all(b"GET /ping HTTP/1.1\r\nHost: lifetime\r\n\r\n")
                    .expect("request");
                assert_eq!(
                    Peer::new(&mut second, &mut Vec::new())
                        .try_response()
                        .expect("lifetime response"),
                    (200, b"pong".to_vec())
                );
            },
            |result| {
                assert!(
                    result.is_ok(),
                    "retires_idle_generation server shutdown: {:?}",
                    result.err()
                );
            },
        )
        .expect("server-driven H1 lifetime proof");
}

#[test]
fn completes_in_order() {
    let pair = Pair::bind().expect("listener");
    let address = pair.addr();
    let mut events = Vec::new();
    pair.check(
        |context, listener| {
            let log = Cell::new(Vec::new());
            let audits = Cell::new(None);
            let state = stacks::State {
                challenge: b"completion".to_vec(), admitted: Cell::new(false),
                invalid: Cell::new(false), sleep: Cell::new(None),
            };
            let plan = Plan::new(address, Duration::from_secs(10)).build();
            let mut reports = Plan::try_reports(&plan).expect("claims");
            stacks::observed(
                plan,
                context,
                listener,
                &state,
                Probe(&log, &audits, diagnose_h1),
                Policy::new(|event| reports.push(event), recover),
                |result| result.expect("observed server"),
            )
            .map(|()| {
                assert!(!state.invalid.get());
                assert_eq!(state.sleep.get(), None);
                assert_eq!(audits.get(), None);
                assert!(reports.events().expect("reclaimed resources").is_empty());
                log.into_inner()
            })
        },
        |address| {
            let mut socket = TcpStream::connect(address).expect("connect");
            socket.set_read_timeout(Some(Duration::from_secs(3))).expect("timeout");
            socket.write_all(b"GET /complete HTTP/1.1\r\nHost: proof\r\nX-Proof-Challenge: completion\r\nConnection: close\r\n\r\n").expect("request");
            let mut response = Vec::new();
            socket.read_to_end(&mut response).expect("response");
            assert!(response.windows(6).any(|part| part == b"200 OK"));
            assert!(response.ends_with(b"completion"));
        },
        |result| events = result.expect("server"),
    ).expect("pair");
    assert_eq!(
        events,
        [
            Step::Opened,
            Step::Established,
            Step::Exchange,
            Step::Admitted,
            Step::Input,
            Step::Response,
            Step::Completed,
            Step::Retired,
            Step::Closed
        ],
    );
}

#[test]
fn deadline_after_eof() {
    let listener = Listener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("listener");
    let address = listener.local_addr();
    let deadline = config::Config::try_new(deadline::Phase::Handler, Duration::from_millis(500))
        .expect("handler deadline");
    let plan = Plan::new(address, Duration::from_secs(10))
        .build()
        .policy(deadline);
    let mut reports = Plan::try_reports(&plan).expect("claims");
    let prepared = plan.with_bound(listener).prepare(()).expect("resources");
    let events = Cell::new(Vec::new());
    let audits = Cell::new(None);
    let state = stacks::State {
        challenge: b"cancellation".to_vec(),
        admitted: Cell::new(false),
        invalid: Cell::new(false),
        sleep: Cell::new(None),
    };
    let mut done = session::Completion::new();
    prepared.enter_with(
        (),
        |worker| worker.lend(|plan, stage| Brand::with(|brand| {
            let router = stacks::Routes::with_observer(
                brand, &state, sark_fiber::Config::new(server::MAX_CONNECTIONS), Probe(&events, &audits, diagnose_h1),
            );
            plan.install(stage, domain::Local::<wire::Identity, _>::new(wire::Identity), router,
                |root, stage| {
                    stage.with_quiesce(&mut done, root, Duration::from_secs(3), |mut app| {
                        let mut socket = TcpStream::connect(address).expect("connect");
                        socket.write_all(b"GET /cancel HTTP/1.1\r\nHost: proof\r\nX-Proof-Challenge: cancellation\r\n\r\n").expect("request");
                        let waiting = fibers::TEST.try_wait(&mut app, &Count { events: &events, step: Step::Pending }, 1);
                        assert!(!state.invalid.get());
                        assert_eq!(state.sleep.get(), None);
                        assert_eq!(waiting.expect("pending task"), fibers::Wait::Reached);
                        assert!(state.admitted.get());
                        socket.shutdown(Shutdown::Write).expect("finish request input");
                        assert_eq!(fibers::TEST.try_wait(&mut app, &Count { events: &events, step: Step::Cancelled }, 1).expect("cancelled task"), fibers::Wait::Reached);
                        assert_eq!(fibers::TEST.try_wait(&mut app, &Count { events: &events, step: Step::Closed }, 1).expect("closed session"), fibers::Wait::Reached);
                        let recorded = events.take();
                        let retired = recorded.contains(&Step::Retired);
                        events.set(recorded);
                        assert!(retired);
                    }).expect("runtime");
                },
            ).expect("install");
        })),
        Policy::new(|event| reports.push(event), recover),
        |result| { result.as_ref().map_err(|error| error.cause()).expect("server teardown"); },
    );
    assert!(
        reports.events().expect("reclaimed resources").is_empty(),
        "retirement: {reports:?}"
    );
    assert!(!state.invalid.get());
    assert_eq!(state.sleep.get(), None);
    assert_eq!(audits.get(), None);
    let events = events.into_inner();
    assert!(events.contains(&Step::Pending));
    let events: Vec<_> = events
        .iter()
        .copied()
        .filter(|step| *step != Step::Pending)
        .collect();
    assert_eq!(
        events,
        [
            Step::Opened,
            Step::Established,
            Step::Exchange,
            Step::Admitted,
            Step::Input,
            Step::Deadline(deadline::Phase::Handler),
            Step::Cancelled,
            Step::Retired,
            Step::Closed
        ]
    );
}

#[test]
fn resume_failure() {
    let mut events = Vec::new();
    let pair = Pair::bind().expect("listener");
    let bind = pair.addr();
    pair.check(
        |context, listener| {
            let log = Cell::new(Vec::new());
            let audits = Cell::new(None);
            let state = stacks::State {
                challenge: Vec::new(),
                admitted: Cell::new(false),
                invalid: Cell::new(false),
                sleep: Cell::new(None),
            };
            let plan = Plan::new(bind, Duration::from_secs(10)).build();
            let mut reports = Plan::try_reports(&plan).expect("claims");
            stacks::observed(
                plan,
                context,
                listener,
                &state,
                Log(&log, &audits, diagnose),
                Policy::new(|event| reports.push(event), recover),
                |result| result.expect("observed server"),
            )
            .map(|()| {
                assert!(!state.invalid.get());
                assert_eq!(state.sleep.get(), None);
                assert_eq!(audits.get(), None);
                assert!(reports.events().expect("reclaimed resources").is_empty());
                log.into_inner()
            })
        },
        |bind| {
            let mut socket = TcpStream::connect(bind).expect("connect");
            socket
                .set_read_timeout(Some(Duration::from_secs(3)))
                .expect("timeout");
            let (head, body) = stacks::invalid_request();
            socket.write_all(head.as_bytes()).expect("request head");
            let mut interim = [0; 25];
            socket.read_exact(&mut interim).expect("100 Continue");
            assert_eq!(&interim, b"HTTP/1.1 100 Continue\r\n\r\n");
            socket.write_all(&body).expect("request body");
            let mut response = Vec::new();
            socket
                .read_to_end(&mut response)
                .expect("rejected response");
            assert!(response.starts_with(b"HTTP/1.1 400 "));
        },
        |result| events = result.expect("server"),
    )
    .expect("server pair");

    let input = events
        .iter()
        .position(|event| *event == "input-finished")
        .expect("complete body");
    assert!(events[..input].contains(&"pre-admission-reserved"));
    let released = events
        .iter()
        .rposition(|event| *event == "pre-admission-released")
        .expect("released body credit");
    assert!(input < released);
    assert_eq!(
        &events[released + 1..],
        [
            "request-owner-scope-closed",
            "terminal",
            "exchange-retired",
            "session-closed",
        ],
    );
}
