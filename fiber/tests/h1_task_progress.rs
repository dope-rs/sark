pub mod common;

use std::{
    cell::Cell,
    convert,
    io::{ErrorKind, Read, Write},
    net::{Shutdown, TcpStream},
    thread,
    time::{Duration, Instant},
};

use common::server::recover;
use common::{
    retained::State,
    route::{
        chunked::{apps::ChunkedDispatch, peer::Peer},
        glue::{hello::HelloDispatch, timer::Chain},
    },
    server::{self, Plan, peer::Receive as _},
};
use dope::{
    core::{
        self, BranchError,
        driver::{
            life::recovery::Policy,
            route::{FrameworkTag, Tag, kind},
            settings::errors::Claim,
            storage::With,
        },
    },
    net::wire::{self, domain},
    runtime::{executor::session, topology},
};
use dope_test::{harness::server::Pair, reclaim};
use sark_fiber::{
    Throughput,
    driver::{self, settings::Config},
};
use sark_h1::server::{date, run::prepare::Error};

#[test]
fn waits_after_eof() {
    let harness = Pair::bind().expect("bind isolated listener");
    let bind = harness.addr();
    let server = server::Plan::new(bind, Duration::from_secs(10)).build();

    harness
        .check(
            |context, listener| {
                let mut reports = Plan::try_reports(&server).expect("server descriptor claims");
                let failures = Cell::new(Vec::new());
                let result = Chain::serve(
                    server.with_bound(listener),
                    context,
                    State(&failures),
                    sark_fiber::Config::new(server::MAX_CONNECTIONS),
                    Policy::new(|event| reports.push(event), recover),
                    |result| {
                        result
                            .as_ref()
                            .map(|_| ())
                            .map_err(|error| error.cause())
                            .expect("server")
                    },
                );
                assert!(
                    reports.events().expect("reclaimed resources").is_empty(),
                    "server retirement: {reports:?}"
                );
                let failures = failures.into_inner();
                assert!(failures.is_empty(), "timer: {failures:?}");
                result
            },
            |bind| {
                let mut stream = TcpStream::connect(bind).expect("connect");
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .expect("bound response wait");
                let start = Instant::now();
                stream
                    .write_all(b"GET /chain HTTP/1.1\r\nHost: x\r\n\r\n")
                    .expect("write complete request");
                stream
                    .shutdown(Shutdown::Write)
                    .expect("finish request input");
                let mut response = String::new();
                stream
                    .read_to_string(&mut response)
                    .expect("read response through close");
                let elapsed = start.elapsed();

                assert!(elapsed >= Duration::from_millis(90), "elapsed: {elapsed:?}",);
                assert!(response.contains("200 OK"), "response: {response}");
                assert!(response.ends_with("two slept"), "response: {response}");
            },
            |result| {
                assert!(
                    result.is_ok(),
                    "waits_after_eof server shutdown: {:?}",
                    result.err()
                );
            },
        )
        .expect("harness");
}

const HEAD_TIMEOUT: Duration = Duration::from_millis(500);

#[test]
fn input_deadline_spans_body() {
    let pair = Pair::bind().expect("body deadline listener");
    let plan = Plan::new(pair.addr(), HEAD_TIMEOUT).build();
    pair.check(
        |context, listener| {
            let mut reports = Plan::try_reports(&plan).expect("claims");
            let failures = Cell::new(Vec::new());
            let result = ChunkedDispatch::serve(
                plan.with_bound(listener),
                context,
                State(&failures),
                sark_fiber::Config::new(server::MAX_CONNECTIONS),
                Policy::new(|event| reports.push(event), recover),
                |result| assert!(result.is_ok(), "body deadline server"),
            );
            assert!(reports.events().expect("retired resources").is_empty());
            assert!(failures.into_inner().is_empty());
            result
        },
        |address| {
            for (head, dribble) in [
                (
                    &b"POST /expect HTTP/1.1\r\nHost: local\r\nContent-Length: 4096\r\n\r\n"[..],
                    &b""[..],
                ),
                (
                    &b"POST /expect HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\n\r\n"
                        [..],
                    &b"1\r\nx\r\n"[..],
                ),
                (
                    &b"POST /discard HTTP/1.1\r\nHost: local\r\nContent-Length: 4096\r\n\r\n"[..],
                    &b"x"[..],
                ),
            ] {
                let mut socket = Peer::try_connect(address).expect("body peer");
                socket
                    .set_read_timeout(Some(Duration::from_millis(50)))
                    .expect("dribble interval");
                socket.write_all(head).expect("admit body");
                let started = Instant::now();
                loop {
                    assert!(
                        started.elapsed() < Duration::from_secs(3),
                        "input window renewed"
                    );
                    match socket.read(&mut [0; 512]) {
                        Ok(0) => break,
                        Ok(_) => {}
                        Err(error) if error.kind() == ErrorKind::ConnectionReset => break,
                        Err(error)
                            if matches!(
                                error.kind(),
                                ErrorKind::WouldBlock | ErrorKind::TimedOut
                            ) =>
                        {
                            if !dribble.is_empty() {
                                match socket.write_all(dribble) {
                                    Ok(()) => {}
                                    Err(error)
                                        if matches!(
                                            error.kind(),
                                            ErrorKind::BrokenPipe | ErrorKind::ConnectionReset
                                        ) =>
                                    {
                                        break;
                                    }
                                    Err(error) => panic!("body dribble: {error}"),
                                }
                            }
                        }
                        Err(error) => panic!("body deadline: {error}"),
                    }
                }
                assert!(
                    started.elapsed() >= HEAD_TIMEOUT / 2,
                    "body rejected before its deadline"
                );
            }

            for (head, response, early) in [
                (
                    &b"POST /expect HTTP/1.1\r\nHost: local\r\nContent-Length: 2\r\n\r\na"[..],
                    &b"ab"[..],
                    false,
                ),
                (
                    &b"POST /discard HTTP/1.1\r\nHost: local\r\nContent-Length: 2\r\n\r\na"[..],
                    &b"2"[..],
                    true,
                ),
            ] {
                let mut socket = Peer::try_connect(address).expect("completed body peer");
                socket.write_all(head).expect("partial body");
                if !early {
                    thread::sleep(Duration::from_millis(100));
                    socket.write_all(b"b").expect("complete body");
                }
                let mut buffered = Vec::new();
                assert_eq!(
                    Peer::new(&mut socket, &mut buffered)
                        .try_response()
                        .expect("body response"),
                    (200, response.to_vec()),
                );
                if early {
                    socket.write_all(b"b").expect("complete native discard");
                }
                thread::sleep(HEAD_TIMEOUT + Duration::from_millis(100));
                assert_eq!(
                    Peer::new(&mut socket, &mut buffered)
                        .try_exchange(b"GET /ping HTTP/1.1\r\nHost: local\r\n\r\n")
                        .expect("completed input cleared the lease"),
                    (200, b"pong".to_vec()),
                );
            }
        },
        |result| assert!(result.is_ok(), "body deadline setup"),
    )
    .expect("body deadline runtime");
}

#[test]
fn stalled_head_times_out() {
    let harness = Pair::bind().expect("reserve test address");
    let bind = harness.addr();
    harness
        .check(
            |context, listener| {
                let server = server::Plan::new(bind, HEAD_TIMEOUT).build();
                {
                    let mut reports = Plan::try_reports(&server).expect("runtime resource claims");
                    let result = HelloDispatch::serve(
                        server.with_bound(listener),
                        context,
                        (),
                        sark_fiber::Config::new(server::MAX_CONNECTIONS),
                        Policy::new(|report| reports.push(report), recover),
                        |outcome| {
                            assert!(outcome.is_ok(), "server execution");
                        },
                    );
                    assert!(result.is_ok(), "slowloris server must shut down cleanly");
                    drop(result);
                    assert!(
                        reports.events().expect("reclaimed resources").is_empty(),
                        "unexpected reclaimed server operations: {reports:?}"
                    );
                    Ok::<(), convert::Infallible>(())
                }
            },
            |bind| {
                let mut sock = TcpStream::connect(bind).expect("connect");
                sock.set_read_timeout(Some(Duration::from_secs(5)))
                    .expect("set timeout");
                sock.write_all(b"GET /hello HTTP/1.1\r\nHost: x\r\n")
                    .expect("write partial head");

                let (buf, elapsed) = sock.try_to_close().expect("partial head retirement");
                assert!(
                    elapsed >= Duration::from_millis(300),
                    "closed too early ({elapsed:?}); deadline must bound from first byte"
                );
                assert!(
                    elapsed < Duration::from_secs(4),
                    "not closed by head deadline ({elapsed:?})"
                );
                let text = String::from_utf8_lossy(&buf);
                assert!(
                    buf.is_empty() || text.contains("408"),
                    "expected close or 408, got: {text:?}"
                );
            },
            |result| {
                (result)
                    .as_ref()
                    .expect("stalled_head_times_out server shutdown");
            },
        )
        .expect("harness");
}

#[test]
fn fast_request_completes() {
    let harness = Pair::bind().expect("reserve test address");
    let bind = harness.addr();
    harness
        .check(
            |context, listener| {
                let server = server::Plan::new(bind, HEAD_TIMEOUT).build();
                {
                    let mut reports = Plan::try_reports(&server).expect("runtime resource claims");
                    let result = HelloDispatch::serve(
                        server.with_bound(listener),
                        context,
                        (),
                        sark_fiber::Config::new(server::MAX_CONNECTIONS),
                        Policy::new(|report| reports.push(report), recover),
                        |outcome| {
                            assert!(outcome.is_ok(), "server execution");
                        },
                    );
                    assert!(result.is_ok(), "slowloris server must shut down cleanly");
                    drop(result);
                    assert!(
                        reports.events().expect("reclaimed resources").is_empty(),
                        "unexpected reclaimed server operations: {reports:?}"
                    );
                    Ok::<(), convert::Infallible>(())
                }
            },
            |bind| {
                let mut sock = TcpStream::connect(bind).expect("connect");
                sock.set_read_timeout(Some(Duration::from_secs(2)))
                    .expect("set timeout");
                sock.write_all(b"GET /hello HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                    .expect("write");
                let (buf, _) = sock.try_to_close().expect("completed head response");
                let text = std::str::from_utf8(&buf).expect("UTF-8 response");
                assert!(text.starts_with("HTTP/1.1 200 "), "status: {text:?}");
                assert!(!text.contains("408"), "false-positive deadline: {text:?}");
                let (_, body) = text
                    .split_once("\r\n\r\n")
                    .expect("blank line separating headers from body");
                assert_eq!(body, "hello", "body mismatch: {text:?}");
            },
            |result| {
                (result)
                    .as_ref()
                    .expect("fast_request_completes server shutdown");
            },
        )
        .expect("harness");
}

#[test]
fn slow_progress_completes() {
    let harness = Pair::bind().expect("reserve test address");
    let bind = harness.addr();
    harness
        .check(
            |context, listener| {
                let server = server::Plan::new(bind, HEAD_TIMEOUT).build();
                {
                    let mut reports = Plan::try_reports(&server).expect("runtime resource claims");
                    let result = HelloDispatch::serve(
                        server.with_bound(listener),
                        context,
                        (),
                        sark_fiber::Config::new(server::MAX_CONNECTIONS),
                        Policy::new(|report| reports.push(report), recover),
                        |outcome| {
                            assert!(outcome.is_ok(), "server execution");
                        },
                    );
                    assert!(result.is_ok(), "slowloris server must shut down cleanly");
                    drop(result);
                    assert!(
                        reports.events().expect("reclaimed resources").is_empty(),
                        "unexpected reclaimed server operations: {reports:?}"
                    );
                    Ok::<(), convert::Infallible>(())
                }
            },
            |bind| {
                let mut sock = TcpStream::connect(bind).expect("connect");
                sock.set_read_timeout(Some(Duration::from_secs(2)))
                    .expect("set timeout");
                sock.write_all(b"GET /hello HTTP/1.1\r\n").expect("write 1");
                thread::sleep(Duration::from_millis(150));
                sock.write_all(b"Host: x\r\n").expect("write 2");
                thread::sleep(Duration::from_millis(150));
                sock.write_all(b"Connection: close\r\n\r\n")
                    .expect("write 3");

                let (buf, _) = sock.try_to_close().expect("segmented head response");
                let text = String::from_utf8_lossy(&buf);
                assert!(text.starts_with("HTTP/1.1 200 "), "status: {text:?}");
                assert!(
                    !text.contains("408"),
                    "false close on slow progress: {text:?}"
                );
                assert!(text.ends_with("hello"), "body: {text:?}");
            },
            |result| {
                (result)
                    .as_ref()
                    .expect("slow_progress_completes server shutdown");
            },
        )
        .expect("harness");
}

#[test]
fn timer_floor_covers_all() {
    use driver::settings::Schedule;

    let harness = Pair::bind().expect("reserve test address");
    let bind = harness.addr();
    harness
        .check(
            |context, listener| {
                let server = server::Plan::new(bind, HEAD_TIMEOUT).build();
                let baseline = Config::for_profile::<Throughput>();
                let baseline = baseline.set_schedule(Schedule::fixed::<1024, 0>());
                let plan = server.with_bound(listener);
                let clock = date::Clock::new()
                    .map_err(|error| core::BranchError::First(Error::Date(error)))?;
                let storage = With::new((), clock);
                let prepared = topology::Plan::new(storage, plan)
                    .with_install(baseline)
                    .map_err(core::BranchError::Second)?;
                assert!(
                    prepared.layout().schedule().timers().count().get() > server::MAX_CONNECTIONS,
                );
                {
                    let mut reports = reclaim::Reports::new(
                        prepared.layout(),
                        [<FrameworkTag<kind::Any> as Tag>::ROUTE],
                    );
                    let mut finish = session::Completion::new();
                    prepared.enter_with(
                        context,
                        |scope| {
                            scope.lend(|plan, stage| {
                                let app = HelloDispatch::new(
                                    &(),
                                    sark_fiber::Config::new(server::MAX_CONNECTIONS),
                                );
                                plan.install(
                                    stage,
                                    domain::Local::<wire::Identity, _>::new(wire::Identity),
                                    app,
                                    |root, stage| {
                                        let result = stage
                                            .run_quiesce(&mut finish, root, Duration::from_secs(2))
                                            .map_err(topology::Failure::Run)?;
                                        result.map_err(|error| {
                                            topology::Failure::Run(session::Error::Drive(error))
                                        })
                                    },
                                )
                                .map_err(topology::Failure::Install)?
                            })
                        },
                        Policy::new(|report| reports.push(report), recover),
                        |result| {
                            let result = result.map_err(topology::RunError::from);
                            result
                                .as_ref()
                                .map_err(|error| error.cause())
                                .expect("topology")
                                .as_ref()
                                .map_err(|error| error.cause())
                                .expect("worker");
                        },
                    );
                    assert!(
                        reports.events().expect("reclaimed resources").is_empty(),
                        "unexpected reclaimed server operations: {reports:?}"
                    );
                    Ok::<_, BranchError<Error, Claim>>(())
                }
            },
            |bind| {
                let mut tracked = TcpStream::connect(bind).expect("connect tracked");
                tracked
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .expect("set timeout");
                tracked
                    .write_all(b"GET /hello HTTP/1.1\r\nHost: tracked\r\n")
                    .expect("write tracked head");
                thread::sleep(Duration::from_millis(50));

                let mut second = TcpStream::connect(bind).expect("connect second");
                second
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .expect("set timeout");
                second
                    .write_all(b"GET /hello HTTP/1.1\r\nHost: second\r\n")
                    .expect("write second head");

                let (reply_b, elapsed) = second.try_to_close().expect("second head retirement");
                assert!(
                    elapsed >= Duration::from_millis(300),
                    "second connection closed before its deadline: {elapsed:?}"
                );
                assert!(
                    elapsed < Duration::from_secs(2),
                    "second connection was not deadline-tracked: {elapsed:?}"
                );
                let (reply_a, _) = tracked.try_to_close().expect("tracked head retirement");
                for reply in [reply_a, reply_b] {
                    let text = String::from_utf8_lossy(&reply);
                    assert!(
                        reply.is_empty() || text.contains("408"),
                        "expected close or 408, got: {text:?}"
                    );
                }
            },
            |result| {
                result.as_ref().expect("server shutdown");
            },
        )
        .expect("harness");
}
