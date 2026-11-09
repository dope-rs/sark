pub mod common;

use std::{
    cell::Cell,
    convert::Infallible,
    io::{Read, Write},
    net::TcpStream,
    rc::Rc,
    time::Duration,
};

use common::server::{peer::Endpoint as _, recover};
use common::{
    route::body::stream::*,
    server::{self, Plan},
};
use dope::{
    core::driver::life::recovery::Policy,
    net::wire::{self, domain},
    runtime::executor::session,
};
use dope_fiber::task::storage::scope::Brand;
use dope_test::harness::server::Pair;
use sark_core::{
    deadline,
    life::{self, audit},
};
use sark_fiber::Config;
use sark_h1::server::run;

#[test]
fn pending_chunks_complete() {
    let harness = Pair::bind().expect("bind isolated listener");
    let bind = harness.addr();
    let server = server::Plan::new(bind, Duration::from_secs(10)).build();

    harness
        .check(
            |context, listener| {
                let mut reports = Plan::try_reports(&server).expect("server descriptor claims");
                let result = StreamPendingDispatch::serve(
                    server.with_bound(listener),
                    context,
                    (),
                    Config::new(server::MAX_CONNECTIONS),
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
                result
            },
            |bind| {
                let resp = bind.try_get("/stream").expect("complete pending stream");
                let resp_str = String::from_utf8_lossy(&resp);

                assert!(resp_str.contains("200 OK"), "resp: {resp_str}");
                assert!(
                    resp_str
                        .to_lowercase()
                        .contains("transfer-encoding: chunked"),
                    "resp: {resp_str}"
                );
                assert!(resp.ends_with(b"5\r\nalpha\r\n5\r\n-beta\r\n6\r\n-gamma\r\n0\r\n\r\n"));
            },
            |result| {
                assert!(
                    result.is_ok(),
                    "pending_chunks_complete server shutdown: {:?}",
                    result.err()
                );
            },
        )
        .expect("harness");
}

#[test]
fn streams_response() {
    let harness = Pair::bind().expect("bind isolated listener");
    let bind = harness.addr();
    let server = server::Plan::new(bind, Duration::from_secs(10)).build();

    harness
        .check(
            |context, listener| {
                let mut reports = Plan::try_reports(&server).expect("server descriptor claims");
                let result = StreamDispatch::serve(server.with_bound(listener), context,
                    State(|result| result.expect("stream response")),
                    Config::new(server::MAX_CONNECTIONS),
                    Policy::new(
                        |event| reports.push(event),
                        recover,
                    ),
                    |result| {
                        result.as_ref().map(|_| ()).map_err(|error| error.cause()).expect("server")
                    },
                );
                assert!(reports.events().expect("reclaimed resources").is_empty(), "server retirement: {reports:?}");
                result
            },
            |bind| {
                let response = bind.try_get("/stream").expect("complete stream");
                assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"));
                assert!(String::from_utf8_lossy(&response).to_lowercase().contains("transfer-encoding: chunked"));
                assert!(response.ends_with(b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n"));
                let response = bind.try_get("/arrays").expect("complete array stream");
                assert!(response.ends_with(b"6\r\nhello \r\n6\r\nworld!\r\n0\r\n\r\n"));
                let mut sock = TcpStream::connect(bind).expect("connect body stream");
                sock.set_read_timeout(Some(Duration::from_secs(3)))
                    .expect("connected socket must accept a read timeout");
                sock.write_all(
                    b"POST /echo HTTP/1.1\r\nHost: x\r\nx-echo: borrowed\r\nContent-Length: 11\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n",
                )
                .expect("connected socket must accept the echo headers");
                let mut interim = Vec::new();
                let mut chunk = [0u8; 256];
                while !interim.ends_with(b"\r\n\r\n") {
                    let count = sock.read(&mut chunk).expect("read 100 Continue");
                    assert!(count > 0, "connection closed before 100 Continue");
                    interim.extend_from_slice(&chunk[..count]);
                }
                assert_eq!(interim, b"HTTP/1.1 100 Continue\r\n\r\n");

                sock.write_all(b"hello world")
                    .expect("connected socket must accept the echo body");
                let mut response = Vec::new();
                while !response.ends_with(b"0\r\n\r\n") {
                    let count = sock.read(&mut chunk).expect("read streamed echo");
                    assert!(count > 0, "connection closed before streamed echo");
                    response.extend_from_slice(&chunk[..count]);
                }
                assert!(
                    response.windows(6).any(|window| window == b"200 OK"),
                    "missing streamed success: {}",
                    String::from_utf8_lossy(&response),
                );
                assert!(
                    response
                        .windows(b"x-echo: borrowed".len())
                        .any(|window| window.eq_ignore_ascii_case(b"x-echo: borrowed")),
                    "missing borrowed response header: {}",
                    String::from_utf8_lossy(&response),
                );
                assert!(
                    response.ends_with(b"b\r\nhello world\r\n0\r\n\r\n"),
                    "request body was not streamed unchanged: {}",
                    String::from_utf8_lossy(&response),
                );
            },
            |result| {
                assert!(result.is_ok(), "streamed response server shutdown: {:?}", result.err());
            },
        )
        .expect("harness");
}

#[test]
fn late_failure_closes() {
    for (method, path) in [("GET", "/direct"), ("GET", "/later"), ("HEAD", "/later")] {
        let pair = Pair::bind().expect("listener");
        let plan = Plan::new(pair.addr(), Duration::from_secs(10))
            .build()
            .policy(
                deadline::config::Config::try_new(deadline::Phase::Total, Duration::from_secs(10))
                    .expect("deadline"),
            );
        pair.check(
            |context, listener| {
                let cause = String::from("read-failed");
                let trace = Rc::new(common::route::body::Trace::default());
                let state = Failure { cause: &cause, trace: Rc::clone(&trace) };
                let diagnostics = Cell::new(None);
                let events = Cell::new(Vec::new());
                let snapshots = Cell::new(Vec::new());
                let mut reports = Plan::try_reports(&plan).expect("claims");
                let prepared = plan.with_bound(listener).prepare(()).expect("resources");
                let mut done = session::Completion::new();
                prepared.enter_with(
                    context,
                    |worker| worker.lend(|plan, stage| Brand::with(|brand| {
                        let app = StreamFailureDispatch::with_observer(
                            brand, &state, Config::new(server::MAX_CONNECTIONS),
                            Observer { events: &events, snapshots: &snapshots, diagnostics: &diagnostics },
                        );
                        plan.install(
                            stage,
                            domain::Local::<wire::Identity, _>::new(wire::Identity),
                            app,
                            |root, wait, stage| run::Failure::<Infallible, _>::session(
                                stage.with_app(&mut done, root, |app| run::run(wait, app)),
                            ),
                        ).expect("install").as_ref().map_err(run::Failure::cause).expect("session");
                    })),
                    Policy::new(|event| reports.push(event), recover),
                    |result| { result.as_ref().map_err(|error| error.cause()).expect("retirement"); },
                );
                assert!(reports.events().expect("reclaimed resources").is_empty());
                assert_eq!(trace.polls.get(), if method == "HEAD" { 0 } else { 4 });
                assert_eq!(trace.drops.get(), 1);
                assert!(!trace.moved.get());
                assert_eq!(diagnostics.into_inner(), (method != "HEAD").then(|| format!("{cause:?}")));
                let mut snapshots = snapshots.into_inner();
                assert!(snapshots.last().expect("final audit").is_zero(
                    common::route::keep::events::OWNERS.union(audit::Capability::TASK_COMPLETION_SLOTS),
                ));
                if method == "GET" && path == "/later" {
                    assert_eq!(events.into_inner(), [
                        Event::Ready, Event::Committed, Event::Terminal(life::Outcome::Rejected), Event::Retired,
                    ]);
                    snapshots.retain(|snapshot| snapshot.capabilities() == audit::Capability::TASK_COMPLETION_SLOTS);
                    snapshots.dedup();
                    assert_eq!(snapshots, [audit::Snapshot::default().task_slots(1), audit::Snapshot::default().task_slots(0)]);
                }
                Ok::<_, Infallible>(())
            },
            |address| {
                let mut socket = TcpStream::connect(address).expect("connect");
                socket.set_read_timeout(Some(Duration::from_secs(3))).expect("timeout");
                let request = if method == "HEAD" {
                    format!("HEAD {path} HTTP/1.1\r\nHost: stream\r\nConnection: close\r\n\r\n")
                } else {
                    format!("GET {path} HTTP/1.1\r\nHost: stream\r\n\r\nGET /later HTTP/1.1\r\nHost: stream\r\nConnection: close\r\n\r\n")
                };
                socket.write_all(request.as_bytes()).expect("request");
                let mut response = String::new();
                socket.read_to_string(&mut response).expect("real EOF after response");
                assert!(response.starts_with("HTTP/1.1 200 "), "{response}");
                assert_eq!(response.matches("HTTP/1.1").count(), 1, "{response}");
                let (_, body) = response.split_once("\r\n\r\n").expect("response head");
                assert_eq!(body, if method == "HEAD" { "" } else { "c\r\nbefore-error\r\n" });
            },
            |result| result.expect("server"),
        ).expect("pair");
    }
}
