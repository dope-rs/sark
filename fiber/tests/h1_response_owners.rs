use dope_test::harness::peer::Peer as TcpPeer;

pub mod common;

use core::mem;
use std::{
    cell::Cell,
    io::{Read, Write},
    net::TcpStream,
    thread,
    time::Duration,
};

use common::server::recover;
use common::{
    route::{
        chunked::peer::Peer,
        keep::{
            KeepAliveDispatch, LARGE_STATIC_BODY, Server,
            events::{Identity, IdentityLog, Log, diagnose},
        },
    },
    server,
};
use dope::{
    core::driver::{life::recovery::Policy, route::KeyTag},
    net::wire,
};
use dope_test::harness::server::Pair;
use sark_fiber::Config;

#[test]
fn rejects_one_exchange() {
    let harness = Pair::bind().expect("bind isolated listener");
    let bind = harness.addr();
    let server = server::Plan::new(bind, Duration::from_secs(10)).build();
    let mut events = Vec::new();

    harness
        .check(
            |context, listener| {
                let log = Cell::new(Vec::new());
                let audits = Cell::new(None);
                let mut reports = server::Plan::try_reports(&server).expect("claims");
                Server::new(server, Log(&log, &audits, diagnose))
                    .try_run(
                        context,
                        listener,
                        Policy::new(|event| reports.push(event), recover),
                        |result| result.expect("observed server"),
                    )
                    .map(|()| {
                        assert_eq!(audits.get(), None);
                        assert!(reports.events().expect("reclaimed resources").is_empty());
                        log.into_inner()
                    })
            },
            |bind| {
                let mut socket = TcpStream::connect(bind).expect("connect observed H1 rejection");
                socket
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .expect("set rejection deadline");
                socket
                    .write_all(b"GET /missing HTTP/1.1\r\nHo")
                    .expect("write partial rejected head");
                thread::sleep(Duration::from_millis(20));
                socket
                    .write_all(b"st: x\r\nConnection: close\r\n\r\n")
                    .expect("finish rejected head");

                let mut response = String::new();
                socket
                    .read_to_string(&mut response)
                    .expect("read rejection response");
                assert!(response.contains("404 Not Found"), "response: {response}");
            },
            |result| events = result.expect("rejects_one_exchange server shutdown"),
        )
        .expect("observed H1 rejection runtime");

    for expected in [
        "exchange-opened",
        "request-owner-scope-closed",
        "terminal",
        "exchange-retired",
    ] {
        assert_eq!(
            events.iter().filter(|event| **event == expected).count(),
            1,
            "slow rejection must commit one {expected}: {events:?}",
        );
    }
    assert_eq!(
        events
            .iter()
            .filter(|event| **event == "head-admitted")
            .count(),
        0,
        "an unmatched request never crosses application head admission: {events:?}",
    );
}

#[test]
fn keeps_connection_open() {
    let harness = Pair::bind().expect("bind isolated listener");
    let bind = harness.addr();
    let server = server::Plan::new(bind, Duration::from_secs(10)).build();
    let mut events = Vec::new();

    harness
        .check(
            |context, listener| {
                let log = Cell::new(Vec::new());
                let audits = Cell::new(None);
                let mut reports = server::Plan::try_reports(&server).expect("claims");
                Server::new(server, Log(&log, &audits, diagnose))
                    .try_run(
                        context,
                        listener,
                        Policy::new(|event| reports.push(event), recover),
                        |result| result.expect("observed server"),
                    )
                    .map(|()| {
                        assert_eq!(audits.get(), None);
                        assert!(reports.events().expect("reclaimed resources").is_empty());
                        log.into_inner()
                    })
            },
            |bind| {
                let mut sock = TcpPeer::at(bind).try_connect().expect("connect");
                sock.read_wait(Some(Duration::from_secs(2)))
                    .expect("set keep-alive deadline");

                sock.write_all(b"GET /ping HTTP/1.1\r\nHost: x\r\n\r\n")
                    .expect("write first keep-alive request");
                let mut buffered = Vec::new();
                let (status, body) = Peer::new(&mut sock, &mut buffered)
                    .try_response()
                    .expect("first keep-alive response");
                assert_eq!(status, 200);
                assert_eq!(body, b"pong");

                sock.write_all(b"GET /ping HTTP/1.1\r\nHost: x\r\n\r\n")
                    .expect("write second keep-alive request");
                let (status, body) = Peer::new(&mut sock, &mut buffered)
                    .try_response()
                    .expect("second keep-alive response");
                assert_eq!(status, 200);
                assert_eq!(body, b"pong");

                sock.write_all(b"GET /ping HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                    .expect("write closing request");
                let mut last = Vec::new();
                sock.read_to_end(&mut last).expect("read closing response");
                let last = String::from_utf8(last).expect("closing response text");
                assert!(last.contains("200 OK"), "last: {}", last);
            },
            |result| events = result.expect("keeps_connection_open server shutdown"),
        )
        .expect("harness");

    for expected in ["session-opened", "session-established", "session-closed"] {
        assert!(events.contains(&expected), "missing {expected}: {events:?}");
    }
    for expected in [
        "exchange-opened",
        "head-admitted",
        "input-finished",
        "response-committed",
        "terminal",
        "exchange-retired",
    ] {
        assert_eq!(
            events.iter().filter(|event| **event == expected).count(),
            3,
            "unexpected {expected} sequence: {events:?}",
        );
    }
    for name in ["response-handoff", "response-transport-released"] {
        assert_eq!(
            events.iter().filter(|event| **event == name).count(),
            0,
            "inline H1 bodies must not manufacture {name}: {events:?}",
        );
    }
}

#[test]
fn orders_owner_release() {
    let harness = Pair::bind().expect("bind isolated listener");
    let bind = harness.addr();
    let server = server::Plan::new(bind, Duration::from_secs(10)).build();
    let mut events = Vec::new();

    harness
        .check(
            |context, listener| {
                let log = Cell::new(Vec::new());
                let audits = Cell::new(None);
                let mut reports = server::Plan::try_reports(&server).expect("claims");
                Server::new(server, IdentityLog(&log, &audits, diagnose))
                    .try_run(
                        context,
                        listener,
                        Policy::new(|event| reports.push(event), recover),
                        |result| result.expect("observed server"),
                    )
                    .map(|()| {
                        assert_eq!(audits.get(), None);
                        assert!(reports.events().expect("reclaimed resources").is_empty());
                        log.into_inner()
                    })
            },
            |bind| {
                let mut socket = TcpStream::connect(bind).expect("connect pipelined H1 client");
                socket
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .expect("set pipeline deadline");
                socket
                    .write_all(b"GET /large HTTP/1.1\r\nHo")
                    .expect("write incomplete first head");
                thread::sleep(Duration::from_millis(20));
                socket
                    .write_all(
                        b"st: x\r\n\r\n\
                          GET /large HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
                    )
                    .expect("write pipelined requests");

                let mut response = String::new();
                socket
                    .read_to_string(&mut response)
                    .expect("read pipelined responses");
                assert_eq!(
                    response.matches("200 OK").count(),
                    2,
                    "response: {response}"
                );
                assert!(response.len() >= 64 * 1024, "response too short");
            },
            |result| events = result.expect("orders_owner_release server shutdown"),
        )
        .expect("pipelined H1 runtime");

    let opened = Identity::select(&events, |event| match event {
        Identity::Opened(ordinal) => Some(ordinal),
        _ => None,
    });
    let handoffs = Identity::select(&events, |event| match event {
        Identity::Handoff(ordinal, _) => Some(ordinal),
        _ => None,
    });
    let released = Identity::select(&events, |event| match event {
        Identity::Released(ordinal, _) => Some(ordinal),
        _ => None,
    });
    let retired = Identity::select(&events, |event| match event {
        Identity::Retired(ordinal) => Some(ordinal),
        _ => None,
    });
    assert_eq!(opened, [1, 2], "pipelined exchanges");
    assert_eq!(
        Identity::select(&events, |event| match event {
            Identity::Committed(ordinal) => Some(ordinal),
            _ => None,
        }),
        [1, 2]
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, Identity::StagingReleased(_)))
    );
    assert_eq!(handoffs, [1, 2], "pipelined handoffs");
    assert_eq!(released, [1, 2], "pipelined final owners");
    assert_eq!(retired, [1, 2], "pipelined retirements");
    let handed = Identity::select(&events, |event| match event {
        Identity::Handoff(_, charge) => Some(charge),
        _ => None,
    });
    let released = Identity::select(&events, |event| match event {
        Identity::Released(_, charge) => Some(charge),
        _ => None,
    });
    assert!(handed == released, "transport charge drift");
    assert!(
        handed
            .iter()
            .all(|charge| !charge.is_empty() && charge.owners == 1),
        "large responses must carry one exact runtime owner",
    );
    let first = events
        .iter()
        .position(|event| matches!(event, Identity::Released(1, _)))
        .expect("first final-owner release");
    let second = events
        .iter()
        .position(|event| *event == Identity::Opened(2))
        .expect("second exchange open");
    assert!(
        first < second,
        "the second H1 identity advanced while the first owner was live: {events:?}",
    );
    let last = events
        .iter()
        .position(|event| matches!(event, Identity::Released(2, _)))
        .expect("second final-owner release");
    assert!(
        last < events
            .iter()
            .position(|event| *event == Identity::Closed)
            .expect("pipelined H1 session close"),
        "the H1 session retired before its last owner: {events:?}",
    );
}

#[test]
fn static_stays_borrowed() {
    let harness = Pair::bind().expect("bind isolated listener");
    let bind = harness.addr();
    let server = server::Plan::new(bind, Duration::from_secs(10)).build();
    let mut events = Vec::new();

    harness
        .check(
            |context, listener| {
                let log = Cell::new(Vec::new());
                let audits = Cell::new(None);
                let mut reports = server::Plan::try_reports(&server).expect("claims");
                Server::new(server, Log(&log, &audits, diagnose))
                    .try_run(
                        context,
                        listener,
                        Policy::new(|event| reports.push(event), recover),
                        |result| result.expect("observed server"),
                    )
                    .map(|()| {
                        assert_eq!(audits.get(), None);
                        assert!(reports.events().expect("reclaimed resources").is_empty());
                        log.into_inner()
                    })
            },
            |bind| {
                let mut socket = TcpStream::connect(bind).expect("connect static H1 client");
                socket
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .expect("set static response deadline");
                socket
                    .write_all(
                        b"GET /large-static HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
                    )
                    .expect("write static request");
                let mut response = Vec::new();
                socket
                    .read_to_end(&mut response)
                    .expect("read static response");
                assert!(
                    response.windows(6).any(|window| window == b"200 OK"),
                    "missing success response",
                );
                let body = response
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map(|head_end| &response[head_end + 4..])
                    .expect("complete response head");
                assert_eq!(body, LARGE_STATIC_BODY);
            },
            |result| events = result.expect("static_stays_borrowed server shutdown"),
        )
        .expect("large static H1 runtime");

    for name in ["response-handoff", "response-transport-released"] {
        assert_eq!(
            events.iter().filter(|event| **event == name).count(),
            0,
            "borrowed static backing must not manufacture {name}: {events:?}",
        );
    }
}

#[test]
fn disabled_omits_ledger() {
    let disabled = KeepAliveDispatch::new::<wire::Identity, KeyTag<0>>(
        &(),
        Config::new(server::MAX_CONNECTIONS),
    );
    let events = Cell::new(Vec::new());
    let audits = Cell::new(None);
    let observer = Log(&events, &audits, diagnose);
    let observed = KeepAliveDispatch::with_observer::<wire::Identity, KeyTag<0>, _>(
        &(),
        Config::new(server::MAX_CONNECTIONS),
        observer,
    );
    assert_eq!(
        mem::size_of_val(&observed),
        mem::size_of_val(&disabled) + mem::size_of_val(&observer),
    );
}

#[test]
fn suspended_body_owns() {
    let harness = Pair::bind().expect("bind isolated listener");
    let bind = harness.addr();
    let server = server::Plan::new(bind, Duration::from_secs(10)).build();
    let mut events = Vec::new();

    harness
        .check(
            |context, listener| {
                let log = Cell::new(Vec::new());
                let audits = Cell::new(None);
                let mut reports = server::Plan::try_reports(&server).expect("claims");
                Server::new(server, Log(&log, &audits, diagnose))
                    .try_run(
                        context,
                        listener,
                        Policy::new(|event| reports.push(event), recover),
                        |result| result.expect("observed server"),
                    )
                    .map(|()| {
                        assert_eq!(audits.get(), None);
                        assert!(reports.events().expect("reclaimed resources").is_empty());
                        log.into_inner()
                    })
            },
            |bind| {
                let mut socket = TcpStream::connect(bind).expect("connect observed H1 body");
                socket
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .expect("set body response deadline");
                socket
                    .write_all(b"POST /body HTTP/1.1\r\nHost: x\r\nContent-Len")
                    .expect("write partial body head");
                thread::sleep(Duration::from_millis(20));
                socket
                    .write_all(b"gth: 12\r\nConnection: close\r\n\r\n")
                    .expect("finish body head");
                thread::sleep(Duration::from_millis(20));
                socket.write_all(b"first-").expect("write first body part");
                thread::sleep(Duration::from_millis(20));
                socket.write_all(b"second").expect("write second body part");

                let mut response = String::new();
                socket
                    .read_to_string(&mut response)
                    .expect("read body response");
                assert!(response.contains("200 OK"), "response: {response}");
                assert!(response.ends_with("body:12"), "response: {response}");
            },
            |result| events = result.expect("suspended_body_owns server shutdown"),
        )
        .expect("observed H1 body runtime");

    let reserved = events
        .iter()
        .filter(|event| **event == "pre-admission-reserved")
        .count();
    assert!(
        reserved >= 1,
        "body never crossed pre-admission: {events:?}"
    );
    for expected in ["exchange-opened", "head-admitted"] {
        assert_eq!(
            events.iter().filter(|event| **event == expected).count(),
            1,
            "slow head must admit one logical exchange: {events:?}",
        );
    }
    assert_eq!(
        events
            .iter()
            .filter(|event| **event == "pre-admission-released")
            .count(),
        1,
        "the exact aggregate must leave pre-admission once: {events:?}",
    );
    for expected in [
        "request-owner-acquired",
        "request-owner-scope-closed",
        "terminal",
        "exchange-retired",
        "session-closed",
    ] {
        assert!(events.contains(&expected), "missing {expected}: {events:?}");
    }
}
