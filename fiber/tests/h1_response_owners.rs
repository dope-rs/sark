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

use common::{
    route::{
        chunked::peer::Peer,
        keep::{
            self, KeepAliveDispatch, LARGE_STATIC_BODY,
            events::{Identity, IdentityLog, Log},
        },
    },
    server,
};
use dope::{core::driver::route::KeyTag, net::wire};
use sark_fiber::Config;

#[test]
fn rejects_one_exchange() {
    let events = keep::record(|bind| {
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
    });

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
    let events = keep::record(|bind| {
        let socket = TcpPeer::at(bind)
            .try_read(Duration::from_secs(2))
            .expect("keep-alive peer");
        let mut peer = Peer::new(socket);
        for turn in 1..=2 {
            let (status, body) = peer
                .try_exchange(b"GET /ping HTTP/1.1\r\nHost: x\r\n\r\n")
                .expect("keep-alive response");
            assert_eq!(
                (status, body.as_slice()),
                (200, b"pong".as_slice()),
                "request {turn}"
            );
        }

        peer.socket
            .write_all(b"GET /ping HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .expect("write closing request");
        let mut last = Vec::new();
        peer.socket
            .read_to_end(&mut last)
            .expect("read closing response");
        let last = String::from_utf8(last).expect("closing response text");
        assert!(last.contains("200 OK"), "last: {}", last);
    });

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
    let (harness, server) = server::Plan::isolated();
    let mut events = Vec::new();

    harness
        .run(
            |context, listener| {
                let audits = Cell::new(None);
                keep::try_run(
                    server,
                    context,
                    listener,
                    IdentityLog(Cell::from_mut(&mut events), &audits),
                    &audits,
                )
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
        )
        .expect("orders_owner_release server shutdown");

    let mut events = events.into_iter();
    for id in [1, 2] {
        assert_eq!(events.next(), Some(Identity::Opened(id)));
        assert_eq!(events.next(), Some(Identity::Committed(id)));
        assert_eq!(events.next(), Some(Identity::Retired(id)));
        let Some(Identity::Handoff(sent, charge)) = events.next() else {
            panic!("missing transport handoff for exchange {id}");
        };
        assert_eq!(sent, id);
        assert!(!charge.is_empty() && charge.owners == 1);
        assert_eq!(events.next(), Some(Identity::Released(id, charge)));
    }
    assert_eq!(events.next(), Some(Identity::Closed));
    assert_eq!(events.next(), None);
}

#[test]
fn static_stays_borrowed() {
    let events = keep::record(|bind| {
        let mut socket = TcpStream::connect(bind).expect("connect static H1 client");
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set static response deadline");
        socket
            .write_all(b"GET /large-static HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
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
    });

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
    let observer = Log(&events, &audits);
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
    let events = keep::record(|bind| {
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
    });

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
