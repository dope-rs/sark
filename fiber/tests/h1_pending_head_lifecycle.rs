pub mod common;

use std::{
    cell::Cell,
    io::{Read, Write},
    net::TcpStream,
    thread,
    time::Duration,
};

use common::server::recover;
use common::{
    route::keep::{
        Server,
        events::{Log, REJECTED_REQUESTS, diagnose},
    },
    server,
};
use dope::core::driver::life::recovery::Policy;
use dope_test::harness::server::Pair;

#[test]
fn rejects_head_once() {
    let pair = Pair::bind().expect("listener");
    let plan = server::Plan::new(pair.addr(), Duration::from_secs(10)).build();
    let mut events = Vec::new();
    pair.check(
        |context, listener| {
            let log = Cell::new(Vec::new());
            let audits = Cell::new(None);
            let mut reports = server::Plan::try_reports(&plan).expect("claims");
            Server::new(plan, Log(&log, &audits, diagnose))
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
        |address| {
            for (prefix, suffix) in REJECTED_REQUESTS {
                for split in [false, true] {
                    let mut socket = TcpStream::connect(address).expect("connect");
                    socket
                        .set_read_timeout(Some(Duration::from_secs(2)))
                        .expect("response deadline");
                    if split {
                        socket.write_all(prefix).expect("pending head");
                        thread::sleep(Duration::from_millis(20));
                        socket.write_all(suffix).expect("rejected continuation");
                    } else {
                        socket
                            .write_all(&[prefix, suffix].concat())
                            .expect("rejected request");
                    }
                    let mut response = Vec::new();
                    socket.read_to_end(&mut response).expect("closed response");
                    assert!(response.starts_with(b"HTTP/1.1 400 "));
                }
            }
        },
        |result| events = result.expect("server shutdown"),
    )
    .expect("observed H1 runtime");

    let events: Vec<_> = events
        .into_iter()
        .filter(|event| {
            matches!(
                *event,
                "exchange-opened"
                    | "request-owner-scope-closed"
                    | "terminal"
                    | "exchange-retired"
                    | "session-closed"
            )
        })
        .collect();
    assert_eq!(
        events,
        [
            "exchange-opened",
            "request-owner-scope-closed",
            "terminal",
            "exchange-retired",
            "session-closed",
        ]
        .repeat(REJECTED_REQUESTS.len() * 2),
    );
}
