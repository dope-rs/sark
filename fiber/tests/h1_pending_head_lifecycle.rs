pub mod common;

use std::{
    io::{Read, Write},
    net::TcpStream,
    thread,
    time::Duration,
};

use common::route::keep::{self, events::REJECTED_REQUESTS};

#[test]
fn rejects_head_once() {
    let events = keep::record(|address| {
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
    });

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
