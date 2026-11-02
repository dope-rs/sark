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
