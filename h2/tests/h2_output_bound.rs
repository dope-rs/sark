use std::{cell::Cell, convert::Infallible, io::Read, time::Duration};

use dope_test::harness::server::Pair;
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ClientRole,
    conn::{config::Config, event::Event, machine::Connection},
};

pub mod common;

use common::{
    body::{self, Configure, Peer, headers},
    bounded::{BODY_LEN, Oversized},
    session::{Connect, Serve},
};

#[test]
fn rejects_oversized_owner() {
    let pair = Pair::bind().expect("H2 output-bound harness");
    let bind = pair.addr();
    pair.check(
        move |context, listener| {
            let calls = Cell::new(0);
            let reports = Oversized::<0>(&calls)
                .try_serve(
                    body::LIMITS.try_plan(bind).expect("H2 server plan"),
                    context,
                    listener,
                    common::recover,
                    |result| result.expect("H2 server runtime"),
                )
                .expect("H2 server resources");
            assert!(
                reports.events().expect("H2 retirement").is_empty(),
                "{reports:?}"
            );
            assert_eq!(calls.get(), 1);
            Ok::<(), Infallible>(())
        },
        |address| {
            let mut socket = address
                .try_connect(Duration::from_secs(3))
                .expect("H2 peer connection");
            let mut client = Connection::<ClientRole>::try_new().expect("H2 client");
            client
                .streams()
                .headers()
                .request(
                    headers(b"/bound", b"0").expect("request fields"),
                    Boundary::End,
                )
                .expect("request oversized response");
            client.try_flush(&mut socket).expect("H2 peer flush");
            let mut response = Vec::new();
            socket
                .read_to_end(&mut response)
                .expect("rejected connection closes");
            client
                .ingress()
                .ingest(&response)
                .expect("response ingress");
            while let Some(event) = client.ingress().poll_event() {
                assert!(!matches!(event, Event::Data { .. }), "rejected owner data");
            }
        },
        |result| result.expect("H2 server runtime"),
    )
    .expect("rejected output-bound E2E");
}

#[test]
fn accepts_bounded_owner() {
    let pair = Pair::bind().expect("H2 output-bound harness");
    let bind = pair.addr();
    pair.check(
        move |context, listener| {
            let calls = Cell::new(0);
            let reports = Oversized::<BODY_LEN>(&calls)
                .try_serve(
                    body::LIMITS.try_plan(bind).expect("H2 server plan"),
                    context,
                    listener,
                    common::recover,
                    |result| result.expect("H2 server runtime"),
                )
                .expect("H2 server resources");
            assert!(
                reports.events().expect("H2 retirement").is_empty(),
                "{reports:?}"
            );
            assert_eq!(calls.get(), 1);
            Ok::<(), Infallible>(())
        },
        |address| {
            let mut socket = address
                .try_connect(Duration::from_secs(3))
                .expect("H2 peer connection");
            let mut client = Connection::<ClientRole>::try_new().expect("H2 client");
            let stream = client
                .streams()
                .headers()
                .request(
                    headers(b"/bound", b"0").expect("request fields"),
                    Boundary::End,
                )
                .expect("request bounded response");
            client.try_flush(&mut socket).expect("H2 peer flush");
            assert_eq!(
                client
                    .try_read(&mut socket, stream)
                    .expect("H2 response body"),
                vec![b'x'; BODY_LEN]
            );
        },
        |result| result.expect("H2 server runtime"),
    )
    .expect("accepted output-bound E2E");
}

#[test]
fn flushes_wrapped_output() {
    use std::net::{TcpListener, TcpStream};

    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("flush receiver");
    let address = listener.local_addr().expect("flush address");
    let timeout = Duration::from_secs(3);
    let mut socket = TcpStream::connect_timeout(&address, timeout).expect("flush sender");
    socket
        .set_write_timeout(Some(timeout))
        .expect("send timeout");
    let (mut receiver, _) = listener.accept().expect("accept flush sender");
    receiver
        .set_read_timeout(Some(timeout))
        .expect("receive timeout");
    let mut client = Connection::<ClientRole>::with_config(Config {
        outbound_capacity: 128,
        ..Config::default()
    })
    .expect("bounded client output");
    client.transfer().drain(usize::MAX);
    for byte in 0..10 {
        if byte == 7 {
            client.transfer().drain(100);
        }
        client.control().ping([byte; 8]).expect("queued output");
    }
    let (first, second) = client.output().slices();
    assert!(!first.is_empty() && !second.is_empty());
    let expected = [first, second].concat();
    client.try_flush(&mut socket).expect("flush both slices");
    assert!(client.output().is_empty());
    let mut received = vec![0; expected.len()];
    receiver.read_exact(&mut received).expect("complete output");
    assert_eq!(received, expected);
}
