use std::{
    cell::{Cell, RefCell},
    convert::Infallible,
    io::{Read as _, Write as _},
    net::TcpStream,
};

use common::{
    app::Authorized,
    echo::Echo,
    peer::Peer as _,
    protocol::Payload as _,
    upgrade::server::{BAD_KEY, BAD_PATH, with_headers},
};
use dope::{
    core::driver::life::recovery,
    manifold::timing::Throughput,
    runtime::{
        executor::session::Completion,
        topology::{self, lending, ops::Run},
    },
};
use dope_test::harness::server::Pair;
use sark_http::h1::head::ParseError;
use sark_ws::{server, upgrade};

pub mod common;

#[test]
fn rejection_cause_once() {
    let pair = Pair::bind().expect("listener");
    let config = Echo::config(pair.addr());
    pair.run(
        move |ctx, bound| -> Result<(), Infallible> {
            let failures = RefCell::new(Vec::new());
            let messages = Cell::new(0);
            let listener = server::Plan::new(config)
                .expect("config")
                .with_bound(Some(bound))
                .with_reader(
                    |_| messages.set(messages.get() + 1),
                    |error| failures.borrow_mut().push(error),
                );
            let prepared = topology::Plan::new((), listener)
                .for_install::<Throughput>()
                .expect("layout");
            let mut finish = Completion::new();
            prepared.enter_with(
                ctx,
                |worker| {
                    worker
                        .with(lending::Clean(&mut finish))
                        .map_err(topology::Failure::Install)?
                        .map_err(|error| error.map_install(|never| match never {}))
                },
                recovery::Policy::new(|_| {}, common::recover),
                |result| assert!(matches!(result, Ok(Ok(())))),
            );
            assert_eq!(messages.get(), 0);
            assert!(matches!(
                failures.into_inner().as_slice(),
                [
                    server::Error::Open(ParseError::Sink(upgrade::Error::Key(
                        upgrade::KeyError::Length { actual: 3 }
                    ))),
                    server::Error::Handshake(upgrade::Error::Target {
                        offset: 1,
                        actual: Some(b'o'),
                        want: None
                    }),
                    server::Error::Forbidden,
                    server::Error::Open(ParseError::Sink(upgrade::Error::Duplicate(
                        upgrade::Header::Origin
                    ))),
                    server::Error::Open(ParseError::Sink(upgrade::Error::Duplicate(
                        upgrade::Header::Cookie
                    ))),
                ]
            ));
            Ok(())
        },
        |bind| {
            for request in [BAD_KEY, BAD_PATH] {
                let mut socket = TcpStream::try_socket(bind).expect("peer");
                socket.write_all(request).expect("request");
                let (head, _) = socket.try_head(&mut Vec::new()).expect("rejection");
                assert!(head.starts_with(b"HTTP/1.1 400"));
            }
            for (fields, status) in [
                (
                    b"Origin: https://other.test\r\n".as_slice(),
                    b"HTTP/1.1 403",
                ),
                (b"Origin: x\r\nOrigin: y\r\n", b"HTTP/1.1 400"),
                (b"Cookie: x\r\nCookie: y\r\n", b"HTTP/1.1 400"),
            ] {
                let mut socket = TcpStream::try_socket(bind).expect("peer");
                socket.write_all(&with_headers(fields)).expect("opening");
                let (head, _) = socket.try_head(&mut Vec::new()).expect("rejection");
                assert!(head.starts_with(status));
            }
        },
    )
    .expect("server");
}

#[test]
fn opening_policy_preserves_borrowed_fields_and_rejects_before_frames() {
    Pair::bind()
        .expect("listener")
        .run(
            |context, bound| -> Result<(), Infallible> {
                let openings = Cell::new(0);
                let messages = Cell::new(0);
                let failures = RefCell::new(Vec::new());
                common::server::try_new(
                    Echo::config(bound.local_addr()),
                    Authorized {
                        openings: &openings,
                        messages: &messages,
                        failures: &failures,
                    },
                    bound,
                )
                .expect("listener")
                .enter(
                    context,
                    Run(&mut Completion::new()),
                    recovery::Policy::new(|_| {}, common::recover),
                    |result| assert!(matches!(result, Ok(Ok(_)))),
                );
                assert_eq!(openings.get(), 3);
                assert_eq!(messages.get(), 1);
                assert!(matches!(
                    failures.into_inner().as_slice(),
                    [server::Error::Forbidden, server::Error::Forbidden]
                ));
                Ok(())
            },
            |bind| {
                for (fields, allowed) in [
                    (
                        b"Origin: https://allowed.test\r\nCookie: session=good\r\n".as_slice(),
                        true,
                    ),
                    (
                        b"Origin: https://other.test\r\nCookie: session=good\r\n",
                        false,
                    ),
                    (
                        b"Origin: https://allowed.test\r\nCookie: session=bad\r\n",
                        false,
                    ),
                ] {
                    let mut socket = TcpStream::try_socket(bind).expect("peer");
                    let mut request = with_headers(fields);
                    request.extend(b"tail".try_masked(0x2, true).expect("coalesced frame"));
                    socket.write_all(&request).expect("opening and frame");
                    let (head, tail) = socket.try_head(&mut Vec::new()).expect("opening response");
                    assert!(head.starts_with(if allowed {
                        b"HTTP/1.1 101"
                    } else {
                        b"HTTP/1.1 403"
                    }));
                    assert!(tail.is_empty());
                    assert_eq!(socket.read(&mut [0; 1]).expect("closed response"), 0);
                }
            },
        )
        .expect("server");
}
